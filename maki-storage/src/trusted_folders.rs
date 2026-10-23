use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::lock::{Lock, LockError};
use crate::paths::{canonicalize_clean, normalize_path};
use crate::{StateDir, StorageError, atomic_write};

const DOCUMENT_FILE: &str = "trusted-folders.json";
/// The store holds no compatibility with older formats, so the way out of an
/// unreadable one is to start over.
const RESET_HINT: &str = "delete the file and makima asks about each folder again";
const DOCUMENT_VERSION: u32 = 2;
const LOCK_FILE: &str = "trusted-folders.lock";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalFolder {
    path: PathBuf,
    entry: String,
}

impl CanonicalFolder {
    pub fn resolve(path: &Path) -> Result<Self, TrustedFoldersError> {
        let metadata = fs::metadata(path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                TrustedFoldersError::Missing {
                    path: path.to_path_buf(),
                }
            } else {
                TrustedFoldersError::Io {
                    path: path.to_path_buf(),
                    source,
                }
            }
        })?;
        if !metadata.is_dir() {
            return Err(TrustedFoldersError::NotDirectory {
                path: path.to_path_buf(),
            });
        }

        let canonical = fs::canonicalize(path).map_err(|source| TrustedFoldersError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let canonical = canonicalize_clean(&canonical);
        let entry = path_entry(&canonical)?;
        Ok(Self {
            path: canonical,
            entry,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn as_str(&self) -> &str {
        &self.entry
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Change {
    Changed,
    Unchanged,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrustStatus {
    Trusted,
    Rejected,
    Unknown,
}

/// What the store says about a folder, read against the gated files that folder
/// ships right now.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TrustDecision {
    /// Trusted, and every kind of gated file present now was named in the
    /// question the user answered.
    Trusted,
    /// Trusted before, and now shipping kinds of gated file that answer never
    /// covered. `added` names them so the question can say what changed.
    Widened {
        added: Vec<String>,
    },
    Rejected,
    Unknown,
}

#[derive(Debug, Eq, PartialEq)]
pub struct FolderTrustDecision {
    pub path: PathBuf,
    pub status: TrustStatus,
}

#[derive(Debug, thiserror::Error)]
pub enum TrustedFoldersError {
    #[error("trusted folder does not exist: {path:?}")]
    Missing { path: PathBuf },
    #[error("trusted folder is not a directory: {path:?}")]
    NotDirectory { path: PathBuf },
    #[error("trusted folder path is not valid UTF-8: {path:?}")]
    NonUtf8 { path: PathBuf },
    #[error("trusted folder path contains a control character: {path:?}")]
    ControlCharacter { path: PathBuf },
    #[error("a missing folder must be removed by absolute path: {path:?}")]
    RelativeMissing { path: PathBuf },
    #[error("unsupported trusted folder store version {version} in {path:?}; {RESET_HINT}")]
    UnsupportedVersion { path: PathBuf, version: u32 },
    #[error("trusted folders are being changed by another Maki process: {path:?}")]
    Locked { path: PathBuf },
    #[error("cannot access trusted folder state at {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("cannot parse trusted folder state at {path:?}: {source}; {RESET_HINT}")]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("cannot write trusted folder state at {path:?}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: StorageError,
    },
}

/// A recorded yes, together with the kinds of gated file the user could see
/// named in the question. Keeping the set means a yes about
/// `.maki/permissions.toml` never quietly covers an `.maki/init.lua` that
/// arrives in a later pull. It is a set of file names, never a hash of their
/// contents, so editing a file the user already trusted asks nothing.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct FolderEntry {
    path: String,
    files: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Document {
    version: u32,
    folders: Vec<FolderEntry>,
    rejected: Vec<String>,
}

impl Default for Document {
    fn default() -> Self {
        Self {
            version: DOCUMENT_VERSION,
            folders: Vec::new(),
            rejected: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct TrustedFolders {
    document_path: PathBuf,
    lock_path: PathBuf,
}

impl TrustedFolders {
    pub fn new(state: &StateDir) -> Self {
        Self {
            document_path: state.path().join(DOCUMENT_FILE),
            lock_path: state.path().join(LOCK_FILE),
        }
    }

    pub fn contains(&self, folder: &CanonicalFolder) -> Result<bool, TrustedFoldersError> {
        Ok(self.status(folder)? == TrustStatus::Trusted)
    }

    pub fn status(&self, folder: &CanonicalFolder) -> Result<TrustStatus, TrustedFoldersError> {
        let document = self.load()?;
        Ok(document.status(folder))
    }

    /// `present` is the gated file kinds the folder ships now. Passing them in
    /// keeps this crate out of the business of knowing which files are gated.
    pub fn decide(
        &self,
        folder: &CanonicalFolder,
        present: &[&str],
    ) -> Result<TrustDecision, TrustedFoldersError> {
        Ok(self.load()?.decide(folder, present))
    }

    /// Records a yes for `folder` covering `files`. An existing entry keeps
    /// what it already covered, so a file that is temporarily absent, say
    /// because another branch is checked out, does not cost the folder its
    /// answer for that file.
    pub fn add(
        &self,
        folder: &CanonicalFolder,
        files: &[&str],
    ) -> Result<Change, TrustedFoldersError> {
        let _lock = self.mutation_lock()?;
        let mut document = self.load()?;
        let index = document
            .folders
            .iter()
            .position(|entry| entry.path == folder.as_str());

        let mut covered: Vec<String> = files.iter().map(|file| (*file).to_owned()).collect();
        if let Some(index) = index {
            covered.extend_from_slice(&document.folders[index].files);
        }
        covered.sort();
        covered.dedup();

        let was_rejected = document
            .rejected
            .iter()
            .any(|entry| entry == folder.as_str());
        let already_covered = index.is_some_and(|index| document.folders[index].files == covered);
        if already_covered && !was_rejected {
            return Ok(Change::Unchanged);
        }

        document.rejected.retain(|entry| entry != folder.as_str());
        let entry = FolderEntry {
            path: folder.as_str().to_owned(),
            files: covered,
        };
        match index {
            Some(index) => document.folders[index] = entry,
            None => document.folders.push(entry),
        }
        self.save(&mut document)?;
        Ok(Change::Changed)
    }

    /// Adds a gated file Maki wrote itself to a folder that is already
    /// trusted, so the next start does not read Maki's own write as something
    /// the project added and ask about it.
    ///
    /// A folder with no answer keeps none: writing a file into a folder is not
    /// a reason to trust it.
    pub fn cover_written_file(
        &self,
        folder: &CanonicalFolder,
        file: &str,
    ) -> Result<Change, TrustedFoldersError> {
        let _lock = self.mutation_lock()?;
        let mut document = self.load()?;
        let Some(FolderEntry { files, .. }) = document
            .folders
            .iter_mut()
            .find(|entry| entry.path == folder.as_str())
        else {
            return Ok(Change::Unchanged);
        };
        if files.iter().any(|covered| covered == file) {
            return Ok(Change::Unchanged);
        }
        files.push(file.to_owned());
        files.sort();
        self.save(&mut document)?;
        Ok(Change::Changed)
    }

    pub fn reject(&self, folder: &CanonicalFolder) -> Result<Change, TrustedFoldersError> {
        let _lock = self.mutation_lock()?;
        let mut document = self.load()?;
        if document
            .rejected
            .iter()
            .any(|entry| entry == folder.as_str())
        {
            return Ok(Change::Unchanged);
        }
        document
            .folders
            .retain(|entry| entry.path != folder.as_str());
        document.rejected.push(folder.as_str().to_owned());
        self.save(&mut document)?;
        Ok(Change::Changed)
    }

    /// Matching on the normalized path rather than the exact stored spelling is
    /// what makes this the way out of a store somebody hand-edited. A stray
    /// trailing slash can never grant trust, because `status` compares exactly,
    /// but it must never be able to trap a decision in the file either.
    pub fn remove(&self, path: &Path) -> Result<Change, TrustedFoldersError> {
        let candidates = removal_entries(path)?;
        let _lock = self.mutation_lock()?;
        let mut document = self.load()?;
        let Some(candidate) = candidates.iter().find(|candidate| {
            document
                .folders
                .iter()
                .any(|entry| names_same_folder(&entry.path, candidate))
                || document
                    .rejected
                    .iter()
                    .any(|entry| names_same_folder(entry, candidate))
        }) else {
            return Ok(Change::Unchanged);
        };
        document
            .folders
            .retain(|entry| !names_same_folder(&entry.path, candidate));
        document
            .rejected
            .retain(|entry| !names_same_folder(entry, candidate));
        self.save(&mut document)?;
        Ok(Change::Changed)
    }

    pub fn list(&self) -> Result<Vec<FolderTrustDecision>, TrustedFoldersError> {
        let document = self.load()?;
        let mut decisions = document
            .folders
            .iter()
            .map(|entry| FolderTrustDecision {
                path: PathBuf::from(&entry.path),
                status: TrustStatus::Trusted,
            })
            .chain(document.rejected.iter().map(|path| FolderTrustDecision {
                path: PathBuf::from(path),
                status: TrustStatus::Rejected,
            }))
            .collect::<Vec<_>>();
        decisions.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(decisions)
    }

    fn mutation_lock(&self) -> Result<Lock, TrustedFoldersError> {
        match Lock::acquire(&self.lock_path) {
            Ok(lock) => Ok(lock),
            Err(LockError::Held { path }) => Err(TrustedFoldersError::Locked { path }),
            Err(LockError::Io { path, source }) => Err(TrustedFoldersError::Io { path, source }),
        }
    }

    fn load(&self) -> Result<Document, TrustedFoldersError> {
        let contents = match fs::read_to_string(&self.document_path) {
            Ok(contents) => contents,
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Document::default());
            }
            Err(source) => {
                return Err(TrustedFoldersError::Io {
                    path: self.document_path.clone(),
                    source,
                });
            }
        };
        let document: Document =
            serde_json::from_str(&contents).map_err(|source| TrustedFoldersError::Json {
                path: self.document_path.clone(),
                source,
            })?;
        if document.version != DOCUMENT_VERSION {
            return Err(TrustedFoldersError::UnsupportedVersion {
                path: self.document_path.clone(),
                version: document.version,
            });
        }

        // A malformed entry cannot grant anything, because `status` compares
        // against a canonical absolute path and nothing else. Refusing to load
        // over one would only turn a cosmetic typo into a store nobody can fix
        // without a text editor, `maki trust remove` included.
        Ok(document)
    }

    fn save(&self, document: &mut Document) -> Result<(), TrustedFoldersError> {
        document
            .folders
            .sort_by(|left, right| left.path.cmp(&right.path));
        document
            .folders
            .dedup_by(|left, right| left.path == right.path);
        document.rejected.sort();
        document.rejected.dedup();
        let mut bytes =
            serde_json::to_vec_pretty(document).map_err(|source| TrustedFoldersError::Json {
                path: self.document_path.clone(),
                source,
            })?;
        bytes.push(b'\n');
        atomic_write(&self.document_path, &bytes).map_err(|source| TrustedFoldersError::Write {
            path: self.document_path.clone(),
            source,
        })
    }
}

impl Document {
    fn entry(&self, folder: &CanonicalFolder) -> Option<&FolderEntry> {
        self.folders
            .iter()
            .find(|entry| entry.path == folder.as_str())
    }

    fn status(&self, folder: &CanonicalFolder) -> TrustStatus {
        if self.entry(folder).is_some() {
            TrustStatus::Trusted
        } else if self.rejected.iter().any(|entry| entry == folder.as_str()) {
            TrustStatus::Rejected
        } else {
            TrustStatus::Unknown
        }
    }

    fn decide(&self, folder: &CanonicalFolder, present: &[&str]) -> TrustDecision {
        let Some(entry) = self.entry(folder) else {
            return match self.status(folder) {
                TrustStatus::Rejected => TrustDecision::Rejected,
                _ => TrustDecision::Unknown,
            };
        };
        let added: Vec<String> = present
            .iter()
            .filter(|file| !entry.files.iter().any(|recorded| recorded == *file))
            .map(|file| (*file).to_owned())
            .collect();
        if added.is_empty() {
            TrustDecision::Trusted
        } else {
            TrustDecision::Widened { added }
        }
    }
}

fn path_entry(path: &Path) -> Result<String, TrustedFoldersError> {
    let Some(value) = path.to_str() else {
        return Err(TrustedFoldersError::NonUtf8 {
            path: path.to_path_buf(),
        });
    };
    if value.chars().any(char::is_control) {
        return Err(TrustedFoldersError::ControlCharacter {
            path: path.to_path_buf(),
        });
    }
    Ok(value.to_owned())
}

/// `candidate` is already normalized, so normalizing the stored side is what
/// lets `remove` reach an entry somebody typed a trailing slash or a `..` into.
fn names_same_folder(entry: &str, candidate: &str) -> bool {
    normalize_path(Path::new(entry)) == Path::new(candidate)
}

fn removal_entries(path: &Path) -> Result<Vec<String>, TrustedFoldersError> {
    match fs::metadata(path) {
        Ok(metadata) => {
            let normalized = normalize_path(path);
            let mut entries = vec![path_entry(&normalized)?];
            if metadata.is_dir() {
                let canonical = CanonicalFolder::resolve(path)?;
                if !entries.iter().any(|entry| entry == canonical.as_str()) {
                    entries.push(canonical.as_str().to_owned());
                }
            }
            Ok(entries)
        }
        Err(source)
            if matches!(
                source.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
            ) =>
        {
            if !path.is_absolute() {
                return Err(TrustedFoldersError::RelativeMissing {
                    path: path.to_path_buf(),
                });
            }
            let normalized = normalize_path(path);
            Ok(vec![path_entry(&normalized)?])
        }
        Err(source) => Err(TrustedFoldersError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    const INIT_LUA: &str = "init.lua";
    const MCP_TOML: &str = "mcp.toml";
    const ENV_FILE: &str = ".env";
    const NOTHING: &[&str] = &[];

    fn store(dir: &tempfile::TempDir) -> TrustedFolders {
        TrustedFolders::new(&StateDir::from_path(dir.path().to_path_buf()))
    }

    fn folder(dir: &tempfile::TempDir, name: &str) -> CanonicalFolder {
        let path = dir.path().join(name);
        fs::create_dir(&path).unwrap();
        CanonicalFolder::resolve(&path).unwrap()
    }

    #[test]
    fn missing_document_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(store(&dir).list().unwrap().is_empty());
    }

    #[test]
    fn add_remove_and_duplicate_changes_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");

        assert_eq!(store.add(&folder, &[INIT_LUA]).unwrap(), Change::Changed);
        assert_eq!(store.add(&folder, &[INIT_LUA]).unwrap(), Change::Unchanged);
        assert!(store.contains(&folder).unwrap());
        assert_eq!(store.remove(folder.path()).unwrap(), Change::Changed);
        assert!(!store.contains(&folder).unwrap());
        assert_eq!(store.remove(folder.path()).unwrap(), Change::Unchanged);
    }

    #[test]
    fn rejection_is_stored_and_later_trust_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");

        assert_eq!(store.reject(&folder).unwrap(), Change::Changed);
        assert_eq!(store.reject(&folder).unwrap(), Change::Unchanged);
        assert_eq!(store.status(&folder).unwrap(), TrustStatus::Rejected);
        assert_eq!(
            store.list().unwrap(),
            vec![FolderTrustDecision {
                path: folder.path().to_path_buf(),
                status: TrustStatus::Rejected,
            }]
        );

        assert_eq!(store.add(&folder, NOTHING).unwrap(), Change::Changed);
        assert_eq!(store.status(&folder).unwrap(), TrustStatus::Trusted);
    }

    #[test]
    fn rejection_replaces_existing_trust() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");
        store.add(&folder, &[INIT_LUA]).unwrap();

        assert_eq!(store.reject(&folder).unwrap(), Change::Changed);
        assert_eq!(store.status(&folder).unwrap(), TrustStatus::Rejected);
        assert_eq!(store.remove(folder.path()).unwrap(), Change::Changed);
        assert_eq!(store.status(&folder).unwrap(), TrustStatus::Unknown);
    }

    /// The recorded set is what a later start compares against, so an answer
    /// only ever covers the kinds of file the user was shown.
    #[test_case(&[INIT_LUA], &[INIT_LUA], TrustDecision::Trusted ; "same_file_is_covered")]
    #[test_case(&[INIT_LUA], NOTHING, TrustDecision::Trusted ; "a_removed_file_is_covered")]
    #[test_case(&[INIT_LUA, MCP_TOML], &[INIT_LUA], TrustDecision::Trusted ; "a_subset_is_covered")]
    fn a_recorded_answer_covers_what_it_named(
        recorded: &[&str],
        present: &[&str],
        expected: TrustDecision,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");
        store.add(&folder, recorded).unwrap();

        assert_eq!(store.decide(&folder, present).unwrap(), expected);
    }

    /// The defect this field exists to prevent: a yes about one file must not
    /// carry over to a kind of file the project added afterwards.
    #[test]
    fn a_newly_shipped_kind_of_file_is_not_covered() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");
        store.add(&folder, &[MCP_TOML]).unwrap();

        assert_eq!(
            store.decide(&folder, &[INIT_LUA, MCP_TOML]).unwrap(),
            TrustDecision::Widened {
                added: vec![INIT_LUA.to_owned()],
            }
        );

        store.add(&folder, &[INIT_LUA]).unwrap();
        assert_eq!(
            store.decide(&folder, &[INIT_LUA, MCP_TOML]).unwrap(),
            TrustDecision::Trusted
        );
    }

    /// A file that comes and goes with the checked out branch must not cost the
    /// folder the answer it already has for that file.
    #[test]
    fn a_file_that_disappears_keeps_its_place_in_the_recorded_set() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");
        store.add(&folder, &[INIT_LUA, ENV_FILE]).unwrap();

        assert_eq!(store.add(&folder, &[ENV_FILE]).unwrap(), Change::Unchanged);
        assert_eq!(
            store.decide(&folder, &[INIT_LUA, ENV_FILE]).unwrap(),
            TrustDecision::Trusted
        );
    }

    #[test]
    fn writes_sorted_pretty_json_with_a_final_newline() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let second = folder(&dir, "z-project");
        let first = folder(&dir, "a-project");

        store.add(&second, NOTHING).unwrap();
        store.add(&first, NOTHING).unwrap();

        let contents = fs::read_to_string(&store.document_path).unwrap();
        assert!(contents.ends_with('\n'));
        assert!(contents.find("a-project").unwrap() < contents.find("z-project").unwrap());
        assert_eq!(
            store.list().unwrap(),
            vec![
                FolderTrustDecision {
                    path: first.path,
                    status: TrustStatus::Trusted,
                },
                FolderTrustDecision {
                    path: second.path,
                    status: TrustStatus::Trusted,
                },
            ]
        );
    }

    #[test]
    fn malformed_and_unsupported_documents_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        fs::write(&store.document_path, "not json").unwrap();
        assert!(matches!(
            store.list(),
            Err(TrustedFoldersError::Json { .. })
        ));

        fs::write(
            &store.document_path,
            r#"{"version":3,"folders":[],"rejected":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            store.list(),
            Err(TrustedFoldersError::UnsupportedVersion { version: 3, .. })
        ));
    }

    /// Stores upgraded from version 1 kept bare-string entries. Nothing reads
    /// them any more, so the store is refused with a way out, not migrated.
    #[test]
    fn old_bare_string_entry_refuses_the_store_and_says_how_to_reset_it() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        fs::write(
            &store.document_path,
            format!(r#"{{"version":{DOCUMENT_VERSION},"folders":["/old/project"],"rejected":[]}}"#),
        )
        .unwrap();
        let error = store.list().unwrap_err();
        assert!(matches!(error, TrustedFoldersError::Json { .. }), "{error}");
        assert!(error.to_string().ends_with(RESET_HINT), "{error}");
    }

    #[test]
    fn unreadable_document_path_is_not_treated_as_missing() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        fs::create_dir(&store.document_path).unwrap();

        assert!(matches!(store.list(), Err(TrustedFoldersError::Io { .. })));
    }

    /// A cosmetic defect somebody typed into the file must not brick the store.
    /// It grants nothing, because trust is an exact compare, and `remove` still
    /// clears it.
    #[test_case("relative" ; "not_absolute")]
    #[test_case("/a/../b" ; "not_normalized")]
    #[test_case("/a\nb" ; "control_character")]
    fn a_hand_edited_entry_loads_and_grants_nothing(entry: &str) {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let real = folder(&dir, "project");
        fs::write(
            &store.document_path,
            serde_json::json!({
                "version": DOCUMENT_VERSION,
                "folders": [
                    {"path": entry, "files": []},
                    {"path": real.as_str(), "files": []},
                ],
                "rejected": [],
            })
            .to_string(),
        )
        .unwrap();

        assert_eq!(store.list().unwrap().len(), 2);
        assert!(store.contains(&real).unwrap());
        assert_eq!(store.remove(real.path()).unwrap(), Change::Changed);
        assert!(!store.contains(&real).unwrap());
    }

    /// The reported way to brick the store: one trailing slash typed by hand
    /// used to make every `maki trust` command fail.
    #[test]
    fn remove_clears_an_entry_spelled_with_a_trailing_slash() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let target = folder(&dir, "project");
        fs::write(
            &store.document_path,
            serde_json::json!({
                "version": DOCUMENT_VERSION,
                "folders": [{"path": format!("{}/", target.as_str()), "files": []}],
                "rejected": [],
            })
            .to_string(),
        )
        .unwrap();

        assert_eq!(
            store.status(&target).unwrap(),
            TrustStatus::Unknown,
            "a defective entry must never grant trust"
        );
        assert_eq!(store.remove(target.path()).unwrap(), Change::Changed);
        assert!(store.list().unwrap().is_empty());
    }

    #[test]
    fn add_rejects_missing_and_non_directory_paths() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(matches!(
            CanonicalFolder::resolve(&missing),
            Err(TrustedFoldersError::Missing { .. })
        ));

        let file = dir.path().join("file");
        fs::write(&file, "x").unwrap();
        assert!(matches!(
            CanonicalFolder::resolve(&file),
            Err(TrustedFoldersError::NotDirectory { .. })
        ));
    }

    #[test]
    fn remove_accepts_a_missing_absolute_path_but_not_a_relative_one() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        assert_eq!(
            store.remove(&dir.path().join("missing")).unwrap(),
            Change::Unchanged
        );
        assert!(matches!(
            store.remove(Path::new("missing")),
            Err(TrustedFoldersError::RelativeMissing { .. })
        ));
    }

    #[cfg(unix)]
    #[test]
    fn remove_uses_the_listed_identity_after_a_folder_is_replaced() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let original = folder(&dir, "original");
        let target = folder(&dir, "target");
        store.add(&original, NOTHING).unwrap();
        store.add(&target, NOTHING).unwrap();

        fs::remove_dir(original.path()).unwrap();
        symlink(target.path(), original.path()).unwrap();

        assert_eq!(store.remove(original.path()).unwrap(), Change::Changed);
        assert!(store.contains(&target).unwrap());
        assert_eq!(
            store.list().unwrap(),
            vec![FolderTrustDecision {
                path: target.path().to_path_buf(),
                status: TrustStatus::Trusted,
            }]
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_symbolic_link_resolves_to_the_same_identity() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = folder(&dir, "target");
        let alias = dir.path().join("alias");
        symlink(target.path(), &alias).unwrap();

        assert_eq!(CanonicalFolder::resolve(&alias).unwrap(), target);
    }

    #[cfg(unix)]
    #[test]
    fn remove_accepts_a_symbolic_link_to_a_trusted_folder() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let target = folder(&dir, "target");
        let alias = dir.path().join("alias");
        symlink(target.path(), &alias).unwrap();
        store.add(&target, NOTHING).unwrap();

        assert_eq!(store.remove(&alias).unwrap(), Change::Changed);
        assert!(store.list().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn remove_does_not_treat_other_metadata_errors_as_missing() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::tempdir().unwrap();
        let invalid = dir.path().join(OsString::from_vec(b"bad\0path".to_vec()));

        assert!(matches!(
            store(&dir).remove(&invalid),
            Err(TrustedFoldersError::Io { .. })
        ));
    }

    #[cfg(not(target_os = "macos"))]
    #[cfg(unix)]
    #[test]
    fn non_utf8_paths_are_refused() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::tempdir().unwrap();
        let non_utf8 = dir.path().join(OsString::from_vec(vec![b'x', 0xff]));
        // macOS 14+ (APFS) rejects non-UTF-8 names at the filesystem layer, so
        // the test only runs where the filesystem admits the name.
        fs::create_dir(&non_utf8).unwrap();
        assert!(matches!(
            CanonicalFolder::resolve(&non_utf8),
            Err(TrustedFoldersError::NonUtf8 { .. })
        ));
    }

    #[test]
    fn control_character_paths_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let control = dir.path().join("line\nbreak");
        fs::create_dir(&control).unwrap();
        assert!(matches!(
            CanonicalFolder::resolve(&control),
            Err(TrustedFoldersError::ControlCharacter { .. })
        ));
    }

    #[test]
    fn mutation_lock_prevents_lost_updates_and_releases_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(&dir);
        let folder = folder(&dir, "project");
        let held = store.mutation_lock().unwrap();

        assert!(matches!(
            store.add(&folder, NOTHING),
            Err(TrustedFoldersError::Locked { .. })
        ));
        drop(held);
        assert_eq!(store.add(&folder, NOTHING).unwrap(), Change::Changed);
    }
}
