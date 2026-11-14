use crate::{StateDir, id::MakimaId};
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use sha2::{Digest, Sha256};
use std::{fmt, io, str::FromStr};

const HASH_LENGTH: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct BlobHash(String);

impl BlobHash {
    pub fn digest(bytes: &[u8]) -> Self {
        Self(format!("{:x}", Sha256::digest(bytes)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for BlobHash {
    type Err = BlobError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != HASH_LENGTH
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(BlobError::InvalidHash);
        }
        Ok(Self(value.to_owned()))
    }
}

impl fmt::Display for BlobHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for BlobHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobRef {
    pub session: MakimaId,
    pub hash: BlobHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum FileState {
    Absent,
    Blob(BlobRef),
}

#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("blob hash must be exactly 64 lowercase hexadecimal characters")]
    InvalidHash,
    #[error("blob owner session {0} is unavailable")]
    SessionUnavailable(MakimaId),
    #[error("blob {0:?} does not match its content hash")]
    Corrupt(BlobRef),
    #[error("unsafe blob storage path: {0}")]
    Unsafe(&'static str),
    #[error("blob storage I/O failed: {0}")]
    Io(#[from] io::Error),
}

#[derive(Clone, Debug)]
pub struct BlobStore {
    state: StateDir,
}

impl BlobStore {
    pub fn new(state: StateDir) -> Self {
        Self { state }
    }

    pub fn put(&self, session: MakimaId, bytes: &[u8]) -> Result<BlobRef, BlobError> {
        self.put_checked(session, bytes, || Ok(()))
    }

    pub fn read(&self, reference: &BlobRef) -> Result<Vec<u8>, BlobError> {
        #[cfg(unix)]
        {
            unix::read(&self.state, reference)
        }
        #[cfg(not(unix))]
        {
            let _ = (&self.state, reference);
            Err(unsupported())
        }
    }

    pub fn copy_to(&self, owner: MakimaId, source: &BlobRef) -> Result<BlobRef, BlobError> {
        let bytes = self.read(source)?;
        self.put_checked(owner, &bytes, || self.ensure_session(source.session))
    }

    fn ensure_session(&self, session: MakimaId) -> Result<(), BlobError> {
        #[cfg(unix)]
        {
            unix::Sessions::open(&self.state, session)?.validate(session)
        }
        #[cfg(not(unix))]
        {
            let _ = session;
            Err(unsupported())
        }
    }

    fn put_checked(
        &self,
        session: MakimaId,
        bytes: &[u8],
        guard: impl FnMut() -> Result<(), BlobError>,
    ) -> Result<BlobRef, BlobError> {
        #[cfg(unix)]
        {
            unix::put(&self.state, session, bytes, guard, |file| file.sync_all())
        }
        #[cfg(not(unix))]
        {
            let _ = (session, bytes, guard);
            Err(unsupported())
        }
    }
}

#[cfg(not(unix))]
fn unsupported() -> BlobError {
    io::Error::new(
        io::ErrorKind::Unsupported,
        "secure blob storage is unavailable on this platform",
    )
    .into()
}

#[cfg(unix)]
mod unix {
    use super::{BlobError, BlobHash, BlobRef};
    use crate::{StateDir, id::MakimaId, sessions::SESSIONS_DIR};
    use rustix::{
        fs::{self, AtFlags, Mode, OFlags},
        io::Errno,
    };
    use std::{
        ffi::{OsStr, OsString},
        fs::File,
        io::{self, Read, Write},
        os::unix::fs::MetadataExt,
        path::{Component, Path},
    };

    const BLOBS_DIR: &str = "blobs";
    const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);
    const READ_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::NOFOLLOW)
        .union(OFlags::NONBLOCK)
        .union(OFlags::CLOEXEC);
    const CREATE_FLAGS: OFlags = OFlags::WRONLY
        .union(OFlags::CREATE)
        .union(OFlags::EXCL)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);

    pub(super) struct Sessions {
        root: File,
        chain: Vec<(OsString, File)>,
    }

    impl Sessions {
        pub(super) fn open(state: &StateDir, session: MakimaId) -> Result<Self, BlobError> {
            let path = if state.path().is_absolute() {
                state.path().to_owned()
            } else {
                std::env::current_dir()?.join(state.path())
            };
            let root = File::from(
                fs::open(Path::new("/"), DIRECTORY_FLAGS, Mode::empty())
                    .map_err(io::Error::from)?,
            );
            let mut directories = Self {
                root,
                chain: Vec::new(),
            };
            for component in path.components() {
                match component {
                    Component::RootDir | Component::CurDir => {}
                    Component::Normal(name) => directories
                        .descend(name, false)
                        .map_err(|error| unavailable_if_missing(error, session))?,
                    _ => return Err(BlobError::Unsafe("parent or prefix component")),
                }
            }
            directories
                .descend(OsStr::new(SESSIONS_DIR), false)
                .map_err(|error| unavailable_if_missing(error, session))?;
            directories.validate(session)?;
            Ok(directories)
        }

        fn current(&self) -> &File {
            self.chain.last().map_or(&self.root, |(_, file)| file)
        }

        fn descend(&mut self, name: &OsStr, create: bool) -> Result<(), BlobError> {
            if create {
                match fs::mkdirat(self.current(), name, Mode::RWXU) {
                    Ok(()) => {
                        self.current().sync_all()?;
                    }
                    Err(Errno::EXIST) => {}
                    Err(error) => return Err(path_error(error)),
                }
            }
            let file = File::from(
                fs::openat(self.current(), name, DIRECTORY_FLAGS, Mode::empty())
                    .map_err(path_error)?,
            );
            self.chain.push((name.to_owned(), file));
            Ok(())
        }

        pub(super) fn validate(&self, session: MakimaId) -> Result<(), BlobError> {
            let sessions = self
                .chain
                .iter()
                .rev()
                .find(|(name, _)| name.as_os_str() == OsStr::new(SESSIONS_DIR))
                .map(|(_, file)| file)
                .ok_or(BlobError::Unsafe("missing sessions anchor"))?;
            let name = format!("{session}.jsonl");
            let file = File::from(
                fs::openat(sessions, name.as_str(), READ_FLAGS, Mode::empty()).map_err(
                    |error| {
                        if error == Errno::NOENT {
                            BlobError::SessionUnavailable(session)
                        } else {
                            path_error(error)
                        }
                    },
                )?,
            );
            if !file.metadata()?.is_file() {
                return Err(BlobError::Unsafe("session is not a regular file"));
            }
            self.validate_chain()?;
            verify_entry(sessions, OsStr::new(&name), &file)
                .map_err(|error| unavailable_if_missing(error, session))?;
            Ok(())
        }

        fn validate_chain(&self) -> Result<(), BlobError> {
            let mut parent = self.root.try_clone()?;
            for (name, expected) in &self.chain {
                let actual = File::from(
                    fs::openat(&parent, name, DIRECTORY_FLAGS, Mode::empty())
                        .map_err(path_error)?,
                );
                let expected = expected.metadata()?;
                let observed = actual.metadata()?;
                if expected.dev() != observed.dev() || expected.ino() != observed.ino() {
                    return Err(BlobError::Unsafe("directory was replaced"));
                }
                parent = actual;
            }
            Ok(())
        }

        fn owner(&mut self, session: MakimaId, create: bool) -> Result<(), BlobError> {
            self.descend(OsStr::new(BLOBS_DIR), create)?;
            self.descend(OsStr::new(&session.to_string()), create)
        }

        fn sync(&self, sync: &mut impl FnMut(&File) -> io::Result<()>) -> Result<(), BlobError> {
            for (_, directory) in self.chain.iter().rev() {
                sync(directory)?;
            }
            sync(&self.root)?;
            Ok(())
        }
    }

    fn unavailable_if_missing(error: BlobError, session: MakimaId) -> BlobError {
        match error {
            BlobError::Io(ref source) if source.kind() == io::ErrorKind::NotFound => {
                BlobError::SessionUnavailable(session)
            }
            error => error,
        }
    }

    fn path_error(error: Errno) -> BlobError {
        if error == Errno::LOOP || error == Errno::NOTDIR {
            BlobError::Unsafe("symlink or non-directory component")
        } else {
            BlobError::Io(error.into())
        }
    }

    fn read_at(parent: &File, reference: &BlobRef) -> Result<Vec<u8>, BlobError> {
        let mut file = File::from(
            fs::openat(parent, reference.hash.as_str(), READ_FLAGS, Mode::empty())
                .map_err(path_error)?,
        );
        if !file.metadata()?.is_file() {
            return Err(BlobError::Unsafe("blob is not a regular file"));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        if BlobHash::digest(&bytes) != reference.hash {
            return Err(BlobError::Corrupt(reference.clone()));
        }
        verify_entry(parent, OsStr::new(reference.hash.as_str()), &file)?;
        Ok(bytes)
    }

    fn verify_entry(parent: &File, name: &OsStr, file: &File) -> Result<(), BlobError> {
        let actual =
            File::from(fs::openat(parent, name, READ_FLAGS, Mode::empty()).map_err(path_error)?);
        let actual = actual.metadata()?;
        let expected = file.metadata()?;
        if actual.dev() != expected.dev() || actual.ino() != expected.ino() {
            return Err(BlobError::Unsafe("file was replaced"));
        }
        Ok(())
    }

    pub(super) fn read(state: &StateDir, reference: &BlobRef) -> Result<Vec<u8>, BlobError> {
        let mut directories = Sessions::open(state, reference.session)?;
        directories.owner(reference.session, false)?;
        let bytes = read_at(directories.current(), reference)?;
        directories.validate(reference.session)?;
        Ok(bytes)
    }

    struct Staged<'a> {
        parent: &'a File,
        name: Option<&'a str>,
    }

    impl Drop for Staged<'_> {
        fn drop(&mut self) {
            if let Some(name) = self.name {
                let _ = fs::unlinkat(self.parent, name, AtFlags::empty());
            }
        }
    }

    pub(super) fn put(
        state: &StateDir,
        session: MakimaId,
        bytes: &[u8],
        mut guard: impl FnMut() -> Result<(), BlobError>,
        mut sync: impl FnMut(&File) -> io::Result<()>,
    ) -> Result<BlobRef, BlobError> {
        let reference = BlobRef {
            session,
            hash: BlobHash::digest(bytes),
        };
        let mut directories = Sessions::open(state, session)?;
        guard()?;
        directories.owner(session, true)?;
        directories.validate(session)?;
        guard()?;
        let parent = directories.current();
        match read_at(parent, &reference) {
            Ok(existing) => {
                if existing != bytes {
                    return Err(BlobError::Corrupt(reference));
                }
            }
            Err(BlobError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                let name = format!(".{}.tmp", MakimaId::generate());
                let mut file = File::from(
                    fs::openat(parent, name.as_str(), CREATE_FLAGS, Mode::RUSR | Mode::WUSR)
                        .map_err(path_error)?,
                );
                let mut temporary = Staged {
                    parent,
                    name: Some(&name),
                };
                file.write_all(bytes)?;
                sync(&file)?;
                directories.validate(session)?;
                guard()?;
                match fs::linkat(
                    parent,
                    name.as_str(),
                    parent,
                    reference.hash.as_str(),
                    AtFlags::empty(),
                ) {
                    Ok(()) | Err(Errno::EXIST) => {}
                    Err(error) => return Err(path_error(error)),
                }
                fs::unlinkat(parent, name.as_str(), AtFlags::empty()).map_err(io::Error::from)?;
                temporary.name = None;
            }
            Err(error) => return Err(error),
        }
        let existing = read_at(parent, &reference)?;
        if existing != bytes {
            return Err(BlobError::Corrupt(reference));
        }
        let file = File::from(
            fs::openat(parent, reference.hash.as_str(), READ_FLAGS, Mode::empty())
                .map_err(path_error)?,
        );
        sync(&file)?;
        directories.sync(&mut sync)?;
        directories.validate(session)?;
        guard()?;
        verify_entry(parent, OsStr::new(reference.hash.as_str()), &file)?;
        let existing = read_at(parent, &reference)?;
        if existing != bytes {
            return Err(BlobError::Corrupt(reference));
        }
        directories.validate(session)?;
        Ok(reference)
    }
}

#[cfg(test)]
mod tests {
    use super::{BlobError, BlobHash, BlobRef, BlobStore, FileState, HASH_LENGTH};
    #[cfg(unix)]
    use crate::sessions::SESSIONS_DIR;
    use crate::{StateDir, id::MakimaId, sessions::Session};
    use serde_json::Value;
    #[cfg(unix)]
    use std::{fs, path::PathBuf};
    use tempfile::TempDir;
    use test_case::test_case;

    const CONTENT: &[u8] = b"\x00\xffblob bytes\n";
    const EMPTY_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    const TEST_MODEL: &str = "blob-test";

    fn fixture() -> (TempDir, StateDir, BlobStore, MakimaId) {
        let temporary = tempfile::tempdir().unwrap();
        let state = StateDir::from_path(temporary.path().to_owned());
        let owner = session(&state);
        let store = BlobStore::new(state.clone());
        (temporary, state, store, owner)
    }

    fn session(state: &StateDir) -> MakimaId {
        let mut session = Session::<Value, Value, Value>::new(TEST_MODEL, "/");
        session.save(state).unwrap();
        session.id
    }

    #[cfg(unix)]
    fn session_path(state: &StateDir, owner: MakimaId) -> PathBuf {
        state
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{owner}.jsonl"))
    }

    #[cfg(unix)]
    fn blob_path(state: &StateDir, reference: &BlobRef) -> PathBuf {
        state
            .path()
            .join(SESSIONS_DIR)
            .join("blobs")
            .join(reference.session.to_string())
            .join(reference.hash.as_str())
    }

    #[test_case(""; "empty")]
    #[test_case("../escape"; "traversal")]
    #[test_case("/absolute"; "absolute")]
    #[test_case("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b85"; "short")]
    #[test_case("E3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; "uppercase")]
    #[test_case("g3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; "non_hex")]
    fn rejects_invalid_hash(value: &str) {
        assert!(matches!(
            value.parse::<BlobHash>(),
            Err(BlobError::InvalidHash)
        ));
        assert!(serde_json::from_value::<BlobHash>(Value::String(value.to_owned())).is_err());
    }

    #[test]
    fn hashes_and_file_states_round_trip() {
        let hash = BlobHash::digest(b"");
        assert_eq!(hash.as_str(), EMPTY_HASH);
        assert_eq!(hash.to_string().len(), HASH_LENGTH);
        assert_eq!(EMPTY_HASH.parse::<BlobHash>().unwrap(), hash);
        let reference = BlobRef {
            session: MakimaId::generate(),
            hash,
        };
        for state in [FileState::Absent, FileState::Blob(reference)] {
            let value = serde_json::to_value(&state).unwrap();
            assert_eq!(
                value["state"],
                if state == FileState::Absent {
                    "absent"
                } else {
                    "blob"
                }
            );
            assert_eq!(serde_json::from_value::<FileState>(value).unwrap(), state);
        }
    }

    #[cfg(unix)]
    #[test_case(b""; "empty")]
    #[test_case(CONTENT; "binary")]
    fn writes_deduplicates_and_copies(bytes: &[u8]) {
        use std::os::unix::fs::MetadataExt;

        let (_temporary, state, store, owner) = fixture();
        let reference = store.put(owner, bytes).unwrap();
        let original = fs::metadata(blob_path(&state, &reference)).unwrap();
        assert_eq!(store.put(owner, bytes).unwrap(), reference);
        assert_eq!(
            fs::metadata(blob_path(&state, &reference)).unwrap().ino(),
            original.ino()
        );
        assert_eq!(store.clone().read(&reference).unwrap(), bytes);
        assert_eq!(
            fs::read_dir(blob_path(&state, &reference).parent().unwrap())
                .unwrap()
                .count(),
            1
        );
        let destination = session(&state);
        let copied = store.copy_to(destination, &reference).unwrap();
        assert_eq!(copied.session, destination);
        assert_eq!(copied.hash, reference.hash);
        assert_eq!(store.read(&copied).unwrap(), bytes);
        fs::remove_file(session_path(&state, owner)).unwrap();
        assert!(
            matches!(store.copy_to(destination, &reference), Err(BlobError::SessionUnavailable(id)) if id == owner)
        );
        assert_eq!(store.read(&copied).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_corruption_without_overwriting() {
        let (_temporary, state, store, owner) = fixture();
        let reference = store.put(owner, CONTENT).unwrap();
        fs::write(blob_path(&state, &reference), b"corrupt").unwrap();
        assert!(
            matches!(store.read(&reference), Err(BlobError::Corrupt(found)) if found == reference)
        );
        assert!(
            matches!(store.put(owner, CONTENT), Err(BlobError::Corrupt(found)) if found == reference)
        );
        assert_eq!(fs::read(blob_path(&state, &reference)).unwrap(), b"corrupt");
    }

    #[cfg(unix)]
    #[test]
    fn requires_live_owner_for_every_operation() {
        let (_temporary, state, store, owner) = fixture();
        let missing = MakimaId::generate();
        assert!(
            matches!(store.put(missing, CONTENT), Err(BlobError::SessionUnavailable(id)) if id == missing)
        );
        assert!(!state.path().join(SESSIONS_DIR).join("blobs").exists());
        let reference = store.put(owner, CONTENT).unwrap();
        fs::remove_file(session_path(&state, owner)).unwrap();
        assert!(
            matches!(store.read(&reference), Err(BlobError::SessionUnavailable(id)) if id == owner)
        );
        assert!(
            matches!(store.put(owner, CONTENT), Err(BlobError::SessionUnavailable(id)) if id == owner)
        );
        let destination = session(&state);
        assert!(
            matches!(store.copy_to(destination, &reference), Err(BlobError::SessionUnavailable(id)) if id == owner)
        );
        assert!(
            !state
                .path()
                .join(SESSIONS_DIR)
                .join("blobs")
                .join(destination.to_string())
                .exists()
        );
    }

    #[cfg(unix)]
    #[test_case(0; "state_ancestor")]
    #[test_case(1; "state_root")]
    #[test_case(2; "sessions")]
    #[test_case(3; "blob_store")]
    #[test_case(4; "blob_owner")]
    #[test_case(5; "blob_file")]
    #[test_case(6; "owner_log")]
    fn rejects_symlinks(level: usize) {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let ancestor = temporary.path().join("ancestor");
        let state = StateDir::from_path(ancestor.join("state"));
        let owner = session(&state);
        let store = BlobStore::new(state.clone());
        let reference = store.put(owner, CONTENT).unwrap();
        let blob = blob_path(&state, &reference);
        let paths = [
            ancestor,
            state.path().to_owned(),
            state.path().join(SESSIONS_DIR),
            state.path().join(SESSIONS_DIR).join("blobs"),
            blob.parent().unwrap().to_owned(),
            blob,
            session_path(&state, owner),
        ];
        let target = temporary.path().join("moved");
        fs::rename(&paths[level], &target).unwrap();
        symlink(&target, &paths[level]).unwrap();
        assert!(matches!(store.read(&reference), Err(BlobError::Unsafe(_))));
        assert!(matches!(
            store.put(owner, CONTENT),
            Err(BlobError::Unsafe(_))
        ));
        assert!(matches!(
            store.copy_to(owner, &reference),
            Err(BlobError::Unsafe(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn sync_failure_before_publication_leaves_no_blob() {
        use super::unix;
        use std::io;

        const SYNC_ERROR: &str = "injected blob file sync failure";
        let (_temporary, state, store, owner) = fixture();
        let reference = BlobRef {
            session: owner,
            hash: BlobHash::digest(CONTENT),
        };
        let error = unix::put(
            &state,
            owner,
            CONTENT,
            || Ok(()),
            |_| Err(io::Error::other(SYNC_ERROR)),
        )
        .unwrap_err();
        assert!(matches!(error, BlobError::Io(source) if source.to_string() == SYNC_ERROR));
        let path = blob_path(&state, &reference);
        assert!(!path.exists());
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
        assert_eq!(store.put(owner, CONTENT).unwrap(), reference);
    }

    #[cfg(unix)]
    #[test]
    fn deletion_at_publication_gate_leaves_no_blob() {
        use super::unix;

        let (_temporary, state, _store, owner) = fixture();
        let reference = BlobRef {
            session: owner,
            hash: BlobHash::digest(CONTENT),
        };
        let result = unix::put(
            &state,
            owner,
            CONTENT,
            || Ok(()),
            |file| {
                file.sync_all()?;
                fs::remove_file(session_path(&state, owner))
            },
        );
        assert!(matches!(result, Err(BlobError::SessionUnavailable(id)) if id == owner));
        assert!(!blob_path(&state, &reference).exists());
    }

    #[cfg(unix)]
    #[test_case(false; "real_directory")]
    #[test_case(true; "symlink")]
    fn directory_replacement_at_publication_gate_is_rejected(symlink_replacement: bool) {
        use super::unix;
        use std::os::unix::fs::symlink;

        let (temporary, state, _store, owner) = fixture();
        let reference = BlobRef {
            session: owner,
            hash: BlobHash::digest(CONTENT),
        };
        let path = blob_path(&state, &reference);
        let directory = path.parent().unwrap();
        let moved = temporary.path().join("moved-owner");
        let result = unix::put(
            &state,
            owner,
            CONTENT,
            || Ok(()),
            |file| {
                file.sync_all()?;
                fs::rename(directory, &moved)?;
                if symlink_replacement {
                    symlink(&moved, directory)
                } else {
                    fs::create_dir(directory)
                }
            },
        );
        assert!(matches!(result, Err(BlobError::Unsafe(_))));
        assert!(!path.exists());
        assert_eq!(fs::read_dir(moved).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn source_deletion_at_copy_publication_gate_is_rejected() {
        use super::unix;

        let (_temporary, state, store, owner) = fixture();
        let source = store.put(owner, CONTENT).unwrap();
        let destination = session(&state);
        let bytes = store.read(&source).unwrap();
        let result = unix::put(
            &state,
            destination,
            &bytes,
            || store.ensure_session(source.session),
            |file| {
                file.sync_all()?;
                fs::remove_file(session_path(&state, owner))
            },
        );
        assert!(matches!(result, Err(BlobError::SessionUnavailable(id)) if id == owner));
        let target = BlobRef {
            session: destination,
            hash: source.hash,
        };
        assert!(!blob_path(&state, &target).exists());
    }

    #[cfg(unix)]
    #[test]
    fn failed_directory_sync_is_reported_and_retry_is_safe() {
        use super::unix;
        use std::io;

        const SYNC_ERROR: &str = "injected blob directory sync failure";
        let (_temporary, state, store, owner) = fixture();
        let reference = BlobRef {
            session: owner,
            hash: BlobHash::digest(CONTENT),
        };
        let result = unix::put(
            &state,
            owner,
            CONTENT,
            || Ok(()),
            |file| {
                if file.metadata()?.is_dir() {
                    Err(io::Error::other(SYNC_ERROR))
                } else {
                    file.sync_all()
                }
            },
        );
        assert!(matches!(result, Err(BlobError::Io(source)) if source.to_string() == SYNC_ERROR));
        assert_eq!(store.read(&reference).unwrap(), CONTENT);
        assert_eq!(store.put(owner, CONTENT).unwrap(), reference);
        assert_eq!(
            fs::read_dir(blob_path(&state, &reference).parent().unwrap())
                .unwrap()
                .count(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn deletion_during_directory_sync_prevents_success() {
        use super::unix;

        let (_temporary, state, _store, owner) = fixture();
        let mut deleted = false;
        let result = unix::put(
            &state,
            owner,
            CONTENT,
            || Ok(()),
            |file| {
                file.sync_all()?;
                if file.metadata()?.is_dir() && !deleted {
                    fs::remove_file(session_path(&state, owner))?;
                    deleted = true;
                }
                Ok(())
            },
        );
        assert!(deleted);
        assert!(matches!(result, Err(BlobError::SessionUnavailable(id)) if id == owner));
    }

    #[cfg(unix)]
    #[test_case(false; "owner_not_regular")]
    #[test_case(true; "blob_not_regular")]
    fn rejects_non_regular_files(blob: bool) {
        let (_temporary, state, store, owner) = fixture();
        let reference = store.put(owner, CONTENT).unwrap();
        let path = if blob {
            blob_path(&state, &reference)
        } else {
            session_path(&state, owner)
        };
        fs::remove_file(&path).unwrap();
        fs::create_dir(path).unwrap();
        assert!(matches!(store.read(&reference), Err(BlobError::Unsafe(_))));
        assert!(matches!(
            store.put(owner, CONTENT),
            Err(BlobError::Unsafe(_))
        ));
    }

    #[cfg(not(unix))]
    #[test]
    fn unsupported_platform_fails_closed() {
        use std::io::ErrorKind;

        let (_temporary, _state, store, owner) = fixture();
        let reference = BlobRef {
            session: owner,
            hash: BlobHash::digest(CONTENT),
        };
        for result in [store.put(owner, CONTENT), store.copy_to(owner, &reference)] {
            assert!(
                matches!(result, Err(BlobError::Io(error)) if error.kind() == ErrorKind::Unsupported)
            );
        }
        assert!(
            matches!(store.read(&reference), Err(BlobError::Io(error)) if error.kind() == ErrorKind::Unsupported)
        );
    }
}
