use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, UNIX_EPOCH};

use globset::{Glob, GlobMatcher};
use maki_agent::tools::grep::GrepParams;
use maki_agent::tools::offload::OffloadBackend;
use maki_agent::{GrepFileEntry, GrepLine, GrepMatchGroup};
use regex::Regex;

use super::{
    BoxFuture, DIR_MISSING_PREFIX, DIR_NOT_A_DIR_PREFIX, FsBackend, FsError, FsMeta,
    TYPE_DIRECTORY, TYPE_FILE,
};

/// Hermetic test backend: a deliberately dumb `BTreeMap` model. No symlinks,
/// no gitignore, no permissions; file mtime is an insertion counter
/// (deterministic, no sleeps or time reads). Behavior tests must not rely on
/// real-FS vagaries — those belong in the `RealFs` suite.
///
/// Deliberate grep/glob divergences from the real backend — in-memory
/// behavior tests must not assert the real shapes:
/// - grep skips lines over `max_line_bytes` instead of truncating and
///   keeping them, emits one group per match (no merging of adjacent
///   matches within the context window), matches per line only (no multiline
///   patterns), skips non-UTF-8 files (real: byte search, NUL quit), keeps
///   `\r` (real strips it), and never excludes `.git`.
/// - glob/grep ignore the `gitignore` flag (gitignored files surface) and
///   match patterns with plain globset: slash-less patterns agree with
///   gitignore (match at any depth), but slash-containing patterns are not
///   anchored to the search root and negation patterns are unsupported.
#[derive(Default)]
pub struct InMemoryFs {
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    entries: BTreeMap<PathBuf, Entry>,
    seq: u64,
}

enum Entry {
    File(Vec<u8>, u64),
    Dir,
}

impl InMemoryFs {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                entries: BTreeMap::new(),
                seq: 0,
            }),
        }
    }

    /// All file entries as `(path, content)` pairs, for test assertions.
    pub fn files(&self) -> Vec<(PathBuf, Vec<u8>)> {
        self.inner
            .read()
            .unwrap()
            .entries
            .iter()
            .filter_map(|(p, e)| match e {
                Entry::File(bytes, _) => Some((p.clone(), bytes.clone())),
                Entry::Dir => None,
            })
            .collect()
    }

    /// Synchronous test convenience: create the parent dirs and write the
    /// file in one call.
    pub fn seed(&self, path: &Path, content: Vec<u8>) {
        let mut dir = path.to_path_buf();
        if dir.pop() && !is_fs_root(&dir) {
            smol::block_on(self.mkdir(dir, true)).unwrap();
        }
        smol::block_on(self.write(path.to_path_buf(), content)).unwrap();
    }

    fn bump_seq(inner: &mut Inner) -> u64 {
        inner.seq += 1;
        inner.seq
    }
}

/// Offload store files inside an `InMemoryFs`, so a hermetic host's Lua
/// tools can read what was offloaded without anything touching disk.
pub struct InMemoryOffloadBackend {
    fs: Arc<InMemoryFs>,
    dir: PathBuf,
}

impl InMemoryOffloadBackend {
    pub fn new(fs: Arc<InMemoryFs>, dir: PathBuf) -> Self {
        Self { fs, dir }
    }

    fn files_in_dir(inner: &Inner, dir: &Path) -> Vec<(String, usize)> {
        inner
            .entries
            .iter()
            .filter(|(path, _)| path.parent() == Some(dir))
            .filter_map(|(path, entry)| match entry {
                Entry::File(bytes, _) => Some((
                    path.file_name()?.to_string_lossy().into_owned(),
                    bytes.len(),
                )),
                Entry::Dir => None,
            })
            .collect()
    }
}

impl OffloadBackend for InMemoryOffloadBackend {
    fn read(&self, name: &str) -> IoResult<Option<Vec<u8>>> {
        let inner = self.fs.inner.read().unwrap();
        Ok(match inner.entries.get(&self.dir.join(name)) {
            Some(Entry::File(bytes, _)) => Some(bytes.clone()),
            _ => None,
        })
    }

    fn create_new(&self, name: &str, bytes: &[u8]) -> IoResult<bool> {
        let mut inner = self.fs.inner.write().unwrap();
        let path = self.dir.join(name);
        if inner.entries.contains_key(&path) {
            return Ok(false);
        }
        create_dir_all(&mut inner, &self.dir)?;
        let seq = InMemoryFs::bump_seq(&mut inner);
        inner.entries.insert(path, Entry::File(bytes.to_vec(), seq));
        Ok(true)
    }

    fn names(&self) -> IoResult<Vec<String>> {
        let inner = self.fs.inner.read().unwrap();
        Ok(Self::files_in_dir(&inner, &self.dir)
            .into_iter()
            .map(|(name, _)| name)
            .collect())
    }

    fn total_bytes(&self) -> IoResult<u64> {
        let inner = self.fs.inner.read().unwrap();
        Ok(Self::files_in_dir(&inner, &self.dir)
            .into_iter()
            .map(|(_, len)| len as u64)
            .sum())
    }

    fn remove_all(&self) -> IoResult<()> {
        let mut inner = self.fs.inner.write().unwrap();
        inner.entries.retain(|path, _| !path.starts_with(&self.dir));
        Ok(())
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

/// The filesystem root (`/`, `C:\`) is implicit: it never needs an entry.
fn is_fs_root(p: &Path) -> bool {
    p.parent().is_none()
}

fn parent_exists(inner: &Inner, path: &Path) -> bool {
    let Some(parent) = path.parent() else {
        return true;
    };
    if is_fs_root(parent) {
        return true;
    }
    matches!(inner.entries.get(parent), Some(Entry::Dir))
}

fn create_dir(inner: &mut Inner, path: &Path) -> IoResult<()> {
    if inner.entries.contains_key(path) {
        return Err(IoError::from(ErrorKind::AlreadyExists));
    }
    if !parent_exists(inner, path) {
        return Err(IoError::from(ErrorKind::NotFound));
    }
    inner.entries.insert(path.to_path_buf(), Entry::Dir);
    Ok(())
}

fn create_dir_all(inner: &mut Inner, path: &Path) -> IoResult<()> {
    let mut current = PathBuf::new();
    for comp in path.components() {
        current.push(comp);
        match comp {
            Component::RootDir | Component::Prefix(_) => continue,
            _ => {}
        }
        match inner.entries.get(&current) {
            Some(Entry::Dir) => {}
            Some(Entry::File(..)) => return Err(IoError::from(ErrorKind::AlreadyExists)),
            None => {
                inner.entries.insert(current.clone(), Entry::Dir);
            }
        }
    }
    Ok(())
}

/// Root for glob/grep: the `path` option (tilde-expanded, cwd-joined when
/// relative), or the cwd.
fn resolve_root(path: Option<&str>) -> Result<PathBuf, FsError> {
    match path {
        Some(p) => {
            let expanded = super::expand_tilde(p);
            if expanded.is_absolute() {
                Ok(expanded)
            } else {
                Ok(std::env::current_dir().map_err(FsError::Io)?.join(expanded))
            }
        }
        None => std::env::current_dir().map_err(FsError::Io),
    }
}

impl FsBackend for InMemoryFs {
    fn read(&self, path: PathBuf) -> BoxFuture<'_, IoResult<String>> {
        Box::pin(async move {
            let inner = self.inner.read().unwrap();
            match inner.entries.get(&path) {
                Some(Entry::File(bytes, _)) => String::from_utf8(bytes.clone())
                    .map_err(|_| IoError::new(ErrorKind::InvalidData, "non-utf8 content")),
                Some(Entry::Dir) => Err(IoError::new(ErrorKind::IsADirectory, "is a directory")),
                None => Err(IoError::from(ErrorKind::NotFound)),
            }
        })
    }

    fn read_bytes(&self, path: PathBuf) -> BoxFuture<'_, IoResult<Vec<u8>>> {
        Box::pin(async move {
            let inner = self.inner.read().unwrap();
            match inner.entries.get(&path) {
                Some(Entry::File(bytes, _)) => Ok(bytes.clone()),
                Some(Entry::Dir) => Err(IoError::new(ErrorKind::IsADirectory, "is a directory")),
                None => Err(IoError::from(ErrorKind::NotFound)),
            }
        })
    }

    fn stat(&self, path: PathBuf) -> BoxFuture<'_, IoResult<FsMeta>> {
        Box::pin(async move {
            let inner = self.inner.read().unwrap();
            match inner.entries.get(&path) {
                Some(Entry::File(bytes, seq)) => Ok(FsMeta {
                    size: bytes.len() as u64,
                    is_file: true,
                    is_dir: false,
                    mtime: Some(UNIX_EPOCH + Duration::from_secs(*seq)),
                }),
                Some(Entry::Dir) => Ok(FsMeta {
                    size: 0,
                    is_file: false,
                    is_dir: true,
                    mtime: None,
                }),
                None => Err(IoError::from(ErrorKind::NotFound)),
            }
        })
    }

    fn write(&self, path: PathBuf, content: Vec<u8>) -> BoxFuture<'_, IoResult<()>> {
        Box::pin(async move {
            let mut inner = self.inner.write().unwrap();
            if matches!(inner.entries.get(&path), Some(Entry::Dir)) {
                return Err(IoError::new(ErrorKind::IsADirectory, "is a directory"));
            }
            if !parent_exists(&inner, &path) {
                return Err(IoError::from(ErrorKind::NotFound));
            }
            let seq = Self::bump_seq(&mut inner);
            inner.entries.insert(path, Entry::File(content, seq));
            Ok(())
        })
    }

    /// Same contract as `write` here: no temp file, no rename.
    fn atomic_write(&self, path: PathBuf, content: Vec<u8>) -> BoxFuture<'_, IoResult<()>> {
        Box::pin(async move { self.write(path, content).await })
    }

    fn rm(&self, path: PathBuf, recursive: bool, force: bool) -> BoxFuture<'_, IoResult<()>> {
        Box::pin(async move {
            let mut inner = self.inner.write().unwrap();
            let is_dir = match inner.entries.get(&path) {
                Some(Entry::Dir) => true,
                Some(Entry::File(..)) => false,
                None => {
                    if force {
                        return Ok(());
                    }
                    return Err(IoError::from(ErrorKind::NotFound));
                }
            };
            if is_dir {
                if !recursive
                    && inner
                        .entries
                        .keys()
                        .any(|p| p != &path && p.starts_with(&path))
                {
                    return Err(IoError::other("rm: directory not empty"));
                }
                inner
                    .entries
                    .retain(|p, _| p != &path && !p.starts_with(&path));
            } else {
                inner.entries.remove(&path);
            }
            Ok(())
        })
    }

    fn mkdir(&self, path: PathBuf, parents: bool) -> BoxFuture<'_, IoResult<()>> {
        Box::pin(async move {
            let mut inner = self.inner.write().unwrap();
            if parents {
                create_dir_all(&mut inner, &path)
            } else {
                create_dir(&mut inner, &path)
            }
        })
    }

    fn dir(
        &self,
        path: PathBuf,
        max_depth: u32,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, FsError>> {
        Box::pin(async move {
            let meta = match self.stat(path.clone()).await {
                Ok(m) => m,
                Err(_) => {
                    return Err(FsError::Message(format!(
                        "{DIR_MISSING_PREFIX}{}",
                        path.display()
                    )));
                }
            };
            if !meta.is_dir {
                return Err(FsError::Message(format!(
                    "{DIR_NOT_A_DIR_PREFIX}{}",
                    path.display()
                )));
            }
            let inner = self.inner.read().unwrap();
            let mut out = Vec::new();
            for (p, entry) in &inner.entries {
                let Some(rel) = p
                    .strip_prefix(&path)
                    .ok()
                    .filter(|r| !r.as_os_str().is_empty())
                else {
                    continue;
                };
                if rel.components().count() as u32 > max_depth {
                    continue;
                }
                let typ = match entry {
                    Entry::File(..) => TYPE_FILE,
                    Entry::Dir => TYPE_DIRECTORY,
                };
                out.push((rel.to_string_lossy().into_owned(), typ));
            }
            Ok(out)
        })
    }

    fn glob(
        &self,
        patterns: Vec<String>,
        path: Option<String>,
        limit: Option<usize>,
        _gitignore: bool,
        sort_mtime: bool,
    ) -> BoxFuture<'_, Result<Vec<String>, FsError>> {
        Box::pin(async move {
            let compiled: Vec<GlobMatcher> = patterns
                .iter()
                .map(|p| {
                    Glob::new(p)
                        .map(|g| g.compile_matcher())
                        .map_err(|e| FsError::Message(format!("invalid glob pattern: {e}")))
                })
                .collect::<Result<_, _>>()?;
            let root = resolve_root(path.as_deref())?;
            let inner = self.inner.read().unwrap();
            let mut hits: Vec<(u64, String)> = Vec::new();
            for (p, entry) in &inner.entries {
                let Entry::File(_, seq) = entry else {
                    continue;
                };
                let Some(rel) = p
                    .strip_prefix(&root)
                    .ok()
                    .filter(|r| !r.as_os_str().is_empty())
                else {
                    continue;
                };
                if compiled.iter().any(|g| g.is_match(rel)) {
                    hits.push((*seq, p.to_string_lossy().into_owned()));
                }
            }
            if sort_mtime {
                hits.sort_by_key(|h| Reverse(h.0));
            }
            if let Some(lim) = limit {
                hits.truncate(lim);
            }
            Ok(hits.into_iter().map(|(_, p)| p).collect())
        })
    }

    fn grep(
        &self,
        params: GrepParams,
    ) -> BoxFuture<'_, Result<(PathBuf, Vec<GrepFileEntry>), FsError>> {
        Box::pin(async move {
            let root = resolve_root(params.path.as_deref())?;
            let re = Regex::new(&params.pattern)
                .map_err(|e| FsError::Message(format!("invalid regex pattern: {e}")))?;
            let include = params
                .include
                .as_deref()
                .map(|p| Glob::new(p).map(|g| g.compile_matcher()))
                .transpose()
                .map_err(|e| FsError::Message(format!("invalid glob pattern: {e}")))?;

            let inner = self.inner.read().unwrap();
            let mut files: Vec<(u64, &PathBuf)> = inner
                .entries
                .iter()
                .filter_map(|(p, e)| match e {
                    Entry::File(_, seq) => Some((*seq, p)),
                    Entry::Dir => None,
                })
                .filter(|(_, p)| p.starts_with(&root))
                .collect();
            files.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(b.1)));

            let mut total_groups = 0;
            let mut out: Vec<GrepFileEntry> = Vec::new();
            for (_, file) in files {
                if total_groups >= params.limit {
                    break;
                }
                let Entry::File(bytes, _) = &inner.entries[file] else {
                    continue;
                };
                let Ok(content) = String::from_utf8(bytes.clone()) else {
                    continue;
                };
                let rel = file.strip_prefix(&root).unwrap_or(file);
                if let Some(g) = &include
                    && !g.is_match(rel)
                {
                    continue;
                }
                let lines: Vec<&str> = content.split('\n').collect();
                let mut groups: Vec<GrepMatchGroup> = Vec::new();
                for (idx, line) in lines.iter().enumerate() {
                    if line.len() > params.max_line_bytes || !re.is_match(line) {
                        continue;
                    }
                    let mut group_lines: Vec<GrepLine> = Vec::new();
                    for (i, &text) in lines
                        .iter()
                        .enumerate()
                        .take(idx)
                        .skip(idx.saturating_sub(params.context_before))
                    {
                        if text.len() <= params.max_line_bytes {
                            group_lines.push(GrepLine::context(i + 1, text));
                        }
                    }
                    group_lines.push(GrepLine::matched(idx + 1, *line));
                    for (i, &text) in lines
                        .iter()
                        .enumerate()
                        .skip(idx + 1)
                        .take(params.context_after)
                    {
                        if text.len() <= params.max_line_bytes {
                            group_lines.push(GrepLine::context(i + 1, text));
                        }
                    }
                    groups.push(GrepMatchGroup { lines: group_lines });
                    total_groups += 1;
                    if total_groups >= params.limit {
                        break;
                    }
                }
                if !groups.is_empty() {
                    out.push(GrepFileEntry {
                        path: rel.to_string_lossy().into_owned(),
                        groups,
                    });
                }
            }
            Ok((root, out))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;
    use std::path::PathBuf;

    use super::*;

    const ROOT: &str = "/t";

    fn seeded() -> InMemoryFs {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from(ROOT), true)).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/a.rs"), b"fn a()".to_vec())).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/b.txt"), b"hello".to_vec())).unwrap();
        fs
    }

    #[test]
    fn write_read_roundtrip_and_stat_fields() {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from(ROOT), true)).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/note.md"), b"hello-maki".to_vec())).unwrap();

        assert_eq!(
            smol::block_on(fs.read(PathBuf::from("/t/note.md"))).unwrap(),
            "hello-maki"
        );
        assert_eq!(
            smol::block_on(fs.read_bytes(PathBuf::from("/t/note.md"))).unwrap(),
            b"hello-maki"
        );

        let meta = smol::block_on(fs.stat(PathBuf::from("/t/note.md"))).unwrap();
        assert!(meta.is_file);
        assert!(!meta.is_dir);
        assert_eq!(meta.size, 10);
        assert!(meta.mtime.is_some(), "file mtime must be present");

        let dir_meta = smol::block_on(fs.stat(PathBuf::from(ROOT))).unwrap();
        assert!(dir_meta.is_dir);
        assert!(!dir_meta.is_file);
        assert_eq!(dir_meta.size, 0);
        assert!(dir_meta.mtime.is_none());
    }

    #[test]
    fn error_kinds_match_real_fs_contract() {
        let fs = seeded();

        assert_eq!(
            smol::block_on(fs.read(PathBuf::from("/t/missing")))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            smol::block_on(fs.read(PathBuf::from(ROOT)))
                .unwrap_err()
                .kind(),
            ErrorKind::IsADirectory
        );

        smol::block_on(fs.write(PathBuf::from("/t/bin.dat"), vec![0xff, 0xfe])).unwrap();
        assert_eq!(
            smol::block_on(fs.read(PathBuf::from("/t/bin.dat")))
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidData
        );
        assert_eq!(
            smol::block_on(fs.read_bytes(PathBuf::from("/t/bin.dat"))).unwrap(),
            vec![0xff, 0xfe]
        );

        assert_eq!(
            smol::block_on(fs.write(PathBuf::from(ROOT), b"x".to_vec()))
                .unwrap_err()
                .kind(),
            ErrorKind::IsADirectory
        );
        assert_eq!(
            smol::block_on(fs.write(PathBuf::from("/t/noparent/x.txt"), b"x".to_vec()))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
        assert_eq!(
            smol::block_on(fs.stat(PathBuf::from("/t/missing")))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn mkdir_existing_fails_and_parents_creates_chain() {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from(ROOT), true)).unwrap();
        assert_eq!(
            smol::block_on(fs.mkdir(PathBuf::from(ROOT), false))
                .unwrap_err()
                .kind(),
            ErrorKind::AlreadyExists
        );
        // `parents = true` is a no-op on an existing dir, like `create_dir_all`.
        smol::block_on(fs.mkdir(PathBuf::from(ROOT), true)).unwrap();

        let deep = PathBuf::from("/t/x/y/z");
        smol::block_on(fs.mkdir(deep.clone(), true)).unwrap();
        assert!(matches!(
            smol::block_on(fs.stat(deep)),
            Ok(m) if m.is_dir
        ));
        assert!(matches!(
            smol::block_on(fs.stat(PathBuf::from("/t/x/y"))),
            Ok(m) if m.is_dir
        ));

        assert_eq!(
            smol::block_on(fs.mkdir(PathBuf::from("/t/p/q"), false))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
    }

    #[test]
    fn rm_edges() {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from("/t/tree/nested"), true)).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/tree/a.txt"), b"a".to_vec())).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/tree/nested/b.txt"), b"b".to_vec())).unwrap();

        assert_eq!(
            smol::block_on(fs.rm(PathBuf::from("/t/tree"), false, false))
                .unwrap_err()
                .kind(),
            ErrorKind::Other
        );
        assert!(smol::block_on(fs.stat(PathBuf::from("/t/tree"))).is_ok());

        assert!(smol::block_on(fs.rm(PathBuf::from("/t/ghost"), false, true)).is_ok());
        assert_eq!(
            smol::block_on(fs.rm(PathBuf::from("/t/ghost"), false, false))
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );

        smol::block_on(fs.rm(PathBuf::from("/t/tree"), true, false)).unwrap();
        assert!(smol::block_on(fs.stat(PathBuf::from("/t/tree"))).is_err());
        assert!(smol::block_on(fs.stat(PathBuf::from("/t/tree/a.txt"))).is_err());
        assert!(smol::block_on(fs.stat(PathBuf::from("/t/tree/nested/b.txt"))).is_err());

        smol::block_on(fs.write(PathBuf::from("/t/solo.txt"), b"x".to_vec())).unwrap();
        smol::block_on(fs.rm(PathBuf::from("/t/solo.txt"), false, false)).unwrap();
        assert!(smol::block_on(fs.stat(PathBuf::from("/t/solo.txt"))).is_err());

        smol::block_on(fs.mkdir(PathBuf::from("/t/empty"), false)).unwrap();
        smol::block_on(fs.rm(PathBuf::from("/t/empty"), false, false)).unwrap();
        assert!(smol::block_on(fs.stat(PathBuf::from("/t/empty"))).is_err());
    }

    #[test]
    fn dir_recursive_lists_relative_names_and_types() {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from("/t/x"), true)).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/x/file.txt"), b"x".to_vec())).unwrap();
        smol::block_on(fs.mkdir(PathBuf::from("/t/y"), false)).unwrap();

        let entries = smol::block_on(fs.dir(PathBuf::from("/t"), 2)).unwrap();
        let mut names: Vec<&str> = entries.iter().map(|(n, _)| n.as_str()).collect();
        names.sort();
        assert_eq!(names, vec!["x", "x/file.txt", "y"]);
        let types: std::collections::HashMap<&str, &str> =
            entries.iter().map(|(n, t)| (n.as_str(), *t)).collect();
        assert_eq!(types["x"], TYPE_DIRECTORY);
        assert_eq!(types["x/file.txt"], TYPE_FILE);
        assert_eq!(types["y"], TYPE_DIRECTORY);

        let shallow = smol::block_on(fs.dir(PathBuf::from("/t"), 1)).unwrap();
        let shallow_names: Vec<&str> = shallow.iter().map(|(n, _)| n.as_str()).collect();
        assert!(shallow_names.contains(&"x"));
        assert!(
            !shallow_names.iter().any(|n| n.contains('/')),
            "depth 1 must not recurse"
        );
    }

    #[test]
    fn dir_missing_and_not_a_directory_messages() {
        let fs = seeded();

        let missing = smol::block_on(fs.dir(PathBuf::from("/t/nope"), 1))
            .unwrap_err()
            .to_string();
        assert!(missing.starts_with(DIR_MISSING_PREFIX), "{missing}");
        let not_dir = smol::block_on(fs.dir(PathBuf::from("/t/a.rs"), 1))
            .unwrap_err()
            .to_string();
        assert!(not_dir.starts_with(DIR_NOT_A_DIR_PREFIX), "{not_dir}");
    }

    #[test]
    fn glob_scope_limit_and_mtime_order() {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from("/t/sub"), true)).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/top.rs"), Vec::new())).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/sub/inner.rs"), Vec::new())).unwrap();

        // gitignore semantics (real backend delegates to the `ignore` crate):
        // a pattern without a slash matches at any depth under the scope.
        let shallow =
            smol::block_on(fs.glob(vec!["*.rs".into()], Some("/t".into()), None, true, false))
                .unwrap();
        let mut shallow: Vec<&str> = shallow.iter().map(String::as_str).collect();
        shallow.sort();
        assert_eq!(shallow, vec!["/t/sub/inner.rs", "/t/top.rs"]);

        let scoped = smol::block_on(fs.glob(
            vec!["*.rs".into()],
            Some("/t/sub".into()),
            None,
            true,
            false,
        ))
        .unwrap();
        assert_eq!(scoped, vec!["/t/sub/inner.rs"]);

        let deep =
            smol::block_on(fs.glob(vec!["**/*.rs".into()], Some("/t".into()), None, true, false))
                .unwrap();
        let mut deep: Vec<&str> = deep.iter().map(String::as_str).collect();
        deep.sort();
        assert_eq!(deep, vec!["/t/sub/inner.rs", "/t/top.rs"]);

        let limited = smol::block_on(fs.glob(
            vec!["**/*.rs".into()],
            Some("/t".into()),
            Some(1),
            true,
            false,
        ))
        .unwrap();
        assert_eq!(limited.len(), 1);

        // mtime sort is insertion order, newest first.
        let f = InMemoryFs::new();
        smol::block_on(f.mkdir(PathBuf::from(ROOT), true)).unwrap();
        smol::block_on(f.write(PathBuf::from("/t/one.rs"), Vec::new())).unwrap();
        smol::block_on(f.write(PathBuf::from("/t/two.rs"), Vec::new())).unwrap();
        smol::block_on(f.write(PathBuf::from("/t/three.rs"), Vec::new())).unwrap();
        let by_mtime =
            smol::block_on(f.glob(vec!["*.rs".into()], Some("/t".into()), None, true, true))
                .unwrap();
        assert_eq!(by_mtime, vec!["/t/three.rs", "/t/two.rs", "/t/one.rs"]);

        assert!(
            smol::block_on(f.glob(vec!["[".into()], Some("/t".into()), None, true, false,))
                .is_err(),
            "invalid pattern must surface as an error"
        );
    }

    #[test]
    fn grep_matches_with_context_include_and_limit() {
        let fs = InMemoryFs::new();
        smol::block_on(fs.mkdir(PathBuf::from(ROOT), true)).unwrap();
        let mut content = String::new();
        for i in 1..=5 {
            content.push_str(&format!("line_{i}\n"));
        }
        smol::block_on(fs.write(PathBuf::from("/t/data.rs"), content.into_bytes())).unwrap();
        smol::block_on(fs.write(PathBuf::from("/t/other.txt"), b"line_99\n".to_vec())).unwrap();

        let mut params = GrepParams::new("line_3".to_string());
        params.path = Some("/t".to_string());
        params.context_before = 1;
        params.context_after = 1;
        params.include = Some("*.rs".to_string());
        let (base, entries) = smol::block_on(fs.grep(params)).unwrap();
        assert_eq!(base, PathBuf::from("/t"));
        assert_eq!(entries.len(), 1, "include must exclude other.txt");
        assert_eq!(entries[0].path, "data.rs");
        assert_eq!(entries[0].groups.len(), 1);
        let lines = &entries[0].groups[0].lines;
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0].line_nr, 2);
        assert!(!lines[0].is_match);
        assert_eq!(lines[0].text, "line_2");
        assert_eq!(lines[1].line_nr, 3);
        assert!(lines[1].is_match);
        assert_eq!(lines[2].line_nr, 4);
        assert!(!lines[2].is_match);

        // Without the include filter both files hit.
        let mut params = GrepParams::new("line_".to_string());
        params.path = Some("/t".to_string());
        let (_, all) = smol::block_on(fs.grep(params)).unwrap();
        assert_eq!(all.len(), 2);

        // The limit caps total groups across files.
        let mut params = GrepParams::new("line_".to_string());
        params.path = Some("/t".to_string());
        params.include = Some("*.rs".to_string());
        params.limit = 2;
        let (_, capped) = smol::block_on(fs.grep(params)).unwrap();
        assert_eq!(capped[0].groups.len(), 2);

        let mut params = GrepParams::new("[".to_string());
        params.path = Some("/t".to_string());
        let err = smol::block_on(fs.grep(params)).unwrap_err().to_string();
        assert!(err.starts_with("invalid regex pattern"), "{err}");
    }

    #[test]
    fn files_lists_all_file_entries() {
        let fs = seeded();
        let mut files = fs.files();
        files.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, PathBuf::from("/t/a.rs"));
        assert_eq!(files[0].1, b"fn a()");
        assert_eq!(files[1].0, PathBuf::from("/t/b.txt"));
        assert_eq!(files[1].1, b"hello");
    }
}
