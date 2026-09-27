use std::cmp::Reverse;
use std::collections::HashSet;
use std::fs::FileType;
use std::io::{Error as IoError, ErrorKind};
use std::path::{Path, PathBuf};

use maki_agent::GrepFileEntry;
use maki_agent::tools::grep::GrepParams;

use super::{
    BoxFuture, DIR_MISSING_PREFIX, DIR_NOT_A_DIR_PREFIX, FsBackend, FsError, FsMeta,
    TYPE_DIRECTORY, TYPE_FILE, TYPE_LINK, TYPE_UNKNOWN,
};

/// Production backend: the exact semantics `maki.fs` always had — symlinks,
/// gitignore, permissions, real mtimes.
pub struct RealFs;

impl FsBackend for RealFs {
    fn read(&self, path: PathBuf) -> BoxFuture<'_, std::io::Result<String>> {
        Box::pin(async move { smol::unblock(move || std::fs::read_to_string(&path)).await })
    }

    fn read_bytes(&self, path: PathBuf) -> BoxFuture<'_, std::io::Result<Vec<u8>>> {
        Box::pin(async move { smol::unblock(move || std::fs::read(&path)).await })
    }

    /// Follows symlinks, like `smol::fs::metadata` did. Directory fields are
    /// preserved verbatim (`len()`, `modified().ok()`), not simplified.
    fn stat(&self, path: PathBuf) -> BoxFuture<'_, std::io::Result<FsMeta>> {
        Box::pin(async move {
            smol::unblock(move || {
                let meta = std::fs::metadata(&path)?;
                Ok(FsMeta {
                    size: meta.len(),
                    is_file: meta.is_file(),
                    is_dir: meta.is_dir(),
                    mtime: meta.modified().ok(),
                })
            })
            .await
        })
    }

    fn write(&self, path: PathBuf, content: Vec<u8>) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move { smol::unblock(move || std::fs::write(&path, &content)).await })
    }

    fn atomic_write(&self, path: PathBuf, content: Vec<u8>) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move {
            smol::unblock(move || match maki_storage::atomic_write(&path, &content) {
                Ok(()) => Ok(()),
                Err(maki_storage::StorageError::Io(e)) => Err(e),
                Err(e) => Err(IoError::other(e)),
            })
            .await
        })
    }

    fn rm(
        &self,
        path: PathBuf,
        recursive: bool,
        force: bool,
    ) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move {
            smol::unblock(move || {
                let meta = match std::fs::symlink_metadata(&path) {
                    Ok(m) => m,
                    Err(e) if force && e.kind() == ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e),
                };
                if meta.is_dir() {
                    if recursive {
                        std::fs::remove_dir_all(&path)
                    } else {
                        std::fs::remove_dir(&path)
                    }
                } else {
                    match std::fs::remove_file(&path) {
                        Ok(()) => Ok(()),
                        Err(e) if meta.file_type().is_symlink() => {
                            std::fs::remove_dir(&path).map_err(|_| e)
                        }
                        Err(e) => Err(e),
                    }
                }
            })
            .await
        })
    }

    fn mkdir(&self, path: PathBuf, parents: bool) -> BoxFuture<'_, std::io::Result<()>> {
        Box::pin(async move {
            smol::unblock(move || {
                if parents {
                    std::fs::create_dir_all(&path)
                } else {
                    std::fs::create_dir(&path)
                }
            })
            .await
        })
    }

    fn dir(
        &self,
        path: PathBuf,
        max_depth: u32,
    ) -> BoxFuture<'_, Result<Vec<(String, &'static str)>, FsError>> {
        Box::pin(async move {
            smol::unblock(move || {
                if !path.exists() {
                    return Err(FsError::Message(format!(
                        "{DIR_MISSING_PREFIX}{}",
                        path.display()
                    )));
                }
                if !path.is_dir() {
                    return Err(FsError::Message(format!(
                        "{DIR_NOT_A_DIR_PREFIX}{}",
                        path.display()
                    )));
                }
                let mut out = Vec::new();
                let mut visited = HashSet::new();
                collect_dir_entries(&path, &path, 1, max_depth, &mut visited, &mut out);
                Ok(out)
            })
            .await
        })
    }

    fn glob(
        &self,
        patterns: Vec<String>,
        path: Option<String>,
        limit: Option<usize>,
        gitignore: bool,
        sort_mtime: bool,
    ) -> BoxFuture<'_, Result<Vec<String>, FsError>> {
        Box::pin(async move {
            smol::unblock(move || {
                let root = maki_agent::tools::resolve_search_path(path.as_deref())
                    .map_err(FsError::Message)?;
                let pattern_refs: Vec<&str> = patterns.iter().map(|s| s.as_str()).collect();

                let walker = maki_agent::tools::walk_builder_opts(&root, &pattern_refs, gitignore)
                    .map_err(FsError::Message)?
                    .build();

                let iter = walker
                    .flatten()
                    .filter(|e| e.file_type().is_some_and(|ft| ft.is_file()));

                let paths: Vec<String> = if sort_mtime {
                    let mut entries: Vec<_> = iter
                        .filter_map(|e| {
                            let p = e.into_path();
                            let mt = maki_agent::tools::mtime(&p);
                            p.to_str().map(|s| (mt, s.to_owned()))
                        })
                        .collect();
                    entries.sort_unstable_by_key(|e| Reverse(e.0));
                    if let Some(lim) = limit {
                        entries.truncate(lim);
                    }
                    entries.into_iter().map(|(_, s)| s).collect()
                } else {
                    let bounded: Box<dyn Iterator<Item = _>> = match limit {
                        Some(lim) => Box::new(iter.take(lim)),
                        None => Box::new(iter),
                    };
                    bounded
                        .filter_map(|e| e.into_path().to_str().map(|s| s.to_owned()))
                        .collect()
                };

                Ok(paths)
            })
            .await
        })
    }

    fn grep(
        &self,
        params: GrepParams,
    ) -> BoxFuture<'_, Result<(PathBuf, Vec<GrepFileEntry>), FsError>> {
        Box::pin(async move {
            smol::unblock(move || {
                maki_agent::tools::grep::grep_search(params).map_err(FsError::Message)
            })
            .await
        })
    }
}

fn collect_dir_entries(
    base: &Path,
    dir: &Path,
    depth: u32,
    max_depth: u32,
    visited: &mut HashSet<PathBuf>,
    out: &mut Vec<(String, &'static str)>,
) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = match path.strip_prefix(base).ok().and_then(|p| p.to_str()) {
            Some(s) => s.to_owned(),
            None => continue,
        };
        let (type_str, is_dir) = match entry.file_type() {
            Ok(ft) if ft.is_symlink() => match std::fs::metadata(&path) {
                Ok(meta) => (filetype_str(&meta.file_type()), meta.is_dir()),
                Err(_) => (TYPE_LINK, false),
            },
            Ok(ft) => (filetype_str(&ft), ft.is_dir()),
            Err(_) => (TYPE_UNKNOWN, false),
        };
        out.push((name, type_str));
        if is_dir && depth < max_depth {
            let canonical = match path.canonicalize() {
                Ok(c) => c,
                Err(_) => continue,
            };
            if visited.insert(canonical) {
                collect_dir_entries(base, &path, depth + 1, max_depth, visited, out);
            }
        }
    }
}

fn filetype_str(ft: &FileType) -> &'static str {
    if ft.is_file() {
        TYPE_FILE
    } else if ft.is_dir() {
        TYPE_DIRECTORY
    } else if ft.is_symlink() {
        TYPE_LINK
    } else {
        TYPE_UNKNOWN
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Pins the dir-metadata fidelity the `RealFs` lift must keep: directory
    /// `size`/`mtime` are the real `len()`/`modified()`, not a `(0, None)`
    /// simplification.
    #[test]
    fn stat_reports_dir_metadata_verbatim() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("d");
        std::fs::create_dir(&dir).unwrap();
        std::fs::write(dir.join("f.txt"), "12345").unwrap();

        let dir_meta = smol::block_on(RealFs.stat(dir.clone())).unwrap();
        assert!(dir_meta.is_dir);
        assert!(!dir_meta.is_file);
        assert_eq!(dir_meta.size, std::fs::metadata(&dir).unwrap().len());
        assert!(dir_meta.mtime.is_some(), "dir mtime must be preserved");

        let file_meta = smol::block_on(RealFs.stat(dir.join("f.txt"))).unwrap();
        assert!(file_meta.is_file);
        assert_eq!(file_meta.size, 5);
        assert!(file_meta.mtime.is_some());
    }
}
