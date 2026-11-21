use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, ErrorKind, Write};
use std::path::{Component, Path, PathBuf, Prefix};

use makima_storage::windows_offload::{RelativeOpen, delete, open_relative, publish};

use super::DiskBackend;

pub(super) use makima_storage::windows_offload::{total_bytes, trusted_root};

pub(super) fn disk_root(path: &Path) -> (io::Result<File>, PathBuf) {
    let result = (|| {
        if path.is_relative() && !matches!(path.components().next(), Some(Component::Prefix(_))) {
            return Ok((trusted_root(Path::new("."))?, path.to_owned()));
        }
        let mut parts = path.components();
        let prefix = match parts.next() {
            Some(Component::Prefix(prefix)) => prefix,
            _ => {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "offload path needs a volume root",
                ));
            }
        };
        if !matches!(
            prefix.kind(),
            Prefix::Disk(_)
                | Prefix::VerbatimDisk(_)
                | Prefix::UNC(_, _)
                | Prefix::VerbatimUNC(_, _)
        ) || !matches!(parts.next(), Some(Component::RootDir))
        {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "offload path needs an absolute disk or UNC root",
            ));
        }
        let mut anchor = prefix.as_os_str().to_owned();
        anchor.push("\\");
        Ok((
            trusted_root(Path::new(&anchor))?,
            parts.as_path().to_owned(),
        ))
    })();
    match result {
        Ok((root, relative)) => (Ok(root), relative),
        Err(error) => (Err(error), PathBuf::new()),
    }
}

pub(super) fn open_read(dir: &File, name: &str) -> io::Result<File> {
    open_relative(dir, OsStr::new(name), RelativeOpen::ReadFile)
}

pub(super) fn walk_dir(root: &File, relative: &Path, create: bool) -> io::Result<File> {
    if relative
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "offload directory must contain only normal relative components",
        ));
    }
    let mut dir = root.try_clone()?;
    for part in relative.components() {
        let mode = if create {
            RelativeOpen::CreateDirectory
        } else {
            RelativeOpen::ReadDirectory
        };
        dir = open_relative(&dir, part.as_os_str(), mode)?;
    }
    Ok(dir)
}

struct Temporary {
    file: File,
    published: bool,
}

impl Drop for Temporary {
    fn drop(&mut self) {
        if !self.published {
            let _ = delete(&self.file);
        }
    }
}

pub(super) fn create_in(dir: &File, name: &str, bytes: &[u8]) -> io::Result<bool> {
    DiskBackend::validate_name(name)?;
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
    let temporary = format!(".offload-{:032x}", u128::from_ne_bytes(random));
    let mut temporary = Temporary {
        file: open_relative(dir, OsStr::new(&temporary), RelativeOpen::CreateTemporary)?,
        published: false,
    };
    temporary.file.write_all(bytes)?;
    temporary.published = publish(&temporary.file, dir, name)?;
    if !temporary.published {
        delete(&temporary.file)?;
    }
    Ok(temporary.published)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::windows::fs::symlink_dir;

    use makima_storage::id::{MakimaId, SessionRef};
    use makima_storage::remove_offload_dir_from;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::{
        RelativeOpen, create_in, open_read, open_relative, publish, total_bytes, walk_dir,
    };
    use crate::tools::offload::{DiskBackend, OffloadError, OffloadStore};
    use std::ffi::OsStr;
    use std::io::Write;
    use std::path::Path;

    const BODY: &[u8] = b"offload handle regression";
    const SLOT: &str = "slot";

    #[test_case("sessions"; "sessions")]
    #[test_case("sessions/offload"; "offload")]
    fn managed_parent_reparse_is_rejected(parent: &str) {
        let root = TempDir::new().unwrap();
        let session = SessionRef::from(MakimaId::generate());
        let relative = Path::new("sessions/offload").join(session.id().to_string());
        let external = root.path().join("outside");
        let external_leaf = external.join(relative.strip_prefix(parent).unwrap());
        fs::create_dir_all(&external_leaf).unwrap();
        fs::write(external_leaf.join(SLOT), BODY).unwrap();
        let link = root.path().join(parent);
        fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink_dir(&external, &link).unwrap();
        let store = OffloadStore::for_session(root.path(), &session);
        assert!(matches!(store.put("new output"), Err(OffloadError::Io(_))));
        assert!(store.close_and_remove().is_err());
        let arbitrary = OffloadStore::on_disk(root.path().join(&relative));
        assert!(matches!(
            arbitrary.put("new output"),
            Err(OffloadError::Io(_))
        ));
        assert_eq!(fs::read(external_leaf.join(SLOT)).unwrap(), BODY);
        assert_eq!(fs::read_dir(&external_leaf).unwrap().count(), 1);
    }

    #[test_case("sessions"; "sessions")]
    #[test_case("sessions/offload"; "offload")]
    fn ancestor_swap_after_open_keeps_all_operations_anchored(parent: &str) {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join(parent));
        let opened_parent = backend.open_dir(true).unwrap();
        let moved = root.path().join("original");
        fs::rename(&backend.dir, &moved).unwrap();
        let external = root.path().join("outside");
        fs::create_dir(&external).unwrap();
        symlink_dir(&external, &backend.dir).unwrap();
        let remaining = if parent == "sessions" {
            "offload/session"
        } else {
            "session"
        };
        let leaf = walk_dir(&opened_parent, Path::new(remaining), true).unwrap();
        assert!(create_in(&leaf, SLOT, BODY).unwrap());
        assert!(!create_in(&leaf, SLOT, b"replacement").unwrap());
        assert_eq!(total_bytes(&leaf).unwrap(), BODY.len() as u64);
        assert_eq!(fs::read(moved.join(remaining).join(SLOT)).unwrap(), BODY);
        assert!(backend.open_dir(true).is_err());
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
        drop(leaf);
        remove_offload_dir_from(&opened_parent, Path::new(remaining)).unwrap();
        assert!(!moved.join(remaining).exists());
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
    }

    #[test]
    fn exclusive_temporary_rejects_replacement_and_publishes_without_clobbering() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        let dir = backend.open_dir(true).unwrap();
        let mut original =
            open_relative(&dir, OsStr::new("temporary"), RelativeOpen::CreateTemporary).unwrap();
        original.write_all(BODY).unwrap();
        assert!(fs::rename(backend.dir.join("temporary"), backend.dir.join("blocked")).is_err());
        assert!(fs::write(backend.dir.join("temporary"), b"replacement").is_err());
        assert!(fs::remove_file(backend.dir.join("temporary")).is_err());
        fs::write(backend.dir.join("occupied"), b"existing").unwrap();
        assert!(!publish(&original, &dir, "occupied").unwrap());
        assert_eq!(fs::read(backend.dir.join("occupied")).unwrap(), b"existing");
        assert!(publish(&original, &dir, SLOT).unwrap());
        assert!(!backend.dir.join("temporary").exists());
        drop(original);
        assert_eq!(fs::read(backend.dir.join(SLOT)).unwrap(), BODY);
    }

    #[test]
    fn cleanup_unlinks_nested_reparse_without_following_target() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        let dir = backend.open_dir(true).unwrap();
        let external = root.path().join("outside");
        fs::create_dir(&external).unwrap();
        fs::write(external.join(SLOT), BODY).unwrap();
        let name = "résultat";
        assert!(create_in(&dir, name, BODY).unwrap());
        assert_eq!(
            open_read(&dir, name).unwrap().metadata().unwrap().len(),
            BODY.len() as u64,
        );
        fs::create_dir(backend.dir.join("nested")).unwrap();
        fs::write(backend.dir.join("nested").join(SLOT), BODY).unwrap();
        symlink_dir(&external, backend.dir.join("nested/link")).unwrap();
        assert_eq!(total_bytes(&dir).unwrap(), BODY.len() as u64);
        drop(dir);
        remove_offload_dir_from(backend.root.as_ref().unwrap(), &backend.relative).unwrap();
        assert!(!backend.dir.exists());
        assert_eq!(fs::read(external.join(SLOT)).unwrap(), BODY);
    }
}
