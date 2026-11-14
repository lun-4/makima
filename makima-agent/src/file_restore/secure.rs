use std::{io, path::Path};

#[cfg(unix)]
use rustix::{
    fs::{self as anchored, AtFlags, FileType, Mode, OFlags, Stat},
    io::Errno,
};
#[cfg(unix)]
use std::{
    ffi::{OsStr, OsString},
    fs::File,
    io::{Read, Write},
    os::unix::ffi::OsStrExt,
    path::Component,
    sync::{Mutex, MutexGuard},
};

#[cfg(unix)]
const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
    .union(OFlags::DIRECTORY)
    .union(OFlags::NOFOLLOW)
    .union(OFlags::CLOEXEC);
#[cfg(unix)]
const INVALID_PATH: &str = "Restore path must be absolute with only normal components";
#[cfg(unix)]
const DIRECTORY_CHANGED: &str = "Restore parent directory was replaced";
#[cfg(unix)]
const CONTENT_CHANGED: &str = "Restore destination changed since capture";
#[cfg(unix)]
const NOT_REGULAR: &str = "Restore destination is not a regular file";
#[cfg(unix)]
const LOCK_POISONED: &str = "Restore directory lock was poisoned";
#[cfg(unix)]
const TEMPORARY_CHANGED: &str = "Restore temporary file was replaced";
#[cfg(not(unix))]
const UNSUPPORTED: &str = "Secure file restoration is not supported on this platform";

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct AnchoredPath {
    parents: Vec<OsString>,
    name: OsString,
    directories: Mutex<Vec<File>>,
    captured_depth: usize,
}

#[cfg(unix)]
impl AnchoredPath {
    pub(crate) fn capture(path: &Path) -> io::Result<Self> {
        let mut parts = path.components();
        if parts.next() != Some(Component::RootDir)
            || path
                .as_os_str()
                .as_bytes()
                .split(|byte| *byte == b'/')
                .any(|part| part == b".")
            || path.as_os_str().as_bytes().ends_with(b"/")
        {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, INVALID_PATH));
        }
        let name = parts
            .next_back()
            .filter(|part| matches!(part, Component::Normal(_)))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, INVALID_PATH))?
            .as_os_str()
            .to_owned();
        let parents = parts
            .map(|part| match part {
                Component::Normal(name) => Ok(name.to_owned()),
                _ => Err(io::Error::new(io::ErrorKind::InvalidInput, INVALID_PATH)),
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut directories = vec![open_root()?];
        for name in &parents {
            match open_directory(&directories[directories.len() - 1], name) {
                Ok(directory) => directories.push(directory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => return Err(error),
            }
        }
        let captured = Self {
            parents,
            name,
            captured_depth: directories.len(),
            directories: Mutex::new(directories),
        };
        {
            let directories = captured.lock_directories()?;
            captured.revalidate(&directories)?;
            if directories.len() == captured.parents.len() + 1 {
                open_regular(&directories[directories.len() - 1], &captured.name)?;
            }
            captured.revalidate(&directories)?;
        }
        Ok(captured)
    }

    pub(crate) fn adopt_created_from(&self, other: &Self) -> io::Result<()> {
        let other_directories = other.lock_directories()?;
        other.revalidate_existing(&other_directories)?;
        let shared = self
            .parents
            .iter()
            .zip(&other.parents)
            .take_while(|(left, right)| left == right)
            .count();
        let available = other_directories
            .iter()
            .take(shared + 1)
            .map(File::try_clone)
            .collect::<io::Result<Vec<_>>>()?;
        drop(other_directories);
        let mut directories = self.lock_directories()?;
        self.revalidate_existing(&directories)?;
        while directories.len() < available.len() {
            if directories.len() < other.captured_depth {
                return Err(io::Error::other(DIRECTORY_CHANGED));
            }
            let parent = &directories[directories.len() - 1];
            let name = &self.parents[directories.len() - 1];
            let actual = open_directory(parent, name)?;
            require_same(&available[directories.len()], &actual)?;
            directories.push(actual);
        }
        self.revalidate_existing(&directories).map(|_| ())
    }

    pub(crate) fn read(&self) -> io::Result<Option<Vec<u8>>> {
        let directories = self.lock_directories()?;
        self.read_validated(&directories)
    }

    pub(crate) fn read_after_mutation(&self) -> io::Result<Option<Vec<u8>>> {
        let mut directories = self.lock_directories()?;
        self.revalidate_existing(&directories)?;
        while directories.len() <= self.parents.len() {
            let parent = &directories[directories.len() - 1];
            let name = &self.parents[directories.len() - 1];
            match open_directory(parent, name) {
                Ok(directory) => directories.push(directory),
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => return Err(error),
            }
        }
        self.read_validated(&directories)
    }

    pub(crate) fn replace(
        &self,
        expected: Option<&[u8]>,
        desired: Option<&[u8]>,
    ) -> io::Result<()> {
        let mut directories = self.lock_directories()?;
        self.revalidate(&directories)?;
        if directories.len() <= self.parents.len() {
            if expected.is_some() {
                return Err(io::Error::other(CONTENT_CHANGED));
            }
            if desired.is_none() {
                return Ok(());
            }
            while directories.len() <= self.parents.len() {
                self.revalidate(&directories)?;
                let parent = &directories[directories.len() - 1];
                let name = &self.parents[directories.len() - 1];
                anchored::mkdirat(parent, name, Mode::RWXU)?;
                let directory = open_directory(parent, name)?;
                let synced = parent.sync_all();
                directories.push(directory);
                synced?;
            }
        }
        let directory = &directories[directories.len() - 1];
        let validate = || {
            self.revalidate(&directories)?;
            if read_in(directory, &self.name)?.as_deref() != expected {
                return Err(io::Error::other(CONTENT_CHANGED));
            }
            self.revalidate(&directories)
        };
        match desired {
            Some(content) => write_in(directory, &self.name, content, validate),
            None => {
                validate()?;
                if expected.is_some() {
                    anchored::unlinkat(directory, &self.name, AtFlags::empty())?;
                    directory.sync_all()?;
                }
                Ok(())
            }
        }
    }

    fn read_validated(&self, directories: &[File]) -> io::Result<Option<Vec<u8>>> {
        self.revalidate(directories)?;
        let bytes = if directories.len() == self.parents.len() + 1 {
            read_in(&directories[directories.len() - 1], &self.name)?
        } else {
            None
        };
        self.revalidate(directories)?;
        Ok(bytes)
    }

    fn lock_directories(&self) -> io::Result<MutexGuard<'_, Vec<File>>> {
        self.directories
            .lock()
            .map_err(|_| io::Error::other(LOCK_POISONED))
    }

    fn revalidate_existing(&self, directories: &[File]) -> io::Result<File> {
        let mut directory = open_root()?;
        require_same(&directories[0], &directory)?;
        for (name, expected) in self.parents.iter().zip(&directories[1..]) {
            directory = open_directory(&directory, name)?;
            require_same(expected, &directory)?;
        }
        Ok(directory)
    }

    fn revalidate(&self, directories: &[File]) -> io::Result<()> {
        let directory = self.revalidate_existing(directories)?;
        if let Some(name) = self.parents.get(directories.len() - 1) {
            match anchored::statat(&directory, name, AtFlags::SYMLINK_NOFOLLOW) {
                Err(error) if error == Errno::NOENT => {}
                Err(error) => return Err(error.into()),
                Ok(_) => return Err(io::Error::other(DIRECTORY_CHANGED)),
            }
        }
        Ok(())
    }
}

#[cfg(unix)]
fn open_root() -> io::Result<File> {
    Ok(File::from(anchored::open(
        "/",
        DIRECTORY_FLAGS,
        Mode::empty(),
    )?))
}

#[cfg(unix)]
fn open_directory(parent: &File, name: &OsStr) -> io::Result<File> {
    Ok(File::from(anchored::openat(
        parent,
        name,
        DIRECTORY_FLAGS,
        Mode::empty(),
    )?))
}

#[cfg(unix)]
fn same_identity(expected: &Stat, actual: &Stat) -> bool {
    expected.st_dev == actual.st_dev && expected.st_ino == actual.st_ino
}

#[cfg(unix)]
fn require_same(expected: &File, actual: &File) -> io::Result<()> {
    if !same_identity(&anchored::fstat(expected)?, &anchored::fstat(actual)?) {
        return Err(io::Error::other(DIRECTORY_CHANGED));
    }
    Ok(())
}

#[cfg(unix)]
fn open_regular(directory: &File, name: &OsStr) -> io::Result<Option<File>> {
    let file = match anchored::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(file) => File::from(file),
        Err(error) if error == Errno::NOENT => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let metadata = anchored::fstat(&file)?;
    if FileType::from_raw_mode(metadata.st_mode) != FileType::RegularFile || metadata.st_nlink != 1
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, NOT_REGULAR));
    }
    Ok(Some(file))
}

#[cfg(unix)]
fn read_in(directory: &File, name: &OsStr) -> io::Result<Option<Vec<u8>>> {
    let Some(mut file) = open_regular(directory, name)? else {
        return Ok(None);
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let current = anchored::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW)?;
    if !same_identity(&anchored::fstat(&file)?, &current) {
        return Err(io::Error::other(CONTENT_CHANGED));
    }
    Ok(Some(bytes))
}

#[cfg(unix)]
fn write_in(
    directory: &File,
    name: &OsStr,
    content: &[u8],
    validate: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
    let temporary = format!(".restore-{:032x}", u128::from_ne_bytes(random));
    let mut file = File::from(anchored::openat(
        directory,
        temporary.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let result: io::Result<()> = (|| {
        file.write_all(content)?;
        file.sync_all()?;
        let current = anchored::statat(directory, temporary.as_str(), AtFlags::SYMLINK_NOFOLLOW)?;
        if !same_identity(&anchored::fstat(&file)?, &current) {
            return Err(io::Error::other(TEMPORARY_CHANGED));
        }
        validate()?;
        anchored::renameat(directory, temporary.as_str(), directory, name)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = anchored::unlinkat(directory, temporary.as_str(), AtFlags::empty());
    }
    result?;
    directory.sync_all()
}

#[cfg(not(unix))]
#[derive(Debug)]
pub(crate) struct AnchoredPath;

#[cfg(not(unix))]
impl AnchoredPath {
    pub(crate) fn capture(_: &Path) -> io::Result<Self> {
        Err(io::Error::new(io::ErrorKind::Unsupported, UNSUPPORTED))
    }

    pub(crate) fn read(&self) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, UNSUPPORTED))
    }

    pub(crate) fn read_after_mutation(&self) -> io::Result<Option<Vec<u8>>> {
        Err(io::Error::new(io::ErrorKind::Unsupported, UNSUPPORTED))
    }

    pub(crate) fn adopt_created_from(&self, _: &Self) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, UNSUPPORTED))
    }

    pub(crate) fn replace(&self, _: Option<&[u8]>, _: Option<&[u8]>) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, UNSUPPORTED))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{AnchoredPath, CONTENT_CHANGED, DIRECTORY_CHANGED, INVALID_PATH};
    use rustix::fs::{CWD, Mode, mkfifoat};
    use std::{fs, io, os::unix::fs::symlink, path::Path};
    use test_case::test_case;

    const BEFORE: &[u8] = b"captured bytes\0\xff";
    const AFTER: &[u8] = b"restored bytes";
    const USER_EDIT: &[u8] = b"user edit";

    #[test]
    fn absent_create_replace_delete_roundtrip() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("missing/nested/file");
        let captured = AnchoredPath::capture(&path).unwrap();
        assert_eq!(captured.read().unwrap(), None);
        captured.replace(None, None).unwrap();
        assert!(!root.join("missing").exists());
        captured.replace(None, Some(BEFORE)).unwrap();
        assert_eq!(captured.read().unwrap().as_deref(), Some(BEFORE));
        captured.replace(Some(BEFORE), Some(AFTER)).unwrap();
        assert_eq!(fs::read(&path).unwrap(), AFTER);
        captured.replace(Some(AFTER), None).unwrap();
        assert_eq!(captured.read().unwrap(), None);
        assert!(path.parent().unwrap().is_dir());
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 0);
    }

    #[test]
    fn sibling_anchors_adopt_only_directories_created_by_restore() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let first = AnchoredPath::capture(&root.join("new/a")).unwrap();
        let second = AnchoredPath::capture(&root.join("new/b")).unwrap();
        first.replace(None, Some(BEFORE)).unwrap();
        second.adopt_created_from(&first).unwrap();
        second.replace(None, Some(AFTER)).unwrap();
        assert_eq!(first.read().unwrap().as_deref(), Some(BEFORE));
        assert_eq!(second.read().unwrap().as_deref(), Some(AFTER));
        fs::rename(root.join("new"), root.join("moved")).unwrap();
        fs::create_dir(root.join("new")).unwrap();
        assert!(second.adopt_created_from(&first).is_err());
        assert_eq!(fs::read(root.join("moved/a")).unwrap(), BEFORE);
        assert_eq!(fs::read(root.join("moved/b")).unwrap(), AFTER);
    }

    #[test]
    fn sibling_anchors_reject_unrelated_new_parent_directory() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let first = AnchoredPath::capture(&root.join("new/a")).unwrap();
        let second = AnchoredPath::capture(&root.join("new/b")).unwrap();
        fs::create_dir(root.join("new")).unwrap();
        second.adopt_created_from(&first).unwrap();
        assert!(first.replace(None, Some(BEFORE)).is_err());
        assert!(second.replace(None, Some(AFTER)).is_err());
        assert!(!root.join("new/a").exists());
        assert!(!root.join("new/b").exists());
    }

    #[test]
    fn post_mutation_read_adopts_tool_created_parents() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("missing/nested/file");
        let captured = AnchoredPath::capture(&path).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, AFTER).unwrap();
        assert_eq!(captured.read().unwrap_err().to_string(), DIRECTORY_CHANGED);
        assert_eq!(
            captured.read_after_mutation().unwrap().as_deref(),
            Some(AFTER)
        );
        assert_eq!(captured.read().unwrap().as_deref(), Some(AFTER));
        captured.replace(Some(AFTER), None).unwrap();
        assert_eq!(captured.read().unwrap(), None);
        assert!(path.parent().unwrap().is_dir());
        fs::rename(root.join("missing"), root.join("moved")).unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        assert_eq!(
            captured.read_after_mutation().unwrap_err().to_string(),
            DIRECTORY_CHANGED
        );
    }

    #[test]
    fn post_mutation_read_adopts_partial_missing_tail_without_creating_directories() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("missing/nested/file");
        let captured = AnchoredPath::capture(&path).unwrap();
        assert_eq!(captured.read_after_mutation().unwrap(), None);
        assert!(!root.join("missing").exists());
        fs::create_dir(root.join("missing")).unwrap();
        assert_eq!(captured.read_after_mutation().unwrap(), None);
        assert!(!path.parent().unwrap().exists());
        assert_eq!(captured.read().unwrap(), None);
        fs::create_dir(path.parent().unwrap()).unwrap();
        fs::write(&path, AFTER).unwrap();
        assert_eq!(
            captured.read_after_mutation().unwrap().as_deref(),
            Some(AFTER)
        );
    }

    #[test_case(false; "symlink")]
    #[test_case(true; "real_directory")]
    fn post_mutation_read_rejects_existing_ancestor_substitution(real_directory: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let ancestor = root.join("ancestor");
        let moved = root.join("moved");
        let path = ancestor.join("parent/missing/file");
        fs::create_dir_all(ancestor.join("parent")).unwrap();
        let captured = AnchoredPath::capture(&path).unwrap();
        fs::rename(&ancestor, &moved).unwrap();
        if real_directory {
            fs::create_dir(&ancestor).unwrap();
            fs::rename(moved.join("parent"), ancestor.join("parent")).unwrap();
        } else {
            symlink(&moved, &ancestor).unwrap();
        }
        fs::create_dir(path.parent().unwrap()).unwrap();
        fs::write(&path, USER_EDIT).unwrap();
        assert!(captured.read_after_mutation().is_err());
        assert!(captured.replace(Some(USER_EDIT), None).is_err());
        assert_eq!(fs::read(&path).unwrap(), USER_EDIT);
    }

    #[test_case(false; "parent_symlink")]
    #[test_case(true; "final_symlink")]
    fn post_mutation_read_rejects_symlink_adoption(final_symlink: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("missing/file");
        let captured = AnchoredPath::capture(&path).unwrap();
        let target = root.join("target");
        fs::create_dir(&target).unwrap();
        fs::write(target.join("file"), USER_EDIT).unwrap();
        if final_symlink {
            fs::create_dir(path.parent().unwrap()).unwrap();
            symlink(target.join("file"), &path).unwrap();
        } else {
            symlink(&target, path.parent().unwrap()).unwrap();
        }
        assert!(captured.read_after_mutation().is_err());
        assert_eq!(fs::read(target.join("file")).unwrap(), USER_EDIT);
    }

    #[test_case(false; "in_place_write")]
    #[test_case(true; "atomic_replace")]
    fn read_observes_fresh_regular_file_contents(atomic_replace: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("file");
        fs::write(&path, BEFORE).unwrap();
        let captured = AnchoredPath::capture(&path).unwrap();
        assert_eq!(captured.read().unwrap().as_deref(), Some(BEFORE));
        if atomic_replace {
            let replacement = root.join("replacement");
            fs::write(&replacement, USER_EDIT).unwrap();
            fs::rename(replacement, &path).unwrap();
        } else {
            fs::write(&path, USER_EDIT).unwrap();
        }
        assert_eq!(captured.read().unwrap().as_deref(), Some(USER_EDIT));
        assert_eq!(
            captured.read_after_mutation().unwrap().as_deref(),
            Some(USER_EDIT)
        );
        captured.replace(Some(USER_EDIT), Some(AFTER)).unwrap();
        assert_eq!(captured.read().unwrap().as_deref(), Some(AFTER));
    }

    #[test_case(false; "overwrite")]
    #[test_case(true; "delete")]
    fn user_edit_fails_compare_and_swap(delete: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("file");
        fs::write(&path, BEFORE).unwrap();
        let captured = AnchoredPath::capture(&path).unwrap();
        fs::write(&path, USER_EDIT).unwrap();
        let desired = if delete { None } else { Some(AFTER) };
        let error = captured.replace(Some(BEFORE), desired).unwrap_err();
        assert_eq!(error.to_string(), CONTENT_CHANGED);
        assert_eq!(fs::read(&path).unwrap(), USER_EDIT);
        assert_eq!(fs::read_dir(path.parent().unwrap()).unwrap().count(), 1);
    }

    #[test]
    fn absence_is_compared_in_both_directions() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("file");
        let captured = AnchoredPath::capture(&path).unwrap();
        fs::write(&path, USER_EDIT).unwrap();
        assert_eq!(
            captured.replace(None, Some(AFTER)).unwrap_err().to_string(),
            CONTENT_CHANGED
        );
        fs::remove_file(&path).unwrap();
        assert_eq!(
            captured
                .replace(Some(BEFORE), None)
                .unwrap_err()
                .to_string(),
            CONTENT_CHANGED
        );
        assert!(!path.exists());
    }

    #[test_case(false; "symlink")]
    #[test_case(true; "directory")]
    fn final_substitution_is_rejected(directory: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("file");
        let target = root.join("target");
        fs::write(&path, BEFORE).unwrap();
        fs::write(&target, USER_EDIT).unwrap();
        let captured = AnchoredPath::capture(&path).unwrap();
        fs::remove_file(&path).unwrap();
        if directory {
            fs::create_dir(&path).unwrap();
        } else {
            symlink(&target, &path).unwrap();
        }
        assert!(AnchoredPath::capture(&path).is_err());
        assert!(captured.read().is_err());
        assert!(captured.replace(Some(BEFORE), Some(AFTER)).is_err());
        assert!(captured.replace(Some(BEFORE), None).is_err());
        assert_eq!(fs::read(&target).unwrap(), USER_EDIT);
        assert!(fs::symlink_metadata(&path).is_ok());
    }

    #[test_case(false; "symlink")]
    #[test_case(true; "real_directory")]
    fn parent_substitution_is_rejected(real_directory: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let parent = root.join("parent");
        let moved = root.join("moved");
        fs::create_dir(&parent).unwrap();
        fs::write(parent.join("file"), BEFORE).unwrap();
        let captured = AnchoredPath::capture(&parent.join("file")).unwrap();
        fs::rename(&parent, &moved).unwrap();
        if real_directory {
            fs::create_dir(&parent).unwrap();
            fs::write(parent.join("file"), BEFORE).unwrap();
        } else {
            symlink(&moved, &parent).unwrap();
            assert!(AnchoredPath::capture(&parent.join("file")).is_err());
        }
        assert!(captured.read().is_err());
        assert!(captured.replace(Some(BEFORE), Some(AFTER)).is_err());
        assert_eq!(fs::read(moved.join("file")).unwrap(), BEFORE);
        assert_eq!(fs::read(parent.join("file")).unwrap(), BEFORE);
    }

    #[test]
    fn ancestor_substitution_with_same_leaf_directory_is_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let ancestor = root.join("ancestor");
        let moved = root.join("moved");
        let path = ancestor.join("parent/file");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, BEFORE).unwrap();
        let captured = AnchoredPath::capture(&path).unwrap();
        fs::rename(&ancestor, &moved).unwrap();
        fs::create_dir(&ancestor).unwrap();
        fs::rename(moved.join("parent"), ancestor.join("parent")).unwrap();
        assert_eq!(captured.read().unwrap_err().to_string(), DIRECTORY_CHANGED);
        assert!(captured.replace(Some(BEFORE), Some(AFTER)).is_err());
        assert_eq!(fs::read(&path).unwrap(), BEFORE);
    }

    #[test_case(false; "symlink")]
    #[test_case(true; "real_directory")]
    fn missing_parent_substitution_is_rejected(real_directory: bool) {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("missing/file");
        let captured = AnchoredPath::capture(&path).unwrap();
        if real_directory {
            fs::create_dir(root.join("missing")).unwrap();
        } else {
            symlink(&root, root.join("missing")).unwrap();
        }
        assert_eq!(captured.read().unwrap_err().to_string(), DIRECTORY_CHANGED);
        assert!(captured.replace(None, Some(AFTER)).is_err());
        assert!(!path.exists());
        assert!(!root.join("file").exists());
    }

    #[test]
    fn created_parent_identity_is_retained() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().canonicalize().unwrap();
        let path = root.join("missing/file");
        let captured = AnchoredPath::capture(&path).unwrap();
        captured.replace(None, Some(BEFORE)).unwrap();
        fs::rename(root.join("missing"), root.join("moved")).unwrap();
        fs::create_dir(root.join("missing")).unwrap();
        assert!(captured.read().is_err());
        assert!(captured.replace(Some(BEFORE), None).is_err());
        assert_eq!(fs::read(root.join("moved/file")).unwrap(), BEFORE);
    }

    #[test]
    fn fifo_is_rejected_without_waiting_for_writer() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().canonicalize().unwrap().join("fifo");
        let captured = AnchoredPath::capture(&path).unwrap();
        mkfifoat(CWD, &path, Mode::RWXU).unwrap();
        let error = AnchoredPath::capture(&path).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(captured.read().is_err());
        assert!(captured.replace(None, Some(AFTER)).is_err());
    }

    #[test_case("relative/file"; "relative")]
    #[test_case("/"; "root")]
    #[test_case("/tmp/../file"; "parent_traversal")]
    #[test_case("/tmp/./file"; "current_directory")]
    #[test_case("/tmp/file/"; "trailing_separator")]
    fn invalid_paths_are_rejected(path: &str) {
        let error = AnchoredPath::capture(Path::new(path)).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), INVALID_PATH);
    }
}
