use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind, Write};
use std::mem::{offset_of, size_of};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Component, Path, PathBuf, Prefix};
use std::ptr::{copy_nonoverlapping, null, null_mut};
use std::slice;

use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows_sys::Wdk::Storage::FileSystem::{
    FILE_CREATE, FILE_DIRECTORY_FILE, FILE_DISPOSITION_INFORMATION, FILE_NON_DIRECTORY_FILE,
    FILE_OPEN, FILE_OPEN_IF, FILE_OPEN_REPARSE_POINT, FILE_RENAME_INFORMATION,
    FILE_SYNCHRONOUS_IO_NONALERT, FileDispositionInformation, FileRenameInformation, NtCreateFile,
    NtSetInformationFile,
};
use windows_sys::Win32::Foundation::{
    ERROR_NO_MORE_FILES, ERROR_REPARSE_POINT_ENCOUNTERED, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE,
    RtlNtStatusToDosError, UNICODE_STRING,
};
use windows_sys::Win32::Storage::FileSystem::{
    DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_READ, FILE_GENERIC_WRITE, FILE_ID_BOTH_DIR_INFO,
    FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdBothDirectoryInfo,
    FileIdBothDirectoryRestartInfo, GetFileInformationByHandleEx,
};
use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

use super::{DiskBackend, OffloadSnapshot};

const DIRECTORY_BUFFER_BYTES: usize = 64 * 1024;

pub(super) fn trusted_root(path: &Path) -> io::Result<File> {
    let root = OpenOptions::new()
        .read(true)
        .access_mode(FILE_GENERIC_READ)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    if !root.metadata()?.is_dir() {
        return Err(io::Error::new(
            ErrorKind::NotADirectory,
            "offload root must be a directory",
        ));
    }
    Ok(root)
}

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

fn open_relative(
    dir: &File,
    name: &OsStr,
    directory: Option<bool>,
    create: bool,
    writable: bool,
    reparse: bool,
) -> io::Result<File> {
    let mut wide: Vec<u16> = name.encode_wide().collect();
    if wide.is_empty()
        || wide.iter().any(|value| matches!(*value, 0 | 47 | 92 | 58))
        || name == "."
        || name == ".."
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "offload name must be a normal component",
        ));
    }
    let length = u16::try_from(wide.len() * size_of::<u16>())
        .map_err(|_| io::Error::new(ErrorKind::InvalidInput, "offload name is too long"))?;
    let unicode = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: wide.as_mut_ptr(),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: dir.as_raw_handle(),
        ObjectName: &unicode,
        Attributes: OBJ_CASE_INSENSITIVE | if reparse { 0 } else { OBJ_DONT_REPARSE },
        SecurityDescriptor: null(),
        SecurityQualityOfService: null(),
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut handle = null_mut();
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            FILE_GENERIC_READ
                | if writable {
                    DELETE
                        | if directory == Some(false) {
                            FILE_GENERIC_WRITE
                        } else {
                            0
                        }
                } else {
                    0
                },
            &attributes,
            &mut status_block,
            null(),
            FILE_ATTRIBUTE_NORMAL,
            if create && directory == Some(false) {
                0
            } else {
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE
            },
            if create {
                if directory == Some(true) {
                    FILE_OPEN_IF
                } else {
                    FILE_CREATE
                }
            } else {
                FILE_OPEN
            },
            FILE_SYNCHRONOUS_IO_NONALERT
                | FILE_OPEN_REPARSE_POINT
                | match directory {
                    Some(true) => FILE_DIRECTORY_FILE,
                    Some(false) => FILE_NON_DIRECTORY_FILE,
                    None => 0,
                },
            null(),
            0,
        )
    };
    if status < 0 {
        return Err(nt_error(status));
    }
    let file = unsafe { File::from_raw_handle(handle) };
    if !reparse && file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::from_raw_os_error(
            ERROR_REPARSE_POINT_ENCOUNTERED as i32,
        ));
    }
    Ok(file)
}

fn nt_error(status: i32) -> io::Error {
    io::Error::from_raw_os_error(unsafe { RtlNtStatusToDosError(status) } as i32)
}

pub(super) fn walk_dir(root: &File, relative: &Path, create: bool) -> io::Result<File> {
    walk(root, relative, create, false)
}

pub(super) fn walk_deletable_dir(root: &File, relative: &Path) -> io::Result<File> {
    walk(root, relative, false, true)
}

fn walk(root: &File, relative: &Path, create: bool, deletable: bool) -> io::Result<File> {
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
    let mut parts = relative.components().peekable();
    while let Some(part) = parts.next() {
        dir = open_relative(
            &dir,
            part.as_os_str(),
            Some(true),
            create,
            deletable && parts.peek().is_none(),
            false,
        )?;
    }
    Ok(dir)
}

pub(super) fn read_file(dir: &File, name: &str) -> io::Result<Option<File>> {
    match open_relative(dir, OsStr::new(name), None, false, false, false) {
        Ok(file) if super::regular_metadata(&file.metadata()?) => Ok(Some(file)),
        Ok(_) => Ok(None),
        Err(error) if super::not_regular_error(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

fn delete(file: &File) -> io::Result<()> {
    let info = FILE_DISPOSITION_INFORMATION { DeleteFile: true };
    let mut status_block = IO_STATUS_BLOCK::default();
    let status = unsafe {
        NtSetInformationFile(
            file.as_raw_handle(),
            &mut status_block,
            (&info as *const FILE_DISPOSITION_INFORMATION).cast(),
            size_of::<FILE_DISPOSITION_INFORMATION>() as u32,
            FileDispositionInformation,
        )
    };
    if status < 0 {
        Err(nt_error(status))
    } else {
        Ok(())
    }
}

fn publish(file: &File, dir: &File, name: &str) -> io::Result<bool> {
    let wide: Vec<u16> = OsStr::new(name).encode_wide().collect();
    let bytes = (offset_of!(FILE_RENAME_INFORMATION, FileName) + wide.len() * size_of::<u16>())
        .max(size_of::<FILE_RENAME_INFORMATION>());
    let mut buffer = vec![0usize; bytes.div_ceil(size_of::<usize>())];
    let info = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    unsafe {
        (*info).Anonymous.ReplaceIfExists = false;
        (*info).RootDirectory = dir.as_raw_handle();
        (*info).FileNameLength = (wide.len() * size_of::<u16>()) as u32;
        copy_nonoverlapping(wide.as_ptr(), (*info).FileName.as_mut_ptr(), wide.len());
    }
    let mut status_block = IO_STATUS_BLOCK::default();
    let status = unsafe {
        NtSetInformationFile(
            file.as_raw_handle(),
            &mut status_block,
            info.cast(),
            bytes as u32,
            FileRenameInformation,
        )
    };
    if status >= 0 {
        return Ok(true);
    }
    let error = nt_error(status);
    if error.kind() == ErrorKind::AlreadyExists {
        Ok(false)
    } else {
        Err(error)
    }
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
        file: open_relative(dir, OsStr::new(&temporary), Some(false), true, true, false)?,
        published: false,
    };
    temporary.file.write_all(bytes)?;
    temporary.published = publish(&temporary.file, dir, name)?;
    if !temporary.published {
        delete(&temporary.file)?;
    }
    Ok(temporary.published)
}

struct Entry {
    name: OsString,
    attributes: u32,
    bytes: u64,
}

fn entries(dir: &File) -> io::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    let mut buffer = vec![0u64; DIRECTORY_BUFFER_BYTES / size_of::<u64>()];
    let mut class = FileIdBothDirectoryRestartInfo;
    loop {
        let success = unsafe {
            GetFileInformationByHandleEx(
                dir.as_raw_handle(),
                class,
                buffer.as_mut_ptr().cast(),
                DIRECTORY_BUFFER_BYTES as u32,
            )
        };
        if success == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                return Ok(entries);
            }
            return Err(error);
        }
        class = FileIdBothDirectoryInfo;
        let mut offset = 0usize;
        loop {
            let header = offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
            let name_offset = offset
                .checked_add(header)
                .filter(|end| *end <= DIRECTORY_BUFFER_BYTES)
                .ok_or_else(|| io::Error::other("invalid offload directory entry"))?;
            let pointer = unsafe {
                buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(offset)
                    .cast::<FILE_ID_BOTH_DIR_INFO>()
            };
            let next = unsafe { (*pointer).NextEntryOffset } as usize;
            let record_end = if next == 0 {
                DIRECTORY_BUFFER_BYTES
            } else {
                if next < header || !next.is_multiple_of(size_of::<u64>()) {
                    return Err(io::Error::other("invalid offload directory offset"));
                }
                offset
                    .checked_add(next)
                    .filter(|end| *end <= DIRECTORY_BUFFER_BYTES)
                    .ok_or_else(|| io::Error::other("invalid offload directory offset"))?
            };
            let name_bytes = unsafe { (*pointer).FileNameLength } as usize;
            if !name_bytes.is_multiple_of(size_of::<u16>())
                || name_offset
                    .checked_add(name_bytes)
                    .is_none_or(|end| end > record_end)
            {
                return Err(io::Error::other("invalid offload directory name"));
            }
            let wide = unsafe {
                slice::from_raw_parts(
                    buffer.as_ptr().cast::<u8>().add(name_offset).cast::<u16>(),
                    name_bytes / size_of::<u16>(),
                )
            };
            let name = OsString::from_wide(wide);
            if name != "." && name != ".." {
                entries.push(Entry {
                    name,
                    attributes: unsafe { (*pointer).FileAttributes },
                    bytes: unsafe { (*pointer).EndOfFile }.max(0) as u64,
                });
            }
            if next == 0 {
                break;
            }
            offset = record_end;
        }
    }
}

pub(super) fn snapshot(dir: &File) -> io::Result<OffloadSnapshot> {
    let mut snapshot = OffloadSnapshot {
        names: Vec::new(),
        total_bytes: 0,
    };
    for entry in entries(dir)? {
        if let Some(name) = entry.name.to_str() {
            snapshot.names.push(name.to_owned());
        }
        if entry.attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) == 0 {
            snapshot.total_bytes = snapshot.total_bytes.saturating_add(entry.bytes);
        }
    }
    Ok(snapshot)
}

pub(super) fn remove_all(dir: &File) -> io::Result<()> {
    for entry in entries(dir)? {
        let child = match open_relative(dir, &entry.name, None, false, true, true) {
            Ok(child) => child,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let attributes = child.metadata()?.file_attributes();
        if attributes & FILE_ATTRIBUTE_DIRECTORY != 0
            && attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
        {
            remove_all(&child)?;
        } else {
            delete(&child)?;
        }
    }
    delete(dir)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::windows::fs::symlink_dir;

    use maki_storage::id::{MakiId, SessionRef};
    use tempfile::TempDir;
    use test_case::test_case;

    use super::{
        create_in, open_relative, publish, read_file, remove_all, snapshot, walk_deletable_dir,
        walk_dir,
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
        let session = SessionRef::from(MakiId::generate());
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
        let file = read_file(&leaf, SLOT).unwrap().unwrap();
        assert!(crate::tools::offload::matches_open_file(file, BODY).unwrap());
        let listing = snapshot(&leaf).unwrap();
        assert_eq!(listing.names, [SLOT]);
        assert_eq!(listing.total_bytes, BODY.len() as u64);
        assert_eq!(fs::read(moved.join(remaining).join(SLOT)).unwrap(), BODY);
        assert!(backend.open_dir(true).is_err());
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
        drop(leaf);
        let leaf = walk_deletable_dir(&opened_parent, Path::new(remaining)).unwrap();
        remove_all(&leaf).unwrap();
        drop(leaf);
        assert!(!moved.join(remaining).exists());
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
    }

    #[test_case(false; "directory")]
    #[test_case(true; "directory_link")]
    fn read_file_rejects_directory_occupants(link: bool) {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        let dir = backend.open_dir(true).unwrap();
        let occupant = backend.dir.join(SLOT);
        if link {
            let target = root.path().join("outside");
            fs::create_dir(&target).unwrap();
            symlink_dir(target, &occupant).unwrap();
        } else {
            fs::create_dir(&occupant).unwrap();
        }
        assert!(read_file(&dir, SLOT).unwrap().is_none());
        assert!(fs::symlink_metadata(&occupant).is_ok());
    }

    #[test]
    fn exclusive_temporary_rejects_replacement_and_publishes_without_clobbering() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        let dir = backend.open_dir(true).unwrap();
        let mut original = open_relative(
            &dir,
            OsStr::new("temporary"),
            Some(false),
            true,
            true,
            false,
        )
        .unwrap();
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
        fs::create_dir(backend.dir.join("nested")).unwrap();
        symlink_dir(&external, backend.dir.join("nested/link")).unwrap();
        drop(dir);
        let dir = walk_deletable_dir(backend.root.as_ref().unwrap(), &backend.relative).unwrap();
        remove_all(&dir).unwrap();
        drop(dir);
        assert!(!backend.dir.exists());
        assert_eq!(fs::read(external.join(SLOT)).unwrap(), BODY);
    }
}
