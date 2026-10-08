use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::mem::{offset_of, size_of};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::{Component, Path, Prefix};
use std::ptr::{null, null_mut};
use std::slice;

use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
use windows_sys::Wdk::Storage::FileSystem::{
    FILE_DIRECTORY_FILE, FILE_DISPOSITION_INFORMATION, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
    FILE_SYNCHRONOUS_IO_NONALERT, FileDispositionInformation, NtCreateFile, NtSetInformationFile,
};
use windows_sys::Win32::Foundation::{
    ERROR_NO_MORE_FILES, ERROR_REPARSE_POINT_ENCOUNTERED, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE,
    RtlNtStatusToDosError, UNICODE_STRING,
};
use windows_sys::Win32::Storage::FileSystem::{
    DELETE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_READ, FILE_ID_BOTH_DIR_INFO, FILE_SHARE_DELETE,
    FILE_SHARE_READ, FILE_SHARE_WRITE, FileIdBothDirectoryInfo, FileIdBothDirectoryRestartInfo,
    GetFileInformationByHandleEx,
};
use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

const DIRECTORY_BUFFER_BYTES: usize = 64 * 1024;

pub(super) fn remove(path: &Path) -> io::Result<()> {
    let (anchor, relative) =
        if path.is_relative() && !matches!(path.components().next(), Some(Component::Prefix(_))) {
            (OsString::from("."), path)
        } else {
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
            (anchor, parts.as_path())
        };
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "offload cleanup needs normal relative components",
        ));
    }
    let mut dir = OpenOptions::new()
        .read(true)
        .access_mode(FILE_GENERIC_READ)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(Path::new(&anchor))?;
    let mut parts = relative.components().peekable();
    while let Some(part) = parts.next() {
        let leaf = parts.peek().is_none();
        dir = open_relative(&dir, part.as_os_str(), !leaf, leaf)?;
    }
    remove_all(&dir)
}

fn open_relative(dir: &File, name: &OsStr, directory: bool, deletable: bool) -> io::Result<File> {
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
        Attributes: OBJ_CASE_INSENSITIVE | if directory { OBJ_DONT_REPARSE } else { 0 },
        SecurityDescriptor: null(),
        SecurityQualityOfService: null(),
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut handle = null_mut();
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            FILE_GENERIC_READ | if deletable { DELETE } else { 0 },
            &attributes,
            &mut status_block,
            null(),
            FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN,
            FILE_SYNCHRONOUS_IO_NONALERT
                | FILE_OPEN_REPARSE_POINT
                | if directory { FILE_DIRECTORY_FILE } else { 0 },
            null(),
            0,
        )
    };
    if status < 0 {
        return Err(nt_error(status));
    }
    let file = unsafe { File::from_raw_handle(handle) };
    if directory && file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::from_raw_os_error(
            ERROR_REPARSE_POINT_ENCOUNTERED as i32,
        ));
    }
    Ok(file)
}

fn nt_error(status: i32) -> io::Error {
    io::Error::from_raw_os_error(unsafe { RtlNtStatusToDosError(status) } as i32)
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

fn entries(dir: &File) -> io::Result<Vec<OsString>> {
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
            return if error.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                Ok(entries)
            } else {
                Err(error)
            };
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
                entries.push(name);
            }
            if next == 0 {
                break;
            }
            offset = record_end;
        }
    }
}

fn remove_all(file: &File) -> io::Result<()> {
    let attributes = file.metadata()?.file_attributes();
    if attributes & FILE_ATTRIBUTE_DIRECTORY != 0 && attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
    {
        for name in entries(file)? {
            let child = match open_relative(file, &name, false, true) {
                Ok(child) => child,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            remove_all(&child)?;
        }
    }
    delete(file)
}
