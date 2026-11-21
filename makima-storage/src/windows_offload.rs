//! Handle-relative Windows operations for offload storage and cleanup.

use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::mem::{offset_of, size_of};
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
use std::os::windows::io::{AsRawHandle, FromRawHandle};
use std::path::Path;
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

const DIRECTORY_BUFFER_BYTES: usize = 64 * 1024;
const SHARE_ALL: u32 = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;

pub enum RelativeOpen {
    ReadDirectory,
    CreateDirectory,
    ReadFile,
    CreateTemporary,
    /// Opens the leaf itself, including a reparse point, for deletion.
    DeleteLeaf,
}

pub fn trusted_root(path: &Path) -> io::Result<File> {
    let root = OpenOptions::new()
        .read(true)
        .access_mode(FILE_GENERIC_READ)
        .share_mode(SHARE_ALL)
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

pub fn open_relative(dir: &File, name: &OsStr, mode: RelativeOpen) -> io::Result<File> {
    let (access, share, disposition, kind) = match mode {
        RelativeOpen::ReadDirectory => (0, SHARE_ALL, FILE_OPEN, FILE_DIRECTORY_FILE),
        RelativeOpen::CreateDirectory => (0, SHARE_ALL, FILE_OPEN_IF, FILE_DIRECTORY_FILE),
        RelativeOpen::ReadFile => (0, SHARE_ALL, FILE_OPEN, FILE_NON_DIRECTORY_FILE),
        RelativeOpen::CreateTemporary => (
            DELETE | FILE_GENERIC_WRITE,
            0,
            FILE_CREATE,
            FILE_NON_DIRECTORY_FILE,
        ),
        RelativeOpen::DeleteLeaf => (DELETE, SHARE_ALL, FILE_OPEN, 0),
    };
    let allow_reparse = matches!(mode, RelativeOpen::DeleteLeaf);
    let mut wide = component_name(name)?;
    let length = (wide.len() * size_of::<u16>()) as u16;
    let unicode = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: wide.as_mut_ptr(),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: dir.as_raw_handle(),
        ObjectName: &unicode,
        Attributes: OBJ_CASE_INSENSITIVE | if allow_reparse { 0 } else { OBJ_DONT_REPARSE },
        SecurityDescriptor: null(),
        SecurityQualityOfService: null(),
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut handle = null_mut();
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            FILE_GENERIC_READ | access,
            &attributes,
            &mut status_block,
            null(),
            FILE_ATTRIBUTE_NORMAL,
            share,
            disposition,
            FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT | kind,
            null(),
            0,
        )
    };
    if status < 0 {
        return Err(nt_error(status));
    }
    let file = unsafe { File::from_raw_handle(handle) };
    if !allow_reparse && file.metadata()?.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::from_raw_os_error(
            ERROR_REPARSE_POINT_ENCOUNTERED as i32,
        ));
    }
    Ok(file)
}

fn component_name(name: &OsStr) -> io::Result<Vec<u16>> {
    let wide: Vec<u16> = name.encode_wide().collect();
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
    if wide.len() > usize::from(u16::MAX) / size_of::<u16>() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "offload name is too long",
        ));
    }
    Ok(wide)
}

fn nt_error(status: i32) -> io::Error {
    io::Error::from_raw_os_error(unsafe { RtlNtStatusToDosError(status) } as i32)
}

pub fn delete(file: &File) -> io::Result<()> {
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

pub fn publish(file: &File, dir: &File, name: &str) -> io::Result<bool> {
    let wide = component_name(OsStr::new(name))?;
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

pub(crate) struct Entry {
    pub(crate) name: OsString,
    attributes: u32,
    bytes: u64,
}

pub(crate) fn entries(dir: &File) -> io::Result<Vec<Entry>> {
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

pub fn total_bytes(dir: &File) -> io::Result<u64> {
    let mut total: u64 = 0;
    for entry in entries(dir)? {
        if entry.attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) == 0 {
            total = total.saturating_add(entry.bytes);
        }
    }
    Ok(total)
}
