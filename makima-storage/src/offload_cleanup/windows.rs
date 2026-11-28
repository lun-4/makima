use std::fs::File;
use std::io::{self, ErrorKind};
use std::os::windows::fs::MetadataExt;
use std::path::Path;

use windows_sys::Win32::Storage::FileSystem::{
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
};

use crate::windows_offload::{RelativeOpen, delete, entries, open_relative, trusted_root};

pub(super) struct Root(File);

impl Root {
    pub(super) fn from_file(root: &File) -> io::Result<Self> {
        Ok(Self(root.try_clone()?))
    }

    pub(super) fn open(path: &Path) -> io::Result<Self> {
        trusted_root(path).map(Self)
    }

    pub(super) fn directory(&self, relative: &Path) -> io::Result<Self> {
        let mut dir = self.0.try_clone()?;
        for part in relative.components() {
            dir = open_relative(&dir, part.as_os_str(), RelativeOpen::ReadDirectory)?;
        }
        Ok(Self(dir))
    }

    pub(super) fn remove(&self, relative: &Path) -> io::Result<()> {
        let mut dir = self.0.try_clone()?;
        let mut parts = relative.components().peekable();
        while let Some(part) = parts.next() {
            let mode = if parts.peek().is_none() {
                RelativeOpen::DeleteLeaf
            } else {
                RelativeOpen::ReadDirectory
            };
            dir = open_relative(&dir, part.as_os_str(), mode)?;
        }
        remove_all(&dir)
    }
}

fn remove_all(file: &File) -> io::Result<()> {
    let attributes = file.metadata()?.file_attributes();
    if attributes & FILE_ATTRIBUTE_DIRECTORY != 0 && attributes & FILE_ATTRIBUTE_REPARSE_POINT == 0
    {
        for entry in entries(file)? {
            let child = match open_relative(file, &entry.name, RelativeOpen::DeleteLeaf) {
                Ok(child) => child,
                Err(error) if error.kind() == ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            remove_all(&child)?;
        }
    }
    delete(file)
}
