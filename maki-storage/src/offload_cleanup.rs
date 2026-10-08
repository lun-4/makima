use std::io;
use std::path::Path;

#[cfg(windows)]
mod windows;

pub(crate) fn remove(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    let result = unix::remove(path);
    #[cfg(windows)]
    let result = windows::remove(path);
    #[cfg(not(any(unix, windows)))]
    let result = Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "anchored offload cleanup is unavailable on this platform",
    ));
    match result {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[cfg(unix)]
mod unix {
    use rustix::fs::{self, AtFlags, Dir, FileType, Mode, OFlags};
    use rustix::io::Errno;
    use std::fs::File;
    use std::io::{self, ErrorKind};
    use std::path::{Component, Path};

    const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);

    pub(super) fn remove(path: &Path) -> io::Result<()> {
        let (anchor, relative) = if path.is_absolute() {
            (
                Path::new("/"),
                path.strip_prefix("/").map_err(io::Error::other)?,
            )
        } else {
            (Path::new("."), path)
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
        let mut parent = File::from(fs::open(anchor, DIRECTORY_FLAGS, Mode::empty())?);
        let mut parts = relative.components().peekable();
        while let Some(part) = parts.next() {
            let name = part.as_os_str();
            if parts.peek().is_none() {
                return remove_entry(&parent, name);
            }
            parent = File::from(fs::openat(&parent, name, DIRECTORY_FLAGS, Mode::empty())?);
        }
        Ok(())
    }

    fn remove_entry(parent: &File, name: impl rustix::path::Arg + Copy) -> io::Result<()> {
        let stat = match fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(Errno::NOENT) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let flags = if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
            let child = File::from(fs::openat(parent, name, DIRECTORY_FLAGS, Mode::empty())?);
            for entry in Dir::read_from(&child)? {
                let entry = entry?;
                let name = entry.file_name();
                if name.to_bytes() != b"." && name.to_bytes() != b".." {
                    remove_entry(&child, name)?;
                }
            }
            AtFlags::REMOVEDIR
        } else {
            AtFlags::empty()
        };
        match fs::unlinkat(parent, name, flags) {
            Err(Errno::NOENT) => Ok(()),
            result => result.map_err(Into::into),
        }
    }
}
