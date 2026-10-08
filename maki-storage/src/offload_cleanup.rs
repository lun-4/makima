use std::fs::File;
use std::io::{self, ErrorKind};
use std::path::{Component, Path};

#[cfg(windows)]
mod windows;

pub(crate) struct Root {
    #[cfg(unix)]
    inner: unix::Root,
    #[cfg(windows)]
    inner: windows::Root,
}

/// Removes an offload directory beneath a captured root handle without following descendant links.
/// The root is trusted; the relative path must be nonempty and contain only normal components.
/// Missing directories are already removed.
pub fn remove_offload_dir_from(root: &File, relative: &Path) -> io::Result<()> {
    let root = Root::from_file(root)?;
    match root.directory(relative) {
        Ok(_) => root.remove(relative),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

impl Root {
    fn from_file(root: &File) -> io::Result<Self> {
        #[cfg(unix)]
        let inner = unix::Root::from_file(root)?;
        #[cfg(windows)]
        let inner = windows::Root::from_file(root)?;
        #[cfg(not(any(unix, windows)))]
        {
            let _ = root;
            Ok(Self {})
        }
        #[cfg(any(unix, windows))]
        {
            Ok(Self { inner })
        }
    }

    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        #[cfg(unix)]
        let inner = unix::Root::open(path)?;
        #[cfg(windows)]
        let inner = windows::Root::open(path)?;
        #[cfg(not(any(unix, windows)))]
        {
            let _ = path;
            Ok(Self {})
        }
        #[cfg(any(unix, windows))]
        {
            Ok(Self { inner })
        }
    }

    pub(crate) fn directory(&self, relative: &Path) -> io::Result<Self> {
        validate_relative(relative)?;
        #[cfg(unix)]
        let inner = self.inner.directory(relative)?;
        #[cfg(windows)]
        let inner = self.inner.directory(relative)?;
        #[cfg(not(any(unix, windows)))]
        {
            let _ = relative;
            Ok(Self {})
        }
        #[cfg(any(unix, windows))]
        {
            Ok(Self { inner })
        }
    }

    pub(crate) fn remove(&self, relative: &Path) -> io::Result<()> {
        validate_relative(relative)?;
        #[cfg(unix)]
        let result = self.inner.remove(relative);
        #[cfg(windows)]
        let result = self.inner.remove(relative);
        #[cfg(not(any(unix, windows)))]
        let result = Err(io::Error::new(
            ErrorKind::Unsupported,
            "anchored offload cleanup is unavailable on this platform",
        ));
        match result {
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            result => result,
        }
    }
}

fn validate_relative(relative: &Path) -> io::Result<()> {
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
    Ok(())
}

#[cfg(unix)]
mod unix {
    use rustix::fs::{self, AtFlags, Dir, FileType, Mode, OFlags};
    use rustix::io::Errno;
    use std::fs::File;
    use std::io;
    use std::path::Path;

    const ROOT_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::CLOEXEC);
    const DESCENDANT_FLAGS: OFlags = ROOT_FLAGS.union(OFlags::NOFOLLOW);

    pub(super) struct Root(File);

    impl Root {
        pub(super) fn from_file(root: &File) -> io::Result<Self> {
            Ok(Self(root.try_clone()?))
        }

        pub(super) fn open(path: &Path) -> io::Result<Self> {
            Ok(Self(File::from(fs::open(path, ROOT_FLAGS, Mode::empty())?)))
        }

        pub(super) fn directory(&self, relative: &Path) -> io::Result<Self> {
            let mut parent = self.0.try_clone()?;
            for part in relative.components() {
                parent = File::from(fs::openat(
                    &parent,
                    part.as_os_str(),
                    DESCENDANT_FLAGS,
                    Mode::empty(),
                )?);
            }
            Ok(Self(parent))
        }

        pub(super) fn remove(&self, relative: &Path) -> io::Result<()> {
            let mut parent = self.0.try_clone()?;
            let mut parts = relative.components().peekable();
            while let Some(part) = parts.next() {
                let name = part.as_os_str();
                if parts.peek().is_none() {
                    return remove_entry(&parent, name);
                }
                parent = File::from(fs::openat(&parent, name, DESCENDANT_FLAGS, Mode::empty())?);
            }
            Ok(())
        }
    }

    fn remove_entry(parent: &File, name: impl rustix::path::Arg + Copy) -> io::Result<()> {
        let stat = match fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(Errno::NOENT) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let flags = if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
            let child = File::from(fs::openat(parent, name, DESCENDANT_FLAGS, Mode::empty())?);
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
