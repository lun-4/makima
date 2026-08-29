//! Persistent storage. `atomic_write` writes to a `tempfile` in the same
//! directory then persists (atomic rename) for crash safety.
//! `atomic_write_permissions` sets file mode before persist (for auth keys at 0600).

pub mod auth;
pub mod id;
pub mod input_history;
pub mod log;
pub mod model;
pub mod paths;
pub mod plans;
pub mod session_lock;
pub mod sessions;
pub mod theme;
pub mod version;

use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
#[cfg(windows)]
use std::thread;
#[cfg(windows)]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::NamedTempFile;

use paths::state_dir;

#[cfg(windows)]
const RENAME_ATTEMPTS: usize = 20;

#[derive(Debug, Clone)]
pub struct StateDir(PathBuf);

impl StateDir {
    pub fn resolve() -> Result<Self, StorageError> {
        let dir = state_dir()?;
        Ok(Self(dir))
    }

    pub fn from_path(path: PathBuf) -> Self {
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn ensure_subdir(&self, name: &str) -> Result<PathBuf, StorageError> {
        let dir = self.0.join(name);
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("home directory not found")]
    HomeNotSet,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("slug collision after max attempts")]
    SlugCollision,
}

pub fn atomic_write(path: &Path, data: &[u8]) -> Result<(), StorageError> {
    atomic_write_at(&atomic_destination(path)?, data)
}

fn atomic_destination(path: &Path) -> Result<PathBuf, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Ok(fs::canonicalize(path)?),
        Ok(_) => Ok(path.to_owned()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(path.to_owned()),
        Err(error) => Err(error.into()),
    }
}

fn atomic_write_at(path: &Path, data: &[u8]) -> Result<(), StorageError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = NamedTempFile::new_in(parent)?;
    tmp.write_all(data)?;
    if let Ok(metadata) = fs::metadata(path) {
        fs::set_permissions(tmp.path(), metadata.permissions())?;
    }
    tmp.as_file().sync_data()?;
    #[cfg(test)]
    atomic_test_hook::wait_at_replacement_boundary();
    persist(tmp, path)
}

pub(crate) fn atomic_write_permissions(
    path: &Path,
    data: &[u8],
    mode: u32,
) -> Result<(), StorageError> {
    let path = atomic_destination(path)?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = NamedTempFile::new_in(parent)?;
    tmp.write_all(data)?;
    #[cfg(unix)]
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(mode))?;
    #[cfg(not(unix))]
    let _ = mode;
    tmp.as_file().sync_all()?;
    persist(tmp, &path)
}

/// Right before the atomic rename, so a test can park the writer with the
/// complete temporary file prepared and observe readers around the boundary.
/// Unarmed (the default) it is a single relaxed atomic load.
#[cfg(test)]
pub mod atomic_test_hook {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    static ARMED: AtomicBool = AtomicBool::new(false);
    type Channels = (flume::Sender<()>, flume::Sender<()>, flume::Receiver<()>);
    static CHANNELS: Mutex<Option<Channels>> = Mutex::new(None);

    /// Arm once: the writer signals `reached` when the temp file is fully
    /// prepared, then blocks until released via [`disarm`]. Returns the
    /// test's `reached` receiver.
    pub fn arm() -> flume::Receiver<()> {
        let (reached_tx, reached_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        *CHANNELS.lock().expect("hook poisoned") = Some((reached_tx, release_tx, release_rx));
        ARMED.store(true, Ordering::SeqCst);
        reached_rx
    }

    /// Send the writer past the boundary and disarm the hook.
    pub fn disarm() {
        ARMED.store(false, Ordering::SeqCst);
        if let Some((_, release_tx, _)) = CHANNELS.lock().expect("hook poisoned").take() {
            release_tx.send(()).ok();
        }
    }

    pub fn wait_at_replacement_boundary() {
        if !ARMED.load(Ordering::SeqCst) {
            return;
        }
        let channels = {
            let guard = CHANNELS.lock().expect("hook poisoned");
            match &*guard {
                Some(ch) => ch.clone(),
                None => return,
            }
        };
        channels.0.send(()).ok();
        let _ = channels.2.recv();
    }
}

/// `into_parts` drops the auto-cleanup-on-drop guarantee, but we need the
/// File handle closed (Windows can't rename an open file) and tempfile's
/// `persist()` doesn't support the fibonacci backoff retry that Windows
/// virus scanners require. On failure, we manually clean up the temp file.
fn persist(tmp: NamedTempFile, path: &Path) -> Result<(), StorageError> {
    let (_, tmp_path) = tmp.into_parts();
    retry_rename(&tmp_path, path).map_err(|e| {
        let _ = fs::remove_file(&tmp_path);
        StorageError::Io(e)
    })?;
    sync_parent_dir(path);
    Ok(())
}

/// A rename is durable only once the directory entry reaches disk; without
/// this a freshly created file can vanish after power loss even though the
/// write returned Ok. Best effort: not every filesystem accepts a directory
/// fsync. Windows gets the same from `MOVEFILE_WRITE_THROUGH` in
/// `retry_rename`.
pub(crate) fn sync_parent_dir(path: &Path) {
    #[cfg(unix)]
    if let Some(dir) = path.parent()
        && let Ok(f) = fs::File::open(dir)
    {
        let _ = f.sync_all();
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// Rename with fibonacci backoff to handle transient `PermissionDenied` from
/// virus scanners on Windows. 20 steps from 1ms sums to ~18 seconds.
/// Matches the pattern used by juliaup and rustup.
///
/// On non-Windows platforms, `PermissionDenied` from rename is a real
/// permissions problem (different user, immutable flag, etc.) that
/// retrying will not fix, so we just call rename once.
#[cfg(windows)]
fn retry_rename(src: &Path, dest: &Path) -> std::io::Result<()> {
    let mut a: u64 = 0;
    let mut b: u64 = 1;
    for _ in 0..RENAME_ATTEMPTS {
        match fs::rename(src, dest) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                thread::sleep(Duration::from_millis(b));
                let next = a.saturating_add(b);
                a = b;
                b = next;
            }
            Err(e) => return Err(e),
        }
    }
    fs::rename(src, dest)
}

#[cfg(not(windows))]
fn retry_rename(src: &Path, dest: &Path) -> std::io::Result<()> {
    fs::rename(src, dest)
}

pub fn now_epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGINAL: &[u8] = b"original";
    const OWNER_ONLY_FILE_MODE: u32 = 0o600;
    const REPLACEMENT: &[u8] = b"replacement";
    #[cfg(unix)]
    const FILE_MODE_MASK: u32 = 0o777;

    #[test]
    fn atomic_write_replaces_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        fs::write(&path, ORIGINAL).unwrap();

        atomic_write(&path, REPLACEMENT).unwrap();

        assert_eq!(fs::read(path).unwrap(), REPLACEMENT);
    }

    #[test]
    fn atomic_write_permissions_replaces_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        fs::write(&path, ORIGINAL).unwrap();

        atomic_write_permissions(&path, REPLACEMENT, OWNER_ONLY_FILE_MODE).unwrap();

        assert_eq!(fs::read(path).unwrap(), REPLACEMENT);
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_creates_owner_only_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");

        atomic_write(&path, ORIGINAL).unwrap();

        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & FILE_MODE_MASK,
            OWNER_ONLY_FILE_MODE
        );
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_preserves_destination_permissions() {
        const MODE: u32 = 0o640;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        fs::write(&path, ORIGINAL).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(MODE)).unwrap();

        atomic_write(&path, REPLACEMENT).unwrap();

        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & FILE_MODE_MASK,
            MODE
        );
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_cleans_up_temp_after_replacement_failure() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("destination");
        fs::create_dir(&destination).unwrap();

        assert!(atomic_write(&destination, REPLACEMENT).is_err());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// Readers racing the replace boundary observe either the complete old
    /// payload or the complete new one, never a torn mix. The writer parks
    /// right before rename with the temp file fully prepared.
    #[test]
    fn real_atomic_replacement_has_no_torn_reads() {
        use std::thread;

        const BIG: usize = 1 << 18;
        const READS: usize = 64;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        let old = vec![b'a'; BIG];
        let new = vec![b'b'; BIG];
        fs::write(&path, &old).unwrap();

        let reached_rx = atomic_test_hook::arm();
        let path_writer = path.clone();
        let new_writer = new.clone();
        let writer = thread::spawn(move || {
            atomic_write(&path_writer, &new_writer).expect("atomic write");
        });

        // The temp file is complete and the rename is about to happen.
        reached_rx.recv().expect("writer reached the boundary");

        let mut observations = std::collections::HashSet::new();
        for _ in 0..READS {
            observations.insert(fs::read(&path).expect("read"));
        }
        atomic_test_hook::disarm();
        writer.join().expect("writer finished");

        observations.insert(fs::read(&path).expect("final read"));
        assert_eq!(
            observations,
            std::collections::HashSet::from([old, new]),
            "readers must see only complete old or new payloads"
        );
    }
}
