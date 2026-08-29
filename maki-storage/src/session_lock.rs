//! Cross-process session locks. One `<id>.lock` file per session inside the
//! sessions dir: the file holds the holder's PID, its mtime is the heartbeat.
//! A session whose lock is fresh and held by another process is open
//! elsewhere and cannot be continued from here.
//!
//! Only write paths (`heartbeat`) mutate lock state. Readers (`open_elsewhere`)
//! never reclaim: a stale lock is cleaned up by the next claimant's heartbeat.

use std::fs::{self, File, OpenOptions};
use std::io;

use fs2::FileExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use crate::id::MakiId;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
pub const STALE_AFTER: Duration = Duration::from_secs(5);
pub const OPEN_ELSEWHERE_MSG: &str = "session is open in another terminal; close it there first";

/// Reasons a stored session cannot be continued from this run.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ResumeBlock {
    #[error("session belongs to {0}; cd there and run `makima -c <ID>` from that directory")]
    OtherCwd(String),
    #[error("{OPEN_ELSEWHERE_MSG}")]
    OpenElsewhere,
}

pub fn resume_block(
    session_cwd: &str,
    current_cwd: &str,
    open_elsewhere: bool,
) -> Option<ResumeBlock> {
    if !crate::paths::dirs_equal(session_cwd, current_cwd) {
        return Some(ResumeBlock::OtherCwd(session_cwd.to_owned()));
    }
    open_elsewhere.then_some(ResumeBlock::OpenElsewhere)
}

pub fn lock_path(dir: &Path, id: &MakiId) -> PathBuf {
    dir.join(format!("{id}.lock"))
}

fn holder_pid(path: &Path) -> Option<u32> {
    fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// Outcome of a `heartbeat` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockBeat {
    /// The lock was absent, stale, or malformed and we now hold it.
    Claimed,
    /// We already held the lock; the beat refreshed it.
    Held,
    /// A fresh foreign lock exists: another process holds this session and
    /// this process must not treat itself as the holder. Callers should stop
    /// beating (or surface the loss to the user); a later beat may still
    /// observe the lock going stale and reclaim it.
    Lost,
}

/// A lock is fresh while its mtime is within `STALE_AFTER` of `now` on
/// either side: a small future skew (coarse clocks, timezone-clobbered mtimes)
/// still counts as fresh, but a far-future mtime goes stale like any other so
/// a skewed lock cannot block a session forever.
fn is_fresh(mtime: SystemTime, now: SystemTime) -> bool {
    let skew = if mtime > now {
        mtime.duration_since(now).unwrap_or_default()
    } else {
        now.duration_since(mtime).unwrap_or_default()
    };
    skew <= STALE_AFTER
}

/// Claim the lock if it is absent, stale, malformed, or ours; never clobber a
/// fresh foreign one. Doubles as the periodic heartbeat: callers that keep
/// beating after an initial claim detect losing the lock through [`LockBeat::Lost`].
pub fn heartbeat(dir: &Path, id: &MakiId) -> io::Result<LockBeat> {
    let path = lock_path(dir, id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    if let Err(error) = file.try_lock_exclusive() {
        if error.kind() == io::ErrorKind::WouldBlock {
            return Ok(LockBeat::Lost);
        }
        return Err(error);
    }
    let pid = std::process::id();
    let holder = holder_pid(&path);
    let foreign = holder.is_some_and(|holder| holder != pid);
    if foreign
        && fs::metadata(&path)
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|mtime| is_fresh(mtime, SystemTime::now()))
    {
        file.unlock()?;
        return Ok(LockBeat::Lost);
    }
    use std::io::{Seek, SeekFrom, Write};
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(pid.to_string().as_bytes())?;
    file.sync_data()?;
    file.unlock()?;
    Ok(if foreign {
        LockBeat::Claimed
    } else {
        LockBeat::Held
    })
}

/// True when another process holds a fresh lock for the session. Read-only:
/// stale or malformed locks are left for the next claimant's heartbeat to
/// reclaim.
pub fn open_elsewhere(dir: &Path, id: &MakiId) -> bool {
    let path = lock_path(dir, id);
    let (Ok(meta), Some(holder)) = (fs::metadata(&path), holder_pid(&path)) else {
        return false;
    };
    if holder == std::process::id() {
        return false;
    }
    meta.modified()
        .is_ok_and(|mtime| is_fresh(mtime, SystemTime::now()))
}

/// Drop the lock if we hold it. Best effort: a foreign lock is left for its
/// staleness window to clear.
pub fn release(dir: &Path, id: &MakiId) {
    let path = lock_path(dir, id);
    let Ok(file) = File::open(&path) else {
        return;
    };
    if file.try_lock_exclusive().is_ok() && holder_pid(&path) == Some(std::process::id()) {
        let _ = fs::remove_file(&path);
    }
    let _ = file.unlock();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::tempdir;
    use test_case::test_case;

    const HERE: &str = "/here";
    const ELSEWHERE: &str = "/elsewhere";
    /// A pid no live process on this machine has.
    const FAKE_PID: u32 = u32::MAX - 1;

    fn fake_lock(dir: &Path, id: &MakiId) {
        fs::write(lock_path(dir, id), FAKE_PID.to_string()).unwrap();
    }

    fn backdate(path: &Path, past: Duration) {
        File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(SystemTime::now() - past)
            .unwrap();
    }

    #[test_case(HERE, HERE, false; "same_cwd_free")]
    #[test_case(HERE, HERE, true; "same_cwd_open_elsewhere")]
    #[test_case(ELSEWHERE, HERE, false; "other_cwd_blocked")]
    #[test_case(ELSEWHERE, HERE, true; "other_cwd_wins_over_lock")]
    fn resume_block_matrix(session_cwd: &str, current_cwd: &str, open: bool) {
        let expected = if session_cwd != current_cwd {
            Some(ResumeBlock::OtherCwd(session_cwd.to_owned()))
        } else {
            open.then_some(ResumeBlock::OpenElsewhere)
        };
        assert_eq!(resume_block(session_cwd, current_cwd, open), expected);
    }

    #[test]
    fn open_elsewhere_display_uses_the_shared_message() {
        assert_eq!(ResumeBlock::OpenElsewhere.to_string(), OPEN_ELSEWHERE_MSG);
    }

    #[test_case(0, true; "now")]
    #[test_case(4, true; "just_under_stale")]
    #[test_case(6, false; "past_stale")]
    fn is_fresh_threshold(age_secs: u64, fresh: bool) {
        let now = SystemTime::now();
        assert_eq!(is_fresh(now - Duration::from_secs(age_secs), now), fresh);
    }

    #[test]
    fn heartbeat_claims_absent_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        heartbeat(dir.path(), &id).unwrap();
        assert_eq!(
            holder_pid(&lock_path(dir.path(), &id)),
            Some(std::process::id())
        );
    }

    #[test]
    fn heartbeat_claims_stale_foreign_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        fake_lock(dir.path(), &id);
        backdate(
            &lock_path(dir.path(), &id),
            STALE_AFTER + Duration::from_secs(5),
        );
        heartbeat(dir.path(), &id).unwrap();
        assert_eq!(
            holder_pid(&lock_path(dir.path(), &id)),
            Some(std::process::id())
        );
    }

    #[test]
    fn heartbeat_never_clobbers_fresh_foreign_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        fake_lock(dir.path(), &id);
        heartbeat(dir.path(), &id).unwrap();
        assert_eq!(holder_pid(&lock_path(dir.path(), &id)), Some(FAKE_PID));
    }

    #[test]
    fn heartbeat_claims_malformed_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        fs::write(lock_path(dir.path(), &id), b"not a pid").unwrap();
        heartbeat(dir.path(), &id).unwrap();
        assert_eq!(
            holder_pid(&lock_path(dir.path(), &id)),
            Some(std::process::id())
        );
    }

    #[test]
    fn open_elsewhere_is_true_for_fresh_foreign_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        fake_lock(dir.path(), &id);
        assert!(open_elsewhere(dir.path(), &id));
    }

    #[test]
    fn open_elsewhere_is_false_for_own_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        heartbeat(dir.path(), &id).unwrap();
        assert!(!open_elsewhere(dir.path(), &id));
    }

    #[test]
    fn open_elsewhere_is_false_when_absent() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        assert!(!open_elsewhere(dir.path(), &id));
    }

    #[test]
    fn open_elsewhere_reclaims_stale_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        fake_lock(dir.path(), &id);
        backdate(
            &lock_path(dir.path(), &id),
            STALE_AFTER + Duration::from_secs(5),
        );
        assert!(!open_elsewhere(dir.path(), &id));
        assert!(lock_path(dir.path(), &id).exists());
        assert_eq!(holder_pid(&lock_path(dir.path(), &id)), Some(FAKE_PID));
    }

    #[test]
    fn release_removes_own_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        heartbeat(dir.path(), &id).unwrap();
        release(dir.path(), &id);
        assert!(!lock_path(dir.path(), &id).exists());
    }

    #[test]
    fn release_keeps_foreign_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        fake_lock(dir.path(), &id);
        release(dir.path(), &id);
        assert_eq!(holder_pid(&lock_path(dir.path(), &id)), Some(FAKE_PID));
    }
}
