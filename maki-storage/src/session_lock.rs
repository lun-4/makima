//! Cross-process session locks. One `<id>.lock` file per session inside the
//! sessions dir holds only the holder's PID so older binaries respect it. A
//! `<id>.lock.owner` sidecar holds the PID and owner token used by newer leases.
//! Both are coordinated under the canonical file lock; its mtime is the
//! heartbeat. Graceful release writes the ephemeral `released` marker, which
//! the next claim replaces.
//! A session whose lock is fresh and held by another process is open
//! elsewhere and cannot be continued from here.
//!
//! Only write paths (`heartbeat`) mutate lock state. Readers (`open_elsewhere`)
//! never reclaim: a stale lock is cleaned up by the next claimant's heartbeat.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use fs2::FileExt;

use crate::id::MakiId;

pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
pub const STALE_AFTER: Duration = Duration::from_secs(5);
pub const OPEN_ELSEWHERE_MSG: &str = "session is open in another terminal; close it there first";

const RELEASED_RECORD: &str = "released";
const OWNER_SUFFIX: &str = ".owner";

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct LockOwner {
    pid: u32,
    token: Option<String>,
}

fn parse_pid(record: &str) -> Option<u32> {
    record.trim().parse().ok()
}

fn parse_owner(record: &str) -> Option<LockOwner> {
    let mut fields = record.split_whitespace();
    let pid = fields.next()?.parse().ok()?;
    let token = fields.next()?.to_owned();
    if fields.next().is_some() {
        return None;
    }
    Some(LockOwner {
        pid,
        token: Some(token),
    })
}

fn owner_path(path: &Path) -> PathBuf {
    let mut owner = path.as_os_str().to_owned();
    owner.push(OWNER_SUFFIX);
    owner.into()
}

fn read_pid(file: &mut File) -> io::Result<Option<u32>> {
    file.seek(SeekFrom::Start(0))?;
    let mut record = String::new();
    file.read_to_string(&mut record)?;
    Ok(parse_pid(&record))
}

fn read_owner(path: &Path, file: &mut File) -> io::Result<Option<LockOwner>> {
    let Some(pid) = read_pid(file)? else {
        return Ok(None);
    };
    let record = match read_owner_sidecar(path) {
        Ok(record) => record,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Some(LockOwner { pid, token: None }));
        }
        Err(error) => return Err(error),
    };
    let owner = match parse_owner(&record) {
        Some(owner) if owner.pid == pid => owner,
        Some(_) | None => LockOwner { pid, token: None },
    };
    Ok(Some(owner))
}

fn read_owner_sidecar(path: &Path) -> io::Result<String> {
    #[cfg(test)]
    if let Some(error) = take_sidecar_read_error(path) {
        return Err(error);
    }
    fs::read_to_string(owner_path(path))
}

fn holder_pid(path: &Path) -> Option<u32> {
    parse_pid(&fs::read_to_string(path).ok()?)
}

fn write_record(file: &mut File, record: &[u8]) -> io::Result<()> {
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(record)?;
    file.sync_data()
}

fn write_owner(path: &Path, file: &mut File, owner: &LockOwner) -> io::Result<()> {
    let token = owner.token.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "claimed lock owner requires a token",
        )
    })?;
    fs::write(owner_path(path), format!("{} {token}", owner.pid))?;
    write_record(file, owner.pid.to_string().as_bytes())
}

fn owner_token() -> io::Result<String> {
    let mut token = [0_u8; 16];
    getrandom::fill(&mut token)
        .map_err(|error| io::Error::other(format!("generate lock owner token: {error}")))?;
    Ok(token.iter().map(|byte| format!("{byte:02x}")).collect())
}

#[cfg(test)]
type ReleaseHook = (PathBuf, Box<dyn FnOnce() + Send>);

#[cfg(test)]
fn sidecar_read_error() -> &'static Mutex<Option<(PathBuf, io::ErrorKind)>> {
    static ERROR: OnceLock<Mutex<Option<(PathBuf, io::ErrorKind)>>> = OnceLock::new();
    ERROR.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn take_sidecar_read_error(path: &Path) -> Option<io::Error> {
    let mut error = sidecar_read_error()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if error.as_ref().is_some_and(|(expected, _)| expected == path) {
        let (_, kind) = error.take().expect("matching sidecar read error");
        return Some(io::Error::new(kind, "injected sidecar read failure"));
    }
    None
}

#[cfg(test)]
fn release_before_write_hook() -> &'static Mutex<Option<ReleaseHook>> {
    static HOOK: OnceLock<Mutex<Option<ReleaseHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn run_release_before_write_hook(path: &Path) {
    let mut hook = release_before_write_hook()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if hook.as_ref().is_some_and(|(expected, _)| expected == path) {
        let (_, callback) = hook.take().expect("matching release hook");
        drop(hook);
        callback();
    }
}

/// The sole identity-bearing lease for a claimed session lock.
#[derive(Debug)]
pub struct ClaimedSessionLock {
    path: PathBuf,
    owner: LockOwner,
    released: bool,
}

impl ClaimedSessionLock {
    /// Refresh the lease only while the on-disk lock still has this lease's identity.
    pub fn heartbeat(&mut self) -> io::Result<LockBeat> {
        let Some(mut file) = open_existing_locked(&self.path)? else {
            return Ok(LockBeat::Lost);
        };
        if read_owner(&self.path, &mut file)?.as_ref() != Some(&self.owner) {
            file.unlock()?;
            return Ok(LockBeat::Lost);
        }
        write_record(&mut file, self.owner.pid.to_string().as_bytes())?;
        file.unlock()?;
        Ok(LockBeat::Held)
    }

    /// Mark this lease's locked inode released without mutating its pathname.
    pub fn release(mut self) -> io::Result<()> {
        self.release_inner()
    }

    fn release_inner(&mut self) -> io::Result<()> {
        let Some(mut file) = open_existing_locked(&self.path)? else {
            self.released = true;
            return Ok(());
        };
        if read_owner(&self.path, &mut file)?.as_ref() == Some(&self.owner) {
            #[cfg(test)]
            run_release_before_write_hook(&self.path);
            write_record(&mut file, RELEASED_RECORD.as_bytes())?;
        }
        file.unlock()?;
        self.released = true;
        Ok(())
    }
}

impl Drop for ClaimedSessionLock {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.release_inner();
        }
    }
}

fn open_existing_locked(path: &Path) -> io::Result<Option<File>> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    file.try_lock_exclusive()?;
    Ok(Some(file))
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

/// Claim the lock if it is absent, stale, or malformed, returning its sole lease.
pub fn claim(dir: &Path, id: &MakiId) -> io::Result<Option<ClaimedSessionLock>> {
    let path = lock_path(dir, id);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)?;
    if let Err(error) = file.try_lock_exclusive() {
        if error.kind() == io::ErrorKind::WouldBlock {
            return Ok(None);
        }
        return Err(error);
    }
    let stored_owner = read_owner(&path, &mut file)?;
    if stored_owner.is_some()
        && fs::metadata(&path)
            .and_then(|metadata| metadata.modified())
            .is_ok_and(|mtime| is_fresh(mtime, SystemTime::now()))
    {
        file.unlock()?;
        return Ok(None);
    }
    let owner = LockOwner {
        pid: std::process::id(),
        token: Some(owner_token()?),
    };
    write_owner(&path, &mut file, &owner)?;
    file.unlock()?;
    Ok(Some(ClaimedSessionLock {
        path,
        owner,
        released: false,
    }))
}

fn deferred_leases() -> &'static Mutex<HashMap<PathBuf, ClaimedSessionLock>> {
    static LEASES: OnceLock<Mutex<HashMap<PathBuf, ClaimedSessionLock>>> = OnceLock::new();
    LEASES.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Compatibility heartbeat for frontends that do not yet retain a lease.
pub fn heartbeat(dir: &Path, id: &MakiId) -> io::Result<LockBeat> {
    let path = lock_path(dir, id);
    let mut leases = deferred_leases()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(lease) = leases.get_mut(&path) {
        let beat = lease.heartbeat()?;
        if beat == LockBeat::Held {
            return Ok(beat);
        }
        leases.remove(&path);
        return Ok(LockBeat::Lost);
    }
    let Some(lease) = claim(dir, id)? else {
        return Ok(LockBeat::Lost);
    };
    leases.insert(path, lease);
    Ok(LockBeat::Claimed)
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

/// Compatibility release for frontends that do not yet retain a lease.
pub fn release(dir: &Path, id: &MakiId) {
    let path = lock_path(dir, id);
    let lease = deferred_leases()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&path);
    if let Some(lease) = lease {
        let _ = lease.release();
    }
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

    fn legacy_holder_pid(path: &Path) -> Option<u32> {
        fs::read_to_string(path).ok()?.trim().parse().ok()
    }

    fn legacy_claim(path: &Path, pid: u32) -> io::Result<LockBeat> {
        let mut file = File::options().read(true).write(true).open(path)?;
        file.try_lock_exclusive()?;
        let holder = legacy_holder_pid(path);
        let foreign = holder.is_some_and(|holder| holder != pid);
        if foreign
            && fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .is_ok_and(|mtime| is_fresh(mtime, SystemTime::now()))
        {
            file.unlock()?;
            return Ok(LockBeat::Lost);
        }
        write_record(&mut file, pid.to_string().as_bytes())?;
        file.unlock()?;
        Ok(if foreign {
            LockBeat::Claimed
        } else {
            LockBeat::Held
        })
    }

    #[test]
    fn legacy_pid_only_record_parses_without_owner_token() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        fake_lock(dir.path(), &id);
        let mut file = File::options().read(true).write(true).open(&path).unwrap();

        assert_eq!(
            read_owner(&path, &mut file).unwrap(),
            Some(LockOwner {
                pid: FAKE_PID,
                token: None,
            })
        );
    }

    #[test]
    fn malformed_sidecar_is_treated_as_tokenless() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        fake_lock(dir.path(), &id);
        fs::write(owner_path(&path), "malformed owner record").unwrap();
        let mut file = File::options().read(true).write(true).open(&path).unwrap();

        assert_eq!(
            read_owner(&path, &mut file).unwrap(),
            Some(LockOwner {
                pid: FAKE_PID,
                token: None,
            })
        );
    }

    #[test]
    fn claimed_record_is_pid_only_with_token_in_sidecar() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let lease = claim(dir.path(), &id).unwrap().unwrap();
        let owner = parse_owner(&fs::read_to_string(owner_path(&path)).unwrap()).unwrap();

        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            std::process::id().to_string()
        );
        assert_eq!(legacy_holder_pid(&path), Some(std::process::id()));
        assert_eq!(owner.pid, std::process::id());
        assert_eq!(owner.token.as_deref().map(str::len), Some(32));
        lease.release().unwrap();
    }

    #[test]
    fn legacy_parser_and_claim_respect_fresh_new_lease() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let lease = claim(dir.path(), &id).unwrap().unwrap();

        assert_eq!(legacy_holder_pid(&path), Some(std::process::id()));
        assert_eq!(legacy_claim(&path, FAKE_PID).unwrap(), LockBeat::Lost);
        assert!(claim(dir.path(), &id).unwrap().is_none());
        lease.release().unwrap();
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
    fn heartbeat_contention_does_not_report_ownership_loss() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let mut lease = claim(dir.path(), &id).unwrap().unwrap();
        let blocker = File::options().read(true).write(true).open(path).unwrap();
        blocker.lock_exclusive().unwrap();

        assert_eq!(
            lease.heartbeat().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        blocker.unlock().unwrap();
        assert_eq!(lease.heartbeat().unwrap(), LockBeat::Held);
    }

    #[test]
    fn heartbeat_recovers_after_sidecar_read_failure() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let mut lease = claim(dir.path(), &id).unwrap().unwrap();
        *sidecar_read_error().lock().unwrap() = Some((path, io::ErrorKind::PermissionDenied));

        assert_eq!(
            lease.heartbeat().unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert_eq!(lease.heartbeat().unwrap(), LockBeat::Held);
    }

    #[test]
    fn stale_guard_heartbeat_does_not_recreate_absent_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let mut lease = claim(dir.path(), &id).unwrap().unwrap();
        fs::remove_file(&path).unwrap();

        assert_eq!(lease.heartbeat().unwrap(), LockBeat::Lost);
        assert!(!path.exists());
    }

    #[test]
    fn stale_guard_heartbeat_does_not_overwrite_replaced_sidecar() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let mut lease = claim(dir.path(), &id).unwrap().unwrap();
        let replacement_owner = format!("{} replacement", std::process::id());
        fs::write(owner_path(&path), &replacement_owner).unwrap();

        assert_eq!(lease.heartbeat().unwrap(), LockBeat::Lost);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            std::process::id().to_string()
        );
        assert_eq!(
            fs::read_to_string(owner_path(&path)).unwrap(),
            replacement_owner
        );
    }

    #[test]
    fn stale_guard_heartbeat_does_not_overwrite_replaced_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let mut lease = claim(dir.path(), &id).unwrap().unwrap();
        let replacement_pid = FAKE_PID.to_string();
        let replacement_owner = format!("{FAKE_PID} replacement");
        fs::write(&path, &replacement_pid).unwrap();
        fs::write(owner_path(&path), &replacement_owner).unwrap();

        assert_eq!(lease.heartbeat().unwrap(), LockBeat::Lost);
        assert_eq!(fs::read_to_string(&path).unwrap(), replacement_pid);
        assert_eq!(
            fs::read_to_string(owner_path(&path)).unwrap(),
            replacement_owner
        );
    }

    #[test]
    fn stale_guard_release_does_not_recreate_absent_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let lease = claim(dir.path(), &id).unwrap().unwrap();
        fs::remove_file(&path).unwrap();

        lease.release().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn stale_guard_release_does_not_delete_replaced_lock() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let lease = claim(dir.path(), &id).unwrap().unwrap();
        let replacement_pid = FAKE_PID.to_string();
        let replacement_owner = format!("{FAKE_PID} replacement");
        fs::write(&path, &replacement_pid).unwrap();
        fs::write(owner_path(&path), &replacement_owner).unwrap();

        lease.release().unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), replacement_pid);
        assert_eq!(
            fs::read_to_string(owner_path(&path)).unwrap(),
            replacement_owner
        );
    }

    #[test]
    fn release_does_not_modify_path_replaced_after_owner_check() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        let path = lock_path(dir.path(), &id);
        let lease = claim(dir.path(), &id).unwrap().unwrap();
        let replacement = format!("{} replacement", std::process::id());
        let replaced_path = path.clone();
        *release_before_write_hook().lock().unwrap() = Some((
            path.clone(),
            Box::new(move || {
                fs::remove_file(&replaced_path).unwrap();
                fs::write(&replaced_path, &replacement).unwrap();
            }),
        ));

        lease.release().unwrap();

        assert_eq!(
            fs::read_to_string(path).unwrap(),
            format!("{} replacement", std::process::id())
        );
    }

    #[test]
    fn release_marker_is_immediately_claimable() {
        let dir = tempdir().unwrap();
        let id = MakiId::generate();
        heartbeat(dir.path(), &id).unwrap();
        release(dir.path(), &id);

        let path = lock_path(dir.path(), &id);
        assert_eq!(fs::read_to_string(&path).unwrap(), RELEASED_RECORD);
        assert!(!open_elsewhere(dir.path(), &id));
        claim(dir.path(), &id).unwrap().unwrap().release().unwrap();
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
