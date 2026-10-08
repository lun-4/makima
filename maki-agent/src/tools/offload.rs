//! Saves oversized tool output with a bounded preview and an inspection path.

use std::borrow::Cow;
use std::fs::File;
#[cfg(unix)]
use std::io::Write;
use std::io::{self, ErrorKind};
#[cfg(unix)]
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use event_listener::Event;
use maki_config::AgentConfig;
use maki_storage::id::SessionRef;
#[cfg(any(unix, windows))]
use maki_storage::remove_offload_dir_from;
use maki_storage::sessions::{SESSIONS_DIR, offload_dir};
#[cfg(unix)]
use rustix::fs::{self as anchored, AtFlags, Dir, FileType, Mode, OFlags};
#[cfg(unix)]
use rustix::io::Errno;
use thiserror::Error;
use tracing::warn;

use super::FILE_TRUNCATED_MARKER;

#[cfg(windows)]
mod windows;

pub const MAX_OFFLOAD_FILE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_OFFLOAD_SESSION_BYTES: u64 = 256 * 1024 * 1024;
pub const LINE_CUT_PREFIX: &str = "[line cut: ";
pub const OFFLOAD_FOOTER_PREFIX: &str = "[output truncated: ";
const CLIPPED_NOTE: &str = "; saved search results also contain clipped lines";
const INSPECTION_ADVICE: &str = "inspect with read or grep; use bash for long lines";
#[cfg(unix)]
const DIR_MODE: u32 = 0o700;

#[derive(Debug, Error)]
pub enum OffloadError {
    #[error("the session's offload store is closed")]
    Closed,
    #[error("session offload quota of {MAX_OFFLOAD_SESSION_BYTES} bytes reached")]
    Quota,
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub trait OffloadBackend: Send + Sync {
    /// Writes `bytes` under `name` unless the name is taken; false if taken.
    fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool>;
    fn total_bytes(&self) -> io::Result<u64>;
    fn remove_all(&self) -> io::Result<()>;
    fn path(&self, name: &str) -> PathBuf;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Saved {
    pub name: String,
    pub original_bytes: usize,
    pub saved_bytes: usize,
}

impl Saved {
    fn capped(&self) -> bool {
        self.saved_bytes < self.original_bytes
    }
}

pub struct OffloadStore {
    backend: Box<dyn OffloadBackend>,
    closed: AtomicBool,
    operation: Mutex<Option<u64>>,
}

impl OffloadStore {
    pub fn new(backend: Box<dyn OffloadBackend>) -> Self {
        Self {
            backend,
            closed: AtomicBool::new(false),
            operation: Mutex::new(None),
        }
    }

    pub fn on_disk(dir: PathBuf) -> Self {
        Self::new(Box::new(DiskBackend::new(dir)))
    }

    pub fn for_session(state_root: &Path, session: &SessionRef) -> Self {
        let dir = offload_dir(&state_root.join(SESSIONS_DIR), session.id());
        #[cfg(unix)]
        let backend = DiskBackend {
            relative: offload_dir(Path::new(SESSIONS_DIR), session.id()),
            root: anchored::open(
                state_root,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(io::Error::from),
            dir,
        };
        #[cfg(windows)]
        let backend = DiskBackend {
            relative: offload_dir(Path::new(SESSIONS_DIR), session.id()),
            root: windows::trusted_root(state_root),
            dir,
        };
        #[cfg(not(any(unix, windows)))]
        let backend = DiskBackend::new(dir);
        Self::new(Box::new(backend))
    }

    pub fn path_of(&self, saved: &Saved) -> PathBuf {
        self.backend.path(&saved.name)
    }

    pub fn dir(&self) -> PathBuf {
        self.backend.path("")
    }

    pub fn put(&self, body: &str) -> Result<Saved, OffloadError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(OffloadError::Closed);
        }
        self.put_serialized(body)
    }

    fn put_serialized(&self, body: &str) -> Result<Saved, OffloadError> {
        let mut usage = self.operation.lock().unwrap_or_else(|e| e.into_inner());
        if self.closed.load(Ordering::Acquire) {
            return Err(OffloadError::Closed);
        }
        let used = match *usage {
            Some(used) => used,
            None => {
                let used = self.backend.total_bytes()?;
                *usage = Some(used);
                used
            }
        };
        let saved_bytes = body.floor_char_boundary(MAX_OFFLOAD_FILE_BYTES);
        let stored = stored_form(body, saved_bytes);
        let total = used.saturating_add(stored.len() as u64);
        if total > MAX_OFFLOAD_SESSION_BYTES {
            return Err(OffloadError::Quota);
        }
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
        let name = format!("{:032x}.txt", u128::from_ne_bytes(random));
        if !self.backend.create_new(&name, stored.as_bytes())? {
            return Err(
                io::Error::new(ErrorKind::AlreadyExists, "offload artifact name occupied").into(),
            );
        }
        *usage = Some(total);
        Ok(Saved {
            name,
            original_bytes: body.len(),
            saved_bytes,
        })
    }

    pub fn request_close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Closes admission immediately, then waits for an in-flight `put` before removal.
    pub fn close_and_remove(&self) -> io::Result<()> {
        self.request_close();
        self.remove_after_close()
    }

    pub fn close_and_drain(&self) {
        self.request_close();
        let _operation = self.operation.lock().unwrap_or_else(|e| e.into_inner());
    }

    fn remove_after_close(&self) -> io::Result<()> {
        let _operation = self.operation.lock().unwrap_or_else(|e| e.into_inner());
        self.backend.remove_all()
    }
}

#[derive(Clone)]
pub struct OffloadCleanup {
    shared: Arc<CleanupShared>,
}

struct CleanupShared {
    store: Arc<OffloadStore>,
    requested: AtomicBool,
    result: Mutex<Option<Result<(), Arc<io::Error>>>>,
    completed: Event,
}

impl OffloadCleanup {
    pub fn new(store: Arc<OffloadStore>) -> Self {
        Self {
            shared: Arc::new(CleanupShared {
                store,
                requested: AtomicBool::new(false),
                result: Mutex::new(None),
                completed: Event::new(),
            }),
        }
    }

    pub fn request(&self) {
        self.shared.store.request_close();
        if self.shared.requested.swap(true, Ordering::AcqRel) {
            return;
        }
        let shared = Arc::clone(&self.shared);
        smol::spawn(async move {
            smol::unblock(move || {
                let result = shared.store.remove_after_close().map_err(Arc::new);
                if let Err(error) = &result {
                    warn!(%error, dir = %shared.store.dir().display(), "offload cleanup failed");
                }
                *shared.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
                shared.completed.notify(usize::MAX);
            })
            .await;
        })
        .detach();
    }

    pub async fn wait(&self) -> Result<(), Arc<io::Error>> {
        self.request();
        loop {
            let listener = self.shared.completed.listen();
            if let Some(result) = self
                .shared
                .result
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
            {
                return result;
            }
            listener.await;
        }
    }
}

fn stored_form(body: &str, saved_bytes: usize) -> Cow<'_, str> {
    if saved_bytes == body.len() {
        return Cow::Borrowed(body);
    }
    Cow::Owned(format!(
        "{}\n[offload capped: first {saved_bytes} of {} bytes saved]",
        &body[..saved_bytes],
        body.len()
    ))
}

pub struct DiskBackend {
    dir: PathBuf,
    #[cfg(any(unix, windows))]
    root: io::Result<File>,
    #[cfg(any(unix, windows))]
    relative: PathBuf,
}

impl DiskBackend {
    fn new(dir: PathBuf) -> Self {
        #[cfg(unix)]
        {
            let (anchor, relative) = if dir.is_absolute() {
                (
                    Path::new("/"),
                    dir.strip_prefix("/").unwrap_or(&dir).to_owned(),
                )
            } else {
                (Path::new("."), dir.clone())
            };
            let root = anchored::open(
                anchor,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map(File::from)
            .map_err(io::Error::from);
            Self {
                dir,
                root,
                relative,
            }
        }
        #[cfg(windows)]
        {
            let (root, relative) = windows::disk_root(&dir);
            Self {
                dir,
                root,
                relative,
            }
        }
        #[cfg(not(any(unix, windows)))]
        Self { dir }
    }

    fn validate_name(name: &str) -> io::Result<()> {
        if name.is_empty() || name == "." || name == ".." || name.contains(['/', '\\', ':', '\0']) {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "offload artifact name must be a single normal path component",
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn open_dir(&self, create: bool) -> io::Result<File> {
        let root = self
            .root
            .as_ref()
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
        Self::walk_dir(root, &self.relative, create)
    }

    #[cfg(unix)]
    fn walk_dir(root: &File, relative: &Path, create: bool) -> io::Result<File> {
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
        for part in relative.components() {
            if create {
                match anchored::mkdirat(&dir, part.as_os_str(), Mode::from_raw_mode(DIR_MODE)) {
                    Err(error) if error == Errno::EXIST => {}
                    result => result?,
                }
            }
            dir = anchored::openat(
                &dir,
                part.as_os_str(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?
            .into();
        }
        Ok(dir)
    }

    #[cfg(unix)]
    fn create_in(dir: &File, name: &str, bytes: &[u8]) -> io::Result<bool> {
        Self::validate_name(name)?;
        anchored::fchmod(dir, Mode::from_raw_mode(DIR_MODE))?;
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
        let temporary = format!(".offload-{:032x}", u128::from_ne_bytes(random));
        let mut file = File::from(anchored::openat(
            dir,
            temporary.as_str(),
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        let result = file.write_all(bytes).and_then(|()| {
            match anchored::linkat(dir, temporary.as_str(), dir, name, AtFlags::empty()) {
                Ok(()) => Ok(true),
                Err(error) if error == Errno::EXIST => Ok(false),
                Err(error) => Err(error.into()),
            }
        });
        let cleanup = anchored::unlinkat(dir, temporary.as_str(), AtFlags::empty());
        let created = result?;
        cleanup?;
        Ok(created)
    }

    #[cfg(windows)]
    fn open_dir(&self, create: bool) -> io::Result<File> {
        let root = self
            .root
            .as_ref()
            .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
        windows::walk_dir(root, &self.relative, create)
    }

    #[cfg(not(any(unix, windows)))]
    fn open_dir(&self, _create: bool) -> io::Result<File> {
        Err(io::Error::new(
            ErrorKind::Unsupported,
            "secure offload directories are unsupported on this platform",
        ))
    }
}

impl OffloadBackend for DiskBackend {
    fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
        Self::validate_name(name)?;
        let _dir = self.open_dir(true)?;
        #[cfg(unix)]
        {
            Self::create_in(&_dir, name, bytes)
        }
        #[cfg(windows)]
        {
            windows::create_in(&_dir, name, bytes)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = bytes;
            Ok(false)
        }
    }

    fn total_bytes(&self) -> io::Result<u64> {
        let dir = match self.open_dir(false) {
            Ok(dir) => dir,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };
        #[cfg(unix)]
        {
            let mut total: u64 = 0;
            for entry in Dir::read_from(&dir)? {
                let entry = entry?;
                let name = entry.file_name();
                match anchored::statat(&dir, name, AtFlags::SYMLINK_NOFOLLOW) {
                    Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile => {
                        total = total.saturating_add(stat.st_size as u64);
                    }
                    Ok(_) => {}
                    Err(error) if error == Errno::NOENT => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(total)
        }
        #[cfg(windows)]
        {
            windows::total_bytes(&dir)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = dir;
            Ok(0)
        }
    }

    fn remove_all(&self) -> io::Result<()> {
        #[cfg(any(unix, windows))]
        {
            let root = self
                .root
                .as_ref()
                .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
            remove_offload_dir_from(root, &self.relative)
        }
        #[cfg(not(any(unix, windows)))]
        self.open_dir(false).map(|_| ())
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviewShape {
    Head,
    HeadTail,
}

pub struct OutputLimitOptions {
    pub deadline: Option<Instant>,
    pub trailer: Option<String>,
    pub shape: PreviewShape,
    pub lines_clipped: bool,
    pub limits: OutputLimits,
}

impl OutputLimitOptions {
    pub fn prepare_for_output_hook(&mut self, body: &mut String) {
        if let Some(trailer) = self.trailer.as_mut() {
            trailer.truncate(trailer.trim_end_matches('\n').len());
            *body = with_trailer(body.trim_end_matches('\n'), Some(trailer));
        }
    }

    pub fn recover_filtered_trailer(&mut self, body: &mut String) {
        let Some(trailer) = self.trailer.take().filter(|trailer| !trailer.is_empty()) else {
            return;
        };
        let filtered = body.trim_end_matches('\n');
        let Some(prefix) = filtered.strip_suffix(&trailer) else {
            return;
        };
        let boundary = if prefix.is_empty() {
            0
        } else if let Some(prefix) = prefix.strip_suffix('\n') {
            prefix.len()
        } else {
            return;
        };
        body.truncate(boundary);
        self.trailer = Some(trailer);
    }

    pub async fn apply(self, body: String, store: Option<Arc<OffloadStore>>) -> String {
        smol::unblock(move || {
            limit_output(
                &body,
                &LimitOpts {
                    trailer: self.trailer.as_deref(),
                    shape: self.shape,
                    lines_clipped: self.lines_clipped,
                    limits: self.limits,
                },
                store.as_deref(),
            )
        })
        .await
    }
}

#[derive(Debug, Clone)]
pub struct OutputLimits {
    pub max_lines: usize,
    pub max_bytes: usize,
}

impl OutputLimits {
    pub fn from_config(config: &AgentConfig) -> Self {
        Self {
            max_lines: config.max_output_lines,
            max_bytes: config.max_output_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LimitOpts<'a> {
    pub trailer: Option<&'a str>,
    pub shape: PreviewShape,
    /// The tool clips long lines before this point, so the saved file holds
    /// them clipped too, and the footer must say so.
    pub lines_clipped: bool,
    pub limits: OutputLimits,
}

/// Bounds tool output for the model. The limits cover the whole returned
/// text: preview, footer and trailer. Metadata is never cut, so when it alone
/// exceeds the limits the result is the metadata with no preview.
pub fn limit_output(body: &str, opts: &LimitOpts, store: Option<&OffloadStore>) -> String {
    let body = body.trim_end_matches('\n');
    let whole = with_trailer(body, opts.trailer);
    if fits(&whole, opts.limits.max_lines, opts.limits.max_bytes) {
        return whole;
    }
    let (metadata, shape) = match store.map(|store| (store, store.put(body))) {
        None => (FILE_TRUNCATED_MARKER.to_owned(), PreviewShape::Head),
        Some((store, Err(e))) => {
            warn!(error = %e, dir = %store.dir().display(), bytes = body.len(), "tool output not offloaded");
            (
                format!("{FILE_TRUNCATED_MARKER} (full output not saved: {e})"),
                PreviewShape::Head,
            )
        }
        Some((store, Ok(saved))) => {
            let path = store.path_of(&saved);
            (footer(&saved, &path, opts), opts.shape)
        }
    };
    let metadata = with_trailer(&metadata, opts.trailer);
    let budget = Budget {
        lines: opts
            .limits
            .max_lines
            .saturating_sub(line_count(&metadata) + 1),
        bytes: opts.limits.max_bytes.saturating_sub(metadata.len() + 2),
    };
    let preview = match shape {
        PreviewShape::Head => head(body, budget),
        PreviewShape::HeadTail => head_tail(body, budget),
    };
    match preview {
        Some(preview) if !preview.is_empty() => format!("{preview}\n\n{metadata}"),
        _ => metadata,
    }
}

/// Recognizes the saved-output footer in model-facing text.
pub fn is_offload_notice(line: &str) -> bool {
    line.starts_with(OFFLOAD_FOOTER_PREFIX)
}

fn with_trailer(text: &str, trailer: Option<&str>) -> String {
    match trailer {
        Some(trailer) if text.is_empty() => trailer.to_owned(),
        Some(trailer) => format!("{text}\n{trailer}"),
        None => text.to_owned(),
    }
}

fn line_count(text: &str) -> usize {
    text.split('\n').count()
}

fn fits(text: &str, max_lines: usize, max_bytes: usize) -> bool {
    text.len() <= max_bytes && line_count(text) <= max_lines
}

fn footer(saved: &Saved, path: &Path, opts: &LimitOpts) -> String {
    let discarded = if saved.capped() {
        format!(
            ", {} bytes discarded",
            saved.original_bytes - saved.saved_bytes
        )
    } else {
        String::new()
    };
    let clipped = if opts.lines_clipped { CLIPPED_NOTE } else { "" };
    format!(
        "{OFFLOAD_FOOTER_PREFIX}{} bytes saved to {}{discarded}; {INSPECTION_ADVICE}{clipped}]",
        saved.saved_bytes,
        path.display(),
    )
}

#[derive(Debug)]
struct Budget {
    lines: usize,
    bytes: usize,
}

pub fn format_line_cut(kept: usize, of: usize, from_start: bool) -> String {
    let end = if from_start { "first" } else { "last" };
    format!("{LINE_CUT_PREFIX}{end} {kept} of {of} bytes]")
}

/// One oversized line cut to `bytes`, its marker included, keeping the start
/// or the end. None when not even the marker fits.
fn cut_line(line: &str, bytes: usize, from_start: bool) -> Option<String> {
    let reserve = format_line_cut(line.len(), line.len(), from_start).len();
    let room = bytes.checked_sub(reserve)?;
    let kept = if from_start {
        &line[..line.floor_char_boundary(room)]
    } else {
        &line[line.ceil_char_boundary(line.len() - room)..]
    };
    if kept.is_empty() {
        return None;
    }
    Some(format!(
        "{kept}{}",
        format_line_cut(kept.len(), line.len(), from_start)
    ))
}

/// Whole lines taken from one end within `budget`. When even the first line
/// taken is too long, it is cut instead.
fn take_lines<'a>(
    lines: impl Iterator<Item = &'a str>,
    budget: Budget,
    from_start: bool,
) -> Option<Vec<String>> {
    let mut taken = Vec::new();
    let mut used = 0;
    for line in lines.take(budget.lines) {
        let cost = line.len() + usize::from(!taken.is_empty());
        if used + cost > budget.bytes {
            if taken.is_empty() {
                return cut_line(line, budget.bytes, from_start).map(|cut| vec![cut]);
            }
            break;
        }
        used += cost;
        taken.push(line.to_owned());
    }
    Some(taken)
}

fn head(body: &str, budget: Budget) -> Option<String> {
    take_lines(body.split('\n'), budget, true).map(|lines| lines.join("\n"))
}

fn omission_line(omitted: usize) -> String {
    format!("[... {omitted} lines omitted ...]")
}

fn head_tail(body: &str, budget: Budget) -> Option<String> {
    let total = line_count(body);
    if total < 2 || budget.lines < 2 {
        return head(body, budget);
    }
    let omission_reserve = omission_line(total).len() + 2;
    let lines = budget.lines.checked_sub(1)?;
    let bytes = budget.bytes.checked_sub(omission_reserve)?;
    let head_budget = Budget {
        lines: lines.div_ceil(2),
        bytes: bytes.div_ceil(2),
    };
    let tail_budget = Budget {
        lines: lines / 2,
        bytes: bytes / 2,
    };
    let head = take_lines(body.split('\n'), head_budget, true).unwrap_or_default();
    let remaining = total - head.len();
    let mut tail =
        take_lines(body.rsplit('\n').take(remaining), tail_budget, false).unwrap_or_default();
    tail.reverse();
    if head.is_empty() && tail.is_empty() {
        return None;
    }
    let omitted = total - head.len() - tail.len();
    let mut parts = head;
    if omitted > 0 {
        parts.push(omission_line(omitted));
    }
    parts.extend(tail);
    Some(parts.join("\n"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(unix)]
    use std::fs;
    use std::io;
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink as symlink_dir};
    #[cfg(unix)]
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    #[cfg(unix)]
    use maki_storage::id::{MakiId, SessionRef};
    #[cfg(unix)]
    use maki_storage::sessions::SESSIONS_DIR;
    use tempfile::TempDir;
    use test_case::test_case;

    use super::{
        CLIPPED_NOTE, LINE_CUT_PREFIX, LimitOpts, MAX_OFFLOAD_FILE_BYTES,
        MAX_OFFLOAD_SESSION_BYTES, OffloadBackend, OffloadError, OffloadStore, OutputLimitOptions,
        OutputLimits, PreviewShape, cut_line, limit_output, line_count,
    };
    #[cfg(unix)]
    use super::{DIR_MODE, DiskBackend};
    use crate::tools::FILE_TRUNCATED_MARKER;

    const TEST_BODY: &str = "stored body";
    const TRAILER: &str = "Exit code: 3";
    const REDACTED_TRAILER: &str = "[redacted]";
    const CREATE_ERROR: &str = "disk full";
    const GATE_TIMEOUT: Duration = Duration::from_secs(5);
    const QUOTA_THREADS: usize = 8;
    const QUOTA_BODY_BYTES: usize = 1024;
    const QUOTA_WRITES: usize = 3;
    const CUT_BYTES: usize = 48;
    #[cfg(unix)]
    const EXTERNAL_MODE: u32 = 0o755;

    #[derive(Default)]
    struct MemoryBackend {
        files: Mutex<BTreeMap<String, Vec<u8>>>,
        reserved_bytes: u64,
        scans: AtomicUsize,
        fail: AtomicBool,
    }

    impl OffloadBackend for Arc<MemoryBackend> {
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            if self.fail.load(Ordering::Relaxed) {
                return Err(io::Error::other(CREATE_ERROR));
            }
            let mut files = self.files.lock().unwrap();
            if files.contains_key(name) {
                return Ok(false);
            }
            files.insert(name.to_owned(), bytes.to_vec());
            Ok(true)
        }

        fn total_bytes(&self) -> io::Result<u64> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(self.reserved_bytes
                + self
                    .files
                    .lock()
                    .unwrap()
                    .values()
                    .map(|bytes| bytes.len() as u64)
                    .sum::<u64>())
        }

        fn remove_all(&self) -> io::Result<()> {
            self.files.lock().unwrap().clear();
            Ok(())
        }

        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/offload").join(name)
        }
    }

    fn memory_store(reserved_bytes: u64) -> (Arc<MemoryBackend>, OffloadStore) {
        let backend = Arc::new(MemoryBackend {
            reserved_bytes,
            ..MemoryBackend::default()
        });
        let store = OffloadStore::new(Box::new(Arc::clone(&backend)));
        (backend, store)
    }

    fn numbered(lines: usize) -> String {
        (1..=lines)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn opts(shape: PreviewShape, max_lines: usize, max_bytes: usize) -> LimitOpts<'static> {
        LimitOpts {
            trailer: None,
            shape,
            lines_clipped: false,
            limits: OutputLimits {
                max_lines,
                max_bytes,
            },
        }
    }

    fn within_limits(text: &str, options: &LimitOpts) {
        assert!(text.len() <= options.limits.max_bytes);
        assert!(line_count(text) <= options.limits.max_lines);
    }

    #[test]
    fn output_that_fits_is_returned_with_trailer() {
        let options = LimitOpts {
            trailer: Some(TRAILER),
            ..opts(PreviewShape::HeadTail, 10, 100)
        };
        assert_eq!(
            limit_output("one\ntwo\n", &options, None),
            format!("one\ntwo\n{TRAILER}")
        );
    }

    #[test]
    fn output_hook_redacted_trailer_is_not_restored() {
        let mut options = OutputLimitOptions {
            deadline: None,
            trailer: Some(TRAILER.to_owned()),
            shape: PreviewShape::Head,
            lines_clipped: false,
            limits: opts(PreviewShape::Head, 10, 100).limits,
        };
        let mut body = TEST_BODY.to_owned();
        options.prepare_for_output_hook(&mut body);
        assert!(body.ends_with(TRAILER));
        body = body.replace(TRAILER, REDACTED_TRAILER);
        options.recover_filtered_trailer(&mut body);
        assert!(options.trailer.is_none());
        let output = smol::block_on(options.apply(body, None));
        assert_eq!(output, format!("{TEST_BODY}\n{REDACTED_TRAILER}"));
    }

    #[test]
    fn offload_saves_raw_body_and_returns_bounded_preview_with_trailer() {
        let (backend, store) = memory_store(0);
        let body = numbered(100);
        let options = LimitOpts {
            trailer: Some(TRAILER),
            ..opts(PreviewShape::HeadTail, 12, 500)
        };
        let output = limit_output(&body, &options, Some(&store));
        let files = backend.files.lock().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files.values().next().unwrap(), body.as_bytes());
        assert!(output.starts_with("line 1\n"), "{output}");
        assert!(output.ends_with(TRAILER), "{output}");
        within_limits(&output, &options);
    }

    #[test]
    fn head_tail_preview_keeps_both_ends_and_omission_count() {
        let (_, store) = memory_store(0);
        let options = opts(PreviewShape::HeadTail, 12, 500);
        let output = limit_output(&numbered(100), &options, Some(&store));
        assert!(output.starts_with("line 1\n"), "{output}");
        assert!(output.contains("line 100"), "{output}");
        assert!(output.contains("lines omitted"), "{output}");
        within_limits(&output, &options);
    }

    #[test_case(true; "head")]
    #[test_case(false; "tail")]
    fn line_cut_preserves_utf8_boundaries(from_start: bool) {
        let line = "é日🙂".repeat(40);
        let cut = cut_line(&line, CUT_BYTES, from_start).unwrap();
        assert!(cut.len() <= CUT_BYTES);
        let kept = &cut[..cut.find(LINE_CUT_PREFIX).unwrap()];
        assert!(kept.chars().all(|ch| "é日🙂".contains(ch)));
    }

    #[test]
    fn store_caps_bytes_on_a_utf8_boundary_and_discloses_discarded_bytes() {
        let (backend, store) = memory_store(0);
        let body = format!("{}é", "x".repeat(MAX_OFFLOAD_FILE_BYTES - 1));
        let saved = store.put(&body).unwrap();
        assert_eq!(saved.original_bytes, body.len());
        assert!(saved.saved_bytes <= MAX_OFFLOAD_FILE_BYTES);
        assert!(body.is_char_boundary(saved.saved_bytes));
        let notice = format!(
            "\n[offload capped: first {} of {} bytes saved]",
            saved.saved_bytes, saved.original_bytes,
        );
        {
            let files = backend.files.lock().unwrap();
            let stored = &files[&saved.name];
            assert!(stored.starts_with(&body.as_bytes()[..saved.saved_bytes]));
            assert!(stored.ends_with(notice.as_bytes()));
        }

        let options = opts(PreviewShape::Head, 8, 500);
        let output = limit_output(&body, &options, Some(&store));
        assert!(output.contains("discard"), "{output}");
    }

    #[test_case(false; "no_store")]
    #[test_case(true; "io_error")]
    fn missing_or_failed_store_falls_back_to_truncated_preview(fail: bool) {
        let options = opts(PreviewShape::HeadTail, 8, 500);
        let (backend, store) = memory_store(0);
        backend.fail.store(fail, Ordering::Relaxed);
        let output = limit_output(&numbered(100), &options, fail.then_some(&store));
        assert!(output.starts_with("line 1\n"), "{output}");
        assert!(output.contains(FILE_TRUNCATED_MARKER));
        if fail {
            assert!(output.contains(CREATE_ERROR));
        }
        within_limits(&output, &options);
    }

    #[test]
    fn zero_budget_keeps_offload_metadata_without_preview() {
        let (_, store) = memory_store(0);
        let output = limit_output(&numbered(20), &opts(PreviewShape::Head, 0, 0), Some(&store));
        assert!(!output.is_empty());
        assert!(!output.contains("line 1"));
    }

    #[test]
    fn each_put_creates_a_fresh_artifact_for_identical_bodies() {
        let (backend, store) = memory_store(0);
        let first = store.put(TEST_BODY).unwrap();
        let second = store.put(TEST_BODY).unwrap();
        assert_ne!(first.name, second.name);
        let files = backend.files.lock().unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[&first.name], TEST_BODY.as_bytes());
        assert_eq!(files[&second.name], TEST_BODY.as_bytes());
    }

    #[test]
    fn quota_scans_initial_usage_once_and_charges_successful_writes() {
        let room = 2 * TEST_BODY.len() as u64;
        let (backend, store) = memory_store(MAX_OFFLOAD_SESSION_BYTES - room);
        assert_eq!(backend.scans.load(Ordering::Relaxed), 0);
        store.put(TEST_BODY).unwrap();
        backend.files.lock().unwrap().clear();
        store.put(TEST_BODY).unwrap();
        assert!(matches!(store.put(TEST_BODY), Err(OffloadError::Quota)));
        assert_eq!(backend.scans.load(Ordering::Relaxed), 1);
        assert_eq!(backend.files.lock().unwrap().len(), 1);
    }

    #[test]
    fn failed_create_does_not_charge_quota() {
        let (backend, store) = memory_store(MAX_OFFLOAD_SESSION_BYTES - TEST_BODY.len() as u64);
        backend.fail.store(true, Ordering::Relaxed);
        let Err(OffloadError::Io(error)) = store.put(TEST_BODY) else {
            panic!("expected create failure");
        };
        assert_eq!(error.to_string(), CREATE_ERROR);
        assert!(backend.files.lock().unwrap().is_empty());
        backend.fail.store(false, Ordering::Relaxed);
        store.put(TEST_BODY).unwrap();
        assert!(matches!(store.put(TEST_BODY), Err(OffloadError::Quota)));
        assert_eq!(backend.scans.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn clipped_line_footer_warns_saved_results_are_also_clipped() {
        let (_, store) = memory_store(0);
        let options = LimitOpts {
            lines_clipped: true,
            ..opts(PreviewShape::Head, 8, 500)
        };
        let output = limit_output(&numbered(100), &options, Some(&store));
        assert!(output.contains(CLIPPED_NOTE), "{output}");
        within_limits(&output, &options);
    }

    #[test]
    fn concurrent_puts_respect_total_quota_without_large_fixture_allocation() {
        let room = (QUOTA_WRITES * QUOTA_BODY_BYTES) as u64;
        let (backend, store) = memory_store(MAX_OFFLOAD_SESSION_BYTES - room);
        let store = Arc::new(store);
        let barrier = Arc::new(Barrier::new(QUOTA_THREADS));
        let joins: Vec<_> = (0..QUOTA_THREADS)
            .map(|_| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    store.put(&"x".repeat(QUOTA_BODY_BYTES)).is_ok()
                })
            })
            .collect();
        let saved = joins
            .into_iter()
            .map(|join| join.join().unwrap())
            .filter(|saved| *saved)
            .count();
        assert_eq!(saved, QUOTA_WRITES);
        assert_eq!(backend.scans.load(Ordering::Relaxed), 1);
        assert_eq!(backend.files.lock().unwrap().len(), QUOTA_WRITES);
    }

    #[cfg(unix)]
    #[test]
    fn disk_backend_creates_private_directory_and_file() {
        let root = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(root.path().join("store"));
        let saved = store.put(TEST_BODY).unwrap();
        let mode = |path: &Path| fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&store.path_of(&saved)), 0o600);
        assert_eq!(mode(&store.dir()), DIR_MODE);
    }

    #[test]
    fn close_rejects_later_puts_and_removes_the_directory() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("store");
        let store = OffloadStore::on_disk(path.clone());
        store.put(TEST_BODY).unwrap();
        store.close_and_remove().unwrap();
        assert!(matches!(store.put("late"), Err(OffloadError::Closed)));
        assert!(!path.exists());
    }

    struct GatedBackend {
        inner: Arc<MemoryBackend>,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl OffloadBackend for GatedBackend {
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            self.events.lock().unwrap().push("put");
            self.inner.create_new(name, bytes)
        }
        fn total_bytes(&self) -> io::Result<u64> {
            self.inner.total_bytes()
        }
        fn remove_all(&self) -> io::Result<()> {
            self.events.lock().unwrap().push("remove");
            self.inner.remove_all()
        }
        fn path(&self, name: &str) -> PathBuf {
            self.inner.path(name)
        }
    }

    #[test]
    fn close_rejects_new_put_and_drains_in_flight_put_before_removal() {
        let inner = Arc::new(MemoryBackend::default());
        let (entered_tx, entered_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let events = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(OffloadStore::new(Box::new(GatedBackend {
            inner: Arc::clone(&inner),
            entered: entered_tx,
            release: release_rx,
            events: Arc::clone(&events),
        })));
        let putter = {
            let store = Arc::clone(&store);
            thread::spawn(move || store.put(TEST_BODY))
        };
        entered_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        store.request_close();
        assert!(matches!(store.put("late"), Err(OffloadError::Closed)));
        let closer = {
            let store = Arc::clone(&store);
            thread::spawn(move || store.close_and_remove().unwrap())
        };
        release_tx.send(()).unwrap();
        putter.join().unwrap().unwrap();
        closer.join().unwrap();
        assert_eq!(*events.lock().unwrap(), ["put", "remove"]);
        assert!(inner.files.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn managed_parent_symlink_rejects_save_without_touching_external_files() {
        let root = TempDir::new().unwrap();
        let external = root.path().join("outside");
        fs::create_dir_all(external.join("offload")).unwrap();
        let sentinel = external.join("offload/sentinel");
        fs::write(&sentinel, b"untouched").unwrap();
        symlink_dir(&external, root.path().join(SESSIONS_DIR)).unwrap();
        let session = SessionRef::from(MakiId::generate());
        let store = OffloadStore::for_session(root.path(), &session);
        assert!(matches!(store.put(TEST_BODY), Err(OffloadError::Io(_))));
        assert_eq!(fs::read(&sentinel).unwrap(), b"untouched");
        assert_eq!(fs::read_dir(external.join("offload")).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn ancestor_handle_swap_cannot_redirect_descendant_creation() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join(SESSIONS_DIR));
        let opened = backend.open_dir(true).unwrap();
        let moved = root.path().join("original");
        fs::rename(&backend.dir, &moved).unwrap();
        let external = root.path().join("outside");
        fs::create_dir(&external).unwrap();
        fs::set_permissions(&external, fs::Permissions::from_mode(EXTERNAL_MODE)).unwrap();
        symlink_dir(&external, &backend.dir).unwrap();
        let leaf = DiskBackend::walk_dir(&opened, Path::new("offload/session"), true).unwrap();
        assert!(DiskBackend::create_in(&leaf, "artifact.txt", TEST_BODY.as_bytes()).unwrap());
        assert_eq!(
            fs::read(moved.join("offload/session/artifact.txt")).unwrap(),
            TEST_BODY.as_bytes()
        );
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
        assert_eq!(
            fs::metadata(&external).unwrap().permissions().mode() & 0o777,
            EXTERNAL_MODE
        );
    }

    #[cfg(unix)]
    #[test]
    fn cleanup_does_not_follow_nested_symlink_to_external_sentinel() {
        let root = TempDir::new().unwrap();
        let external = root.path().join("outside");
        fs::create_dir(&external).unwrap();
        fs::write(external.join("sentinel"), b"untouched").unwrap();
        let store = OffloadStore::on_disk(root.path().join("store"));
        store.put(TEST_BODY).unwrap();
        fs::create_dir(store.dir().join("nested")).unwrap();
        symlink_dir(&external, store.dir().join("nested/link")).unwrap();
        let store_dir = store.dir();
        store.close_and_remove().unwrap();
        assert!(!store_dir.exists());
        assert_eq!(fs::read(external.join("sentinel")).unwrap(), b"untouched");
    }
}
