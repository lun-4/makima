//! Session-scoped store for tool output that would otherwise be cut.
//!
//! Output past the limits is saved whole and the model gets a bounded
//! preview plus the file's path. Files are named by content, and one locked
//! `put` both deduplicates and enforces the session quota, so parallel tools
//! and subagents sharing a store can't race past either.

use std::borrow::Cow;
#[cfg(test)]
use std::fs::OpenOptions;
use std::fs::{self, File};
#[cfg(any(unix, test))]
use std::io::Write;
use std::io::{self, ErrorKind, Read};
#[cfg(unix)]
use std::path::Component;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use event_listener::Event;
use maki_config::AgentConfig;
use maki_storage::id::SessionRef;
use maki_storage::session_lock::SessionPublicationGuard;
use maki_storage::sessions::{SESSIONS_DIR, offload_dir};
#[cfg(unix)]
use rustix::fs::{self as anchored, AtFlags, Dir, FileType, Mode, OFlags};
#[cfg(unix)]
use rustix::io::Errno;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::warn;

use super::FILE_TRUNCATED_MARKER;

#[cfg(windows)]
mod windows;

pub const MAX_OFFLOAD_FILE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_OFFLOAD_SESSION_BYTES: u64 = 256 * 1024 * 1024;
pub const LINE_CUT_PREFIX: &str = "[line cut: ";
pub const OFFLOAD_FOOTER_PREFIX: &str = "[output truncated: ";
pub const OFFLOAD_POINTER_PREFIX: &str = "[output identical to a result saved earlier";
pub const DEFAULT_LABEL: &str = "output";
const HASH_HEX_CHARS: usize = 16;
const SLOT_EXT: &str = ".txt";
const MAX_PUT_ATTEMPTS: usize = 8;
const COMPARE_CHUNK_BYTES: usize = 16 * 1024;
const READ_ADVICE: &str = "inspect it with grep, or read with offset and limit";
const BASH_ADVICE: &str =
    "inspect it with bash (e.g. jq, or cut -c) since some lines exceed agent.max_line_bytes";
const CLIPPED_NOTE: &str =
    "; lines longer than agent.max_line_bytes are clipped in the saved file too";
const SHELL_SAFE: &[u8] = b"/._-+:,@%=";
const SIZE_UNITS: [&str; 3] = ["KB", "MB", "GB"];
const ROUNDS_TO_NEXT_UNIT: f64 = 1023.95;
#[cfg(unix)]
const DIR_MODE: u32 = 0o700;

#[derive(Debug, Error)]
pub enum OffloadError {
    #[error("the session's offload store is closed")]
    Closed,
    #[error("session offload quota of {MAX_OFFLOAD_SESSION_BYTES} bytes reached")]
    Quota,
    #[error("offload slot names kept being taken")]
    SlotsTaken,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Raw file operations under one directory. The store logic sits above this
/// once, so the disk and in-memory variants can't drift apart.
pub trait OffloadBackend: Send + Sync {
    fn matches(&self, name: &str, expected: &[u8]) -> io::Result<bool>;
    /// Writes `bytes` under `name` unless the name is taken; false if taken.
    fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool>;
    fn snapshot(&self) -> io::Result<OffloadSnapshot>;
    fn remove_all(&self) -> io::Result<()>;
    fn path(&self, name: &str) -> PathBuf;
}

pub struct OffloadSnapshot {
    pub names: Vec<String>,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Created,
    Existing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Saved {
    pub name: String,
    pub outcome: PutOutcome,
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
    operation: Mutex<()>,
}

impl OffloadStore {
    pub fn new(backend: Box<dyn OffloadBackend>) -> Self {
        Self {
            backend,
            closed: AtomicBool::new(false),
            operation: Mutex::new(()),
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

    /// Saves `body` unless an identical result is already stored. Existing
    /// files are compared byte for byte, because the model may have edited
    /// one, so neither a name nor the lowest free slot proves anything.
    pub fn put(&self, body: &str) -> Result<Saved, OffloadError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(OffloadError::Closed);
        }
        self.put_serialized(body)
    }

    fn put_serialized(&self, body: &str) -> Result<Saved, OffloadError> {
        let _operation = self.operation.lock().unwrap_or_else(|e| e.into_inner());
        if self.closed.load(Ordering::Acquire) {
            return Err(OffloadError::Closed);
        }
        let saved_bytes = body.floor_char_boundary(MAX_OFFLOAD_FILE_BYTES);
        let stored = stored_form(body, saved_bytes);
        let hash = content_hash(body);
        let saved = |name: String, outcome| Saved {
            name,
            outcome,
            original_bytes: body.len(),
            saved_bytes,
        };
        for _ in 0..MAX_PUT_ATTEMPTS {
            let snapshot = self.backend.snapshot()?;
            let slots: Vec<_> = snapshot
                .names
                .iter()
                .filter(|name| slot_number(name, &hash).is_some())
                .collect();
            for name in &slots {
                if self.backend.matches(name, stored.as_bytes())? {
                    return Ok(saved((*name).clone(), PutOutcome::Existing));
                }
            }
            if snapshot.total_bytes.saturating_add(stored.len() as u64) > MAX_OFFLOAD_SESSION_BYTES
            {
                return Err(OffloadError::Quota);
            }
            let free = (1..)
                .map(|n| slot_name(&hash, n))
                .find(|name| !slots.contains(&name))
                .expect("slot numbers are unbounded");
            if self.backend.create_new(&free, stored.as_bytes())? {
                return Ok(saved(free, PutOutcome::Created));
            }
        }
        Err(OffloadError::SlotsTaken)
    }

    pub fn request_close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// Closes admission immediately, then waits for an in-flight `put` before removal.
    pub fn close_and_remove(&self) -> io::Result<()> {
        self.request_close();
        self.remove_after_close()
    }

    pub fn close_and_remove_guarded(&self, guard: &SessionPublicationGuard) -> io::Result<()> {
        self.request_close();
        let _operation = self.operation.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .publish(|| self.backend.remove_all())?
            .ok_or_else(|| {
                io::Error::new(ErrorKind::PermissionDenied, "session lock ownership lost")
            })?
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

pub fn offload_dir_for(state_dir: &Path, session: Option<&SessionRef>) -> Option<PathBuf> {
    session.map(|session| offload_dir(&state_dir.join(SESSIONS_DIR), session.id()))
}

fn content_hash(body: &str) -> String {
    Sha256::digest(body.as_bytes())
        .iter()
        .take(HASH_HEX_CHARS / 2)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn slot_name(hash: &str, n: usize) -> String {
    match n {
        1 => format!("{hash}{SLOT_EXT}"),
        _ => format!("{hash}-{n}{SLOT_EXT}"),
    }
}

fn slot_number(name: &str, hash: &str) -> Option<usize> {
    let rest = name.strip_prefix(hash)?.strip_suffix(SLOT_EXT)?;
    match rest {
        "" => Some(1),
        _ => rest.strip_prefix('-')?.parse().ok().filter(|&n| n > 1),
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

#[cfg(test)]
fn open_regular(path: &Path) -> io::Result<Option<File>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        };
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if not_regular_error(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if !regular_metadata(&file.metadata()?) {
        return Ok(None);
    }
    Ok(Some(file))
}

fn not_regular_error(error: &io::Error) -> bool {
    if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) {
        return true;
    }
    #[cfg(unix)]
    if matches!(
        error.raw_os_error(),
        Some(libc::ELOOP | libc::ENXIO | libc::ENODEV)
    ) {
        return true;
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{
            ERROR_CANT_ACCESS_FILE, ERROR_DIRECTORY, ERROR_REPARSE_POINT_ENCOUNTERED,
        };
        if matches!(error.raw_os_error(), Some(code) if code == ERROR_CANT_ACCESS_FILE as i32 || code == ERROR_DIRECTORY as i32 || code == ERROR_REPARSE_POINT_ENCOUNTERED as i32)
        {
            return true;
        }
    }
    false
}

fn regular_metadata(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return false;
        }
    }
    metadata.is_file()
}

fn compare_reader(reader: &mut impl Read, expected: &[u8]) -> io::Result<bool> {
    let mut buffer = [0; COMPARE_CHUNK_BYTES];
    let mut consumed = 0;
    loop {
        let limit = buffer
            .len()
            .min(expected.len().saturating_sub(consumed) + 1);
        let read = match reader.read(&mut buffer[..limit]) {
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            result => result?,
        };
        if read == 0 {
            return Ok(consumed == expected.len());
        }
        let end = consumed + read;
        if end > expected.len() || buffer[..read] != expected[consumed..end] {
            return Ok(false);
        }
        consumed = end;
    }
}

fn matches_reader(
    mut reader: impl Read,
    expected: &[u8],
    mut length: impl FnMut() -> io::Result<u64>,
) -> io::Result<bool> {
    if length()? != expected.len() as u64 {
        return Ok(false);
    }
    if !compare_reader(&mut reader, expected)? {
        return Ok(false);
    }
    Ok(length()? == expected.len() as u64)
}

fn matches_open_file(file: File, expected: &[u8]) -> io::Result<bool> {
    matches_reader(&file, expected, || Ok(file.metadata()?.len()))
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
    fn remove_contents(dir: &File) -> io::Result<()> {
        for entry in Dir::read_from(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            let stat = match anchored::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) => stat,
                Err(Errno::NOENT) => continue,
                Err(error) => return Err(error.into()),
            };
            let flags = if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
                let child = File::from(anchored::openat(
                    dir,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )?);
                Self::remove_contents(&child)?;
                AtFlags::REMOVEDIR
            } else {
                AtFlags::empty()
            };
            match anchored::unlinkat(dir, name, flags) {
                Err(Errno::NOENT) => {}
                result => result?,
            }
        }
        Ok(())
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
    fn matches(&self, name: &str, expected: &[u8]) -> io::Result<bool> {
        Self::validate_name(name)?;
        #[cfg(unix)]
        {
            let dir = self.open_dir(false)?;
            let file = match anchored::openat(
                &dir,
                name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            ) {
                Ok(fd) => File::from(fd),
                Err(error) if not_regular_error(&error.into()) => return Ok(false),
                Err(error) => return Err(error.into()),
            };
            if !regular_metadata(&file.metadata()?) {
                return Ok(false);
            }
            matches_open_file(file, expected)
        }
        #[cfg(windows)]
        {
            let dir = self.open_dir(false)?;
            match windows::read_file(&dir, name)? {
                Some(file) => matches_open_file(file, expected),
                None => Ok(false),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            self.open_dir(false)?;
            Ok(false)
        }
    }

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

    fn snapshot(&self) -> io::Result<OffloadSnapshot> {
        #[cfg(unix)]
        let mut snapshot = OffloadSnapshot {
            names: Vec::new(),
            total_bytes: 0,
        };
        #[cfg(not(unix))]
        let snapshot = OffloadSnapshot {
            names: Vec::new(),
            total_bytes: 0,
        };
        let dir = match self.open_dir(false) {
            Ok(dir) => dir,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(snapshot),
            Err(error) => return Err(error),
        };
        #[cfg(unix)]
        for entry in Dir::read_from(&dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if name.to_bytes() == b"." || name.to_bytes() == b".." {
                continue;
            }
            if let Ok(name) = name.to_str() {
                snapshot.names.push(name.to_owned());
            }
            match anchored::statat(&dir, name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile => {
                    snapshot.total_bytes = snapshot.total_bytes.saturating_add(stat.st_size as u64);
                }
                Ok(_) => {}
                Err(error) if error == Errno::NOENT => {}
                Err(error) => return Err(error.into()),
            }
        }
        #[cfg(windows)]
        {
            windows::snapshot(&dir)
        }
        #[cfg(not(windows))]
        {
            Ok(snapshot)
        }
    }

    fn remove_all(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            let result: io::Result<()> = (|| {
                if self.relative.as_os_str().is_empty() {
                    return Err(io::Error::new(
                        ErrorKind::InvalidInput,
                        "cannot remove the offload root",
                    ));
                }
                let dir = self.open_dir(false)?;
                Self::remove_contents(&dir)?;
                let parent = self.relative.parent().unwrap_or_else(|| Path::new(""));
                let root = self
                    .root
                    .as_ref()
                    .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
                let parent = Self::walk_dir(root, parent, false)?;
                let name = self.relative.file_name().ok_or_else(|| {
                    io::Error::new(ErrorKind::InvalidInput, "cannot remove the offload root")
                })?;
                anchored::unlinkat(&parent, name, AtFlags::REMOVEDIR)?;
                Ok(())
            })();
            match result {
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                result => result,
            }
        }
        #[cfg(windows)]
        {
            if self.relative.as_os_str().is_empty() {
                return Err(io::Error::new(
                    ErrorKind::InvalidInput,
                    "cannot remove the offload root",
                ));
            }
            let root = self
                .root
                .as_ref()
                .map_err(|error| io::Error::new(error.kind(), error.to_string()))?;
            match windows::walk_deletable_dir(root, &self.relative) {
                Ok(dir) => windows::remove_all(&dir),
                Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
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
    pub label: String,
    pub lines_clipped: bool,
    pub limits: OutputLimits,
}

impl OutputLimitOptions {
    pub fn prepare_for_output_hook(&mut self, body: &mut String) {
        self.label = DEFAULT_LABEL.to_owned();
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
                    label: &self.label,
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
    pub max_line_bytes: usize,
}

impl OutputLimits {
    pub fn from_config(config: &AgentConfig) -> Self {
        Self {
            max_lines: config.max_output_lines,
            max_bytes: config.max_output_bytes,
            max_line_bytes: config.max_line_bytes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LimitOpts<'a> {
    pub trailer: Option<&'a str>,
    pub shape: PreviewShape,
    pub label: &'a str,
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
            match saved.outcome {
                PutOutcome::Existing => {
                    return with_trailer(&pointer(body, &saved, &path, opts), opts.trailer);
                }
                PutOutcome::Created => (footer(body, &saved, &path, opts), opts.shape),
            }
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

/// Whether `line` is a footer or pointer `limit_output` wrote, for views
/// that rebuild themselves from the model-facing text.
pub fn is_offload_notice(line: &str) -> bool {
    line.starts_with(OFFLOAD_POINTER_PREFIX)
        || line
            .split_once(" truncated: ")
            .is_some_and(|(head, rest)| head.starts_with('[') && rest.contains(" saved "))
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

fn human_size(bytes: usize) -> String {
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut size = bytes as f64 / 1024.0;
    let mut unit = 0;
    // Anything that would print as "1024.0" belongs to the next unit.
    while size >= ROUNDS_TO_NEXT_UNIT && unit + 1 < SIZE_UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.1} {}", SIZE_UNITS[unit])
}

fn shell_quoted(path: &str) -> Option<String> {
    let safe = |b: u8| b.is_ascii_alphanumeric() || SHELL_SAFE.contains(&b);
    (!path.bytes().all(safe)).then(|| format!("'{}'", path.replace('\'', r"'\''")))
}

fn advice(body: &str, path: &str, opts: &LimitOpts) -> String {
    if !body
        .split('\n')
        .any(|line| line.len() > opts.limits.max_line_bytes)
    {
        return READ_ADVICE.to_owned();
    }
    match shell_quoted(path) {
        Some(quoted) => format!("{BASH_ADVICE} (shell path: {quoted})"),
        None => BASH_ADVICE.to_owned(),
    }
}

fn size_summary(body: &str, saved: &Saved) -> String {
    format!(
        "{} lines, {}",
        line_count(body),
        human_size(saved.original_bytes)
    )
}

fn footer(body: &str, saved: &Saved, path: &Path, opts: &LimitOpts) -> String {
    let path = path.display().to_string();
    let saved_where = if saved.capped() {
        format!(
            "first {} saved to {path}, the remaining {} discarded",
            human_size(saved.saved_bytes),
            human_size(saved.original_bytes - saved.saved_bytes)
        )
    } else {
        format!("all of it saved to {path}")
    };
    let clipped = if opts.lines_clipped { CLIPPED_NOTE } else { "" };
    format!(
        "[{} truncated: {}; {saved_where}; {}{clipped}]",
        opts.label,
        size_summary(body, saved),
        advice(&body[..saved.saved_bytes], &path, opts)
    )
}

fn pointer(body: &str, saved: &Saved, path: &Path, opts: &LimitOpts) -> String {
    let capped = if saved.capped() {
        format!(
            " (first {} saved, {} discarded)",
            human_size(saved.saved_bytes),
            human_size(saved.original_bytes - saved.saved_bytes)
        )
    } else {
        String::new()
    };
    let label_note = if opts.label == DEFAULT_LABEL {
        String::new()
    } else {
        format!(" ({})", opts.label)
    };
    let path = path.display().to_string();
    let clipped = if opts.lines_clipped { CLIPPED_NOTE } else { "" };
    format!(
        "{OFFLOAD_POINTER_PREFIX} in this session (possibly by another agent){label_note}: {}{capped}, at {path}; read the file if that result is not in this conversation; {}{clipped}]",
        size_summary(body, saved),
        advice(&body[..saved.saved_bytes], &path, opts)
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
    mod offload_benchmark_fixtures {
        include!("offload_benchmark_fixtures.rs");
    }

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicUsize;
    use std::thread;
    use std::time::Duration;

    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    const BIG: usize = 100_000;
    const GATE_TIMEOUT: Duration = Duration::from_secs(10);
    const TEST_BODY: &str = "stored body";
    const REMOVE_ERROR: &str = "remove denied";
    const CREATE_EVENT: &str = "create";
    const REMOVE_EVENT: &str = "remove";
    const SHORT_TRAILER: &str = "Exit code: 3";
    const LONG_TRAILER: &str = "Exit code: 3 after a trailer long enough to eat a real share of the byte budget, as a long path or reason would";

    #[derive(Default)]
    struct MapBackend {
        files: Mutex<BTreeMap<String, Vec<u8>>>,
        fail: bool,
    }

    impl OffloadBackend for Arc<MapBackend> {
        fn matches(&self, name: &str, expected: &[u8]) -> io::Result<bool> {
            Ok(self.files.lock().unwrap().get(name).map(Vec::as_slice) == Some(expected))
        }
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            if self.fail {
                return Err(io::Error::other("disk full"));
            }
            let mut files = self.files.lock().unwrap();
            if files.contains_key(name) {
                return Ok(false);
            }
            files.insert(name.to_owned(), bytes.to_vec());
            Ok(true)
        }
        fn snapshot(&self) -> io::Result<OffloadSnapshot> {
            let files = self.files.lock().unwrap();
            Ok(OffloadSnapshot {
                names: files.keys().cloned().collect(),
                total_bytes: files.values().map(|v| v.len() as u64).sum(),
            })
        }
        fn remove_all(&self) -> io::Result<()> {
            self.files.lock().unwrap().clear();
            Ok(())
        }
        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/offload").join(name)
        }
    }

    #[test_case("stored body\nExit code: 3", Some(SHORT_TRAILER); "unchanged")]
    #[test_case("stored body\nExit code: [redacted]", None; "implicit_redaction_unprotected")]
    #[test_case("replacement first\nreplacement second", None; "same_line_count_replacement")]
    #[test_case("stored body\nprefixExit code: 3", None; "not_separate_terminal_suffix")]
    #[test_case(TEST_BODY, None; "removed")]
    #[test_case("replacement", None; "whole_output_replaced")]
    fn filtered_trailer_recovery_preserves_only_retained_terminal_metadata(
        filtered: &str,
        expected: Option<&str>,
    ) {
        let mut options = OutputLimitOptions {
            deadline: None,
            trailer: Some(SHORT_TRAILER.to_owned()),
            shape: PreviewShape::Head,
            label: TEST_BODY.to_owned(),
            lines_clipped: false,
            limits: OutputLimits {
                max_lines: 0,
                max_bytes: 0,
                max_line_bytes: 0,
            },
        };
        let mut body = TEST_BODY.to_owned();
        options.prepare_for_output_hook(&mut body);
        assert_eq!(body, format!("{TEST_BODY}\n{SHORT_TRAILER}"));
        assert_eq!(options.label, DEFAULT_LABEL);
        body = filtered.to_owned();
        options.recover_filtered_trailer(&mut body);
        assert_eq!(options.trailer.as_deref(), expected);
        assert_eq!(
            body,
            if expected.is_some() {
                TEST_BODY
            } else {
                filtered
            }
        );
    }

    fn map_store() -> (Arc<MapBackend>, OffloadStore) {
        let backend = Arc::new(MapBackend::default());
        let store = OffloadStore::new(Box::new(Arc::clone(&backend)));
        (backend, store)
    }

    fn numbered(lines: usize) -> String {
        (1..=lines)
            .map(|i| format!("line {i}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn opts(shape: PreviewShape, max_lines: usize, max_bytes: usize) -> LimitOpts<'static> {
        LimitOpts {
            trailer: None,
            shape,
            label: DEFAULT_LABEL,
            lines_clipped: false,
            limits: OutputLimits {
                max_lines,
                max_bytes,
                max_line_bytes: 1000,
            },
        }
    }

    fn within(text: &str, opts: &LimitOpts) {
        assert!(
            text.len() <= opts.limits.max_bytes,
            "{} bytes > {}",
            text.len(),
            opts.limits.max_bytes
        );
        assert!(
            line_count(text) <= opts.limits.max_lines,
            "{} lines > {}",
            line_count(text),
            opts.limits.max_lines
        );
    }

    #[test]
    fn within_limits_returns_body_and_trailer() {
        let o = LimitOpts {
            trailer: Some("Exit code: 1"),
            ..opts(PreviewShape::HeadTail, 10, BIG)
        };
        assert_eq!(limit_output("a\nb\n", &o, None), "a\nb\nExit code: 1");
    }

    #[test]
    fn offloads_over_limit_with_footer_and_trailer() {
        let (backend, store) = map_store();
        let body = numbered(100);
        let o = LimitOpts {
            trailer: Some("Exit code: 2"),
            ..opts(PreviewShape::HeadTail, 20, BIG)
        };
        let out = limit_output(&format!("{body}\n"), &o, Some(&store));

        let name = slot_name(&content_hash(&body), 1);
        assert_eq!(
            backend.files.lock().unwrap().get(&name).unwrap(),
            body.as_bytes()
        );
        assert!(out.starts_with("line 1\n"), "{out}");
        assert!(
            out.contains(&format!(
                "all of it saved to {}",
                backend.path(&name).display()
            )),
            "{out}"
        );
        assert!(out.ends_with("]\nExit code: 2"), "{out}");
        within(&out, &o);
    }

    #[test]
    fn identical_body_returns_pointer_only() {
        let (backend, store) = map_store();
        let o = opts(PreviewShape::Head, 5, BIG);
        let body = numbered(50);
        limit_output(&body, &o, Some(&store));
        let again = limit_output(&body, &o, Some(&store));

        assert!(again.starts_with(OFFLOAD_POINTER_PREFIX), "{again}");
        assert!(!again.contains("line 1\n"), "no preview with a pointer");
        assert_eq!(backend.files.lock().unwrap().len(), 1);
    }

    #[test]
    fn trailing_newline_variants_dedupe() {
        let (backend, store) = map_store();
        let o = opts(PreviewShape::Head, 5, BIG);
        let body = numbered(50);
        limit_output(&body, &o, Some(&store));
        let again = limit_output(&format!("{body}\n\n"), &o, Some(&store));
        assert!(again.starts_with(OFFLOAD_POINTER_PREFIX), "{again}");
        assert_eq!(backend.files.lock().unwrap().len(), 1);
    }

    #[test]
    fn edited_artifact_gets_new_slot() {
        let (backend, store) = map_store();
        let body = numbered(50);
        let first = store.put(&body).unwrap();
        backend
            .files
            .lock()
            .unwrap()
            .insert(first.name.clone(), b"edited".to_vec());

        let second = store.put(&body).unwrap();
        assert_eq!(second.outcome, PutOutcome::Created);
        assert_eq!(second.name, slot_name(&content_hash(&body), 2));
        assert_eq!(backend.files.lock().unwrap()[&first.name], b"edited");
    }

    #[test]
    fn dedup_finds_higher_slot_after_lower_deleted() {
        let (backend, store) = map_store();
        let body = numbered(50);
        let first = store.put(&body).unwrap();
        backend
            .files
            .lock()
            .unwrap()
            .insert(first.name.clone(), b"edited".to_vec());
        let second = store.put(&body).unwrap();
        backend.files.lock().unwrap().remove(&first.name);

        let third = store.put(&body).unwrap();
        assert_eq!(third.outcome, PutOutcome::Existing);
        assert_eq!(third.name, second.name);
    }

    #[test]
    fn dedup_at_full_quota_returns_existing() {
        let (backend, store) = map_store();
        let body = numbered(50);
        store.put(&body).unwrap();
        backend.files.lock().unwrap().insert(
            "filler".to_owned(),
            vec![0; MAX_OFFLOAD_SESSION_BYTES as usize],
        );
        assert_eq!(store.put(&body).unwrap().outcome, PutOutcome::Existing);
        assert!(matches!(store.put("other"), Err(OffloadError::Quota)));
    }

    #[test]
    fn capped_body_footer_and_pointer_disclose_sizes() {
        let (backend, store) = map_store();
        let body = "z\n".repeat(MAX_OFFLOAD_FILE_BYTES);
        let body = body.trim_end_matches('\n');
        let o = opts(PreviewShape::Head, 10, BIG);
        let out = limit_output(body, &o, Some(&store));

        let saved = human_size(MAX_OFFLOAD_FILE_BYTES);
        let discarded = human_size(body.len() - MAX_OFFLOAD_FILE_BYTES);
        assert!(out.contains(&format!("first {saved} saved to")), "{out}");
        assert!(
            out.contains(&format!("remaining {discarded} discarded")),
            "{out}"
        );
        let again = limit_output(body, &o, Some(&store));
        assert!(
            again.contains(&format!("(first {saved} saved, {discarded} discarded)")),
            "{again}"
        );

        let files = backend.files.lock().unwrap();
        let stored = files.values().next().unwrap();
        assert!(stored.starts_with(&body.as_bytes()[..MAX_OFFLOAD_FILE_BYTES]));
        assert_eq!(
            files.keys().next().unwrap(),
            &slot_name(&content_hash(body), 1),
            "the hash covers the full body"
        );
    }

    #[test]
    fn no_store_falls_back_to_truncated_head() {
        let o = opts(PreviewShape::HeadTail, 5, BIG);
        let out = limit_output(&numbered(50), &o, None);
        assert!(out.starts_with("line 1\n"), "{out}");
        assert!(out.ends_with(FILE_TRUNCATED_MARKER), "{out}");
        within(&out, &o);
    }

    #[test]
    fn put_error_falls_back_with_reason() {
        let backend = Arc::new(MapBackend {
            fail: true,
            ..Default::default()
        });
        let store = OffloadStore::new(Box::new(backend));
        let o = opts(PreviewShape::Head, 5, BIG);
        let out = limit_output(&numbered(50), &o, Some(&store));
        assert!(out.contains("full output not saved: disk full"), "{out}");
        within(&out, &o);
    }

    #[test_case(PreviewShape::Head, 10, 400, SHORT_TRAILER ; "head_even")]
    #[test_case(PreviewShape::Head, 7, 333, SHORT_TRAILER ; "head_odd")]
    #[test_case(PreviewShape::HeadTail, 10, 400, SHORT_TRAILER ; "head_tail_even")]
    #[test_case(PreviewShape::HeadTail, 7, 333, SHORT_TRAILER ; "head_tail_odd")]
    #[test_case(PreviewShape::HeadTail, 4, 260, SHORT_TRAILER ; "head_tail_tiny")]
    #[test_case(PreviewShape::HeadTail, 30, 900, LONG_TRAILER ; "head_tail_long_trailer")]
    #[test_case(PreviewShape::Head, 30, 900, LONG_TRAILER ; "head_long_trailer")]
    fn total_output_within_limits(
        shape: PreviewShape,
        max_lines: usize,
        max_bytes: usize,
        trailer: &'static str,
    ) {
        let o = LimitOpts {
            trailer: Some(trailer),
            ..opts(shape, max_lines, max_bytes)
        };
        for body in [
            numbered(200),
            format!("{}\n{}", "a".repeat(5000), numbered(20)),
            format!("{}\n{}", numbered(20), "b".repeat(5000)),
            format!("{}\n{}", "é".repeat(3000), "日".repeat(2000)),
        ] {
            let (_, store) = map_store();
            for store in [None, Some(&store)] {
                let out = limit_output(&body, &o, store);
                // A preview is always joined to the metadata by a blank
                // line; without one the result is metadata alone, which
                // may exceed the limits by design.
                if out.contains("\n\n") {
                    within(&out, &o);
                }
                assert!(out.ends_with(trailer), "{out}");
            }
        }
    }

    #[test]
    fn head_tail_keeps_both_ends_and_counts_omitted() {
        let o = opts(PreviewShape::HeadTail, 9, BIG);
        let (_, store) = map_store();
        let out = limit_output(&numbered(100), &o, Some(&store));
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "line 1");
        assert!(lines.contains(&"line 100"), "{out}");
        assert!(
            lines.contains(&omission_line(100 - lines.len() + 3).as_str()),
            "{out}"
        );
        within(&out, &o);
    }

    #[test]
    fn head_tail_odd_budget_adds_no_extra_line() {
        let o = opts(PreviewShape::HeadTail, 8, BIG);
        let (_, store) = map_store();
        let out = limit_output(&numbered(100), &o, Some(&store));
        assert_eq!(line_count(&out), 8, "{out}");
    }

    #[test_case(true ; "head_cut_keeps_start")]
    #[test_case(false ; "tail_cut_keeps_end")]
    fn oversized_line_cut_on_utf8_boundary(from_start: bool) {
        let line = "é".repeat(500);
        let cut = cut_line(&line, 101, from_start).unwrap();
        assert!(cut.len() <= 101);
        let marker_at = cut.find(LINE_CUT_PREFIX).unwrap();
        assert!(cut[..marker_at].chars().all(|c| c == 'é'));
        assert!(cut.ends_with(" bytes]"), "marker ends the line: {cut}");
        assert!(crate::tools::trailing_truncation_marker(&cut).is_some());
    }

    #[test]
    fn segment_omitted_when_cut_marker_cannot_fit() {
        assert_eq!(cut_line(&"x".repeat(500), 10, true), None);
        let out = limit_output(&"x".repeat(500), &opts(PreviewShape::Head, 10, 45), None);
        assert_eq!(out, FILE_TRUNCATED_MARKER);
    }

    #[test]
    fn metadata_kept_when_over_limits() {
        let (_, store) = map_store();
        let o = LimitOpts {
            trailer: Some("Exit code: 4"),
            ..opts(PreviewShape::HeadTail, 2, 30)
        };
        let out = limit_output(&numbered(100), &o, Some(&store));
        assert!(out.starts_with(OFFLOAD_FOOTER_PREFIX), "{out}");
        assert!(out.ends_with("Exit code: 4"), "{out}");
    }

    #[test_case(None ; "no_store")]
    #[test_case(Some(true) ; "io_error")]
    #[test_case(Some(false) ; "closed")]
    fn fallback_tiny_budget_keeps_metadata(failing: Option<bool>) {
        let store = failing.map(|io_error| {
            let backend = Arc::new(MapBackend {
                fail: io_error,
                ..Default::default()
            });
            let store = OffloadStore::new(Box::new(backend));
            if !io_error {
                store.close_and_remove().unwrap();
            }
            store
        });
        let o = LimitOpts {
            trailer: Some("Exit code: 5"),
            ..opts(PreviewShape::Head, 2, 20)
        };
        let out = limit_output(&numbered(100), &o, store.as_ref());
        assert!(out.starts_with(FILE_TRUNCATED_MARKER), "{out}");
        assert!(out.ends_with("Exit code: 5"), "{out}");
    }

    #[test]
    fn quota_error_falls_back_with_reason() {
        let (backend, store) = map_store();
        backend.files.lock().unwrap().insert(
            "filler".to_owned(),
            vec![0; MAX_OFFLOAD_SESSION_BYTES as usize],
        );
        let out = limit_output(
            &numbered(50),
            &opts(PreviewShape::Head, 5, BIG),
            Some(&store),
        );
        assert!(out.contains(&OffloadError::Quota.to_string()), "{out}");
    }

    #[test]
    fn long_line_body_gets_bash_advice() {
        let (_, store) = map_store();
        let body = format!("{}\n{}", "j".repeat(2000), numbered(50));
        let out = limit_output(&body, &opts(PreviewShape::Head, 5, BIG), Some(&store));
        assert!(out.contains(BASH_ADVICE), "{out}");
        let plain = limit_output(
            &numbered(60),
            &opts(PreviewShape::Head, 5, BIG),
            Some(&store),
        );
        assert!(plain.contains(READ_ADVICE), "{plain}");
    }

    #[test_case("/plain/path.txt", None ; "plain")]
    #[test_case("/with space/p.txt", Some("'/with space/p.txt'") ; "space")]
    #[test_case("/it's/p.txt", Some(r"'/it'\''s/p.txt'") ; "quote")]
    fn shell_quoted_path_in_advice(path: &str, expected: Option<&str>) {
        assert_eq!(shell_quoted(path).as_deref(), expected);
    }

    #[test]
    fn notices_are_recognised() {
        let (_, store) = map_store();
        let o = LimitOpts {
            label: "search results",
            ..opts(PreviewShape::Head, 5, BIG)
        };
        let footer = limit_output(&numbered(50), &o, Some(&store));
        let pointer = limit_output(&numbered(50), &o, Some(&store));
        assert!(
            is_offload_notice(footer.lines().last().unwrap()),
            "{footer}"
        );
        assert!(is_offload_notice(&pointer), "{pointer}");
        assert!(!is_offload_notice(FILE_TRUNCATED_MARKER));
        assert!(!is_offload_notice("No files found"));
    }

    #[test]
    fn clipped_lines_are_disclosed() {
        let (_, store) = map_store();
        let o = LimitOpts {
            label: "search results",
            lines_clipped: true,
            ..opts(PreviewShape::Head, 5, BIG)
        };
        let out = limit_output(&numbered(50), &o, Some(&store));
        assert!(out.contains("[search results truncated: "), "{out}");
        assert!(out.contains(CLIPPED_NOTE), "{out}");
    }

    #[test]
    fn concurrent_puts_respect_quota() {
        const THREADS: usize = 8;
        let backend = Arc::new(MapBackend::default());
        let filler = MAX_OFFLOAD_SESSION_BYTES as usize - 3 * 1000;
        backend
            .files
            .lock()
            .unwrap()
            .insert("filler".to_owned(), vec![0; filler]);
        let store = Arc::new(OffloadStore::new(Box::new(Arc::clone(&backend))));
        let barrier = Arc::new(Barrier::new(THREADS));
        let handles: Vec<_> = (0..THREADS)
            .map(|i| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    store.put(&format!("{i}{}", "q".repeat(999))).is_ok()
                })
            })
            .collect();
        let stored = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|ok| *ok)
            .count();
        assert_eq!(stored, 3);
        assert!(backend.snapshot().unwrap().total_bytes <= MAX_OFFLOAD_SESSION_BYTES);
    }

    #[test]
    fn quota_counts_stored_capped_size() {
        let (backend, store) = map_store();
        let room = MAX_OFFLOAD_FILE_BYTES + 100;
        backend.files.lock().unwrap().insert(
            "filler".to_owned(),
            vec![0; MAX_OFFLOAD_SESSION_BYTES as usize - room],
        );
        let huge = "h".repeat(MAX_OFFLOAD_FILE_BYTES * 2);
        assert_eq!(store.put(&huge).unwrap().outcome, PutOutcome::Created);
    }

    #[test]
    fn quota_reflects_grow_shrink_delete() {
        let dir = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(dir.path().join("store"));
        let first = store.put(&numbered(10)).unwrap();
        let path = store.path_of(&first);
        let max = MAX_OFFLOAD_SESSION_BYTES as usize;

        fs::write(&path, vec![b'g'; max]).unwrap();
        assert!(matches!(store.put("grown"), Err(OffloadError::Quota)));
        fs::write(&path, b"shrunk").unwrap();
        assert_eq!(
            store.put("after shrink").unwrap().outcome,
            PutOutcome::Created
        );
        fs::write(&path, vec![b'g'; max]).unwrap();
        fs::remove_file(&path).unwrap();
        assert_eq!(
            store.put("after delete").unwrap().outcome,
            PutOutcome::Created
        );
    }

    #[cfg(unix)]
    #[test]
    fn disk_backend_creates_private_files() {
        use std::os::unix::fs::PermissionsExt;
        let dir = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(dir.path().join("store"));
        let saved = store.put("secret").unwrap();
        let mode = |p: &Path| fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&store.path_of(&saved)), 0o600);
        assert_eq!(mode(&dir.path().join("store")), DIR_MODE);
    }

    #[test]
    fn close_and_remove_removes_dir() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("store");
        let store = OffloadStore::on_disk(root.clone());
        store.put("x").unwrap();
        store.close_and_remove().unwrap();
        assert!(!root.exists());
    }

    #[test]
    fn put_after_close_fails_without_recreating_dir() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("store");
        let store = OffloadStore::on_disk(root.clone());
        store.close_and_remove().unwrap();
        assert!(matches!(store.put("late"), Err(OffloadError::Closed)));
        assert!(!root.exists());
    }

    /// Parks `create_new` until released, so a close can be raced against
    /// an in-flight put.
    struct GatedBackend {
        inner: Arc<MapBackend>,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    impl OffloadBackend for GatedBackend {
        fn matches(&self, name: &str, expected: &[u8]) -> io::Result<bool> {
            self.inner.matches(name, expected)
        }
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            self.events.lock().unwrap().push("create");
            self.inner.create_new(name, bytes)
        }
        fn snapshot(&self) -> io::Result<OffloadSnapshot> {
            self.inner.snapshot()
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
    fn close_waits_for_in_flight_put() {
        let inner = Arc::new(MapBackend::default());
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
            thread::spawn(move || store.put("in flight").is_ok())
        };
        entered_rx.recv().unwrap();
        let (closing_tx, closing_rx) = flume::unbounded();
        let closer = {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                closing_tx.send(()).unwrap();
                store.close_and_remove().unwrap();
            })
        };
        closing_rx.recv().unwrap();
        release_tx.send(()).unwrap();
        assert!(putter.join().unwrap(), "the in-flight put completes");
        closer.join().unwrap();
        assert_eq!(
            *events.lock().unwrap(),
            ["create", "remove"],
            "close must wait for the in-flight put"
        );
        assert!(
            inner.files.lock().unwrap().is_empty(),
            "removed after the put"
        );
        assert!(matches!(store.put("late"), Err(OffloadError::Closed)));
    }

    #[test_case(-1; "shrinking_reader")]
    #[test_case(0; "equal_reader")]
    #[test_case(1; "growing_reader")]
    fn comparison_reader_is_bounded(delta: isize) {
        struct CountingReader {
            reader: Cursor<Vec<u8>>,
            bytes: usize,
            largest_request: usize,
        }
        impl Read for CountingReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                self.largest_request = self.largest_request.max(buffer.len());
                let read = self.reader.read(buffer)?;
                self.bytes += read;
                Ok(read)
            }
        }
        let expected = vec![b'x'; COMPARE_CHUNK_BYTES * 2];
        let actual_len = expected.len().checked_add_signed(delta).unwrap();
        let mut reader = CountingReader {
            reader: Cursor::new(vec![b'x'; actual_len]),
            bytes: 0,
            largest_request: 0,
        };
        assert_eq!(compare_reader(&mut reader, &expected).unwrap(), delta == 0);
        assert!(reader.bytes <= expected.len() + 1);
        assert!(reader.largest_request <= COMPARE_CHUNK_BYTES);
    }

    #[test_case(-1; "shrunk_after_read")]
    #[test_case(1; "grown_after_read")]
    fn comparison_rechecks_opened_handle_length(delta: isize) {
        let expected = TEST_BODY.as_bytes();
        let mut lengths = [
            expected.len() as u64,
            expected.len().checked_add_signed(delta).unwrap() as u64,
        ]
        .into_iter();
        assert!(
            !matches_reader(Cursor::new(expected), expected, || {
                Ok(lengths.next().unwrap())
            })
            .unwrap()
        );
    }

    #[test]
    fn comparison_rejects_wrong_initial_length_without_reading() {
        struct NeverRead;
        impl Read for NeverRead {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                panic!("initial size mismatch must not read")
            }
        }
        assert!(!matches_reader(NeverRead, TEST_BODY.as_bytes(), || Ok(0)).unwrap());
    }

    #[test]
    fn comparison_propagates_read_errors() {
        struct FailedReader;
        impl Read for FailedReader {
            fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::from(ErrorKind::PermissionDenied))
            }
        }
        assert_eq!(
            compare_reader(&mut FailedReader, TEST_BODY.as_bytes())
                .unwrap_err()
                .kind(),
            ErrorKind::PermissionDenied
        );
    }

    #[cfg(any(unix, windows))]
    fn symlink_file(target: &Path, link: &Path) {
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, link).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(target, link).unwrap();
    }

    #[cfg(any(unix, windows))]
    fn symlink_dir(target: &Path, link: &Path) {
        #[cfg(unix)]
        std::os::unix::fs::symlink(target, link).unwrap();
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(target, link).unwrap();
    }

    #[test_case("../outside"; "parent_traversal")]
    #[test_case("nested/slot"; "nested_path")]
    #[test_case("nested\\slot"; "windows_separator")]
    #[test_case("slot:stream"; "windows_stream")]
    #[test_case(""; "empty")]
    #[test_case("."; "current_directory")]
    #[test_case(".."; "parent_directory")]
    #[test_case("slot\0"; "nul")]
    fn disk_backend_rejects_non_component_names(name: &str) {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        assert_eq!(
            backend
                .create_new(name, TEST_BODY.as_bytes())
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            backend
                .matches(name, TEST_BODY.as_bytes())
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[cfg(any(unix, windows))]
    #[test_case(false; "existing_target")]
    #[test_case(true; "dangling_target")]
    fn symlink_store_directory_rejects_saves(dangling: bool) {
        let root = TempDir::new().unwrap();
        let target = root.path().join("outside");
        if !dangling {
            fs::create_dir(&target).unwrap();
        }
        #[cfg(unix)]
        let permissions = if dangling {
            None
        } else {
            use std::os::unix::fs::PermissionsExt;
            const TARGET_MODE: u32 = 0o755;
            fs::set_permissions(&target, fs::Permissions::from_mode(TARGET_MODE)).unwrap();
            Some(fs::metadata(&target).unwrap().permissions().mode())
        };
        let path = root.path().join("store");
        symlink_dir(&target, &path);
        let store = OffloadStore::on_disk(path.clone());
        assert!(matches!(store.put(TEST_BODY), Err(OffloadError::Io(_))));
        assert!(
            store
                .backend
                .create_new("slot", TEST_BODY.as_bytes())
                .is_err()
        );
        assert!(store.backend.matches("slot", TEST_BODY.as_bytes()).is_err());
        assert_eq!(fs::read_link(path).unwrap(), target);
        if dangling {
            assert!(!target.exists());
        } else {
            assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&target).unwrap().permissions().mode(),
                    permissions.unwrap()
                );
            }
        }
    }

    #[cfg(unix)]
    #[test_case(SESSIONS_DIR, true; "sessions_parent")]
    #[test_case("sessions/offload", true; "offload_parent")]
    #[test_case("sessions/offload", false; "arbitrary_disk_path")]
    fn parent_symlink_rejects_operations_without_external_changes(
        parent: &str,
        session_store: bool,
    ) {
        use std::os::unix::fs::PermissionsExt;
        const TARGET_MODE: u32 = 0o755;
        let root = TempDir::new().unwrap();
        let session = SessionRef::from(maki_storage::id::MakiId::generate());
        let relative = offload_dir(Path::new(SESSIONS_DIR), session.id());
        let external = root.path().join("outside");
        let external_leaf = external.join(relative.strip_prefix(parent).unwrap());
        fs::create_dir_all(&external_leaf).unwrap();
        fs::set_permissions(&external_leaf, fs::Permissions::from_mode(TARGET_MODE)).unwrap();
        let permissions = fs::metadata(&external_leaf).unwrap().permissions().mode();
        let link = root.path().join(parent);
        fs::create_dir_all(link.parent().unwrap()).unwrap();
        symlink_dir(&external, &link);
        let store = if session_store {
            OffloadStore::for_session(root.path(), &session)
        } else {
            OffloadStore::on_disk(root.path().join(&relative))
        };
        assert!(matches!(store.put(TEST_BODY), Err(OffloadError::Io(_))));
        assert!(
            store
                .backend
                .create_new("slot", TEST_BODY.as_bytes())
                .is_err()
        );
        assert!(store.backend.matches("slot", TEST_BODY.as_bytes()).is_err());
        assert!(store.backend.snapshot().is_err());
        assert!(store.close_and_remove().is_err());
        assert_eq!(fs::read_dir(&external_leaf).unwrap().count(), 0);
        assert_eq!(
            fs::metadata(&external_leaf).unwrap().permissions().mode(),
            permissions
        );
        assert_eq!(fs::read_link(link).unwrap(), external);
    }

    #[cfg(unix)]
    #[test_case("../outside"; "parent_escape")]
    #[test_case("nested/../../outside"; "nested_parent_escape")]
    fn disk_directory_rejects_parent_components(relative: &str) {
        let root = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(root.path().join(relative));
        assert!(matches!(store.put(TEST_BODY), Err(OffloadError::Io(_))));
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn cleanup_rejects_empty_managed_suffix_without_deleting_root_contents() {
        let root = TempDir::new().unwrap();
        fs::write(root.path().join("slot"), TEST_BODY).unwrap();
        #[cfg(unix)]
        let handle = File::from(
            anchored::open(
                root.path(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .unwrap(),
        );
        #[cfg(windows)]
        let handle = windows::trusted_root(root.path()).unwrap();
        let backend = DiskBackend {
            dir: root.path().to_owned(),
            root: Ok(handle),
            relative: PathBuf::new(),
        };
        assert_eq!(
            backend.remove_all().unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            fs::read(root.path().join("slot")).unwrap(),
            TEST_BODY.as_bytes()
        );
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_cleanup_does_not_follow_nested_symlinks() {
        let root = TempDir::new().unwrap();
        let external = root.path().join("outside");
        fs::create_dir(&external).unwrap();
        fs::write(external.join("slot"), TEST_BODY).unwrap();
        let store = OffloadStore::on_disk(root.path().join("store"));
        store.put(TEST_BODY).unwrap();
        fs::create_dir(store.dir().join("nested")).unwrap();
        symlink_dir(&external, &store.dir().join("nested/link"));
        store.close_and_remove().unwrap();
        assert!(!store.dir().exists());
        assert_eq!(
            fs::read(external.join("slot")).unwrap(),
            TEST_BODY.as_bytes()
        );
    }

    #[cfg(unix)]
    #[test]
    fn trusted_state_root_symlink_is_anchored_at_construction() {
        let root = TempDir::new().unwrap();
        let original = root.path().join("state");
        let external = root.path().join("outside");
        fs::create_dir(&original).unwrap();
        fs::create_dir(&external).unwrap();
        let link = root.path().join("state-link");
        symlink_dir(&original, &link);
        let session = SessionRef::from(maki_storage::id::MakiId::generate());
        let store = OffloadStore::for_session(&link, &session);
        fs::remove_file(&link).unwrap();
        symlink_dir(&external, &link);
        let saved = store.put(TEST_BODY).unwrap();
        assert_eq!(fs::read_dir(&external).unwrap().count(), 0);
        assert_eq!(
            fs::read(offload_dir(&original.join(SESSIONS_DIR), session.id()).join(saved.name))
                .unwrap(),
            TEST_BODY.as_bytes()
        );
        store.close_and_remove().unwrap();
    }

    #[cfg(unix)]
    #[test_case(SESSIONS_DIR; "sessions")]
    #[test_case("sessions/offload"; "offload")]
    fn ancestor_replacement_during_traversal_cannot_redirect_descendants(parent: &str) {
        use std::os::unix::fs::PermissionsExt;
        const TARGET_MODE: u32 = 0o755;
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join(parent));
        let opened = backend.open_dir(true).unwrap();
        let moved = root.path().join("original");
        fs::rename(&backend.dir, &moved).unwrap();
        let target = root.path().join("outside");
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(TARGET_MODE)).unwrap();
        let permissions = fs::metadata(&target).unwrap().permissions().mode();
        symlink_dir(&target, &backend.dir);
        let remaining = if parent == SESSIONS_DIR {
            "offload/session"
        } else {
            "session"
        };
        let leaf = DiskBackend::walk_dir(&opened, Path::new(remaining), true).unwrap();
        assert!(DiskBackend::create_in(&leaf, "slot", TEST_BODY.as_bytes()).unwrap());
        assert_eq!(
            fs::read(moved.join(remaining).join("slot")).unwrap(),
            TEST_BODY.as_bytes()
        );
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode(),
            permissions
        );
        assert!(backend.open_dir(true).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn directory_swap_after_open_does_not_redirect_chmod_or_save() {
        use std::os::unix::fs::PermissionsExt;
        const TARGET_MODE: u32 = 0o755;
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        let opened = backend.open_dir(true).unwrap();
        let original = root.path().join("original");
        fs::rename(&backend.dir, &original).unwrap();
        let target = root.path().join("outside");
        fs::create_dir(&target).unwrap();
        fs::set_permissions(&target, fs::Permissions::from_mode(TARGET_MODE)).unwrap();
        let permissions = fs::metadata(&target).unwrap().permissions().mode();
        symlink_dir(&target, &backend.dir);
        assert!(DiskBackend::create_in(&opened, "slot", TEST_BODY.as_bytes()).unwrap());
        assert!(!DiskBackend::create_in(&opened, "slot", b"replacement").unwrap());
        assert_eq!(
            fs::read(original.join("slot")).unwrap(),
            TEST_BODY.as_bytes()
        );
        assert_eq!(fs::read_dir(&original).unwrap().count(), 1);
        assert_eq!(
            fs::metadata(&target).unwrap().permissions().mode(),
            permissions
        );
        assert_eq!(fs::read_dir(&target).unwrap().count(), 0);
        assert!(backend.create_new("other", TEST_BODY.as_bytes()).is_err());
        assert!(backend.snapshot().is_err());
        assert!(backend.matches("slot", TEST_BODY.as_bytes()).is_err());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn symlink_swap_before_open_is_not_followed() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().join("store"));
        backend.create_new("slot", TEST_BODY.as_bytes()).unwrap();
        let snapshot = backend.snapshot().unwrap();
        let path = backend.path("slot");
        let target = root.path().join("outside");
        fs::write(&target, TEST_BODY).unwrap();
        fs::remove_file(&path).unwrap();
        symlink_file(&target, &path);
        assert!(snapshot.names.iter().any(|name| name == "slot"));
        assert!(!backend.matches("slot", TEST_BODY.as_bytes()).unwrap());
        assert_eq!(backend.snapshot().unwrap().total_bytes, 0);
        assert_eq!(fs::read(&target).unwrap(), TEST_BODY.as_bytes());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn replacement_after_open_does_not_change_compared_handle() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().to_owned());
        backend.create_new("slot", TEST_BODY.as_bytes()).unwrap();
        let opened = open_regular(&backend.path("slot")).unwrap().unwrap();
        fs::rename(backend.path("slot"), backend.path("original")).unwrap();
        let target = root.path().join("outside");
        fs::write(&target, "replacement").unwrap();
        symlink_file(&target, &backend.path("slot"));
        assert!(matches_open_file(opened, TEST_BODY.as_bytes()).unwrap());
        assert!(!backend.matches("slot", TEST_BODY.as_bytes()).unwrap());
        assert_eq!(fs::read(&target).unwrap(), b"replacement");
    }

    #[cfg(any(unix, windows))]
    #[test_case("directory"; "directory")]
    #[test_case("file_link"; "file_link")]
    #[test_case("directory_link"; "directory_link")]
    #[test_case("dangling_link"; "dangling_link")]
    #[test_case("oversized"; "oversized")]
    fn wrong_disk_occupants_are_preserved(kind: &str) {
        let root = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(root.path().join("store"));
        let first = store.put(TEST_BODY).unwrap();
        let path = store.path_of(&first);
        fs::remove_file(&path).unwrap();
        let target = root.path().join("outside");
        let expected_bytes = match kind {
            "directory" => {
                fs::create_dir(&path).unwrap();
                0
            }
            "file_link" => {
                fs::write(&target, TEST_BODY).unwrap();
                symlink_file(&target, &path);
                0
            }
            "directory_link" => {
                fs::create_dir(&target).unwrap();
                symlink_dir(&target, &path);
                0
            }
            "dangling_link" => {
                symlink_file(&target, &path);
                0
            }
            "oversized" => {
                let file = File::create(&path).unwrap();
                let len = MAX_OFFLOAD_FILE_BYTES as u64 + 1;
                file.set_len(len).unwrap();
                len
            }
            _ => unreachable!(),
        };
        let snapshot = store.backend.snapshot().unwrap();
        assert!(snapshot.names.contains(&first.name));
        assert_eq!(snapshot.total_bytes, expected_bytes);
        let next = store.put(TEST_BODY).unwrap();
        assert_eq!(next.outcome, PutOutcome::Created);
        assert_ne!(next.name, first.name);
        match kind {
            "directory" => assert!(path.is_dir()),
            "oversized" => assert_eq!(fs::metadata(&path).unwrap().len(), expected_bytes),
            _ => assert_eq!(fs::read_link(&path).unwrap(), target),
        }
        if kind == "file_link" {
            assert_eq!(fs::read(&target).unwrap(), TEST_BODY.as_bytes());
        }
    }

    #[cfg(unix)]
    #[test_case(false; "fifo")]
    #[test_case(true; "socket")]
    fn special_occupants_do_not_block_and_are_preserved(socket: bool) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::{FileTypeExt, OpenOptionsExt};
        use std::os::unix::net::UnixListener;

        struct SupervisedPut {
            path: PathBuf,
            fifo: Option<File>,
            worker: Option<thread::JoinHandle<()>>,
        }
        impl Drop for SupervisedPut {
            fn drop(&mut self) {
                if let Some(worker) = self.worker.take() {
                    if self.fifo.is_some() {
                        // Release a regressed reader whether it is opening or reading the FIFO.
                        let _ = fs::remove_file(&self.path);
                        let _ = fs::write(&self.path, TEST_BODY);
                        self.fifo.take();
                    }
                    let _ = worker.join();
                }
            }
        }
        let root = TempDir::new().unwrap();
        let store = Arc::new(OffloadStore::on_disk(root.path().join("store")));
        let first = store.put(TEST_BODY).unwrap();
        let path = store.path_of(&first);
        fs::remove_file(&path).unwrap();
        let listener = socket.then(|| UnixListener::bind(&path).unwrap());
        let fifo = if socket {
            None
        } else {
            let name = CString::new(path.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), DIR_MODE) }, 0);
            Some(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&path)
                    .unwrap(),
            )
        };
        let (done_tx, done_rx) = flume::bounded(1);
        let worker = thread::spawn({
            let store = Arc::clone(&store);
            move || {
                done_tx.send(store.put(TEST_BODY)).unwrap();
            }
        });
        let mut supervised = SupervisedPut {
            path: path.clone(),
            fifo,
            worker: Some(worker),
        };
        let saved = done_rx
            .recv_timeout(GATE_TIMEOUT)
            .expect("special occupant blocked put")
            .unwrap();
        supervised.worker.take().unwrap().join().unwrap();
        assert_eq!(saved.outcome, PutOutcome::Created);
        assert_ne!(saved.name, first.name);
        let kind = fs::symlink_metadata(&path).unwrap().file_type();
        assert!(if socket {
            kind.is_socket()
        } else {
            kind.is_fifo()
        });
        drop(listener);
    }

    struct CountingBackend {
        inner: Arc<MapBackend>,
        snapshots: Arc<AtomicUsize>,
        collisions: AtomicUsize,
    }

    impl OffloadBackend for CountingBackend {
        fn matches(&self, name: &str, expected: &[u8]) -> io::Result<bool> {
            self.inner.matches(name, expected)
        }
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            let collisions = self.collisions.load(Ordering::SeqCst);
            if collisions > 0 {
                self.collisions.fetch_sub(1, Ordering::SeqCst);
                self.inner
                    .files
                    .lock()
                    .unwrap()
                    .insert(name.to_owned(), Vec::new());
                return Ok(false);
            }
            self.inner.create_new(name, bytes)
        }
        fn snapshot(&self) -> io::Result<OffloadSnapshot> {
            self.snapshots.fetch_add(1, Ordering::SeqCst);
            self.inner.snapshot()
        }
        fn remove_all(&self) -> io::Result<()> {
            self.inner.remove_all()
        }
        fn path(&self, name: &str) -> PathBuf {
            self.inner.path(name)
        }
    }

    #[test_case(0; "ordinary_put")]
    #[test_case(1; "collision_resnapshots")]
    fn put_takes_one_snapshot_per_attempt(collisions: usize) {
        let inner = Arc::new(MapBackend::default());
        let snapshots = Arc::new(AtomicUsize::new(0));
        let store = OffloadStore::new(Box::new(CountingBackend {
            inner,
            snapshots: Arc::clone(&snapshots),
            collisions: AtomicUsize::new(collisions),
        }));
        let first = store.put(TEST_BODY).unwrap();
        assert_eq!(first.outcome, PutOutcome::Created);
        assert_eq!(snapshots.load(Ordering::SeqCst), collisions + 1);
        snapshots.store(0, Ordering::SeqCst);
        assert_eq!(store.put(TEST_BODY).unwrap().outcome, PutOutcome::Existing);
        assert_eq!(snapshots.load(Ordering::SeqCst), 1);
    }

    struct CleanupBackend {
        put_entered: flume::Sender<()>,
        put_release: flume::Receiver<()>,
        remove_entered: flume::Sender<()>,
        remove_release: flume::Receiver<()>,
        events: Arc<Mutex<Vec<&'static str>>>,
        fail: bool,
    }

    impl OffloadBackend for CleanupBackend {
        fn matches(&self, _name: &str, _expected: &[u8]) -> io::Result<bool> {
            Ok(false)
        }
        fn snapshot(&self) -> io::Result<OffloadSnapshot> {
            Ok(OffloadSnapshot {
                names: Vec::new(),
                total_bytes: 0,
            })
        }
        fn create_new(&self, _name: &str, _bytes: &[u8]) -> io::Result<bool> {
            self.put_entered.send(()).unwrap();
            self.put_release.recv().unwrap();
            self.events.lock().unwrap().push(CREATE_EVENT);
            Ok(true)
        }
        fn remove_all(&self) -> io::Result<()> {
            self.remove_entered.send(()).unwrap();
            self.remove_release.recv().unwrap();
            self.events.lock().unwrap().push(REMOVE_EVENT);
            if self.fail {
                Err(io::Error::other(REMOVE_ERROR))
            } else {
                Ok(())
            }
        }
        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/offload").join(name)
        }
    }

    struct CleanupDrain {
        cleanup: OffloadCleanup,
        put_release: flume::Sender<()>,
        remove_release: flume::Sender<()>,
        putter: Option<thread::JoinHandle<Result<Saved, OffloadError>>>,
    }

    impl Drop for CleanupDrain {
        fn drop(&mut self) {
            let _ = self.put_release.send(());
            let _ = self.remove_release.send(());
            if let Some(putter) = self.putter.take() {
                let _ = putter.join();
            }
            let _ = smol::block_on(self.cleanup.wait());
        }
    }

    #[test_case(false; "success")]
    #[test_case(true; "failure")]
    fn cleanup_request_is_nonblocking_ordered_and_idempotent(fail: bool) {
        let (put_entered_tx, put_entered_rx) = flume::unbounded();
        let (put_release_tx, put_release_rx) = flume::unbounded();
        let (remove_entered_tx, remove_entered_rx) = flume::unbounded();
        let (remove_release_tx, remove_release_rx) = flume::unbounded();
        let events = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(OffloadStore::new(Box::new(CleanupBackend {
            put_entered: put_entered_tx,
            put_release: put_release_rx,
            remove_entered: remove_entered_tx,
            remove_release: remove_release_rx,
            events: Arc::clone(&events),
            fail,
        })));
        let cleanup = OffloadCleanup::new(Arc::clone(&store));
        let putter = thread::spawn({
            let store = Arc::clone(&store);
            move || store.put(TEST_BODY)
        });
        let mut drain = CleanupDrain {
            cleanup: cleanup.clone(),
            put_release: put_release_tx,
            remove_release: remove_release_tx,
            putter: Some(putter),
        };
        put_entered_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        const REQUESTERS: usize = 4;
        let barrier = Arc::new(Barrier::new(REQUESTERS));
        let (requested_tx, requested_rx) = flume::unbounded();
        let requesters: Vec<_> = (0..REQUESTERS)
            .map(|_| {
                let cleanup = cleanup.clone();
                let barrier = Arc::clone(&barrier);
                let requested_tx = requested_tx.clone();
                thread::spawn(move || {
                    barrier.wait();
                    cleanup.request();
                    drop(cleanup);
                    let _ = requested_tx.send(());
                })
            })
            .collect();
        let requested: Result<Vec<_>, _> = (0..REQUESTERS)
            .map(|_| requested_rx.recv_timeout(GATE_TIMEOUT))
            .collect();
        if requested.is_err() {
            let _ = drain.put_release.send(());
            let _ = drain.remove_release.send(());
        }
        for requester in requesters {
            let _ = requester.join();
        }
        requested.expect("cleanup request or drop blocked on backend I/O");
        let mut wait = Box::pin(cleanup.wait());
        assert!(smol::block_on(futures_lite::future::poll_once(&mut wait)).is_none());
        drop(wait);
        cleanup.request();
        cleanup.clone().request();
        drop(cleanup.clone());
        assert!(matches!(store.put("late"), Err(OffloadError::Closed)));
        assert!(events.lock().unwrap().is_empty());
        drain.put_release.send(()).unwrap();
        drain.putter.take().unwrap().join().unwrap().unwrap();
        remove_entered_rx.recv_timeout(GATE_TIMEOUT).unwrap();
        let mut first_waiter = Box::pin(cleanup.wait());
        assert!(smol::block_on(futures_lite::future::poll_once(&mut first_waiter)).is_none());
        drain.remove_release.send(()).unwrap();
        let first = smol::block_on(first_waiter);
        let late = smol::block_on(cleanup.wait());
        match (first, late) {
            (Ok(()), Ok(())) => assert!(!fail),
            (Err(first), Err(late)) => {
                assert!(fail);
                assert_eq!(first.to_string(), REMOVE_ERROR);
                assert!(Arc::ptr_eq(&first, &late));
            }
            _ => panic!("cleanup waiters disagreed"),
        }
        assert_eq!(*events.lock().unwrap(), [CREATE_EVENT, REMOVE_EVENT]);
        assert!(matches!(store.put("later"), Err(OffloadError::Closed)));
    }

    #[test]
    fn queued_put_rejects_after_closure_without_backend_work() {
        let inner = Arc::new(MapBackend::default());
        let snapshots = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(OffloadStore::new(Box::new(CountingBackend {
            inner,
            snapshots: Arc::clone(&snapshots),
            collisions: AtomicUsize::new(0),
        })));
        let operation = store.operation.lock().unwrap();
        let putter = thread::spawn({
            let store = Arc::clone(&store);
            move || store.put_serialized(TEST_BODY)
        });
        store.request_close();
        drop(operation);
        assert!(matches!(putter.join().unwrap(), Err(OffloadError::Closed)));
        assert_eq!(snapshots.load(Ordering::SeqCst), 0);
    }

    #[test_case(false, false, None, 0; "tiny_short")]
    #[test_case(true, false, Some(SHORT_TRAILER), 0; "tiny_long_trailer")]
    #[test_case(false, true, Some(LONG_TRAILER), 0; "tiny_clipped_trailer")]
    #[test_case(true, true, None, BIG; "long_clipped")]
    #[test_case(false, false, Some(SHORT_TRAILER), BIG; "short_trailer")]
    fn repeated_output_keeps_advice_and_clipping(
        long_lines: bool,
        clipped: bool,
        trailer: Option<&'static str>,
        max_bytes: usize,
    ) {
        let (_, store) = map_store();
        let mut options = opts(PreviewShape::HeadTail, 0, max_bytes);
        options.limits.max_line_bytes = if long_lines { 1 } else { TEST_BODY.len() };
        options.lines_clipped = clipped;
        options.trailer = trailer;
        limit_output(TEST_BODY, &options, Some(&store));
        let output = limit_output(TEST_BODY, &options, Some(&store));
        assert!(output.starts_with(OFFLOAD_POINTER_PREFIX), "{output}");
        assert!(
            output.contains(if long_lines { BASH_ADVICE } else { READ_ADVICE }),
            "{output}"
        );
        assert_eq!(output.contains(CLIPPED_NOTE), clipped, "{output}");
        assert!(!output.starts_with(TEST_BODY), "pointer has no preview");
        if let Some(trailer) = trailer {
            assert!(output.ends_with(trailer), "{output}");
        }
    }

    #[test_case(false; "short_lines")]
    #[test_case(true; "long_lines")]
    fn capped_pointer_advice_uses_only_saved_prefix(long_lines: bool) {
        let (_, store) = map_store();
        let mut body = if long_lines {
            "x".repeat(MAX_OFFLOAD_FILE_BYTES)
        } else {
            "x\n".repeat(MAX_OFFLOAD_FILE_BYTES / 2)
        };
        body.push_str(&"z".repeat(COMPARE_CHUNK_BYTES));
        let mut options = opts(PreviewShape::Head, 0, 0);
        options.limits.max_line_bytes = 2;
        options.lines_clipped = true;
        options.trailer = Some(LONG_TRAILER);
        limit_output(&body, &options, Some(&store));
        let output = limit_output(&body, &options, Some(&store));
        assert!(
            output.contains(if long_lines { BASH_ADVICE } else { READ_ADVICE }),
            "{output}"
        );
        assert!(output.contains(CLIPPED_NOTE), "{output}");
        assert!(output.contains("discarded"), "{output}");
        assert!(output.ends_with(LONG_TRAILER), "{output}");
    }

    #[test]
    fn repeated_long_line_pointer_quotes_shell_path() {
        let root = TempDir::new().unwrap();
        let store = OffloadStore::on_disk(root.path().join("a path's artifacts"));
        let mut options = opts(PreviewShape::Head, 0, 0);
        options.limits.max_line_bytes = 1;
        limit_output(TEST_BODY, &options, Some(&store));
        let output = limit_output(TEST_BODY, &options, Some(&store));
        let saved = store.put(TEST_BODY).unwrap();
        let path = store.path_of(&saved).display().to_string();
        assert!(
            output.contains(&format!("shell path: {}", shell_quoted(&path).unwrap())),
            "{output}"
        );
    }

    #[test]
    fn capped_comparison_includes_note_and_never_reads_beyond_expected_plus_one() {
        struct CountingReader {
            bytes: Vec<u8>,
            consumed: usize,
        }
        impl Read for CountingReader {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let count = buffer.len().min(self.bytes.len() - self.consumed);
                buffer[..count].copy_from_slice(&self.bytes[self.consumed..self.consumed + count]);
                self.consumed += count;
                Ok(count)
            }
        }
        let body = "x".repeat(MAX_OFFLOAD_FILE_BYTES + COMPARE_CHUNK_BYTES);
        let expected = stored_form(&body, MAX_OFFLOAD_FILE_BYTES);
        assert!(expected.len() > MAX_OFFLOAD_FILE_BYTES);
        let mut equal = CountingReader {
            bytes: expected.as_bytes().to_vec(),
            consumed: 0,
        };
        assert!(compare_reader(&mut equal, expected.as_bytes()).unwrap());
        assert_eq!(equal.consumed, expected.len());
        let mut growing = CountingReader {
            bytes: [expected.as_bytes(), body.as_bytes()].concat(),
            consumed: 0,
        };
        assert!(!compare_reader(&mut growing, expected.as_bytes()).unwrap());
        assert_eq!(growing.consumed, expected.len() + 1);
        let mut changed_note = expected.as_bytes().to_vec();
        *changed_note.last_mut().unwrap() = b'!';
        assert!(!compare_reader(&mut Cursor::new(changed_note), expected.as_bytes()).unwrap());
    }

    #[test_case(ErrorKind::NotFound, true; "disappeared")]
    #[test_case(ErrorKind::NotADirectory, true; "not_directory")]
    #[test_case(ErrorKind::PermissionDenied, false; "access_failure")]
    #[test_case(ErrorKind::Other, false; "io_failure")]
    fn expected_occupant_errors_only_are_ignored(kind: ErrorKind, ignored: bool) {
        assert_eq!(not_regular_error(&io::Error::from(kind)), ignored);
    }

    #[cfg(unix)]
    #[test_case(libc::ELOOP; "symlink")]
    #[test_case(libc::ENXIO; "socket")]
    #[test_case(libc::ENODEV; "device")]
    fn unix_special_occupant_errors_are_ignored(code: i32) {
        assert!(not_regular_error(&io::Error::from_raw_os_error(code)));
    }

    #[test]
    fn disappeared_entry_is_ignored_by_snapshot() {
        let root = TempDir::new().unwrap();
        let backend = DiskBackend::new(root.path().to_owned());
        backend.create_new("slot", TEST_BODY.as_bytes()).unwrap();
        let entries = fs::read_dir(&backend.dir)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        fs::remove_file(backend.path("slot")).unwrap();
        assert!(open_regular(&entries[0].path()).unwrap().is_none());
        assert_eq!(backend.snapshot().unwrap().total_bytes, 0);
    }

    #[test]
    fn offload_benchmark_fixtures_reset_per_sample() {
        for artifact_count in [10, 100, 1000] {
            let new_sample = offload_benchmark_fixtures::Fixture::new(artifact_count).unwrap();
            assert_eq!(new_sample.file_count().unwrap(), artifact_count);
            new_sample.put_new().unwrap();
            assert_eq!(new_sample.file_count().unwrap(), artifact_count + 1);

            let duplicate_sample =
                offload_benchmark_fixtures::Fixture::new(artifact_count).unwrap();
            assert_eq!(duplicate_sample.file_count().unwrap(), artifact_count);
            duplicate_sample.put_duplicate().unwrap();
            assert_eq!(duplicate_sample.file_count().unwrap(), artifact_count);

            let sequence_sample = offload_benchmark_fixtures::Fixture::new(artifact_count).unwrap();
            assert_eq!(sequence_sample.file_count().unwrap(), artifact_count);
            sequence_sample.put_sequence().unwrap();
            assert_eq!(sequence_sample.file_count().unwrap(), artifact_count * 2);
        }
    }

    #[test_case(Some("abc") ; "some_session_maps_to_sessions_offload_id")]
    #[test_case(None ; "no_session_is_none")]
    fn offload_dir_for_cases(session: Option<&str>) {
        let session = session.map(|_| SessionRef::from(maki_storage::id::MakiId::generate()));
        let dir = offload_dir_for(Path::new("/state"), session.as_ref());
        match session {
            Some(session) => assert_eq!(
                dir.unwrap(),
                Path::new("/state")
                    .join(SESSIONS_DIR)
                    .join(maki_storage::sessions::OFFLOAD_DIR)
                    .join(session.id().to_string())
            ),
            None => assert_eq!(dir, None),
        }
    }
}
