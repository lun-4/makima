//! Session-scoped store for tool output that would otherwise be cut.
//!
//! Output past the limits is saved whole and the model gets a bounded
//! preview plus the file's path. Files are named by content, and one locked
//! `put` both deduplicates and enforces the session quota, so parallel tools
//! and subagents sharing a store can't race past either.

use std::fs;
use std::io::{self, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use maki_config::AgentConfig;
use maki_storage::id::SessionRef;
use maki_storage::sessions::{SESSIONS_DIR, offload_dir};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;
use tracing::warn;

use super::FILE_TRUNCATED_MARKER;

pub const MAX_OFFLOAD_FILE_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_OFFLOAD_SESSION_BYTES: u64 = 256 * 1024 * 1024;
pub const LINE_CUT_PREFIX: &str = "[line cut: ";
pub const OFFLOAD_FOOTER_PREFIX: &str = "[output truncated: ";
pub const OFFLOAD_POINTER_PREFIX: &str = "[output identical to a result saved earlier";
pub const DEFAULT_LABEL: &str = "output";
const HASH_HEX_CHARS: usize = 16;
const SLOT_EXT: &str = ".txt";
const MAX_PUT_ATTEMPTS: usize = 8;
const READ_ADVICE: &str = "inspect it with grep, or read with offset and limit";
const BASH_ADVICE: &str =
    "inspect it with bash (e.g. jq, or cut -c) since some lines exceed agent.max_line_bytes";
const CLIPPED_NOTE: &str =
    "; lines longer than agent.max_line_bytes are clipped in the saved file too";
const SHELL_SAFE: &[u8] = b"/._-+:,@%=";
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
    fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>>;
    /// Writes `bytes` under `name` unless the name is taken; false if taken.
    fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool>;
    fn names(&self) -> io::Result<Vec<String>>;
    fn total_bytes(&self) -> io::Result<u64>;
    fn remove_all(&self) -> io::Result<()>;
    fn path(&self, name: &str) -> PathBuf;
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
    closed: Mutex<bool>,
}

impl OffloadStore {
    pub fn new(backend: Box<dyn OffloadBackend>) -> Self {
        Self {
            backend,
            closed: Mutex::new(false),
        }
    }

    pub fn on_disk(dir: PathBuf) -> Self {
        Self::new(Box::new(DiskBackend { dir }))
    }

    pub fn path_of(&self, saved: &Saved) -> PathBuf {
        self.backend.path(&saved.name)
    }

    /// Saves `body` unless an identical result is already stored. Existing
    /// files are compared byte for byte, because the model may have edited
    /// one, so neither a name nor the lowest free slot proves anything.
    pub fn put(&self, body: &str) -> Result<Saved, OffloadError> {
        let closed = self.closed.lock().unwrap_or_else(|e| e.into_inner());
        if *closed {
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
            let slots: Vec<String> = self
                .backend
                .names()?
                .into_iter()
                .filter(|name| slot_number(name, &hash).is_some())
                .collect();
            for name in &slots {
                if self.backend.read(name)?.as_deref() == Some(stored.as_bytes()) {
                    return Ok(saved(name.clone(), PutOutcome::Existing));
                }
            }
            if self.backend.total_bytes()? + stored.len() as u64 > MAX_OFFLOAD_SESSION_BYTES {
                return Err(OffloadError::Quota);
            }
            let free = (1..)
                .map(|n| slot_name(&hash, n))
                .find(|name| !slots.contains(name))
                .expect("slot numbers are unbounded");
            if self.backend.create_new(&free, stored.as_bytes())? {
                return Ok(saved(free, PutOutcome::Created));
            }
        }
        Err(OffloadError::SlotsTaken)
    }

    /// Waits for any in-flight `put`, then refuses all later ones and removes
    /// the files, so a late subagent can't recreate the directory.
    pub fn close_and_remove(&self) -> io::Result<()> {
        let mut closed = self.closed.lock().unwrap_or_else(|e| e.into_inner());
        *closed = true;
        self.backend.remove_all()
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

fn stored_form(body: &str, saved_bytes: usize) -> String {
    if saved_bytes == body.len() {
        return body.to_owned();
    }
    format!(
        "{}\n[offload capped: first {saved_bytes} of {} bytes saved]",
        &body[..saved_bytes],
        body.len()
    )
}

pub struct DiskBackend {
    dir: PathBuf,
}

impl DiskBackend {
    fn ensure_dir(&self) -> io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.dir, fs::Permissions::from_mode(DIR_MODE))?;
        }
        Ok(())
    }

    fn files(&self) -> io::Result<Vec<fs::DirEntry>> {
        match fs::read_dir(&self.dir) {
            Ok(entries) => entries
                .filter(|entry| entry.as_ref().map_or(true, |e| e.path().is_file()))
                .collect(),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }
}

impl OffloadBackend for DiskBackend {
    fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
        match fs::read(self.dir.join(name)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
        self.ensure_dir()?;
        let mut file = NamedTempFile::new_in(&self.dir)?;
        file.write_all(bytes)?;
        match file.persist_noclobber(self.dir.join(name)) {
            Ok(_) => Ok(true),
            Err(e) if e.error.kind() == ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(e.error),
        }
    }

    fn names(&self) -> io::Result<Vec<String>> {
        Ok(self
            .files()?
            .into_iter()
            .filter_map(|entry| entry.file_name().into_string().ok())
            .collect())
    }

    fn total_bytes(&self) -> io::Result<u64> {
        self.files()?
            .into_iter()
            .map(|entry| entry.metadata().map(|meta| meta.len()))
            .sum()
    }

    fn remove_all(&self) -> io::Result<()> {
        match fs::remove_dir_all(&self.dir) {
            Err(e) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
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

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone, Copy)]
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
        Some((_, Err(e))) => {
            warn!(error = %e, bytes = body.len(), "tool output not offloaded");
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
    const UNITS: [&str; 3] = ["KB", "MB", "GB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut size = bytes as f64 / 1024.0;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.1} {}", UNITS[unit])
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
    format!(
        "{OFFLOAD_POINTER_PREFIX} in this session (possibly by another agent){label_note}: {}{capped}, at {}; read the file if that result is not in this conversation]",
        size_summary(body, saved),
        path.display()
    )
}

#[derive(Debug, Clone, Copy)]
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
    if total < 2 {
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
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::TempDir;
    use test_case::test_case;

    use super::*;

    const BIG: usize = 100_000;

    #[derive(Default)]
    struct MapBackend {
        files: Mutex<BTreeMap<String, Vec<u8>>>,
        fail: bool,
    }

    impl OffloadBackend for Arc<MapBackend> {
        fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
            Ok(self.files.lock().unwrap().get(name).cloned())
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
        fn names(&self) -> io::Result<Vec<String>> {
            Ok(self.files.lock().unwrap().keys().cloned().collect())
        }
        fn total_bytes(&self) -> io::Result<u64> {
            Ok(self
                .files
                .lock()
                .unwrap()
                .values()
                .map(|v| v.len() as u64)
                .sum())
        }
        fn remove_all(&self) -> io::Result<()> {
            self.files.lock().unwrap().clear();
            Ok(())
        }
        fn path(&self, name: &str) -> PathBuf {
            PathBuf::from("/offload").join(name)
        }
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
            out.contains(&format!("all of it saved to /offload/{name}")),
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

    #[test_case(PreviewShape::Head, 10, 400 ; "head_even")]
    #[test_case(PreviewShape::Head, 7, 333 ; "head_odd")]
    #[test_case(PreviewShape::HeadTail, 10, 400 ; "head_tail_even")]
    #[test_case(PreviewShape::HeadTail, 7, 333 ; "head_tail_odd")]
    #[test_case(PreviewShape::HeadTail, 4, 260 ; "head_tail_tiny")]
    fn total_output_within_limits(shape: PreviewShape, max_lines: usize, max_bytes: usize) {
        let o = LimitOpts {
            trailer: Some("Exit code: 3"),
            ..opts(shape, max_lines, max_bytes)
        };
        for body in [
            numbered(200),
            format!("{}\n{}", "a".repeat(5000), numbered(20)),
            format!("{}\n{}", numbered(20), "b".repeat(5000)),
        ] {
            let (_, store) = map_store();
            for store in [None, Some(&store)] {
                let out = limit_output(&body, &o, store);
                let metadata_alone = out.lines().next().unwrap_or("").starts_with('[');
                if !metadata_alone {
                    within(&out, &o);
                }
                assert!(out.ends_with("Exit code: 3"), "{out}");
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
        assert!(backend.total_bytes().unwrap() <= MAX_OFFLOAD_SESSION_BYTES);
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
    }

    impl OffloadBackend for GatedBackend {
        fn read(&self, name: &str) -> io::Result<Option<Vec<u8>>> {
            self.inner.read(name)
        }
        fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
            self.entered.send(()).unwrap();
            self.release.recv().unwrap();
            self.inner.create_new(name, bytes)
        }
        fn names(&self) -> io::Result<Vec<String>> {
            self.inner.names()
        }
        fn total_bytes(&self) -> io::Result<u64> {
            self.inner.total_bytes()
        }
        fn remove_all(&self) -> io::Result<()> {
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
        let store = Arc::new(OffloadStore::new(Box::new(GatedBackend {
            inner: Arc::clone(&inner),
            entered: entered_tx,
            release: release_rx,
        })));

        let putter = {
            let store = Arc::clone(&store);
            thread::spawn(move || store.put("in flight").is_ok())
        };
        entered_rx.recv().unwrap();
        let (closed_tx, closed_rx) = flume::unbounded();
        let closer = {
            let store = Arc::clone(&store);
            thread::spawn(move || {
                store.close_and_remove().unwrap();
                closed_tx.send(()).unwrap();
            })
        };
        assert!(
            closed_rx.is_empty(),
            "close must wait for the in-flight put"
        );
        release_tx.send(()).unwrap();
        assert!(putter.join().unwrap(), "the in-flight put completes");
        closer.join().unwrap();
        assert!(
            inner.files.lock().unwrap().is_empty(),
            "removed after the put"
        );
        assert!(matches!(store.put("late"), Err(OffloadError::Closed)));
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
