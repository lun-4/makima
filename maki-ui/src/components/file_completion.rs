use std::io;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crossterm::event::{KeyCode, KeyEvent};
use maki_lua::ItemSpec;
#[cfg(test)]
use maki_match::completion_match_default;
use maki_match::{
    CompletionMatch, CompletionMatchOptions, CompletionRanking, compare_completion_matches,
    completion_match,
};
use nucleo::Nucleo;
use nucleo::pattern::{CaseMatching, Normalization};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use tracing::warn;
use unicode_width::UnicodeWidthChar;

use crate::repaint::{Cadence, Dirty};
use crate::text_buffer::TextBuffer;
use crate::theme;

const WALKER_CRASHED_MSG: &str = "File scanner crashed";
const COL_GAP: usize = 2;
const PENDING_DEBOUNCE_MS: u128 = 100;
const MAX_MATERIALIZED: u32 = 640;
const FILE_KIND: &str = "file";
const DIRECTORY_KIND: &str = "directory";
const DIRECTORY_SUFFIX: char = std::path::MAIN_SEPARATOR;

/// Byte range of the `@`-token under the cursor (including its leading `@`),
/// or `None` when the most recent `@` does not begin a token.
pub fn at_token_range(line: &str, cursor_chars: usize) -> Option<(usize, usize)> {
    let cursor_byte = TextBuffer::char_to_byte(line, cursor_chars);
    maki_lua::active_at_token(line, cursor_byte).map(|token| (token.range.start, token.range.end))
}

/// Query for the active token, decoded by the shared Lua parser. The query
/// excludes the leading `@`, prefix, and quote delimiters.
pub fn at_token_query(line: &str, cursor_chars: usize) -> Option<String> {
    let cursor_byte = TextBuffer::char_to_byte(line, cursor_chars);
    maki_lua::active_at_token(line, cursor_byte).map(|token| {
        if token.prefix.is_empty() {
            token.value
        } else {
            format!("{}:{}", token.prefix, token.value)
        }
    })
}

/// A generic completion candidate. `label` is the fuzzy-match target and
/// display text; `kind` drives rendering via `Theme::completion_kinds`;
/// `insertion` replaces the whole `@`-token (including its leading `@`);
/// `description` is shown beside the label when present.
#[derive(Debug, Clone)]
pub struct CompletionItem {
    pub label: String,
    pub kind: String,
    pub insertion: String,
    pub description: Option<String>,
}

impl CompletionItem {
    /// Text that replaces the whole `@`-token (including its leading `@`).
    pub(crate) fn replacement(&self) -> String {
        normalize_completion_insertion(
            &self.insertion,
            self.kind == FILE_KIND || self.kind == DIRECTORY_KIND,
        )
    }

    pub(crate) fn advance_replacement(&self) -> String {
        let replacement = self.replacement();
        let Some(quote) = replacement
            .chars()
            .nth(1)
            .filter(|quote| matches!(quote, '\'' | '"'))
        else {
            return replacement;
        };
        replacement
            .strip_suffix(quote)
            .unwrap_or(&replacement)
            .to_string()
    }

    fn display(&self) -> String {
        match &self.description {
            Some(d) if !d.is_empty() => format!("{}  {}", self.label, d),
            _ => self.label.clone(),
        }
    }

    fn file(path: String) -> Self {
        Self::path(path, false)
    }

    fn directory(path: String) -> Self {
        Self::path(path, true)
    }

    fn path(mut path: String, directory: bool) -> Self {
        if directory && !path.ends_with(['/', '\\']) {
            path.push(DIRECTORY_SUFFIX);
        }
        let kind = if directory { DIRECTORY_KIND } else { FILE_KIND };
        Self {
            label: path.clone(),
            kind: kind.to_string(),
            insertion: format!("@{path}"),
            description: None,
        }
    }
}

fn normalize_completion_insertion(insertion: &str, file: bool) -> String {
    let delimiter_start = insertion.trim_end_matches(char::is_whitespace).len();
    let (token, delimiter) = insertion.split_at(delimiter_start);
    let Some(body) = token.strip_prefix('@') else {
        return insertion.to_string();
    };
    let (prefix, value) = if file {
        ("", body)
    } else {
        body.split_once(':')
            .map_or(("", body), |(prefix, value)| (prefix, value))
    };
    if matches!(value.chars().next(), Some('\'' | '"'))
        && value.len() >= 2
        && value.chars().next() == value.chars().next_back()
    {
        return insertion.to_string();
    }
    let unsafe_value = value.chars().any(char::is_whitespace)
        || value
            .chars()
            .next_back()
            .is_some_and(maki_lua::is_trailing_at_token_punctuation);
    if !unsafe_value {
        return insertion.to_string();
    }
    let quote = if value.contains('"') && !value.contains('\'') {
        '\''
    } else {
        '"'
    };
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character == quote || character == '\\' {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    let prefix = if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}:")
    };
    format!("@{prefix}{quote}{escaped}{quote}{delimiter}")
}

#[derive(Debug, Clone)]
struct FileCandidate {
    path: String,
    is_directory: bool,
}

#[derive(Debug, Clone)]
struct Candidate {
    item: CompletionItem,
    matching: CompletionMatch,
    source_rank: u8,
    source_order: usize,
    descendable: bool,
}

#[derive(Debug, Clone)]
struct QueryIntent {
    payload: String,
    kind: Option<&'static str>,
    has_colon: bool,
}

#[derive(Debug)]
pub enum CompletionAction {
    Consumed,
    Select(CompletionItem),
    /// Move filesystem discovery into the selected directory and refresh.
    Advance(CompletionItem),
    Close,
    Passthrough,
}

enum Discovery {
    Project {
        nucleo: Nucleo<()>,
        done_rx: flume::Receiver<()>,
        cancel: Arc<AtomicBool>,
    },
    Explicit {
        candidates: Vec<FileCandidate>,
    },
}

struct Session {
    discovery: Discovery,
    query: String,
    query_refresh_pending: bool,
    intent: QueryIntent,
    /// Non-file candidates from Lua sources, as `(matchable label, item)`.
    /// Re-fuzzy-matched against each new query in `sync_query`.
    ref_items: Vec<(String, CompletionItem)>,
    ref_matches: Vec<Candidate>,
    file_matches: Vec<Candidate>,
    matches: Vec<Candidate>,
    coarse_match_count: u32,
    materialized_count: u32,
    final_match_count: u32,
    truncated: bool,

    selected: usize,
    /// Grid layout: columns used, and scroll/viewport in whole rows.
    cols: usize,
    scroll_offset: usize,
    viewport_height: usize,

    started_at: Instant,

    walking: bool,
    root: PathBuf,
    matching: bool,
    visible: bool,

    token_byte_range: (usize, usize),
}

impl Drop for Session {
    fn drop(&mut self) {
        if let Discovery::Project { cancel, .. } = &self.discovery {
            cancel.store(true, Ordering::Relaxed);
        }
    }
}

trait FileResolver: Send + Sync {
    fn read_dir(&self, path: &Path) -> io::Result<Vec<FileCandidate>>;
}

#[derive(Debug, Default)]
struct RealFileResolver;

impl FileResolver for RealFileResolver {
    fn read_dir(&self, path: &Path) -> io::Result<Vec<FileCandidate>> {
        discover_one_level(path)
    }
}

fn resolve_file_path(cwd: &Path, home: Option<&Path>, value: &str) -> PathBuf {
    let path = if value == "~" {
        home.map_or_else(|| PathBuf::from(value), Path::to_path_buf)
    } else if let Some(rest) = value.strip_prefix("~/") {
        home.map(|home| home.join(rest))
            .unwrap_or_else(|| PathBuf::from(value))
    } else {
        PathBuf::from(value)
    };
    if path.is_relative() {
        cwd.join(path)
    } else {
        path
    }
}

fn discovery_path(cwd: &Path, home: Option<&Path>, value: &str) -> (PathBuf, String, String) {
    let path = resolve_file_path(cwd, home, value);
    let lists_path = value.ends_with(['/', '\\']) || matches!(value, "~" | "." | "..");
    if lists_path {
        let display_prefix = if value.ends_with(['/', '\\']) {
            value.to_string()
        } else {
            format!("{value}{DIRECTORY_SUFFIX}")
        };
        return (path, String::new(), display_prefix);
    }
    let leaf = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    let display_prefix = value.strip_suffix(&leaf).unwrap_or(value).to_string();
    (
        path.parent().unwrap_or(cwd).to_path_buf(),
        leaf,
        display_prefix,
    )
}

fn discover_one_level(path: &Path) -> io::Result<Vec<FileCandidate>> {
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(path)? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(_) => continue,
        };
        if !metadata.is_file() && !metadata.is_dir() {
            continue;
        }
        entries.push(FileCandidate {
            path: entry.file_name().to_string_lossy().into_owned(),
            is_directory: metadata.is_dir(),
        });
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(entries)
}

fn explicit_candidates(
    resolver: &dyn FileResolver,
    cwd: &Path,
    home: Option<&Path>,
    value: &str,
) -> Vec<FileCandidate> {
    if value.starts_with('~') && home.is_none() {
        return Vec::new();
    }
    let (parent, leaf, display_prefix) = discovery_path(cwd, home, value);
    resolver
        .read_dir(&parent)
        .unwrap_or_default()
        .into_iter()
        .filter(|candidate| {
            leaf.is_empty()
                || completion_match(
                    &leaf,
                    &candidate.path,
                    CompletionMatchOptions {
                        case_matching: CaseMatching::Smart,
                        normalization: Normalization::Smart,
                    },
                )
                .is_some()
        })
        .map(|candidate| FileCandidate {
            path: format!("{display_prefix}{}", candidate.path),
            is_directory: candidate.is_directory,
        })
        .collect()
}

type Walker = (Nucleo<()>, flume::Receiver<()>, Arc<AtomicBool>);
type WalkerSpawner = Arc<dyn Fn(&str) -> Option<Walker> + Send + Sync>;

pub struct FileCompletionMenu {
    session: Option<Session>,
    resolver: Arc<dyn FileResolver>,
    walker_spawner: WalkerSpawner,
    home: Option<PathBuf>,
}

impl FileCompletionMenu {
    pub fn new() -> Self {
        Self::with_dependencies(
            Arc::new(RealFileResolver),
            Arc::new(super::file_picker::spawn_file_walker),
            maki_storage::paths::home(),
        )
    }

    #[cfg(test)]
    fn with_resolver(resolver: Arc<dyn FileResolver>, home: Option<PathBuf>) -> Self {
        Self::with_dependencies(
            resolver,
            Arc::new(super::file_picker::spawn_file_walker),
            home,
        )
    }

    fn with_dependencies(
        resolver: Arc<dyn FileResolver>,
        walker_spawner: WalkerSpawner,
        home: Option<PathBuf>,
    ) -> Self {
        Self {
            session: None,
            resolver,
            walker_spawner,
            home,
        }
    }

    /// Open the popup. `items` are the non-file candidates gathered from Lua
    /// completion sources by the caller. Files are discovered relative to cwd.
    pub fn open(
        &mut self,
        cwd: &str,
        items: Vec<ItemSpec>,
        query: &str,
        token_byte_range: (usize, usize),
    ) {
        self.close();
        let root = PathBuf::from(cwd);
        let (discovery, walking) = if query.starts_with(['~', '/', '.']) {
            (
                Discovery::Explicit {
                    candidates: explicit_candidates(
                        self.resolver.as_ref(),
                        &root,
                        self.home.as_deref(),
                        query,
                    ),
                },
                false,
            )
        } else {
            let Some((nucleo, done_rx, cancel)) = (self.walker_spawner)(cwd) else {
                return;
            };
            (
                Discovery::Project {
                    nucleo,
                    done_rx,
                    cancel,
                },
                true,
            )
        };

        let ref_items = items
            .into_iter()
            .map(|spec| {
                let item = CompletionItem {
                    label: spec.label,
                    kind: spec.kind,
                    insertion: spec.insertion,
                    description: spec.description,
                };
                (item.label.clone(), item)
            })
            .collect();

        let session = Session {
            discovery,
            query: String::new(),
            query_refresh_pending: false,
            intent: QueryIntent {
                payload: String::new(),
                kind: None,
                has_colon: false,
            },
            ref_items,
            ref_matches: Vec::new(),
            file_matches: Vec::new(),
            matches: Vec::new(),
            coarse_match_count: 0,
            materialized_count: 0,
            final_match_count: 0,
            truncated: false,
            selected: 0,
            cols: 1,
            scroll_offset: 0,
            viewport_height: 0,
            started_at: Instant::now(),
            walking,
            root,
            matching: false,
            visible: false,
            token_byte_range,
        };
        self.session = Some(session);
        self.sync_query(query);
    }

    pub fn close(&mut self) {
        self.session = None;
    }

    pub fn is_active(&self) -> bool {
        self.session.is_some()
    }

    #[cfg(test)]
    pub fn has_selectable(&self) -> bool {
        self.session
            .as_ref()
            .is_some_and(|s| s.visible && !s.matches.is_empty())
    }

    #[cfg(test)]
    pub fn match_items(&self) -> Vec<CompletionItem> {
        self.session
            .as_ref()
            .map(|s| s.matches.iter().map(|c| c.item.clone()).collect())
            .unwrap_or_default()
    }

    pub fn token_byte_range(&self) -> (usize, usize) {
        self.session.as_ref().map_or((0, 0), |s| s.token_byte_range)
    }

    pub fn set_token_byte_range(&mut self, range: (usize, usize)) {
        if let Some(s) = &mut self.session {
            s.token_byte_range = range;
        }
    }

    pub fn sync_query(&mut self, query: &str) {
        let Some(session) = &mut self.session else {
            return;
        };
        let explicit = query.starts_with(['~', '/', '.']);
        let was_explicit = matches!(session.discovery, Discovery::Explicit { .. });
        if explicit {
            if let Discovery::Project { cancel, .. } = &session.discovery {
                cancel.store(true, Ordering::Relaxed);
            }
            session.discovery = Discovery::Explicit {
                candidates: explicit_candidates(
                    self.resolver.as_ref(),
                    &session.root,
                    self.home.as_deref(),
                    query,
                ),
            };
            session.walking = false;
            session.matching = false;
            session.query_refresh_pending = false;
            session.ref_matches.clear();
            session.file_matches.clear();
        } else {
            if was_explicit {
                let Some((nucleo, done_rx, cancel)) =
                    (self.walker_spawner)(&session.root.to_string_lossy())
                else {
                    return;
                };
                session.discovery = Discovery::Project {
                    nucleo,
                    done_rx,
                    cancel,
                };
                session.walking = true;
                session.started_at = Instant::now();
                session.file_matches.clear();
            }
            if let Discovery::Project { nucleo, .. } = &mut session.discovery {
                nucleo.pattern.reparse(
                    0,
                    &query.to_lowercase(),
                    CaseMatching::Smart,
                    Normalization::Smart,
                    false,
                );
            }
            let query_changed = session.query != query;
            if query_changed {
                session.query_refresh_pending = matches!(&session.discovery, Discovery::Project { nucleo, .. } if nucleo.injector().injected_items() > 0);
            }
            session.ref_matches = fuzzy_match(
                &parse_query(query),
                session
                    .ref_items
                    .iter()
                    .cloned()
                    .enumerate()
                    .map(|(order, (label, item))| (label, item, order)),
            );
        }
        session.intent = parse_query(query);
        session.query = query.to_string();
        session.selected = 0;
        session.scroll_offset = 0;
        session.coarse_match_count = 0;
        session.materialized_count = 0;
        session.final_match_count = 0;
        session.truncated = false;
        if explicit {
            refresh_explicit_matches(session);
            rebuild_combined(session);
            session.visible = !session.matches.is_empty();
        } else if !session.query_refresh_pending {
            rebuild_combined(session);
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> CompletionAction {
        let Some(s) = &mut self.session else {
            return CompletionAction::Close;
        };

        match key.code {
            KeyCode::Esc => return CompletionAction::Close,
            KeyCode::Enter | KeyCode::Tab => {
                if !s.visible || s.query_refresh_pending {
                    return CompletionAction::Passthrough;
                }
                return match s.matches.get(s.selected) {
                    Some(candidate) if candidate.descendable => {
                        CompletionAction::Advance(candidate.item.clone())
                    }
                    Some(candidate) => CompletionAction::Select(candidate.item.clone()),
                    None => CompletionAction::Passthrough,
                };
            }
            KeyCode::Up => move_selection(s, -1),
            KeyCode::Down => move_selection(s, 1),
            KeyCode::Left if !super::menu_navigation_blocked(&key) => move_column(s, -1),
            KeyCode::Right if !super::menu_navigation_blocked(&key) => move_column(s, 1),
            _ => return CompletionAction::Passthrough,
        }
        CompletionAction::Consumed
    }

    pub fn cadence(&self) -> Cadence {
        let Some(s) = self.session.as_ref() else {
            return Cadence::IDLE;
        };
        Cadence::any([
            Cadence::when(s.visible && s.walking, Cadence::SPINNER),
            Cadence::when(s.matching && !s.walking, Cadence::PENDING),
        ])
    }

    pub fn tick(&mut self) -> (Dirty, Option<String>) {
        let Some(s) = &mut self.session else {
            return (Dirty::NO, None);
        };

        let mut dirty = Dirty::NO;
        let mut status_changed = false;
        if let Discovery::Project {
            nucleo, done_rx, ..
        } = &mut s.discovery
        {
            let status = nucleo.tick(0);
            s.matching = status.running;
            status_changed = status.changed;
            dirty = Dirty::from(status.changed);
            if s.walking {
                match done_rx.try_recv() {
                    Ok(()) => {
                        s.walking = false;
                        dirty = Dirty::YES;
                    }
                    Err(flume::TryRecvError::Disconnected) => {
                        warn!("{WALKER_CRASHED_MSG}: walker channel disconnected");
                        self.session = None;
                        return (Dirty::YES, Some(WALKER_CRASHED_MSG.into()));
                    }
                    Err(flume::TryRecvError::Empty) => {}
                }
            }
        }
        if matches!(s.discovery, Discovery::Explicit { .. }) {
            s.walking = false;
            s.matching = false;
        }

        if !s.visible {
            let has_files = match &s.discovery {
                Discovery::Project { nucleo, .. } => nucleo.injector().injected_items() > 0,
                Discovery::Explicit { candidates } => !candidates.is_empty(),
            };
            let has_refs = !s.ref_matches.is_empty();
            let debounce_elapsed = s.started_at.elapsed().as_millis() >= PENDING_DEBOUNCE_MS;

            if has_files || has_refs || (s.walking && debounce_elapsed) {
                s.visible = true;
                dirty = Dirty::YES;
            }
        }

        if let Some(s) = self.session.as_mut() {
            let refresh_finished = s.query_refresh_pending && !s.matching;
            if refresh_finished {
                refresh_file_matches(s);
                rebuild_combined(s);
                clamp_selection(s);
                s.query_refresh_pending = false;
            } else if !s.query_refresh_pending && status_changed {
                refresh_file_matches(s);
                rebuild_combined(s);
                clamp_selection(s);
            }
        }

        (dirty, None)
    }

    pub fn view(&mut self, frame: &mut Frame, input_area: Rect) -> Option<Rect> {
        let s = match &mut self.session {
            Some(s) if s.visible && !s.matches.is_empty() => s,
            _ => return None,
        };

        let len = s.matches.len();
        // Cap taken from the screen height: the popup is a compact overlay, not
        // a full-height list.
        let max_height = ((frame.area().height as u32 * 30 / 100) as u16).max(2);
        let avail = max_height.saturating_sub(1) as usize;
        if avail == 0 || input_area.y == 0 {
            return None;
        }

        let cols = if len <= avail {
            1
        } else if len <= avail.saturating_mul(2) {
            2
        } else {
            len.min(3)
        };
        s.cols = cols;
        let total_rows = len.div_ceil(cols);
        let view_rows = avail.min(total_rows);
        s.viewport_height = view_rows;
        ensure_visible(s);

        let budget = (input_area.width as usize).saturating_sub(COL_GAP * (cols - 1)) / cols;
        let col_widths: Vec<usize> = (0..cols)
            .map(|j| {
                s.matches
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| i % cols == j)
                    .map(|(_, c)| c.item.display().chars().count())
                    .max()
                    .unwrap_or(0)
                    .min(budget)
            })
            .collect();
        let total_width = col_widths.iter().sum::<usize>() + COL_GAP * (cols - 1);
        let popup_height = (view_rows as u16 + 1).min(max_height);
        let popup = Rect {
            x: input_area.x,
            y: input_area.y.saturating_sub(popup_height),
            width: total_width.clamp(1, input_area.width.max(1) as usize) as u16,
            height: popup_height,
        };

        let t = theme::current();
        let lines = build_grid(s, view_rows, cols, &col_widths, &t);

        frame.render_widget(Clear, popup);
        let block = Block::default()
            .borders(Borders::TOP)
            .style(Style::new().bg(t.background));
        let inner = block.inner(popup);
        frame.render_widget(block, popup);
        frame.render_widget(Paragraph::new(lines), inner);

        Some(popup)
    }
}

/// Character indices of `query` fuzzy-matched within `label`, for
/// highlighting. `None` when the query does not match.
fn parse_query(query: &str) -> QueryIntent {
    let lowered = query.to_lowercase();
    for (aliases, kind) in [
        (&["skill", "sk"][..], "skill"),
        (&["subagent", "su", "a"][..], "subagent"),
        (&["model", "m"][..], "model"),
    ] {
        for alias in aliases {
            if lowered == *alias {
                return QueryIntent {
                    payload: String::new(),
                    kind: Some(kind),
                    has_colon: false,
                };
            }
            if let Some(payload_start) = lowered.strip_prefix(&format!("{alias}:")) {
                let payload_start = payload_start.as_ptr() as usize - lowered.as_ptr() as usize;
                let payload = &query[payload_start..];
                return QueryIntent {
                    payload: payload.into(),
                    kind: Some(kind),
                    has_colon: true,
                };
            }
        }
    }
    QueryIntent {
        payload: query.to_string(),
        kind: None,
        has_colon: false,
    }
}

fn match_candidate(
    item: CompletionItem,
    intent: &QueryIntent,
    source: u8,
    order: usize,
    descendable: bool,
) -> Option<Candidate> {
    if intent.kind.is_some_and(|kind| kind != item.kind) {
        return None;
    }
    let target = match intent.kind {
        Some(kind) => {
            let prefix = format!("{kind}:");
            item.label.strip_prefix(&prefix).unwrap_or(&item.label)
        }
        None => item.label.as_str(),
    };

    let display_prefix = if intent.has_colon {
        intent.kind.map_or(0, |kind| kind.chars().count() + 1)
    } else {
        0
    };
    if intent.payload.is_empty() {
        return Some(Candidate {
            item,
            matching: CompletionMatch {
                indices: Vec::new(),
                ranking: CompletionRanking {
                    quality_rank: 4,
                    boundary_rank: 1,
                    start_index: 0,
                    gap_count: 0,
                    span_length: 0,
                    unmatched_suffix: 0,
                    fuzzy_score: 0,
                },
            },
            source_rank: source,
            source_order: order,
            descendable,
        });
    }
    let matched = completion_match(
        &intent.payload,
        target,
        CompletionMatchOptions {
            case_matching: CaseMatching::Smart,
            normalization: Normalization::Smart,
        },
    )?;
    let mut matching = matched;
    for index in &mut matching.indices {
        *index += display_prefix as u32;
    }
    Some(Candidate {
        item,
        matching,
        source_rank: source,
        source_order: order,
        descendable,
    })
}

fn fuzzy_match(
    intent: &QueryIntent,
    items: impl IntoIterator<Item = (String, CompletionItem, usize)>,
) -> Vec<Candidate> {
    items
        .into_iter()
        .filter_map(|(_, item, order)| match_candidate(item, intent, 1, order, false))
        .collect()
}

fn refresh_file_matches(s: &mut Session) {
    let Discovery::Project { nucleo, .. } = &s.discovery else {
        return;
    };
    let snapshot = nucleo.snapshot();
    let coarse_match_count = snapshot.matched_item_count();
    let materialized_count = coarse_match_count.min(MAX_MATERIALIZED);
    let mut paths: Vec<String> = snapshot
        .matched_items(0..materialized_count)
        .map(|item| item.matcher_columns[0].to_string())
        .collect();
    paths.sort();
    s.coarse_match_count = coarse_match_count;
    s.materialized_count = materialized_count;
    s.truncated = coarse_match_count > materialized_count;
    s.file_matches.clear();
    for (order, path) in paths.into_iter().enumerate() {
        let item = CompletionItem::file(path);
        if let Some(candidate) = match_candidate(item, &s.intent, 0, order, false) {
            s.file_matches.push(candidate);
        }
    }
    s.final_match_count = s.file_matches.len() as u32;
}

fn refresh_explicit_matches(s: &mut Session) {
    let Discovery::Explicit { candidates } = &s.discovery else {
        return;
    };
    let mut paths = candidates.clone();
    paths.sort_by(|a, b| a.path.cmp(&b.path));
    s.file_matches.clear();
    for (order, candidate) in paths.into_iter().enumerate() {
        let item = if candidate.is_directory {
            CompletionItem::directory(candidate.path)
        } else {
            CompletionItem::file(candidate.path)
        };
        if let Some(candidate) = match_candidate(item, &s.intent, 0, order, candidate.is_directory)
        {
            s.file_matches.push(candidate);
        }
    }
    s.coarse_match_count = s.file_matches.len() as u32;
    s.materialized_count = s.coarse_match_count;
    s.final_match_count = s.coarse_match_count;
    s.truncated = false;
}

fn rebuild_combined(s: &mut Session) {
    s.matches.clear();
    s.matches.extend(s.file_matches.iter().cloned());
    s.matches.extend(s.ref_matches.iter().cloned());
    s.matches.sort_by(|a, b| {
        compare_completion_matches(
            &a.matching,
            &b.matching,
            a.source_rank,
            b.source_rank,
            a.source_order,
            b.source_order,
            &a.item.label,
            &b.item.label,
        )
    });
}

#[cfg(test)]
fn highlight_indices(label: &str, query: &str) -> Option<Vec<u32>> {
    completion_match_default(query, label).map(|matching| matching.indices)
}

fn move_selection(s: &mut Session, rows: isize) {
    if s.matches.is_empty() {
        return;
    }
    let cols = s.cols.max(1);
    let last = s.matches.len() - 1;
    let col = (s.selected % cols).min(last);
    let last_row = last / cols;
    let row = ((s.selected / cols) as isize + rows).clamp(0, last_row as isize) as usize;
    s.selected = (row * cols + col).min(last);
    ensure_visible(s);
}

/// Moves one column left or right within the same grid row, clamped at the
/// row's boundaries. The final row may hold fewer than `cols` items.
fn move_column(s: &mut Session, delta: isize) {
    if s.matches.is_empty() || s.cols < 2 {
        return;
    }
    let cols = s.cols;
    let last = s.matches.len() - 1;
    let row = s.selected / cols;
    let col = s.selected % cols;
    let last_col = (last - row * cols).min(cols - 1);
    let new_col = (col as isize + delta).clamp(0, last_col as isize) as usize;
    s.selected = row * cols + new_col;
    ensure_visible(s);
}

fn clamp_selection(s: &mut Session) {
    if s.matches.is_empty() {
        s.selected = 0;
        s.scroll_offset = 0;
    } else {
        s.selected = s.selected.min(s.matches.len() - 1);
        ensure_visible(s);
    }
}

fn ensure_visible(s: &mut Session) {
    let cols = s.cols.max(1);
    let total_rows = s.matches.len().div_ceil(cols);
    let vh = s.viewport_height.max(1);

    if total_rows > vh {
        s.scroll_offset = s.scroll_offset.min(total_rows - vh);
    } else {
        s.scroll_offset = 0;
    }

    let row = s.selected / cols;
    if row < s.scroll_offset {
        s.scroll_offset = row;
    } else if row >= s.scroll_offset + vh {
        s.scroll_offset = row + 1 - vh;
    }
}

fn build_grid<'a>(
    s: &Session,
    view_rows: usize,
    cols: usize,
    col_widths: &[usize],
    t: &'a theme::Theme,
) -> Vec<Line<'a>> {
    let len = s.matches.len();
    let mut lines = Vec::with_capacity(view_rows);

    for r in 0..view_rows {
        let row = s.scroll_offset + r;
        let mut spans = Vec::new();
        for (j, width) in col_widths.iter().enumerate() {
            let idx = row * cols + j;
            if idx < len {
                spans.extend(cell_line(&s.matches[idx], *width, idx == s.selected, t).spans);
            } else {
                spans.push(Span::raw(" ".repeat(*width)));
            }
            if j + 1 < cols {
                spans.push(Span::raw(" ".repeat(COL_GAP)));
            }
        }
        lines.push(Line::from(spans));
    }
    lines
}

fn cell_line<'a>(c: &Candidate, width: usize, selected: bool, t: &'a theme::Theme) -> Line<'a> {
    let base = if selected { t.item_selected } else { t.item };
    let kind_style = t
        .completion_kinds
        .get(&c.item.kind)
        .copied()
        .unwrap_or(base);
    // Matched characters keep the kind foreground but also carry the
    // selection background, so the highlight is not cut out of the selected
    // row.
    let match_style = if selected {
        Style {
            bg: base.bg,
            ..kind_style
        }
    } else {
        kind_style
    };
    let text = c.item.display();
    let mut spans: Vec<Span<'a>> = Vec::new();
    let mut used = 0usize;
    let mut in_match = false;
    let mut run = String::new();

    for (i, ch) in text.chars().enumerate() {
        let cw = ch.width().unwrap_or(0);
        if used + cw > width {
            break;
        }
        used += cw;
        let is_match = c.matching.indices.binary_search(&(i as u32)).is_ok();
        if is_match != in_match && !run.is_empty() {
            spans.push(Span::styled(
                mem::take(&mut run),
                if in_match { match_style } else { base },
            ));
        }
        in_match = is_match;
        run.push(ch);
    }
    if !run.is_empty() {
        spans.push(Span::styled(run, if in_match { match_style } else { base }));
    }

    if used < width {
        spans.push(Span::raw(" ".repeat(width - used)));
    }
    Line::from(spans)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::time::{Duration, Instant};

    use crate::theme::ThemesProvider;

    use crossterm::event::{KeyEventKind, KeyEventState, KeyModifiers};
    use nucleo::{Config, Nucleo, Utf32String};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;

    use crate::text_buffer::TextBuffer;
    use maki_lua::ItemSpec;

    use super::*;
    use test_case::test_case;

    const MATCHER_SETTLE_TIMEOUT: Duration = Duration::from_secs(5);

    fn wait_for_matcher(
        menu: &mut FileCompletionMenu,
        settled: impl Fn(&FileCompletionMenu) -> bool,
        message: &str,
    ) {
        let deadline = Instant::now() + MATCHER_SETTLE_TIMEOUT;
        loop {
            let _ = menu.tick();
            if settled(menu) {
                break;
            }
            assert!(Instant::now() < deadline, "{message}");
            std::thread::yield_now();
        }
    }

    fn item(label: &str, kind: &str, insertion: &str) -> ItemSpec {
        ItemSpec {
            label: label.into(),
            kind: kind.into(),
            insertion: insertion.into(),
            description: None,
        }
    }

    fn project_nucleo_mut(session: &mut Session) -> &mut Nucleo<()> {
        let Discovery::Project { nucleo, .. } = &mut session.discovery else {
            panic!("test session must use project discovery");
        };
        nucleo
    }

    fn session_with_items(items: Vec<ItemSpec>) -> FileCompletionMenu {
        let nucleo = Nucleo::new(Config::DEFAULT.match_paths(), Arc::new(|| {}), None, 1);
        let (_, done_rx) = flume::bounded(1);
        let mut menu = FileCompletionMenu::new();
        let ref_items = items
            .into_iter()
            .map(|spec| {
                let item = CompletionItem {
                    label: spec.label,
                    kind: spec.kind,
                    insertion: spec.insertion,
                    description: spec.description,
                };
                (item.label.clone(), item)
            })
            .collect();
        menu.session = Some(Session {
            discovery: Discovery::Project {
                nucleo,
                done_rx,
                cancel: Arc::new(AtomicBool::new(false)),
            },
            query: String::new(),
            query_refresh_pending: false,
            intent: QueryIntent {
                payload: String::new(),
                kind: None,
                has_colon: false,
            },
            ref_items,
            ref_matches: Vec::new(),
            file_matches: Vec::new(),
            matches: Vec::new(),
            coarse_match_count: 0,
            materialized_count: 0,
            final_match_count: 0,
            truncated: false,
            selected: 0,
            cols: 1,
            scroll_offset: 0,
            viewport_height: 0,
            started_at: Instant::now(),
            walking: true,
            root: PathBuf::new(),
            matching: false,
            visible: false,
            token_byte_range: (0, 0),
        });
        menu
    }

    struct CountingResolver {
        reads: std::sync::Mutex<Vec<PathBuf>>,
        entries: Vec<FileCandidate>,
    }

    impl FileResolver for CountingResolver {
        fn read_dir(&self, path: &Path) -> io::Result<Vec<FileCandidate>> {
            self.reads.lock().unwrap().push(path.to_path_buf());
            Ok(self.entries.clone())
        }
    }

    fn test_walker() -> Walker {
        let nucleo = Nucleo::new(Config::DEFAULT.match_paths(), Arc::new(|| {}), None, 1);
        let (done_tx, done_rx) = flume::bounded(1);
        std::mem::forget(done_tx);
        (nucleo, done_rx, Arc::new(AtomicBool::new(false)))
    }

    #[test]
    fn explicit_mode_project_walker_lifecycle() {
        let spawns = Arc::new(std::sync::Mutex::new(Vec::new()));
        let cancellations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let spawner: WalkerSpawner = {
            let spawns = spawns.clone();
            let cancellations = cancellations.clone();
            Arc::new(move |cwd| {
                spawns.lock().unwrap().push(cwd.to_string());
                let walker = test_walker();
                cancellations.lock().unwrap().push(walker.2.clone());
                Some(walker)
            })
        };
        let resolver = Arc::new(CountingResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries: Vec::new(),
        });
        let mut menu = FileCompletionMenu::with_dependencies(resolver, spawner, None);

        menu.open("/project", Vec::new(), "../", (0, 3));
        assert!(spawns.lock().unwrap().is_empty());
        menu.sync_query("src");
        assert_eq!(spawns.lock().unwrap().as_slice(), &["/project"]);
        menu.sync_query("../");
        assert!(cancellations.lock().unwrap()[0].load(Ordering::Relaxed));
        menu.sync_query("src");
        assert_eq!(spawns.lock().unwrap().len(), 2);
    }

    #[test]
    fn explicit_mode_ignores_stale_walker_signals() {
        let (done_tx, done_rx) = flume::bounded(1);
        let receiver = Arc::new(std::sync::Mutex::new(Some(done_rx)));
        let spawner: WalkerSpawner = {
            let receiver = receiver.clone();
            Arc::new(move |_| {
                let nucleo = Nucleo::new(Config::DEFAULT.match_paths(), Arc::new(|| {}), None, 1);
                Some((
                    nucleo,
                    receiver.lock().unwrap().take().unwrap(),
                    Arc::new(AtomicBool::new(false)),
                ))
            })
        };
        let resolver = Arc::new(CountingResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries: vec![FileCandidate {
                path: "outside.txt".into(),
                is_directory: false,
            }],
        });
        let mut menu = FileCompletionMenu::with_dependencies(resolver, spawner, None);
        menu.open("/project", Vec::new(), "src", (0, 4));
        menu.sync_query("../out");
        drop(done_tx);

        assert_eq!(menu.tick(), (Dirty::NO, None));
        assert!(menu.is_active());
        assert_eq!(menu.match_items()[0].label, "../outside.txt");
    }

    struct RecoveringResolver;

    impl FileResolver for RecoveringResolver {
        fn read_dir(&self, path: &Path) -> io::Result<Vec<FileCandidate>> {
            if path.ends_with("missing") {
                Err(io::Error::new(io::ErrorKind::NotFound, "missing"))
            } else {
                Ok(vec![FileCandidate {
                    path: "found.txt".into(),
                    is_directory: false,
                }])
            }
        }
    }

    #[test]
    fn explicit_discovery_failures_are_recoverable() {
        let mut menu = FileCompletionMenu::with_resolver(Arc::new(RecoveringResolver), None);
        menu.open("/project", Vec::new(), "./missing/", (0, 10));
        assert!(menu.is_active());
        assert!(menu.match_items().is_empty());

        menu.sync_query("./valid/");
        assert!(menu.has_selectable());
        assert_eq!(menu.match_items()[0].label, "./valid/found.txt");
    }

    #[test]
    fn project_walker_disconnect_flashes_and_closes() {
        let nucleo = Nucleo::new(Config::DEFAULT.match_paths(), Arc::new(|| {}), None, 1);
        let (done_tx, done_rx) = flume::bounded(1);
        drop(done_tx);
        let walker = Arc::new(std::sync::Mutex::new(Some((nucleo, done_rx))));
        let spawner: WalkerSpawner = Arc::new(move |_| {
            let (nucleo, done_rx) = walker.lock().unwrap().take().unwrap();
            Some((nucleo, done_rx, Arc::new(AtomicBool::new(false))))
        });
        let mut menu = FileCompletionMenu::with_dependencies(
            Arc::new(CountingResolver {
                reads: std::sync::Mutex::new(Vec::new()),
                entries: Vec::new(),
            }),
            spawner,
            None,
        );
        menu.open("/project", Vec::new(), "src", (0, 4));

        assert_eq!(
            menu.tick(),
            (Dirty::YES, Some(WALKER_CRASHED_MSG.to_string()))
        );
        assert!(!menu.is_active());
    }

    #[test]
    fn switching_from_explicit_to_project_restores_project_and_lua_matches() {
        let spawner: WalkerSpawner = Arc::new(move |_| Some(test_walker()));
        let resolver = Arc::new(CountingResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries: vec![FileCandidate {
                path: "outside.txt".into(),
                is_directory: false,
            }],
        });
        let mut menu = FileCompletionMenu::with_dependencies(resolver, spawner, None);
        menu.open(
            "/project",
            vec![item("skill:review", "skill", "@skill:review")],
            "../out",
            (0, 7),
        );
        assert_eq!(menu.match_items()[0].kind, FILE_KIND);

        menu.sync_query("review");
        let session = menu.session.as_mut().unwrap();
        project_nucleo_mut(session)
            .injector()
            .push((), |_, columns| {
                columns[0] = Utf32String::from("review.txt")
            });
        session.walking = false;
        wait_for_matcher(
            &mut menu,
            |menu| {
                let session = menu.session.as_ref().unwrap();
                !session.query_refresh_pending
                    && menu
                        .match_items()
                        .iter()
                        .any(|item| item.label == "review.txt")
            },
            "project matcher did not surface the injected review.txt match",
        );
        let items = menu.match_items();
        let kinds = items
            .iter()
            .map(|item| item.kind.clone())
            .collect::<Vec<_>>();
        assert!(kinds.contains(&FILE_KIND.to_string()));
        assert!(kinds.contains(&"skill".to_string()));
        assert!(
            items.iter().all(|item| item.label != "../outside.txt"),
            "stale explicit candidate must not survive the project transition"
        );
    }

    #[test]
    fn lua_directory_kind_does_not_advance() {
        let mut menu = session_with_items(vec![item(
            "directory:plugin",
            DIRECTORY_KIND,
            "@directory:plugin",
        )]);
        menu.sync_query("plugin");
        menu.session.as_mut().unwrap().visible = true;
        assert!(matches!(
            menu.handle_key(key(KeyCode::Enter)),
            CompletionAction::Select(_)
        ));
    }

    #[test]
    fn explicit_discovery_reads_parent_once_and_marks_directories() {
        let cwd = PathBuf::from("/workspace/project");
        let resolver = CountingResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries: vec![
                FileCandidate {
                    path: "alpha.txt".into(),
                    is_directory: false,
                },
                FileCandidate {
                    path: "archive".into(),
                    is_directory: true,
                },
            ],
        };
        let candidates = explicit_candidates(&resolver, &cwd, None, "../ar");
        assert_eq!(resolver.reads.lock().unwrap().as_slice(), &[cwd.join("..")]);
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].path, "../archive");
        assert!(candidates[0].is_directory);
    }

    #[test]
    fn home_discovery_preserves_tilde_namespace() {
        let cwd = PathBuf::from("/workspace/project");
        let home = PathBuf::from("/home/tester");
        let resolver = CountingResolver {
            reads: std::sync::Mutex::new(Vec::new()),
            entries: vec![FileCandidate {
                path: "notes.txt".into(),
                is_directory: false,
            }],
        };
        let candidates = explicit_candidates(&resolver, &cwd, Some(&home), "~/not");
        assert_eq!(resolver.reads.lock().unwrap().as_slice(), &[home]);
        assert_eq!(candidates[0].path, "~/notes.txt");
    }

    #[test]
    fn quoted_directory_advance_keeps_quote_open() {
        let item = CompletionItem::directory("../release notes".into());
        assert_eq!(
            item.advance_replacement(),
            format!("@\"../release notes{}", DIRECTORY_SUFFIX)
        );
        assert_eq!(
            item.replacement(),
            format!("@\"../release notes{}\"", DIRECTORY_SUFFIX)
        );
    }

    #[test]
    fn explicit_directory_selection_returns_advance() {
        let mut menu = session_with_items(Vec::new());
        let session = menu.session.as_mut().unwrap();
        let item = CompletionItem::directory("../archive".into());
        session.matches.push(Candidate {
            item,
            matching: completion_match_default("ar", "../archive").unwrap(),
            source_rank: 0,
            source_order: 0,
            descendable: true,
        });
        session.visible = true;
        session.walking = false;
        assert!(matches!(
            menu.handle_key(key(KeyCode::Enter)),
            CompletionAction::Advance(item) if item.insertion.ends_with(DIRECTORY_SUFFIX)
        ));
    }

    fn key(code: KeyCode) -> KeyEvent {
        key_with(code, KeyModifiers::NONE)
    }

    fn key_with(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent {
            code,
            modifiers,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        }
    }

    #[test_case("@", 1        => Some((0, 1))  ; "bare_at")]
    #[test_case("@src", 4      => Some((0, 4))  ; "query_no_prefix")]
    #[test_case(" @src", 5     => Some((1, 5))  ; "space_prefixed")]
    #[test_case("prefix @src", 11 => Some((7, 11)) ; "word_then_token")]
    #[test_case("foo@bar", 7   => None          ; "mid_word_at_rejected")]
    #[test_case("@src tail", 3 => Some((0, 3))  ; "cursor_mid_token")]
    fn at_token_cases(line: &str, cursor: usize) -> Option<(usize, usize)> {
        at_token_range(line, cursor)
    }

    #[test_case("@src tail", 9 => None; "unquoted_whitespace_closes")]
    #[test_case("@\"src tail", 10 => Some((0, 10)); "unfinished_quote_keeps_spaces")]
    #[test_case("@\"src tail\"", 11 => Some((0, 11)); "closed_quote_is_active_at_end")]
    fn active_at_token_quote_and_whitespace_cases(
        line: &str,
        cursor: usize,
    ) -> Option<(usize, usize)> {
        at_token_range(line, cursor)
    }

    #[test_case("@main.rs" => "@main.rs"; "safe_file")]
    #[test_case("@docs/read me.md" => "@\"docs/read me.md\""; "file_space")]
    #[test_case("@skill:release review " => "@skill:\"release review\" "; "tagged_space_and_delimiter")]
    #[test_case("@file?" => "@\"file?\""; "trailing_punctuation")]
    #[test_case("@\"already quoted\"" => "@\"already quoted\""; "already_quoted")]
    #[test_case("@say\"what?" => "@'say\"what?'"; "alternate_delimiter")]
    fn completion_replacement_quotes_unsafe_values(insertion: &str) -> String {
        normalize_completion_insertion(insertion, false)
    }

    #[test]
    fn windows_drive_file_insertion_quotes_whole_path() {
        let item = CompletionItem::file(r"C:\Program Files\notes.txt".into());
        assert_eq!(item.replacement(), r#"@"C:\\Program Files\\notes.txt""#);
    }

    #[test]
    fn insertion_replaces_token_keeps_single_at() {
        let mut buf = TextBuffer::new("foo @xyz".into());
        let range = at_token_range(&buf.lines()[0], 8).unwrap();
        let item = CompletionItem::file("docs/read me.md".into());
        buf.replace_range_on_current_line(range.0, range.1, &item.replacement());
        assert_eq!(buf.value(), "foo @\"docs/read me.md\"");
    }

    #[test]
    fn skill_replacement_uses_skill_prefix() {
        let mut buf = TextBuffer::new("foo @".into());
        let range = at_token_range(&buf.lines()[0], 5).unwrap();
        let item = CompletionItem {
            label: "skill:review".into(),
            kind: "skill".into(),
            insertion: "@skill:review".into(),
            description: None,
        };
        buf.replace_range_on_current_line(range.0, range.1, &item.replacement());
        assert_eq!(buf.value(), "foo @skill:review");
    }

    #[test]
    fn cursor_lands_after_insertion() {
        let mut buf = TextBuffer::new("foo @xyz".into());
        let range = at_token_range(&buf.lines()[0], 8).unwrap();
        assert_eq!(range, (4, 8)); // `foo @xyz` -> token is `@xyz`
        let item = CompletionItem::file("main.rs".into());
        buf.replace_range_on_current_line(range.0, range.1, &item.replacement());
        assert_eq!(buf.value(), "foo @main.rs");
        assert_eq!(buf.x(), 12); // cursor just past the inserted `@main.rs`
    }

    #[test]
    fn name_needle_matches_refs_in_unified_list() {
        let mut menu = session_with_items(vec![item("skill:review", "skill", "@skill:review")]);
        menu.sync_query("rev");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["skill:review"]);
    }

    #[test]
    fn skill_prefix_filters_to_skills_only() {
        let mut menu = session_with_items(vec![
            item("skill:review", "skill", "@skill:review"),
            item("skill:tests", "skill", "@skill:tests"),
        ]);
        menu.sync_query("skill:");
        let s = menu.session.as_ref().unwrap();
        assert!(s.matches.iter().all(|c| c.item.kind == "skill"));
        assert_eq!(s.matches.len(), 2);

        menu.sync_query("skill:t");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["skill:tests"]);
    }

    #[test]
    fn skills_match_without_prefix() {
        let mut menu = session_with_items(vec![item("skill:review", "skill", "@skill:review")]);
        menu.sync_query("rev");
        let s = menu.session.as_ref().unwrap();
        let offered: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert!(offered.contains(&"skill:review"));
    }

    #[test]
    fn subagent_replacement_has_prefix_and_trailing_space() {
        let item = CompletionItem {
            label: "subagent:research".into(),
            kind: "subagent".into(),
            insertion: "@subagent:research ".into(),
            description: None,
        };
        assert_eq!(item.replacement(), "@subagent:research ");
    }

    #[test]
    fn model_replacement_has_prefix_and_trailing_space() {
        let item = CompletionItem {
            label: "model:zai/glm-5".into(),
            kind: "model".into(),
            insertion: "@model:zai/glm-5 ".into(),
            description: None,
        };
        assert_eq!(item.replacement(), "@model:zai/glm-5 ");
    }

    fn session_with_all() -> FileCompletionMenu {
        session_with_items(vec![
            item("skill:review", "skill", "@skill:review"),
            item("subagent:research", "subagent", "@subagent:research "),
            item("subagent:general", "subagent", "@subagent:general "),
            item("model:zai/glm-5", "model", "@model:zai/glm-5 "),
            item(
                "model:anthropic/claude",
                "model",
                "@model:anthropic/claude ",
            ),
        ])
    }

    #[test]
    fn subagent_prefix_filters_to_subagents() {
        let mut menu = session_with_all();
        menu.sync_query("subagent:");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["subagent:research", "subagent:general"]);
        assert!(s.matches.iter().all(|c| c.item.kind == "subagent"));
    }

    #[test]
    fn subagent_prefix_without_colon_filters_to_subagents() {
        let mut menu = session_with_all();
        menu.sync_query("subagent");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["subagent:research", "subagent:general"]);
        assert!(s.matches.iter().all(|c| c.item.kind == "subagent"));
    }

    #[test]
    fn a_short_prefix_filters_to_subagents() {
        let mut menu = session_with_all();
        menu.sync_query("a:rese");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["subagent:research"]);
    }

    #[test]
    fn model_prefix_filters_to_models() {
        let mut menu = session_with_all();
        menu.sync_query("model:");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["model:zai/glm-5", "model:anthropic/claude"]);
        assert!(s.matches.iter().all(|c| c.item.kind == "model"));
    }

    #[test]
    fn m_short_prefix_filters_to_models() {
        let mut menu = session_with_all();
        menu.sync_query("m:claude");
        let s = menu.session.as_ref().unwrap();
        let labels: Vec<_> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["model:anthropic/claude"]);
    }

    #[test]
    fn s_short_prefix_matches_skills_and_subagents() {
        // `s:` fuzzy-matches both `skill:` and `subagent:` labels; the unified
        // list shows both, and the user narrows with `sk:` or `su:`.
        let mut menu = session_with_all();
        menu.sync_query("s:");
        let s = menu.session.as_ref().unwrap();
        let kinds: Vec<_> = s.matches.iter().map(|c| c.item.kind.as_str()).collect();
        assert!(kinds.contains(&"skill"));
        assert!(kinds.contains(&"subagent"));
    }

    #[test]
    fn bare_at_shows_all_ref_kinds() {
        let mut menu = session_with_all();
        menu.sync_query("");
        let s = menu.session.as_ref().unwrap();
        let mut kinds = s
            .matches
            .iter()
            .map(|c| c.item.kind.as_str())
            .collect::<Vec<_>>();
        kinds.sort();
        kinds.dedup();
        assert_eq!(kinds, vec!["model", "skill", "subagent"]);
    }

    #[test]
    fn bare_at_lists_files_before_refs() {
        let mut menu = session_with_all();
        let s = menu.session.as_mut().unwrap();
        s.file_matches = (0..64)
            .map(|i| Candidate {
                item: CompletionItem::file(format!("src/file{i}.rs")),
                matching: CompletionMatch {
                    indices: Vec::new(),
                    ranking: CompletionRanking {
                        quality_rank: 4,
                        boundary_rank: 1,
                        start_index: 0,
                        gap_count: 0,
                        span_length: 0,
                        unmatched_suffix: 0,
                        fuzzy_score: 0,
                    },
                },
                source_rank: 0,
                source_order: i,
                descendable: false,
            })
            .collect();
        menu.sync_query("");
        let s = menu.session.as_ref().unwrap();
        assert!(!s.matches.is_empty());
        assert_eq!(s.matches[0].item.kind, "file");
    }

    #[test]
    fn prefix_match_ranks_before_non_prefix_files() {
        let mut menu = session_with_all();
        let s = menu.session.as_mut().unwrap();
        s.file_matches = vec![Candidate {
            item: CompletionItem::file("novo_nordisk_report.csv".into()),
            matching: CompletionMatch {
                indices: Vec::new(),
                ranking: CompletionRanking {
                    quality_rank: 4,
                    boundary_rank: 1,
                    start_index: 0,
                    gap_count: 0,
                    span_length: 0,
                    unmatched_suffix: 0,
                    fuzzy_score: 0,
                },
            },
            source_rank: 0,
            source_order: 0,
            descendable: false,
        }];
        project_nucleo_mut(s).restart(true);
        s.file_matches.clear();
        menu.sync_query("sk");
        let s = menu.session.as_ref().unwrap();
        assert!(!s.matches.is_empty());
        assert_eq!(s.matches[0].item.kind, "skill");
    }

    #[test]
    fn candidate_carries_kind_for_theme_lookup() {
        let mut menu = session_with_items(vec![item("skill:review", "skill", "@skill:review")]);
        menu.sync_query("skill:");
        let s = menu.session.as_ref().unwrap();
        assert_eq!(s.matches[0].item.kind, "skill");
        let t = theme::current();
        // Every kind plugins can emit is seeded in the theme's lookup, so
        // rendering never silently falls back to the generic item style.
        for kind in ["file", "skill", "subagent", "model"] {
            assert!(
                t.completion_kinds.contains_key(kind),
                "kind {kind} not seeded"
            );
        }
        assert!(s.matches[0].matching.indices.is_empty());
    }

    #[test]
    fn completion_kind_highlights_do_not_collapse_into_item_colour() {
        let t = theme::InMemoryThemesProvider::bundled()
            .load("lunared")
            .unwrap();
        // A kind highlight equal to the base item colour renders as plain
        // unhighlighted text, hiding prefix matches whose label starts with
        // the kind name.
        for kind in ["file", "skill", "subagent", "model"] {
            let style = t.completion_kinds.get(kind).expect("kind seeded");
            assert_ne!(
                style.fg, t.item.fg,
                "{kind} highlight equals the item colour"
            );
        }
    }

    fn menu_with_matches(count: usize) -> FileCompletionMenu {
        let mut menu = session_with_items(Vec::new());
        let s = menu.session.as_mut().unwrap();
        s.matches = (0..count)
            .map(|i| Candidate {
                item: CompletionItem::file(format!("file{i}")),
                matching: CompletionMatch {
                    indices: Vec::new(),
                    ranking: CompletionRanking {
                        quality_rank: 4,
                        boundary_rank: 1,
                        start_index: 0,
                        gap_count: 0,
                        span_length: 0,
                        unmatched_suffix: 0,
                        fuzzy_score: 0,
                    },
                },
                source_rank: 0,
                source_order: i,
                descendable: false,
            })
            .collect();
        menu
    }

    #[test_case(0, -5, 0    ; "clamps_at_start")]
    #[test_case(4, 5, 4     ; "clamps_at_end")]
    #[test_case(2, 1, 3     ; "moves_down")]
    #[test_case(2, -1, 1    ; "moves_up")]
    fn move_selection_behavior(start: usize, delta: isize, expected: usize) {
        let mut menu = menu_with_matches(5);
        let s = menu.session.as_mut().unwrap();
        s.viewport_height = 10;
        s.selected = start;
        move_selection(s, delta);
        assert_eq!(s.selected, expected);
    }

    #[test_case(1, -1, 0   ; "left_steps_to_prev_column")]
    #[test_case(0, -1, 0   ; "left_clamps_at_first_column")]
    #[test_case(0, 1, 1    ; "right_steps_to_next_column")]
    #[test_case(1, 1, 1    ; "right_clamps_at_row_end")]
    #[test_case(4, 1, 4    ; "partial_last_row_clamps")]
    fn move_column_behavior(start: usize, delta: isize, expected: usize) {
        // 5 items in 2 columns: row 0 = 0,1; row 1 = 2,3; row 2 = 4.
        let mut menu = menu_with_matches(5);
        let s = menu.session.as_mut().unwrap();
        s.cols = 2;
        s.viewport_height = 10;
        s.selected = start;
        move_column(s, delta);
        assert_eq!(s.selected, expected);
    }

    #[test]
    fn left_right_consumed_and_step_columns() {
        let mut menu = menu_with_matches(5);
        let s = menu.session.as_mut().unwrap();
        s.visible = true;
        s.cols = 2;
        assert!(matches!(
            menu.handle_key(key(KeyCode::Right)),
            CompletionAction::Consumed
        ));
        assert_eq!(menu.session.as_ref().unwrap().selected, 1);
        // Row 0 is full (0,1); a further Right clamps in place.
        assert!(matches!(
            menu.handle_key(key(KeyCode::Right)),
            CompletionAction::Consumed
        ));
        assert_eq!(menu.session.as_ref().unwrap().selected, 1);
        assert!(matches!(
            menu.handle_key(key(KeyCode::Left)),
            CompletionAction::Consumed
        ));
        assert_eq!(menu.session.as_ref().unwrap().selected, 0);
    }

    #[test]
    fn modified_left_right_pass_through_to_buffer() {
        let word_motion_mods = [
            KeyModifiers::CONTROL,
            KeyModifiers::ALT,
            KeyModifiers::CONTROL | KeyModifiers::SHIFT,
        ];
        for mods in word_motion_mods {
            let mut menu = menu_with_matches(5);
            let s = menu.session.as_mut().unwrap();
            s.visible = true;
            s.cols = 2;
            for code in [KeyCode::Left, KeyCode::Right] {
                assert!(
                    matches!(
                        menu.handle_key(key_with(code, mods)),
                        CompletionAction::Passthrough
                    ),
                    "{mods:?}+{code:?} should reach the prompt buffer"
                );
                assert_eq!(menu.session.as_ref().unwrap().selected, 0);
            }
        }
    }

    #[test]
    fn enter_returns_select() {
        let mut menu = menu_with_matches(3);
        menu.session.as_mut().unwrap().visible = true;
        match menu.handle_key(key(KeyCode::Enter)) {
            CompletionAction::Select(item) => assert_eq!(item.label, "file0"),
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn esc_returns_close() {
        let mut menu = menu_with_matches(3);
        menu.session.as_mut().unwrap().visible = true;
        assert!(matches!(
            menu.handle_key(key(KeyCode::Esc)),
            CompletionAction::Close
        ));
    }

    #[test]
    fn other_keys_passthrough_and_updown_consumed() {
        let mut menu = menu_with_matches(3);
        menu.session.as_mut().unwrap().visible = true;
        assert!(matches!(
            menu.handle_key(key(KeyCode::Char('a'))),
            CompletionAction::Passthrough
        ));
        let sel = menu.session.as_ref().unwrap().selected;
        assert!(matches!(
            menu.handle_key(key(KeyCode::Down)),
            CompletionAction::Consumed
        ));
        assert_eq!(menu.session.as_ref().unwrap().selected, sel + 1);
    }

    #[test]
    fn highlight_indices_shared_for_prefix_and_fuzzy() {
        assert_eq!(highlight_indices("skill:review", "sk"), Some(vec![0, 1]));
        // Fuzzy: the needle is scattered through the label.
        assert_eq!(
            highlight_indices("skill:review", "rev"),
            Some(vec![6, 7, 8])
        );
        assert!(highlight_indices("skill:review", "zzz").is_none());
    }

    #[test]
    fn empty_kind_queries_do_not_highlight() {
        let mut menu = session_with_items(vec![item("skill:review", "skill", "@skill:review")]);
        menu.sync_query("sk");
        assert!(
            menu.session.as_ref().unwrap().matches[0]
                .matching
                .indices
                .is_empty()
        );
    }

    #[test]
    fn selected_row_fuzzy_match_spans_keep_selection_background() {
        let mut menu = session_with_items(vec![item("skill:review", "skill", "@skill:review")]);
        menu.sync_query("rev");
        let c = menu.session.as_ref().unwrap().matches[0].clone();
        assert_eq!(
            c.matching.indices,
            vec![6, 7, 8],
            "fuzzy query must highlight the scattered chars"
        );

        let t = theme::InMemoryThemesProvider::bundled()
            .load("lunared")
            .unwrap();
        let line = cell_line(&c, 40, true, &t);
        let kind = t.completion_kinds.get("skill").copied().unwrap_or(t.item);
        let expected_match = Style {
            bg: t.item_selected.bg,
            ..kind
        };

        // The scattered match still carries the selection background, so it is
        // not cut out of the selected row.
        let match_span = line
            .spans
            .iter()
            .find(|sp| sp.content.as_ref() == "rev")
            .expect("matched span present");
        assert_eq!(match_span.style, expected_match);
        let lead_span = line
            .spans
            .iter()
            .find(|sp| sp.content.as_ref() == "skill:")
            .expect("leading span present");
        assert_eq!(lead_span.style, t.item_selected);
    }

    fn labels(menu: &FileCompletionMenu) -> Vec<String> {
        menu.session
            .as_ref()
            .unwrap()
            .matches
            .iter()
            .map(|candidate| candidate.item.label.clone())
            .collect()
    }

    fn add_file(menu: &mut FileCompletionMenu, path: &str) {
        let session = menu.session.as_mut().unwrap();
        let order = session.file_matches.len();
        let candidate = match_candidate(
            CompletionItem::file(path.into()),
            &session.intent,
            0,
            order,
            false,
        )
        .unwrap();
        session.file_matches.push(candidate);
        rebuild_combined(session);
    }

    #[test]
    fn completion_quality_tiers_are_ordered() {
        let mut menu = session_with_items(vec![
            item("zzneedle", "skill", "@zzneedle"),
            item("needle-long", "model", "@needle-long"),
            item("a-needle", "subagent", "@a-needle"),
            item("nxeedlxe", "skill", "@nxeedlxe"),
        ]);
        menu.sync_query("needle");
        assert_eq!(
            labels(&menu),
            vec!["needle-long", "a-needle", "zzneedle", "nxeedlxe"]
        );
    }

    #[test]
    fn closer_match_beats_longer_suffix_match() {
        let mut menu = session_with_items(vec![
            item("github-issue-simple", "skill", "@github-issue-simple"),
            item("github-issue", "skill", "@github-issue"),
        ]);
        menu.sync_query("github-iss");
        assert_eq!(labels(&menu), vec!["github-issue", "github-issue-simple"]);
    }

    #[test]
    fn textual_quality_beats_default_file_preference() {
        let mut menu = session_with_items(vec![item("tagged", "skill", "@tagged")]);
        menu.sync_query("tag");
        menu.session.as_mut().unwrap().file_matches.push(Candidate {
            item: CompletionItem::file("weak_match".into()),
            matching: CompletionMatch {
                indices: Vec::new(),
                ranking: CompletionRanking {
                    quality_rank: 3,
                    boundary_rank: 1,
                    start_index: 0,
                    gap_count: 0,
                    span_length: 0,
                    unmatched_suffix: 0,
                    fuzzy_score: 0,
                },
            },
            source_rank: 0,
            source_order: 0,
            descendable: false,
        });
        rebuild_combined(menu.session.as_mut().unwrap());
        assert_eq!(labels(&menu), vec!["tagged", "weak_match"]);
    }

    #[test]
    fn files_win_equal_quality_ties() {
        let mut menu = session_with_items(vec![item("tag", "skill", "@tag")]);
        menu.sync_query("tag");
        add_file(&mut menu, "tag-file");
        assert_eq!(labels(&menu), vec!["tag", "tag-file"]);
    }

    #[test]
    fn explicit_kind_query_ranks_payload_and_offsets_highlights() {
        let mut menu = session_with_items(vec![
            item("skill:testing", "skill", "@skill:testing"),
            item("skill:review", "skill", "@skill:review"),
            item("model:testing", "model", "@model:testing"),
        ]);
        menu.sync_query("sk:tes");
        let session = menu.session.as_ref().unwrap();
        assert_eq!(labels(&menu), vec!["skill:testing"]);
        assert_eq!(session.matches[0].matching.indices, vec![6, 7, 8]);
    }

    #[test]
    fn explicit_kind_prefixes_filter_candidates() {
        for query in ["skill:", "sk:", "subagent:", "su:", "a:", "model:", "m:"] {
            let mut menu = session_with_all();
            menu.sync_query(query);
            let kind = menu.session.as_ref().unwrap().matches[0].item.kind.clone();
            assert!(
                menu.session
                    .as_ref()
                    .unwrap()
                    .matches
                    .iter()
                    .all(|candidate| candidate.item.kind == kind)
            );
        }
    }

    #[test]
    fn ambiguous_kind_remains_broad() {
        let mut menu = session_with_all();
        menu.sync_query("s:");
        let kinds: Vec<_> = menu
            .session
            .as_ref()
            .unwrap()
            .matches
            .iter()
            .map(|candidate| candidate.item.kind.as_str())
            .collect();
        assert!(kinds.contains(&"skill"));
        assert!(kinds.contains(&"subagent"));
    }

    #[test]
    fn unrecognized_kind_prefix_is_free_text() {
        let mut menu = session_with_items(vec![item("x:needle", "skill", "@x:needle")]);
        menu.sync_query("x:nee");
        assert_eq!(labels(&menu), vec!["x:needle"]);
    }

    #[test]
    fn concatenated_aliases_remain_free_text() {
        let mut menu = session_with_items(vec![
            item("skillfoo", "skill", "@skillfoo"),
            item("modelish", "model", "@modelish"),
            item("subagent-tools", "subagent", "@subagent-tools"),
        ]);
        for query in ["skillfoo", "modelish", "subagent-tools"] {
            menu.sync_query(query);
            assert_eq!(labels(&menu), vec![query]);
        }
    }

    #[test]
    fn case_insensitive_ranking_and_codepoint_highlights() {
        let mut menu = session_with_items(vec![item("東京", "skill", "@東京")]);
        let intent = parse_query("東");
        let session = menu.session.as_mut().unwrap();
        session.query = "東".into();
        session.ref_matches = fuzzy_match(
            &intent,
            session
                .ref_items
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, (label, item))| (label, item, i)),
        );
        rebuild_combined(session);
        assert_eq!(session.ref_matches.len(), 1);
        let candidate = &menu.session.as_ref().unwrap().matches[0];
        assert_eq!(candidate.matching.indices, vec![0]);
        assert_eq!(candidate.item.label, "東京");
    }

    #[test]
    fn explicit_non_file_kind_queries_exclude_files() {
        let mut menu = session_with_items(vec![item("skill:one", "skill", "@skill:one")]);
        menu.sync_query("skill:");
        assert!(
            menu.session
                .as_ref()
                .unwrap()
                .matches
                .iter()
                .all(|candidate| candidate.item.kind != FILE_KIND)
        );
    }

    #[test]
    fn equal_rank_preserves_source_order() {
        let mut menu = session_with_items(vec![
            item("same", "skill", "@same"),
            item("same", "model", "@same-model"),
        ]);
        menu.sync_query("same");
        assert_eq!(labels(&menu), vec!["same", "same"]);
    }

    #[test]
    fn file_refresh_rebuilds_with_heuristic_order() {
        let mut menu = session_with_items(Vec::new());
        let session = menu.session.as_mut().unwrap();
        session.walking = false;
        project_nucleo_mut(session)
            .injector()
            .push((), |_, columns| {
                columns[0] = Utf32String::from("weak_match");
            });
        project_nucleo_mut(session)
            .injector()
            .push((), |_, columns| {
                columns[0] = Utf32String::from("needle-file");
            });
        menu.sync_query("needle");
        for _ in 0..100 {
            let _ = menu.tick();
            if !menu.session.as_ref().unwrap().matches.is_empty() {
                break;
            }
        }
        let session = menu.session.as_ref().unwrap();
        assert_eq!(labels(&menu), vec!["needle-file"]);
        assert_eq!(session.coarse_match_count, 1);
        assert_eq!(session.materialized_count, 1);
        assert_eq!(session.final_match_count, 1);
        assert!(!session.truncated);
    }

    #[test]
    fn file_refresh_uses_lexical_source_order_for_ties() {
        let mut menu = session_with_items(Vec::new());
        {
            let session = menu.session.as_mut().unwrap();
            session.walking = false;
            project_nucleo_mut(session)
                .injector()
                .push((), |_, columns| {
                    columns[0] = Utf32String::from("zeta-file");
                });
            project_nucleo_mut(session)
                .injector()
                .push((), |_, columns| {
                    columns[0] = Utf32String::from("alpha-file");
                });
        }
        menu.sync_query("");
        for _ in 0..100 {
            let _ = menu.tick();
            if menu.session.as_ref().unwrap().file_matches.len() == 2 {
                break;
            }
        }
        let session = menu.session.as_ref().unwrap();
        let labels: Vec<_> = session
            .file_matches
            .iter()
            .map(|candidate| candidate.item.label.as_str())
            .collect();
        assert_eq!(labels, vec!["alpha-file", "zeta-file"]);
        assert_eq!(session.materialized_count, 2);
        assert!(!session.truncated);
    }

    #[test]
    fn file_refresh_rejects_coarse_matches_for_non_file_kind_queries() {
        let mut menu = session_with_items(Vec::new());
        {
            let session = menu.session.as_mut().unwrap();
            session.walking = false;
            project_nucleo_mut(session)
                .injector()
                .push((), |_, columns| {
                    columns[0] = Utf32String::from("skill:example");
                });
        }
        menu.sync_query("skill:");
        {
            let session = menu.session.as_mut().unwrap();
            while project_nucleo_mut(session).tick(0).running {}
            refresh_file_matches(session);
        }

        let session = menu.session.as_ref().unwrap();
        assert_eq!(session.coarse_match_count, 1);
        assert_eq!(session.materialized_count, 1);
        assert_eq!(session.final_match_count, 0);
        assert!(!session.truncated);
        assert!(session.file_matches.is_empty());
    }

    #[test]
    fn file_refresh_tracks_materialization_boundary() {
        let mut menu = session_with_items(Vec::new());
        {
            let session = menu.session.as_mut().unwrap();
            session.walking = false;
            for index in 0..=MAX_MATERIALIZED {
                project_nucleo_mut(session)
                    .injector()
                    .push((), |_, columns| {
                        columns[0] = Utf32String::from(format!("file-{index:03}.rs").as_str());
                    });
            }
            while project_nucleo_mut(session).tick(0).running {}
            refresh_file_matches(session);
        }
        let session = menu.session.as_ref().unwrap();
        assert_eq!(session.coarse_match_count, MAX_MATERIALIZED + 1);
        assert_eq!(session.materialized_count, MAX_MATERIALIZED);
        assert_eq!(session.final_match_count, MAX_MATERIALIZED);
        assert!(session.truncated);
        assert_eq!(session.file_matches.len(), MAX_MATERIALIZED as usize);
        assert_eq!(session.file_matches[0].item.label, "file-000.rs");
        assert_eq!(
            session.file_matches.last().unwrap().item.label,
            format!("file-{:03}.rs", MAX_MATERIALIZED - 1)
        );
    }

    #[test]
    fn refresh_clamps_selection_after_reordering() {
        let mut menu = session_with_items(Vec::new());
        let session = menu.session.as_mut().unwrap();
        project_nucleo_mut(session)
            .injector()
            .push((), |_, columns| {
                columns[0] = Utf32String::from("needle-file");
            });
        session.walking = false;
        session.selected = 99;
        project_nucleo_mut(session).tick(0);
        menu.sync_query("needle");
        let _ = menu.tick();
        assert_eq!(menu.session.as_ref().unwrap().selected, 0);
    }

    #[test]
    fn refresh_then_accept_inserts_selected_item() {
        let mut menu = session_with_items(Vec::new());
        let session = menu.session.as_mut().unwrap();
        project_nucleo_mut(session)
            .injector()
            .push((), |_, columns| {
                columns[0] = Utf32String::from("needle-file");
            });
        session.walking = false;
        menu.sync_query("needle");
        wait_for_matcher(
            &mut menu,
            |menu| {
                let session = menu.session.as_ref().unwrap();
                !session.query_refresh_pending && !session.file_matches.is_empty()
            },
            "file matcher did not settle",
        );
        menu.session.as_mut().unwrap().visible = true;
        assert!(
            matches!(menu.handle_key(key(KeyCode::Enter)), CompletionAction::Select(item) if item.label == "needle-file")
        );
    }

    #[test]
    fn host_and_guest_candidates_share_heuristic_order() {
        let mut menu = session_with_items(vec![item("needle-plugin", "skill", "@needle-plugin")]);
        menu.sync_query("needle");
        add_file(&mut menu, "needle-file");
        assert_eq!(labels(&menu), vec!["needle-file", "needle-plugin"]);
    }

    #[test]
    fn query_refresh_keeps_previous_result_set_until_matching_finishes() {
        let mut menu = session_with_items(Vec::new());
        {
            let session = menu.session.as_mut().unwrap();
            session.walking = false;
            for path in ["alpha-file", "beta-file", "gamma-file"] {
                project_nucleo_mut(session)
                    .injector()
                    .push((), |_, columns| {
                        columns[0] = Utf32String::from(path);
                    });
            }
        }
        menu.sync_query("");
        wait_for_matcher(
            &mut menu,
            |menu| menu.session.as_ref().unwrap().file_matches.len() == 3,
            "file matcher did not surface the three injected files",
        );
        let before = labels(&menu);
        assert_eq!(before, vec!["alpha-file", "beta-file", "gamma-file"]);

        menu.sync_query("gamma");
        assert!(menu.session.as_ref().unwrap().query_refresh_pending);
        assert_eq!(labels(&menu), before);

        wait_for_matcher(
            &mut menu,
            |menu| !menu.session.as_ref().unwrap().query_refresh_pending,
            "file matcher did not settle on the filtered gamma query",
        );
        assert_eq!(labels(&menu), vec!["gamma-file"]);
    }

    #[test]
    fn view_popup_above_input_area() {
        let mut menu = menu_with_matches(3);
        let s = menu.session.as_mut().unwrap();
        s.visible = true;
        s.walking = false;
        s.started_at = Instant::now() - std::time::Duration::from_secs(1);
        let backend = TestBackend::new(40, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        let input_area = Rect {
            x: 0,
            y: 10,
            width: 40,
            height: 3,
        };
        terminal
            .draw(|frame| {
                let rect = menu.view(frame, input_area).unwrap();
                assert_eq!(rect.y, 10 - rect.height);
            })
            .unwrap();
    }

    #[test]
    fn uppercase_file_query_does_not_panic() {
        // Nucleo uses a normalized retrieval query, while final matching and
        // highlighting use the original query.
        let mut menu = session_with_items(Vec::new());
        let s = menu.session.as_mut().unwrap();
        for path in ["Cargo.lock", "justfile", "maki-ui/src/app/mod.rs"] {
            project_nucleo_mut(s).injector().push((), |_, cols| {
                cols[0] = Utf32String::from(path);
            });
        }
        s.walking = false;
        while project_nucleo_mut(s).tick(0).running {}

        menu.sync_query("Cargo");
        wait_for_matcher(
            &mut menu,
            |menu| !menu.session.as_ref().unwrap().matching,
            "file matcher did not settle",
        );
        let s = menu.session.as_ref().unwrap();
        assert_eq!(s.query, "Cargo");
        let c = s
            .file_matches
            .iter()
            .find(|c| c.item.label == "Cargo.lock")
            .expect("Cargo.lock matches the Cargo query");
        assert_eq!(c.matching.indices, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn uppercase_ref_query_does_not_panic() {
        // Refs go through the same matcher in sync_query; `Model:` against
        // `model:zai/glm-5` is the same uppercase-needle panic on the ref side.
        let mut menu = session_with_all();
        menu.sync_query("Model:");
        let s = menu.session.as_ref().unwrap();
        assert_eq!(s.query, "Model:");
        let labels: Vec<&str> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["model:zai/glm-5", "model:anthropic/claude"]);
    }
}
