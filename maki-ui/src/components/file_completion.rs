use std::mem;
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

/// `maki_lua::at_is_token_start` re-export so the popup and the (Lua-side)
/// expander parser agree on what counts as a reference. Kept here as the
/// canonical entry for `maki-ui` callers.
pub(crate) use maki_lua::at_is_token_start;

/// Byte range of the `@`-token under the cursor (including its leading `@`),
/// or `None` when the most recent `@` does not begin a token (e.g. `foo@bar`).
pub fn at_token_range(line: &str, cursor_chars: usize) -> Option<(usize, usize)> {
    let cursor_byte = TextBuffer::char_to_byte(line, cursor_chars);
    let before = &line[..cursor_byte];
    let bytes = before.as_bytes();
    let mut i = before.len();
    while i > 0 {
        i -= 1;
        if bytes[i] != b'@' {
            continue;
        }
        if at_is_token_start(line, i) {
            return Some((i, cursor_byte));
        }
    }
    None
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
        self.insertion.clone()
    }

    fn display(&self) -> String {
        match &self.description {
            Some(d) if !d.is_empty() => format!("{}  {}", self.label, d),
            _ => self.label.clone(),
        }
    }

    fn file(path: String) -> Self {
        Self {
            label: path.clone(),
            kind: FILE_KIND.to_string(),
            insertion: format!("@{path}"),
            description: None,
        }
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    item: CompletionItem,
    matching: CompletionMatch,
    source_rank: u8,
    source_order: usize,
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
    Close,
    Passthrough,
}

struct Session {
    nucleo: Nucleo<()>,
    query: String,
    intent: QueryIntent,
    /// Non-file candidates from Lua sources, as `(matchable label, item)`.
    /// Re-fuzzy-matched against each new query in `sync_query`.
    ref_items: Vec<(String, CompletionItem)>,
    ref_matches: Vec<Candidate>,
    file_matches: Vec<Candidate>,
    matches: Vec<Candidate>,

    selected: usize,
    /// Grid layout: columns used, and scroll/viewport in whole rows.
    cols: usize,
    scroll_offset: usize,
    viewport_height: usize,

    cancel: Arc<AtomicBool>,
    done_rx: flume::Receiver<()>,
    started_at: Instant,

    walking: bool,
    matching: bool,
    visible: bool,

    token_byte_range: (usize, usize),
}

impl Drop for Session {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

pub struct FileCompletionMenu {
    session: Option<Session>,
}

impl FileCompletionMenu {
    pub fn new() -> Self {
        Self { session: None }
    }

    /// Open the popup. `items` are the non-file candidates gathered from Lua
    /// completion sources by the caller; the file walker is spawned for `cwd`.
    pub fn open(
        &mut self,
        cwd: &str,
        items: Vec<ItemSpec>,
        query: &str,
        token_byte_range: (usize, usize),
    ) {
        self.close();

        let Some((nucleo, done_rx, cancel_clone)) = super::file_picker::spawn_file_walker(cwd)
        else {
            return;
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
            nucleo,
            query: String::new(),
            intent: QueryIntent {
                payload: String::new(),
                kind: None,
                has_colon: false,
            },
            ref_items,
            ref_matches: Vec::new(),
            file_matches: Vec::new(),
            matches: Vec::new(),
            selected: 0,
            cols: 1,
            scroll_offset: 0,
            viewport_height: 0,
            cancel: cancel_clone,
            done_rx,
            started_at: Instant::now(),
            walking: true,
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
        let Some(s) = &mut self.session else {
            return;
        };
        s.nucleo
            .pattern
            .reparse(0, query, CaseMatching::Smart, Normalization::Smart, false);
        let intent = parse_query(query);
        let normalized_query = query.to_lowercase();
        if s.query != normalized_query {
            s.file_matches.clear();
        }
        s.intent = intent;
        s.query = normalized_query;
        s.selected = 0;
        s.scroll_offset = 0;

        s.ref_matches = fuzzy_match(
            &s.intent,
            s.ref_items
                .iter()
                .cloned()
                .enumerate()
                .map(|(order, (label, item))| (label, item, order)),
        );
        rebuild_combined(s);
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> CompletionAction {
        let Some(s) = &mut self.session else {
            return CompletionAction::Close;
        };

        match key.code {
            KeyCode::Esc => return CompletionAction::Close,
            KeyCode::Enter | KeyCode::Tab => {
                if !s.visible {
                    return CompletionAction::Passthrough;
                }
                return match s.matches.get(s.selected).map(|c| c.item.clone()) {
                    Some(item) => CompletionAction::Select(item),
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

        let status = s.nucleo.tick(0);
        s.matching = status.running;
        let mut dirty = Dirty::from(status.changed);

        if s.walking {
            match s.done_rx.try_recv() {
                Ok(()) => {
                    s.walking = false;
                    dirty = Dirty::YES;
                }
                Err(flume::TryRecvError::Disconnected) => {
                    warn!("{WALKER_CRASHED_MSG}: walker thread panicked");
                    self.session = None;
                    return (Dirty::YES, Some(WALKER_CRASHED_MSG.into()));
                }
                Err(flume::TryRecvError::Empty) => {}
            }
        }

        if !s.visible {
            let has_files = s.nucleo.injector().injected_items() > 0;
            let has_refs = !s.ref_matches.is_empty();
            let debounce_elapsed = s.started_at.elapsed().as_millis() >= PENDING_DEBOUNCE_MS;

            if has_files || has_refs || (s.walking && debounce_elapsed) {
                s.visible = true;
                dirty = Dirty::YES;
            }
        }

        if status.changed
            && let Some(s) = self.session.as_mut()
        {
            refresh_file_matches(s);
            rebuild_combined(s);
            clamp_selection(s);
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
    })
}

fn fuzzy_match(
    intent: &QueryIntent,
    items: impl IntoIterator<Item = (String, CompletionItem, usize)>,
) -> Vec<Candidate> {
    items
        .into_iter()
        .filter_map(|(_, item, order)| match_candidate(item, intent, 1, order))
        .collect()
}

fn refresh_file_matches(s: &mut Session) {
    let snapshot = s.nucleo.snapshot();
    let count = snapshot.matched_item_count().min(MAX_MATERIALIZED);
    let mut paths: Vec<String> = snapshot
        .matched_items(0..count)
        .map(|item| item.matcher_columns[0].to_string())
        .collect();
    paths.sort();
    s.file_matches.clear();
    for (order, path) in paths.into_iter().enumerate() {
        if let Some(candidate) = match_candidate(CompletionItem::file(path), &s.intent, 0, order) {
            s.file_matches.push(candidate);
        }
    }
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
    use std::time::Instant;

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

    fn item(label: &str, kind: &str, insertion: &str) -> ItemSpec {
        ItemSpec {
            label: label.into(),
            kind: kind.into(),
            insertion: insertion.into(),
            description: None,
        }
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
            nucleo,
            query: String::new(),
            intent: QueryIntent {
                payload: String::new(),
                kind: None,
                has_colon: false,
            },
            ref_items,
            ref_matches: Vec::new(),
            file_matches: Vec::new(),
            matches: Vec::new(),
            selected: 0,
            cols: 1,
            scroll_offset: 0,
            viewport_height: 0,
            cancel: Arc::new(AtomicBool::new(false)),
            done_rx,
            started_at: Instant::now(),
            walking: true,
            matching: false,
            visible: false,
            token_byte_range: (0, 0),
        });
        menu
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

    #[test]
    fn insertion_replaces_token_keeps_single_at() {
        let mut buf = TextBuffer::new("foo @xyz".into());
        let range = at_token_range(&buf.lines()[0], 8).unwrap();
        let item = CompletionItem::file("docs/read me.md".into());
        buf.replace_range_on_current_line(range.0, range.1, &item.replacement());
        assert_eq!(buf.value(), "foo @docs/read me.md");
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
        }];
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
        let candidate =
            match_candidate(CompletionItem::file(path.into()), &session.intent, 0, order).unwrap();
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
        menu.sync_query("needle");
        let session = menu.session.as_mut().unwrap();
        session.nucleo.injector().push((), |_, columns| {
            columns[0] = Utf32String::from("weak_match");
        });
        session.nucleo.injector().push((), |_, columns| {
            columns[0] = Utf32String::from("needle-file");
        });
        for _ in 0..100 {
            let _ = menu.tick();
            if !menu.session.as_ref().unwrap().matches.is_empty() {
                break;
            }
        }
        assert_eq!(labels(&menu), vec!["needle-file"]);
    }

    #[test]
    fn refresh_clamps_selection_after_reordering() {
        let mut menu = session_with_items(Vec::new());
        let session = menu.session.as_mut().unwrap();
        session.nucleo.injector().push((), |_, columns| {
            columns[0] = Utf32String::from("needle-file");
        });
        session.walking = false;
        session.selected = 99;
        session.nucleo.tick(0);
        menu.sync_query("needle");
        let _ = menu.tick();
        assert_eq!(menu.session.as_ref().unwrap().selected, 0);
    }

    #[test]
    fn refresh_then_accept_inserts_selected_item() {
        let mut menu = session_with_items(Vec::new());
        let session = menu.session.as_mut().unwrap();
        session.nucleo.injector().push((), |_, columns| {
            columns[0] = Utf32String::from("needle-file");
        });
        session.walking = false;
        session.nucleo.tick(0);
        menu.sync_query("needle");
        for _ in 0..100 {
            let _ = menu.tick();
            if !menu.session.as_ref().unwrap().file_matches.is_empty() {
                break;
            }
        }
        let session = menu.session.as_mut().unwrap();
        session.visible = true;
        session.matches = session.file_matches.clone();
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
        // The ignore_case matcher panics on an uppercase needle (its prefilter
        // is case-insensitive, the optimal matrix is not), so the session must
        // store a lowercased query. Backspacing `@Cargo.lock` is the original
        // crash: query `Cargo` matched `Cargo.lock` case-sensitively, then the
        // highlight pass ran with the raw uppercase needle.
        let mut menu = session_with_items(Vec::new());
        let s = menu.session.as_mut().unwrap();
        for path in ["Cargo.lock", "justfile", "maki-ui/src/app/mod.rs"] {
            s.nucleo.injector().push((), |_, cols| {
                cols[0] = Utf32String::from(path);
            });
        }
        s.walking = false;
        while s.nucleo.tick(0).running {}

        menu.sync_query("Cargo");
        for _ in 0..100 {
            let (_, _) = menu.tick();
            if !menu.session.as_ref().unwrap().file_matches.is_empty() {
                break;
            }
        }
        let s = menu.session.as_ref().unwrap();
        assert_eq!(s.query, "cargo");
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
        assert_eq!(s.query, "model:");
        let labels: Vec<&str> = s.matches.iter().map(|c| c.item.label.as_str()).collect();
        assert_eq!(labels, vec!["model:zai/glm-5", "model:anthropic/claude"]);
    }
}
