use std::mem;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use maki_match::{
    CompletionMatch, CompletionMatchOptions, CompletionRanking, compare_completion_matches,
    completion_match,
};
use nucleo_matcher::pattern::{CaseMatching, Normalization};

use crate::animation::{animation_elapsed_ms, spinner_str};
use crate::click::{ClickTarget, HitTarget, PickerId, ScrollOwner};
use crate::components::Overlay;
use crate::components::coherent_completion::Published;
use crate::components::is_ctrl;
use crate::components::keybindings::key;
use crate::components::modal::{CHROME_LINES, Modal};
use crate::components::scrollbar::{ScrollbarHit, render_vertical_scrollbar, saturating_u16};
use crate::repaint::Cadence;
use crate::text_buffer::TextBuffer;
use crate::theme;

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Position, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

const NO_MATCHES: &str = "No matches";
const MIN_WIDTH_PERCENT: u16 = 65;
const MAX_HEIGHT_PERCENT: u16 = 80;
const SEARCH_ROW: u16 = 1;
pub(super) const SEARCH_PREFIX_WIDTH: u16 = 2;
const DETAIL_RIGHT_PAD: u16 = 1;
const LABEL_PREFIX_WIDTH: usize = 2;
const DEPTH_INDENT_WIDTH: usize = 2;
const AGE_TITLE_MIN_WIDTH: usize = 4;
const AGE_ELLIPSIS_MIN_WIDTH: usize = 6;

/// One sampling of both clocks used by a picker draw. Monotonic time measures
/// task ages; Unix time measures timestamps supplied by Lua.
pub struct ClockSample {
    pub monotonic: Instant,
    pub unix: Duration,
}

impl ClockSample {
    pub fn now() -> Self {
        Self {
            monotonic: Instant::now(),
            unix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default(),
        }
    }
}

pub trait PickerItem {
    fn label(&self) -> &str;
    fn depth(&self) -> usize {
        0
    }
    fn suffix(&self) -> Option<&str> {
        None
    }
    fn detail(&self) -> Option<&str> {
        None
    }
    fn section(&self) -> Option<&str> {
        None
    }
    fn section_detail(&self) -> Option<&str> {
        None
    }
    fn is_spinning(&self) -> bool {
        false
    }
    fn is_highlighted(&self) -> bool {
        false
    }
    fn context_str(&self) -> Option<&str> {
        None
    }
    fn age(&self, _now: &ClockSample) -> Option<Duration> {
        None
    }
    fn is_finished(&self) -> bool {
        false
    }
}

impl PickerItem for String {
    fn label(&self) -> &str {
        self
    }
}

pub enum PickerAction<T> {
    Consumed,
    Select(T),
    Toggle(usize, bool),
    Delete(usize),
    Close,
}

pub struct ListPicker<T> {
    state: Option<State<T>>,
    picker: Option<PickerId>,
    scrollbar_hit: Option<ScrollbarHit>,
    row_hits: Vec<(Rect, usize)>,
    title: String,
    max_visible: Option<u16>,
    footer: Option<Line<'static>>,
    error_text: Option<String>,
    submit_keys: Vec<KeyEvent>,
    delete_key: Option<KeyEvent>,
    confirming_delete: Option<usize>,
}

struct FilteredItems {
    filtered: Vec<usize>,
    /// Per-`filtered`-entry label match codepoint indices; `None` when no
    /// query is active, `Some(empty)` when a word matched only the section.
    match_indices: Vec<Option<Vec<u32>>>,
}

struct State<T> {
    items: Vec<T>,
    publication: Published<String, FilteredItems>,
    selected: usize,
    search: TextBuffer,
    scroll_offset: usize,
    viewport_height: usize,
    inner_area: Rect,
    input_area: Rect,
    enabled: Option<Vec<bool>>,
}

impl<T: PickerItem> State<T> {
    fn new(items: Vec<T>) -> Self {
        let filtered: Vec<usize> = (0..items.len()).collect();
        let match_indices: Vec<Option<Vec<u32>>> = vec![None; filtered.len()];
        let mut publication = Published::default();
        publication.commit_sync(
            String::new(),
            FilteredItems {
                filtered,
                match_indices,
            },
        );
        Self {
            items,
            publication,
            selected: 0,
            search: TextBuffer::new(String::new()),
            scroll_offset: 0,
            viewport_height: 20,
            inner_area: Rect::default(),
            input_area: Rect::default(),
            enabled: None,
        }
    }

    fn filtered(&self) -> &[usize] {
        &self
            .publication
            .value()
            .expect("synchronous commit")
            .filtered
    }

    fn match_indices(&self) -> &[Option<Vec<u32>>] {
        &self
            .publication
            .value()
            .expect("synchronous commit")
            .match_indices
    }

    fn replace_items(&mut self, items: Vec<T>) {
        self.items = items;
        self.rebuild_filter();
        self.clamp_selection();
    }

    fn rebuild_filter(&mut self) {
        let query = self.search.value();
        let filtered = self.build_filter(&query);
        self.publication.commit_sync(query, filtered);
    }

    fn build_filter(&self, query: &str) -> FilteredItems {
        let options = CompletionMatchOptions {
            case_matching: CaseMatching::Smart,
            normalization: Normalization::Smart,
        };
        if query.split_whitespace().next().is_none() {
            let filtered: Vec<usize> = (0..self.items.len()).collect();
            return FilteredItems {
                match_indices: vec![None; filtered.len()],
                filtered,
            };
        }
        let mut kept = Vec::new();
        for (source_order, item) in self.items.iter().enumerate() {
            if let Some(completion) = completion_match(query, item.label(), options) {
                kept.push((source_order, completion));
                continue;
            }
            let mut indices = Vec::new();
            let mut fuzzy_score: u32 = 0;
            let mut matched = true;
            let mut label_matched = false;
            for term in query.split_whitespace() {
                let label_match = completion_match(term, item.label(), options);
                let section_match = item
                    .section()
                    .and_then(|section| completion_match(term, section, options));
                let Some(completion) = label_match.as_ref().or(section_match.as_ref()) else {
                    matched = false;
                    break;
                };
                let term_score = match (label_match.as_ref(), section_match.as_ref()) {
                    (Some(label), Some(section)) => {
                        label.ranking.fuzzy_score.min(section.ranking.fuzzy_score)
                    }
                    _ => completion.ranking.fuzzy_score,
                };
                fuzzy_score = fuzzy_score.saturating_add(term_score);
                if let Some(label_match) = label_match {
                    label_matched = true;
                    indices.extend(label_match.indices);
                }
            }
            if !matched {
                continue;
            }
            indices.sort_unstable();
            indices.dedup();
            kept.push((
                source_order,
                CompletionMatch {
                    indices: if label_matched { indices } else { Vec::new() },
                    ranking: CompletionRanking {
                        quality_rank: 4,
                        boundary_rank: 1,
                        start_index: usize::MAX,
                        gap_count: usize::MAX,
                        span_length: usize::MAX,
                        unmatched_suffix: usize::MAX,
                        fuzzy_score,
                    },
                },
            ));
        }
        if self.items.iter().any(|item| item.depth() > 0) {
            let mut included = vec![false; self.items.len()];
            let mut matches = vec![None; self.items.len()];
            for (index, completion) in kept {
                matches[index] = Some(completion.indices);
            }
            let mut ancestors: Vec<usize> = Vec::new();
            for (index, item) in self.items.iter().enumerate() {
                while ancestors
                    .last()
                    .is_some_and(|parent| self.items[*parent].depth() >= item.depth())
                {
                    ancestors.pop();
                }
                if matches[index].is_some() {
                    included[index] = true;
                    for &parent in ancestors.iter().rev() {
                        if included[parent] {
                            break;
                        }
                        included[parent] = true;
                    }
                }
                ancestors.push(index);
            }
            let filtered: Vec<usize> = (0..self.items.len())
                .filter(|&index| included[index])
                .collect();
            let match_indices = filtered
                .iter()
                .map(|&index| matches[index].take())
                .collect();
            return FilteredItems {
                filtered,
                match_indices,
            };
        }
        let mut runs: Vec<Vec<(usize, CompletionMatch)>> = Vec::new();
        let mut last = None;
        for (idx, completion) in kept {
            let section = self.items[idx].section();
            if last == Some(section) {
                runs.last_mut().unwrap().push((idx, completion));
            } else {
                last = Some(section);
                runs.push(vec![(idx, completion)]);
            }
        }
        let mut filtered = Vec::new();
        let mut match_indices = Vec::new();
        for mut run in runs {
            run.sort_by(|(left_idx, left), (right_idx, right)| {
                compare_completion_matches(
                    left,
                    right,
                    0,
                    0,
                    *left_idx,
                    *right_idx,
                    self.items[*left_idx].label(),
                    self.items[*right_idx].label(),
                )
            });
            filtered.extend(run.iter().map(|(idx, _)| *idx));
            match_indices.extend(run.into_iter().map(|(_, m)| Some(m.indices)));
        }
        FilteredItems {
            filtered,
            match_indices,
        }
    }

    fn clamp_selection(&mut self) {
        if self.filtered().is_empty() {
            self.selected = 0;
            self.scroll_offset = 0;
        } else {
            self.selected = self.selected.min(self.filtered().len() - 1);
            self.clamp_scroll();
        }
    }

    fn update_search_and_clamp(&mut self) {
        self.rebuild_filter();
        self.selected = if self.items.iter().any(|item| item.depth() > 0) {
            self.match_indices()
                .iter()
                .position(Option::is_some)
                .unwrap_or(0)
        } else {
            0
        };
        self.scroll_offset = 0;
        self.ensure_visible();
    }

    fn move_up(&mut self) {
        let len = self.filtered().len();
        if len == 0 {
            return;
        }
        self.selected = if self.selected == 0 {
            len - 1
        } else {
            self.selected - 1
        };
        self.ensure_visible();
    }

    fn page_up(&mut self) {
        let len = self.filtered().len();
        if len == 0 {
            return;
        }
        let step = self.viewport_height.max(1);
        self.selected = self.selected.saturating_sub(step);
        self.ensure_visible();
    }

    fn page_down(&mut self) {
        let len = self.filtered().len();
        if len == 0 {
            return;
        }
        let step = self.viewport_height.max(1);
        self.selected = (self.selected + step).min(len - 1);
        self.ensure_visible();
    }

    fn move_down(&mut self) {
        let len = self.filtered().len();
        if len == 0 {
            return;
        }
        self.selected = if self.selected == len - 1 {
            0
        } else {
            self.selected + 1
        };
        self.ensure_visible();
    }

    fn selection_visible(&self) -> bool {
        !self.filtered().is_empty()
            && self.selected >= self.scroll_offset
            && visual_rows_in_range(
                self.filtered(),
                &self.items,
                self.scroll_offset,
                self.selected + 1,
            ) <= self.viewport_height
    }

    fn clamp_scroll(&mut self) {
        self.scroll_offset = self.scroll_offset.min(find_scroll_offset_for_bottom(
            self.filtered(),
            &self.items,
            self.viewport_height,
        ));
    }

    fn ensure_visible(&mut self) {
        if self.filtered().is_empty() {
            return;
        }
        if self.selected < self.scroll_offset {
            self.scroll_offset = self.selected;
        }
        let visual = visual_rows_in_range(
            self.filtered(),
            &self.items,
            self.scroll_offset,
            self.selected + 1,
        );
        if visual > self.viewport_height {
            self.scroll_offset = find_scroll_offset_for(
                self.filtered(),
                &self.items,
                self.selected,
                self.viewport_height,
            );
        }
        self.clamp_scroll();
    }

    fn selected_item_index(&self) -> Option<usize> {
        self.filtered().get(self.selected).copied()
    }
}

impl<T: PickerItem> ListPicker<T> {
    pub fn new() -> Self {
        Self {
            state: None,
            picker: None,
            scrollbar_hit: None,
            row_hits: Vec::new(),
            title: String::new(),
            max_visible: None,
            footer: None,
            error_text: None,
            submit_keys: Vec::new(),
            delete_key: None,
            confirming_delete: None,
        }
    }

    pub fn with_max_visible(mut self, max: u16) -> Self {
        self.max_visible = Some(max);
        self
    }

    pub fn with_footer_line(mut self, line: Line<'static>) -> Self {
        self.footer = Some(line);
        self
    }

    pub fn with_submit_keys(mut self, keys: Vec<KeyEvent>) -> Self {
        self.submit_keys = keys;
        self
    }

    pub fn with_delete_key(mut self, key: KeyEvent) -> Self {
        self.delete_key = Some(key);
        self
    }

    /// True while a delete key press awaits its confirming second press.
    pub fn delete_confirming(&self) -> bool {
        self.confirming_delete.is_some()
    }

    pub fn delete_key(&self) -> Option<&KeyEvent> {
        self.delete_key.as_ref()
    }

    pub fn open_toggleable(&mut self, items: Vec<T>, enabled: Vec<bool>, title: impl Into<String>) {
        assert_eq!(
            items.len(),
            enabled.len(),
            "items and enabled must have same length"
        );
        self.title = title.into();
        let mut state = State::new(items);
        state.enabled = Some(enabled);
        self.state = Some(state);
    }

    pub fn open(&mut self, items: Vec<T>, title: impl Into<String>) {
        self.title = title.into();
        self.state = Some(State::new(items));
    }

    pub fn select(&mut self, index: usize) -> bool {
        let Some(s) = self.state.as_mut() else {
            return false;
        };
        if index >= s.filtered().len() {
            return false;
        }
        s.selected = index;
        s.ensure_visible();
        true
    }

    pub fn select_item_by(&mut self, predicate: impl Fn(&T) -> bool) -> bool {
        let Some(s) = self.state.as_mut() else {
            return false;
        };
        let Some(selected) = s
            .filtered()
            .iter()
            .position(|&item_idx| predicate(&s.items[item_idx]))
        else {
            return false;
        };
        s.selected = selected;
        s.ensure_visible();
        true
    }

    pub fn set_error_text(&mut self, text: Option<String>) {
        self.error_text = text;
    }

    pub fn replace_items(&mut self, items: Vec<T>) {
        if let Some(s) = self.state.as_mut() {
            s.replace_items(items);
        }
    }

    pub fn replace_items_preserving_order(
        &mut self,
        items: Vec<T>,
        same_item: impl Fn(&T, &T) -> bool,
    ) {
        let Some(state) = self.state.as_mut() else {
            return;
        };
        if items.iter().any(|item| item.depth() > 0) {
            state.replace_items(items);
            return;
        }
        let previous_visible = state.filtered().to_vec();
        let previous_items = mem::replace(&mut state.items, items);
        let query = state.search.value();
        let filtered = state.build_filter(&query);
        let mut matches: Vec<_> = filtered
            .filtered
            .into_iter()
            .zip(filtered.match_indices)
            .collect();
        matches.sort_by_key(|(index, _)| {
            previous_visible
                .iter()
                .position(|previous| same_item(&previous_items[*previous], &state.items[*index]))
                .unwrap_or(usize::MAX)
        });
        let (filtered, match_indices) = matches.into_iter().unzip();
        state.publication.commit_sync(
            query,
            FilteredItems {
                filtered,
                match_indices,
            },
        );
        state.clamp_selection();
    }

    pub fn replace_toggleable(&mut self, items: Vec<T>, enabled: Vec<bool>) {
        if let Some(s) = self.state.as_mut() {
            s.enabled = Some(enabled);
            s.replace_items(items);
        }
    }

    pub fn is_open(&self) -> bool {
        self.state.is_some()
    }

    pub fn cadence(&self) -> Cadence {
        let Some(s) = self.state.as_ref() else {
            return Cadence::IDLE;
        };
        let now = ClockSample::now();
        let any_spinning = s.items.iter().any(PickerItem::is_spinning);
        let any_aged = s
            .filtered()
            .iter()
            .any(|&idx| s.items[idx].age(&now).is_some());
        Cadence::any([
            Cadence::when(any_spinning, Cadence::SPINNER),
            Cadence::when(any_aged, Cadence::AGE),
        ])
    }

    pub fn close(&mut self) {
        self.state = None;
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> PickerAction<T> {
        if self.state.is_none() {
            return PickerAction::Close;
        }
        self.handle_ready_key(key)
    }

    fn handle_ready_key(&mut self, key: KeyEvent) -> PickerAction<T> {
        let s = self
            .state
            .as_mut()
            .expect("handle_ready_key called without state");

        if key::QUIT.matches(key) {
            self.state = None;
            return PickerAction::Close;
        }
        if self
            .delete_key
            .as_ref()
            .is_some_and(|k| key_matches(key, k))
        {
            if let Some(sel) = s.selected_item_index() {
                if self.confirming_delete == Some(sel) {
                    self.confirming_delete = None;
                    return PickerAction::Delete(sel);
                }
                self.confirming_delete = Some(sel);
            }
            return PickerAction::Consumed;
        }
        // Any other key clears a pending delete confirm.
        self.confirming_delete = None;
        if key::DELETE_WORD.matches(key) {
            s.search.remove_word_before_cursor();
            s.update_search_and_clamp();
            return PickerAction::Consumed;
        }
        if key::SCROLL_HALF_UP.matches(key) {
            s.page_up();
            return PickerAction::Consumed;
        }
        if key::SCROLL_HALF_DOWN.matches(key) {
            s.page_down();
            return PickerAction::Consumed;
        }
        if self.submit_keys.iter().any(|k| key_matches(key, k)) {
            let idx = s.selected_item_index();
            return match idx {
                Some(idx) => {
                    let mut state = self.state.take().unwrap();
                    PickerAction::Select(state.items.swap_remove(idx))
                }
                None => PickerAction::Consumed,
            };
        }
        if is_ctrl(&key) {
            return PickerAction::Consumed;
        }
        match key.code {
            KeyCode::Up => {
                s.move_up();
                PickerAction::Consumed
            }
            KeyCode::Down => {
                s.move_down();
                PickerAction::Consumed
            }
            KeyCode::PageUp => {
                s.page_up();
                PickerAction::Consumed
            }
            KeyCode::PageDown => {
                s.page_down();
                PickerAction::Consumed
            }
            KeyCode::Enter => {
                let idx = s.selected_item_index();
                if let (Some(enabled), Some(idx)) = (&mut s.enabled, idx) {
                    enabled[idx] = !enabled[idx];
                    return PickerAction::Toggle(idx, enabled[idx]);
                }
                if s.enabled.is_some() {
                    return PickerAction::Consumed;
                }
                match idx {
                    Some(idx) => {
                        let mut state = self.state.take().unwrap();
                        PickerAction::Select(state.items.swap_remove(idx))
                    }
                    None => PickerAction::Consumed,
                }
            }
            KeyCode::Esc => {
                self.state = None;
                PickerAction::Close
            }
            KeyCode::Char(c) => {
                s.search.push_char(c);
                s.update_search_and_clamp();
                PickerAction::Consumed
            }
            KeyCode::Backspace => {
                s.search.remove_char();
                s.update_search_and_clamp();
                PickerAction::Consumed
            }
            KeyCode::Left => {
                s.search.move_left();
                PickerAction::Consumed
            }
            KeyCode::Right => {
                s.search.move_right();
                PickerAction::Consumed
            }
            KeyCode::Home => {
                s.search.move_home();
                PickerAction::Consumed
            }
            KeyCode::End => {
                s.search.move_end();
                PickerAction::Consumed
            }
            _ => PickerAction::Consumed,
        }
    }

    pub fn selected_item(&self) -> Option<&T> {
        let s = self.state.as_ref()?;
        s.selected_item_index().map(|i| &s.items[i])
    }

    pub fn selected_index(&self) -> Option<usize> {
        self.state.as_ref().and_then(|s| s.selected_item_index())
    }

    pub fn item(&self, idx: usize) -> Option<&T> {
        self.state.as_ref().and_then(|s| s.items.get(idx))
    }

    pub fn items(&self) -> Option<&[T]> {
        self.state.as_ref().map(|s| s.items.as_slice())
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        let Some(s) = self.state.as_mut() else {
            return false;
        };
        // Paste is user input like any other key: a pending delete confirm
        // must not survive it.
        self.confirming_delete = None;
        s.search.insert_text(text);
        s.update_search_and_clamp();
        true
    }

    pub fn click_input(&mut self, pos: Position) -> bool {
        let Some(s) = self.state.as_mut() else {
            return false;
        };
        let clicked = click_search_buffer(&mut s.search, s.input_area, pos, SEARCH_PREFIX_WIDTH);
        if clicked {
            self.confirming_delete = None;
        }
        clicked
    }

    pub fn footer_area(&self) -> Option<Rect> {
        let s = self.state.as_ref()?;
        let y = s.input_area.bottom();
        (y < s.inner_area.bottom()).then(|| {
            Rect::new(
                s.input_area.x,
                y,
                s.input_area.width,
                s.inner_area.bottom() - y,
            )
        })
    }

    pub fn selection_visible(&self) -> bool {
        self.state
            .as_ref()
            .is_some_and(|s| s.inner_area.is_empty() || s.selection_visible())
    }

    pub fn scroll(&mut self, delta: i32) {
        let Some(s) = self.state.as_mut() else {
            return;
        };
        if delta > 0 {
            s.scroll_offset = s.scroll_offset.saturating_sub(delta as usize);
        } else {
            let total_visual = visual_rows_in_range(s.filtered(), &s.items, 0, s.filtered().len());
            let max_offset = if total_visual <= s.viewport_height {
                0
            } else {
                find_scroll_offset_for_bottom(s.filtered(), &s.items, s.viewport_height)
            };
            s.scroll_offset = (s.scroll_offset + delta.unsigned_abs() as usize).min(max_offset);
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        let footer = self.footer.clone().into_iter().collect();
        self.view_with_footer_lines(frame, area, footer)
    }

    #[cfg(test)]
    pub fn view_at(&mut self, frame: &mut Frame, area: Rect, now: &ClockSample) -> Rect {
        let footer = self.footer.clone().into_iter().collect();
        self.view_with_footer_lines_at(frame, area, footer, now)
    }

    pub fn view_with_footer_lines(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        footer: Vec<Line<'static>>,
    ) -> Rect {
        self.view_with_footer_lines_at(frame, area, footer, &ClockSample::now())
    }

    pub fn view_with_footer_lines_at(
        &mut self,
        frame: &mut Frame,
        area: Rect,
        footer: Vec<Line<'static>>,
        now: &ClockSample,
    ) -> Rect {
        let rendered = match self.state.as_mut() {
            None => (Rect::default(), None, Vec::new()),
            Some(s) => render_ready(
                frame,
                area,
                s,
                &self.title,
                self.max_visible,
                footer,
                self.error_text.as_deref(),
                now,
            ),
        };
        self.scrollbar_hit = rendered.1;
        self.row_hits = rendered.2;
        rendered.0
    }

    pub fn with_picker(mut self, picker: PickerId) -> Self {
        self.picker = Some(picker);
        self
    }

    pub fn hits(&self) -> Vec<HitTarget> {
        let Some(picker) = self.picker else {
            return Vec::new();
        };
        let Some(s) = self.state.as_ref() else {
            return Vec::new();
        };
        let mut hits: Vec<HitTarget> = self
            .row_hits
            .iter()
            .map(|&(rect, index)| HitTarget {
                rect,
                target: ClickTarget::PickerRow { picker, index },
            })
            .collect();
        if !s.input_area.is_empty() {
            hits.push(HitTarget {
                rect: s.input_area,
                target: ClickTarget::PickerInput(picker),
            });
        }
        if let Some(hit) = self.scrollbar_hit {
            hits.push(HitTarget {
                rect: hit.area,
                target: ClickTarget::Scrollbar(ScrollOwner::Picker(picker)),
            });
        }
        hits
    }

    pub fn content_len(&self) -> Option<u16> {
        let s = self.state.as_ref()?;
        Some(saturating_u16(visual_rows_in_range(
            s.filtered(),
            &s.items,
            0,
            s.filtered().len(),
        )))
    }

    pub fn scroll_line(&self) -> u16 {
        let Some(s) = self.state.as_ref() else {
            return 0;
        };
        saturating_u16(visual_rows_in_range(
            s.filtered(),
            &s.items,
            0,
            s.scroll_offset,
        ))
    }

    pub fn scroll_to_line(&mut self, line: u16) {
        let Some(s) = self.state.as_mut() else {
            return;
        };
        let total_visual = visual_rows_in_range(s.filtered(), &s.items, 0, s.filtered().len());
        let max_offset = if total_visual <= s.viewport_height {
            0
        } else {
            find_scroll_offset_for_bottom(s.filtered(), &s.items, s.viewport_height)
        };
        s.scroll_offset = if usize::from(line) >= total_visual.saturating_sub(s.viewport_height) {
            max_offset
        } else {
            item_at_visual_row(s.filtered(), &s.items, usize::from(line)).min(max_offset)
        };
    }
}

impl<T: PickerItem> Overlay for ListPicker<T> {
    fn is_open(&self) -> bool {
        self.is_open()
    }

    fn close(&mut self) {
        self.close()
    }

    fn cadence(&self) -> Cadence {
        self.cadence()
    }
}

fn key_matches(key: KeyEvent, other: &KeyEvent) -> bool {
    key.code == other.code && key.modifiers == other.modifiers
}

#[allow(clippy::too_many_arguments)]
fn render_ready<T: PickerItem>(
    frame: &mut Frame,
    area: Rect,
    s: &mut State<T>,
    title: &str,
    max_visible: Option<u16>,
    footer: Vec<Line<'static>>,
    error_text: Option<&str>,
    now: &ClockSample,
) -> (Rect, Option<ScrollbarHit>, Vec<(Rect, usize)>) {
    let footer_rows = u16::try_from(footer.len()).unwrap_or(u16::MAX);
    let content_rows = if s.filtered().is_empty() {
        1
    } else {
        let rows = saturating_u16(visual_rows_in_range(
            s.filtered(),
            &s.items,
            0,
            s.filtered().len(),
        ));
        match max_visible {
            Some(max) => rows.min(max),
            None => rows,
        }
    };
    let error_rows = error_text.is_some() as u16;
    let modal = Modal {
        title,
        width_percent: MIN_WIDTH_PERCENT,
        max_height_percent: MAX_HEIGHT_PERCENT,
    };
    let (popup, inner) = modal.render(
        frame,
        area,
        content_rows
            .saturating_add(SEARCH_ROW)
            .saturating_add(footer_rows)
            .saturating_add(error_rows)
            .min(area.height.saturating_sub(CHROME_LINES)),
    );
    let footer_rows = footer_rows.min(inner.height.saturating_sub(SEARCH_ROW + error_rows));
    let viewport_h = inner
        .height
        .saturating_sub(error_rows + SEARCH_ROW + footer_rows);
    let follow_selection = s.inner_area.is_empty() || s.selection_visible();
    let resized = s.viewport_height != viewport_h as usize;
    s.viewport_height = viewport_h as usize;
    if resized && follow_selection {
        s.ensure_visible();
    } else {
        s.clamp_scroll();
    }

    let mut constraints: Vec<Constraint> =
        Vec::with_capacity(3 + usize::from(footer_rows > 0) + error_text.is_some() as usize);
    if error_text.is_some() {
        constraints.push(Constraint::Length(1));
    }
    constraints.push(Constraint::Length(viewport_h));
    constraints.push(Constraint::Length(SEARCH_ROW));
    if footer_rows > 0 {
        constraints.push(Constraint::Length(footer_rows));
    }

    let areas = Layout::vertical(constraints).split(inner);
    let mut area_idx = 0;

    if let Some(err) = error_text {
        let line = Line::from(Span::styled(
            format!("  Error: {err}"),
            theme::current().error,
        ));
        frame.render_widget(Paragraph::new(vec![line]), areas[area_idx]);
        area_idx += 1;
    }

    let list_area = areas[area_idx];
    area_idx += 1;

    let search_area = areas[area_idx];
    area_idx += 1;

    let row_hits = render_list(
        frame,
        list_area,
        s.filtered(),
        &s.items,
        s.match_indices(),
        s.selected,
        s.scroll_offset,
        s.viewport_height,
        s.enabled.as_deref(),
        now,
    );
    s.input_area = search_area;
    render_search(frame, search_area, &s.search);

    if footer_rows > 0 {
        frame.render_widget(Paragraph::new(footer), areas[area_idx]);
    }

    let total_visual = visual_rows_in_range(s.filtered(), &s.items, 0, s.filtered().len());
    let scrollbar_hit = if total_visual > usize::from(viewport_h) {
        let visual_offset = visual_rows_in_range(s.filtered(), &s.items, 0, s.scroll_offset);
        render_vertical_scrollbar(
            frame,
            list_area,
            saturating_u16(total_visual),
            saturating_u16(visual_offset),
        )
    } else {
        None
    };

    s.inner_area = inner;
    (popup, scrollbar_hit, row_hits)
}

fn section_gap<T: PickerItem>(filtered: &[usize], items: &[T], idx: usize, start: usize) -> usize {
    let Some(sec) = items[filtered[idx]].section() else {
        return 0;
    };
    if idx == start {
        return 1;
    }
    let is_break = items[filtered[idx - 1]]
        .section()
        .is_none_or(|prev| prev != sec);
    if is_break { 2 } else { 0 }
}

fn visual_rows_in_range<T: PickerItem>(
    filtered: &[usize],
    items: &[T],
    start: usize,
    end: usize,
) -> usize {
    let item_count = end.saturating_sub(start);
    let section_rows: usize = (start..end)
        .map(|i| section_gap(filtered, items, i, start))
        .sum();
    item_count + section_rows
}

fn item_at_visual_row<T: PickerItem>(filtered: &[usize], items: &[T], visual: usize) -> usize {
    let mut top = 0;
    for idx in 0..filtered.len() {
        let item_row = top + section_gap(filtered, items, idx, 0);
        if visual <= item_row {
            return idx;
        }
        top = item_row + 1;
    }
    filtered.len().saturating_sub(1)
}

fn find_scroll_offset_for<T: PickerItem>(
    filtered: &[usize],
    items: &[T],
    target: usize,
    viewport_height: usize,
) -> usize {
    for start in (0..=target).rev() {
        let rows = visual_rows_in_range(filtered, items, start, target + 1);
        if rows > viewport_height {
            return (start + 1).min(target);
        }
    }
    0
}

fn find_scroll_offset_for_bottom<T: PickerItem>(
    filtered: &[usize],
    items: &[T],
    viewport_height: usize,
) -> usize {
    let len = filtered.len();
    if len == 0 {
        return 0;
    }
    find_scroll_offset_for(filtered, items, len - 1, viewport_height)
}

/// Keep the rightmost `max_width` display cells. Ages are ASCII, but the
/// helper stays display-width aware so it can be reused safely.
fn tail_by_width(text: &str, max_width: usize) -> String {
    let mut width = 0;
    let mut start = text.len();
    for (idx, ch) in text.char_indices().rev() {
        let cw = ch.width().unwrap_or(0);
        if width + cw > max_width {
            break;
        }
        width += cw;
        start = idx;
    }
    text[start..].to_string()
}

fn join_right(head: &[String], age: &str) -> String {
    if head.is_empty() {
        age.to_string()
    } else {
        format!("{} {age}", head.join(" "))
    }
}

fn elide_left(text: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if text.width() <= max_width {
        return text.to_string();
    }
    if max_width < AGE_ELLIPSIS_MIN_WIDTH {
        return tail_by_width(text, max_width);
    }
    format!("\u{2026}{}", tail_by_width(text, max_width - 1))
}

fn truncate_label(label: &str, max_width: usize) -> String {
    if max_width == 0 {
        return String::new();
    }
    if label.width() <= max_width {
        return label.to_string();
    }
    let target = max_width.saturating_sub(1);
    let mut width = 0;
    let mut result = String::with_capacity(label.len());
    for ch in label.chars() {
        let cw = ch.width().unwrap_or(0);
        if width + cw > target {
            break;
        }
        width += cw;
        result.push(ch);
    }
    result.push('\u{2026}');
    result
}

fn label_spans(
    label: &str,
    indices: Option<&[u32]>,
    base: Style,
    match_style: Style,
    max_width: usize,
    prefix_width: usize,
) -> (Vec<Span<'static>>, usize) {
    let prefix = " ".repeat(prefix_width);
    let truncated = truncate_label(label, max_width);
    let Some(indices) = indices.filter(|i| !i.is_empty()) else {
        return (
            vec![Span::styled(format!("{prefix}{truncated}"), base)],
            prefix_width + truncated.width(),
        );
    };
    let mut spans: Vec<Span<'static>> = vec![Span::styled(prefix, base)];
    let mut run = String::new();
    let mut in_match = false;
    for (pos, ch) in truncated.chars().enumerate() {
        let is_match = indices.binary_search(&(pos as u32)).is_ok();
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
    (spans, prefix_width + truncated.width())
}

#[allow(clippy::too_many_arguments)]
fn render_list<T: PickerItem>(
    frame: &mut Frame,
    area: Rect,
    filtered: &[usize],
    items: &[T],
    match_indices: &[Option<Vec<u32>>],
    selected: usize,
    scroll_offset: usize,
    viewport_height: usize,
    enabled: Option<&[bool]>,
    now: &ClockSample,
) -> Vec<(Rect, usize)> {
    if filtered.is_empty() {
        let line = Line::from(Span::styled(
            format!("  {NO_MATCHES}"),
            theme::current().item_desc,
        ));
        frame.render_widget(Paragraph::new(vec![line]), area);
        return Vec::new();
    }

    let mut lines: Vec<Line> = Vec::new();
    let mut row_hits: Vec<(Rect, usize)> = Vec::new();
    let mut i = scroll_offset;
    let mut last_section: Option<&str> = None;

    while lines.len() < viewport_height && i < filtered.len() {
        let item_idx = filtered[i];
        let item = &items[item_idx];

        if let Some(sec) = item.section()
            && last_section.is_none_or(|prev| prev != sec)
        {
            if !lines.is_empty() && lines.len() < viewport_height {
                lines.push(Line::raw(""));
            }
            if lines.len() < viewport_height {
                let mut header = vec![Span::styled(
                    format!("  {sec}"),
                    theme::current().keybind_section,
                )];
                if let Some(detail) = item.section_detail() {
                    header.push(Span::styled(
                        format!(" {detail}"),
                        theme::current().item_desc,
                    ));
                }
                lines.push(Line::from(header));
            }
            last_section = Some(sec);
        }

        if lines.len() >= viewport_height {
            break;
        }

        let highlighted = item.is_highlighted();
        let t = theme::current();
        let (style, detail_style) = match (i == selected, highlighted) {
            (true, true) => {
                let s = t.item_selected.fg(t.accent.fg.unwrap_or_default());
                (s, theme::dim_style(s, 0.4))
            }
            (true, false) => (t.item_selected, t.item_selected),
            (false, true) => (t.accent, theme::dim_style(t.accent, 0.4)),
            (false, false) => (t.item, t.item_desc),
        };
        let label_style = if item.is_finished() && i != selected {
            theme::dim_style(t.item, 0.5)
        } else {
            style
        };
        let checkbox = enabled.map(|en| {
            let sym = if en[item_idx] { "✓ " } else { "✗ " };
            let sty = if i == selected {
                style
            } else if en[item_idx] {
                theme::current().item
            } else {
                theme::current().item_desc
            };
            Span::styled(sym, sty)
        });
        let mut suffix = item.suffix();
        let status: Option<String> = if item.is_spinning() {
            Some(spinner_str(animation_elapsed_ms()).to_string())
        } else {
            item.detail().map(String::from)
        };
        let mut head_parts: Vec<String> = Vec::new();
        let status_parts = usize::from(status.is_some());
        if let Some(s) = status {
            head_parts.push(s);
        }
        if let Some(ctx) = item.context_str() {
            head_parts.push(ctx.to_string());
        }
        let suffix_gap = 2usize;
        let trailing_gap = suffix.map_or(0, |s| s.width() + suffix_gap);
        let checkbox_w = checkbox.as_ref().map_or(0, Span::width);
        let width = area.width as usize;
        let prefix_width = item
            .depth()
            .saturating_mul(DEPTH_INDENT_WIDTH)
            .saturating_add(LABEL_PREFIX_WIDTH)
            .min((area.width as usize).saturating_sub(checkbox_w));
        let right = if let Some(elapsed) = item.age(now) {
            let age = maki_lua::format_ago(elapsed);
            let full = join_right(&head_parts, &age);
            let available = |trailing: usize| {
                width.saturating_sub(trailing + 1 + DETAIL_RIGHT_PAD as usize + checkbox_w)
            };
            let fits = |right: &str, trailing: usize| {
                let room = available(trailing);
                let title_min = (prefix_width + AGE_TITLE_MIN_WIDTH).min(room);
                right.width() <= room.saturating_sub(title_min)
            };
            if fits(&full, trailing_gap) {
                full
            } else {
                suffix = None;
                if fits(&full, 0) {
                    full
                } else {
                    let room = available(0);
                    let title_min = (prefix_width + AGE_TITLE_MIN_WIDTH).min(room);
                    let budget = room.saturating_sub(title_min);
                    while head_parts.len() > status_parts
                        && join_right(&head_parts, &age).width() > budget
                    {
                        head_parts.pop();
                    }
                    if join_right(&head_parts, &age).width() <= budget {
                        join_right(&head_parts, &age)
                    } else {
                        let head = head_parts.join(" ");
                        let head_width = if head.is_empty() { 0 } else { head.width() + 1 };
                        let age_budget = budget.saturating_sub(head_width);
                        if age_budget == 0 {
                            head
                        } else {
                            join_right(&head_parts, &elide_left(&age, age_budget))
                        }
                    }
                }
            }
        } else {
            head_parts.join(" ")
        };
        let trailing_gap = suffix.map_or(0, |s| s.width() + suffix_gap);
        let label_indices = match_indices.get(i).and_then(|v| v.as_deref());
        let match_style = if i == selected {
            t.item_match_selected
        } else {
            t.item_match
        };
        let line = if !right.is_empty() {
            let right_budget = (area.width as usize)
                .saturating_sub(trailing_gap + 1 + DETAIL_RIGHT_PAD as usize + checkbox_w);
            let right = truncate_label(&right, right_budget);
            let right_w = right.width();
            let max_label = (area.width as usize).saturating_sub(
                right_w + trailing_gap + 1 + DETAIL_RIGHT_PAD as usize + checkbox_w,
            );
            let prefix_width = prefix_width.min(max_label);
            let (label_spans, label_w) = label_spans(
                item.label(),
                label_indices,
                label_style,
                match_style,
                max_label.saturating_sub(prefix_width),
                prefix_width,
            );
            let pad = (area.width as usize).saturating_sub(
                checkbox_w + label_w + trailing_gap + right_w + DETAIL_RIGHT_PAD as usize + 1,
            );
            let mut spans = Vec::with_capacity(8);
            if let Some(cb) = checkbox {
                spans.push(cb);
            }
            spans.extend(label_spans);
            if let Some(s) = suffix {
                spans.push(Span::styled(" ".repeat(suffix_gap), style));
                spans.push(Span::styled(s.to_string(), theme::dim_style(style, 0.4)));
            }
            spans.push(Span::styled(" ".repeat(pad), style));
            spans.push(Span::styled(right, detail_style));
            spans.push(Span::styled(" ".repeat(DETAIL_RIGHT_PAD as usize), style));
            Line::from(spans)
        } else {
            let (label_spans, _) = label_spans(
                item.label(),
                label_indices,
                label_style,
                match_style,
                usize::MAX,
                prefix_width,
            );
            let mut spans: Vec<Span> = Vec::with_capacity(5);
            if let Some(cb) = checkbox {
                spans.push(cb);
            }
            spans.extend(label_spans);
            if let Some(s) = suffix {
                spans.push(Span::styled(" ".repeat(suffix_gap), style));
                spans.push(Span::styled(s.to_string(), theme::dim_style(style, 0.4)));
            }
            Line::from(spans)
        };
        row_hits.push((
            Rect::new(area.x, area.y + lines.len() as u16, area.width, 1),
            i,
        ));
        lines.push(line);
        i += 1;
    }

    frame.render_widget(Paragraph::new(lines), area);
    row_hits
}

pub(super) fn click_search_buffer(
    search: &mut TextBuffer,
    area: Rect,
    pos: Position,
    prefix_width: u16,
) -> bool {
    if !area.contains(pos) {
        return false;
    }
    let column = pos.x.saturating_sub(area.x + prefix_width) as usize;
    let query = search.value();
    let mut width = 0;
    let mut offset = query.len();
    for (index, ch) in query.char_indices() {
        width += ch.width().unwrap_or(0);
        if width > column {
            offset = index;
            break;
        }
    }
    search.set_cursor_byte_offset(offset);
    true
}

fn render_search(frame: &mut Frame, area: Rect, search: &TextBuffer) {
    let query = search.value();
    let cursor_x = search.x();
    let chars: Vec<char> = query.chars().collect();
    let before: String = chars[..cursor_x].iter().collect();
    let cursor_char = chars.get(cursor_x).copied().unwrap_or(' ');
    let after_start = cursor_x.saturating_add(1).min(chars.len());
    let after: String = chars[after_start..].iter().collect();

    let line = Line::from(vec![
        super::chevron_span(),
        Span::styled(before, Style::default()),
        Span::styled(cursor_char.to_string(), theme::current().cursor),
        Span::styled(after, Style::default()),
    ]);
    frame.render_widget(Paragraph::new(vec![line]), area);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_clock() -> ClockSample {
        ClockSample {
            monotonic: Instant::now(),
            unix: Duration::ZERO,
        }
    }
    use crate::components::key;
    use crate::components::keybindings::key as kb;
    use crossterm::event::{KeyCode, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use test_case::test_case;

    #[test_case(2; "bottom")]
    #[test_case(u16::MAX; "past_bottom")]
    fn scroll_to_line_reaches_sectioned_bottom(line: u16) {
        let mut picker = ListPicker::new();
        picker.open(
            (0..3)
                .map(|i| SectionEntry {
                    label: i.to_string(),
                    section: "A",
                })
                .collect(),
            " Test ",
        );
        ready_state_mut(&mut picker).viewport_height = 2;
        picker.scroll_to_line(line);
        assert_eq!(ready_state(&picker).scroll_offset, 2);
    }

    #[test_case(u16::MAX as usize; "maximum")]
    #[test_case(u16::MAX as usize + 2; "over_maximum")]
    fn large_picker_saturates_rendered_ranges(count: usize) {
        let mut picker = ListPicker::new();
        picker.open(vec![String::new(); count], " Test ");
        ready_state_mut(&mut picker).scroll_offset = count - 1;
        assert_eq!(picker.scroll_line(), saturating_u16(count - 1));
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        assert_eq!(picker.content_len(), Some(u16::MAX));
        assert_eq!(picker.scrollbar_hit.unwrap().content_len, u16::MAX);
        assert_eq!(ready_state(&picker).items.len(), count);
    }

    #[test_case(false; "wheel")]
    #[test_case(true; "absolute")]
    fn manual_viewport_survives_render_and_resize(absolute: bool) {
        let mut picker = ListPicker::new().with_picker(PickerId::Model);
        picker.open((0..40).map(|i| format!("Item {i:02}")).collect(), " Test ");
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        let mut render = |picker: &mut ListPicker<String>, height| {
            terminal
                .draw(|frame| {
                    picker.view(frame, Rect::new(0, 0, 80, height));
                })
                .unwrap();
        };
        render(&mut picker, 24);
        if absolute {
            picker.scroll_to_line(20);
        } else {
            picker.scroll(-20);
        }
        render(&mut picker, 24);
        assert_eq!(picker.scroll_line(), 20);
        assert_eq!(picker.selected_index(), Some(0));
        assert_eq!(picker.row_hits.first().unwrap().1, 20);
        picker.replace_items((0..40).map(|i| format!("Item {i:02}")).collect());
        render(&mut picker, 24);
        assert_eq!(picker.scroll_line(), 20);
        render(&mut picker, 18);
        assert_eq!(picker.scroll_line(), 20);
        picker.handle_key(key(KeyCode::Down));
        render(&mut picker, 18);
        assert_eq!(picker.row_hits.first().unwrap().1, 1);
        picker.scroll_to_line(u16::MAX);
        render(&mut picker, 24);
        let state = ready_state(&picker);
        assert_eq!(state.scroll_offset, 40 - state.viewport_height);
    }

    #[test_case(0, 0; "prefix")]
    #[test_case(3, 1; "wide_character")]
    #[test_case(5, 2; "after_wide_character")]
    #[test_case(6, 4; "combining_character")]
    #[test_case(20, 5; "past_end")]
    fn rendered_search_click_moves_caret(column: u16, expected: usize) {
        let mut picker = ListPicker::new().with_picker(PickerId::Model);
        picker.open(vec!["a界e\u{301}z".to_string()], " Test ");
        picker.handle_paste("a界e\u{301}z");
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        terminal
            .draw(|frame| {
                picker.view(frame, frame.area());
            })
            .unwrap();
        let hit = picker
            .hits()
            .into_iter()
            .find(|hit| hit.target == ClickTarget::PickerInput(PickerId::Model))
            .unwrap();
        assert!(!picker.click_input(Position::new(hit.rect.x, hit.rect.y - 1)));
        assert!(picker.click_input(Position::new(hit.rect.x + column, hit.rect.y)));
        assert_eq!(ready_state(&picker).search.x(), expected);
        assert_eq!(ready_state(&picker).search.value(), "a界e\u{301}z");
        assert_eq!(picker.selected_index(), Some(0));
        picker.close();
        assert!(picker.hits().is_empty());
    }

    fn ready_state<T>(p: &ListPicker<T>) -> &State<T> {
        p.state.as_ref().expect("expected open state")
    }

    fn ready_state_mut<T>(p: &mut ListPicker<T>) -> &mut State<T> {
        p.state.as_mut().expect("expected open state")
    }

    struct Entry {
        label: String,
        detail: Option<String>,
        context: Option<String>,
        spinning: bool,
        age: Option<Duration>,
    }

    impl Entry {
        fn new(label: &str) -> Self {
            Self {
                label: label.into(),
                detail: None,
                context: None,
                spinning: false,
                age: None,
            }
        }
    }

    impl PickerItem for Entry {
        fn label(&self) -> &str {
            &self.label
        }
        fn detail(&self) -> Option<&str> {
            self.detail.as_deref()
        }
        fn context_str(&self) -> Option<&str> {
            self.context.as_deref()
        }
        fn is_spinning(&self) -> bool {
            self.spinning
        }
        fn age(&self, _now: &ClockSample) -> Option<Duration> {
            self.age
        }
    }

    /// The running-task spinner is drawn here and nowhere else, so this is the
    /// only place that can tell the loop to keep painting it.

    #[test]
    fn picker_age_cadence_tracks_filtered_rows() {
        let mut p = ListPicker::new();
        let mut aged = Entry::new("alpha");
        aged.age = Some(Duration::from_secs(9));
        p.open(vec![aged, Entry::new("beta")], " Test ");

        let cadence = p.cadence();
        assert_eq!(cadence.frame(), Some(crate::repaint::AGE_FRAME));
        assert!(cadence.moves(), "a live age owes a frame");

        p.handle_key(key(KeyCode::Char('z')));
        assert_eq!(
            p.cadence(),
            Cadence::IDLE,
            "a filtered-out age must not keep animating"
        );

        p.close();
        assert_eq!(p.cadence(), Cadence::IDLE, "a closed picker draws nothing");
    }

    #[test]
    fn picker_compound_age_layout() {
        let mut p = ListPicker::new();
        let mut aged = Entry::new("task");
        aged.context = Some("12k".into());
        aged.age = Some(Duration::from_secs(4 * 86400 + 7 * 3600 + 23 * 60 + 9));
        p.open(vec![aged], " Age ");
        let now = fixed_clock();
        let render = |p: &mut ListPicker<Entry>, width: u16| {
            let area = Rect::new(0, 0, width, 8);
            let mut terminal = Terminal::new(TestBackend::new(width, 8)).unwrap();
            terminal
                .draw(|frame| {
                    p.view_at(frame, area, &now);
                })
                .unwrap();
            buffer_lines(&terminal, area)
        };

        let wide = render(&mut p, 80);
        let wide_row = wide.iter().find(|line| line.contains("task")).unwrap();
        assert!(wide_row.contains("12k 4d 7h 23m 9s"));
        assert!(wide_row.chars().count() <= 80);

        let narrow = render(&mut p, 34);
        let narrow_row = narrow.iter().find(|line| line.contains("9s")).unwrap();
        assert!(
            !narrow_row.contains("12k"),
            "the snippet/context yields first"
        );
        assert!(narrow_row.contains("4d 7h 23m 9s"));

        let elided = render(&mut p, 28);
        let elided_row = elided.iter().find(|line| line.contains("9s")).unwrap();
        assert!(elided_row.contains('…'), "higher units elide first");
        assert!(!elided_row.contains("4d 7h 23m 9s"));
        assert!(elided_row.chars().count() <= 28);
    }

    #[test]
    fn narrow_aged_row_keeps_status_marker() {
        let mut p = ListPicker::new();
        let mut running = Entry::new("task");
        running.spinning = true;
        running.age = Some(Duration::from_secs(4 * 86400 + 7 * 3600 + 23 * 60 + 9));
        p.open(vec![running], " Age ");
        let now = fixed_clock();
        let area = Rect::new(0, 0, 24, 8);
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        terminal
            .draw(|frame| {
                p.view_at(frame, area, &now);
            })
            .unwrap();
        let lines = buffer_lines(&terminal, area);
        let row = lines.iter().find(|line| line.contains("9s")).unwrap();
        assert!(
            row.chars().any(|c| ('\u{2800}'..='\u{28ff}').contains(&c)),
            "the spinner survives age shortening: {row:?}"
        );
    }

    #[test]
    fn a_spinning_item_animates_the_picker() {
        let mut p = ListPicker::new();
        p.open(entries(&["idle task"]), " Test ");
        assert_eq!(p.cadence(), Cadence::IDLE);

        let mut running = Entry::new("running task");
        running.spinning = true;
        p.replace_items(vec![running]);
        assert_eq!(p.cadence(), Cadence::SPINNER);

        p.close();
        assert_eq!(p.cadence(), Cadence::IDLE, "a closed picker draws nothing");
    }

    fn entries(names: &[&str]) -> Vec<Entry> {
        names.iter().map(|n| Entry::new(n)).collect()
    }

    #[test]
    fn select_item_by_uses_visible_row_and_preserves_filter() {
        let mut p = ListPicker::new();
        p.open(entries(&["Alpha", "Beta", "Alpine"]), " Test ");
        p.handle_key(key(KeyCode::Char('a')));
        p.handle_key(key(KeyCode::Char('l')));

        assert!(p.select_item_by(|entry| entry.label == "Alpine"));
        assert_eq!(p.selected_item().unwrap().label, "Alpine");
        assert_eq!(ready_state(&p).search.value(), "al");
        assert!(!p.select_item_by(|entry| entry.label == "Beta"));
        assert_eq!(p.selected_item().unwrap().label, "Alpine");
    }

    #[test]
    fn navigation_wraps_around() {
        let mut p = ListPicker::new();
        p.open(entries(&["A", "B", "C"]), " Test ");

        p.handle_key(key(KeyCode::Up));
        assert_eq!(ready_state(&p).selected, 2);

        p.handle_key(key(KeyCode::Down));
        assert_eq!(ready_state(&p).selected, 0);
    }

    #[test]
    fn page_down_advances_and_clamps() {
        let items: Vec<Entry> = (0..50).map(|i| Entry::new(&format!("Item {i}"))).collect();
        let mut p = ListPicker::new();
        p.open(items, " Test ");
        ready_state_mut(&mut p).viewport_height = 10;

        p.handle_key(key(KeyCode::PageDown));
        assert_eq!(ready_state(&p).selected, 10);

        for _ in 0..10 {
            p.handle_key(key(KeyCode::PageDown));
        }
        assert_eq!(ready_state(&p).selected, 49);
    }

    #[test]
    fn page_up_retreats_and_clamps() {
        let items: Vec<Entry> = (0..50).map(|i| Entry::new(&format!("Item {i}"))).collect();
        let mut p = ListPicker::new();
        p.open(items, " Test ");
        let s = ready_state_mut(&mut p);
        s.viewport_height = 10;
        s.selected = 25;

        p.handle_key(key(KeyCode::PageUp));
        assert_eq!(ready_state(&p).selected, 15);

        for _ in 0..5 {
            p.handle_key(key(KeyCode::PageUp));
        }
        assert_eq!(ready_state(&p).selected, 0);
    }

    #[test]
    fn ctrl_d_and_ctrl_u_page_like_page_keys() {
        let items: Vec<Entry> = (0..50).map(|i| Entry::new(&format!("Item {i}"))).collect();
        let mut p = ListPicker::new();
        p.open(items, " Test ");
        ready_state_mut(&mut p).viewport_height = 10;

        p.handle_key(key::SCROLL_HALF_DOWN.to_key_event());
        assert_eq!(ready_state(&p).selected, 10);

        p.handle_key(key::SCROLL_HALF_UP.to_key_event());
        assert_eq!(ready_state(&p).selected, 0);
    }

    #[test]
    fn search_filters_progressively() {
        let mut p = ListPicker::new();
        p.open(entries(&["Alpha", "Beta"]), " Test ");
        assert_eq!(ready_state(&p).filtered(), vec![0, 1]);

        p.handle_key(key(KeyCode::Char('a')));
        assert_eq!(ready_state(&p).filtered(), vec![0, 1]);

        p.handle_key(key(KeyCode::Char('l')));
        assert_eq!(ready_state(&p).filtered(), vec![0]);
    }

    #[test]
    fn search_change_resets_selection_to_top_result() {
        let mut p = ListPicker::new();
        p.open(entries(&["Alpha", "Beta", "Gamma"]), " Test ");
        p.handle_key(key(KeyCode::Down));
        assert_eq!(ready_state(&p).selected, 1);

        p.handle_key(key(KeyCode::Char('a')));
        assert_eq!(ready_state(&p).selected, 0);
        assert_eq!(p.selected_item().unwrap().label, "Alpha");
    }

    #[test]
    fn fuzzy_search_uses_shared_matcher() {
        let mut p = ListPicker::new();
        p.open(
            entries(&["claude-sonnet", "claude-opus", "gemini-pro", "gpt-4"]),
            " Test ",
        );

        // Test fuzzy matching - should find "claude-sonnet" with "clu"
        p.handle_key(key(KeyCode::Char('c')));
        p.handle_key(key(KeyCode::Char('l')));
        p.handle_key(key(KeyCode::Char('u')));
        let filtered = ready_state(&p).filtered().to_vec();
        assert!(filtered.contains(&0)); // claude-sonnet should match
        assert!(filtered.contains(&1)); // claude-opus should match

        // Test that non-matching items are filtered out
        p.close();
        p.open(entries(&["claude-sonnet", "gemini-pro", "gpt-4"]), " Test ");
        p.handle_key(key(KeyCode::Char('c')));
        p.handle_key(key(KeyCode::Char('l')));
        p.handle_key(key(KeyCode::Char('u')));
        let filtered = ready_state(&p).filtered().to_vec();
        assert_eq!(filtered, vec![0]); // only claude-sonnet should match
    }

    fn set_query<T: PickerItem>(p: &mut ListPicker<T>, query: &str) {
        ready_state_mut(p).search.insert_text(query);
        ready_state_mut(p).update_search_and_clamp();
    }

    struct TreeEntry {
        label: &'static str,
        depth: usize,
        detail: Option<&'static str>,
    }

    impl PickerItem for TreeEntry {
        fn label(&self) -> &str {
            self.label
        }

        fn depth(&self) -> usize {
            self.depth
        }

        fn detail(&self) -> Option<&str> {
            self.detail
        }
    }

    fn tree_entries(items: &[(&'static str, usize)]) -> Vec<TreeEntry> {
        items
            .iter()
            .map(|&(label, depth)| TreeEntry {
                label,
                depth,
                detail: None,
            })
            .collect()
    }

    #[test_case("app", &[0, 1, 2, 3, 5, 6]; "ancestors_and_matches_in_source_order")]
    #[test_case("sibling", &[0, 4]; "sibling_drops_previous_branch")]
    #[test_case("unknown", &[7]; "depth_zero_resets_ancestry")]
    #[test_case("missing", &[]; "no_matches")]
    #[test_case("  ", &[0, 1, 2, 3, 4, 5, 6, 7]; "blank_query")]
    fn tree_filter_retains_ancestors_in_source_order(query: &str, expected: &[usize]) {
        let mut picker = ListPicker::new();
        picker.open(
            tree_entries(&[
                ("root", 0),
                ("branch", 1),
                ("zzapp", 3),
                ("app", 3),
                ("sibling", 1),
                ("other", 0),
                ("application", 1),
                ("unknown", 0),
            ]),
            " Test ",
        );
        set_query(&mut picker, query);
        assert_eq!(ready_state(&picker).filtered(), expected);
        if query == "app" {
            assert_eq!(
                ready_state(&picker).match_indices(),
                &[
                    None,
                    None,
                    Some(vec![2, 3, 4]),
                    Some(vec![0, 1, 2]),
                    None,
                    Some(vec![0, 1, 2]),
                ]
            );
        }
    }

    #[test_case(false; "immediate_enter_selects_match")]
    #[test_case(true; "ancestor_remains_navigable")]
    fn tree_search_selects_direct_match_before_ancestors(select_ancestor: bool) {
        let mut picker = ListPicker::new();
        picker.open(tree_entries(&[("Main", 0), ("leaf", 1)]), " Test ");
        set_query(&mut picker, "leaf");
        assert_eq!(picker.selected_item().unwrap().label, "leaf");
        if select_ancestor {
            picker.handle_key(key(KeyCode::Up));
        }
        let expected = if select_ancestor { "Main" } else { "leaf" };
        assert!(matches!(
            picker.handle_key(key(KeyCode::Enter)),
            PickerAction::Select(item) if item.label == expected
        ));
    }

    #[test_case(""; "unfiltered")]
    #[test_case("app"; "filtered")]
    fn tree_refresh_inserts_new_subtree_before_later_roots(query: &str) {
        let mut picker = ListPicker::new();
        picker.open(
            tree_entries(&[("root", 0), ("apple", 1), ("other", 0), ("application", 1)]),
            " Test ",
        );
        set_query(&mut picker, query);
        picker.replace_items_preserving_order(
            tree_entries(&[
                ("root", 0),
                ("apple", 1),
                ("app", 2),
                ("other", 0),
                ("application", 1),
            ]),
            |a, b| a.label == b.label,
        );
        assert_eq!(ready_state(&picker).filtered(), &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn filter_ranks_by_score_not_source_order() {
        let mut p = ListPicker::new();
        p.open(entries(&["axxapp", "apple"]), " Test ");
        set_query(&mut p, "app");
        assert_eq!(ready_state(&p).filtered(), vec![1, 0]);
    }

    #[test_case(false, &[1, 0]; "default_refresh_reranks")]
    #[test_case(true, &[0, 1]; "stable_refresh_appends")]
    fn refresh_ordering_is_opt_in(preserve_order: bool, expected: &[usize]) {
        let mut picker = ListPicker::new();
        picker.open(entries(&["apple"]), " Test ");
        set_query(&mut picker, "app");
        let replacement = entries(&["apple", "app"]);
        if preserve_order {
            picker.replace_items_preserving_order(replacement, |a, b| a.label == b.label);
        } else {
            picker.replace_items(replacement);
        }
        assert_eq!(ready_state(&picker).filtered(), expected);
    }

    #[test]
    fn stable_refresh_remaps_survivors_after_source_removal() {
        let mut picker = ListPicker::new();
        picker.open(entries(&["apple", "app", "application"]), " Test ");
        set_query(&mut picker, "app");
        picker.replace_items_preserving_order(
            entries(&["application", "apple", "appended"]),
            |a, b| a.label == b.label,
        );
        assert_eq!(ready_state(&picker).filtered(), &[1, 0, 2]);
        assert_eq!(picker.selected_item().unwrap().label, "apple");
    }

    #[test]
    fn filter_ties_keep_source_order() {
        let mut p = ListPicker::new();
        p.open(entries(&["bapp", "capp", "dapp"]), " Test ");
        set_query(&mut p, "app");
        assert_eq!(ready_state(&p).filtered(), vec![0, 1, 2]);
    }

    #[test]
    fn filter_duplicate_labels_all_kept_in_order() {
        let mut p = ListPicker::new();
        p.open(entries(&["app", "app", "bax"]), " Test ");
        set_query(&mut p, "app");
        assert_eq!(ready_state(&p).filtered(), vec![0, 1]);
    }

    #[test]
    fn filter_whitespace_query_returns_all_items_unindexed() {
        let mut p = ListPicker::new();
        p.open(entries(&["Alpha", "Beta", "Gamma"]), " Test ");
        set_query(&mut p, "  ");
        let s = ready_state(&p);
        assert_eq!(s.filtered(), vec![0, 1, 2]);
        assert_eq!(s.match_indices(), vec![None, None, None]);
    }

    #[test]
    fn filter_section_word_matches_section_only_keeps_item_unhighlighted() {
        let mut p = ListPicker::new();
        p.open(
            vec![
                SectionEntry {
                    label: "foo".into(),
                    section: "Anthropic",
                },
                SectionEntry {
                    label: "bar".into(),
                    section: "Gemini",
                },
            ],
            " Test ",
        );
        set_query(&mut p, "gemini");
        let s = ready_state(&p);
        assert_eq!(s.filtered(), vec![1]);
        assert_eq!(s.match_indices(), vec![Some(Vec::new())]);
    }

    #[test]
    fn filter_section_groups_stay_contiguous() {
        let mut p = ListPicker::new();
        p.open(
            vec![
                SectionEntry {
                    label: "zzapp".into(),
                    section: "A",
                },
                SectionEntry {
                    label: "app".into(),
                    section: "B",
                },
                SectionEntry {
                    label: "zapp".into(),
                    section: "B",
                },
            ],
            " Test ",
        );
        set_query(&mut p, "app");
        // The group-A item keeps its position even though its score is the
        // lowest; a global score sort would reorder to [1, 2, 0].
        assert_eq!(ready_state(&p).filtered(), vec![0, 1, 2]);
    }

    struct OptSection {
        label: String,
        section: Option<&'static str>,
    }

    impl PickerItem for OptSection {
        fn label(&self) -> &str {
            &self.label
        }
        fn section(&self) -> Option<&str> {
            self.section
        }
    }

    #[test]
    fn filter_grouping_is_run_based_not_first_appearance() {
        let mut p = ListPicker::new();
        p.open(
            vec![
                OptSection {
                    label: "app".into(),
                    section: Some("A"),
                },
                OptSection {
                    label: "app".into(),
                    section: None,
                },
                OptSection {
                    label: "app".into(),
                    section: Some("A"),
                },
            ],
            " Test ",
        );
        set_query(&mut p, "app");
        // First-appearance grouping would hoist the second "A" item into the
        // first group and reorder to [0, 2, 1]; run-based keeps the tail
        // un-sectioned run in place (the login "Custom provider..." layout).
        assert_eq!(ready_state(&p).filtered(), vec![0, 1, 2]);
    }

    #[test]
    fn filter_mixed_label_and_section_query_keeps_item_with_label_indices() {
        let mut p = ListPicker::new();
        p.open(
            vec![
                SectionEntry {
                    label: "claude-opus".into(),
                    section: "Anthropic",
                },
                SectionEntry {
                    label: "gpt".into(),
                    section: "OpenAI",
                },
            ],
            " Test ",
        );
        set_query(&mut p, "claude anthropic");
        // "claude" hits the label, "anthropic" only the section: the item is
        // kept with label-only indices.
        let s = ready_state(&p);
        assert_eq!(s.filtered(), vec![0]);
        assert_eq!(s.match_indices(), vec![Some(vec![0, 1, 2, 3, 4, 5])]);
    }

    #[test]
    fn filter_match_indices_are_codepoints_not_graphemes() {
        // \U0001F1FA\U0001F1FC (a flag) is one grapheme of two codepoints, so a
        // grapheme haystack (Utf32Str::new) would index "cd" at [3, 4]; the
        // codepoint haystack indexes it at [4, 5], matching maki.match.fuzzy.
        let mut p = ListPicker::new();
        p.open(vec![Entry::new("ab\u{1F1FA}\u{1F1FC}cd")], " Test ");
        set_query(&mut p, "cd");
        let s = ready_state(&p);
        assert_eq!(s.filtered(), vec![0]);
        assert_eq!(s.match_indices(), vec![Some(vec![4, 5])]);
    }

    #[test_case(0, false, true, "界application"; "flat_selected")]
    #[test_case(1, false, false, "界application"; "child_unselected")]
    #[test_case(3, false, true, "界application"; "nested_without_metadata")]
    #[test_case(1, true, true, "界application"; "child_with_metadata")]
    #[test_case(3, true, false, "界applicat…"; "nested_metadata_truncates_label")]
    fn tree_render_indents_without_shifting_match_indices(
        depth: usize,
        metadata: bool,
        selected: bool,
        expected_label: &str,
    ) {
        let items = [TreeEntry {
            label: "界application",
            depth,
            detail: metadata.then_some("12k"),
        }];
        let area = Rect::new(0, 0, 24, 1);
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        terminal
            .draw(|frame| {
                render_list(
                    frame,
                    area,
                    &[0],
                    &items,
                    &[Some(vec![1, 2, 3])],
                    if selected { 0 } else { usize::MAX },
                    0,
                    1,
                    None,
                    &fixed_clock(),
                );
            })
            .unwrap();
        let prefix_width = LABEL_PREFIX_WIDTH + depth * DEPTH_INDENT_WIDTH;
        let buffer = terminal.backend().buffer();
        for column in 0..prefix_width {
            assert_eq!(buffer[(column as u16, 0)].symbol(), " ");
        }
        assert_eq!(buffer[(prefix_width as u16, 0)].symbol(), "界");
        let t = theme::current();
        let match_style = if selected {
            t.item_match_selected
        } else {
            t.item_match
        };
        for (offset, ch) in ["a", "p", "p"].iter().enumerate() {
            let cell = &buffer[((prefix_width + 2 + offset) as u16, 0)];
            assert_eq!(cell.symbol(), *ch);
            assert_eq!(Some(cell.fg), match_style.fg);
        }
        let mut column = prefix_width;
        for ch in expected_label.chars() {
            assert_eq!(buffer[(column as u16, 0)].symbol(), ch.to_string());
            column += ch.width().unwrap_or(0);
        }
        if metadata {
            assert_eq!(buffer[(19, 0)].symbol(), "1");
            assert_eq!(buffer[(20, 0)].symbol(), "2");
            assert_eq!(buffer[(21, 0)].symbol(), "k");
        }
    }

    #[test_case(1, 1, " "; "one_column")]
    #[test_case(3, 2, "…  "; "indent_exceeds_width")]
    #[test_case(8, usize::MAX, "   12k  "; "depth_saturates")]
    fn tree_render_saturates_indentation_in_narrow_areas(width: u16, depth: usize, expected: &str) {
        let items = [TreeEntry {
            label: "apple",
            depth,
            detail: Some("12k"),
        }];
        let area = Rect::new(0, 0, width, 1);
        let mut terminal = Terminal::new(TestBackend::new(width, 1)).unwrap();
        terminal
            .draw(|frame| {
                render_list(
                    frame,
                    area,
                    &[0],
                    &items,
                    &[Some(vec![0])],
                    0,
                    0,
                    1,
                    None,
                    &fixed_clock(),
                );
            })
            .unwrap();
        assert_eq!(buffer_lines(&terminal, area), vec![expected]);
    }

    #[test]
    fn render_highlights_matched_chars_selected_and_unselected() {
        let mut p = ListPicker::new();
        p.open(entries(&["apple", "apricot"]), " Test ");
        p.handle_key(key(KeyCode::Char('a')));
        assert_eq!(ready_state(&p).filtered(), vec![0, 1]);

        let area = Rect::new(0, 0, 80, 40);
        let backend = TestBackend::new(area.width, area.height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                p.view(frame, area);
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let t = theme::current();

        let line_at = |y: u16| -> String {
            (0..area.width)
                .map(|x| {
                    buffer
                        .cell(Position::new(x, y))
                        .map(|c| c.symbol())
                        .unwrap_or(" ")
                })
                .collect()
        };
        let item_rows: Vec<u16> = (0..area.height)
            .filter(|y| {
                let line = line_at(*y);
                line.contains("apple") || line.contains("apricot")
            })
            .collect();
        assert_eq!(item_rows.len(), 2, "both items must be visible");
        // str::find returns byte offsets but buffer columns count
        // characters (cells); the modal's multi-byte border chars would
        // skew a byte offset, so count chars up to the label.
        let label_col = |y: u16| -> u16 {
            let line = line_at(y);
            line.char_indices()
                .position(|(b, _)| {
                    line[b..].starts_with("apple") || line[b..].starts_with("apricot")
                })
                .unwrap() as u16
        };
        // The first item row is the selected one (selection starts at 0).
        let (sel_row, unsel_row) = (item_rows[0], item_rows[1]);
        let sel_cell = buffer
            .cell(Position::new(label_col(sel_row), sel_row))
            .unwrap();
        assert_eq!(
            Some(sel_cell.fg),
            t.item_match_selected.fg,
            "selected row match char uses the stronger match style"
        );
        let unsel_cell = buffer
            .cell(Position::new(label_col(unsel_row), unsel_row))
            .unwrap();
        assert_eq!(
            Some(unsel_cell.fg),
            t.item_match.fg,
            "unselected row match char uses the plain match style"
        );
    }

    #[test]
    fn render_list_composes_right_cell_and_dims_finished_titles() {
        struct Task {
            label: String,
            finished: bool,
            ctx: Option<String>,
            age: Option<Duration>,
        }
        impl PickerItem for Task {
            fn label(&self) -> &str {
                &self.label
            }
            fn is_finished(&self) -> bool {
                self.finished
            }
            fn context_str(&self) -> Option<&str> {
                self.ctx.as_deref()
            }
            fn age(&self, _now: &ClockSample) -> Option<Duration> {
                self.age
            }
        }

        let items = vec![
            Task {
                label: "running".into(),
                finished: false,
                ctx: Some("12k".into()),
                age: Some(Duration::from_secs(120)),
            },
            Task {
                label: "done".into(),
                finished: true,
                ctx: Some("3k".into()),
                age: Some(Duration::from_secs(300)),
            },
        ];
        let area = Rect::new(0, 0, 80, 12);
        let t = theme::current();

        let mut p = ListPicker::new();
        p.open(items, " Tasks ");
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        terminal
            .draw(|f| {
                p.view(f, area);
            })
            .unwrap();
        let buf = terminal.backend().buffer();
        let lines = buffer_lines(&terminal, area);

        let running_row = lines.iter().position(|l| l.contains("running")).unwrap() as u16;
        let done_row = lines.iter().position(|l| l.contains("done")).unwrap() as u16;
        assert!(lines[running_row as usize].contains("12k"));
        assert!(lines[running_row as usize].contains("2m 0s"));
        assert!(lines[done_row as usize].contains("3k"));
        assert!(lines[done_row as usize].contains("5m 0s"));

        let done_line = &lines[done_row as usize];
        let done_col = done_line
            .char_indices()
            .position(|(b, _)| done_line[b..].starts_with("done"))
            .unwrap() as u16;
        let done_fg = buf.cell(Position::new(done_col, done_row)).unwrap().fg;
        assert_eq!(
            Some(done_fg),
            theme::dim_style(t.item, 0.5).fg,
            "unselected finished title is dimmed"
        );

        // Selecting the finished row restores full brightness.
        p.handle_key(key(KeyCode::Down));
        terminal
            .draw(|f| {
                p.view(f, area);
            })
            .unwrap();
        let buf = terminal.backend().buffer();
        let lines = buffer_lines(&terminal, area);
        let done_row = lines.iter().position(|l| l.contains("done")).unwrap() as u16;
        let done_line = &lines[done_row as usize];
        let done_col = done_line
            .char_indices()
            .position(|(b, _)| done_line[b..].starts_with("done"))
            .unwrap() as u16;
        let done_fg = buf.cell(Position::new(done_col, done_row)).unwrap().fg;
        assert_eq!(
            Some(done_fg),
            t.item_selected.fg,
            "selected finished title is not dimmed"
        );
    }

    #[test]
    fn enter_returns_selected_item() {
        let mut p = ListPicker::new();
        p.open(entries(&["A", "B", "C"]), " Test ");
        p.handle_key(key(KeyCode::Down));

        let action = p.handle_key(key(KeyCode::Enter));
        assert!(matches!(action, PickerAction::Select(ref e) if e.label == "B"));
        assert!(!p.is_open());
    }

    #[test_case(key(KeyCode::Esc) ; "esc_returns_close")]
    #[test_case(kb::QUIT.to_key_event() ; "ctrl_c_returns_close")]
    fn cancel_returns_close(cancel_key: KeyEvent) {
        let mut p = ListPicker::new();
        p.open(entries(&["A", "B"]), " Test ");

        let action = p.handle_key(cancel_key);
        assert!(matches!(action, PickerAction::Close));
        assert!(!p.is_open());
    }

    #[test]
    fn enter_on_empty_results_consumed() {
        let mut p = ListPicker::new();
        p.open(entries(&["Alpha"]), " Test ");
        p.handle_key(key(KeyCode::Char('z')));

        let action = p.handle_key(key(KeyCode::Enter));
        assert!(matches!(action, PickerAction::Consumed));
    }

    #[test_case(0, -3, 3  ; "scroll_down")]
    #[test_case(0, 100, 0  ; "clamp_at_top")]
    #[test_case(5, 3, 2    ; "scroll_up")]
    #[test_case(0, -100, 20 ; "clamp_at_bottom")]
    fn scroll_bounds(initial: usize, delta: i32, expected: usize) {
        let items: Vec<Entry> = (0..30).map(|i| Entry::new(&format!("Item {i}"))).collect();
        let mut p = ListPicker::new();
        p.open(items, " Test ");
        let s = ready_state_mut(&mut p);
        s.viewport_height = 10;
        s.scroll_offset = initial;

        p.scroll(delta);
        assert_eq!(ready_state(&p).scroll_offset, expected);
    }

    #[test]
    fn ctrl_w_deletes_word() {
        let mut p = ListPicker::new();
        p.open(entries(&["A", "B"]), " Test ");
        p.handle_key(key(KeyCode::Char('h')));
        p.handle_key(key(KeyCode::Char('i')));
        assert_eq!(ready_state(&p).search.value(), "hi");

        p.handle_key(kb::DELETE_WORD.to_key_event());
        assert_eq!(ready_state(&p).search.value(), "");
    }

    struct SectionEntry {
        label: String,
        section: &'static str,
    }

    impl PickerItem for SectionEntry {
        fn label(&self) -> &str {
            &self.label
        }
        fn section(&self) -> Option<&str> {
            Some(self.section)
        }
    }

    fn section_entries() -> Vec<SectionEntry> {
        vec![
            SectionEntry {
                label: "a1".into(),
                section: "A",
            },
            SectionEntry {
                label: "a2".into(),
                section: "A",
            },
            SectionEntry {
                label: "b1".into(),
                section: "B",
            },
        ]
    }

    #[test]
    fn section_headers_counted_in_visual_rows() {
        let items = section_entries();
        let filtered: Vec<usize> = (0..items.len()).collect();
        let rows = visual_rows_in_range(&filtered, &items, 0, items.len());
        assert_eq!(rows, 6);
    }

    #[test]
    fn section_navigation_accounts_for_headers() {
        let mut p = ListPicker::new();
        p.open(section_entries(), " Test ");
        let s = ready_state_mut(&mut p);
        s.viewport_height = 3;

        s.selected = 2;
        s.ensure_visible();
        assert_eq!(s.scroll_offset, 2);
    }

    #[test]
    fn ensure_visible_clamps_scroll_offset_after_filter() {
        let mut p = ListPicker::new();
        let items: Vec<Entry> = (0..20).map(|i| Entry::new(&format!("Item {i}"))).collect();
        p.open(items, " Test ");
        let s = ready_state_mut(&mut p);
        s.viewport_height = 10;
        s.scroll_offset = 10;
        s.selected = 15;

        s.search.insert_text("0");
        s.update_search_and_clamp();
        s.ensure_visible();
        assert_eq!(s.scroll_offset, 0);
    }

    #[test]
    fn toggle_mode_enter_flips_enabled() {
        let mut p = ListPicker::new();
        p.open_toggleable(entries(&["A", "B"]), vec![true, true], " Test ");
        let action = p.handle_key(key(KeyCode::Enter));
        assert!(matches!(action, PickerAction::Toggle(0, false)));
        assert!(p.is_open());
    }

    #[test]
    fn toggle_mode_search_targets_correct_item() {
        let mut p = ListPicker::new();
        p.open_toggleable(entries(&["Alpha", "Beta"]), vec![true, true], " Test ");
        p.handle_key(key(KeyCode::Char('b')));
        let action = p.handle_key(key(KeyCode::Enter));
        assert!(matches!(action, PickerAction::Toggle(1, false)));
    }

    #[test_case("short", 10 => "short" ; "no_truncation_needed")]
    #[test_case("abcdefghijklmno", 10 => "abcdefghi\u{2026}" ; "long_ascii_truncated")]
    #[test_case("ab\u{4e16}\u{754c}cde", 6 => "ab\u{4e16}\u{2026}" ; "wide_chars_truncated")]
    fn truncate_label_cases(label: &str, max_width: usize) -> String {
        truncate_label(label, max_width)
    }

    #[test]
    fn detail_right_edge_consistent_for_long_and_short_labels() {
        let width: u16 = 40;
        let detail = "2h ago";
        let suffix_gap = 2usize;

        let end_col = |label: &str, suffix_w: usize| -> usize {
            let trailing = suffix_w + if suffix_w > 0 { suffix_gap } else { 0 };
            let max_label = width
                .saturating_sub(detail.width() as u16 + trailing as u16 + 1 + DETAIL_RIGHT_PAD)
                as usize;
            let t = truncate_label(label, max_label);
            let pad = (width as usize).saturating_sub(
                t.width() + trailing + detail.width() + DETAIL_RIGHT_PAD as usize + 1,
            );
            t.width() + trailing + pad + detail.width() + DETAIL_RIGHT_PAD as usize
        };

        let long = "  ".to_string() + &"x".repeat(60);
        assert_eq!(end_col(&long, 0), end_col("  hi", 0));
        assert!(end_col(&long, 0) <= width as usize);

        let sfx = "Anthropic".width();
        assert_eq!(end_col(&long, sfx), end_col("  hi", sfx));
        assert!(end_col(&long, sfx) <= width as usize);
    }

    fn ctrl_key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::CONTROL)
    }

    fn buffer_lines(terminal: &Terminal<TestBackend>, area: Rect) -> Vec<String> {
        let buffer = terminal.backend().buffer();
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| {
                        buffer
                            .cell(Position::new(x, y))
                            .map(|c| c.symbol())
                            .unwrap_or(" ")
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn custom_submit_keys_select_like_enter() {
        let mut p = ListPicker::new().with_submit_keys(vec![ctrl_key(KeyCode::Char('s'))]);
        p.open(entries(&["A", "B", "C"]), " Test ");
        p.handle_key(key(KeyCode::Down));

        let action = p.handle_key(ctrl_key(KeyCode::Char('s')));
        assert!(matches!(action, PickerAction::Select(ref e) if e.label == "B"));
        assert!(!p.is_open());
    }

    #[test]
    fn submit_key_on_empty_results_consumed() {
        let mut p = ListPicker::new().with_submit_keys(vec![ctrl_key(KeyCode::Char('s'))]);
        p.open(entries(&["A"]), " Test ");
        p.handle_key(key(KeyCode::Char('z')));

        let action = p.handle_key(ctrl_key(KeyCode::Char('s')));
        assert!(matches!(action, PickerAction::Consumed));
        assert!(p.is_open());
    }

    fn with_delete_key() -> ListPicker<Entry> {
        ListPicker::new().with_delete_key(ctrl_key(KeyCode::Char('d')))
    }

    #[test]
    fn delete_key_arms_then_deletes_selected() {
        let mut p = with_delete_key();
        p.open(entries(&["A", "B", "C"]), " Test ");
        p.handle_key(key(KeyCode::Down));

        assert!(matches!(
            p.handle_key(ctrl_key(KeyCode::Char('d'))),
            PickerAction::Consumed
        ));
        assert!(p.delete_confirming());

        assert!(matches!(
            p.handle_key(ctrl_key(KeyCode::Char('d'))),
            PickerAction::Delete(1)
        ));
        assert!(!p.delete_confirming());
        assert!(p.is_open(), "the host decides what a delete closes");
    }

    #[test]
    fn other_key_clears_pending_delete() {
        let mut p = with_delete_key();
        p.open(entries(&["A", "B"]), " Test ");

        p.handle_key(ctrl_key(KeyCode::Char('d')));
        assert!(p.delete_confirming());

        p.handle_key(key(KeyCode::Down));
        assert!(!p.delete_confirming());

        // A fresh arm needs two presses and now targets the new selection.
        p.handle_key(ctrl_key(KeyCode::Char('d')));
        assert!(p.delete_confirming());
        assert!(matches!(
            p.handle_key(ctrl_key(KeyCode::Char('d'))),
            PickerAction::Delete(1)
        ));
    }

    #[test]
    fn delete_key_without_selection_is_consumed() {
        let mut p = with_delete_key();
        p.open(entries(&["A"]), " Test ");
        p.handle_key(key(KeyCode::Char('z')));

        assert!(matches!(
            p.handle_key(ctrl_key(KeyCode::Char('d'))),
            PickerAction::Consumed
        ));
        assert!(!p.delete_confirming());
    }

    #[test]
    fn delete_key_precedes_ctrl_d_page_down() {
        let items: Vec<Entry> = (0..50).map(|i| Entry::new(&format!("Item {i}"))).collect();
        let mut p = with_delete_key();
        p.open(items, " Test ");

        assert!(matches!(
            p.handle_key(ctrl_key(KeyCode::Char('d'))),
            PickerAction::Consumed
        ));
        assert!(p.delete_confirming());
        assert_eq!(
            ready_state(&p).selected,
            0,
            "no page-down while delete is armed"
        );
    }

    #[test]
    fn paste_clears_pending_delete() {
        let mut p = with_delete_key();
        p.open(entries(&["A", "B"]), " Test ");

        p.handle_key(ctrl_key(KeyCode::Char('d')));
        assert!(p.delete_confirming());

        p.handle_paste("A");
        assert!(!p.delete_confirming());
    }

    #[test]
    fn enter_under_active_filter_selects_original_index() {
        let mut p = ListPicker::new();
        p.open(entries(&["Alpha", "Beta", "Alpine"]), " Test ");
        p.handle_key(key(KeyCode::Char('a')));
        p.handle_key(key(KeyCode::Char('l')));
        assert_eq!(ready_state(&p).filtered(), vec![0, 2]);

        p.handle_key(key(KeyCode::Down));
        let action = p.handle_key(key(KeyCode::Enter));
        // "Alpine" is filtered row 1 but original index 2.
        assert!(matches!(action, PickerAction::Select(ref e) if e.label == "Alpine"));
        assert!(!p.is_open());
    }

    #[test]
    fn static_footer_line_renders_below_list_and_search() {
        let mut p = ListPicker::new().with_footer_line(Line::from("ctrl+d delete"));
        p.open(entries(&["alpha", "beta"]), " Test ");

        let area = Rect::new(0, 0, 80, 40);
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        terminal
            .draw(|frame| {
                p.view(frame, area);
            })
            .unwrap();
        let lines = buffer_lines(&terminal, area);

        let item_y = lines
            .iter()
            .position(|l| l.contains("alpha"))
            .expect("items must be drawn");
        let footer_y = lines
            .iter()
            .position(|l| l.contains("ctrl+d delete"))
            .expect("footer must be drawn");
        assert!(
            footer_y > item_y + 1,
            "footer sits below the list and search rows"
        );
    }

    #[test_case(80, 24; "normal")]
    #[test_case(40, 12; "narrow_short")]
    fn list_picker_multiline_footer_reserves_viewport(width: u16, height: u16) {
        const FIRST_FOOTER: &str = "option one";
        const SECOND_FOOTER: &str = "option two";
        const CONFIRM_FOOTER: &str = "Enter confirm Esc cancel";
        const LAST_ITEM: &str = "item 19";
        let mut picker = ListPicker::new();
        picker.open(
            (0..20).map(|index| format!("item {index:02}")).collect(),
            " Test ",
        );
        picker.select(19);
        let area = Rect::new(0, 0, width, height);
        let mut popup = Rect::default();
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                popup = picker.view_with_footer_lines(
                    frame,
                    area,
                    vec![
                        Line::from(FIRST_FOOTER),
                        Line::from(SECOND_FOOTER),
                        Line::from(CONFIRM_FOOTER),
                    ],
                );
            })
            .unwrap();
        let lines = buffer_lines(&terminal, area);
        let first_y = lines
            .iter()
            .position(|line| line.contains(FIRST_FOOTER))
            .unwrap();
        let second_y = lines
            .iter()
            .position(|line| line.contains(SECOND_FOOTER))
            .unwrap();
        let hints_y = lines
            .iter()
            .position(|line| line.contains("Enter"))
            .unwrap();
        let item_y = lines
            .iter()
            .position(|line| line.contains(LAST_ITEM))
            .unwrap();
        assert_eq!(second_y, first_y + 1);
        assert_eq!(hints_y, first_y + 2);
        assert!(first_y > item_y + 1);
        assert!(hints_y < popup.bottom() as usize - 1);
        let state = ready_state(&picker);
        assert_eq!(
            state.viewport_height,
            state.inner_area.height.saturating_sub(4) as usize
        );
        assert!(state.scroll_offset > 0);
        assert!(popup.bottom() <= area.bottom());
        assert!(popup.right() <= area.right());
    }

    #[test]
    fn multiline_footer_is_clipped_inside_small_popup() {
        const HINT: &str = "confirm";
        let mut picker = ListPicker::new();
        picker.open(vec!["item".to_owned()], " Test ");
        let area = Rect::new(0, 0, 40, 6);
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        let mut popup = Rect::default();
        terminal
            .draw(|frame| {
                popup = picker.view_with_footer_lines(frame, area, vec![Line::from(HINT); 20]);
            })
            .unwrap();
        let lines = buffer_lines(&terminal, area);
        for (row, line) in lines.iter().enumerate() {
            if line.contains(HINT) {
                assert!((popup.y as usize + 1..popup.bottom() as usize - 1).contains(&row));
            }
        }
        assert_eq!(ready_state(&picker).viewport_height, 0);
    }

    struct DetailSectionEntry {
        label: String,
        section: &'static str,
        detail: &'static str,
    }

    impl PickerItem for DetailSectionEntry {
        fn label(&self) -> &str {
            &self.label
        }
        fn section(&self) -> Option<&str> {
            Some(self.section)
        }
        fn section_detail(&self) -> Option<&str> {
            Some(self.detail)
        }
    }

    #[test]
    fn section_detail_renders_next_to_header() {
        let mut p = ListPicker::new();
        p.open(
            vec![DetailSectionEntry {
                label: "first".into(),
                section: "Anthropic",
                detail: "2 models",
            }],
            " Test ",
        );

        let area = Rect::new(0, 0, 80, 40);
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        terminal
            .draw(|frame| {
                p.view(frame, area);
            })
            .unwrap();
        let lines = buffer_lines(&terminal, area);
        assert!(lines.iter().any(|l| l.contains("Anthropic 2 models")));
    }

    struct Sectioned {
        label: String,
        section: String,
    }

    impl PickerItem for Sectioned {
        fn label(&self) -> &str {
            &self.label
        }
        fn section(&self) -> Option<&str> {
            Some(&self.section)
        }
    }

    #[test]
    fn item_at_visual_row_skips_section_headers() {
        let items = vec![
            Sectioned {
                label: "a".into(),
                section: "A".into(),
            },
            Sectioned {
                label: "b".into(),
                section: "A".into(),
            },
            Sectioned {
                label: "c".into(),
                section: "B".into(),
            },
        ];
        let filtered = [0, 1, 2];
        assert_eq!(item_at_visual_row(&filtered, &items, 0), 0);
        assert_eq!(item_at_visual_row(&filtered, &items, 1), 0);
        assert_eq!(item_at_visual_row(&filtered, &items, 2), 1);
        assert_eq!(item_at_visual_row(&filtered, &items, 4), 2);
        assert_eq!(item_at_visual_row(&filtered, &items, 99), 2);
    }

    #[test]
    fn scroll_to_line_converts_visual_rows_to_item_indices() {
        let mut p = ListPicker::new();
        p.open(entries(&["a", "b", "c", "d", "e", "f", "g", "h"]), " Test ");
        ready_state_mut(&mut p).viewport_height = 2;
        p.scroll_to_line(3);
        assert_eq!(ready_state(&p).scroll_offset, 3);
    }

    #[test]
    fn select_rejects_out_of_range_indices() {
        let mut p = ListPicker::new();
        p.open(entries(&["a", "b"]), " Test ");
        assert!(!p.select(2));
        assert!(p.select(1));
        assert_eq!(p.selected_index(), Some(1));
    }
}
