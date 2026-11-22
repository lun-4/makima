use crate::click::{ClickTarget, HitTarget};
use crate::components::form::{form_height, render_form, selected_prefix};
use crate::components::hint_line;
use crate::components::keybindings::key;
use crate::theme::{self, Theme};
use crate::wrap::Measure;

use std::borrow::Cow;
use std::ops::Range;
use std::path::{Path, PathBuf};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use makima_providers::{ModelOption, ModelSelection};
use ratatui::Frame;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget, Wrap};

const FORM_LABEL: &str = " Plan complete ";

const DISMISS_KEYS: &str = if cfg!(target_os = "macos") {
    "⌃T/Esc"
} else {
    "Ctrl+T/Esc"
};
const HINT_PAIRS: &[(&str, &str)] = &[
    ("↑↓", "select"),
    ("Space", "toggle parallel"),
    ("Enter", "confirm"),
    (key::OPEN_EDITOR.label, "edit plan"),
    (DISMISS_KEYS, "dismiss"),
];

struct MenuItem {
    label: &'static str,
    desc: &'static str,
    action: fn() -> PlanFormAction,
}

const MENU: &[MenuItem] = &[
    MenuItem {
        label: "Refine plan",
        desc: "  Dismiss and keep editing the plan",
        action: || PlanFormAction::Hide,
    },
    MenuItem {
        label: "Clear context and implement",
        desc: "  Clear this agent's context, then implement the plan",
        action: || PlanFormAction::ClearAndImplement,
    },
    MenuItem {
        label: "Implement plan",
        desc: "  Keep current context, implement the plan",
        action: || PlanFormAction::Implement,
    },
];

const MODEL_ROW: usize = MENU.len();
const USE_CURRENT_ROW: usize = MODEL_ROW + 1;
const MODEL_LABEL: &str = "Implementation model";
const MODEL_STAGED_TAG: &str = "  selected";
const MODEL_CURRENT_TAG: &str = "  current";
const USE_CURRENT_LABEL: &str = "Use current model";
const USE_CURRENT_DESC: &str = "  Clear implementation model and option choices";
const PARALLEL_TAG: &str = " (parallel)";

#[derive(Debug, PartialEq)]
pub enum PlanFormAction {
    Consumed,
    Passthrough,
    ClearAndImplement,
    Implement,
    OpenEditor,
    OpenModelPicker,
    UseCurrentModel,
    Hide,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Visibility {
    Shown,
    Hidden,
    UserDismissed,
}

pub struct PlanForm {
    visibility: Visibility,
    selected: usize,
    parallel: bool,
    implementation_selection: ModelSelection,
    plan_path: Option<PathBuf>,
}

impl PlanForm {
    pub fn new() -> Self {
        Self {
            visibility: Visibility::Hidden,
            selected: 0,
            parallel: false,
            implementation_selection: ModelSelection::default(),
            plan_path: None,
        }
    }

    pub fn is_visible(&self) -> bool {
        self.visibility == Visibility::Shown
    }

    pub fn on_plan_ready(&mut self) {
        if self.visibility != Visibility::UserDismissed {
            self.visibility = Visibility::Shown;
            self.selected = 0;
        }
    }

    pub fn on_plan_drafting(&mut self) {
        self.visibility = Visibility::Hidden;
    }

    pub fn toggle(&mut self) {
        self.visibility = if self.is_visible() {
            Visibility::UserDismissed
        } else {
            self.selected = 0;
            Visibility::Shown
        };
    }

    pub fn hide(&mut self) {
        if self.is_visible() {
            self.visibility = Visibility::UserDismissed;
        }
    }

    pub fn parallel(&self) -> bool {
        self.parallel
    }

    pub fn reset(&mut self) {
        self.visibility = Visibility::Hidden;
        self.selected = 0;
        self.implementation_selection = ModelSelection::default();
        self.plan_path = None;
    }

    pub(crate) fn sync_plan_path(&mut self, path: Option<&Path>) -> bool {
        let changed = self
            .plan_path
            .as_deref()
            .is_some_and(|previous| Some(previous) != path);
        if changed {
            self.reset();
        }
        self.plan_path = path.map(Path::to_path_buf);
        changed
    }

    pub fn implementation_selection(&self) -> &ModelSelection {
        &self.implementation_selection
    }

    pub fn set_implementation_selection(&mut self, selection: ModelSelection) {
        self.implementation_selection = selection;
        self.selected = self
            .selected
            .min(Self::menu_len(!self.implementation_selection.is_empty()) - 1);
    }

    pub fn use_current_model(&mut self) {
        self.implementation_selection = ModelSelection::default();
        self.selected = MODEL_ROW;
    }

    pub fn clear_model_if_current(&mut self, current_model: &str) {
        if self.implementation_selection.spec.as_deref() == Some(current_model) {
            self.implementation_selection.spec = None;
            self.selected = self
                .selected
                .min(Self::menu_len(!self.implementation_selection.is_empty()) - 1);
        }
    }

    fn menu_len(staged: bool) -> usize {
        MODEL_ROW + 1 + usize::from(staged)
    }

    pub fn hint_line(&self) -> Option<Line<'static>> {
        if self.visibility != Visibility::UserDismissed {
            return None;
        }
        let t = theme::current();
        Some(Line::from(vec![
            Span::styled(" Plan ", Style::new().fg(t.foreground)),
            Span::styled(key::PLAN_TOGGLE.label, t.keybind_key),
            Span::raw(" "),
        ]))
    }

    pub fn height(&self, width: u16, current_model: &str) -> u16 {
        if self.is_visible() {
            form_height(self.lines(current_model), width)
        } else {
            0
        }
    }

    pub fn handle_key(&mut self, key_event: KeyEvent) -> PlanFormAction {
        if key::MODEL_PICKER.matches(key_event) {
            return PlanFormAction::OpenModelPicker;
        }
        if key::QUIT.matches(key_event)
            || key_event.code == KeyCode::Esc
            || key::PLAN_TOGGLE.matches(key_event)
        {
            return PlanFormAction::Hide;
        }
        if key::OPEN_EDITOR.matches(key_event) {
            return PlanFormAction::OpenEditor;
        }
        match key_event.code {
            KeyCode::Up => {
                self.selected = self.selected.saturating_sub(1);
                PlanFormAction::Consumed
            }
            KeyCode::Down => {
                self.selected = (self.selected + 1)
                    .min(Self::menu_len(!self.implementation_selection.is_empty()) - 1);
                PlanFormAction::Consumed
            }
            KeyCode::Char(' ') => {
                self.parallel = !self.parallel;
                PlanFormAction::Consumed
            }
            KeyCode::Enter => match self.selected {
                MODEL_ROW => PlanFormAction::OpenModelPicker,
                USE_CURRENT_ROW => PlanFormAction::UseCurrentModel,
                row => (MENU[row].action)(),
            },
            KeyCode::Tab => PlanFormAction::Passthrough,
            _ => PlanFormAction::Consumed,
        }
    }

    pub fn view(&self, frame: &mut Frame, area: Rect, current_model: &str) -> Vec<HitTarget> {
        if !self.is_visible() {
            return Vec::new();
        }
        let lines = self.lines(current_model);
        let width = area.width.saturating_sub(2);
        let mut measure = Measure::new(width);
        let mut row = 0u16;
        let inner = Rect::new(area.x + 1, area.y + 1, width, area.height.saturating_sub(2));
        let mut hits = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            let rows = measure.rows(line).max(1);
            let line_area =
                Rect::new(inner.x, inner.y.saturating_add(row), width, rows).intersection(inner);
            let menu_row = match index {
                0..=MODEL_ROW => Some(index),
                _ if !self.implementation_selection.is_empty() && index == lines.len() - 3 => {
                    Some(USE_CURRENT_ROW)
                }
                _ => None,
            };
            if let Some(menu_row) = menu_row {
                hits.push(HitTarget {
                    rect: line_area,
                    target: ClickTarget::PlanRow(menu_row as u16),
                });
                if index < MENU.len() && self.parallel {
                    let end = line
                        .spans
                        .iter()
                        .map(|span| span.content.len())
                        .sum::<usize>();
                    hits.extend(wrapped_key_hits(
                        line,
                        &[(
                            end - PARALLEL_TAG.len() + 1..end,
                            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                        )],
                        line_area,
                    ));
                }
            } else if index == lines.len() - 1 {
                let events = [
                    KeyEvent::new(KeyCode::Down, KeyModifiers::NONE),
                    KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                    KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
                    KeyEvent::new(key::OPEN_EDITOR.code, key::OPEN_EDITOR.modifiers),
                    KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
                ];
                let mut offset = 0;
                let ranges: Vec<_> = HINT_PAIRS
                    .iter()
                    .zip(events)
                    .map(|(&(key, desc), event)| {
                        let start = offset + 2;
                        offset = start + key.len() + 1 + desc.len();
                        (start..offset, event)
                    })
                    .collect();
                hits.extend(wrapped_key_hits(line, &ranges, line_area));
            }
            row = row.saturating_add(rows);
        }
        let t = theme::current();
        render_form(&t, FORM_LABEL, frame, area, lines, (0, 0));
        hits
    }

    pub fn select_row(&mut self, row: usize) -> bool {
        if row >= Self::menu_len(!self.implementation_selection.is_empty()) {
            return false;
        }
        self.selected = row;
        true
    }

    fn lines(&self, current_model: &str) -> Vec<Line<'static>> {
        let t = theme::current();
        let staged = &self.implementation_selection;
        let menu_len = Self::menu_len(!staged.is_empty());
        let mut lines: Vec<Line<'static>> = Vec::with_capacity(menu_len + 3);

        for (row, item) in MENU.iter().enumerate() {
            let mut spans = self.row_spans(&t, row, item.label, item.desc);
            if self.parallel {
                spans.push(Span::styled(PARALLEL_TAG, t.tool_dim.bold()));
            }
            lines.push(Line::from(spans));
        }
        let (spec, tag) = match staged.spec.as_deref() {
            Some(spec) => (spec, MODEL_STAGED_TAG),
            None => (current_model, MODEL_CURRENT_TAG),
        };
        lines.push(Line::from(self.row_spans(
            &t,
            MODEL_ROW,
            format!("{MODEL_LABEL}: {spec}"),
            tag,
        )));
        let overrides: Vec<String> = ModelOption::ALL
            .into_iter()
            .filter_map(|option| Some(format!("{}={}", option.key(), option.get(staged)?)))
            .collect();
        if !overrides.is_empty() {
            lines.push(Line::from(vec![
                Span::styled(format!("    {}", overrides.join(" ")), t.tool_dim),
                Span::styled(MODEL_STAGED_TAG, t.tool_dim),
            ]));
        }
        if !staged.is_empty() {
            lines.push(Line::from(self.row_spans(
                &t,
                USE_CURRENT_ROW,
                USE_CURRENT_LABEL,
                USE_CURRENT_DESC,
            )));
        }
        lines.push(Line::default());
        lines.push(hint_line(HINT_PAIRS));
        lines
    }

    fn row_spans(
        &self,
        t: &Theme,
        row: usize,
        label: impl Into<Cow<'static, str>>,
        desc: &'static str,
    ) -> Vec<Span<'static>> {
        let (prefix, style) = selected_prefix(t, row == self.selected);
        vec![
            Span::styled(prefix, t.tool_dim),
            Span::styled(label, style),
            Span::styled(desc, t.tool_dim),
        ]
    }
}

pub(super) fn wrapped_key_hits(
    line: &Line<'_>,
    ranges: &[(Range<usize>, KeyEvent)],
    area: Rect,
) -> Vec<HitTarget> {
    if area.is_empty() {
        return Vec::new();
    }
    let text: String = line
        .spans
        .iter()
        .map(|span| span.content.as_ref())
        .collect();
    let mut spans = Vec::new();
    let mut offset = 0;
    for (index, (range, _)) in ranges.iter().enumerate() {
        spans.push(Span::raw(&text[offset..range.start]));
        spans.push(Span::styled(
            &text[range.clone()],
            Style::new().bg(Color::Indexed(index as u8)),
        ));
        offset = range.end;
    }
    spans.push(Span::raw(&text[offset..]));
    let mut buffer = Buffer::empty(area);
    Paragraph::new(Line::from(spans))
        .wrap(Wrap { trim: false })
        .render(area, &mut buffer);
    let mut hits = Vec::new();
    for y in area.top()..area.bottom() {
        let mut x = area.left();
        while x < area.right() {
            let Color::Indexed(index) = buffer[(x, y)].bg else {
                x += 1;
                continue;
            };
            let start = x;
            while x < area.right() && buffer[(x, y)].bg == Color::Indexed(index) {
                x += 1;
            }
            hits.push(HitTarget {
                rect: Rect::new(start, y, x - start, 1),
                target: ClickTarget::KeyHint(ranges[index as usize].1),
            });
        }
    }
    hits
}

#[cfg(test)]
mod tests {
    use super::{
        MODEL_ROW, ModelSelection, PlanForm, PlanFormAction, USE_CURRENT_LABEL, USE_CURRENT_ROW,
    };
    use crate::click::ClickTarget;
    use crate::components::keybindings::key;
    use crate::components::{buffer_text, key};
    use crate::wrap::Measure;
    use crossterm::event::{KeyCode, KeyEvent};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;
    use std::path::Path;
    use test_case::test_case;

    const CURRENT_MODEL: &str = "provider/current";
    const OTHER_MODEL: &str = "provider/other";
    const LONG_MODEL: &str = "openrouter/vendor/a-very-long-implementation-model-name-that-wraps";
    const WIDE: u16 = 200;
    const HINT_TAIL: &str = "dismiss";
    const PLAN_PATH: &str = "plan.md";
    const REPLACEMENT_PLAN_PATH: &str = "replacement-plan.md";

    fn selection(spec: Option<&str>, options: &[(&str, &str)]) -> ModelSelection {
        let mut selection = ModelSelection {
            spec: spec.map(Into::into),
            ..ModelSelection::default()
        };
        for (key, value) in options {
            selection.set_option(key, value).unwrap();
        }
        selection
    }

    #[test_case(30; "narrow")]
    #[test_case(80; "normal")]
    #[test_case(WIDE; "wide")]
    fn visible_actions_have_clipped_hits(width: u16) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.parallel = true;
        let height = form.height(width, CURRENT_MODEL);
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let mut hits = Vec::new();
        terminal
            .draw(|frame| {
                hits = form.view(frame, frame.area(), CURRENT_MODEL);
            })
            .unwrap();
        for target in [
            ClickTarget::KeyHint(key(KeyCode::Char(' '))),
            ClickTarget::KeyHint(key(KeyCode::Enter)),
            ClickTarget::KeyHint(crate::components::keybindings::key::OPEN_EDITOR.to_key_event()),
            ClickTarget::KeyHint(key(KeyCode::Esc)),
        ] {
            assert!(hits.iter().any(|hit| hit.target == target));
        }
        for hit in &hits {
            assert_eq!(
                hit.rect
                    .intersection(Rect::new(1, 1, width - 2, height - 2)),
                hit.rect
            );
        }
        terminal
            .draw(|frame| {
                hits = form.view(frame, Rect::new(0, 0, width, 3), CURRENT_MODEL);
            })
            .unwrap();
        assert!(
            hits.iter()
                .filter(|hit| !hit.rect.is_empty())
                .all(|hit| hit.rect.bottom() <= 2)
        );
    }

    #[test_case(30, None, true; "narrow_options_only")]
    #[test_case(80, None, true; "normal_options_only")]
    #[test_case(WIDE, None, true; "wide_options_only")]
    #[test_case(30, Some(LONG_MODEL), true; "narrow_model_and_options")]
    #[test_case(80, Some(LONG_MODEL), true; "normal_model_and_options")]
    #[test_case(WIDE, Some(LONG_MODEL), true; "wide_model_and_options")]
    #[test_case(30, Some(LONG_MODEL), false; "narrow_model_only")]
    #[test_case(80, Some(LONG_MODEL), false; "normal_model_only")]
    #[test_case(WIDE, Some(LONG_MODEL), false; "wide_model_only")]
    fn staged_rows_keep_logical_mouse_targets(width: u16, spec: Option<&str>, has_options: bool) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        let options = if has_options {
            &[("thinking", "off"), ("fast", "false")][..]
        } else {
            &[]
        };
        form.set_implementation_selection(selection(spec, options));
        let height = form.height(width, CURRENT_MODEL);
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        let mut hits = Vec::new();
        terminal
            .draw(|frame| {
                hits = form.view(frame, frame.area(), CURRENT_MODEL);
            })
            .unwrap();
        let row_targets: Vec<_> = hits
            .iter()
            .filter_map(|hit| match hit.target {
                ClickTarget::PlanRow(row) => Some(row),
                _ => None,
            })
            .collect();
        assert_eq!(
            row_targets,
            (0..=USE_CURRENT_ROW as u16).collect::<Vec<_>>()
        );

        let lines = form.lines(CURRENT_MODEL);
        let inner_width = width - 2;
        let mut measure = Measure::new(inner_width);
        let mut y = 1;
        for line in lines.iter().take(MODEL_ROW + 1) {
            y += measure.rows(line).max(1);
        }
        if has_options {
            let summary_height = measure.rows(&lines[MODEL_ROW + 1]).max(1);
            let summary_area = Rect::new(1, y, inner_width, summary_height);
            assert!(hits.iter().all(|hit| !hit.rect.intersects(summary_area)));
            y += summary_height;
        }
        assert!(hits.iter().any(|hit| {
            hit.target == ClickTarget::PlanRow(USE_CURRENT_ROW as u16) && hit.rect.y == y
        }));
        assert!(form.select_row(USE_CURRENT_ROW));
        assert_eq!(
            form.handle_key(key(KeyCode::Enter)),
            PlanFormAction::UseCurrentModel
        );
        assert!(!form.select_row(USE_CURRENT_ROW + 1));
        form.use_current_model();
        assert!(!form.select_row(USE_CURRENT_ROW));
    }

    fn render(form: &PlanForm, width: u16) -> String {
        let height = form.height(width, CURRENT_MODEL);
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                form.view(frame, frame.area(), CURRENT_MODEL);
            })
            .unwrap();
        buffer_text(terminal.backend().buffer())
    }

    #[test]
    fn on_plan_ready_shows_and_resets_selected() {
        let mut form = PlanForm::new();
        form.selected = 1;
        form.on_plan_ready();
        assert!(form.is_visible());
        assert_eq!(form.selected, 0);
    }

    #[test]
    fn on_plan_ready_respects_user_dismissed() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.hide();
        form.on_plan_ready();
        assert!(!form.is_visible());
    }

    #[test]
    fn on_plan_drafting_clears_user_dismissed() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.hide();
        form.on_plan_drafting();
        form.on_plan_ready();
        assert!(
            form.is_visible(),
            "drafting should clear dismiss so next ready shows"
        );
    }

    #[test]
    fn toggle_cycles_visibility() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        assert!(form.is_visible());
        form.toggle();
        assert!(!form.is_visible());
        form.toggle();
        assert!(form.is_visible());
    }

    #[test_case(Some(PLAN_PATH), false ; "unchanged")]
    #[test_case(Some(REPLACEMENT_PLAN_PATH), true ; "replacement")]
    #[test_case(None, true ; "retired")]
    fn plan_path_sync_discards_only_retired_stage(next: Option<&str>, retired: bool) {
        let mut form = PlanForm::new();
        assert!(!form.sync_plan_path(Some(Path::new(PLAN_PATH))));
        form.on_plan_ready();
        let staged = selection(Some(OTHER_MODEL), &[("thinking", "off"), ("fast", "false")]);
        form.set_implementation_selection(staged.clone());
        assert_eq!(
            form.handle_key(key(KeyCode::Char(' '))),
            PlanFormAction::Consumed
        );
        form.selected = USE_CURRENT_ROW;
        assert_eq!(form.sync_plan_path(next.map(Path::new)), retired);
        assert!(form.parallel());
        if retired {
            assert!(!form.is_visible());
            assert!(form.implementation_selection().is_empty());
            assert_eq!(form.selected, 0);
            form.on_plan_ready();
            assert!(form.is_visible());
            assert!(form.implementation_selection().is_empty());
        } else {
            assert!(form.is_visible());
            assert_eq!(form.implementation_selection(), &staged);
            assert_eq!(form.selected, USE_CURRENT_ROW);
        }
        assert!(!form.sync_plan_path(next.map(Path::new)));
    }

    #[test]
    fn reset_clears_state() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.selected = 1;
        form.reset();
        assert!(!form.is_visible());
        assert_eq!(form.selected, 0);
    }

    #[test]
    fn hint_line_only_when_dismissed() {
        let mut form = PlanForm::new();
        assert!(form.hint_line().is_none());
        form.on_plan_ready();
        assert!(form.hint_line().is_none());
        form.hide();
        assert!(form.hint_line().is_some());
    }

    #[test]
    fn height_reflects_visibility() {
        let mut form = PlanForm::new();
        assert_eq!(form.height(WIDE, CURRENT_MODEL), 0);
        form.on_plan_ready();
        assert!(form.height(WIDE, CURRENT_MODEL) > 0);
        form.hide();
        assert_eq!(form.height(WIDE, CURRENT_MODEL), 0);
    }

    #[test_case(80, true, None, MODEL_ROW - 1, "Implement plan" ; "parallel_at_80_cols")]
    #[test_case(50, false, Some(LONG_MODEL), USE_CURRENT_ROW, "Use current model" ; "long_model_at_50_cols")]
    fn wrapped_form_keeps_hint_and_cursor_visible(
        width: u16,
        parallel: bool,
        staged: Option<&str>,
        selected: usize,
        row_label: &str,
    ) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.parallel = parallel;
        if let Some(spec) = staged {
            form.set_implementation_selection(selection(
                Some(spec),
                &[("thinking", "off"), ("fast", "false")],
            ));
        }
        form.selected = selected;
        let text = render(&form, width);
        assert!(text.contains(HINT_TAIL));
        assert!(text.contains(&format!("▸ {row_label}")));
    }

    #[test_case(0, KeyCode::Up,   0    ; "up_at_zero_stays")]
    #[test_case(0, KeyCode::Down, 1    ; "down_from_zero")]
    #[test_case(MODEL_ROW, KeyCode::Down, MODEL_ROW ; "down_at_max_stays")]
    #[test_case(MODEL_ROW, KeyCode::Up, MODEL_ROW - 1 ; "up_from_max")]
    fn navigation(start: usize, code: KeyCode, expected: usize) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.selected = start;
        assert_eq!(form.handle_key(key(code)), PlanFormAction::Consumed);
        assert_eq!(form.selected, expected);
    }

    #[test_case(0, PlanFormAction::Hide              ; "enter_at_0_refine")]
    #[test_case(1, PlanFormAction::ClearAndImplement ; "enter_at_1")]
    #[test_case(2, PlanFormAction::Implement          ; "enter_at_2")]
    #[test_case(MODEL_ROW, PlanFormAction::OpenModelPicker ; "enter_at_model_row")]
    #[test_case(USE_CURRENT_ROW, PlanFormAction::UseCurrentModel ; "enter_at_use_current_row")]
    fn enter_dispatches(selected: usize, expected: PlanFormAction) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.selected = selected;
        assert_eq!(form.handle_key(key(KeyCode::Enter)), expected);
    }

    #[test]
    fn staged_identity_becoming_current_keeps_options() {
        let mut form = PlanForm::new();
        let options = [("thinking", "off"), ("fast", "false")];
        form.set_implementation_selection(selection(Some(OTHER_MODEL), &options));
        form.selected = USE_CURRENT_ROW;
        form.clear_model_if_current(OTHER_MODEL);
        assert_eq!(form.implementation_selection(), &selection(None, &options));
        assert_eq!(form.selected, USE_CURRENT_ROW);
    }

    #[test_case(None ; "options_only")]
    #[test_case(Some(OTHER_MODEL) ; "identity_and_options")]
    fn plan_form_use_current_clears_all_overrides(spec: Option<&str>) {
        let mut form = PlanForm::new();
        form.parallel = true;
        form.set_implementation_selection(selection(
            spec,
            &[("thinking", "off"), ("fast", "false")],
        ));
        form.use_current_model();
        assert!(form.implementation_selection().is_empty());
        assert!(form.parallel());
        assert_eq!(form.selected, MODEL_ROW);
        form.handle_key(key(KeyCode::Down));
        assert_eq!(form.selected, MODEL_ROW);
    }

    #[test]
    fn staged_selection_survives_planning_lifecycle_until_reset() {
        let mut form = PlanForm::new();
        let selection = selection(Some(OTHER_MODEL), &[("thinking", "high"), ("fast", "true")]);
        form.set_implementation_selection(selection.clone());
        form.parallel = true;
        form.on_plan_ready();
        form.hide();
        form.toggle();
        form.on_plan_drafting();
        form.on_plan_ready();
        assert_eq!(form.implementation_selection(), &selection);
        form.reset();
        assert!(form.implementation_selection().is_empty());
        assert!(form.parallel());
    }

    #[test]
    fn matching_identity_cleanup_clamps_reset_row() {
        let mut form = PlanForm::new();
        form.set_implementation_selection(selection(Some(OTHER_MODEL), &[]));
        form.selected = USE_CURRENT_ROW;
        form.clear_model_if_current(OTHER_MODEL);
        assert!(form.implementation_selection().is_empty());
        assert_eq!(form.selected, MODEL_ROW);
    }

    #[test_case(None ; "options_only")]
    #[test_case(Some(OTHER_MODEL) ; "identity_and_options")]
    fn renders_staged_selection(spec: Option<&str>) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        form.set_implementation_selection(selection(
            spec,
            &[("thinking", "off"), ("fast", "false")],
        ));
        let text = render(&form, WIDE);
        assert!(text.contains(&format!(
            "Implementation model: {}",
            spec.unwrap_or(CURRENT_MODEL)
        )));
        assert!(text.contains("thinking=off fast=false  selected"));
        assert!(text.contains(USE_CURRENT_LABEL));
        assert!(text.contains(HINT_TAIL));
    }

    #[test]
    fn unstaged_form_shows_current_model_only() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        let text = render(&form, WIDE);
        assert!(text.contains(&format!("Implementation model: {CURRENT_MODEL}  current")));
        assert!(!text.contains("thinking="));
        assert!(!text.contains(USE_CURRENT_LABEL));
    }

    #[test]
    fn model_shortcut_opens_picker() {
        let mut form = PlanForm::new();
        assert_eq!(
            form.handle_key(key::MODEL_PICKER.to_key_event()),
            PlanFormAction::OpenModelPicker
        );
    }

    #[test]
    fn space_toggles_parallel() {
        let mut form = PlanForm::new();
        let initial = form.parallel();
        form.on_plan_ready();
        assert_eq!(form.parallel(), initial);
        assert_eq!(
            form.handle_key(key(KeyCode::Char(' '))),
            PlanFormAction::Consumed
        );
        assert_eq!(form.parallel(), !initial);
        assert_eq!(
            form.handle_key(key(KeyCode::Char(' '))),
            PlanFormAction::Consumed
        );
        assert_eq!(form.parallel(), initial);
    }

    #[test_case(key(KeyCode::Esc)              ; "esc")]
    #[test_case(key::QUIT.to_key_event()      ; "ctrl_c")]
    #[test_case(key::PLAN_TOGGLE.to_key_event(); "ctrl_t")]
    fn dismiss(k: KeyEvent) {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        assert_eq!(form.handle_key(k), PlanFormAction::Hide);
    }

    #[test]
    fn ctrl_o_opens_editor() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        assert_eq!(
            form.handle_key(key::OPEN_EDITOR.to_key_event()),
            PlanFormAction::OpenEditor
        );
    }

    #[test]
    fn unknown_key_consumed() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        assert_eq!(
            form.handle_key(key(KeyCode::Char('x'))),
            PlanFormAction::Consumed
        );
    }

    #[test]
    fn tab_passes_through() {
        let mut form = PlanForm::new();
        form.on_plan_ready();
        assert_eq!(
            form.handle_key(key(KeyCode::Tab)),
            PlanFormAction::Passthrough
        );
    }
}
