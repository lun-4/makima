use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};

use makima_agent::permissions::{DEFAULT_DENY_GUIDANCE, PermissionAnswer, generalized_scopes};
use makima_config::ToolKey;

use crate::click::{ClickTarget, HitTarget};
use crate::components::Overlay;
use crate::components::is_ctrl;
use crate::components::keybindings::key;
use crate::text_buffer::TextBuffer;
use crate::theme;
use crate::wrap::{Measure, index_at};

const GUIDANCE_PREFIX: &str = "  guide ";

const HINT_ALLOW_ROW: &[(&str, &str)] = &[
    ("y", "Allow"),
    ("a", "Always (project)"),
    ("A", "Always (all projects)"),
    ("s", "Session"),
];
const HINT_DENY_ROW: &[(&str, &str)] = &[
    ("n", "Deny"),
    ("d", "Deny-always (project)"),
    ("D", "Deny-always (all)"),
];
const HINT_DEFER_ROW: &[(&str, &str)] = &[(key::DEFER_INPUT.label, "defer, keep typing")];

const CONFIRM_ALLOW_PROJECT_HINTS: &[(&str, &str)] = &[
    ("Enter / y", "Confirm allow-always (project)"),
    ("any", "Cancel"),
];
const CONFIRM_ALLOW_ALL_HINTS: &[(&str, &str)] = &[
    ("Enter / y", "Confirm allow-always (all projects)"),
    ("any", "Cancel"),
];
const CONFIRM_SESSION_HINTS: &[(&str, &str)] =
    &[("Enter / y", "Confirm allow (session)"), ("any", "Cancel")];
const CONFIRM_DENY_PROJECT_HINTS: &[(&str, &str)] = &[
    ("Enter / y", "Confirm deny-always (project)"),
    ("any", "Cancel"),
];
const CONFIRM_DENY_ALL_HINTS: &[(&str, &str)] = &[
    ("Enter / y", "Confirm deny-always (all projects)"),
    ("any", "Cancel"),
];

const DENY_GUIDANCE_HINTS: &[(&str, &str)] = &[("Enter", "Deny"), ("Esc", "Cancel")];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum PromptState {
    #[default]
    Normal,
    ConfirmAllowAlwaysLocal,
    ConfirmAllowAlwaysGlobal,
    ConfirmAllowSession,
    ConfirmDenyAlwaysLocal,
    ConfirmDenyAlwaysGlobal,
    DenyEditing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HintAction {
    Char(char),
    Enter,
    Esc,
    AnyCancel,
    Defer,
}

#[derive(Debug, Clone)]
struct HintCell {
    key: &'static str,
    desc: &'static str,
    action: HintAction,
}

impl HintCell {
    fn new(key: &'static str, desc: &'static str, action: HintAction) -> Self {
        Self { key, desc, action }
    }
}

fn hint_cells(row: &[(&'static str, &'static str)]) -> Vec<HintCell> {
    row.iter()
        .map(|&(key, desc)| {
            let action = key
                .chars()
                .next()
                .map_or(HintAction::Defer, HintAction::Char);
            HintCell::new(key, desc, action)
        })
        .collect()
}

fn confirm_row(hints: &[(&'static str, &'static str)], action: HintAction) -> Vec<Vec<HintCell>> {
    vec![vec![
        HintCell::new(hints[0].0, hints[0].1, action),
        HintCell::new("any", "Cancel", HintAction::AnyCancel),
    ]]
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct GuidanceHit {
    rect: Rect,
}

#[allow(clippy::large_enum_variant)]
pub enum PermissionPrompt {
    Closed,
    Open {
        guidance: Option<Box<GuidanceHit>>,
        #[allow(dead_code)]
        id: String,
        tool: ToolKey,
        scopes: Vec<String>,
        subagent_id: Option<String>,
        agent_id: Option<makima_agent::AgentId>,
        allow_scopes: Vec<String>,
        state: PromptState,
        buffer: TextBuffer,
    },
}

impl Overlay for PermissionPrompt {
    fn is_open(&self) -> bool {
        matches!(self, Self::Open { .. })
    }

    fn is_modal(&self) -> bool {
        false
    }

    fn close(&mut self) {
        *self = Self::Closed;
    }
}

impl PermissionPrompt {
    pub fn new() -> Self {
        Self::Closed
    }

    #[cfg(test)]
    pub fn open(
        &mut self,
        id: String,
        tool: ToolKey,
        scopes: Vec<String>,
        subagent_id: Option<String>,
    ) {
        self.open_for_agent(id, tool, scopes, subagent_id, None);
    }

    pub fn open_for_agent(
        &mut self,
        id: String,
        tool: ToolKey,
        scopes: Vec<String>,
        subagent_id: Option<String>,
        agent_id: Option<makima_agent::AgentId>,
    ) {
        let allow_scopes = generalized_scopes(&tool, &scopes);
        let allow_scopes = if allow_scopes == scopes {
            vec![]
        } else {
            allow_scopes
        };
        *self = Self::Open {
            id,
            tool,
            scopes,
            subagent_id,
            agent_id,
            allow_scopes,
            state: PromptState::Normal,
            buffer: TextBuffer::new(String::new()),
            guidance: None,
        };
    }

    pub(crate) fn tool(&self) -> Option<&ToolKey> {
        match self {
            Self::Open { tool, .. } => Some(tool),
            Self::Closed => None,
        }
    }

    pub fn agent_id(&self) -> Option<makima_agent::AgentId> {
        match self {
            Self::Open { agent_id, .. } => *agent_id,
            Self::Closed => None,
        }
    }

    /// The ask the agent is parked on. Its answer has to name it, or the agent
    /// refuses it as one meant for some other request.
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::Open { id, .. } => Some(id),
            Self::Closed => None,
        }
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<PermissionAnswer> {
        let Self::Open { state, buffer, .. } = self else {
            return None;
        };
        if is_ctrl(&key) && key.code == KeyCode::Char('c') {
            return Some(PermissionAnswer::Deny);
        }
        if *state == PromptState::DenyEditing {
            return match key.code {
                KeyCode::Enter => {
                    let text = buffer.value().trim().to_string();
                    if text.is_empty() {
                        Some(PermissionAnswer::Deny)
                    } else {
                        Some(PermissionAnswer::DenyWithGuidance(text))
                    }
                }
                KeyCode::Esc => {
                    *buffer = TextBuffer::new(String::new());
                    *state = PromptState::Normal;
                    None
                }
                _ => {
                    buffer.handle_key(key);
                    None
                }
            };
        }
        if key
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
        {
            return None;
        }
        let confirm_answer = match *state {
            PromptState::ConfirmAllowAlwaysLocal => Some(PermissionAnswer::AllowAlwaysLocal),
            PromptState::ConfirmAllowAlwaysGlobal => Some(PermissionAnswer::AllowAlwaysGlobal),
            PromptState::ConfirmAllowSession => Some(PermissionAnswer::AllowSession),
            PromptState::ConfirmDenyAlwaysLocal => Some(PermissionAnswer::DenyAlwaysLocal),
            PromptState::ConfirmDenyAlwaysGlobal => Some(PermissionAnswer::DenyAlwaysGlobal),
            _ => None,
        };
        if let Some(answer) = confirm_answer {
            return match key.code {
                KeyCode::Char('y') | KeyCode::Enter => Some(answer),
                _ => {
                    *state = PromptState::Normal;
                    None
                }
            };
        }
        match key.code {
            KeyCode::Char('y') => Some(PermissionAnswer::AllowOnce),
            KeyCode::Char('n') => {
                *state = PromptState::DenyEditing;
                None
            }
            KeyCode::Char('a') => {
                *state = PromptState::ConfirmAllowAlwaysLocal;
                None
            }
            KeyCode::Char('A') => {
                *state = PromptState::ConfirmAllowAlwaysGlobal;
                None
            }
            KeyCode::Char('d') => {
                *state = PromptState::ConfirmDenyAlwaysLocal;
                None
            }
            KeyCode::Char('D') => {
                *state = PromptState::ConfirmDenyAlwaysGlobal;
                None
            }
            KeyCode::Char('s') => {
                *state = PromptState::ConfirmAllowSession;
                None
            }
            _ => None,
        }
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        let Self::Open { state, buffer, .. } = self else {
            return false;
        };
        if *state == PromptState::DenyEditing {
            buffer.insert_text(text);
            return true;
        }
        false
    }

    fn body_lines(&self) -> Vec<Line<'static>> {
        let Self::Open {
            tool,
            scopes,
            subagent_id,
            allow_scopes,
            state,
            buffer,
            ..
        } = self
        else {
            return vec![];
        };
        let t = theme::current();
        let label_style = t.tool_dim;
        let value_style = Style::new().fg(t.foreground);

        let mut tool_spans = vec![Span::raw("  "), Span::styled("tool  ", label_style)];
        if subagent_id.is_some() {
            tool_spans.push(Span::styled("[subtask] ", t.item_desc));
        }
        tool_spans.push(Span::styled(tool.to_string(), value_style));

        let mut lines = vec![Line::raw(""), Line::from(tool_spans)];
        for (i, s) in scopes.iter().enumerate() {
            let label = if i == 0 { "scope " } else { "    + " };
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(label, label_style),
                Span::styled(s.clone(), value_style),
            ]));
        }

        if !allow_scopes.is_empty() {
            for (i, g) in allow_scopes.iter().enumerate() {
                let label = if i == 0 { "allow " } else { "    + " };
                lines.push(Line::from(vec![
                    Span::raw("  "),
                    Span::styled(label, label_style),
                    Span::styled(g.clone(), value_style),
                ]));
            }
        }

        if *state == PromptState::DenyEditing {
            let text = buffer.value();
            let (display_text, cursor_pos) = if text.is_empty() {
                (DEFAULT_DENY_GUIDANCE, 0)
            } else {
                (text.as_str(), buffer.cursor_byte_offset())
            };
            let display = Span::raw(display_text);
            let cursor = display
                .styled_graphemes(Style::default())
                .find_map(|grapheme| {
                    let start = grapheme.symbol.as_ptr() as usize - display_text.as_ptr() as usize;
                    let end = start + grapheme.symbol.len();
                    (cursor_pos < end).then_some((start, end))
                });
            let (start, end) = cursor.unwrap_or((cursor_pos, cursor_pos));
            let cursor_text = cursor.map_or(" ", |_| &display_text[start..end]);
            let mut spans = vec![Span::styled(GUIDANCE_PREFIX, label_style)];
            spans.push(Span::styled(
                display_text[..start].to_string(),
                if text.is_empty() {
                    t.tool_dim
                } else {
                    Style::default()
                },
            ));
            spans.push(Span::styled(
                cursor_text.to_string(),
                Style::new().reversed(),
            ));
            spans.push(Span::styled(
                display_text[end..].to_string(),
                if text.is_empty() {
                    t.tool_dim
                } else {
                    Style::default()
                },
            ));
            lines.push(Line::from(spans));
        }

        lines.push(Line::raw(""));
        lines
    }

    fn hint_rows(&self) -> Vec<Vec<HintCell>> {
        let Self::Open { state, .. } = self else {
            return vec![];
        };
        match *state {
            PromptState::ConfirmAllowAlwaysLocal => {
                confirm_row(CONFIRM_ALLOW_PROJECT_HINTS, HintAction::Enter)
            }
            PromptState::ConfirmAllowAlwaysGlobal => {
                confirm_row(CONFIRM_ALLOW_ALL_HINTS, HintAction::Enter)
            }
            PromptState::ConfirmAllowSession => {
                confirm_row(CONFIRM_SESSION_HINTS, HintAction::Enter)
            }
            PromptState::ConfirmDenyAlwaysLocal => {
                confirm_row(CONFIRM_DENY_PROJECT_HINTS, HintAction::Enter)
            }
            PromptState::ConfirmDenyAlwaysGlobal => {
                confirm_row(CONFIRM_DENY_ALL_HINTS, HintAction::Enter)
            }
            PromptState::DenyEditing => vec![vec![
                HintCell::new(
                    DENY_GUIDANCE_HINTS[0].0,
                    DENY_GUIDANCE_HINTS[0].1,
                    HintAction::Enter,
                ),
                HintCell::new(
                    DENY_GUIDANCE_HINTS[1].0,
                    DENY_GUIDANCE_HINTS[1].1,
                    HintAction::Esc,
                ),
            ]],
            PromptState::Normal => vec![
                hint_cells(HINT_ALLOW_ROW),
                hint_cells(HINT_DENY_ROW),
                vec![HintCell::new(
                    HINT_DEFER_ROW[0].0,
                    HINT_DEFER_ROW[0].1,
                    HintAction::Defer,
                )],
            ],
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Vec<HitTarget> {
        if !self.is_open() {
            return Vec::new();
        }
        let t = theme::current();
        let block = Block::default()
            .borders(ratatui::widgets::Borders::ALL)
            .border_type(ratatui::widgets::BorderType::Rounded)
            .border_style(t.panel_border)
            .title_top(Line::from(" Permission Required ").left_aligned())
            .title_style(t.panel_title);
        let inner = block.inner(area);
        frame.render_widget(block, area);
        frame.render_widget(Block::default().style(Style::new().bg(t.background)), inner);

        let body = self.body_lines();
        let guidance_start = {
            let mut measure = Measure::new(inner.width);
            let mut row = 0u16;
            let mut start = None;
            for (index, line) in body.iter().enumerate() {
                if index + 2 == body.len() {
                    start = Some(row);
                }
                row = row.saturating_add(measure.rows(line).max(1));
            }
            start
        };
        let body_rows = Paragraph::new(body.clone())
            .wrap(Wrap { trim: false })
            .line_count(inner.width) as u16;
        let body_h = body_rows.min(inner.height);
        let body_area = Rect {
            x: inner.x,
            y: inner.y,
            width: inner.width,
            height: body_h,
        };
        frame.render_widget(
            Paragraph::new(body)
                .style(Style::new().fg(t.foreground))
                .wrap(Wrap { trim: false }),
            body_area,
        );

        let rows = self.hint_rows();
        let cell_hits = render_hints(frame, inner, body_area.bottom(), &rows);
        let mut hits = Vec::new();
        for (row, cells) in rows.iter().enumerate() {
            for (cell, hint) in cells.iter().enumerate() {
                let Some(rect) = cell_hits
                    .iter()
                    .find(|((r, c), _)| *r == row && *c == cell)
                    .map(|(_, rect)| *rect)
                else {
                    continue;
                };
                hits.push(HitTarget {
                    rect,
                    target: if hint.action == HintAction::Defer {
                        ClickTarget::DeferInput
                    } else {
                        ClickTarget::PermissionHint {
                            row: row as u16,
                            cell: cell as u16,
                        }
                    },
                });
            }
        }
        if let Self::Open {
            state,
            guidance: guidance_hit,
            ..
        } = self
            && *state == PromptState::DenyEditing
        {
            let start = guidance_start.unwrap_or(0);
            let rect = Rect::new(
                inner.x,
                inner.y.saturating_add(start),
                inner.width,
                body_h.saturating_sub(start),
            );
            *guidance_hit = (rect.height > 0).then(|| Box::new(GuidanceHit { rect }));
            if rect.height > 0 {
                hits.push(HitTarget {
                    rect,
                    target: ClickTarget::PermissionGuidance,
                });
            }
        }
        hits
    }

    pub fn click_hint(&mut self, row: u16, cell: u16) -> Option<PermissionAnswer> {
        let rows = self.hint_rows();
        let action = rows.get(row as usize)?.get(cell as usize)?.action;
        let key = match action {
            HintAction::Char(ch) => KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE),
            HintAction::Enter => KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
            HintAction::Esc | HintAction::AnyCancel => {
                KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)
            }
            HintAction::Defer => return None,
        };
        self.handle_key(key)
    }

    pub fn click_guidance(&mut self, pos: ratatui::layout::Position) {
        let Some(line) = self.body_lines().into_iter().rev().nth(1) else {
            return;
        };
        let Self::Open {
            state,
            buffer,
            guidance,
            ..
        } = self
        else {
            return;
        };
        if *state != PromptState::DenyEditing {
            return;
        }
        let Some(guidance) = guidance.as_deref() else {
            return;
        };
        if !guidance.rect.contains(pos) {
            return;
        }
        let row = pos.y.saturating_sub(guidance.rect.y);
        let col = pos.x.saturating_sub(guidance.rect.x);
        let index = index_at(&line, guidance.rect.width, row, col)
            .saturating_sub(GUIDANCE_PREFIX.len())
            .min(buffer.value().len());
        buffer.set_cursor_byte_offset(index);
    }

    pub fn height(&self, width: u16) -> u16 {
        let inner_width = width.saturating_sub(2);
        let body_rows = Paragraph::new(self.body_lines())
            .wrap(Wrap { trim: false })
            .line_count(inner_width) as u16;
        body_rows + self.hint_rows().len() as u16 + 3
    }
}

fn render_hints(
    frame: &mut Frame,
    inner: Rect,
    top: u16,
    rows: &[Vec<HintCell>],
) -> Vec<((usize, usize), Rect)> {
    let t = theme::current();
    let cols = rows.iter().map(Vec::len).max().unwrap_or(0);
    let mut widths = vec![0usize; cols];
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.key.len() + cell.desc.len() + 3);
        }
    }
    let mut hits = Vec::new();
    for (row_index, row) in rows.iter().enumerate() {
        let y = top.saturating_add(row_index as u16);
        if y >= inner.bottom() {
            break;
        }
        let mut x = inner.x;
        for (cell_index, cell) in row.iter().enumerate() {
            let width = widths[cell_index] as u16;
            let right = x.saturating_add(width).min(inner.right());
            if right <= x {
                break;
            }
            let rect = Rect::new(x, y, right - x, 1);
            let spans = vec![
                Span::raw("  "),
                Span::styled(format!("{} ", cell.key), t.keybind_key),
                Span::styled(cell.desc, t.tool_dim),
            ];
            frame.render_widget(Paragraph::new(Line::from(spans)), rect);
            if x.saturating_add(width) <= inner.right() {
                hits.push(((row_index, cell_index), rect));
            }
            x = x.saturating_add(width);
        }
    }
    hits
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use makima_agent::permissions::PermissionAnswer;
    use makima_config::ToolKey;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::{Position, Rect};
    use test_case::test_case;

    use crate::click::ClickTarget;
    use crate::text_buffer::TextBuffer;

    use super::{PermissionPrompt, PromptState};

    fn open_prompt() -> PermissionPrompt {
        let mut prompt = PermissionPrompt::new();
        prompt.open(
            "id".into(),
            ToolKey::native("bash"),
            vec!["execute".into()],
            None,
        );
        prompt
    }

    fn ctrl_c() -> KeyEvent {
        KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn ctrl_c_denies() {
        let mut prompt = open_prompt();
        assert_eq!(prompt.handle_key(ctrl_c()), Some(PermissionAnswer::Deny));
        // Also test from editing state
        let mut prompt2 = open_prompt();
        prompt2.handle_key(key(KeyCode::Char('n')));
        prompt2.handle_key(key(KeyCode::Char('t')));
        assert_eq!(prompt2.handle_key(ctrl_c()), Some(PermissionAnswer::Deny));
    }

    #[test]
    fn n_goes_to_deny_editing() {
        let mut prompt = open_prompt();
        assert_eq!(prompt.handle_key(key(KeyCode::Char('n'))), None);
        if let PermissionPrompt::Open { state, .. } = &prompt {
            assert_eq!(*state, PromptState::DenyEditing);
        } else {
            panic!("expected Open");
        }
    }

    #[test]
    fn deny_editing_esc_returns_to_normal() {
        let mut prompt = open_prompt();
        prompt.handle_key(key(KeyCode::Char('n')));
        prompt.handle_key(key(KeyCode::Char('t')));
        assert_eq!(prompt.handle_key(key(KeyCode::Esc)), None);
        if let PermissionPrompt::Open { state, buffer, .. } = &prompt {
            assert_eq!(*state, PromptState::Normal);
            assert!(buffer.value().is_empty());
        } else {
            panic!("expected Open");
        }
    }

    #[test]
    fn deny_editing_enter_empty_sends_deny() {
        let mut prompt = open_prompt();
        prompt.handle_key(key(KeyCode::Char('n')));
        assert_eq!(
            prompt.handle_key(key(KeyCode::Enter)),
            Some(PermissionAnswer::Deny)
        );
    }

    #[test]
    fn deny_editing_with_text_sends_guidance() {
        let mut prompt = open_prompt();
        prompt.handle_key(key(KeyCode::Char('n')));
        prompt.handle_paste("Use cat");
        assert_eq!(
            prompt.handle_key(key(KeyCode::Enter)),
            Some(PermissionAnswer::DenyWithGuidance("Use cat".into()))
        );
    }

    #[test]
    fn handle_paste_requires_editing_mode() {
        let mut prompt = open_prompt();
        assert!(!prompt.handle_paste("ignored"));
        prompt.handle_key(key(KeyCode::Char('n')));
        assert!(prompt.handle_paste("accepted"));
        if let PermissionPrompt::Open { buffer, .. } = &prompt {
            assert_eq!(buffer.value(), "accepted");
        } else {
            panic!("expected Open");
        }
    }

    #[test]
    fn wildcard_tool_key_opens() {
        let mut prompt = PermissionPrompt::new();
        prompt.open("id".into(), ToolKey::Wildcard, vec![], None);
        assert!(matches!(prompt, PermissionPrompt::Open { .. }));
    }

    #[test]
    fn click_hint_matches_its_key() {
        let mut prompt = open_prompt();
        assert_eq!(prompt.click_hint(0, 0), Some(PermissionAnswer::AllowOnce));
        let mut prompt = open_prompt();
        prompt.click_hint(1, 0);
        if let PermissionPrompt::Open { state, .. } = &prompt {
            assert_eq!(*state, PromptState::DenyEditing);
        } else {
            panic!("expected Open");
        }
    }

    #[test]
    fn defer_hint_has_actionable_hit() {
        let mut prompt = open_prompt();
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        let mut hits = Vec::new();
        terminal
            .draw(|frame| hits = prompt.view(frame, frame.area()))
            .unwrap();
        let hit = hits
            .iter()
            .find(|hit| hit.target == ClickTarget::DeferInput)
            .unwrap();
        let rendered = terminal.backend().buffer();
        let text: String = (hit.rect.x..hit.rect.right())
            .map(|x| rendered[Position::new(x, hit.rect.y)].symbol())
            .collect();
        assert!(text.contains("defer, keep typing"));
    }

    #[test_case("hello world", 16, 0, 14, 5 ; "past_word_wrapped_row_end")]
    #[test_case("hello world", 16, 1, 0, 6 ; "continuation_word_start")]
    #[test_case("hello world", 16, 1, 2, 8 ; "continuation_word_inside")]
    #[test_case("hello world", 16, 1, 15, 11 ; "past_final_row_end")]
    #[test_case("abcdefghijklmnopqrst", 12, 2, 0, 12 ; "continuation_long_word")]
    #[test_case("漢字 abc", 16, 0, 9, 0 ; "wide_grapheme_second_cell")]
    #[test_case("e\u{301}cole", 16, 0, 9, 3 ; "combining_grapheme")]
    #[test_case("a    b", 12, 1, 0, 5 ; "swallowed_whitespace")]
    #[test_case("ab\ncd", 16, 0, 10, 3 ; "filtered_newline_keeps_source_offset")]
    fn guidance_click_matches_rendered_text(
        text: &str,
        width: u16,
        row: u16,
        col: u16,
        expected_byte: usize,
    ) {
        let mut prompt = open_prompt();
        prompt.handle_key(key(KeyCode::Char('n')));
        prompt.handle_paste(text);
        let mut terminal = Terminal::new(TestBackend::new(80, 60)).unwrap();
        let mut hits = Vec::new();
        terminal
            .draw(|frame| hits = prompt.view(frame, Rect::new(2, 3, width + 2, 50)))
            .unwrap();
        let hit = hits
            .iter()
            .find(|hit| hit.target == ClickTarget::PermissionGuidance)
            .unwrap();
        let position = Position::new(hit.rect.x + col, hit.rect.y + row);
        prompt.click_guidance(position);
        let PermissionPrompt::Open { buffer, .. } = &mut prompt else {
            panic!("expected Open");
        };
        assert_eq!(buffer.cursor_byte_offset(), expected_byte);
        buffer.insert_text("X");
        let mut expected = text.to_string();
        expected.insert(expected_byte, 'X');
        assert_eq!(buffer.value(), expected);
        terminal
            .draw(|frame| {
                prompt.view(frame, Rect::new(2, 3, width + 2, 50));
            })
            .unwrap();
        let rendered = terminal.backend().buffer();
        assert!(rendered.content.iter().any(|cell| cell.symbol() == "X"));
    }

    #[test]
    fn guidance_caret_preserves_combining_grapheme() {
        let mut prompt = open_prompt();
        prompt.handle_key(key(KeyCode::Char('n')));
        if let PermissionPrompt::Open { buffer, .. } = &mut prompt {
            *buffer = TextBuffer::new("e\u{301}cole".to_string());
            buffer.set_cursor(0, 1);
        }
        let lines = prompt.body_lines();
        let guide = &lines[lines.len() - 2];
        assert!(guide.spans.iter().any(|span| span.content == "e\u{301}"));
    }

    #[test]
    fn click_confirm_row_accepts() {
        let mut prompt = open_prompt();
        prompt.click_hint(0, 1);
        if let PermissionPrompt::Open { state, .. } = &prompt {
            assert_eq!(*state, PromptState::ConfirmAllowAlwaysLocal);
        } else {
            panic!("expected Open");
        }
        assert_eq!(
            prompt.click_hint(0, 0),
            Some(PermissionAnswer::AllowAlwaysLocal)
        );
    }
}
