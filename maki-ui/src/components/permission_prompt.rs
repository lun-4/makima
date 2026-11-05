use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph, Wrap};

use maki_agent::permissions::{DEFAULT_DENY_GUIDANCE, PermissionAnswer, generalized_scopes};
use maki_config::ToolKey;

use crate::click::{ClickTarget, HitTarget};
use crate::components::Overlay;
use crate::components::is_ctrl;
use crate::components::keybindings::key;
use crate::text_buffer::TextBuffer;
use crate::theme;
use crate::wrap::Measure;
use unicode_width::UnicodeWidthChar;

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
        agent_id: Option<maki_agent::AgentId>,
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
        agent_id: Option<maki_agent::AgentId>,
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

    pub fn agent_id(&self) -> Option<maki_agent::AgentId> {
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
                (text.as_str(), TextBuffer::char_to_byte(&text, buffer.x()))
            };
            let (before, after) = display_text.split_at(cursor_pos);
            let mut chars = after.chars();
            let cursor_ch = chars.next().unwrap_or(' ');
            let rest: String = chars.collect();

            let mut spans = vec![Span::raw("  "), Span::styled("guide ", label_style)];
            if text.is_empty() {
                spans.push(Span::styled(cursor_ch.to_string(), Style::new().reversed()));
                spans.push(Span::styled(rest, t.tool_dim));
            } else {
                spans.push(Span::raw(before.to_string()));
                spans.push(Span::styled(cursor_ch.to_string(), Style::new().reversed()));
                if !rest.is_empty() {
                    spans.push(Span::raw(rest));
                }
            }
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
                if hint.action == HintAction::Defer {
                    continue;
                }
                hits.push(HitTarget {
                    rect,
                    target: ClickTarget::PermissionHint {
                        row: row as u16,
                        cell: cell as u16,
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
        let row = pos.y.saturating_sub(guidance.rect.y);
        let col = pos.x.saturating_sub(guidance.rect.x);
        let text = if buffer.value().is_empty() {
            DEFAULT_DENY_GUIDANCE.to_string()
        } else {
            buffer.value()
        };
        let index = guidance_index(&text, guidance.rect.width, row, col);
        buffer.set_cursor(0, index);
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

fn guidance_index(text: &str, width: u16, click_row: u16, click_col: u16) -> usize {
    let prefix = "  guide ";
    let mut row = 0u16;
    let mut col = prefix
        .chars()
        .map(|c| c.width().unwrap_or(0) as u16)
        .sum::<u16>();
    for (index, ch) in text.chars().enumerate() {
        let w = ch.width().unwrap_or(0) as u16;
        if col.saturating_add(w) > width {
            row = row.saturating_add(1);
            col = 0;
        }
        if row == click_row && col.saturating_add(w) > click_col {
            return index;
        }
        col = col.saturating_add(w);
    }
    text.chars().count()
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use maki_agent::permissions::PermissionAnswer;
    use maki_config::ToolKey;

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
}
