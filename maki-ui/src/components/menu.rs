use crate::click::{ClickTarget, HitTarget};
use crate::theme;
use maki_providers::ThinkingConfig;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Clear, Paragraph};
use unicode_width::UnicodeWidthStr;

const BORDER: u16 = 2;
const PAD: u16 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MenuKind {
    Mode,
    Thinking,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MenuAction {
    Mode(String),
    Thinking(ThinkingConfig),
}

#[derive(Clone, Debug, PartialEq)]
pub struct MenuItem {
    pub label: String,
    pub current: bool,
    pub action: MenuAction,
}

struct MenuState {
    kind: MenuKind,
    items: Vec<MenuItem>,
    selected: usize,
    anchor: Rect,
    area: Rect,
}

#[derive(Default)]
pub struct Menu {
    state: Option<MenuState>,
}

impl Menu {
    pub fn is_open(&self) -> bool {
        self.state.is_some()
    }

    pub fn kind(&self) -> Option<MenuKind> {
        self.state.as_ref().map(|s| s.kind)
    }

    pub fn open(&mut self, kind: MenuKind, anchor: Rect, items: Vec<MenuItem>) {
        let selected = items
            .iter()
            .position(|item| item.current)
            .unwrap_or_default();
        self.state = Some(MenuState {
            kind,
            items,
            selected,
            anchor,
            area: Rect::default(),
        });
    }

    pub fn close(&mut self) {
        self.state = None;
    }

    pub fn area(&self) -> Option<Rect> {
        self.state
            .as_ref()
            .filter(|s| s.area.height > 0)
            .map(|s| s.area)
    }

    pub fn up(&mut self) {
        if let Some(state) = &mut self.state
            && state.selected > 0
        {
            state.selected -= 1;
        }
    }

    pub fn down(&mut self) {
        if let Some(state) = &mut self.state
            && state.selected + 1 < state.items.len()
        {
            state.selected += 1;
        }
    }

    pub fn take_action(&mut self) -> Option<MenuAction> {
        self.state
            .as_ref()
            .and_then(|s| s.items.get(s.selected))
            .map(|item| item.action.clone())
    }

    pub fn click_row(&mut self, index: usize) -> Option<MenuAction> {
        let state = self.state.as_mut()?;
        if index >= state.items.len() {
            return None;
        }
        state.selected = index;
        Some(state.items[index].action.clone())
    }

    pub fn view(
        &mut self,
        frame: &mut Frame,
        frame_area: Rect,
        hover: Option<ClickTarget>,
    ) -> Vec<HitTarget> {
        let Some(state) = &mut self.state else {
            return Vec::new();
        };
        let label_width = state
            .items
            .iter()
            .map(|item| item.label.width() as u16 + PAD)
            .max()
            .unwrap_or(0);
        let width = (label_width + BORDER).min(frame_area.width);
        let height = (state.items.len() as u16 + BORDER).min(frame_area.height);
        let below = state.anchor.bottom().saturating_add(height) <= frame_area.bottom();
        let y = if below {
            state.anchor.bottom()
        } else {
            state.anchor.y.saturating_sub(height)
        };
        let area = Rect {
            x: state.anchor.x.min(frame_area.right().saturating_sub(width)),
            y: y.min(frame_area.bottom().saturating_sub(height)),
            width,
            height,
        };
        state.area = area;
        if width == 0 || height == 0 {
            return Vec::new();
        }

        let t = theme::current();
        frame.render_widget(Clear, area);
        let block = Block::default()
            .borders(ratatui::widgets::Borders::ALL)
            .border_style(t.status_dim)
            .style(Style::new().bg(t.background));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let mut hits = Vec::new();
        for (i, item) in state.items.iter().enumerate() {
            let y = inner.y + i as u16;
            if y >= inner.bottom() {
                break;
            }
            let row = Rect::new(inner.x, y, inner.width, 1);
            let hovered = hover == Some(ClickTarget::MenuRow(i as u16));
            let mut style = if i == state.selected {
                t.item_selected
            } else {
                Style::new()
            };
            if hovered {
                style = style.add_modifier(Modifier::UNDERLINED);
            }
            let marker = if item.current { "\u{2022} " } else { "  " };
            frame.render_widget(
                Paragraph::new(Line::from(Span::styled(
                    format!("{marker}{}", item.label),
                    style,
                ))),
                row,
            );
            hits.push(HitTarget {
                rect: row,
                target: ClickTarget::MenuRow(i as u16),
            });
        }
        hits
    }
}
