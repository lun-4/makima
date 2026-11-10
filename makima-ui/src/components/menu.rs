use crate::click::{ClickTarget, HitTarget};
use crate::theme;
use makima_providers::ThinkingConfig;
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
    Permission,
}

#[derive(Clone, Debug, PartialEq)]
pub enum MenuAction {
    Mode(String),
    Thinking(ThinkingConfig),
    Permission(bool),
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

    pub fn open_permission(&mut self, anchor: Rect, yolo: bool) {
        let items = [("Normal", false), ("YOLO", true)]
            .into_iter()
            .map(|(label, enabled)| MenuItem {
                label: label.to_owned(),
                current: enabled == yolo,
                action: MenuAction::Permission(enabled),
            })
            .collect();
        self.open(MenuKind::Permission, anchor, items);
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

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use test_case::test_case;

    const FRAME_WIDTH: u16 = 40;
    const FRAME_HEIGHT: u16 = 10;

    #[test_case(false ; "normal")]
    #[test_case(true ; "yolo")]
    fn permission_menu_has_exclusive_current_selection(yolo: bool) {
        let mut menu = Menu::default();
        menu.open_permission(Rect::new(30, FRAME_HEIGHT - 1, 8, 1), yolo);
        assert_eq!(menu.kind(), Some(MenuKind::Permission));
        let state = menu.state.as_ref().unwrap();
        assert_eq!(state.items.len(), 2);
        assert_eq!(state.items.iter().filter(|item| item.current).count(), 1);
        assert_eq!(
            state.items[state.selected].action,
            MenuAction::Permission(yolo)
        );
        assert_eq!(state.items[0].label, "Normal");
        assert_eq!(state.items[1].label, "YOLO");
        assert_eq!(menu.take_action(), Some(MenuAction::Permission(yolo)));
        assert_eq!(menu.click_row(0), Some(MenuAction::Permission(false)));
        assert_eq!(menu.click_row(1), Some(MenuAction::Permission(true)));
        assert_eq!(menu.click_row(2), None);
    }

    #[test_case(false ; "normal_to_yolo")]
    #[test_case(true ; "yolo_to_normal")]
    fn permission_menu_keyboard_selection_sets_explicit_value(yolo: bool) {
        let mut menu = Menu::default();
        menu.open_permission(Rect::default(), yolo);
        if yolo {
            menu.up();
        } else {
            menu.down();
        }
        assert_eq!(menu.take_action(), Some(MenuAction::Permission(!yolo)));
        assert_eq!(menu.take_action(), Some(MenuAction::Permission(!yolo)));
        menu.close();
        assert!(!menu.is_open());
        assert_eq!(menu.take_action(), None);
    }

    #[test]
    fn permission_menu_renders_above_status_chip() {
        let mut terminal = Terminal::new(TestBackend::new(FRAME_WIDTH, FRAME_HEIGHT)).unwrap();
        let anchor = Rect::new(FRAME_WIDTH - 8, FRAME_HEIGHT - 1, 8, 1);
        let mut menu = Menu::default();
        menu.open_permission(anchor, false);
        let mut hits = Vec::new();
        terminal
            .draw(|frame| {
                hits = menu.view(frame, frame.area(), None);
            })
            .unwrap();
        let area = menu.area().unwrap();
        assert_eq!(area.bottom(), anchor.y);
        assert!(area.right() <= FRAME_WIDTH);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].target, ClickTarget::MenuRow(0));
        assert_eq!(hits[1].target, ClickTarget::MenuRow(1));
        let text = crate::components::buffer_text(terminal.backend().buffer());
        assert!(text.contains("Normal"));
        assert!(text.contains("YOLO"));
    }
}
