use std::time::{Duration, Instant};

use crate::click::{ClickTarget, ScrollOwner, Surface};
use crate::clipboard::CopyResult;
use crate::selection::{self, ContentRegion, EdgeScroll, Selection, SelectionState, SelectionZone};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};

use crate::repaint::Dirty;

use super::{Action, App, FLASH_NO_AGENT_CHAT};

pub(super) const EDGE_SCROLL_LINES: i32 = 1;
pub(super) const EDGE_SCROLL_INTERVAL: Duration = Duration::from_millis(25);

pub(super) const MIDDLE_SCROLL_INTERVAL: Duration = Duration::from_millis(25);
const MIDDLE_SCROLL_DEAD_ZONE: i32 = 1;
const MIDDLE_SCROLL_RATE: f64 = 2.0;
const MIDDLE_SCROLL_EXPONENT: f64 = 1.5;
const MIDDLE_SCROLL_MAX_RATE: f64 = 120.0;
const MIDDLE_SCROLL_MAX_ELAPSED: Duration = Duration::from_millis(100);
pub(super) const MIDDLE_SCROLL_ANCHOR: &str = "+";

fn result_action_hit(action: &str, width: u16, row: u16, col: u16) -> bool {
    let line = ratatui::text::Line::raw(action);
    crate::wrap::index_at(&line, width, row, col) < action.len()
}

pub(super) struct MiddleScroll {
    pub(super) origin: Position,
    chat: usize,
    chat_id: Option<String>,
    area: Rect,
    displacement: i32,
    last_update: Instant,
    fractional_lines: f64,
}

impl MiddleScroll {
    fn rate(&self) -> f64 {
        let distance = (self.displacement.abs() - MIDDLE_SCROLL_DEAD_ZONE).max(0);
        -(self.displacement.signum() as f64)
            * ((distance as f64).powf(MIDDLE_SCROLL_EXPONENT) * MIDDLE_SCROLL_RATE)
                .min(MIDDLE_SCROLL_MAX_RATE)
    }

    pub(super) fn move_to(&mut self, row: u16, now: Instant) {
        let old_rate = self.rate();
        self.displacement = i32::from(row) - i32::from(self.origin.y);
        if self.rate().signum() != old_rate.signum() || self.rate() == 0.0 {
            self.fractional_lines = 0.0;
        }
        self.last_update = now;
    }

    pub(super) fn delta(&mut self, now: Instant) -> i32 {
        let elapsed = now
            .saturating_duration_since(self.last_update)
            .min(MIDDLE_SCROLL_MAX_ELAPSED);
        self.last_update = now;
        self.fractional_lines += self.rate() * elapsed.as_secs_f64();
        let delta = self.fractional_lines.trunc() as i32;
        self.fractional_lines -= f64::from(delta);
        delta
    }
}

pub(super) struct ScrollDrag {
    pub(super) owner: ScrollOwner,
    pub(super) area: Rect,
    pub(super) content_len: u16,
    pub(super) grab: u16,
    chat: usize,
}

impl App {
    pub(crate) fn cancel_middle_scroll(&mut self) -> Dirty {
        self.middle_scroll.take().is_some().into()
    }

    fn middle_scroll_obstructed(&self) -> bool {
        self.any_overlay_open()
            || self.command_palette.is_active()
            || self.file_completion.is_active()
            || self.plan_form_active()
    }

    pub(super) fn validate_middle_scroll(&mut self) -> Dirty {
        let invalid = self.middle_scroll.as_ref().is_some_and(|state| {
            state.chat != self.active_chat
                || self
                    .chats
                    .get(state.chat)
                    .is_none_or(|chat| chat.subagent_id != state.chat_id)
                || self.middle_scroll_obstructed()
                || self.selection_state.is_some()
                || self
                    .zones
                    .find(SelectionZone::Messages)
                    .is_none_or(|zone| zone.area != state.area)
                || self
                    .zone_at(state.origin.y, state.origin.x)
                    .is_none_or(|zone| zone.zone != SelectionZone::Messages)
        });
        if invalid {
            self.cancel_middle_scroll()
        } else {
            Dirty::NO
        }
    }

    pub(crate) fn tick_middle_scroll_at(&mut self, now: Instant) -> Dirty {
        let dirty = self.validate_middle_scroll();
        let Some(state) = self.middle_scroll.as_mut() else {
            return dirty;
        };
        let delta = state.delta(now);
        if delta == 0 {
            return dirty;
        }
        let chat = &mut self.chats[state.chat];
        let before = chat.scroll_top();
        chat.scroll(delta);
        let after = chat.scroll_top();
        if u32::from(before.abs_diff(after)) < delta.unsigned_abs() {
            state.fractional_lines = 0.0;
        }
        dirty | Dirty::from(before != after)
    }

    pub(super) fn handle_mouse(&mut self, event: MouseEvent) -> (Dirty, Vec<Action>) {
        let mut dirty = Dirty::NO;
        match event.kind {
            MouseEventKind::Down(MouseButton::Middle) => {
                if self.middle_scroll.is_some() {
                    dirty |= self.cancel_middle_scroll();
                } else if !self.middle_scroll_obstructed()
                    && self.selection_state.is_none()
                    && let Some(zone) = self.zone_at(event.row, event.column)
                    && zone.zone == SelectionZone::Messages
                {
                    let top = self.chats[self.active_chat].scroll_top();
                    self.chats[self.active_chat].set_scroll_top(top);
                    self.middle_scroll = Some(MiddleScroll {
                        origin: Position::new(event.column, event.row),
                        chat: self.active_chat,
                        chat_id: self.chats[self.active_chat].subagent_id.clone(),
                        area: zone.area,
                        displacement: 0,
                        last_update: Instant::now(),
                        fractional_lines: 0.0,
                    });
                    dirty |= Dirty::YES;
                }
                return (dirty, vec![]);
            }
            MouseEventKind::Moved | MouseEventKind::Drag(MouseButton::Middle) => {
                let now = Instant::now();
                dirty |= self.tick_middle_scroll_at(now);
                if let Some(state) = self.middle_scroll.as_mut() {
                    state.move_to(event.row, now);
                }
                if event.kind == MouseEventKind::Moved {
                    dirty |= self.hover_update(Position::new(event.column, event.row));
                }
                return (dirty, vec![]);
            }
            MouseEventKind::Down(MouseButton::Left | MouseButton::Right)
            | MouseEventKind::ScrollUp
            | MouseEventKind::ScrollDown
            | MouseEventKind::ScrollLeft
            | MouseEventKind::ScrollRight => {
                dirty |= self.cancel_middle_scroll();
            }
            _ => {}
        }
        let actions = match event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                dirty |= self.left_down(event.row, event.column);
                vec![]
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                dirty |= self.left_drag(event.row, event.column);
                vec![]
            }
            MouseEventKind::Up(MouseButton::Left) => {
                let (d, actions) = self.left_up(event.row, event.column);
                dirty |= d;
                actions
            }
            _ => vec![],
        };
        (dirty, actions)
    }

    fn left_down(&mut self, row: u16, col: u16) -> Dirty {
        let pos = Position::new(col, row);
        self.pending_click = None;
        self.selection_state = None;
        if (self.hits.modal().is_some() || self.has_modal_overlay())
            && !self.hits.inside_modal(pos)
            && self.hits.surface_at(pos).is_none()
        {
            self.pending_click = self
                .hits
                .modal()
                .map(|(surface, _)| (ClickTarget::DismissSurface(surface), pos));
            return Dirty::NO;
        }

        let target = self.hits.target_at(pos);
        if self.menu.is_open() {
            let owns = matches!(target, Some(ClickTarget::MenuRow(_)))
                || (self.menu.kind() == Some(crate::components::menu::MenuKind::Mode)
                    && target == Some(ClickTarget::ModeChip))
                || (self.menu.kind() == Some(crate::components::menu::MenuKind::Thinking)
                    && target == Some(ClickTarget::ThinkingChip))
                || (self.menu.kind() == Some(crate::components::menu::MenuKind::Permission)
                    && target == Some(ClickTarget::PermissionChip))
                || target == Some(ClickTarget::Surface(Surface::Menu));
            if !owns {
                self.menu.close();
                return Dirty::NO;
            }
        }
        if let Some(ClickTarget::Scrollbar(owner)) = target {
            self.pending_click = None;
            self.start_scroll_drag(owner, pos);
            return Dirty::NO;
        }
        self.pending_click = target.map(|target| (target, pos));
        self.start_selection(pos);
        Dirty::NO
    }

    fn start_selection(&mut self, pos: Position) {
        let Some(zone) = self.zone_at(pos.y, pos.x) else {
            return;
        };
        if zone.zone == SelectionZone::Input {
            let focused = !self.any_overlay_open();
            self.input_box
                .handle_click(zone.area, pos.y, pos.x, focused);
            if focused {
                let input = self.input_box.buffer.value();
                self.sync_command_arguments(&input, self.input_box.buffer.cursor_byte_offset());
                self.sync_file_completion();
            }
        }
        let scroll = self.scroll_offset(zone.zone);
        self.selection_state = Some(SelectionState::Dragging {
            sel: Selection::start(pos.y, pos.x, zone.area, zone.zone, scroll),
            edge_scroll: None,
            last_drag_col: pos.x,
        });
    }

    fn left_drag(&mut self, row: u16, col: u16) -> Dirty {
        if self.scroll_drag.is_some() {
            self.apply_scroll_drag(row);
            Dirty::YES
        } else {
            self.handle_drag(row, col);
            Dirty::NO
        }
    }

    fn left_up(&mut self, row: u16, col: u16) -> (Dirty, Vec<Action>) {
        if self.scroll_drag.is_some() {
            self.apply_scroll_drag(row);
            self.scroll_drag = None;
            return (Dirty::YES, vec![]);
        }
        let pos = Position::new(col, row);
        let target = self.hits.target_at(pos);
        if let Some((ClickTarget::DismissSurface(surface), _)) = self
            .pending_click
            .take_if(|(target, _)| matches!(target, ClickTarget::DismissSurface(_)))
        {
            if self
                .hits
                .modal()
                .is_some_and(|(current, rect)| current == surface && !rect.contains(pos))
                && self.hits.surface_at(pos).is_none()
            {
                return (Dirty::YES, self.dismiss_surface(surface));
            }
            return (Dirty::NO, vec![]);
        }
        let click = self
            .pending_click
            .take()
            .filter(|(pending, _)| Some(*pending) == target);
        let empty = match &self.selection_state {
            None => true,
            Some(SelectionState::Dragging { sel, .. }) => sel.is_empty(),
            Some(SelectionState::PendingCopy { .. }) => false,
        };
        if let Some((target, pos)) = click
            && empty
        {
            self.selection_state = None;
            return (Dirty::NO, self.dispatch_click(target, pos));
        }
        if let Some(SelectionState::Dragging { sel, .. }) = self.selection_state {
            if !sel.is_empty() {
                self.selection_state = Some(SelectionState::PendingCopy { sel });
            } else {
                let zone = sel.zone;
                self.selection_state = None;
                if zone == SelectionZone::Messages {
                    let area = self.msg_area();
                    if let Some(id) = self.chats[self.active_chat].result_navigation_at(row, area) {
                        if !self.result_action_contains(row, col, area, id) {
                            return (Dirty::NO, vec![]);
                        }
                        let historical = self.chats[self.active_chat]
                            .message_at_row(row, area)
                            .is_some_and(|message| message.result_historical);
                        let target = if historical {
                            self.historical_chat_index(id)
                        } else {
                            self.live_chat_index.get(&id).copied()
                        };
                        if let Some(index) = target {
                            self.active_chat = index;
                        } else {
                            self.flash(format!("Child transcript unavailable for {id}"));
                        }
                        return (Dirty::YES, vec![]);
                    }
                    if let Some(id) = self.chats[self.active_chat].post_agent_at(row, area) {
                        let mut matches = self
                            .chats
                            .iter()
                            .enumerate()
                            .filter(|(_, chat)| chat.subagent_id.as_deref() == Some(id));
                        let target = matches.next().map(|(index, _)| index);
                        if let Some(index) = target.filter(|_| matches.next().is_none()) {
                            self.active_chat = index;
                        } else {
                            self.flash(FLASH_NO_AGENT_CHAT.into());
                        }
                    } else {
                        self.chats[self.active_chat].handle_click(row, area);
                    }
                }
            }
        }
        (Dirty::NO, vec![])
    }

    fn result_action_contains(
        &self,
        row: u16,
        col: u16,
        area: Rect,
        id: makima_agent::AgentId,
    ) -> bool {
        let width = area.width;
        if col < area.x || col >= area.x.saturating_add(width) {
            return false;
        }
        let chat = &self.chats[self.active_chat];
        let Some(message) = chat.message_at_row(row, area) else {
            return false;
        };
        let text = if message.result_expanded {
            &message.text
        } else {
            message.result_preview.as_ref().unwrap_or(&message.text)
        };
        let Some(action) = text.lines().last() else {
            return false;
        };
        let mut first_row = row;
        while first_row > area.y && chat.result_navigation_at(first_row - 1, area) == Some(id) {
            first_row -= 1;
        }
        let mut last_row = row;
        while last_row + 1 < area.bottom()
            && chat.result_navigation_at(last_row + 1, area) == Some(id)
        {
            last_row += 1;
        }
        let line = ratatui::text::Line::raw(action);
        let height = crate::wrap::total_rows(std::slice::from_ref(&line), width);
        let wrapped_row = if last_row + 1 < area.bottom() {
            height.saturating_sub(1 + last_row - row)
        } else {
            row - first_row
        };
        result_action_hit(action, width, wrapped_row, col - area.x)
    }

    fn hover_update(&mut self, pos: Position) -> Dirty {
        let target = self.hits.target_at(pos);
        if target == self.hover {
            Dirty::NO
        } else {
            self.hover = target;
            Dirty::YES
        }
    }

    fn dismiss_surface(&mut self, surface: Surface) -> Vec<Action> {
        let key = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
        match surface {
            Surface::Picker(picker) => self.handle_picker_key(picker, key),
            Surface::Help => {
                self.help_modal.handle_key(key);
                vec![]
            }
            Surface::Btw => {
                self.btw_modal.handle_key(key);
                vec![]
            }
            _ => vec![],
        }
    }

    fn start_scroll_drag(&mut self, owner: ScrollOwner, pos: Position) {
        let Some(rect) = self.hits.rect_of(ClickTarget::Scrollbar(owner)) else {
            return;
        };
        let Some(content_len) = self.scroll_content_len(owner) else {
            return;
        };
        let current = self.scroll_line(owner);
        let y = pos.y.saturating_sub(rect.y);
        let (thumb_len, thumb_start) =
            crate::components::scrollbar::thumb(rect.height, content_len, current);
        let on_thumb = (thumb_start..thumb_start + thumb_len).contains(&y);
        let grab = crate::components::scrollbar::grab_at(rect.height, content_len, current, y);
        let line = if on_thumb {
            current
        } else {
            crate::components::scrollbar::position_at(rect.height, content_len, grab, y, current)
        };
        self.selection_state = None;
        self.scroll_drag = Some(ScrollDrag {
            owner,
            area: rect,
            content_len,
            grab,
            chat: self.resolve_render_chat(),
        });
        self.scroll_owner_to(owner, line);
    }

    fn apply_scroll_drag(&mut self, row: u16) {
        let dirty = self.validate_scroll_drag();
        self.pending_dirty |= dirty;
        let Some(drag) = self.scroll_drag.as_ref() else {
            return;
        };
        let y = row
            .saturating_sub(drag.area.y)
            .min(drag.area.height.saturating_sub(1));
        let line = crate::components::scrollbar::position_at(
            drag.area.height,
            drag.content_len,
            drag.grab,
            y,
            self.scroll_line(drag.owner),
        );
        let owner = drag.owner;
        self.scroll_owner_to(owner, line);
    }

    pub(super) fn validate_scroll_drag(&mut self) -> Dirty {
        let invalid = self.scroll_drag.as_ref().is_some_and(|drag| {
            drag.chat != self.resolve_render_chat()
                || self
                    .hits
                    .rect_of(ClickTarget::Scrollbar(drag.owner))
                    .is_none_or(|rect| rect != drag.area)
                || self.scroll_content_len(drag.owner) != Some(drag.content_len)
                || (drag.area.y..drag.area.bottom()).any(|row| {
                    self.hits.target_at(Position::new(drag.area.x, row))
                        != Some(ClickTarget::Scrollbar(drag.owner))
                })
        });
        if invalid {
            self.scroll_drag = None;
            Dirty::YES
        } else {
            Dirty::NO
        }
    }

    pub(super) fn handle_scroll(&mut self, column: u16, row: u16, delta: i32) {
        let drag_zone = match self.selection_state {
            Some(SelectionState::Dragging { ref sel, .. }) => Some(sel.zone),
            _ => None,
        };
        match self.scroll_at(column, row, delta) {
            Some(zone) if drag_zone == Some(zone) => {
                let scroll = self.scroll_offset(zone);
                if let Some(SelectionState::Dragging { sel, .. }) = &mut self.selection_state {
                    sel.update(row, column, scroll);
                }
            }
            _ => self.clear_selection_unless_pending_copy(),
        }
    }

    fn handle_drag(&mut self, row: u16, col: u16) {
        let (zone, area) = match self.selection_state {
            Some(SelectionState::Dragging {
                ref sel,
                ref mut last_drag_col,
                ..
            }) => {
                *last_drag_col = col;
                (sel.zone, sel.area)
            }
            _ => return,
        };

        let at_top = row <= area.y;
        let at_bottom = row + 1 >= area.bottom();

        if at_top || at_bottom {
            let dir = if at_top {
                EDGE_SCROLL_LINES
            } else {
                -EDGE_SCROLL_LINES
            };
            let first_edge_hit = if let Some(SelectionState::Dragging { edge_scroll, .. }) =
                &mut self.selection_state
            {
                let first = edge_scroll.is_none();
                match edge_scroll {
                    Some(es) => es.dir = dir,
                    None => {
                        *edge_scroll = Some(EdgeScroll {
                            dir,
                            last_tick: Instant::now(),
                        });
                    }
                }
                first
            } else {
                false
            };
            if first_edge_hit {
                self.scroll_zone(zone, dir);
            }
            self.update_selection_to_edge(zone, col);
        } else {
            if let Some(SelectionState::Dragging { edge_scroll, .. }) = &mut self.selection_state {
                *edge_scroll = None;
            }
            let scroll = self.scroll_offset(zone);
            if let Some(SelectionState::Dragging { sel, .. }) = &mut self.selection_state {
                sel.update(row, col, scroll);
            }
        }
    }

    fn update_selection_to_edge(&mut self, zone: SelectionZone, col: u16) {
        let scroll = self.scroll_offset(zone);
        let Some(SelectionState::Dragging {
            ref mut sel,
            ref edge_scroll,
            ..
        }) = self.selection_state
        else {
            return;
        };
        let edge_row = if edge_scroll.as_ref().is_some_and(|es| es.dir > 0) {
            sel.area.y
        } else {
            sel.area.bottom().saturating_sub(1)
        };
        sel.update(edge_row, col, scroll);
    }

    pub fn tick_edge_scroll(&mut self) -> Dirty {
        let (dir, zone, col) = match self.selection_state {
            Some(SelectionState::Dragging {
                ref sel,
                ref mut edge_scroll,
                last_drag_col,
            }) => {
                let Some(es) = edge_scroll else {
                    return Dirty::NO;
                };
                if es.last_tick.elapsed() < EDGE_SCROLL_INTERVAL {
                    return Dirty::NO;
                }
                let dir = es.dir;
                es.last_tick = Instant::now();
                (dir, sel.zone, last_drag_col)
            }
            _ => return Dirty::NO,
        };

        self.scroll_zone(zone, dir);
        self.update_selection_to_edge(zone, col);
        Dirty::YES
    }

    pub(super) fn copy_selection(
        &mut self,
        buf: &mut ratatui::buffer::Buffer,
        sel: &Selection,
        render_chat: usize,
    ) {
        let text = match sel.zone {
            SelectionZone::Messages => {
                let msg_area = self.msg_area();
                self.chats[render_chat].extract_selection_text(sel, msg_area)
            }
            SelectionZone::Input => {
                let scroll = self.scroll_offset(sel.zone);
                let Some(screen_sel) = sel.to_screen(scroll) else {
                    self.selection_state = None;
                    return;
                };
                let copy_text = self.input_box.copy_text();
                let input_area = sel.area;
                let line_breaks = self.input_box.line_breaks(input_area.width);
                let regions = [ContentRegion {
                    area: input_area,
                    raw_text: &copy_text,
                    line_breaks,
                }];
                selection::extract_selected_text(buf, &screen_sel, &regions)
            }
            SelectionZone::Overlay => {
                let scroll = self.scroll_offset(sel.zone);
                let Some(screen_sel) = sel.to_screen(scroll) else {
                    self.selection_state = None;
                    return;
                };
                let regions = [ContentRegion {
                    area: sel.area,
                    ..Default::default()
                }];
                selection::extract_selected_text(buf, &screen_sel, &regions)
            }
        };

        match self.clipboard.copy_text(&text) {
            Ok(CopyResult::Noop) => {}
            Ok(CopyResult::Copied) => self.status_bar.flash("Copied selection".into()),
            Err(e) => self.status_bar.flash(format!("Copy failed: {e}")),
        }
        self.selection_state = None;
    }

    pub(super) fn zone_at(&self, row: u16, col: u16) -> Option<selection::SelectableZone> {
        self.zones.zone_at(row, col)
    }

    pub(super) fn scroll_offset(&self, zone: SelectionZone) -> u32 {
        match zone {
            SelectionZone::Messages => self.chats[self.active_chat].scroll_top() as u32,
            SelectionZone::Input => self.input_box.scroll_y() as u32,
            SelectionZone::Overlay => 0,
        }
    }

    pub(super) fn scroll_zone(&mut self, zone: SelectionZone, delta: i32) {
        match zone {
            SelectionZone::Messages => self.chats[self.active_chat].scroll(delta),
            SelectionZone::Input => self.input_box.scroll(delta),
            SelectionZone::Overlay => {}
        }
    }

    pub(super) fn msg_area(&self) -> Rect {
        self.zones
            .find(SelectionZone::Messages)
            .map(|z| {
                let a = z.area;
                Rect::new(a.x, a.y, a.width.saturating_sub(1), a.height)
            })
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod result_action_tests {
    use super::result_action_hit;
    use test_case::test_case;

    #[test_case(80, 0, 0, true; "first_column")]
    #[test_case(80, 0, 22, true; "last_column")]
    #[test_case(80, 0, 23, false; "blank_after_action")]
    #[test_case(10, 2, 9, true; "wrapped_text")]
    fn action_bounds(width: u16, row: u16, col: u16, expected: bool) {
        assert_eq!(
            result_action_hit("[View child transcript]", width, row, col),
            expected
        );
    }
}
