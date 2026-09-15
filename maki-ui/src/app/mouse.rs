use std::time::{Duration, Instant};

use crate::clipboard::CopyResult;
use crate::selection::{self, ContentRegion, EdgeScroll, Selection, SelectionState, SelectionZone};
use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};

use crate::repaint::Dirty;

use super::App;

pub(super) const EDGE_SCROLL_LINES: i32 = 1;
pub(super) const EDGE_SCROLL_INTERVAL: Duration = Duration::from_millis(25);

pub(super) const MIDDLE_SCROLL_INTERVAL: Duration = Duration::from_millis(25);
const MIDDLE_SCROLL_DEAD_ZONE: i32 = 1;
const MIDDLE_SCROLL_RATE: f64 = 2.0;
const MIDDLE_SCROLL_EXPONENT: f64 = 1.5;
const MIDDLE_SCROLL_MAX_RATE: f64 = 120.0;
const MIDDLE_SCROLL_MAX_ELAPSED: Duration = Duration::from_millis(100);
pub(super) const MIDDLE_SCROLL_ANCHOR: &str = "+";

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

    pub(super) fn handle_mouse(&mut self, event: MouseEvent) {
        match event.kind {
            MouseEventKind::Down(MouseButton::Middle) => {
                if self.middle_scroll.is_some() {
                    let _ = self.cancel_middle_scroll();
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
                }
                return;
            }
            MouseEventKind::Moved | MouseEventKind::Drag(MouseButton::Middle) => {
                let now = Instant::now();
                let _ = self.tick_middle_scroll_at(now);
                if let Some(state) = self.middle_scroll.as_mut() {
                    state.move_to(event.row, now);
                }
                return;
            }
            MouseEventKind::Down(MouseButton::Left | MouseButton::Right)
            | MouseEventKind::ScrollUp
            | MouseEventKind::ScrollDown
            | MouseEventKind::ScrollLeft
            | MouseEventKind::ScrollRight => {
                let _ = self.cancel_middle_scroll();
            }
            _ => {}
        }
        match event.kind {
            MouseEventKind::Down(MouseButton::Left) => {
                if let Some(zone) = self.zone_at(event.row, event.column) {
                    if self.has_modal_overlay() && zone.zone != SelectionZone::Overlay {
                        return;
                    }
                    // Move the cursor to the click position in the input area.
                    if zone.zone == SelectionZone::Input {
                        let focused = !self.any_overlay_open();
                        self.input_box
                            .handle_click(zone.area, event.row, event.column, focused);
                        if focused && self.is_main_chat() {
                            let input = self.input_box.buffer.value();
                            self.sync_command_arguments(
                                &input,
                                self.input_box.buffer.cursor_byte_offset(),
                            );
                            self.sync_file_completion();
                        }
                    }
                    let scroll = self.scroll_offset(zone.zone);
                    self.selection_state = Some(SelectionState::Dragging {
                        sel: Selection::start(
                            event.row,
                            event.column,
                            zone.area,
                            zone.zone,
                            scroll,
                        ),
                        edge_scroll: None,
                        last_drag_col: event.column,
                    });
                }
            }
            MouseEventKind::Drag(MouseButton::Left) => {
                self.handle_drag(event.row, event.column);
            }
            MouseEventKind::Up(MouseButton::Left) => {
                if let Some(SelectionState::Dragging { sel, .. }) = self.selection_state {
                    if !sel.is_empty() {
                        self.selection_state = Some(SelectionState::PendingCopy { sel });
                    } else {
                        let zone = sel.zone;
                        self.selection_state = None;
                        if zone == SelectionZone::Messages {
                            let area = self.msg_area();
                            self.chats[self.active_chat].handle_click(event.row, area);
                        }
                    }
                }
            }
            _ => {}
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
