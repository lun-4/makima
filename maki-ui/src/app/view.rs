use std::sync::atomic::Ordering;

use crate::components::Overlay;
use crate::components::input::Placeholder;
#[cfg(test)]
use crate::components::keybindings::KeybindContext;
use crate::components::keybindings::key;
use crate::components::queue_panel;
use crate::components::split_layout::{MIN_CHAT_ROWS, SplitLayout, carve};
use crate::components::status_bar::{self, StatusBarContext, UsageStats};
use crate::components::usage_modal::{UsageFetchState, UsageModalContext, compact_usage_line};
use crate::selection::{self, SelectableZone, SelectionZone, ZoneRegistry};
use crate::theme;
use maki_lua::Split;
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Widget};

use super::{App, Mode, Status};

const SUBAGENT_INPUT_HINT: &str = "sends to this subagent \u{b7} TAB mode \u{b7} ESC cancel";

/// Target hint shown under the input box so a user typing on a subagent chat
/// knows their Enter goes to that subagent, not the main agent.
fn subagent_input_hint(is_subagent: bool) -> Option<Line<'static>> {
    is_subagent.then(|| Line::from(SUBAGENT_INPUT_HINT))
}

struct ViewLayout {
    msg_area: Rect,
    bottom_area: Rect,
    status_area: Rect,
    defer_hint_area: Rect,
    queue_area: Rect,
    panel_windows: Vec<(usize, Rect)>,
    input_area: Rect,
    splits: SplitLayout,
    bottom_takeover: bool,
    top_bar_area: Rect,
}

impl App {
    pub fn view(&mut self, frame: &mut Frame) {
        let form_visible = self.permission_active() || self.plan_form_active();
        let layout = self.compute_layout(frame.area(), form_visible);
        let render_chat = self.resolve_render_chat();

        self.render_background(frame);
        self.render_top_bar(frame, &layout);
        self.render_messages(frame, &layout, render_chat);
        self.render_bottom_panel(frame, &layout);
        self.render_splits(frame, &layout);
        let mut overlay_rect = self.render_picker_overlays(frame, &layout);
        self.render_status_bar(frame, layout.status_area, render_chat);
        self.render_defer_hint(frame, layout.defer_hint_area);
        overlay_rect = self.render_top_modals(frame, overlay_rect);
        self.register_zones(&layout, overlay_rect);
        self.apply_selection(frame, render_chat);
        self.render_active_input(frame, &layout);
    }

    fn compute_layout(&self, area: Rect, form_visible: bool) -> ViewLayout {
        let permission_open = self.permission_active();

        // Carve the full-width status bar first so the split carving below only
        // ever deals with the content region above it. A manually deferred
        // (Alt+M) input demand pins an undefer hint on the row above it.
        let defer_hint_h = u16::from(self.held_input_pending());
        let [mut content, defer_hint_area, status_area] = Layout::vertical([
            Constraint::Min(1),
            Constraint::Length(defer_hint_h),
            Constraint::Length(1),
        ])
        .areas(area);

        // A persistent one-line info bar pinned to the top: the active chat
        // badge, a running-subagent hint, and the cwd:branch. It never crowds
        // the chat below the minimum height.
        let top_bar_h = 1u16.min(content.height.saturating_sub(MIN_CHAT_ROWS));
        let [top_bar_area, rest] =
            Layout::vertical([Constraint::Length(top_bar_h), Constraint::Min(1)]).areas(content);
        content = rest;

        // The active permission prompt owns the bottom area, and a deferred
        // (queued) below-input split reserves nothing, so drop `below` splits
        // for either case at the source.
        let reqs: Vec<_> = self
            .float_mgr
            .split_reqs(content)
            .into_iter()
            .filter(|r| {
                !(r.split == Split::Below && (permission_open || self.below_input_hidden()))
            })
            .collect();
        let splits = carve(content, &reqs);
        let inner = splits.inner;

        let below_active = splits.rect(Split::Below).is_some();
        let bottom_takeover = form_visible || below_active;
        let max_bottom = inner.height.saturating_sub(MIN_CHAT_ROWS);
        let bottom_height = if permission_open {
            self.permission_prompt.height(inner.width).min(max_bottom)
        } else if below_active {
            0
        } else if form_visible {
            self.plan_form.height().min(max_bottom)
        } else if self.is_main_chat() {
            let panel_h: u16 = self.float_mgr.panel_reqs().iter().map(|(_, h)| *h).sum();
            queue_panel::height(self.queue.panel_len())
                + panel_h
                + self.input_box.height(inner.width).min(max_bottom)
        } else {
            // Subagent tab: float panels (if any), a separator, then the input
            // box so the user can send a message to that subagent.
            let panel_h: u16 = self.float_mgr.panel_reqs().iter().map(|(_, h)| *h).sum();
            let sep: u16 = if panel_h > 0 { 1 } else { 0 };
            panel_h + sep + self.input_box.height(inner.width).min(max_bottom)
        };

        // The `below` split lives outside `inner` (drawn by render_splits), so
        // the bottom panel only ever splits the chat region.
        let [msg_area, bottom_area] =
            Layout::vertical([Constraint::Min(1), Constraint::Length(bottom_height)]).areas(inner);

        let panel_reqs = if bottom_takeover {
            Vec::new()
        } else {
            self.float_mgr.panel_reqs()
        };

        let queue_height = if bottom_takeover {
            0
        } else {
            queue_panel::height(self.queue.panel_len())
        };

        let mut constraints = vec![Constraint::Length(queue_height)];
        for &(_, h) in &panel_reqs {
            constraints.push(Constraint::Length(h));
        }
        constraints.push(Constraint::Min(1));

        let areas = Layout::vertical(constraints).split(bottom_area);
        let queue_area = areas[0];
        let panel_windows: Vec<(usize, Rect)> = panel_reqs
            .iter()
            .enumerate()
            .map(|(i, &(idx, _))| (idx, areas[1 + i]))
            .collect();
        let input_area = areas[areas.len() - 1];

        ViewLayout {
            msg_area,
            bottom_area,
            status_area,
            defer_hint_area,
            queue_area,
            panel_windows,
            input_area,
            splits,
            bottom_takeover,
            top_bar_area,
        }
    }

    fn resolve_render_chat(&self) -> usize {
        if self.task_picker.is_open() {
            self.task_picker
                .selected_index()
                .unwrap_or(self.active_chat)
        } else {
            self.active_chat
        }
    }

    fn render_background(&self, frame: &mut Frame) {
        let bg =
            Block::default().style(ratatui::style::Style::new().bg(theme::current().background));
        bg.render(frame.area(), frame.buffer_mut());
    }

    fn render_top_bar(&self, frame: &mut Frame, layout: &ViewLayout) {
        let area = layout.top_bar_area;
        if area.height == 0 {
            return;
        }
        let t = theme::current();
        let active = &self.chats[self.active_chat];
        let mut left_spans: Vec<Span> = Vec::new();
        if active.subagent_id.is_some() {
            left_spans.push(Span::styled(
                status_bar::subagent_label(&active.name),
                t.accent,
            ));
        } else {
            left_spans.push(Span::styled(format!(" [{}]", active.name), t.status_dim));
        }
        // Always surface the running-task hint so tasks are discoverable from
        // any view, even when none are running yet.
        let n_running = self
            .chats
            .iter()
            .skip(1)
            .filter(|c| !c.is_finished())
            .count();
        let task_hint = if n_running > 0 {
            format!(" ({} to see {n_running} tasks)", key::TASKS.label)
        } else {
            format!(" ({} to see tasks)", key::TASKS.label)
        };
        left_spans.push(Span::styled(task_hint, t.item_desc));

        let left_width: usize = left_spans.iter().map(Span::width).sum();
        let cwd_max = (area.width as usize).saturating_sub(left_width + 1);
        let cwd = status_bar::truncate_tail(self.status_bar.cwd_branch(), cwd_max);
        let right_span = Span::styled(cwd.into_owned(), t.status_dim);

        let [left_area, right_area] = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(right_span.width() as u16),
        ])
        .areas(area);
        frame.render_widget(Paragraph::new(Line::from(left_spans)), left_area);
        frame.render_widget(
            Paragraph::new(Line::from(right_span)).alignment(Alignment::Right),
            right_area,
        );
    }

    fn render_messages(&mut self, frame: &mut Frame, layout: &ViewLayout, render_chat: usize) {
        let accent = self.effective_mode_color();
        self.chats[render_chat].set_accent(accent);
        self.chats[render_chat].view(frame, layout.msg_area, self.selection_state.is_some());
    }

    fn render_bottom_panel(&mut self, frame: &mut Frame, layout: &ViewLayout) {
        if self.permission_active() {
            // The active permission form is painted by the topmost pass
            // (`render_active_input`) so it lands above pickers/modals.
        } else if !self.is_main_chat() {
            let panel_reqs = self.float_mgr.panel_reqs();
            let panel_h: u16 = panel_reqs.iter().map(|(_, h)| *h).sum();
            let (panel_areas, sep_area) = if panel_h > 0 {
                let [panels, s] = Layout::vertical([Constraint::Min(0), Constraint::Length(1)])
                    .areas(layout.bottom_area);
                let constraints: Vec<_> = panel_reqs
                    .iter()
                    .map(|&(_, h)| Constraint::Length(h))
                    .collect();
                let sub = Layout::vertical(constraints).split(panels);
                let areas: Vec<(usize, Rect)> = panel_reqs
                    .iter()
                    .enumerate()
                    .map(|(i, &(idx, _))| (idx, sub[i]))
                    .collect();
                (Some(areas), s)
            } else {
                (None, layout.bottom_area)
            };
            if let Some(areas) = panel_areas {
                for (idx, rect) in areas {
                    self.float_mgr.view_panel(frame, idx, rect);
                }
            }
            if panel_h > 0 {
                let sep = Block::default()
                    .borders(Borders::TOP)
                    .border_style(self.separator_style());
                frame.render_widget(sep, sep_area);
            }
            if layout.input_area.height > 0 {
                self.input_box.view(
                    frame,
                    layout.input_area,
                    Placeholder::Blank,
                    self.separator_style(),
                    !self.any_overlay_open(),
                    subagent_input_hint(true),
                    &self.state.session.cwd,
                );
            }
        } else if self.plan_form_active() {
            self.plan_form.view(frame, layout.bottom_area);
        } else if layout.bottom_area.height > 0 {
            let queue_entries = self.queue.panel_entries();
            queue_panel::view(frame, layout.queue_area, &queue_entries, self.queue.focus());
            for &(idx, rect) in &layout.panel_windows {
                self.float_mgr.view_panel(frame, idx, rect);
            }
            let streaming = self.status == Status::Streaming;
            let placeholder = if streaming {
                Placeholder::Queue
            } else if self.state.session.messages().is_empty() {
                Placeholder::Suggestion
            } else {
                Placeholder::Blank
            };
            let panel_hint = (self.state.mode == Mode::Plan)
                .then(|| self.plan_form.hint_line())
                .flatten()
                .or_else(|| self.lua_hint_line());
            let panel_hint = self.main_chat().splash_debug_line().or(panel_hint);
            self.input_box.view(
                frame,
                layout.input_area,
                placeholder,
                self.separator_style(),
                !self.any_overlay_open(),
                panel_hint,
                &self.state.session.cwd,
            );
            if !streaming && !self.command_palette.is_active() && !self.any_overlay_open() {
                self.file_completion.view(frame, layout.input_area);
            }
            self.command_palette.view(frame, layout.input_area);
        }
    }

    fn render_splits(&mut self, frame: &mut Frame, layout: &ViewLayout) {
        for dir in Split::ALL {
            // The active question split is painted topmost by `render_active_input`;
            // a queued one is hidden. Either way, skip it here.
            if dir == Split::Below && self.float_mgr.below_is_input() {
                continue;
            }
            if let Some(rect) = layout.splits.rect(dir) {
                self.float_mgr.view_split(frame, dir, rect);
            }
        }
    }

    /// Topmost pass: paints the single active input surface last so it wins
    /// every overlap with pickers/modals.
    fn render_active_input(&mut self, frame: &mut Frame, layout: &ViewLayout) {
        if self.permission_active() {
            // Clear then re-fill the theme background so the prompt occludes any
            // picker/modal drawn beneath it in this rect (the form has no fill).
            frame.render_widget(Clear, layout.bottom_area);
            frame.render_widget(
                Block::default().style(Style::new().bg(theme::current().background)),
                layout.bottom_area,
            );
            self.permission_prompt.view(frame, layout.bottom_area);
        } else if self.question_active()
            && let Some(rect) = layout.splits.rect(Split::Below)
        {
            self.float_mgr.view_split(frame, Split::Below, rect);
        }
    }

    fn render_picker_overlays(&mut self, frame: &mut Frame, layout: &ViewLayout) -> Rect {
        let mut overlay_rect = Rect::default();
        let full = frame.area();

        if self.search_modal.is_open() {
            overlay_rect = self.search_modal.view(frame, layout.msg_area);
        }

        if self.task_picker.is_open() {
            overlay_rect = self.task_picker.view(frame, full);
        }

        if self.lua_picker.is_open() {
            overlay_rect = self.lua_picker.view(frame, full);
        }

        if self.file_picker.is_open() {
            overlay_rect = self.file_picker.view(frame, full);
        }

        macro_rules! render_if_open {
            ($overlay:expr) => {
                if $overlay.is_open() {
                    overlay_rect = $overlay.view(frame, full);
                }
            };
        }

        render_if_open!(self.rewind_picker);
        render_if_open!(self.theme_picker);
        render_if_open!(self.model_picker);
        render_if_open!(self.login_picker);
        render_if_open!(self.mcp_picker);

        overlay_rect
    }

    fn render_top_modals(&mut self, frame: &mut Frame, mut overlay_rect: Rect) -> Rect {
        let full = frame.area();
        let r = self
            .btw_modal
            .view(frame, full, self.theme_provider.generation());
        if r.width > 0 {
            overlay_rect = r;
        }
        let r = self.help_modal.view(frame, full);
        if r.width > 0 {
            overlay_rect = r;
        }
        if self.usage_modal.is_open() {
            let ctx = UsageModalContext {
                total: &self.state.token_usage,
                total_cost: self.state.cost,
                by_model: self.state.session.usage_by_model(),
                model: &self.state.model,
                fast: self.state.fast,
                clock_format: self.ui_config.clock_format,
            };
            let r = self.usage_modal.view(frame, full, &ctx);
            if r.width > 0 {
                overlay_rect = r;
            }
        }
        let r = self.float_mgr.view(frame, full);
        if r.width > 0 {
            overlay_rect = r;
        }
        overlay_rect
    }

    fn render_status_bar(&mut self, frame: &mut Frame, status_area: Rect, render_chat: usize) {
        let chat = &self.chats[render_chat];
        let (mode_label, mode_style) = self.mode_label();
        let ctx = StatusBarContext {
            status: &self.status,
            mode_label,
            mode_style,
            model_id: chat
                .model_id
                .as_deref()
                .unwrap_or(&self.state.session.model),
            stats: UsageStats {
                global_cost: self.state.cost,
                context_size: chat.context_size,
                cost: chat.cost,
                context_window: self.state.model.context_window,
                show_global: self.chats.len() > 1,
            },
            auto_scroll: chat.auto_scroll(),
            retry_info: self.retry_info.as_ref(),
            thinking_label: self.state.thinking.status_label(),
            fast: self.state.fast,
            workflow: self.state.workflow,
            restoring: self.restoring.load(Ordering::Relaxed),
            usage: self.usage_readout(),
        };
        self.status_bar.view(frame, status_area, &ctx);
    }

    /// Left-aligned affordance pinned on the row above the status bar while an
    /// Alt+M-deferred prompt waits: how to bring it back without submitting.
    fn render_defer_hint(&self, frame: &mut Frame, area: Rect) {
        if area.height == 0 {
            return;
        }
        let t = theme::current();
        let line = Line::from(vec![
            Span::styled("(", t.tool_dim),
            Span::styled(key::DEFER_INPUT.label, t.keybind_key),
            Span::styled(" Undefer pending model input)", t.tool_dim),
        ]);
        frame.render_widget(Paragraph::new(line), area);
    }

    fn register_zones(&mut self, layout: &ViewLayout, overlay_rect: Rect) {
        // Push order = z-order. zone_at() walks in reverse, so later entries win.
        self.zones = ZoneRegistry::new();

        self.zones.push(SelectableZone {
            area: layout.msg_area,
            zone: SelectionZone::Messages,
        });

        if layout.input_area.height > 0 && !layout.bottom_takeover && self.is_main_chat() {
            let input_inner = Rect::new(
                layout.input_area.x,
                layout.input_area.y + 1,
                layout.input_area.width,
                layout.input_area.height.saturating_sub(2),
            );
            self.zones.push(SelectableZone {
                area: input_inner,
                zone: SelectionZone::Input,
            });
        }

        self.zones.push_overlay(layout.status_area);

        if self.plan_form_active() {
            self.zones.push_overlay(layout.bottom_area);
        }

        for &(_, rect) in &layout.panel_windows {
            self.zones.push_overlay(selection::inset_border(rect));
        }

        if !self.is_main_chat() && layout.bottom_area.height > 0 {
            self.zones.push_overlay(layout.bottom_area);
        }

        if layout.queue_area.height > 0 && !layout.bottom_takeover {
            self.zones.push_overlay(layout.queue_area);
        }

        for dir in Split::ALL {
            // The active question split is pushed topmost below; a queued one
            // pushes nothing.
            if dir == Split::Below && self.float_mgr.below_is_input() {
                continue;
            }
            if let Some(rect) = layout.splits.rect(dir) {
                self.zones.push_overlay(selection::inset_border(rect));
            }
        }

        if overlay_rect.width > 0 {
            self.zones
                .push_overlay(selection::inset_border(overlay_rect));
        }

        // The active input surface is the single topmost zone so it wins clicks
        // over any picker/modal that overlaps it.
        if self.permission_active() {
            self.zones.push_overlay(layout.bottom_area);
        } else if self.question_active()
            && let Some(rect) = layout.splits.rect(Split::Below)
        {
            self.zones.push_overlay(selection::inset_border(rect));
        }

        // Overlay zone was removed (e.g. dialog closed), drop the dangling selection
        if let Some(ref state) = self.selection_state
            && state.sel().zone == SelectionZone::Overlay
            && self.zones.find_area(state.sel().area).is_none()
        {
            self.selection_state = None;
        }
    }

    fn apply_selection(&mut self, frame: &mut Frame, render_chat: usize) {
        let Some(ref state) = self.selection_state else {
            return;
        };

        let sel = state.sel();
        let scroll = self.scroll_offset(sel.zone);
        if let Some(screen_sel) = sel.to_screen(scroll) {
            selection::apply_highlight(frame.buffer_mut(), sel.highlight_area(), &screen_sel);
        }
        if state.is_pending_copy() {
            let sel = *sel;
            self.copy_selection(frame.buffer_mut(), &sel, render_chat);
        }
    }

    /// Layout geometry for tests: `(msg_area, bottom_area, status_area,
    /// input_area, splits)`.
    #[cfg(test)]
    pub(super) fn layout_geometry(&self, area: Rect) -> (Rect, Rect, Rect, Rect, SplitLayout) {
        let form_visible = self.permission_active() || self.plan_form_active();
        let layout = self.compute_layout(area, form_visible);
        (
            layout.msg_area,
            layout.bottom_area,
            layout.status_area,
            layout.input_area,
            layout.splits,
        )
    }

    #[cfg(test)]
    pub(super) fn top_bar_rect(&self, area: Rect) -> Rect {
        let form_visible = self.permission_active() || self.plan_form_active();
        self.compute_layout(area, form_visible).top_bar_area
    }

    /// Inline quota readout (`5h30% w50%`) drawn in the status bar after the
    /// context length and cost. Only `Ready` states render; providers without
    /// a quota endpoint stay clean.
    pub(super) fn usage_readout(&self) -> Option<Line<'static>> {
        let state = self.usage_slot.load_full()?;
        match &*state {
            UsageFetchState::Ready(usage) if !usage.limits.is_empty() => {
                Some(compact_usage_line(usage))
            }
            _ => None,
        }
    }

    fn lua_hint_line(&self) -> Option<Line<'static>> {
        let snap = self.hints.get()?;
        if snap.entries.is_empty() {
            return None;
        }
        let mut spans = Vec::new();
        for (_, pairs) in &snap.entries {
            for (text, style_name) in pairs {
                let style = theme::style_by_name(style_name);
                spans.push(Span::styled(text.clone(), style));
            }
        }
        Some(Line::from(spans))
    }

    #[cfg(test)]
    pub(super) fn active_keybind_contexts(&self) -> Vec<KeybindContext> {
        let mut contexts = vec![KeybindContext::General];
        if self.plan_form_active() {
            contexts.push(KeybindContext::FormInput);
        } else if self.queue.focus().is_some() {
            contexts.push(KeybindContext::QueueFocus);
        } else if self.rewind_picker.is_open() {
            contexts.push(KeybindContext::RewindPicker);
        } else if self.task_picker.is_open() {
            contexts.push(KeybindContext::TaskPicker);
        } else if self.theme_picker.is_open() {
            contexts.push(KeybindContext::ThemePicker);
        } else if self.model_picker.is_open() {
            contexts.push(KeybindContext::ModelPicker);
        } else if self.command_palette.is_active() {
            contexts.push(KeybindContext::CommandPalette);
        } else if self.search_modal.is_open() {
            contexts.push(KeybindContext::Search);
        } else if self.file_picker.is_open() {
            contexts.push(KeybindContext::FilePicker);
        } else {
            if self.status == Status::Streaming {
                contexts.push(KeybindContext::Streaming);
            }
            contexts.push(KeybindContext::Editing);
        }
        contexts
    }
}

#[cfg(test)]
mod tests {
    use ratatui::text::Line;

    use super::{SUBAGENT_INPUT_HINT, subagent_input_hint};

    #[test]
    fn subagent_input_hint_cases() {
        let hint = subagent_input_hint(true).expect("subagent gets a target hint");
        assert_eq!(hint, Line::from(SUBAGENT_INPUT_HINT));
        assert!(subagent_input_hint(false).is_none());
    }
}
