use std::mem;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use maki_commands::{
    CommandRegistry, CompletionCandidate, CompletionItem, CompletionResult, CompletionSession,
    RegistrySnapshot, ResolvedCommand, SlashClass, TargetHandle, classify_input,
};
use maki_match::{CompletionMatchOptions, completion_match};
use nucleo::pattern::{CaseMatching, Normalization};
use nucleo::{Config, Nucleo, Utf32String};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::{repaint::Dirty, theme};

const TICK_TIMEOUT_MS: u64 = 10;
/// Note appended to builtin alias rows: `(Alias for /new)`.
const ALIAS_NOTE: &str = " (Alias for ";

#[cfg(test)]
pub struct ParsedCommand {
    pub name: String,
    pub args: String,
}

pub struct ConfirmedCommand {
    pub command: ResolvedCommand,
    pub args: String,
}

pub enum CommandAction {
    Consumed,
    SelectionChanged,
    Execute(ConfirmedCommand),
    AcceptArgument { text: String, cursor: usize },
    Complete { text: String, cursor: usize },
    Passthrough,
}

struct CommandItem {
    command: ResolvedCommand,
    source_order: usize,
}

struct Match {
    command: ResolvedCommand,
    indices: Vec<u32>,
}

pub struct CommandPalette {
    command_selected: usize,
    command_query: String,
    argument_selected: usize,
    argument_scroll_offset: usize,
    filtered: Vec<Match>,
    registry: CommandRegistry,
    target: TargetHandle,
    snapshot: RegistrySnapshot,
    nucleo: Nucleo<CommandItem>,
    current_arg_count: usize,
    argument_items: Vec<ArgumentMatch>,
    argument_range: Option<(usize, usize)>,
    argument_generation: u64,
    completion_session: Option<CompletionSession>,
    pending_arguments: Option<PendingArguments>,
    accepted_argument_input: Option<String>,
}

struct ArgumentMatch {
    candidate: Option<CompletionCandidate>,
    item: CompletionItem,
    indices: Vec<u32>,
    ranking: maki_match::CompletionRanking,
    order: usize,
}

#[derive(Clone, Copy)]
enum PaletteLifecycle {
    Highlight,
    Accept,
    Cancel,
}

struct PendingArguments {
    rx: flume::Receiver<CompletionResult>,
    generation: u64,
    query: String,
    range: (usize, usize),
}

impl CommandPalette {
    pub fn new(registry: CommandRegistry, target: TargetHandle) -> Self {
        let snapshot = registry
            .snapshot_for(&target)
            .expect("new command target is live");
        let nucleo = Self::build_nucleo(&snapshot);
        Self {
            command_selected: 0,
            command_query: String::new(),
            argument_selected: 0,
            argument_scroll_offset: 0,
            filtered: Vec::new(),
            registry,
            target,
            snapshot,
            nucleo,
            current_arg_count: 0,
            argument_items: Vec::new(),
            argument_range: None,
            argument_generation: 0,
            completion_session: None,
            pending_arguments: None,
            accepted_argument_input: None,
        }
    }

    fn build_nucleo(snapshot: &RegistrySnapshot) -> Nucleo<CommandItem> {
        let nucleo = Nucleo::new(Config::DEFAULT, Arc::new(|| {}), None, 1);
        let injector = nucleo.injector();
        for (source_order, command) in snapshot.commands().iter().enumerate() {
            injector.push(
                CommandItem {
                    command: command.clone(),
                    source_order,
                },
                |item, cols| {
                    cols[0] = Utf32String::from(item.command.invoked_name());
                },
            );
        }
        nucleo
    }

    pub fn handle_key(&mut self, key: KeyEvent, input: &str) -> CommandAction {
        if self.accepted_argument_input.as_deref() == Some(input) {
            return if key.code == KeyCode::Enter {
                self.confirm_close(input)
            } else {
                CommandAction::Passthrough
            };
        }
        if !self.is_active() {
            return CommandAction::Passthrough;
        }
        match key.code {
            KeyCode::Up => {
                if !self.argument_items.is_empty() {
                    self.argument_selected = if self.argument_selected == 0 {
                        self.argument_items.len() - 1
                    } else {
                        self.argument_selected - 1
                    };
                    self.notify_lifecycle(PaletteLifecycle::Highlight);
                    CommandAction::Consumed
                } else {
                    self.move_up();
                    CommandAction::SelectionChanged
                }
            }
            KeyCode::Down => {
                if !self.argument_items.is_empty() {
                    self.argument_selected =
                        if self.argument_selected == self.argument_items.len() - 1 {
                            0
                        } else {
                            self.argument_selected + 1
                        };
                    self.notify_lifecycle(PaletteLifecycle::Highlight);
                    CommandAction::Consumed
                } else {
                    self.move_down();
                    CommandAction::SelectionChanged
                }
            }
            KeyCode::Esc => {
                self.close();
                CommandAction::Consumed
            }
            KeyCode::Enter => {
                if let Some((range, item)) = self
                    .argument_range
                    .zip(self.argument_items.get(self.argument_selected))
                {
                    let insertion = &item.item.insertion;
                    let text = self.replace_argument(input, insertion);
                    let cursor = range.0 + insertion.len();
                    // The token already is the selected item, so accepting it
                    // would change nothing: run the command instead of
                    // spending an Enter on a no-op accept.
                    let exact = input.get(range.0..range.1) == Some(insertion.as_ref());
                    self.notify_lifecycle(PaletteLifecycle::Accept);
                    self.reset_argument_state();
                    if exact {
                        self.confirm_close(input)
                    } else {
                        self.accepted_argument_input = Some(text.clone());
                        CommandAction::AcceptArgument { text, cursor }
                    }
                } else {
                    // Not settled: no items, or stale ones still on screen
                    // while the current query is in flight.
                    self.confirm_close(input)
                }
            }
            KeyCode::Tab => {
                if let Some((range, item)) = self
                    .argument_range
                    .zip(self.argument_items.get(self.argument_selected))
                {
                    let insertion = item.item.insertion.clone();
                    let text = self.replace_argument(input, &insertion);
                    self.notify_lifecycle(PaletteLifecycle::Accept);
                    self.reset_argument_state();
                    self.accepted_argument_input = Some(text.clone());
                    return CommandAction::Complete {
                        text,
                        cursor: range.0 + insertion.len(),
                    };
                }
                if let Some(item) = self.filtered.get(self.command_selected) {
                    let name = item.command.invoked_name().to_string();
                    let text = if self.item_has_args(item) {
                        format!("{name} ")
                    } else {
                        name
                    };
                    CommandAction::Complete {
                        cursor: text.len(),
                        text,
                    }
                } else {
                    CommandAction::Consumed
                }
            }
            _ => CommandAction::Passthrough,
        }
    }

    pub fn is_active(&self) -> bool {
        self.accepted_argument_input.is_none()
            && (!self.filtered.is_empty()
                || !self.argument_items.is_empty()
                || self.completion_session.is_some())
    }

    #[cfg(test)]
    pub(crate) fn argument_generation(&self) -> u64 {
        self.argument_generation
    }

    #[cfg(test)]
    pub(crate) fn completion_session_id(&self) -> Option<maki_commands::CompletionSessionId> {
        self.completion_session.as_ref().map(CompletionSession::id)
    }

    #[cfg(test)]
    pub(crate) fn argument_selected_for_test(&self) -> usize {
        self.argument_selected
    }

    #[cfg(test)]
    pub(crate) fn set_argument_completion(
        &mut self,
        range: (usize, usize),
        item: maki_lua::CommandArgumentItem,
    ) {
        self.set_argument_completions(range, vec![item]);
    }

    #[cfg(test)]
    pub(crate) fn set_argument_completions(
        &mut self,
        range: (usize, usize),
        items: Vec<maki_lua::CommandArgumentItem>,
    ) {
        self.argument_range = Some(range);
        self.argument_items = items
            .into_iter()
            .enumerate()
            .map(|(order, item)| {
                let ranking = completion_match(
                    "",
                    &item.label,
                    CompletionMatchOptions {
                        case_matching: CaseMatching::Ignore,
                        normalization: Normalization::Smart,
                    },
                )
                .unwrap()
                .ranking;
                ArgumentMatch {
                    candidate: None,
                    item: CompletionItem {
                        label: Arc::from(item.label),
                        insertion: Arc::from(item.insertion),
                        description: item.description.map(Arc::from),
                    },
                    indices: Vec::new(),
                    ranking,
                    order,
                }
            })
            .collect();
        self.argument_selected = 0;
        self.argument_scroll_offset = 0;
    }

    pub fn sync_arguments(&mut self, input: &str, cursor: usize, mode: &str) -> bool {
        self.argument_generation = self.argument_generation.wrapping_add(1);
        self.reset_argument_state();
        if self.accepted_argument_input.as_deref() == Some(input) {
            return false;
        }
        let abandoned = self.accepted_argument_input.take().is_some();
        let Some(command) = self
            .filtered
            .get(self.command_selected)
            .map(|item| item.command.clone())
        else {
            self.cancel_arguments();
            return abandoned;
        };
        let Some((start, end, argument, index)) = argument_at_cursor(input, cursor) else {
            self.cancel_arguments();
            return abandoned;
        };
        let same_session = self.completion_session.as_ref().is_some_and(|session| {
            session.command().command_id() == command.command_id()
                && session.command().invoked_name() == command.invoked_name()
        });
        if !same_session {
            self.cancel_arguments();
            self.completion_session = self
                .registry
                .open_completion(command, self.target.id())
                .ok();
        }
        let Some(session) = self.completion_session.clone() else {
            self.argument_items.clear();
            return abandoned;
        };
        let (tx, rx) = flume::bounded(1);
        let request = session.complete(
            Arc::from(command_args(input)),
            Arc::from(argument.as_str()),
            index,
            Arc::from(mode),
        );
        smol::spawn(async move {
            let _ = tx.send_async(request.await).await;
        })
        .detach();
        self.pending_arguments = Some(PendingArguments {
            rx,
            generation: self.argument_generation,
            query: argument,
            range: (start, end),
        });
        abandoned
    }

    pub fn poll_arguments(&mut self) -> Dirty {
        let Some(pending) = self.pending_arguments.take() else {
            return Dirty::NO;
        };
        let Ok(result) = pending.rx.try_recv() else {
            if !pending.rx.is_disconnected() {
                self.pending_arguments = Some(pending);
            }
            return Dirty::NO;
        };
        if pending.generation != self.argument_generation {
            self.cancel_arguments();
            return Dirty::YES;
        }
        let CompletionResult::Items(items) = result else {
            self.cancel_arguments();
            return Dirty::YES;
        };
        if items.is_empty() {
            self.cancel_arguments();
            return Dirty::YES;
        }
        self.argument_items.clear();
        self.argument_selected = 0;
        self.argument_scroll_offset = 0;
        for (order, candidate) in items.into_iter().enumerate() {
            let item = candidate.item().clone();
            let Some(matched) = completion_match(
                &pending.query,
                &item.label,
                CompletionMatchOptions {
                    case_matching: CaseMatching::Ignore,
                    normalization: Normalization::Smart,
                },
            ) else {
                continue;
            };
            self.argument_items.push(ArgumentMatch {
                candidate: Some(candidate),
                item,
                indices: matched.indices,
                ranking: matched.ranking,
                order,
            });
        }
        self.argument_items.sort_by(|a, b| {
            maki_match::compare_completion_matches(
                &maki_match::CompletionMatch {
                    indices: a.indices.clone(),
                    ranking: a.ranking,
                },
                &maki_match::CompletionMatch {
                    indices: b.indices.clone(),
                    ranking: b.ranking,
                },
                0,
                0,
                a.order,
                b.order,
                &a.item.label,
                &b.item.label,
            )
        });
        if self.argument_items.is_empty() {
            self.cancel_arguments();
            return Dirty::YES;
        }
        self.argument_range = Some(pending.range);
        self.notify_lifecycle(PaletteLifecycle::Highlight);
        Dirty::YES
    }

    fn notify_lifecycle(&mut self, event: PaletteLifecycle) {
        let Some(session) = &self.completion_session else {
            return;
        };
        match event {
            PaletteLifecycle::Highlight => {
                if let Some(candidate) = self
                    .argument_items
                    .get(self.argument_selected)
                    .and_then(|item| item.candidate.as_ref())
                {
                    let _ = session.highlight(candidate);
                }
            }
            PaletteLifecycle::Accept => {
                if let Some(candidate) = self
                    .argument_items
                    .get_mut(self.argument_selected)
                    .and_then(|item| item.candidate.take())
                {
                    let _ = session.accept(candidate);
                }
                self.completion_session = None;
            }
            PaletteLifecycle::Cancel => {
                let _ = session.cancel();
                self.completion_session = None;
            }
        }
    }

    fn reset_argument_state(&mut self) {
        self.argument_items.clear();
        self.argument_range = None;
        self.pending_arguments = None;
        self.argument_selected = 0;
        self.argument_scroll_offset = 0;
    }

    pub fn cancel_arguments(&mut self) {
        self.notify_lifecycle(PaletteLifecycle::Cancel);
        self.reset_argument_state();
    }

    fn replace_argument(&self, input: &str, replacement: &str) -> String {
        let Some((start, end)) = self.argument_range else {
            return input.to_string();
        };
        format!("{}{}{}", &input[..start], replacement, &input[end..])
    }

    pub fn sync(&mut self, input: &str) {
        let Ok(snapshot) = self.registry.snapshot_for(&self.target) else {
            self.close();
            return;
        };
        if snapshot.generation() != self.snapshot.generation() {
            self.snapshot = snapshot;
            self.nucleo = Self::build_nucleo(&self.snapshot);
        }
        let SlashClass::Command(trimmed) = classify_input(input) else {
            self.filtered.clear();
            self.current_arg_count = 0;
            return;
        };
        let stripped = &trimmed[1..]; // trimmed starts with exactly one '/'

        let parts: Vec<&str> = stripped.split_whitespace().collect();
        let cmd_word = parts.first().copied().unwrap_or(stripped);
        let trailing_space = stripped.ends_with(char::is_whitespace);

        self.current_arg_count = if trailing_space {
            parts.len()
        } else {
            parts.len().saturating_sub(1)
        };

        self.nucleo.pattern.reparse(
            0,
            cmd_word,
            CaseMatching::Ignore,
            Normalization::Smart,
            false,
        );

        self.tick(cmd_word);
    }

    fn tick(&mut self, query: &str) {
        loop {
            let status = self.nucleo.tick(TICK_TIMEOUT_MS);
            if status.changed {
                self.refresh_matches(query);
            }
            if !status.running {
                break;
            }
        }
    }

    fn refresh_matches(&mut self, query: &str) {
        let options = CompletionMatchOptions {
            case_matching: CaseMatching::Ignore,
            normalization: Normalization::Smart,
        };
        let mut matches = Vec::new();
        let snapshot = self.nucleo.snapshot();
        let count = snapshot.matched_item_count();
        for item in snapshot.matched_items(0..count) {
            let cmd_item = &item.data;

            if !cmd_item
                .command
                .spec()
                .arguments
                .accepts(self.current_arg_count)
            {
                continue;
            }
            let Some(completion) =
                completion_match(query, cmd_item.command.invoked_name(), options)
            else {
                continue;
            };
            matches.push((cmd_item.source_order, cmd_item.command.clone(), completion));
        }
        matches.sort_by(
            |(left_order, left, left_match), (right_order, right, right_match)| {
                maki_match::compare_completion_matches(
                    left_match,
                    right_match,
                    0,
                    0,
                    *left_order,
                    *right_order,
                    left.invoked_name(),
                    right.invoked_name(),
                )
            },
        );
        self.filtered = matches
            .into_iter()
            .map(|(_, command, completion)| Match {
                command,
                indices: completion.indices,
            })
            .collect();

        if self.command_query != query {
            self.command_selected = 0;
            self.command_query = query.to_owned();
        } else {
            self.command_selected = self
                .command_selected
                .min(self.filtered.len().saturating_sub(1));
        }
        // Argument items survive here: sync_arguments follows every sync
        // and clears them when their session ends.
    }

    pub fn has_accepted_argument(&self) -> bool {
        self.accepted_argument_input.is_some()
    }

    pub fn close(&mut self) {
        self.cancel_arguments();
        self.filtered.clear();
        self.argument_items.clear();
        self.argument_range = None;
        self.pending_arguments = None;
        self.accepted_argument_input = None;
        self.current_arg_count = 0;
    }

    pub fn move_up(&mut self) {
        if self.filtered.is_empty() {
            return;
        }
        self.command_selected = if self.command_selected == 0 {
            self.filtered.len() - 1
        } else {
            self.command_selected - 1
        };
    }

    pub fn move_down(&mut self) {
        if self.filtered.is_empty() {
            return;
        }
        self.command_selected = if self.command_selected == self.filtered.len() - 1 {
            0
        } else {
            self.command_selected + 1
        };
    }

    fn item_has_args(&self, item: &Match) -> bool {
        item.command.spec().arguments.max != Some(0)
    }

    fn item_description<'a>(&self, item: &'a Match) -> &'a str {
        &item.command.spec().docs.summary
    }

    fn alias_target<'a>(&self, item: &'a Match) -> Option<&'a str> {
        (item.command.invoked_name() != item.command.spec().name.as_ref())
            .then_some(item.command.spec().name.as_ref())
    }

    /// Description as rendered, with the alias note appended for alias rows.
    fn rendered_description(&self, m: &Match) -> String {
        let desc = self.item_description(m);
        match self.alias_target(m) {
            Some(target) => format!("{desc}{ALIAS_NOTE}{target})"),
            None => desc.to_string(),
        }
    }

    /// Description column spans; alias rows highlight the target name.
    fn description_spans<'a>(&self, m: &'a Match, desc_style: Style) -> Vec<Span<'a>> {
        let desc = self.item_description(m);
        let Some(target) = self.alias_target(m) else {
            return vec![Span::styled(desc.to_string(), desc_style)];
        };
        let t = theme::current();
        let target_style = desc_style
            .fg(t.accent.fg.unwrap_or_default())
            .add_modifier(Modifier::BOLD);
        vec![
            Span::styled(desc.to_string(), desc_style),
            Span::styled(ALIAS_NOTE, desc_style),
            Span::styled(target, target_style),
            Span::styled(")", desc_style),
        ]
    }

    pub fn confirm(&self, input: &str) -> Option<ConfirmedCommand> {
        let command = self.filtered.get(self.command_selected)?.command.clone();
        Some(ConfirmedCommand {
            command,
            args: command_args(input).trim().to_string(),
        })
    }

    /// Confirm the input as a command and close the palette; consume the key
    /// when the input no longer confirms (the command list went stale).
    fn confirm_close(&mut self, input: &str) -> CommandAction {
        match self.confirm(input) {
            Some(cmd) => {
                self.close();
                CommandAction::Execute(cmd)
            }
            None => CommandAction::Consumed,
        }
    }

    pub fn view(
        &mut self,
        frame: &mut Frame,
        input_area: Rect,
        autocomplete_height: f64,
    ) -> Option<Rect> {
        if !self.is_active() {
            return None;
        }
        let filtered = if self.argument_items.is_empty() {
            &self.filtered
        } else {
            return self.view_arguments(frame, input_area, autocomplete_height);
        };
        if filtered.is_empty() {
            return None;
        }

        let popup_height = (filtered.len() as u16).min(input_area.y);
        if popup_height == 0 {
            return None;
        }

        const GAP: usize = 2;
        let max_name = filtered
            .iter()
            .map(|item| item.command.invoked_name().len())
            .max()
            .unwrap_or(0);
        let max_desc = filtered
            .iter()
            .map(|item| self.rendered_description(item).len())
            .max()
            .unwrap_or(0);
        const PAD: usize = 1;
        let popup_width = (PAD + max_name + GAP + max_desc + PAD) as u16;

        let popup = Rect {
            x: input_area.x,
            y: input_area.y.saturating_sub(popup_height),
            width: popup_width.min(input_area.width),
            height: popup_height,
        };

        let t = theme::current();
        let lines: Vec<Line> = filtered
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let name = m.command.invoked_name().to_string();
                let selected = i == self.command_selected;
                let name_pad = max_name - name.len() + GAP;

                if selected {
                    let s = t.item_selected;
                    let highlighted_name = self.build_highlighted_spans(&name, &m.indices, s);
                    let mut spans = vec![Span::styled(" ".repeat(PAD), s)];
                    spans.extend(highlighted_name);
                    spans.push(Span::styled(" ".repeat(name_pad), s));
                    spans.extend(self.description_spans(m, s));
                    spans.push(Span::styled(" ".repeat(PAD), s));
                    Line::from(spans)
                } else {
                    let highlighted_name = self.build_highlighted_spans(&name, &m.indices, t.item);
                    let mut spans = vec![Span::raw(" ".repeat(PAD))];
                    spans.extend(highlighted_name);
                    spans.push(Span::raw(" ".repeat(name_pad)));
                    spans.extend(self.description_spans(m, t.item_desc));
                    spans.push(Span::raw(" ".repeat(PAD)));
                    Line::from(spans)
                }
            })
            .collect();

        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(lines).style(Style::new().bg(t.background)),
            popup,
        );

        Some(popup)
    }

    fn view_arguments(
        &mut self,
        frame: &mut Frame,
        input_area: Rect,
        autocomplete_height: f64,
    ) -> Option<Rect> {
        if input_area.y == 0 {
            return None;
        }
        let visible_rows = argument_visible_rows(
            self.argument_items.len(),
            input_area.y,
            frame.area().height,
            autocomplete_height,
        );
        if visible_rows == 0 {
            return None;
        }
        self.argument_selected = self
            .argument_selected
            .min(self.argument_items.len().saturating_sub(1));
        self.ensure_argument_visible(visible_rows);
        let height = visible_rows as u16;
        let width = self
            .argument_items
            .iter()
            .map(|m| m.item.label.len() + m.item.description.as_deref().map_or(0, |d| d.len() + 2))
            .max()
            .unwrap_or(0) as u16
            + 2;
        let popup = Rect {
            x: input_area.x,
            y: input_area.y.saturating_sub(height),
            width: width.min(input_area.width),
            height,
        };
        let t = theme::current();
        let lines = self
            .argument_items
            .iter()
            .enumerate()
            .skip(self.argument_scroll_offset)
            .take(visible_rows)
            .map(|(i, m)| {
                let selected = i == self.argument_selected;
                let style = if selected { t.item_selected } else { t.item };
                let desc = m.item.description.as_deref().unwrap_or("");
                let label = self.build_highlighted_spans(&m.item.label, &m.indices, style);
                let mut spans = vec![Span::raw(" ")];
                spans.extend(label);
                spans.push(Span::styled(format!("  {desc}"), style));
                Line::from(spans)
            })
            .collect::<Vec<_>>();
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(lines).style(Style::new().bg(t.background)),
            popup,
        );
        Some(popup)
    }

    fn ensure_argument_visible(&mut self, visible_rows: usize) {
        let len = self.argument_items.len();
        if len == 0 || visible_rows == 0 {
            self.argument_scroll_offset = 0;
            return;
        }
        self.argument_scroll_offset = self
            .argument_scroll_offset
            .min(len.saturating_sub(visible_rows));
        if self.argument_selected < self.argument_scroll_offset {
            self.argument_scroll_offset = self.argument_selected;
        } else if self.argument_selected >= self.argument_scroll_offset + visible_rows {
            self.argument_scroll_offset = self.argument_selected + 1 - visible_rows;
        }
    }

    fn build_highlighted_spans(&self, text: &str, indices: &[u32], base: Style) -> Vec<Span<'_>> {
        if indices.is_empty() {
            return vec![Span::styled(text.to_string(), base)];
        }

        let t = theme::current();
        let highlight = base
            .fg(t.accent.fg.unwrap_or_default())
            .add_modifier(Modifier::BOLD);

        let mut spans = Vec::new();
        let mut in_match = false;
        let mut run = String::new();

        for (i, ch) in text.chars().enumerate() {
            let is_match = indices.binary_search(&(i as u32)).is_ok();
            if is_match != in_match && !run.is_empty() {
                spans.push(Span::styled(
                    mem::take(&mut run),
                    if in_match { highlight } else { base },
                ));
            }
            in_match = is_match;
            run.push(ch);
        }

        if !run.is_empty() {
            spans.push(Span::styled(run, if in_match { highlight } else { base }));
        }

        spans
    }
}

fn argument_visible_rows(
    candidate_count: usize,
    input_y: u16,
    frame_height: u16,
    fraction: f64,
) -> usize {
    if input_y == 0 || candidate_count == 0 || !fraction.is_finite() || fraction <= 0.0 {
        return 0;
    }
    let fraction = fraction.min(1.0);
    let fraction_rows = (f64::from(frame_height) * fraction).floor();
    let fraction_rows = usize::try_from(fraction_rows as u64).unwrap_or(usize::MAX);
    candidate_count
        .min(input_y as usize)
        .min(fraction_rows.max(1))
}

fn command_args(input: &str) -> &str {
    let SlashClass::Command(input) = classify_input(input) else {
        return "";
    };
    let input = &input[1..];
    input
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or("", |(index, ch)| &input[index + ch.len_utf8()..])
}

fn argument_at_cursor(input: &str, cursor: usize) -> Option<(usize, usize, String, usize)> {
    let SlashClass::Command(command_input) = classify_input(input) else {
        return None;
    };
    let offset = input.len() - command_input.len();
    let cursor = cursor.checked_sub(offset)?;
    let slash = &command_input[1..];
    let command_end = slash
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map(|(index, ch)| 1 + index + ch.len_utf8())?;
    if cursor < command_end || !command_input.is_char_boundary(cursor) {
        return None;
    }
    let start = command_input[..cursor]
        .char_indices()
        .rev()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(command_end, |(index, ch)| index + ch.len_utf8());
    let end = command_input[cursor..]
        .char_indices()
        .find(|(_, ch)| ch.is_whitespace())
        .map_or(command_input.len(), |(index, _)| cursor + index);
    let arg = command_input[start..end].to_string();
    let index = command_input[command_end..start].split_whitespace().count();
    Some((offset + start, offset + end, arg, index))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use maki_commands::{
        ArgumentArity, CommandBehavior, CommandDocs, CommandError, CommandFuture,
        CommandInvocation, CommandOutcome, CommandRegistry, CommandSpec, CompletionItem,
        HostResponse, ProducerPrecedence, Registration, TargetCapabilities,
    };
    use maki_config::DEFAULT_AUTOCOMPLETE_HEIGHT;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;

    use super::{
        ArgumentMatch, CaseMatching, CommandPalette, CompletionMatchOptions, Normalization,
        argument_at_cursor, argument_visible_rows, command_args, completion_match,
    };

    struct Noop;

    impl CommandBehavior for Noop {
        fn execute(
            &self,
            _invocation: CommandInvocation,
        ) -> CommandFuture<Result<CommandOutcome, CommandError>> {
            Box::pin(async { Ok(CommandOutcome::Completed) })
        }
    }

    impl maki_commands::CommandHost for Noop {
        fn request(
            &self,
            _request: maki_commands::HostRequest,
        ) -> CommandFuture<Result<HostResponse, CommandError>> {
            Box::pin(async { Ok(HostResponse::Completed) })
        }
    }

    fn registration(name: &str, summary: &str) -> Registration {
        Registration {
            spec: CommandSpec {
                name: Arc::from(name),
                aliases: Arc::from([]),
                arguments: ArgumentArity::bounded(0, 1),
                docs: CommandDocs {
                    summary: Arc::from(summary),
                    argument_hint: None,
                },
                required_capabilities: TargetCapabilities::default(),
            },
            behavior: Arc::new(Noop),
            completion: None,
        }
    }

    #[test]
    fn palette_projects_only_registry_snapshot() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/dynamic", "Dynamic command")])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);

        palette.sync("/");

        assert_eq!(palette.filtered.len(), 1);
        assert_eq!(palette.filtered[0].command.invoked_name(), "/dynamic");
    }

    #[test]
    fn escaped_input_never_matches_commands() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/model", "Switch model")])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);

        palette.sync("/model");
        assert!(palette.is_active());

        palette.sync("//model");
        assert!(!palette.is_active());
        assert!(palette.filtered.is_empty());
    }

    #[test]
    fn confirmation_owns_selected_resolution_across_registry_refresh() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/dynamic", "First")])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        palette.sync("/dynamic arg");
        let confirmed = palette.confirm("  /dynamic arg").unwrap();

        producer
            .replace(vec![registration("/dynamic", "Second")])
            .unwrap();

        assert_eq!(confirmed.command.spec().docs.summary.as_ref(), "First");
        assert_eq!(confirmed.args, "arg");
    }

    #[test]
    fn shared_ranking_orders_matches_over_registration_order() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![
                registration("/remodel", "Remodel"),
                registration("/model", "Model"),
            ])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);

        palette.sync("/mo");

        let names: Vec<&str> = palette
            .filtered
            .iter()
            .map(|item| item.command.invoked_name())
            .collect();
        assert_eq!(names, vec!["/model", "/remodel"]);
    }

    fn argument_palette(count: usize) -> CommandPalette {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/test", "Test")])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        palette.argument_range = Some((6, 7));
        palette.argument_items = (0..count)
            .map(|i| ArgumentMatch {
                candidate: None,
                item: CompletionItem {
                    label: format!("item-{i}").into(),
                    insertion: format!("item-{i}").into(),
                    description: None,
                },
                indices: Vec::new(),
                ranking: completion_match(
                    "",
                    &format!("item-{i}"),
                    CompletionMatchOptions {
                        case_matching: CaseMatching::Ignore,
                        normalization: Normalization::Smart,
                    },
                )
                .unwrap()
                .ranking,
                order: i,
            })
            .collect();
        palette
    }

    fn rendered_rows(
        palette: &mut CommandPalette,
        width: u16,
        height: u16,
        input_y: u16,
    ) -> Vec<String> {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
        terminal
            .draw(|frame| {
                palette.view(
                    frame,
                    Rect::new(0, input_y, width, 1),
                    DEFAULT_AUTOCOMPLETE_HEIGHT,
                );
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer.cell((x, y)).unwrap().symbol().to_string())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn command_name_navigation_still_wraps() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![
                registration("/one", "One"),
                registration("/two", "Two"),
            ])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        palette.sync("/");

        palette.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), "/");
        assert_eq!(palette.command_selected, 1);
        palette.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), "/");
        palette.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), "/");
        assert_eq!(palette.command_selected, 1);
    }

    #[test]
    fn argument_visible_rows_clamps_fraction_and_space() {
        assert_eq!(
            argument_visible_rows(20, 0, 20, DEFAULT_AUTOCOMPLETE_HEIGHT),
            0
        );
        assert_eq!(argument_visible_rows(20, 5, 20, 0.01), 1);
        assert_eq!(argument_visible_rows(20, 5, 20, f64::NAN), 0);
        assert_eq!(argument_visible_rows(20, 5, 20, f64::INFINITY), 0);
        assert_eq!(argument_visible_rows(20, 5, 20, -0.1), 0);
        assert_eq!(argument_visible_rows(20, 5, 20, 2.0), 5);
        assert_eq!(
            argument_visible_rows(3, 20, 20, DEFAULT_AUTOCOMPLETE_HEIGHT),
            3
        );
    }

    #[test]
    fn argument_parser_handles_multibyte_whitespace() {
        let input = "/test\u{3000}alpha\u{3000}beta";

        assert_eq!(command_args(input), "alpha\u{3000}beta");
        assert_eq!(
            argument_at_cursor(input, input.find("alpha").unwrap() + "alpha".len()),
            Some((8, 13, "alpha".into(), 0))
        );
    }

    #[test]
    fn argument_parser_uses_trimmed_command_slice() {
        let input = "  /test alpha";

        assert_eq!(command_args(input), "alpha");
        assert_eq!(
            argument_at_cursor(input, input.len()),
            Some((8, 13, "alpha".into(), 0))
        );
    }

    #[test]
    fn command_palette_argument_navigation_wraps_up_and_down() {
        let mut palette = argument_palette(3);

        palette.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), "/test ");
        assert_eq!(palette.argument_selected, 2);
        palette.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), "/test ");
        assert_eq!(palette.argument_selected, 0);
    }

    #[test]
    fn argument_completion_tab_clears_the_popup() {
        let mut palette = argument_palette(3);

        assert!(matches!(
            palette.handle_key(
                KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE),
                "/test item-0"
            ),
            super::CommandAction::Complete { .. }
        ));
        assert!(palette.argument_items.is_empty());
        assert!(!palette.is_active());
    }

    #[test]
    fn argument_completion_viewport_respects_height_fraction() {
        let mut palette = argument_palette(20);
        let rows = rendered_rows(&mut palette, 30, 20, 7);
        assert_eq!(rows[0].trim(), "item-0");
        assert_eq!(rows[6].trim(), "item-6");
        assert!(
            palette
                .view_in_test(20, 7, DEFAULT_AUTOCOMPLETE_HEIGHT)
                .is_some()
        );
        let mut one = argument_palette(20);
        assert!(one.view_in_test(20, 5, 0.01).is_some());
        assert_eq!(one.argument_scroll_offset, 0);
        assert!(
            one.view_in_test(20, 0, DEFAULT_AUTOCOMPLETE_HEIGHT)
                .is_none()
        );
    }

    #[test]
    fn argument_completion_scroll_follows_selection_and_resize() {
        let mut palette = argument_palette(20);
        let _ = rendered_rows(&mut palette, 30, 20, 7);
        palette.argument_selected = 19;
        let rows = rendered_rows(&mut palette, 30, 20, 7);
        assert_eq!(rows[0].trim(), "item-13");
        assert_eq!(rows[6].trim(), "item-19");

        palette.argument_selected = 0;
        let rows = rendered_rows(&mut palette, 30, 20, 7);
        assert_eq!(rows[0].trim(), "item-0");
        assert_eq!(rows[6].trim(), "item-6");

        let rows = rendered_rows(&mut palette, 30, 8, 4);
        assert_eq!(rows[0].trim(), "item-0");
        assert_eq!(rows[3].trim(), "item-3");
    }

    impl CommandPalette {
        fn view_in_test(&mut self, frame_height: u16, input_y: u16, fraction: f64) -> Option<Rect> {
            let mut terminal = Terminal::new(TestBackend::new(20, frame_height)).unwrap();
            let mut result = None;
            terminal
                .draw(|frame| {
                    result = self.view(frame, Rect::new(0, input_y, 20, 1), fraction);
                })
                .unwrap();
            result
        }
    }
}
