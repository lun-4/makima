use std::mem;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use maki_commands::{
    CommandRegistry, CompletionCandidate, CompletionItem, CompletionResult, CompletionSession,
    InvocationTargetId, RegistrySnapshot, ResolvedCommand,
};
use nucleo::pattern::{CaseMatching, Normalization};
use nucleo::{Config, Matcher, Nucleo, Utf32String};
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
}

struct Match {
    command: ResolvedCommand,
    indices: Vec<u32>,
}

pub struct CommandPalette {
    selected: usize,
    filtered: Vec<Match>,
    registry: CommandRegistry,
    target: InvocationTargetId,
    snapshot: RegistrySnapshot,
    nucleo: Nucleo<CommandItem>,
    matcher: Matcher,
    current_arg_count: usize,
    argument_items: Vec<ArgumentMatch>,
    argument_range: Option<(usize, usize)>,
    argument_session: u64,
    argument_generation: u64,
    completion_session: Option<CompletionSession>,
    pending_arguments: Option<PendingArguments>,
    accepted_argument_input: Option<String>,
}

struct ArgumentMatch {
    candidate: Option<CompletionCandidate>,
    item: CompletionItem,
    indices: Vec<u32>,
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
    pub fn new(registry: CommandRegistry, target: InvocationTargetId) -> Self {
        let snapshot = registry.snapshot();
        let nucleo = Self::build_nucleo(&snapshot);
        Self {
            selected: 0,
            filtered: Vec::new(),
            registry,
            target,
            snapshot,
            nucleo,
            matcher: Matcher::new(Config::DEFAULT),
            current_arg_count: 0,
            argument_items: Vec::new(),
            argument_range: None,
            argument_session: 0,
            argument_generation: 0,
            completion_session: None,
            pending_arguments: None,
            accepted_argument_input: None,
        }
    }

    fn build_nucleo(snapshot: &RegistrySnapshot) -> Nucleo<CommandItem> {
        let nucleo = Nucleo::new(Config::DEFAULT, Arc::new(|| {}), None, 1);
        let injector = nucleo.injector();
        for command in snapshot.commands() {
            injector.push(
                CommandItem {
                    command: command.clone(),
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
                    self.selected = self.selected.saturating_sub(1);
                    self.notify_lifecycle(PaletteLifecycle::Highlight);
                    CommandAction::Consumed
                } else {
                    self.move_up();
                    CommandAction::SelectionChanged
                }
            }
            KeyCode::Down => {
                if !self.argument_items.is_empty() {
                    self.selected = (self.selected + 1).min(self.argument_items.len() - 1);
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
                    .zip(self.argument_items.get(self.selected))
                {
                    let insertion = &item.item.insertion;
                    let text = self.replace_argument(input, insertion);
                    let cursor = range.0 + insertion.len();
                    // The token already is the selected item, so accepting it
                    // would change nothing: run the command instead of
                    // spending an Enter on a no-op accept.
                    let exact = input.get(range.0..range.1) == Some(insertion.as_ref());
                    self.notify_lifecycle(PaletteLifecycle::Accept);
                    self.argument_items.clear();
                    self.argument_range = None;
                    self.pending_arguments = None;
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
                    .zip(self.argument_items.get(self.selected))
                {
                    let insertion = item.item.insertion.clone();
                    self.notify_lifecycle(PaletteLifecycle::Accept);
                    let text = self.replace_argument(input, &insertion);
                    self.accepted_argument_input = Some(text.clone());
                    return CommandAction::Complete {
                        text,
                        cursor: range.0 + insertion.len(),
                    };
                }
                if let Some(item) = self.filtered.get(self.selected) {
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
    pub(crate) fn set_argument_completion(
        &mut self,
        range: (usize, usize),
        item: maki_lua::CommandArgumentItem,
    ) {
        self.argument_range = Some(range);
        self.argument_items = vec![ArgumentMatch {
            candidate: None,
            item: CompletionItem {
                label: Arc::from(item.label),
                insertion: Arc::from(item.insertion),
                description: item.description.map(Arc::from),
            },
            indices: Vec::new(),
        }];
        self.selected = 0;
    }

    pub fn sync_arguments(&mut self, input: &str, cursor: usize, mode: &str) -> bool {
        self.argument_generation = self.argument_generation.wrapping_add(1);
        self.argument_range = None;
        self.pending_arguments = None;
        if self.accepted_argument_input.as_deref() == Some(input) {
            return false;
        }
        let abandoned = self.accepted_argument_input.take().is_some();
        let Some(command) = self
            .filtered
            .get(self.selected)
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
            self.argument_session = self.argument_session.wrapping_add(1);
            self.completion_session = self.registry.open_completion(command, self.target).ok();
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
            return Dirty::NO;
        }
        let CompletionResult::Items(items) = result else {
            return Dirty::NO;
        };
        self.argument_items.clear();
        self.selected = 0;
        let mut matcher = Matcher::new(Config::DEFAULT);
        let pattern = nucleo::pattern::Pattern::parse(
            &pending.query,
            CaseMatching::Ignore,
            Normalization::Smart,
        );
        for candidate in items {
            let item = candidate.item().clone();
            let mut indices = Vec::new();
            if pattern
                .indices(
                    Utf32String::from(item.label.as_ref()).slice(..),
                    &mut matcher,
                    &mut indices,
                )
                .is_none()
            {
                continue;
            }
            self.argument_items.push(ArgumentMatch {
                candidate: Some(candidate),
                item,
                indices,
            });
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
                    .get(self.selected)
                    .and_then(|item| item.candidate.as_ref())
                {
                    let _ = session.highlight(candidate);
                }
            }
            PaletteLifecycle::Accept => {
                if let Some(candidate) = self
                    .argument_items
                    .get_mut(self.selected)
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

    pub fn cancel_arguments(&mut self) {
        self.notify_lifecycle(PaletteLifecycle::Cancel);
        self.argument_items.clear();
        self.argument_range = None;
        self.pending_arguments = None;
    }

    fn replace_argument(&self, input: &str, replacement: &str) -> String {
        let Some((start, end)) = self.argument_range else {
            return input.to_string();
        };
        format!("{}{}{}", &input[..start], replacement, &input[end..])
    }

    pub fn sync(&mut self, input: &str) {
        let snapshot = self.registry.snapshot();
        if snapshot.generation() != self.snapshot.generation() {
            self.snapshot = snapshot;
            self.nucleo = Self::build_nucleo(&self.snapshot);
        }
        let Some(stripped) = input.strip_prefix('/') else {
            self.filtered.clear();
            self.current_arg_count = 0;
            return;
        };

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

        self.tick();
    }

    fn tick(&mut self) {
        loop {
            let status = self.nucleo.tick(TICK_TIMEOUT_MS);
            if status.changed {
                self.refresh_matches();
            }
            if !status.running {
                break;
            }
        }
    }

    fn refresh_matches(&mut self) {
        let snapshot = self.nucleo.snapshot();
        let pattern = snapshot.pattern();
        let has_pattern = !pattern.column_pattern(0).atoms.is_empty();

        self.filtered.clear();
        let count = snapshot.matched_item_count();
        for item in snapshot.matched_items(0..count) {
            let cmd_item = &item.data;
            let col = &item.matcher_columns[0];

            if !cmd_item
                .command
                .spec()
                .arguments
                .accepts(self.current_arg_count)
            {
                continue;
            }

            let indices = if has_pattern {
                let mut indices_buf = vec![];
                pattern.column_pattern(0).indices(
                    col.slice(..),
                    &mut self.matcher,
                    &mut indices_buf,
                );
                indices_buf
            } else {
                Vec::new()
            };

            self.filtered.push(Match {
                command: cmd_item.command.clone(),
                indices,
            });
        }

        self.selected = self.selected.min(self.filtered.len().saturating_sub(1));
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
        self.selected = if self.selected == 0 {
            self.filtered.len() - 1
        } else {
            self.selected - 1
        };
    }

    pub fn move_down(&mut self) {
        if self.filtered.is_empty() {
            return;
        }
        self.selected = if self.selected == self.filtered.len() - 1 {
            0
        } else {
            self.selected + 1
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
        let command = self.filtered.get(self.selected)?.command.clone();
        let args = input
            .strip_prefix('/')
            .and_then(|s| s.split_once(char::is_whitespace))
            .map(|(_, a)| a.trim())
            .unwrap_or("");
        Some(ConfirmedCommand {
            command,
            args: args.to_string(),
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

    pub fn view(&self, frame: &mut Frame, input_area: Rect) -> Option<Rect> {
        let filtered = if self.argument_items.is_empty() {
            &self.filtered
        } else {
            return self.view_arguments(frame, input_area);
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
                let selected = i == self.selected;
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

    fn view_arguments(&self, frame: &mut Frame, input_area: Rect) -> Option<Rect> {
        let height = (self.argument_items.len() as u16).min(input_area.y);
        if height == 0 {
            return None;
        }
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
            .map(|(i, m)| {
                let selected = i == self.selected;
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

fn command_args(input: &str) -> &str {
    input
        .strip_prefix('/')
        .and_then(|input| input.find(char::is_whitespace).map(|i| &input[i + 1..]))
        .unwrap_or("")
}

fn argument_at_cursor(input: &str, cursor: usize) -> Option<(usize, usize, String, usize)> {
    let slash = input.strip_prefix('/')?;
    let command_end = slash.find(char::is_whitespace)? + 1;
    if cursor < command_end || !input.is_char_boundary(cursor) {
        return None;
    }
    let start = input[..cursor]
        .rfind(char::is_whitespace)
        .map_or(command_end, |i| i + 1);
    let end = input[cursor..]
        .find(char::is_whitespace)
        .map_or(input.len(), |i| cursor + i);
    let arg = input[start..end].to_string();
    let index = input[command_end..start].split_whitespace().count();
    Some((start, end, arg, index))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use maki_commands::{
        ArgumentArity, CommandBehavior, CommandDocs, CommandError, CommandFuture,
        CommandInvocation, CommandRegistry, CommandSpec, ProducerPrecedence, Registration,
    };

    use super::CommandPalette;

    struct Noop;

    impl CommandBehavior for Noop {
        fn execute(
            &self,
            _invocation: CommandInvocation,
        ) -> CommandFuture<Result<(), CommandError>> {
            Box::pin(async { Ok(()) })
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
        let target = registry.create_target();
        let mut palette = CommandPalette::new(registry, target);

        palette.sync("/");

        assert_eq!(palette.filtered.len(), 1);
        assert_eq!(palette.filtered[0].command.invoked_name(), "/dynamic");
    }

    #[test]
    fn confirmation_owns_selected_resolution_across_registry_refresh() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/dynamic", "First")])
            .unwrap();
        let target = registry.create_target();
        let mut palette = CommandPalette::new(registry, target);
        palette.sync("/dynamic arg");
        let confirmed = palette.confirm("/dynamic arg").unwrap();

        producer
            .replace(vec![registration("/dynamic", "Second")])
            .unwrap();

        assert_eq!(confirmed.command.spec().docs.summary.as_ref(), "First");
        assert_eq!(confirmed.args, "arg");
    }
}
