use std::mem;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use maki_commands::{
    ArgumentKind, CommandArguments, CommandId, CommandRegistry, CompletionCandidate,
    CompletionInput, CompletionItem, CompletionItemNavigation, CompletionProviders,
    CompletionResult, CompletionSession, CompletionSnapshot, CompletionSnapshotSink,
    PositionalArgument, QuoteStyle, RegistrySnapshot, ResolvedCommand, SlashClass, TargetHandle,
    classify_input, encode_completion_value, lex_tolerant,
};
use maki_match::{CompletionMatchOptions, completion_match};
use nucleo::pattern::{CaseMatching, Normalization};
use nucleo::{Config, Nucleo, Utf32String};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::components::coherent_completion::{Publication, Published};
use crate::components::file_completion::{
    CompletionGridItem, CompletionGridState, render_completion_grid,
};
use crate::{
    repaint::{Cadence, Dirty},
    theme,
};
/// Note appended to builtin alias rows: `(Alias for /new)`.
const ALIAS_NOTE: &str = " (Alias for ";
#[cfg(test)]
const MATCHER_SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
#[cfg(test)]
const MATCHER_SETTLE_POLL: std::time::Duration = std::time::Duration::from_millis(1);

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
    argument_grid: CompletionGridState,
    filtered: Vec<Match>,
    registry: CommandRegistry,
    target: TargetHandle,
    defaults: CompletionProviders,
    cwd: Arc<str>,
    snapshot: RegistrySnapshot,
    nucleo: Nucleo<CommandItem>,
    current_arg_count: usize,
    argument_items: Vec<ArgumentMatch>,
    argument_range: Option<(usize, usize)>,
    argument_kind: Option<ArgumentKind>,
    typed_argument_owned: bool,
    argument_generation: u64,
    argument_revision: u64,
    completion_session: Option<CompletionSession>,
    completion_session_cwd: Option<Arc<str>>,
    pending_arguments: Option<PendingArguments>,
    accepted_argument_input: Option<(String, usize)>,
    dismissed_argument_input: Option<(String, usize)>,
    command_publication: Published<CommandRequest, ()>,
    pending_command: Option<(u64, CommandRequest)>,
    argument_publication: Published<ArgumentRequest, ()>,
    latest_argument_context: Option<(String, usize, String)>,
    command_matching: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommandRequest {
    query: String,
    registry_generation: u64,
    argument_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArgumentRequest {
    command_id: CommandId,
    invoked_name: Arc<str>,
    argument_index: usize,
    query: String,
    range: (usize, usize),
    mode: String,
}

#[derive(Clone)]
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
    snapshots: Option<Arc<std::sync::Mutex<Option<CompletionSnapshot>>>>,
    generation: u64,
    key: ArgumentRequest,
    snapshot_applied: bool,
    last_highlighted_item: Option<CompletionItem>,
}

impl CommandPalette {
    #[cfg(test)]
    pub fn new(registry: CommandRegistry, target: TargetHandle) -> Self {
        Self::with_defaults(
            registry,
            target,
            CompletionProviders::default(),
            Arc::from(""),
        )
    }

    #[cfg(test)]
    pub(crate) fn with_defaults(
        registry: CommandRegistry,
        target: TargetHandle,
        defaults: CompletionProviders,
        cwd: Arc<str>,
    ) -> Self {
        let snapshot = registry
            .snapshot_for(&target)
            .expect("new command target is live");
        Self::from_snapshot(registry, target, snapshot, defaults, cwd)
    }

    pub(crate) fn prepared(
        registry: CommandRegistry,
        target: TargetHandle,
        snapshot: RegistrySnapshot,
        defaults: CompletionProviders,
        cwd: Arc<str>,
    ) -> Self {
        Self::from_snapshot(registry, target, snapshot, defaults, cwd)
    }

    fn from_snapshot(
        registry: CommandRegistry,
        target: TargetHandle,
        snapshot: RegistrySnapshot,
        defaults: CompletionProviders,
        cwd: Arc<str>,
    ) -> Self {
        let nucleo = Self::build_nucleo(&snapshot);
        Self {
            command_selected: 0,
            command_query: String::new(),
            argument_selected: 0,
            argument_scroll_offset: 0,
            argument_grid: CompletionGridState::default(),
            filtered: Vec::new(),
            registry,
            target,
            defaults,
            cwd,
            snapshot,
            nucleo,
            current_arg_count: 0,
            argument_items: Vec::new(),
            argument_range: None,
            argument_kind: None,
            typed_argument_owned: false,
            argument_generation: 0,
            argument_revision: 0,
            completion_session: None,
            completion_session_cwd: None,
            pending_arguments: None,
            accepted_argument_input: None,
            dismissed_argument_input: None,
            command_publication: Published::default(),
            pending_command: None,
            argument_publication: Published::default(),
            latest_argument_context: None,
            command_matching: false,
        }
    }

    fn build_nucleo(snapshot: &RegistrySnapshot) -> Nucleo<CommandItem> {
        let mut nucleo = Nucleo::new(Config::DEFAULT, Arc::new(|| {}), None, 1);
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
        nucleo.tick(0);
        nucleo
    }

    pub fn handle_key(&mut self, key: KeyEvent, input: &str) -> CommandAction {
        if self
            .accepted_argument_input
            .as_ref()
            .is_some_and(|(accepted, _)| accepted == input)
            || self
                .dismissed_argument_input
                .as_ref()
                .is_some_and(|(dismissed, _)| dismissed == input)
        {
            return if key.code == KeyCode::Enter {
                self.confirm_close(input)
            } else {
                CommandAction::Passthrough
            };
        }
        if !self.is_active() {
            return CommandAction::Passthrough;
        }
        if self.is_typed_path_grid()
            && (self.argument_publication.is_pending() || !self.argument_items.is_empty())
        {
            let selected = self.argument_grid.selected();
            if self.argument_grid.handle_key(
                &key,
                self.argument_items.len(),
                self.argument_publication.is_pending(),
            ) {
                if self.argument_grid.selected() != selected {
                    self.notify_keyboard_highlight();
                }
                return CommandAction::Consumed;
            }
        }
        match key.code {
            KeyCode::Up => {
                if self.argument_publication.is_pending() || self.command_publication.is_pending() {
                    CommandAction::Consumed
                } else if !self.argument_items.is_empty() {
                    self.argument_selected = if self.argument_selected == 0 {
                        self.argument_items.len() - 1
                    } else {
                        self.argument_selected - 1
                    };
                    self.notify_keyboard_highlight();
                    CommandAction::Consumed
                } else {
                    self.move_up();
                    CommandAction::SelectionChanged
                }
            }
            KeyCode::Down => {
                if self.argument_publication.is_pending() || self.command_publication.is_pending() {
                    CommandAction::Consumed
                } else if !self.argument_items.is_empty() {
                    self.argument_selected =
                        if self.argument_selected == self.argument_items.len() - 1 {
                            0
                        } else {
                            self.argument_selected + 1
                        };
                    self.notify_keyboard_highlight();
                    CommandAction::Consumed
                } else {
                    self.move_down();
                    CommandAction::SelectionChanged
                }
            }
            KeyCode::Esc => {
                if self.argument_range.is_some() || self.completion_session.is_some() {
                    let dismissed_start = self.argument_range.map(|(start, _)| start);
                    self.cancel_arguments();
                    self.dismissed_argument_input =
                        dismissed_start.map(|start| (input.to_owned(), start));
                    self.filtered.clear();
                    self.argument_items.clear();
                    self.argument_range = None;
                    self.pending_arguments = None;
                } else {
                    self.close();
                }
                CommandAction::Consumed
            }
            KeyCode::Enter => {
                if self.command_publication.is_pending() || self.argument_publication.is_pending() {
                    if self.argument_items.is_empty()
                        && let Some(command) = self.confirm_exact(input)
                    {
                        self.close();
                        return CommandAction::Execute(command);
                    }
                    return CommandAction::Consumed;
                }
                if let Some((range, item)) = self
                    .argument_range
                    .zip(self.argument_items.get(self.argument_selection()).cloned())
                {
                    if item.candidate.as_ref().is_some_and(|candidate| {
                        candidate.navigation() == CompletionItemNavigation::Directory
                    }) && (key.code == KeyCode::Tab
                        || self.active_argument_kind() == Some(ArgumentKind::File))
                    {
                        return self.advance_argument(input, range, &item);
                    }
                    return self.accept_argument(input, range, &item, false);
                }
                self.confirm_close(input)
            }
            KeyCode::Tab => {
                if self.command_publication.is_pending() || self.argument_publication.is_pending() {
                    if self.argument_range.is_none()
                        && let Some(command) = self.confirm_command_name(input)
                    {
                        let name = command.command.invoked_name().to_owned();
                        let text = if command_has_args(&command.command) {
                            format!("{name} ")
                        } else {
                            name
                        };
                        return CommandAction::Complete {
                            cursor: text.len(),
                            text,
                        };
                    }
                    return CommandAction::Consumed;
                }
                if self.typed_argument_owned && self.argument_items.is_empty() {
                    return CommandAction::Consumed;
                }
                if self.argument_range.is_some() {
                    let Some((range, item)) = self
                        .argument_range
                        .zip(self.argument_items.get(self.argument_selection()).cloned())
                    else {
                        return CommandAction::Consumed;
                    };
                    if item.candidate.as_ref().is_some_and(|candidate| {
                        candidate.navigation() == CompletionItemNavigation::Directory
                    }) {
                        return self.advance_argument(input, range, &item);
                    }
                    return self.accept_argument(input, range, &item, true);
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
            && self.dismissed_argument_input.is_none()
            && (self.command_publication.is_pending()
                || !self.filtered.is_empty()
                || !self.argument_items.is_empty()
                || self.completion_session.is_some())
    }

    pub(crate) fn selected_command(&self) -> Option<ResolvedCommand> {
        self.filtered
            .get(self.command_selected)
            .map(|item| item.command.clone())
    }

    pub(crate) fn typed_path_argument(
        &self,
        input: &str,
        cursor: usize,
    ) -> Option<(ArgumentKind, (usize, usize), String)> {
        let (command, typed) = self.argument_command(input)?;
        if !typed {
            return None;
        }
        let schema = command.spec().arguments.positional()?;
        let (start, end, _, index) = typed_argument_at_cursor(input, cursor, schema)?;
        let kind = schema
            .get(index)
            .or_else(|| schema.last().filter(|argument| argument.variadic))?
            .kind
            .clone();
        if !matches!(kind, ArgumentKind::File | ArgumentKind::Directory) {
            return None;
        }
        let query_end = cursor.clamp(start, end);
        Some((kind, (start, end), input[start..query_end].to_owned()))
    }

    fn argument_command(&self, input: &str) -> Option<(ResolvedCommand, bool)> {
        let SlashClass::Command(input) = classify_input(input) else {
            return None;
        };
        let command_name = input
            .strip_prefix('/')?
            .split_whitespace()
            .next()
            .unwrap_or_default();
        let exact = self
            .registry
            .resolve_for(&self.target, &format!("/{command_name}"))
            .ok();
        exact
            .map(|command| {
                let typed = command.spec().arguments.positional().is_some();
                (command, typed)
            })
            .or_else(|| self.selected_command().map(|command| (command, false)))
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
        self.argument_selection()
    }

    #[cfg(test)]
    pub(crate) fn set_typed_path_grid_for_test(&mut self, kind: ArgumentKind, count: usize) {
        self.typed_argument_owned = true;
        self.argument_kind = Some(kind);
        self.argument_range = Some((0, 1));
        self.argument_items = (0..count)
            .map(|index| ArgumentMatch {
                candidate: None,
                item: CompletionItem {
                    label: format!("item-{index}").into(),
                    insertion: format!("item-{index}").into(),
                    description: None,
                },
                indices: Vec::new(),
                ranking: completion_match(
                    "",
                    &format!("item-{index}"),
                    CompletionMatchOptions {
                        case_matching: CaseMatching::Ignore,
                        normalization: Normalization::Smart,
                    },
                )
                .unwrap()
                .ranking,
                order: index,
            })
            .collect();
        self.argument_selected = 0;
        self.argument_grid.reset();
    }

    /// Select a specific argument item (test seam; `argument_items` are
    /// refiltered on every poll, so tests must target the current rows).
    #[cfg(test)]
    pub(crate) fn select_for_test(&mut self, item: &maki_commands::CompletionItem) {
        self.argument_selected = self
            .argument_items
            .iter()
            .position(|candidate| candidate.item.label == item.label)
            .unwrap_or(0);
    }

    #[cfg(test)]
    pub(crate) fn has_argument_selectable(&self) -> bool {
        !self.argument_publication.is_pending() && !self.argument_items.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn argument_match_items(&self) -> Vec<CompletionItem> {
        self.argument_items
            .iter()
            .map(|item| item.item.clone())
            .collect()
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
        let deadline = std::time::Instant::now() + MATCHER_SETTLE_TIMEOUT;
        while self.command_publication.is_pending() {
            let _ = self.tick_commands();
            assert!(
                std::time::Instant::now() < deadline,
                "command matcher did not settle"
            );
            std::thread::sleep(MATCHER_SETTLE_POLL);
        }
        self.argument_publication.commit_sync(
            ArgumentRequest {
                command_id: self.filtered[self.command_selected].command.command_id(),
                invoked_name: Arc::from(
                    self.filtered[self.command_selected].command.invoked_name(),
                ),
                argument_index: 0,
                query: String::new(),
                range,
                mode: String::new(),
            },
            (),
        );
        self.argument_range = Some(range);
        self.typed_argument_owned = true;
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
        self.argument_grid.reset();
    }

    pub fn sync_arguments(&mut self, input: &str, cursor: usize, mode: &str) -> bool {
        self.latest_argument_context = Some((input.to_owned(), cursor, mode.to_owned()));
        self.argument_generation = self.argument_generation.wrapping_add(1);
        if self.command_publication.is_pending() {
            self.cancel_arguments();
            return false;
        }
        let Some((command, typed)) = self.argument_command(input) else {
            let abandoned = self.accepted_argument_input.take().is_some()
                || self.dismissed_argument_input.take().is_some();
            self.cancel_arguments();
            return abandoned;
        };
        let argument_at_cursor = if typed {
            command
                .spec()
                .arguments
                .positional()
                .and_then(|schema| typed_argument_at_cursor(input, cursor, schema))
        } else {
            argument_at_cursor(input, cursor)
        };
        let suppressed = argument_at_cursor.as_ref().is_some_and(|(start, ..)| {
            self.accepted_argument_input
                .as_ref()
                .is_some_and(|(accepted, accepted_start)| {
                    accepted == input && accepted_start == start
                })
                || self.dismissed_argument_input.as_ref().is_some_and(
                    |(dismissed, dismissed_start)| dismissed == input && dismissed_start == start,
                )
        });
        if suppressed {
            return false;
        }
        let abandoned = self.accepted_argument_input.take().is_some()
            || self.dismissed_argument_input.take().is_some();
        let Some((start, end, argument, index)) = argument_at_cursor else {
            self.cancel_arguments();
            return abandoned;
        };
        self.typed_argument_owned = typed;
        self.argument_kind = typed
            .then(|| {
                command.spec().arguments.positional().and_then(|schema| {
                    schema
                        .get(index)
                        .or_else(|| schema.last().filter(|argument| argument.variadic))
                        .map(|argument| argument.kind.clone())
                })
            })
            .flatten();
        let request_key = ArgumentRequest {
            command_id: command.command_id(),
            invoked_name: Arc::from(command.invoked_name()),
            argument_index: index,
            query: argument.clone(),
            range: (start, end),
            mode: mode.to_owned(),
        };
        self.argument_generation = self.argument_publication.begin(request_key.clone());
        let same_session = self.completion_session.as_ref().is_some_and(|session| {
            session.command().command_id() == command.command_id()
                && session.command().invoked_name() == command.invoked_name()
                && self.completion_session_cwd.as_deref() == Some(self.cwd.as_ref())
        });
        if !same_session {
            self.notify_lifecycle(PaletteLifecycle::Cancel);
            self.argument_revision = 0;
            self.completion_session = self
                .registry
                .open_completion_with_defaults(
                    command,
                    self.target.id(),
                    self.defaults.clone(),
                    Arc::clone(&self.cwd),
                )
                .ok();
            self.completion_session_cwd = self
                .completion_session
                .as_ref()
                .map(|_| Arc::clone(&self.cwd));
        }
        let Some(session) = self.completion_session.clone() else {
            self.argument_publication.clear();
            self.argument_items.clear();
            self.argument_range = None;
            self.argument_grid.reset();
            return abandoned;
        };
        let (tx, rx) = flume::bounded(1);
        let latest_snapshot = Arc::new(std::sync::Mutex::new(None::<CompletionSnapshot>));
        let sink_snapshot = Arc::clone(&latest_snapshot);
        let arguments = command_args(input);
        let arguments_start = input.len() - arguments.len();
        let request = session.complete_input_with_sink(
            CompletionInput {
                arguments: Arc::from(arguments),
                argument: Arc::from(argument.as_str()),
                argument_index: index,
                argument_range: Some(start - arguments_start..end - arguments_start),
                mode: Arc::from(mode),
            },
            Some(CompletionSnapshotSink::new(move |snapshot| {
                *sink_snapshot
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()) = Some(snapshot);
            })),
        );
        smol::spawn(async move {
            let _ = tx.send_async(request.await).await;
        })
        .detach();
        self.pending_arguments = Some(PendingArguments {
            rx,
            snapshots: Some(latest_snapshot),
            generation: self.argument_generation,
            key: request_key,
            snapshot_applied: false,
            last_highlighted_item: None,
        });
        abandoned
    }

    fn poll_argument_response(&mut self) -> Dirty {
        let Some(mut pending) = self.pending_arguments.take() else {
            return Dirty::NO;
        };
        if pending.generation != self.argument_generation {
            return Dirty::NO;
        }
        let mut dirty = Dirty::NO;
        if let Some(latest) = pending.snapshots.as_deref().and_then(|slab| {
            slab.lock()
                .unwrap_or_else(|error| error.into_inner())
                .take()
        }) && latest.revision >= self.argument_revision
        {
            let publication = if self.argument_publication.is_pending() {
                self.argument_publication
                    .commit(pending.generation, pending.key.clone(), ())
            } else {
                self.argument_publication.stream(pending.key.clone(), ())
            };
            if publication != Publication::Wait {
                dirty |= self.apply_completion_items(
                    &pending.key.query,
                    pending.key.range,
                    latest.candidates,
                    pending.snapshot_applied,
                    latest.finished,
                    &mut pending.last_highlighted_item,
                );
                self.argument_revision = latest.revision;
                pending.snapshot_applied = true;
            }
        }
        let result = match pending.rx.try_recv() {
            Ok(result) => result,
            Err(flume::TryRecvError::Disconnected) => {
                return if dirty == Dirty::YES {
                    dirty
                } else {
                    self.finish_empty_arguments(pending);
                    Dirty::YES
                };
            }
            Err(flume::TryRecvError::Empty) => {
                self.pending_arguments = Some(pending);
                return dirty;
            }
        };
        match result {
            CompletionResult::Items(items) => {
                let publication = if self.argument_publication.is_pending() {
                    self.argument_publication
                        .commit(pending.generation, pending.key.clone(), ())
                } else {
                    self.argument_publication.stream(pending.key.clone(), ())
                };
                if publication == Publication::Wait {
                    return dirty;
                }
                dirty |= self.apply_completion_items(
                    &pending.key.query,
                    pending.key.range,
                    items,
                    pending.snapshot_applied,
                    true,
                    &mut pending.last_highlighted_item,
                );
                dirty
            }
            CompletionResult::Stale | CompletionResult::Cancelled | CompletionResult::Failed => {
                self.finish_empty_arguments(pending);
                Dirty::YES
            }
        }
    }

    fn apply_completion_items(
        &mut self,
        query: &str,
        range: (usize, usize),
        items: Vec<CompletionCandidate>,
        preserve_selection: bool,
        finished: bool,
        last_highlighted_item: &mut Option<CompletionItem>,
    ) -> Dirty {
        let mut matches = Vec::new();
        for (order, candidate) in items.into_iter().enumerate() {
            let item = candidate.item().clone();
            let Some(matched) = completion_match(
                query,
                &item.label,
                CompletionMatchOptions {
                    case_matching: CaseMatching::Ignore,
                    normalization: Normalization::Smart,
                },
            ) else {
                continue;
            };
            matches.push(ArgumentMatch {
                candidate: Some(candidate),
                item,
                indices: matched.indices,
                ranking: matched.ranking,
                order,
            });
        }
        matches.sort_by(|a, b| {
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
        let previous_item = preserve_selection
            .then(|| self.argument_items.get(self.argument_selection()))
            .flatten()
            .map(|item| item.item.clone());
        self.argument_items = matches;
        self.argument_range = (!self.argument_items.is_empty()).then_some(range);
        let selected = previous_item
            .as_ref()
            .and_then(|item| {
                self.argument_items
                    .iter()
                    .position(|candidate| candidate.item == *item)
            })
            .unwrap_or(0);
        self.argument_selected = selected;
        self.argument_scroll_offset = self.argument_scroll_offset.min(selected);
        self.argument_grid.set_selected(selected);
        let selected_item = self
            .argument_items
            .get(selected)
            .map(|item| item.item.clone());
        if selected_item.is_none() {
            *last_highlighted_item = None;
            if finished {
                self.notify_lifecycle(PaletteLifecycle::Cancel);
            }
        } else if selected_item.as_ref() != last_highlighted_item.as_ref() {
            *last_highlighted_item = selected_item;
            self.notify_lifecycle(PaletteLifecycle::Highlight);
        }
        Dirty::YES
    }

    fn accept_argument(
        &mut self,
        input: &str,
        range: (usize, usize),
        item: &ArgumentMatch,
        tab: bool,
    ) -> CommandAction {
        let edit = encode_completion_value(
            &item.item.insertion,
            range.0..range.1,
            argument_quote_style(input, range),
        );
        let text = self.replace_argument(input, &edit.text);
        if !self.notify_lifecycle(PaletteLifecycle::Accept) {
            return CommandAction::Consumed;
        }
        self.reset_argument_state();
        if tab {
            self.accepted_argument_input = Some((text.clone(), range.0));
            return CommandAction::Complete {
                text,
                cursor: edit.cursor,
            };
        }
        let exact = input.get(range.0..range.1) == Some(edit.text.as_ref());
        if exact {
            self.confirm_close(input)
        } else {
            self.accepted_argument_input = Some((text.clone(), range.0));
            CommandAction::AcceptArgument {
                text,
                cursor: edit.cursor,
            }
        }
    }

    fn advance_argument(
        &mut self,
        input: &str,
        range: (usize, usize),
        item: &ArgumentMatch,
    ) -> CommandAction {
        let Some(candidate) = item.candidate.as_ref() else {
            return CommandAction::Consumed;
        };
        let Some(session) = self.completion_session.as_ref() else {
            return CommandAction::Consumed;
        };
        if session.validate(candidate).is_err() {
            return CommandAction::Consumed;
        }
        let edit = encode_completion_value(
            &item.item.insertion,
            range.0..range.1,
            argument_quote_style(input, range),
        );
        let text = self.replace_argument(input, &edit.text);
        let cursor = if edit.text.ends_with(['\'', '"']) {
            edit.cursor - 1
        } else {
            edit.cursor
        };
        CommandAction::Complete { text, cursor }
    }

    fn active_argument_kind(&self) -> Option<ArgumentKind> {
        self.argument_kind.clone()
    }

    fn is_typed_path_grid(&self) -> bool {
        self.typed_argument_owned
            && matches!(
                self.argument_kind,
                Some(ArgumentKind::File | ArgumentKind::Directory)
            )
    }

    fn argument_selection(&self) -> usize {
        if self.is_typed_path_grid() {
            self.argument_grid.selected()
        } else {
            self.argument_selected
        }
    }

    fn finish_empty_arguments(&mut self, pending: PendingArguments) {
        if self
            .argument_publication
            .clear_request(pending.generation, &pending.key)
            == Publication::Clear
        {
            self.argument_items.clear();
            self.argument_range = None;
            self.argument_grid.reset();
            self.notify_lifecycle(PaletteLifecycle::Cancel);
        }
    }

    fn notify_keyboard_highlight(&mut self) {
        let selected_item = self
            .argument_items
            .get(self.argument_selection())
            .map(|item| item.item.clone());
        if let Some(pending) = self.pending_arguments.as_mut() {
            pending.last_highlighted_item = selected_item;
        }
        self.notify_lifecycle(PaletteLifecycle::Highlight);
    }

    fn notify_lifecycle(&mut self, event: PaletteLifecycle) -> bool {
        let Some(session) = &self.completion_session else {
            return true;
        };
        let selected = self.argument_selection();
        match event {
            PaletteLifecycle::Highlight => {
                if let Some(candidate) = self
                    .argument_items
                    .get(selected)
                    .and_then(|item| item.candidate.as_ref())
                {
                    let _ = session.highlight(candidate);
                }
            }
            PaletteLifecycle::Accept => {
                if let Some(candidate) = self
                    .argument_items
                    .get(selected)
                    .and_then(|item| item.candidate.clone())
                    && session.accept(candidate).is_err()
                {
                    return false;
                }
                self.completion_session = None;
                self.completion_session_cwd = None;
            }
            PaletteLifecycle::Cancel => {
                let _ = session.cancel();
                self.completion_session = None;
                self.completion_session_cwd = None;
            }
        }
        true
    }

    fn reset_argument_state(&mut self) {
        self.argument_items.clear();
        self.argument_range = None;
        self.argument_kind = None;
        self.typed_argument_owned = false;
        self.pending_arguments = None;
        self.argument_publication.clear();
        self.argument_selected = 0;
        self.argument_scroll_offset = 0;
        self.argument_grid.reset();
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

    pub(crate) fn set_cwd(&mut self, cwd: Arc<str>) {
        self.cwd = cwd;
    }

    pub fn sync(&mut self, input: &str) {
        let Ok(snapshot) = self.registry.snapshot_for(&self.target) else {
            self.close();
            return;
        };
        let registry_changed = snapshot.generation() != self.snapshot.generation();
        if registry_changed {
            self.snapshot = snapshot;
            self.nucleo = Self::build_nucleo(&self.snapshot);
        }
        let SlashClass::Command(trimmed) = classify_input(input) else {
            self.filtered.clear();
            self.current_arg_count = 0;
            self.pending_command = None;
            self.command_publication.clear();
            return;
        };
        let stripped = &trimmed[1..]; // trimmed starts with exactly one '/'

        let parts: Vec<&str> = stripped.split_whitespace().collect();
        let cmd_word = parts.first().copied().unwrap_or(stripped);
        let trailing_space = stripped.ends_with(char::is_whitespace);
        let whitespace_argument_count = if trailing_space {
            parts.len()
        } else {
            parts.len().saturating_sub(1)
        };
        let argument_count = self
            .registry
            .resolve_for(&self.target, &format!("/{cmd_word}"))
            .ok()
            .filter(|command| matches!(command.spec().arguments, CommandArguments::Positional(_)))
            .and_then(|_| lex_tolerant(command_args(input)).ok())
            .map_or(whitespace_argument_count, |tokens| {
                tokens.len() + usize::from(command_args(input).ends_with(char::is_whitespace))
            });
        let request = CommandRequest {
            query: cmd_word.to_owned(),
            registry_generation: self.snapshot.generation(),
            argument_count,
        };
        if !registry_changed
            && self.command_publication.can_accept()
            && self.command_query == request.query
            && request.registry_generation == self.snapshot.generation()
        {
            if self.current_arg_count != request.argument_count {
                self.current_arg_count = request.argument_count;
                self.command_publication.commit_sync(request.clone(), ());
                self.refresh_matches(&request.query);
            }
            return;
        }

        let pending_same_query = self
            .pending_command
            .as_ref()
            .is_some_and(|(_, pending)| pending.query == request.query);
        if pending_same_query
            && self
                .registry
                .resolve_for(&self.target, &format!("/{cmd_word}"))
                .is_ok()
        {
            self.pending_command = None;
            self.command_publication.commit_sync(request.clone(), ());
            self.current_arg_count = request.argument_count;
            self.refresh_matches(&request.query);
            return;
        }

        self.nucleo.pattern.reparse(
            0,
            cmd_word,
            CaseMatching::Ignore,
            Normalization::Smart,
            false,
        );
        self.notify_lifecycle(PaletteLifecycle::Cancel);
        self.pending_arguments = None;
        self.argument_publication.cancel();

        let generation = self.command_publication.begin(request.clone());
        self.pending_command = Some((generation, request));
        let _ = self.tick_commands();
    }

    pub fn tick(&mut self) -> Dirty {
        self.tick_commands() | self.poll_argument_response()
    }

    #[cfg(test)]
    pub fn poll_arguments(&mut self) -> Dirty {
        self.tick()
    }

    pub fn cadence(&self) -> Cadence {
        Cadence::any([
            self.command_publication.cadence(),
            self.argument_publication.cadence(),
            Cadence::when(self.command_matching, Cadence::PENDING),
        ])
    }

    fn tick_commands(&mut self) -> Dirty {
        let status = self.nucleo.tick(0);
        self.command_matching = status.running;
        let Some((generation, request)) = self.pending_command.clone() else {
            if status.changed
                && self.command_publication.can_accept()
                && !self.command_query.is_empty()
            {
                let query = self.command_query.clone();
                self.refresh_matches(&query);
                return Dirty::YES;
            }
            return Dirty::NO;
        };
        if !status.changed
            || self.nucleo.snapshot().pattern().column_pattern(0).atoms
                != self.nucleo.pattern.column_pattern(0).atoms
        {
            return Dirty::NO;
        }
        if self
            .command_publication
            .commit(generation, request.clone(), ())
            != Publication::Commit
        {
            return Dirty::NO;
        }
        self.pending_command = None;
        self.current_arg_count = request.argument_count;
        self.refresh_matches(&request.query);
        self.command_query = request.query.clone();
        if let Some((input, cursor, mode)) = self.latest_argument_context.clone() {
            let _ = self.sync_arguments(&input, cursor, &mode);
        } else {
            self.cancel_arguments();
        }
        Dirty::YES
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

            if let CommandArguments::Positional(arguments) = &cmd_item.command.spec().arguments
                && !positional_accepts(arguments, self.current_arg_count)
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
        self.pending_command = None;
        self.command_publication.clear();
        self.latest_argument_context = None;
        self.command_matching = false;
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
        self.resync_selected_arguments();
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
        self.resync_selected_arguments();
    }

    fn resync_selected_arguments(&mut self) {
        if !self.command_publication.can_accept() {
            return;
        }
        if let Some((input, cursor, mode)) = self.latest_argument_context.clone() {
            let _ = self.sync_arguments(&input, cursor, &mode);
        }
    }

    fn item_has_args(&self, item: &Match) -> bool {
        command_has_args(&item.command)
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
        if !self.command_publication.can_accept() || self.argument_publication.is_pending() {
            return None;
        }
        let command = self
            .selected_command()
            .or_else(|| self.argument_command(input).map(|(command, _)| command))?;
        let args = command_args(input).trim().to_owned();
        Some(ConfirmedCommand { command, args })
    }

    fn confirm_exact(&self, input: &str) -> Option<ConfirmedCommand> {
        let resolved = self.registry.resolve_input_for(&self.target, input).ok()?;
        Some(ConfirmedCommand {
            command: resolved.command,
            args: resolved.arguments.trim().to_owned(),
        })
    }

    fn confirm_command_name(&self, input: &str) -> Option<ConfirmedCommand> {
        let SlashClass::Command(input) = classify_input(input) else {
            return None;
        };
        let name_end = input.find(char::is_whitespace).unwrap_or(input.len());
        if name_end != input.len() {
            return None;
        }
        let name = &input[..name_end];
        let command = self.registry.resolve_for(&self.target, name).ok()?;
        Some(ConfirmedCommand {
            command,
            args: String::new(),
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
        if !self.is_typed_path_grid() {
            self.argument_selected = self
                .argument_selected
                .min(self.argument_items.len().saturating_sub(1));
        }
        if self.typed_argument_owned
            && matches!(
                self.argument_kind,
                Some(ArgumentKind::File | ArgumentKind::Directory)
            )
        {
            return self.view_typed_path_arguments(frame, input_area);
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

    fn view_typed_path_arguments(&mut self, frame: &mut Frame, input_area: Rect) -> Option<Rect> {
        let items: Vec<_> = self
            .argument_items
            .iter()
            .map(|item| CompletionGridItem {
                label: &item.item.label,
                description: item.item.description.as_deref(),
                indices: &item.indices,
                kind: if matches!(self.argument_kind, Some(ArgumentKind::Directory)) {
                    "directory"
                } else {
                    "file"
                },
            })
            .collect();
        render_completion_grid(frame, input_area, &items, &mut self.argument_grid)
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

fn argument_quote_style(input: &str, range: (usize, usize)) -> QuoteStyle {
    match input
        .get(range.0..range.1)
        .and_then(|value| value.chars().next())
    {
        Some('\'') => QuoteStyle::Single,
        Some('"') => QuoteStyle::Double,
        _ => QuoteStyle::None,
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

fn command_has_args(command: &ResolvedCommand) -> bool {
    match &command.spec().arguments {
        CommandArguments::Positional(arguments) => !arguments.is_empty(),
        CommandArguments::Raw { .. } => true,
    }
}

fn positional_accepts(arguments: &[PositionalArgument], count: usize) -> bool {
    arguments.last().is_some_and(|argument| argument.variadic) || count <= arguments.len()
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

fn typed_argument_at_cursor(
    input: &str,
    cursor: usize,
    schema: &[PositionalArgument],
) -> Option<(usize, usize, String, usize)> {
    let args = command_args(input);
    let args_start = input.len() - args.len();
    if args.is_empty()
        && !input[..args_start]
            .chars()
            .next_back()
            .is_some_and(char::is_whitespace)
    {
        return None;
    }
    if cursor < args_start
        || !input.is_char_boundary(cursor)
        || input[..cursor].contains(['\n', '\r'])
    {
        return None;
    }
    let relative_cursor = cursor - args_start;
    let line_end = args.find(['\n', '\r']).unwrap_or(args.len());
    if relative_cursor > line_end {
        return None;
    }
    let tokens = lex_tolerant(&args[..line_end]).ok()?;
    let (start, end, index) = tokens
        .iter()
        .enumerate()
        .find(|(_, token)| {
            relative_cursor >= token.range.start && relative_cursor <= token.range.end
        })
        .map(|(index, token)| (token.range.start, token.range.end, index))
        .unwrap_or_else(|| {
            let index = tokens.partition_point(|token| token.range.end < relative_cursor);
            (relative_cursor, relative_cursor, index)
        });
    let prefix_end = relative_cursor.clamp(start, end);
    let decoded_argument = lex_tolerant(&args[start..prefix_end])
        .ok()
        .and_then(|tokens| tokens.into_iter().next())
        .map_or_else(
            || input[args_start + start..args_start + prefix_end].to_owned(),
            |token| token.value.to_string(),
        );
    let index = if index < schema.len() {
        index
    } else if schema.last().is_some_and(|argument| argument.variadic) {
        schema.len() - 1
    } else {
        return None;
    };
    Some((
        args_start + start,
        args_start + end,
        decoded_argument,
        index,
    ))
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
    use std::sync::{Arc, Mutex, mpsc};

    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use maki_commands::{
        ArgumentKind, CancellationToken, CommandArguments, CommandBehavior, CommandCompletion,
        CommandDocs, CommandError, CommandFuture, CommandInvocation, CommandOutcome,
        CommandRegistry, CommandSpec, CompletionContext, CompletionError, CompletionItem,
        CompletionItemNavigation, CompletionPolicy, CompletionPublisher, HostResponse,
        PositionalArgument, ProducerPrecedence, QuoteStyle, Registration, TargetCapabilities,
        encode_completion_value,
    };
    use maki_config::DEFAULT_AUTOCOMPLETE_HEIGHT;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::layout::Rect;
    use test_case::test_case;

    use super::{
        ArgumentMatch, CaseMatching, CommandAction, CommandPalette, CommandRequest,
        CompletionMatchOptions, MATCHER_SETTLE_POLL, MATCHER_SETTLE_TIMEOUT, Normalization,
        argument_at_cursor, argument_visible_rows, command_args, completion_match,
        typed_argument_at_cursor,
    };
    struct Noop;

    struct GatedDirectoryCompletion {
        started: mpsc::SyncSender<CompletionPublisher>,
        release: Arc<Mutex<mpsc::Receiver<()>>>,
        events: Arc<Mutex<Vec<maki_commands::CompletionLifecycleEvent>>>,
    }

    impl CommandCompletion for GatedDirectoryCompletion {
        fn complete(
            &self,
            _context: CompletionContext,
            _cancellation: CancellationToken,
        ) -> CommandFuture<Result<Vec<CompletionItem>, CompletionError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn navigation(
            &self,
            _context: &CompletionContext,
            _item: &CompletionItem,
        ) -> CompletionItemNavigation {
            CompletionItemNavigation::Directory
        }

        fn complete_incremental(
            &self,
            _context: CompletionContext,
            _cancellation: CancellationToken,
            publisher: CompletionPublisher,
        ) -> CommandFuture<Result<(), CompletionError>> {
            let started = self.started.clone();
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                started.send(publisher).unwrap();
                release.lock().unwrap().recv().unwrap();
                Ok(())
            })
        }

        fn lifecycle(
            &self,
            _context: &CompletionContext,
            event: &maki_commands::CompletionLifecycleEvent,
            _cancellation: &CancellationToken,
        ) -> Result<(), CompletionError> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

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

    fn settle(palette: &mut CommandPalette) {
        let deadline = std::time::Instant::now() + MATCHER_SETTLE_TIMEOUT;
        while palette.command_publication.is_pending() {
            let _ = palette.tick();
            assert!(
                std::time::Instant::now() < deadline,
                "command matcher did not settle"
            );
            std::thread::sleep(MATCHER_SETTLE_POLL);
        }
    }

    fn registration(name: &str, summary: &str) -> Registration {
        Registration {
            spec: CommandSpec {
                name: Arc::from(name),
                aliases: Arc::from([]),
                arguments: CommandArguments::Raw { required: false },
                docs: CommandDocs {
                    summary: Arc::from(summary),
                    argument_hint: None,
                },
                required_capabilities: TargetCapabilities::default(),
            },
            behavior: Arc::new(Noop),
            argument_completions: Vec::new(),
        }
    }

    type GatedPalette = (
        CommandPalette,
        mpsc::Receiver<CompletionPublisher>,
        mpsc::SyncSender<()>,
        Arc<Mutex<Vec<maki_commands::CompletionLifecycleEvent>>>,
    );

    fn gated_directory_palette() -> GatedPalette {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let events = Arc::default();
        let provider = Arc::new(GatedDirectoryCompletion {
            started: started_tx,
            release: Arc::new(Mutex::new(release_rx)),
            events: Arc::clone(&events),
        });
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/cd"),
                    aliases: Arc::from([]),
                    arguments: CommandArguments::Positional(Arc::from([
                        PositionalArgument::optional("path", ArgumentKind::Directory)
                            .with_completion(CompletionPolicy::Replace),
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("Change directory"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Noop),
                argument_completions: vec![Some(provider)],
            }])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        (
            CommandPalette::new(registry, target),
            started_rx,
            release_tx,
            events,
        )
    }

    #[test_case(None; "empty")]
    #[test_case(Some("other"); "nonmatching")]
    fn settled_path_completion_without_visible_rows_allows_cursor_keys(label: Option<&str>) {
        let (mut palette, started, release, _) = gated_directory_palette();
        let input = "/cd missing";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started.recv().unwrap();
        publisher
            .publish(
                label
                    .map(|label| CompletionItem {
                        label: Arc::from(label),
                        insertion: Arc::from(label),
                        description: None,
                    })
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        publisher.finish().unwrap();
        release.send(()).unwrap();
        while palette.pending_arguments.is_some() {
            let _ = palette.poll_arguments();
            std::thread::yield_now();
        }
        assert!(palette.argument_items.is_empty());
        assert!(!palette.argument_publication.is_pending());
        assert!(matches!(
            palette.handle_key(KeyEvent::new(KeyCode::Left, KeyModifiers::NONE), input),
            CommandAction::Passthrough
        ));
    }

    #[test_case(None; "empty")]
    #[test_case(Some("other"); "nonmatching")]
    fn intermediate_snapshot_without_visible_rows_keeps_session(label: Option<&str>) {
        let (mut palette, started, release, _) = gated_directory_palette();
        let input = "/cd match";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started.recv().unwrap();
        publisher
            .publish(
                label
                    .map(|label| CompletionItem {
                        label: Arc::from(label),
                        insertion: Arc::from(label),
                        description: None,
                    })
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        let _ = palette.poll_arguments();
        assert!(palette.argument_items.is_empty());

        publisher
            .publish(vec![CompletionItem {
                label: Arc::from("match"),
                insertion: Arc::from("match"),
                description: None,
            }])
            .unwrap();
        let _ = palette.poll_arguments();
        assert_eq!(palette.argument_match_items()[0].label.as_ref(), "match");
        release.send(()).unwrap();
    }

    #[test]
    fn cancelled_incremental_completion_clears_visible_rows() {
        let (mut palette, started, release, _) = gated_directory_palette();
        let input = "/cd ";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started.recv().unwrap();
        publisher
            .publish(vec![CompletionItem {
                label: Arc::from("visible"),
                insertion: Arc::from("visible"),
                description: None,
            }])
            .unwrap();
        let _ = palette.poll_arguments();
        assert_eq!(palette.argument_match_items()[0].label.as_ref(), "visible");

        palette
            .completion_session
            .as_ref()
            .unwrap()
            .cancel()
            .unwrap();
        release.send(()).unwrap();
        while palette.pending_arguments.is_some() {
            let _ = palette.poll_arguments();
            std::thread::yield_now();
        }

        assert!(palette.argument_items.is_empty());
        assert!(palette.argument_range.is_none());
        assert_eq!(palette.argument_grid.selected(), 0);
    }

    #[test_case(None; "empty_to_nonempty")]
    #[test_case(Some("old"); "replaced_selection")]
    fn final_snapshot_highlights_changed_selection(initial: Option<&str>) {
        let (mut palette, started, release, events) = gated_directory_palette();
        let input = "/cd ";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started.recv().unwrap();
        publisher
            .publish(
                initial
                    .map(|label| CompletionItem {
                        label: Arc::from(label),
                        insertion: Arc::from(label),
                        description: None,
                    })
                    .into_iter()
                    .collect(),
            )
            .unwrap();
        let _ = palette.poll_arguments();
        events.lock().unwrap().clear();

        publisher
            .publish(vec![CompletionItem {
                label: Arc::from("new"),
                insertion: Arc::from("new"),
                description: None,
            }])
            .unwrap();
        publisher.finish().unwrap();
        let _ = palette.poll_arguments();
        let events = events.lock().unwrap();
        assert!(matches!(
            events.as_slice(),
            [maki_commands::CompletionLifecycleEvent::Highlight(item)]
                if item.label.as_ref() == "new"
        ));
        drop(events);
        release.send(()).unwrap();
    }

    #[test]
    fn snapshot_after_keyboard_navigation_highlights_new_selection() {
        let (mut palette, started, release, events) = gated_directory_palette();
        let input = "/cd ";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started.recv().unwrap();
        publisher
            .publish(
                ["first", "second"]
                    .into_iter()
                    .map(|label| CompletionItem {
                        label: Arc::from(label),
                        insertion: Arc::from(label),
                        description: None,
                    })
                    .collect(),
            )
            .unwrap();
        let _ = palette.poll_arguments();
        palette.argument_grid.set_layout(2, 2, 1);
        palette.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE), input);
        events.lock().unwrap().clear();

        publisher
            .publish(vec![CompletionItem {
                label: Arc::from("first"),
                insertion: Arc::from("first"),
                description: None,
            }])
            .unwrap();
        publisher.finish().unwrap();
        let _ = palette.poll_arguments();
        let events = events.lock().unwrap();
        assert!(matches!(
            events.as_slice(),
            [maki_commands::CompletionLifecycleEvent::Highlight(item)]
                if item.label.as_ref() == "first"
        ));
        drop(events);
        release.send(()).unwrap();
    }

    #[test]
    fn path_grid_navigation_notifies_highlight() {
        let (mut palette, started, release, events) = gated_directory_palette();
        let input = "/cd ";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started.recv().unwrap();
        publisher
            .publish(
                ["first", "second"]
                    .into_iter()
                    .map(|label| CompletionItem {
                        label: Arc::from(label),
                        insertion: Arc::from(label),
                        description: None,
                    })
                    .collect(),
            )
            .unwrap();
        let _ = palette.poll_arguments();
        palette.argument_grid.set_layout(2, 2, 1);
        events.lock().unwrap().clear();

        palette.handle_key(KeyEvent::new(KeyCode::Right, KeyModifiers::NONE), input);
        assert!(matches!(
            events.lock().unwrap().as_slice(),
            [maki_commands::CompletionLifecycleEvent::Highlight(item)]
                if item.label.as_ref() == "second"
        ));
        release.send(()).unwrap();
    }

    #[test]
    fn stale_directory_candidate_is_not_advanced_before_snapshot_poll() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let provider = Arc::new(GatedDirectoryCompletion {
            started: started_tx,
            release: Arc::new(Mutex::new(release_rx)),
            events: Arc::default(),
        });
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/cd"),
                    aliases: Arc::from([]),
                    arguments: CommandArguments::Positional(Arc::from([
                        PositionalArgument::optional("path", ArgumentKind::Directory)
                            .with_completion(CompletionPolicy::Replace),
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("Change directory"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Noop),
                argument_completions: vec![Some(provider)],
            }])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        let input = "/cd old";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started_rx.recv().unwrap();
        publisher
            .publish(vec![CompletionItem {
                label: Arc::from("old/"),
                insertion: Arc::from("old/"),
                description: None,
            }])
            .unwrap();
        let _ = palette.poll_arguments();
        assert_eq!(palette.argument_match_items()[0].insertion.as_ref(), "old/");

        publisher.publish(Vec::new()).unwrap();
        publisher.finish().unwrap();
        assert!(matches!(
            palette.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), input),
            super::CommandAction::Consumed
        ));
        assert!(matches!(
            palette.handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE), input),
            super::CommandAction::Consumed
        ));
        assert!(palette.argument_items[0].candidate.is_some());
        assert_eq!(input, "/cd old");
        release_tx.send(()).unwrap();
    }

    #[test]
    fn directory_descent_retains_rows_but_blocks_old_selection() {
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(2);
        let provider = Arc::new(GatedDirectoryCompletion {
            started: started_tx,
            release: Arc::new(Mutex::new(release_rx)),
            events: Arc::default(),
        });
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/cd"),
                    aliases: Arc::from([]),
                    arguments: CommandArguments::Positional(Arc::from([
                        PositionalArgument::optional("path", ArgumentKind::Directory)
                            .with_completion(CompletionPolicy::Replace),
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("Change directory"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Noop),
                argument_completions: vec![Some(provider)],
            }])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        let input = "/cd old";
        palette.sync_arguments(input, input.len(), "insert");
        let publisher = started_rx.recv().unwrap();
        publisher
            .publish(vec![CompletionItem {
                label: Arc::from("old/"),
                insertion: Arc::from("old/"),
                description: None,
            }])
            .unwrap();
        let _ = palette.poll_arguments();
        release_tx.send(()).unwrap();
        while palette.pending_arguments.is_some() {
            let _ = palette.poll_arguments();
            std::thread::yield_now();
        }

        let CommandAction::Complete { text, cursor } =
            palette.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), input)
        else {
            panic!("directory Tab should start descent");
        };
        palette.sync_arguments(&text, cursor, "insert");
        let _child_publisher = started_rx.recv().unwrap();

        assert!(palette.argument_publication.is_pending());
        assert_eq!(
            palette
                .argument_match_items()
                .into_iter()
                .map(|item| item.label.to_string())
                .collect::<Vec<_>>(),
            vec!["old/"]
        );
        assert!(
            palette
                .view_in_test(20, 7, DEFAULT_AUTOCOMPLETE_HEIGHT)
                .is_some()
        );
        for key_code in [KeyCode::Tab, KeyCode::Enter] {
            assert!(matches!(
                palette.handle_key(KeyEvent::new(key_code, KeyModifiers::NONE), &text),
                CommandAction::Consumed
            ));
        }
        release_tx.send(()).unwrap();
    }

    #[test_case(false; "required_scalar")]
    #[test_case(true; "required_variadic")]
    fn required_typed_command_is_discoverable_and_completable(variadic: bool) {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        let mut destination = PositionalArgument::required("destination", ArgumentKind::Directory);
        destination.variadic = variadic;
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/copy"),
                    aliases: Arc::from([]),
                    arguments: CommandArguments::Positional(Arc::from([
                        PositionalArgument::required("source", ArgumentKind::File),
                        destination,
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("Copy files"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Noop),
                argument_completions: vec![None, None],
            }])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        palette.sync("/cop");
        settle(&mut palette);
        assert_eq!(palette.filtered[0].command.invoked_name(), "/copy");
        assert!(matches!(
            palette.handle_key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE), "/cop"),
            CommandAction::Complete { text, .. } if text == "/copy "
        ));
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
        settle(&mut palette);

        assert_eq!(palette.filtered.len(), 1);
        assert_eq!(palette.filtered[0].command.invoked_name(), "/dynamic");
    }

    #[test]
    fn confirmation_uses_highlighted_command_over_exact_input() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![
                registration("/one", "Exact command"),
                registration("/oner", "Highlighted command"),
            ])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        palette.sync("/one");
        settle(&mut palette);
        assert_eq!(palette.selected_command().unwrap().invoked_name(), "/one");

        palette.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), "/one");

        assert_eq!(palette.selected_command().unwrap().invoked_name(), "/oner");
        assert_eq!(
            palette.confirm("/one").unwrap().command.invoked_name(),
            "/oner"
        );
    }

    #[test_case("/sessions \"two words\"", false; "initial_closed_quote")]
    #[test_case("/sessions \"two words", false; "initial_unfinished_quote")]
    #[test_case("/sessions \"two words\"", true; "settled_closed_quote")]
    #[test_case("/sessions \"two words", true; "settled_unfinished_quote")]
    fn quoted_scalar_argument_keeps_command_visible(input: &str, settle_command_first: bool) {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![Registration {
                spec: CommandSpec {
                    name: Arc::from("/sessions"),
                    aliases: Arc::from([]),
                    arguments: CommandArguments::Positional(Arc::from([
                        PositionalArgument::optional("query", ArgumentKind::String),
                    ])),
                    docs: CommandDocs {
                        summary: Arc::from("List sessions"),
                        argument_hint: None,
                    },
                    required_capabilities: TargetCapabilities::default(),
                },
                behavior: Arc::new(Noop),
                argument_completions: vec![None],
            }])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        if settle_command_first {
            palette.sync("/sessions");
            settle(&mut palette);
        }

        palette.sync(input);
        settle(&mut palette);

        assert_eq!(palette.filtered.len(), 1);
        assert_eq!(palette.filtered[0].command.invoked_name(), "/sessions");
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
        settle(&mut palette);
        assert!(palette.is_active());

        palette.sync("//model");
        assert!(!palette.is_active());
        assert!(palette.filtered.is_empty());
    }

    #[test_case(KeyCode::Enter ; "enter")]
    #[test_case(KeyCode::Tab ; "tab")]
    fn pending_command_without_published_rows_consumes_submission(key_code: KeyCode) {
        let registry = CommandRegistry::new();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        let request = CommandRequest {
            query: "model".into(),
            registry_generation: palette.snapshot.generation(),
            argument_count: 0,
        };
        let generation = palette.command_publication.begin(request.clone());
        palette.pending_command = Some((generation, request));

        assert!(matches!(
            palette.handle_key(KeyEvent::new(key_code, KeyModifiers::NONE), "/model"),
            CommandAction::Consumed
        ));
    }

    #[test_case(true ; "dismissed")]
    #[test_case(false ; "leading_slash_removed")]
    fn late_matcher_refresh_does_not_reopen_cleared_palette(close: bool) {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/model", "Switch model")])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);

        palette.sync("/model");
        settle(&mut palette);
        assert!(palette.is_active());
        palette
            .nucleo
            .pattern
            .reparse(0, "mod", CaseMatching::Ignore, Normalization::Smart, false);
        palette.command_matching = true;
        if close {
            palette.close();
        } else {
            palette.sync("model");
        }

        let deadline = std::time::Instant::now() + MATCHER_SETTLE_TIMEOUT;
        while palette.command_matching
            || palette.nucleo.snapshot().pattern().column_pattern(0).atoms
                != palette.nucleo.pattern.column_pattern(0).atoms
        {
            let _ = palette.tick();
            assert!(
                std::time::Instant::now() < deadline,
                "command matcher did not settle"
            );
            std::thread::sleep(MATCHER_SETTLE_POLL);
        }

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
        palette.sync("  /dynamic arg");
        settle(&mut palette);
        let confirmed = palette.confirm("  /dynamic arg").unwrap();

        producer
            .replace(vec![registration("/dynamic", "Second")])
            .unwrap();

        assert_eq!(confirmed.command.spec().docs.summary.as_ref(), "First");
        assert_eq!(confirmed.args, "arg");
    }

    #[test]
    fn registry_refresh_replaces_same_query_projection() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![registration("/dynamic", "First")])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);
        palette.sync("/dynamic");
        settle(&mut palette);
        assert_eq!(
            palette.filtered[0].command.spec().docs.summary.as_ref(),
            "First"
        );

        producer
            .replace(vec![registration("/dynamic", "Second")])
            .unwrap();
        palette.sync("/dynamic");
        settle(&mut palette);

        assert_eq!(palette.filtered.len(), 1);
        assert_eq!(
            palette.filtered[0].command.spec().docs.summary.as_ref(),
            "Second"
        );
        assert_eq!(
            palette
                .confirm("/dynamic")
                .unwrap()
                .command
                .spec()
                .docs
                .summary
                .as_ref(),
            "Second"
        );
    }

    #[test]
    fn settled_command_without_argument_context_clears_stale_rows() {
        let registry = CommandRegistry::new();
        let producer = registry.create_producer(ProducerPrecedence::Plugin);
        producer
            .replace(vec![
                registration("/deploy", "Deploy"),
                registration("/plain", "Plain"),
            ])
            .unwrap();
        let target = registry.bind_target(TargetCapabilities::default(), Arc::new(Noop));
        let mut palette = CommandPalette::new(registry, target);

        palette.sync("/deploy a");
        settle(&mut palette);
        palette.set_argument_completion(
            (8, 9),
            maki_lua::CommandArgumentItem {
                label: "old-result".into(),
                insertion: "old-result".into(),
                description: None,
            },
        );
        assert!(!palette.argument_items.is_empty());

        palette.sync("/plain value");
        settle(&mut palette);

        assert!(palette.argument_items.is_empty());
        assert!(palette.argument_range.is_none());
        assert!(matches!(
            palette.handle_key(
                KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE),
                "/plain value"
            ),
            CommandAction::Execute(confirmed) if confirmed.command.invoked_name() == "/plain"
        ));
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
        settle(&mut palette);

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
        settle(&mut palette);

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
    fn typed_argument_parser_rejects_later_prompt_lines() {
        let schema = [PositionalArgument::required("value", ArgumentKind::String)];
        let input = "/test first\nsecond";
        assert!(typed_argument_at_cursor(input, input.len(), &schema).is_none());
    }

    #[test]
    fn typed_argument_parser_limits_unfinished_quote_to_first_line() {
        let schema = [PositionalArgument::required(
            "path",
            ArgumentKind::Directory,
        )];
        let input = "/test \"pro\nkeep this text";
        let cursor = input.find("pro").unwrap() + 3;
        assert_eq!(
            typed_argument_at_cursor(input, cursor, &schema),
            Some((6, 10, "pro".into(), 0))
        );
    }

    #[test]
    fn completion_in_argument_gap_inserts_without_replacing_next_argument() {
        let schema = [
            PositionalArgument::required("source", ArgumentKind::String),
            PositionalArgument::required("destination", ArgumentKind::String),
        ];
        let input = "/copy  destination";
        let cursor = 6;
        let (start, end, query, index) = typed_argument_at_cursor(input, cursor, &schema).unwrap();
        assert_eq!((start, end, query.as_str(), index), (6, 6, "", 0));
        let edit = encode_completion_value("source", start..end, QuoteStyle::None);
        let completed = format!("{}{}{}", &input[..start], edit.text, &input[end..]);
        assert_eq!(completed, "/copy source destination");
        let parsed = CommandArguments::Positional(schema.into())
            .parse_invocation(command_args(&completed))
            .unwrap()
            .unwrap();
        assert_eq!(parsed.get("source").unwrap().as_str(), Some("source"));
        assert_eq!(
            parsed.get("destination").unwrap().as_str(),
            Some("destination")
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

    #[test_case(true; "accepted")]
    #[test_case(false; "dismissed")]
    fn completion_suppression_is_scoped_to_argument_slot(accepted: bool) {
        let mut palette = argument_palette(0);
        let input = "/test one two";
        let marker = Some((input.to_owned(), 6));
        if accepted {
            palette.accepted_argument_input = marker;
        } else {
            palette.dismissed_argument_input = marker;
        }

        assert!(palette.sync_arguments(input, input.len(), "insert"));
        assert!(palette.accepted_argument_input.is_none());
        assert!(palette.dismissed_argument_input.is_none());
    }

    #[test]
    fn command_palette_argument_navigation_wraps_up_and_down() {
        let mut palette = argument_palette(3);

        palette.handle_key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE), "/test ");
        assert_eq!(palette.argument_selected, 2);
        palette.handle_key(KeyEvent::new(KeyCode::Down, KeyModifiers::NONE), "/test ");
        assert_eq!(palette.argument_selected, 0);
    }

    #[test_case(KeyCode::Right, 1; "right")]
    #[test_case(KeyCode::Down, 4; "down_to_partial_last_row")]
    #[test_case(KeyCode::Left, 0; "left_clamps")]
    #[test_case(KeyCode::Up, 0; "up_clamps")]
    fn typed_path_grid_navigation_matches_shared_at_grid(key: KeyCode, expected: usize) {
        let mut palette = argument_palette(5);
        palette.set_typed_path_grid_for_test(ArgumentKind::Directory, 5);
        palette.argument_grid.set_layout(5, 2, 10);
        palette
            .argument_grid
            .set_selected(if key == KeyCode::Down { 2 } else { 0 });
        palette.handle_key(KeyEvent::new(key, KeyModifiers::NONE), "/test ");
        assert_eq!(palette.argument_selection(), expected);

        let mut at_grid = super::super::file_completion::CompletionGridState::default();
        at_grid.set_layout(5, 2, 10);
        at_grid.set_selected(if key == KeyCode::Down { 2 } else { 0 });
        assert!(at_grid.handle_key(&KeyEvent::new(key, KeyModifiers::NONE), 5, false));
        assert_eq!(palette.argument_selection(), at_grid.selected());
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
    fn typed_path_arguments_reuse_full_width_grid() {
        let mut palette = argument_palette(3);
        palette.typed_argument_owned = true;
        palette.argument_kind = Some(ArgumentKind::Directory);
        let mut terminal = Terminal::new(TestBackend::new(40, 20)).unwrap();
        let mut popup = None;
        terminal
            .draw(|frame| {
                popup = palette.view(frame, Rect::new(0, 10, 40, 1), DEFAULT_AUTOCOMPLETE_HEIGHT);
            })
            .unwrap();
        assert_eq!(popup.unwrap().width, 40);
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
