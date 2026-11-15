use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use crate::chat::{Chat, DONE_TEXT, history_to_display_with_target};
use crate::components::DisplayRole;
use crate::components::rewind_picker::{RewindEntry, RewindSelection};
use crate::components::{Action, SessionReplacementKind, SessionReplacementRequest};
use makima_agent::actor::HistoryChange;
use makima_agent::agent::estimate_message_tokens;
use makima_agent::{AgentId, HistorySnapshot, ModeId};
#[cfg(test)]
use makima_providers::Model;
use makima_storage::id::MakimaId;
use makima_storage::session_state::StoredAgentLifecycle;
use makima_storage::sessions::{SessionMeta, StoredMode, StoredPost, StoredSubagent};

use crate::AppSession;
use makima_providers::RequestOptions;

use super::PendingInput;
#[cfg(test)]
use super::session_state::SessionState;
use super::{App, Status};
#[cfg(test)]
use makima_agent::permissions::PermissionSnapshot;

/// The shortest gap between two writes that carry only UI state.
const SOFT_SAVE_DELAY: Duration = Duration::from_millis(1000);
const FLASH_AGENT_UNAVAILABLE: &str = "agent is no longer available";
const FLASH_REWIND_UNAVAILABLE: &str = "rewind selection is no longer available";
const BRANCH_EXPIRED: &str = "branch_expired";
const DORMANT_AGENT_TEXT: &str = "Dormant";
const CLOSED_AGENT_TEXT: &str = "Closed";
const RETIRED_AGENT_TEXT: &str = "Retired";

/// What `App::checkpoint` last handed to the writer: which session, how far
/// along it was, and when. The id is part of it because a session swapped into
/// the tab starts its revisions back at zero and would otherwise look older
/// than the stamp left by the one it replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Sent {
    pub id: MakimaId,
    pub revision: u64,
    pub content_revision: u64,
    pub at: Instant,
}

/// The one content check: `App::checkpoint` saves a session only when this
/// holds, and the shutdown report reuses it to say which tabs were saved, so
/// the report and the disk can never disagree.
pub(crate) fn session_has_content(session: &AppSession) -> bool {
    !session.messages().is_empty()
        || !session.posts().is_empty()
        || session.meta.input_draft.is_some()
        || !session.meta.queued_messages.is_empty()
        || session
            .subagent_messages()
            .values()
            .any(|messages| !messages.is_empty())
        || session
            .agents()
            .values()
            .any(|record| record.parent.is_some())
        || session.latest_state().is_ok_and(|view| {
            view.scopes
                .values()
                .any(|scope| scope.plugins.values().any(|stored| stored.value.is_some()))
                || session
                    .root_agent_id()
                    .and_then(|root| view.scopes.get(&Some(root)))
                    .and_then(|scope| scope.config.as_ref())
                    .map_or_else(
                        || {
                            matches!(
                                session.meta.mode,
                                Some(StoredMode::Plan | StoredMode::Custom(_))
                            )
                        },
                        |config| config.mode.id != ModeId::Build.key(),
                    )
        })
}

impl App {
    pub(crate) fn has_content(&self) -> bool {
        session_has_content(&self.state.session)
    }

    /// The event loop runs this once per frame per session. It syncs whatever
    /// the session mirrors from live state, then writes only if a mutator
    /// really changed something. No dirty flags and no per-event save calls,
    /// so there is nothing left to forget.
    pub(crate) fn capture_restore_snapshot(&mut self) -> AppSession {
        self.sync_subagents();
        let meta = self.build_meta();
        AppSession::checkpoint(&mut self.state.session, None, meta, self.state.token_usage);
        self.state.session.as_ref().clone()
    }

    pub(crate) fn checkpoint(&mut self) {
        self.checkpoint_with(SOFT_SAVE_DELAY);
    }

    /// A checkpoint for the paths that get no later frame, so a draft typed a
    /// keystroke ago still reaches disk: shutdown, and swapping the session out
    /// from under the tab.
    pub(crate) fn checkpoint_now(&mut self) {
        self.checkpoint_with(Duration::ZERO);
    }

    pub(crate) fn sync_live_children(&self) {
        self.storage_writer
            .set_live_children(self.state.session.id, self.live_child_keys());
    }

    fn live_child_keys(&self) -> Vec<String> {
        self.chats
            .iter()
            .filter(|chat| {
                chat.agent_id.is_some_and(|id| {
                    self.subagent_channels
                        .get(&id)
                        .is_some_and(|channels| !channels.closed)
                })
            })
            .filter_map(|chat| chat.subagent_id.clone())
            .collect()
    }

    pub(super) fn checkpoint_with(&mut self, soft_delay: Duration) {
        if self.restore_in_progress {
            return;
        }
        self.sync_live_children();
        let snapshot = self.shared_history.as_ref().map(|h| h.load_full());
        let meta = self.build_meta();
        AppSession::checkpoint(
            &mut self.state.session,
            snapshot.as_deref(),
            meta,
            self.state.token_usage,
        );

        if !self.has_content() {
            // A draft typed and then deleted is already on disk, and a file with
            // nothing in it is a session the picker still offers to resume. Idle
            // only: submitting empties the draft a frame before the agent mirrors
            // the prompt back, and that gap is not an abandoned session.
            let id = self.state.session.id;
            if self.status == Status::Idle && self.last_sent.take_if(|last| last.id == id).is_some()
            {
                self.storage_writer.delete_empty(id);
            }
            return;
        }
        let session = &self.state.session;
        let sent = Sent {
            id: session.id,
            revision: session.revision(),
            content_revision: session.content_revision(),
            at: Instant::now(),
        };
        if let Some(last) = &self.last_sent
            && last.id == sent.id
        {
            if last.revision == sent.revision {
                return;
            }
            // Only UI state moved: a keystroke in the draft, the queue or a
            // session rule. Each one costs a meta record plus an fsync, so they
            // land at most once per `soft_delay`, which bounds what a crash
            // takes with it. Anything the agent produced skips the wait.
            if last.content_revision == sent.content_revision && last.at.elapsed() < soft_delay {
                return;
            }
        }

        self.storage_writer
            .send_fenced(Arc::clone(&self.state.session), &self.storage_fence);
        self.last_sent = Some(sent);
    }

    /// Presentation metadata, with legacy Config and Rules fields left neutral.
    pub(super) fn build_meta(&self) -> SessionMeta {
        let state = &self.state;
        let draft = self.input_box.buffer.value();
        SessionMeta {
            compacted: state.session.meta.compacted,
            conversation: state.session.meta.conversation.clone(),
            mode: None,
            session_rules: Vec::new(),
            context_size: state.context_size,
            input_draft: (!draft.is_empty()).then_some(draft),
            queued_messages: if self.recoverable_queue.is_empty() {
                self.queue.root_text_messages()
            } else {
                self.recoverable_queue.clone()
            },
            thinking: None,
            fast: false,
            workflow: false,
            yolo: None,
            session_options: Default::default(),
        }
    }

    /// Called where the set of subagent tabs changes, not at checkpoint time:
    /// the turn-end path clears `chat_index` right after pruning it, so a later
    /// rebuild would only ever find an empty map.
    pub(super) fn sync_subagents(&mut self) {
        let view = self.state.session.latest_state().unwrap_or_else(|error| {
            tracing::warn!(%error, "could not fold child configuration");
            Default::default()
        });
        let mut ordered: Vec<_> = self.chat_index.iter().collect();
        ordered.sort_by_key(|&(_, chat_index)| chat_index);
        let subagents = ordered
            .into_iter()
            .map(|(tool_id, &chat_index)| {
                let chat = &self.chats[chat_index];
                let previous = self
                    .state
                    .session
                    .subagents()
                    .iter()
                    .find(|sa| sa.tool_use_id == *tool_id);
                let current_id = chat.agent_id.map(|id| id.to_string());
                let ambiguous = previous.is_some_and(|sa| {
                    sa.ambiguous
                        || (current_id.is_some()
                            && sa.agent_id != current_id
                            && (sa.agent_id.is_some()
                                || self.state.session.subagent_messages().contains_key(tool_id)))
                }) || self
                    .chats
                    .iter()
                    .filter(|chat| chat.subagent_id.as_deref() == Some(tool_id))
                    .count()
                    != 1;
                let agent_id = if ambiguous {
                    None
                } else {
                    current_id.or_else(|| previous.and_then(|sa| sa.agent_id.clone()))
                };
                let journal_backed = self.state.session.agents().values().any(|record| {
                    record.parent.is_some()
                        && record.transcript_key == *tool_id
                        && (!record.legacy
                            || view
                                .scopes
                                .get(&Some(record.id))
                                .is_some_and(|scope| scope.config.is_some()))
                });
                let legacy = previous.filter(|_| !journal_backed);
                StoredSubagent {
                    tool_use_id: tool_id.clone(),
                    agent_id,
                    ambiguous,
                    name: chat.name.clone(),
                    model: legacy.and_then(|sa| sa.model.clone()),
                    thinking: legacy.and_then(|sa| sa.thinking),
                    fast: legacy.is_some_and(|sa| sa.fast),
                }
            })
            .collect();
        self.state.session_mut().set_subagents(subagents);
    }

    pub(super) fn save_input_history(&self) {
        if let Err(e) = self.input_box.history().save(&self.storage) {
            tracing::warn!(error = %e, "input history save failed");
        }
    }

    #[cfg(test)]
    pub(super) fn reset_ui_chrome(&mut self) {
        let _ = self.cancel_middle_scroll();
        let splash_frame = self.chats.first().and_then(Chat::splash_frame).cloned();
        let root = self.chats.first().and_then(|chat| chat.agent_id);
        self.chats.clear();
        let mut main = Chat::new(
            "Main".into(),
            self.ui_config.clone(),
            self.lua_event_handle.clone(),
            Arc::clone(&self.theme_provider),
        );
        main.set_restore_channel(self.restore_event_tx.clone());
        main.set_splash_frame(splash_frame);
        main.agent_id = root;
        self.chats.push(main);
        self.active_chat = 0;
        self.chat_index.clear();
        self.agent_chat_index.clear();
        self.terminal_subagents.clear();
        self.status = super::Status::Idle;
        self.clear_exit_request();
        self.queue.clear_root_user_work();
        self.recoverable_queue.clear();
        self.close_all_overlays();
        self.pending_input = PendingInput::None;
        self.status_bar.clear_flash();
        self.task_picker_original = None;
        self.last_esc = None;
        self.restoring = Arc::new(AtomicBool::new(false));
        self.plan_form.reset();
        self.plan_approval_pending = false;
    }

    pub(crate) fn restore_display(&mut self) {
        let restoring = Arc::new(AtomicBool::new(true));
        self.restoring = restoring.clone();

        let cost = self.state.cost;
        let context_size = self.state.context_size;
        let root_id = self.state.session.root_agent_id();
        let main = self.main_chat();
        main.agent_id = root_id.or(main.agent_id);
        main.cost = cost;
        main.context_size = context_size;
        if let Some(id) = main.agent_id {
            self.agent_chat_index.insert(id, 0);
        }
        if let Some(draft) = self.state.session.meta.input_draft.clone() {
            self.refresh_at_ref_labels(&draft);
            self.input_box.set_input(draft);
            self.input_box.buffer.move_to_end();
        }

        // Read, not taken: the live chats below are the source `sync_subagents`
        // mirrors back, so emptying the session here would only make the next
        // checkpoint write the same list again.
        let view = self.state.session.latest_state().unwrap_or_else(|error| {
            tracing::warn!(%error, "could not fold restored child configuration");
            Default::default()
        });
        let mut subagents = self.state.session.subagents().to_vec();
        for record in self
            .state
            .session
            .agents()
            .values()
            .filter(|record| record.parent.is_some())
        {
            if !subagents
                .iter()
                .any(|entry| entry.tool_use_id == record.transcript_key)
            {
                subagents.push(StoredSubagent {
                    tool_use_id: record.transcript_key.clone(),
                    agent_id: Some(record.id.to_string()),
                    ambiguous: false,
                    name: record
                        .label
                        .clone()
                        .unwrap_or_else(|| record.id.to_string()),
                    model: None,
                    thinking: None,
                    fast: false,
                });
            }
        }
        for sa in subagents {
            let idx = self.chats.len();
            self.chat_index.insert(sa.tool_use_id.clone(), idx);
            let mut chat = Chat::new(
                sa.name,
                self.ui_config.clone(),
                self.lua_event_handle.clone(),
                Arc::clone(&self.theme_provider),
            );
            chat.set_restore_channel(self.restore_event_tx.clone());
            chat.subagent_id = Some(sa.tool_use_id.clone());
            let mut records = self.state.session.agents().values().filter(|record| {
                record.parent.is_some() && record.transcript_key == sa.tool_use_id
            });
            let record = records
                .next()
                .filter(|_| !sa.ambiguous && records.next().is_none());
            chat.agent_id = record
                .map(|record| record.id)
                .or_else(|| sa.agent_id.as_deref().and_then(|id| id.parse().ok()));
            chat.parent_agent_id = record.and_then(|record| record.parent);
            if let Some(id) = chat.agent_id {
                self.agent_chat_index.insert(id, idx);
            }
            if let Some(config) = record
                .and_then(|record| view.scopes.get(&Some(record.id)))
                .and_then(|scope| scope.config.as_ref())
            {
                chat.model_id = Some(config.model.clone());
                chat.opts = Some(RequestOptions {
                    thinking: config.thinking.map(Into::into).unwrap_or_default(),
                    fast: config.fast,
                });
            } else {
                chat.model_id = sa.model;
                chat.opts = sa.thinking.map(|thinking| RequestOptions {
                    thinking: thinking.into(),
                    fast: sa.fast,
                });
            }
            self.chats.push(chat);
        }

        self.sync_subagents();
        for chat in &mut self.chats {
            let target = chat.agent_id.and_then(|id| {
                self.root_agent
                    .as_ref()
                    .map(|root| root.manager().reference(id))
            });
            chat.set_restore_target(target);
        }

        let (display, items) = history_to_display_with_target(
            self.state.session.messages(),
            self.state.session.tool_outputs(),
            &self.ui_config.tool_output_lines,
            &posts_in(self.state.session.posts(), None),
            self.chats[0].agent_id.and_then(|id| {
                self.root_agent
                    .as_ref()
                    .map(|root| root.manager().reference(id))
            }),
        );
        self.main_chat().load_messages(display);
        self.fire_restore_items(items);

        for index in 1..self.chats.len() {
            let Some(key) = self.chats[index].subagent_id.as_deref() else {
                continue;
            };
            if let Some(messages) = self.state.session.subagent_messages().get(key) {
                let (display, items) = history_to_display_with_target(
                    messages,
                    self.state.session.tool_outputs(),
                    &self.ui_config.tool_output_lines,
                    &posts_in(self.state.session.posts(), Some(key)),
                    self.chats[index].agent_id.and_then(|id| {
                        self.root_agent
                            .as_ref()
                            .map(|root| root.manager().reference(id))
                    }),
                );
                self.chats[index].load_messages(display);
                self.fire_restore_items(items);
            }
            let status = self.chats[index]
                .agent_id
                .and_then(|id| self.state.session.agents().get(&id))
                .map_or(DONE_TEXT, |record| match record.lifecycle {
                    StoredAgentLifecycle::Open => DORMANT_AGENT_TEXT,
                    StoredAgentLifecycle::Closed => CLOSED_AGENT_TEXT,
                    StoredAgentLifecycle::Retired => RETIRED_AGENT_TEXT,
                });
            self.chats[index].mark_finished(DisplayRole::Done, status);
        }

        let eh = &self.lua_event_handle;
        if eh.is_disconnected() {
            self.restoring
                .store(false, std::sync::atomic::Ordering::Relaxed);
        } else {
            eh.send_restore_complete(restoring);
        }
    }

    fn fire_restore_items(&self, items: Vec<makima_lua::RestoreItem>) {
        let Some(tx) = &self.restore_event_tx else {
            return;
        };
        let eh = &self.lua_event_handle;
        let theme_gen = self.theme_provider.generation();
        for mut item in items {
            item.theme_gen = Some(theme_gen);
            eh.request_restore(item, tx.clone());
        }
    }

    pub(crate) fn restore_resumed_session(&mut self) {
        self.restore_display();
        for w in self.state.warnings.drain(..) {
            self.status_bar.flash(w);
        }
    }

    pub(super) fn reset_session(&mut self) -> Vec<Action> {
        self.checkpoint_now();
        let ended_id = self.state.session.id;
        let config = self.root_config();
        let session = AppSession::new(
            &config.model.spec(),
            &config.context.cwd().to_string_lossy(),
        );
        vec![Action::ReplaceSession(Box::new(
            SessionReplacementRequest {
                session,
                kind: SessionReplacementKind::Reset { ended_id },
                provider: None,
            },
        ))]
    }

    pub(super) fn open_rewind_picker(&mut self) -> Vec<Action> {
        let result = self
            .command_runtime
            .target_agent(&self.command_target)
            .filter(|target| Some(target.id()) == self.selected_agent_id())
            .ok_or_else(|| FLASH_AGENT_UNAVAILABLE.to_owned())
            .and_then(|target| self.rewind_picker.open_actor(target));
        let branches = self.state.session.list_branches_in(
            &self
                .storage
                .path()
                .join(makima_storage::sessions::SESSIONS_DIR),
        );
        let mut result = match branches {
            Ok(branches) => {
                let branches: Vec<_> = branches
                    .into_iter()
                    .filter(|entry| self.selected_agent_id() == Some(entry.branch.agent))
                    .collect();
                let has_branches = !branches.is_empty();
                if has_branches && self.rewind_picker.target().is_some() {
                    self.rewind_picker.append_branches(
                        branches
                            .into_iter()
                            .rev()
                            .map(|entry| (entry.branch.id, !entry.available)),
                    );
                    Ok(())
                } else {
                    result
                }
            }
            Err(error) => Err(error.to_string()),
        };
        if self.selected_agent_id() == self.state.session.root_agent_id()
            && let Some(link) = self.state.session.parent().cloned()
        {
            match AppSession::load(link.session, &self.storage)
                .map_err(|error| error.to_string())
                .and_then(|parent| {
                    makima_agent::parent_rewind::boundaries(&parent, &link)
                        .map_err(|error| error.to_string())
                }) {
                Ok(entries) => {
                    self.rewind_picker.append_parent(link.session, entries);
                    result = Ok(());
                }
                Err(error) => self.flash(error),
            }
        }
        match result {
            Ok(()) => vec![],
            Err(msg) => {
                self.status_bar.flash(msg);
                vec![]
            }
        }
    }

    pub(super) fn rewind_to(&mut self, entry: RewindEntry) -> Vec<Action> {
        if let RewindSelection::Parent { session, seq } = entry.selection {
            return vec![Action::RewindParent { session, seq }];
        }
        if let Some(target) = self.rewind_picker.target() {
            return match entry.selection {
                RewindSelection::Boundary { boundary, durable } => {
                    vec![Action::RewindAgent {
                        target,
                        boundary,
                        durable,
                    }]
                }
                RewindSelection::Parent { session, seq } => {
                    vec![Action::RewindParent { session, seq }]
                }
                RewindSelection::Branch { id, expired: false } => {
                    vec![Action::RestoreBranch { target, id }]
                }
                RewindSelection::Branch { expired: true, .. } => {
                    self.flash(BRANCH_EXPIRED.into());
                    vec![]
                }
            };
        }
        self.flash(FLASH_REWIND_UNAVAILABLE.into());
        vec![]
    }

    pub(crate) fn apply_full_restore(
        &mut self,
        session: AppSession,
        affected: &BTreeSet<AgentId>,
        target: AgentId,
        draft: Option<String>,
    ) {
        let selected = self.selected_agent_id();
        let input = self.input_box.buffer.value();
        let root = self.chats[0].agent_id;
        let root_affected = root.is_some_and(|id| affected.contains(&id));
        let mut previous = std::mem::take(&mut self.chats);
        let mut main = Chat::new(
            "Main".into(),
            self.ui_config.clone(),
            self.lua_event_handle.clone(),
            Arc::clone(&self.theme_provider),
        );
        main.agent_id = root;
        main.set_restore_channel(self.restore_event_tx.clone());
        self.chats.push(main);
        self.chat_index.clear();
        self.agent_chat_index.clear();
        self.terminal_subagents.retain(|id| !affected.contains(id));
        self.stamped_subagent_outcomes
            .retain(|(id, _)| !affected.contains(id));
        self.subagent_channels
            .retain(|id, _| !affected.contains(id));
        self.pending_input = PendingInput::None;
        self.state.token_usage = session.token_usage;
        self.state.context_size = session.meta.context_size;
        self.state.session = Arc::new(session);
        if root_affected {
            self.status = super::Status::Idle;
            self.clear_exit_request();
            self.clear_recoverable_queue();
            let config = self.root_config();
            self.state.adopt(&config);
        }
        self.close_all_overlays();
        self.restore_display();
        for chat in &mut self.chats {
            if chat.agent_id.is_some_and(|id| !affected.contains(&id))
                && let Some(index) = previous
                    .iter()
                    .position(|old| old.agent_id == chat.agent_id)
            {
                std::mem::swap(chat, &mut previous[index]);
            }
        }
        self.active_chat = selected
            .and_then(|id| self.agent_chat_index.get(&id).copied())
            .unwrap_or(0);
        let input = if self.selected_agent_id() == Some(target) {
            draft.unwrap_or(input)
        } else {
            input
        };
        self.refresh_at_ref_labels(&input);
        self.input_box.set_input(input);
        self.input_box.buffer.move_to_end();
        self.sync_focused_queue();
    }

    pub(crate) fn apply_history_change(&mut self, change: HistoryChange) {
        let Some(index) = self
            .chats
            .iter()
            .position(|chat| chat.agent_id == Some(change.agent_id))
        else {
            return;
        };
        let draft = change.draft.user_text().unwrap_or("").to_owned();
        let baseline = self.chats[index]
            .context_size
            .saturating_sub(if index == 0 {
                estimate_message_tokens(self.state.session.messages())
            } else {
                self.chats[index]
                    .subagent_id
                    .as_ref()
                    .and_then(|key| self.state.session.subagent_messages().get(key))
                    .map_or(0, |messages| estimate_message_tokens(messages))
            });
        let context_size = baseline.saturating_add(estimate_message_tokens(&change.messages));
        if index == 0 {
            let baseline = self
                .state
                .context_size
                .saturating_sub(estimate_message_tokens(self.state.session.messages()));
            self.state
                .session_mut()
                .replace_shared_messages(Arc::clone(&change.messages));
            self.state.context_size =
                baseline.saturating_add(estimate_message_tokens(&change.messages));
            self.state.session_mut().meta.input_draft = Some(draft.clone());
        } else if let Some(key) = self.chats[index].subagent_id.clone()
            && !self.state.session_mut().adopt_subagent_history(
                key,
                &HistorySnapshot {
                    epoch: change.epoch,
                    compacted: false,
                    conversation: None,
                    messages: Arc::clone(&change.messages),
                },
            )
        {
            return;
        }
        let chat = self.chats[index].subagent_id.clone();
        self.state
            .session_mut()
            .prune_posts(chat.as_deref(), change.messages.len());
        if index == 0 {
            let live_children = self.live_child_keys();
            self.state.session_mut().prune_orphans_preserving(
                |message| {
                    message
                        .tool_uses()
                        .map(|(id, _, _)| id.to_owned())
                        .collect()
                },
                &live_children,
            );
        }
        let (display, items) = history_to_display_with_target(
            &change.messages,
            self.state.session.tool_outputs(),
            &self.ui_config.tool_output_lines,
            &posts_in(self.state.session.posts(), chat.as_deref()),
            self.chats[index].agent_id.and_then(|id| {
                self.root_agent
                    .as_ref()
                    .map(|root| root.manager().reference(id))
            }),
        );
        self.chats[index].load_messages(display);
        self.chats[index].context_size = context_size;
        self.fire_restore_items(items);
        self.refresh_at_ref_labels(&draft);
        self.input_box.set_input(draft);
        self.input_box.buffer.move_to_end();
    }

    #[cfg(test)]
    pub(crate) fn apply_loaded_session(&mut self, session: AppSession, fallback_model: &Model) {
        self.checkpoint_now();
        self.rotate_command_target();
        let view = session.latest_state().unwrap();
        let snapshot = view
            .scopes
            .get(&None)
            .map_or_else(
                || PermissionSnapshot::from_stored(&session.meta.session_rules, session.meta.yolo),
                |scope| PermissionSnapshot::from_stored(&scope.rules, scope.yolo),
            )
            .unwrap();
        self.permissions.restore(snapshot);
        self.state = SessionState::from_session(
            session,
            fallback_model,
            &self.model_policy,
            &makima_providers::ProviderRegistry::default(),
        );
        for w in self.state.warnings.drain(..) {
            self.status_bar.flash(w);
        }
        self.reset_ui_chrome();
        self.restore_display();
        self.shared_history = None;
    }

    /// Applies an already-loaded session, for callers that loaded it
    /// themselves (e.g. to run their own checks first).
    #[cfg(test)]
    pub(crate) fn load_loaded_session(&mut self, session: AppSession) -> Vec<Action> {
        self.checkpoint_now();
        vec![Action::ReplaceSession(Box::new(
            SessionReplacementRequest {
                session,
                kind: SessionReplacementKind::Load,
                provider: None,
            },
        ))]
    }
}

fn posts_in<'a>(posts: &'a [StoredPost], chat: Option<&str>) -> Vec<&'a StoredPost> {
    posts
        .iter()
        .filter(|stored| stored.chat.as_deref() == chat)
        .collect()
}

#[cfg(test)]
mod tests {
    use makima_agent::actor::{ConfigChange, ConfigPatch};
    use makima_agent::permissions::PermissionSnapshot;
    use makima_agent::{AgentId, SubagentInfo};
    use makima_providers::{Effort, Message, ModelSelection, RequestOptions, ThinkingConfig};
    use makima_storage::session_state::{
        StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredAgentSpec, StoredModeCeiling,
        StoredModeSnapshot, StoredStateRecord,
    };
    use makima_storage::sessions::{StoredEffect, StoredRule, StoredSubagent, StoredThinking};

    use super::{CLOSED_AGENT_TEXT, DORMANT_AGENT_TEXT, RETIRED_AGENT_TEXT, session_has_content};

    const TRANSCRIPT: &str = "configured-child";
    const CONFIG_MODEL: &str = "provider/committed";
    const CACHE_MODEL: &str = "provider/cache";
    const SPAWN_MODEL: &str = "provider/spawn";

    fn stored_config() -> StoredAgentConfig {
        StoredAgentConfig {
            cwd: "/tmp".into(),
            model: CONFIG_MODEL.into(),
            thinking: Some(ThinkingConfig::Effort(Effort::High).into()),
            fast: true,
            workflow: true,
            mode: StoredModeSnapshot {
                id: "build".into(),
                label: "Build".into(),
                system_prompt: None,
                restrict_write_to: None,
                tools: None,
            },
            options: Default::default(),
        }
    }

    #[test]
    fn build_meta_leaves_live_config_and_rules_to_the_journal() {
        let mut app = super::super::tests::test_app();
        app.state.plan = super::super::mode::PlanState::Drafting(TRANSCRIPT.into());
        let actor = app.queue.root_actor().unwrap();
        smol::block_on(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    thinking: Some(ThinkingConfig::Effort(Effort::High)),
                    workflow: Some(true),
                    ..Default::default()
                }))),
        )
        .unwrap();
        let rules = vec![StoredRule {
            tool: "bash".into(),
            scope: None,
            effect: StoredEffect::Allow,
        }];
        app.permissions
            .restore(PermissionSnapshot::from_stored(&rules, Some(true)).unwrap());
        app.input_box.set_input(TRANSCRIPT.into());
        let before = app.state.session.state().to_vec();
        let meta = app.build_meta();
        assert!(meta.mode.is_none());
        assert!(meta.thinking.is_none());
        assert!(!meta.fast);
        assert!(!meta.workflow);
        assert!(meta.yolo.is_none());
        assert!(meta.session_rules.is_empty());
        assert!(meta.session_options.is_empty());
        assert_eq!(meta.input_draft.as_deref(), Some(TRANSCRIPT));
        assert_eq!(app.state.session.state(), before);
        assert_eq!(
            app.root_config().thinking,
            ThinkingConfig::Effort(Effort::High)
        );
        assert!(app.root_config().workflow);
        assert_eq!(app.permissions.snapshot().stored_rules(), rules);
        assert_eq!(app.permissions.snapshot().yolo, Some(true));
    }

    #[test_case::test_case(false, false; "folded_config")]
    #[test_case::test_case(false, true; "folded_default_thinking")]
    #[test_case::test_case(true, false; "ambiguous_cache")]
    fn restored_child_config_beats_cache_and_spawn(ambiguous: bool, default_thinking: bool) {
        let mut app = super::super::tests::test_app();
        let root = app
            .state
            .session
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        let child = AgentId::generate();
        let session = app.state.session_mut();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = TRANSCRIPT.into();
        record.spec = Some(StoredAgentSpec {
            model: ModelSelection {
                spec: Some(SPAWN_MODEL.into()),
                ..Default::default()
            },
            system: String::new(),
            tools: Vec::new(),
            audience: "all".into(),
            mcp: false,
            output_schema: None,
            name: None,
            silent: false,
            auto_deliver: false,
            inherit_provider: false,
            mode_ceiling: StoredModeCeiling {
                restrict_write_to: None,
                tools: None,
            },
            plugin: None,
        });
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        let mut config = stored_config();
        if default_thinking {
            config.thinking = None;
        }
        session.push_state(StoredStateRecord {
            seq: seq + 1,
            agent: Some(child),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        session.set_subagents(vec![StoredSubagent {
            tool_use_id: TRANSCRIPT.into(),
            agent_id: Some(child.to_string()),
            ambiguous,
            name: TRANSCRIPT.into(),
            model: Some(CACHE_MODEL.into()),
            thinking: Some(StoredThinking::Off),
            fast: false,
        }]);
        session.set_subagent_messages(TRANSCRIPT.into(), vec![Message::user(TRANSCRIPT.into())]);
        app.restore_display();
        let chat = &app.chats[1];
        assert_eq!(chat.agent_id, (!ambiguous).then_some(child));
        assert_eq!(
            chat.model_id.as_deref(),
            Some(if ambiguous { CACHE_MODEL } else { CONFIG_MODEL })
        );
        assert_eq!(
            chat.opts,
            Some(RequestOptions {
                thinking: if ambiguous || default_thinking {
                    ThinkingConfig::Off
                } else {
                    ThinkingConfig::Effort(Effort::High)
                },
                fast: !ambiguous,
            })
        );
        app.chats[1].model_id = Some(SPAWN_MODEL.into());
        app.chats[1].opts = Some(RequestOptions {
            thinking: ThinkingConfig::Off,
            fast: false,
        });
        app.sync_subagents();
        let cache = &app.state.session.subagents()[0];
        assert!(cache.model.is_none());
        assert!(cache.thinking.is_none());
        assert!(!cache.fast);
        assert_eq!(
            app.state.session.latest_state().unwrap().scopes[&Some(child)]
                .config
                .as_ref(),
            Some(&config)
        );
    }

    #[test]
    fn content_check_ignores_blank_root_but_keeps_folded_mode_and_children() {
        let mut session = crate::AppSession::new(CONFIG_MODEL, "/tmp");
        session.meta.mode = None;
        assert!(!session_has_content(&session));
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut config = stored_config();
        session.push_state(StoredStateRecord {
            seq: 1,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        assert!(!session_has_content(&session));
        config.mode.id = "plan".into();
        session.push_state(StoredStateRecord {
            seq: 2,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        assert!(session_has_content(&session));
        config.mode.id = "build".into();
        session.push_state(StoredStateRecord {
            seq: 3,
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config),
        });
        session.meta.mode = Some(super::StoredMode::Plan);
        assert!(!session_has_content(&session));
        session.set_subagent_messages(TRANSCRIPT.into(), vec![Message::user(TRANSCRIPT.into())]);
        assert!(session_has_content(&session));
    }

    #[test]
    fn reactivated_agent_reuses_restored_transcript_tab() {
        const TRANSCRIPT: &str = "reactivated-child";
        let mut app = super::super::tests::test_app();
        let root = app
            .state
            .session
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        let child = AgentId::generate();
        let session = app.state.session_mut();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = TRANSCRIPT.into();
        record.label = Some(TRANSCRIPT.into());
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        session.set_subagent_messages(TRANSCRIPT.into(), vec![Message::user(TRANSCRIPT.into())]);
        app.restore_display();
        let index = app.agent_chat_index[&child];
        let chat_count = app.chats.len();
        let info = SubagentInfo {
            agent_id: child,
            parent_agent_id: Some(root),
            parent_tool_use_id: TRANSCRIPT.into(),
            name: TRANSCRIPT.into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            cancel: None,
        };
        assert_eq!(app.resolve_or_create_chat(&info), index);
        assert_eq!(app.chats.len(), chat_count);
        assert_eq!(app.agent_chat_index[&child], index);
        assert!(!app.chats[index].is_finished());
    }

    #[test_case::test_case(StoredAgentLifecycle::Open, DORMANT_AGENT_TEXT; "dormant")]
    #[test_case::test_case(StoredAgentLifecycle::Closed, CLOSED_AGENT_TEXT; "closed")]
    #[test_case::test_case(StoredAgentLifecycle::Retired, RETIRED_AGENT_TEXT; "retired")]
    fn restored_graph_tabs_use_durable_identity_and_parent(
        lifecycle: StoredAgentLifecycle,
        expected: &str,
    ) {
        const TRANSCRIPT: &str = "restored-child";
        const CHILD_TEXT: &str = "stored child history";
        let mut app = super::super::tests::test_app();
        let root = app
            .state
            .session
            .root_agent_id()
            .unwrap_or_else(AgentId::generate);
        let child = AgentId::generate();
        let session = app.state.session_mut();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = TRANSCRIPT.into();
        record.lifecycle = lifecycle;
        record.label = Some(TRANSCRIPT.into());
        let seq = session.agent_revisions().last().unwrap().0 + 1;
        record.created_seq = seq;
        record.updated_seq = seq;
        session.push_agent_revision(seq, record);
        session.set_subagent_messages(TRANSCRIPT.into(), vec![Message::user(CHILD_TEXT.into())]);
        app.restore_display();
        assert_eq!(app.chats[0].agent_id, Some(root));
        let index = app.agent_chat_index[&child];
        assert_eq!(app.chats[index].agent_id, Some(child));
        assert_eq!(app.chats[index].parent_agent_id, Some(root));
        assert_eq!(app.chats[index].last_message_text(), expected);
        let info = SubagentInfo {
            agent_id: child,
            parent_agent_id: Some(root),
            parent_tool_use_id: TRANSCRIPT.into(),
            name: TRANSCRIPT.into(),
            prompt: None,
            model: None,
            opts: None,
            answer_tx: None,
            cancel: None,
        };
        app.push_post(
            Some(&info),
            makima_agent::Post {
                text: CHILD_TEXT.into(),
                level: Default::default(),
                agent: None,
            },
            None,
        );
        assert!(app.chats[index].is_finished());
        assert_eq!(app.chats[index].last_message_text(), CHILD_TEXT);
        assert_eq!(
            app.state.session.posts()[0].chat.as_deref(),
            Some(TRANSCRIPT)
        );
        assert_eq!(app.agent_chat_index[&root], 0);
        assert!(app.queue.is_empty());
    }
}
