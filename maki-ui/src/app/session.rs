use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use crate::chat::{Chat, DONE_TEXT, history_to_display};
use crate::components::DisplayRole;
use crate::components::rewind_picker::RewindEntry;
use crate::components::{Action, SessionReplacementKind, SessionReplacementRequest};
use maki_agent::HistorySnapshot;
use maki_agent::actor::HistoryChange;
use maki_agent::agent::estimate_message_tokens;
#[cfg(test)]
use maki_providers::Model;
use maki_storage::id::MakiId;
use maki_storage::sessions::{SessionMeta, StoredPost, StoredSubagent};

use crate::AppSession;
use maki_providers::RequestOptions;

#[cfg(test)]
use super::PendingInput;
use super::mode::Mode;
#[cfg(test)]
use super::session_state::SessionState;
use super::session_state::rules_to_stored;
#[cfg(test)]
use super::session_state::stored_to_rules;
use super::{App, Status};

/// The shortest gap between two writes that carry only UI state.
const SOFT_SAVE_DELAY: Duration = Duration::from_millis(1000);
const FLASH_AGENT_UNAVAILABLE: &str = "agent is no longer available";
const FLASH_REWIND_UNAVAILABLE: &str = "rewind selection is no longer available";

/// What `App::checkpoint` last handed to the writer: which session, how far
/// along it was, and when. The id is part of it because a session swapped into
/// the tab starts its revisions back at zero and would otherwise look older
/// than the stamp left by the one it replaced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Sent {
    pub id: MakiId,
    pub revision: u64,
    pub content_revision: u64,
    pub at: Instant,
}

/// The one content check: `App::checkpoint` saves a session only when this
/// holds, and the shutdown report reuses it to say which tabs were saved, so
/// the report and the disk can never disagree.
pub(crate) fn session_has_content(session: &AppSession) -> bool {
    !session.messages().is_empty()
        || session.meta.input_draft.is_some()
        || !session.meta.queued_messages.is_empty()
        || session.meta.mode != Some(maki_storage::sessions::StoredMode::Build)
}

impl App {
    pub(crate) fn has_content(&self) -> bool {
        session_has_content(&self.state.session)
    }

    /// The event loop runs this once per frame per session. It syncs whatever
    /// the session mirrors from live state, then writes only if a mutator
    /// really changed something. No dirty flags and no per-event save calls,
    /// so there is nothing left to forget.
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

        self.storage_writer.send(Arc::clone(&self.state.session));
        self.last_sent = Some(sent);
    }

    /// Everything the session mirrors from live state, built field by field so
    /// a new `SessionMeta` field forces a decision here. Every frame calls it,
    /// so it stays cheap: an idle UI has an empty draft, queue and rule list,
    /// and an empty `Vec` does not allocate.
    pub(super) fn build_meta(&self) -> SessionMeta {
        let state = &self.state;
        let config = self.root_config();
        let draft = self.input_box.buffer.value();
        SessionMeta {
            compacted: state.session.meta.compacted,
            mode: Some(Mode::from(&config.mode).into()),
            plan_path: state.plan.path().map(|p| p.to_string_lossy().into_owned()),
            plan_written: state.plan.is_ready(),
            session_rules: rules_to_stored(&self.permissions.session_rules_snapshot()),
            context_size: state.context_size,
            input_draft: (!draft.is_empty()).then_some(draft),
            queued_messages: if self.recoverable_queue.is_empty() {
                self.queue.root_text_messages()
            } else {
                self.recoverable_queue.clone()
            },
            thinking: Some(config.thinking.into()),
            fast: config.fast || state.fast_pending,
            workflow: config.workflow,
            yolo: self.permissions.persisted_yolo(),
            session_options: Default::default(),
        }
    }

    /// Called where the set of subagent tabs changes, not at checkpoint time:
    /// the turn-end path clears `chat_index` right after pruning it, so a later
    /// rebuild would only ever find an empty map.
    pub(super) fn sync_subagents(&mut self) {
        let mut ordered: Vec<_> = self.chat_index.iter().collect();
        ordered.sort_by_key(|&(_, chat_index)| chat_index);
        let subagents = ordered
            .into_iter()
            .map(|(tool_id, &chat_index)| {
                let chat = &self.chats[chat_index];
                StoredSubagent {
                    tool_use_id: tool_id.clone(),
                    name: chat.name.clone(),
                    model: chat.model_id.clone(),
                    thinking: chat.opts.map(|o| o.thinking.into()),
                    fast: chat.opts.is_some_and(|o| o.fast),
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
        self.live_chat_index.clear();
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

        let (display_msgs, restore_items) = history_to_display(
            self.state.session.messages(),
            self.state.session.tool_outputs(),
            &self.ui_config.tool_output_lines,
            &posts_in(self.state.session.posts(), None),
        );
        self.main_chat().load_messages(display_msgs);
        let cost = self.state.cost;
        let context_size = self.state.context_size;
        let main = self.main_chat();
        main.cost = cost;
        main.context_size = context_size;
        if let Some(draft) = self.state.session.meta.input_draft.clone() {
            self.refresh_at_ref_labels(&draft);
            self.input_box.set_input(draft);
            self.input_box.buffer.move_to_end();
        }

        self.fire_restore_items(restore_items);

        // Read, not taken: the live chats below are the source `sync_subagents`
        // mirrors back, so emptying the session here would only make the next
        // checkpoint write the same list again.
        for sa in self.state.session.subagents().to_vec() {
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
            chat.model_id = sa.model;
            chat.opts = sa.thinking.map(|thinking| RequestOptions {
                thinking: thinking.into(),
                fast: sa.fast,
            });
            if let Some(messages) = self.state.session.subagent_messages().get(&sa.tool_use_id) {
                let (display, items) = history_to_display(
                    messages,
                    self.state.session.tool_outputs(),
                    &self.ui_config.tool_output_lines,
                    &posts_in(self.state.session.posts(), Some(&sa.tool_use_id)),
                );
                chat.load_messages(display);
                chat.mark_finished(DisplayRole::Done, DONE_TEXT);
                self.fire_restore_items(items);
            }
            self.chats.push(chat);
        }

        self.sync_subagents();

        let eh = &self.lua_event_handle;
        if eh.is_disconnected() {
            self.restoring
                .store(false, std::sync::atomic::Ordering::Relaxed);
        } else {
            eh.send_restore_complete(restoring);
        }
    }

    fn fire_restore_items(&self, items: Vec<maki_lua::RestoreItem>) {
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

    /// Resume at process start: the agent was already spawned with this
    /// history, so the restored queue must be flushed here.
    pub(crate) fn restore_resumed_session(&mut self) {
        self.permissions
            .set_session_yolo(self.state.session.meta.yolo);
        self.restore_display();
        self.flush_restored_queue();
        for w in self.state.warnings.drain(..) {
            self.status_bar.flash(w);
        }
    }

    pub(super) fn reset_session(&mut self) -> Vec<Action> {
        self.checkpoint_now();
        let ended_id = self.state.session.id;
        let mut session = AppSession::new(&self.state.session.model, &self.state.session.cwd);
        let config = self.root_config();
        session.meta.mode = Some(Mode::from(&config.mode).into());
        session.meta.session_rules = self.state.session.meta.session_rules.clone();
        session.meta.thinking = Some(config.thinking.into());
        session.meta.fast = config.fast || self.state.fast_pending;
        session.meta.workflow = config.workflow;
        session.meta.yolo = self.permissions.persisted_yolo();
        vec![Action::ReplaceSession(Box::new(
            SessionReplacementRequest {
                session,
                kind: SessionReplacementKind::Reset { ended_id },
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
        match result {
            Ok(()) => vec![],
            Err(msg) => {
                self.status_bar.flash(msg);
                vec![]
            }
        }
    }

    pub(super) fn rewind_to(&mut self, entry: RewindEntry) -> Vec<Action> {
        if let Some((target, boundary)) = self.rewind_picker.target_boundary(entry.turn_index) {
            return vec![Action::RewindAgent { target, boundary }];
        }
        self.flash(FLASH_REWIND_UNAVAILABLE.into());
        vec![]
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
        let (display, items) = history_to_display(
            &change.messages,
            self.state.session.tool_outputs(),
            &self.ui_config.tool_output_lines,
            &posts_in(self.state.session.posts(), chat.as_deref()),
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
        self.permissions.set_session_yolo(session.meta.yolo);
        self.permissions
            .load_session_rules(stored_to_rules(&session.meta.session_rules));
        self.state = SessionState::from_session(session, fallback_model, &self.model_policy);
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
