//! Focused actor pending-message presentation.

use crate::theme;
use maki_agent::actor::{
    AgentActorHandle, PendingQueueEntry, SubmissionId, TurnAdmissionOptions, TurnOrigin,
    TurnProvenance,
};
use maki_agent::{AgentId, AgentInput, AgentRef, EffectiveAgentConfig};
use maki_commands::COMPACT_COMMAND_NAME;
use std::borrow::Cow;
use std::sync::Arc;

use super::{Action, App, Status};

use crate::agent::shared_queue::{QueueItem, QueueSender, visible_pending};
use crate::components::queue_panel::QueueEntry;

pub(crate) use crate::agent::shared_queue::QueuedMessage;

pub(crate) const EMPTY_PROMPT_ERR: &str = "prompt is empty";
pub(crate) const NO_SUBAGENT_ERR: &str = "subagent is no longer accepting messages";

pub(crate) enum SubmitOutcome {
    Started(Vec<Action>),
    Queued,
    Rejected(String),
}

#[derive(Default)]
pub(crate) struct MessageQueue {
    shared: Option<QueueSender>,
    target: Option<AgentActorHandle>,
    target_id: Option<AgentId>,
    child_selected: bool,
    focused: Option<PendingIdentity>,
    cancelled_through: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingIdentity {
    Message(SubmissionId),
    Compact(u64),
}

fn pending_identity(entry: &PendingQueueEntry) -> PendingIdentity {
    match entry {
        PendingQueueEntry::Message(entry) => PendingIdentity::Message(entry.id),
        PendingQueueEntry::Compact { operation_id, .. } => PendingIdentity::Compact(*operation_id),
    }
}

fn panel_entry(entry: PendingQueueEntry) -> QueueEntry<'static> {
    match entry {
        PendingQueueEntry::Message(entry) => QueueEntry {
            text: Cow::Owned(entry.text),
            color: theme::current().foreground,
        },
        PendingQueueEntry::Compact { instructions, .. } => QueueEntry {
            text: instructions.map_or(Cow::Borrowed(COMPACT_COMMAND_NAME), |extra| {
                Cow::Owned(format!("{COMPACT_COMMAND_NAME} {extra}"))
            }),
            color: theme::current()
                .queue
                .fg
                .unwrap_or(theme::current().foreground),
        },
    }
}

impl MessageQueue {
    pub(crate) fn select(
        &mut self,
        id: Option<AgentId>,
        child: bool,
        actor: Option<AgentActorHandle>,
    ) {
        if self.target_id != id || self.child_selected != child {
            self.unfocus();
        }
        self.target_id = id;
        self.child_selected = child;
        self.target = actor;
    }

    pub(crate) fn set_shared(&mut self, shared: QueueSender) {
        self.shared = Some(shared);
    }

    pub(crate) fn fence_cancelled_presentations(&mut self, run_id: u64) {
        self.cancelled_through = self.cancelled_through.max(run_id);
    }

    pub(crate) fn accepts_actor_presentation(&self, run_id: u64) -> bool {
        run_id > self.cancelled_through
            && self
                .current_run_id()
                .is_some_and(|allocated| run_id <= allocated)
    }

    pub(crate) fn reserve_run_id(&self, displayed_run_id: u64) -> u64 {
        self.shared.as_ref().map_or(displayed_run_id + 1, |shared| {
            shared.reserve_run_id(displayed_run_id)
        })
    }

    pub(crate) fn current_run_id(&self) -> Option<u64> {
        self.shared.as_ref().map(QueueSender::current_run_id)
    }

    pub(crate) fn set_run_id(&self, run_id: u64) {
        if let Some(shared) = &self.shared {
            shared.set_run_id(run_id);
        }
    }

    fn entries(&self) -> Vec<PendingQueueEntry> {
        self.target.as_ref().map_or(Vec::new(), visible_pending)
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn len(&self) -> usize {
        self.entries().len()
    }

    fn remove(&self, index: usize) {
        let Some(actor) = &self.target else {
            return;
        };
        match self.entries().get(index).map(pending_identity) {
            Some(PendingIdentity::Message(id)) => {
                actor.remove_submission(id);
            }
            Some(PendingIdentity::Compact(operation_id)) => {
                actor.remove_pending_compact(operation_id);
            }
            None => {}
        }
    }

    pub(crate) fn clear_root_user_work(&mut self) {
        if let Some(ref shared) = self.shared {
            shared.clear_user_work();
        }
        if !self.child_selected {
            self.unfocus();
        }
    }

    #[cfg(test)]
    pub(crate) fn root_actor(&self) -> Option<&AgentActorHandle> {
        self.shared.as_ref().map(QueueSender::actor)
    }

    pub(crate) fn root_config(&self) -> Option<Arc<EffectiveAgentConfig>> {
        self.shared.as_ref()?.effective_config()
    }

    pub(crate) fn root_text_messages(&self) -> Vec<String> {
        self.shared
            .as_ref()
            .map_or(Vec::new(), QueueSender::text_messages)
    }

    pub(crate) fn focus(&self) -> Option<usize> {
        let identity = self.focused?;
        self.entries()
            .iter()
            .position(|entry| pending_identity(entry) == identity)
    }

    pub(crate) fn set_focus(&mut self) {
        self.set_focus_at(0);
    }

    pub(crate) fn unfocus(&mut self) {
        self.focused = None;
    }

    pub(crate) fn move_focus_up(&mut self) {
        if let Some(sel) = self.focus()
            && sel > 0
        {
            self.set_focus_at(sel - 1);
        }
    }

    pub(crate) fn move_focus_down(&mut self) {
        if let Some(sel) = self.focus() {
            self.set_focus_at(sel + 1);
        }
    }

    /// Removes the focused entry and moves focus to whatever now sits in its
    /// row, so repeated removals walk down the list.
    pub(crate) fn remove_focused(&mut self) {
        let Some(index) = self.focus() else {
            return;
        };
        self.remove(index);
        self.unfocus();
        let len = self.len();
        if len > 0 {
            self.set_focus_at(index.min(len - 1));
        }
    }

    pub(crate) fn pop_newest(&mut self) {
        if let Some(index) = self.len().checked_sub(1) {
            self.remove(index);
        }
    }

    pub(crate) fn panel_entries(&self) -> Vec<QueueEntry<'static>> {
        self.entries().into_iter().map(panel_entry).collect()
    }

    #[cfg(test)]
    pub(crate) fn text_messages(&self) -> Vec<String> {
        crate::agent::shared_queue::pending_texts(self.entries())
    }

    pub(crate) fn set_focus_at(&mut self, index: usize) {
        if let Some(identity) = self.entries().get(index).map(pending_identity) {
            self.focused = Some(identity);
        }
    }
}

impl App {
    /// Submits raw keyboard input to the focused actor after reference expansion.
    /// The caller handles slash commands, `exit`, and shell prefixes.
    pub(crate) fn submit_prompt(&mut self, msg: QueuedMessage) -> SubmitOutcome {
        if msg.text.trim().is_empty() && msg.images.is_empty() {
            return SubmitOutcome::Rejected(EMPTY_PROMPT_ERR.into());
        }
        // Expand `@prefix:value` tokens once, here, via the registered Lua
        // expanders. A rejected expander flashes and aborts the run. The
        // expanded text is stored so `build_agent_input` reuses it without
        // re-expanding.
        let expanded = match self.lua_event_handle.expand_references(&msg.text) {
            Ok(text) => text,
            Err(e) => return SubmitOutcome::Rejected(e),
        };
        let mut msg = msg;
        msg.text = expanded;
        self.submit_to_focused_actor(msg)
    }

    /// Keyboard path: nobody is around to receive an `Err`, so
    /// rejections flash on screen instead.
    pub(super) fn submit_or_queue(&mut self, msg: QueuedMessage) -> Vec<Action> {
        match self.submit_prompt(msg) {
            SubmitOutcome::Started(actions) => actions,
            SubmitOutcome::Queued => vec![],
            SubmitOutcome::Rejected(e) => {
                self.flash(e);
                vec![]
            }
        }
    }

    fn submit_to_focused_actor(&mut self, msg: QueuedMessage) -> SubmitOutcome {
        if self.active_subagent_closed() {
            return SubmitOutcome::Rejected(NO_SUBAGENT_ERR.into());
        }
        let Some(target) = self
            .command_runtime
            .target_agent(&self.command_target)
            .filter(|target| Some(target.id()) == self.selected_agent_id())
        else {
            return SubmitOutcome::Rejected(NO_SUBAGENT_ERR.into());
        };
        let input = self.build_agent_input(&msg);
        match self.admit_user_turn(&target, input, Some(msg.text)) {
            Ok(true) => SubmitOutcome::Started(Vec::new()),
            Ok(false) => SubmitOutcome::Queued,
            Err(error) => SubmitOutcome::Rejected(error),
        }
    }

    /// Admits a user turn to `target` and reports whether it started a root
    /// run. Typed input (`display`) is submitted so it can fold into an active
    /// turn, and a starting one is drawn at once; command turns are admitted
    /// as their own turn and drawn on consumption. A starting root turn is
    /// admitted under the presentation reserved here, so the app records the
    /// id the actor stamps.
    pub(super) fn admit_user_turn(
        &mut self,
        target: &AgentRef,
        mut input: AgentInput,
        display: Option<String>,
    ) -> Result<bool, String> {
        let actor = target
            .validate_live()
            .and_then(|()| target.actor())
            .map_err(|error| error.to_string())?;
        let root = self.chats[0].agent_id == Some(target.id());
        input.preamble = self.shell.results_for(Some(target.id())).to_vec();
        if root {
            input
                .preamble
                .extend_from_slice(self.shell.results_for(None));
        }
        let starting = root && self.status != Status::Streaming;
        let reserved = starting.then(|| actor.reserve_presentation(self.run_id));
        let images = input.images.clone();
        let options = TurnAdmissionOptions {
            provenance: TurnProvenance {
                origin: TurnOrigin::User,
                ..Default::default()
            },
            already_displayed: starting && display.is_some(),
            displayed_presentation: reserved,
            ..Default::default()
        };
        let landed = if display.is_some() {
            actor
                .submit_presented(input, None, String::new(), options)
                .map(|(_, presentation)| reserved.and(Some(presentation)))
        } else {
            actor
                .admit_turn_with_options(input, None, String::new(), options)
                .map(|_| reserved)
        }
        .map_err(|error| error.to_string())?;
        self.shell.drain_results_for(Some(target.id()));
        if root {
            self.shell.drain_results();
        }
        if let Some(run_id) = landed {
            self.record_run_start(run_id);
            if let Some(text) = display {
                self.main_chat().show_user_message(text, images);
            }
        }
        Ok(starting)
    }

    /// Deferred path: the agent is busy, so park the message and let
    /// `QueueItemConsumed` draw it once the agent picks it up. Returns
    /// false when there is no shared queue, meaning the message was dropped.
    pub(super) fn queue_and_notify(&mut self, msg: QueuedMessage) -> bool {
        let Some(ref shared) = self.queue.shared else {
            return false;
        };
        let input = self.build_agent_input(&msg);
        shared
            .try_push(QueueItem {
                input,
                displayed: None,
            })
            .is_ok()
    }

    /// Push restored queue items only here, never in `restore_display`, which
    /// runs before the loaded or rewound history is committed.
    pub(crate) fn flush_restored_queue(&mut self) {
        // The live queue owns the text from here on, so the recovery
        // snapshot must stop overriding it on save.
        self.recoverable_queue.clear();
        // Read, not taken: the live queue is what the next checkpoint mirrors
        // back into the session, so emptying it here changes nothing on disk.
        for text in self.state.session.meta.queued_messages.clone() {
            self.queue_and_notify(QueuedMessage {
                text,
                images: Vec::new(),
            });
        }
    }

    pub(crate) fn start_mailbox_run(
        &mut self,
        preamble: Vec<maki_providers::Message>,
    ) -> Vec<Action> {
        let mut input = self.build_agent_input(&QueuedMessage {
            text: String::new(),
            images: Vec::new(),
        });
        input.preamble = preamble;
        self.start_run(input, String::new())
    }

    /// The one place a fresh run starts: every path that emits
    /// `Action::SendMessage` must go through here so `run_id` bumps exactly
    /// once per run.
    pub(super) fn start_run(&mut self, input: AgentInput, display: String) -> Vec<Action> {
        self.record_run_start(self.queue.reserve_run_id(self.run_id));
        if !display.is_empty() || !input.images.is_empty() {
            self.main_chat()
                .show_user_message(display, input.images.clone());
        }
        vec![Action::SendMessage(Box::new(input))]
    }

    pub(super) fn record_run_start(&mut self, run_id: u64) {
        self.run_id = run_id;
        self.queue.set_run_id(run_id);
        self.clear_exit_request();
        self.stamped_subagent_outcomes.clear();
        // New work supersedes text held for recovery after an agent error.
        self.recoverable_queue.clear();
        self.status = Status::Streaming;
        // The turn runs on whatever the model is now; a change after this
        // point lands on the next turn.
        self.run_model = Some(self.state.session.model.clone());
        self.run_context_window = Some(self.root_config().model.context_window);
    }
}
