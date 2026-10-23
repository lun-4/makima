//! The app's handle on the root actor's scheduling queue.
//!
//! `push` translates a TUI [`QueueItem`] into an acknowledged ordinary
//! submission; reads project the actor's own queue. There is no second deque.

use std::sync::Arc;

use maki_agent::actor::{
    ActorError, AgentActorHandle, PendingQueueEntry, TurnAdmissionOptions, TurnOrigin,
    TurnProvenance,
};
use maki_agent::{AgentInput, EffectiveAgentConfig, ImageSource};
use tracing::warn;

use crate::components::input::Submission;

pub(crate) struct QueuedMessage {
    pub(crate) text: String,
    pub(crate) images: Vec<ImageSource>,
}

impl From<Submission> for QueuedMessage {
    fn from(sub: Submission) -> Self {
        Self {
            text: sub.text,
            images: sub.images,
        }
    }
}

pub(crate) struct QueueItem {
    pub(crate) input: AgentInput,
    /// The presentation the UI already drew the bubble under, which the turn
    /// keeps. `None` when the user typed while the agent was busy: the turn
    /// gets a fresh presentation and the UI draws on `QueueItemConsumed`.
    pub(crate) displayed: Option<u64>,
}

/// Queue facade shared with the app. Clones all reference the same actor.
#[derive(Clone)]
pub(crate) struct QueueSender {
    actor: Arc<AgentActorHandle>,
}

pub(crate) fn actor_queue(actor: Arc<AgentActorHandle>) -> QueueSender {
    QueueSender { actor }
}

/// The pending work a queue panel shows: compacts, and messages whose bubble
/// is not drawn yet, in FIFO order.
pub(crate) fn visible_pending(actor: &AgentActorHandle) -> Vec<PendingQueueEntry> {
    actor
        .pending_queue()
        .into_iter()
        .filter(|entry| match entry {
            PendingQueueEntry::Message(entry) => !entry.already_displayed,
            PendingQueueEntry::Compact { .. } => true,
        })
        .collect()
}

pub(crate) fn pending_texts(entries: Vec<PendingQueueEntry>) -> Vec<String> {
    entries
        .into_iter()
        .filter_map(|entry| match entry {
            PendingQueueEntry::Message(entry) => Some(entry.text),
            PendingQueueEntry::Compact { .. } => None,
        })
        .collect()
}

impl QueueSender {
    /// The presentation the app should treat as current; the actor owns the
    /// counter.
    pub(crate) fn current_run_id(&self) -> u64 {
        self.actor.presentation_id()
    }

    pub(crate) fn set_run_id(&self, displayed: u64) {
        self.actor.observe_presentation(displayed);
    }

    pub(crate) fn reserve_run_id(&self, displayed: u64) -> u64 {
        self.actor.reserve_presentation(displayed)
    }

    pub(crate) fn push(&self, entry: QueueItem) {
        if let Err(error) = self.try_push(entry) {
            warn!(error = %error, "agent queue push failed");
        }
    }

    pub(crate) fn try_push(
        &self,
        QueueItem { input, displayed }: QueueItem,
    ) -> Result<(), ActorError> {
        self.actor.submit(
            input,
            None,
            String::new(),
            TurnAdmissionOptions {
                provenance: TurnProvenance {
                    origin: TurnOrigin::User,
                    ..Default::default()
                },
                already_displayed: displayed.is_some(),
                displayed_presentation: displayed,
                ..Default::default()
            },
        )?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn actor(&self) -> &AgentActorHandle {
        &self.actor
    }

    pub(crate) fn effective_config(&self) -> Option<Arc<EffectiveAgentConfig>> {
        self.actor.effective_config()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.actor.pending_queue().is_empty()
    }

    /// Clears what the user queued while keeping child-result deliveries.
    pub(crate) fn clear_user_work(&self) {
        let _ = self.actor.clear_user_work();
    }

    pub(crate) fn text_messages(&self) -> Vec<String> {
        pending_texts(visible_pending(&self.actor))
    }
}
