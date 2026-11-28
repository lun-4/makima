//! Actor-ordered facts for opt-in tools with an external policy controller.

use std::collections::VecDeque;

use makima_storage::id::MakimaId;
use serde::Serialize;

use crate::tools::RegisteredTool;
use crate::{CancelToken, DoneReason, TurnId, TurnOutcome};

use super::{ActorQueue, ActorState};

const MAX_FACTS: usize = 256;

#[derive(Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ToolFact {
    Exposed { turn: TurnId, scope: MakimaId },
    Settled { turn: TurnId, outcome: &'static str },
    Stopped { reason: &'static str },
}

#[derive(Debug, Clone, Copy)]
pub enum ToolLifecycleTrigger {
    Request,
    Facts,
    Idle,
    Closing,
}

impl ToolLifecycleTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::Facts => "facts",
            Self::Idle => "idle",
            Self::Closing => "closing",
        }
    }
}

#[derive(Clone)]
pub struct ToolIdleFence {
    pub(crate) name: String,
    pub(crate) revision: MakimaId,
    pub(crate) policy_generation: u64,
    pub(crate) history_epoch: u64,
}

pub struct ToolTransition {
    pub facts: Vec<ToolFact>,
    pub latest: Option<TurnId>,
    pub plan: bool,
    pub faulted: bool,
    pub closing: bool,
    pub(crate) policy_generation: u64,
    pub(crate) history_epoch: u64,
    pub(crate) reservation: MakimaId,
}

pub(crate) struct ToolMailbox {
    pub(crate) entry: RegisteredTool,
    pub(crate) owner: CancelToken,
    pub(crate) reservation: Option<MakimaId>,
    pub(crate) revision: Option<MakimaId>,
    pub(crate) scope: Option<MakimaId>,
    pub(crate) open: bool,
    pub(crate) closing: bool,
    pub(crate) faulted: bool,
    pub(crate) facts: VecDeque<ToolFact>,
    pub(crate) nudged: bool,
    pub(crate) last_exposure: Option<(TurnId, MakimaId)>,
    pub(crate) idle_notified: Option<TurnId>,
}

impl ToolMailbox {
    pub(crate) fn new(entry: RegisteredTool, owner: CancelToken) -> Self {
        Self {
            entry,
            owner,
            reservation: None,
            revision: None,
            scope: None,
            open: false,
            closing: false,
            faulted: false,
            facts: VecDeque::new(),
            nudged: false,
            last_exposure: None,
            idle_notified: None,
        }
    }

    pub(crate) fn accepts(&self, revision: MakimaId) -> bool {
        self.open
            && !self.closing
            && !self.faulted
            && !self.owner.is_cancelled()
            && self.reservation.is_none()
            && self.revision == Some(revision)
    }

    pub(crate) fn push(&mut self, fact: ToolFact) {
        if self.closing || self.faulted || self.owner.is_cancelled() {
            return;
        }
        if self.facts.len() == MAX_FACTS {
            self.faulted = true;
            self.open = false;
            self.facts.clear();
        } else {
            self.facts.push_back(fact);
        }
    }
}

impl ActorState {
    pub(super) fn tool_settled(&mut self, turn: TurnId, outcome: &TurnOutcome, ran: bool) {
        let outcome = match outcome {
            TurnOutcome::Completed {
                reason: DoneReason::EndTurn,
                ..
            } => "finished",
            TurnOutcome::Completed {
                reason: DoneReason::MaxTurns,
                ..
            } => "max_turns",
            TurnOutcome::Completed {
                reason: DoneReason::MaxTokens,
                ..
            } => "max_tokens",
            TurnOutcome::Completed {
                reason: DoneReason::Compact,
                ..
            } => return,
            TurnOutcome::Failed { .. } => "failed",
            TurnOutcome::Cancelled { .. } if ran => "cancelled",
            // A withdrawn or cancelled queue entry did not run. Explicit
            // root-wide cancellation has its own cut, even when already idle.
            TurnOutcome::Cancelled { .. } => return,
        };
        for mailbox in self.tool_mailboxes.values_mut() {
            mailbox.push(ToolFact::Settled { turn, outcome });
        }
    }

    pub(super) fn stop_tools(&mut self, reason: &'static str) {
        for mailbox in self.tool_mailboxes.values_mut() {
            mailbox.open = false;
            mailbox.push(ToolFact::Stopped { reason });
        }
    }

    pub(crate) fn tool_wake_ready(&self, queue: &ActorQueue) -> bool {
        self.idle.is_none()
            && !self.runner_busy
            && !self.has_pending_work(queue)
            && (self.delivery_suspended || self.deliveries.is_empty())
    }

    pub(crate) fn matches_tool_idle_fence(&self, fence: &ToolIdleFence, epoch: u64) -> bool {
        self.policy_generation == fence.policy_generation
            && epoch == fence.history_epoch
            && self
                .tool_attachments
                .get(&fence.name)
                .is_some_and(|activation| {
                    activation.id == fence.revision && !activation.owner.is_cancelled()
                })
            && self
                .tool_mailboxes
                .get(&fence.name)
                .is_some_and(|mailbox| mailbox.accepts(fence.revision) && mailbox.facts.is_empty())
    }
}
