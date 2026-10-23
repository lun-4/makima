use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};

use maki_providers::{ImageSource, Message};

use super::{
    ActorInner, ActorLifecycle, AgentActorHandle, EffectiveAgentConfig, TurnProvenance, TurnTicket,
};
use crate::{AgentId, AgentInput, CurrentManagedTurn, TurnId};

pub const MAX_PENDING_INSTRUCTIONS: usize = 64;
pub const MAX_PENDING_INSTRUCTION_BYTES: usize = 256 * 1024;

/// Safe-boundary input source. All methods are synchronous: draining observations
/// and inserting them into history must be one no-await commit path.
pub trait TurnInbox: Send + Sync {
    fn drain_instructions(&self) -> Vec<AgentInput>;
    fn drain_presented_instructions(&self) -> Vec<(PendingSubmission, AgentInput)>;
    fn take_initial_submission(&self) -> Option<PendingSubmission> {
        None
    }
    fn drain_completions(&self) -> Vec<Message>;
    fn commit_completions(&self, commit: &mut dyn FnMut(Message)) {
        for message in self.drain_completions() {
            commit(message);
        }
    }
    /// Closes admission only when every accepted instruction reached a request
    /// and no child completion is waiting to be consumed.
    fn finish(&self) -> bool;
    /// Closes admission once every accepted instruction reached a request,
    /// leaving pending completions for a successor turn.
    fn finish_instructions(&self) -> bool;
    fn close(&self) {}
    /// Whether this turn's agent reports its results to a managed parent.
    fn is_managed_child(&self) -> bool {
        false
    }
    /// Resolves once no child that reports to this agent has unsettled work,
    /// yielding the turn permit while it waits. Roots resolve immediately,
    /// since their idle continuations already reach the user.
    fn join_children(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
    /// Acknowledge only previously drained instructions, after request submission.
    fn instructions_submitted(&self);
    fn has_unsubmitted_instructions(&self) -> bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubmissionId(pub u64);

#[derive(Clone)]
pub enum PendingQueueEntry {
    Message(PendingSubmission),
    Compact {
        operation_id: u64,
        run_id: u64,
        instructions: Option<String>,
    },
}

#[derive(Clone)]
pub struct PendingSubmission {
    pub id: SubmissionId,
    pub turn_id: TurnId,
    pub text: String,
    pub images: Vec<ImageSource>,
    pub already_displayed: bool,
    pub mcp_startup_notice: Option<usize>,
}

pub(super) struct InstructionEntry {
    pub presentation: PendingSubmission,
    pub input: AgentInput,
    pub provenance: TurnProvenance,
    pub bytes: usize,
}

pub(super) struct InstructionInbox {
    pub open: bool,
    pub ticket: TurnTicket,
    pub presentation: u64,
    pub pending: VecDeque<InstructionEntry>,
    pub drained: Vec<(TurnProvenance, usize)>,
    pub submit_operation: Option<u64>,
    pub policy: Option<Arc<EffectiveAgentConfig>>,
    pub initial: Option<PendingSubmission>,
    pub operation: u64,
    pub batchable: bool,
    pub correlations: Vec<String>,
}

impl InstructionInbox {
    pub fn new(ticket: TurnTicket, presentation: u64) -> Self {
        Self {
            open: true,
            ticket,
            presentation,
            pending: VecDeque::new(),
            drained: Vec::new(),
            submit_operation: None,
            policy: None,
            initial: None,
            operation: 0,
            batchable: false,
            correlations: Vec::new(),
        }
    }

    pub fn outstanding(&self) -> (usize, usize) {
        (
            self.pending.len() + self.drained.len(),
            self.pending.iter().map(|entry| entry.bytes).sum::<usize>()
                + self.drained.iter().map(|(_, bytes)| bytes).sum::<usize>(),
        )
    }
}

pub(super) struct ActorTurnInbox {
    pub inner: Weak<ActorInner>,
    pub turn_id: TurnId,
    pub managed_turn: Option<CurrentManagedTurn>,
}

impl ActorTurnInbox {
    pub fn source(
        inner: &Arc<ActorInner>,
        turn_id: TurnId,
        managed_turn: Option<CurrentManagedTurn>,
    ) -> Arc<dyn TurnInbox> {
        Arc::new(Self {
            inner: Arc::downgrade(inner),
            turn_id,
            managed_turn,
        })
    }

    fn unsettled_reporting_child(
        current: &CurrentManagedTurn,
        skipped: &[AgentId],
    ) -> Option<(AgentId, AgentActorHandle, TurnTicket)> {
        let manager = current.manager();
        current
            .node_snapshot()
            .ok()?
            .children
            .into_iter()
            .filter(|child_id| !skipped.contains(child_id))
            .find_map(|child_id| {
                let actor = manager.actor(child_id).ok()?;
                if !actor.reports_to_parent() {
                    return None;
                }
                Some((child_id, actor.clone(), actor.unsettled_ticket()?))
            })
    }

    fn close_admission(&self, await_completions: bool) -> bool {
        let Some(inner) = self.inner.upgrade() else {
            return true;
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let pending_completions = await_completions && !state.deliveries.is_empty();
        let Some(inbox) = state.instructions.get_mut(&self.turn_id) else {
            return true;
        };
        if inbox.outstanding().0 != 0 || pending_completions {
            return false;
        }
        inbox.open = false;
        true
    }
}

impl TurnInbox for ActorTurnInbox {
    fn drain_instructions(&self) -> Vec<AgentInput> {
        self.drain_presented_instructions()
            .into_iter()
            .map(|(_, input)| input)
            .collect()
    }

    fn take_initial_submission(&self) -> Option<PendingSubmission> {
        let inner = self.inner.upgrade()?;
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let submission = state.instructions.get_mut(&self.turn_id)?.initial.take()?;
        state.emit(super::ActorEvent::SubmissionConsumed(submission.clone()));
        Some(submission)
    }

    fn drain_presented_instructions(&self) -> Vec<(PendingSubmission, AgentInput)> {
        let Some(inner) = self.inner.upgrade() else {
            return Vec::new();
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Vec::new();
        }
        let Some(inbox) = state.instructions.get_mut(&self.turn_id) else {
            return Vec::new();
        };
        if !inbox.open {
            return Vec::new();
        }
        let mut inputs = Vec::with_capacity(inbox.pending.len());
        for entry in inbox.pending.drain(..) {
            inbox.drained.push((entry.provenance, entry.bytes));
            inputs.push((entry.presentation, entry.input));
        }
        for (submission, _) in &inputs {
            state.emit(super::ActorEvent::SubmissionConsumed(submission.clone()));
        }
        inputs
    }

    fn drain_completions(&self) -> Vec<Message> {
        let Some(inner) = self.inner.upgrade() else {
            return Vec::new();
        };
        super::delivery::drain(&inner, self.turn_id)
    }

    fn commit_completions(&self, commit: &mut dyn FnMut(Message)) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        super::delivery::commit(&inner, self.turn_id, commit);
    }

    fn finish(&self) -> bool {
        self.close_admission(true)
    }

    fn finish_instructions(&self) -> bool {
        self.close_admission(false)
    }

    fn is_managed_child(&self) -> bool {
        self.inner.upgrade().is_some_and(|inner| {
            inner.completion_policy == super::types::CompletionPolicy::JoinReportingChildren
        })
    }

    fn join_children(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let Some(current) = &self.managed_turn else {
                return;
            };
            if !self.is_managed_child() {
                return;
            }
            let mut skipped = Vec::new();
            while let Some((child_id, actor, ticket)) =
                Self::unsettled_reporting_child(current, &skipped)
            {
                let waited = match current
                    .lease()
                    .observe_descendant(current, child_id, &actor, ticket, None)
                {
                    Ok(wait) => wait.wait().await.is_ok(),
                    Err(_) => false,
                };
                if !waited {
                    skipped.push(child_id);
                }
            }
        })
    }

    fn close(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(inbox) = state.instructions.get_mut(&self.turn_id) {
            inbox.open = false;
        }
    }

    fn instructions_submitted(&self) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(inbox) = state.instructions.get_mut(&self.turn_id) {
            inbox.drained.clear();
        }
    }

    fn has_unsubmitted_instructions(&self) -> bool {
        let Some(inner) = self.inner.upgrade() else {
            return false;
        };
        let state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state
            .instructions
            .get(&self.turn_id)
            .is_some_and(|inbox| inbox.outstanding().0 != 0)
    }
}
