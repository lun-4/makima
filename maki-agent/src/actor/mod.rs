//! Single-actor turn scheduler.
//!
//! One persistent actor owns a stable [`AgentId`], the [`History`], and a
//! lock-backed FIFO deque of work. It admits turns with an immediately
//! allocated [`TurnId`] ([`AgentActorHandle::admit_turn`] returns a
//! [`TurnTicket`] whose async wait is exact and cannot strand), runs them
//! strictly in order through an [`ActorBackend`], and retains the terminal
//! [`TurnOutcome`] of every admitted turn for lookup and exact waiting.
//!
//! Ordinary submissions share an eligible open turn or reserve a successor.
//! Exact admissions always reserve a separate turn. Controls and compaction
//! have no turn identity. Removed queued turns receive a terminal outcome.

mod actor_error;
mod config;
pub use config::{
    ConfigChange, ConfigCommit, ConfigPatch, ModeCeiling, PreparedModel, rediscovered_model,
};
pub(crate) mod delivery;
pub use delivery::{
    ChildDelivery, DeliveryEvent, DeliveryRejection, DeliveryStatus, MAX_DELIVERY_BATCH,
    MAX_DELIVERY_BATCH_BYTES, MAX_DELIVERY_PREVIEW_BYTES, MAX_PENDING_DELIVERIES,
    MAX_PENDING_PREVIEW_BYTES,
};
mod inbox;
pub use inbox::{MAX_PENDING_INSTRUCTION_BYTES, MAX_PENDING_INSTRUCTIONS, TurnInbox};
mod idle;
use idle::IdleReservation;
pub use idle::{ActorIdleGuard, ActorSourceSnapshot};
mod prepared;
pub use prepared::{
    PreparedCommit, PreparedFinalization, PreparedOperationTicket, PreparedPublication,
    PreparedTransaction, PreparedTransactionSnapshot, PreparedTurn,
};
mod queue;
mod runner;
mod tickets;
mod transcript;
pub use transcript::{TranscriptRequest, TranscriptSnapshot};
mod context;
mod types;
pub use context::AgentContextSnapshot;
mod history_edit;
pub use history_edit::{HistoryBoundary, HistoryChange, HistoryEditWork};

#[cfg(test)]
mod tests;

pub use actor_error::ActorError;
pub use inbox::{PendingQueueEntry, PendingSubmission, SubmissionId};
pub use queue::{ActorQueue, InterruptQueue, QueueProjection};
pub use tickets::{TurnOrigin, TurnOutput, TurnProvenance, TurnResult, TurnTicket};

pub const MAX_RETAINED_TURN_RESULTS: usize = 128;
#[cfg(any(test, feature = "test-util"))]
const QUIESCENCE_BARRIER_ROUNDS: usize = 128;

#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorRuntimeProbe {
    pub operations: usize,
    pub queued: usize,
    pub deliveries: usize,
    pub pending_publications: usize,
    pub retained_results: usize,
    pub processing: bool,
}

#[cfg(any(test, feature = "test-util"))]
impl ActorRuntimeProbe {
    pub fn is_quiescent(&self) -> bool {
        self.operations == 0
            && self.queued == 0
            && self.deliveries == 0
            && self.pending_publications == 0
            && !self.processing
    }
}

#[cfg(any(test, feature = "test-util"))]
pub async fn graph_quiescence_barrier(
    actors: &[AgentActorHandle],
) -> Result<Vec<ActorRuntimeProbe>, ActorError> {
    let mut previous = None;
    for _ in 0..QUIESCENCE_BARRIER_ROUNDS {
        let mut probes = Vec::with_capacity(actors.len());
        for actor in actors {
            probes.push(actor.runtime_barrier().await?);
        }
        if probes.iter().all(ActorRuntimeProbe::is_quiescent) && previous.as_ref() == Some(&probes)
        {
            return Ok(probes);
        }
        previous = Some(probes);
        smol::future::yield_now().await;
    }
    Err(ActorError::Busy)
}

pub type AdmissionPolicyConstraint =
    Arc<dyn Fn(&EffectiveAgentConfig) -> Result<(), String> + Send + Sync>;

#[derive(Clone, Default)]
pub struct TurnAdmissionOptions {
    pub idle_only: bool,
    pub existing_only: bool,
    pub after_turn: Option<TurnId>,
    pub provenance: TurnProvenance,
    pub policy_constraint: Option<AdmissionPolicyConstraint>,
    pub already_displayed: bool,
    pub mcp_startup_notice: Option<usize>,
    /// The presentation the frontend already associates with this submission.
    /// When set, admission uses it verbatim instead of allocating one, because
    /// the app has already drawn (or will draw) the bubble under that id.
    pub displayed_presentation: Option<u64>,
}

#[derive(Clone)]
pub enum ActorEvent {
    Start {
        agent_id: AgentId,
        turn_id: TurnId,
        provenance: TurnProvenance,
        config: Option<ConfigCommit>,
    },
    End(TurnResult),
    SubmissionConsumed(PendingSubmission),
    Delivery(DeliveryEvent),
    DeliveryContinuationFailed {
        parent_turn_id: TurnId,
        reason: Option<TurnCancellationReason>,
    },
    Idle {
        agent_id: AgentId,
        /// The work that settled last, or `None` when the actor went idle
        /// without running any.
        settled: Option<ActorWorkIdentity>,
    },
    Config(ConfigCommit),
    /// An idle compaction ended. Compaction inside a turn ends with the turn.
    CompactionEnd(CompactionStatus),
    Close {
        agent_id: AgentId,
        lifecycle: ActorLifecycle,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionStatus {
    Completed,
    Failed,
    Cancelled,
}

pub(crate) use types::ManagedTurnAdmission;
pub use types::{
    ActorBackend, ActorLifecycle, ActorSnapshot, ActorStatus, AdmissionPreparation, BackendResult,
    CompletionPolicy, ControlWork, EffectiveAgentConfig, PreparedReadiness, TurnAdmission,
    TurnContext, WorkKind,
};

use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use event_listener::Event;

use maki_providers::{Message, TokenUsage};
use tracing::info;

use crate::cancel::{CancelToken, ReasonedCancelToken, ReasonedCancelTrigger};
use crate::tools::offload::OffloadStore;
use crate::types::{
    ActorWorkIdentity, ActorWorkKind, AgentEvent, AgentId, EventSender, TurnCancellationReason,
    TurnId, TurnOutcome,
};
use crate::{AgentInput, AgentMode, CancelTrigger, History, ModeDef, RunSettings, SharedMessages};

/// One scheduler operation. Every admitted turn settles into one outcome;
/// controls and compaction do not produce turn outcomes.
pub enum ActorWork {
    Turn(Box<TurnAdmission>),
    HistoryEdit(HistoryEditWork),
    Control(ControlWork),
    Compact {
        operation_id: u64,
        run_id: u64,
        instructions: Option<String>,
        generation: u64,
        policy: Option<Arc<EffectiveAgentConfig>>,
        work: ActorWorkIdentity,
    },
}

impl ActorWork {
    /// The settlement identity for work that retires no turn. Turns record
    /// theirs when the outcome is finalized.
    pub(crate) fn identity(&self) -> Option<ActorWorkIdentity> {
        match self {
            Self::Control(control) => Some(control.work),
            Self::Compact { work, .. } => Some(*work),
            Self::Turn(_) | Self::HistoryEdit(_) => None,
        }
    }
    /// Every correlation targeted cancels can match this work by. Compacts
    /// re-derive theirs from the run id (see [`run_correlation`]), so they
    /// match cancels like run work but never retire precancel marks; see
    /// `ActorState::retire_superseded_precancels`.
    fn correlations(&self) -> Vec<String> {
        match self {
            Self::Turn(admission) => vec![admission.correlation.clone()],
            Self::HistoryEdit(_) => Vec::new(),
            Self::Control(control) => vec![control.correlation.clone()],
            Self::Compact { run_id, .. } => vec![run_correlation(*run_id)],
        }
    }
}

/// The mutable half of an actor, shared with every clone of the handle.
pub(crate) struct ActorInner {
    pub(crate) agent_id: AgentId,
    pub(crate) identity: Arc<()>,
    #[cfg(any(test, feature = "test-util"))]
    runtime_probes: Mutex<Vec<flume::Sender<ActorRuntimeProbe>>>,
    pub(crate) state: Mutex<ActorState>,
    pub(crate) policy_changed: Event,
    /// Notified each time the runner has run out of work.
    pub(crate) drained: Event,
    pub(crate) config_waiters: Mutex<VecDeque<Arc<()>>>,
    pub(crate) queue: Arc<ActorQueue>,
    pub(crate) outcomes: Mutex<HashMap<TurnId, TurnOutcome>>,
    pub(crate) tickets: Mutex<HashMap<TurnId, TurnTicket>>,
    retained_tickets: Mutex<HashMap<TurnId, TurnTicket>>,
    latest_admitted: Mutex<Option<TurnId>>,
    history: SharedMessages,
    coverage: Mutex<HashMap<TurnId, Option<(u64, usize)>>>,
    pub(crate) managed_admission: Option<ManagedTurnAdmission>,
    mode_ceiling: Option<config::ModeCeiling>,
    pub(crate) completion_policy: types::CompletionPolicy,
    offload: Option<Arc<OffloadStore>>,
    admission_preparation: Option<AdmissionPreparation>,
    prepared_readiness: Option<PreparedReadiness>,
    #[cfg(test)]
    pub(crate) after_pop: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    after_close_cut: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    after_work_execution: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    pub(crate) after_finalization_retire: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    before_snapshot_state: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    stale_preparation: Mutex<Option<flume::Sender<()>>>,
}

impl ActorInner {
    pub(super) fn validate_mode_config(
        &self,
        config: &EffectiveAgentConfig,
    ) -> Result<(), ActorError> {
        match &self.mode_ceiling {
            Some(ceiling) => ceiling.validate(&config.mode, config.mode_def.as_ref()),
            None => Ok(()),
        }
    }

    /// Registers a live ticket and records it as the most recently admitted turn.
    pub(crate) fn admit_ticket(&self, turn_id: TurnId, ticket: TurnTicket) {
        self.tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(turn_id, ticket);
        *self
            .latest_admitted
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(turn_id);
    }
}

pub(crate) struct ProcessingWork {
    turn_id: Option<TurnId>,
    correlations: Vec<String>,
    cancellation_reason: Option<TurnCancellationReason>,
}

impl ProcessingWork {
    fn new(work: &ActorWork) -> Self {
        let turn_id = match work {
            ActorWork::Turn(admission) => Some(admission.turn_id),
            _ => None,
        };
        Self {
            turn_id,
            correlations: work.correlations(),
            cancellation_reason: None,
        }
    }

    fn matches_correlation(&self, correlation: &str) -> bool {
        self.correlations.iter().any(|key| key == correlation)
    }
}

/// Lifecycle, run status, retained totals, and the active turn's
/// cancellation wiring. One lock keeps admission/close and snapshots of
/// lifecycle, status, queue, and latest outcome consistent.
/// `cancelled_correlations` remembers correlations cancelled before their
/// work was pushed (precancel), so a later matching push is dropped or
/// terminalized immediately. Marks for run correlations are retired once a
/// run starts; runs always push with the latest run id, so an older mark can
/// never match again.
pub(crate) struct ActorState {
    instructions: HashMap<TurnId, inbox::InstructionInbox>,
    pub(super) deliveries: HashMap<TurnId, delivery::PendingDelivery>,
    pub(super) delivery_order: VecDeque<TurnId>,
    pub(super) delivery_seen: VecDeque<TurnId>,
    pub(super) delivery_runs: HashMap<TurnId, Vec<TurnId>>,
    pub(super) delivery_suspended: bool,
    pub(super) parent_delivery_policy: Option<delivery::ParentDeliveryPolicy>,

    pub(crate) lifecycle: ActorLifecycle,
    close_pending: bool,
    pending_publications: usize,
    idle_pending: bool,
    /// The runner took work and has not run out yet; set across the whole
    /// batch, so a child's result is with its parent before this clears.
    runner_busy: bool,
    pub(crate) status: ActorStatus,
    pub(crate) active: Option<ActiveCancel>,
    pub(crate) processing: Option<ProcessingWork>,
    idle: Option<IdleReservation>,
    pub(crate) cancelled_correlations: HashMap<String, TurnCancellationReason>,
    pub(crate) cancellation_generation: u64,
    pub(crate) policy_generation: u64,
    pub(crate) policy: Option<Arc<EffectiveAgentConfig>>,
    latest: Option<TurnOutcome>,
    latest_history_turn: Option<(TurnId, u64, usize)>,
    retained_order: VecDeque<TurnId>,
    presentation: u64,
    usage: TokenUsage,
    operations: VecDeque<ActorOperation>,
    preparing: Option<u64>,
    prepared_publication: Option<prepared::PreparedPublicationState>,
    config_observers: Vec<flume::Sender<ConfigCommit>>,
    event_observers: Vec<flume::Sender<ActorEvent>>,
    graph_observers: Vec<(AgentId, flume::Sender<(AgentId, ActorEvent)>)>,
    next_operation_id: u64,
    next_submission_id: u64,
    /// Identity of the work that settled most recently, published with the
    /// idle notification so a frontend can fence the settlement against the
    /// presentation that actually produced it.
    settled: Option<ActorWorkIdentity>,
}

enum ActorOperation {
    Prepared(Box<prepared::PreparedOperation>),
    Control {
        id: u64,
        control: ControlWork,
    },
    Turn {
        id: u64,
        input: Box<AgentInput>,
        event_sender: Option<EventSender>,
        correlation: String,
        ticket: TurnTicket,
        policy_constraint: Option<AdmissionPolicyConstraint>,
        identity: ActorWorkIdentity,
    },
    Compact {
        id: u64,
        run_id: u64,
        instructions: Option<String>,
        presentation_id: u64,
    },
}

impl ActorOperation {
    fn id(&self) -> u64 {
        match self {
            Self::Prepared(pending) => pending.id,
            Self::Turn { id: after, .. }
            | Self::Control { id: after, .. }
            | Self::Compact { id: after, .. } => *after,
        }
    }

    fn projection(&self) -> Option<QueueProjection> {
        match self {
            Self::Prepared(pending) => pending
                .turn
                .as_ref()
                .map(|turn| QueueProjection::Turn(turn.correlation.clone())),
            Self::Control { control, .. } => Some(QueueProjection::Control(control.name.clone())),
            Self::Turn { correlation, .. } => Some(QueueProjection::Turn(correlation.clone())),
            Self::Compact { instructions, .. } => {
                Some(QueueProjection::Compact(instructions.clone()))
            }
        }
    }
}

fn lifecycle_error(lifecycle: ActorLifecycle) -> ActorError {
    match lifecycle {
        ActorLifecycle::Closed => ActorError::Closed,
        ActorLifecycle::Shutdown => ActorError::Shutdown,
        ActorLifecycle::Open => unreachable!(),
    }
}

fn settle_deferred(
    inner: &ActorInner,
    admissions: Vec<ActorOperation>,
    reason: TurnCancellationReason,
) {
    for admission in admissions {
        if let ActorOperation::Prepared(pending) = admission {
            pending.fail(ActorError::PolicyCancelled);
            inner.policy_changed.notify(usize::MAX);
            continue;
        }
        if let ActorOperation::Turn {
            input,
            event_sender,
            correlation,
            ticket,
            identity,
            ..
        } = admission
        {
            let turn_id = ticket.turn_id();
            let outcome = cancelled_outcome(inner.agent_id, turn_id, reason);
            let admission = TurnAdmission {
                turn_id,
                input: Some(*input),
                event_sender,
                correlation,
                work: identity,
                generation: 0,
                policy: None,
                admission: None,
                ticket,
            };
            finalize_turn(inner, turn_id, outcome, &admission, true);
        }
    }
    inner
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .publish_idle(inner);
}

fn drive_operations(inner: &Arc<ActorInner>, state: &mut ActorState) {
    if state.prepared_publication.is_some()
        || state.idle.as_ref().is_some_and(|idle| idle.committing)
    {
        return;
    }
    inner.policy_changed.notify(usize::MAX);
    state.idle_pending |= state.has_pending_work(&inner.queue);
    loop {
        if state.lifecycle != ActorLifecycle::Open {
            return;
        }
        let Some(head) = state.operations.front() else {
            state.preparing = None;
            return;
        };
        if state.idle.is_some()
            && !matches!(
                head,
                ActorOperation::Prepared(_) | ActorOperation::Turn { .. }
            )
        {
            return;
        }
        let id = head.id();
        if state.preparing == Some(id) {
            return;
        }
        state.preparing = None;
        if matches!(head, ActorOperation::Prepared(_)) {
            prepared::drive(inner, state);
            return;
        }
        if let ActorOperation::Turn { ticket, .. } = head
            && let Some(inbox) = state.instructions.get_mut(&ticket.turn_id())
        {
            inbox.policy = state.policy.clone();
        }
        if let ActorOperation::Turn {
            policy_constraint: Some(constraint),
            ..
        } = head
        {
            let constraint = Arc::clone(constraint);
            let policy = state.policy.clone();
            let inner = Arc::clone(inner);
            state.preparing = Some(id);
            smol::spawn(async move {
                let result = smol::unblock(move || {
                    catch_unwind(AssertUnwindSafe(|| {
                        let policy = policy
                            .ok_or_else(|| "actor configuration is not initialized".to_string())?;
                        constraint(&policy)
                    }))
                    .unwrap_or_else(|_| Err("admission policy constraint panicked".into()))
                })
                .await;
                let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.preparing != Some(id)
                    || state.operations.front().is_none_or(|work| work.id() != id)
                {
                    return;
                }
                state.preparing = None;
                match result {
                    Ok(()) => {
                        if let Some(ActorOperation::Turn {
                            policy_constraint, ..
                        }) = state.operations.front_mut()
                        {
                            *policy_constraint = None;
                        }
                        drive_operations(&inner, &mut state);
                    }
                    Err(diagnostic) => {
                        let work = state.operations.pop_front().unwrap();
                        state.pending_publications += 1;
                        drop(state);
                        fail_admission(&inner, work, diagnostic, false);
                        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
                        drive_operations(&inner, &mut state);
                    }
                }
            })
            .detach();
            return;
        }
        let input = match head {
            ActorOperation::Turn { input, .. } => Some(input),
            _ => None,
        };
        if let (Some(input), Some(prepare)) = (input, &inner.admission_preparation) {
            let policy = state.policy.clone();
            let input = preparation_input(input);
            let prepare = Arc::clone(prepare);
            let inner = Arc::clone(inner);
            state.preparing = Some(id);
            smol::spawn(async move {
                let snapshot = smol::unblock(move || {
                    catch_unwind(AssertUnwindSafe(|| {
                        let mut snapshot = prepare(&input, policy.as_deref());
                        if let Some(config) = policy
                            && let Some(mode_def) = &config.mode_def
                        {
                            snapshot.mode_def = Some(Arc::new(mode_def.clone()));
                        }
                        snapshot
                    }))
                })
                .await;
                let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.lifecycle != ActorLifecycle::Open
                    || state.preparing != Some(id)
                    || state.operations.front().is_none_or(|head| head.id() != id)
                {
                    #[cfg(test)]
                    if let Some(sender) = inner.stale_preparation.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    return;
                }
                state.preparing = None;
                let work = state.operations.pop_front().unwrap();
                match snapshot {
                    Ok(snapshot) => {
                        materialize(&inner, &mut state, work, Some(snapshot));
                        drive_operations(&inner, &mut state);
                    }
                    Err(_) => {
                        state.pending_publications += 1;
                        drive_operations(&inner, &mut state);
                        drop(state);
                        fail_preparation(&inner, work);
                    }
                }
            })
            .detach();
            return;
        }
        let work = state.operations.pop_front().unwrap();
        materialize(inner, state, work, None);
    }
}

fn fail_preparation(inner: &ActorInner, work: ActorOperation) {
    fail_admission(inner, work, "admission preparation panicked".into(), true);
}

fn fail_admission(inner: &ActorInner, work: ActorOperation, diagnostic: String, retryable: bool) {
    tracing::error!(agent_id = %inner.agent_id, %diagnostic, retryable, "turn admission failed");
    if let ActorOperation::Turn {
        input,
        event_sender,
        correlation,
        ticket,
        identity,
        ..
    } = work
    {
        let turn_id = ticket.turn_id();
        let admission = TurnAdmission {
            turn_id,
            input: Some(*input),
            event_sender,
            correlation,
            work: identity,
            ticket,
            generation: 0,
            policy: None,
            admission: None,
        };
        let outcome = TurnOutcome::failed(
            inner.agent_id,
            turn_id,
            TokenUsage::default(),
            0,
            crate::types::TurnFailure {
                kind: crate::types::TurnFailureKind::Internal,
                diagnostic,
                user_message: "The agent could not start this turn.".into(),
                retryable,
            },
        );
        finalize_turn(inner, turn_id, outcome, &admission, true);
    }
    let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
    state.pending_publications -= 1;
    state.finish_close(inner);
    state.publish_idle(inner);
}

fn submission_bytes(input: &AgentInput) -> Result<usize, ActorError> {
    Ok(input.message.len()
        + serde_json::to_vec(&input.preamble)
            .map_err(|_| ActorError::InvalidInstruction)?
            .len())
}

fn preparation_input(input: &AgentInput) -> AgentInput {
    AgentInput {
        message: input.message.clone(),
        images: input.images.clone(),
        preamble: input.preamble.clone(),
        prompt: input.prompt.clone(),
    }
}

fn materialize(
    inner: &ActorInner,
    state: &mut ActorState,
    work: ActorOperation,
    snapshot: Option<crate::agent::TurnAdmissionSnapshot>,
) {
    match work {
        ActorOperation::Turn {
            input,
            event_sender,
            correlation,
            ticket,
            identity,
            ..
        } => {
            if !state.delivery_runs.contains_key(&ticket.turn_id()) {
                state
                    .instructions
                    .entry(ticket.turn_id())
                    .or_insert_with(|| {
                        inbox::InstructionInbox::new(ticket.clone(), identity.presentation_id)
                    });
            }
            if state
                .instructions
                .get(&ticket.turn_id())
                .is_some_and(|inbox| inbox.initial.is_none())
            {
                let presentation = state.pending_submission(ticket.turn_id(), &input, false, None);
                state
                    .instructions
                    .get_mut(&ticket.turn_id())
                    .unwrap()
                    .initial = Some(presentation);
            }
            if let Some(inbox) = state.instructions.get_mut(&ticket.turn_id()) {
                inbox.policy = state.policy.clone();
                if let Some(initial) = &mut inbox.initial {
                    initial.mcp_startup_notice = initial.mcp_startup_notice.or(snapshot
                        .as_ref()
                        .and_then(|snapshot| snapshot.mcp_startup_notice));
                }
            }
            inner.queue.push(ActorWork::Turn(Box::new(TurnAdmission {
                turn_id: ticket.turn_id(),
                input: Some(*input),
                event_sender,
                correlation,
                work: identity,
                ticket,
                generation: state.policy_generation,
                policy: state.policy.clone(),
                admission: snapshot,
            })));
        }
        ActorOperation::Compact {
            id,
            run_id,
            instructions,
            presentation_id,
        } => inner.queue.push(ActorWork::Compact {
            operation_id: id,
            run_id,
            instructions,
            generation: state.policy_generation,
            policy: state.policy.clone(),
            work: ActorWorkIdentity::new(
                inner.agent_id,
                ActorWorkKind::Compact { operation_id: id },
                presentation_id,
            ),
        }),
        ActorOperation::Control { control, .. } => inner.queue.push(ActorWork::Control(control)),
        ActorOperation::Prepared(_) => unreachable!(),
    }
}

impl ActorState {
    fn check_instruction_capacity(&self, bytes: usize) -> Result<(), ActorError> {
        let (count, total) = self
            .instructions
            .values()
            .map(inbox::InstructionInbox::outstanding)
            .fold((0, 0), |(count, bytes), (n, b)| (count + n, bytes + b));
        if count >= MAX_PENDING_INSTRUCTIONS || total + bytes > MAX_PENDING_INSTRUCTION_BYTES {
            return Err(ActorError::InstructionOverflow);
        }
        Ok(())
    }

    /// Allocates the next presentation id, never reusing or going below the
    /// given floor.
    fn reserve_presentation(&mut self, displayed: u64) -> u64 {
        self.presentation = self.presentation.max(displayed) + 1;
        self.presentation
    }

    /// Uses the presentation the frontend already holds, or allocates one.
    /// The counter still moves past it so later work cannot reuse the id.
    fn adopt_presentation(&mut self, displayed: Option<u64>) -> u64 {
        match displayed {
            Some(displayed) => {
                self.presentation = self.presentation.max(displayed);
                displayed
            }
            None => self.reserve_presentation(0),
        }
    }

    fn pending_submission(
        &mut self,
        turn_id: TurnId,
        input: &AgentInput,
        already_displayed: bool,
        mcp_startup_notice: Option<usize>,
    ) -> PendingSubmission {
        self.next_submission_id += 1;
        PendingSubmission {
            id: SubmissionId(self.next_submission_id),
            turn_id,
            text: input.message.clone(),
            images: input.images.clone(),
            already_displayed,
            mcp_startup_notice,
        }
    }

    pub(super) fn publish_idle(&mut self, inner: &ActorInner) {
        if self.idle_pending
            && self.lifecycle == ActorLifecycle::Open
            && self.pending_publications == 0
            && !self.has_pending_work(&inner.queue)
            && inner
                .tickets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
        {
            self.idle_pending = false;
            self.emit(ActorEvent::Idle {
                agent_id: inner.agent_id,
                settled: self.settled,
            });
        }
    }

    pub(super) fn emit(&mut self, event: ActorEvent) {
        if let ActorEvent::Delivery(event) = &event {
            let delivery = &event.delivery;
            info!(
                child_id = %delivery.result.outcome.agent_id(),
                child_turn_id = %delivery.result.outcome.turn_id(),
                status = ?event.status,
                origin = ?delivery.result.provenance.origin,
                plugin = ?delivery.result.provenance.plugin,
                plugin_generation = ?delivery.result.provenance.plugin_generation,
                "child lifecycle delivery"
            );
        }
        self.event_observers
            .retain(|sender| sender.send(event.clone()).is_ok());
        self.graph_observers
            .retain(|(agent_id, sender)| sender.send((*agent_id, event.clone())).is_ok());
    }
    pub(super) fn finish_close(&mut self, inner: &ActorInner) {
        if self.close_pending
            && self.processing.is_none()
            && self.pending_publications == 0
            && inner
                .tickets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
        {
            self.close_pending = false;
            self.emit(ActorEvent::Close {
                agent_id: inner.agent_id,
                lifecycle: self.lifecycle,
            });
            self.event_observers.clear();
            self.graph_observers.clear();
            self.config_observers.clear();
        }
    }

    /// After Close, a new subscriber gets a disconnected channel rather than
    /// one that never receives anything.
    fn close_emitted(&self) -> bool {
        self.lifecycle != ActorLifecycle::Open && !self.close_pending
    }

    pub(crate) fn active_matches_correlation(&self, correlation: &str) -> bool {
        self.active.as_ref().is_some_and(|active| {
            active.correlation() == Some(correlation)
                || self
                    .processing
                    .as_ref()
                    .is_some_and(|processing| processing.matches_correlation(correlation))
        })
    }

    fn drain_work(&mut self) -> Vec<ActorOperation> {
        self.drain_work_except(|_| false)
    }

    /// Drains queued work, keeping configuration barriers and entries `keep`
    /// selects in their original order.
    fn drain_work_except(&mut self, keep: impl Fn(&ActorOperation) -> bool) -> Vec<ActorOperation> {
        let mut work = Vec::new();
        let mut configs = VecDeque::new();
        for entry in self.operations.drain(..) {
            if matches!(&entry, ActorOperation::Prepared(pending) if pending.turn.is_none())
                || keep(&entry)
            {
                configs.push_back(entry);
            } else {
                work.push(entry);
            }
        }
        self.operations = configs;
        work
    }

    pub(crate) fn settle_runner(&mut self) {
        self.active = None;
        self.processing = None;
        self.status = ActorStatus::Idle;
    }

    /// A started run supersedes every precancel mark it could match: pushes
    /// stamp the latest run id, and no mark can exist for a run that is
    /// starting (its own pushes already matched). Non-run correlations keep
    /// their marks; nothing here knows when their pushes stop.
    fn retire_superseded_precancels(&mut self, work: &ActorWork) {
        let correlation = match work {
            // A compact starts under a run that already started, so its own
            // pushes already matched; surviving marks still guard later
            // pushes into that run.
            ActorWork::Compact { .. } | ActorWork::HistoryEdit(_) => return,
            ActorWork::Turn(admission) => &admission.correlation,
            ActorWork::Control(control) => &control.correlation,
        };
        if correlation_run_id(correlation).is_none() {
            return;
        }
        self.cancelled_correlations
            .retain(|marked, _| correlation_run_id(marked).is_none());
    }

    fn idle(policy: Option<Arc<EffectiveAgentConfig>>) -> Self {
        Self {
            instructions: HashMap::new(),
            deliveries: HashMap::new(),
            delivery_order: VecDeque::new(),
            delivery_seen: VecDeque::new(),
            delivery_runs: HashMap::new(),
            delivery_suspended: false,
            parent_delivery_policy: None,
            lifecycle: ActorLifecycle::Open,
            close_pending: false,
            pending_publications: 0,
            idle_pending: false,
            runner_busy: false,
            status: ActorStatus::Idle,
            active: None,
            processing: None,
            idle: None,
            cancelled_correlations: HashMap::new(),
            cancellation_generation: 0,
            policy_generation: 0,
            policy,
            latest: None,
            latest_history_turn: None,
            retained_order: VecDeque::new(),
            presentation: 0,
            usage: TokenUsage::default(),
            operations: VecDeque::new(),
            preparing: None,
            prepared_publication: None,
            config_observers: Vec::new(),
            event_observers: Vec::new(),
            graph_observers: Vec::new(),
            next_operation_id: 0,
            next_submission_id: 0,
            settled: None,
        }
    }
}

pub(crate) struct ActorCloseCut {
    deferred: Vec<ActorOperation>,
    deliveries: Vec<Arc<ChildDelivery>>,
    reason: TurnCancellationReason,
    edit: Option<HistoryEditWork>,
    lifecycle: ActorLifecycle,
}

pub(crate) struct LocalCancellationCut {
    pub(crate) turn_id: Option<TurnId>,
    pub(crate) fence: u64,
    deferred: Vec<ActorOperation>,
    drained: Vec<ActorWork>,
}

pub(crate) struct TurnCancelCut {
    deferred: Vec<ActorOperation>,
    queued: Option<TurnAdmission>,
}

pub(crate) struct CorrelationCancelCut {
    pub(crate) active_turn: Option<TurnId>,
    deferred: Vec<ActorOperation>,
    matched: Vec<ActorWork>,
}

/// Per-turn cancellation wiring: a plain token aborts the run, a reasoned
/// token records why, and the actor fires both when the turn must stop. The
/// correlation lets a targeted cancel match only its own active run.
pub(crate) struct ActiveCancel {
    plain: CancelTrigger,
    reasoned: ReasonedCancelTrigger,
    correlation: Option<String>,
}

impl ActiveCancel {
    pub(crate) fn new(correlation: Option<String>) -> (Self, CancelToken, ReasonedCancelToken) {
        let (plain, plain_token) = CancelToken::new();
        let (reasoned, reasoned_token) = ReasonedCancelToken::new();
        (
            Self {
                plain,
                reasoned,
                correlation,
            },
            plain_token,
            reasoned_token,
        )
    }

    pub(crate) fn correlation(&self) -> Option<&str> {
        self.correlation.as_deref()
    }

    /// Installs the winning reason on the token `Agent::run` reads, then
    /// fires the abort signal so the run unwinds. First reason wins.
    pub(crate) fn fire(self, reason: TurnCancellationReason) {
        self.reasoned.cancel(reason);
        self.plain.cancel();
    }
}

/// Retains one outcome and retires its cancellation and ticket registration.
/// The first call wins; terminal registration is gone before any waiter or
/// event recipient can observe the outcome. When `settle_runner` is set, the
/// runner's active slot clears in the same critical section, so no snapshot
/// pairs a running or idle status with the wrong latest outcome.
fn retire_turn(
    inner: &ActorInner,
    turn_id: TurnId,
    outcome: &mut TurnOutcome,
    settled: ActorWorkIdentity,
    settle_runner: bool,
    history_ready: bool,
) -> Option<Option<TurnTicket>> {
    let mut outcomes = inner.outcomes.lock().unwrap_or_else(|e| e.into_inner());
    let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(reason) = state
        .processing
        .as_ref()
        .filter(|processing| processing.turn_id == Some(turn_id))
        .and_then(|processing| processing.cancellation_reason)
        && !matches!(outcome, TurnOutcome::Cancelled { .. })
    {
        *outcome = TurnOutcome::Cancelled {
            agent_id: outcome.agent_id(),
            turn_id,
            usage: outcome.usage(),
            num_turns: outcome.num_turns(),
            reason,
            cost: outcome.cost(),
            list_cost: outcome.list_cost(),
            context_size: outcome.context_size(),
            context_window: outcome.context_window(),
        };
    }
    state.instructions.remove(&turn_id);
    if settle_runner {
        state.settle_runner();
    }
    let std::collections::hash_map::Entry::Vacant(vacant) = outcomes.entry(turn_id) else {
        return None;
    };
    let mut tickets = inner.tickets.lock().unwrap_or_else(|e| e.into_inner());
    vacant.insert(outcome.clone());
    if state.release_idle_permission(turn_id) {
        inner.queue.notify();
        inner.policy_changed.notify(usize::MAX);
    }
    let history = inner.history.load();
    inner
        .coverage
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(
            turn_id,
            history_ready.then_some((history.epoch, history.messages.len())),
        );
    state.latest = Some(outcome.clone());
    state.settled = Some(settled);
    if history_ready {
        state.latest_history_turn = Some((turn_id, history.epoch, history.messages.len()));
    }
    state.retained_order.push_back(turn_id);
    if state.retained_order.len() > MAX_RETAINED_TURN_RESULTS
        && let Some(evicted) = state.retained_order.pop_front()
    {
        outcomes.remove(&evicted);
        inner
            .retained_tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&evicted);
        inner
            .coverage
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&evicted);
    }
    state.usage += outcome.usage();
    state.pending_publications += 1;
    let ticket = tickets.remove(&turn_id);
    if let Some(ticket) = &ticket {
        inner
            .retained_tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(turn_id, ticket.clone());
    }
    Some(ticket)
}

pub(crate) fn finalize_turn(
    inner: &ActorInner,
    turn_id: TurnId,
    outcome: TurnOutcome,
    admission: &TurnAdmission,
    deliver: bool,
) {
    retire_and_publish(inner, turn_id, outcome, admission, deliver, false, false);
}

/// Finalizes the runner's own turn, clearing its active slot atomically
/// with retaining the outcome.
pub(crate) fn settle_and_finalize_turn(
    inner: &ActorInner,
    admission: &TurnAdmission,
    outcome: TurnOutcome,
    deliver: bool,
    history_ready: bool,
) {
    retire_and_publish(
        inner,
        admission.turn_id,
        outcome,
        admission,
        deliver,
        true,
        history_ready,
    );
}

fn retire_and_publish(
    inner: &ActorInner,
    turn_id: TurnId,
    outcome: TurnOutcome,
    admission: &TurnAdmission,
    deliver: bool,
    settle_runner: bool,
    history_ready: bool,
) {
    let presentation = admission
        .event_sender
        .as_ref()
        .filter(|_| deliver)
        .map(|sender| sender.with_identity_of(sender.identity().or(Some(admission.work))));
    let mut outcome = outcome;
    if let Some(ticket) = retire_turn(
        inner,
        turn_id,
        &mut outcome,
        admission.work,
        settle_runner,
        history_ready,
    ) {
        #[cfg(test)]
        if let Some((retired, release)) = inner
            .after_finalization_retire
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = retired.send(());
            let _ = release.recv();
        }
        if let Some(ticket) = &ticket {
            ticket.seal(&outcome);
        }
        let registered_sender = ticket
            .as_ref()
            .and_then(|ticket| ticket.output().take_terminal_sender());
        if let Some(sender) = registered_sender.or(presentation) {
            let _ = sender.send(AgentEvent::TurnOutcome(outcome.clone()));
        }
        if let Some(ticket) = &ticket {
            delivery::publish(inner, ticket);
        }
        delivery::settle(
            inner,
            turn_id,
            match &outcome {
                TurnOutcome::Cancelled { reason, .. } => Some(*reason),
                _ => None,
            },
        );
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ticket) = ticket {
            ticket.resolve(outcome);
            if let Some(result) = ticket.peek_result() {
                state.emit(ActorEvent::End(result));
            }
        }
        state.pending_publications -= 1;
        state.finish_close(inner);
        state.publish_idle(inner);
    }
}

fn terminalize_work(inner: &ActorInner, drained: Vec<ActorWork>, reason: TurnCancellationReason) {
    for work in drained {
        if let ActorWork::Turn(admission) = work {
            let outcome = cancelled_outcome(inner.agent_id, admission.turn_id, reason);
            finalize_turn(inner, admission.turn_id, outcome, &admission, true);
        }
    }
    inner
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .publish_idle(inner);
}

pub(super) fn cancelled_outcome(
    agent_id: AgentId,
    turn_id: TurnId,
    reason: TurnCancellationReason,
) -> TurnOutcome {
    TurnOutcome::cancelled(agent_id, turn_id, TokenUsage::default(), 0, reason)
}

/// Canonical correlation for root/compact run work. The TUI addresses runs
/// as `r{run_id}` (e.g. `r7`); compacts carry only a bare `run_id`, so every
/// comparison against `cancelled_correlations` and every queue correlation
/// view must use this same encoding or targeted cancels miss.
pub(crate) fn run_correlation(run_id: u64) -> String {
    format!("r{run_id}")
}

pub(crate) fn correlation_run_id(correlation: &str) -> Option<u64> {
    correlation.strip_prefix('r')?.parse().ok()
}

/// Stable handle to one actor task. Cloneable; every clone shares the same
/// history, queue, and retained outcomes.
#[derive(Clone)]
pub struct AgentActorHandle {
    pub(crate) inner: Arc<ActorInner>,
    wake: Arc<runner::WakeFlag>,
}

impl AgentActorHandle {
    /// Spawns the actor task. The restored history is sanitized and
    /// published to `shared_messages` synchronously, before the handle is
    /// handed out.
    pub fn spawn(
        agent_id: AgentId,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
    ) -> (Self, smol::Task<()>) {
        Self::spawn_inner(
            agent_id,
            initial_messages,
            shared_messages,
            backend,
            None,
            None,
            None,
        )
    }

    /// Spawns a manager-owned actor. With `start`, the runner waits for that
    /// signal before consuming any work.
    pub(crate) fn spawn_managed(
        agent_id: AgentId,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
        managed_admission: ManagedTurnAdmission,
        initial_config: Option<EffectiveAgentConfig>,
        start: Option<flume::Receiver<()>>,
    ) -> (Self, smol::Task<()>) {
        Self::spawn_inner(
            agent_id,
            initial_messages,
            shared_messages,
            backend,
            Some(managed_admission),
            initial_config.map(Arc::new),
            start,
        )
    }

    fn spawn_inner(
        agent_id: AgentId,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
        managed_admission: Option<ManagedTurnAdmission>,
        initial_config: Option<Arc<EffectiveAgentConfig>>,
        start: Option<flume::Receiver<()>>,
    ) -> (Self, smol::Task<()>) {
        let mirror = shared_messages.unwrap_or_else(|| {
            Arc::new(arc_swap::ArcSwap::from_pointee(
                crate::HistorySnapshot::default(),
            ))
        });
        let history = History::restored(initial_messages).with_mirror(Arc::clone(&mirror));
        let admission_preparation = backend.admission_preparation();
        let mode_ceiling = managed_admission
            .as_ref()
            .filter(|managed| managed.is_child)
            .and(initial_config.as_deref())
            .map(config::ModeCeiling::new);
        let initial_config = initial_config.map(|config| {
            let authority = mode_ceiling
                .clone()
                .or_else(|| config.mode_authority())
                .unwrap_or_default();
            Arc::new(config.as_ref().clone().with_mode_authority(authority))
        });
        let inner = Arc::new(ActorInner {
            agent_id,
            identity: Arc::new(()),
            #[cfg(any(test, feature = "test-util"))]
            runtime_probes: Mutex::new(Vec::new()),
            state: Mutex::new(ActorState::idle(initial_config)),
            policy_changed: Event::new(),
            drained: Event::new(),
            config_waiters: Mutex::default(),
            queue: Arc::new(ActorQueue::new()),
            outcomes: Mutex::new(HashMap::new()),
            tickets: Mutex::new(HashMap::new()),
            retained_tickets: Mutex::new(HashMap::new()),
            latest_admitted: Mutex::new(None),
            history: mirror,
            coverage: Mutex::new(HashMap::new()),
            completion_policy: if managed_admission
                .as_ref()
                .is_some_and(|managed| managed.parent_delivery.is_some())
            {
                types::CompletionPolicy::JoinReportingChildren
            } else {
                types::CompletionPolicy::Independent
            },
            managed_admission,
            mode_ceiling,
            offload: backend.offload_store(),
            admission_preparation,
            prepared_readiness: backend.prepared_readiness(),
            #[cfg(test)]
            after_pop: Mutex::new(None),
            #[cfg(test)]
            after_close_cut: Mutex::new(None),
            #[cfg(test)]
            after_work_execution: Mutex::new(None),
            #[cfg(test)]
            after_finalization_retire: Mutex::new(None),
            #[cfg(test)]
            before_snapshot_state: Mutex::new(None),
            #[cfg(test)]
            stale_preparation: Mutex::new(None),
        });
        let wake = Arc::new(runner::WakeFlag::new());
        let handle = Self {
            inner: Arc::clone(&inner),
            wake: Arc::clone(&wake),
        };
        let deferred_handle = handle.clone();
        let task = smol::spawn(async move {
            if let Some(start) = start {
                let started =
                    futures_lite::future::or(async { start.recv_async().await.is_ok() }, async {
                        loop {
                            let listener = inner.policy_changed.listen();
                            if inner
                                .state
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .lifecycle
                                != ActorLifecycle::Open
                            {
                                return false;
                            }
                            listener.await;
                        }
                    })
                    .await;
                if !started {
                    deferred_handle.close();
                    return;
                }
            }
            drop(deferred_handle);
            runner::Runner::new(inner, history, backend, wake)
                .run()
                .await;
        });
        (handle, task)
    }

    /// Sets child-result routing before any work is admitted.
    pub fn configure_parent_delivery(&self, auto_deliver: bool) -> Result<(), ActorError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if state.next_operation_id != 0 || state.parent_delivery_policy.is_some() {
            return Err(ActorError::Busy);
        }
        state.parent_delivery_policy = Some(delivery::ParentDeliveryPolicy { auto_deliver });
        Ok(())
    }

    pub fn agent_id(&self) -> AgentId {
        self.inner.agent_id
    }

    /// Any admitted turn whose ticket has not resolved, including one retired
    /// but still publishing its delivery.
    pub(crate) fn unsettled_ticket(&self) -> Option<TurnTicket> {
        let active = self
            .inner
            .tickets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .find(|ticket| ticket.peek().is_none())
            .cloned();
        active.or_else(|| {
            self.inner
                .retained_tickets
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .values()
                .find(|ticket| ticket.peek().is_none())
                .cloned()
        })
    }

    /// Whether settled turns are published to the recorded parent's inbox.
    pub(crate) fn reports_to_parent(&self) -> bool {
        self.inner
            .managed_admission
            .as_ref()
            .is_some_and(|managed| managed.parent_delivery.is_some())
            && self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .parent_delivery_policy
                .as_ref()
                .is_some_and(|policy| policy.auto_deliver)
    }

    pub(crate) fn lifecycle(&self) -> ActorLifecycle {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .lifecycle
    }

    /// Captures the nominal processing turn, including the pop/install gap.
    #[cfg(test)]
    pub(crate) fn processing_turn_id(&self) -> Option<TurnId> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .processing
            .as_ref()
            .and_then(|processing| processing.turn_id)
    }

    pub fn cancel_local(&self, displayed: u64) -> (Option<TurnId>, u64) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let cut = self.capture_local_cancel(&mut state, displayed);
        drop(state);
        let captured = (cut.turn_id, cut.fence);
        self.settle_local_cancel(cut);
        captured
    }

    pub(crate) fn capture_local_cancel(
        &self,
        state: &mut ActorState,
        displayed: u64,
    ) -> LocalCancellationCut {
        state.cancel_prepared_where(|turn| turn.is_some());
        let turn_id = state
            .processing
            .as_ref()
            .and_then(|processing| processing.turn_id)
            .or(match state.status {
                ActorStatus::Running(id) => Some(id),
                ActorStatus::Idle => None,
            });
        if let Some(processing) = &mut state.processing
            && processing.turn_id.is_some()
        {
            processing
                .cancellation_reason
                .get_or_insert(TurnCancellationReason::User);
        }
        let deliveries: HashSet<_> = state.delivery_runs.keys().copied().collect();
        state.instructions.retain(|id, _| deliveries.contains(id));
        state.cancellation_generation = state.cancellation_generation.wrapping_add(1);
        let deferred = state.drain_work_except(|work| match work {
            ActorOperation::Turn { ticket, .. } => deliveries.contains(&ticket.turn_id()),
            ActorOperation::Prepared(pending) => pending.turn.is_none(),
            _ => true,
        });
        if let Some(active) = turn_id.and_then(|_| state.active.take()) {
            active.fire(TurnCancellationReason::User);
        }
        let drained = self.inner.queue.drain_except(|work| {
            !matches!(work,
            ActorWork::Turn(admission) if !deliveries.contains(&admission.turn_id))
        });
        drive_operations(&self.inner, state);
        LocalCancellationCut {
            turn_id,
            fence: state.reserve_presentation(displayed),
            deferred,
            drained,
        }
    }

    pub(crate) fn settle_local_cancel(&self, cut: LocalCancellationCut) {
        settle_deferred(&self.inner, cut.deferred, TurnCancellationReason::User);
        terminalize_work(&self.inner, cut.drained, TurnCancellationReason::User);
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn runtime_barrier(&self) -> Result<ActorRuntimeProbe, ActorError> {
        let (send, receive) = flume::bounded(1);
        {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(lifecycle_error(state.lifecycle));
            }
            self.inner
                .runtime_probes
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(send);
            self.inner.queue.notify();
        }
        receive.recv_async().await.map_err(|_| ActorError::Closed)
    }

    pub fn pending_queue(&self) -> Vec<PendingQueueEntry> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let messages = |turn_id| {
            state
                .instructions
                .get(&turn_id)
                .map_or_else(Vec::new, |inbox| {
                    inbox
                        .initial
                        .iter()
                        .cloned()
                        .chain(inbox.pending.iter().map(|entry| entry.presentation.clone()))
                        .map(PendingQueueEntry::Message)
                        .collect()
                })
        };
        let mut entries = state
            .processing
            .as_ref()
            .and_then(|work| work.turn_id)
            .map_or_else(Vec::new, messages);
        entries.extend(self.inner.queue.project_pending(&messages));
        entries.extend(
            state
                .operations
                .iter()
                .flat_map(|operation| match operation {
                    ActorOperation::Turn { ticket, .. } => messages(ticket.turn_id()),
                    ActorOperation::Compact {
                        id,
                        run_id,
                        instructions,
                        ..
                    } => vec![PendingQueueEntry::Compact {
                        operation_id: *id,
                        run_id: *run_id,
                        instructions: instructions.clone(),
                    }],
                    _ => Vec::new(),
                }),
        );
        entries
    }

    pub fn remove_pending_compact(&self, operation_id: u64) -> Option<PendingQueueEntry> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(index) = state.operations.iter().position(|operation| {
            operation.id() == operation_id && matches!(operation, ActorOperation::Compact { .. })
        }) && let ActorOperation::Compact {
            run_id,
            instructions,
            ..
        } = state.operations.remove(index)?
        {
            drive_operations(&self.inner, &mut state);
            return Some(PendingQueueEntry::Compact {
                operation_id,
                run_id,
                instructions,
            });
        }
        self.inner.queue.remove_compact(operation_id)
    }

    pub fn pending_submissions(&self) -> Vec<PendingSubmission> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut pending: Vec<_> = state
            .instructions
            .values()
            .flat_map(|inbox| {
                inbox
                    .initial
                    .iter()
                    .cloned()
                    .chain(inbox.pending.iter().map(|entry| entry.presentation.clone()))
            })
            .collect();
        pending.sort_by_key(|submission| submission.id);
        pending
    }

    pub fn remove_submission(&self, id: SubmissionId) -> Option<PendingSubmission> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let turn_id = state.instructions.iter().find_map(|(turn_id, inbox)| {
            (inbox.initial.as_ref().is_some_and(|entry| entry.id == id)
                || inbox
                    .pending
                    .iter()
                    .any(|entry| entry.presentation.id == id))
            .then_some(*turn_id)
        })?;
        let inbox = state.instructions.get_mut(&turn_id)?;
        if let Some(index) = inbox
            .pending
            .iter()
            .position(|entry| entry.presentation.id == id)
        {
            return inbox.pending.remove(index).map(|entry| entry.presentation);
        }
        // Initial input is held by the operation until the runner commits it.
        if state
            .processing
            .as_ref()
            .is_some_and(|work| work.turn_id == Some(turn_id))
        {
            return None;
        }
        if state.preparing.is_some_and(|operation| {
            state.operations.iter().any(|work| {
                matches!(work, ActorOperation::Turn { id, ticket, .. }
                if *id == operation && ticket.turn_id() == turn_id)
            })
        }) {
            return None;
        }
        if self.inner.queue.has_prepared_turn(turn_id) {
            return None;
        }
        let submission = state.instructions.get_mut(&turn_id)?.initial.take()?;
        if let Some(entry) = state.instructions.get_mut(&turn_id)?.pending.pop_front() {
            let replacement = if let Some(ActorOperation::Turn { input, .. }) = state.operations.iter_mut()
                .find(|work| matches!(work, ActorOperation::Turn { ticket, .. } if ticket.turn_id() == turn_id))
            {
                **input = entry.input;
                Ok(())
            } else {
                self.inner.queue.replace_turn_input(turn_id, entry.input)
            };
            if replacement.is_ok() {
                let inbox = state.instructions.get_mut(&turn_id)?;
                inbox.initial = Some(entry.presentation);
                inbox.drained.clear();
                inbox.drained.push((entry.provenance, entry.bytes));
                return Some(submission);
            }
            return None;
        }
        let cut = self
            .capture_turn_cancel(&mut state, turn_id, TurnCancellationReason::User)
            .ok()?;
        drop(state);
        self.settle_turn_cancel(cut, TurnCancellationReason::User);
        Some(submission)
    }

    pub fn presentation_id(&self) -> u64 {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .presentation
    }

    pub fn observe_presentation(&self, id: u64) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.presentation = state.presentation.max(id);
    }

    pub fn reserve_presentation(&self, displayed: u64) -> u64 {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reserve_presentation(displayed)
    }

    pub fn allocate_processing_presentation(
        &self,
        turn_id: TurnId,
        displayed: Option<u64>,
    ) -> Result<u64, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        let processing = state
            .processing
            .as_ref()
            .filter(|processing| processing.turn_id == Some(turn_id))
            .ok_or(ActorError::UnknownTurn(turn_id))?;
        if processing.cancellation_reason.is_some() {
            return Err(ActorError::PolicyCancelled);
        }
        if displayed == Some(state.presentation) {
            return Ok(state.presentation);
        }
        state.presentation += 1;
        Ok(state.presentation)
    }

    pub fn identity(&self) -> Arc<()> {
        Arc::clone(&self.inner.identity)
    }

    /// Tags every later event with this actor's id for a whole-graph
    /// subscriber.
    pub(crate) fn attach_graph_observer(&self, sender: flume::Sender<(AgentId, ActorEvent)>) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.close_emitted() {
            state.graph_observers.push((self.inner.agent_id, sender));
        }
    }

    pub fn subscribe_events(&self) -> flume::Receiver<ActorEvent> {
        let (sender, receiver) = flume::unbounded();
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.close_emitted() {
            state.event_observers.push(sender);
        }
        receiver
    }

    /// The ticket of the most recently admitted turn, whatever admitted it,
    /// while it is live or retained.
    pub fn latest_ticket(&self) -> Option<TurnTicket> {
        let turn_id = (*self
            .inner
            .latest_admitted
            .lock()
            .unwrap_or_else(|e| e.into_inner()))?;
        self.turn_ticket(turn_id)
    }

    pub fn turn_ticket(&self, turn_id: TurnId) -> Option<TurnTicket> {
        self.inner
            .tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&turn_id)
            .cloned()
            .or_else(|| {
                self.inner
                    .retained_tickets
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&turn_id)
                    .cloned()
            })
    }

    pub fn subscribe_config_commits(&self) -> flume::Receiver<ConfigCommit> {
        let (sender, receiver) = flume::unbounded();
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.close_emitted() {
            state.config_observers.push(sender);
        }
        receiver
    }

    pub(crate) fn same_actor(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner.identity, &other.inner.identity)
    }

    pub(crate) fn owns_ticket(&self, ticket: &TurnTicket) -> bool {
        ticket.belongs_to(&self.inner.identity)
    }

    #[cfg(test)]
    pub(crate) fn pause_after_next_pop(&self) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (popped_tx, popped_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        *self
            .inner
            .after_pop
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((popped_tx, release_rx));
        (popped_rx, release_tx)
    }

    #[cfg(test)]
    pub(crate) fn pause_before_next_snapshot_state(
        &self,
    ) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (entered_tx, entered_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        *self
            .inner
            .before_snapshot_state
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    #[cfg(test)]
    fn pause_after_next_finalization_retire(&self) -> (flume::Receiver<()>, flume::Sender<()>) {
        let (retired_tx, retired_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        *self
            .inner
            .after_finalization_retire
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some((retired_tx, release_rx));
        (retired_rx, release_tx)
    }

    /// Admits one turn. Preparation runs before the actor state lock; the
    /// admission and close decision then linearize under that lock, so a
    /// concurrent `close()`/`shutdown()` cannot admit after the queue drains.
    pub fn admit_turn(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
    ) -> Result<TurnTicket, ActorError> {
        self.admit_turn_with_options(
            input,
            event_sender,
            correlation,
            TurnAdmissionOptions::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn admit_turn_if_idle(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
        after_turn: Option<TurnId>,
    ) -> Result<TurnTicket, ActorError> {
        self.admit_turn_with_options(
            input,
            event_sender,
            correlation,
            TurnAdmissionOptions {
                idle_only: true,
                after_turn,
                ..TurnAdmissionOptions::default()
            },
        )
    }

    /// Starts ordinary work or steers its open instruction admission. The host
    /// must authorize the sender before calling; provenance never changes the
    /// execution's original authority. Lock order is actor state, then queue or
    /// tickets. No policy callback or backend operation runs under these locks.
    pub fn submit(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
        options: TurnAdmissionOptions,
    ) -> Result<TurnTicket, ActorError> {
        self.submit_presented(input, event_sender, correlation, options)
            .map(|(ticket, _)| ticket)
    }

    /// `submit`, also returning the presentation the input landed under: the
    /// open turn it folded into, or the new turn's.
    pub fn submit_presented(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
        options: TurnAdmissionOptions,
    ) -> Result<(TurnTicket, u64), ActorError> {
        let bytes = submission_bytes(&input)?;
        let batchable = crate::batchable(&input);
        if bytes > MAX_PENDING_INSTRUCTION_BYTES {
            return Err(ActorError::InstructionOverflow);
        }
        loop {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(lifecycle_error(state.lifecycle));
            }
            let active =
                state
                    .processing
                    .as_ref()
                    .and_then(|work| work.turn_id)
                    .or(match state.status {
                        ActorStatus::Running(id) => Some(id),
                        ActorStatus::Idle => None,
                    });
            let target = active
                .filter(|id| {
                    state.instructions.get(id).is_some_and(|inbox| {
                        inbox.open
                            && batchable
                            && inbox.batchable
                            && match (&inbox.policy, &state.policy) {
                                (Some(active), Some(current)) => Arc::ptr_eq(active, current),
                                (None, None) => true,
                                _ => false,
                            }
                            && !state
                                .operations
                                .iter()
                                .any(|work| work.id() > inbox.operation)
                            && self.inner.queue.is_empty()
                    })
                })
                .or_else(|| {
                    state.instructions.iter().find_map(|(id, inbox)| {
                        (inbox.open
                            && batchable
                            && inbox.batchable
                            && inbox.submit_operation == Some(state.next_operation_id))
                        .then_some(*id)
                    })
                });
            if let Some(target) = target {
                if options.idle_only || options.after_turn.is_some() {
                    return Err(ActorError::Busy);
                }
                if state.cancelled_correlations.contains_key(&correlation) {
                    return Err(ActorError::Busy);
                }
                let deferred_constraint = options.policy_constraint.is_some()
                    && state
                        .instructions
                        .get(&target)
                        .is_some_and(|inbox| inbox.policy.is_none());
                if let Some(constraint) = &options.policy_constraint
                    && !deferred_constraint
                {
                    let generation = state.policy_generation;
                    let operation_boundary = state.next_operation_id;
                    let policy = state.instructions[&target].policy.clone().unwrap();
                    drop(state);
                    constraint(&policy).map_err(ActorError::InvalidConfig)?;
                    state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                    if state.policy_generation != generation
                        || state.next_operation_id != operation_boundary
                        || state.cancelled_correlations.contains_key(&correlation)
                        || state.lifecycle != ActorLifecycle::Open
                        || state.instructions.get(&target).is_none_or(|inbox| {
                            !inbox.open
                                || !batchable
                                || !inbox.batchable
                                || (inbox.submit_operation != Some(operation_boundary)
                                    && (state
                                        .operations
                                        .iter()
                                        .any(|work| work.id() > inbox.operation)
                                        || !self.inner.queue.is_empty()
                                        || inbox
                                            .policy
                                            .as_ref()
                                            .zip(state.policy.as_ref())
                                            .is_some_and(|(active, current)| {
                                                !Arc::ptr_eq(active, current)
                                            })))
                        })
                    {
                        continue;
                    }
                }
                state.check_instruction_capacity(bytes)?;
                if deferred_constraint {
                    let Some(ActorOperation::Turn { policy_constraint, .. }) = state.operations.iter_mut()
                        .find(|work| matches!(work, ActorOperation::Turn { ticket, .. } if ticket.turn_id() == target))
                    else {
                        return Err(ActorError::PolicyPending);
                    };
                    let constraint = options.policy_constraint.as_ref().unwrap().clone();
                    let previous = policy_constraint.take();
                    *policy_constraint = Some(Arc::new(move |policy| {
                        if let Some(previous) = &previous {
                            previous(policy)?;
                        }
                        constraint(policy)
                    }));
                }
                if let Some(processing) = &mut state.processing
                    && processing.turn_id == Some(target)
                    && !processing.correlations.contains(&correlation)
                {
                    processing.correlations.push(correlation.clone());
                }
                let presentation = state.pending_submission(
                    target,
                    &input,
                    options.already_displayed,
                    options.mcp_startup_notice,
                );
                let inbox = state
                    .instructions
                    .get_mut(&target)
                    .expect("target checked under actor lock");
                if !inbox.correlations.contains(&correlation) {
                    inbox.correlations.push(correlation.clone());
                }
                inbox.pending.push_back(inbox::InstructionEntry {
                    presentation,
                    input,
                    provenance: options.provenance,
                    bytes,
                });
                return Ok((inbox.ticket.clone(), inbox.presentation));
            }
            if options.existing_only {
                return Err(ActorError::Busy);
            }
            if (options.idle_only || options.after_turn.is_some())
                && (state.idle.is_some() || state.has_pending_work(&self.inner.queue))
            {
                return Err(ActorError::Busy);
            }
            if let Some(after) = options.after_turn
                && state.latest.as_ref().map(TurnOutcome::turn_id) != Some(after)
            {
                return Err(ActorError::StaleTurn);
            }
            state.check_instruction_capacity(bytes)?;
            if self
                .inner
                .tickets
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len()
                >= MAX_RETAINED_TURN_RESULTS
            {
                return Err(ActorError::InstructionOverflow);
            }
            let turn_id = TurnId::generate();
            let ticket = TurnTicket::new(turn_id, Arc::clone(&self.inner.identity))
                .with_provenance(options.provenance);
            state.next_operation_id = state.next_operation_id.wrapping_add(1);
            let id = state.next_operation_id;
            let presentation_id = state.adopt_presentation(options.displayed_presentation);
            let identity = ActorWorkIdentity::new(
                self.inner.agent_id,
                ActorWorkKind::Turn {
                    operation_id: id,
                    turn_id,
                },
                presentation_id,
            );
            let mut inbox = inbox::InstructionInbox::new(ticket.clone(), presentation_id);
            inbox.drained.push((ticket.provenance().clone(), bytes));
            inbox.submit_operation = Some(id);
            inbox.operation = id;
            inbox.batchable = batchable;
            inbox.initial = Some(state.pending_submission(
                turn_id,
                &input,
                options.already_displayed,
                options.mcp_startup_notice,
            ));
            state.delivery_suspended = false;
            state.instructions.insert(turn_id, inbox);
            self.inner.admit_ticket(turn_id, ticket.clone());
            let reason = state.cancelled_correlations.get(&correlation).copied();
            let work = ActorOperation::Turn {
                id,
                input: Box::new(input),
                event_sender,
                correlation,
                ticket: ticket.clone(),
                policy_constraint: options.policy_constraint,
                identity,
            };
            if let Some(reason) = reason {
                state.instructions.remove(&turn_id);
                drop(state);
                settle_deferred(&self.inner, vec![work], reason);
            } else {
                state.operations.push_back(work);
                drive_operations(&self.inner, &mut state);
            }
            return Ok((ticket, presentation_id));
        }
    }

    pub fn admit_turn_with_options(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
        options: TurnAdmissionOptions,
    ) -> Result<TurnTicket, ActorError> {
        let bytes = submission_bytes(&input)?;
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.check_instruction_capacity(bytes)?;
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if (options.idle_only || options.after_turn.is_some())
            && (state.idle.is_some() || state.has_pending_work(&self.inner.queue))
        {
            return Err(ActorError::Busy);
        }
        if let Some(after) = options.after_turn
            && state.latest.as_ref().map(TurnOutcome::turn_id) != Some(after)
        {
            return Err(ActorError::StaleTurn);
        }
        if self
            .inner
            .tickets
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .len()
            >= MAX_RETAINED_TURN_RESULTS
        {
            return Err(ActorError::InstructionOverflow);
        }
        let turn_id = TurnId::generate();
        let ticket = TurnTicket::new(turn_id, Arc::clone(&self.inner.identity))
            .with_provenance(options.provenance);
        state.delivery_suspended = false;
        state.next_operation_id = state.next_operation_id.wrapping_add(1);
        let after = state.next_operation_id;
        let presentation_id = state.adopt_presentation(options.displayed_presentation);
        state.instructions.insert(
            turn_id,
            inbox::InstructionInbox::new(ticket.clone(), presentation_id),
        );
        let identity = ActorWorkIdentity::new(
            self.inner.agent_id,
            ActorWorkKind::Turn {
                operation_id: after,
                turn_id,
            },
            presentation_id,
        );
        let presentation = state.pending_submission(
            turn_id,
            &input,
            options.already_displayed,
            options.mcp_startup_notice,
        );
        if let Some(inbox) = state.instructions.get_mut(&turn_id) {
            inbox.initial = Some(presentation);
            inbox.drained.push((ticket.provenance().clone(), bytes));
            inbox.operation = after;
        }
        let reason = state.cancelled_correlations.get(&correlation).copied();
        let work = ActorOperation::Turn {
            id: after,
            input: Box::new(input),
            event_sender,
            correlation,
            ticket: ticket.clone(),
            policy_constraint: options.policy_constraint,
            identity,
        };
        self.inner.admit_ticket(turn_id, ticket.clone());
        if let Some(reason) = reason {
            drop(state);
            settle_deferred(&self.inner, vec![work], reason);
        } else {
            state.operations.push_back(work);
            drive_operations(&self.inner, &mut state);
        }
        Ok(ticket)
    }

    /// Installs a policy snapshot for subsequent admissions.
    pub fn effective_config(&self) -> Option<Arc<EffectiveAgentConfig>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .policy
            .clone()
    }

    /// Checks a resolved mode against the child's immutable creation ceiling.
    /// Root actors have no mode ceiling.
    pub fn validate_mode_change(
        &self,
        mode: &AgentMode,
        mode_def: &ModeDef,
    ) -> Result<(), ActorError> {
        match &self.inner.mode_ceiling {
            Some(ceiling) => ceiling.validate(mode, Some(mode_def)),
            None => Ok(()),
        }
    }

    pub fn config_snapshot(&self) -> Option<ConfigCommit> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.policy.clone().map(|config| ConfigCommit {
            identity: Arc::clone(&self.inner.identity),
            generation: state.policy_generation,
            config,
        })
    }

    pub fn policy_snapshot(&self) -> Option<Arc<RunSettings>> {
        self.effective_config()
            .map(|config| Arc::new(config.settings.clone()))
    }

    pub fn initialize_config(&self, config: EffectiveAgentConfig) -> Result<u64, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(match state.lifecycle {
                ActorLifecycle::Closed => ActorError::Closed,
                ActorLifecycle::Shutdown => ActorError::Shutdown,
                ActorLifecycle::Open => unreachable!(),
            });
        }
        if state.policy.is_some()
            || state.status != ActorStatus::Idle
            || !state.operations.is_empty()
            || !self.inner.queue.is_empty()
        {
            return Err(ActorError::PolicyPending);
        }
        if matches!(config.mode, crate::AgentMode::Custom(_)) && config.mode_def.is_none() {
            return Err(ActorError::InvalidConfig(
                "custom mode requires a resolved definition".into(),
            ));
        }
        let config = Arc::new(config);
        state.policy = Some(Arc::clone(&config));
        let generation = state.policy_generation;
        state.emit(ActorEvent::Config(ConfigCommit {
            identity: Arc::clone(&self.inner.identity),
            generation,
            config,
        }));
        Ok(generation)
    }

    pub fn push_control(
        &self,
        name: String,
        correlation: String,
        payload: Option<Arc<dyn Any + Send + Sync>>,
    ) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        state.next_operation_id = state.next_operation_id.wrapping_add(1);
        let id = state.next_operation_id;
        let presentation_id = state.reserve_presentation(0);
        state.operations.push_back(ActorOperation::Control {
            id,
            control: ControlWork {
                name,
                correlation,
                work: ActorWorkIdentity::new(
                    self.inner.agent_id,
                    ActorWorkKind::Control { operation_id: id },
                    presentation_id,
                ),
                payload,
            },
        });
        drive_operations(&self.inner, &mut state);
        Ok(())
    }

    pub fn push_compact(
        &self,
        run_id: u64,
        instructions: Option<String>,
    ) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(match state.lifecycle {
                ActorLifecycle::Closed => ActorError::Closed,
                ActorLifecycle::Shutdown => ActorError::Shutdown,
                ActorLifecycle::Open => unreachable!(),
            });
        }
        // A compact whose run_id was precancelled is dropped.
        if state
            .cancelled_correlations
            .contains_key(&run_correlation(run_id))
        {
            return Ok(());
        }
        state.next_operation_id = state.next_operation_id.wrapping_add(1);
        let after = state.next_operation_id;
        let presentation_id = state.reserve_presentation(0);
        state.operations.push_back(ActorOperation::Compact {
            id: after,
            run_id,
            instructions,
            presentation_id,
        });
        drive_operations(&self.inner, &mut state);
        Ok(())
    }

    /// Removes an admitted turn from the queue and terminalizes it instead
    /// of stranding it.
    pub fn remove(&self, turn_id: TurnId) -> Result<TurnOutcome, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let deferred = state
            .operations
            .iter()
            .position(|admission| matches!(admission, ActorOperation::Turn { ticket, .. } if ticket.turn_id() == turn_id))
            .and_then(|index| state.operations.remove(index));
        let queued = self.inner.queue.remove_turn(turn_id);
        if deferred.is_some() || queued.is_some() {
            state.instructions.remove(&turn_id);
        }
        drive_operations(&self.inner, &mut state);
        drop(state);
        if let Some(admission) = deferred {
            let outcome =
                cancelled_outcome(self.inner.agent_id, turn_id, TurnCancellationReason::User);
            settle_deferred(&self.inner, vec![admission], TurnCancellationReason::User);
            return Ok(outcome);
        }
        let Some(admission) = queued else {
            return Err(ActorError::UnknownTurn(turn_id));
        };
        let outcome = cancelled_outcome(self.inner.agent_id, turn_id, TurnCancellationReason::User);
        finalize_turn(&self.inner, turn_id, outcome.clone(), &admission, true);
        Ok(outcome)
    }

    /// Clears every queued item, terminalizing the admitted turns. Returns
    /// the number of items removed.
    pub fn clear(&self) -> usize {
        self.clear_except(false)
    }

    /// Clears queued user work after a failed turn while keeping child-result
    /// deliveries, which the user did not author and cannot resubmit.
    pub fn clear_user_work(&self) -> usize {
        self.clear_except(true)
    }

    fn clear_except(&self, keep_deliveries: bool) -> usize {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if !keep_deliveries {
            delivery::discard(&mut state, TurnCancellationReason::User);
        }
        let deliveries = if keep_deliveries {
            state.delivery_runs.keys().copied().collect()
        } else {
            HashSet::new()
        };
        let deferred = state.drain_work_except(|entry| {
            matches!(entry, ActorOperation::Turn { ticket, .. } if deliveries.contains(&ticket.turn_id()))
        });
        let drained = self.inner.queue.drain_except(
            |work| matches!(work, ActorWork::Turn(admission) if deliveries.contains(&admission.turn_id)),
        );
        for work in &drained {
            if let ActorWork::Turn(admission) = work {
                state.instructions.remove(&admission.turn_id);
            }
        }
        for work in &deferred {
            if let ActorOperation::Turn { ticket, .. } = work {
                state.instructions.remove(&ticket.turn_id());
            }
        }
        drive_operations(&self.inner, &mut state);
        let len = drained.len()
            + deferred
                .iter()
                .filter(|entry| entry.projection().is_some())
                .count();
        drop(state);
        settle_deferred(&self.inner, deferred, TurnCancellationReason::User);
        terminalize_work(&self.inner, drained, TurnCancellationReason::User);
        len
    }

    /// Cancels the active turn, terminalizes every queued admitted turn, and
    /// drops queued roots/controls. The actor stays open and reusable.
    pub fn cancel_all(&self) {
        self.cancel_existing();
    }

    /// Cancels exactly the active and queued work present at one actor cut.
    /// The state lock spans active removal and queue draining, so a runner
    /// cannot move a turn through the pop/install gap without observing the cut.
    pub fn cancel_existing(&self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.cancel_prepared_where(|turn| turn.is_some());
        state.instructions.clear();
        state.cancellation_generation = state.cancellation_generation.wrapping_add(1);
        if let Some(processing) = &mut state.processing {
            processing
                .cancellation_reason
                .get_or_insert(TurnCancellationReason::User);
        }
        let deferred = state.drain_work();
        let active = state.active.take();
        let drained = self.inner.queue.drain_all();
        drive_operations(&self.inner, &mut state);
        drop(state);
        settle_deferred(&self.inner, deferred, TurnCancellationReason::User);
        if let Some(active) = active {
            active.fire(TurnCancellationReason::User);
        }
        terminalize_work(&self.inner, drained, TurnCancellationReason::User);
    }

    /// Cancels one already-admitted turn by nominal identity. The exact turn
    /// is caught while queued, active, or between queue pop and active install.
    /// Unknown and already-terminal turns are rejected; no future admission is
    /// affected.
    pub fn cancel_turn(&self, turn_id: TurnId) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let cut = self.capture_turn_cancel(&mut state, turn_id, TurnCancellationReason::User)?;
        drop(state);
        self.settle_turn_cancel(cut, TurnCancellationReason::User);
        Ok(())
    }

    pub(crate) fn capture_turn_cancel(
        &self,
        state: &mut ActorState,
        turn_id: TurnId,
        reason: TurnCancellationReason,
    ) -> Result<TurnCancelCut, ActorError> {
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if !self
            .inner
            .tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&turn_id)
        {
            return Err(ActorError::UnknownTurn(turn_id));
        }
        state.instructions.remove(&turn_id);
        let running = state.status == ActorStatus::Running(turn_id);
        if let Some(processing) = &mut state.processing
            && (running || processing.turn_id == Some(turn_id))
        {
            processing.cancellation_reason.get_or_insert(reason);
        }
        if running && let Some(active) = state.active.take() {
            active.fire(reason);
        }
        let queued = self.inner.queue.remove_turn(turn_id);
        let deferred = state.operations.iter().position(|admission| {
            matches!(admission, ActorOperation::Turn { ticket, .. } if ticket.turn_id() == turn_id)
        }).and_then(|index| state.operations.remove(index));
        drive_operations(&self.inner, state);
        Ok(TurnCancelCut {
            deferred: deferred.into_iter().collect(),
            queued,
        })
    }

    pub(crate) fn settle_turn_cancel(&self, cut: TurnCancelCut, reason: TurnCancellationReason) {
        settle_deferred(&self.inner, cut.deferred, reason);
        if let Some(admission) = cut.queued {
            let outcome = cancelled_outcome(self.inner.agent_id, admission.turn_id, reason);
            finalize_turn(&self.inner, admission.turn_id, outcome, &admission, true);
        }
    }

    /// Closes the actor: admissions are rejected, the active turn is
    /// cancelled with `Closed`, queued turns terminalize immediately, and the
    /// runner retains the active backend outcome before emitting `Close` and exiting.
    pub fn close(&self) {
        if let Some(cut) = self.capture_close(false) {
            self.settle_close(cut);
        }
    }

    pub fn shutdown(&self) {
        if let Some(cut) = self.capture_close(true) {
            self.settle_close(cut);
        }
    }

    /// Cancels work matching one correlation: the active run if its
    /// correlation matches (first reason wins), every queue item carrying
    /// that correlation (admitted turns are terminalized exactly once and
    /// delivered, roots/compacts are dropped), and remembers the correlation
    /// so a later push with it is precancelled. Unrelated work is untouched,
    /// and the actor stays open and reusable.
    pub fn cancel_correlation(&self, correlation: &str, reason: TurnCancellationReason) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let cut = self.capture_correlation_cancel(&mut state, correlation, reason);
        drop(state);
        self.settle_correlation_cancel(cut, correlation, reason);
    }

    pub(crate) fn capture_correlation_cancel(
        &self,
        state: &mut ActorState,
        correlation: &str,
        reason: TurnCancellationReason,
    ) -> CorrelationCancelCut {
        state.cancel_prepared_where(|turn| turn == Some(correlation));
        let matched_active = state.active_matches_correlation(correlation);
        let active_turn = match state.status {
            ActorStatus::Running(turn_id) if matched_active => Some(turn_id),
            ActorStatus::Running(_) | ActorStatus::Idle => None,
        };
        if matched_active && let Some(active) = state.active.take() {
            active.fire(reason);
        }
        let matched_processing = state.processing.as_mut().is_some_and(|processing| {
            if !processing.matches_correlation(correlation) {
                return false;
            }
            processing.cancellation_reason.get_or_insert(reason);
            true
        });
        // Scan the queue under the state lock so the request linearizes with
        // admission/close; turn out the matching items.
        let joined: HashSet<_> = state
            .instructions
            .iter()
            .filter_map(|(id, inbox)| {
                inbox
                    .correlations
                    .iter()
                    .any(|key| key == correlation)
                    .then_some(*id)
            })
            .collect();
        let mut matched: Vec<ActorWork> = self
            .inner
            .queue
            .remove_correlation(correlation)
            .into_iter()
            .collect();
        for id in &joined {
            if let Some(admission) = self.inner.queue.remove_turn(*id) {
                matched.push(ActorWork::Turn(Box::new(admission)));
            }
        }
        let mut deferred = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(admission) = state.operations.pop_front() {
            let matches = match &admission {
                ActorOperation::Control { .. } => false,
                ActorOperation::Prepared(pending) => pending
                    .turn
                    .as_ref()
                    .is_some_and(|turn| turn.correlation == correlation),
                ActorOperation::Turn {
                    correlation: key,
                    ticket,
                    ..
                } => key == correlation || joined.contains(&ticket.turn_id()),
                ActorOperation::Compact { run_id, .. } => run_correlation(*run_id) == correlation,
            };
            if matches {
                deferred.push(admission);
            } else {
                remaining.push_back(admission);
            }
        }
        if let Some(id) = active_turn {
            state.instructions.remove(&id);
        }
        if matched_processing
            && let Some(id) = state.processing.as_ref().and_then(|work| work.turn_id)
        {
            state.instructions.remove(&id);
        }
        for work in &matched {
            if let ActorWork::Turn(admission) = work {
                state.instructions.remove(&admission.turn_id);
            }
        }
        for work in &deferred {
            if let ActorOperation::Turn { ticket, .. } = work {
                state.instructions.remove(&ticket.turn_id());
            }
        }
        state.operations = remaining;
        drive_operations(&self.inner, state);
        if !matched_active && !matched_processing && matched.is_empty() && deferred.is_empty() {
            // Nothing matched now; precancel any later push with this
            // correlation, remembering the reason to terminalize with.
            state
                .cancelled_correlations
                .insert(correlation.to_owned(), reason);
        }
        CorrelationCancelCut {
            active_turn,
            deferred,
            matched,
        }
    }

    pub(crate) fn settle_correlation_cancel(
        &self,
        cut: CorrelationCancelCut,
        correlation: &str,
        reason: TurnCancellationReason,
    ) {
        settle_deferred(&self.inner, cut.deferred, reason);

        terminalize_work(&self.inner, cut.matched, reason);
        info!(
            agent_id = %self.inner.agent_id,
            %correlation,
            ?reason,
            "correlation cancelled"
        );
    }

    /// Nothing runs or waits here: no work, no child result on its way in.
    /// A closed actor is settled.
    pub(crate) fn is_settled(&self) -> bool {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.lifecycle != ActorLifecycle::Open
            || !(state.runner_busy
                || state.preparing.is_some()
                || state.has_pending_work(&self.inner.queue)
                || !state.deliveries.is_empty()
                || !state.delivery_runs.is_empty())
    }

    pub fn snapshot(&self) -> ActorSnapshot {
        #[cfg(test)]
        if let Some((entered, release)) = self
            .inner
            .before_snapshot_state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let lifecycle = state.lifecycle;
        let status = state.status;
        let active_turn = match status {
            ActorStatus::Running(turn_id) => Some(turn_id),
            ActorStatus::Idle => None,
        };
        let mut queue = self.inner.queue.snapshot();
        queue.extend(
            state
                .operations
                .iter()
                .filter_map(ActorOperation::projection),
        );
        ActorSnapshot {
            lifecycle,
            status,
            active_turn,
            queued: queue
                .iter()
                .filter(|item| !matches!(item, QueueProjection::Control(_)))
                .count(),
            queue,
            latest: state.latest.clone(),
            cumulative_usage: state.usage,
        }
    }

    pub fn outcome(&self, turn_id: TurnId) -> Option<TurnOutcome> {
        self.inner
            .outcomes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&turn_id)
            .cloned()
    }

    /// Exact wait for a turn's outcome, by id. Never strands: the waiter
    /// resolves as soon as the outcome is retained.
    pub async fn wait_outcome(&self, turn_id: TurnId) -> Result<TurnOutcome, ActorError> {
        if let Some(outcome) = self.outcome(turn_id) {
            return Ok(outcome);
        }
        let ticket = self
            .inner
            .tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&turn_id)
            .cloned();
        match ticket {
            Some(ticket) => Ok(ticket.wait().await),
            None => self
                .outcome(turn_id)
                .ok_or(ActorError::UnknownTurn(turn_id)),
        }
    }

    pub(crate) fn capture_close(&self, shutdown: bool) -> Option<ActorCloseCut> {
        let (lifecycle, reason) = if shutdown {
            (ActorLifecycle::Shutdown, TurnCancellationReason::Shutdown)
        } else {
            (ActorLifecycle::Closed, TurnCancellationReason::Closed)
        };
        let (deferred, deliveries, edit) = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            // First terminal lifecycle/reason wins: repeated close/shutdown are
            // idempotent no-ops, so a race cannot overwrite Closed with Shutdown
            // or vice versa.
            if state.lifecycle != ActorLifecycle::Open {
                return None;
            }
            if let Some(processing) = &mut state.processing {
                processing.cancellation_reason.get_or_insert(reason);
            }
            state.cancel_prepared_where(|_| true);
            state.instructions.clear();
            let deliveries = delivery::take_pending(&mut state);
            state.lifecycle = lifecycle;
            state.close_pending = true;
            state.preparing = None;
            for entry in &state.operations {
                if let ActorOperation::Prepared(pending) = entry {
                    pending
                        .failure
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .get_or_insert_with(|| lifecycle_error(lifecycle));
                }
            }
            state.cancelled_correlations.clear();
            let edit = state.idle.take().and_then(|idle| idle.edit);
            if let Some(id) = edit.as_ref().map(HistoryEditWork::prepared_id) {
                state.end_prepared_publication(id);
            }
            if let Some(active) = state.active.take() {
                let active_reason = state
                    .processing
                    .as_ref()
                    .and_then(|processing| processing.cancellation_reason)
                    .unwrap_or(reason);
                active.fire(active_reason);
            }
            (
                state.operations.drain(..).collect::<Vec<_>>(),
                deliveries,
                edit,
            )
        };
        Some(ActorCloseCut {
            deferred,
            deliveries,
            reason,
            edit,
            lifecycle,
        })
    }

    pub(crate) fn settle_close(&self, cut: ActorCloseCut) {
        let ActorCloseCut {
            deferred,
            deliveries,
            reason,
            edit,
            lifecycle,
        } = cut;
        self.inner.policy_changed.notify(usize::MAX);
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            for delivery in deliveries {
                state.emit(ActorEvent::Delivery(DeliveryEvent {
                    delivery,
                    status: DeliveryStatus::Discarded(reason),
                }));
            }
        }
        #[cfg(test)]
        if let Some((captured, release)) = self.inner.after_close_cut.lock().unwrap().take() {
            let _ = captured.send(());
            let _ = release.recv();
        }
        if let Some(edit) = edit {
            edit.fail(lifecycle_error(lifecycle));
        }
        settle_deferred(&self.inner, deferred, reason);
        let drained = self.inner.queue.drain_all();
        terminalize_work(&self.inner, drained, reason);
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.finish_close(&self.inner);
        }
        self.inner.queue.notify();
        self.wake.wake();
        info!(agent_id = %self.inner.agent_id, ?reason, ?lifecycle, "actor closed");
    }
}
