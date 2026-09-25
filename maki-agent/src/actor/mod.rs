//! Single-actor turn scheduler.
//!
//! One persistent actor owns a stable [`AgentId`], the [`History`], and a
//! lock-backed FIFO deque of work. It admits turns with an immediately
//! allocated [`TurnId`] ([`AgentActorHandle::admit_turn`] returns a
//! [`TurnTicket`] whose async wait is exact and cannot strand), runs them
//! strictly in order through an [`ActorBackend`], and retains the terminal
//! [`TurnOutcome`] of every admitted turn for lookup and exact waiting.
//!
//! Work that is not a turn has no [`TurnId`]: root inputs get one only when
//! the scheduler pops them and starts them, and controls never do. A root
//! input extracted while a turn is active folds into the active turn through
//! the [`InterruptSource`] and creates no [`TurnId`] and no outcome. An
//! admitted turn that is removed or cleared from the queue is terminalized
//! rather than stranded.

mod actor_error;
mod queue;
mod runner;
mod tickets;
mod types;

#[cfg(test)]
mod tests;

pub use actor_error::ActorError;
pub use queue::{ActorQueue, InterruptQueue, QueueProjection};
pub use tickets::TurnTicket;
pub(crate) use types::ManagedTurnAdmission;
pub use types::{
    ActorBackend, ActorLifecycle, ActorSnapshot, ActorStatus, AdmissionPreparation, BackendResult,
    ControlWork, EffectiveAgentConfig, RootWork, TurnAdmission, TurnContext, WorkKind,
};

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use event_listener::Event;

use maki_providers::{Message, TokenUsage};
use tracing::info;

use crate::cancel::{CancelToken, ReasonedCancelToken, ReasonedCancelTrigger};
use crate::types::{AgentEvent, AgentId, EventSender, TurnCancellationReason, TurnId, TurnOutcome};
use crate::{AgentInput, CancelTrigger, History, InterruptSource, RunSettings, SharedMessages};

/// One unit of work the scheduler consumes. Variants map onto behavior: a
/// `Turn` always settles into exactly one [`TurnOutcome`], `Root` becomes a
/// turn once started (and folds while one is active), `Control` and `Compact`
/// never produce an outcome.
pub enum ActorWork {
    Turn(TurnAdmission),
    Root(RootWork),
    PolicyBarrier {
        generation: u64,
    },
    Control(ControlWork),
    Compact {
        run_id: u64,
        instructions: Option<String>,
    },
}

/// The mutable half of an actor, shared with every clone of the handle.
pub(crate) struct ActorInner {
    pub(crate) agent_id: AgentId,
    pub(crate) identity: Arc<()>,
    pub(crate) state: Mutex<ActorState>,
    policy_changed: Event,
    pub(crate) queue: Arc<ActorQueue>,
    pub(crate) outcomes: Mutex<HashMap<TurnId, TurnOutcome>>,
    pub(crate) latest: Mutex<Option<TurnOutcome>>,
    pub(crate) usage: Mutex<TokenUsage>,
    pub(crate) tickets: Mutex<HashMap<TurnId, TurnTicket>>,
    pub(crate) managed_admission: Option<ManagedTurnAdmission>,
    admission_preparation: Option<AdmissionPreparation>,
    #[cfg(test)]
    pub(crate) after_pop: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    pub(crate) after_finalization_retire: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
    #[cfg(test)]
    before_snapshot_state: Mutex<Option<(flume::Sender<()>, flume::Receiver<()>)>>,
}

/// Lifecycle, run status, and the active turn's cancellation wiring. One
/// lock keeps admission/close and lifecycle + status snapshots consistent.
/// `cancelled_correlations` remembers correlations cancelled before their
/// work was pushed (precancel), so a later matching push is dropped or
/// terminalized immediately.
pub(crate) struct ActorState {
    pub(crate) lifecycle: ActorLifecycle,
    pub(crate) status: ActorStatus,
    pub(crate) active: Option<ActiveCancel>,
    pub(crate) cancelled_correlations: HashMap<String, TurnCancellationReason>,
    pub(crate) cancelled_turns: HashSet<TurnId>,
    pub(crate) cancellation_generation: u64,
    pub(crate) policy_generation: u64,
    pub(crate) policy: Option<Arc<EffectiveAgentConfig>>,
    pending_policy: VecDeque<PendingPolicy>,
    deferred_admissions: VecDeque<DeferredAdmission>,
    next_policy_id: u64,
}

struct PendingPolicy {
    id: u64,
    result: Option<Result<RunSettings, ActorError>>,
    completion: Arc<Mutex<Option<Result<(), ActorError>>>>,
}

enum DeferredAdmission {
    Turn {
        after: u64,
        input: AgentInput,
        admission: Option<crate::agent::TurnAdmissionSnapshot>,
        event_sender: Option<EventSender>,
        correlation: String,
        ticket: TurnTicket,
    },
    Root {
        after: u64,
        root: RootWork,
    },
}

impl DeferredAdmission {
    fn after(&self) -> u64 {
        match self {
            Self::Turn { after, .. } | Self::Root { after, .. } => *after,
        }
    }

    fn projection(&self) -> QueueProjection {
        match self {
            Self::Turn { correlation, .. } => QueueProjection::Turn(correlation.clone()),
            Self::Root { root, .. } => root.into(),
        }
    }

    fn visible(&self) -> bool {
        matches!(self, Self::Root { root, .. } if !root.displayed)
    }
}

pub struct PolicyUpdateTicket {
    inner: Arc<ActorInner>,
    id: u64,
    completion: Arc<Mutex<Option<Result<(), ActorError>>>>,
}

impl ActorInner {
    fn validate_policy(&self, policy: &RunSettings) -> Result<(), ActorError> {
        let Some(ceiling) = self
            .managed_admission
            .as_ref()
            .and_then(|admission| admission.ceiling.as_ref())
        else {
            return Ok(());
        };
        if !Arc::ptr_eq(&policy.provider, &ceiling.provider)
            || policy.model.spec() != ceiling.model.spec()
            || (policy.fast && !ceiling.fast)
            || (policy.workflow && !ceiling.workflow)
            || policy.thinking != ceiling.thinking
        {
            return Err(ActorError::PolicyCeiling);
        }
        Ok(())
    }
}

impl PolicyUpdateTicket {
    pub fn resolve(&self, result: Result<RunSettings, ActorError>) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        let pending = state
            .pending_policy
            .iter_mut()
            .find(|pending| pending.id == self.id)
            .ok_or(ActorError::PolicyCancelled)?;
        if pending.result.is_some() {
            return Err(ActorError::PolicyCancelled);
        }
        let result = result.and_then(|policy| {
            self.inner.validate_policy(&policy)?;
            Ok(policy)
        });
        let rejected = result.as_ref().err().cloned();
        pending.result = Some(result);
        flush_policy_updates(&self.inner, &mut state);
        rejected.map_or(Ok(()), Err)
    }

    pub fn cancel(&self) -> Result<(), ActorError> {
        self.resolve(Err(ActorError::PolicyCancelled))
    }

    fn pending_result(&self) -> Result<Option<Result<(), ActorError>>, ActorError> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if state
            .pending_policy
            .iter()
            .any(|pending| pending.id == self.id)
        {
            return Ok(None);
        }
        Ok(Some(
            self.completion
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone()
                .unwrap_or(Ok(())),
        ))
    }

    pub async fn wait(&self) -> Result<(), ActorError> {
        loop {
            let listener = self.inner.policy_changed.listen();
            if let Some(result) = self.pending_result()? {
                return result;
            }
            listener.await;
        }
    }
}

impl Drop for PolicyUpdateTicket {
    fn drop(&mut self) {
        let _ = self.cancel();
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
    admissions: Vec<DeferredAdmission>,
    reason: TurnCancellationReason,
) {
    for admission in admissions {
        if let DeferredAdmission::Turn {
            input,
            event_sender,
            correlation,
            ticket,
            ..
        } = admission
        {
            let turn_id = ticket.turn_id();
            let outcome = cancelled_outcome(inner.agent_id, turn_id, reason);
            let admission = TurnAdmission {
                turn_id,
                input: Some(input),
                event_sender,
                correlation,
                root: false,
                generation: 0,
                policy: None,
                admission: None,
                ticket,
            };
            finalize_turn(inner, turn_id, outcome, Some(&admission), true);
        }
    }
}

fn flush_policy_updates(inner: &ActorInner, state: &mut ActorState) {
    while state
        .pending_policy
        .front()
        .is_some_and(|pending| pending.result.is_some())
    {
        let pending = state.pending_policy.pop_front().unwrap();
        let result = pending.result.unwrap();
        *pending.completion.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(result.as_ref().map(|_| ()).map_err(Clone::clone));
        if let Ok(policy) = result {
            state.policy_generation = state.policy_generation.wrapping_add(1);
            state.policy = Some(Arc::new(policy));
            inner.queue.push(ActorWork::PolicyBarrier {
                generation: state.policy_generation,
            });
        }
        while state
            .deferred_admissions
            .front()
            .is_some_and(|admission| admission.after() == pending.id)
        {
            match state.deferred_admissions.pop_front().unwrap() {
                DeferredAdmission::Turn {
                    input,
                    admission: snapshot,
                    event_sender,
                    correlation,
                    ticket,
                    ..
                } => {
                    let turn_id = ticket.turn_id();
                    let admission = TurnAdmission {
                        turn_id,
                        admission: snapshot,
                        input: Some(input),
                        event_sender,
                        correlation,
                        root: false,
                        generation: state.policy_generation,
                        policy: state.policy.clone(),
                        ticket,
                    };
                    inner.queue.push(ActorWork::Turn(admission));
                }
                DeferredAdmission::Root { mut root, .. } => {
                    if !state.cancelled_correlations.contains_key(&root.correlation) {
                        root.generation = state.policy_generation;
                        root.policy = state.policy.clone();
                        inner.queue.push(ActorWork::Root(root));
                    }
                }
            }
        }
        inner.policy_changed.notify(usize::MAX);
    }
}

impl ActorState {
    fn idle(policy: Option<Arc<EffectiveAgentConfig>>) -> Self {
        Self {
            lifecycle: ActorLifecycle::Open,
            status: ActorStatus::Idle,
            active: None,
            cancelled_correlations: HashMap::new(),
            cancelled_turns: HashSet::new(),
            cancellation_generation: 0,
            policy_generation: 0,
            policy,
            pending_policy: VecDeque::new(),
            deferred_admissions: VecDeque::new(),
            next_policy_id: 0,
        }
    }
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
/// event recipient can observe the outcome.
pub(crate) fn retire_turn(inner: &ActorInner, turn_id: TurnId, outcome: &TurnOutcome) -> bool {
    let mut outcomes = inner.outcomes.lock().unwrap_or_else(|e| e.into_inner());
    let std::collections::hash_map::Entry::Vacant(vacant) = outcomes.entry(turn_id) else {
        return false;
    };
    let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
    let mut tickets = inner.tickets.lock().unwrap_or_else(|e| e.into_inner());
    vacant.insert(outcome.clone());
    state.cancelled_turns.remove(&turn_id);
    tickets.remove(&turn_id);
    drop(tickets);
    drop(state);
    *inner.latest.lock().unwrap_or_else(|e| e.into_inner()) = Some(outcome.clone());
    *inner.usage.lock().unwrap_or_else(|e| e.into_inner()) += outcome.usage();
    true
}

pub(crate) fn publish_turn(outcome: TurnOutcome, admission: Option<&TurnAdmission>, deliver: bool) {
    if deliver
        && let Some(admission) = admission
        && let Some(sender) = &admission.event_sender
    {
        let _ = sender.send(AgentEvent::TurnOutcome(outcome.clone()));
    }
    if let Some(admission) = admission {
        admission.ticket.resolve(outcome);
    }
}

pub(crate) fn finalize_turn(
    inner: &ActorInner,
    turn_id: TurnId,
    outcome: TurnOutcome,
    admission: Option<&TurnAdmission>,
    deliver: bool,
) {
    if retire_turn(inner, turn_id, &outcome) {
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
        publish_turn(outcome, admission, deliver);
    }
}

fn terminalize_work(inner: &ActorInner, drained: Vec<ActorWork>, reason: TurnCancellationReason) {
    for work in drained {
        if let ActorWork::Turn(admission) = work {
            let outcome = cancelled_outcome(inner.agent_id, admission.turn_id, reason);
            finalize_turn(inner, admission.turn_id, outcome, Some(&admission), true);
        }
    }
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

/// Stable handle to one actor task. Cloneable; every clone shares the same
/// history, queue, and retained outcomes.
#[derive(Clone)]
pub struct AgentActorHandle {
    inner: Arc<ActorInner>,
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
        )
    }

    pub(crate) fn spawn_managed(
        agent_id: AgentId,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
        managed_admission: ManagedTurnAdmission,
        initial_config: Option<EffectiveAgentConfig>,
    ) -> (Self, smol::Task<()>) {
        Self::spawn_inner(
            agent_id,
            initial_messages,
            shared_messages,
            backend,
            Some(managed_admission),
            initial_config.map(Arc::new),
        )
    }

    fn spawn_inner(
        agent_id: AgentId,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
        managed_admission: Option<ManagedTurnAdmission>,
        initial_config: Option<Arc<EffectiveAgentConfig>>,
    ) -> (Self, smol::Task<()>) {
        let history = match shared_messages {
            Some(mirror) => History::restored(initial_messages).with_mirror(mirror),
            None => History::restored(initial_messages),
        };
        let admission_preparation = backend.admission_preparation();
        let inner = Arc::new(ActorInner {
            agent_id,
            identity: Arc::new(()),
            state: Mutex::new(ActorState::idle(initial_config)),
            policy_changed: Event::new(),
            queue: Arc::new(ActorQueue::new()),
            outcomes: Mutex::new(HashMap::new()),
            latest: Mutex::new(None),
            usage: Mutex::new(TokenUsage::default()),
            tickets: Mutex::new(HashMap::new()),
            managed_admission,
            admission_preparation,
            #[cfg(test)]
            after_pop: Mutex::new(None),
            #[cfg(test)]
            after_finalization_retire: Mutex::new(None),
            #[cfg(test)]
            before_snapshot_state: Mutex::new(None),
        });
        let wake = Arc::new(runner::WakeFlag::new());
        let handle = Self {
            inner: Arc::clone(&inner),
            wake: Arc::clone(&wake),
        };
        let task = smol::spawn(runner::Runner::new(inner, history, backend, wake).run());
        (handle, task)
    }

    pub fn agent_id(&self) -> AgentId {
        self.inner.agent_id
    }

    pub(crate) fn same_actor(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner.identity, &other.inner.identity)
    }

    pub(crate) fn owns_ticket(&self, ticket: &TurnTicket) -> bool {
        ticket.belongs_to(&self.inner.identity)
    }

    #[cfg(test)]
    fn pause_after_next_pop(&self) -> (flume::Receiver<()>, flume::Sender<()>) {
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

    /// Admits one turn. The [`TurnId`] and ticket are allocated immediately
    /// and the turn is queued strictly behind every already admitted turn.
    /// Admission and close linearize under one lock, so a concurrent
    /// `close()`/`shutdown()` cannot admit a turn after the queue drains.
    pub fn admit_turn(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
    ) -> Result<TurnTicket, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if let Some(after) = state.pending_policy.back().map(|pending| pending.id) {
            if self
                .inner
                .managed_admission
                .as_ref()
                .and_then(|admission| admission.mode_ceiling.as_ref())
                .is_some_and(|ceiling| &input.mode != ceiling)
            {
                return Err(ActorError::PolicyCeiling);
            }
            let turn_id = TurnId::generate();
            let ticket = TurnTicket::new(turn_id, Arc::clone(&self.inner.identity));
            let snapshot = self
                .inner
                .admission_preparation
                .as_ref()
                .map(|prepare| prepare(&input));
            if let Some(reason) = state.cancelled_correlations.get(&correlation).copied() {
                let admission = TurnAdmission {
                    turn_id,
                    admission: snapshot,
                    input: Some(input),
                    event_sender,
                    correlation,
                    root: false,
                    generation: state.policy_generation,
                    policy: state.policy.clone(),
                    ticket: ticket.clone(),
                };
                let outcome = cancelled_outcome(self.inner.agent_id, turn_id, reason);
                drop(state);
                finalize_turn(&self.inner, turn_id, outcome, Some(&admission), true);
                return Ok(ticket);
            }
            self.inner
                .tickets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(turn_id, ticket.clone());
            state
                .deferred_admissions
                .push_back(DeferredAdmission::Turn {
                    after,
                    admission: snapshot,
                    input,
                    event_sender,
                    correlation,
                    ticket: ticket.clone(),
                });
            return Ok(ticket);
        }
        self.admit_turn_locked(state, input, event_sender, correlation)
    }

    fn admit_turn_locked(
        &self,
        state: std::sync::MutexGuard<'_, ActorState>,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
    ) -> Result<TurnTicket, ActorError> {
        if self
            .inner
            .managed_admission
            .as_ref()
            .and_then(|admission| admission.mode_ceiling.as_ref())
            .is_some_and(|ceiling| &input.mode != ceiling)
        {
            return Err(ActorError::PolicyCeiling);
        }
        let turn_id = TurnId::generate();
        let ticket = TurnTicket::new(turn_id, Arc::clone(&self.inner.identity));
        if let Some(reason) = state.cancelled_correlations.get(&correlation) {
            // Precancelled before admission: terminalize exactly once with the
            // remembered reason, deliver, and resolve the waiter immediately.
            // The mark stays until a matching run is consumed by the runner.
            let reason = *reason;
            let admission = TurnAdmission {
                turn_id,
                admission: self
                    .inner
                    .admission_preparation
                    .as_ref()
                    .map(|prepare| prepare(&input)),
                input: Some(input),
                event_sender,
                correlation: correlation.clone(),
                root: false,
                generation: state.policy_generation,
                policy: state.policy.clone(),
                ticket: ticket.clone(),
            };
            let outcome = cancelled_outcome(self.inner.agent_id, turn_id, reason);
            drop(state);
            finalize_turn(
                &self.inner,
                turn_id,
                outcome.clone(),
                Some(&admission),
                true,
            );
            info!(
                agent_id = %self.inner.agent_id,
                %turn_id,
                correlation = %correlation,
                ?reason,
                "precancelled admission terminalized"
            );
            return Ok(ticket);
        }
        self.inner
            .tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(turn_id, ticket.clone());
        self.inner.queue.push(ActorWork::Turn(TurnAdmission {
            turn_id,
            admission: self
                .inner
                .admission_preparation
                .as_ref()
                .map(|prepare| prepare(&input)),
            input: Some(input),
            event_sender,
            correlation: correlation.clone(),
            root: false,
            generation: state.policy_generation,
            policy: state.policy.clone(),
            ticket: ticket.clone(),
        }));
        info!(
            agent_id = %self.inner.agent_id,
            %turn_id,
            correlation = %correlation,
            "turn admitted"
        );
        Ok(ticket)
    }

    /// Queues a root input. It has no [`TurnId`]: the scheduler assigns one
    /// when it starts it, and an active run folds it instead. A root whose
    /// correlation was precancelled is dropped. The mark stays until a
    /// matching run is consumed by the runner.
    pub fn rush(&self, root: RootWork) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if let Some(after) = state.pending_policy.back().map(|pending| pending.id) {
            if self
                .inner
                .managed_admission
                .as_ref()
                .is_some_and(|admission| admission.ceiling.is_some())
                && root.input.mode != crate::AgentMode::Build
            {
                return Err(ActorError::PolicyCeiling);
            }
            let mut root = root;
            root.admission = self
                .inner
                .admission_preparation
                .as_ref()
                .map(|prepare| prepare(&root.input));
            state
                .deferred_admissions
                .push_back(DeferredAdmission::Root { after, root });
            return Ok(());
        }
        self.rush_locked(&state, root)
    }

    fn rush_locked(&self, state: &ActorState, root: RootWork) -> Result<(), ActorError> {
        if self
            .inner
            .managed_admission
            .as_ref()
            .is_some_and(|admission| admission.ceiling.is_some())
            && root.input.mode != crate::AgentMode::Build
        {
            return Err(ActorError::PolicyCeiling);
        }
        if state.cancelled_correlations.contains_key(&root.correlation) {
            return Ok(());
        }
        let mut root = root;
        root.generation = state.policy_generation;
        root.policy = state.policy.clone();
        root.admission = self
            .inner
            .admission_preparation
            .as_ref()
            .map(|prepare| prepare(&root.input));
        self.inner.queue.push(ActorWork::Root(root));
        Ok(())
    }

    /// Installs a policy snapshot for subsequent admissions. The queued barrier
    /// keeps earlier work ahead of the policy change in FIFO order.
    pub fn effective_config(&self) -> Option<Arc<EffectiveAgentConfig>> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .policy
            .clone()
    }

    pub fn policy_snapshot(&self) -> Option<Arc<RunSettings>> {
        self.effective_config()
    }

    pub fn update_policy(&self, policy: RunSettings) -> Result<u64, ActorError> {
        self.set_effective_config(policy)
    }

    pub fn reserve_policy_update(&self) -> Result<PolicyUpdateTicket, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        state.next_policy_id = state.next_policy_id.wrapping_add(1);
        let id = state.next_policy_id;
        let completion = Arc::new(Mutex::new(None));
        state.pending_policy.push_back(PendingPolicy {
            id,
            result: None,
            completion: Arc::clone(&completion),
        });
        Ok(PolicyUpdateTicket {
            inner: Arc::clone(&self.inner),
            id,
            completion,
        })
    }

    pub fn admit_turn_after_policy(
        &self,
        input: AgentInput,
        event_sender: Option<EventSender>,
        correlation: String,
    ) -> std::future::Ready<Result<TurnTicket, ActorError>> {
        std::future::ready(self.admit_turn(input, event_sender, correlation))
    }

    pub fn rush_after_policy(&self, root: RootWork) -> std::future::Ready<Result<(), ActorError>> {
        std::future::ready(self.rush(root))
    }

    fn has_pending_policy_updates(&self) -> Result<bool, ActorError> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        Ok(!state.pending_policy.is_empty())
    }

    pub async fn wait_policy_updates(&self) -> Result<(), ActorError> {
        loop {
            let listener = self.inner.policy_changed.listen();
            if !self.has_pending_policy_updates()? {
                return Ok(());
            }
            listener.await;
        }
    }

    pub fn set_effective_config(&self, config: EffectiveAgentConfig) -> Result<u64, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(match state.lifecycle {
                ActorLifecycle::Closed => ActorError::Closed,
                ActorLifecycle::Shutdown => ActorError::Shutdown,
                ActorLifecycle::Open => unreachable!(),
            });
        }
        if !state.pending_policy.is_empty() {
            return Err(ActorError::PolicyPending);
        }
        self.inner.validate_policy(&config)?;
        state.policy_generation = state.policy_generation.wrapping_add(1);
        state.policy = Some(Arc::new(config));
        let generation = state.policy_generation;
        self.inner
            .queue
            .push(ActorWork::PolicyBarrier { generation });
        Ok(generation)
    }

    pub fn push_control(&self, control: ControlWork) -> Result<(), ActorError> {
        self.push_checked(ActorWork::Control(control))
    }

    pub fn push_compact(
        &self,
        run_id: u64,
        instructions: Option<String>,
    ) -> Result<(), ActorError> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
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
        self.inner.queue.push(ActorWork::Compact {
            run_id,
            instructions,
        });
        Ok(())
    }

    /// Removes an admitted turn from the queue and terminalizes it instead
    /// of stranding it.
    pub fn remove(&self, turn_id: TurnId) -> Result<TurnOutcome, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let deferred = state
            .deferred_admissions
            .iter()
            .position(|admission| matches!(admission, DeferredAdmission::Turn { ticket, .. } if ticket.turn_id() == turn_id))
            .and_then(|index| state.deferred_admissions.remove(index));
        let queued = self.inner.queue.remove_turn(turn_id);
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
        finalize_turn(
            &self.inner,
            turn_id,
            outcome.clone(),
            Some(&admission),
            true,
        );
        Ok(outcome)
    }

    /// Removes the queue item at raw `index` (the same index
    /// [`snapshot`](Self::snapshot) reports) under the queue lock. Admitted
    /// turns are terminalized exactly once with `User` and delivered; roots,
    /// compacts, and controls are dropped. Policy barriers are invariant and
    /// cannot be removed. Returns the removed item's projection, or `None`
    /// when the raw index is out of bounds or points to a barrier.
    pub fn remove_at(&self, index: usize) -> Option<QueueProjection> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let queued = self.inner.queue.len();
        if index >= queued {
            let admission = state.deferred_admissions.remove(index - queued)?;
            let projection = admission.projection();
            drop(state);
            settle_deferred(&self.inner, vec![admission], TurnCancellationReason::User);
            return Some(projection);
        }
        let work = self.inner.queue.remove_at(index)?;
        drop(state);
        let projection = (&work).into();
        if let ActorWork::Turn(admission) = work {
            let outcome = cancelled_outcome(
                self.inner.agent_id,
                admission.turn_id,
                TurnCancellationReason::User,
            );
            finalize_turn(
                &self.inner,
                admission.turn_id,
                outcome,
                Some(&admission),
                true,
            );
        }
        Some(projection)
    }

    /// Removes the `visible_index`-th item the TUI panel displays (deferred
    /// roots and compacts; admitted turns, controls, and already-displayed
    /// roots are hidden rows), under the queue lock. Returns the projection
    /// of the removed item, or `None` when the panel has fewer rows.
    pub fn remove_visible_at(&self, visible_index: usize) -> Option<QueueProjection> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let queued_visible = self
            .inner
            .queue
            .snapshot()
            .iter()
            .filter(|work| {
                matches!(
                    work,
                    QueueProjection::Message {
                        displayed: false,
                        ..
                    } | QueueProjection::Compact(_)
                )
            })
            .count();
        if visible_index >= queued_visible {
            let index = state
                .deferred_admissions
                .iter()
                .enumerate()
                .filter(|(_, admission)| admission.visible())
                .nth(visible_index - queued_visible)?
                .0;
            let admission = state.deferred_admissions.remove(index)?;
            let projection = admission.projection();
            drop(state);
            settle_deferred(&self.inner, vec![admission], TurnCancellationReason::User);
            return Some(projection);
        }
        let (work, projection) = self.inner.queue.remove_visible_at(visible_index)?;
        drop(state);
        if let ActorWork::Turn(admission) = work {
            let outcome = cancelled_outcome(
                self.inner.agent_id,
                admission.turn_id,
                TurnCancellationReason::User,
            );
            finalize_turn(
                &self.inner,
                admission.turn_id,
                outcome,
                Some(&admission),
                true,
            );
        }
        Some(projection)
    }

    /// Clears every queued item, terminalizing the admitted turns. Returns
    /// the number of items removed.
    pub fn clear(&self) -> usize {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let deferred = state.deferred_admissions.drain(..).collect::<Vec<_>>();
        let drained = self.inner.queue.drain_all();
        let len = drained.len() + deferred.len();
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
        state.cancellation_generation = state.cancellation_generation.wrapping_add(1);
        for pending in &mut state.pending_policy {
            pending.result = Some(Err(ActorError::PolicyCancelled));
        }
        flush_policy_updates(&self.inner, &mut state);
        let deferred = state.deferred_admissions.drain(..).collect::<Vec<_>>();
        let active = state.active.take();
        let drained = self.inner.queue.drain_all();
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
        if state.lifecycle != ActorLifecycle::Open {
            return Err(match state.lifecycle {
                ActorLifecycle::Closed => ActorError::Closed,
                ActorLifecycle::Shutdown => ActorError::Shutdown,
                ActorLifecycle::Open => unreachable!(),
            });
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
        let active = if state.status == ActorStatus::Running(turn_id) {
            state.active.take()
        } else {
            None
        };
        let queued = self.inner.queue.remove_turn(turn_id);
        let deferred = state.deferred_admissions.iter().position(|admission| {
            matches!(admission, DeferredAdmission::Turn { ticket, .. } if ticket.turn_id() == turn_id)
        }).and_then(|index| state.deferred_admissions.remove(index));
        if active.is_none()
            && queued.is_none()
            && deferred.is_none()
            && state.lifecycle == ActorLifecycle::Open
            && state.status == ActorStatus::Idle
        {
            state.cancelled_turns.insert(turn_id);
        }
        drop(state);
        settle_deferred(
            &self.inner,
            deferred.into_iter().collect(),
            TurnCancellationReason::User,
        );
        if let Some(active) = active {
            active.fire(TurnCancellationReason::User);
        }
        if let Some(admission) = queued {
            let outcome = cancelled_outcome(
                self.inner.agent_id,
                admission.turn_id,
                TurnCancellationReason::User,
            );
            finalize_turn(
                &self.inner,
                admission.turn_id,
                outcome,
                Some(&admission),
                true,
            );
        }
        Ok(())
    }

    /// Closes the actor: admissions are rejected, the active turn is
    /// cancelled with `Closed`, queued turns terminalize, and the runner
    /// task exits.
    pub fn close(&self) {
        self.close_internal(ActorLifecycle::Closed, TurnCancellationReason::Closed);
    }

    pub fn shutdown(&self) {
        self.close_internal(ActorLifecycle::Shutdown, TurnCancellationReason::Shutdown);
    }

    /// Cancels work matching one correlation: the active run if its
    /// correlation matches (first reason wins), every queue item carrying
    /// that correlation (admitted turns are terminalized exactly once and
    /// delivered, roots/compacts are dropped), and remembers the correlation
    /// so a later push with it is precancelled. Unrelated work is untouched,
    /// and the actor stays open and reusable.
    pub fn cancel_correlation(&self, correlation: &str, reason: TurnCancellationReason) {
        self.cancel_correlation_with_active(correlation, reason, |_| {});
    }

    pub fn cancel_correlation_with_active(
        &self,
        correlation: &str,
        reason: TurnCancellationReason,
        operation: impl FnOnce(TurnId),
    ) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let matched_active = state
            .active
            .as_ref()
            .is_some_and(|a| a.correlation() == Some(correlation));
        let active = if matched_active {
            let active = state.active.take();
            match state.status {
                ActorStatus::Running(turn_id) => operation(turn_id),
                ActorStatus::Idle => {}
            }
            active
        } else {
            None
        };
        // Scan the queue under the state lock so the request linearizes with
        // admission/close; turn out the matching items.
        let matched: Vec<ActorWork> = self
            .inner
            .queue
            .remove_correlation(correlation)
            .into_iter()
            .collect();
        let mut deferred = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(admission) = state.deferred_admissions.pop_front() {
            let matches = match &admission {
                DeferredAdmission::Turn {
                    correlation: key, ..
                } => key == correlation,
                DeferredAdmission::Root { root, .. } => root.correlation == correlation,
            };
            if matches {
                deferred.push(admission);
            } else {
                remaining.push_back(admission);
            }
        }
        state.deferred_admissions = remaining;
        if !matched_active && matched.is_empty() && deferred.is_empty() {
            // Nothing matched now; precancel any later push with this
            // correlation, remembering the reason to terminalize with.
            state
                .cancelled_correlations
                .insert(correlation.to_owned(), reason);
        }
        drop(state);
        settle_deferred(&self.inner, deferred, reason);

        for work in matched {
            if let ActorWork::Turn(admission) = work {
                let outcome = cancelled_outcome(self.inner.agent_id, admission.turn_id, reason);
                finalize_turn(
                    &self.inner,
                    admission.turn_id,
                    outcome,
                    Some(&admission),
                    true,
                );
            }
        }
        if let Some(active) = active {
            active.fire(reason);
        }
        info!(
            agent_id = %self.inner.agent_id,
            %correlation,
            ?reason,
            "correlation cancelled"
        );
    }

    /// A scheduler view of the queue, usable as the running agent's
    /// [`InterruptSource`].
    pub fn interrupt_source(&self) -> Arc<dyn InterruptSource> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let generation = state.cancellation_generation;
        let policy_generation = state.policy_generation;
        Arc::new(queue::InterruptQueue::new(
            Arc::clone(&self.inner),
            generation,
            policy_generation,
            None,
        ))
    }

    /// Runs the drain publication only when the queue is empty, under the
    /// queue lock, so a drain event can never interleave with a concurrent
    /// push.
    pub fn publish_if_empty(&self, publish: impl FnOnce()) {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.deferred_admissions.is_empty() {
            self.inner.queue.publish_if_empty(publish);
        }
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
                .deferred_admissions
                .iter()
                .map(DeferredAdmission::projection),
        );
        drop(state);
        ActorSnapshot {
            lifecycle,
            status,
            active_turn,
            queued: queue.len(),
            queue,
            latest: self
                .inner
                .latest
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            cumulative_usage: *self.inner.usage.lock().unwrap_or_else(|e| e.into_inner()),
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

    fn push_checked(&self, work: ActorWork) -> Result<(), ActorError> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(match state.lifecycle {
                ActorLifecycle::Closed => ActorError::Closed,
                ActorLifecycle::Shutdown => ActorError::Shutdown,
                ActorLifecycle::Open => unreachable!(),
            });
        }
        self.inner.queue.push(work);
        Ok(())
    }

    fn close_internal(&self, lifecycle: ActorLifecycle, reason: TurnCancellationReason) {
        let (active, deferred) = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            // First terminal lifecycle/reason wins: repeated close/shutdown are
            // idempotent no-ops, so a race cannot overwrite Closed with Shutdown
            // or vice versa.
            if state.lifecycle != ActorLifecycle::Open {
                return;
            }
            state.lifecycle = lifecycle;
            state.pending_policy.clear();
            self.inner.policy_changed.notify(usize::MAX);
            state.cancelled_correlations.clear();
            state.cancelled_turns.clear();
            (
                state.active.take(),
                state.deferred_admissions.drain(..).collect::<Vec<_>>(),
            )
        };
        settle_deferred(&self.inner, deferred, reason);
        if let Some(active) = active {
            active.fire(reason);
        }
        let drained = self.inner.queue.drain_all();
        terminalize_work(&self.inner, drained, reason);
        self.inner.queue.notify();
        self.wake.wake();
        info!(agent_id = %self.inner.agent_id, ?reason, ?lifecycle, "actor closed");
    }
}
