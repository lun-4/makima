//! Per-outer-session agent graph and production actor factory.
//!
//! The manager owns topology, actor tasks, graph limits, and managed turn
//! admission. Node ids describe topology but grant no authority: descendants
//! can only be created with a manager-issued token for a currently executing
//! turn. Idle reservation takes the graph lock before the actor state lock;
//! actor callbacks run without the state lock, and no graph lock is held across awaits.

mod manager_error;
#[cfg(test)]
mod report_restart_tests;
#[cfg(test)]
mod restore_tests;
#[cfg(test)]
mod result_tests;
mod rewind;
#[cfg(test)]
mod rewind_tests;
#[cfg(test)]
mod tests;
mod transition;
mod types;
mod work;
pub(crate) use work::{WorkAdmission, WorkLedger, WorkReservation};

pub use manager_error::{ManagerError, ReactivationError};
pub use rewind::ManagerRewindGuard;
pub use transition::ManagerTransitionGuard;
pub use types::{
    AgentIncarnation, AgentLimits, AgentMetadata, AgentNodeSnapshot, AgentRef, AgentResidency,
    CurrentManagedTurn, GraphLifecycle, ManagedPromptWait, PromptWaitError, ReactivationInput,
    ResidentAgent, ResidentInitialization, ResidentInitializer, ShutdownReport, TurnPermitLease,
};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::{Future, poll_fn};
use std::io::{self, Error as IoError, ErrorKind};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use async_lock::{Semaphore, SemaphoreGuardArc};
use event_listener::Event;
use makima_providers::{Message, TokenUsage};
use tracing::{info, warn};

use crate::actor::delivery::{ChildDelivery, DeliveryRejection};
use crate::actor::{
    ActorCloseCut, ActorIdleGuard, ActorRestoration, ManagedTurnAdmission, TranscriptRequest,
    TranscriptSnapshot,
};
#[cfg(any(test, feature = "test-util"))]
use crate::journal::{GraphRestartError, GraphStore};
use crate::journal::{
    JournalChange, JournalCommit, JournalIncarnation, SessionJournal, SessionJournalSnapshot,
};
use crate::tools::offload::OffloadStore;
use crate::{
    ActorBackend, ActorError, ActorEvent, ActorLifecycle, ActorStatus, AgentActorHandle, AgentId,
    HistorySnapshot, SharedMessages, TurnCancellationReason, TurnId, TurnOutcome,
};
use makima_storage::session_state::{
    SessionStateView, StateChange, StoredAgentLifecycle, StoredAgentRecord, StoredAgentSpec,
    StoredDeliveryOp, StoredOffloadRef,
};

const OPAQUE_BACKEND: &str = "agent backend has no stored restoration specification";
const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(any(test, feature = "test-util"))]
const MAX_GRAPH_DRAIN_PASSES: usize = 256;
static NEXT_MANAGER_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_TURN_NONCE: AtomicU64 = AtomicU64::new(1);

struct ActivationAttempt {
    result: Mutex<Option<Result<(), ManagerError>>>,
    completed: Event,
}

impl ActivationAttempt {
    fn new() -> Self {
        Self {
            result: Mutex::new(None),
            completed: Event::new(),
        }
    }

    fn complete(&self, result: Result<(), ManagerError>) {
        let mut current = self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if current.is_none() {
            *current = Some(result);
            self.completed.notify(usize::MAX);
        }
    }

    async fn wait(&self) -> Result<(), ManagerError> {
        loop {
            let listener = self.completed.listen();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
            {
                return result;
            }
            listener.await;
        }
    }
}

struct ActivationGuard {
    manager: AgentManagerHandle,
    incarnation: AgentIncarnation,
    attempt: Arc<ActivationAttempt>,
    committed: bool,
}

impl Drop for ActivationGuard {
    fn drop(&mut self) {
        if !self.committed {
            self.manager.rollback_activation(&self.incarnation);
            self.attempt
                .complete(Err(ReactivationError::Cancelled.into()));
        }
    }
}

type DeliveryWork = Mutex<HashMap<(AgentId, AgentId, TurnId), Vec<WorkReservation>>>;
type RunnerTask = smol::Task<()>;

pub struct GraphEvents {
    manager: Weak<ManagerInner>,
    events: flume::Receiver<(AgentId, ActorEvent)>,
}

impl GraphEvents {
    /// The next event and its agent, whether or not that agent is still live.
    pub async fn recv(&self) -> Option<(AgentRef, ActorEvent)> {
        let (agent_id, event) = self.events.recv_async().await.ok()?;
        let manager = AgentManagerHandle(self.manager.upgrade()?);
        Some((manager.reference(agent_id), event))
    }
}

impl AgentManagerHandle {
    pub fn prepare_idle_subtree(&self, agent_id: AgentId) -> Result<ActorIdleGuard, ManagerError> {
        let graph = self.lock_graph();
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        let root = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if root.lifecycle != GraphLifecycle::Live {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        let ids = Self::subtree_ids(&graph, agent_id);
        if graph.active_turns.keys().any(|(id, _)| ids.contains(id))
            || ids
                .iter()
                .skip(1)
                .any(|id| Self::descendant_busy(&graph.nodes[id]))
        {
            return Err(ManagerError::BusySubtree(agent_id));
        }
        let actor = root
            .actor
            .as_ref()
            .ok_or(ManagerError::NonLiveAgent(agent_id))?;
        actor.prepare_idle().map_err(|error| match error {
            ActorError::Closed | ActorError::Shutdown => ManagerError::NonLiveAgent(agent_id),
            _ => ManagerError::BusySubtree(agent_id),
        })
    }

    /// Live descendants stay registered after their turn (e.g. background
    /// tasks awaiting despawn), so only their actor's work makes them busy.
    fn descendant_busy(node: &Node) -> bool {
        match node.lifecycle {
            GraphLifecycle::Live => node
                .actor
                .as_ref()
                .is_none_or(AgentActorHandle::has_pending_work),
            lifecycle => lifecycle.consumes_capacity(),
        }
    }
}

struct Node {
    parent_id: Option<AgentId>,
    root_id: AgentId,
    depth: usize,
    children: Vec<AgentId>,
    lifecycle: GraphLifecycle,
    residency: AgentResidency,
    activation: Option<Arc<ActivationAttempt>>,
    actor: Option<AgentActorHandle>,
    task: Option<RunnerTask>,
    metadata: AgentMetadata,
    reservation: u64,
    reservation_pending: bool,
    detach_pending: bool,
    cancel_on_commit: bool,
    turn_owner: Option<(AgentId, TurnId)>,
    terminal: Arc<Mutex<Option<AgentNodeSnapshot>>>,
}

struct CapturedNodeSnapshot {
    agent_id: AgentId,
    parent_id: Option<AgentId>,
    root_id: AgentId,
    depth: usize,
    children: Vec<AgentId>,
    graph_lifecycle: GraphLifecycle,
    residency: AgentResidency,
    actor: Option<AgentActorHandle>,
    metadata: AgentMetadata,
}

impl CapturedNodeSnapshot {
    fn snapshot(self) -> AgentNodeSnapshot {
        AgentNodeSnapshot {
            agent_id: self.agent_id,
            parent_id: self.parent_id,
            root_id: self.root_id,
            depth: self.depth,
            children: self.children,
            graph_lifecycle: self.graph_lifecycle,
            residency: self.residency,
            actor: self.actor.map(|actor| actor.snapshot()),
            metadata: self.metadata,
        }
    }
}

#[derive(Default)]
struct WatcherRegistry {
    tasks: HashMap<u64, smol::Task<()>>,
    reapable: HashSet<u64>,
}

struct GraphState {
    root_id: Option<AgentId>,
    nodes: HashMap<AgentId, Node>,
    active_turns: HashMap<(AgentId, TurnId), u64>,
    revision: u64,
    next_reservation: u64,
    shutting_down: bool,
    rewinding: bool,
    rewind_affected: HashSet<AgentId>,
    rewind_quiescent: bool,
    observers: Vec<(u64, flume::Sender<(AgentId, ActorEvent)>)>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestGate {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
}

pub(crate) struct ManagerInner {
    generation: u64,
    limits: AgentLimits,
    graph: Mutex<GraphState>,
    pub(crate) journal: Arc<SessionJournal>,
    pub(crate) work: Arc<WorkLedger>,
    restored_delivery_work: DeliveryWork,
    result_store: Mutex<Option<Arc<OffloadStore>>>,
    cleanup_tasks: Mutex<Vec<RunnerTask>>,
    tool_completions: Mutex<Vec<(AgentId, RunnerTask)>>,
    limiter: Arc<Semaphore>,
    watchers: Mutex<WatcherRegistry>,
    next_watcher: AtomicU64,
    shutdown: AtomicBool,
    reaped: Event,
    #[cfg(test)]
    runner_deferred: Mutex<Option<flume::Sender<()>>>,
    #[cfg(test)]
    commit_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    prompt_wait_registration_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    descendant_cut_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    shutdown_cut_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    managed_acquire_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    managed_registration_rejected: Mutex<Option<flume::Sender<()>>>,
}

#[derive(Clone)]
pub struct AgentManagerHandle(Arc<ManagerInner>);

#[derive(Clone)]
pub struct WeakAgentManagerHandle(Weak<ManagerInner>);

impl WeakAgentManagerHandle {
    pub fn upgrade(&self) -> Option<AgentManagerHandle> {
        self.0.upgrade().map(AgentManagerHandle)
    }
}

impl AgentManagerHandle {
    pub fn downgrade(&self) -> WeakAgentManagerHandle {
        WeakAgentManagerHandle(Arc::downgrade(&self.0))
    }
    #[cfg(any(test, feature = "test-util"))]
    pub fn restore_empty_graph<S: GraphStore>(
        limits: AgentLimits,
        store: &S,
    ) -> Result<Self, GraphRestartError<S::Error>> {
        if !store
            .load()
            .map_err(GraphRestartError::Store)?
            .records()
            .is_empty()
        {
            return Err(GraphRestartError::NonemptyGraph);
        }
        Ok(Self::new(limits)?)
    }

    pub fn new(limits: AgentLimits) -> Result<Self, ManagerError> {
        Self::new_with_journal(limits, Arc::new(SessionJournal::default()))
    }

    pub fn new_with_journal(
        limits: AgentLimits,
        journal: Arc<SessionJournal>,
    ) -> Result<Self, ManagerError> {
        if limits.max_concurrent_agent_turns == 0
            || limits.max_agent_depth == 0
            || limits.max_children_per_agent == 0
            || limits.max_active_agents == 0
            || limits.max_retained_agents == 0
        {
            return Err(ManagerError::InvalidLimits);
        }
        Ok(AgentManagerHandle(Arc::new(ManagerInner {
            generation: NEXT_MANAGER_GENERATION.fetch_add(1, Ordering::Relaxed),
            limiter: Arc::new(Semaphore::new(limits.max_concurrent_agent_turns)),
            limits,
            work: WorkLedger::new(limits),
            restored_delivery_work: Mutex::new(HashMap::new()),
            result_store: Mutex::new(None),
            cleanup_tasks: Mutex::new(Vec::new()),
            tool_completions: Mutex::new(Vec::new()),
            journal,
            watchers: Mutex::new(WatcherRegistry::default()),
            next_watcher: AtomicU64::new(1),
            graph: Mutex::new(GraphState {
                root_id: None,
                nodes: HashMap::new(),
                active_turns: HashMap::new(),
                revision: 0,
                next_reservation: 1,
                shutting_down: false,
                rewinding: false,
                rewind_affected: HashSet::new(),
                rewind_quiescent: false,
                observers: Vec::new(),
            }),
            shutdown: AtomicBool::new(false),
            reaped: Event::new(),
            #[cfg(test)]
            runner_deferred: Mutex::new(None),
            #[cfg(test)]
            commit_gate: Mutex::new(None),
            #[cfg(test)]
            prompt_wait_registration_gate: Mutex::new(None),
            #[cfg(test)]
            descendant_cut_gate: Mutex::new(None),
            #[cfg(test)]
            shutdown_cut_gate: Mutex::new(None),
            #[cfg(test)]
            managed_acquire_gate: Mutex::new(None),
            #[cfg(test)]
            managed_registration_rejected: Mutex::new(None),
        })))
    }
}

impl AgentManagerHandle {
    pub fn restore_graph(
        limits: AgentLimits,
        journal: Arc<SessionJournal>,
        view: SessionStateView,
        histories: BTreeMap<AgentId, HistorySnapshot>,
    ) -> Result<Self, ManagerError> {
        let snapshot = journal
            .checkpoint_snapshot()
            .map_err(ManagerError::InvalidGraph)?;
        let folded =
            SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
        if folded != view
            || histories.len() != snapshot.histories.len()
            || histories.iter().any(|(id, history)| {
                snapshot.histories.get(id).is_none_or(|saved| {
                    saved.epoch != history.epoch
                        || saved.compacted != history.compacted
                        || saved.conversation != history.conversation
                        || saved.messages != history.messages
                })
            })
        {
            return Err(ManagerError::InvalidGraph(
                "recovery inputs do not match the journal checkpoint".into(),
            ));
        }
        if view.scopes.values().any(|scope| !scope.admitted.is_empty()) {
            return Err(ManagerError::InvalidGraph(
                "recovery has unterminated admissions".into(),
            ));
        }
        let roots: Vec<_> = view
            .agents
            .values()
            .filter(|record| record.parent.is_none())
            .collect();
        if roots.len() != 1 {
            return Err(ManagerError::InvalidGraph(
                "recovered graph must have exactly one root".into(),
            ));
        }
        let root_id = roots[0].id;
        if roots[0].depth != 0 {
            return Err(ManagerError::InvalidGraph("root depth must be zero".into()));
        }
        let manager = Self::new_with_journal(limits, journal)?;
        let mut nodes = HashMap::new();
        for record in view.agents.values() {
            let mut cursor = record;
            let mut ancestry = HashSet::new();
            while let Some(parent) = cursor.parent {
                if !ancestry.insert(cursor.id) {
                    return Err(ManagerError::InvalidGraph(
                        "agent ancestry contains a cycle".into(),
                    ));
                }
                let parent = view
                    .agents
                    .get(&parent)
                    .ok_or_else(|| ManagerError::InvalidGraph("agent parent is missing".into()))?;
                if parent.depth.checked_add(1) != Some(cursor.depth) {
                    return Err(ManagerError::InvalidGraph(
                        "agent depth does not match its parent".into(),
                    ));
                }
                cursor = parent;
            }
            if cursor.id != root_id || !histories.contains_key(&record.id) {
                return Err(ManagerError::InvalidGraph(
                    "agent has no root or transcript snapshot".into(),
                ));
            }
            nodes.insert(
                record.id,
                Node {
                    parent_id: record.parent,
                    root_id,
                    depth: record.depth as usize,
                    children: Vec::new(),
                    lifecycle: match record.lifecycle {
                        StoredAgentLifecycle::Open => GraphLifecycle::Dormant,
                        StoredAgentLifecycle::Closed => GraphLifecycle::Closed,
                        StoredAgentLifecycle::Retired => GraphLifecycle::Retired,
                    },
                    residency: AgentResidency::Dormant,
                    activation: None,
                    actor: None,
                    task: None,
                    metadata: AgentMetadata {
                        label: record.label.clone(),
                        spawned_by_tool_use_id: record.spawned_by_tool_use_id.clone(),
                        transcript_key: record.parent.map(|_| record.transcript_key.clone()),
                    },
                    reservation: 0,
                    reservation_pending: false,
                    detach_pending: false,
                    cancel_on_commit: false,
                    turn_owner: None,
                    terminal: Arc::new(Mutex::new(None)),
                },
            );
        }
        for record in view.agents.values() {
            if let Some(parent) = record.parent {
                nodes
                    .get_mut(&parent)
                    .ok_or_else(|| ManagerError::InvalidGraph("agent parent is missing".into()))?
                    .children
                    .push(record.id);
            }
        }
        for record in view.agents.values() {
            manager.0.journal.set_incarnation(record.id, 0);
            manager.0.work.register(record.id, record.parent, 0, false);
            if let Some(spec) = &record.spec {
                manager
                    .0
                    .work
                    .set_reporting(record.id, spec.auto_deliver && !spec.silent)?;
            }
        }
        for (scope, state) in &view.scopes {
            let Some(parent) = scope else {
                continue;
            };
            for (&(source, turn), delivery) in &state.deliveries {
                if matches!(delivery, StoredDeliveryOp::Queued(_)) {
                    let mut work = manager.0.work.restore_hold(source)?;
                    let work = work.take_report().ok_or_else(|| {
                        ManagerError::InvalidGraph(
                            "delivery source has no parent work reservation".into(),
                        )
                    })?;
                    if work.agent_id() != *parent {
                        return Err(ManagerError::InvalidGraph(
                            "delivery recipient is not its source parent".into(),
                        ));
                    }
                    manager
                        .0
                        .restored_delivery_work
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .insert((*parent, source, turn), vec![work]);
                }
            }
        }
        {
            let mut graph = manager.lock_graph();
            graph.root_id = Some(root_id);
            graph.nodes = nodes;
        }
        Ok(manager)
    }

    pub async fn ensure_resident_with<F, Fut>(
        &self,
        agent_id: AgentId,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(ReactivationInput) -> Fut,
        Fut: Future<Output = Result<ResidentAgent, ReactivationError>>,
    {
        let (attempt, incarnation, input) = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            if node.detach_pending {
                return Err(ReactivationError::Cancelled.into());
            }
            if node.lifecycle == GraphLifecycle::Retired {
                return Err(ManagerError::Retired(agent_id));
            }
            if node.lifecycle == GraphLifecycle::Live
                && node.residency == AgentResidency::Resident
                && node
                    .actor
                    .as_ref()
                    .is_some_and(|actor| actor.lifecycle() == ActorLifecycle::Open)
            {
                return Ok(AgentRef {
                    manager: self.clone(),
                    agent_id,
                    terminal: node.terminal.clone(),
                });
            }
            if let Some(attempt) = node.activation.clone() {
                (attempt, None, None)
            } else {
                if node.lifecycle != GraphLifecycle::Dormant {
                    return Err(ManagerError::NonLiveAgent(agent_id));
                }
                if !self.tool_completions_finished(Some(agent_id)) {
                    return Err(ReactivationError::ToolCompletionPending.into());
                }
                let record = self
                    .0
                    .journal
                    .agent(agent_id)
                    .ok_or(ManagerError::UnknownAgent(agent_id))?;
                if record.lifecycle != StoredAgentLifecycle::Open {
                    return Err(ManagerError::NonLiveAgent(agent_id));
                }
                if let Err(reason) = &record.restorable {
                    return Err(ReactivationError::NotRestorable {
                        reason: reason.clone(),
                    }
                    .into());
                }
                let snapshot = self
                    .0
                    .journal
                    .checkpoint_snapshot()
                    .map_err(ManagerError::InvalidGraph)?;
                let view = SessionStateView::fold(
                    &snapshot.state,
                    &snapshot.agent_revisions,
                    snapshot.seq,
                )
                .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
                let input = ReactivationInput {
                    config: view
                        .scopes
                        .get(&Some(agent_id))
                        .and_then(|scope| scope.config.clone()),
                    history: snapshot.histories.get(&agent_id).cloned().ok_or_else(|| {
                        ManagerError::InvalidGraph("agent transcript snapshot is missing".into())
                    })?,
                    record,
                };
                let reservation = Self::next_reservation(&mut graph);
                self.0.journal.set_incarnation(agent_id, reservation);
                self.0.work.bind_incarnation(agent_id, reservation)?;
                self.0.work.set_open(agent_id, false)?;
                let attempt = Arc::new(ActivationAttempt::new());
                let node = graph
                    .nodes
                    .get_mut(&agent_id)
                    .ok_or(ManagerError::UnknownAgent(agent_id))?;
                node.reservation = reservation;
                node.reservation_pending = true;
                node.residency = AgentResidency::Activating;
                node.activation = Some(attempt.clone());
                let incarnation = AgentIncarnation {
                    agent_id,
                    manager_generation: self.generation(),
                    reservation,
                };
                (attempt, Some(incarnation), Some(input))
            }
        };
        let Some(incarnation) = incarnation else {
            attempt.wait().await?;
            return self.lookup(agent_id).and_then(|agent| {
                agent.validate_live()?;
                Ok(agent)
            });
        };
        let mut guard = ActivationGuard {
            manager: self.clone(),
            incarnation: incarnation.clone(),
            attempt: attempt.clone(),
            committed: false,
        };
        let result = self
            .activate_reserved(
                &incarnation,
                input.ok_or(ManagerError::UnknownAgent(agent_id))?,
                factory,
            )
            .await;
        if result.is_err() {
            self.rollback_activation(&incarnation);
        }
        attempt.complete(result.as_ref().map(|_| ()).map_err(Clone::clone));
        guard.committed = true;
        result
    }

    async fn activate_reserved<F, Fut>(
        &self,
        incarnation: &AgentIncarnation,
        input: ReactivationInput,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(ReactivationInput) -> Fut,
        Fut: Future<Output = Result<ResidentAgent, ReactivationError>>,
    {
        let record = input.record.clone();
        let history = input.history.clone();
        let future = catch_unwind(AssertUnwindSafe(|| factory(input)))
            .map_err(|_| ReactivationError::Panicked)?;
        let mut future = Box::pin(future);
        let resident = poll_fn(|context| {
            match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
                Ok(Poll::Ready(result)) => Poll::Ready(result),
                Ok(Poll::Pending) => Poll::Pending,
                Err(_) => Poll::Ready(Err(ReactivationError::Panicked)),
            }
        })
        .await?;
        Self::validate_restored_ceiling(&record, &resident.config)?;
        let mut admission =
            ManagedTurnAdmission::new(Arc::downgrade(&self.0), incarnation.agent_id);
        admission.reservation = incarnation.reservation;
        admission.work = Some({
            let graph = self.lock_graph();
            let node = graph
                .nodes
                .get(&incarnation.agent_id)
                .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewind_affected.contains(&incarnation.agent_id) {
                return Err(ManagerError::RewindInProgress);
            }
            if node.reservation != incarnation.reservation
                || node.detach_pending
                || node.lifecycle != GraphLifecycle::Dormant
                || !node.reservation_pending
            {
                return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
            }
            self.0
                .work
                .bind_incarnation(incarnation.agent_id, incarnation.reservation)?
        });
        admission.is_child = record.parent.is_some();
        admission.parent_delivery =
            record
                .parent
                .map(|parent_id| crate::actor::delivery::ParentDelivery {
                    parent_id,
                    metadata: AgentMetadata {
                        label: record.label.clone(),
                        spawned_by_tool_use_id: record.spawned_by_tool_use_id.clone(),
                        transcript_key: Some(record.transcript_key.clone()),
                    },
                });
        let (start, ready) = flume::bounded(1);
        let actor_start = if let Some(external_start) = resident.start {
            let (combined, receiver) = flume::bounded(1);
            smol::spawn(async move {
                if ready.recv_async().await.is_ok() && external_start.recv_async().await.is_ok() {
                    let _ = combined.try_send(());
                }
            })
            .detach();
            receiver
        } else {
            ready
        };
        let snapshot = self
            .0
            .journal
            .checkpoint_snapshot()
            .map_err(ManagerError::InvalidGraph)?;
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
        let scope = view.scopes.get(&Some(incarnation.agent_id));
        let results = scope
            .into_iter()
            .flat_map(|scope| &scope.results)
            .filter_map(|(_, result)| {
                crate::actor::TurnResult::from_stored(result.clone(), |reference| {
                    self.resolve_result_payload(reference)
                })
                .ok()
            })
            .collect();
        let mut deliveries = Vec::new();
        let mut delivery_seen = Vec::new();
        if let Some(scope) = scope {
            for ((source, turn), op) in &scope.deliveries {
                match op {
                    StoredDeliveryOp::Queued(delivery) => {
                        let result = crate::actor::TurnResult::from_stored(
                            delivery.result.clone(),
                            |reference| self.resolve_result_payload(reference),
                        )
                        .map_err(|_| {
                            ManagerError::TurnResultUnavailable {
                                agent_id: *source,
                                turn_id: *turn,
                            }
                        })?;
                        deliveries.push(Arc::new(ChildDelivery {
                            label: delivery.label.clone(),
                            result,
                        }));
                    }
                    _ => delivery_seen.push(*turn),
                }
            }
        }
        let restoration = ActorRestoration {
            history,
            results,
            deliveries,
            delivery_seen,
            usage: Self::restored_usage(&snapshot, &record),
            generation: scope.map_or(0, |scope| scope.generation),
        };
        let (actor, task) = AgentActorHandle::spawn_managed_restored(
            incarnation.agent_id,
            restoration,
            resident.shared_messages,
            resident.backend,
            admission,
            resident.config,
            actor_start,
        );
        let task = self.track_runner(
            incarnation.agent_id,
            incarnation.reservation,
            actor.clone(),
            task,
        );
        let agent = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                drop(graph);
                actor.shutdown();
                task.detach();
                return Err(ManagerError::GraphShutdown);
            }
            let node = graph
                .nodes
                .get_mut(&incarnation.agent_id)
                .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
            if node.reservation != incarnation.reservation
                || node.detach_pending
                || node.lifecycle != GraphLifecycle::Dormant
                || node.residency != AgentResidency::Activating
            {
                drop(graph);
                actor.shutdown();
                task.detach();
                return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
            }
            node.actor = Some(actor.clone());
            node.task = Some(task);
            AgentRef {
                manager: self.clone(),
                agent_id: incarnation.agent_id,
                terminal: node.terminal.clone(),
            }
        };
        if let Some(initialize) = resident.initialize {
            let future = catch_unwind(AssertUnwindSafe(|| {
                initialize(ResidentInitialization {
                    agent: agent.clone(),
                    actor: actor.clone(),
                    incarnation: incarnation.clone(),
                })
            }))
            .map_err(|_| ReactivationError::Panicked)?;
            let mut future = future;
            poll_fn(|context| {
                match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
                    Ok(Poll::Ready(result)) => Poll::Ready(result),
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(_) => Poll::Ready(Err(ReactivationError::Panicked)),
                }
            })
            .await?;
        }
        {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewind_affected.contains(&incarnation.agent_id) {
                return Err(ManagerError::RewindInProgress);
            }
            let node = graph
                .nodes
                .get_mut(&incarnation.agent_id)
                .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
            if node.reservation != incarnation.reservation
                || node.detach_pending
                || node.lifecycle != GraphLifecycle::Dormant
                || node.residency != AgentResidency::Activating
            {
                return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
            }
            if actor.lifecycle() != ActorLifecycle::Open {
                return Err(ReactivationError::Initialization(
                    "initializer closed the actor".into(),
                )
                .into());
            }
            actor
                .reconcile_pending_deliveries(|source, turn| {
                    self.take_delivery_work(incarnation.agent_id, source, turn)
                })
                .map_err(|error| ReactivationError::Initialization(error.to_string()))?;
            self.0.work.set_open_for_incarnation(
                incarnation.agent_id,
                incarnation.reservation,
                true,
            )?;
            node.lifecycle = GraphLifecycle::Live;
            node.residency = AgentResidency::Resident;
            node.reservation_pending = false;
            node.activation = None;
            self.0
                .restored_delivery_work
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|(parent, _, _), _| *parent != incarnation.agent_id);
            graph.revision += 1;
            for (_, observer) in &graph.observers {
                actor.attach_graph_observer(observer.clone());
            }
        }
        let _ = start.try_send(());
        Ok(agent)
    }

    fn restored_usage(snapshot: &SessionJournalSnapshot, record: &StoredAgentRecord) -> TokenUsage {
        let mut seen = HashSet::new();
        let mut usage = None;
        for state in snapshot.state.iter().rev() {
            if state.agent == Some(record.id)
                && state.seq <= snapshot.seq
                && let StateChange::Turn { turn, result } = &state.change
                && seen.insert(*turn)
            {
                let stored = &result.outcome.usage;
                *usage.get_or_insert_with(TokenUsage::default) += TokenUsage {
                    input: stored.input,
                    output: stored.output,
                    cache_creation: stored.cache_creation,
                    cache_read: stored.cache_read,
                };
            }
        }
        usage.unwrap_or(TokenUsage {
            input: record.usage.input,
            output: record.usage.output,
            cache_creation: record.usage.cache_creation,
            cache_read: record.usage.cache_read,
        })
    }

    fn validate_restored_ceiling(
        record: &StoredAgentRecord,
        config: &crate::actor::EffectiveAgentConfig,
    ) -> Result<(), ManagerError> {
        let Some(spec) = record.spec.as_ref() else {
            return Ok(());
        };
        let mode = config.mode_def.as_ref().ok_or_else(|| {
            ReactivationError::ModeCeiling("resolved mode definition is missing".into())
        })?;
        if spec.mode_ceiling.restrict_write_to.is_some()
            && spec.mode_ceiling.restrict_write_to != mode.restrict_write_to
        {
            return Err(ReactivationError::ModeCeiling(
                "saved write restriction would be widened".into(),
            )
            .into());
        }
        if let Some(allowed) = &spec.mode_ceiling.tools
            && !mode
                .tools
                .as_ref()
                .is_some_and(|tools| tools.iter().all(|tool| allowed.contains(tool)))
        {
            return Err(ReactivationError::ModeCeiling(
                "saved tool ceiling would be widened".into(),
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn retain_tool_completion(
        &self,
        agent: AgentId,
        completion: impl Future<Output = ()> + Send + 'static,
    ) -> Result<(), ManagerError> {
        let graph = self.lock_graph();
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        let node = graph
            .nodes
            .get(&agent)
            .ok_or(ManagerError::UnknownAgent(agent))?;
        if node.detach_pending || node.lifecycle != GraphLifecycle::Live {
            return Err(ManagerError::NonLiveAgent(agent));
        }
        let mut tasks = self
            .0
            .tool_completions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        tasks.retain(|(_, task)| !task.is_finished());
        tasks.push((agent, smol::spawn(completion)));
        self.0.reaped.notify(usize::MAX);
        Ok(())
    }

    fn tool_completions_finished(&self, agent: Option<AgentId>) -> bool {
        let mut tasks = self
            .0
            .tool_completions
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        tasks.retain(|(_, task)| !task.is_finished());
        !tasks
            .iter()
            .any(|(id, _)| agent.is_none_or(|agent| agent == *id))
    }

    async fn drain_tool_completions(&self, agent: AgentId) {
        while !self.tool_completions_finished(Some(agent)) {
            smol::Timer::after(SHUTDOWN_POLL_INTERVAL).await;
        }
    }

    fn retain_cleanup_task(&self, task: RunnerTask) {
        let mut tasks = self
            .0
            .cleanup_tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        self.0.reaped.notify(usize::MAX);
    }

    fn cleanup_finished(&self) -> bool {
        let mut tasks = self
            .0
            .cleanup_tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        tasks.retain(|task| !task.is_finished());
        tasks.is_empty() && self.tool_completions_finished(None)
    }

    fn rollback_activation(&self, incarnation: &AgentIncarnation) {
        let Some(_publication) = self.0.journal.try_publication_read() else {
            let _ = self.defer_incarnation_cleanup(incarnation, true);
            return;
        };
        self.rollback_activation_held(incarnation);
    }

    fn rollback_activation_held(&self, incarnation: &AgentIncarnation) {
        let resources = {
            let mut graph = self.lock_graph();
            if graph.rewind_affected.contains(&incarnation.agent_id) {
                return;
            }
            let replacement = Self::next_reservation(&mut graph);
            let Some(node) = graph.nodes.get_mut(&incarnation.agent_id) else {
                return;
            };
            if node.reservation != incarnation.reservation || !node.reservation_pending {
                return;
            }
            node.reservation = replacement;
            node.detach_pending = false;
            self.0
                .journal
                .set_incarnation(incarnation.agent_id, replacement);
            let _ = self
                .0
                .work
                .bind_incarnation(incarnation.agent_id, replacement);
            let _ = self.0.work.set_open(incarnation.agent_id, false);
            node.reservation_pending = false;
            node.activation = None;
            node.residency = AgentResidency::Dormant;
            if node.lifecycle == GraphLifecycle::Closing
                && node.actor.is_none()
                && node.task.is_none()
            {
                node.lifecycle = GraphLifecycle::Closed;
            }
            let resources = (node.actor.take(), node.task.take());
            graph.revision += 1;
            resources
        };
        if let Some(actor) = resources.0 {
            actor.shutdown();
        }
        if let Some(task) = resources.1 {
            self.retain_cleanup_task(task);
        }
        self.0.reaped.notify(usize::MAX);
    }

    pub fn validate_incarnation(&self, incarnation: &AgentIncarnation) -> Result<(), ManagerError> {
        if incarnation.manager_generation != self.generation() {
            return Err(ManagerError::WrongManager);
        }
        let graph = self.lock_graph();
        let node = graph
            .nodes
            .get(&incarnation.agent_id)
            .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
        if node.reservation != incarnation.reservation {
            return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
        }
        Ok(())
    }

    pub fn validate_activation(&self, incarnation: &AgentIncarnation) -> Result<(), ManagerError> {
        if incarnation.manager_generation != self.generation() {
            return Err(ManagerError::WrongManager);
        }
        let graph = self.lock_graph();
        let node = graph
            .nodes
            .get(&incarnation.agent_id)
            .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
        if node.reservation != incarnation.reservation
            || node.detach_pending
            || !matches!(
                node.residency,
                AgentResidency::Activating | AgentResidency::Resident
            )
        {
            return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
        }
        Ok(())
    }

    pub fn retire(
        &self,
        current: &CurrentManagedTurn,
        agent_id: AgentId,
    ) -> Result<(), ManagerError> {
        self.validate_manager(current)?;
        self.retire_inner(agent_id, Some(current))
    }

    pub fn retire_trusted(&self, agent: &AgentRef) -> Result<(), ManagerError> {
        if !self.same_manager(&agent.manager) {
            return Err(ManagerError::WrongManager);
        }
        self.retire_inner(agent.agent_id, None)
    }

    fn retire_inner(
        &self,
        agent_id: AgentId,
        current: Option<&CurrentManagedTurn>,
    ) -> Result<(), ManagerError> {
        let actor = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            if let Some(current) = current {
                if graph
                    .nodes
                    .get(&current.agent_id())
                    .is_some_and(|node| node.detach_pending)
                {
                    return Err(ManagerError::NonLiveAgent(current.agent_id()));
                }
                if graph
                    .active_turns
                    .get(&(current.agent_id(), current.turn_id()))
                    != Some(&current.token.nonce)
                {
                    return Err(ManagerError::InactiveTurn {
                        agent_id: current.agent_id(),
                        turn_id: current.turn_id(),
                    });
                }
                let mut cursor = agent_id;
                loop {
                    let parent = graph
                        .nodes
                        .get(&cursor)
                        .ok_or(ManagerError::UnknownAgent(cursor))?
                        .parent_id
                        .ok_or(ManagerError::NotDescendant {
                            parent_id: current.agent_id(),
                            child_id: agent_id,
                        })?;
                    if parent == current.agent_id() {
                        break;
                    }
                    cursor = parent;
                }
            }
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            if node.detach_pending {
                return Err(ReactivationError::Cancelled.into());
            }
            if node.lifecycle == GraphLifecycle::Retired {
                return Ok(());
            }
            if node.parent_id.is_none()
                || node.children.iter().any(|id| {
                    graph
                        .nodes
                        .get(id)
                        .is_some_and(|child| child.lifecycle != GraphLifecycle::Retired)
                })
            {
                return Err(ManagerError::NotLeaf(agent_id));
            }
            if node.reservation_pending
                || self.0.work.is_busy(agent_id)
                || node
                    .actor
                    .as_ref()
                    .is_some_and(AgentActorHandle::has_pending_work)
            {
                return Err(ManagerError::BusySubtree(agent_id));
            }
            let work_idle = self.0.work.freeze_if_idle(agent_id)?;
            let mut record = self
                .0
                .journal
                .agent(agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            let snapshot = self
                .0
                .journal
                .checkpoint_snapshot()
                .map_err(ManagerError::InvalidGraph)?;
            let view =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                    .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
            if view.scopes.iter().any(|(target, scope)| {
                scope.deliveries.iter().any(|((source, _), delivery)| {
                    (*source == agent_id || *target == Some(agent_id))
                        && matches!(delivery, StoredDeliveryOp::Queued(_))
                })
            }) {
                return Err(ManagerError::BusySubtree(agent_id));
            }
            record.lifecycle = StoredAgentLifecycle::Retired;
            self.0
                .journal
                .commit(JournalCommit {
                    agents: vec![record],
                    ..Default::default()
                })
                .map_err(|error| ManagerError::Policy(error.to_string()))?;
            let node = graph
                .nodes
                .get_mut(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            node.lifecycle = GraphLifecycle::Retired;
            work_idle.commit();
            let actor = node.actor.clone();
            graph.revision += 1;
            actor
        };
        if let Some(actor) = actor {
            actor.close();
        }
        Ok(())
    }

    pub fn detach_incarnation(&self, incarnation: &AgentIncarnation) -> Result<(), ManagerError> {
        if !self.tool_completions_finished(Some(incarnation.agent_id)) {
            return self.defer_incarnation_cleanup(incarnation, false);
        }
        let Some(_publication) = self.0.journal.try_publication_read() else {
            return self.defer_incarnation_cleanup(incarnation, false);
        };
        self.detach_incarnation_held(incarnation)
    }

    fn defer_incarnation_cleanup(
        &self,
        incarnation: &AgentIncarnation,
        rollback: bool,
    ) -> Result<(), ManagerError> {
        if incarnation.manager_generation != self.generation() {
            return Err(ManagerError::WrongManager);
        }
        let actor = {
            let mut graph = self.lock_graph();
            if graph.rewinding
                && !(rollback
                    && !graph.rewind_affected.contains(&incarnation.agent_id)
                    && graph
                        .nodes
                        .get(&incarnation.agent_id)
                        .is_some_and(|node| node.reservation_pending))
            {
                return Err(ManagerError::RewindInProgress);
            }
            let node = graph
                .nodes
                .get_mut(&incarnation.agent_id)
                .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
            if node.reservation != incarnation.reservation {
                return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
            }
            if rollback && !node.reservation_pending {
                return Ok(());
            }
            if node.detach_pending {
                return Ok(());
            }
            node.detach_pending = true;
            self.0.journal.revoke_state_agent(incarnation.agent_id);
            self.0.work.set_open_for_incarnation(
                incarnation.agent_id,
                incarnation.reservation,
                false,
            )?;
            let actor = node.actor.clone();
            graph.revision += 1;
            actor
        };
        if let Some(actor) = actor {
            actor.shutdown();
        }
        let manager = self.clone();
        let incarnation = incarnation.clone();
        self.retain_cleanup_task(smol::spawn(async move {
            manager.drain_tool_completions(incarnation.agent_id).await;
            let _publication = manager.0.journal.publication_read().await;
            if rollback {
                manager.rollback_activation_held(&incarnation);
            } else if let Err(error) = manager.detach_incarnation_held(&incarnation)
                && !matches!(error, ManagerError::StaleIncarnation(_))
            {
                warn!(agent_id = %incarnation.agent_id, %error, "deferred agent cleanup failed");
            }
            manager.0.reaped.notify(usize::MAX);
        }));
        Ok(())
    }

    fn detach_incarnation_held(&self, incarnation: &AgentIncarnation) -> Result<(), ManagerError> {
        self.validate_incarnation(incarnation)?;
        let resources = {
            let mut graph = self.lock_graph();
            if graph.rewinding
                && (graph.rewind_quiescent
                    || !graph
                        .nodes
                        .get(&incarnation.agent_id)
                        .is_some_and(|node| node.detach_pending))
            {
                return Err(ManagerError::RewindInProgress);
            }
            if !self.tool_completions_finished(Some(incarnation.agent_id)) {
                drop(graph);
                return self.defer_incarnation_cleanup(incarnation, false);
            }
            let replacement = Self::next_reservation(&mut graph);
            let node = graph
                .nodes
                .get_mut(&incarnation.agent_id)
                .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
            if node.reservation != incarnation.reservation {
                return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
            }
            node.reservation = replacement;
            node.detach_pending = false;
            self.0
                .journal
                .set_incarnation(incarnation.agent_id, replacement);
            self.0
                .work
                .bind_incarnation(incarnation.agent_id, replacement)?;
            self.0.work.set_open(incarnation.agent_id, false)?;
            node.reservation_pending = false;
            node.residency = AgentResidency::Dormant;
            if node.lifecycle == GraphLifecycle::Closing {
                node.lifecycle = GraphLifecycle::Closed;
            } else if !matches!(
                node.lifecycle,
                GraphLifecycle::Closed | GraphLifecycle::Retired
            ) {
                node.lifecycle = GraphLifecycle::Dormant;
            }
            if let Some(attempt) = node.activation.take() {
                attempt.complete(Err(ReactivationError::Cancelled.into()));
            }
            let resources = (node.actor.take(), node.task.take());
            graph
                .active_turns
                .retain(|(id, _), _| *id != incarnation.agent_id);
            self.0.journal.revoke_state_agent(incarnation.agent_id);
            graph.revision += 1;
            resources
        };
        if let Some(actor) = resources.0 {
            actor.shutdown();
        }
        if let Some(task) = resources.1 {
            self.retain_cleanup_task(task);
        }
        self.0.reaped.notify(usize::MAX);
        Ok(())
    }

    pub fn incarnation(&self, agent_id: AgentId) -> Result<AgentIncarnation, ManagerError> {
        let graph = self.lock_graph();
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        Ok(AgentIncarnation {
            agent_id,
            manager_generation: self.generation(),
            reservation: node.reservation,
        })
    }

    pub fn is_live(&self, agent_id: AgentId) -> bool {
        let graph = self.lock_graph();
        graph.nodes.get(&agent_id).is_some_and(|node| {
            node.lifecycle == GraphLifecycle::Live
                && node.residency == AgentResidency::Resident
                && !node.detach_pending
        })
    }

    pub(crate) fn take_delivery_work(
        &self,
        parent: AgentId,
        source: AgentId,
        turn: TurnId,
    ) -> Vec<WorkReservation> {
        self.0
            .restored_delivery_work
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&(parent, source, turn))
            .cloned()
            .unwrap_or_default()
    }

    pub fn active_agent_count(&self) -> usize {
        self.0.work.active_agents()
    }

    pub fn active_child_count(&self, parent_id: AgentId) -> usize {
        self.0.work.active_children(parent_id)
    }

    pub fn journal(&self) -> Arc<SessionJournal> {
        Arc::clone(&self.0.journal)
    }

    pub(crate) fn from_inner(inner: Arc<ManagerInner>) -> Self {
        Self(inner)
    }

    #[cfg(test)]
    pub(crate) fn deliver_reserved(
        &self,
        parent_id: AgentId,
        delivery: Arc<ChildDelivery>,
        work: Vec<WorkReservation>,
    ) -> Result<(), DeliveryRejection> {
        self.deliver_reserved_for(parent_id, delivery, work, None)
    }

    pub(crate) fn deliver_reserved_for(
        &self,
        parent_id: AgentId,
        delivery: Arc<ChildDelivery>,
        work: Vec<WorkReservation>,
        source_incarnation: Option<JournalIncarnation>,
    ) -> Result<(), DeliveryRejection> {
        let graph = self.lock_graph();
        if graph.rewind_quiescent
            || graph.rewind_affected.contains(&parent_id)
            || graph
                .rewind_affected
                .contains(&delivery.result.outcome.agent_id())
        {
            return Err(DeliveryRejection::ParentUnavailable);
        }
        if let Some(source) = &source_incarnation
            && graph
                .nodes
                .get(&source.agent)
                .is_some_and(|node| node.reservation != source.reservation || node.detach_pending)
        {
            return Err(DeliveryRejection::ParentUnavailable);
        }
        let target = if let Some(node) = graph.nodes.get(&parent_id) {
            if !graph.shutting_down
                && matches!(
                    node.lifecycle,
                    GraphLifecycle::Closing
                        | GraphLifecycle::Closed
                        | GraphLifecycle::Retired
                        | GraphLifecycle::Removed
                )
            {
                return Err(DeliveryRejection::ParentClosed);
            }
            (node.lifecycle == GraphLifecycle::Live
                && node.residency == AgentResidency::Resident
                && !node.detach_pending)
                .then(|| node.actor.clone())
                .flatten()
        } else {
            match self.0.journal.agent(parent_id) {
                Some(record) if record.lifecycle == StoredAgentLifecycle::Open => None,
                Some(_) => return Err(DeliveryRejection::ParentClosed),
                None => return Err(DeliveryRejection::ParentUnavailable),
            }
        };
        match target {
            Some(parent) => {
                let result = crate::actor::delivery::admit_delivery_bound(
                    &parent.inner,
                    delivery,
                    work,
                    source_incarnation,
                );
                drop(graph);
                result
            }
            None => {
                let preview = delivery.preview(None);
                let bytes = preview
                    .input()
                    .preamble
                    .remove(0)
                    .first_text_content()
                    .map_or(0, str::len);
                let source_agent = delivery.result.outcome.agent_id();
                let turn = delivery.result.outcome.turn_id();
                if self.0.journal.queue_delivery_for(
                    parent_id,
                    delivery,
                    bytes,
                    &source_incarnation.into_iter().collect::<Vec<_>>(),
                )? {
                    self.retain_delivery_work(parent_id, source_agent, turn, work);
                }
                Ok(())
            }
        }
    }

    pub(crate) fn retain_delivery_work(
        &self,
        parent: AgentId,
        source: AgentId,
        turn: TurnId,
        work: Vec<WorkReservation>,
    ) {
        if !work.is_empty() {
            self.0
                .restored_delivery_work
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .entry((parent, source, turn))
                .or_default()
                .extend(work);
        }
    }

    #[cfg(test)]
    pub(crate) fn deliver(
        &self,
        parent_id: AgentId,
        delivery: Arc<ChildDelivery>,
    ) -> Result<(), DeliveryRejection> {
        self.deliver_reserved(parent_id, delivery, Vec::new())
    }

    #[cfg(test)]
    pub(crate) fn register_delivery_actor_for_test(&self, actor: &AgentActorHandle) {
        let id = actor.agent_id();
        let node = Node {
            parent_id: None,
            root_id: id,
            depth: 0,
            children: Vec::new(),
            lifecycle: GraphLifecycle::Live,
            residency: AgentResidency::Resident,
            activation: None,
            actor: Some(actor.clone()),
            task: None,
            metadata: AgentMetadata::default(),
            reservation: 0,
            reservation_pending: false,
            detach_pending: false,
            cancel_on_commit: false,
            turn_owner: None,
            terminal: Arc::new(Mutex::new(None)),
        };
        self.0.work.register(id, None, 0, true);
        self.journal_node(id, &node, actor).unwrap();
        self.lock_graph().nodes.insert(id, node);
    }

    #[cfg(test)]
    pub(crate) fn delivery_admission_for_test(
        &self,
        agent_id: AgentId,
        parent_id: AgentId,
    ) -> ManagedTurnAdmission {
        let mut admission = ManagedTurnAdmission::new(Arc::downgrade(&self.0), agent_id);
        admission.parent_delivery = Some(crate::actor::delivery::ParentDelivery {
            parent_id,
            metadata: AgentMetadata::default(),
        });
        admission
    }

    pub fn transcript_key_in_use(&self, transcript_key: &str) -> bool {
        self.transcript_key_in_use_locked(&self.lock_graph(), transcript_key, None)
    }

    fn transcript_key_in_use_locked(
        &self,
        graph: &GraphState,
        transcript_key: &str,
        except: Option<AgentId>,
    ) -> bool {
        graph.nodes.iter().any(|(id, node)| {
            Some(*id) != except && node.metadata.transcript_key.as_deref() == Some(transcript_key)
        }) || self.0.journal.transcript_key_in_use(transcript_key, except)
    }

    pub fn same_manager(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn validate_live_ref(&self, agent: &AgentRef) -> Result<(), ManagerError> {
        if !self.same_manager(&agent.manager) {
            return Err(ManagerError::WrongManager);
        }
        let graph = self.lock_graph();
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        let Some(node) = graph.nodes.get(&agent.agent_id) else {
            return Err(
                if agent
                    .terminal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_some()
                {
                    ManagerError::NonLiveAgent(agent.agent_id)
                } else {
                    ManagerError::UnknownAgent(agent.agent_id)
                },
            );
        };
        if node.lifecycle != GraphLifecycle::Live
            || node.detach_pending
            || node
                .actor
                .as_ref()
                .is_none_or(|actor| actor.lifecycle() != ActorLifecycle::Open)
        {
            return Err(ManagerError::NonLiveAgent(agent.agent_id));
        }
        Ok(())
    }

    /// Resolves a retained ticket without granting live mutation authority.
    pub fn transcript(
        &self,
        agent_id: AgentId,
        request: TranscriptRequest,
    ) -> Result<TranscriptSnapshot, ActorError> {
        let actor = {
            let graph = self.lock_graph();
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or_else(|| ActorError::InvalidConfig("agent is unknown".into()))?;
            if node.residency == AgentResidency::Resident {
                node.actor.clone()
            } else {
                None
            }
        };
        if let Some(actor) = actor {
            return actor.transcript(request);
        }
        let snapshot = self
            .0
            .journal
            .checkpoint_snapshot()
            .map_err(ActorError::InvalidConfig)?;
        let history = snapshot
            .histories
            .get(&agent_id)
            .ok_or_else(|| ActorError::InvalidConfig("agent transcript is unavailable".into()))?;
        if let Some(turn) = request.through_turn {
            let view =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                    .map_err(|error| ActorError::InvalidConfig(error.to_string()))?;
            return Err(
                if view
                    .scopes
                    .get(&Some(agent_id))
                    .is_some_and(|scope| scope.results.iter().any(|(id, _)| *id == turn))
                {
                    ActorError::UnavailableTurnHistory(turn)
                } else {
                    ActorError::UnknownTurn(turn)
                },
            );
        }
        crate::actor::transcript_snapshot(history, &request)
    }

    pub fn bind_result_store(&self, store: Arc<OffloadStore>) -> Result<(), ManagerError> {
        if self.0.journal.session_id().is_none() {
            return Err(ManagerError::InvalidGraph(
                "result store requires a bound session".into(),
            ));
        }
        let mut current = self
            .0
            .result_store
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if current
            .as_ref()
            .is_some_and(|bound| !Arc::ptr_eq(bound, &store))
        {
            return Err(ManagerError::InvalidGraph(
                "result store is already bound".into(),
            ));
        }
        *current = Some(store);
        Ok(())
    }

    fn resolve_result_payload(&self, reference: &StoredOffloadRef) -> io::Result<Vec<u8>> {
        if Some(reference.session) != self.0.journal.session_id() {
            return Err(IoError::new(
                ErrorKind::PermissionDenied,
                "stored result belongs to another session",
            ));
        }
        let store = self
            .0
            .result_store
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or_else(|| {
                IoError::new(
                    ErrorKind::NotFound,
                    "owning session result store is not bound",
                )
            })?;
        let name = reference.path.to_str().ok_or_else(|| {
            IoError::new(
                ErrorKind::InvalidInput,
                "stored result artifact name is not UTF-8",
            )
        })?;
        store.read(name, reference.bytes)
    }

    pub fn retained_turn_ticket_with(
        &self,
        agent: &AgentRef,
        turn_id: TurnId,
        resolve: impl FnMut(&StoredOffloadRef) -> io::Result<Vec<u8>>,
    ) -> Result<crate::TurnTicket, ManagerError> {
        self.retained_turn_ticket_inner(agent, turn_id, resolve)
    }

    pub fn retained_turn_ticket(
        &self,
        agent: &AgentRef,
        turn_id: TurnId,
    ) -> Result<crate::TurnTicket, ManagerError> {
        self.retained_turn_ticket_inner(agent, turn_id, |reference| {
            self.resolve_result_payload(reference)
        })
    }

    fn retained_turn_ticket_inner(
        &self,
        agent: &AgentRef,
        turn_id: TurnId,
        resolve: impl FnMut(&StoredOffloadRef) -> io::Result<Vec<u8>>,
    ) -> Result<crate::TurnTicket, ManagerError> {
        if !self.same_manager(&agent.manager) {
            return Err(ManagerError::WrongManager);
        }
        let actor = {
            let graph = self.lock_graph();
            let node = graph
                .nodes
                .get(&agent.agent_id)
                .ok_or(ManagerError::UnknownAgent(agent.agent_id))?;
            node.actor.clone()
        };
        if let Some(ticket) = actor.and_then(|actor| actor.turn_ticket(turn_id)) {
            return Ok(ticket);
        }
        let snapshot = self
            .0
            .journal
            .checkpoint_snapshot()
            .map_err(ManagerError::InvalidGraph)?;
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
        let stored = view
            .scopes
            .get(&Some(agent.agent_id))
            .and_then(|scope| scope.results.iter().find(|(turn, _)| *turn == turn_id))
            .map(|(_, result)| result.clone())
            .ok_or(ManagerError::TurnResultUnavailable {
                agent_id: agent.agent_id,
                turn_id,
            })?;
        let owning_session = self.0.journal.session_id();
        let mut resolve = resolve;
        let result = crate::actor::TurnResult::from_stored(stored, |reference| {
            if Some(reference.session) != owning_session {
                return Err(IoError::new(
                    ErrorKind::PermissionDenied,
                    "stored result belongs to another session",
                ));
            }
            resolve(reference)
        })
        .map_err(|_| ManagerError::TurnResultUnavailable {
            agent_id: agent.agent_id,
            turn_id,
        })?;
        Ok(crate::TurnTicket::from_result(result))
    }

    /// A reference to {agent_id} that grants nothing until validated, for
    /// naming an agent that may already have left the graph.
    pub fn reference(&self, agent_id: AgentId) -> AgentRef {
        let terminal = self
            .lock_graph()
            .nodes
            .get(&agent_id)
            .map(|node| node.terminal.clone())
            .unwrap_or_default();
        AgentRef {
            manager: self.clone(),
            agent_id,
            terminal,
        }
    }

    pub fn lookup(&self, agent_id: AgentId) -> Result<AgentRef, ManagerError> {
        let terminal = self
            .lock_graph()
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?
            .terminal
            .clone();
        let agent = AgentRef {
            manager: self.clone(),
            agent_id,
            terminal,
        };
        Ok(agent)
    }

    pub fn root(&self) -> Result<AgentRef, ManagerError> {
        self.lookup(self.root_id()?)
    }

    /// Every event of every actor in this graph, actors committed later
    /// included, tagged with the agent that emitted it, or `None` when
    /// {subscriber} already listens. Holds the manager weakly, so it ends
    /// once the graph is dropped.
    pub fn subscribe_events(&self, subscriber: u64) -> Option<GraphEvents> {
        let mut graph = self.lock_graph();
        graph
            .observers
            .retain(|(_, sender)| !sender.is_disconnected());
        if graph.observers.iter().any(|(key, _)| *key == subscriber) {
            return None;
        }
        let (sender, events) = flume::unbounded();
        for actor in graph
            .nodes
            .values()
            .filter(|node| node.residency == AgentResidency::Resident)
            .filter_map(|node| node.actor.as_ref())
        {
            actor.attach_graph_observer(sender.clone());
        }
        graph.observers.push((subscriber, sender));
        Some(GraphEvents {
            manager: Arc::downgrade(&self.0),
            events,
        })
    }

    pub fn list(&self) -> Vec<AgentRef> {
        let ids = self.lock_graph().nodes.keys().copied().collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| self.lookup(id).ok())
            .collect()
    }

    /// Resolves once no agent in the graph has work running or waiting,
    /// child results still on their way to a parent included. A pass that
    /// saw an agent run out of work mid-check is repeated, since that agent
    /// may have handed work to one already checked.
    pub async fn wait_settled(&self) {
        loop {
            let actors: Vec<_> = self
                .list()
                .into_iter()
                .filter_map(|agent| agent.actor().ok())
                .collect();
            let mut drained: Vec<_> = actors
                .iter()
                .map(|actor| actor.inner.drained.listen())
                .collect();
            if actors.iter().all(AgentActorHandle::is_settled) {
                let mut changed = false;
                for listener in &mut drained {
                    changed |= futures_lite::future::poll_once(listener).await.is_some();
                }
                if !changed {
                    return;
                }
                continue;
            }
            std::future::poll_fn(|context| {
                if drained
                    .iter_mut()
                    .any(|listener| Pin::new(listener).poll(context).is_ready())
                {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }

    pub fn limits(&self) -> AgentLimits {
        self.0.limits
    }

    pub fn generation(&self) -> u64 {
        self.0.generation
    }

    pub fn root_id(&self) -> Result<AgentId, ManagerError> {
        self.lock_graph().root_id.ok_or(ManagerError::MissingRoot)
    }

    pub fn create_root(
        &self,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
    ) -> Result<AgentRef, ManagerError> {
        self.create_root_with(initial_messages, shared_messages, |_| {
            Ok::<_, String>(backend)
        })
    }

    pub fn create_root_with<F, E>(
        &self,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.create_root_with_config(None, initial_messages, shared_messages, factory)
    }

    pub fn create_root_with_config<F, E>(
        &self,
        config: Option<crate::actor::EffectiveAgentConfig>,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.create_root_with_id_and_config(
            AgentId::generate(),
            config,
            initial_messages,
            shared_messages,
            factory,
        )
    }

    pub fn create_root_deferred_with_config<F, E>(
        &self,
        config: Option<crate::actor::EffectiveAgentConfig>,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
        start: flume::Receiver<()>,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.create_root_deferred_with_id_and_config(
            AgentId::generate(),
            config,
            initial_messages,
            shared_messages,
            factory,
            start,
        )
    }

    pub fn create_root_with_id(
        &self,
        agent_id: AgentId,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
    ) -> Result<AgentRef, ManagerError> {
        self.create_root_with_id_and_config(
            agent_id,
            None,
            initial_messages,
            shared_messages,
            |_| Ok::<_, String>(backend),
        )
    }

    pub fn create_root_with_id_and_config<F, E>(
        &self,
        agent_id: AgentId,
        config: Option<crate::actor::EffectiveAgentConfig>,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.create_root_inner(
            agent_id,
            config,
            initial_messages,
            shared_messages,
            factory,
            None,
        )
    }

    pub fn create_root_deferred_with_id_and_config<F, E>(
        &self,
        agent_id: AgentId,
        config: Option<crate::actor::EffectiveAgentConfig>,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
        start: flume::Receiver<()>,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.create_root_inner(
            agent_id,
            config,
            initial_messages,
            shared_messages,
            factory,
            Some(start),
        )
    }

    fn create_root_inner<F, E>(
        &self,
        agent_id: AgentId,
        config: Option<crate::actor::EffectiveAgentConfig>,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
        start: Option<flume::Receiver<()>>,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        let config = config.map(crate::actor::EffectiveAgentConfig::normalize_initial_mode);
        if let Some(config) = &config {
            crate::session_checkpoint::stored_agent_config(config, Default::default())
                .map_err(|error| ManagerError::Policy(error.to_string()))?;
        }
        let reservation = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            if graph.root_id.is_some() {
                return Err(ManagerError::DuplicateRoot);
            }
            let snapshot = self.0.journal.snapshot();
            if snapshot.agent_revisions.iter().any(|(_, record)| {
                (record.parent.is_none() && record.id != agent_id)
                    || (record.id == agent_id && record.parent.is_some())
            }) {
                return Err(ManagerError::Policy(
                    "root identity conflicts with persisted graph".into(),
                ));
            }
            let reservation = Self::next_reservation(&mut graph);
            self.0.journal.set_incarnation(agent_id, reservation);
            self.0.work.register(agent_id, None, reservation, false);
            graph.root_id = Some(agent_id);
            graph.nodes.insert(
                agent_id,
                Node {
                    parent_id: None,
                    root_id: agent_id,
                    depth: 0,
                    children: Vec::new(),
                    lifecycle: GraphLifecycle::Reserved,
                    residency: AgentResidency::Activating,
                    activation: None,
                    actor: None,
                    task: None,
                    metadata: AgentMetadata::default(),
                    reservation,
                    reservation_pending: true,
                    detach_pending: false,
                    cancel_on_commit: false,
                    turn_owner: None,
                    terminal: Arc::new(Mutex::new(None)),
                },
            );
            graph.revision += 1;
            reservation
        };
        let backend = match catch_unwind(AssertUnwindSafe(|| factory(agent_id))) {
            Ok(Ok(backend)) => backend,
            Ok(Err(error)) => {
                self.rollback_reservation(agent_id, reservation);
                return Err(ManagerError::Factory(error.to_string()));
            }
            Err(_) => {
                self.rollback_reservation(agent_id, reservation);
                return Err(ManagerError::Factory("agent factory panicked".into()));
            }
        };
        self.commit_actor(agent_id, reservation, |admission| {
            AgentActorHandle::spawn_managed(
                agent_id,
                initial_messages,
                shared_messages,
                backend,
                admission,
                config,
                start,
            )
        })
    }

    pub fn spawn_child(
        &self,
        current: &CurrentManagedTurn,
        metadata: AgentMetadata,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
    ) -> Result<AgentRef, ManagerError> {
        self.spawn_child_with(current, metadata, initial_messages, shared_messages, |_| {
            Ok::<_, String>(backend)
        })
    }

    pub fn spawn_child_with<F, E>(
        &self,
        current: &CurrentManagedTurn,
        metadata: AgentMetadata,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.validate_active(current)?;
        let parent_mode = current.mode.clone().ok_or_else(|| {
            ManagerError::Policy("child delegation requires a parent mode snapshot".into())
        })?;
        let inherited_config = current.policy_snapshot().ok_or_else(|| {
            ManagerError::Policy("child delegation requires a parent policy snapshot".into())
        })?;
        let inherited_config = crate::actor::EffectiveAgentConfig::new(
            (**inherited_config).clone(),
            parent_mode.clone(),
        )
        .with_mode_def(current.mode_def.clone())
        .with_context(current.context.clone().ok_or_else(|| {
            ManagerError::Policy("child delegation requires a parent context snapshot".into())
        })?);
        self.spawn_child_with_config(
            current,
            inherited_config,
            metadata,
            initial_messages,
            shared_messages,
            factory,
        )
    }

    pub fn spawn_child_with_config<F, E>(
        &self,
        current: &CurrentManagedTurn,
        config: crate::actor::EffectiveAgentConfig,
        metadata: AgentMetadata,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.validate_active(current)?;
        self.spawn_child_inner(
            current.agent_id(),
            Some(current),
            config,
            metadata,
            initial_messages,
            shared_messages,
            factory,
        )
    }

    /// Host-only admission. Do not expose this as model tool authority.
    pub fn spawn_child_trusted_with_config<F, E>(
        &self,
        parent: &AgentRef,
        config: crate::actor::EffectiveAgentConfig,
        metadata: AgentMetadata,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        self.validate_live_ref(parent)?;
        self.spawn_child_inner(
            parent.id(),
            None,
            config,
            metadata,
            initial_messages,
            shared_messages,
            factory,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn spawn_child_inner<F, E>(
        &self,
        parent_id: AgentId,
        current: Option<&CurrentManagedTurn>,
        config: crate::actor::EffectiveAgentConfig,
        metadata: AgentMetadata,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        factory: F,
    ) -> Result<AgentRef, ManagerError>
    where
        F: FnOnce(AgentId) -> Result<Box<dyn ActorBackend>, E>,
        E: ToString,
    {
        if matches!(config.mode, crate::AgentMode::Custom(_)) && config.mode_def.is_none() {
            return Err(ManagerError::Policy(
                "custom mode requires a resolved definition".into(),
            ));
        }
        let child_id = AgentId::generate();
        let reservation = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            Self::reclaim_finished_closings(&mut graph, &self.0.journal);
            if let Some(current) = current
                && graph.active_turns.get(&(parent_id, current.turn_id()))
                    != Some(&current.token.nonce)
            {
                return Err(ManagerError::InactiveTurn {
                    agent_id: parent_id,
                    turn_id: current.turn_id(),
                });
            }
            let (root_id, depth) = {
                let parent = graph
                    .nodes
                    .get(&parent_id)
                    .ok_or(ManagerError::UnknownAgent(parent_id))?;
                if parent.lifecycle != GraphLifecycle::Live
                    || parent.detach_pending
                    || parent
                        .actor
                        .as_ref()
                        .is_none_or(|actor| actor.lifecycle() != ActorLifecycle::Open)
                {
                    return Err(ManagerError::NonLiveAgent(parent_id));
                }
                (parent.root_id, parent.depth + 1)
            };
            if depth > self.0.limits.max_agent_depth {
                return Err(ManagerError::DepthExceeded {
                    depth,
                    max: self.0.limits.max_agent_depth,
                });
            }
            if let Some(key) = &metadata.transcript_key
                && (key.is_empty() || self.transcript_key_in_use_locked(&graph, key, None))
            {
                return Err(ManagerError::Policy(
                    "child transcript key is empty or already assigned".into(),
                ));
            }
            let retained_nodes = graph
                .nodes
                .iter()
                .filter(|(id, node)| {
                    node.lifecycle != GraphLifecycle::Retired
                        && !self
                            .0
                            .journal
                            .agent(**id)
                            .is_some_and(|record| record.legacy)
                })
                .count();
            if retained_nodes >= self.0.limits.max_retained_agents {
                return Err(ManagerError::RetentionLimit {
                    max: self.0.limits.max_retained_agents,
                });
            }
            let reservation = Self::next_reservation(&mut graph);
            self.0.journal.set_incarnation(child_id, reservation);
            self.0
                .work
                .register(child_id, Some(parent_id), reservation, false);
            graph
                .nodes
                .get_mut(&parent_id)
                .unwrap()
                .children
                .push(child_id);
            graph.nodes.insert(
                child_id,
                Node {
                    parent_id: Some(parent_id),
                    root_id,
                    depth,
                    children: Vec::new(),
                    lifecycle: GraphLifecycle::Reserved,
                    residency: AgentResidency::Activating,
                    activation: None,
                    actor: None,
                    task: None,
                    metadata,
                    reservation,
                    reservation_pending: true,
                    detach_pending: false,
                    cancel_on_commit: false,
                    turn_owner: current.map(|current| (parent_id, current.turn_id())),
                    terminal: Arc::new(Mutex::new(None)),
                },
            );
            graph.revision += 1;
            reservation
        };
        let backend = match catch_unwind(AssertUnwindSafe(|| factory(child_id))) {
            Ok(Ok(backend)) => backend,
            Ok(Err(error)) => {
                self.rollback_reservation(child_id, reservation);
                return Err(ManagerError::Factory(error.to_string()));
            }
            Err(_) => {
                self.rollback_reservation(child_id, reservation);
                return Err(ManagerError::Factory("agent factory panicked".into()));
            }
        };
        self.commit_actor(child_id, reservation, |admission| {
            AgentActorHandle::spawn_managed(
                child_id,
                initial_messages,
                shared_messages,
                backend,
                admission,
                Some(config),
                None,
            )
        })
    }

    fn validate_manager(&self, current: &CurrentManagedTurn) -> Result<(), ManagerError> {
        if Arc::ptr_eq(&self.0, &current.token.manager.0) {
            Ok(())
        } else {
            Err(ManagerError::WrongManager)
        }
    }

    fn validate_active(&self, current: &CurrentManagedTurn) -> Result<(), ManagerError> {
        self.validate_manager(current)?;
        let graph = self.lock_graph();
        if graph
            .nodes
            .get(&current.agent_id())
            .is_some_and(|node| node.detach_pending)
        {
            return Err(ManagerError::NonLiveAgent(current.agent_id()));
        }
        if graph
            .active_turns
            .get(&(current.agent_id(), current.turn_id()))
            != Some(&current.token.nonce)
        {
            return Err(ManagerError::InactiveTurn {
                agent_id: current.agent_id(),
                turn_id: current.turn_id(),
            });
        }
        let node = graph
            .nodes
            .get(&current.agent_id())
            .ok_or(ManagerError::UnknownAgent(current.agent_id()))?;
        if node.lifecycle != GraphLifecycle::Live || node.detach_pending {
            return Err(ManagerError::NonLiveAgent(current.agent_id()));
        }
        Ok(())
    }

    fn validate_descendant(
        &self,
        current: &CurrentManagedTurn,
        child_id: AgentId,
    ) -> Result<(), ManagerError> {
        self.validate_manager(current)?;
        let graph = self.lock_graph();
        if graph
            .nodes
            .get(&current.agent_id())
            .is_some_and(|node| node.detach_pending)
        {
            return Err(ManagerError::NonLiveAgent(current.agent_id()));
        }
        if graph
            .active_turns
            .get(&(current.agent_id(), current.turn_id()))
            != Some(&current.token.nonce)
        {
            return Err(ManagerError::InactiveTurn {
                agent_id: current.agent_id(),
                turn_id: current.turn_id(),
            });
        }
        let mut cursor = child_id;
        loop {
            let node = graph
                .nodes
                .get(&cursor)
                .ok_or(ManagerError::UnknownAgent(cursor))?;
            let Some(parent_id) = node.parent_id else {
                return Err(ManagerError::NotDescendant {
                    parent_id: current.agent_id(),
                    child_id,
                });
            };
            if parent_id == current.agent_id() {
                return Ok(());
            }
            cursor = parent_id;
        }
    }

    fn validated_descendant_actor(
        &self,
        current: &CurrentManagedTurn,
        child_id: AgentId,
    ) -> Result<AgentActorHandle, ManagerError> {
        self.validate_descendant(current, child_id)?;
        self.actor(child_id)
    }

    fn register_turn_wait(
        &self,
        current: &CurrentManagedTurn,
        lease: &TurnPermitLease,
        child_id: AgentId,
        actor: &AgentActorHandle,
        ticket: crate::TurnTicket,
        timeout: Option<Duration>,
    ) -> Result<ManagedPromptWait, ManagerError> {
        if !Arc::ptr_eq(&lease.inner, &current.lease.inner) {
            return Err(ManagerError::WrongManager);
        }
        let managed_actor = self.validated_descendant_actor(current, child_id)?;
        if !managed_actor.same_actor(actor) {
            return Err(ManagerError::ActorMismatch {
                expected_id: child_id,
                actual_id: actor.agent_id(),
            });
        }
        if !managed_actor.owns_ticket(&ticket) {
            return Err(ManagerError::TicketActorMismatch {
                agent_id: child_id,
                turn_id: ticket.turn_id(),
            });
        }
        #[cfg(test)]
        self.wait_at_prompt_wait_registration_gate();

        let watcher_id = self.0.next_watcher.fetch_add(1, Ordering::Relaxed);
        let (cancel, cancel_token) = crate::ReasonedCancelToken::new();
        lease.inner.suspend(watcher_id, cancel.clone())?;
        let wait = Arc::new(PromptWaitInner {
            lease: Arc::clone(&lease.inner),
            manager: Arc::downgrade(&self.0),
            watcher_id,
            result: Mutex::new(None),
            completed: Event::new(),
            cancel: Mutex::new(Some(cancel)),
        });
        let watcher_wait = Arc::clone(&wait);
        let suspension = PermitSuspensionGuard {
            lease: Arc::clone(&lease.inner),
            watcher_id,
        };
        let result_ticket = ticket.clone();
        let task = smol::spawn(async move {
            let outcome = match timeout {
                Some(duration) => {
                    let timed =
                        futures_lite::future::race(async { Ok(ticket.wait().await) }, async {
                            smol::Timer::after(duration).await;
                            Err(PromptWaitError::Timeout)
                        });
                    cancel_token
                        .race(timed)
                        .await
                        .unwrap_or(Err(PromptWaitError::Cancelled))
                }
                None => cancel_token
                    .race(ticket.wait())
                    .await
                    .map_err(|_| PromptWaitError::Cancelled),
            };
            drop(suspension);
            watcher_wait.complete(outcome);
            watcher_wait.mark_reapable();
        });
        let task = {
            let mut watchers = self
                .0
                .watchers
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if watchers.reapable.remove(&watcher_id) {
                Some(task)
            } else {
                watchers.tasks.insert(watcher_id, task);
                None
            }
        };
        if let Some(task) = task {
            smol::spawn(async move {
                task.await;
            })
            .detach();
        }
        Ok(ManagedPromptWait {
            inner: wait,
            ticket: result_ticket,
        })
    }

    fn rollback_reservation(&self, agent_id: AgentId, reservation: u64) {
        let mut graph = self.lock_graph();
        if graph.rewind_affected.contains(&agent_id) {
            return;
        }
        let Some(node) = graph.nodes.get(&agent_id) else {
            return;
        };
        if node.reservation != reservation || node.actor.is_some() || !node.reservation_pending {
            return;
        }
        let parent_id = node.parent_id;
        graph.nodes.remove(&agent_id);
        let _ = self.0.work.remove(agent_id);
        if let Some(parent_id) = parent_id
            && let Some(parent) = graph.nodes.get_mut(&parent_id)
        {
            parent.children.retain(|id| *id != agent_id);
        }
        if graph.root_id == Some(agent_id) {
            graph.root_id = None;
        }
        graph.revision += 1;
        warn!(manager_generation = self.0.generation, %agent_id, revision = graph.revision, "agent reservation rolled back");
    }

    fn track_runner(
        &self,
        agent_id: AgentId,
        reservation: u64,
        actor: AgentActorHandle,
        task: RunnerTask,
    ) -> RunnerTask {
        let manager = Arc::downgrade(&self.0);
        smol::spawn(async move {
            task.await;
            let Some(manager) = manager.upgrade() else {
                return;
            };
            loop {
                let released = manager.reaped.listen();
                {
                    let mut graph = manager
                        .graph
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if !graph.rewind_quiescent || !graph.rewind_affected.is_empty() {
                        let closed = actor.lifecycle() == ActorLifecycle::Closed;
                        if graph.rewind_quiescent || graph.rewind_affected.contains(&agent_id) {
                            manager.reaped.notify(usize::MAX);
                            return;
                        }
                        if let Some(node) = graph.nodes.get_mut(&agent_id)
                            && node.reservation == reservation
                            && !node.reservation_pending
                            && !node.detach_pending
                            && node.lifecycle == GraphLifecycle::Live
                        {
                            let _ =
                                manager
                                    .work
                                    .set_open_for_incarnation(agent_id, reservation, false);
                            node.residency = AgentResidency::Dormant;
                            node.lifecycle = if closed {
                                GraphLifecycle::Closed
                            } else {
                                GraphLifecycle::Dormant
                            };
                            if closed && let Some(mut record) = manager.journal.agent(agent_id) {
                                record.lifecycle = StoredAgentLifecycle::Closed;
                                if let Err(error) = manager.journal.commit(JournalCommit {
                                    agents: vec![record],
                                    ..Default::default()
                                }) {
                                    warn!(%agent_id, %error, "failed to record agent closure");
                                }
                            }
                            graph.revision += 1;
                        }
                        if !closed
                            && let Some(node) = graph.nodes.get_mut(&agent_id)
                            && node.reservation == reservation
                            && node.lifecycle == GraphLifecycle::Closing
                            && !node.reservation_pending
                            && !node.detach_pending
                        {
                            node.lifecycle = GraphLifecycle::Dormant;
                            node.residency = AgentResidency::Dormant;
                            graph.revision += 1;
                        }
                        Self::reclaim_finished_closings(&mut graph, &manager.journal);
                        manager.reaped.notify(usize::MAX);
                        return;
                    }
                    #[cfg(test)]
                    if let Some(deferred) = manager.runner_deferred.lock().unwrap().take() {
                        let _ = deferred.try_send(());
                    }
                }
                released.await;
            }
        })
    }

    fn commit_actor(
        &self,
        agent_id: AgentId,
        reservation: u64,
        spawn: impl FnOnce(ManagedTurnAdmission) -> (AgentActorHandle, RunnerTask),
    ) -> Result<AgentRef, ManagerError> {
        let mut admission = ManagedTurnAdmission::new(Arc::downgrade(&self.0), agent_id);
        admission.reservation = reservation;
        {
            let graph = self.lock_graph();
            if graph.rewind_affected.contains(&agent_id) {
                return Err(ManagerError::RewindInProgress);
            }
            if graph
                .nodes
                .get(&agent_id)
                .is_none_or(|node| node.reservation != reservation || !node.reservation_pending)
            {
                return Err(ManagerError::StaleIncarnation(agent_id));
            }
            admission.work = Some(self.0.work.bind_incarnation(agent_id, reservation)?);
            admission.is_child = graph
                .nodes
                .get(&agent_id)
                .is_some_and(|node| node.parent_id.is_some());
            if let Some(node) = graph.nodes.get(&agent_id)
                && let Some(parent_id) = node.parent_id
            {
                admission.parent_delivery = Some(crate::actor::delivery::ParentDelivery {
                    parent_id,
                    metadata: node.metadata.clone(),
                });
            }
        }
        let (actor, task) = spawn(admission);
        let task = self.track_runner(agent_id, reservation, actor.clone(), task);
        let mut graph = self.lock_graph();
        let can_commit = !graph.rewind_affected.contains(&agent_id)
            && graph.nodes.get(&agent_id).is_some_and(|node| {
                node.reservation == reservation
                    && node.reservation_pending
                    && matches!(
                        node.lifecycle,
                        GraphLifecycle::Reserved | GraphLifecycle::Closing
                    )
            });
        if !can_commit {
            drop(graph);
            actor.shutdown();
            task.detach();
            self.rollback_reservation(agent_id, reservation);
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        let shutting_down = graph.shutting_down;
        let node = graph.nodes.get_mut(&agent_id).unwrap();
        let closing = node.lifecycle == GraphLifecycle::Closing;
        let cancel_on_commit = std::mem::take(&mut node.cancel_on_commit);
        node.actor = Some(actor.clone());
        node.task = Some(task);
        graph
            .observers
            .retain(|(_, sender)| !sender.is_disconnected());
        for (_, observer) in &graph.observers {
            actor.attach_graph_observer(observer.clone());
        }
        if !closing && !cancel_on_commit {
            if let Err(error) = self.journal_node(agent_id, &graph.nodes[&agent_id], &actor) {
                drop(graph);
                actor.close();
                self.finish_failed_commit(agent_id, reservation);
                return Err(error);
            }
            self.0.work.set_open(agent_id, true)?;
            let node = graph.nodes.get_mut(&agent_id).unwrap();
            node.reservation_pending = false;
            node.lifecycle = GraphLifecycle::Live;
            node.residency = AgentResidency::Resident;
            graph.revision += 1;
            let revision = graph.revision;
            let depth = graph.nodes[&agent_id].depth;
            let terminal = graph.nodes[&agent_id].terminal.clone();
            let live_count = graph
                .nodes
                .values()
                .filter(|node| node.lifecycle.consumes_capacity())
                .count();
            drop(graph);
            info!(manager_generation = self.0.generation, %agent_id, depth, revision, live_count, "agent node committed");
            return Ok(AgentRef {
                manager: self.clone(),
                agent_id,
                terminal,
            });
        }
        drop(graph);
        if closing {
            if shutting_down {
                actor.shutdown();
            } else {
                actor.close();
            }
            self.finish_failed_commit(agent_id, reservation);
            return Err(if shutting_down {
                ManagerError::GraphShutdown
            } else {
                ManagerError::NonLiveAgent(agent_id)
            });
        }
        actor.cancel_existing();
        #[cfg(test)]
        self.wait_at_commit_gate();
        let mut graph = self.lock_graph();
        let can_publish = !graph.shutting_down
            && !graph.rewind_affected.contains(&agent_id)
            && graph.nodes.get(&agent_id).is_some_and(|node| {
                node.reservation == reservation
                    && node.reservation_pending
                    && node.lifecycle == GraphLifecycle::Reserved
                    && node.actor.is_some()
                    && node.task.is_some()
            });
        if !can_publish {
            let shutting_down = graph.shutting_down;
            drop(graph);
            if shutting_down {
                actor.shutdown();
            } else {
                actor.close();
            }
            self.finish_failed_commit(agent_id, reservation);
            return Err(if shutting_down {
                ManagerError::GraphShutdown
            } else {
                ManagerError::NonLiveAgent(agent_id)
            });
        }
        if let Err(error) = self.journal_node(agent_id, &graph.nodes[&agent_id], &actor) {
            drop(graph);
            actor.close();
            self.finish_failed_commit(agent_id, reservation);
            return Err(error);
        }
        self.0.work.set_open(agent_id, true)?;
        let node = graph.nodes.get_mut(&agent_id).unwrap();
        node.reservation_pending = false;
        node.lifecycle = GraphLifecycle::Live;
        node.residency = AgentResidency::Resident;
        graph.revision += 1;
        let revision = graph.revision;
        let depth = graph.nodes[&agent_id].depth;
        let terminal = graph.nodes[&agent_id].terminal.clone();
        let live_count = graph
            .nodes
            .values()
            .filter(|node| node.lifecycle.consumes_capacity())
            .count();
        drop(graph);
        info!(manager_generation = self.0.generation, %agent_id, depth, revision, live_count, "agent node committed");
        Ok(AgentRef {
            manager: self.clone(),
            agent_id,
            terminal,
        })
    }

    fn journal_node(
        &self,
        agent_id: AgentId,
        node: &Node,
        actor: &AgentActorHandle,
    ) -> Result<(), ManagerError> {
        let existing = self.0.journal.agent(agent_id);
        if existing
            .as_ref()
            .is_some_and(|record| record.parent != node.parent_id)
        {
            return Err(ManagerError::Policy(
                "persisted agent parent does not match graph".into(),
            ));
        }
        let depth =
            u32::try_from(node.depth).map_err(|error| ManagerError::Policy(error.to_string()))?;
        if let Some(key) = &node.metadata.transcript_key
            && (key.is_empty() || self.0.journal.transcript_key_in_use(key, Some(agent_id)))
        {
            return Err(ManagerError::Policy(
                "child transcript key is empty or already assigned".into(),
            ));
        }
        let mut record = existing.unwrap_or_else(|| StoredAgentRecord {
            id: agent_id,
            parent: node.parent_id,
            depth,
            transcript_key: if node.parent_id.is_none() {
                String::new()
            } else {
                node.metadata
                    .transcript_key
                    .clone()
                    .unwrap_or_else(|| agent_id.to_string())
            },
            lifecycle: StoredAgentLifecycle::Open,
            ancestry_unknown: false,
            label: node.metadata.label.clone(),
            spawned_by_tool_use_id: node.metadata.spawned_by_tool_use_id.clone(),
            spec: None,
            restorable: if node.parent_id.is_none() {
                Ok(())
            } else {
                Err(OPAQUE_BACKEND.into())
            },
            usage: Default::default(),
            created_seq: 0,
            updated_seq: 0,
            legacy: false,
        });
        record.lifecycle = StoredAgentLifecycle::Open;
        if node.parent_id.is_some() {
            record.label.clone_from(&node.metadata.label);
            record
                .spawned_by_tool_use_id
                .clone_from(&node.metadata.spawned_by_tool_use_id);
        }
        let mut commit = JournalCommit {
            agents: vec![record],
            histories: [(agent_id, actor.committed_history_snapshot())].into(),
            ..Default::default()
        };
        if let Some(config) = actor.config_snapshot()
            && !self.0.journal.snapshot().state.iter().any(|record| {
                record.agent == Some(agent_id) && matches!(record.change, StateChange::Config(_))
            })
        {
            let mut effective = (*config.config).clone();
            if effective.mode_def.is_none()
                && !matches!(effective.mode, crate::AgentMode::Custom(_))
            {
                effective.mode_def = Some(crate::ModeDef::default_for(effective.mode.id()));
            }
            let stored =
                crate::session_checkpoint::stored_agent_config(&effective, Default::default())
                    .map_err(|error| ManagerError::Policy(error.to_string()))?;
            commit.changes.push(JournalChange {
                agent: Some(agent_id),
                generation: config.generation,
                change: StateChange::Config(stored),
            });
        }
        if let Some(config) = actor.config_snapshot()
            && let Some(path) = config.config.mode.plan_path()
            && let Some(edit) = crate::plan_state::allocation_edit_with_access(
                &self.0.journal.state_access(agent_id, None),
                path,
            )
            .map_err(|error| ManagerError::Policy(error.to_string()))?
        {
            commit.state_edits.push(edit);
        }
        self.0
            .journal
            .commit(commit)
            .map_err(|error| ManagerError::Policy(error.to_string()))?;
        Ok(())
    }

    pub fn set_agent_transcript_key(
        &self,
        agent_id: AgentId,
        transcript_key: String,
    ) -> Result<(), ManagerError> {
        let graph = self.lock_graph();
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        if let Some(node) = graph.nodes.get(&agent_id)
            && (node.reservation_pending || node.lifecycle != GraphLifecycle::Live)
        {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        let mut record = self
            .0
            .journal
            .agent(agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if record.parent.is_none() || transcript_key.is_empty() {
            return Err(ManagerError::Policy(
                "child transcript key must be nonempty".into(),
            ));
        }
        if record.transcript_key == transcript_key {
            return Ok(());
        }
        if self.transcript_key_in_use_locked(&graph, &transcript_key, Some(agent_id)) {
            return Err(ManagerError::Policy(
                "child transcript key is already assigned".into(),
            ));
        }
        record.transcript_key = transcript_key;
        self.0
            .journal
            .commit(JournalCommit {
                agents: vec![record],
                ..Default::default()
            })
            .map_err(|error| ManagerError::Policy(error.to_string()))?;
        Ok(())
    }

    pub fn set_agent_restoration(
        &self,
        incarnation: &AgentIncarnation,
        spec: StoredAgentSpec,
        restorable: Result<(), String>,
    ) -> Result<(), ManagerError> {
        let graph = self.lock_graph();
        if incarnation.manager_generation != self.generation() {
            return Err(ManagerError::WrongManager);
        }
        let node = graph
            .nodes
            .get(&incarnation.agent_id)
            .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
        if node.reservation != incarnation.reservation {
            return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
        }
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        if node.reservation_pending || node.lifecycle != GraphLifecycle::Live {
            return Err(ManagerError::NonLiveAgent(incarnation.agent_id));
        }
        let mut record = self
            .0
            .journal
            .agent(incarnation.agent_id)
            .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
        record.spec = Some(spec);
        record.restorable = restorable;
        self.0
            .journal
            .commit(JournalCommit {
                agents: vec![record],
                ..Default::default()
            })
            .map_err(|error| ManagerError::Policy(error.to_string()))?;
        Ok(())
    }

    pub fn set_agent_spec(
        &self,
        agent_id: AgentId,
        spec: StoredAgentSpec,
    ) -> Result<(), ManagerError> {
        let incarnation = self.incarnation(agent_id)?;
        self.set_agent_restoration(&incarnation, spec, Ok(()))
    }

    fn finish_failed_commit(&self, agent_id: AgentId, reservation: u64) {
        let mut graph = self.lock_graph();
        if graph.rewind_affected.contains(&agent_id) {
            return;
        }
        if self.0.journal.agent(agent_id).is_none() {
            if let Some(node) = graph.nodes.get(&agent_id)
                && node.reservation == reservation
            {
                let parent = node.parent_id;
                graph.nodes.remove(&agent_id);
                if let Some(parent) = parent.and_then(|id| graph.nodes.get_mut(&id)) {
                    parent.children.retain(|id| *id != agent_id);
                }
                if graph.root_id == Some(agent_id) {
                    graph.root_id = None;
                }
            }
            return;
        }
        if let Some(node) = graph.nodes.get_mut(&agent_id)
            && node.reservation == reservation
        {
            node.reservation_pending = false;
        }
        Self::reclaim_finished_closings(&mut graph, &self.0.journal);
    }

    /// Removed nodes have no runner resources remaining.
    pub fn runner_finished(&self, agent_id: AgentId) -> Result<bool, ManagerError> {
        let mut graph = self.lock_graph();
        Self::reclaim_finished_closings(&mut graph, &self.0.journal);
        Ok(graph
            .nodes
            .get(&agent_id)
            .is_none_or(|node| !node.reservation_pending && node.task.is_none()))
    }

    pub fn actor_for_incarnation(
        &self,
        incarnation: &AgentIncarnation,
    ) -> Result<AgentActorHandle, ManagerError> {
        if incarnation.manager_generation != self.generation() {
            return Err(ManagerError::WrongManager);
        }
        let graph = self.lock_graph();
        let node = graph
            .nodes
            .get(&incarnation.agent_id)
            .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
        if node.reservation != incarnation.reservation
            || node.lifecycle != GraphLifecycle::Live
            || node.residency != AgentResidency::Resident
            || node.detach_pending
        {
            return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
        }
        node.actor
            .clone()
            .ok_or(ManagerError::NonLiveAgent(incarnation.agent_id))
    }

    pub fn actor(&self, agent_id: AgentId) -> Result<AgentActorHandle, ManagerError> {
        let graph = self.lock_graph();
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if node.lifecycle != GraphLifecycle::Live || node.detach_pending {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        node.actor
            .clone()
            .ok_or(ManagerError::NonLiveAgent(agent_id))
    }

    pub fn effective_config(
        &self,
        agent_id: AgentId,
    ) -> Result<Option<Arc<crate::actor::EffectiveAgentConfig>>, ManagerError> {
        Ok(self.actor(agent_id)?.effective_config())
    }

    pub fn node(&self, agent_id: AgentId) -> Result<AgentNodeSnapshot, ManagerError> {
        let node = {
            let graph = self.lock_graph();
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            Self::capture_node_snapshot(agent_id, node)
        };
        Ok(node.snapshot())
    }

    pub fn snapshot(&self) -> Vec<AgentNodeSnapshot> {
        let nodes = {
            let graph = self.lock_graph();
            graph
                .nodes
                .iter()
                .map(|(&id, node)| Self::capture_node_snapshot(id, node))
                .collect::<Vec<_>>()
        };
        let mut nodes = nodes
            .into_iter()
            .map(CapturedNodeSnapshot::snapshot)
            .collect::<Vec<_>>();
        nodes.sort_by_key(|node| (node.depth, node.agent_id.to_string()));
        nodes
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn drain_graph_for_test(
        &self,
    ) -> Result<Vec<(AgentId, crate::actor::ActorRuntimeProbe)>, crate::ActorError> {
        let mut previous = None;
        for _ in 0..MAX_GRAPH_DRAIN_PASSES {
            let actors: Vec<_> = self
                .snapshot()
                .into_iter()
                .filter_map(|node| {
                    let actor = self.actor(node.agent_id).ok()?;
                    (actor.snapshot().lifecycle == crate::ActorLifecycle::Open)
                        .then_some((node.agent_id, actor))
                })
                .collect();
            let mut probes = Vec::with_capacity(actors.len());
            for (id, actor) in actors {
                match actor.runtime_barrier().await {
                    Ok(probe) => probes.push((id, probe)),
                    Err(ActorError::Closed | ActorError::Shutdown) => {}
                    Err(error) => return Err(error),
                }
            }
            if probes.iter().all(|(_, probe)| probe.is_quiescent())
                && previous.as_ref() == Some(&probes)
            {
                return Ok(probes);
            }
            previous = Some(probes);
            smol::future::yield_now().await;
        }
        Err(crate::ActorError::Busy)
    }

    /// Cuts local work and its managed authority without closing descendants.
    pub fn local_cancel(
        &self,
        agent_id: AgentId,
        displayed: u64,
    ) -> Result<(Option<TurnId>, u64), ManagerError> {
        let (actor, cut) = {
            let mut graph = self.lock_graph();
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            if !node.lifecycle.consumes_capacity() {
                return Err(ManagerError::NonLiveAgent(agent_id));
            }
            let actor = node
                .actor
                .clone()
                .ok_or(ManagerError::NonLiveAgent(agent_id))?;
            let mut state = actor
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(ManagerError::NonLiveAgent(agent_id));
            }
            let cut = actor.capture_local_cancel(&mut state, displayed);
            if let Some(turn_id) = cut.turn_id {
                graph.active_turns.remove(&(agent_id, turn_id));
                self.0.journal.revoke_state_turn(agent_id, turn_id);
            }
            drop(state);
            (actor, cut)
        };
        let captured = (cut.turn_id, cut.fence);
        actor.settle_local_cancel(cut);
        Ok(captured)
    }

    pub fn cancel_agent(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let actors = self.capture_cancel_cut(agent_id, false)?;
        for actor in actors {
            actor.cancel_existing();
        }
        Ok(())
    }

    pub fn cancel_subtree(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let actors = self.capture_cancel_cut(agent_id, true)?;
        for actor in actors {
            actor.cancel_existing();
        }
        Ok(())
    }

    pub fn close_incarnation(&self, incarnation: &AgentIncarnation) -> Result<(), ManagerError> {
        if incarnation.manager_generation != self.generation() {
            return Err(ManagerError::WrongManager);
        }
        let actors = {
            let mut graph = self.lock_graph();
            let node = graph
                .nodes
                .get(&incarnation.agent_id)
                .ok_or(ManagerError::UnknownAgent(incarnation.agent_id))?;
            if node.reservation != incarnation.reservation {
                return Err(ManagerError::StaleIncarnation(incarnation.agent_id));
            }
            Self::capture_subtree_locked(
                &self.0.journal,
                &self.0.work,
                &self.0.restored_delivery_work,
                &mut graph,
                incarnation.agent_id,
                true,
            )?
        };
        for (actor, cut) in actors {
            actor.settle_close(cut);
        }
        Ok(())
    }

    pub fn close_subtree(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let actors = self.capture_subtree(agent_id, true)?;
        for (actor, cut) in actors {
            actor.settle_close(cut);
        }
        Ok(())
    }

    pub fn close_descendants(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let actors = self.capture_subtree(agent_id, false)?;
        for (actor, cut) in actors {
            actor.settle_close(cut);
        }
        Ok(())
    }

    /// Cancels one nominal turn and closes only its delegated child subtrees.
    pub fn cancel_turn(
        &self,
        agent_id: AgentId,
        turn_id: TurnId,
        reason: TurnCancellationReason,
    ) -> Result<(), ManagerError> {
        let (actor, cut, descendants) = {
            let mut graph = self.lock_graph();
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            if !node.lifecycle.consumes_capacity() {
                return Err(ManagerError::NonLiveAgent(agent_id));
            }
            let actor = node
                .actor
                .clone()
                .ok_or(ManagerError::NonLiveAgent(agent_id))?;
            let mut state = actor
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let cut = match actor.capture_turn_cancel(&mut state, turn_id, reason) {
                Ok(cut) => cut,
                Err(ActorError::Closed | ActorError::Shutdown) => {
                    return Err(ManagerError::NonLiveAgent(agent_id));
                }
                Err(_) => {
                    drop(state);
                    // The runner can retire a turn the UI already cut before
                    // this request arrives; its delegated children still go.
                    if !matches!(actor.outcome(turn_id), Some(TurnOutcome::Cancelled { .. })) {
                        return Err(ManagerError::InactiveTurn { agent_id, turn_id });
                    }
                    let descendants = Self::capture_turn_children(
                        &self.0.journal,
                        &self.0.work,
                        &self.0.restored_delivery_work,
                        &mut graph,
                        agent_id,
                        Some(turn_id),
                    )?;
                    drop(graph);
                    for (descendant, close) in descendants {
                        descendant.settle_close(close);
                    }
                    return Err(ManagerError::InactiveTurn { agent_id, turn_id });
                }
            };
            let descendants = Self::capture_turn_children(
                &self.0.journal,
                &self.0.work,
                &self.0.restored_delivery_work,
                &mut graph,
                agent_id,
                Some(turn_id),
            )?;
            drop(state);
            (actor, cut, descendants)
        };
        #[cfg(test)]
        self.wait_at_descendant_cut_gate();
        actor.settle_turn_cancel(cut, reason);
        for (descendant, close) in descendants {
            descendant.settle_close(close);
        }
        Ok(())
    }

    /// Cancels matching root work and atomically retires its descendant authority.
    pub fn cancel_correlation(
        &self,
        agent_id: AgentId,
        correlation: &str,
        reason: TurnCancellationReason,
    ) -> Result<(), ManagerError> {
        let (actor, cut, descendants) = {
            let mut graph = self.lock_graph();
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            let node = graph
                .nodes
                .get(&agent_id)
                .ok_or(ManagerError::UnknownAgent(agent_id))?;
            if !node.lifecycle.consumes_capacity() {
                return Err(ManagerError::NonLiveAgent(agent_id));
            }
            let actor = node
                .actor
                .clone()
                .ok_or(ManagerError::NonLiveAgent(agent_id))?;
            let mut state = actor
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let cut = actor.capture_correlation_cancel(&mut state, correlation, reason);
            let descendants = Self::capture_turn_children(
                &self.0.journal,
                &self.0.work,
                &self.0.restored_delivery_work,
                &mut graph,
                agent_id,
                cut.active_turn,
            )?;
            drop(state);
            (actor, cut, descendants)
        };
        #[cfg(test)]
        self.wait_at_descendant_cut_gate();
        actor.settle_correlation_cancel(cut, correlation, reason);
        for (descendant, close) in descendants {
            descendant.settle_close(close);
        }
        Ok(())
    }

    pub async fn shutdown(&self, timeout: Duration) -> ShutdownReport {
        let deadline = std::time::Instant::now() + timeout;
        let publication = if let Some(publication) = self.0.journal.try_publication_read() {
            Some(publication)
        } else if timeout.is_zero() {
            None
        } else {
            futures_lite::future::or(
                async { Some(self.0.journal.publication_read().await) },
                async {
                    smol::Timer::at(deadline).await;
                    None
                },
            )
            .await
        };
        let Some(publication) = publication else {
            let mut timed_out = {
                let mut graph = self.lock_graph();
                graph.shutting_down = true;
                self.0.journal.revoke_all_state_turns();
                self.0.shutdown.store(true, Ordering::Release);
                graph
                    .nodes
                    .iter()
                    .filter_map(|(&id, node)| {
                        let pending = node.lifecycle.consumes_capacity()
                            || node.reservation_pending
                            || node.task.as_ref().is_some_and(|task| !task.is_finished());
                        if pending {
                            let _ = self.0.work.set_open(id, false);
                        }
                        pending.then_some(id)
                    })
                    .collect::<Vec<_>>()
            };
            for &id in &timed_out {
                if let Ok(incarnation) = self.incarnation(id) {
                    let _ = self.defer_incarnation_cleanup(&incarnation, false);
                }
            }
            timed_out.sort_by_key(ToString::to_string);
            let manager = self.clone();
            smol::spawn(async move {
                manager.reap_shutdown().await;
            })
            .detach();
            return ShutdownReport {
                joined: Vec::new(),
                timed_out,
            };
        };
        let (ids, actors, finished_tombstone_tasks) = {
            let mut graph = self.lock_graph();
            let ids: Vec<_> = graph
                .nodes
                .iter()
                .filter_map(|(&id, node)| {
                    (node.lifecycle.consumes_capacity()
                        || node.reservation_pending
                        || node.task.as_ref().is_some_and(|task| !task.is_finished()))
                    .then_some(id)
                })
                .collect();
            let actors = ids
                .iter()
                .filter_map(|id| {
                    let actor = graph.nodes[id].actor.clone()?;
                    let cut = actor.capture_close(true)?;
                    Some((actor, cut))
                })
                .collect::<Vec<_>>();
            graph.shutting_down = true;
            self.0.journal.revoke_all_state_turns();
            self.0.shutdown.store(true, Ordering::Release);
            for id in &ids {
                let replacement = Self::next_reservation(&mut graph);
                if let Some(node) = graph.nodes.get_mut(id) {
                    let _ = self.0.work.set_open(*id, false);
                    if let Some(attempt) = node.activation.take() {
                        attempt.complete(Err(ManagerError::GraphShutdown));
                        node.reservation_pending = false;
                        node.reservation = replacement;
                        node.detach_pending = false;
                        self.0.journal.set_incarnation(*id, replacement);
                        let _ = self.0.work.bind_incarnation(*id, replacement);
                        node.residency = AgentResidency::Dormant;
                        node.lifecycle = GraphLifecycle::Dormant;
                    } else if node.lifecycle.consumes_capacity() {
                        node.lifecycle = GraphLifecycle::Closing;
                    }
                }
            }
            self.0.reaped.notify(usize::MAX);
            let finished_tombstone_tasks = graph
                .nodes
                .values_mut()
                .filter(|node| {
                    !node.lifecycle.consumes_capacity()
                        && !node.reservation_pending
                        && node.task.as_ref().is_some_and(smol::Task::is_finished)
                })
                .filter_map(|node| node.task.take())
                .collect::<Vec<_>>();
            (ids, actors, finished_tombstone_tasks)
        };
        drop(publication);
        #[cfg(test)]
        self.wait_at_test_gate(&self.0.shutdown_cut_gate);
        for (actor, cut) in actors {
            actor.settle_close(cut);
        }
        for task in finished_tombstone_tasks {
            task.await;
        }
        loop {
            let done = self.finished_nodes(&ids);
            if (done.len() == ids.len() && self.cleanup_finished())
                || std::time::Instant::now() >= deadline
            {
                let mut joined: Vec<_> = done.into_iter().collect();
                joined.sort_by_key(ToString::to_string);
                let mut timed_out: Vec<_> = ids
                    .iter()
                    .copied()
                    .filter(|id| !joined.contains(id))
                    .collect();
                timed_out.sort_by_key(ToString::to_string);
                if timed_out.is_empty() && self.cleanup_finished() {
                    self.take_finished_tasks(&joined).await;
                    self.take_watcher_tasks().await;
                } else {
                    warn!(
                        manager_generation = self.0.generation,
                        timed_out = timed_out.len(),
                        "agent graph shutdown timed out; cleanup transferred to reaper"
                    );
                    let inner = Arc::clone(&self.0);
                    smol::spawn(async move {
                        AgentManagerHandle(inner).reap_shutdown().await;
                    })
                    .detach();
                }
                return ShutdownReport { joined, timed_out };
            }
            smol::Timer::after(SHUTDOWN_POLL_INTERVAL).await;
        }
    }

    async fn take_finished_tasks(&self, ids: &[AgentId]) {
        let tasks = {
            let mut graph = self.lock_graph();
            ids.iter()
                .filter_map(|id| graph.nodes.get_mut(id)?.task.take())
                .collect::<Vec<_>>()
        };
        for task in tasks {
            task.await;
        }
    }

    async fn take_watcher_tasks(&self) {
        let tasks = {
            let mut watchers = self
                .0
                .watchers
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            watchers.reapable.clear();
            watchers
                .tasks
                .drain()
                .map(|(_, task)| task)
                .collect::<Vec<_>>()
        };
        for task in tasks {
            task.await;
        }
    }

    async fn reap_shutdown(&self) {
        loop {
            let pending = {
                let graph = self.lock_graph();
                graph
                    .nodes
                    .iter()
                    .filter_map(|(&id, node)| {
                        (node.reservation_pending || node.task.is_some()).then_some(id)
                    })
                    .collect::<Vec<_>>()
            };
            if pending.is_empty() && self.cleanup_finished() {
                break;
            }
            let finished = self.finished_nodes(&pending);
            if finished.is_empty() {
                smol::Timer::after(SHUTDOWN_POLL_INTERVAL).await;
                continue;
            }
            self.take_finished_tasks(&finished.into_iter().collect::<Vec<_>>())
                .await;
        }
        self.take_watcher_tasks().await;
        self.0.reaped.notify(usize::MAX);
    }

    #[cfg(test)]
    async fn wait_until_reaped(&self) {
        loop {
            let listener = self.0.reaped.listen();
            let done = self
                .lock_graph()
                .nodes
                .values()
                .all(|node| !node.reservation_pending && node.task.is_none());
            if done && self.cleanup_finished() {
                return;
            }
            listener.await;
        }
    }

    fn reclaim_finished_closings(graph: &mut GraphState, journal: &SessionJournal) {
        for (&id, node) in &mut graph.nodes {
            if node.reservation_pending
                || node.detach_pending
                || node.task.as_ref().is_some_and(|task| !task.is_finished())
            {
                continue;
            }
            if node.lifecycle == GraphLifecycle::Closing {
                node.lifecycle = GraphLifecycle::Closed;
                node.residency = AgentResidency::Dormant;
                graph.revision += 1;
            }
            if matches!(
                node.lifecycle,
                GraphLifecycle::Closed | GraphLifecycle::Retired | GraphLifecycle::Dormant
            ) {
                if node.residency != AgentResidency::Dormant {
                    node.residency = AgentResidency::Dormant;
                    graph.revision += 1;
                }
                if let Some(task) = node.task.take() {
                    task.detach();
                }
                graph
                    .active_turns
                    .retain(|(agent_id, _), _| *agent_id != id);
                journal.revoke_state_agent(id);
            }
        }
    }

    fn finished_nodes(&self, ids: &[AgentId]) -> HashSet<AgentId> {
        let mut graph = self.lock_graph();
        let shutting_down = graph.shutting_down;
        let done: HashSet<_> = ids
            .iter()
            .copied()
            .filter(|id| {
                graph.nodes.get(id).is_none_or(|node| {
                    !node.reservation_pending
                        && node.task.as_ref().is_none_or(smol::Task::is_finished)
                })
            })
            .collect();
        for id in &done {
            if let Some(node) = graph.nodes.get_mut(id)
                && !node.detach_pending
            {
                if shutting_down
                    && matches!(
                        node.lifecycle,
                        GraphLifecycle::Closing | GraphLifecycle::Closed
                    )
                    && node
                        .actor
                        .as_ref()
                        .is_some_and(|actor| actor.lifecycle() == ActorLifecycle::Shutdown)
                {
                    node.lifecycle = GraphLifecycle::Dormant;
                    node.residency = AgentResidency::Dormant;
                } else if !matches!(
                    node.lifecycle,
                    GraphLifecycle::Retired | GraphLifecycle::Dormant
                ) {
                    node.lifecycle = GraphLifecycle::Closed;
                }
            }
        }
        Self::reclaim_finished_closings(&mut graph, &self.0.journal);
        done
    }

    fn capture_cancel_cut(
        &self,
        agent_id: AgentId,
        subtree: bool,
    ) -> Result<Vec<AgentActorHandle>, ManagerError> {
        let mut graph = self.lock_graph();
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if !node.lifecycle.consumes_capacity() {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        let ids = if subtree {
            Self::subtree_ids(&graph, agent_id)
        } else {
            vec![agent_id]
        };
        let mut actors = Vec::new();
        let mut reserved_marked_count = 0;
        for id in ids {
            self.0.journal.revoke_state_agent(id);
            let Some(node) = graph.nodes.get_mut(&id) else {
                continue;
            };
            if node.lifecycle == GraphLifecycle::Reserved {
                node.cancel_on_commit = true;
                reserved_marked_count += 1;
            } else if let Some(actor) = &node.actor {
                actors.push(actor.clone());
            }
        }
        graph.revision += 1;
        let revision = graph.revision;
        let captured_actor_count = actors.len();
        drop(graph);
        info!(
            manager_generation = self.0.generation,
            target = %agent_id,
            subtree,
            revision,
            captured_actor_count,
            reserved_marked_count,
            "agent cancellation cut captured"
        );
        Ok(actors)
    }

    fn capture_subtree(
        &self,
        agent_id: AgentId,
        include_root: bool,
    ) -> Result<Vec<(AgentActorHandle, ActorCloseCut)>, ManagerError> {
        let mut graph = self.lock_graph();
        Self::capture_subtree_locked(
            &self.0.journal,
            &self.0.work,
            &self.0.restored_delivery_work,
            &mut graph,
            agent_id,
            include_root,
        )
    }

    fn capture_subtree_locked(
        journal: &SessionJournal,
        work: &WorkLedger,
        delivery_work: &DeliveryWork,
        graph: &mut GraphState,
        agent_id: AgentId,
        include_root: bool,
    ) -> Result<Vec<(AgentActorHandle, ActorCloseCut)>, ManagerError> {
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if node.lifecycle == GraphLifecycle::Retired {
            return Err(ManagerError::Retired(agent_id));
        }
        let mut ids = Self::subtree_ids(graph, agent_id);
        if !include_root {
            ids.remove(0);
        }
        let agents = ids
            .iter()
            .filter_map(|id| journal.agent(*id))
            .filter_map(|mut record| {
                if record.lifecycle != StoredAgentLifecycle::Open {
                    return None;
                }
                record.lifecycle = StoredAgentLifecycle::Closed;
                Some(record)
            })
            .collect();
        let snapshot = journal
            .checkpoint_snapshot()
            .map_err(ManagerError::InvalidGraph)?;
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
        let changes = ids
            .iter()
            .filter(|id| {
                graph
                    .nodes
                    .get(id)
                    .is_some_and(|node| node.residency != AgentResidency::Resident)
            })
            .flat_map(|id| {
                view.scopes
                    .get(&Some(*id))
                    .into_iter()
                    .flat_map(move |scope| {
                        scope
                            .deliveries
                            .iter()
                            .filter(|(_, op)| matches!(op, StoredDeliveryOp::Queued(_)))
                            .map(move |(delivery, _)| JournalChange {
                                agent: Some(*id),
                                generation: scope.generation,
                                change: StateChange::Delivery {
                                    id: *delivery,
                                    op: StoredDeliveryOp::Dropped,
                                },
                            })
                    })
            })
            .collect();
        journal
            .commit(JournalCommit {
                agents,
                changes,
                ..Default::default()
            })
            .map_err(|error| ManagerError::Policy(error.to_string()))?;
        delivery_work
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|(target, _, _), _| !ids.contains(target));
        let mut transitioned = false;
        let actors = ids
            .into_iter()
            .filter_map(|id| {
                journal.revoke_state_agent(id);
                let node = graph.nodes.get_mut(&id)?;
                if matches!(
                    node.lifecycle,
                    GraphLifecycle::Closed | GraphLifecycle::Retired | GraphLifecycle::Removed
                ) {
                    return None;
                }
                let _ = work.set_open(id, false);
                if let Some(attempt) = node.activation.take() {
                    attempt.complete(Err(ManagerError::NonLiveAgent(id)));
                }
                let actor = node.actor.clone();
                let cut = actor.as_ref().and_then(|actor| actor.capture_close(false));
                if node.lifecycle != GraphLifecycle::Closing {
                    node.lifecycle = if actor.is_some() || node.reservation_pending {
                        GraphLifecycle::Closing
                    } else {
                        GraphLifecycle::Closed
                    };
                    transitioned = true;
                }
                actor.zip(cut)
            })
            .collect();
        graph.revision += u64::from(transitioned);
        Ok(actors)
    }

    fn capture_turn_children(
        journal: &SessionJournal,
        work: &WorkLedger,
        delivery_work: &DeliveryWork,
        graph: &mut GraphState,
        agent_id: AgentId,
        turn_id: Option<TurnId>,
    ) -> Result<Vec<(AgentActorHandle, ActorCloseCut)>, ManagerError> {
        let Some(turn_id) = turn_id else {
            return Ok(Vec::new());
        };
        graph.active_turns.remove(&(agent_id, turn_id));
        journal.revoke_state_turn(agent_id, turn_id);
        let owned = graph
            .nodes
            .get(&agent_id)
            .map(|node| {
                node.children
                    .iter()
                    .copied()
                    .filter(|id| {
                        graph.nodes.get(id).is_some_and(|child| {
                            child.turn_owner == Some((agent_id, turn_id))
                                && child.lifecycle.consumes_capacity()
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut actors = Vec::new();
        for id in owned {
            actors.extend(Self::capture_subtree_locked(
                journal,
                work,
                delivery_work,
                graph,
                id,
                true,
            )?);
        }
        Ok(actors)
    }

    fn subtree_ids(graph: &GraphState, root: AgentId) -> Vec<AgentId> {
        let mut result = Vec::new();
        let mut pending = vec![root];
        while let Some(id) = pending.pop() {
            result.push(id);
            if let Some(node) = graph.nodes.get(&id) {
                pending.extend(node.children.iter().rev().copied());
            }
        }
        result
    }

    fn capture_node_snapshot(agent_id: AgentId, node: &Node) -> CapturedNodeSnapshot {
        CapturedNodeSnapshot {
            agent_id,
            parent_id: node.parent_id,
            root_id: node.root_id,
            depth: node.depth,
            children: node.children.clone(),
            graph_lifecycle: node.lifecycle,
            residency: node.residency,
            actor: node.actor.clone(),
            metadata: node.metadata.clone(),
        }
    }

    fn next_reservation(graph: &mut GraphState) -> u64 {
        let value = graph.next_reservation;
        graph.next_reservation += 1;
        value
    }

    #[cfg(test)]
    fn set_commit_gate(&self, entered: flume::Sender<()>, release: flume::Receiver<()>) {
        *self
            .0
            .commit_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(TestGate { entered, release });
    }

    #[cfg(test)]
    fn wait_at_commit_gate(&self) {
        self.wait_at_test_gate(&self.0.commit_gate);
    }

    #[cfg(test)]
    fn set_descendant_cut_gate(&self, entered: flume::Sender<()>, release: flume::Receiver<()>) {
        *self
            .0
            .descendant_cut_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(TestGate { entered, release });
    }

    #[cfg(test)]
    fn wait_at_descendant_cut_gate(&self) {
        self.wait_at_test_gate(&self.0.descendant_cut_gate);
    }

    #[cfg(test)]
    fn set_managed_acquire_gate(&self, entered: flume::Sender<()>, release: flume::Receiver<()>) {
        *self
            .0
            .managed_acquire_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(TestGate { entered, release });
    }

    #[cfg(test)]
    fn wait_at_managed_acquire_gate(&self) {
        self.wait_at_test_gate(&self.0.managed_acquire_gate);
    }

    #[cfg(test)]
    fn set_prompt_wait_registration_gate(
        &self,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    ) {
        *self
            .0
            .prompt_wait_registration_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(TestGate { entered, release });
    }

    #[cfg(test)]
    fn wait_at_prompt_wait_registration_gate(&self) {
        self.wait_at_test_gate(&self.0.prompt_wait_registration_gate);
    }

    #[cfg(test)]
    fn wait_at_test_gate(&self, slot: &Mutex<Option<TestGate>>) {
        let gate = slot
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.release.recv().unwrap();
        }
    }

    fn lock_graph(&self) -> std::sync::MutexGuard<'_, GraphState> {
        self.0
            .graph
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

struct LeaseState {
    permit: Option<SemaphoreGuardArc>,
    owns_permit: bool,
    suspensions: usize,
    suspensions_sealed: bool,
    closing: bool,
    wake: Option<std::task::Waker>,
    watcher_cancels: HashMap<u64, Option<crate::cancel::ReasonedCancelTrigger>>,
}

pub(crate) struct LeaseInner {
    agent_id: AgentId,
    turn_id: TurnId,
    limiter: Arc<Semaphore>,
    state: Mutex<LeaseState>,
    owned: Event,
}

impl LeaseInner {
    fn suspend(
        &self,
        watcher_id: u64,
        cancel: crate::cancel::ReasonedCancelTrigger,
    ) -> Result<(), ManagerError> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing || state.suspensions_sealed {
            return Err(ManagerError::InactiveTurn {
                agent_id: self.agent_id,
                turn_id: self.turn_id,
            });
        }
        state.watcher_cancels.insert(watcher_id, Some(cancel));
        state.suspensions += 1;
        if state.suspensions == 1 {
            state.permit.take();
            state.owns_permit = false;
        }
        if let Some(wake) = state.wake.take() {
            wake.wake();
        }
        Ok(())
    }

    fn retire(&self, watcher_id: u64) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.watcher_cancels.remove(&watcher_id).is_none() {
            return;
        }
        state.suspensions -= 1;
        if state.suspensions == 0
            && let Some(wake) = state.wake.take()
        {
            wake.wake();
        }
    }

    fn cancel_waiters(&self) {
        let cancels = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.suspensions_sealed = true;
            state
                .watcher_cancels
                .values_mut()
                .filter_map(Option::take)
                .collect::<Vec<_>>()
        };
        drop(cancels);
    }

    fn close(&self) {
        let cancels = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closing = true;
            state.permit.take();
            state.owns_permit = false;
            if let Some(wake) = state.wake.take() {
                wake.wake();
            }
            state
                .watcher_cancels
                .values_mut()
                .filter_map(Option::take)
                .collect::<Vec<_>>()
        };
        drop(cancels);
        self.owned.notify(usize::MAX);
    }

    async fn wait_until_owned(&self) {
        loop {
            let listener = self.owned.listen();
            let ready = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                state.closing || (state.suspensions == 0 && state.owns_permit)
            };
            if ready {
                return;
            }
            listener.await;
        }
    }
}

pub(crate) struct PromptWaitInner {
    lease: Arc<LeaseInner>,
    manager: Weak<ManagerInner>,
    watcher_id: u64,
    result: Mutex<Option<Result<crate::TurnOutcome, PromptWaitError>>>,
    completed: Event,
    cancel: Mutex<Option<crate::cancel::ReasonedCancelTrigger>>,
}

impl PromptWaitInner {
    fn complete(&self, result: Result<crate::TurnOutcome, PromptWaitError>) {
        let mut slot = self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot.is_none() {
            *slot = Some(result);
            self.completed.notify(usize::MAX);
        }
    }

    fn cancel(&self) {
        self.cancel
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
    }

    async fn wait_result(&self) -> Result<crate::TurnOutcome, PromptWaitError> {
        loop {
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
            {
                return result;
            }
            let listener = self.completed.listen();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
            {
                return result;
            }
            listener.await;
        }
    }

    fn mark_reapable(&self) {
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let task = {
            let mut watchers = manager
                .watchers
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(task) = watchers.tasks.remove(&self.watcher_id) {
                Some(task)
            } else {
                watchers.reapable.insert(self.watcher_id);
                None
            }
        };
        if let Some(task) = task {
            smol::spawn(async move {
                task.await;
            })
            .detach();
        }
    }
}

struct PermitSuspensionGuard {
    lease: Arc<LeaseInner>,
    watcher_id: u64,
}

impl Drop for PermitSuspensionGuard {
    fn drop(&mut self) {
        self.lease.retire(self.watcher_id);
    }
}

pub(crate) struct ManagedTurnGuard {
    manager: Weak<ManagerInner>,
    lease: Arc<LeaseInner>,
    agent_id: AgentId,
    turn_id: TurnId,
    nonce: u64,
}

impl Drop for ManagedTurnGuard {
    fn drop(&mut self) {
        if let Some(manager) = self.manager.upgrade() {
            let mut graph = manager
                .graph
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if graph.active_turns.get(&(self.agent_id, self.turn_id)) == Some(&self.nonce) {
                graph.active_turns.remove(&(self.agent_id, self.turn_id));
                manager
                    .journal
                    .revoke_state_turn(self.agent_id, self.turn_id);
            }
        }
        self.lease.close();
    }
}

pub(crate) struct ManagedExecutionFuture<'a> {
    backend: Pin<Box<dyn Future<Output = crate::BackendResult> + Send + 'a>>,
    guard: Option<ManagedTurnGuard>,
    lease: Arc<LeaseInner>,
    acquire: Option<Pin<Box<dyn Future<Output = SemaphoreGuardArc> + Send + 'static>>>,
    cancellation: Pin<Box<dyn Future<Output = crate::TurnCancellationReason> + Send + 'static>>,
    cancellation_observed: bool,
}

impl Future for ManagedExecutionFuture<'_> {
    type Output = crate::BackendResult;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let cancellation_ready =
                !self.cancellation_observed && self.cancellation.as_mut().poll(context).is_ready();
            if cancellation_ready {
                self.cancellation_observed = true;
                self.lease.cancel_waiters();
                if let Some(manager) = self
                    .guard
                    .as_ref()
                    .and_then(|guard| guard.manager.upgrade())
                {
                    let guard = self
                        .guard
                        .as_ref()
                        .expect("guard exists while execution is active");
                    let mut graph = manager
                        .graph
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if graph.active_turns.get(&(guard.agent_id, guard.turn_id))
                        == Some(&guard.nonce)
                    {
                        graph.active_turns.remove(&(guard.agent_id, guard.turn_id));
                        manager
                            .journal
                            .revoke_state_turn(guard.agent_id, guard.turn_id);
                    }
                }
            }
            let mut state = self
                .lease
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.suspensions > 0 {
                state.wake = Some(context.waker().clone());
                drop(state);
                self.acquire = None;
                return Poll::Pending;
            }
            if let Some(permit) = state.permit.take() {
                state.wake = Some(context.waker().clone());
                drop(state);
                let result = self.backend.as_mut().poll(context);
                let mut state = self
                    .lease
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let restored_ownership =
                    result.is_pending() && state.suspensions == 0 && !state.closing;
                if restored_ownership {
                    state.permit = Some(permit);
                    state.owns_permit = true;
                }
                drop(state);
                if restored_ownership {
                    self.lease.owned.notify(usize::MAX);
                }
                if result.is_ready() {
                    self.guard.take();
                }
                return result;
            }
            if state.closing {
                state.wake = Some(context.waker().clone());
                return Poll::Pending;
            }
            state.wake = Some(context.waker().clone());
            drop(state);

            let limiter = Arc::clone(&self.lease.limiter);
            let acquire = self
                .acquire
                .get_or_insert_with(|| Box::pin(limiter.acquire_arc()));
            let Poll::Ready(permit) = acquire.as_mut().poll(context) else {
                return Poll::Pending;
            };
            self.acquire = None;
            let mut state = self
                .lease
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.suspensions == 0 && !state.closing {
                state.permit = Some(permit);
                state.owns_permit = true;
                self.lease.owned.notify(usize::MAX);
            }
        }
    }
}

impl Drop for ManagedExecutionFuture<'_> {
    fn drop(&mut self) {
        self.guard.take();
        self.lease.close();
    }
}

pub(crate) fn manage_execution<'a>(
    backend: Pin<Box<dyn Future<Output = crate::BackendResult> + Send + 'a>>,
    guard: ManagedTurnGuard,
    lease: Arc<LeaseInner>,
    cancel: crate::ReasonedCancelToken,
) -> ManagedExecutionFuture<'a> {
    let cancellation = Box::pin(async move { cancel.cancelled().await });
    ManagedExecutionFuture {
        backend,
        guard: Some(guard),
        lease,
        acquire: None,
        cancellation,
        cancellation_observed: false,
    }
}

pub(crate) async fn enter_managed_turn(
    manager: &Weak<ManagerInner>,
    agent_id: AgentId,
    turn_id: TurnId,
    cancel: &crate::ReasonedCancelToken,
) -> Result<(ManagedTurnGuard, CurrentManagedTurn), crate::TurnCancellationReason> {
    let Some(inner) = manager.upgrade() else {
        return Err(crate::TurnCancellationReason::Shutdown);
    };
    if inner.shutdown.load(Ordering::Acquire) {
        return Err(crate::TurnCancellationReason::Shutdown);
    }
    #[cfg(test)]
    AgentManagerHandle(Arc::clone(&inner)).wait_at_managed_acquire_gate();
    let permit = cancel.race(inner.limiter.acquire_arc()).await?;
    if inner.shutdown.load(Ordering::Acquire) {
        return Err(crate::TurnCancellationReason::Shutdown);
    }
    let nonce = NEXT_TURN_NONCE.fetch_add(1, Ordering::Relaxed);
    let registered = {
        let mut graph = inner
            .graph
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if graph.shutting_down {
            return Err(crate::TurnCancellationReason::Shutdown);
        }
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(crate::TurnCancellationReason::Closed)?;
        if node.lifecycle != GraphLifecycle::Live || node.detach_pending {
            return Err(crate::TurnCancellationReason::Closed);
        }
        let actor = node
            .actor
            .as_ref()
            .ok_or(crate::TurnCancellationReason::Closed)?;
        let state = actor
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(match state.lifecycle {
                ActorLifecycle::Shutdown => crate::TurnCancellationReason::Shutdown,
                ActorLifecycle::Open | ActorLifecycle::Closed => {
                    crate::TurnCancellationReason::Closed
                }
            });
        }
        let registered = state.status == ActorStatus::Running(turn_id) && state.active.is_some();
        drop(state);
        if registered {
            graph.active_turns.insert((agent_id, turn_id), nonce);
            inner.journal.register_state_turn(agent_id, turn_id, nonce);
        }
        registered
    };
    if !registered {
        drop(permit);
        #[cfg(test)]
        if let Some(rejected) = inner
            .managed_registration_rejected
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            rejected.send(()).unwrap();
        }
        return Err(cancel.cancelled().await);
    }
    let token = types::ManagedTurnToken {
        manager: AgentManagerHandle(Arc::clone(&inner)),
        agent_id,
        turn_id,
        nonce,
    };
    let lease = Arc::new(LeaseInner {
        agent_id,
        turn_id,
        limiter: Arc::clone(&inner.limiter),
        state: Mutex::new(LeaseState {
            permit: Some(permit),
            owns_permit: true,
            suspensions: 0,
            suspensions_sealed: false,
            closing: false,
            wake: None,
            watcher_cancels: HashMap::new(),
        }),
        owned: Event::new(),
    });
    let current = CurrentManagedTurn {
        state_access: inner.journal.state_access(agent_id, Some((turn_id, nonce))),
        token,
        lease: TurnPermitLease {
            inner: Arc::clone(&lease),
        },
        policy: None,
        mode: None,
        mode_def: None,
        context: None,
    };
    Ok((
        ManagedTurnGuard {
            manager: Arc::downgrade(&inner),
            lease,
            agent_id,
            turn_id,
            nonce,
        },
        current,
    ))
}
