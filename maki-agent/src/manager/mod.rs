//! Per-outer-session agent graph and production actor factory.
//!
//! The manager owns topology, actor tasks, graph limits, and managed turn
//! admission. Node ids describe topology but grant no authority: descendants
//! can only be created with a manager-issued token for a currently executing
//! turn. The graph lock is never held across actor calls or awaits.

mod manager_error;
#[cfg(test)]
mod tests;
mod types;

pub use manager_error::ManagerError;
pub use types::{
    AgentLimits, AgentMetadata, AgentNodeSnapshot, AgentRef, CurrentManagedTurn, GraphLifecycle,
    ManagedPromptWait, PromptAdmission, PromptWaitError, ShutdownReport, TurnPermitLease,
};

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use async_lock::{Semaphore, SemaphoreGuardArc};
use event_listener::Event;
use maki_providers::Message;
use tracing::{info, warn};

use crate::actor::ManagedTurnAdmission;
use crate::{ActorBackend, AgentActorHandle, AgentId, SharedMessages, TurnId};

const SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(10);
static NEXT_MANAGER_GENERATION: AtomicU64 = AtomicU64::new(1);
static NEXT_TURN_NONCE: AtomicU64 = AtomicU64::new(1);

type RunnerTask = smol::Task<()>;

struct Node {
    parent_id: Option<AgentId>,
    root_id: AgentId,
    depth: usize,
    children: Vec<AgentId>,
    lifecycle: GraphLifecycle,
    actor: Option<AgentActorHandle>,
    task: Option<RunnerTask>,
    metadata: AgentMetadata,
    reservation: u64,
    reservation_pending: bool,
    cancel_on_commit: bool,
}

struct CapturedNodeSnapshot {
    agent_id: AgentId,
    parent_id: Option<AgentId>,
    root_id: AgentId,
    depth: usize,
    children: Vec<AgentId>,
    graph_lifecycle: GraphLifecycle,
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
}

#[cfg(test)]
#[derive(Clone)]
struct TestGate {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
}

#[cfg(test)]
#[derive(Clone)]
struct PromptAdmissionGate {
    admitted: flume::Sender<TurnId>,
    release: flume::Receiver<()>,
}

pub(crate) struct ManagerInner {
    generation: u64,
    limits: AgentLimits,
    graph: Mutex<GraphState>,
    limiter: Arc<Semaphore>,
    watchers: Mutex<WatcherRegistry>,
    next_watcher: AtomicU64,
    shutdown: AtomicBool,
    reaped: Event,
    #[cfg(test)]
    commit_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    prompt_admission_gate: Mutex<Option<PromptAdmissionGate>>,
    #[cfg(test)]
    prompt_wait_registration_gate: Mutex<Option<TestGate>>,
    #[cfg(test)]
    descendant_cut_gate: Mutex<Option<TestGate>>,
}

#[derive(Clone)]
pub struct AgentManagerHandle(Arc<ManagerInner>);

impl AgentManagerHandle {
    pub fn new(limits: AgentLimits) -> Result<Self, ManagerError> {
        if limits.max_concurrent_agent_turns == 0
            || limits.max_agent_depth == 0
            || limits.max_children_per_agent == 0
            || limits.max_live_agents == 0
        {
            return Err(ManagerError::InvalidLimits);
        }
        Ok(AgentManagerHandle(Arc::new(ManagerInner {
            generation: NEXT_MANAGER_GENERATION.fetch_add(1, Ordering::Relaxed),
            limiter: Arc::new(Semaphore::new(limits.max_concurrent_agent_turns)),
            limits,
            watchers: Mutex::new(WatcherRegistry::default()),
            next_watcher: AtomicU64::new(1),
            graph: Mutex::new(GraphState {
                root_id: None,
                nodes: HashMap::new(),
                active_turns: HashMap::new(),
                revision: 0,
                next_reservation: 1,
                shutting_down: false,
            }),
            shutdown: AtomicBool::new(false),
            reaped: Event::new(),
            #[cfg(test)]
            commit_gate: Mutex::new(None),
            #[cfg(test)]
            prompt_admission_gate: Mutex::new(None),
            #[cfg(test)]
            prompt_wait_registration_gate: Mutex::new(None),
            #[cfg(test)]
            descendant_cut_gate: Mutex::new(None),
        })))
    }
}

impl AgentManagerHandle {
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
        let agent_id = AgentId::generate();
        let reservation = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.root_id.is_some() {
                return Err(ManagerError::DuplicateRoot);
            }
            let reservation = Self::next_reservation(&mut graph);
            graph.root_id = Some(agent_id);
            graph.nodes.insert(
                agent_id,
                Node {
                    parent_id: None,
                    root_id: agent_id,
                    depth: 0,
                    children: Vec::new(),
                    lifecycle: GraphLifecycle::Reserved,
                    actor: None,
                    task: None,
                    metadata: AgentMetadata::default(),
                    reservation,
                    reservation_pending: true,
                    cancel_on_commit: false,
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
        self.commit_actor(
            agent_id,
            reservation,
            initial_messages,
            shared_messages,
            backend,
        )
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
        let parent_id = current.agent_id();
        let child_id = AgentId::generate();
        let reservation = {
            self.validate_manager(current)?;
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            Self::reclaim_finished_closings(&mut graph);
            if graph.active_turns.get(&(parent_id, current.turn_id())) != Some(&current.token.nonce)
            {
                return Err(ManagerError::InactiveTurn {
                    agent_id: parent_id,
                    turn_id: current.turn_id(),
                });
            }
            let (root_id, depth, child_count) = {
                let parent = graph
                    .nodes
                    .get(&parent_id)
                    .ok_or(ManagerError::UnknownAgent(parent_id))?;
                if parent.lifecycle != GraphLifecycle::Live {
                    return Err(ManagerError::NonLiveAgent(parent_id));
                }
                (
                    parent.root_id,
                    parent.depth + 1,
                    parent
                        .children
                        .iter()
                        .filter(|id| {
                            graph
                                .nodes
                                .get(id)
                                .is_some_and(|node| node.lifecycle.consumes_capacity())
                        })
                        .count(),
                )
            };
            if depth > self.0.limits.max_agent_depth {
                return Err(ManagerError::DepthExceeded {
                    depth,
                    max: self.0.limits.max_agent_depth,
                });
            }
            if child_count >= self.0.limits.max_children_per_agent {
                return Err(ManagerError::ChildLimit {
                    parent_id,
                    max: self.0.limits.max_children_per_agent,
                });
            }
            let live_count = graph
                .nodes
                .values()
                .filter(|node| node.lifecycle.consumes_capacity())
                .count();
            if live_count >= self.0.limits.max_live_agents {
                return Err(ManagerError::LiveAgentLimit {
                    max: self.0.limits.max_live_agents,
                });
            }
            let reservation = Self::next_reservation(&mut graph);
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
                    actor: None,
                    task: None,
                    metadata,
                    reservation,
                    reservation_pending: true,
                    cancel_on_commit: false,
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
        self.commit_actor(
            child_id,
            reservation,
            initial_messages,
            shared_messages,
            backend,
        )
    }

    fn validate_manager(&self, current: &CurrentManagedTurn) -> Result<(), ManagerError> {
        if Arc::ptr_eq(&self.0, &current.token.manager.0)
            && current.token.generation == self.0.generation
        {
            Ok(())
        } else {
            Err(ManagerError::WrongManager)
        }
    }

    fn validate_active(&self, current: &CurrentManagedTurn) -> Result<(), ManagerError> {
        self.validate_manager(current)?;
        let graph = self.lock_graph();
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
        if node.lifecycle != GraphLifecycle::Live {
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
            if node.lifecycle != GraphLifecycle::Live {
                return Err(ManagerError::NonLiveAgent(cursor));
            }
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

    fn register_prompt_wait(
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
        let manager = Arc::downgrade(&self.0);
        let suspension = PermitSuspensionGuard {
            lease: Arc::clone(&lease.inner),
            watcher_id,
        };
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
            if outcome == Err(PromptWaitError::Timeout)
                && let Some(manager) = manager.upgrade()
            {
                let _ = AgentManagerHandle(manager).close_subtree(child_id);
            }
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
        Ok(ManagedPromptWait { inner: wait })
    }

    fn rollback_reservation(&self, agent_id: AgentId, reservation: u64) {
        let mut graph = self.lock_graph();
        let Some(node) = graph.nodes.get(&agent_id) else {
            return;
        };
        if node.reservation != reservation || node.actor.is_some() || !node.reservation_pending {
            return;
        }
        let parent_id = node.parent_id;
        graph.nodes.remove(&agent_id);
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

    fn commit_actor(
        &self,
        agent_id: AgentId,
        reservation: u64,
        initial_messages: Vec<Message>,
        shared_messages: Option<SharedMessages>,
        backend: Box<dyn ActorBackend>,
    ) -> Result<AgentRef, ManagerError> {
        let admission = ManagedTurnAdmission::new(Arc::downgrade(&self.0), agent_id);
        let (actor, task) = AgentActorHandle::spawn_managed(
            agent_id,
            initial_messages,
            shared_messages,
            backend,
            admission,
        );
        let manager = Arc::downgrade(&self.0);
        let task = smol::spawn(async move {
            task.await;
            let Some(manager) = manager.upgrade() else {
                return;
            };
            let mut graph = manager
                .graph
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Some(node) = graph.nodes.get_mut(&agent_id) else {
                return;
            };
            if node.reservation == reservation
                && !matches!(
                    node.lifecycle,
                    GraphLifecycle::Closed | GraphLifecycle::Removed
                )
            {
                node.lifecycle = GraphLifecycle::Closed;
                graph.revision += 1;
            }
        });
        let mut graph = self.lock_graph();
        let can_commit = graph.nodes.get(&agent_id).is_some_and(|node| {
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
        if !closing && !cancel_on_commit {
            node.reservation_pending = false;
            node.lifecycle = GraphLifecycle::Live;
            graph.revision += 1;
            let revision = graph.revision;
            let depth = graph.nodes[&agent_id].depth;
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
        let node = graph.nodes.get_mut(&agent_id).unwrap();
        node.reservation_pending = false;
        node.lifecycle = GraphLifecycle::Live;
        graph.revision += 1;
        let revision = graph.revision;
        let depth = graph.nodes[&agent_id].depth;
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
        })
    }

    fn finish_failed_commit(&self, agent_id: AgentId, reservation: u64) {
        let mut graph = self.lock_graph();
        if let Some(node) = graph.nodes.get_mut(&agent_id)
            && node.reservation == reservation
        {
            node.reservation_pending = false;
        }
    }

    pub fn runner_finished(&self, agent_id: AgentId) -> Result<bool, ManagerError> {
        let mut graph = self.lock_graph();
        Self::reclaim_finished_closings(&mut graph);
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        Ok(!node.reservation_pending && node.task.as_ref().is_none_or(smol::Task::is_finished))
    }

    pub fn actor(&self, agent_id: AgentId) -> Result<AgentActorHandle, ManagerError> {
        let graph = self.lock_graph();
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if node.lifecycle != GraphLifecycle::Live {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        node.actor
            .clone()
            .ok_or(ManagerError::NonLiveAgent(agent_id))
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

    pub fn close_subtree(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let actors = self.capture_subtree(agent_id, true)?;
        for actor in actors {
            actor.close();
        }
        Ok(())
    }

    pub fn close_descendants(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let actors = self.capture_subtree(agent_id, false)?;
        for actor in actors {
            actor.close();
        }
        Ok(())
    }

    pub fn close_descendants_for_turn(
        &self,
        agent_id: AgentId,
        turn_id: TurnId,
    ) -> Result<(), ManagerError> {
        let actors = {
            let mut graph = self.lock_graph();
            let actors = Self::capture_subtree_locked(&mut graph, agent_id, false)?;
            graph.active_turns.remove(&(agent_id, turn_id));
            actors
        };
        #[cfg(test)]
        self.wait_at_descendant_cut_gate();
        for actor in actors {
            actor.close();
        }
        Ok(())
    }

    pub async fn shutdown(&self, timeout: Duration) -> ShutdownReport {
        let (ids, actors) = {
            let mut graph = self.lock_graph();
            graph.shutting_down = true;
            self.0.shutdown.store(true, Ordering::Release);
            let ids: Vec<_> = graph
                .nodes
                .iter()
                .filter_map(|(&id, node)| {
                    (node.lifecycle.consumes_capacity()
                        || node.reservation_pending
                        || node.task.is_some())
                    .then_some(id)
                })
                .collect();
            for id in &ids {
                if let Some(node) = graph.nodes.get_mut(id) {
                    node.lifecycle = GraphLifecycle::Closing;
                }
            }
            let actors = ids
                .iter()
                .filter_map(|id| graph.nodes[id].actor.clone())
                .collect::<Vec<_>>();
            (ids, actors)
        };
        for actor in &actors {
            actor.shutdown();
        }
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let done = self.finished_nodes(&ids);
            if done.len() == ids.len() || std::time::Instant::now() >= deadline {
                let mut joined: Vec<_> = done.into_iter().collect();
                joined.sort_by_key(ToString::to_string);
                let mut timed_out: Vec<_> = ids
                    .iter()
                    .copied()
                    .filter(|id| !joined.contains(id))
                    .collect();
                timed_out.sort_by_key(ToString::to_string);
                if timed_out.is_empty() {
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
            if pending.is_empty() {
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
            if done {
                return;
            }
            listener.await;
        }
    }

    fn reclaim_finished_closings(graph: &mut GraphState) {
        let mut reclaimed = false;
        for node in graph.nodes.values_mut() {
            if node.lifecycle == GraphLifecycle::Closing
                && !node.reservation_pending
                && node.task.as_ref().is_some_and(smol::Task::is_finished)
            {
                node.lifecycle = GraphLifecycle::Closed;
                reclaimed = true;
            }
        }
        graph.revision += u64::from(reclaimed);
    }

    fn finished_nodes(&self, ids: &[AgentId]) -> HashSet<AgentId> {
        let mut graph = self.lock_graph();
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
            if let Some(node) = graph.nodes.get_mut(id) {
                node.lifecycle = GraphLifecycle::Closed;
            }
        }
        done
    }

    fn capture_cancel_cut(
        &self,
        agent_id: AgentId,
        subtree: bool,
    ) -> Result<Vec<AgentActorHandle>, ManagerError> {
        let mut graph = self.lock_graph();
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
        for id in ids {
            let Some(node) = graph.nodes.get_mut(&id) else {
                continue;
            };
            if node.lifecycle == GraphLifecycle::Reserved {
                node.cancel_on_commit = true;
            } else if let Some(actor) = &node.actor {
                actors.push(actor.clone());
            }
        }
        Ok(actors)
    }

    fn capture_subtree(
        &self,
        agent_id: AgentId,
        include_root: bool,
    ) -> Result<Vec<AgentActorHandle>, ManagerError> {
        let mut graph = self.lock_graph();
        Self::capture_subtree_locked(&mut graph, agent_id, include_root)
    }

    fn capture_subtree_locked(
        graph: &mut GraphState,
        agent_id: AgentId,
        include_root: bool,
    ) -> Result<Vec<AgentActorHandle>, ManagerError> {
        let node = graph
            .nodes
            .get(&agent_id)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if !node.lifecycle.consumes_capacity() {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        let mut ids = Self::subtree_ids(graph, agent_id);
        if !include_root {
            ids.remove(0);
        }
        for id in &ids {
            if let Some(node) = graph.nodes.get_mut(id) {
                node.lifecycle = GraphLifecycle::Closing;
            }
        }
        graph.revision += u64::from(!ids.is_empty());
        Ok(ids
            .into_iter()
            .filter_map(|id| graph.nodes.get(&id)?.actor.clone())
            .collect())
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
    fn set_prompt_admission_gate(
        &self,
        admitted: flume::Sender<TurnId>,
        release: flume::Receiver<()>,
    ) {
        *self
            .0
            .prompt_admission_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner()) =
            Some(PromptAdmissionGate { admitted, release });
    }

    #[cfg(test)]
    fn wait_at_prompt_admission_gate(&self, turn_id: TurnId) {
        let gate = self
            .0
            .prompt_admission_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(gate) = gate {
            gate.admitted.send(turn_id).unwrap();
            gate.release.recv().unwrap();
        }
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
        self.lease.close();
        let Some(manager) = self.manager.upgrade() else {
            return;
        };
        let mut graph = manager
            .graph
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if graph.active_turns.get(&(self.agent_id, self.turn_id)) == Some(&self.nonce) {
            graph.active_turns.remove(&(self.agent_id, self.turn_id));
        }
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
        self.lease.close();
        self.guard.take();
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
    let permit = cancel.race(inner.limiter.acquire_arc()).await?;
    if inner.shutdown.load(Ordering::Acquire) {
        return Err(crate::TurnCancellationReason::Shutdown);
    }
    let nonce = NEXT_TURN_NONCE.fetch_add(1, Ordering::Relaxed);
    {
        let mut graph = inner
            .graph
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if graph.shutting_down {
            return Err(crate::TurnCancellationReason::Shutdown);
        }
        graph.active_turns.insert((agent_id, turn_id), nonce);
    }
    let token = types::ManagedTurnToken {
        manager: AgentManagerHandle(Arc::clone(&inner)),
        generation: inner.generation,
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
        token,
        lease: TurnPermitLease {
            inner: Arc::clone(&lease),
        },
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
