use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use makima_config::ModelPolicy;
use makima_storage::session_state::{SessionStateView, StoredAgentLifecycle, StoredDeliveryOp};

use crate::actor::{
    ActorIdleGuard, AgentStateSnapshot, ChildDelivery, PreparedActorRewind, TurnResult,
};
use crate::config_change;
use crate::journal::SessionJournalSnapshot;
use crate::modes::ModeRegistry;
use crate::session_coordinator::ModelPreparer;
use crate::{ActorError, ActorLifecycle, AgentActorHandle, AgentId};

use super::{
    AgentManagerHandle, AgentMetadata, AgentResidency, GraphLifecycle, ManagerError, Node,
    ReactivationError,
};

pub struct ManagerRewindGuard {
    manager: AgentManagerHandle,
    affected: BTreeSet<AgentId>,
    guards: BTreeMap<AgentId, ActorIdleGuard>,
    actors: BTreeMap<AgentId, AgentActorHandle>,
}

impl AgentManagerHandle {
    pub async fn begin_rewind(&self, scope: AgentId) -> Result<ManagerRewindGuard, ManagerError> {
        let (affected, actors, discarded) = {
            let mut graph = self.lock_graph();
            if graph.shutting_down {
                return Err(ManagerError::GraphShutdown);
            }
            if graph.rewinding {
                return Err(ManagerError::RewindInProgress);
            }
            if !graph.nodes.contains_key(&scope) {
                return Err(ManagerError::UnknownAgent(scope));
            }
            if self.0.journal.agent(scope).is_none() {
                return Err(ManagerError::BusySubtree(scope));
            }
            let affected: BTreeSet<_> = Self::subtree_ids(&graph, scope).into_iter().collect();
            self.0.work.fence_rewind(&affected);
            graph.rewinding = true;
            graph.rewind_affected = affected.iter().copied().collect();
            let pending: Vec<_> = graph
                .nodes
                .iter()
                .filter_map(|(id, node)| {
                    (affected.contains(id) && node.reservation_pending).then_some(*id)
                })
                .collect();
            let mut discarded = Vec::new();
            for id in pending {
                let reservation = Self::next_reservation(&mut graph);
                let Some(node) = graph.nodes.get_mut(&id) else {
                    continue;
                };
                node.reservation = reservation;
                node.reservation_pending = false;
                node.detach_pending = false;
                node.residency = AgentResidency::Dormant;
                node.lifecycle = GraphLifecycle::Dormant;
                if let Some(attempt) = node.activation.take() {
                    attempt.complete(Err(ReactivationError::Cancelled.into()));
                }
                self.0.journal.set_incarnation(id, reservation);
                self.0.work.register(id, node.parent_id, reservation, false);
                discarded.push((node.actor.take(), node.task.take()));
                if self.0.journal.agent(id).is_none() {
                    let parent = node.parent_id;
                    graph.nodes.remove(&id);
                    let _ = self.0.work.remove(id);
                    if let Some(parent) = parent.and_then(|parent| graph.nodes.get_mut(&parent)) {
                        parent.children.retain(|child| *child != id);
                    }
                }
            }
            let actors = graph
                .nodes
                .iter()
                .filter_map(|(id, node)| {
                    node.actor
                        .as_ref()
                        .filter(|actor| actor.lifecycle() == ActorLifecycle::Open)
                        .map(|actor| (*id, actor.clone()))
                })
                .collect::<BTreeMap<_, _>>();
            graph.revision += 1;
            (affected, actors, discarded)
        };
        let mut guard = ManagerRewindGuard {
            manager: self.clone(),
            affected,
            actors,
            guards: BTreeMap::new(),
        };
        for (actor, task) in discarded {
            if let Some(actor) = actor {
                actor.shutdown();
            }
            if let Some(task) = task {
                self.retain_cleanup_task(task);
            }
        }
        for (id, actor) in &guard.actors {
            if guard.affected.contains(id) {
                self.0.journal.revoke_state_agent(*id);
                actor.cancel_existing();
            }
        }
        guard.actors = loop {
            let cleanup_finished = self.cleanup_finished();
            let actors = {
                let graph = self.lock_graph();
                if graph.shutting_down {
                    return Err(ManagerError::GraphShutdown);
                }
                if !cleanup_finished
                    || graph
                        .nodes
                        .values()
                        .any(|node| node.reservation_pending || node.detach_pending)
                {
                    None
                } else {
                    Some(
                        graph
                            .nodes
                            .iter()
                            .filter_map(|(id, node)| {
                                node.actor
                                    .as_ref()
                                    .filter(|actor| actor.lifecycle() == ActorLifecycle::Open)
                                    .map(|actor| (*id, actor.clone()))
                            })
                            .collect(),
                    )
                }
            };
            if let Some(actors) = actors {
                break actors;
            }
            smol::future::yield_now().await;
        };
        for (id, actor) in &guard.actors {
            let idle = loop {
                let drained = actor.inner.drained.listen();
                let changed = actor.inner.policy_changed.listen();
                match actor.prepare_idle() {
                    Ok(idle) => break idle,
                    Err(ActorError::Busy) => {
                        futures_lite::future::or(drained, changed).await;
                    }
                    Err(error) => return Err(ManagerError::Policy(error.to_string())),
                }
            };
            guard.guards.insert(*id, idle);
        }
        while !self.cleanup_finished() {
            smol::future::yield_now().await;
        }
        self.lock_graph().rewind_quiescent = true;
        Ok(guard)
    }
}

impl ManagerRewindGuard {
    pub fn affected_agents(&self) -> &BTreeSet<AgentId> {
        &self.affected
    }

    pub fn actor_guards(&self) -> &BTreeMap<AgentId, ActorIdleGuard> {
        &self.guards
    }

    pub fn actor_guard(&self, agent: AgentId) -> Option<&ActorIdleGuard> {
        self.guards.get(&agent)
    }

    pub fn take_actor_guard(&mut self, agent: AgentId) -> Option<ActorIdleGuard> {
        self.guards.remove(&agent)
    }

    pub fn resident_actors(&self) -> Vec<(AgentId, AgentActorHandle)> {
        self.actors
            .iter()
            .map(|(id, actor)| (*id, actor.clone()))
            .collect()
    }

    pub async fn prepare_actors(
        &mut self,
        snapshot: &SessionJournalSnapshot,
        policy: &ModelPolicy,
        preparer: &ModelPreparer,
        modes: &ModeRegistry,
    ) -> Result<Vec<PreparedActorRewind>, ManagerError> {
        let view = self.replacement_view(snapshot)?;
        let ids: Vec<_> = self
            .affected
            .iter()
            .copied()
            .filter(|id| {
                self.actors.contains_key(id)
                    && view
                        .agents
                        .get(id)
                        .is_some_and(|record| record.lifecycle == StoredAgentLifecycle::Open)
            })
            .collect();
        let mut prepared = Vec::with_capacity(ids.len());
        for id in ids {
            let record = &view.agents[&id];
            let scope = view.scopes.get(&Some(id));
            let saved = scope
                .and_then(|scope| scope.config.clone())
                .ok_or_else(|| {
                    ManagerError::InvalidGraph(format!(
                        "replacement agent {id} has no configuration"
                    ))
                })?;
            let config = config_change::prepare_restore(saved, policy, preparer, modes)
                .await
                .map_err(|error| ManagerError::Policy(error.to_string()))?;
            if let Some(warning) = &config.warning {
                tracing::warn!(agent_id = %id, %warning, "rewind configuration restored with fallback");
            }
            let mut results = Vec::new();
            let mut deliveries = Vec::new();
            let mut delivery_seen = Vec::new();
            if let Some(scope) = scope {
                for (turn, stored) in &scope.results {
                    let result = TurnResult::from_stored(stored.clone(), |reference| {
                        self.manager.resolve_result_payload(reference)
                    })
                    .map_err(|_| ManagerError::TurnResultUnavailable {
                        agent_id: id,
                        turn_id: *turn,
                    })?;
                    results.push(result);
                }
                for ((source, turn), op) in &scope.deliveries {
                    match op {
                        StoredDeliveryOp::Queued(delivery) => {
                            let result =
                                TurnResult::from_stored(delivery.result.clone(), |reference| {
                                    self.manager.resolve_result_payload(reference)
                                })
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
            let state = AgentStateSnapshot {
                results,
                deliveries,
                delivery_seen,
                usage: AgentManagerHandle::restored_usage(snapshot, record),
            };
            let history = snapshot.histories.get(&id).cloned().ok_or_else(|| {
                ManagerError::InvalidGraph(format!("replacement agent {id} has no history"))
            })?;
            let idle = self
                .take_actor_guard(id)
                .ok_or(ManagerError::BusySubtree(id))?;
            let actor = idle
                .prepare_rewind(history, config.saved, config.model, config.mode)
                .await
                .map_err(|error| ManagerError::Policy(error.to_string()))?
                .with_state(state)
                .map_err(|error| ManagerError::Policy(error.to_string()))?;
            prepared.push(actor);
        }
        Ok(prepared)
    }

    pub fn validate_replacement(
        &self,
        snapshot: &SessionJournalSnapshot,
    ) -> Result<(), ManagerError> {
        self.replacement_view(snapshot).map(|_| ())
    }

    fn replacement_view(
        &self,
        snapshot: &SessionJournalSnapshot,
    ) -> Result<SessionStateView, ManagerError> {
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .map_err(|error| ManagerError::InvalidGraph(error.to_string()))?;
        let graph = self.manager.lock_graph();
        if graph.shutting_down {
            return Err(ManagerError::GraphShutdown);
        }
        if !graph.rewinding {
            return Err(ManagerError::RewindInProgress);
        }
        if view.scopes.values().any(|scope| !scope.admitted.is_empty()) {
            return Err(ManagerError::InvalidGraph(
                "replacement has unterminated admissions".into(),
            ));
        }
        let roots: Vec<_> = view
            .agents
            .values()
            .filter(|record| record.parent.is_none())
            .collect();
        if roots.len() != 1 || Some(roots[0].id) != graph.root_id || roots[0].depth != 0 {
            return Err(ManagerError::InvalidGraph(
                "replacement must preserve the session root".into(),
            ));
        }
        for record in view.agents.values() {
            if !snapshot.histories.contains_key(&record.id) {
                return Err(ManagerError::InvalidGraph(
                    "replacement agent has no history".into(),
                ));
            }
            if let Some(node) = graph.nodes.get(&record.id)
                && node.parent_id != record.parent
            {
                return Err(ManagerError::InvalidGraph(
                    "replacement changes an agent parent".into(),
                ));
            }
            let mut cursor = record;
            let mut ancestry = HashSet::new();
            while let Some(parent) = cursor.parent {
                if !ancestry.insert(cursor.id) {
                    return Err(ManagerError::InvalidGraph(
                        "replacement ancestry contains a cycle".into(),
                    ));
                }
                let parent = view.agents.get(&parent).ok_or_else(|| {
                    ManagerError::InvalidGraph("replacement agent parent is missing".into())
                })?;
                if parent.depth.checked_add(1) != Some(cursor.depth) {
                    return Err(ManagerError::InvalidGraph(
                        "replacement agent depth does not match its parent".into(),
                    ));
                }
                cursor = parent;
            }
            if cursor.id != roots[0].id {
                return Err(ManagerError::InvalidGraph(
                    "replacement agent has a different root".into(),
                ));
            }
        }
        for (scope, state) in &view.scopes {
            for ((source, _), delivery) in &state.deliveries {
                if matches!(delivery, StoredDeliveryOp::Queued(_))
                    && (scope.is_none()
                        || view
                            .agents
                            .get(source)
                            .is_none_or(|record| record.parent != *scope))
                {
                    return Err(ManagerError::InvalidGraph(
                        "replacement delivery recipient is not its source parent".into(),
                    ));
                }
            }
        }
        Ok(view)
    }

    pub fn reconcile(&mut self, snapshot: &SessionJournalSnapshot) -> Result<(), ManagerError> {
        let view = self.replacement_view(snapshot)?;
        let mut graph = self.manager.lock_graph();
        let root_id = graph.root_id.ok_or(ManagerError::MissingRoot)?;
        let mut old = std::mem::take(&mut graph.nodes);
        let mut nodes = HashMap::new();
        let mut discarded = Vec::new();
        for record in view.agents.values() {
            let existing = old.contains_key(&record.id);
            let mut node = old.remove(&record.id).unwrap_or_else(|| Node {
                parent_id: record.parent,
                root_id,
                depth: record.depth as usize,
                children: Vec::new(),
                lifecycle: GraphLifecycle::Dormant,
                residency: AgentResidency::Dormant,
                activation: None,
                actor: None,
                task: None,
                metadata: AgentMetadata::default(),
                reservation: AgentManagerHandle::next_reservation(&mut graph),
                reservation_pending: false,
                detach_pending: false,
                cancel_on_commit: false,
                turn_owner: None,
                terminal: Arc::new(Mutex::new(None)),
            });
            let resident = record.lifecycle == StoredAgentLifecycle::Open
                && node
                    .actor
                    .as_ref()
                    .is_some_and(|actor| actor.lifecycle() == ActorLifecycle::Open)
                && self.actors.contains_key(&record.id);
            if !resident {
                let discarded_resident = node.actor.is_some() || node.task.is_some();
                if discarded_resident {
                    discarded.push((node.actor.take(), node.task.take()));
                }
                if !existing || self.affected.contains(&record.id) {
                    node.reservation = AgentManagerHandle::next_reservation(&mut graph);
                }
            }
            node.parent_id = record.parent;
            node.root_id = root_id;
            node.depth = record.depth as usize;
            node.children.clear();
            node.lifecycle = match record.lifecycle {
                StoredAgentLifecycle::Open if resident => GraphLifecycle::Live,
                StoredAgentLifecycle::Open => GraphLifecycle::Dormant,
                StoredAgentLifecycle::Closed => GraphLifecycle::Closed,
                StoredAgentLifecycle::Retired => GraphLifecycle::Retired,
            };
            node.residency = if resident {
                AgentResidency::Resident
            } else {
                AgentResidency::Dormant
            };
            node.metadata = AgentMetadata {
                label: record.label.clone(),
                spawned_by_tool_use_id: record.spawned_by_tool_use_id.clone(),
                transcript_key: record.parent.map(|_| record.transcript_key.clone()),
            };
            node.reservation_pending = false;
            node.detach_pending = false;
            node.cancel_on_commit = false;
            node.turn_owner = None;
            *node
                .terminal
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = None;
            self.manager
                .0
                .journal
                .set_incarnation(record.id, node.reservation);
            self.manager
                .0
                .work
                .register(record.id, record.parent, node.reservation, resident);
            self.manager.0.work.set_reporting(
                record.id,
                record
                    .spec
                    .as_ref()
                    .is_none_or(|spec| spec.auto_deliver && !spec.silent),
            )?;
            nodes.insert(record.id, node);
        }
        for record in view.agents.values() {
            if let Some(parent) = record.parent {
                nodes
                    .get_mut(&parent)
                    .ok_or(ManagerError::UnknownAgent(parent))?
                    .children
                    .push(record.id);
            }
        }
        for (id, mut node) in old {
            node.lifecycle = GraphLifecycle::Removed;
            *node
                .terminal
                .lock()
                .unwrap_or_else(|error| error.into_inner()) =
                Some(AgentManagerHandle::capture_node_snapshot(id, &node).snapshot());
            let _ = self.manager.0.work.remove(id);
            let reservation = AgentManagerHandle::next_reservation(&mut graph);
            self.manager.0.journal.set_incarnation(id, reservation);
            discarded.push((node.actor.take(), node.task.take()));
        }
        graph.nodes = nodes;
        graph.active_turns.clear();
        graph.revision += 1;
        self.manager
            .0
            .restored_delivery_work
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        for (scope, state) in &view.scopes {
            let Some(parent) = scope else {
                continue;
            };
            for ((source, turn), delivery) in &state.deliveries {
                if matches!(delivery, StoredDeliveryOp::Queued(_)) {
                    let mut hold = self.manager.0.work.restore_hold(*source)?;
                    let work = hold.take_report().ok_or_else(|| {
                        ManagerError::InvalidGraph(
                            "replacement delivery has no recipient reservation".into(),
                        )
                    })?;
                    self.manager
                        .0
                        .restored_delivery_work
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .insert((*parent, *source, *turn), vec![work]);
                }
            }
        }
        let actors: Vec<_> = graph
            .nodes
            .iter()
            .filter_map(|(id, node)| node.actor.clone().map(|actor| (*id, actor)))
            .collect();
        drop(graph);
        for (id, actor) in actors {
            actor.reconcile_rewind_delivery_work(|source, turn| {
                self.manager.take_delivery_work(id, source, turn)
            });
            self.manager
                .0
                .restored_delivery_work
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|(parent, _, _), _| *parent != id);
        }
        for (actor, task) in discarded {
            if let Some(actor) = actor {
                actor.shutdown();
            }
            if let Some(task) = task {
                self.manager.retain_cleanup_task(task);
            }
        }
        Ok(())
    }
}

impl Drop for ManagerRewindGuard {
    fn drop(&mut self) {
        let mut graph = self.manager.lock_graph();
        self.guards.clear();
        self.manager.0.work.release_rewind();
        graph.rewinding = false;
        graph.rewind_affected.clear();
        graph.rewind_quiescent = false;
        graph.revision += 1;
        self.manager.0.reaped.notify(usize::MAX);
    }
}
