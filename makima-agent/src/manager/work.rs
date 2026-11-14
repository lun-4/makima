use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use crate::{AgentId, cancel::PublicationGuard};
use makima_storage::checkpoint::CheckpointPublication;

use super::{AgentLimits, ManagerError};

#[derive(Debug)]
pub(crate) struct WorkLedger {
    limits: AgentLimits,
    state: Mutex<WorkState>,
}

#[derive(Debug, Default)]
struct WorkState {
    agents: HashMap<AgentId, WorkAgent>,
    rewind_fence: bool,
    rewind_reporting: HashMap<AgentId, u64>,
    active_agents: usize,
    active_children: HashMap<AgentId, usize>,
}

#[derive(Debug)]
struct WorkAgent {
    parent_id: Option<AgentId>,
    incarnation: u64,
    open: bool,
    frozen: bool,
    reporting: bool,
    removed: bool,
    references: usize,
    revision: u64,
    publications: Vec<Weak<PublicationGuard>>,
}

#[derive(Clone, Debug)]
pub(crate) struct WorkAdmission {
    ledger: Arc<WorkLedger>,
    agent_id: AgentId,
    incarnation: u64,
}

/// Clones share admitted work. They cannot activate an unreserved recipient.
#[derive(Clone, Debug)]
pub(crate) struct WorkReservation {
    work: Arc<WorkHold>,
    report: Option<Box<WorkReservation>>,
    sender: Option<Arc<WorkHold>>,
}

#[derive(Debug)]
pub(crate) struct WorkIdleGuard {
    ledger: Arc<WorkLedger>,
    agent_id: AgentId,
    revision: u64,
    was_open: bool,
    committed: bool,
}

#[derive(Debug)]
struct WorkHold {
    ledger: Arc<WorkLedger>,
    agent_id: AgentId,
}

impl WorkLedger {
    pub(crate) fn new(limits: AgentLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(WorkState::default()),
        })
    }

    /// Parent links are immutable; rebinding an actor preserves outstanding work.
    pub(crate) fn register(
        self: &Arc<Self>,
        agent_id: AgentId,
        parent_id: Option<AgentId>,
        incarnation: u64,
        open: bool,
    ) -> WorkAdmission {
        let mut state = self.lock();
        let agent = state.agents.entry(agent_id).or_insert(WorkAgent {
            parent_id,
            incarnation,
            open,
            frozen: false,
            reporting: true,
            removed: false,
            references: 0,
            revision: 0,
            publications: Vec::new(),
        });
        PublicationGuard::cancel_all(&mut agent.publications);
        agent.revision += 1;
        agent.incarnation = incarnation;
        agent.open = open;
        agent.frozen = false;
        agent.removed = false;
        WorkAdmission {
            ledger: Arc::clone(self),
            agent_id,
            incarnation,
        }
    }

    pub(crate) fn fence_rewind(&self, agents: &BTreeSet<AgentId>) {
        let mut state = self.lock();
        state.rewind_fence = true;
        state.rewind_reporting = state
            .agents
            .iter()
            .filter_map(|(id, agent)| {
                (!agents.contains(id) && !agent.open && !agent.removed && !agent.frozen)
                    .then_some((*id, agent.incarnation))
            })
            .collect();
        for id in agents {
            if let Some(agent) = state.agents.get_mut(id) {
                PublicationGuard::cancel_all(&mut agent.publications);
            }
        }
    }

    pub(crate) fn release_rewind(&self) {
        let mut state = self.lock();
        state.rewind_fence = false;
        state.rewind_reporting.clear();
    }

    pub(crate) fn set_open(&self, agent_id: AgentId, open: bool) -> Result<(), ManagerError> {
        let mut state = self.lock();
        let agent = state
            .agents
            .get_mut(&agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if !open {
            PublicationGuard::cancel_all(&mut agent.publications);
        }
        agent.open = open;
        agent.frozen = false;
        agent.revision += 1;
        Ok(())
    }

    pub(crate) fn set_open_for_incarnation(
        &self,
        agent_id: AgentId,
        incarnation: u64,
        open: bool,
    ) -> Result<(), ManagerError> {
        let mut state = self.lock();
        let agent = state
            .agents
            .get_mut(&agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if agent.incarnation != incarnation {
            return Err(ManagerError::StaleIncarnation(agent_id));
        }
        if !open {
            PublicationGuard::cancel_all(&mut agent.publications);
        }
        agent.open = open;
        agent.frozen = false;
        agent.revision += 1;
        Ok(())
    }

    pub(crate) fn is_busy(&self, agent_id: AgentId) -> bool {
        self.lock()
            .agents
            .get(&agent_id)
            .is_some_and(|agent| agent.references != 0)
    }

    pub(crate) fn freeze_if_idle(
        self: &Arc<Self>,
        agent_id: AgentId,
    ) -> Result<WorkIdleGuard, ManagerError> {
        let mut state = self.lock();
        let agent = state
            .agents
            .get_mut(&agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        if agent.references != 0 {
            return Err(ManagerError::BusySubtree(agent_id));
        }
        if agent.frozen {
            return Err(ManagerError::NonLiveAgent(agent_id));
        }
        PublicationGuard::cancel_all(&mut agent.publications);
        let was_open = agent.open;
        agent.open = false;
        agent.frozen = true;
        agent.revision += 1;
        Ok(WorkIdleGuard {
            ledger: Arc::clone(self),
            agent_id,
            revision: agent.revision,
            was_open,
            committed: false,
        })
    }

    pub(crate) fn bind_incarnation(
        self: &Arc<Self>,
        agent_id: AgentId,
        incarnation: u64,
    ) -> Result<WorkAdmission, ManagerError> {
        let mut state = self.lock();
        let agent = state
            .agents
            .get_mut(&agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        PublicationGuard::cancel_all(&mut agent.publications);
        agent.incarnation = incarnation;
        agent.revision += 1;
        Ok(WorkAdmission {
            ledger: Arc::clone(self),
            agent_id,
            incarnation,
        })
    }

    pub(crate) fn set_reporting(
        &self,
        agent_id: AgentId,
        reporting: bool,
    ) -> Result<(), ManagerError> {
        let mut state = self.lock();
        let agent = state
            .agents
            .get_mut(&agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        agent.reporting = reporting;
        Ok(())
    }

    pub(crate) fn remove(&self, agent_id: AgentId) -> Result<(), ManagerError> {
        let mut state = self.lock();
        let agent = state
            .agents
            .get_mut(&agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(agent_id))?;
        PublicationGuard::cancel_all(&mut agent.publications);
        agent.open = false;
        agent.removed = true;
        agent.revision += 1;
        if agent.references == 0 {
            state.agents.remove(&agent_id);
        }
        Ok(())
    }

    /// Only restart seeding may bypass limits and closed ordinary admission.
    pub(crate) fn restore_hold(
        self: &Arc<Self>,
        agent_id: AgentId,
    ) -> Result<WorkReservation, ManagerError> {
        self.reserve(agent_id, None, true, true)
    }

    pub(crate) fn active_agents(&self) -> usize {
        self.lock().active_agents
    }

    pub(crate) fn active_children(&self, parent_id: AgentId) -> usize {
        self.lock()
            .active_children
            .get(&parent_id)
            .copied()
            .unwrap_or_default()
    }

    fn reserve(
        self: &Arc<Self>,
        agent_id: AgentId,
        incarnation: Option<u64>,
        reporting: bool,
        restored: bool,
    ) -> Result<WorkReservation, ManagerError> {
        let mut state = self.lock();
        let mut chain = Vec::new();
        let mut next = Some(agent_id);
        while let Some(id) = next {
            if !restored && state.rewind_fence {
                return Err(ManagerError::RewindInProgress);
            }
            let agent = state
                .agents
                .get(&id)
                .filter(|agent| !agent.removed)
                .ok_or(ManagerError::UnknownAgent(id))?;
            if id == agent_id
                && incarnation.is_some_and(|incarnation| incarnation != agent.incarnation)
            {
                return Err(ManagerError::StaleIncarnation(id));
            }
            if !restored && (agent.frozen || (id == agent_id && !agent.open)) {
                return Err(ManagerError::NonLiveAgent(id));
            }
            chain.push(id);
            next = if (id == agent_id && reporting) || (id != agent_id && agent.reporting) {
                agent.parent_id
            } else {
                None
            };
        }

        let mut additional_agents = 0;
        let mut additional_children = HashMap::<AgentId, usize>::new();
        for id in &chain {
            let agent = &state.agents[id];
            if agent.references == 0 {
                additional_agents += 1;
                if let Some(parent_id) = agent.parent_id {
                    *additional_children.entry(parent_id).or_default() += 1;
                }
            }
        }
        if !restored {
            if additional_agents > 0
                && state.active_agents + additional_agents > self.limits.max_active_agents
            {
                return Err(ManagerError::ActiveAgentLimit {
                    max: self.limits.max_active_agents,
                });
            }
            for (parent_id, additional) in &additional_children {
                if state
                    .active_children
                    .get(parent_id)
                    .copied()
                    .unwrap_or_default()
                    + additional
                    > self.limits.max_children_per_agent
                {
                    return Err(ManagerError::ChildLimit {
                        parent_id: *parent_id,
                        max: self.limits.max_children_per_agent,
                    });
                }
            }
        }

        state.active_agents += additional_agents;
        for (parent_id, additional) in additional_children {
            *state.active_children.entry(parent_id).or_default() += additional;
        }
        let mut reservation = None;
        for id in chain.into_iter().rev() {
            state
                .agents
                .get_mut(&id)
                .expect("validated work agent")
                .references += 1;
            reservation = Some(Box::new(WorkReservation {
                work: Arc::new(WorkHold {
                    ledger: Arc::clone(self),
                    agent_id: id,
                }),
                report: reservation,
                sender: None,
            }));
        }
        Ok(*reservation.expect("work always reserves its source"))
    }

    fn release(&self, agent_id: AgentId) {
        let mut state = self.lock();
        let Some(agent) = state.agents.get_mut(&agent_id) else {
            return;
        };
        agent.references -= 1;
        if agent.references != 0 {
            return;
        }
        let parent_id = agent.parent_id;
        let removed = agent.removed;
        state.active_agents -= 1;
        if let Some(parent_id) = parent_id
            && let Some(children) = state.active_children.get_mut(&parent_id)
        {
            *children -= 1;
            if *children == 0 {
                state.active_children.remove(&parent_id);
            }
        }
        if removed {
            state.agents.remove(&agent_id);
        }
    }

    fn lock(&self) -> MutexGuard<'_, WorkState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

impl WorkIdleGuard {
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for WorkIdleGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = self.ledger.lock();
        if let Some(agent) = state.agents.get_mut(&self.agent_id)
            && agent.revision == self.revision
            && !agent.removed
        {
            agent.open = self.was_open;
            agent.frozen = false;
            agent.revision += 1;
        }
    }
}

impl WorkAdmission {
    pub(crate) fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> Result<Arc<PublicationGuard>, ManagerError> {
        let mut state = self.ledger.lock();
        if state.rewind_fence {
            return Err(ManagerError::RewindInProgress);
        }
        let agent = state
            .agents
            .get_mut(&self.agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(self.agent_id))?;
        if agent.incarnation != self.incarnation {
            return Err(ManagerError::StaleIncarnation(self.agent_id));
        }
        if !agent.open || agent.frozen {
            return Err(ManagerError::NonLiveAgent(self.agent_id));
        }
        Ok(PublicationGuard::register(
            publication,
            &mut agent.publications,
        ))
    }

    pub(crate) fn validate_open(&self) -> Result<(), ManagerError> {
        let state = self.ledger.lock();
        if state.rewind_fence {
            return Err(ManagerError::RewindInProgress);
        }
        let agent = state
            .agents
            .get(&self.agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(self.agent_id))?;
        if agent.incarnation != self.incarnation {
            return Err(ManagerError::StaleIncarnation(self.agent_id));
        }
        if !agent.open || agent.frozen {
            return Err(ManagerError::NonLiveAgent(self.agent_id));
        }
        Ok(())
    }

    pub(crate) fn set_open(&self, open: bool) -> Result<(), ManagerError> {
        self.ledger
            .set_open_for_incarnation(self.agent_id, self.incarnation, open)
    }

    pub(crate) fn set_reporting(&self, reporting: bool) -> Result<(), ManagerError> {
        let mut state = self.ledger.lock();
        let fenced = state.rewind_fence;
        let initializing = state.rewind_reporting.get(&self.agent_id) == Some(&self.incarnation);
        if fenced && !initializing {
            return Err(ManagerError::RewindInProgress);
        }
        let agent = state
            .agents
            .get_mut(&self.agent_id)
            .filter(|agent| !agent.removed)
            .ok_or(ManagerError::UnknownAgent(self.agent_id))?;
        if agent.incarnation != self.incarnation {
            return Err(ManagerError::StaleIncarnation(self.agent_id));
        }
        if fenced && (agent.open || agent.frozen) {
            return Err(ManagerError::RewindInProgress);
        }
        agent.reporting = reporting;
        Ok(())
    }

    pub(crate) fn agent_id(&self) -> AgentId {
        self.agent_id
    }

    pub(crate) fn try_reserve(&self) -> Result<WorkReservation, ManagerError> {
        self.try_reserve_reporting(false)
    }

    pub(crate) fn try_reserve_reporting(
        &self,
        reporting: bool,
    ) -> Result<WorkReservation, ManagerError> {
        self.ledger
            .reserve(self.agent_id, Some(self.incarnation), reporting, false)
    }
}

impl WorkReservation {
    pub(crate) fn agent_id(&self) -> AgentId {
        self.work.agent_id
    }

    pub(crate) fn hold_only(&self) -> Self {
        Self {
            work: Arc::clone(&self.work),
            report: None,
            sender: self.sender.clone(),
        }
    }

    /// Moves the reserved recipient chain and holds the sender through delivery.
    pub(crate) fn take_report(&mut self) -> Option<Self> {
        self.report.take().map(|report| {
            let mut report = *report;
            report.sender = Some(Arc::clone(&self.work));
            report
        })
    }
}

impl Drop for WorkHold {
    fn drop(&mut self) {
        self.ledger.release(self.agent_id);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{Arc, Barrier, mpsc},
        thread,
    };

    use test_case::test_case;

    use super::{WorkAdmission, WorkLedger};
    use crate::{
        AgentId,
        manager::{AgentLimits, ManagerError},
    };
    use makima_storage::checkpoint::CheckpointPublication;

    #[test]
    fn rewind_fence_blocks_admission_across_rebinding_until_release() {
        let ledger = ledger(SATURATED_ACTIVE_LIMIT, SATURATED_CHILD_LIMIT);
        let root = AgentId::generate();
        let child = AgentId::generate();
        let outside = ledger.register(root, None, INCARNATION, true);
        let inside = ledger.register(child, Some(root), INCARNATION, true);
        let pending = CheckpointPublication::default();
        let _registration = inside.register_publication(&pending).unwrap();
        ledger.fence_rewind(&BTreeSet::from([child]));
        assert!(pending.is_cancelled());
        assert!(matches!(
            outside.try_reserve(),
            Err(ManagerError::RewindInProgress)
        ));
        assert!(matches!(
            inside.try_reserve(),
            Err(ManagerError::RewindInProgress)
        ));
        let rebound = ledger.register(child, Some(root), NEXT_INCARNATION, true);
        assert!(matches!(
            rebound.validate_open(),
            Err(ManagerError::RewindInProgress)
        ));
        assert!(matches!(
            rebound.register_publication(&CheckpointPublication::default()),
            Err(ManagerError::RewindInProgress)
        ));
        let hold = ledger.restore_hold(child).unwrap();
        assert_eq!(ledger.active_agents(), 2);
        drop(hold);
        assert_eq!(ledger.active_agents(), 0);
        ledger.release_rewind();
        assert!(outside.try_reserve().is_ok());
        assert!(rebound.try_reserve().is_ok());
        assert!(
            matches!(inside.try_reserve(), Err(ManagerError::StaleIncarnation(id)) if id == child)
        );
    }

    #[test]
    fn rewind_reporting_exemption_only_configures_preexisting_outside_initializer() {
        let ledger = ledger(SATURATED_ACTIVE_LIMIT, SATURATED_CHILD_LIMIT);
        let root = AgentId::generate();
        let child = AgentId::generate();
        let outside = ledger.register(root, None, INCARNATION, false);
        let inside = ledger.register(child, Some(root), INCARNATION, false);
        ledger.fence_rewind(&BTreeSet::from([child]));
        outside.set_reporting(false).unwrap();
        assert!(matches!(
            inside.set_reporting(false),
            Err(ManagerError::RewindInProgress)
        ));
        assert!(matches!(
            outside.try_reserve(),
            Err(ManagerError::RewindInProgress)
        ));
        assert!(matches!(
            outside.register_publication(&CheckpointPublication::default()),
            Err(ManagerError::RewindInProgress)
        ));
        outside.set_open(true).unwrap();
        assert!(matches!(
            outside.set_reporting(true),
            Err(ManagerError::RewindInProgress)
        ));
        outside.set_open(false).unwrap();
        let rebound = ledger.bind_incarnation(root, NEXT_INCARNATION).unwrap();
        assert!(matches!(
            rebound.set_reporting(true),
            Err(ManagerError::RewindInProgress)
        ));
        let fresh = ledger.register(AgentId::generate(), None, INCARNATION, false);
        assert!(matches!(
            fresh.set_reporting(true),
            Err(ManagerError::RewindInProgress)
        ));
        ledger.release_rewind();
        rebound.set_reporting(true).unwrap();
    }

    #[test_case(false; "pending")]
    #[test_case(true; "claimed")]
    fn incarnation_replacement_fences_registered_publications(claimed: bool) {
        let ledger = ledger(SATURATED_ACTIVE_LIMIT, SATURATED_CHILD_LIMIT);
        let agent_id = AgentId::generate();
        let original = ledger.register(agent_id, None, INCARNATION, true);
        let publication = CheckpointPublication::default();
        let _registration = original.register_publication(&publication).unwrap();
        if claimed {
            assert!(publication.try_publish());
        }
        let current = ledger.bind_incarnation(agent_id, NEXT_INCARNATION).unwrap();
        assert_eq!(publication.is_cancelled(), !claimed);
        let stale = CheckpointPublication::default();
        assert!(original.register_publication(&stale).is_err());
        let next = CheckpointPublication::default();
        let registration = current.register_publication(&next).unwrap();
        assert!(!next.is_cancelled());
        drop(registration);
        assert!(next.is_cancelled());
    }

    const INCARNATION: u64 = 1;
    const NEXT_INCARNATION: u64 = INCARNATION + 1;
    const SATURATED_ACTIVE_LIMIT: usize = 64;
    const SATURATED_CHILD_LIMIT: usize = SATURATED_ACTIVE_LIMIT - 1;
    const SEQUENTIAL_CHILDREN: usize = 33;

    fn ledger(active: usize, children: usize) -> Arc<WorkLedger> {
        WorkLedger::new(AgentLimits {
            max_active_agents: active,
            max_children_per_agent: children,
            ..AgentLimits::default()
        })
    }

    fn register(ledger: &Arc<WorkLedger>, parent: Option<AgentId>) -> WorkAdmission {
        ledger.register(AgentId::generate(), parent, INCARNATION, true)
    }

    #[test_case(false; "global_last_slot")]
    #[test_case(true; "last_child_slot")]
    fn concurrent_admission_cannot_overbook_last_slot(child_limit: bool) {
        let ledger = ledger(if child_limit { 3 } else { 2 }, 1);
        let root = register(&ledger, None);
        let root_hold = root.try_reserve().unwrap();
        let first = register(&ledger, Some(root.agent_id()));
        let second = register(&ledger, Some(root.agent_id()));
        let start = Arc::new(Barrier::new(3));
        let release = Arc::new(Barrier::new(3));
        let (sender, receiver) = mpsc::channel();
        thread::scope(|scope| {
            for admission in [first, second] {
                let start = Arc::clone(&start);
                let release = Arc::clone(&release);
                let sender = sender.clone();
                scope.spawn(move || {
                    start.wait();
                    let reservation = admission.try_reserve();
                    sender.send(reservation.as_ref().err().cloned()).unwrap();
                    release.wait();
                    drop(reservation);
                });
            }
            start.wait();
            let outcomes = [receiver.recv().unwrap(), receiver.recv().unwrap()];
            let expected = if child_limit {
                ManagerError::ChildLimit {
                    parent_id: root.agent_id(),
                    max: 1,
                }
            } else {
                ManagerError::ActiveAgentLimit { max: 2 }
            };
            let active_agents = ledger.active_agents();
            let active_children = ledger.active_children(root.agent_id());
            release.wait();
            assert_eq!(outcomes.iter().filter(|result| result.is_none()).count(), 1);
            assert_eq!(
                outcomes.into_iter().flatten().collect::<Vec<_>>(),
                [expected]
            );
            assert_eq!(active_agents, 2);
            assert_eq!(active_children, 1);
        });
        drop(root_hold);
        assert_eq!(ledger.active_agents(), 0);
        assert_eq!(ledger.active_children(root.agent_id()), 0);
    }

    #[test]
    fn same_agent_queue_and_clones_hold_one_slot_until_last_reference() {
        let ledger = ledger(1, 1);
        let agent = register(&ledger, None);
        let first = agent.try_reserve().unwrap();
        let second = agent.clone().try_reserve().unwrap();
        let clone = first.clone();
        let waiting = register(&ledger, None);
        let expected = ManagerError::ActiveAgentLimit { max: 1 };
        drop(first);
        drop(second);
        assert_eq!(waiting.try_reserve().unwrap_err(), expected);
        assert_eq!(ledger.active_agents(), 1);
        drop(clone);
        let next = waiting.try_reserve().unwrap();
        assert_eq!(ledger.active_agents(), 1);
        drop(next);
        assert_eq!(ledger.active_agents(), 0);
    }

    #[test]
    fn suspended_root_and_63_children_exhaust_capacity() {
        let ledger = ledger(SATURATED_ACTIVE_LIMIT, SATURATED_CHILD_LIMIT);
        let root = register(&ledger, None);
        let suspended_root = root.try_reserve().unwrap();
        let children: Vec<_> = (0..SATURATED_CHILD_LIMIT)
            .map(|_| {
                register(&ledger, Some(root.agent_id()))
                    .try_reserve_reporting(true)
                    .unwrap()
            })
            .collect();
        let extra = register(&ledger, Some(root.agent_id()));
        assert_eq!(ledger.active_agents(), SATURATED_ACTIVE_LIMIT);
        assert_eq!(
            ledger.active_children(root.agent_id()),
            SATURATED_CHILD_LIMIT
        );
        assert_eq!(
            extra.try_reserve().unwrap_err(),
            ManagerError::ActiveAgentLimit {
                max: SATURATED_ACTIVE_LIMIT,
            }
        );
        drop(suspended_root);
        assert_eq!(ledger.active_agents(), SATURATED_ACTIVE_LIMIT);
        drop(children);
        assert_eq!(ledger.active_agents(), 0);
        assert_eq!(ledger.active_children(root.agent_id()), 0);
    }

    #[test]
    fn sequential_distinct_children_do_not_consume_idle_retention_capacity() {
        let ledger = ledger(2, 1);
        let root = register(&ledger, None);
        let root_hold = root.try_reserve().unwrap();
        let children: Vec<_> = (0..SEQUENTIAL_CHILDREN)
            .map(|_| register(&ledger, Some(root.agent_id())))
            .collect();
        for child in &children {
            let reservation = child.try_reserve_reporting(true).unwrap();
            assert_eq!(ledger.active_agents(), 2);
            assert_eq!(ledger.active_children(root.agent_id()), 1);
            drop(reservation);
            assert_eq!(ledger.active_agents(), 1);
        }
        assert_eq!(ledger.lock().agents.len(), SEQUENTIAL_CHILDREN + 1);
        drop(root_hold);
        assert_eq!(ledger.active_agents(), 0);
    }

    #[test_case(false; "active_limit")]
    #[test_case(true; "child_limit")]
    fn failed_reporting_reservation_is_atomic(child_limit: bool) {
        let ledger = ledger(if child_limit { 4 } else { 2 }, 1);
        let root = register(&ledger, None);
        let root_hold = root.try_reserve().unwrap();
        let sibling = register(&ledger, Some(root.agent_id()));
        let sibling_hold = sibling.try_reserve().unwrap();
        let parent = register(&ledger, Some(root.agent_id()));
        let child = register(&ledger, Some(parent.agent_id()));
        let expected = if child_limit {
            ManagerError::ChildLimit {
                parent_id: root.agent_id(),
                max: 1,
            }
        } else {
            ManagerError::ActiveAgentLimit { max: 2 }
        };
        assert_eq!(child.try_reserve_reporting(true).unwrap_err(), expected);
        assert_eq!(ledger.active_agents(), 2);
        assert_eq!(ledger.active_children(root.agent_id()), 1);
        assert_eq!(ledger.active_children(parent.agent_id()), 0);
        drop(sibling_hold);
        if child_limit {
            let work = child.try_reserve_reporting(true).unwrap();
            assert_eq!(ledger.active_agents(), 3);
            drop(work);
        }
        drop(root_hold);
        assert_eq!(ledger.active_agents(), 0);
    }

    #[test]
    fn stale_handle_and_closed_root_cannot_admit_new_work() {
        let ledger = ledger(1, 1);
        let root = register(&ledger, None);
        let admitted = root.try_reserve().unwrap();
        let rebound = ledger
            .bind_incarnation(root.agent_id(), NEXT_INCARNATION)
            .unwrap();
        assert_eq!(
            root.try_reserve().unwrap_err(),
            ManagerError::StaleIncarnation(root.agent_id())
        );
        ledger.set_open(root.agent_id(), false).unwrap();
        assert_eq!(
            rebound.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(root.agent_id())
        );
        drop(admitted);
        assert_eq!(ledger.active_agents(), 0);
        ledger.set_open(root.agent_id(), true).unwrap();
        let next = rebound.try_reserve().unwrap();
        ledger.remove(root.agent_id()).unwrap();
        assert_eq!(
            rebound.try_reserve().unwrap_err(),
            ManagerError::UnknownAgent(root.agent_id())
        );
        assert_eq!(ledger.active_agents(), 1);
        drop(next);
        assert_eq!(ledger.active_agents(), 0);
    }

    #[test]
    fn activation_binding_does_not_open_admission() {
        let ledger = ledger(1, 1);
        let id = AgentId::generate();
        let dormant = ledger.register(id, None, INCARNATION, false);
        let bound = ledger.bind_incarnation(id, NEXT_INCARNATION).unwrap();
        assert_eq!(
            dormant.try_reserve().unwrap_err(),
            ManagerError::StaleIncarnation(id)
        );
        assert_eq!(
            bound.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(id)
        );
        ledger.set_open(id, true).unwrap();
        let reservation = bound.try_reserve().unwrap();
        assert_eq!(reservation.agent_id(), id);
    }

    #[test]
    fn report_transfer_reuses_reserved_capacity_and_holds_sender() {
        let ledger = ledger(3, 1);
        let root = register(&ledger, None);
        let parent = register(&ledger, Some(root.agent_id()));
        let child = register(&ledger, Some(parent.agent_id()));
        let mut work = child.try_reserve_reporting(true).unwrap();
        ledger.set_open(root.agent_id(), false).unwrap();
        ledger.set_open(parent.agent_id(), false).unwrap();
        let mut report = work.take_report().unwrap();
        assert!(work.take_report().is_none());
        assert_eq!(report.agent_id(), parent.agent_id());
        drop(work);
        assert_eq!(ledger.active_agents(), 3);
        let queued = report.clone();
        let next_report = report.take_report().unwrap();
        drop(report);
        assert_eq!(ledger.active_agents(), 3);
        drop(queued);
        assert_eq!(ledger.active_agents(), 2);
        assert_eq!(next_report.agent_id(), root.agent_id());
        drop(next_report);
        assert_eq!(ledger.active_agents(), 0);
    }

    #[test]
    fn reporting_policy_stops_ancestor_reservation() {
        let ledger = ledger(2, 1);
        let root = register(&ledger, None);
        let parent = register(&ledger, Some(root.agent_id()));
        let child = register(&ledger, Some(parent.agent_id()));
        ledger.set_reporting(parent.agent_id(), false).unwrap();
        ledger.set_open(root.agent_id(), false).unwrap();
        let mut work = child.try_reserve_reporting(true).unwrap();
        let mut report = work.take_report().unwrap();
        assert!(report.take_report().is_none());
        assert_eq!(ledger.active_agents(), 2);
        drop(work);
        drop(report);
        assert_eq!(ledger.active_agents(), 0);
    }

    #[test]
    fn freeze_idle_rolls_back_without_overwriting_newer_binding() {
        let ledger = ledger(2, 1);
        let root = register(&ledger, None);
        let work = root.try_reserve().unwrap();
        assert!(ledger.is_busy(root.agent_id()));
        assert!(
            matches!(ledger.freeze_if_idle(root.agent_id()), Err(ManagerError::BusySubtree(id)) if id == root.agent_id())
        );
        drop(work);
        let freeze = ledger.freeze_if_idle(root.agent_id()).unwrap();
        assert_eq!(
            root.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(root.agent_id())
        );
        let child = register(&ledger, Some(root.agent_id()));
        assert_eq!(
            child.try_reserve_reporting(true).unwrap_err(),
            ManagerError::NonLiveAgent(root.agent_id())
        );
        drop(freeze);
        let work = root.try_reserve().unwrap();
        drop(work);
        let freeze = ledger.freeze_if_idle(root.agent_id()).unwrap();
        let rebound = ledger
            .bind_incarnation(root.agent_id(), NEXT_INCARNATION)
            .unwrap();
        drop(freeze);
        assert_eq!(
            rebound.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(root.agent_id())
        );
        assert_eq!(
            root.set_open(true).unwrap_err(),
            ManagerError::StaleIncarnation(root.agent_id())
        );
        assert_eq!(
            root.set_reporting(false).unwrap_err(),
            ManagerError::StaleIncarnation(root.agent_id())
        );
        rebound.set_open(true).unwrap();
        let freeze = ledger.freeze_if_idle(root.agent_id()).unwrap();
        freeze.commit();
        assert_eq!(
            rebound.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(root.agent_id())
        );
    }

    #[test]
    fn dormant_parent_accepts_reserved_reports_without_source_admission() {
        let ledger = ledger(2, 1);
        let root = register(&ledger, None);
        let child = register(&ledger, Some(root.agent_id()));
        root.set_open(false).unwrap();
        root.set_reporting(false).unwrap();
        let mut work = child.try_reserve_reporting(true).unwrap();
        assert_eq!(ledger.active_agents(), 2);
        assert_eq!(
            root.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(root.agent_id())
        );
        let delivery = work.take_report().unwrap();
        drop(work);
        assert!(ledger.is_busy(child.agent_id()));
        drop(delivery);
        assert!(!ledger.is_busy(child.agent_id()));
    }

    #[test]
    fn restored_delivery_holds_over_limit_block_only_new_distinct_agents() {
        let ledger = ledger(1, 1);
        let root_id = AgentId::generate();
        let child_id = AgentId::generate();
        let root = ledger.register(root_id, None, INCARNATION, false);
        let child = ledger.register(child_id, Some(root_id), INCARNATION, false);
        let sibling = register(&ledger, Some(root_id));
        let other = register(&ledger, None);
        let mut restored = ledger.restore_hold(child_id).unwrap();
        let second_restore = ledger.restore_hold(sibling.agent_id()).unwrap();
        assert_eq!(ledger.active_agents(), 3);
        assert_eq!(ledger.active_children(root_id), 2);
        ledger.set_open(child_id, true).unwrap();
        let queued = child.try_reserve().unwrap();
        assert_eq!(
            other.try_reserve().unwrap_err(),
            ManagerError::ActiveAgentLimit { max: 1 }
        );
        assert_eq!(
            root.try_reserve().unwrap_err(),
            ManagerError::NonLiveAgent(root_id)
        );
        let delivery = restored.take_report().unwrap();
        drop(restored);
        drop(second_restore);
        drop(queued);
        assert_eq!(ledger.active_agents(), 2);
        drop(delivery);
        assert_eq!(ledger.active_agents(), 0);
        assert_eq!(ledger.active_children(root_id), 0);
        assert!(other.try_reserve().is_ok());
    }
}
