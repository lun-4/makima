use super::{
    ActorError, ActorLifecycle, ActorOperation, ActorQueue, ActorState, ActorStatus,
    AgentActorHandle, TurnTicket,
};

pub struct ActorIdleGuard {
    actor: AgentActorHandle,
}

impl ActorIdleGuard {
    pub fn allow_turn(&self, ticket: &TurnTicket) -> Result<(), ActorError> {
        if !self.actor.owns_ticket(ticket) {
            return Err(ActorError::PolicyCancelled);
        }
        let mut state = self
            .actor
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open
            || state
                .idle_permission
                .is_some_and(|id| id != ticket.turn_id())
        {
            return Err(ActorError::PolicyCancelled);
        }
        state.idle_permission = Some(ticket.turn_id());
        self.actor.inner.queue.notify();
        Ok(())
    }
}

impl Drop for ActorIdleGuard {
    fn drop(&mut self) {
        let mut state = self
            .actor
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.idle_reserved = false;
        state.idle_permission = None;
        self.actor.inner.queue.notify();
    }
}

impl ActorState {
    fn has_pending_work(&self, queue: &ActorQueue) -> bool {
        self.status != ActorStatus::Idle
            || self.active.is_some()
            || self.processing.is_some()
            || self
                .operations
                .iter()
                .any(|operation| !matches!(operation, ActorOperation::Config(_)))
            || !queue.is_empty()
    }
}

impl AgentActorHandle {
    pub(crate) fn has_pending_work(&self) -> bool {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .has_pending_work(&self.inner.queue)
    }

    pub fn prepare_idle(&self) -> Result<ActorIdleGuard, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open
            || state.idle_reserved
            || state.has_pending_work(&self.inner.queue)
        {
            return Err(ActorError::PolicyPending);
        }
        state.idle_reserved = true;
        Ok(ActorIdleGuard {
            actor: self.clone(),
        })
    }
}
