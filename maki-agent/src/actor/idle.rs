use super::{
    ActorError, ActorLifecycle, ActorOperation, ActorStatus, AgentActorHandle, TurnTicket,
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

impl AgentActorHandle {
    pub fn prepare_idle(&self) -> Result<ActorIdleGuard, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open
            || state.status != ActorStatus::Idle
            || state.active.is_some()
            || state.processing.is_some()
            || state.idle_reserved
            || state
                .operations
                .iter()
                .any(|operation| !matches!(operation, ActorOperation::Config(_)))
            || !self.inner.queue.is_empty()
        {
            return Err(ActorError::PolicyPending);
        }
        state.idle_reserved = true;
        Ok(ActorIdleGuard {
            actor: self.clone(),
        })
    }
}
