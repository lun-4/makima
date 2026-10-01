use std::sync::Arc;

use super::{
    ActorError, ActorLifecycle, ActorOperation, ActorQueue, ActorState, ActorStatus, ActorWork,
    AgentActorHandle, TurnTicket,
};
use crate::TurnId;

pub(super) struct IdleReservation {
    owner: Arc<()>,
    permission: Option<TurnId>,
}

/// Holds an idle actor until its permitted turn starts. Starting that turn
/// ends the reservation, so dropping the guard afterwards changes nothing.
pub struct ActorIdleGuard {
    actor: AgentActorHandle,
    owner: Arc<()>,
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
        let retired = !self
            .actor
            .inner
            .tickets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&ticket.turn_id());
        if state.lifecycle != ActorLifecycle::Open || retired {
            return Err(ActorError::PolicyCancelled);
        }
        let reservation = state
            .owned_idle(&self.owner)
            .filter(|reservation| {
                reservation
                    .permission
                    .is_none_or(|id| id == ticket.turn_id())
            })
            .ok_or(ActorError::PolicyCancelled)?;
        reservation.permission = Some(ticket.turn_id());
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
        if state.owned_idle(&self.owner).is_some() {
            state.idle = None;
            self.actor.inner.queue.notify();
        }
    }
}

impl ActorState {
    fn owned_idle(&mut self, owner: &Arc<()>) -> Option<&mut IdleReservation> {
        self.idle
            .as_mut()
            .filter(|reservation| Arc::ptr_eq(&reservation.owner, owner))
    }

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

    /// While reserved, only the permitted turn may start, and starting it
    /// ends the reservation so it runs like any other turn.
    pub(super) fn pop_work(&mut self, queue: &ActorQueue) -> Option<ActorWork> {
        let Some(reservation) = &self.idle else {
            return queue.pop();
        };
        let admission = queue.remove_turn(reservation.permission?)?;
        self.idle = None;
        Some(ActorWork::Turn(admission))
    }

    /// Returns whether a reservation waiting for `turn_id` was released.
    pub(super) fn release_idle_permission(&mut self, turn_id: TurnId) -> bool {
        let permitted = self
            .idle
            .as_ref()
            .is_some_and(|reservation| reservation.permission == Some(turn_id));
        if permitted {
            self.idle = None;
        }
        permitted
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
            || state.idle.is_some()
            || state.has_pending_work(&self.inner.queue)
        {
            return Err(ActorError::PolicyPending);
        }
        let owner = Arc::new(());
        state.idle = Some(IdleReservation {
            owner: Arc::clone(&owner),
            permission: None,
        });
        Ok(ActorIdleGuard {
            actor: self.clone(),
            owner,
        })
    }
}
