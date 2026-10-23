use std::sync::Arc;

use maki_providers::Message;
use maki_storage::sessions::HistorySnapshot;

use super::{
    ActorError, ActorLifecycle, ActorQueue, ActorState, ActorStatus, ActorWork, AgentActorHandle,
    ConfigCommit, HistoryEditWork, PreparedOperationTicket, PreparedTurn, TurnTicket,
    lifecycle_error,
};
use crate::{AgentId, TurnId};

#[derive(Clone)]
pub struct ActorSourceSnapshot {
    pub agent_id: AgentId,
    pub config: ConfigCommit,
    pub history: HistorySnapshot<Message>,
}

pub(super) struct IdleReservation {
    pub(super) owner: Arc<()>,
    pub(super) permission: Option<TurnId>,
    pub(super) committing: bool,
    pub(super) edit: Option<HistoryEditWork>,
}

/// Reserves an idle actor for a permitted turn or a checkpointed history edit.
pub struct ActorIdleGuard {
    pub(super) actor: AgentActorHandle,
    pub(super) owner: Arc<()>,
    pub(super) release: bool,
}

impl ActorIdleGuard {
    /// Reserves an operation for the guard owner, which other callers cannot
    /// do while the guard is held.
    pub fn reserve_prepared_operation(
        &self,
        turn: Option<PreparedTurn>,
    ) -> Result<PreparedOperationTicket, ActorError> {
        self.actor.reserve_operation(turn, Some(&self.owner))
    }

    pub fn source_snapshot(&self) -> Result<ActorSourceSnapshot, ActorError> {
        let mut state = self
            .actor
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        state
            .owned_idle(&self.owner)
            .filter(|reservation| {
                !reservation.committing
                    && reservation.permission.is_none()
                    && reservation.edit.is_none()
            })
            .ok_or(ActorError::Busy)?;
        let config = state.policy.clone().ok_or_else(|| {
            ActorError::InvalidConfig("actor configuration is not initialized".into())
        })?;
        Ok(ActorSourceSnapshot {
            agent_id: self.actor.agent_id(),
            config: ConfigCommit {
                identity: Arc::clone(&self.actor.inner.identity),
                generation: state.policy_generation,
                config,
            },
            history: self.actor.inner.history.load_full().as_ref().clone(),
        })
    }

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
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if retired {
            return Err(ActorError::PolicyCancelled);
        }
        let reservation = state
            .owned_idle(&self.owner)
            .filter(|reservation| {
                reservation.edit.is_none()
                    && reservation
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
        if !self.release {
            return;
        }
        let mut state = self
            .actor
            .inner
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if state.owned_idle(&self.owner).is_some() {
            state.idle = None;
            super::drive_operations(&self.actor.inner, &mut state);
            self.actor.inner.queue.notify();
        }
    }
}

impl ActorState {
    pub(super) fn owned_idle(&mut self, owner: &Arc<()>) -> Option<&mut IdleReservation> {
        self.idle
            .as_mut()
            .filter(|reservation| Arc::ptr_eq(&reservation.owner, owner))
    }

    pub(super) fn has_pending_work(&self, queue: &ActorQueue) -> bool {
        self.status != ActorStatus::Idle
            || self.active.is_some()
            || self.processing.is_some()
            || self.pending_publications != 0
            || !self.operations.is_empty()
            || !queue.is_empty()
    }

    /// While reserved, only the permitted turn may start, and starting it
    /// ends the reservation so it runs like any other turn.
    pub(super) fn pop_work(&mut self, queue: &ActorQueue) -> Option<ActorWork> {
        let Some(reservation) = &mut self.idle else {
            return queue.pop();
        };
        if let Some(edit) = reservation.edit.take() {
            return Some(ActorWork::HistoryEdit(edit));
        }
        let admission = queue.remove_turn(reservation.permission?)?;
        self.idle = None;
        Some(ActorWork::Turn(Box::new(admission)))
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

    /// Waits until no idle guard is held or the actor stops.
    pub async fn wait_unguarded(&self) {
        loop {
            let listener = self.inner.policy_changed.listen();
            {
                let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if state.idle.is_none() || state.lifecycle != ActorLifecycle::Open {
                    return;
                }
            }
            listener.await;
        }
    }

    pub fn prepare_idle(&self) -> Result<ActorIdleGuard, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if state.idle.is_some()
            || !state.operations.is_empty()
            || state.has_pending_work(&self.inner.queue)
        {
            return Err(ActorError::Busy);
        }
        let owner = Arc::new(());
        state.idle = Some(IdleReservation {
            owner: Arc::clone(&owner),
            permission: None,
            committing: false,
            edit: None,
        });
        Ok(ActorIdleGuard {
            actor: self.clone(),
            owner,
            release: true,
        })
    }
}
