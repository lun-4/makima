use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use futures_lite::FutureExt;

use crate::agent::TurnAdmissionSnapshot;
use crate::{AgentInput, CancelToken, CancelTrigger, EventSender, TurnId};

use super::{
    ActorError, ActorInner, ActorLifecycle, ActorOperation, ActorState, AgentActorHandle,
    ConfigChange, ConfigCommit, EffectiveAgentConfig, TurnTicket, config, drive_operations,
    lifecycle_error, materialize, preparation_input,
};

pub struct PreparedTurn {
    pub input: AgentInput,
    pub event_sender: Option<EventSender>,
    pub correlation: String,
}

pub struct PreparedCommit {
    pub config: ConfigCommit,
    pub ticket: Option<TurnTicket>,
}

pub(super) struct PreparedOperation {
    pub id: u64,
    pub turn: Option<PreparedTurn>,
    change: Option<Option<ConfigChange>>,
    ready: Option<(Arc<EffectiveAgentConfig>, Option<TurnAdmissionSnapshot>)>,
    pub(super) failure: Arc<Mutex<Option<ActorError>>>,
    cancel: CancelToken,
    _trigger: CancelTrigger,
}

pub struct PreparedOperationTicket {
    inner: Arc<ActorInner>,
    id: u64,
    failure: Arc<Mutex<Option<ActorError>>>,
}

impl AgentActorHandle {
    pub fn reserve_prepared_operation(
        &self,
        turn: Option<PreparedTurn>,
    ) -> Result<PreparedOperationTicket, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if turn
            .as_ref()
            .is_some_and(|turn| state.cancelled_correlations.contains_key(&turn.correlation))
        {
            return Err(ActorError::PolicyCancelled);
        }
        state.next_operation_id = state.next_operation_id.wrapping_add(1);
        let id = state.next_operation_id;
        let failure = Arc::new(Mutex::new(None));
        let (trigger, cancel) = CancelToken::new();
        state
            .operations
            .push_back(ActorOperation::Prepared(Box::new(PreparedOperation {
                id,
                turn,
                change: None,
                ready: None,
                failure: Arc::clone(&failure),
                cancel,
                _trigger: trigger,
            })));
        Ok(PreparedOperationTicket {
            inner: Arc::clone(&self.inner),
            id,
            failure,
        })
    }
}

impl PreparedOperationTicket {
    pub fn replace_turn_input(&self, input: AgentInput) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let pending = self.unresolved(&mut state)?;
        let turn = pending.turn.as_mut().ok_or(ActorError::PolicyCancelled)?;
        turn.input = input;
        Ok(())
    }

    pub fn resolve(&self, change: Option<ConfigChange>) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        self.unresolved(&mut state)?.change = Some(change);
        drive_operations(&self.inner, &mut state);
        Ok(())
    }

    pub async fn wait_ready(&self) -> Result<(), ActorError> {
        loop {
            let listener = self.inner.policy_changed.listen();
            {
                let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(error) = self.failure() {
                    return Err(error);
                }
                if state
                    .pending_prepared(self.id)
                    .is_some_and(|pending| pending.ready.is_some())
                {
                    return Ok(());
                }
            }
            listener.await;
        }
    }

    pub fn ready_config(&self) -> Result<Arc<EffectiveAgentConfig>, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(error) = self.failure() {
            return Err(error);
        }
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        state
            .pending_prepared(self.id)
            .and_then(|pending| pending.ready.as_ref())
            .map(|(config, _)| Arc::clone(config))
            .ok_or(ActorError::PolicyPending)
    }

    pub fn cancel(&self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.failure().is_some() {
            return;
        }
        if let Some(index) = state
            .operations
            .iter()
            .position(|entry| entry.id() == self.id)
        {
            if let Some(ActorOperation::Prepared(pending)) = state.operations.remove(index) {
                pending.fail(ActorError::PolicyCancelled);
            }
            drive_operations(&self.inner, &mut state);
            self.inner.policy_changed.notify(usize::MAX);
        }
    }

    pub fn commit(self) -> Result<PreparedCommit, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if let Some(error) = self.failure() {
            return Err(error);
        }
        if !state.prepared_head_ready(self.id) {
            return Err(ActorError::PolicyPending);
        }
        let Some(ActorOperation::Prepared(mut pending)) = state.operations.pop_front() else {
            unreachable!()
        };
        let (config, snapshot) = pending.ready.take().unwrap();
        let changed = state
            .policy
            .as_ref()
            .is_none_or(|current| !config::equivalent(current, &config));
        if changed {
            state.policy_generation = state.policy_generation.wrapping_add(1);
            state.policy = Some(Arc::clone(&config));
        }
        let commit = ConfigCommit {
            identity: Arc::clone(&self.inner.identity),
            generation: state.policy_generation,
            config: state.policy.clone().unwrap(),
        };
        let ticket = pending.turn.take().map(|turn| {
            let turn_id = TurnId::generate();
            let ticket = TurnTicket::new(turn_id, Arc::clone(&self.inner.identity));
            self.inner
                .tickets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(turn_id, ticket.clone());
            materialize(
                &self.inner,
                &state,
                ActorOperation::Turn {
                    id: pending.id,
                    input: turn.input,
                    event_sender: turn.event_sender,
                    correlation: turn.correlation,
                    ticket: ticket.clone(),
                },
                snapshot,
            );
            ticket
        });
        let result = PreparedCommit {
            config: commit.clone(),
            ticket,
        };
        if changed {
            state
                .config_observers
                .retain(|observer| observer.send(commit.clone()).is_ok());
        }
        drive_operations(&self.inner, &mut state);
        self.inner.policy_changed.notify(usize::MAX);
        Ok(result)
    }
}

impl PreparedOperationTicket {
    fn failure(&self) -> Option<ActorError> {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The pending operation, while it still accepts its turn input or change.
    fn unresolved<'a>(
        &self,
        state: &'a mut ActorState,
    ) -> Result<&'a mut PreparedOperation, ActorError> {
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if state.preparing == Some(self.id) {
            return Err(ActorError::PolicyPending);
        }
        let pending = state
            .pending_prepared(self.id)
            .ok_or(ActorError::PolicyCancelled)?;
        if pending.change.is_some() || pending.ready.is_some() {
            return Err(ActorError::PolicyPending);
        }
        Ok(pending)
    }
}

impl ActorState {
    fn pending_prepared(&mut self, id: u64) -> Option<&mut PreparedOperation> {
        self.operations.iter_mut().find_map(|entry| match entry {
            ActorOperation::Prepared(pending) if pending.id == id => Some(&mut **pending),
            _ => None,
        })
    }

    fn prepared_head_ready(&self, id: u64) -> bool {
        match self.operations.front() {
            Some(ActorOperation::Prepared(pending)) => pending.id == id && pending.ready.is_some(),
            _ => false,
        }
    }
}

impl Drop for PreparedOperationTicket {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl PreparedOperation {
    pub(super) fn fail(self, error: ActorError) {
        self.failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(error);
    }
}

pub(super) fn drive(inner: &Arc<ActorInner>, state: &mut ActorState) {
    let Some(ActorOperation::Prepared(pending)) = state.operations.front_mut() else {
        return;
    };
    if pending.ready.is_some() {
        return;
    }
    let Some(change) = pending.change.take() else {
        return;
    };
    let config = state
        .policy
        .as_ref()
        .ok_or_else(|| ActorError::InvalidConfig("actor configuration is not initialized".into()))
        .and_then(|current| {
            change.map_or_else(
                || Ok(Arc::clone(current)),
                |change| change.apply(current).map(Arc::new),
            )
        });
    let config = match config {
        Ok(config) => config,
        Err(error) => {
            let Some(ActorOperation::Prepared(pending)) = state.operations.pop_front() else {
                unreachable!()
            };
            pending.fail(error);
            inner.policy_changed.notify(usize::MAX);
            drive_operations(inner, state);
            return;
        }
    };
    let input = pending
        .turn
        .as_ref()
        .map(|turn| preparation_input(&turn.input));
    let prepare = inner.admission_preparation.clone();
    let readiness = inner.prepared_readiness.clone();
    let cancel = pending.cancel.clone();
    let id = pending.id;
    state.preparing = Some(id);
    let inner = Arc::clone(inner);
    smol::spawn(async move {
        let candidate = Arc::clone(&config);
        let result = cancel
            .race(async move {
                let snapshot = if let (Some(mut input), Some(prepare)) = (input, prepare) {
                    input.mode = candidate.mode.clone();
                    input.thinking = candidate.thinking;
                    input.fast = candidate.fast;
                    input.workflow = candidate.workflow;
                    Some(
                        smol::unblock(move || {
                            catch_unwind(AssertUnwindSafe(|| {
                                let mut snapshot = prepare(&input, &input.mode, Some(&candidate));
                                if let Some(mode_def) = &candidate.mode_def {
                                    snapshot.mode_def = Some(Arc::new(mode_def.clone()));
                                }
                                snapshot
                            }))
                        })
                        .await
                        .map_err(|_| {
                            ActorError::InvalidConfig("admission preparation panicked".into())
                        })?,
                    )
                } else {
                    None
                };
                match (snapshot, readiness) {
                    (Some(snapshot), Some(readiness)) => {
                        AssertUnwindSafe(async move { readiness(snapshot).await })
                            .catch_unwind()
                            .await
                            .map_err(|_| {
                                ActorError::InvalidConfig("prepared readiness panicked".into())
                            })?
                            .map(Some)
                    }
                    (snapshot, _) => Ok(snapshot),
                }
            })
            .await
            .unwrap_or(Err(ActorError::PolicyCancelled));
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open
            || state.preparing != Some(id)
            || state
                .operations
                .front()
                .is_none_or(|entry| entry.id() != id)
        {
            return;
        }
        state.preparing = None;
        match result {
            Ok(snapshot) => {
                if let Some(ActorOperation::Prepared(pending)) = state.operations.front_mut() {
                    pending.ready = Some((config, snapshot));
                }
            }
            Err(error) => {
                if let Some(ActorOperation::Prepared(pending)) = state.operations.pop_front() {
                    pending.fail(error);
                }
                drive_operations(&inner, &mut state);
            }
        }
        inner.policy_changed.notify(usize::MAX);
    })
    .detach();
}
