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

#[derive(Clone)]
pub struct PreparedCommit {
    pub config: ConfigCommit,
    pub ticket: Option<TurnTicket>,
}

pub(super) struct PreparedOperation {
    pub id: u64,
    pub turn: Option<PreparedTurn>,
    change: Option<Result<Option<ConfigChange>, ActorError>>,
    ready: Option<(Arc<EffectiveAgentConfig>, Option<TurnAdmissionSnapshot>)>,
    pub(super) completion: Arc<Mutex<Option<Result<PreparedCommit, ActorError>>>>,
    cancel: CancelToken,
    _trigger: CancelTrigger,
}

pub struct PreparedOperationTicket {
    inner: Arc<ActorInner>,
    id: u64,
    completion: Arc<Mutex<Option<Result<PreparedCommit, ActorError>>>>,
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
        let completion = Arc::new(Mutex::new(None));
        let (trigger, cancel) = CancelToken::new();
        state
            .operations
            .push_back(ActorOperation::Prepared(Box::new(PreparedOperation {
                id,
                turn,
                change: None,
                ready: None,
                completion: Arc::clone(&completion),
                cancel,
                _trigger: trigger,
            })));
        Ok(PreparedOperationTicket {
            inner: Arc::clone(&self.inner),
            id,
            completion,
        })
    }
}

impl PreparedOperationTicket {
    pub fn replace_turn_input(&self, input: AgentInput) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if state.preparing == Some(self.id) {
            return Err(ActorError::PolicyPending);
        }
        let pending = state
            .operations
            .iter_mut()
            .find_map(|entry| match entry {
                ActorOperation::Prepared(pending) if pending.id == self.id => Some(pending),
                _ => None,
            })
            .ok_or(ActorError::PolicyCancelled)?;
        if pending.change.is_some() || pending.ready.is_some() {
            return Err(ActorError::PolicyPending);
        }
        let turn = pending.turn.as_mut().ok_or(ActorError::PolicyCancelled)?;
        turn.input = input;
        Ok(())
    }

    pub fn resolve(
        &self,
        change: Result<Option<ConfigChange>, ActorError>,
    ) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if state.preparing == Some(self.id) {
            return Err(ActorError::PolicyPending);
        }
        let pending = state
            .operations
            .iter_mut()
            .find_map(|entry| match entry {
                ActorOperation::Prepared(pending) if pending.id == self.id => Some(pending),
                _ => None,
            })
            .ok_or(ActorError::PolicyCancelled)?;
        if pending.change.is_some() || pending.ready.is_some() {
            return Err(ActorError::PolicyPending);
        }
        pending.change = Some(change);
        drive_operations(&self.inner, &mut state);
        Ok(())
    }

    pub async fn wait_ready(&self) -> Result<(), ActorError> {
        loop {
            let listener = self.inner.policy_changed.listen();
            {
                let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(result) = self
                    .completion
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone()
                {
                    return result.map(|_| ());
                }
                if state.operations.iter().any(|entry| matches!(entry, ActorOperation::Prepared(pending) if pending.id == self.id && pending.ready.is_some())) { return Ok(()); }
            }
            listener.await;
        }
    }

    pub fn ready_config(&self) -> Result<ConfigCommit, ActorError> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(result) = self
            .completion
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return result.map(|commit| commit.config);
        }
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        let config = state
            .operations
            .iter()
            .find_map(|entry| match entry {
                ActorOperation::Prepared(pending) if pending.id == self.id => {
                    pending.ready.as_ref().map(|(config, _)| Arc::clone(config))
                }
                _ => None,
            })
            .ok_or(ActorError::PolicyPending)?;
        Ok(ConfigCommit {
            identity: Arc::clone(&self.inner.identity),
            generation: state.policy_generation,
            config,
        })
    }

    pub fn cancel(&self) -> Result<Option<PreparedCommit>, ActorError> {
        self.retire(ActorError::PolicyCancelled)
    }
    pub fn expire(&self) -> Result<Option<PreparedCommit>, ActorError> {
        self.retire(ActorError::ConfigExpired)
    }

    fn retire(&self, error: ActorError) -> Result<Option<PreparedCommit>, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(result) = self
            .completion
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return result.map(Some);
        }
        if let Some(index) = state
            .operations
            .iter()
            .position(|entry| entry.id() == self.id)
        {
            if let Some(ActorOperation::Prepared(pending)) = state.operations.remove(index) {
                pending.fail(error);
            }
            drive_operations(&self.inner, &mut state);
            self.inner.policy_changed.notify(usize::MAX);
        }
        Ok(None)
    }

    pub fn commit(self) -> Result<PreparedCommit, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        if let Some(result) = self
            .completion
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            return result;
        }
        if !matches!(state.operations.front(), Some(ActorOperation::Prepared(pending)) if pending.id == self.id && pending.ready.is_some())
        {
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
        *pending.completion.lock().unwrap_or_else(|e| e.into_inner()) = Some(Ok(result.clone()));
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

impl Drop for PreparedOperationTicket {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

impl PreparedOperation {
    pub(super) fn fail(self, error: ActorError) {
        self.completion
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_or_insert(Err(error));
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
    let config = change.and_then(|change| {
        let current = state.policy.as_ref().ok_or_else(|| {
            ActorError::InvalidConfig("actor configuration is not initialized".into())
        })?;
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
