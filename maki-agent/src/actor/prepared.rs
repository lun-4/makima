use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures_lite::FutureExt;
use maki_providers::Message;
use maki_storage::checkpoint::CheckpointPublication;
use maki_storage::sessions::HistorySnapshot;

use crate::agent::TurnAdmissionSnapshot;
use crate::cancel::PublicationRegistration;
use crate::types::{ActorWorkIdentity, ActorWorkKind};
use crate::{AgentInput, CancelToken, CancelTrigger, EventSender, TurnId};

use super::{
    ActorError, ActorInner, ActorLifecycle, ActorOperation, ActorState, AgentActorHandle,
    ConfigChange, ConfigCommit, EffectiveAgentConfig, TurnTicket, config, drive_operations,
    lifecycle_error, materialize, preparation_input,
};

const PUBLICATION_CALLBACK_PANICKED: &str = "publication callback panicked";
const PUBLICATION_FUTURE_PANICKED: &str = "publication future panicked";

pub struct PreparedTurn {
    pub input: AgentInput,
    pub event_sender: Option<EventSender>,
    pub correlation: String,
}

pub struct PreparedCommit {
    pub config: ConfigCommit,
    pub ticket: Option<TurnTicket>,
}

#[derive(Clone)]
pub struct PreparedTransactionSnapshot {
    pub config: Arc<EffectiveAgentConfig>,
    pub history: HistorySnapshot<Message>,
    pub commit: ConfigCommit,
}

/// Stages and publishes the captured candidate outside the actor lock. The host
/// must claim `publication` at durable replacement, and finish rollback before
/// returning an error. It must not await work on this actor's occupied runner.
pub type PreparedPublication = Box<
    dyn FnOnce(
            PreparedTransactionSnapshot,
            CheckpointPublication,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>
        + Send,
>;

/// Installs reserved public options and the matching host snapshot before config
/// observers or the caller see success. It must not fail or reenter the actor.
pub type PreparedFinalization = Box<dyn FnOnce(&ConfigCommit) + Send>;

pub struct PreparedTransaction {
    pub publication: CheckpointPublication,
    pub publish: PreparedPublication,
    pub finalize: PreparedFinalization,
}

pub(super) struct PreparedPublicationState {
    id: u64,
    correlation: Option<String>,
    publication: CheckpointPublication,
}

pub(super) struct CancelPublication(pub CheckpointPublication);

impl Drop for CancelPublication {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl PreparedTransaction {
    pub(super) async fn publish(
        self,
        snapshot: PreparedTransactionSnapshot,
    ) -> Result<PreparedFinalization, ActorError> {
        let future = catch_unwind(AssertUnwindSafe(|| {
            (self.publish)(snapshot, self.publication.clone())
        }))
        .map_err(|_| ActorError::HistoryCheckpoint(PUBLICATION_CALLBACK_PANICKED.into()))?;
        AssertUnwindSafe(future)
            .catch_unwind()
            .await
            .map_err(|_| ActorError::HistoryCheckpoint(PUBLICATION_FUTURE_PANICKED.into()))?
            .map_err(ActorError::HistoryCheckpoint)?;
        if self.publication.is_cancelled() {
            return Err(ActorError::PolicyCancelled);
        }
        if self.publication.cancel() {
            return Err(ActorError::HistoryCheckpoint(
                "publication callback succeeded without claiming durable publication".into(),
            ));
        }
        Ok(self.finalize)
    }
}

pub(super) struct PreparedOperation {
    pub id: u64,
    pub turn: Option<PreparedTurn>,
    turn_displayed: bool,
    change: Option<Option<ConfigChange>>,
    ready: Option<(Arc<EffectiveAgentConfig>, Option<TurnAdmissionSnapshot>)>,
    pub(super) failure: Arc<Mutex<Option<ActorError>>>,
    cancel: CancelToken,
    _trigger: CancelTrigger,
    publication: Option<PublicationRegistration>,
    /// Presentation reserved at admission. The turn id is stamped when this
    /// operation commits into a real turn.
    identity: ActorWorkIdentity,
}

pub struct PreparedOperationTicket {
    inner: Arc<ActorInner>,
    id: u64,
    presentation_id: u64,
    failure: Arc<Mutex<Option<ActorError>>>,
}

impl AgentActorHandle {
    /// Reserves the next FIFO operation slot. Fails with
    /// [`ActorError::Busy`] while an idle guard is held; the guard owner
    /// reserves through [`ActorIdleGuard::reserve_prepared_operation`].
    pub fn reserve_prepared_operation(
        &self,
        turn: Option<PreparedTurn>,
    ) -> Result<PreparedOperationTicket, ActorError> {
        self.reserve_operation(turn, None)
    }

    pub(super) fn reserve_operation(
        &self,
        turn: Option<PreparedTurn>,
        idle_owner: Option<&Arc<()>>,
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
        if let Some(idle) = &state.idle
            && idle_owner.is_none_or(|owner| !Arc::ptr_eq(&idle.owner, owner))
        {
            return Err(ActorError::Busy);
        }
        state.next_operation_id = state.next_operation_id.wrapping_add(1);
        let id = state.next_operation_id;
        let presentation_id = state.reserve_presentation(0);
        let identity = ActorWorkIdentity::new(
            self.inner.agent_id,
            ActorWorkKind::Control { operation_id: id },
            presentation_id,
        );
        let failure = Arc::new(Mutex::new(None));
        let (trigger, cancel) = CancelToken::new();
        state.idle_pending = true;
        state
            .operations
            .push_back(ActorOperation::Prepared(Box::new(PreparedOperation {
                id,
                turn,
                turn_displayed: false,
                change: None,
                ready: None,
                failure: Arc::clone(&failure),
                cancel,
                _trigger: trigger,
                publication: None,
                identity,
            })));
        Ok(PreparedOperationTicket {
            inner: Arc::clone(&self.inner),
            id,
            presentation_id,
            failure,
        })
    }
}

impl PreparedOperationTicket {
    /// The presentation this operation reserved; its turn, if any, is
    /// stamped with it.
    pub fn presentation_id(&self) -> u64 {
        self.presentation_id
    }

    /// Registers cancellation before asynchronous host preparation begins.
    pub fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(error) = self.failure() {
            publication.cancel();
            return Err(error);
        }
        if state.lifecycle != ActorLifecycle::Open {
            publication.cancel();
            return Err(lifecycle_error(state.lifecycle));
        }
        let pending = state
            .pending_prepared(self.id)
            .ok_or(ActorError::PolicyCancelled)?;
        if pending.publication.is_some() {
            return Err(ActorError::PolicyPending);
        }
        pending.publication = Some(pending.cancel.register_publication(publication));
        Ok(())
    }

    /// Swaps in a new turn input while the operation still accepts one.
    /// Fails with [`ActorError::PolicyCancelled`] when the operation was
    /// cancelled, closed, or carries no turn, and
    /// [`ActorError::PolicyPending`] once a change was already resolved and
    /// preparation is under way.
    pub fn replace_turn_input(&self, input: AgentInput) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let pending = self.unresolved(&mut state)?;
        let turn = pending.turn.as_mut().ok_or(ActorError::PolicyCancelled)?;
        turn.input = input;
        Ok(())
    }

    /// Sets whether the caller already displayed this turn, including after readiness.
    /// Must be set before commit; config-only or cancelled operations reject it.
    pub fn set_turn_displayed(&self, displayed: bool) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(error) = self.failure() {
            return Err(error);
        }
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        let pending = state
            .pending_prepared(self.id)
            .filter(|pending| pending.turn.is_some())
            .ok_or(ActorError::PolicyCancelled)?;
        pending.turn_displayed = displayed;
        Ok(())
    }

    /// Resolves the configuration change the prepared operation commits.
    /// `None` keeps the actor's current configuration. Fails with
    /// [`ActorError::PolicyPending`] when the change or turn input was
    /// already resolved.
    pub fn resolve(&self, change: Option<ConfigChange>) -> Result<(), ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        self.unresolved(&mut state)?.change = Some(change);
        drive_operations(&self.inner, &mut state);
        Ok(())
    }

    /// Waits for earlier operations to commit and returns their resulting config.
    /// An active turn need not finish before this configuration boundary.
    pub async fn wait_predecessor(&self) -> Result<Arc<EffectiveAgentConfig>, ActorError> {
        loop {
            let listener = self.inner.policy_changed.listen();
            {
                let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(error) = self.failure() {
                    return Err(error);
                }
                if state.lifecycle != ActorLifecycle::Open {
                    return Err(lifecycle_error(state.lifecycle));
                }
                if state.prepared_publication.is_none()
                    && state
                        .operations
                        .front()
                        .is_some_and(|entry| entry.id() == self.id)
                {
                    return state.policy.clone().ok_or_else(|| {
                        ActorError::InvalidConfig("actor configuration is not initialized".into())
                    });
                }
            }
            listener.await;
        }
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

    /// The only production cancel path is `Drop`; `pub(crate)` exists for
    /// same-crate tests that need to cancel without dropping.
    pub(crate) fn cancel(&self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(publication) = &state.prepared_publication
            && publication.id == self.id
            && publication.publication.cancel()
        {
            let edit = state.idle.as_mut().and_then(|idle| {
                if idle.edit.as_ref().map(super::HistoryEditWork::prepared_id) == Some(self.id) {
                    idle.edit.take()
                } else {
                    None
                }
            });
            if let Some(edit) = edit {
                state.end_prepared_publication(self.id);
                state.idle = None;
                edit.fail(ActorError::PolicyCancelled);
                drive_operations(&self.inner, &mut state);
                state.publish_idle(&self.inner);
            }
        }
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
            state.publish_idle(&self.inner);
            self.inner.policy_changed.notify(usize::MAX);
        }
    }

    /// Resolves `change`, waits until it is the FIFO head and commits it
    /// without a durable publication.
    pub async fn commit_when_ready(
        self,
        change: Option<ConfigChange>,
    ) -> Result<PreparedCommit, ActorError> {
        self.resolve(change)?;
        self.wait_ready().await?;
        self.commit()
    }

    pub fn commit(self) -> Result<PreparedCommit, ActorError> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        let pending = self.take_ready_locked(&mut state)?;
        Ok(pending.commit_locked(&self.inner, &mut state, Box::new(|_| {})))
    }

    /// Transfers a ready config-only operation to an actor-owned task. Dropping
    /// the waiter cancels before the durable claim, but never drops that task.
    pub async fn commit_durable(
        self,
        transaction: PreparedTransaction,
    ) -> Result<PreparedCommit, ActorError> {
        let _cancellation = CancelPublication(transaction.publication.clone());
        let (completion, response) = flume::bounded(1);
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.lifecycle != ActorLifecycle::Open {
                return Err(lifecycle_error(state.lifecycle));
            }
            if state
                .pending_prepared(self.id)
                .is_some_and(|pending| pending.turn.is_some())
            {
                return Err(ActorError::Busy);
            }
            let operation = self.take_ready_locked(&mut state)?;
            let snapshot = operation.transaction_snapshot(
                &self.inner,
                &state,
                (*self.inner.history.load_full()).clone(),
            );
            state.begin_prepared_publication(&operation, transaction.publication.clone());
            let inner = Arc::clone(&self.inner);
            smol::spawn(async move {
                let result = transaction.publish(snapshot).await;
                let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
                let id = operation.id;
                let result = match result {
                    Ok(finalize) => Ok(operation.commit_locked(&inner, &mut state, finalize)),
                    Err(error) => {
                        operation.fail(error.clone());
                        Err(error)
                    }
                };
                state.end_prepared_publication(id);
                drive_operations(&inner, &mut state);
                state.publish_idle(&inner);
                state.finish_close(&inner);
                inner.policy_changed.notify(usize::MAX);
                inner.queue.notify();
                let _ = completion.send(result);
            })
            .detach();
        }
        response
            .recv_async()
            .await
            .map_err(|_| ActorError::Closed)?
    }

    pub(super) fn operation_id(&self) -> u64 {
        self.id
    }

    pub(super) fn belongs_to(&self, actor: &AgentActorHandle) -> bool {
        Arc::ptr_eq(&self.inner, &actor.inner)
    }

    pub(super) fn take_ready_locked(
        &self,
        state: &mut ActorState,
    ) -> Result<PreparedOperation, ActorError> {
        if let Some(error) = self.failure() {
            return Err(error);
        }
        if !state.prepared_head_ready(self.id) {
            return Err(ActorError::PolicyPending);
        }
        let Some(ActorOperation::Prepared(pending)) = state.operations.pop_front() else {
            unreachable!()
        };
        Ok(*pending)
    }
}

impl PreparedOperation {
    pub(super) fn transaction_snapshot(
        &self,
        inner: &ActorInner,
        state: &ActorState,
        history: HistorySnapshot<Message>,
    ) -> PreparedTransactionSnapshot {
        PreparedTransactionSnapshot {
            config: Arc::clone(&self.ready.as_ref().unwrap().0),
            history,
            commit: self.config_commit(inner, state),
        }
    }

    fn config_commit(&self, inner: &ActorInner, state: &ActorState) -> ConfigCommit {
        let candidate =
            config::with_discovery(state.policy.as_deref(), &self.ready.as_ref().unwrap().0);
        let current = state
            .policy
            .as_ref()
            .filter(|current| config::equivalent(current, &candidate));
        ConfigCommit {
            identity: Arc::clone(&inner.identity),
            generation: state
                .policy_generation
                .wrapping_add(u64::from(current.is_none())),
            config: current.map_or(candidate, Arc::clone),
        }
    }

    pub(super) fn commit_locked(
        mut self,
        inner: &Arc<ActorInner>,
        state: &mut ActorState,
        finalize: PreparedFinalization,
    ) -> PreparedCommit {
        let commit = self.config_commit(inner, state);
        let (_, snapshot) = self.ready.take().unwrap();
        let changed = commit.generation != state.policy_generation;
        if changed {
            state.policy_generation = commit.generation;
            state.policy = Some(Arc::clone(&commit.config));
        }
        finalize(&commit);
        state.delivery_suspended = false;
        inner.queue.notify();
        let ticket = self.turn.take().map(|turn| {
            let turn_id = TurnId::generate();
            let ticket = TurnTicket::new(turn_id, Arc::clone(&inner.identity));
            inner.admit_ticket(turn_id, ticket.clone());
            materialize(
                inner,
                state,
                ActorOperation::Turn {
                    id: self.id,
                    input: Box::new(turn.input),
                    event_sender: turn.event_sender,
                    correlation: turn.correlation,
                    ticket: ticket.clone(),
                    policy_constraint: None,
                    identity: ActorWorkIdentity::new(
                        inner.agent_id,
                        ActorWorkKind::Turn {
                            operation_id: self.id,
                            turn_id,
                        },
                        self.identity.presentation_id,
                    ),
                },
                snapshot,
            );
            if let Some(initial) = state
                .instructions
                .get_mut(&turn_id)
                .and_then(|inbox| inbox.initial.as_mut())
            {
                initial.already_displayed = self.turn_displayed;
            }
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
            state.emit(super::ActorEvent::Config(commit.clone()));
        }
        drive_operations(inner, state);
        state.publish_idle(inner);
        inner.policy_changed.notify(usize::MAX);
        result
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
    pub(super) fn begin_prepared_publication(
        &mut self,
        operation: &PreparedOperation,
        publication: CheckpointPublication,
    ) {
        self.prepared_publication = Some(PreparedPublicationState {
            id: operation.id,
            correlation: operation.turn.as_ref().map(|turn| turn.correlation.clone()),
            publication,
        });
        self.pending_publications += 1;
    }

    pub(super) fn end_prepared_publication(&mut self, id: u64) {
        if self
            .prepared_publication
            .as_ref()
            .is_some_and(|pending| pending.id == id)
        {
            self.prepared_publication = None;
            self.pending_publications -= 1;
        }
    }

    /// Cancels the publication of every prepared operation whose turn
    /// correlation `select` accepts; config-only operations pass `None`.
    pub(super) fn cancel_prepared_where(&self, select: impl Fn(Option<&str>) -> bool) {
        for operation in &self.operations {
            if let ActorOperation::Prepared(pending) = operation
                && select(pending.turn.as_ref().map(|turn| turn.correlation.as_str()))
                && let Some(publication) = &pending.publication
            {
                publication.cancel();
            }
        }
        if let Some(pending) = &self.prepared_publication
            && select(pending.correlation.as_deref())
        {
            pending.publication.cancel();
        }
    }

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
        })
        .and_then(|config| {
            inner.validate_mode_config(&config)?;
            Ok(config)
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
            state.publish_idle(inner);
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
                let snapshot = if let (Some(input), Some(prepare)) = (input, prepare) {
                    Some(
                        smol::unblock(move || {
                            catch_unwind(AssertUnwindSafe(|| {
                                let mut snapshot = prepare(&input, Some(&candidate));
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
                state.publish_idle(&inner);
            }
        }
        inner.policy_changed.notify(usize::MAX);
    })
    .detach();
}
