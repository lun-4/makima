use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use crate::manager::WorkReservation;
use futures_lite::FutureExt;
use makima_providers::Message;
use makima_storage::checkpoint::CheckpointPublication;
use makima_storage::session_state::{StateChange, StoredAgentConfig};
use makima_storage::sessions::HistorySnapshot;
use std::collections::BTreeMap;

use crate::agent::{TurnAdmissionSnapshot, resolve_prompt_inputs};
use crate::cancel::{PublicationGuard, PublicationRegistration};
use crate::context::{ContextPolicy, ContextUpdate};
use crate::journal::{
    JournalChange, JournalCommit, JournalIncarnation, SessionJournal, SessionJournalSnapshot,
};
use crate::rewind::RewindBoundary;
use crate::session_checkpoint::stored_agent_config;
use crate::session_options::{FAST_PENDING_MODEL_OPTION_ID, YOLO_OPTION_ID};
use crate::state::StateEdit;
use crate::types::{ActorWorkIdentity, ActorWorkKind};
use crate::{AgentInput, AgentMode, CancelToken, CancelTrigger, EventSender, ModeDef, TurnId};

use super::{
    ActorError, ActorInner, ActorLifecycle, ActorOperation, ActorState, AgentActorHandle,
    ConfigChange, ConfigCommit, EffectiveAgentConfig, PreparedModel, TurnTicket, config,
    drive_operations, lifecycle_error, materialize, preparation_input,
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
    pub boundary: Option<RewindBoundary>,
}

#[derive(Clone)]
pub struct PreparedTransactionSnapshot {
    pub config: Arc<EffectiveAgentConfig>,
    pub history: HistorySnapshot<Message>,
    pub commit: ConfigCommit,
    pub journal: Option<SessionJournalSnapshot>,
    pub boundary: Option<RewindBoundary>,
    pub clear_pending_fast: bool,
    pub options: Option<BTreeMap<String, String>>,
    journal_context: Option<(Arc<SessionJournal>, JournalIncarnation)>,
    context_updates: Vec<ContextUpdate>,
    context_event_sender: Option<EventSender>,
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

pub(super) type PreparedPublicationFinalization =
    Box<dyn FnOnce(&ConfigCommit) -> Option<RewindBoundary> + Send>;

pub struct PreparedTransaction {
    pub publication: CheckpointPublication,
    pub options: Option<BTreeMap<String, String>>,
    pub state_edits: Vec<StateEdit>,
    pub history_changed: bool,
    pub boundary: bool,
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
    pub fn with_boundary(mut self) -> Self {
        self.boundary = true;
        self
    }

    pub fn with_state_edit(mut self, edit: StateEdit) -> Self {
        self.state_edits.push(edit);
        self
    }
    pub(super) async fn publish(
        self,
        mut snapshot: PreparedTransactionSnapshot,
    ) -> Result<PreparedPublicationFinalization, ActorError> {
        if snapshot
            .journal_context
            .as_ref()
            .is_some_and(|(journal, _)| !journal.is_persistent())
            && self.state_edits.is_empty()
            && snapshot.context_updates.is_empty()
            && !self.history_changed
            && !self.boundary
        {
            snapshot.journal_context = None;
        }
        if snapshot.journal_context.is_none()
            && (!self.state_edits.is_empty()
                || !snapshot.context_updates.is_empty()
                || self.boundary)
        {
            return Err(ActorError::HistoryCheckpoint(
                "state edit or boundary requires a session journal".into(),
            ));
        }
        let context_updates = snapshot.context_updates.clone();
        let context_event_sender = snapshot.context_event_sender.clone();
        let mut prepared =
            if let Some((journal, incarnation)) = snapshot.journal_context.take() {
                let mut config = stored_agent_config(&snapshot.config, BTreeMap::new())
                    .map_err(|error| ActorError::InvalidConfig(error.to_string()))?;
                let options = snapshot.options.clone().or(self.options);
                let clear_pending_fast = snapshot.clear_pending_fast;
                let state_edits = self.state_edits;
                let context_updates = snapshot.context_updates.clone();
                let history_changed = self.history_changed;
                let history = history_changed.then(|| snapshot.history.clone());
                let generation = snapshot.commit.generation;
                let boundary = self.boundary;
                let persistent = journal.is_persistent();
                Some(
                    journal
                        .prepare_with(move |current| {
                            let latest =
                                current.state.iter().rev().find_map(|record| {
                                    match &record.change {
                                        StateChange::Config(config)
                                            if record.agent == Some(incarnation.agent) =>
                                        {
                                            Some(config)
                                        }
                                        _ => None,
                                    }
                                });
                            config.options = options.unwrap_or_else(|| {
                                latest
                                    .map(|config| config.options.clone())
                                    .unwrap_or_default()
                            });
                            config.options.remove(YOLO_OPTION_ID);
                            if clear_pending_fast {
                                config.options.remove(FAST_PENDING_MODEL_OPTION_ID);
                            }
                            let changes = (persistent && latest != Some(&config))
                                .then_some(JournalChange {
                                    agent: Some(incarnation.agent),
                                    generation,
                                    change: StateChange::Config(config),
                                })
                                .into_iter()
                                .collect();
                            let history = history
                                .or_else(|| current.histories.get(&incarnation.agent).cloned());
                            JournalCommit {
                                boundary: boundary.then_some((incarnation.agent, generation)),
                                state_edits,
                                context_updates,
                                state_epoch_agents: if history_changed {
                                    [incarnation.agent].into()
                                } else {
                                    Default::default()
                                },
                                incarnations: vec![incarnation.clone()],
                                changes,
                                histories: history
                                    .filter(|_| persistent)
                                    .map(|history| (incarnation.agent, history))
                                    .into_iter()
                                    .collect(),
                                managed_histories: if persistent {
                                    [incarnation.agent].into()
                                } else {
                                    Default::default()
                                },
                                ..Default::default()
                            }
                        })
                        .await
                        .map_err(|error| ActorError::HistoryCheckpoint(error.to_string()))?,
                )
            } else {
                None
            };
        snapshot.journal = prepared
            .as_ref()
            .filter(|prepared| prepared.is_persistent())
            .map(|prepared| prepared.snapshot().clone());
        let boundary = prepared
            .as_ref()
            .filter(|_| self.boundary)
            .and_then(|prepared| {
                let snapshot = prepared.snapshot();
                snapshot.state.last().and_then(|record| {
                    if !matches!(record.change, StateChange::Boundary { .. }) {
                        return None;
                    }
                    record.agent.map(|agent| RewindBoundary {
                        agent,
                        seq: record.seq,
                        epoch: snapshot.epoch,
                    })
                })
            });
        snapshot.boundary = boundary.clone();
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
        if let Some(prepared) = &mut prepared {
            prepared
                .install()
                .map_err(|error| ActorError::HistoryCheckpoint(error.to_string()))?;
        }
        Ok(Box::new(move |commit| {
            let _prepared = prepared;
            (self.finalize)(commit);
            if let Some(sender) = context_event_sender {
                for update in context_updates {
                    update.notify(&sender);
                }
            }
            boundary
        }))
    }
}

pub(super) struct PreparedOperation {
    pub id: u64,
    pub turn: Option<PreparedTurn>,
    work: Vec<WorkReservation>,
    turn_displayed: bool,
    context_policy: Option<ContextPolicy>,
    change: Option<Option<ConfigChange>>,
    clear_pending_fast: bool,
    restore: Option<(PreparedModel, (AgentMode, ModeDef))>,
    force_generation: bool,
    restore_options: Option<BTreeMap<String, String>>,
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

pub(crate) struct ConfigPublicationGuard {
    inner: Arc<ActorInner>,
    owner: Arc<()>,
}

impl Drop for ConfigPublicationGuard {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state
            .config_publication
            .as_ref()
            .is_some_and(|owner| Arc::ptr_eq(owner, &self.owner))
        {
            state.config_publication = None;
            drive_operations(&self.inner, &mut state);
            state.publish_idle(&self.inner);
            self.inner.drained.notify(usize::MAX);
            self.inner.policy_changed.notify(usize::MAX);
            self.inner.queue.notify();
        }
    }
}

impl AgentActorHandle {
    pub fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> Result<Vec<Arc<PublicationGuard>>, ActorError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            publication.cancel();
            return Err(lifecycle_error(state.lifecycle));
        }
        let actor = PublicationGuard::register(publication, &mut state.publication_registrations);
        let mut registrations = vec![actor];
        if let Some(work) = self
            .inner
            .managed_admission
            .as_ref()
            .and_then(|managed| managed.work.as_ref())
        {
            registrations.push(
                work.register_publication(publication)
                    .map_err(|error| ActorError::WorkAdmission(error.to_string()))?,
            );
        }
        Ok(registrations)
    }

    pub(crate) fn reserve_config_publication(&self) -> Result<ConfigPublicationGuard, ActorError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.lifecycle != ActorLifecycle::Open {
            return Err(lifecycle_error(state.lifecycle));
        }
        self.inner.validate_effect_admission()?;
        if state.config_publication.is_some()
            || state.prepared_publication.is_some()
            || state.idle.as_ref().is_some_and(|idle| idle.committing)
        {
            return Err(ActorError::Busy);
        }
        let owner = Arc::new(());
        state.config_publication = Some(Arc::clone(&owner));
        Ok(ConfigPublicationGuard {
            inner: Arc::clone(&self.inner),
            owner,
        })
    }

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
        let work = if turn.is_some() {
            self.inner.reserve_turn_work(&state)?
        } else {
            Vec::new()
        };
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
                work,
                turn_displayed: false,
                context_policy: None,
                change: None,
                clear_pending_fast: false,
                restore: None,
                force_generation: false,
                restore_options: None,
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
    pub fn resolve_restore(
        &self,
        stored: StoredAgentConfig,
        prepared: PreparedModel,
        mode: (AgentMode, ModeDef),
    ) -> Result<(), ActorError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let pending = self.unresolved(&mut state)?;
        pending.restore = Some((prepared, mode));
        pending.change = Some(Some(ConfigChange::Restore(stored)));
        drive_operations(&self.inner, &mut state);
        Ok(())
    }

    pub(crate) fn request_context(&self, policy: ContextPolicy) -> Result<(), ActorError> {
        if self.inner.admission_preparation.is_none() {
            return Err(ActorError::InvalidConfig(
                "context preparation is unavailable".into(),
            ));
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let pending = self.unresolved(&mut state)?;
        if pending.turn.is_some() {
            return Err(ActorError::Busy);
        }
        pending.context_policy = Some(policy);
        Ok(())
    }

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
                    && state.config_publication.is_none()
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
                if state.config_publication.is_none()
                    && state
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
        let context_update = pending
            .ready
            .as_ref()
            .and_then(|(_, snapshot)| snapshot.as_ref())
            .and_then(|snapshot| snapshot.prompt_inputs.as_ref())
            .and_then(|prompt| prompt.context_update());
        let sender = self.inner.context_event_sender.clone().or_else(|| {
            pending
                .turn
                .as_ref()
                .and_then(|turn| turn.event_sender.clone())
        });
        let context_updates = context_update.clone().into_iter().collect();
        let result = pending.commit_with_context_locked(
            &self.inner,
            &mut state,
            context_updates,
            Box::new(move |_| {
                if let Some(update) = context_update
                    && let Some(sender) = sender
                {
                    update.notify(&sender);
                }
                None
            }),
        );
        if result.is_err() {
            drive_operations(&self.inner, &mut state);
            state.publish_idle(&self.inner);
            self.inner.policy_changed.notify(usize::MAX);
        }
        result
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
                    Ok(finalize) => operation.commit_locked(&inner, &mut state, finalize),
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
        if state.config_publication.is_some() {
            return Err(ActorError::PolicyPending);
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
            journal: None,
            boundary: None,
            clear_pending_fast: self.clear_pending_fast,
            options: self.restore_options.clone(),
            journal_context: inner.context_journal(),
            context_updates: self
                .ready
                .as_ref()
                .and_then(|(_, snapshot)| snapshot.as_ref())
                .and_then(|snapshot| snapshot.prompt_inputs.as_ref())
                .and_then(|prompt| prompt.context_update())
                .into_iter()
                .collect(),
            context_event_sender: inner.context_event_sender.clone().or_else(|| {
                self.turn
                    .as_ref()
                    .and_then(|turn| turn.event_sender.clone())
            }),
        }
    }

    fn config_commit(&self, inner: &ActorInner, state: &ActorState) -> ConfigCommit {
        let candidate = &self.ready.as_ref().unwrap().0;
        let current = state
            .policy
            .as_ref()
            .filter(|current| !self.force_generation && config::equivalent(current, candidate));
        ConfigCommit {
            identity: Arc::clone(&inner.identity),
            generation: state
                .policy_generation
                .wrapping_add(u64::from(current.is_none())),
            config: Arc::clone(current.unwrap_or(candidate)),
        }
    }

    pub(super) fn commit_locked(
        self,
        inner: &Arc<ActorInner>,
        state: &mut ActorState,
        finalize: PreparedPublicationFinalization,
    ) -> Result<PreparedCommit, ActorError> {
        self.commit_with_context_locked(inner, state, Vec::new(), finalize)
    }

    fn commit_with_context_locked(
        mut self,
        inner: &Arc<ActorInner>,
        state: &mut ActorState,
        context_updates: Vec<ContextUpdate>,
        finalize: PreparedPublicationFinalization,
    ) -> Result<PreparedCommit, ActorError> {
        if state.config_publication.is_some() {
            return Err(ActorError::PolicyPending);
        }
        let commit = self.config_commit(inner, state);
        if let Some(journal) = &inner.journal {
            let config = stored_agent_config(&commit.config, BTreeMap::new())
                .map_err(|error| ActorError::InvalidConfig(error.to_string()))?;
            let options = self.restore_options.clone().or_else(|| {
                self.clear_pending_fast.then(|| {
                    let mut options = journal
                        .snapshot()
                        .state
                        .iter()
                        .rev()
                        .find_map(|record| match &record.change {
                            StateChange::Config(config) if record.agent == Some(inner.agent_id) => {
                                Some(config.options.clone())
                            }
                            _ => None,
                        })
                        .unwrap_or_default();
                    options.remove(FAST_PENDING_MODEL_OPTION_ID);
                    options
                })
            });
            journal
                .record_config_with_context_for(
                    inner.journal_incarnation.clone().ok_or_else(|| {
                        ActorError::InvalidConfig("journal incarnation is not initialized".into())
                    })?,
                    commit.generation,
                    config,
                    options,
                    context_updates,
                )
                .map_err(|error| ActorError::HistoryCheckpoint(error.to_string()))?;
        } else if !context_updates.is_empty() {
            let (journal, incarnation) = inner.context_journal().ok_or_else(|| {
                ActorError::InvalidConfig("context update requires a session journal".into())
            })?;
            journal
                .commit_for(
                    incarnation,
                    JournalCommit {
                        context_updates,
                        ..Default::default()
                    },
                )
                .map_err(|error| ActorError::HistoryCheckpoint(error.to_string()))?;
        }
        let (_, snapshot) = self.ready.take().unwrap();
        let changed = commit.generation != state.policy_generation;
        if changed {
            state.policy_generation = commit.generation;
            state.policy = Some(Arc::clone(&commit.config));
        }
        state.delivery_suspended = false;
        inner.queue.notify();
        let ticket = self.turn.take().map(|turn| {
            let turn_id = TurnId::generate();
            state.turn_work.insert(turn_id, self.work);
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
        let boundary = finalize(&commit);
        let result = PreparedCommit {
            config: commit.clone(),
            ticket,
            boundary,
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
            if let Some(change) = &change {
                pending.clear_pending_fast = change.clears_pending_fast(current);
                pending.force_generation = matches!(change, ConfigChange::Restore(_));
            }
            let pending_fast = inner.journal.as_ref().is_some_and(|journal| {
                journal
                    .snapshot()
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Config(config) if record.agent == Some(inner.agent_id) => {
                            Some(
                                config
                                    .options
                                    .get(FAST_PENDING_MODEL_OPTION_ID)
                                    .is_some_and(|model| model == &current.model.spec()),
                            )
                        }
                        _ => None,
                    })
                    .unwrap_or(false)
            });
            let change = change.map(|change| {
                if pending_fast && matches!(change, ConfigChange::ToggleFast) {
                    ConfigChange::Patch(super::ConfigPatch {
                        fast: Some(false),
                        ..Default::default()
                    })
                } else {
                    change
                }
            });
            let restore_fast = pending_fast
                && change
                    .as_ref()
                    .is_some_and(|change| change.restores_pending_fast(current));
            let candidate = change.map_or_else(
                || Ok(Arc::clone(current)),
                |change| match (change, pending.restore.take()) {
                    (ConfigChange::Restore(stored), Some((prepared, mode))) => {
                        pending.restore_options = Some(stored.options.clone());
                        ConfigChange::resolved_restore(&stored, prepared, mode, current)
                            .map(Arc::new)
                    }
                    (ConfigChange::Restore(_), None) => Err(ActorError::InvalidConfig(
                        "restore requires resources resolved against current discovery".into(),
                    )),
                    (change, _) => change.apply(current).map(Arc::new),
                },
            )?;
            if restore_fast {
                ConfigChange::Patch(super::ConfigPatch {
                    fast: Some(true),
                    ..Default::default()
                })
                .apply(&candidate)
                .map(Arc::new)
            } else {
                Ok(candidate)
            }
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
    let context_policy = pending.context_policy.clone();
    let input = pending
        .turn
        .as_ref()
        .map(|turn| preparation_input(&turn.input))
        .or_else(|| {
            context_policy
                .is_some()
                .then(|| AgentInput::new(String::new(), Vec::new()))
        });
    let prepare = inner.admission_preparation.clone();
    let readiness = inner.prepared_readiness.clone();
    let cancel = pending.cancel.clone();
    let id = pending.id;
    let access = inner
        .context_journal()
        .map(|(journal, _)| journal.state_access(inner.agent_id, None));
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
                let snapshot = if let Some(mut snapshot) = snapshot {
                    ActorInner::bind_prompt_context(&mut snapshot, access)?;
                    if let Some(policy) = context_policy {
                        let snapshot = resolve_prompt_inputs(snapshot, true, policy).await?;
                        if snapshot.prompt_inputs.as_ref().is_none_or(|prompt| {
                            prompt.root_context().is_none() || prompt.context_update().is_none()
                        }) {
                            return Err(ActorError::InvalidConfig(
                                "root context reload is unavailable".into(),
                            ));
                        }
                        return Ok(Some(snapshot));
                    }
                    Some(snapshot)
                } else if context_policy.is_some() {
                    return Err(ActorError::InvalidConfig(
                        "root context reload is unavailable".into(),
                    ));
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
                    (Some(snapshot), None) => {
                        resolve_prompt_inputs(snapshot, true, ContextPolicy::Admission)
                            .await
                            .map(Some)
                    }
                    (None, _) => Ok(None),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use makima_providers::Message;
    use test_case::test_case;

    use super::ConfigChange;
    use crate::AgentMode;
    use crate::actor::tests::{ScriptedBackend, policy};
    use crate::actor::{ActorError, EffectiveAgentConfig};
    use crate::agent::{
        PromptContext, ResolvedPromptInputs, TurnAdmissionSnapshot, TurnPromptInputs,
        resolve_prompt_inputs,
    };
    use crate::context::{ContextPolicy, root_context};
    use crate::journal::JournalError;
    use crate::manager::{AgentLimits, AgentManagerHandle};
    use crate::tools::TurnToolBindings;

    #[test_case("commit"; "config_and_context_commit_together")]
    #[test_case("invalid_config"; "invalid_config_preserves_context")]
    #[test_case("stale_incarnation"; "stale_incarnation_preserves_context")]
    fn nondurable_context_commit_is_atomic(action: &str) {
        smol::block_on(async {
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let agent = manager
                .create_root(
                    Vec::<Message>::new(),
                    None,
                    Box::new(ScriptedBackend::new()),
                )
                .unwrap();
            let actor = agent.actor().unwrap();
            actor
                .initialize_config(EffectiveAgentConfig::new(policy(false), AgentMode::Build))
                .unwrap();
            let journal = manager.journal();
            let access = journal.state_access(agent.id(), None);
            let mut prompt = TurnPromptInputs {
                cwd: actor
                    .effective_config()
                    .unwrap()
                    .context
                    .cwd()
                    .to_path_buf(),
                instructions: Default::default(),
                mcp_prompt: None,
                resolved: None,
                ready: Some(ResolvedPromptInputs {
                    slots: Arc::default(),
                    slots_error: None,
                    mcp_messages: None,
                }),
                context: Some(PromptContext::default()),
            };
            prompt.bind_context(access.clone()).unwrap();
            let snapshot = resolve_prompt_inputs(
                TurnAdmissionSnapshot {
                    mode_def: None,
                    bindings: Arc::new(TurnToolBindings::default()),
                    prompt_inputs: Some(Arc::new(prompt)),
                    mcp_startup_notice: None,
                },
                true,
                ContextPolicy::Admission,
            )
            .await
            .unwrap();
            let operation = actor.reserve_prepared_operation(None).unwrap();
            operation
                .resolve(Some(ConfigChange::ToggleWorkflow))
                .unwrap();
            operation.wait_ready().await.unwrap();
            {
                let mut state = actor.inner.state.lock().unwrap();
                let pending = state.pending_prepared(operation.id).unwrap();
                let (config, admission) = pending.ready.as_mut().unwrap();
                if action == "invalid_config" {
                    Arc::make_mut(config).mode_def = None;
                }
                *admission = Some(snapshot);
            }
            if action == "stale_incarnation" {
                let incarnation = journal.incarnation(agent.id());
                journal.set_incarnation(agent.id(), incarnation.reservation.wrapping_add(1));
            }
            let before = journal.snapshot();
            assert!(root_context(&access).unwrap().is_none());
            let result = operation.commit();
            match action {
                "commit" => {
                    assert!(result.unwrap().config.config.workflow);
                    assert!(root_context(&access).unwrap().is_some());
                    assert!(actor.effective_config().unwrap().workflow);
                    assert_eq!(journal.snapshot().revision, before.revision + 1);
                }
                "invalid_config" => {
                    assert!(matches!(result, Err(ActorError::InvalidConfig(_))));
                    assert_eq!(journal.snapshot().state, before.state);
                    assert!(!actor.effective_config().unwrap().workflow);
                    assert!(root_context(&access).unwrap().is_none());
                }
                "stale_incarnation" => {
                    assert!(matches!(result, Err(ActorError::HistoryCheckpoint(error))
                        if error == JournalError::StaleIncarnation(agent.id()).to_string()));
                    assert_eq!(journal.snapshot().state, before.state);
                    assert!(!actor.effective_config().unwrap().workflow);
                    assert!(root_context(&access).unwrap().is_none());
                }
                _ => unreachable!(),
            }
            actor.shutdown();
        });
    }
}
