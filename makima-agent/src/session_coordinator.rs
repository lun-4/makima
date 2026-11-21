use makima_domain::LockExt;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use makima_config::ModelPolicy;
use makima_domain::ModelSelection;
use makima_providers::provider::BoxFuture;
use makima_providers::{Message, ThinkingConfig};
use makima_storage::checkpoint::{
    CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication, CheckpointRequest,
    CheckpointVersion, CheckpointWriter,
};
use makima_storage::id::MakimaId;
use makima_storage::session_state::StateChange;
use makima_storage::sessions::HistorySnapshot;
use thiserror::Error;

use crate::actor::{
    ActorError, ConfigChange, ConfigCommit, ConfigPatch, ConfigPublicationGuard,
    EffectiveAgentConfig, PreparedModel, PreparedOperationTicket, PreparedTransaction,
};
use crate::cancel::{CancelToken, CancelTrigger, PublicationGuard, PublicationRegistration};
use crate::config_change::{
    ConfigChangeError, ConfigChangeTicket, QueuedChange, prepare_patch, reserve_in_turn,
    reserve_in_turn_for_actor, waiting,
};
use crate::context::{ContextPolicy, RootContext, root_context};
use crate::journal::{
    JournalChange, JournalCommit, JournalPrepared, JournalPublication, SessionJournal,
    SessionJournalSnapshot,
};
use crate::manager::{AgentRef, ManagerError};
use crate::permissions::{PermissionManager, PermissionSnapshot};
use crate::session_checkpoint::stored_agent_config;
use crate::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, FAST_PENDING_MODEL_OPTION_ID, FreeValueDomain,
    MODEL_OPTION_ID, SessionOptionCategory, SessionOptionDefinition, SessionOptionError,
    SessionOptionOwner, SessionOptionValue, SessionOptions, SessionOptionsCandidate,
    SessionOptionsReservation, SessionOptionsSnapshot, SessionOptionsSubscription,
    THINKING_OPTION_ID, WORKFLOW_OPTION_ID, YOLO_OPTION_ID,
};
use crate::{AgentActorHandle, SessionMailbox};

static DIRECTORY: LazyLock<Mutex<HashMap<MakimaId, DirectoryEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static GENERATION: AtomicU64 = AtomicU64::new(1);
const ACK_MISMATCH: &str = "checkpoint acknowledgement did not match request";
const CHECKPOINT_CANCELLED: &str = "checkpoint cancelled";
const STALE_SNAPSHOT: &str = "session option snapshot changed during validation";
const PARTICIPANT_CLOSED_MESSAGE: &str =
    "session option participant closed before the catalog decision";

#[derive(Default)]
struct CatalogState {
    definitions: BTreeMap<Arc<str>, Vec<SessionOptionDefinition>>,
    sessions: HashMap<MakimaId, CatalogSession>,
    revision: u64,
}

#[derive(Clone)]
struct CatalogSession {
    generation: u64,
    tx: flume::Sender<Operation>,
}

#[derive(Clone, Default)]
pub struct SessionOptionCatalog {
    state: Arc<Mutex<CatalogState>>,
}

#[derive(Clone)]
struct DirectoryEntry {
    handle: SessionCoordinatorHandle,
    mailbox: SessionMailbox,
}

struct PublicationLifetime {
    token: CancelToken,
    trigger: Mutex<Option<CancelTrigger>>,
}

impl PublicationLifetime {
    fn new() -> Self {
        let (trigger, token) = CancelToken::new();
        Self {
            token,
            trigger: Mutex::new(Some(trigger)),
        }
    }

    fn cancel(&self) {
        drop(self.trigger.lock_unpoisoned().take());
    }
}

/// Resolves a spec and prepares its provider without changing host state or
/// awaiting the actor runner.
pub type ModelPreparer = Arc<
    dyn Fn(Arc<str>) -> BoxFuture<'static, Result<PreparedModel, ConfigChangeError>> + Send + Sync,
>;
/// Applies the committed host snapshot synchronously. It must not fail, panic,
/// acquire the actor lock, or enqueue work on that actor.
pub type ConfigProjector = Arc<dyn Fn(&ConfigCommit) + Send + Sync>;
pub type ConfigMutation = Box<
    dyn FnOnce(
            Arc<EffectiveAgentConfig>,
            CheckpointClaim,
        ) -> Result<PreparedTransaction, ConfigChangeError>
        + Send,
>;

#[derive(Clone)]
struct ActorBinding {
    agent: AgentRef,
    prepare_model: ModelPreparer,
    project_config: ConfigProjector,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionCoordinatorError {
    #[error("session already live: {0}")]
    DuplicateSession(MakimaId),
    #[error("session not live: {0}")]
    StaleSession(MakimaId),
    #[error("session is busy: {0}")]
    SessionBusy(MakimaId),
    #[error(transparent)]
    Option(#[from] SessionOptionError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("session options were installed but could not be saved: {0}")]
    OptionsInstalled(CheckpointError),
    #[error("session actor is not bound: {0}")]
    ActorUnbound(MakimaId),
}

#[derive(Clone)]
pub struct CommittedConfigProjection {
    pub generation: u64,
    pub runtime_epoch: u64,
    pub config: Arc<EffectiveAgentConfig>,
}

impl std::fmt::Debug for CommittedConfigProjection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CommittedConfigProjection")
            .field("generation", &self.generation)
            .field("runtime_epoch", &self.runtime_epoch)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionCheckpointKind {
    Config,
    Options,
    History,
}

#[derive(Debug, Clone)]
pub struct SessionCheckpoint {
    pub kind: SessionCheckpointKind,
    pub history_revision: Option<u64>,
    pub history: Option<HistorySnapshot<Message>>,
    pub options: SessionOptionsSnapshot,
    pub config: Option<CommittedConfigProjection>,
    pub journal: Option<SessionJournalSnapshot>,
}

#[derive(Clone)]
pub struct SessionCoordinatorHandle {
    session_id: MakimaId,
    generation: u64,
    tx: flume::Sender<Operation>,
    read: SessionReadHandle,
    catalog: SessionOptionCatalog,
    publication_lifetime: Arc<PublicationLifetime>,
}

pub struct ContextReloadTicket {
    coordinator: SessionCoordinatorHandle,
    actor: AgentActorHandle,
    validate: Box<dyn Fn() -> Result<(), ConfigChangeError> + Send + Sync>,
    publication: CheckpointPublication,
    claim: CheckpointClaim,
    queued: Option<QueuedChange>,
    reserved: Option<(PreparedOperationTicket, SessionTransactionReservation)>,
    _caller: PublicationGuard,
    _actor_registrations: Vec<Arc<PublicationGuard>>,
    _registration: PublicationRegistration,
}

impl ContextReloadTicket {
    fn validate(&self) -> Result<(), ConfigChangeError> {
        if self.publication.is_cancelled() {
            return Err(ActorError::PolicyCancelled.into());
        }
        self.coordinator.ensure_live()?;
        (self.validate)()?;
        let binding = self.coordinator.actor_binding()?;
        binding.agent.validate_live()?;
        if !binding.agent.actor()?.same_actor(&self.actor) {
            return Err(ActorError::PolicyCancelled.into());
        }
        Ok(())
    }

    fn try_reserve(&mut self) -> Result<(), ConfigChangeError> {
        self.validate()?;
        let operation = self.actor.reserve_prepared_operation(None)?;
        let reservation = self
            .coordinator
            .reserve_transaction(self.publication.clone())?;
        operation.register_publication(&self.publication)?;
        self.reserved = Some((operation, reservation));
        Ok(())
    }

    pub async fn commit(self) -> Result<(), ConfigChangeError> {
        self.commit_context(ContextPolicy::Reload).await.map(|_| ())
    }

    async fn commit_context(
        mut self,
        policy: ContextPolicy,
    ) -> Result<Option<Arc<RootContext>>, ConfigChangeError> {
        self.validate()?;
        if let Some(place) = &self.queued {
            place.wait_turn().await;
            loop {
                match self.try_reserve() {
                    Err(error) if error.is_busy() => error.wait_released(&self.actor).await,
                    result => {
                        result?;
                        break;
                    }
                }
            }
            self.queued = None;
        }
        self.validate()?;
        let (operation, reservation) = self.reserved.take().ok_or(ActorError::PolicyCancelled)?;
        if matches!(policy, ContextPolicy::Admission) {
            operation.wait_predecessor().await?;
            self.validate()?;
            let access = self.coordinator.actor_binding()?.agent.state_access();
            if let Some(context) = root_context(&access)
                .map_err(|error| ConfigChangeError::Rejected(error.to_string().into()))?
            {
                return Ok(Some(context));
            }
        }
        operation.request_context(policy)?;
        operation.resolve(None)?;
        operation.wait_ready().await?;
        self.validate()?;
        let transaction =
            reservation.prepare(operation.ready_config()?, self.claim.clone(), false)?;
        operation.commit_durable(transaction).await?;
        Ok(None)
    }
}

pub struct SessionTransactionReservation {
    coordinator: SessionCoordinatorHandle,
    publication: CheckpointPublication,
    reservation: Option<SessionOptionsReservation>,
    registration: Option<PublicationRegistration>,
}

impl Drop for SessionTransactionReservation {
    fn drop(&mut self) {
        if self.reservation.is_some() {
            self.publication.cancel();
        }
    }
}

impl SessionTransactionReservation {
    /// Fails before taking the option reservation, so a failed prepare still
    /// cancels the publication when the reservation drops.
    pub fn prepare(
        mut self,
        config: Arc<EffectiveAgentConfig>,
        claim: CheckpointClaim,
        history_changed: bool,
    ) -> Result<PreparedTransaction, SessionCoordinatorError> {
        let coordinator = &self.coordinator;
        let binding = coordinator.actor_binding()?;
        let mut candidate = actor_options(&coordinator.read, &config)?;
        if let Some(binding) = coordinator.read.binding.lock_unpoisoned().as_ref() {
            let journal = binding.agent.manager().journal();
            let current = journal.snapshot();
            if let Some(saved) =
                current
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Config(config) if record.agent == Some(binding.agent.id()) => {
                            Some(config)
                        }
                        _ => None,
                    })
            {
                candidate.set_pending_fast(
                    saved
                        .options
                        .get(FAST_PENDING_MODEL_OPTION_ID)
                        .map(String::as_str),
                );
            }
        }
        let candidate = Arc::new(Mutex::new(candidate));
        let options = SessionOptions::candidate_snapshot(&candidate.lock_unpoisoned());
        let publish_candidate = Arc::clone(&candidate);
        let publish_read = coordinator.read.clone();
        let session_id = coordinator.session_id;
        let runtime_epoch = coordinator.generation;
        let reservation = self
            .reservation
            .take()
            .expect("transaction owns option reservation");
        let registration = Arc::new(
            self.registration
                .take()
                .expect("transaction owns publication registration"),
        );
        let final_registration = Arc::clone(&registration);
        Ok(PreparedTransaction {
            state_edits: Vec::new(),
            publication: self.publication.clone(),
            options: Some(options.config_values()),
            history_changed,
            publish: Box::new(move |snapshot, publication| {
                let registration = registration;
                if !Arc::ptr_eq(&config, &snapshot.config) {
                    return Box::pin(async {
                        Err(
                            "transaction configuration does not match prepared actor operation"
                                .into(),
                        )
                    });
                }
                if snapshot.clear_pending_fast {
                    publish_candidate.lock_unpoisoned().clear_pending_fast();
                }
                let options =
                    SessionOptions::candidate_snapshot(&publish_candidate.lock_unpoisoned());
                let history = history_changed.then_some(snapshot.history);
                let (version, history_revision) =
                    publish_read.next_version(options.version, history.is_some());
                let install = snapshot.install.clone();
                let claim = Arc::clone(&claim);
                let claim: CheckpointClaim = Arc::new(move |publication| {
                    claim(publication)?;
                    if let Some(install) = &install {
                        install().map_err(Arc::from)?;
                    }
                    Ok(())
                });
                let checkpoint = publish_read.checkpoint_writer().checkpoint_guarded(
                    CheckpointRequest {
                        session_id,
                        version,
                        snapshot: Arc::new(SessionCheckpoint {
                            kind: SessionCheckpointKind::Config,
                            journal: snapshot.journal,
                            history_revision,
                            history,
                            options,
                            config: Some(CommittedConfigProjection {
                                generation: snapshot.commit.generation,
                                runtime_epoch,
                                config: snapshot.commit.config,
                            }),
                        }),
                    },
                    publication,
                    claim,
                );
                Box::pin(async move {
                    let _registration = registration;
                    verify_ack(session_id, version, checkpoint)
                        .await
                        .map_err(|error| error.to_string())
                })
            }),
            finalize: Box::new(move |commit| {
                let _registration = final_registration;
                (binding.project_config)(commit);
                reservation.commit(candidate.lock_unpoisoned().clone());
            }),
        })
    }

    pub fn into_config_mutation(self) -> ConfigMutation {
        Box::new(move |config, claim| Ok(self.prepare(config, claim, false)?))
    }
}

pub struct PreparedSessionCoordinator {
    handle: SessionCoordinatorHandle,
    catalog_revision: u64,
    mailbox: SessionMailbox,
    rx: flume::Receiver<Operation>,
}

#[derive(Clone)]
pub struct SessionReadHandle {
    session_id: MakimaId,
    options: Arc<SessionOptions>,
    state: Arc<Mutex<CoordinatorState>>,
    binding: Arc<Mutex<Option<ActorBinding>>>,
    permissions: Arc<Mutex<Option<Arc<PermissionManager>>>>,
    checkpoint: Arc<Mutex<Arc<dyn CheckpointWriter<SessionCheckpoint>>>>,
    model_policy: Arc<ModelPolicy>,
}

struct CoordinatorState {
    checkpoint_revision: u64,
    history_revision: u64,
}

pub struct SessionCoordinatorParams {
    pub session_id: MakimaId,
    pub catalog: SessionOptionCatalog,
    pub definitions: Vec<SessionOptionDefinition>,
    pub persisted_options: BTreeMap<String, String>,
    pub model_policy: Arc<ModelPolicy>,
    pub checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    pub mailbox: SessionMailbox,
}

struct OptionsAdmission {
    actor: Option<AgentActorHandle>,
    validate: Arc<dyn Fn() -> Result<(), ConfigChangeError> + Send + Sync>,
    publication: CheckpointPublication,
    claim: CheckpointClaim,
}

struct OptionsPublication {
    journal: Option<JournalPrepared>,
    _permit: Option<JournalPublication>,
    permissions: Option<(Arc<PermissionManager>, PermissionSnapshot)>,
    _operation: ConfigPublicationGuard,
}

struct PreparedPluginOptions {
    session_id: MakimaId,
    generation: u64,
    publication_lifetime: Arc<PublicationLifetime>,
    previous: SessionOptionsSnapshot,
    candidate: SessionOptionsCandidate,
    reservation: Arc<SessionOptionsReservation>,
    read: SessionReadHandle,
    operation: Arc<Mutex<Option<ConfigPublicationGuard>>>,
}

struct CatalogPublication {
    participants: Vec<usize>,
    prepared: JournalPrepared,
}

enum PluginOptionDecision {
    Commit(flume::Sender<Result<(), SessionCoordinatorError>>),
    Complete(flume::Sender<Result<(), SessionCoordinatorError>>),
    Abort(flume::Sender<Result<(), SessionCoordinatorError>>),
}

enum Operation {
    SetOption {
        actor: Option<AgentActorHandle>,
        id: Arc<str>,
        value: Arc<str>,
        reservation: SessionOptionsReservation,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
        reply: flume::Sender<Result<SessionOptionsSnapshot, SessionCoordinatorError>>,
    },
    ToggleYolo {
        actor: AgentActorHandle,
        validate: Arc<dyn Fn() -> Result<(), ConfigChangeError> + Send + Sync>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
        reservation: SessionOptionsReservation,
        reply: flume::Sender<Result<(bool, SessionOptionsSnapshot), ConfigChangeError>>,
    },
    UpdateModelValues {
        specs: Vec<Arc<str>>,
        reservation: SessionOptionsReservation,
        reply: flume::Sender<Result<SessionOptionsSnapshot, SessionCoordinatorError>>,
    },
    PreparePluginOptions {
        plugin: Arc<str>,
        definitions: Vec<SessionOptionDefinition>,
        reservation: Arc<SessionOptionsReservation>,
        prepared: flume::Sender<Result<PreparedPluginOptions, SessionCoordinatorError>>,
        decision: flume::Receiver<PluginOptionDecision>,
    },
    Close {
        reply: flume::Sender<()>,
    },
    #[cfg(test)]
    Barrier(flume::Sender<()>),
}

pub fn publication_claim() -> CheckpointClaim {
    Arc::new(|publication| {
        if publication.try_publish() {
            Ok(())
        } else {
            Err(Arc::from(CHECKPOINT_CANCELLED))
        }
    })
}

fn actor_options(
    read: &SessionReadHandle,
    config: &EffectiveAgentConfig,
) -> Result<SessionOptionsCandidate, SessionCoordinatorError> {
    let boolean = |value| if value { ENABLED_VALUE } else { DISABLED_VALUE };
    Ok(read
        .options
        .prepare_actor_config(
            &config.model.spec(),
            boolean(config.fast),
            &config.thinking.to_string(),
            boolean(config.workflow),
        )?
        .unwrap_or_else(|| read.options.unchanged_candidate()))
}

impl SessionOptionCatalog {
    /// Every plugin's definitions, in plugin order.
    pub fn definitions(&self) -> Vec<SessionOptionDefinition> {
        self.state
            .lock_unpoisoned()
            .definitions
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn plugin_definitions(&self, plugin: &str) -> Vec<SessionOptionDefinition> {
        self.state
            .lock_unpoisoned()
            .definitions
            .get(plugin)
            .cloned()
            .unwrap_or_default()
    }

    fn unregister(&self, session_id: MakimaId, generation: u64) {
        let mut state = self.state.lock_unpoisoned();
        if state
            .sessions
            .get(&session_id)
            .is_some_and(|session| session.generation == generation)
        {
            state.sessions.remove(&session_id);
        }
    }

    pub async fn replace_plugin_options(
        &self,
        plugin: impl Into<Arc<str>>,
        definitions: Vec<SessionOptionDefinition>,
    ) -> Result<Vec<(MakimaId, SessionOptionsSnapshot)>, SessionCoordinatorError> {
        self.replace_plugin_options_with_validator(plugin, definitions, |_, _, _| async { Ok(()) })
            .await
    }

    pub async fn replace_plugin_options_with_validator<F, Fut>(
        &self,
        plugin: impl Into<Arc<str>>,
        definitions: Vec<SessionOptionDefinition>,
        validator: F,
    ) -> Result<Vec<(MakimaId, SessionOptionsSnapshot)>, SessionCoordinatorError>
    where
        F: Fn(MakimaId, Arc<str>, Arc<str>) -> Fut,
        Fut: Future<Output = Result<(), SessionCoordinatorError>>,
    {
        let plugin = plugin.into();
        let (_invocation, invocation) = CancelToken::new();
        let publication = CheckpointPublication::default();
        let registration = invocation.register_publication(&publication);
        let mut prepared: Vec<PluginOptionParticipant> = Vec::new();
        let revision = loop {
            let mut sessions = self
                .state
                .lock_unpoisoned()
                .sessions
                .iter()
                .map(|(session_id, session)| (*session_id, session.generation, session.tx.clone()))
                .collect::<Vec<_>>();
            sessions
                .sort_by_key(|(session_id, generation, _)| (session_id.to_string(), *generation));
            let current = sessions
                .iter()
                .map(|(session_id, generation, _)| (*session_id, *generation))
                .collect::<HashMap<_, _>>();
            let (stale, retained): (Vec<_>, Vec<_>) =
                prepared.into_iter().partition(|(staged, _)| {
                    current.get(&staged.session_id) != Some(&staged.generation)
                });
            prepared = retained;
            abort_plugin_options(
                stale
                    .into_iter()
                    .filter(|(_, decision)| !decision.is_disconnected())
                    .collect(),
            )
            .await?;
            for (session_id, generation, tx) in sessions {
                if prepared.iter().any(|(staged, _)| {
                    staged.session_id == session_id && staged.generation == generation
                }) {
                    continue;
                }
                let reservation =
                    SessionCoordinatorHandle::resolve(session_id).and_then(|handle| {
                        if handle.generation != generation {
                            return Err(SessionCoordinatorError::StaleSession(session_id));
                        }
                        handle.reserve_options()
                    });
                let reservation = match reservation {
                    Ok(reservation) => Arc::new(reservation),
                    Err(error) => {
                        abort_plugin_options(prepared).await?;
                        return Err(error);
                    }
                };
                let (prepared_tx, prepared_rx) = flume::bounded(1);
                let (decision_tx, decision_rx) = flume::bounded(1);
                if tx
                    .send_async(Operation::PreparePluginOptions {
                        plugin: Arc::clone(&plugin),
                        definitions: definitions.clone(),
                        reservation,
                        prepared: prepared_tx,
                        decision: decision_rx,
                    })
                    .await
                    .is_err()
                {
                    abort_plugin_options(prepared).await?;
                    return Err(SessionCoordinatorError::StaleSession(session_id));
                }
                let staged = match prepared_rx.recv_async().await {
                    Ok(Ok(staged)) => staged,
                    Ok(Err(error)) => {
                        abort_plugin_options(prepared).await?;
                        return Err(error);
                    }
                    Err(_) => {
                        abort_plugin_options(prepared).await?;
                        return Err(SessionCoordinatorError::StaleSession(session_id));
                    }
                };
                if let Err(error) = validate_plugin_options(&plugin, &staged, &validator).await {
                    prepared.push((staged, decision_tx));
                    if let Err(abort_error) = abort_plugin_options(prepared).await {
                        tracing::warn!(%abort_error, "failed to abort session option validation");
                    }
                    return Err(error);
                }
                prepared.push((staged, decision_tx));
            }
            prepared.sort_by_key(|(staged, _)| (staged.session_id.to_string(), staged.generation));
            let commit = {
                let directory = DIRECTORY.lock_unpoisoned();
                let state = self.state.lock_unpoisoned();
                let participants = prepared
                    .iter()
                    .map(|(staged, _)| (staged.session_id, staged.generation))
                    .collect::<HashMap<_, _>>();
                let current = state
                    .sessions
                    .iter()
                    .map(|(session_id, session)| (*session_id, session.generation))
                    .collect::<HashMap<_, _>>();
                if prepared.iter().any(|(staged, _)| {
                    staged.publication_lifetime.token.is_cancelled()
                        && directory
                            .get(&staged.session_id)
                            .is_none_or(|entry| entry.handle.generation == staged.generation)
                }) {
                    Some(Err(SessionOptionError::StaleHandle(Arc::from(
                        PARTICIPANT_CLOSED_MESSAGE,
                    ))))
                } else if participants != current {
                    None
                } else if prepared
                    .iter()
                    .any(|(_, decision)| decision.is_disconnected())
                {
                    Some(Err(SessionOptionError::StaleHandle(Arc::from(
                        PARTICIPANT_CLOSED_MESSAGE,
                    ))))
                } else {
                    Some(Ok(state.revision))
                }
            };
            match commit {
                None => continue,
                Some(Ok(committed)) => break committed,
                Some(Err(error)) => {
                    abort_plugin_options(prepared).await?;
                    return Err(error.into());
                }
            }
        };
        let catalog = self.clone();
        let (finished, response) = flume::bounded(1);
        smol::spawn(async move {
            let _registration = registration;
            let result = finish_plugin_options(
                catalog,
                plugin,
                definitions,
                prepared,
                revision,
                invocation,
                publication,
            )
            .await;
            let _ = finished.send(result);
        })
        .detach();
        response.recv_async().await.unwrap_or_else(|_| {
            Err(
                SessionOptionError::CallbackFailed(Arc::from("catalog decision task stopped"))
                    .into(),
            )
        })
    }
}

impl SessionCoordinatorHandle {
    pub fn register(params: SessionCoordinatorParams) -> Result<Self, SessionCoordinatorError> {
        PreparedSessionCoordinator::new(params)?.activate()
    }

    pub fn resolve(session_id: MakimaId) -> Result<Self, SessionCoordinatorError> {
        DIRECTORY
            .lock_unpoisoned()
            .get(&session_id)
            .map(|entry| entry.handle.clone())
            .ok_or(SessionCoordinatorError::StaleSession(session_id))
    }

    pub fn rebind_checkpoint_writer(
        &self,
        checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    ) -> Result<(), SessionCoordinatorError> {
        self.ensure_live()?;
        *self.read.checkpoint.lock_unpoisoned() = checkpoint;
        Ok(())
    }

    pub fn read(&self) -> SessionReadHandle {
        self.read.clone()
    }
    pub fn session_id(&self) -> MakimaId {
        self.session_id
    }
    pub fn runtime_epoch(&self) -> u64 {
        self.generation
    }

    pub fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> Result<PublicationRegistration, SessionCoordinatorError> {
        let directory = DIRECTORY.lock_unpoisoned();
        let entry = directory
            .get(&self.session_id)
            .filter(|entry| entry.handle.generation == self.generation)
            .ok_or(SessionCoordinatorError::StaleSession(self.session_id))?;
        Ok(entry
            .handle
            .publication_lifetime
            .token
            .register_publication(publication))
    }

    pub fn mailbox(&self) -> Result<SessionMailbox, SessionCoordinatorError> {
        DIRECTORY
            .lock_unpoisoned()
            .get(&self.session_id)
            .filter(|entry| entry.handle.generation == self.generation)
            .map(|entry| entry.mailbox.clone())
            .ok_or(SessionCoordinatorError::StaleSession(self.session_id))
    }

    pub fn bind_permissions(
        &self,
        permissions: Arc<PermissionManager>,
    ) -> Result<(), SessionCoordinatorError> {
        self.ensure_bindable()?;
        let mut bound = self.read.permissions.lock_unpoisoned();
        if let Some(current) = &*bound {
            if !Arc::ptr_eq(current, &permissions) {
                return Err(SessionCoordinatorError::SessionBusy(self.session_id));
            }
        } else {
            *bound = Some(permissions);
        }
        Ok(())
    }

    pub fn permissions(&self) -> Result<Arc<PermissionManager>, SessionCoordinatorError> {
        self.ensure_live()?;
        self.read
            .permissions
            .lock_unpoisoned()
            .clone()
            .ok_or_else(|| {
                SessionCoordinatorError::Checkpoint(CheckpointError::Save {
                    session_id: self.session_id,
                    message: Arc::from("session permissions are not bound"),
                })
            })
    }

    pub fn bind_actor(
        &self,
        agent: AgentRef,
        prepare_model: ModelPreparer,
        project_config: ConfigProjector,
    ) -> Result<(), ConfigChangeError> {
        self.ensure_bindable()?;
        agent.validate_live()?;
        self.bind_actor_config(
            agent.clone(),
            agent
                .actor()?
                .config_snapshot()
                .ok_or_else(|| {
                    ActorError::InvalidConfig("actor configuration is not initialized".into())
                })?
                .config,
            prepare_model,
            project_config,
        )
    }

    fn bind_actor_config(
        &self,
        agent: AgentRef,
        config: Arc<EffectiveAgentConfig>,
        prepare_model: ModelPreparer,
        project_config: ConfigProjector,
    ) -> Result<(), ConfigChangeError> {
        let reservation = self.reserve_options()?;
        let candidate = actor_options(&self.read, &config)?;
        let mut binding = self.read.binding.lock_unpoisoned();
        if binding.is_some() {
            return Err(SessionCoordinatorError::SessionBusy(self.session_id).into());
        }
        *binding = Some(ActorBinding {
            agent,
            prepare_model,
            project_config,
        });
        drop(binding);
        reservation.commit(candidate);
        Ok(())
    }

    fn actor_binding(&self) -> Result<ActorBinding, SessionCoordinatorError> {
        self.ensure_live()?;
        self.read
            .binding
            .lock_unpoisoned()
            .clone()
            .ok_or(SessionCoordinatorError::ActorUnbound(self.session_id))
    }

    fn reserve_options(&self) -> Result<SessionOptionsReservation, SessionCoordinatorError> {
        self.read.options.try_reserve(None).map_err(|error| {
            if matches!(error, SessionOptionError::Busy) {
                SessionCoordinatorError::SessionBusy(self.session_id)
            } else {
                error.into()
            }
        })
    }

    pub fn reserve_transaction(
        &self,
        publication: CheckpointPublication,
    ) -> Result<SessionTransactionReservation, SessionCoordinatorError> {
        self.actor_binding()?;
        let reservation = self.reserve_options()?;
        let registration = self.register_publication(&publication)?;
        Ok(SessionTransactionReservation {
            coordinator: self.clone(),
            publication,
            reservation: Some(reservation),
            registration: Some(registration),
        })
    }

    pub async fn persist_history(
        &self,
        history: HistorySnapshot<Message>,
    ) -> Result<(), SessionCoordinatorError> {
        self.ensure_live()?;
        let binding = self.read.binding.lock_unpoisoned().clone();
        let actor = binding
            .as_ref()
            .and_then(|binding| binding.agent.actor().ok());
        if let Some(actor) = actor
            && let Some(journal) = &actor.inner.journal
            && let Some(binding) = binding
        {
            journal
                .commit_with(|snapshot| {
                    if snapshot.managed_histories.contains(&binding.agent.id())
                        && snapshot
                            .histories
                            .get(&binding.agent.id())
                            .is_some_and(|current| {
                                current.epoch == history.epoch
                                    && Arc::ptr_eq(&current.messages, &history.messages)
                                    && current.compacted == history.compacted
                                    && current.conversation == history.conversation
                            })
                    {
                        return JournalCommit::default();
                    }
                    JournalCommit {
                        histories: [(binding.agent.id(), history.clone())].into(),
                        managed_histories: [binding.agent.id()].into(),
                        ..Default::default()
                    }
                })
                .map_err(|error| CheckpointError::Save {
                    session_id: self.session_id,
                    message: error.to_string().into(),
                })?;
        }
        let options = self.read.options();
        let (version, revision) = self.read.next_version(options.version, true);
        let revision = revision.expect("history checkpoint reserves a revision");
        let checkpoint = self.read.checkpoint_writer().checkpoint(CheckpointRequest {
            session_id: self.session_id,
            version,
            snapshot: Arc::new(SessionCheckpoint {
                kind: SessionCheckpointKind::History,
                journal: None,
                history_revision: Some(revision),
                history: Some(history),
                options,
                config: None,
            }),
        });
        Ok(verify_ack(self.session_id, version, checkpoint).await?)
    }

    fn reserve_actor_change(
        &self,
        publication: CheckpointPublication,
    ) -> Result<(ActorBinding, ConfigChangeTicket<'_>), ConfigChangeError> {
        let binding = self.actor_binding()?;
        let ticket =
            ConfigChangeTicket::reserve(&binding.agent, publication, move |publication| {
                Ok(self
                    .reserve_transaction(publication)?
                    .into_config_mutation())
            })?;
        Ok((binding, ticket))
    }

    /// The option lease, in the bound actor's config FIFO while that actor
    /// lives.
    async fn reserve_options_in_turn(
        &self,
    ) -> Result<SessionOptionsReservation, ConfigChangeError> {
        let binding = self
            .read
            .binding
            .lock_unpoisoned()
            .clone()
            .filter(|binding| binding.agent.actor().is_ok());
        Ok(match binding {
            Some(binding) => {
                reserve_in_turn(&binding.agent, || Ok(self.reserve_options()?)).await?
            }
            None => self.reserve_options()?,
        })
    }

    async fn send_set_option(
        &self,
        id: Arc<str>,
        value: Arc<str>,
        reservation: SessionOptionsReservation,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        let _caller = PublicationGuard::new(publication.clone());
        let actor = self
            .read
            .binding
            .lock_unpoisoned()
            .as_ref()
            .map(|binding| binding.agent.actor())
            .transpose()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send(Operation::SetOption {
                actor,
                id,
                value,
                reservation,
                publication,
                claim,
                reply,
            })
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        Ok(response
            .recv_async()
            .await
            .unwrap_or(Err(SessionCoordinatorError::StaleSession(self.session_id)))?)
    }

    async fn commit_patch(
        &self,
        spec: Option<Arc<str>>,
        patch: ConfigPatch,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        let (binding, ticket) = self.reserve_actor_change(publication)?;
        let policy = &self.read.model_policy;
        ticket
            .commit(async |_, _| {
                let patch =
                    prepare_patch(patch, spec.as_deref(), policy, &binding.prepare_model).await?;
                Ok((claim, ConfigChange::Patch(patch)))
            })
            .await?;
        Ok(self.read.options())
    }

    pub async fn set_option(
        &self,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        self.set_option_if_version(id, value, None).await
    }

    /// {version} fences the snapshot the caller validated against at
    /// dispatch. A set that then waits behind other changes applies to the
    /// state it lands on, checked against the definitions there.
    pub async fn set_option_if_version(
        &self,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
        version: Option<u64>,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        self.set_option_if_version_with_claim(
            id,
            value,
            version,
            CheckpointPublication::default(),
            publication_claim(),
        )
        .await
    }

    pub async fn set_option_if_version_with_claim(
        &self,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
        version: Option<u64>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        let _caller = PublicationGuard::new(publication.clone());
        self.ensure_live()?;
        let _registrations = self
            .read
            .binding
            .lock_unpoisoned()
            .as_ref()
            .map(|binding| {
                binding
                    .agent
                    .actor()?
                    .register_publication(&publication)
                    .map_err(ConfigChangeError::from)
            })
            .transpose()?;
        if version.is_some_and(|version| version != self.read.options().version) {
            return Err(SessionOptionError::StaleHandle(Arc::from(STALE_SNAPSHOT)).into());
        }
        let id = id.into();
        let value = value.into();
        let enabled = value.as_ref() == ENABLED_VALUE;
        let (spec, patch) = match id.as_ref() {
            MODEL_OPTION_ID => (Some(value), ConfigPatch::default()),
            THINKING_OPTION_ID | FAST_OPTION_ID | WORKFLOW_OPTION_ID => {
                self.read.options.prepare_set(&id, &value)?;
                let patch = match id.as_ref() {
                    THINKING_OPTION_ID => ConfigPatch {
                        thinking: Some(
                            value
                                .parse()
                                .map_err(|_| SessionOptionError::InvalidValue { id, value })?,
                        ),
                        ..Default::default()
                    },
                    FAST_OPTION_ID => ConfigPatch {
                        fast: Some(enabled),
                        ..Default::default()
                    },
                    _ => ConfigPatch {
                        workflow: Some(enabled),
                        ..Default::default()
                    },
                };
                (None, patch)
            }
            _ => {
                let reservation = self.reserve_options_in_turn().await?;
                return self
                    .send_set_option(id, value, reservation, publication, claim)
                    .await;
            }
        };
        self.commit_patch(spec, patch, publication, claim).await
    }

    /// Flips {id} on the state the toggle lands on, so a toggle queued behind
    /// other changes flips their result, and two quick toggles cancel out.
    /// Returns whether it is now enabled.
    pub async fn toggle_boolean_option(
        &self,
        id: impl Into<Arc<str>>,
    ) -> Result<(bool, SessionOptionsSnapshot), ConfigChangeError> {
        let id = id.into();
        let change = match id.as_ref() {
            FAST_OPTION_ID => ConfigChange::ToggleFast,
            WORKFLOW_OPTION_ID => ConfigChange::ToggleWorkflow,
            YOLO_OPTION_ID => {
                self.ensure_live()?;
                let reservation = self.reserve_options_in_turn().await?;
                let enabled = self
                    .read
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id == id)
                    .is_some_and(|option| option.current_value.as_ref() == DISABLED_VALUE);
                let value = if enabled {
                    ENABLED_VALUE
                } else {
                    DISABLED_VALUE
                };
                let snapshot = self
                    .send_set_option(
                        id,
                        value.into(),
                        reservation,
                        CheckpointPublication::default(),
                        publication_claim(),
                    )
                    .await?;
                return Ok((enabled, snapshot));
            }
            _ => {
                return Err(SessionOptionError::InvalidValue {
                    id,
                    value: Arc::from("toggle"),
                }
                .into());
            }
        };
        self.ensure_live()?;
        let (_, ticket) = self.reserve_actor_change(CheckpointPublication::default())?;
        let config = ticket.commit_change(change).await?.config;
        let enabled = match id.as_ref() {
            FAST_OPTION_ID => config.settings.fast,
            _ => config.settings.workflow,
        };
        Ok((enabled, self.read.options()))
    }

    pub async fn toggle_yolo_for_actor_with_claim(
        &self,
        actor: AgentActorHandle,
        validate: impl Fn() -> Result<(), ConfigChangeError> + Send + Sync + 'static,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<(bool, SessionOptionsSnapshot), ConfigChangeError> {
        let _caller = PublicationGuard::new(publication.clone());
        let validate: Arc<dyn Fn() -> Result<(), ConfigChangeError> + Send + Sync> =
            Arc::new(validate);
        let reservation = reserve_in_turn_for_actor(actor.clone(), || {
            validate()?;
            Ok(self.reserve_options()?)
        })
        .await?;
        validate()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send(Operation::ToggleYolo {
                actor,
                validate,
                publication,
                claim,
                reservation,
                reply,
            })
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .unwrap_or_else(|_| Err(SessionCoordinatorError::StaleSession(self.session_id).into()))
    }

    pub async fn set_model(
        &self,
        selection: ModelSelection,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        self.set_model_with_claim(
            selection,
            CheckpointPublication::default(),
            publication_claim(),
        )
        .await
    }

    pub async fn set_model_with_claim(
        &self,
        selection: ModelSelection,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        self.commit_patch(
            selection.spec.clone(),
            ConfigPatch::from_model_selection(&selection),
            publication,
            claim,
        )
        .await
    }

    pub fn reserve_context_reload_for_actor(
        &self,
        actor: AgentActorHandle,
        validate: impl Fn() -> Result<(), ConfigChangeError> + Send + Sync + 'static,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<ContextReloadTicket, ConfigChangeError> {
        let caller = PublicationGuard::new(publication.clone());
        self.ensure_live()?;
        validate()?;
        let binding = self.actor_binding()?;
        binding.agent.validate_live()?;
        if !binding.agent.actor()?.same_actor(&actor) {
            return Err(ActorError::PolicyCancelled.into());
        }
        let access = binding.agent.state_access();
        let actor_registrations = actor.register_publication(&publication)?;
        let registration = self.register_publication(&publication)?;
        let mut ticket = ContextReloadTicket {
            coordinator: self.clone(),
            actor: actor.clone(),
            validate: Box::new(move || {
                validate()?;
                crate::context::root_context(&access)
                    .map_err(|error| ConfigChangeError::Rejected(error.to_string().into()))?;
                Ok(())
            }),
            publication,
            claim,
            queued: None,
            reserved: None,
            _caller: caller,
            _actor_registrations: actor_registrations,
            _registration: registration,
        };
        if waiting(&actor).is_empty() {
            match ticket.try_reserve() {
                Err(error) if error.is_busy() => {}
                result => return result.map(|()| ticket),
            }
        }
        ticket.queued = Some(QueuedChange::enqueue(actor));
        Ok(ticket)
    }

    pub async fn ensure_root_context_for_actor(
        &self,
        expected: &AgentActorHandle,
    ) -> Result<Arc<RootContext>, ConfigChangeError> {
        self.ensure_live()?;
        let binding = self.actor_binding()?;
        binding.agent.validate_live()?;
        if !binding.agent.actor()?.same_actor(expected)
            || binding.agent.manager().root_id()? != binding.agent.id()
        {
            return Err(ActorError::PolicyCancelled.into());
        }
        let access = binding.agent.state_access();
        if let Some(context) = root_context(&access)
            .map_err(|error| ConfigChangeError::Rejected(error.to_string().into()))?
        {
            return Ok(context);
        }
        let existing = self
            .reserve_context_reload_for_actor(
                expected.clone(),
                || Ok(()),
                CheckpointPublication::default(),
                publication_claim(),
            )?
            .commit_context(ContextPolicy::Admission)
            .await?;
        if let Some(context) = existing {
            return Ok(context);
        }
        root_context(&access)
            .map_err(|error| ConfigChangeError::Rejected(error.to_string().into()))?
            .ok_or_else(|| {
                ActorError::InvalidConfig("recorded root context is unavailable".into()).into()
            })
    }

    pub async fn reload_context(&self) -> Result<(), ConfigChangeError> {
        let binding = self.actor_binding()?;
        self.reserve_context_reload_for_actor(
            binding.agent.actor()?,
            || Ok(()),
            CheckpointPublication::default(),
            publication_claim(),
        )?
        .commit()
        .await
    }

    pub async fn change_directory(&self, path: PathBuf) -> Result<PathBuf, ConfigChangeError> {
        let (_, ticket) = self.reserve_actor_change(CheckpointPublication::default())?;
        let commit = ticket
            .commit(async |_, predecessor| {
                let canonical = canonical_directory(predecessor.context.cwd(), path).await?;
                Ok((
                    publication_claim(),
                    ConfigChange::WorkingDirectory(canonical),
                ))
            })
            .await?;
        Ok(commit.config.context.cwd().to_path_buf())
    }

    /// Waits its turn like any other option change, so a batch that lands
    /// while a change holds the lease is applied after it, not dropped.
    pub async fn update_model_values(
        &self,
        specs: Vec<Arc<str>>,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        self.ensure_live()?;
        let reservation = self.reserve_options_in_turn().await?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send(Operation::UpdateModelValues {
                specs,
                reservation,
                reply,
            })
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        Ok(response
            .recv_async()
            .await
            .unwrap_or(Err(SessionCoordinatorError::StaleSession(self.session_id)))?)
    }

    fn request_close(&self) -> Result<flume::Receiver<()>, SessionCoordinatorError> {
        let directory = DIRECTORY.lock_unpoisoned();
        let entry = directory
            .get(&self.session_id)
            .filter(|entry| entry.handle.generation == self.generation)
            .ok_or(SessionCoordinatorError::StaleSession(self.session_id))?;
        entry.handle.publication_lifetime.cancel();
        let (reply, response) = flume::bounded(1);
        entry
            .handle
            .tx
            .send(Operation::Close { reply })
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        Ok(response)
    }

    pub async fn close(&self) -> Result<(), SessionCoordinatorError> {
        self.request_close()?
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))
    }

    pub fn retire(&self) {
        if self.ensure_live().is_err() {
            return;
        }
        unregister(self.session_id, self.generation);
        self.catalog.unregister(self.session_id, self.generation);
        let (reply, _) = flume::bounded(1);
        let _ = self.tx.send(Operation::Close { reply });
    }

    fn ensure_bindable(&self) -> Result<(), SessionCoordinatorError> {
        if DIRECTORY
            .lock_unpoisoned()
            .get(&self.session_id)
            .is_some_and(|entry| entry.handle.generation == self.generation)
        {
            Ok(())
        } else {
            Err(SessionCoordinatorError::StaleSession(self.session_id))
        }
    }

    fn ensure_live(&self) -> Result<(), SessionCoordinatorError> {
        if DIRECTORY
            .lock_unpoisoned()
            .get(&self.session_id)
            .is_some_and(|entry| entry.handle.generation == self.generation)
        {
            Ok(())
        } else {
            Err(SessionCoordinatorError::StaleSession(self.session_id))
        }
    }
}

impl PreparedSessionCoordinator {
    pub fn new(params: SessionCoordinatorParams) -> Result<Self, SessionCoordinatorError> {
        let SessionCoordinatorParams {
            session_id,
            catalog,
            mut definitions,
            persisted_options,
            model_policy,
            checkpoint,
            mailbox,
        } = params;
        let catalog_revision = {
            let state = catalog.state.lock_unpoisoned();
            definitions.extend(
                state
                    .definitions
                    .values()
                    .flat_map(|definitions| definitions.iter().cloned()),
            );
            state.revision
        };
        let options = SessionOptions::new(definitions, &persisted_options)?;
        let read = SessionReadHandle {
            session_id,
            options,
            binding: Arc::new(Mutex::new(None)),
            permissions: Arc::new(Mutex::new(None)),
            checkpoint: Arc::new(Mutex::new(checkpoint)),
            model_policy,
            state: Arc::new(Mutex::new(CoordinatorState {
                checkpoint_revision: 0,
                history_revision: 0,
            })),
        };
        let (tx, rx) = flume::unbounded();
        Ok(Self {
            handle: SessionCoordinatorHandle {
                session_id,
                generation: GENERATION.fetch_add(1, Ordering::Relaxed),
                tx,
                read,
                catalog,
                publication_lifetime: Arc::new(PublicationLifetime::new()),
            },
            catalog_revision,
            mailbox,
            rx,
        })
    }

    pub fn activate(self) -> Result<SessionCoordinatorHandle, SessionCoordinatorError> {
        self.activate_inner(None)
    }

    pub fn activate_replacing(
        self,
        current: &SessionCoordinatorHandle,
    ) -> Result<SessionCoordinatorHandle, SessionCoordinatorError> {
        if self.handle.session_id != current.session_id {
            return Err(SessionCoordinatorError::StaleSession(current.session_id));
        }
        self.activate_inner(Some(current.generation))
    }

    fn activate_inner(
        self,
        replaced_generation: Option<u64>,
    ) -> Result<SessionCoordinatorHandle, SessionCoordinatorError> {
        let Self {
            handle,
            catalog_revision,
            mailbox,
            rx,
        } = self;
        let mut directory = DIRECTORY.lock_unpoisoned();
        let retired = match (directory.get(&handle.session_id), replaced_generation) {
            (None, None) => None,
            (Some(entry), Some(generation)) if entry.handle.generation == generation => {
                Some(entry.handle.clone())
            }
            (Some(_), None) => {
                return Err(SessionCoordinatorError::DuplicateSession(handle.session_id));
            }
            _ => return Err(SessionCoordinatorError::StaleSession(handle.session_id)),
        };
        let mut catalog_state = handle.catalog.state.lock_unpoisoned();
        if catalog_revision != catalog_state.revision {
            reconcile_options(&handle.read.options, &catalog_state.definitions)?;
        }
        if let Some(entry) = directory.get(&handle.session_id) {
            entry.handle.publication_lifetime.cancel();
        }
        directory.insert(
            handle.session_id,
            DirectoryEntry {
                handle: handle.clone(),
                mailbox,
            },
        );
        catalog_state.sessions.insert(
            handle.session_id,
            CatalogSession {
                generation: handle.generation,
                tx: handle.tx.clone(),
            },
        );
        drop(catalog_state);
        drop(directory);
        smol::spawn(run(handle.clone(), rx)).detach();
        if let Some(retired) = retired {
            let (reply, _) = flume::bounded(1);
            let _ = retired.tx.send(Operation::Close { reply });
        }
        Ok(handle)
    }
}

impl SessionReadHandle {
    fn checkpoint_writer(&self) -> Arc<dyn CheckpointWriter<SessionCheckpoint>> {
        Arc::clone(&self.checkpoint.lock_unpoisoned())
    }
    pub fn session_id(&self) -> MakimaId {
        self.session_id
    }
    pub fn options(&self) -> SessionOptionsSnapshot {
        let mut snapshot = self.options.snapshot();
        let binding = self.binding.lock_unpoisoned().clone();
        if let Some(binding) = binding {
            let journal = binding.agent.manager().journal();
            let current = journal.snapshot();
            if let Some(config) =
                current
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Config(config) if record.agent == Some(binding.agent.id()) => {
                            Some(config)
                        }
                        _ => None,
                    })
            {
                snapshot.set_pending_fast_model(
                    config
                        .options
                        .get(FAST_PENDING_MODEL_OPTION_ID)
                        .map(String::as_str),
                );
            }
        }
        if let Some(permissions) = self.permissions.lock_unpoisoned().clone() {
            snapshot.set_yolo(permissions.is_yolo());
        }
        snapshot
    }

    fn config(&self) -> Option<Arc<EffectiveAgentConfig>> {
        let binding = self.binding.lock_unpoisoned().clone()?;
        binding.agent.effective_config().ok().flatten()
    }

    /// See [`SessionOptions::project`].
    #[cfg(test)]
    pub fn project_options(
        &self,
        project: impl Fn(&SessionOptionsSnapshot) + Send + Sync + 'static,
    ) {
        self.options.project(project);
    }

    pub fn model(&self) -> Option<Arc<str>> {
        self.config().map(|config| Arc::from(config.model.spec()))
    }
    pub fn cwd(&self) -> Option<PathBuf> {
        self.config()
            .map(|config| config.context.cwd().to_path_buf())
    }

    /// Waits until no change holds the session option lease.
    pub async fn wait_unreserved(&self) {
        self.options.wait_unreserved().await;
    }

    pub fn subscribe(&self) -> SessionOptionsSubscription {
        self.options.subscribe()
    }

    fn next_version(&self, option_version: u64, history: bool) -> (CheckpointVersion, Option<u64>) {
        let mut state = self.state.lock_unpoisoned();
        state.checkpoint_revision += 1;
        let revision = history.then(|| {
            state.history_revision += 1;
            state.history_revision
        });
        (
            CheckpointVersion {
                revision: state.checkpoint_revision,
                epoch: option_version,
            },
            revision,
        )
    }
}

fn reject_operation(operation: Operation, session_id: MakimaId, busy: bool) {
    let error = if busy {
        SessionCoordinatorError::SessionBusy(session_id)
    } else {
        SessionCoordinatorError::StaleSession(session_id)
    };
    match operation {
        Operation::SetOption { reply, .. } | Operation::UpdateModelValues { reply, .. } => {
            let _ = reply.send(Err(error));
        }
        Operation::ToggleYolo { reply, .. } => {
            let _ = reply.send(Err(error.into()));
        }
        Operation::PreparePluginOptions { prepared, .. } => {
            let _ = prepared.send(Err(error));
        }
        Operation::Close { reply } => {
            let _ = reply.send(());
        }
        #[cfg(test)]
        Operation::Barrier(reply) => {
            let _ = reply.send(());
        }
    }
}

async fn run(handle: SessionCoordinatorHandle, rx: flume::Receiver<Operation>) {
    while let Ok(operation) = rx.recv_async().await {
        if handle_operation(&handle, operation, &rx).await.is_break() {
            while let Ok(operation) = rx.try_recv() {
                reject_operation(operation, handle.session_id, false);
            }
            return;
        }
    }
    unregister(handle.session_id, handle.generation);
    handle
        .catalog
        .unregister(handle.session_id, handle.generation);
}

async fn handle_operation(
    ctx: &SessionCoordinatorHandle,
    operation: Operation,
    rx: &flume::Receiver<Operation>,
) -> ControlFlow<()> {
    match operation {
        Operation::SetOption {
            actor,
            id,
            value,
            reservation,
            publication,
            claim,
            reply,
        } => {
            let result = async {
                let _registration = ctx.register_publication(&publication)?;
                let candidate = ctx.read.options.prepare_set(&id, &value)?;
                let candidate = candidate.or_else(|| {
                    (id.as_ref() == YOLO_OPTION_ID
                        && ctx.read.permissions.lock_unpoisoned().as_ref().is_none_or(
                            |permissions| {
                                permissions.snapshot().yolo != Some(value.as_ref() == ENABLED_VALUE)
                            },
                        ))
                    .then(|| ctx.read.options.unchanged_candidate())
                });
                let Some(candidate) = candidate else {
                    return Ok(ctx.read.options());
                };
                let reservation = Arc::new(reservation);
                let commit_reservation = Arc::clone(&reservation);
                checkpoint_options_for_actor(
                    &ctx.read,
                    SessionOptions::candidate_snapshot(&candidate),
                    Some(OptionsAdmission {
                        actor,
                        validate: Arc::new(|| Ok(())),
                        publication,
                        claim,
                    }),
                    id.as_ref() == YOLO_OPTION_ID,
                    Some(Box::new(move || {
                        commit_reservation.install(candidate);
                    })),
                )
                .await?;
                Ok(ctx.read.options())
            }
            .await;
            let _ = reply.send(result);
        }
        Operation::ToggleYolo {
            actor,
            validate,
            publication,
            claim,
            reservation,
            reply,
        } => {
            let result = async {
                validate()?;
                ctx.ensure_live()?;
                let enabled = ctx
                    .read
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == YOLO_OPTION_ID)
                    .is_some_and(|option| option.current_value.as_ref() == DISABLED_VALUE);
                let value = if enabled {
                    ENABLED_VALUE
                } else {
                    DISABLED_VALUE
                };
                let candidate = ctx
                    .read
                    .options
                    .prepare_set(YOLO_OPTION_ID, value)?
                    .unwrap_or_else(|| ctx.read.options.unchanged_candidate());
                let _registration = ctx.register_publication(&publication)?;
                let reservation = Arc::new(reservation);
                let commit_reservation = Arc::clone(&reservation);
                checkpoint_options_for_actor(
                    &ctx.read,
                    SessionOptions::candidate_snapshot(&candidate),
                    Some(OptionsAdmission {
                        actor: Some(actor),
                        validate,
                        publication,
                        claim,
                    }),
                    true,
                    Some(Box::new(move || {
                        commit_reservation.install(candidate);
                    })),
                )
                .await
                .map_err(SessionCoordinatorError::from)?;
                Ok((enabled, ctx.read.options()))
            }
            .await;
            let _ = reply.send(result);
        }
        Operation::UpdateModelValues {
            specs,
            reservation,
            reply,
        } => {
            let _ = reply.send(update_model_values(&ctx.read, reservation, specs).await);
        }
        Operation::PreparePluginOptions {
            plugin,
            definitions,
            reservation,
            prepared,
            decision,
        } => {
            let previous = ctx.read.options();
            let candidate = match ctx
                .read
                .options
                .prepare_replace_plugin(&plugin, definitions)
            {
                Ok(candidate) => {
                    candidate.unwrap_or_else(|| ctx.read.options.unchanged_candidate())
                }
                Err(error) => {
                    drop(reservation);
                    let _ = prepared.send(Err(error.into()));
                    return ControlFlow::Continue(());
                }
            };
            let operation = Arc::new(Mutex::new(None));
            let staged = PreparedPluginOptions {
                session_id: ctx.session_id,
                generation: ctx.generation,
                publication_lifetime: Arc::clone(&ctx.publication_lifetime),
                previous,
                candidate,
                reservation: Arc::clone(&reservation),
                read: ctx.read.clone(),
                operation: Arc::clone(&operation),
            };
            if prepared.send(Ok(staged)).is_err() {
                return ControlFlow::Continue(());
            }
            let mut closing = Vec::new();
            loop {
                let event = futures_lite::future::or(
                    async { Either::Left(decision.recv_async().await) },
                    async { Either::Right(rx.recv_async().await) },
                )
                .await;
                match event {
                    Either::Left(Ok(PluginOptionDecision::Commit(reply))) => {
                        let _ = reply.send(Ok(()));
                    }
                    Either::Left(Ok(
                        PluginOptionDecision::Complete(reply) | PluginOptionDecision::Abort(reply),
                    )) => {
                        drop(operation.lock_unpoisoned().take());
                        drop(reservation);
                        let _ = reply.send(Ok(()));
                        break;
                    }
                    Either::Right(Ok(Operation::Close { reply })) => closing.push(reply),
                    Either::Right(Ok(operation)) => {
                        reject_operation(operation, ctx.session_id, true)
                    }
                    Either::Left(Err(_)) | Either::Right(Err(_)) => break,
                }
            }
            if !closing.is_empty() {
                unregister(ctx.session_id, ctx.generation);
                ctx.catalog.unregister(ctx.session_id, ctx.generation);
                for reply in closing {
                    let _ = reply.send(());
                }
                return ControlFlow::Break(());
            }
        }
        Operation::Close { reply } => {
            unregister(ctx.session_id, ctx.generation);
            ctx.catalog.unregister(ctx.session_id, ctx.generation);
            let _ = reply.send(());
            return ControlFlow::Break(());
        }
        #[cfg(test)]
        Operation::Barrier(reply) => {
            let _ = reply.send(());
        }
    }
    ControlFlow::Continue(())
}

enum Either<L, R> {
    Left(L),
    Right(R),
}

async fn validate_plugin_options<F, Fut>(
    plugin: &str,
    staged: &PreparedPluginOptions,
    validator: &F,
) -> Result<(), SessionCoordinatorError>
where
    F: Fn(MakimaId, Arc<str>, Arc<str>) -> Fut,
    Fut: Future<Output = Result<(), SessionCoordinatorError>>,
{
    let candidate = SessionOptions::candidate_snapshot(&staged.candidate);
    for option in candidate.options.iter() {
        if !matches!(&option.definition.owner, SessionOptionOwner::Plugin { plugin: owner, .. } if owner.as_ref() == plugin)
        {
            continue;
        }
        if staged
            .previous
            .options
            .iter()
            .find(|old| old.definition.id == option.definition.id)
            .is_none_or(|old| old.current_value != option.current_value)
        {
            validator(
                staged.session_id,
                Arc::clone(&option.definition.id),
                Arc::clone(&option.current_value),
            )
            .await?;
        }
    }
    Ok(())
}

fn reconcile_options(
    options: &Arc<SessionOptions>,
    definitions: &BTreeMap<Arc<str>, Vec<SessionOptionDefinition>>,
) -> Result<(), SessionOptionError> {
    let mut plugins = options
        .snapshot()
        .options
        .iter()
        .filter_map(|option| match &option.definition.owner {
            SessionOptionOwner::Plugin { plugin, .. } => Some((Arc::clone(plugin), ())),
            SessionOptionOwner::Builtin => None,
        })
        .collect::<BTreeMap<_, ()>>();
    plugins.extend(definitions.keys().map(|plugin| (Arc::clone(plugin), ())));
    for (plugin, ()) in plugins {
        if let Some(candidate) = options.prepare_replace_plugin(
            &plugin,
            definitions.get(&plugin).cloned().unwrap_or_default(),
        )? {
            options.reserve(&candidate)?.commit(candidate);
        }
    }
    Ok(())
}

type PluginOptionParticipant = (PreparedPluginOptions, flume::Sender<PluginOptionDecision>);

async fn decide_plugin_options(
    decision: &flume::Sender<PluginOptionDecision>,
    session_id: MakimaId,
    make: fn(flume::Sender<Result<(), SessionCoordinatorError>>) -> PluginOptionDecision,
) -> Result<(), SessionCoordinatorError> {
    let (reply, response) = flume::bounded(1);
    if decision.send(make(reply)).is_err() {
        return Err(SessionCoordinatorError::StaleSession(session_id));
    }
    response
        .recv_async()
        .await
        .unwrap_or(Err(SessionCoordinatorError::StaleSession(session_id)))
}

async fn finish_plugin_options(
    catalog: SessionOptionCatalog,
    plugin: Arc<str>,
    definitions: Vec<SessionOptionDefinition>,
    prepared: Vec<PluginOptionParticipant>,
    revision: u64,
    invocation: CancelToken,
    publication: CheckpointPublication,
) -> Result<Vec<(MakimaId, SessionOptionsSnapshot)>, SessionCoordinatorError> {
    let _registrations = prepared
        .iter()
        .map(|(staged, _)| {
            staged
                .publication_lifetime
                .token
                .register_publication(&publication)
        })
        .collect::<Vec<_>>();
    let mut installed = false;
    let result = async {
        for (staged, decision) in &prepared {
            invocation
                .race(decide_plugin_options(
                    decision,
                    staged.session_id,
                    PluginOptionDecision::Commit,
                ))
                .await
                .map_err(|_| {
                    SessionOptionError::CallbackFailed(Arc::from(CHECKPOINT_CANCELLED))
                })??;
        }
        let mut publications = invocation
            .race(prepare_catalog_publications(&prepared))
            .await
            .map_err(|_| SessionOptionError::CallbackFailed(Arc::from(CHECKPOINT_CANCELLED)))??;
        if !invocation.claim_publication(&publication) {
            return Err(SessionOptionError::CallbackFailed(Arc::from(CHECKPOINT_CANCELLED)).into());
        }
        let snapshots = {
            let directory = DIRECTORY.lock_unpoisoned();
            let mut state = catalog.state.lock_unpoisoned();
            if revision != state.revision
                || prepared.len() != state.sessions.len()
                || prepared.iter().any(|(staged, decision)| {
                    staged.publication_lifetime.token.is_cancelled()
                        || decision.is_disconnected()
                        || state
                            .sessions
                            .get(&staged.session_id)
                            .is_none_or(|session| session.generation != staged.generation)
                        || directory
                            .get(&staged.session_id)
                            .is_none_or(|entry| entry.handle.generation != staged.generation)
                })
            {
                return Err(
                    SessionOptionError::StaleHandle(Arc::from(PARTICIPANT_CLOSED_MESSAGE)).into(),
                );
            }
            let candidates = prepared
                .iter()
                .map(|(staged, _)| (staged.reservation.as_ref(), staged.candidate.clone()))
                .collect();
            SessionOptions::commit_batch_unpublished(candidates, || {
                let mut tokens = publications
                    .iter_mut()
                    .map(|group| &mut group.prepared)
                    .collect::<Vec<_>>();
                JournalPrepared::install_batch(&mut tokens).map_err(|error| {
                    SessionOptionError::CallbackFailed(Arc::from(error.to_string()))
                })?;
                if definitions.is_empty() {
                    state.definitions.remove(plugin.as_ref());
                } else {
                    state.definitions.insert(Arc::clone(&plugin), definitions);
                }
                state.revision += 1;
                Ok(())
            })?
        };
        SessionOptions::publish_batch(
            prepared
                .iter()
                .zip(&snapshots)
                .map(|((staged, _), snapshot)| (staged.reservation.as_ref(), snapshot)),
        );
        installed = true;
        let mut save_error = None;
        for (index, ((staged, _), snapshot)) in prepared.iter().zip(&snapshots).enumerate() {
            let journal = publications
                .iter()
                .find(|group| group.participants.contains(&index))
                .map(|group| group.prepared.snapshot().clone());
            if let Err(error) = publish_catalog_options(staged, snapshot.clone(), journal).await {
                save_error.get_or_insert(error);
            }
        }
        match save_error {
            Some(error) => Err(SessionCoordinatorError::OptionsInstalled(error)),
            None => Ok(prepared
                .iter()
                .zip(snapshots)
                .map(|((staged, _), snapshot)| (staged.session_id, snapshot))
                .collect()),
        }
    }
    .await;
    let decision: fn(flume::Sender<Result<(), SessionCoordinatorError>>) -> PluginOptionDecision =
        if installed {
            PluginOptionDecision::Complete
        } else {
            PluginOptionDecision::Abort
        };
    for (staged, sender) in prepared {
        let session_id = staged.session_id;
        drop(staged);
        if let Err(error) = decide_plugin_options(&sender, session_id, decision).await {
            tracing::warn!(%error, %session_id, "catalog participant release failed");
        }
    }
    result
}

async fn prepare_catalog_publications(
    participants: &[PluginOptionParticipant],
) -> Result<Vec<CatalogPublication>, SessionCoordinatorError> {
    let mut groups: BTreeMap<usize, (Arc<SessionJournal>, Vec<usize>)> = BTreeMap::new();
    for (index, (staged, _)) in participants.iter().enumerate() {
        let binding = staged.read.binding.lock_unpoisoned().clone();
        let Some(binding) = binding else { continue };
        let actor = binding.agent.actor().map_err(|error| match error {
            ManagerError::NonLiveAgent(_) => {
                SessionCoordinatorError::SessionBusy(staged.session_id)
            }
            error => SessionOptionError::CallbackFailed(Arc::from(error.to_string())).into(),
        })?;
        let Some(journal) = &actor.inner.journal else {
            continue;
        };
        let operation = actor
            .reserve_config_publication()
            .map_err(|error| match error {
                ActorError::Busy => SessionCoordinatorError::SessionBusy(staged.session_id),
                error => SessionOptionError::CallbackFailed(Arc::from(error.to_string())).into(),
            })?;
        *staged.operation.lock_unpoisoned() = Some(operation);
        groups
            .entry(Arc::as_ptr(journal) as usize)
            .or_insert_with(|| (Arc::clone(journal), Vec::new()))
            .1
            .push(index);
    }
    let mut publications = Vec::with_capacity(groups.len());
    for (_, (journal, indexes)) in groups {
        let mut configs = Vec::with_capacity(indexes.len());
        for &index in &indexes {
            let staged = &participants[index].0;
            let binding = staged
                .read
                .binding
                .lock_unpoisoned()
                .clone()
                .ok_or(SessionCoordinatorError::ActorUnbound(staged.session_id))?;
            let actor = binding.agent.actor().map_err(|error| {
                SessionOptionError::CallbackFailed(Arc::from(error.to_string()))
            })?;
            let config = actor
                .config_snapshot()
                .ok_or(SessionCoordinatorError::ActorUnbound(staged.session_id))?;
            let stored = stored_agent_config(
                &config.config,
                SessionOptions::candidate_snapshot(&staged.candidate).config_values(),
            )
            .map_err(|error| SessionOptionError::CallbackFailed(Arc::from(error.to_string())))?;
            configs.push((
                binding.agent.id(),
                actor.inner.journal_incarnation.clone(),
                config.generation,
                stored,
            ));
        }
        let token = journal
            .prepare_with(|current| {
                let mut commit = JournalCommit::default();
                for (agent, incarnation, generation, mut stored) in configs {
                    commit.incarnations.extend(incarnation);
                    commit.managed_histories.insert(agent);
                    let latest =
                        current
                            .state
                            .iter()
                            .rev()
                            .find_map(|record| match &record.change {
                                StateChange::Config(config) if record.agent == Some(agent) => {
                                    Some(config)
                                }
                                _ => None,
                            });
                    if let Some(config) = latest {
                        let options = stored.options;
                        stored = config.clone();
                        stored.options = options;
                        stored.options.remove(FAST_PENDING_MODEL_OPTION_ID);
                        if let Some(model) = config.options.get(FAST_PENDING_MODEL_OPTION_ID) {
                            stored
                                .options
                                .insert(FAST_PENDING_MODEL_OPTION_ID.into(), model.clone());
                        }
                    }
                    if latest != Some(&stored) {
                        commit.changes.push(JournalChange {
                            agent: Some(agent),
                            generation,
                            change: StateChange::Config(stored),
                        });
                    }
                }
                commit
            })
            .await
            .map_err(|error| SessionOptionError::CallbackFailed(Arc::from(error.to_string())))?;
        publications.push(CatalogPublication {
            participants: indexes,
            prepared: token,
        });
    }
    Ok(publications)
}

async fn publish_catalog_options(
    staged: &PreparedPluginOptions,
    options: SessionOptionsSnapshot,
    journal: Option<SessionJournalSnapshot>,
) -> Result<(), CheckpointError> {
    let (version, _) = staged.read.next_version(options.version, false);
    let checkpoint = staged
        .read
        .checkpoint_writer()
        .checkpoint(CheckpointRequest {
            session_id: staged.session_id,
            version,
            snapshot: Arc::new(SessionCheckpoint {
                kind: SessionCheckpointKind::Options,
                history_revision: None,
                history: None,
                options,
                config: None,
                journal,
            }),
        });
    verify_ack(staged.session_id, version, checkpoint).await
}

async fn abort_plugin_options(
    prepared: Vec<PluginOptionParticipant>,
) -> Result<(), SessionCoordinatorError> {
    let mut responses = Vec::with_capacity(prepared.len());
    let mut first_error = None;
    for (staged, decision) in prepared {
        let session_id = staged.session_id;
        drop(staged);
        let (reply, response) = flume::bounded(1);
        if decision.send(PluginOptionDecision::Abort(reply)).is_err() {
            first_error.get_or_insert(SessionCoordinatorError::StaleSession(session_id));
        } else {
            responses.push((session_id, response));
        }
    }
    for (session_id, response) in responses {
        match response.recv_async().await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert(SessionCoordinatorError::StaleSession(session_id));
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn verify_ack(
    session_id: MakimaId,
    version: CheckpointVersion,
    checkpoint: CheckpointFuture,
) -> Result<(), CheckpointError> {
    let ack = checkpoint.await?;
    if ack.session_id == session_id && ack.version == version {
        Ok(())
    } else {
        Err(CheckpointError::Save {
            session_id,
            message: Arc::from(ACK_MISMATCH),
        })
    }
}

fn install_options_publication(
    publication: &mut Option<OptionsPublication>,
    session_id: MakimaId,
) -> Result<(), CheckpointError> {
    if let Some(publication) = publication {
        if let Some(journal) = &mut publication.journal {
            journal.install().map_err(|error| CheckpointError::Save {
                session_id,
                message: Arc::from(error.to_string()),
            })?;
        }
        if let Some((permissions, snapshot)) = &publication.permissions {
            permissions.restore(snapshot.clone());
        }
    }
    Ok(())
}

async fn checkpoint_options_for_actor(
    read: &SessionReadHandle,
    options: SessionOptionsSnapshot,
    admission: Option<OptionsAdmission>,
    explicit_yolo: bool,
    commit: Option<Box<dyn FnOnce() + Send>>,
) -> Result<(), CheckpointError> {
    let yolo = options.enabled(YOLO_OPTION_ID);
    let permissions = read.permissions.lock_unpoisoned().clone();
    if explicit_yolo && permissions.is_none() {
        return Err(CheckpointError::Save {
            session_id: read.session_id,
            message: Arc::from("session permissions are not bound"),
        });
    }
    let actor = if let Some(admission) = &admission {
        (admission.validate)().map_err(|error| CheckpointError::Save {
            session_id: read.session_id,
            message: Arc::from(error.to_string()),
        })?;
        admission.actor.clone().or_else(|| {
            read.binding
                .lock_unpoisoned()
                .as_ref()
                .and_then(|binding| binding.agent.actor().ok())
        })
    } else {
        read.binding
            .lock_unpoisoned()
            .clone()
            .map(|binding| binding.agent.actor())
            .transpose()
            .map_err(|error| CheckpointError::Save {
                session_id: read.session_id,
                message: Arc::from(error.to_string()),
            })?
    };
    let _registrations = if let (Some(actor), Some(admission)) = (&actor, &admission) {
        actor
            .register_publication(&admission.publication)
            .map_err(|error| CheckpointError::Save {
                session_id: read.session_id,
                message: Arc::from(error.to_string()),
            })?
    } else {
        Vec::new()
    };
    let mut publication = None;
    if let Some(actor) = &actor
        && let Some(journal) = &actor.inner.journal
    {
        let operation =
            actor
                .reserve_config_publication()
                .map_err(|error| CheckpointError::Save {
                    session_id: read.session_id,
                    message: Arc::from(error.to_string()),
                })?;
        let permit = journal.reserve_publication().await;
        let permission_snapshot =
            permissions
                .as_ref()
                .filter(|_| explicit_yolo)
                .and_then(|permissions| {
                    let mut snapshot = permissions.snapshot();
                    if snapshot.yolo == Some(yolo) {
                        return None;
                    }
                    snapshot.yolo = Some(yolo);
                    Some(snapshot)
                });
        let rules = permission_snapshot
            .as_ref()
            .map(|snapshot| (snapshot.stored_rules(), snapshot.yolo));
        let config = actor
            .config_snapshot()
            .ok_or_else(|| CheckpointError::Save {
                session_id: read.session_id,
                message: Arc::from("actor configuration is not initialized"),
            })?;
        let mut stored =
            stored_agent_config(&config.config, options.config_values()).map_err(|error| {
                CheckpointError::Save {
                    session_id: read.session_id,
                    message: Arc::from(error.to_string()),
                }
            })?;
        let incarnation = actor.inner.journal_incarnation.clone();
        let agent = actor.agent_id();
        let prepared = permit
            .prepare_with(move |current| {
                let latest = current
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Config(config) if record.agent == Some(agent) => Some(config),
                        _ => None,
                    });
                if let Some(config) = latest {
                    let options = stored.options;
                    stored = config.clone();
                    stored.options = options;
                    stored.options.remove(FAST_PENDING_MODEL_OPTION_ID);
                    if let Some(model) = config.options.get(FAST_PENDING_MODEL_OPTION_ID) {
                        stored
                            .options
                            .insert(FAST_PENDING_MODEL_OPTION_ID.into(), model.clone());
                    }
                }
                let mut changes: Vec<_> = (latest != Some(&stored))
                    .then_some(JournalChange {
                        agent: Some(agent),
                        generation: config.generation,
                        change: StateChange::Config(stored),
                    })
                    .into_iter()
                    .collect();
                if let Some((rules, yolo)) = rules {
                    let latest_rules = current.state.iter().rev().find_map(|record| match &record
                        .change
                    {
                        StateChange::Rules { rules, yolo } if record.agent.is_none() => {
                            Some((rules, *yolo))
                        }
                        _ => None,
                    });
                    if latest_rules != Some((&rules, yolo)) {
                        changes.push(JournalChange {
                            agent: None,
                            generation: 0,
                            change: StateChange::Rules { rules, yolo },
                        });
                    }
                }
                JournalCommit {
                    incarnations: incarnation.into_iter().collect(),
                    changes,
                    managed_histories: [agent].into(),
                    ..Default::default()
                }
            })
            .await
            .map_err(|error| CheckpointError::Save {
                session_id: read.session_id,
                message: Arc::from(error.to_string()),
            })?;
        publication = Some(OptionsPublication {
            journal: Some(prepared),
            _permit: None,
            permissions: permissions.clone().zip(permission_snapshot),
            _operation: operation,
        });
    }
    if publication.is_none() && explicit_yolo {
        let permissions = permissions.ok_or_else(|| CheckpointError::Save {
            session_id: read.session_id,
            message: Arc::from("session permissions are not bound"),
        })?;
        let operation = actor
            .as_ref()
            .map(|actor| actor.reserve_config_publication())
            .transpose()
            .map_err(|error| CheckpointError::Save {
                session_id: read.session_id,
                message: Arc::from(error.to_string()),
            })?;
        let journal = actor
            .as_ref()
            .and_then(|actor| actor.inner.managed_admission.as_ref())
            .and_then(|admission| admission.manager.upgrade())
            .map(|manager| Arc::clone(&manager.journal));
        let permit = match journal {
            Some(journal) => Some(journal.reserve_publication().await),
            None => None,
        };
        let mut snapshot = permissions.snapshot();
        snapshot.yolo = Some(yolo);
        if let Some(operation) = operation {
            publication = Some(OptionsPublication {
                journal: None,
                _permit: permit,
                permissions: Some((permissions, snapshot)),
                _operation: operation,
            });
        } else {
            return Err(CheckpointError::Save {
                session_id: read.session_id,
                message: Arc::from("session actor is not bound"),
            });
        }
    }
    let (version, _) = read.next_version(options.version, false);
    let request = CheckpointRequest {
        session_id: read.session_id,
        version,
        snapshot: Arc::new(SessionCheckpoint {
            kind: SessionCheckpointKind::Options,
            journal: publication.as_ref().and_then(|publication| {
                publication
                    .journal
                    .as_ref()
                    .map(|journal| journal.snapshot().clone())
            }),
            history_revision: None,
            history: None,
            options,
            config: None,
        }),
    };
    let staged = Arc::new(Mutex::new((publication, commit)));
    let install = Arc::clone(&staged);
    let original_claim = admission
        .as_ref()
        .map(|admission| Arc::clone(&admission.claim));
    let session_id = read.session_id;
    let claim: CheckpointClaim = Arc::new(move |publication| {
        if let Some(claim) = &original_claim {
            claim(publication)?;
        } else if !publication.try_publish() {
            return Err(Arc::from(CHECKPOINT_CANCELLED));
        }
        let mut staged = install.lock_unpoisoned();
        install_options_publication(&mut staged.0, session_id)
            .map_err(|error| Arc::from(error.to_string()))?;
        if let Some(commit) = staged.1.take() {
            commit();
        }
        Ok(())
    });
    let checkpoint = read.checkpoint_writer().checkpoint_guarded(
        request,
        admission
            .as_ref()
            .map(|admission| admission.publication.clone())
            .unwrap_or_default(),
        claim,
    );
    verify_ack(read.session_id, version, checkpoint).await
}

async fn update_model_values(
    read: &SessionReadHandle,
    reservation: SessionOptionsReservation,
    specs: Vec<Arc<str>>,
) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
    let snapshot = read.options();
    let current = snapshot
        .options
        .iter()
        .find(|option| option.definition.id.as_ref() == MODEL_OPTION_ID)
        .ok_or_else(|| SessionOptionError::UnknownId(Arc::from(MODEL_OPTION_ID)))?;
    let mut values = Vec::with_capacity(specs.len() + 1);
    if !specs.iter().any(|spec| spec == &current.current_value) {
        values.push(SessionOptionValue {
            value: Arc::clone(&current.current_value),
            name: Arc::clone(&current.current_value),
        });
    }
    for spec in specs {
        if !values
            .iter()
            .any(|value: &SessionOptionValue| value.value == spec)
        {
            values.push(SessionOptionValue {
                name: Arc::clone(&spec),
                value: spec,
            });
        }
    }
    let mut definition = current.definition.clone();
    definition.values = values.into();
    definition.initial_value = Arc::clone(&current.current_value);
    let Some(candidate) = read.options.prepare_replace_definition(definition)? else {
        return Ok(snapshot);
    };
    let reservation = Arc::new(reservation);
    let commit_reservation = Arc::clone(&reservation);
    checkpoint_options_for_actor(
        read,
        SessionOptions::candidate_snapshot(&candidate),
        None,
        false,
        Some(Box::new(move || {
            commit_reservation.install(candidate);
        })),
    )
    .await?;
    Ok(read.options())
}

fn unregister(session_id: MakimaId, generation: u64) {
    let mut directory = DIRECTORY.lock_unpoisoned();
    if directory
        .get(&session_id)
        .is_some_and(|entry| entry.handle.generation == generation)
        && let Some(entry) = directory.remove(&session_id)
    {
        entry.handle.publication_lifetime.cancel();
    }
}

/// Resolves `path` against `cwd` and requires an existing directory.
pub async fn canonical_directory(cwd: &Path, path: PathBuf) -> Result<PathBuf, ConfigChangeError> {
    let path = cwd.join(path);
    smol::unblock(move || {
        let canonical = path.canonicalize().map_err(|error| error.to_string())?;
        if !canonical.is_dir() {
            return Err(format!("{} is not a directory", canonical.display()));
        }
        Ok(canonical)
    })
    .await
    .map_err(|error| ConfigChangeError::DirectoryPreparation(error.into()))
}

pub fn builtin_option_definitions(
    current_model: impl Into<Arc<str>>,
    model_specs: impl IntoIterator<Item = Arc<str>>,
    yolo: bool,
    fast: bool,
    workflow: bool,
    thinking: ThinkingConfig,
) -> Vec<SessionOptionDefinition> {
    let current_model = current_model.into();
    let mut models: Vec<_> = model_specs
        .into_iter()
        .map(|spec| SessionOptionValue {
            name: Arc::clone(&spec),
            value: spec,
        })
        .collect();
    if !models.iter().any(|value| value.value == current_model) {
        models.insert(
            0,
            SessionOptionValue {
                name: Arc::clone(&current_model),
                value: Arc::clone(&current_model),
            },
        );
    }
    vec![
        SessionOptionDefinition {
            id: Arc::from(MODEL_OPTION_ID),
            owner: SessionOptionOwner::Builtin,
            name: Arc::from("Model"),
            description: Arc::from("Model used for future turns"),
            category: SessionOptionCategory::Model,
            values: models.into(),
            free_value: None,
            initial_value: current_model,
            persistent: true,
        },
        yolo_definition(yolo),
        toggle_definition(
            FAST_OPTION_ID,
            "Fast",
            "Use the model's fast service tier",
            fast,
        ),
        workflow_definition(workflow),
        thinking_definition(thinking),
    ]
}

/// The built-in options a plain set accepts; model, thinking and fast have
/// typed setters instead.
pub fn settable_builtin_definitions() -> [SessionOptionDefinition; 2] {
    [yolo_definition(false), workflow_definition(false)]
}

fn yolo_definition(yolo: bool) -> SessionOptionDefinition {
    toggle_definition(YOLO_OPTION_ID, "YOLO", "Skip permission prompts", yolo)
}

fn workflow_definition(workflow: bool) -> SessionOptionDefinition {
    toggle_definition(
        WORKFLOW_OPTION_ID,
        "Workflow",
        "Enable workflow tools for future turns",
        workflow,
    )
}

fn thinking_definition(thinking: ThinkingConfig) -> SessionOptionDefinition {
    SessionOptionDefinition {
        id: Arc::from(THINKING_OPTION_ID),
        owner: SessionOptionOwner::Builtin,
        name: Arc::from("Thinking"),
        description: Arc::from("Reasoning effort used for future requests"),
        category: SessionOptionCategory::Mode,
        values: ThinkingConfig::options()
            .iter()
            .map(|value| SessionOptionValue {
                value: Arc::from(*value),
                name: Arc::from(*value),
            })
            .collect::<Vec<_>>()
            .into(),
        free_value: Some(FreeValueDomain::ThinkingSetting),
        initial_value: Arc::from(thinking.to_string()),
        persistent: true,
    }
}

fn toggle_definition(
    id: &'static str,
    name: &'static str,
    description: &'static str,
    enabled: bool,
) -> SessionOptionDefinition {
    SessionOptionDefinition {
        id: Arc::from(id),
        owner: SessionOptionOwner::Builtin,
        name: Arc::from(name),
        description: Arc::from(description),
        category: SessionOptionCategory::Mode,
        values: Arc::from([
            SessionOptionValue {
                value: Arc::from(ENABLED_VALUE),
                name: Arc::from("Enabled"),
            },
            SessionOptionValue {
                value: Arc::from(DISABLED_VALUE),
                name: Arc::from("Disabled"),
            },
        ]),
        free_value: None,
        initial_value: Arc::from(if enabled {
            ENABLED_VALUE
        } else {
            DISABLED_VALUE
        }),
        persistent: true,
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::actor::{
        ActorBackend, ActorHistory, AdmissionPreparation, AgentContextSnapshot, BackendResult,
        ControlWork, WorkKind,
    };
    use crate::agent::{PromptSlotSource, TurnSetup};
    use crate::context::root_context;
    use crate::manager::{AgentLimits, AgentManagerHandle, ResidentInitialization};
    use crate::modes::{ModeDef, ModeId};
    use crate::test_util::MockBackend;
    use crate::tools::{LocalTools, NullProvider, ToolRegistry};
    use crate::{AgentInput, AgentMode, ModeRegistry, RunSettings, TurnContext};
    use makima_providers::Effort;
    use makima_providers::model::FastSupport;
    use makima_providers::{Model, ThinkingSupport};
    use makima_storage::checkpoint::CheckpointAck;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use test_case::test_case;

    const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
    const YOLO_PUBLICATION_TIMEOUT: &str =
        "YOLO publication or following policy commit did not complete";
    const FAST_TOGGLE_TIMEOUT: &str = "pending fast toggle did not complete publication";
    const OLD: &str = "ollama/old";
    const NEW: &str = "ollama/new";
    const FAILED: &str = "failed";
    const CAPABLE_MODEL: &str = "anthropic/claude-opus-4-8";
    const FAST_TIER_TARGET: &str = "openai/gpt-5";
    const NO_THINKING_TARGET: &str = "ollama/llama3";
    const RELOAD_INSTRUCTIONS: &str = "Use the reloaded root instructions.";

    /// {spec} with the capabilities the tests give it: the capable model
    /// has every one, the fast-tier target lacks fast mode, the rest lack
    /// thinking and fast mode.
    fn test_model(spec: &str) -> Model {
        let mut model = Model::from_spec(spec).unwrap();
        if spec != CAPABLE_MODEL {
            model.fast_support = Some(FastSupport::Unsupported);
        }
        if spec != CAPABLE_MODEL && spec != FAST_TIER_TARGET {
            model.thinking_support = Some(ThinkingSupport::No);
        }
        model
    }
    const CURRENT_MODEL: &str = "current/model";
    const DISCOVERED_MODEL: &str = "other/model";
    const THINKING_OFF: &str = "off";

    impl SessionCoordinatorHandle {
        async fn barrier(&self) {
            let (reply, response) = flume::bounded(1);
            self.tx.send(Operation::Barrier(reply)).unwrap();
            response.recv_async().await.unwrap();
        }
    }

    pub(crate) fn bind_initializing_actor(
        coordinator: &SessionCoordinatorHandle,
        initialization: &ResidentInitialization,
        prepare_model: ModelPreparer,
        project_config: ConfigProjector,
    ) -> Result<(), ConfigChangeError> {
        coordinator.ensure_bindable()?;
        let manager = initialization.agent.manager();
        manager.validate_activation(&initialization.incarnation)?;
        if initialization.actor.agent_id() != initialization.agent.id()
            || initialization.incarnation.agent_id != initialization.agent.id()
            || initialization
                .actor
                .inner
                .journal_incarnation
                .as_ref()
                .is_none_or(|binding| {
                    binding.agent != initialization.incarnation.agent_id
                        || binding.reservation != initialization.incarnation.reservation
                })
        {
            return Err(
                ActorError::InvalidConfig("initializing actor identity mismatch".into()).into(),
            );
        }
        let config = initialization.actor.config_snapshot().ok_or_else(|| {
            ActorError::InvalidConfig("actor configuration is not initialized".into())
        })?;
        coordinator.bind_actor_config(
            initialization.agent.clone(),
            config.config,
            prepare_model,
            project_config,
        )
    }

    fn projection_config(workflow: bool) -> Arc<EffectiveAgentConfig> {
        let mut config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(NullProvider),
                model: test_model(OLD),
                thinking: ThinkingConfig::Off,
                fast: false,
                workflow,
            },
            AgentMode::Build,
        );
        config.mode_def = Some(ModeDef::default_for(ModeId::parse("build")));
        config.context = AgentContextSnapshot::new(PathBuf::from("/project"));
        Arc::new(config)
    }

    struct ContextBackend(TurnSetup);

    impl ActorBackend for ContextBackend {
        fn admission_preparation(&self) -> Option<AdmissionPreparation> {
            Some(self.0.clone().preparation())
        }

        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> BoxFuture<'a, BackendResult> {
            Box::pin(async { unreachable!() })
        }

        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> BoxFuture<'a, BackendResult> {
            Box::pin(async { unreachable!() })
        }

        fn run_compact<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> BoxFuture<'a, BackendResult> {
            Box::pin(async { unreachable!() })
        }
    }

    struct PublicationGate {
        claimed: bool,
        reached: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    struct GatedWriter {
        writer: GuardedWriter,
        gate: Arc<Mutex<Option<PublicationGate>>>,
    }

    impl CheckpointWriter<SessionCheckpoint> for GatedWriter {
        fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
            self.writer.checkpoint(request)
        }
        fn checkpoint_guarded(
            &self,
            request: CheckpointRequest<SessionCheckpoint>,
            publication: CheckpointPublication,
            claim: CheckpointClaim,
        ) -> CheckpointFuture {
            let gate = self.gate.lock_unpoisoned().take();
            let session_id = request.session_id;
            let save = self.writer.checkpoint(request);
            Box::pin(async move {
                let claim = || {
                    claim(&publication).map_err(|message| CheckpointError::Save {
                        session_id,
                        message,
                    })
                };
                let claim_first = gate.as_ref().is_none_or(|gate| gate.claimed);
                if claim_first {
                    claim()?;
                }
                if let Some(gate) = gate {
                    gate.reached.send_async(()).await.unwrap();
                    gate.release.recv_async().await.unwrap();
                    if !claim_first {
                        claim()?;
                    }
                }
                save.await
            })
        }
    }

    struct GuardedWriter {
        fail: bool,
        saved: Arc<Mutex<Vec<Arc<SessionCheckpoint>>>>,
    }
    impl CheckpointWriter<SessionCheckpoint> for GuardedWriter {
        fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
            self.checkpoint_guarded(
                request,
                CheckpointPublication::default(),
                Arc::new(|_| Ok(())),
            )
        }
        fn checkpoint_guarded(
            &self,
            request: CheckpointRequest<SessionCheckpoint>,
            publication: CheckpointPublication,
            claim: CheckpointClaim,
        ) -> CheckpointFuture {
            let fail = self.fail;
            let saved = Arc::clone(&self.saved);
            Box::pin(async move {
                if fail {
                    return Err(CheckpointError::Save {
                        session_id: request.session_id,
                        message: Arc::from(FAILED),
                    });
                }
                claim(&publication).map_err(|message| CheckpointError::Save {
                    session_id: request.session_id,
                    message,
                })?;
                let ack = CheckpointAck::for_request(&request);
                saved.lock_unpoisoned().push(request.snapshot);
                Ok(ack)
            })
        }
    }

    fn writer(fail: bool) -> Arc<dyn CheckpointWriter<SessionCheckpoint>> {
        Arc::new(GuardedWriter {
            fail,
            saved: Arc::default(),
        })
    }
    fn params(
        id: MakimaId,
        checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    ) -> SessionCoordinatorParams {
        SessionCoordinatorParams {
            session_id: id,
            catalog: SessionOptionCatalog::default(),
            definitions: builtin_option_definitions(
                OLD,
                [Arc::from(OLD), Arc::from(NEW)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            persisted_options: BTreeMap::new(),
            model_policy: Arc::new(ModelPolicy::default()),
            checkpoint,
            mailbox: SessionMailbox::new(id),
        }
    }

    fn bind(
        coordinator: &SessionCoordinatorHandle,
        installed: Arc<Mutex<Vec<String>>>,
    ) -> AgentRef {
        bind_config(coordinator, (*projection_config(false)).clone(), installed)
    }

    fn bind_test_permissions(coordinator: &SessionCoordinatorHandle, agent: &AgentRef) {
        let cwd = PathBuf::from("/project");
        let permissions = Arc::new(PermissionManager::new(
            Default::default(),
            cwd.clone(),
            makima_config::ProjectConfig::for_project(&cwd),
            Arc::default(),
        ));
        permissions
            .bind_journal(
                agent.manager().journal(),
                agent.actor().unwrap().inner.journal_incarnation.clone(),
            )
            .unwrap();
        coordinator.bind_permissions(permissions).unwrap();
    }

    fn bind_config(
        coordinator: &SessionCoordinatorHandle,
        config: EffectiveAgentConfig,
        installed: Arc<Mutex<Vec<String>>>,
    ) -> AgentRef {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let agent = manager
            .create_root(Vec::new(), None, Box::new(MockBackend::STRICT))
            .unwrap();
        agent.actor().unwrap().initialize_config(config).unwrap();
        bind_test_permissions(coordinator, &agent);
        coordinator
            .bind_actor(
                agent.clone(),
                Arc::new(|spec| {
                    Box::pin(async move {
                        Ok(PreparedModel {
                            provider: Arc::new(NullProvider),
                            model: test_model(&spec),
                        })
                    })
                }),
                Arc::new(move |commit| {
                    installed.lock_unpoisoned().push(commit.config.model.spec())
                }),
            )
            .unwrap();
        agent
    }

    fn register(id: MakimaId) -> SessionCoordinatorHandle {
        register_with_catalog(id, writer(false), &SessionOptionCatalog::default())
    }
    fn register_with_catalog(
        id: MakimaId,
        checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
        catalog: &SessionOptionCatalog,
    ) -> SessionCoordinatorHandle {
        let mut params = params(id, checkpoint);
        params.catalog = catalog.clone();
        let coordinator = SessionCoordinatorHandle::register(params).unwrap();
        bind(&coordinator, Arc::default());
        coordinator
    }

    fn plugin_option(generation: u64, values: &[&str], initial: &str) -> SessionOptionDefinition {
        SessionOptionDefinition {
            id: Arc::from("test.choice"),
            owner: SessionOptionOwner::Plugin {
                plugin: Arc::from("test"),
                generation,
            },
            name: Arc::from("Choice"),
            description: Arc::from("Test choice"),
            category: SessionOptionCategory::Mode,
            values: values
                .iter()
                .map(|value| SessionOptionValue {
                    value: Arc::from(*value),
                    name: Arc::from(*value),
                })
                .collect::<Vec<_>>()
                .into(),
            free_value: None,
            initial_value: Arc::from(initial),
            persistent: true,
        }
    }

    #[test]
    fn plugin_replacement_preserves_compatible_values_falls_back_and_unloads() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let first = register_with_catalog(MakimaId::generate(), writer(false), &catalog);
            let second = register_with_catalog(MakimaId::generate(), writer(false), &catalog);
            catalog
                .replace_plugin_options("test", vec![plugin_option(1, &["a", "b"], "a")])
                .await
                .unwrap();
            first.set_option("test.choice", "b").await.unwrap();
            catalog
                .replace_plugin_options("test", vec![plugin_option(2, &["a", "b", "c"], "c")])
                .await
                .unwrap();
            let value = |coordinator: &SessionCoordinatorHandle| {
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == "test.choice")
                    .unwrap()
                    .current_value
                    .to_string()
            };
            assert_eq!(value(&first), "b");
            assert_eq!(value(&second), "a");
            catalog
                .replace_plugin_options("test", vec![plugin_option(3, &["c"], "c")])
                .await
                .unwrap();
            assert_eq!(value(&first), "c");
            assert_eq!(value(&second), "c");
            catalog
                .replace_plugin_options("test", Vec::new())
                .await
                .unwrap();
            for coordinator in [&first, &second] {
                assert!(
                    coordinator
                        .read()
                        .options()
                        .options
                        .iter()
                        .all(|option| option.definition.id.as_ref() != "test.choice")
                );
                coordinator.close().await.unwrap();
            }
        });
    }

    #[test]
    fn option_and_catalog_commits_record_complete_values_and_deduplicate_metadata() {
        smol::block_on(async {
            const OPTION: &str = "test.choice";
            let catalog = SessionOptionCatalog::default();
            let coordinator = register_with_catalog(MakimaId::generate(), writer(false), &catalog);
            let agent = coordinator
                .read
                .binding
                .lock_unpoisoned()
                .as_ref()
                .unwrap()
                .agent
                .clone();
            let journal = agent.manager().journal();
            let mut definition = plugin_option(1, &["a", "b"], "a");
            definition.persistent = false;
            catalog
                .replace_plugin_options("test", vec![definition.clone()])
                .await
                .unwrap();
            coordinator.set_option(OPTION, "b").await.unwrap();
            coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            let before = journal.snapshot();
            let latest = before
                .state
                .iter()
                .rev()
                .find_map(|record| match &record.change {
                    StateChange::Config(config) if record.agent == Some(agent.id()) => Some(config),
                    _ => None,
                })
                .unwrap();
            assert_eq!(latest.options, coordinator.read().options().config_values());
            assert_eq!(latest.options[OPTION], "b");
            assert!(!latest.options.contains_key(YOLO_OPTION_ID));
            assert!(before.state.iter().any(|record| record.agent.is_none()
                && matches!(
                    &record.change,
                    StateChange::Rules {
                        yolo: Some(true),
                        ..
                    }
                )));
            assert_eq!(latest.model, OLD);
            assert_eq!(latest.cwd, "/project");
            coordinator.set_option(OPTION, "b").await.unwrap();
            definition.owner = SessionOptionOwner::Plugin {
                plugin: Arc::from("test"),
                generation: 2,
            };
            catalog
                .replace_plugin_options("test", vec![definition])
                .await
                .unwrap();
            assert_eq!(journal.snapshot().state, before.state);
            catalog
                .replace_plugin_options("test", Vec::new())
                .await
                .unwrap();
            let after = journal.snapshot();
            assert_eq!(after.state.iter().len(), before.state.iter().len() + 1);
            let StateChange::Config(config) = &after.state.iter().next_back().unwrap().change
            else {
                panic!("expected configuration record");
            };
            assert!(!config.options.contains_key(OPTION));
            assert!(!config.options.contains_key(YOLO_OPTION_ID));
            assert!(coordinator.read().options().enabled(YOLO_OPTION_ID));
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn yolo_publication_pins_committed_policy_without_waiting_for_unresolved_operation() {
        smol::block_on(futures_lite::future::or(
            async {
                let coordinator = register(MakimaId::generate());
                let agent = coordinator
                    .read
                    .binding
                    .lock_unpoisoned()
                    .as_ref()
                    .unwrap()
                    .agent
                    .clone();
                let actor = agent.actor().unwrap();
                let pending = actor.reserve_prepared_operation(None).unwrap();
                coordinator
                    .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                    .await
                    .unwrap();
                assert!(coordinator.read().options().enabled(YOLO_OPTION_ID));
                pending
                    .commit_when_ready(Some(ConfigChange::ToggleWorkflow))
                    .await
                    .unwrap();
                let snapshot = agent.manager().journal().snapshot();
                let StateChange::Config(config) =
                    &snapshot.state.iter().next_back().unwrap().change
                else {
                    panic!("expected configuration record");
                };
                assert!(config.workflow);
                assert!(!config.options.contains_key(YOLO_OPTION_ID));
                assert!(coordinator.read().options().enabled(YOLO_OPTION_ID));
                coordinator.close().await.unwrap();
            },
            async {
                smol::Timer::after(COMPLETION_TIMEOUT).await;
                panic!("{YOLO_PUBLICATION_TIMEOUT}");
            },
        ));
    }

    #[test]
    fn options_publication_blocks_later_direct_policy_commit_until_install() {
        smol::block_on(async {
            const OPTION: &str = "test.choice";
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            let gate = Arc::new(Mutex::new(None));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: false,
                        saved: Arc::default(),
                    },
                    gate: Arc::clone(&gate),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::default());
            coordinator
                .catalog
                .replace_plugin_options("test", vec![plugin_option(1, &["a", "b"], "a")])
                .await
                .unwrap();
            *gate.lock_unpoisoned() = Some(PublicationGate {
                claimed: true,
                reached,
                release: proceed,
            });
            let mutation = smol::spawn({
                let coordinator = coordinator.clone();
                async move { coordinator.set_option(OPTION, "b").await.unwrap() }
            });
            waiting.recv_async().await.unwrap();
            let actor = agent.actor().unwrap();
            let policy = actor.reserve_prepared_operation(None).unwrap();
            policy.resolve(Some(ConfigChange::ToggleWorkflow)).unwrap();
            let mut ready = Box::pin(policy.wait_ready());
            assert!(
                futures_lite::future::poll_once(ready.as_mut())
                    .await
                    .is_none()
            );
            release.send_async(()).await.unwrap();
            mutation.await;
            ready.await.unwrap();
            policy.commit().unwrap();
            let snapshot = agent.manager().journal().snapshot();
            let StateChange::Config(config) = &snapshot.state.iter().next_back().unwrap().change
            else {
                panic!("expected configuration record");
            };
            assert!(config.workflow);
            assert_eq!(config.options[OPTION], "b");
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn catalog_reports_busy_actor_by_session_and_other_session_can_change_options() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let busy = register_with_catalog(MakimaId::generate(), writer(false), &catalog);
            let other = register_with_catalog(MakimaId::generate(), writer(false), &catalog);
            let binding = busy.actor_binding().unwrap();
            let actor = binding.agent.actor().unwrap();
            let publication = actor.reserve_config_publication().unwrap();
            let definitions = vec![plugin_option(1, &["a", "b"], "a")];
            assert!(matches!(
                catalog.replace_plugin_options("test", definitions.clone()).await,
                Err(SessionCoordinatorError::SessionBusy(id)) if id == busy.session_id()
            ));
            assert!(catalog.plugin_definitions("test").is_empty());
            assert!(
                other
                    .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                    .await
                    .unwrap()
                    .enabled(YOLO_OPTION_ID)
            );
            assert!(!busy.read().options().enabled(YOLO_OPTION_ID));
            drop(publication);
            assert_eq!(
                catalog
                    .replace_plugin_options("test", definitions)
                    .await
                    .unwrap()
                    .len(),
                2
            );
            busy.close().await.unwrap();
            other.close().await.unwrap();
        });
    }

    #[test]
    fn plugin_replacement_rejects_reserved_options_without_partial_commit() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let mut ids = [MakimaId::generate(), MakimaId::generate()];
            ids.sort_by_key(ToString::to_string);
            let first_saved = Arc::new(Mutex::new(Vec::new()));
            let second_saved = Arc::new(Mutex::new(Vec::new()));
            let first = register_with_catalog(
                ids[0],
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&first_saved),
                }),
                &catalog,
            );
            let second = register_with_catalog(
                ids[1],
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&second_saved),
                }),
                &catalog,
            );
            let initial = vec![plugin_option(1, &["a", "b"], "a")];
            catalog
                .replace_plugin_options("test", initial.clone())
                .await
                .unwrap();
            first.set_option("test.choice", "b").await.unwrap();
            let before_first = first.read().options();
            let before_second = second.read().options();
            let predecessor = second
                .read
                .binding
                .lock_unpoisoned()
                .as_ref()
                .unwrap()
                .agent
                .actor()
                .unwrap()
                .reserve_prepared_operation(None)
                .unwrap();
            let mut mutation = Box::pin(second.set_model(ModelSelection {
                fast: Some(false),
                ..Default::default()
            }));
            assert!(
                futures_lite::future::poll_once(mutation.as_mut())
                    .await
                    .is_none()
            );
            assert!(
                matches!(catalog.replace_plugin_options("test", vec![plugin_option(2, &["c"], "c")]).await, Err(SessionCoordinatorError::SessionBusy(busy)) if busy == ids[1])
            );
            assert_eq!(catalog.plugin_definitions("test"), initial);
            assert_eq!(first.read().options(), before_first);
            assert_eq!(second.read().options(), before_second);
            drop(predecessor);
            mutation.await.unwrap();
            catalog
                .replace_plugin_options("test", vec![plugin_option(2, &["c"], "c")])
                .await
                .unwrap();
            for close in [false, true] {
                let before_first = first.read().options();
                let before_second = second.read().options();
                let previous_definitions = catalog.plugin_definitions("test");
                let (started_tx, started_rx) = flume::bounded(1);
                let (release_tx, release_rx) = flume::bounded::<()>(1);
                let mut replace = Box::pin(catalog.replace_plugin_options_with_validator(
                    "test",
                    vec![plugin_option(3, &["d"], "d")],
                    move |session_id, _, _| {
                        let started_tx = started_tx.clone();
                        let release_rx = release_rx.clone();
                        async move {
                            if session_id == ids[1] {
                                started_tx.send_async(()).await.unwrap();
                                release_rx.recv_async().await.unwrap();
                            }
                            Ok(())
                        }
                    },
                ));
                futures_lite::future::or(
                    async {
                        replace.as_mut().await.unwrap();
                        panic!("validator must remain gated")
                    },
                    async { started_rx.recv_async().await.unwrap() },
                )
                .await;
                for coordinator in [&first, &second] {
                    assert!(matches!(
                        coordinator.read.options.try_reserve(None),
                        Err(SessionOptionError::Busy)
                    ));
                }
                assert!(
                    futures_lite::future::poll_once(first.set_model(ModelSelection {
                        fast: Some(false),
                        ..Default::default()
                    }))
                    .await
                    .is_none()
                );
                if close {
                    let closed = first.request_close().unwrap();
                    assert!(closed.is_empty());
                    assert_eq!(
                        DIRECTORY
                            .lock_unpoisoned()
                            .get(&ids[0])
                            .unwrap()
                            .handle
                            .generation,
                        first.generation
                    );
                    release_tx.send(()).unwrap();
                    assert!(replace.await.is_err());
                    closed.recv_async().await.unwrap();
                } else {
                    drop(replace);
                    first.barrier().await;
                    first
                        .set_option(YOLO_OPTION_ID, DISABLED_VALUE)
                        .await
                        .unwrap();
                }
                assert_eq!(catalog.plugin_definitions("test"), previous_definitions);
                assert_eq!(
                    first_saved.lock_unpoisoned().last().unwrap().options,
                    before_first
                );
                drop(first.read.options.try_reserve(None).unwrap());
                second.barrier().await;
                assert_eq!(
                    second_saved.lock_unpoisoned().last().unwrap().options,
                    before_second
                );
                drop(second.read.options.try_reserve(None).unwrap());
                second
                    .set_option(YOLO_OPTION_ID, DISABLED_VALUE)
                    .await
                    .unwrap();
            }
            second
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            second.close().await.unwrap();
        });
    }

    #[test_case::test_case(false; "successful_vote_after_cancel")]
    #[test_case::test_case(true; "rejected_vote_after_cancel")]
    fn dropped_catalog_caller_before_claim_aborts_without_publication(abort: bool) {
        smol::block_on(async {
            let mut ids = [MakimaId::generate(), MakimaId::generate()];
            ids.sort_by_key(ToString::to_string);
            let saved = Arc::new(Mutex::new(Vec::new()));
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> =
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                });
            let catalog = SessionOptionCatalog::default();
            let first = register_with_catalog(ids[0], Arc::clone(&checkpoint), &catalog);
            let second = register_with_catalog(ids[1], checkpoint, &catalog);
            let before = first.read().options();
            let (gate_tx, gate_rx) = flume::unbounded();
            catalog
                .state
                .lock_unpoisoned()
                .sessions
                .get_mut(&ids[1])
                .unwrap()
                .tx = gate_tx;
            let replacement = smol::spawn({
                let catalog = catalog.clone();
                async move {
                    catalog
                        .replace_plugin_options("test", vec![plugin_option(1, &["a"], "a")])
                        .await
                }
            });
            let Operation::PreparePluginOptions {
                plugin,
                definitions,
                reservation,
                prepared,
                decision,
            } = gate_rx.recv_async().await.unwrap()
            else {
                unreachable!();
            };
            let (forward, forwarded) = flume::unbounded();
            second
                .tx
                .send(Operation::PreparePluginOptions {
                    plugin,
                    definitions,
                    reservation,
                    prepared,
                    decision: forwarded,
                })
                .unwrap();
            let PluginOptionDecision::Commit(reply) = decision.recv_async().await.unwrap() else {
                unreachable!();
            };
            let (ack, acknowledged) = flume::bounded(1);
            forward.send(PluginOptionDecision::Commit(ack)).unwrap();
            acknowledged.recv_async().await.unwrap().unwrap();
            assert_eq!(saved.lock_unpoisoned().len(), 0);
            for coordinator in [&first, &second] {
                for value in [ENABLED_VALUE, DISABLED_VALUE] {
                    assert!(matches!(
                        coordinator.read.options.try_reserve(None),
                        Err(SessionOptionError::Busy)
                    ));
                    assert!(
                        futures_lite::future::poll_once(
                            coordinator.set_option(YOLO_OPTION_ID, value)
                        )
                        .await
                        .is_none()
                    );
                }
            }
            assert_eq!(saved.lock_unpoisoned().len(), 0);
            assert!(replacement.cancel().await.is_none());
            reply
                .send(if abort {
                    Err(SessionCoordinatorError::StaleSession(ids[1]))
                } else {
                    Ok(())
                })
                .ok();
            let final_decision = decision.recv_async().await.unwrap();
            let (ack, acknowledged) = flume::bounded(1);
            let reply = match final_decision {
                PluginOptionDecision::Complete(_) => unreachable!(),
                PluginOptionDecision::Abort(reply) => {
                    forward.send(PluginOptionDecision::Abort(ack)).unwrap();
                    reply
                }
                _ => unreachable!(),
            };
            acknowledged.recv_async().await.unwrap().unwrap();
            drop(first.read.options.try_reserve(None).unwrap());
            drop(second.read.options.try_reserve(None).unwrap());
            reply.send(Ok(())).unwrap();
            assert!(catalog.plugin_definitions("test").is_empty());
            assert_eq!(first.read().options(), before);
            assert_eq!(second.read().options(), before);
            assert_eq!(saved.lock_unpoisoned().len(), 0);
            first
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            assert_eq!(saved.lock_unpoisoned().len(), 1);
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn prepared_session_reconciles_plugin_replacement_before_activation() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let id = MakimaId::generate();
            let mut session_params = params(id, writer(false));
            session_params.catalog = catalog.clone();
            let prepared = PreparedSessionCoordinator::new(session_params).unwrap();
            catalog
                .replace_plugin_options("test", vec![plugin_option(1, &["a"], "a")])
                .await
                .unwrap();
            let coordinator = prepared.activate().unwrap();
            assert!(
                coordinator
                    .read()
                    .options()
                    .options
                    .iter()
                    .any(|option| option.definition.id.as_ref() == "test.choice"
                        && option.current_value.as_ref() == "a")
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn replacement_reprepares_session_activated_during_validator() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let id = MakimaId::generate();
            let current = register_with_catalog(id, writer(false), &catalog);
            let mut replacement_params = params(id, writer(false));
            replacement_params.catalog = catalog.clone();
            let replacement = PreparedSessionCoordinator::new(replacement_params).unwrap();
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let gate = Arc::new(AtomicBool::new(true));
            let replace = smol::spawn({
                let catalog = catalog.clone();
                async move {
                    catalog
                        .replace_plugin_options_with_validator(
                            "test",
                            vec![plugin_option(1, &["a"], "a")],
                            move |_, _, _| {
                                let wait = gate.swap(false, Ordering::Relaxed);
                                let started_tx = started_tx.clone();
                                let release_rx = release_rx.clone();
                                async move {
                                    if wait {
                                        started_tx.send_async(()).await.unwrap();
                                        release_rx.recv_async().await.unwrap();
                                    }
                                    Ok(())
                                }
                            },
                        )
                        .await
                }
            });
            started_rx.recv_async().await.unwrap();
            assert!(matches!(
                current.read.options.try_reserve(None),
                Err(SessionOptionError::Busy)
            ));
            let activated = replacement.activate_replacing(&current).unwrap();
            release_tx.send_async(()).await.unwrap();
            let committed = replace.await.unwrap();
            assert_eq!(committed.len(), 1);
            assert_eq!(committed[0].0, id);
            assert!(
                activated
                    .read()
                    .options()
                    .options
                    .iter()
                    .any(|option| option.definition.id.as_ref() == "test.choice"
                        && option.current_value.as_ref() == "a")
            );
            assert!(
                matches!(current.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await, Err(ConfigChangeError::Coordinator(SessionCoordinatorError::StaleSession(stale))) if stale == id)
            );
            activated.close().await.unwrap();
        });
    }

    #[test_case(false; "shared_journal")]
    #[test_case(true; "independent_journals")]
    fn catalog_installs_in_all_journals_even_when_a_receipt_fails(independent: bool) {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let saved = Arc::new(Mutex::new(Vec::new()));
            let fail = Arc::new(Mutex::new(false));
            let mut ids = [MakimaId::generate(), MakimaId::generate()];
            ids.sort_by_key(ToString::to_string);
            let first = register_with_catalog(
                ids[0],
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
                &catalog,
            );
            let second = SessionCoordinatorHandle::register({
                let mut params = params(
                    ids[1],
                    Arc::new({
                        let fail = Arc::clone(&fail);
                        let saved = Arc::clone(&saved);
                        move |request: CheckpointRequest<SessionCheckpoint>| {
                            let rejected = *fail.lock_unpoisoned();
                            let saved = Arc::clone(&saved);
                            Box::pin(async move {
                                if rejected {
                                    return Err(CheckpointError::Save {
                                        session_id: request.session_id,
                                        message: Arc::from(FAILED),
                                    });
                                }
                                let ack = CheckpointAck::for_request(&request);
                                saved.lock_unpoisoned().push(request.snapshot);
                                Ok(ack)
                            }) as CheckpointFuture
                        }
                    }),
                );
                params.catalog = catalog.clone();
                params
            })
            .unwrap();
            let root = first
                .read
                .binding
                .lock_unpoisoned()
                .as_ref()
                .unwrap()
                .agent
                .clone();
            let child = if independent {
                bind(&second, Arc::default())
            } else {
                let child = root
                    .manager()
                    .spawn_child_trusted_with_config(
                        &root,
                        (*projection_config(false)).clone(),
                        Default::default(),
                        Vec::new(),
                        None,
                        |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(MockBackend::STRICT)),
                    )
                    .unwrap();
                second
                    .bind_actor(
                        child.clone(),
                        Arc::new(|_| Box::pin(async { unreachable!() })),
                        Arc::new(|_| {}),
                    )
                    .unwrap();
                bind_test_permissions(&second, &child);
                child
            };
            catalog
                .replace_plugin_options("test", vec![plugin_option(1, &["a", "b"], "a")])
                .await
                .unwrap();
            if !independent {
                let saved = saved.lock_unpoisoned();
                let first = saved[0].journal.as_ref().unwrap();
                let second = saved[1].journal.as_ref().unwrap();
                assert_eq!(first.state, second.state);
                assert_eq!(first.seq, second.seq);
                for id in [root.id(), child.id()] {
                    assert!(first.state.iter().any(|record| record.agent == Some(id) && matches!(&record.change, StateChange::Config(config) if config.options.get("test.choice").is_some_and(|value| value == "a"))));
                }
            }
            *fail.lock_unpoisoned() = true;
            let updated = vec![plugin_option(2, &["b"], "b")];
            assert!(matches!(
                catalog.replace_plugin_options("test", updated.clone()).await,
                Err(SessionCoordinatorError::OptionsInstalled(CheckpointError::Save { session_id, .. }))
                    if session_id == ids[1]
            ));
            assert_eq!(catalog.plugin_definitions("test"), updated);
            assert_eq!(
                saved.lock_unpoisoned().last().unwrap().options,
                first.read().options()
            );
            for (coordinator, agent) in [(&first, &root), (&second, &child)] {
                let snapshot = agent.manager().journal().snapshot();
                let config = snapshot
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Config(config) if record.agent == Some(agent.id()) => {
                            Some(config)
                        }
                        _ => None,
                    })
                    .unwrap();
                assert_eq!(config.options["test.choice"], "b");
                assert_eq!(coordinator.read().options().values()["test.choice"], "b");
                drop(coordinator.read.options.try_reserve(None).unwrap());
                coordinator.close().await.unwrap();
            }
            root.close_subtree().unwrap();
            if independent {
                child.close_subtree().unwrap();
            }
        });
    }

    #[test]
    fn catalog_projector_can_reenter_directory_catalog_and_options() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let id = MakimaId::generate();
            let coordinator = register_with_catalog(id, writer(false), &catalog);
            let (projected, observed) = flume::bounded(1);
            coordinator.read().project_options({
                let catalog = catalog.clone();
                move |snapshot| {
                    if snapshot.values().contains_key("test.choice") {
                        let resolved = SessionCoordinatorHandle::resolve(id).unwrap();
                        assert_eq!(resolved.read().options(), *snapshot);
                        assert_eq!(
                            catalog.plugin_definitions("test"),
                            vec![plugin_option(1, &["a"], "a")]
                        );
                        projected.send(()).unwrap();
                    }
                }
            });
            catalog
                .replace_plugin_options("test", vec![plugin_option(1, &["a"], "a")])
                .await
                .unwrap();
            observed.recv_async().await.unwrap();
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn catalog_caller_cancel_after_publication_claim_finishes_transaction() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let id = MakimaId::generate();
            let saved = Arc::new(Mutex::new(Vec::new()));
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            let coordinator = register_with_catalog(
                id,
                Arc::new({
                    let saved = Arc::clone(&saved);
                    move |request: CheckpointRequest<SessionCheckpoint>| {
                        let reached = reached.clone();
                        let proceed = proceed.clone();
                        let saved = Arc::clone(&saved);
                        Box::pin(async move {
                            reached.send_async(()).await.unwrap();
                            proceed.recv_async().await.unwrap();
                            let ack = CheckpointAck::for_request(&request);
                            saved.lock_unpoisoned().push(request.snapshot);
                            Ok(ack)
                        }) as CheckpointFuture
                    }
                }),
                &catalog,
            );
            let replacement = smol::spawn({
                let catalog = catalog.clone();
                async move {
                    catalog
                        .replace_plugin_options("test", vec![plugin_option(1, &["a"], "a")])
                        .await
                }
            });
            waiting.recv_async().await.unwrap();
            assert_eq!(coordinator.read().options().values()["test.choice"], "a");
            assert_eq!(
                catalog.plugin_definitions("test"),
                vec![plugin_option(1, &["a"], "a")]
            );
            assert!(saved.lock_unpoisoned().is_empty());
            assert!(replacement.cancel().await.is_none());
            release.send_async(()).await.unwrap();
            coordinator.read().wait_unreserved().await;
            assert_eq!(coordinator.read().options().values()["test.choice"], "a");
            assert_eq!(
                catalog.plugin_definitions("test"),
                vec![plugin_option(1, &["a"], "a")]
            );
            assert_eq!(saved.lock_unpoisoned().len(), 1);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn catalog_release_failure_after_install_does_not_rollback_committed_values() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let id = MakimaId::generate();
            let coordinator = register_with_catalog(id, writer(false), &catalog);
            let agent = coordinator
                .read
                .binding
                .lock_unpoisoned()
                .as_ref()
                .unwrap()
                .agent
                .clone();
            let (gate, requests) = flume::unbounded();
            catalog
                .state
                .lock_unpoisoned()
                .sessions
                .get_mut(&id)
                .unwrap()
                .tx = gate;
            let replacement = smol::spawn({
                let catalog = catalog.clone();
                async move {
                    catalog
                        .replace_plugin_options("test", vec![plugin_option(1, &["a"], "a")])
                        .await
                }
            });
            let Operation::PreparePluginOptions {
                plugin,
                definitions,
                reservation,
                prepared,
                decision,
            } = requests.recv_async().await.unwrap()
            else {
                unreachable!()
            };
            let (forward, forwarded) = flume::unbounded();
            coordinator
                .tx
                .send(Operation::PreparePluginOptions {
                    plugin,
                    definitions,
                    reservation,
                    prepared,
                    decision: forwarded,
                })
                .unwrap();
            let PluginOptionDecision::Commit(reply) = decision.recv_async().await.unwrap() else {
                unreachable!()
            };
            let (ack, acknowledged) = flume::bounded(1);
            forward.send(PluginOptionDecision::Commit(ack)).unwrap();
            acknowledged.recv_async().await.unwrap().unwrap();
            reply.send(Ok(())).unwrap();
            let PluginOptionDecision::Complete(reply) = decision.recv_async().await.unwrap() else {
                unreachable!()
            };
            assert_eq!(coordinator.read().options().values()["test.choice"], "a");
            assert_eq!(
                catalog.plugin_definitions("test"),
                vec![plugin_option(1, &["a"], "a")]
            );
            let installed = agent.manager().journal().snapshot().state;
            drop(reply);
            assert!(replacement.await.is_ok());
            let (reply, response) = flume::bounded(1);
            forward.send(PluginOptionDecision::Complete(reply)).unwrap();
            response.recv_async().await.unwrap().unwrap();
            assert_eq!(agent.manager().journal().snapshot().state, installed);
            assert_eq!(coordinator.read().options().values()["test.choice"], "a");
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn coordinator_directory_owns_mailbox_routing_lifetime() {
        smol::block_on(async {
            let id = MakimaId::generate();
            let coordinator = register(id);
            let mailbox = coordinator.mailbox().unwrap();
            SessionMailbox::notify(id, "live".into(), false).unwrap();
            assert_eq!(mailbox.drain()[0].user_text(), Some("live"));
            assert!(
                matches!(SessionCoordinatorHandle::register(params(id, writer(false))), Err(SessionCoordinatorError::DuplicateSession(duplicate)) if duplicate == id)
            );
            coordinator.close().await.unwrap();
            assert!(SessionMailbox::notify(id, "late".into(), false).is_err());
            assert!(mailbox.drain().is_empty());
            assert!(
                matches!(coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await, Err(ConfigChangeError::Coordinator(SessionCoordinatorError::StaleSession(stale))) if stale == id)
            );
        });
    }

    #[test_case(false; "published")]
    #[test_case(true; "checkpoint_failure")]
    fn model_settings_commit_atomically_with_one_checkpoint(fail: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let installed = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::clone(&installed));
            let journal = agent.manager().journal();
            let before_journal = journal.snapshot().state;
            let before = coordinator.read().options();
            let result = coordinator
                .set_model(ModelSelection {
                    spec: Some(Arc::from(NEW)),
                    fast: Some(false),
                    thinking: Some(ThinkingConfig::Off),
                })
                .await;
            if fail {
                assert!(result.is_err());
                assert_eq!(coordinator.read().options(), before);
                assert_eq!(agent.effective_config().unwrap().unwrap().model.spec(), OLD);
                assert!(installed.lock_unpoisoned().is_empty());
                assert!(saved.lock_unpoisoned().is_empty());
                assert_eq!(journal.snapshot().state, before_journal);
            } else {
                let options = result.unwrap();
                assert_eq!(coordinator.read().model().unwrap().as_ref(), NEW);
                assert_eq!(agent.effective_config().unwrap().unwrap().model.spec(), NEW);
                assert_eq!(installed.lock_unpoisoned().as_slice(), &[NEW]);
                let saved = saved.lock_unpoisoned();
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].options, options);
                assert!(saved[0].history.is_none());
                assert_eq!(saved[0].kind, SessionCheckpointKind::Config);
                let candidate = saved[0].journal.as_ref().unwrap();
                let installed = journal.snapshot();
                assert_eq!(candidate.state, installed.state);
                assert_eq!(
                    installed.state.iter().len(),
                    before_journal.iter().len() + 1
                );
                let StateChange::Config(config) =
                    &installed.state.iter().next_back().unwrap().change
                else {
                    panic!("expected configuration record");
                };
                assert_eq!(config.model, NEW);
                assert_eq!(config.cwd, "/project");
                assert_eq!(config.options, options.config_values());
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "unbound_checkpoint_writer")]
    #[test_case(true; "bound_actor")]
    fn persisted_history_saves_a_history_checkpoint(bound_actor: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            if bound_actor {
                bind(&coordinator, Arc::default());
            }
            let mut history = HistorySnapshot::new(vec![Message::user("terminal".into())]);
            history.compacted = true;
            coordinator.persist_history(history.clone()).await.unwrap();
            {
                let saved = saved.lock_unpoisoned();
                assert_eq!(saved[0].kind, SessionCheckpointKind::History);
                let saved_history = saved[0].history.as_ref().unwrap();
                assert_eq!(saved_history.epoch, history.epoch);
                assert!(saved_history.compacted);
                assert!(saved[0].history_revision.is_some());
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "revoked_before_claim")]
    #[test_case(true; "revoked_after_claim")]
    fn model_checkpoint_observes_authorized_publication_claim(claimed: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let installed = Arc::new(Mutex::new(Vec::new()));
            let (reached_tx, reached_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: false,
                        saved: Arc::clone(&saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed,
                        reached: reached_tx,
                        release: release_rx,
                    }))),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::clone(&installed));
            let before = coordinator.read().options();
            let authority = Arc::new(Mutex::new(true));
            let captured_authority = Arc::clone(&authority);
            let claim: CheckpointClaim = Arc::new(move |publication| {
                let live = captured_authority.lock_unpoisoned();
                if !*live {
                    publication.cancel();
                    return Err(Arc::from(FAILED));
                }
                if publication.try_publish() {
                    Ok(())
                } else {
                    Err(Arc::from(FAILED))
                }
            });
            let operation = smol::spawn({
                let coordinator = coordinator.clone();
                async move {
                    coordinator
                        .set_model_with_claim(
                            ModelSelection {
                                spec: Some(Arc::from(NEW)),
                                ..Default::default()
                            },
                            CheckpointPublication::default(),
                            claim,
                        )
                        .await
                }
            });
            reached_rx.recv_async().await.unwrap();
            assert_eq!(agent.effective_config().unwrap().unwrap().model.spec(), OLD);
            assert_eq!(
                agent
                    .manager()
                    .journal()
                    .snapshot()
                    .state
                    .iter()
                    .any(|record| {
                        matches!(&record.change, StateChange::Config(config) if config.model == NEW)
                    }),
                claimed
            );
            *authority.lock_unpoisoned() = false;
            release_tx.send_async(()).await.unwrap();
            assert_eq!(operation.await.is_ok(), claimed);
            assert_eq!(
                agent.effective_config().unwrap().unwrap().model.spec(),
                if claimed { NEW } else { OLD }
            );
            assert_eq!(
                coordinator.read().model().unwrap().as_ref(),
                if claimed { NEW } else { OLD }
            );
            if claimed {
                let options = coordinator.read().options();
                assert!(options.version > before.version);
                let saved = saved.lock_unpoisoned();
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].options, options);
                assert_eq!(saved[0].config.as_ref().unwrap().config.model.spec(), NEW);
                assert_eq!(installed.lock_unpoisoned().as_slice(), &[NEW]);
            } else {
                assert_eq!(coordinator.read().options(), before);
                assert!(saved.lock_unpoisoned().is_empty());
                assert!(installed.lock_unpoisoned().is_empty());
            }
            drop(coordinator.read.options.try_reserve(None).unwrap());
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn option_set_waits_for_the_lease_instead_of_failing_busy() {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let _agent = bind(&coordinator, Arc::new(Mutex::new(Vec::new())));
            let lease = coordinator.reserve_options().unwrap();
            let mut set = std::pin::pin!(coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE));
            assert!(futures_lite::future::poll_once(&mut set).await.is_none());
            drop(lease);
            set.await.unwrap();
            assert!(coordinator.read().options().options.iter().any(|option| {
                option.definition.id.as_ref() == YOLO_OPTION_ID
                    && option.current_value.as_ref() == ENABLED_VALUE
            }));
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn revoked_model_preparation_does_not_install_or_checkpoint() {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let installed = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::clone(&installed));
            let publication = CheckpointPublication::default();
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            coordinator
                .read
                .binding
                .lock_unpoisoned()
                .as_mut()
                .unwrap()
                .prepare_model = Arc::new(move |spec| {
                let started_tx = started_tx.clone();
                let release_rx = release_rx.clone();
                Box::pin(async move {
                    started_tx.send_async(()).await.unwrap();
                    release_rx.recv_async().await.unwrap();
                    Ok(PreparedModel {
                        provider: Arc::new(NullProvider),
                        model: test_model(&spec),
                    })
                })
            });
            let before = coordinator.read().options();
            let operation = smol::spawn({
                let coordinator = coordinator.clone();
                let publication = publication.clone();
                async move {
                    coordinator
                        .set_model_with_claim(
                            ModelSelection {
                                spec: Some(Arc::from(NEW)),
                                ..Default::default()
                            },
                            publication,
                            publication_claim(),
                        )
                        .await
                }
            });
            started_rx.recv_async().await.unwrap();
            assert!(
                futures_lite::future::poll_once(
                    coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                )
                .await
                .is_none()
            );
            publication.cancel();
            release_tx.send_async(()).await.unwrap();
            assert!(operation.await.is_err());
            assert_eq!(coordinator.read().options(), before);
            assert_eq!(agent.effective_config().unwrap().unwrap().model.spec(), OLD);
            assert!(saved.lock_unpoisoned().is_empty());
            assert!(installed.lock_unpoisoned().is_empty());
            coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false, false; "reload_commits")]
    #[test_case(true, false; "reload_checkpoint_fails")]
    #[test_case(false, true; "reload_cancelled_before_claim")]
    fn context_reload_installs_at_authorized_admission(fail: bool, cancel: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail,
                        saved: Arc::clone(&saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed: false,
                        reached,
                        release: proceed,
                    }))),
                }),
            ))
            .unwrap();
            let directory = tempfile::tempdir().unwrap();
            smol::fs::write(directory.path().join("AGENTS.md"), RELOAD_INSTRUCTIONS)
                .await
                .unwrap();
            let mut config = (*projection_config(false)).clone();
            config.context = AgentContextSnapshot::new(directory.path().to_path_buf());
            let setup = TurnSetup {
                registry: Arc::new(ToolRegistry::new()),
                local_tools: LocalTools::default(),
                mcp: None,
                slots: PromptSlotSource::default(),
                modes: Arc::new(ModeRegistry::builtin()),
                instructions: true,
            };
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let agent = manager
                .create_root(Vec::new(), None, Box::new(ContextBackend(setup)))
                .unwrap();
            let actor = agent.actor().unwrap();
            actor.initialize_config(config).unwrap();
            bind_test_permissions(&coordinator, &agent);
            coordinator
                .bind_actor(
                    agent.clone(),
                    Arc::new(|_| Box::pin(async { unreachable!() })),
                    Arc::new(|_| {}),
                )
                .unwrap();
            let journal = manager.journal();
            let access = journal.state_access(agent.id(), None);
            let before = journal.snapshot();
            let options = coordinator.read().options();
            let generation = actor.config_snapshot().unwrap().generation;
            let reload = smol::spawn({
                let coordinator = coordinator.clone();
                async move { coordinator.reload_context().await }
            });
            waiting.recv_async().await.unwrap();
            assert_eq!(journal.snapshot().state, before.state);
            assert!(root_context(&access).unwrap().is_none());
            if cancel {
                reload.cancel().await;
                release.send_async(()).await.unwrap();
                let barrier = actor.reserve_prepared_operation(None).unwrap();
                barrier.wait_predecessor().await.unwrap();
            } else {
                release.send_async(()).await.unwrap();
                let result = reload.await;
                assert_eq!(result.is_err(), fail);
            }
            assert_eq!(actor.config_snapshot().unwrap().generation, generation);
            if cancel {
                assert_eq!(journal.snapshot().state, before.state);
                assert_eq!(coordinator.read().options(), options);
                assert!(root_context(&access).unwrap().is_none());
                assert!(saved.lock_unpoisoned().is_empty());
            } else if fail {
                assert!(root_context(&access).unwrap().is_some());
                assert!(journal.snapshot().revision > before.revision);
                assert!(saved.lock_unpoisoned().is_empty());
            } else {
                assert!(root_context(&access).unwrap().is_some());
                let saved = saved.lock_unpoisoned();
                assert_eq!(saved.len(), 1);
                assert!(
                    saved[0]
                        .journal
                        .as_ref()
                        .unwrap()
                        .state
                        .iter()
                        .any(|record| { matches!(&record.change, StateChange::Context { .. }) })
                );
            }
            coordinator.close().await.unwrap();
        });
    }

    struct ContextReloadFixture {
        coordinator: SessionCoordinatorHandle,
        agent: AgentRef,
        directory: tempfile::TempDir,
        saved: Arc<Mutex<Vec<Arc<SessionCheckpoint>>>>,
    }

    impl ContextReloadFixture {
        const INSTRUCTIONS_FILE: &str = "AGENTS.md";

        async fn new() -> Self {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let directory = tempfile::tempdir().unwrap();
            smol::fs::write(
                directory
                    .path()
                    .join(ContextReloadFixture::INSTRUCTIONS_FILE),
                RELOAD_INSTRUCTIONS,
            )
            .await
            .unwrap();
            let mut config = (*projection_config(false)).clone();
            config.context = AgentContextSnapshot::new(directory.path().to_path_buf());
            let setup = TurnSetup {
                registry: Arc::new(ToolRegistry::new()),
                local_tools: LocalTools::default(),
                mcp: None,
                slots: PromptSlotSource::default(),
                modes: Arc::new(ModeRegistry::builtin()),
                instructions: true,
            };
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let agent = manager
                .create_root(Vec::new(), None, Box::new(ContextBackend(setup)))
                .unwrap();
            agent.actor().unwrap().initialize_config(config).unwrap();
            bind_test_permissions(&coordinator, &agent);
            coordinator
                .bind_actor(
                    agent.clone(),
                    Arc::new(|_| Box::pin(async { unreachable!() })),
                    Arc::new(|_| {}),
                )
                .unwrap();
            Self {
                coordinator,
                agent,
                directory,
                saved,
            }
        }
    }

    #[test_case(false; "supplied_publication_cancelled")]
    #[test_case(true; "unpolled_ticket_dropped")]
    fn context_reload_ticket_retirement_has_no_side_effects(drop_ticket: bool) {
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let actor = fixture.agent.actor().unwrap();
            let journal = fixture.agent.manager().journal();
            let before = journal.snapshot();
            let options = fixture.coordinator.read().options();
            let generation = actor.config_snapshot().unwrap().generation;
            let access = journal.state_access(fixture.agent.id(), None);
            let publication = CheckpointPublication::default();
            let ticket = fixture
                .coordinator
                .reserve_context_reload_for_actor(
                    actor.clone(),
                    || Ok(()),
                    publication.clone(),
                    publication_claim(),
                )
                .unwrap();
            if drop_ticket {
                drop(ticket);
            } else {
                assert!(publication.cancel());
                assert!(ticket.commit().await.is_err());
            }
            assert!(publication.is_cancelled());
            assert_eq!(journal.snapshot().state, before.state);
            assert_eq!(fixture.coordinator.read().options(), options);
            assert_eq!(actor.config_snapshot().unwrap().generation, generation);
            assert!(root_context(&access).unwrap().is_none());
            assert!(fixture.saved.lock_unpoisoned().is_empty());
            let barrier = actor.reserve_prepared_operation(None).unwrap();
            barrier.wait_predecessor().await.unwrap();
            drop(barrier);
            actor.shutdown();
            fixture.coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn context_reload_ticket_revalidates_after_busy_option_lease() {
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let actor = fixture.agent.actor().unwrap();
            let journal = fixture.agent.manager().journal();
            let before = journal.snapshot();
            let held = fixture.coordinator.reserve_options().unwrap();
            let valid = Arc::new(AtomicBool::new(true));
            let (checked, checks) = flume::unbounded();
            let publication = CheckpointPublication::default();
            let ticket = fixture
                .coordinator
                .reserve_context_reload_for_actor(
                    actor.clone(),
                    {
                        let valid = Arc::clone(&valid);
                        move || {
                            let admitted = valid.load(Ordering::SeqCst);
                            checked.send(()).unwrap();
                            if admitted {
                                Ok(())
                            } else {
                                Err(ActorError::PolicyCancelled.into())
                            }
                        }
                    },
                    publication.clone(),
                    publication_claim(),
                )
                .unwrap();
            while checks.try_recv().is_ok() {}
            let commit = smol::spawn(async move { ticket.commit().await });
            checks.recv_async().await.unwrap();
            checks.recv_async().await.unwrap();
            valid.store(false, Ordering::SeqCst);
            drop(held);
            actor.inner.policy_changed.notify(usize::MAX);
            assert!(commit.await.is_err());
            assert!(publication.is_cancelled());
            assert_eq!(journal.snapshot().state, before.state);
            assert!(fixture.saved.lock_unpoisoned().is_empty());
            actor.shutdown();
            fixture.coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn context_reload_ticket_uses_supplied_publication_claim() {
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let actor = fixture.agent.actor().unwrap();
            let publication = CheckpointPublication::default();
            let claimed = Arc::new(AtomicBool::new(false));
            let claim: CheckpointClaim = {
                let expected = publication.clone();
                let claimed = Arc::clone(&claimed);
                Arc::new(move |received| {
                    assert!(!claimed.swap(true, Ordering::SeqCst));
                    assert!(received.same_request(&expected));
                    if !received.try_publish() {
                        return Err(Arc::from(CHECKPOINT_CANCELLED));
                    }
                    Ok(())
                })
            };
            fixture
                .coordinator
                .reserve_context_reload_for_actor(
                    actor.clone(),
                    || Ok(()),
                    publication.clone(),
                    claim,
                )
                .unwrap()
                .commit()
                .await
                .unwrap();
            assert!(claimed.load(Ordering::SeqCst));
            assert!(!publication.is_cancelled());
            assert!(!publication.cancel());
            assert_eq!(fixture.saved.lock_unpoisoned().len(), 1);
            let access = fixture
                .agent
                .manager()
                .journal()
                .state_access(fixture.agent.id(), None);
            assert!(root_context(&access).unwrap().is_some());
            actor.shutdown();
            fixture.coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "uninitialized_other_root")]
    #[test_case(true; "recorded_other_root")]
    fn ensure_root_context_rejects_other_actor_before_read_or_initialization(recorded: bool) {
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let other = ContextReloadFixture::new().await;
            if recorded {
                fixture
                    .coordinator
                    .ensure_root_context_for_actor(&fixture.agent.actor().unwrap())
                    .await
                    .unwrap();
            }
            let journal = fixture.agent.manager().journal();
            let other_journal = other.agent.manager().journal();
            let before = journal.snapshot();
            let other_before = other_journal.snapshot();
            let checkpoints = fixture.saved.lock_unpoisoned().len();
            let result = fixture
                .coordinator
                .ensure_root_context_for_actor(&other.agent.actor().unwrap())
                .await;
            assert!(matches!(
                result,
                Err(ConfigChangeError::Actor(ActorError::PolicyCancelled))
            ));
            assert_eq!(journal.snapshot().state, before.state);
            assert_eq!(other_journal.snapshot().state, other_before.state);
            assert_eq!(fixture.saved.lock_unpoisoned().len(), checkpoints);
            assert!(other.saved.lock_unpoisoned().is_empty());
            fixture.agent.actor().unwrap().shutdown();
            other.agent.actor().unwrap().shutdown();
            fixture.coordinator.close().await.unwrap();
            other.coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn context_reload_ticket_rejects_mismatched_actor() {
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let other = ContextReloadFixture::new().await;
            let journal = fixture.agent.manager().journal();
            let other_journal = other.agent.manager().journal();
            let before = journal.snapshot();
            let other_before = other_journal.snapshot();
            let publication = CheckpointPublication::default();
            let result = fixture.coordinator.reserve_context_reload_for_actor(
                other.agent.actor().unwrap(),
                || Ok(()),
                publication.clone(),
                publication_claim(),
            );
            assert!(result.is_err());
            assert!(publication.is_cancelled());
            assert_eq!(journal.snapshot().state, before.state);
            assert_eq!(other_journal.snapshot().state, other_before.state);
            assert!(fixture.saved.lock_unpoisoned().is_empty());
            assert!(other.saved.lock_unpoisoned().is_empty());
            fixture.agent.actor().unwrap().shutdown();
            other.agent.actor().unwrap().shutdown();
            fixture.coordinator.close().await.unwrap();
            other.coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn ensure_root_context_initializes_before_first_turn_and_reuses_recorded_context() {
        use crate::actor::ActorStatus;

        const CHANGED_INSTRUCTIONS: &str = "Do not adopt these instructions during startup reuse.";
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let actor = fixture.agent.actor().unwrap();
            let journal = fixture.agent.manager().journal();
            let access = journal.state_access(fixture.agent.id(), None);
            let generation = actor.config_snapshot().unwrap().generation;
            assert!(root_context(&access).unwrap().is_none());
            assert!(actor.latest_ticket().is_none());
            let initialized = fixture
                .coordinator
                .ensure_root_context_for_actor(&actor)
                .await
                .unwrap();
            assert!(initialized.instructions.text.contains(RELOAD_INSTRUCTIONS));
            assert_eq!(
                root_context(&access).unwrap().unwrap().instructions.text,
                initialized.instructions.text
            );
            let recorded = journal.snapshot();
            {
                let saved = fixture.saved.lock_unpoisoned();
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].journal.as_ref().unwrap().state, recorded.state);
            }
            smol::fs::write(
                fixture
                    .directory
                    .path()
                    .join(ContextReloadFixture::INSTRUCTIONS_FILE),
                CHANGED_INSTRUCTIONS,
            )
            .await
            .unwrap();
            let reused = fixture
                .coordinator
                .ensure_root_context_for_actor(&actor)
                .await
                .unwrap();
            assert_eq!(reused.instructions.text, initialized.instructions.text);
            assert!(!reused.instructions.text.contains(CHANGED_INSTRUCTIONS));
            assert_eq!(journal.snapshot().state, recorded.state);
            assert_eq!(journal.snapshot().revision, recorded.revision);
            assert_eq!(fixture.saved.lock_unpoisoned().len(), 1);
            let snapshot = actor.snapshot();
            assert_eq!(snapshot.status, ActorStatus::Idle);
            assert!(snapshot.active_turn.is_none());
            assert_eq!(snapshot.queued, 0);
            assert!(actor.latest_ticket().is_none());
            assert_eq!(actor.config_snapshot().unwrap().generation, generation);
            actor.shutdown();
            fixture.coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn concurrent_missing_root_context_ensures_share_first_initialization() {
        const LOCAL_INSTRUCTIONS_FILE: &str = "AGENTS.local.md";
        const LOCAL_INSTRUCTIONS: &str = "Do not collect this source in the queued startup ensure.";
        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let actor = fixture.agent.actor().unwrap();
            let journal = fixture.agent.manager().journal();
            let access = journal.state_access(fixture.agent.id(), None);
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            fixture
                .coordinator
                .rebind_checkpoint_writer(Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: false,
                        saved: Arc::clone(&fixture.saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed: false,
                        reached,
                        release: proceed,
                    }))),
                }))
                .unwrap();
            let first = smol::spawn({
                let coordinator = fixture.coordinator.clone();
                let actor = actor.clone();
                async move { coordinator.ensure_root_context_for_actor(&actor).await }
            });
            waiting.recv_async().await.unwrap();
            assert!(root_context(&access).unwrap().is_none());
            let mut second = Box::pin(fixture.coordinator.ensure_root_context_for_actor(&actor));
            assert!(
                futures_lite::future::poll_once(second.as_mut())
                    .await
                    .is_none()
            );
            assert!(!actor.inner.config_waiters.lock_unpoisoned().is_empty());
            smol::fs::write(
                fixture.directory.path().join(LOCAL_INSTRUCTIONS_FILE),
                LOCAL_INSTRUCTIONS,
            )
            .await
            .unwrap();
            release.send_async(()).await.unwrap();
            let initialized = first.await.unwrap();
            let recorded = journal.snapshot();
            let reused = second.await.unwrap();
            assert!(initialized.instructions.text.contains(RELOAD_INSTRUCTIONS));
            assert_eq!(reused.instructions.text, initialized.instructions.text);
            assert!(!reused.instructions.text.contains(LOCAL_INSTRUCTIONS));
            assert!(!reused.instructions.sources.iter().any(|source| {
                source
                    .path
                    .file_name()
                    .is_some_and(|name| name == LOCAL_INSTRUCTIONS_FILE)
            }));
            assert_eq!(journal.snapshot().state, recorded.state);
            assert_eq!(journal.snapshot().revision, recorded.revision);
            assert_eq!(fixture.saved.lock_unpoisoned().len(), 1);
            assert!(actor.latest_ticket().is_none());
            assert!(actor.inner.config_waiters.lock_unpoisoned().is_empty());
            actor.shutdown();
            fixture.coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn ensure_root_context_checkpoint_failure_retains_admitted_initialization() {
        use crate::actor::ActorStatus;

        smol::block_on(async {
            let fixture = ContextReloadFixture::new().await;
            let actor = fixture.agent.actor().unwrap();
            let journal = fixture.agent.manager().journal();
            let access = journal.state_access(fixture.agent.id(), None);
            let before = journal.snapshot();
            let options = fixture.coordinator.read().options();
            let generation = actor.config_snapshot().unwrap().generation;
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            fixture
                .coordinator
                .rebind_checkpoint_writer(Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: true,
                        saved: Arc::clone(&fixture.saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed: false,
                        reached,
                        release: proceed,
                    }))),
                }))
                .unwrap();
            let ensure = smol::spawn({
                let coordinator = fixture.coordinator.clone();
                let actor = actor.clone();
                async move { coordinator.ensure_root_context_for_actor(&actor).await }
            });
            waiting.recv_async().await.unwrap();
            assert!(root_context(&access).unwrap().is_none());
            assert_eq!(journal.snapshot().state, before.state);
            assert!(fixture.saved.lock_unpoisoned().is_empty());
            assert_eq!(actor.snapshot().status, ActorStatus::Idle);
            assert!(actor.latest_ticket().is_none());
            release.send_async(()).await.unwrap();
            assert!(ensure.await.is_err());
            assert!(root_context(&access).unwrap().is_some());
            assert!(journal.snapshot().revision > before.revision);
            assert_eq!(fixture.coordinator.read().options(), options);
            assert!(fixture.saved.lock_unpoisoned().is_empty());
            assert_eq!(actor.snapshot().status, ActorStatus::Idle);
            assert!(actor.latest_ticket().is_none());
            assert_eq!(actor.config_snapshot().unwrap().generation, generation);
            fixture
                .coordinator
                .rebind_checkpoint_writer(Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&fixture.saved),
                }))
                .unwrap();
            let initialized = fixture
                .coordinator
                .ensure_root_context_for_actor(&actor)
                .await
                .unwrap();
            assert!(initialized.instructions.text.contains(RELOAD_INSTRUCTIONS));
            assert!(root_context(&access).unwrap().is_some());
            assert!(fixture.saved.lock_unpoisoned().is_empty());
            assert!(actor.latest_ticket().is_none());
            actor.shutdown();
            fixture.coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "directory_published")]
    #[test_case(true; "directory_checkpoint_failed")]
    fn directory_checkpoint_failure_preserves_runtime(fail: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let installed = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::clone(&installed));
            let directory = tempfile::tempdir().unwrap();
            let canonical = smol::fs::canonicalize(directory.path()).await.unwrap();
            let before = coordinator.read().cwd().unwrap();
            let before_options = coordinator.read().options();
            let result = coordinator
                .change_directory(directory.path().to_path_buf())
                .await;
            if fail {
                assert!(result.is_err());
                assert_eq!(coordinator.read().cwd().unwrap(), before);
                assert_eq!(
                    agent.effective_config().unwrap().unwrap().context.cwd(),
                    before
                );
                assert_eq!(coordinator.read().options(), before_options);
                assert!(installed.lock_unpoisoned().is_empty());
                assert!(saved.lock_unpoisoned().is_empty());
            } else {
                assert_eq!(result.unwrap(), canonical);
                assert_eq!(coordinator.read().cwd().unwrap(), canonical);
                assert_eq!(
                    agent.effective_config().unwrap().unwrap().context.cwd(),
                    canonical
                );
                assert_eq!(
                    saved.lock_unpoisoned()[0]
                        .config
                        .as_ref()
                        .unwrap()
                        .config
                        .context
                        .cwd(),
                    canonical
                );
                assert_eq!(installed.lock_unpoisoned().len(), 1);
                coordinator
                    .change_directory(PathBuf::from("."))
                    .await
                    .unwrap();
                assert_eq!(coordinator.read().cwd().unwrap(), canonical);
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "committed")]
    #[test_case(true; "save_failed")]
    fn fast_toggle_cancels_pending_wish_at_the_durable_commit_boundary(fail: bool) {
        smol::block_on(futures_lite::future::or(
            async {
                const MODEL: &str = "provider/pending";
                let saved = Arc::new(Mutex::new(Vec::new()));
                let (reached, waiting) = flume::bounded(1);
                let (release, proceed) = flume::bounded(1);
                let coordinator = SessionCoordinatorHandle::register(params(
                    MakimaId::generate(),
                    Arc::new(GatedWriter {
                        writer: GuardedWriter {
                            fail,
                            saved: Arc::clone(&saved),
                        },
                        gate: Arc::new(Mutex::new(Some(PublicationGate {
                            claimed: true,
                            reached,
                            release: proceed,
                        }))),
                    }),
                ))
                .unwrap();
                let mut config = (*projection_config(false)).clone();
                config.settings.model = Model::from_spec(MODEL).unwrap();
                config.settings.model.fast_support = Some(FastSupport::Pending);
                let agent = bind_config(&coordinator, config, Arc::default());
                let journal = agent.manager().journal();
                let mut options = coordinator.read().options().config_values();
                options.insert(FAST_PENDING_MODEL_OPTION_ID.into(), MODEL.into());
                journal
                    .record_config(
                        agent.id(),
                        0,
                        stored_agent_config(
                            &agent.effective_config().unwrap().unwrap(),
                            options.clone(),
                        )
                        .unwrap(),
                        Some(options),
                    )
                    .unwrap();
                let before = journal.snapshot().state;
                let toggle = smol::spawn({
                    let coordinator = coordinator.clone();
                    async move { coordinator.toggle_boolean_option(FAST_OPTION_ID).await }
                });
                waiting.recv_async().await.unwrap();
                assert!(!agent.effective_config().unwrap().unwrap().fast);
                assert_eq!(
                    coordinator
                        .read()
                        .options()
                        .values()
                        .get(FAST_PENDING_MODEL_OPTION_ID)
                        .map(String::as_str),
                    None
                );
                assert!(journal.snapshot().revision > 0);
                release.send_async(()).await.unwrap();
                let result = toggle.await;
                if fail {
                    assert!(result.is_err());
                    assert_eq!(
                        coordinator
                            .read()
                            .options()
                            .values()
                            .get(FAST_PENDING_MODEL_OPTION_ID)
                            .map(String::as_str),
                        None
                    );
                    assert_eq!(
                        journal.snapshot().state.iter().len(),
                        before.iter().len() + 1
                    );
                    assert!(!agent.effective_config().unwrap().unwrap().fast);
                    assert!(saved.lock_unpoisoned().is_empty());
                } else {
                    let (enabled, snapshot) = result.unwrap();
                    assert!(!enabled);
                    assert!(!snapshot.values().contains_key(FAST_PENDING_MODEL_OPTION_ID));
                    assert!(!agent.effective_config().unwrap().unwrap().fast);
                    let after = journal.snapshot();
                    assert_eq!(after.state.iter().len(), before.iter().len() + 1);
                    let StateChange::Config(config) =
                        &after.state.iter().next_back().unwrap().change
                    else {
                        panic!("expected configuration record")
                    };
                    assert!(!config.fast);
                    assert!(!config.options.contains_key(FAST_PENDING_MODEL_OPTION_ID));
                    assert!(
                        !saved.lock_unpoisoned()[0]
                            .options
                            .values()
                            .contains_key(FAST_PENDING_MODEL_OPTION_ID)
                    );
                }
                coordinator.close().await.unwrap();
            },
            async {
                smol::Timer::after(COMPLETION_TIMEOUT).await;
                panic!("{FAST_TOGGLE_TIMEOUT}");
            },
        ));
    }

    #[test_case(false; "saved")]
    #[test_case(true; "save_failed")]
    fn explicit_fast_off_consumes_pending_intent_only_after_successful_publication(fail: bool) {
        smol::block_on(async {
            const MODEL: &str = "provider/pending";
            let saved = Arc::new(Mutex::new(Vec::new()));
            let mut parameters = params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail,
                    saved: Arc::clone(&saved),
                }),
            );
            parameters.persisted_options.insert(
                crate::session_options::FAST_PENDING_MODEL_OPTION_ID.into(),
                MODEL.into(),
            );
            let coordinator = SessionCoordinatorHandle::register(parameters).unwrap();
            let mut config = (*projection_config(false)).clone();
            config.settings.model = Model::from_spec(MODEL).unwrap();
            config.settings.model.fast_support = Some(FastSupport::Pending);
            let agent = bind_config(&coordinator, config, Arc::default());
            let journal = agent.manager().journal();
            let options = coordinator.read.options.snapshot().values();
            let initial =
                stored_agent_config(&agent.effective_config().unwrap().unwrap(), options.clone())
                    .unwrap();
            journal
                .record_config(agent.id(), 0, initial, Some(options))
                .unwrap();
            let before = journal.snapshot();
            let result = coordinator.set_option(FAST_OPTION_ID, DISABLED_VALUE).await;
            if fail {
                assert!(result.is_err());
                assert_eq!(
                    coordinator
                        .read()
                        .options()
                        .values()
                        .get(FAST_PENDING_MODEL_OPTION_ID)
                        .map(String::as_str),
                    Some(MODEL)
                );
                assert_eq!(journal.snapshot().state, before.state);
                assert!(saved.lock_unpoisoned().is_empty());
            } else {
                assert!(
                    !result
                        .unwrap()
                        .values()
                        .contains_key(FAST_PENDING_MODEL_OPTION_ID)
                );
                let snapshot = journal.snapshot();
                let StateChange::Config(config) =
                    &snapshot.state.iter().next_back().unwrap().change
                else {
                    panic!("expected configuration record")
                };
                assert!(!config.fast);
                assert!(
                    !config
                        .options
                        .contains_key(crate::session_options::FAST_PENDING_MODEL_OPTION_ID)
                );
                assert_eq!(snapshot.state.iter().len(), before.state.iter().len() + 1);
                assert!(
                    !saved.lock_unpoisoned()[0]
                        .options
                        .values()
                        .contains_key(FAST_PENDING_MODEL_OPTION_ID)
                );
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false, false; "set_dropped_before_claim")]
    #[test_case(true, false; "set_dropped_after_claim")]
    #[test_case(false, true; "toggle_dropped_before_claim")]
    #[test_case(true, true; "toggle_dropped_after_claim")]
    fn dropped_option_waiter_observes_publication_claim(claimed: bool, toggle: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: false,
                        saved: Arc::clone(&saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed,
                        reached,
                        release: proceed,
                    }))),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::default());
            let actor = agent.actor().unwrap();
            let permissions = coordinator.permissions().unwrap();
            let before = coordinator.read().options();
            let journal = agent.manager().journal();
            let previous = journal.snapshot();
            let pending = actor.reserve_prepared_operation(None).unwrap();
            let mut operation = Box::pin(async {
                if toggle {
                    coordinator
                        .toggle_yolo_for_actor_with_claim(
                            actor.clone(),
                            || Ok(()),
                            CheckpointPublication::default(),
                            publication_claim(),
                        )
                        .await
                        .map(|(_, options)| options)
                } else {
                    coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await
                }
            });
            assert!(
                futures_lite::future::poll_once(operation.as_mut())
                    .await
                    .is_none()
            );
            waiting.recv_async().await.unwrap();
            assert_eq!(permissions.is_yolo(), claimed);
            assert_eq!(
                coordinator.read().options().enabled(YOLO_OPTION_ID),
                claimed
            );
            if !claimed {
                assert_eq!(coordinator.read().options().version, before.version);
            }
            drop(operation);
            release.send_async(()).await.unwrap();
            coordinator.barrier().await;
            assert!(Arc::ptr_eq(
                &permissions,
                &coordinator.permissions().unwrap()
            ));
            assert_eq!(permissions.is_yolo(), claimed);
            assert_eq!(
                coordinator.read().options().enabled(YOLO_OPTION_ID),
                claimed
            );
            assert_eq!(saved.lock_unpoisoned().len(), usize::from(claimed));
            if claimed {
                assert!(journal.snapshot().state.iter().any(|record| matches!(
                    record.change,
                    StateChange::Rules {
                        yolo: Some(true),
                        ..
                    }
                )));
                assert!(coordinator.read().options().version > before.version);
            } else {
                assert_eq!(journal.snapshot().state, previous.state);
                assert_eq!(coordinator.read().options().version, before.version);
            }
            pending
                .commit_when_ready(Some(ConfigChange::ToggleWorkflow))
                .await
                .unwrap();
            assert!(actor.effective_config().unwrap().workflow);
            coordinator.close().await.unwrap();
            agent.close_subtree().unwrap();
        });
    }

    #[test_case(false, false; "lease_then_cancel")]
    #[test_case(false, true; "lease_then_publish")]
    #[test_case(true, false; "idle_then_cancel")]
    #[test_case(true, true; "idle_then_publish")]
    fn queued_option_setter_keeps_supplied_publication_fence(idle: bool, publish: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::default());
            let lease = (!idle).then(|| coordinator.reserve_options().unwrap());
            let guard = idle.then(|| agent.actor().unwrap().prepare_idle().unwrap());
            let publication = CheckpointPublication::default();
            let mut operation = Box::pin(coordinator.set_option_if_version_with_claim(
                WORKFLOW_OPTION_ID,
                ENABLED_VALUE,
                None,
                publication.clone(),
                publication_claim(),
            ));
            assert!(
                futures_lite::future::poll_once(operation.as_mut())
                    .await
                    .is_none()
            );
            assert!(!publication.is_cancelled());
            if !publish {
                publication.cancel();
            }
            drop(lease);
            drop(guard);
            assert_eq!(operation.await.is_ok(), publish);
            assert_eq!(agent.effective_config().unwrap().unwrap().workflow, publish);
            assert_eq!(
                coordinator.read().options().enabled(WORKFLOW_OPTION_ID),
                publish
            );
            assert_eq!(saved.lock_unpoisoned().len(), usize::from(publish));
            coordinator.close().await.unwrap();
            agent.close_subtree().unwrap();
        });
    }

    #[test_case(false, YOLO_OPTION_ID; "rules_cancelled_before_claim")]
    #[test_case(true, YOLO_OPTION_ID; "rules_cancelled_after_claim")]
    #[test_case(false, WORKFLOW_OPTION_ID; "config_cancelled_before_claim")]
    #[test_case(true, WORKFLOW_OPTION_ID; "config_cancelled_after_claim")]
    fn option_setter_preserves_supplied_publication_claim(claimed: bool, id: &str) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: false,
                        saved: Arc::clone(&saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed,
                        reached,
                        release: proceed,
                    }))),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::default());
            let before = coordinator.read().options();
            let publication = CheckpointPublication::default();
            let (trigger, invocation) = CancelToken::new();
            let _registration = invocation.register_publication(&publication);
            let claim: CheckpointClaim = Arc::new(move |publication| {
                if invocation.claim_publication(publication) {
                    Ok(())
                } else {
                    Err(Arc::from(CHECKPOINT_CANCELLED))
                }
            });
            let operation = smol::spawn({
                let coordinator = coordinator.clone();
                let id: Arc<str> = Arc::from(id);
                async move {
                    coordinator
                        .set_option_if_version_with_claim(
                            id,
                            ENABLED_VALUE,
                            Some(before.version),
                            publication,
                            claim,
                        )
                        .await
                }
            });
            futures_lite::future::or(async { waiting.recv_async().await.unwrap() }, async {
                smol::Timer::after(COMPLETION_TIMEOUT).await;
                panic!("{YOLO_PUBLICATION_TIMEOUT}");
            })
            .await;
            trigger.cancel();
            release.send_async(()).await.unwrap();
            assert_eq!(operation.await.is_ok(), claimed);
            assert_eq!(coordinator.read().options().enabled(id), claimed);
            assert_eq!(saved.lock_unpoisoned().len(), usize::from(claimed));
            assert_eq!(
                agent.effective_config().unwrap().unwrap().workflow,
                claimed && id == WORKFLOW_OPTION_ID
            );
            assert_eq!(
                coordinator.permissions().unwrap().is_yolo(),
                claimed && id == YOLO_OPTION_ID
            );
            coordinator.close().await.unwrap();
            agent.close_subtree().unwrap();
        });
    }

    #[test_case(false, false; "actor_closed_before_claim")]
    #[test_case(true, false; "actor_closed_after_claim")]
    #[test_case(false, true; "incarnation_detached_before_claim")]
    #[test_case(true, true; "incarnation_detached_after_claim")]
    fn yolo_publication_fences_target_without_claim_validation(claimed: bool, detach: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let (reached, waiting) = flume::bounded(1);
            let (release, proceed) = flume::bounded(1);
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GatedWriter {
                    writer: GuardedWriter {
                        fail: false,
                        saved: Arc::clone(&saved),
                    },
                    gate: Arc::new(Mutex::new(Some(PublicationGate {
                        claimed,
                        reached,
                        release: proceed,
                    }))),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::default());
            let actor = agent.actor().unwrap();
            let permissions = coordinator.permissions().unwrap();
            let validating = Arc::new(AtomicBool::new(true));
            let operation = smol::spawn({
                let coordinator = coordinator.clone();
                let actor = actor.clone();
                let validating = Arc::clone(&validating);
                async move {
                    coordinator
                        .toggle_yolo_for_actor_with_claim(
                            actor,
                            move || {
                                assert!(validating.load(Ordering::SeqCst));
                                Ok(())
                            },
                            CheckpointPublication::default(),
                            publication_claim(),
                        )
                        .await
                }
            });
            waiting.recv_async().await.unwrap();
            validating.store(false, Ordering::SeqCst);
            if detach {
                agent
                    .manager()
                    .detach_incarnation(&agent.incarnation().unwrap())
                    .unwrap();
            } else {
                actor.close();
            }
            release.send_async(()).await.unwrap();
            assert_eq!(operation.await.is_ok(), claimed);
            coordinator.barrier().await;
            assert_eq!(permissions.is_yolo(), claimed);
            assert_eq!(saved.lock_unpoisoned().len(), usize::from(claimed));
            assert_eq!(
                coordinator.read().options().enabled(YOLO_OPTION_ID),
                claimed
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "published")]
    #[test_case(true; "disk_failed")]
    fn nonpersistent_yolo_toggle_installs_only_after_publication_claim(fail: bool) {
        smol::block_on(futures_lite::future::or(
            async {
                let saved = Arc::new(Mutex::new(Vec::new()));
                let coordinator = SessionCoordinatorHandle::register(params(
                    MakimaId::generate(),
                    Arc::new(GuardedWriter {
                        fail,
                        saved: Arc::clone(&saved),
                    }),
                ))
                .unwrap();
                let journal = Arc::new(SessionJournal::nonpersistent());
                let manager =
                    AgentManagerHandle::new_with_journal(Default::default(), Arc::clone(&journal))
                        .unwrap();
                let root = manager
                    .create_root_with_config(
                        Some((*projection_config(false)).clone()),
                        Vec::new(),
                        None,
                        |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(MockBackend::STRICT)),
                    )
                    .unwrap();
                assert!(root.actor().unwrap().inner.journal.is_none());
                let cwd = PathBuf::from("/project");
                let permissions = Arc::new(PermissionManager::new(
                    makima_config::PermissionsConfig {
                        yolo: true,
                        ..Default::default()
                    },
                    cwd.clone(),
                    makima_config::ProjectConfig::for_project(&cwd),
                    Arc::default(),
                ));
                permissions
                    .bind_journal(Arc::clone(&journal), Some(journal.incarnation(root.id())))
                    .unwrap();
                coordinator
                    .bind_permissions(Arc::clone(&permissions))
                    .unwrap();
                coordinator
                    .bind_actor(
                        root.clone(),
                        Arc::new(|_| Box::pin(async { unreachable!() })),
                        Arc::new(|_| {}),
                    )
                    .unwrap();
                assert!(!coordinator.read.options.snapshot().enabled(YOLO_OPTION_ID));
                assert!(coordinator.read().options().enabled(YOLO_OPTION_ID));
                let result = coordinator
                    .toggle_yolo_for_actor_with_claim(
                        root.actor().unwrap(),
                        || Ok(()),
                        CheckpointPublication::default(),
                        publication_claim(),
                    )
                    .await;
                if fail {
                    assert!(result.is_err());
                    assert!(permissions.is_yolo());
                    assert_eq!(permissions.snapshot().yolo, None);
                    assert!(saved.lock_unpoisoned().is_empty());
                } else {
                    let (enabled, snapshot) = result.unwrap();
                    assert!(!enabled);
                    assert!(!snapshot.enabled(YOLO_OPTION_ID));
                    assert!(!permissions.is_yolo());
                    assert_eq!(permissions.snapshot().yolo, Some(false));
                    coordinator
                        .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                        .await
                        .unwrap();
                    assert!(permissions.is_yolo());
                    assert_eq!(permissions.snapshot().yolo, Some(true));
                }
                coordinator.close().await.unwrap();
                root.close_subtree().unwrap();
            },
            async {
                smol::Timer::after(COMPLETION_TIMEOUT).await;
                panic!("{YOLO_PUBLICATION_TIMEOUT}");
            },
        ));
    }

    #[test_case(true; "enabled_seed")]
    #[test_case(false; "disabled_seed")]
    fn explicit_yolo_matching_invocation_seed_survives_restart(seed: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let mut parameters = params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            );
            parameters.definitions = builtin_option_definitions(
                OLD,
                [Arc::from(OLD)],
                seed,
                false,
                false,
                ThinkingConfig::Off,
            );
            let coordinator = SessionCoordinatorHandle::register(parameters).unwrap();
            let manager = AgentManagerHandle::new(Default::default()).unwrap();
            let agent = manager
                .create_root_with_config(
                    Some((*projection_config(false)).clone()),
                    Vec::new(),
                    None,
                    |_| Ok::<Box<dyn ActorBackend>, String>(Box::new(MockBackend::STRICT)),
                )
                .unwrap();
            let cwd = PathBuf::from("/project");
            let permissions = Arc::new(PermissionManager::new(
                makima_config::PermissionsConfig {
                    yolo: seed,
                    ..Default::default()
                },
                cwd.clone(),
                makima_config::ProjectConfig::for_project(&cwd),
                Arc::default(),
            ));
            permissions
                .bind_journal(
                    manager.journal(),
                    agent.actor().unwrap().inner.journal_incarnation.clone(),
                )
                .unwrap();
            coordinator
                .bind_permissions(Arc::clone(&permissions))
                .unwrap();
            coordinator
                .bind_actor(
                    agent,
                    Arc::new(|_| Box::pin(async { unreachable!() })),
                    Arc::new(|_| {}),
                )
                .unwrap();
            assert_eq!(permissions.is_yolo(), seed);
            assert_eq!(permissions.snapshot().yolo, None);
            let value = if seed { ENABLED_VALUE } else { DISABLED_VALUE };
            coordinator.set_option(YOLO_OPTION_ID, value).await.unwrap();
            assert_eq!(permissions.snapshot().yolo, Some(seed));
            let published = saved
                .lock_unpoisoned()
                .last()
                .unwrap()
                .journal
                .clone()
                .unwrap();
            let folded = published.fold().unwrap();
            let rules = &folded.scopes[&None];
            assert_eq!(rules.yolo, Some(seed));
            let restarted = PermissionManager::new(
                Default::default(),
                cwd.clone(),
                makima_config::ProjectConfig::for_project(&cwd),
                Arc::default(),
            );
            assert!(!restarted.is_yolo());
            restarted.restore(PermissionSnapshot::from_stored(&rules.rules, rules.yolo).unwrap());
            assert_eq!(restarted.is_yolo(), seed);
            assert_eq!(restarted.snapshot().yolo, Some(seed));
            let before = manager.journal().snapshot().state;
            coordinator.set_option(YOLO_OPTION_ID, value).await.unwrap();
            assert_eq!(manager.journal().snapshot().state, before);
            assert_eq!(saved.lock_unpoisoned().len(), 1);
            coordinator.close().await.unwrap();
            manager
                .lookup(manager.root_id().unwrap())
                .unwrap()
                .close_subtree()
                .unwrap();
        });
    }

    #[test_case(false; "published")]
    #[test_case(true; "disk_failed")]
    fn yolo_publication_records_only_session_rules_and_preserves_permission_grants(fail: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakimaId::generate(),
                Arc::new(GuardedWriter {
                    fail,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::default());
            let permissions = coordinator
                .read
                .permissions
                .lock_unpoisoned()
                .clone()
                .unwrap();
            let rule = makima_config::PermissionRule {
                tool: makima_config::ToolKey::parse("read").unwrap(),
                scope: Some("/allowed".into()),
                effect: makima_config::Effect::Allow,
            };
            permissions.add_session_rule(rule);
            let options = coordinator.read().options().config_values();
            let config =
                stored_agent_config(&agent.effective_config().unwrap().unwrap(), options.clone())
                    .unwrap();
            let journal = agent.manager().journal();
            journal
                .record_config(agent.id(), 0, config, Some(options))
                .unwrap();
            let before = journal.snapshot();
            let result = coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await;
            if fail {
                assert!(result.is_err());
                assert!(!permissions.is_yolo());
                assert_eq!(journal.snapshot().state, before.state);
                assert!(saved.lock_unpoisoned().is_empty());
            } else {
                result.unwrap();
                assert!(permissions.is_yolo());
                let after = journal.snapshot();
                assert_eq!(after.state.iter().len(), before.state.iter().len() + 1);
                let last = after.state.iter().next_back().unwrap();
                assert!(last.agent.is_none());
                let StateChange::Rules { rules, yolo } = &last.change else {
                    panic!("expected session rules")
                };
                assert_eq!(*yolo, Some(true));
                assert_eq!(rules.len(), 1);
                assert_eq!(rules[0].scope.as_deref(), Some("/allowed"));
                assert_eq!(
                    saved.lock_unpoisoned()[0].journal.as_ref().unwrap().state,
                    after.state
                );
                coordinator
                    .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                    .await
                    .unwrap();
                assert_eq!(journal.snapshot().state, after.state);
            }
            assert_eq!(permissions.snapshot().rules.len(), 1);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn committed_projection_preserves_yolo_plugin_values_and_catalog() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let coordinator = register_with_catalog(MakimaId::generate(), writer(false), &catalog);
            catalog
                .replace_plugin_options("test", vec![plugin_option(1, &["a", "b"], "a")])
                .await
                .unwrap();
            coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            coordinator.set_option("test.choice", "b").await.unwrap();
            let before = coordinator.read().options();
            let after = coordinator
                .set_model(ModelSelection {
                    spec: Some(Arc::from(NEW)),
                    ..Default::default()
                })
                .await
                .unwrap();
            for id in [YOLO_OPTION_ID, "test.choice"] {
                assert_eq!(
                    before
                        .options
                        .iter()
                        .find(|option| option.definition.id.as_ref() == id),
                    after
                        .options
                        .iter()
                        .find(|option| option.definition.id.as_ref() == id)
                );
            }
            assert_eq!(
                catalog.plugin_definitions("test"),
                vec![plugin_option(1, &["a", "b"], "a")]
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn two_sessions_serialize_without_value_leakage() {
        smol::block_on(async {
            let first = register(MakimaId::generate());
            let second = register(MakimaId::generate());
            let before = first.read().options();
            let committed = first
                .set_option_if_version(WORKFLOW_OPTION_ID, ENABLED_VALUE, Some(before.version))
                .await
                .unwrap();
            assert!(committed.version > before.version);
            assert!(
                first
                    .set_option_if_version(WORKFLOW_OPTION_ID, DISABLED_VALUE, Some(before.version))
                    .await
                    .is_err()
            );
            assert_eq!(first.read().options(), committed);
            assert_eq!(
                second
                    .read()
                    .options()
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == WORKFLOW_OPTION_ID)
                    .unwrap()
                    .current_value
                    .as_ref(),
                DISABLED_VALUE
            );
            assert!(
                first
                    .read
                    .binding
                    .lock_unpoisoned()
                    .as_ref()
                    .unwrap()
                    .agent
                    .effective_config()
                    .unwrap()
                    .unwrap()
                    .workflow
            );
            assert!(
                !second
                    .read
                    .binding
                    .lock_unpoisoned()
                    .as_ref()
                    .unwrap()
                    .agent
                    .effective_config()
                    .unwrap()
                    .unwrap()
                    .workflow
            );
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test_case(FAST_TIER_TARGET, FAST_OPTION_ID, DISABLED_VALUE, false; "model_change_disables_fast")]
    #[test_case(NO_THINKING_TARGET, THINKING_OPTION_ID, THINKING_OFF, false; "model_change_disables_thinking")]
    #[test_case(FAST_TIER_TARGET, FAST_OPTION_ID, DISABLED_VALUE, true; "selection_disables_fast")]
    #[test_case(NO_THINKING_TARGET, THINKING_OPTION_ID, THINKING_OFF, true; "selection_disables_thinking")]
    fn model_change_clamps_unsupported_settings(
        target: &str,
        id: &str,
        expected: &str,
        structured: bool,
    ) {
        smol::block_on(async {
            let mut params = params(MakimaId::generate(), writer(false));
            params.definitions = builtin_option_definitions(
                CAPABLE_MODEL,
                [Arc::from(CAPABLE_MODEL)],
                false,
                true,
                false,
                ThinkingConfig::Effort(Effort::High),
            );
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let mut config = (*projection_config(false)).clone();
            config.settings.model = test_model(CAPABLE_MODEL);
            config.settings.fast = true;
            config.settings.thinking = ThinkingConfig::Effort(Effort::High);
            let agent = bind_config(&coordinator, config, Arc::default());
            let before = coordinator.read().options();
            let after = if structured {
                coordinator
                    .set_model(ModelSelection {
                        spec: Some(target.into()),
                        ..Default::default()
                    })
                    .await
            } else {
                coordinator.set_option(MODEL_OPTION_ID, target).await
            }
            .unwrap();
            assert_eq!(after.version, before.version + 1);
            let value = |id: &str| {
                after
                    .options
                    .iter()
                    .find(|option| option.definition.id.as_ref() == id)
                    .unwrap()
            };
            assert_eq!(value(MODEL_OPTION_ID).current_value.as_ref(), target);
            assert!(value(MODEL_OPTION_ID).definition.accepts(target));
            assert_eq!(value(id).current_value.as_ref(), expected);
            let settings = agent.effective_config().unwrap().unwrap();
            if id == FAST_OPTION_ID {
                assert!(!settings.fast);
            } else {
                assert_eq!(settings.thinking, ThinkingConfig::Off);
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(THINKING_OPTION_ID, "high", true; "listed_thinking_effort")]
    #[test_case(THINKING_OPTION_ID, "8192", true; "thinking_budget_outside_value_list")]
    #[test_case(THINKING_OPTION_ID, "deeply", false; "unknown_thinking_value")]
    #[test_case(MODEL_OPTION_ID, CURRENT_MODEL, true; "current_model_missing_from_discovery")]
    fn builtin_definitions_accept_values(id: &str, value: &str, accepted: bool) {
        let definitions = builtin_option_definitions(
            CURRENT_MODEL,
            [Arc::from(DISCOVERED_MODEL)],
            false,
            false,
            false,
            ThinkingConfig::Off,
        );
        let definition = definitions
            .iter()
            .find(|definition| definition.id.as_ref() == id)
            .unwrap();
        assert_eq!(definition.accepts(value), accepted);
    }

    #[test_case(FAST_OPTION_ID, ENABLED_VALUE; "unsupported_fast")]
    #[test_case(THINKING_OPTION_ID, "high"; "unsupported_thinking")]
    fn model_settings_reject_unsupported_values(id: &str, value: &str) {
        smol::block_on(async {
            let coordinator = register(MakimaId::generate());
            let before = coordinator.read().options();
            assert!(coordinator.set_option(id, value).await.is_err());
            assert_eq!(coordinator.read().options(), before);
            assert_eq!(coordinator.read().model().unwrap().as_ref(), OLD);
            drop(
                coordinator
                    .reserve_transaction(CheckpointPublication::default())
                    .unwrap(),
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test_case::test_case(true; "idle_guard")]
    #[test_case::test_case(false; "session_lease")]
    fn queued_model_changes_wait_for_the_holder_and_apply_in_order(idle_guard: bool) {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakimaId::generate(), writer(false)))
                    .unwrap();
            let installed = Arc::new(Mutex::new(Vec::new()));
            let agent = bind(&coordinator, Arc::clone(&installed));
            let guard = idle_guard.then(|| agent.actor().unwrap().prepare_idle().unwrap());
            let lease = (!idle_guard).then(|| {
                coordinator
                    .reserve_transaction(CheckpointPublication::default())
                    .unwrap()
            });
            let mut first = Box::pin(coordinator.set_model(ModelSelection {
                spec: Some(Arc::from(NEW)),
                ..Default::default()
            }));
            let mut second = Box::pin(coordinator.set_model(ModelSelection {
                spec: Some(Arc::from(OLD)),
                ..Default::default()
            }));
            assert!(futures_lite::future::poll_once(&mut first).await.is_none());
            assert!(futures_lite::future::poll_once(&mut second).await.is_none());
            drop((guard, lease));
            let (first, second) = futures_lite::future::zip(first, second).await;
            first.unwrap();
            second.unwrap();
            assert_eq!(installed.lock_unpoisoned().as_slice(), &[NEW, OLD]);
            assert_eq!(coordinator.read().model().unwrap().as_ref(), OLD);
            coordinator.close().await.unwrap();
        });
    }

    fn option_value(coordinator: &SessionCoordinatorHandle, id: &str) -> Arc<str> {
        coordinator
            .read()
            .options()
            .options
            .iter()
            .find(|option| option.definition.id.as_ref() == id)
            .map(|option| Arc::clone(&option.current_value))
            .unwrap()
    }

    #[test_case::test_case(WORKFLOW_OPTION_ID; "workflow")]
    #[test_case::test_case(YOLO_OPTION_ID; "yolo")]
    fn queued_toggles_apply_to_the_state_they_land_on(id: &str) {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakimaId::generate(), writer(false)))
                    .unwrap();
            let agent = bind(&coordinator, Arc::new(Mutex::new(Vec::new())));
            let initial = option_value(&coordinator, id);
            let guard = agent.actor().unwrap().prepare_idle().unwrap();
            let mut model = Box::pin(coordinator.set_model(ModelSelection {
                spec: Some(Arc::from(NEW)),
                ..Default::default()
            }));
            let mut first = Box::pin(coordinator.toggle_boolean_option(id));
            let mut second = Box::pin(coordinator.toggle_boolean_option(id));
            assert!(futures_lite::future::poll_once(&mut model).await.is_none());
            assert!(futures_lite::future::poll_once(&mut first).await.is_none());
            assert!(futures_lite::future::poll_once(&mut second).await.is_none());
            drop(guard);
            let (model, (first, second)) =
                futures_lite::future::zip(model, futures_lite::future::zip(first, second)).await;
            model.unwrap();
            let (first, _) = first.unwrap();
            let (second, _) = second.unwrap();
            assert_ne!(first, second);
            assert_eq!(option_value(&coordinator, id), initial);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn fenced_set_that_waits_applies_after_the_holder_commits() {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakimaId::generate(), writer(false)))
                    .unwrap();
            let agent = bind(&coordinator, Arc::new(Mutex::new(Vec::new())));
            let guard = agent.actor().unwrap().prepare_idle().unwrap();
            let version = coordinator.read().options().version;
            let mut model = Box::pin(coordinator.set_model(ModelSelection {
                spec: Some(Arc::from(NEW)),
                ..Default::default()
            }));
            let mut set = Box::pin(coordinator.set_option_if_version(
                WORKFLOW_OPTION_ID,
                ENABLED_VALUE,
                Some(version),
            ));
            assert!(futures_lite::future::poll_once(&mut model).await.is_none());
            assert!(futures_lite::future::poll_once(&mut set).await.is_none());
            drop(guard);
            let (model, set) = futures_lite::future::zip(model, set).await;
            model.unwrap();
            set.unwrap();
            assert_eq!(
                option_value(&coordinator, WORKFLOW_OPTION_ID).as_ref(),
                ENABLED_VALUE
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn queued_model_change_fails_when_the_target_closes() {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakimaId::generate(), writer(false)))
                    .unwrap();
            let installed = Arc::new(Mutex::new(Vec::new()));
            let agent = bind(&coordinator, Arc::clone(&installed));
            let _guard = agent.actor().unwrap().prepare_idle().unwrap();
            let mut change = Box::pin(coordinator.set_model(ModelSelection {
                spec: Some(Arc::from(NEW)),
                ..Default::default()
            }));
            assert!(futures_lite::future::poll_once(&mut change).await.is_none());
            agent.close_subtree().unwrap();
            assert!(change.await.is_err());
            assert!(installed.lock_unpoisoned().is_empty());
        });
    }

    #[test]
    fn discovered_models_wait_for_a_held_change_instead_of_being_dropped() {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakimaId::generate(), writer(false)))
                    .unwrap();
            let _agent = bind(&coordinator, Arc::new(Mutex::new(Vec::new())));
            let lease = coordinator
                .reserve_transaction(CheckpointPublication::default())
                .unwrap();
            let mut update =
                Box::pin(coordinator.update_model_values(vec![Arc::from(DISCOVERED_MODEL)]));
            assert!(futures_lite::future::poll_once(&mut update).await.is_none());
            drop(lease);
            let snapshot = update.await.unwrap();
            let definition = snapshot
                .options
                .iter()
                .find(|option| option.definition.id.as_ref() == MODEL_OPTION_ID)
                .unwrap();
            assert!(definition.definition.accepts(DISCOVERED_MODEL));
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn model_discovery_checkpoint_failure_preserves_definition() {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakimaId::generate(), writer(true)))
                    .unwrap();
            let before = coordinator.read().options();
            assert!(
                coordinator
                    .update_model_values(vec![Arc::from("ollama/discovered")])
                    .await
                    .is_err()
            );
            assert_eq!(coordinator.read().options(), before);
            coordinator.close().await.unwrap();
        });
    }
}
