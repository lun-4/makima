use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use maki_config::ModelPolicy;
use maki_providers::{Message, Model, ThinkingConfig};
use maki_storage::checkpoint::{
    CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication, CheckpointRequest,
    CheckpointVersion, CheckpointWriter,
};
use maki_storage::id::MakiId;
use maki_storage::sessions::HistorySnapshot;
use thiserror::Error;

use crate::SessionMailbox;
use crate::actor::{
    ActorError, ConfigChange, ConfigCommit, ConfigPatch, EffectiveAgentConfig, PreparedModel,
    PreparedTransaction,
};
use crate::cancel::{CancelToken, CancelTrigger, PublicationRegistration};
use crate::config_change::{ConfigChangeError, ConfigChangeTicket, prepare_patch, reserve_in_turn};
use crate::manager::AgentRef;
use crate::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, FreeValueDomain, MODEL_OPTION_ID,
    SessionOptionCategory, SessionOptionDefinition, SessionOptionError, SessionOptionOwner,
    SessionOptionValue, SessionOptions, SessionOptionsCandidate, SessionOptionsReservation,
    SessionOptionsSnapshot, SessionOptionsSubscription, THINKING_OPTION_ID, WORKFLOW_OPTION_ID,
    YOLO_OPTION_ID,
};

static DIRECTORY: LazyLock<Mutex<HashMap<MakiId, DirectoryEntry>>> =
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
    sessions: HashMap<MakiId, CatalogSession>,
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
        drop(lock(&self.trigger).take());
    }
}

/// Resolves a provider without changing host state or awaiting the actor runner.
pub type ModelPreparationFuture =
    Pin<Box<dyn Future<Output = Result<PreparedModel, Arc<str>>> + Send + 'static>>;
pub type ModelPreparer = Arc<dyn Fn(Model) -> ModelPreparationFuture + Send + Sync>;
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
    DuplicateSession(MakiId),
    #[error("session not live: {0}")]
    StaleSession(MakiId),
    #[error("session is busy: {0}")]
    SessionBusy(MakiId),
    #[error(transparent)]
    Option(#[from] SessionOptionError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("session actor is not bound: {0}")]
    ActorUnbound(MakiId),
}

#[derive(Clone)]
pub struct CommittedConfigProjection {
    pub runtime: Arc<()>,
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
}

#[derive(Clone)]
pub struct SessionCoordinatorHandle {
    session_id: MakiId,
    generation: u64,
    tx: flume::Sender<Operation>,
    read: SessionReadHandle,
    catalog: SessionOptionCatalog,
    publication_lifetime: Arc<PublicationLifetime>,
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
        let candidate = actor_options(&coordinator.read, &config)?;
        let options = SessionOptions::candidate_snapshot(&candidate);
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
            publication: self.publication.clone(),
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
                let history = history_changed.then_some(snapshot.history);
                let (version, history_revision) =
                    publish_read.next_version(options.version, history.is_some());
                let checkpoint = publish_read.checkpoint.checkpoint_guarded(
                    CheckpointRequest {
                        session_id,
                        version,
                        snapshot: Arc::new(SessionCheckpoint {
                            kind: SessionCheckpointKind::Config,
                            history_revision,
                            history,
                            options,
                            config: Some(CommittedConfigProjection {
                                runtime: snapshot.commit.identity,
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
                reservation.commit(candidate);
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
    session_id: MakiId,
    options: Arc<SessionOptions>,
    state: Arc<Mutex<CoordinatorState>>,
    binding: Arc<Mutex<Option<ActorBinding>>>,
    checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    model_policy: Arc<ModelPolicy>,
}

struct CoordinatorState {
    checkpoint_revision: u64,
    history_revision: u64,
}

pub struct SessionCoordinatorParams {
    pub session_id: MakiId,
    pub catalog: SessionOptionCatalog,
    pub definitions: Vec<SessionOptionDefinition>,
    pub persisted_options: BTreeMap<String, String>,
    pub model_policy: Arc<ModelPolicy>,
    pub checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    pub mailbox: SessionMailbox,
}

struct PreparedPluginOptions {
    session_id: MakiId,
    generation: u64,
    publication_lifetime: Arc<PublicationLifetime>,
    previous: SessionOptionsSnapshot,
    candidate: SessionOptionsCandidate,
    reservation: Arc<SessionOptionsReservation>,
}

enum PluginOptionDecision {
    Commit(flume::Sender<Result<(), SessionCoordinatorError>>),
    Complete(flume::Sender<Result<(), SessionCoordinatorError>>),
    Abort(flume::Sender<Result<(), SessionCoordinatorError>>),
}

enum Operation {
    SetOption {
        id: Arc<str>,
        value: Arc<str>,
        reservation: SessionOptionsReservation,
        reply: flume::Sender<Result<SessionOptionsSnapshot, SessionCoordinatorError>>,
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

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
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
        lock(&self.state)
            .definitions
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn plugin_definitions(&self, plugin: &str) -> Vec<SessionOptionDefinition> {
        lock(&self.state)
            .definitions
            .get(plugin)
            .cloned()
            .unwrap_or_default()
    }

    fn unregister(&self, session_id: MakiId, generation: u64) {
        let mut state = lock(&self.state);
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
    ) -> Result<Vec<(MakiId, SessionOptionsSnapshot)>, SessionCoordinatorError> {
        self.replace_plugin_options_with_validator(plugin, definitions, |_, _, _| async { Ok(()) })
            .await
    }

    pub async fn replace_plugin_options_with_validator<F, Fut>(
        &self,
        plugin: impl Into<Arc<str>>,
        definitions: Vec<SessionOptionDefinition>,
        validator: F,
    ) -> Result<Vec<(MakiId, SessionOptionsSnapshot)>, SessionCoordinatorError>
    where
        F: Fn(MakiId, Arc<str>, Arc<str>) -> Fut,
        Fut: Future<Output = Result<(), SessionCoordinatorError>>,
    {
        let plugin = plugin.into();
        let mut prepared: Vec<PluginOptionParticipant> = Vec::new();
        let (snapshots, previous_definitions) = loop {
            let mut sessions = lock(&self.state)
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
                let directory = lock(&DIRECTORY);
                let mut state = lock(&self.state);
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
                    let candidates = prepared
                        .iter()
                        .map(|(staged, _)| (staged.reservation.as_ref(), staged.candidate.clone()))
                        .collect();
                    Some(SessionOptions::commit_batch(candidates).map(|snapshots| {
                        let previous_definitions = if definitions.is_empty() {
                            state.definitions.remove(plugin.as_ref())
                        } else {
                            state
                                .definitions
                                .insert(Arc::clone(&plugin), definitions.clone())
                        };
                        state.revision += 1;
                        (snapshots, previous_definitions)
                    }))
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
            let result =
                finish_plugin_options(catalog, plugin, prepared, snapshots, previous_definitions)
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

    pub fn resolve(session_id: MakiId) -> Result<Self, SessionCoordinatorError> {
        lock(&DIRECTORY)
            .get(&session_id)
            .map(|entry| entry.handle.clone())
            .ok_or(SessionCoordinatorError::StaleSession(session_id))
    }

    pub fn read(&self) -> SessionReadHandle {
        self.read.clone()
    }
    pub fn session_id(&self) -> MakiId {
        self.session_id
    }
    pub fn runtime_epoch(&self) -> u64 {
        self.generation
    }

    pub fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> Result<PublicationRegistration, SessionCoordinatorError> {
        let directory = lock(&DIRECTORY);
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
        lock(&DIRECTORY)
            .get(&self.session_id)
            .filter(|entry| entry.handle.generation == self.generation)
            .map(|entry| entry.mailbox.clone())
            .ok_or(SessionCoordinatorError::StaleSession(self.session_id))
    }

    pub fn bind_actor(
        &self,
        agent: AgentRef,
        prepare_model: ModelPreparer,
        project_config: ConfigProjector,
    ) -> Result<(), ConfigChangeError> {
        self.ensure_live()?;
        agent.validate_live()?;
        let config = agent.actor()?.config_snapshot().ok_or_else(|| {
            ActorError::InvalidConfig("actor configuration is not initialized".into())
        })?;
        let reservation = self.reserve_options()?;
        let candidate = actor_options(&self.read, &config.config)?;
        let mut binding = lock(&self.read.binding);
        if binding.is_some() {
            return Err(SessionCoordinatorError::SessionBusy(self.session_id).into());
        }
        *binding = Some(ActorBinding {
            agent,
            prepare_model,
            project_config,
        });
        reservation.commit(candidate);
        Ok(())
    }

    fn actor_binding(&self) -> Result<ActorBinding, SessionCoordinatorError> {
        self.ensure_live()?;
        lock(&self.read.binding)
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
        let options = self.read.options();
        let (version, revision) = self.read.next_version(options.version, true);
        let revision = revision.expect("history checkpoint reserves a revision");
        let checkpoint = self.read.checkpoint.checkpoint(CheckpointRequest {
            session_id: self.session_id,
            version,
            snapshot: Arc::new(SessionCheckpoint {
                kind: SessionCheckpointKind::History,
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
        let binding = lock(&self.read.binding)
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
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        let (reply, response) = flume::bounded(1);
        self.tx
            .send(Operation::SetOption {
                id,
                value,
                reservation,
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
        self.ensure_live()?;
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
                return self.send_set_option(id, value, reservation).await;
            }
        };
        self.commit_patch(
            spec,
            patch,
            CheckpointPublication::default(),
            publication_claim(),
        )
        .await
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
                let snapshot = self.send_set_option(id, value.into(), reservation).await?;
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

    pub async fn set_model(
        &self,
        spec: Option<Arc<str>>,
        fast: Option<bool>,
        thinking: Option<ThinkingConfig>,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        self.set_model_with_claim(
            spec,
            fast,
            thinking,
            CheckpointPublication::default(),
            publication_claim(),
        )
        .await
    }

    pub async fn set_model_with_claim(
        &self,
        spec: Option<Arc<str>>,
        fast: Option<bool>,
        thinking: Option<ThinkingConfig>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<SessionOptionsSnapshot, ConfigChangeError> {
        let patch = ConfigPatch {
            fast,
            thinking,
            ..Default::default()
        };
        self.commit_patch(spec, patch, publication, claim).await
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
        let directory = lock(&DIRECTORY);
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
        unregister(self.session_id, self.generation);
        self.catalog.unregister(self.session_id, self.generation);
        let (reply, _) = flume::bounded(1);
        let _ = self.tx.send(Operation::Close { reply });
    }

    fn ensure_live(&self) -> Result<(), SessionCoordinatorError> {
        if lock(&DIRECTORY)
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
            let state = lock(&catalog.state);
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
            checkpoint,
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
        let mut directory = lock(&DIRECTORY);
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
        let mut catalog_state = lock(&handle.catalog.state);
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
    pub fn session_id(&self) -> MakiId {
        self.session_id
    }
    pub fn options(&self) -> SessionOptionsSnapshot {
        self.options.snapshot()
    }

    fn config(&self) -> Option<Arc<EffectiveAgentConfig>> {
        let binding = lock(&self.binding).clone()?;
        binding.agent.effective_config().ok().flatten()
    }

    /// See [`SessionOptions::project`].
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
        let mut state = lock(&self.state);
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

fn reject_operation(operation: Operation, session_id: MakiId, busy: bool) {
    let error = if busy {
        SessionCoordinatorError::SessionBusy(session_id)
    } else {
        SessionCoordinatorError::StaleSession(session_id)
    };
    match operation {
        Operation::SetOption { reply, .. } | Operation::UpdateModelValues { reply, .. } => {
            let _ = reply.send(Err(error));
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
            id,
            value,
            reservation,
            reply,
        } => {
            let result = async {
                let Some(candidate) = ctx.read.options.prepare_set(&id, &value)? else {
                    return Ok(ctx.read.options());
                };
                checkpoint_options(&ctx.read, SessionOptions::candidate_snapshot(&candidate))
                    .await?;
                Ok(reservation.commit(candidate))
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
            if let Err(error) =
                checkpoint_options(&ctx.read, SessionOptions::candidate_snapshot(&candidate)).await
            {
                drop(reservation);
                let _ = prepared.send(Err(error.into()));
                return ControlFlow::Continue(());
            }
            let staged = PreparedPluginOptions {
                session_id: ctx.session_id,
                generation: ctx.generation,
                publication_lifetime: Arc::clone(&ctx.publication_lifetime),
                previous: previous.clone(),
                candidate,
                reservation: Arc::clone(&reservation),
            };
            if prepared.send(Ok(staged)).is_err() {
                return restore_plugin_options(ctx, previous).await;
            }
            let mut reservation = Some(reservation);
            let mut committed = false;
            let mut closing: Vec<flume::Sender<()>> = Vec::new();
            loop {
                let event = futures_lite::future::or(
                    async { Either::Left(decision.recv_async().await) },
                    async { Either::Right(rx.recv_async().await) },
                )
                .await;
                match event {
                    Either::Left(Ok(PluginOptionDecision::Commit(reply))) => {
                        committed = true;
                        let _ = reply.send(Ok(()));
                    }
                    Either::Left(Ok(PluginOptionDecision::Complete(reply))) => {
                        drop(reservation.take());
                        let _ = reply.send(Ok(()));
                        break;
                    }
                    Either::Left(Ok(PluginOptionDecision::Abort(reply))) => {
                        if let Err(error) = checkpoint_options(&ctx.read, previous).await {
                            retire_failed_plugin_rollback(ctx, &error);
                            let _ = reply.send(Err(error.into()));
                            for reply in closing {
                                let _ = reply.send(());
                            }
                            return ControlFlow::Break(());
                        }
                        drop(reservation.take());
                        let _ = reply.send(Ok(()));
                        break;
                    }
                    Either::Right(Ok(Operation::Close { reply })) if committed => {
                        closing.push(reply)
                    }
                    Either::Right(Ok(Operation::Close { reply })) => {
                        if let Err(error) = checkpoint_options(&ctx.read, previous).await {
                            retire_failed_plugin_rollback(ctx, &error);
                        }
                        unregister(ctx.session_id, ctx.generation);
                        ctx.catalog.unregister(ctx.session_id, ctx.generation);
                        let _ = reply.send(());
                        return ControlFlow::Break(());
                    }
                    Either::Right(Ok(operation)) => {
                        reject_operation(operation, ctx.session_id, true)
                    }
                    Either::Left(Err(_)) | Either::Right(Err(_)) => {
                        return restore_plugin_options(ctx, previous).await;
                    }
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
    F: Fn(MakiId, Arc<str>, Arc<str>) -> Fut,
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
    session_id: MakiId,
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

/// Runs detached so a dropped catalog caller cannot abandon a decision.
async fn finish_plugin_options(
    catalog: SessionOptionCatalog,
    plugin: Arc<str>,
    prepared: Vec<PluginOptionParticipant>,
    snapshots: Vec<SessionOptionsSnapshot>,
    previous_definitions: Option<Vec<SessionOptionDefinition>>,
) -> Result<Vec<(MakiId, SessionOptionsSnapshot)>, SessionCoordinatorError> {
    let reservations = prepared
        .iter()
        .map(|(staged, _)| Arc::clone(&staged.reservation))
        .collect::<Vec<_>>();
    let restore = prepared
        .iter()
        .zip(&snapshots)
        .map(|((staged, _), snapshot)| (snapshot.version, staged.previous.clone()))
        .collect::<Vec<_>>();
    let mut committed = Vec::with_capacity(prepared.len());
    let mut commit_error = None;
    for ((staged, decision), snapshot) in prepared.iter().zip(snapshots) {
        let result =
            decide_plugin_options(decision, staged.session_id, PluginOptionDecision::Commit).await;
        if let Err(error) = result {
            commit_error.get_or_insert(error);
        }
        committed.push((staged.session_id, snapshot));
    }
    if let Some(error) = commit_error {
        {
            let _directory = lock(&DIRECTORY);
            let mut state = lock(&catalog.state);
            let restored = SessionOptions::restore_reserved_batch_if_versions(
                reservations
                    .iter()
                    .zip(restore)
                    .map(|(reservation, (version, previous))| {
                        (reservation.as_ref(), version, previous)
                    })
                    .collect(),
            );
            if restored {
                match previous_definitions {
                    Some(definitions) => state.definitions.insert(Arc::clone(&plugin), definitions),
                    None => state.definitions.remove(plugin.as_ref()),
                };
                state.revision += 1;
            } else {
                tracing::warn!(%plugin, "session options advanced before plugin rollback");
            }
        }
        drop(reservations);
        if let Err(rollback) = abort_plugin_options(prepared).await {
            return Err(SessionOptionError::CallbackFailed(Arc::from(format!(
                "{error}; checkpoint rollback: {rollback}"
            )))
            .into());
        }
        return Err(error);
    }
    drop(reservations);
    let mut release_error = None;
    for (staged, decision) in prepared {
        let session_id = staged.session_id;
        drop(staged);
        let result =
            decide_plugin_options(&decision, session_id, PluginOptionDecision::Complete).await;
        if let Err(error) = result {
            release_error.get_or_insert(error);
        }
    }
    release_error.map_or(Ok(committed), Err)
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

fn retire_failed_plugin_rollback(ctx: &SessionCoordinatorHandle, error: &CheckpointError) {
    tracing::error!(%error, session_id = %ctx.session_id, "session option checkpoint rollback failed; retiring session");
    unregister(ctx.session_id, ctx.generation);
    ctx.catalog.unregister(ctx.session_id, ctx.generation);
}

async fn restore_plugin_options(
    ctx: &SessionCoordinatorHandle,
    previous: SessionOptionsSnapshot,
) -> ControlFlow<()> {
    if let Err(error) = checkpoint_options(&ctx.read, previous).await {
        retire_failed_plugin_rollback(ctx, &error);
        return ControlFlow::Break(());
    }
    ControlFlow::Continue(())
}

async fn verify_ack(
    session_id: MakiId,
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

async fn checkpoint_options(
    read: &SessionReadHandle,
    options: SessionOptionsSnapshot,
) -> Result<(), CheckpointError> {
    let (version, _) = read.next_version(options.version, false);
    let checkpoint = read.checkpoint.checkpoint(CheckpointRequest {
        session_id: read.session_id,
        version,
        snapshot: Arc::new(SessionCheckpoint {
            kind: SessionCheckpointKind::Options,
            history_revision: None,
            history: None,
            options,
            config: None,
        }),
    });
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
    checkpoint_options(read, SessionOptions::candidate_snapshot(&candidate)).await?;
    Ok(reservation.commit(candidate))
}

fn unregister(session_id: MakiId, generation: u64) {
    let mut directory = lock(&DIRECTORY);
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
mod tests {
    use super::*;
    use crate::actor::{ActorBackend, AgentContextSnapshot, BackendResult, ControlWork, WorkKind};
    use crate::manager::{AgentLimits, AgentManagerHandle};
    use crate::tools::NullProvider;
    use crate::{AgentInput, AgentMode, History, RunSettings, TurnContext};
    use maki_providers::Effort;
    use maki_providers::provider::BoxFuture;
    use maki_storage::checkpoint::CheckpointAck;
    use test_case::test_case;

    const OLD: &str = "ollama/old";
    const NEW: &str = "ollama/new";
    const FAILED: &str = "failed";
    const CAPABLE_MODEL: &str = "anthropic/claude-opus-4-8";
    const FAST_TIER_TARGET: &str = "openai/gpt-5";
    const NO_THINKING_TARGET: &str = "ollama/llama3";
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

    fn projection_config(workflow: bool) -> Arc<EffectiveAgentConfig> {
        let mut config = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::new(NullProvider),
                model: Model::from_spec(OLD).unwrap(),
                thinking: ThinkingConfig::Off,
                fast: false,
                workflow,
            },
            AgentMode::Build,
        );
        config.context = AgentContextSnapshot::new(PathBuf::from("/project"));
        Arc::new(config)
    }

    struct TestBackend;
    impl ActorBackend for TestBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> BoxFuture<'a, BackendResult> {
            Box::pin(async { unreachable!() })
        }
        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> BoxFuture<'a, BackendResult> {
            Box::pin(async { unreachable!() })
        }
        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut History,
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
            let gate = lock(&self.gate).take();
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
                lock(&saved).push(request.snapshot);
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
        id: MakiId,
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

    fn bind_config(
        coordinator: &SessionCoordinatorHandle,
        config: EffectiveAgentConfig,
        installed: Arc<Mutex<Vec<String>>>,
    ) -> AgentRef {
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let agent = manager
            .create_root(Vec::new(), None, Box::new(TestBackend))
            .unwrap();
        agent.actor().unwrap().initialize_config(config).unwrap();
        coordinator
            .bind_actor(
                agent.clone(),
                Arc::new(|model| {
                    Box::pin(async move {
                        Ok(PreparedModel {
                            provider: Arc::new(NullProvider),
                            model,
                        })
                    })
                }),
                Arc::new(move |commit| lock(&installed).push(commit.config.model.spec())),
            )
            .unwrap();
        agent
    }

    fn register(id: MakiId) -> SessionCoordinatorHandle {
        register_with_catalog(id, writer(false), &SessionOptionCatalog::default())
    }
    fn register_with_catalog(
        id: MakiId,
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
            let first = register_with_catalog(MakiId::generate(), writer(false), &catalog);
            let second = register_with_catalog(MakiId::generate(), writer(false), &catalog);
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
    fn plugin_replacement_rejects_reserved_options_without_partial_commit() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let mut ids = [MakiId::generate(), MakiId::generate()];
            ids.sort_by_key(ToString::to_string);
            let first_saved = Arc::new(Mutex::new(Vec::new()));
            let second_saved = Arc::new(Mutex::new(Vec::new()));
            let rollback_gate = Arc::new(Mutex::new(None::<flume::Receiver<()>>));
            let first = register_with_catalog(
                ids[0],
                Arc::new({
                    let saved = Arc::clone(&first_saved);
                    let rollback_gate = Arc::clone(&rollback_gate);
                    move |request: CheckpointRequest<SessionCheckpoint>| {
                        let saved = Arc::clone(&saved);
                        let wait = lock(&rollback_gate).take();
                        Box::pin(async move {
                            if let Some(wait) = wait {
                                wait.recv_async().await.unwrap();
                            }
                            let ack = CheckpointAck::for_request(&request);
                            lock(&saved).push(request.snapshot);
                            Ok(ack)
                        }) as CheckpointFuture
                    }
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
            let predecessor = lock(&second.read.binding)
                .as_ref()
                .unwrap()
                .agent
                .actor()
                .unwrap()
                .reserve_prepared_operation(None)
                .unwrap();
            let mut mutation = Box::pin(second.set_model(None, Some(false), None));
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
                    futures_lite::future::poll_once(first.set_model(None, Some(false), None))
                        .await
                        .is_none()
                );
                if close {
                    let (rollback_tx, rollback_rx) = flume::bounded(1);
                    *lock(&rollback_gate) = Some(rollback_rx);
                    let closed = first.request_close().unwrap();
                    assert!(closed.is_empty());
                    assert_eq!(
                        lock(&DIRECTORY).get(&ids[0]).unwrap().handle.generation,
                        first.generation
                    );
                    release_tx.send(()).unwrap();
                    assert!(
                        futures_lite::future::poll_once(replace.as_mut())
                            .await
                            .is_none()
                    );
                    assert_eq!(catalog.plugin_definitions("test"), previous_definitions);
                    assert_eq!(first.read().options(), before_first);
                    assert_eq!(second.read().options(), before_second);
                    rollback_tx.send(()).unwrap();
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
                assert_eq!(lock(&first_saved).last().unwrap().options, before_first);
                drop(first.read.options.try_reserve(None).unwrap());
                second.barrier().await;
                assert_eq!(lock(&second_saved).last().unwrap().options, before_second);
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

    #[test_case::test_case(false; "complete")]
    #[test_case::test_case(true; "abort")]
    fn dropped_catalog_caller_settles_before_queued_checkpoint(abort: bool) {
        smol::block_on(async {
            let mut ids = [MakiId::generate(), MakiId::generate()];
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
            lock(&catalog.state).sessions.get_mut(&ids[1]).unwrap().tx = gate_tx;
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
            assert_eq!(lock(&saved).len(), 2);
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
            assert_eq!(lock(&saved).len(), 2);
            assert!(replacement.cancel().await.is_none());
            reply
                .send(if abort {
                    Err(SessionCoordinatorError::StaleSession(ids[1]))
                } else {
                    Ok(())
                })
                .unwrap();
            let final_decision = decision.recv_async().await.unwrap();
            let (ack, acknowledged) = flume::bounded(1);
            let reply = match final_decision {
                PluginOptionDecision::Complete(reply) if !abort => {
                    forward.send(PluginOptionDecision::Complete(ack)).unwrap();
                    reply
                }
                PluginOptionDecision::Abort(reply) if abort => {
                    forward.send(PluginOptionDecision::Abort(ack)).unwrap();
                    reply
                }
                _ => unreachable!(),
            };
            acknowledged.recv_async().await.unwrap().unwrap();
            drop(first.read.options.try_reserve(None).unwrap());
            drop(second.read.options.try_reserve(None).unwrap());
            reply.send(Ok(())).unwrap();
            if abort {
                assert!(catalog.plugin_definitions("test").is_empty());
                assert_eq!(first.read().options(), before);
                assert_eq!(second.read().options(), before);
                assert_eq!(lock(&saved).len(), 4);
            } else {
                assert_eq!(catalog.plugin_definitions("test").len(), 1);
                assert_eq!(lock(&saved).len(), 2);
            }
            first
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            assert_eq!(lock(&saved).len(), if abort { 5 } else { 3 });
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn prepared_session_reconciles_plugin_replacement_before_activation() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let id = MakiId::generate();
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
            let id = MakiId::generate();
            let current = register_with_catalog(id, writer(false), &catalog);
            let mut replacement_params = params(id, writer(false));
            replacement_params.catalog = catalog.clone();
            let replacement = PreparedSessionCoordinator::new(replacement_params).unwrap();
            let (started_tx, started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let gate = Arc::new(std::sync::atomic::AtomicBool::new(true));
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

    #[test]
    fn plugin_prepare_checkpoint_failure_exposes_no_mixed_generation() {
        smol::block_on(async {
            let mut ids = [MakiId::generate(), MakiId::generate()];
            ids.sort_by_key(ToString::to_string);
            let saved = Arc::new(Mutex::new(Vec::new()));
            let catalog = SessionOptionCatalog::default();
            let first = register_with_catalog(
                ids[0],
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
                &catalog,
            );
            let second = register_with_catalog(ids[1], writer(true), &catalog);
            let before_first = first.read().options();
            let before_second = second.read().options();
            assert!(matches!(
                catalog
                    .replace_plugin_options("test", vec![plugin_option(1, &["a"], "a")])
                    .await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(first.read().options(), before_first);
            assert_eq!(second.read().options(), before_second);
            drop(first.read.options.try_reserve(None).unwrap());
            drop(second.read.options.try_reserve(None).unwrap());
            {
                let saved = lock(&saved);
                assert_eq!(saved.len(), 2);
                assert!(
                    saved[0]
                        .options
                        .options
                        .iter()
                        .any(|option| option.definition.id.as_ref() == "test.choice")
                );
                assert!(
                    saved[1]
                        .options
                        .options
                        .iter()
                        .all(|option| option.definition.id.as_ref() != "test.choice")
                );
            }
            first
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            second.close().await.unwrap();
            first.close().await.unwrap();
        });
    }

    #[test]
    fn coordinator_directory_owns_mailbox_routing_lifetime() {
        smol::block_on(async {
            let id = MakiId::generate();
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
                MakiId::generate(),
                Arc::new(GuardedWriter {
                    fail,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            let agent = bind(&coordinator, Arc::clone(&installed));
            let before = coordinator.read().options();
            let result = coordinator
                .set_model(Some(Arc::from(NEW)), Some(false), Some(ThinkingConfig::Off))
                .await;
            if fail {
                assert!(result.is_err());
                assert_eq!(coordinator.read().options(), before);
                assert_eq!(agent.effective_config().unwrap().unwrap().model.spec(), OLD);
                assert!(lock(&installed).is_empty());
                assert!(lock(&saved).is_empty());
            } else {
                let options = result.unwrap();
                assert_eq!(coordinator.read().model().unwrap().as_ref(), NEW);
                assert_eq!(agent.effective_config().unwrap().unwrap().model.spec(), NEW);
                assert_eq!(lock(&installed).as_slice(), &[NEW]);
                let saved = lock(&saved);
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].options, options);
                assert!(saved[0].history.is_none());
                assert_eq!(saved[0].kind, SessionCheckpointKind::Config);
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn persisted_history_saves_a_history_checkpoint() {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakiId::generate(),
                Arc::new(GuardedWriter {
                    fail: false,
                    saved: Arc::clone(&saved),
                }),
            ))
            .unwrap();
            bind(&coordinator, Arc::default());
            let mut history = HistorySnapshot::new(vec![Message::user("terminal".into())]);
            history.compacted = true;
            coordinator.persist_history(history.clone()).await.unwrap();
            {
                let saved = lock(&saved);
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
                MakiId::generate(),
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
                let live = lock(&captured_authority);
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
                            Some(Arc::from(NEW)),
                            None,
                            None,
                            CheckpointPublication::default(),
                            claim,
                        )
                        .await
                }
            });
            reached_rx.recv_async().await.unwrap();
            *lock(&authority) = false;
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
                let saved = lock(&saved);
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].options, options);
                assert_eq!(saved[0].config.as_ref().unwrap().config.model.spec(), NEW);
                assert_eq!(lock(&installed).as_slice(), &[NEW]);
            } else {
                assert_eq!(coordinator.read().options(), before);
                assert!(lock(&saved).is_empty());
                assert!(lock(&installed).is_empty());
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
                MakiId::generate(),
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
                MakiId::generate(),
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
            lock(&coordinator.read.binding)
                .as_mut()
                .unwrap()
                .prepare_model = Arc::new(move |model| {
                let started_tx = started_tx.clone();
                let release_rx = release_rx.clone();
                Box::pin(async move {
                    started_tx.send_async(()).await.unwrap();
                    release_rx.recv_async().await.unwrap();
                    Ok(PreparedModel {
                        provider: Arc::new(NullProvider),
                        model,
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
                            Some(Arc::from(NEW)),
                            None,
                            None,
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
            assert!(lock(&saved).is_empty());
            assert!(lock(&installed).is_empty());
            coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            coordinator.close().await.unwrap();
        });
    }

    #[test_case(false; "directory_published")]
    #[test_case(true; "directory_checkpoint_failed")]
    fn directory_checkpoint_failure_preserves_runtime(fail: bool) {
        smol::block_on(async {
            let saved = Arc::new(Mutex::new(Vec::new()));
            let installed = Arc::new(Mutex::new(Vec::new()));
            let coordinator = SessionCoordinatorHandle::register(params(
                MakiId::generate(),
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
                assert!(lock(&installed).is_empty());
                assert!(lock(&saved).is_empty());
            } else {
                assert_eq!(result.unwrap(), canonical);
                assert_eq!(coordinator.read().cwd().unwrap(), canonical);
                assert_eq!(
                    agent.effective_config().unwrap().unwrap().context.cwd(),
                    canonical
                );
                assert_eq!(
                    lock(&saved)[0]
                        .config
                        .as_ref()
                        .unwrap()
                        .config
                        .context
                        .cwd(),
                    canonical
                );
                assert_eq!(lock(&installed).len(), 1);
                coordinator
                    .change_directory(PathBuf::from("."))
                    .await
                    .unwrap();
                assert_eq!(coordinator.read().cwd().unwrap(), canonical);
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn committed_projection_preserves_yolo_plugin_values_and_catalog() {
        smol::block_on(async {
            let catalog = SessionOptionCatalog::default();
            let coordinator = register_with_catalog(MakiId::generate(), writer(false), &catalog);
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
                .set_model(Some(Arc::from(NEW)), None, None)
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
            let first = register(MakiId::generate());
            let second = register(MakiId::generate());
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
                lock(&first.read.binding)
                    .as_ref()
                    .unwrap()
                    .agent
                    .effective_config()
                    .unwrap()
                    .unwrap()
                    .workflow
            );
            assert!(
                !lock(&second.read.binding)
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

    #[test_case(FAST_TIER_TARGET, FAST_OPTION_ID, DISABLED_VALUE; "model_change_disables_fast")]
    #[test_case(NO_THINKING_TARGET, THINKING_OPTION_ID, THINKING_OFF; "model_change_disables_thinking")]
    fn model_change_clamps_unsupported_settings(target: &str, id: &str, expected: &str) {
        smol::block_on(async {
            let mut params = params(MakiId::generate(), writer(false));
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
            config.settings.model = Model::from_spec(CAPABLE_MODEL).unwrap();
            config.settings.fast = true;
            config.settings.thinking = ThinkingConfig::Effort(Effort::High);
            let agent = bind_config(&coordinator, config, Arc::default());
            let before = coordinator.read().options();
            let after = coordinator
                .set_option(MODEL_OPTION_ID, target)
                .await
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
            let coordinator = register(MakiId::generate());
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
                SessionCoordinatorHandle::register(params(MakiId::generate(), writer(false)))
                    .unwrap();
            let installed = Arc::new(Mutex::new(Vec::new()));
            let agent = bind(&coordinator, Arc::clone(&installed));
            let guard = idle_guard.then(|| agent.actor().unwrap().prepare_idle().unwrap());
            let lease = (!idle_guard).then(|| {
                coordinator
                    .reserve_transaction(CheckpointPublication::default())
                    .unwrap()
            });
            let mut first = Box::pin(coordinator.set_model(Some(Arc::from(NEW)), None, None));
            let mut second = Box::pin(coordinator.set_model(Some(Arc::from(OLD)), None, None));
            assert!(futures_lite::future::poll_once(&mut first).await.is_none());
            assert!(futures_lite::future::poll_once(&mut second).await.is_none());
            drop((guard, lease));
            let (first, second) = futures_lite::future::zip(first, second).await;
            first.unwrap();
            second.unwrap();
            assert_eq!(lock(&installed).as_slice(), &[NEW, OLD]);
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
                SessionCoordinatorHandle::register(params(MakiId::generate(), writer(false)))
                    .unwrap();
            let agent = bind(&coordinator, Arc::new(Mutex::new(Vec::new())));
            let initial = option_value(&coordinator, id);
            let guard = agent.actor().unwrap().prepare_idle().unwrap();
            let mut model = Box::pin(coordinator.set_model(Some(Arc::from(NEW)), None, None));
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
                SessionCoordinatorHandle::register(params(MakiId::generate(), writer(false)))
                    .unwrap();
            let agent = bind(&coordinator, Arc::new(Mutex::new(Vec::new())));
            let guard = agent.actor().unwrap().prepare_idle().unwrap();
            let version = coordinator.read().options().version;
            let mut model = Box::pin(coordinator.set_model(Some(Arc::from(NEW)), None, None));
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
                SessionCoordinatorHandle::register(params(MakiId::generate(), writer(false)))
                    .unwrap();
            let installed = Arc::new(Mutex::new(Vec::new()));
            let agent = bind(&coordinator, Arc::clone(&installed));
            let _guard = agent.actor().unwrap().prepare_idle().unwrap();
            let mut change = Box::pin(coordinator.set_model(Some(Arc::from(NEW)), None, None));
            assert!(futures_lite::future::poll_once(&mut change).await.is_none());
            agent.close_subtree().unwrap();
            assert!(change.await.is_err());
            assert!(lock(&installed).is_empty());
        });
    }

    #[test]
    fn discovered_models_wait_for_a_held_change_instead_of_being_dropped() {
        smol::block_on(async {
            let coordinator =
                SessionCoordinatorHandle::register(params(MakiId::generate(), writer(false)))
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
                SessionCoordinatorHandle::register(params(MakiId::generate(), writer(true)))
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
