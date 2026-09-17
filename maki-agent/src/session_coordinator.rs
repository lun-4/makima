use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::ops::ControlFlow;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use maki_config::ModelPolicy;
use maki_providers::{Message, Model, ThinkingConfig};
use maki_storage::checkpoint::{
    CheckpointError, CheckpointRequest, CheckpointVersion, CheckpointWriter,
};
use maki_storage::id::MakiId;
use thiserror::Error;

use crate::SessionMailbox;
use crate::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, FreeValueDomain, MODEL_OPTION_ID,
    SessionOptionCategory, SessionOptionDefinition, SessionOptionError, SessionOptionOwner,
    SessionOptionValue, SessionOptions, SessionOptionsSnapshot, SessionOptionsSubscription,
    THINKING_OPTION_ID, WORKFLOW_OPTION_ID, YOLO_OPTION_ID,
};

#[derive(Default)]
struct LiveSessions {
    entries: HashMap<MakiId, DirectoryEntry>,
}

#[derive(Default)]
struct CatalogState {
    definitions: BTreeMap<Arc<str>, Vec<SessionOptionDefinition>>,
    sessions: HashMap<MakiId, CatalogSession>,
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

static DIRECTORY: LazyLock<Mutex<LiveSessions>> =
    LazyLock::new(|| Mutex::new(LiveSessions::default()));
static GENERATION: AtomicU64 = AtomicU64::new(1);
const CHECKPOINT_TIMEOUT_MESSAGE: &str = "checkpoint deadline exceeded";

#[derive(Clone)]
struct DirectoryEntry {
    generation: u64,
    tx: flume::Sender<Operation>,
    read: SessionReadHandle,
    catalog: SessionOptionCatalog,
    mailbox: SessionMailbox,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SessionCoordinatorError {
    #[error("session already live: {0}")]
    DuplicateSession(MakiId),
    #[error("session not live: {0}")]
    StaleSession(MakiId),
    #[error("session is busy executing a turn: {0}")]
    SessionBusy(MakiId),
    #[error(transparent)]
    Option(#[from] SessionOptionError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("model runtime transition failed: {0}")]
    ModelAdoption(Arc<str>),
    #[error(
        "model checkpoint failed and runtime rollback also failed; runtime state was adopted: {0}"
    )]
    ModelRollback(Arc<str>),
    #[error("directory runtime transition failed: {0}")]
    DirectoryAdoption(Arc<str>),
    #[error("directory checkpoint failed and runtime rollback also failed: {0}")]
    DirectoryRollback(Arc<str>),
}

#[derive(Debug, Clone)]
pub struct SessionCheckpoint {
    /// `None` when the operation being checkpointed does not change history,
    /// which is every operation except a replacement. A lease holder's turn
    /// has not committed yet, so the coordinator's copy is the pre-turn one:
    /// writing it would rewind the stored session to before the running turn.
    pub history: Option<Arc<Vec<Message>>>,
    pub model: Arc<str>,
    pub cwd: PathBuf,
    pub options: SessionOptionsSnapshot,
}

#[derive(Clone)]
pub struct SessionCoordinatorHandle {
    session_id: MakiId,
    generation: u64,
    tx: flume::Sender<Operation>,
    read: SessionReadHandle,
    catalog: SessionOptionCatalog,
}

pub struct PreparedSessionCoordinator {
    handle: SessionCoordinatorHandle,
    catalog: SessionOptionCatalog,
    model_policy: Arc<ModelPolicy>,
    model_adopter: Arc<dyn ModelAdopter>,
    directory_adopter: Arc<dyn DirectoryAdopter>,
    checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    mailbox: SessionMailbox,
    rx: flume::Receiver<Operation>,
}

#[derive(Clone)]
pub struct SessionReadHandle {
    session_id: MakiId,
    options: Arc<SessionOptions>,
    state: Arc<Mutex<CoordinatorState>>,
}

pub struct SessionLease {
    session_id: MakiId,
    released: Option<flume::Sender<LeaseRelease>>,
    read: SessionReadHandle,
}

#[derive(Clone)]
pub struct SessionLeaseCommitter {
    session_id: MakiId,
    released: flume::Sender<LeaseRelease>,
}

pub struct SessionHistoryCommit {
    session_id: MakiId,
    response: flume::Receiver<Result<(), SessionCoordinatorError>>,
}

enum LeaseRelease {
    CommitHistory {
        history: Arc<Vec<Message>>,
        timeout: Option<Duration>,
        reply: flume::Sender<Result<(), SessionCoordinatorError>>,
    },
    Release,
}

pub type ModelAdoptionFuture = Pin<Box<dyn Future<Output = Result<(), Arc<str>>> + Send + 'static>>;
pub type DirectoryAdoptionFuture =
    Pin<Box<dyn Future<Output = Result<PathBuf, Arc<str>>> + Send + 'static>>;

pub trait DirectoryAdopter: Send + Sync {
    fn adopt(&self, path: PathBuf) -> DirectoryAdoptionFuture;
}

impl<F> DirectoryAdopter for F
where
    F: Fn(PathBuf) -> DirectoryAdoptionFuture + Send + Sync,
{
    fn adopt(&self, path: PathBuf) -> DirectoryAdoptionFuture {
        self(path)
    }
}

pub trait ModelAdopter: Send + Sync {
    fn adopt(&self, model: Model) -> ModelAdoptionFuture;
}

impl<F> ModelAdopter for F
where
    F: Fn(Model) -> ModelAdoptionFuture + Send + Sync,
{
    fn adopt(&self, model: Model) -> ModelAdoptionFuture {
        self(model)
    }
}

struct CoordinatorState {
    history: Arc<Vec<Message>>,
    model: Arc<str>,
    cwd: PathBuf,
    checkpoint_revision: u64,
    history_revision: u64,
}

pub struct SessionCoordinatorParams {
    pub session_id: MakiId,
    pub catalog: SessionOptionCatalog,
    pub definitions: Vec<SessionOptionDefinition>,
    pub persisted_options: BTreeMap<String, String>,
    pub history: Vec<Message>,
    pub model: Arc<str>,
    pub cwd: PathBuf,
    pub model_policy: Arc<ModelPolicy>,
    pub model_adopter: Arc<dyn ModelAdopter>,
    pub directory_adopter: Arc<dyn DirectoryAdopter>,
    pub checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    pub mailbox: SessionMailbox,
}

struct PreparedPluginOptions {
    session_id: MakiId,
    options: Arc<SessionOptions>,
    previous: SessionOptionsSnapshot,
    candidate: crate::session_options::SessionOptionsCandidate,
}

enum PluginOptionDecision {
    Commit(flume::Sender<Result<(), SessionCoordinatorError>>),
    Abort(flume::Sender<Result<(), SessionCoordinatorError>>),
}

enum Operation {
    AcquireLease {
        reply: flume::Sender<Result<SessionLease, SessionCoordinatorError>>,
    },
    SetOption {
        id: Arc<str>,
        value: Arc<str>,
        version: Option<u64>,
        reply: flume::Sender<Result<SessionOptionsSnapshot, SessionCoordinatorError>>,
    },
    ToggleBooleanOption {
        id: Arc<str>,
        reply: flume::Sender<Result<(bool, SessionOptionsSnapshot), SessionCoordinatorError>>,
    },
    SetModel {
        spec: Option<Arc<str>>,
        fast: Option<bool>,
        thinking: Option<ThinkingConfig>,
        reply: flume::Sender<Result<SessionOptionsSnapshot, SessionCoordinatorError>>,
    },
    ReplaceHistory {
        history: Arc<Vec<Message>>,
        reply: flume::Sender<Result<(), SessionCoordinatorError>>,
    },
    ChangeDirectory {
        path: PathBuf,
        reply: flume::Sender<Result<PathBuf, SessionCoordinatorError>>,
    },
    UpdateModelValues {
        specs: Vec<Arc<str>>,
        reply: flume::Sender<Result<SessionOptionsSnapshot, SessionCoordinatorError>>,
    },
    PreparePluginOptions {
        plugin: Arc<str>,
        definitions: Vec<SessionOptionDefinition>,
        prepared: flume::Sender<Result<PreparedPluginOptions, SessionCoordinatorError>>,
        decision: flume::Receiver<PluginOptionDecision>,
    },
    Close {
        reply: flume::Sender<()>,
    },
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionOptionCatalog {
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
        let mut sessions = lock(&self.state)
            .sessions
            .iter()
            .map(|(session_id, session)| (*session_id, session.tx.clone()))
            .collect::<Vec<_>>();
        sessions.sort_by_key(|(session_id, _)| session_id.to_string());

        let mut prepared = Vec::with_capacity(sessions.len());
        for (session_id, tx) in sessions {
            let (prepared_tx, prepared_rx) = flume::bounded(1);
            let (decision_tx, decision_rx) = flume::bounded(1);
            if tx
                .send_async(Operation::PreparePluginOptions {
                    plugin: Arc::clone(&plugin),
                    definitions: definitions.clone(),
                    prepared: prepared_tx,
                    decision: decision_rx,
                })
                .await
                .is_err()
            {
                abort_plugin_options(prepared).await?;
                return Err(SessionCoordinatorError::StaleSession(session_id));
            }
            let result = match prepared_rx.recv_async().await {
                Ok(result) => result,
                Err(_) => {
                    abort_plugin_options(prepared).await?;
                    return Err(SessionCoordinatorError::StaleSession(session_id));
                }
            };
            match result {
                Ok(candidate) => prepared.push((candidate, decision_tx)),
                Err(error) => {
                    abort_plugin_options(prepared).await?;
                    return Err(error);
                }
            }
        }

        for (staged, _) in &prepared {
            let previous = &staged.previous;
            let candidate = SessionOptions::candidate_snapshot(&staged.candidate);
            for option in candidate.options.iter() {
                let is_plugin_option = matches!(
                    &option.definition.owner,
                    SessionOptionOwner::Plugin { plugin: owner, .. } if owner == &plugin
                );
                if !is_plugin_option {
                    continue;
                }
                let changed = previous
                    .options
                    .iter()
                    .find(|old| old.definition.id == option.definition.id)
                    .is_none_or(|old| old.current_value != option.current_value);
                if changed
                    && let Err(error) = validator(
                        staged.session_id,
                        Arc::clone(&option.definition.id),
                        Arc::clone(&option.current_value),
                    )
                    .await
                {
                    if let Err(abort_error) = abort_plugin_options(prepared).await {
                        tracing::warn!(%abort_error, "failed to abort session option validation");
                    }
                    return Err(error);
                }
            }
        }

        let candidates = prepared
            .iter()
            .map(|(prepared, _)| (Arc::clone(&prepared.options), prepared.candidate.clone()))
            .collect();
        let (snapshots, previous_definitions) = {
            let mut state = lock(&self.state);
            let snapshots = match SessionOptions::commit_batch(candidates) {
                Ok(snapshots) => snapshots,
                Err(error) => {
                    drop(state);
                    abort_plugin_options(prepared).await?;
                    return Err(error.into());
                }
            };
            let previous_definitions = if definitions.is_empty() {
                state.definitions.remove(plugin.as_ref())
            } else {
                state.definitions.insert(Arc::clone(&plugin), definitions)
            };
            (snapshots, previous_definitions)
        };
        let restore = prepared
            .iter()
            .zip(&snapshots)
            .map(|((prepared, _), snapshot)| {
                (
                    Arc::clone(&prepared.options),
                    snapshot.version,
                    prepared.previous.clone(),
                )
            })
            .collect();
        let mut committed = Vec::with_capacity(prepared.len());
        let mut commit_error = None;
        for ((prepared, decision), snapshot) in prepared.into_iter().zip(snapshots) {
            let (reply, response) = flume::bounded(1);
            let result =
                if decision.send(PluginOptionDecision::Commit(reply)).is_err() {
                    Err(SessionCoordinatorError::StaleSession(prepared.session_id))
                } else {
                    response.recv_async().await.unwrap_or(Err(
                        SessionCoordinatorError::StaleSession(prepared.session_id),
                    ))
                };
            if let Err(error) = result {
                commit_error.get_or_insert(error);
            }
            committed.push((prepared.session_id, snapshot));
        }
        if let Some(error) = commit_error {
            if SessionOptions::restore_batch_if_versions(restore) {
                let mut state = lock(&self.state);
                if let Some(definitions) = previous_definitions {
                    state.definitions.insert(Arc::clone(&plugin), definitions);
                } else {
                    state.definitions.remove(plugin.as_ref());
                }
            } else {
                tracing::warn!(%plugin, "session options advanced before plugin rollback");
            }
            return Err(error);
        }
        Ok(committed)
    }
}

impl SessionCoordinatorHandle {
    pub fn prepare(
        params: SessionCoordinatorParams,
    ) -> Result<PreparedSessionCoordinator, SessionCoordinatorError> {
        PreparedSessionCoordinator::new(params)
    }

    pub fn register(params: SessionCoordinatorParams) -> Result<Self, SessionCoordinatorError> {
        Self::prepare(params)?.activate()
    }

    pub fn resolve(session_id: MakiId) -> Result<Self, SessionCoordinatorError> {
        let directory = lock(&DIRECTORY);
        let entry = directory
            .entries
            .get(&session_id)
            .ok_or(SessionCoordinatorError::StaleSession(session_id))?;
        Ok(Self {
            session_id,
            generation: entry.generation,
            tx: entry.tx.clone(),
            read: entry.read.clone(),
            catalog: entry.catalog.clone(),
        })
    }

    pub fn read(&self) -> SessionReadHandle {
        self.read.clone()
    }

    pub fn mailbox(&self) -> Result<SessionMailbox, SessionCoordinatorError> {
        let directory = lock(&DIRECTORY);
        directory
            .entries
            .get(&self.session_id)
            .filter(|entry| entry.generation == self.generation)
            .map(|entry| entry.mailbox.clone())
            .ok_or(SessionCoordinatorError::StaleSession(self.session_id))
    }

    pub async fn acquire_lease(&self) -> Result<SessionLease, SessionCoordinatorError> {
        self.ensure_live()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::AcquireLease { reply })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }

    pub async fn set_option(
        &self,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
    ) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
        self.set_option_if_version(id, value, None).await
    }

    pub async fn set_option_if_version(
        &self,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
        version: Option<u64>,
    ) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
        self.ensure_live()?;
        if let Some(version) = version
            && self.read.options().version != version
        {
            return Err(SessionOptionError::StaleHandle(Arc::from(
                "session option snapshot changed during validation",
            ))
            .into());
        }
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::SetOption {
                id: id.into(),
                value: value.into(),
                version,
                reply,
            })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }

    pub fn toggle_boolean_option(
        &self,
        id: impl Into<Arc<str>>,
    ) -> impl Future<Output = Result<(bool, SessionOptionsSnapshot), SessionCoordinatorError>> + Send
    {
        let coordinator = self.clone();
        let id = id.into();
        async move {
            coordinator.ensure_live()?;
            let (reply, response) = flume::bounded(1);
            coordinator
                .tx
                .send_async(Operation::ToggleBooleanOption { id, reply })
                .await
                .map_err(|_| SessionCoordinatorError::StaleSession(coordinator.session_id))?;
            response
                .recv_async()
                .await
                .map_err(|_| SessionCoordinatorError::StaleSession(coordinator.session_id))?
        }
    }

    pub async fn set_model(
        &self,
        spec: Option<Arc<str>>,
        fast: Option<bool>,
        thinking: Option<ThinkingConfig>,
    ) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
        self.ensure_live()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::SetModel {
                spec,
                fast,
                thinking,
                reply,
            })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }

    pub async fn replace_history(
        &self,
        history: Vec<Message>,
    ) -> Result<(), SessionCoordinatorError> {
        self.ensure_live()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::ReplaceHistory {
                history: Arc::new(history),
                reply,
            })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }

    pub async fn change_directory(
        &self,
        path: PathBuf,
    ) -> Result<PathBuf, SessionCoordinatorError> {
        self.ensure_live()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::ChangeDirectory { path, reply })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }

    pub async fn update_model_values(
        &self,
        specs: Vec<Arc<str>>,
    ) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
        self.ensure_live()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::UpdateModelValues { specs, reply })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }

    pub async fn close(&self) -> Result<(), SessionCoordinatorError> {
        self.ensure_live()?;
        let (reply, response) = flume::bounded(1);
        self.tx
            .send_async(Operation::Close { reply })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        response
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
        let directory = lock(&DIRECTORY);
        if directory
            .entries
            .get(&self.session_id)
            .is_some_and(|entry| entry.generation == self.generation)
        {
            Ok(())
        } else {
            Err(SessionCoordinatorError::StaleSession(self.session_id))
        }
    }
}

impl PreparedSessionCoordinator {
    fn new(params: SessionCoordinatorParams) -> Result<Self, SessionCoordinatorError> {
        let SessionCoordinatorParams {
            session_id,
            catalog,
            mut definitions,
            persisted_options,
            history,
            model,
            cwd,
            model_policy,
            model_adopter,
            directory_adopter,
            checkpoint,
            mailbox,
        } = params;
        definitions.extend(
            lock(&catalog.state)
                .definitions
                .values()
                .flat_map(|definitions| definitions.iter().cloned()),
        );
        let options = SessionOptions::new(definitions, &persisted_options)?;
        let read = SessionReadHandle {
            session_id,
            options,
            state: Arc::new(Mutex::new(CoordinatorState {
                history: Arc::new(history),
                model,
                cwd,
                checkpoint_revision: 0,
                history_revision: 0,
            })),
        };
        let (tx, rx) = flume::unbounded();
        let generation = GENERATION.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            handle: SessionCoordinatorHandle {
                session_id,
                generation,
                tx,
                read,
                catalog: catalog.clone(),
            },
            catalog,
            model_policy,
            model_adopter,
            directory_adopter,
            checkpoint,
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
            catalog,
            model_policy,
            model_adopter,
            directory_adopter,
            checkpoint,
            mailbox,
            rx,
        } = self;
        let mut directory = lock(&DIRECTORY);
        let retired = match (
            directory.entries.get(&handle.session_id),
            replaced_generation,
        ) {
            (None, None) => None,
            (Some(entry), Some(generation)) if entry.generation == generation => {
                Some(entry.tx.clone())
            }
            (Some(_), None) => {
                return Err(SessionCoordinatorError::DuplicateSession(handle.session_id));
            }
            _ => return Err(SessionCoordinatorError::StaleSession(handle.session_id)),
        };
        let mut catalog_state = lock(&catalog.state);
        directory.entries.insert(
            handle.session_id,
            DirectoryEntry {
                generation: handle.generation,
                tx: handle.tx.clone(),
                read: handle.read.clone(),
                catalog: catalog.clone(),
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
        smol::spawn(run(
            handle.session_id,
            handle.generation,
            catalog,
            handle.read.clone(),
            model_policy,
            model_adopter,
            directory_adopter,
            checkpoint,
            rx,
        ))
        .detach();
        if let Some(retired) = retired {
            let (reply, _) = flume::bounded(1);
            let _ = retired.send(Operation::Close { reply });
        }
        Ok(handle)
    }
}

impl SessionLease {
    pub fn read(&self) -> SessionReadHandle {
        self.read.clone()
    }

    pub fn committer(&self) -> Option<SessionLeaseCommitter> {
        self.released
            .as_ref()
            .map(|released| SessionLeaseCommitter {
                session_id: self.session_id,
                released: released.clone(),
            })
    }

    pub async fn set_option(
        &self,
        session_id: MakiId,
        id: impl Into<Arc<str>>,
        value: impl Into<Arc<str>>,
    ) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
        if session_id == self.session_id {
            return Err(SessionCoordinatorError::SessionBusy(session_id));
        }
        SessionCoordinatorHandle::resolve(session_id)?
            .set_option(id, value)
            .await
    }
}

impl SessionLeaseCommitter {
    pub fn session_id(&self) -> MakiId {
        self.session_id
    }

    pub async fn commit_history(
        &self,
        history: Vec<Message>,
    ) -> Result<(), SessionCoordinatorError> {
        self.begin_history_commit(history, None).await?.wait().await
    }

    pub async fn begin_history_commit(
        &self,
        history: Vec<Message>,
        timeout: Option<Duration>,
    ) -> Result<SessionHistoryCommit, SessionCoordinatorError> {
        let (reply, response) = flume::bounded(1);
        self.released
            .send_async(LeaseRelease::CommitHistory {
                history: Arc::new(history),
                timeout,
                reply,
            })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?;
        Ok(SessionHistoryCommit {
            session_id: self.session_id,
            response,
        })
    }
}

impl SessionHistoryCommit {
    pub async fn wait(self) -> Result<(), SessionCoordinatorError> {
        self.response
            .recv_async()
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(self.session_id))?
    }
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        let _ = self
            .released
            .take()
            .and_then(|released| released.send(LeaseRelease::Release).ok());
    }
}

impl SessionReadHandle {
    pub fn session_id(&self) -> MakiId {
        self.session_id
    }

    pub fn options(&self) -> SessionOptionsSnapshot {
        self.options.snapshot()
    }

    pub fn history(&self) -> Arc<Vec<Message>> {
        Arc::clone(&lock(&self.state).history)
    }

    pub fn model(&self) -> Arc<str> {
        Arc::clone(&lock(&self.state).model)
    }

    pub fn cwd(&self) -> PathBuf {
        lock(&self.state).cwd.clone()
    }

    pub fn subscribe(&self) -> SessionOptionsSubscription {
        self.options.subscribe()
    }
}

/// Everything `run` needs to serve one operation. Bundled so operation
/// handling can live outside the loop, which lets a held lease keep serving
/// the operations it does not guard.
struct CoordinatorCtx {
    session_id: MakiId,
    generation: u64,
    catalog: SessionOptionCatalog,
    read: SessionReadHandle,
    model_policy: Arc<ModelPolicy>,
    model_adopter: Arc<dyn ModelAdopter>,
    directory_adopter: Arc<dyn DirectoryAdopter>,
    checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
}

/// A lease exists so a running turn's history is not replaced underneath it.
/// That is all it guards: an operation that leaves history alone runs while
/// the lease is held, so toggling YOLO reaches the turn that is prompting and
/// a model change is not stalled behind a long run. Anything that changes what
/// the turn is operating on waits.
fn defers_behind_lease(operation: &Operation) -> bool {
    matches!(
        operation,
        Operation::ReplaceHistory { .. }
            | Operation::ChangeDirectory { .. }
            | Operation::PreparePluginOptions { .. }
            | Operation::Close { .. }
            | Operation::AcquireLease { .. }
    )
}

fn reject_operation(operation: Operation, session_id: MakiId) {
    let error = || SessionCoordinatorError::StaleSession(session_id);
    match operation {
        Operation::AcquireLease { reply } => {
            let _ = reply.send(Err(error()));
        }
        Operation::SetOption { reply, .. } | Operation::UpdateModelValues { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        Operation::ToggleBooleanOption { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        Operation::SetModel { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        Operation::ReplaceHistory { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        Operation::ChangeDirectory { reply, .. } => {
            let _ = reply.send(Err(error()));
        }
        Operation::PreparePluginOptions { prepared, .. } => {
            let _ = prepared.send(Err(error()));
        }
        Operation::Close { reply } => {
            let _ = reply.send(());
        }
    }
}

async fn handle_operation(ctx: &CoordinatorCtx, operation: Operation) -> ControlFlow<()> {
    match operation {
        Operation::AcquireLease { .. } => {
            // Held leases are driven by `run`; a nested one cannot happen.
            unreachable!("lease acquisition is handled by the coordinator loop")
        }
        Operation::SetOption {
            id,
            value,
            version,
            reply,
        } => {
            let result = if version.is_some_and(|version| ctx.read.options().version != version) {
                Err(SessionOptionError::StaleHandle(Arc::from(
                    "session option snapshot changed during validation",
                ))
                .into())
            } else if id.as_ref() == MODEL_OPTION_ID {
                #[allow(clippy::explicit_auto_deref)]
                set_model(
                    &ctx.read,
                    &*ctx.model_policy,
                    &*ctx.model_adopter,
                    &*ctx.checkpoint,
                    Some(Arc::clone(&value)),
                    None,
                    None,
                )
                .await
            } else {
                set_option(&ctx.read, &*ctx.checkpoint, &id, &value).await
            };
            let _ = reply.send(result);
        }
        Operation::ToggleBooleanOption { id, reply } => {
            let result = toggle_boolean_option(&ctx.read, &*ctx.checkpoint, &id).await;
            let _ = reply.send(result);
        }
        Operation::SetModel {
            spec,
            fast,
            thinking,
            reply,
        } => {
            let result = set_model(
                &ctx.read,
                &ctx.model_policy,
                &*ctx.model_adopter,
                &*ctx.checkpoint,
                spec,
                fast,
                thinking,
            )
            .await;
            let _ = reply.send(result);
        }
        Operation::ReplaceHistory { history, reply } => {
            let result = replace_history(&ctx.read, &*ctx.checkpoint, history).await;
            let _ = reply.send(result);
        }
        Operation::ChangeDirectory { path, reply } => {
            let result =
                change_directory(&ctx.read, &*ctx.directory_adopter, &*ctx.checkpoint, path).await;
            let _ = reply.send(result);
        }
        Operation::UpdateModelValues { specs, reply } => {
            let result = update_model_values(&ctx.read, &*ctx.checkpoint, specs).await;
            let _ = reply.send(result);
        }
        Operation::PreparePluginOptions {
            plugin,
            definitions,
            prepared,
            decision,
        } => {
            let previous = ctx.read.options.snapshot();
            let result = ctx
                .read
                .options
                .prepare_replace_plugin(&plugin, definitions)
                .map(|candidate| {
                    candidate.unwrap_or_else(|| ctx.read.options.unchanged_candidate())
                })
                .map_err(Into::into);
            let candidate = match result {
                Ok(candidate) => candidate,
                Err(error) => {
                    let _ = prepared.send(Err(error));
                    return ControlFlow::Continue(());
                }
            };
            let candidate_snapshot = SessionOptions::candidate_snapshot(&candidate);
            let (model, cwd) = {
                let state = lock(&ctx.read.state);
                (Arc::clone(&state.model), state.cwd.clone())
            };
            if let Err(error) = checkpoint_state(
                &ctx.read,
                &*ctx.checkpoint,
                None,
                model,
                cwd,
                candidate_snapshot,
            )
            .await
            {
                let _ = prepared.send(Err(error.into()));
                return ControlFlow::Continue(());
            }
            let staged = PreparedPluginOptions {
                session_id: ctx.session_id,
                options: Arc::clone(&ctx.read.options),
                previous: previous.clone(),
                candidate,
            };
            if prepared.send(Ok(staged)).is_err() {
                let _ = checkpoint_options(&ctx.read, &*ctx.checkpoint, previous).await;
                return ControlFlow::Continue(());
            }
            match decision.recv_async().await {
                Ok(PluginOptionDecision::Commit(reply)) => {
                    let _ = reply.send(Ok(()));
                }
                Ok(PluginOptionDecision::Abort(reply)) => {
                    let result = checkpoint_options(&ctx.read, &*ctx.checkpoint, previous)
                        .await
                        .map_err(Into::into);
                    let _ = reply.send(result);
                }
                Err(_) => {
                    let _ = checkpoint_options(&ctx.read, &*ctx.checkpoint, previous).await;
                }
            }
        }
        Operation::Close { reply } => {
            lock(&ctx.read.state).history_revision += 1;
            unregister(ctx.session_id, ctx.generation);
            ctx.catalog.unregister(ctx.session_id, ctx.generation);
            let _ = reply.send(());
            return ControlFlow::Break(());
        }
    }
    ControlFlow::Continue(())
}

#[allow(clippy::too_many_arguments)]
async fn run(
    session_id: MakiId,
    generation: u64,
    catalog: SessionOptionCatalog,
    read: SessionReadHandle,
    model_policy: Arc<ModelPolicy>,
    model_adopter: Arc<dyn ModelAdopter>,
    directory_adopter: Arc<dyn DirectoryAdopter>,
    checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
    rx: flume::Receiver<Operation>,
) {
    let ctx = CoordinatorCtx {
        session_id,
        generation,
        catalog,
        read,
        model_policy,
        model_adopter,
        directory_adopter,
        checkpoint,
    };
    let mut deferred: VecDeque<Operation> = VecDeque::new();
    loop {
        let operation = match deferred.pop_front() {
            Some(operation) => operation,
            None => match rx.recv_async().await {
                Ok(operation) => operation,
                Err(_) => break,
            },
        };
        match operation {
            Operation::AcquireLease { reply } => {
                let (released, wait) = flume::bounded(1);
                let lease = SessionLease {
                    session_id: ctx.session_id,
                    released: Some(released),
                    read: ctx.read.clone(),
                };
                if reply.send(Ok(lease)).is_err() {
                    continue;
                }
                hold_lease(&ctx, &rx, &wait, &mut deferred).await;
            }
            other => {
                if handle_operation(&ctx, other).await.is_break() {
                    for operation in deferred.drain(..).chain(rx.try_iter()) {
                        reject_operation(operation, ctx.session_id);
                    }
                    return;
                }
            }
        }
    }
    unregister(ctx.session_id, ctx.generation);
    ctx.catalog.unregister(ctx.session_id, ctx.generation);
}

/// Serves operations for as long as the lease is held. History replacement and
/// anything else the lease guards is queued for after the release, so the turn
/// still owns what it is working on; everything else runs now.
async fn hold_lease(
    ctx: &CoordinatorCtx,
    rx: &flume::Receiver<Operation>,
    wait: &flume::Receiver<LeaseRelease>,
    deferred: &mut VecDeque<Operation>,
) {
    let mut closing = false;
    loop {
        let release = std::pin::pin!(wait.recv_async());
        let incoming = std::pin::pin!(rx.recv_async());
        match futures_lite::future::or(async { Either::Left(release.await) }, async {
            Either::Right(incoming.await)
        })
        .await
        {
            Either::Left(Ok(LeaseRelease::CommitHistory {
                history,
                timeout,
                reply,
            })) => {
                let (completed_tx, completed_rx) = flume::bounded(1);
                let read = ctx.read.clone();
                let checkpoint = Arc::clone(&ctx.checkpoint);
                smol::spawn(async move {
                    let result = replace_history(&read, &*checkpoint, history).await;
                    let _ = completed_tx.send(result);
                })
                .detach();

                finish_history_commit(ctx, rx, wait, deferred, completed_rx, timeout, reply).await;
                return;
            }
            // Released without a commit, or the holder dropped.
            Either::Left(_) => return,
            Either::Right(Ok(operation)) => {
                if closing {
                    reject_operation(operation, ctx.session_id);
                } else if matches!(operation, Operation::Close { .. }) {
                    lock(&ctx.read.state).history_revision += 1;
                    deferred.push_back(operation);
                    closing = true;
                } else if defers_behind_lease(&operation) {
                    deferred.push_back(operation);
                } else {
                    let _ = handle_operation(ctx, operation).await;
                }
            }
            // The last handle is gone; nothing more will arrive.
            Either::Right(Err(_)) => return,
        }
    }
}

async fn finish_history_commit(
    ctx: &CoordinatorCtx,
    rx: &flume::Receiver<Operation>,
    wait: &flume::Receiver<LeaseRelease>,
    deferred: &mut VecDeque<Operation>,
    completed: flume::Receiver<Result<(), SessionCoordinatorError>>,
    timeout: Option<Duration>,
    reply: flume::Sender<Result<(), SessionCoordinatorError>>,
) {
    let (timeout_tx, timeout_rx) = flume::bounded(1);
    if let Some(timeout) = timeout {
        smol::spawn(async move {
            smol::Timer::after(timeout).await;
            let _ = timeout_tx.send(());
        })
        .detach();
    }
    let mut checkpoint_completed = false;
    let mut lease_released = false;
    let mut closing = false;
    let mut replied = false;
    loop {
        let completion_pending = checkpoint_completed;
        let completion = std::pin::pin!(async {
            if completion_pending {
                futures_lite::future::pending().await
            } else {
                completed.recv_async().await
            }
        });
        let release_pending = lease_released;
        let release = std::pin::pin!(async {
            if release_pending {
                futures_lite::future::pending().await
            } else {
                wait.recv_async().await
            }
        });
        let incoming = std::pin::pin!(rx.recv_async());
        let deadline_pending = replied;
        let deadline = std::pin::pin!(async {
            if deadline_pending {
                futures_lite::future::pending().await
            } else {
                timeout_rx.recv_async().await
            }
        });
        let event =
            futures_lite::future::or(async { CommitEvent::Completed(completion.await) }, async {
                futures_lite::future::or(async { CommitEvent::Released(release.await) }, async {
                    futures_lite::future::or(
                        async { CommitEvent::TimedOut(deadline.await) },
                        async { CommitEvent::Incoming(incoming.await) },
                    )
                    .await
                })
                .await
            })
            .await;
        match event {
            CommitEvent::Completed(result) => {
                checkpoint_completed = true;
                if !replied {
                    let result = result
                        .unwrap_or(Err(SessionCoordinatorError::StaleSession(ctx.session_id)));
                    let _ = reply.send(result);
                    replied = true;
                }
            }
            CommitEvent::TimedOut(Ok(())) if !replied => {
                let _ = reply.send(Err(CheckpointError::Save {
                    session_id: ctx.session_id,
                    message: Arc::from(CHECKPOINT_TIMEOUT_MESSAGE),
                }
                .into()));
                replied = true;
            }
            CommitEvent::Released(Ok(LeaseRelease::Release)) | CommitEvent::Released(Err(_)) => {
                lease_released = true;
            }
            CommitEvent::Released(Ok(LeaseRelease::CommitHistory { reply, .. })) => {
                let _ = reply.send(Err(SessionCoordinatorError::SessionBusy(ctx.session_id)));
            }
            CommitEvent::Incoming(Ok(operation)) => {
                if closing {
                    reject_operation(operation, ctx.session_id);
                } else if matches!(operation, Operation::Close { .. }) {
                    lock(&ctx.read.state).history_revision += 1;
                    deferred.push_back(operation);
                    closing = true;
                } else if defers_behind_lease(&operation) {
                    deferred.push_back(operation);
                } else {
                    let _ = handle_operation(ctx, operation).await;
                }
            }
            CommitEvent::Incoming(Err(_)) => closing = true,
            CommitEvent::TimedOut(_) => {}
        }
        if lease_released && (checkpoint_completed || closing) {
            return;
        }
    }
}

enum CommitEvent {
    Completed(Result<Result<(), SessionCoordinatorError>, flume::RecvError>),
    Released(Result<LeaseRelease, flume::RecvError>),
    Incoming(Result<Operation, flume::RecvError>),
    TimedOut(Result<(), flume::RecvError>),
}

enum Either<L, R> {
    Left(L),
    Right(R),
}

async fn abort_plugin_options(
    prepared: Vec<(PreparedPluginOptions, flume::Sender<PluginOptionDecision>)>,
) -> Result<(), SessionCoordinatorError> {
    let mut responses = Vec::with_capacity(prepared.len());
    let mut first_error = None;
    for (staged, decision) in prepared {
        let (reply, response) = flume::bounded(1);
        if decision.send(PluginOptionDecision::Abort(reply)).is_err() {
            first_error.get_or_insert(SessionCoordinatorError::StaleSession(staged.session_id));
        } else {
            responses.push((staged.session_id, response));
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

async fn checkpoint_options(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    options: SessionOptionsSnapshot,
) -> Result<(), CheckpointError> {
    let (model, cwd) = {
        let state = lock(&read.state);
        (Arc::clone(&state.model), state.cwd.clone())
    };
    checkpoint_state(read, checkpoint, None, model, cwd, options).await
}

async fn checkpoint_state(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    history: Option<Arc<Vec<Message>>>,
    model: Arc<str>,
    cwd: PathBuf,
    options: SessionOptionsSnapshot,
) -> Result<(), CheckpointError> {
    let version = {
        let mut state = lock(&read.state);
        state.checkpoint_revision += 1;
        CheckpointVersion {
            revision: state.checkpoint_revision,
            epoch: options.version,
        }
    };
    let ack = checkpoint
        .checkpoint(CheckpointRequest {
            session_id: read.session_id,
            version,
            snapshot: Arc::new(SessionCheckpoint {
                history,
                model,
                cwd,
                options,
            }),
        })
        .await?;
    if ack.session_id == read.session_id && ack.version == version {
        Ok(())
    } else {
        Err(CheckpointError::Save {
            session_id: read.session_id,
            message: Arc::from("checkpoint acknowledgement did not match request"),
        })
    }
}

async fn replace_history(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    history: Arc<Vec<Message>>,
) -> Result<(), SessionCoordinatorError> {
    let (model, cwd, history_revision) = {
        let mut state = lock(&read.state);
        state.history_revision += 1;
        (
            Arc::clone(&state.model),
            state.cwd.clone(),
            state.history_revision,
        )
    };
    checkpoint_state(
        read,
        checkpoint,
        Some(Arc::clone(&history)),
        model,
        cwd,
        read.options.snapshot(),
    )
    .await?;
    let mut state = lock(&read.state);
    if state.history_revision == history_revision {
        state.history = history;
    }
    Ok(())
}

async fn update_model_values(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    specs: Vec<Arc<str>>,
) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
    let snapshot = read.options.snapshot();
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
    let options = SessionOptions::candidate_snapshot(&candidate);
    let (model, cwd) = {
        let state = lock(&read.state);
        (Arc::clone(&state.model), state.cwd.clone())
    };
    checkpoint_state(read, checkpoint, None, model, cwd, options).await?;
    read.options.commit(candidate).map_err(Into::into)
}

async fn change_directory(
    read: &SessionReadHandle,
    directory_adopter: &dyn DirectoryAdopter,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    path: PathBuf,
) -> Result<PathBuf, SessionCoordinatorError> {
    let (model, previous) = {
        let state = lock(&read.state);
        (Arc::clone(&state.model), state.cwd.clone())
    };
    let canonical = directory_adopter
        .adopt(path)
        .await
        .map_err(SessionCoordinatorError::DirectoryAdoption)?;
    if canonical == previous {
        return Ok(canonical);
    }
    let result = checkpoint_state(
        read,
        checkpoint,
        None,
        model,
        canonical.clone(),
        read.options.snapshot(),
    )
    .await;
    if let Err(error) = result {
        if let Err(rollback_error) = directory_adopter.adopt(previous).await {
            return Err(SessionCoordinatorError::DirectoryRollback(Arc::from(
                format!("{error}; rollback: {rollback_error}"),
            )));
        }
        return Err(error.into());
    }
    lock(&read.state).cwd = canonical.clone();
    Ok(canonical)
}

async fn set_model(
    read: &SessionReadHandle,
    model_policy: &ModelPolicy,
    model_adopter: &dyn ModelAdopter,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    spec: Option<Arc<str>>,
    fast: Option<bool>,
    thinking: Option<ThinkingConfig>,
) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
    let previous_spec = read.model();
    let target_spec = spec.as_deref().unwrap_or(&previous_spec);
    if !model_policy.allows(target_spec) {
        return Err(SessionOptionError::PolicyRejected(Arc::from(target_spec)).into());
    }
    let model = Model::from_spec(target_spec).map_err(|_| SessionOptionError::InvalidValue {
        id: Arc::from(MODEL_OPTION_ID),
        value: Arc::from(target_spec),
    })?;
    if fast == Some(true) && !model.supports_fast() {
        return Err(SessionOptionError::FastUnsupported.into());
    }
    if thinking.is_some_and(ThinkingConfig::is_enabled) && !model.supports_thinking() {
        return Err(SessionOptionError::ThinkingUnsupported.into());
    }
    let fast_value: Arc<str> = match fast {
        Some(true) => Arc::from(ENABLED_VALUE),
        Some(false) => Arc::from(DISABLED_VALUE),
        None if model.supports_fast() => {
            current_option_value(read, FAST_OPTION_ID).unwrap_or_else(|| Arc::from(DISABLED_VALUE))
        }
        None => Arc::from(DISABLED_VALUE),
    };
    let thinking_value: Arc<str> = match thinking {
        Some(thinking) => Arc::from(thinking.to_string()),
        None if model.supports_thinking() => current_option_value(read, THINKING_OPTION_ID)
            .unwrap_or_else(|| Arc::from(ThinkingConfig::Off.to_string())),
        None => Arc::from(ThinkingConfig::Off.to_string()),
    };
    let values = [
        (MODEL_OPTION_ID, target_spec),
        (FAST_OPTION_ID, fast_value.as_ref()),
        (THINKING_OPTION_ID, thinking_value.as_ref()),
    ];
    let Some(candidate) = read.options.prepare_set_values(&values)? else {
        return Ok(read.options.snapshot());
    };
    let options = SessionOptions::candidate_snapshot(&candidate);
    let model_changed = target_spec != previous_spec.as_ref();
    if model_changed {
        model_adopter
            .adopt(model)
            .await
            .map_err(SessionCoordinatorError::ModelAdoption)?;
    }
    let cwd = lock(&read.state).cwd.clone();
    let checkpoint_result =
        checkpoint_state(read, checkpoint, None, Arc::from(target_spec), cwd, options).await;
    if let Err(error) = checkpoint_result {
        if model_changed {
            let previous_model = Model::from_spec(&previous_spec).map_err(|rollback_error| {
                SessionCoordinatorError::ModelRollback(Arc::from(rollback_error.to_string()))
            })?;
            if let Err(rollback_error) = model_adopter.adopt(previous_model).await {
                lock(&read.state).model = Arc::from(target_spec);
                read.options.set_values_atomically(&values)?;
                return Err(SessionCoordinatorError::ModelRollback(Arc::from(format!(
                    "{error}; rollback: {rollback_error}; live runtime remains on {target_spec}"
                ))));
            }
        }
        return Err(error.into());
    }
    lock(&read.state).model = Arc::from(target_spec);
    match read.options.commit(candidate) {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => {
            lock(&read.state).model = previous_spec;
            Err(error.into())
        }
    }
}

fn current_option_value(read: &SessionReadHandle, id: &str) -> Option<Arc<str>> {
    read.options
        .snapshot()
        .options
        .iter()
        .find(|option| option.definition.id.as_ref() == id)
        .map(|option| Arc::clone(&option.current_value))
}

async fn toggle_boolean_option(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    id: &str,
) -> Result<(bool, SessionOptionsSnapshot), SessionCoordinatorError> {
    if !matches!(id, YOLO_OPTION_ID | FAST_OPTION_ID | WORKFLOW_OPTION_ID) {
        return Err(SessionOptionError::InvalidValue {
            id: Arc::from(id),
            value: Arc::from("toggle"),
        }
        .into());
    }
    let enabled =
        current_option_value(read, id).is_some_and(|value| value.as_ref() == DISABLED_VALUE);
    let value = if enabled {
        ENABLED_VALUE
    } else {
        DISABLED_VALUE
    };
    set_option(read, checkpoint, id, value)
        .await
        .map(|snapshot| (enabled, snapshot))
}

async fn set_option(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    id: &str,
    value: &str,
) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
    if id == FAST_OPTION_ID && value == ENABLED_VALUE {
        let state = lock(&read.state);
        if !Model::from_spec(&state.model).is_ok_and(|model| model.supports_fast()) {
            return Err(SessionOptionError::FastUnsupported.into());
        }
    }
    if id == THINKING_OPTION_ID
        && value
            .parse::<ThinkingConfig>()
            .is_ok_and(ThinkingConfig::is_enabled)
    {
        let state = lock(&read.state);
        if !Model::from_spec(&state.model).is_ok_and(|model| model.supports_thinking()) {
            return Err(SessionOptionError::ThinkingUnsupported.into());
        }
    }
    let Some(candidate) = read.options.prepare_set(id, value)? else {
        return Ok(read.options.snapshot());
    };
    let options = SessionOptions::candidate_snapshot(&candidate);
    let (model, cwd) = {
        let state = lock(&read.state);
        (Arc::clone(&state.model), state.cwd.clone())
    };
    checkpoint_state(read, checkpoint, None, model, cwd, options).await?;
    read.options.commit(candidate).map_err(Into::into)
}

fn unregister(session_id: MakiId, generation: u64) {
    let mut directory = lock(&DIRECTORY);
    if directory
        .entries
        .get(&session_id)
        .is_some_and(|entry| entry.generation == generation)
    {
        directory.entries.remove(&session_id);
    }
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
        toggle_definition(YOLO_OPTION_ID, "YOLO", "Skip permission prompts", yolo),
        toggle_definition(
            FAST_OPTION_ID,
            "Fast",
            "Use the model's fast service tier",
            fast,
        ),
        toggle_definition(
            WORKFLOW_OPTION_ID,
            "Workflow",
            "Enable workflow tools for future turns",
            workflow,
        ),
        thinking_definition(thinking),
    ]
}

/// Thinking is the one builtin whose domain is open: the named efforts are
/// what a client can offer, and a token budget is anything a user types.
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
    use maki_storage::checkpoint::{CheckpointAck, CheckpointFuture};

    use super::*;

    fn writer(fail: bool) -> Arc<dyn CheckpointWriter<SessionCheckpoint>> {
        Arc::new(move |request: CheckpointRequest<SessionCheckpoint>| {
            Box::pin(async move {
                if fail {
                    Err(CheckpointError::Save {
                        session_id: request.session_id,
                        message: Arc::from("failed"),
                    })
                } else {
                    Ok(CheckpointAck {
                        session_id: request.session_id,
                        version: request.version,
                    })
                }
            }) as CheckpointFuture
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
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            persisted_options: BTreeMap::new(),
            history: Vec::new(),
            model: Arc::from("test/model"),
            cwd: PathBuf::from("/project"),
            model_policy: Arc::new(ModelPolicy::default()),
            model_adopter: Arc::new(|_: Model| Box::pin(async { Ok(()) }) as ModelAdoptionFuture),
            directory_adopter: Arc::new(|path: PathBuf| {
                Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
            }),
            checkpoint,
            mailbox: SessionMailbox::new(id),
        }
    }

    fn register(id: MakiId) -> SessionCoordinatorHandle {
        SessionCoordinatorHandle::register(params(id, writer(false))).unwrap()
    }

    fn register_with_catalog(
        id: MakiId,
        checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>>,
        catalog: &SessionOptionCatalog,
    ) -> SessionCoordinatorHandle {
        let mut params = params(id, checkpoint);
        params.catalog = catalog.clone();
        SessionCoordinatorHandle::register(params).unwrap()
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
            assert!(
                first
                    .read()
                    .options()
                    .options
                    .iter()
                    .all(|option| option.definition.id.as_ref() != "test.choice")
            );
            assert!(
                second
                    .read()
                    .options()
                    .options
                    .iter()
                    .all(|option| option.definition.id.as_ref() != "test.choice")
            );
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn plugin_prepare_checkpoint_failure_exposes_no_mixed_generation() {
        smol::block_on(async {
            let mut ids = [MakiId::generate(), MakiId::generate()];
            ids.sort_by_key(ToString::to_string);
            let saved = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
            let first_writer: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let saved = Arc::clone(&saved);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    lock(&saved).push(
                        request
                            .snapshot
                            .options
                            .options
                            .iter()
                            .map(|option| option.definition.id.to_string())
                            .collect(),
                    );
                    Box::pin(async move {
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                }
            });
            let catalog = SessionOptionCatalog::default();
            let first = register_with_catalog(ids[0], first_writer, &catalog);
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
            {
                let checkpoints = lock(&saved);
                assert_eq!(checkpoints.len(), 2);
                assert!(checkpoints[0].iter().any(|id| id == "test.choice"));
                assert!(checkpoints[1].iter().all(|id| id != "test.choice"));
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
    fn duplicate_registration_is_rejected() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            assert!(matches!(
                SessionCoordinatorHandle::register(params(id, writer(false))),
                Err(SessionCoordinatorError::DuplicateSession(duplicate)) if duplicate == id
            ));
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn close_rejects_stale_routing() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let stale = coordinator.clone();
            coordinator.close().await.unwrap();

            assert!(matches!(
                SessionCoordinatorHandle::resolve(id),
                Err(SessionCoordinatorError::StaleSession(stale_id)) if stale_id == id
            ));
            assert!(matches!(
                stale.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await,
                Err(SessionCoordinatorError::StaleSession(stale_id)) if stale_id == id
            ));
        });
    }

    #[test]
    fn coordinator_directory_owns_mailbox_routing_lifetime() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let mailbox = coordinator.mailbox().unwrap();
            drop(coordinator.mailbox().unwrap());

            SessionMailbox::notify(id, "live".into(), false).unwrap();
            assert_eq!(mailbox.drain()[0].user_text(), Some("live"));

            coordinator.close().await.unwrap();
            assert!(SessionMailbox::notify(id, "late".into(), false).is_err());
            assert!(mailbox.drain().is_empty());
        });
    }

    #[test]
    fn same_session_mutation_during_lease_returns_session_busy_without_deadlock() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();

            assert!(matches!(
                lease.set_option(id, YOLO_OPTION_ID, ENABLED_VALUE).await,
                Err(SessionCoordinatorError::SessionBusy(busy)) if busy == id
            ));
            assert_eq!(
                lease.read().options().options[1].current_value.as_ref(),
                DISABLED_VALUE
            );

            drop(lease);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    /// The lease guards history, not options. Toggling YOLO has to reach the
    /// turn that is prompting, and a model change should not wait out a long
    /// run, so an option change runs while the lease is held.
    fn an_option_change_runs_while_a_lease_is_held() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();

            let snapshot = coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .expect("an option change must not wait for the lease");
            assert_eq!(snapshot.options[1].current_value.as_ref(), ENABLED_VALUE);

            drop(lease);
            coordinator.close().await.unwrap();
        });
    }

    /// An option change while a turn holds the lease must not carry history.
    /// The coordinator's copy is the last committed one, so the running turn's
    /// messages are not in it; writing it would rewind the stored session to
    /// before the turn. Only a replacement owns history.
    #[test]
    fn an_option_change_does_not_checkpoint_history() {
        smol::block_on(async {
            let carried: Arc<Mutex<Vec<Option<usize>>>> = Arc::default();
            let seen = Arc::clone(&carried);
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> =
                Arc::new(move |request: CheckpointRequest<SessionCheckpoint>| {
                    lock(&seen).push(request.snapshot.history.as_ref().map(|h| h.len()));
                    Box::pin(async move {
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                });
            let id = MakiId::generate();
            let coordinator = SessionCoordinatorHandle::register(params(id, checkpoint)).unwrap();
            let lease = coordinator.acquire_lease().await.unwrap();

            coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            coordinator
                .update_model_values(vec![Arc::from("openai/gpt-5")])
                .await
                .unwrap();

            assert_eq!(
                *lock(&carried),
                vec![None, None],
                "an option change must leave the stored history alone"
            );

            // A replacement is the one operation that owns history.
            drop(lease);
            coordinator
                .replace_history(vec![Message::user("committed".into())])
                .await
                .unwrap();
            assert_eq!(lock(&carried).last().copied().flatten(), Some(1));
            coordinator.close().await.unwrap();
        });
    }

    /// The other half: replacing history under a running turn is exactly what
    /// the lease exists to prevent.
    #[test]
    fn replacing_history_waits_for_the_lease() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();
            let (done_tx, done_rx) = flume::bounded(1);
            let queued = coordinator.clone();
            smol::spawn(async move {
                let result = queued
                    .replace_history(vec![Message::user("late".into())])
                    .await;
                let _ = done_tx.send(result);
            })
            .detach();

            assert!(
                done_rx.try_recv().is_err(),
                "history replacement must wait for the turn to release the lease"
            );
            drop(lease);
            done_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(coordinator.read().history().len(), 1);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn close_rejects_later_deferred_operations_explicitly() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();
            let (close_reply, close_response) = flume::bounded(1);
            coordinator
                .tx
                .send_async(Operation::Close { reply: close_reply })
                .await
                .unwrap();
            let (replace_reply, replace_response) = flume::bounded(1);
            coordinator
                .tx
                .send_async(Operation::ReplaceHistory {
                    history: Arc::new(vec![Message::user("late".into())]),
                    reply: replace_reply,
                })
                .await
                .unwrap();

            drop(lease);

            close_response.recv_async().await.unwrap();
            assert!(matches!(
                replace_response.recv_async().await,
                Ok(Err(SessionCoordinatorError::StaleSession(stale))) if stale == id
            ));
        });
    }

    #[test]
    fn lease_history_commit_precedes_queued_mutation_and_release() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();
            let committer = lease.committer().unwrap();
            let history = vec![Message::user("committed".into())];
            let (done_tx, done_rx) = flume::bounded(1);
            let queued = coordinator.clone();
            smol::spawn(async move {
                let result = queued
                    .replace_history(vec![Message::user("queued".into())])
                    .await;
                let _ = done_tx.send(result);
            })
            .detach();

            committer.commit_history(history.clone()).await.unwrap();
            assert_eq!(
                serde_json::to_value(coordinator.read().history().as_ref()).unwrap(),
                serde_json::to_value(&history).unwrap()
            );
            assert!(done_rx.try_recv().is_err());

            drop(lease);
            done_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(
                coordinator.read().history().len(),
                1,
                "the queued replacement lands after the commit, not before it"
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn timed_out_history_commit_finishes_checkpoint_and_publishes_history() {
        smol::block_on(async {
            let id = MakiId::generate();
            let (checkpoint_started_tx, checkpoint_started_rx) = flume::bounded(1);
            let (release_tx, release_rx) = flume::bounded(1);
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> =
                Arc::new(move |request: CheckpointRequest<SessionCheckpoint>| {
                    let checkpoint_started_tx = checkpoint_started_tx.clone();
                    let release_rx = release_rx.clone();
                    Box::pin(async move {
                        if request.snapshot.history.is_some() {
                            checkpoint_started_tx.send_async(()).await.unwrap();
                            release_rx.recv_async().await.unwrap();
                        }
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                });
            let coordinator = SessionCoordinatorHandle::register(params(id, checkpoint)).unwrap();
            let lease = coordinator.acquire_lease().await.unwrap();
            let committer = lease.committer().unwrap();
            let history = vec![Message::user("committed".into())];
            let commit = committer
                .begin_history_commit(history.clone(), Some(Duration::ZERO))
                .await
                .unwrap();

            checkpoint_started_rx.recv_async().await.unwrap();
            assert!(matches!(
                commit.wait().await,
                Err(SessionCoordinatorError::Checkpoint(CheckpointError::Save {
                    message,
                    ..
                })) if message.as_ref() == CHECKPOINT_TIMEOUT_MESSAGE
            ));
            assert!(coordinator.read().history().is_empty());
            coordinator
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .expect("non-history operations must run while timed-out commit is observed");

            let (lease_tx, lease_rx) = flume::bounded(1);
            coordinator
                .tx
                .send_async(Operation::AcquireLease { reply: lease_tx })
                .await
                .unwrap();
            smol::future::yield_now().await;
            assert!(lease_rx.try_recv().is_err());

            release_tx.send_async(()).await.unwrap();
            assert!(lease_rx.try_recv().is_err());

            drop(lease);
            let next = lease_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(
                serde_json::to_value(coordinator.read().history().as_ref()).unwrap(),
                serde_json::to_value(&history).unwrap()
            );
            drop(next);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn close_can_finish_after_timed_out_commit_owner_releases() {
        smol::block_on(async {
            let id = MakiId::generate();
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new(|_| {
                Box::pin(async { futures_lite::future::pending().await }) as CheckpointFuture
            });
            let coordinator = SessionCoordinatorHandle::register(params(id, checkpoint)).unwrap();
            let lease = coordinator.acquire_lease().await.unwrap();
            let commit = lease
                .committer()
                .unwrap()
                .begin_history_commit(Vec::new(), Some(Duration::ZERO))
                .await
                .unwrap();

            assert!(matches!(
                commit.wait().await,
                Err(SessionCoordinatorError::Checkpoint(CheckpointError::Save {
                    message,
                    ..
                })) if message.as_ref() == CHECKPOINT_TIMEOUT_MESSAGE
            ));
            let (closed_tx, closed_rx) = flume::bounded(1);
            let closing = coordinator.clone();
            smol::spawn(async move {
                let _ = closed_tx.send(closing.close().await);
            })
            .detach();
            drop(lease);

            let result =
                futures_lite::future::race(async { Some(closed_rx.recv_async().await) }, async {
                    smol::Timer::after(Duration::from_millis(100)).await;
                    None
                })
                .await;
            assert!(matches!(result, Some(Ok(Ok(())))));
        });
    }

    #[test]
    fn cross_session_mutation_during_lease_routes_normally() {
        smol::block_on(async {
            let first_id = MakiId::generate();
            let second_id = MakiId::generate();
            let first = register(first_id);
            let second = register(second_id);
            let lease = first.acquire_lease().await.unwrap();

            let snapshot = lease
                .set_option(second_id, YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();
            assert_eq!(snapshot.options[1].current_value.as_ref(), ENABLED_VALUE);
            assert_eq!(
                lease.read().options().options[1].current_value.as_ref(),
                DISABLED_VALUE
            );

            drop(lease);
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn two_sessions_serialize_without_value_leakage() {
        smol::block_on(async {
            let first = register(MakiId::generate());
            let second = register(MakiId::generate());

            first
                .set_option(YOLO_OPTION_ID, ENABLED_VALUE)
                .await
                .unwrap();

            assert_eq!(
                first.read().options().options[1].current_value.as_ref(),
                ENABLED_VALUE
            );
            assert_eq!(
                second.read().options().options[1].current_value.as_ref(),
                DISABLED_VALUE
            );
            first.close().await.unwrap();
            second.close().await.unwrap();
        });
    }

    #[test]
    fn checkpoint_failure_preserves_committed_state() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = SessionCoordinatorHandle::register(params(id, writer(true))).unwrap();
            let before = coordinator.read().options();

            assert!(matches!(
                coordinator.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(coordinator.read().options(), before);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn change_directory_returns_and_commits_canonical_path() {
        smol::block_on(async {
            let id = MakiId::generate();
            let saved = Arc::new(Mutex::new(Vec::new()));
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let saved = Arc::clone(&saved);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    lock(&saved).push(request.snapshot.cwd.clone());
                    Box::pin(async move {
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                }
            });
            let mut params = params(id, checkpoint);
            params.directory_adopter = Arc::new(|_: PathBuf| {
                Box::pin(async { Ok(PathBuf::from("/canonical")) }) as DirectoryAdoptionFuture
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            let canonical = coordinator
                .change_directory(PathBuf::from("relative"))
                .await
                .unwrap();

            assert_eq!(canonical, PathBuf::from("/canonical"));
            assert_eq!(coordinator.read().cwd(), canonical);
            assert_eq!(lock(&saved).as_slice(), [PathBuf::from("/canonical")]);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn failed_change_directory_is_atomic() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(false));
            params.directory_adopter = Arc::new(|_: PathBuf| {
                Box::pin(async { Err(Arc::from("not a directory")) }) as DirectoryAdoptionFuture
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            assert!(matches!(
                coordinator.change_directory(PathBuf::from("bad")).await,
                Err(SessionCoordinatorError::DirectoryAdoption(message))
                    if message.as_ref() == "not a directory"
            ));
            assert_eq!(coordinator.read().cwd(), PathBuf::from("/project"));
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn directory_checkpoint_failure_rolls_back_runtime() {
        smol::block_on(async {
            let id = MakiId::generate();
            let adopted = Arc::new(Mutex::new(Vec::new()));
            let mut params = params(id, writer(true));
            params.directory_adopter = Arc::new({
                let adopted = Arc::clone(&adopted);
                move |path: PathBuf| {
                    lock(&adopted).push(path.clone());
                    Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
                }
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            assert!(matches!(
                coordinator.change_directory(PathBuf::from("/next")).await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(coordinator.read().cwd(), PathBuf::from("/project"));
            assert_eq!(
                lock(&adopted).as_slice(),
                [PathBuf::from("/next"), PathBuf::from("/project")]
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn history_replacement_checkpoints_before_publication() {
        smol::block_on(async {
            let id = MakiId::generate();
            let saved = Arc::new(Mutex::new(Vec::new()));
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let saved = Arc::clone(&saved);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    if let Some(history) = &request.snapshot.history {
                        lock(&saved).push(history.as_ref().clone());
                    }
                    Box::pin(async move {
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                }
            });
            let coordinator = SessionCoordinatorHandle::register(params(id, checkpoint)).unwrap();
            let history = vec![Message::user("saved".into())];

            coordinator.replace_history(history.clone()).await.unwrap();

            let committed = coordinator.read().history();
            assert_eq!(committed.len(), 1);
            assert_eq!(
                serde_json::to_value(&committed[0]).unwrap(),
                serde_json::to_value(&history[0]).unwrap()
            );
            {
                let saved = lock(&saved);
                assert_eq!(saved.len(), 1);
                assert_eq!(
                    serde_json::to_value(&saved[0][0]).unwrap(),
                    serde_json::to_value(&history[0]).unwrap()
                );
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn history_checkpoint_failure_preserves_committed_state() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(true));
            params.history = vec![Message::user("before".into())];
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let before = coordinator.read().history();

            assert!(matches!(
                coordinator
                    .replace_history(vec![Message::user("after".into())])
                    .await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            let after = coordinator.read().history();
            assert_eq!(after.len(), before.len());
            assert_eq!(
                serde_json::to_value(&after[0]).unwrap(),
                serde_json::to_value(&before[0]).unwrap()
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn model_adoption_failure_preserves_committed_state() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(false));
            params.definitions = builtin_option_definitions(
                "test/model",
                [Arc::from("test/model"), Arc::from("openai/gpt-5")],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            params.model_adopter = Arc::new(|_: Model| {
                Box::pin(async { Err(Arc::from("rejected")) }) as ModelAdoptionFuture
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let before = coordinator.read().options();

            assert!(matches!(
                coordinator.set_option(MODEL_OPTION_ID, "openai/gpt-5").await,
                Err(SessionCoordinatorError::ModelAdoption(message)) if message.as_ref() == "rejected"
            ));
            assert_eq!(coordinator.read().options(), before);
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn model_change_atomically_disables_fast() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(false));
            params.model = Arc::from("anthropic/claude-opus-4-8");
            params.definitions = builtin_option_definitions(
                "anthropic/claude-opus-4-8",
                [
                    Arc::from("anthropic/claude-opus-4-8"),
                    Arc::from("openai/gpt-5"),
                ],
                false,
                true,
                false,
                ThinkingConfig::Off,
            );
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let before = coordinator.read().options();

            let after = coordinator
                .set_option(MODEL_OPTION_ID, "openai/gpt-5")
                .await
                .unwrap();

            assert_eq!(after.version, before.version + 1);
            assert_eq!(after.options[0].current_value.as_ref(), "openai/gpt-5");
            assert_eq!(after.options[2].current_value.as_ref(), DISABLED_VALUE);
            coordinator.close().await.unwrap();
        });
    }

    /// Thinking is a session option like fast, so a switch to a model that
    /// cannot think has to take it down with the model rather than leave the
    /// session claiming an effort no request will carry.
    #[test]
    fn model_change_atomically_disables_thinking() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(false));
            params.model = Arc::from("anthropic/claude-opus-4-8");
            params.definitions = builtin_option_definitions(
                "anthropic/claude-opus-4-8",
                [
                    Arc::from("anthropic/claude-opus-4-8"),
                    Arc::from("ollama/llama3"),
                ],
                false,
                false,
                false,
                ThinkingConfig::Effort(maki_providers::Effort::High),
            );
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            let after = coordinator
                .set_option(MODEL_OPTION_ID, "ollama/llama3")
                .await
                .unwrap();

            assert_eq!(thinking_value(&after).as_ref(), "off");
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn model_settings_commit_atomically_with_one_checkpoint() {
        smol::block_on(async {
            let id = MakiId::generate();
            let saved = Arc::new(Mutex::new(Vec::new()));
            let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = Arc::new({
                let saved = Arc::clone(&saved);
                move |request: CheckpointRequest<SessionCheckpoint>| {
                    lock(&saved).push(Arc::clone(&request.snapshot));
                    Box::pin(async move {
                        Ok(CheckpointAck {
                            session_id: request.session_id,
                            version: request.version,
                        })
                    }) as CheckpointFuture
                }
            });
            let mut params = params(id, checkpoint);
            params.model = Arc::from("ollama/llama3");
            params.definitions = builtin_option_definitions(
                "ollama/llama3",
                [
                    Arc::from("ollama/llama3"),
                    Arc::from("anthropic/claude-opus-4-8"),
                ],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let before = coordinator.read().options();

            let after = coordinator
                .set_model(
                    Some(Arc::from("anthropic/claude-opus-4-8")),
                    Some(true),
                    Some(ThinkingConfig::Effort(maki_providers::Effort::High)),
                )
                .await
                .unwrap();

            assert_eq!(after.version, before.version + 1);
            assert_eq!(
                coordinator.read().model().as_ref(),
                "anthropic/claude-opus-4-8"
            );
            assert_eq!(
                current_option_value(&coordinator.read(), FAST_OPTION_ID).as_deref(),
                Some(ENABLED_VALUE)
            );
            assert_eq!(thinking_value(&after).as_ref(), "high");
            {
                let saved = lock(&saved);
                assert_eq!(saved.len(), 1);
                assert_eq!(saved[0].model.as_ref(), "anthropic/claude-opus-4-8");
                assert_eq!(saved[0].options, after);
            }
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn model_settings_checkpoint_failure_rolls_back_runtime_and_options() {
        smol::block_on(async {
            let id = MakiId::generate();
            let adopted = Arc::new(Mutex::new(Vec::new()));
            let mut params = params(id, writer(true));
            params.model = Arc::from("ollama/llama3");
            params.definitions = builtin_option_definitions(
                "ollama/llama3",
                [
                    Arc::from("ollama/llama3"),
                    Arc::from("anthropic/claude-opus-4-8"),
                ],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            params.model_adopter = Arc::new({
                let adopted = Arc::clone(&adopted);
                move |model: Model| {
                    lock(&adopted).push(model.spec());
                    Box::pin(async { Ok(()) }) as ModelAdoptionFuture
                }
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let before = coordinator.read().options();

            assert!(matches!(
                coordinator
                    .set_model(
                        Some(Arc::from("anthropic/claude-opus-4-8")),
                        Some(true),
                        Some(ThinkingConfig::Effort(maki_providers::Effort::High)),
                    )
                    .await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));

            assert_eq!(coordinator.read().model().as_ref(), "ollama/llama3");
            assert_eq!(coordinator.read().options(), before);
            assert_eq!(
                lock(&adopted).as_slice(),
                [
                    "anthropic/claude-opus-4-8".to_string(),
                    "ollama/llama3".to_string(),
                ]
            );
            coordinator.close().await.unwrap();
        });
    }

    /// The reverse of the clamp: asking for thinking on a model that has none
    /// is refused rather than stored and silently dropped at request time.
    #[test]
    fn thinking_is_refused_on_a_model_that_cannot_think() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(false));
            params.model = Arc::from("ollama/llama3");
            params.definitions = builtin_option_definitions(
                "ollama/llama3",
                [Arc::from("ollama/llama3")],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            let error = coordinator
                .set_option(THINKING_OPTION_ID, "high")
                .await
                .unwrap_err();

            assert!(matches!(
                error,
                SessionCoordinatorError::Option(SessionOptionError::ThinkingUnsupported)
            ));
            coordinator.close().await.unwrap();
        });
    }

    /// A token budget is not in the option's value list, and has to be
    /// accepted anyway: the list is what a client can offer, not the domain.
    #[test]
    fn thinking_accepts_a_budget_outside_its_value_list() {
        let definitions = builtin_option_definitions(
            "anthropic/claude-opus-4-8",
            [],
            false,
            false,
            false,
            ThinkingConfig::Off,
        );
        let thinking = definitions
            .iter()
            .find(|definition| definition.id.as_ref() == THINKING_OPTION_ID)
            .expect("thinking is a builtin option");

        assert!(thinking.accepts("high"), "a listed effort is selectable");
        assert!(thinking.accepts("8192"), "a budget is accepted as typed");
        assert!(!thinking.accepts("deeply"), "nonsense is still rejected");
    }

    fn thinking_value(snapshot: &SessionOptionsSnapshot) -> Arc<str> {
        snapshot
            .options
            .iter()
            .find(|option| option.definition.id.as_ref() == THINKING_OPTION_ID)
            .map(|option| Arc::clone(&option.current_value))
            .expect("thinking is a builtin option")
    }

    #[test]
    fn model_checkpoint_failure_rolls_back_runtime_and_snapshot() {
        smol::block_on(async {
            let id = MakiId::generate();
            let adopted = Arc::new(Mutex::new(Vec::new()));
            let mut params = params(id, writer(true));
            params.model = Arc::from("anthropic/claude-opus-4-8");
            params.definitions = builtin_option_definitions(
                "anthropic/claude-opus-4-8",
                [
                    Arc::from("anthropic/claude-opus-4-8"),
                    Arc::from("openai/gpt-5"),
                ],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            params.model_adopter = Arc::new({
                let adopted = Arc::clone(&adopted);
                move |model: Model| {
                    lock(&adopted).push(model.spec());
                    Box::pin(async { Ok(()) }) as ModelAdoptionFuture
                }
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();
            let before = coordinator.read().options();

            assert!(matches!(
                coordinator
                    .set_option(MODEL_OPTION_ID, "openai/gpt-5")
                    .await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(coordinator.read().options(), before);
            assert_eq!(
                lock(&adopted).as_slice(),
                [
                    "openai/gpt-5".to_string(),
                    "anthropic/claude-opus-4-8".to_string(),
                ]
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn failed_model_checkpoint_and_rollback_adopts_live_runtime_in_coordinator() {
        smol::block_on(async {
            let id = MakiId::generate();
            let adopted = Arc::new(Mutex::new(Vec::new()));
            let mut params = params(id, writer(true));
            params.model = Arc::from("anthropic/claude-opus-4-8");
            params.definitions = builtin_option_definitions(
                "anthropic/claude-opus-4-8",
                [
                    Arc::from("anthropic/claude-opus-4-8"),
                    Arc::from("openai/gpt-5"),
                ],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            params.model_adopter = Arc::new({
                let adopted = Arc::clone(&adopted);
                move |model: Model| {
                    let spec = model.spec();
                    let fail = spec == "anthropic/claude-opus-4-8";
                    lock(&adopted).push(spec);
                    Box::pin(async move {
                        if fail {
                            Err(Arc::from("rollback rejected"))
                        } else {
                            Ok(())
                        }
                    }) as ModelAdoptionFuture
                }
            });
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            let error = coordinator
                .set_option(MODEL_OPTION_ID, "openai/gpt-5")
                .await
                .unwrap_err();

            assert!(matches!(error, SessionCoordinatorError::ModelRollback(_)));
            assert_eq!(coordinator.read().model().as_ref(), "openai/gpt-5");
            assert_eq!(
                current_option_value(&coordinator.read(), MODEL_OPTION_ID).as_deref(),
                Some("openai/gpt-5")
            );
            assert_eq!(
                lock(&adopted).as_slice(),
                [
                    "openai/gpt-5".to_string(),
                    "anthropic/claude-opus-4-8".to_string(),
                ]
            );
            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn retire_immediately_allows_same_id_while_committer_is_retained() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();
            let committer = lease.committer().unwrap();
            std::mem::forget(lease);

            coordinator.retire();
            assert!(matches!(
                SessionCoordinatorHandle::resolve(id),
                Err(SessionCoordinatorError::StaleSession(_))
            ));
            let replacement = register(id);

            drop(committer);
            replacement.close().await.unwrap();
        });
    }

    #[test]
    fn model_discovery_checkpoint_failure_preserves_definition() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = SessionCoordinatorHandle::register(params(id, writer(true))).unwrap();
            let before = coordinator.read().options();

            assert!(matches!(
                coordinator
                    .update_model_values(vec![Arc::from("openai/gpt-5")])
                    .await,
                Err(SessionCoordinatorError::Checkpoint(_))
            ));
            assert_eq!(coordinator.read().options(), before);
            coordinator.close().await.unwrap();
        });
    }

    /// A session registered before providers finish discovering models knows
    /// only the model it started on, so selecting anything else is refused.
    /// Every frontend must republish the discovered list, or that session is
    /// stuck on its startup model for as long as it lives.
    #[test]
    fn a_model_discovered_after_registration_becomes_selectable() {
        smol::block_on(async {
            let id = MakiId::generate();
            let mut params = params(id, writer(false));
            params.definitions = builtin_option_definitions(
                "test/model",
                [],
                false,
                false,
                false,
                ThinkingConfig::Off,
            );
            let coordinator = SessionCoordinatorHandle::register(params).unwrap();

            assert!(
                matches!(
                    coordinator.set_option("model", "openai/gpt-5").await,
                    Err(SessionCoordinatorError::Option(
                        SessionOptionError::InvalidValue { .. }
                    ))
                ),
                "a model the session has never been told about must be refused"
            );

            coordinator
                .update_model_values(vec![Arc::from("openai/gpt-5")])
                .await
                .expect("discovery republishes the model list");
            coordinator
                .set_option("model", "openai/gpt-5")
                .await
                .expect("a discovered model must become selectable");

            coordinator.close().await.unwrap();
        });
    }

    #[test]
    fn model_snapshot_retains_current_model_missing_from_discovery() {
        let definitions = builtin_option_definitions(
            "current/model",
            [Arc::from("other/model")],
            false,
            false,
            false,
            ThinkingConfig::Off,
        );
        assert_eq!(definitions[0].values[0].value.as_ref(), "current/model");
    }
}
