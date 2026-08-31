use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError};

use maki_config::ModelPolicy;
use maki_providers::{Message, Model};
use maki_storage::checkpoint::{
    CheckpointError, CheckpointRequest, CheckpointVersion, CheckpointWriter,
};
use maki_storage::id::MakiId;
use thiserror::Error;

use crate::SessionMailbox;
use crate::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, FAST_OPTION_ID, MODEL_OPTION_ID, SessionOptionCategory,
    SessionOptionDefinition, SessionOptionError, SessionOptionOwner, SessionOptionValue,
    SessionOptions, SessionOptionsSnapshot, SessionOptionsSubscription, WORKFLOW_OPTION_ID,
    YOLO_OPTION_ID,
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

#[derive(Clone)]
struct DirectoryEntry {
    generation: u64,
    tx: flume::Sender<Operation>,
    read: SessionReadHandle,
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
    #[error("model checkpoint failed and runtime rollback also failed: {0}")]
    ModelRollback(Arc<str>),
    #[error("directory runtime transition failed: {0}")]
    DirectoryAdoption(Arc<str>),
    #[error("directory checkpoint failed and runtime rollback also failed: {0}")]
    DirectoryRollback(Arc<str>),
}

#[derive(Debug, Clone)]
pub struct SessionCheckpoint {
    pub history: Arc<Vec<Message>>,
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

enum LeaseRelease {
    CommitHistory {
        history: Arc<Vec<Message>>,
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
            tx.send_async(Operation::PreparePluginOptions {
                plugin: Arc::clone(&plugin),
                definitions: definitions.clone(),
                prepared: prepared_tx,
                decision: decision_rx,
            })
            .await
            .map_err(|_| SessionCoordinatorError::StaleSession(session_id))?;
            match prepared_rx
                .recv_async()
                .await
                .map_err(|_| SessionCoordinatorError::StaleSession(session_id))?
            {
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
        let snapshots = {
            let mut state = lock(&self.state);
            let snapshots = match SessionOptions::commit_batch(candidates) {
                Ok(snapshots) => snapshots,
                Err(error) => {
                    drop(state);
                    abort_plugin_options(prepared).await?;
                    return Err(error.into());
                }
            };
            if definitions.is_empty() {
                state.definitions.remove(plugin.as_ref());
            } else {
                state.definitions.insert(Arc::clone(&plugin), definitions);
            }
            snapshots
        };
        let mut committed = Vec::with_capacity(prepared.len());
        for ((prepared, decision), snapshot) in prepared.into_iter().zip(snapshots) {
            let (reply, response) = flume::bounded(1);
            decision
                .send(PluginOptionDecision::Commit(reply))
                .map_err(|_| SessionCoordinatorError::StaleSession(prepared.session_id))?;
            response
                .recv_async()
                .await
                .map_err(|_| SessionCoordinatorError::StaleSession(prepared.session_id))??;
            committed.push((prepared.session_id, snapshot));
        }
        Ok(committed)
    }
}

impl SessionCoordinatorHandle {
    pub fn register(params: SessionCoordinatorParams) -> Result<Self, SessionCoordinatorError> {
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
        let mut directory = lock(&DIRECTORY);
        if directory.entries.contains_key(&session_id) {
            return Err(SessionCoordinatorError::DuplicateSession(session_id));
        }
        let mut catalog_state = lock(&catalog.state);
        definitions.extend(
            catalog_state
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
            })),
        };
        let (tx, rx) = flume::unbounded();
        let generation = GENERATION.fetch_add(1, Ordering::Relaxed);
        directory.entries.insert(
            session_id,
            DirectoryEntry {
                generation,
                tx: tx.clone(),
                read: read.clone(),
                mailbox,
            },
        );
        catalog_state.sessions.insert(
            session_id,
            CatalogSession {
                generation,
                tx: tx.clone(),
            },
        );
        drop(catalog_state);
        drop(directory);
        smol::spawn(run(
            session_id,
            generation,
            catalog,
            read.clone(),
            model_policy,
            model_adopter,
            directory_adopter,
            checkpoint,
            rx,
        ))
        .detach();
        Ok(Self {
            session_id,
            generation,
            tx,
            read,
        })
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
        let (reply, response) = flume::bounded(1);
        self.released
            .send_async(LeaseRelease::CommitHistory {
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
    while let Ok(operation) = rx.recv_async().await {
        match operation {
            Operation::AcquireLease { reply } => {
                let (released, wait) = flume::bounded(1);
                let lease = SessionLease {
                    session_id,
                    released: Some(released),
                    read: read.clone(),
                };
                if reply.send(Ok(lease)).is_err() {
                    continue;
                }
                if let Ok(LeaseRelease::CommitHistory { history, reply }) = wait.recv_async().await
                {
                    let result = replace_history(&read, &*checkpoint, history).await;
                    let _ = reply.send(result);
                    let _ = wait.recv_async().await;
                }
            }
            Operation::SetOption {
                id,
                value,
                version,
                reply,
            } => {
                let result = if version.is_some_and(|version| read.options().version != version) {
                    Err(SessionOptionError::StaleHandle(Arc::from(
                        "session option snapshot changed during validation",
                    ))
                    .into())
                } else if id.as_ref() == MODEL_OPTION_ID {
                    #[allow(clippy::explicit_auto_deref)]
                    set_model(&read, &*model_policy, &*model_adopter, &*checkpoint, &value).await
                } else {
                    set_option(&read, &*checkpoint, &id, &value).await
                };
                let _ = reply.send(result);
            }
            Operation::ReplaceHistory { history, reply } => {
                let result = replace_history(&read, &*checkpoint, history).await;
                let _ = reply.send(result);
            }
            Operation::ChangeDirectory { path, reply } => {
                let result = change_directory(&read, &*directory_adopter, &*checkpoint, path).await;
                let _ = reply.send(result);
            }
            Operation::UpdateModelValues { specs, reply } => {
                let result = update_model_values(&read, &*checkpoint, specs).await;
                let _ = reply.send(result);
            }
            Operation::PreparePluginOptions {
                plugin,
                definitions,
                prepared,
                decision,
            } => {
                let previous = read.options.snapshot();
                let result = read
                    .options
                    .prepare_replace_plugin(&plugin, definitions)
                    .map(|candidate| {
                        candidate.unwrap_or_else(|| read.options.unchanged_candidate())
                    })
                    .map_err(Into::into);
                let candidate = match result {
                    Ok(candidate) => candidate,
                    Err(error) => {
                        let _ = prepared.send(Err(error));
                        continue;
                    }
                };
                let candidate_snapshot = SessionOptions::candidate_snapshot(&candidate);
                let (history, model, cwd) = {
                    let state = lock(&read.state);
                    (
                        Arc::clone(&state.history),
                        Arc::clone(&state.model),
                        state.cwd.clone(),
                    )
                };
                if let Err(error) =
                    checkpoint_state(&read, &*checkpoint, history, model, cwd, candidate_snapshot)
                        .await
                {
                    let _ = prepared.send(Err(error.into()));
                    continue;
                }
                let staged = PreparedPluginOptions {
                    session_id,
                    options: Arc::clone(&read.options),
                    previous: previous.clone(),
                    candidate,
                };
                if prepared.send(Ok(staged)).is_err() {
                    let _ = checkpoint_options(&read, &*checkpoint, previous).await;
                    continue;
                }
                match decision.recv_async().await {
                    Ok(PluginOptionDecision::Commit(reply)) => {
                        let _ = reply.send(Ok(()));
                    }
                    Ok(PluginOptionDecision::Abort(reply)) => {
                        let result = checkpoint_options(&read, &*checkpoint, previous)
                            .await
                            .map_err(Into::into);
                        let _ = reply.send(result);
                    }
                    Err(_) => {
                        let _ = checkpoint_options(&read, &*checkpoint, previous).await;
                    }
                }
            }
            Operation::Close { reply } => {
                unregister(session_id, generation);
                catalog.unregister(session_id, generation);
                let _ = reply.send(());
                break;
            }
        }
    }
    unregister(session_id, generation);
    catalog.unregister(session_id, generation);
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
    let (history, model, cwd) = {
        let state = lock(&read.state);
        (
            Arc::clone(&state.history),
            Arc::clone(&state.model),
            state.cwd.clone(),
        )
    };
    checkpoint_state(read, checkpoint, history, model, cwd, options).await
}

async fn checkpoint_state(
    read: &SessionReadHandle,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    history: Arc<Vec<Message>>,
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
    let (model, cwd) = {
        let state = lock(&read.state);
        (Arc::clone(&state.model), state.cwd.clone())
    };
    checkpoint_state(
        read,
        checkpoint,
        Arc::clone(&history),
        model,
        cwd,
        read.options.snapshot(),
    )
    .await?;
    lock(&read.state).history = history;
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
    let (history, model, cwd) = {
        let state = lock(&read.state);
        (
            Arc::clone(&state.history),
            Arc::clone(&state.model),
            state.cwd.clone(),
        )
    };
    checkpoint_state(read, checkpoint, history, model, cwd, options).await?;
    read.options.commit(candidate).map_err(Into::into)
}

async fn change_directory(
    read: &SessionReadHandle,
    directory_adopter: &dyn DirectoryAdopter,
    checkpoint: &dyn CheckpointWriter<SessionCheckpoint>,
    path: PathBuf,
) -> Result<PathBuf, SessionCoordinatorError> {
    let (history, model, previous) = {
        let state = lock(&read.state);
        (
            Arc::clone(&state.history),
            Arc::clone(&state.model),
            state.cwd.clone(),
        )
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
        history,
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
    spec: &str,
) -> Result<SessionOptionsSnapshot, SessionCoordinatorError> {
    if !model_policy.allows(spec) {
        return Err(SessionOptionError::PolicyRejected(Arc::from(spec)).into());
    }
    let model = Model::from_spec(spec).map_err(|_| SessionOptionError::InvalidValue {
        id: Arc::from(MODEL_OPTION_ID),
        value: Arc::from(spec),
    })?;
    let previous_spec = {
        let state = lock(&read.state);
        Arc::clone(&state.model)
    };
    let fast_value: Arc<str> = if model.supports_fast() {
        read.options
            .snapshot()
            .options
            .iter()
            .find(|option| option.definition.id.as_ref() == FAST_OPTION_ID)
            .map_or_else(
                || Arc::from(DISABLED_VALUE),
                |option| Arc::clone(&option.current_value),
            )
    } else {
        Arc::from(DISABLED_VALUE)
    };
    let Some(candidate) = read.options.prepare_set_values(&[
        (MODEL_OPTION_ID, spec),
        (FAST_OPTION_ID, fast_value.as_ref()),
    ])?
    else {
        return Ok(read.options.snapshot());
    };
    let options = SessionOptions::candidate_snapshot(&candidate);
    model_adopter
        .adopt(model)
        .await
        .map_err(SessionCoordinatorError::ModelAdoption)?;
    let (history, cwd) = {
        let state = lock(&read.state);
        (Arc::clone(&state.history), state.cwd.clone())
    };
    let checkpoint_result =
        checkpoint_state(read, checkpoint, history, Arc::from(spec), cwd, options).await;
    if let Err(error) = checkpoint_result {
        let previous_model = Model::from_spec(&previous_spec).map_err(|rollback_error| {
            SessionCoordinatorError::ModelRollback(Arc::from(rollback_error.to_string()))
        })?;
        if let Err(rollback_error) = model_adopter.adopt(previous_model).await {
            return Err(SessionCoordinatorError::ModelRollback(Arc::from(format!(
                "{error}; rollback: {rollback_error}"
            ))));
        }
        return Err(error.into());
    }
    lock(&read.state).model = Arc::from(spec);
    match read.options.commit(candidate) {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => {
            lock(&read.state).model = previous_spec;
            Err(error.into())
        }
    }
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
    let Some(candidate) = read.options.prepare_set(id, value)? else {
        return Ok(read.options.snapshot());
    };
    let options = SessionOptions::candidate_snapshot(&candidate);
    let (history, model, cwd) = {
        let state = lock(&read.state);
        (
            Arc::clone(&state.history),
            Arc::clone(&state.model),
            state.cwd.clone(),
        )
    };
    checkpoint_state(read, checkpoint, history, model, cwd, options).await?;
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
    ]
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
    fn external_mutation_queues_until_lease_release() {
        smol::block_on(async {
            let id = MakiId::generate();
            let coordinator = register(id);
            let lease = coordinator.acquire_lease().await.unwrap();
            let (done_tx, done_rx) = flume::bounded(1);
            let queued = coordinator.clone();
            smol::spawn(async move {
                let result = queued.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await;
                let _ = done_tx.send(result);
            })
            .detach();

            assert!(done_rx.try_recv().is_err());
            drop(lease);
            let snapshot = done_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(snapshot.options[1].current_value.as_ref(), ENABLED_VALUE);
            coordinator.close().await.unwrap();
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
                let result = queued.set_option(YOLO_OPTION_ID, ENABLED_VALUE).await;
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
            let snapshot = done_rx.recv_async().await.unwrap().unwrap();
            assert_eq!(snapshot.options[1].current_value.as_ref(), ENABLED_VALUE);
            coordinator.close().await.unwrap();
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
                    lock(&saved).push(request.snapshot.history.as_ref().clone());
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

    #[test]
    fn model_snapshot_retains_current_model_missing_from_discovery() {
        let definitions = builtin_option_definitions(
            "current/model",
            [Arc::from("other/model")],
            false,
            false,
            false,
        );
        assert_eq!(definitions[0].values[0].value.as_ref(), "current/model");
    }
}
