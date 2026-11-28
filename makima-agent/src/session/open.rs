use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use makima_config::SessionDefaults;
use makima_domain::LockExt;
use makima_providers::provider::BoxFuture;
use makima_providers::{Message, ProviderRegistry};
use makima_storage::StateDir;
use makima_storage::checkpoint::{
    CheckpointClaim, CheckpointPublication, CheckpointWriter, ClaimingWriter,
};
use makima_storage::id::SessionRef;
use makima_storage::session_lock::{ClaimedSessionLock, LockHeartbeat, SessionPublicationGuard};
use makima_storage::session_state::StateChange;
use makima_storage::sessions::{SESSIONS_DIR, read_prefs};
use smol::Task;
use tracing::warn;

use crate::actor::{PreparedModel, PreparedTransactionSnapshot};
use crate::cancel::SessionCancellation;
use crate::command::SessionCommandHost;
use crate::headless::{
    self, InteractiveControlSender, InteractiveHandle, InteractiveInputSender, InteractiveParams,
    McpLifecycle, ProviderPreparation, RootBinding, SessionRunnerSettings,
};
use crate::journal::{JournalChange, JournalCommit, SessionJournalSnapshot};
use crate::mcp::McpHandle;
use crate::permissions::{PermissionManager, PermissionSnapshot};
use crate::session_checkpoint::stored_agent_config;
use crate::session_coordinator::{
    ConfigProjector, ModelPreparer, PreparedSessionCoordinator, SessionCheckpoint,
    SessionCoordinatorHandle, SessionCoordinatorParams, SessionOptionCatalog,
    builtin_option_definitions,
};
use crate::session_defaults;
use crate::session_options::MODEL_OPTION_ID;
use crate::session_recovery::{
    OpenModel, OpenSessionError, RecoveryCandidate, SessionSource, continuation_notice,
    open_session, resolve_legacy_mode,
};
use crate::session_writer::{SessionWriter, StoredSession, WriterStatus};
use crate::tools::offload::{OffloadCleanup, OffloadStore};
use crate::tools::{FileWriteLocks, ToolContext};
use crate::{
    AgentId, AgentManagerHandle, AgentRef, CancelToken, Envelope, SessionMailbox, SharedMessages,
    TurnId,
};

use super::{AnswerRouter, ResetSeed, SessionEventPump};

const REUSE_UNAVAILABLE: &str = "session replacement requires the current session writer";
const ROOT_CONFIG_UNAVAILABLE: &str = "session root configuration is unavailable";

pub enum Durability<'a> {
    Persistent {
        storage: &'a StateDir,
        source: Box<SessionSource>,
        explicit_model: bool,
        require_matching_cwd: bool,
        skip_startup_if_children_continue: bool,
    },
    Ephemeral,
}

pub type FrontendObserver = Box<dyn FnMut(&Envelope) + Send>;

pub trait FrontendServices {
    fn configure<'a>(
        &'a mut self,
        _params: &'a mut InteractiveParams,
        _continuing_root: bool,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn root_binding(&mut self) -> Option<RootBinding> {
        None
    }

    fn bind_root<'a>(
        &'a mut self,
        _session: &'a SessionHandle,
    ) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async { Ok(()) })
    }

    fn observe(&mut self, _session: &SessionHandle) -> FrontendObserver {
        Box::new(|_| {})
    }
}

pub struct OpenRequest<'a> {
    pub params: InteractiveParams,
    pub durability: Durability<'a>,
    pub continue_turns: bool,
    pub options: SessionOptionCatalog,
    pub model_options: Vec<Arc<str>>,
    pub prepare: ProviderPreparation,
    pub services: &'a mut (dyn FrontendServices + Send),
}

pub struct SessionHandle {
    pub session_id: SessionRef,
    pub manager: AgentManagerHandle,
    pub coordinator: SessionCoordinatorHandle,
    pub events: Arc<SessionEventPump>,
    pub cancellation: SessionCancellation,
    pub answers: AnswerRouter,
    pub input_tx: InteractiveInputSender,
    pub control_tx: InteractiveControlSender,
    pub permissions: Arc<PermissionManager>,
    pub tool_names: Vec<String>,
    root: AgentRef,
    providers: Arc<ProviderRegistry>,
    task: Mutex<Option<Task<()>>>,
    offload_cleanup: Option<OffloadCleanup>,
    mcp: Option<McpHandle>,
    checkpoint: Option<Arc<SessionWriter>>,
    lock: Mutex<Option<LockHeartbeat>>,
    mailbox: SessionMailbox,
    template: ToolContext,
    agent_tx: flume::Sender<Envelope>,
    history: SharedMessages,
    offload: Option<Arc<OffloadStore>>,
    start: Mutex<Option<flume::Sender<()>>>,
    was_existing: bool,
    continuing_root: BTreeSet<TurnId>,
    continuation_count: usize,
    opening_history: Arc<Vec<Message>>,
    opening_cost: Option<f64>,
    cwd: PathBuf,
}

impl SessionHandle {
    pub fn activate(&self) {
        if let Some(start) = self.start.lock_unpoisoned().take() {
            let _ = start.try_send(());
        }
    }

    pub fn runner_start_is_pending(&self) -> bool {
        self.start.lock_unpoisoned().is_some()
    }

    pub fn template(&self) -> &ToolContext {
        &self.template
    }
    pub fn agent_sender(&self) -> flume::Sender<Envelope> {
        self.agent_tx.clone()
    }
    pub fn shared_history(&self) -> SharedMessages {
        Arc::clone(&self.history)
    }
    pub fn mailbox(&self) -> &SessionMailbox {
        &self.mailbox
    }
    pub fn offload(&self) -> Option<Arc<OffloadStore>> {
        self.offload.clone()
    }
    pub fn writer(&self) -> Option<SessionPersistence> {
        self.checkpoint.clone().map(SessionPersistence)
    }

    pub fn reuse(
        &self,
        guard: SessionPublicationGuard,
        seed: StoredSession,
    ) -> Result<SessionReuse, OpenSessionError> {
        let checkpoint = self
            .checkpoint
            .as_ref()
            .filter(|writer| writer.session_id() == seed.id)
            .ok_or_else(|| OpenSessionError::Compose(REUSE_UNAVAILABLE.into()))?;
        Ok(SessionReuse {
            guard,
            seed,
            checkpoint: Arc::clone(checkpoint),
            coordinator: self.coordinator.clone(),
            offload: self.offload(),
        })
    }

    pub fn root(&self) -> &AgentRef {
        &self.root
    }

    pub fn projection(&self) -> Arc<SessionJournalSnapshot> {
        self.manager.journal().projection()
    }

    pub fn is_persistent(&self) -> bool {
        self.checkpoint.is_some()
    }

    pub fn command_host(&self) -> SessionCommandHost {
        SessionCommandHost::new(
            self.control_tx.clone(),
            self.coordinator.clone(),
            Arc::clone(&self.providers),
        )
    }

    pub fn was_existing(&self) -> bool {
        self.was_existing
    }

    pub fn continuing_root(&self) -> &BTreeSet<TurnId> {
        &self.continuing_root
    }

    pub fn continuation_notice(&self) -> Option<String> {
        continuation_notice(self.continuation_count)
    }

    pub fn opening_history(&self) -> &[Message] {
        &self.opening_history
    }

    pub fn opening_cost(&self) -> Option<f64> {
        self.opening_cost
    }

    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub async fn drain(&self) -> Result<(), OpenSessionError> {
        if let Some(writer) = &self.checkpoint {
            writer.drain().await?;
        }
        Ok(())
    }

    pub async fn close(&self, timeout: Duration) -> Result<(), OpenSessionError> {
        self.manager.suspend(timeout).await;
        self.manager.journal().invalidate_state_access();
        let task = self.task.lock_unpoisoned().take();
        if let Some(task) = task {
            task.cancel().await;
        }
        let closed = if SessionCoordinatorHandle::resolve(self.session_id.id()).is_ok() {
            self.coordinator.close().await
        } else {
            Ok(())
        };
        let drained = self.drain().await;
        if let Some(mcp) = &self.mcp {
            mcp.shutdown().await;
        }
        let lock = self.lock.lock_unpoisoned().take();
        drop(lock);
        closed?;
        drained
    }

    pub fn into_ephemeral_shutdown(mut self) -> (Option<Task<()>>, Option<OffloadCleanup>) {
        (
            self.task.lock_unpoisoned().take(),
            self.offload_cleanup.take(),
        )
    }
}

#[derive(Default)]
pub struct PrepareOptions {
    pub permission_base: Option<Arc<PermissionManager>>,
    pub model_prepare: Option<ModelPreparer>,
    pub file_write_locks: Option<Arc<FileWriteLocks>>,
    pub reuse: Option<SessionReuse>,
    pub reset: Option<ResetSeed>,
    pub expected_cwd: Option<String>,
    pub project_config: Option<ConfigProjector>,
    pub offload: Option<Arc<OffloadStore>>,
    pub startup: Option<PreparedModel>,
}

pub struct SessionReuse {
    guard: SessionPublicationGuard,
    seed: StoredSession,
    checkpoint: Arc<SessionWriter>,
    coordinator: SessionCoordinatorHandle,
    offload: Option<Arc<OffloadStore>>,
}

#[derive(Clone)]
pub struct SessionPersistence(Arc<SessionWriter>);

impl SessionPersistence {
    pub fn from_writer(writer: Arc<SessionWriter>) -> Self {
        Self(writer)
    }

    pub async fn drain(&self) -> Result<(), OpenSessionError> {
        Ok(self.0.drain().await?)
    }

    pub async fn close(&self) -> Result<(), OpenSessionError> {
        self.0.close().await?;
        Ok(())
    }

    pub fn status(&self) -> WriterStatus {
        self.0.status()
    }

    pub async fn publish_transaction(
        &self,
        snapshot: PreparedTransactionSnapshot,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<(), String> {
        claim(&publication).map_err(|error| error.to_string())?;
        if let Some(install) = snapshot.install {
            install()?;
        }
        self.drain().await.map_err(|error| error.to_string())
    }
}

struct Publication {
    recovery: RecoveryCandidate,
    guard: SessionPublicationGuard,
    lease: Option<ClaimedSessionLock>,
    replacing: Option<SessionCoordinatorHandle>,
}

pub struct PreparedSession {
    session: SessionHandle,
    coordinator: PreparedSessionCoordinator,
    publication: Option<Publication>,
    warnings: Vec<String>,
}

impl PreparedSession {
    pub fn session(&self) -> &SessionHandle {
        &self.session
    }

    pub fn stored(&self) -> Option<&StoredSession> {
        self.publication
            .as_ref()
            .map(|pending| pending.recovery.session())
    }

    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    pub fn take_lease(&mut self) -> Option<ClaimedSessionLock> {
        self.publication.as_mut()?.lease.take()
    }

    pub fn writer(&self) -> Option<SessionPersistence> {
        self.session.writer()
    }

    pub async fn publish(self) -> Result<SessionHandle, OpenSessionError> {
        let Self {
            session,
            coordinator,
            publication,
            ..
        } = self;
        let mut replacing = None;
        if let Some(Publication {
            recovery,
            guard,
            lease,
            replacing: current,
        }) = publication
        {
            let writer = session
                .checkpoint
                .as_ref()
                .ok_or_else(|| OpenSessionError::Compose(REUSE_UNAVAILABLE.into()))?;
            let id = session.session_id.id();
            guard
                .publish(|| ())?
                .ok_or(OpenSessionError::LockLost(id))?;
            recovery.publish_staged(writer, guard.clone()).await?;
            if let Some(lease) = lease {
                let coordinator = session.coordinator.clone();
                *session.lock.lock_unpoisoned() =
                    Some(LockHeartbeat::spawn(lease, move || coordinator.retire()));
            }
            replacing = current;
        }
        if let Some(current) = replacing {
            coordinator.activate_replacing(&current)?;
        } else {
            coordinator.activate()?;
        }
        Ok(session)
    }
}

pub async fn open(request: OpenRequest<'_>) -> Result<SessionHandle, OpenSessionError> {
    let session = prepare(request, PrepareOptions::default())
        .await?
        .publish()
        .await?;
    session.activate();
    Ok(session)
}

pub async fn prepare(
    request: OpenRequest<'_>,
    settings: PrepareOptions,
) -> Result<PreparedSession, OpenSessionError> {
    let OpenRequest {
        mut params,
        durability,
        continue_turns,
        options,
        model_options,
        prepare,
        services,
    } = request;
    let PrepareOptions {
        permission_base,
        model_prepare,
        file_write_locks,
        reuse,
        reset,
        expected_cwd,
        project_config,
        mut offload,
        startup,
    } = settings;
    let mut warnings = Vec::new();
    let mut was_existing = false;
    let mut continuing_root = BTreeSet::new();
    let mut continuation_count = 0;
    let mut persisted_options = Default::default();
    let mut checkpoint = None;
    let mut publication = None;
    let mut history = None;
    let mut opening_cost = None;
    let mut skip_startup = false;
    let mut effective_config = None;
    let mut permissions = None;
    if let Durability::Persistent {
        storage,
        source,
        explicit_model,
        require_matching_cwd,
        skip_startup_if_children_continue,
    } = durability
    {
        let (source, reset_config) = match reset {
            Some(seed) => (SessionSource::New(seed.session), seed.config),
            None => (*source, None),
        };
        let working_dir = params.initial_wd.to_string_lossy();
        let expected_cwd = expected_cwd
            .as_deref()
            .or_else(|| require_matching_cwd.then_some(working_dir.as_ref()));
        let (mut stored, guard, mut lease, writer, replacing) = if let Some(reuse) = reuse {
            offload = reuse.offload;
            let (model, warning) = OpenModel {
                startup: &params.model,
                explicit: explicit_model,
                providers: &params.providers,
                policy: &params.model_policy,
            }
            .select(&reuse.seed)?;
            params.model = model;
            warnings.extend(warning);
            (
                reuse.seed,
                reuse.guard,
                None,
                Some(reuse.checkpoint),
                Some(reuse.coordinator),
            )
        } else {
            let opening = open_session(
                storage,
                source,
                OpenModel {
                    startup: &params.model,
                    explicit: explicit_model,
                    providers: &params.providers,
                    policy: &params.model_policy,
                },
                expected_cwd,
            )?;
            was_existing = opening.resumed;
            warnings.extend(opening.warning.clone());
            params.model = opening.model.clone();
            let (stored, lease) = opening.into_claimed();
            let guard = lease.publication_guard();
            (stored, guard, Some(lease), None, None)
        };
        let root = stored.root_agent_id().unwrap_or_else(AgentId::generate);
        stored.bind_root_id(root)?;
        if permission_base.is_some()
            && stored
                .latest_state()?
                .scopes
                .get(&Some(root))
                .is_none_or(|scope| scope.config.is_none())
        {
            let defaults = session_defaults::resolve(
                params.defaults,
                &read_prefs(storage),
                &stored.meta,
                stored.messages().is_empty(),
                &params.model,
            );
            stored.meta.fast = defaults.fast;
            stored.meta.workflow = defaults.workflow;
            stored.meta.thinking = defaults.thinking;
        }
        if was_existing || permission_base.is_some() {
            warnings.extend(resolve_legacy_mode(&mut stored, &params.modes, storage)?);
        }
        let state = stored.latest_state()?;
        let root_scope = state.scopes.get(&Some(root));
        let config = root_scope.and_then(|scope| scope.config.as_ref());
        let snapshot = stored.history_snapshot();
        let cwd = config.map(|config| config.cwd.as_str()).or_else(|| {
            (!require_matching_cwd || permission_base.is_some()).then_some(stored.cwd.as_str())
        });
        if let Some(path) = cwd
            .map(Path::new)
            .filter(|path| require_matching_cwd || path.is_absolute())
        {
            params.initial_wd = path.to_path_buf();
        }
        params.defaults = if let Some(config) = &reset_config {
            let config = &config.0;
            SessionDefaults {
                fast: config.fast,
                workflow: config.workflow,
                thinking: Some(config.thinking.into()),
            }
        } else if let Some(config) = config {
            SessionDefaults {
                fast: config.fast,
                workflow: config.workflow,
                thinking: config.thinking,
            }
        } else {
            session_defaults::resolve(
                params.defaults,
                &read_prefs(storage),
                &stored.meta,
                snapshot.messages.is_empty(),
                &params.model,
            )
        };
        persisted_options = config
            .map(|config| &config.options)
            .unwrap_or(&stored.meta.session_options)
            .clone();
        if !require_matching_cwd && permission_base.is_none() {
            persisted_options.remove(MODEL_OPTION_ID);
        }
        if let Some(base) = permission_base {
            let mut snapshot = if let Some((config, rules)) = reset_config {
                params.initial_wd = config.context.cwd().to_path_buf();
                params.model = config.model.clone();
                effective_config = Some((*config).clone());
                rules
            } else {
                let (rules, yolo) = state
                    .scopes
                    .get(&None)
                    .map(|scope| (&scope.rules, scope.yolo))
                    .unwrap_or((&stored.meta.session_rules, stored.meta.yolo));
                PermissionSnapshot::from_stored(rules, yolo).map_err(compose)?
            };
            if !was_existing && !state.scopes.contains_key(&None) {
                snapshot.yolo = snapshot.yolo.or_else(|| base.persisted_yolo());
            }
            let manager = Arc::new(base.fork());
            manager.restore(snapshot);
            manager.set_cwd(params.initial_wd.clone());
            permissions = Some(manager);
        }
        let candidate =
            RecoveryCandidate::prepare_with_continuation(stored.clone(), continue_turns)?;
        let state = candidate.session().latest_state()?;
        continuation_count = state
            .scopes
            .values()
            .map(|scope| scope.admitted.len())
            .sum();
        if let Some(scope) = state.scopes.get(&Some(root)) {
            continuing_root.extend(scope.admitted.keys().copied());
        }
        if let Some(config) = config {
            let restored = crate::plan_state::prepare_restore_for_agent(
                &state,
                root,
                &config.mode,
                &params.modes,
            )
            .map_err(compose)?;
            if crate::modes::is_read_only_fallback(&restored.mode) {
                continuation_count -= continuing_root.len();
                continuing_root.clear();
            }
        }
        skip_startup = permissions.is_none()
            && (!continuing_root.is_empty()
                || (skip_startup_if_children_continue && continuation_count != 0));
        let journal = candidate.journal();
        params.session_id = Some(SessionRef::from(stored.id));
        params.root_id = Some(root);
        params.journal = Some(Arc::clone(&journal));
        opening_cost = journal.cost();
        history = Some(candidate.session().history_snapshot());
        let writer = match writer {
            Some(writer) => writer,
            None => {
                let directory = storage.ensure_subdir(SESSIONS_DIR)?;
                let lease = lease
                    .as_mut()
                    .ok_or_else(|| OpenSessionError::Compose(REUSE_UNAVAILABLE.into()))?;
                let writer = Arc::new(SessionWriter::open_leased(&directory, &stored, lease)?);
                writer.append(Vec::new()).await?;
                writer
            }
        };
        checkpoint = Some(writer);
        publication = Some(Publication {
            recovery: candidate,
            guard,
            lease,
            replacing,
        });
    }
    services
        .configure(&mut params, !continuing_root.is_empty())
        .await
        .map_err(OpenSessionError::Compose)?;
    let model = params.model.clone();
    let model = match (&effective_config, startup) {
        (Some(config), _) => PreparedModel {
            model: config.model.clone(),
            provider: Arc::clone(&config.provider),
        },
        (None, startup) => match prepare(model.clone()).await {
            Ok(prepared) => prepared,
            Err(error) => {
                let error = error.user_message();
                let Some(startup) = startup else {
                    return Err(OpenSessionError::Compose(error));
                };
                warnings.push(format!(
                    "failed to restore model {}: {error}; using {}",
                    model.spec(),
                    startup.model.spec()
                ));
                startup
            }
        },
    };
    params.model = model.model.clone();
    let opening_history = history
        .as_ref()
        .map(|history| Arc::clone(&history.messages))
        .unwrap_or_default();
    let providers = Arc::clone(&params.providers);
    let model_policy = Arc::clone(&params.model_policy);
    let defaults = params.defaults;
    let ephemeral_yolo = params.permissions_config.yolo;
    let mcp = if matches!(params.profile.mcp_lifecycle, McpLifecycle::Owned) {
        params.mcp_handle.clone()
    } else {
        None
    };
    let init_trigger = if matches!(params.profile.mcp_lifecycle, McpLifecycle::Shared { .. }) {
        let (trigger, init_cancel) = CancelToken::new();
        params.profile.mcp_lifecycle = McpLifecycle::Shared { init_cancel };
        Some(trigger)
    } else {
        None
    };
    let spec = params.model.spec();
    let binding = services.root_binding();
    let interactive = headless::spawn_session_runner(
        params,
        Box::new(move |_| Box::pin(async { Ok(model) })),
        history,
        binding,
        checkpoint.is_some(),
        checkpoint.clone(),
        SessionRunnerSettings {
            permissions,
            file_write_locks,
            offload,
            effective_config,
            deferred: true,
            init_trigger,
            ..Default::default()
        },
    );
    let root = interactive
        .initialized_root()
        .await
        .map_err(OpenSessionError::Compose)?;
    let template = interactive.template.recv_async().await.map_err(compose)?;
    let InteractiveHandle {
        session_id,
        manager,
        events,
        cancellation,
        answers,
        input_tx,
        control_tx,
        permissions,
        mailbox,
        tool_names,
        task,
        offload_cleanup,
        agent_tx,
        runner_start,
        history,
        offload,
        ..
    } = interactive;
    let commit = root
        .actor()
        .map_err(compose)?
        .config_snapshot()
        .ok_or_else(|| OpenSessionError::Compose(ROOT_CONFIG_UNAVAILABLE.into()))?;
    let cwd = commit.config.context.cwd().to_path_buf();
    let coordinator = PreparedSessionCoordinator::new(SessionCoordinatorParams {
        session_id: session_id.id(),
        catalog: options,
        definitions: builtin_option_definitions(
            spec,
            model_options,
            if checkpoint.is_some() {
                permissions.is_yolo()
            } else {
                ephemeral_yolo
            },
            defaults.fast,
            defaults.workflow,
            defaults.thinking.into(),
        ),
        persisted_options,
        model_policy,
        checkpoint: checkpoint.clone().map_or_else(
            ClaimingWriter::accept_all,
            |writer| -> Arc<dyn CheckpointWriter<SessionCheckpoint>> { writer },
        ),
        mailbox: mailbox.clone(),
    })?;
    let binding = coordinator
        .bind(
            Arc::clone(&permissions),
            root.clone(),
            model_prepare
                .unwrap_or_else(|| headless::default_model_preparer(Arc::clone(&providers))),
            Arc::new({
                let permissions = Arc::clone(&permissions);
                move |commit| {
                    permissions.set_cwd(commit.config.context.cwd().to_path_buf());
                    if let Some(project) = &project_config {
                        project(commit);
                    }
                }
            }),
        )
        .map_err(compose)?;
    let session = SessionHandle {
        session_id,
        manager,
        coordinator: binding,
        events,
        cancellation,
        answers,
        input_tx,
        control_tx,
        permissions,
        tool_names,
        root,
        providers,
        task: Mutex::new(Some(task)),
        offload_cleanup,
        mcp,
        checkpoint,
        lock: Mutex::new(None),
        mailbox,
        template,
        agent_tx,
        history,
        offload,
        start: Mutex::new(runner_start),
        was_existing,
        continuing_root,
        continuation_count,
        opening_history,
        opening_cost,
        cwd,
    };
    services
        .bind_root(&session)
        .await
        .map_err(OpenSessionError::Compose)?;
    session.events.set_observer(services.observe(&session));
    if session.is_persistent() && !skip_startup {
        let config = stored_agent_config(
            &commit.config,
            session.coordinator.read().options().config_values(),
        )
        .map_err(compose)?;
        let id = session.root.id();
        session
            .permissions
            .commit_startup(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(id),
                    generation: commit.generation,
                    change: StateChange::Config(config),
                }],
                histories: [(
                    id,
                    session
                        .root
                        .actor()
                        .map_err(compose)?
                        .committed_history_snapshot(),
                )]
                .into(),
                managed_histories: [id].into(),
                ..Default::default()
            })
            .map_err(compose)?;
    }
    for warning in &warnings {
        warn!(%warning, session_id = %session.session_id, "session restoration");
    }
    Ok(PreparedSession {
        session,
        coordinator,
        publication,
        warnings,
    })
}

fn compose(error: impl ToString) -> OpenSessionError {
    OpenSessionError::Compose(error.to_string())
}
