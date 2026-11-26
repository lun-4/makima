use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use makima_config::SessionDefaults;
use makima_domain::LockExt;
use makima_providers::provider::BoxFuture;
use makima_providers::{Message, ProviderRegistry};
use makima_storage::StateDir;
use makima_storage::checkpoint::{CheckpointWriter, ClaimingWriter};
use makima_storage::id::SessionRef;
use makima_storage::session_lock::LockHeartbeat;
use makima_storage::sessions::read_prefs;
use smol::Task;
use tracing::warn;

use crate::cancel::SessionCancellation;
use crate::command::SessionCommandHost;
use crate::headless::{
    self, InteractiveControlSender, InteractiveHandle, InteractiveInputSender, InteractiveParams,
    McpLifecycle, ProviderPreparation, RootBinding,
};
use crate::journal::SessionJournalSnapshot;
use crate::mcp::McpHandle;
use crate::permissions::PermissionManager;
use crate::session_coordinator::{
    SessionCheckpoint, SessionCoordinatorHandle, SessionCoordinatorParams, SessionOptionCatalog,
    builtin_option_definitions,
};
use crate::session_defaults;
use crate::session_options::MODEL_OPTION_ID;
use crate::session_recovery::{
    OpenModel, OpenSessionError, SessionSource, continuation_notice, open_session,
};
use crate::session_writer::SessionWriter;
use crate::tools::offload::OffloadCleanup;
use crate::{AgentManagerHandle, AgentRef, Envelope, TurnId};

use super::{AnswerRouter, SessionEventPump};

const MISSING_ROOT: &str = "recovered session has no root agent";

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

    fn root_binding(&mut self) -> RootBinding;

    fn bind_root<'a>(&'a mut self, session: &'a SessionHandle)
    -> BoxFuture<'a, Result<(), String>>;

    fn observe(&mut self, session: &SessionHandle) -> FrontendObserver;
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
    _lock: Option<Arc<LockHeartbeat>>,
    was_existing: bool,
    continuing_root: BTreeSet<TurnId>,
    continuation_count: usize,
    opening_history: Arc<Vec<Message>>,
    opening_cost: Option<f64>,
    cwd: PathBuf,
}

impl SessionHandle {
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

pub async fn open(request: OpenRequest<'_>) -> Result<SessionHandle, OpenSessionError> {
    let OpenRequest {
        mut params,
        durability,
        continue_turns,
        options,
        model_options,
        prepare,
        services,
    } = request;
    let mut was_existing = false;
    let mut continuing_root = BTreeSet::new();
    let mut continuation_count = 0;
    let mut persisted_options = Default::default();
    let mut checkpoint = None;
    let mut lock = None;
    let mut history = None;
    let mut opening_cost = None;
    let mut skip_startup = false;
    if let Durability::Persistent {
        storage,
        source,
        explicit_model,
        require_matching_cwd,
        skip_startup_if_children_continue,
    } = durability
    {
        let working_dir = params.initial_wd.to_string_lossy();
        let opening = open_session(
            storage,
            *source,
            OpenModel {
                startup: &params.model,
                explicit: explicit_model,
                providers: &params.providers,
                policy: &params.model_policy,
            },
            require_matching_cwd.then_some(working_dir.as_ref()),
        )?;
        was_existing = opening.resumed;
        if let Some(warning) = &opening.warning {
            let session_id = opening.session.id;
            warn!(%warning, %session_id, "session model restoration");
        }
        params.model = opening.model.clone();
        let (writer, lease, recovered) = opening
            .recover_with_continuation(storage, &params.modes, continue_turns)
            .await?;
        let session = recovered.session();
        let root = session
            .root_agent_id()
            .ok_or_else(|| OpenSessionError::Compose(MISSING_ROOT.into()))?;
        let snapshot = session.history_snapshot();
        let state = session.latest_state()?;
        let root_scope = state.scopes.get(&Some(root));
        let config = root_scope.and_then(|scope| scope.config.as_ref());
        continuation_count = state
            .scopes
            .values()
            .map(|scope| scope.admitted.len())
            .sum();
        if let Some(scope) = root_scope {
            continuing_root.extend(scope.admitted.keys().copied());
        }
        if let Some(config) = config {
            let restored = crate::plan_state::prepare_restore_for_agent(
                &state,
                root,
                &config.mode,
                &params.modes,
            )
            .map_err(|error| OpenSessionError::Compose(error.to_string()))?;
            if crate::modes::is_read_only_fallback(&restored.mode) {
                continuation_count -= continuing_root.len();
                continuing_root.clear();
            }
        }
        skip_startup = !continuing_root.is_empty()
            || (skip_startup_if_children_continue && continuation_count != 0);
        let cwd = config
            .map(|config| config.cwd.as_str())
            .or_else(|| (!require_matching_cwd).then_some(session.cwd.as_str()));
        if let Some(path) = cwd
            .map(Path::new)
            .filter(|path| require_matching_cwd || path.is_absolute())
        {
            params.initial_wd = path.to_path_buf();
        }
        params.defaults = config.map_or_else(
            || {
                session_defaults::resolve(
                    params.defaults,
                    &read_prefs(storage),
                    &session.meta,
                    snapshot.messages.is_empty(),
                    &params.model,
                )
            },
            |config| SessionDefaults {
                fast: config.fast,
                workflow: config.workflow,
                thinking: config.thinking,
            },
        );
        persisted_options = config
            .map(|config| &config.options)
            .unwrap_or(&session.meta.session_options)
            .clone();
        if !require_matching_cwd {
            persisted_options.remove(MODEL_OPTION_ID);
        }
        params.session_id = Some(SessionRef::from(session.id));
        params.root_id = Some(root);
        params.journal = Some(recovered.journal());
        opening_cost = recovered.journal().cost();
        history = Some(snapshot);
        checkpoint = Some(writer);
        lock = Some(lease);
    }
    services
        .configure(&mut params, !continuing_root.is_empty())
        .await
        .map_err(OpenSessionError::Compose)?;
    let opening_history = history
        .as_ref()
        .map(|history| Arc::clone(&history.messages))
        .unwrap_or_default();
    let cwd = params.initial_wd.clone();
    let model = params.model.spec();
    let defaults = params.defaults;
    let ephemeral_yolo = params.permissions_config.yolo;
    let providers = Arc::clone(&params.providers);
    let model_policy = Arc::clone(&params.model_policy);
    let mcp = if matches!(params.profile.mcp_lifecycle, McpLifecycle::Owned) {
        params.mcp_handle.clone()
    } else {
        None
    };
    let binding = Some(services.root_binding());
    let interactive = match &checkpoint {
        Some(writer) => headless::spawn_interactive_with_checkpoint(
            params,
            prepare,
            history,
            binding,
            Arc::clone(writer),
        ),
        None => headless::spawn_nonpersistent_with_binding(params, prepare, binding),
    };
    let setup = async {
        let root = interactive
            .initialized_root()
            .await
            .map_err(OpenSessionError::Compose)?;
        if let Some(writer) = &checkpoint {
            writer.bind_journal(interactive.manager.journal()).await?;
        }
        let definitions = builtin_option_definitions(
            model,
            model_options,
            if checkpoint.is_some() {
                interactive.permissions.is_yolo()
            } else {
                ephemeral_yolo
            },
            defaults.fast,
            defaults.workflow,
            defaults.thinking.into(),
        );
        let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id: interactive.session_id.id(),
            catalog: options,
            definitions,
            persisted_options,
            model_policy,
            checkpoint: checkpoint.clone().map_or_else(
                ClaimingWriter::accept_all,
                |writer| -> Arc<dyn CheckpointWriter<SessionCheckpoint>> { writer },
            ),
            mailbox: interactive.mailbox.clone(),
        })?;
        Ok::<_, OpenSessionError>((root, coordinator))
    }
    .await;
    let (root, coordinator) = match setup {
        Ok(setup) => setup,
        Err(error) => {
            interactive.task.cancel().await;
            if let Some(mcp) = mcp {
                mcp.shutdown().await;
            }
            return Err(error);
        }
    };
    let InteractiveHandle {
        session_id,
        manager,
        events,
        cancellation,
        answers,
        input_tx,
        control_tx,
        permissions,
        tool_names,
        task,
        offload_cleanup,
        ..
    } = interactive;
    let session = SessionHandle {
        session_id,
        manager,
        coordinator,
        events,
        cancellation,
        answers,
        input_tx,
        control_tx,
        permissions,
        tool_names,
        root,
        providers: Arc::clone(&providers),
        task: Mutex::new(Some(task)),
        offload_cleanup,
        mcp,
        checkpoint,
        _lock: lock,
        was_existing,
        continuing_root,
        continuation_count,
        opening_history,
        opening_cost,
        cwd,
    };
    let bound = async {
        session
            .coordinator
            .bind_permissions(Arc::clone(&session.permissions))?;
        let permissions = Arc::clone(&session.permissions);
        session
            .coordinator
            .bind_actor(
                session.root.clone(),
                headless::default_model_preparer(providers),
                Arc::new(move |commit| {
                    permissions.set_cwd(commit.config.context.cwd().to_path_buf())
                }),
            )
            .map_err(|error| OpenSessionError::Compose(error.to_string()))?;
        services
            .bind_root(&session)
            .await
            .map_err(OpenSessionError::Compose)?;
        if session.is_persistent() && !skip_startup {
            headless::checkpoint_startup_config(session.root(), &session.coordinator)
                .await
                .map_err(OpenSessionError::Compose)?;
        }
        session.events.set_observer(services.observe(&session));
        Ok::<_, OpenSessionError>(())
    }
    .await;
    if let Err(error) = bound {
        session.coordinator.retire();
        let task = session.task.lock_unpoisoned().take();
        if let Some(task) = task {
            task.cancel().await;
        }
        if let Some(mcp) = &session.mcp {
            mcp.shutdown().await;
        }
        return Err(error);
    }
    Ok(session)
}
