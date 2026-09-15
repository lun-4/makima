mod agent_loop;
mod command_router;
pub(crate) mod shared_queue;

#[cfg(test)]
use std::mem;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use maki_agent::permissions::PermissionManager;
use maki_agent::{
    AgentConfig, AgentEvent, AgentLimits, AgentManagerHandle, CancelMap, Envelope, HistorySnapshot,
    McpCommand, McpConfigErrors, McpHandle, McpSnapshotReader, PreparedSessionMailbox,
    SessionMailbox, SharedMessages, ToolOutputLines,
};
use maki_config::ModelPolicy;
use maki_lua::EventHandle;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, Message, Model, ModelInfo, ProviderEvent, ProviderUsage, RequestOptions,
    StreamResponse,
};
use maki_storage::id::SessionRef;
use tracing::{info, warn};

use crate::app::App;
use crate::provider_usage::{ProviderAuthGeneration, ProviderIdentity, ProviderInstanceGeneration};

use self::agent_loop::new_backend;
use self::command_router::spawn_command_router;
use self::shared_queue::actor_queue;
pub(crate) use self::shared_queue::{QueueSender, QueuedMessage};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProviderChange {
    Installed(ProviderIdentity),
    Auth(ProviderIdentity),
}

pub(crate) struct TrackedProvider {
    inner: Arc<dyn Provider>,
    instance: ProviderInstanceGeneration,
    auth_generation: AtomicU64,
    change_tx: flume::Sender<ProviderChange>,
}

impl TrackedProvider {
    fn new(
        inner: Arc<dyn Provider>,
        instance: ProviderInstanceGeneration,
        change_tx: flume::Sender<ProviderChange>,
    ) -> Self {
        Self {
            inner,
            instance,
            auth_generation: AtomicU64::new(0),
            change_tx,
        }
    }

    pub(crate) fn identity(&self) -> ProviderIdentity {
        ProviderIdentity::new(
            self.instance,
            ProviderAuthGeneration(self.auth_generation.load(Ordering::Acquire)),
        )
    }

    fn bump_auth_generation(&self) {
        let auth = self
            .auth_generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let _ = self
            .change_tx
            .send(ProviderChange::Auth(ProviderIdentity::new(
                self.instance,
                ProviderAuthGeneration(auth),
            )));
    }
}

impl Provider for TrackedProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a serde_json::Value,
        event_tx: &'a flume::Sender<ProviderEvent>,
        opts: RequestOptions,
        session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        self.inner
            .stream_message(model, messages, system, tools, event_tx, opts, session_id)
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        self.inner.list_models()
    }

    fn fetch_usage(&self) -> BoxFuture<'_, Result<Option<ProviderUsage>, AgentError>> {
        self.inner.fetch_usage()
    }

    fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async {
            self.inner.refresh_auth().await?;
            self.bump_auth_generation();
            Ok(())
        })
    }

    fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
        Box::pin(async {
            self.inner.reload_auth().await?;
            self.bump_auth_generation();
            Ok(())
        })
    }

    fn rotate_key(&self) -> BoxFuture<'_, Result<bool, AgentError>> {
        Box::pin(async {
            let rotated = self.inner.rotate_key().await?;
            if rotated {
                self.bump_auth_generation();
            }
            Ok(rotated)
        })
    }

    fn adjust_model(&self, model: &mut Model) {
        self.inner.adjust_model(model);
    }
}

pub(crate) struct ProviderSnapshot {
    pub(crate) model: Model,
    pub(crate) provider: Arc<TrackedProvider>,
}

/// Process-wide: every session owns a slot, but they all report into one
/// usage coordinator, which compares identities across slots it does not own.
/// A per-slot counter would hand two sessions the same instance generation.
static NEXT_PROVIDER_INSTANCE: AtomicU64 = AtomicU64::new(0);

fn next_provider_instance() -> ProviderInstanceGeneration {
    ProviderInstanceGeneration(NEXT_PROVIDER_INSTANCE.fetch_add(1, Ordering::AcqRel))
}

pub(crate) struct ProviderSlot {
    current: ArcSwap<ProviderSnapshot>,
    change_tx: flume::Sender<ProviderChange>,
}

impl ProviderSlot {
    /// The event loop's own slot: it owns the receiving end every other slot
    /// reports into.
    pub(crate) fn new(
        model: Model,
        provider: Arc<dyn Provider>,
    ) -> (Arc<Self>, flume::Receiver<ProviderChange>) {
        let (change_tx, change_rx) = flume::unbounded();
        (Self::with_change_tx(model, provider, change_tx), change_rx)
    }

    /// A session's slot, sharing the event loop's channel so that installs and
    /// re-auths on a session-local provider still reach the usage coordinator.
    pub(crate) fn with_change_tx(
        model: Model,
        provider: Arc<dyn Provider>,
        change_tx: flume::Sender<ProviderChange>,
    ) -> Arc<Self> {
        let tracked = Arc::new(TrackedProvider::new(
            provider,
            next_provider_instance(),
            change_tx.clone(),
        ));
        Arc::new(Self {
            current: ArcSwap::from_pointee(ProviderSnapshot {
                model,
                provider: tracked,
            }),
            change_tx,
        })
    }

    pub(crate) fn load(&self) -> Guard<Arc<ProviderSnapshot>> {
        self.current.load()
    }

    pub(crate) fn change_tx(&self) -> flume::Sender<ProviderChange> {
        self.change_tx.clone()
    }

    pub(crate) fn install(&self, model: Model, provider: Arc<dyn Provider>) -> ProviderIdentity {
        let instance = next_provider_instance();
        let tracked = Arc::new(TrackedProvider::new(
            provider,
            instance,
            self.change_tx.clone(),
        ));
        let identity = tracked.identity();
        self.current.store(Arc::new(ProviderSnapshot {
            model,
            provider: tracked,
        }));
        let _ = self.change_tx.send(ProviderChange::Installed(identity));
        identity
    }
}

impl maki_agent::ModelSource for ProviderSlot {
    fn current(&self) -> Option<(Arc<dyn Provider>, Model)> {
        let snapshot = self.load();
        Some((
            Arc::clone(&snapshot.provider) as Arc<dyn Provider>,
            snapshot.model.clone(),
        ))
    }
}

/// Inherited via CLI across every session (including respawns).
#[derive(Clone, Default)]
pub(crate) struct SystemPromptOverride {
    pub(crate) override_text: Option<String>,
    pub(crate) append_text: Option<String>,
}

pub(crate) enum AgentCommand {
    Cancel { run_id: u64 },
    CancelAll,
    CancelSubagent { tool_use_id: String },
}

/// Input channels (`cmd_tx`, `answer_tx`, `queue`) are per-agent, so an old
/// actor can never steal new input. The output channel (`agent_tx`/`agent_rx`)
/// is per-tab: `respawn` reuses it, so anyone still holding a sender (a Lua
/// restore reply, a click, an old agent winding down) can always deliver.
/// Stale events are filtered by `run_id`, not by killing the channel.
///
/// The scheduler, history, lifecycle, and retained outcomes live in the
/// actor owned here: `actor` is the handle, `task` the runner task.
pub(crate) struct PreparedAgentHandles {
    handles: Option<AgentHandles>,
    mailbox: Option<PreparedSessionMailbox>,
}

impl PreparedAgentHandles {
    #[cfg(test)]
    pub(crate) fn manager_and_root(&self) -> (AgentManagerHandle, maki_agent::AgentId) {
        let handles = self.handles.as_ref().expect("prepared handles");
        (handles.manager.clone(), handles.root_id)
    }

    pub(crate) fn mcp_reader(&self) -> McpSnapshotReader {
        self.handles
            .as_ref()
            .expect("prepared handles")
            .mcp_reader()
    }

    pub(crate) fn mailbox(&self) -> Option<SessionMailbox> {
        self.handles.as_ref().expect("prepared handles").mailbox()
    }

    pub(crate) fn cwd_slot(&self) -> Arc<ArcSwap<PathBuf>> {
        self.handles.as_ref().expect("prepared handles").cwd_slot()
    }

    pub(crate) fn activate(mut self) -> AgentHandles {
        if let Some(mailbox) = self.mailbox.take() {
            let activated = mailbox.activate();
            self.handles.as_mut().expect("prepared handles").mailbox = Some(activated);
        }
        self.handles.take().expect("prepared handles")
    }
}

impl Drop for PreparedAgentHandles {
    fn drop(&mut self) {
        if let Some(handles) = self.handles.take() {
            handles.shutdown_actor();
        }
    }
}

pub(crate) struct AgentHandles {
    pub(crate) cmd_tx: flume::Sender<AgentCommand>,
    pub(crate) agent_rx: flume::Receiver<Envelope>,
    pub(crate) agent_tx: flume::Sender<Envelope>,
    pub(crate) answer_tx: flume::Sender<String>,
    pub(crate) history: SharedMessages,
    pub(crate) btw_system: Arc<ArcSwap<String>>,
    pub(crate) mcp_handle: Option<McpHandle>,
    #[cfg(test)]
    pub(crate) mcp_config_errors: McpConfigErrors,
    pub(crate) queue: QueueSender,
    #[cfg(test)]
    pub(crate) timeouts: maki_providers::Timeouts,
    #[cfg(test)]
    model_policy: Arc<ModelPolicy>,
    #[cfg(test)]
    system_prompt: SystemPromptOverride,
    mailbox: Option<SessionMailbox>,
    cwd: Arc<ArcSwap<PathBuf>>,
    subagent_cancels: Arc<CancelMap<String>>,
    manager: AgentManagerHandle,
    root_id: maki_agent::AgentId,
}

impl AgentHandles {
    /// MCP is shared across sessions and agent respawns; the event loop starts it
    /// once and shuts it down at exit. Only the actor task lives here.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        model_slot: &Arc<ProviderSlot>,
        initial_history: Vec<Message>,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        permissions: &Arc<PermissionManager>,
        initial_cwd: PathBuf,
        session_id: Option<SessionRef>,
        timeouts: maki_providers::Timeouts,
        lua_handle: EventHandle,
        mcp_handle: Option<McpHandle>,
        mcp_config_errors: McpConfigErrors,
        model_policy: Arc<ModelPolicy>,
        system_prompt: SystemPromptOverride,
    ) -> Self {
        Self::prepare(
            model_slot,
            initial_history,
            config,
            tool_output_lines,
            permissions,
            initial_cwd,
            session_id,
            timeouts,
            lua_handle,
            mcp_handle,
            mcp_config_errors,
            model_policy,
            system_prompt,
        )
        .activate()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare(
        model_slot: &Arc<ProviderSlot>,
        initial_history: Vec<Message>,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        permissions: &Arc<PermissionManager>,
        initial_cwd: PathBuf,
        session_id: Option<SessionRef>,
        timeouts: maki_providers::Timeouts,
        lua_handle: EventHandle,
        mcp_handle: Option<McpHandle>,
        mcp_config_errors: McpConfigErrors,
        model_policy: Arc<ModelPolicy>,
        system_prompt: SystemPromptOverride,
    ) -> PreparedAgentHandles {
        spawn_agent_internal(
            flume::unbounded(),
            model_slot,
            initial_history,
            config,
            tool_output_lines,
            permissions,
            Arc::new(ArcSwap::from_pointee(initial_cwd)),
            None,
            mcp_handle,
            mcp_config_errors,
            session_id,
            timeouts,
            lua_handle,
            model_policy,
            system_prompt,
        )
    }

    pub(crate) fn mailbox(&self) -> Option<SessionMailbox> {
        self.mailbox.clone()
    }

    pub(crate) fn cwd_slot(&self) -> Arc<ArcSwap<PathBuf>> {
        Arc::clone(&self.cwd)
    }

    #[cfg(test)]
    pub(crate) fn set_mailbox(&mut self, mailbox: SessionMailbox) {
        self.mailbox = Some(mailbox);
    }

    #[cfg(test)]
    pub(crate) fn manager_and_root(&self) -> (AgentManagerHandle, maki_agent::AgentId) {
        (self.manager.clone(), self.root_id)
    }

    pub(crate) fn mcp_reader(&self) -> McpSnapshotReader {
        self.mcp_handle
            .as_ref()
            .map(McpHandle::reader)
            .unwrap_or_else(McpSnapshotReader::empty)
    }

    pub(crate) fn apply_to_app(&self, app: &mut App) {
        app.answer_tx = Some(self.answer_tx.clone());
        app.cmd_tx = Some(self.cmd_tx.clone());
        app.shared_history = Some(Arc::clone(&self.history));
        app.btw_system = Some(Arc::clone(&self.btw_system));
        app.queue.set_shared(self.queue.clone());
        let restore_tx =
            maki_agent::EventSender::new(self.agent_tx.clone(), crate::app::RESTORE_RUN_ID);
        app.restore_event_tx = Some(restore_tx.clone());
        for chat in &mut app.chats {
            chat.set_restore_channel(Some(restore_tx.clone()));
        }
    }

    /// Shuts the old actor down after the app and queue are repointed, so
    /// its close cannot poison the replacement. Everything the old agent
    /// still owns drains through the retained per-tab output channel.
    fn shutdown_actor(&self) {
        let manager = self.manager.clone();
        smol::spawn(async move {
            let _ = manager.shutdown(Duration::from_secs(3)).await;
        })
        .detach();
        self.subagent_cancels.cancel_all();
    }

    pub(crate) fn send_mcp(&self, cmd: McpCommand) {
        if let Some(ref h) = self.mcp_handle {
            h.send(cmd);
        }
    }

    pub(crate) fn claim_mailbox_wake(&self) -> Vec<Message> {
        self.mailbox
            .as_ref()
            .map(SessionMailbox::claim_wake)
            .unwrap_or_default()
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn respawn(
        &mut self,
        history: Vec<Message>,
        model_slot: &Arc<ProviderSlot>,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        permissions: &Arc<PermissionManager>,
        app: &mut App,
        lua_handle: EventHandle,
    ) {
        // The output channel survives the respawn, so this bump is the only
        // thing that makes the old actor's in-flight envelopes stale. It lives
        // here so no caller can respawn without it.
        app.run_id += 1;
        let slot = model_slot.load();
        if let Err(e) = smol::block_on(slot.provider.reload_auth()) {
            warn!(error = %e, "failed to reload auth, continuing with existing credentials");
        }
        let new = spawn_agent_internal(
            (self.agent_tx.clone(), self.agent_rx.clone()),
            model_slot,
            history,
            config,
            tool_output_lines,
            permissions,
            Arc::clone(&self.cwd),
            self.mailbox.clone(),
            self.mcp_handle.clone(),
            self.mcp_config_errors.clone(),
            Some(SessionRef::from(app.state.session.id)),
            self.timeouts,
            lua_handle,
            Arc::clone(&self.model_policy),
            self.system_prompt.clone(),
        )
        .activate();
        let old = mem::replace(self, new);
        // Repoint the app at the new queue before dropping `old`, otherwise the app keeps
        // the last old `QueueSender` alive and the old actor parks on its notify forever.
        self.apply_to_app(app);
        app.flush_restored_queue();
        // Shut the old actor down after the app and queue are repointed, so
        // its close cannot poison the replacement. Everything the old agent
        // still owns drains through the retained per-tab output channel.
        old.shutdown_actor();
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.manager.runner_finished(self.root_id).unwrap_or(true)
    }

    pub(crate) fn shutdown(self) -> smol::Task<()> {
        self.subagent_cancels.cancel_all();
        smol::spawn(async move {
            let _ = self.manager.shutdown(Duration::from_secs(3)).await;
        })
    }
}

/// Wait for every agent task under one shared timeout, not one per task.
pub(crate) fn join_all(tasks: Vec<smol::Task<()>>, timeout: Duration) {
    info!(
        count = tasks.len(),
        "waiting for agents to finish (timeout {timeout:?})"
    );
    smol::block_on(async {
        let finished = futures_lite::future::or(
            async {
                for task in tasks {
                    task.await;
                }
                true
            },
            async {
                smol::Timer::after(timeout).await;
                false
            },
        )
        .await;
        if !finished {
            warn!("agents did not finish within {timeout:?}, forcing shutdown");
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn spawn_agent_internal(
    (agent_tx, agent_rx): (flume::Sender<Envelope>, flume::Receiver<Envelope>),
    model_slot: &Arc<ProviderSlot>,
    initial_history: Vec<Message>,
    config: AgentConfig,
    tool_output_lines: ToolOutputLines,
    permissions: &Arc<PermissionManager>,
    cwd: Arc<ArcSwap<PathBuf>>,
    mailbox: Option<SessionMailbox>,
    mcp_handle: Option<McpHandle>,
    mcp_config_errors: McpConfigErrors,
    session_id: Option<SessionRef>,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
) -> PreparedAgentHandles {
    #[cfg(not(test))]
    let _ = mcp_config_errors;
    let (cmd_tx, cmd_rx) = flume::unbounded::<AgentCommand>();
    let (answer_tx, answer_rx) = flume::unbounded::<String>();
    // Seeded empty because `AgentActorHandle::spawn` publishes the real
    // snapshot synchronously, before any handle escapes.
    let shared_history: SharedMessages =
        Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()));
    let btw_system: Arc<ArcSwap<String>> = Arc::new(ArcSwap::from_pointee(String::new()));
    let subagent_cancels: Arc<CancelMap<String>> = Arc::new(CancelMap::new());
    let prepared_mailbox = mailbox
        .is_none()
        .then(|| {
            session_id
                .as_ref()
                .map(|session_id| SessionMailbox::prepare(session_id.id()))
        })
        .flatten();
    let mailbox = mailbox.or_else(|| {
        prepared_mailbox
            .as_ref()
            .map(PreparedSessionMailbox::mailbox)
    });

    let (init_trigger, init_cancel) = maki_agent::CancelToken::new();

    let (drain_tx, drain_rx) = flume::unbounded::<u64>();
    let run_id = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let limits = AgentLimits {
        max_concurrent_agent_turns: config.max_concurrent_agent_turns,
        max_agent_depth: config.max_agent_depth,
        max_children_per_agent: config.max_children_per_agent,
        max_live_agents: config.max_live_agents,
    };
    let manager = AgentManagerHandle::new(limits).expect("validated agent limits");
    let root = manager
        .create_root_with(
            initial_history.clone(),
            Some(Arc::clone(&shared_history)),
            |agent_id| {
                Ok::<Box<dyn maki_agent::ActorBackend>, String>(Box::new(new_backend(
                    agent_id,
                    Arc::clone(model_slot),
                    Arc::clone(&cwd),
                    config,
                    tool_output_lines,
                    Arc::clone(&btw_system),
                    mcp_handle.clone(),
                    &initial_history,
                    Arc::clone(permissions),
                    agent_tx.clone(),
                    answer_rx,
                    session_id,
                    mailbox.clone(),
                    timeouts,
                    lua_handle,
                    Arc::clone(&subagent_cancels),
                    Arc::clone(&model_policy),
                    system_prompt.clone(),
                    Arc::new(maki_agent::tools::FileWriteLocks::new()),
                    init_cancel,
                    drain_tx,
                    Arc::clone(&run_id),
                )))
            },
        )
        .expect("root agent factory");
    let root_id = root.id();
    let actor = Arc::new(root.actor().expect("committed root actor"));
    let queue_tx = actor_queue(Arc::clone(&actor), Arc::clone(&run_id));

    spawn_command_router(
        cmd_rx,
        Arc::clone(&actor),
        manager.clone(),
        root_id,
        Arc::clone(&subagent_cancels),
        init_trigger,
    );

    // Drain driver: the actor's runner drains the queue; this task watches
    // each completed item and publishes `QueueDrained` once, under the actor
    // queue lock, correlated with the finishing item's run id.
    let drain_actor = Arc::clone(&actor);
    let drain_agent_tx = agent_tx.clone();
    smol::spawn(async move {
        while let Ok(run_id) = drain_rx.recv_async().await {
            drain_actor.publish_if_empty(|| {
                maki_agent::EventSender::new(drain_agent_tx.clone(), run_id)
                    .try_send(AgentEvent::QueueDrained);
            });
        }
    })
    .detach();

    PreparedAgentHandles {
        handles: Some(AgentHandles {
            cmd_tx,
            agent_rx,
            agent_tx,
            answer_tx,
            history: shared_history,
            btw_system,
            mcp_handle,
            #[cfg(test)]
            mcp_config_errors,
            queue: queue_tx,
            #[cfg(test)]
            timeouts,
            #[cfg(test)]
            model_policy,
            #[cfg(test)]
            system_prompt,
            mailbox,
            cwd,
            subagent_cancels,
            manager,
            root_id,
        }),
        mailbox: prepared_mailbox,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Instant;

    use maki_agent::AgentEvent;
    use maki_config::PermissionsConfig;
    use maki_providers::provider::BoxFuture;
    use maki_providers::{AgentError, ModelInfo, ProviderEvent, RequestOptions, StreamResponse};

    use super::*;

    const LONG_TIMEOUT: Duration = Duration::from_secs(60);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);
    const PROBE_TEXT: &str = "probe-through-old-sender";
    const RESTORED_TEXT: &str = "restored-queued-message";
    const RESUMED_HISTORY_TEXT: &str = "resumed-conversation";

    struct StubProvider;

    impl Provider for StubProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }
    }

    struct AuthProvider {
        reload_ok: bool,
        rotate: bool,
    }

    impl Provider for AuthProvider {
        fn stream_message<'a>(
            &'a self,
            _model: &'a Model,
            _messages: &'a [Message],
            _system: &'a str,
            _tools: &'a serde_json::Value,
            _event_tx: &'a flume::Sender<ProviderEvent>,
            _opts: RequestOptions,
            _session_id: Option<&'a SessionRef>,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
        }

        fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
            Box::pin(async { Ok(Vec::new()) })
        }

        fn refresh_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
            Box::pin(async { Ok(()) })
        }

        fn reload_auth(&self) -> BoxFuture<'_, Result<(), AgentError>> {
            Box::pin(async move {
                if self.reload_ok {
                    Ok(())
                } else {
                    Err(AgentError::Config {
                        message: "reload failed".into(),
                    })
                }
            })
        }

        fn rotate_key(&self) -> BoxFuture<'_, Result<bool, AgentError>> {
            Box::pin(async move { Ok(self.rotate) })
        }
    }

    fn auth_slot(
        reload_ok: bool,
        rotate: bool,
    ) -> (Arc<ProviderSlot>, flume::Receiver<ProviderChange>) {
        ProviderSlot::new(
            crate::components::test_model(),
            Arc::new(AuthProvider { reload_ok, rotate }),
        )
    }

    fn stub_spawn() -> (AgentHandles, Arc<ProviderSlot>, Arc<PermissionManager>) {
        stub_spawn_with(Vec::new())
    }

    fn stub_spawn_with(
        initial_history: Vec<Message>,
    ) -> (AgentHandles, Arc<ProviderSlot>, Arc<PermissionManager>) {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let permissions = Arc::new(PermissionManager::new(
            PermissionsConfig::default(),
            PathBuf::from("/tmp"),
            Arc::default(),
        ));
        let handles = AgentHandles::spawn(
            &model_slot,
            initial_history,
            AgentConfig::default(),
            ToolOutputLines::default(),
            &permissions,
            PathBuf::from("/tmp"),
            None,
            maki_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            None,
            McpConfigErrors::new(PathBuf::new()),
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
        );
        (handles, model_slot, permissions)
    }

    /// Registers a coordinator so `SessionMailbox::notify` -- which resolves
    /// through one -- has something to resolve.
    fn register_coordinator(
        session_id: maki_storage::id::MakiId,
        mailbox: SessionMailbox,
    ) -> maki_agent::session_coordinator::SessionCoordinatorHandle {
        use maki_agent::session_coordinator::{
            DirectoryAdoptionFuture, ModelAdoptionFuture, SessionCheckpoint,
            SessionCoordinatorHandle, SessionCoordinatorParams, builtin_option_definitions,
        };
        use maki_storage::checkpoint::{CheckpointAck, CheckpointFuture, CheckpointRequest};

        SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id,
            catalog: Default::default(),
            definitions: builtin_option_definitions(
                "test/model",
                [Arc::from("test/model")],
                false,
                false,
                false,
                maki_agent::ThinkingConfig::Off,
            ),
            persisted_options: Default::default(),
            history: Vec::new(),
            model: Arc::from("test/model"),
            cwd: PathBuf::from("/tmp"),
            model_policy: Arc::default(),
            model_adopter: Arc::new(|_: Model| Box::pin(async { Ok(()) }) as ModelAdoptionFuture),
            directory_adopter: Arc::new(|path: PathBuf| {
                Box::pin(async move { Ok(path) }) as DirectoryAdoptionFuture
            }),
            checkpoint: Arc::new(|request: CheckpointRequest<SessionCheckpoint>| {
                Box::pin(async move {
                    Ok(CheckpointAck {
                        session_id: request.session_id,
                        version: request.version,
                    })
                }) as CheckpointFuture
            }),
            mailbox,
        })
        .expect("coordinator registration")
    }

    fn respawn(
        handles: &mut AgentHandles,
        model_slot: &Arc<ProviderSlot>,
        permissions: &Arc<PermissionManager>,
        app: &mut App,
    ) {
        handles.respawn(
            Vec::new(),
            model_slot,
            AgentConfig::default(),
            ToolOutputLines::default(),
            permissions,
            app,
            EventHandle::disconnected_for_test(),
        );
    }

    /// Instance generations come from a process-wide counter (sessions each
    /// own a slot and share one usage coordinator), so this asserts the
    /// invariants rather than absolute numbers.
    #[test]
    fn provider_install_increments_instance_and_resets_auth_generation() {
        let (slot, change_rx) = auth_slot(true, false);
        let before = slot.load().provider.identity();
        assert_eq!(before.auth, ProviderAuthGeneration(0));

        let provider = Arc::clone(&slot.load().provider);
        smol::block_on(provider.reload_auth()).expect("reload succeeds");
        assert_eq!(provider.identity().auth, ProviderAuthGeneration(1));
        change_rx.recv().expect("reload notification");

        let identity = slot.install(
            crate::components::test_model(),
            Arc::new(AuthProvider {
                reload_ok: true,
                rotate: false,
            }),
        );

        assert!(
            identity.instance.0 > before.instance.0,
            "an install must take a fresh instance generation"
        );
        assert_eq!(
            identity.auth,
            ProviderAuthGeneration(0),
            "a new provider starts its auth generation over"
        );
        assert_eq!(slot.load().provider.identity(), identity);
        assert_eq!(
            change_rx.recv().expect("install notification"),
            ProviderChange::Installed(identity)
        );
    }

    #[test]
    fn successful_auth_operations_bump_before_notification() {
        let (slot, change_rx) = auth_slot(true, true);
        let provider = Arc::clone(&slot.load().provider);

        smol::block_on(provider.reload_auth()).expect("reload succeeds");
        assert_eq!(
            change_rx.recv().expect("reload notification"),
            ProviderChange::Auth(provider.identity())
        );
        assert_eq!(provider.identity().auth, ProviderAuthGeneration(1));

        smol::block_on(provider.refresh_auth()).expect("refresh succeeds");
        assert_eq!(
            change_rx.recv().expect("refresh notification"),
            ProviderChange::Auth(provider.identity())
        );
        assert_eq!(provider.identity().auth, ProviderAuthGeneration(2));

        assert!(smol::block_on(provider.rotate_key()).expect("rotation succeeds"));
        assert_eq!(
            change_rx.recv().expect("rotation notification"),
            ProviderChange::Auth(provider.identity())
        );
        assert_eq!(provider.identity().auth, ProviderAuthGeneration(3));
    }

    #[test]
    fn failed_reload_and_false_rotation_do_not_bump_auth_generation() {
        let (failed_slot, failed_rx) = auth_slot(false, false);
        let failed = Arc::clone(&failed_slot.load().provider);
        assert!(smol::block_on(failed.reload_auth()).is_err());
        assert_eq!(failed.identity().auth, ProviderAuthGeneration(0));
        assert!(failed_rx.is_empty());

        let (slot, change_rx) = auth_slot(true, false);
        let provider = Arc::clone(&slot.load().provider);
        assert!(!smol::block_on(provider.rotate_key()).expect("rotation succeeds"));
        assert_eq!(provider.identity().auth, ProviderAuthGeneration(0));
        assert!(change_rx.is_empty());
    }

    /// Senders captured before any respawn (Lua restore replies, clicks) must
    /// still reach the live receiver, and restored queue items must land in
    /// the freshly wired queue, not the one that just died.
    #[test]
    fn respawn_twice_keeps_channel_and_delivers_restored_queue() {
        let (mut handles, model_slot, permissions) = stub_spawn();
        let pre_gen1_sender =
            maki_agent::EventSender::new(handles.agent_tx.clone(), crate::app::RESTORE_RUN_ID);

        let mut app = crate::app::tests::test_app();
        let run_id_before = app.run_id;
        respawn(&mut handles, &model_slot, &permissions, &mut app);
        assert_eq!(app.run_id, run_id_before + 1);

        app.state.session_mut().meta.queued_messages = vec![RESTORED_TEXT.into()];
        respawn(&mut handles, &model_slot, &permissions, &mut app);
        assert_eq!(
            app.run_id,
            run_id_before + 2,
            "each respawn must bump run_id exactly once"
        );

        // The restored item is drained from the new queue by the live actor,
        // which may consume it before this thread reads the shared queue, so
        // asserting `text_messages()` here would race. The channel is the
        // deterministic witness: `QueueItemConsumed` only leaves the new queue.
        pre_gen1_sender
            .send(AgentEvent::TextDelta {
                text: PROBE_TEXT.into(),
            })
            .expect("pre-generation-1 sender must still deliver after two respawns");

        let mut probe_seen = false;
        let mut restored_seen = 0;
        while !probe_seen || restored_seen < 1 {
            let envelope = handles
                .agent_rx
                .recv_timeout(LONG_TIMEOUT)
                .expect("probe or restored queue item never reached the tab channel");
            match envelope.event {
                AgentEvent::TextDelta { ref text } if text == PROBE_TEXT => probe_seen = true,
                AgentEvent::QueueItemConsumed { ref text, .. } if text == RESTORED_TEXT => {
                    assert_eq!(envelope.run_id, app.run_id);
                    restored_seen += 1;
                }
                _ => {}
            }
        }
        assert_eq!(
            restored_seen, 1,
            "the restored item is consumed exactly once, from the new queue"
        );
    }

    /// If the seeded empty snapshot ever outlived `spawn`, the next checkpoint
    /// would adopt it and wipe a resumed conversation from disk.
    #[test]
    fn spawn_publishes_the_resumed_history_before_the_handles_escape() {
        let (handles, _model_slot, _permissions) =
            stub_spawn_with(vec![Message::user(RESUMED_HISTORY_TEXT.into())]);
        let snapshot = handles.history.load();
        assert_eq!(
            snapshot.messages.len(),
            1,
            "the seeded empty snapshot must be replaced synchronously"
        );
        assert_eq!(snapshot.messages[0].user_text(), Some(RESUMED_HISTORY_TEXT));
    }

    #[test]
    fn respawn_publishes_the_new_history_into_the_app_mirror() {
        let (mut handles, model_slot, permissions) = stub_spawn();
        let mut app = crate::app::tests::test_app();
        handles.respawn(
            vec![Message::user(RESUMED_HISTORY_TEXT.into())],
            &model_slot,
            AgentConfig::default(),
            ToolOutputLines::default(),
            &permissions,
            &mut app,
            EventHandle::disconnected_for_test(),
        );

        let mirror = app
            .shared_history
            .as_ref()
            .expect("respawn wires the live mirror into the app");
        let snapshot = mirror.load();
        assert_eq!(
            snapshot.messages.len(),
            1,
            "a checkpoint right after respawn must not see the seeded empty snapshot"
        );
        assert_eq!(snapshot.messages[0].user_text(), Some(RESUMED_HISTORY_TEXT));
    }

    /// A respawn must keep the session's mailbox. The coordinator handed the
    /// original out to `SessionMailbox::notify` at registration and has no way
    /// to learn about a replacement, so minting a fresh one on respawn leaves
    /// every later notification in an instance nobody polls.
    #[test]
    fn respawn_keeps_the_mailbox_the_coordinator_hands_out() {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let permissions = Arc::new(PermissionManager::new(
            PermissionsConfig::default(),
            PathBuf::from("/tmp"),
            Arc::default(),
        ));
        let mut app = crate::app::tests::test_app();
        let session_id = app.state.session.id;
        let mut handles = AgentHandles::spawn(
            &model_slot,
            Vec::new(),
            AgentConfig::default(),
            ToolOutputLines::default(),
            &permissions,
            PathBuf::from("/tmp"),
            Some(SessionRef::from(session_id)),
            maki_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            None,
            McpConfigErrors::new(PathBuf::new()),
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
        );
        let coordinator = register_coordinator(
            session_id,
            handles
                .mailbox()
                .expect("a session-backed agent has a mailbox"),
        );

        respawn(&mut handles, &model_slot, &permissions, &mut app);

        SessionMailbox::notify(session_id, "wake up".into(), true)
            .expect("notify resolves through the registered coordinator");
        let claimed = handles.claim_mailbox_wake();
        assert_eq!(
            claimed.len(),
            1,
            "a notification after respawn must reach the live agent's mailbox"
        );
        assert!(
            format!("{:?}", claimed[0]).contains("wake up"),
            "the notification text must survive: {:?}",
            claimed[0]
        );
        let _ = smol::block_on(coordinator.close());
    }

    /// `/new` rotates the tab onto a fresh session id, and the coordinator
    /// registered around it hands out the mailbox. Keeping the old one would
    /// leave the new session's notifications going to the retired session's
    /// instance.
    #[test]
    fn setting_a_mailbox_repoints_the_agent_at_the_new_session() {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let permissions = Arc::new(PermissionManager::new(
            PermissionsConfig::default(),
            PathBuf::from("/tmp"),
            Arc::default(),
        ));
        let first = maki_storage::id::MakiId::generate();
        let mut handles = AgentHandles::spawn(
            &model_slot,
            Vec::new(),
            AgentConfig::default(),
            ToolOutputLines::default(),
            &permissions,
            PathBuf::from("/tmp"),
            Some(SessionRef::from(first)),
            maki_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            None,
            McpConfigErrors::new(PathBuf::new()),
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
        );
        assert_eq!(handles.mailbox().map(|m| m.session_id()), Some(first));

        let second = maki_storage::id::MakiId::generate();
        handles.set_mailbox(SessionMailbox::new(second));
        assert_eq!(
            handles.mailbox().map(|m| m.session_id()),
            Some(second),
            "the agent must poll the mailbox the new session's coordinator owns"
        );
    }

    /// A session owns its provider slot, but the usage coordinator listens on
    /// exactly one channel. A re-auth on a session-local provider has to reach
    /// it, or the status line and usage panel go stale with no way to notice.
    #[test]
    fn session_slot_reports_into_the_shared_change_channel() {
        let (loop_slot, change_rx) = auth_slot(true, false);
        let session_slot = ProviderSlot::with_change_tx(
            crate::components::test_model(),
            Arc::new(AuthProvider {
                reload_ok: true,
                rotate: false,
            }),
            loop_slot.change_tx(),
        );
        assert!(
            change_rx.is_empty(),
            "creating a session slot is not itself a provider change"
        );
        assert_ne!(
            session_slot.load().provider.identity().instance,
            loop_slot.load().provider.identity().instance,
            "instance generations must be unique across slots, or the usage \
             coordinator cannot tell two sessions' providers apart"
        );

        let provider = Arc::clone(&session_slot.load().provider);
        smol::block_on(provider.reload_auth()).expect("reload succeeds");
        assert_eq!(
            change_rx
                .recv_timeout(SHORT_TIMEOUT)
                .expect("the re-auth reaches the event loop"),
            ProviderChange::Auth(provider.identity())
        );

        let installed = session_slot.install(
            crate::components::test_model(),
            Arc::new(AuthProvider {
                reload_ok: true,
                rotate: false,
            }),
        );
        assert_eq!(
            change_rx
                .recv_timeout(SHORT_TIMEOUT)
                .expect("the install reaches the event loop"),
            ProviderChange::Installed(installed)
        );
    }

    #[test]
    fn join_all_returns_when_all_tasks_complete() {
        join_all(Vec::new(), LONG_TIMEOUT);
        join_all(
            (0..3).map(|_| smol::spawn(async {})).collect(),
            LONG_TIMEOUT,
        );
    }

    #[test]
    fn join_all_stuck_task_returns_after_shared_timeout() {
        let start = Instant::now();
        join_all(
            vec![
                smol::spawn(async {}),
                smol::spawn(futures_lite::future::pending::<()>()),
            ],
            SHORT_TIMEOUT,
        );
        assert!(start.elapsed() >= SHORT_TIMEOUT);
    }
}
