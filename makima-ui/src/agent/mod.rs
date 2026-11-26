mod command_router;
pub(crate) mod pending;
#[cfg(test)]
mod runtime_tests;
pub(crate) mod shared_queue;

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use async_lock::Mutex;
use makima_agent::actor::{ActorEvent, AgentContextSnapshot, EffectiveAgentConfig, PreparedModel};
use makima_agent::headless::{FrontendProfile, HeadlessBackend, HeadlessBackendOptions};
use makima_agent::journal::SessionJournal;
use makima_agent::manager::ResidentAgent;
use makima_agent::mcp::McpSession;
use makima_agent::permissions::PermissionManager;
use makima_agent::session_coordinator::ModelPreparer;
use makima_agent::session_recovery::RecoveredSession;
use makima_agent::tools::offload::OffloadStore;
use makima_agent::tools::{
    FileReadTracker, FileWriteLocks, QuestionMode, ToolAudience, ToolContext, ToolRegistry,
};
use makima_agent::{
    AgentConfig, AgentId, AgentLimits, AgentManagerHandle, AgentParams, CancelMap, CancelToken,
    Envelope, HistorySnapshot, McpCommand, McpHandle, PreparedSessionMailbox, SessionMailbox,
    SharedMessages, ToolOutputLines,
};
use makima_config::ModelPolicy;
use makima_lua::EventHandle;
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    AgentError, ConversationEnd, Invocation, Message, Model, ProviderEvent, ProviderRegistry,
    ProviderUsage, RequestDelivery, RequestOptions, StreamResponse,
};
use makima_storage::id::SessionRef;
use tracing::{info, warn};

use crate::app::App;
use crate::provider_usage::{ProviderAuthGeneration, ProviderIdentity, ProviderInstanceGeneration};

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
    fn wrap(provider: Arc<dyn Provider>, change_tx: &flume::Sender<ProviderChange>) -> Arc<Self> {
        let erased: Arc<dyn Any + Send + Sync> = provider.clone();
        Arc::downcast(erased).unwrap_or_else(|_| {
            Arc::new(Self {
                inner: provider,
                instance: next_provider_instance(),
                auth_generation: AtomicU64::new(0),
                change_tx: change_tx.clone(),
            })
        })
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
        invocation: &'a Invocation,
        delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        self.inner.stream_message(
            model, messages, system, tools, event_tx, opts, session_id, invocation, delivery,
        )
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

    fn key_count(&self) -> usize {
        self.inner.key_count()
    }

    fn rotate_key(&self) -> BoxFuture<'_, bool> {
        self.inner.rotate_key()
    }

    fn end_conversation(
        &self,
        conversation_id: &str,
        reason: ConversationEnd,
    ) -> BoxFuture<'_, ()> {
        self.inner.end_conversation(conversation_id, reason)
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
        let tracked = TrackedProvider::wrap(provider, &change_tx);
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

    pub(crate) fn track_model(&self, mut prepared: PreparedModel) -> PreparedModel {
        prepared.provider = TrackedProvider::wrap(prepared.provider, &self.change_tx);
        prepared
    }

    pub(crate) fn track_preparation(self: &Arc<Self>, prepare: ModelPreparer) -> ModelPreparer {
        let slot = Arc::clone(self);
        Arc::new(move |spec| {
            let prepare = Arc::clone(&prepare);
            let slot = Arc::clone(&slot);
            Box::pin(async move { Ok(slot.track_model(prepare(spec).await?)) })
        })
    }

    pub(crate) fn project(&self, model: Model, provider: Arc<dyn Provider>) {
        let current = self.load();
        let tracked = Arc::clone(&current.provider) as Arc<dyn Provider>;
        if Arc::ptr_eq(&tracked, &provider) || Arc::ptr_eq(&current.provider.inner, &provider) {
            self.current.store(Arc::new(ProviderSnapshot {
                model,
                provider: Arc::clone(&current.provider),
            }));
        } else {
            drop(current);
            self.install(model, provider);
        }
    }

    pub(crate) fn install(&self, model: Model, provider: Arc<dyn Provider>) -> ProviderIdentity {
        let tracked = TrackedProvider::wrap(provider, &self.change_tx);
        let identity = tracked.identity();
        self.current.store(Arc::new(ProviderSnapshot {
            model,
            provider: tracked,
        }));
        let _ = self.change_tx.send(ProviderChange::Installed(identity));
        identity
    }
}

/// Inherited via CLI across every session.
#[derive(Clone, Default)]
pub(crate) struct SystemPromptOverride {
    pub(crate) override_text: Option<String>,
    pub(crate) append_text: Option<String>,
}

pub(crate) enum AgentCommand {
    CancelAll,
    CancelSubagent { tool_use_id: String },
}

type AgentHandlesBuilder = Box<dyn FnOnce(RecoveredSession) -> Result<AgentHandles, String> + Send>;

/// Stale events on the output channel (`agent_tx`/`agent_rx`) are filtered
/// by `run_id`, not by killing the channel.
///
/// The scheduler, history, lifecycle, and retained outcomes live in the
/// actor owned here: `actor` is the handle, `task` the runner task.
pub(crate) struct PreparedAgentHandles {
    handles: Option<AgentHandles>,
    mailbox: Option<PreparedSessionMailbox>,
    build: Option<AgentHandlesBuilder>,
}

impl PreparedAgentHandles {
    #[cfg(test)]
    pub(crate) fn is_initialized(&self) -> bool {
        self.handles.is_some()
    }

    pub(crate) fn initialized(&self) -> &AgentHandles {
        self.handles.as_ref().expect("initialized agent handles")
    }

    pub(crate) fn mailbox(&self) -> Option<SessionMailbox> {
        self.mailbox
            .as_ref()
            .map(PreparedSessionMailbox::mailbox)
            .or_else(|| self.handles.as_ref().and_then(AgentHandles::mailbox))
    }

    #[cfg(test)]
    pub(crate) fn fail_initialization_after(
        &mut self,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
        message: &'static str,
    ) {
        self.build = Some(Box::new(move |_| {
            entered.send(()).map_err(|error| error.to_string())?;
            release.recv().map_err(|error| error.to_string())?;
            Err(message.into())
        }));
    }

    #[cfg(test)]
    pub(crate) fn gate_initialization(
        &mut self,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    ) {
        let build = self.build.take().expect("deferred agent builder");
        self.build = Some(Box::new(move |recovered| {
            let handles = build(recovered)?;
            entered.send(()).map_err(|error| error.to_string())?;
            release.recv().map_err(|error| error.to_string())?;
            Ok(handles)
        }));
    }

    pub(crate) fn initialize(&mut self, recovered: RecoveredSession) -> Result<(), String> {
        let build = self
            .build
            .take()
            .ok_or_else(|| "agent runtime already initialized".to_owned())?;
        self.handles = Some(build(recovered)?);
        Ok(())
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
    pub(crate) actor_rx: flume::Receiver<ActorEvent>,
    pub(crate) agent_tx: flume::Sender<Envelope>,
    pub(crate) answer_tx: flume::Sender<String>,
    pub(crate) history: SharedMessages,
    pub(crate) mcp_handle: Option<McpHandle>,
    pub(crate) queue: QueueSender,
    mailbox: Option<SessionMailbox>,
    cwd: Arc<ArcSwap<PathBuf>>,
    pub(crate) offload: Option<Arc<OffloadStore>>,
    pub(crate) orchestration_template: ToolContext,
    manager: AgentManagerHandle,
    root_id: makima_agent::AgentId,
    runner_start: Option<flume::Sender<()>>,
}

impl AgentHandles {
    #[cfg(test)]
    pub(crate) fn runner_start_is_pending(&self) -> bool {
        self.runner_start.is_some()
    }

    pub(crate) fn release_runner(&mut self) {
        if let Some(start) = self.runner_start.take() {
            let _ = start.send(());
        }
    }

    /// MCP is shared across sessions; the event loop starts it once and shuts
    /// it down at exit. Only the actor task lives here.
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
        timeouts: makima_providers::Timeouts,
        lua_handle: EventHandle,
        mcp_handle: Option<McpHandle>,
        model_policy: Arc<ModelPolicy>,
        providers: Arc<ProviderRegistry>,
        system_prompt: SystemPromptOverride,
    ) -> Self {
        let selected = model_slot.load();
        let initial_config = makima_agent::EffectiveAgentConfig::new(
            makima_agent::RunSettings {
                provider: Arc::clone(&selected.provider) as Arc<dyn Provider>,
                model: selected.model.clone(),
                fast: false,
                workflow: false,
                thinking: Default::default(),
            },
            makima_agent::AgentMode::Build,
        );
        drop(selected);
        let mut handles = Self::prepare(
            initial_config,
            HistorySnapshot::new(initial_history),
            config,
            tool_output_lines,
            permissions,
            initial_cwd,
            session_id,
            None,
            Arc::default(),
            None,
            timeouts,
            lua_handle,
            mcp_handle,
            model_policy,
            providers,
            system_prompt,
        )
        .activate();
        handles.release_runner();
        handles
    }

    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare(
        initial_config: EffectiveAgentConfig,
        initial_history: HistorySnapshot,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        permissions: &Arc<PermissionManager>,
        initial_cwd: PathBuf,
        session_id: Option<SessionRef>,
        root_id: Option<AgentId>,
        journal: Arc<SessionJournal>,
        offload: Option<Arc<OffloadStore>>,
        timeouts: makima_providers::Timeouts,
        lua_handle: EventHandle,
        mcp_handle: Option<McpHandle>,
        model_policy: Arc<ModelPolicy>,
        providers: Arc<ProviderRegistry>,
        system_prompt: SystemPromptOverride,
    ) -> PreparedAgentHandles {
        spawn_agent_internal(
            initial_config,
            initial_history,
            config,
            tool_output_lines,
            permissions,
            Arc::new(ArcSwap::from_pointee(initial_cwd)),
            mcp_handle,
            session_id,
            root_id,
            journal,
            offload,
            timeouts,
            lua_handle,
            model_policy,
            providers,
            system_prompt,
            None,
            None,
            Arc::new(FileWriteLocks::new()),
        )
        .expect("root agent factory")
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_recovered(
        initial_config: EffectiveAgentConfig,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        permissions: &Arc<PermissionManager>,
        initial_cwd: PathBuf,
        session_id: SessionRef,
        offload: Option<Arc<OffloadStore>>,
        timeouts: makima_providers::Timeouts,
        lua_handle: EventHandle,
        mcp_handle: Option<McpHandle>,
        model_policy: Arc<ModelPolicy>,
        providers: Arc<ProviderRegistry>,
        system_prompt: SystemPromptOverride,
        file_write_locks: Arc<FileWriteLocks>,
    ) -> PreparedAgentHandles {
        let prepared_mailbox = SessionMailbox::prepare(session_id.id());
        let mailbox = prepared_mailbox.mailbox();
        let permissions = Arc::clone(permissions);
        let cwd = Arc::new(ArcSwap::from_pointee(initial_cwd));
        PreparedAgentHandles {
            handles: None,
            mailbox: Some(prepared_mailbox),
            build: Some(Box::new(move |recovered| {
                let (session, journal) = recovered.into_parts();
                let root_id = session
                    .root_agent_id()
                    .ok_or_else(|| "recovered root identity is missing".to_owned())?;
                let snapshot = journal
                    .checkpoint_snapshot()
                    .map_err(|error| error.to_string())?;
                let view = session.latest_state().map_err(|error| error.to_string())?;
                let manager = AgentManagerHandle::restore_graph(
                    agent_limits(&config),
                    Arc::clone(&journal),
                    view,
                    snapshot.histories,
                )
                .map_err(|error| error.to_string())?;
                if makima_agent::modes::is_read_only_fallback(&initial_config.mode) {
                    manager
                        .suppress_root_continuations()
                        .map_err(|error| error.to_string())?;
                }
                let mut prepared = spawn_agent_internal(
                    initial_config,
                    session.history_snapshot(),
                    config,
                    tool_output_lines,
                    &permissions,
                    cwd,
                    mcp_handle,
                    Some(session_id),
                    Some(root_id),
                    journal,
                    offload,
                    timeouts,
                    lua_handle,
                    model_policy,
                    providers,
                    system_prompt,
                    Some(manager),
                    Some(mailbox),
                    file_write_locks,
                )?;
                Ok(prepared.handles.take().expect("initialized agent handles"))
            })),
        }
    }

    pub(crate) fn mailbox(&self) -> Option<SessionMailbox> {
        self.mailbox.clone()
    }

    pub(crate) fn cwd_slot(&self) -> Arc<ArcSwap<PathBuf>> {
        Arc::clone(&self.cwd)
    }

    pub(crate) fn manager_and_root(&self) -> (AgentManagerHandle, makima_agent::AgentId) {
        (self.manager.clone(), self.root_id)
    }

    pub(crate) fn identity(&self) -> Arc<()> {
        self.manager
            .actor(self.root_id)
            .expect("live runtime actor")
            .identity()
    }

    pub(crate) fn apply_to_app(&self, app: &mut App) {
        app.chats[0].agent_id = Some(self.root_id);
        app.agent_chat_index.insert(self.root_id, 0);
        app.root_agent = Some(self.manager.reference(self.root_id));
        app.lua_event_handle.observe_agents(&self.manager);
        app.answer_tx = Some(self.answer_tx.clone());
        app.cmd_tx = Some(self.cmd_tx.clone());
        app.shared_history = Some(Arc::clone(&self.history));
        app.queue.set_shared(self.queue.clone());
        let restore_tx = makima_agent::EventSender::with_identity(
            self.agent_tx.clone(),
            Some(makima_agent::ActorWorkIdentity::new(
                self.manager_and_root().1,
                makima_agent::ActorWorkKind::Control { operation_id: 0 },
                crate::app::RESTORE_PRESENTATION,
            )),
        );
        app.restore_event_tx = Some(restore_tx.clone());
        for chat in &mut app.chats {
            chat.set_restore_channel(Some(restore_tx.clone()));
            chat.set_restore_target(chat.agent_id.map(|id| self.manager.reference(id)));
        }
    }

    /// Shuts the old actor down after the app and queue are repointed, so
    /// its close cannot poison the replacement. Everything the old agent
    /// still owns drains through the retained per-tab output channel.
    fn shutdown_actor(&self) {
        let manager = self.manager.clone();
        smol::spawn(async move {
            let _ = manager.suspend(Duration::from_secs(3)).await;
        })
        .detach();
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

    pub(crate) fn is_finished(&self) -> bool {
        self.manager.runner_finished(self.root_id).unwrap_or(true)
    }

    pub(crate) fn suspend(self) -> smol::Task<()> {
        smol::spawn(async move {
            let _ = self.manager.suspend(Duration::from_secs(3)).await;
        })
    }

    pub(crate) fn shutdown(self) -> smol::Task<()> {
        self.suspend()
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

fn agent_limits(config: &AgentConfig) -> AgentLimits {
    AgentLimits {
        max_concurrent_agent_turns: config.max_concurrent_agent_turns,
        max_agent_depth: config.max_agent_depth,
        max_children_per_agent: config.max_children_per_agent,
        max_active_agents: config.max_active_agents,
        max_retained_agents: config.max_retained_agents,
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_agent_internal(
    initial_config: EffectiveAgentConfig,
    initial_history: HistorySnapshot,
    config: AgentConfig,
    tool_output_lines: ToolOutputLines,
    permissions: &Arc<PermissionManager>,
    cwd: Arc<ArcSwap<PathBuf>>,
    mcp_handle: Option<McpHandle>,
    session_id: Option<SessionRef>,
    root_id: Option<AgentId>,
    journal: Arc<SessionJournal>,
    offload: Option<Arc<OffloadStore>>,
    timeouts: makima_providers::Timeouts,
    lua_handle: EventHandle,
    model_policy: Arc<ModelPolicy>,
    providers: Arc<ProviderRegistry>,
    system_prompt: SystemPromptOverride,
    restored_manager: Option<AgentManagerHandle>,
    existing_mailbox: Option<SessionMailbox>,
    file_write_locks: Arc<FileWriteLocks>,
) -> Result<PreparedAgentHandles, String> {
    let (agent_tx, agent_rx) = flume::unbounded::<Envelope>();
    let (cmd_tx, cmd_rx) = flume::unbounded::<AgentCommand>();
    let (answer_tx, answer_rx) = flume::unbounded::<String>();
    // Seeded empty because `AgentActorHandle::spawn` publishes the real
    // snapshot synchronously, before any handle escapes.
    let shared_history: SharedMessages = Arc::new(ArcSwap::from_pointee(initial_history.clone()));
    let initial_history = Arc::unwrap_or_clone(initial_history.messages);
    let subagent_cancels: Arc<CancelMap<String>> = Arc::new(CancelMap::new());
    let prepared_mailbox = session_id
        .as_ref()
        .filter(|_| existing_mailbox.is_none())
        .map(|session_id| SessionMailbox::prepare(session_id.id()));
    let mailbox = existing_mailbox.or_else(|| {
        prepared_mailbox
            .as_ref()
            .map(PreparedSessionMailbox::mailbox)
    });

    let (init_trigger, init_cancel) = CancelToken::new();

    let restored = restored_manager.is_some();
    let manager = match restored_manager {
        Some(manager) => manager,
        None => AgentManagerHandle::new_with_journal(agent_limits(&config), journal)
            .map_err(|error| error.to_string())?,
    };
    if let Some(store) = &offload {
        manager
            .bind_result_store(Arc::clone(store))
            .map_err(|error| error.to_string())?;
    }
    let (runner_start, start_rx) = flume::bounded(1);
    let initial_config =
        initial_config.with_context(AgentContextSnapshot::new((**cwd.load()).clone()));
    let root_id = root_id.unwrap_or_else(AgentId::generate);
    let backend = HeadlessBackend::new(
        AgentParams {
            agent_id: root_id,
            provider: Arc::clone(&initial_config.provider),
            model: initial_config.model.clone(),
            config,
            tool_output_lines,
            permissions: Arc::clone(permissions),
            session_id,
            mailbox: mailbox.clone(),
            timeouts,
            file_tracker: FileReadTracker::fresh(),
            prompt_slots: Arc::default(),
            root_context: None,
            modes: lua_handle.mode_registry(),
            subagent_cancels: Arc::clone(&subagent_cancels),
            registry: Arc::clone(ToolRegistry::global_arc()),
            audience: ToolAudience::MAIN,
            question_mode: QuestionMode::Tui,
            interactive_reauth: true,
            model_policy,
            providers,
            file_write_locks,
            offload: offload.clone(),
            managed_turn: None,
        },
        FrontendProfile::tui(tool_output_lines, init_cancel),
        agent_tx.clone(),
        HeadlessBackendOptions {
            persist_turn_results: true,
            system_override: system_prompt.override_text,
            append_system: system_prompt.append_text,
            prompt_slots: lua_handle.prompt_slot_source(),
            mcp: mcp_handle
                .clone()
                .map(|handle| McpSession::new(handle, &initial_history)),
            answer_rx: Some(Arc::new(Mutex::new(answer_rx))),
            ..Default::default()
        },
    );
    let orchestration_template = backend.template(&initial_config);
    let root = if restored {
        smol::block_on(manager.ensure_resident_with(root_id, |_| async {
            Ok(ResidentAgent {
                backend: Box::new(backend),
                config: initial_config,
                shared_messages: Some(Arc::clone(&shared_history)),
                start: Some(start_rx),
                initialize: None,
            })
        }))
    } else {
        manager.create_root_deferred_with_id_and_config(
            root_id,
            Some(initial_config),
            initial_history,
            Some(Arc::clone(&shared_history)),
            |_| Ok::<_, String>(Box::new(backend)),
            start_rx,
        )
    }
    .map_err(|error| error.to_string())?;
    let root_id = root.id();
    let actor = Arc::new(root.actor().expect("committed root actor"));
    let queue_tx = actor_queue(Arc::clone(&actor));

    spawn_command_router(
        cmd_rx,
        manager.clone(),
        root_id,
        Arc::clone(&subagent_cancels),
        init_trigger,
    );

    Ok(PreparedAgentHandles {
        handles: Some(AgentHandles {
            cmd_tx,
            agent_rx,
            actor_rx: actor.subscribe_events(),
            agent_tx,
            answer_tx,
            history: shared_history,
            mcp_handle,
            queue: queue_tx,
            mailbox,
            cwd,
            offload,
            orchestration_template,
            manager,
            root_id,
            runner_start: Some(runner_start),
        }),
        mailbox: prepared_mailbox,
        build: None,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Instant;

    use makima_config::PermissionsConfig;
    use makima_providers::provider::BoxFuture;
    use makima_providers::{
        AgentError, Invocation, ProviderEvent, RequestDelivery, RequestOptions, StreamResponse,
    };

    use super::{
        AgentConfig, AgentHandles, Arc, Duration, EventHandle, Message, Model, ModelPolicy,
        PermissionManager, Provider, ProviderAuthGeneration, ProviderChange, ProviderSlot,
        SessionRef, SystemPromptOverride, ToolOutputLines, join_all,
    };

    const LONG_TIMEOUT: Duration = Duration::from_secs(60);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);
    const TEST_CWD: &str = "/tmp";
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
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
        }
    }

    struct AuthProvider {
        reload_ok: bool,
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
            _invocation: &'a Invocation,
            _delivery: RequestDelivery,
        ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
            Box::pin(std::future::pending())
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
    }

    fn auth_slot(reload_ok: bool) -> (Arc<ProviderSlot>, flume::Receiver<ProviderChange>) {
        ProviderSlot::new(
            crate::components::test_model(),
            Arc::new(AuthProvider { reload_ok }),
        )
    }

    fn stub_spawn(initial_history: Vec<Message>) -> AgentHandles {
        let (model_slot, _change_rx) =
            ProviderSlot::new(crate::components::test_model(), Arc::new(StubProvider));
        let permissions = Arc::new(PermissionManager::new(
            PermissionsConfig::default(),
            PathBuf::from(TEST_CWD),
            makima_config::ProjectConfig::for_project(std::path::Path::new(TEST_CWD)),
            Arc::default(),
        ));
        AgentHandles::spawn(
            &model_slot,
            initial_history,
            AgentConfig::default(),
            ToolOutputLines::default(),
            &permissions,
            PathBuf::from(TEST_CWD),
            None,
            makima_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            None,
            Arc::new(ModelPolicy::default()),
            Arc::default(),
            SystemPromptOverride::default(),
        )
    }

    #[test]
    fn inherited_template_has_environment_before_initialization() {
        let handles = stub_spawn(Vec::new());
        let template = &handles.orchestration_template;
        assert_eq!(template.cwd, PathBuf::from(TEST_CWD));
        let tools = template.request_tools.as_ref().unwrap().definitions();
        let tools = serde_json::to_string(tools).unwrap();
        for unresolved in ["{cwd}", "{platform}", "{date}"] {
            assert!(!tools.contains(unresolved));
        }
        smol::block_on(handles.shutdown());
    }

    #[test]
    fn shutdown_preserves_admitted_turn_for_restart() {
        const INPUT: &str = "unfinished work";
        let handles = stub_spawn(Vec::new());
        let (manager, root) = handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let idle = actor.prepare_idle().unwrap();
        actor
            .admit_turn(
                makima_agent::AgentInput::new(INPUT.into(), vec![]),
                None,
                String::new(),
            )
            .unwrap();
        let before = manager.journal().snapshot().fold().unwrap();
        assert!(!before.scopes[&Some(root)].admitted.is_empty());
        smol::block_on(handles.shutdown());
        let after = manager.journal().snapshot().fold().unwrap();
        assert_eq!(
            after.scopes[&Some(root)].admitted,
            before.scopes[&Some(root)].admitted
        );
        assert_eq!(
            after.scopes[&Some(root)].results,
            before.scopes[&Some(root)].results
        );
        drop(idle);
    }

    /// Instance generations come from a process-wide counter (sessions each
    /// own a slot and share one usage coordinator), so this asserts the
    /// invariants rather than absolute numbers.
    #[test]
    fn provider_install_increments_instance_and_resets_auth_generation() {
        let (slot, change_rx) = auth_slot(true);
        let before = slot.load().provider.identity();
        assert_eq!(before.auth, ProviderAuthGeneration(0));

        let provider = Arc::clone(&slot.load().provider);
        smol::block_on(provider.reload_auth()).expect("reload succeeds");
        assert_eq!(provider.identity().auth, ProviderAuthGeneration(1));
        change_rx.recv().expect("reload notification");

        let identity = slot.install(
            crate::components::test_model(),
            Arc::new(AuthProvider { reload_ok: true }),
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
        let (slot, change_rx) = auth_slot(true);
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
    }

    #[test]
    fn failed_reload_does_not_bump_auth_generation() {
        let (failed_slot, failed_rx) = auth_slot(false);
        let failed = Arc::clone(&failed_slot.load().provider);
        assert!(smol::block_on(failed.reload_auth()).is_err());
        assert_eq!(failed.identity().auth, ProviderAuthGeneration(0));
        assert!(failed_rx.is_empty());
    }

    /// If the seeded empty snapshot ever outlived `spawn`, the next checkpoint
    /// would adopt it and wipe a resumed conversation from disk.
    #[test]
    fn spawn_publishes_the_resumed_history_before_the_handles_escape() {
        let handles = stub_spawn(vec![Message::user(RESUMED_HISTORY_TEXT.into())]);
        let snapshot = handles.history.load();
        assert_eq!(
            snapshot.messages.len(),
            1,
            "the seeded empty snapshot must be replaced synchronously"
        );
        assert_eq!(snapshot.messages[0].user_text(), Some(RESUMED_HISTORY_TEXT));
    }

    /// A session owns its provider slot, but the usage coordinator listens on
    /// exactly one channel. A re-auth on a session-local provider has to reach
    /// it, or the status line and usage panel go stale with no way to notice.
    #[test]
    fn session_slot_reports_into_the_shared_change_channel() {
        let (loop_slot, change_rx) = auth_slot(true);
        let session_slot = ProviderSlot::with_change_tx(
            crate::components::test_model(),
            Arc::new(AuthProvider { reload_ok: true }),
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
            Arc::new(AuthProvider { reload_ok: true }),
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
