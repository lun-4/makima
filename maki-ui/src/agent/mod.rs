mod agent_loop;
mod cancel_map;
mod command_router;
pub(crate) mod shared_queue;

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arc_swap::{ArcSwap, Guard};
use maki_agent::permissions::PermissionManager;
use maki_agent::{
    AgentConfig, CancelMap, CancelToken, Envelope, HistorySnapshot, McpCommand, McpConfigErrors,
    McpHandle, McpSnapshotReader, SessionMailbox, SharedMessages, ToolOutputLines,
};
use maki_config::ModelPolicy;
use maki_lua::EventHandle;
use maki_storage::id::SessionRef;
use serde_json::Value;

use self::cancel_map::new_run_cancel_map;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, Message, Model, ModelInfo, ProviderEvent, ProviderUsage, RequestOptions,
    StreamResponse,
};
use tracing::{info, warn};

use crate::app::App;
use crate::provider_usage::{ProviderAuthGeneration, ProviderIdentity, ProviderInstanceGeneration};

use self::agent_loop::AgentLoop;
use self::command_router::spawn_command_router;
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

pub(crate) struct ProviderSlot {
    current: ArcSwap<ProviderSnapshot>,
    next_instance: AtomicU64,
    change_tx: flume::Sender<ProviderChange>,
}

impl ProviderSlot {
    pub(crate) fn new(
        model: Model,
        provider: Arc<dyn Provider>,
    ) -> (Arc<Self>, flume::Receiver<ProviderChange>) {
        let (change_tx, change_rx) = flume::unbounded();
        let tracked = Arc::new(TrackedProvider::new(
            provider,
            ProviderInstanceGeneration(0),
            change_tx.clone(),
        ));
        (
            Arc::new(Self {
                current: ArcSwap::from_pointee(ProviderSnapshot {
                    model,
                    provider: tracked,
                }),
                next_instance: AtomicU64::new(1),
                change_tx,
            }),
            change_rx,
        )
    }

    pub(crate) fn load(&self) -> Guard<Arc<ProviderSnapshot>> {
        self.current.load()
    }

    pub(crate) fn install(&self, model: Model, provider: Arc<dyn Provider>) -> ProviderIdentity {
        let instance =
            ProviderInstanceGeneration(self.next_instance.fetch_add(1, Ordering::AcqRel));
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
/// loop can never steal new input. The output channel (`agent_tx`/`agent_rx`)
/// is per-tab: `respawn` reuses it, so anyone still holding a sender (a Lua
/// restore reply, a click, an old agent winding down) can always deliver.
/// Stale events are filtered by `run_id`, not by killing the channel.
pub(crate) struct AgentHandles {
    pub(crate) cmd_tx: flume::Sender<AgentCommand>,
    pub(crate) agent_rx: flume::Receiver<Envelope>,
    pub(crate) agent_tx: flume::Sender<Envelope>,
    pub(crate) answer_tx: flume::Sender<String>,
    pub(crate) history: SharedMessages,
    pub(crate) btw_system: Arc<ArcSwap<String>>,
    pub(crate) mcp_handle: Option<McpHandle>,
    pub(crate) mcp_config_errors: McpConfigErrors,
    pub(crate) queue: QueueSender,
    pub(crate) timeouts: maki_providers::Timeouts,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
    mailbox: Option<SessionMailbox>,
    task: smol::Task<()>,
}

impl AgentHandles {
    /// MCP is shared across sessions and agent respawns; the event loop starts it
    /// once and shuts it down at exit. Only the agent loop task lives here.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn(
        model_slot: &Arc<ProviderSlot>,
        initial_history: Vec<Message>,
        config: AgentConfig,
        tool_output_lines: ToolOutputLines,
        permissions: &Arc<PermissionManager>,
        session_id: Option<SessionRef>,
        plugin_data: HashMap<String, Value>,
        timeouts: maki_providers::Timeouts,
        lua_handle: EventHandle,
        mcp_handle: Option<McpHandle>,
        mcp_config_errors: McpConfigErrors,
        model_policy: Arc<ModelPolicy>,
        system_prompt: SystemPromptOverride,
    ) -> Self {
        spawn_agent_internal(
            flume::unbounded(),
            model_slot,
            initial_history,
            config,
            tool_output_lines,
            permissions,
            mcp_handle,
            mcp_config_errors,
            session_id,
            plugin_data,
            timeouts,
            lua_handle,
            model_policy,
            system_prompt,
        )
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

    pub(crate) fn cancel(self) {
        let _ = self.cmd_tx.try_send(AgentCommand::CancelAll);
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
        // thing that makes the old loop's in-flight envelopes stale. It lives
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
            self.mcp_handle.clone(),
            self.mcp_config_errors.clone(),
            Some(SessionRef::from(app.state.session.id)),
            app.state.session.meta.plugin_data.clone(),
            self.timeouts,
            lua_handle,
            Arc::clone(&self.model_policy),
            self.system_prompt.clone(),
        );
        let old = mem::replace(self, new);
        // Repoint the app at the new queue before dropping `old`, otherwise the app keeps
        // the last old `QueueSender` alive and the old loop parks in `recv_notify` forever.
        self.apply_to_app(app);
        app.flush_restored_queue();
        old.cancel();
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    /// Hand back the agent task, dropping every channel so the loop can
    /// wind down. The caller sends `CancelAll` first and then awaits all
    /// tabs at once via [`join_all`] instead of paying a serial timeout
    /// per tab.
    pub(crate) fn into_task(self) -> smol::Task<()> {
        self.task
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
    mcp_handle: Option<McpHandle>,
    mcp_config_errors: McpConfigErrors,
    session_id: Option<SessionRef>,
    plugin_data: HashMap<String, Value>,
    timeouts: maki_providers::Timeouts,
    lua_handle: EventHandle,
    model_policy: Arc<ModelPolicy>,
    system_prompt: SystemPromptOverride,
) -> AgentHandles {
    let (cmd_tx, cmd_rx) = flume::unbounded::<AgentCommand>();
    let (answer_tx, answer_rx) = flume::unbounded::<String>();
    let (queue_tx, queue_rx) = shared_queue::queue();
    let queue_rx = Arc::new(queue_rx);
    // Seeded empty because `AgentLoop::new` below publishes the real snapshot
    // synchronously, before any handle escapes.
    let shared_history: SharedMessages =
        Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()));
    let btw_system: Arc<ArcSwap<String>> = Arc::new(ArcSwap::from_pointee(String::new()));
    let (init_trigger, init_cancel) = CancelToken::new();
    let cancel_map = Arc::new(new_run_cancel_map(0, init_trigger));
    let subagent_cancels: Arc<CancelMap<String>> = Arc::new(CancelMap::new());
    let mailbox = session_id
        .as_ref()
        .map(|session_id| SessionMailbox::register_with_data(session_id.id(), plugin_data));

    spawn_command_router(
        cmd_rx,
        Arc::clone(&cancel_map),
        Arc::clone(&subagent_cancels),
    );

    let agent_loop = AgentLoop::new(
        Arc::clone(model_slot),
        config,
        tool_output_lines,
        initial_history,
        Arc::clone(&shared_history),
        Arc::clone(&btw_system),
        mcp_handle.clone(),
        Arc::clone(permissions),
        agent_tx.clone(),
        answer_rx,
        queue_rx,
        cancel_map,
        init_cancel,
        session_id,
        mailbox.clone(),
        timeouts,
        lua_handle,
        subagent_cancels,
        Arc::clone(&model_policy),
        system_prompt.clone(),
        Arc::new(maki_agent::tools::FileWriteLocks::new()),
    );

    let task = smol::spawn(agent_loop.run());

    AgentHandles {
        cmd_tx,
        agent_rx,
        agent_tx,
        answer_tx,
        history: shared_history,
        btw_system,
        mcp_handle,
        mcp_config_errors,
        queue: queue_tx,
        timeouts,
        model_policy,
        system_prompt,
        mailbox,
        task,
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

    struct AuthProvider {
        reload_ok: bool,
        rotate: bool,
    }

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
            None,
            HashMap::new(),
            maki_providers::Timeouts::default(),
            EventHandle::disconnected_for_test(),
            None,
            McpConfigErrors::new(PathBuf::new()),
            Arc::new(ModelPolicy::default()),
            SystemPromptOverride::default(),
        );
        (handles, model_slot, permissions)
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

    #[test]
    fn provider_install_increments_instance_and_resets_auth_generation() {
        let (slot, change_rx) = auth_slot(true, false);
        assert_eq!(
            slot.load().provider.identity(),
            ProviderIdentity::new(ProviderInstanceGeneration(0), ProviderAuthGeneration(0))
        );

        let identity = slot.install(
            crate::components::test_model(),
            Arc::new(AuthProvider {
                reload_ok: true,
                rotate: false,
            }),
        );

        assert_eq!(
            identity,
            ProviderIdentity::new(ProviderInstanceGeneration(1), ProviderAuthGeneration(0))
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

        // The restored item is drained from the new queue by the live agent
        // loop, which may pop it before this thread reads the shared queue, so
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
