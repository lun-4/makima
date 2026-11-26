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
use makima_agent::actor::{ActorEvent, PreparedModel};
use makima_agent::session::{SessionHandle, SessionSubscription};
use makima_agent::session_coordinator::ModelPreparer;
use makima_agent::tools::{ToolContext, offload::OffloadStore};
use makima_agent::{
    AgentManagerHandle, CancelScope, Envelope, McpCommand, McpHandle, SessionMailbox,
    SharedMessages,
};
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    AgentError, ConversationEnd, Invocation, Message, Model, ProviderEvent, ProviderUsage,
    RequestDelivery, RequestOptions, StreamResponse,
};
use makima_storage::id::SessionRef;
use tracing::{info, warn};

use crate::app::App;
use crate::provider_usage::{ProviderAuthGeneration, ProviderIdentity, ProviderInstanceGeneration};

use self::pending::PendingProvider;
use self::shared_queue::QueueSender;
pub(crate) use self::shared_queue::QueuedMessage;

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

    pub(crate) fn pending(&self) -> Option<&PendingProvider> {
        let inner: &dyn Any = self.inner.as_ref();
        inner.downcast_ref()
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

pub(crate) struct RuntimeChannels {
    pub(crate) session: SessionHandle,
    pub(crate) events: SessionSubscription,
    pub(crate) actor_rx: flume::Receiver<ActorEvent>,
    pub(crate) agent_tx: flume::Sender<Envelope>,
    pub(crate) history: SharedMessages,
    pub(crate) queue: QueueSender,
    pub(crate) offload: Option<Arc<OffloadStore>>,
    pub(crate) orchestration_template: ToolContext,
    cwd: Arc<ArcSwap<PathBuf>>,
    mcp_handle: Option<McpHandle>,
}

impl RuntimeChannels {
    pub(crate) fn new(
        session: SessionHandle,
        actor_rx: flume::Receiver<ActorEvent>,
        queue: QueueSender,
        cwd: Arc<ArcSwap<PathBuf>>,
        mcp_handle: Option<McpHandle>,
    ) -> Self {
        Self {
            events: session.events.subscribe(),
            agent_tx: session.agent_sender(),
            history: session.shared_history(),
            offload: session.offload(),
            orchestration_template: session.template().clone(),
            session,
            actor_rx,
            queue,
            cwd,
            mcp_handle,
        }
    }

    pub(crate) fn cancel_root(&self) {
        let _ = self
            .session
            .cancellation
            .cancel(CancelScope::Subtree(self.session.root().id()));
    }

    #[cfg(test)]
    pub(crate) fn runner_start_is_pending(&self) -> bool {
        self.session.runner_start_is_pending()
    }

    pub(crate) fn release_runner(&self) {
        self.session.activate();
    }

    pub(crate) fn mailbox(&self) -> Option<SessionMailbox> {
        Some(self.session.mailbox().clone())
    }

    pub(crate) fn cwd_slot(&self) -> Arc<ArcSwap<PathBuf>> {
        Arc::clone(&self.cwd)
    }

    pub(crate) fn manager_and_root(&self) -> (AgentManagerHandle, makima_agent::AgentId) {
        (self.session.manager.clone(), self.session.root().id())
    }

    pub(crate) fn identity(&self) -> Arc<()> {
        self.session
            .root()
            .actor()
            .expect("live runtime actor")
            .identity()
    }

    pub(crate) fn apply_to_app(&self, app: &mut App) {
        let (manager, root_id) = self.manager_and_root();
        app.chats[0].agent_id = Some(root_id);
        app.agent_chat_index.insert(root_id, 0);
        app.root_agent = Some(self.session.root().clone());
        app.lua_event_handle.observe_agents(&manager);
        app.answers = Some(self.session.answers.clone());
        app.shared_history = Some(Arc::clone(&self.history));
        app.queue.set_shared(self.queue.clone());
        let restore_tx = makima_agent::EventSender::with_identity(
            self.agent_tx.clone(),
            Some(makima_agent::ActorWorkIdentity::new(
                root_id,
                makima_agent::ActorWorkKind::Control { operation_id: 0 },
                crate::app::RESTORE_PRESENTATION,
            )),
        );
        app.restore_event_tx = Some(restore_tx.clone());
        for chat in &mut app.chats {
            chat.set_restore_channel(Some(restore_tx.clone()));
            chat.set_restore_target(chat.agent_id.map(|id| manager.reference(id)));
        }
    }

    pub(crate) fn send_mcp(&self, cmd: McpCommand) {
        if let Some(ref h) = self.mcp_handle {
            h.send(cmd);
        }
    }

    pub(crate) fn claim_mailbox_wake(&self) -> Vec<Message> {
        self.session.mailbox().claim_wake()
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.session
            .manager
            .runner_finished(self.session.root().id())
            .unwrap_or(true)
    }

    pub(crate) fn suspend(self) -> smol::Task<()> {
        smol::spawn(async move {
            let _ = self.session.manager.suspend(Duration::from_secs(3)).await;
        })
    }

    pub(crate) fn shutdown(self) -> smol::Task<()> {
        self.suspend()
    }
}

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

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use makima_providers::provider::BoxFuture;
    use makima_providers::{
        AgentError, Invocation, ProviderEvent, RequestDelivery, RequestOptions, StreamResponse,
    };

    use super::{
        Arc, Duration, Provider, ProviderAuthGeneration, ProviderChange, ProviderSlot, SessionRef,
        join_all,
    };
    use makima_providers::{Message, Model};

    const LONG_TIMEOUT: Duration = Duration::from_secs(60);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(50);

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
