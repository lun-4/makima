//! Shared canned-provider harness for makima-lua integration tests.
//!
//! Every test here drives the real code path without a model API: `CannedProvider`
//! hands back deterministic single-turn replies and records the `RequestOptions`
//! (thinking) and the tools JSON each spawned session received, so tests can
//! assert what actually reached the "model".
//!
//! Each `tests/*.rs` binary compiles its own copy of this module, so a helper
//! used by one binary but not another looks dead to that other binary. Allow it:
//! the module is a shared helper, not every item is used by every consumer.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use makima_agent::actor::{
    ActorHistory, AdmissionPreparation, AgentContextSnapshot, EffectiveAgentConfig, PreparedModel,
    PreparedReadiness,
};
use makima_agent::agent::{RunSettings, TurnSetup};
use makima_agent::cancel::{CancelMap, CancelToken, CancelTrigger};
use makima_agent::config_change::unresolved_model;
use makima_agent::journal::SessionJournal;
use makima_agent::manager::ResidentAgent;
use makima_agent::permissions::PermissionSnapshot;
use makima_agent::session_checkpoint::{SessionLogCheckpoint, stored_agent_config};
use makima_agent::session_coordinator::{
    SessionCoordinatorHandle, SessionCoordinatorParams, builtin_option_definitions,
};
use makima_agent::template::Vars;
use makima_agent::tools::offload::OffloadStore;
use makima_agent::tools::test_support::stub_ctx;
use makima_agent::tools::{DescriptionContext, RequestTools, ToolContext, ToolRegistry};
use makima_agent::{
    ActorBackend, AgentEvent, AgentInput, AgentLimits, AgentManagerHandle, AgentMode, AgentRef,
    BackendResult, ControlWork, CurrentManagedTurn, Envelope, EventSender, McpSession,
    SessionMailbox, SharedMessages, ToolOutput, TurnCancellationReason, TurnContext, TurnOutcome,
    WorkKind, modes,
};
use makima_lua::PluginHost;
use makima_lua::orchestration::{
    AgentOrchestration, CommandRunner, TrustedTarget, child_persistence,
};
use makima_lua::test_support::InMemoryFs;
use makima_lua::ui_sink::HostUi;
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::test_util::offline_registry;
use makima_providers::{
    AgentError, ContentBlock, FastSupport, Invocation, Message, Model, ModelInfo, ModelTier,
    ProviderEvent, ProviderRegistry, RequestDelivery, RequestOptions, Role, StopReason,
    StreamResponse, ThinkingConfig, TokenUsage,
};
use makima_storage::StateDir;
use makima_storage::checkpoint::CheckpointPublication;
use makima_storage::id::{MakimaId, SessionRef};
use makima_storage::session_state::{StateChange, StoredStateRecord};
use makima_storage::sessions::{SESSIONS_DIR, Session};
use serde_json::Value;

const TEST_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);
const PARKED_ROOT_PROMPT: &str = "parked root turn";

pub async fn bounded<T>(future: impl Future<Output = T>) -> T {
    futures_lite::future::race(future, async {
        smol::Timer::after(TEST_OPERATION_TIMEOUT).await;
        panic!("test operation did not finish within {TEST_OPERATION_TIMEOUT:?}");
    })
    .await
}

/// A provider that hands back canned single-turn responses so a run completes
/// deterministically without any network or credentials. It records the
/// thinking config (`RequestOptions.thinking`) and the tools JSON passed to
/// `stream_message`, so tests can assert what the model was offered.
pub struct CannedProvider {
    replies: Mutex<Vec<StreamResponse>>,
    captured_opts: Mutex<Vec<RequestOptions>>,
    captured_tools: Mutex<Vec<Value>>,
}

impl CannedProvider {
    pub fn new(replies: Vec<StreamResponse>) -> Self {
        Self {
            replies: Mutex::new(replies),
            captured_opts: Mutex::new(Vec::new()),
            captured_tools: Mutex::new(Vec::new()),
        }
    }

    /// The `ThinkingConfig`s the provider observed, in request order. Lets tests
    /// assert which effort/off/budget a spawned session actually used.
    pub fn captured_thinking(&self) -> Vec<ThinkingConfig> {
        self.captured_opts
            .lock()
            .unwrap()
            .iter()
            .map(|o| o.thinking)
            .collect()
    }

    pub fn captured_tools(&self) -> Vec<Value> {
        self.captured_tools.lock().unwrap().clone()
    }
}

impl Provider for CannedProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        _messages: &'a [Message],
        _system: &'a str,
        tools: &'a Value,
        _event_tx: &'a flume::Sender<ProviderEvent>,
        opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
        _invocation: &'a Invocation,
        _delivery: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.captured_opts.lock().unwrap().push(opts);
            self.captured_tools.lock().unwrap().push(tools.clone());
            let mut replies = self.replies.lock().unwrap();
            assert!(!replies.is_empty(), "CannedProvider: no more responses");
            Ok(replies.remove(0))
        })
    }
}

const DEFAULT_MODEL: &str = "anthropic/claude-sonnet-4-20250514";

pub fn normalized_history_epoch_state(state: &[StoredStateRecord]) -> Vec<StoredStateRecord> {
    let mut epochs = BTreeSet::new();
    for record in state {
        match &record.change {
            StateChange::Boundary { lens, .. } => {
                epochs.extend(lens.values().map(|(epoch, _)| *epoch));
            }
            StateChange::Compaction { epoch } => {
                epochs.insert(*epoch);
            }
            _ => {}
        }
    }
    let epochs: BTreeMap<_, _> = epochs
        .into_iter()
        .enumerate()
        .map(|(rank, epoch)| (epoch, rank as u64))
        .collect();
    let mut state = state.to_vec();
    for record in &mut state {
        match &mut record.change {
            StateChange::Boundary { lens, .. } => {
                for (epoch, _) in lens.values_mut() {
                    *epoch = epochs[epoch];
                }
            }
            StateChange::Compaction { epoch } => *epoch = epochs[epoch],
            _ => {}
        }
    }
    state
}

pub fn default_model() -> Model {
    Model::from_spec(DEFAULT_MODEL).unwrap()
}

/// The providers of the specs these tests name, each model with every
/// capability: a weak, a medium and a strong model of the default model's
/// provider, plus the other providers' models without curated rows.
pub fn test_providers() -> Arc<ProviderRegistry> {
    let model = |id: &str, tier| ModelInfo {
        tier: Some(tier),
        supports_thinking: Some(true),
        supports_vision: Some(true),
        supports_fast: Some(FastSupport::Supported),
        supports_tool_examples: Some(true),
        ..ModelInfo::id_only(id.into())
    };
    let (provider, default_id) = Model::parse_spec(DEFAULT_MODEL).unwrap();
    Arc::new(offline_registry(vec![
        (
            provider,
            vec![
                model("claude-haiku-4-5", ModelTier::Weak),
                model(default_id, ModelTier::Medium),
                model("claude-opus-4-8", ModelTier::Strong),
            ],
        ),
        ("openai", Vec::new()),
        ("ollama", Vec::new()),
    ]))
}

pub fn canned_reply(text: &str) -> StreamResponse {
    StreamResponse {
        message: Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: text.into() }],
            ..Default::default()
        },
        usage: TokenUsage::default(),
        stop_reason: Some(StopReason::EndTurn),
    }
}

/// The model asks to call `tool_name` with `input` next.
pub fn canned_tool_use(tool_name: &str, input: Value) -> StreamResponse {
    StreamResponse {
        message: Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use("t1", tool_name, input)],
            ..Default::default()
        },
        usage: TokenUsage::default(),
        stop_reason: Some(StopReason::ToolUse),
    }
}

/// Tool names from a tools JSON array, in order.
pub fn tool_names(tools: &Value) -> Vec<String> {
    tools
        .as_array()
        .expect("tools must be a JSON array")
        .iter()
        .filter_map(|t| t["name"].as_str().map(String::from))
        .collect()
}

/// A real tool context with a canned provider serving `replies` and a live event
/// channel back to the "ui" (the returned receiver), mimicking a production run
/// whose cancel we can fire on demand via the returned `CancelTrigger`.
pub fn ctx_with_replies(
    replies: Vec<StreamResponse>,
) -> (ToolContext, flume::Receiver<Envelope>, CancelTrigger) {
    ctx_with_provider(Arc::new(CannedProvider::new(replies)))
}

/// Like [`ctx_with_replies`] but with a caller-held `provider` so the test can
/// inspect its captured opts/tools after a run.
pub fn ctx_with_provider<P>(
    provider: Arc<P>,
) -> (ToolContext, flume::Receiver<Envelope>, CancelTrigger)
where
    P: Provider + 'static,
{
    let (run_trigger, run_cancel) = CancelToken::new();
    let (tx, rx) = flume::unbounded::<Envelope>();
    let event_tx = EventSender::new(tx);
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.cancel = run_cancel;
    ctx.subagent_cancels = Arc::new(CancelMap::new());
    ctx.event_tx = event_tx;
    ctx.provider = provider;
    ctx.model = Arc::new(default_model());
    ctx.providers = test_providers();
    (ctx, rx, run_trigger)
}

/// Convenience: a canned context with two generic text replies so a subagent
/// run completes one turn.
pub fn ctx_with_canned_provider() -> (ToolContext, flume::Receiver<Envelope>, CancelTrigger) {
    ctx_with_replies(vec![canned_reply("the answer"), canned_reply("the answer")])
}

/// A real tool context that mimics production for no-run tests: a live parent
/// cancel plus a real subagent-cancel map. Uses the stock stub provider (the
/// subagent never runs, so no provider is needed).
pub fn production_like_ctx() -> (ToolContext, CancelTrigger) {
    let mut ctx = stub_ctx(&AgentMode::Build);
    let (run_trigger, run_cancel) = CancelToken::new();
    ctx.cancel = run_cancel;
    ctx.subagent_cancels = Arc::new(CancelMap::new());
    ctx.providers = test_providers();
    (ctx, run_trigger)
}

pub fn with_request_tools(registry: &ToolRegistry, ctx: &ToolContext) -> ToolContext {
    let mut ctx = ctx.clone();
    let definitions = registry.definitions(
        &Vars::new(),
        &DescriptionContext {
            filter: &ctx.tool_filter,
            audience: ctx.audience,
            workflow: ctx.workflow,
            mcp: false,
        },
        ctx.model.supports_tool_examples(),
    );
    let tools = RequestTools::assembled(definitions, &ctx.config, &ctx.model);
    ctx.tool_filter = Arc::clone(tools.filter());
    ctx.request_tools = Some(tools);
    ctx
}

pub fn exec_tool_text(
    reg: &ToolRegistry,
    ctx: &ToolContext,
    name: &str,
    input: Value,
) -> Result<String, String> {
    let ctx = with_request_tools(reg, ctx);
    let inv = reg
        .get(name)
        .unwrap_or_else(|| panic!("tool {name} not registered"))
        .tool
        .parse(&input)
        .expect("parse failed");
    smol::block_on(bounded(inv.execute(&ctx)))
        .output
        .map(|out| match out {
            ToolOutput::Plain(s) | ToolOutput::Markdown(s) => s.text,
            other => panic!("unexpected output: {other:?}"),
        })
        .map_err(|e| e.to_string())
}

pub fn exec_tool(
    reg: &ToolRegistry,
    ctx: &ToolContext,
    name: &str,
    input: Value,
) -> Result<Value, String> {
    let out = exec_tool_text(reg, ctx, name, input)?;
    serde_json::from_str(&out).map_err(|e| format!("invalid json {out:?}: {e}"))
}

/// The effective config a root built from {ctx} starts with.
pub fn effective_config(ctx: &ToolContext) -> EffectiveAgentConfig {
    EffectiveAgentConfig::new(
        RunSettings {
            provider: Arc::clone(&ctx.provider),
            model: (*ctx.model).clone(),
            fast: ctx.opts.fast,
            workflow: ctx.workflow,
            thinking: ctx.opts.thinking,
        },
        ctx.mode.clone(),
    )
}

/// A root whose turns never finish.
pub struct IdleRoot;

pub type TestSession = Session<Message, TokenUsage, ToolOutput>;

pub struct RestartHost {
    pub host: PluginHost,
    pub manager: AgentManagerHandle,
    pub session: TestSession,
    pub fs: Arc<InMemoryFs>,
    pub root: TrustedTarget,
    pub history: SharedMessages,
    plugins: Vec<String>,
    provider: Option<Arc<dyn Provider>>,
    providers: Option<Arc<ProviderRegistry>>,
    _cancel: CancelTrigger,
    shutdown: RestartShutdown,
}

struct RestartShutdown {
    manager: AgentManagerHandle,
    coordinator: Option<SessionCoordinatorHandle>,
}

impl Drop for RestartShutdown {
    fn drop(&mut self) {
        if let Some(coordinator) = &self.coordinator {
            smol::block_on(bounded(coordinator.close())).unwrap();
        }
        for (_, record) in self.manager.journal().snapshot().agent_revisions {
            if self.manager.is_live(record.id) {
                let incarnation = self.manager.incarnation(record.id).unwrap();
                self.manager.detach_incarnation(&incarnation).unwrap();
            }
        }
        let report = smol::block_on(bounded(self.manager.shutdown(TEST_OPERATION_TIMEOUT)));
        assert!(report.timed_out.is_empty());
    }
}

impl RestartHost {
    pub fn open(
        state_dir: &Path,
        session_id: MakimaId,
        fs: Arc<InMemoryFs>,
        plugins: Vec<String>,
    ) -> Self {
        let session = TestSession::load(session_id, &StateDir::from_path(state_dir.into()))
            .expect("load restart session from disk");
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::with_session_provider_preparer(
            Arc::clone(&registry),
            Default::default(),
            true,
            fs.clone(),
            Some(state_dir.into()),
            None,
            HostUi::Frontend,
        )
        .unwrap();
        let files = fs.files();
        for name in &plugins {
            let path = state_dir.join("plugins").join(format!("{name}.lua"));
            let (_, bytes) = files.iter().find(|(file, _)| file == &path).unwrap();
            host.load_source(name, std::str::from_utf8(bytes).unwrap())
                .unwrap();
        }
        let (mut context, _events, cancel) = ctx_with_canned_provider();
        context.session_id = Some(session_id.into());
        context.cwd = session.cwd.clone().into();
        context.model = Arc::new(Model::from_spec(&session.model).unwrap());
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let history: SharedMessages = Arc::default();
        let root = manager
            .create_root_with_config(
                Some(
                    effective_config(&context)
                        .with_context(AgentContextSnapshot::new(context.cwd.clone())),
                ),
                session.messages().to_vec(),
                Some(Arc::clone(&history)),
                |_| Ok::<_, String>(Box::new(IdleRoot) as Box<dyn ActorBackend>),
            )
            .unwrap();
        context.execution_agent_id = Some(root.id());
        let root = install_root(&host, root, with_request_tools(&registry, &context));
        let root_manager = manager.clone();
        Self {
            host,
            manager,
            session,
            fs,
            root,
            history,
            plugins,
            provider: None,
            providers: None,
            _cancel: cancel,
            shutdown: RestartShutdown {
                manager: root_manager,
                coordinator: None,
            },
        }
    }

    pub fn open_with_provider(
        state_dir: &Path,
        session_id: MakimaId,
        fs: Arc<InMemoryFs>,
        plugins: Vec<String>,
        provider: Arc<dyn Provider>,
    ) -> Self {
        Self::open_with_provider_registry(
            state_dir,
            session_id,
            fs,
            plugins,
            provider,
            test_providers(),
        )
    }

    pub fn open_with_provider_registry(
        state_dir: &Path,
        session_id: MakimaId,
        fs: Arc<InMemoryFs>,
        plugins: Vec<String>,
        provider: Arc<dyn Provider>,
        providers: Arc<ProviderRegistry>,
    ) -> Self {
        Self::open_with_provider_registry_and_mcp(
            state_dir, session_id, fs, plugins, provider, providers, None,
        )
    }

    pub fn open_with_provider_registry_and_mcp(
        state_dir: &Path,
        session_id: MakimaId,
        fs: Arc<InMemoryFs>,
        plugins: Vec<String>,
        provider: Arc<dyn Provider>,
        providers: Arc<ProviderRegistry>,
        mcp: Option<McpSession>,
    ) -> Self {
        let state = StateDir::from_path(state_dir.into());
        let mut session = TestSession::load(session_id, &state).unwrap();
        let registry = Arc::new(ToolRegistry::new());
        let prepared_provider = Arc::clone(&provider);
        let host = PluginHost::with_session_provider_preparer(
            Arc::clone(&registry),
            Default::default(),
            true,
            fs.clone(),
            Some(state_dir.into()),
            Some(Arc::new(move |model| {
                Ok((model, Arc::clone(&prepared_provider)))
            })),
            HostUi::Frontend,
        )
        .unwrap();
        let files = fs.files();
        for name in &plugins {
            let path = state_dir.join("plugins").join(format!("{name}.lua"));
            let (_, bytes) = files.iter().find(|(file, _)| file == &path).unwrap();
            host.load_source(name, std::str::from_utf8(bytes).unwrap())
                .unwrap();
        }
        let (mut context, _events, cancel) = ctx_with_canned_provider();
        context.provider = Arc::clone(&provider);
        context.providers = Arc::clone(&providers);
        context.mcp = mcp;
        context.session_id = Some(session_id.into());
        context.offload = Some(Arc::new(OffloadStore::for_session(
            state_dir,
            context.session_id.as_ref().unwrap(),
        )));
        context.modes = host.mode_registry();
        context.registry = Arc::clone(&registry);
        let unresolved = session.root_agent_id().is_none_or(|root| {
            session.latest_state().unwrap().scopes[&Some(root)]
                .config
                .is_none()
        });
        if unresolved {
            context.cwd = session.cwd.clone().into();
            context.model = Arc::new(Model::from_spec(&session.model).unwrap());
            context.opts.fast = session.meta.fast;
            context.opts.thinking = session.meta.thinking.map(Into::into).unwrap_or_default();
            context.workflow = session.meta.workflow;
            if let Some((key, _)) = modes::saved(&session.meta) {
                let (mode, definition) =
                    modes::resolve(&context.modes, key, &context.mode, &state).unwrap();
                context.mode = mode;
                context.mode_def = Some(Arc::new(definition));
            }
            let initial = effective_config(&context)
                .with_context(AgentContextSnapshot::new(context.cwd.clone()))
                .with_mode_def(Some(
                    context
                        .mode_def
                        .as_deref()
                        .cloned()
                        .unwrap_or_else(|| context.modes.current(&context.mode)),
                ));
            session
                .resolve_legacy_config(
                    stored_agent_config(&initial, session.meta.session_options.clone()).unwrap(),
                )
                .unwrap();
        }
        let root_id = session.root_agent_id().unwrap();
        let restored = session.latest_state().unwrap();
        let stored = restored.scopes[&Some(root_id)].config.as_ref().unwrap();
        let persisted_options = stored.options.clone();
        let yolo = restored.scopes.get(&None).and_then(|scope| scope.yolo);
        if let Some(scope) = restored.scopes.get(&None) {
            context
                .permissions
                .restore(PermissionSnapshot::from_stored(&scope.rules, scope.yolo).unwrap());
        }
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        let snapshot = journal.checkpoint_snapshot().unwrap();
        let manager = AgentManagerHandle::restore_graph(
            AgentLimits::default(),
            Arc::clone(&journal),
            session.latest_state().unwrap(),
            snapshot.histories,
        )
        .unwrap();
        let checkpoint = Arc::new(
            SessionLogCheckpoint::open_with_session(state, session.clone(), None).unwrap(),
        );
        context
            .permissions
            .bind_journal(Arc::clone(&journal), None)
            .unwrap();
        smol::block_on(bounded(checkpoint.bind_journal(journal))).unwrap();
        let history: SharedMessages = Arc::default();
        let shared = Arc::clone(&history);
        let root_provider = Arc::clone(&provider);
        let modes = Arc::clone(&context.modes);
        let event_tx = context.event_tx.clone();
        let setup = TurnSetup {
            registry: Arc::clone(&registry),
            local_tools: Arc::clone(&context.local_tools),
            mcp: context.mcp.clone(),
            slots: host.event_handle().prompt_slot_source(),
            modes: Arc::clone(&context.modes),
            instructions: true,
        };
        let root = smol::block_on(bounded(manager.ensure_resident_with(
            root_id,
            move |input| async move {
                let stored = input.config.expect("restored root configuration");
                let restored = modes::prepare_restore(&stored.mode, &modes).unwrap();
                if let Some(message) = restored.warning {
                    let _ = event_tx.send(AgentEvent::ControlError { message });
                }
                let mut config = EffectiveAgentConfig::from_stored(
                    &stored,
                    PreparedModel {
                        provider: root_provider,
                        model: Model::from_spec(&stored.model).unwrap(),
                    },
                    (restored.mode, restored.definition),
                )
                .unwrap();
                if let Some(spec) = input.record.spec {
                    config = config.with_mode_authority(spec.mode_ceiling.into());
                }
                Ok(ResidentAgent {
                    backend: Box::new(ContextRoot(setup)),
                    config,
                    shared_messages: Some(shared),
                    start: None,
                    initialize: None,
                })
            },
        )))
        .unwrap();
        let config = root.effective_config().unwrap().unwrap();
        context.execution_agent_id = Some(root.id());
        context.cwd = config.context.cwd().to_path_buf();
        context.model = Arc::new(config.model.clone());
        context.mode = config.mode.clone();
        context.mode_def = config.mode_def.clone().map(Arc::new);
        context.opts.fast = config.fast;
        context.opts.thinking = config.thinking;
        context.workflow = config.workflow;
        let coordinator = SessionCoordinatorHandle::register(SessionCoordinatorParams {
            session_id,
            catalog: host.event_handle().session_option_catalog(),
            definitions: builtin_option_definitions(
                config.model.spec(),
                [Arc::from(config.model.spec())],
                yolo.unwrap_or_default(),
                config.fast,
                config.workflow,
                config.thinking,
            ),
            persisted_options,
            model_policy: Arc::clone(&context.model_policy),
            checkpoint: checkpoint.clone(),
            mailbox: SessionMailbox::new(session_id),
        })
        .unwrap();
        coordinator
            .bind_permissions(Arc::clone(&context.permissions))
            .unwrap();
        let model_provider = Arc::clone(&provider);
        let model_registry = Arc::clone(&context.providers);
        coordinator
            .bind_actor(
                root.clone(),
                Arc::new(move |spec| {
                    let model = model_registry.model(&spec).map_err(unresolved_model);
                    let provider = Arc::clone(&model_provider);
                    Box::pin(async move {
                        Ok(PreparedModel {
                            model: model?,
                            provider,
                        })
                    })
                }),
                Arc::new(|_| {}),
            )
            .unwrap();
        let root = TrustedTarget {
            target: root,
            template: with_request_tools(&registry, &context),
        };
        let root_id = root.target.id();
        let root_config = coordinator.clone();
        let root_completion = coordinator.clone();
        let services = Arc::new_cyclic(|services| {
            let (child_config, child_completion) =
                child_persistence("restart fixture", services.clone(), checkpoint);
            let services = AgentOrchestration::new(Arc::default()).with_completion(Arc::new(
                move |target, history| {
                    if target.id() != root_id {
                        return child_completion(target, history);
                    }
                    let coordinator = root_completion.clone();
                    Box::pin(async move {
                        coordinator
                            .persist_history(history)
                            .await
                            .map_err(|error| error.to_string())
                    })
                },
            ));
            services.register_root(
                root.clone(),
                Some(Arc::new(move |target, publication| {
                    if target.id() != root_id {
                        return child_config(target, publication);
                    }
                    Ok(root_config
                        .reserve_transaction(publication)?
                        .into_config_mutation())
                })),
            );
            services.select(Some(root.target.clone()));
            services
        });
        host.event_handle().install_orchestration_services(services);
        host.wait_for_worker_barrier_for_test();
        Self {
            host,
            manager: manager.clone(),
            session,
            fs,
            root,
            history,
            plugins,
            provider: Some(provider),
            providers: Some(providers),
            _cancel: cancel,
            shutdown: RestartShutdown {
                manager,
                coordinator: Some(coordinator),
            },
        }
    }

    pub fn restart_host_with_provider_registry(
        mut self,
        state_dir: &Path,
        providers: Arc<ProviderRegistry>,
    ) -> Self {
        self.providers = Some(providers);
        self.restart_host(state_dir)
    }

    pub fn crash_restart_host(self, state_dir: &Path) -> Self {
        let path = state_dir
            .join(SESSIONS_DIR)
            .join(format!("{}.jsonl", self.session.id));
        let published = std::fs::read(&path).unwrap();
        let files = self.fs.files();
        let plugins = self.plugins.clone();
        let session_id = self.session.id;
        let provider = self.provider.clone().expect("crash fixture provider");
        let providers = self.providers.clone().unwrap();
        drop(self);
        std::fs::write(&path, published).unwrap();
        let state = StateDir::from_path(state_dir.into());
        let seed = TestSession::load(session_id, &state).unwrap();
        let checkpoint = SessionLogCheckpoint::open_with_session(state, seed, None).unwrap();
        smol::block_on(bounded(checkpoint.recover(
            CheckpointPublication::default(),
            Arc::new(|publication| {
                assert!(publication.try_publish());
                Ok(())
            }),
        )))
        .unwrap();
        drop(checkpoint);
        let fs = Arc::new(InMemoryFs::new());
        for (path, bytes) in files {
            fs.seed(&path, bytes);
        }
        Self::open_with_provider_registry(state_dir, session_id, fs, plugins, provider, providers)
    }

    pub fn restart_host_with_mcp(mut self, state_dir: &Path, mcp: Option<McpSession>) -> Self {
        self.root.template.mcp = mcp;
        self.restart_host(state_dir)
    }

    pub fn restart_host(self, state_dir: &Path) -> Self {
        if let Some(coordinator) = &self.shutdown.coordinator {
            smol::block_on(bounded(
                coordinator.persist_history(self.history.load_full().as_ref().clone()),
            ))
            .unwrap();
        }
        let files = self.fs.files();
        let plugins = self.plugins.clone();
        let session_id = self.session.id;
        let provider = self.provider.clone();
        let providers = self.providers.clone();
        let mcp = self.root.template.mcp.clone();
        drop(self);
        let fs = Arc::new(InMemoryFs::new());
        for (path, bytes) in files {
            fs.seed(&path, bytes);
        }
        match provider {
            Some(provider) => Self::open_with_provider_registry_and_mcp(
                state_dir,
                session_id,
                fs,
                plugins,
                provider,
                providers.unwrap(),
                mcp,
            ),
            None => Self::open(state_dir, session_id, fs, plugins),
        }
    }
}

struct ContextRoot(TurnSetup);

impl ActorBackend for ContextRoot {
    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        Some(self.0.clone().preparation())
    }

    fn prepared_readiness(&self) -> Option<PreparedReadiness> {
        Some(TurnSetup::readiness())
    }

    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }
}

impl ActorBackend for IdleRoot {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

/// A root that hands out its managed turn and parks it until {cancel} fires.
struct ParkedRoot {
    turn: flume::Sender<CurrentManagedTurn>,
    cancel: CancelToken,
}

impl ActorBackend for ParkedRoot {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        ctx: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            self.turn.send(ctx.managed_turn.unwrap()).unwrap();
            self.cancel.cancelled().await;
            BackendResult::EnteredRun(TurnOutcome::cancelled(
                ctx.agent_id,
                ctx.turn_id.unwrap(),
                TokenUsage::default(),
                0,
                TurnCancellationReason::User,
            ))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

/// Installs a root on {host} running {config}, parked inside one managed
/// turn until `ctx.cancel` fires, and points {ctx} at that turn.
pub fn bind_parked_root(
    host: &PluginHost,
    reg: &ToolRegistry,
    ctx: &mut ToolContext,
    config: EffectiveAgentConfig,
    limits: AgentLimits,
) {
    let manager = AgentManagerHandle::new(limits).unwrap();
    let (turn_tx, turn_rx) = flume::bounded(1);
    let cancel = ctx.cancel.clone();
    let root = manager
        .create_root_with_config(Some(config), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(ParkedRoot {
                turn: turn_tx,
                cancel,
            }) as Box<dyn ActorBackend>)
        })
        .unwrap();
    ctx.execution_agent_id = Some(root.id());
    root.actor()
        .unwrap()
        .admit_turn(
            AgentInput {
                message: PARKED_ROOT_PROMPT.into(),
                images: Vec::new(),
                preamble: Vec::new(),
                prompt: None,
            },
            None,
            PARKED_ROOT_PROMPT.into(),
        )
        .unwrap();
    ctx.managed_turn = Some(smol::block_on(bounded(turn_rx.recv_async())).unwrap());
    install_root(host, root, with_request_tools(reg, ctx));
}

/// Installs {root} as {host}'s orchestration root, returning it as the
/// target a command dispatched on it pins.
pub fn install_root(host: &PluginHost, root: AgentRef, template: ToolContext) -> TrustedTarget {
    let target = TrustedTarget {
        target: root,
        template,
    };
    host.event_handle()
        .install_orchestration_services(Arc::new(AgentOrchestration::bind_root(
            Arc::default(),
            target.clone(),
        )));
    host.wait_for_worker_barrier_for_test();
    target
}

/// Installs an idle root as {host}'s orchestration root whose plugin-requested
/// commands go to {runner}, and returns its manager.
pub fn install_command_root(host: &PluginHost, runner: CommandRunner) -> AgentManagerHandle {
    let (template, _events, _cancel) = ctx_with_canned_provider();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root_with_config(Some(effective_config(&template)), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(IdleRoot) as Box<dyn ActorBackend>)
        })
        .unwrap();
    let services = AgentOrchestration::bind_root(
        Arc::default(),
        TrustedTarget {
            target: root.clone(),
            template,
        },
    );
    services.bind_commands(&root, runner);
    host.event_handle()
        .install_orchestration_services(Arc::new(services));
    host.wait_for_worker_barrier_for_test();
    manager
}
