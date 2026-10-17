use maki_agent::{
    ActorBackend, AgentId, AgentInput, AgentLimits, AgentManagerHandle, AgentMode, BackendResult,
    ControlWork, DoneReason, History, ManagerError, ModeId, RunSettings, TurnCancellationReason,
    TurnContext, TurnId, TurnOutcome, WorkKind,
    actor::{ActorEvent, DeliveryStatus, EffectiveAgentConfig, TurnOrigin, TurnProvenance},
    tools::{PLAN_WRITE_RESTRICTED, ToolContext, ToolRegistry},
};
use maki_commands::CommandInvocation;
use maki_lua::{
    PluginHost, SessionRequest, UiAction,
    agent_errors::{
        AGENT_BUSY, DELEGATED_FOCUS, DELEGATED_HOST_MUTATION, DELEGATED_REGISTRATION,
        FOREIGN_AGENT_REFERENCE, INVOCATION_EXPIRED, MANAGED_AUTHORITY_INACTIVE, NO_FOCUSED_AGENT,
        NOT_DESCENDANT_OF_CALLER, ORCHESTRATION_CONTEXT_EXPIRED, OTHER_RUNTIME,
        OUTPUT_SCHEMA_NOT_OBJECT, RESTRICTED_MODE_CHANGE, SELF_TURN_WAIT, STALE_TURN,
        STRUCTURED_OUTPUT_CONFLICT, TRUSTED_RUN_COMMAND_REQUIRED, TRUSTED_SUBSCRIPTION_REQUIRED,
        TRUSTED_TIMER_REQUIRED, TURN_WAIT_TIMEOUT, UNKNOWN_SPAWN_OPTION,
    },
    orchestration::{OrchestrationServices, TrustedTarget},
    test_support::InMemoryFs,
};
use maki_providers::{
    AgentError, ContentBlock, Effort, Message, Model, ModelError, ModelInfo, ObservationSource,
    ProviderEvent, RequestOptions, Role, StreamResponse, ThinkingConfig,
    provider::{BoxFuture, Provider},
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

mod common;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const SYSTEM: &str = "deterministic child system";
const FIRST_REPLY: &str = "first gated reply";
const SECOND_REPLY: &str = "second gated reply";
const CAPTURE_TOOL: &str = "capture_result";
const CAPTURE_REPLY: &str = "structured response committed";
const UPDATED_MODEL: &str = "anthropic/claude-opus-4-20250514";
const FAST_MODEL: &str = "anthropic/claude-opus-4-8";
const MODEL_CATALOG_UNAVAILABLE: &str = "host model catalog is unavailable";
const TOOLS_ONLY_MODE: &str = "pr5_tools_only";

async fn bounded<T>(future: impl Future<Output = T>, operation: &str) -> T {
    futures_lite::future::race(future, async {
        smol::Timer::after(TEST_TIMEOUT).await;
        panic!("{operation} did not complete within {TEST_TIMEOUT:?}")
    })
    .await
}

struct CapturedRequest {
    model: String,
    messages: Vec<Message>,
    options: RequestOptions,
    system: String,
    tools: Value,
    reply: flume::Sender<StreamResponse>,
}

async fn request_with_command_diagnostics(
    requests: &flume::Receiver<CapturedRequest>,
    command: &flume::Receiver<Result<(), String>>,
    operation: &str,
) -> CapturedRequest {
    futures_lite::future::race(
        async {
            requests
                .recv_async()
                .await
                .expect("provider request channel closed")
        },
        async {
            smol::Timer::after(TEST_TIMEOUT).await;
            panic!(
                "{operation} did not arrive; command: {:?}",
                command.try_recv()
            );
        },
    )
    .await
}

async fn wait_for_gate_drop(request: &CapturedRequest, operation: &str) {
    bounded(
        async {
            while !request.reply.is_disconnected() {
                smol::future::yield_now().await;
            }
        },
        operation,
    )
    .await;
}

struct ScriptedProvider {
    requests: flume::Sender<CapturedRequest>,
}

struct RegisteredOrchestrationTargets {
    targets: Mutex<Vec<TrustedTarget>>,
    selected: AtomicUsize,
    models: Result<Vec<String>, String>,
}

impl RegisteredOrchestrationTargets {
    fn new(targets: Vec<TrustedTarget>) -> Self {
        Self {
            targets: Mutex::new(targets),
            selected: AtomicUsize::new(0),
            models: Err(MODEL_CATALOG_UNAVAILABLE.into()),
        }
    }
}

impl OrchestrationServices for RegisteredOrchestrationTargets {
    fn resolve_target(
        &self,
        _: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        let target = self
            .targets
            .lock()
            .unwrap()
            .get(self.selected.load(Ordering::SeqCst))
            .map(|registered| registered.target.clone());
        target
            .map(|target| {
                self.template(&target)
                    .map(|template| TrustedTarget { target, template })
            })
            .transpose()
    }

    fn register_target(&self, target: TrustedTarget) -> Result<(), String> {
        target.target.actor().map_err(|error| error.to_string())?;
        let mut targets = self.targets.lock().unwrap();
        if !targets.iter().any(|registered| {
            registered
                .target
                .manager()
                .same_manager(&target.target.manager())
        }) {
            return Err("unregistered test runtime".into());
        }
        if let Some(registered) = targets.iter_mut().find(|registered| {
            registered.target.id() == target.target.id()
                && registered
                    .target
                    .manager()
                    .same_manager(&target.target.manager())
        }) {
            *registered = target;
        } else {
            targets.push(target);
        }
        Ok(())
    }

    fn available_models(&self, target: &maki_agent::AgentRef) -> Result<Vec<String>, String> {
        let template = self.template(target)?;
        self.models.clone().map(|models| {
            models
                .into_iter()
                .filter(|spec| template.model_policy.allows(spec))
                .collect()
        })
    }

    fn template(&self, target: &maki_agent::AgentRef) -> Result<ToolContext, String> {
        let config = target
            .effective_config()
            .map_err(|error| error.to_string())?
            .ok_or("test target configuration is unavailable")?;
        let mut template = self
            .targets
            .lock()
            .unwrap()
            .iter()
            .find(|registered| {
                registered.target.id() == target.id()
                    && registered.target.manager().same_manager(&target.manager())
            })
            .map(|registered| registered.template.clone())
            .ok_or_else(|| "unregistered test target".to_owned())?;
        template.provider = Arc::clone(&config.provider);
        template.model = Arc::new(config.model.clone());
        template.opts.thinking = config.thinking;
        template.opts.fast = config.fast;
        template.workflow = config.workflow;
        template.mode = config.mode.clone();
        template.mode_def = config.mode_def.clone().map(Arc::new);
        Ok(template)
    }
}

impl Provider for ScriptedProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        tools: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        options: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let (reply, response) = flume::bounded(1);
            self.requests
                .send(CapturedRequest {
                    model: model.spec(),
                    messages: messages.to_vec(),
                    options,
                    system: system.into(),
                    tools: tools.clone(),
                    reply,
                })
                .map_err(|_| AgentError::Config {
                    message: "scripted request receiver closed".into(),
                })?;
            response.recv_async().await.map_err(|_| AgentError::Config {
                message: "scripted response gate closed".into(),
            })
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct LuaInvocationBackend {
    registry: Arc<ToolRegistry>,
    context: ToolContext,
    tool: &'static str,
    completed: flume::Sender<Result<(), String>>,
    observations: flume::Sender<(TurnId, TurnProvenance, Vec<Message>)>,
}

impl ActorBackend for LuaInvocationBackend {
    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut History,
        context: TurnContext,
        mut input: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(inbox) = &context.inbox {
                for instruction in inbox.drain_instructions() {
                    history.push(Message::user(instruction.message));
                }
                inbox.commit_completions(&mut |message| {
                    history.push(message.clone());
                    input.preamble.push(message);
                });
                inbox.instructions_submitted();
            }
            if input.preamble.iter().any(|message| {
                matches!(
                    message.observation_source,
                    Some(ObservationSource::ChildResult { .. })
                )
            }) {
                if let Some(inbox) = &context.inbox {
                    assert!(inbox.finish());
                }
                self.observations
                    .send((context.turn_id.unwrap(), context.provenance, input.preamble))
                    .unwrap();
                return BackendResult::EnteredRun(TurnOutcome::completed(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    Default::default(),
                    1,
                    DoneReason::EndTurn,
                ));
            }
            if let Some(config) = &context.policy {
                self.context.provider = Arc::clone(&config.provider);
                self.context.model = Arc::new(config.model.clone());
                self.context.opts.thinking = config.thinking;
                self.context.opts.fast = config.fast;
                self.context.workflow = config.workflow;
                self.context.mode = config.mode.clone();
                self.context.mode_def = config.mode_def.clone().map(Arc::new);
            }
            self.context.cancel = context.cancel.clone();
            self.context.managed_turn = context.managed_turn;
            let invocation = self
                .registry
                .get(self.tool)
                .unwrap()
                .tool
                .parse(&json!({}))
                .unwrap();
            let result = invocation.execute(&self.context).await.output.map(|_| ());
            let _ = self.completed.send(result.clone());
            if let Some(inbox) = &context.inbox {
                loop {
                    for instruction in inbox.drain_instructions() {
                        history.push(Message::user(instruction.message));
                    }
                    let mut observations = Vec::new();
                    inbox.commit_completions(&mut |message| {
                        history.push(message.clone());
                        observations.push(message);
                    });
                    if !observations.is_empty() {
                        self.observations
                            .send((
                                context.turn_id.unwrap(),
                                context.provenance.clone(),
                                observations,
                            ))
                            .unwrap();
                    }
                    inbox.instructions_submitted();
                    if inbox.finish() {
                        break;
                    }
                }
            }
            if result.is_err() {
                return BackendResult::SetupFailed {
                    agent_id: context.agent_id,
                    turn_id: context.turn_id.unwrap(),
                };
            }
            BackendResult::EnteredRun(TurnOutcome::completed(
                context.agent_id,
                context.turn_id.unwrap(),
                Default::default(),
                1,
                DoneReason::EndTurn,
            ))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

#[derive(Default)]
struct FixtureOptions {
    mode: Option<AgentMode>,
    modes: Vec<maki_agent::ModeDefSpec>,
    limits: AgentLimits,
    bundled_tasks: bool,
    authority_probes: bool,
    audience_ceiling: bool,
    nested_capture: bool,
    schema_inheritance: bool,
    plugin_source: String,
}

async fn settle_legacy_reply(manager: &AgentManagerHandle, request: &CapturedRequest, reply: &str) {
    let child = manager
        .snapshot()
        .into_iter()
        .find(|node| node.parent_id.is_some())
        .expect("legacy Session child missing");
    let events = manager.actor(child.agent_id).unwrap().subscribe_events();
    request.reply.send(common::canned_reply(reply)).unwrap();
    bounded(
        async { while !matches!(events.recv_async().await.unwrap(), ActorEvent::Idle { .. }) {} },
        "legacy Session idle publication",
    )
    .await;
}

async fn wait_for_child_reclamation(manager: &AgentManagerHandle, operation: &str) {
    let wait = async {
        loop {
            if manager
                .snapshot()
                .iter()
                .all(|node| node.parent_id.is_none())
            {
                return;
            }
            smol::future::yield_now().await;
        }
    };
    let watchdog = async {
        smol::Timer::after(TEST_TIMEOUT).await;
        let nodes: Vec<String> = manager
            .snapshot()
            .iter()
            .map(|node| {
                format!(
                    "{} parent={:?} graph={:?} actor={:?}",
                    node.agent_id,
                    node.parent_id,
                    node.graph_lifecycle,
                    node.actor.as_ref().map(|actor| (
                        actor.lifecycle,
                        actor.status,
                        actor.active_turn
                    ))
                )
            })
            .collect();
        panic!("{operation} did not complete: {nodes:?}");
    };
    futures_lite::future::race(wait, watchdog).await;
}

async fn shutdown_manager(manager: &AgentManagerHandle) {
    let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
    assert!(report.timed_out.is_empty(), "{report:?}");
}

struct Fixture {
    _host: PluginHost,
    _state_dir: tempfile::TempDir,
    manager: AgentManagerHandle,
    requests: flume::Receiver<CapturedRequest>,
    completed: flume::Receiver<Result<(), String>>,
    observations: flume::Receiver<(TurnId, TurnProvenance, Vec<Message>)>,
    root_ticket: maki_agent::TurnTicket,
}

impl Fixture {
    fn start(tool: &'static str, handler: &str) -> Self {
        Self::start_with_mode(tool, handler, AgentMode::Build)
    }

    fn start_with_mode(tool: &'static str, handler: &str, mode: AgentMode) -> Self {
        Self::start_with_options(
            tool,
            FixtureOptions {
                mode: Some(mode),
                ..Default::default()
            },
            handler,
        )
    }

    fn start_with_options(tool: &'static str, options: FixtureOptions, handler: &str) -> Self {
        let FixtureOptions {
            mode,
            modes,
            limits,
            bundled_tasks,
            authority_probes,
            audience_ceiling,
            nested_capture,
            schema_inheritance,
            plugin_source,
        } = options;
        let mode = mode.unwrap_or(AgentMode::Build);
        let registry = Arc::clone(ToolRegistry::global_arc());
        let (requests_tx, requests) = flume::unbounded();
        let provider = Arc::new(ScriptedProvider {
            requests: requests_tx,
        });
        let prepared_provider: Arc<dyn Provider> = provider.clone();
        let state_dir = tempfile::tempdir().unwrap();
        let host = PluginHost::with_session_provider_preparer(
            Arc::clone(&registry),
            Default::default(),
            true,
            Arc::new(InMemoryFs::new()),
            Some(state_dir.path().to_path_buf()),
            Some(Arc::new(move |model| {
                Ok((model, Arc::clone(&prepared_provider)))
            })),
        )
        .unwrap();
        if bundled_tasks {
            host.load_source(
                "pr5_bundled_task",
                include_str!("../../plugins/task/init.lua"),
            )
            .unwrap();
        }
        host.load_source(
            tool,
            &format!(
                r#"
            {plugin_source}
            if {audience_ceiling} then
                maki.api.register_tool({{
                    name = "{tool}_sub", description = "subagent-only registry tool",
                    schema = {{ type = "object", properties = {{}} }}, audiences = {{ "general_sub" }},
                    handler = function() error("audience capability escalated") end,
                }})
                maki.api.register_tool({{
                    name = "{tool}_unpublished", description = "unadvertised registry tool",
                    schema = {{ type = "object", properties = {{}} }}, audiences = {{ "main", "general_sub" }},
                    handler = function() error("request capability escalated") end,
                }})
            end
            if {nested_capture} then
                maki.api.register_tool({{
                    name = "pr6_spawn_grandchild", description = "exercise inherited capture",
                    schema = {{ type = "object", properties = {{}} }}, audiences = {{ "main", "general_sub" }},
                    handler = function(_, child_ctx)
                        local grandchild = assert(maki.agent.spawn(child_ctx, {{ inherit_provider = true, silent = true }}))
                        local first = assert(grandchild:send(child_ctx, "grandchild capture"))
                        local result = assert(first:wait(child_ctx))
                        assert(#result.output == 1 and result.output[1].answer == 42 and result.captured.answer == 42)
                        local second = assert(grandchild:send(child_ctx, "grandchild capture again"))
                        local next_result = assert(second:wait(child_ctx))
                        assert(#next_result.output == 1 and next_result.output[1].answer == 7 and next_result.captured.answer == 7)
                        local frozen = assert(first:result(child_ctx))
                        assert(#frozen.output == 1 and frozen.output[1].answer == 42 and frozen.captured.answer == 42)
                        assert(grandchild:close(child_ctx))
                        local closed = assert(first:result(child_ctx))
                        assert(#closed.output == 1 and closed.output[1].answer == 42 and closed.captured.answer == 42)
                        return "grandchild complete"
                    end,
                }})
            end
            if {schema_inheritance} then
                maki.api.register_tool({{
                    name = "pr5_spawn_schema_children", description = "spawn children from a schema-enabled parent",
                    schema = {{ type = "object", properties = {{}} }}, audiences = {{ "main", "general_sub" }},
                    handler = function(input, ctx) return pr5_schema_children_handler(input, ctx) end,
                }})
            end
            if {authority_probes} then
            maki.api.register_tool({{
                name = "{tool}_probe", description = "probe executing authority",
                schema = {{ type = "object", properties = {{}} }}, audiences = {{ "main", "general_sub" }},
                handler = function(input, ctx) return pr5_child_authority_handler(input, ctx) end,
            }})
            maki.api.register_tool({{
                name = "{tool}_write", description = "nested write restriction probe",
                schema = {{ type = "object", properties = {{ path = {{ type = "string" }} }} }},
                audiences = {{ "main", "general_sub" }},
                mutable_path = "path",
                handler = function()
                    pr5_nested_write_entered = true
                    return "escaped"
                end,
            }})
            end
            maki.api.register_tool({{
                name = "{tool}", description = "exercise public agent handles",
                schema = {{ type = "object", properties = {{}} }},
                handler = function(_, ctx)
                    {handler}
                    return "ok"
                end,
            }})
        "#
            ),
        )
        .unwrap();
        let (mut context, _events, _cancel) = common::ctx_with_provider(provider);
        context.mode = mode.clone();
        context.registry = Arc::clone(&registry);
        context.turn_bindings = Arc::new(maki_agent::tools::TurnToolBindings::capture(
            &registry,
            &context.local_tools,
            context.mcp.as_ref(),
        ));
        for definition in modes {
            context.modes.define(definition).unwrap();
        }
        let mut definition = context.modes.current(&mode);
        if authority_probes {
            definition.tools = Some(vec![
                tool.into(),
                format!("{tool}_probe"),
                format!("{tool}_write"),
            ]);
            context
                .modes
                .define(maki_agent::ModeDefSpec {
                    name: mode.id().key().into(),
                    tools: definition.tools.clone(),
                    restrict_write_to: context.restrict_write_to(),
                    ..Default::default()
                })
                .unwrap();
            definition = context.modes.current(&mode);
        }
        context.mode_def = Some(Arc::new(definition.clone()));
        let mut definitions = vec![json!({
            "name": tool, "description": "exercise public agent handles",
            "input_schema": { "type": "object", "properties": {} },
        })];
        if authority_probes {
            definitions.push(json!({
                "name": format!("{tool}_probe"), "description": "probe executing authority",
                "input_schema": { "type": "object", "properties": {} },
            }));
        }
        if authority_probes {
            definitions.push(json!({ "name": format!("{tool}_write"), "description": "nested write", "input_schema": { "type": "object", "properties": {} } }));
        }
        if audience_ceiling {
            definitions.push(json!({ "name": format!("{tool}_sub"), "description": "subagent-only registry tool", "input_schema": { "type": "object", "properties": {} } }));
        }
        if nested_capture {
            definitions.push(json!({ "name": "pr6_spawn_grandchild", "description": "spawn grandchild", "input_schema": { "type": "object" } }));
        }
        if schema_inheritance {
            definitions.push(json!({ "name": "pr5_spawn_schema_children", "description": "spawn schema children", "input_schema": { "type": "object", "properties": {} } }));
        }
        if bundled_tasks {
            for name in ["task_spawn", "task_send", "task_wait", "task_despawn"] {
                definitions.push(json!({ "name": name, "description": "managed task", "input_schema": { "type": "object" } }));
            }
        }
        let request_tools = maki_agent::tools::RequestTools::assembled(
            Value::Array(definitions),
            &context.config,
            &context.model,
        );
        context.tool_filter = Arc::clone(request_tools.filter());
        context.request_tools = Some(request_tools);
        let policy = EffectiveAgentConfig::new(
            RunSettings {
                provider: Arc::clone(&context.provider),
                model: (*context.model).clone(),
                fast: false,
                workflow: context.workflow,
                thinking: Default::default(),
            },
            mode.clone(),
        )
        .with_mode_def(context.mode_def.as_deref().cloned());
        let manager = AgentManagerHandle::new(limits).unwrap();
        let (completed_tx, completed) = flume::bounded(1);
        let (observations_tx, observations) = flume::unbounded();
        let template = context.clone();
        let root = manager
            .create_root_with_config(Some(policy), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaInvocationBackend {
                    registry,
                    context,
                    tool,
                    completed: completed_tx,
                    observations: observations_tx,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        host.event_handle().install_orchestration_services(Arc::new(
            RegisteredOrchestrationTargets::new(vec![TrustedTarget {
                target: root.clone(),
                template,
            }]),
        ));
        host.wait_for_worker_barrier_for_test();
        let root_ticket = root
            .actor()
            .unwrap()
            .admit_turn(
                AgentInput {
                    message: "exercise handles".into(),
                    mode,
                    images: Vec::new(),
                    preamble: Vec::new(),
                    thinking: Default::default(),
                    fast: false,
                    workflow: false,
                    prompt: None,
                    cancel: None,
                    lease_committer: None,
                },
                None,
                tool.into(),
            )
            .unwrap();
        Self {
            _host: host,
            _state_dir: state_dir,
            manager,
            requests,
            completed,
            observations,
            root_ticket,
        }
    }

    async fn request(&self) -> CapturedRequest {
        futures_lite::future::race(
            async { self.requests.recv_async().await.unwrap() },
            futures_lite::future::race(
                async {
                    let completed = self.completed.recv_async().await.unwrap();
                    panic!("Lua handler finished before expected provider request: {completed:?}")
                },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("provider request never arrived")
                },
            ),
        )
        .await
    }

    async fn finish(self) {
        let result = futures_lite::future::race(
            async { self.completed.recv_async().await.unwrap() },
            async {
                smol::Timer::after(TEST_TIMEOUT).await;
                panic!("Lua handler never completed")
            },
        )
        .await;
        assert_eq!(result, Ok(()));
        assert!(matches!(
            bounded(self.root_ticket.wait(), "root turn settlement").await,
            TurnOutcome::Completed { .. }
        ));
        assert!(
            self.requests.is_empty(),
            "unexpected extra provider request"
        );
        shutdown_manager(&self.manager).await;
    }
}

#[test_case::test_case(0; "zero_timeout")]
#[test_case::test_case(1; "positive_timeout")]
fn public_agent_prompt_timeout_retains_recoverable_ticket(timeout: u64) {
    smol::block_on(async {
        let tool = if timeout == 0 {
            "pr7_prompt_zero"
        } else {
            "pr7_prompt_positive"
        };
        let fixture = Fixture::start(
            tool,
            &format!(
                r#"
            local child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            assert(not pcall(child.prompt, child, ctx, "malformed timeout", {{ timeout = -1 }}))
            local untouched = assert(child:status(ctx))
            assert(untouched.queued == 0 and untouched.active_turn == nil)
            local result, err = child:prompt(ctx, "held prompt", {{ timeout = {timeout} }})
            assert(result == nil and type(err) == "table")
            assert(err.kind == "timeout" and err.message == "{TURN_WAIT_TIMEOUT}")
            assert(err.ticket:agent_id() == child:id())
            assert(err.ticket:result(ctx) == nil)
            local checkpoint = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            assert(assert(checkpoint:prompt(ctx, "timeout observed")).text == "{SECOND_REPLY}")
            local recovered = assert(err.ticket:wait(ctx))
            assert(recovered.text == "{FIRST_REPLY}" and recovered.status == "completed")
            assert(assert(err.ticket:result(ctx)).text == recovered.text)
            assert(assert(child:prompt(ctx, "successful prompt")).text == "{SECOND_REPLY}")
            assert(child:close(ctx))
            assert(checkpoint:close(ctx))
        "#
            ),
        );
        let a = fixture.request().await;
        let b = fixture.request().await;
        let (held, checkpoint) = if serde_json::to_string(&a.messages)
            .unwrap()
            .contains("held prompt")
        {
            (a, b)
        } else {
            (b, a)
        };
        assert!(
            serde_json::to_string(&checkpoint.messages)
                .unwrap()
                .contains("timeout observed")
        );
        assert!(
            !held.reply.is_disconnected(),
            "timeout cancelled the admitted turn"
        );
        held.reply.send(common::canned_reply(FIRST_REPLY)).unwrap();
        checkpoint
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        let successful = fixture.request().await;
        assert!(
            serde_json::to_string(&successful.messages)
                .unwrap()
                .contains("successful prompt")
        );
        successful
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn trusted_model_catalog_filters_destination_policy_and_preserves_error_pair() {
    smol::block_on(async {
        let fixture = Fixture::start("pr7_catalog_root", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "root setup")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(matches!(
            bounded(fixture.root_ticket.wait(), "root settlement").await,
            TurnOutcome::Completed { .. }
        ));
        let handle = fixture._host.event_handle();
        fixture._host.load_source("pr7_catalog", &format!(r#"
            maki.api.register_command({{
                name = "/pr7_catalog_error", tui_only = false,
                handler = function(_, ctx)
                    local models, err = assert(maki.agent.current(ctx)):available_models(ctx)
                    assert(models == nil and err == "{MODEL_CATALOG_UNAVAILABLE}")
                end,
            }})
            maki.api.register_command({{
                name = "/pr7_catalog_filtered", tui_only = false,
                handler = function(_, ctx)
                    local root = assert(maki.agent.current(ctx))
                    local before = assert(root:model(ctx))
                    local models, err = root:available_models(ctx)
                    assert(err == nil and #models == 1 and models[1] == "{FAST_MODEL}")
                    local after = assert(root:model(ctx))
                    assert(before.spec == after.spec and before.thinking == after.thinking and before.fast == after.fast)
                    assert(assert(maki.agent.current(ctx)):id() == root:id())
                end,
            }})
        "#)).unwrap();
        assert_eq!(
            handle
                .run_command_for_test(
                    Arc::from("pr7_catalog"),
                    Arc::from("/pr7_catalog_error"),
                    String::new(),
                    0
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        let (mut template, _events, _cancel) = common::ctx_with_canned_provider();
        template.model_policy = Arc::new(
            maki_config::ModelPolicy::new(&["anthropic/*".into()], &[UPDATED_MODEL.into()])
                .unwrap(),
        );
        let mut services = RegisteredOrchestrationTargets::new(vec![TrustedTarget {
            target: fixture.manager.root().unwrap(),
            template,
        }]);
        services.models = Ok(vec![
            UPDATED_MODEL.into(),
            FAST_MODEL.into(),
            "openai/gpt-4.1".into(),
        ]);
        handle.install_orchestration_services(Arc::new(services));
        fixture._host.wait_for_worker_barrier_for_test();
        assert_eq!(
            handle
                .run_command_for_test(
                    Arc::from("pr7_catalog"),
                    Arc::from("/pr7_catalog_filtered"),
                    String::new(),
                    0
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test_case::test_case(false; "plugin_unload")]
#[test_case::test_case(true; "manager_shutdown")]
fn trusted_config_callback_survives_subscription_gc_and_revocation(shutdown: bool) {
    smol::block_on(async {
        let fixture = Fixture::start(
            if shutdown {
                "pr7_gc_shutdown_root"
            } else {
                "pr7_gc_root"
            },
            "",
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "root setup")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(matches!(
            bounded(fixture.root_ticket.wait(), "root settlement").await,
            TurnOutcome::Completed { .. }
        ));
        let handle = fixture._host.event_handle();
        fixture._host.load_source("pr7_gc_config", &format!(r#"
            maki.api.register_command({{
                name = "/pr7_gc_config", tui_only = false,
                handler = function(_, ctx)
                    local root = assert(maki.agent.current(ctx))
                    local selection = assert(root:set_model(ctx, {{ spec = "{FAST_MODEL}", thinking = "low", fast = true }}))
                    assert(selection.spec == "{FAST_MODEL}" and selection.thinking == "low" and selection.fast == true)
                    local roundtrip = assert(root:model(ctx))
                    for _, key in ipairs({{ "spec", "thinking", "fast" }}) do assert(selection[key] == roundtrip[key]) end
                    assert(root:mode(ctx) == "build")
                    local subscription = assert(root:on_config_change(ctx, function(event, callback_ctx)
                        assert(event.agent_id == root:id() and event.mode == "plan" and event.generation > 0)
                        for _, key in ipairs({{ "spec", "thinking", "fast" }}) do assert(event.selection[key] == selection[key]) end
                        assert(root:mode(callback_ctx) == "plan")
                        local child = assert(maki.agent.spawn(callback_ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                        assert(callback_ctx:defer_close(child))
                        assert(child:prompt(callback_ctx, "callback delivered after GC"))
                    end))
                    subscription = nil
                end,
            }})
            maki.api.register_command({{
                name = "/pr7_gc_deliver", tui_only = false,
                handler = function(_, ctx)
                    local root = assert(maki.agent.current(ctx))
                    local selection = assert(root:model(ctx))
                    local listed_root
                    for _, reference in ipairs(assert(maki.agent.list(ctx))) do
                        if reference:id() == root:id() then listed_root = assert(maki.agent.get(ctx, reference)) end
                    end
                    assert(listed_root:set_mode(ctx, "plan") == "plan")
                    assert(root:mode(ctx) == "plan")
                    local visible = assert(maki.agent.list(ctx))
                    local found = false
                    for _, reference in ipairs(visible) do
                        local agent = assert(maki.agent.get(ctx, reference))
                        if agent:id() == root:id() then
                            found = true
                            assert(agent:mode(ctx) == "plan")
                            assert(assert(agent:model(ctx)).spec == selection.spec)
                        end
                    end
                    assert(found)
                end,
            }})
        "#)).unwrap();
        let setup = handle.run_command_for_test(
            Arc::from("pr7_gc_config"),
            Arc::from("/pr7_gc_config"),
            String::new(),
            0,
        );
        assert_eq!(setup.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        fixture._host.collect_garbage_for_test().unwrap();
        let command = handle.run_command_for_test(
            Arc::from("pr7_gc_config"),
            Arc::from("/pr7_gc_deliver"),
            String::new(),
            0,
        );
        let callback = request_with_command_diagnostics(
            &fixture.requests,
            &command,
            "callback after subscription GC",
        )
        .await;
        assert!(
            serde_json::to_string(&callback.messages)
                .unwrap()
                .contains("callback delivered after GC")
        );
        assert_eq!(command.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        if shutdown {
            shutdown_manager(&fixture.manager).await;
        } else {
            fixture._host.unload("pr7_gc_config").unwrap();
        }
        wait_for_gate_drop(
            &callback,
            "revocation cancels collected subscription handler",
        )
        .await;
        if !shutdown {
            shutdown_manager(&fixture.manager).await;
        }
    });
}

#[test]
fn public_agent_tickets_retain_exact_results_across_queued_turns() {
    smol::block_on(async {
        let busy_error = AGENT_BUSY;
        let fixture = Fixture::start(
            "pr5_ticket_roundtrip",
            &format!(
                r#"
            local child = assert(maki.agent.spawn(ctx, {{
                inherit_provider = true, system = "{SYSTEM}", model = {{ thinking = "off" }}, silent = true, tools = {{}},
            }}))
            local id = assert(child:id())
            assert(assert(maki.agent.get(ctx, assert(child:ref()))):id() == id)
            local first = assert(child:send(ctx, "first prompt"))
            local busy, busy_err = child:send(ctx, "must not run")
            assert(busy == nil and busy_err == {busy_error:?}, busy_err)
            local second = assert(child:enqueue(ctx, "second prompt"))
            assert(first:id() ~= second:id())
            assert(first:agent_id() == id and second:agent_id() == id)
            local pending, pending_err = second:result(ctx)
            assert(pending == nil and pending_err == nil)
            local invalid, invalid_err = first:wait(ctx, {{ timeout = 0 }})
            assert(invalid == nil and invalid_err == nil)
            local checkpoint = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            assert(assert(assert(checkpoint:send(ctx, "checks complete")):wait(ctx)).text == "checks complete")
            assert(checkpoint:close(ctx))
            assert(assert(first:wait(ctx)).text == "{FIRST_REPLY}")
            assert(assert(second:wait(ctx)).text == "{SECOND_REPLY}")
            assert(assert(first:result(ctx)).text == "{FIRST_REPLY}")
            assert(assert(first:wait(ctx)).text == "{FIRST_REPLY}")
            assert(assert(second:result(ctx)).text == "{SECOND_REPLY}")
            assert(child:close(ctx))
        "#
            ),
        );
        let request_a = fixture.request().await;
        let request_b = fixture.request().await;
        let (first, checkpoint) = if serde_json::to_string(&request_a.messages)
            .unwrap()
            .contains("first prompt")
        {
            (request_a, request_b)
        } else {
            (request_b, request_a)
        };
        assert!(
            serde_json::to_string(&checkpoint.messages)
                .unwrap()
                .contains("checks complete")
        );
        checkpoint
            .reply
            .send(common::canned_reply("checks complete"))
            .unwrap();
        assert_eq!(first.model, common::default_model().spec());
        assert_eq!(first.options.thinking, maki_providers::ThinkingConfig::Off);
        assert!(!first.options.fast);
        assert!(first.system.contains(SYSTEM));
        assert!(common::tool_names(&first.tools).is_empty());
        assert!(
            serde_json::to_string(&first.messages)
                .unwrap()
                .contains("first prompt")
        );
        assert!(
            fixture.requests.is_empty(),
            "queued turn started before gated turn finished"
        );
        first.reply.send(common::canned_reply(FIRST_REPLY)).unwrap();
        let second = fixture.request().await;
        let history = serde_json::to_string(&second.messages).unwrap();
        assert!(history.contains(FIRST_REPLY));
        assert!(history.contains("second prompt"));
        assert!(!history.contains("must not run"));
        second
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn public_agent_model_change_controls_next_request_without_losing_history() {
    smol::block_on(async {
        let stale_error = STALE_TURN;
        let fixture = Fixture::start(
            "pr5_model_change",
            &format!(
                r#"
            local child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            local first = assert(child:send(ctx, "before model change"))
            assert(assert(first:wait(ctx)).text == "{FIRST_REPLY}")
            assert(child:set_model(ctx, "{UPDATED_MODEL}"))
            local second = assert(child:send(ctx, "after model change", {{ after_turn = first:id() }}))
            assert(assert(second:wait(ctx)).text == "{SECOND_REPLY}")
            local stale, stale_err = child:send(ctx, "stale checkpoint must not run", {{ after_turn = first:id() }})
            assert(stale == nil and stale_err == {stale_error:?}, stale_err)
            local transcript = assert(child:transcript(ctx, {{ through_turn = first:id(), last_messages = 100, max_bytes = 65536 }}))
            assert(transcript.through_turn == first:id())
            assert(transcript.truncated == false)
            assert(#transcript.messages == 2)
            local latest = assert(child:transcript(ctx, {{ last_messages = 1, max_bytes = 65536 }}))
            assert(latest.truncated == true and #latest.messages == 1)
            assert(latest.omitted_messages >= 3)
            assert(assert(first:result(ctx)).text == "{FIRST_REPLY}")
            assert(child:close(ctx))
        "#
            ),
        );
        let first = fixture.request().await;
        assert_eq!(first.model, common::default_model().spec());
        first.reply.send(common::canned_reply(FIRST_REPLY)).unwrap();
        let second = fixture.request().await;
        assert_eq!(second.model, UPDATED_MODEL);
        let history = serde_json::to_string(&second.messages).unwrap();
        assert!(history.contains(FIRST_REPLY));
        assert!(history.contains("after model change"));
        second
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test_case::test_case(Some(SYSTEM); "custom_system")]
#[test_case::test_case(None; "empty_custom_system")]
fn public_child_mode_changes_provider_system_after_queued_turns(custom: Option<&str>) {
    const OWNER: &str = "pr160_mode_system";
    const COMMAND: &str = "/pr160_mode_system";
    const OLD_MODE: &str = "pr160_old_mode";
    const NEW_MODE: &str = "pr160_new_mode";
    const OLD_PROMPT: &str = "old mode directive";
    const NEW_PROMPT: &str = "new mode directive";
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            OWNER,
            FixtureOptions {
                mode: Some(AgentMode::Custom(ModeId::parse(OLD_MODE))),
                modes: vec![
                    maki_agent::ModeDefSpec {
                        name: OLD_MODE.into(),
                        system_prompt: Some(OLD_PROMPT.into()),
                        ..Default::default()
                    },
                    maki_agent::ModeDefSpec {
                        name: NEW_MODE.into(),
                        system_prompt: Some(NEW_PROMPT.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            "",
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "mode setup")
                .await
                .unwrap(),
            Ok(())
        );
        bounded(fixture.root_ticket.wait(), "mode root settlement").await;
        let system_option = custom
            .map(|system| format!("system = {system:?},"))
            .unwrap_or_default();
        fixture._host.load_source(OWNER, &format!(r#"
            maki.api.register_command({{
                name = "{COMMAND}", tui_only = false,
                handler = function(_, ctx)
                    local child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}}, {system_option} }}))
                    local first = assert(child:send(ctx, "old mode first"))
                    local queued = assert(child:enqueue(ctx, "old mode queued"))
                    assert(child:set_mode(ctx, "{NEW_MODE}") == "{NEW_MODE}")
                    assert(assert(first:wait(ctx)).text == "{FIRST_REPLY}")
                    assert(assert(queued:wait(ctx)).text == "{SECOND_REPLY}")
                    local latest = assert(child:send(ctx, "new mode turn"))
                    assert(assert(latest:wait(ctx)).text == "{FIRST_REPLY}")
                    assert(assert(first:result(ctx)).text == "{FIRST_REPLY}")
                    assert(child:close(ctx))
                end,
            }})
        "#)).unwrap();
        let command = fixture._host.event_handle().run_command_for_test(
            Arc::from(OWNER),
            Arc::from(COMMAND),
            String::new(),
            0,
        );
        for (prompt, reply) in [
            (OLD_PROMPT, FIRST_REPLY),
            (OLD_PROMPT, SECOND_REPLY),
            (NEW_PROMPT, FIRST_REPLY),
        ] {
            let request =
                request_with_command_diagnostics(&fixture.requests, &command, "child mode request")
                    .await;
            let expected = custom.map_or_else(
                || prompt.to_owned(),
                |system| format!("{prompt}\n\n{system}"),
            );
            assert_eq!(request.system, expected);
            request.reply.send(common::canned_reply(reply)).unwrap();
        }
        assert_eq!(
            bounded(command.recv_async(), "mode command completion")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(fixture.requests.is_empty());
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn required_output_schema_rejects_excluded_mode_without_allocating_and_preserves_mode() {
    const OWNER: &str = "pr160_schema_policy";
    const COMMAND: &str = "/pr160_schema_policy";
    const DENIED_MODE: &str = "pr160_schema_denied";
    const ALLOWED_MODE: &str = "pr160_schema_allowed";
    const POLICY_ERROR: &str =
        "output_schema requires tool 'structured_output' but the selected mode excludes it";
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            OWNER,
            FixtureOptions {
                mode: Some(AgentMode::Custom(ModeId::parse(DENIED_MODE))),
                modes: vec![
                    maki_agent::ModeDefSpec {
                        name: DENIED_MODE.into(),
                        tools: Some(vec![OWNER.into()]),
                        ..Default::default()
                    },
                    maki_agent::ModeDefSpec {
                        name: ALLOWED_MODE.into(),
                        tools: Some(vec!["structured_output".into()]),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            "",
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "schema setup")
                .await
                .unwrap(),
            Ok(())
        );
        bounded(fixture.root_ticket.wait(), "schema root settlement").await;
        fixture._host.load_source(OWNER, &format!(r#"
            maki.api.register_command({{
                name = "{COMMAND}", tui_only = false,
                handler = function(_, ctx)
                    local opts = {{ inherit_provider = true, silent = true, output_schema = {{ type = "object", properties = {{ answer = {{ type = "integer" }} }}, required = {{ "answer" }} }} }}
                    local before = #assert(maki.agent.list(ctx))
                    for _, constructor in ipairs({{ maki.agent.spawn, maki.agent.session }}) do
                        local child, err = constructor(ctx, opts)
                        assert(child == nil and err == {POLICY_ERROR:?}, err)
                        assert(#assert(maki.agent.list(ctx)) == before)
                    end
                    local root = assert(maki.agent.current(ctx))
                    assert(root:set_mode(ctx, "{ALLOWED_MODE}") == "{ALLOWED_MODE}")
                    local child = assert(maki.agent.spawn(ctx, opts))
                    local mode, err = child:set_mode(ctx, "{DENIED_MODE}")
                    assert(mode == nil and err == {POLICY_ERROR:?}, err)
                    assert(child:mode(ctx) == "{ALLOWED_MODE}")
                    local turn = assert(child:send(ctx, "required schema under allowed mode"))
                    local result = assert(turn:wait(ctx))
                    assert(result.status == "completed" and result.captured.answer == 42)
                    assert(child:close(ctx))
                    assert(assert(turn:result(ctx)).captured.answer == 42)
                    local session = assert(maki.agent.session(ctx, opts))
                    local found = false
                    for _, reference in ipairs(assert(maki.agent.list(ctx))) do
                        if reference:id() ~= root:id() then
                            local controlled = assert(maki.agent.get(ctx, reference))
                            local changed, change_err = controlled:set_mode(ctx, "{DENIED_MODE}")
                            assert(changed == nil and change_err == {POLICY_ERROR:?}, change_err)
                            assert(controlled:mode(ctx) == "{ALLOWED_MODE}")
                            found = true
                        end
                    end
                    assert(found)
                    assert(session:close())
                end,
            }})
        "#)).unwrap();
        let command = fixture._host.event_handle().run_command_for_test(
            Arc::from(OWNER),
            Arc::from(COMMAND),
            String::new(),
            0,
        );
        let request =
            request_with_command_diagnostics(&fixture.requests, &command, "allowed schema request")
                .await;
        assert_eq!(
            common::tool_names(&request.tools),
            vec!["structured_output"]
        );
        assert!(
            serde_json::to_string(&request.messages)
                .unwrap()
                .contains("required schema under allowed mode")
        );
        request
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 42 }),
            ))
            .unwrap();
        let completion = request_with_command_diagnostics(
            &fixture.requests,
            &command,
            "allowed schema completion",
        )
        .await;
        completion
            .reply
            .send(common::canned_reply(CAPTURE_REPLY))
            .unwrap();
        assert_eq!(
            bounded(command.recv_async(), "schema command completion")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(fixture.requests.is_empty());
        shutdown_manager(&fixture.manager).await;
    });
}

#[test_case::test_case(AgentMode::Build, "pr5_tool_authority_build", false, false ; "build")]
#[test_case::test_case(AgentMode::Plan("/test/authority-plan.md".into()), "pr5_tool_authority_plan", true, true ; "restricted_plan")]
#[test_case::test_case(AgentMode::Custom(ModeId::Custom(TOOLS_ONLY_MODE.into())), "pr5_tool_authority_tools_only", true, false ; "tools_only_custom")]
fn executing_child_tool_cannot_control_ancestor_or_sibling(
    mode: AgentMode,
    tool: &'static str,
    mode_locked: bool,
    write_restricted: bool,
) {
    smol::block_on(async {
        let mode_key = mode.id().key().to_owned();
        let probe = format!("{tool}_probe");
        let (parent_id, child_id) = (AgentId::generate(), AgentId::generate());
        let not_descendant = ManagerError::NotDescendant {
            parent_id,
            child_id,
        }
        .to_string();
        let (parent_placeholder, child_placeholder) = (parent_id.to_string(), child_id.to_string());
        let fixture = Fixture::start_with_options(
            tool,
            FixtureOptions {
                mode: Some(mode),
                authority_probes: true,
                ..Default::default()
            },
            &format!(
                r#"
            local parent = assert(maki.agent.current(ctx))
            local parent_id = assert(parent:id())
            local sibling = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            local sibling_ref = assert(sibling:ref())
            pr5_child_authority_handler = function(_, child_ctx)
                    local current = assert(maki.agent.current(child_ctx))
                    assert(current:id() ~= parent_id)
                    local function not_descendant(target)
                        return (({not_descendant:?}):gsub({child_placeholder:?}, target:id()):gsub({parent_placeholder:?}, current:id()))
                    end
                    if {mode_locked} then
                        assert(assert(current:mode(child_ctx)) == {mode_key:?})
                        local changed, mode_err = current:set_mode(child_ctx, "build")
                        assert(changed == nil and mode_err == {RESTRICTED_MODE_CHANGE:?}, mode_err)
                    end
                    if {write_restricted} then
                        local written, write_err = maki.agent.call_tool(child_ctx, "{tool}_write", {{ path = "/test/forbidden.txt" }})
                        assert(written == nil and write_err == {PLAN_WRITE_RESTRICTED:?}, write_err)
                        assert(not pr5_nested_write_entered)
                    end
                    for _, target in ipairs({{ parent, sibling }}) do
                        local denied = not_descendant(target)
                        local status, status_err = target:status(child_ctx)
                        assert(status == nil and status_err == {NOT_DESCENDANT_OF_CALLER:?}, status_err)
                        local sent, send_err = target:send(child_ctx, "forbidden")
                        assert(sent == nil and send_err == denied, send_err)
                        local cancelled, cancel_err = target:cancel(child_ctx)
                        assert(cancelled == nil and cancel_err == denied, cancel_err)
                        local closed, close_err = target:close(child_ctx)
                        assert(closed == nil and close_err == denied, close_err)
                        local changed, mode_err = target:set_mode(child_ctx, "build")
                        assert(changed == nil and mode_err == denied, mode_err)
                        local updated, model_err = target:set_model(child_ctx, "{UPDATED_MODEL}")
                        assert(updated == nil and model_err == denied, model_err)
                    end
                    local visible = {{}}
                    for _, reference in ipairs(assert(maki.agent.list(child_ctx))) do
                        visible[reference:id()] = reference
                    end
                    assert(visible[parent_id] ~= nil and visible[current:id()] ~= nil)
                    assert(visible[sibling:id()] == nil)
                    assert(assert(maki.agent.get(child_ctx, visible[current:id()])):id() == current:id())
                    local listed_parent, listed_err = maki.agent.get(child_ctx, visible[parent_id])
                    assert(listed_parent == nil and listed_err == not_descendant(parent), listed_err)
                    local foreign, foreign_err = maki.agent.get(child_ctx, sibling_ref)
                    assert(foreign == nil and foreign_err == not_descendant(sibling), foreign_err)
                    local root_ref = assert(maki.agent.root(child_ctx))
                    local ancestor, ancestor_err = maki.agent.get(child_ctx, root_ref)
                    assert(ancestor == nil and ancestor_err == not_descendant(parent), ancestor_err)
                    return "scope enforced"
                end
            local callable = assert(maki.agent.callable_tools(ctx))
            local found = false
            for _, entry in ipairs(callable) do if entry.name == "{probe}" then found = true end end
            assert(found, "parent callable probe missing before child preparation")
            local child = assert(maki.agent.spawn(ctx, {{
                inherit_provider = true, silent = true,
                tools = {{
                    {{ name = "{probe}", description = "check scope", input_schema = {{ type = "object", properties = {{}} }} }},
                    {{ name = "{tool}_write", description = "nested write", input_schema = {{ type = "object", properties = {{}} }} }},
                }},
            }}))
            local ticket = assert(child:send(ctx, "check authority"))
            assert(assert(ticket:wait(ctx)).text == "{SECOND_REPLY}")
            assert(assert(sibling:status(ctx)).status == "idle")
            assert(child:close(ctx))
            for _ = 1, 3 do
                assert(assert(ticket:result(ctx)).text == "{SECOND_REPLY}")
                assert(assert(ticket:wait(ctx)).text == "{SECOND_REPLY}")
            end
            assert(sibling:close(ctx))
        "#
            ),
        );
        let first = fixture.request().await;
        assert!(
            common::tool_names(&first.tools).contains(&probe),
            "child admission filtered probe before dispatch (mode={mode_key}): {:?}",
            first.tools
        );
        first
            .reply
            .send(common::canned_tool_use(&probe, json!({})))
            .unwrap();
        let second = fixture.request().await;
        assert!(
            serde_json::to_string(&second.messages)
                .unwrap()
                .contains("scope enforced"),
            "child tool response: {:?}",
            second.messages
        );
        second
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn trusted_command_controls_real_root_and_rejects_foreign_task_context() {
    smol::block_on(async {
        let fixture = Fixture::start("pr5_trusted_root_setup", "");
        assert_eq!(
            bounded(
                fixture.completed.recv_async(),
                "integration channel receive"
            )
            .await
            .unwrap(),
            Ok(())
        );
        assert!(matches!(
            bounded(fixture.root_ticket.wait(), "fixture root settlement").await,
            TurnOutcome::Completed { .. }
        ));
        let provider = Arc::new(ScriptedProvider {
            requests: flume::unbounded().0,
        });
        let (template, _events, _cancel) = common::ctx_with_provider(provider);
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: fixture.manager.root().unwrap(),
                template,
            },
        ])));
        fixture._host.load_source("pr5_trusted_command", &format!(r#"
            local retained
            maki.api.register_command({{
                name = "/pr5_capture", description = "retain an invocation-bound context", tui_only = false,
                handler = function(_, ctx)
                    assert(ctx ~= nil)
                    local root = assert(maki.agent.current(ctx))
                    assert(root:id() == assert(maki.agent.root(ctx)):id())
                    assert(root:id() == assert(maki.agent.get(ctx, assert(root:ref()))):id())
                    assert(root:set_mode(ctx, "plan") == "plan")
                    retained = {{ ctx = ctx, root = root }}
                end,
            }})
            maki.api.register_command({{
                name = "/pr5_verify", description = "reject expired authority and use fresh authority", tui_only = false,
                handler = function(_, ctx)
                    local rejected, err = retained.root:status(retained.ctx)
                    assert(rejected == nil and err == {ORCHESTRATION_CONTEXT_EXPIRED:?}, err)
                    local root = assert(maki.agent.current(ctx))
                    assert(root:id() == retained.root:id())
                    assert(root:set_mode(ctx, "build") == "build")
                end,
            }})
        "#)).unwrap();
        for command in ["/pr5_capture", "/pr5_verify"] {
            let completion = handle.run_command_for_test(
                Arc::from("pr5_trusted_command"),
                Arc::from(command),
                String::new(),
                0,
            );
            let result = futures_lite::future::race(
                async { completion.recv_async().await.unwrap() },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    panic!("trusted command never completed")
                },
            )
            .await;
            assert_eq!(result, Ok(()));
            let config = fixture
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap();
            if command == "/pr5_capture" {
                assert!(matches!(config.mode, AgentMode::Plan(_)));
            } else {
                assert_eq!(config.mode, AgentMode::Build);
            }
        }
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn tool_spawn_preserves_executing_turn_configuration_after_committed_change() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_tool_spawn_config",
            &format!(
                r#"
            local parent = assert(maki.agent.current(ctx))
            assert(parent:set_model(ctx, {{ spec = "{FAST_MODEL}", thinking = "low", fast = true }}))
            assert(parent:set_mode(ctx, "plan") == "plan")
            local child = assert(maki.agent.spawn(ctx, {{ silent = true, tools = {{}} }}))
            assert(assert(assert(child:send(ctx, "turn snapshot")):wait(ctx)).text == "{FIRST_REPLY}")
            assert(child:close(ctx))
        "#
            ),
        );
        let request = fixture.request().await;
        let child = fixture
            .manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id.is_some())
            .unwrap();
        let config = fixture
            .manager
            .lookup(child.agent_id)
            .unwrap()
            .effective_config()
            .unwrap()
            .unwrap();
        assert_eq!(config.model.spec(), common::default_model().spec());
        assert_eq!(config.thinking, ThinkingConfig::Off);
        assert!(!config.fast);
        assert_eq!(config.mode, AgentMode::Build);
        assert_eq!(request.model, common::default_model().spec());
        assert_eq!(request.options.thinking, config.thinking);
        assert!(!request.options.fast);
        request
            .reply
            .send(common::canned_reply(FIRST_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn trusted_spawn_inherits_configuration_committed_in_same_invocation() {
    smol::block_on(async {
        let fixture = Fixture::start("pr5_spawn_config_setup", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "root completion")
                .await
                .unwrap(),
            Ok(())
        );
        bounded(fixture.root_ticket.wait(), "root settlement").await;
        fixture._host.load_source("pr5_spawn_config", &format!(r#"
            maki.api.register_command({{
                name = "/pr5_spawn_config", tui_only = false,
                handler = function(_, ctx)
                    local parent = assert(maki.agent.current(ctx))
                    assert(parent:set_model(ctx, {{ spec = "{FAST_MODEL}", thinking = "low", fast = true }}))
                    assert(parent:set_mode(ctx, "plan") == "plan")
                    local child = assert(maki.agent.spawn(ctx, {{ silent = true, tools = {{}} }}))
                    local selection = assert(child:model(ctx))
                    assert(selection.spec == "{FAST_MODEL}")
                    assert(selection.thinking == "low" and selection.fast == true)
                    assert(assert(assert(child:send(ctx, "inherited configuration")):wait(ctx)).text == "{FIRST_REPLY}")
                    assert(child:close(ctx))
                end,
            }})
        "#)).unwrap();
        fixture._host.wait_for_worker_barrier_for_test();
        let handle = fixture._host.event_handle();
        let command = handle.run_command_for_test(
            Arc::from("pr5_spawn_config"),
            Arc::from("/pr5_spawn_config"),
            String::new(),
            0,
        );
        let request = request_with_command_diagnostics(
            &fixture.requests,
            &command,
            "trusted child first request",
        )
        .await;
        let child = fixture
            .manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id.is_some())
            .unwrap();
        let config = fixture
            .manager
            .lookup(child.agent_id)
            .unwrap()
            .effective_config()
            .unwrap()
            .unwrap();
        assert_eq!(config.model.spec(), FAST_MODEL);
        assert_eq!(config.thinking, ThinkingConfig::Effort(Effort::Low));
        assert!(config.fast);
        assert!(
            matches!(&config.mode, AgentMode::Plan(path) if path.starts_with(fixture._state_dir.path())),
            "{:?}",
            config.mode
        );
        assert_eq!(config.mode_def.as_ref().unwrap().id.key(), "plan");
        assert_eq!(request.model, FAST_MODEL);
        assert_eq!(request.options.thinking, config.thinking);
        assert!(request.options.fast);
        assert!(request.tools.as_array().unwrap().is_empty());
        request
            .reply
            .send(common::canned_reply(FIRST_REPLY))
            .unwrap();
        assert_eq!(
            bounded(command.recv_async(), "trusted command completion")
                .await
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn trusted_context_controls_another_registered_graph_with_fresh_authority() {
    smol::block_on(async {
        let first = Fixture::start("pr5_registered_first", "");
        let second = Fixture::start("pr5_registered_second", "");
        assert_eq!(
            bounded(first.completed.recv_async(), "integration channel receive")
                .await
                .unwrap(),
            Ok(())
        );
        assert_eq!(
            bounded(second.completed.recv_async(), "integration channel receive")
                .await
                .unwrap(),
            Ok(())
        );
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let services = Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: first.manager.root().unwrap(),
                template: template.clone(),
            },
            TrustedTarget {
                target: second.manager.root().unwrap(),
                template,
            },
        ]));
        let handle = first._host.event_handle();
        handle.install_orchestration_services(services.clone());
        first._host.load_source("pr5_registered_graphs", r#"
            local other
            local other_ref
            maki.api.register_command({
                name = "/pr5_register_capture", description = "capture the second graph", tui_only = false,
                handler = function(_, ctx)
                    other = assert(maki.agent.current(ctx))
                    other_ref = assert(other:ref())
                end,
            })
            maki.api.register_command({
                name = "/pr5_register_control", description = "control a registered graph from another target", tui_only = false,
                handler = function(_, ctx)
                    local current = assert(maki.agent.current(ctx))
                    local resolved = assert(maki.agent.get(ctx, other_ref))
                    assert(resolved:id() == other:id())
                    assert(resolved:id() ~= current:id())
                    assert(other:set_mode(ctx, "plan") == "plan")
                    assert(other:cancel(ctx))
                    assert(assert(current:status(ctx)).status == "idle")
                end,
            })
        "#).unwrap();
        for (index, command) in ["/pr5_register_capture", "/pr5_register_control"]
            .into_iter()
            .enumerate()
        {
            services
                .selected
                .store(usize::from(index == 0), Ordering::SeqCst);
            assert_eq!(
                handle
                    .run_command_for_test(
                        Arc::from("pr5_registered_graphs"),
                        Arc::from(command),
                        String::new(),
                        0,
                    )
                    .recv_timeout(TEST_TIMEOUT)
                    .unwrap(),
                Ok(())
            );
        }
        assert_eq!(
            first
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            AgentMode::Build
        );
        assert!(matches!(
            second
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            AgentMode::Plan(_)
        ));
        shutdown_manager(&first.manager).await;
        shutdown_manager(&second.manager).await;
    });
}

#[test_case::test_case(false ; "build")]
#[test_case::test_case(true ; "restricted_plan")]
fn tool_context_rejects_foreign_manager_handles_captured_by_trusted_command(restricted: bool) {
    smol::block_on(async {
        let owner = Fixture::start("pr5_foreign_owner", "");
        assert_eq!(
            bounded(owner.completed.recv_async(), "integration channel receive")
                .await
                .unwrap(),
            Ok(())
        );
        let other = Fixture::start("pr5_foreign_other", "");
        assert_eq!(
            bounded(other.completed.recv_async(), "integration channel receive")
                .await
                .unwrap(),
            Ok(())
        );
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let handle = owner._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: other.manager.root().unwrap(),
                template,
            },
        ])));
        owner._host.load_source("pr5_foreign_scope", &format!(r#"
            local foreign
            local foreign_ref
            maki.api.register_command({{
                name = "/pr5_foreign_capture", description = "capture a host-registered second runtime", tui_only = false,
                handler = function(_, ctx)
                    foreign = assert(maki.agent.current(ctx))
                    foreign_ref = assert(foreign:ref())
                    assert(foreign:set_mode(ctx, "plan") == "plan")
                end,
            }})
            maki.api.register_tool({{
                name = "pr5_reject_foreign", description = "reject handles from another manager",
                schema = {{ type = "object", properties = {{}} }},
                handler = function(_, ctx)
                    local target, lookup_err = maki.agent.get(ctx, foreign_ref)
                    assert(target == nil and lookup_err == {FOREIGN_AGENT_REFERENCE:?}, lookup_err)
                    for _, operation in ipairs({{ "status", "cancel", "close" }}) do
                        local result, err = foreign[operation](foreign, ctx)
                        assert(result == nil and err == {OTHER_RUNTIME:?}, err)
                    end
                    local sent, send_err = foreign:send(ctx, "foreign must not run")
                    assert(sent == nil and send_err == {OTHER_RUNTIME:?}, send_err)
                    local changed, mode_err = foreign:set_mode(ctx, "build")
                    assert(changed == nil and mode_err == {OTHER_RUNTIME:?}, mode_err)
                    local updated, model_err = foreign:set_model(ctx, {UPDATED_MODEL:?})
                    assert(updated == nil and model_err == {OTHER_RUNTIME:?}, model_err)
                    return "foreign runtime rejected"
                end,
            }})
        "#)).unwrap();
        assert_eq!(
            handle
                .run_command_for_test(
                    Arc::from("pr5_foreign_scope"),
                    Arc::from("/pr5_foreign_capture"),
                    String::new(),
                    0,
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        assert!(matches!(
            other
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            AgentMode::Plan(_)
        ));
        let (mut context, _events, _cancel) = common::ctx_with_canned_provider();
        if restricted {
            context.mode = AgentMode::Plan("/test/foreign-plan.md".into());
        }
        let (completed_tx, completed) = flume::bounded(1);
        let root = owner.manager.root().unwrap();
        let mut config = (*root.actor().unwrap().effective_config().unwrap()).clone();
        config.mode = context.mode.clone();
        let child = owner
            .manager
            .spawn_child_trusted_with_config(
                &root,
                config,
                Default::default(),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(Box::new(LuaInvocationBackend {
                        registry: owner._host.registry(),
                        context,
                        tool: "pr5_reject_foreign",
                        completed: completed_tx,
                        observations: flume::unbounded().0,
                    }) as Box<dyn ActorBackend>)
                },
            )
            .unwrap();
        let ticket = child
            .actor()
            .unwrap()
            .admit_turn(
                AgentInput {
                    message: "check foreign scope".into(),
                    mode: AgentMode::Build,
                    images: Vec::new(),
                    preamble: Vec::new(),
                    thinking: Default::default(),
                    fast: false,
                    workflow: false,
                    prompt: None,
                    cancel: None,
                    lease_committer: None,
                },
                None,
                String::new(),
            )
            .unwrap();
        assert_eq!(
            bounded(completed.recv_async(), "integration channel receive")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(matches!(
            bounded(ticket.wait(), "child turn settlement").await,
            TurnOutcome::Completed { .. }
        ));
        assert!(matches!(
            other
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .mode,
            AgentMode::Plan(_)
        ));
        shutdown_manager(&owner.manager).await;
        shutdown_manager(&other.manager).await;
    });
}

#[test]
fn trusted_mode_changes_preserve_registered_target_model_policy() {
    smol::block_on(async {
        let owner = Fixture::start("pr5_destination_owner", "");
        let restricted = Fixture::start_with_mode(
            "pr5_destination_restricted",
            "",
            AgentMode::Plan("/test/destination-plan.md".into()),
        );
        assert_eq!(
            bounded(owner.completed.recv_async(), "integration channel receive")
                .await
                .unwrap(),
            Ok(())
        );
        assert_eq!(
            bounded(
                restricted.completed.recv_async(),
                "integration channel receive"
            )
            .await
            .unwrap(),
            Ok(())
        );
        let (build_template, _events, _cancel) = common::ctx_with_canned_provider();
        let mut restricted_template = build_template.clone();
        restricted_template.mode = AgentMode::Plan("/test/destination-plan.md".into());
        restricted_template.model_policy = Arc::new(
            maki_config::ModelPolicy::new(&[common::default_model().spec()], &[]).unwrap(),
        );
        let services = Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: owner.manager.root().unwrap(),
                template: build_template,
            },
            TrustedTarget {
                target: restricted.manager.root().unwrap(),
                template: restricted_template,
            },
        ]));
        let handle = owner._host.event_handle();
        handle.install_orchestration_services(services.clone());
        let not_allowed = ModelError::NotAllowed(UPDATED_MODEL.into()).to_string();
        owner
            ._host
            .load_source(
                "pr5_destination_policy",
                &format!(
                    r#"
            local target
            maki.api.register_command({{
                name = "/pr5_destination_capture", tui_only = false,
                handler = function(_, ctx) target = assert(maki.agent.current(ctx)) end,
            }})
            maki.api.register_command({{
                name = "/pr5_destination_attack", tui_only = false,
                handler = function(_, ctx)
                    assert(assert(maki.agent.current(ctx)):id() ~= target:id())
                    assert(target:set_mode(ctx, "build") == "build")
                    local changed, model_err = target:set_model(ctx, {UPDATED_MODEL:?})
                    assert(changed == nil and model_err == {not_allowed:?}, model_err)
                    assert(target:set_model(ctx, {{ thinking = "low" }}))
                    assert(assert(target:model(ctx)).thinking == "low")
                end,
            }})
        "#
                ),
            )
            .unwrap();
        for (selected, command) in [
            (1, "/pr5_destination_capture"),
            (0, "/pr5_destination_attack"),
        ] {
            services.selected.store(selected, Ordering::SeqCst);
            assert_eq!(
                handle
                    .run_command_for_test(
                        Arc::from("pr5_destination_policy"),
                        Arc::from(command),
                        String::new(),
                        0
                    )
                    .recv_timeout(TEST_TIMEOUT)
                    .unwrap(),
                Ok(())
            );
        }
        let config = restricted
            .manager
            .root()
            .unwrap()
            .actor()
            .unwrap()
            .effective_config()
            .unwrap();
        assert!(matches!(config.mode, AgentMode::Build));
        assert_eq!(config.model.spec(), common::default_model().spec());
        assert_eq!(config.thinking, ThinkingConfig::Effort(Effort::Low));
        shutdown_manager(&owner.manager).await;
        shutdown_manager(&restricted.manager).await;
    });
}

#[test]
fn focus_reads_and_keybind_targets_ignore_later_focus_changes() {
    smol::block_on(async {
        const PLUGIN: &str = "pr5_focus_pinning";
        const KEY: &str = "<C-y>";
        let focused = Fixture::start("pr5_focus_first", "");
        let pinned = Fixture::start("pr5_focus_second", "");
        for fixture in [&focused, &pinned] {
            assert_eq!(
                bounded(
                    fixture.completed.recv_async(),
                    "integration channel receive"
                )
                .await
                .unwrap(),
                Ok(())
            );
        }
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let pinned_target = TrustedTarget {
            target: pinned.manager.root().unwrap(),
            template: template.clone(),
        };
        let services = Arc::new(RegisteredOrchestrationTargets::new(vec![
            pinned_target.clone(),
            TrustedTarget {
                target: focused.manager.root().unwrap(),
                template,
            },
        ]));
        let handle = focused._host.event_handle();
        handle.install_orchestration_services(services.clone());
        focused
            ._host
            .load_source(
                PLUGIN,
                &format!(
                    r#"
            local captured
            maki.api.register_command({{
                name = "/pr5_focus_capture", tui_only = false,
                handler = function() captured = assert(maki.ui.focused_agent()) end,
            }})
            maki.api.register_command({{
                name = "/pr5_focus_check", tui_only = false,
                handler = function(_, ctx)
                    assert(assert(maki.ui.focused_agent()):id() ~= captured:id())
                    assert(assert(maki.agent.get(ctx, captured)):id() == captured:id())
                end,
            }})
            maki.keymap.set("n", "{KEY}", function(_, ctx)
                assert(assert(maki.agent.current(ctx)):set_model(ctx, {{ thinking = "low" }}))
            end)
        "#
                ),
            )
            .unwrap();
        for (selected, command) in [(0, "/pr5_focus_capture"), (1, "/pr5_focus_check")] {
            services.selected.store(selected, Ordering::SeqCst);
            assert_eq!(
                handle
                    .run_command_for_test(Arc::from(PLUGIN), Arc::from(command), String::new(), 0)
                    .recv_timeout(TEST_TIMEOUT)
                    .unwrap(),
                Ok(())
            );
        }
        let keymap = focused._host.keymap_reader().load().entries[0].id;
        assert!(handle.run_keybind_callback(keymap, Some(pinned_target)));
        let thinking = |fixture: &Fixture| {
            fixture
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .thinking
        };
        bounded(
            async {
                while thinking(&pinned) != ThinkingConfig::Effort(Effort::Low) {
                    smol::future::yield_now().await;
                }
            },
            "pinned keybind target update",
        )
        .await;
        assert_eq!(thinking(&focused), ThinkingConfig::Off);
        shutdown_manager(&focused.manager).await;
        shutdown_manager(&pinned.manager).await;
    });
}

#[test]
fn tool_ctx_reads_the_admitted_model_after_the_agent_commits_a_change() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_admitted_model",
            r#"
            local agent = assert(maki.agent.current(ctx))
            local admitted = assert(ctx:model())
            assert(admitted.thinking == "off", tostring(admitted.thinking))
            assert(agent:set_model(ctx, { thinking = "high" }))
            assert(assert(agent:model(ctx)).thinking == "high")
            assert(assert(ctx:model()).thinking == "off")
        "#,
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "admitted model reads")
                .await
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn turn_waits_and_focus_reads_refuse_without_a_valid_subject() {
    smol::block_on(async {
        const PLUGIN: &str = "pr5_no_focus";
        let fixture = Fixture::start(
            "pr5_self_wait",
            &format!(
                r#"
            local own = assert(assert(maki.agent.current(ctx)):latest_turn(ctx))
            local waited, wait_err = own:wait(ctx)
            assert(waited == nil and wait_err == {SELF_TURN_WAIT:?}, wait_err)
            local focused, focus_err = maki.ui.focused_agent()
            assert(focused == nil and focus_err == {DELEGATED_FOCUS:?}, focus_err)
        "#
            ),
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "self wait refusal")
                .await
                .unwrap(),
            Ok(())
        );
        fixture
            ._host
            .event_handle()
            .install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(
                Vec::new(),
            )));
        fixture
            ._host
            .load_source(
                PLUGIN,
                &format!(
                    r#"
            maki.api.register_command({{
                name = "/pr5_no_focus", tui_only = false,
                handler = function()
                    local focused, focus_err = maki.ui.focused_agent()
                    assert(focused == nil and focus_err == {NO_FOCUSED_AGENT:?}, focus_err)
                end,
            }})
        "#
                ),
            )
            .unwrap();
        assert_eq!(
            fixture
                ._host
                .event_handle()
                .run_command_for_test(
                    Arc::from(PLUGIN),
                    Arc::from("/pr5_no_focus"),
                    String::new(),
                    0
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn trusted_ctx_keeps_authority_after_its_pinned_target_closes() {
    smol::block_on(async {
        const PLUGIN: &str = "pr5_closed_pin";
        const KEY: &str = "<C-k>";
        let fixture = Fixture::start("pr5_closed_pin_root", "");
        assert_eq!(
            bounded(
                fixture.completed.recv_async(),
                "integration channel receive"
            )
            .await
            .unwrap(),
            Ok(())
        );
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let root = fixture.manager.root().unwrap();
        let services = Arc::new(RegisteredOrchestrationTargets::new(vec![TrustedTarget {
            target: root.clone(),
            template,
        }]));
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(services.clone());
        fixture
            ._host
            .load_source(
                PLUGIN,
                &format!(
                    r#"
            local outcome = "keymap did not run"
            maki.api.register_command({{
                name = "/pr5_closed_pin_spawn", tui_only = false,
                handler = function(_, ctx)
                    assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                end,
            }})
            maki.api.register_command({{
                name = "/pr5_closed_pin_check", tui_only = false,
                handler = function() assert(outcome == "ok", outcome) end,
            }})
            maki.keymap.set("n", "{KEY}", function(_, ctx)
                local ok, err = pcall(function()
                    local pinned = assert(maki.agent.current(ctx))
                    assert(pinned:close(ctx))
                    local current, current_err = maki.agent.current(ctx)
                    assert(current == nil and current_err ~= nil, "closed target stayed current")
                    local spawned, spawn_err = maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }})
                    assert(spawned == nil and spawn_err ~= nil, "spawned under a closed target")
                    local root = assert(maki.agent.root(ctx))
                    local listed = false
                    for _, reference in ipairs(assert(maki.agent.list(ctx))) do
                        listed = listed or reference:id() == root:id()
                    end
                    assert(listed, "root missing from list")
                end)
                outcome = ok and "ok" or tostring(err)
            end)
        "#
                ),
            )
            .unwrap();
        let run = |command: &str| {
            handle
                .run_command_for_test(Arc::from(PLUGIN), Arc::from(command), String::new(), 0)
                .recv_timeout(TEST_TIMEOUT)
                .unwrap()
        };
        assert_eq!(run("/pr5_closed_pin_spawn"), Ok(()));
        let child = fixture
            .manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id.is_some())
            .expect("spawned child");
        let child = fixture.manager.lookup(child.agent_id).unwrap();
        let pinned = TrustedTarget {
            template: services.template(&child).unwrap(),
            target: child,
        };
        let keymap = fixture._host.keymap_reader().load().entries[0].id;
        assert!(handle.run_keybind_callback(keymap, Some(pinned)));
        fixture._host.wait_for_worker_barrier_for_test();
        assert_eq!(run("/pr5_closed_pin_check"), Ok(()));
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn bundled_thinking_command_updates_only_its_target_agent() {
    smol::block_on(async {
        const PLUGIN: &str = "thinking";
        let target = Fixture::start("pr5_thinking_target", "");
        let other = Fixture::start("pr5_thinking_other", "");
        for fixture in [&target, &other] {
            assert_eq!(
                bounded(
                    fixture.completed.recv_async(),
                    "integration channel receive"
                )
                .await
                .unwrap(),
                Ok(())
            );
        }
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let services = Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: target.manager.root().unwrap(),
                template: template.clone(),
            },
            TrustedTarget {
                target: other.manager.root().unwrap(),
                template,
            },
        ]));
        let handle = target._host.event_handle();
        handle.install_orchestration_services(services);
        target
            ._host
            .load_source(PLUGIN, include_str!("../../plugins/thinking/init.lua"))
            .unwrap();
        let ui = target._host.ui_action_rx();
        let (flash_tx, flashes) = flume::unbounded();
        let (default_tx, defaults) = flume::unbounded();
        std::thread::spawn(move || {
            while let Ok(action) = ui.recv() {
                match action {
                    UiAction::Flash(message) => {
                        let _ = flash_tx.send(message);
                    }
                    UiAction::Session {
                        req: SessionRequest::SetDefaultThinking { thinking },
                        reply_tx,
                    } => {
                        let _ = reply_tx.send(Ok(json!({ "mode": thinking.clone() })));
                        let _ = default_tx.send(thinking);
                    }
                    _ => {}
                }
            }
        });
        assert_eq!(
            handle
                .run_command_for_test(Arc::from(PLUGIN), Arc::from("/thinking"), "low".into(), 0)
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        assert_eq!(defaults.recv_timeout(TEST_TIMEOUT).unwrap(), "low");
        assert_eq!(
            flashes.recv_timeout(TEST_TIMEOUT).unwrap(),
            "Thinking: low (default for new sessions)"
        );
        let thinking = |fixture: &Fixture| {
            fixture
                .manager
                .root()
                .unwrap()
                .actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .thinking
        };
        assert_eq!(thinking(&target), ThinkingConfig::Effort(Effort::Low));
        assert_eq!(thinking(&other), ThinkingConfig::Off);
        shutdown_manager(&target.manager).await;
        shutdown_manager(&other.manager).await;
    });
}

#[test]
fn trusted_ticket_reads_survive_close_from_fresh_command_context() {
    smol::block_on(async {
        let fixture = Fixture::start("pr5_retained_root", "");
        assert_eq!(
            bounded(
                fixture.completed.recv_async(),
                "integration channel receive"
            )
            .await
            .unwrap(),
            Ok(())
        );
        let rx = fixture.requests.clone();
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: fixture.manager.root().unwrap(),
                template,
            },
        ])));
        fixture._host.load_source("pr5_retained_ticket", r#"
            local child, first, second
            maki.api.register_command({
                name = "/pr5_retained_begin", tui_only = false,
                handler = function(_, ctx)
                    child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    first = assert(child:send(ctx, "terminal history"))
                    assert(assert(first:wait(ctx)).text == "terminal reply")
                    second = assert(child:send(ctx, "active history"))
                end,
            })
            maki.api.register_command({
                name = "/pr5_retained_inspect", tui_only = false,
                handler = function(_, ctx)
                    local transcript = assert(child:transcript(ctx, { last_messages = 100, max_bytes = 65536 }))
                    assert(#transcript.messages == 2)
                    assert(transcript.through_turn == first:id())
                    local checkpoint = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    assert(assert(assert(checkpoint:send(ctx, "transcript inspected")):wait(ctx)).text == "inspected")
                    assert(checkpoint:close(ctx))
                    assert(assert(second:wait(ctx)).text == "active reply")
                    assert(child:close(ctx))
                end,
            })
            maki.api.register_command({
                name = "/pr5_retained_verify", tui_only = false,
                handler = function(_, ctx)
                    for _ = 1, 3 do
                        assert(assert(first:result(ctx)).text == "terminal reply")
                        assert(assert(first:wait(ctx)).text == "terminal reply")
                        assert(assert(second:result(ctx)).text == "active reply")
                        assert(assert(second:wait(ctx)).text == "active reply")
                    end
                end,
            })
        "#).unwrap();
        let begin = handle.run_command_for_test(
            Arc::from("pr5_retained_ticket"),
            Arc::from("/pr5_retained_begin"),
            String::new(),
            0,
        );
        let terminal = request_with_command_diagnostics(&rx, &begin, "terminal request").await;
        terminal
            .reply
            .send(common::canned_reply("terminal reply"))
            .unwrap();
        let active = request_with_command_diagnostics(&rx, &begin, "active request").await;
        assert_eq!(begin.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        let inspect = handle.run_command_for_test(
            Arc::from("pr5_retained_ticket"),
            Arc::from("/pr5_retained_inspect"),
            String::new(),
            0,
        );
        let checkpoint =
            request_with_command_diagnostics(&rx, &inspect, "checkpoint request").await;
        assert!(
            serde_json::to_string(&checkpoint.messages)
                .unwrap()
                .contains("transcript inspected")
        );
        checkpoint
            .reply
            .send(common::canned_reply("inspected"))
            .unwrap();
        active
            .reply
            .send(common::canned_reply("active reply"))
            .unwrap();
        assert_eq!(inspect.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        assert_eq!(
            handle
                .run_command_for_test(
                    Arc::from("pr5_retained_ticket"),
                    Arc::from("/pr5_retained_verify"),
                    String::new(),
                    0
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn trusted_turn_callbacks_overlap_and_revocation_cancels_deferred_children() {
    smol::block_on(async {
        let fixture = Fixture::start("pr5_event_root", "");
        assert_eq!(
            bounded(
                fixture.completed.recv_async(),
                "integration channel receive"
            )
            .await
            .unwrap(),
            Ok(())
        );
        let requests = fixture.requests.clone();
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: fixture.manager.root().unwrap(),
                template,
            },
        ])));
        fixture._host.load_source("pr5_event_callbacks", r#"
            local callback_errors, callback_states = {}, {}
            local function guard(kind, handler)
                return function(event, ctx)
                    callback_states[kind] = "entered"
                    local ok, err = pcall(handler, event, ctx)
                    callback_states[kind] = ok and "completed" or "failed"
                    if not ok then callback_errors[#callback_errors + 1] = kind .. ": " .. tostring(err) end
                end
            end
            maki.api.register_command({ name = "/pr5_event_diagnostics", tui_only = false,
                handler = function() error("callback states: " .. maki.json.encode(callback_states) .. "; errors: " .. table.concat(callback_errors, "\n")) end,
            })
            local observed
            local start_subscription
            local end_subscription
            local close_subscription
            local close_probe
            local function hold(ctx, message)
                local child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                assert(ctx:defer_close(child))
                assert(assert(child:send(ctx, message)):wait(ctx))
            end
            maki.api.register_command({
                name = "/pr5_event_begin", description = "subscribe and run an observed turn", tui_only = false,
                handler = function(_, ctx)
                    observed = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    close_probe = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    start_subscription = assert(observed:on_turn_start(ctx, guard("start", function(event, callback_ctx)
                        assert(event.agent_id == observed:id())
                        assert(event.origin.kind == "plugin" and event.origin.plugin == "pr5_event_callbacks")
                        assert(event.origin.plugin_generation ~= nil)
                        hold(callback_ctx, "start callback blocked")
                    end)))
                    end_subscription = assert(observed:on_turn_end(ctx, guard("end", function(event, callback_ctx)
                        assert(event.status == "completed" and event.text == "observed reply")
                        assert(event.origin.kind == "plugin" and event.origin.plugin == "pr5_event_callbacks")
                        hold(callback_ctx, "end callback blocked")
                    end)))
                    close_subscription = assert(observed:on_close(ctx, guard("close", function(event, callback_ctx)
                        assert(event.lifecycle == "closed")
                        assert(callback_ctx:defer_close(close_probe))
                        assert(assert(close_probe:send(callback_ctx, "close callback delivered")):wait(callback_ctx))
                    end)))
                    local turn = assert(observed:send(ctx, "observed turn"))
                    assert(assert(turn:wait(ctx)).text == "observed reply")
                end,
            })
            maki.api.register_command({
                name = "/pr5_event_revoke", description = "cancel an active subscription and close the actor", tui_only = false,
                handler = function(_, ctx)
                    assert(start_subscription:close())
                    assert(start_subscription:close())
                    assert(observed:close(ctx))
                end,
            })
        "#).unwrap();
        let command = handle.run_command_for_test(
            Arc::from("pr5_event_callbacks"),
            Arc::from("/pr5_event_begin"),
            String::new(),
            0,
        );
        let receive = || {
            futures_lite::future::race(async { requests.recv_async().await.unwrap() }, async {
                smol::Timer::after(TEST_TIMEOUT).await;
                let diagnostics = handle
                    .run_command_for_test(
                        Arc::from("pr5_event_callbacks"),
                        Arc::from("/pr5_event_diagnostics"),
                        String::new(),
                        0,
                    )
                    .recv_timeout(SHUTDOWN_TIMEOUT);
                panic!(
                    "event callback request never arrived; callbacks: {diagnostics:?}; command: {:?}",
                    command.try_recv()
                )
            })
        };
        let a: CapturedRequest = receive().await;
        let b = receive().await;
        let (observed, start) = if serde_json::to_string(&a.messages)
            .unwrap()
            .contains("observed turn")
        {
            (a, b)
        } else {
            (b, a)
        };
        assert!(
            serde_json::to_string(&start.messages)
                .unwrap()
                .contains("start callback blocked")
        );
        observed
            .reply
            .send(common::canned_reply("observed reply"))
            .unwrap();
        let end = receive().await;
        assert!(
            serde_json::to_string(&end.messages)
                .unwrap()
                .contains("end callback blocked")
        );
        assert!(
            !start.reply.is_disconnected(),
            "end callback did not overlap suspended start callback"
        );
        assert_eq!(command.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        assert_eq!(
            handle
                .run_command_for_test(
                    Arc::from("pr5_event_callbacks"),
                    Arc::from("/pr5_event_revoke"),
                    String::new(),
                    0
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        let close = receive().await;
        assert!(
            serde_json::to_string(&close.messages)
                .unwrap()
                .contains("close callback delivered")
        );
        wait_for_gate_drop(&start, "subscription close child cancellation").await;
        let (unloaded_tx, unloaded) = flume::bounded(1);
        let manager = fixture.manager.clone();
        std::thread::spawn(move || {
            let result = fixture._host.unload("pr5_event_callbacks");
            let _ = unloaded_tx.send(result);
        });
        unloaded
            .recv_timeout(TEST_TIMEOUT)
            .expect("plugin unload deadlocked while callback provider futures were gated")
            .unwrap();
        wait_for_gate_drop(&end, "plugin unload end callback cancellation").await;
        wait_for_gate_drop(&close, "plugin unload close callback cancellation").await;
        shutdown_manager(&manager).await;
    });
}

#[test]
fn delegated_tools_cannot_reach_trusted_entry_points() {
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            "pr5_tool_events",
            FixtureOptions {
                plugin_source: r#"
                    local effects = 0
                    function pr5_timer_escape()
                        effects = effects + 1
                        assert(maki.api.run_command("/pr5_timer_effect"))
                    end
                    maki.api.register_command({ name = "/pr5_timer_effect", tui_only = false,
                        handler = function() effects = effects + 1 end,
                    })
                    maki.api.create_autocmd("User", { pattern = "pr5_escape", callback = function()
                        local ran, err = maki.api.run_command("/pr5_timer_effect")
                        pr5_autocmd_escape = { ran = ran, err = err }
                    end })
                    maki.api.register_command({ name = "/pr5_timer_check", tui_only = false,
                        handler = function()
                            assert(effects == 0, "scheduled callback or command escaped")
                            local id, err = maki.timer.set(3600, function() end)
                            assert(id == 0 and err == nil, "denied timer mutated the store")
                            maki.timer.del(id)
                        end,
                    })
                "#
                .into(),
                ..Default::default()
            },
            &format!(
                "local subscription_error = {TRUSTED_SUBSCRIPTION_REQUIRED:?}\n\
                 local command_error = {TRUSTED_RUN_COMMAND_REQUIRED:?}\n\
                 local timer_error = {TRUSTED_TIMER_REQUIRED:?}\n\
                 local registration_error = {DELEGATED_REGISTRATION:?}\n\
                 local host_mutation_error = {DELEGATED_HOST_MUTATION:?}\n{}",
                r#"
            local child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
            local function callback(_, callback_ctx)
                assert(assert(child:send(callback_ctx, "forbidden callback")):wait(callback_ctx))
            end
            for _, kind in ipairs({ "on_turn_start", "on_turn_end", "on_idle", "on_config_change", "on_close" }) do
                local subscription, err = child[kind](child, ctx, callback)
                assert(subscription == nil and err == subscription_error, kind .. ": " .. tostring(err))
            end
            local ran, err = maki.api.run_command("/pr5_timer_effect")
            assert(ran == nil and err == command_error)
            local function deny_timers()
                local id, timer_err = maki.timer.set(0.001, pr5_timer_escape)
                assert(id == nil and timer_err == timer_error, tostring(timer_err))
                local handle, defer_err = maki.defer_fn(pr5_timer_escape, 0)
                assert(handle == nil and defer_err == timer_error, tostring(defer_err))
            end
            deny_timers()
            local registrations = {
                register_command = function()
                    return maki.api.register_command({ name = "/pr5_escape", tui_only = false, handler = pr5_timer_escape })
                end,
                keymap_set = function() return maki.keymap.set("n", "<C-y>", pr5_timer_escape) end,
                keymap_del = function() return maki.keymap.del("n", "<C-y>") end,
                usage_on_change = function() return maki.usage.on_change(pr5_timer_escape) end,
                set_notify_handler = function() return maki.set_notify_handler(pr5_timer_escape) end,
                set_slot = function() return maki.api.set_slot("pr5.escape", pr5_timer_escape) end,
                create_autocmd = function() return maki.api.create_autocmd("User", { callback = pr5_timer_escape }) end,
                register_expander = function() return maki.api.register_expander("pr5", pr5_timer_escape) end,
            }
            for name, register in pairs(registrations) do
                local registered, register_err = register()
                assert(registered == nil and register_err == registration_error, name .. ": " .. tostring(register_err))
            end
            local assigned, assignment_err = pcall(function() maki.notify = pr5_timer_escape end)
            assert(not assigned and tostring(assignment_err):find(registration_error, 1, true), tostring(assignment_err))
            maki.api.exec_autocmds("User", { pattern = "pr5_escape" })
            assert(pr5_autocmd_escape.ran == nil and pr5_autocmd_escape.err == command_error, tostring(pr5_autocmd_escape.err))
            for name, mutate in pairs({
                mode_set = function() return maki.api.mode.set("build") end,
                mode_reset = function() return maki.api.mode.reset() end,
                session_set_thinking = function() return maki.session.set_thinking({ mode = "max" }) end,
                session_set_option = function() return maki.session.set_option("fast", "true") end,
                session_set_default_thinking = function() return maki.session.set_default_thinking("max") end,
            }) do
                local changed, mutate_err = mutate()
                assert(changed == nil and mutate_err == host_mutation_error, name .. ": " .. tostring(mutate_err))
            end
            local async_err = maki.async.await(1, function(done)
                maki.async.run(deny_timers, function(err) done(err or false) end)
            end)
            assert(async_err == false, tostring(async_err))
            assert(child:set_model(ctx, { thinking = "low" }))
            assert(child:close(ctx))
        "#
            ),
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "delegated timer denial")
                .await
                .unwrap(),
            Ok(())
        );
        bounded(fixture.root_ticket.wait(), "delegated turn settlement").await;
        fixture._host.wait_for_worker_barrier_for_test();
        let check = fixture._host.event_handle().run_command_for_test(
            Arc::from("pr5_tool_events"),
            Arc::from("/pr5_timer_check"),
            String::new(),
            0,
        );
        assert_eq!(
            bounded(check.recv_async(), "timer effects and registration check")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(
            fixture.requests.is_empty(),
            "timer callback caused agent work"
        );
        assert!(fixture._host.keymap_reader().load().entries.is_empty());
        let handle = fixture._host.event_handle();
        assert_ne!(
            handle.command_generation_for_test("pr5_tool_events", "/pr5_timer_effect"),
            0
        );
        assert_eq!(
            handle.command_generation_for_test("pr5_tool_events", "/pr5_escape"),
            0
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test_case::test_case(false ; "inherited_snapshot")]
#[test_case::test_case(true ; "explicit_registry_definitions")]
fn trusted_spawn_restores_host_bindings_without_expanding_capabilities(explicit: bool) {
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            "pr5_audience_ceiling",
            FixtureOptions {
                audience_ceiling: true,
                ..Default::default()
            },
            "",
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "root completion")
                .await
                .unwrap(),
            Ok(())
        );
        bounded(fixture.root_ticket.wait(), "root settlement").await;
        let (mut template, _events, _cancel) = common::ctx_with_canned_provider();
        template.registry = Arc::clone(ToolRegistry::global_arc());
        template.turn_bindings = Arc::new(maki_agent::tools::TurnToolBindings::default());
        let definitions = json!([
            { "name": "pr5_audience_ceiling", "description": "exact parent snapshot", "input_schema": { "type": "object", "properties": {} } },
            { "name": "pr5_audience_ceiling_sub", "description": "subagent only", "input_schema": { "type": "object", "properties": {} } },
        ]);
        let tools = maki_agent::tools::RequestTools::assembled(
            definitions.clone(),
            &template.config,
            &template.model,
        );
        template.tool_filter = Arc::clone(tools.filter());
        template.request_tools = Some(tools);
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: fixture.manager.root().unwrap(),
                template,
            },
        ])));
        let selection = if explicit {
            r#"tools = {
                { name = "pr5_audience_ceiling", description = "exact parent snapshot", input_schema = { type = "object", properties = {} } },
                { name = "pr5_audience_ceiling_sub", description = "subagent only", input_schema = { type = "object", properties = {} } },
                { name = "pr5_audience_ceiling_unpublished", description = "not advertised", input_schema = { type = "object", properties = {} } },
            },"#
        } else {
            ""
        };
        fixture._host.load_source("pr5_trusted_bindings", &format!(r#"
            maki.api.register_command({{
                name = "/pr5_trusted_bindings", tui_only = false,
                handler = function(_, ctx)
                    local child = assert(maki.agent.spawn(ctx, {{
                        inherit_provider = true, silent = true, audience = "main", {selection}
                    }}))
                    assert(assert(assert(child:send(ctx, "trusted bindings")):wait(ctx)).text == "{SECOND_REPLY}")
                    assert(child:close(ctx))
                end,
            }})
        "#)).unwrap();
        fixture._host.wait_for_worker_barrier_for_test();
        let command = handle.run_command_for_test(
            Arc::from("pr5_trusted_bindings"),
            Arc::from("/pr5_trusted_bindings"),
            String::new(),
            0,
        );
        let first = fixture.request().await;
        assert_eq!(first.tools, json!([definitions[0].clone()]));
        first
            .reply
            .send(common::canned_tool_use("pr5_audience_ceiling", json!({})))
            .unwrap();
        let second = fixture.request().await;
        assert!(
            serde_json::to_string(&second.messages)
                .unwrap()
                .contains("ok")
        );
        second
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        assert_eq!(
            bounded(command.recv_async(), "trusted command completion")
                .await
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test_case::test_case(false ; "inherited_snapshot")]
#[test_case::test_case(true ; "explicit_registry_definitions")]
fn child_audience_cannot_expand_parent_capabilities(explicit: bool) {
    smol::block_on(async {
        let tools = if explicit {
            r#"tools = {
                { name = "pr5_audience_ceiling_sub", description = "subagent-only registry tool", input_schema = { type = "object", properties = {} } },
                { name = "pr5_audience_ceiling_unpublished", description = "unadvertised registry tool", input_schema = { type = "object", properties = {} } },
            },"#
        } else {
            ""
        };
        let fixture = Fixture::start_with_options(
            "pr5_audience_ceiling",
            FixtureOptions {
                audience_ceiling: true,
                ..Default::default()
            },
            &format!(
                r#"
                local callable = assert(maki.agent.callable_tools(ctx))
                for _, tool in ipairs(callable) do
                    assert(tool.name ~= "pr5_audience_ceiling_sub")
                    assert(tool.name ~= "pr5_audience_ceiling_unpublished")
                end
                local child = assert(maki.agent.spawn(ctx, {{
                    inherit_provider = true, silent = true, audience = "general_sub", {tools}
                    local_tools = {{ fresh = {{
                        description = "new local tool remains available",
                        input_schema = {{ type = "object", properties = {{}} }},
                        handler = function() return "fresh local tool ran" end,
                    }} }},
                }}))
                assert(assert(assert(child:send(ctx, "audience ceiling")):wait(ctx)).text == "{SECOND_REPLY}")
                assert(child:close(ctx))
            "#
            ),
        );
        let first = fixture.request().await;
        let names = common::tool_names(&first.tools);
        assert!(names.iter().any(|name| name == "fresh"));
        assert!(!names.iter().any(|name| name == "pr5_audience_ceiling_sub"));
        assert!(
            !names
                .iter()
                .any(|name| name == "pr5_audience_ceiling_unpublished")
        );
        first
            .reply
            .send(common::canned_tool_use("fresh", json!({})))
            .unwrap();
        let second = fixture.request().await;
        assert!(
            serde_json::to_string(&second.messages)
                .unwrap()
                .contains("fresh local tool ran")
        );
        second
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn inherited_tools_do_not_leak_parent_structured_output_into_children() {
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            "pr5_schema_inheritance",
            FixtureOptions {
                schema_inheritance: true,
                ..Default::default()
            },
            r#"
            local schema = { type = "object", properties = { answer = { type = "integer" } }, required = { "answer" } }
            pr5_schema_children_handler = function(_, parent_ctx)
                local plain, spawn_err = maki.agent.spawn(parent_ctx, { inherit_provider = true, silent = true })
                assert(plain, "plain child spawn failed: " .. tostring(spawn_err))
                local plain_ticket, send_err = plain:send(parent_ctx, "plain inherited child")
                assert(plain_ticket, "plain child send failed: " .. tostring(send_err))
                local plain_result, wait_err = plain_ticket:wait(parent_ctx)
                assert(plain_result, "plain child wait failed: " .. tostring(wait_err))
                assert(plain_result.status == "completed" and #plain_result.output == 0 and plain_result.captured == nil)
                assert(plain:close(parent_ctx))
                local own, own_err = maki.agent.spawn(parent_ctx, { inherit_provider = true, silent = true, output_schema = schema })
                assert(own, "own-schema child spawn failed: " .. tostring(own_err))
                local own_result = assert(assert(own:send(parent_ctx, "own schema inherited child")):wait(parent_ctx))
                assert(own_result.status == "completed" and #own_result.output == 1 and own_result.output[1].answer == 7)
                assert(own_result.captured.answer == 7)
                assert(own:close(parent_ctx))
                return "children isolated"
            end
            local parent = assert(maki.agent.spawn(ctx, {
                inherit_provider = true, silent = true, output_schema = schema,
                tools = {{ name = "pr5_spawn_schema_children", description = "spawn schema children", input_schema = { type = "object", properties = {} } }},
                local_tools = { inherited_ordinary = {
                    description = "ordinary local handler inherited by children",
                    input_schema = { type = "object", properties = {} },
                    handler = function() return "ordinary inherited handler ran" end,
                } },
            }))
            local ticket = assert(parent:send(ctx, "schema parent"))
            local result = assert(ticket:wait(ctx))
            assert(result.status == "completed" and #result.output == 1 and result.output[1].answer == 42)
            assert(result.captured.answer == 42)
            assert(parent:close(ctx))
        "#,
        );
        let parent = fixture.request().await;
        parent
            .reply
            .send(common::canned_tool_use(
                "pr5_spawn_schema_children",
                json!({}),
            ))
            .unwrap();
        let plain = fixture.request().await;
        let plain_history = serde_json::to_string(&plain.messages).unwrap();
        assert!(
            plain.messages.iter().any(|message| {
                matches!(message.role, Role::User)
                    && message.content.iter().any(|block| {
                        matches!(block, ContentBlock::Text { text, .. } if text == "plain inherited child")
                    })
            }),
            "expected plain child request, received parent/tool retry instead; history: {plain_history}; tools: {:?}",
            plain.tools
        );
        let plain_tools = common::tool_names(&plain.tools);
        assert!(
            !plain_tools.iter().any(|name| name == "structured_output"),
            "parent-owned reporting tool leaked into plain child; history: {plain_history}; tools: {:?}",
            plain.tools
        );
        assert!(
            plain_tools
                .iter()
                .any(|name| name == "pr5_spawn_schema_children"),
            "ordinary inherited tool was lost: {:?}",
            plain.tools
        );
        plain
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 999 }),
            ))
            .unwrap();
        let denied = fixture.request().await;
        let history = serde_json::to_string(&denied.messages).unwrap();
        assert!(
            history.contains("structured_output")
                && (history.contains("unknown tool") || history.contains("not available")),
            "child unexpectedly executed parent reporter: {history}"
        );
        assert!(plain_tools.iter().any(|name| name == "inherited_ordinary"));
        denied
            .reply
            .send(common::canned_tool_use("inherited_ordinary", json!({})))
            .unwrap();
        let ordinary = fixture.request().await;
        assert!(
            serde_json::to_string(&ordinary.messages)
                .unwrap()
                .contains("ordinary inherited handler ran")
        );
        ordinary
            .reply
            .send(common::canned_reply("plain done"))
            .unwrap();
        let own = fixture.request().await;
        let own_history = serde_json::to_string(&own.messages).unwrap();
        assert!(
            own_history.contains("own schema inherited child"),
            "expected own-schema child request, received retry; history: {own_history}; tools: {:?}",
            own.tools
        );
        assert_eq!(
            common::tool_names(&own.tools)
                .iter()
                .filter(|name| name.as_str() == "structured_output")
                .count(),
            1,
            "child's own schema reporter collided with inherited reporter: {:?}",
            own.tools
        );
        own.reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 7 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("own child done"))
            .unwrap();
        let parent_resume = fixture.request().await;
        assert!(
            serde_json::to_string(&parent_resume.messages)
                .unwrap()
                .contains("children isolated"),
            "parent tool history: {:?}",
            parent_resume.messages
        );
        parent_resume
            .reply
            .send(common::canned_reply("parent without report"))
            .unwrap();
        let nudged = fixture.request().await;
        assert!(
            serde_json::to_string(&nudged.messages)
                .unwrap()
                .contains("You did not call the structured_output tool"),
            "child report incorrectly satisfied parent completion: {:?}",
            nudged.messages
        );
        nudged
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 42 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("parent done"))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn public_output_schema_retries_invalid_reports_and_resets_each_turn() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_schema_retry",
            r#"
            local child = assert(maki.agent.spawn(ctx, {
                inherit_provider = true, silent = true,
                output_schema = { type = "object", properties = { answer = { type = "integer" } }, required = { "answer" } },
                local_tools = { ordinary = {
                    description = "ordinary tool alongside schema reporting",
                    input_schema = { type = "object", properties = {} },
                    handler = function() return "ordinary completed" end,
                } },
            }))
            local first = assert(child:send(ctx, "first schema turn"))
            local result = assert(first:wait(ctx))
            assert(result.status == "completed" and #result.output == 1 and result.output[1].answer == 42)
            assert(result.captured.answer == result.output[#result.output].answer)
            local second = assert(child:send(ctx, "second schema turn"))
            local next_result = assert(second:wait(ctx))
            assert(next_result.status == "completed" and #next_result.output == 1 and next_result.output[1].answer == 7)
            assert(next_result.captured.answer == next_result.output[#next_result.output].answer)
            assert(assert(first:result(ctx)).output[1].answer == 42)
            assert(child:close(ctx))
            assert(assert(first:result(ctx)).output[1].answer == 42)
        "#,
        );
        let first = fixture.request().await;
        assert!(common::tool_names(&first.tools).contains(&"structured_output".to_owned()));
        assert!(common::tool_names(&first.tools).contains(&"ordinary".to_owned()));
        first
            .reply
            .send(common::canned_tool_use("ordinary", json!({})))
            .unwrap();
        let ordinary = fixture.request().await;
        assert!(
            serde_json::to_string(&ordinary.messages)
                .unwrap()
                .contains("ordinary completed")
        );
        ordinary
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": "invalid" }),
            ))
            .unwrap();
        let invalid = fixture.request().await;
        assert!(
            serde_json::to_string(&invalid.messages)
                .unwrap()
                .contains("Fix the errors"),
            "invalid report history: {:?}",
            invalid.messages
        );
        invalid
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 42 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("first complete"))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("missing report on second turn"))
            .unwrap();
        let nudged = fixture.request().await;
        assert!(
            serde_json::to_string(&nudged.messages)
                .unwrap()
                .contains("You did not call the structured_output tool")
        );
        nudged
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 7 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("second complete"))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn retained_unmanaged_tool_context_cannot_authorize_another_invocation() {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_session_provider_preparer(
        Arc::clone(&registry),
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
    )
    .unwrap();
    host.load_source("pr5_unmanaged_origin", &format!(r#"
        local retained
        maki.api.register_tool({{
            name = "pr5_origin_capture", description = "retain an unmanaged invocation context",
            schema = {{ type = "object", properties = {{}} }},
            handler = function(_, ctx) retained = ctx; return "captured" end,
        }})
        maki.api.register_tool({{
            name = "pr5_origin_check", description = "reject a context from another invocation",
            schema = {{ type = "object", properties = {{}} }},
            handler = function(_, ctx)
                local stale, stale_err = maki.agent.spawn(retained, {{ inherit_provider = true, tools = {{}} }})
                assert(stale == nil and stale_err == {INVOCATION_EXPIRED:?}, stale_err)
                local fresh = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                local ticket = assert(fresh:send(ctx, "fresh unmanaged context"))
                assert(assert(ticket:wait(ctx)).text == "fresh reply")
                assert(fresh:close(ctx))
                return "origin rejected"
            end,
        }})
    "#)).unwrap();
    let (context, _events, _cancel) =
        common::ctx_with_replies(vec![common::canned_reply("fresh reply")]);
    assert_eq!(
        common::exec_tool_text(&registry, &context, "pr5_origin_capture", json!({})).unwrap(),
        "captured"
    );
    assert_eq!(
        common::exec_tool_text(&registry, &context, "pr5_origin_check", json!({})).unwrap(),
        "origin rejected"
    );
}

#[test]
fn trusted_legacy_session_survives_commands_but_not_plugin_reload() {
    smol::block_on(async {
        const OWNER: &str = "pr6_legacy_session_owner";
        const FOREIGN: &str = "pr6_legacy_session_foreign";
        const CREATE: &str = "/pr6_legacy_session_create";
        const SEND: &str = "/pr6_legacy_session_send";
        const DENIED: &str = "/pr6_legacy_session_denied";
        const REVOKED: &str = "/pr6_legacy_session_revoked";
        const REPLY: &str = "trusted legacy reply";
        const CHECKPOINT: &str = "trusted legacy status checkpoint";
        const GENERATION_ERROR: &str = "session belongs to another plugin generation";
        let fixture = Fixture::start("pr6_legacy_session_root", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "legacy root completion")
                .await
                .unwrap(),
            Ok(())
        );
        let handle = fixture._host.event_handle();
        fixture._host.load_source(OWNER, &format!(r#"
            maki.api.register_command({{
                name = "{CREATE}", tui_only = false,
                handler = function(_, ctx)
                    local session = assert(maki.agent.session(ctx, {{
                        inherit_provider = true, silent = true, auto_deliver = false, tools = {{}},
                    }}))
                    maki.store.register("pr6_legacy_sessions", "owner", session)
                    assert(assert(session:prompt("create trusted legacy session")).text == "{REPLY}")
                    local checkpoint = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                    assert(assert(checkpoint:prompt(ctx, "{CHECKPOINT}")).text == "{CHECKPOINT}")
                    assert(checkpoint:close(ctx))
                    local status = assert(session:status())
                    assert(status.status == "done" and status.result and status.result.text == "{REPLY}", maki.json.encode(status))
                end,
            }})
            maki.api.register_command({{
                name = "{SEND}", tui_only = false,
                handler = function()
                    local session = assert(maki.store.collect("pr6_legacy_sessions").owner)
                    assert(session:send("later trusted legacy command"))
                    assert(assert(session:status()).status == "running")
                end,
            }})
        "#)).unwrap();
        let run = |plugin: &str, command: &str| {
            handle.run_command_for_test(Arc::from(plugin), Arc::from(command), String::new(), 0)
        };
        let created = run(OWNER, CREATE);
        let request = request_with_command_diagnostics(
            &fixture.requests,
            &created,
            "trusted legacy creation request",
        )
        .await;
        assert!(
            serde_json::to_string(&request.messages)
                .unwrap()
                .contains("create trusted legacy session")
        );
        settle_legacy_reply(&fixture.manager, &request, REPLY).await;
        let checkpoint = request_with_command_diagnostics(
            &fixture.requests,
            &created,
            "trusted legacy status checkpoint",
        )
        .await;
        assert!(
            serde_json::to_string(&checkpoint.messages)
                .unwrap()
                .contains(CHECKPOINT)
        );
        checkpoint
            .reply
            .send(common::canned_reply(CHECKPOINT))
            .unwrap();
        assert_eq!(created.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        assert_eq!(run(OWNER, SEND).recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        let gated = bounded(fixture.requests.recv_async(), "later legacy send request")
            .await
            .unwrap();
        assert!(
            serde_json::to_string(&gated.messages)
                .unwrap()
                .contains("later trusted legacy command")
        );
        let denied_source = format!(
            r#"
            local retained = assert(maki.store.collect("pr6_legacy_sessions").owner)
            maki.api.register_command({{
                name = "{DENIED}", tui_only = false,
                handler = function()
                    local session = retained
                    for _, operation in ipairs({{ "send", "prompt", "status", "close" }}) do
                        local value, err = session[operation](session, "forbidden legacy request")
                        assert(value == nil and err == "{GENERATION_ERROR}", tostring(err))
                    end
                end,
            }})
        "#
        );
        fixture
            ._host
            .load_source(
                FOREIGN,
                &format!(
                    "{denied_source}\nmaki.store.register('pr6_legacy_sessions', 'retained', retained)"
                ),
            )
            .unwrap();
        assert_eq!(
            run(FOREIGN, DENIED).recv_timeout(TEST_TIMEOUT).unwrap(),
            Ok(())
        );
        assert!(!gated.reply.is_disconnected());
        assert!(
            fixture.requests.is_empty(),
            "foreign Session admitted a request"
        );
        fixture
            ._host
            .queue_load_source_for_test(
                OWNER,
                &denied_source
                    .replace(DENIED, REVOKED)
                    .replace(".owner)", ".retained)"),
            )
            .unwrap()
            .recv_timeout(TEST_TIMEOUT)
            .unwrap()
            .unwrap();
        wait_for_gate_drop(&gated, "legacy Session reload cancellation").await;
        assert_eq!(
            run(OWNER, REVOKED).recv_timeout(TEST_TIMEOUT).unwrap(),
            Ok(())
        );
        wait_for_child_reclamation(
            &fixture.manager,
            "reload closes retained legacy Session child",
        )
        .await;
        assert!(
            fixture.requests.is_empty(),
            "revoked Session admitted a request"
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn delegated_legacy_session_expires_after_creating_managed_turn() {
    smol::block_on(async {
        const TOOL: &str = "pr6_delegated_legacy_session";
        const CHECK: &str = "/pr6_delegated_legacy_expired";
        const REPLY: &str = "delegated legacy reply";
        const CHECKPOINT: &str = "delegated legacy status checkpoint";
        let checker_source = format!(
            r#"
            maki.api.register_command({{
                name = "{CHECK}", tui_only = false,
                handler = function()
                    local session = assert(maki.store.collect("pr6_delegated_sessions").retained)
                    for _, operation in ipairs({{ "send", "prompt", "status", "close" }}) do
                        local value, err = session[operation](session, "expired legacy request")
                        assert(value == nil and err == {MANAGED_AUTHORITY_INACTIVE:?}, tostring(err))
                    end
                end,
            }})
        "#
        );
        let fixture = Fixture::start_with_options(
            TOOL,
            FixtureOptions {
                plugin_source: checker_source,
                ..Default::default()
            },
            &format!(
                r#"
            local session = assert(maki.agent.session(ctx, {{
                inherit_provider = true, silent = true, auto_deliver = false, tools = {{}},
            }}))
            maki.store.register("pr6_delegated_sessions", "retained", session)
            assert(assert(session:prompt("delegated legacy request")).text == "{REPLY}")
            local checkpoint = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            assert(assert(checkpoint:prompt(ctx, "{CHECKPOINT}")).text == "{CHECKPOINT}")
            assert(checkpoint:close(ctx))
            local status = assert(session:status())
            assert(status.status == "done" and status.result and status.result.text == "{REPLY}", maki.json.encode(status))
        "#
            ),
        );
        let request = fixture.request().await;
        assert!(
            serde_json::to_string(&request.messages)
                .unwrap()
                .contains("delegated legacy request")
        );
        settle_legacy_reply(&fixture.manager, &request, REPLY).await;
        let checkpoint = fixture.request().await;
        assert!(
            serde_json::to_string(&checkpoint.messages)
                .unwrap()
                .contains(CHECKPOINT)
        );
        checkpoint
            .reply
            .send(common::canned_reply(CHECKPOINT))
            .unwrap();
        assert_eq!(
            bounded(
                fixture.completed.recv_async(),
                "delegated legacy tool completion"
            )
            .await
            .unwrap(),
            Ok(())
        );
        assert!(matches!(
            bounded(
                fixture.root_ticket.wait(),
                "delegated legacy turn settlement"
            )
            .await,
            TurnOutcome::Completed { turn_id, .. } if turn_id == fixture.root_ticket.turn_id()
        ));
        assert_eq!(
            fixture
                ._host
                .event_handle()
                .run_command_for_test(Arc::from(TOOL), Arc::from(CHECK), String::new(), 0)
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        assert!(
            fixture.requests.is_empty(),
            "expired Session admitted a request"
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test_case::test_case("error('reject candidate')"; "execution_failure")]
#[test_case::test_case(r#"
    maki.api.register_tool({
        name = "pr5_reload_root", description = "conflicting candidate",
        schema = { type = "object", properties = {} }, handler = function() end,
    })
"#; "publication_failure")]
fn rejected_reload_preserves_agents_and_subscriptions(candidate: &str) {
    smol::block_on(async {
        const OWNER: &str = "pr5_reload_resources";
        const BEGIN: &str = "/pr5_reload_begin";
        const EXERCISE: &str = "/pr5_reload_exercise";
        const REPLY: &str = "resources survived";
        let fixture = Fixture::start("pr5_reload_root", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "reload root completion")
                .await
                .unwrap(),
            Ok(())
        );
        let handle = fixture._host.event_handle();
        fixture._host.load_source(OWNER, r#"
            local child, observer, subscription, ticket, observer_ticket
            maki.api.register_command({
                name = "/pr5_reload_begin", tui_only = false,
                handler = function(_, ctx)
                    child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    observer = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    subscription = assert(child:on_turn_start(ctx, function(_, callback_ctx)
                        observer_ticket = assert(observer:send(callback_ctx, "subscription after rejected reload"))
                        assert(observer_ticket:wait(callback_ctx))
                    end))
                    ticket = assert(child:send(ctx, "work across rejected reload"))
                end,
            })
            maki.api.register_command({
                name = "/pr5_reload_exercise", tui_only = false,
                handler = function(_, ctx)
                    assert(assert(ticket:wait(ctx)).text == "resources survived")
                    assert(observer_ticket:wait(ctx))
                    local next_ticket = assert(child:send(ctx, "child after rejected reload"))
                    assert(assert(next_ticket:wait(ctx)).text == "resources survived")
                end,
            })
        "#).unwrap();
        let run = |command: &str| {
            handle.run_command_for_test(Arc::from(OWNER), Arc::from(command), String::new(), 0)
        };
        assert_eq!(run(BEGIN).recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        let first = bounded(fixture.requests.recv_async(), "persistent reload request")
            .await
            .unwrap();
        let second = bounded(fixture.requests.recv_async(), "parked callback request")
            .await
            .unwrap();
        let (active, callback_work) = if serde_json::to_string(&first.messages)
            .unwrap()
            .contains("work across rejected reload")
        {
            (first, second)
        } else {
            (second, first)
        };
        let rejected = fixture
            ._host
            .queue_load_source_for_test(OWNER, candidate)
            .unwrap();
        assert!(rejected.recv_timeout(TEST_TIMEOUT).unwrap().is_err());
        assert!(
            !callback_work.reply.is_disconnected(),
            "rejected reload cancelled callback work"
        );
        callback_work
            .reply
            .send(common::canned_reply(REPLY))
            .unwrap();
        assert!(
            !active.reply.is_disconnected(),
            "rejected reload cancelled old work"
        );
        active.reply.send(common::canned_reply(REPLY)).unwrap();
        let command = run(EXERCISE);
        let first = request_with_command_diagnostics(
            &fixture.requests,
            &command,
            "first preserved resource request",
        )
        .await;
        let second = request_with_command_diagnostics(
            &fixture.requests,
            &command,
            "second preserved resource request",
        )
        .await;
        let messages =
            [&first, &second].map(|request| serde_json::to_string(&request.messages).unwrap());
        assert!(
            messages
                .iter()
                .any(|text| text.contains("child after rejected reload"))
        );
        assert!(
            messages
                .iter()
                .any(|text| text.contains("subscription after rejected reload"))
        );
        first.reply.send(common::canned_reply(REPLY)).unwrap();
        second.reply.send(common::canned_reply(REPLY)).unwrap();
        assert_eq!(command.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));

        assert_eq!(run(BEGIN).recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        let replacement_work =
            bounded(fixture.requests.recv_async(), "work before accepted reload")
                .await
                .unwrap();
        let replacement_callback = bounded(
            fixture.requests.recv_async(),
            "parked callback before accepted reload",
        )
        .await
        .unwrap();
        fixture
            ._host
            .queue_load_source_for_test(OWNER, "")
            .unwrap()
            .recv_timeout(TEST_TIMEOUT)
            .unwrap()
            .unwrap();
        wait_for_gate_drop(&replacement_work, "accepted reload cancellation").await;
        wait_for_gate_drop(
            &replacement_callback,
            "accepted reload callback cancellation",
        )
        .await;
        wait_for_child_reclamation(&fixture.manager, "accepted reload closes owned agents").await;
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn repeated_deferred_subtrees_close_and_release_provider_gates() {
    smol::block_on(async {
        let fixture = Fixture::start("pr5_resource_root", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "resource root completion")
                .await
                .unwrap(),
            Ok(())
        );
        let rx = fixture.requests.clone();
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: fixture.manager.root().unwrap(),
                template,
            },
        ])));
        fixture._host.load_source("pr5_resource_cycles", r#"
            maki.api.register_command({
                name = "/pr5_resource_cycle", tui_only = false,
                handler = function(_, ctx)
                    local parent = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                    assert(ctx:defer_close(parent))
                    local subscription = assert(parent:on_turn_start(ctx, function(_, callback_ctx)
                        local child = assert(maki.agent.spawn(callback_ctx, { inherit_provider = true, silent = true, tools = {} }))
                        assert(callback_ctx:defer_close(child))
                        assert(assert(child:send(callback_ctx, "cycle descendant")):wait(callback_ctx))
                    end))
                    assert(assert(parent:send(ctx, "cycle parent")):wait(ctx))
                    assert(subscription:close())
                end,
            })
        "#).unwrap();
        for _ in 0..4 {
            let command = handle.run_command_for_test(
                Arc::from("pr5_resource_cycles"),
                Arc::from("/pr5_resource_cycle"),
                String::new(),
                0,
            );
            let a = request_with_command_diagnostics(&rx, &command, "cycle first provider request")
                .await;
            let b = request_with_command_diagnostics(
                &rx,
                &command,
                "cycle descendant provider request",
            )
            .await;
            let (parent, descendant) = if serde_json::to_string(&a.messages)
                .unwrap()
                .contains("cycle parent")
            {
                (a, b)
            } else {
                (b, a)
            };
            parent
                .reply
                .send(common::canned_reply("cycle finished"))
                .unwrap();
            assert_eq!(command.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
            wait_for_gate_drop(&descendant, "deferred descendant provider cancellation").await;
            wait_for_child_reclamation(&fixture.manager, "deferred subtree graph drain").await;
        }
        fixture._host.unload("pr5_resource_cycles").unwrap();
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn public_output_schema_invalid_reports_exhaust_without_committing_output() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_schema_invalid_exhaustion",
            r#"
            local child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {},
                output_schema = { type = "object", properties = { answer = { type = "integer" } }, required = { "answer" } },
            }))
            local ticket = assert(child:send(ctx, "never correct report"))
            local result = assert(ticket:wait(ctx))
            assert(result.status == "failed" and #result.output == 0 and result.captured == nil)
            assert(result.error:find("does not match output_schema", 1, true))
            assert(child:close(ctx))
            assert(assert(ticket:result(ctx)).status == "failed")
        "#,
        );
        for _ in 0..3 {
            fixture
                .request()
                .await
                .reply
                .send(common::canned_tool_use(
                    "structured_output",
                    json!({ "answer": "invalid" }),
                ))
                .unwrap();
            let failed_report = fixture.request().await;
            assert!(
                serde_json::to_string(&failed_report.messages)
                    .unwrap()
                    .contains("Fix the errors"),
                "invalid report tool history: {:?}",
                failed_report.messages
            );
            failed_report
                .reply
                .send(common::canned_reply("invalid report is final"))
                .unwrap();
        }
        fixture.finish().await;
    });
}

#[test]
fn public_output_schema_missing_reports_exhaust_bounded_nudges() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_schema_missing",
            r#"
            local child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true,
                output_schema = { type = "object", properties = {}, additionalProperties = false },
            }))
            local result = assert(assert(child:send(ctx, "never report")):wait(ctx))
            assert(result.status == "failed" and #result.output == 0)
            assert(result.error:find("without calling structured_output", 1, true))
            assert(child:close(ctx))
        "#,
        );
        for _ in 0..3 {
            fixture
                .request()
                .await
                .reply
                .send(common::canned_reply("no structured report"))
                .unwrap();
        }
        fixture.finish().await;
    });
}

#[test]
fn saturated_event_callbacks_can_dispatch_a_child_lua_tool() {
    smol::block_on(async {
        const CALLBACKS: usize = maki_lua::MAX_INFLIGHT_TOOLS;
        const TOOL: &str = "pr5_saturation_child_tool";
        let fixture = Fixture::start("pr5_saturation_root", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "saturation root completion")
                .await
                .unwrap(),
            Ok(())
        );
        let requests = fixture.requests.clone();
        let (mut template, _events, _cancel) = common::ctx_with_canned_provider();
        let effective = fixture
            .manager
            .root()
            .unwrap()
            .effective_config()
            .unwrap()
            .unwrap();
        template.provider = Arc::clone(&effective.settings.provider);
        template.model = Arc::new(effective.settings.model.clone());
        template.mode = effective.mode.clone();
        template.mode_def = effective.mode_def.clone().map(Arc::new);
        let handle = fixture._host.event_handle();
        fixture._host.load_source("pr5_saturation", &format!(r#"
            local subscriptions = {{}}
            local observed, worker, checkpoint, worker_turn
            local entered = 0
            local errors = {{}}
            local tool_entered = 0
            maki.api.register_command({{
                name = "/pr5_saturation_diagnostic", tui_only = false,
                handler = function()
                    error("entered=" .. entered .. " tool_entered=" .. tool_entered .. " errors=" .. table.concat(errors, " | "))
                end,
            }})
            maki.api.register_tool({{
                name = "{TOOL}", description = "saturation child tool", audiences = {{ "main", "general_sub" }},
                schema = {{ type = "object", properties = {{}} }},
                handler = function() tool_entered = tool_entered + 1; return "child tool reached" end,
            }})
            maki.api.register_command({{
                name = "/pr5_saturation_begin", tui_only = false,
                handler = function(_, ctx)
                    observed = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                    worker = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, audience = "general_sub",
                        tools = {{{{ name = "{TOOL}", description = "saturation child tool",
                            input_schema = {{ type = "object", properties = {{}} }} }}}} }}))
                    checkpoint = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                    worker_turn = assert(worker:send(ctx, "worker callback"))
                    for i = 1, {CALLBACKS} do
                        subscriptions[i] = assert(observed:on_turn_start(ctx, function(_, callback_ctx)
                            local ok, err = pcall(function()
                                entered = entered + 1
                                if entered == {CALLBACKS} then
                                    assert(assert(checkpoint:send(callback_ctx, "all callbacks entered")):wait(callback_ctx))
                                end
                                assert(worker_turn:wait(callback_ctx))
                            end)
                            if not ok then errors[#errors + 1] = tostring(err) end
                        end))
                    end
                    assert(assert(observed:send(ctx, "observed callback trigger")):wait(ctx))
                end,
            }})
            maki.api.register_command({{
                name = "/pr5_saturation_close", tui_only = false,
                handler = function(_, ctx)
                    for _, subscription in ipairs(subscriptions) do assert(subscription:close()) end
                    assert(observed:close(ctx))
                    assert(worker:close(ctx))
                    assert(checkpoint:close(ctx))
                end,
            }})
        "#)).unwrap();
        template.registry = Arc::clone(ToolRegistry::global_arc());
        assert!(
            template.registry.get(TOOL).is_some(),
            "saturation tool must be in parent registry"
        );
        let request_tools = maki_agent::tools::RequestTools::assembled(
            json!([
                { "name": "pr5_saturation_root", "description": "saturation driver", "input_schema": { "type": "object", "properties": {} } },
                { "name": TOOL, "description": "saturation child tool", "input_schema": { "type": "object", "properties": {} } },
            ]),
            &template.config,
            &template.model,
        );
        template.mode_def = Some(Arc::new(template.modes.current(&template.mode)));
        template.tool_filter = Arc::clone(request_tools.filter());
        template.request_tools = Some(request_tools);
        template.turn_bindings = Arc::new(maki_agent::tools::TurnToolBindings::capture(
            &template.registry,
            &template.local_tools,
            template.mcp.as_ref(),
        ));
        let callable = maki_agent::agent::tool_dispatch::callable(&template);
        assert!(
            callable.iter().any(|tool| tool.name == TOOL),
            "parent saturation capability missing: callable={:?}, audience={:?}, filter={}, mode={:?}, binding={}, registered_audience={:?}",
            callable.iter().map(|tool| &tool.name).collect::<Vec<_>>(),
            template.audience,
            template.tool_filter.matches(TOOL),
            template.mode_def,
            template.turn_bindings.get(TOOL).is_some(),
            template
                .registry
                .get(TOOL)
                .map(|entry| entry.tool.audience())
        );
        let services = Arc::new(RegisteredOrchestrationTargets::new(vec![TrustedTarget {
            target: fixture.manager.root().unwrap(),
            template,
        }]));
        handle.install_orchestration_services(services.clone());
        fixture._host.wait_for_worker_barrier_for_test();
        let command = handle.run_command_for_test(
            Arc::from("pr5_saturation"),
            Arc::from("/pr5_saturation_begin"),
            String::new(),
            0,
        );
        let received = Mutex::new(Vec::<String>::new());
        let receive = || {
            futures_lite::future::race(
                async {
                    let request = requests.recv_async().await.unwrap();
                    received.lock().unwrap().push(format!(
                        "model={} tools={} messages={}",
                        request.model,
                        request.tools,
                        serde_json::to_string(&request.messages).unwrap()
                    ));
                    request
                },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    let diagnostic = handle
                        .run_command_for_test(
                            Arc::from("pr5_saturation"),
                            Arc::from("/pr5_saturation_diagnostic"),
                            String::new(),
                            0,
                        )
                        .recv_timeout(TEST_TIMEOUT);
                    panic!(
                        "saturated callback timeout: command={:?}, diagnostic={diagnostic:?}, received={:?}",
                        command.try_recv(),
                        received.lock().unwrap()
                    )
                },
            )
        };
        let mut observed = None;
        let mut worker = None;
        let mut checkpoint = None;
        for _ in 0..3 {
            let request = receive().await;
            let messages = serde_json::to_string(&request.messages).unwrap();
            if messages.contains("all callbacks entered") {
                checkpoint = Some(request);
            } else if messages.contains("observed callback trigger") {
                observed = Some(request);
            } else {
                assert!(
                    messages.contains("worker callback"),
                    "unclassified saturation request: {messages}"
                );
                assert!(
                    request
                        .tools
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|definition| definition["name"] == TOOL),
                    "worker missing saturation tool: {}",
                    request.tools
                );
                assert!(
                    worker.is_none(),
                    "duplicate worker initial request: {messages}"
                );
                worker = Some(request);
            }
        }
        // Reloading an unrelated plugin must not wait on these callbacks or block their child tool.
        let release = fixture._host.pause_worker_for_test();
        let reloaded = fixture
            ._host
            .queue_load_source_for_test("pr5_unrelated_reload", "")
            .unwrap();
        worker
            .unwrap()
            .reply
            .send(common::canned_tool_use(TOOL, json!({})))
            .unwrap();
        release.send(()).unwrap();
        reloaded
            .recv_timeout(TEST_TIMEOUT)
            .expect("unrelated plugin reload waited on callback child tool")
            .unwrap();
        let after_tool = receive().await;
        let history = serde_json::to_string(&after_tool.messages).unwrap();
        let capabilities = services.targets.lock().unwrap().iter().map(|target| {
            let ctx = &target.template;
            format!("id={} audience={:?} filter={} mode_tools={:?} binding={} registry_audience={:?} effective={:?}", target.target.id(), ctx.audience, ctx.tool_filter.matches(TOOL), ctx.mode_def.as_ref().map(|mode| &mode.tools), ctx.turn_bindings.get(TOOL).is_some(), ctx.registry.get(TOOL).map(|entry| entry.tool.audience()), target.target.effective_config().ok().flatten().map(|config| config.mode_def.clone()))
        }).collect::<Vec<_>>();
        assert!(
            history.contains("child tool reached"),
            "worker tool history: {history}; capabilities={capabilities:?}"
        );
        assert_eq!(command.try_recv(), Err(flume::TryRecvError::Empty));
        assert_eq!(
            handle
                .run_command_for_test(
                    Arc::from("pr5_saturation"),
                    Arc::from("/pr5_saturation_close"),
                    String::new(),
                    0
                )
                .recv_timeout(TEST_TIMEOUT)
                .unwrap(),
            Ok(())
        );
        drop(after_tool);
        drop(checkpoint);
        drop(observed);
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn public_agent_ticket_cancel_stops_gated_request_and_keeps_actor_reusable() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_ticket_cancel",
            &format!(
                r#"
            local invalid, schema_err = maki.agent.spawn(ctx, {{ inherit_provider = true, tools = {{}}, output_schema = {{ type = "string" }} }})
            assert(invalid == nil and schema_err == {OUTPUT_SCHEMA_NOT_OBJECT:?}, schema_err)
            local collision, collision_err = maki.agent.spawn(ctx, {{ inherit_provider = true,
                tools = {{{{ name = "structured_output", description = "conflict", input_schema = {{ type = "object" }} }}}},
                output_schema = {{ type = "object", properties = {{}} }},
            }})
            assert(collision == nil and collision_err == {STRUCTURED_OUTPUT_CONFLICT:?}, collision_err)
            local local_collision, local_err = maki.agent.spawn(ctx, {{ inherit_provider = true, tools = {{}},
                output_schema = {{ type = "object", properties = {{}} }},
                local_tools = {{ structured_output = {{ description = "conflict", input_schema = {{ type = "object" }}, handler = function() return "never" end }} }},
            }})
            assert(local_collision == nil and local_err == {STRUCTURED_OUTPUT_CONFLICT:?}, local_err)
            local child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}}, output_schema = {{ type = "object", properties = {{ answer = {{ type = "integer" }} }}, required = {{ "answer" }} }} }}))
            local cancelled = assert(child:send(ctx, "cancel gated request"))
            local checkpoint = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            assert(assert(assert(checkpoint:send(ctx, "ready to cancel")):wait(ctx)).text == "ready to cancel")
            assert(cancelled:cancel(ctx))
            local result, err = cancelled:wait(ctx)
            assert(err == nil and result.status == "cancelled" and #result.output == 0)
            local retained, retained_err = cancelled:result(ctx)
            assert(retained_err == nil and retained.status == "cancelled")
            assert(retained.cancellation_reason == result.cancellation_reason)
            local next = assert(child:send(ctx, "after cancellation"))
            assert(assert(next:wait(ctx)).text == "{SECOND_REPLY}")
            assert(child:close(ctx))
            assert(checkpoint:close(ctx))
        "#
            ),
        );
        let request_a = fixture.request().await;
        let request_b = fixture.request().await;
        let (cancelled, checkpoint) = if serde_json::to_string(&request_a.messages)
            .unwrap()
            .contains("cancel gated request")
        {
            (request_a, request_b)
        } else {
            (request_b, request_a)
        };
        checkpoint
            .reply
            .send(common::canned_reply("ready to cancel"))
            .unwrap();
        let next = fixture.request().await;
        assert!(
            cancelled.reply.is_disconnected(),
            "cancelled provider future is still alive"
        );
        assert!(
            serde_json::to_string(&next.messages)
                .unwrap()
                .contains("after cancellation")
        );
        next.reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 7 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

#[test_case::test_case(true; "capture_input")]
#[test_case::test_case(false; "explicit_report")]
fn inherited_local_capture_belongs_to_executing_grandchild(capture_input: bool) {
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            "pr6_nested_capture",
            FixtureOptions {
                nested_capture: true,
                ..Default::default()
            },
            &format!(
                r#"
            local child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true,
                tools = {{ {{ name = "pr6_spawn_grandchild", description = "spawn grandchild", input_schema = {{ type = "object" }} }} }},
                local_tools = {{ inherited_capture = {{
                    description = "capture answer", input_schema = {{ type = "object" }},
                    capture_input = {capture_input},
                    handler = function(input)
                        if not {capture_input} then assert(maki.agent.report_task_result(input)) end
                        return "captured"
                    end,
                }} }},
            }}))
            local result = assert(assert(child:send(ctx, "spawn grandchild")):wait(ctx))
            assert(result.captured == nil and #result.output == 0, "grandchild capture leaked into parent")
            assert(child:close(ctx))
        "#
            ),
        );
        fixture
            .request()
            .await
            .reply
            .send(common::canned_tool_use("pr6_spawn_grandchild", json!({})))
            .unwrap();
        let grandchild = fixture.request().await;
        assert!(common::tool_names(&grandchild.tools).contains(&"inherited_capture".to_owned()));
        grandchild
            .reply
            .send(common::canned_tool_use(
                "inherited_capture",
                json!({ "answer": 42 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("grandchild complete"))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_tool_use(
                "inherited_capture",
                json!({ "answer": 7 }),
            ))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("grandchild second turn complete"))
            .unwrap();
        fixture
            .request()
            .await
            .reply
            .send(common::canned_reply("parent complete"))
            .unwrap();
        fixture.finish().await;
    });
}

#[test]
fn bundled_task_gates_subagent_types_on_the_executing_agent_mode() {
    smol::block_on(async {
        const GENERAL_BLOCKED: &str =
            "general subagents are blocked in plan mode; use research or plan_reviewer (read-only)";
        let fixture = Fixture::start_with_options(
            "pr5_plan_mode_gate",
            FixtureOptions {
                mode: Some(AgentMode::Plan("/test/gate-plan.md".into())),
                bundled_tasks: true,
                ..Default::default()
            },
            &format!(
                r#"
            local text, err = maki.agent.call_tool(ctx, "task_spawn", {{
                description = "blocked", prompt = "write things", subagent_type = "general",
            }})
            assert(text == nil and err == "{GENERAL_BLOCKED}", tostring(err))
        "#
            ),
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "plan mode gate")
                .await
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn bundled_task_gates_new_work_on_admitted_mode_across_configuration_changes() {
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            "pr5_plan_mode_send",
            FixtureOptions {
                bundled_tasks: true,
                ..Default::default()
            },
            r#"
            local text = assert(maki.agent.call_tool(ctx, "task_spawn", {
                description = "writer", prompt = "edit things", subagent_type = "general",
            }))
            local task = maki.json.decode(text)
            local parent = assert(maki.agent.current(ctx))
            assert(parent:set_mode(ctx, "plan") == "plan")
            assert(ctx:mode() == "build" and parent:mode(ctx) == "plan")
            local sent, err = maki.agent.call_tool(ctx, "task_send", {
                task_id = task.task_id, message = "keep editing",
            })
            assert(sent ~= nil and err == nil, tostring(err))
            assert(maki.agent.call_tool(ctx, "task_despawn", { task_id = task.task_id }))
        "#,
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "admitted mode send gate")
                .await
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn spawn_takes_the_shared_selection_and_rejects_other_options() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_spawn_selection",
            &format!(
                r#"
            local parent = assert(maki.agent.current(ctx))
            local selection = assert(parent:model(ctx))
            local child = assert(maki.agent.spawn(ctx, {{
                inherit_provider = true, silent = true, tools = {{}}, model = selection,
            }}))
            local copied = assert(child:model(ctx))
            assert(copied.spec == selection.spec and copied.thinking == selection.thinking and copied.fast == selection.fast)
            assert(child:close(ctx))
            for key, opts in pairs({{
                model_spec = {{ model_spec = selection.spec }},
                thinking = {{ thinking = "high" }},
                fast = {{ fast = true }},
            }}) do
                local spawned, err = maki.agent.spawn(ctx, opts)
                assert(spawned == nil and err == "{UNKNOWN_SPAWN_OPTION}: " .. key, tostring(err))
            end
            local spawned, err = maki.agent.spawn(ctx, {{ model = {{ spec = selection.spec, fast = "maybe" }} }})
            assert(spawned == nil and err == "{FAST_INVALID}", tostring(err))
        "#,
                FAST_INVALID = maki_providers::ModelSelectionError::InvalidFast,
            ),
        );
        assert_eq!(
            bounded(fixture.completed.recv_async(), "spawn selection")
                .await
                .unwrap(),
            Ok(())
        );
        shutdown_manager(&fixture.manager).await;
    });
}

#[test_case::test_case(false, false; "agent_without_wait")]
#[test_case::test_case(false, true; "agent_with_wait")]
#[test_case::test_case(true, false; "bundled_task_without_wait")]
#[test_case::test_case(true, true; "bundled_task_with_wait")]
fn managed_capacity_one_delivers_child_once_unless_waited(bundled_tasks: bool, wait: bool) {
    smol::block_on(async {
        const TOOL: &str = "managed_capacity_one_delivery";
        let spawn = if bundled_tasks {
            r#"
            local text, err = maki.agent.call_tool(ctx, "task_spawn", {
                description = "automatic delivery", prompt = "report answer", subagent_type = "general",
                output_schema = schema,
            })
            assert(text ~= nil and err == nil, err)
            local task = maki.json.decode(text)
            "#
        } else {
            r#"
            local child = assert(maki.agent.spawn(ctx, {
                inherit_provider = true, tools = {}, output_schema = schema,
            }))
            local ticket = assert(child:send(ctx, "report answer"))
            "#
        };
        let checkpoint = if !wait {
            ""
        } else if bundled_tasks {
            r#"
            local text, err = maki.agent.call_tool(ctx, "task_wait", {
                task_id = task.task_id, turn_id = task.turn_id,
            })
            assert(text ~= nil and err == nil, err)
            local result = maki.json.decode(text)
            assert(result.status == "done" and result.result.captured.answer == 42)
            local again = assert(maki.agent.call_tool(ctx, "task_wait", {
                task_id = task.task_id, turn_id = task.turn_id,
            }))
            assert(maki.json.decode(again).result.captured.answer == 42)
            "#
        } else {
            r#"
            local result = assert(ticket:wait(ctx))
            assert(result.captured.answer == 42 and result.output[1].answer == 42)
            assert(assert(ticket:wait(ctx)).captured.answer == 42)
            assert(assert(ticket:result(ctx)).captured.answer == 42)
            "#
        };
        let fixture = Fixture::start_with_options(
            TOOL,
            FixtureOptions {
                bundled_tasks,
                limits: AgentLimits {
                    max_concurrent_agent_turns: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
            &format!(
                r#"
                local schema = {{ type = "object", properties = {{ answer = {{ type = "integer" }} }}, required = {{ "answer" }} }}
                {spawn}
                {checkpoint}
                "#
            ),
        );
        let root = fixture.manager.root().unwrap().actor().unwrap();
        let events = root.subscribe_events();
        let request = bounded(fixture.requests.recv_async(), "capacity-one child request")
            .await
            .unwrap();
        if wait {
            assert!(fixture.root_ticket.peek().is_none());
            assert!(fixture.completed.is_empty());
        } else {
            bounded(fixture.root_ticket.wait(), "nonwaiting spawn settlement").await;
        }
        let child = fixture
            .manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id.is_some())
            .unwrap();
        assert!(fixture.observations.is_empty());
        assert!(common::tool_names(&request.tools).contains(&"structured_output".to_owned()));
        request
            .reply
            .send(common::canned_tool_use(
                "structured_output",
                json!({ "answer": 42 }),
            ))
            .unwrap();
        let final_request = bounded(
            fixture.requests.recv_async(),
            "structured child continuation",
        )
        .await
        .unwrap();
        assert!(fixture.observations.is_empty());
        final_request
            .reply
            .send(common::canned_reply(FIRST_REPLY))
            .unwrap();
        if wait {
            bounded(fixture.root_ticket.wait(), "waiting root settlement").await;
            let statuses = bounded(
                async {
                    let mut statuses = Vec::new();
                    while !statuses.contains(&DeliveryStatus::Discarded(
                        TurnCancellationReason::Withdrawn,
                    )) {
                        if let ActorEvent::Delivery(event) = events.recv_async().await.unwrap() {
                            statuses.push(event.status);
                        }
                    }
                    statuses
                },
                "waited delivery withdrawal",
            )
            .await;
            assert!(matches!(
                statuses[..],
                [
                    DeliveryStatus::Accepted,
                    DeliveryStatus::Discarded(TurnCancellationReason::Withdrawn)
                ]
            ));
            assert!(fixture.observations.is_empty());
            fixture.finish().await;
            return;
        }
        let (parent_turn_id, provenance, messages) = bounded(
            fixture.observations.recv_async(),
            "parent lifecycle observation execution",
        )
        .await
        .unwrap();
        assert!(fixture.root_ticket.peek().is_some());
        let lifecycle = bounded(async {
            let mut lifecycle = Vec::new();
            loop {
                let event = events.recv_async().await.unwrap();
                let idle = matches!(event, ActorEvent::Idle { .. });
                lifecycle.push(event);
                if idle && lifecycle.iter().any(|event| matches!(event,
                    ActorEvent::Delivery(event) if matches!(event.status, DeliveryStatus::Executed { .. })
                )) {
                    return lifecycle;
                }
            }
        }, "parent delivery settlement").await;
        let deliveries: Vec<_> = lifecycle
            .iter()
            .filter_map(|event| match event {
                ActorEvent::Delivery(event) => Some(event),
                _ => None,
            })
            .collect();
        assert_eq!(
            deliveries
                .iter()
                .map(|event| event.status.clone())
                .collect::<Vec<_>>(),
            vec![
                DeliveryStatus::Accepted,
                DeliveryStatus::Executed { parent_turn_id },
            ]
        );
        let envelope = &deliveries[0].delivery;
        assert_eq!(envelope.parent_id, root.agent_id());
        assert_eq!(envelope.result.outcome.agent_id(), child.agent_id);
        assert!(matches!(
            envelope.result.outcome,
            TurnOutcome::Completed { .. }
        ));
        assert_eq!(envelope.result.text, FIRST_REPLY);
        assert_eq!(envelope.result.output, vec![json!({ "answer": 42 })]);
        assert_eq!(envelope.result.provenance.origin, TurnOrigin::Plugin);
        assert_eq!(
            envelope.result.provenance.source_agent,
            Some(root.agent_id())
        );
        assert_eq!(
            envelope.result.provenance.source_turn,
            Some(fixture.root_ticket.turn_id())
        );
        assert!(envelope.result.provenance.plugin.is_some());
        assert!(envelope.result.provenance.plugin_generation.is_some());
        assert_eq!(
            envelope.child_config.as_ref().unwrap().config.model.spec(),
            request.model
        );
        assert_eq!(
            envelope.child_config.as_ref().unwrap().config.mode,
            AgentMode::Build
        );
        assert_eq!(provenance.origin, TurnOrigin::Internal);
        assert_eq!(provenance.source_agent, None);
        assert_eq!(provenance.source_turn, None);
        assert_eq!(messages.len(), 1);
        assert!(messages[0].is_observation());
        assert_eq!(
            messages[0].observation_source,
            Some(ObservationSource::ChildResult {
                agent_id: child.agent_id.to_string(),
                turn_id: envelope.result.outcome.turn_id().to_string(),
            })
        );
        let starts: Vec<_> = lifecycle
            .iter()
            .filter_map(|event| match event {
                ActorEvent::Start { turn_id, .. } if *turn_id != fixture.root_ticket.turn_id() => {
                    Some(*turn_id)
                }
                _ => None,
            })
            .collect();
        assert_eq!(starts, vec![parent_turn_id]);
        assert_ne!(parent_turn_id, fixture.root_ticket.turn_id());
        assert!(fixture.observations.is_empty());
        fixture.finish().await;
    });
}

#[test]
fn managed_task_spawn_survives_origin_invocation_completion() {
    smol::block_on(async {
        let fixture = Fixture::start_with_options(
            "pr6_persistent_spawn",
            FixtureOptions {
                bundled_tasks: true,
                ..Default::default()
            },
            r#"
            local text, err = maki.agent.call_tool(ctx, "task_spawn", {
                description = "persistent child", prompt = "wait forever", subagent_type = "general",
            })
            assert(text ~= nil and err == nil, err)
        "#,
        );
        let gate = bounded(fixture.requests.recv_async(), "persistent child request")
            .await
            .unwrap();
        assert_eq!(
            bounded(fixture.completed.recv_async(), "spawn caller completion")
                .await
                .unwrap(),
            Ok(())
        );
        assert!(!gate.reply.is_disconnected());
        assert_eq!(fixture.manager.list().len(), 2);
        gate.reply
            .send(common::canned_reply("persistent complete"))
            .unwrap();
        shutdown_manager(&fixture.manager).await;
    });
}

#[test]
fn public_agent_ticket_surfaces_captured_local_tool_output() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_captured_output",
            &format!(
                r#"
            local child = assert(maki.agent.spawn(ctx, {{
                inherit_provider = true, silent = true, tools = {{}},
                local_tools = {{
                    {CAPTURE_TOOL} = {{
                        description = "commit a structured result",
                        input_schema = {{ type = "object", properties = {{ answer = {{ type = "integer" }} }}, required = {{ "answer" }} }},
                        capture_input = true,
                        handler = function(input)
                            assert(input.answer == 42)
                            return "committed"
                        end,
                    }},
                }},
            }}))
            local ticket = assert(child:send(ctx, "capture a structured answer"))
            local result = assert(ticket:wait(ctx))
            assert(result.text == "{CAPTURE_REPLY}")
            assert(result.captured.answer == 42)
            assert(assert(ticket:result(ctx)).captured.answer == 42)
            assert(child:close(ctx))
        "#
            ),
        );
        let first = fixture.request().await;
        assert_eq!(common::tool_names(&first.tools), [CAPTURE_TOOL]);
        assert_eq!(
            first.tools[0]["input_schema"]["required"],
            json!(["answer"])
        );
        first
            .reply
            .send(common::canned_tool_use(
                CAPTURE_TOOL,
                json!({ "answer": 42 }),
            ))
            .unwrap();
        let second = fixture.request().await;
        assert!(
            serde_json::to_string(&second.messages)
                .unwrap()
                .contains("committed"),
            "capture tool response history: {:?}",
            second.messages
        );
        second
            .reply
            .send(common::canned_reply(CAPTURE_REPLY))
            .unwrap();
        fixture.finish().await;
    });
}

async fn collect_deliveries(
    events: &flume::Receiver<ActorEvent>,
    statuses: &mut Vec<(String, DeliveryStatus)>,
    done: impl Fn(&[(String, DeliveryStatus)]) -> bool,
    operation: &str,
) {
    bounded(
        async {
            while !done(statuses) {
                if let ActorEvent::Delivery(event) = events.recv_async().await.unwrap() {
                    statuses.push((event.delivery.result.text.clone(), event.status));
                }
            }
        },
        operation,
    )
    .await
}

fn statuses_for(statuses: &[(String, DeliveryStatus)], text: &str) -> Vec<DeliveryStatus> {
    statuses
        .iter()
        .filter(|(delivered, _)| delivered == text)
        .map(|(_, status)| status.clone())
        .collect()
}

fn settled_deliveries(statuses: &[(String, DeliveryStatus)]) -> usize {
    statuses
        .iter()
        .filter(|(_, status)| !matches!(status, DeliveryStatus::Accepted))
        .count()
}

fn is_withdrawn(statuses: &[DeliveryStatus]) -> bool {
    matches!(
        statuses,
        [
            DeliveryStatus::Accepted,
            DeliveryStatus::Discarded(TurnCancellationReason::Withdrawn)
        ]
    )
}

#[test_case::test_case(false ; "result_read_keeps_delivery")]
#[test_case::test_case(true ; "settled_wait_withdraws_delivery")]
fn settled_turn_withdraws_delivery_only_when_waited(wait_settled: bool) {
    smol::block_on(async {
        const FIRST_PROMPT: &str = "settle first child";
        const SECOND_PROMPT: &str = "settle second child";
        let fixture = Fixture::start(
            "pr5_settled_delivery",
            &format!(
                r#"
            local spawn = {{ inherit_provider = true, tools = {{}} }}
            local first = assert(assert(maki.agent.spawn(ctx, spawn)):send(ctx, {FIRST_PROMPT:?}))
            local second = assert(assert(maki.agent.spawn(ctx, spawn)):send(ctx, {SECOND_PROMPT:?}))
            assert(assert(second:wait(ctx)).text == {SECOND_REPLY:?})
            assert(assert(first:result(ctx)).text == {FIRST_REPLY:?})
            if {wait_settled} then
                assert(assert(first:wait(ctx)).text == {FIRST_REPLY:?})
            end
        "#
            ),
        );
        let root = fixture.manager.root().unwrap().actor().unwrap();
        let events = root.subscribe_events();
        let request_a = fixture.request().await;
        let request_b = fixture.request().await;
        let (first, second) = if serde_json::to_string(&request_a.messages)
            .unwrap()
            .contains(FIRST_PROMPT)
        {
            (request_a, request_b)
        } else {
            (request_b, request_a)
        };
        first.reply.send(common::canned_reply(FIRST_REPLY)).unwrap();
        let mut statuses = Vec::new();
        collect_deliveries(
            &events,
            &mut statuses,
            |statuses| !statuses_for(statuses, FIRST_REPLY).is_empty(),
            "first child delivery acceptance",
        )
        .await;
        assert_eq!(
            statuses_for(&statuses, FIRST_REPLY),
            vec![DeliveryStatus::Accepted]
        );
        assert_eq!(
            root.latest_ticket().unwrap().turn_id(),
            fixture.root_ticket.turn_id()
        );
        assert!(fixture.observations.is_empty());
        second
            .reply
            .send(common::canned_reply(SECOND_REPLY))
            .unwrap();
        collect_deliveries(
            &events,
            &mut statuses,
            |statuses| settled_deliveries(statuses) == 2,
            "child delivery settlement",
        )
        .await;
        assert!(is_withdrawn(&statuses_for(&statuses, SECOND_REPLY)));
        let first_statuses = statuses_for(&statuses, FIRST_REPLY);
        if wait_settled {
            assert!(is_withdrawn(&first_statuses), "{first_statuses:?}");
            assert!(fixture.observations.is_empty());
        } else {
            let (parent_turn_id, _, _) = bounded(
                fixture.observations.recv_async(),
                "first delivery execution",
            )
            .await
            .unwrap();
            assert_eq!(
                first_statuses,
                vec![
                    DeliveryStatus::Accepted,
                    DeliveryStatus::Executed { parent_turn_id },
                ]
            );
        }
        fixture.finish().await;
    });
}

#[test]
fn trusted_command_wait_keeps_child_delivery() {
    smol::block_on(async {
        const PLUGIN: &str = "pr5_trusted_wait";
        const COMMAND: &str = "/pr5_trusted_wait";
        let fixture = Fixture::start("pr5_trusted_wait_root", "");
        assert_eq!(
            bounded(
                fixture.completed.recv_async(),
                "integration channel receive"
            )
            .await
            .unwrap(),
            Ok(())
        );
        bounded(fixture.root_ticket.wait(), "root turn settlement").await;
        let root = fixture.manager.root().unwrap();
        let events = root.actor().unwrap().subscribe_events();
        let (template, _events, _cancel) = common::ctx_with_canned_provider();
        let handle = fixture._host.event_handle();
        handle.install_orchestration_services(Arc::new(RegisteredOrchestrationTargets::new(vec![
            TrustedTarget {
                target: root,
                template,
            },
        ])));
        fixture
            ._host
            .load_source(
                PLUGIN,
                &format!(
                    r#"
            maki.api.register_command({{
                name = "{COMMAND}", tui_only = false,
                handler = function(_, ctx)
                    local child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, tools = {{}} }}))
                    local ticket = assert(child:send(ctx, "trusted"))
                    assert(assert(ticket:wait(ctx)).text == {FIRST_REPLY:?})
                end,
            }})
        "#
                ),
            )
            .unwrap();
        let command =
            handle.run_command_for_test(Arc::from(PLUGIN), Arc::from(COMMAND), String::new(), 0);
        bounded(fixture.requests.recv_async(), "trusted child request")
            .await
            .unwrap()
            .reply
            .send(common::canned_reply(FIRST_REPLY))
            .unwrap();
        assert_eq!(
            bounded(command.recv_async(), "trusted command wait")
                .await
                .unwrap(),
            Ok(())
        );
        let (parent_turn_id, _, _) = bounded(
            fixture.observations.recv_async(),
            "trusted delivery execution",
        )
        .await
        .unwrap();
        let mut statuses = Vec::new();
        collect_deliveries(
            &events,
            &mut statuses,
            |statuses| settled_deliveries(statuses) == 1,
            "trusted delivery settlement",
        )
        .await;
        assert_eq!(
            statuses_for(&statuses, FIRST_REPLY),
            vec![
                DeliveryStatus::Accepted,
                DeliveryStatus::Executed { parent_turn_id },
            ]
        );
        shutdown_manager(&fixture.manager).await;
    });
}
