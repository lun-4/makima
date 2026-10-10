use maki_agent::{
    ActorBackend, AgentInput, AgentLimits, AgentManagerHandle, AgentMode, BackendResult,
    ControlWork, DoneReason, History, RunSettings, TurnContext, TurnOutcome, WorkKind,
    actor::EffectiveAgentConfig,
    tools::{ToolContext, ToolRegistry},
};
use maki_commands::CommandInvocation;
use maki_lua::{
    PluginHost,
    orchestration::{OrchestrationServices, TrustedTarget},
    test_support::InMemoryFs,
};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role,
    StreamResponse,
    provider::{BoxFuture, Provider},
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};
use std::{
    future::Future,
    path::PathBuf,
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
const SYNTHETIC_PLAN_PATH: &str = "/test/host-plan.md";

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
}

impl RegisteredOrchestrationTargets {
    fn new(targets: Vec<TrustedTarget>) -> Self {
        Self {
            targets: Mutex::new(targets),
            selected: AtomicUsize::new(0),
        }
    }
}

impl OrchestrationServices for RegisteredOrchestrationTargets {
    fn resolve_target(
        &self,
        _: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        Ok(self
            .targets
            .lock()
            .unwrap()
            .get(self.selected.load(Ordering::SeqCst))
            .cloned())
    }

    fn plan_path_preparer(&self) -> Option<Arc<dyn Fn() -> Result<PathBuf, String> + Send + Sync>> {
        Some(Arc::new(|| Ok(PathBuf::from(SYNTHETIC_PLAN_PATH))))
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

    fn template(&self, target: &maki_agent::AgentRef) -> Result<ToolContext, String> {
        target.actor().map_err(|error| error.to_string())?;
        self.targets
            .lock()
            .unwrap()
            .iter()
            .find(|registered| {
                registered.target.id() == target.id()
                    && registered.target.manager().same_manager(&target.manager())
            })
            .map(|registered| registered.template.clone())
            .ok_or_else(|| "unregistered test target".into())
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
}

impl ActorBackend for LuaInvocationBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
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

struct Fixture {
    _host: PluginHost,
    manager: AgentManagerHandle,
    requests: flume::Receiver<CapturedRequest>,
    completed: flume::Receiver<Result<(), String>>,
    root_ticket: maki_agent::TurnTicket,
}

impl Fixture {
    fn start(tool: &'static str, handler: &str) -> Self {
        Self::start_with_mode(tool, handler, AgentMode::Build)
    }

    fn start_with_mode(tool: &'static str, handler: &str, mode: AgentMode) -> Self {
        let registry = Arc::clone(ToolRegistry::global_arc());
        let (requests_tx, requests) = flume::unbounded();
        let provider = Arc::new(ScriptedProvider {
            requests: requests_tx,
        });
        let prepared_provider: Arc<dyn Provider> = provider.clone();
        let host = PluginHost::with_session_provider_preparer(
            Arc::clone(&registry),
            Default::default(),
            true,
            Arc::new(InMemoryFs::new()),
            None,
            Some(Arc::new(move |model| {
                Ok((model, Arc::clone(&prepared_provider)))
            })),
        )
        .unwrap();
        if tool == "pr5_bundled_schema" {
            host.load_source(
                "pr5_bundled_task",
                &format!(
                    "maki.api.mode.get = function() return 'build' end\n{}",
                    include_str!("../../plugins/task/init.lua")
                ),
            )
            .unwrap();
        }
        host.load_source(
            tool,
            &format!(
                r#"
            if "{tool}" == "pr5_schema_inheritance" then
                maki.api.register_tool({{
                    name = "pr5_spawn_schema_children", description = "spawn children from a schema-enabled parent",
                    schema = {{ type = "object", properties = {{}} }}, audiences = {{ "main", "general_sub" }},
                    handler = function(input, ctx) return pr5_schema_children_handler(input, ctx) end,
                }})
            end
            if "{tool}" == "pr5_tool_events" then
                maki.api.register_command({{ name = "/pr5_callback_diagnostics", tui_only = false,
                    handler = function()
                        error("callback states: " .. maki.json.encode(pr5_callback_states) .. "; errors: " .. table.concat(pr5_callback_errors, "\n"))
                    end,
                }})
            end
            if string.find("{tool}", "pr5_tool_authority", 1, true) then
            maki.api.register_tool({{
                name = "{tool}_probe", description = "probe executing authority",
                schema = {{ type = "object", properties = {{}} }}, audiences = {{ "main", "general_sub" }},
                handler = function(input, ctx) return pr5_child_authority_handler(input, ctx) end,
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
        let mut definition = context.modes.current(&mode);
        if tool.starts_with("pr5_tool_authority") {
            definition.tools = Some(vec![tool.into(), format!("{tool}_probe")]);
            context
                .modes
                .define(maki_agent::ModeDefSpec {
                    name: mode.id().key().into(),
                    tools: definition.tools.clone(),
                    restrict_write_to: context.restrict_write_to(),
                    ..Default::default()
                })
                .unwrap();
        }
        context.mode_def = Some(Arc::new(definition.clone()));
        let mut definitions = vec![json!({
            "name": tool, "description": "exercise public agent handles",
            "input_schema": { "type": "object", "properties": {} },
        })];
        if tool.starts_with("pr5_tool_authority") {
            definitions.push(json!({
                "name": format!("{tool}_probe"), "description": "probe executing authority",
                "input_schema": { "type": "object", "properties": {} },
            }));
        }
        if tool == "pr5_schema_inheritance" {
            definitions.push(json!({ "name": "pr5_spawn_schema_children", "description": "spawn schema children", "input_schema": { "type": "object", "properties": {} } }));
        }
        if tool == "pr5_bundled_schema" {
            definitions.push(json!({ "name": "task", "description": "managed blocking task", "input_schema": { "type": "object" } }));
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
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed) = flume::bounded(1);
        let template = context.clone();
        let root = manager
            .create_root_with_config(Some(policy), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaInvocationBackend {
                    registry,
                    context,
                    tool,
                    completed: completed_tx,
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
            manager,
            requests,
            completed,
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
        self.manager.shutdown(SHUTDOWN_TIMEOUT).await;
    }
}

#[test]
fn public_agent_tickets_retain_exact_results_across_queued_turns() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_ticket_roundtrip",
            &format!(
                r#"
            local child = assert(maki.agent.spawn(ctx, {{
                inherit_provider = true, system = "{SYSTEM}", thinking = "off", silent = true, tools = {{}},
            }}))
            local id = assert(child:id())
            assert(assert(maki.agent.get(ctx, assert(child:ref()))):id() == id)
            local first = assert(child:send(ctx, "first prompt"))
            local busy, busy_err = child:send(ctx, "must not run")
            assert(busy == nil and busy_err ~= nil)
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
            assert(stale == nil and stale_err ~= nil)
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

#[test_case::test_case(false ; "build")]
#[test_case::test_case(true ; "restricted_plan")]
fn executing_child_tool_cannot_control_ancestor_or_sibling(restricted: bool) {
    smol::block_on(async {
        let mode = if restricted {
            AgentMode::Plan("/test/authority-plan.md".into())
        } else {
            AgentMode::Build
        };
        let tool = if restricted {
            "pr5_tool_authority_plan"
        } else {
            "pr5_tool_authority_build"
        };
        let probe = format!("{tool}_probe");
        let fixture = Fixture::start_with_mode(
            tool,
            &format!(
                r#"
            local parent = assert(maki.agent.current(ctx))
            local parent_id = assert(parent:id())
            local sibling = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
            local sibling_ref = assert(sibling:ref())
            pr5_child_authority_handler = function(_, child_ctx)
                    local current = assert(maki.agent.current(child_ctx))
                    assert(current:id() ~= parent_id)
                    if {restricted} then
                        local changed, mode_err = current:set_mode(child_ctx, "build")
                        assert(changed == nil and mode_err ~= nil)
                    end
                    for _, target in ipairs({{ parent, sibling }}) do
                        local status, status_err = target:status(child_ctx)
                        assert(status == nil and status_err ~= nil, "ancestor/sibling status unexpectedly succeeded")
                        local sent, send_err = target:send(child_ctx, "forbidden")
                        assert(sent == nil and send_err ~= nil)
                        local cancelled, cancel_err = target:cancel(child_ctx)
                        assert(cancelled == nil and cancel_err ~= nil, "ancestor/sibling cancel unexpectedly succeeded")
                        local closed, close_err = target:close(child_ctx)
                        assert(closed == nil and close_err ~= nil, "ancestor/sibling close unexpectedly succeeded")
                    end
                    for _, target in ipairs({{ parent, sibling }}) do
                        local changed, mode_err = target:set_mode(child_ctx, "build")
                        assert(changed == nil and mode_err ~= nil)
                        local updated, model_err = target:set_model(child_ctx, "{UPDATED_MODEL}")
                        assert(updated == nil and model_err ~= nil)
                    end
                    local foreign, foreign_err = maki.agent.get(child_ctx, sibling_ref)
                    assert(foreign == nil and foreign_err ~= nil)
                    local root_ref = assert(maki.agent.root(child_ctx))
                    local ancestor, ancestor_err = maki.agent.get(child_ctx, root_ref)
                    assert(ancestor == nil and ancestor_err ~= nil)
                    return "scope enforced"
                end
            local callable = assert(maki.agent.callable_tools(ctx))
            local found = false
            for _, entry in ipairs(callable) do if entry.name == "{probe}" then found = true end end
            assert(found, "parent callable probe missing before child preparation")
            local child = assert(maki.agent.spawn(ctx, {{
                inherit_provider = true, silent = true,
                tools = {{{{ name = "{probe}", description = "check scope", input_schema = {{ type = "object", properties = {{}} }} }}}},
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
            mode,
        );
        let first = fixture.request().await;
        assert!(
            common::tool_names(&first.tools).contains(&probe),
            "child admission filtered probe before dispatch (restricted={restricted}): {:?}",
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
        fixture._host.load_source("pr5_trusted_command", r#"
            local retained
            maki.api.register_command({
                name = "/pr5_capture", description = "retain an invocation-bound context", tui_only = false,
                handler = function(_, ctx)
                    assert(ctx ~= nil)
                    local root = assert(maki.agent.current(ctx))
                    assert(root:id() == assert(maki.agent.root(ctx)):id())
                    assert(root:id() == assert(maki.agent.get(ctx, assert(root:ref()))):id())
                    assert(root:set_mode(ctx, "plan"))
                    retained = { ctx = ctx, root = root }
                end,
            })
            maki.api.register_command({
                name = "/pr5_verify", description = "reject expired authority and use fresh authority", tui_only = false,
                handler = function(_, ctx)
                    local rejected, err = retained.root:status(retained.ctx)
                    assert(rejected == nil and err ~= nil)
                    local root = assert(maki.agent.current(ctx))
                    assert(root:id() == retained.root:id())
                    assert(root:set_mode(ctx, "build"))
                end,
            })
        "#).unwrap();
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
        fixture.manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
                    assert(other:set_mode(ctx, "plan"))
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
        first.manager.shutdown(SHUTDOWN_TIMEOUT).await;
        second.manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
        owner._host.load_source("pr5_foreign_scope", r#"
            local foreign
            local foreign_ref
            maki.api.register_command({
                name = "/pr5_foreign_capture", description = "capture a host-registered second runtime", tui_only = false,
                handler = function(_, ctx)
                    foreign = assert(maki.agent.current(ctx))
                    foreign_ref = assert(foreign:ref())
                    assert(foreign:set_mode(ctx, "plan"))
                end,
            })
            maki.api.register_tool({
                name = "pr5_reject_foreign", description = "reject handles from another manager",
                schema = { type = "object", properties = {} },
                handler = function(_, ctx)
                    local target, lookup_err = maki.agent.get(ctx, foreign_ref)
                    assert(target == nil and lookup_err ~= nil)
                    for _, operation in ipairs({ "status", "cancel", "close" }) do
                        local result, err = foreign[operation](foreign, ctx)
                        assert(result == nil and err ~= nil)
                    end
                    local sent, send_err = foreign:send(ctx, "foreign must not run")
                    assert(sent == nil and send_err ~= nil)
                    local changed, mode_err = foreign:set_mode(ctx, "build")
                    assert(changed == nil and mode_err ~= nil)
                    local updated, model_err = foreign:set_model(ctx, "anthropic/claude-opus-4-20250514")
                    assert(updated == nil and model_err ~= nil)
                    return "foreign runtime rejected"
                end,
            })
        "#).unwrap();
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
        owner.manager.shutdown(SHUTDOWN_TIMEOUT).await;
        other.manager.shutdown(SHUTDOWN_TIMEOUT).await;
    });
}

#[test]
fn build_trusted_context_cannot_expand_registered_restricted_target_policy() {
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
        owner._host.load_source("pr5_destination_policy", r#"
            local target
            maki.api.register_command({
                name = "/pr5_destination_capture", tui_only = false,
                handler = function(_, ctx) target = assert(maki.agent.current(ctx)) end,
            })
            maki.api.register_command({
                name = "/pr5_destination_attack", tui_only = false,
                handler = function(_, ctx)
                    assert(assert(maki.agent.current(ctx)):id() ~= target:id())
                    local expanded, mode_err = target:set_mode(ctx, "build")
                    assert(expanded == nil and mode_err ~= nil)
                    local changed, model_err = target:set_model(ctx, "anthropic/claude-opus-4-20250514")
                    assert(changed == nil and model_err ~= nil)
                    assert(target:set_model(ctx, { thinking = "off" }))
                end,
            })
        "#).unwrap();
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
        assert!(matches!(config.mode, AgentMode::Plan(_)));
        assert_eq!(config.model.spec(), common::default_model().spec());
        owner.manager.shutdown(SHUTDOWN_TIMEOUT).await;
        restricted.manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
        let (tx, rx) = flume::unbounded();
        let (template, _events, _cancel) =
            common::ctx_with_provider(Arc::new(ScriptedProvider { requests: tx }));
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
        let terminal: CapturedRequest = bounded(rx.recv_async(), "integration channel receive")
            .await
            .unwrap();
        terminal
            .reply
            .send(common::canned_reply("terminal reply"))
            .unwrap();
        let active = bounded(rx.recv_async(), "integration channel receive")
            .await
            .unwrap();
        assert_eq!(begin.recv_timeout(TEST_TIMEOUT).unwrap(), Ok(()));
        let inspect = handle.run_command_for_test(
            Arc::from("pr5_retained_ticket"),
            Arc::from("/pr5_retained_inspect"),
            String::new(),
            0,
        );
        let checkpoint = bounded(rx.recv_async(), "integration channel receive")
            .await
            .unwrap();
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
        fixture.manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
        let (requests_tx, requests) = flume::unbounded();
        let (template, _events, _cancel) = common::ctx_with_provider(Arc::new(ScriptedProvider {
            requests: requests_tx,
        }));
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
        manager.shutdown(SHUTDOWN_TIMEOUT).await;
    });
}

#[test]
fn tool_origin_subscriptions_deliver_config_and_idle_with_fresh_contexts() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_tool_events",
            r#"
            pr5_callback_errors = {}
            pr5_callback_states = {}
            local function guard(kind, handler)
                return function(event, callback_ctx)
                    pr5_callback_states[kind] = "entered"
                    local ok, err = pcall(handler, event, callback_ctx)
                    pr5_callback_states[kind] = ok and "completed" or "failed"
                    if not ok then pr5_callback_errors[#pr5_callback_errors + 1] = kind .. ": " .. tostring(err) end
                end
            end
            local child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
            local subscriptions = {}
            subscriptions[1] = assert(child:on_config_change(ctx, guard("config", function(event, callback_ctx)
                assert(event.selection.thinking.kind == "effort" and event.selection.thinking.level == "low", "config event thinking has unexpected shape")
                local probe = assert(maki.agent.spawn(callback_ctx, { inherit_provider = true, silent = true, tools = {} }))
                assert(callback_ctx:defer_close(probe))
                assert(assert(probe:send(callback_ctx, "config callback")):wait(callback_ctx))
            end)))
            subscriptions[2] = assert(child:on_idle(ctx, guard("idle", function(event, callback_ctx)
                assert(event.agent_id == child:id())
                local probe = assert(maki.agent.spawn(callback_ctx, { inherit_provider = true, silent = true, tools = {} }))
                assert(callback_ctx:defer_close(probe))
                assert(assert(probe:send(callback_ctx, "idle callback")):wait(callback_ctx))
            end)))
            assert(child:set_model(ctx, { thinking = "low" }))
            assert(assert(assert(child:send(ctx, "tool-origin turn")):wait(ctx)).text == "completed")
            local checkpoint = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
            assert(assert(checkpoint:send(ctx, "callbacks ready")):wait(ctx))
            for _, subscription in ipairs(subscriptions) do assert(subscription:close()) end
            assert(checkpoint:close(ctx))
            assert(child:close(ctx))
        "#,
        );
        let receive = || {
            futures_lite::future::race(
                async { fixture.requests.recv_async().await.unwrap() },
                async {
                    smol::Timer::after(TEST_TIMEOUT).await;
                    let diagnostics = fixture
                        ._host
                        .event_handle()
                        .run_command_for_test(
                            Arc::from("pr5_tool_events"),
                            Arc::from("/pr5_callback_diagnostics"),
                            String::new(),
                            0,
                        )
                        .recv_timeout(SHUTDOWN_TIMEOUT);
                    panic!(
                        "tool-origin callback request never arrived; callback diagnostics: {diagnostics:?}"
                    )
                },
            )
        };
        let mut gates = Vec::new();
        let mut checkpoint = None;
        let mut turn_replied = false;
        while gates.len() < 2 || checkpoint.is_none() || !turn_replied {
            let request: CapturedRequest = receive().await;
            let history = serde_json::to_string(&request.messages).unwrap();
            if history.contains("tool-origin turn") {
                assert!(!turn_replied, "duplicate observed turn: {history}");
                request
                    .reply
                    .send(common::canned_reply("completed"))
                    .unwrap();
                turn_replied = true;
            } else if history.contains("callbacks ready") {
                assert!(checkpoint.is_none(), "duplicate checkpoint: {history}");
                checkpoint = Some(request);
            } else {
                assert!(
                    history.contains("config callback") || history.contains("idle callback"),
                    "unexpected callback history: {history}"
                );
                gates.push(request);
            }
        }
        checkpoint
            .unwrap()
            .reply
            .send(common::canned_reply("ready"))
            .unwrap();
        fixture.finish().await;
        for gate in gates {
            wait_for_gate_drop(&gate, "tool-origin subscription cancellation").await;
        }
    });
}

#[test]
fn inherited_tools_do_not_leak_parent_structured_output_into_children() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_schema_inheritance",
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
    host.load_source("pr5_unmanaged_origin", r#"
        local retained
        maki.api.register_tool({
            name = "pr5_origin_capture", description = "retain an unmanaged invocation context",
            schema = { type = "object", properties = {} },
            handler = function(_, ctx) retained = ctx; return "captured" end,
        })
        maki.api.register_tool({
            name = "pr5_origin_check", description = "reject a context from another invocation",
            schema = { type = "object", properties = {} },
            handler = function(_, ctx)
                local stale, stale_err = maki.agent.spawn(retained, { inherit_provider = true, tools = {} })
                assert(stale == nil and stale_err ~= nil)
                local fresh = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
                local ticket = assert(fresh:send(ctx, "fresh unmanaged context"))
                assert(assert(ticket:wait(ctx)).text == "fresh reply")
                assert(fresh:close(ctx))
                return "origin rejected"
            end,
        })
    "#).unwrap();
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
fn repeated_deferred_subtrees_close_and_release_provider_gates() {
    smol::block_on(async {
        let fixture = Fixture::start("pr5_resource_root", "");
        assert_eq!(
            bounded(fixture.completed.recv_async(), "resource root completion")
                .await
                .unwrap(),
            Ok(())
        );
        let (tx, rx) = flume::unbounded();
        let (template, _events, _cancel) =
            common::ctx_with_provider(Arc::new(ScriptedProvider { requests: tx }));
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
            let a: CapturedRequest = bounded(rx.recv_async(), "cycle first provider request")
                .await
                .unwrap();
            let b = bounded(rx.recv_async(), "cycle descendant provider request")
                .await
                .unwrap();
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
            bounded(
                async {
                    loop {
                        if fixture
                            .manager
                            .snapshot()
                            .iter()
                            .filter(|node| node.parent_id.is_some())
                            .all(|node| {
                                matches!(
                                    node.graph_lifecycle,
                                    maki_agent::GraphLifecycle::Closed
                                        | maki_agent::GraphLifecycle::Removed
                                )
                            })
                        {
                            break;
                        }
                        smol::future::yield_now().await;
                    }
                },
                "deferred subtree graph drain",
            )
            .await;
        }
        fixture._host.unload("pr5_resource_cycles").unwrap();
        fixture.manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
fn bundled_managed_blocking_task_returns_schema_object_without_wrapper() {
    smol::block_on(async {
        let fixture = Fixture::start(
            "pr5_bundled_schema",
            r#"
            local text, err = maki.agent.call_tool(ctx, "task", {
                description = "structured task", prompt = "report answer", subagent_type = "general",
                output_schema = { type = "object", properties = { answer = { type = "integer" } }, required = { "answer" } },
            })
            assert(text ~= nil and err == nil, err)
            local value = maki.json.decode(text)
            assert(value.answer == 42)
            assert(value[1] == nil and value.text == nil and value.output == nil)
        "#,
        );
        let request = fixture.request().await;
        assert!(common::tool_names(&request.tools).contains(&"structured_output".to_owned()));
        request
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
            .send(common::canned_reply("done"))
            .unwrap();
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
        fixture.manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
            assert(invalid == nil and schema_err ~= nil)
            local collision, collision_err = maki.agent.spawn(ctx, {{ inherit_provider = true,
                tools = {{{{ name = "structured_output", description = "conflict", input_schema = {{ type = "object" }} }}}},
                output_schema = {{ type = "object", properties = {{}} }},
            }})
            assert(collision == nil and collision_err ~= nil)
            local local_collision, local_err = maki.agent.spawn(ctx, {{ inherit_provider = true, tools = {{}},
                output_schema = {{ type = "object", properties = {{}} }},
                local_tools = {{ structured_output = {{ description = "conflict", input_schema = {{ type = "object" }}, handler = function() return "never" end }} }},
            }})
            assert(local_collision == nil and local_err ~= nil)
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
