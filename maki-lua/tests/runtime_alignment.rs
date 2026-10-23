use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use maki_agent::{
    ActorBackend, AgentContextSnapshot, AgentInput, AgentManagerHandle, AgentMode, BackendResult,
    ConfigChange, ConfigPatch, ControlWork, DoneReason, History, PreparedModel, RunSettings,
    ToolOutput, TurnContext, TurnOutcome, WorkKind,
    actor::{ActorEvent, DeliveryStatus, EffectiveAgentConfig, graph_quiescence_barrier},
    cancel::CancelTrigger,
    tools::{ToolContext, ToolRegistry, TurnToolBindings},
};
use maki_commands::CommandInvocation;
use maki_lua::{
    PluginHost,
    orchestration::{OrchestrationServices, TrustedTarget},
    test_support::run_command,
};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role,
    StreamResponse, TokenUsage,
    provider::{BoxFuture, Provider},
};
use maki_storage::id::{MakiId, SessionRef};
use serde_json::{Value, json};

mod common;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const INITIAL: &str = "alignment initial";
const STEERING: &str = "alignment steering";
const USER_INTERRUPT_PREFIX: &str = "<user-interrupt>\n";
const UNKNOWN_TASK_ERR: &str = "unknown task_id";
const REPLY: &str = "alignment complete";
const PROVIDER_FAILURE: &str = "alignment provider failure";
const UPDATED_MODEL: &str = "anthropic/claude-opus-4-20250514";
const INSTRUCTION_BUDGET_EXHAUSTED: &str =
    "The request budget was exhausted before all accepted instructions reached the provider.";
const REQUEST_USAGE: TokenUsage = TokenUsage {
    input: 11,
    output: 7,
    cache_creation: 3,
    cache_read: 5,
};
const PROMPTS: [&str; 3] = ["alignment A", "alignment B", "alignment C"];
const TASK_SPAWN: &str = "task_spawn";
const TASK_SEND: &str = "task_send";
const TASK_WAIT: &str = "task_wait";
const TASK_DESPAWN: &str = "task_despawn";
const ECHO_TOOL: &str = "alignment_echo";
const ECHO_REPLY: &str = "alignment tool complete";
const CONTEXT_TOOL: &str = "alignment_context";
const CHILD_CONTEXT: &str = "alignment inherited cwd";
const AUTHORITY_TOOL: &str = "alignment_authority";
const TRUSTED_MESSAGE: &str = "alignment trusted sender";
const DELEGATED_MESSAGE: &str = "alignment delegated sender";
const PRELUDE: &str = r#"
local handlers = {}
local register_tool = maki.api.register_tool
maki.api.register_tool = function(spec)
    if spec.name == "task_wait" then
        local handler = spec.handler
        spec.handler = function(input, ctx)
            alignment_wait_entered = true
            return handler(input, ctx)
        end
    end
    handlers[spec.name] = spec.handler
    return register_tool(spec)
end
maki.api.register_command({
    name = "/alignment", tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local call = maki.json.decode(opts.args)
        local output = handlers[call.name](call.input, ctx)
        assert(not output.is_error, output.llm_output)
        alignment_result = output.llm_output
    end,
})
maki.api.register_command({
    name = "/alignment_submit", tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local input = maki.json.decode(opts.args)
        alignment_ancestor = assert(ctx:agent())
        local target = assert(maki.agent.get(ctx, assert(maki.agent.root(ctx))))
        for _, reference in ipairs(assert(maki.agent.list(ctx))) do
            if reference:id() == input.agent_id then target = assert(maki.agent.get(ctx, reference)) end
        end
        assert(target:id() == input.agent_id)
        alignment_trusted = assert(target:send(ctx, input.message, { when = "join" }))
    end,
})
maki.api.register_tool({
    name = "alignment_authority", description = "check mixed sender authority",
    schema = { type = "object", properties = {} },
    audiences = { "main", "general_sub" },
    handler = function(_, ctx)
        local current = assert(ctx:agent())
        local changed, err = alignment_ancestor:set_mode(ctx, "plan")
        assert(changed == nil and err ~= nil)
        local closed, close_err = alignment_ancestor:close(ctx)
        assert(closed == nil and close_err ~= nil)
        return maki.json.encode({ agent_id = current:id(), mode = assert(ctx:mode()), denied = err })
    end,
})
maki.api.register_tool({
    name = "alignment_context", description = "read admitted cwd and inherit it",
    schema = { type = "object", properties = { spawn_child = { type = "boolean" } } },
    audiences = { "main", "general_sub" },
    handler = function(input, ctx)
        local cwd = assert(ctx:resolve_path("."))
        local job = maki.fn.jobstart({ "sh", "-c", "pwd; printf '%s\\n' \"$PWD\"" }, { cwd = cwd })
        local process = assert(maki.fn.jobwait(job))
        assert(process.exit_code == 0)
        local output = {}
        for line in process.stdout:gmatch("[^\n]+") do output[#output + 1] = line end
        local child_reply
        if input.spawn_child then
            local child = assert(maki.agent.spawn(ctx, { inherit_provider = true, silent = true, tools = {} }))
            child_reply = assert(assert(child:send(ctx, "alignment inherited cwd", { when = "idle" })):wait(ctx)).text
            assert(child:close(ctx))
        end
        return maki.json.encode({ cwd = cwd, output = output, child = child_reply })
    end,
})
maki.api.register_tool({
    name = "alignment_wait_entered", description = "read task wait checkpoint",
    schema = { type = "object", properties = {} },
    handler = function() return maki.json.encode(alignment_wait_entered == true) end,
})
maki.api.register_tool({
    name = "alignment_result", description = "read command result",
    schema = { type = "object", properties = {} },
    handler = function() return alignment_result end,
})
maki.api.register_tool({
    name = "alignment_echo", description = "complete an alignment tool group",
    schema = { type = "object", properties = {} },
    audiences = { "main", "general_sub" },
    handler = function() return "alignment tool complete" end,
})
local real_spawn = maki.agent.spawn
maki.agent.spawn = function(ctx, opts)
    opts.inherit_provider = true
    return real_spawn(ctx, opts)
end
maki.agent.resolve_model = function()
    return { spec = "anthropic/claude-sonnet-4-20250514" }
end
maki.agent.system_prompt = function() return "alignment system" end
maki.agent.tools = function()
    return {
        { name = "task_spawn", description = "spawn", input_schema = { type = "object" } },
        { name = "task_wait", description = "wait", input_schema = { type = "object" } },
        { name = "alignment_echo", description = "echo", input_schema = { type = "object" } },
        { name = "alignment_authority", description = "authority", input_schema = { type = "object" } },
        { name = "alignment_context", description = "cwd", input_schema = { type = "object" } },
    }
end
"#;

struct Request {
    system: String,
    model: String,
    messages: Vec<Message>,
    reply: flume::Sender<Result<StreamResponse, AgentError>>,
}

impl Request {
    fn respond(self, mut response: StreamResponse) {
        for block in &mut response.message.content {
            if let ContentBlock::ToolUse { id, .. } = block {
                *id = MakiId::generate().to_string();
            }
        }
        self.reply.send(Ok(response)).unwrap();
    }

    fn contains(&self, text: &str) -> bool {
        self.messages.iter().any(|message| {
            message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text: value } if value.contains(text)),
            )
        })
    }

    fn last_tool_result(&self) -> Value {
        self.messages
            .iter()
            .rev()
            .flat_map(|message| &message.content)
            .find_map(|block| match block {
                ContentBlock::ToolResult { content, .. } => Some(
                    serde_json::from_str(content)
                        .unwrap_or_else(|error| panic!("invalid tool result {content:?}: {error}")),
                ),
                _ => None,
            })
            .expect("provider request must contain the completed tool result")
    }
}

struct GatedProvider {
    requests: flume::Sender<Request>,
}

impl Provider for GatedProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let (reply, response) = flume::bounded(1);
            self.requests
                .send(Request {
                    system: system.into(),
                    model: model.spec(),
                    messages: messages.to_vec(),
                    reply,
                })
                .unwrap();
            common::bounded(response.recv_async()).await.unwrap()
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

struct Targets(Mutex<Vec<TrustedTarget>>);

impl OrchestrationServices for Targets {
    fn resolve_target(
        &self,
        _: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        Ok(self.0.lock().unwrap().first().cloned())
    }

    fn register_child_target(&self, target: TrustedTarget, _: String) -> Result<(), String> {
        self.0.lock().unwrap().push(target);
        Ok(())
    }

    fn template(&self, target: &maki_agent::AgentRef) -> Result<ToolContext, String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.target.id() == target.id()
                    && entry.target.manager().same_manager(&target.manager())
            })
            .map(|entry| entry.template.clone())
            .ok_or_else(|| "unregistered alignment target".into())
    }
}

struct Fixture {
    _host: PluginHost,
    _cancel: CancelTrigger,
    _release: flume::Sender<()>,
    manager: AgentManagerHandle,
    registry: Arc<ToolRegistry>,
    context: ToolContext,
    requests: flume::Receiver<Request>,
    pending_requests: Mutex<Vec<Request>>,
}

impl Fixture {
    fn new() -> Self {
        Self::with_max_turns(None)
    }

    fn with_max_turns(max_turns: Option<u32>) -> Self {
        Self::with_context(max_turns, None)
    }

    fn with_context(max_turns: Option<u32>, cwd: Option<&Path>) -> Self {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source_with_opts(
            "runtime_alignment",
            &format!("{PRELUDE}\n{}", include_str!("../../plugins/task/init.lua")),
            serde_json::Map::new(),
        )
        .unwrap();
        let (requests_tx, requests) = flume::unbounded();
        let (mut context, _events, cancel) = common::ctx_with_provider(Arc::new(GatedProvider {
            requests: requests_tx,
        }));
        context.tool_use_id = None;
        context.config.max_turns = max_turns;
        if let Some(cwd) = cwd {
            context.cwd = cwd.to_path_buf();
            context
                .modes
                .define(maki_agent::ModeDefSpec {
                    name: "build".into(),
                    system_prompt: Some("alignment cwd {cwd}".into()),
                    ..Default::default()
                })
                .unwrap();
            context.mode_def = Some(Arc::new(context.modes.current(&context.mode)));
        }
        context.registry = Arc::clone(&registry);
        context.turn_bindings = Arc::new(TurnToolBindings::capture(
            &registry,
            &context.local_tools,
            None,
        ));
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let (spawned, ready) = flume::unbounded();
        let (release, release_rx) = flume::unbounded();
        let held_context = Arc::new(Mutex::new(None));
        let root = manager
            .create_root_with_config(Some(policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(ManagedDriver {
                    context: context.clone(),
                    registry: Arc::clone(&registry),
                    spawned,
                    release: release_rx,
                    started: true,
                    held_context: Some(Arc::clone(&held_context)),
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        host.event_handle()
            .install_orchestration_services(Arc::new(Targets(Mutex::new(vec![TrustedTarget {
                target: root.clone(),
                template: context.clone(),
            }]))));
        host.wait_for_worker_barrier_for_test();
        root.actor()
            .unwrap()
            .admit_turn(ordinary_input(INITIAL), None, INITIAL.into())
            .unwrap();
        ready.recv_timeout(SHUTDOWN_TIMEOUT).unwrap();
        context = held_context.lock().unwrap().take().unwrap();
        Self {
            _host: host,
            _cancel: cancel,
            _release: release,
            manager,
            registry,
            context,
            requests,
            pending_requests: Mutex::new(Vec::new()),
        }
    }

    fn tool(&self, name: &str, input: Value) -> Value {
        common::exec_tool(&self.registry, &self.context, name, input).unwrap()
    }

    fn spawn(&self, prompt: &str) -> Value {
        self.tool(TASK_SPAWN, spawn_input(prompt))
    }

    async fn request(&self) -> Request {
        common::bounded(self.requests.recv_async()).await.unwrap()
    }

    async fn request_for(&self, prompt: &str) -> Request {
        {
            let mut pending = self.pending_requests.lock().unwrap();
            if let Some(index) = pending.iter().position(|request| request.contains(prompt)) {
                return pending.remove(index);
            }
        }
        loop {
            let request = self.request().await;
            if request.contains(prompt) {
                return request;
            }
            self.pending_requests.lock().unwrap().push(request);
        }
    }

    fn trusted_submit(&self, actor: &maki_agent::AgentActorHandle, message: &str) {
        let args = json!({ "agent_id": actor.agent_id(), "message": message });
        let result = run_command(
            &self._host.command_registry(),
            &format!("/alignment_submit {args}"),
            0,
            None,
        );
        assert_eq!(result.recv_timeout(SHUTDOWN_TIMEOUT).unwrap(), Ok(()));
    }

    fn close(&self, task: &Value) {
        assert_eq!(
            self.tool(TASK_DESPAWN, json!({ "task_id": task["task_id"] }))["ok"],
            true
        );
    }
}

fn policy(context: &ToolContext) -> EffectiveAgentConfig {
    EffectiveAgentConfig::new(
        RunSettings {
            provider: Arc::clone(&context.provider),
            model: (*context.model).clone(),
            fast: false,
            workflow: false,
            thinking: Default::default(),
        },
        context.mode.clone(),
    )
    .with_mode_def(Some(context.modes.current(&context.mode)))
    .with_context(AgentContextSnapshot::new(context.cwd.clone()))
}

fn spawn_input(prompt: &str) -> Value {
    json!({ "description": "alignment", "prompt": prompt, "subagent_type": "general" })
}

fn wait_input(task: &Value) -> Value {
    json!({ "task_id": task["task_id"], "turn_id": task["turn_id"] })
}

fn decode_output(output: ToolOutput) -> Value {
    let (ToolOutput::Plain(output) | ToolOutput::Markdown(output)) = output else {
        panic!("unexpected task output")
    };
    serde_json::from_str(&output.text).unwrap()
}

#[test]
fn runtime_gate_controls_real_transition() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        let request = fixture.request().await;
        assert!(request.contains(INITIAL));
        assert!(!request.contains(STEERING));
        let input = wait_input(&task);
        let invocation = fixture
            .registry
            .get(TASK_WAIT)
            .unwrap()
            .tool
            .parse(&input)
            .unwrap();
        let context = common::with_request_tools(&fixture.registry, &fixture.context);
        let wait = invocation.execute(&context);
        futures_lite::pin!(wait);
        assert!(futures_lite::future::poll_once(&mut wait).await.is_none());

        let admitted = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": STEERING }),
        );
        assert_eq!(admitted["turn_id"], task["turn_id"]);
        assert!(fixture.requests.is_empty());
        request.respond(common::canned_reply("before steering"));

        let request = fixture.request().await;
        assert!(request.contains(INITIAL));
        assert!(request.contains(STEERING));
        assert!(futures_lite::future::poll_once(&mut wait).await.is_none());
        request.respond(common::canned_reply(REPLY));
        let result = decode_output(common::bounded(wait).await.output.unwrap());
        assert_eq!(result["status"], "done");
        assert_eq!(result["result"]["turn_id"], task["turn_id"]);
        assert_eq!(result["result"]["text"], REPLY);

        let mut successor = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": INITIAL }),
        );
        successor["task_id"] = task["task_id"].clone();
        assert_ne!(successor["turn_id"], task["turn_id"]);
        fixture.request().await.respond(common::canned_reply(REPLY));
        assert_eq!(
            fixture.tool(TASK_WAIT, wait_input(&successor))["status"],
            "done"
        );
        fixture.close(&task);
    });
}

#[test_case::test_case(false; "delegated_starts")]
#[test_case::test_case(true; "trusted_starts")]
fn lua_mixed_sender_submission_preserves_authority(trusted_starts: bool) {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        fixture.request().await.respond(common::canned_reply(REPLY));
        fixture.tool(TASK_WAIT, wait_input(&task));
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        if trusted_starts {
            fixture.trusted_submit(&actor, TRUSTED_MESSAGE);
        } else {
            fixture.tool(
                TASK_SEND,
                json!({ "task_id": task["task_id"], "message": DELEGATED_MESSAGE }),
            );
        }
        let request = fixture.request().await;
        let ticket = actor.latest_ticket().unwrap();
        let provenance = ticket.provenance().clone();
        assert_eq!(
            provenance.source_agent,
            (!trusted_starts).then_some(root.id())
        );
        if trusted_starts {
            let sent = fixture.tool(
                TASK_SEND,
                json!({ "task_id": task["task_id"], "message": DELEGATED_MESSAGE }),
            );
            assert_eq!(sent["turn_id"], json!(ticket.turn_id()));
        } else {
            fixture.trusted_submit(&actor, TRUSTED_MESSAGE);
        }
        assert_eq!(actor.latest_ticket().unwrap().turn_id(), ticket.turn_id());
        assert_eq!(actor.latest_ticket().unwrap().provenance(), &provenance);
        request.respond(common::canned_tool_use(AUTHORITY_TOOL, json!({})));
        let request = fixture.request().await;
        assert!(request.contains(TRUSTED_MESSAGE));
        assert!(request.contains(DELEGATED_MESSAGE));
        let authority = request.last_tool_result();
        assert_eq!(authority["agent_id"], json!(actor.agent_id()));
        assert_eq!(authority["mode"], "build");
        assert!(
            authority["denied"]
                .as_str()
                .unwrap()
                .contains("not a descendant")
        );
        assert_eq!(
            root.actor().unwrap().effective_config().unwrap().mode,
            AgentMode::Build
        );
        request.respond(common::canned_reply(REPLY));
        let result = common::bounded(ticket.wait_result()).await;
        assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
        assert_eq!(result.provenance, provenance);
        assert!(root.actor().is_ok());
        fixture.close(&task);
    });
}

#[test_case::test_case(false; "prompt_and_tool_environment")]
#[test_case::test_case(true; "new_child_inheritance")]
fn lua_cwd_admission_order_is_target_local(spawn_child: bool) {
    const QUEUED: &str = "alignment queued old cwd";
    const AFTER: &str = "alignment queued new cwd";
    smol::block_on(async {
        let old = tempfile::tempdir().unwrap();
        let new = tempfile::tempdir().unwrap();
        let fixture = Fixture::with_context(None, Some(old.path()));
        let task = fixture.spawn(INITIAL);
        let first = fixture.request().await;
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        let queued = actor
            .admit_turn(ordinary_input(QUEUED), None, QUEUED.into())
            .unwrap();
        let change = smol::spawn(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::WorkingDirectory(
                    new.path().to_path_buf(),
                ))),
        );
        let after = actor
            .submit(
                ordinary_input(AFTER),
                None,
                AFTER.into(),
                Default::default(),
            )
            .unwrap();
        assert_ne!(queued.turn_id(), after.turn_id());
        let mut request = first;
        for (prompt, cwd) in [
            (INITIAL, old.path()),
            (QUEUED, old.path()),
            (AFTER, new.path()),
        ] {
            assert!(request.contains(prompt));
            assert!(
                request.system.contains(cwd.to_str().unwrap()),
                "{}",
                request.system
            );
            request.respond(common::canned_tool_use(
                CONTEXT_TOOL,
                json!({ "spawn_child": spawn_child }),
            ));
            if spawn_child {
                let child = fixture.request().await;
                assert!(
                    child.contains(CHILD_CONTEXT),
                    "expected nested child request, got {:?}",
                    child.messages
                );
                assert!(
                    child.system.contains(cwd.to_str().unwrap()),
                    "child spawned during {prompt} must inherit {}, got {}",
                    cwd.display(),
                    child.system
                );
                child.respond(common::canned_reply(REPLY));
            }
            let followup = fixture.request_for(prompt).await;
            let context = followup.last_tool_result();
            assert_eq!(PathBuf::from(context["cwd"].as_str().unwrap()), cwd);
            assert_eq!(context["output"], json!([cwd, cwd]));
            if spawn_child {
                assert_eq!(context["child"], REPLY);
            }
            followup.respond(common::canned_reply(REPLY));
            if prompt != AFTER {
                request = fixture.request().await;
            } else {
                break;
            }
        }
        common::bounded(change).await.unwrap();
        for ticket in [queued, after] {
            assert!(matches!(
                common::bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
        }
        assert_eq!(actor.effective_config().unwrap().context.cwd(), new.path());
        assert_eq!(
            root.actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .context
                .cwd(),
            old.path()
        );
        assert!(fixture.requests.is_empty());
        fixture.close(&task);
    });
}

#[test]
fn lua_local_cancel_releases_task_wait_and_preserves_child_config_and_input() {
    const BEFORE_CUT: &str = "alignment discarded before cancel";
    const AFTER_CUT: &str = "alignment preserved after cancel";
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(PROMPTS[0]);
        fixture
            .request_for(PROMPTS[0])
            .await
            .respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[1])));
        let request = fixture.request_for(PROMPTS[0]).await;
        let child = request.last_tool_result();
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        let child_actor = ActorProbe::child(&fixture.manager, actor.agent_id()).actor;
        let original = actor.latest_ticket().unwrap();
        let child_ticket = child_actor.latest_ticket().unwrap();
        request.respond(common::canned_tool_use(TASK_WAIT, wait_input(&child)));
        let child_request = fixture.request_for(PROMPTS[1]).await;
        common::bounded(async {
            while fixture.tool("alignment_wait_entered", json!({})) != json!(true) {
                smol::future::yield_now().await;
            }
        })
        .await;
        let joined = actor
            .submit(
                ordinary_input(BEFORE_CUT),
                None,
                BEFORE_CUT.into(),
                Default::default(),
            )
            .unwrap();
        assert_eq!(joined.turn_id(), original.turn_id());
        let change = smol::spawn(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        provider: Arc::clone(&fixture.context.provider),
                        model: Model::from_spec(UPDATED_MODEL).unwrap(),
                    }),
                    ..Default::default()
                }))),
        );
        actor.cancel_local(0);
        let after = actor
            .submit(
                ordinary_input(AFTER_CUT),
                None,
                AFTER_CUT.into(),
                Default::default(),
            )
            .unwrap();
        assert_ne!(after.turn_id(), original.turn_id());
        let cancelled = common::bounded(original.wait_result()).await;
        assert!(matches!(cancelled.outcome, TurnOutcome::Cancelled { .. }));
        assert!(child_ticket.peek_result().is_none());
        assert!(!child_request.reply.is_disconnected());
        assert!(child_actor.snapshot().active_turn.is_some());
        common::bounded(change).await.unwrap();
        let resumed = fixture.request_for(PROMPTS[0]).await;
        assert_eq!(resumed.model, UPDATED_MODEL);
        assert!(resumed.contains(AFTER_CUT));
        assert!(!resumed.contains(BEFORE_CUT));
        child_request.respond(common::canned_reply(REPLY));
        assert!(matches!(
            common::bounded(child_ticket.wait()).await,
            TurnOutcome::Completed { .. }
        ));
        resumed.respond(common::canned_reply(REPLY));
        let reported = fixture.request_for(PROMPTS[0]).await;
        assert_eq!(reported.model, UPDATED_MODEL);
        assert!(reported.messages.iter().any(Message::is_observation));
        assert!(reported.contains(AFTER_CUT));
        reported.respond(common::canned_reply(REPLY));
        assert!(matches!(
            common::bounded(after.wait()).await,
            TurnOutcome::Completed { .. }
        ));
        assert_eq!(
            common::bounded(original.wait_result()).await.outcome,
            cancelled.outcome
        );
        assert_eq!(
            actor.effective_config().unwrap().model.spec(),
            UPDATED_MODEL
        );
        assert_eq!(
            child_actor.effective_config().unwrap().model.spec(),
            common::default_model().spec()
        );
        let drained = common::bounded(graph_quiescence_barrier(&[child_actor, actor]))
            .await
            .unwrap();
        assert!(drained.iter().all(|probe| probe.is_quiescent()));
        assert!(fixture.requests.is_empty());
        fixture.close(&task);
    });
}

#[test]
fn lua_submission_preserves_exact_and_configuration_boundaries() {
    const EXACT: &str = "alignment exact boundary";
    const BEFORE_CHANGE: &str = "alignment before configuration";
    const AFTER_CHANGE: &str = "alignment after configuration";
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        let first_request = fixture.request().await;
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        let first = actor.latest_ticket().unwrap();
        let exact = actor
            .admit_turn(ordinary_input(EXACT), None, EXACT.into())
            .unwrap();
        let before = actor
            .submit(
                ordinary_input(BEFORE_CHANGE),
                None,
                BEFORE_CHANGE.into(),
                Default::default(),
            )
            .unwrap();
        assert_ne!(before.turn_id(), first.turn_id());
        assert_ne!(before.turn_id(), exact.turn_id());
        let change = smol::spawn(
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Patch(ConfigPatch {
                    model: Some(PreparedModel {
                        provider: Arc::clone(&fixture.context.provider),
                        model: Model::from_spec(UPDATED_MODEL).unwrap(),
                    }),
                    ..Default::default()
                }))),
        );
        let after = actor
            .submit(
                ordinary_input(AFTER_CHANGE),
                None,
                AFTER_CHANGE.into(),
                Default::default(),
            )
            .unwrap();
        assert_ne!(after.turn_id(), before.turn_id());
        assert_eq!(first_request.model, common::default_model().spec());
        assert!(!first_request.contains(EXACT));
        assert!(!first_request.contains(BEFORE_CHANGE));
        assert!(!first_request.contains(AFTER_CHANGE));
        first_request.respond(common::canned_reply(REPLY));
        for prompt in [EXACT, BEFORE_CHANGE, AFTER_CHANGE] {
            let request = fixture.request().await;
            assert_eq!(
                request.messages.last().unwrap().first_text_content(),
                Some(prompt)
            );
            assert_eq!(
                request.model,
                if prompt == AFTER_CHANGE {
                    UPDATED_MODEL.to_owned()
                } else {
                    common::default_model().spec()
                }
            );
            if prompt != AFTER_CHANGE {
                assert!(!request.contains(AFTER_CHANGE));
            }
            if prompt == EXACT {
                assert!(!request.contains(BEFORE_CHANGE));
            }
            request.respond(common::canned_reply(REPLY));
        }
        common::bounded(change).await.unwrap();
        for ticket in [first, exact, before, after] {
            assert!(matches!(
                common::bounded(ticket.wait()).await,
                TurnOutcome::Completed { .. }
            ));
        }
        assert_eq!(
            actor.effective_config().unwrap().model.spec(),
            UPDATED_MODEL
        );
        assert_eq!(
            root.actor()
                .unwrap()
                .effective_config()
                .unwrap()
                .model
                .spec(),
            common::default_model().spec()
        );
        let drained = common::bounded(graph_quiescence_barrier(&[actor]))
            .await
            .unwrap();
        assert!(drained.iter().all(|probe| probe.is_quiescent()));
        assert!(fixture.requests.is_empty());
        fixture.close(&task);
    });
}

#[test]
fn lua_shared_turn_preserves_tool_group_wrapper_and_usage_once() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        fixture.request().await.respond(common::canned_reply(REPLY));
        let original = fixture.tool(TASK_WAIT, wait_input(&task));
        let root = fixture.manager.root().unwrap();
        let mut probe = ActorProbe::child(&fixture.manager, root.id());
        let mut successor = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": INITIAL }),
        );
        successor["task_id"] = task["task_id"].clone();
        assert_ne!(successor["turn_id"], task["turn_id"]);
        let request = fixture.request().await;
        let initial = request.messages.last().unwrap();
        assert!(matches!(initial.role, Role::User));
        assert_eq!(initial.first_text_content(), Some(INITIAL));
        assert_eq!(initial.display_text, None);
        let joined = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": STEERING }),
        );
        assert_eq!(joined["turn_id"], successor["turn_id"]);
        let mut response = common::canned_tool_use(ECHO_TOOL, json!({}));
        response.usage = REQUEST_USAGE;
        request.respond(response);

        let request = fixture.request().await;
        let tail = &request.messages[request.messages.len() - 3..];
        let tool_id = tail[0]
            .content
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolUse { id, name, .. } if name == ECHO_TOOL => Some(id),
                _ => None,
            })
            .expect("assistant tool group must precede results and steering");
        assert!(matches!(tail[0].role, Role::Assistant));
        assert!(tail[1].content.iter().any(|block| matches!(block,
            ContentBlock::ToolResult { tool_use_id, content, is_error }
                if tool_use_id == tool_id && content == ECHO_REPLY && !is_error
        )));
        assert!(matches!(tail[2].role, Role::User));
        let steering = tail[2].first_text_content().unwrap();
        assert!(steering.starts_with(USER_INTERRUPT_PREFIX), "{steering}");
        assert!(steering.ends_with(&format!("\n\n{STEERING}\n</user-interrupt>")));
        assert_eq!(tail[2].display_text.as_deref(), Some(STEERING));
        assert_eq!(
            request
                .messages
                .iter()
                .filter(|message| { message.display_text.as_deref() == Some(STEERING) })
                .count(),
            1
        );
        let mut response = common::canned_reply(REPLY);
        response.usage = REQUEST_USAGE;
        request.respond(response);
        probe.idle_after_end().await;
        let result = common::bounded(probe.actor.latest_ticket().unwrap().wait_result()).await;
        assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
        assert_eq!(result.outcome.num_turns(), 2);
        assert_eq!(
            result.outcome.usage(),
            TokenUsage {
                input: REQUEST_USAGE.input * 2,
                output: REQUEST_USAGE.output * 2,
                cache_creation: REQUEST_USAGE.cache_creation * 2,
                cache_read: REQUEST_USAGE.cache_read * 2,
            }
        );
        assert_eq!(
            probe
                .observed
                .iter()
                .filter(|event| matches!(event,
                    ActorEvent::Start { turn_id, .. } if *turn_id == result.outcome.turn_id()
                ))
                .count(),
            1
        );
        let ends: Vec<_> = probe
            .observed
            .iter()
            .filter_map(|event| match event {
                ActorEvent::End(ended) if ended.outcome.turn_id() == result.outcome.turn_id() => {
                    Some(ended)
                }
                _ => None,
            })
            .collect();
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0].outcome, result.outcome);
        assert_eq!(
            fixture.tool(TASK_WAIT, wait_input(&successor))["result"]["text"],
            REPLY
        );
        assert_eq!(fixture.tool(TASK_WAIT, wait_input(&task)), original);
        assert_eq!(
            fixture.tool(TASK_WAIT, wait_input(&successor))["result"]["text"],
            REPLY
        );
        let drained = common::bounded(graph_quiescence_barrier(&[probe.actor.clone()]))
            .await
            .unwrap();
        assert!(drained.iter().all(|probe| probe.is_quiescent()));
        assert!(fixture.requests.is_empty());
        fixture.close(&task);
    });
}

#[test_case::test_case(false; "provider_failure")]
#[test_case::test_case(true; "accepted_input_exhausts_request_budget")]
fn lua_shared_turn_failure_never_completes_omitted_steering(budget: bool) {
    smol::block_on(async {
        let fixture = Fixture::with_max_turns(budget.then_some(1));
        let task = fixture.spawn(INITIAL);
        let request = fixture.request().await;
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        let ticket = actor.latest_ticket().unwrap();
        let joined = fixture.tool(
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": STEERING }),
        );
        assert_eq!(joined["turn_id"], task["turn_id"]);
        assert!(!request.contains(STEERING));
        if budget {
            request.respond(common::canned_reply(REPLY));
        } else {
            request
                .reply
                .send(Err(AgentError::Config {
                    message: PROVIDER_FAILURE.into(),
                }))
                .unwrap();
        }
        let result = common::bounded(ticket.wait_result()).await;
        let TurnOutcome::Failed { failure, .. } = &result.outcome else {
            panic!(
                "omitted steering must not be reported completed: {:?}",
                result.outcome
            );
        };
        assert!(
            failure.diagnostic.contains(if budget {
                INSTRUCTION_BUDGET_EXHAUSTED
            } else {
                PROVIDER_FAILURE
            }),
            "{failure:?}"
        );
        let waited = fixture.tool(TASK_WAIT, wait_input(&task));
        assert_eq!(waited["status"], "done");
        assert_eq!(waited["result"]["status"], "failed");
        assert_eq!(waited["result"]["turn_id"], task["turn_id"]);
        assert_eq!(fixture.tool(TASK_WAIT, wait_input(&task)), waited);
        let drained = common::bounded(graph_quiescence_barrier(&[actor]))
            .await
            .unwrap();
        assert!(drained.iter().all(|probe| probe.is_quiescent()));
        assert!(fixture.requests.is_empty());
        fixture.close(&task);
    });
}

struct ManagedDriver {
    context: ToolContext,
    registry: Arc<ToolRegistry>,
    spawned: flume::Sender<Value>,
    release: flume::Receiver<()>,
    started: bool,
    held_context: Option<Arc<Mutex<Option<ToolContext>>>>,
}

impl ActorBackend for ManagedDriver {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(config) = &context.policy {
                self.context.provider = Arc::clone(&config.provider);
                self.context.model = Arc::new(config.model.clone());
                self.context.mode = config.mode.clone();
                self.context.mode_def = config.mode_def.clone().map(Arc::new);
                self.context.opts.thinking = config.thinking;
                self.context.opts.fast = config.fast;
                self.context.workflow = config.workflow;
                self.context.cwd = config.context.cwd().to_path_buf();
            }
            if let Some(held) = &self.held_context {
                let mut tools = self.context.clone();
                tools.execution_agent_id = Some(context.agent_id);
                tools.managed_turn = context.managed_turn.clone();
                tools.cancel = context.cancel.clone();
                *held.lock().unwrap() = Some(tools);
                self.spawned.send(json!(true)).unwrap();
                let _ = self.release.recv_async().await;
            }
            if self.started {
                if let Some(inbox) = &context.inbox {
                    inbox.commit_completions(&mut |_| {});
                    inbox.instructions_submitted();
                    assert!(inbox.finish());
                }
                return BackendResult::EnteredRun(TurnOutcome::completed(
                    context.agent_id,
                    context.turn_id.unwrap(),
                    Default::default(),
                    0,
                    DoneReason::EndTurn,
                ));
            }
            self.started = true;
            self.context.tool_use_id = None;
            self.context.execution_agent_id = Some(context.agent_id);
            self.context.managed_turn = context.managed_turn;
            self.context.cancel = context.cancel;
            let context_tools = common::with_request_tools(&self.registry, &self.context);
            let input = spawn_input(PROMPTS[0]);
            let invocation = self
                .registry
                .get(TASK_SPAWN)
                .unwrap()
                .tool
                .parse(&input)
                .unwrap();
            let spawned = decode_output(invocation.execute(&context_tools).await.output.unwrap());
            assert!(spawned["task_id"].is_string(), "spawn failed: {spawned}");
            self.spawned.send(spawned).unwrap();
            self.release.recv_async().await.unwrap();
            if let Some(inbox) = &context.inbox {
                inbox.commit_completions(&mut |_| {});
                inbox.instructions_submitted();
                assert!(inbox.finish());
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

fn ordinary_input(message: &str) -> AgentInput {
    AgentInput {
        message: message.into(),
        images: Vec::new(),
        preamble: Vec::new(),
        prompt: None,
    }
}

struct ActorProbe {
    actor: maki_agent::AgentActorHandle,
    events: flume::Receiver<ActorEvent>,
    observed: Vec<ActorEvent>,
}

impl ActorProbe {
    fn child(manager: &AgentManagerHandle, parent: maki_agent::AgentId) -> Self {
        let node = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(parent))
            .unwrap_or_else(|| panic!("missing child of {parent}: {:?}", manager.snapshot()));
        let actor = manager.actor(node.agent_id).unwrap();
        let events = actor.subscribe_events();
        Self {
            actor,
            events,
            observed: Vec::new(),
        }
    }

    async fn idle_after_end(&mut self) {
        common::bounded(async {
            let mut ended = false;
            loop {
                let event = self.events.recv_async().await.unwrap();
                ended |= matches!(event, ActorEvent::End(_));
                let idle = ended && matches!(event, ActorEvent::Idle { .. });
                self.observed.push(event);
                if idle {
                    break;
                }
            }
        })
        .await;
        assert!(self.actor.latest_ticket().unwrap().peek_result().is_some());
        assert_eq!(self.actor.snapshot().queued, 0);
    }

    fn executed_deliveries(&mut self) -> usize {
        self.observed.extend(self.events.try_iter());
        let accepted = self.observed.iter().filter(|event| matches!(event,
            ActorEvent::Delivery(delivery) if matches!(delivery.status, DeliveryStatus::Accepted)
        )).count();
        let executed = self.observed.iter().filter(|event| matches!(event,
            ActorEvent::Delivery(delivery) if matches!(delivery.status, DeliveryStatus::Executed { .. })
        )).count();
        assert_eq!(
            accepted, executed,
            "every accepted automatic report must execute"
        );
        executed
    }
}

async fn graph_probe(probes: &mut [&mut ActorProbe; 3]) -> [usize; 3] {
    let actors: Vec<_> = probes.iter().map(|probe| probe.actor.clone()).collect();
    let drained = common::bounded(graph_quiescence_barrier(&actors))
        .await
        .unwrap();
    assert!(drained.iter().all(|probe| probe.is_quiescent()));
    std::array::from_fn(|index| probes[index].executed_deliveries())
}

#[test]
fn nested_continuations_report_once_and_quiesce() {
    automatic_graph_continuations(false);
}

#[test]
fn graph_quiescence_probe_detects_extra_continuation() {
    automatic_graph_continuations(true);
}

fn automatic_graph_continuations(extra_continuation: bool) {
    smol::block_on(async {
        let fixture = Fixture::new();
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let policy = policy(&fixture.context);
        let (spawned_tx, spawned) = flume::bounded(1);
        let (release, release_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(policy), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(ManagedDriver {
                    context: fixture.context.clone(),
                    registry: Arc::clone(&fixture.registry),
                    spawned: spawned_tx,
                    release: release_rx,
                    started: false,
                    held_context: None,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let driver = root
            .actor()
            .unwrap()
            .admit_turn(ordinary_input(INITIAL), None, INITIAL.into())
            .unwrap();
        common::bounded(spawned.recv_async()).await.unwrap();
        let mut a = ActorProbe::child(&manager, root.id());
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[1])));
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        assert!(
            request.last_tool_result()["task_id"].is_string(),
            "nested spawn failed: {}",
            request.last_tool_result()
        );
        let mut b = ActorProbe::child(&manager, a.actor.agent_id());
        let original_b = b
            .actor
            .turn_ticket(
                serde_json::from_value(request.last_tool_result()["turn_id"].clone()).unwrap(),
            )
            .unwrap();
        request.respond(common::canned_reply(REPLY));
        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[2])));
        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        assert!(
            request.last_tool_result()["task_id"].is_string(),
            "nested spawn failed: {}",
            request.last_tool_result()
        );
        let mut c = ActorProbe::child(&manager, b.actor.agent_id());
        let blocked_b = request;
        let leaf = fixture.request_for(PROMPTS[2]).await;
        assert!(leaf.contains(PROMPTS[2]));

        // End B's original ticket without closing C, so C must wake an idle B.
        assert!(original_b.peek().is_none());
        assert_eq!(
            b.actor.latest_ticket().unwrap().turn_id(),
            original_b.turn_id()
        );
        b.actor.cancel_local(0);
        b.idle_after_end().await;
        drop(blocked_b);
        assert!(matches!(
            original_b.peek(),
            Some(TurnOutcome::Cancelled { .. })
        ));
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        request.respond(common::canned_reply(REPLY));
        a.idle_after_end().await;
        let original_a = a.actor.latest_ticket().unwrap();
        let original_a_result = common::bounded(original_a.wait_result()).await;
        let original_b_result = common::bounded(original_b.wait_result()).await;

        leaf.respond(common::canned_reply(REPLY));
        c.idle_after_end().await;
        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        assert!(request.messages.iter().any(Message::is_observation));
        request.respond(common::canned_reply(REPLY));
        b.idle_after_end().await;
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        assert!(request.messages.iter().any(Message::is_observation));
        request.respond(common::canned_reply(REPLY));
        a.idle_after_end().await;
        assert_ne!(
            a.actor.latest_ticket().unwrap().turn_id(),
            original_a.turn_id()
        );
        assert_ne!(
            b.actor.latest_ticket().unwrap().turn_id(),
            original_b.turn_id()
        );
        assert!(matches!(
            original_b.peek(),
            Some(TurnOutcome::Cancelled { .. })
        ));
        let retained_a = common::bounded(original_a.wait_result()).await;
        let retained_b = common::bounded(original_b.wait_result()).await;
        assert_eq!(retained_a.outcome, original_a_result.outcome);
        assert_eq!(retained_a.text, original_a_result.text);
        assert_eq!(retained_b.outcome, original_b_result.outcome);
        assert_eq!(retained_b.text, original_b_result.text);
        let baseline = graph_probe(&mut [&mut c, &mut b, &mut a]).await;
        assert_eq!(baseline, [0, 1, 2]);
        if extra_continuation {
            let leaf_turn = c
                .actor
                .admit_turn(ordinary_input(STEERING), None, STEERING.into())
                .unwrap();
            let request = fixture.request().await;
            assert!(request.contains(PROMPTS[2]));
            let actors = [c.actor.clone(), b.actor.clone(), a.actor.clone()];
            let barrier = graph_quiescence_barrier(&actors);
            futures_lite::pin!(barrier);
            assert!(
                futures_lite::future::poll_once(&mut barrier)
                    .await
                    .is_none()
            );
            assert!(leaf_turn.peek_result().is_none());
            request.respond(common::canned_reply(REPLY));
            c.idle_after_end().await;
            common::bounded(leaf_turn.wait()).await;
            for probe in [&mut b, &mut a] {
                let request = fixture.request().await;
                assert!(request.messages.iter().any(Message::is_observation));
                assert!(
                    futures_lite::future::poll_once(&mut barrier)
                        .await
                        .is_none()
                );
                assert!(probe.actor.latest_ticket().unwrap().peek_result().is_none());
                request.respond(common::canned_reply(REPLY));
                probe.idle_after_end().await;
            }
            let drained = common::bounded(barrier).await.unwrap();
            assert!(drained.iter().all(|probe| probe.is_quiescent()));
            let extra = graph_probe(&mut [&mut c, &mut b, &mut a]).await;
            assert_ne!(
                extra, baseline,
                "probe must detect the extra upward continuation"
            );
            assert_eq!(extra, [0, 2, 3]);
        }
        release.send(()).unwrap();
        common::bounded(driver.wait()).await;
        let drained = common::bounded(manager.drain_graph_for_test())
            .await
            .unwrap();
        assert_eq!(drained.len(), 4);
        assert!(drained.iter().all(|(_, probe)| probe.is_quiescent()));
        assert_eq!(
            graph_probe(&mut [&mut c, &mut b, &mut a]).await,
            if extra_continuation {
                [0, 2, 3]
            } else {
                baseline
            }
        );
        assert!(fixture.requests.is_empty());
        assert!(
            manager
                .shutdown(SHUTDOWN_TIMEOUT)
                .await
                .timed_out
                .is_empty()
        );
    });
}

enum SettledState {
    Done,
    Cancelled,
    Failed,
}

#[test_case::test_case(SettledState::Done; "done")]
#[test_case::test_case(SettledState::Cancelled; "cancel")]
#[test_case::test_case(SettledState::Failed; "provider_failure")]
fn task_send_after_settlement_starts_successor(state: SettledState) {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        let request = fixture.request().await;
        let root = fixture.manager.root().unwrap();
        let actor = ActorProbe::child(&fixture.manager, root.id()).actor;
        let original = actor.latest_ticket().unwrap();
        match state {
            SettledState::Done => request.respond(common::canned_reply(REPLY)),
            SettledState::Cancelled => {
                actor.cancel_local(0);
                common::bounded(original.wait_result()).await;
                drop(request);
            }
            SettledState::Failed => request
                .reply
                .send(Err(AgentError::Config {
                    message: PROVIDER_FAILURE.into(),
                }))
                .unwrap(),
        }
        let result = common::bounded(original.wait_result()).await;
        assert!(match state {
            SettledState::Done => matches!(result.outcome, TurnOutcome::Completed { .. }),
            SettledState::Cancelled => matches!(result.outcome, TurnOutcome::Cancelled { .. }),
            SettledState::Failed => matches!(result.outcome, TurnOutcome::Failed { .. }),
        });
        let mut successor = fixture.tool(
            TASK_SEND,
            json!({
                "task_id": task["task_id"], "message": STEERING,
            }),
        );
        assert_eq!(successor["queued"], true);
        assert_ne!(successor["turn_id"], task["turn_id"]);
        successor["task_id"] = task["task_id"].clone();
        let request = fixture.request().await;
        assert!(request.contains(STEERING));
        request.respond(common::canned_reply(REPLY));
        let settled = fixture.tool(TASK_WAIT, wait_input(&successor));
        assert_eq!(settled["status"], "done");
        assert_eq!(settled["result"]["status"], "completed");
        assert_eq!(settled["result"]["turn_id"], successor["turn_id"]);
        assert_eq!(original.wait_result().await.outcome, result.outcome);
        fixture.close(&task);
    });
}

#[test]
fn task_send_after_despawn_rejects() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let task = fixture.spawn(INITIAL);
        fixture.request().await.respond(common::canned_reply(REPLY));
        assert_eq!(fixture.tool(TASK_WAIT, wait_input(&task))["status"], "done");
        fixture.close(&task);
        let error = common::exec_tool_text(
            &fixture.registry,
            &fixture.context,
            TASK_SEND,
            json!({ "task_id": task["task_id"], "message": STEERING }),
        )
        .unwrap_err();
        assert!(error.contains(UNKNOWN_TASK_ERR));
        assert!(fixture.requests.is_empty());
    });
}

#[test]
fn nested_provider_gates_cross_bundled_task_wait_boundaries() {
    smol::block_on(async {
        let fixture = Fixture::new();
        let a = fixture.spawn(PROMPTS[0]);
        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[1])));

        let request = fixture.request_for(PROMPTS[0]).await;
        assert!(request.contains(PROMPTS[0]));
        let b = request.last_tool_result();
        assert!(b["task_id"].is_string());
        request.respond(common::canned_tool_use(TASK_WAIT, wait_input(&b)));

        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        request.respond(common::canned_tool_use(TASK_SPAWN, spawn_input(PROMPTS[2])));

        let request = fixture.request_for(PROMPTS[1]).await;
        assert!(request.contains(PROMPTS[1]));
        let c = request.last_tool_result();
        assert!(c["task_id"].is_string());
        request.respond(common::canned_tool_use(TASK_WAIT, wait_input(&c)));

        let request = fixture.request_for(PROMPTS[2]).await;
        assert!(request.contains(PROMPTS[2]));
        let admitted = fixture.tool(
            TASK_SEND,
            json!({ "task_id": a["task_id"], "message": STEERING }),
        );
        assert_eq!(admitted["turn_id"], a["turn_id"]);
        assert!(fixture.requests.is_empty());
        request.respond(common::canned_reply(REPLY));

        for prompt in [PROMPTS[1], PROMPTS[0]] {
            let request = fixture.request().await;
            assert!(request.contains(prompt));
            if prompt == PROMPTS[0] {
                assert!(request.contains(STEERING));
            }
            let waited = request.last_tool_result();
            assert_eq!(waited["status"], "done");
            assert_eq!(waited["result"]["text"], REPLY);
            request.respond(common::canned_reply(REPLY));
        }
        let result = fixture.tool(TASK_WAIT, wait_input(&a));
        assert_eq!(result["status"], "done");
        assert_eq!(result["result"]["turn_id"], a["turn_id"]);
        assert_eq!(result["result"]["text"], REPLY);
        fixture.close(&a);
    });
}
