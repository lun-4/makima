use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use maki_agent::actor::DeliveryStatus;
use maki_agent::tools::{ToolAudience, ToolContext, ToolRegistry, TurnToolBindings};
use maki_agent::{
    ActorBackend, ActorEvent, AgentEvent, AgentId, AgentInput, AgentLimits, AgentManagerHandle,
    AgentMetadata, AgentMode, AgentRef, BackendResult, ControlWork, DoneReason, Envelope,
    GraphLifecycle, History, RunSettings, ToolOutput, TurnContext, TurnId, TurnOutcome, TurnResult,
    TurnTicket, WorkKind,
};
use maki_lua::PluginHost;
use maki_lua::agent_errors::{INVOCATION_EXPIRED, MANAGED_AUTHORITY_INACTIVE};
use maki_lua::test_support::InMemoryFs;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ObservationSource, ProviderEvent,
    RequestOptions, Role, StreamResponse,
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};

mod common;

const TOOL_NAME: &str = "managed_session";
const SILENT_TOOL_NAME: &str = "managed_session_silent";
const TIMEOUT_TOOL_NAME: &str = "managed_session_timeout";
const SILENT_REPLY: &str = "silent child result";
const RETAIN_TOOL_NAME: &str = "managed_session_retain";
const OUTSIDE_TIMEOUT_TOOL_NAME: &str = "managed_session_outside_timeout";
const JOIN_TOOL_NAME: &str = "managed_session_join";
const RETAIN_ASYNC_TOOL_NAME: &str = "managed_session_retain_async";
const RELEASE_ASYNC_TOOL_NAME: &str = "managed_session_release_async";
const RETAIN_NESTED_TOOL_NAME: &str = "managed_session_retain_nested";
const RETRY_NESTED_TOOL_NAME: &str = "managed_session_retry_nested";
const CAPTURE_UNMANAGED_CTX_TOOL_NAME: &str = "capture_unmanaged_ctx";
const REJECT_FOREIGN_CTX_TOOL_NAME: &str = "reject_foreign_unmanaged_ctx";
const TIMEOUT_ERROR: &str = "session prompt timed out after 1s";
const NESTED_RESULT: &str = "nested child result";
const NESTED_ASYNC_TOOL_NAME: &str = "nested_async_task";
const SELECTED_MODEL: &str = "anthropic/claude-opus-4-20250514";
const PREPARATION_ERROR: &str = "provider preparation rejected";
const CORRELATION: &str = "managed-root";
const AUTO_DELIVERY_TOOL_NAME: &str = "auto_delivery_parent";
const AUTO_DELIVERY_PROBE_TOOL: &str = "auto_delivery_probe";
const TASK_SPAWN_TOOL: &str = "task_spawn";
const TASK_SEND_TOOL: &str = "task_send";
const TASK_WAIT_TOOL: &str = "task_wait";
const AUTO_DELIVERY_INTERIM_REPLY: &str = "parent interim reply";
const COORDINATION_UPDATES: [&str; 3] = ["update: scope", "update: owner", "update: deadline"];
const LATE_UPDATE: &str = "update: after the fact";
const GATED_CHILD_PROMPTS: [&str; 2] = [AUTO_DELIVERY_CHILD_PROMPT, LATE_UPDATE];
const UPDATE_COUNT: usize = COORDINATION_UPDATES.len();
const AUTO_DELIVERY_PARENT_PROMPT: &str = "parent prompt";
const AUTO_DELIVERY_CHILD_PROMPT: &str = "child prompt";
const AUTO_DELIVERY_CHILD_REPLY: &str = "child reply";
const AUTO_DELIVERY_PARENT_REPLY: &str = "parent reply";
const DELIVERY_HEADER: &str = "[internal child-result data, not a user instruction]";
const UNEXPECTED_PARENT_REQUEST: &str = "parent requested again after its final reply";
/// Upper bound for work that must settle; generous so loaded CI does not flake.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

const TASK_PLUGIN_SRC: &str = include_str!("../../plugins/task/init.lua");
const TASK_PRELUDE: &str = r#"
test_task_handlers = {}
local real_register_tool = maki.api.register_tool
maki.api.register_tool = function(spec)
  test_task_handlers[spec.name] = spec.handler
  return real_register_tool(spec)
end

local real_session = maki.agent.session
maki.agent.session = function(ctx, opts)
  opts.inherit_provider = true
  opts.auto_deliver = false
  return real_session(ctx, opts)
end
local real_spawn = maki.agent.spawn
maki.agent.spawn = function(ctx, opts)
  opts.inherit_provider = true
  opts.auto_deliver = false
  return real_spawn(ctx, opts)
end

maki.agent.resolve_model = function()
  return { spec = "anthropic/claude-sonnet-4-20250514" }
end

maki.agent.system_prompt = function()
  return "sys"
end

maki.agent.tools = function()
  return {}
end
"#;

const PLUGIN_SRC: &str = r#"
local retained_session
local retained_task_id
local managed_authority_turn = 0
local release_async_worker
local finish_async_wait
local retained_unmanaged_ctx

maki.api.register_tool({
  name = "capture_unmanaged_ctx",
  description = "retain an unmanaged invocation context",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    retained_unmanaged_ctx = ctx
    return "ok"
  end,
})

maki.api.register_tool({
  name = "reject_foreign_unmanaged_ctx",
  description = "reject an unmanaged context inside a managed invocation",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    local session, err = maki.agent.session(retained_unmanaged_ctx, {
      audience = {},
      model = { spec = "not/a/model" },
    })
    if session ~= nil or err ~= "$EXPIRED_CONTEXT_ERROR" then
      return { llm_output = "unexpected foreign context result: " .. tostring(err), is_error = true }
    end
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_join",
  description = "prompt an existing child from a joined worker",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    local session = assert(maki.agent.session(ctx, {
      name = "managed-joined-child",
      inherit_provider = true,
      auto_deliver = false,
    }))
    local result, prompt_err
    maki.async.join(1, {
      function()
        result, prompt_err = session:prompt("reply")
      end,
    })
    session:close()
    if not result then
      return { llm_output = prompt_err, is_error = true }
    end
    return result.text
  end,
})

maki.api.register_tool({
  name = "managed_session_retain_async",
  description = "retain task authority past a managed invocation",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    managed_authority_turn = managed_authority_turn + 1
    if managed_authority_turn == 2 then
      local sent = test_task_handlers.task_send({
        task_id = retained_task_id,
        message = "valid later turn",
      }, ctx)
      if sent.is_error then
        return sent
      end
      return "ok"
    end

    local spawned = test_task_handlers.task_spawn({
      description = "retained-child",
      prompt = "initial child turn",
      subagent_type = "general",
    }, ctx)
    if spawned.is_error then
      return spawned
    end
    retained_task_id = maki.json.decode(spawned.llm_output).task_id
    maki.async.await(1, function(worker_started)
      maki.async.run(function()
        maki.async.await(1, function(done)
          release_async_worker = done
          worker_started()
        end)
        local sent = test_task_handlers.task_send({
          task_id = retained_task_id,
          message = "stale turn",
        })
        local despawned = test_task_handlers.task_despawn({ task_id = retained_task_id })
        finish_async_wait(sent.llm_output, sent.is_error, despawned.llm_output, despawned.is_error)
      end)
    end)
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_release_async",
  description = "release and await retained stale task authority",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    local send_err, send_is_error, despawn_err, despawn_is_error = maki.async.await(1, function(done)
      finish_async_wait = done
      release_async_worker()
    end)
    if send_err ~= "task is owned by another agent branch" or send_is_error ~= true then
      return { llm_output = "unexpected stale task_send result: " .. tostring(send_err), is_error = true }
    end
    if despawn_err ~= "task is owned by another agent branch" or despawn_is_error ~= true then
      return { llm_output = "unexpected stale task_despawn result: " .. tostring(despawn_err), is_error = true }
    end
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_retain",
  description = "retain a managed child",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    retained_session = assert(maki.agent.session(ctx, {
      name = "managed-retained-child",
      inherit_provider = true,
      auto_deliver = false,
    }))
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_outside_timeout",
  description = "reject a retained managed child outside its turn",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    local result, err = retained_session:prompt("wait", { timeout = 1 })
    if result ~= nil or err ~= "$MANAGED_AUTHORITY_ERROR" then
      return { llm_output = "unexpected retained authority result: " .. tostring(err), is_error = true }
    end
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_retain_nested",
  description = "retain a nested managed child after its first prompt",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "general_sub" },
  handler = function(_, ctx)
    retained_session = assert(maki.agent.session(ctx, {
      name = "managed-retained-nested-child",
      inherit_provider = true,
      auto_deliver = false,
    }))
    local result, err = retained_session:prompt("nested initial prompt")
    if not result then
      return { llm_output = err, is_error = true }
    end
    return result.text
  end,
})

maki.api.register_tool({
  name = "managed_session_retry_nested",
  description = "verify a retained nested child is permanently closed",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    local result, err = retained_session:prompt("must reject")
    if result ~= nil or err ~= "$MANAGED_AUTHORITY_ERROR" then
      return { llm_output = "unexpected retained close result: " .. tostring(err), is_error = true }
    end
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_timeout",
  description = "time out a managed child prompt",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    local session, err = maki.agent.session(ctx, {
      name = "managed-timeout-child",
      inherit_provider = true,
      auto_deliver = false,
    })
    if not session then
      return { llm_output = err, is_error = true }
    end
    local result, prompt_err = session:prompt("wait", { timeout = 1 })
    if result ~= nil or prompt_err ~= "session prompt timed out after 1s" then
      return { llm_output = "unexpected timeout result", is_error = true }
    end
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session",
  description = "create and close a managed child",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    local session, err = maki.agent.session(ctx, {
      name = "managed-child",
      inherit_provider = true,
      auto_deliver = false,
    })
    if not session then
      return { llm_output = err, is_error = true }
    end
    local result, prompt_err = session:prompt("reply")
    session:close()
    if not result then
      return { llm_output = prompt_err, is_error = true }
    end
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_silent",
  description = "create and close a silent managed child",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    local session, err = maki.agent.session(ctx, {
      name = "managed-silent-child",
      inherit_provider = true,
      auto_deliver = false,
      silent = true,
    })
    if not session then
      return { llm_output = err, is_error = true }
    end
    local result, prompt_err = session:prompt("reply")
    session:close()
    if not result then
      return { llm_output = prompt_err, is_error = true }
    end
    if result.text ~= "silent child result" then
      return { llm_output = "unexpected silent result: " .. result.text, is_error = true }
    end
    return result.text
  end,
})
"#;

/// Leaves `auto_deliver` at its default so the model-driven parent receives its
/// child's result from the runtime instead of a `task_wait`.
const AUTO_DELIVERY_PRELUDE: &str = r#"
test_task_handlers = {}
local real_register_tool = maki.api.register_tool
maki.api.register_tool = function(spec)
  test_task_handlers[spec.name] = spec.handler
  return real_register_tool(spec)
end

local real_spawn = maki.agent.spawn
maki.agent.spawn = function(ctx, opts)
  opts.inherit_provider = true
  return real_spawn(ctx, opts)
end

maki.agent.resolve_model = function()
  return { spec = "anthropic/claude-sonnet-4-20250514" }
end

maki.agent.system_prompt = function()
  return "sys"
end

maki.agent.tools = function(_, opts)
  if opts.audience ~= "general_sub" then
    return nil
  end
  return {
    { name = "$TASK_SPAWN_TOOL", description = "spawn", input_schema = { type = "object" } },
    { name = "$TASK_SEND_TOOL", description = "send", input_schema = { type = "object" } },
    { name = "$TASK_WAIT_TOOL", description = "wait", input_schema = { type = "object" } },
    { name = "$PROBE_TOOL", description = "probe", input_schema = { type = "object" } },
  }
end

maki.api.register_tool({
  name = "$PROBE_TOOL",
  description = "no-op tool between the parent's requests",
  schema = { type = "object", properties = {} },
  audiences = { "general_sub" },
  handler = function()
    return "ok"
  end,
})
"#;

const AUTO_DELIVERY_DRIVER: &str = r#"
maki.api.register_tool({
  name = "$DRIVER_TOOL",
  description = "spawn a model-driven parent and wait for its turn",
  schema = { type = "object", properties = {} },
  audiences = { "general_sub" },
  handler = function(_, ctx)
    local spawned = test_task_handlers.task_spawn({
      description = "auto-delivery-parent", prompt = "$PARENT_PROMPT",
      subagent_type = "general",
    }, ctx)
    if spawned.is_error then return spawned end
    local ids = maki.json.decode(spawned.llm_output)
    local waited = test_task_handlers.task_wait({
      task_id = ids.task_id, turn_id = ids.turn_id,
    }, ctx)
    if waited.is_error then return waited end
    local result = maki.json.decode(waited.llm_output)
    local closed = test_task_handlers.task_despawn({ task_id = ids.task_id }, ctx)
    if closed.is_error then return closed end
    return result.result.text
  end,
})
"#;

fn session_plugin_source() -> String {
    PLUGIN_SRC
        .replace("$EXPIRED_CONTEXT_ERROR", INVOCATION_EXPIRED)
        .replace("$MANAGED_AUTHORITY_ERROR", MANAGED_AUTHORITY_INACTIVE)
}

struct LuaToolBackend {
    registry: Arc<ToolRegistry>,
    context: ToolContext,
    completed: flume::Sender<Result<(), String>>,
    tool_name: &'static str,
}

impl ActorBackend for LuaToolBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            assert!(context.policy.is_some());
            assert!(
                context
                    .managed_turn
                    .as_ref()
                    .and_then(|turn| turn.policy_snapshot())
                    .is_some()
            );
            assert!(!context.cancel.is_cancelled());
            self.context.cancel = context.cancel;
            self.context.managed_turn = context.managed_turn;
            self.context = common::with_request_tools(&self.registry, &self.context);
            let invocation = self
                .registry
                .get(self.tool_name)
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

struct NestedTaskBackend {
    registry: Arc<ToolRegistry>,
    context: ToolContext,
    tool_name: &'static str,
    input: Value,
    completed: flume::Sender<Result<String, String>>,
    release: flume::Receiver<()>,
}

impl ActorBackend for NestedTaskBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            assert!(context.policy.is_some());
            assert!(
                context
                    .managed_turn
                    .as_ref()
                    .and_then(|turn| turn.policy_snapshot())
                    .is_some()
            );
            assert!(!context.cancel.is_cancelled());
            self.context.cancel = context.cancel;
            self.context.managed_turn = context.managed_turn;
            self.context.audience = ToolAudience::GENERAL_SUB;
            self.context = common::with_request_tools(&self.registry, &self.context);
            let invocation = self
                .registry
                .get(self.tool_name)
                .unwrap()
                .tool
                .parse(&self.input)
                .unwrap();
            let result = invocation
                .execute(&self.context)
                .await
                .output
                .map(|output| {
                    let (ToolOutput::Plain(output) | ToolOutput::Markdown(output)) = output else {
                        panic!("unexpected nested task output")
                    };
                    output.text
                });
            let _ = self.completed.send(result.clone());
            self.release.recv_async().await.unwrap();
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

struct SpawnManagedChildBackend {
    child_backend: Option<Box<dyn ActorBackend>>,
    child: flume::Sender<(AgentRef, TurnTicket)>,
}

impl ActorBackend for SpawnManagedChildBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            let current = context.managed_turn.unwrap();
            let child = current
                .spawn_child(
                    AgentMetadata::default(),
                    Vec::new(),
                    None,
                    self.child_backend.take().unwrap(),
                )
                .unwrap();
            let ticket = child
                .actor()
                .unwrap()
                .admit_turn(input(), None, CORRELATION.into())
                .unwrap();
            self.child.send((child, ticket)).unwrap();
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

fn parent_policy(context: &ToolContext) -> maki_agent::actor::EffectiveAgentConfig {
    maki_agent::actor::EffectiveAgentConfig::new(
        RunSettings {
            provider: Arc::clone(&context.provider),
            model: (*context.model).clone(),
            fast: false,
            workflow: context.workflow,
            thinking: Default::default(),
        },
        AgentMode::Build,
    )
}

fn load_task_host() -> (Arc<ToolRegistry>, PluginHost) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(
        "managed-nested-task",
        &format!(
            "{TASK_PRELUDE}\n{TASK_PLUGIN_SRC}\n{}",
            r#"
            maki.api.register_tool({
                name = "nested_async_task",
                description = "spawn and wait for a managed descendant",
                schema = { type = "object", properties = {} },
                audiences = { "general_sub" },
                handler = function(_, ctx)
                    local spawned = test_task_handlers.task_spawn({
                        description = "nested-child", prompt = "nested prompt",
                        subagent_type = "general",
                    }, ctx)
                    if spawned.is_error then return spawned end
                    local ids = maki.json.decode(spawned.llm_output)
                    assert(ids.task_id and ids.turn_id)
                    local waited = test_task_handlers.task_wait({
                        task_id = ids.task_id, turn_id = ids.turn_id,
                    }, ctx)
                    if waited.is_error then return waited end
                    local result = maki.json.decode(waited.llm_output)
                    assert(result.status == "done")
                    assert(result.result.turn_id == ids.turn_id)
                    local closed = test_task_handlers.task_despawn({ task_id = ids.task_id }, ctx)
                    if closed.is_error then return closed end
                    return result.result.text
                end,
            })
            "#
        ),
    )
    .unwrap();
    (registry, host)
}

fn load_auto_delivery_host() -> (Arc<ToolRegistry>, PluginHost) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    let source = format!("{AUTO_DELIVERY_PRELUDE}\n{TASK_PLUGIN_SRC}\n{AUTO_DELIVERY_DRIVER}")
        .replace("$TASK_SPAWN_TOOL", TASK_SPAWN_TOOL)
        .replace("$TASK_SEND_TOOL", TASK_SEND_TOOL)
        .replace("$TASK_WAIT_TOOL", TASK_WAIT_TOOL)
        .replace("$PROBE_TOOL", AUTO_DELIVERY_PROBE_TOOL)
        .replace("$DRIVER_TOOL", AUTO_DELIVERY_TOOL_NAME)
        .replace("$PARENT_PROMPT", AUTO_DELIVERY_PARENT_PROMPT);
    host.load_source("managed-auto-delivery", &source).unwrap();
    (registry, host)
}

async fn spawn_managed_parent(
    manager: &AgentManagerHandle,
    policy: maki_agent::actor::EffectiveAgentConfig,
    child_backend: Box<dyn ActorBackend>,
) -> (AgentRef, TurnTicket) {
    let (child_tx, child_rx) = flume::bounded(1);
    let root = manager
        .create_root_with_config(Some(policy), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(SpawnManagedChildBackend {
                child_backend: Some(child_backend),
                child: child_tx,
            }) as Box<dyn ActorBackend>)
        })
        .unwrap();
    let root_ticket = root
        .actor()
        .unwrap()
        .admit_turn(input(), None, CORRELATION.into())
        .unwrap();
    let child = child_rx.recv_async().await.unwrap();
    assert!(matches!(
        root_ticket.wait().await,
        TurnOutcome::Completed { .. }
    ));
    child
}

struct RecordingProvider {
    requests: flume::Sender<(String, RequestOptions)>,
}

struct NestedRecordingProvider {
    requests: flume::Sender<(String, RequestOptions)>,
    calls: std::sync::atomic::AtomicUsize,
}

impl Provider for NestedRecordingProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        _: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        opts: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.requests.send((model.spec(), opts)).unwrap();
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                Ok(common::canned_tool_use("spawn_nested", json!({})))
            } else {
                Ok(common::canned_reply(NESTED_RESULT))
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl Provider for RecordingProvider {
    fn stream_message<'a>(
        &'a self,
        model: &'a Model,
        _: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        opts: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.requests.send((model.spec(), opts)).unwrap();
            Ok(common::canned_reply(SILENT_REPLY))
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

#[test]
fn managed_lua_requests_follow_resolved_and_updated_configuration() {
    smol::block_on(async {
        let registry = Arc::clone(ToolRegistry::global_arc());
        let (first_tx, first_rx) = flume::unbounded();
        let selected_provider: Arc<dyn Provider> =
            Arc::new(RecordingProvider { requests: first_tx });
        let prepared_provider = Arc::clone(&selected_provider);
        let (prepared_tx, prepared_rx) = flume::unbounded();
        let host = PluginHost::with_session_provider_preparer(
            Arc::clone(&registry),
            Default::default(),
            true,
            Arc::new(InMemoryFs::new()),
            None,
            Some(Arc::new(move |model| {
                prepared_tx.send(model.spec()).unwrap();
                if model.spec() != SELECTED_MODEL {
                    return Err(PREPARATION_ERROR.into());
                }
                Ok((model, Arc::clone(&prepared_provider)))
            })),
        )
        .unwrap();
        host.load_source(
            "managed-config",
            &r#"
            local child
            maki.api.register_tool({
                name = "spawn_nested",
                description = "spawn a child from the executing configuration",
                schema = { type = "object", properties = {} },
                audiences = { "general_sub" },
                handler = function(_, ctx)
                    assert(ctx:workflow() == true)
                    local nested = assert(maki.agent.session(ctx, { inherit_provider = true, auto_deliver = false }))
                    local result = assert(nested:prompt("nested"))
                    nested:close()
                    return result.text
                end,
            })
            maki.api.register_tool({
                name = "managed_session",
                description = "create and retain a configured child",
                schema = { type = "object", properties = {} },
                handler = function(_, ctx)
                    local rejected, policy_err = maki.agent.session(ctx, { model = { spec = "openai/gpt-4.1" } })
                    assert(rejected == nil and policy_err ~= nil)
                    local failed, preparation_err = maki.agent.session(ctx, { model = { spec = "anthropic/claude-opus-5" } })
                    assert(failed == nil and preparation_err == "$PREPARATION_ERROR")
                    child = assert(maki.agent.session(ctx, {
                        model = { spec = "anthropic/claude-opus-4-20250514", fast = true, thinking = "off" }, auto_deliver = false,
                        tools = {{ name = "spawn_nested", description = "spawn nested", input_schema = { type = "object", properties = {} } }},
                    }))
                    assert(child:prompt("first"))
                    return "ok"
                end,
            })
        "#.replace("$PREPARATION_ERROR", PREPARATION_ERROR),
        )
        .unwrap();
        let (mut context, events, _cancel) = common::ctx_with_canned_provider();
        let (parent_tx, parent_rx) = flume::unbounded();
        context.provider = Arc::new(RecordingProvider {
            requests: parent_tx,
        });
        context.model = Arc::new(Model::from_spec("anthropic/claude-opus-5").unwrap());
        let parent_model = context.model.spec();
        context.model_policy =
            Arc::new(maki_config::ModelPolicy::new(&["anthropic/*".to_owned()], &[]).unwrap());
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(parent_policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaToolBackend {
                    registry,
                    context,
                    completed: completed_tx,
                    tool_name: TOOL_NAME,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();
        assert_eq!(completed_rx.recv_async().await.unwrap(), Ok(()));
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let child_id = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .unwrap()
            .agent_id;
        let actor = manager.actor(child_id).unwrap();
        let initial = actor.effective_config().unwrap();
        assert_eq!(manager.snapshot().len(), 2);
        assert_eq!(
            prepared_rx.try_iter().collect::<Vec<_>>(),
            [parent_model.clone(), SELECTED_MODEL.to_owned()]
        );
        assert_eq!(initial.model.spec(), SELECTED_MODEL);
        assert_ne!(initial.model.spec(), parent_model);
        assert!(Arc::ptr_eq(&initial.provider, &selected_provider));
        assert!(parent_rx.is_empty());
        let (model, opts) = first_rx.try_recv().unwrap();
        assert_eq!(model, initial.model.spec());
        assert_eq!(opts.fast, initial.fast);
        assert_eq!(opts.thinking, initial.thinking);
        assert!(!initial.fast);
        let (updated_tx, updated_rx) = flume::unbounded();
        let mut updated = (*initial).clone();
        updated.settings.provider = Arc::new(NestedRecordingProvider {
            requests: updated_tx,
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        updated.settings.model = Model::from_spec(&parent_model).unwrap();
        updated.settings.fast = false;
        updated.settings.thinking = maki_providers::ThinkingConfig::Budget(4096);
        updated.settings.workflow = true;
        let update = actor.reserve_config_update().unwrap();
        update
            .resolve(Ok(maki_agent::actor::ConfigChange::Patch(
                maki_agent::actor::ConfigPatch {
                    model: Some(maki_agent::actor::PreparedModel {
                        provider: Arc::clone(&updated.provider),
                        model: updated.model.clone(),
                    }),
                    fast: Some(false),
                    thinking: Some(updated.thinking),
                    workflow: Some(true),
                    ..Default::default()
                },
            )))
            .unwrap();
        update.wait().await.unwrap();
        let ticket = actor.admit_turn(input(), None, CORRELATION.into()).unwrap();
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let requests: Vec<_> = updated_rx.try_iter().collect();
        assert_eq!(requests.len(), 3);
        for (model, opts) in requests {
            assert_eq!(model, updated.model.spec());
            assert!(!opts.fast);
            assert_eq!(opts.thinking, updated.thinking);
        }
        let nested_id = closed_child(&events, child_id).await;
        wait_for_runner_finished(&manager, nested_id).await;
        assert!(
            manager
                .snapshot()
                .iter()
                .all(|node| node.agent_id != nested_id)
        );
        assert!(first_rx.is_empty());
        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

struct PendingProvider {
    dropped: flume::Sender<()>,
}

struct DropSignal(flume::Sender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

impl Provider for PendingProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let _signal = DropSignal(self.dropped.clone());
            std::future::pending::<()>().await;
            unreachable!()
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn input() -> AgentInput {
    AgentInput {
        message: "create child".into(),
        mode: AgentMode::Build,
        images: Vec::new(),
        preamble: Vec::new(),
        thinking: Default::default(),
        fast: false,
        workflow: false,
        prompt: None,
        cancel: None,
        lease_committer: None,
    }
}

async fn closed_child(events: &flume::Receiver<Envelope>, parent_id: AgentId) -> AgentId {
    futures_lite::future::race(
        async {
            loop {
                let envelope = events.recv_async().await.unwrap();
                if matches!(envelope.event, AgentEvent::SubagentClosed)
                    && let Some(info) = envelope.subagent
                    && info.parent_agent_id == Some(parent_id)
                {
                    return info.agent_id;
                }
            }
        },
        async {
            smol::Timer::after(SETTLE_TIMEOUT).await;
            panic!("managed child of {parent_id} did not close");
        },
    )
    .await
}

async fn wait_for_runner_finished(manager: &AgentManagerHandle, agent_id: AgentId) {
    let finished = futures_lite::future::race(
        async {
            while !manager.runner_finished(agent_id).unwrap() {
                smol::future::yield_now().await;
            }
            true
        },
        async {
            smol::Timer::after(SETTLE_TIMEOUT).await;
            false
        },
    )
    .await;
    assert!(finished, "managed agent {agent_id} runner did not finish");
}

#[test]
fn managed_nested_task_completes_before_parent_turn_ends_at_capacity_one() {
    smol::block_on(async {
        let (registry, _host) = load_task_host();
        let (context, events, _cancel) =
            common::ctx_with_replies(vec![common::canned_reply(NESTED_RESULT)]);
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        })
        .unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let (parent, parent_ticket) = spawn_managed_parent(
            &manager,
            parent_policy(&context),
            Box::new(NestedTaskBackend {
                registry,
                context,
                tool_name: NESTED_ASYNC_TOOL_NAME,
                input: json!({}),
                completed: completed_tx,
                release: release_rx,
            }),
        )
        .await;

        let completed = futures_lite::future::race(
            async { Some(completed_rx.recv_async().await.unwrap()) },
            async {
                smol::Timer::after(SETTLE_TIMEOUT).await;
                None
            },
        )
        .await
        .expect("nested child B starved behind parent A's turn permit");
        assert_eq!(completed, Ok(NESTED_RESULT.into()));
        assert!(
            parent_ticket.peek().is_none(),
            "parent A turn ended before inspection"
        );
        assert_eq!(parent.snapshot().unwrap().depth, 1);
        let nested_id = closed_child(&events, parent.id()).await;
        wait_for_runner_finished(&manager, nested_id).await;
        assert!(
            manager
                .snapshot()
                .iter()
                .all(|node| node.agent_id != nested_id)
        );

        release_tx.send(()).unwrap();
        assert!(matches!(
            parent_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

/// The parent spawns a child without waiting on it, then holds its second
/// request until the test has seen the child's delivery admitted. The child
/// holds its only request until the parent's second request is in flight.
struct AutoDeliveryProvider {
    parent_requests: flume::Sender<Vec<Message>>,
    parent_paused: flume::Sender<()>,
    parent_release: flume::Receiver<()>,
    child_started: flume::Sender<()>,
    child_release: flume::Receiver<()>,
}

impl Provider for AutoDeliveryProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if !messages
                .iter()
                .any(|message| has_text(message, AUTO_DELIVERY_PARENT_PROMPT))
            {
                self.child_started.send(()).unwrap();
                common::bounded(self.child_release.recv_async())
                    .await
                    .unwrap();
                return Ok(common::canned_reply(AUTO_DELIVERY_CHILD_REPLY));
            }
            self.parent_requests.send(messages.to_vec()).unwrap();
            let responses = messages
                .iter()
                .filter(|message| matches!(message.role, Role::Assistant))
                .count();
            match responses {
                0 => Ok(common::canned_tool_use(
                    TASK_SPAWN_TOOL,
                    json!({ "description": "child", "prompt": AUTO_DELIVERY_CHILD_PROMPT }),
                )),
                1 => {
                    self.parent_paused.send(()).unwrap();
                    common::bounded(self.parent_release.recv_async())
                        .await
                        .unwrap();
                    Ok(common::canned_tool_use(AUTO_DELIVERY_PROBE_TOOL, json!({})))
                }
                2 => Ok(common::canned_reply(AUTO_DELIVERY_PARENT_REPLY)),
                _ => Err(AgentError::Config {
                    message: UNEXPECTED_PARENT_REQUEST.into(),
                }),
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn has_text(message: &Message, needle: &str) -> bool {
    message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::Text { text } if text.contains(needle)))
}

fn has_tool_result(message: &Message) -> bool {
    message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
}

fn only_child(manager: &AgentManagerHandle, parent_id: AgentId) -> AgentId {
    let children: Vec<_> = manager
        .snapshot()
        .into_iter()
        .filter(|node| node.parent_id == Some(parent_id))
        .map(|node| node.agent_id)
        .collect();
    assert_eq!(children.len(), 1, "children of {parent_id}: {children:?}");
    children[0]
}

#[test]
fn nested_lua_parent_receives_completions_mid_turn() {
    smol::block_on(async {
        let (registry, _host) = load_auto_delivery_host();
        let (parent_requests_tx, parent_requests) = flume::unbounded();
        let (parent_paused_tx, parent_paused) = flume::unbounded();
        let (parent_release, parent_release_rx) = flume::bounded(1);
        let (child_started_tx, child_started) = flume::unbounded();
        let (child_release, child_release_rx) = flume::bounded(1);
        let (mut context, _events, _cancel) =
            common::ctx_with_provider(Arc::new(AutoDeliveryProvider {
                parent_requests: parent_requests_tx,
                parent_paused: parent_paused_tx,
                parent_release: parent_release_rx,
                child_started: child_started_tx,
                child_release: child_release_rx,
            }));
        context.registry = Arc::clone(&registry);
        context.turn_bindings = Arc::new(TurnToolBindings::capture(
            &registry,
            &context.local_tools,
            None,
        ));
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed) = flume::bounded(1);
        let (driver_release, driver_release_rx) = flume::bounded(1);
        let (driver, driver_ticket) = spawn_managed_parent(
            &manager,
            parent_policy(&context),
            Box::new(NestedTaskBackend {
                registry,
                context,
                tool_name: AUTO_DELIVERY_TOOL_NAME,
                input: json!({}),
                completed: completed_tx,
                release: driver_release_rx,
            }),
        )
        .await;

        common::bounded(parent_paused.recv_async()).await.unwrap();
        common::bounded(child_started.recv_async()).await.unwrap();
        let parent_id = only_child(&manager, driver.id());
        let child_id = only_child(&manager, parent_id);
        let parent_events = manager.actor(parent_id).unwrap().subscribe_events();

        child_release.send(()).unwrap();
        let mut observed = Vec::new();
        let child_turn = common::bounded(async {
            loop {
                let event = parent_events.recv_async().await.unwrap();
                if let ActorEvent::Delivery(delivery) = &event
                    && matches!(delivery.status, DeliveryStatus::Accepted)
                {
                    break delivery.delivery.result.outcome.turn_id();
                }
                observed.push(event);
            }
        })
        .await;

        parent_release.send(()).unwrap();
        let parent_result = common::bounded(async {
            loop {
                match parent_events.recv_async().await.unwrap() {
                    ActorEvent::End(result) => break result,
                    event => observed.push(event),
                }
            }
        })
        .await;
        let requests: Vec<_> = parent_requests.try_iter().collect();
        let deliveries: Vec<_> = requests
            .iter()
            .map(|messages| {
                messages
                    .iter()
                    .filter(|message| has_text(message, DELIVERY_HEADER))
                    .count()
            })
            .collect();
        assert_eq!(deliveries, [0, 0, 1]);
        let [.., before, observation] = requests[2].as_slice() else {
            panic!("third parent request is too short")
        };
        assert!(
            has_tool_result(before),
            "delivery must follow the tool result"
        );
        assert!(observation.is_observation());
        assert!(has_text(observation, AUTO_DELIVERY_CHILD_REPLY));
        assert_eq!(
            observation.observation_source,
            Some(ObservationSource::ChildResult {
                agent_id: child_id.to_string(),
                turn_id: child_turn.to_string(),
            })
        );
        assert!(matches!(
            parent_result.outcome,
            TurnOutcome::Completed { .. }
        ));
        assert!(
            observed
                .iter()
                .all(|event| !matches!(event, ActorEvent::Start { .. })),
            "child result must not start another parent turn"
        );
        let executed: Vec<_> = observed
            .iter()
            .filter_map(|event| match event {
                ActorEvent::Delivery(delivery) => Some((
                    delivery.delivery.result.outcome.turn_id(),
                    match delivery.status {
                        DeliveryStatus::Executed { parent_turn_id } => Some(parent_turn_id),
                        _ => None,
                    },
                )),
                _ => None,
            })
            .collect();
        assert_eq!(
            executed,
            [(child_turn, Some(parent_result.outcome.turn_id()))]
        );
        assert_eq!(parent_result.text, AUTO_DELIVERY_PARENT_REPLY);

        let completed = common::bounded(completed.recv_async()).await.unwrap();
        assert_eq!(completed, Ok(AUTO_DELIVERY_PARENT_REPLY.into()));
        driver_release.send(()).unwrap();
        assert!(matches!(
            driver_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

enum ParentStep {
    Spawn,
    Send(&'static str),
    Wait,
    Reply(&'static str),
}

/// Plays `script` for the model-driven parent, one step per prior assistant
/// message, pausing before step `pause_at`. Child requests whose last message
/// is one of `GATED_CHILD_PROMPTS` are held until the test releases them.
struct ScriptedTaskProvider {
    script: Vec<ParentStep>,
    pause_at: Option<usize>,
    parent_requests: flume::Sender<Vec<Message>>,
    parent_paused: flume::Sender<()>,
    parent_release: flume::Receiver<()>,
    child_requests: flume::Sender<Vec<Message>>,
    child_started: flume::Sender<()>,
    child_release: flume::Receiver<()>,
}

struct ScriptHandles {
    parent_requests: flume::Receiver<Vec<Message>>,
    parent_paused: flume::Receiver<()>,
    parent_release: flume::Sender<()>,
    child_requests: flume::Receiver<Vec<Message>>,
    child_started: flume::Receiver<()>,
    child_release: flume::Sender<()>,
}

fn scripted_provider(
    script: Vec<ParentStep>,
    pause_at: Option<usize>,
) -> (ScriptedTaskProvider, ScriptHandles) {
    let (parent_requests_tx, parent_requests) = flume::unbounded();
    let (parent_paused_tx, parent_paused) = flume::unbounded();
    let (parent_release, parent_release_rx) = flume::unbounded();
    let (child_requests_tx, child_requests) = flume::unbounded();
    let (child_started_tx, child_started) = flume::unbounded();
    let (child_release, child_release_rx) = flume::unbounded();
    (
        ScriptedTaskProvider {
            script,
            pause_at,
            parent_requests: parent_requests_tx,
            parent_paused: parent_paused_tx,
            parent_release: parent_release_rx,
            child_requests: child_requests_tx,
            child_started: child_started_tx,
            child_release: child_release_rx,
        },
        ScriptHandles {
            parent_requests,
            parent_paused,
            parent_release,
            child_requests,
            child_started,
            child_release,
        },
    )
}

impl ScriptedTaskProvider {
    async fn child_reply(&self, messages: &[Message]) -> StreamResponse {
        self.child_requests.send(messages.to_vec()).unwrap();
        if messages.last().is_some_and(|last| {
            GATED_CHILD_PROMPTS
                .iter()
                .any(|prompt| has_text(last, prompt))
        }) {
            self.child_started.send(()).unwrap();
            common::bounded(self.child_release.recv_async())
                .await
                .unwrap();
        }
        common::canned_reply(AUTO_DELIVERY_CHILD_REPLY)
    }
}

impl Provider for ScriptedTaskProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        _: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if !messages
                .iter()
                .any(|message| has_text(message, AUTO_DELIVERY_PARENT_PROMPT))
            {
                return Ok(self.child_reply(messages).await);
            }
            self.parent_requests.send(messages.to_vec()).unwrap();
            let step = messages
                .iter()
                .filter(|message| matches!(message.role, Role::Assistant))
                .count();
            if self.pause_at == Some(step) {
                self.parent_paused.send(()).unwrap();
                common::bounded(self.parent_release.recv_async())
                    .await
                    .unwrap();
            }
            let spawned = || tool_results(messages).remove(0);
            match self.script.get(step) {
                Some(ParentStep::Spawn) => Ok(common::canned_tool_use(
                    TASK_SPAWN_TOOL,
                    json!({ "description": "child", "prompt": AUTO_DELIVERY_CHILD_PROMPT }),
                )),
                Some(ParentStep::Send(message)) => Ok(common::canned_tool_use(
                    TASK_SEND_TOOL,
                    json!({ "task_id": spawned()["task_id"], "message": message }),
                )),
                Some(ParentStep::Wait) => {
                    let spawned = spawned();
                    Ok(common::canned_tool_use(
                        TASK_WAIT_TOOL,
                        json!({ "task_id": spawned["task_id"], "turn_id": spawned["turn_id"] }),
                    ))
                }
                Some(ParentStep::Reply(text)) => Ok(common::canned_reply(text)),
                None => Err(AgentError::Config {
                    message: UNEXPECTED_PARENT_REQUEST.into(),
                }),
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn tool_results(messages: &[Message]) -> Vec<Value> {
    messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult { content, .. } => {
                Some(serde_json::from_str(content).unwrap())
            }
            _ => None,
        })
        .collect()
}

fn delivery_counts(requests: &[Vec<Message>]) -> Vec<usize> {
    requests
        .iter()
        .map(|messages| {
            messages
                .iter()
                .filter(|message| has_text(message, DELIVERY_HEADER))
                .count()
        })
        .collect()
}

fn executed_parent_turns(events: &[ActorEvent]) -> Vec<TurnId> {
    events
        .iter()
        .filter_map(|event| match event {
            ActorEvent::Delivery(delivery) => match delivery.status {
                DeliveryStatus::Executed { parent_turn_id } => Some(parent_turn_id),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn turn_starts(events: &[ActorEvent]) -> usize {
    events
        .iter()
        .filter(|event| matches!(event, ActorEvent::Start { .. }))
        .count()
}

fn turn_ends(events: &[ActorEvent]) -> Vec<TurnId> {
    events
        .iter()
        .filter_map(|event| match event {
            ActorEvent::End(result) => Some(result.outcome.turn_id()),
            _ => None,
        })
        .collect()
}

async fn next_end(
    events: &flume::Receiver<ActorEvent>,
    observed: &mut Vec<ActorEvent>,
) -> TurnResult {
    common::bounded(async {
        loop {
            match events.recv_async().await.unwrap() {
                ActorEvent::End(result) => break result,
                event => observed.push(event),
            }
        }
    })
    .await
}

/// A driver turn whose Lua tool spawns the model-driven parent and waits on it.
struct AutoDeliveryRun {
    _host: PluginHost,
    manager: AgentManagerHandle,
    driver: AgentRef,
    driver_ticket: TurnTicket,
    completed: flume::Receiver<Result<String, String>>,
    driver_release: flume::Sender<()>,
}

impl AutoDeliveryRun {
    async fn start<P: Provider + 'static>(provider: Arc<P>, limits: AgentLimits) -> Self {
        let (registry, host) = load_auto_delivery_host();
        let (mut context, _events, _cancel) = common::ctx_with_provider(provider);
        context.registry = Arc::clone(&registry);
        context.turn_bindings = Arc::new(TurnToolBindings::capture(
            &registry,
            &context.local_tools,
            None,
        ));
        let manager = AgentManagerHandle::new(limits).unwrap();
        let (completed_tx, completed) = flume::bounded(1);
        let (driver_release, driver_release_rx) = flume::bounded(1);
        let (driver, driver_ticket) = spawn_managed_parent(
            &manager,
            parent_policy(&context),
            Box::new(NestedTaskBackend {
                registry,
                context,
                tool_name: AUTO_DELIVERY_TOOL_NAME,
                input: json!({}),
                completed: completed_tx,
                release: driver_release_rx,
            }),
        )
        .await;
        Self {
            _host: host,
            manager,
            driver,
            driver_ticket,
            completed,
            driver_release,
        }
    }

    async fn finish(self, expected: &str) {
        let completed = common::bounded(self.completed.recv_async()).await.unwrap();
        assert_eq!(completed, Ok(expected.into()));
        self.driver_release.send(()).unwrap();
        assert!(matches!(
            self.driver_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let report = self.manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    }
}

#[test]
fn subagent_joins_unwaited_child_before_reporting() {
    smol::block_on(async {
        let (provider, handles) = scripted_provider(
            vec![
                ParentStep::Spawn,
                ParentStep::Reply(AUTO_DELIVERY_INTERIM_REPLY),
                ParentStep::Reply(AUTO_DELIVERY_PARENT_REPLY),
            ],
            None,
        );
        let run = AutoDeliveryRun::start(
            Arc::new(provider),
            AgentLimits {
                max_concurrent_agent_turns: 1,
                ..AgentLimits::default()
            },
        )
        .await;

        common::bounded(handles.child_started.recv_async())
            .await
            .unwrap();
        let parent_id = only_child(&run.manager, run.driver.id());
        let child_id = only_child(&run.manager, parent_id);
        let parent = run.manager.actor(parent_id).unwrap();
        let parent_events = parent.subscribe_events();
        assert!(
            parent.latest_ticket().unwrap().peek().is_none(),
            "parent turn settled while its child was still running"
        );
        assert!(
            run.completed.is_empty(),
            "driver saw the parent's report before its child finished"
        );

        handles.child_release.send(()).unwrap();
        let mut observed = Vec::new();
        let parent_result = next_end(&parent_events, &mut observed).await;
        let requests: Vec<_> = handles.parent_requests.try_iter().collect();
        assert_eq!(delivery_counts(&requests), [0, 0, 1]);
        let observation = requests[2].last().unwrap();
        assert!(observation.is_observation());
        assert!(has_text(observation, AUTO_DELIVERY_CHILD_REPLY));
        assert!(matches!(
            &observation.observation_source,
            Some(ObservationSource::ChildResult { agent_id, .. }) if *agent_id == child_id.to_string()
        ));
        assert_eq!(turn_starts(&observed), 0);
        assert_eq!(
            executed_parent_turns(&observed),
            [parent_result.outcome.turn_id()]
        );
        assert!(matches!(
            parent_result.outcome,
            TurnOutcome::Completed { .. }
        ));
        assert_eq!(parent_result.text, AUTO_DELIVERY_PARENT_REPLY);
        run.finish(AUTO_DELIVERY_PARENT_REPLY).await;
    });
}

#[test_case::test_case(false ; "busy_updates_only")]
#[test_case::test_case(true ; "late_idle_update")]
fn incident_coordination_updates_do_not_trigger_post_answer_turns(late_update: bool) {
    smol::block_on(async {
        let wait_step = UPDATE_COUNT + 1;
        let mut script = vec![ParentStep::Spawn];
        script.extend(COORDINATION_UPDATES.map(ParentStep::Send));
        script.push(ParentStep::Wait);
        if late_update {
            script.extend([
                ParentStep::Send(LATE_UPDATE),
                ParentStep::Reply(AUTO_DELIVERY_INTERIM_REPLY),
            ]);
        }
        script.push(ParentStep::Reply(AUTO_DELIVERY_PARENT_REPLY));
        let request_count = script.len();
        let (provider, handles) = scripted_provider(script, Some(1));
        let run = AutoDeliveryRun::start(Arc::new(provider), AgentLimits::default()).await;

        common::bounded(handles.parent_paused.recv_async())
            .await
            .unwrap();
        common::bounded(handles.child_started.recv_async())
            .await
            .unwrap();
        let parent_id = only_child(&run.manager, run.driver.id());
        let worker_id = only_child(&run.manager, parent_id);
        let parent_events = run.manager.actor(parent_id).unwrap().subscribe_events();
        let worker_events = run.manager.actor(worker_id).unwrap().subscribe_events();

        handles.parent_release.send(()).unwrap();
        let mut requests = Vec::new();
        let mut await_request = async |step: usize| {
            while requests.len() <= step {
                requests.push(
                    common::bounded(handles.parent_requests.recv_async())
                        .await
                        .unwrap(),
                );
            }
        };
        await_request(wait_step).await;
        handles.child_release.send(()).unwrap();
        if late_update {
            common::bounded(handles.child_started.recv_async())
                .await
                .unwrap();
            await_request(wait_step + 2).await;
            handles.child_release.send(()).unwrap();
        }

        let mut observed = Vec::new();
        let parent_result = next_end(&parent_events, &mut observed).await;
        requests.extend(handles.parent_requests.try_iter());
        assert_eq!(requests.len(), request_count);
        let results = tool_results(requests.last().unwrap());
        let spawn_turn = &results[0]["turn_id"];
        for sent in &results[1..=UPDATE_COUNT] {
            assert_eq!(
                sent["turn_id"], *spawn_turn,
                "update did not join the busy turn"
            );
        }
        assert_eq!(results[wait_step]["status"], "done");
        assert_eq!(results[wait_step]["result"]["turn_id"], *spawn_turn);

        let worker_requests: Vec<_> = handles.child_requests.try_iter().collect();
        assert_eq!(worker_requests.len(), if late_update { 3 } else { 2 });
        assert!(COORDINATION_UPDATES.iter().all(|update| {
            worker_requests[1]
                .iter()
                .any(|message| has_text(message, update))
        }));
        let worker_observed: Vec<_> = worker_events.try_iter().collect();
        let worker_turns = turn_ends(&worker_observed);
        assert_eq!(worker_turns[0].to_string(), spawn_turn.as_str().unwrap());

        let mut expected_deliveries = vec![0; request_count];
        if late_update {
            assert_ne!(results[wait_step + 1]["turn_id"], *spawn_turn);
            assert_eq!(worker_turns.len(), 2);
            assert_eq!(turn_starts(&worker_observed), 1);
            assert_eq!(
                executed_parent_turns(&observed),
                [parent_result.outcome.turn_id()]
            );
            expected_deliveries[request_count - 1] = 1;
        } else {
            assert_eq!(worker_turns.len(), 1);
            assert_eq!(turn_starts(&worker_observed), 0);
            assert!(executed_parent_turns(&observed).is_empty());
        }
        assert_eq!(delivery_counts(&requests), expected_deliveries);
        assert_eq!(turn_starts(&observed), 0);
        assert!(matches!(
            parent_result.outcome,
            TurnOutcome::Completed { .. }
        ));
        assert_eq!(parent_result.text, AUTO_DELIVERY_PARENT_REPLY);
        run.finish(AUTO_DELIVERY_PARENT_REPLY).await;
    });
}

#[test]
fn closing_managed_subtree_closes_retained_nested_session_adapter() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (context, events, _cancel) =
            common::ctx_with_replies(vec![common::canned_reply(NESTED_RESULT)]);
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let (parent, _parent_ticket) = spawn_managed_parent(
            &manager,
            parent_policy(&context),
            Box::new(NestedTaskBackend {
                registry: Arc::clone(&registry),
                context: context.clone(),
                tool_name: RETAIN_NESTED_TOOL_NAME,
                input: json!({}),
                completed: completed_tx,
                release: release_rx,
            }),
        )
        .await;

        assert_eq!(
            completed_rx.recv_async().await.unwrap(),
            Ok(NESTED_RESULT.into())
        );
        let descendant_id = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(parent.id()))
            .expect("retained nested managed session")
            .agent_id;
        let descendant = manager.lookup(descendant_id).unwrap();
        manager.close_subtree(parent.id()).unwrap();

        let closed = futures_lite::future::race(
            async {
                loop {
                    let envelope = events.recv_async().await.unwrap();
                    if matches!(envelope.event, AgentEvent::SubagentClosed)
                        && envelope
                            .subagent
                            .as_ref()
                            .is_some_and(|info| info.agent_id == descendant_id)
                    {
                        break true;
                    }
                }
            },
            async {
                smol::Timer::after(SETTLE_TIMEOUT).await;
                false
            },
        )
        .await;
        assert!(closed, "descendant UI adapter did not permanently close");

        let retry = registry
            .get(RETRY_NESTED_TOOL_NAME)
            .unwrap()
            .tool
            .parse(&json!({}))
            .unwrap()
            .execute(&context)
            .await;
        assert!(retry.output.is_ok(), "retained descendant accepted input");
        wait_for_runner_finished(&manager, descendant_id).await;
        assert_eq!(
            descendant.snapshot().unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        assert!(
            manager
                .snapshot()
                .iter()
                .all(|node| node.agent_id != descendant_id)
        );

        release_tx.send(()).unwrap();
        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_join_worker_prompts_existing_child_at_capacity_one() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (context, events, _cancel) =
            common::ctx_with_replies(vec![common::canned_reply(SILENT_REPLY)]);
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        })
        .unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let (parent, parent_ticket) = spawn_managed_parent(
            &manager,
            parent_policy(&context),
            Box::new(NestedTaskBackend {
                registry,
                context,
                tool_name: JOIN_TOOL_NAME,
                input: json!({}),
                completed: completed_tx,
                release: release_rx,
            }),
        )
        .await;

        let completed = futures_lite::future::race(
            async { Some(completed_rx.recv_async().await.unwrap()) },
            async {
                smol::Timer::after(SETTLE_TIMEOUT).await;
                None
            },
        )
        .await
        .expect("joined child starved behind its parent's turn permit");
        assert_eq!(completed, Ok(SILENT_REPLY.into()));
        assert!(parent_ticket.peek().is_none());
        let child_id = closed_child(&events, parent.id()).await;
        wait_for_runner_finished(&manager, child_id).await;
        assert!(
            manager
                .snapshot()
                .iter()
                .all(|node| node.agent_id != child_id)
        );

        release_tx.send(()).unwrap();
        assert!(matches!(
            parent_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_async_worker_outliving_turn_loses_stale_task_authority() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source(
            "managed-session",
            &format!(
                "{TASK_PRELUDE}\n{TASK_PLUGIN_SRC}\n{}",
                session_plugin_source()
            ),
        )
        .unwrap();
        let (mut context, _events, _cancel) = common::ctx_with_canned_provider();
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        })
        .unwrap();
        let (first_tx, first_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(parent_policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaToolBackend {
                    registry: Arc::clone(&registry),
                    context: context.clone(),
                    completed: first_tx,
                    tool_name: RETAIN_ASYNC_TOOL_NAME,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let first_ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();
        assert_eq!(first_rx.recv_async().await.unwrap(), Ok(()));
        assert!(matches!(
            first_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let child_id = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .unwrap()
            .agent_id;
        let child_before = futures_lite::future::race(
            async {
                loop {
                    let child = manager.node(child_id).unwrap();
                    if child.actor.as_ref().is_some_and(|actor| {
                        actor.latest.is_some() && actor.active_turn.is_none() && actor.queued == 0
                    }) {
                        break Some(child);
                    }
                    smol::future::yield_now().await;
                }
            },
            async {
                smol::Timer::after(SETTLE_TIMEOUT).await;
                None
            },
        )
        .await
        .expect("managed task child did not settle");

        context.managed_turn = None;
        let invocation = registry
            .get(RELEASE_ASYNC_TOOL_NAME)
            .unwrap()
            .tool
            .parse(&json!({}))
            .unwrap();
        let completed = futures_lite::future::race(
            async { Some(invocation.execute(&context).await.output.map(|_| ())) },
            async {
                smol::Timer::after(SETTLE_TIMEOUT).await;
                None
            },
        )
        .await
        .expect("stale async worker did not finish task authorization checks");
        assert_eq!(completed, Ok(()));

        let child_after = manager.node(child_id).unwrap();
        assert_eq!(child_after.graph_lifecycle, child_before.graph_lifecycle);
        let before_actor = child_before.actor.unwrap();
        let after_actor = child_after.actor.unwrap();
        assert_eq!(after_actor.lifecycle, before_actor.lifecycle);
        assert_eq!(after_actor.status, before_actor.status);
        assert_eq!(after_actor.active_turn, before_actor.active_turn);
        assert_eq!(after_actor.queued, before_actor.queued);
        assert_eq!(after_actor.latest, before_actor.latest);
        assert_eq!(after_actor.cumulative_usage, before_actor.cumulative_usage);

        let second_ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();
        assert_eq!(first_rx.recv_async().await.unwrap(), Ok(()));
        assert!(matches!(
            second_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));

        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_scope_rejects_foreign_unmanaged_ctx_before_side_effects() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (mut unmanaged_context, unmanaged_events, _cancel) = common::ctx_with_canned_provider();
        let (live_tx, live_rx) = flume::unbounded();
        unmanaged_context.live_sink = Some(live_tx);
        let capture = registry
            .get(CAPTURE_UNMANAGED_CTX_TOOL_NAME)
            .unwrap()
            .tool
            .parse(&json!({}))
            .unwrap();
        assert!(capture.execute(&unmanaged_context).await.output.is_ok());

        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(
                Some(parent_policy(&unmanaged_context)),
                Vec::new(),
                None,
                |_| {
                    Ok::<_, String>(Box::new(LuaToolBackend {
                        registry,
                        context: unmanaged_context,
                        completed: completed_tx,
                        tool_name: REJECT_FOREIGN_CTX_TOOL_NAME,
                    }) as Box<dyn ActorBackend>)
                },
            )
            .unwrap();
        let before = manager.node(root.id()).unwrap();
        assert_eq!(manager.snapshot().len(), 1);
        assert!(before.children.is_empty());
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();

        assert_eq!(
            completed_rx.recv_async().await.unwrap(),
            Ok(()),
            "{INVOCATION_EXPIRED}"
        );
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let after = manager.node(root.id()).unwrap();
        assert_eq!(manager.snapshot().len(), 1);
        assert_eq!(after.agent_id, before.agent_id);
        assert_eq!(after.parent_id, before.parent_id);
        assert_eq!(after.root_id, before.root_id);
        assert_eq!(after.depth, before.depth);
        assert_eq!(after.children, before.children);
        assert_eq!(after.graph_lifecycle, before.graph_lifecycle);
        assert_eq!(after.metadata, before.metadata);
        assert!(unmanaged_events.is_empty());
        assert!(live_rx.is_empty());

        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_session_uses_root_authority_and_closes_its_node() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let _host = PluginHost::new(Arc::clone(&registry)).unwrap();
        _host
            .load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (context, events, _cancel) = common::ctx_with_canned_provider();
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(parent_policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaToolBackend {
                    registry,
                    context,
                    completed: completed_tx,
                    tool_name: TOOL_NAME,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();

        assert_eq!(
            completed_rx.recv_async().await.unwrap(),
            Ok(()),
            "managed Lua tool failed"
        );
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let envelope = events
            .try_iter()
            .find(|envelope| matches!(envelope.event, AgentEvent::SubagentHistory { .. }))
            .expect("managed child history envelope");
        let child = envelope.subagent.unwrap();
        assert_eq!(child.parent_agent_id, Some(root.id()));
        assert!(child.parent_is_root);
        wait_for_runner_finished(&manager, child.agent_id).await;
        let root_node = root.snapshot().unwrap();
        assert_eq!(root_node.graph_lifecycle, GraphLifecycle::Live);
        assert!(root_node.children.is_empty());
        assert_eq!(manager.snapshot().len(), 1);

        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn silent_managed_session_returns_result_without_parent_visibility() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (context, events, _cancel) =
            common::ctx_with_replies(vec![common::canned_reply(SILENT_REPLY)]);
        assert!(events.is_empty(), "parent event stream must start empty");
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(parent_policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaToolBackend {
                    registry,
                    context,
                    completed: completed_tx,
                    tool_name: SILENT_TOOL_NAME,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();

        assert_eq!(completed_rx.recv_async().await.unwrap(), Ok(()));
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let reclaimed = futures_lite::future::race(
            async {
                loop {
                    let children = root.snapshot().unwrap().children;
                    if children.is_empty() {
                        break true;
                    }
                    for child_id in children {
                        wait_for_runner_finished(&manager, child_id).await;
                    }
                    smol::future::yield_now().await;
                }
            },
            async {
                smol::Timer::after(SETTLE_TIMEOUT).await;
                false
            },
        )
        .await;
        assert!(reclaimed, "silent child was not reclaimed");
        assert_eq!(manager.snapshot().len(), 1);
        assert!(
            events.is_empty(),
            "silent child emitted parent envelopes: {:?}",
            events.try_iter().collect::<Vec<_>>()
        );

        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn retained_managed_prompt_outside_turn_is_rejected_without_mutation() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (requests_tx, requests_rx) = flume::unbounded();
        let provider = Arc::new(RecordingProvider {
            requests: requests_tx,
        });
        let (context, _events, _cancel) = common::ctx_with_provider(provider);
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(parent_policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaToolBackend {
                    registry: Arc::clone(&registry),
                    context: context.clone(),
                    completed: completed_tx,
                    tool_name: RETAIN_TOOL_NAME,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();

        assert_eq!(completed_rx.recv_async().await.unwrap(), Ok(()));
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let child_id = root.snapshot().unwrap().children[0];
        let child = manager.lookup(child_id).unwrap();
        let before = child.snapshot().unwrap();
        let invocation = registry
            .get(OUTSIDE_TIMEOUT_TOOL_NAME)
            .unwrap()
            .tool
            .parse(&json!({}))
            .unwrap();
        assert!(
            invocation.execute(&context).await.output.is_ok(),
            "{MANAGED_AUTHORITY_INACTIVE}"
        );
        let after = child.snapshot().unwrap();
        assert_eq!(after.graph_lifecycle, before.graph_lifecycle);
        let before_actor = before.actor.unwrap();
        let after_actor = after.actor.unwrap();
        assert_eq!(after_actor.lifecycle, before_actor.lifecycle);
        assert_eq!(after_actor.status, before_actor.status);
        assert_eq!(after_actor.active_turn, before_actor.active_turn);
        assert_eq!(after_actor.queued, before_actor.queued);
        assert_eq!(after_actor.latest, before_actor.latest);
        assert_eq!(after_actor.cumulative_usage, before_actor.cumulative_usage);
        assert!(
            requests_rx.is_empty(),
            "unauthorized prompt entered provider"
        );

        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_prompt_timeout_returns_pair_closes_child_and_resumes_parent() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let _host = PluginHost::new(Arc::clone(&registry)).unwrap();
        _host
            .load_source("managed-session", &session_plugin_source())
            .unwrap();
        let (dropped_tx, dropped_rx) = flume::bounded(1);
        let provider = Arc::new(PendingProvider {
            dropped: dropped_tx,
        });
        let (mut context, events, _cancel) = common::ctx_with_provider(provider);
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        })
        .unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root_with_config(Some(parent_policy(&context)), Vec::new(), None, |_| {
                Ok::<_, String>(Box::new(LuaToolBackend {
                    registry,
                    context: context.clone(),
                    completed: completed_tx,
                    tool_name: TIMEOUT_TOOL_NAME,
                }) as Box<dyn ActorBackend>)
            })
            .unwrap();
        context.managed_turn = None;
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();

        assert_eq!(
            completed_rx.recv_async().await.unwrap(),
            Ok(()),
            "{TIMEOUT_ERROR}"
        );
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        dropped_rx
            .recv_async()
            .await
            .expect("timed-out managed provider request remained alive");
        let child_id = closed_child(&events, root.id()).await;
        wait_for_runner_finished(&manager, child_id).await;
        assert!(root.snapshot().unwrap().children.is_empty());
        assert_eq!(manager.snapshot().len(), 1);

        let report = manager.shutdown(SETTLE_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}
