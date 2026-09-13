use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use maki_agent::tools::{ToolAudience, ToolContext, ToolRegistry};
use maki_agent::{
    ActorBackend, AgentEvent, AgentInput, AgentLimits, AgentManagerHandle, AgentMetadata,
    AgentMode, AgentRef, BackendResult, ControlWork, DoneReason, GraphLifecycle, History,
    ToolOutput, TurnContext, TurnOutcome, TurnTicket, WorkKind,
};
use maki_lua::PluginHost;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, Message, Model, ModelInfo, ProviderEvent, RequestOptions, StreamResponse,
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
const TIMEOUT_ERROR: &str = "session prompt timed out after 1s";
const NESTED_RESULT: &str = "nested child result";
const MANAGED_NESTED_SPAWN_ERR: &str = "managed general subagents must use the blocking task tool";
const CORRELATION: &str = "managed-root";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

const TASK_PLUGIN_SRC: &str = include_str!("../../plugins/task/init.lua");
const TASK_PRELUDE: &str = r#"
test_task_handlers = {}
local real_register_tool = maki.api.register_tool
maki.api.register_tool = function(spec)
  test_task_handlers[spec.name] = spec.handler
  return real_register_tool(spec)
end

maki.api.mode.get = function() return "build" end

local real_session = maki.agent.session
maki.agent.session = function(ctx, opts)
  opts.inherit_provider = true
  return real_session(ctx, opts)
end

maki.agent.resolve_model = function()
  return { spec = "anthropic/claude-sonnet-4-20250514" }
end

maki.agent.system_prompt = function()
  return "sys"
end

maki.agent.tools = function()
  return nil
end
"#;

const PLUGIN_SRC: &str = r#"
local retained_session
local retained_task_id
local managed_authority_turn = 0
local release_async_worker
local finish_async_wait

maki.api.register_tool({
  name = "managed_session_join",
  description = "prompt an existing child from a joined worker",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(_, ctx)
    local session = assert(maki.agent.session(ctx, {
      name = "managed-joined-child",
      inherit_provider = true,
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
      })
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
    }))
    return "ok"
  end,
})

maki.api.register_tool({
  name = "managed_session_outside_timeout",
  description = "time out a retained managed child",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    local result, err = retained_session:prompt("wait", { timeout = 1 })
    if result ~= nil or err ~= "session prompt timed out after 1s" then
      return { llm_output = "unexpected retained timeout result", is_error = true }
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
    if result ~= nil or err ~= "session closed" then
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
            self.context.managed_turn = context.managed_turn;
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
            BackendResult::EnteredRun(TurnOutcome::Completed {
                agent_id: context.agent_id,
                turn_id: context.turn_id.unwrap(),
                usage: Default::default(),
                num_turns: 1,
                reason: DoneReason::EndTurn,
            })
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
            self.context.managed_turn = context.managed_turn;
            self.context.audience = ToolAudience::GENERAL_SUB;
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
            BackendResult::EnteredRun(TurnOutcome::Completed {
                agent_id: context.agent_id,
                turn_id: context.turn_id.unwrap(),
                usage: Default::default(),
                num_turns: 1,
                reason: DoneReason::EndTurn,
            })
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
            BackendResult::EnteredRun(TurnOutcome::Completed {
                agent_id: context.agent_id,
                turn_id: context.turn_id.unwrap(),
                usage: Default::default(),
                num_turns: 1,
                reason: DoneReason::EndTurn,
            })
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
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

fn task_input() -> Value {
    json!({
        "description": "nested-child",
        "prompt": "nested prompt",
        "subagent_type": "general",
    })
}

fn load_task_host() -> (Arc<ToolRegistry>, PluginHost) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(
        "managed-nested-task",
        &format!("{TASK_PRELUDE}\n{TASK_PLUGIN_SRC}"),
    )
    .unwrap();
    (registry, host)
}

async fn spawn_managed_parent(
    manager: &AgentManagerHandle,
    child_backend: Box<dyn ActorBackend>,
) -> (AgentRef, TurnTicket) {
    let (child_tx, child_rx) = flume::bounded(1);
    let root = manager
        .create_root(
            Vec::new(),
            None,
            Box::new(SpawnManagedChildBackend {
                child_backend: Some(child_backend),
                child: child_tx,
            }),
        )
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
    }
}

#[test]
fn managed_nested_blocking_task_completes_before_parent_turn_ends_at_capacity_one() {
    smol::block_on(async {
        let (registry, _host) = load_task_host();
        let (context, _events, _cancel) =
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
            Box::new(NestedTaskBackend {
                registry,
                context,
                tool_name: "task",
                input: task_input(),
                completed: completed_tx,
                release: release_rx,
            }),
        )
        .await;

        let completed = futures_lite::future::race(
            async { Some(completed_rx.recv_async().await.unwrap()) },
            async {
                smol::Timer::after(SHUTDOWN_TIMEOUT).await;
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
        let nested = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(parent.id()))
            .expect("managed nested child B");
        assert_eq!(nested.depth, 2);
        let nested_id = nested.agent_id;
        while !manager.runner_finished(nested_id).unwrap() {
            smol::future::yield_now().await;
        }
        assert_eq!(
            manager.node(nested_id).unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );

        release_tx.send(()).unwrap();
        assert!(matches!(
            parent_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn closing_managed_subtree_closes_retained_nested_session_adapter() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", PLUGIN_SRC).unwrap();
        let (context, events, _cancel) =
            common::ctx_with_replies(vec![common::canned_reply(NESTED_RESULT)]);
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let (parent, _parent_ticket) = spawn_managed_parent(
            &manager,
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
                smol::Timer::after(SHUTDOWN_TIMEOUT).await;
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
        while !manager.runner_finished(descendant_id).unwrap() {
            smol::future::yield_now().await;
        }
        assert_eq!(
            manager.node(descendant_id).unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );

        release_tx.send(()).unwrap();
        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_nested_task_spawn_rejects_before_creating_child() {
    smol::block_on(async {
        let (registry, _host) = load_task_host();
        let (context, _events, _cancel) = common::ctx_with_canned_provider();
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        })
        .unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let (release_tx, release_rx) = flume::bounded(1);
        let (_parent, parent_ticket) = spawn_managed_parent(
            &manager,
            Box::new(NestedTaskBackend {
                registry,
                context,
                tool_name: "task_spawn",
                input: task_input(),
                completed: completed_tx,
                release: release_rx,
            }),
        )
        .await;

        assert_eq!(
            completed_rx.recv_async().await.unwrap(),
            Err(MANAGED_NESTED_SPAWN_ERR.into())
        );
        assert!(
            parent_ticket.peek().is_none(),
            "parent A turn ended before inspection"
        );
        assert_eq!(
            manager.snapshot().len(),
            2,
            "nested child was created before rejection"
        );

        release_tx.send(()).unwrap();
        assert!(matches!(
            parent_ticket.wait().await,
            TurnOutcome::Failed { .. }
        ));
        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_join_worker_prompts_existing_child_at_capacity_one() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", PLUGIN_SRC).unwrap();
        let (context, _events, _cancel) =
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
                smol::Timer::after(SHUTDOWN_TIMEOUT).await;
                None
            },
        )
        .await
        .expect("joined child starved behind its parent's turn permit");
        assert_eq!(completed, Ok(SILENT_REPLY.into()));
        assert!(parent_ticket.peek().is_none());
        assert!(
            manager
                .snapshot()
                .iter()
                .any(|node| node.parent_id == Some(parent.id())),
            "managed joined child missing"
        );

        release_tx.send(()).unwrap();
        assert!(matches!(
            parent_ticket.wait().await,
            TurnOutcome::Completed { .. }
        ));
        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
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
            &format!("{TASK_PRELUDE}\n{TASK_PLUGIN_SRC}\n{PLUGIN_SRC}"),
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
            .create_root(
                Vec::new(),
                None,
                Box::new(LuaToolBackend {
                    registry: Arc::clone(&registry),
                    context: context.clone(),
                    completed: first_tx,
                    tool_name: RETAIN_ASYNC_TOOL_NAME,
                }),
            )
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
                    if child
                        .actor
                        .as_ref()
                        .is_some_and(|actor| actor.latest.is_some())
                    {
                        break Some(child);
                    }
                    smol::future::yield_now().await;
                }
            },
            async {
                smol::Timer::after(SHUTDOWN_TIMEOUT).await;
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
                smol::Timer::after(SHUTDOWN_TIMEOUT).await;
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

        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_session_uses_root_authority_and_closes_its_node() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let _host = PluginHost::new(Arc::clone(&registry)).unwrap();
        _host.load_source("managed-session", PLUGIN_SRC).unwrap();
        let (context, events, _cancel) = common::ctx_with_canned_provider();
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(LuaToolBackend {
                    registry,
                    context,
                    completed: completed_tx,
                    tool_name: TOOL_NAME,
                }),
            )
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
        let nodes = manager.snapshot();
        assert_eq!(nodes.len(), 2);
        let root_node = nodes
            .iter()
            .find(|node| node.agent_id == root.id())
            .unwrap();
        let child = nodes
            .iter()
            .find(|node| node.parent_id == Some(root.id()))
            .unwrap();
        assert_eq!(root_node.graph_lifecycle, GraphLifecycle::Live);
        assert_eq!(root_node.children, vec![child.agent_id]);
        assert_eq!(child.root_id, root.id());
        assert_eq!(child.depth, 1);
        let child_id = child.agent_id;
        while !manager.runner_finished(child_id).unwrap() {
            smol::future::yield_now().await;
        }
        assert_eq!(
            manager.node(child_id).unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        let envelope = events
            .try_iter()
            .find(|envelope| matches!(envelope.event, AgentEvent::SubagentHistory { .. }))
            .expect("managed child history envelope");
        assert_eq!(envelope.subagent.unwrap().agent_id, child.agent_id);

        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn silent_managed_session_returns_result_without_parent_visibility() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", PLUGIN_SRC).unwrap();
        let (context, events, _cancel) =
            common::ctx_with_replies(vec![common::canned_reply(SILENT_REPLY)]);
        assert!(events.is_empty(), "parent event stream must start empty");
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(LuaToolBackend {
                    registry,
                    context,
                    completed: completed_tx,
                    tool_name: SILENT_TOOL_NAME,
                }),
            )
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
            .expect("silent managed child")
            .agent_id;
        while !manager.runner_finished(child_id).unwrap() {
            smol::future::yield_now().await;
        }
        assert_eq!(
            manager.node(child_id).unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );
        assert!(
            events.is_empty(),
            "silent child emitted parent envelopes: {:?}",
            events.try_iter().collect::<Vec<_>>()
        );

        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn retained_managed_prompt_timeout_outside_invocation_closes_graph_node() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let host = PluginHost::new(Arc::clone(&registry)).unwrap();
        host.load_source("managed-session", PLUGIN_SRC).unwrap();
        let (dropped_tx, dropped_rx) = flume::bounded(1);
        let provider = Arc::new(PendingProvider {
            dropped: dropped_tx,
        });
        let (context, _events, _cancel) = common::ctx_with_provider(provider);
        let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(LuaToolBackend {
                    registry: Arc::clone(&registry),
                    context: context.clone(),
                    completed: completed_tx,
                    tool_name: RETAIN_TOOL_NAME,
                }),
            )
            .unwrap();
        let ticket = root
            .actor()
            .unwrap()
            .admit_turn(input(), None, CORRELATION.into())
            .unwrap();

        assert_eq!(completed_rx.recv_async().await.unwrap(), Ok(()));
        assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
        let invocation = registry
            .get(OUTSIDE_TIMEOUT_TOOL_NAME)
            .unwrap()
            .tool
            .parse(&json!({}))
            .unwrap();
        assert!(
            invocation.execute(&context).await.output.is_ok(),
            "{TIMEOUT_ERROR}"
        );
        dropped_rx
            .recv_async()
            .await
            .expect("timed-out retained provider request remained alive");
        let child_id = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .expect("retained managed child")
            .agent_id;
        while !manager.runner_finished(child_id).unwrap() {
            smol::future::yield_now().await;
        }
        assert_eq!(
            manager.node(child_id).unwrap().graph_lifecycle,
            GraphLifecycle::Closed
        );

        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}

#[test]
fn managed_prompt_timeout_returns_pair_closes_child_and_resumes_parent() {
    smol::block_on(async {
        let registry = Arc::new(ToolRegistry::new());
        let _host = PluginHost::new(Arc::clone(&registry)).unwrap();
        _host.load_source("managed-session", PLUGIN_SRC).unwrap();
        let (dropped_tx, dropped_rx) = flume::bounded(1);
        let provider = Arc::new(PendingProvider {
            dropped: dropped_tx,
        });
        let (mut context, _events, _cancel) = common::ctx_with_provider(provider);
        let manager = AgentManagerHandle::new(AgentLimits {
            max_concurrent_agent_turns: 1,
            ..AgentLimits::default()
        })
        .unwrap();
        let (completed_tx, completed_rx) = flume::bounded(1);
        let root = manager
            .create_root(
                Vec::new(),
                None,
                Box::new(LuaToolBackend {
                    registry,
                    context: context.clone(),
                    completed: completed_tx,
                    tool_name: TIMEOUT_TOOL_NAME,
                }),
            )
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
        let child = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .expect("managed timeout child");
        assert_eq!(child.graph_lifecycle, GraphLifecycle::Closed);

        let report = manager.shutdown(SHUTDOWN_TIMEOUT).await;
        assert!(report.timed_out.is_empty());
    });
}
