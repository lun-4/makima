//! Reproduction tests for async subagent lifecycle regressions.
//!
//! 1. `subagent_outlives_the_run_that_spawned_it`: a subagent must survive its
//!    parent run ending normally (the run's cancel must not close it).
//! 2. `subagent_completion_surfaces_reply_to_parent`: after a subagent finishes
//!    a run, its transcript must be surfaced to the parent so the UI can queue
//!    the reply back to the main agent (SubagentHistory must be emitted, not
//!    only on explicit close).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use maki_agent::tools::ToolContext;
use maki_agent::tools::ToolRegistry;
use maki_agent::{
    ActorBackend, AgentEvent, AgentInput, AgentLimits, AgentManagerHandle, AgentRef, BackendResult,
    ControlWork, History, TurnContext, WorkKind,
};
use maki_commands::CommandInvocation;
use maki_lua::orchestration::{OrchestrationServices, TrustedTarget};
use maki_lua::{EventHandle, PluginHost};
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role, StreamResponse,
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};

mod common;
use common::{
    ctx_with_canned_provider, ctx_with_provider, exec_tool, production_like_ctx, with_request_tools,
};

const PROVIDER_FAILURE: &str = "deterministic provider failure";
const RECOVERED_REPLY: &str = "recovered on the same session";
const FIRST_REPLY: &str = "READY";
const SECOND_REPLY: &str = "VIOLET";
const TURN_TIMEOUT: Duration = Duration::from_secs(2);

const PROBE_SRC: &str = r#"
local session_holder = { sess = nil }
local results = {}

local function register_probe(spec)
  maki.api.register_command({
    name = "/" .. spec.name,
    tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
      local input = assert(maki.json.decode(opts.args))
      results[input.request_id] = spec.handler(input, ctx)
    end,
  })
end

maki.api.register_tool({
  name = "probe_result",
  description = "read a completed probe command result",
  schema = { type = "object", properties = { request_id = { type = "string" } }, required = { "request_id" } },
  handler = function(input)
    local result = assert(results[input.request_id], "missing probe result")
    results[input.request_id] = nil
    return result
  end,
})

register_probe({
  name = "probe_spawn",
  handler = function(input, ctx)
    local sess, err = maki.agent.session(ctx, { name = "probe", inherit_provider = true })
    if not sess then
      return { llm_output = "spawn failed: " .. err, is_error = true }
    end
    session_holder.sess = sess
    return maki.json.encode({ task_id = sess:session_id() })
  end,
})

register_probe({
  name = "probe_status",
  handler = function()
    return maki.json.encode(session_holder.sess:status())
  end,
})

register_probe({
  name = "probe_send",
  handler = function(input)
    local _, err = session_holder.sess:send(input.message)
    if err then
      return { llm_output = "send failed: " .. err, is_error = true }
    end
    return maki.json.encode({ ok = true })
  end,
})

register_probe({
  name = "probe_prompt",
  handler = function(input)
    local opts = input.timeout and { timeout = input.timeout } or nil
    local result, err = session_holder.sess:prompt(input.message, opts)
    return maki.json.encode({ result = result, error = err })
  end,
})
"#;

struct ProbeServices(TrustedTarget);

impl OrchestrationServices for ProbeServices {
    fn resolve_target(
        &self,
        _: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String> {
        Ok(Some(self.0.clone()))
    }

    fn template(&self, _: &AgentRef) -> Result<ToolContext, String> {
        Ok(self.0.template.clone())
    }

    fn register_target(&self, _: TrustedTarget) -> Result<(), String> {
        Ok(())
    }
}

struct IdleRoot;

impl ActorBackend for IdleRoot {
    fn run_turn<'a>(
        &'a mut self,
        _: &'a mut History,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(std::future::pending())
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

fn load_probe_host(ctx: &ToolContext) -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("probe", PROBE_SRC).unwrap();
    let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
    let root = manager
        .create_root(Vec::new(), None, Box::new(IdleRoot))
        .unwrap();
    let mut template = with_request_tools(&reg, ctx);
    template.registry = Arc::clone(&reg);
    host.event_handle()
        .install_orchestration_services(Arc::new(ProbeServices(TrustedTarget {
            target: root,
            template,
        })));
    host.wait_for_worker_barrier_for_test();
    (reg, host)
}

fn exec_probe(
    reg: &ToolRegistry,
    handle: &EventHandle,
    ctx: &ToolContext,
    name: &str,
    mut input: Value,
) -> Result<Value, String> {
    static NEXT_REQUEST: AtomicUsize = AtomicUsize::new(0);
    let request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed).to_string();
    input["request_id"] = json!(request_id);
    handle
        .run_command_for_test(
            Arc::from("probe"),
            Arc::from(format!("/{name}")),
            input.to_string(),
            0,
        )
        .recv_timeout(TURN_TIMEOUT)
        .map_err(|error| format!("probe command did not resolve: {error}"))??;
    exec_tool(
        reg,
        ctx,
        "probe_result",
        json!({ "request_id": request_id }),
    )
}

struct FailOnceProvider {
    calls: AtomicUsize,
}

struct PendingProvider {
    dropped_tx: flume::Sender<()>,
}

struct DropSignal(flume::Sender<()>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct GatedProvider {
    replies: Mutex<Vec<&'static str>>,
    entered: flume::Sender<usize>,
    release: flume::Receiver<()>,
    requests: Mutex<Vec<Vec<String>>>,
}

impl Provider for GatedProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        messages: &'a [Message],
        _system: &'a str,
        _tools: &'a Value,
        _event_tx: &'a flume::Sender<ProviderEvent>,
        _opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let request = messages
                .iter()
                .filter_map(|message| {
                    message.content.iter().find_map(|block| {
                        if let maki_providers::ContentBlock::Text { text } = block {
                            Some(text.clone())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            let index = {
                let mut requests = self.requests.lock().unwrap();
                let index = requests.len();
                requests.push(request);
                index
            };
            self.entered.send(index).unwrap();
            self.release.recv_async().await.unwrap();
            let reply = self.replies.lock().unwrap().remove(0);
            Ok(common::canned_reply(reply))
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

struct CancelThenRecoverProvider {
    calls: AtomicUsize,
    entered: flume::Sender<usize>,
}

impl Provider for CancelThenRecoverProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        _messages: &'a [Message],
        _system: &'a str,
        _tools: &'a Value,
        event_tx: &'a flume::Sender<ProviderEvent>,
        _opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.send(call).unwrap();
            if call == 0 {
                event_tx
                    .send_async(ProviderEvent::ThinkingDelta {
                        text: String::new(),
                    })
                    .await
                    .unwrap();
                std::future::pending().await
            } else {
                Ok(common::canned_reply(RECOVERED_REPLY))
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

impl Provider for FailOnceProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        _messages: &'a [Message],
        _system: &'a str,
        _tools: &'a Value,
        _event_tx: &'a flume::Sender<ProviderEvent>,
        _opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(AgentError::Config {
                    message: PROVIDER_FAILURE.into(),
                })
            } else {
                Ok(common::canned_reply(RECOVERED_REPLY))
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

impl Provider for PendingProvider {
    fn stream_message<'a>(
        &'a self,
        _model: &'a Model,
        _messages: &'a [Message],
        _system: &'a str,
        _tools: &'a Value,
        _event_tx: &'a flume::Sender<ProviderEvent>,
        _opts: RequestOptions,
        _session_id: Option<&'a SessionRef>,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            let _signal = DropSignal(self.dropped_tx.clone());
            std::future::pending::<()>().await;
            unreachable!()
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

#[test]
fn async_sends_are_fifo_and_surface_each_history_snapshot() {
    let (entered_tx, entered_rx) = flume::bounded(2);
    let (release_tx, release_rx) = flume::unbounded();
    let provider = Arc::new(GatedProvider {
        replies: Mutex::new(vec![FIRST_REPLY, SECOND_REPLY]),
        entered: entered_tx,
        release: release_rx,
        requests: Mutex::new(Vec::new()),
    });
    let (ctx, parent_rx, _run_trigger) = ctx_with_provider(Arc::clone(&provider));
    let (reg, host) = load_probe_host(&ctx);
    let handle = host.event_handle();
    let spawn = exec_probe(&reg, &handle, &ctx, "probe_spawn", json!({})).unwrap();
    let first = {
        let reg = Arc::clone(&reg);
        let ctx = ctx.clone();
        let handle = handle.clone();
        std::thread::spawn(move || {
            exec_probe(
                &reg,
                &handle,
                &ctx,
                "probe_send",
                json!({"message": "first"}),
            )
        })
    };
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 0);
    let second = {
        let reg = Arc::clone(&reg);
        let ctx = ctx.clone();
        let handle = handle.clone();
        std::thread::spawn(move || {
            exec_probe(
                &reg,
                &handle,
                &ctx,
                "probe_send",
                json!({"message": "second"}),
            )
        })
    };
    release_tx.send(()).unwrap();
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 1);
    release_tx.send(()).unwrap();
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();

    let mut replies = Vec::new();
    let mut snapshots = Vec::new();
    while let Ok(envelope) = parent_rx.recv_timeout(TURN_TIMEOUT) {
        if let AgentEvent::SubagentHistory { messages, .. } = envelope.event {
            snapshots.push(messages.clone());
            if let Some(reply) = messages.iter().rev().find_map(|message| {
                message.content.iter().find_map(|block| match block {
                    maki_providers::ContentBlock::Text { text }
                        if matches!(message.role, Role::Assistant) =>
                    {
                        Some(text.clone())
                    }
                    _ => None,
                })
            }) {
                replies.push(reply);
            }
            if snapshots.len() == 2 {
                break;
            }
        }
    }
    assert!(!spawn["task_id"].as_str().unwrap().is_empty());
    assert_eq!(replies, vec![FIRST_REPLY, SECOND_REPLY]);
    assert!(snapshots[1].iter().any(|message| {
        matches!(message.role, Role::Assistant)
            && message.content.iter().any(|block| {
                matches!(block, maki_providers::ContentBlock::Text { text } if text == FIRST_REPLY)
            })
    }));
    assert_eq!(
        provider.requests.lock().unwrap()[1][..2],
        ["first".to_owned(), FIRST_REPLY.to_owned()]
    );
}

#[test]
fn child_cancellation_keeps_session_open_for_recovery() {
    let (entered_tx, entered_rx) = flume::bounded(1);
    let provider = Arc::new(CancelThenRecoverProvider {
        calls: AtomicUsize::new(0),
        entered: entered_tx,
    });
    let (ctx, parent_rx, _run_trigger) = ctx_with_provider(Arc::clone(&provider));
    let (reg, host) = load_probe_host(&ctx);
    let handle = host.event_handle();
    exec_probe(&reg, &handle, &ctx, "probe_spawn", json!({})).unwrap();
    let reg_for_prompt = Arc::clone(&reg);
    let ctx_for_prompt = ctx.clone();
    let handle_for_prompt = handle.clone();
    let prompt = std::thread::spawn(move || {
        exec_probe(
            &reg_for_prompt,
            &handle_for_prompt,
            &ctx_for_prompt,
            "probe_prompt",
            json!({"message": "cancel me"}),
        )
    });
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 0);
    let cancel = loop {
        let envelope = parent_rx.recv_timeout(TURN_TIMEOUT).unwrap();
        if let Some(cancel) = envelope.subagent.and_then(|info| info.cancel) {
            break cancel;
        }
    };
    cancel.cancel();
    exec_probe(
        &reg,
        &handle,
        &ctx,
        "probe_send",
        json!({"message": "recover"}),
    )
    .unwrap();

    let cancelled = prompt.join().unwrap().unwrap();
    assert!(
        cancelled["error"]
            .as_str()
            .is_some_and(|error| error.contains("cancel"))
    );
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 1);
    loop {
        let envelope = parent_rx.recv_timeout(TURN_TIMEOUT).unwrap();
        let AgentEvent::SubagentHistory { messages, .. } = envelope.event else {
            continue;
        };
        assert!(
            envelope.subagent.is_some(),
            "recovered history must remain stamped as child activity"
        );
        let recovered = messages.iter().rev().any(|message| {
            matches!(message.role, Role::Assistant)
                && message.content.iter().any(|block| {
                    matches!(block, maki_providers::ContentBlock::Text { text } if text == RECOVERED_REPLY)
                })
        });
        if recovered {
            break;
        }
    }
}

/// A subagent spawned during a run must survive that run ending normally.
/// `status()` should report it as `running`/`done`, never `closed`.
#[test]
fn subagent_outlives_the_run_that_spawned_it() {
    let (ctx, run_trigger) = production_like_ctx();
    let (reg, host) = load_probe_host(&ctx);
    let handle = host.event_handle();

    exec_probe(&reg, &handle, &ctx, "probe_spawn", json!({})).expect("spawn failed");

    let before = exec_probe(&reg, &handle, &ctx, "probe_status", json!({})).unwrap();
    eprintln!("status (before run end) -> {before}");
    assert_ne!(
        before["status"], "closed",
        "subagent must be alive after spawn"
    );

    run_trigger.cancel();

    let mut status = Value::Null;
    for _ in 0..20 {
        status = exec_probe(&reg, &handle, &ctx, "probe_status", json!({})).unwrap();
        eprintln!("status (after run end) -> {status}");
        if status["status"] == "done" || status["status"] == "closed" {
            break;
        }
    }
    assert_ne!(
        status["status"], "closed",
        "a spawned subagent must not be closed by its parent run ending normally: {status}"
    );
}

#[test]
fn failed_subagent_turn_resolves_and_same_session_recovers() {
    let provider = Arc::new(FailOnceProvider {
        calls: AtomicUsize::new(0),
    });
    let (ctx, parent_rx, _run_trigger) = ctx_with_provider(provider);
    let (reg, host) = load_probe_host(&ctx);
    let handle = host.event_handle();

    exec_probe(&reg, &handle, &ctx, "probe_spawn", json!({})).expect("spawn failed");

    let failed = exec_probe(
        &reg,
        &handle,
        &ctx,
        "probe_prompt",
        json!({ "message": "fail this turn" }),
    )
    .expect("failed turn did not resolve");
    assert_eq!(failed["error"], json!(PROVIDER_FAILURE));
    // `prompt` resolves its ticket inside `finalize_turn`, which runs before
    // the actor flips to idle, so a status read straight afterwards can still
    // see the turn running and carry no error. `status` is a polling API, so
    // wait for it to settle instead of racing it.
    let deadline = Instant::now() + TURN_TIMEOUT;
    let status = loop {
        let status = exec_probe(&reg, &handle, &ctx, "probe_status", json!({})).unwrap();
        if status["status"] == json!("done") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the failed turn's status never settled: {status}"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    };
    assert_eq!(status["error"], json!(PROVIDER_FAILURE));
    // The parent channel also carries subagent activity, so scan for the
    // history envelope rather than assuming it arrives first.
    loop {
        let envelope = parent_rx
            .recv_timeout(TURN_TIMEOUT)
            .expect("failure history event");
        if matches!(envelope.event, AgentEvent::SubagentHistory { .. }) {
            break;
        }
    }

    let recovered = exec_probe(
        &reg,
        &handle,
        &ctx,
        "probe_prompt",
        json!({ "message": "reuse the same session" }),
    )
    .expect("later turn did not resolve");
    assert!(recovered["error"].is_null(), "got: {recovered}");
    assert_eq!(recovered["result"]["text"], json!(RECOVERED_REPLY));
}

#[test]
fn prompt_timeout_cancels_in_flight_provider_request() {
    let (dropped_tx, dropped_rx) = flume::bounded(1);
    let provider = Arc::new(PendingProvider { dropped_tx });
    let (ctx, _parent_rx, _run_trigger) = ctx_with_provider(provider);
    let (reg, host) = load_probe_host(&ctx);
    let handle = host.event_handle();

    exec_probe(&reg, &handle, &ctx, "probe_spawn", json!({})).expect("spawn failed");
    let result = exec_probe(
        &reg,
        &handle,
        &ctx,
        "probe_prompt",
        json!({ "message": "wait forever", "timeout": 1 }),
    )
    .expect("timed prompt did not resolve");

    assert_eq!(result["error"], "session prompt timed out after 1s");
    dropped_rx
        .recv_timeout(TURN_TIMEOUT)
        .expect("provider request remained alive after prompt timeout");
}

/// After a subagent finishes a run, its transcript must be surfaced to the
/// parent (as a SubagentHistory envelope) so the UI can queue the reply back to
/// the main agent with a header. Regression: this was only emitted on explicit
/// close, so a completed async subagent's reply never reached the main agent.
#[test]
fn subagent_completion_surfaces_reply_to_parent() {
    let (ctx, parent_rx, _run_trigger) = ctx_with_canned_provider();
    let (reg, host) = load_probe_host(&ctx);
    let handle = host.event_handle();

    exec_probe(&reg, &handle, &ctx, "probe_spawn", json!({})).expect("spawn failed");
    exec_probe(
        &reg,
        &handle,
        &ctx,
        "probe_send",
        json!({ "message": "do the thing" }),
    )
    .expect("send failed");

    // Pump the executor and drain the parent event channel until the subagent's
    // completed run surfaces its history.
    let mut found = false;
    for _ in 0..100 {
        for envelope in parent_rx.try_iter() {
            if let AgentEvent::SubagentHistory { messages, .. } = &envelope.event {
                let has_reply = messages
                    .iter()
                    .any(|m| matches!(m.role, Role::Assistant) && !m.content.is_empty());
                eprintln!("subagent surfaced history, has reply: {has_reply}");
                if has_reply {
                    found = true;
                }
            }
        }
        if found {
            break;
        }
        smol::block_on(async { smol::Timer::after(std::time::Duration::from_millis(5)).await });
    }
    assert!(
        found,
        "a completed subagent's reply must reach the parent as SubagentHistory"
    );
}
