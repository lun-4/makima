//! Reproduction tests for async subagent lifecycle regressions.
//!
//! 1. `subagent_outlives_the_run_that_spawned_it`: a subagent must survive its
//!    parent run ending normally (the run's cancel must not close it).
//! 2. `subagent_completion_surfaces_reply_to_parent`: after a subagent finishes
//!    a run, its transcript must be surfaced to the parent so the UI can queue
//!    the reply back to the main agent (SubagentHistory must be emitted, not
//!    only on explicit close).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use maki_agent::AgentEvent;
use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role, StreamResponse,
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};

mod common;
use common::{ctx_with_canned_provider, ctx_with_provider, exec_tool, production_like_ctx};

const PROVIDER_FAILURE: &str = "deterministic provider failure";
const RECOVERED_REPLY: &str = "recovered on the same session";
const FIRST_REPLY: &str = "READY";
const SECOND_REPLY: &str = "VIOLET";
const TURN_TIMEOUT: Duration = Duration::from_secs(2);

const PROBE_SRC: &str = r#"
session_holder = { sess = nil }

maki.api.register_tool({
  name = "probe_spawn",
  description = "create a subagent session without running it",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(input, ctx)
    local sess, err = maki.agent.session(ctx, { name = "probe" })
    if not sess then
      return { llm_output = "spawn failed: " .. err, is_error = true }
    end
    session_holder.sess = sess
    return maki.json.encode({ task_id = sess:session_id() })
  end,
})

maki.api.register_tool({
  name = "probe_status",
  description = "read the subagent session's status",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    return maki.json.encode(session_holder.sess:status())
  end,
})

maki.api.register_tool({
  name = "probe_send",
  description = "send a message to the subagent session",
  schema = { type = "object", properties = { message = { type = "string" } }, additionalProperties = false },
  audiences = { "main" },
  handler = function(input)
    local _, err = session_holder.sess:send(input.message)
    if err then
      return { llm_output = "send failed: " .. err, is_error = true }
    end
    return maki.json.encode({ ok = true })
  end,
})

maki.api.register_tool({
  name = "probe_prompt",
  description = "run one blocking turn on the subagent session",
  schema = { type = "object", properties = { message = { type = "string" } }, additionalProperties = false },
  audiences = { "main" },
  handler = function(input)
    local result, err = session_holder.sess:prompt(input.message)
    return maki.json.encode({ result = result, error = err })
  end,
})
"#;

fn load_probe_host() -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("probe", PROBE_SRC).unwrap();
    (reg, host)
}

struct FailOnceProvider {
    calls: AtomicUsize,
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

#[test]
fn async_sends_are_fifo_and_surface_each_history_snapshot() {
    let (reg, _host) = load_probe_host();
    let (entered_tx, entered_rx) = flume::bounded(2);
    let (release_tx, release_rx) = flume::unbounded();
    let provider = Arc::new(GatedProvider {
        replies: Mutex::new(vec![FIRST_REPLY, SECOND_REPLY]),
        entered: entered_tx,
        release: release_rx,
        requests: Mutex::new(Vec::new()),
    });
    let (ctx, parent_rx, _run_trigger) = ctx_with_provider(Arc::clone(&provider));
    let spawn = exec_tool(&reg, &ctx, "probe_spawn", json!({})).unwrap();
    let first = {
        let reg = Arc::clone(&reg);
        let ctx = ctx.clone();
        std::thread::spawn(move || exec_tool(&reg, &ctx, "probe_send", json!({"message": "first"})))
    };
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 0);
    let second = {
        let reg = Arc::clone(&reg);
        let ctx = ctx.clone();
        std::thread::spawn(move || {
            exec_tool(&reg, &ctx, "probe_send", json!({"message": "second"}))
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
    let (reg, _host) = load_probe_host();
    let (entered_tx, entered_rx) = flume::bounded(1);
    let provider = Arc::new(CancelThenRecoverProvider {
        calls: AtomicUsize::new(0),
        entered: entered_tx,
    });
    let (ctx, parent_rx, _run_trigger) = ctx_with_provider(Arc::clone(&provider));
    exec_tool(&reg, &ctx, "probe_spawn", json!({})).unwrap();
    let reg_for_prompt = Arc::clone(&reg);
    let ctx_for_prompt = ctx.clone();
    let prompt = std::thread::spawn(move || {
        exec_tool(
            &reg_for_prompt,
            &ctx_for_prompt,
            "probe_prompt",
            json!({"message": "cancel me"}),
        )
    });
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 0);
    let cancel_tx = loop {
        let envelope = parent_rx.recv_timeout(TURN_TIMEOUT).unwrap();
        if let Some(cancel_tx) = envelope.subagent.and_then(|info| info.cancel_tx) {
            break cancel_tx;
        }
    };
    cancel_tx.send(()).unwrap();
    let cancelled = prompt.join().unwrap().unwrap();
    assert!(
        cancelled["error"]
            .as_str()
            .is_some_and(|error| error.contains("cancel"))
    );

    exec_tool(&reg, &ctx, "probe_send", json!({"message": "recover"})).unwrap();
    assert_eq!(entered_rx.recv_timeout(TURN_TIMEOUT).unwrap(), 1);
    loop {
        let envelope = parent_rx.recv_timeout(TURN_TIMEOUT).unwrap();
        let AgentEvent::SubagentHistory { messages, .. } = envelope.event else {
            continue;
        };
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
    let (reg, _host) = load_probe_host();
    let (ctx, run_trigger) = production_like_ctx();

    exec_tool(&reg, &ctx, "probe_spawn", json!({})).expect("spawn failed");

    let before = exec_tool(&reg, &ctx, "probe_status", json!({})).unwrap();
    eprintln!("status (before run end) -> {before}");
    assert_ne!(
        before["status"], "closed",
        "subagent must be alive after spawn"
    );

    run_trigger.cancel();

    let mut status = Value::Null;
    for _ in 0..20 {
        status = exec_tool(&reg, &ctx, "probe_status", json!({})).unwrap();
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
    let (reg, _host) = load_probe_host();
    let provider = Arc::new(FailOnceProvider {
        calls: AtomicUsize::new(0),
    });
    let (ctx, parent_rx, _run_trigger) = ctx_with_provider(provider);

    exec_tool(&reg, &ctx, "probe_spawn", json!({})).expect("spawn failed");

    let failed = exec_tool(
        &reg,
        &ctx,
        "probe_prompt",
        json!({ "message": "fail this turn" }),
    )
    .expect("failed turn did not resolve");
    assert_eq!(failed["error"], json!(PROVIDER_FAILURE));
    let status = exec_tool(&reg, &ctx, "probe_status", json!({})).unwrap();
    assert_eq!(status["error"], json!(PROVIDER_FAILURE));
    let history = parent_rx
        .recv_timeout(TURN_TIMEOUT)
        .expect("failure history event");
    assert!(matches!(history.event, AgentEvent::SubagentHistory { .. }));

    let recovered = exec_tool(
        &reg,
        &ctx,
        "probe_prompt",
        json!({ "message": "reuse the same session" }),
    )
    .expect("later turn did not resolve");
    assert!(recovered["error"].is_null(), "got: {recovered}");
    assert_eq!(recovered["result"]["text"], json!(RECOVERED_REPLY));
}

/// After a subagent finishes a run, its transcript must be surfaced to the
/// parent (as a SubagentHistory envelope) so the UI can queue the reply back to
/// the main agent with a header. Regression: this was only emitted on explicit
/// close, so a completed async subagent's reply never reached the main agent.
#[test]
fn subagent_completion_surfaces_reply_to_parent() {
    let (reg, _host) = load_probe_host();
    let (ctx, parent_rx, _run_trigger) = ctx_with_canned_provider();

    exec_tool(&reg, &ctx, "probe_spawn", json!({})).expect("spawn failed");
    exec_tool(
        &reg,
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
