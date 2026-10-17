//! Shared canned-provider harness for maki-lua integration tests.
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

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maki_agent::cancel::{CancelMap, CancelToken, CancelTrigger};
use maki_agent::template::Vars;
use maki_agent::tools::test_support::stub_ctx;
use maki_agent::tools::{DescriptionContext, RequestTools, ToolContext, ToolRegistry};
use maki_agent::{AgentMode, Envelope, EventSender, ToolOutput};
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role,
    StopReason, StreamResponse, ThinkingConfig, TokenUsage,
};
use maki_storage::id::SessionRef;
use serde_json::Value;

const TEST_OPERATION_TIMEOUT: Duration = Duration::from_secs(5);

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
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            self.captured_opts.lock().unwrap().push(opts);
            self.captured_tools.lock().unwrap().push(tools.clone());
            let mut replies = self.replies.lock().unwrap();
            assert!(!replies.is_empty(), "CannedProvider: no more responses");
            Ok(replies.remove(0))
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

pub fn default_model() -> Model {
    Model::from_spec("anthropic/claude-sonnet-4-20250514").unwrap()
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
    let event_tx = EventSender::new(tx, 0);
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.cancel = run_cancel;
    ctx.subagent_cancels = Arc::new(CancelMap::new());
    ctx.event_tx = event_tx;
    ctx.provider = provider;
    ctx.model = Arc::new(default_model());
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
