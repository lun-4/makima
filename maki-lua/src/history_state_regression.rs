use std::borrow::Cow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use maki_agent::agent::tool_dispatch;
use maki_agent::cancel::CancelToken;
use maki_agent::tools::{
    CallOrigin, DescriptionContext, ExecFuture, HeaderFuture, HeaderResult, ParseError, Tool,
    ToolContext, ToolExecResult, ToolInvocation, ToolRegistry, ToolSource, TurnToolBindings,
};
use maki_agent::{AgentMode, History, HistorySnapshot, SharedMessages, ToolDoneEvent, ToolOutput};
use maki_providers::{ContentBlock, Message, Role};
use maki_storage::id::MakiId;
use maki_storage::sessions::{HistoryStateOperation, Session, SessionLog, SessionMeta};
use serde_json::{Value, json};
use tempfile::TempDir;
use test_case::test_case;

use crate::PluginHost;
use crate::api::fs::InMemoryFs;

const PLUGIN: &str = "history_state_regression";
const GATE_PLUGIN: &str = "history_state_gate";
const GATE_TOOL: &str = "history_test_gate";
const OUTER_TOOL: &str = "history_test_outer";
const WRITER_TOOL: &str = "history_test_writer";
const READER_TOOL: &str = "history_test_reader";
const NAMESPACE: &str = "test.history";
const CALL_ID: &str = "outer-call";
const FIRST: &str = "first";
const SECOND: &str = "second";
const BEFORE: &str = "before";
const AFTER: &str = "after";
const OUTER_FAILURE: &str = "outer failed after nested commit";
const STALE_ERROR: &str = "history state handle belongs to a replaced history";
const CANCELLED_ERROR: &str = "history state operation was cancelled";
const TIMEOUT: Duration = Duration::from_secs(10);
const SOURCE: &str = r#"
local function failure(err)
  return { llm_output = err, is_error = true }
end
local function gate(ctx, label)
  local text, err = maki.agent.call_tool(ctx, "history_test_gate", { label = label })
  return text, err
end
local schema = {
  type = "object",
  properties = {
    namespace = { type = "string", required = true },
    value = { type = "string" },
    before = { type = "string" },
    after = { type = "string" },
    fail = { type = "boolean" },
    delete = { type = "boolean" },
  },
}
maki.api.register_tool({
  name = "history_test_writer",
  description = "Replace or delete history state",
  schema = schema,
  handler = function(input, ctx)
    if input.before then
      local _, err = gate(ctx, input.before)
      if err then return failure(err) end
    end
    local ok, err
    if input.delete then
      ok, err = ctx:history_delete(input.namespace)
    else
      ok, err = ctx:history_set(input.namespace, { value = input.value })
    end
    if not ok then return failure(err) end
    return input.value or "deleted"
  end,
})
maki.api.register_tool({
  name = "history_test_outer",
  description = "Run a nested history writer before returning",
  schema = schema,
  handler = function(input, ctx)
    local text, err = maki.agent.call_tool(ctx, "history_test_writer", input)
    if err then return failure(err) end
    if input.after then
      local _, gate_err = gate(ctx, input.after)
      if gate_err then return failure(gate_err) end
    end
    if input.fail then return failure("outer failed after nested commit") end
    return text
  end,
})
maki.api.register_tool({
  name = "history_test_reader",
  description = "Read history state",
  schema = schema,
  handler = function(input, ctx)
    local value, err = ctx:history_get(input.namespace)
    if err then return failure(err) end
    return value and value.value or "absent"
  end,
})
"#;

type DiskSession = Session<Message, Value, Value>;

struct Gate {
    entered_tx: flume::Sender<String>,
    entered_rx: flume::Receiver<String>,
    releases: Mutex<HashMap<String, flume::Sender<()>>>,
}

impl Gate {
    fn new() -> Arc<Self> {
        let (entered_tx, entered_rx) = flume::unbounded();
        Arc::new(Self {
            entered_tx,
            entered_rx,
            releases: Mutex::new(HashMap::new()),
        })
    }

    fn wait(&self, expected: &str) {
        let entered = smol::block_on(futures_lite::future::race(
            async { self.entered_rx.recv_async().await.unwrap() },
            async {
                smol::Timer::after(TIMEOUT).await;
                panic!("nested tool did not reach gate {expected}");
            },
        ));
        assert_eq!(entered, expected);
    }

    fn release(&self, label: &str) {
        let _ = self
            .releases
            .lock()
            .unwrap()
            .remove(label)
            .unwrap()
            .send(());
    }
}

struct GateTool(Arc<Gate>);

struct GateInvocation {
    gate: Arc<Gate>,
    label: String,
}

impl Tool for GateTool {
    fn name(&self) -> &str {
        GATE_TOOL
    }

    fn description(&self, _ctx: &DescriptionContext) -> Cow<'_, str> {
        "Pause a nested tool invocation".into()
    }

    fn schema(&self) -> Value {
        json!({"type": "object", "properties": {"label": {"type": "string"}}})
    }

    fn parse(&self, input: &Value) -> Result<Box<dyn ToolInvocation>, ParseError> {
        Ok(Box::new(GateInvocation {
            gate: Arc::clone(&self.0),
            label: input["label"].as_str().unwrap().to_owned(),
        }))
    }
}

impl ToolInvocation for GateInvocation {
    fn start_header(&self) -> HeaderFuture {
        HeaderFuture::Ready(HeaderResult::plain(self.label.clone()))
    }

    fn execute<'a>(self: Box<Self>, _ctx: &'a ToolContext) -> ExecFuture<'a> {
        Box::pin(async move {
            let (release_tx, release_rx) = flume::bounded(1);
            self.gate
                .releases
                .lock()
                .unwrap()
                .insert(self.label.clone(), release_tx);
            self.gate.entered_tx.send(self.label).unwrap();
            let output = release_rx
                .recv_async()
                .await
                .map(|()| ToolOutput::Plain("released".into()))
                .map_err(|error| error.to_string());
            ToolExecResult::from(output)
        })
    }
}

fn boot() -> (Arc<ToolRegistry>, PluginHost, Arc<Gate>) {
    let registry = Arc::new(ToolRegistry::new());
    let gate = Gate::new();
    registry
        .register(
            Arc::new(GateTool(Arc::clone(&gate))),
            ToolSource::Lua {
                plugin: GATE_PLUGIN.into(),
            },
        )
        .unwrap();
    let host = PluginHost::with_fs_for_tests(
        Arc::clone(&registry),
        Arc::new(InMemoryFs::new()),
        PathBuf::from(crate::test_support::TEST_STATE_DIR),
    )
    .unwrap();
    host.load_source(PLUGIN, SOURCE).unwrap();
    (registry, host, gate)
}

fn history() -> (History, SharedMessages) {
    let mirror = Arc::new(ArcSwap::from_pointee(HistorySnapshot::default()));
    let history = History::new(vec![Message {
        role: Role::Assistant,
        content: vec![ContentBlock::tool_use(CALL_ID, OUTER_TOOL, input(FIRST))],
        ..Default::default()
    }])
    .with_mirror(Arc::clone(&mirror));
    (history, mirror)
}

fn shared_ctx(registry: &Arc<ToolRegistry>, history: &History) -> ToolContext {
    let mut ctx = maki_agent::tools::test_support::stub_ctx(&AgentMode::Build);
    ctx.registry = Arc::clone(registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        &ctx.registry,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    ctx.history_state = Some(history.state_handle(history.len(), ctx.cancel.clone()));
    ctx
}

fn input(value: &str) -> Value {
    json!({"namespace": NAMESPACE, "value": value})
}

fn dispatch(ctx: &ToolContext, name: &str, input: Value) -> ToolDoneEvent {
    smol::block_on(tool_dispatch::run(
        CALL_ID.into(),
        name,
        &input,
        ctx,
        CallOrigin::Model,
    ))
}

fn spawn(ctx: ToolContext, input: Value) -> (smol::Task<ToolDoneEvent>, flume::Receiver<()>) {
    let (done_tx, done_rx) = flume::bounded(1);
    let task = smol::spawn(async move {
        let done =
            tool_dispatch::run(CALL_ID.into(), OUTER_TOOL, &input, &ctx, CallOrigin::Model).await;
        done_tx.send(()).unwrap();
        done
    });
    (task, done_rx)
}

#[track_caller]
fn assert_success(done: &ToolDoneEvent, expected: &str) {
    assert!(!done.is_error, "{}", done.output.as_text());
    assert_eq!(done.output.as_text(), expected);
}

#[track_caller]
fn assert_state(history: &History, mirror: &SharedMessages, expected: Option<&str>) {
    let snapshot = history.snapshot();
    assert_eq!(
        snapshot.state.get(NAMESPACE),
        expected.map(|value| json!({"value": value})).as_ref()
    );
    let published = mirror.load_full();
    assert_eq!(
        serde_json::to_value(published.messages.as_ref()).unwrap(),
        serde_json::to_value(snapshot.messages.as_ref()).unwrap()
    );
    assert_eq!(published.state, snapshot.state);
    assert_eq!(published.epoch, snapshot.epoch);
    assert_eq!(published.compacted, snapshot.compacted);
}

#[test]
fn dispatch_harness_nested_barrier_smoke() {
    let (registry, _host, gate) = boot();
    let (history, mirror) = history();
    let mut args = input(FIRST);
    args[BEFORE] = json!(BEFORE);
    args[AFTER] = json!(AFTER);
    let (task, done) = spawn(shared_ctx(&registry, &history), args);

    gate.wait(BEFORE);
    assert_state(&history, &mirror, None);
    assert!(done.is_empty());
    gate.release(BEFORE);
    gate.wait(AFTER);
    assert_state(&history, &mirror, Some(FIRST));
    assert!(done.is_empty());
    gate.release(AFTER);
    assert_success(&smol::block_on(task), FIRST);
    assert_eq!(done.recv_timeout(TIMEOUT), Ok(()));
}

#[test]
fn history_state_nested_commit() {
    let (registry, _host, gate) = boot();
    let (history, mirror) = history();
    let ctx = shared_ctx(&registry, &history);
    let mut args = input(FIRST);
    args[AFTER] = json!(AFTER);
    let (task, done) = spawn(ctx.clone(), args);

    gate.wait(AFTER);
    assert!(done.is_empty());
    assert_state(&history, &mirror, Some(FIRST));
    assert_success(&dispatch(&ctx, READER_TOOL, input(FIRST)), FIRST);
    gate.release(AFTER);
    assert_success(&smol::block_on(task), FIRST);

    let mut deletion = input(FIRST);
    deletion["delete"] = json!(true);
    assert_success(&dispatch(&ctx, OUTER_TOOL, deletion), FIRST);
    assert_state(&history, &mirror, None);
    let snapshot = history.snapshot();
    assert_eq!(snapshot.state.updates.len(), 2);
    assert!(
        snapshot
            .state
            .updates
            .iter()
            .all(|update| update.boundary == history.len())
    );
    assert_eq!(
        snapshot.state.updates[1].operation,
        HistoryStateOperation::Delete
    );
    assert_success(&dispatch(&ctx, READER_TOOL, input(FIRST)), "absent");
}

#[test]
fn history_state_nested_commit_outer_failure() {
    let (registry, _host, gate) = boot();
    let (history, mirror) = history();
    let mut args = input(FIRST);
    args[AFTER] = json!(AFTER);
    args["fail"] = json!(true);
    let (task, done) = spawn(shared_ctx(&registry, &history), args);

    gate.wait(AFTER);
    assert!(done.is_empty());
    assert_state(&history, &mirror, Some(FIRST));
    gate.release(AFTER);
    let result = smol::block_on(task);
    assert!(result.is_error);
    assert_eq!(result.output.as_text(), OUTER_FAILURE);
    assert_state(&history, &mirror, Some(FIRST));
}

#[test_case(false; "cancelled_before_nested_commit")]
#[test_case(true; "committed_before_cancellation")]
fn history_state_cancellation(committed: bool) {
    let (registry, _host, gate) = boot();
    let (history, mirror) = history();
    let mut ctx = shared_ctx(&registry, &history);
    let (trigger, cancel) = CancelToken::new();
    ctx.cancel = cancel.clone();
    ctx.history_state = Some(history.state_handle(history.len(), cancel));
    let label = if committed { AFTER } else { BEFORE };
    let mut args = input(FIRST);
    args[label] = json!(label);
    let (task, done) = spawn(ctx, args);

    gate.wait(label);
    assert!(done.is_empty());
    assert_state(&history, &mirror, committed.then_some(FIRST));
    trigger.cancel();
    gate.release(label);
    let result = smol::block_on(task);
    assert!(result.is_error, "{}", result.output.as_text());
    assert_state(&history, &mirror, committed.then_some(FIRST));
}

#[test]
fn history_state_cancelled_capability_rejects_commit() {
    let (registry, _host, _gate) = boot();
    let (history, mirror) = history();
    let mut ctx = shared_ctx(&registry, &history);
    let (trigger, cancel) = CancelToken::new();
    ctx.history_state = Some(history.state_handle(history.len(), cancel));
    trigger.cancel();

    let result = dispatch(&ctx, OUTER_TOOL, input(FIRST));
    assert!(result.is_error);
    assert!(result.output.as_text().contains(CANCELLED_ERROR));
    assert_state(&history, &mirror, None);
}

#[test_case(false; "first_then_second")]
#[test_case(true; "second_then_first")]
fn history_state_concurrent_ordering(reverse: bool) {
    let (registry, _host, gate) = boot();
    let (history, mirror) = history();
    let ctx = shared_ctx(&registry, &history);
    let mut first = input(FIRST);
    first[BEFORE] = json!(FIRST);
    first[AFTER] = json!("first-committed");
    let (first_task, first_done) = spawn(ctx.clone(), first);
    gate.wait(FIRST);
    let mut second = input(SECOND);
    second[BEFORE] = json!(SECOND);
    second[AFTER] = json!("second-committed");
    let (second_task, second_done) = spawn(ctx.clone(), second);
    gate.wait(SECOND);
    let order = if reverse {
        [SECOND, FIRST]
    } else {
        [FIRST, SECOND]
    };
    for value in order {
        gate.release(value);
        let label = format!("{value}-committed");
        gate.wait(&label);
        assert_state(&history, &mirror, Some(value));
        assert_success(&dispatch(&ctx, READER_TOOL, input(value)), value);
        assert!(first_done.is_empty());
        assert!(second_done.is_empty());
    }
    gate.release("first-committed");
    gate.release("second-committed");
    assert_success(&smol::block_on(first_task), FIRST);
    assert_success(&smol::block_on(second_task), SECOND);
    assert_state(&history, &mirror, Some(order[1]));
    let snapshot = history.snapshot();
    let operations: Vec<_> = snapshot
        .state
        .updates
        .iter()
        .map(|update| {
            assert_eq!(update.boundary, history.len());
            assert_eq!(update.namespace, NAMESPACE);
            update.operation.clone()
        })
        .collect();
    assert_eq!(
        operations,
        order.map(|value| HistoryStateOperation::Set(json!({"value": value})))
    );
}

#[test]
fn history_state_parent_child_isolation() {
    let (registry, _host, gate) = boot();
    let (parent, parent_mirror) = history();
    let (child, child_mirror) = history();
    let mut parent_ctx = shared_ctx(&registry, &parent);
    maki_agent::tools::test_support::set_session(&mut parent_ctx, MakiId::generate());
    let mut child_ctx = shared_ctx(&registry, &child);
    child_ctx.session_id = parent_ctx.session_id.clone();
    assert!(parent_ctx.session_id.is_some());
    let mut args = input(FIRST);
    args[AFTER] = json!(AFTER);
    let (task, done) = spawn(parent_ctx.clone(), args);

    gate.wait(AFTER);
    assert!(done.is_empty());
    assert_state(&parent, &parent_mirror, Some(FIRST));
    assert_state(&child, &child_mirror, None);
    assert_success(&dispatch(&child_ctx, OUTER_TOOL, input(SECOND)), SECOND);
    assert_state(&parent, &parent_mirror, Some(FIRST));
    assert_state(&child, &child_mirror, Some(SECOND));
    assert_success(&dispatch(&parent_ctx, READER_TOOL, input(FIRST)), FIRST);
    assert_success(&dispatch(&child_ctx, READER_TOOL, input(SECOND)), SECOND);
    gate.release(AFTER);
    assert_success(&smol::block_on(task), FIRST);
}

#[test]
fn history_state_pre_join_write_survives_result_append_and_resume() {
    let (registry, _host, gate) = boot();
    let (mut history, mirror) = history();
    let mut args = input(FIRST);
    args[AFTER] = json!(AFTER);
    let temp = TempDir::new().unwrap();
    let mut session = Arc::new(DiskSession::new("test", "/tmp"));
    Session::checkpoint(
        &mut session,
        Some(&history.snapshot()),
        SessionMeta::default(),
        Value::Null,
    );
    let mut log = SessionLog::rewrite(temp.path(), &session).unwrap();
    let (task, done) = spawn(shared_ctx(&registry, &history), args);

    gate.wait(AFTER);
    assert!(done.is_empty());
    assert_state(&history, &mirror, Some(FIRST));
    let frozen = history.snapshot();
    Session::checkpoint(
        &mut session,
        Some(&mirror.load_full()),
        SessionMeta::default(),
        Value::Null,
    );
    log.append(&session).unwrap();
    let pre_join = DiskSession::load_from(session.id, temp.path()).unwrap();
    assert_eq!(pre_join.history_snapshot().state, frozen.state);
    assert_eq!(pre_join.history_snapshot().messages.len(), 1);

    gate.release(AFTER);
    let result = smol::block_on(task);
    assert_success(&result, FIRST);
    history.push(Message {
        role: Role::User,
        content: vec![ContentBlock::ToolResult {
            tool_use_id: result.id,
            content: result.output.as_text(),
            is_error: result.is_error,
        }],
        display_text: Some(String::new()),
        ..Default::default()
    });
    assert_state(&history, &mirror, Some(FIRST));
    assert_eq!(frozen.messages.len(), 1);
    Session::checkpoint(
        &mut session,
        Some(&history.snapshot()),
        SessionMeta::default(),
        Value::Null,
    );
    log.append(&session).unwrap();
    let loaded = DiskSession::load_from(session.id, temp.path()).unwrap();
    assert_eq!(loaded.history_snapshot().messages.len(), 2);
    assert_eq!(loaded.history_snapshot().state, frozen.state);
    let resumed = History::restored_snapshot(loaded.history_snapshot());
    assert_eq!(resumed.snapshot().messages.len(), 2);
    assert_success(
        &dispatch(&shared_ctx(&registry, &resumed), READER_TOOL, input(FIRST)),
        FIRST,
    );
    let mut rewound = resumed;
    rewound.truncate(1);
    assert_success(
        &dispatch(&shared_ctx(&registry, &rewound), READER_TOOL, input(FIRST)),
        FIRST,
    );
    rewound.truncate(0);
    assert_success(
        &dispatch(&shared_ctx(&registry, &rewound), READER_TOOL, input(FIRST)),
        "absent",
    );
}

#[test_case("rewind"; "rewind")]
#[test_case("compaction"; "compaction")]
#[test_case("reset"; "reset")]
fn history_state_stale_handle_rejected(replacement: &str) {
    let (registry, _host, gate) = boot();
    let (mut history, mirror) = history();
    let ctx = shared_ctx(&registry, &history);
    assert_success(&dispatch(&ctx, WRITER_TOOL, input(FIRST)), FIRST);
    let mut args = input(SECOND);
    args[BEFORE] = json!(BEFORE);
    let (task, done) = spawn(ctx, args);
    gate.wait(BEFORE);
    assert!(done.is_empty());

    match replacement {
        "rewind" => history.truncate(0),
        "compaction" => history.replace_compacted(vec![Message {
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "compacted context".into(),
            }],
            ..Default::default()
        }]),
        "reset" => history.replace(Vec::new()),
        _ => unreachable!(),
    }
    let expected = (replacement == "compaction").then_some(FIRST);
    assert_state(&history, &mirror, expected);
    gate.release(BEFORE);
    let result = smol::block_on(task);
    assert!(result.is_error);
    assert!(
        result.output.as_text().contains(STALE_ERROR),
        "{}",
        result.output.as_text()
    );
    assert_state(&history, &mirror, expected);
    history.push(Message {
        role: Role::Assistant,
        content: vec![ContentBlock::tool_use(CALL_ID, OUTER_TOOL, input(SECOND))],
        ..Default::default()
    });
    let fresh = shared_ctx(&registry, &history);
    assert_success(&dispatch(&fresh, OUTER_TOOL, input(SECOND)), SECOND);
    assert_state(&history, &mirror, Some(SECOND));
}
