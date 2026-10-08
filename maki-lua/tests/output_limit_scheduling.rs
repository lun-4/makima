use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maki_agent::agent::tool_dispatch;
use maki_agent::cancel::CancelToken;
use maki_agent::tools::hook::{
    HookCall, HookStage, OUTPUT_TEXT, OUTPUT_TRAILER, ToolHook, Verdict,
};
use maki_agent::tools::offload::{
    OFFLOAD_FOOTER_PREFIX, OFFLOAD_POINTER_PREFIX, OffloadBackend, OffloadCleanup, OffloadSnapshot,
    OffloadStore,
};
use maki_agent::tools::registry::BoxFuture;
use maki_agent::tools::test_support::stub_ctx;
use maki_agent::tools::{
    CallOrigin, Deadline, ToolContext, ToolExecResult, ToolRegistry, TurnToolBindings,
};
use maki_agent::{AgentMode, ToolDoneEvent, ToolOutput};
use maki_lua::{PluginHost, UiAction};
use serde_json::{Value, json};
use test_case::test_case;

const WATCHDOG: Duration = Duration::from_secs(10);
const FINALIZATION_TIMEOUT: Duration = Duration::from_secs(1);
const TIMEOUT_ERROR: &str = "timeout exceeded";
const CANCELLED_ERROR: &str = "cancelled";
const BODY: &str = "first\nsecond\nthird\nfourth";
const OTHER_BODY: &str = "other\nsecond\nthird\nfourth";
const TRAILER: &str = "[cancelled by user; output above is partial]";
const SIBLING_REPLY: &str = "sibling completed";
const STORE_DIR: &str = "/output-limit-test";
const SECRET_TRAILER: &str = "secret cancellation token";
const REDACTED_TRAILER: &str = "[redacted cancellation token]";
const TIMEOUT_FRAGMENT: &str = "timeout";
const DEADLINE_FRAGMENT: &str = "deadline exceeded";
const DISPATCH_ID: &str = "output-limit-dispatch";
const EXIT_TRAILER: &str = "Exit code: 7";
const SECRET_LABEL: &str = "secret cancellation token label";
const REPLACEMENT: &str = "replacement first\nreplacement second";
const REPLACEMENT_TRAILER: &str = "Exit code: 3";
const SOURCE: &str = r#"
maki.api.register_tool({
    name = "limited", description = "limits output", schema = {
        type = "object", properties = {
            mode = { type = "string" }, body = { type = "string" },
            trailer = { type = "string" }, is_error = { type = "boolean" }, max_lines = { type = "integer" },
            deadline = { type = "integer" }, max_bytes = { type = "integer" },
            label = { type = "string" },
        },
    },
    handler = function(input, ctx)
        if input.deadline then ctx:set_deadline(input.deadline) end
        maki.ui.flash(input.mode .. ":ready")
        local limits = { max_lines = input.max_lines, max_bytes = input.max_bytes,
            trailer = input.trailer, label = input.label }
        if input.mode == "deferred" then
            return { llm_output = input.body, output_limits = limits, is_error = input.is_error }
        end
        if input.mode == "content_deferred" then
            return { content = input.body, output_limits = limits, is_error = input.is_error }
        end
        if input.mode == "finish" then
            ctx:finish({ llm_output = input.body, output_limits = limits, is_error = input.is_error })
            return nil
        end
        if input.mode == "content_finish" then
            ctx:finish({ content = input.body, output_limits = limits, is_error = input.is_error })
            return nil
        end
        if input.mode == "cancel" then
            maki.async.on_cancel(function()
                ctx:finish({ llm_output = input.body, output_limits = limits, is_error = true })
                maki.ui.flash("hook:finished")
            end)
        end
        local output, err = ctx:limit_output(input.body, limits)
        if err then return { llm_output = err, is_error = true } end
        return output
    end,
})
maki.api.register_tool({
    name = "sibling", description = "responds without offloading",
    schema = { type = "object", properties = {}, additionalProperties = false },
    handler = function() return "sibling completed" end,
})
"#;

struct OutputHook(flume::Sender<String>);

impl ToolHook for OutputHook {
    fn wraps(&self, tool: &str, stage: HookStage) -> bool {
        tool == "limited" && stage == HookStage::Output
    }

    fn run<'a>(
        &'a self,
        _stage: HookStage,
        mut value: Value,
        _call: &'a HookCall<'a>,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            let body = value[OUTPUT_TEXT].as_str().unwrap();
            self.0.send(body.to_owned()).unwrap();
            value[OUTPUT_TEXT] = Value::String(body.replace(SECRET_TRAILER, REDACTED_TRAILER));
            if let Some(trailer) = value[OUTPUT_TRAILER].as_str() {
                value[OUTPUT_TRAILER] =
                    Value::String(trailer.replace(SECRET_TRAILER, REDACTED_TRAILER));
            }
            Verdict::Replaced(value)
        })
    }
}

#[test_case(REPLACEMENT, None, false, REPLACEMENT, None; "same_line_count_replacement")]
#[test_case(REPLACEMENT, Some(REPLACEMENT_TRAILER), false, REPLACEMENT, None; "stale_original_field")]
#[test_case(REPLACEMENT, Some("redacted exit"), false, REPLACEMENT, None; "metadata_cannot_reintroduce_missing_trailer")]
#[test_case("replacement first\nredacted exit", None, false, "replacement first\nredacted exit", None; "implicit_changed_trailer")]
#[test_case("replacement first\nredacted exit", Some("redacted exit"), false, "replacement first", Some("redacted exit"); "explicit_changed_trailer")]
#[test_case("replacement first\nredacted exit", Some("mismatch"), false, "replacement first\nredacted exit", None; "mismatched_changed_trailer")]
#[test_case("replacement first\nredacted exit", Some(""), false, "replacement first\nredacted exit", None; "empty_trailer")]
#[test_case("replacement first\nExit code: 3", Some(REPLACEMENT_TRAILER), false, "replacement first", Some(REPLACEMENT_TRAILER); "body_only_redaction")]
#[test_case("replacement first\nExit code: 3", None, false, "replacement first\nExit code: 3", None; "original_suffix_without_metadata")]
#[test_case("replacement first\nreplacement second\nExit code: 3", Some(REPLACEMENT_TRAILER), false, REPLACEMENT, Some(REPLACEMENT_TRAILER); "body_restructured_original_suffix")]
#[test_case("replacement first\nprefixredacted exit", Some("redacted exit"), false, "replacement first\nprefixredacted exit", None; "changed_suffix_without_boundary")]
#[test_case(REPLACEMENT, None, true, "body", Some(REPLACEMENT_TRAILER); "unchanged_verdict")]
fn replacement_trailer_protection_requires_terminal_provenance(
    text: &'static str,
    trailer: Option<&str>,
    unchanged: bool,
    saved_body: &str,
    protected: Option<&str>,
) {
    for mode in ["async", "deferred"] {
        for budget in [0, 1] {
            for null_trailer in [false, true] {
                let (registry, _host) = host();
                registry.set_hook(ReplacementHook {
                    text,
                    trailer: trailer
                        .map(|trailer| json!(trailer))
                        .or_else(|| null_trailer.then_some(Value::Null)),
                    unchanged,
                });
                let mut gate = Gate::new();
                smol::block_on(async {
                    let ctx = dispatch_ctx(&registry, &gate);
                    let input = json!({
                        "mode": mode, "body": "body", "trailer": REPLACEMENT_TRAILER,
                        "max_lines": budget, "max_bytes": budget,
                    });
                    let caller = smol::spawn(async move {
                        tool_dispatch::run(
                            DISPATCH_ID.to_owned(),
                            "limited",
                            &input,
                            &ctx,
                            CallOrigin::Nested,
                        )
                        .await
                    });
                    checked(gate.entered.recv_async()).await.unwrap();
                    gate.release.release();
                    let done = checked(caller).await;
                    assert_eq!(gate.saved_bodies(), vec![saved_body.to_owned()]);
                    let output = done.output.as_text();
                    assert!(output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
                    if let Some(trailer) = protected {
                        assert!(output.ends_with(trailer), "{output}");
                    } else {
                        assert!(!output.contains(text), "{output}");
                        assert!(!output.ends_with(REPLACEMENT_TRAILER), "{output}");
                    }
                });
            }
        }
    }
}

#[test_case(true, false; "explicit_trailer_redaction")]
#[test_case(false, false; "stale_original_trailer")]
#[test_case(false, true; "removed_trailer_field")]
fn lua_output_slot_trailer_metadata_survives_conversion(
    update_trailer: bool,
    remove_trailer: bool,
) {
    let (registry, host) = host();
    let source = format!(
        r#"
maki.api.set_slot("tool.limited.output", function(prev, out, ctx)
    assert(out.trailer == "secret cancellation token")
    out.text = out.text:gsub("secret cancellation token", "[redacted cancellation token]")
    if {update_trailer} then
        out.trailer = "[redacted cancellation token]"
    end
    if {remove_trailer} then out.trailer = nil end
    return prev(out, ctx)
end)
"#
    );
    host.load_source("trailer_redactor", &source).unwrap();
    let mut gate = Gate::new();
    smol::block_on(async {
        let caller = dispatch(
            dispatch_ctx(&registry, &gate),
            "deferred",
            SECRET_TRAILER,
            false,
        );
        checked(gate.entered.recv_async()).await.unwrap();
        gate.release.release();
        let done = checked(caller).await;
        let output = done.output.as_text();
        assert!(!output.contains(SECRET_TRAILER), "{output}");
        if update_trailer {
            assert_eq!(gate.saved_bodies(), vec![BODY.to_owned()]);
            assert!(output.ends_with(REDACTED_TRAILER), "{output}");
        } else {
            assert_eq!(
                gate.saved_bodies(),
                vec![format!("{BODY}\n{REDACTED_TRAILER}")]
            );
            assert!(!output.contains(REDACTED_TRAILER), "{output}");
        }
    });
}

struct ReplacementHook {
    text: &'static str,
    trailer: Option<Value>,
    unchanged: bool,
}

impl ToolHook for ReplacementHook {
    fn wraps(&self, tool: &str, stage: HookStage) -> bool {
        tool == "limited" && stage == HookStage::Output
    }

    fn run<'a>(
        &'a self,
        _stage: HookStage,
        value: Value,
        _call: &'a HookCall<'a>,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            assert_eq!(value[OUTPUT_TRAILER], REPLACEMENT_TRAILER);
            if self.unchanged {
                return Verdict::Unchanged;
            }
            let mut replacement = json!({ OUTPUT_TEXT: self.text });
            if let Some(trailer) = &self.trailer {
                replacement[OUTPUT_TRAILER] = trailer.clone();
            }
            Verdict::Replaced(replacement)
        })
    }
}

struct ReleaseOnDrop(Option<flume::Sender<()>>);

impl ReleaseOnDrop {
    fn release(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone)]
struct GatedBackend {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
    created: flume::Sender<()>,
    operations: Arc<Mutex<Vec<&'static str>>>,
}

impl OffloadBackend for GatedBackend {
    fn matches(&self, name: &str, expected: &[u8]) -> io::Result<bool> {
        Ok(self
            .files
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|bytes| bytes == expected))
    }

    fn create_new(&self, name: &str, bytes: &[u8]) -> io::Result<bool> {
        let _ = self.entered.send(());
        let _ = self.release.recv();
        let mut files = self.files.lock().unwrap();
        let created = if files.contains_key(name) {
            false
        } else {
            files.insert(name.to_owned(), bytes.to_vec());
            true
        };
        self.operations.lock().unwrap().push("create");
        let _ = self.created.send(());
        Ok(created)
    }

    fn snapshot(&self) -> io::Result<OffloadSnapshot> {
        let files = self.files.lock().unwrap();
        Ok(OffloadSnapshot {
            names: files.keys().cloned().collect(),
            total_bytes: files.values().map(|bytes| bytes.len() as u64).sum(),
        })
    }

    fn remove_all(&self) -> io::Result<()> {
        self.operations.lock().unwrap().push("remove");
        self.files.lock().unwrap().clear();
        Ok(())
    }

    fn path(&self, name: &str) -> PathBuf {
        PathBuf::from(STORE_DIR).join(name)
    }
}

struct Gate {
    backend: GatedBackend,
    store: Arc<OffloadStore>,
    entered: flume::Receiver<()>,
    created: flume::Receiver<()>,
    release: ReleaseOnDrop,
}

impl Gate {
    fn new() -> Self {
        let (entered_tx, entered) = flume::unbounded();
        let (created_tx, created) = flume::unbounded();
        let (release_tx, release_rx) = flume::bounded(1);
        let backend = GatedBackend {
            files: Arc::new(Mutex::new(HashMap::new())),
            entered: entered_tx,
            release: release_rx,
            created: created_tx,
            operations: Arc::new(Mutex::new(Vec::new())),
        };
        Self {
            store: Arc::new(OffloadStore::new(Box::new(backend.clone()))),
            backend,
            entered,
            created,
            release: ReleaseOnDrop(Some(release_tx)),
        }
    }

    fn ctx(&self) -> ToolContext {
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.offload = Some(Arc::clone(&self.store));
        ctx
    }

    fn saved_bodies(&self) -> Vec<String> {
        self.backend
            .files
            .lock()
            .unwrap()
            .values()
            .map(|body| String::from_utf8(body.clone()).unwrap())
            .collect()
    }
}

fn host() -> (Arc<ToolRegistry>, PluginHost) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source("output_limit_scheduling", SOURCE).unwrap();
    (registry, host)
}

async fn checked<T>(future: impl Future<Output = T>) -> T {
    futures_lite::future::race(future, async {
        smol::Timer::after(WATCHDOG).await;
        panic!("output limit scheduling rendezvous timed out");
    })
    .await
}

fn start(
    registry: &ToolRegistry,
    ctx: ToolContext,
    mode: &str,
    body: &str,
) -> smol::Task<ToolExecResult> {
    start_with_error(registry, ctx, mode, body, true)
}

fn start_with_error(
    registry: &ToolRegistry,
    ctx: ToolContext,
    mode: &str,
    body: &str,
    is_error: bool,
) -> smol::Task<ToolExecResult> {
    let invocation = registry
        .get("limited")
        .unwrap()
        .tool
        .parse(&json!({
            "mode": mode, "body": body, "trailer": TRAILER, "is_error": is_error, "max_lines": 0,
        }))
        .unwrap();
    smol::spawn(async move { invocation.execute(&ctx).await })
}

fn dispatch_ctx(registry: &Arc<ToolRegistry>, gate: &Gate) -> ToolContext {
    let mut ctx = gate.ctx();
    ctx.registry = Arc::clone(registry);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(registry, &ctx.local_tools, None));
    ctx
}

fn dispatch(
    ctx: ToolContext,
    mode: &str,
    trailer: &str,
    is_error: bool,
) -> smol::Task<ToolDoneEvent> {
    let input = json!({
        "mode": mode, "body": BODY, "trailer": trailer, "is_error": is_error, "max_lines": 0,
    });
    smol::spawn(async move {
        tool_dispatch::run(
            DISPATCH_ID.to_owned(),
            "limited",
            &input,
            &ctx,
            CallOrigin::Nested,
        )
        .await
    })
}

async fn dispatched_sibling(ctx: &ToolContext) {
    let done = checked(tool_dispatch::run(
        DISPATCH_ID.to_owned(),
        "sibling",
        &json!({}),
        ctx,
        CallOrigin::Nested,
    ))
    .await;
    assert!(!done.is_error);
    assert_eq!(done.output.as_text(), SIBLING_REPLY);
}

async fn sibling(registry: &ToolRegistry, ctx: &ToolContext) {
    let invocation = registry
        .get("sibling")
        .unwrap()
        .tool
        .parse(&json!({}))
        .unwrap();
    let reply = checked(invocation.execute(ctx)).await;
    assert_eq!(text(reply.output), Ok(SIBLING_REPLY.to_owned()));
}

fn text(output: Result<ToolOutput, String>) -> Result<String, String> {
    output.map(|output| match output {
        ToolOutput::Plain(output) => output.text,
        other => panic!("expected plain output, got {other:?}"),
    })
}

async fn flash(actions: &flume::Receiver<UiAction>, expected: &str) {
    loop {
        if let UiAction::Flash(actual) = checked(actions.recv_async()).await.unwrap() {
            assert_eq!(actual, expected);
            return;
        }
    }
}

#[test_case("async"; "ctx_limit_output")]
#[test_case("deferred"; "direct_reply")]
#[test_case("finish"; "ctx_finish")]
fn blocked_output_creation_keeps_same_host_sibling_responsive(mode: &str) {
    let (registry, _host) = host();
    let mut gate = Gate::new();
    smol::block_on(async {
        let worker = start(&registry, gate.ctx(), mode, BODY);
        checked(gate.entered.recv_async()).await.unwrap();
        sibling(&registry, &gate.ctx()).await;
        assert!(gate.saved_bodies().is_empty());
        gate.release.release();
        let reply = text(checked(worker).await.output);
        let output = if mode == "async" {
            reply.unwrap()
        } else {
            reply.unwrap_err()
        };
        assert!(output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
        assert!(output.ends_with(TRAILER), "{output}");
        assert_eq!(gate.saved_bodies(), [BODY]);
    });
}

#[test]
fn output_store_lock_wait_keeps_same_host_sibling_responsive() {
    let (registry, host) = host();
    let actions = host.ui_action_rx();
    let mut gate = Gate::new();
    smol::block_on(async {
        let first = start(&registry, gate.ctx(), "async", BODY);
        flash(&actions, "async:ready").await;
        checked(gate.entered.recv_async()).await.unwrap();
        let second = start(&registry, gate.ctx(), "async", BODY);
        flash(&actions, "async:ready").await;
        sibling(&registry, &gate.ctx()).await;
        assert!(
            gate.entered.is_empty(),
            "second create must wait on the operation lock"
        );
        gate.release.release();
        assert!(
            text(checked(first).await.output)
                .unwrap()
                .contains(OFFLOAD_FOOTER_PREFIX)
        );
        assert!(
            text(checked(second).await.output)
                .unwrap()
                .contains(OFFLOAD_POINTER_PREFIX)
        );
        assert_eq!(gate.saved_bodies(), [BODY]);
    });
}

#[test]
fn cancel_hook_defers_limits_and_releases_ctx_borrow() {
    let (registry, host) = host();
    let actions = host.ui_action_rx();
    let mut gate = Gate::new();
    let (trigger, cancel) = CancelToken::new();
    let mut ctx = gate.ctx();
    ctx.cancel = cancel;
    smol::block_on(async {
        let worker = start(&registry, ctx, "cancel", BODY);
        flash(&actions, "cancel:ready").await;
        checked(gate.entered.recv_async()).await.unwrap();
        trigger.cancel();
        flash(&actions, "hook:finished").await;
        sibling(&registry, &gate.ctx()).await;
        gate.release.release();
        let output = text(checked(worker).await.output).unwrap_err();
        assert!(output.contains(OFFLOAD_POINTER_PREFIX), "{output}");
        assert!(output.ends_with(TRAILER), "{output}");
        assert_eq!(gate.saved_bodies(), [BODY]);
    });
}

#[test_case("async"; "ctx_limit_output")]
#[test_case("deferred"; "direct_reply")]
#[test_case("finish"; "ctx_finish")]
fn hooked_dispatch_keeps_sibling_responsive_during_finalization(mode: &str) {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    registry.set_hook(OutputHook(seen_tx));
    let mut gate = Gate::new();
    smol::block_on(async {
        let ctx = dispatch_ctx(&registry, &gate);
        let caller = dispatch(ctx.clone(), mode, TRAILER, true);
        checked(gate.entered.recv_async()).await.unwrap();
        assert_eq!(
            checked(seen.recv_async()).await.unwrap(),
            format!("{BODY}\n{TRAILER}")
        );
        dispatched_sibling(&ctx).await;
        assert!(gate.saved_bodies().is_empty());
        gate.release.release();
        let done = checked(caller).await;
        assert_eq!(done.is_error, mode != "async");
        let output = done.output.as_text();
        assert!(output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
    });
}

#[test_case("async", CallOrigin::Nested; "immediate_nested")]
#[test_case("deferred", CallOrigin::Nested; "deferred_nested")]
#[test_case("finish", CallOrigin::Nested; "finish_nested")]
#[test_case("async", CallOrigin::Model; "immediate_model")]
#[test_case("deferred", CallOrigin::Model; "deferred_model")]
#[test_case("finish", CallOrigin::Model; "finish_model")]
fn cancellation_interrupts_filtered_output_persistence(mode: &str, origin: CallOrigin) {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    registry.set_hook(OutputHook(seen_tx));
    let mut gate = Gate::new();
    smol::block_on(async {
        let mut ctx = dispatch_ctx(&registry, &gate);
        ctx.deadline = Deadline::None;
        let (trigger, cancel) = CancelToken::new();
        ctx.cancel = cancel;
        let input = json!({
            "mode": mode, "body": BODY, "trailer": SECRET_TRAILER,
            "is_error": false, "max_lines": 0,
        });
        let caller = smol::spawn(async move {
            tool_dispatch::run(DISPATCH_ID.to_owned(), "limited", &input, &ctx, origin).await
        });
        checked(gate.entered.recv_async()).await.unwrap();
        assert_eq!(
            checked(seen.recv_async()).await.unwrap(),
            format!("{BODY}\n{SECRET_TRAILER}")
        );
        trigger.cancel();
        let done = checked(caller).await;
        let output = done.output.as_text();
        assert!(done.is_error, "{output}");
        assert!(!output.contains(BODY), "{output}");
        assert!(!output.contains(SECRET_TRAILER), "{output}");
        assert!(!output.contains(OFFLOAD_POINTER_PREFIX), "{output}");
        assert!(!output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
        assert!(gate.saved_bodies().is_empty());
        dispatched_sibling(&dispatch_ctx(&registry, &gate)).await;
        gate.release.release();
        checked(gate.created.recv_async()).await.unwrap();
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
        assert!(!saved[0].contains(SECRET_TRAILER));
    });
}

#[test_case("async", CallOrigin::Nested, false; "immediate_nested")]
#[test_case("deferred", CallOrigin::Nested, false; "deferred_nested")]
#[test_case("finish", CallOrigin::Nested, false; "finish_nested")]
#[test_case("async", CallOrigin::Model, false; "immediate_model")]
#[test_case("deferred", CallOrigin::Model, false; "deferred_model")]
#[test_case("finish", CallOrigin::Model, false; "finish_model")]
#[test_case("deferred", CallOrigin::Nested, true; "with_deadline")]
fn cancellation_interrupts_unhooked_output_persistence(
    mode: &str,
    origin: CallOrigin,
    deadline: bool,
) {
    let (registry, _host) = host();
    let mut gate = Gate::new();
    smol::block_on(async {
        let mut ctx = dispatch_ctx(&registry, &gate);
        ctx.deadline = if deadline {
            Deadline::after(WATCHDOG)
        } else {
            Deadline::None
        };
        let (trigger, cancel) = CancelToken::new();
        ctx.cancel = cancel;
        let input = json!({
            "mode": mode, "body": BODY, "trailer": TRAILER,
            "is_error": false, "max_lines": 0,
        });
        let caller = smol::spawn(async move {
            tool_dispatch::run(DISPATCH_ID.to_owned(), "limited", &input, &ctx, origin).await
        });
        checked(gate.entered.recv_async()).await.unwrap();
        trigger.cancel();
        let done = checked(caller).await;
        assert!(done.is_error);
        let output = done.output.as_text();
        assert!(output.ends_with(CANCELLED_ERROR), "{output}");
        assert!(!output.contains(BODY), "{output}");
        assert!(!output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
        assert!(gate.saved_bodies().is_empty());
        dispatched_sibling(&dispatch_ctx(&registry, &gate)).await;
        gate.release.release();
        checked(gate.created.recv_async()).await.unwrap();
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
    });
}

#[test_case(false, false; "registered_timeout_string")]
#[test_case(false, true; "registered_timeout_table")]
#[test_case(true, false; "dynamic_deadline_string")]
#[test_case(true, true; "dynamic_deadline_table")]
fn plain_lua_reply_deadline_bounds_lua_output_hook(dynamic: bool, table: bool) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    let actions = host.ui_action_rx();
    let timeout = if dynamic {
        0
    } else {
        FINALIZATION_TIMEOUT.as_secs()
    };
    let deadline = if dynamic { "ctx:set_deadline(1)" } else { "" };
    let reply = if table {
        "{ llm_output = input.body }"
    } else {
        "input.body"
    };
    host.load_source(
        "plain_reply_deadline",
        &format!(
            r#"
        maki.api.register_tool({{
            name = "limited", timeout = {timeout}, description = "plain reply deadline",
            schema = {{ type = "object", properties = {{ body = {{ type = "string" }} }} }},
            handler = function(input, ctx)
                {deadline}
                return {reply}
            end,
        }})
        maki.api.set_slot("tool.limited.output", function(prev, out, ctx)
            maki.ui.flash("output-hook:entered")
            maki.async.await(1, function(callback) end)
            return prev(out, ctx)
        end)
        "#
        ),
    )
    .unwrap();
    let gate = Gate::new();
    smol::block_on(async {
        let caller = dispatch(dispatch_ctx(&registry, &gate), "plain", TRAILER, false);
        flash(&actions, "output-hook:entered").await;
        let done = checked(caller).await;
        let output = done.output.as_text();
        assert!(done.is_error, "{output}");
        assert!(!output.contains(BODY), "{output}");
        assert!(
            output.contains(TIMEOUT_FRAGMENT) || output.contains(DEADLINE_FRAGMENT),
            "{output}"
        );
        assert!(gate.saved_bodies().is_empty());
        assert!(gate.entered.try_recv().is_err());
    });
}

#[test_case("deferred", false; "direct_reply_absolute_deadline")]
#[test_case("finish", false; "ctx_finish_absolute_deadline")]
#[test_case("deferred", true; "direct_reply_tool_timeout")]
#[test_case("finish", true; "ctx_finish_tool_timeout")]
fn hooked_dispatch_finalization_obeys_timeout(mode: &str, tool_timeout: bool) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    let source = if tool_timeout {
        SOURCE.replacen(
            "name = \"limited\",",
            &format!(
                "name = \"limited\", timeout = {},",
                FINALIZATION_TIMEOUT.as_secs()
            ),
            1,
        )
    } else {
        SOURCE.to_owned()
    };
    host.load_source("output_limit_scheduling", &source)
        .unwrap();
    let (seen_tx, seen) = flume::unbounded();
    registry.set_hook(OutputHook(seen_tx));
    let mut gate = Gate::new();
    smol::block_on(async {
        let mut ctx = dispatch_ctx(&registry, &gate);
        if !tool_timeout {
            ctx.deadline = Deadline::after(FINALIZATION_TIMEOUT);
        }
        let caller = dispatch(ctx, mode, TRAILER, false);
        checked(gate.entered.recv_async()).await.unwrap();
        assert_eq!(
            checked(seen.recv_async()).await.unwrap(),
            format!("{BODY}\n{TRAILER}")
        );
        let done = checked(caller).await;
        assert!(done.is_error);
        let error = done.output.as_text();
        assert!(
            error.contains(TIMEOUT_FRAGMENT) || error.contains(DEADLINE_FRAGMENT),
            "{error}"
        );
        assert!(gate.saved_bodies().is_empty());
        dispatched_sibling(&dispatch_ctx(&registry, &gate)).await;
        gate.release.release();
        checked(gate.created.recv_async()).await.unwrap();
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
    });
}

#[test_case("deferred", false; "direct_reply_execute")]
#[test_case("finish", false; "ctx_finish_execute")]
#[test_case("deferred", true; "direct_reply_hooked_dispatch")]
#[test_case("finish", true; "ctx_finish_hooked_dispatch")]
fn handler_deadline_bounds_blocked_finalization(mode: &str, hooked: bool) {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    if hooked {
        registry.set_hook(OutputHook(seen_tx));
    }
    let mut gate = Gate::new();
    smol::block_on(async {
        let mut ctx = dispatch_ctx(&registry, &gate);
        ctx.deadline = Deadline::None;
        let input = json!({
            "mode": mode, "body": BODY, "trailer": TRAILER, "max_lines": 0,
            "deadline": FINALIZATION_TIMEOUT.as_secs(),
        });
        let invocation = registry.get("limited").unwrap().tool.parse(&input).unwrap();
        let caller = smol::spawn(async move {
            if hooked {
                let done = tool_dispatch::run(
                    DISPATCH_ID.to_owned(),
                    "limited",
                    &input,
                    &ctx,
                    CallOrigin::Nested,
                )
                .await;
                (done.is_error, done.output.as_text())
            } else {
                let result = invocation.execute(&ctx).await;
                let is_error = result.output.is_err();
                let body = match text(result.output) {
                    Ok(body) | Err(body) => body,
                };
                (is_error, body)
            }
        });
        checked(gate.entered.recv_async()).await.unwrap();
        if hooked {
            assert_eq!(
                checked(seen.recv_async()).await.unwrap(),
                format!("{BODY}\n{TRAILER}")
            );
        }
        let (is_error, error) = checked(caller).await;
        assert!(is_error, "{error}");
        assert!(
            error.contains(TIMEOUT_FRAGMENT) || error.contains(DEADLINE_FRAGMENT),
            "{error}"
        );
        assert!(gate.saved_bodies().is_empty());
        gate.release.release();
        checked(gate.created.recv_async()).await.unwrap();
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
    });
}

#[test_case("async", false; "immediate_success")]
#[test_case("deferred", false; "deferred_success")]
#[test_case("deferred", true; "deferred_error")]
#[test_case("finish", true; "finish_error")]
fn output_hook_redacts_trailer_before_offloading(mode: &str, is_error: bool) {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    registry.set_hook(OutputHook(seen_tx));
    let mut gate = Gate::new();
    smol::block_on(async {
        let caller = dispatch(
            dispatch_ctx(&registry, &gate),
            mode,
            SECRET_TRAILER,
            is_error,
        );
        checked(gate.entered.recv_async()).await.unwrap();
        assert_eq!(
            checked(seen.recv_async()).await.unwrap(),
            format!("{BODY}\n{SECRET_TRAILER}")
        );
        gate.release.release();
        let done = checked(caller).await;
        assert_eq!(done.is_error, is_error);
        let output = done.output.as_text();
        assert!(output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
        assert!(!output.contains(SECRET_TRAILER), "{output}");
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
        assert!(output.ends_with(REDACTED_TRAILER), "{output}");
        assert!(!saved[0].contains(SECRET_TRAILER), "{}", saved[0]);
    });
}

#[test_case("async", 0; "immediate_zero")]
#[test_case("async", 1; "immediate_tiny")]
#[test_case("deferred", 0; "deferred_zero")]
#[test_case("deferred", 1; "deferred_tiny")]
fn hooked_trailers_and_labels_survive_fresh_and_deduplicated_limits(mode: &str, budget: usize) {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    registry.set_hook(OutputHook(seen_tx));
    let mut gate = Gate::new();
    smol::block_on(async {
        for notice in [OFFLOAD_FOOTER_PREFIX, OFFLOAD_POINTER_PREFIX] {
            let ctx = dispatch_ctx(&registry, &gate);
            let input = json!({
                "mode": mode, "body": format!("{BODY}\n{SECRET_TRAILER}"),
                "trailer": EXIT_TRAILER, "is_error": true,
                "label": SECRET_LABEL, "max_lines": budget, "max_bytes": budget,
            });
            let caller = smol::spawn(async move {
                tool_dispatch::run(
                    DISPATCH_ID.to_owned(),
                    "limited",
                    &input,
                    &ctx,
                    CallOrigin::Nested,
                )
                .await
            });
            checked(seen.recv_async()).await.unwrap();
            if notice == OFFLOAD_FOOTER_PREFIX {
                checked(gate.entered.recv_async()).await.unwrap();
                gate.release.release();
            }
            let done = checked(caller).await;
            let output = done.output.as_text();
            assert!(output.contains(notice), "{output}");
            assert!(output.ends_with(EXIT_TRAILER), "{output}");
            assert!(!output.contains(SECRET_TRAILER), "{output}");
            assert!(!output.contains(SECRET_LABEL), "{output}");
            if mode == "deferred" {
                assert!(done.is_error);
            }
        }
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(REDACTED_TRAILER));
        assert!(!saved[0].contains(SECRET_TRAILER));
    });
}

#[test_case("deferred"; "direct_reply")]
#[test_case("finish"; "ctx_finish")]
fn deferred_finalization_obeys_timeout_without_cancelling_started_worker(mode: &str) {
    let (registry, _host) = host();
    let mut gate = Gate::new();
    smol::block_on(async {
        let mut ctx = gate.ctx();
        ctx.deadline = Deadline::after(FINALIZATION_TIMEOUT);
        let caller = start(&registry, ctx, mode, BODY);
        checked(gate.entered.recv_async()).await.unwrap();
        assert_eq!(
            text(checked(caller).await.output),
            Err(TIMEOUT_ERROR.to_owned())
        );
        assert!(gate.saved_bodies().is_empty());
        gate.release.release();
        checked(gate.created.recv_async()).await.unwrap();
        assert_eq!(gate.saved_bodies(), [BODY]);
    });
}

#[test_case("async"; "ctx_worker")]
#[test_case("deferred"; "reply_worker")]
fn deferred_limit_started_worker_survives_caller_drop(mode: &str) {
    let (registry, _host) = host();
    let mut gate = Gate::new();
    smol::block_on(async {
        let caller = start(&registry, gate.ctx(), mode, BODY);
        checked(gate.entered.recv_async()).await.unwrap();
        checked(caller.cancel()).await;
        sibling(&registry, &gate.ctx()).await;
        gate.release.release();
        checked(gate.created.recv_async()).await.unwrap();
        assert_eq!(gate.saved_bodies(), [BODY]);
        let store = Arc::clone(&gate.store);
        smol::unblock(move || store.put(BODY)).await.unwrap();
    });
}

#[test_case("async"; "ctx_worker")]
#[test_case("deferred"; "reply_worker")]
fn deferred_caller_drop_cleanup_removes_late_artifact(mode: &str) {
    let (registry, _host) = host();
    let mut gate = Gate::new();
    let cleanup = OffloadCleanup::new(Arc::clone(&gate.store));
    smol::block_on(async {
        let caller = start(&registry, gate.ctx(), mode, BODY);
        checked(gate.entered.recv_async()).await.unwrap();
        checked(caller.cancel()).await;
        cleanup.request();
        let store = Arc::clone(&gate.store);
        assert!(smol::unblock(move || store.put(OTHER_BODY)).await.is_err());
        sibling(&registry, &gate.ctx()).await;
        gate.release.release();
        checked(cleanup.wait()).await.unwrap();
        assert_eq!(
            *gate.backend.operations.lock().unwrap(),
            ["create", "remove"]
        );
        assert!(gate.saved_bodies().is_empty());
        let store = Arc::clone(&gate.store);
        assert!(smol::unblock(move || store.put(BODY)).await.is_err());
        assert!(gate.saved_bodies().is_empty());
    });
}

#[test_case(false; "success")]
#[test_case(true; "error")]
fn content_alias_limits_preserve_result_kind_and_store_raw_body(is_error: bool) {
    let (registry, _host) = host();
    smol::block_on(async {
        for mode in ["content_deferred", "content_finish"] {
            let mut gate = Gate::new();
            let worker = start_with_error(&registry, gate.ctx(), mode, BODY, is_error);
            gate.release.release();
            checked(gate.entered.recv_async()).await.unwrap();
            let result = checked(worker).await;
            assert_eq!(result.output.is_err(), is_error, "{mode}");
            let body = match text(result.output) {
                Ok(body) | Err(body) => body,
            };
            assert!(body.contains(OFFLOAD_FOOTER_PREFIX), "{body}");
            assert_eq!(gate.saved_bodies(), [BODY], "{mode}");
            assert!(gate.backend.operations.lock().unwrap().contains(&"create"));
        }
    });
}

#[test_case(false; "success")]
#[test_case(true; "error")]
fn deferred_limits_preserve_result_kind_and_use_context_defaults(is_error: bool) {
    let (registry, _host) = host();
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.config.max_output_lines = 0;
    ctx.config.max_output_bytes = 0;
    let invocation = registry
        .get("limited")
        .unwrap()
        .tool
        .parse(&json!({
            "mode": "deferred", "body": BODY, "is_error": is_error,
        }))
        .unwrap();
    let reply = smol::block_on(invocation.execute(&ctx));
    assert_eq!(reply.output.is_err(), is_error);
    let output = match text(reply.output) {
        Ok(body) | Err(body) => body,
    };
    assert_eq!(output, maki_agent::tools::FILE_TRUNCATED_MARKER);
}

#[test_case(json!({"output_limits": false}); "non_table")]
#[test_case(json!({"output_limits": {"preview": "middle"}}); "invalid_preview")]
#[test_case(json!({"output_limits": {"max_lines": -1}}); "negative_limit")]
#[test_case(json!({"output_limits": {}, "llm_output": {"nested": true}}); "structured_output")]
#[test_case(json!({"output_limits": {}, "image": {"media_type": "image/png", "data": "aGVsbG8="}}); "image")]
#[test_case(json!({"output_limits": {}, "diff_path": "file"}); "diff")]
#[test_case(json!({"output_limits": {}, "state": {"nested": true}}); "structured_state")]
#[test_case(json!({"output_limits": {}, "format": "markdown"}); "markdown")]
fn invalid_deferred_output_limits_are_rejected(fields: Value) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(
        "invalid_limits",
        r#"
        maki.api.register_tool({
            name = "invalid_limits", description = "returns its input",
            schema = { type = "object", properties = { reply = { type = "string" } } },
            handler = function(input)
                local reply = maki.json.decode(input.reply)
                if reply.llm_output == nil then reply.llm_output = "body" end
                return reply
            end,
        })
    "#,
    )
    .unwrap();
    let invocation = registry
        .get("invalid_limits")
        .unwrap()
        .tool
        .parse(&json!({ "reply": fields.to_string() }))
        .unwrap();
    let error = smol::block_on(invocation.execute(&stub_ctx(&AgentMode::Build)))
        .output
        .unwrap_err();
    assert!(error.starts_with("output_limits:"), "{error}");
}
