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
use maki_agent::tools::offload::{OFFLOAD_FOOTER_PREFIX, OffloadBackend, OffloadStore};
use maki_agent::tools::registry::BoxFuture;
use maki_agent::tools::test_support::stub_ctx;
use maki_agent::tools::{CallOrigin, Deadline, ToolContext, ToolRegistry, TurnToolBindings};
use maki_agent::{AgentMode, ToolDoneEvent};
use maki_lua::{PluginHost, UiAction};
use serde_json::{Value, json};
use test_case::test_case;

const WATCHDOG: Duration = Duration::from_secs(10);
const FINALIZATION_TIMEOUT: Duration = Duration::from_secs(1);
const BODY: &str = "first\nsecond\nthird\nfourth";
const OTHER_BODY: &str = "other\nsecond\nthird\nfourth";
const TRAILER: &str = "[cancelled by user; output above is partial]";
const SECRET_TRAILER: &str = "secret cancellation token";
const REDACTED_TRAILER: &str = "[redacted cancellation token]";
const SIBLING_REPLY: &str = "sibling completed";
const STORE_DIR: &str = "/output-limit-test";
const DISPATCH_ID: &str = "output-limit-dispatch";
const CANCELLED_ERROR: &str = "cancelled";

const SOURCE: &str = r#"
maki.api.register_tool({
    name = "limited", description = "limits output", schema = {
        type = "object", properties = {
            mode = { type = "string" }, body = { type = "string" },
            trailer = { type = "string" }, is_error = { type = "boolean" },
            deadline = { type = "integer" },
        },
    },
    handler = function(input, ctx)
        if input.deadline then ctx:set_deadline(input.deadline) end
        maki.ui.flash(input.mode .. ":ready")
        local reply = {
            llm_output = input.body,
            output_limits = { max_lines = 0, trailer = input.trailer },
            is_error = input.is_error,
        }
        if input.mode == "finish" then
            ctx:finish(reply)
            return nil
        end
        if input.mode == "cancel_finish" then
            maki.async.on_cancel(function()
                ctx:finish({ llm_output = input.body,
                    output_limits = { max_lines = 0, trailer = input.trailer }, is_error = true })
                maki.ui.flash("cancel:finished")
            end)
            maki.async.await(1, function(callback) end)
            return nil
        end
        return reply
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

#[derive(Clone)]
struct GatedBackend {
    files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
    created: flume::Sender<()>,
}

impl OffloadBackend for GatedBackend {
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
        let _ = self.created.send(());
        Ok(created)
    }

    fn total_bytes(&self) -> io::Result<u64> {
        Ok(self
            .files
            .lock()
            .unwrap()
            .values()
            .map(|bytes| bytes.len() as u64)
            .sum())
    }

    fn remove_all(&self) -> io::Result<()> {
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
    release: flume::Sender<()>,
}

impl Gate {
    fn new() -> Self {
        let (entered_tx, entered) = flume::unbounded();
        let (created_tx, created) = flume::unbounded();
        let (release, release_rx) = flume::unbounded();
        let backend = GatedBackend {
            files: Arc::new(Mutex::new(HashMap::new())),
            entered: entered_tx,
            release: release_rx,
            created: created_tx,
        };
        Self {
            store: Arc::new(OffloadStore::new(Box::new(backend.clone()))),
            backend,
            entered,
            created,
            release,
        }
    }

    fn ctx(&self) -> ToolContext {
        let mut ctx = stub_ctx(&AgentMode::Build);
        ctx.offload = Some(Arc::clone(&self.store));
        ctx
    }

    fn release(&self) {
        let _ = self.release.send(());
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
        panic!("output finalization rendezvous timed out");
    })
    .await
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
    body: &str,
    trailer: &str,
    is_error: bool,
    deadline: Option<u64>,
) -> smol::Task<ToolDoneEvent> {
    let input = json!({
        "mode": mode,
        "body": body,
        "trailer": trailer,
        "is_error": is_error,
        "deadline": deadline,
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

async fn sibling(ctx: &ToolContext) {
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

async fn flash(actions: &flume::Receiver<UiAction>, expected: &str) {
    loop {
        if let UiAction::Flash(actual) = checked(actions.recv_async()).await.unwrap() {
            assert_eq!(actual, expected);
            return;
        }
    }
}

fn assert_offloaded(output: &str) {
    assert!(output.contains(OFFLOAD_FOOTER_PREFIX), "{output}");
}

fn assert_timeout(output: &str) {
    assert!(
        output.contains("timeout") || output.contains("deadline exceeded"),
        "{output}"
    );
}

#[test]
fn blocked_store_and_store_lock_leave_same_host_sibling_responsive() {
    let (registry, host) = host();
    let actions = host.ui_action_rx();
    let gate = Gate::new();
    smol::block_on(async {
        let ctx = dispatch_ctx(&registry, &gate);
        let first = dispatch(ctx.clone(), "return", BODY, TRAILER, false, None);
        flash(&actions, "return:ready").await;
        checked(gate.entered.recv_async()).await.unwrap();

        let second = dispatch(ctx.clone(), "return", OTHER_BODY, TRAILER, false, None);
        flash(&actions, "return:ready").await;
        sibling(&ctx).await;
        assert!(
            gate.entered.is_empty(),
            "the second save waits for the store lock"
        );

        gate.release();
        let first_done = checked(first).await;
        assert!(!first_done.is_error);
        assert_offloaded(&first_done.output.as_text());
        checked(gate.entered.recv_async()).await.unwrap();
        gate.release();
        let second_done = checked(second).await;
        assert!(!second_done.is_error);
        assert_offloaded(&second_done.output.as_text());

        let mut saved = gate.saved_bodies();
        saved.sort();
        let mut expected = vec![BODY.to_owned(), OTHER_BODY.to_owned()];
        expected.sort();
        assert_eq!(saved, expected);
    });
}

#[test]
fn output_hook_redacts_terminal_trailer_before_offloading() {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    registry.set_hook(OutputHook(seen_tx));
    let gate = Gate::new();
    smol::block_on(async {
        let caller = dispatch(
            dispatch_ctx(&registry, &gate),
            "return",
            BODY,
            SECRET_TRAILER,
            false,
            None,
        );
        checked(gate.entered.recv_async()).await.unwrap();
        assert_eq!(
            checked(seen.recv_async()).await.unwrap(),
            format!("{BODY}\n{SECRET_TRAILER}")
        );
        gate.release();
        let done = checked(caller).await;
        let output = done.output.as_text();
        assert!(!done.is_error, "{output}");
        assert_offloaded(&output);
        assert!(!output.contains(SECRET_TRAILER), "{output}");
        assert!(output.ends_with(REDACTED_TRAILER), "{output}");
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
        assert!(!saved[0].contains(SECRET_TRAILER), "{}", saved[0]);
    });
}

#[test_case(false; "without_output_hook")]
#[test_case(true; "with_output_hook")]
fn cancellation_returns_while_store_worker_is_blocked(hooked: bool) {
    let (registry, _host) = host();
    let (seen_tx, seen) = flume::unbounded();
    if hooked {
        registry.set_hook(OutputHook(seen_tx));
    }
    let gate = Gate::new();
    let (trigger, cancel) = CancelToken::new();
    let mut ctx = dispatch_ctx(&registry, &gate);
    ctx.cancel = cancel;
    smol::block_on(async {
        let caller = dispatch(ctx.clone(), "return", BODY, SECRET_TRAILER, false, None);
        checked(gate.entered.recv_async()).await.unwrap();
        if hooked {
            assert_eq!(
                checked(seen.recv_async()).await.unwrap(),
                format!("{BODY}\n{SECRET_TRAILER}")
            );
        }
        trigger.cancel();
        let done = checked(caller).await;
        let output = done.output.as_text();
        assert!(done.is_error, "{output}");
        assert!(output.ends_with(CANCELLED_ERROR), "{output}");
        assert!(gate.saved_bodies().is_empty());

        sibling(&ctx).await;
        gate.release();
        checked(gate.created.recv_async()).await.unwrap();
        let saved = gate.saved_bodies();
        assert_eq!(saved.len(), 1);
        assert!(saved[0].contains(BODY));
    });
}

#[test]
fn synchronous_cancel_callback_can_finish_with_output_limits() {
    let (registry, host) = host();
    let actions = host.ui_action_rx();
    let gate = Gate::new();
    let (trigger, cancel) = CancelToken::new();
    let mut ctx = dispatch_ctx(&registry, &gate);
    ctx.cancel = cancel;
    smol::block_on(async {
        let caller = dispatch(ctx.clone(), "cancel_finish", BODY, TRAILER, true, None);
        flash(&actions, "cancel_finish:ready").await;
        trigger.cancel();
        flash(&actions, "cancel:finished").await;
        checked(gate.entered.recv_async()).await.unwrap();
        sibling(&ctx).await;
        gate.release();
        let done = checked(caller).await;
        let output = done.output.as_text();
        assert!(done.is_error, "{output}");
        assert_offloaded(&output);
        assert!(output.ends_with(TRAILER), "{output}");
        assert_eq!(gate.saved_bodies(), [BODY]);
    });
}

async fn finalization_times_out(caller: smol::Task<ToolDoneEvent>, gate: &Gate) {
    checked(gate.entered.recv_async()).await.unwrap();
    let done = checked(caller).await;
    let output = done.output.as_text();
    assert!(done.is_error, "{output}");
    assert_timeout(&output);
    assert!(gate.saved_bodies().is_empty());
    gate.release();
    checked(gate.created.recv_async()).await.unwrap();
    assert_eq!(gate.saved_bodies(), [BODY]);
}

#[test]
fn caller_deadline_bounds_output_finalization() {
    let (registry, _host) = host();
    let gate = Gate::new();
    let mut ctx = dispatch_ctx(&registry, &gate);
    ctx.deadline = Deadline::after(FINALIZATION_TIMEOUT);
    let caller = dispatch(ctx, "return", BODY, TRAILER, false, None);
    smol::block_on(finalization_times_out(caller, &gate));
}

#[test]
fn handler_set_deadline_bounds_output_finalization() {
    let (registry, _host) = host();
    let gate = Gate::new();
    let mut ctx = dispatch_ctx(&registry, &gate);
    ctx.deadline = Deadline::None;
    let caller = dispatch(
        ctx,
        "return",
        BODY,
        TRAILER,
        false,
        Some(FINALIZATION_TIMEOUT.as_secs()),
    );
    smol::block_on(finalization_times_out(caller, &gate));
}

#[test_case(json!({"output_limits": false}); "non_table_limits")]
#[test_case(json!({"output_limits": {"preview": "middle"}}); "invalid_preview")]
#[test_case(json!({"output_limits": {"max_lines": -1}}); "negative_limit")]
#[test_case(json!({"output_limits": {}, "instructions": [{"path": "secret.txt", "content": "secret"}]}); "instructions")]
#[test_case(json!({"output_limits": {}, "image": {"media_type": "image/png", "data": "aGVsbG8="}}); "image")]
#[test_case(json!({"output_limits": {}, "state": {"nested": true}}); "state")]
fn malformed_limits_and_unfilterable_sidecars_are_rejected(fields: Value) {
    let registry = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&registry)).unwrap();
    host.load_source(
        "invalid_limits",
        r#"
        maki.api.register_tool({
            name = "invalid_limits", description = "returns a reply",
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
