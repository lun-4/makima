use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maki_agent::cancel::CancelToken;
use maki_agent::tools::offload::{
    OFFLOAD_FOOTER_PREFIX, OFFLOAD_POINTER_PREFIX, OffloadBackend, OffloadCleanup, OffloadSnapshot,
    OffloadStore,
};
use maki_agent::tools::test_support::stub_ctx;
use maki_agent::tools::{ToolContext, ToolExecResult, ToolRegistry};
use maki_agent::{AgentMode, ToolOutput};
use maki_lua::{PluginHost, UiAction};
use serde_json::{Value, json};
use test_case::test_case;

const WATCHDOG: Duration = Duration::from_secs(10);
const BODY: &str = "first\nsecond\nthird\nfourth";
const OTHER_BODY: &str = "other\nsecond\nthird\nfourth";
const TRAILER: &str = "[cancelled by user; output above is partial]";
const SIBLING_REPLY: &str = "sibling completed";
const STORE_DIR: &str = "/output-limit-test";
const SOURCE: &str = r#"
maki.api.register_tool({
    name = "limited", description = "limits output", schema = {
        type = "object", properties = {
            mode = { type = "string" }, body = { type = "string" },
            trailer = { type = "string" }, is_error = { type = "boolean" }, max_lines = { type = "integer" },
        },
    },
    handler = function(input, ctx)
        maki.ui.flash(input.mode .. ":ready")
        local limits = { max_lines = input.max_lines, trailer = input.trailer }
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
