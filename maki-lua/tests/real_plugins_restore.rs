//! Exercises real plugins (bash, grep, batch) through `request_restore`.
//! A broken restore silently falls back to raw LLM output, so we assert
//! things only the real views produce (gutters, command headers, truncation).

use std::path::PathBuf;
use std::sync::Arc;

use maki_agent::AgentEvent;
use maki_agent::ToolOutput;
use maki_agent::permissions::PermissionManager;
use maki_agent::permissions::{DEFAULT_DENY_GUIDANCE, PERMISSION_DENIED_PREFIX};
use maki_agent::tools::ToolRegistry;
use maki_config::{
    DefaultEffect, Effect, PermissionRule, PermissionsConfig, ToolKey, ToolOutputLines,
};
use maki_lua::PluginHost;
use maki_providers::StreamResponse;
use serde_json::{Value, json};

mod common;

const BASH_SRC: &str = include_str!("../../plugins/bash/init.lua");
const GREP_SRC: &str = include_str!("../../plugins/grep/init.lua");
const BATCH_SRC: &str = include_str!("../../plugins/batch/init.lua");

/// Only the real ToolView emits this when collapsed.
const EXPAND_HINT: &str = "click to expand";
/// Fixed caps so truncation tests don't depend on the product defaults. The
/// index and read caps differ so a body rendered through the wrong view is
/// visibly different.
const VIEW_CAP: usize = 3;
const INDEX_VIEW_CAP: usize = 2;
const READ_VIEW_CAP: usize = 5;

fn view_lines() -> ToolOutputLines {
    ToolOutputLines {
        other: VIEW_CAP,
        index: INDEX_VIEW_CAP,
        read: READ_VIEW_CAP,
        ..ToolOutputLines::DEFAULT
    }
}

const GREP_OUT: &str =
    "src/a.rs:\n  1: fn main() {}\n  2: fn helper() {}\n\nsrc/b.rs:\n  10: fn other() {}";

const BATCH_INPUT_GREP_BASH: &str = r#"{ "tool_calls": [
    { "tool": "grep", "parameters": { "pattern": "fn" } },
    { "tool": "bash", "parameters": { "command": "echo hello-from-bash" } }
]}"#;

fn load_host() -> PluginHost {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("bash", BASH_SRC).unwrap();
    host.load_source("grep", GREP_SRC).unwrap();
    host.load_source("batch", BATCH_SRC).unwrap();
    host
}

fn batch_state() -> Value {
    json!({ "children": [
        { "tool": "grep", "status": "success", "output": GREP_OUT },
        { "tool": "bash", "status": "success", "output": "hello-from-bash" },
    ]})
}

struct Restored {
    body: String,
    header: String,
}

fn restore(
    host: &PluginHost,
    tool: &str,
    input: Value,
    output: &str,
    state: Option<Value>,
    clicks: Vec<usize>,
) -> Restored {
    let handle = host.event_handle();
    let (tx, rx) = flume::unbounded();
    handle.request_restore(
        maki_lua::RestoreItem {
            tool: Arc::from(tool),
            tool_use_id: "restore_id".to_owned(),
            output: output.to_owned(),
            input,
            is_error: false,
            tool_output_lines: view_lines(),
            theme_gen: None,
            clicks,
            state,
        },
        maki_agent::EventSender::new(tx, 0),
    );
    handle.wait_restore_complete_for_test();
    // The empty LoadSource drains the async gate, so spawned highlight tasks
    // finish before we inspect the buffers.
    host.load_source("barrier", "").unwrap();
    let mut out = Restored {
        body: String::new(),
        header: String::new(),
    };
    for env in rx.drain() {
        match env.event {
            AgentEvent::ToolSnapshot { snapshot, .. } => out.body = snapshot.text(),
            AgentEvent::ToolHeaderSnapshot { snapshot, .. } => out.header = snapshot.text(),
            _ => {}
        }
    }
    out
}

#[test]
fn bash_restore_renders_real_view() {
    let host = load_host();
    let r = restore(
        &host,
        "bash",
        json!({ "command": "echo hi", "description": "print hi" }),
        "hi",
        None,
        Vec::new(),
    );
    assert!(
        r.body.contains("echo hi"),
        "real view renders the command header; the fallback body is raw output only: {}",
        r.body
    );
    assert!(r.header.contains("print hi"), "header: {}", r.header);
}

/// Phase 1: children render through their own real views (grep gutter,
/// bash command header), not the raw-llm fallback. Phase 2: a replayed
/// click inside grep's range reaches its real toggle and expands only it.
#[test]
fn batch_restore_renders_real_children_and_click_expands_grep() {
    let host = load_host();
    let input: Value = serde_json::from_str(BATCH_INPUT_GREP_BASH).unwrap();
    let collapsed = restore(
        &host,
        "batch",
        input.clone(),
        "whatever",
        Some(batch_state()),
        Vec::new(),
    );
    let text = &collapsed.body;
    assert!(text.contains("grep> "), "grep child header: {text}");
    assert!(text.contains("bash> "), "bash child header: {text}");
    // grep's real view reformats `nr:` into gutter lines.
    assert!(text.contains("    1: fn main() {}"), "grep gutter: {text}");
    assert!(
        !text.contains("\n1: fn main"),
        "raw llm text means the child restore degraded to fallback: {text}"
    );
    assert!(
        text.contains(EXPAND_HINT),
        "grep view collapsed past its cap: {text}"
    );
    assert!(
        text.contains("echo hello-from-bash"),
        "bash child rendered its real view (command header): {text}"
    );
    assert!(
        text.lines().any(|l| l.trim() == "hello-from-bash"),
        "bash output line: {text}"
    );

    // Rows are 1-based (row 0 = header), so snapshot line i = row i+1.
    let notice_row = 1 + collapsed
        .body
        .lines()
        .position(|l| l.contains(EXPAND_HINT))
        .expect("grep truncation notice in collapsed render");
    let clicked = restore(
        &host,
        "batch",
        input,
        "whatever",
        Some(batch_state()),
        vec![notice_row],
    );
    let text = &clicked.body;
    assert!(
        text.contains("    10: fn other() {}"),
        "expanded grep tail visible: {text}"
    );
    assert!(
        !text.contains(EXPAND_HINT),
        "grep no longer collapsed: {text}"
    );
    assert!(
        text.contains("hello-from-bash"),
        "bash child untouched: {text}"
    );
}

/// Header fn that yields (e.g. highlight) must work, not fall back.
#[test]
fn restore_header_fn_may_await_async_apis() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source(
        "hdr",
        r#"maki.api.register_tool({
            name = "hdr_await",
            description = "t",
            schema = { type = "object", properties = {} },
            handler = function() return "ok" end,
            header = function(input)
                local hl = maki.ui.highlight("echo marker", "bash") or { { { "echo marker" } } }
                local buf = maki.ui.buf()
                buf:set_lines(hl)
                return buf
            end,
            restore = function(input, output)
                local buf = maki.ui.buf()
                buf:line("body")
                return buf
            end,
        })"#,
    )
    .unwrap();
    let r = restore(&host, "hdr_await", json!({}), "ok", None, Vec::new());
    assert_eq!(r.body.trim(), "body");
    assert!(
        r.header.contains("echo marker"),
        "awaiting header fn must survive: {}",
        r.header
    );
}

/// Standalone edit diffs never truncate (Rust hardcodes it), so batch
/// children must match: whole diff, `-` lines numbered by finding the new
/// text in the edited file, `+` lines with a blank gutter.
#[test]
fn multiedit_batch_child_shows_full_numbered_diff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("f.rs");
    std::fs::write(&path, "top\nzzz\nn1\nn2\nn3\nn4\nn5\nbottom\n").unwrap();

    let host = PluginHost::with_all_builtins(Arc::new(ToolRegistry::new())).unwrap();
    let input = json!({ "tool_calls": [{ "tool": "multiedit", "parameters": {
        "path": path.to_str().unwrap(),
        "edits": [{ "old_string": "old1\nold2\nold3\nold4\nold5", "new_string": "n1\nn2\nn3\nn4\nn5" }],
    }}]});
    let state = json!({ "children": [
        { "tool": "multiedit", "status": "success", "output": "applied 1 edit" },
    ]});
    let r = restore(&host, "batch", input, "whatever", Some(state), Vec::new());

    let text = &r.body;
    // keep = "head" truncation would cut the tail, so the last added line
    // present plus no collapse notice proves the 10-line diff is whole.
    assert!(
        text.contains("+ n5") && !text.contains(EXPAND_HINT),
        "edit diffs must never truncate: {text}"
    );
    assert!(
        text.contains("3 - old1") && text.contains("7 - old5"),
        "removed lines numbered from the new text's file position: {text}"
    );
    assert!(
        !text.contains("3 + n1"),
        "added lines get a blank gutter: {text}"
    );
}

const INDEX_TOOL: &str = "index";
const LIVE_TOOL_USE_ID: &str = "live_id";
/// More than the index view cap, exactly the read view cap, so a listing
/// rendered through the index view is visibly truncated.
const DIR_ENTRIES: [&str; READ_VIEW_CAP] = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];
const ENTRIES_SUFFIX: &str = " entries";

struct Live {
    body: String,
    output: String,
    annotation: Option<String>,
}

fn exec_live(host: &PluginHost, reg: &ToolRegistry, tool: &str, input: Value) -> Live {
    let (tx, rx) = flume::unbounded();
    let event_tx = maki_agent::EventSender::new(tx, 0);
    let mut ctx = maki_agent::tools::test_support::stub_ctx_with(
        &maki_agent::AgentMode::Build,
        Some(&event_tx),
        Some(LIVE_TOOL_USE_ID),
    );
    ctx.tool_output_lines = view_lines();
    let inv = reg
        .get(tool)
        .unwrap_or_else(|| panic!("tool {tool} not registered"))
        .tool
        .parse(&input)
        .expect("parse failed");
    let result = smol::block_on(async { inv.execute(&ctx).await });
    host.load_source("live_barrier", "").unwrap();
    let mut body = String::new();
    for env in rx.drain() {
        if let AgentEvent::ToolSnapshot { snapshot, .. } = env.event {
            body = snapshot.text();
        }
    }
    let output = match result.output.expect("tool failed") {
        maki_agent::ToolOutput::Plain(s) | maki_agent::ToolOutput::Markdown(s) => s.text,
        other => panic!("unexpected output: {other:?}"),
    };
    Live {
        body,
        output,
        annotation: result.annotation,
    }
}

/// A directory has no skeleton, so index shows the plain listing. Restore must
/// rebuild that same listing view instead of the index skeleton view, which
/// would truncate to the index cap and highlight the entries as code.
#[test]
fn index_dir_renders_identically_live_and_restored() {
    let dir = tempfile::tempdir().unwrap();
    for name in DIR_ENTRIES {
        std::fs::write(dir.path().join(name), "").unwrap();
    }
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let input = json!({ "path": dir.path().to_str().unwrap() });

    let live = exec_live(&host, &reg, INDEX_TOOL, input.clone());
    let restored = restore(&host, INDEX_TOOL, input, &live.output, None, Vec::new());

    let expected_annotation = format!("{}{ENTRIES_SUFFIX}", DIR_ENTRIES.len());
    assert_eq!(
        live.annotation.as_deref(),
        Some(expected_annotation.as_str()),
        "live dir listing is annotated with entry count"
    );
    for name in DIR_ENTRIES {
        assert!(
            live.body.contains(name),
            "entry {name} missing: {}",
            live.body
        );
    }
    assert!(
        !live.body.contains(EXPAND_HINT),
        "listing fits the read view cap: {}",
        live.body
    );
    assert_eq!(
        restored.body, live.body,
        "restored dir listing must match the live one"
    );
}

// Phase 4: bash auto-mode gate integration. We drive the real bash handler
// with `classify_verdict` stubbed on the shared `bash_helpers` module (require
// caches the singleton, so `init.lua` sees the override). The deny and error
// paths return synchronously *before* jobstart, so they are deterministic here;
// the approve path falls through to the async jobstart loop, which this harness
// cannot observe, so it is covered at the `spec.lua` unit layer instead.

/// Load the real bash plugin with auto mode forced on and the classifier
/// stubbed per `stub_code` (defines `bh.classify_verdict`).
fn bash_host_with_classifier(stub_code: &str) -> (PluginHost, Arc<ToolRegistry>) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    // The classifier stub and the auto toggle must reach the same `bash_helpers`
    // instance the handler captured. Every `load_source` gets a fresh per-env
    // require cache, so a separate plugin chunk wouldn't share the module; run
    // them as one source instead. The setup comes after `BASH_SRC` because it
    // defaults auto mode off via `bh.set_auto_mode(opts.auto_mode)`.
    let classifier_setup = format!(
        r#"local bh = require("bash_helpers")
bh.set_auto_mode(true)
{stub_code}
"#
    );
    host.load_source("bash", &format!("{BASH_SRC}\n{classifier_setup}"))
        .unwrap();
    (host, reg)
}

/// Like [`bash_host_with_classifier`] but with the `auto_mode_ask_on_deny`
/// option set, so a classifier deny escalates to the permission prompt. The
/// unknown-option validation at load doubles as the registration check: a
/// typo here fails `load_source_with_opts`.
fn bash_host_with_classifier_ask_on_deny(stub_code: &str) -> (PluginHost, Arc<ToolRegistry>) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let classifier_setup = format!(
        r#"local bh = require("bash_helpers")
bh.set_auto_mode(true)
{stub_code}
"#
    );
    let mut opts = serde_json::Map::new();
    opts.insert(
        "auto_mode_ask_on_deny".to_owned(),
        serde_json::Value::Bool(true),
    );
    host.load_source_with_opts("bash", &format!("{BASH_SRC}\n{classifier_setup}"), opts)
        .unwrap();
    (host, reg)
}

struct Verdict {
    is_error: bool,
    output: String,
    /// Events emitted during the run (PermissionRequest, tool snapshots).
    events: Vec<AgentEvent>,
}

/// Run the bash tool and return whether it produced an error plus the output
/// text. Mirrors `exec_live` but surfaces `is_error` (the gate's deny/fallback
/// paths return error tool output, not live view bodies).
fn exec_verdict(host: &PluginHost, reg: &ToolRegistry, input: Value) -> Verdict {
    let mut ctx = maki_agent::tools::test_support::stub_ctx_with(
        &maki_agent::AgentMode::Build,
        None,
        Some("classifier_tool_use_id"),
    );
    ctx.tool_output_lines = view_lines();
    let inv = reg
        .get("bash")
        .expect("bash tool registered")
        .tool
        .parse(&input)
        .expect("parse failed");
    let result = smol::block_on(async { inv.execute(&ctx).await });
    // Drain the async gate so spawned tasks settle before the host drops.
    host.load_source("auto_barrier", "").unwrap();
    let (is_error, output) = match result.output {
        Ok(maki_agent::ToolOutput::Plain(s)) | Ok(maki_agent::ToolOutput::Markdown(s)) => {
            (false, s.text)
        }
        Err(e) => (true, e),
        other => panic!("unexpected output: {other:?}"),
    };
    Verdict {
        is_error,
        output,
        events: Vec::new(),
    }
}

/// Prompt-default `PermissionManager` so unclaimed scopes produce a real
/// `NeedsPrompt` (the stock stub ctx silently allows them).
fn prompt_permissions() -> Arc<PermissionManager> {
    Arc::new(PermissionManager::new(
        PermissionsConfig {
            default: DefaultEffect::Prompt,
            rules: vec![],
            ..PermissionsConfig::default()
        },
        PathBuf::from("/tmp"),
        Arc::default(),
    ))
}

/// Like [`exec_verdict`], but with a live event channel (so tests can assert
/// `AgentEvent::PermissionRequest` was emitted) and an optional canned user
/// answer pre-loaded on the response channel for the deny→prompt path to
/// consume. `answer = None` leaves `user_response_rx` unset, modeling a
/// headless/ACP run without an answerer.
fn exec_verdict_prompt(
    host: &PluginHost,
    reg: &ToolRegistry,
    input: Value,
    permissions: Arc<PermissionManager>,
    answer: Option<&str>,
) -> Verdict {
    let (tx, rx) = flume::unbounded();
    let event_tx = maki_agent::EventSender::new(tx, 0);
    let mut ctx = maki_agent::tools::test_support::stub_ctx_with(
        &maki_agent::AgentMode::Build,
        Some(&event_tx),
        Some("classifier_tool_use_id"),
    );
    ctx.permissions = permissions;
    if let Some(answer) = answer {
        let (answer_tx, answer_rx) = flume::unbounded::<String>();
        ctx.user_response_rx = Some(Arc::new(async_lock::Mutex::new(answer_rx)));
        answer_tx.send(answer.to_owned()).unwrap();
    }
    ctx.tool_output_lines = view_lines();
    let inv = reg
        .get("bash")
        .expect("bash tool registered")
        .tool
        .parse(&input)
        .expect("parse failed");
    let result = smol::block_on(async { inv.execute(&ctx).await });
    host.load_source("auto_barrier", "").unwrap();
    let (is_error, output) = match result.output {
        Ok(maki_agent::ToolOutput::Plain(s)) | Ok(maki_agent::ToolOutput::Markdown(s)) => {
            (false, s.text)
        }
        Err(e) => (true, e),
        other => panic!("unexpected output: {other:?}"),
    };
    let events = rx.drain().map(|env| env.event).collect();
    Verdict {
        is_error,
        output,
        events,
    }
}

const CLASSIFY_DENY_STUB: &str =
    r#"bh.classify_verdict = function(...) return "deny", "stub deny reason", nil end"#;
const CLASSIFY_ERROR_STUB: &str =
    r#"bh.classify_verdict = function(...) return "error", nil, "stub boom" end"#;

#[test]
fn auto_mode_deny_rejects_command_without_running_jobstart() {
    let (host, reg) = bash_host_with_classifier(CLASSIFY_DENY_STUB);
    let result = exec_verdict(&host, &reg, json!({ "command": "echo denied-side-effect" }));
    assert!(result.is_error, "a deny must fail the tool");
    assert!(
        result.output.contains("denied"),
        "deny surfaces the classifier reason: {}",
        result.output
    );
    assert!(
        result.output.contains("stub deny reason"),
        "deny carries the classifier reason: {}",
        result.output
    );
}

#[test]
fn auto_mode_error_denies_fail_closed_without_prompting() {
    let (host, reg) = bash_host_with_classifier(CLASSIFY_ERROR_STUB);
    let result = exec_verdict(&host, &reg, json!({ "command": "echo never-runs-2" }));
    assert!(
        result.is_error,
        "a classifier error must fail closed (deny)"
    );
    assert!(
        result.output.contains("denied by auto-mode"),
        "a classifier error must not prompt and never auto-run: {}",
        result.output
    );
}

// Phase 4 (real driver): the classifier agent session runs for real against a
// canned provider (via `inherit_provider`), instead of a synchronous Lua stub.
// This is what lets the approve path be observed at all: after the classifier
// approves, the bash handler falls through to the real jobstart loop.

/// Load the real bash plugin with auto mode on and `bh.classify_verdict` wrapped
/// so the classifier session is the real `maki.agent.session` reusing the parent
/// (canned) provider via `inherit_provider`. The plugin's gating logic runs
/// unchanged; only the spawn is forwarded.
fn bash_host_with_real_classifier() -> (PluginHost, Arc<ToolRegistry>) {
    bash_host_with_real_classifier_auto(true)
}

/// Like [`bash_host_with_real_classifier`] but with auto mode initialized to
/// `auto_on`. The bash plugin defaults auto mode OFF, so a separate host with
/// `auto_on = false` models the state after `/automode` toggled it off. (Each
/// `load_source` gets its own per-env require cache, so toggling `bash_helpers`
/// from a separate chunk would hit a different singleton than the handler
/// captured; two hosts avoid that footgun.)
fn bash_host_with_real_classifier_auto(auto_on: bool) -> (PluginHost, Arc<ToolRegistry>) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let classifier_setup = format!(
        r#"
local bh = require("bash_helpers")
bh.set_auto_mode({auto})
local real_classify = bh.classify_verdict
local real_session = maki.agent.session
local function canned_spawn(ctx, opts)
  local so = bh.spawn_opts(opts)
  so.inherit_provider = true
  return real_session(ctx, so)
end
bh.classify_verdict = function(command, cwd, opts, ctx)
  return real_classify(command, cwd, opts, ctx, canned_spawn)
end
"#,
        auto = if auto_on { "true" } else { "false" },
    );
    host.load_source("bash", &format!("{BASH_SRC}\n{classifier_setup}"))
        .unwrap();
    (host, reg)
}

fn verdict_tool_use(approved: bool, reason: &str) -> StreamResponse {
    common::canned_tool_use(
        "classifier_verdict",
        json!({ "approved": approved, "reason": reason }),
    )
}

/// Run the real bash handler against a canned-provider ctx and return its
/// `output` (`Ok` for the jobstart path, `Err` for the gate's deny/error paths).
fn exec_bash_real(
    host: &PluginHost,
    reg: &ToolRegistry,
    provider: Arc<common::CannedProvider>,
    input: Value,
) -> Result<ToolOutput, String> {
    let (mut ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    ctx.tool_output_lines = view_lines();
    let inv = reg
        .get("bash")
        .expect("bash tool registered")
        .tool
        .parse(&input)
        .expect("parse failed");
    let result = smol::block_on(async { inv.execute(&ctx).await });
    host.load_source("auto_barrier", "").unwrap();
    result.output
}

fn bash_output(out: ToolOutput) -> String {
    match out {
        ToolOutput::Plain(s) | ToolOutput::Markdown(s) => s.text,
        other => panic!("unexpected output: {other:?}"),
    }
}

/// Deny blocks the command (carrying the classifier reason); approve falls
/// through to the real jobstart loop and the command runs. The approve path is
/// the one the stub suite could not observe.
#[test]
fn automode_deny_blocks_and_approve_runs() {
    let (host, reg) = bash_host_with_real_classifier();

    let deny = Arc::new(common::CannedProvider::new(vec![
        verdict_tool_use(false, "stub deny reason"),
        common::canned_reply("done"),
    ]));
    let err = exec_bash_real(
        &host,
        &reg,
        Arc::clone(&deny),
        json!({ "command": "echo denied-side-effect" }),
    )
    .expect_err("a deny must fail the tool");
    assert!(err.contains("denied"), "{err}");
    assert!(
        err.contains("stub deny reason"),
        "deny carries the reason: {err}"
    );

    let approve = Arc::new(common::CannedProvider::new(vec![
        verdict_tool_use(true, "ok"),
        common::canned_reply("done"),
    ]));
    let out = exec_bash_real(
        &host,
        &reg,
        Arc::clone(&approve),
        json!({ "command": "echo approved-side-effect" }),
    )
    .expect("an approve must run the command");
    assert_eq!(bash_output(out), "approved-side-effect");
}

/// A classifier error (here: a verdict the tool rejects, so none is captured)
/// fails closed — denied, no prompt, no run.
#[test]
fn automode_error_fails_closed_without_prompting() {
    let (host, reg) = bash_host_with_real_classifier();
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_tool_use("classifier_verdict", json!({ "approved": "not-a-bool" })),
        common::canned_reply("done"),
    ]));
    let err = exec_bash_real(
        &host,
        &reg,
        Arc::clone(&provider),
        json!({ "command": "echo never-runs" }),
    )
    .expect_err("a classifier error must deny");
    assert!(err.contains("denied by auto-mode"), "{err}");
}

/// With auto mode OFF the bash handler runs the plain path and never consults
/// the classifier; with auto mode ON a subsequent command is gated by it. Two
/// hosts model the `/automode` toggle state (each `load_source` has its own
/// require cache, so the toggle is expressed as the host's initial auto state
/// rather than a cross-chunk mutation).
#[test]
fn automode_toggle_flows_through_ui() {
    let (host_off, reg_off) = bash_host_with_real_classifier_auto(false);
    let idle = Arc::new(common::CannedProvider::new(vec![]));
    let out = exec_bash_real(
        &host_off,
        &reg_off,
        Arc::clone(&idle),
        json!({ "command": "echo auto-off-runs" }),
    )
    .expect("with auto mode off the plain path runs");
    assert_eq!(bash_output(out), "auto-off-runs");
    assert!(
        idle.captured_thinking().is_empty(),
        "classifier must not run with auto mode off"
    );

    let (host_on, reg_on) = bash_host_with_real_classifier_auto(true);
    let deny = Arc::new(common::CannedProvider::new(vec![
        verdict_tool_use(false, "denied with auto on"),
        common::canned_reply("done"),
    ]));
    let err = exec_bash_real(
        &host_on,
        &reg_on,
        Arc::clone(&deny),
        json!({ "command": "echo should-be-denied" }),
    )
    .expect_err("with auto mode on the classifier gates the command");
    assert!(err.contains("denied with auto on"), "{err}");
}

// Phase 4 (ask-on-deny): with `auto_mode_ask_on_deny` the classifier deny
// escalates to the same permission prompt the automode-off path uses
// (`PermissionManager::enforce`). The stock stub ctx would silently allow
// unclaimed scopes, so these tests install a Prompt-default manager and a
// canned user answer on the response channel.

/// PermissionRequest scopes plus the event's tool, in emission order.
fn permission_requests(events: &[AgentEvent]) -> Vec<(String, Vec<String>)> {
    events
        .iter()
        .filter_map(|e| match e {
            AgentEvent::PermissionRequest { tool, scopes, .. } => {
                Some((tool.to_string(), scopes.clone()))
            }
            _ => None,
        })
        .collect()
}

#[test]
fn auto_mode_ask_on_deny_user_allow_runs() {
    let (host, reg) = bash_host_with_classifier_ask_on_deny(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "echo ask-allow-side-effect" }),
        prompt_permissions(),
        Some("allow"),
    );
    assert!(
        !result.is_error,
        "the user's allow must fall through to the jobstart run: {}",
        result.output
    );
    assert_eq!(result.output, "ask-allow-side-effect");
    assert_eq!(
        permission_requests(&result.events),
        vec![(
            "bash".to_owned(),
            vec!["echo ask-allow-side-effect".to_owned()]
        )],
        "a PermissionRequest for the command's scopes must be emitted"
    );
}

#[test]
fn auto_mode_ask_on_deny_user_deny_fails() {
    let (host, reg) = bash_host_with_classifier_ask_on_deny(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "echo ask-deny-side-effect" }),
        prompt_permissions(),
        Some("deny"),
    );
    assert!(result.is_error, "a user deny must fail the tool");
    assert_eq!(
        result.output,
        format!(
            "{PERMISSION_DENIED_PREFIX} `bash` (echo ask-deny-side-effect). {DEFAULT_DENY_GUIDANCE}"
        ),
        "the deny must surface the standard permission message, byte-identical to automode-off"
    );
}

#[test]
fn auto_mode_ask_on_deny_classifier_error_still_fails_closed() {
    let (host, reg) = bash_host_with_classifier_ask_on_deny(CLASSIFY_ERROR_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "echo never-runs-ask" }),
        prompt_permissions(),
        Some("allow"),
    );
    assert!(
        result.is_error,
        "a classifier error must fail closed even with ask-on-deny on"
    );
    assert!(
        result.output.contains("denied by auto-mode"),
        "a classifier error must never prompt and never auto-run: {}",
        result.output
    );
    assert!(
        permission_requests(&result.events).is_empty(),
        "a classifier error must not emit a PermissionRequest"
    );
}

#[test]
fn auto_mode_ask_on_deny_no_response_channel_fails_closed() {
    let (host, reg) = bash_host_with_classifier_ask_on_deny(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "echo no-answerer" }),
        prompt_permissions(),
        None,
    );
    assert!(
        result.is_error,
        "a deny with no answerer must fail closed instead of hanging"
    );
    assert!(
        result.output.contains(PERMISSION_DENIED_PREFIX),
        "no response channel surfaces the permission message: {}",
        result.output
    );
}

#[test]
fn auto_mode_ask_on_deny_allow_rule_skips_prompt() {
    let perms = Arc::new(PermissionManager::new(
        PermissionsConfig {
            default: DefaultEffect::Prompt,
            rules: vec![PermissionRule {
                tool: ToolKey::native("bash"),
                scope: Some("echo *".to_owned()),
                effect: Effect::Allow,
            }],
            ..PermissionsConfig::default()
        },
        PathBuf::from("/tmp"),
        Arc::default(),
    ));
    let (host, reg) = bash_host_with_classifier_ask_on_deny(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "echo allow-rule-side-effect" }),
        perms,
        None,
    );
    assert!(
        !result.is_error,
        "an allow rule matching the command must short-circuit the prompt: {}",
        result.output
    );
    assert_eq!(result.output, "allow-rule-side-effect");
    assert!(
        permission_requests(&result.events).is_empty(),
        "an allow rule must run without prompting"
    );
}

/// A `cd`-hint input prompts on the raw `input.command` scopes (the two
/// segments), not the `parse_cd_hint`-rewritten command. With only `echo *`
/// allowed, the `cd /tmp` segment still prompts; with both segments allowed
/// the whole call short-circuits like the automode-off path.
#[test]
fn auto_mode_ask_on_deny_cd_hint_prompts_on_raw_input_scopes() {
    let (host, reg) = bash_host_with_classifier_ask_on_deny(CLASSIFY_DENY_STUB);

    let echo_only = Arc::new(PermissionManager::new(
        PermissionsConfig {
            default: DefaultEffect::Prompt,
            rules: vec![PermissionRule {
                tool: ToolKey::native("bash"),
                scope: Some("echo *".to_owned()),
                effect: Effect::Allow,
            }],
            ..PermissionsConfig::default()
        },
        PathBuf::from("/tmp"),
        Arc::default(),
    ));
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "cd /tmp && echo foo" }),
        Arc::clone(&echo_only),
        Some("allow"),
    );
    assert!(
        !result.is_error,
        "the user's allow must run: {}",
        result.output
    );
    assert_eq!(result.output, "foo");
    assert_eq!(
        permission_requests(&result.events),
        vec![("bash".to_owned(), vec!["cd /tmp".to_owned()])],
        "scopes must come from the raw input.command, not the rewritten command"
    );

    let both = Arc::new(PermissionManager::new(
        PermissionsConfig {
            default: DefaultEffect::Prompt,
            rules: vec![
                PermissionRule {
                    tool: ToolKey::native("bash"),
                    scope: Some("cd *".to_owned()),
                    effect: Effect::Allow,
                },
                PermissionRule {
                    tool: ToolKey::native("bash"),
                    scope: Some("echo *".to_owned()),
                    effect: Effect::Allow,
                },
            ],
            ..PermissionsConfig::default()
        },
        PathBuf::from("/tmp"),
        Arc::default(),
    ));
    let result = exec_verdict_prompt(
        &host,
        &reg,
        json!({ "command": "cd /tmp && echo foo" }),
        both,
        None,
    );
    assert!(
        !result.is_error,
        "allowing both raw segments must short-circuit: {}",
        result.output
    );
    assert_eq!(result.output, "foo");
    assert!(
        permission_requests(&result.events).is_empty(),
        "both segments allowed means no prompt"
    );
}
