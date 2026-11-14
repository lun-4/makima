//! Exercises real plugins (bash, grep, batch) through `request_restore`.
//! A broken restore silently falls back to raw LLM output, so we assert
//! things only the real views produce (gutters, command headers, truncation).

use std::future::Future;
use std::io::{Error as IoError, Result as IoResult};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use makima_agent::actor::{ActorHistory, AgentContextSnapshot, PreparedModel};
use makima_agent::agent::tool_dispatch;
use makima_agent::cancel::{CancelToken, CancelTrigger};
use makima_agent::config_change::unresolved_model;
use makima_agent::file_restore::{self, RestorePreview, RestoreRequest, RestoreSegment};
use makima_agent::manager::AgentResidency;
use makima_agent::permissions::PermissionManager;
use makima_agent::permissions::{DEFAULT_DENY_GUIDANCE, PERMISSION_DENIED_PREFIX};
use makima_agent::session_checkpoint::SessionLogCheckpoint;
use makima_agent::session_coordinator::{
    SessionCheckpoint, SessionCoordinatorHandle, SessionCoordinatorParams,
    builtin_option_definitions,
};
use makima_agent::session_options::{DISABLED_VALUE, ENABLED_VALUE, YOLO_OPTION_ID};
use makima_agent::tools::offload::{OffloadStore, is_offload_notice};
use makima_agent::tools::{
    CallOrigin, FILE_TRUNCATED_MARKER, FileWriteLocks, ToolContext, ToolRegistry, TurnToolBindings,
};
use makima_agent::{
    ActorBackend, ActorLifecycle, AgentEvent, AgentInput, AgentLimits, AgentManagerHandle,
    BackendResult, ControlWork, DoneReason, GraphLifecycle, ManagerError, SessionMailbox,
    TurnContext, TurnOutcome, WorkKind,
};
use makima_agent::{SnapshotLine, SpanStyle, ToolOutput};
use makima_config::{
    DefaultEffect, Effect, PermissionRule, PermissionsConfig, ProjectConfig, ToolKey,
    ToolOutputLines,
};
use makima_lua::PluginHost;
use makima_lua::orchestration::{AgentOrchestration, TrustedTarget};
use makima_lua::test_support::{FsBackend, FsError, FsMeta, InMemoryFs, RealFs, run_command};
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    AgentError, ContentBlock, Invocation, Message, Model, ProviderEvent, RequestDelivery,
    RequestOptions, StreamResponse,
};
use makima_storage::StateDir;
use makima_storage::blobs::{BlobStore, FileState};
use makima_storage::checkpoint::{CheckpointWriter, ClaimingWriter};
use makima_storage::id::MakimaId;
use makima_storage::id::SessionRef;
use makima_storage::restore::{
    PathRestore, RestoreChoice, RestoreEndpoint, RestoreProgress, RestoreStore, RestoreTransition,
};
use makima_storage::session_state::{SessionStateView, StateChange, StoredAgentLifecycle};
use makima_storage::sessions::SESSIONS_DIR;
use serde_json::{Value, json};

mod common;

const BASH_SRC: &str = include_str!("../../plugins/bash/init.lua");
const GLOB_SRC: &str = include_str!("../../plugins/glob/init.lua");
const GREP_SRC: &str = include_str!("../../plugins/grep/init.lua");
const BATCH_SRC: &str = include_str!("../../plugins/batch/init.lua");
const TASK_SRC: &str = include_str!("../../plugins/task/init.lua");
const TASK_RETIRE_COMMAND: &str = "/task-retire";
const TASK_RETIRE_FIXTURE: &str = "task_retire_fixture";
const TASK_RETIRE_RUN: &str = "/task-retire-fixture";
const TASK_RETIRE_REPLY: &str = "retained retirement result";
const TASK_RETIRE_IDS: &str = "task-retire-ids.json";
const TASK_RETIRE_NO_CONTEXT: &str = "task-retire requires an agent context";
const TASK_RETIRE_RETAINED_LIMIT: usize = 3;
const TASK_RETIRE_SOURCE: &str = r#"
makima.api.register_command({
    name = "/task-retire-fixture", tui_only = false,
    arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local input = assert(makima.json.decode(opts.args))
        if input.action == "create" then
            local child = assert(makima.agent.spawn(ctx, {
                silent = true, auto_deliver = false, mcp = false, tools = {},
            }))
            local ticket = assert(child:send(ctx, "preserve this task history", { when = "idle" }))
            local result = assert(ticket:wait(ctx))
            assert(result.status == "completed", result.error)
            assert(makima.fs.write(input.path, makima.json.encode({
                agent_id = child:id(), turn_id = ticket:id(), text = result.text,
            })))
        else
            local ids = assert(makima.json.decode(assert(makima.fs.read(input.path))))
            local child = assert(makima.agent.get(ctx, ids.agent_id))
            if input.action == "close" then
                assert(child:close(ctx))
            else
                local status = assert(child:status(ctx))
                assert(status.status == "closed" and not status.live)
                local ticket = assert(child:turn(ctx, ids.turn_id))
                assert(assert(ticket:result(ctx)).text == ids.text)
                assert(assert(ticket:wait(ctx, { timeout = 0 })).text == ids.text)
                local transcript = assert(child:transcript(ctx, { last_messages = 100, max_bytes = 65536 }))
                local history = makima.json.encode(transcript.messages)
                assert(history:find("preserve this task history", 1, true))
                assert(history:find(ids.text, 1, true))
                local sent, err = child:send(ctx, "must not run", { when = "idle" })
                assert(sent == nil and err:find("retired:", 1, true), tostring(err))
                assert(not child:is_live())
            end
        end
    end,
})
"#;
const TASK_RESTART_PROBE: &str = "task_restart_probe";
const TASK_RESTART_COMMAND: &str = "/task-restart-fixture";
const TASK_RESTART_REPLY: &str = "durable task reply";
const TASK_RESTART_INSTRUCTIONS: &str = "recorded task startup instructions";
const TASK_RESTART_CHANGED_INSTRUCTIONS: &str = "unreloaded task startup instructions";
const TASK_RESTART_TIMEOUT: Duration = Duration::from_secs(60);
const TASK_RESTART_SOURCE: &str = r#"
local function call(ctx, name, input)
    if name == "task_wait" then input = { task_id = input.task_id, turn_id = input.turn_id } end
    local out, err = makima.agent.call_tool(ctx, name, input)
    assert(out, err)
    return assert(makima.json.decode(out))
end
local function read(path) return assert(makima.json.decode(assert(makima.fs.read(path)))) end
makima.api.register_tool({
    name = "task_restart_probe", description = "exercise durable task tools",
    audiences = { "main", "general_sub" },
    schema = { type = "object", additionalProperties = true },
    handler = function(input, ctx)
        if input.action == "create" then
            local ids = call(ctx, "task_spawn", {
                description = input.kind, prompt = "task work", subagent_type = input.kind,
            })
            local result = call(ctx, "task_wait", ids)
            assert(result.result.text == "durable task reply")
            ids.owner = assert(ctx:agent()):id()
            assert(makima.fs.write(input.path, makima.json.encode(ids)))
        elseif input.action == "oversize" then
            local out, err = makima.agent.call_tool(ctx, "task_spawn", {
                description = string.rep("x", 256 * 1024), prompt = "must not start",
            })
            assert(out == nil and err:find("state value exceeds maximum encoded size", 1, true), tostring(err))
        else
            local ids = read(input.path)
            if input.action == "denied" then
                for _, name in ipairs({ "task_wait", "task_send", "task_despawn", "task_stop" }) do
                    local out, err = makima.agent.call_tool(ctx, name, {
                        task_id = ids.task_id, message = name == "task_send" and "forbidden" or nil,
                    })
                    assert(out == nil and err:find("is not a descendant", 1, true), tostring(err))
                end
            elseif input.action == "send" then
                local out, err = makima.agent.call_tool(ctx, "task_send", { task_id = ids.task_id, message = "follow up" })
                if input.blocked then
                    assert(out == nil and err:find("general subagents cannot take new work in plan mode", 1, true), tostring(err))
                else
                    assert(out, err)
                    local sent = assert(makima.json.decode(out))
                    assert(sent.turn_id ~= ids.turn_id)
                    local result = call(ctx, "task_wait", { task_id = ids.task_id, turn_id = sent.turn_id })
                    assert(result.result.text == "durable task reply")
                end
            elseif input.action == "legacy" then
                local out, err = makima.agent.call_tool(ctx, "task_wait", { task_id = input.legacy })
                assert(out == nil and err == "unknown task_id", tostring(err))
            elseif input.action == "close" or input.action == "retained" then
                if input.action == "close" then call(ctx, "task_despawn", { task_id = ids.task_id }) end
                assert(call(ctx, "task_wait", ids).result.text == "durable task reply")
            else
                local result = call(ctx, "task_wait", ids)
                assert(result.result.text == "durable task reply")
                local latest = call(ctx, "task_wait", { task_id = ids.task_id })
                assert(latest.result.turn_id == ids.turn_id)
                local agent = assert(makima.agent.get(ctx, ids.task_id))
                assert(not agent:is_live())
            end
        end
        return "verified"
    end,
})
makima.api.register_command({
    name = "/task-restart-fixture", tui_only = false, arguments = { raw = true, required = true },
    handler = function(opts, ctx)
        local input = assert(makima.json.decode(opts.args))
        local driver
        if input.driver then
            driver = assert(makima.agent.get(ctx, input.driver))
        else
            driver = assert(makima.agent.spawn(ctx, {
                silent = true, auto_deliver = false, mcp = false, audience = "general_sub",
                tools = {
                    { name = "task_restart_probe", description = "probe", input_schema = { type = "object" } },
                    { name = "task_spawn", description = "spawn", input_schema = { type = "object" } },
                    { name = "task_wait", description = "wait", input_schema = { type = "object" } },
                    { name = "task_send", description = "send", input_schema = { type = "object" } },
                    { name = "task_despawn", description = "close", input_schema = { type = "object" } },
                    { name = "task_stop", description = "stop", input_schema = { type = "object" } },
                },
            }))
        end
        if input.mode then assert(driver:set_mode(ctx, input.mode)) end
        local ticket = assert(driver:send(ctx, makima.json.encode(input), { when = "idle" }))
        local result = assert(ticket:wait(ctx))
        assert(result.status == "completed", result.error)
        assert(result.text == "verified", result.text)
    end,
})
"#;
const MANAGED_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const MANAGED_BASH_TIMEOUT: Duration = Duration::from_secs(5);
const CLASSIFIER_TOOL_NAME: &str = "classifier_verdict";
const AUTO_MODE_DENIED: &str = "command denied by auto-mode: ";

/// Only the real ToolView emits this when collapsed.
const EXPAND_HINT: &str = "click to expand";
/// Fixed caps so truncation tests don't depend on the product defaults. The
/// index and read caps differ so a body rendered through the wrong view is
/// visibly different.
const VIEW_CAP: usize = 3;
const INDEX_VIEW_CAP: usize = 2;
const READ_VIEW_CAP: usize = 5;
const GREP_NO_MATCHES: &str = "No files found";
const GREP_ERROR_OUTPUT: &str = "error: pattern is required";
const TASK_WAIT_TOOL: &str = "task_wait";
const TASK_SEND_TOOL: &str = "task_send";
const TASK_SEND_LINE_PREFIX: &str = "Instruction line";
const TASK_SEND_QUOTE_BAR: &str = "│ ";
const TASK_ID: &str = "task-0123456789abcdef";
const TASK_SHORT_ID: &str = "89abcdef";
const TASK_TURN_ID: &str = "turn-restored-result";
const TASK_INPUT_TURN_ID: &str = "turn-requested";
const TASK_DESCRIPTION: &str = "Review restored task reports";
const TASK_RUNNING: &str =
    "Still running when checked. The wait timed out; the task was not cancelled.";
const TASK_COMPLETED: &str = "Completed";
const TASK_DETAILS_HINT: &str = "Details (click to expand)";
const TASK_REPORT: &str = "**Restored report**";
const TASK_REPORT_TEXT: &str = "Restored report";
const TASK_PARTIAL_REPORT: &str = "Partial findings survive";
const TASK_CHILD_ERROR: &str = "provider unavailable";
const TASK_CHILD_USER_MESSAGE: &str = "Please retry the provider";
const TASK_FAILED: &str = "Subagent failed: provider unavailable";
const TASK_FAILED_USER_MESSAGE: &str = "Subagent failed: Please retry the provider";
const TASK_CANCEL_REASON: &str = "stopped by parent";
const TASK_CANCELLED: &str = "Subagent cancelled: stopped by parent";
const TASK_UNKNOWN_ERROR: &str = "unknown task_id";
const TASK_UNKNOWN_MESSAGE: &str =
    "Cannot retrieve this task: it is no longer available. (unknown task_id)";
const TASK_EVICTED_ERROR: &str = "turn_result_unavailable";
const TASK_EVICTED_MESSAGE: &str =
    "Cannot retrieve this turn: its result is no longer retained. (turn_result_unavailable)";
const TASK_ACCESS_ERROR: &str = "task is owned by another agent branch";
const TASK_ACCESS_MESSAGE: &str = "Cannot retrieve this task: it belongs to another agent branch. (task is owned by another agent branch)";
const TASK_LEGACY_OUTPUT: &str = "Legacy report without JSON";
const TASK_MALFORMED_OUTPUT: &str = "{\"status\":";
const TASK_LEGACY_STATUS: &str = "Wait result";
const TASK_STRUCTURED_MESSAGE: &str = "Structured result available in details.";
const TASK_EMPTY_MESSAGE: &str = "No report returned.";
const TASK_TOKEN_DETAILS: &str = "Tokens: 17 input, 9 output";
const TASK_CAPTURED_DETAILS: &str = "Captured: {\"verified\":true}";
const TASK_REPORT_PREFIX: &str = "Report line";
const TASK_REPORT_EXTRA_LINES: usize = 3;
const TASK_DURATION_MS: u64 = 2000;
const TASK_COMPLETED_WITH_DURATION: &str = "Completed · 2s";
const TASK_INPUT_TOKENS: u64 = 17;
const TASK_OUTPUT_TOKENS: u64 = 9;

fn task_retire_host(state_dir: &Path) -> common::RestartHost {
    let state = StateDir::from_path(state_dir.into());
    let mut session =
        common::TestSession::new(&common::default_model().spec(), state_dir.to_str().unwrap());
    session.save(&state).unwrap();
    let fs = Arc::new(InMemoryFs::new());
    for (name, source) in [
        ("task", TASK_SRC),
        (TASK_RETIRE_FIXTURE, TASK_RETIRE_SOURCE),
    ] {
        fs.seed(
            &state_dir.join("plugins").join(format!("{name}.lua")),
            source.as_bytes().to_vec(),
        );
    }
    common::RestartHost::open_with_provider(
        state_dir,
        session.id,
        fs,
        vec!["task".into(), TASK_RETIRE_FIXTURE.into()],
        Arc::new(common::CannedProvider::new(vec![common::canned_reply(
            TASK_RETIRE_REPLY,
        )])),
    )
}

fn task_retire_command(
    host: &PluginHost,
    command: &str,
    target: Option<TrustedTarget>,
) -> Result<(), String> {
    let completed = run_command(&host.command_registry(), command, 0, target);
    smol::block_on(common::bounded(completed.recv_async())).unwrap()
}

#[test_case::test_case(false, false; "resident")]
#[test_case::test_case(true, false; "dormant")]
#[test_case::test_case(false, true; "closed_resident")]
#[test_case::test_case(true, true; "closed_dormant")]
fn task_retire_command_preserves_tombstones_across_restart(dormant: bool, closed: bool) {
    let dir = tempfile::tempdir().unwrap();
    let host = task_retire_host(dir.path());
    let path = dir.path().join(TASK_RETIRE_IDS);
    let fixture = |host: &common::RestartHost, action: &str| {
        task_retire_command(
            &host.host,
            &format!(
                "{TASK_RETIRE_RUN} {}",
                json!({ "action": action, "path": path })
            ),
            Some(host.root.clone()),
        )
        .unwrap();
    };
    fixture(&host, "create");
    smol::block_on(common::bounded(host.manager.drain_graph_for_test())).unwrap();
    if closed {
        fixture(&host, "close");
        smol::block_on(common::bounded(host.manager.drain_graph_for_test())).unwrap();
    }
    let files = host.fs.files();
    let ids: Value =
        serde_json::from_slice(&files.iter().find(|(file, _)| file == &path).unwrap().1).unwrap();
    let id = ids["agent_id"].as_str().unwrap().parse().unwrap();
    let host = if dormant {
        host.restart_host(dir.path())
    } else {
        host
    };
    assert_eq!(host.manager.is_live(id), !dormant && !closed);
    let command = format!("{TASK_RETIRE_COMMAND} {id}");
    task_retire_command(&host.host, &command, Some(host.root.clone())).unwrap();
    assert_eq!(
        host.manager.journal().agent(id).unwrap().lifecycle,
        StoredAgentLifecycle::Retired,
    );
    fixture(&host, "inspect");
    let host = host.restart_host(dir.path());
    assert_eq!(
        host.manager.journal().agent(id).unwrap().lifecycle,
        StoredAgentLifecycle::Retired,
    );
    fixture(&host, "inspect");
    task_retire_command(&host.host, &command, Some(host.root.clone())).unwrap();
}

struct TaskRestartProvider;

impl Provider for TaskRestartProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        messages: &'a [Message],
        system: &'a str,
        _: &'a Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
        _: &'a Invocation,
        _: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if messages.first().is_some_and(|message| {
                message.content.iter().any(
                    |block| matches!(block, ContentBlock::Text { text } if text == "task work"),
                )
            }) {
                assert!(system.contains(TASK_RESTART_INSTRUCTIONS), "{system}");
                assert!(!system.contains(TASK_RESTART_CHANGED_INSTRUCTIONS));
            }
            let message = messages.last().unwrap();
            if let Some((content, is_error)) =
                message.content.iter().find_map(|block| match block {
                    ContentBlock::ToolResult {
                        content, is_error, ..
                    } => Some((content, is_error)),
                    _ => None,
                })
            {
                assert!(!is_error, "{content}");
                return Ok(common::canned_reply("verified"));
            }
            let input = message.content.iter().find_map(|block| match block {
                ContentBlock::Text { text } => serde_json::from_str::<Value>(text).ok(),
                _ => None,
            });
            Ok(match input {
                Some(input) => common::canned_tool_use(TASK_RESTART_PROBE, input),
                None => common::canned_reply(TASK_RESTART_REPLY),
            })
        })
    }
}

fn task_restart_host(state_dir: &Path) -> common::RestartHost {
    let state = StateDir::from_path(state_dir.into());
    let mut session =
        common::TestSession::new(&common::default_model().spec(), state_dir.to_str().unwrap());
    session.save(&state).unwrap();
    let fs = Arc::new(InMemoryFs::new());
    for (name, source) in [("task", TASK_SRC), ("task_restart", TASK_RESTART_SOURCE)] {
        fs.seed(
            &state_dir.join("plugins").join(format!("{name}.lua")),
            source.as_bytes().to_vec(),
        );
    }
    common::RestartHost::open_with_provider(
        state_dir,
        session.id,
        fs,
        vec!["task".into(), "task_restart".into()],
        Arc::new(TaskRestartProvider),
    )
}

fn task_restart_run(host: &common::RestartHost, input: Value) {
    let completed = run_command(
        &host.host.command_registry(),
        &format!("{TASK_RESTART_COMMAND} {input}"),
        0,
        Some(host.root.clone()),
    );
    smol::block_on(futures_lite::future::race(
        async {
            completed.recv_async().await.unwrap().unwrap();
        },
        async {
            smol::Timer::after(TASK_RESTART_TIMEOUT).await;
            panic!("task restart fixture timed out");
        },
    ));
    smol::block_on(common::bounded(host.manager.drain_graph_for_test())).unwrap();
}

fn task_restart_ids(host: &common::RestartHost, path: &Path) -> Value {
    let files = host.fs.files();
    serde_json::from_slice(&files.iter().find(|(file, _)| file == path).unwrap().1).unwrap()
}

#[test_case::test_case("research"; "research")]
#[test_case::test_case("general"; "general")]
fn task_tools_restore_policy_and_exact_turns(kind: &str) {
    use makima_agent::context::root_context;

    let dir = tempfile::tempdir().unwrap();
    let instructions_path = dir.path().join("AGENTS.md");
    std::fs::write(&instructions_path, TASK_RESTART_INSTRUCTIONS).unwrap();
    let host = task_restart_host(dir.path());
    let access = host.root.target.state_access();
    assert!(root_context(&access).unwrap().is_none());
    assert!(host.root.target.actor().unwrap().latest_ticket().is_none());
    assert!(host.root.template.root_context.is_none());
    let path = dir.path().join("task-restart.json");
    task_restart_run(
        &host,
        json!({ "action": "create", "kind": kind, "path": path }),
    );
    let recorded = root_context(&access).unwrap().unwrap();
    assert!(
        recorded
            .instructions
            .text
            .contains(TASK_RESTART_INSTRUCTIONS)
    );
    assert!(host.root.target.actor().unwrap().latest_ticket().is_none());
    assert!(host.history.load().messages.is_empty());
    let persisted =
        common::TestSession::load(host.session.id, &StateDir::from_path(dir.path().into()))
            .unwrap();
    let persisted_view = persisted.latest_state().unwrap();
    let snapshot = host.manager.journal().checkpoint_snapshot().unwrap();
    let folded =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
    assert_eq!(
        persisted_view.scopes[&Some(host.root.target.id())].context,
        folded.scopes[&Some(host.root.target.id())].context
    );
    std::fs::write(&instructions_path, TASK_RESTART_CHANGED_INSTRUCTIONS).unwrap();
    let ids = task_restart_ids(&host, &path);
    let id = ids["task_id"].as_str().unwrap().parse().unwrap();
    let record = host.manager.journal().agent(id).unwrap();
    assert_ne!(ids["task_id"], record.transcript_key);
    let policy = &folded.scopes[&None].plugins["task/policy"].value;
    assert_eq!(
        policy.as_ref().unwrap()[ids["task_id"].as_str().unwrap()],
        json!({
            "description": kind, "writes": kind == "general", "owner": ids["owner"],
        })
    );
    let host = host.restart_host(dir.path());
    assert!(!host.manager.is_live(id));
    let driver = &ids["owner"];
    task_restart_run(
        &host,
        json!({ "action": "wait", "driver": driver, "path": path }),
    );
    assert!(!host.manager.is_live(id));
    task_restart_run(
        &host,
        json!({ "action": "legacy", "driver": driver, "path": path, "legacy": record.transcript_key }),
    );
    task_restart_run(&host, json!({ "action": "denied", "path": path }));
    task_restart_run(
        &host,
        json!({ "action": "send", "driver": driver, "path": path, "mode": "plan", "blocked": kind == "general" }),
    );
    task_restart_run(
        &host,
        json!({ "action": "send", "driver": driver, "path": path, "mode": "build" }),
    );
    task_restart_run(
        &host,
        json!({ "action": "close", "driver": driver, "path": path }),
    );
    assert_eq!(
        host.manager.journal().agent(id).unwrap().lifecycle,
        StoredAgentLifecycle::Closed
    );
    task_retire_command(
        &host.host,
        &format!("{TASK_RETIRE_COMMAND} {id}"),
        Some(host.root.clone()),
    )
    .unwrap();
    task_restart_run(
        &host,
        json!({ "action": "retained", "driver": driver, "path": path }),
    );
    let host = host.restart_host(dir.path());
    task_restart_run(
        &host,
        json!({ "action": "retained", "driver": driver, "path": path }),
    );
}

#[test_case::test_case(false; "uninitialized_foreign_context")]
#[test_case::test_case(true; "recorded_foreign_context")]
fn trusted_spawn_rejects_foreign_session_coordinator(recorded: bool) {
    use makima_agent::ActorError;
    use makima_agent::agent::InstructionSource;
    use makima_agent::context::{ContextBinding, ContextPolicy};
    use makima_agent::journal::JournalCommit;

    const COMMAND: &str = "/foreign-context-fixture";
    const FOREIGN_INSTRUCTIONS: &str = "foreign session instructions";
    const SOURCE: &str = r#"
makima.api.register_command({
    name = "/foreign-context-fixture", tui_only = false,
    handler = function(_, ctx)
        local child, err = makima.agent.spawn(ctx, {
            silent = true, auto_deliver = false, mcp = false, tools = {},
        })
        assert(child == nil and err == EXPECTED_ERROR, tostring(err))
    end,
})
"#;
    let target_dir = tempfile::tempdir().unwrap();
    let foreign_dir = tempfile::tempdir().unwrap();
    let target = task_restart_host(target_dir.path());
    let foreign = task_restart_host(foreign_dir.path());
    target
        .host
        .load_source(
            "foreign_context",
            &format!(
                "local EXPECTED_ERROR = {:?}\n{SOURCE}",
                ActorError::PolicyCancelled.to_string()
            ),
        )
        .unwrap();
    if recorded {
        let instructions = makima_agent::Instructions::from_sources(vec![InstructionSource {
            path: foreign_dir.path().join("AGENTS.md"),
            label: "foreign project".into(),
            content: FOREIGN_INSTRUCTIONS.into(),
        }]);
        let (_, update) = ContextBinding::capture(foreign.root.target.state_access())
            .unwrap()
            .prepare(
                &instructions,
                &Default::default(),
                true,
                ContextPolicy::Admission,
            )
            .unwrap();
        foreign
            .manager
            .journal()
            .commit(JournalCommit {
                context_updates: vec![update],
                ..Default::default()
            })
            .unwrap();
    }
    let target_before = target.manager.journal().snapshot();
    let foreign_before = foreign.manager.journal().snapshot();
    let mut mismatched = target.root.clone();
    mismatched.template.session_id = foreign.root.template.session_id.clone();
    let completed = run_command(
        &target.host.command_registry(),
        COMMAND,
        0,
        Some(mismatched),
    );
    assert_eq!(
        smol::block_on(common::bounded(completed.recv_async())).unwrap(),
        Ok(())
    );
    for (host, before) in [(&target, target_before), (&foreign, foreign_before)] {
        let after = host.manager.journal().snapshot();
        assert_eq!(after.state, before.state);
        assert_eq!(after.agent_revisions, before.agent_revisions);
        assert_eq!(after.revision, before.revision);
        assert_eq!(host.manager.snapshot().len(), 1);
        assert!(host.root.target.actor().unwrap().latest_ticket().is_none());
    }
}

#[test]
fn task_policy_size_rejection_never_admits_orphan_work() {
    let dir = tempfile::tempdir().unwrap();
    let host = task_restart_host(dir.path());
    task_restart_run(&host, json!({ "action": "oversize" }));
    let snapshot = host.manager.journal().checkpoint_snapshot().unwrap();
    let folded =
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap();
    let children: Vec<_> = folded
        .agents
        .values()
        .filter(|record| record.depth == 2)
        .collect();
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].lifecycle, StoredAgentLifecycle::Retired);
    assert!(
        !snapshot
            .state
            .iter()
            .any(|record| record.agent == Some(children[0].id)
                && matches!(record.change, StateChange::Admitted { .. }))
    );
    assert!(
        folded
            .scopes
            .get(&None)
            .is_none_or(|scope| !scope.plugins.contains_key("task/policy"))
    );
}

struct RetirementBackend;

impl ActorBackend for RetirementBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        context: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            let reason = context.cancel_reason.cancelled().await;
            BackendResult::EnteredRun(TurnOutcome::cancelled(
                context.agent_id,
                context.turn_id.unwrap(),
                Default::default(),
                0,
                reason,
            ))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

#[test]
fn task_retire_command_rejects_busy_nonleaf_and_recovers_retained_quota() {
    let host = PluginHost::new(Arc::new(ToolRegistry::new())).unwrap();
    host.load_source("task", TASK_SRC).unwrap();
    let limits = AgentLimits {
        max_retained_agents: TASK_RETIRE_RETAINED_LIMIT,
        ..Default::default()
    };
    let manager = AgentManagerHandle::new(limits).unwrap();
    let (context, _events, _cancel) = common::ctx_with_canned_provider();
    let config = common::effective_config(&context);
    let root = manager
        .create_root_with_config(Some(config.clone()), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(common::IdleRoot) as Box<dyn ActorBackend>)
        })
        .unwrap();
    let target = common::install_root(&host, root.clone(), context);
    let spawn = |parent| {
        manager.spawn_child_trusted_with_config(
            parent,
            config.clone(),
            Default::default(),
            Vec::new(),
            None,
            |_| Ok::<_, String>(Box::new(RetirementBackend) as Box<dyn ActorBackend>),
        )
    };
    let child = spawn(&root).unwrap();
    let leaf = spawn(&child).unwrap();
    let quota_error = ManagerError::RetentionLimit {
        max: limits.max_retained_agents,
    };
    assert_eq!(spawn(&root).unwrap_err(), quota_error);
    let retire = |id| {
        task_retire_command(
            &host,
            &format!("{TASK_RETIRE_COMMAND} {id}"),
            Some(target.clone()),
        )
    };
    let nonleaf = retire(child.id()).unwrap_err();
    assert!(
        nonleaf.contains(&ManagerError::NotLeaf(child.id()).to_string()),
        "{nonleaf}"
    );
    let ticket = leaf
        .actor()
        .unwrap()
        .admit_turn(
            AgentInput::new("keep retirement busy".into(), Vec::new()),
            None,
            "retire busy".into(),
        )
        .unwrap();
    let busy = retire(leaf.id()).unwrap_err();
    assert!(
        busy.contains(&ManagerError::BusySubtree(leaf.id()).to_string()),
        "{busy}"
    );
    assert_eq!(
        manager.journal().agent(leaf.id()).unwrap().lifecycle,
        StoredAgentLifecycle::Open
    );
    assert_eq!(spawn(&root).unwrap_err(), quota_error);
    leaf.cancel().unwrap();
    smol::block_on(common::bounded(ticket.wait()));
    smol::block_on(common::bounded(manager.drain_graph_for_test())).unwrap();
    manager.close_subtree(leaf.id()).unwrap();
    smol::block_on(common::bounded(manager.drain_graph_for_test())).unwrap();
    assert_eq!(spawn(&root).unwrap_err(), quota_error);
    retire(leaf.id()).unwrap();
    assert!(ticket.peek().is_some());
    let replacement = spawn(&root).unwrap();
    assert_ne!(replacement.id(), leaf.id());
    retire(child.id()).unwrap();
    assert_eq!(
        manager.journal().agent(leaf.id()).unwrap().lifecycle,
        StoredAgentLifecycle::Retired
    );
    let report = smol::block_on(manager.shutdown(MANAGED_SHUTDOWN_TIMEOUT));
    assert!(report.timed_out.is_empty());
}

#[test]
fn task_retire_command_never_uses_selected_root_without_context() {
    let dir = tempfile::tempdir().unwrap();
    let host = task_retire_host(dir.path());
    let root = host.root.target.id();
    let before = serde_json::to_value(host.manager.journal().agent(root).unwrap()).unwrap();
    let error = task_retire_command(&host.host, &format!("{TASK_RETIRE_COMMAND} {root}"), None)
        .unwrap_err();
    assert!(error.contains(TASK_RETIRE_NO_CONTEXT), "{error}");
    assert_eq!(
        serde_json::to_value(host.manager.journal().agent(root).unwrap()).unwrap(),
        before
    );
    let unknown = MakimaId::generate();
    assert!(
        task_retire_command(
            &host.host,
            &format!("{TASK_RETIRE_COMMAND} {unknown}"),
            Some(host.root.clone()),
        )
        .is_err()
    );
    assert_eq!(
        host.manager.journal().agent(root).unwrap().lifecycle,
        StoredAgentLifecycle::Open,
    );
    assert_eq!(
        serde_json::to_value(host.manager.journal().agent(root).unwrap()).unwrap(),
        before
    );
}

fn view_lines() -> ToolOutputLines {
    ToolOutputLines {
        other: VIEW_CAP,
        index: INDEX_VIEW_CAP,
        read: READ_VIEW_CAP,
        task_spawn: VIEW_CAP,
        ..ToolOutputLines::DEFAULT
    }
}

const GREP_OUT: &str =
    "src/a.rs:\n  1: fn main() {}\n  2: fn helper() {}\n\nsrc/b.rs:\n  10: fn other() {}";

const BATCH_INPUT_GREP_BASH: &str = r#"{ "tool_calls": [
    { "tool": "grep", "parameters": { "pattern": "fn" } },
    { "tool": "bash", "parameters": { "command": "echo hello-from-bash" } }
]}"#;

const DISK_BEFORE: &str = "alpha\nbeta\n";
const DISK_WRITTEN: &str = "written\nbeta\n";
const DISK_EDITED: &str = "written\nBETA\n";
const DISK_WRITE_FAILURE: &str = "injected error after disk write";
type DiskFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

struct ProjectDiskFs {
    project: PathBuf,
    fixtures: InMemoryFs,
    fail_after_write: AtomicBool,
}

impl ProjectDiskFs {
    fn backend(&self, path: &Path) -> &dyn FsBackend {
        if path.starts_with(&self.project) {
            &RealFs
        } else {
            &self.fixtures
        }
    }
}

impl FsBackend for ProjectDiskFs {
    fn read(&self, path: PathBuf) -> DiskFuture<'_, IoResult<String>> {
        self.backend(&path).read(path)
    }
    fn read_bytes(&self, path: PathBuf) -> DiskFuture<'_, IoResult<Vec<u8>>> {
        self.backend(&path).read_bytes(path)
    }
    fn stat(&self, path: PathBuf) -> DiskFuture<'_, IoResult<FsMeta>> {
        self.backend(&path).stat(path)
    }
    fn write(&self, path: PathBuf, content: Vec<u8>) -> DiskFuture<'_, IoResult<()>> {
        self.backend(&path).write(path, content)
    }
    fn atomic_write(&self, path: PathBuf, content: Vec<u8>) -> DiskFuture<'_, IoResult<()>> {
        Box::pin(async move {
            self.backend(&path).atomic_write(path, content).await?;
            if self.fail_after_write.swap(false, Ordering::SeqCst) {
                return Err(IoError::other(DISK_WRITE_FAILURE));
            }
            Ok(())
        })
    }
    fn rm(&self, path: PathBuf, recursive: bool, force: bool) -> DiskFuture<'_, IoResult<()>> {
        self.backend(&path).rm(path, recursive, force)
    }
    fn mkdir(&self, path: PathBuf, parents: bool) -> DiskFuture<'_, IoResult<()>> {
        self.backend(&path).mkdir(path, parents)
    }
    fn dir(
        &self,
        path: PathBuf,
        depth: u32,
    ) -> DiskFuture<'_, Result<Vec<(String, &'static str)>, FsError>> {
        self.backend(&path).dir(path, depth)
    }
    fn glob(
        &self,
        patterns: Vec<String>,
        path: Option<String>,
        limit: Option<usize>,
        gitignore: bool,
        sort_mtime: bool,
    ) -> DiskFuture<'_, Result<Vec<String>, FsError>> {
        let backend = if path
            .as_ref()
            .is_some_and(|path| Path::new(path).starts_with(&self.project))
        {
            &RealFs as &dyn FsBackend
        } else {
            &self.fixtures
        };
        backend.glob(patterns, path, limit, gitignore, sort_mtime)
    }
    fn grep(
        &self,
        params: makima_agent::tools::grep::GrepParams,
    ) -> DiskFuture<'_, Result<(PathBuf, Vec<makima_agent::GrepFileEntry>), FsError>> {
        self.fixtures.grep(params)
    }
}

struct DiskTurnBackend {
    turn: flume::Sender<makima_agent::CurrentManagedTurn>,
}

impl ActorBackend for DiskTurnBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        turn: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> DiskFuture<'a, BackendResult> {
        Box::pin(async move {
            self.turn.send(turn.managed_turn.unwrap()).unwrap();
            turn.cancel.cancelled().await;
            BackendResult::EnteredRun(TurnOutcome::completed(
                turn.agent_id,
                turn.turn_id.unwrap(),
                Default::default(),
                0,
                DoneReason::EndTurn,
            ))
        })
    }
    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> DiskFuture<'a, BackendResult> {
        Box::pin(async { BackendResult::ControlDone })
    }
    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> DiskFuture<'a, BackendResult> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

struct DiskRestoreHost {
    _host: PluginHost,
    manager: AgentManagerHandle,
    checkpoint: SessionLogCheckpoint,
    context: ToolContext,
    backend: Arc<ProjectDiskFs>,
    state: StateDir,
    _cancel: CancelTrigger,
}

impl DiskRestoreHost {
    fn new(directory: &Path) -> Self {
        let project = directory.join("project");
        std::fs::create_dir(&project).unwrap();
        let state = StateDir::from_path(directory.join("state"));
        std::fs::create_dir_all(state.path()).unwrap();
        let mut session =
            common::TestSession::new(&common::default_model().spec(), project.to_str().unwrap());
        session.save(&state).unwrap();
        let registry = Arc::new(ToolRegistry::new());
        let backend = Arc::new(ProjectDiskFs {
            project,
            fixtures: InMemoryFs::new(),
            fail_after_write: AtomicBool::new(false),
        });
        let mut host = PluginHost::with_session_provider_preparer(
            registry.clone(),
            Default::default(),
            true,
            backend.clone(),
            Some(state.path().to_path_buf()),
            None,
            makima_lua::ui_sink::HostUi::Frontend,
        )
        .unwrap();
        host.load_builtins(&makima_config::PluginsConfig {
            enabled: true,
            names: vec!["write".into(), "edit".into()],
            ..Default::default()
        })
        .unwrap();
        let (mut context, cancel) = common::production_like_ctx();
        context.cwd = backend.project.clone();
        context.session_id = Some(session.id.into());
        context.offload = Some(Arc::new(OffloadStore::for_session(
            state.path(),
            context.session_id.as_ref().unwrap(),
        )));
        context.registry = registry;
        context.modes = host.mode_registry();
        context.turn_bindings = Arc::new(TurnToolBindings::capture(
            &context.registry,
            &context.local_tools,
            context.mcp.as_ref(),
        ));
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        manager.journal().bind_session(session.id).unwrap();
        let config = common::effective_config(&context)
            .with_context(AgentContextSnapshot::new(context.cwd.clone()));
        let (turn_tx, turn_rx) = flume::bounded(1);
        let root =
            manager
                .create_root_with_config(Some(config), Vec::new(), None, |_| {
                    Ok::<_, String>(
                        Box::new(DiskTurnBackend { turn: turn_tx }) as Box<dyn ActorBackend>
                    )
                })
                .unwrap();
        let checkpoint =
            SessionLogCheckpoint::open_with_session(state.clone(), session, None).unwrap();
        smol::block_on(common::bounded(checkpoint.bind_journal(manager.journal()))).unwrap();
        context.execution_agent_id = Some(root.id());
        root.actor()
            .unwrap()
            .admit_turn(
                AgentInput::new("disk restore acceptance".into(), Vec::new()),
                None,
                "disk restore".into(),
            )
            .unwrap();
        context.managed_turn = Some(smol::block_on(common::bounded(turn_rx.recv_async())).unwrap());
        common::install_root(&host, root, context.clone());
        Self {
            _host: host,
            manager,
            checkpoint,
            context,
            backend,
            state,
            _cancel: cancel,
        }
    }

    fn dispatch(&self, tool: &str, input: Value) -> makima_agent::ToolDoneEvent {
        smol::block_on(common::bounded(tool_dispatch::run(
            "".into(),
            tool,
            &input,
            &self.context,
            CallOrigin::Nested,
        )))
    }

    fn finish(self) -> (StateDir, common::TestSession) {
        let state = self.state.clone();
        let id = self.context.session_id.as_ref().unwrap().id();
        drop(self);
        let session = common::TestSession::load(id, &state).unwrap();
        (state, session)
    }
}

impl Drop for DiskRestoreHost {
    fn drop(&mut self) {
        let report = smol::block_on(common::bounded(
            self.manager.shutdown(MANAGED_SHUTDOWN_TIMEOUT),
        ));
        assert!(report.timed_out.is_empty());
        smol::block_on(common::bounded(self.checkpoint.drain())).unwrap();
    }
}

fn disk_endpoint(session: &common::TestSession) -> RestoreEndpoint {
    RestoreEndpoint {
        session: session.id,
        branch: None,
        seq: session.sequence_high_water().unwrap_or_default(),
        digest: file_restore::endpoint_digest(session).unwrap(),
    }
}

fn disk_restore_request(
    state: &StateDir,
    session: &common::TestSession,
    preview: RestorePreview,
) -> RestoreRequest {
    RestoreRequest {
        sessions_dir: state.path().join(SESSIONS_DIR),
        owner: session.id,
        source: disk_endpoint(session),
        target: disk_endpoint(session),
        choice: RestoreChoice::FilesOnly,
        preview,
        projection: None,
        locks: Arc::new(FileWriteLocks::new()),
        cancel: CancelToken::none(),
    }
}

fn disk_preview(session: &common::TestSession, after_seq: u64) -> RestorePreview {
    file_restore::plan(&[RestoreSegment {
        session: session.clone(),
        after_seq,
        through_seq: session.sequence_high_water().unwrap(),
        affected: [session.root_agent_id().unwrap()].into(),
    }])
    .unwrap()
}

fn disk_bytes(blobs: &BlobStore, state: &FileState) -> Option<Vec<u8>> {
    match state {
        FileState::Absent => None,
        FileState::Blob(reference) => Some(blobs.read(reference).unwrap()),
    }
}

#[test_case::test_case(false, false; "existing_write_edit")]
#[test_case::test_case(true, false; "creation_write_edit")]
#[test_case::test_case(false, true; "error_after_write_captures_actual_postimage")]
fn bundled_disk_mutations_capture_and_restore(created: bool, fail: bool) {
    let directory = tempfile::tempdir().unwrap();
    let fixture = DiskRestoreHost::new(directory.path());
    let path = fixture.backend.project.join("file.txt");
    if !created {
        std::fs::write(&path, DISK_BEFORE).unwrap();
    }
    let start = fixture.manager.journal().snapshot().seq;
    fixture
        .backend
        .fail_after_write
        .store(fail, Ordering::SeqCst);
    let written = fixture.dispatch("write", json!({ "path": path, "content": DISK_WRITTEN }));
    assert_eq!(written.is_error, fail, "{}", written.output.as_text());
    if fail {
        assert!(written.output.as_text().contains(DISK_WRITE_FAILURE));
    }
    assert_eq!(std::fs::read_to_string(&path).unwrap(), DISK_WRITTEN);
    let edited = fixture.dispatch(
        "edit",
        json!({ "path": path, "old_string": "beta", "new_string": "BETA" }),
    );
    assert!(!edited.is_error, "{}", edited.output.as_text());
    let (state, mut session) = fixture.finish();
    let records: Vec<_> = session
        .state()
        .iter()
        .filter_map(|record| match &record.change {
            StateChange::File {
                path: recorded,
                before,
                after,
            } if recorded == &path => Some((record.agent, before, after)),
            _ => None,
        })
        .collect();
    assert_eq!(records.len(), 2);
    let blobs = BlobStore::new(state.clone());
    assert_eq!(records[0].0, session.root_agent_id());
    assert_eq!(
        disk_bytes(&blobs, records[0].1).as_deref(),
        (!created).then_some(DISK_BEFORE.as_bytes())
    );
    assert_eq!(
        disk_bytes(&blobs, records[0].2).unwrap(),
        DISK_WRITTEN.as_bytes()
    );
    assert_eq!(records[0].2, records[1].1);
    assert_eq!(
        disk_bytes(&blobs, records[1].2).unwrap(),
        DISK_EDITED.as_bytes()
    );
    let preview = disk_preview(&session, start);
    assert_eq!(preview.paths.len(), 1);
    smol::block_on(common::bounded(async {
        let mut undo = file_restore::prepare(disk_restore_request(&state, &session, preview))
            .await
            .unwrap();
        assert!(
            undo.apply(&makima_agent::ModeRegistry::builtin())
                .unwrap()
                .warnings
                .is_empty()
        );
        assert_eq!(
            std::fs::read(&path).ok().as_deref(),
            (!created).then_some(DISK_BEFORE.as_bytes())
        );
        let reference = undo.transition_ref();
        session.push_restore_ref(reference.clone());
        session.save(&state).unwrap();
        undo.complete().unwrap();
        let inverse =
            file_restore::plan_inverse(&state.path().join(SESSIONS_DIR), &reference).unwrap();
        let mut redo = file_restore::prepare(disk_restore_request(&state, &session, inverse))
            .await
            .unwrap();
        redo.apply(&makima_agent::ModeRegistry::builtin()).unwrap();
        session.push_restore_ref(redo.transition_ref());
        session.save(&state).unwrap();
        redo.complete().unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), DISK_EDITED);
    }));
}

#[test_case::test_case(false, false; "existing_unpublished_rolls_back")]
#[test_case::test_case(false, true; "existing_published_finishes_forward")]
#[test_case::test_case(true, false; "creation_unpublished_rolls_back")]
#[test_case::test_case(true, true; "creation_published_finishes_forward")]
fn bundled_disk_restore_recovers_persisted_manifest_after_restart(created: bool, publish: bool) {
    let directory = tempfile::tempdir().unwrap();
    let fixture = DiskRestoreHost::new(directory.path());
    let path = fixture.backend.project.join("file.txt");
    if !created {
        std::fs::write(&path, DISK_BEFORE).unwrap();
    }
    let start = fixture.manager.journal().snapshot().seq;
    let written = fixture.dispatch("write", json!({ "path": path, "content": DISK_WRITTEN }));
    assert!(!written.is_error, "{}", written.output.as_text());
    let edited = fixture.dispatch(
        "edit",
        json!({ "path": path, "old_string": "beta", "new_string": "BETA" }),
    );
    assert!(!edited.is_error, "{}", edited.output.as_text());
    let (state, mut session) = fixture.finish();
    let preview = disk_preview(&session, start);
    assert_eq!(preview.paths.len(), 1);
    let blobs = BlobStore::new(state.clone());
    let transition = RestoreTransition {
        id: MakimaId::generate(),
        choice: RestoreChoice::FilesOnly,
        source: disk_endpoint(&session),
        target: disk_endpoint(&session),
        inverse_of: None,
        paths: preview
            .paths
            .into_iter()
            .map(|file| PathRestore {
                path: file.path,
                before: file.expected,
                after: file.desired,
            })
            .collect(),
        skipped: Vec::new(),
    };
    let store = RestoreStore::new(state.clone());
    let reference = store.prepare(session.id, transition).unwrap();
    let durable = store.load(&reference).unwrap();
    let desired = disk_bytes(&blobs, &durable.transition.paths[0].after);
    match desired {
        Some(bytes) => std::fs::write(&path, bytes).unwrap(),
        None => std::fs::remove_file(&path).unwrap(),
    }
    assert!(durable.progress.is_empty());
    if publish {
        session.push_restore_ref(reference.clone());
        session.save(&state).unwrap();
    }
    let published_state = common::normalized_history_epoch_state(session.state());
    let id = session.id;
    drop(store);
    let restarted_state = StateDir::from_path(directory.path().join("state"));
    let restarted_session = common::TestSession::load(id, &restarted_state).unwrap();
    assert_eq!(
        restarted_session.restore_refs().contains(&reference),
        publish
    );
    let locks = Arc::new(FileWriteLocks::new());
    smol::block_on(common::bounded(file_restore::reconcile_session(
        &restarted_state.path().join(SESSIONS_DIR),
        id,
        locks.clone(),
        CancelToken::none(),
    )))
    .unwrap();
    let expected = if publish {
        (!created).then_some(DISK_BEFORE.as_bytes())
    } else {
        Some(DISK_EDITED.as_bytes())
    };
    assert_eq!(std::fs::read(&path).ok().as_deref(), expected);
    let restarted_store = RestoreStore::new(restarted_state.clone());
    let repaired = restarted_store.load(&reference).unwrap();
    assert!(repaired.progress.contains(&RestoreProgress::Complete));
    assert_eq!(
        repaired.progress.contains(&RestoreProgress::RollbackIntent),
        !publish
    );
    assert!(restarted_store.pending(id).unwrap().is_empty());
    smol::block_on(common::bounded(file_restore::reconcile_session(
        &restarted_state.path().join(SESSIONS_DIR),
        id,
        locks,
        CancelToken::none(),
    )))
    .unwrap();
    assert_eq!(std::fs::read(&path).ok().as_deref(), expected);
    assert_eq!(
        common::normalized_history_epoch_state(
            common::TestSession::load(id, &restarted_state)
                .unwrap()
                .state()
        ),
        published_state
    );
    assert_eq!(
        restarted_store.load(&reference).unwrap().progress,
        repaired.progress
    );
}

fn load_host() -> PluginHost {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source("bash", BASH_SRC).unwrap();
    host.load_source("glob", GLOB_SRC).unwrap();
    host.load_source("grep", GREP_SRC).unwrap();
    host.load_source("batch", BATCH_SRC).unwrap();
    host.load_source("task", TASK_SRC).unwrap();
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
    spans: Vec<SnapshotLine>,
}

fn restore(
    host: &PluginHost,
    tool: &str,
    input: Value,
    output: &str,
    state: Option<Value>,
    clicks: Vec<usize>,
) -> Restored {
    restore_as(host, tool, input, output, false, state, clicks)
}

fn restore_as(
    host: &PluginHost,
    tool: &str,
    input: Value,
    output: &str,
    is_error: bool,
    state: Option<Value>,
    clicks: Vec<usize>,
) -> Restored {
    restore_item(
        host,
        makima_lua::RestoreItem {
            state_target: None,
            tool: Arc::from(tool),
            tool_use_id: "restore_id".to_owned(),
            output: output.to_owned(),
            input,
            is_error,
            tool_output_lines: view_lines(),
            theme_gen: None,
            clicks,
            state,
        },
    )
}

fn restore_item(host: &PluginHost, item: makima_lua::RestoreItem) -> Restored {
    let handle = host.event_handle();
    let (tx, rx) = flume::unbounded();
    handle.request_restore(item, makima_agent::EventSender::new(tx));
    handle.wait_restore_complete_for_test();
    // The empty LoadSource drains the async gate, so spawned highlight tasks
    // finish before we inspect the buffers.
    host.load_source("barrier", "").unwrap();
    let mut out = Restored {
        body: String::new(),
        header: String::new(),
        spans: Vec::new(),
    };
    for env in rx.drain() {
        match env.event {
            AgentEvent::ToolSnapshot { snapshot, .. } => {
                out.body = snapshot.text();
                out.spans = snapshot.lines.to_vec();
            }
            AgentEvent::ToolHeaderSnapshot { snapshot, .. } => out.header = snapshot.text(),
            _ => {}
        }
    }
    out
}

const GREP_VIEW_CAP: usize = 2;
const GLOB_OUT: &str = "/a/1.txt\n/a/2.txt\n/a/3.txt\n/a/4.txt";

fn restore_with_capped_grep(host: &PluginHost, tool: &str, output: &str) -> Restored {
    let lines = ToolOutputLines {
        grep: GREP_VIEW_CAP,
        other: VIEW_CAP,
        ..view_lines()
    };
    restore_item(
        host,
        makima_lua::RestoreItem {
            state_target: None,
            tool: Arc::from(tool),
            tool_use_id: "restore_id".to_owned(),
            output: output.to_owned(),
            input: json!({ "pattern": "fn" }),
            is_error: false,
            tool_output_lines: lines,
            theme_gen: None,
            clicks: Vec::new(),
            state: None,
        },
    )
}

#[test]
fn grep_view_honors_the_grep_output_limit() {
    let host = load_host();
    let restored = restore_with_capped_grep(&host, "grep", GREP_OUT);
    assert!(restored.body.contains("src/a.rs"), "{}", restored.body);
    assert!(restored.body.contains("fn main"), "{}", restored.body);
    assert!(!restored.body.contains("fn helper"), "{}", restored.body);
    assert!(restored.body.contains(EXPAND_HINT), "{}", restored.body);
}

#[test]
fn glob_view_honors_the_grep_output_limit() {
    let host = load_host();
    let restored = restore_with_capped_grep(&host, "glob", GLOB_OUT);
    assert!(restored.body.contains("1.txt"), "{}", restored.body);
    assert!(restored.body.contains("2.txt"), "{}", restored.body);
    assert!(!restored.body.contains("3.txt"), "{}", restored.body);
    assert!(restored.body.contains(EXPAND_HINT), "{}", restored.body);
}

#[test_case::test_case(Some(TASK_DESCRIPTION); "persisted_description")]
#[test_case::test_case(None; "short_id_fallback")]
fn task_wait_restore_running_is_historical(description: Option<&str>) {
    let host = load_host();
    let restored = restore(
        &host,
        TASK_WAIT_TOOL,
        json!({ "task_id": TASK_ID }),
        &json!({ "status": "running" }).to_string(),
        description.map(|description| json!({ "description": description })),
        Vec::new(),
    );

    assert_eq!(restored.header.trim(), description.unwrap_or(TASK_SHORT_ID));
    assert_eq!(
        restored.body.lines().collect::<Vec<_>>(),
        [TASK_RUNNING, TASK_DETAILS_HINT]
    );
}

#[test_case::test_case(false; "without_duration")]
#[test_case::test_case(true; "with_duration")]
fn task_wait_restore_done_renders_markdown(with_duration: bool) {
    let host = load_host();
    let mut result = json!({ "text": TASK_REPORT });
    if with_duration {
        result["duration_ms"] = json!(TASK_DURATION_MS);
    }
    let restored = restore(
        &host,
        TASK_WAIT_TOOL,
        json!({ "task_id": TASK_ID }),
        &json!({ "status": "done", "result": result }).to_string(),
        None,
        Vec::new(),
    );

    let status = if with_duration {
        TASK_COMPLETED_WITH_DURATION
    } else {
        TASK_COMPLETED
    };
    assert_eq!(restored.body.lines().next(), Some(status));
    assert!(
        restored.body.contains(TASK_REPORT_TEXT),
        "{}",
        restored.body
    );
    assert!(!restored.body.contains(TASK_REPORT), "{}", restored.body);
    assert!(
        restored
            .spans
            .iter()
            .flat_map(|line| &line.spans)
            .any(|span| {
                span.text.contains(TASK_REPORT_TEXT)
                    && matches!(&span.style, SpanStyle::Named(style) if style == "bold")
            })
    );
}

#[test_case::test_case(
    json!({ "status": "failed", "error": TASK_CHILD_ERROR }), TASK_FAILED;
    "failed"
)]
#[test_case::test_case(
    json!({ "status": "failed", "error": TASK_CHILD_ERROR, "user_message": TASK_CHILD_USER_MESSAGE }),
    TASK_FAILED_USER_MESSAGE;
    "failed_user_message"
)]
#[test_case::test_case(
    json!({ "status": "cancelled", "error": TASK_CANCEL_REASON }), TASK_CANCELLED;
    "cancelled_error"
)]
#[test_case::test_case(
    json!({ "status": "cancelled", "cancellation_reason": TASK_CANCEL_REASON }), TASK_CANCELLED;
    "cancelled_reason"
)]
fn task_wait_restore_child_failure_keeps_partial_report(mut result: Value, status: &str) {
    let host = load_host();
    result["text"] = json!(TASK_PARTIAL_REPORT);
    let restored = restore(
        &host,
        TASK_WAIT_TOOL,
        json!({ "task_id": TASK_ID }),
        &json!({ "status": "done", "result": result }).to_string(),
        None,
        Vec::new(),
    );

    assert_eq!(restored.body.lines().next(), Some(status));
    assert!(
        restored.body.contains(TASK_PARTIAL_REPORT),
        "{}",
        restored.body
    );
    assert!(matches!(
        &restored.spans[0].spans[0].style,
        SpanStyle::Named(style) if style == "error"
    ));
}

#[test_case::test_case(TASK_UNKNOWN_ERROR, TASK_UNKNOWN_MESSAGE; "unknown_task")]
#[test_case::test_case(TASK_EVICTED_ERROR, TASK_EVICTED_MESSAGE; "evicted_turn")]
#[test_case::test_case(TASK_ACCESS_ERROR, TASK_ACCESS_MESSAGE; "access_denied")]
fn task_wait_restore_wait_errors_are_not_child_failures(output: &str, status: &str) {
    let host = load_host();
    let restored = restore_as(
        &host,
        TASK_WAIT_TOOL,
        json!({ "task_id": TASK_ID }),
        output,
        true,
        None,
        Vec::new(),
    );

    assert_eq!(
        restored.body.lines().collect::<Vec<_>>(),
        [status, TASK_DETAILS_HINT]
    );
    assert!(matches!(
        &restored.spans[0].spans[0].style,
        SpanStyle::Named(style) if style == "error"
    ));
}

#[test_case::test_case(TASK_LEGACY_OUTPUT; "legacy_text")]
#[test_case::test_case(TASK_MALFORMED_OUTPUT; "malformed_json")]
fn task_wait_restore_legacy_output_uses_text_fallback(output: &str) {
    let host = load_host();
    let restored = restore(
        &host,
        TASK_WAIT_TOOL,
        json!({ "task_id": TASK_ID }),
        output,
        None,
        Vec::new(),
    );

    assert_eq!(restored.body.lines().next(), Some(TASK_LEGACY_STATUS));
    assert!(restored.body.contains(output), "{}", restored.body);
    assert!(
        restored.body.contains(TASK_DETAILS_HINT),
        "{}",
        restored.body
    );
}

#[test_case::test_case(false, TASK_EMPTY_MESSAGE; "no_report")]
#[test_case::test_case(true, TASK_STRUCTURED_MESSAGE; "structured_result")]
fn task_wait_restore_empty_report_and_expanded_details(captured: bool, message: &str) {
    let host = load_host();
    let mut result = json!({
        "input_tokens": TASK_INPUT_TOKENS, "output_tokens": TASK_OUTPUT_TOKENS,
    });
    if captured {
        result["turn_id"] = json!(TASK_TURN_ID);
        result["captured"] = json!({ "verified": true });
    }
    let input = json!({ "task_id": TASK_ID, "turn_id": TASK_INPUT_TURN_ID });
    let output = json!({ "status": "done", "result": result }).to_string();
    let collapsed = restore(
        &host,
        TASK_WAIT_TOOL,
        input.clone(),
        &output,
        None,
        Vec::new(),
    );
    assert!(collapsed.body.contains(message), "{}", collapsed.body);
    assert!(!collapsed.body.contains(TASK_ID), "{}", collapsed.body);
    assert!(
        !collapsed.body.contains(TASK_TOKEN_DETAILS),
        "{}",
        collapsed.body
    );
    assert!(
        !collapsed.body.contains(TASK_CAPTURED_DETAILS),
        "{}",
        collapsed.body
    );

    let expanded = restore(&host, TASK_WAIT_TOOL, input.clone(), &output, None, vec![0]);
    assert!(expanded.body.contains(message), "{}", expanded.body);
    assert!(
        expanded.body.contains(&format!("Task: {TASK_ID}")),
        "{}",
        expanded.body
    );
    let turn_id = if captured {
        TASK_TURN_ID
    } else {
        TASK_INPUT_TURN_ID
    };
    assert!(
        expanded.body.contains(&format!("Turn: {turn_id}")),
        "{}",
        expanded.body
    );
    assert!(
        expanded.body.contains(TASK_TOKEN_DETAILS),
        "{}",
        expanded.body
    );
    assert_eq!(expanded.body.contains(TASK_CAPTURED_DETAILS), captured);
    assert!(!expanded.body.contains(EXPAND_HINT), "{}", expanded.body);

    let recollapsed = restore(&host, TASK_WAIT_TOOL, input, &output, None, vec![0, 0]);
    assert_eq!(recollapsed.body, collapsed.body);
}

#[test_case::test_case(false; "standalone")]
#[test_case::test_case(true; "batch_child")]
fn task_send_restore_message_truncation_click_roundtrip(in_batch: bool) {
    let host = load_host();
    let message = (1..=VIEW_CAP + TASK_REPORT_EXTRA_LINES)
        .map(|line| format!("{TASK_SEND_LINE_PREFIX} {line}"))
        .collect::<Vec<_>>()
        .join("\n\n");
    let output = json!({ "queued": true, "turn_id": TASK_TURN_ID }).to_string();
    let input = json!({ "task_id": TASK_ID, "message": message });
    let state = json!({ "description": TASK_DESCRIPTION });
    let (tool, input, state) = if in_batch {
        (
            "batch",
            json!({ "tool_calls": [{ "tool": TASK_SEND_TOOL, "parameters": input }] }),
            json!({ "children": [{
                "tool": TASK_SEND_TOOL, "status": "success", "output": output, "state": state,
            }] }),
        )
    } else {
        (TASK_SEND_TOOL, input, state)
    };
    let collapsed = restore(
        &host,
        tool,
        input.clone(),
        &output,
        Some(state.clone()),
        Vec::new(),
    );
    let tail = format!(
        "{TASK_SEND_LINE_PREFIX} {}",
        VIEW_CAP + TASK_REPORT_EXTRA_LINES
    );
    if !in_batch {
        assert_eq!(
            collapsed.header.trim(),
            format!("To \"{TASK_DESCRIPTION}\"")
        );
    }
    assert!(
        collapsed
            .body
            .contains(&format!("{TASK_SEND_QUOTE_BAR}{TASK_SEND_LINE_PREFIX} 1")),
        "{}",
        collapsed.body
    );
    assert!(!collapsed.body.contains(&tail), "{}", collapsed.body);
    assert!(!collapsed.body.contains(TASK_ID), "{}", collapsed.body);
    let notice_row = 1 + collapsed
        .body
        .lines()
        .position(|line| line.contains(EXPAND_HINT))
        .unwrap();
    let expanded = restore(
        &host,
        tool,
        input.clone(),
        &output,
        Some(state.clone()),
        vec![notice_row],
    );
    assert!(
        expanded
            .body
            .contains(&format!("{TASK_SEND_QUOTE_BAR}{tail}")),
        "{}",
        expanded.body
    );
    assert!(!expanded.body.contains(TASK_ID), "{}", expanded.body);
    assert!(!expanded.body.contains(EXPAND_HINT), "{}", expanded.body);
    let recollapsed = restore(
        &host,
        tool,
        input,
        &output,
        Some(state),
        vec![notice_row, notice_row],
    );
    assert_eq!(recollapsed.body, collapsed.body);
}

#[test_case::test_case(false; "standalone")]
#[test_case::test_case(true; "batch_child")]
fn task_wait_restore_report_truncation_click_roundtrip(in_batch: bool) {
    let host = load_host();
    let report = (1..=VIEW_CAP + TASK_REPORT_EXTRA_LINES)
        .map(|line| format!("{TASK_REPORT_PREFIX} {line}"))
        .collect::<Vec<_>>()
        .join("\n\n");
    let output = json!({ "status": "done", "result": {
        "text": report, "turn_id": TASK_TURN_ID,
    }})
    .to_string();
    let input = json!({ "task_id": TASK_ID });
    let state = json!({ "description": TASK_DESCRIPTION });
    let (tool, input, state) = if in_batch {
        (
            "batch",
            json!({ "tool_calls": [{ "tool": TASK_WAIT_TOOL, "parameters": input }] }),
            json!({ "children": [{
                "tool": TASK_WAIT_TOOL, "status": "success", "output": output, "state": state,
            }] }),
        )
    } else {
        (TASK_WAIT_TOOL, input, state)
    };
    let collapsed = restore(
        &host,
        tool,
        input.clone(),
        &output,
        Some(state.clone()),
        Vec::new(),
    );
    let tail = format!(
        "{TASK_REPORT_PREFIX} {}",
        VIEW_CAP + TASK_REPORT_EXTRA_LINES
    );
    assert!(
        collapsed.body.contains(TASK_COMPLETED),
        "{}",
        collapsed.body
    );
    assert!(
        collapsed.body.contains(&format!("{TASK_REPORT_PREFIX} 1")),
        "{}",
        collapsed.body
    );
    assert!(!collapsed.body.contains(&tail), "{}", collapsed.body);
    assert!(
        !collapsed.body.contains(TASK_DETAILS_HINT),
        "{}",
        collapsed.body
    );
    let notice_row = 1 + collapsed
        .body
        .lines()
        .position(|line| line.contains(EXPAND_HINT))
        .unwrap();
    let expanded = restore(
        &host,
        tool,
        input.clone(),
        &output,
        Some(state.clone()),
        vec![notice_row],
    );
    assert!(expanded.body.contains(&tail), "{}", expanded.body);
    assert!(
        expanded.body.contains(&format!("Task: {TASK_ID}")),
        "{}",
        expanded.body
    );
    assert!(
        expanded.body.contains(&format!("Turn: {TASK_TURN_ID}")),
        "{}",
        expanded.body
    );
    assert!(!expanded.body.contains(EXPAND_HINT), "{}", expanded.body);
    let recollapsed = restore(
        &host,
        tool,
        input,
        &output,
        Some(state),
        vec![notice_row, notice_row],
    );
    assert_eq!(recollapsed.body, collapsed.body);
    assert_eq!(recollapsed.header, collapsed.header);
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
    assert!(text.contains(" 1 fn main() {}"), "grep gutter: {text}");
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
        text.contains("10 fn other() {}"),
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
        r#"makima.api.register_tool({
            name = "hdr_await",
            description = "t",
            schema = { type = "object", properties = {} },
            handler = function() return "ok" end,
            header = function(input)
                local hl = makima.ui.highlight("echo marker", "bash") or { { { "echo marker" } } }
                local buf = makima.ui.buf()
                buf:set_lines(hl)
                return buf
            end,
            restore = function(input, output)
                local buf = makima.ui.buf()
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
    spans: Vec<SnapshotLine>,
    output: String,
    state: Option<Value>,
    annotation: Option<String>,
}

fn exec_live(host: &PluginHost, reg: &ToolRegistry, tool: &str, input: Value) -> Live {
    exec_live_with(host, reg, tool, input, |_| {})
}

fn exec_live_with(
    host: &PluginHost,
    reg: &ToolRegistry,
    tool: &str,
    input: Value,
    shape: impl FnOnce(&mut ToolContext),
) -> Live {
    let (tx, rx) = flume::unbounded();
    let event_tx = makima_agent::EventSender::new(tx);
    let mut ctx = makima_agent::tools::test_support::stub_ctx_with(
        &makima_agent::AgentMode::Build,
        Some(&event_tx),
        Some(LIVE_TOOL_USE_ID),
    );
    ctx.tool_output_lines = view_lines();
    shape(&mut ctx);
    let inv = reg
        .get(tool)
        .unwrap_or_else(|| panic!("tool {tool} not registered"))
        .tool
        .parse(&input)
        .expect("parse failed");
    let result = smol::block_on(async { inv.execute(&ctx).await });
    host.load_source("live_barrier", "").unwrap();
    let mut body = String::new();
    let mut spans = Vec::new();
    for env in rx.drain() {
        if let AgentEvent::ToolSnapshot { snapshot, .. } = env.event {
            body = snapshot.text();
            spans = snapshot.lines.to_vec();
        }
    }
    let (output, state) = match result.output.expect("tool failed") {
        makima_agent::ToolOutput::Plain(s) | makima_agent::ToolOutput::Markdown(s) => {
            (s.text, s.state)
        }
        other => panic!("unexpected output: {other:?}"),
    };
    Live {
        body,
        spans,
        output,
        state,
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

const READ_TOOL: &str = "read";
const RUST_SRC: &str = "fn main() {\n    let x = 42;\n    println!(\"{x}\");\n}\n";
const RUST_LINES: usize = 4;

fn has_syntax_colors(lines: &[SnapshotLine]) -> bool {
    lines
        .iter()
        .flat_map(|l| &l.spans)
        .any(|s| matches!(&s.style, SpanStyle::Inline(inline) if inline.fg.is_some()))
}

/// A finished tool is re-rendered through `restore` once its warm buffer is
/// gone (cache eviction, theme rebake), so restore must produce the same
/// highlighted view the live handler did, truncation notice included.
/// Anything less shows up as "clicking lost the syntax highlighting".
/// `path_key` is the model's spelling: restore gets the raw input, before
/// alias resolution, so the view must not depend on it.
#[test_case::test_case(0, "path" ; "whole_file")]
#[test_case::test_case(2, "path" ; "truncated")]
#[test_case::test_case(0, "file_path" ; "aliased_path")]
fn read_renders_identically_live_and_restored(limit: usize, path_key: &str) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("main.rs");
    std::fs::write(&path, RUST_SRC).unwrap();
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let input = json!({ path_key: path.to_str().unwrap(), "offset": 1, "limit": limit });

    let live = exec_live(&host, &reg, READ_TOOL, input.clone());
    let restored = restore(
        &host,
        READ_TOOL,
        input,
        &live.output,
        live.state,
        Vec::new(),
    );

    assert!(
        has_syntax_colors(&live.spans),
        "live read view is syntax highlighted: {}",
        live.body
    );
    if limit > 0 && limit < RUST_LINES {
        assert!(
            live.body.contains("Truncated"),
            "live view notes the cut: {}",
            live.body
        );
    }
    assert_eq!(
        restored.spans, live.spans,
        "restored read view must match the live one, colors included"
    );
}

const GREP_TOOL: &str = "grep";

/// Same contract as read: grep's restore used to rebuild a plain view from
/// the LLM output while the live handler highlighted, so an expand that went
/// through restore dropped the colors.
#[test]
fn grep_renders_identically_live_and_restored() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.rs"), "fn one() {}\nfn two() {}\n").unwrap();
    std::fs::write(dir.path().join("b.rs"), "fn three() {}\n").unwrap();
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let input = json!({ "pattern": "fn", "path": dir.path().to_str().unwrap() });

    let live = exec_live(&host, &reg, GREP_TOOL, input.clone());
    let restored = restore(&host, GREP_TOOL, input, &live.output, None, Vec::new());

    assert!(
        has_syntax_colors(&live.spans),
        "live grep view is syntax highlighted: {}",
        live.body
    );
    assert_eq!(
        restored.spans, live.spans,
        "restored grep view must match the live one, colors included"
    );
}

/// The host appends its truncation marker after the last entry. The view
/// has to keep it, or a cut result set looks complete.
#[test]
fn grep_restore_keeps_truncation_marker() {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let output = format!("src/a.rs:\n  1: fn main() {{}}\n\n{FILE_TRUNCATED_MARKER}");
    let restored = restore(
        &host,
        GREP_TOOL,
        json!({ "pattern": "fn" }),
        &output,
        None,
        Vec::new(),
    );
    assert!(
        restored.body.contains("fn main() {}"),
        "entry rendered: {}",
        restored.body
    );
    assert!(
        restored.body.ends_with(FILE_TRUNCATED_MARKER),
        "marker kept as the last line: {}",
        restored.body
    );
}

const GREP_MATCH_FILES: usize = 40;
const SMALL_OUTPUT_LINES: usize = 12;

fn many_match_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..GREP_MATCH_FILES {
        std::fs::write(dir.path().join(format!("f{i}.rs")), "fn one() {}\n").unwrap();
    }
    dir
}

fn offloading(store: &Arc<OffloadStore>) -> impl FnOnce(&mut ToolContext) {
    let store = Arc::clone(store);
    move |ctx| {
        ctx.offload = Some(store);
        ctx.config.max_output_lines = SMALL_OUTPUT_LINES;
    }
}

#[test]
fn grep_offload_restore_keeps_preview_and_notice() {
    let dir = many_match_dir();
    let store_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(OffloadStore::on_disk(
        store_dir.path().canonicalize().unwrap(),
    ));
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let input = json!({ "pattern": "fn", "path": dir.path().to_str().unwrap() });

    let live = exec_live_with(&host, &reg, GREP_TOOL, input.clone(), offloading(&store));
    let preview_matches = live.output.matches("fn one() {}").count();
    assert!(preview_matches > 0 && preview_matches < GREP_MATCH_FILES);
    let notice = live
        .output
        .lines()
        .find(|line| is_offload_notice(line))
        .unwrap();
    let restored = restore(&host, GREP_TOOL, input, &live.output, None, vec![0]);
    assert_eq!(
        restored.body.matches("fn one() {}").count(),
        preview_matches
    );
    assert!(restored.body.ends_with(notice), "{}", restored.body);
}

#[test_case::test_case(GREP_NO_MATCHES, false; "no_matches")]
#[test_case::test_case(GREP_ERROR_OUTPUT, true; "error")]
fn grep_no_matches_restore_unchanged(output: &str, is_error: bool) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let restored = restore_as(
        &host,
        GREP_TOOL,
        json!({ "pattern": "fn" }),
        output,
        is_error,
        None,
        Vec::new(),
    );
    assert!(
        restored.body.is_empty(),
        "grep must keep declining to restore no-match output: {}",
        restored.body
    );
}

/// A batch rebuilds each child through the child's own restore, both when
/// the child settles live and on batch restore. The child's tool state has
/// to travel with it, or a read child comes back plain and its expand click
/// finds no truncated view to open.
#[test]
fn batch_read_child_restores_highlighted_and_click_expands_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("long.rs");
    let src: String = (0..READ_VIEW_CAP + 3)
        .map(|i| format!("fn f{i}() {{}}\n"))
        .collect();
    std::fs::write(&path, &src).unwrap();
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::with_all_builtins(Arc::clone(&reg)).unwrap();
    let read_input = json!({ "path": path.to_str().unwrap(), "offset": 1, "limit": 0 });

    let read = exec_live(&host, &reg, READ_TOOL, read_input.clone());
    assert!(read.state.is_some(), "read reply carries state for restore");
    let batch_input = json!({ "tool_calls": [
        { "tool": READ_TOOL, "parameters": read_input },
    ]});
    let state = json!({ "children": [
        { "tool": READ_TOOL, "status": "success", "output": read.output, "state": read.state },
    ]});

    let collapsed = restore(
        &host,
        "batch",
        batch_input.clone(),
        "whatever",
        Some(state.clone()),
        Vec::new(),
    );
    assert!(
        has_syntax_colors(&collapsed.spans),
        "read child keeps its highlighting inside a batch: {}",
        collapsed.body
    );
    assert!(
        collapsed.body.contains(EXPAND_HINT),
        "read child collapsed past its cap: {}",
        collapsed.body
    );
    let last_fn = format!("f{}()", READ_VIEW_CAP + 2);
    assert!(
        !collapsed.body.contains(&last_fn),
        "tail hidden while collapsed: {}",
        collapsed.body
    );

    let notice_row = 1 + collapsed
        .body
        .lines()
        .position(|l| l.contains(EXPAND_HINT))
        .expect("read truncation notice in collapsed render");
    let clicked = restore(
        &host,
        "batch",
        batch_input,
        "whatever",
        Some(state),
        vec![notice_row],
    );
    assert!(
        clicked.body.contains(&last_fn),
        "click on the read child expands it: {}",
        clicked.body
    );
    assert!(
        has_syntax_colors(&clicked.spans),
        "expanded read child is still highlighted: {}",
        clicked.body
    );
}

// Phase 4: bash auto-mode gate integration. We drive the real bash handler
// with `classify_verdict` stubbed on the shared `bash_helpers` module (require
// caches the singleton, so `init.lua` sees the override). The deny and error
// paths return synchronously *before* jobstart, so they are deterministic here;
// the approve path falls through to the async jobstart loop, which this harness
// cannot observe, so it is covered at the `spec.lua` unit layer instead.

fn session_for(host: &PluginHost) -> SessionCoordinatorHandle {
    let id = MakimaId::generate();
    let checkpoint: Arc<dyn CheckpointWriter<SessionCheckpoint>> = ClaimingWriter::accept_all();
    SessionCoordinatorHandle::register(SessionCoordinatorParams {
        session_id: id,
        catalog: host.event_handle().session_option_catalog(),
        definitions: builtin_option_definitions(
            "test/model",
            [Arc::from("test/model")],
            false,
            false,
            false,
            makima_agent::ThinkingConfig::Off,
        ),
        persisted_options: Default::default(),
        model_policy: Arc::default(),
        checkpoint,
        mailbox: SessionMailbox::new(id),
    })
    .unwrap()
}

/// Load the real bash plugin with auto mode forced on and the classifier
/// stubbed per `stub_code` (defines `bh.classify_verdict`).
fn bash_host_with_classifier(
    stub_code: &str,
) -> (PluginHost, Arc<ToolRegistry>, SessionCoordinatorHandle) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    // The classifier stub must reach the same `bash_helpers` instance the
    // handler captured. Every `load_source` gets a fresh per-env require cache,
    // so run the setup in the same source.
    let classifier_setup = format!(
        r#"local bh = require("bash_helpers")
{stub_code}
"#
    );
    let mut opts = serde_json::Map::new();
    opts.insert("auto_mode".to_string(), json!(true));
    host.load_source_with_opts("bash", &format!("{BASH_SRC}\n{classifier_setup}"), opts)
        .unwrap();
    let session = session_for(&host);
    (host, reg, session)
}

struct Verdict {
    is_error: bool,
    output: String,
    /// Events emitted during the run (PermissionRequest, tool snapshots).
    events: Vec<AgentEvent>,
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
        ProjectConfig::for_project(std::path::Path::new("/tmp")),
        Arc::default(),
    ))
}

/// YOLO on {session} and its `PermissionManager`, as the frontends keep them:
/// prompts are skipped, so a classifier deny must reject outright through the
/// session's `yolo` option in the plugin (never reaching `enforce` at all).
fn yolo_permissions(session: &SessionCoordinatorHandle) -> Arc<PermissionManager> {
    let permissions = prompt_permissions();
    permissions.set_yolo(true);
    session.bind_permissions(Arc::clone(&permissions)).unwrap();
    permissions
}

/// Like [`exec_verdict`], but with a live event channel (so tests can assert
/// `AgentEvent::PermissionRequest` was emitted) and an optional canned user
/// answer pre-loaded on the response channel for the deny→prompt path to
/// consume. `answer = None` leaves `user_response_rx` unset, modeling a
/// headless/ACP run without an answerer.
fn exec_verdict_prompt(
    host: &PluginHost,
    reg: &ToolRegistry,
    session: &SessionCoordinatorHandle,
    input: Value,
    permissions: Arc<PermissionManager>,
    answer: Option<&str>,
) -> Verdict {
    let (tx, rx) = flume::unbounded();
    let event_tx = makima_agent::EventSender::new(tx);
    let mut ctx = makima_agent::tools::test_support::stub_ctx_with_session(
        &makima_agent::AgentMode::Build,
        Some(&event_tx),
        Some("classifier_tool_use_id"),
        session.read().session_id(),
    );
    ctx.permissions = permissions;
    if let Some(answer) = answer {
        let (answer_tx, answer_rx) = flume::unbounded::<String>();
        ctx.user_response_rx = Some(Arc::new(async_lock::Mutex::new(answer_rx)));
        let answer_parsed = makima_agent::permissions::PermissionAnswer::decode(answer)
            .expect("valid permission answer");
        let tagged =
            makima_agent::permissions::TaggedAnswer::new("classifier_tool_use_id", answer_parsed)
                .encode();
        answer_tx.send(tagged).unwrap();
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
        Ok(makima_agent::ToolOutput::Plain(s)) | Ok(makima_agent::ToolOutput::Markdown(s)) => {
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
fn auto_mode_deny_yolo_rejects_without_running_jobstart() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_DENY_STUB);
    let err = exec_bash_real(
        &host,
        &reg,
        &session,
        Arc::new(common::CannedProvider::new(vec![])),
        yolo_permissions(&session),
        json!({ "command": "echo denied-side-effect" }),
    )
    .expect_err("a yolo deny must fail the tool");
    assert!(
        err.starts_with(AUTO_MODE_DENIED) && err.contains("stub deny reason"),
        "a yolo deny is final and carries the classifier reason: {err}"
    );
}

#[test]
fn auto_mode_error_without_responder_fails_closed() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_ERROR_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
        json!({ "command": "echo never-runs-2" }),
        prompt_permissions(),
        None,
    );
    assert!(
        result.is_error,
        "a classifier error without a responder must fail closed"
    );
    assert!(
        result.output.contains(PERMISSION_DENIED_PREFIX),
        "the fallback permission denial must be reported: {}",
        result.output
    );
}

/// Load the bundled bash handler unchanged. Its classifier inherits the provider
/// from the manager-owned root when the handler executes.
fn bash_host_with_real_classifier() -> (PluginHost, Arc<ToolRegistry>, SessionCoordinatorHandle) {
    bash_host_with_real_classifier_auto(true)
}

/// Like [`bash_host_with_real_classifier`] but with auto mode initialized to
/// `auto_on`. The bash plugin defaults auto mode OFF, so a separate host with
/// `auto_on = false` models the state after `/automode` toggled it off. (Each
/// `load_source` gets its own per-env require cache, so toggling `bash_helpers`
/// from a separate chunk would hit a different singleton than the handler
/// captured; two hosts avoid that footgun.)
fn bash_host_with_real_classifier_auto(
    auto_on: bool,
) -> (PluginHost, Arc<ToolRegistry>, SessionCoordinatorHandle) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let mut opts = serde_json::Map::new();
    opts.insert("auto_mode".to_string(), json!(auto_on));
    host.load_source_with_opts("bash", BASH_SRC, opts).unwrap();
    let session = session_for(&host);
    (host, reg, session)
}

fn verdict_tool_use(approved: bool, reason: &str) -> StreamResponse {
    common::canned_tool_use(
        "classifier_verdict",
        json!({ "approved": approved, "reason": reason }),
    )
}

struct BashInvocationBackend {
    context: ToolContext,
    input: Value,
    completed: flume::Sender<Result<ToolOutput, String>>,
}

impl ActorBackend for BashInvocationBackend {
    fn run_turn<'a>(
        &'a mut self,
        mut history: ActorHistory<'a>,
        turn: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async move {
            if let Some(inbox) = &turn.inbox {
                let mut batch = history.input_batch().await;
                for instruction in inbox.drain_instructions() {
                    batch.push(Message::user(instruction.message));
                }
                inbox.instructions_submitted();
            }
            self.context
                .apply_effective_config(turn.policy.as_ref().expect("root policy"));
            self.context.cancel = turn.cancel;
            self.context.managed_turn = turn.managed_turn;
            let invocation = self
                .context
                .registry
                .get("bash")
                .expect("bash tool registered")
                .tool
                .parse(&self.input)
                .expect("parse failed");
            let result = invocation.execute(&self.context).await.output;
            self.completed.send(result).unwrap();
            if let Some(inbox) = turn.inbox {
                assert!(inbox.finish());
            }
            BackendResult::EnteredRun(TurnOutcome::completed(
                turn.agent_id,
                turn.turn_id.unwrap(),
                Default::default(),
                0,
                DoneReason::EndTurn,
            ))
        })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::ControlDone })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { BackendResult::CompactDone })
    }
}

/// Run the real bash handler inside a managed root turn. Classifier children use
/// the root's canned provider through the production orchestration binding.
fn exec_bash_real(
    host: &PluginHost,
    reg: &ToolRegistry,
    session: &SessionCoordinatorHandle,
    provider: Arc<common::CannedProvider>,
    permissions: Arc<PermissionManager>,
    input: Value,
) -> Result<ToolOutput, String> {
    // The plugin highlights the command header, and the first highlight in a
    // process builds the syntax set. The TUI pays that on its startup thread,
    // so pay it here rather than inside the managed-turn budget below.
    makima_highlight::warmup();
    let (mut ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    makima_agent::tools::test_support::set_session(&mut ctx, session.read().session_id());
    ctx.permissions = permissions;
    ctx.tool_output_lines = view_lines();
    ctx.registry = host.registry();
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(
        reg,
        &ctx.local_tools,
        ctx.mcp.as_ref(),
    ));
    ctx = common::with_request_tools(reg, &ctx);
    let policy =
        common::effective_config(&ctx).with_context(AgentContextSnapshot::new(ctx.cwd.clone()));
    let manager = AgentManagerHandle::new(Default::default()).unwrap();
    let (completed_tx, completed) = flume::bounded(1);
    let template = ctx.clone();
    let root = manager
        .create_root_with_config(Some(policy), Vec::new(), None, |_| {
            Ok::<_, String>(Box::new(BashInvocationBackend {
                context: ctx,
                input,
                completed: completed_tx,
            }) as Box<dyn ActorBackend>)
        })
        .unwrap();
    session
        .bind_permissions(Arc::clone(&template.permissions))
        .unwrap();
    template
        .permissions
        .bind_journal(manager.journal(), None)
        .unwrap();
    let prepared_provider = Arc::clone(&template.provider);
    session
        .bind_actor(
            root.clone(),
            Arc::new(move |spec| {
                let model = Model::from_spec(&spec);
                let provider = Arc::clone(&prepared_provider);
                Box::pin(async move {
                    Ok(PreparedModel {
                        model: model.map_err(unresolved_model)?,
                        provider,
                    })
                })
            }),
            Arc::new(|_| {}),
        )
        .unwrap();
    let yolo = if template.permissions.is_yolo() {
        ENABLED_VALUE
    } else {
        DISABLED_VALUE
    };
    smol::block_on(session.set_option(YOLO_OPTION_ID, yolo)).unwrap();
    host.event_handle()
        .install_orchestration_services(Arc::new(AgentOrchestration::bind_root(
            Arc::default(),
            TrustedTarget {
                target: root.clone(),
                template,
            },
        )));
    host.wait_for_worker_barrier_for_test();
    let ticket = root
        .actor()
        .unwrap()
        .admit_turn(
            AgentInput::new("run bash".into(), Vec::new()),
            None,
            "bash".into(),
        )
        .unwrap();
    let result = smol::block_on(futures_lite::future::race(
        async {
            let result = completed.recv_async().await.unwrap();
            assert!(matches!(ticket.wait().await, TurnOutcome::Completed { .. }));
            let children = root.snapshot().unwrap().children;
            assert_eq!(
                children.len(),
                usize::from(!provider.captured_tools().is_empty())
            );
            assert_eq!(manager.snapshot().len(), children.len() + 1);
            for child_id in children {
                while !manager.runner_finished(child_id).unwrap() {
                    smol::future::yield_now().await;
                }
                let child = manager.node(child_id).unwrap();
                assert_eq!(child.parent_id, Some(root.id()));
                assert_eq!(child.graph_lifecycle, GraphLifecycle::Closed);
                assert_eq!(child.residency, AgentResidency::Dormant);
                assert_eq!(
                    manager.journal().agent(child_id).unwrap().lifecycle,
                    StoredAgentLifecycle::Closed
                );
                assert!(matches!(
                    manager.actor(child_id),
                    Err(ManagerError::NonLiveAgent(id)) if id == child_id
                ));
                if let Some(actor) = child.actor {
                    assert_eq!(actor.lifecycle, ActorLifecycle::Closed);
                    assert!(actor.active_turn.is_none());
                    assert_eq!(actor.queued, 0);
                    assert!(matches!(actor.latest, Some(TurnOutcome::Completed { .. })));
                }
            }
            host.load_source("auto_barrier", "").unwrap();
            let report = manager.shutdown(MANAGED_SHUTDOWN_TIMEOUT).await;
            assert!(
                report.timed_out.is_empty(),
                "managed bash shutdown: {report:?}"
            );
            result
        },
        async {
            smol::Timer::after(MANAGED_BASH_TIMEOUT).await;
            panic!(
                "managed bash did not complete: agents={:?}, requests={}",
                manager
                    .list()
                    .iter()
                    .map(|agent| agent.snapshot())
                    .collect::<Vec<_>>(),
                provider.captured_tools().len(),
            );
        },
    ));
    for tools in provider.captured_tools() {
        assert_eq!(common::tool_names(&tools), [CLASSIFIER_TOOL_NAME]);
    }
    result
}

fn bash_output(out: ToolOutput) -> String {
    match out {
        ToolOutput::Plain(s) | ToolOutput::Markdown(s) => s.text,
        other => panic!("unexpected output: {other:?}"),
    }
}

/// Deny blocks the command (carrying the classifier reason) under YOLO;
/// approve falls through to the real jobstart loop and the command runs. The
/// approve path is the one the stub suite could not observe.
#[test]
fn automode_deny_blocks_and_approve_runs() {
    let (host, reg, session) = bash_host_with_real_classifier();

    let deny = Arc::new(common::CannedProvider::new(vec![
        verdict_tool_use(false, "stub deny reason"),
        common::canned_reply("done"),
    ]));
    let permissions = yolo_permissions(&session);
    let err = exec_bash_real(
        &host,
        &reg,
        &session,
        Arc::clone(&deny),
        Arc::clone(&permissions),
        json!({ "command": "echo denied-side-effect" }),
    )
    .expect_err("under yolo a deny must fail the tool");
    assert!(err.contains("denied"), "{err}");
    assert!(
        err.contains("stub deny reason"),
        "deny carries the reason: {err}"
    );
    assert!(
        !deny.captured_tools().is_empty(),
        "the deny verdict must come from the real classifier"
    );

    let (host, reg, session) = bash_host_with_real_classifier();
    let permissions = prompt_permissions();
    let approve = Arc::new(common::CannedProvider::new(vec![
        verdict_tool_use(true, "ok"),
        common::canned_reply("done"),
    ]));
    let out = exec_bash_real(
        &host,
        &reg,
        &session,
        Arc::clone(&approve),
        permissions,
        json!({ "command": "echo approved-side-effect" }),
    )
    .expect("an approve must run the command");
    assert_eq!(bash_output(out), "approved-side-effect");
}

/// A classifier error (here: a verdict the tool rejects, so none is captured)
/// falls back to permission enforcement and stays denied without a responder.
#[test]
fn automode_error_without_responder_fails_closed() {
    let (host, reg, session) = bash_host_with_real_classifier();
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_tool_use("classifier_verdict", json!({ "approved": "not-a-bool" })),
        common::canned_reply("done"),
    ]));
    let err = exec_bash_real(
        &host,
        &reg,
        &session,
        Arc::clone(&provider),
        prompt_permissions(),
        json!({ "command": "echo never-runs" }),
    )
    .expect_err("a classifier error without a responder must deny");
    assert!(err.contains(PERMISSION_DENIED_PREFIX), "{err}");
}

/// With auto mode OFF the bash handler runs the plain path and never consults
/// the classifier; with auto mode ON a subsequent command is gated by it. Two
/// hosts model the `/automode` toggle state (each `load_source` has its own
/// require cache, so the toggle is expressed as the host's initial auto state
/// rather than a cross-chunk mutation).
#[test]
fn automode_toggle_flows_through_ui() {
    let (host_off, reg_off, session_off) = bash_host_with_real_classifier_auto(false);
    let idle = Arc::new(common::CannedProvider::new(vec![]));
    let out = exec_bash_real(
        &host_off,
        &reg_off,
        &session_off,
        Arc::clone(&idle),
        prompt_permissions(),
        json!({ "command": "echo auto-off-runs" }),
    )
    .expect("with auto mode off the plain path runs");
    assert_eq!(bash_output(out), "auto-off-runs");
    assert!(
        idle.captured_thinking().is_empty(),
        "classifier must not run with auto mode off"
    );

    let (host_on, reg_on, session_on) = bash_host_with_real_classifier_auto(true);
    let deny = Arc::new(common::CannedProvider::new(vec![
        verdict_tool_use(false, "denied with auto on"),
        common::canned_reply("done"),
    ]));
    let err = exec_bash_real(
        &host_on,
        &reg_on,
        &session_on,
        Arc::clone(&deny),
        yolo_permissions(&session_on),
        json!({ "command": "echo should-be-denied" }),
    )
    .expect_err("with auto mode on under yolo the classifier gates the command");
    assert!(err.contains("denied with auto on"), "{err}");
}

// The default deny path: a classifier deny escalates to the same permission
// prompt the automode-off path uses (`PermissionManager::enforce`). The stock
// stub ctx would silently allow unclaimed scopes, so these tests install a
// Prompt-default manager and a canned user answer on the response channel.

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
fn auto_mode_deny_user_allow_runs() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
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
fn auto_mode_deny_user_deny_fails() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
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
fn auto_mode_classifier_error_user_allow_runs() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_ERROR_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
        json!({ "command": "echo classifier-error-allowed" }),
        prompt_permissions(),
        Some("allow"),
    );
    assert!(
        !result.is_error,
        "a classifier error should fall back to user approval: {}",
        result.output
    );
    assert_eq!(result.output, "classifier-error-allowed");
    assert!(
        !permission_requests(&result.events).is_empty(),
        "a classifier error must emit a PermissionRequest"
    );
}

#[test]
fn auto_mode_deny_no_response_channel_fails_closed() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
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
fn auto_mode_deny_allow_rule_skips_prompt() {
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
        ProjectConfig::for_project(std::path::Path::new("/tmp")),
        Arc::default(),
    ));
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_DENY_STUB);
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
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
fn auto_mode_deny_cd_hint_prompts_on_raw_input_scopes() {
    let (host, reg, session) = bash_host_with_classifier(CLASSIFY_DENY_STUB);

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
        ProjectConfig::for_project(std::path::Path::new("/tmp")),
        Arc::default(),
    ));
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
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
        ProjectConfig::for_project(std::path::Path::new("/tmp")),
        Arc::default(),
    ));
    let result = exec_verdict_prompt(
        &host,
        &reg,
        &session,
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
