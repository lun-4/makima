//! Tests the task plugin's structured-output policy end-to-end: real plugin
//! source, real `maki.json` / `maki.async`, with model I/O replaced by
//! scriptable Lua stubs.

use std::sync::Arc;

use maki_agent::tools::ToolContext;
use maki_agent::tools::ToolRegistry;
use maki_agent::tools::test_support::stub_ctx;
use maki_agent::{AgentMode, ToolOutput};
use maki_lua::PluginHost;
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions,
    StreamResponse,
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};

mod common;

const TASK_PLUGIN_SRC: &str = include_str!("../../plugins/task/init.lua");

// Mirrors of the plugin's error contracts and policy numbers.
const STRUCTURED_OUTPUT_TOOL: &str = "structured_output";
const MAX_STRUCTURED_RETRIES: usize = 2;
const MAX_SCHEMA_ERRORS: usize = 3;
const SCHEMA_COMPILE_ERROR: &str = "invalid output_schema";
const SCHEMA_ROOT_ERROR: &str = "output_schema must have type object";
const STRUCTURED_MISSING_ERROR: &str = "subagent finished without calling structured_output";
const STRUCTURED_INVALID_ERROR: &str = "subagent result does not match output_schema";
const SUMMARY_MISSING_ERROR: &str = "subagent finished without providing a summary";
const UNKNOWN_SUBAGENT_ERR: &str = "unknown subagent type: bogus";
const SUB_AGENT_ERROR_PREFIX: &str = "sub-agent error: ";

const TASK_TOOL: &str = "task";
const PROBE_TOOL: &str = "probe";
const TASK_PROMPT: &str = "do the thing";
const PLAIN_TEXT: &str = "plain text result";
const RECOVERED_TEXT: &str = "summary after nudge";
/// Mirrors the task plugin's `NUDGE_SUMMARY` wording.
const SUMMARY_NUDGE_FRAGMENT: &str = "concise summary";
const PROMPT_ERR_MSG: &str = "model exploded";
const RAISE_MSG: &str = "stub prompt kaboom";
const PARTIAL_TEXT: &str = "half a transcript";
const CANCELLED_ERR: &str = "cancelled";
/// Mirrors the task plugin's `max_concurrent` default.
const TASK_DEFAULT_MAX_CONCURRENT: u64 = 8;

const SCENARIO_PLAIN: &str = "plain";
const SCENARIO_HAPPY: &str = "happy";
const SCENARIO_INVALID_THEN_VALID: &str = "invalid_then_valid";
const SCENARIO_NEVER_STRUCTURED: &str = "never_structured";
const SCENARIO_INVALID_ONLY: &str = "invalid_only";
const SCENARIO_PROMPT_ERROR: &str = "prompt_error";
const SCENARIO_PARTIAL_ERROR: &str = "partial_error";
const SCENARIO_RAISE: &str = "raise";
const SCENARIO_NO_SUMMARY: &str = "no_summary";
const SCENARIO_NO_SUMMARY_THEN_RECOVER: &str = "no_summary_then_recover";

/// Stubs keyed by `opts.name` (the task's `description`). `maki.json` and
/// `maki.async` stay real so schema validation and semaphore behavior are tested.
const STUB_PRELUDE: &str = r#"
recorder = { prompts = {}, closed = 0, sessions = 0 }

local real_semaphore = maki.async.semaphore
maki.async.semaphore = function(n)
  recorder.sem_size = n
  return real_semaphore(n)
end

maki.agent.resolve_model = function(ctx, opts)
  recorder.resolve_opts = opts
  return { spec = "test/model" }
end

maki.agent.system_prompt = function(ctx, opts)
  return "sys"
end

maki.agent.tools = function(ctx, opts)
  return {}
end

local behaviors = {}

behaviors.plain = function(sess, msg)
  return { text = "@PLAIN_TEXT@" }
end

behaviors.happy = function(sess, msg)
  local h = sess.opts.local_tools.structured_output.handler
  recorder.first_ack, recorder.first_err = h({ answer = "42" })
  recorder.captured = { answer = "42" }
  return { text = "raw text ignored" }
end

behaviors.invalid_then_valid = function(sess, msg)
  local h = sess.opts.local_tools.structured_output.handler
  recorder.first_ack, recorder.first_err = h({ answer = 42 })
  recorder.second_ack, recorder.second_err = h({ answer = "42" })
  recorder.captured = { answer = "42" }
  return { text = "raw text ignored" }
end

behaviors.never_structured = function(sess, msg)
  return { text = "no structured call" }
end

behaviors.invalid_only = function(sess, msg)
  local h = sess.opts.local_tools.structured_output.handler
  recorder.first_ack, recorder.first_err = h({ a = 1, b = 2, c = 3, d = 4 })
  return { text = "still invalid" }
end

behaviors.prompt_error = function(sess, msg)
  return nil, "@PROMPT_ERR@"
end

behaviors.partial_error = function(sess, msg)
  return { text = "@PARTIAL_TEXT@" }, "@CANCELLED_ERR@"
end

behaviors.no_summary = function(sess, msg)
  return { text = "" }
end

behaviors.no_summary_then_recover = function(sess, msg)
  if #recorder.prompts == 1 then
    return { text = "" }
  end
  return { text = "@RECOVERED_TEXT@" }
end

behaviors.raise = function(sess, msg)
  error("@RAISE_MSG@")
end

maki.agent.session = function(ctx, opts)
  recorder.sessions = recorder.sessions + 1
  recorder.has_local_tools = opts.local_tools ~= nil
  local sess = { opts = opts }
  function sess:prompt(msg)
    recorder.prompts[#recorder.prompts + 1] = msg
    local res, err = behaviors[opts.name](self, msg)
    if res and recorder.captured ~= nil then
      res.captured = recorder.captured
    end
    return res, err
  end
  function sess:send(msg)
    return self:prompt(msg)
  end
  function sess:status()
    return { status = "done", result = sess:prompt(recorder.last or "") }
  end
  function sess:session_id()
    return opts.name
  end
  function sess:close()
    recorder.closed = recorder.closed + 1
  end
  return sess
end

maki.api.register_tool({
  name = "probe",
  description = "recorder snapshot",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function(input, ctx)
    local snap = {
      sessions = recorder.sessions,
      closed = recorder.closed,
      prompt_count = #recorder.prompts,
      has_local_tools = recorder.has_local_tools,
      first_ack = recorder.first_ack,
      first_err = recorder.first_err,
      second_ack = recorder.second_ack,
      second_err = recorder.second_err,
      sem_size = recorder.sem_size,
    }
    if recorder.resolve_opts then
      snap.resolve_opts = recorder.resolve_opts
    end
    if #recorder.prompts > 0 then
      snap.prompts = recorder.prompts
    end
    return (maki.json.encode(snap))
  end,
})
"#;

fn load_task_host() -> (Arc<ToolRegistry>, PluginHost) {
    load_task_host_with_opts(serde_json::Map::new())
}

fn load_task_host_with_opts(
    opts: serde_json::Map<String, serde_json::Value>,
) -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let prelude = STUB_PRELUDE
        .replace("@PLAIN_TEXT@", PLAIN_TEXT)
        .replace("@RECOVERED_TEXT@", RECOVERED_TEXT)
        .replace("@PROMPT_ERR@", PROMPT_ERR_MSG)
        .replace("@RAISE_MSG@", RAISE_MSG)
        .replace("@PARTIAL_TEXT@", PARTIAL_TEXT)
        .replace("@CANCELLED_ERR@", CANCELLED_ERR);
    host.load_source_with_opts(
        "task_policy",
        &format!("{prelude}\n{TASK_PLUGIN_SRC}"),
        opts,
    )
    .unwrap();
    (reg, host)
}

/// Loads the task plugin with `maki.api.mode.get` stubbed, so plan-mode gating
/// can be exercised without a UI (the real `mode.get` replies via the UI lane).
fn load_task_host_in_mode(mode: &str) -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let prelude = STUB_PRELUDE
        .replace("@PLAIN_TEXT@", PLAIN_TEXT)
        .replace("@RECOVERED_TEXT@", RECOVERED_TEXT)
        .replace("@PROMPT_ERR@", PROMPT_ERR_MSG)
        .replace("@RAISE_MSG@", RAISE_MSG);
    let mode_stub = format!("maki.api.mode.get = function() return '{mode}' end\n\n");
    host.load_source_with_opts(
        "task_policy",
        &format!("{prelude}{mode_stub}\n{TASK_PLUGIN_SRC}"),
        serde_json::Map::new(),
    )
    .unwrap();
    (reg, host)
}

fn exec_tool(reg: &ToolRegistry, name: &str, input: Value) -> Result<String, String> {
    let entry = reg
        .get(name)
        .unwrap_or_else(|| panic!("tool {name} not registered"));
    let inv = entry.tool.parse(&input).expect("parse failed");
    let ctx = stub_ctx(&AgentMode::Build);
    smol::block_on(async { inv.execute(&ctx).await })
        .output
        .map(|out| match out {
            ToolOutput::Plain(s) | ToolOutput::Markdown(s) => s.text,
            other => panic!("unexpected output: {other:?}"),
        })
}

/// Runs a tool and parses its `llm_output` as JSON (the four-lifecycle tools
/// return JSON strings).
fn exec_tool_json(reg: &ToolRegistry, name: &str, input: Value) -> Value {
    let out = exec_tool(reg, name, input).expect("tool failed");
    serde_json::from_str(&out).expect("tool returned invalid json")
}

fn exec_tool_json_with_ctx(
    reg: &ToolRegistry,
    ctx: &ToolContext,
    name: &str,
    input: Value,
) -> Value {
    common::exec_tool(reg, ctx, name, input).expect("tool failed")
}

fn probe(reg: &ToolRegistry) -> Value {
    let out = exec_tool(reg, PROBE_TOOL, json!({})).expect("probe failed");
    serde_json::from_str(&out).expect("probe returned invalid json")
}

fn task_input(scenario: &str, output_schema: Option<Value>) -> Value {
    let mut input = json!({ "description": scenario, "prompt": TASK_PROMPT });
    if let Some(schema) = output_schema {
        input["output_schema"] = schema;
    }
    input
}

const FULL_MODEL_SPEC: &str = "aperture/ollama/glm-5.2";

#[test]
fn model_spec_forwards_full_spec_to_resolve_model() {
    let mut opts = serde_json::Map::new();
    opts.insert("allow_model".into(), json!(true));
    let (reg, _host) = load_task_host_with_opts(opts);
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["model"] = json!(FULL_MODEL_SPEC);
    let out = exec_tool(&reg, TASK_TOOL, input).expect("task with model spec failed");
    assert_eq!(out, PLAIN_TEXT);

    let snap = probe(&reg);
    let opts = snap["resolve_opts"]
        .as_object()
        .expect("resolve_opts missing");
    assert_eq!(opts["spec"], json!(FULL_MODEL_SPEC));
    assert!(
        opts.get("tier").is_none_or(Value::is_null),
        "tier should be unset when only model spec is given"
    );
}

#[test]
fn model_spec_ignored_when_allow_model_off() {
    let (reg, _host) = load_task_host();
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["model"] = json!(FULL_MODEL_SPEC);
    let out = exec_tool(&reg, TASK_TOOL, input).expect("task with model spec failed");
    assert_eq!(out, PLAIN_TEXT);

    let snap = probe(&reg);
    let opts = snap["resolve_opts"]
        .as_object()
        .expect("resolve_opts missing");
    assert!(
        opts.get("spec").is_none_or(Value::is_null),
        "spec should not be forwarded when allow_model is off"
    );
}

fn answer_schema() -> Value {
    json!({
        "type": "object",
        "properties": { "answer": { "type": "string" } },
        "required": ["answer"],
        "additionalProperties": false,
    })
}

/// Four wrong-typed properties, one more than MAX_SCHEMA_ERRORS, so
/// truncation in `bounded_errors` is observable.
fn multi_error_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "a": { "type": "string" },
            "b": { "type": "string" },
            "c": { "type": "string" },
            "d": { "type": "string" },
        },
        "required": ["a", "b", "c", "d"],
    })
}

#[test_case::test_case(json!({"subagent_type": "bogus"}), UNKNOWN_SUBAGENT_ERR ; "unknown_subagent_type")]
#[test_case::test_case(json!({"output_schema": {"type": "object", "properties": {"x": {"type": 42}}}}), SCHEMA_COMPILE_ERROR ; "invalid_output_schema")]
#[test_case::test_case(json!({"output_schema": {"type": "array"}}), SCHEMA_ROOT_ERROR ; "non_object_output_schema")]
#[test_case::test_case(json!({"output_schema": "not an object"}), SCHEMA_ROOT_ERROR ; "non_table_output_schema")]
#[test_case::test_case(json!({"output_schema": {"description": "missing type"}}), SCHEMA_ROOT_ERROR ; "output_schema_missing_type")]
fn bad_input_errors_before_any_session(extra: Value, expected_prefix: &str) {
    let (reg, _host) = load_task_host();
    let mut input = task_input(SCENARIO_PLAIN, None);
    for (k, v) in extra.as_object().unwrap() {
        input[k.as_str()] = v.clone();
    }
    let err = exec_tool(&reg, TASK_TOOL, input).unwrap_err();
    assert!(err.starts_with(expected_prefix), "got: {err}");
    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(0));
    assert_eq!(snap["prompt_count"], json!(0));
}

#[test]
fn general_task_blocked_in_plan_mode() {
    let (reg, _host) = load_task_host_in_mode("plan");
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["subagent_type"] = json!("general");
    let err = exec_tool(&reg, TASK_TOOL, input)
        .expect_err("general subagents must be blocked while in plan mode");
    assert!(err.contains("blocked in plan mode"), "got: {err}");
    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(0));
    assert_eq!(snap["prompt_count"], json!(0));
}

#[test]
fn research_and_reviewer_allowed_in_plan_mode() {
    let (reg, _host) = load_task_host_in_mode("plan");
    let mut research = task_input(SCENARIO_PLAIN, None);
    research["subagent_type"] = json!("research");
    exec_tool(&reg, TASK_TOOL, research).expect("research must be allowed in plan mode");

    let mut reviewer = task_input(SCENARIO_PLAIN, None);
    reviewer["subagent_type"] = json!("plan_reviewer");
    exec_tool(&reg, TASK_TOOL, reviewer).expect("plan_reviewer must be allowed in plan mode");

    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(2));
}

#[test]
fn plan_reviewer_blocked_outside_plan_mode() {
    let (reg, _host) = load_task_host_in_mode("build");
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["subagent_type"] = json!("plan_reviewer");
    let err = exec_tool(&reg, TASK_TOOL, input)
        .expect_err("plan_reviewer must be available only inside plan mode");
    assert!(err.contains("only available in plan mode"), "got: {err}");
    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(0));
}

#[test]
fn structured_happy_path_returns_validated_json() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    )
    .expect("structured task failed");
    let parsed: Value = serde_json::from_str(&out).expect("result is not json");
    assert_eq!(parsed, json!({ "answer": "42" }));

    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(1));
    assert_eq!(snap["closed"], json!(1));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["has_local_tools"], json!(true));
    assert!(snap["first_ack"].is_string(), "valid input must be acked");
    assert!(snap.get("first_err").is_none_or(Value::is_null));
    let prompt = snap["prompts"][0].as_str().expect("prompt missing");
    assert!(prompt.starts_with(TASK_PROMPT), "got: {prompt}");
    assert!(
        prompt.contains(STRUCTURED_OUTPUT_TOOL),
        "prompt must point at the structured_output tool: {prompt}"
    );
}

#[test]
fn invalid_then_valid_recovers_within_one_prompt() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_INVALID_THEN_VALID, Some(answer_schema())),
    )
    .expect("task should succeed after inline retry");
    let parsed: Value = serde_json::from_str(&out).expect("result is not json");
    assert_eq!(parsed, json!({ "answer": "42" }));

    let snap = probe(&reg);
    assert!(snap.get("first_ack").is_none_or(Value::is_null));
    let first_err = snap["first_err"].as_str().expect("first_err missing");
    assert!(
        first_err.contains("/answer"),
        "inline error should point at the failing path: {first_err}"
    );
    assert!(snap["second_ack"].is_string(), "valid retry must be acked");
    assert!(snap.get("second_err").is_none_or(Value::is_null));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn missing_structured_output_nudges_then_errors() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_NEVER_STRUCTURED, Some(answer_schema())),
    )
    .unwrap_err();
    assert_eq!(err, STRUCTURED_MISSING_ERROR);

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(1 + MAX_STRUCTURED_RETRIES));
    for i in 1..=MAX_STRUCTURED_RETRIES {
        let nudge = snap["prompts"][i].as_str().expect("nudge prompt missing");
        assert!(nudge.contains(STRUCTURED_OUTPUT_TOOL), "got: {nudge}");
    }
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn invalid_only_errors_with_bounded_schema_errors() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_INVALID_ONLY, Some(multi_error_schema())),
    )
    .unwrap_err();
    assert!(err.starts_with(STRUCTURED_INVALID_ERROR), "got: {err}");
    assert_eq!(err.lines().count(), 1 + MAX_SCHEMA_ERRORS, "got: {err}");

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(1 + MAX_STRUCTURED_RETRIES));
    let first_err = snap["first_err"].as_str().expect("first_err missing");
    assert_eq!(
        first_err.lines().count(),
        1 + MAX_SCHEMA_ERRORS,
        "inline error must carry at most MAX_SCHEMA_ERRORS validation lines: {first_err}"
    );
}

#[test]
fn prompt_error_maps_to_sub_agent_error() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PROMPT_ERROR, None)).unwrap_err();
    assert_eq!(err, format!("{SUB_AGENT_ERROR_PREFIX}{PROMPT_ERR_MSG}"));
    let snap = probe(&reg);
    assert_eq!(snap["closed"], json!(1));
}

/// Esc during a sub-agent run: the prompt hands back both an error and
/// whatever the sub-agent managed to say, and half a transcript is worth
/// more to the model than a bare "cancelled".
#[test]
fn interrupted_prompt_reports_the_partial_transcript() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PARTIAL_ERROR, None)).unwrap_err();
    assert_eq!(
        err,
        format!("sub-agent interrupted ({CANCELLED_ERR}). Partial output:\n{PARTIAL_TEXT}")
    );
}

#[test]
fn plain_path_returns_text_without_local_tools() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PLAIN, None)).unwrap();
    assert_eq!(out, PLAIN_TEXT);

    let snap = probe(&reg);
    assert_eq!(snap["has_local_tools"], json!(false));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["prompts"][0], json!(TASK_PROMPT));
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn no_summary_nudges_then_recovers() {
    let (reg, _host) = load_task_host();
    let out = exec_tool(
        &reg,
        TASK_TOOL,
        task_input(SCENARIO_NO_SUMMARY_THEN_RECOVER, None),
    )
    .unwrap();
    assert_eq!(out, RECOVERED_TEXT);

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(2));
    let nudge = snap["prompts"][1].as_str().expect("nudge prompt missing");
    assert!(nudge.contains(SUMMARY_NUDGE_FRAGMENT), "got: {nudge}");
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn no_summary_errors_after_nudges() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_NO_SUMMARY, None)).unwrap_err();
    assert_eq!(err, SUMMARY_MISSING_ERROR);

    let snap = probe(&reg);
    assert_eq!(snap["prompt_count"], json!(1 + MAX_STRUCTURED_RETRIES));
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn raising_prompt_does_not_exhaust_semaphore() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_RAISE, None)).unwrap_err();
    assert!(err.contains(RAISE_MSG), "got: {err}");

    let snap = probe(&reg);
    assert_eq!(
        snap["sem_size"],
        json!(TASK_DEFAULT_MAX_CONCURRENT),
        "semaphore not sized from the default max_concurrent option"
    );
    // The next call cannot complete if the failed call retained its permit.
    let out = exec_tool(&reg, TASK_TOOL, task_input(SCENARIO_PLAIN, None)).unwrap();
    assert_eq!(out, PLAIN_TEXT);
}

// --- Four-tool async lifecycle (AC.1 - AC.5, AC.7) --------------------------

#[test]
fn spawn_returns_task_id_immediately() {
    let (reg, _host) = load_task_host();
    let out = exec_tool_json(&reg, "task_spawn", task_input(SCENARIO_PLAIN, None));
    assert!(
        out.get("task_id").and_then(Value::as_str).is_some(),
        "task_spawn must return a task_id: {out}"
    );
}

#[test]
fn spawn_structured_task_registers_commit_tool() {
    let (reg, _host) = load_task_host();
    let out = exec_tool_json(
        &reg,
        "task_spawn",
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    );
    assert!(
        out.get("task_id").and_then(Value::as_str).is_some(),
        "structured spawn must return a task_id: {out}"
    );
    let snap = probe(&reg);
    assert_eq!(snap["has_local_tools"], json!(true));
}

#[test]
fn despawn_releases_permit_and_clears_task() {
    let (reg, _host) = load_task_host();
    let spawn = exec_tool_json(&reg, "task_spawn", task_input(SCENARIO_PLAIN, None));
    let task_id = spawn["task_id"].as_str().unwrap();

    let ok = exec_tool_json(&reg, "task_despawn", json!({ "task_id": task_id }));
    assert_eq!(ok["ok"], json!(true));

    // Unknown task after despawn.
    let err = exec_tool(&reg, "task_despawn", json!({ "task_id": task_id })).unwrap_err();
    assert!(err.contains("unknown task_id"), "got: {err}");

    let snap = probe(&reg);
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn send_queues_and_reactivates() {
    let (reg, _host) = load_task_host();
    let spawn = exec_tool_json(&reg, "task_spawn", task_input(SCENARIO_PLAIN, None));
    let task_id = spawn["task_id"].as_str().unwrap().to_string();

    let queued = exec_tool_json(
        &reg,
        "task_send",
        json!({ "task_id": task_id, "message": "continue" }),
    );
    assert_eq!(queued["queued"], json!(true));
}

#[test]
fn unknown_task_errors_across_lifecycle_tools() {
    let (reg, _host) = load_task_host();
    for tool in ["task_get", "task_send", "task_despawn"] {
        let input = match tool {
            "task_send" => json!({ "task_id": "missing", "message": "x" }),
            _ => json!({ "task_id": "missing" }),
        };
        let err = exec_tool(&reg, tool, input).unwrap_err();
        assert!(err.contains("unknown task_id"), "{tool} got: {err}");
    }
}

// --- Real-driver pair (AC.3, AC.4) ------------------------------------------
//
// The stub suite above tests the task plugin's policy logic with a Lua
// `maki.agent.session` stand-in. These tests swap that stand-in for the REAL
// bound `maki.agent.session` (wrapped only to set `inherit_provider = true`, so
// the spawned driver reuses the parent's canned provider instead of building
// one from `model_spec` over the network). The plugin's gating and
// structured-output policy run unchanged; we assert the gating matrix holds AND
// that the granted session is actually served by the canned provider (real
// driver, not the stub).

/// Like [`STUB_PRELUDE`] but the real `maki.agent.session` is preserved (only
/// wrapped to force `inherit_provider = true`). `resolve_model`/`system_prompt`/
/// `tools` stay stubbed so no real model lookup, file read, or registry scan is
/// needed; only the session spawn and its driver run for real against the canned
/// provider. Structured-output capture flows through the real session driver.
const REAL_DRIVER_PRELUDE: &str = r#"
recorder = { sessions = 0 }

local real_session = maki.agent.session
maki.agent.session = function(ctx, opts)
  recorder.sessions = recorder.sessions + 1
  opts.inherit_provider = true
  return real_session(ctx, opts)
end

maki.agent.resolve_model = function(ctx, opts)
  return { spec = "anthropic/claude-sonnet-4-20250514" }
end

maki.agent.system_prompt = function(ctx, opts)
  return "sys"
end

maki.agent.tools = function(ctx, opts)
  return nil
end

maki.api.register_tool({
  name = "probe",
  description = "recorder snapshot",
  schema = { type = "object", properties = {}, additionalProperties = false },
  audiences = { "main" },
  handler = function()
    return maki.json.encode({ sessions = recorder.sessions })
  end,
})
"#;

fn load_real_driver_host(mode: &str) -> (Arc<ToolRegistry>, PluginHost) {
    load_real_driver_host_with_opts(mode, serde_json::Map::new())
}

fn load_real_driver_host_with_opts(
    mode: &str,
    opts: serde_json::Map<String, Value>,
) -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let mode_stub = format!("maki.api.mode.get = function() return '{mode}' end\n\n");
    host.load_source_with_opts(
        "task_policy_real",
        &format!("{mode_stub}\n{REAL_DRIVER_PRELUDE}\n{TASK_PLUGIN_SRC}"),
        opts,
    )
    .unwrap();
    (reg, host)
}

struct GatedProvider {
    started_tx: flume::Sender<String>,
    release_rx: flume::Receiver<()>,
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
            let message = messages
                .last()
                .and_then(|message| {
                    message.content.iter().find_map(|block| match block {
                        ContentBlock::Text { text } => Some(text.clone()),
                        _ => None,
                    })
                })
                .unwrap_or_default();
            self.started_tx.send(message).unwrap();
            self.release_rx.recv_async().await.unwrap();
            Ok(common::canned_reply("done"))
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

fn probe_real(reg: &ToolRegistry, ctx: &ToolContext) -> Value {
    common::exec_tool(reg, ctx, "probe", json!({})).expect("probe failed")
}

/// Run the blocking `task` composite (which drives the real `maki.agent.session`
/// via `:prompt`) against the canned provider. Mirrors the passing automode
/// pattern: `:prompt` blocks on the driver's `done_rx`, and `smol::block_on`
/// pumps the background driver to completion. No `task_spawn`/`task_get` polling.
/// Returns the raw tool output text: the plain path yields prose, the
/// structured path yields a JSON string (parsed by the caller when needed).
fn run_task(reg: &ToolRegistry, ctx: &ToolContext, input: Value) -> Result<String, String> {
    common::exec_tool_text(reg, ctx, TASK_TOOL, input)
}

#[test]
fn task_policy_spawn_honors_mode_gating_real() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_reply("ok"),
        common::canned_reply("ok"),
        common::canned_reply("ok"),
        common::canned_reply("ok"),
    ]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));

    // general in plan mode -> blocked before any session is spawned.
    let (reg, _host) = load_real_driver_host("plan");
    let mut general = task_input(SCENARIO_PLAIN, None);
    general["subagent_type"] = json!("general");
    let err = run_task(&reg, &ctx, general).expect_err("general must be blocked in plan mode");
    assert!(err.contains("blocked in plan mode"), "got: {err}");
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(0));

    // plan_reviewer in plan mode -> allowed; outside plan -> blocked.
    let mut reviewer = task_input(SCENARIO_PLAIN, None);
    reviewer["subagent_type"] = json!("plan_reviewer");
    run_task(&reg, &ctx, reviewer).expect("plan_reviewer must run in plan mode");
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(1));

    let (reg_build, _host) = load_real_driver_host("build");
    let mut reviewer_build = task_input(SCENARIO_PLAIN, None);
    reviewer_build["subagent_type"] = json!("plan_reviewer");
    let err = run_task(&reg_build, &ctx, reviewer_build)
        .expect_err("plan_reviewer must be blocked outside plan mode");
    assert!(err.contains("only available in plan mode"), "got: {err}");
    assert_eq!(probe_real(&reg_build, &ctx)["sessions"], json!(0));

    // research is allowed in every mode.
    let mut research = task_input(SCENARIO_PLAIN, None);
    research["subagent_type"] = json!("research");
    run_task(&reg, &ctx, research).expect("research must run in plan mode");

    let (reg_build2, _host) = load_real_driver_host("build");
    let mut research_build = task_input(SCENARIO_PLAIN, None);
    research_build["subagent_type"] = json!("research");
    run_task(&reg_build2, &ctx, research_build).expect("research must run in build mode");

    // The granted sessions were served by the canned provider (real driver),
    // not a Lua stub: the provider recorded the requests it received.
    assert!(
        !provider.captured_thinking().is_empty(),
        "the real driver must have called the canned provider"
    );
}

#[test]
fn async_turns_acquire_capacity_only_when_the_driver_runs_them() {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let provider = Arc::new(GatedProvider {
            started_tx,
            release_rx,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let mut opts = serde_json::Map::new();
        opts.insert("max_concurrent".into(), json!(1));
        let (reg, _host) = load_real_driver_host_with_opts("build", opts);

        let first = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", task_input("first", None));
        let first_id = first["task_id"].as_str().unwrap();
        assert_eq!(started_rx.recv_async().await.unwrap(), TASK_PROMPT);

        let queued = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            "task_send",
            json!({ "task_id": first_id, "message": "second turn" }),
        );
        assert_eq!(queued["queued"], json!(true));
        let status =
            exec_tool_json_with_ctx(&reg, &ctx, "task_get", json!({ "task_id": first_id }));
        assert_eq!(status["status"], json!("running"));

        let second = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", task_input("second", None));
        let second_id = second["task_id"].as_str().unwrap();
        assert!(started_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let next = started_rx.recv_async().await.unwrap();
        assert!(matches!(next.as_str(), "second turn" | TASK_PROMPT));
        assert!(started_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let last = started_rx.recv_async().await.unwrap();
        assert_ne!(last, next);
        assert!(matches!(last.as_str(), "second turn" | TASK_PROMPT));
        release_tx.send(()).unwrap();

        for task_id in [first_id, second_id] {
            let out =
                exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": task_id }));
            assert_eq!(out["ok"], json!(true));
        }
    });
}

#[test]
fn despawn_cancels_a_turn_waiting_for_capacity() {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let provider = Arc::new(GatedProvider {
            started_tx,
            release_rx,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let mut opts = serde_json::Map::new();
        opts.insert("max_concurrent".into(), json!(1));
        let (reg, _host) = load_real_driver_host_with_opts("build", opts);

        let first = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", task_input("first", None));
        let first_id = first["task_id"].as_str().unwrap();
        assert_eq!(started_rx.recv_async().await.unwrap(), TASK_PROMPT);

        let mut waiting_input = task_input("waiting", None);
        waiting_input["prompt"] = json!("waiting prompt");
        let waiting = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", waiting_input);
        let waiting_id = waiting["task_id"].as_str().unwrap();
        let out =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": waiting_id }));
        assert_eq!(out["ok"], json!(true));

        release_tx.send(()).unwrap();

        let mut third_input = task_input("third", None);
        third_input["prompt"] = json!("third prompt");
        let third = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", third_input);
        let third_id = third["task_id"].as_str().unwrap();
        assert_eq!(started_rx.recv_async().await.unwrap(), "third prompt");
        release_tx.send(()).unwrap();

        for task_id in [first_id, third_id] {
            let out =
                exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": task_id }));
            assert_eq!(out["ok"], json!(true));
        }
    });
}

#[test]
fn despawn_releases_capacity_from_an_active_turn() {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::unbounded();
        let (_release_tx, release_rx) = flume::unbounded();
        let provider = Arc::new(GatedProvider {
            started_tx,
            release_rx,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let mut opts = serde_json::Map::new();
        opts.insert("max_concurrent".into(), json!(1));
        let (reg, _host) = load_real_driver_host_with_opts("build", opts);

        let first = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", task_input("first", None));
        let first_id = first["task_id"].as_str().unwrap();
        assert_eq!(started_rx.recv_async().await.unwrap(), TASK_PROMPT);

        let second = exec_tool_json_with_ctx(&reg, &ctx, "task_spawn", task_input("second", None));
        let second_id = second["task_id"].as_str().unwrap();
        assert!(started_rx.try_recv().is_err());

        let out =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": first_id }));
        assert_eq!(out["ok"], json!(true));
        let error =
            common::exec_tool(&reg, &ctx, "task_get", json!({ "task_id": first_id })).unwrap_err();
        assert!(error.contains("unknown task_id"), "got: {error}");
        assert_eq!(started_rx.recv_async().await.unwrap(), TASK_PROMPT);

        let out =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": second_id }));
        assert_eq!(out["ok"], json!(true));
    });
}

#[test]
fn task_policy_structured_output_real_driver() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_tool_use(STRUCTURED_OUTPUT_TOOL, json!({ "answer": "42" })),
        common::canned_reply("done"),
    ]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));

    let (reg, _host) = load_real_driver_host("build");
    let out = run_task(
        &reg,
        &ctx,
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    )
    .expect("structured task failed");
    let parsed: Value = serde_json::from_str(&out).expect("result is not json");
    assert_eq!(parsed, json!({ "answer": "42" }));

    // The structured_output local tool was offered to the model.
    let names = common::tool_names(&provider.captured_tools()[0]);
    assert!(
        names.iter().any(|n| n == STRUCTURED_OUTPUT_TOOL),
        "session tools must include structured_output: {names:?}"
    );
}
