//! Tests the task plugin's structured-output policy end-to-end: real plugin
//! source, real `maki.json` / `maki.async`, with model I/O replaced by
//! scriptable Lua stubs.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use maki_agent::tools::test_support::stub_ctx;
use maki_agent::tools::{
    DescriptionContext, ToolAudience, ToolContext, ToolFilter, ToolRegistry, TurnToolBindings,
};
use maki_agent::{AgentMode, ModeDef, ModeId, ToolOutput};
use maki_lua::{PluginHost, agent_errors::OUTPUT_SCHEMA_NOT_OBJECT};
use maki_providers::provider::{BoxFuture, Provider};
use maki_providers::{
    AgentError, ContentBlock, Message, Model, ModelInfo, ProviderEvent, RequestOptions, Role,
    StopReason, StreamResponse, TokenUsage,
};
use maki_storage::id::SessionRef;
use serde_json::{Value, json};

mod common;

const TASK_PLUGIN_SRC: &str = include_str!("../../plugins/task/init.lua");
const SCHEMA_MISSING_ERROR: &str = "agent finished without calling structured_output";

// Mirrors of the plugin's error contracts and policy numbers.
const STRUCTURED_OUTPUT_TOOL: &str = "structured_output";
const MAX_STRUCTURED_RETRIES: usize = 2;
const UNKNOWN_SUBAGENT_ERR: &str = "unknown subagent type: bogus";

const TASK_SPAWN_TOOL: &str = "task_spawn";
const PROBE_TOOL: &str = "probe";
const TASK_PROMPT: &str = "do the thing";
const PLAIN_TEXT: &str = "plain text result";
const PROMPT_ERR_MSG: &str = "model exploded";
const RAISE_MSG: &str = "stub prompt kaboom";
const PARTIAL_TEXT: &str = "half a transcript";
const CANCELLED_ERR: &str = "cancelled";
const DUPLICATE_TASK_ID_ERR: &str = "duplicate task_id";
const UNKNOWN_TASK_ERR: &str = "unknown task_id";
const TASK_WAIT_TOOL: &str = "task_wait";
/// Mirrors the task plugin's `max_concurrent` default.
const TASK_DEFAULT_MAX_CONCURRENT: u64 = 8;
/// Mirrors the task plugin's per-task turn ticket cap.
const RETAINED_TURN_LIMIT: usize = 64;
const TURN_UNAVAILABLE_ERR: &str = "turn_result_unavailable";
const TICKET_LIMIT_BUSY_ERR: &str = "stub admission busy";
const SCENARIO_UNRESOLVED: &str = "unresolved";
const JOIN_PENDING_MESSAGE: &str = "join pending";

const NESTED_CHILD_PROMPT: &str = "child prompt";
const NESTED_CHILD_REPLY: &str = "child finished";

const SCENARIO_PLAIN: &str = "plain";
const SCENARIO_HAPPY: &str = "happy";
const SCENARIO_PROMPT_ERROR: &str = "prompt_error";
const SCENARIO_NESTED_FAILED: &str = "nested_failed";
const SCENARIO_PARTIAL_ERROR: &str = "partial_error";
const SCENARIO_RAISE: &str = "raise";

/// Stubs keyed by `opts.name` (the task's `description`). `maki.json` and
/// `maki.async` stays real; structured-output validation uses the real-driver tests.
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
behaviors.happy = behaviors.plain


behaviors.prompt_error = function(sess, msg)
  return nil, "@PROMPT_ERR@"
end

behaviors.nested_failed = function(sess, msg)
  if #recorder.prompts == 1 then
    return { text = "started" }
  end
  return nil, "@PROMPT_ERR@"
end

behaviors.partial_error = function(sess, msg)
  return { text = "@PARTIAL_TEXT@" }, "@CANCELLED_ERR@"
end

behaviors.raise = function(sess, msg)
  error("@RAISE_MSG@")
end

maki.agent.spawn = function(ctx, opts)
  recorder.sessions = recorder.sessions + 1
  recorder.has_local_tools = opts.local_tools ~= nil
  recorder.output_schema = opts.output_schema
  local sess = { opts = opts }
  function sess:submit(call_ctx, msg, submit_opts)
    if opts.name == "unresolved" then
      assert(submit_opts.existing_only == (#recorder.prompts >= @TURN_LIMIT@))
      local turn_id = tostring(#recorder.prompts)
      if submit_opts.existing_only then
        if msg ~= "@JOIN_PENDING_MESSAGE@" then return nil, "@TICKET_LIMIT_BUSY_ERR@" end
      else
        recorder.prompts[#recorder.prompts + 1] = msg
        turn_id = tostring(#recorder.prompts)
      end
      return {
        id = function() return turn_id end,
        result = function() return nil end,
        wait = function() return nil, "agent turn wait timed out" end,
      }
    end
    recorder.prompts[#recorder.prompts + 1] = msg
    local res, err = behaviors[opts.name](self, msg)
    if not res then return nil, err end
    res.status = err and "cancelled" or "completed"
    res.error = err
    res.usage = { input_tokens = 0, output_tokens = 0 }
    local turn_id = tostring(#recorder.prompts)
    res.turn_id = turn_id
    res.agent_id = opts.name
    return {
      id = function() return turn_id end,
      result = function(_, ticket_ctx) return res end,
      wait = function(_, ticket_ctx, wait_opts) return res end,
    }
  end
  function sess:status(call_ctx)
    return { status = "idle", queued = 0, input_tokens = 0, output_tokens = 0 }
  end
  function sess:latest_turn(call_ctx)
    return nil
  end
  function sess:_maki_session_id()
    return opts.name
  end
  function sess:_maki_task_owner()
    return {
      _maki_can_control = function()
        return recorder.can_control ~= false
      end,
      _maki_retired = function()
        return false
      end,
    }
  end
  function sess:defer_close(call_ctx)
    return true
  end
  function sess:close()
    recorder.closed = recorder.closed + 1
  end
  if opts.name == "runtime" then
    local serial = 0
    local latest
    local settled = {}
    local function admit(done)
      serial = serial + 1
      local id = "runtime-" .. tostring(serial)
      settled[id] = done
      local function result()
        if settled[id] then
          return { turn_id = id, text = id, status = "completed" }
        end
      end
      latest = {
        id = function() return id end,
        result = result,
        wait = function()
          local selected = result()
          if recorder.advance_during_wait then
            recorder.advance_during_wait = false
            admit(true)
          end
          if selected then return selected end
          return nil, "agent turn wait timed out"
        end,
      }
      return latest
    end
    function sess:submit(call_ctx, msg, submit_opts)
      assert(not submit_opts.existing_only)
      return admit(false)
    end
    function sess:latest_turn()
      return latest
    end
    recorder.advance = function(input)
      if input.settle then settled[input.settle] = true end
      if input.admit then admit(input.done == true) end
      recorder.advance_during_wait = input.advance_during_wait
      return latest:id()
    end
  end
  return sess
end

maki.api.register_tool({
  name = "advance_runtime_turn",
  description = "control runtime-created tickets",
  schema = {
    type = "object",
    properties = {
      admit = { type = "boolean" },
      done = { type = "boolean" },
      settle = { type = "string" },
      advance_during_wait = { type = "boolean" },
    },
  },
  handler = function(input)
    return recorder.advance(input)
  end,
})

maki.api.register_tool({
  name = "set_task_access",
  description = "set task access",
  schema = {
    type = "object",
    required = { "allowed" },
    properties = { allowed = { type = "boolean" } },
    additionalProperties = false,
  },
  audiences = { "main" },
  handler = function(input)
    recorder.can_control = input.allowed
    return "ok"
  end,
})

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
      output_schema = recorder.output_schema,
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
        .replace("@PROMPT_ERR@", PROMPT_ERR_MSG)
        .replace("@RAISE_MSG@", RAISE_MSG)
        .replace("@PARTIAL_TEXT@", PARTIAL_TEXT)
        .replace("@CANCELLED_ERR@", CANCELLED_ERR)
        .replace("@TURN_LIMIT@", &RETAINED_TURN_LIMIT.to_string())
        .replace("@JOIN_PENDING_MESSAGE@", JOIN_PENDING_MESSAGE)
        .replace("@TICKET_LIMIT_BUSY_ERR@", TICKET_LIMIT_BUSY_ERR);
    host.load_source_with_opts(
        "task_policy",
        &format!("{prelude}\n{TASK_PLUGIN_SRC}"),
        opts,
    )
    .unwrap();
    (reg, host)
}

fn load_task_host_in_mode(mode: &str) -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let prelude = STUB_PRELUDE
        .replace("@PLAIN_TEXT@", PLAIN_TEXT)
        .replace("@PROMPT_ERR@", PROMPT_ERR_MSG)
        .replace("@RAISE_MSG@", RAISE_MSG)
        .replace("@TURN_LIMIT@", &RETAINED_TURN_LIMIT.to_string())
        .replace("@JOIN_PENDING_MESSAGE@", JOIN_PENDING_MESSAGE)
        .replace("@TICKET_LIMIT_BUSY_ERR@", TICKET_LIMIT_BUSY_ERR);
    let mode_stub = format!(
        r#"
        local register_tool = maki.api.register_tool
        maki.api.register_tool = function(spec)
            local handler = spec.handler
            spec.handler = function(input, ctx)
                return handler(input, {{
                    mode = function() return {mode:?} end,
                    tool_output_lines = function() return ctx:tool_output_lines() end,
                }})
            end
            return register_tool(spec)
        end
    "#
    );
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

/// Spawns a task and blocks in `task_wait` on its first turn. A turn that
/// settles with an error comes back as `Err`, like a failed spawn.
fn spawn_and_wait(reg: &ToolRegistry, ctx: &ToolContext, input: Value) -> Result<Value, String> {
    let spawned = common::exec_tool(reg, ctx, TASK_SPAWN_TOOL, input)?;
    let waited = common::exec_tool(
        reg,
        ctx,
        TASK_WAIT_TOOL,
        json!({ "task_id": spawned["task_id"], "turn_id": spawned["turn_id"] }),
    )?;
    match waited["error"].as_str() {
        Some(error) => Err(error.to_owned()),
        None => Ok(waited["result"].clone()),
    }
}

fn spawn_and_wait_stub(reg: &ToolRegistry, input: Value) -> Result<Value, String> {
    spawn_and_wait(reg, &stub_ctx(&AgentMode::Build), input)
}

const FULL_MODEL_SPEC: &str = "aperture/ollama/glm-5.2";

#[test]
fn model_spec_forwards_full_spec_to_resolve_model() {
    let mut opts = serde_json::Map::new();
    opts.insert("allow_model".into(), json!(true));
    let (reg, _host) = load_task_host_with_opts(opts);
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["model"] = json!(FULL_MODEL_SPEC);
    let result = spawn_and_wait_stub(&reg, input).expect("task with model spec failed");
    assert_eq!(result["text"], PLAIN_TEXT);

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
    let result = spawn_and_wait_stub(&reg, input).expect("task with model spec failed");
    assert_eq!(result["text"], PLAIN_TEXT);

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

#[test_case::test_case(json!({"subagent_type": "bogus"}), UNKNOWN_SUBAGENT_ERR ; "unknown_subagent_type")]
fn bad_input_errors_before_any_session(extra: Value, expected_prefix: &str) {
    let (reg, _host) = load_task_host();
    let mut input = task_input(SCENARIO_PLAIN, None);
    for (k, v) in extra.as_object().unwrap() {
        input[k.as_str()] = v.clone();
    }
    let err = exec_tool(&reg, TASK_SPAWN_TOOL, input).unwrap_err();
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
    let err = exec_tool(&reg, TASK_SPAWN_TOOL, input)
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
    exec_tool(&reg, TASK_SPAWN_TOOL, research).expect("research must be allowed in plan mode");

    let mut reviewer = task_input(SCENARIO_HAPPY, None);
    reviewer["subagent_type"] = json!("plan_reviewer");
    exec_tool(&reg, TASK_SPAWN_TOOL, reviewer).expect("plan_reviewer must be allowed in plan mode");

    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(2));
}

#[test]
fn plan_reviewer_blocked_outside_plan_mode() {
    let (reg, _host) = load_task_host_in_mode("build");
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["subagent_type"] = json!("plan_reviewer");
    let err = exec_tool(&reg, TASK_SPAWN_TOOL, input)
        .expect_err("plan_reviewer must be available only inside plan mode");
    assert!(err.contains("only available in plan mode"), "got: {err}");
    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(0));
}

/// Esc during a sub-agent run: the turn settles with both an error and
/// whatever the sub-agent managed to say, and half a transcript is worth
/// more to the model than a bare "cancelled".
#[test]
fn interrupted_turn_reports_the_partial_transcript() {
    let (reg, _host) = load_task_host();
    let spawned = exec_tool_json(
        &reg,
        TASK_SPAWN_TOOL,
        task_input(SCENARIO_PARTIAL_ERROR, None),
    );
    let waited = exec_tool_json(
        &reg,
        TASK_WAIT_TOOL,
        json!({ "task_id": spawned["task_id"] }),
    );
    assert_eq!(waited["error"], CANCELLED_ERR);
    assert_eq!(waited["result"]["text"], PARTIAL_TEXT);
}

#[test]
fn plain_path_returns_text_without_local_tools() {
    let (reg, _host) = load_task_host();
    let result = spawn_and_wait_stub(&reg, task_input(SCENARIO_PLAIN, None)).unwrap();
    assert_eq!(result["text"], PLAIN_TEXT);
    exec_tool_json(&reg, "task_despawn", json!({ "task_id": SCENARIO_PLAIN }));

    let snap = probe(&reg);
    assert_eq!(snap["has_local_tools"], json!(false));
    assert_eq!(snap["prompt_count"], json!(1));
    assert_eq!(snap["prompts"][0], json!(TASK_PROMPT));
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn raising_prompt_preserves_error_and_next_spawn() {
    let (reg, _host) = load_task_host();
    let err = exec_tool(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_RAISE, None)).unwrap_err();
    assert!(err.contains(RAISE_MSG), "got: {err}");

    let snap = probe(&reg);
    assert_eq!(
        snap["sem_size"],
        json!(TASK_DEFAULT_MAX_CONCURRENT),
        "semaphore not sized from the default max_concurrent option"
    );
    let result = spawn_and_wait_stub(&reg, task_input(SCENARIO_PLAIN, None)).unwrap();
    assert_eq!(result["text"], PLAIN_TEXT);
}

// --- Four-tool async lifecycle (AC.1 - AC.5, AC.7) --------------------------

#[test]
fn unmanaged_general_child_can_access_all_nested_lifecycle_tools() {
    let provider = Arc::new(common::CannedProvider::new(vec![common::canned_reply(
        "done",
    )]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    let (reg, _host) = load_real_driver_host();

    let mut input = task_input(SCENARIO_PLAIN, None);
    input["subagent_type"] = json!("general");
    let output = spawn_and_wait(&reg, &ctx, input)
        .expect("general child should complete after rejected mutation attempts");
    assert_eq!(output["text"], "done");

    let filter = ToolFilter::All;
    let description_ctx = DescriptionContext {
        filter: &filter,
        audience: ToolAudience::GENERAL_SUB,
        workflow: false,
        mcp: false,
    };
    for name in [TASK_SPAWN_TOOL, TASK_WAIT_TOOL, "task_send", "task_despawn"] {
        assert!(
            !reg.get(name)
                .expect("lifecycle tool is registered")
                .tool
                .description(&description_ctx)
                .is_empty(),
            "unmanaged general child cannot invoke {name}"
        );
    }
}

#[test]
fn spawn_returns_task_and_turn_ids() {
    let (reg, _host) = load_task_host();
    let out = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None));
    assert!(out["task_id"].is_string(), "missing task_id: {out}");
    assert!(out["turn_id"].is_string(), "missing turn_id: {out}");
}

#[test]
fn task_ticket_cache_evicts_oldest_settled_turn() {
    let (reg, _host) = load_task_host();
    let first = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None));
    let task_id = first["task_id"].as_str().unwrap();
    let mut latest = first["turn_id"].clone();
    for _ in 0..RETAINED_TURN_LIMIT {
        latest = exec_tool_json(
            &reg,
            "task_send",
            json!({"task_id": task_id, "message": TASK_PROMPT}),
        )["turn_id"]
            .clone();
    }
    assert_eq!(
        exec_tool(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": first["turn_id"]})
        )
        .unwrap_err(),
        TURN_UNAVAILABLE_ERR
    );
    let result = exec_tool_json(&reg, TASK_WAIT_TOOL, json!({"task_id": task_id}));
    assert_eq!(result["result"]["turn_id"], latest);
    assert_eq!(result["result"]["text"], PLAIN_TEXT);
    exec_tool_json(&reg, "task_despawn", json!({"task_id": task_id}));
    assert_eq!(probe(&reg)["closed"], json!(1));
}

#[test]
fn implicit_runtime_ticket_is_retained_without_following_successors() {
    let (reg, _host) = load_task_host();
    let spawned = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input("runtime", None));
    let task_id = &spawned["task_id"];
    let selected = exec_tool(
        &reg,
        "advance_runtime_turn",
        json!({"admit": true, "done": true, "advance_during_wait": true}),
    )
    .unwrap();
    let implicit = exec_tool_json(&reg, TASK_WAIT_TOOL, json!({"task_id": task_id}));
    assert_eq!(implicit["result"]["turn_id"], selected);
    let explicit = exec_tool_json(
        &reg,
        TASK_WAIT_TOOL,
        json!({"task_id": task_id, "turn_id": selected}),
    );
    assert_eq!(explicit, implicit);
    let successor = exec_tool_json(&reg, TASK_WAIT_TOOL, json!({"task_id": task_id}));
    assert_ne!(
        successor["result"]["turn_id"],
        implicit["result"]["turn_id"]
    );
}

#[test]
fn implicit_runtime_tickets_respect_bounds_and_preserve_unresolved_work() {
    let (reg, _host) = load_task_host();
    let spawned = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input("runtime", None));
    let task_id = &spawned["task_id"];
    let mut retained = vec![spawned["turn_id"].as_str().unwrap().to_owned()];
    for _ in 1..RETAINED_TURN_LIMIT {
        let id = exec_tool(&reg, "advance_runtime_turn", json!({"admit": true})).unwrap();
        let pending = exec_tool_json(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "timeout": 0}),
        );
        assert_eq!(pending["status"], "running");
        retained.push(id);
    }
    let overflow = exec_tool(&reg, "advance_runtime_turn", json!({"admit": true})).unwrap();
    assert_eq!(
        exec_tool(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "timeout": 0})
        )
        .unwrap_err(),
        TURN_UNAVAILABLE_ERR
    );
    assert_eq!(
        exec_tool(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": overflow, "timeout": 0})
        )
        .unwrap_err(),
        TURN_UNAVAILABLE_ERR
    );
    for id in &retained {
        let pending = exec_tool_json(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": id, "timeout": 0}),
        );
        assert_eq!(pending["status"], "running");
    }
    // Free a settled slot behind unresolved work, then retain the same snapshot.
    exec_tool(&reg, "advance_runtime_turn", json!({"settle": retained[1]})).unwrap();
    let pending = exec_tool_json(
        &reg,
        TASK_WAIT_TOOL,
        json!({"task_id": task_id, "timeout": 0}),
    );
    assert_eq!(pending["status"], "running");
    exec_tool(&reg, "advance_runtime_turn", json!({"settle": overflow})).unwrap();
    let completed = exec_tool_json(
        &reg,
        TASK_WAIT_TOOL,
        json!({"task_id": task_id, "turn_id": overflow}),
    );
    assert_eq!(completed["result"]["turn_id"], overflow);
    assert_eq!(
        exec_tool(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": retained[1]})
        )
        .unwrap_err(),
        TURN_UNAVAILABLE_ERR
    );
    for id in retained
        .iter()
        .enumerate()
        .filter_map(|(index, id)| (index != 1).then_some(id))
    {
        let pending = exec_tool_json(
            &reg,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": id, "timeout": 0}),
        );
        assert_eq!(pending["status"], "running");
    }
}

#[test]
fn task_ticket_limit_atomically_rejects_successors_but_joins_pending_work() {
    let (reg, _host) = load_task_host();
    let first = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_UNRESOLVED, None));
    let task_id = first["task_id"].as_str().unwrap();
    let mut latest = first["turn_id"].clone();
    for _ in 1..RETAINED_TURN_LIMIT {
        latest = exec_tool_json(
            &reg,
            "task_send",
            json!({"task_id": task_id, "message": TASK_PROMPT}),
        )["turn_id"]
            .clone();
    }
    assert_eq!(
        exec_tool(
            &reg,
            "task_send",
            json!({"task_id": task_id, "message": TASK_PROMPT}),
        )
        .unwrap_err(),
        TICKET_LIMIT_BUSY_ERR
    );
    let joined = exec_tool_json(
        &reg,
        "task_send",
        json!({"task_id": task_id, "message": JOIN_PENDING_MESSAGE}),
    );
    assert_eq!(joined["turn_id"], latest);
    assert_eq!(probe(&reg)["prompt_count"], RETAINED_TURN_LIMIT);
    assert_eq!(probe(&reg)["closed"], 0);
    let first_pending = exec_tool_json(
        &reg,
        TASK_WAIT_TOOL,
        json!({"task_id": task_id, "turn_id": first["turn_id"], "timeout": 0}),
    );
    assert_eq!(first_pending["status"], "running");
    exec_tool_json(&reg, "task_despawn", json!({"task_id": task_id}));
}

#[test]
fn unmanaged_nested_task_lifecycle_remains_compatible() {
    let (reg, _host) = load_task_host();
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.audience = ToolAudience::GENERAL_SUB;
    let spawned = exec_tool_json_with_ctx(
        &reg,
        &ctx,
        TASK_SPAWN_TOOL,
        task_input(SCENARIO_PLAIN, None),
    );
    let task_id = spawned["task_id"].as_str().unwrap();
    let status = exec_tool_json_with_ctx(&reg, &ctx, TASK_WAIT_TOOL, json!({ "task_id": task_id }));
    assert_eq!(status["status"], json!("done"));
    assert_eq!(status["result"]["text"], json!(PLAIN_TEXT));

    let queued = exec_tool_json_with_ctx(
        &reg,
        &ctx,
        "task_send",
        json!({ "task_id": task_id, "message": "follow up" }),
    );
    assert_eq!(queued["queued"], json!(true));

    let closed = exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": task_id }));
    assert_eq!(closed["ok"], json!(true));
}

#[test]
fn spawn_forwards_output_schema_without_lua_local_tools() {
    let (reg, _host) = load_task_host();
    let out = exec_tool_json(
        &reg,
        TASK_SPAWN_TOOL,
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    );
    assert!(
        out.get("task_id").and_then(Value::as_str).is_some(),
        "structured spawn must return a task_id: {out}"
    );
    let snap = probe(&reg);
    assert_eq!(snap["has_local_tools"], json!(false));
    assert_eq!(snap["output_schema"], answer_schema());
}

#[test]
fn duplicate_task_id_is_rejected_without_overwrite() {
    let (reg, _host) = load_task_host();
    let first = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None));
    let task_id = first["task_id"].as_str().unwrap();

    let error = exec_tool(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None)).unwrap_err();
    assert_eq!(error, DUPLICATE_TASK_ID_ERR);

    let status = exec_tool_json(&reg, TASK_WAIT_TOOL, json!({ "task_id": task_id }));
    assert_eq!(status["status"], json!("done"));
    let snap = probe(&reg);
    assert_eq!(snap["sessions"], json!(2));
    assert_eq!(snap["closed"], json!(1));
}

#[test]
fn failed_initial_admission_closes_new_session() {
    let (reg, _host) = load_task_host();
    let error = exec_tool(
        &reg,
        TASK_SPAWN_TOOL,
        task_input(SCENARIO_PROMPT_ERROR, None),
    )
    .unwrap_err();
    assert_eq!(error, PROMPT_ERR_MSG);
    let snapshot = probe(&reg);
    assert_eq!(snapshot["sessions"], json!(1));
    assert_eq!(snapshot["closed"], json!(1));
    assert!(
        exec_tool(
            &reg,
            TASK_WAIT_TOOL,
            json!({ "task_id": SCENARIO_PROMPT_ERROR })
        )
        .unwrap_err()
        .contains(UNKNOWN_TASK_ERR)
    );
}

#[test]
fn failed_later_admission_preserves_existing_session() {
    let (reg, _host) = load_task_host();
    let spawned = exec_tool_json(
        &reg,
        TASK_SPAWN_TOOL,
        task_input(SCENARIO_NESTED_FAILED, None),
    );
    let task_id = spawned["task_id"].as_str().unwrap();
    assert_eq!(
        exec_tool(
            &reg,
            "task_send",
            json!({ "task_id": task_id, "message": "rejected" })
        )
        .unwrap_err(),
        PROMPT_ERR_MSG
    );
    assert_eq!(probe(&reg)["closed"], json!(0));
    let retained = exec_tool_json(
        &reg,
        TASK_WAIT_TOOL,
        json!({ "task_id": task_id, "turn_id": spawned["turn_id"] }),
    );
    assert_eq!(retained["status"], json!("done"));
    assert_eq!(retained["result"]["text"], json!("started"));
    let despawned = exec_tool_json(&reg, "task_despawn", json!({ "task_id": task_id }));
    assert_eq!(despawned["ok"], json!(true));
    assert_eq!(probe(&reg)["closed"], json!(1));
}

#[test]
fn lifecycle_tools_deny_cross_branch_access_without_mutating_task() {
    const ACCESS_ERR: &str = "task is owned by another agent branch";

    let (reg, _host) = load_task_host();
    let spawn = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None));
    let task_id = spawn["task_id"].as_str().unwrap();
    exec_tool(&reg, "set_task_access", json!({ "allowed": false })).unwrap();

    for tool in [TASK_WAIT_TOOL, "task_send", "task_despawn"] {
        let input = if tool == "task_send" {
            json!({ "task_id": task_id, "message": "forbidden" })
        } else {
            json!({ "task_id": task_id })
        };
        assert_eq!(exec_tool(&reg, tool, input).unwrap_err(), ACCESS_ERR);
    }

    exec_tool(&reg, "set_task_access", json!({ "allowed": true })).unwrap();
    let status = exec_tool_json(&reg, TASK_WAIT_TOOL, json!({ "task_id": task_id }));
    assert_eq!(status["status"], json!("done"));
    let closed = exec_tool_json(&reg, "task_despawn", json!({ "task_id": task_id }));
    assert_eq!(closed["ok"], json!(true));
}

#[test]
fn despawn_releases_permit_and_clears_task() {
    let (reg, _host) = load_task_host();
    let spawn = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None));
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
fn send_reports_queued_turn() {
    let (reg, _host) = load_task_host();
    let spawn = exec_tool_json(&reg, TASK_SPAWN_TOOL, task_input(SCENARIO_PLAIN, None));
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
    for tool in [TASK_WAIT_TOOL, "task_send", "task_despawn"] {
        let input = match tool {
            "task_send" => json!({ "task_id": "missing", "message": "x" }),
            _ => json!({ "task_id": "missing" }),
        };
        let err = exec_tool(&reg, tool, input).unwrap_err();
        assert!(err.contains("unknown task_id"), "{tool} got: {err}");
    }
}

/// The real agent reuses the canned provider; model lookup and tool discovery stay stubbed.
const REAL_DRIVER_PRELUDE: &str = r#"
recorder = { sessions = 0 }
local real_register_tool = maki.api.register_tool
maki.api.register_tool = function(spec)
  if spec.name == "task_wait" then
    local handler = spec.handler
    spec.handler = function(input, ctx)
      recorder.wait_entered = true
      return handler(input, ctx)
    end
  end
  return real_register_tool(spec)
end

local real_spawn = maki.agent.spawn
maki.agent.spawn = function(ctx, opts)
  recorder.sessions = recorder.sessions + 1
  opts.inherit_provider = true
  return real_spawn(ctx, opts)
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
    return maki.json.encode({ sessions = recorder.sessions, wait_entered = recorder.wait_entered })
  end,
})
"#;

fn load_real_driver_host() -> (Arc<ToolRegistry>, PluginHost) {
    load_real_driver_host_with_opts(serde_json::Map::new())
}

fn load_real_driver_host_with_opts(
    opts: serde_json::Map<String, Value>,
) -> (Arc<ToolRegistry>, PluginHost) {
    load_real_driver_host_with_prelude(opts, "")
}

fn load_real_driver_host_with_prelude(
    opts: serde_json::Map<String, Value>,
    extra_prelude: &str,
) -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let host = PluginHost::new(Arc::clone(&reg)).unwrap();
    host.load_source_with_opts(
        "task_policy_real",
        &format!("{REAL_DRIVER_PRELUDE}\n{extra_prelude}\n{TASK_PLUGIN_SRC}"),
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
            common::bounded(self.release_rx.recv_async()).await.unwrap();
            Ok(common::canned_reply("done"))
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

struct SteeringReportProvider {
    request: AtomicUsize,
    started: flume::Sender<()>,
    release: flume::Receiver<()>,
    fresh_report: bool,
}

impl Provider for SteeringReportProvider {
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
            match self.request.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(common::canned_tool_use(
                    STRUCTURED_OUTPUT_TOOL,
                    json!({"answer": "old"}),
                )),
                1 => {
                    self.started.send(()).unwrap();
                    common::bounded(self.release.recv_async()).await.unwrap();
                    Ok(common::canned_reply(PLAIN_TEXT))
                }
                2 => {
                    assert!(
                        messages
                            .iter()
                            .any(|message| matches!(message.role, Role::User)
                                && message.content.iter().any(|block| matches!(block,
                            ContentBlock::Text { text } if text.contains(NESTED_CHILD_PROMPT))))
                    );
                    if self.fresh_report {
                        Ok(common::canned_tool_use(
                            STRUCTURED_OUTPUT_TOOL,
                            json!({"answer": "fresh"}),
                        ))
                    } else {
                        Ok(common::canned_reply(PLAIN_TEXT))
                    }
                }
                _ => Ok(common::canned_reply(PLAIN_TEXT)),
            }
        })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

#[test_case::test_case(true ; "fresh_report")]
#[test_case::test_case(false ; "stale_report_rejected")]
fn steering_requires_fresh_structured_report(fresh_report: bool) {
    smol::block_on(async {
        let (started, started_rx) = flume::unbounded();
        let (release_tx, release) = flume::unbounded();
        let provider = Arc::new(SteeringReportProvider {
            request: AtomicUsize::new(0),
            started,
            release,
            fresh_report,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let (reg, _host) = load_real_driver_host();
        let spawned = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_SPAWN_TOOL,
            task_input(SCENARIO_HAPPY, Some(answer_schema())),
        );
        common::bounded(started_rx.recv_async()).await.unwrap();
        let sent = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            "task_send",
            json!({"task_id": spawned["task_id"], "message": NESTED_CHILD_PROMPT}),
        );
        assert_eq!(sent["turn_id"], spawned["turn_id"]);
        release_tx.send(()).unwrap();
        let settled = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_WAIT_TOOL,
            json!({"task_id": spawned["task_id"], "turn_id": spawned["turn_id"]}),
        );
        if fresh_report {
            assert_eq!(settled["result"]["captured"], json!({"answer": "fresh"}));
            assert_eq!(settled["result"]["status"], "completed");
        } else {
            assert!(settled["result"]["captured"].is_null());
            assert_eq!(settled["result"]["status"], "failed");
        }
        exec_tool_json_with_ctx(
            &reg,
            &ctx,
            "task_despawn",
            json!({"task_id": spawned["task_id"]}),
        );
    });
}

fn probe_real(reg: &ToolRegistry, ctx: &ToolContext) -> Value {
    common::exec_tool(reg, ctx, "probe", json!({})).expect("probe failed")
}

#[test]
fn task_policy_spawn_honors_mode_gating_real() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_reply("ok"),
        common::canned_reply("ok"),
        common::canned_reply("ok"),
        common::canned_reply("ok"),
    ]));
    let (mut ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    ctx.mode = AgentMode::Plan("/test/task-plan.md".into());
    ctx.mode_def = Some(Arc::new(ModeDef::default_for(ModeId::Plan)));

    // general in plan mode -> blocked before any session is spawned.
    let (reg, _host) = load_real_driver_host();
    let mut general = task_input(SCENARIO_PLAIN, None);
    general["subagent_type"] = json!("general");
    let err =
        spawn_and_wait(&reg, &ctx, general).expect_err("general must be blocked in plan mode");
    assert!(err.contains("blocked in plan mode"), "got: {err}");
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(0));

    // plan_reviewer in plan mode -> allowed; outside plan -> blocked.
    let mut reviewer = task_input(SCENARIO_PLAIN, None);
    reviewer["subagent_type"] = json!("plan_reviewer");
    spawn_and_wait(&reg, &ctx, reviewer).expect("plan_reviewer must run in plan mode");
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(1));

    let mut build_ctx = ctx.clone();
    build_ctx.mode = AgentMode::Build;
    build_ctx.mode_def = Some(Arc::new(ModeDef::default_for(ModeId::Build)));
    let (reg_build, _host) = load_real_driver_host();
    let mut reviewer_build = task_input(SCENARIO_PLAIN, None);
    reviewer_build["subagent_type"] = json!("plan_reviewer");
    let err = spawn_and_wait(&reg_build, &build_ctx, reviewer_build)
        .expect_err("plan_reviewer must be blocked outside plan mode");
    assert!(err.contains("only available in plan mode"), "got: {err}");
    assert_eq!(probe_real(&reg_build, &build_ctx)["sessions"], json!(0));

    // research is allowed in every mode.
    let mut research = task_input(SCENARIO_PLAIN, None);
    research["subagent_type"] = json!("research");
    spawn_and_wait(&reg, &ctx, research).expect("research must run in plan mode");

    let (reg_build2, _host) = load_real_driver_host();
    let mut research_build = task_input(SCENARIO_PLAIN, None);
    research_build["subagent_type"] = json!("research");
    spawn_and_wait(&reg_build2, &build_ctx, research_build)
        .expect("research must run in build mode");

    // The granted sessions were served by the canned provider (real driver),
    // not a Lua stub: the provider recorded the requests it received.
    assert!(
        !provider.captured_thinking().is_empty(),
        "the real driver must have called the canned provider"
    );
}

#[test]
fn task_docs_describe_same_turn_submission() {
    for contract in [
        "Busy sends join open work and return the same turn_id",
        "next safe model request",
        "without cancelling the child",
        "a successor never retargets an existing wait",
        "batched idle continuation",
    ] {
        assert!(
            TASK_PLUGIN_SRC.contains(contract),
            "missing contract: {contract}"
        );
    }
}

#[test]
fn repeated_busy_sends_retain_one_ticket() {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let provider = Arc::new(GatedProvider {
            started_tx,
            release_rx,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let (reg, _host) = load_real_driver_host();
        let spawned =
            exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("first", None));
        let task_id = spawned["task_id"].as_str().unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );
        for _ in 0..RETAINED_TURN_LIMIT {
            let admitted = exec_tool_json_with_ctx(
                &reg,
                &ctx,
                "task_send",
                json!({"task_id": task_id, "message": TASK_PROMPT}),
            );
            assert_eq!(admitted["turn_id"], spawned["turn_id"]);
        }
        let error = common::exec_tool_text(
            &reg,
            &ctx,
            "task_send",
            json!({"task_id": task_id, "message": TASK_PROMPT}),
        )
        .unwrap_err();
        assert!(!error.is_empty());
        release_tx.send(()).unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );
        release_tx.send(()).unwrap();
        let settled = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": spawned["turn_id"]}),
        );
        assert_eq!(settled["result"]["turn_id"], spawned["turn_id"]);
        assert_eq!(settled["result"]["status"], "completed");
        let successor = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            "task_send",
            json!({"task_id": task_id, "message": TASK_PROMPT}),
        );
        assert_ne!(successor["turn_id"], spawned["turn_id"]);
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );
        release_tx.send(()).unwrap();
        let settled = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": successor["turn_id"]}),
        );
        assert_eq!(settled["result"]["status"], "completed");
        exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({"task_id": task_id}));
    });
}

#[test_case::test_case(0 ; "zero")]
#[test_case::test_case(1 ; "positive")]
fn timed_out_task_wait_reports_running_and_preserves_child(timeout: u64) {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let provider = Arc::new(GatedProvider {
            started_tx,
            release_rx,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let (reg, _host) = load_real_driver_host();
        let spawned =
            exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("first", None));
        let task_id = spawned["task_id"].as_str().unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );
        let pending = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": spawned["turn_id"], "timeout": timeout}),
        );
        assert_eq!(pending["status"], "running");
        release_tx.send(()).unwrap();
        let settled = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_WAIT_TOOL,
            json!({"task_id": task_id, "turn_id": spawned["turn_id"]}),
        );
        assert_eq!(settled["result"]["turn_id"], spawned["turn_id"]);
        assert_eq!(settled["result"]["text"], "done");
        exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({"task_id": task_id}));
    });
}

#[test_case::test_case(false ; "snapshots_latest_once")]
#[test_case::test_case(true ; "explicit_exact_ticket")]
fn task_wait_same_turn_includes_later_send(explicit_turn: bool) {
    smol::block_on(async {
        let (started_tx, started_rx) = flume::unbounded();
        let (release_tx, release_rx) = flume::unbounded();
        let provider = Arc::new(GatedProvider {
            started_tx,
            release_rx,
        });
        let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
        let (reg, _host) = load_real_driver_host();
        let first = exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("first", None));
        let task_id = first["task_id"].as_str().unwrap();
        assert!(first["turn_id"].is_string());
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );

        let mut wait_input = json!({ "task_id": task_id });
        if explicit_turn {
            wait_input["turn_id"] = first["turn_id"].clone();
        }
        let invocation = reg
            .get(TASK_WAIT_TOOL)
            .unwrap()
            .tool
            .parse(&wait_input)
            .unwrap();
        let wait_ctx = common::with_request_tools(&reg, &ctx);
        let wait = invocation.execute(&wait_ctx);
        futures_lite::pin!(wait);
        assert!(futures_lite::future::poll_once(&mut wait).await.is_none());
        common::bounded(async {
            while probe_real(&reg, &ctx)["wait_entered"] != json!(true) {
                smol::future::yield_now().await;
            }
        })
        .await;

        let second = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            "task_send",
            json!({ "task_id": task_id, "message": "second turn" }),
        );
        assert_eq!(second["queued"], json!(true));
        assert!(second["turn_id"].is_string());
        assert_eq!(first["turn_id"], second["turn_id"]);
        release_tx.send(()).unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            "second turn"
        );

        assert!(futures_lite::future::poll_once(&mut wait).await.is_none());
        release_tx.send(()).unwrap();
        let completed = common::bounded(wait).await.output.unwrap();
        let (ToolOutput::Plain(completed) | ToolOutput::Markdown(completed)) = completed else {
            panic!("unexpected task_wait output");
        };
        let result: Value = serde_json::from_str(&completed.text).unwrap();
        assert_eq!(result["status"], json!("done"));
        assert_eq!(result["result"]["turn_id"], first["turn_id"]);
        assert_eq!(result["result"]["text"], json!("done"));
        let closed =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": task_id }));
        assert_eq!(closed["ok"], json!(true));
    });
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
        let (reg, _host) = load_real_driver_host_with_opts(opts);

        let first = exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("first", None));
        let first_id = first["task_id"].as_str().unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );

        let queued = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            "task_send",
            json!({ "task_id": first_id, "message": "second turn" }),
        );
        assert_eq!(queued["queued"], json!(true));
        let status = exec_tool_json_with_ctx(
            &reg,
            &ctx,
            TASK_WAIT_TOOL,
            json!({ "task_id": first_id, "timeout": 0 }),
        );
        assert_eq!(status["status"], json!("running"));

        let second =
            exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("second", None));
        let second_id = second["task_id"].as_str().unwrap();
        assert!(started_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let next = common::bounded(started_rx.recv_async()).await.unwrap();
        assert!(matches!(next.as_str(), "second turn" | TASK_PROMPT));
        assert!(started_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        let last = common::bounded(started_rx.recv_async()).await.unwrap();
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
        let (reg, _host) = load_real_driver_host_with_opts(opts);

        let first = exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("first", None));
        let first_id = first["task_id"].as_str().unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );

        let mut waiting_input = task_input("waiting", None);
        waiting_input["prompt"] = json!("waiting prompt");
        let waiting = exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, waiting_input);
        let waiting_id = waiting["task_id"].as_str().unwrap();
        let out =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": waiting_id }));
        assert_eq!(out["ok"], json!(true));

        release_tx.send(()).unwrap();

        let mut third_input = task_input("third", None);
        third_input["prompt"] = json!("third prompt");
        let third = exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, third_input);
        let third_id = third["task_id"].as_str().unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            "third prompt"
        );
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
        let (reg, _host) = load_real_driver_host_with_opts(opts);

        let first = exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("first", None));
        let first_id = first["task_id"].as_str().unwrap();
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );

        let second =
            exec_tool_json_with_ctx(&reg, &ctx, TASK_SPAWN_TOOL, task_input("second", None));
        let second_id = second["task_id"].as_str().unwrap();
        assert!(started_rx.try_recv().is_err());

        let out =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": first_id }));
        assert_eq!(out["ok"], json!(true));
        let error = common::exec_tool(&reg, &ctx, TASK_WAIT_TOOL, json!({ "task_id": first_id }))
            .unwrap_err();
        assert!(error.contains(UNKNOWN_TASK_ERR), "got: {error}");
        assert_eq!(
            common::bounded(started_rx.recv_async()).await.unwrap(),
            TASK_PROMPT
        );

        let out =
            exec_tool_json_with_ctx(&reg, &ctx, "task_despawn", json!({ "task_id": second_id }));
        assert_eq!(out["ok"], json!(true));
    });
}

#[test]
fn task_policy_general_task_runs_in_custom_mode_real_driver() {
    let provider = Arc::new(common::CannedProvider::new(vec![common::canned_reply(
        "done",
    )]));
    let (mut ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    ctx.mode = AgentMode::Custom(ModeId::parse("audit"));
    ctx.mode_def = Some(Arc::new(ModeDef::default_for(ctx.mode.id())));

    let (reg, _host) = load_real_driver_host();
    let mut input = task_input(SCENARIO_PLAIN, None);
    input["subagent_type"] = json!("general");
    let out = spawn_and_wait(&reg, &ctx, input).expect("general task must run in custom mode");

    assert_eq!(out["text"], "done");
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(1));
    let captured_tools = provider.captured_tools();
    assert_eq!(captured_tools.len(), 1);
    assert!(
        common::tool_names(&captured_tools[0]).is_empty(),
        "the custom-mode test harness offers no tools to the child"
    );
}

#[test]
fn plan_mode_task_supports_structured_output_real_driver() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_tool_use(STRUCTURED_OUTPUT_TOOL, json!({ "answer": "42" })),
        common::canned_reply("done"),
    ]));
    let (mut ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    ctx.mode = AgentMode::Plan("/test/task-plan.md".into());
    ctx.mode_def = Some(Arc::new(ModeDef::default_for(ModeId::Plan)));

    let (reg, _host) = load_real_driver_host();
    let mut input = task_input(SCENARIO_HAPPY, Some(answer_schema()));
    input["subagent_type"] = json!("plan_reviewer");
    let out = spawn_and_wait(&reg, &ctx, input).expect("structured plan-mode task failed");

    assert_eq!(out["captured"], json!({ "answer": "42" }));
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(1));

    let tools = provider.captured_tools();
    let structured_output = tools[0]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == STRUCTURED_OUTPUT_TOOL)
        .expect("session tools must include structured_output");
    assert_eq!(structured_output["input_schema"], answer_schema());
}

#[test_case::test_case(json!({"type": "array"}), OUTPUT_SCHEMA_NOT_OBJECT ; "non_object")]
#[test_case::test_case(json!({"description": "missing type"}), OUTPUT_SCHEMA_NOT_OBJECT ; "missing_type")]
#[test_case::test_case(json!({"type": "object", "properties": {"x": {"type": 42}}}), "invalid output_schema" ; "invalid_schema")]
fn real_schema_validation_rejects_before_provider_request(schema: Value, expected: &str) {
    let provider = Arc::new(common::CannedProvider::new(vec![]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    let (reg, _host) = load_real_driver_host();
    let error = spawn_and_wait(&reg, &ctx, task_input(SCENARIO_HAPPY, Some(schema))).unwrap_err();
    assert!(error.contains(expected), "got: {error}");
    assert!(provider.captured_tools().is_empty());
}

#[test]
fn real_schema_invalid_report_recovers() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_tool_use(STRUCTURED_OUTPUT_TOOL, json!({ "answer": 42 })),
        common::canned_tool_use(STRUCTURED_OUTPUT_TOOL, json!({ "answer": "42" })),
        common::canned_reply("done"),
    ]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(provider);
    let (reg, _host) = load_real_driver_host();
    let out = spawn_and_wait(
        &reg,
        &ctx,
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    )
    .unwrap();
    assert_eq!(out["captured"], json!({ "answer": "42" }));
}

#[test]
fn real_schema_missing_report_fails_boundedly() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_reply("no report"),
        common::canned_reply("no report"),
        common::canned_reply("no report"),
    ]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));
    let (reg, _host) = load_real_driver_host();
    let error = spawn_and_wait(
        &reg,
        &ctx,
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    )
    .unwrap_err();
    assert!(error.contains(SCHEMA_MISSING_ERROR), "got: {error}");
    assert_eq!(provider.captured_tools().len(), 1 + MAX_STRUCTURED_RETRIES);
}

#[test]
fn task_policy_structured_output_real_driver() {
    let provider = Arc::new(common::CannedProvider::new(vec![
        common::canned_tool_use(STRUCTURED_OUTPUT_TOOL, json!({ "answer": "42" })),
        common::canned_reply("done"),
    ]));
    let (ctx, _rx, _trigger) = common::ctx_with_provider(Arc::clone(&provider));

    let (reg, _host) = load_real_driver_host();
    let out = spawn_and_wait(
        &reg,
        &ctx,
        task_input(SCENARIO_HAPPY, Some(answer_schema())),
    )
    .expect("structured task failed");
    assert_eq!(out["captured"], json!({ "answer": "42" }));

    // The structured_output local tool was offered to the model.
    let names = common::tool_names(&provider.captured_tools()[0]);
    assert!(
        names.iter().any(|n| n == STRUCTURED_OUTPUT_TOOL),
        "session tools must include structured_output: {names:?}"
    );
}

/// General subagents see the lifecycle tools, so a real child can spawn and
/// wait on a grandchild of its own.
const NESTED_LIFECYCLE_TOOLS_PRELUDE: &str = r#"
maki.agent.tools = function(ctx, opts)
  if opts.audience ~= "general_sub" then
    return nil
  end
  return {
    { name = "task_spawn", description = "spawn", input_schema = { type = "object" } },
    { name = "task_wait", description = "wait", input_schema = { type = "object" } },
  }
end
"#;

/// Drives a general subagent through `task_spawn`, then a blocking
/// `task_wait` on the returned id, then echoes the wait result as its reply.
/// The grandchild it spawned answers its prompt with a fixed reply.
struct NestedTaskProvider;

impl NestedTaskProvider {
    fn tool_use(id: &str, name: &str, input: Value) -> StreamResponse {
        StreamResponse {
            message: Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use(id, name, input)],
                ..Default::default()
            },
            usage: TokenUsage::default(),
            stop_reason: Some(StopReason::ToolUse),
        }
    }

    fn reply(messages: &[Message]) -> StreamResponse {
        let first_text = messages.first().and_then(|message| {
            message.content.iter().find_map(|block| match block {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
        });
        if first_text == Some(NESTED_CHILD_PROMPT) {
            return common::canned_reply(NESTED_CHILD_REPLY);
        }
        let last_result = messages.last().and_then(|message| {
            message.content.iter().find_map(|block| match block {
                ContentBlock::ToolResult { content, .. } => Some(content.as_str()),
                _ => None,
            })
        });
        let Some(last_result) = last_result else {
            return Self::tool_use(
                "spawn",
                TASK_SPAWN_TOOL,
                json!({ "description": "grandchild", "prompt": NESTED_CHILD_PROMPT }),
            );
        };
        let parsed: Value = serde_json::from_str(last_result).unwrap_or_default();
        if parsed.get("status").is_none()
            && let Some(task_id) = parsed["task_id"].as_str()
        {
            return Self::tool_use("wait", TASK_WAIT_TOOL, json!({ "task_id": task_id }));
        }
        common::canned_reply(last_result)
    }
}

impl Provider for NestedTaskProvider {
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
        Box::pin(async move { Ok(Self::reply(messages)) })
    }

    fn list_models(&self) -> BoxFuture<'_, Result<Vec<ModelInfo>, AgentError>> {
        Box::pin(async { unimplemented!() })
    }
}

/// With one permit, an unmanaged general subagent that blocks in `task_wait`
/// must hand its permit to the grandchild it waits on, or neither can finish.
#[test]
fn unmanaged_blocking_wait_yields_the_only_permit_to_its_child() {
    let (mut ctx, _rx, _trigger) = common::ctx_with_provider(Arc::new(NestedTaskProvider));
    let mut opts = serde_json::Map::new();
    opts.insert("max_concurrent".into(), json!(1));
    let (reg, _host) = load_real_driver_host_with_prelude(opts, NESTED_LIFECYCLE_TOOLS_PRELUDE);
    ctx.registry = Arc::clone(&reg);
    ctx.turn_bindings = Arc::new(TurnToolBindings::capture(&reg, &ctx.local_tools, None));

    let mut input = task_input("parent", None);
    input["subagent_type"] = json!("general");
    let result = spawn_and_wait(&reg, &ctx, input).expect("general subagent failed");

    let echoed: Value = serde_json::from_str(result["text"].as_str().unwrap())
        .unwrap_or_else(|error| panic!("reply is not the task_wait result ({error}): {result}"));
    assert_eq!(echoed["status"], "done");
    assert_eq!(echoed["result"]["text"], NESTED_CHILD_REPLY);
    assert_eq!(probe_real(&reg, &ctx)["sessions"], json!(2));
}
