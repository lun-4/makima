local bh = require("bash_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local CLASSIFIER_SYSTEM = require("auto_classifier_prompt")
local SPAWN_ERROR = "stub spawn failed"
local PROMPT_ERROR = "prompt boom"
local TIMEOUT_ERROR = "agent turn wait timed out"
local NO_VERDICT_ERROR = "classifier returned no verdict"
local VERDICT_INPUT_ERROR = "classifier_verdict input must be { approved: boolean }"

local function fake_agent(spawn_log, result, prompt_err, throws)
  return {
    send = function(_self, ctx, message, opts)
      spawn_log.prompt_ctx = ctx
      spawn_log.message = message
      spawn_log.send_opts = opts
      return {
        wait = function(_turn, _wait_ctx, wait_opts)
          spawn_log.prompt_opts = wait_opts
          spawn_log.live_ticket = true
          if throws then
            error(prompt_err, 0)
          end
          if prompt_err then
            return nil, prompt_err
          end
          return result
        end,
      }
    end,
    close = function(_self, ctx)
      spawn_log.close_ctx = ctx
      spawn_log.close_calls = spawn_log.close_calls + 1
      spawn_log.live_ticket = false
      return true
    end,
    cancel = function()
      spawn_log.cancel_calls = spawn_log.cancel_calls + 1
      spawn_log.live_ticket = false
      return true
    end,
  }
end

local function fake_spawn(spawn_log, result, prompt_err, throws)
  return function(ctx, opts)
    spawn_log.called = (spawn_log.called or 0) + 1
    spawn_log.ctx = ctx
    spawn_log.opts = opts
    spawn_log.close_calls = 0
    spawn_log.cancel_calls = 0
    if result == "error" then
      return nil, SPAWN_ERROR
    end
    return fake_agent(spawn_log, result, prompt_err, throws), nil
  end
end

local OPT = { auto_model = "test/model", auto_mode = true, auto_timeout_secs = 17 }

case("parse_verdict_approve", function()
  local verdict, reason, err = bh.parse_verdict('{"approved": true}')
  eq(verdict, "approve")
  eq(reason, nil)
  eq(err, nil)
end)

case("parse_verdict_deny_with_reason", function()
  local verdict, reason, err = bh.parse_verdict('{"approved": false, "reason": "sus"}')
  eq(verdict, "deny")
  eq(reason, "sus")
  eq(err, nil)
end)

case("parse_verdict_deny_without_reason", function()
  local verdict, reason, err = bh.parse_verdict('{"approved": false}')
  eq(verdict, "deny")
  eq(reason, nil)
  eq(err, nil)
end)

case("parse_verdict_string_approved_is_error", function()
  local verdict, _, err = bh.parse_verdict('{"approved": "yes"}')
  eq(verdict, "error")
  assert(err ~= nil, "must carry an error")
end)

case("parse_verdict_non_json_is_error", function()
  local verdict, _, err = bh.parse_verdict("not json")
  eq(verdict, "error")
  assert(err ~= nil, "must carry an error")
end)

case("parse_verdict_missing_approved_is_error", function()
  local verdict, _, _ = bh.parse_verdict('{"reason": "x"}')
  eq(verdict, "error")
end)

case("build_classifier_message_contains_command_and_cwd", function()
  local msg = bh.build_classifier_message("echo hi", "/work/dir")
  assert(msg:find("echo hi"), "message embeds the command")
  assert(msg:find("/work/dir"), "message embeds the working directory")
  assert(msg:find("classifier_verdict"), "message points at the verdict tool")
end)

case("classify_verdict_approve_runs_spawn", function()
  local log, ctx = {}, {}
  local spawn = fake_spawn(log, { status = "completed", captured = { approved = true, reason = "ok" } })
  local verdict, reason, err = bh.classify_verdict("echo hi", "/tmp", OPT, ctx, spawn)
  eq(verdict, "approve")
  eq(reason, "ok")
  eq(err, nil)
  eq(log.called, 1, "trivial command is still gated: spawn must be called")
  eq(log.ctx, ctx)
  eq(log.prompt_ctx, ctx)
  eq(log.close_ctx, ctx)
  eq(log.opts, OPT)
  eq(log.send_opts.when, "idle")
  eq(log.prompt_opts.timeout, OPT.auto_timeout_secs)
  eq(log.close_calls, 1)
  eq(log.cancel_calls, 0)
end)

case("classify_verdict_deny", function()
  local log = {}
  local spawn = fake_spawn(log, { status = "completed", captured = { approved = false, reason = "nope" } })
  local verdict, reason, err = bh.classify_verdict("rm -rf /", "/tmp", OPT, {}, spawn)
  eq(verdict, "deny")
  eq(reason, "nope")
  eq(err, nil)
  eq(log.close_calls, 1)
  eq(log.cancel_calls, 0)
end)

case("classify_verdict_spawn_error_falls_back", function()
  local log = {}
  local spawn = fake_spawn(log, "error")
  local verdict, reason, err = bh.classify_verdict("echo hi", "/tmp", OPT, {}, spawn)
  eq(verdict, "error")
  eq(reason, nil)
  eq(err, SPAWN_ERROR)
  eq(log.close_calls, 0)
  eq(log.cancel_calls, 0)
end)

case("classify_verdict_prompt_error_falls_back", function()
  for _, row in ipairs({
    { name = "returned", error = PROMPT_ERROR },
    { name = "thrown", error = PROMPT_ERROR, throws = true },
    { name = "timeout", error = TIMEOUT_ERROR },
    { name = "failed_result", status = "failed", result_error = PROMPT_ERROR },
    { name = "cancelled_result", status = "cancelled", cancellation_reason = "cancelled" },
  }) do
    local log, ctx = {}, {}
    local result = {
      status = row.status or "completed",
      captured = { approved = true, reason = "stale" },
      error = row.result_error,
      cancellation_reason = row.cancellation_reason,
    }
    local spawn = fake_spawn(log, result, row.error, row.throws)
    local verdict, reason, err = bh.classify_verdict("echo hi", "/tmp", OPT, ctx, spawn)
    eq(verdict, "error", row.name)
    eq(reason, nil, row.name .. " must not use a stale verdict")
    eq(err, row.error or row.result_error or row.cancellation_reason, row.name)
    eq(log.close_calls, 1, row.name)
    eq(log.cancel_calls, 0, row.name)
    eq(log.close_ctx, ctx, row.name)
    eq(log.live_ticket, false, row.name .. " must leave no live ticket")
  end
end)

case("classify_verdict_no_verdict_falls_back", function()
  local log = {}
  local spawn = fake_spawn(log, { status = "completed" })
  local verdict, reason, err = bh.classify_verdict("echo hi", "/tmp", OPT, {}, spawn)
  eq(verdict, "error")
  eq(reason, nil)
  eq(err, NO_VERDICT_ERROR)
  eq(log.close_calls, 1)
  eq(log.cancel_calls, 0)
end)

case("spawn_opts_wires_model_system_isolation_and_silence", function()
  local o = bh.spawn_opts({ auto_model = "openrouter/x" })
  eq(o.model.spec, "openrouter/x")
  eq(o.system, CLASSIFIER_SYSTEM)
  eq(type(o.tools), "table")
  eq(next(o.tools), nil, "classifier must not inherit request tools")
  eq(o.audience, "general_sub")
  eq(o.mcp, false)
  eq(o.silent, true, "classifier must not relay its turn into the main session")
  eq(o.name, "bash-guardian")
  local tool = o.local_tools.classifier_verdict
  eq(tool.capture_input, true)
  for _, input in ipairs({ {}, { approved = "yes" }, false }) do
    local ack, err = tool.handler(input)
    eq(ack, nil, "invalid verdict must not be captured")
    eq(err, VERDICT_INPUT_ERROR)
  end
  for _, approved in ipairs({ true, false }) do
    local ack, err = tool.handler({ approved = approved })
    assert(type(ack) == "string", "valid verdict must be acknowledged")
    eq(err, nil)
  end
end)

case("spawn_opts_inherits_session_model_when_unset", function()
  local o = bh.spawn_opts({})
  eq(o.model, nil, "an omitted model makes maki.agent.spawn inherit the parent model")
  eq(o.system, CLASSIFIER_SYSTEM)
  eq(o.silent, true)
end)

th.report()
