local bh = require("bash_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local DENY_MSG = "command denied by auto-mode"
local CLASSIFIER_SYSTEM = require("auto_classifier_prompt")

-- A fake classifier spawn. `verdict` is one of "approve"/"deny"/"error"; the
-- fake records every opts/call it sees so tests can assert wiring.
local function fake_session(spawn_log, result, prompt_err)
  return {
    prompt = function(_self, message)
      spawn_log.message = message
      return result, prompt_err
    end,
    close = function() end,
  }
end

local function fake_spawn(spawn_log, result, prompt_err)
  return function(ctx, opts)
    spawn_log.called = (spawn_log.called or 0) + 1
    spawn_log.opts = opts
    if result == "error" then
      return nil, "stub spawn failed"
    end
    return fake_session(spawn_log, result, prompt_err), nil
  end
end

local OPT = { auto_model = "test/model", auto_mode = true }

case("defaults_off", function()
  local d = bh.defaults()
  eq(d.auto_mode, false)
  eq(d.auto_model, nil, "no default model: unset means inherit the session model")
end)

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
  local log = {}
  local spawn = fake_spawn(log, { captured = { approved = true, reason = "ok" } })
  local verdict, reason, err = bh.classify_verdict("echo hi", "/tmp", OPT, {}, spawn)
  eq(verdict, "approve")
  eq(reason, "ok")
  eq(err, nil)
  eq(log.called, 1, "trivial command is still gated: spawn must be called")
end)

case("classify_verdict_deny", function()
  local log = {}
  local spawn = fake_spawn(log, { captured = { approved = false, reason = "nope" } })
  local verdict, reason, err = bh.classify_verdict("rm -rf /", "/tmp", OPT, {}, spawn)
  eq(verdict, "deny")
  eq(reason, "nope")
  eq(err, nil)
end)

case("classify_verdict_spawn_error_falls_back", function()
  local log = {}
  local spawn = fake_spawn(log, "error")
  local verdict, _, err = bh.classify_verdict("echo hi", "/tmp", OPT, {}, spawn)
  eq(verdict, "error")
  assert(err ~= nil, "must carry spawn error")
end)

case("classify_verdict_prompt_error_falls_back", function()
  local log = {}
  local spawn = fake_spawn(log, { captured = { approved = true } }, "prompt boom")
  local verdict, _, err = bh.classify_verdict("echo hi", "/tmp", OPT, {}, spawn)
  eq(verdict, "error")
  eq(err, "prompt boom")
end)

case("classify_verdict_no_verdict_falls_back", function()
  local log = {}
  local spawn = fake_spawn(log, {})
  local verdict, _, err = bh.classify_verdict("echo hi", "/tmp", OPT, {}, spawn)
  eq(verdict, "error")
  assert(err ~= nil, "must carry an error")
end)

case("spawn_opts_wires_model_system_isolation_and_silence", function()
  local o = bh.spawn_opts({ auto_model = "openrouter/x" })
  eq(o.model_spec, "openrouter/x")
  eq(o.system, CLASSIFIER_SYSTEM)
  eq(o.mcp, false)
  eq(o.silent, true, "classifier must not relay its turn into the main session")
  eq(o.name, "bash-guardian")
  assert(o.local_tools.classifier_verdict ~= nil, "verdict tool is a local_tool")
end)

case("spawn_opts_inherits_session_model_when_unset", function()
  local o = bh.spawn_opts({})
  eq(o.model_spec, nil, "omitted model_spec makes maki.agent.session inherit the parent model")
  eq(o.system, CLASSIFIER_SYSTEM)
  eq(o.silent, true)
end)

th.report()
