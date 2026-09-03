-- Pure logic + classifier glue for bash auto mode, shared by `init.lua` and
-- `tests/spec.lua`. Following the `*_helpers` pattern: the harness loads this
-- module directly and never `init.lua`, so everything testable lives here.
--
-- Verdicts are tri-state: "approve" (run), "deny" (clean, do not run), or
-- "error" (classifier failed -> fall back to the interactive user prompt).

local classifier_system = require("auto_classifier_prompt")

local M = {}

local VERDICT_TOOL_NAME = "classifier_verdict"
local VERDICT_ACK = "Verdict recorded."

M.auto_mode_on = false

function M.set_auto_mode(v)
  M.auto_mode_on = not not v
end

function M.defaults()
  return { auto_mode = false, auto_timeout_secs = 30 }
end

-- Strict tri-state decoder: anything that is not `{approved: boolean}` (with an
-- optional string `reason`) is "error" (never silently approve or deny).
function M.parse_verdict(text)
  if type(text) ~= "string" then
    return "error", nil, "verdict is not text"
  end
  local ok, decoded = pcall(maki.json.decode, text)
  if not ok or not decoded then
    return "error", nil, "verdict is not valid JSON"
  end
  if type(decoded) ~= "table" then
    return "error", nil, "verdict is not an object"
  end
  local approved = decoded.approved
  if type(approved) ~= "boolean" then
    return "error", nil, "verdict.approved is not a boolean"
  end
  local reason = decoded.reason
  if type(reason) ~= "string" or reason == "" then
    reason = nil
  end
  if approved then
    return "approve", reason, nil
  end
  return "deny", reason, nil
end

-- Mirrors pi-luna's final classifier user message (`buildClassifierThread`):
-- the verbatim command in a bash block plus the working directory.
function M.build_classifier_message(command, cwd)
  return "The agent requests permission to run this command:\n\n```bash\n"
    .. command
    .. "\n```\n\nWorking directory: "
    .. (cwd or "")
    .. "\n\nApprove or deny using the "
    .. VERDICT_TOOL_NAME
    .. " tool."
end

local classifier_verdict_tool = {
  description = "Return the permission verdict for the single command shown above. Approve only if the command is clearly aligned with the user's stated task and intent. When in doubt, deny. You must call this tool exactly once.",
  input_schema = {
    type = "object",
    properties = {
      approved = { type = "boolean", description = "true to allow executing this exact command" },
      reason = { type = "string", description = "one-line justification for the verdict" },
    },
  },
  handler = function(value)
    if type(value) ~= "table" or type(value.approved) ~= "boolean" then
      return nil, VERDICT_TOOL_NAME .. " input must be { approved: boolean }"
    end
    local _, commit_err = maki.agent.report_task_result(value)
    if commit_err then
      return nil, "no active subagent session"
    end
    return VERDICT_ACK
  end,
}

local function default_spawn(ctx, opts)
  return maki.agent.session(ctx, M.spawn_opts(opts))
end

-- The classifier-session table passed to `maki.agent.session`. Factored out so
-- tests can assert the model/system/isolation wiring without a real provider.
function M.spawn_opts(opts)
  local out = {
    system = classifier_system,
    local_tools = { [VERDICT_TOOL_NAME] = classifier_verdict_tool },
    name = "bash-guardian",
    mcp = false,
    silent = true,
  }
  if opts.auto_model then
    out.model_spec = opts.auto_model
  end
  return out
end

-- One classifier call. `spawn` is injectable for tests; it returns the pair
-- `(session, err)`. Any spawn/send/prompt failure or a non-decodable verdict
-- yields ("error", nil, err) so the caller falls back to the user prompt.
function M.classify_verdict(command, cwd, opts, ctx, spawn)
  spawn = spawn or default_spawn
  local ok, sess, sess_err = pcall(spawn, ctx, opts)
  if not ok then
    return "error", nil, tostring(sess_err)
  end
  if sess_err then
    return "error", nil, sess_err
  end
  local result, prompt_err = sess:prompt(M.build_classifier_message(command, cwd), { timeout = opts.auto_timeout_secs })
  sess:close()
  if prompt_err then
    return "error", nil, prompt_err
  end
  local encoded
  if result and result.captured then
    encoded = maki.json.encode(result.captured)
  elseif result and result.text and result.text ~= "" then
    encoded = result.text
  else
    return "error", nil, "classifier returned no verdict"
  end
  return M.parse_verdict(encoded)
end

return M
