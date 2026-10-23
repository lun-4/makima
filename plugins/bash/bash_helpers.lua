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
  capture_input = true,
  handler = function(value)
    if type(value) ~= "table" or type(value.approved) ~= "boolean" then
      return nil, VERDICT_TOOL_NAME .. " input must be { approved: boolean }"
    end
    return VERDICT_ACK
  end,
}

local function default_spawn(ctx, opts)
  return maki.agent.spawn(ctx, M.spawn_opts(opts))
end

-- Explicit tools prevent the classifier from inheriting request tools.
function M.spawn_opts(opts)
  local out = {
    system = classifier_system,
    tools = {},
    local_tools = { [VERDICT_TOOL_NAME] = classifier_verdict_tool },
    name = "bash-guardian",
    audience = "general_sub",
    mcp = false,
    silent = true,
  }
  if opts.auto_model then
    out.model = { spec = opts.auto_model }
  end
  return out
end

-- `spawn` is injectable and returns (Agent, err). Failures fall back to the user prompt.
function M.classify_verdict(command, cwd, opts, ctx, spawn)
  spawn = spawn or default_spawn
  local ok, agent, spawn_err = pcall(spawn, ctx, opts)
  if not ok then
    return "error", nil, tostring(agent)
  end
  if spawn_err then
    return "error", nil, spawn_err
  end
  local prompt_ok, result, prompt_err = pcall(function()
    local turn, send_err = agent:send(ctx, M.build_classifier_message(command, cwd), { when = "idle" })
    if not turn then
      return nil, send_err
    end
    return turn:wait(ctx, { timeout = opts.auto_timeout_secs })
  end)
  agent:close(ctx)
  if not prompt_ok then
    return "error", nil, tostring(result)
  end
  if prompt_err then
    return "error", nil, prompt_err
  end
  if not result or result.status ~= "completed" then
    return "error", nil, result and (result.error or result.cancellation_reason) or "classifier returned no result"
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
