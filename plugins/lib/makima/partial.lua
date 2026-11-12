-- When a tool is cut short, it still hands back what it printed. The marker
-- tells the model that output is real but unfinished. One home for the
-- wording and the painting, so every tool says it the same way.
local M = {}

local CANCELLED_FMT = "[cancelled by user after %s, %s]"
local TIMEOUT_FMT = "[timed out after %ds, %s]"
-- Never claim output the model cannot see above the marker: it would go
-- looking for it, or invent it.
local SOME_OUTPUT = "output above is partial"
local NO_OUTPUT = "no output before the cut"

local replies = setmetatable({}, { __mode = "k" })

--- Close {view} once and return raw output with a deferred marker trailer.
--- {reason} is a cancel-hook reason ("cancelled" | "timeout") and {started_at}
--- the `os.time()` the tool began at.
function M.cut(view, out, reason, timeout_secs, started_at, limits)
  if replies[view] then
    return replies[view]
  end
  local tail = out ~= "" and SOME_OUTPUT or NO_OUTPUT
  local marker
  if reason == "timeout" then
    marker = TIMEOUT_FMT:format(timeout_secs, tail)
  else
    marker = CANCELLED_FMT:format(makima.ui.humantime(math.max(os.time() - started_at, 0)), tail)
  end

  if out == "" then
    view:clear()
  end
  view:append({ { marker, "dim" } })
  view:finish()

  local output_limits = {}
  for key, value in pairs(limits or {}) do
    output_limits[key] = value
  end
  output_limits.trailer = marker
  local reply = {
    llm_output = out,
    output_limits = output_limits,
    is_error = true,
    body = view.buf,
  }
  replies[view] = reply
  return reply
end

return M
