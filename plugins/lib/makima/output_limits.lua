-- Shared per-tool output limit options so the tools cannot drift apart.

local DEFAULT_LINES = 2000
local DEFAULT_BYTES = 50 * 1024
local DEFAULT_LINE_BYTES = 1000

local M = {}

M.DEFAULT_MAX_LINE_BYTES = DEFAULT_LINE_BYTES
M.specs = {
  max_output_lines = { type = "integer", desc = "Model-facing `max_output_lines` cap." },
  max_output_bytes = { type = "integer", desc = "Model-facing `max_output_bytes` cap." },
}

--- Model output limits, or nil, nil for full output.
function M.resolve(opts, ctx)
  if ctx:output_mode() == "full" then
    return nil, nil
  end
  return opts.max_output_lines or ctx:config("max_output_lines", DEFAULT_LINES),
    opts.max_output_bytes or ctx:config("max_output_bytes", DEFAULT_BYTES)
end

--- Per-line cap, or nil for full output.
function M.line_bytes(ctx)
  if ctx:output_mode() ~= "full" then
    return ctx:config("max_line_bytes", DEFAULT_LINE_BYTES)
  end
end

function M.extend(spec)
  for name, s in pairs(M.specs) do
    spec[name] = s
  end
  return spec
end

return M
