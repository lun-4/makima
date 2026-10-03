-- Write-time guard against lossy write-back of truncated tool output.
--
-- read and grep cut lines longer than agent.max_line_bytes, so the model may
-- only ever have seen a prefix of them. These checks compare content, not
-- read history: they hold after resume, in subagents, and whatever the model
-- last read. Lines are compared exactly, and every protected line consumes
-- its own occurrence, so two identical long lines cannot collapse into one.

local M = {}

M.LONG_LINE_CHANGED = "line %d is %d bytes, longer than agent.max_line_bytes (%d), so it may have been shown "
  .. "truncated; this change would drop or alter it. Use edit with an exact old_string to change text "
  .. "inside that line, or edit_lines with an empty new_string to delete it."

local SHELL_HINT = " If this line really is content that ends with that text, write it with a shell command."

M.TRUNCATED_MARKER_ADDED = {
  read = "line %d ends with a [line truncated] marker from truncated read or grep output; the text after it "
    .. "was never shown. Change the original line with edit instead."
    .. SHELL_HINT,
}

local function lines_of(text)
  local lines = maki.split(text, "\n")
  for i, line in ipairs(lines) do
    if line:sub(-1) == "\r" then
      lines[i] = line:sub(1, -2)
    end
  end
  return lines
end

local function occurrences(lines)
  local counts = {}
  for _, line in ipairs(lines) do
    counts[line] = (counts[line] or 0) + 1
  end
  return counts
end

-- First long line in old_lines[from..to] without an occurrence left in
-- `available`, consuming one occurrence for each line that has one.
local function first_unmatched_long(old_lines, from, to, available, max_line_bytes)
  for i = from, to do
    local line = old_lines[i]
    if #line > max_line_bytes then
      local left = available[line] or 0
      if left == 0 then
        return i, #line
      end
      available[line] = left - 1
    end
  end
end

local function long_line_error(line_nr, size, max_line_bytes)
  return string.format(M.LONG_LINE_CHANGED, line_nr, size, max_line_bytes)
end

--- Every long line of `before` must survive unchanged somewhere in `after`.
function M.check_write(before, after, max_line_bytes)
  local old = lines_of(before)
  local line_nr, size = first_unmatched_long(old, 1, #old, occurrences(lines_of(after)), max_line_bytes)
  if line_nr then
    return long_line_error(line_nr, size, max_line_bytes)
  end
end

--- Every long line in content's [start_line, end_line] must survive in
--- `new_string`. An empty `new_string` is an explicit deletion and passes.
function M.check_replace_lines(content, start_line, end_line, new_string, max_line_bytes)
  if new_string == "" then
    return nil
  end
  local old = lines_of(content)
  local line_nr, size =
    first_unmatched_long(old, start_line, math.min(end_line, #old), occurrences(lines_of(new_string)), max_line_bytes)
  if line_nr then
    return long_line_error(line_nr, size, max_line_bytes)
  end
end

--- Reject lines of `after` that end in a truncation marker unless an
--- identical line is still available in `before`. Removing a marker passes.
function M.check_markers(before, after)
  local available = occurrences(lines_of(before))
  for i, line in ipairs(lines_of(after)) do
    if line:sub(-1) == "]" then
      local kind = maki.text.truncation_marker(line)
      if kind then
        local left = available[line] or 0
        if left == 0 then
          return string.format(M.TRUNCATED_MARKER_ADDED[kind], i)
        end
        available[line] = left - 1
      end
    end
  end
end

return M
