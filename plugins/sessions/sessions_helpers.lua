-- Pure picker logic for the /sessions plugin, shared by `init.lua` and
-- `tests/spec.lua`. Following the `*_helpers` pattern: the harness loads this
-- module directly and never `init.lua`, so everything testable lives here.

local M = {}

local function dispw(s)
  return utf8.len(s) or #s
end

local CURRENT_LABEL = "current"
local OPEN_LABEL = "open"
local COUNT_WIDTH = 15
local STATUS_WIDTH = 10
local ROW_PREFIX_WIDTH = 2
local ICON_WIDTH = 2
local RIGHT_COLUMNS_WIDTH = COUNT_WIDTH + 1 + STATUS_WIDTH
local HEADER_STYLE = "path"
local AGE_UNITS = {
  { 31536000, "y" },
  { 2592000, "mo" },
  { 604800, "w" },
  { 86400, "d" },
  { 3600, "h" },
  { 60, "m" },
}

-- Live rows win over their stored copies and keep `live = true` (read by the
-- icon). Stored-only rows become idle and unfocused; `open_elsewhere` rides
-- through untouched either way.
function M.merge(live, stored)
  local seen, all = {}, {}
  for _, s in ipairs(live) do
    seen[s.id] = true
    s.live = true
    all[#all + 1] = s
  end
  for _, s in ipairs(stored) do
    if not seen[s.id] then
      s.status = "idle"
      s.focused = false
      all[#all + 1] = s
    end
  end
  return all
end

-- Base row style: selection wins, then the greyed-out open-elsewhere dim.
function M.row_style(s, selected)
  if selected then
    return "selected"
  end
  if s.open_elsewhere then
    return "dim"
  end
  return "item"
end

function M.format_count(count)
  return tostring(count or 0)
end

-- Widest everything left of the separator (prefix, icon, title) may render
-- and still leave the fixed right columns on screen.
function M.title_width_budget(confirm_width, inner_width)
  return math.max(inner_width - confirm_width - 1 - RIGHT_COLUMNS_WIDTH, 1)
end

function M.row_right_columns(left_width, inner_width, count)
  local count_text = M.format_count(count)
  local padding = math.max(inner_width - left_width - RIGHT_COLUMNS_WIDTH, 1)
  return count_text, padding
end

function M.spans_width(spans)
  local total = 0
  for _, span in ipairs(spans) do
    total = total + dispw(span[1])
  end
  return total
end

function M.clip_spans(spans, max_width)
  local clipped, used = {}, 0
  for _, span in ipairs(spans) do
    if used >= max_width then
      break
    end
    local width = dispw(span[1])
    if used + width <= max_width then
      clipped[#clipped + 1] = span
      used = used + width
    else
      local text = span[1]
      clipped[#clipped + 1] = { text:sub(1, utf8.offset(text, max_width - used + 1) - 1), span[2] }
      used = max_width
    end
  end
  return clipped
end

function M.row_spans(title_spans, count_text, count_style, padding, status, status_style)
  local spans = {}
  for _, span in ipairs(title_spans) do
    spans[#spans + 1] = span
  end
  spans[#spans + 1] = { string.rep(" ", padding), count_style }
  spans[#spans + 1] = { string.rep(" ", math.max(COUNT_WIDTH - dispw(count_text), 0)), count_style }
  spans[#spans + 1] = { count_text, count_style }
  spans[#spans + 1] = { " ", count_style }
  spans[#spans + 1] = { string.rep(" ", math.max(STATUS_WIDTH - dispw(status), 0)), status_style }
  spans[#spans + 1] = { status, status_style }
  return spans
end

function M.header_spans(inner_width)
  local title = "title"
  local count = "messages"
  local status = "age"
  local used = ROW_PREFIX_WIDTH + ICON_WIDTH + dispw(title)
  local padding = math.max(inner_width - used - RIGHT_COLUMNS_WIDTH, 1)
  return {
    { string.rep(" ", ROW_PREFIX_WIDTH + ICON_WIDTH) .. title, HEADER_STYLE },
    { string.rep(" ", padding), HEADER_STYLE },
    { string.rep(" ", math.max(COUNT_WIDTH - dispw(count), 0)), HEADER_STYLE },
    { count, HEADER_STYLE },
    { " ", HEADER_STYLE },
    { string.rep(" ", math.max(STATUS_WIDTH - dispw(status), 0)), HEADER_STYLE },
    { status, HEADER_STYLE },
  }
end

-- Right side labels: focused says "current", open-elsewhere says "open",
-- everything else shows its age; selection restyles whatever label.
function M.right(s, selected)
  local text, style
  if s.focused then
    text, style = CURRENT_LABEL, "accent"
  elseif s.open_elsewhere then
    text, style = OPEN_LABEL, "dim"
  else
    text, style = M.age(s.updated_at), "dim"
  end
  if selected then
    style = "selected"
  end
  return text, style
end

-- A session open in another terminal cannot be opened from here.
function M.can_open(s)
  return not s.open_elsewhere
end

function M.filter_rows(rows, query, matcher, compare)
  local matched = {}
  for position, row in ipairs(rows) do
    row._match = matcher(query, row.title)
    if row._match then
      matched[#matched + 1] = { row = row, match = row._match, position = position }
    end
  end
  table.sort(matched, function(left, right)
    local ordering = compare(left.match, right.match)
    if ordering ~= 0 then
      return ordering < 0
    end
    return left.position < right.position
  end)
  local result = {}
  for _, entry in ipairs(matched) do
    result[#result + 1] = entry.row
  end
  return result
end

function M.reconcile_selection(previous_id, previous_position, rows)
  if previous_id then
    for _, row in ipairs(rows) do
      if row.id == previous_id then
        return previous_id
      end
    end
  end
  if #rows == 0 then
    return nil
  end
  local position = math.min(math.max(previous_position or 1, 1), #rows)
  return rows[position].id
end

function M.age(updated_at)
  local secs = math.max(os.time() - (updated_at or 0), 0)
  for _, u in ipairs(AGE_UNITS) do
    if secs >= u[1] then
      return math.floor(secs / u[1]) .. u[2] .. " ago"
    end
  end
  return "just now"
end

return M
