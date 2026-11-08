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
local TITLE_MIN_WIDTH = 4
local ROW_PREFIX_WIDTH = 2
local ICON_WIDTH = 2
local RIGHT_SEPARATOR_WIDTH = 1
local ROW_TITLE_FLOOR = 1
local HEADER_STYLE = "path"
local AGE_HEADER = "age"
local COUNT_HEADER = "messages"
local AGE_ELLIPSIS_MIN_WIDTH = 6

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

local function max_width(values, minimum)
  local width = minimum
  for _, value in ipairs(values) do
    width = math.max(width, dispw(value))
  end
  return width
end

-- One column budget per render. Ages keep priority over the message count:
-- status gets what it needs, the count shrinks from its preferred 15 cells,
-- and disappears before an age is elided.
function M.layout(inner_width, statuses, counts)
  local left_min = math.min(inner_width, ROW_PREFIX_WIDTH + ICON_WIDTH + TITLE_MIN_WIDTH)
  local available = math.max(inner_width - left_min, 0)
  local status_width = math.min(max_width(statuses, dispw(AGE_HEADER)), available)
  local count_min = max_width(counts, dispw(COUNT_HEADER))
  local show_count = available >= status_width + RIGHT_SEPARATOR_WIDTH + count_min
  local count_width = 0
  if show_count then
    count_width = math.min(COUNT_WIDTH, available - status_width - RIGHT_SEPARATOR_WIDTH)
  end
  local right_width = status_width + (show_count and RIGHT_SEPARATOR_WIDTH + count_width or 0)
  return {
    status_width = status_width,
    count_width = count_width,
    show_count = show_count,
    right_width = right_width,
  }
end

-- The row asking for delete confirmation lends its message count cell to the
-- confirmation so the destructive hint stays readable next to a wide age; the
-- age keeps its column.
function M.confirm_layout(layout)
  return {
    status_width = layout.status_width,
    count_width = 0,
    show_count = false,
    right_width = layout.status_width,
  }
end

-- Widest everything left of the separator (prefix, icon, title) may render
-- and still leave the right columns on screen.
function M.title_width_budget(confirm_width, inner_width, layout)
  return math.max(inner_width - confirm_width - layout.right_width - RIGHT_SEPARATOR_WIDTH, ROW_TITLE_FLOOR)
end

-- Widest delete confirmation that still leaves the title floor and the
-- separator before the right columns.
function M.confirm_width_budget(inner_width, layout)
  return math.max(inner_width - layout.right_width - RIGHT_SEPARATOR_WIDTH - ROW_TITLE_FLOOR, 0)
end

function M.row_right_columns(left_width, inner_width, count, layout)
  local count_text = layout.show_count and M.format_count(count) or ""
  local padding = math.max(inner_width - left_width - layout.right_width, 1)
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
      local offset = utf8.offset(text, max_width - used + 1)
      if offset then
        clipped[#clipped + 1] = { text:sub(1, offset - 1), span[2] }
      end
      used = max_width
    end
  end
  return clipped
end

local function elide_left(text, max_width)
  if max_width <= 0 then
    return ""
  end
  if dispw(text) <= max_width then
    return text
  end
  local chars = {}
  for _, cp in utf8.codes(text) do
    chars[#chars + 1] = utf8.char(cp)
  end
  local keep = max_width
  local ellipsis = ""
  if max_width >= AGE_ELLIPSIS_MIN_WIDTH then
    keep = max_width - 1
    ellipsis = "…"
  end
  return ellipsis .. table.concat(chars, "", math.max(#chars - keep + 1, 1))
end

M.elide_left = elide_left

function M.row_spans(title_spans, count_text, count_style, padding, status, status_style, layout)
  local spans = {}
  for _, span in ipairs(title_spans) do
    spans[#spans + 1] = span
  end
  spans[#spans + 1] = { string.rep(" ", padding), count_style }
  if layout.show_count then
    spans[#spans + 1] = {
      string.rep(" ", math.max(layout.count_width - dispw(count_text), 0)),
      count_style,
    }
    spans[#spans + 1] = { count_text, count_style }
    spans[#spans + 1] = { string.rep(" ", RIGHT_SEPARATOR_WIDTH), count_style }
  end
  status = elide_left(status, layout.status_width)
  spans[#spans + 1] = {
    string.rep(" ", math.max(layout.status_width - dispw(status), 0)),
    status_style,
  }
  spans[#spans + 1] = { status, status_style }
  return spans
end

function M.header_spans(inner_width, layout)
  local budget = math.max(inner_width - layout.right_width - RIGHT_SEPARATOR_WIDTH, 1)
  local title = M.clip_spans({ { string.rep(" ", ROW_PREFIX_WIDTH + ICON_WIDTH) .. "title", HEADER_STYLE } }, budget)
  local used = M.spans_width(title)
  local padding = math.max(inner_width - used - layout.right_width, 1)
  local spans = {
    { title[1][1], HEADER_STYLE },
    { string.rep(" ", padding), HEADER_STYLE },
  }
  if layout.show_count then
    spans[#spans + 1] = {
      string.rep(" ", math.max(layout.count_width - dispw(COUNT_HEADER), 0)),
      HEADER_STYLE,
    }
    spans[#spans + 1] = { COUNT_HEADER, HEADER_STYLE }
    spans[#spans + 1] = { string.rep(" ", RIGHT_SEPARATOR_WIDTH), HEADER_STYLE }
  end
  local status = elide_left(AGE_HEADER, layout.status_width)
  spans[#spans + 1] = {
    string.rep(" ", math.max(layout.status_width - dispw(status), 0)),
    HEADER_STYLE,
  }
  spans[#spans + 1] = { status, HEADER_STYLE }
  return spans
end

-- Right side labels: focused says "current", open-elsewhere says "open",
-- everything else shows its age; selection restyles whatever label.
function M.right(s, selected, reference)
  local text, style
  if s.focused then
    text, style = CURRENT_LABEL, "accent"
  elseif s.open_elsewhere then
    text, style = OPEN_LABEL, "dim"
  else
    text, style = M.age(s.updated_at, reference), "dim"
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

function M.age(updated_at, reference)
  return maki.time.ago(maki.time.at(updated_at or 0), reference or maki.time.now())
end

return M
