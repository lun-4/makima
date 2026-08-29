-- Pure picker logic for the /sessions plugin, shared by `init.lua` and
-- `tests/spec.lua`. Following the `*_helpers` pattern: the harness loads this
-- module directly and never `init.lua`, so everything testable lives here.

local M = {}

local CURRENT_LABEL = "current"
local OPEN_LABEL = "open"

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

-- Right column as {text, style}. Focused says "current", open-elsewhere says
-- "open", everything else shows its age; selection restyles whatever label.
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
