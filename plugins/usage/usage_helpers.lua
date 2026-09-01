local M = {}

local PREFIX = "  "
local EMPTY = "—"
local DAY = 24 * 60 * 60
local MODAL_BORDER_ROWS = 2

function M.is_close_key(key)
  return key == "q" or key == "esc" or key == "ctrl+c"
end

function M.modal_height(line_count, terminal_rows)
  local max_height = math.max(1, math.floor(terminal_rows * 0.7) - MODAL_BORDER_ROWS)
  return math.max(1, math.min(line_count, max_height))
end

function M.modal_window_height(content_height)
  return content_height + MODAL_BORDER_ROWS
end

local function value_or(value, fallback)
  if value == nil then
    return fallback
  end
  return value
end

function M.format_tokens(value)
  value = tonumber(value) or 0
  if value < 1000 then
    return tostring(math.floor(value))
  elseif value < 1000000 then
    return string.format(value < 10000 and "%.1fk" or "%.0fk", value / 1000)
  end
  return string.format(value < 10000000 and "%.1fm" or "%.0fm", value / 1000000)
end

function M.kind(kind)
  if type(kind) == "string" then
    local labels = {
      Monthly = { "Monthly usage", "m" },
      Credits = { "Usage credits", "cr" },
      Subscription = { "Subscription", "sub" },
    }
    local label = labels[kind]
    return label and label[1] or kind, label and label[2] or kind
  end
  if type(kind) ~= "table" then
    return "Usage", "?"
  end
  if kind.kind == "hours" then
    return kind.value .. "-hour usage", kind.value .. "h"
  elseif kind.kind == "days" then
    local days = kind.value
    return days == 1 and "Daily usage" or days .. "-day usage", days == 1 and "d" or days .. "d"
  elseif kind.kind == "monthly" then
    return "Monthly usage", "m"
  elseif kind.kind == "weekly" then
    return kind.model and "Current week (" .. kind.model .. ")" or "Weekly usage", "w"
  elseif kind.kind == "credits" then
    return "Usage credits", "cr"
  elseif kind.kind == "subscription" then
    return "Subscription", "sub"
  elseif kind.kind == "other" then
    return tostring(kind.label), tostring(kind.label)
  elseif kind.Hours then
    return kind.Hours .. "-hour usage", kind.Hours .. "h"
  elseif kind.Days then
    local days = kind.Days
    return days == 1 and "Daily usage" or days .. "-day usage", days == 1 and "d" or days .. "d"
  elseif kind.Monthly ~= nil then
    return "Monthly usage", "m"
  elseif kind.Weekly ~= nil then
    local weekly = kind.Weekly
    local model = type(weekly) == "table" and weekly.model or nil
    return model and "Current week (" .. model .. ")" or "Weekly usage", "w"
  elseif kind.Credits ~= nil then
    return "Usage credits", "cr"
  elseif kind.Subscription ~= nil then
    return "Subscription", "sub"
  elseif kind.Other then
    return tostring(kind.Other), tostring(kind.Other)
  end
  return "Usage", "?"
end

function M.normalize(state)
  if state == nil then
    return nil, nil
  end
  if type(state) == "string" then
    return state:lower(), nil
  end
  if type(state) ~= "table" then
    return "error", tostring(state)
  end
  if type(state.status) == "table" then
    return M.normalize(state.status)
  end
  if state.Ready then
    return "ready", state.Ready
  elseif state.Error then
    return "error", state.Error
  elseif state.Loading ~= nil then
    return "loading", nil
  elseif state.Unsupported ~= nil then
    return "unsupported", nil
  end
  if type(state.status) == "string" then
    if state.status == "ready" then
      return state.status, state
    elseif state.status == "error" then
      return state.status, state.error
    end
    return state.status, nil
  end
  local tag = state.state or state.status or state.kind
  if type(tag) == "string" then
    tag = tag:lower()
  elseif state.limits then
    tag = "ready"
  end
  if tag == "error" then
    return tag, state.message or state.error or state.value or state.data or state
  end
  return tag, state.usage or state.value or state.data or state
end

local function percentage_style(percentage)
  local blue = { 0x58, 0x96, 0xff }
  local red = { 0xff, 0x5c, 0x5c }
  local ratio = math.max(0, math.min(100, percentage)) / 100
  local color = string.format(
    "#%02x%02x%02x",
    math.floor(blue[1] + (red[1] - blue[1]) * ratio),
    math.floor(blue[2] + (red[2] - blue[2]) * ratio),
    math.floor(blue[3] + (red[3] - blue[3]) * ratio)
  )
  return color
end

function M.status_content(state)
  local tag, usage = M.normalize(state)
  if tag ~= "ready" or type(usage) ~= "table" then
    return {}
  end
  local spans = {}
  for _, limit in ipairs(usage.limits or {}) do
    local percentage = tonumber(limit.percentage)
    if percentage then
      if #spans > 0 then
        spans[#spans + 1] = { " ", "status_dim" }
      end
      local _, short = M.kind(limit.window or limit.kind)
      spans[#spans + 1] = { short .. math.floor(percentage) .. "%", percentage_style(percentage) }
    end
  end
  return spans
end

local function line(text, style)
  return { { text, style or "foreground" } }
end

local function append(lines, text, style)
  lines[#lines + 1] = line(text, style)
end

function M.reset_text(epoch_ms, now)
  local seconds = math.floor(epoch_ms / 1000)
  local delta = seconds - (now or os.time())
  if delta > 0 and delta < DAY then
    local hours = math.floor(delta / 3600)
    local minutes = math.floor((delta % 3600) / 60)
    return string.format("Resets in %d hr %d min", hours, minutes)
  end
  local style = delta < 7 * DAY and "weekday" or "date"
  local ok, formatted = pcall(maki.ui.format_time, epoch_ms, style)
  return "Resets " .. (ok and formatted or tostring(epoch_ms))
end

local function quota_lines(lines, state, now)
  local tag, usage = M.normalize(state)
  local provider = type(state) == "table" and state.provider or nil
  append(lines, PREFIX .. (provider and provider .. " quota" or "Provider quota"), "keybind_section")
  if tag == "loading" or tag == nil then
    append(lines, PREFIX .. "loading…", "status_dim")
  elseif tag == "unsupported" then
    append(lines, PREFIX .. "no usage endpoint for this provider", "status_dim")
  elseif tag == "error" then
    append(lines, PREFIX .. tostring(usage or "usage fetch failed"), "error")
  elseif tag == "ready" and type(usage) == "table" then
    if usage.plan then
      append(lines, PREFIX .. "plan: " .. usage.plan)
    end
    for _, limit in ipairs(usage.limits or {}) do
      local label = M.kind(limit.window or limit.kind)
      local parts = { PREFIX, label }
      if limit.percentage ~= nil then
        parts[#parts + 1] = string.format("  %d%% used", tonumber(limit.percentage) or 0)
      end
      if limit.detail then
        parts[#parts + 1] = "  " .. limit.detail
      end
      if limit.reset_at_ms then
        parts[#parts + 1] = "  " .. M.reset_text(limit.reset_at_ms, now)
      end
      append(lines, table.concat(parts))
    end
  end
end

local function total_tokens(usage)
  return value_or(usage.input, 0)
    + value_or(usage.output, 0)
    + value_or(usage.cache_creation, 0)
    + value_or(usage.cache_read, 0)
end

local function usage_row(label, usage)
  return string.format(
    "%s%-18s  in %7s  out %7s  cache-read %7s  cache-create %7s  total %7s  %6s",
    PREFIX,
    label,
    M.format_tokens(usage.input),
    M.format_tokens(usage.output),
    M.format_tokens(value_or(usage.cache_read, 0)),
    M.format_tokens(value_or(usage.cache_creation, 0)),
    M.format_tokens(total_tokens(usage)),
    usage.cost ~= nil and string.format("$%.3f", usage.cost) or EMPTY
  )
end

function M.lines(state, session, now)
  local lines = {}
  quota_lines(lines, state, now)
  lines[#lines + 1] = {}
  append(lines, PREFIX .. "Session total", "keybind_section")
  if type(session) ~= "table" then
    append(lines, PREFIX .. "session usage unavailable", "status_dim")
    return lines
  end
  if type(session.total) == "table" then
    append(lines, usage_row("all models", session.total))
  else
    append(lines, PREFIX .. "session usage unavailable", "status_dim")
  end
  if #(session.models or {}) > 0 then
    lines[#lines + 1] = {}
    append(lines, PREFIX .. "Per model", "keybind_section")
    for _, model in ipairs(session.models) do
      append(lines, usage_row(model.model or EMPTY, model))
    end
  end
  return lines
end

function M.clamp_scroll(scroll, line_count, height)
  return math.max(0, math.min(scroll, math.max(0, line_count - height)))
end

function M.scroll(scroll, key, line_count, height)
  local page = math.max(1, height - 1)
  if key == "up" or key == "k" then
    scroll = scroll - 1
  elseif key == "down" or key == "j" then
    scroll = scroll + 1
  elseif key == "pageup" or key == "ctrl+u" then
    scroll = scroll - page
  elseif key == "pagedown" or key == "ctrl+d" then
    scroll = scroll + page
  elseif key == "home" or key == "g" then
    scroll = 0
  elseif key == "end" or key == "G" then
    scroll = line_count
  end
  return M.clamp_scroll(scroll, line_count, height)
end

function M.viewport(lines, scroll, height)
  scroll = M.clamp_scroll(scroll, #lines, height)
  local visible = {}
  for index = scroll + 1, math.min(#lines, scroll + height) do
    visible[#visible + 1] = lines[index]
  end
  return visible, scroll
end

return M
