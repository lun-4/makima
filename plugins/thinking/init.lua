-- /thinking selector: pick an effort level by navigating left/right and
-- confirm with Enter. Any argument still passes straight through to
-- set_thinking, and choosing a level (or passing an argument) saves it as the
-- default for new sessions.

local THINKING_LEVELS = {
  "off",
  "adaptive",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
}

local function index_of(list, value)
  for i, v in ipairs(list) do
    if v == value then
      return i
    end
  end
  return nil
end

local function wrap_index(cursor, delta)
  return (cursor - 1 + delta) % #THINKING_LEVELS + 1
end

local function render_ladder(active)
  local spans = {}
  for i, level in ipairs(THINKING_LEVELS) do
    if i > 1 then
      spans[#spans + 1] = { "  ", "dim" }
    end
    spans[#spans + 1] = { level, (i == active) and "selected" or "item" }
  end
  return spans
end

local function set_thinking(mode)
  local result, err = maki.session.set_thinking({ mode = mode, set_default = true })
  if err then
    maki.ui.flash("Thinking: " .. err)
  else
    maki.ui.flash("Thinking: " .. result.mode .. " (default for new sessions)")
  end
end

local function open_selector()
  local info, err = maki.session.thinking()
  if err then
    maki.ui.flash("Thinking: " .. err)
    return
  end
  if not info.supports_thinking then
    maki.ui.flash("Thinking requires a model that supports it")
    return
  end

  local cursor = index_of(THINKING_LEVELS, info.mode) or 1
  local buf = maki.ui.buf()
  buf:set_lines({ render_ladder(cursor) })

  local win = maki.ui.open_win(buf, {
    title = "Thinking",
    height = 3,
    footer = {
      { "h/l", "level" },
      { "enter", "set" },
      { "esc", "cancel" },
    },
  })

  while true do
    local ev = win:recv()
    if not ev or ev.type == "close" then
      break
    end
    if ev.type == "key" then
      if ev.key == "left" or ev.key == "h" then
        cursor = wrap_index(cursor, -1)
        buf:set_lines({ render_ladder(cursor) })
      elseif ev.key == "right" or ev.key == "l" then
        cursor = wrap_index(cursor, 1)
        buf:set_lines({ render_ladder(cursor) })
      elseif ev.key == "enter" then
        win:close()
        set_thinking(THINKING_LEVELS[cursor])
        return
      elseif ev.key == "esc" then
        win:close()
        return
      end
    end
  end
end

maki.api.register_command({
  name = "/thinking",
  description = "Set thinking effort (bare opens a selector)",
  argument_hint = "[effort]",
  nargs = "?",
  tui_only = true,
  handler = function(opts)
    local args = tostring(opts.args or ""):gsub("^%s+", ""):gsub("%s+$", "")
    if args ~= "" then
      set_thinking(args)
      return
    end
    open_selector()
  end,
})
