-- /thinking selector: pick an effort level by navigating left/right and
-- confirm with Enter. Any argument still passes straight through to
-- set_thinking, and choosing a level (or passing an argument) saves it as the
-- default for new sessions.

local function index_of(list, value)
  for i, v in ipairs(list) do
    if v == value then
      return i
    end
  end
  return nil
end

local function wrap_index(cursor, delta, levels)
  return (cursor - 1 + delta) % #levels + 1
end

local function render_ladder(active, levels)
  local spans = {}
  for i, level in ipairs(levels) do
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

  local levels = info.options or {}
  if #levels == 0 then
    maki.ui.flash("Thinking: host returned no options")
    return
  end
  local cursor = index_of(levels, info.mode) or 1
  local buf = maki.ui.buf()
  buf:set_lines({ render_ladder(cursor, levels) })

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
        cursor = wrap_index(cursor, -1, levels)
        buf:set_lines({ render_ladder(cursor, levels) })
      elseif ev.key == "right" or ev.key == "l" then
        cursor = wrap_index(cursor, 1, levels)
        buf:set_lines({ render_ladder(cursor, levels) })
      elseif ev.key == "enter" then
        win:close()
        set_thinking(levels[cursor])
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
  completion = {
    get_items = function()
      local info, err = maki.session.thinking()
      if err or not info.supports_thinking or not info.options then
        return {}
      end
      local items = {}
      for _, option in ipairs(info.options) do
        items[#items + 1] = { label = option, insertion = option }
      end
      return items
    end,
  },
  handler = function(opts)
    local args = tostring(opts.args or ""):gsub("^%s+", ""):gsub("%s+$", "")
    if args ~= "" then
      set_thinking(args)
      return
    end
    open_selector()
  end,
})
