-- /thinking selector: pick an effort level by navigating left/right and
-- confirm with Enter. Any argument passes straight through to the target
-- agent's set_model, and choosing a level (or passing an argument) saves it as
-- the default for new sessions. Without an interactive UI the selector has
-- nowhere to open, so only the argument form works there. A level that cannot
-- be set fails the command; the M-t keymap flashes the reason instead.

local NO_SELECTOR = "the selector needs the TUI; pass a level, e.g. /thinking high"

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

local function fail(err)
  error("Thinking: " .. tostring(err), 0)
end

-- Resolves the agent this invocation targets: the command's pinned target or
-- the agent selected when the key was pressed.
local function target(ctx)
  if not ctx then
    return nil, nil, "no agent target"
  end
  local agent, err = ctx:agent()
  if not agent then
    return nil, nil, err
  end
  local config, config_err = agent:config(ctx)
  if not config then
    return nil, nil, config_err
  end
  return agent, config.model
end

local function set_thinking(ctx, agent, mode)
  local committed, err = agent:set_model(ctx, { thinking = mode })
  if not committed then
    fail(err)
  end
  local _, default_err = maki.session.set_default_thinking(committed.thinking)
  if default_err then
    maki.ui.flash("Thinking: " .. committed.thinking .. " (default not saved: " .. default_err .. ")")
  else
    maki.ui.flash("Thinking: " .. committed.thinking .. " (default for new sessions)")
  end
end

local function open_selector(ctx)
  local agent, selection, err = target(ctx)
  if not agent then
    fail(err)
  end
  local info, info_err = maki.model.info(selection.spec)
  if not info then
    fail(info_err)
  end
  if not info.supports_thinking then
    fail("requires a model that supports it")
  end

  local levels = maki.model.thinking_levels()
  local cursor = index_of(levels, selection.thinking) or 1
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

  local opened = false
  while true do
    local ev = win:recv()
    if not ev and not opened then
      error(NO_SELECTOR)
    end
    opened = true
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
        set_thinking(ctx, agent, levels[cursor])
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
  tui_only = false,
  arguments = {
    {
      name = "effort",
      type = "string",
      optional = true,
      completion = {
        get_items = function()
          local items = {}
          for _, level in ipairs(maki.model.thinking_levels()) do
            items[#items + 1] = { label = level, insertion = level }
          end
          return items
        end,
      },
    },
  },
  handler = function(opts, ctx)
    local effort = opts.values.effort
    if not effort then
      open_selector(ctx)
      return
    end
    local agent, _, err = target(ctx)
    if not agent then
      fail(err)
    end
    set_thinking(ctx, agent, effort)
  end,
})

maki.keymap.set("n", "<M-t>", function(_, ctx)
  local ok, err = pcall(open_selector, ctx)
  if not ok then
    maki.ui.flash(tostring(err))
  end
end, { desc = "Thinking effort" })
