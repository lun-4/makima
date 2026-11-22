local th = require("makima.test_helpers")
local case, eq = th.case, th.eq

local function selector(events)
  local originals = {
    buf = makima.ui.buf,
    open_win = makima.ui.open_win,
    flash = makima.ui.flash,
    register_command = makima.api.register_command,
    keymap = makima.keymap.set,
    info = makima.model.info,
    levels = makima.model.thinking_levels,
    default = makima.session.set_default_thinking,
  }
  local command, lines, committed, saved, closed
  makima.api.register_command = function(spec)
    command = spec.handler
  end
  makima.keymap.set = function() end
  makima.model.info = function()
    return { supports_thinking = true }
  end
  makima.model.thinking_levels = function()
    return { "off", "low", "high" }
  end
  makima.session.set_default_thinking = function(level)
    saved = level
    return true
  end
  makima.ui.flash = function() end
  makima.ui.buf = function()
    return {
      set_lines = function(_, value)
        lines = value
      end,
    }
  end
  local step = 0
  makima.ui.open_win = function()
    return {
      width = 50,
      close = function()
        closed = true
      end,
      recv = function()
        step = step + 1
        return events[step] or { type = "close" }
      end,
    }
  end
  local agent = {
    config = function()
      return { model = { spec = "fixture", thinking = "low" } }
    end,
    set_model = function(_, _, patch)
      committed = patch.thinking
      return patch
    end,
  }
  local ok, err = pcall(function()
    load_plugin()
    command({ values = {} }, {
      agent = function()
        return agent
      end,
    })
  end)
  makima.ui.buf, makima.ui.open_win, makima.ui.flash = originals.buf, originals.open_win, originals.flash
  makima.api.register_command, makima.keymap.set = originals.register_command, originals.keymap
  makima.model.info, makima.model.thinking_levels = originals.info, originals.levels
  makima.session.set_default_thinking = originals.default
  assert(ok, tostring(err))
  return committed, saved, closed, lines
end

case("click_selects_the_rendered_level", function()
  local committed, saved, closed = selector({ { type = "click", row = 1, col = 12 } })
  eq(committed, "high")
  eq(saved, "high")
  eq(closed, true)
end)

case("chrome_spacing_and_empty_content_are_not_levels", function()
  local committed, saved, closed = selector({
    { type = "click", row = 0, col = 12 },
    { type = "click", row = 1, col = 4 },
    { type = "click", row = 1, col = 40 },
    { type = "key", key = "esc" },
  })
  eq(committed, nil)
  eq(saved, nil)
  eq(closed, true)
end)

case("keyboard_and_footer_submit_keep_current_selection", function()
  local committed = selector({ { type = "key", key = "right" }, { type = "key", key = "enter" } })
  eq(committed, "high")
end)

th.report()
