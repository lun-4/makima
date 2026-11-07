local th = require("maki.test_helpers")
local case, eq = th.case, th.eq

local function selector(events)
  local originals = {
    buf = maki.ui.buf,
    open_win = maki.ui.open_win,
    flash = maki.ui.flash,
    register_command = maki.api.register_command,
    keymap = maki.keymap.set,
    info = maki.model.info,
    levels = maki.model.thinking_levels,
    default = maki.session.set_default_thinking,
  }
  local command, lines, committed, saved, closed
  maki.api.register_command = function(spec)
    command = spec.handler
  end
  maki.keymap.set = function() end
  maki.model.info = function()
    return { supports_thinking = true }
  end
  maki.model.thinking_levels = function()
    return { "off", "low", "high" }
  end
  maki.session.set_default_thinking = function(level)
    saved = level
    return true
  end
  maki.ui.flash = function() end
  maki.ui.buf = function()
    return {
      set_lines = function(_, value)
        lines = value
      end,
    }
  end
  local step = 0
  maki.ui.open_win = function()
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
  maki.ui.buf, maki.ui.open_win, maki.ui.flash = originals.buf, originals.open_win, originals.flash
  maki.api.register_command, maki.keymap.set = originals.register_command, originals.keymap
  maki.model.info, maki.model.thinking_levels = originals.info, originals.levels
  maki.session.set_default_thinking = originals.default
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
