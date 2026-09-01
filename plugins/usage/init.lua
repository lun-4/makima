local helpers = require("usage_helpers")

local REFRESH_SECONDS = 30
local MODAL_TICK_MS = 500
local MODAL_WIDTH = "60%"
local MODAL_HEIGHT = "70%"

local active = false
local modal = nil
local generation = 0

local function quota_state()
  return maki.usage.get()
end

local function repaint_status(state)
  maki.ui.set_status_content(helpers.status_content(state or quota_state()))
end

local function fetch(force)
  local _, err = maki.usage.fetch({ force = force })
  if err then
    maki.log.warn("usage refresh failed: " .. err)
  end
end

local function activate()
  if active then
    return
  end
  active = true
  maki.usage.on_change(function(state)
    repaint_status(state)
    if modal then
      modal.dirty = true
    end
  end)
  repaint_status()
  maki.async.run(function()
    fetch(false)
  end)
  maki.timer.set(REFRESH_SECONDS, function()
    fetch(false)
  end)
end

local function close_modal(expected_generation)
  if not modal or modal.generation ~= expected_generation then
    return
  end
  local win = modal.win
  modal = nil
  generation = generation + 1
  win:close()
end

local function render(current)
  if modal ~= current then
    return
  end
  local lines = helpers.lines(quota_state(), current.session)
  local size = maki.ui.terminal_size()
  local height = helpers.modal_height(#lines, size.rows)
  if height ~= current.height then
    current.height = height
    current.win:set_config({ height = helpers.modal_window_height(height) })
  end
  local visible, scroll = helpers.viewport(lines, current.scroll, current.height)
  current.scroll = scroll
  current.line_count = #lines
  current.buf:set_lines(visible)
end

local function refresh_modal(current, force)
  local expected_generation = current.generation
  maki.async.run(function()
    fetch(force)
    if modal and modal.generation == expected_generation then
      modal.dirty = true
    end
  end)
end

local function update_session(current)
  local usage, err = maki.session.usage()
  if modal ~= current then
    return false
  end
  if usage then
    current.session = usage
  elseif err then
    maki.log.warn("session usage refresh failed: " .. err)
  end
  return true
end

local function open()
  if modal then
    close_modal(modal.generation)
    return
  end

  generation = generation + 1
  local buf = maki.ui.buf()
  local win = maki.ui.open_win(buf, {
    title = " Usage ",
    width = MODAL_WIDTH,
    height = MODAL_HEIGHT,
    border = "rounded",
    focus = true,
    footer = { { "Ctrl+R", "refresh" }, { "q", "close" } },
  })
  local current = {
    generation = generation,
    buf = buf,
    win = win,
    height = win.height,
    scroll = 0,
    line_count = 0,
    dirty = true,
  }
  modal = current
  if not update_session(current) then
    return
  end
  render(current)
  refresh_modal(current, true)

  while modal == current do
    local ev = win:recv(MODAL_TICK_MS)
    if not ev or ev.type == "close" then
      close_modal(current.generation)
    elseif ev.type == "resize" then
      current.height = ev.height
      current.dirty = true
    elseif ev.type == "key" then
      if helpers.is_close_key(ev.key) then
        close_modal(current.generation)
      elseif ev.key == "ctrl+r" then
        refresh_modal(current, true)
      else
        local scroll = helpers.scroll(current.scroll, ev.key, current.line_count, current.height)
        current.dirty = current.dirty or scroll ~= current.scroll
        current.scroll = scroll
      end
    elseif ev.type == "timeout" then
      update_session(current)
      current.dirty = true
    end
    if modal == current and current.dirty then
      current.dirty = false
      render(current)
    end
  end
end

maki.api.register_command({
  name = "/usage",
  description = "Show provider quota and focused-session token usage",
  tui_only = true,
  handler = open,
})

maki.api.create_autocmd("SessionFocusChanged", {
  once = true,
  callback = activate,
})

maki.api.create_autocmd("ProviderChanged", {
  callback = function()
    maki.ui.set_status_content(nil)
    if active then
      maki.async.run(function()
        fetch(false)
      end)
    end
  end,
})
