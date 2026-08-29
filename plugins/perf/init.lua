-- Splash fps overlay, driven through the `maki.store` registry "perf": while
-- the `fps_overlay` key is true, a timer repaints a live fps and per-frame
-- render-time readout into the input-bar hint row, reading the host-measured
-- numbers from `maki.perf.timings()`.
--
-- Toggle with `/splash-fps`; the store flag makes the state inspectable and
-- reload-safe.

local REGISTRY = "perf"
local FPS_KEY = "fps_overlay"
local REFRESH_SECS = 0.25

local timer_id = nil

local function render_readout()
  local t = maki.perf.timings()
  local text = string.format("splash %.0f fps · %.1f ms", t.fps, t.render_ms)
  maki.ui.set_status_hint({ { text, "dim" } })
end

local function set_overlay(enabled)
  if enabled and timer_id == nil then
    timer_id = maki.timer.set(REFRESH_SECS, render_readout)
    render_readout()
  elseif not enabled and timer_id ~= nil then
    maki.timer.del(timer_id)
    timer_id = nil
    maki.ui.set_status_hint(nil)
  end
end

local function is_enabled()
  return maki.store.collect(REGISTRY)[FPS_KEY] == true
end

maki.api.register_command({
  name = "/splash-fps",
  description = "Toggle the splash fps overlay: live fps and per-frame render time.",
  tui_only = true,
  handler = function()
    maki.store.register(REGISTRY, FPS_KEY, not is_enabled())
  end,
})

maki.api.create_autocmd("StoreChanged", {
  callback = function(ev)
    if ev.data.registry == REGISTRY then
      set_overlay(is_enabled())
    end
  end,
})

set_overlay(is_enabled())
