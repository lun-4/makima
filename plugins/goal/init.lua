-- Packaging scaffold. The controller and model tool are not wired yet.
makima.api.register_command({
  name = "goal",
  description = "Goal tracking (not available yet)",
  arguments = { raw = true },
  tui_only = true,
  handler = function()
    error("Goal tracking is not available yet")
  end,
})
