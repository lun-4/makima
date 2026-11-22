local command
local register = makima.api.register_command
makima.api.register_command = function(spec)
  command = spec
  register(spec)
end
load_plugin()
assert(command.name == "goal" and command.tui_only and command.arguments.raw)
local ok, err = pcall(command.handler)
assert(not ok and string.find(err, "Goal tracking is not available yet", 1, true))
