local helpers = require("options_helpers")

local function read_options(ctx, agent)
  local config, err = agent:config(ctx)
  if not config then
    maki.ui.flash("Options unavailable: " .. tostring(err))
    return nil
  end
  return helpers.options(maki.api.get_session_options(), config.options)
end

local function command(_, ctx)
  local agent, agent_err = ctx and ctx:agent()
  if not agent then
    maki.ui.flash("Options unavailable: " .. tostring(agent_err or "no agent"))
    return
  end
  local options = read_options(ctx, agent)
  if not options then
    return
  end
  local option_items = helpers.option_items(options)
  if #option_items == 0 then
    maki.ui.flash("No session options available")
    return
  end
  local selected = maki.ui.open_list_picker(option_items, { title = "Session options" })
  if not selected or selected.type ~= "choice" then
    return
  end
  local selected_item = option_items[selected.index]
  if not selected_item then
    return
  end

  options = read_options(ctx, agent)
  if not options then
    return
  end
  local option = helpers.find(options, selected_item.id)
  if not option then
    maki.ui.flash("Option is no longer available: " .. selected_item.id)
    return
  end
  local value_items, cursor = helpers.value_items(option)
  local value_selected = maki.ui.open_list_picker(value_items, {
    title = option.name,
    cursor = cursor,
  })
  if not value_selected or value_selected.type ~= "choice" then
    return
  end
  local value_item = value_items[value_selected.index]
  if not value_item then
    return
  end
  local ok, err = agent:set_option(ctx, selected_item.id, value_item.value)
  if not ok then
    maki.ui.flash("Option update failed: " .. tostring(err))
  end
end

maki.api.register_command({
  name = "/options",
  description = "Browse and change session options",
  tui_only = true,
  handler = command,
})
