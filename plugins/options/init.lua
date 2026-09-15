local helpers = require("options_helpers")

local function read_options()
  local snapshot, err = maki.session.options()
  if not snapshot then
    maki.ui.flash("Options unavailable: " .. tostring(err))
  end
  return snapshot
end

local function command()
  local snapshot = read_options()
  if not snapshot then
    return
  end
  local option_items = helpers.option_items(snapshot)
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

  snapshot = read_options()
  if not snapshot then
    return
  end
  local option = helpers.find(snapshot, selected_item.id)
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

  snapshot = read_options()
  if not snapshot then
    return
  end
  option = helpers.find(snapshot, selected_item.id)
  if not option then
    maki.ui.flash("Option is no longer available: " .. selected_item.id)
    return
  end
  local valid = false
  for _, value in ipairs(option.values or {}) do
    if value.value == value_item.value then
      valid = true
      break
    end
  end
  if not valid then
    maki.ui.flash("Option values changed: " .. selected_item.id)
    return
  end
  local ok, err = maki.session.set_option(selected_item.id, value_item.value)
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
