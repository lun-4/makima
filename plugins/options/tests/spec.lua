local helpers = require("options_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local function definitions()
  return {
    {
      id = "yolo",
      name = "YOLO",
      description = "Skip permission prompts",
      category = "mode",
      values = {
        { value = "enabled", name = "Enabled" },
        { value = "disabled", name = "Disabled" },
      },
    },
    {
      id = "bash.auto_mode",
      name = "Bash auto mode",
      description = "Classify commands",
      category = "mode",
      values = {
        { value = "enabled", name = "Enabled" },
        { value = "disabled", name = "Disabled" },
      },
    },
  }
end

local function snapshot()
  return helpers.options(definitions(), { yolo = "disabled", ["bash.auto_mode"] = "enabled" })
end

case("lists_only_definitions_the_session_has_a_value_for", function()
  local options = helpers.options(definitions(), { yolo = "enabled" })
  eq(#options, 1)
  eq(options[1].id, "yolo")
  eq(options[1].current_value, "enabled")
end)

case("renders_options_with_current_values", function()
  local items = helpers.option_items(snapshot())
  eq(#items, 2)
  eq(items[1].label, "YOLO: disabled")
  eq(items[2].label, "Bash auto mode: enabled")
  eq(items[2].detail, "Classify commands (mode)")
end)

case("opens_values_at_current_selection", function()
  local option = helpers.find(snapshot(), "yolo")
  local items, cursor = helpers.value_items(option)
  eq(#items, 2)
  eq(cursor, 2)
  eq(items[2].value, "disabled")
end)

case("missing_option_is_detected", function()
  eq(helpers.find(snapshot(), "removed"), nil)
end)
