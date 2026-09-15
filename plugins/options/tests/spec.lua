local helpers = require("options_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

local function snapshot()
  return {
    version = 3,
    options = {
      {
        id = "model",
        name = "Model",
        description = "Model used for future turns",
        category = "model",
        current_value = "test/model",
        values = { { value = "test/model", name = "test/model" } },
      },
      {
        id = "fast",
        name = "Fast",
        description = "Use fast tier",
        category = "mode",
        current_value = "disabled",
        values = {
          { value = "enabled", name = "Enabled" },
          { value = "disabled", name = "Disabled" },
        },
      },
    },
  }
end

case("renders_options_with_current_values", function()
  local items = helpers.option_items(snapshot())
  eq(#items, 2)
  eq(items[1].label, "Model: test/model")
  eq(items[2].label, "Fast: disabled")
  eq(items[2].detail, "Use fast tier (mode)")
end)

case("opens_values_at_current_selection", function()
  local option = helpers.find(snapshot(), "fast")
  local items, cursor = helpers.value_items(option)
  eq(#items, 2)
  eq(cursor, 2)
  eq(items[2].value, "disabled")
end)

case("missing_option_is_detected", function()
  eq(helpers.find(snapshot(), "removed"), nil)
end)
