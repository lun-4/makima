local regolo = require("maki.regolo")
local models = regolo.join_models({ "chat", "embedding", "missing" }, {
  {
    model_group = "chat",
    mode = "chat",
    max_tokens = 500,
    max_input_tokens = 400,
    max_output_tokens = 50,
    input_cost_per_token = 0.000001,
    output_cost_per_token = 0.000002,
    supports_reasoning = true,
    supports_vision = false,
  },
  { model_group = "embedding", mode = "embedding" },
})
assert(#models == 1 and models[1].id == "chat")
assert(models[1].context_window == 400 and models[1].max_output_tokens == 50)
assert(models[1].pricing.input == 1 and models[1].pricing.output == 2)
assert(models[1].supports_thinking and not models[1].supports_vision)
local bounded = regolo.join_models({ "large", "fractional" }, {
  { model_group = "large", mode = "chat", max_input_tokens = 4294967296, max_output_tokens = -2 },
  { model_group = "fractional", mode = "chat", max_tokens = 123.9 },
})
assert(bounded[1].context_window == nil and bounded[1].max_output_tokens == 0)
assert(bounded[2].context_window == 123)
