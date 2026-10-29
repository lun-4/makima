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
