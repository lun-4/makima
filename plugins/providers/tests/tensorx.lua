local tensorx = require("providers.tensorx")
local compat = require("providers.openai_compat")
local t = require("maki.test_helpers")

local function encode(knobs, kind, id)
  local body = {}
  tensorx.encode(body, { thinking = { kind = kind }, model = {} }, { id = id or "z-ai/glm-5.2", provider_data = knobs })
  return body
end

t.case("parameter_flags_pick_the_knob", function()
  t.same(encode({ thinking = true, reasoning_effort = false }, "off"), { thinking = false })
  t.same(
    encode({ thinking = true, reasoning_effort = true }, "adaptive"),
    { thinking = true, reasoning_effort = "high" }
  )
  t.same(encode({ thinking = false, reasoning_effort = true }, "off"), { reasoning_effort = "none" })
  t.same(encode(nil, "adaptive"), {})
end)

t.case("deepseek_thinks_through_the_chat_template", function()
  t.same(encode(nil, "adaptive", "deepseek/deepseek-flash"), { chat_template_kwargs = { thinking = true } })
  t.same(encode(nil, "off", "deepseek/deepseek-flash"), {})
  t.same(encode(nil, "adaptive", "deepseek/deepseek-reasoner"), {})
  t.same(encode({ thinking = true }, "adaptive", "deepseek/deepseek-flash"), { thinking = true })
end)

t.case("model_info_listing", function()
  local models = compat.parse_listing(
    maki.json.decode([[{"data":[
      {"model_name": "z-ai/glm-5.2", "model_info": {"mode": "chat", "max_tokens": 200000, "max_output_tokens": 128000,
       "input_cost_per_token": 1e-06, "output_cost_per_token": 3e-06, "cache_read_input_token_cost": 2e-07,
       "supports_vision": false, "supports_reasoning": true,
       "supported_openai_params": ["thinking", "tools"]}},
      {"model_name": "embed", "model_info": {"mode": "embedding"}},
      {"model_name": "a-plain", "model_info": {"max_input_tokens": 1000}}
    ]}]]),
    tensorx.parse_model
  )
  t.eq(#models, 2)
  t.same(models[1], { id = "a-plain", context_window = 1000, supports_vision = false })
  local glm = models[2]
  t.eq(glm.context_window, 200000)
  t.eq(glm.max_output_tokens, nil)
  t.same(glm.provider_data, { thinking = true, reasoning_effort = false })
  t.eq(glm.supports_thinking, true)
  assert(math.abs(glm.pricing.cache_read - 0.2) < 1e-9)
  t.eq(glm.pricing.cache_write, 0)
end)

t.case("discovery_reads_the_model_info_endpoint", function()
  local ctx = t.provider_ctx({ t.response({ '{"data":[]}' }) })
  local models = compat.provider(tensorx.def).discover(ctx, { api_key = "k" })
  t.same(models, {})
  t.eq(ctx.requests[1].url, "https://api.tensorx.ai/v1/model/info")
end)

t.report()
