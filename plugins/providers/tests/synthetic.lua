local synthetic = require("providers.synthetic")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local t = require("maki.test_helpers")

local Y2K_MS = 946684800000
local QUOTA_URL = "https://api.synthetic.new/v2/quotas"
local V3_BODY = [[{
  "subscription": {"limit": 500, "requests": 0, "renewsAt": "2030-01-01T00:00:00.000Z"},
  "rollingFiveHourLimit": {"remaining": 300, "max": 1000, "limited": false, "nextTickAt": "2000-01-01T00:00:00.000Z"},
  "weeklyTokenLimit": {"percentRemaining": 50.0, "maxCredits": "$24.00", "nextRegenAt": "2000-01-01T00:00:00.000Z"}
}]]
local V2_BODY = [[{
  "subscription": {"limit": 500, "requests": 250, "renewsAt": "2000-01-01T00:00:00.000Z"},
  "search": {"hourly": {"limit": 10, "requests": 2}}
}]]

local function usage(body)
  local result
  t.with_env({ SYNTHETIC_API_KEY = "k" }, function()
    local ctx = t.provider_ctx({ t.response({ body }) })
    local instance = compat.provider(synthetic.def).prepare(ctx, { id = "hf:moonshotai/Kimi-K2.5" }, {})
    result = { instance.fetch_usage(ctx) }
    t.eq(ctx.requests[1].url, QUOTA_URL)
  end)
  return result[1], result[2]
end

t.case("v3_rolling_lanes", function()
  t.same(usage(V3_BODY), {
    limits = {
      { kind = { Hours = 5 }, percentage = 70, reset_at = Y2K_MS },
      { kind = { Weekly = {} }, percentage = 50, reset_at = Y2K_MS },
    },
  })
end)

t.case("v2_subscription_fallback", function()
  t.same(usage(V2_BODY), { limits = { { kind = { Hours = 5 }, percentage = 50, reset_at = Y2K_MS } } })
end)

t.case("malformed_or_empty_bodies_fail", function()
  for _, body in ipairs({ "not json", "{}", '{"rollingFiveHourLimit": {"remaining": 1}}' }) do
    local result, err = usage(body)
    t.eq(result, nil)
    assert(err, body)
  end
  t.same(select(2, usage("{}")), http.config_error(synthetic.EMPTY_USAGE_ERROR))
end)

t.case("percentage_clamps", function()
  t.eq(synthetic.percentage(300, 1000), 30)
  t.eq(synthetic.percentage(250, 1000), 25)
  t.eq(synthetic.percentage(999, 1000), 100)
  t.eq(synthetic.percentage(1500, 1000), 100)
  t.eq(synthetic.percentage(1, 0), nil)
end)

t.case("curated_models_take_tool_examples_and_thinking", function()
  local models = compat.provider(synthetic.def).models
  t.eq(models[1].id, "hf:moonshotai/Kimi-K2.5")
  t.eq(models[1].tier, "strong")
  t.eq(models[1].supports_tool_examples, true)
  t.eq(models[1].supports_thinking, true)
  local unknown = compat.describe(synthetic.def, { id = "hf:nonexistent" })
  t.eq(unknown.context_window, 128000)
  t.eq(unknown.max_output_tokens, 32000)
end)

t.case("standard_effort_and_completion_tokens", function()
  local body = { messages = {} }
  synthetic.def.encode(body, { thinking = { kind = "effort", level = "max" }, model = {} })
  t.eq(body.reasoning_effort, "high")
  synthetic.def.encode(body, { thinking = { kind = "off" }, model = {} })
  t.eq(body.reasoning_effort, nil)
  t.eq(synthetic.def.max_tokens_field, "max_completion_tokens")
end)

t.report()
