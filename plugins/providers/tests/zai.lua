local zai = require("providers.zai")
local compat = require("providers.openai_compat")
local t = require("maki.test_helpers")

local QUOTA_BODY = [[{"code":200,"data":{"limits":[
  {"type":"TOKENS_LIMIT","unit":3,"percentage":16,"nextResetTime":1777819631597},
  {"type":"TOKENS_LIMIT","unit":6,"percentage":4,"nextResetTime":1778262784969},
  {"type":"TIME_LIMIT","unit":5,"percentage":0,"nextResetTime":1780336384978}
],"level":"lite"}}]]
local FUNDS = "insufficient balance"

t.case("thinking_from_glm_5_2", function()
  for id, expected in pairs({
    ["glm-5.3"] = true,
    ["glm-5.3-flash"] = true,
    ["glm-5.4"] = true,
    ["glm-6"] = true,
    ["glm-5.2"] = true,
    ["glm-5.1"] = false,
    ["glm-5-code"] = false,
    ["glm-4.7"] = false,
  }) do
    t.eq(compat.describe(zai.def, { id = id }).supports_thinking, expected, id)
  end
end)

t.case("curated_rows", function()
  t.eq(compat.describe(zai.def, { id = "glm-5.3" }).context_window, 1000000)
  t.eq(compat.describe(zai.def, { id = "glm-5.1" }).context_window, 200000)
  t.eq(compat.describe(zai.def, { id = "glm-5.4" }).context_window, 200000)
  local flash = compat.describe(zai.def, { id = "glm-5.3-flash" })
  t.eq(flash.tier, "weak")
  t.eq(flash.supports_vision, true)
  local tiers = {}
  for _, model in ipairs(compat.provider(zai.def).models) do
    tiers[model.tier] = tiers[model.tier] or model.id
  end
  t.same(tiers, { strong = "glm-5-code", medium = "glm-4.7", weak = "glm-4.7-flash" })
end)

t.case("glm_effort_only_when_thinking", function()
  local body = {}
  zai.def.encode(body, { thinking = { kind = "off" }, model = {} }, { supports_thinking = true })
  t.eq(body.reasoning_effort, "none")
  body = {}
  zai.def.encode(body, { thinking = { kind = "effort", level = "max" }, model = {} }, { supports_thinking = true })
  t.eq(body.reasoning_effort, "xhigh")
  body = {}
  zai.def.encode(body, { thinking = { kind = "off" }, model = {} }, { supports_thinking = false })
  t.eq(body.reasoning_effort, nil)
end)

t.case("insufficient_funds_is_a_billing_error", function()
  t.same(zai.api_error({ status = 429, message = "code 1113" }), { status = 402, message = "code 1113" })
  t.same(zai.api_error({ status = 503, message = FUNDS }), { status = 402, message = FUNDS })
  t.same(zai.api_error({ status = 400, message = FUNDS }), { status = 400, message = FUNDS })
  t.same(zai.api_error({ status = 429, message = "slow down" }), { status = 429, message = "slow down" })
end)

t.case("coding_plan_base_url", function()
  t.with_env({}, function()
    t.eq(compat.base_url(zai.def, { plan = "coding" }), "https://api.z.ai/api/coding/paas/v4")
    t.eq(compat.base_url(zai.def, {}), "https://api.z.ai/api/paas/v4")
  end)
end)

t.case("quota", function()
  t.same(zai.parse_quota(maki.json.decode(QUOTA_BODY)), {
    plan = "lite",
    limits = {
      { kind = { Hours = 5 }, percentage = 16, reset_at = 1777819631597 },
      { kind = { Weekly = {} }, percentage = 4, reset_at = 1778262784969 },
      { kind = "Subscription", percentage = 0, reset_at = 1780336384978 },
    },
  })
  t.same(zai.parse_quota({ data = { limits = { { type = "TOKENS_LIMIT", unit = 9, percentage = 50 } } } }), {
    limits = { { kind = { Other = "TOKENS_LIMIT #9" }, percentage = 50 } },
  })
end)

t.report()
