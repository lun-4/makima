local regolo = require("providers.regolo")
local compat = require("providers.openai_compat")
local t = require("makima.test_helpers")

local MODEL_GROUPS = [[{"data":[
  {"model_group": "glm5.2", "mode": "chat", "input_cost_per_token": 2e-06, "output_cost_per_token": 5.2e-06,
   "max_input_tokens": 96000.0, "max_output_tokens": 96000.0, "max_tokens": null,
   "supports_reasoning": true, "supports_vision": false},
  {"model_group": "qwen3-coder-next", "mode": "chat", "input_cost_per_token": 5e-07, "output_cost_per_token": 2e-06,
   "max_input_tokens": null, "max_output_tokens": 120000.0, "max_tokens": 240000.0,
   "supports_reasoning": false, "supports_vision": true},
  {"model_group": "Qwen3-Embedding-8B", "mode": "embedding", "input_cost_per_token": 0.0, "output_cost_per_token": 0.0,
   "supports_reasoning": false, "supports_vision": false}
]}]]
local SPEND_LOGS = {
  {
    model_group = "qwen3-coder-next",
    total_tokens = 20000,
    prompt_tokens = 15000,
    completion_tokens = 5000,
    spend = 0.01,
  },
  {
    model_group = "qwen3-coder-next",
    total_tokens = 8000,
    prompt_tokens = 5000,
    completion_tokens = 3000,
    spend = 0.0042,
  },
  { model_group = "glm5.2", total_tokens = 360, prompt_tokens = 200, completion_tokens = 160, spend = 0.0009 },
}
local AUGUST_25_AFTERNOON = 1787665510

local function near(actual, expected)
  assert(math.abs(actual - expected) < 1e-9, tostring(actual) .. " ~= " .. tostring(expected))
end

local function join(ids)
  return regolo.join(ids, makima.json.decode(MODEL_GROUPS).data)
end

t.case("group_metadata_maps_to_descriptors", function()
  local model = join({ "glm5.2" })[1]
  t.eq(model.context_window, 96000)
  t.eq(model.max_output_tokens, 96000)
  near(model.pricing.input, 2)
  near(model.pricing.output, 5.2)
  t.eq(model.supports_thinking, true)
  t.eq(model.supports_vision, false)
  local coder = join({ "qwen3-coder-next" })[1]
  t.eq(coder.context_window, 240000)
  t.eq(coder.supports_thinking, false)
end)

t.case("non_chat_groups_and_ungrouped_ids_are_dropped", function()
  local models = join({ "Qwen3-Embedding-8B", "glm5.2", "brand-new-model" })
  t.eq(#models, 1)
  t.eq(models[1].id, "glm5.2")
end)

t.case("listing_falls_back_to_bare_ids", function()
  local ctx = t.provider_ctx({
    t.response({ '{"data":[{"id":"b"},{"id":"a"}]}' }),
    t.response({ "down" }, { status = 500 }),
  })
  local client = assert(compat.client(ctx, regolo.def, { api_key = "k" }))
  t.same(regolo.list_models(ctx, client), { { id = "a" }, { id = "b" } })
  t.eq(ctx.requests[2].url, "https://api.regolo.ai/model_group/info")
end)

t.case("root_url_strips_the_version", function()
  t.eq(regolo.root_url("https://api.regolo.ai/v1"), "https://api.regolo.ai")
  t.eq(regolo.root_url("https://api.regolo.ai/v1/"), "https://api.regolo.ai")
  t.eq(regolo.root_url("https://api.regolo.ai"), "https://api.regolo.ai")
end)

t.case("key_budget", function()
  t.same(regolo.key_limit({ spend = 2.5, max_budget = 10.0, budget_reset_at = "2026-09-01T00:00:00Z" }), {
    kind = "Credits",
    percentage = 25,
    reset_at = 1788220800000,
    detail = "$2.50 spent of $10.00 budget",
  })
  t.same(regolo.key_limit({ spend = 0.42 }), { kind = "Credits", detail = "$0.42 spent" })
end)

t.case("daily_activity", function()
  t.same(regolo.activity_limit({ sum_api_requests = 14, sum_total_tokens = 29027 }, AUGUST_25_AFTERNOON), {
    kind = { Days = 1 },
    reset_at = 1787702400000,
    detail = "14 requests · 29027 tokens",
  })
end)

t.case("spend_logs_aggregate_by_model", function()
  t.same(regolo.spend_rows(SPEND_LOGS), {
    {
      model = "qwen3-coder-next",
      input_tokens = 20000,
      output_tokens = 8000,
      total_tokens = 28000,
      spend_microdollars = 14200,
    },
    { model = "glm5.2", input_tokens = 200, output_tokens = 160, total_tokens = 360, spend_microdollars = 900 },
  })
end)

t.case("usage_queries_today_at_the_root", function()
  local ctx = t.provider_ctx({
    t.response({ '{"info":{"spend":1,"max_budget":null}}' }),
    t.response({ '{"sum_api_requests":1,"sum_total_tokens":2}' }),
    t.response({ "down" }, { status = 500 }),
  })
  local client = assert(compat.client(ctx, regolo.def, { api_key = "k" }))
  local usage = assert(regolo.fetch_usage(ctx, client))
  t.eq(#usage.limits, 2)
  t.eq(usage.by_model_today, nil)
  t.eq(ctx.requests[1].url, "https://api.regolo.ai/key/info")
  local day = makima.time.now().secs
  assert(ctx.requests[2].url:find("/global/activity?start_date=", 1, true))
  assert(ctx.requests[3].url:find("/spend/logs/v2?start_date=", 1, true))
  assert(day > 0)
end)

t.case("curated_defaults", function()
  local models = compat.provider(regolo.def).models
  t.eq(models[2].id, "qwen3-coder-next")
  t.eq(models[2].tier, "medium")
  t.eq(regolo.def.max_tokens_field, "max_completion_tokens")
end)

t.report()
