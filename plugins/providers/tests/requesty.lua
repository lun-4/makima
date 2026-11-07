local requesty = require("providers.requesty")
local compat = require("providers.openai_compat")
local t = require("maki.test_helpers")

local SONNET = "anthropic/claude-sonnet-4-5"
local BASE = "https://router.requesty.ai/v1"

local function sonnet(overrides)
  local row = {
    id = SONNET,
    api = "chat",
    context_window = 200000,
    max_output_tokens = 64000,
    input_price = 0.000003,
    output_price = 0.000015,
    caching_price = 0.00000375,
    cached_price = 0.0000003,
    supports_reasoning = true,
    supports_vision = true,
  }
  for key, value in pairs(overrides or {}) do
    row[key] = value
  end
  return row
end

local function near(actual, expected)
  assert(math.abs(actual - expected) < 1e-9, tostring(actual) .. " ~= " .. tostring(expected))
end

t.case("entry_scales_prices_to_per_million", function()
  local model = requesty.parse_model(sonnet())
  t.eq(model.id, SONNET)
  t.eq(model.context_window, 200000)
  t.eq(model.max_output_tokens, 64000)
  t.eq(model.supports_vision, true)
  t.eq(model.supports_thinking, true)
  near(model.pricing.input, 3)
  near(model.pricing.output, 15)
  near(model.pricing.cache_write, 3.75)
  near(model.pricing.cache_read, 0.3)
end)

t.case("half_a_price_is_no_price", function()
  t.eq(requesty.parse_model(sonnet({ input_price = false })).pricing, nil)
  local row = sonnet()
  row.output_price = nil
  t.eq(requesty.parse_model(row).pricing, nil)
end)

t.case("zero_limits_are_unknown", function()
  local model = requesty.parse_model(sonnet({ context_window = 0, max_output_tokens = 0 }))
  t.eq(model.context_window, nil)
  t.eq(model.max_output_tokens, nil)
end)

t.case("unreadable_field_costs_only_that_field", function()
  local model = requesty.parse_model(sonnet({ input_price = "0.000003", max_output_tokens = "64000" }))
  t.eq(model.context_window, 200000)
  t.eq(model.max_output_tokens, nil)
  t.eq(model.pricing, nil)
  t.eq(model.supports_thinking, true)
end)

t.case("missing_flags_read_as_unsupported", function()
  local row = sonnet()
  row.supports_reasoning, row.supports_vision = nil, nil
  local model = requesty.parse_model(row)
  t.eq(model.supports_thinking, false)
  t.eq(model.supports_vision, false)
end)

t.case("only_chat_entries_with_ids", function()
  t.eq(requesty.parse_model(sonnet({ api = "embedding" })), nil)
  t.eq(requesty.parse_model(sonnet({ api = "image" })), nil)
  local unlabelled = sonnet()
  unlabelled.api = nil
  assert(requesty.parse_model(unlabelled))
  local anonymous = sonnet()
  anonymous.id = nil
  t.eq(requesty.parse_model(anonymous), nil)
end)

t.case("managed_first_without_repeats", function()
  local merged = requesty.merge(
    { { id = "claude-sonnet-4-5" }, { id = "gpt-5.4-mini" } },
    { { id = SONNET }, { id = "gpt-5.4-mini" }, { id = "openai/gpt-4o-mini" } }
  )
  local ids = {}
  for _, model in ipairs(merged) do
    ids[#ids + 1] = model.id
  end
  t.same(ids, { "claude-sonnet-4-5", "gpt-5.4-mini", SONNET, "openai/gpt-4o-mini" })
end)

local function list(responses)
  local ctx = t.provider_ctx(responses)
  local client = assert(compat.client(ctx, requesty.def, { api_key = "k" }))
  local models, err = requesty.list_models(ctx, client)
  return models, err, ctx.requests
end

t.case("lists_managed_and_full_catalogs", function()
  local models, _, requests = list({
    t.response({ '{"data":[{"id":"managed"}]}' }),
    t.response({ '{"data":[{"id":"vendor/full"}]}' }),
  })
  t.eq(#models, 2)
  t.eq(models[1].id, "managed")
  t.eq(requests[1].url, BASE .. "/models/managed")
  t.eq(requests[2].url, BASE .. "/models")
end)

t.case("one_failed_listing_keeps_the_other", function()
  local models = list({ t.response({ "down" }, { status = 500 }), t.response({ '{"data":[{"id":"vendor/full"}]}' }) })
  t.eq(models[1].id, "vendor/full")
  local both, err = list({ t.response({ "down" }, { status = 500 }), t.response({ "down" }, { status = 502 }) })
  t.eq(both, nil)
  t.same(err, { status = 500, message = "down", retry = "transient" })
end)

t.case("auto_cache_effort_and_attribution", function()
  local body = {}
  local headers = requesty.def.encode(
    body,
    { thinking = { kind = "adaptive" }, model = {} },
    { supports_thinking = true }
  )
  t.same(body, { requesty = { auto_cache = true }, reasoning_effort = "high" })
  t.same(headers, { ["HTTP-Referer"] = "https://maki.sh", ["X-Title"] = "Makima" })
  body = {}
  requesty.def.encode(body, { thinking = { kind = "off" }, model = {} }, { supports_thinking = false })
  t.eq(body.reasoning_effort, nil)
end)

t.report()
