local openrouter = require("providers.openrouter")
local t = require("maki.test_helpers")

local KIMI = "moonshotai/kimi-k3"

local function kimi(overrides)
  local row = {
    id = KIMI,
    context_length = 1048576,
    architecture = { input_modalities = { "text", "image" }, output_modalities = { "text" } },
    pricing = { prompt = "0.000003", completion = "0.000015", input_cache_read = "0.0000003" },
    supported_parameters = { "reasoning" },
  }
  for key, value in pairs(overrides or {}) do
    row[key] = value
  end
  return row
end

local function near(actual, expected)
  assert(math.abs(actual - expected) < 1e-9, tostring(actual) .. " ~= " .. tostring(expected))
end

t.case("pricing_scales_to_per_million", function()
  local model = openrouter.parse_model(kimi())
  t.eq(model.id, KIMI)
  t.eq(model.context_window, 1048576)
  t.eq(model.supports_vision, true)
  t.eq(model.supports_thinking, true)
  near(model.pricing.input, 3)
  near(model.pricing.output, 15)
  near(model.pricing.cache_read, 0.3)
  t.eq(model.pricing.cache_write, 0)
  local priced = kimi()
  priced.pricing.input_cache_write = "0.00000375"
  near(openrouter.parse_model(priced).pricing.cache_write, 3.75)
end)

t.case("reasoning_efforts_skip_unknown_and_sort", function()
  local model = openrouter.parse_model(kimi({
    reasoning = { mandatory = false, default_enabled = true, supported_efforts = { "high", "bogus", "low", "none" } },
  }))
  t.same(model.provider_data, { mandatory = false, default_enabled = true, efforts = { "low", "high" } })
end)

t.case("non_text_models_are_skipped", function()
  for _, modalities in ipairs({ { { "image" }, { "image" } }, { { "image" }, { "text" } }, { { "text" }, { "image" } } }) do
    local row = kimi({ architecture = { input_modalities = modalities[1], output_modalities = modalities[2] } })
    t.eq(openrouter.parse_model(row), nil)
  end
end)

local function effort(reasoning, thinking)
  local body = {}
  openrouter.def.encode(
    body,
    { thinking = thinking, model = {} },
    { supports_thinking = true, provider_data = reasoning }
  )
  return body.reasoning and body.reasoning.effort
end

t.case("declared_efforts_are_snapped_once", function()
  local declared = { mandatory = false, default_enabled = false, efforts = { "high", "xhigh" } }
  t.eq(effort(declared, { kind = "effort", level = "xhigh" }), "xhigh")
  t.eq(effort(declared, { kind = "effort", level = "max" }), "xhigh")
  t.eq(effort({ efforts = { "minimal", "low" } }, { kind = "adaptive" }), "low")
  t.eq(effort({ efforts = {} }, { kind = "effort", level = "xhigh" }), "high")
  t.eq(effort(nil, { kind = "adaptive" }), "high")
end)

t.case("off_follows_reasoning_flags", function()
  t.eq(effort({ efforts = {}, default_enabled = false, mandatory = false }, { kind = "off" }), nil)
  t.eq(effort({ efforts = {}, default_enabled = true, mandatory = false }, { kind = "off" }), "none")
  t.eq(effort({ efforts = {}, default_enabled = true, mandatory = true }, { kind = "off" }), nil)
end)

t.case("cache_session_and_attribution", function()
  local body = {}
  local headers = openrouter.def.encode(
    body,
    { thinking = { kind = "off" }, model = {}, session_id = "s1" },
    { supports_thinking = false }
  )
  t.same(body, { cache_control = { type = "ephemeral" }, session_id = "s1" })
  t.same(headers, { ["HTTP-Referer"] = "https://makima.ln4.net", ["X-OpenRouter-Title"] = "Makima" })
end)

t.report()
