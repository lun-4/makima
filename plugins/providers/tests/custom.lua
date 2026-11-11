local custom = require("providers.custom")
local http = require("providers.http")
local t = require("makima.test_helpers")

local ID = "my-proxy"
local KEY_ENV = "MY_PROXY_API_KEY"
local BASE = "https://llm.example.invalid/v1"
local DONE = "data: [DONE]\n\n"
local CREATED_NOTE =
  "Added my-proxy. Restart makima, then set MY_PROXY_API_KEY or run `makima auth login my-proxy`; use it as my-proxy/<model>."

local function section(overrides)
  local result = {
    protocol = "openai",
    base_url = BASE,
    models = {
      {
        id = "fast-v1",
        tier = "weak",
        context_window = 64000,
        pricing_input = 0.5,
        pricing_output = 1.5,
        pricing_fast_input = 1,
      },
      { id = "smart-v1", tier = "strong", supports_thinking = false },
    },
  }
  for key, value in pairs(overrides or {}) do
    result[key] = value
  end
  return result
end

local function request(id, thinking)
  return {
    model = { id = id, max_output_tokens = 16384, context_window = 128000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = thinking or { kind = "off" },
  }
end

local function chat_sse()
  return t.response({ 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n' .. DONE })
end

-- The request one stream of {id} sends through a provider for {config}.
local function sent(config, id, response, env, thinking)
  local ctx = t.provider_ctx({ response or chat_sse() })
  t.with_env(env or { [KEY_ENV] = "k" }, function()
    local spec = custom.provider(ID, config)
    local instance, err = spec.prepare(ctx, { id = id }, config)
    assert(instance, http.message(err))
    assert(instance.stream(ctx, request(id, thinking), t.recorder()))
  end)
  return ctx.requests[1]
end

t.case("declared_models_register_with_tiers_and_pricing", function()
  local models = custom.provider(ID, section()).models
  t.same(models[1], {
    id = "fast-v1",
    tier = "weak",
    context_window = 64000,
    pricing = { input = 0.5, output = 1.5, cache_write = 0, cache_read = 0, fast = { input = 1, output = 0 } },
  })
  t.eq(models[2].tier, "strong")
  t.eq(models[2].pricing, nil)
  t.eq(custom.declared({ id = "x" }).tier, "medium")
end)

t.case("declared_fields_win_over_discovered_and_defaults", function()
  local protocol = custom.PROTOCOLS.openai
  local model = custom.describe(protocol, section(), { id = "fast-v1", context_window = 1, max_output_tokens = 2048 })
  t.eq(model.context_window, 64000)
  t.eq(model.max_output_tokens, 2048)
  t.eq(model.supports_thinking, true)
  t.eq(custom.describe(protocol, section(), { id = "smart-v1" }).supports_thinking, false)
  local unknown = custom.describe(protocol, section(), { id = "fast-v1-preview" })
  t.eq(unknown.context_window, 200000)
  t.eq(unknown.max_output_tokens, 100000)
  t.eq(unknown.tier, nil)
end)

t.case("openai_protocol_streams_chat_with_a_bearer_key", function()
  local req = sent(section(), "fast-v1")
  t.eq(req.url, BASE .. "/chat/completions")
  t.eq(req.headers.Authorization, "Bearer k")
  local body = makima.json.decode(req.body)
  t.eq(body.max_tokens, 16384)
  t.eq(body.thinking.type, "disabled")
  t.eq(body.reasoning_effort, "none")
  body = makima.json.decode(sent(section(), "fast-v1", nil, nil, { kind = "effort", level = "high" }).body)
  t.eq(body.reasoning_effort, "high")
  t.eq(body.thinking, nil)
end)

t.case("key_env_base_url_override_and_headers", function()
  local config = section({ api_key_env = "OTHER_KEY", headers = { ["X-Gateway"] = "${GATEWAY}" } })
  local req = sent(config, "fast-v1", nil, {
    OTHER_KEY = "other",
    GATEWAY = "gw",
    MY_PROXY_BASE_URL = "https://env.example.invalid/v1",
  })
  t.eq(req.url, "https://env.example.invalid/v1/chat/completions")
  t.eq(req.headers.Authorization, "Bearer other")
  t.eq(req.headers["X-Gateway"], "gw")
end)

t.case("missing_key_is_a_config_error", function()
  t.with_env({}, function()
    local instance, err = custom.provider(ID, section()).prepare(t.provider_ctx({}), { id = "fast-v1" }, section())
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, KEY_ENV)
  end)
end)

t.case("responses_protocol_streams_responses", function()
  local response = t.response({
    'data: {"type":"response.output_text.delta","delta":"ok"}\n\n',
    'data: {"type":"response.completed","response":{"usage":{"input_tokens":1,"output_tokens":1}}}\n\n',
  })
  local req = sent(section({ protocol = "openai-responses" }), "fast-v1", response)
  t.eq(req.url, BASE .. "/responses")
end)

t.case("anthropic_protocol_streams_messages_from_the_origin", function()
  local response = t.response({
    'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
  })
  local req =
    sent(section({ protocol = "anthropic", base_url = "https://gw.example.invalid/v1/messages" }), "c", response)
  t.eq(req.url, "https://gw.example.invalid/v1/messages")
  t.eq(req.headers.Authorization, "Bearer k")
  t.eq(req.headers["anthropic-version"], "2023-06-01")
  t.eq(req.headers["x-api-key"], nil)
end)

t.case("google_protocol_streams_generate_content", function()
  local response =
    t.response({ 'data: {"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}\n\n' })
  local req = sent(section({ protocol = "google", base_url = "https://gw.example.invalid/v1beta" }), "g-1", response)
  t.eq(req.url, "https://gw.example.invalid/v1beta/models/g-1:streamGenerateContent?alt=sse")
  t.eq(req.headers.Authorization, "Bearer k")
end)

t.case("discovery_off_never_sends", function()
  local ctx = t.provider_ctx({})
  t.with_env({ [KEY_ENV] = "k" }, function()
    local models = custom.provider(ID, section()).discover(ctx, section())
    t.eq(#models, 2)
  end)
  t.eq(#ctx.requests, 0)
end)

t.case("discovery_merges_declared_over_listed", function()
  local config = section({ discover_models = true })
  local listing =
    '{"data":[{"id":"extra","context_length":32000},{"id":"fast-v1","context_length":1,"max_tokens":4096}]}'
  local ctx = t.provider_ctx({ t.response({ listing }) })
  local models
  t.with_env({ [KEY_ENV] = "k" }, function()
    models = assert(custom.provider(ID, config).discover(ctx, config))
  end)
  t.eq(ctx.requests[1].url, BASE .. "/models")
  t.eq(models[1].id, "fast-v1")
  t.eq(models[1].tier, "weak")
  t.eq(models[1].context_window, 64000)
  t.eq(models[1].max_output_tokens, 4096)
  t.eq(models[2].id, "smart-v1")
  t.eq(models[3].id, "extra")
  t.eq(models[3].tier, nil)
  t.eq(models[3].context_window, 32000)
end)

t.case("creator_saves_a_section", function()
  local saved = {}
  local save = makima.api.save_provider_config
  makima.api.save_provider_config = function(id, value)
    saved[id] = value
    return true
  end
  local answers = { "My Proxy!", "anthropic", " https://gw.example.invalid " }
  local notes = {}
  local ui = {
    input = function()
      return table.remove(answers, 1)
    end,
    choice = function(_, _, options)
      t.eq(#options, 3)
      return table.remove(answers, 1)
    end,
    instructions = function(_, text)
      notes[#notes + 1] = text
      return true
    end,
  }
  local creator = custom.creator("")
  t.eq(creator.auth_status().state, "missing")
  local ok = creator.auth[1].login(t.provider_ctx({}), ui)
  makima.api.save_provider_config = save
  t.eq(ok, true)
  t.same(saved[ID], {
    display_name = "Custom (my-proxy)",
    protocol = "anthropic",
    base_url = "https://gw.example.invalid",
    api_key_env = KEY_ENV,
    discover_models = true,
  })
  t.same(notes, { CREATED_NOTE })
  t.eq(creator.auth_status().state, "configured")
end)

t.case("creator_rejects_an_empty_name", function()
  local ui = {
    input = function()
      return " ... "
    end,
  }
  local ok, err = custom.creator("").auth[1].login(t.provider_ctx({}), ui)
  t.eq(ok, nil)
  t.eq(err.message, custom.EMPTY_NAME)
end)

t.case("slugify", function()
  t.eq(custom.slugify("My Cool Provider"), "my-cool-provider")
  t.eq(custom.slugify("  My__Provider--"), "my-provider")
  t.eq(custom.slugify("My.Cool@Provider!"), "my-cool-provider")
end)

t.report()
