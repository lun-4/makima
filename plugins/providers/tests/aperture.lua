local anthropic = require("providers.anthropic_provider")
local aperture = require("providers.aperture")
local http = require("providers.http")
local t = require("makima.test_helpers")

local HOST = "https://aperture.example.invalid"
local DONE = "data: [DONE]\n\n"
local ROUTED_MODEL = "claude-sonnet-4-5"
local OVERRIDDEN_CONTEXT = 123456

local spec = aperture.provider()

local function chat_sse()
  return t.response({ 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n' .. DONE })
end

local function messages_sse()
  return t.response({
    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}\n\n',
    'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
  })
end

local function gemini_sse()
  return t.response({ 'data: {"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}\n\n' })
end

local function request(id)
  return {
    model = { id = id, max_output_tokens = 16384, context_window = 128000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
  }
end

-- The request a stream of {id} sends, with {config} and `APERTURE_HOST`.
local function sent(id, response, config, env)
  local ctx = t.provider_ctx({ response or chat_sse() })
  t.with_env(env or { APERTURE_HOST = HOST }, function()
    local instance, err = spec.prepare(ctx, { id = id }, config or {})
    assert(instance, err)
    assert(instance.stream(ctx, request(id), t.recorder()))
  end)
  return ctx.requests[1]
end

local function described(id, config, info)
  local info_table = info or {}
  info_table.id = id
  return aperture.describe(info_table, config or {})
end

local function overrides(upstream, fields)
  return { overrides = { [upstream] = fields } }
end

t.case("host_from_env_then_config", function()
  t.with_env({ APERTURE_HOST = " " .. HOST .. "/ " }, function()
    t.eq(aperture.host({ base_url = "https://config.invalid" }), HOST)
  end)
  t.with_env({ APERTURE_HOST = "  " }, function()
    t.eq(aperture.host({ base_url = HOST .. "/" }), HOST)
    t.eq(aperture.host({}), nil)
  end)
end)

t.case("missing_host_is_a_config_error", function()
  t.with_env({}, function()
    local instance, err = spec.prepare(t.provider_ctx({}), { id = "zai/glm-5.2" }, {})
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.eq(err.message, "APERTURE_HOST not set")
  end)
end)

t.case("routes_follow_base_then_upstream", function()
  t.eq(aperture.route("ollama", {}), aperture.ROUTES.ollama)
  t.eq(aperture.route("zai", {}), aperture.ROUTES.zai)
  t.eq(aperture.route("google", {}), aperture.ROUTES.google)
  for _, upstream in ipairs({ "openai", "copilot", "aperture", "gemini", "ikora-openai" }) do
    t.eq(aperture.route(upstream, {}), nil)
  end
  t.eq(aperture.route("gemini", { base = "google" }), aperture.ROUTES.google)
  t.eq(aperture.route("ikora-openai", { base = "llama-cpp" }), aperture.ROUTES["llama-cpp"])
  t.eq(aperture.route("ollama", { base = "not-a-real-provider" }), aperture.ROUTES.ollama)
end)

t.case("model_overrides_win_field_by_field", function()
  local config = overrides("ikora-openai", {
    base = "ollama",
    context_window = 128000,
    max_output_tokens = 8192,
    models = { special = { base = "llama-cpp", context_window = 200000 } },
  })
  local special = aperture.overrides(config, "ikora-openai", "special")
  t.eq(special.base, "llama-cpp")
  t.eq(special.context_window, 200000)
  t.eq(special.max_output_tokens, 8192)
  t.eq(aperture.overrides(config, "ikora-openai", "other").base, "ollama")
  t.same(aperture.overrides({}, "nobody", "x"), {})
end)

t.case("prefix_per_route_and_override", function()
  t.eq(aperture.prefix(aperture.ROUTES.ollama, {}), "/v1")
  t.eq(aperture.prefix(nil, {}), "/v1")
  t.eq(aperture.prefix(aperture.ROUTES.google, {}), "/v1beta")
  t.eq(aperture.prefix(aperture.ROUTES.anthropic, {}), "")
  t.eq(aperture.prefix(aperture.ROUTES.zai, {}), "")
  t.eq(aperture.prefix(aperture.ROUTES.deepseek, {}), "/v1")
  local anthropic_route = aperture.ROUTES.anthropic
  t.eq(aperture.prefix(anthropic_route, { path_prefix = "" }), "")
  t.eq(aperture.prefix(anthropic_route, { path_prefix = "v1" }), "/v1")
  t.eq(aperture.prefix(anthropic_route, { path_prefix = "api/paas/v4" }), "/api/paas/v4")
  t.eq(aperture.prefix(anthropic_route, { path_prefix = "/v1/" }), "/v1")
end)

t.case("anthropic_route_keeps_bare_host_and_vendor_id", function()
  local req = sent("anthropic/" .. ROUTED_MODEL, messages_sse())
  t.eq(req.url, HOST .. "/v1/messages")
  t.eq(req.headers["anthropic-version"], anthropic.API_VERSION)
  t.eq(req.headers["x-api-key"], nil)
  t.eq(makima.json.decode(req.body).model, "anthropic/" .. ROUTED_MODEL)
end)

t.case("google_route_uses_v1beta_and_bare_id", function()
  local req = sent("gemini/gemini-pro-latest", gemini_sse(), overrides("gemini", { base = "google" }))
  t.eq(req.url, HOST .. "/v1beta/models/gemini-pro-latest:streamGenerateContent?alt=sse")
  t.eq(req.headers["x-goog-api-key"], nil)
end)

t.case("compat_routes_keep_vendor_ids_and_native_paths", function()
  local zai = sent("zai/glm-5.2")
  t.eq(zai.url, HOST .. "/chat/completions")
  t.eq(makima.json.decode(zai.body).model, "zai/glm-5.2")
  local deepseek = sent("deepseek/deepseek-chat")
  t.eq(deepseek.url, HOST .. "/v1/chat/completions")
  local ollama = sent("ollama/glm-5.2")
  t.eq(ollama.url, HOST .. "/v1/chat/completions")
  t.eq(makima.json.decode(ollama.body).model, "ollama/glm-5.2")
  t.eq(ollama.headers.Authorization, nil)
end)

t.case("generic_route_streams_chat_completions", function()
  local req = sent("ikora-openai/gemma4")
  t.eq(req.url, HOST .. "/v1/chat/completions")
  local decoded = makima.json.decode(req.body)
  t.eq(decoded.model, "ikora-openai/gemma4")
  t.eq(decoded.max_tokens, 16384)
  t.eq(decoded.stream_options.include_usage, true)
  t.eq(sent("raw-model").url, HOST .. "/v1/chat/completions")
end)

t.case("configured_headers_reach_every_route", function()
  local config = { headers = { ["X-Gateway"] = "${GATEWAY}" } }
  local env = { APERTURE_HOST = HOST, GATEWAY = "gw" }
  t.eq(sent("zai/glm-5.2", nil, config, env).headers["X-Gateway"], "gw")
  t.eq(sent("anthropic/" .. ROUTED_MODEL, messages_sse(), config, env).headers["X-Gateway"], "gw")
end)

t.case("routed_models_take_the_routed_tables", function()
  local native = require("providers.openai_compat").describe(anthropic.def, { id = ROUTED_MODEL })
  local model = described("anthropic/" .. ROUTED_MODEL)
  t.eq(model.context_window, native.context_window)
  t.eq(model.max_output_tokens, native.max_output_tokens)
  t.eq(model.supports_vision, true)
  t.eq(model.supports_thinking, true)
  t.eq(described("ollama/qwen3").supports_thinking, false)
end)

t.case("overrides_win_over_routed_tables", function()
  local model = described(
    "anthropic/" .. ROUTED_MODEL,
    overrides("anthropic", {
      context_window = OVERRIDDEN_CONTEXT,
      supports_vision = false,
      supports_thinking = false,
    })
  )
  t.eq(model.context_window, OVERRIDDEN_CONTEXT)
  t.eq(model.supports_vision, false)
  t.eq(model.supports_thinking, false)
  local qwen = described("ollama/qwen3", overrides("ollama", { models = { qwen3 = { supports_thinking = true } } }))
  t.eq(qwen.supports_thinking, true)
  local gemma = described("ikora-openai/gemma4", overrides("ikora-openai", { base = "llama-cpp" }))
  t.eq(gemma.supports_thinking, true)
end)

t.case("unrouted_models_keep_gateway_defaults_and_pricing", function()
  local pricing = { input = 1, output = 2, cache_read = 0.1, cache_write = 0 }
  local model = described("ikora-openai/gemma4", {}, { pricing = pricing })
  t.same(model, {
    id = "ikora-openai/gemma4",
    context_window = 128000,
    max_output_tokens = 16384,
    supports_thinking = false,
    pricing = pricing,
  })
end)

t.case("discovery_prefixes_upstreams_and_prices_per_million", function()
  local listing = {
    object = "list",
    data = {
      {
        id = "qwen3.6",
        metadata = { provider = { id = "ollama" } },
        pricing = { input = "0.000001", output = 0.000002, input_cache_read = "0.0000001" },
      },
      { id = "gemma4", metadata = { provider = { id = "ikora" } } },
      { id = "raw-model" },
    },
  }
  local ctx = t.provider_ctx({ t.response({ assert(makima.json.encode(listing)) }) })
  local config = overrides("ollama", { models = { ["qwen3.6"] = { context_window = 65536, supports_vision = true } } })
  config.headers = { ["X-Gateway"] = "gw" }
  local models
  t.with_env({ APERTURE_HOST = HOST }, function()
    models = spec.discover(ctx, config)
  end)
  t.eq(ctx.requests[1].url, HOST .. "/v1/models")
  t.eq(ctx.requests[1].headers["X-Gateway"], "gw")
  t.eq(#models, 3)
  t.eq(models[1].id, "ollama/qwen3.6")
  t.eq(models[1].context_window, 65536)
  t.eq(models[1].supports_vision, true)
  t.eq(models[1].supports_thinking, false)
  t.eq(models[1].max_output_tokens, nil)
  assert(math.abs(models[1].pricing.input - 1) < 1e-9)
  assert(math.abs(models[1].pricing.output - 2) < 1e-9)
  assert(math.abs(models[1].pricing.cache_read - 0.1) < 1e-9)
  t.eq(models[2].id, "ikora/gemma4")
  t.eq(models[2].pricing, nil)
  t.eq(models[3].id, "raw-model")
end)

t.case("discovery_without_host_lists_nothing", function()
  t.with_env({}, function()
    t.same(spec.discover(t.provider_ctx({}), {}), {})
  end)
end)

t.case("auth_status_reads_host", function()
  t.with_env({ APERTURE_HOST = HOST }, function()
    t.eq(spec.auth_status(t.provider_ctx({}), {}).state, "env")
  end)
  t.with_env({}, function()
    t.eq(spec.auth_status(t.provider_ctx({}), { base_url = HOST }).state, "configured")
    t.eq(spec.auth_status(t.provider_ctx({}), {}).state, "missing")
  end)
end)

t.report()
