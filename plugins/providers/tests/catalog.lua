local catalog = require("providers.catalog")
local http = require("providers.http")
local t = require("maki.test_helpers")

local CHAT_API = "https://chat.example.invalid/v1"
local MESSAGES_API = "https://messages.example.invalid/v1"
local DONE = "data: [DONE]\n\n"
local HEADER_ERROR = "provider 'chatty' header 'X-Gateway': environment variable 'GATEWAY' is unset or empty"

local function index()
  return {
    chatty = {
      name = "Chatty",
      env = { "CHATTY_KEY", "CHATTY_ALT_KEY" },
      npm = "@ai-sdk/openai-compatible",
      api = CHAT_API,
      unknown_field = { kept = true },
      models = {
        ["vendor/big"] = {
          limit = { context = 1000000, output = 32000 },
          cost = { input = 1.5, output = 6, cache_read = 0.1 },
          reasoning = true,
          modalities = { input = { "text", "image" }, output = { "text" } },
        },
        small = { limit = { context = 0 }, attachment = true },
      },
    },
    claudish = {
      name = "Claudish",
      env = { "CLAUDISH_KEY" },
      npm = "@ai-sdk/anthropic",
      api = MESSAGES_API,
      models = { sonnet = { limit = { context = 200000, output = 64000 } } },
    },
    ["zai-coding-plan"] = { name = "Plan", npm = "@ai-sdk/openai-compatible", api = CHAT_API, models = {} },
    ["regolo-ai"] = { name = "Regolo", npm = "@ai-sdk/openai-compatible", api = CHAT_API, models = {} },
    unsupported = { name = "Other", npm = "@ai-sdk/openai", api = CHAT_API, models = {} },
    nowhere = { name = "Nowhere", npm = "@ai-sdk/openai-compatible", models = {} },
    taken = { name = "Taken", npm = "@ai-sdk/openai-compatible", api = CHAT_API, models = {} },
  }
end

local function body(value)
  return assert(maki.json.encode(value))
end

local function seeded(dir)
  local cache = catalog.new(dir)
  cache.index, cache.mtime = index(), maki.time.now().secs
  return cache
end

local function request(id, thinking)
  return {
    model = { id = id, max_output_tokens = 32000, context_window = 1000000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = thinking or { kind = "adaptive" },
  }
end

local function chat_sse()
  return t.response({ 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n' .. DONE })
end

local function messages_sse()
  return t.response({
    'event: message_start\ndata: {"type":"message_start","message":{"usage":{"input_tokens":1}}}\n\n',
    'event: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\n',
    'event: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}\n\n',
    'event: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
  })
end

local function sent(cache, id, model_id, env, config, response)
  local spec = catalog.provider(cache, id)
  local ctx = t.provider_ctx({ response })
  t.with_env(env, function()
    local instance, descriptor = spec.prepare(ctx, { id = model_id }, config or {})
    assert(instance, descriptor)
    assert(instance.stream(ctx, request(model_id), t.recorder()))
  end)
  return ctx.requests[1]
end

t.case("servable_skips_unsupported_blocked_renamed_and_taken", function()
  t.same(seeded():servable({ taken = true }), { "chatty", "claudish" })
end)

t.case("descriptor_reads_limits_pricing_and_capabilities", function()
  local cache = seeded()
  local entry = cache:entry("chatty")
  t.same(catalog.descriptor("vendor/big", entry.models["vendor/big"]), {
    id = "vendor/big",
    context_window = 1000000,
    max_output_tokens = 32000,
    pricing = { input = 1.5, output = 6, cache_write = 0, cache_read = 0.1 },
    supports_thinking = true,
    supports_vision = true,
  })
  t.same(catalog.descriptor("small", entry.models.small), {
    id = "small",
    context_window = 128000,
    max_output_tokens = 64000,
    pricing = { input = 0, output = 0, cache_write = 0, cache_read = 0 },
    supports_vision = true,
  })
  t.eq(catalog.is_free(entry.models.small), true)
  t.eq(catalog.is_free(entry.models["vendor/big"]), false)
end)

t.case("refresh_skips_fresh_cache_and_fetches_stale", function()
  local cache = seeded()
  local ctx = t.provider_ctx({ t.response({ body({ fresh = { name = "Fresh" } }) }) })
  t.eq(cache:refresh(ctx).chatty.name, "Chatty")
  t.eq(#ctx.requests, 0)
  cache.mtime = maki.time.now().secs - catalog.TTL_SECS - 1
  t.eq(cache:refresh(ctx).fresh.name, "Fresh")
  t.eq(ctx.requests[1].url, catalog.URL)
end)

t.case("refresh_splits_the_catalog_into_index_and_providers", function()
  local root = t.mktmpdir("catalog")
  local dir = maki.fs.joinpath(root, catalog.CACHE_DIR)
  seeded(dir):refresh(t.provider_ctx({ t.response({ body(index()) }) }), true)
  local reloaded = catalog.new(dir)
  local loaded = reloaded:load()
  t.eq(loaded.chatty.api, CHAT_API)
  t.eq(loaded.chatty.models, nil)
  t.eq(reloaded:model("chatty", "small").attachment, true)
  local file = maki.json.decode(maki.fs.read(maki.fs.joinpath(dir, "chatty.json")))
  t.eq(file.unknown_field.kept, true)
  t.rmtree(root)
end)

t.case("failed_refresh_keeps_the_catalog_and_reports", function()
  local cache = seeded()
  local index_before = cache.index
  local result, err = cache:refresh(t.provider_ctx({ t.response({ "down" }, { status = 503 }) }), true)
  t.eq(result, index_before)
  t.eq(err.status, 503)
end)

t.case("refresh_after_a_concurrent_attempt_reuses_it", function()
  local cache = seeded()
  cache.attempted = math.huge
  local ctx = t.provider_ctx({})
  cache:refresh(ctx, true)
  t.eq(#ctx.requests, 0)
end)

t.case("load_without_a_file_has_no_catalog", function()
  t.eq(catalog.new(nil):load(), nil)
  t.eq(catalog.new("/nonexistent/maki/" .. catalog.CACHE_DIR):load(), nil)
end)

t.case("discovery_hides_models_without_a_key", function()
  local spec = catalog.provider(seeded(), "chatty")
  local models
  t.with_env({}, function()
    models = spec.discover(t.provider_ctx({}), {})
  end)
  t.same(models, {})
  t.with_env({ CHATTY_ALT_KEY = "k" }, function()
    models = spec.discover(t.provider_ctx({}), {})
  end)
  t.eq(#models, 2)
  t.eq(models[1].id, "small")
  t.eq(models[2].id, "vendor/big")
  models = spec.discover(t.provider_ctx({}, { api_key = "saved" }), {})
  t.eq(#models, 2)
end)

t.case("spec_lists_under_the_catalog_section_with_key_login", function()
  local spec = catalog.provider(seeded(), "claudish")
  t.eq(spec.display_name, "Claudish")
  t.eq(spec.section, catalog.SECTION)
  t.eq(spec.auth[1].id, "api_key")
end)

t.case("chat_request_uses_bearer_key_and_prefers_high_effort", function()
  local req = sent(seeded(), "chatty", "vendor/big", { CHATTY_KEY = "k1,k2" }, {}, chat_sse())
  t.eq(req.url, CHAT_API .. "/chat/completions")
  t.eq(req.headers.Authorization, "Bearer k1")
  local decoded = maki.json.decode(req.body)
  t.eq(decoded.model, "vendor/big")
  t.eq(decoded.max_tokens, 32000)
  t.eq(decoded.reasoning_effort, "high")
  t.eq(decoded.stream_options.include_usage, true)
end)

t.case("messages_request_uses_api_key_header", function()
  local req = sent(seeded(), "claudish", "sonnet", { CLAUDISH_KEY = "c" }, {}, messages_sse())
  t.eq(req.url, MESSAGES_API .. "/messages")
  t.eq(req.headers["x-api-key"], "c")
  t.eq(req.headers.Authorization, nil)
  t.eq(req.headers["anthropic-version"], "2023-06-01")
  local decoded = maki.json.decode(req.body)
  t.eq(decoded.model, "sonnet")
  t.eq(decoded.stream, true)
end)

t.case("configured_headers_expand_and_replace", function()
  local config = { headers = { Authorization = "Bearer ${GATEWAY}" } }
  local req = sent(seeded(), "chatty", "small", { CHATTY_KEY = "k", GATEWAY = "gw" }, config, chat_sse())
  t.eq(req.headers.Authorization, "Bearer gw")
  local spec = catalog.provider(seeded(), "chatty")
  t.with_env({ CHATTY_KEY = "k" }, function()
    local instance, err = spec.prepare(
      t.provider_ctx({}),
      { id = "small" },
      { headers = { ["X-Gateway"] = "${GATEWAY}" } }
    )
    t.eq(instance, nil)
    t.eq(err.message, HEADER_ERROR)
  end)
end)

t.case("prepare_errors_are_config_errors", function()
  local spec = catalog.provider(seeded(), "chatty")
  t.with_env({}, function()
    local _, err = spec.prepare(t.provider_ctx({}), { id = "small" }, {})
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, "CHATTY_KEY not set")
  end)
  t.with_env({ CHATTY_KEY = "k" }, function()
    local _, err = spec.prepare(t.provider_ctx({}), { id = "missing" }, {})
    t.eq(err.kind, http.CONFIG_KIND)
    t.eq(err.message, "model 'missing' not found from provider 'chatty'")
  end)
end)

t.case("auth_status_names_the_variable_in_use", function()
  local spec = catalog.provider(seeded(), "chatty")
  t.with_env({ CHATTY_ALT_KEY = "k" }, function()
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "env", detail = "via CHATTY_ALT_KEY" })
  end)
  t.with_env({}, function()
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "missing" })
  end)
end)

t.report()
