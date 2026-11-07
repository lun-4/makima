local anthropic = require("providers.anthropic_provider")
local http = require("providers.http")
local t = require("maki.test_helpers")

local KEY_ENV = "ANTHROPIC_API_KEY"
local ORIGIN = "https://api.anthropic.com"
local PROXY = "https://proxy.example.invalid"
local FAST_BETA = "fast-mode-2026-02-01"
local LONG_CONTEXT_BETA = "context-1m-2025-08-07"
local OAUTH_BETA = "oauth-2025-04-20"
local RESET_MS = 1770415200000
local WEEK_RESET_MS = 1770595200000

local spec = anthropic.provider()

local function sse_text(text)
  local events = {
    { type = "message_start", message = { usage = { input_tokens = 3 } } },
    { type = "content_block_start", index = 0, content_block = { type = "text", text = "" } },
    { type = "content_block_delta", index = 0, delta = { type = "text_delta", text = text } },
    { type = "message_delta", delta = { stop_reason = "end_turn" }, usage = { output_tokens = 1 } },
    { type = "message_stop" },
  }
  local frames = {}
  for _, event in ipairs(events) do
    frames[#frames + 1] = "event: " .. event.type .. "\ndata: " .. assert(maki.json.encode(event)) .. "\n\n"
  end
  return t.response({ table.concat(frames) })
end

local function json(value)
  return t.response({ assert(maki.json.encode(value)) })
end

local function request(id, extra)
  local result = {
    model = {
      id = id,
      output_tokens = 8000,
      max_output_tokens = 64000,
      context_window = 200000,
    },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
    fast = false,
  }
  for key, value in pairs(extra or {}) do
    result[key] = value
  end
  return result
end

local function header(headers, name)
  for key, value in pairs(headers) do
    if key:lower() == name:lower() then
      return value
    end
  end
end

local function prepared(env, id, config, responses, saved)
  local ctx = t.provider_ctx(responses or {}, saved)
  local instance, model
  t.with_env(env, function()
    instance, model = spec.prepare(ctx, { id = id }, config or {})
  end)
  return ctx, assert(instance, model), model
end

local function stream(env, id, extra, config)
  local ctx, instance = prepared(env, id, config, { sse_text("ok") })
  local emit = t.recorder()
  local result
  t.with_env(env, function()
    result = assert(instance.stream(ctx, request(id, extra), emit))
  end)
  t.eq(result.message.content[1].text, "ok")
  return ctx.requests[1], instance
end

t.case("origin_normalizes_base_urls", function()
  for input, expected in pairs({
    ["https://api.anthropic.com/v1/messages"] = ORIGIN,
    ["https://proxy.example.com/v1/messages/"] = "https://proxy.example.com",
    [ORIGIN] = ORIGIN,
    ["http://localhost:8080/"] = "http://localhost:8080",
    ["https://api.anthropic.com/v1/messages?beta=true"] = ORIGIN,
  }) do
    t.eq(anthropic.origin(input), expected, input)
  end
end)

t.case("stream_sends_key_version_and_body", function()
  local sent = stream({ [KEY_ENV] = "sk-1" }, "claude-sonnet-4-5")
  t.eq(sent.url, ORIGIN .. "/v1/messages")
  t.eq(sent.method, "POST")
  t.eq(header(sent.headers, "x-api-key"), "sk-1")
  t.eq(header(sent.headers, "anthropic-version"), "2023-06-01")
  t.eq(header(sent.headers, "anthropic-beta"), nil)
  local body = maki.json.decode(sent.body)
  t.eq(body.model, "claude-sonnet-4-5")
  t.eq(body.stream, true)
  t.eq(body.speed, nil)
  t.eq(body.max_tokens, 64000)
end)

t.case("base_url_override_keeps_the_messages_path_once", function()
  local sent = stream({ [KEY_ENV] = "k", ANTHROPIC_BASE_URL = PROXY .. "/v1/messages" }, "claude-sonnet-4-5")
  t.eq(sent.url, PROXY .. "/v1/messages")
  sent = stream({ [KEY_ENV] = "k" }, "claude-sonnet-4-5", nil, { base_url = PROXY })
  t.eq(sent.url, PROXY .. "/v1/messages")
end)

t.case("saved_key_and_configured_headers", function()
  local ctx, instance = prepared({ TOKEN = "oauth" }, "claude-sonnet-4-5", {
    headers = { Authorization = "Bearer ${TOKEN}", ["X-Api-Key"] = "gateway" },
  }, { sse_text("ok") }, { api_key = "saved" })
  t.with_env({ TOKEN = "oauth" }, function()
    assert(instance.stream(ctx, request("claude-sonnet-4-5"), t.recorder()))
  end)
  local headers = ctx.requests[1].headers
  t.eq(header(headers, "x-api-key"), "gateway")
  t.eq(header(headers, "authorization"), "Bearer oauth")
end)

t.case("missing_key_fails_prepare_with_a_config_error", function()
  t.with_env({}, function()
    local instance, err = spec.prepare(t.provider_ctx({}), { id = "claude-sonnet-4-5" }, {})
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, KEY_ENV .. " not set")
  end)
end)

t.case("unset_header_variable_fails_prepare", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    local instance, err = spec.prepare(t.provider_ctx({}), { id = "claude-sonnet-4-5" }, {
      headers = { Authorization = "Bearer ${MISSING}" },
    })
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, "'MISSING'")
  end)
end)

t.case("keys_rotate_and_reload", function()
  local env = { [KEY_ENV] = "a, b" }
  local ctx, instance = prepared(env, "claude-sonnet-4-5", {}, { sse_text("1"), sse_text("2"), sse_text("3") })
  t.eq(instance.key_count(), 2)
  t.with_env(env, function()
    assert(instance.rotate_key())
    assert(instance.stream(ctx, request("claude-sonnet-4-5"), t.recorder()))
  end)
  t.eq(ctx.requests[1].headers["x-api-key"], "b")
  t.with_env({ [KEY_ENV] = "c" }, function()
    t.eq(instance.reload_auth(ctx), nil)
    t.eq(instance.key_count(), 1)
    assert(instance.stream(ctx, request("claude-sonnet-4-5"), t.recorder()))
  end)
  t.eq(ctx.requests[2].headers["x-api-key"], "c")
end)

t.case("fast_mode_only_on_capable_models", function()
  local sent = stream({ [KEY_ENV] = "k" }, "claude-opus-4-8", { fast = true })
  t.eq(maki.json.decode(sent.body).speed, "fast")
  t.eq(header(sent.headers, "anthropic-beta"), FAST_BETA)
  sent = stream({ [KEY_ENV] = "k" }, "claude-sonnet-4-5", { fast = true })
  t.eq(maki.json.decode(sent.body).speed, nil)
  t.eq(header(sent.headers, "anthropic-beta"), nil)
end)

t.case("long_context_strips_the_suffix_and_adds_the_beta", function()
  local sent = stream({ [KEY_ENV] = "k" }, "claude-opus-4-8-1m", { fast = true })
  t.eq(maki.json.decode(sent.body).model, "claude-opus-4-8")
  t.eq(header(sent.headers, "anthropic-beta"), FAST_BETA .. "," .. LONG_CONTEXT_BETA)
end)

t.case("prepare_describes_snapshots_long_context_and_unknown_models", function()
  local _, _, snapshot = prepared({ [KEY_ENV] = "k" }, "claude-sonnet-4-5-20250929")
  t.eq(snapshot.max_output_tokens, 64000)
  t.eq(snapshot.pricing.input, 3.00)
  t.eq(snapshot.tier, "medium")
  local _, _, long = prepared({ [KEY_ENV] = "k" }, "claude-opus-4-8-1m")
  t.eq(long.context_window, anthropic.LONG_CONTEXT_WINDOW)
  t.eq(long.supports_fast, true)
  t.eq(long.pricing.fast.output, 50.00)
  local _, _, unknown = prepared({ [KEY_ENV] = "k" }, "claude-99-turbo")
  t.same(unknown, {
    id = "claude-99-turbo",
    context_window = 200000,
    max_output_tokens = 128000,
    supports_thinking = true,
    supports_vision = true,
    supports_tool_examples = true,
    supports_fast = false,
  })
end)

t.case("curated_tier_defaults_come_first", function()
  local first = {}
  for _, model in ipairs(spec.models) do
    first[model.tier] = first[model.tier] or model.id
  end
  t.same(first, { weak = "claude-haiku-4-5", medium = "claude-sonnet-5", strong = "claude-opus-5" })
end)

t.case("discover_pages_adds_long_context_rows_and_keeps_curated", function()
  local ctx = t.provider_ctx({
    json({
      data = { { id = "claude-opus-4-8", max_input_tokens = 1000000 }, { id = "claude-new-9" } },
      has_more = true,
      last_id = "claude-new-9",
    }),
    json({ data = { { id = "claude-opus-4-5-20251101", max_input_tokens = 200000 } }, has_more = false }),
  })
  local models
  t.with_env({ [KEY_ENV] = "k" }, function()
    models = assert(spec.discover(ctx, {}))
  end)
  t.eq(ctx.requests[1].url, ORIGIN .. "/v1/models?limit=1000")
  t.eq(ctx.requests[2].url, ORIGIN .. "/v1/models?limit=1000&after_id=claude-new-9")
  t.eq(ctx.requests[2].headers["x-api-key"], "k")
  local ids = {}
  for index, model in ipairs(models) do
    ids[model.id] = index
  end
  t.eq(ids["claude-haiku-4-5"], 1)
  assert(ids["claude-opus-4-8-1m"] > #spec.models)
  t.eq(models[ids["claude-opus-4-8-1m"]].context_window, anthropic.LONG_CONTEXT_WINDOW)
  t.eq(models[ids["claude-opus-4-5-20251101"]].max_output_tokens, 64000)
  assert(ids["claude-new-9"])
end)

t.case("discover_without_api_access_lists_curated", function()
  for _, env in ipairs({ {}, { CLAUDE_CODE_USE_BEDROCK = "1", [KEY_ENV] = "k" } }) do
    local ctx = t.provider_ctx({})
    t.with_env(env, function()
      t.eq(#assert(spec.discover(ctx, {})), #spec.models)
    end)
    t.eq(#ctx.requests, 0)
  end
end)

t.case("usage_needs_an_authorization_header", function()
  local ctx, instance = prepared({ [KEY_ENV] = "k" }, "claude-sonnet-4-5")
  t.with_env({}, function()
    local usage, err = instance.fetch_usage(ctx)
    t.eq(usage, nil)
    t.eq(err, nil)
  end)
  t.eq(#ctx.requests, 0)

  local env = { [KEY_ENV] = "k", TOKEN = "t" }
  local body = { five_hour = { utilization = 14.0, resets_at = "2026-02-06T22:00:00+00:00" } }
  ctx, instance = prepared(env, "claude-sonnet-4-5", { headers = { authorization = "Bearer ${TOKEN}" } }, {
    json(body),
  })
  local usage
  t.with_env(env, function()
    usage = assert(instance.fetch_usage(ctx))
  end)
  t.eq(ctx.requests[1].url, ORIGIN .. "/api/oauth/usage")
  t.eq(header(ctx.requests[1].headers, "anthropic-beta"), OAUTH_BETA)
  t.same(usage, { limits = { { kind = { Hours = 5 }, percentage = 14, reset_at = RESET_MS } } })
end)

t.case("parse_usage_reads_limits_then_credits", function()
  local usage = anthropic.parse_usage({
    five_hour = { utilization = 14.0, resets_at = "2026-02-06T22:00:00+00:00" },
    limits = {
      { kind = "session", percent = 14, resets_at = "2026-02-06T22:00:00+00:00" },
      { kind = "weekly_all", percent = 2, resets_at = "2026-02-09T00:00:00+00:00" },
      {
        kind = "weekly_scoped",
        percent = 3,
        resets_at = "2026-02-09T00:00:00+00:00",
        scope = { model = { display_name = "Fable" } },
      },
    },
    extra_usage = { is_enabled = true, used_credits = 233.0, utilization = 1.55 },
    spend = { used = { amount_minor = 233, currency = "USD", exponent = 2 }, percent = 2, enabled = true },
  })
  t.same(usage, {
    limits = {
      { kind = { Hours = 5 }, percentage = 14, reset_at = RESET_MS },
      { kind = { Weekly = {} }, percentage = 2, reset_at = WEEK_RESET_MS },
      { kind = { Weekly = { model = "Fable" } }, percentage = 3, reset_at = WEEK_RESET_MS },
      { kind = "Credits", percentage = 2, detail = "$2.33 spent" },
    },
  })
end)

t.case("parse_usage_falls_back_to_windows", function()
  local usage = anthropic.parse_usage({
    five_hour = { utilization = 35.4, resets_at = "2026-02-06T22:00:00+00:00" },
    seven_day = { utilization = 14.0 },
    seven_day_sonnet = { utilization = 39.0 },
    seven_day_opus = { utilization = 2.6 },
    extra_usage = { is_enabled = true, used_credits = 233.0, utilization = 2.0, currency = "EUR" },
  })
  t.same(usage, {
    limits = {
      { kind = { Hours = 5 }, percentage = 35, reset_at = RESET_MS },
      { kind = { Weekly = {} }, percentage = 14 },
      { kind = { Weekly = { model = "Sonnet" } }, percentage = 39 },
      { kind = { Weekly = { model = "Opus" } }, percentage = 3 },
      { kind = "Credits", percentage = 2, detail = "2.33 EUR spent" },
    },
  })
end)

t.case("parse_usage_skips_unknown_values", function()
  local usage = anthropic.parse_usage({
    five_hour = { utilization = 5.0, resets_at = "not a timestamp" },
    extra_usage = { is_enabled = true },
  })
  t.same(usage, { limits = { { kind = { Hours = 5 }, percentage = 5 } } })
end)

t.case("bedrock_mode_owns_auth_status", function()
  local status
  t.with_env({ [KEY_ENV] = "k", CLAUDE_CODE_USE_BEDROCK = "1" }, function()
    status = spec.auth_status(t.provider_ctx({}), {})
  end)
  t.eq(status.state, "missing")
  t.with_env({ [KEY_ENV] = "k" }, function()
    status = spec.auth_status(t.provider_ctx({}), {})
  end)
  t.eq(status.state, "env")
end)

t.report()
