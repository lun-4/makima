local catalog = require("providers.catalog")
local http = require("providers.http")
local opencode = require("providers.opencode")
local t = require("maki.test_helpers")

local ZEN_API = "https://opencode.example.invalid/zen/v1"
local GO_API = "https://opencode.example.invalid/go/v1"
local NVIDIA_API = "https://nvidia.example.invalid/v1"
local SESSION = "session-123"
local DONE = "data: [DONE]\n\n"
local SAMPLE_USAGE =
  '{"usage":{"rolling":{"status":"ok","percent":1,"resetsAt":"2026-08-29T06:01:16.964Z"},"weekly":{"status":"ok","percent":4,"resetsAt":"2026-08-31T00:00:00.964Z"},"monthly":{"status":"ok","percent":2,"resetsAt":"2026-09-18T15:23:06.964Z"}}}'
local ROLLING_RESET_MS = 1787983276964
local WEEKLY_RESET_MS = 1788134400964
local MONTHLY_RESET_MS = 1789744986964
local GATED_ERROR =
  "provider 'opencode-go' has no API key; run `makima auth login opencode-go` or set providers.opencode.enable_free_models = true to use its free models"

-- What `[opencode]` holds, which Go reads `enable_free_models` from.
local zen_section = {}
maki.api.provider_config = function(id)
  return id == opencode.ZEN.id and zen_section or nil
end

local function models()
  return {
    paid = { cost = { input = 3, output = 15 }, limit = { context = 400000, output = 128000 } },
    ["free-one"] = { cost = { input = 0, output = 0 } },
    unpriced = {},
  }
end

local function cache()
  local result = catalog.new(nil)
  result.mtime = maki.time.now().secs
  result.index = {
    opencode = {
      name = "OpenCode",
      env = { "OPENCODE_API_KEY" },
      npm = "@ai-sdk/openai-compatible",
      api = ZEN_API,
      models = models(),
    },
    ["opencode-go"] = {
      name = "OpenCode Go",
      env = { "OPENCODE_API_KEY" },
      npm = "@ai-sdk/openai-compatible",
      api = GO_API,
      models = models(),
    },
    nvidia = {
      name = "NVIDIA",
      env = { "NVIDIA_API_KEY" },
      npm = "@ai-sdk/openai-compatible",
      api = NVIDIA_API,
      models = { ["openai/gpt-oss-120b"] = {} },
    },
  }
  return result
end

local function ids(list)
  local out = {}
  for _, model in ipairs(list) do
    out[#out + 1] = model.id
  end
  return out
end

local function discover(member, env, config, saved)
  local spec = opencode.provider(cache(), member)
  local found
  t.with_env(env, function()
    found = spec.discover(t.provider_ctx({}, saved), config or {})
  end)
  return found
end

local function request(id, session_id)
  return {
    model = { id = id, max_output_tokens = 128000, context_window = 400000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
    session_id = session_id,
  }
end

local function sse()
  return t.response({ 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n' .. DONE })
end

local function prepare(member, id, env, config, responses, saved)
  local ctx = t.provider_ctx(responses or { sse() }, saved)
  local instance, model
  t.with_env(env, function()
    instance, model = opencode.provider(cache(), member).prepare(ctx, { id = id }, config or {})
  end)
  return ctx, instance, model
end

local function streamed(member, id, env, config, session_id)
  local ctx, instance = prepare(member, id, env, config)
  local result, err
  t.with_env(env, function()
    result, err = instance.stream(ctx, request(id, session_id), t.recorder())
  end)
  return ctx.requests[1], result, err
end

t.case("free_models_hidden_unless_enabled", function()
  t.same(ids(discover(opencode.ZEN, { OPENCODE_API_KEY = "k" })), { "paid" })
  t.same(ids(discover(opencode.GO, {}, nil, { api_key = "saved" })), { "paid" })
  t.same(
    ids(discover(opencode.ZEN, { OPENCODE_API_KEY = "k" }, { enable_free_models = true })),
    { "free-one", "paid", "unpriced" }
  )
end)

t.case("public_fallback_lists_only_free_models", function()
  t.same(ids(discover(opencode.ZEN, {}, { enable_free_models = true })), { "free-one", "unpriced" })
  t.same(discover(opencode.GO, {}), {})
end)

t.case("opencode_go_reads_free_models_from_zen_section", function()
  t.same(discover(opencode.GO, {}, { enable_free_models = true }), {})
  zen_section = { enable_free_models = true }
  t.same(ids(discover(opencode.GO, {}, {})), { "free-one", "unpriced" })
  zen_section = {}
end)

t.case("descriptors_mark_free_models", function()
  local found = discover(opencode.ZEN, { OPENCODE_API_KEY = "k" }, { enable_free_models = true })
  t.eq(found[1].free, true)
  t.eq(found[2].free, false)
  t.eq(found[2].context_window, 400000)
end)

t.case("public_fallback_streams_with_the_public_key", function()
  local req = streamed(opencode.ZEN, "free-one", {}, { enable_free_models = true })
  t.eq(req.url, ZEN_API .. "/chat/completions")
  t.eq(req.headers.Authorization, "Bearer " .. opencode.PUBLIC_KEY)
end)

t.case("gated_stream_says_how_to_get_access", function()
  local _, _, err = streamed(opencode.GO, "free-one", {})
  t.eq(err.kind, http.CONFIG_KIND)
  t.eq(err.message, GATED_ERROR)
end)

t.case("opencode_go_stream_sends_session_header", function()
  local req = streamed(opencode.GO, "paid", { OPENCODE_API_KEY = "k" }, {}, SESSION)
  t.eq(req.url, GO_API .. "/chat/completions")
  t.eq(req.headers.Authorization, "Bearer k")
  t.eq(req.headers[opencode.SESSION_HEADER], SESSION)
  t.eq(maki.json.decode(req.body).model, "paid")
  req = streamed(opencode.ZEN, "paid", { OPENCODE_API_KEY = "k" })
  t.eq(req.headers[opencode.SESSION_HEADER], nil)
end)

t.case("unknown_model_fails_at_stream", function()
  local _, _, err = streamed(opencode.ZEN, "missing", { OPENCODE_API_KEY = "k" })
  t.eq(err.message, "model 'missing' not found from provider 'opencode'")
end)

t.case("zen_sends_catalog_prefixed_models_to_that_provider", function()
  local req = streamed(opencode.ZEN, "nvidia/openai/gpt-oss-120b", { NVIDIA_API_KEY = "n" }, {}, SESSION)
  t.eq(req.url, NVIDIA_API .. "/chat/completions")
  t.eq(req.headers.Authorization, "Bearer n")
  t.eq(req.headers[opencode.SESSION_HEADER], nil)
  t.eq(maki.json.decode(req.body).model, "openai/gpt-oss-120b")
  local saved_ctx, saved_instance, saved_err = prepare(
    opencode.ZEN,
    "nvidia/openai/gpt-oss-120b",
    {},
    {},
    nil,
    { api_key = "opencode-secret" }
  )
  t.eq(saved_instance, nil)
  t.eq(saved_err.kind, http.CONFIG_KIND)
  t.eq(#saved_ctx.requests, 0)
  assert(saved_err.message:find("NVIDIA_API_KEY not set", 1, true))
end)

t.case("own_prefix_names_the_own_model", function()
  local req = streamed(opencode.ZEN, "opencode/paid", { OPENCODE_API_KEY = "k" })
  t.eq(req.url, ZEN_API .. "/chat/completions")
  t.eq(maki.json.decode(req.body).model, "paid")
end)

t.case("go_usage_maps_lanes_to_windows", function()
  local usage = opencode.parse_usage(maki.json.decode(SAMPLE_USAGE))
  t.same(usage.limits, {
    { kind = { Hours = 5 }, percentage = 1, reset_at = ROLLING_RESET_MS },
    { kind = { Weekly = {} }, percentage = 4, reset_at = WEEKLY_RESET_MS },
    { kind = "Monthly", percentage = 2, reset_at = MONTHLY_RESET_MS },
  })
  local _, err = opencode.parse_usage({ usage = {} })
  t.eq(err.message, opencode.EMPTY_USAGE_ERROR)
end)

t.case("only_go_reports_usage", function()
  local ctx, instance = prepare(opencode.GO, "paid", { OPENCODE_API_KEY = "k" }, {}, { t.response({ SAMPLE_USAGE }) })
  local usage = instance.fetch_usage(ctx)
  t.eq(ctx.requests[1].url, GO_API .. "/usage")
  t.eq(ctx.requests[1].headers.Authorization, "Bearer k")
  t.eq(#usage.limits, 3)
  local _, zen = prepare(opencode.ZEN, "paid", { OPENCODE_API_KEY = "k" })
  t.eq(zen.fetch_usage, nil)
end)

t.case("forced_refresh_failure_is_reported", function()
  local spec = opencode.provider(cache(), opencode.ZEN)
  local models, err = spec.discover(t.provider_ctx({ t.response({ "down" }, { status = 502 }) }), {}, true)
  t.eq(models, nil)
  t.eq(err.status, 502)
end)

t.case("cold_catalog_still_prepares", function()
  local spec = opencode.provider(catalog.new(nil), opencode.ZEN)
  local instance, model
  t.with_env({ OPENCODE_API_KEY = "k" }, function()
    instance, model = spec.prepare(t.provider_ctx({}), { id = "paid" }, {})
  end)
  t.eq(model, nil)
  local _, err = instance.stream(t.provider_ctx({}), request("paid"), t.recorder())
  t.eq(err.kind, http.CONFIG_KIND)
end)

t.report()
