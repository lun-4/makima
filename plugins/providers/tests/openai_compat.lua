local compat = require("providers.openai_compat")
local http = require("providers.http")
local t = require("makima.test_helpers")

local KEY_ENV = "TEST_API_KEY"
local BASE = "https://api.example.invalid/v1"
local PLAN_BASE = "https://plan.example.invalid/v1"
local LOGIN_URL = "https://example.invalid/keys"
local PLAN_LOGIN_URL = "https://example.invalid/plan-keys"
local HEADER_ERROR = "provider 'test' header 'X-Gateway': environment variable 'GATEWAY' is unset or empty"
local PROVIDERS = { "synthetic", "mistral", "zai", "deepseek", "openrouter", "requesty", "regolo", "tensorx" }
local DONE = "data: [DONE]\n\n"

local function def(overrides)
  local result = {
    id = "test",
    display_name = "Test",
    env = KEY_ENV,
    base_url = BASE,
    login_url = LOGIN_URL,
    model = "row-1",
    plans = {
      { id = "standard", label = "Standard" },
      { id = "plan", label = "Plan", base_url = PLAN_BASE, model = "plan-model", login_url = PLAN_LOGIN_URL },
    },
    defaults = { context_window = 1000, supports_thinking = true },
    models = {
      { id = "row-1", aliases = { "row-1-alias" }, tier = "strong", context_window = 2000 },
      { id = "row-1-long", tier = "weak" },
    },
  }
  for key, value in pairs(overrides or {}) do
    result[key] = value
  end
  return result
end

local function sse(text)
  return t.response({
    'data: {"choices":[{"delta":{"content":"' .. text .. '"},"finish_reason":"stop"}]}\n\n' .. DONE,
  })
end

local function request(overrides)
  local result = {
    model = { id = "row-1", output_tokens = 100, max_output_tokens = 512, context_window = 2000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
    session_id = "session-1",
  }
  for key, value in pairs(overrides or {}) do
    result[key] = value
  end
  return result
end

local function header(headers, name)
  local found
  for key, value in pairs(headers) do
    if key:lower() == name:lower() then
      assert(found == nil, "duplicate header " .. name)
      found = value
    end
  end
  return found
end

t.case("base_url_precedence", function()
  local d = def()
  t.with_env({}, function()
    t.eq(compat.base_url(d, {}), BASE)
    t.eq(compat.base_url(d, { plan = "plan" }), PLAN_BASE)
    t.eq(compat.base_url(d, { plan = "plan", base_url = "https://config.invalid" }), "https://config.invalid")
  end)
  t.with_env({ TEST_BASE_URL = "https://env.invalid" }, function()
    t.eq(compat.base_url(d, { base_url = "https://config.invalid" }), "https://env.invalid")
  end)
end)

t.case("base_url_trims_trailing_slashes", function()
  local d = def()
  t.with_env({}, function()
    t.eq(compat.base_url(d, { base_url = "https://config.invalid/" }), "https://config.invalid")
    t.eq(compat.base_url(d, { base_url = "https://config.invalid///" }), "https://config.invalid")
    t.eq(compat.base_url(d, { base_url = "https://config.invalid/v1/" }), "https://config.invalid/v1")
  end)
  t.with_env({ TEST_BASE_URL = "https://env.invalid//" }, function()
    t.eq(compat.base_url(d, {}), "https://env.invalid")
  end)
end)

t.case("describe_takes_the_longest_prefix_over_defaults", function()
  local d = def({
    adjust = function(model)
      model.adjusted = true
      return model
    end,
  })
  t.same(compat.describe(d, { id = "row-1-alias-2024" }), {
    id = "row-1-alias-2024",
    tier = "strong",
    context_window = 2000,
    supports_thinking = true,
    adjusted = true,
  })
  t.eq(compat.describe(d, { id = "row-1-long-x" }).tier, "weak")
  t.same(compat.describe(d, { id = "unknown", context_window = 5 }), {
    id = "unknown",
    context_window = 5,
    supports_thinking = true,
    adjusted = true,
  })
end)

t.case("descriptors_list_rows_before_aliases", function()
  local ids = {}
  for _, model in ipairs(compat.descriptors(def())) do
    ids[#ids + 1] = model.id
  end
  t.same(ids, { "row-1", "row-1-long", "row-1-alias" })
end)

t.case("parse_model_reads_common_fields", function()
  t.same(
    compat.parse_model({
      id = "m",
      max_model_len = 4096,
      max_output_length = 512,
      input_modalities = { "text", "image" },
      pricing = { prompt = "1", completion = "2", cache_creation = "x", cache_read = "0.5" },
    }),
    {
      id = "m",
      context_window = 4096,
      max_output_tokens = 512,
      supports_vision = true,
      pricing = { input = 1, output = 2, cache_write = 0, cache_read = 0.5 },
    }
  )
  t.same(compat.parse_model({ id = "m", pricing = { prompt = "1", completion = "2" } }), { id = "m" })
  t.eq(compat.parse_model({ id = "m", input_modalities = { "text" } }).supports_vision, false)
end)

t.case("prepare_without_key_fails", function()
  t.with_env({}, function()
    local instance, err = compat.provider(def()).prepare(t.provider_ctx({}), { id = "row-1" }, {})
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, KEY_ENV)
  end)
end)

t.case("prepare_names_unset_header_variable", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    local _, err = compat.provider(def()).prepare(t.provider_ctx({}), { id = "row-1" }, {
      headers = { ["X-Gateway"] = "${GATEWAY}" },
    })
    t.same(err, http.config_error(HEADER_ERROR))
  end)
end)

t.case("prepare_returns_the_described_model", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    local _, model = compat.provider(def()).prepare(t.provider_ctx({}), { id = "row-1-alias" }, {})
    t.eq(model.context_window, 2000)
    t.eq(model.tier, "strong")
  end)
end)

t.case("stream_rotates_keys_and_reclassifies_errors", function()
  t.with_env({ [KEY_ENV] = "a,b" }, function()
    local failed = t.response({ "boom" }, { status = 500 })
    local ctx = t.provider_ctx({ failed, sse("ok") })
    local spec = compat.provider(def({
      api_error = function(err)
        return { status = 402, message = err.message }
      end,
    }))
    local instance = spec.prepare(ctx, { id = "row-1" }, {})
    t.eq(instance.key_count, 2)
    local result, err = instance.stream(ctx, request(), t.recorder())
    t.eq(result, nil)
    t.same(err, { status = 402, message = "boom" })
    t.eq(instance.rotate_key(), true)
    assert(instance.stream(ctx, request(), t.recorder()))
    t.eq(ctx.requests[1].headers.Authorization, "Bearer a")
    t.eq(ctx.requests[2].headers.Authorization, "Bearer b")
    t.eq(ctx.requests[2].url, BASE .. "/chat/completions")
    t.eq(ctx.requests[2].method, "POST")
    t.eq(makima.json.decode(ctx.requests[2].body).max_tokens, 512)
  end)
end)

t.case("discover_without_key_keeps_curated_models", function()
  t.with_env({}, function()
    local ctx = t.provider_ctx({})
    local models = compat.provider(def()).discover(ctx, {})
    t.eq(#models, 3)
    t.eq(#ctx.requests, 0)
  end)
end)

t.case("discover_reports_header_setup_errors", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    local config = { headers = { ["X-Gateway"] = "${MISSING_GATEWAY}" } }
    local spec = compat.provider(def())
    local _, discover_err = spec.discover(t.provider_ctx({}), config)
    local _, prepare_err = spec.prepare(t.provider_ctx({}), { id = "row-1" }, config)
    t.same(discover_err, prepare_err)
    t.eq(discover_err.kind, http.CONFIG_KIND)
  end)
end)

t.case("discover_merges_listing_over_curated_rows", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    local listing = t.response({ '{"data":[{"id":"new"},{"id":"row-1","context_length":3000}]}' })
    local ctx = t.provider_ctx({ listing })
    local models = compat.provider(def()).discover(ctx, {})
    t.eq(ctx.requests[1].url, BASE .. "/models")
    local ids = {}
    for _, model in ipairs(models) do
      ids[#ids + 1] = model.id
    end
    t.same(ids, { "row-1", "row-1-long", "row-1-alias", "new" })
    t.eq(models[1].context_window, 3000)
    t.eq(models[1].tier, "strong")
    t.eq(models[4].context_window, 1000)
  end)
end)

t.case("discover_failure_is_an_error", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    local ctx = t.provider_ctx({ t.response({ "nope" }, { status = 401 }) })
    local models, err = compat.provider(def()).discover(ctx, {})
    t.eq(models, nil)
    t.same(err, { status = 401, message = "nope" })
  end)
end)

local function ui(answer)
  local log = {}
  return {
    open_browser = function(_, url)
      log[#log + 1] = { "open", url }
      return true
    end,
    input = function(_, prompt, secret)
      log[#log + 1] = { "input", prompt, secret }
      return answer
    end,
  },
    log
end

t.case("login_opens_the_plan_page_and_saves_the_key", function()
  local ctx = t.provider_ctx({})
  local login_ui, log = ui("  key  ")
  local result = compat.provider(def()).auth[1].login(ctx, login_ui, { plan = "plan" })
  t.same(result, { model = "plan-model" })
  t.same(log, { { "open", PLAN_LOGIN_URL }, { "input", "Test API key", true } })
  t.same(ctx.saved, { api_key = "key" })
end)

t.case("login_with_empty_key_saves_nothing", function()
  local ctx = t.provider_ctx({})
  local login_ui, log = ui("")
  t.same(compat.provider(def()).auth[1].login(ctx, login_ui, {}), { model = "row-1" })
  t.eq(log[1][2], LOGIN_URL)
  t.eq(ctx.saved, nil)
end)

t.case("auth_status_reports_the_key_source", function()
  local status = compat.provider(def()).auth_status
  t.with_env({}, function()
    t.same(status(t.provider_ctx({}), {}), { state = "missing" })
    t.same(status(t.provider_ctx({}, { api_key = "sk-1234567890" }), { plan = "plan" }), {
      state = "saved",
      detail = "key: sk-1...7890, plan",
    })
    t.same(status(t.provider_ctx({}), { api_key = "short" }), { state = "configured" })
  end)
  t.with_env({ OTHER = "k" }, function()
    t.same(status(t.provider_ctx({}), { api_key_env = "OTHER" }), { state = "env", detail = "via OTHER" })
  end)
end)

t.case("plans_become_a_config_choice", function()
  t.same(compat.provider(def()).config, {
    {
      key = "plan",
      prompt = "Plan",
      choices = { { id = "standard", label = "Standard" }, { id = "plan", label = "Plan" } },
    },
  })
end)

-- Every provider built on the factory expands `${VAR}` in configured headers,
-- lets them replace the key, and sends one Content-Type.
t.case("every_provider_expands_configured_headers", function()
  for _, name in ipairs(PROVIDERS) do
    local provider_def = require("providers." .. name).def
    t.with_env({ [provider_def.env] = "key-a,key-b", TOKEN = "secret" }, function()
      local ctx = t.provider_ctx({ sse("ok") })
      ctx.id = provider_def.id
      local config = { headers = { authorization = "Token ${TOKEN}", ["content-type"] = "application/x" } }
      local spec = compat.provider(provider_def)
      local instance = assert(spec.prepare(ctx, spec.models[1] or { id = "vendor/model" }, config))
      t.eq(instance.key_count, 2, name)
      assert(instance.stream(ctx, request(), t.recorder()))
      local headers = ctx.requests[1].headers
      t.eq(header(headers, "Authorization"), "Token secret", name)
      t.eq(header(headers, "Content-Type"), "application/x", name)
    end)
  end
end)

t.report()
