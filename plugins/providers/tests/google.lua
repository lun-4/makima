local google = require("providers.google")
local http = require("providers.http")
local t = require("makima.test_helpers")

local KEY_ENV = "GEMINI_API_KEY"
local BASE = "https://generativelanguage.googleapis.com/v1beta"
local PROXY = "https://proxy.example.invalid/v1beta"
local LOGIN_URL = "https://aistudio.google.com/apikey"
local KEY_HEADER = "x-goog-api-key"

local spec = google.provider()

local function sse(text)
  local chunk = { candidates = { { content = { parts = { { text = text } } }, finishReason = "STOP" } } }
  return t.response({ "data: " .. assert(makima.json.encode(chunk)) .. "\n\n" })
end

local function json(value, opts)
  return t.response({ assert(makima.json.encode(value)) }, opts)
end

local function request(id)
  return {
    model = { id = id, max_output_tokens = 65536, context_window = 1048576 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
  }
end

local function prepared(env, id, config, responses, saved)
  local ctx = t.provider_ctx(responses or {}, saved)
  local instance, model
  t.with_env(env, function()
    instance, model = spec.prepare(ctx, { id = id }, config or {})
  end)
  return ctx, assert(instance, model), model
end

local function stream(env, id, config, saved)
  local ctx, instance = prepared(env, id, config, { sse("ok") }, saved)
  local result
  t.with_env(env, function()
    result = assert(instance.stream(ctx, request(id), t.recorder()))
  end)
  t.eq(result.message.content[1].text, "ok")
  return ctx.requests[1]
end

t.case("stream_sends_key_header_and_gemini_body", function()
  local sent = stream({ [KEY_ENV] = "g-1" }, "gemini-2.5-flash")
  t.eq(sent.url, BASE .. "/models/gemini-2.5-flash:streamGenerateContent?alt=sse")
  t.eq(sent.method, "POST")
  t.eq(sent.headers[KEY_HEADER], "g-1")
  t.eq(sent.headers.Authorization, nil)
  t.eq(sent.headers["Content-Type"], "application/json")
  local body = makima.json.decode(sent.body)
  t.eq(body.contents[1].parts[1].text, "hi")
  t.eq(body.systemInstruction.parts[1].text, "sys")
  t.eq(body.generationConfig.maxOutputTokens, 65536)
end)

t.case("model_id_is_url_encoded", function()
  local sent = stream({ [KEY_ENV] = "k" }, "tuned/model a")
  t.eq(sent.url, BASE .. "/models/tuned%2Fmodel%20a:streamGenerateContent?alt=sse")
end)

t.case("base_url_from_env_then_config", function()
  local sent = stream({ [KEY_ENV] = "k", GOOGLE_BASE_URL = PROXY }, "gemini-2.5-pro", { base_url = "http://unused" })
  t.has(sent.url, PROXY .. "/models/")
  sent = stream({ [KEY_ENV] = "k" }, "gemini-2.5-pro", { base_url = PROXY })
  t.has(sent.url, PROXY .. "/models/")
end)

t.case("saved_key_and_configured_header_override", function()
  local sent = stream({ TOKEN = "gw" }, "gemini-2.5-pro", {
    headers = { ["X-Goog-Api-Key"] = "${TOKEN}", ["X-Extra"] = "1" },
  }, { api_key = "saved" })
  t.eq(sent.headers[KEY_HEADER], nil)
  t.eq(sent.headers["X-Goog-Api-Key"], "gw")
  t.eq(sent.headers["X-Extra"], "1")
  sent = stream({}, "gemini-2.5-pro", {}, { api_key = "saved" })
  t.eq(sent.headers[KEY_HEADER], "saved")
end)

t.case("missing_key_fails_prepare", function()
  t.with_env({}, function()
    local instance, err = spec.prepare(t.provider_ctx({}), { id = "gemini-2.5-pro" }, {})
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, KEY_ENV)
  end)
end)

t.case("keys_rotate_and_reload", function()
  local env = { [KEY_ENV] = "a,b" }
  local ctx, instance = prepared(env, "gemini-2.5-pro", {}, { sse("1"), sse("2") })
  t.eq(instance.key_count(), 2)
  t.with_env(env, function()
    assert(instance.rotate_key())
    assert(instance.stream(ctx, request("gemini-2.5-pro"), t.recorder()))
  end)
  t.eq(ctx.requests[1].headers[KEY_HEADER], "b")
  t.with_env({ [KEY_ENV] = "c" }, function()
    t.eq(instance.reload_auth(ctx), nil)
    t.eq(instance.key_count(), 1)
    assert(instance.stream(ctx, request("gemini-2.5-pro"), t.recorder()))
  end)
  t.eq(ctx.requests[2].headers[KEY_HEADER], "c")
end)

t.case("api_errors_keep_status_and_retry_after", function()
  local ctx, instance = prepared({ [KEY_ENV] = "k" }, "gemini-2.5-pro", {}, {
    t.response({ "quota" }, { status = 429, headers = { ["retry-after"] = "7" } }),
  })
  local result, err = instance.stream(ctx, request("gemini-2.5-pro"), t.recorder())
  t.eq(result, nil)
  t.same(err, { status = 429, message = "quota", retry_after = 7, retry = "rate_limit" })
end)

t.case("prepare_describes_curated_prefixes_and_unknown_models", function()
  local _, _, flash = prepared({ [KEY_ENV] = "k" }, "gemini-2.5-flash-preview-05-20")
  t.eq(flash.tier, "medium")
  t.eq(flash.pricing.input, 0.15)
  t.eq(flash.context_window, 1048576)
  local _, _, unknown = prepared({ [KEY_ENV] = "k" }, "gemini-9-ultra")
  t.same(unknown, {
    id = "gemini-9-ultra",
    context_window = 1000000,
    max_output_tokens = 65536,
    supports_thinking = true,
    supports_vision = true,
    supports_tool_examples = false,
  })
end)

t.case("curated_tier_defaults", function()
  local first = {}
  for _, model in ipairs(spec.models) do
    first[model.tier] = first[model.tier] or model.id
  end
  t.same(first, { weak = "gemini-2.0-flash-lite", medium = "gemini-2.5-flash", strong = "gemini-2.5-pro" })
end)

t.case("discover_lists_generate_content_models", function()
  local ctx = t.provider_ctx({
    json({
      models = {
        { name = "models/gemini-zeta", supportedGenerationMethods = { "generateContent" } },
        { name = "models/text-embedding-004", supportedGenerationMethods = { "embedContent" } },
        { name = "models/gemini-2.5-pro", supportedGenerationMethods = { "countTokens", "generateContent" } },
        { name = "models/no-methods" },
      },
    }),
  })
  local models
  t.with_env({ [KEY_ENV] = "k" }, function()
    models = assert(spec.discover(ctx, {}))
  end)
  local sent = ctx.requests[1]
  t.eq(sent.url, BASE .. "/models?key=k&pageSize=1000")
  t.eq(sent.headers[KEY_HEADER], "k")
  local ids = {}
  for _, model in ipairs(models) do
    ids[#ids + 1] = model.id
  end
  t.same(ids, { "gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.0-flash-lite", "gemini-zeta" })
  t.eq(models[1].pricing.input, 1.25)
end)

t.case("discover_failure_returns_the_api_error", function()
  local ctx = t.provider_ctx({ t.response({ "forbidden" }, { status = 403 }) })
  t.with_env({ [KEY_ENV] = "k" }, function()
    local models, err = spec.discover(ctx, {})
    t.eq(models, nil)
    t.same(err, { status = 403, message = "forbidden" })
  end)
end)

t.case("login_opens_the_key_page_and_saves_the_key", function()
  local opened
  local ui = {
    open_browser = function(_, url)
      opened = url
      return true
    end,
    input = function(_, prompt, secret)
      t.eq(prompt, "Google API key")
      t.eq(secret, true)
      return "  g-key  "
    end,
  }
  local ctx = t.provider_ctx({})
  local result = assert(spec.auth[1].login(ctx, ui, {}))
  t.eq(opened, LOGIN_URL)
  t.same(ctx.saved, { api_key = "g-key" })
  t.same(result, { model = "gemini-2.5-pro" })
end)

t.case("auth_status_reports_the_key_source", function()
  t.with_env({ [KEY_ENV] = "k" }, function()
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "env", detail = "via " .. KEY_ENV })
  end)
  t.with_env({}, function()
    t.same(spec.auth_status(t.provider_ctx({}, { api_key = "AIzaSy-secret" }), {}), {
      state = "saved",
      detail = "key: AIza...cret",
    })
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "missing" })
  end)
end)

t.report()
