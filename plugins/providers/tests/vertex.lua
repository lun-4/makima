local http = require("providers.http")
local t = require("makima.test_helpers")
local vertex = require("providers.vertex")

local PROJECT = "proj"
local NOW = 1000
local GLOBAL_URL =
  "https://aiplatform.googleapis.com/v1/projects/proj/locations/global/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
local ADC = [[{
  "type": "authorized_user",
  "client_id": "client",
  "client_secret": "se cret",
  "refresh_token": "refresh",
  "quota_project_id": "quota"
}]]

local spec = vertex.provider()

local function sse(text)
  local chunk = { candidates = { { content = { parts = { { text = text } } }, finishReason = "STOP" } } }
  return t.response({ "data: " .. assert(makima.json.encode(chunk)) .. "\n\n" })
end

local function token(value, expires_in)
  return t.response({ assert(makima.json.encode({ access_token = value, expires_in = expires_in or 3600 })) })
end

local function request(id, tools)
  return {
    model = { id = id or "gemini-2.5-flash", max_output_tokens = 65536, context_window = 1048576 },
    system = "",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = tools or {},
    thinking = { kind = "off" },
  }
end

-- Runs {fn} with {env} and `makima.time.now()` at {secs}.
local function at(secs, env, fn)
  local now = makima.time.now
  makima.time.now = function()
    return { secs = secs, nanosecs = 0 }
  end
  local ok, err = pcall(t.with_env, env, fn)
  makima.time.now = now
  if not ok then
    error(err, 0)
  end
end

local function adc_file(content)
  local dir = t.mktmpdir("vertex_adc")
  makima.fs.write(dir .. "/adc.json", content)
  return dir .. "/adc.json"
end

local function prepared(env, responses, config)
  local ctx = t.provider_ctx(responses)
  local instance, model
  t.with_env(env, function()
    instance, model = spec.prepare(ctx, { id = "gemini-2.5-flash" }, config or {})
  end)
  return ctx, assert(instance, model), model
end

t.case("authorized_user_adc_keeps_quota_project_and_default_token_uri", function()
  t.same(assert(vertex.parse_adc(ADC)), {
    client_id = "client",
    client_secret = "se cret",
    refresh_token = "refresh",
    token_uri = vertex.OAUTH_TOKEN_URL,
    quota_project = "quota",
  })
end)

t.case("unsupported_and_invalid_adc_files_are_rejected", function()
  for _, kind in ipairs({ "service_account", "external_account" }) do
    local source, err = vertex.parse_adc('{"type":"' .. kind .. '"}')
    t.eq(source, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.has(err.message, "'" .. kind .. "' is not supported")
  end
  t.eq(select(2, vertex.parse_adc("not json")), vertex.INVALID_ADC)
  t.eq(select(2, vertex.parse_adc('{"type":"authorized_user"}')), vertex.INVALID_ADC)
end)

t.case("source_precedence", function()
  local missing = "/does/not/exist"
  t.with_env({ GOOGLE_APPLICATION_CREDENTIALS = missing }, function()
    local source, err = vertex.source()
    t.eq(source, nil)
    t.same(err, http.config_error("GOOGLE_APPLICATION_CREDENTIALS does not exist: " .. missing))
  end)
  local explicit = adc_file(ADC)
  t.with_env({ GOOGLE_APPLICATION_CREDENTIALS = explicit, HOME = "/nonexistent" }, function()
    t.eq(assert(vertex.source()).client_id, "client")
  end)
  local home = t.mktmpdir("vertex_home")
  makima.fs.mkdir(home .. "/.config/gcloud", { parents = true })
  makima.fs.write(home .. "/.config/gcloud/application_default_credentials.json", ADC)
  t.with_env({ HOME = home }, function()
    t.eq(assert(vertex.source()).refresh_token, "refresh")
  end)
  t.with_env({ OS = "Windows_NT", APPDATA = home .. "/.config", HOME = "/nonexistent" }, function()
    t.eq(assert(vertex.source()).refresh_token, "refresh")
  end)
  t.with_env({ HOME = "/nonexistent" }, function()
    t.same(assert(vertex.source()), { metadata = true })
  end)
end)

t.case("project_precedence", function()
  local adc = adc_file(ADC)
  local function project(env, config)
    env.GOOGLE_APPLICATION_CREDENTIALS = adc
    local settings, err
    t.with_env(env, function()
      settings, err = vertex.settings(config or {})
    end)
    return settings and settings.project, err
  end
  t.eq(project({ GOOGLE_CLOUD_PROJECT = "env", GCLOUD_PROJECT = "gcloud" }, { project = "conf" }), "env")
  t.eq(project({ GOOGLE_CLOUD_PROJECT = "", GCLOUD_PROJECT = "gcloud" }, { project = "conf" }), "gcloud")
  t.eq(project({}, { project = "conf" }), "conf")
  t.eq(project({}, { project = "" }), "quota")
  t.with_env({ HOME = "/nonexistent" }, function()
    t.same(select(2, vertex.settings({})), http.config_error(vertex.PROJECT_ERROR))
  end)
end)

t.case("location_precedence_and_endpoint_host", function()
  local function settings(env, config)
    env.GOOGLE_CLOUD_PROJECT = PROJECT
    local result
    t.with_env(env, function()
      result = assert(vertex.settings(config))
    end)
    return result
  end
  t.eq(settings({ GOOGLE_CLOUD_LOCATION = "us-central1" }, { location = "conf" }).location, "us-central1")
  t.eq(settings({}, { location = "conf" }).location, "conf")
  t.eq(settings({}, { location = "" }).location, "global")
  t.eq(vertex.stream_url(settings({}, {}), "gemini-2.5-flash"), GLOBAL_URL)
  t.eq(
    vertex.stream_url(settings({}, { location = "us-central1" }), "gemini-2.5-flash"),
    "https://us-central1-aiplatform.googleapis.com/v1/projects/proj/locations/us-central1/publishers/google/models/gemini-2.5-flash:streamGenerateContent?alt=sse"
  )
end)

t.case("schema_types_are_uppercased_only_in_tools", function()
  local req = request(nil, {
    {
      name = "read",
      description = "",
      input_schema = {
        type = "object",
        properties = { path = { type = "string", default = { type = "file" } } },
      },
    },
  })
  req.messages = {
    { role = "assistant", content = { { type = "tool_use", id = "c1", name = "read", input = { type = "string" } } } },
  }
  local body = vertex.build_request(req)
  local parameters = body.tools[1].functionDeclarations[1].parameters
  t.eq(parameters.type, "OBJECT")
  t.eq(parameters.properties.path.type, "STRING")
  t.eq(parameters.properties.path.default.type, "file")
  t.eq(body.contents[1].parts[1].functionCall.args.type, "string")
  t.eq(req.tools[1].input_schema.type, "object")
end)

t.case("authorized_user_refreshes_once_and_sends_quota_project", function()
  local env = { GOOGLE_APPLICATION_CREDENTIALS = adc_file(ADC), GOOGLE_CLOUD_PROJECT = PROJECT }
  local ctx, instance = prepared(env, { token("tok"), sse("1"), sse("2") })
  at(NOW, env, function()
    assert(instance.stream(ctx, request(), t.recorder()))
    assert(instance.stream(ctx, request(), t.recorder()))
  end)
  t.eq(#ctx.requests, 3)
  local refresh = ctx.requests[1]
  t.eq(refresh.url, vertex.OAUTH_TOKEN_URL)
  t.eq(refresh.method, "POST")
  t.eq(refresh.headers["Content-Type"], "application/x-www-form-urlencoded")
  t.eq(
    refresh.body,
    "client_id=client&client_secret=se%20cret&refresh_token=refresh&grant_type=refresh_token&scope=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform"
  )
  local sent = ctx.requests[2]
  t.eq(sent.url, GLOBAL_URL)
  t.eq(sent.headers.Authorization, "Bearer tok")
  t.eq(sent.headers["x-goog-user-project"], "quota")
  t.eq(sent.headers["Content-Type"], "application/json")
end)

t.case("metadata_token_is_cached_until_a_minute_before_expiry", function()
  local env = { HOME = "/nonexistent", GOOGLE_CLOUD_PROJECT = PROJECT }
  local ctx, instance = prepared(env, { token("a", 120), sse("1"), sse("2"), token("b"), sse("3") })
  at(NOW, env, function()
    assert(instance.stream(ctx, request(), t.recorder()))
  end)
  at(NOW + 59, env, function()
    assert(instance.stream(ctx, request(), t.recorder()))
  end)
  at(NOW + 60, env, function()
    assert(instance.stream(ctx, request(), t.recorder()))
  end)
  local metadata = ctx.requests[1]
  t.eq(metadata.url, vertex.METADATA_TOKEN_URL)
  t.eq(metadata.headers["Metadata-Flavor"], "Google")
  t.eq(ctx.requests[2].headers["x-goog-user-project"], PROJECT)
  t.eq(ctx.requests[3].headers.Authorization, "Bearer a")
  t.eq(ctx.requests[4].url, vertex.METADATA_TOKEN_URL)
  t.eq(ctx.requests[5].headers.Authorization, "Bearer b")
end)

t.case("unauthorized_refreshes_and_retries_once", function()
  local env = { HOME = "/nonexistent", GOOGLE_CLOUD_PROJECT = PROJECT }
  local stale = t.response({ "expired" }, { status = 401 })
  local ctx, instance = prepared(env, { token("a"), stale, token("b"), sse("ok") })
  at(NOW, env, function()
    t.eq(assert(instance.stream(ctx, request(), t.recorder())).message.content[1].text, "ok")
  end)
  t.eq(stale.closed, true)
  t.eq(ctx.requests[4].headers.Authorization, "Bearer b")

  ctx, instance = prepared(env, {
    token("a"),
    t.response({ "no" }, { status = 401 }),
    token("b"),
    t.response({ "still no" }, { status = 401 }),
  })
  at(NOW, env, function()
    local result, err = instance.stream(ctx, request(), t.recorder())
    t.eq(result, nil)
    t.same(err, { status = 401, message = "still no" })
  end)
end)

t.case("reload_auth_drops_the_cached_token", function()
  local env = { HOME = "/nonexistent", GOOGLE_CLOUD_PROJECT = PROJECT }
  local ctx, instance = prepared(env, { token("a"), sse("1"), token("b"), sse("2") })
  at(NOW, env, function()
    assert(instance.stream(ctx, request(), t.recorder()))
    instance.reload_auth(ctx)
    assert(instance.stream(ctx, request(), t.recorder()))
  end)
  t.eq(ctx.requests[4].headers.Authorization, "Bearer b")
end)

t.case("token_failures", function()
  local env = { HOME = "/nonexistent", GOOGLE_CLOUD_PROJECT = PROJECT }
  local ctx, instance = prepared(env, {})
  at(NOW, env, function()
    local _, err = instance.stream(ctx, request(), t.recorder())
    t.same(err, http.config_error(vertex.METADATA_UNAVAILABLE .. "nil"))
  end)
  ctx, instance = prepared(env, { t.response({ "{}" }) })
  at(NOW, env, function()
    t.eq(select(2, instance.stream(ctx, request(), t.recorder())), vertex.INVALID_TOKEN)
  end)
  env.GOOGLE_APPLICATION_CREDENTIALS = adc_file(ADC)
  ctx, instance = prepared(env, { t.response({ "invalid_grant" }, { status = 400 }) })
  at(NOW, env, function()
    t.same(select(2, instance.stream(ctx, request(), t.recorder())), { status = 400, message = "invalid_grant" })
  end)
end)

t.case("prepare_fails_without_a_project", function()
  t.with_env({ HOME = "/nonexistent" }, function()
    local instance, err = spec.prepare(t.provider_ctx({}), { id = "gemini-2.5-flash" }, {})
    t.eq(instance, nil)
    t.same(err, http.config_error(vertex.PROJECT_ERROR))
  end)
end)

t.case("curated_models_and_context_windows", function()
  local windows = {}
  for _, model in ipairs(spec.models) do
    windows[model.id] = model.context_window
  end
  t.eq(windows["gemini-3.6-flash"], 1048576)
  t.eq(windows["gemini-3.7-flash"], 1048576)
  t.eq(windows["gemini-3.8-flash"], 500000)
  local first = {}
  for _, model in ipairs(spec.models) do
    first[model.tier] = first[model.tier] or model.id
  end
  t.same(first, { weak = "gemini-2.5-flash-lite", medium = "gemini-2.5-flash", strong = "gemini-2.5-pro" })
  local _, _, unknown = prepared({ HOME = "/nonexistent", GOOGLE_CLOUD_PROJECT = PROJECT }, {})
  t.eq(unknown.pricing.input, 0.3)
end)

t.case("login_shows_instructions_and_status_follows_settings", function()
  local shown
  local ui = {
    instructions = function(_, text)
      shown = text
      return true
    end,
  }
  t.eq(spec.auth[1].login(t.provider_ctx({}), ui, {}), true)
  t.eq(shown, vertex.LOGIN_INSTRUCTIONS)
  t.with_env({ HOME = "/nonexistent" }, function()
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "missing", detail = vertex.PROJECT_ERROR })
    t.same(spec.auth_status(t.provider_ctx({}), { project = PROJECT }), {
      state = "env",
      detail = "via metadata server, project proj",
    })
  end)
  t.with_env({ GOOGLE_APPLICATION_CREDENTIALS = adc_file(ADC) }, function()
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "env", detail = "via ADC, project quota" })
  end)
end)

t.report()
