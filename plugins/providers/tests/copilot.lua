local copilot = require("providers.copilot")
local http = require("providers.http")
local t = require("makima.test_helpers")

local TOKEN_ENV = "GH_COPILOT_TOKEN"
local ENTERPRISE = "https://copilot-api.myco.ghe.com"
local DONE = "data: [DONE]\n\n"

local spec = copilot.provider()

local function json(value, status)
  return t.response({ assert(makima.json.encode(value)) }, { status = status })
end

local function graphql(api)
  return json({ data = { viewer = { copilotEndpoints = { api = api } } } })
end

local function chat_sse(text)
  return t.response({
    'data: {"choices":[{"delta":{"content":"' .. text .. '"},"finish_reason":"stop"}]}\n\n' .. DONE,
  })
end

local function responses_sse(text)
  return t.response({
    'data: {"type":"response.output_text.delta","delta":"' .. text .. '"}\n\n',
    'data: {"type":"response.completed","response":{"status":"completed"}}\n\n',
  })
end

local function messages_sse(text)
  local frames = {}
  for _, event in ipairs({
    { type = "message_start", message = { usage = { input_tokens = 1 } } },
    { type = "content_block_start", index = 0, content_block = { type = "text", text = "" } },
    { type = "content_block_delta", index = 0, delta = { type = "text_delta", text = text } },
    { type = "message_delta", delta = { stop_reason = "end_turn" }, usage = { output_tokens = 1 } },
    { type = "message_stop" },
  }) do
    frames[#frames + 1] = "event: " .. event.type .. "\ndata: " .. assert(makima.json.encode(event)) .. "\n\n"
  end
  return t.response({ table.concat(frames) })
end

local function request(id, extra)
  local result = {
    model = { id = id, output_tokens = 8000, max_output_tokens = 64000, context_window = 200000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
    fast = true,
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

local function body(sent)
  return makima.json.decode(sent.body)
end

local function stream(env, info, responses, extra, config, saved)
  local ctx = t.provider_ctx(responses, saved)
  local result, err
  t.with_env(env, function()
    local instance = assert(spec.prepare(ctx, info, config or {}))
    result, err = instance.stream(ctx, request(info.id, extra), t.recorder())
  end)
  return ctx, assert(result, err)
end

local function listed(id, extra)
  local entry = { id = id, model_picker_enabled = true, capabilities = { type = "chat" } }
  for key, value in pairs(extra or {}) do
    entry[key] = value
  end
  return entry
end

local function priced(id)
  return listed(id, {
    billing = {
      token_prices = { batch_size = 500000, default = { input_price = 250, output_price = 1250, cache_price = 25 } },
    },
  })
end

t.case("curated_defaults_and_prefix_pricing", function()
  local defaults = {}
  for _, model in ipairs(spec.models) do
    defaults[model.tier] = defaults[model.tier] or model.id
  end
  t.same(defaults, { weak = "gpt-5.6-luna", medium = "gpt-5.6-terra", strong = "claude-opus-5" })
  local compat = require("providers.openai_compat")
  t.eq(compat.describe(copilot.def, { id = "gpt-5.4-mini" }).pricing.input, 0.75)
  t.eq(compat.describe(copilot.def, { id = "claude-opus-4.8-fast" }).pricing.input, 10.00)
  t.eq(compat.describe(copilot.def, { id = "claude-opus-4.7" }).pricing.input, 5.00)
end)

t.case("descriptor_reads_capabilities_and_category", function()
  local model = copilot.descriptor(listed("gpt-5.6-sol", {
    model_picker_category = "powerful",
    supported_endpoints = { "/responses" },
    capabilities = {
      type = "chat",
      limits = { max_context_window_tokens = 1050000, max_output_tokens = 128000 },
      supports = {
        reasoning_effort = { "none", "low", "medium", "high" },
        adaptive_thinking = true,
        max_thinking_budget = 64000,
        vision = true,
      },
    },
  }))
  t.same(model, {
    id = "gpt-5.6-sol",
    context_window = 1050000,
    max_output_tokens = 128000,
    supports_thinking = true,
    supports_vision = true,
    tier = "strong",
    provider_data = { route = "responses", efforts = { "low", "medium", "high" }, off = true, adaptive_thinking = true },
  })
  t.eq(copilot.descriptor(listed("gpt-6", { model_picker_category = "reasoning" })).tier, nil)
end)

t.case("thinking_and_route_follow_supported_endpoints", function()
  local supports = { reasoning_effort = { "low", "high" }, adaptive_thinking = true }
  for _, case in ipairs({
    { { "/chat/completions", "/v1/messages" }, "messages", true },
    { { "/responses" }, "responses", true },
    { { "/chat/completions" }, "chat", false },
    { {}, "chat", false },
  }) do
    local model = copilot.descriptor(listed("reasoner", {
      supported_endpoints = case[1],
      capabilities = { type = "chat", supports = supports },
    }))
    t.eq(model.provider_data.route, case[2])
    t.eq(model.supports_thinking, case[3])
  end
  t.eq(copilot.guess_route("claude-sonnet-4.5"), "messages")
  t.eq(copilot.guess_route("gpt-5.4"), "responses")
  t.eq(copilot.guess_route("o1-codex"), "responses")
  t.eq(copilot.guess_route("gemini-3.1-pro-preview"), "chat")
end)

t.case("ai_credit_prices_become_usd_per_million", function()
  local pricing = copilot.descriptor(listed("gpt-5", {
    billing = {
      token_prices = { batch_size = 500000, default = { input_price = 500, output_price = 3000, cache_price = 50 } },
    },
  })).pricing
  t.same(pricing, { input = 10, output = 60, cache_read = 1, cache_write = 0 })
  t.same(
    copilot.descriptor(priced("claude-opus-5")).pricing,
    { input = 5, output = 25, cache_read = 0.5, cache_write = 6.25 }
  )
  t.eq(copilot.descriptor(listed("gpt-5")).pricing, nil)
  local zero = listed("gpt-5", { billing = { token_prices = { batch_size = 0, default = { input_price = 1 } } } })
  t.eq(copilot.descriptor(zero).pricing, nil)
end)

t.case("responses_dialect_uses_declared_levels", function()
  local dialect = copilot.dialect({ efforts = { "low", "medium", "high" }, off = true, adaptive_thinking = false })
  local responses = require("providers.responses")
  t.same(responses.reasoning({ kind = "off" }, dialect), { effort = "none" })
  t.same(responses.reasoning({ kind = "effort", level = "medium" }, dialect), { effort = "medium", summary = "auto" })
  t.same(responses.reasoning({ kind = "adaptive" }, dialect), { effort = "high", summary = "auto" })
  local adaptive = copilot.dialect({ efforts = {}, off = false, adaptive_thinking = true })
  t.same(responses.reasoning({ kind = "adaptive" }, adaptive), { summary = "auto" })
  t.same(responses.reasoning({ kind = "effort", level = "minimal" }, adaptive), { effort = "low", summary = "auto" })
end)

t.case("token_precedence", function()
  local ctx = t.provider_ctx({}, { api_key = "saved", host = "myco.ghe.com" })
  t.with_env({ GH_COPILOT_TOKEN = " ", COPILOT_GITHUB_TOKEN = " second " }, function()
    t.same(copilot.token(ctx), { token = "second", host = "github.com", env = "COPILOT_GITHUB_TOKEN" })
  end)
  t.with_env({ GH_COPILOT_TOKEN = "first", COPILOT_GITHUB_TOKEN = "second" }, function()
    t.eq(copilot.token(ctx).token, "first")
  end)
  t.with_env({}, function()
    t.same(copilot.token(ctx), { token = "saved", host = "myco.ghe.com" })
    t.eq(copilot.token(t.provider_ctx({})), nil)
  end)
end)

t.case("enterprise_endpoint_discovery_and_headers", function()
  local config = { headers = { ["X-Gateway"] = "${GATEWAY}" } }
  local listing = json({ data = { listed("custom-model", { supported_endpoints = { "/chat/completions" } }) } })
  local ctx = stream(
    { GATEWAY = "gw" },
    { id = "custom-model" },
    { graphql(ENTERPRISE), listing, chat_sse("ok") },
    nil,
    config,
    {
      api_key = "ghu_token",
      host = "myco.ghe.com",
    }
  )
  local query = ctx.requests[1]
  t.eq(query.url, "https://myco.ghe.com/api/graphql")
  t.eq(header(query.headers, "authorization"), "Bearer ghu_token")
  t.eq(ctx.requests[2].url, ENTERPRISE .. "/models")
  t.eq(header(ctx.requests[2].headers, "x-initiator"), nil)
  local sent = ctx.requests[3]
  t.eq(sent.url, ENTERPRISE .. "/chat/completions")
  t.eq(header(sent.headers, "authorization"), "Bearer ghu_token")
  t.eq(header(sent.headers, "x-github-api-version"), "2025-10-01")
  t.has(header(sent.headers, "editor-version"), "Makima/")
  t.eq(header(sent.headers, "x-initiator"), "agent")
  t.eq(header(sent.headers, "x-interaction-type"), "conversation-agent")
  t.eq(header(sent.headers, "openai-intent"), "conversation-agent")
  t.eq(header(sent.headers, "x-gateway"), "gw")
  local sent_body = body(sent)
  t.eq(sent_body.n, 1)
  t.eq(sent_body.temperature, 0.1)
  t.eq(sent_body.max_tokens, nil)
  t.eq(sent_body.stream_options, nil)
end)

t.case("failed_endpoint_discovery_falls_back", function()
  local data = { route = "chat" }
  local ctx = stream({ [TOKEN_ENV] = "tok" }, { id = "m", provider_data = data }, {
    t.response({ "nope" }, { status = 500 }),
    chat_sse("ok"),
  })
  t.eq(ctx.requests[1].url, "https://api.github.com/graphql")
  t.eq(ctx.requests[2].url, copilot.DEFAULT_ENDPOINT .. "/chat/completions")
end)

t.case("discovered_routes_pick_the_wire_protocol", function()
  local env = { [TOKEN_ENV] = "tok" }
  local ctx = stream(env, { id = "claude-opus-5", provider_data = { route = "messages" } }, {
    graphql(ENTERPRISE),
    messages_sse("ok"),
  }, { thinking = { kind = "effort", level = "high" } })
  local sent = ctx.requests[2]
  t.eq(sent.url, ENTERPRISE .. "/v1/messages")
  t.eq(header(sent.headers, "anthropic-version"), "2023-06-01")
  local sent_body = body(sent)
  t.eq(sent_body.speed, nil)
  t.eq(sent_body.thinking.type, "adaptive")
  local data = { route = "responses", efforts = { "low", "high" }, off = true, adaptive_thinking = false }
  ctx = stream(env, { id = "gpt-5.6-sol", provider_data = data }, { graphql(ENTERPRISE), responses_sse("ok") })
  sent = ctx.requests[2]
  t.eq(sent.url, ENTERPRISE .. "/responses")
  t.same(body(sent).reasoning, { effort = "none" })
end)

t.case("unlisted_models_guess_their_route_once", function()
  local env = { [TOKEN_ENV] = "tok" }
  local ctx = t.provider_ctx({ graphql(ENTERPRISE), json({ data = {} }), messages_sse("a"), messages_sse("b") })
  t.with_env(env, function()
    local instance = assert(spec.prepare(ctx, { id = "claude-new" }, {}))
    assert(instance.stream(ctx, request("claude-new"), t.recorder()))
    assert(instance.stream(ctx, request("claude-new"), t.recorder()))
  end)
  t.eq(#ctx.requests, 4)
  t.eq(ctx.requests[3].url, ENTERPRISE .. "/v1/messages")
  t.eq(ctx.requests[4].url, ENTERPRISE .. "/v1/messages")
end)

t.case("reload_forgets_the_endpoint_and_listing", function()
  local env = { [TOKEN_ENV] = "tok" }
  local data = { route = "chat" }
  local ctx = t.provider_ctx({ graphql(ENTERPRISE), chat_sse("a"), graphql(copilot.DEFAULT_ENDPOINT), chat_sse("b") })
  t.with_env(env, function()
    local instance = assert(spec.prepare(ctx, { id = "m", provider_data = data }, {}))
    assert(instance.stream(ctx, request("m"), t.recorder()))
    instance.reload_auth(ctx)
    assert(instance.stream(ctx, request("m"), t.recorder()))
  end)
  t.eq(ctx.requests[2].url, ENTERPRISE .. "/chat/completions")
  t.eq(ctx.requests[4].url, copilot.DEFAULT_ENDPOINT .. "/chat/completions")
end)

t.case("discovery_keeps_enabled_chat_models_default_first", function()
  local ctx = t.provider_ctx({
    graphql(ENTERPRISE),
    json({
      data = {
        listed("alpha"),
        listed("pending", { policy = { state = "pending" } }),
        listed("embedder", { capabilities = { type = "embeddings" } }),
        listed("hidden", { model_picker_enabled = false }),
        listed("beta", { is_chat_default = true, policy = { state = "enabled" } }),
        "malformed",
      },
    }),
  })
  local models
  t.with_env({ [TOKEN_ENV] = "tok" }, function()
    models = assert(spec.discover(ctx, {}))
  end)
  local extras = {}
  for i = #spec.models + 1, #models do
    extras[#extras + 1] = models[i].id
  end
  t.same(extras, { "beta", "alpha" })
  t.with_env({}, function()
    t.eq(#assert(spec.discover(t.provider_ctx({}), {})), #spec.models)
  end)
end)

t.case("discovery_reports_credential_read_errors", function()
  local ctx = t.provider_ctx({})
  ctx.credentials.read = function()
    return nil, "credentials unavailable"
  end
  local models, err = spec.discover(ctx, {})
  t.eq(models, nil)
  t.eq(err, "credentials unavailable")
end)

t.case("import_reads_config_files_then_the_keyring", function()
  t.same(
    copilot.import(
      { { ["enterprise.example.com"] = { oauth_token = "x" } }, { ["github.com"] = { oauth_token = "t1" } } },
      {}
    ),
    {
      token = "t1",
      host = "github.com",
    }
  )
  t.same(
    copilot.import({ { ["myco.ghe.com"] = { oauth_token = "ghe" } } }, {}),
    { token = "ghe", host = "myco.ghe.com" }
  )
  t.eq(copilot.github_host("evil-ghe.com"), false)
  local keyring_get = makima.host.keyring_get
  local asked = {}
  makima.host.keyring_get = function(service, account)
    asked[#asked + 1] = service .. "/" .. account
    if account == "janesmith" then
      return nil, "access denied"
    elseif service == "gh:myco.ghe.com" and account == "" then
      return "go-keyring-base64:Z2hvX3Rva2Vu"
    end
  end
  local hints = {}
  local gh = assert(makima.yaml.decode("myco.ghe.com:\n  user: janedoe\n  users:\n    janedoe:\n    janesmith:\n"))
  local found = copilot.import({ gh }, hints)
  makima.host.keyring_get = keyring_get
  t.same(found, { token = "gho_token", host = "myco.ghe.com" })
  t.same(asked, { "gh:github.com/", "gh:myco.ghe.com/janedoe", "gh:myco.ghe.com/janesmith", "gh:myco.ghe.com/" })
  t.eq(#hints, 1)
  t.has(hints[1], "myco.ghe.com/janesmith " .. copilot.KEYRING_LOCKED .. " (access denied)")
  t.eq(copilot.decode_go_keyring("gho_plain"), "gho_plain")
  t.eq(copilot.decode_go_keyring("go-keyring-base64:!!!"), "go-keyring-base64:!!!")
end)

t.case("config_files_follow_xdg_config_home", function()
  local dir = t.mktmpdir("copilot")
  makima.fs.mkdir(dir .. "/github-copilot", { parents = true })
  makima.fs.mkdir(dir .. "/gh", { parents = true })
  makima.fs.write(dir .. "/github-copilot/hosts.json", '{"github.com": {"oauth_token": "from-json"}}')
  makima.fs.write(dir .. "/gh/hosts.yml", "myco.ghe.com:\n  oauth_token: from-yaml\n  user: octocat\n")
  local files
  t.with_env({ XDG_CONFIG_HOME = dir }, function()
    files = copilot.config_files()
  end)
  t.rmtree(dir)
  t.eq(#files, 2)
  t.eq(files[1]["github.com"].oauth_token, "from-json")
  t.eq(files[2]["myco.ghe.com"].user, "octocat")
end)

t.case("login_imports_once_and_status_reports_the_source", function()
  local login = spec.auth[1].login
  local notes = {}
  local ui = {
    instructions = function(_, text)
      notes[#notes + 1] = text
      return true
    end,
  }
  t.with_env({ [TOKEN_ENV] = "tok" }, function()
    local ctx = t.provider_ctx({})
    assert(login(ctx, ui, {}))
    t.eq(ctx.saved, nil)
    t.same(spec.auth_status(ctx, {}), { state = "env", detail = "via " .. TOKEN_ENV })
  end)
  t.eq(notes[1], copilot.ALREADY_AUTHENTICATED)
  local config_files, keyring_get = copilot.config_files, makima.host.keyring_get
  copilot.config_files = function()
    return { { ["myco.ghe.com"] = { oauth_token = "ghe" } } }
  end
  local ctx = t.provider_ctx({})
  t.with_env({}, function()
    assert(login(ctx, ui, {}))
    t.same(spec.auth_status(ctx, {}), { state = "saved", detail = "myco.ghe.com" })
  end)
  t.same(ctx.saved, { api_key = "ghe", host = "myco.ghe.com" })
  copilot.config_files = function()
    return {}
  end
  makima.host.keyring_get = function() end
  local empty = t.provider_ctx({})
  local ok, err
  t.with_env({}, function()
    ok, err = login(empty, ui, {})
    t.same(spec.auth_status(empty, {}), { state = "missing" })
  end)
  copilot.config_files, makima.host.keyring_get = config_files, keyring_get
  t.eq(ok, nil)
  t.eq(err, copilot.NOT_FOUND)
  t.eq(empty.saved, nil)
end)

t.case("prepare_without_a_token_is_a_config_error", function()
  t.with_env({}, function()
    local instance, err = spec.prepare(t.provider_ctx({}), { id = "gpt-5" }, {})
    t.eq(instance, nil)
    t.same(err, http.config_error(copilot.NOT_AUTHENTICATED))
  end)
end)

t.report()
