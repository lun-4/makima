local http = require("providers.http")
local openai = require("providers.openai")
local t = require("maki.test_helpers")

local KEY_ENV = "OPENAI_API_KEY"
local API = "https://api.openai.com/v1"
local PROXY = "https://proxy.example.invalid/v1"
local ACCOUNT = "account-a"
local OTHER_ACCOUNT = "account-b"
local HOUR_MS = 3600000
local DONE = "data: [DONE]\n\n"
local TRANSPORT_ERROR = "could not connect: connection refused"

local spec = openai.provider()

local function now_ms()
  local now = maki.time.now()
  return now.secs * 1000 + math.floor(now.nanosecs / 1000000)
end

local function jwt(claims)
  local payload = maki.base64.encode(assert(maki.json.encode(claims))):gsub("%+", "-"):gsub("/", "_"):gsub("=", "")
  return "e30." .. payload .. ".sig"
end

local function tokens(overrides)
  local result = { access = "access-1", refresh = "refresh-1", expires = now_ms() + HOUR_MS, account_id = ACCOUNT }
  for key, value in pairs(overrides or {}) do
    result[key] = value
  end
  return result
end

local function json(value, status)
  return t.response({ assert(maki.json.encode(value)) }, { status = status })
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

local function request(id, extra)
  local result = {
    model = { id = id, output_tokens = 8000, max_output_tokens = 128000, context_window = 272000 },
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

local function prepared(env, info, saved, responses, config)
  local ctx = t.provider_ctx(responses or {}, saved)
  local instance, model
  t.with_env(env, function()
    instance, model = spec.prepare(ctx, info, config or {})
  end)
  return ctx, assert(instance, model), model
end

local function stream(env, info, saved, responses, extra, config)
  local ctx, instance, model = prepared(env, info, saved, responses, config)
  local result, err
  t.with_env(env, function()
    result, err = instance.stream(ctx, request(info.id, extra), t.recorder())
  end)
  return ctx, result, err, model
end

local function body(sent)
  return maki.json.decode(sent.body)
end

local function plan_data(fast, account, efforts)
  return { efforts = efforts or { "high" }, adaptive = "high", off = false, fast = fast, account_id = account }
end

local PLAN_MODELS_RESPONSE = {
  models = {
    {
      slug = "gpt-7-nova",
      visibility = "list",
      context_window = 300000,
      default_reasoning_level = "low",
      supported_reasoning_levels = {
        { effort = "low" },
        { effort = "medium" },
        { effort = "max" },
        { effort = "ultra" },
      },
      input_modalities = { "text", "image" },
    },
    {
      slug = "gpt-5.5",
      visibility = "list",
      context_window = 272000,
      supported_reasoning_levels = { { effort = "none" }, { effort = "high" } },
      input_modalities = { "text" },
    },
    { slug = "gpt-5.1-codex", visibility = "hide", supported_reasoning_levels = {} },
  },
}

t.case("curated_models_keep_tier_pricing_and_window", function()
  local by_id = {}
  for _, model in ipairs(spec.models) do
    by_id[model.id] = model
  end
  for _, case in ipairs({
    { "gpt-5.6-luna", "weak", 1.0, 0.1, 1.25, 6.0, 372000 },
    { "gpt-5.6-terra", "medium", 2.5, 0.25, 3.125, 15.0, 372000 },
    { "gpt-5.6-sol", "strong", 5.0, 0.5, 6.25, 30.0, 372000 },
    { "gpt-6.1-sol", "medium", 2.0, 0.1, 2.5, 10.0, 500000 },
    { "gpt-6.1-terra", "medium", 1.0, 0.05, 1.25, 5.0, 500000 },
    { "gpt-6-astra", "strong", 10.0, 1.0, 12.5, 50.0, 272000 },
    { "gpt-6-sol", "medium", 2.0, 0.2, 2.5, 10.0, 272000 },
    { "gpt-6-luna", "weak", 0.1, 0.01, 0.125, 0.5, 272000 },
  }) do
    local model = assert(by_id[case[1]], case[1])
    t.eq(model.tier, case[2], case[1])
    t.same(model.pricing, { input = case[3], cache_read = case[4], cache_write = case[5], output = case[6] })
    t.eq(model.context_window, case[7], case[1])
  end
  local defaults = {}
  for _, model in ipairs(spec.models) do
    defaults[model.tier] = defaults[model.tier] or model.id
  end
  t.same(defaults, { weak = "gpt-5.6-luna", medium = "gpt-5.6-terra", strong = "gpt-5.6-sol" })
end)

t.case("plan_window_matches_plan_models", function()
  for id, expected in pairs({
    ["gpt-5.6-luna"] = 372000,
    ["gpt-5.6-sol"] = 372000,
    ["gpt-6.1-sol"] = 500000,
    ["gpt-6.1-terra"] = 500000,
    ["gpt-6.1-terra-codex"] = 500000,
    ["gpt-6.1-sol-codex"] = 500000,
    ["gpt-6-astra"] = 272000,
    ["gpt-6-luna"] = 272000,
    ["gpt-5.5"] = 272000,
    ["gpt-5.3-codex"] = 272000,
    ["gpt-5.7-codex"] = 272000,
  }) do
    t.eq(openai.plan_window(id), expected, id)
  end
  for _, id in ipairs({ "gpt-6.1-terra-preview", "gpt-5.6-terra-preview", "gpt-5.4-nano" }) do
    t.eq(openai.plan_window(id), nil, id)
  end
end)

t.case("account_id_reads_jwt_claims", function()
  t.eq(openai.account_id(jwt({ chatgpt_account_id = "acct_123" })), "acct_123")
  t.eq(
    openai.account_id(jwt({ ["https://api.openai.com/auth"] = { chatgpt_account_id = "acct_nested" } })),
    "acct_nested"
  )
  t.eq(openai.account_id(jwt({ organizations = { { id = "org_1" } } })), "org_1")
  t.eq(openai.account_id("not.a.jwt"), nil)
  t.eq(openai.account_id("invalid"), nil)
end)

t.case("plan_models_keep_listed_rows_with_declared_metadata", function()
  local models = assert(openai.parse_plan_models(PLAN_MODELS_RESPONSE, ACCOUNT))
  t.eq(#models, 2)
  t.same(models[1], {
    id = "gpt-7-nova",
    context_window = 300000,
    supports_thinking = true,
    supports_vision = true,
    provider_data = {
      efforts = { "low", "medium", "max" },
      adaptive = "low",
      off = false,
      fast = false,
      account_id = ACCOUNT,
    },
  })
  t.eq(models[2].supports_vision, false)
  t.same(models[2].provider_data, { efforts = { "high" }, off = true, fast = false, account_id = ACCOUNT })
end)

t.case("plan_models_parse_fast_capability", function()
  for _, case in ipairs({
    { {}, false },
    { { service_tiers = {} }, false },
    { { service_tiers = { { id = "flex" } } }, false },
    { { service_tiers = { { id = "priority" } } }, true },
    { { additional_speed_tiers = { "fast" } }, false },
  }) do
    local entry = case[1]
    entry.slug, entry.visibility = "gpt-7-nova", "list"
    local models = assert(openai.parse_plan_models({ models = { entry } }, ACCOUNT))
    t.eq(models[1].provider_data.fast, case[2])
  end
end)

t.case("plan_models_reject_responses_without_visible_models", function()
  for _, response in ipairs({ {}, { models = { { slug = "x", visibility = "hide" } } } }) do
    local models, err = openai.parse_plan_models(response, ACCOUNT)
    t.eq(models, nil)
    t.same(err, http.config_error(openai.EMPTY_MODELS_ERROR))
  end
end)

t.case("fast_is_bound_to_the_logged_in_account", function()
  for _, case in ipairs({
    { tokens(), plan_data(true, ACCOUNT), true },
    { tokens(), plan_data(false, ACCOUNT), false },
    { tokens(), plan_data(true, OTHER_ACCOUNT), false },
    { tokens({ account_id = false }), plan_data(true, ACCOUNT), false },
    { tokens(), plan_data(true, nil), false },
    { tokens({ account_id = "" }), plan_data(true, ""), false },
  }) do
    local saved = case[1]
    saved.account_id = saved.account_id or nil
    local ctx, result, err, model = stream(
      {},
      { id = "gpt-7-nova", provider_data = case[2] },
      saved,
      { responses_sse("ok") },
      { fast = true }
    )
    assert(result, err)
    t.eq(model.supports_fast, case[3])
    t.eq(body(ctx.requests[1]).service_tier, case[3] and "priority" or nil)
  end
  local _, _, _, model = stream(
    { [KEY_ENV] = "sk" },
    { id = "gpt-7-nova", provider_data = plan_data(true, ACCOUNT) },
    nil,
    {
      responses_sse("ok"),
    }
  )
  t.eq(model.supports_fast, nil)
end)

t.case("fast_is_pending_until_the_plan_listing_answers", function()
  local fresh = openai.provider()
  local unlisted = { id = "gpt-7-nova" }
  local ctx = t.provider_ctx({ responses_sse("ok") }, tokens())
  local instance, model = fresh.prepare(ctx, unlisted, {})
  assert(instance, model)
  t.eq(model.supports_fast, openai.FAST_PENDING)
  local result, err = instance.stream(ctx, request(unlisted.id, { fast = true }), t.recorder())
  assert(result, err)
  t.eq(body(ctx.requests[1]).service_tier, nil)

  assert(fresh.discover(t.provider_ctx({ t.response({ "down" }, { status = 503 }) }, tokens()), {}))
  local _, after = fresh.prepare(t.provider_ctx({}, tokens()), unlisted, {})
  t.eq(after.supports_fast, false)
end)

t.case("fast_off_keeps_the_body_unchanged", function()
  local info = { id = "gpt-7-nova", provider_data = plan_data(true, ACCOUNT) }
  local fast_ctx = stream({}, info, tokens(), { responses_sse("ok") }, { fast = true })
  local plain_ctx = stream({}, info, tokens(), { responses_sse("ok") })
  local fast_body, plain_body = body(fast_ctx.requests[1]), body(plain_ctx.requests[1])
  t.eq(fast_body.service_tier, "priority")
  fast_body.service_tier = nil
  t.same(fast_body, plain_body)
end)

t.case("discovered_dialect_speaks_declared_levels", function()
  local models = assert(openai.parse_plan_models(PLAN_MODELS_RESPONSE, ACCOUNT))
  for _, case in ipairs({
    { models[1], { kind = "adaptive" }, "low" },
    { models[1], { kind = "effort", level = "xhigh" }, "medium" },
    { models[1], { kind = "off" }, nil },
    { models[2], { kind = "off" }, "none" },
  }) do
    local ctx = stream({}, case[1], tokens(), { responses_sse("ok") }, { thinking = case[2] })
    local reasoning = body(ctx.requests[1]).reasoning
    t.eq(reasoning and reasoning.effort, case[3])
  end
end)

t.case("api_key_chat_request", function()
  local ctx, result = stream({ [KEY_ENV] = "sk-1" }, { id = "gpt-4.1" }, nil, { chat_sse("ok") }, {
    thinking = { kind = "effort", level = "xhigh" },
  })
  t.eq(result.message.content[1].text, "ok")
  local sent = ctx.requests[1]
  t.eq(sent.url, API .. "/chat/completions")
  t.eq(header(sent.headers, "authorization"), "Bearer sk-1")
  local sent_body = body(sent)
  t.eq(sent_body.max_completion_tokens, 128000)
  t.eq(sent_body.stream_options.include_usage, true)
  t.eq(sent_body.reasoning_effort, "high")
end)

t.case("api_key_plan_models_use_responses_at_the_platform_api", function()
  local env = { [KEY_ENV] = "sk-1", OPENAI_BASE_URL = PROXY }
  local ctx = stream(env, { id = "gpt-5.3-codex" }, nil, { responses_sse("ok") })
  local sent = ctx.requests[1]
  t.eq(sent.url, PROXY .. "/responses")
  t.eq(header(sent.headers, "chatgpt-account-id"), nil)
  ctx = stream(
    { [KEY_ENV] = "sk-1" },
    { id = "gpt-5.3-codex" },
    nil,
    { responses_sse("ok") },
    nil,
    { base_url = PROXY }
  )
  t.eq(ctx.requests[1].url, PROXY .. "/responses")
end)

t.case("chatgpt_login_uses_the_fixed_backend_and_account_header", function()
  local config = { base_url = PROXY, headers = { ["X-Gateway"] = "${GATEWAY}" } }
  local env = { OPENAI_BASE_URL = PROXY, GATEWAY = "gw" }
  local ctx = stream(env, { id = "gpt-5.5" }, tokens(), { responses_sse("ok") }, nil, config)
  local sent = ctx.requests[1]
  t.eq(sent.url, openai.BACKEND_URL .. "/responses")
  t.eq(header(sent.headers, "authorization"), "Bearer access-1")
  t.eq(header(sent.headers, "chatgpt-account-id"), ACCOUNT)
  t.eq(header(sent.headers, "x-gateway"), "gw")
  ctx = stream(env, { id = "gpt-4.1" }, tokens(), { chat_sse("ok") }, nil, config)
  sent = ctx.requests[1]
  t.eq(sent.url, PROXY .. "/chat/completions")
  t.eq(header(sent.headers, "authorization"), "Bearer access-1")
  t.eq(header(sent.headers, "chatgpt-account-id"), nil)
end)

t.case("chatgpt_tokens_win_over_the_api_key_and_clamp_plan_windows", function()
  local ctx, _, _, model = stream({ [KEY_ENV] = "sk-1" }, { id = "gpt-5.5" }, tokens(), { responses_sse("ok") })
  t.eq(header(ctx.requests[1].headers, "authorization"), "Bearer access-1")
  t.eq(model.context_window, 272000)
  local _, _, key_model = prepared({ [KEY_ENV] = "sk-1" }, { id = "gpt-5.5" }, nil)
  t.eq(key_model.context_window, 1050000)
end)

t.case("expired_tokens_refresh_before_the_request", function()
  local refreshed = {
    access_token = "access-2",
    refresh_token = "refresh-2",
    expires_in = 60,
    id_token = jwt({ chatgpt_account_id = "acct_new" }),
  }
  local ctx, result = stream({}, { id = "gpt-5.5" }, tokens({ expires = 0 }), { json(refreshed), responses_sse("ok") })
  assert(result)
  t.eq(ctx.requests[1].url, openai.TOKEN_URL)
  t.has(ctx.requests[1].body, "grant_type=refresh_token&refresh_token=refresh-1&client_id=")
  t.eq(header(ctx.requests[2].headers, "authorization"), "Bearer access-2")
  t.eq(ctx.saved.access, "access-2")
  t.eq(ctx.saved.refresh, "refresh-2")
  t.eq(ctx.saved.account_id, "acct_new")
end)

t.case("rejected_token_refreshes_once_and_retries", function()
  local refreshed = { access_token = "access-2", refresh_token = "refresh-2" }
  local ctx, result = stream({}, { id = "gpt-5.5" }, tokens(), {
    t.response({ "expired" }, { status = 401 }),
    json(refreshed),
    responses_sse("ok"),
  })
  assert(result)
  t.eq(#ctx.requests, 3)
  t.eq(header(ctx.requests[3].headers, "authorization"), "Bearer access-2")
end)

t.case("refresh_skips_tokens_another_process_replaced", function()
  local ctx, instance = prepared(
    {},
    { id = "gpt-5.5" },
    tokens(),
    { t.response({ "no" }, { status = 401 }), responses_sse("ok") }
  )
  ctx.saved = tokens({ access = "access-other" })
  assert(instance.stream(ctx, request("gpt-5.5"), t.recorder()))
  t.eq(#ctx.requests, 2)
  t.eq(header(ctx.requests[2].headers, "authorization"), "Bearer access-other")
end)

t.case("refresh_keeps_a_key_another_process_saved", function()
  local ctx, instance = prepared({}, { id = "gpt-4.1" }, tokens({ expires = 0 }), { chat_sse("ok") })
  ctx.saved = { api_key = "sk-saved" }
  assert(instance.stream(ctx, request("gpt-4.1"), t.recorder()))
  t.same(ctx.saved, { api_key = "sk-saved" })
  t.eq(header(ctx.requests[1].headers, "authorization"), "Bearer sk-saved")
end)

t.case("failed_refresh_deletes_tokens_and_falls_back_to_the_api_key", function()
  local ctx, result = stream({ [KEY_ENV] = "sk-1" }, { id = "gpt-4.1" }, tokens({ expires = 0 }), {
    t.response({ "revoked" }, { status = 400 }),
    chat_sse("ok"),
  })
  assert(result)
  t.eq(ctx.saved, nil)
  t.eq(header(ctx.requests[2].headers, "authorization"), "Bearer sk-1")
  local no_key_ctx, no_result, err = stream({}, { id = "gpt-4.1" }, tokens({ expires = 0 }), {
    t.response({ "revoked" }, { status = 400 }),
  })
  t.eq(no_result, nil)
  t.eq(err.status, 400)
  t.eq(no_key_ctx.saved, nil)
end)

t.case("refresh_that_never_reached_the_server_keeps_the_tokens", function()
  local saved = tokens({ expires = 0 })
  local ctx, instance = prepared({ [KEY_ENV] = "sk-1" }, { id = "gpt-4.1" }, saved)
  ctx.send = function(sent)
    ctx.requests[#ctx.requests + 1] = sent
    return nil, TRANSPORT_ERROR
  end
  local result, err = instance.stream(ctx, request("gpt-4.1"), t.recorder())
  t.eq(result, nil)
  t.has(tostring(err), TRANSPORT_ERROR)
  t.eq(#ctx.requests, 1)
  t.eq(ctx.saved, saved)
end)

t.case("device_login_polls_at_the_server_interval_and_saves_tokens", function()
  local slept = {}
  local sleep = openai.sleep
  openai.sleep = function(seconds)
    slept[#slept + 1] = seconds
  end
  local ctx = t.provider_ctx({
    json({ device_auth_id = "dev_abc", user_code = "ABCD-EFGH", interval = "7" }),
    t.response({}, { status = 403 }),
    json({ authorization_code = "code-1", code_verifier = "verifier/1" }),
    json({ access_token = "access-1", refresh_token = "refresh-1", id_token = jwt({ chatgpt_account_id = ACCOUNT }) }),
  })
  local shown = {}
  local ui = {
    open_browser = function(_, url)
      shown[#shown + 1] = url
      return true
    end,
    device_code = function(_, url, code)
      shown[#shown + 1] = url .. " " .. code
      return true
    end,
  }
  local chatgpt
  for _, method in ipairs(spec.auth) do
    chatgpt = method.id == "chatgpt" and method or chatgpt
  end
  local ok, err = chatgpt.login(ctx, ui, {})
  openai.sleep = sleep
  assert(ok, err)
  t.same(slept, { 10, 10 })
  t.same(shown, { openai.DEVICE_URL, openai.DEVICE_URL .. " ABCD-EFGH" })
  t.eq(ctx.requests[1].url, openai.DEVICE_CODE_URL)
  t.same(body(ctx.requests[2]), { device_auth_id = "dev_abc", user_code = "ABCD-EFGH" })
  t.has(ctx.requests[4].body, "grant_type=authorization_code&code=code-1&redirect_uri=")
  t.has(ctx.requests[4].body, "code_verifier=verifier%2F1")
  t.eq(ctx.saved.access, "access-1")
  t.eq(ctx.saved.account_id, ACCOUNT)
end)

t.case("device_login_saves_nothing_when_cancelled_or_failed", function()
  local sleep = openai.sleep
  openai.sleep = function() end
  local chatgpt = spec.auth[2]
  t.eq(chatgpt.id, "chatgpt")
  local cancelled_ui = {
    open_browser = function()
      return nil, "cancelled"
    end,
  }
  local ctx = t.provider_ctx({ json({ device_auth_id = "d", user_code = "c", interval = 1 }) }, { api_key = "old" })
  local ok, err = chatgpt.login(ctx, cancelled_ui, {})
  t.eq(ok, nil)
  t.eq(err, "cancelled")
  t.same(ctx.saved, { api_key = "old" })
  local ui = {
    open_browser = function()
      return true
    end,
    device_code = function()
      return true
    end,
  }
  ctx = t.provider_ctx({
    json({ device_auth_id = "d", user_code = "c" }),
    t.response({ "denied" }, { status = 500 }),
  })
  ok, err = chatgpt.login(ctx, ui, {})
  openai.sleep = sleep
  t.eq(ok, nil)
  t.eq(err.status, 500)
  t.eq(ctx.saved, nil)
end)

t.case("usage_parses_quota_windows", function()
  local usage = assert(openai.parse_usage({
    plan_type = "pro",
    rate_limit = {
      primary_window = { used_percent = 12.6, limit_window_seconds = 18000, reset_at = 1760000000 },
      secondary_window = { used_percent = 120, limit_window_seconds = 604800, reset_at = 1760100000 },
    },
  }))
  t.same(usage, {
    plan = "pro",
    limits = {
      { kind = { Hours = 5 }, percentage = 13, reset_at = 1760000000000 },
      { kind = { Weekly = {} }, percentage = 100, reset_at = 1760100000000 },
    },
  })
  usage = assert(openai.parse_usage({
    rate_limit = {
      primary_window = { used_percent = 10 },
      secondary_window = { used_percent = -2, limit_window_seconds = 86400 },
    },
  }))
  t.same(usage.limits, { { kind = { Days = 1 }, percentage = 0 } })
  for _, response in ipairs({ {}, { rate_limit = {} } }) do
    local empty, err = openai.parse_usage(response)
    t.eq(empty, nil)
    t.same(err, http.config_error(openai.EMPTY_USAGE_ERROR))
  end
end)

t.case("usage_is_for_chatgpt_logins_only", function()
  local ctx, instance = prepared({ [KEY_ENV] = "sk" }, { id = "gpt-5.5" }, nil)
  t.eq(instance.fetch_usage(ctx), nil)
  t.eq(#ctx.requests, 0)
  ctx, instance = prepared({}, { id = "gpt-5.5" }, tokens(), { json({ plan_type = "plus" }) })
  t.same(assert(instance.fetch_usage(ctx)), { plan = "plus", limits = {} })
  t.eq(ctx.requests[1].url, openai.USAGE_URL)
  t.eq(header(ctx.requests[1].headers, "chatgpt-account-id"), ACCOUNT)
end)

t.case("discovery_lists_the_account_plan_models", function()
  local ctx = t.provider_ctx({ json(PLAN_MODELS_RESPONSE) }, tokens())
  local models = assert(spec.discover(ctx, {}))
  t.eq(ctx.requests[1].url, openai.BACKEND_URL .. "/models?client_version=" .. openai.CODEX_CLIENT_VERSION)
  t.eq(header(ctx.requests[1].headers, "chatgpt-account-id"), ACCOUNT)
  local nova
  for _, model in ipairs(models) do
    nova = model.id == "gpt-7-nova" and model or nova
  end
  t.eq(nova.provider_data.account_id, ACCOUNT)
  t.eq(#models, #spec.models + 1)
  ctx = t.provider_ctx({ t.response({ "down" }, { status = 503 }) }, tokens())
  t.eq(#assert(spec.discover(ctx, {})), #spec.models)
end)

t.case("state_setup_errors_are_reported", function()
  local config = { headers = { ["X-Gateway"] = "${MISSING_GATEWAY}" } }
  local _, discover_err = spec.discover(t.provider_ctx({}), config)
  local _, prepare_err = spec.prepare(t.provider_ctx({}), { id = "gpt-4.1" }, config)
  t.same(discover_err, prepare_err)
  t.eq(discover_err.kind, http.CONFIG_KIND)
end)

t.case("discovery_with_an_api_key_lists_platform_models", function()
  local ctx = t.provider_ctx({ json({ data = { { id = "gpt-new" }, { id = "gpt-4.1" } } }) })
  local models
  t.with_env({ [KEY_ENV] = "sk" }, function()
    models = assert(spec.discover(ctx, {}))
  end)
  t.eq(ctx.requests[1].url, API .. "/models")
  t.eq(models[#models].id, "gpt-new")
  t.eq(#models, #spec.models + 1)
end)

t.case("unreadable_oauth_falls_back_to_api_key_status", function()
  local ctx = t.provider_ctx({})
  ctx.credentials.read = function()
    return nil, "keyring locked"
  end
  t.with_env({ [KEY_ENV] = "sk" }, function()
    t.same(spec.auth_status(ctx, {}), { state = "env", detail = "via " .. KEY_ENV })
  end)
end)

t.case("login_methods_alias_and_status", function()
  t.eq(spec.auth[1].id, "api_key")
  t.eq(spec.auth[2].id, "chatgpt")
  t.same(spec.aliases, { { id = "openai-codex", label = "OpenAI Codex (ChatGPT login)", method = "chatgpt" } })
  t.same(spec.auth_status(t.provider_ctx({}, tokens()), {}), { state = "saved", detail = "oauth" })
  t.with_env({ [KEY_ENV] = "sk" }, function()
    t.same(spec.auth_status(t.provider_ctx({}), {}), { state = "env", detail = "via " .. KEY_ENV })
  end)
end)

t.report()
