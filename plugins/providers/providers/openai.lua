-- OpenAI: API keys over Chat Completions and Responses, or a ChatGPT login
-- (Codex) whose plan models talk to the ChatGPT backend.

local chat = require("providers.chat_completions")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local responses = require("providers.responses")
local thinking = require("providers.thinking")

local M = {}

M.DEVICE_URL = "https://auth.openai.com/codex/device"
M.BACKEND_URL = "https://chatgpt.com/backend-api/codex"
M.USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
M.DEVICE_CODE_URL = "https://auth.openai.com/api/accounts/deviceauth/usercode"
M.DEVICE_TOKEN_URL = "https://auth.openai.com/api/accounts/deviceauth/token"
M.TOKEN_URL = "https://auth.openai.com/oauth/token"
M.EMPTY_USAGE_ERROR = "OpenAI usage response contained no plan or rate limits; the endpoint schema likely changed"
M.EMPTY_MODELS_ERROR = "Codex models response listed no visible models; the endpoint schema likely changed"
M.TIMEOUT_ERROR = "device authorization timed out"
-- The backend hides models newer than this Codex CLI version, so bump it
-- when a fresh model is missing from the list.
M.CODEX_CLIENT_VERSION = "0.153.4"
-- Offline fallback; the backend's `/models` is the source of truth. Codex
-- models match by their `-codex` substring, so they are not listed.
M.PLAN_MODELS = {
  "gpt-6.1-sol",
  "gpt-6.1-terra",
  "gpt-6-astra",
  "gpt-6-sol",
  "gpt-6-luna",
  "gpt-5.6-luna",
  "gpt-5.6-terra",
  "gpt-5.6-sol",
  "gpt-5.5",
  "gpt-5.4",
  "gpt-5.4-mini",
  "gpt-5.2",
}

local CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
local REDIRECT_URI = "https://auth.openai.com/deviceauth/callback"
local PLAN_MODELS_PATH = "/models?client_version="
local RESPONSES_PATH = "/responses"
local CHAT_PATH = "/chat/completions"
local ACCOUNT_HEADER = "chatgpt-account-id"
local AUTH_CLAIMS = "https://api.openai.com/auth"
local JSON_TYPE = "application/json"
local FORM_TYPE = "application/x-www-form-urlencoded"
local MAX_TOKENS_FIELD = "max_completion_tokens"
local FAST_TIER = "priority"
local LISTED = "list"
local IMAGE = "image"
local CODEX_MARKER = "-codex"
local CODEX_WINDOW = 272000
local GPT_5_6_PLAN_WINDOW = 372000
-- GPT-6.1's default context on Codex went up to 500k (max 1M).
local GPT_6_1_PLAN_WINDOW = 500000
local GPT_6_1_PREFIXES = { "gpt-6.1-sol", "gpt-6.1-terra" }
local GPT_5_6_PREFIX = "gpt-5.6-"
local DEFAULT_INTERVAL = 5
local POLL_MARGIN = 3
local POLL_TIMEOUT = 300
local DEFAULT_EXPIRES_IN = 3600
local REFRESH_MARGIN_MS = 60000
local MILLIS = 1000
local NANOS_PER_MILLI = 1000000
local UNAUTHORIZED = 401
local REFRESH_REJECTED = { [400] = true, [401] = true }
local PENDING_STATUS = { [403] = true, [404] = true }
local SECONDS_PER_HOUR = 3600
local SECONDS_PER_DAY = 24 * SECONDS_PER_HOUR
local SECONDS_PER_WEEK = 7 * SECONDS_PER_DAY
local MAX_PERCENT = 100
local CHATGPT_METHOD = { id = "chatgpt", label = "ChatGPT login" }
M.FAST_PENDING = "pending"
local CODEX_ALIAS = { id = "openai-codex", label = "OpenAI Codex (ChatGPT login)", method = CHATGPT_METHOD.id }
local OAUTH_DETAIL = "oauth"
local INVALID_DEVICE_RESPONSE = "invalid OpenAI device authorization response"
local INVALID_TOKEN_RESPONSE = "invalid OpenAI token response"

local function pricing(input, output, cache_write, cache_read)
  return { input = input, output = output, cache_write = cache_write, cache_read = cache_read }
end

local function row(id, tier, price, max_output_tokens, context_window)
  return {
    id = id,
    tier = tier,
    pricing = price,
    max_output_tokens = max_output_tokens,
    context_window = context_window,
  }
end

local GPT_5_6_MAX_OUTPUT = 128000
local GPT_5_6_WINDOW = 372000
local GPT_6_WINDOW = 272000
local CODEX_MAX_OUTPUT = 128000
local CODEX_API_WINDOW = 400000

local NOTES = [[
#### Auth

You can use OpenAI two ways.

**API key**: set `OPENAI_API_KEY`, or pick the API key method in `/login` and paste a key from [platform.openai.com/api-keys](https://platform.openai.com/api-keys).

**ChatGPT subscription**: use Codex through your ChatGPT login. Run `makima auth login openai-codex`, or pick the ChatGPT login method in `/login`. Makima walks you through the device flow at [auth.openai.com/codex](https://auth.openai.com/codex) and saves the tokens, which win over the API key. `OPENAI_BASE_URL` only redirects the API key path, never the ChatGPT backend.

With a ChatGPT login the model list comes from the Codex backend's own `/models`, so a model your plan gains shows up without a Makima update, with the context window and reasoning levels the backend declares. The table above is the offline fallback. The backend hides models newer than the Codex CLI version Makima reports, so a brand new release can lag until that version is bumped.]]

-- The first row of a tier is its default.
M.def = {
  id = "openai",
  display_name = "OpenAI",
  env = "OPENAI_API_KEY",
  base_url = "https://api.openai.com/v1",
  login_url = "https://platform.openai.com/api-keys",
  model = "gpt-5.5",
  notes = NOTES,
  max_tokens_field = MAX_TOKENS_FIELD,
  include_stream_usage = true,
  defaults = {
    context_window = 200000,
    max_output_tokens = 100000,
    supports_thinking = true,
    supports_vision = true,
    supports_tool_examples = true,
  },
  models = {
    row("gpt-5.6-luna", "weak", pricing(1.00, 6.00, 1.25, 0.10), GPT_5_6_MAX_OUTPUT, GPT_5_6_WINDOW),
    row("gpt-5.6-terra", "medium", pricing(2.50, 15.00, 3.125, 0.25), GPT_5_6_MAX_OUTPUT, GPT_5_6_WINDOW),
    row("gpt-5.6-sol", "strong", pricing(5.00, 30.00, 6.25, 0.50), GPT_5_6_MAX_OUTPUT, GPT_5_6_WINDOW),
    row("gpt-6.1-sol", "medium", pricing(2.00, 10.00, 2.50, 0.10), GPT_5_6_MAX_OUTPUT, GPT_6_1_PLAN_WINDOW),
    row("gpt-6.1-terra", "medium", pricing(1.00, 5.00, 1.25, 0.05), GPT_5_6_MAX_OUTPUT, GPT_6_1_PLAN_WINDOW),
    row("gpt-6-astra", "strong", pricing(10.00, 50.00, 12.50, 1.00), GPT_5_6_MAX_OUTPUT, GPT_6_WINDOW),
    row("gpt-6-sol", "medium", pricing(2.00, 10.00, 2.50, 0.20), GPT_5_6_MAX_OUTPUT, GPT_6_WINDOW),
    row("gpt-6-luna", "weak", pricing(0.10, 0.50, 0.125, 0.01), GPT_5_6_MAX_OUTPUT, GPT_6_WINDOW),
    row("gpt-5.4-nano", "weak", pricing(0.20, 1.25, 0, 0.02), 128000, 400000),
    row("gpt-5.4-mini", "weak", pricing(0.75, 4.50, 0, 0.075), 128000, 400000),
    row("gpt-4.1-nano", "weak", pricing(0.10, 0.40, 0, 0.025), 32768, 1047576),
    row("gpt-4.1-mini", "medium", pricing(0.40, 1.60, 0, 0.10), 32768, 1047576),
    row("gpt-4.1", "medium", pricing(2.00, 8.00, 0, 0.50), 32768, 1047576),
    row("o4-mini", "medium", pricing(1.10, 4.40, 0, 0.275), 100000, 200000),
    row("gpt-5.5", "strong", pricing(5.00, 30.00, 0, 0.50), 128000, 1050000),
    row("gpt-5.4", "strong", pricing(2.50, 15.00, 0, 0.25), 128000, 1050000),
    row("o3", "strong", pricing(2.00, 8.00, 0, 1.00), 100000, 200000),
    row("gpt-5.3-codex", "strong", pricing(1.75, 14.00, 0, 0.175), CODEX_MAX_OUTPUT, CODEX_API_WINDOW),
    row("gpt-5.2-codex", "strong", pricing(1.75, 14.00, 0, 0.175), CODEX_MAX_OUTPUT, CODEX_API_WINDOW),
    row("gpt-5.1-codex-mini", "medium", pricing(0.25, 2.00, 0, 0.025), CODEX_MAX_OUTPUT, CODEX_API_WINDOW),
    row("gpt-5.1-codex-max", "strong", pricing(1.25, 10.00, 0, 0.125), CODEX_MAX_OUTPUT, CODEX_API_WINDOW),
    row("gpt-5.1-codex", "strong", pricing(1.25, 10.00, 0, 0.125), CODEX_MAX_OUTPUT, CODEX_API_WINDOW),
  },
}

-- Waits {seconds} without blocking the Lua thread. Specs replace it.
function M.sleep(seconds)
  maki.async.await(1, function(done)
    maki.defer_fn(done, math.ceil(seconds * MILLIS))
  end)
end

local function now_ms()
  local now = maki.time.now()
  return now.secs * MILLIS + math.floor(now.nanosecs / NANOS_PER_MILLI)
end

local function starts_with(text, prefix)
  return text:sub(1, #prefix) == prefix
end

-- The context window a ChatGPT plan gives {id}, or nil when the model is not
-- a plan model. Codex models match by substring so future releases route
-- without an edit; named plan models match exactly so `gpt-5.6-terra-preview`
-- does not.
function M.plan_window(id)
  local gpt_6_1 = starts_with(id, GPT_6_1_PREFIXES[1]) or starts_with(id, GPT_6_1_PREFIXES[2])
  if id:find(CODEX_MARKER, 1, true) then
    return gpt_6_1 and GPT_6_1_PLAN_WINDOW or CODEX_WINDOW
  end
  if not table.find(M.PLAN_MODELS, id) then
    return nil
  end
  if gpt_6_1 then
    return GPT_6_1_PLAN_WINDOW
  elseif starts_with(id, GPT_5_6_PREFIX) then
    return GPT_5_6_PLAN_WINDOW
  end
  return CODEX_WINDOW
end

-- The ChatGPT account a JWT names, from its claims.
function M.account_id(token)
  local payload = type(token) == "string" and token:match("^[^.]+%.([^.]+)%.[^.]+$")
  if not payload then
    return nil
  end
  payload = payload:gsub("%-", "+"):gsub("_", "/")
  local ok, decoded = pcall(maki.base64.decode, payload .. string.rep("=", -#payload % 4))
  local claims = ok and maki.json.decode(decoded)
  if type(claims) ~= "table" then
    return nil
  end
  local auth = type(claims[AUTH_CLAIMS]) == "table" and claims[AUTH_CLAIMS] or {}
  local org = type(claims.organizations) == "table" and claims.organizations[1]
  local candidates = { claims.chatgpt_account_id, auth.chatgpt_account_id, type(org) == "table" and org.id }
  for i = 1, 3 do
    if type(candidates[i]) == "string" then
      return candidates[i]
    end
  end
end

local function oauth_tokens(saved)
  if type(saved) == "table" and type(saved.access) == "string" and type(saved.refresh) == "string" then
    return saved
  end
end

local function expired(tokens)
  return type(tokens.expires) ~= "number" or now_ms() + REFRESH_MARGIN_MS >= tokens.expires
end

local function post(ctx, url, content_type, body)
  local response, err =
    http.send(ctx, { url = url, method = "POST", headers = { ["Content-Type"] = content_type }, body = body })
  if not response then
    return nil, err
  end
  return http.read_json(response)
end

local function token_grant(ctx, fields)
  local value, err = post(ctx, M.TOKEN_URL, FORM_TYPE, http.form(fields))
  if err then
    return nil, err
  end
  if type(value) ~= "table" or type(value.access_token) ~= "string" or type(value.refresh_token) ~= "string" then
    return nil, INVALID_TOKEN_RESPONSE
  end
  local expires_in = type(value.expires_in) == "number" and value.expires_in or DEFAULT_EXPIRES_IN
  return {
    access = value.access_token,
    refresh = value.refresh_token,
    expires = now_ms() + expires_in * MILLIS,
    account_id = M.account_id(value.id_token) or M.account_id(value.access_token),
  }
end

-- Polls once: the grant, false while the user has not approved yet, or nil
-- plus an error.
local function poll(ctx, body)
  local response, err = ctx.send({
    url = M.DEVICE_TOKEN_URL,
    method = "POST",
    headers = { ["Content-Type"] = JSON_TYPE },
    body = body,
  })
  if not response then
    return nil, err
  end
  if PENDING_STATUS[response.status] then
    response:close()
    return false
  end
  if response.status ~= 200 then
    return nil, http.error(response)
  end
  local grant, read_err = http.read_json(response)
  if read_err then
    return nil, read_err
  end
  if type(grant) ~= "table" or type(grant.authorization_code) ~= "string" or type(grant.code_verifier) ~= "string" then
    return nil, INVALID_DEVICE_RESPONSE
  end
  return grant
end

-- The device flow: the user approves the code in a browser while this polls
-- at the server's interval. Nothing is stored until the tokens arrive, so a
-- cancelled login leaves the credentials as they were.
local function chatgpt_login(ctx, ui)
  local device, err = post(ctx, M.DEVICE_CODE_URL, JSON_TYPE, assert(maki.json.encode({ client_id = CLIENT_ID })))
  if err then
    return nil, err
  end
  if type(device) ~= "table" or type(device.device_auth_id) ~= "string" or type(device.user_code) ~= "string" then
    return nil, INVALID_DEVICE_RESPONSE
  end
  local interval = math.max(tonumber(device.interval) or DEFAULT_INTERVAL, 1) + POLL_MARGIN
  local opened, open_err = ui:open_browser(M.DEVICE_URL)
  if not opened then
    return nil, open_err
  end
  local shown, show_err = ui:device_code(M.DEVICE_URL, device.user_code)
  if not shown then
    return nil, show_err
  end
  local body = assert(maki.json.encode({ device_auth_id = device.device_auth_id, user_code = device.user_code }))
  local deadline = now_ms() + POLL_TIMEOUT * MILLIS
  while now_ms() < deadline do
    M.sleep(interval)
    local grant, poll_err = poll(ctx, body)
    if grant then
      local tokens, grant_err = token_grant(ctx, {
        { "grant_type", "authorization_code" },
        { "code", grant.authorization_code },
        { "redirect_uri", REDIRECT_URI },
        { "client_id", CLIENT_ID },
        { "code_verifier", grant.code_verifier },
      })
      if not tokens then
        return nil, grant_err
      end
      local _, write_err = ctx.credentials:write(tokens)
      if write_err then
        return nil, write_err
      end
      return true
    elseif grant == nil then
      return nil, poll_err
    end
  end
  return nil, M.TIMEOUT_ERROR
end

-- Request auth for one instance: ChatGPT `tokens`, or a `pool` of API keys.
local function load_auth(ctx, config, state)
  local saved, err = ctx.credentials:read()
  if err then
    return nil, err
  end
  state.tokens, state.pool = oauth_tokens(saved), nil
  if not state.tokens then
    local keys, key_err = http.api_keys(ctx, config, M.def.env)
    if not keys then
      return nil, key_err
    end
    state.pool = http.key_pool(keys)
  end
  return state
end

-- Swaps expired (or, with {force}, rejected) tokens for fresh ones under the
-- credential lock. Credentials another process already replaced are taken
-- as they are. When the token endpoint rejects the refresh token, the stale
-- tokens are deleted and an API key, if there is one, takes over; otherwise
-- the refresh error is returned. Any other failure keeps the tokens.
local function fresh(ctx, config, state, force)
  if not state.tokens or not (force or expired(state.tokens)) then
    return true
  end
  local stale = state.tokens.access
  local tokens, err = ctx.credentials:refresh(function(current)
    current = oauth_tokens(current)
    if not current or (current.access ~= stale and not expired(current)) then
      return nil
    end
    return token_grant(ctx, {
      { "grant_type", "refresh_token" },
      { "refresh_token", current.refresh },
      { "client_id", CLIENT_ID },
    })
  end)
  if oauth_tokens(tokens) then
    state.tokens = tokens
    return true
  end
  if err then
    if not (type(err) == "table" and REFRESH_REJECTED[err.status]) then
      return nil, err
    end
    ctx.credentials:delete()
  end
  local loaded, load_err = load_auth(ctx, config, state)
  if not loaded then
    return nil, err or load_err
  end
  return true
end

local function headers(state, extra, plan)
  local out = table.clone(extra or {})
  local tokens = state.tokens
  out.Authorization = "Bearer " .. (tokens and tokens.access or state.pool.current())
  if plan and tokens and tokens.account_id then
    out[ACCOUNT_HEADER] = tokens.account_id
  end
  for name, value in pairs(state.configured) do
    http.set_header(out, name, value)
  end
  return out
end

-- Runs {call} with fresh auth, and once more after a refresh when ChatGPT
-- rejects the token.
local function authorized(ctx, config, state, call)
  local ok, err = fresh(ctx, config, state)
  if not ok then
    return nil, err
  end
  local result, call_err = call()
  if state.tokens and type(call_err) == "table" and call_err.status == UNAUTHORIZED then
    if fresh(ctx, config, state, true) then
      return call()
    end
  end
  return result, call_err
end

local function get_json(ctx, url, request_headers)
  local response, err = http.send(ctx, { url = url, headers = request_headers })
  if not response then
    return nil, err
  end
  return http.read_json(response)
end

-- Fast is a subscription perk: it takes a ChatGPT login and a listing that
-- was fetched for that same account and offers the priority tier.
local function fast_eligible(model, tokens)
  local plan = model.provider_data
  local account = tokens and tokens.account_id
  return type(plan) == "table"
    and plan.fast == true
    and type(account) == "string"
    and account ~= ""
    and plan.account_id == account
end

local function plan_model(row, account_id)
  local levels = {}
  for _, level in ipairs(type(row.supported_reasoning_levels) == "table" and row.supported_reasoning_levels or {}) do
    levels[#levels + 1] = type(level) == "table" and level.effort or nil
  end
  local efforts, off = thinking.parse_levels(levels)
  local fast = false
  for _, tier in ipairs(type(row.service_tiers) == "table" and row.service_tiers or {}) do
    fast = fast or (type(tier) == "table" and tier.id == FAST_TIER)
  end
  local vision
  local modalities = type(row.input_modalities) == "table" and row.input_modalities or {}
  if #modalities > 0 then
    vision = table.find(modalities, IMAGE) ~= nil
  end
  local default = row.default_reasoning_level
  return {
    id = row.slug,
    context_window = compat.limit(row.context_window),
    supports_thinking = #efforts > 0,
    supports_vision = vision,
    -- The account rides along so fast can tell whether the listing still
    -- belongs to whoever is logged in.
    provider_data = {
      efforts = efforts,
      adaptive = table.find(thinking.LEVELS, default) and default or nil,
      off = off,
      fast = fast,
      account_id = account_id,
    },
  }
end

-- The visible rows of the backend's `/models`, with the effort levels each
-- declares.
function M.parse_plan_models(body, account_id)
  local models = {}
  for _, entry in ipairs(type(body) == "table" and type(body.models) == "table" and body.models or {}) do
    if type(entry) == "table" and entry.visibility == LISTED and type(entry.slug) == "string" then
      models[#models + 1] = plan_model(entry, account_id)
    end
  end
  if #models == 0 then
    return nil, http.config_error(M.EMPTY_MODELS_ERROR)
  end
  return models
end

local function usage_window(seconds)
  if seconds == SECONDS_PER_WEEK then
    return { Weekly = {} }
  elseif seconds % SECONDS_PER_DAY == 0 then
    return { Days = math.floor(seconds / SECONDS_PER_DAY) }
  elseif seconds % SECONDS_PER_HOUR == 0 then
    return { Hours = math.floor(seconds / SECONDS_PER_HOUR) }
  end
  return { Other = seconds .. "-second usage" }
end

-- The backend's quota windows as usage limits, skipping incomplete ones.
function M.parse_usage(body)
  body = type(body) == "table" and body or {}
  local rate = type(body.rate_limit) == "table" and body.rate_limit or {}
  local limits = {}
  for _, window in ipairs({ rate.primary_window, rate.secondary_window }) do
    if
      type(window) == "table"
      and type(window.limit_window_seconds) == "number"
      and type(window.used_percent) == "number"
    then
      limits[#limits + 1] = {
        kind = usage_window(window.limit_window_seconds),
        percentage = math.clamp(math.floor(window.used_percent + 0.5), 0, MAX_PERCENT),
        reset_at = type(window.reset_at) == "number" and window.reset_at * MILLIS or nil,
      }
    end
  end
  local plan = type(body.plan_type) == "string" and body.plan_type or nil
  if not plan and #limits == 0 then
    return nil, http.config_error(M.EMPTY_USAGE_ERROR)
  end
  return { plan = plan, limits = limits }
end

-- Plan models and models the backend declared speak Responses, with the
-- declared effort levels; everything else speaks Chat Completions.
local function stream(state, config, model, ctx, request, emit)
  local max = thinking.max_budget(request.model)
  local plan = model.provider_data
  if not M.plan_window(model.id) and type(plan) ~= "table" then
    local body = chat.build_request(request, { max_tokens_field = MAX_TOKENS_FIELD, include_stream_usage = true })
    body.reasoning_effort = thinking.effort(request.thinking, thinking.dialects.standard, max)
    return authorized(ctx, config, state, function()
      return http.post_stream(ctx, state.base .. CHAT_PATH, headers(state), body, chat.stream, emit)
    end)
  end
  local dialect = thinking.dialects.standard
  if type(plan) == "table" then
    dialect = { supported = plan.efforts or {}, adaptive = plan.adaptive, off = plan.off and thinking.OFF or nil }
  end
  local body = responses.build_request(request, responses.reasoning(request.thinking, dialect, max))
  return authorized(ctx, config, state, function()
    -- Checked against the live tokens, which may have changed accounts since
    -- the model was prepared.
    body.service_tier = request.fast and fast_eligible(model, state.tokens) and FAST_TIER or nil
    -- The ChatGPT backend keeps its fixed URL; base URL overrides apply to
    -- the platform API only.
    local base = state.tokens and M.BACKEND_URL or state.base
    return http.post_stream(ctx, base .. RESPONSES_PATH, headers(state, nil, true), body, responses.stream, emit)
  end)
end

-- Streams {request} through {client} (`base` and `headers(extra)`), whose
-- credentials come from elsewhere: plan models speak Responses and the rest
-- Chat Completions, both with the standard effort levels.
function M.route_stream(client, ctx, request, emit)
  local max = thinking.max_budget(request.model)
  local dialect = thinking.dialects.standard
  if M.plan_window(request.model.id) then
    local body = responses.build_request(request, responses.reasoning(request.thinking, dialect, max))
    return http.post_stream(ctx, client.base .. RESPONSES_PATH, client.headers(), body, responses.stream, emit)
  end
  local body = chat.build_request(request, { max_tokens_field = MAX_TOKENS_FIELD, include_stream_usage = true })
  body.reasoning_effort = thinking.effort(request.thinking, dialect, max)
  return http.post_stream(ctx, client.base .. CHAT_PATH, client.headers(), body, chat.stream, emit)
end

local function new_state(ctx, config)
  local configured, err = http.headers(M.def.id, {}, config.headers)
  if not configured then
    return nil, err
  end
  return load_auth(ctx, config, { configured = configured, base = compat.base_url(M.def, config) })
end

function M.provider()
  local spec = compat.provider(M.def)
  -- Until the plan listing answers, a ChatGPT login cannot tell whether a
  -- model has fast, so it says "pending" and the host keeps a /fast wish.
  local discovered = false
  local key_status = spec.auth_status
  spec.auth[#spec.auth + 1] = { id = CHATGPT_METHOD.id, label = CHATGPT_METHOD.label, login = chatgpt_login }
  spec.aliases = { CODEX_ALIAS }

  function spec.auth_status(ctx, config)
    local saved = ctx.credentials:read()
    if oauth_tokens(saved) then
      return { state = "saved", detail = OAUTH_DETAIL }
    end
    return key_status(ctx, config)
  end

  function spec.discover(ctx, config)
    local state, state_err = new_state(ctx, config)
    if not state then
      return nil, state_err
    end
    if state.tokens then
      local url = M.BACKEND_URL .. PLAN_MODELS_PATH .. M.CODEX_CLIENT_VERSION
      local rows = authorized(ctx, config, state, function()
        local body, err = get_json(ctx, url, headers(state, nil, true))
        if err then
          return nil, err
        end
        return M.parse_plan_models(body, state.tokens and state.tokens.account_id)
      end)
      discovered = true
      -- A failed listing falls back to the curated plan models.
      return compat.merge(M.def, rows or {})
    end
    local body, err = authorized(ctx, config, state, function()
      return get_json(ctx, state.base .. compat.MODELS_PATH, headers(state))
    end)
    if err then
      return nil, err
    end
    return compat.merge(M.def, compat.parse_listing(body, compat.parse_model))
  end

  function spec.prepare(ctx, info, config)
    local state, err = new_state(ctx, config)
    if not state then
      return nil, err
    end
    local model = compat.describe(M.def, info)
    -- API-key users get no opinion on fast; the host's pricing gate decides.
    if state.tokens then
      local listed = type(model.provider_data) == "table" and model.provider_data.fast ~= nil
      if fast_eligible(model, state.tokens) then
        model.supports_fast = true
      elseif not listed and not discovered then
        model.supports_fast = M.FAST_PENDING
      else
        model.supports_fast = false
      end
      local window = M.plan_window(model.id)
      if window and model.context_window then
        model.context_window = math.min(model.context_window, window)
      end
    end
    local instance = {
      key_count = function()
        return state.pool and state.pool.count or 1
      end,
      rotate_key = function()
        return state.pool ~= nil and state.pool.rotate()
      end,
      reload_auth = function(call_ctx)
        local loaded, load_err = load_auth(call_ctx, config, state)
        if not loaded then
          return nil, load_err
        end
      end,
      refresh_auth = function(call_ctx)
        local loaded, load_err = load_auth(call_ctx, config, state)
        if not loaded then
          return nil, load_err
        end
        local ok, refresh_err = fresh(call_ctx, config, state)
        if not ok then
          return nil, refresh_err
        end
      end,
      stream = function(call_ctx, request, emit)
        return stream(state, config, model, call_ctx, request, emit)
      end,
      -- Quota exists only for ChatGPT logins; API keys have none.
      fetch_usage = function(call_ctx)
        if not state.tokens then
          return nil
        end
        local body, usage_err = authorized(call_ctx, config, state, function()
          return get_json(call_ctx, M.USAGE_URL, headers(state, nil, true))
        end)
        if usage_err then
          return nil, usage_err
        end
        return M.parse_usage(body)
      end,
    }
    return instance, model
  end

  return spec
end

return M
