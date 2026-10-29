local M = {}

local SECONDS_PER_HOUR = 3600
local SECONDS_PER_DAY = 86400
local SECONDS_PER_WEEK = 604800
local MAX_U64 = 18446744073709551616
local MILLIS_PER_SECOND = 1000
local INVALID_USAGE = "Invalid Codex usage response"
local EMPTY_USAGE = "OpenAI usage response contained no plan or rate limits; the endpoint schema likely changed"
local CODEX_BASE = "https://chatgpt.com/backend-api/codex"
local USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
local TOKEN_KEY = "chatgpt"
local REFRESH_BUFFER_MS = 60000

local function unsigned(value)
  return type(value) == "number" and value >= 0 and value < MAX_U64 and value == math.floor(value)
end

local function usage_window(seconds)
  if seconds == SECONDS_PER_WEEK then
    return { type = "weekly" }
  elseif seconds % SECONDS_PER_DAY == 0 then
    return { type = "days", days = seconds / SECONDS_PER_DAY }
  elseif seconds % SECONDS_PER_HOUR == 0 then
    return { type = "hours", hours = seconds / SECONDS_PER_HOUR }
  end
  return { type = "other", label = string.format("%.0f-second usage", seconds) }
end

function M.parse_usage(response)
  local value, err = maki.json.decode(response)
  if not value then
    return nil, { kind = "api", message = tostring(err) }
  end
  if type(value) ~= "table" or (value.plan_type ~= nil and type(value.plan_type) ~= "string") then
    return nil, { kind = "api", message = INVALID_USAGE }
  end
  local rate = value.rate_limit
  if rate == nil then
    rate = {}
  end
  if type(rate) ~= "table" then
    return nil, { kind = "api", message = INVALID_USAGE }
  end
  local limits = {}
  for _, key in ipairs({ "primary_window", "secondary_window" }) do
    local window = rate[key]
    if window ~= nil then
      if
        type(window) ~= "table"
        or (window.used_percent ~= nil and type(window.used_percent) ~= "number")
        or (window.limit_window_seconds ~= nil and not unsigned(window.limit_window_seconds))
        or (window.reset_at ~= nil and not unsigned(window.reset_at))
      then
        return nil, { kind = "api", message = INVALID_USAGE }
      end
      if window.used_percent ~= nil and window.limit_window_seconds ~= nil then
        local percentage = window.used_percent
        if percentage ~= percentage or math.abs(percentage) == math.huge then
          percentage = nil
        else
          percentage = math.clamp(math.floor(percentage + 0.5), 0, 100)
        end
        local reset = window.reset_at
        if reset and reset >= MAX_U64 / MILLIS_PER_SECOND then
          reset = nil
        end
        limits[#limits + 1] = {
          kind = usage_window(window.limit_window_seconds),
          percentage = percentage,
          reset_at = reset and reset * MILLIS_PER_SECOND or nil,
        }
      end
    end
  end
  if value.plan_type == nil and #limits == 0 then
    return nil, { kind = "config", message = EMPTY_USAGE }
  end
  return { plan = value.plan_type, limits = limits, by_model_today = {} }
end

local thinking = { mode = "effort", levels = { "minimal", "low", "medium", "high", "xhigh" } }

local function read_tokens(ctx)
  if not ctx or not ctx.credentials then
    return nil
  end
  local tokens, err = ctx.credentials:read(TOKEN_KEY)
  if err then
    return nil, err
  end
  if tokens and (type(tokens) ~= "table" or type(tokens.access) ~= "string" or tokens.access == "") then
    return nil, { kind = "config", message = "Invalid OpenAI credentials" }
  end
  return tokens
end

local function now_ms()
  local now = maki.time.now()
  return now.secs * 1000 + math.floor(now.nanosecs / 1000000)
end

local function expired(tokens)
  return type(tokens.expires) ~= "number"
    or tokens.expires ~= tokens.expires
    or now_ms() + REFRESH_BUFFER_MS >= tokens.expires
end

local function account_id(tokens)
  return type(tokens.account_id) == "string" and tokens.account_id ~= "" and tokens.account_id or nil
end

local function refresh_tokens(ctx, previous, force)
  local current, err = ctx.credentials:refresh(TOKEN_KEY, function(saved)
    if type(saved) ~= "table" or type(saved.access) ~= "string" or saved.access == "" then
      return nil, { kind = "config", message = "OpenAI credentials are missing or invalid" }
    end
    if account_id(saved) ~= account_id(previous) then
      return nil, { kind = "config", message = "OpenAI account changed; prepare the provider again" }
    end
    if saved.access ~= previous.access or (not force and not expired(saved)) then
      return nil
    end
    local refreshed, refresh_err = require("maki.openai_auth").refresh(ctx, saved, now_ms())
    if refreshed and account_id(refreshed) ~= account_id(saved) then
      return nil, { kind = "config", message = "OpenAI account changed; prepare the provider again" }
    end
    return refreshed, refresh_err
  end)
  if not current then
    return nil, err or { kind = "config", message = "OpenAI credentials are missing" }
  end
  return current
end

local function usable_tokens(ctx, tokens)
  if tokens and expired(tokens) then
    return refresh_tokens(ctx, tokens, false)
  end
  return tokens
end

local function api_keys(ctx, config)
  local key = maki.uv.os_getenv(config.api_key_env or "OPENAI_API_KEY")
  if not key or key == "" then
    key = config.api_key
  end
  if (not key or key == "") and ctx and ctx.credentials then
    local saved, err = ctx.credentials:read("api_key")
    if err then
      return nil, err
    end
    if saved ~= nil then
      if type(saved) ~= "table" or type(saved.api_key) ~= "string" then
        return nil, { kind = "config", message = "Invalid OpenAI saved API key" }
      end
      key = saved.api_key
    end
  end
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  return keys
end

local function codex_headers(tokens)
  local headers = { Authorization = "Bearer " .. tokens.access }
  if type(tokens.account_id) == "string" and tokens.account_id ~= "" then
    headers["chatgpt-account-id"] = tokens.account_id
  end
  return headers
end

function M.prepare(ctx, model, config)
  config = config or {}
  local tokens, token_err = read_tokens(ctx)
  if token_err then
    return nil, token_err
  end
  if tokens then
    tokens, token_err = usable_tokens(ctx, tokens)
    if token_err and token_err.kind == "cancelled" then
      return nil, token_err
    end
  end
  local keys, key_err
  if tokens then
    keys, key_err = api_keys(nil, config)
  else
    keys, key_err = api_keys(ctx, config)
  end
  if not keys then
    return nil, key_err
  end
  if #keys == 0 and not tokens then
    return nil, token_err or { kind = "config", message = "OpenAI API key is not configured" }
  end
  local index = 1
  local base = maki.uv.os_getenv("OPENAI_BASE_URL")
  if not base or base == "" then
    base = config.base_url or "https://api.openai.com/v1"
  end
  base = base:gsub("/+$", "")
  return {
    model = model,
    key_count = #keys,
    rotate_key = function()
      if tokens or #keys < 2 then
        return false
      end
      index = index % #keys + 1
      return true
    end,
    reload_auth = tokens and function(ctx)
      local current, err = read_tokens(ctx)
      if not current then
        return nil, err or { kind = "config", message = "OpenAI credentials are missing" }
      end
      tokens = current
      return true
    end or nil,
    refresh_auth = tokens and function(ctx)
      if not tokens then
        return true
      end
      local current, err = refresh_tokens(ctx, tokens, true)
      if not current then
        return nil, err
      end
      tokens = current
      return true
    end or nil,
    fetch_usage = tokens
        and function(ctx)
          if not tokens then
            return nil
          end
          local current, token_err = usable_tokens(ctx, tokens)
          if not current then
            return nil, token_err
          end
          tokens = current
          local sent_tokens = tokens
          local response, err = ctx:send({ url = USAGE_URL, method = "GET", headers = codex_headers(sent_tokens) })
          if response and response.status == 401 then
            response:close()
            current, token_err = refresh_tokens(ctx, sent_tokens, true)
            if not current then
              return nil, token_err
            end
            tokens = current
            response, err = ctx:send({ url = USAGE_URL, method = "GET", headers = codex_headers(tokens) })
          end
          if not response then
            return nil, err
          end
          if response.status < 200 or response.status >= 300 then
            response:close()
            return nil, { kind = "api", status = response.status, message = "OpenAI usage request failed" }
          end
          local value, read_err = require("maki.json_response").read(response, 1024 * 1024)
          response:close()
          if not value then
            return nil, read_err
          end
          local encoded, encode_err = maki.json.encode(value)
          if not encoded then
            return nil, { kind = "api", message = tostring(encode_err) }
          end
          return M.parse_usage(encoded)
        end
      or nil,
    stream = function(ctx, request, emit)
      if tokens then
        local current, token_err = usable_tokens(ctx, tokens)
        if not current then
          if token_err and token_err.kind == "cancelled" then
            return nil, token_err
          end
          local fallback, key_err = api_keys(ctx, config)
          if not fallback then
            return nil, key_err
          end
          if #fallback == 0 then
            return nil, token_err
          end
          keys = fallback
          index = 1
        end
        tokens = current
      end
      local responses = require("maki.openai_models").plan_context_window(request.model.id) ~= nil
      local codex = responses and tokens ~= nil
      if not tokens and #keys == 0 then
        return nil, { kind = "config", message = "OpenAI API key is not configured for this model" }
      end
      local adapter = require(responses and "maki.responses" or "maki.chat_completions")
      local resolution, err = require("maki.thinking").resolve(request.thinking, thinking, {
        max_tokens = request.model.max_output_tokens,
      })
      if not resolution then
        return nil, { kind = "config", message = tostring(err) }
      end
      local effective = resolution.effective
      local reasoning
      if effective.mode == "effort" then
        reasoning = { effort = effective.level, summary = "auto" }
      elseif request.thinking.mode ~= "off" then
        reasoning = { summary = "auto" }
      end
      local body, body_err
      if responses then
        body, body_err = adapter.build_request(request, { reasoning = reasoning })
      else
        body, body_err =
          adapter.build_request(request, { max_tokens_field = "max_completion_tokens", include_stream_usage = true })
        if body and effective.mode == "effort" then
          body.reasoning_effort = effective.level
        end
      end
      if not body then
        return nil, { kind = "config", message = tostring(body_err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local function send(current)
        local headers = codex and codex_headers(current)
          or { Authorization = "Bearer " .. (current and current.access or keys[index]) }
        headers["Content-Type"] = "application/json"
        for name, value in pairs(codex and {} or config.headers or {}) do
          for existing in pairs(headers) do
            if existing:lower() == name:lower() then
              headers[existing] = nil
            end
          end
          headers[name] = value
        end
        return ctx:send({
          url = (codex and CODEX_BASE or base) .. (responses and "/responses" or "/chat/completions"),
          method = "POST",
          headers = headers,
          body = encoded,
        })
      end
      local sent_tokens = tokens
      local overridden_auth = false
      if not codex then
        for name in pairs(config.headers or {}) do
          if name:lower() == "authorization" then
            overridden_auth = true
          end
        end
      end
      local response, send_err = send(sent_tokens)
      if sent_tokens and not overridden_auth and response and response.status == 401 then
        response:close()
        local current, refresh_err = refresh_tokens(ctx, sent_tokens, true)
        if not current then
          return nil, refresh_err
        end
        tokens = current
        response, send_err = send(current)
      end
      if not response then
        return nil, type(send_err) == "table" and send_err or { kind = "network", message = "OpenAI request failed" }
      end
      if response.status < 200 or response.status >= 300 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "OpenAI request failed" }
      end
      local result, stream_err = adapter.stream(response, emit)
      response:close()
      if stream_err and stream_err.kind == "api" then
        return nil, { kind = "api", status = stream_err.status, message = "OpenAI request failed" }
      end
      return result, stream_err
    end,
  }
end

function M.discover(ctx, config)
  config = config or {}
  local tokens, token_err = read_tokens(ctx)
  if token_err then
    return nil, token_err
  end
  if tokens then
    tokens, token_err = usable_tokens(ctx, tokens)
    if token_err and token_err.kind == "cancelled" then
      return nil, token_err
    end
  end
  if tokens then
    return require("maki.openai_models").discover(ctx, {
      kind = "codex",
      headers = codex_headers(tokens),
      account_id = tokens.account_id,
    })
  end
  local keys, key_err = api_keys(ctx, config)
  if not keys then
    return nil, key_err
  end
  if #keys == 0 then
    return nil, token_err or { kind = "config", message = "OpenAI API key is not configured" }
  end
  local headers = { Authorization = "Bearer " .. keys[1] }
  for name, value in pairs(config.headers or {}) do
    for existing in pairs(headers) do
      if existing:lower() == name:lower() then
        headers[existing] = nil
      end
    end
    headers[name] = value
  end
  local base = maki.uv.os_getenv("OPENAI_BASE_URL")
  if not base or base == "" then
    base = config.base_url
  end
  return require("maki.openai_models").discover(ctx, { kind = "platform", base_url = base, headers = headers })
end

function M.login_api_key(ctx, ui)
  if type(ui) ~= "table" or type(ui.input) ~= "function" then
    return nil, { kind = "config", message = "OpenAI API key login requires interactive authentication" }
  end
  local cancelled = false
  maki.async.on_cancel(function()
    cancelled = true
  end)
  if cancelled then
    return nil, { kind = "cancelled", message = "OpenAI login cancelled" }
  end
  local key, err = ui:input("OpenAI API key", true, false)
  if cancelled then
    return nil, { kind = "cancelled", message = "OpenAI login cancelled" }
  end
  if err then
    return nil, err
  end
  if type(key) ~= "string" or key:match("^%s*$") then
    return nil, { kind = "config", message = "OpenAI API key is empty" }
  end
  return ctx.credentials:write("api_key", { api_key = key:match("^%s*(.-)%s*$") })
end

function M.auth_status(ctx, config)
  config = config or {}
  local tokens, err = read_tokens(ctx)
  if err then
    return nil, err
  end
  if tokens then
    return { configured = true, source = "stored", method = "chatgpt", expires_at = tokens.expires }
  end
  local key = maki.uv.os_getenv(config.api_key_env or "OPENAI_API_KEY")
  if key and key ~= "" then
    return { configured = true, source = "env", method = "api_key" }
  end
  if type(config.api_key) == "string" and config.api_key ~= "" then
    return { configured = true, source = "config", method = "api_key" }
  end
  local keys, key_err = api_keys(ctx, config)
  if not keys then
    return nil, key_err
  end
  return { configured = #keys > 0, source = #keys > 0 and "stored" or nil, method = #keys > 0 and "api_key" or nil }
end

function M.logout(ctx)
  local ok, err = ctx.credentials:delete(TOKEN_KEY)
  if not ok then
    return nil, err
  end
  return ctx.credentials:delete("api_key")
end

function M.register()
  maki.api.register_provider({
    id = "openai",
    display_name = "OpenAI",
    prepare = M.prepare,
    discover = M.discover,
    auth = {
      { id = "api_key", label = "API key", login = M.login_api_key },
      { id = "chatgpt", label = "ChatGPT login", login = require("maki.openai_auth").login },
    },
    auth_status = M.auth_status,
    logout = M.logout,
  })
end

return M
