local M = {}

local SECONDS_PER_HOUR = 3600
local SECONDS_PER_DAY = 86400
local SECONDS_PER_WEEK = 604800
local MAX_U64 = 18446744073709551616
local MILLIS_PER_SECOND = 1000
local INVALID_USAGE = "Invalid Codex usage response"
local EMPTY_USAGE = "OpenAI usage response contained no plan or rate limits; the endpoint schema likely changed"

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

function M.prepare(_, model, config)
  config = config or {}
  local key = maki.uv.os_getenv(config.api_key_env or "OPENAI_API_KEY")
  if not key or key == "" then
    key = config.api_key
  end
  if not key or key == "" then
    return nil, { kind = "config", message = "OpenAI API key is not configured" }
  end
  local keys = {}
  for part in key:gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "OpenAI API key is not configured" }
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
      if #keys < 2 then
        return false
      end
      index = index % #keys + 1
      return true
    end,
    stream = function(ctx, request, emit)
      local responses = require("maki.openai_models").plan_context_window(request.model.id) ~= nil
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
      local headers = { Authorization = "Bearer " .. keys[index], ["Content-Type"] = "application/json" }
      for name, value in pairs(config.headers or {}) do
        for existing in pairs(headers) do
          if existing:lower() == name:lower() then
            headers[existing] = nil
          end
        end
        headers[name] = value
      end
      local response, send_err = ctx:send({
        url = base .. (responses and "/responses" or "/chat/completions"),
        method = "POST",
        headers = headers,
        body = encoded,
      })
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
  local key = maki.uv.os_getenv(config.api_key_env or "OPENAI_API_KEY")
  if not key or key == "" then
    key = config.api_key
  end
  if not key or key == "" then
    return nil, { kind = "config", message = "OpenAI API key is not configured" }
  end
  key = (key:match("^[^,]+") or ""):match("^%s*(.-)%s*$")
  if not key or key == "" then
    return nil, { kind = "config", message = "OpenAI API key is not configured" }
  end
  local headers = { Authorization = "Bearer " .. key }
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

function M.register()
  maki.api.register_provider({ id = "openai", display_name = "OpenAI", prepare = M.prepare, discover = M.discover })
end

return M
