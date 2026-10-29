local M = {}

local thinking = { mode = "effort", levels = { "minimal", "low", "medium", "high" } }

M.models = {
  {
    id = "hf:moonshotai/Kimi-K2.5",
    prefixes = { "hf:moonshotai/Kimi-K2.5" },
    tier = "strong",
    family = "synthetic",
    default = true,
    context_window = 200000,
    max_output_tokens = 131072,
    vision = false,
    thinking = thinking,
    pricing = { input = 0.45, output = 3.40, cache_write = 0, cache_read = 0 },
  },
  {
    id = "hf:deepseek-ai/DeepSeek-V3.2",
    prefixes = { "hf:deepseek-ai/DeepSeek-V3.2" },
    tier = "medium",
    family = "synthetic",
    default = true,
    context_window = 200000,
    max_output_tokens = 131072,
    vision = false,
    thinking = thinking,
    pricing = { input = 0.56, output = 1.68, cache_write = 0, cache_read = 0 },
  },
  {
    id = "hf:zai-org/GLM-4.7-Flash",
    prefixes = { "hf:zai-org/GLM-4.7-Flash" },
    tier = "weak",
    family = "synthetic",
    default = true,
    context_window = 200000,
    max_output_tokens = 131072,
    vision = false,
    thinking = thinking,
    pricing = { input = 0.10, output = 0.50, cache_write = 0, cache_read = 0 },
  },
}

local function percentage(used, limit)
  if
    type(used) ~= "number"
    or type(limit) ~= "number"
    or used ~= used
    or limit ~= limit
    or math.abs(used) == math.huge
    or math.abs(limit) == math.huge
    or limit <= 0
  then
    return nil
  end
  return math.clamp(math.floor(100 * used / limit + 0.5), 0, 100)
end

function M.parse_quotas(response, parse_timestamp)
  local value, err = maki.json.decode(response)
  if not value then
    return nil, { kind = "api", message = tostring(err) }
  end
  if type(value) ~= "table" then
    return nil, { kind = "api", message = "Invalid Synthetic usage response" }
  end
  local rolling, subscription, weekly = value.rollingFiveHourLimit, value.subscription, value.weeklyTokenLimit
  local function valid_lane(lane, first, second, timestamp)
    return lane == nil
      or (
        type(lane) == "table"
        and type(lane[first]) == "number"
        and (not second or type(lane[second]) == "number")
        and (lane[timestamp] == nil or type(lane[timestamp]) == "string")
      )
  end
  if
    not valid_lane(rolling, "remaining", "max", "nextTickAt")
    or not valid_lane(subscription, "requests", "limit", "renewsAt")
    or not valid_lane(weekly, "percentRemaining", nil, "nextRegenAt")
  then
    return nil, { kind = "api", message = "Invalid Synthetic usage response" }
  end
  local limits = {}
  local function add(kind, used, maximum, timestamp)
    local reset = type(timestamp) == "string" and parse_timestamp and parse_timestamp(timestamp) or nil
    limits[#limits + 1] = { kind = kind, percentage = percentage(used, maximum), reset_at = reset }
  end
  if rolling then
    add({ type = "hours", hours = 5 }, rolling.max - rolling.remaining, rolling.max, rolling.nextTickAt)
  elseif subscription then
    add({ type = "hours", hours = 5 }, subscription.requests, subscription.limit, subscription.renewsAt)
  end
  if weekly then
    add({ type = "weekly" }, 100 - weekly.percentRemaining, 100, weekly.nextRegenAt)
  end
  if #limits == 0 then
    return nil,
      {
        kind = "config",
        message = "Synthetic usage response contained no recognisable quota lane; the endpoint schema likely changed",
      }
  end
  return { limits = limits, by_model_today = {} }, nil
end

function M.encode_thinking(body, intent, max_thinking_budget)
  local constraints = max_thinking_budget and { max_tokens = max_thinking_budget } or nil
  local resolution, err = require("maki.thinking").resolve(intent, thinking, constraints)
  if not resolution then
    return nil, err
  end
  if resolution.effective.mode == "effort" then
    body.reasoning_effort = resolution.effective.level
  end
  return true
end

local function credentials(config)
  local key = maki.uv.os_getenv(config.api_key_env or "SYNTHETIC_API_KEY")
  if not key or key == "" then
    key = config.api_key
  end
  if not key or key == "" then
    return nil, { kind = "config", message = "Synthetic API key is not configured" }
  end
  local keys = {}
  for part in key:gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "Synthetic API key is not configured" }
  end
  return keys
end

local function headers(config, key)
  local result = { Authorization = "Bearer " .. key, ["Content-Type"] = "application/json" }
  for name, value in pairs(config.headers or {}) do
    for existing in pairs(result) do
      if existing:lower() == name:lower() then
        result[existing] = nil
      end
    end
    result[name] = value
  end
  return result
end

local function base_url(config)
  local base = maki.uv.os_getenv("SYNTHETIC_BASE_URL")
  if not base or base == "" then
    base = config.base_url or "https://api.synthetic.new/openai/v1"
  end
  return base:gsub("/+$", "")
end

function M.discover(ctx, config)
  config = config or {}
  local keys, err = credentials(config)
  if not keys then
    return nil, err
  end
  local response, send_err = ctx:send({
    url = base_url(config) .. "/models",
    method = "GET",
    headers = headers(config, keys[1]),
  })
  if not response then
    return nil, send_err or { kind = "network", message = "Synthetic model discovery failed" }
  end
  if response.status < 200 or response.status >= 300 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "Synthetic model discovery failed" }
  end
  local value, read_err = require("maki.json_response").read(response, 4 * 1024 * 1024)
  response:close()
  if not value then
    return nil, read_err or { kind = "api", message = "Invalid Synthetic models response" }
  end
  return require("maki.openai_models").parse_platform(value)
end

function M.prepare(_, model, config)
  config = config or {}
  local keys, err = credentials(config)
  if not keys then
    return nil, err
  end
  local index = 1
  local function safe_message(message)
    if type(message) ~= "string" or message == "" then
      return "Synthetic request failed"
    end
    for _, secret in ipairs(keys) do
      if message:find(secret, 1, true) then
        return "Synthetic request failed"
      end
    end
    for name, value in pairs(config.headers or {}) do
      if type(value) == "string" and value ~= "" and message:find(value, 1, true) then
        return "Synthetic request failed"
      end
    end
    return message
  end
  local base = base_url(config)
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
    fetch_usage = function(ctx)
      local response, err = ctx:send({
        url = "https://api.synthetic.new/v2/quotas",
        method = "GET",
        headers = headers(config, keys[index]),
      })
      if not response then
        return nil, err
      end
      if response.status < 200 or response.status >= 300 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "Synthetic request failed" }
      end
      local chunks, size = {}, 0
      while true do
        local chunk, read_err = response:read()
        if not chunk then
          response:close()
          return nil, read_err or { kind = "network", message = "Synthetic usage response read failed" }
        end
        if chunk == "" then
          break
        end
        size = size + #chunk
        if size > 65536 then
          response:close()
          return nil, { kind = "api", message = "Synthetic usage response too large" }
        end
        chunks[#chunks + 1] = chunk
      end
      response:close()
      return M.parse_quotas(table.concat(chunks), maki.time.parse_rfc3339)
    end,
    stream = function(ctx, request, emit)
      local chat = require("maki.chat_completions")
      local body, err = chat.build_request(request, { max_tokens_field = "max_completion_tokens" })
      if not body then
        return nil, { kind = "config", message = tostring(err) }
      end
      local ok, thinking_err = M.encode_thinking(body, request.thinking, request.model.max_output_tokens)
      if not ok then
        return nil, { kind = "config", message = tostring(thinking_err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local response, send_err = ctx:send({
        url = base .. "/chat/completions",
        method = "POST",
        headers = headers(config, keys[index]),
        body = encoded,
      })
      if not response then
        if type(send_err) == "table" then
          return nil, send_err
        end
        return nil, { kind = "network", message = tostring(send_err or "Synthetic request failed") }
      end
      if response.status < 200 or response.status >= 300 then
        local value = response.headers and response.headers["retry-after"]
        local seconds = type(value) == "string" and value:match("^%s*(%d+)%s*$")
        seconds = seconds and tonumber(seconds)
        local retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil
        local chunks, size = {}, 0
        while size < 65536 do
          local chunk = response:read()
          if not chunk or chunk == "" then
            break
          end
          size = size + #chunk
          if size > 65536 then
            chunks = {}
            break
          end
          chunks[#chunks + 1] = chunk
        end
        response:close()
        return nil,
          {
            kind = "api",
            status = response.status,
            message = safe_message(table.concat(chunks)),
            retry_after_ms = retry_after_ms,
          }
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      if stream_err and stream_err.kind == "api" then
        return nil, { kind = "api", status = stream_err.status, message = "Synthetic request failed" }
      end
      return result, stream_err
    end,
  }
end

function M.register()
  maki.api.register_provider({
    id = "synthetic",
    display_name = "Synthetic",
    models = M.models,
    prepare = M.prepare,
    discover = M.discover,
  })
end

return M
