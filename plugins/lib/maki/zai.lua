local M = {}
local chat = require("maki.chat_completions")
local thinking = { mode = "effort", levels = { "high", "xhigh" } }
local none = { mode = "none" }

local function model(id, prefixes, tier, default, context, output, vision, price, supports_thinking)
  return {
    id = id,
    prefixes = prefixes,
    tier = tier,
    family = "glm",
    default = default,
    context_window = context,
    max_output_tokens = output,
    vision = vision,
    thinking = supports_thinking and thinking or none,
    pricing = { input = price[1], output = price[2], cache_write = 0, cache_read = price[3] },
  }
end

M.models = {
  model("glm-5-code", { "glm-5-code" }, "strong", true, 200000, 131072, false, { 1.2, 5, 0.3 }, false),
  model("glm-5.3", { "glm-5.3" }, "strong", false, 1000000, 131072, false, { 1.4, 4.4, 0.26 }, true),
  model("glm-5.3-flash", { "glm-5.3-flash" }, "weak", false, 1000000, 131072, true, { 0.15, 0.5, 0.03 }, true),
  model("glm-5.2", { "glm-5.2" }, "strong", false, 1000000, 131072, false, { 1, 3.2, 0.2 }, true),
  model("glm-5.1", { "glm-5.1", "glm-5" }, "strong", false, 200000, 131072, false, { 1, 3.2, 0.2 }, false),
  model("glm-4.7-flash", { "glm-4.7-flash" }, "weak", true, 200000, 131072, false, { 0, 0, 0 }, false),
  model("glm-4.7", { "glm-4.7", "glm-4.6" }, "medium", true, 200000, 131072, false, { 0.6, 2.2, 0.11 }, false),
  model("glm-4.5-flash", { "glm-4.5-flash" }, "weak", false, 131072, 98304, false, { 0, 0, 0 }, false),
  model("glm-4.5-air", { "glm-4.5-air" }, "weak", false, 131072, 98304, false, { 0.2, 1.1, 0.03 }, false),
  model("glm-4.5", { "glm-4.5" }, "medium", false, 131072, 98304, false, { 0.6, 2.2, 0.11 }, false),
}

function M.supports_thinking(id)
  local version = id:match("^glm%-(.+)$")
  if not version then
    return false
  end
  local major_text, minor_text = version:match("^([^.%-]+)[.%-]([^.%-]+)")
  major_text = major_text or version:match("^([^.%-]+)")
  local major = major_text and major_text:match("^%d+$") and tonumber(major_text)
  local minor = minor_text and minor_text:match("^%d+$") and tonumber(minor_text) or 0
  return major ~= nil and (major > 5 or (major == 5 and minor >= 2))
end

function M.build_request(request)
  local body, err = chat.build_request(request, { max_tokens_field = "max_tokens" })
  if not body then
    return nil, err
  end
  if M.supports_thinking(request.model.id) then
    local intent = request.thinking
    if intent.mode == "off" then
      body.reasoning_effort = "none"
    elseif intent.mode == "adaptive" then
      body.reasoning_effort = "high"
    else
      local maximum = request.model.max_output_tokens
        and math.max(1024, math.floor(request.model.max_output_tokens / 2))
      if intent.mode == "budget" then
        if
          type(intent.tokens) ~= "number"
          or intent.tokens <= 0
          or intent.tokens % 1 ~= 0
          or intent.tokens == math.huge
        then
          return nil, "invalid thinking preference"
        end
        local percent = math.floor(intent.tokens * 100 / (maximum or 32768))
        intent = { mode = "effort", level = percent <= 60 and "high" or "xhigh" }
      end
      local result, resolve_err = require("maki.thinking").resolve(intent, thinking)
      if not result then
        return nil, resolve_err
      end
      body.reasoning_effort = result.effective.level
    end
  end
  return body
end

local function unsigned_integer(value, maximum)
  return type(value) == "number" and value >= 0 and value <= maximum and value % 1 == 0
end

function M.parse_quotas(body)
  local value = maki.json.decode(body)
  if type(value) ~= "table" or type(value.data) ~= "table" then
    return nil, { kind = "api", message = "Invalid Z.AI quota response" }
  end
  local data, limits = value.data, {}
  if data.level ~= nil and type(data.level) ~= "string" then
    return nil, { kind = "api", message = "Invalid Z.AI quota response" }
  end
  if data.limits ~= nil and type(data.limits) ~= "table" then
    return nil, { kind = "api", message = "Invalid Z.AI quota response" }
  end
  for key in pairs(data.limits or {}) do
    if type(key) ~= "number" or key < 1 or key % 1 ~= 0 or key > #data.limits then
      return nil, { kind = "api", message = "Invalid Z.AI quota response" }
    end
  end
  for _, item in ipairs(data.limits or {}) do
    if
      type(item) ~= "table"
      or type(item.type) ~= "string"
      or not unsigned_integer(item.unit, 4294967295)
      or not unsigned_integer(item.percentage, 4294967295)
      or (item.nextResetTime ~= nil and not unsigned_integer(item.nextResetTime, 9007199254740991))
    then
      return nil, { kind = "api", message = "Invalid Z.AI quota response" }
    end
    local kind
    if item.type == "TOKENS_LIMIT" and item.unit == 3 then
      kind = { type = "hours", hours = 5 }
    elseif item.type == "TOKENS_LIMIT" and item.unit == 6 then
      kind = { type = "weekly" }
    elseif item.type == "TIME_LIMIT" then
      kind = { type = "subscription" }
    else
      kind = { type = "other", label = item.type .. " #" .. item.unit }
    end
    limits[#limits + 1] = { kind = kind, percentage = item.percentage, reset_at = item.nextResetTime }
  end
  return { plan = data.level, limits = limits, by_model_today = {} }
end

local CREDENTIAL_KEY = "api_key"
local MISSING_KEY = "Z.AI API key is not configured"

local function split_keys(value)
  local keys = {}
  for part in (value or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  return keys
end

local function credentials(ctx, config)
  local keys = split_keys(maki.uv.os_getenv(config.api_key_env or "ZHIPU_API_KEY"))
  if #keys > 0 then
    return keys, nil, "env"
  end
  if ctx and ctx.credentials then
    local saved, err = ctx.credentials:read(CREDENTIAL_KEY)
    if err then
      return nil, err
    end
    if saved ~= nil then
      if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
        return nil, { kind = "config", message = "Invalid Z.AI credentials" }
      end
      return { saved.api_key }, nil, "stored"
    end
  end
  if type(config.api_key) == "string" and config.api_key ~= "" then
    return { config.api_key }, nil, "config"
  end
  return nil, { kind = "config", message = MISSING_KEY }
end

function M.login(ctx, ui)
  local key, err = ui:input("Z.AI API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  key = key:match("^%s*(.-)%s*$")
  if key == "" then
    return nil, { kind = "config", message = MISSING_KEY }
  end
  return ctx.credentials:write(CREDENTIAL_KEY, { api_key = key })
end

function M.auth_status(ctx, config)
  local keys, err, source = credentials(ctx, config or {})
  if not keys then
    if err and err.message ~= MISSING_KEY then
      return nil, err
    end
    return { configured = false }
  end
  return { configured = true, source = source, method = "api_key" }
end

function M.logout(ctx)
  return ctx.credentials:delete(CREDENTIAL_KEY)
end

function M.discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
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
  local base = (config.base_url or "https://api.z.ai/api/paas/v4"):gsub("/+$", "")
  local response, send_err = ctx:send({ url = base .. "/models", method = "GET", headers = headers })
  if not response then
    return nil, send_err
  end
  if response.status ~= 200 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "Z.AI model discovery failed" }
  end
  local value, read_err = require("maki.json_response").read(response, 4 * 1024 * 1024)
  response:close()
  if not value then
    return nil, read_err
  end
  return require("maki.openai_models").parse_platform(value)
end

function M.prepare(ctx, model_value, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local index = 1
  if type(model_value.id) == "string" and M.supports_thinking(model_value.id) then
    model_value.thinking = thinking
  end
  local base = (config.base_url or "https://api.z.ai/api/paas/v4"):gsub("/+$", "")
  local function headers()
    local result = { Authorization = "Bearer " .. keys[index], ["Content-Type"] = "application/json" }
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
  local function safe_message(message)
    if type(message) ~= "string" then
      return "Z.AI request failed"
    end
    for _, secret in ipairs(keys) do
      if message:find(secret, 1, true) then
        return "Z.AI request failed"
      end
    end
    for _, value in pairs(config.headers or {}) do
      if type(value) == "string" and value ~= "" and message:find(value, 1, true) then
        return "Z.AI request failed"
      end
    end
    return message
  end
  local function read_body(response)
    local chunks, size = {}, 0
    while true do
      local chunk, err = response:read()
      if not chunk then
        return nil, err or { kind = "network", message = "Z.AI response read failed" }
      end
      if chunk == "" then
        return table.concat(chunks)
      end
      size = size + #chunk
      if size > 65536 then
        return nil, { kind = "api", message = "Z.AI response too large" }
      end
      chunks[#chunks + 1] = chunk
    end
  end
  return {
    model = model_value,
    key_count = #keys,
    rotate_key = function()
      if #keys < 2 then
        return false
      end
      index = index % #keys + 1
      return true
    end,
    fetch_usage = function(ctx)
      local response, err =
        ctx:send({ url = "https://api.z.ai/api/monitor/usage/quota/limit", method = "GET", headers = headers() })
      if not response then
        return nil, err
      end
      if response.status ~= 200 then
        local text = read_body(response)
        local retry = response.headers and response.headers["retry-after"]
        local seconds = type(retry) == "string" and tonumber(retry:match("^%s*(%d+)%s*$"))
        response:close()
        return nil,
          {
            kind = "api",
            status = response.status,
            message = safe_message(text or "unable to read error body"),
            retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil,
          }
      end
      local body, read_err = read_body(response)
      response:close()
      if not body then
        return nil, read_err
      end
      return M.parse_quotas(body)
    end,
    stream = function(ctx, request, emit)
      local body, err = M.build_request(request)
      if not body then
        return nil, { kind = "config", message = tostring(err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local response, send_err =
        ctx:send({ url = base .. "/chat/completions", method = "POST", headers = headers(), body = encoded })
      if not response then
        return nil, send_err
      end
      if response.status ~= 200 then
        local text = read_body(response) or ""
        local status = response.status
        local retry = response.headers and response.headers["retry-after"]
        local seconds = type(retry) == "string" and tonumber(retry:match("^%s*(%d+)%s*$"))
        response:close()
        if (status == 429 or status >= 500) and (text:find("1113", 1, true) or text:find("nsufficien", 1, true)) then
          status = 402
          seconds = nil
        end
        return nil,
          {
            kind = "api",
            status = status,
            message = safe_message(text),
            retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil,
          }
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      if stream_err and stream_err.kind == "api" then
        local status = stream_err.status
        local message = stream_err.message or "Z.AI request failed"
        local retry = stream_err.retry_after_ms
        if
          (status == 429 or status and status >= 500)
          and (message:find("1113", 1, true) or message:find("nsufficien", 1, true))
        then
          status, retry = 402, nil
        end
        return nil, { kind = "api", status = status, message = safe_message(message), retry_after_ms = retry }
      end
      return result, stream_err
    end,
  }
end

function M.register()
  maki.api.register_provider({
    id = "zai",
    display_name = "Z.AI",
    docs = {
      auth_line = "`ZHIPU_API_KEY` (shared across both endpoints)",
      urls = { "https://api.z.ai/api/paas/v4", "https://api.z.ai/api/coding/paas/v4" },
    },
    models = M.models,
    prepare = M.prepare,
    discover = M.discover,
    auth = { { id = "api_key", label = "API key", login = M.login } },
    auth_status = M.auth_status,
    logout = M.logout,
  })
end

return M
