local chat = require("maki.chat_completions")
local endpoint = "https://router.requesty.ai/v1"
local max_response_bytes = 4194304
local per_million = 1000000

local credential_key = "api_key"
local missing_key = "Requesty API key is not configured"

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
  local keys = split_keys(maki.uv.os_getenv(config.api_key_env or "REQUESTY_API_KEY"))
  if #keys > 0 then
    return keys, nil, "env"
  end
  if ctx and ctx.credentials then
    local saved, err = ctx.credentials:read(credential_key)
    if err then
      return nil, err
    end
    if saved ~= nil then
      if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
        return nil, { kind = "config", message = "Invalid Requesty credentials" }
      end
      return { saved.api_key }, nil, "stored"
    end
  end
  if type(config.api_key) == "string" and config.api_key ~= "" then
    keys = split_keys(config.api_key)
    if #keys > 0 then
      return keys, nil, "config"
    end
  end
  return nil, { kind = "config", message = missing_key }
end

local function login(ctx, ui)
  local key, err = ui:input("Requesty API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  key = key:match("^%s*(.-)%s*$")
  if key == "" then
    return nil, { kind = "config", message = missing_key }
  end
  return ctx.credentials:write(credential_key, { api_key = key })
end

local function auth_status(ctx, config)
  local keys, err, source = credentials(ctx, config or {})
  if not keys then
    if err and err.message ~= missing_key then
      return nil, err
    end
    return { configured = false }
  end
  return { configured = true, source = source, method = "api_key" }
end

local function logout(ctx)
  return ctx.credentials:delete(credential_key)
end

local function headers(config, key, attribution)
  local result = { Authorization = "Bearer " .. key, ["Content-Type"] = "application/json" }
  if attribution then
    result["HTTP-Referer"] = "https://maki.sh"
    result["X-Title"] = "Makima"
  end
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

local function parse_model(entry)
  if type(entry) ~= "table" or type(entry.id) ~= "string" then
    return nil
  end
  if type(entry.api) == "string" and entry.api ~= "chat" then
    return nil
  end
  local function limit(value)
    if type(value) == "number" and value > 0 and value <= 4294967295 and value % 1 == 0 then
      return value
    end
  end
  local pricing
  if type(entry.input_price) == "number" and type(entry.output_price) == "number" then
    pricing = {
      input = entry.input_price * per_million,
      output = entry.output_price * per_million,
      cache_write = (type(entry.caching_price) == "number" and entry.caching_price or 0) * per_million,
      cache_read = (type(entry.cached_price) == "number" and entry.cached_price or 0) * per_million,
    }
  end
  return {
    id = entry.id,
    context_window = limit(entry.context_window),
    max_output_tokens = limit(entry.max_output_tokens),
    pricing = pricing,
    supports_thinking = entry.supports_reasoning == true,
    supports_vision = entry.supports_vision == true,
  }
end

local function api_error(response, message, secrets)
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
  local status = response.status
  response:close()
  local detail = table.concat(chunks)
  if detail ~= "" then
    for _, secret in ipairs(secrets) do
      if secret ~= "" and detail:find(secret, 1, true) then
        return { kind = "api", status = status, message = message, retry_after_ms = retry_after_ms }
      end
    end
    message = detail
  end
  return { kind = "api", status = status, message = message, retry_after_ms = retry_after_ms }
end

local function fetch_models(ctx, url, request_headers, secrets)
  local response, err = ctx:send({ url = url, method = "GET", headers = request_headers })
  if not response then
    return nil, err
  end
  if response.status < 200 or response.status >= 300 then
    return nil, api_error(response, "Requesty models request failed", secrets)
  end
  local chunks, size = {}, 0
  while true do
    local chunk, read_err = response:read()
    if read_err then
      response:close()
      return nil, read_err
    end
    if not chunk or chunk == "" then
      break
    end
    size = size + #chunk
    if size > max_response_bytes then
      response:close()
      return nil, { kind = "api", message = "Requesty models response too large" }
    end
    chunks[#chunks + 1] = chunk
  end
  response:close()
  local data, decode_err = maki.json.decode(table.concat(chunks))
  if not data then
    return nil, { kind = "api", message = tostring(decode_err) }
  end
  local models = {}
  for _, entry in ipairs(type(data) == "table" and type(data.data) == "table" and data.data or {}) do
    local model = parse_model(entry)
    if model then
      models[#models + 1] = model
    end
  end
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  return models
end

local function secret_values(config, keys)
  local secrets = {}
  for _, key in ipairs(keys) do
    secrets[#secrets + 1] = key
  end
  for _, value in pairs(config.headers or {}) do
    if type(value) == "string" then
      secrets[#secrets + 1] = value
    end
  end
  return secrets
end

local function discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local base = (config.base_url or endpoint):gsub("/+$", "")
  local request_headers = headers(config, keys[1], false)
  local secrets = secret_values(config, keys)
  local results = maki.async.gather({
    function()
      local models, failure = fetch_models(ctx, base .. "/models/managed", request_headers, secrets)
      return { models = models, err = failure }
    end,
    function()
      local models, failure = fetch_models(ctx, base .. "/models", request_headers, secrets)
      return { models = models, err = failure }
    end,
  })
  local managed = results[1].ok and results[1].value or { err = { kind = "api", message = tostring(results[1].err) } }
  local catalog = results[2].ok and results[2].value or { err = { kind = "api", message = tostring(results[2].err) } }
  if not managed.models and not catalog.models then
    return nil, managed.err
  end
  if not managed.models then
    maki.log.warn("requesty: managed models unavailable")
  end
  if not catalog.models then
    maki.log.warn("requesty: full catalog unavailable")
  end
  local models, seen = {}, {}
  for _, model in ipairs(managed.models or {}) do
    models[#models + 1] = model
    seen[model.id] = true
  end
  for _, model in ipairs(catalog.models or {}) do
    if not seen[model.id] then
      models[#models + 1] = model
      seen[model.id] = true
    end
  end
  return models
end

local function prepare(ctx, model, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local index = 1
  local secrets = secret_values(config, keys)
  local base = (config.base_url or endpoint):gsub("/+$", "")
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
      local body, build_err =
        chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
      if not body then
        return nil, { kind = "config", message = tostring(build_err) }
      end
      body.requesty = { auto_cache = true }
      local intent = request.thinking or { mode = "off" }
      if request.model.thinking and request.model.thinking.mode ~= "none" then
        if intent.mode == "adaptive" then
          body.reasoning_effort = "high"
        elseif intent.mode == "effort" then
          body.reasoning_effort = ({
            minimal = "low",
            low = "low",
            medium = "medium",
            high = "high",
            xhigh = "high",
            max = "high",
          })[intent.level]
        end
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local response, send_err = ctx:send({
        url = base .. "/chat/completions",
        method = "POST",
        headers = headers(config, keys[index], true),
        body = encoded,
      })
      if not response then
        return nil, send_err
      end
      if response.status < 200 or response.status >= 300 then
        return nil, api_error(response, "Requesty request failed", secrets)
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

local function register()
  maki.api.register_provider({
    id = "requesty",
    display_name = "Requesty",
    models = {},
    discover = discover,
    prepare = prepare,
    auth = { { id = "api_key", label = "API key", login = login } },
    auth_status = auth_status,
    logout = logout,
    supports_thinking = true,
    accepts_arbitrary_models = true,
    fallback_context_window = 128000,
  })
end

return { register = register }
