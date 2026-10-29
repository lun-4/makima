local chat = require("maki.chat_completions")

local function settings(ctx, config)
  local key = maki.uv.os_getenv(config.api_key_env or "OLLAMA_API_KEY")
  if not key or key == "" then
    if ctx and ctx.credentials then
      local saved, err = ctx.credentials:read("api_key")
      if err and err.message ~= "credential access denied" and not config.api_key then
        return nil, nil, err
      end
      if saved ~= nil and (type(saved) ~= "table" or type(saved.api_key) ~= "string") then
        return nil, nil, { kind = "config", message = "Invalid Ollama credentials" }
      end
      key = saved and saved.api_key
    end
    if not key or key == "" then
      key = config.api_key
    end
  end
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  local host = config.base_url or maki.uv.os_getenv("OLLAMA_HOST")
  if not host then
    if #keys == 0 then
      return nil, nil, { kind = "config", message = "OLLAMA_HOST not set" }
    end
    return "https://ollama.com/v1", keys
  end
  return host:gsub("/+$", "") .. "/v1", keys
end

local function headers(config, key)
  local result = { ["Content-Type"] = "application/json" }
  if key then
    result.Authorization = "Bearer " .. key
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

local function api_error(response)
  local chunks, size = {}, 0
  while true do
    local chunk = response:read()
    if not chunk then
      chunks = { "unable to read error body" }
      break
    end
    if chunk == "" then
      break
    end
    local remaining = 4194304 - size
    chunks[#chunks + 1] = chunk:sub(1, remaining)
    size = size + #chunk
    if size >= 4194304 then
      break
    end
  end
  response:close()
  return { kind = "api", status = response.status, message = table.concat(chunks) }
end

local function json_request(ctx, url, method, request_headers, body)
  local response, err = ctx:send({ url = url, method = method, headers = request_headers, body = body })
  if not response then
    return nil, err
  end
  if response.status < 200 or response.status >= 300 then
    return nil, api_error(response)
  end
  local chunks, size = {}, 0
  while true do
    local chunk, read_err = response:read()
    if not chunk then
      response:close()
      return nil, read_err
    end
    if chunk == "" then
      break
    end
    size = size + #chunk
    if size > 4194304 then
      response:close()
      return nil, { kind = "api", message = "Ollama response too large" }
    end
    chunks[#chunks + 1] = chunk
  end
  response:close()
  local data, decode_err = maki.json.decode(table.concat(chunks))
  if not data then
    return nil, { kind = "api", message = tostring(decode_err) }
  end
  return data
end

local function u32(value)
  if type(value) == "number" and value >= 0 and value <= 4294967295 and value % 1 == 0 then
    return value
  end
end

local function details(show)
  if type(show) ~= "table" then
    return nil, nil
  end
  local context
  if type(show.model_info) == "table" then
    local names = {}
    for name in pairs(show.model_info) do
      if type(name) == "string" and name:match("%.context_length$") then
        names[#names + 1] = name
      end
    end
    table.sort(names)
    if names[1] then
      context = u32(show.model_info[names[1]])
    end
    if context == nil then
      context = u32(show.model_info.context_length)
    end
  end
  if context == nil and type(show.parameters) == "string" then
    for line in show.parameters:gmatch("[^\n]+") do
      local value = line:match("^%s*num_ctx +(%d+)%s*$")
      if value then
        local parsed = u32(tonumber(value))
        if parsed and parsed > 0 then
          context = parsed
          break
        end
      end
    end
  end
  local vision
  if type(show.capabilities) == "table" then
    vision = false
    for _, capability in ipairs(show.capabilities) do
      if capability == "vision" then
        vision = true
        break
      end
    end
  end
  return context, vision
end

local function discover(ctx, config)
  config = config or {}
  local base, keys, err = settings(ctx, config)
  if not base then
    return nil, err
  end
  local request_headers = headers(config, keys[1])
  local data, fetch_err = json_request(ctx, base .. "/models", "GET", request_headers)
  if not data then
    return nil, fetch_err
  end
  if type(data) ~= "table" or type(data.data) ~= "table" then
    return nil, { kind = "api", message = "Invalid Ollama models response" }
  end
  local models = {}
  local root = base:gsub("/v1$", "")
  for _, entry in ipairs(data.data) do
    if type(entry.id) ~= "string" then
      return nil, { kind = "api", message = "Invalid Ollama model id" }
    end
    local body, encode_err = maki.json.encode({ model = entry.id })
    if not body then
      return nil, { kind = "config", message = tostring(encode_err) }
    end
    local show = json_request(ctx, root .. "/api/show", "POST", request_headers, body)
    local context, vision = details(show)
    models[#models + 1] = {
      id = entry.id,
      context_window = context,
      supports_vision = vision,
      pricing = { input = 0, output = 0, cache_read = 0, cache_write = 0 },
    }
  end
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  return models
end

local function prepare(ctx, model, config)
  config = config or {}
  local base, keys, err = settings(ctx, config)
  if not base then
    return nil, err
  end
  local index = 1
  return {
    model = model,
    key_count = math.max(1, #keys),
    rotate_key = function()
      if #keys < 2 then
        return false
      end
      index = index % #keys + 1
      return true
    end,
    stream = function(ctx, request, emit)
      local responses = config.protocol == "openai-responses" and require("maki.responses") or nil
      local body, build_err
      if responses then
        body, build_err = responses.build_request(request)
        if body then
          body.return_progress = true
        end
      else
        body, build_err = chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
      end
      if not body then
        return nil, { kind = "config", message = tostring(build_err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local path = responses and "/responses" or "/chat/completions"
      local response, send_err =
        ctx:send({ url = base .. path, method = "POST", headers = headers(config, keys[index]), body = encoded })
      if not response then
        return nil, send_err
      end
      if response.status < 200 or response.status >= 300 then
        return nil, api_error(response)
      end
      local result, stream_err = (responses or chat).stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

maki.api.register_provider({
  id = "ollama",
  display_name = "Ollama",
  models = {},
  discover = discover,
  prepare = prepare,
  supports_thinking = true,
  accepts_arbitrary_models = true,
  fallback_context_window = 128000,
  fallback_max_output = 16384,
})
