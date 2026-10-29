local M = {}

local high = { mode = "effort", levels = { "high" } }
local none = { mode = "none" }

M.models = {
  {
    id = "mistral-medium-latest",
    prefixes = { "mistral-medium-latest", "mistral-medium-3.5", "mistral-medium-3-5", "mistral-medium-2604" },
    tier = "strong",
    family = "generic",
    default = true,
    context_window = 262144,
    vision = true,
    thinking = high,
    pricing = { input = 1.5, output = 7.5, cache_write = 0, cache_read = 0 },
  },
  {
    id = "glm-5-2",
    prefixes = { "glm-5-2", "zai-glm-5-2" },
    tier = "strong",
    family = "glm",
    context_window = 1000000,
    vision = false,
    thinking = high,
    pricing = { input = 1.4, output = 4.4, cache_write = 0, cache_read = 0.14 },
  },
  {
    id = "mistral-small-latest",
    prefixes = { "mistral-small-latest", "mistral-small-2603" },
    tier = "medium",
    family = "generic",
    default = true,
    context_window = 262144,
    vision = true,
    thinking = high,
    pricing = { input = 0.15, output = 0.6, cache_write = 0, cache_read = 0 },
  },
  {
    id = "ministral-14b-latest",
    prefixes = { "ministral-14b-latest", "ministral-14b-2512" },
    tier = "weak",
    family = "generic",
    default = true,
    context_window = 262144,
    vision = false,
    thinking = none,
    pricing = { input = 0.2, output = 0.2, cache_write = 0, cache_read = 0 },
  },
}

function M.convert_messages(messages)
  for _, message in ipairs(messages) do
    if message.role == "assistant" and message.reasoning_content ~= nil then
      local reasoning = message.reasoning_content
      message.reasoning_content = nil
      if type(reasoning) == "string" then
        local content = message.content
        local blocks = { { type = "thinking", thinking = { { type = "text", text = reasoning } } } }
        if type(content) == "string" and content ~= "" then
          blocks[#blocks + 1] = { type = "text", text = content }
        elseif type(content) == "table" then
          for _, block in ipairs(content) do
            blocks[#blocks + 1] = block
          end
        end
        message.content = blocks
      end
    end
  end
end

function M.build_request(request)
  local body, err = require("maki.chat_completions").build_request(
    request,
    { max_tokens_field = "max_tokens", include_stream_usage = true }
  )
  if not body then
    return nil, err
  end
  M.convert_messages(body.messages)
  if
    request.thinking
    and request.thinking.mode ~= "off"
    and request.model.thinking
    and request.model.thinking.mode ~= "none"
    and not request.model.id:match("^ministral%-")
  then
    body.reasoning_effort = "high"
  end
  return body
end

local function credentials(config)
  local key = maki.uv.os_getenv(config.api_key_env or "MISTRAL_API_KEY")
  if not key or key == "" then
    key = config.api_key
  end
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "Mistral API key is not configured" }
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
  return (config.base_url or "https://api.mistral.ai/v1"):gsub("/+$", "")
end

local function read_response(response)
  local chunks, size = {}, 0
  while true do
    local chunk, err = response:read()
    if not chunk then
      response:close()
      return nil, err
    end
    if chunk == "" then
      break
    end
    size = size + #chunk
    if size > 65536 then
      response:close()
      return nil, { kind = "api", message = "Mistral response too large" }
    end
    chunks[#chunks + 1] = chunk
  end
  response:close()
  return table.concat(chunks)
end

function M.discover(ctx, config)
  config = config or {}
  local keys, err = credentials(config)
  if not keys then
    return nil, err
  end
  local response, send_err =
    ctx:send({ url = base_url(config) .. "/models", method = "GET", headers = headers(config, keys[1]) })
  if not response then
    return nil, send_err
  end
  if response.status < 200 or response.status >= 300 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "Mistral request failed" }
  end
  local body, read_err = read_response(response)
  if not body then
    return nil, read_err
  end
  local data, decode_err = maki.json.decode(body)
  if not data or type(data.data) ~= "table" then
    return nil, { kind = "api", message = tostring(decode_err or "Invalid Mistral models response") }
  end
  local models = {}
  for _, model in ipairs(data.data) do
    local caps = model.capabilities
    if type(model.id) == "string" and type(caps) == "table" and caps.completion_chat == true then
      local context = model.max_context_length
      if type(context) ~= "number" or context < 0 or context > 4294967295 or context % 1 ~= 0 then
        context = nil
      end
      models[#models + 1] = {
        id = model.id,
        context_window = context,
        supports_thinking = type(caps.reasoning) == "boolean" and caps.reasoning or nil,
        supports_vision = caps.vision == true,
      }
      if caps.reasoning == false then
        models[#models].supports_thinking = false
      end
    end
  end
  return models
end

function M.prepare(_, model, config)
  config = config or {}
  local keys, err = credentials(config)
  if not keys then
    return nil, err
  end
  local index = 1
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
      local body, build_err = M.build_request(request)
      if not body then
        return nil, { kind = "config", message = tostring(build_err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local request_headers = headers(config, keys[index])
      if request.session_id then
        request_headers["x-affinity"] = request.session_id
      end
      local response, send_err = ctx:send({
        url = base_url(config) .. "/chat/completions",
        method = "POST",
        headers = request_headers,
        body = encoded,
      })
      if not response then
        return nil, send_err
      end
      if response.status < 200 or response.status >= 300 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "Mistral request failed" }
      end
      local result, stream_err = require("maki.chat_completions").stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

function M.register()
  maki.api.register_provider({
    id = "mistral",
    display_name = "Mistral",
    models = M.models,
    discover = M.discover,
    prepare = M.prepare,
    supports_thinking = true,
    accepts_arbitrary_models = true,
    fallback_context_window = 128000,
  })
end

return M
