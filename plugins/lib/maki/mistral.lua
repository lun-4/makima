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
    and (not request.model.thinking or request.model.thinking.mode ~= "none")
    and not request.model.id:match("^ministral%-")
  then
    body.reasoning_effort = "high"
  end
  return body
end

local function credentials(ctx, config)
  local key = maki.uv.os_getenv(config.api_key_env or "MISTRAL_API_KEY")
  local source = key and key ~= "" and "env" or nil
  if not key or key == "" then
    if ctx and ctx.credentials then
      local saved, err = ctx.credentials:read("api_key")
      if err then
        return nil, err
      end
      if saved ~= nil then
        if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
          return nil, { kind = "config", message = "Invalid Mistral credentials" }
        end
        key = saved.api_key
        source = "stored"
      end
    end
    if not key or key == "" then
      key = config.api_key
      source = "config"
    end
  end
  if key ~= nil and type(key) ~= "string" then
    return nil, { kind = "config", message = "Invalid Mistral credentials" }
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
  return keys, nil, source
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
  local base = maki.uv.os_getenv("MISTRAL_BASE_URL")
  if not base or base == "" then
    base = config.base_url or "https://api.mistral.ai/v1"
  end
  return base:gsub("/+$", "")
end

function M.discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local response, send_err =
    ctx:send({ url = base_url(config) .. "/models", method = "GET", headers = headers(config, keys[1]) })
  if not response then
    return nil, send_err
  end
  if response.status ~= 200 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "Mistral request failed" }
  end
  local data, read_err = require("maki.json_response").read(response, 4 * 1024 * 1024)
  response:close()
  if not data then
    return nil, read_err
  end
  if type(data) ~= "table" or type(data.data) ~= "table" then
    return nil, { kind = "api", message = "Invalid Mistral models response" }
  end
  local models = {}
  for _, model in ipairs(data.data) do
    local caps = type(model) == "table" and model.capabilities
    if
      type(model) == "table"
      and type(model.id) == "string"
      and model.id ~= ""
      and type(caps) == "table"
      and caps.completion_chat == true
    then
      local context = model.max_context_length
      if type(context) ~= "number" or context <= 0 or context > 4294967295 or context % 1 ~= 0 then
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
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  return models
end

function M.prepare(ctx, model, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local index = 1
  if model.id:match("^ministral%-") then
    model.thinking = none
  end
  local function safe_message(message)
    if type(message) ~= "string" or message == "" then
      return "Mistral request failed"
    end
    for _, key in ipairs(keys) do
      if message:find(key, 1, true) then
        return "Mistral request failed"
      end
    end
    for _, value in pairs(config.headers or {}) do
      if type(value) == "string" and value ~= "" and message:find(value, 1, true) then
        return "Mistral request failed"
      end
    end
    return message
  end
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
      if request.session_id ~= nil and request.session_id ~= maki.json.null() then
        if type(request.session_id) ~= "string" then
          return nil, { kind = "config", message = "Invalid Mistral session ID" }
        end
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
      if response.status ~= 200 then
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
        local retry = response.headers and response.headers["retry-after"]
        local seconds = type(retry) == "string" and tonumber(retry:match("^%s*(%d+)%s*$"))
        response:close()
        return nil,
          {
            kind = "api",
            status = response.status,
            message = safe_message(table.concat(chunks)),
            retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil,
          }
      end
      local result, stream_err = require("maki.chat_completions").stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

function M.login(ctx, ui)
  local key, err = ui:input("Mistral API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  if key:match("^%s*$") then
    return nil, { kind = "config", message = "Mistral API key is empty" }
  end
  return ctx.credentials:write("api_key", { api_key = key:match("^%s*(.-)%s*$") })
end

function M.auth_status(ctx, config)
  local keys, err, source = credentials(ctx, config or {})
  if err and err.message ~= "Mistral API key is not configured" then
    return nil, err
  end
  return { configured = keys ~= nil, source = source, method = keys and "api_key" or nil }
end

function M.logout(ctx)
  return ctx.credentials:delete("api_key")
end

function M.register()
  maki.api.register_provider({
    id = "mistral",
    display_name = "Mistral",
    docs = { auth_line = "`MISTRAL_API_KEY`", urls = { "https://api.mistral.ai/v1" } },
    models = M.models,
    discover = M.discover,
    prepare = M.prepare,
    auth = { { id = "api_key", label = "API key", login = M.login } },
    auth_status = M.auth_status,
    logout = M.logout,
    supports_thinking = true,
    accepts_arbitrary_models = true,
    fallback_context_window = 128000,
  })
end

return M
