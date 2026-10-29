local chat = require("maki.chat_completions")
local parameter_flags = {}
local endpoint = "https://api.tensorx.ai/v1"

local function credentials(ctx, config)
  local key = maki.uv.os_getenv(config.api_key_env or "TENSORX_API_KEY")
  local source = "env"
  if not key or not key:find("[^,%s]") then
    source = "stored"
    if ctx and ctx.credentials then
      local saved, err = ctx.credentials:read("api_key")
      if err then
        return nil, err
      end
      if saved ~= nil then
        if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
          return nil, { kind = "config", message = "Invalid TensorX credentials" }
        end
        return { saved.api_key }, nil, source
      end
    end
    key = config.api_key
    source = "config"
  end
  if key ~= nil and type(key) ~= "string" then
    return nil, { kind = "config", message = "Invalid TensorX credentials" }
  end
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "TensorX API key is not configured" }
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

local function discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local response, send_err =
    ctx:send({ url = endpoint .. "/model/info", method = "GET", headers = headers(config, keys[1]) })
  if not response then
    return nil, send_err
  end
  if response.status < 200 or response.status >= 300 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "TensorX request failed" }
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
      return nil, { kind = "api", message = "TensorX models response too large" }
    end
    chunks[#chunks + 1] = chunk
  end
  response:close()
  local data, decode_err = maki.json.decode(table.concat(chunks))
  if not data then
    return nil, { kind = "api", message = tostring(decode_err) }
  end
  local models, flags = {}, {}
  for _, entry in ipairs(type(data.data) == "table" and data.data or {}) do
    local info = type(entry) == "table" and entry.model_info
    if
      type(entry) == "table"
      and type(entry.model_name) == "string"
      and type(info) == "table"
      and (type(info.mode) ~= "string" or info.mode == "chat")
    then
      local context = info.max_tokens
      if type(context) ~= "number" or context < 0 or context % 1 ~= 0 then
        context = info.max_input_tokens
      end
      if type(context) ~= "number" or context < 0 or context > 4294967295 or context % 1 ~= 0 then
        context = nil
      end
      local pricing
      if type(info.input_cost_per_token) == "number" or type(info.output_cost_per_token) == "number" then
        pricing = {
          input = (type(info.input_cost_per_token) == "number" and info.input_cost_per_token or 0) * 1000000,
          output = (type(info.output_cost_per_token) == "number" and info.output_cost_per_token or 0) * 1000000,
          cache_write = (
            type(info.cache_creation_input_token_cost) == "number" and info.cache_creation_input_token_cost or 0
          ) * 1000000,
          cache_read = (type(info.cache_read_input_token_cost) == "number" and info.cache_read_input_token_cost or 0)
            * 1000000,
        }
      end
      local model = {
        id = entry.model_name,
        context_window = context,
        pricing = pricing,
        supports_vision = info.supports_vision == true,
      }
      if type(info.supports_reasoning) == "boolean" then
        model.supports_thinking = info.supports_reasoning
      end
      models[#models + 1] = model
      local supported = {}
      if type(info.supported_openai_params) == "table" then
        for _, param in ipairs(info.supported_openai_params) do
          if type(param) == "string" then
            supported[param] = true
          end
        end
      end
      flags[entry.model_name] = supported
    end
  end
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  parameter_flags = flags
  return models
end

local function prepare(ctx, model, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local index = 1
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
      local flags = parameter_flags[request.model.id] or {}
      local intent = request.thinking or { mode = "off" }
      local enabled = intent.mode ~= "off"
      if flags.thinking then
        body.thinking = enabled
      end
      if flags.reasoning_effort then
        if not enabled then
          body.reasoning_effort = "none"
        elseif intent.mode == "adaptive" then
          body.reasoning_effort = "high"
        elseif intent.mode == "effort" then
          body.reasoning_effort = ({ minimal = "low", low = "low", medium = "medium", high = "high", max = "high" })[intent.level]
        end
      elseif
        not flags.thinking
        and enabled
        and request.model.id:match("^deepseek/")
        and not request.model.id:match("^deepseek/deepseek%-reasoner")
      then
        body.chat_template_kwargs = { thinking = true }
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
        return nil, send_err
      end
      if response.status < 200 or response.status >= 300 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "TensorX request failed" }
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

local function login(ctx, ui)
  local key, err = ui:input("TensorX API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  if key:match("^%s*$") then
    return nil, { kind = "config", message = "TensorX API key is empty" }
  end
  return ctx.credentials:write("api_key", { api_key = key:match("^%s*(.-)%s*$") })
end

local function auth_status(ctx, config)
  local keys, err, source = credentials(ctx, config or {})
  if not keys then
    if err and err.message ~= "TensorX API key is not configured" then
      return nil, err
    end
    return { configured = false }
  end
  return { configured = true, method = "api_key", source = source }
end

maki.api.register_provider({
  id = "tensorx",
  display_name = "TensorX",
  models = {},
  discover = discover,
  prepare = prepare,
  auth = { { id = "api_key", label = "API key", login = login } },
  auth_status = auth_status,
  logout = function(ctx)
    return ctx.credentials:delete("api_key")
  end,
  supports_thinking = true,
  accepts_arbitrary_models = true,
  fallback_context_window = 200000,
})
