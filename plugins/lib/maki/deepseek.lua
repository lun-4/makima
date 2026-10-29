local chat = require("maki.chat_completions")

local models = {
  {
    id = "deepseek-v4-flash",
    prefixes = { "deepseek-v4-flash" },
    tier = "medium",
    family = "generic",
    default = true,
    context_window = 1000000,
    max_output_tokens = 384000,
    vision = false,
    thinking = { mode = "effort", levels = { "max" } },
    pricing = { input = 0.22, output = 0.66, cache_write = 0, cache_read = 0.007 },
  },
  {
    id = "deepseek-v4-pro",
    prefixes = { "deepseek-v4-pro" },
    tier = "strong",
    family = "generic",
    default = true,
    context_window = 1000000,
    max_output_tokens = 384000,
    vision = false,
    thinking = { mode = "effort", levels = { "max" } },
    pricing = { input = 0.66, output = 1.98, cache_write = 0, cache_read = 0.022 },
  },
}

local function split_keys(key)
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  return keys
end

local function credentials(ctx, config)
  local keys = split_keys(maki.uv.os_getenv(config.api_key_env or "DEEPSEEK_API_KEY"))
  if #keys > 0 then
    return keys, nil, "env"
  end
  local saved, err = ctx.credentials:read("api_key")
  if err then
    return nil, err
  end
  if saved ~= nil then
    if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
      return nil, { kind = "config", message = "Invalid DeepSeek credentials" }
    end
    return { saved.api_key }, nil, "stored"
  end
  keys = split_keys(config.api_key)
  if #keys == 0 then
    return nil, { kind = "config", message = "DeepSeek API key is not configured" }
  end
  return keys, nil, "config"
end

local function auth_status(ctx, config)
  local keys, err, source = credentials(ctx, config or {})
  if not keys then
    if err and err.message ~= "DeepSeek API key is not configured" then
      return nil, err
    end
    return { configured = false }
  end
  return { configured = true, source = source, method = "api_key" }
end

local function login(ctx, ui)
  local key, err = ui:input("DeepSeek API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  key = key:match("^%s*(.-)%s*$")
  if key == "" then
    return nil, { kind = "config", message = "DeepSeek API key is not configured" }
  end
  return ctx.credentials:write("api_key", { api_key = key })
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
  local override = maki.uv.os_getenv("DEEPSEEK_BASE_URL")
  return ((override and override ~= "" and override) or config.base_url or "https://api.deepseek.com"):gsub("/+$", "")
end

local function get_json(ctx, url, request_headers)
  local response, err = ctx:send({ url = url, method = "GET", headers = request_headers })
  if not response then
    return nil, type(err) == "table" and err or { kind = "network", message = "DeepSeek request failed" }
  end
  if response.status < 200 or response.status >= 300 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "DeepSeek request failed" }
  end
  local value, read_err = require("maki.json_response").read(response, 1048576)
  response:close()
  return value, read_err
end

local function discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local value, get_err = get_json(ctx, base_url(config) .. "/models", headers(config, keys[1]))
  if not value then
    return nil, get_err
  end
  local result = {}
  if type(value) ~= "table" or type(value.data) ~= "table" then
    return result
  end
  local function uint(value)
    return type(value) == "number" and value > 0 and value <= 4294967295 and value % 1 == 0 and value or nil
  end
  for _, item in ipairs(value.data) do
    if type(item) == "table" and type(item.id) == "string" and item.id ~= "" then
      local model = {
        id = item.id,
        context_window = uint(item.context_length) or uint(item.max_model_len) or uint(item.max_context_length),
        max_output_tokens = uint(item.max_tokens) or uint(item.max_output_length),
      }
      if type(item.input_modalities) == "table" then
        model.supports_vision = false
        for _, modality in ipairs(item.input_modalities) do
          if modality == "image" then
            model.supports_vision = true
          end
        end
      end
      local pricing = item.pricing
      if type(pricing) == "table" and type(pricing.prompt) == "string" and type(pricing.completion) == "string" then
        local input, output = tonumber(pricing.prompt), tonumber(pricing.completion)
        if input and output and input >= 0 and output >= 0 and input < math.huge and output < math.huge then
          local write = type(pricing.cache_creation) == "string" and tonumber(pricing.cache_creation) or 0
          local read = type(pricing.cache_read) == "string" and tonumber(pricing.cache_read) or 0
          model.pricing = { input = input, output = output, cache_write = write or 0, cache_read = read or 0 }
        end
      end
      result[#result + 1] = model
    end
  end
  table.sort(result, function(a, b)
    return a.id < b.id
  end)
  return result, nil
end

local function prepare(ctx, model, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local index = 1
  local base = base_url(config)
  local function current_headers()
    return headers(config, keys[index])
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
    fetch_usage = function(ctx)
      local response, err =
        ctx:send({ url = "https://api.deepseek.com/user/balance", method = "GET", headers = current_headers() })
      if not response then
        if type(err) == "table" then
          return nil, err
        end
        return nil, { kind = "network", message = "DeepSeek balance request failed" }
      end
      if response.status < 200 or response.status >= 300 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "DeepSeek balance request failed" }
      end
      local chunks, size = {}, 0
      while true do
        local chunk, read_err = response:read()
        if read_err then
          response:close()
          return nil, read_err
        end
        if chunk == nil then
          response:close()
          return nil, { kind = "network", message = "DeepSeek balance response read failed" }
        end
        if chunk == "" then
          break
        end
        size = size + #chunk
        if size > 65536 then
          response:close()
          return nil, { kind = "api", message = "DeepSeek balance response too large" }
        end
        chunks[#chunks + 1] = chunk
      end
      response:close()
      local value = maki.json.decode(table.concat(chunks))
      if type(value) ~= "table" or type(value.balance_infos) ~= "table" then
        return nil, { kind = "api", message = "Invalid DeepSeek balance response" }
      end
      local limits = {}
      for _, info in ipairs(value.balance_infos) do
        if
          type(info) ~= "table"
          or type(info.currency) ~= "string"
          or type(info.total_balance) ~= "string"
          or type(info.topped_up_balance) ~= "string"
          or type(info.granted_balance) ~= "string"
        then
          return nil, { kind = "api", message = "Invalid DeepSeek balance response" }
        end
        local symbol = info.currency == "USD" and "$" or info.currency == "CNY" and "¥" or ""
        limits[#limits + 1] = {
          kind = { type = "credits" },
          detail = "total: "
            .. symbol
            .. info.total_balance
            .. ", topped-up: "
            .. symbol
            .. info.topped_up_balance
            .. ", granted: "
            .. symbol
            .. info.granted_balance,
        }
      end
      return { limits = limits, by_model_today = {} }, nil
    end,
    stream = function(ctx, request, emit)
      local body, err = chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
      if not body then
        return nil, { kind = "config", message = tostring(err) }
      end
      local intent = request.thinking
      if intent.mode == "off" then
        body.thinking = { type = "disabled" }
      else
        body.thinking = { type = "enabled" }
        if intent.mode == "effort" or intent.mode == "budget" then
          body.reasoning_effort = "max"
        end
        if body.tools and not request.model.id:find("^deepseek%-reasoner") then
          for _, message in ipairs(body.messages) do
            if message.role == "assistant" and type(message.reasoning_content) ~= "string" then
              message.reasoning_content = ""
            end
          end
        end
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local response, send_err =
        ctx:send({ url = base .. "/chat/completions", method = "POST", headers = current_headers(), body = encoded })
      if not response then
        if type(send_err) == "table" then
          return nil, send_err
        end
        return nil, { kind = "network", message = "DeepSeek request failed" }
      end
      if response.status < 200 or response.status >= 300 then
        local value = response.headers and response.headers["retry-after"]
        local seconds = type(value) == "string" and tonumber(value:match("^%s*(%d+)%s*$"))
        response:close()
        return nil,
          {
            kind = "api",
            status = response.status,
            message = "DeepSeek request failed",
            retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil,
          }
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      if stream_err and stream_err.kind == "api" then
        return nil, { kind = "api", status = stream_err.status, message = "DeepSeek request failed" }
      end
      return result, stream_err
    end,
  },
    nil
end

local function register()
  maki.api.register_provider({
    id = "deepseek",
    display_name = "DeepSeek",
    docs = {
      auth_line = "`DEEPSEEK_API_KEY`",
      urls = { "https://api.deepseek.com" },
      features = "Thinking mode toggle (on/off), open-weight models",
    },
    models = models,
    auth = { { id = "api_key", label = "API key", login = login } },
    auth_status = auth_status,
    logout = function(ctx)
      return ctx.credentials:delete("api_key")
    end,
    discover = discover,
    prepare = prepare,
  })
end

return { register = register }
