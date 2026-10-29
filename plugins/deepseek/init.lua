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

local function prepare(_, model, config)
  config = config or {}
  local key = maki.uv.os_getenv(config.api_key_env or "DEEPSEEK_API_KEY")
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
    return nil, { kind = "config", message = "DeepSeek API key is not configured" }
  end
  local index = 1
  local base = (config.base_url or "https://api.deepseek.com"):gsub("/+$", "")
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
        ctx:send({ url = "https://api.deepseek.com/user/balance", method = "GET", headers = headers() })
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
        if not chunk or chunk == "" then
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
        if intent.mode == "effort" then
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
        ctx:send({ url = base .. "/chat/completions", method = "POST", headers = headers(), body = encoded })
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

maki.api.register_provider({ id = "deepseek", display_name = "DeepSeek", models = models, prepare = prepare })
