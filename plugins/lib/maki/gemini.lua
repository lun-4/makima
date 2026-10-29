local M = {}

local FLASH_MAX_THINKING = 24576
local PRO_MAX_THINKING = 32768
local MIN_THINKING = 1024
local percentages = { minimal = 10, low = 20, medium = 40, high = 60, xhigh = 80, max = 100 }

local function array()
  return assert(maki.json.decode("[]"))
end

function M.convert_messages(messages)
  local names, result = {}, array()
  for _, message in ipairs(messages) do
    for _, block in ipairs(message.content) do
      if block.type == "tool_use" then
        names[block.id] = block.name
      end
    end
  end
  for _, message in ipairs(messages) do
    local parts, images = array(), array()
    local has_results = false
    for _, block in ipairs(message.content) do
      if block.type == "tool_result" then
        has_results = true
        break
      end
    end
    for _, block in ipairs(message.content) do
      local part
      if block.type == "text" then
        part = { text = block.text }
      elseif block.type == "thinking" then
        part = { text = block.thinking, thought = true, thoughtSignature = block.signature }
      elseif block.type == "tool_use" then
        part = {
          functionCall = { name = block.name, args = block.input },
          thoughtSignature = block.thought_signature,
        }
      elseif block.type == "tool_result" then
        local value, err = maki.json.decode(block.content, { preserve_null = true })
        if err then
          value = block.content
        end
        local encoded = not err and maki.json.encode(value)
        if not encoded or encoded:sub(1, 1) ~= "{" then
          value = { result = value }
        end
        if block.is_error then
          value = { error = value }
        end
        part = { functionResponse = { name = names[block.tool_use_id] or "unknown", response = value } }
      elseif block.type == "image" then
        part = { inlineData = { mimeType = block.source.media_type, data = block.source.data } }
        if has_results then
          images[#images + 1] = part
          part = nil
        end
      end
      if part then
        parts[#parts + 1] = part
      end
    end
    if #parts > 0 then
      result[#result + 1] = { role = message.role == "assistant" and "model" or "user", parts = parts }
    end
    if #images > 0 then
      result[#result + 1] = { role = "user", parts = images }
    end
  end
  return result
end

local function strip_additional_properties(value)
  if type(value) == "table" then
    value.additionalProperties = nil
    for _, item in pairs(value) do
      strip_additional_properties(item)
    end
  end
end

function M.convert_tools(tools)
  local result = array()
  if type(tools) ~= "table" then
    return result
  end
  for _, tool in ipairs(tools) do
    if type(tool) == "table" and type(tool.name) == "string" and tool.description ~= nil then
      local decl = { name = tool.name, description = type(tool.description) == "string" and tool.description or "" }
      local schema = tool.input_schema
      if type(schema) == "table" and type(schema.properties) == "table" and next(schema.properties) ~= nil then
        local properties, err = maki.json.encode(schema.properties)
        if not properties then
          return nil, err
        end
        if properties:sub(1, 1) == "{" then
          local encoded, schema_err = maki.json.encode(schema)
          if not encoded then
            return nil, schema_err
          end
          decl.parameters = assert(maki.json.decode(encoded, { preserve_null = true }))
          strip_additional_properties(decl.parameters)
        end
      end
      result[#result + 1] = decl
    end
  end
  return result
end

function M.build_request(request)
  local body = { contents = M.convert_messages(request.messages) }
  if request.system and request.system ~= "" then
    body.systemInstruction = { parts = { { text = request.system } } }
  end
  local thinking = request.thinking
  local config = {}
  if thinking and thinking.mode ~= "off" then
    local thought = { includeThoughts = true }
    if thinking.mode ~= "adaptive" then
      local maximum = request.model.id:find("flash", 1, true) and FLASH_MAX_THINKING or PRO_MAX_THINKING
      if request.model.max_output_tokens then
        maximum = math.min(maximum, math.max(MIN_THINKING, math.floor(request.model.max_output_tokens / 2)))
      end
      local tokens = thinking.tokens
      if thinking.mode == "effort" then
        tokens = math.floor(maximum * percentages[thinking.level] / 100)
      end
      thought.thinkingBudget = math.min(maximum, math.max(MIN_THINKING, tokens))
    end
    config.thinkingConfig = thought
  end
  if request.model.max_output_tokens ~= nil then
    config.maxOutputTokens = request.model.max_output_tokens
  end
  if next(config) then
    body.generationConfig = config
  end
  local tools, err = M.convert_tools(request.tools)
  if not tools then
    return nil, err
  end
  if #tools > 0 then
    body.tools = { { functionDeclarations = tools } }
  end
  return body
end

local next_id = 0
local id_tag = tostring({}):gsub("[^%w]", "")

function M.stream(body, emit, max_event_bytes)
  local next_event = require("maki.sse").new(body, max_event_bytes or 1024 * 1024)
  local content = array()
  local usage = { input = 0, output = 0, cache_read = 0, cache_creation = 0 }
  local stop_reason, thinking_open = nil, false

  local function close_thinking()
    if thinking_open then
      thinking_open = false
      return emit({ type = "thinking_block_end" })
    end
    return true
  end

  while true do
    local event, err = next_event()
    if not event then
      if err then
        if type(err) == "table" then
          return nil, err
        end
        return nil, { kind = err == "response is closed" and "closed" or "network", message = tostring(err) }
      end
      break
    end
    local chunk = maki.json.decode(event.data, { preserve_null = true })
    if type(chunk) == "table" then
      local meta = chunk.usageMetadata
      if type(meta) == "table" then
        local cached = tonumber(meta.cachedContentTokenCount) or 0
        usage.input = math.max(0, (tonumber(meta.promptTokenCount) or 0) - cached)
        usage.output = (tonumber(meta.candidatesTokenCount) or 0) + (tonumber(meta.thoughtsTokenCount) or 0)
        usage.cache_read = cached
      end
      for _, candidate in ipairs(type(chunk.candidates) == "table" and chunk.candidates or {}) do
        if candidate.finishReason then
          stop_reason = candidate.finishReason == "MAX_TOKENS" and "max_tokens" or "end_turn"
        end
        local parts = type(candidate.content) == "table" and candidate.content.parts or {}
        for _, part in ipairs(type(parts) == "table" and parts or {}) do
          local call = part.functionCall
          if type(call) == "table" and type(call.name) == "string" then
            local ok, send_err = close_thinking()
            if not ok then
              return nil, send_err
            end
            next_id = next_id + 1
            local id = "call_" .. call.name .. "_" .. id_tag .. "_" .. next_id
            ok, send_err = emit({ type = "tool_use_start", id = id, name = call.name })
            if not ok then
              return nil, send_err
            end
            content[#content + 1] = {
              type = "tool_use",
              id = id,
              name = call.name,
              input = call.args or assert(maki.json.decode("null", { preserve_null = true })),
              thought_signature = call.thoughtSignature or part.thoughtSignature,
            }
            stop_reason = "tool_use"
          elseif type(part.text) == "string" then
            local thought = part.thought == true
            if not thought and part.text ~= "" then
              local ok, send_err = close_thinking()
              if not ok then
                return nil, send_err
              end
            end
            if part.text ~= "" then
              local ok, send_err = emit({ type = thought and "thinking_delta" or "text_delta", text = part.text })
              if not ok then
                return nil, send_err
              end
              if thought then
                thinking_open = true
              end
            end
            if thought or part.text ~= "" then
              local kind = thought and "thinking" or "text"
              local key = thought and "thinking" or "text"
              local last = content[#content]
              if not last or last.type ~= kind then
                last = { type = kind, [key] = "" }
                content[#content + 1] = last
              end
              last[key] = last[key] .. part.text
              if thought and part.thoughtSignature ~= nil then
                last.signature = part.thoughtSignature
              end
            end
          end
        end
      end
    end
  end
  local ok, err = close_thinking()
  if not ok then
    return nil, err
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

local BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
local models = {
  {
    id = "gemini-2.5-pro",
    tier = "strong",
    pricing = { input = 1.25, output = 5, cache_write = 0, cache_read = 0.31 },
  },
  {
    id = "gemini-2.5-flash",
    tier = "medium",
    pricing = { input = 0.15, output = 0.60, cache_write = 0, cache_read = 0.04 },
  },
  {
    id = "gemini-2.0-flash-lite",
    tier = "weak",
    pricing = { input = 0.075, output = 0.30, cache_write = 0, cache_read = 0.01 },
  },
}
for _, model in ipairs(models) do
  model.prefixes = { model.id }
  model.family = "gemini"
  model.default = true
  model.vision = true
  model.context_window = 1048576
  model.max_output_tokens = 65536
  model.thinking =
    { mode = "budget", min_tokens = 1024, max_tokens = model.id:find("flash", 1, true) and 24576 or 32768 }
end
M.models = models

local function settings(ctx, config)
  config = config or {}
  local key = maki.uv.os_getenv(config.api_key_env or "GEMINI_API_KEY")
  local source = "env"
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    local saved, err
    if ctx and ctx.credentials then
      saved, err = ctx.credentials:read("api_key")
      if err and err.message ~= "credential access denied" then
        return nil, err
      end
    end
    if saved ~= nil and (type(saved) ~= "table" or type(saved.api_key) ~= "string") then
      return nil, { kind = "config", message = "Invalid Google credentials" }
    end
    key = saved and saved.api_key or config.api_key
    source = saved and "stored" or "config"
    if type(key) == "string" and key ~= "" then
      keys[1] = key
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "Google API key is not configured" }
  end
  local base = maki.uv.os_getenv("GOOGLE_BASE_URL")
  return {
    keys = keys,
    source = source,
    base = (base and base ~= "" and base or config.base_url or BASE_URL):gsub("/+$", ""),
    headers = config.headers or {},
  }
end

local function url_encode(value)
  return (value:gsub("([^%w%-._~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end))
end

local function headers(settings, key)
  local result = { ["x-goog-api-key"] = key, ["Content-Type"] = "application/json" }
  for name, value in pairs(settings.headers) do
    for existing in pairs(result) do
      if existing:lower() == name:lower() then
        result[existing] = nil
      end
    end
    result[name] = value
  end
  return result
end

local function response_error(response, opts)
  local retry = response.headers and response.headers["retry-after"]
  local seconds = type(retry) == "string" and retry:match("^%s*(%d+)%s*$")
  seconds = seconds and tonumber(seconds)
  local chunks, size = {}, 0
  while size < 65536 do
    local chunk, err = response:read()
    if err or not chunk then
      chunks = { "unable to read error body" }
      break
    end
    if chunk == "" then
      break
    end
    size = size + #chunk
    if size > 65536 then
      chunks = { "Google error response too large" }
      break
    end
    chunks[#chunks + 1] = chunk
  end
  response:close()
  local message = table.concat(chunks)
  for _, secret in ipairs(opts.keys) do
    if secret ~= "" and message:find(secret, 1, true) then
      message = "Google request failed"
      break
    end
  end
  for _, secret in pairs(opts.headers) do
    if type(secret) == "string" and secret ~= "" and message:find(secret, 1, true) then
      message = "Google request failed"
      break
    end
  end
  return {
    kind = "api",
    status = response.status,
    message = message,
    retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil,
  }
end

function M.prepare(ctx, model, config)
  local opts, err = settings(ctx, config)
  if not opts then
    return nil, err
  end
  local index = 1
  return {
    model = model,
    key_count = #opts.keys,
    reload_auth = function(reload_ctx)
      local refreshed, reload_err = settings(reload_ctx, config)
      if not refreshed then
        return nil, reload_err
      end
      opts.keys = refreshed.keys
      index = 1
      return true
    end,
    rotate_key = function()
      if #opts.keys < 2 then
        return false
      end
      index = index % #opts.keys + 1
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
      local response, send_err = ctx:send({
        url = opts.base .. "/models/" .. url_encode(request.model.id) .. ":streamGenerateContent?alt=sse",
        method = "POST",
        headers = headers(opts, opts.keys[index]),
        body = encoded,
      })
      if not response then
        return nil, type(send_err) == "table" and send_err or { kind = "network", message = "Google request failed" }
      end
      if response.status < 200 or response.status >= 300 then
        return nil, response_error(response, opts)
      end
      local result, stream_err = M.stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

function M.discover(ctx, config)
  local opts, err = settings(ctx, config)
  if not opts then
    return nil, err
  end
  local response, send_err = ctx:send({
    url = opts.base .. "/models?key=" .. url_encode(opts.keys[1]) .. "&pageSize=1000",
    method = "GET",
    headers = headers(opts, opts.keys[1]),
  })
  if not response then
    return nil, type(send_err) == "table" and send_err or { kind = "network", message = "Google discovery failed" }
  end
  if response.status ~= 200 then
    return nil, response_error(response, opts)
  end
  local value, read_err = require("maki.json_response").read(response, 4 * 1024 * 1024)
  response:close()
  if read_err then
    return nil, read_err
  end
  if type(value) ~= "table" or type(value.models) ~= "table" then
    return nil, { kind = "api", message = "Invalid Google models response" }
  end
  local result = {}
  for _, model in ipairs(value.models) do
    if type(model.name) == "string" and type(model.supportedGenerationMethods) == "table" then
      for _, method in ipairs(model.supportedGenerationMethods) do
        if method == "generateContent" then
          result[#result + 1] = { id = model.name:gsub("^models/", "") }
          break
        end
      end
    end
  end
  table.sort(result, function(a, b)
    return a.id < b.id
  end)
  return result
end

function M.login(ctx, ui)
  local key, err = ui:input("Google API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  key = key:match("^%s*(.-)%s*$")
  if key == "" then
    return nil, { kind = "config", message = "Google API key is not configured" }
  end
  return ctx.credentials:write("api_key", { api_key = key })
end

function M.auth_status(ctx, config)
  local opts, err = settings(ctx, config)
  if not opts then
    if err.message == "Google API key is not configured" then
      return { configured = false }
    end
    return nil, err
  end
  return { configured = true, source = opts.source, method = "api_key" }
end

function M.logout(ctx)
  return ctx.credentials:delete("api_key")
end

function M.register()
  maki.api.register_provider({
    id = "google",
    display_name = "Google",
    docs = {
      auth_line = "`GEMINI_API_KEY`",
      urls = { BASE_URL },
      features = "Native Gemini API with thinking support",
    },
    models = M.models,
    auth = { { id = "api_key", label = "API key", login = M.login } },
    auth_status = M.auth_status,
    logout = M.logout,
    discover = M.discover,
    prepare = M.prepare,
  })
end

return M
