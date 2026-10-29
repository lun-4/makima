local chat = require("maki.chat_completions")
local responses = require("maki.responses")

local function settings(ctx, config)
  config = config or {}
  local host = config.base_url or maki.uv.os_getenv("LLAMA_CPP_HOST")
  if not host or host == "" then
    return nil, { kind = "config", message = "LLAMA_CPP_HOST not set" }
  end
  local keys = {}
  local key = maki.uv.os_getenv(config.api_key_env or "LLAMA_CPP_API_KEY")
  if not key or key == "" then
    if ctx and ctx.credentials then
      local saved, err = ctx.credentials:read("api_key")
      if err and err.message ~= "credential access denied" and not config.api_key then
        return nil, err
      end
      if saved ~= nil and (type(saved) ~= "table" or type(saved.api_key) ~= "string") then
        return nil, { kind = "config", message = "Invalid LlamaCpp credentials" }
      end
      key = saved and saved.api_key
    end
    if not key or key == "" then
      key = config.api_key
    end
  end
  for part in (key or ""):gmatch("[^,]+") do
    local value = part:match("^%s*(.-)%s*$")
    if value ~= "" then
      keys[#keys + 1] = value
    end
  end
  return { root = host:gsub("/+$", ""), keys = keys, config = config }
end

local function headers(settings, index)
  local result = { ["Content-Type"] = "application/json" }
  if settings.keys[index] then
    result.Authorization = "Bearer " .. settings.keys[index]
  end
  for name, value in pairs(settings.config.headers or {}) do
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
  local limit = 4 * 1024 * 1024
  while size < limit do
    local chunk = response:read()
    if not chunk then
      chunks = { "unable to read error body" }
      break
    end
    if chunk == "" then
      break
    end
    chunks[#chunks + 1] = chunk:sub(1, limit - size)
    size = size + #chunk
  end
  response:close()
  return { kind = "api", status = response.status, message = table.concat(chunks) }
end

local function get_json(ctx, settings, path)
  local response, err = ctx:send({ url = settings.root .. path, method = "GET", headers = headers(settings, 1) })
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
      return nil, { kind = "api", message = "LlamaCpp response too large" }
    end
    chunks[#chunks + 1] = chunk
  end
  response:close()
  local value, decode_err = maki.json.decode(table.concat(chunks))
  if type(value) ~= "table" then
    return nil, { kind = "api", message = tostring(decode_err or "Invalid LlamaCpp response") }
  end
  return value
end

local function positive(value)
  return type(value) == "number" and value > 0 and value <= 4294967295 and value % 1 == 0 and value or nil
end

local function contains(values, value)
  for _, item in ipairs(type(values) == "table" and values or {}) do
    if item == value then
      return true
    end
  end
  return false
end

local function argument(args, flag)
  for i, value in ipairs(type(args) == "table" and args or {}) do
    if value == flag then
      return positive(tonumber(args[i + 1]))
    end
  end
end

local function discover(ctx, config)
  local s, err = settings(ctx, config)
  if not s then
    return nil, err
  end
  local props, props_err = get_json(ctx, s, "/props?autoload=false")
  if not props then
    return nil, props_err
  end
  local data, models_err = get_json(ctx, s, "/v1/models")
  if not data then
    return nil, models_err
  end
  local entries = type(data.data) == "table" and data.data or {}
  for _, entry in ipairs(entries) do
    if type(entry) ~= "table" or type(entry.id) ~= "string" then
      return nil, { kind = "api", message = "Invalid LlamaCpp model entry" }
    end
  end
  local mode = props.role == "router" and "router"
    or entries[1] and entries[1].max_model_len ~= nil and "legacy"
    or "single"
  local models = {}
  for _, entry in ipairs(entries) do
    local arch = entry.architecture
    if
      type(arch) ~= "table" or (contains(arch.input_modalities, "text") and contains(arch.output_modalities, "text"))
    then
      local context
      if mode == "legacy" then
        context = positive(entry.max_model_len) or positive(props.n_ctx)
      else
        context = type(entry.meta) == "table" and positive(entry.meta.n_ctx) or nil
        if mode == "router" and not context then
          local args = type(entry.status) == "table" and entry.status.args
          context = argument(args, "--ctx-size") or argument(args, "--fit-ctx")
        end
      end
      local vision
      if type(arch) == "table" then
        vision = contains(arch.input_modalities, "image")
      elseif mode ~= "router" and type(props.modalities) == "table" and type(props.modalities.vision) == "boolean" then
        vision = props.modalities.vision
      end
      models[#models + 1] = {
        id = entry.id,
        context_window = context or 128000,
        supports_vision = vision,
        pricing = { input = 0, output = 0, cache_write = 0, cache_read = 0 },
      }
    end
  end
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  return models
end

local function merge_body(target, fragment)
  for key, value in pairs(fragment) do
    if type(value) == "table" and type(target[key]) == "table" and value[1] == nil then
      merge_body(target[key], value)
    else
      target[key] = value
    end
  end
end

local function prepare(ctx, model, config)
  local s, err = settings(ctx, config)
  if not s then
    return nil, err
  end
  local index = 1
  return {
    model = model,
    key_count = math.max(1, #s.keys),
    rotate_key = function()
      if #s.keys < 2 then
        return false
      end
      index = index % #s.keys + 1
      return true
    end,
    stream = function(ctx, request, emit)
      local is_responses = s.config.protocol == "openai-responses"
      local body, build_err
      if is_responses then
        local intent = request.thinking or { mode = "off" }
        local reasoning
        if intent.mode == "adaptive" then
          reasoning = { effort = "medium", summary = "auto" }
        elseif intent.mode == "effort" then
          reasoning = { effort = ({ xhigh = "high", max = "high" })[intent.level] or intent.level, summary = "auto" }
        elseif intent.mode == "budget" then
          local maximum = request.model.max_output_tokens
              and math.max(1024, math.floor(request.model.max_output_tokens / 2))
            or 32768
          local percent = math.floor(intent.tokens * 100 / maximum)
          local effort = percent <= 10 and "minimal" or percent <= 20 and "low" or percent <= 40 and "medium" or "high"
          reasoning = { effort = effort, summary = "auto" }
        end
        body, build_err = responses.build_request(request, { reasoning = reasoning })
        if body then
          body.return_progress = true
        end
      else
        body, build_err = chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
        if body then
          merge_body(body, request.local_thinking)
        end
      end
      if not body then
        return nil, { kind = "config", message = tostring(build_err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local response, send_err = ctx:send({
        url = s.root .. (is_responses and "/v1/responses" or "/v1/chat/completions"),
        method = "POST",
        headers = headers(s, index),
        body = encoded,
      })
      if not response then
        return nil, send_err
      end
      if response.status < 200 or response.status >= 300 then
        return nil, api_error(response)
      end
      local result, stream_err = (is_responses and responses or chat).stream(response, emit)
      response:close()
      return result, stream_err
    end,
  },
    nil
end

local function register()
  maki.api.register_provider({
    id = "llama-cpp",
    display_name = "LlamaCpp",
    docs = {
      auth_line = "`LLAMA_CPP_API_KEY`",
      urls = { "http://localhost:8080/v1" },
      features = "Local or remote inference via LLAMA_CPP_HOST, set optional key via LLAMA_CPP_API_KEY",
      no_catalog_note = "Connects to any OpenAI-compatible `/v1` endpoint. Point `LLAMA_CPP_HOST` to your server address (defaults to `http://localhost:8080`).",
    },
    models = {},
    discover = discover,
    prepare = prepare,
    supports_thinking = true,
    accepts_arbitrary_models = true,
    fallback_context_window = 128000,
  })
end

return { register = register }
