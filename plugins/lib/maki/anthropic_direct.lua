local messages = require("maki.anthropic")
local sse = require("maki.sse")
local M = {}
local MAX_ERROR_BYTES = 65536
local MAX_BLOCKS = 4096
local ERROR_STATUS = {
  overloaded_error = 529,
  server_is_overloaded = 529,
  service_unavailable_error = 503,
  provider_overloaded = 503,
  provider_unavailable = 502,
  api_error = 500,
  server_error = 500,
  rate_limit_error = 429,
  rate_limit_exceeded = 429,
  tokens = 429,
  request_too_large = 413,
  not_found_error = 404,
  permission_error = 403,
  billing_error = 402,
  insufficient_quota = 402,
  authentication_error = 401,
  invalid_api_key = 401,
}

local function error_status(detail)
  local code = detail.code
  local numeric = tonumber(code)
  if not numeric or numeric % 1 ~= 0 or numeric < 100 or numeric >= 600 then
    numeric = nil
  end
  local metadata = type(detail.metadata) == "table" and detail.metadata or {}
  return ERROR_STATUS[code] or numeric or ERROR_STATUS[detail.type] or ERROR_STATUS[metadata.error_type] or 400
end

local function valid_index(index)
  return type(index) == "number" and index >= 0 and index < MAX_BLOCKS and index % 1 == 0
end

local profiles = {
  { "claude-haiku-4-5", "weak", true, 64000, 1, 5, 1.25, 0.1 },
  { "claude-sonnet-4-5", "medium", false, 64000, 3, 15, 3.75, 0.3 },
  { "claude-sonnet-4-6", "medium", false, 64000, 3, 15, 3.75, 0.3 },
  { "claude-sonnet-5", "medium", true, 128000, 2, 10, 2.5, 0.2 },
  { "claude-sonnet-4", "medium", false, 64000, 3, 15, 3.75, 0.3 },
  { "claude-opus-4-5", "strong", false, 64000, 5, 25, 6.25, 0.5 },
  { "claude-opus-4-6", "strong", false, 128000, 5, 25, 6.25, 0.5 },
  { "claude-opus-4-7", "strong", false, 128000, 5, 25, 6.25, 0.5 },
  { "claude-opus-4-8", "strong", false, 128000, 5, 25, 6.25, 0.5, true },
  { "claude-opus-5", "strong", true, 128000, 5, 25, 6.25, 0.5, true },
  { "claude-fable-5", "strong", false, 128000, 10, 50, 12.5, 1 },
  { "claude-opus-4-0", "strong", false, 32000, 15, 75, 18.75, 1.5 },
  { "claude-opus-4-1", "strong", false, 32000, 15, 75, 18.75, 1.5 },
}
M.models = {}
for _, p in ipairs(profiles) do
  M.models[#M.models + 1] = {
    id = p[1],
    prefixes = { p[1] },
    tier = p[2],
    default = p[3],
    family = "claude",
    context_window = 200000,
    max_output_tokens = p[4],
    vision = true,
    fast = p[9] or false,
    pricing = {
      input = p[5],
      output = p[6],
      cache_write = p[7],
      cache_read = p[8],
      fast = p[9] and { input = 10, output = 50 } or nil,
    },
    thinking = { mode = "adaptive", levels = { "minimal", "low", "medium", "high", "xhigh", "max" } },
  }
end

function M.stream(response, emit)
  local next_event = sse.new(response, 1024 * 1024)
  local content, arguments = assert(maki.json.decode("[]")), {}
  local usage = { input = 0, output = 0, cache_creation = 0, cache_read = 0 }
  local stop_reason
  while true do
    local event, err = next_event()
    if not event then
      if err then
        return nil, type(err) == "table" and err or { kind = "network", message = tostring(err) }
      end
      break
    end
    local value = maki.json.decode(event.data)
    if type(value) ~= "table" then
      value = {}
    end
    local kind = event.event
    if kind == "message_start" and type(value.message) == "table" and type(value.message.usage) == "table" then
      local u = value.message.usage
      usage = {
        input = u.input_tokens or 0,
        output = u.output_tokens or 0,
        cache_creation = u.cache_creation_input_tokens or 0,
        cache_read = u.cache_read_input_tokens or 0,
      }
    elseif kind == "content_block_start" and valid_index(value.index) and type(value.content_block) == "table" then
      local b = value.content_block
      local block
      if b.type == "text" then
        block = { type = "text", text = "" }
      elseif b.type == "thinking" then
        block = { type = "thinking", thinking = "" }
      elseif b.type == "redacted_thinking" then
        block = { type = b.type, data = b.data }
      elseif b.type == "tool_use" then
        block = { type = b.type, id = b.id, name = b.name, input = maki.json.null() }
        arguments[value.index] = {}
        local ok, emit_err = emit({ type = "tool_use_start", id = b.id, name = b.name })
        if not ok then
          return nil, emit_err
        end
      end
      if block then
        content[value.index + 1] = block
      end
    elseif kind == "content_block_delta" and valid_index(value.index) and type(value.delta) == "table" then
      local b, d = content[value.index + 1], value.delta
      if d.type == "text_delta" or d.type == "thinking_delta" then
        local field = d.type == "text_delta" and "text" or "thinking"
        local text = d[field]
        if type(text) == "string" and text ~= "" then
          if b and b[field] then
            b[field] = b[field] .. text
          end
          local ok, emit_err = emit({ type = d.type, text = text })
          if not ok then
            return nil, emit_err
          end
        end
      elseif d.type == "signature_delta" and b and b.type == "thinking" then
        b.signature = d.signature
      elseif d.type == "input_json_delta" and arguments[value.index] then
        arguments[value.index][#arguments[value.index] + 1] = d.partial_json or ""
      end
    elseif kind == "content_block_stop" and valid_index(value.index) then
      local b = content[value.index + 1]
      if b and b.type == "tool_use" then
        b.input = maki.json.decode(table.concat(arguments[value.index] or {}), { preserve_null = true }) or {}
      end
    elseif kind == "message_delta" then
      if type(value.usage) == "table" then
        usage.output = value.usage.output_tokens or 0
      end
      if type(value.delta) == "table" and type(value.delta.stop_reason) == "string" then
        stop_reason = ({ end_turn = "end_turn", tool_use = "tool_use", max_tokens = "max_tokens" })[value.delta.stop_reason]
          or "end_turn"
      end
    elseif kind == "error" then
      local detail = type(value.error) == "table" and value.error or {}
      local message = detail.message
      if type(message) ~= "string" or message:match("^%s*$") then
        message = "provider sent an error frame with no detail"
      end
      return nil, { kind = "api", status = error_status(detail), message = message }
    elseif kind == "message_stop" then
      break
    end
  end
  return { message = { role = "assistant", content = content }, usage = usage, stop_reason = stop_reason }
end

local function credentials(ctx, config)
  local key = maki.uv.os_getenv(config.api_key_env or "ANTHROPIC_API_KEY")
  if (not key or key == "") and ctx and ctx.credentials then
    local saved, err = ctx.credentials:read("api_key")
    if err and err.message ~= "credential access denied" then
      return nil, err
    end
    if saved ~= nil then
      if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
        return nil, { kind = "config", message = "Invalid Anthropic credentials" }
      end
      key = saved.api_key
    end
  end
  if not key or key == "" then
    key = config.api_key
  end
  if not key or key == "" then
    return nil, { kind = "config", message = "Anthropic API key is not configured" }
  end
  local keys = {}
  for part in key:gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "Anthropic API key is not configured" }
  end
  return keys
end

local function base_url(config)
  local base = maki.uv.os_getenv("ANTHROPIC_BASE_URL")
  if not base or base == "" then
    base = config.base_url or "https://api.anthropic.com"
  end
  return base:match("^[^?#]*"):gsub("/+$", ""):gsub("/v1/messages$", "")
end

local function auth_headers(config, key)
  local headers = { ["x-api-key"] = key, ["anthropic-version"] = "2023-06-01" }
  for name, value in pairs(config.headers or {}) do
    for existing in pairs(headers) do
      if existing:lower() == name:lower() then
        headers[existing] = nil
      end
    end
    headers[name] = value
  end
  return headers
end

function M.discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local models, cursor, seen = {}, nil, {}
  repeat
    local path = "/v1/models?limit=1000"
    if cursor then
      path = path .. "&after_id=" .. cursor
    end
    local response, send_err = ctx:send({
      url = base_url(config) .. path,
      method = "GET",
      headers = auth_headers(config, keys[1]),
    })
    if not response then
      return nil, send_err
    end
    if response.status ~= 200 then
      response:close()
      return nil, { kind = "api", status = response.status, message = "Anthropic model discovery failed" }
    end
    local page, read_err = require("maki.json_response").read(response, 4 * 1024 * 1024)
    response:close()
    if type(page) ~= "table" or type(page.data) ~= "table" then
      return nil, read_err or { kind = "api", message = "Invalid Anthropic models response" }
    end
    for _, item in ipairs(page.data) do
      if type(item.id) == "string" then
        models[#models + 1] = { id = item.id }
        if type(item.max_input_tokens) == "number" and item.max_input_tokens >= 1000000 then
          models[#models + 1] = { id = item.id .. "-1m" }
        end
      end
    end
    if page.has_more and (type(page.last_id) ~= "string" or page.last_id == "") then
      return nil, { kind = "api", message = "Invalid Anthropic model pagination cursor" }
    end
    cursor = page.has_more and page.last_id or nil
    if cursor and (type(cursor) ~= "string" or not cursor:match("^[%w_-]+$") or seen[cursor]) then
      return nil, { kind = "api", message = "Invalid Anthropic model pagination cursor" }
    end
    if cursor then
      seen[cursor] = true
    end
  until not cursor
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
  local function safe_message(message)
    if type(message) ~= "string" or message == "" then
      return "Anthropic request failed"
    end
    for _, secret in ipairs(keys) do
      if message:find(secret, 1, true) then
        return "Anthropic request failed"
      end
    end
    for _, secret in pairs(config.headers or {}) do
      if type(secret) == "string" and secret ~= "" and message:find(secret, 1, true) then
        return "Anthropic request failed"
      end
    end
    return message
  end
  local base = base_url(config)
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
    reload_auth = function(reload_ctx)
      local reloaded, reload_err = credentials(reload_ctx, config)
      if not reloaded then
        return nil, reload_err
      end
      keys, index = reloaded, 1
      return true
    end,
    fetch_usage = function(usage_ctx)
      return M.fetch_usage(usage_ctx, base, auth_headers(config, keys[index]), base)
    end,
    stream = function(ctx, request, emit)
      local body, err = messages.build_request(request)
      if not body then
        return nil, { kind = "config", message = tostring(err) }
      end
      local encoded, encode_err = maki.json.encode(body)
      if not encoded then
        return nil, { kind = "config", message = tostring(encode_err) }
      end
      local headers = auth_headers(config, keys[index])
      headers["content-type"] = "application/json"
      headers["anthropic-beta"] = messages.beta_header(request)
      local response, send_err =
        ctx:send({ url = base .. "/v1/messages", method = "POST", headers = headers, body = encoded })
      if not response then
        return nil, type(send_err) == "table" and send_err or { kind = "network", message = "Anthropic request failed" }
      end
      if response.status ~= 200 then
        local chunks, size = {}, 0
        while size < MAX_ERROR_BYTES do
          local chunk = response:read()
          if not chunk or chunk == "" then
            break
          end
          size = size + #chunk
          if size > MAX_ERROR_BYTES then
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
      local result, stream_err = M.stream(response, emit)
      response:close()
      if stream_err and stream_err.kind == "api" then
        stream_err.message = safe_message(stream_err.message)
      end
      return result, stream_err
    end,
  }
end

function M.parse_usage(value, parse_timestamp)
  if type(value) ~= "table" or (value.limits ~= nil and type(value.limits) ~= "table") then
    return nil, { kind = "api", message = "Invalid Anthropic usage response" }
  end
  local limits = {}
  local function add(kind, percent, reset)
    if type(percent) == "number" then
      limits[#limits + 1] = {
        kind = kind,
        percentage = math.clamp(math.floor(percent + 0.5), 0, 4294967295),
        reset_at = type(reset) == "string" and parse_timestamp and parse_timestamp(reset) or nil,
      }
    end
  end
  for _, lane in ipairs(value.limits or {}) do
    if type(lane) ~= "table" or type(lane.kind) ~= "string" then
      return nil, { kind = "api", message = "Invalid Anthropic usage response" }
    end
    local kind
    if lane.kind == "session" then
      kind = { type = "hours", hours = 5 }
    elseif lane.kind == "weekly_all" then
      kind = { type = "weekly" }
    elseif lane.kind == "weekly_scoped" then
      local scope = lane.scope
      local model = type(scope) == "table" and scope.model
      kind = {
        type = "weekly",
        model = type(model) == "table" and type(model.display_name) == "string" and model.display_name or nil,
      }
    else
      kind = { type = "other", label = lane.kind }
    end
    add(kind, lane.percent, lane.resets_at)
  end
  if #limits == 0 then
    for _, window in ipairs({
      { "five_hour", { type = "hours", hours = 5 } },
      { "seven_day", { type = "weekly" } },
      { "seven_day_sonnet", { type = "weekly", model = "Sonnet" } },
      { "seven_day_opus", { type = "weekly", model = "Opus" } },
    }) do
      local lane = value[window[1]]
      if type(lane) == "table" then
        add(window[2], lane.utilization, lane.resets_at)
      end
    end
  end
  local spend, extra = value.spend, value.extra_usage
  local percent, amount, exponent, currency
  if type(spend) == "table" and spend.enabled then
    percent = spend.percent or 0
    if type(spend.used) == "table" then
      amount, exponent, currency = spend.used.amount_minor, spend.used.exponent, spend.used.currency
    end
  elseif spend == nil and type(extra) == "table" and extra.is_enabled then
    percent, amount, exponent, currency = extra.utilization, extra.used_credits, extra.decimal_places, extra.currency
  end
  if type(percent) == "number" then
    add({ type = "credits" }, percent)
    if type(amount) == "number" then
      local formatted = string.format("%.2f", amount / 10 ^ (exponent or 2))
      limits[#limits].detail = (currency == nil or currency == "USD") and "$" .. formatted .. " spent"
        or formatted .. " " .. currency .. " spent"
    end
  end
  return { limits = limits, by_model_today = {} }
end

function M.fetch_usage(ctx, base, headers, configured_override)
  local authorization = false
  for name in pairs(headers) do
    if name:lower() == "authorization" then
      authorization = true
    end
  end
  local function origin(url)
    return url:match("^[^?#]*"):gsub("/+$", ""):gsub("/v1/messages$", "")
  end
  base = origin(base or "https://api.anthropic.com")
  local host = base:match("^https://([^/:]+)")
  if
    not authorization
    or (host ~= "api.anthropic.com" and (not configured_override or base ~= origin(configured_override)))
  then
    return nil
  end
  local usage_headers = {}
  for name, value in pairs(headers) do
    if name:lower() ~= "anthropic-beta" then
      usage_headers[name] = value
    end
  end
  usage_headers["anthropic-beta"] = "oauth-2025-04-20"
  local response, err = ctx:send({ url = base .. "/api/oauth/usage", method = "GET", headers = usage_headers })
  if not response then
    return nil, err
  end
  if response.status ~= 200 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "Anthropic usage request failed" }
  end
  local value, read_err = require("maki.json_response").read(response, 65536)
  response:close()
  if read_err then
    return nil, read_err
  end
  return M.parse_usage(value, maki.time.parse_rfc3339)
end

function M.register()
  maki.api.register_provider({
    id = "anthropic",
    display_name = "Anthropic",
    docs = {
      auth_line = "`ANTHROPIC_API_KEY`",
      urls = { "https://api.anthropic.com/v1/messages" },
      features = "Prompt caching, thinking mode (adaptive/budgeted), advanced tool use",
    },
    models = M.models,
    discover = M.discover,
    prepare = M.prepare,
  })
end

return M
