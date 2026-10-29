local M = {}

local thinking = { mode = "effort", levels = { "minimal", "low", "medium", "high" } }
M.models = {
  {
    id = "qwen3.5-122b",
    prefixes = { "qwen3.5-122b" },
    tier = "strong",
    family = "generic",
    default = true,
    context_window = 120000,
    max_output_tokens = 120000,
    vision = true,
    thinking = thinking,
    pricing = { input = 1, output = 4.2, cache_write = 0, cache_read = 0 },
  },
  {
    id = "qwen3-coder-next",
    prefixes = { "qwen3-coder-next" },
    tier = "medium",
    family = "generic",
    default = true,
    context_window = 120000,
    max_output_tokens = 120000,
    vision = false,
    thinking = thinking,
    pricing = { input = 0.5, output = 2, cache_write = 0, cache_read = 0 },
  },
  {
    id = "qwen3.5-9b",
    prefixes = { "qwen3.5-9b" },
    tier = "weak",
    family = "generic",
    default = true,
    context_window = 80000,
    max_output_tokens = 80000,
    vision = false,
    thinking = thinking,
    pricing = { input = 0.07, output = 0.35, cache_write = 0, cache_read = 0 },
  },
}

local function credentials(ctx, config)
  local key = maki.uv.os_getenv(config.api_key_env or "REGOLO_API_KEY")
  local source = "env"
  if not key or not key:find("[^,%s]") then
    key = nil
    source = "stored"
    if ctx and ctx.credentials then
      local saved, err = ctx.credentials:read("api_key")
      if err then
        return nil, err
      end
      if saved ~= nil then
        if type(saved) ~= "table" or type(saved.api_key) ~= "string" or saved.api_key == "" then
          return nil, { kind = "config", message = "Invalid Regolo credentials" }
        end
        return { saved.api_key }, nil, source
      end
    end
    if not key or key == "" then
      key = config.api_key
      source = "config"
    end
  end
  if key ~= nil and type(key) ~= "string" then
    return nil, { kind = "config", message = "Invalid Regolo credentials" }
  end
  local keys = {}
  for part in (key or ""):gmatch("[^,]+") do
    local trimmed = part:match("^%s*(.-)%s*$")
    if trimmed ~= "" then
      keys[#keys + 1] = trimmed
    end
  end
  if #keys == 0 then
    return nil, { kind = "config", message = "Regolo API key is not configured" }
  end
  return keys, nil, source
end

local function base_url(config)
  return (config.base_url or "https://api.regolo.ai/v1"):gsub("/+$", "")
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

local function api_error(response)
  local value = response.headers and response.headers["retry-after"]
  local seconds = type(value) == "string" and tonumber(value:match("^%s*(%d+)%s*$")) or nil
  local retry_after_ms = seconds and seconds > 0 and seconds <= 9007199254740 and seconds * 1000 or nil
  local status = response.status
  response:close()
  return { kind = "api", status = status, message = "Regolo request failed", retry_after_ms = retry_after_ms }
end

local function get(ctx, url, request_headers)
  local response, err = ctx:send({ url = url, method = "GET", headers = request_headers })
  if not response then
    return nil, err
  end
  if response.status ~= 200 then
    return nil, api_error(response)
  end
  local value, read_err = require("maki.json_response").read(response, 1048576)
  response:close()
  return value, read_err
end

function M.join_models(ids, groups)
  local by_id = {}
  for _, group in ipairs(groups) do
    if group.mode == "chat" and type(group.model_group) == "string" then
      by_id[group.model_group] = group
    end
  end
  local models = {}
  for _, id in ipairs(ids) do
    local group = by_id[id]
    if group then
      local pricing
      if type(group.input_cost_per_token) == "number" and type(group.output_cost_per_token) == "number" then
        pricing = {
          input = group.input_cost_per_token * 1000000,
          output = group.output_cost_per_token * 1000000,
          cache_write = 0,
          cache_read = 0,
        }
      end
      models[#models + 1] = {
        id = id,
        context_window = group.max_input_tokens or group.max_tokens,
        max_output_tokens = group.max_output_tokens,
        pricing = pricing,
        supports_thinking = group.supports_reasoning == true,
        supports_vision = group.supports_vision == true,
      }
    end
  end
  return models
end

function M.discover(ctx, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local base = base_url(config)
  local request_headers = headers(config, keys[1])
  local response, get_err = get(ctx, base .. "/models", request_headers)
  if not response then
    return nil, get_err
  end
  local ids = {}
  local data = type(response) == "table" and type(response.data) == "table" and response.data or {}
  for _, model in ipairs(data) do
    if type(model) == "table" and type(model.id) == "string" then
      ids[#ids + 1] = model.id
    end
  end
  table.sort(ids)
  local groups = get(ctx, base:gsub("/v1$", "") .. "/model_group/info", request_headers)
  if type(groups) == "table" and type(groups.data) == "table" then
    local valid = true
    for _, group in ipairs(groups.data) do
      if
        type(group) ~= "table"
        or type(group.model_group) ~= "string"
        or type(group.mode) ~= "string"
        or type(group.supports_reasoning) ~= "boolean"
        or type(group.supports_vision) ~= "boolean"
      then
        valid = false
        break
      end
    end
    if valid then
      return M.join_models(ids, groups.data)
    end
  end
  local models = {}
  for _, id in ipairs(ids) do
    models[#models + 1] = { id = id }
  end
  return models
end

function M.prepare(ctx, model, config)
  config = config or {}
  local keys, err = credentials(ctx, config)
  if not keys then
    return nil, err
  end
  local index = 1
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
    fetch_usage = function(ctx)
      local root = base:gsub("/v1$", "")
      local request_headers = headers(config, keys[index])
      local key, key_err = get(ctx, root .. "/key/info", request_headers)
      if not key then
        return nil, key_err
      end
      if type(key.info) ~= "table" or type(key.info.spend) ~= "number" then
        return nil, { kind = "api", message = "Invalid Regolo key info response" }
      end
      local info = key.info
      local detail = string.format("$%.2f spent", info.spend)
      local percentage
      if type(info.max_budget) == "number" then
        detail = detail .. string.format(" of $%.2f budget", info.max_budget)
        if info.max_budget > 0 then
          percentage = math.clamp(math.floor(info.spend / info.max_budget * 100), 0, 100)
        end
      end
      local reset_at = type(info.budget_reset_at) == "string" and maki.time.parse_rfc3339(info.budget_reset_at) or nil
      local limits = { { kind = { type = "credits" }, percentage = percentage, reset_at = reset_at, detail = detail } }
      local now = maki.time.now().secs
      local day = os.date("!%Y-%m-%d", now)
      local query = "?start_date=" .. day .. "&end_date=" .. day
      local activity = get(ctx, root .. "/global/activity" .. query, request_headers)
      if
        type(activity) == "table"
        and type(activity.sum_api_requests) == "number"
        and type(activity.sum_total_tokens) == "number"
      then
        limits[#limits + 1] = {
          kind = { type = "days", days = 1 },
          reset_at = (math.floor(now / 86400) + 1) * 86400000,
          detail = string.format("%d requests · %d tokens", activity.sum_api_requests, activity.sum_total_tokens),
        }
      end
      local logs = get(ctx, root .. "/spend/logs/v2" .. query, request_headers)
      local by_model, rows = {}, {}
      if type(logs) == "table" and type(logs.data) == "table" then
        for _, entry in ipairs(logs.data) do
          if
            type(entry.model_group) == "string"
            and type(entry.prompt_tokens) == "number"
            and type(entry.completion_tokens) == "number"
            and type(entry.total_tokens) == "number"
            and type(entry.spend) == "number"
          then
            local row = by_model[entry.model_group]
            if not row then
              row = { model = entry.model_group, input_tokens = 0, output_tokens = 0, total_tokens = 0, spend = 0 }
              by_model[entry.model_group] = row
              rows[#rows + 1] = row
            end
            row.input_tokens = row.input_tokens + entry.prompt_tokens
            row.output_tokens = row.output_tokens + entry.completion_tokens
            row.total_tokens = row.total_tokens + entry.total_tokens
            row.spend = row.spend + entry.spend
          else
            rows = {}
            break
          end
        end
      end
      for _, row in ipairs(rows) do
        row.spend_microdollars = math.max(0, math.floor(row.spend * 1000000 + 0.5))
        row.spend = nil
      end
      table.sort(rows, function(a, b)
        if a.spend_microdollars == b.spend_microdollars then
          return a.model < b.model
        end
        return a.spend_microdollars > b.spend_microdollars
      end)
      return { limits = limits, by_model_today = rows }
    end,
    stream = function(ctx, request, emit)
      local chat = require("maki.chat_completions")
      local body, build_err =
        chat.build_request(request, { max_tokens_field = "max_completion_tokens", include_stream_usage = true })
      if not body then
        return nil, { kind = "config", message = tostring(build_err) }
      end
      local constraints = request.model.max_output_tokens and { max_tokens = request.model.max_output_tokens } or nil
      local resolution, thinking_err = require("maki.thinking").resolve(request.thinking, thinking, constraints)
      if not resolution then
        return nil, { kind = "config", message = tostring(thinking_err) }
      end
      if resolution.effective.mode == "effort" then
        body.reasoning_effort = resolution.effective.level
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
      if response.status ~= 200 then
        return nil, api_error(response)
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      if stream_err and stream_err.kind == "api" then
        return nil, { kind = "api", status = stream_err.status, message = "Regolo request failed" }
      end
      return result, stream_err
    end,
  }
end

function M.login(ctx, ui)
  local key, err = ui:input("Regolo API key", true, false)
  if err then
    return nil, err
  end
  if not key then
    return false
  end
  if key:match("^%s*$") then
    return nil, { kind = "config", message = "Regolo API key is empty" }
  end
  return ctx.credentials:write("api_key", { api_key = key:match("^%s*(.-)%s*$") })
end

function M.auth_status(ctx, config)
  local keys, err, source = credentials(ctx, config or {})
  if not keys then
    if err and err.message ~= "Regolo API key is not configured" then
      return nil, err
    end
    return { configured = false }
  end
  return { configured = true, method = "api_key", source = source }
end

function M.logout(ctx)
  return ctx.credentials:delete("api_key")
end

function M.register()
  maki.api.register_provider({
    id = "regolo",
    display_name = "Regolo",
    models = M.models,
    discover = M.discover,
    prepare = M.prepare,
    auth = { { id = "api_key", label = "API key", login = M.login } },
    auth_status = M.auth_status,
    logout = M.logout,
    supports_thinking = true,
    accepts_arbitrary_models = true,
    fallback_context_window = 120000,
  })
end

return M
