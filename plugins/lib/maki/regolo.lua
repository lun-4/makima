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

local function credentials(config)
  local key = maki.uv.os_getenv(config.api_key_env or "REGOLO_API_KEY")
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
    return nil, { kind = "config", message = "Regolo API key is not configured" }
  end
  return keys
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

local function get(ctx, url, request_headers)
  local response, err = ctx:send({ url = url, method = "GET", headers = request_headers })
  if not response then
    return nil, err
  end
  if response.status < 200 or response.status >= 300 then
    response:close()
    return nil, { kind = "api", status = response.status, message = "Regolo request failed" }
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
  local keys, err = credentials(config)
  if not keys then
    return nil, err
  end
  local base = base_url(config)
  local request_headers = headers(config, keys[1])
  local response, get_err = get(ctx, base .. "/models", request_headers)
  if not response then
    return nil, get_err
  end
  if type(response.data) ~= "table" then
    return nil, { kind = "api", message = "Invalid Regolo models response" }
  end
  local ids = {}
  for _, model in ipairs(response.data) do
    if type(model.id) == "string" then
      ids[#ids + 1] = model.id
    end
  end
  table.sort(ids)
  local groups = get(ctx, base:gsub("/v1$", "") .. "/model_group/info", request_headers)
  if type(groups) == "table" and type(groups.data) == "table" then
    return M.join_models(ids, groups.data)
  end
  local models = {}
  for _, id in ipairs(ids) do
    models[#models + 1] = { id = id }
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
      local limits = { { kind = { type = "credits" }, percentage = percentage, detail = detail } }
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
        chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
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
      if response.status < 200 or response.status >= 300 then
        response:close()
        return nil, { kind = "api", status = response.status, message = "Regolo request failed" }
      end
      local result, stream_err = chat.stream(response, emit)
      response:close()
      return result, stream_err
    end,
  }
end

function M.register()
  maki.api.register_provider({
    id = "regolo",
    display_name = "Regolo",
    models = M.models,
    discover = M.discover,
    prepare = M.prepare,
    supports_thinking = true,
    accepts_arbitrary_models = true,
    fallback_context_window = 120000,
  })
end

return M
