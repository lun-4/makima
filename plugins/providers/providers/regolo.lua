local clock = require("providers.clock")
local compat = require("providers.openai_compat")
local thinking = require("providers.thinking")

local M = {}

local CHAT_MODE = "chat"
local KEY_INFO_PATH = "/key/info"
local ACTIVITY_PATH = "/global/activity"
local SPEND_LOGS_PATH = "/spend/logs/v2"
local MODEL_GROUP_INFO_PATH = "/model_group/info"
local VERSION_SUFFIX = "/v1"
local FULL = 100

local function pricing(input, output)
  return { input = input, output = output, cache_write = 0, cache_read = 0 }
end

local function number(value)
  return type(value) == "number" and value or nil
end

-- The management endpoints live at the host root, outside `/v1`. Deriving
-- them from the base URL keeps a custom base or a gateway in the path.
function M.root_url(base)
  local trimmed = base:gsub("/+$", "")
  if trimmed:sub(-#VERSION_SUFFIX) == VERSION_SUFFIX then
    return trimmed:sub(1, -#VERSION_SUFFIX - 1)
  end
  return trimmed
end

local function tokens(value)
  value = number(value)
  return value and compat.limit(math.floor(value))
end

-- The callable ids from `/models` with the metadata from
-- `/model_group/info`. Non-chat groups and ids without a group are dropped.
function M.join(ids, groups)
  local by_group = {}
  for _, group in ipairs(groups) do
    if type(group) == "table" and group.mode == CHAT_MODE then
      by_group[group.model_group] = group
    end
  end
  local models = {}
  for _, id in ipairs(ids) do
    local group = by_group[id]
    if group then
      local input, output = number(group.input_cost_per_token), number(group.output_cost_per_token)
      models[#models + 1] = {
        id = id,
        context_window = tokens(group.max_input_tokens) or tokens(group.max_tokens),
        max_output_tokens = tokens(group.max_output_tokens),
        pricing = (input and output) and pricing(input * compat.PER_MILLION, output * compat.PER_MILLION) or nil,
        supports_thinking = group.supports_reasoning == true,
        supports_vision = group.supports_vision == true,
      }
    end
  end
  return models
end

function M.key_limit(info)
  local spend, budget = number(info.spend) or 0, number(info.max_budget)
  local detail = string.format("$%.2f spent", spend)
  local percentage
  if budget then
    detail ..= string.format(" of $%.2f budget", budget)
    if budget > 0 then
      percentage = math.clamp(math.floor(spend / budget * FULL), 0, FULL)
    end
  end
  local reset_at = clock.parse_rfc3339(info.budget_reset_at)
  return { kind = "Credits", percentage = percentage, reset_at = reset_at and math.max(reset_at, 0), detail = detail }
end

-- Regolo tracks a daily token cap but reports no endpoint for it, so there is
-- no honest percentage.
function M.activity_limit(activity, now_secs)
  return {
    kind = { Days = 1 },
    reset_at = clock.next_utc_midnight(now_secs),
    detail = string.format("%d requests · %d tokens", activity.sum_api_requests, activity.sum_total_tokens),
  }
end

-- Hourly rows summed per model, ranked by spend.
function M.spend_rows(entries)
  local by_model, order = {}, {}
  for _, entry in ipairs(entries) do
    local row = by_model[entry.model_group]
    if not row then
      row = { model = entry.model_group, input_tokens = 0, output_tokens = 0, total_tokens = 0, spend = 0 }
      by_model[entry.model_group] = row
      order[#order + 1] = row
    end
    row.input_tokens = row.input_tokens + entry.prompt_tokens
    row.output_tokens = row.output_tokens + entry.completion_tokens
    row.total_tokens = row.total_tokens + entry.total_tokens
    row.spend = row.spend + entry.spend
  end
  for _, row in ipairs(order) do
    row.spend_microdollars = math.floor(row.spend * compat.PER_MILLION + 0.5)
    row.spend = nil
  end
  table.sort(order, function(a, b)
    if a.spend_microdollars ~= b.spend_microdollars then
      return a.spend_microdollars > b.spend_microdollars
    end
    return a.model < b.model
  end)
  return order
end

local function day_query(root, path, day)
  return string.format("%s%s?start_date=%s&end_date=%s", root, path, day, day)
end

function M.list_models(ctx, client)
  local body, err = client.get_json(ctx, client.base .. compat.MODELS_PATH)
  if err then
    return nil, err
  end
  local ids = {}
  for _, model in ipairs(compat.parse_listing(body, compat.parse_model)) do
    ids[#ids + 1] = model.id
  end
  -- The group endpoint hiccups (it has 500ed): keep the live ids without
  -- metadata instead of dropping to the curated few.
  local groups = client.get_json(ctx, M.root_url(client.base) .. MODEL_GROUP_INFO_PATH)
  if type(groups) == "table" and type(groups.data) == "table" then
    return M.join(ids, groups.data)
  end
  local models = {}
  for _, id in ipairs(ids) do
    models[#models + 1] = { id = id }
  end
  return models
end

function M.fetch_usage(ctx, client)
  local root = M.root_url(client.base)
  local key, err = client.get_json(ctx, root .. KEY_INFO_PATH)
  if err then
    return nil, err
  end
  if type(key) ~= "table" or type(key.info) ~= "table" then
    return nil, "invalid Regolo key info response"
  end
  local now = maki.time.now().secs
  local today = clock.utc_date(now)
  local limits = { M.key_limit(key.info) }
  local activity = client.get_json(ctx, day_query(root, ACTIVITY_PATH, today))
  if type(activity) == "table" and number(activity.sum_api_requests) and number(activity.sum_total_tokens) then
    limits[#limits + 1] = M.activity_limit(activity, now)
  end
  local logs = client.get_json(ctx, day_query(root, SPEND_LOGS_PATH, today))
  local rows = type(logs) == "table" and type(logs.data) == "table" and M.spend_rows(logs.data) or {}
  return { limits = limits, by_model_today = #rows > 0 and rows or nil }
end

M.def = {
  id = "regolo",
  display_name = "Regolo",
  env = "REGOLO_API_KEY",
  base_url = "https://api.regolo.ai/v1",
  login_url = "https://dashboard.regolo.ai",
  model = "qwen3-coder-next",
  max_tokens_field = "max_completion_tokens",
  include_stream_usage = true,
  defaults = {
    context_window = 120000,
    max_output_tokens = 120000,
    supports_thinking = true,
    supports_vision = false,
  },
  -- Tier defaults only; the full catalogue and prices come from discovery.
  -- Context windows mirror the group's `max_input_tokens`.
  models = {
    {
      id = "qwen3.5-122b",
      tier = "strong",
      context_window = 120000,
      max_output_tokens = 120000,
      supports_vision = true,
      pricing = pricing(1.00, 4.20),
    },
    {
      id = "qwen3-coder-next",
      tier = "medium",
      context_window = 120000,
      max_output_tokens = 120000,
      pricing = pricing(0.50, 2.00),
    },
    {
      id = "qwen3.5-9b",
      tier = "weak",
      -- The group advertises 120k output against an 80k window; capped so
      -- the default never promises more output than fits.
      context_window = 80000,
      max_output_tokens = 80000,
      pricing = pricing(0.07, 0.35),
    },
  },
  list_models = M.list_models,
  fetch_usage = M.fetch_usage,
  encode = function(body, request)
    body.reasoning_effort =
      thinking.effort(request.thinking, thinking.dialects.standard, thinking.max_budget(request.model))
  end,
}

return M
