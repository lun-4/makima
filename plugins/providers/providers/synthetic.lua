local clock = require("providers.clock")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local thinking = require("providers.thinking")

local M = {}

local QUOTA_URL = "https://api.synthetic.new/v2/quotas"
local FIVE_HOURS = 5
local FULL = 100
M.EMPTY_USAGE_ERROR =
  "Synthetic usage response contained no recognisable quota lane; the endpoint schema likely changed"

local function pricing(input, output)
  return { input = input, output = output, cache_write = 0, cache_read = 0 }
end

local function number(value)
  return type(value) == "number" and value == value and math.abs(value) ~= math.huge and value or nil
end

-- Rounded and clamped to 0-100; nil when the limit is not positive.
function M.percentage(used, limit)
  used, limit = number(used), number(limit)
  if not used or not limit or limit <= 0 then
    return nil
  end
  return math.clamp(math.floor(FULL * used / limit + 0.5), 0, FULL)
end

local function lane(value)
  return type(value) == "table" and value or nil
end

-- v3 rolling lanes, with the v2 `subscription` lane as the five-hour
-- fallback.
function M.parse_quotas(body)
  if type(body) ~= "table" then
    return nil, http.config_error(M.EMPTY_USAGE_ERROR)
  end
  local limits = {}
  local rolling, subscription, weekly =
    lane(body.rollingFiveHourLimit), lane(body.subscription), lane(body.weeklyTokenLimit)
  if rolling and number(rolling.max) and number(rolling.remaining) then
    limits[#limits + 1] = {
      kind = { Hours = FIVE_HOURS },
      percentage = M.percentage(rolling.max - rolling.remaining, rolling.max),
      reset_at = clock.parse_rfc3339(rolling.nextTickAt),
    }
  elseif subscription and number(subscription.limit) and number(subscription.requests) then
    limits[#limits + 1] = {
      kind = { Hours = FIVE_HOURS },
      percentage = M.percentage(subscription.requests, subscription.limit),
      reset_at = clock.parse_rfc3339(subscription.renewsAt),
    }
  end
  if weekly and number(weekly.percentRemaining) then
    limits[#limits + 1] = {
      kind = { Weekly = {} },
      percentage = M.percentage(FULL - weekly.percentRemaining, FULL),
      reset_at = clock.parse_rfc3339(weekly.nextRegenAt),
    }
  end
  if #limits == 0 then
    return nil, http.config_error(M.EMPTY_USAGE_ERROR)
  end
  return { limits = limits }
end

M.def = {
  id = "synthetic",
  display_name = "Synthetic",
  env = "SYNTHETIC_API_KEY",
  base_url = "https://api.synthetic.new/openai/v1",
  login_url = "https://synthetic.new",
  model = "hf:moonshotai/Kimi-K2.5",
  max_tokens_field = "max_completion_tokens",
  defaults = {
    context_window = 128000,
    max_output_tokens = 32000,
    supports_thinking = true,
    supports_vision = false,
    supports_tool_examples = true,
  },
  models = {
    {
      id = "hf:moonshotai/Kimi-K2.5",
      tier = "strong",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(0.45, 3.40),
    },
    {
      id = "hf:deepseek-ai/DeepSeek-V3.2",
      tier = "medium",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(0.56, 1.68),
    },
    {
      id = "hf:zai-org/GLM-4.7-Flash",
      tier = "weak",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(0.10, 0.50),
    },
  },
  encode = function(body, request)
    body.reasoning_effort =
      thinking.effort(request.thinking, thinking.dialects.standard, thinking.max_budget(request.model))
  end,
  fetch_usage = function(ctx, client)
    local body, err = client.get_json(ctx, QUOTA_URL)
    if err then
      return nil, err
    end
    return M.parse_quotas(body)
  end,
}

return M
