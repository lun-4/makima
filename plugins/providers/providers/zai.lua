local compat = require("providers.openai_compat")
local thinking = require("providers.thinking")

local M = {}

local STANDARD_URL = "https://api.z.ai/api/paas/v4"
local QUOTA_URL = "https://api.z.ai/api/monitor/usage/quota/limit"
local INSUFFICIENT_FUNDS_STATUS = 402
local RATE_LIMITED_STATUS = 429
local SERVER_ERROR_STATUS = 500
local INSUFFICIENT_FUNDS_MARKERS = { "1113", "nsufficien" }
-- First GLM that takes thinking parameters. A floor instead of a list, so the
-- next GLM gets thinking without a release.
local THINKING_SINCE = { 5, 2 }
local WINDOWS = {
  TOKENS_LIMIT = { [3] = { Hours = 5 }, [6] = { Weekly = {} } },
}
local TIME_LIMIT = "TIME_LIMIT"

local function pricing(input, output, cache_read)
  return { input = input, output = output, cache_write = 0, cache_read = cache_read }
end

-- `glm-5.3` -> 5, 3; `glm-5.3-flash` -> 5, 3; `glm-5-code` -> 5, 0.
function M.glm_version(id)
  local major, minor = id:match("^glm%-(%d+)[%.%-]?(%d*)")
  if not major then
    return nil
  end
  return tonumber(major), tonumber(minor) or 0
end

local function thinks(id)
  local major, minor = M.glm_version(id)
  return major ~= nil and (major > THINKING_SINCE[1] or (major == THINKING_SINCE[1] and minor >= THINKING_SINCE[2]))
end

-- Z.AI reports an empty balance as a rate limit or a server error; neither
-- clears on retry, so it becomes a billing error.
function M.api_error(err)
  if err.status ~= RATE_LIMITED_STATUS and err.status < SERVER_ERROR_STATUS then
    return err
  end
  for _, marker in ipairs(INSUFFICIENT_FUNDS_MARKERS) do
    if err.message:find(marker, 1, true) then
      return { status = INSUFFICIENT_FUNDS_STATUS, message = err.message }
    end
  end
  return err
end

local function window(kind, unit)
  if kind == TIME_LIMIT then
    return "Subscription"
  end
  return WINDOWS[kind] and WINDOWS[kind][unit] or { Other = tostring(kind) .. " #" .. tostring(unit) }
end

function M.parse_quota(body)
  local data = type(body) == "table" and body.data
  if type(data) ~= "table" then
    return nil, "invalid Z.AI quota response"
  end
  local limits = {}
  for _, limit in ipairs(type(data.limits) == "table" and data.limits or {}) do
    limits[#limits + 1] = {
      kind = window(limit.type, limit.unit),
      percentage = limit.percentage,
      reset_at = limit.nextResetTime,
    }
  end
  return { plan = data.level, limits = limits }
end

M.def = {
  id = "zai",
  display_name = "Z.AI",
  env = "ZHIPU_API_KEY",
  base_url = STANDARD_URL,
  login_url = "https://z.ai/manage-apikey/apikey-list",
  model = "glm-5.1",
  plans = {
    { id = "standard", label = "Pay-as-you-go", base_url = STANDARD_URL, model = "glm-5.1" },
    { id = "coding", label = "Coding plan", base_url = "https://api.z.ai/api/coding/paas/v4", model = "glm-5-code" },
  },
  defaults = { context_window = 128000, max_output_tokens = 16000, supports_thinking = false, supports_vision = false },
  models = {
    {
      id = "glm-5-code",
      tier = "strong",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(1.20, 5.00, 0.30),
    },
    {
      id = "glm-4.7",
      aliases = { "glm-4.6" },
      tier = "medium",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(0.60, 2.20, 0.11),
    },
    {
      id = "glm-4.7-flash",
      tier = "weak",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(0, 0, 0),
    },
    {
      id = "glm-5.3",
      tier = "strong",
      context_window = 1000000,
      max_output_tokens = 131072,
      pricing = pricing(1.40, 4.40, 0.26),
    },
    {
      id = "glm-5.3-flash",
      tier = "weak",
      context_window = 1000000,
      max_output_tokens = 131072,
      supports_vision = true,
      pricing = pricing(0.15, 0.50, 0.03),
    },
    {
      id = "glm-5.2",
      tier = "strong",
      context_window = 1000000,
      max_output_tokens = 131072,
      pricing = pricing(1.00, 3.20, 0.20),
    },
    {
      id = "glm-5.1",
      aliases = { "glm-5" },
      tier = "strong",
      context_window = 200000,
      max_output_tokens = 131072,
      pricing = pricing(1.00, 3.20, 0.20),
    },
    {
      id = "glm-4.5-flash",
      tier = "weak",
      context_window = 131072,
      max_output_tokens = 98304,
      pricing = pricing(0, 0, 0),
    },
    {
      id = "glm-4.5-air",
      tier = "weak",
      context_window = 131072,
      max_output_tokens = 98304,
      pricing = pricing(0.20, 1.10, 0.03),
    },
    {
      id = "glm-4.5",
      tier = "medium",
      context_window = 131072,
      max_output_tokens = 98304,
      pricing = pricing(0.60, 2.20, 0.11),
    },
  },
  adjust = function(model)
    if thinks(model.id) then
      model.supports_thinking = true
    end
    return model
  end,
  api_error = M.api_error,
  encode = function(body, request, model)
    if model.supports_thinking then
      body.reasoning_effort =
        thinking.effort(request.thinking, thinking.dialects.glm, thinking.max_budget(request.model))
    end
  end,
  fetch_usage = function(ctx, client)
    local body, err = client.get_json(ctx, QUOTA_URL)
    if err then
      return nil, err
    end
    return M.parse_quota(body)
  end,
}

return M
