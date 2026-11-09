local thinking = require("providers.thinking")

local M = {}

local BALANCE_URL = "https://api.deepseek.com/user/balance"
-- R1 reasons unconditionally and refuses `reasoning_content` as input; every
-- later model speaks the V4 thinking protocol.
local REASONER_ID = "deepseek-reasoner"
local PAD = ""
local BUDGET_UNSUPPORTED = "DeepSeek reasoning does not support token budgets"
local CURRENCY_SYMBOLS = { USD = "$", CNY = "¥" }
-- Peak hours double every rate; descriptors quote the off-peak ones. The
-- weekend stays off-peak around the clock.
-- <https://api-docs.deepseek.com/quick_start/pricing/>
local PEAK_HOURS = {
  windows = { { start = 1, ["end"] = 4 }, { start = 6, ["end"] = 10 } },
  weekdays_only = true,
  multiplier = 2,
}

function M.uses_v4_thinking_protocol(id)
  return id:sub(1, #REASONER_ID) ~= REASONER_ID
end

-- With tools, V4 wants `reasoning_content` on every assistant turn (missing
-- is a 400), and only checks that it exists.
function M.pad_reasoning(body, id)
  if not M.uses_v4_thinking_protocol(id) or not body.tools then
    return
  end
  for _, message in ipairs(body.messages) do
    if message.role == "assistant" and type(message.reasoning_content) ~= "string" then
      message.reasoning_content = PAD
    end
  end
end

function M.parse_balance(body)
  local infos = type(body) == "table" and body.balance_infos
  if type(infos) ~= "table" then
    return nil, "invalid DeepSeek balance response"
  end
  local limits = {}
  for _, info in ipairs(infos) do
    local symbol = CURRENCY_SYMBOLS[info.currency] or ""
    limits[#limits + 1] = {
      kind = "Credits",
      detail = string.format(
        "total: %s%s, topped-up: %s%s, granted: %s%s",
        symbol,
        tostring(info.total_balance),
        symbol,
        tostring(info.topped_up_balance),
        symbol,
        tostring(info.granted_balance)
      ),
    }
  end
  return { limits = limits }
end

local function pricing(input, output, cache_read)
  return { input = input, output = output, cache_write = 0, cache_read = cache_read }
end

M.def = {
  id = "deepseek",
  display_name = "DeepSeek",
  env = "DEEPSEEK_API_KEY",
  base_url = "https://api.deepseek.com",
  login_url = "https://platform.deepseek.com/api_keys",
  model = "deepseek-v4-flash",
  include_stream_usage = true,
  defaults = {
    context_window = 1000000,
    max_output_tokens = 384000,
    supports_thinking = true,
    supports_vision = false,
    pricing_schedule = PEAK_HOURS,
  },
  models = {
    {
      id = "deepseek-v4-flash",
      tier = "medium",
      max_output_tokens = 384000,
      pricing = pricing(0.22, 0.66, 0.007),
    },
    {
      id = "deepseek-v4-pro",
      tier = "strong",
      max_output_tokens = 384000,
      pricing = pricing(0.66, 1.98, 0.022),
    },
  },
  encode = function(body, request, model)
    if request.thinking.kind == "off" then
      body.thinking = { type = "disabled" }
      return
    end
    body.thinking = { type = "enabled" }
    body.reasoning_effort =
      thinking.effort(request.thinking, thinking.dialects.deepseek, thinking.max_budget(request.model))
    if request.thinking.kind == "budget" then
      maki.log.warn(BUDGET_UNSUPPORTED)
    end
    M.pad_reasoning(body, model.id)
  end,
  fetch_usage = function(ctx, client)
    local body, err = client.get_json(ctx, BALANCE_URL)
    if err then
      return nil, err
    end
    return M.parse_balance(body)
  end,
}

return M
