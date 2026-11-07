local compat = require("providers.openai_compat")
local thinking = require("providers.thinking")

local M = {}

local REFERER = "https://makima.ln4.net"
local APP_TITLE = "Makima"
local TEXT = "text"
local IMAGE = "image"
local REASONING_PARAMETER = "reasoning"
local OFF = "none"

local RANK = {}
for rank, level in ipairs(thinking.LEVELS) do
  RANK[level] = rank
end

local function per_million(value)
  local number = type(value) == "string" and tonumber(value)
  return number and number * compat.PER_MILLION
end

local function has(list, value)
  return type(list) == "table" and table.find(list, value) ~= nil
end

-- Text-in, text-out models, priced per million tokens. `provider_data` keeps
-- the model's reasoning flags for the effort dialect.
function M.parse_model(row)
  local architecture = type(row.architecture) == "table" and row.architecture or {}
  local inputs, outputs = architecture.input_modalities, architecture.output_modalities
  if type(row.id) ~= "string" or not has(inputs, TEXT) or not has(outputs, TEXT) then
    return nil
  end
  local p = type(row.pricing) == "table" and row.pricing or {}
  local input, output = per_million(p.prompt), per_million(p.completion)
  local reasoning = type(row.reasoning) == "table" and row.reasoning or nil
  local provider_data
  if reasoning then
    local efforts = {}
    for _, effort in ipairs(type(reasoning.supported_efforts) == "table" and reasoning.supported_efforts or {}) do
      if RANK[effort] then
        efforts[#efforts + 1] = effort
      end
    end
    table.sort(efforts, function(a, b)
      return RANK[a] < RANK[b]
    end)
    provider_data = {
      mandatory = reasoning.mandatory == true,
      default_enabled = reasoning.default_enabled == true,
      efforts = efforts,
    }
  end
  return {
    id = row.id,
    context_window = compat.limit(row.context_length),
    pricing = (input and output) and {
      input = input,
      output = output,
      cache_write = per_million(p.input_cache_write) or 0,
      cache_read = per_million(p.input_cache_read) or 0,
    } or { input = 0, output = 0, cache_write = 0, cache_read = 0 },
    supports_thinking = reasoning ~= nil or has(row.supported_parameters, REASONING_PARAMETER),
    supports_vision = has(inputs, IMAGE),
    provider_data = provider_data,
  }
end

-- Mandatory reasoning cannot be turned off; reasoning that is on by default
-- takes an explicit "none"; declared efforts replace the static levels.
function M.dialect(reasoning)
  local base = thinking.dialects.prefer_high
  if not reasoning then
    return base
  end
  return {
    supported = #reasoning.efforts > 0 and reasoning.efforts or base.supported,
    adaptive = base.adaptive,
    off = (reasoning.default_enabled and not reasoning.mandatory) and OFF or nil,
  }
end

M.def = {
  id = "openrouter",
  display_name = "OpenRouter",
  env = "OPENROUTER_API_KEY",
  base_url = "https://openrouter.ai/api/v1",
  notes = "OpenRouter serves models from many providers behind one key. Browse them at [openrouter.ai/models](https://openrouter.ai/models) and use any id directly, like `openrouter/anthropic/claude-sonnet-4`.",
  login_url = "https://openrouter.ai/keys",
  model = "openai/gpt-5.5",
  include_stream_usage = true,
  defaults = {
    context_window = 200000,
    max_output_tokens = 128000,
    supports_thinking = true,
    supports_vision = false,
  },
  parse_model = M.parse_model,
  encode = function(body, request, model)
    body.cache_control = { type = "ephemeral" }
    if model.supports_thinking then
      local effort =
        thinking.effort(request.thinking, M.dialect(model.provider_data), thinking.max_budget(request.model))
      body.reasoning = effort and { effort = effort } or nil
    end
    body.session_id = request.session_id
    return { ["HTTP-Referer"] = REFERER, ["X-OpenRouter-Title"] = APP_TITLE }
  end,
}

return M
