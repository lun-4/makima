local compat = require("providers.openai_compat")
local deepseek = require("providers.deepseek")
local thinking = require("providers.thinking")

local M = {}

local BASE_URL = "https://api.tensorx.ai/v1"
local MODEL_INFO_URL = BASE_URL .. "/model/info"
local CHAT_MODE = "chat"
-- TensorX namespaces resold models by vendor.
local DEEPSEEK_PREFIX = "deepseek/"

local function cost(value)
  return type(value) == "number" and value * compat.PER_MILLION or nil
end

local function has(list, value)
  return type(list) == "table" and table.find(list, value) ~= nil
end

-- `provider_data` records which thinking knobs the model takes.
function M.parse_model(entry)
  local info = entry.model_info
  if type(entry.model_name) ~= "string" or type(info) ~= "table" then
    return nil
  end
  if info.mode ~= nil and info.mode ~= CHAT_MODE then
    return nil
  end
  local input, output = cost(info.input_cost_per_token), cost(info.output_cost_per_token)
  local params = info.supported_openai_params
  return {
    id = entry.model_name,
    context_window = compat.limit(info.max_tokens) or compat.limit(info.max_input_tokens),
    -- No max_output_tokens: the API rejects a request whose input plus its
    -- maximum output exceeds the context window.
    pricing = (input or output) and {
      input = input or 0,
      output = output or 0,
      cache_write = cost(info.cache_creation_input_token_cost) or 0,
      cache_read = cost(info.cache_read_input_token_cost) or 0,
    } or nil,
    supports_vision = info.supports_vision == true,
    supports_thinking = type(info.supports_reasoning) == "boolean" and info.supports_reasoning or nil,
    provider_data = type(params) == "table"
        and { thinking = has(params, "thinking"), reasoning_effort = has(params, "reasoning_effort") }
      or nil,
  }
end

function M.encode(body, request, model)
  local knobs = model.provider_data or {}
  local enabled = request.thinking.kind ~= "off"
  if knobs.thinking then
    body.thinking = enabled
  end
  if knobs.reasoning_effort then
    body.reasoning_effort =
      thinking.effort(request.thinking, thinking.dialects.tensorx, thinking.max_budget(request.model))
  elseif not knobs.thinking and enabled and model.id:sub(1, #DEEPSEEK_PREFIX) == DEEPSEEK_PREFIX then
    -- DeepSeek takes the toggle through its chat template, and TensorX
    -- advertises neither knob for it.
    if deepseek.uses_v4_thinking_protocol(model.id:sub(#DEEPSEEK_PREFIX + 1)) then
      body.chat_template_kwargs = { thinking = true }
    end
  end
end

M.def = {
  id = "tensorx",
  display_name = "TensorX",
  env = "TENSORX_API_KEY",
  base_url = BASE_URL,
  login_url = "https://tensorx.ai",
  model = "z-ai/glm-5.2",
  include_stream_usage = true,
  defaults = { context_window = 200000, supports_thinking = true, supports_vision = false },
  list_models = function(ctx, client)
    local body, err = client.get_json(ctx, MODEL_INFO_URL)
    if err then
      return nil, err
    end
    return compat.parse_listing(body, M.parse_model)
  end,
  encode = M.encode,
}

return M
