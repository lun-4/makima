local compat = require("providers.openai_compat")
local thinking = require("providers.thinking")

local M = {}

local BASE_URL = "https://api.mistral.ai/v1"
local NO_THINKING_PREFIX = "ministral-"

local function pricing(input, output, cache_read)
  return { input = input, output = output, cache_write = 0, cache_read = cache_read or 0 }
end

-- Mistral takes earlier reasoning as a structured `thinking` block at the
-- head of the assistant content.
function M.structure_reasoning(messages)
  for _, message in ipairs(messages) do
    local reasoning = message.role == "assistant" and message.reasoning_content
    if type(reasoning) == "string" then
      message.reasoning_content = nil
      local content = { { type = "thinking", thinking = { { type = "text", text = reasoning } } } }
      if type(message.content) == "string" and message.content ~= "" then
        content[2] = { type = "text", text = message.content }
      end
      message.content = content
    end
  end
end

-- Only chat-capable models, with Mistral's capability flags.
function M.parse_model(row)
  local capabilities = type(row.capabilities) == "table" and row.capabilities or {}
  if type(row.id) ~= "string" or capabilities.completion_chat ~= true then
    return nil
  end
  return {
    id = row.id,
    context_window = compat.limit(row.max_context_length),
    supports_thinking = type(capabilities.reasoning) == "boolean" and capabilities.reasoning or nil,
    supports_vision = capabilities.vision == true,
  }
end

M.def = {
  id = "mistral",
  display_name = "Mistral",
  env = "MISTRAL_API_KEY",
  base_url = BASE_URL,
  login_url = "https://admin.mistral.ai/organization/api-keys",
  model = "mistral-medium-latest",
  plans = {
    { id = "standard", label = "Standard", base_url = BASE_URL, model = "mistral-medium-latest" },
    {
      id = "coding",
      label = "Vibe / Coding",
      base_url = BASE_URL,
      model = "mistral-vibe-cli-latest",
      login_url = "https://console.mistral.ai/codestral/cli",
    },
  },
  include_stream_usage = true,
  defaults = { context_window = 128000, supports_thinking = true, supports_vision = false },
  models = {
    {
      id = "mistral-medium-latest",
      aliases = { "mistral-medium-3.5", "mistral-medium-3-5", "mistral-medium-2604" },
      tier = "strong",
      context_window = 262144,
      supports_vision = true,
      pricing = pricing(1.5, 7.5),
    },
    {
      id = "mistral-small-latest",
      aliases = { "mistral-small-2603" },
      tier = "medium",
      context_window = 262144,
      supports_vision = true,
      pricing = pricing(0.15, 0.60),
    },
    {
      id = "ministral-14b-latest",
      aliases = { "ministral-14b-2512" },
      tier = "weak",
      context_window = 262144,
      pricing = pricing(0.20, 0.20),
    },
    {
      id = "glm-5-2",
      aliases = { "zai-glm-5-2" },
      tier = "strong",
      context_window = 1000000,
      pricing = pricing(1.40, 4.40, 0.14),
    },
  },
  adjust = function(model)
    if model.id:sub(1, #NO_THINKING_PREFIX) == NO_THINKING_PREFIX then
      model.supports_thinking = false
    end
    return model
  end,
  parse_model = M.parse_model,
  encode = function(body, request)
    body.reasoning_effort =
      thinking.effort(request.thinking, thinking.dialects.high_only, thinking.max_budget(request.model))
    M.structure_reasoning(body.messages)
    return { ["x-affinity"] = request.session_id }
  end,
}

return M
