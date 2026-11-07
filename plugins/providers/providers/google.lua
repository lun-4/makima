-- The Gemini API with API keys. Keys, base URL, headers, login and auth
-- status follow the shared provider shape; requests use the Gemini codec.

local compat = require("providers.openai_compat")
local gemini = require("providers.gemini")
local http = require("providers.http")

local M = {}

local API_KEY_HEADER = "x-goog-api-key"
local MODELS_PATH = "/models?key="
local PAGE_SIZE = "&pageSize=1000"
local MODEL_NAME_PREFIX = "^models/"
local GENERATE_METHOD = "generateContent"
local STREAM_SUFFIX = ":streamGenerateContent?alt=sse"
local CONTEXT_WINDOW = 1048576

local function row(id, tier, input, output, cache_read)
  return {
    id = id,
    tier = tier,
    context_window = CONTEXT_WINDOW,
    pricing = { input = input, output = output, cache_write = 0, cache_read = cache_read },
  }
end

M.def = {
  id = "google",
  display_name = "Google",
  env = "GEMINI_API_KEY",
  base_url = "https://generativelanguage.googleapis.com/v1beta",
  login_url = "https://aistudio.google.com/apikey",
  model = "gemini-2.5-pro",
  defaults = {
    context_window = 1000000,
    max_output_tokens = 65536,
    supports_thinking = true,
    supports_vision = true,
    supports_tool_examples = false,
  },
  models = {
    row("gemini-2.5-pro", "strong", 1.25, 5.00, 0.31),
    row("gemini-2.5-flash", "medium", 0.15, 0.60, 0.04),
    row("gemini-2.0-flash-lite", "weak", 0.075, 0.30, 0.01),
  },
  key_headers = function(key)
    return { [API_KEY_HEADER] = key }
  end,
}

-- Models that can generate content, without the `models/` prefix.
function M.def.list_models(ctx, client)
  local body, err = client.get_json(ctx, client.base .. MODELS_PATH .. client.pool.current() .. PAGE_SIZE)
  if err then
    return nil, err
  end
  local rows = {}
  for _, model in ipairs(type(body) == "table" and type(body.models) == "table" and body.models or {}) do
    local methods = type(model) == "table" and model.supportedGenerationMethods
    if type(methods) == "table" and type(model.name) == "string" and table.find(methods, GENERATE_METHOD) then
      rows[#rows + 1] = { id = (model.name:gsub(MODEL_NAME_PREFIX, "")) }
    end
  end
  table.sort(rows, function(a, b)
    return a.id < b.id
  end)
  return rows
end

-- Streams {request} to the Gemini API at {client} (`base` and
-- `headers(extra)`).
function M.stream(client, ctx, request, emit)
  local url = client.base .. "/models/" .. http.urlencode(request.model.id) .. STREAM_SUFFIX
  return http.post_stream(ctx, url, client.headers(), gemini.build_request(request), gemini.stream, emit)
end

function M.provider()
  local spec = compat.provider(M.def)

  function spec.prepare(ctx, info, config)
    local client, err = compat.client(ctx, M.def, config)
    if not client then
      return nil, err
    end
    return {
      key_count = function()
        return client.pool.count
      end,
      rotate_key = function()
        return client.pool.rotate()
      end,
      reload_auth = function(call_ctx)
        local keys, key_err = http.api_keys(call_ctx, config, M.def.env)
        if not keys then
          return nil, key_err
        end
        client.pool = http.key_pool(keys)
      end,
      stream = function(call_ctx, request, emit)
        return M.stream(client, call_ctx, request, emit)
      end,
    },
      compat.describe(M.def, info)
  end

  return spec
end

return M
