-- Ollama: a local or remote server at `OLLAMA_HOST`, or Ollama Cloud with
-- just `OLLAMA_API_KEY`. Discovery asks `/api/show` about each listed model
-- for its context window and vision support.

local compat = require("providers.openai_compat")
local local_server = require("providers.local_server")

local M = {}

local MODELS_PATH = "/v1/models"
local SHOW_PATH = "/api/show"
local CONTEXT_KEY = "context_length"
local CONTEXT_SUFFIX = "." .. CONTEXT_KEY
local VISION = "vision"
local SHOW_FAILED = "Ollama POST /api/show failed for %s: %s"

-- The context window from `model_info` (`<arch>.context_length`, then
-- `context_length`), else from the `num_ctx` parameter.
function M.context_window(show)
  local info = type(show.model_info) == "table" and show.model_info or {}
  local keys = {}
  for key in pairs(info) do
    if type(key) == "string" and key:sub(-#CONTEXT_SUFFIX) == CONTEXT_SUFFIX then
      keys[#keys + 1] = key
    end
  end
  table.sort(keys)
  local window = keys[1] and compat.limit(info[keys[1]]) or compat.limit(info[CONTEXT_KEY])
  if window then
    return window
  end
  for line in (type(show.parameters) == "string" and show.parameters or ""):gmatch("[^\n]+") do
    local num_ctx = compat.limit(tonumber(line:match("^%s*num_ctx%s+(%d+)%s*$")))
    if num_ctx and num_ctx > 0 then
      return num_ctx
    end
  end
end

-- Nil when the server lists no capabilities (older Ollama), so the model
-- is not claimed to lack vision without a check.
function M.vision(show)
  if type(show.capabilities) == "table" then
    return table.find(show.capabilities, VISION) ~= nil
  end
end

local function describe(ctx, client, id)
  local show, err = client.request_json(ctx, {
    url = client.root .. SHOW_PATH,
    method = "POST",
    headers = { ["Content-Type"] = "application/json" },
    body = assert(maki.json.encode({ model = id })),
  })
  local row = { id = id, pricing = local_server.PRICING_FREE }
  if type(show) ~= "table" then
    maki.log.warn(SHOW_FAILED:format(id, tostring(err)))
    return row
  end
  row.context_window = M.context_window(show)
  row.supports_vision = M.vision(show)
  return row
end

-- The listed models, each described by its own `/api/show` at the same
-- time. A failed query leaves that model's details unknown.
function M.list_models(ctx, client)
  local body, err = client.request_json(ctx, { url = client.root .. MODELS_PATH })
  if err then
    return nil, err
  end
  local ids = compat.parse_listing(body, function(row)
    return type(row.id) == "string" and { id = row.id }
  end)
  local tasks = {}
  for i, row in ipairs(ids) do
    tasks[i] = function()
      return describe(ctx, client, row.id)
    end
  end
  local out = {}
  for i, result in ipairs(maki.async.gather(tasks)) do
    out[i] = result.ok and result.value or { id = ids[i].id, pricing = local_server.PRICING_FREE }
  end
  return out
end

M.def = {
  id = "ollama",
  display_name = "Ollama",
  env = "OLLAMA_API_KEY",
  host_env = "OLLAMA_HOST",
  default_host = "http://localhost:11434",
  cloud_url = "https://ollama.com/v1",
  notes = "Talks to the server at the `base_url` saved by `makima auth login ollama`, else at `OLLAMA_HOST`, or to Ollama Cloud when only `OLLAMA_API_KEY` is set. Models come from the server, with the context window and vision support it reports for each.",
  defaults = {
    context_window = 128000,
    max_output_tokens = 16384,
    supports_thinking = false,
    pricing = local_server.PRICING_FREE,
  },
  list_models = M.list_models,
}

return M
