-- llama.cpp: a server at `LLAMA_CPP_HOST`. Discovery reads `/props` to tell
-- a router (many models), a single-model server and a legacy server apart,
-- since each reports context windows and vision in its own place. Chat
-- requests carry the local thinking fields.

local compat = require("providers.openai_compat")
local local_server = require("providers.local_server")

local M = {}

M.DEFAULT_CONTEXT = 128000

local PROPS_PATH = "/props?autoload=false"
local MODELS_PATH = "/v1/models"
local ROUTER_ROLE = "router"
local CONTEXT_FLAGS = { "--ctx-size", "--fit-ctx" }

local function field(value, key)
  if type(value) == "table" then
    return value[key]
  end
end

local function positive(value)
  local limit = compat.limit(value)
  return limit and limit > 0 and limit or nil
end

-- The value after {flag} in {args}, as a context window.
function M.flag_value(args, flag)
  local index = type(args) == "table" and table.find(args, flag)
  return index and compat.limit(tonumber(args[index + 1]))
end

-- `router` when `/props` says so, `legacy` when the first listed model has
-- `max_model_len`, else `single`.
function M.mode(props, rows)
  if field(props, "role") == ROUTER_ROLE then
    return "router"
  elseif rows[1] and rows[1].max_model_len ~= nil then
    return "legacy"
  end
  return "single"
end

-- Router: `meta.n_ctx`, then the `--ctx-size` or `--fit-ctx` it was started
-- with. Single: `meta.n_ctx`. Legacy: `max_model_len`, then the server's
-- `n_ctx`.
function M.context_window(row, mode, props_n_ctx)
  local window
  if mode == "legacy" then
    window = positive(row.max_model_len) or positive(props_n_ctx)
  else
    window = positive(field(row.meta, "n_ctx"))
    if not window and mode == "router" then
      local args = field(row.status, "args")
      window = M.flag_value(args, CONTEXT_FLAGS[1]) or M.flag_value(args, CONTEXT_FLAGS[2])
    end
  end
  return window or M.DEFAULT_CONTEXT
end

-- `/props` vision describes the router itself in router mode, not the
-- models it serves.
function M.props_vision(props, mode)
  local vision = field(field(props, "modalities"), "vision")
  if mode ~= "router" and type(vision) == "boolean" then
    return vision
  end
end

local function has(list, item)
  return type(list) == "table" and table.find(list, item) ~= nil
end

-- Text-in, text-out models with their context windows and vision support,
-- sorted by id.
function M.parse_models(props, body)
  local rows = {}
  for _, row in ipairs(field(body, "data") or {}) do
    if type(row) == "table" and type(row.id) == "string" then
      rows[#rows + 1] = row
    end
  end
  local mode = M.mode(props, rows)
  local props_vision = M.props_vision(props, mode)
  local props_n_ctx = field(props, "n_ctx")
  local models = {}
  for _, row in ipairs(rows) do
    local arch = type(row.architecture) == "table" and row.architecture or nil
    if not arch or (has(arch.input_modalities, "text") and has(arch.output_modalities, "text")) then
      local vision = props_vision
      if arch then
        vision = has(arch.input_modalities, "image")
      end
      models[#models + 1] = {
        id = row.id,
        context_window = M.context_window(row, mode, props_n_ctx),
        supports_vision = vision,
        pricing = local_server.PRICING_FREE,
      }
    end
  end
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  return models
end

function M.list_models(ctx, client)
  local props, err = client.request_json(ctx, { url = client.root .. PROPS_PATH })
  if err then
    return nil, err
  end
  local body, models_err = client.request_json(ctx, { url = client.root .. MODELS_PATH })
  if models_err then
    return nil, models_err
  end
  return M.parse_models(props, body)
end

M.def = {
  id = "llama-cpp",
  display_name = "LlamaCpp",
  env = "LLAMA_CPP_API_KEY",
  host_env = "LLAMA_CPP_HOST",
  default_host = "http://localhost:8080",
  notes = "Talks to the server at the `base_url` saved by `makima auth login llama-cpp`, else at `LLAMA_CPP_HOST`. Models come from the server: a router lists many, a single-model server one. The key is optional.",
  defaults = {
    context_window = M.DEFAULT_CONTEXT,
    supports_thinking = true,
    pricing = local_server.PRICING_FREE,
  },
  local_thinking = true,
  list_models = M.list_models,
}

return M
