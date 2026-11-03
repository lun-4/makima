-- Tailscale Aperture, one gateway in front of many upstream providers. It
-- lists each model as `<upstream>/<model>` and appends the whole request path
-- to the upstream's base URL, so a model whose upstream names a provider we
-- implement streams through that provider's own request code, against the
-- gateway and with the gateway's headers. Tailscale identifies the user, so
-- nothing carries a key. `[aperture.overrides.<upstream>]` in
-- `providers.toml` remaps an upstream (`base`), changes its path prefix and
-- overrides limits and capabilities, with `models."<id>"` winning field by
-- field.

local anthropic = require("providers.anthropic_provider")
local compat = require("providers.openai_compat")
local google = require("providers.google")
local http = require("providers.http")
local local_server = require("providers.local_server")

local M = {}

M.ID = "aperture"
M.HOST_ENV = "APERTURE_HOST"
M.DEFAULT_PREFIX = "/v1"
M.DEFAULTS = { context_window = 128000, max_output_tokens = 16384, supports_thinking = false }

local DISPLAY_NAME = "Aperture"
local MODELS_PATH = "/v1/models"
local GEMINI_PREFIX = "/v1beta"
local BASE_URL_KEY = "base_url"
local HOST_PROMPT = "Aperture host URL"
local PER_MILLION = 1000000
local OVERRIDABLE = { "context_window", "max_output_tokens", "supports_thinking", "supports_vision" }
-- The gateway serves a plain chat completions route for upstreams without one.
local GENERIC = { id = M.ID, include_stream_usage = true }

local function compat_route(name, prefix)
  local def = require("providers." .. name).def
  return {
    def = def,
    prefix = prefix,
    stream = function(client, model, ctx, request, emit)
      return compat.stream(def, client, model, ctx, request, emit)
    end,
  }
end

local function local_route(name)
  local def = require("providers." .. name).def
  return {
    def = def,
    stream = function(client, _, ctx, request, emit)
      return local_server.stream(def, client, {}, ctx, request, emit)
    end,
  }
end

-- Upstreams by provider id. Anthropic rebuilds `/v1/messages` from the bare
-- origin and Z.AI has no `/v1` segment, so neither gets a prefix; Gemini puts
-- the model in the path, where an encoded slash would not survive, so it
-- takes the bare id. OpenAI and Copilot stay on the generic route: their
-- own code would leave the gateway.
M.ROUTES = {
  anthropic = { def = anthropic.def, prefix = "", stream = anthropic.stream },
  google = {
    def = google.def,
    prefix = GEMINI_PREFIX,
    bare_id = true,
    stream = function(client, _, ctx, request, emit)
      return google.stream(client, ctx, request, emit)
    end,
  },
  ollama = local_route("ollama"),
  ["llama-cpp"] = local_route("llama_cpp"),
  zai = compat_route("zai", ""),
  mistral = compat_route("mistral"),
  deepseek = compat_route("deepseek"),
  openrouter = compat_route("openrouter"),
  requesty = compat_route("requesty"),
  synthetic = compat_route("synthetic"),
  regolo = compat_route("regolo"),
  tensorx = compat_route("tensorx"),
}

local function trim(text)
  return type(text) == "string" and text:match("^%s*(.-)%s*$") or ""
end

-- `APERTURE_HOST`, then `base_url`, without a trailing slash.
function M.host(config)
  local host = trim(maki.uv.os_getenv(M.HOST_ENV))
  if host == "" then
    host = trim(config.base_url)
  end
  return host ~= "" and (host:gsub("/+$", "")) or nil
end

-- The overrides for {model_id} of {upstream}: the model's fields over the
-- upstream's.
function M.overrides(config, upstream, model_id)
  local all = type(config.overrides) == "table" and config.overrides or {}
  local provider = type(all[upstream]) == "table" and all[upstream] or {}
  local merged = {}
  for key, value in pairs(provider) do
    if key ~= "models" then
      merged[key] = value
    end
  end
  local model = type(provider.models) == "table" and provider.models[model_id]
  for key, value in pairs(type(model) == "table" and model or {}) do
    merged[key] = value
  end
  return merged
end

-- The route an override's `base` names, else the upstream's own, else nil
-- for the generic route.
function M.route(upstream, overrides)
  return M.ROUTES[overrides.base] or M.ROUTES[upstream]
end

-- The path sent after the host: `path_prefix` from the overrides, else the
-- route's own, else `/v1`.
function M.prefix(route, overrides)
  local configured = overrides.path_prefix
  if configured == nil then
    return route and route.prefix or M.DEFAULT_PREFIX
  end
  local path = trim(configured):gsub("^/+", ""):gsub("/+$", "")
  return path == "" and "" or "/" .. path
end

local function split(id)
  local upstream, model_id = id:match("^([^/]+)/(.+)$")
  return upstream or "", model_id or id
end

-- {info} with the routed provider's limits and capabilities for its model,
-- then the overrides on top. Discovery's pricing stays.
function M.describe(info, config)
  local upstream, model_id = split(info.id)
  local overrides = M.overrides(config, upstream, model_id)
  local route = M.route(upstream, overrides)
  local out = table.clone(info)
  if route then
    local native = compat.describe(route.def, { id = model_id })
    out.context_window, out.max_output_tokens = native.context_window, native.max_output_tokens
    for _, key in ipairs({ "supports_thinking", "supports_vision", "supports_fast", "supports_tool_examples" }) do
      if out[key] == nil then
        out[key] = native[key]
      end
    end
  end
  for key, value in pairs(M.DEFAULTS) do
    if out[key] == nil then
      out[key] = value
    end
  end
  for _, key in ipairs(OVERRIDABLE) do
    if overrides[key] ~= nil then
      out[key] = overrides[key]
    end
  end
  return out, route, model_id, overrides
end

local function client(config, prefix)
  local host = M.host(config)
  if not host then
    return nil, http.config_error(M.HOST_ENV .. " not set")
  end
  local configured, err = http.headers(M.ID, {}, config.headers)
  if not configured then
    return nil, err
  end
  local api = { base = host .. prefix }
  -- {extra} with configured headers replacing any of them.
  function api.headers(extra)
    local headers = table.clone(extra or {})
    for name, value in pairs(configured) do
      http.set_header(headers, name, value)
    end
    return headers
  end
  return api
end

local function price(value)
  local number = type(value) == "number" and value or tonumber(value)
  return (number or 0) * PER_MILLION
end

-- One `/v1/models` row as a descriptor.
function M.parse_model(row, config)
  if type(row) ~= "table" or type(row.id) ~= "string" then
    return nil
  end
  local metadata = type(row.metadata) == "table" and row.metadata or {}
  local provider = type(metadata.provider) == "table" and metadata.provider or {}
  local upstream = type(provider.id) == "string" and provider.id or ""
  local overrides = M.overrides(config, upstream, row.id)
  local route = M.route(upstream, overrides)
  local pricing
  if type(row.pricing) == "table" then
    pricing = {
      input = price(row.pricing.input),
      output = price(row.pricing.output),
      cache_read = price(row.pricing.input_cache_read),
      cache_write = 0,
    }
  end
  local thinks = overrides.supports_thinking
  if thinks == nil and route then
    thinks = compat.describe(route.def, { id = row.id }).supports_thinking
  end
  return {
    id = upstream == "" and row.id or upstream .. "/" .. row.id,
    context_window = overrides.context_window,
    max_output_tokens = overrides.max_output_tokens,
    pricing = pricing,
    supports_thinking = thinks,
    supports_vision = overrides.supports_vision,
  }
end

function M.provider()
  local spec = {
    id = M.ID,
    display_name = DISPLAY_NAME,
    config = { { key = BASE_URL_KEY, prompt = HOST_PROMPT } },
    auth = compat.api_key_auth({ display_name = DISPLAY_NAME, key_optional = true }),
  }

  function spec.auth_status(_, config)
    if trim(maki.uv.os_getenv(M.HOST_ENV)) ~= "" then
      return { state = "env", detail = "via " .. M.HOST_ENV }
    end
    return { state = M.host(config) and "configured" or "missing" }
  end

  function spec.discover(ctx, config)
    local api = client(config, "")
    if not api then
      return {}
    end
    local response, err = http.send(ctx, { url = api.base .. MODELS_PATH, headers = api.headers() })
    if not response then
      return nil, err
    end
    local body, read_err = http.read_json(response)
    if read_err then
      return nil, read_err
    end
    local models = {}
    for _, row in ipairs(type(body) == "table" and type(body.data) == "table" and body.data or {}) do
      models[#models + 1] = M.parse_model(row, config)
    end
    return models
  end

  function spec.prepare(_, info, config)
    local model, route, model_id, overrides = M.describe(info, config)
    if overrides.base and not M.ROUTES[overrides.base] then
      maki.log.warn(string.format("aperture: base %q is not a provider it routes to, ignoring", overrides.base))
    end
    local api, err = client(config, M.prefix(route, overrides))
    if not api then
      return nil, err
    end
    local routed = table.clone(model)
    routed.id = model_id
    return {
      stream = function(call_ctx, request, emit)
        if not route then
          return compat.stream(GENERIC, api, model, call_ctx, request, emit)
        end
        if route.bare_id then
          request.model.id = model_id
        end
        return route.stream(api, routed, call_ctx, request, emit)
      end,
    },
      model
  end

  return spec
end

return M
