-- How a provider whose credentials come from elsewhere, such as an
-- executable provider's script or the Aperture gateway, reaches a provider
-- implemented here. A route streams through a client: `base`, the base URL,
-- and `headers(extra)`, which carry the credentials. Listings also use
-- `get_json(ctx, url, extra)`, `request_json(ctx, request)` and `root`, the
-- base without `/v1`.
--
-- Route fields: `def` (curated models and the default `base_url`),
-- `stream(client, model, ctx, request, emit)` for descriptor {model},
-- optional `list_models(ctx, client)` returning discovered rows, optional
-- `fetch_usage(ctx, client)`, and `system_prefix`: `"block"` when `stream`
-- takes the prefix as a sixth argument, `"none"` when the provider drops it.
-- Without it the prefix goes before the system prompt.

local anthropic = require("providers.anthropic_provider")
local compat = require("providers.openai_compat")
local copilot = require("providers.copilot")
local google = require("providers.google")
local local_server = require("providers.local_server")
local openai = require("providers.openai")

local M = {}

local function compat_route(name)
  local def = require("providers." .. name).def
  return {
    def = def,
    stream = function(client, model, ctx, request, emit)
      return compat.stream(def, client, model, ctx, request, emit)
    end,
    list_models = def.list_models or compat.list_models(def),
    fetch_usage = def.fetch_usage,
  }
end

local function local_route(name)
  local def = require("providers." .. name).def
  return {
    def = def,
    base_url = def.default_host .. "/v1",
    stream = function(client, model, ctx, request, emit)
      local data = type(model.provider_data) == "table" and model.provider_data or {}
      return local_server.stream(def, client, {}, ctx, request, emit, data.thinking_fields)
    end,
    list_models = def.list_models,
  }
end

-- The Messages API wants the bare origin and its version header.
local function messages_api(client)
  return {
    base = anthropic.origin(client.base),
    headers = client.headers,
    get_json = function(ctx, url, extra)
      local headers = table.clone(extra or {})
      headers["anthropic-version"] = anthropic.API_VERSION
      return client.get_json(ctx, url, headers)
    end,
  }
end

M.ROUTES = {
  anthropic = {
    def = anthropic.def,
    system_prefix = "block",
    stream = function(client, model, ctx, request, emit, system_prefix)
      return anthropic.stream(messages_api(client), model, ctx, request, emit, system_prefix)
    end,
    list_models = function(ctx, client)
      return anthropic.list_models(ctx, messages_api(client))
    end,
    fetch_usage = function(ctx, client)
      return anthropic.fetch_usage(ctx, messages_api(client))
    end,
  },
  openai = {
    def = openai.def,
    stream = function(client, _, ctx, request, emit)
      return openai.route_stream(client, ctx, request, emit)
    end,
    list_models = compat.list_models(openai.def),
  },
  google = {
    def = google.def,
    system_prefix = "none",
    stream = function(client, _, ctx, request, emit)
      return google.stream(client, ctx, request, emit)
    end,
    list_models = google.def.list_models,
  },
  copilot = {
    def = copilot.def,
    base_url = copilot.DEFAULT_ENDPOINT,
    stream = copilot.route_stream,
    list_models = copilot.route_list,
  },
  ollama = local_route("ollama"),
  ["llama-cpp"] = local_route("llama_cpp"),
}

for _, name in ipairs({ "zai", "mistral", "deepseek", "openrouter", "requesty", "synthetic", "regolo", "tensorx" }) do
  M.ROUTES[require("providers." .. name).def.id] = compat_route(name)
end

return M
