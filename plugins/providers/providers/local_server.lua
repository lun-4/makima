-- Local OpenAI-compatible servers (Ollama, llama.cpp): the host comes from
-- `providers.toml` `base_url`, then `<host_env>`, and the API key is
-- optional. `protocol = "openai-responses"` in `providers.toml` switches
-- streaming to the Responses API with prompt progress. `M.provider(def)`
-- returns a `register_provider` spec.
--
-- {def} fields:
--   `id`, `display_name`, `env` (API key variable), `host_env`,
--   `default_host` (offered at login).
--   `cloud_url`: the base URL used with a key but no host.
--   `defaults`: descriptor fields every model starts from.
--   `local_thinking`: Chat requests carry the local thinking fields.
--   `list_models(ctx, client)`: the discovered descriptors, sorted by id.
--   `notes`: Markdown for the providers reference page.

local chat = require("providers.chat_completions")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local responses = require("providers.responses")
local thinking = require("providers.thinking")

local M = {}

M.PRICING_FREE = { input = 0, output = 0, cache_write = 0, cache_read = 0 }

local API_SUFFIX = "/v1"
local CHAT_PATH = "/chat/completions"
local RESPONSES_PATH = "/responses"
local RESPONSES_PROTOCOL = "openai-responses"
local MAX_TOKENS_FIELD = "max_tokens"
local BASE_URL_KEY = "base_url"
local HOST_PROMPT = "%s host URL"

local function host(def, config)
  local value = config.base_url or maki.uv.os_getenv(def.host_env)
  return value ~= "" and value or nil
end

-- Base URL, key pool and headers for {config}. Nil plus a config error
-- without a host (and no cloud fallback) or with an unset header variable.
function M.client(ctx, def, config)
  local keys = http.api_keys(ctx, config, def.env)
  local root = host(def, config)
  local base
  if root then
    base = root .. API_SUFFIX
  elseif keys and def.cloud_url then
    base = def.cloud_url
  else
    return nil, http.config_error(def.host_env .. " not set")
  end
  local configured, err = http.headers(def.id, {}, config.headers)
  if not configured then
    return nil, err
  end
  local client = { base = base, root = base:gsub(API_SUFFIX .. "$", ""), pool = keys and http.key_pool(keys) }
  -- {extra} plus the bearer key, if any, with configured headers replacing
  -- any of them.
  function client.headers(extra)
    local headers = table.clone(extra or {})
    if client.pool then
      headers.Authorization = "Bearer " .. client.pool.current()
    end
    for name, value in pairs(configured) do
      http.set_header(headers, name, value)
    end
    return headers
  end
  function client.request_json(call_ctx, request)
    request.headers = client.headers(request.headers)
    local response, send_err = http.send(call_ctx, request)
    if not response then
      return nil, send_err
    end
    return http.read_json(response)
  end
  return client
end

-- Streams {request} through {client} (`base` and `headers(extra)`), with
-- the Responses API when {config} asks for it. {fields} are the model's
-- local thinking fields, if it declares any.
function M.stream(def, client, config, ctx, request, emit, fields)
  local max = thinking.max_budget(request.model)
  if config.protocol == RESPONSES_PROTOCOL then
    local body =
      responses.build_request(request, responses.reasoning(request.thinking, thinking.dialects.standard, max))
    body.return_progress = true
    return http.post_stream(ctx, client.base .. RESPONSES_PATH, client.headers(), body, responses.stream, emit)
  end
  local body = chat.build_request(request, { max_tokens_field = MAX_TOKENS_FIELD, include_stream_usage = true })
  if def.local_thinking then
    thinking.apply_local(body, request.thinking, fields, max)
  end
  return http.post_stream(ctx, client.base .. CHAT_PATH, client.headers(), body, chat.stream, emit)
end

local function auth_status(def)
  local key_status = compat.auth_status(def)
  return function(ctx, config)
    local status = key_status(ctx, config)
    if status.state == "missing" and config.base_url then
      return { state = "configured" }
    end
    return status
  end
end

function M.provider(def)
  local spec = {
    id = def.id,
    display_name = def.display_name,
    config = { { key = BASE_URL_KEY, prompt = HOST_PROMPT:format(def.display_name), default = def.default_host } },
    auth = compat.api_key_auth({ display_name = def.display_name, key_optional = true }),
    auth_status = auth_status(def),
    docs = { env = { def.host_env, def.env }, endpoint = def.default_host .. API_SUFFIX, notes = def.notes },
  }

  function spec.discover(ctx, config)
    local client = M.client(ctx, def, config)
    if not client then
      return {}
    end
    local rows, err = def.list_models(ctx, client)
    if not rows then
      return nil, err
    end
    local out = {}
    for i, row in ipairs(rows) do
      out[i] = compat.describe(def, row)
    end
    return out
  end

  function spec.prepare(ctx, info, config)
    local client, err = M.client(ctx, def, config)
    if not client then
      return nil, err
    end
    local instance = {
      stream = function(call_ctx, request, emit)
        return M.stream(def, client, config, call_ctx, request, emit)
      end,
    }
    if client.pool then
      instance.key_count = client.pool.count
      instance.rotate_key = function()
        return client.pool.rotate()
      end
    end
    return instance, compat.describe(def, info)
  end

  return spec
end

return M
