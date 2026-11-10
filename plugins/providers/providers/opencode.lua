-- Opencode Zen and Opencode Go, served from their models.dev catalog
-- entries. Free models stay hidden unless `enable_free_models` is set, and
-- then work without a key through the public key. Without either, nothing is
-- listed and a request says how to get access. Zen also takes
-- `<catalog provider>/<model>` ids and sends them to that provider.

local catalog = require("providers.catalog")
local clock = require("providers.clock")
local compat = require("providers.openai_compat")
local http = require("providers.http")

local M = {}

M.SESSION_HEADER = "x-opencode-session"
M.PUBLIC_KEY = "public"
M.EMPTY_USAGE_ERROR = "Opencode Go usage response contained no usage lanes; the endpoint schema likely changed"
local FREE_MODELS_NOTE = [[
Models come from the provider's models.dev catalog entry. Free models stay hidden until you turn them on in `~/.config/makima/providers.toml`; they then work without a key through the public key:

```toml
[opencode]
enable_free_models = true
```

Zen and Go both read it from `[opencode]`.]]

M.ZEN = {
  id = "opencode",
  display_name = "Opencode Zen",
  delegates = true,
  notes = FREE_MODELS_NOTE
    .. "\n\nZen also takes `<catalog provider>/<model>` ids and sends them to that provider. A delegated model reads the catalog provider's API key from its environment variable. Saved credentials are provider-scoped: `makima auth login nvidia`, for example, configures the direct NVIDIA provider and does not configure `opencode/nvidia/<model>`. Set the delegated provider's environment variable separately. It can contain the same API key.",
}
M.GO = { id = "opencode-go", display_name = "Opencode Go", usage = true, notes = FREE_MODELS_NOTE }

-- Stands in for the catalog entry until the catalog is downloaded.
local FALLBACK_ENTRY = { env = { "OPENCODE_API_KEY" } }
local USAGE_PATH = "/usage"
local LANES = {
  { "rolling", { Hours = 5 } },
  { "weekly", { Weekly = {} } },
  { "monthly", "Monthly" },
}

local function gated_error(id)
  return http.config_error(
    string.format(
      "provider '%s' has no API key; run `makima auth login %s` or set providers.%s.enable_free_models = true to use its free models",
      id,
      id,
      M.ZEN.id
    )
  )
end

-- Whether free models are shown. Both members read it from Zen's section,
-- `[opencode]`.
function M.free_models(config, member)
  if member ~= M.ZEN then
    config = makima.api.provider_config(M.ZEN.id) or {}
  end
  return config.enable_free_models == true
end

-- Keys and whether they unlock every model: a real key does, the public key
-- only the free ones, nil none.
local function access(ctx, config, member, entry)
  local keys = catalog.api_keys(ctx, config, entry)
  if keys then
    return keys, true
  end
  if M.free_models(config, member) then
    return { M.PUBLIC_KEY }, false
  end
end

local function describe(id, row)
  local model = catalog.descriptor(id, row)
  model.free = catalog.is_free(row)
  return model
end

-- The `/usage` lanes as usage limits.
function M.parse_usage(body)
  local lanes = type(body) == "table" and type(body.usage) == "table" and body.usage or {}
  local limits = {}
  for _, lane in ipairs(LANES) do
    local value = lanes[lane[1]]
    if type(value) == "table" then
      limits[#limits + 1] = {
        kind = lane[2],
        percentage = compat.limit(value.percent),
        reset_at = clock.parse_rfc3339(value.resetsAt),
      }
    end
  end
  if #limits == 0 then
    return nil, http.config_error(M.EMPTY_USAGE_ERROR)
  end
  return { limits = limits }
end

-- An instance for {model_id} of catalog provider {sub}, keyed only from its
-- environment. The Opencode context cannot access another provider's saved
-- credentials and must never substitute its own.
local function delegate(ctx, config, sub, entry, model_id)
  local env = catalog.env_name(type(entry.env) == "table" and entry.env or {})
  local keys, key_err = http.env_api_keys({ id = sub }, env)
  if not keys then
    return nil, key_err
  end
  local client, err = catalog.client(sub, entry, http.key_pool(keys), config)
  if not client then
    return nil, err
  end
  return {
    stream = function(call_ctx, request, emit)
      request.model.id = model_id
      return catalog.stream(client, call_ctx, request, emit)
    end,
  }
end

-- A route for credentials that come from elsewhere: `<catalog
-- provider>/<model>` goes to that provider and anything else to Zen, at the
-- client's base if it has one.
function M.delegated(cache)
  return {
    def = { models = {} },
    stream = function(client, _, ctx, request, emit)
      local sub, model_id = request.model.id:match("^([^/]+)/(.+)$")
      local entry = sub and cache:served(sub)
      if not entry then
        model_id, entry = request.model.id, cache:entry(M.ZEN.id)
      end
      if not entry then
        return nil, http.config_error("provider '" .. M.ZEN.id .. "' is not in the models.dev catalog yet")
      end
      request.model.id = model_id
      local api = { base = client.base or entry.api, protocol = catalog.protocol(entry), headers = client.headers }
      return catalog.stream(api, ctx, request, emit)
    end,
  }
end

-- The provider for {member} (`M.ZEN` or `M.GO`), backed by {cache}.
function M.provider(cache, member)
  local id = member.id
  local function entry()
    return cache:entry(id)
  end
  local spec = {
    id = id,
    display_name = member.display_name,
    docs = { env = FALLBACK_ENTRY.env, notes = member.notes },
    auth = compat.api_key_auth({ display_name = member.display_name }),
    auth_status = function(ctx, config)
      return catalog.auth_status(ctx, config, entry() or FALLBACK_ENTRY)
    end,
  }

  function spec.discover(ctx, config, refresh)
    local _, err = cache:refresh(ctx, refresh)
    if refresh and err then
      return nil, err
    end
    local own = entry()
    local keys, keyed = access(ctx, config, member, own or FALLBACK_ENTRY)
    if not own or not keys then
      return {}
    end
    return catalog.descriptors(cache:models(id) or {}, function(row)
      local free = catalog.is_free(row)
      return (not free or M.free_models(config, member)) and (keyed or free)
    end, describe)
  end

  function spec.prepare(ctx, info, config)
    local sub, model_id = info.id:match("^([^/]+)/(.+)$")
    local sub_entry = member.delegates and sub ~= id and sub and cache:served(sub)
    if sub_entry then
      return delegate(ctx, config, sub, sub_entry, model_id)
    end
    -- `<own id>/<model>` names the provider's own model.
    local own_id = sub == id and model_id or info.id
    local keys = access(ctx, config, member, entry() or FALLBACK_ENTRY)
    if not keys then
      return {
        stream = function()
          return nil, gated_error(id)
        end,
      }
    end
    local pool = http.key_pool(keys)
    -- The entry is read per call, so a catalog downloaded after startup
    -- serves a model prepared before it.
    local function client()
      local own = entry()
      if not own then
        return nil, http.config_error("provider '" .. id .. "' is not in the models.dev catalog yet")
      end
      return catalog.client(id, own, pool, config)
    end
    local instance = {
      key_count = pool.count,
      rotate_key = function()
        return pool.rotate()
      end,
      stream = function(call_ctx, request, emit)
        local api, err = client()
        if not api then
          return nil, err
        end
        request.model.id = own_id
        if not cache:model(id, own_id) then
          return nil, http.config_error(string.format("model '%s' not found from provider '%s'", own_id, id))
        end
        local extra = request.session_id and { [M.SESSION_HEADER] = request.session_id } or nil
        return catalog.stream(api, call_ctx, request, emit, extra)
      end,
    }
    if member.usage then
      function instance.fetch_usage(call_ctx)
        local api, err = client()
        if not api then
          return nil, err
        end
        local response, send_err = http.send(call_ctx, { url = api.base .. USAGE_PATH, headers = api.headers() })
        if not response then
          return nil, send_err
        end
        local body, read_err = http.read_json(response)
        if read_err then
          return nil, read_err
        end
        return M.parse_usage(body)
      end
    end
    local row = cache:model(id, own_id)
    return instance, row and describe(info.id, row) or nil
  end

  return spec
end

return M
