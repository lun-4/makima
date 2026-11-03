-- The models.dev catalog: every provider it lists that speaks Chat
-- Completions or Anthropic Messages at a published base URL becomes a
-- provider of its own. The catalog is cached on disk as an index of
-- providers plus one file per provider with its models, kept as models.dev
-- sent them. Startup reads only the index and never the network; a
-- provider's models are read when first needed. Discovery refreshes the cache
-- once it is a day old, or whenever the user asks.

local anthropic = require("providers.anthropic")
local anthropic_provider = require("providers.anthropic_provider")
local chat = require("providers.chat_completions")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local thinking = require("providers.thinking")

local M = {}

M.URL = "https://models.dev/api.json"
M.CACHE_DIR = "models-dev"
M.INDEX_FILE = "index.json"
M.TTL_SECS = 86400
M.SECTION = "Providers from Models.dev"
-- The providers reference page section on catalog providers.
M.NOTES = string.format(
  [[
#### Providers from models.dev

Every provider in the [models.dev](https://models.dev) catalog that speaks Chat Completions or Anthropic Messages at a published base URL becomes a provider of its own, under its models.dev id. A built-in provider, a `providers.toml` section or a script with the same id wins. The login picker lists them under "%s". Each takes its key from the environment variables models.dev names for it, or from `makima auth login <id>`, and shows its models once it has one.

The catalog is cached in the cache directory (`%s/`) and fetched again once it is a day old, or by `makima models --refresh`. Startup reads only the cache and never waits on the network, so a provider new to the catalog appears from the next start.]],
  M.SECTION,
  M.CACHE_DIR
)
M.MESSAGES = "messages"
M.CHAT = "chat"

local MAX_BYTES = 64 * 1024 * 1024
local DEFAULT_CONTEXT = 128000
local DEFAULT_OUTPUT = 64000
local IMAGE = "image"
local CHAT_PATH = "/chat/completions"
local MESSAGES_PATH = "/messages"
local JSON_SUFFIX = ".json"
local NANOS = 1e9
local INDEX_FIELDS = { "name", "env", "npm", "api" }
-- What a provider id and a file name may hold.
local VALID_ID = "^[%w_%-][%w_%-%.]*$"
local PROTOCOLS = {
  ["@ai-sdk/openai-compatible"] = M.CHAT,
  ["@ai-sdk/anthropic"] = M.MESSAGES,
}
-- Priced per subscription, so every model reads as free; `zai` carries the
-- pay as you go rates for the same models.
local BLOCKED = { ["zai-coding-plan"] = true }
-- models.dev ids of providers registered under another id.
local RENAMED = { ["github-copilot"] = "copilot", ["regolo-ai"] = "regolo" }

local function object(value)
  return type(value) == "table" and value or nil
end

local function now()
  local time = maki.time.now()
  return time.secs + time.nanosecs / NANOS
end

local function read_json(path)
  local meta = maki.fs.metadata(path)
  local text = meta and maki.fs.read(path)
  return text and object(maki.json.decode(text)), meta
end

local function write(path, value)
  local _, err = maki.fs.atomic_write(path, assert(maki.json.encode(value)))
  if err then
    maki.log.warn("models.dev cache not written: " .. err)
  end
end

-- The cache directory, or nil where the host has none.
function M.cache_dir()
  local dir = maki.env.cache_dir()
  return dir and maki.fs.joinpath(dir, M.CACHE_DIR)
end

local Cache = {}
Cache.__index = Cache

-- A catalog cached in {dir}; nil keeps it in memory only.
function M.new(dir)
  return setmetatable({ dir = dir, lock = maki.async.semaphore(1) }, Cache)
end

function Cache:file(id)
  return maki.fs.joinpath(self.dir, id .. JSON_SUFFIX)
end

-- Keeps the whole {catalog} and splits it into the cache, the index last so
-- its age is the cache's.
function Cache:store(catalog)
  self.index = catalog
  if not self.dir then
    return
  end
  maki.fs.mkdir(self.dir, { parents = true })
  local index = {}
  for id, entry in pairs(catalog) do
    if object(entry) and id:match(VALID_ID) then
      write(self:file(id), entry)
      index[id] = {}
      for _, field in ipairs(INDEX_FIELDS) do
        index[id][field] = entry[field]
      end
    end
  end
  write(maki.fs.joinpath(self.dir, M.INDEX_FILE), index)
end

-- Reads the index, whatever its age. Never touches the network.
function Cache:load()
  local index, meta
  if self.dir then
    index, meta = read_json(maki.fs.joinpath(self.dir, M.INDEX_FILE))
  end
  if index then
    self.index, self.mtime = index, meta.mtime
  end
  return self.index
end

local function fetch(ctx)
  local response, err = http.send(ctx, { url = M.URL })
  if not response then
    return nil, err
  end
  local body, read_err = http.read_body(response, MAX_BYTES)
  response:close()
  if not body then
    return nil, read_err
  end
  local catalog = object(maki.json.decode(body))
  if not catalog then
    return nil, "invalid models.dev catalog"
  end
  return catalog
end

-- The catalog, fetched again when the cache is older than `TTL_SECS` or
-- {force} asks, plus the error of the last fetch if it failed. One fetch
-- serves every caller that was waiting on it; a failed fetch keeps the old
-- catalog.
function Cache:refresh(ctx, force)
  local started = now()
  local permit = self.lock:acquire()
  local fresh = self.mtime and started - self.mtime < M.TTL_SECS
  if not (self.attempted and self.attempted >= started) and (force or not fresh) then
    self.attempted = now()
    local catalog, err = fetch(ctx)
    self.error = not catalog and err or nil
    if catalog then
      self:store(catalog)
      self.mtime = self.attempted
    end
  end
  permit:release()
  return self.index, self.error
end

-- Provider {id}'s entry; its `models` may not be read yet.
function Cache:entry(id)
  return self.index and object(self.index[id])
end

-- Provider {id}'s models, read from its file the first time.
function Cache:models(id)
  local entry = self:entry(id)
  if entry and entry.models == nil and self.dir then
    local full = read_json(self:file(id))
    entry.models = full and object(full.models) or {}
  end
  return entry and object(entry.models)
end

-- Model {model_id} of provider {id}.
function Cache:model(id, model_id)
  local models = self:models(id)
  return models and object(models[model_id])
end

local function servable(id, entry)
  return object(entry)
    and id:match(VALID_ID)
    and not RENAMED[id]
    and not BLOCKED[id]
    and PROTOCOLS[entry.npm] ~= nil
    and type(entry.api) == "string"
end

-- Provider {id}'s entry, if it can be served.
function Cache:served(id)
  local entry = self:entry(id)
  return servable(id, entry) and entry or nil
end

-- The ids of catalog providers that can be served and are not in {taken},
-- sorted.
function Cache:servable(taken)
  local ids = {}
  for id, entry in pairs(self.index or {}) do
    if not taken[id] and servable(id, entry) then
      ids[#ids + 1] = id
    end
  end
  table.sort(ids)
  return ids
end

local function positive(value)
  local limit = compat.limit(value)
  return limit and limit > 0 and limit or nil
end

local function number(value)
  return type(value) == "number" and value or 0
end

-- No price, or zero input and output, is free.
function M.is_free(row)
  local cost = object(row.cost)
  return not cost or (number(cost.input) == 0 and number(cost.output) == 0)
end

-- A descriptor for catalog model {id}.
function M.descriptor(id, row)
  local limit, cost, modalities = object(row.limit) or {}, object(row.cost) or {}, object(row.modalities)
  local vision
  if modalities then
    vision = table.find(object(modalities.input) or {}, IMAGE) ~= nil
  elseif type(row.attachment) == "boolean" then
    vision = row.attachment
  end
  return {
    id = id,
    context_window = positive(limit.context) or DEFAULT_CONTEXT,
    max_output_tokens = positive(limit.output) or DEFAULT_OUTPUT,
    pricing = {
      input = number(cost.input),
      output = number(cost.output),
      cache_write = number(cost.cache_write),
      cache_read = number(cost.cache_read),
    },
    supports_thinking = type(row.reasoning) == "boolean" and row.reasoning or nil,
    supports_vision = vision,
  }
end

-- Descriptors from {describe} (default `M.descriptor`) for the {models} that
-- {keep}`(row)` accepts, sorted.
function M.descriptors(models, keep, describe)
  local out = {}
  for id, row in pairs(models) do
    if object(row) and keep(row) then
      out[#out + 1] = (describe or M.descriptor)(id, row)
    end
  end
  table.sort(out, function(a, b)
    return a.id < b.id
  end)
  return out
end

-- The first of {names} that is set, else the first: where a key comes from.
function M.env_name(names)
  for _, name in ipairs(names) do
    if (maki.uv.os_getenv(name) or "") ~= "" then
      return name
    end
  end
  return names[1]
end

local function env_names(entry)
  return entry and object(entry.env) or {}
end

-- Keys for {entry} from its variables, saved credentials or {config}.
function M.api_keys(ctx, config, entry)
  return http.api_keys(ctx, config, M.env_name(env_names(entry)))
end

-- The protocol {entry} speaks: `M.CHAT` or `M.MESSAGES`.
function M.protocol(entry)
  return PROTOCOLS[entry.npm]
end

-- The base URL, protocol and headers of {entry} with keys from {pool}, or nil
-- plus a config error for an unset header variable.
function M.client(id, entry, pool, config)
  local configured, err = http.headers(id, {}, config.headers)
  if not configured then
    return nil, err
  end
  local protocol = M.protocol(entry)
  local client = { base = entry.api, protocol = protocol }
  -- {extra} plus the key, with configured headers replacing any of them.
  function client.headers(extra)
    local headers = table.clone(extra or {})
    local key = pool.current()
    if protocol == M.MESSAGES then
      headers["x-api-key"] = key
    else
      headers.Authorization = "Bearer " .. key
    end
    for name, value in pairs(configured) do
      http.set_header(headers, name, value)
    end
    return headers
  end
  return client
end

-- Streams {request} through {client} in its protocol, with {extra} headers.
function M.stream(client, ctx, request, emit, extra)
  local headers = table.clone(extra or {})
  if client.protocol == M.MESSAGES then
    headers["anthropic-version"] = anthropic_provider.API_VERSION
    local body = anthropic.build_request(request)
    return http.post_stream(ctx, client.base .. MESSAGES_PATH, client.headers(headers), body, anthropic.stream, emit)
  end
  local body = chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
  body.reasoning_effort =
    thinking.effort(request.thinking, thinking.dialects.prefer_high, thinking.max_budget(request.model))
  return http.post_stream(ctx, client.base .. CHAT_PATH, client.headers(headers), body, chat.stream, emit)
end

-- The status of {entry}'s key, without the network.
function M.auth_status(ctx, config, entry)
  return compat.auth_status({ env = M.env_name(env_names(entry)) })(ctx, config)
end

-- The provider for catalog entry {id} of {cache}. Its models stay hidden
-- until it has a key.
function M.provider(cache, id)
  local name = cache:entry(id).name
  name = type(name) == "string" and name or id
  local spec = {
    id = id,
    display_name = name,
    section = M.SECTION,
    auth = compat.api_key_auth({ display_name = name }),
    auth_status = function(ctx, config)
      return M.auth_status(ctx, config, cache:entry(id))
    end,
  }

  function spec.discover(ctx, config, refresh)
    cache:refresh(ctx, refresh)
    local entry = cache:entry(id)
    if not entry or not M.api_keys(ctx, config, entry) then
      return {}
    end
    return M.descriptors(cache:models(id) or {}, function()
      return true
    end)
  end

  function spec.prepare(ctx, info, config)
    local entry = cache:entry(id)
    if not entry then
      return nil, http.config_error("unknown provider '" .. id .. "'")
    end
    local row = cache:model(id, info.id)
    if not row then
      return nil, http.config_error(string.format("model '%s' not found from provider '%s'", info.id, id))
    end
    local keys, err = M.api_keys(ctx, config, entry)
    if not keys then
      return nil, err
    end
    local pool = http.key_pool(keys)
    local client, client_err = M.client(id, entry, pool, config)
    if not client then
      return nil, client_err
    end
    return {
      key_count = pool.count,
      rotate_key = function()
        return pool.rotate()
      end,
      stream = function(call_ctx, request, emit)
        return M.stream(client, call_ctx, request, emit)
      end,
    },
      M.descriptor(info.id, row)
  end

  return spec
end

return M
