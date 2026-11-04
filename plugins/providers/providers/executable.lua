-- Executable providers: every executable file in `<config dir>/providers/`
-- is a provider named after the file. maki runs it with one subcommand and
-- reads JSON from its output:
--   info      `{ display_name, base, system_prefix?, has_auth }`
--   models    optional list of models; without it the base's curated
--             models stand and the base lists the rest
--   resolve   `{ base_url?, headers }`, for each new instance
--   refresh   the same, after the server rejects the credentials
--   reload    the same, after a login elsewhere; it spends no token
--   login, logout   only with `has_auth`; login runs on the terminal
-- `base` names the provider whose wire protocol the script speaks. `info`
-- and `models` change only with the script, so their output is cached by
-- the file's size and modification time.

local compat = require("providers.openai_compat")
local http = require("providers.http")

local M = {}

M.DIR = "providers"
M.CACHE_FILE = "provider-scripts.json"
M.INFO_TIMEOUT_MS = 5000
M.SCRIPT_TIMEOUT_MS = 30000

local UNAUTHORIZED = 401
local DEFAULT_TIER = "medium"
local DEFAULT_OUTPUT = 16384
local DEFAULT_CONTEXT = 128000
local LOCAL_THINKING_BASE = "llama-cpp"
local GOOGLE_KEY_HEADER = "x-goog-api-key"
local API_SUFFIX = "/v1$"
local VALID_SLUG = "^[%w][%w_%-]*$"
local WINDOWS_EXTENSIONS = { exe = true, bat = true, cmd = true, ps1 = true }
local TIERS = { weak = true, medium = true, strong = true, compaction = true }
local FLAGS = { "supports_tool_examples", "supports_thinking", "requires_thinking", "supports_vision" }
local LIMITS = { "max_output_tokens", "context_window" }
local PRICES = { "input", "output", "cache_write", "cache_read" }
local THINKING_KEYS = {
  off = true,
  adaptive = true,
  minimal = true,
  low = true,
  medium = true,
  high = true,
  xhigh = true,
  max = true,
}

local function trim(text)
  return text:match("^%s*(.-)%s*$")
end

local function failure(message)
  return http.config_error(message)
end

-- The providers reference page section on scripts, naming {bases}, the
-- providers a script may name as its `base`.
function M.notes(bases)
  local ids = {}
  for id in pairs(bases) do
    ids[#ids + 1] = "`" .. id .. "`"
  end
  table.sort(ids)
  return string.format(
    [[
#### Executable providers

An executable file in the config `providers/` directory (`~/.config/makima/providers/` on Linux and macOS, `%%APPDATA%%\makima\providers\` on Windows) is a provider named after the file. Makima runs it with one subcommand and reads JSON from its output:

| Subcommand | Timeout | What it returns |
|------------|---------|-----------------|
| `info` | %ds | `display_name`, `base`, `has_auth` and an optional `system_prefix` |
| `models` | %ds | An optional list of models |
| `resolve` | %ds | Auth for each new agent: `headers` and an optional `base_url` |
| `refresh` | %ds | The same, after the server rejects the credentials |
| `reload` | %ds | The same, after a login elsewhere |
| `login` | none | Only with `has_auth`. Runs on your terminal, so use `makima auth login <name>`; `/login` in the TUI cannot |
| `logout` | %ds | Only with `has_auth`. Runs on `makima auth logout <name>` |

`resolve` runs each time an agent starts, so read tokens from disk instead of caching them, and a login in another process is picked up. `info` and `models` change only with the script, so their output is cached by the file's size and modification time.

`base` names the provider whose protocol the script speaks and whose curated models it inherits: one of %s. A `models` list replaces them:

```json
[{"id": "my-model-v2", "tier": "strong", "context_window": 200000, "max_output_tokens": 16384}]
```

Only `id` is required. Optional fields: `tier` (default `medium`), `context_window` (default 128K), `max_output_tokens` (default 16K), `pricing` (`{input, output, cache_write, cache_read}` per 1M tokens), and `supports_tool_examples`, `supports_thinking`, `requires_thinking` and `supports_vision`, which default to the base's. A `llama-cpp` based model can also declare `thinking_fields`: the JSON merged into the request body for `off`, `adaptive` and each effort level it accepts.

Script models are `<file name>/<model id>`. The name starts with a letter or digit, continues with letters, digits, `_` and `-`, and cannot take an id that is already registered.]],
    math.floor(M.INFO_TIMEOUT_MS / 1000),
    math.floor(M.INFO_TIMEOUT_MS / 1000),
    math.floor(M.SCRIPT_TIMEOUT_MS / 1000),
    math.floor(M.SCRIPT_TIMEOUT_MS / 1000),
    math.floor(M.SCRIPT_TIMEOUT_MS / 1000),
    math.floor(M.SCRIPT_TIMEOUT_MS / 1000),
    table.concat(ids, ", ")
  )
end

-- Runs `{path} {command}` without a terminal and returns its output, or nil
-- plus a config error. {owner} is the job owner `maki.fn.jobstart` takes.
function M.run(path, command, timeout_ms, owner)
  local label = path .. " " .. command
  local started, id = pcall(maki.fn.jobstart, { path, command }, { owner = owner })
  if not started then
    return nil, failure("failed to run " .. label .. ": " .. tostring(id))
  end
  local waited, result = pcall(maki.fn.jobwait, id, timeout_ms)
  if not waited then
    return nil, failure("failed to wait on " .. label .. ": " .. tostring(result))
  end
  if not result then
    maki.fn.jobstop(id)
    return nil, failure(string.format("%s timed out after %ds", label, math.floor(timeout_ms / 1000)))
  end
  if result.exit_code ~= 0 then
    local stderr = trim(result.stderr)
    return nil, failure(stderr ~= "" and stderr or label .. " exited with " .. result.exit_code)
  end
  return result.stdout
end

local function decode(path, command, stdout)
  local value, err = maki.json.decode(stdout)
  if type(value) ~= "table" then
    return nil, failure(string.format("%s %s: invalid JSON: %s", path, command, err or "not an object"))
  end
  return value
end

-- The script's `info`, or nil plus why it is unusable.
function M.parse_info(path, stdout, bases)
  local info, err = decode(path, "info", stdout)
  if not info then
    return nil, err
  end
  if
    type(info.display_name) ~= "string"
    or type(info.base) ~= "string"
    or type(info.has_auth) ~= "boolean"
    or (info.system_prefix ~= nil and type(info.system_prefix) ~= "string")
  then
    return nil, failure(path .. " info: needs display_name, base and has_auth")
  end
  if not bases[info.base] then
    return nil, failure(string.format("%s info: unknown base provider '%s'", path, info.base))
  end
  if info.system_prefix == "" then
    info.system_prefix = nil
  end
  return info
end

local function valid_pricing(pricing)
  if type(pricing) ~= "table" then
    return false
  end
  for _, key in ipairs(PRICES) do
    if type(pricing[key]) ~= "number" then
      return false
    end
  end
  local fast = pricing.fast
  return fast == nil or (type(fast) == "table" and type(fast.input) == "number" and type(fast.output) == "number")
end

local function valid_fields(fields)
  if type(fields) ~= "table" then
    return false
  end
  for key, fragment in pairs(fields) do
    if not THINKING_KEYS[key] or type(fragment) ~= "table" then
      return false
    end
  end
  return true
end

-- One `models` entry as a descriptor, or nil when it is malformed. Malformed
-- `thinking_fields` cost the field, not the model, and set the second result.
function M.model(entry)
  if type(entry) ~= "table" or type(entry.id) ~= "string" then
    return nil
  end
  if entry.tier ~= nil and not TIERS[entry.tier] then
    return nil
  end
  for _, key in ipairs(FLAGS) do
    if entry[key] ~= nil and type(entry[key]) ~= "boolean" then
      return nil
    end
  end
  for _, key in ipairs(LIMITS) do
    if entry[key] ~= nil and not compat.limit(entry[key]) then
      return nil
    end
  end
  if entry.pricing ~= nil and not valid_pricing(entry.pricing) then
    return nil
  end
  local fields_ok = entry.thinking_fields == nil or valid_fields(entry.thinking_fields)
  local model = {
    id = entry.id,
    tier = entry.tier or DEFAULT_TIER,
    max_output_tokens = entry.max_output_tokens or DEFAULT_OUTPUT,
    context_window = entry.context_window or DEFAULT_CONTEXT,
    pricing = entry.pricing,
    provider_data = fields_ok and entry.thinking_fields and { thinking_fields = entry.thinking_fields } or nil,
  }
  for _, key in ipairs(FLAGS) do
    model[key] = entry[key]
  end
  return model, not fields_ok
end

-- The script's `models` output as descriptors, entry by entry, so one bad
-- model never costs the list.
function M.parse_models(slug, stdout, base)
  if not stdout then
    return {}
  end
  local list, err = maki.json.decode(stdout)
  if type(list) ~= "table" then
    maki.log.warn(string.format("%s: invalid models JSON, using the base's models: %s", slug, err or "not a list"))
    return {}
  end
  local models = {}
  for _, entry in ipairs(list) do
    local model, dropped = M.model(entry)
    if not model then
      maki.log.warn(slug .. ": invalid model entry, skipping")
    else
      if dropped then
        maki.log.warn(string.format("%s: invalid thinking_fields on %s, dropping them", slug, model.id))
      elseif model.provider_data and base ~= LOCAL_THINKING_BASE then
        maki.log.warn(string.format("%s: thinking_fields only apply to %s models, ignoring", slug, LOCAL_THINKING_BASE))
      end
      models[#models + 1] = model
    end
  end
  return models
end

local function read_cache(path)
  local text = path and maki.fs.read(path)
  local cache = text and maki.json.decode(text)
  return type(cache) == "table" and cache or {}
end

-- The cached description of the script at {path}, or a fresh one while the
-- file changed. A failing `info` is not cached; `models` is optional.
local function describe(path, cached, run)
  local meta = maki.fs.metadata(path)
  if cached and meta and cached.mtime == meta.mtime and cached.size == meta.size then
    return cached, false
  end
  local info, err = run(path, "info", M.INFO_TIMEOUT_MS, "plugin")
  if not info then
    return nil, false, err
  end
  local models = run(path, "models", M.INFO_TIMEOUT_MS, "plugin")
  return { mtime = meta and meta.mtime or 0, size = meta and meta.size or 0, info = info, models = models }, true
end

local function slug_of(name)
  local stem, extension = name:match("^(.*)%.(%w+)$")
  return stem and WINDOWS_EXTENSIONS[extension:lower()] and stem or name
end

-- The scripts in {dir} that describe a provider with one of {bases}, as
-- `{ slug, path, info, models }`. Descriptions are cached in {cache_path}.
function M.scan(dir, cache_path, bases, run)
  run = run or M.run
  local cache, next_cache, changed = read_cache(cache_path), {}, false
  local scripts = {}
  for _, entry in ipairs(maki.fs.dir(dir) or {}) do
    local path = maki.fs.joinpath(dir, entry[1])
    local slug = slug_of(entry[1])
    if entry[2] ~= "file" or maki.fn.executable(path) ~= 1 then
      maki.log.debug("skipping non-executable " .. path)
    elseif not slug:match(VALID_SLUG) then
      maki.log.warn(string.format("invalid provider slug %q, skipping", slug))
    else
      local described, fresh, err = describe(path, cache[slug], run)
      changed = changed or fresh
      if not described then
        maki.log.warn(string.format("%s: no provider info, skipping: %s", slug, http.message(err)))
      else
        -- Kept even when rejected, so a refused script is not run again.
        next_cache[slug] = described
        local info, info_err = M.parse_info(path, described.info, bases)
        if info then
          local models = M.parse_models(slug, described.models, info.base)
          scripts[#scripts + 1] = { slug = slug, path = path, info = info, models = models }
        else
          maki.log.warn(string.format("%s: skipping: %s", slug, http.message(info_err)))
        end
      end
    end
  end
  for slug in pairs(cache) do
    changed = changed or next_cache[slug] == nil
  end
  if changed and cache_path then
    local _, err = maki.fs.atomic_write(cache_path, assert(maki.json.encode(next_cache)))
    if err then
      maki.log.debug("provider script cache not written: " .. err)
    end
  end
  table.sort(scripts, function(a, b)
    return a.slug < b.slug
  end)
  return scripts
end

-- `{ base_url?, headers }` from a script's {command} output.
function M.parse_auth(path, command, stdout)
  local auth, err = decode(path, command, stdout)
  if not auth then
    return nil, err
  end
  local valid = type(auth.headers) == "table" and (auth.base_url == nil or type(auth.base_url) == "string")
  for name, value in pairs(valid and auth.headers or {}) do
    valid = valid and type(name) == "string" and type(value) == "string"
  end
  if not valid then
    return nil, failure(string.format("%s %s: needs headers mapping names to strings", path, command))
  end
  return { base_url = auth.base_url, headers = auth.headers }
end

local function header(headers, name)
  for key, value in pairs(headers) do
    if key:lower() == name then
      return value
    end
  end
end

-- The provider for {script} (from `M.scan`), speaking {route}.
function M.provider(script, route, run)
  run = run or M.run
  local slug, path, info = script.slug, script.path, script.info
  local def = #script.models > 0 and { models = script.models } or route.def
  local default_base = route.base_url or route.def.base_url
  -- The credentials every instance uses, so a refresh serves them all.
  local auth

  local function fetch(command)
    local stdout, err = run(path, command, M.SCRIPT_TIMEOUT_MS)
    if not stdout then
      return nil, err
    end
    local fresh, parse_err = M.parse_auth(path, command, stdout)
    if not fresh then
      return nil, parse_err
    end
    -- A script that leaves base_url out keeps the one in use; the default
    -- origin would silently repoint the token.
    if command ~= "resolve" and not fresh.base_url and auth then
      fresh.base_url = auth.base_url
    end
    auth = fresh
    return auth
  end

  -- Runs {command} under the provider's credential lock, which other
  -- processes take too.
  local function locked(ctx, command)
    return ctx.credentials:lock(function()
      return fetch(command)
    end)
  end

  local function client(config)
    local configured, err = http.headers(slug, {}, config.headers)
    if not configured then
      return nil, err
    end
    local api = { config = config }
    -- Follows the credentials, which a refresh may move.
    function api.sync()
      api.base = auth.base_url or default_base
      api.root = api.base and api.base:gsub(API_SUFFIX, "")
    end
    function api.headers(extra)
      local headers = table.clone(extra or {})
      for name, value in pairs(auth.headers) do
        http.set_header(headers, name, value)
      end
      for name, value in pairs(configured) do
        http.set_header(headers, name, value)
      end
      return headers
    end
    function api.request_json(call_ctx, request)
      request.headers = api.headers(request.headers)
      local response, send_err = http.send(call_ctx, request)
      if not response then
        return nil, send_err
      end
      return http.read_json(response)
    end
    function api.get_json(call_ctx, url, extra)
      return api.request_json(call_ctx, { url = url, headers = extra })
    end
    api.pool = {
      current = function()
        return header(auth.headers, GOOGLE_KEY_HEADER) or ""
      end,
    }
    return api
  end

  local spec = {
    id = slug,
    display_name = info.display_name,
    models = compat.descriptors(def),
    auth_status = function()
      return { state = "configured" }
    end,
  }
  if info.has_auth then
    spec.auth = {
      {
        id = "login",
        label = "Log in with " .. info.display_name,
        login = function(_, ui)
          return ui:run({ path, "login" })
        end,
      },
    }
    function spec.logout()
      local _, err = run(path, "logout", M.SCRIPT_TIMEOUT_MS)
      if err then
        return nil, err
      end
    end
  end

  function spec.discover(ctx, config)
    if #script.models > 0 or not route.list_models then
      return spec.models
    end
    local api, err = client(config)
    if not api then
      return nil, err
    end
    local _, resolve_err = locked(ctx, "resolve")
    if resolve_err then
      return nil, resolve_err
    end
    api.sync()
    local rows, list_err = route.list_models(ctx, api)
    if not rows then
      return nil, list_err
    end
    return compat.merge(def, rows)
  end

  function spec.prepare(ctx, model_info, config)
    local api, err = client(config)
    if not api then
      return nil, err
    end
    local _, resolve_err = locked(ctx, "resolve")
    if resolve_err then
      return nil, resolve_err
    end
    local model = compat.describe(def, model_info)

    local function attempt(call_ctx, request, emit)
      api.sync()
      local copy = table.clone(request)
      copy.model = table.clone(request.model)
      local prefix = info.system_prefix
      if prefix and route.system_prefix == nil then
        copy.system = prefix .. "\n\n" .. (copy.system or "")
      end
      return route.stream(api, model, call_ctx, copy, emit, route.system_prefix == "block" and prefix or nil)
    end

    local instance = {
      stream = function(call_ctx, request, emit)
        -- A 401 is retried after a refresh only when it came before any
        -- event: replaying a partly delivered answer would repeat it.
        local emitted = 0
        local result, stream_err = attempt(call_ctx, request, function(event)
          emitted = emitted + 1
          return emit(event)
        end)
        if result or emitted > 0 or type(stream_err) ~= "table" or stream_err.status ~= UNAUTHORIZED then
          return result, stream_err
        end
        local _, refresh_err = locked(call_ctx, "refresh")
        if refresh_err then
          maki.log.warn(slug .. ": silent refresh failed: " .. http.message(refresh_err))
          return nil, stream_err
        end
        return attempt(call_ctx, request, emit)
      end,
      refresh_auth = function(call_ctx)
        local _, refresh_err = locked(call_ctx, "refresh")
        if refresh_err then
          return nil, refresh_err
        end
      end,
      -- Not locked: it spends no token, and must not wait on another
      -- process's slow refresh.
      reload_auth = function()
        local _, reload_err = fetch("reload")
        if reload_err then
          return nil, reload_err
        end
      end,
    }
    if route.fetch_usage then
      function instance.fetch_usage(call_ctx)
        api.sync()
        return route.fetch_usage(call_ctx, api)
      end
    end
    return instance, model
  end

  return spec
end

return M
