-- The shape every OpenAI Chat Completions provider shares: API keys and
-- rotation, base URL, configured headers, curated models, discovery, login
-- and auth status. `M.provider(def)` returns a `register_provider` spec.
--
-- {def} fields:
--   `id`, `display_name`, `env` (API key variable), `base_url` (default).
--   `plans`: optional `{ id, label, base_url?, model?, login_url? }` list,
--   chosen at login and stored as `plan` in `providers.toml`.
--   `login_url`, `model`: the key page and default model id after login.
--   `key_optional`: login says the key can be skipped.
--   `models`: curated descriptors, each with optional `aliases`: ids that
--   share the row. Unknown ids take the row with the longest matching prefix.
--   `defaults`: descriptor fields every model starts from.
--   `key_headers(key)`: the headers that carry a key, instead of a bearer
--   `Authorization`.
--   `max_tokens_field` (default `max_tokens`), `include_stream_usage`.
--   `adjust(descriptor)`: returns the descriptor with model quirks applied.
--   `encode(body, request, model)`: edits the request body; may return
--   extra headers.
--   `api_error(err)`: reclassifies an API error.
--   `parse_model(row)`: one `/models` row to a descriptor, or nil.
--   `list_models(ctx, client)`: replaces the `/models` listing.
--   `fetch_usage(ctx, client)`: the instance's `fetch_usage`.
--   `notes`: Markdown for the providers reference page.

local chat = require("providers.chat_completions")
local http = require("providers.http")

local M = {}

M.MODELS_PATH = "/models"
M.PER_MILLION = 1000000

local CHAT_PATH = "/chat/completions"
local JSON_TYPE = "application/json"
local DEFAULT_MAX_TOKENS_FIELD = "max_tokens"
local BASE_URL_SUFFIX = "_BASE_URL"
local METHOD_ID = "api_key"
local METHOD_LABEL = "API key"
local KEY_SKIP_HINT = " (or Enter to skip)"
local PLAN_KEY = "plan"
local PLAN_PROMPT = "Plan"
local MASK_EDGE = 4
local MAX_U32 = 4294967295

local function find(list, id)
  for _, item in ipairs(list or {}) do
    if item.id == id then
      return item
    end
  end
end

local function plan(def, config)
  return config.plan and find(def.plans, config.plan)
end

function M.base_url_env(def)
  return (def.id:upper():gsub("-", "_")) .. BASE_URL_SUFFIX
end

local function trim(url)
  return (url:gsub("/+$", ""))
end

-- `<ID>_BASE_URL`, then `base_url` and the plan's from `providers.toml`,
-- then the default. Trailing slashes are removed so path joins do not
-- produce a double slash.
function M.base_url(def, config)
  local override = makima.uv.os_getenv(M.base_url_env(def))
  if override and override ~= "" then
    return trim(override)
  end
  local chosen = plan(def, config)
  return trim(config.base_url or (chosen and chosen.base_url) or def.base_url)
end

-- A JSON number that fits a u32 token count; anything else is unknown.
function M.limit(value)
  if type(value) == "number" and value >= 0 and value <= MAX_U32 and value % 1 == 0 then
    return value
  end
end

local function lookup(models, id)
  local best, best_len = nil, 0
  for _, row in ipairs(models or {}) do
    for _, prefix in ipairs({ row.id, table.unpack(row.aliases or {}) }) do
      if #prefix > best_len and id:sub(1, #prefix) == prefix then
        best, best_len = row, #prefix
      end
    end
  end
  return best
end

-- {info} over the curated row it matches over `def.defaults`, then
-- `def.adjust`.
function M.describe(def, info)
  local out = table.clone(def.defaults or {})
  for key, value in pairs(lookup(def.models, info.id) or {}) do
    if key ~= "aliases" then
      out[key] = value
    end
  end
  for key, value in pairs(info) do
    out[key] = value
  end
  return def.adjust and def.adjust(out) or out
end

-- The curated rows and their aliases as descriptors, rows first so a row is
-- its tier's default before any alias.
function M.descriptors(def)
  local out = {}
  for _, row in ipairs(def.models or {}) do
    out[#out + 1] = M.describe(def, { id = row.id })
  end
  for _, row in ipairs(def.models or {}) do
    for _, alias in ipairs(row.aliases or {}) do
      out[#out + 1] = M.describe(def, { id = alias })
    end
  end
  return out
end

-- Discovered rows merged over the curated ones, which keep their place.
function M.merge(def, rows)
  local by_id, seen, out = {}, {}, {}
  for _, row in ipairs(rows) do
    by_id[row.id] = by_id[row.id] or row
  end
  for _, curated in ipairs(M.descriptors(def)) do
    out[#out + 1] = M.describe(def, by_id[curated.id] or curated)
    seen[curated.id] = true
  end
  for _, row in ipairs(rows) do
    if not seen[row.id] then
      out[#out + 1] = M.describe(def, row)
      seen[row.id] = true
    end
  end
  return out
end

local function price(value)
  return type(value) == "string" and tonumber(value)
end

-- The listing fields OpenAI-compatible servers commonly send.
function M.parse_model(row)
  if type(row.id) ~= "string" then
    return nil
  end
  local pricing
  local p = type(row.pricing) == "table" and row.pricing or {}
  local input, output = price(p.prompt), price(p.completion)
  if input and output and type(p.cache_creation) == "string" and type(p.cache_read) == "string" then
    pricing = {
      input = input,
      output = output,
      cache_write = price(p.cache_creation) or 0,
      cache_read = price(p.cache_read) or 0,
    }
  end
  local vision
  if type(row.input_modalities) == "table" then
    vision = table.find(row.input_modalities, "image") ~= nil
  end
  return {
    id = row.id,
    context_window = M.limit(row.context_length) or M.limit(row.max_model_len) or M.limit(row.max_context_length),
    max_output_tokens = M.limit(row.max_tokens) or M.limit(row.max_output_length),
    pricing = pricing,
    supports_vision = vision,
  }
end

-- Parses a listing's `data` rows with {parse}, sorted by id.
function M.parse_listing(body, parse)
  local models = {}
  for _, row in ipairs(type(body) == "table" and type(body.data) == "table" and body.data or {}) do
    local model = type(row) == "table" and parse(row)
    if model then
      models[#models + 1] = model
    end
  end
  table.sort(models, function(a, b)
    return a.id < b.id
  end)
  return models
end

-- Keys, base URL and headers for one provider, resolved from `providers.toml`
-- {config}. Nil plus an error without a key or with an unset header variable.
function M.client(ctx, def, config)
  local configured, header_err = http.headers(def.id, {}, config.headers)
  if not configured then
    return nil, header_err, true
  end
  local keys, err = http.api_keys(ctx, config, def.env)
  if not keys then
    return nil, err
  end
  local client = { base = M.base_url(def, config), pool = http.key_pool(keys) }
  -- {extra} plus the key, with configured headers replacing any of them.
  function client.headers(extra)
    local headers = table.clone(extra or {})
    local key = client.pool.current()
    if def.key_headers then
      for name, value in pairs(def.key_headers(key)) do
        headers[name] = value
      end
    else
      headers.Authorization = "Bearer " .. key
    end
    for name, value in pairs(configured) do
      http.set_header(headers, name, value)
    end
    return headers
  end
  function client.get_json(call_ctx, url, extra)
    local response, send_err = http.send(call_ctx, { url = url, headers = client.headers(extra) })
    if not response then
      return nil, send_err
    end
    return http.read_json(response)
  end
  return client
end

-- The `/models` listing of {def}'s server, parsed with `def.parse_model`.
function M.list_models(def)
  return function(ctx, client)
    local body, err = client.get_json(ctx, client.base .. M.MODELS_PATH)
    if err then
      return nil, err
    end
    return M.parse_listing(body, def.parse_model or M.parse_model)
  end
end

local function reclassify(def, err)
  if def.api_error and type(err) == "table" and err.status then
    return def.api_error(err)
  end
  return err
end

-- Streams {request} through {client} (`base` and `headers(extra)`) with
-- {def}'s request quirks applied for descriptor {model}.
function M.stream(def, client, model, ctx, request, emit)
  local body = chat.build_request(request, {
    max_tokens_field = def.max_tokens_field or DEFAULT_MAX_TOKENS_FIELD,
    include_stream_usage = def.include_stream_usage,
  })
  local extra = def.encode and def.encode(body, request, model) or {}
  extra["Content-Type"] = JSON_TYPE
  local response, err = http.send(ctx, {
    url = client.base .. CHAT_PATH,
    method = "POST",
    headers = client.headers(extra),
    body = assert(makima.json.encode(body)),
  })
  if not response then
    return nil, reclassify(def, err)
  end
  local result, stream_err = chat.stream(response, emit)
  response:close()
  if not result then
    return nil, reclassify(def, stream_err)
  end
  return result
end

local function default_model(def, config)
  local chosen = plan(def, config)
  return (chosen and chosen.model) or def.model
end

local function login(def)
  return function(ctx, ui, config)
    local chosen = plan(def, config)
    local url = (chosen and chosen.login_url) or def.login_url
    if url then
      local _, open_err = ui:open_browser(url)
      if open_err then
        return nil, open_err
      end
    end
    local prompt = def.display_name .. " " .. METHOD_LABEL .. (def.key_optional and KEY_SKIP_HINT or "")
    local key, err = ui:input(prompt, true)
    if not key then
      return nil, err
    end
    key = key:match("^%s*(.-)%s*$")
    if key ~= "" then
      local _, write_err = ctx.credentials:write({ api_key = key })
      if write_err then
        return nil, write_err
      end
    end
    return { model = default_model(def, config) }
  end
end

local function mask(key)
  if #key > 2 * MASK_EDGE then
    return key:sub(1, MASK_EDGE) .. "..." .. key:sub(-MASK_EDGE)
  end
  return "****"
end

-- The API key login method: opens the key page, asks for a key and saves
-- it unless empty.
function M.api_key_auth(def)
  return { { id = METHOD_ID, label = METHOD_LABEL, login = login(def) } }
end

-- Where the key comes from: `env`, `saved`, `configured` or `missing`.
function M.auth_status(def)
  return function(ctx, config)
    local keys, source = http.api_keys(ctx, config, def.env)
    if not keys then
      return { state = "missing" }
    end
    local details = {}
    if source == "env" then
      details[#details + 1] = "via " .. (config.api_key_env or def.env)
    elseif source == "saved" then
      details[#details + 1] = "key: " .. mask(keys[1])
    end
    if config.plan then
      details[#details + 1] = config.plan
    end
    return { state = source, detail = #details > 0 and table.concat(details, ", ") or nil }
  end
end

-- The providers reference page entry: the key and base URL variables, the
-- default base URL and `def.notes`.
function M.docs(def)
  return { env = { def.env, M.base_url_env(def) }, endpoint = def.base_url, notes = def.notes }
end

function M.provider(def)
  local spec = {
    id = def.id,
    display_name = def.display_name,
    models = M.descriptors(def),
    auth = M.api_key_auth(def),
    auth_status = M.auth_status(def),
    docs = M.docs(def),
  }
  if def.plans then
    local choices = {}
    for _, item in ipairs(def.plans) do
      choices[#choices + 1] = { id = item.id, label = item.label }
    end
    spec.config = { { key = PLAN_KEY, prompt = PLAN_PROMPT, choices = choices } }
  end

  local list = def.list_models or M.list_models(def)
  function spec.discover(ctx, config)
    local client, err, setup_error = M.client(ctx, def, config)
    if not client then
      if setup_error then
        return nil, err
      end
      return M.descriptors(def)
    end
    local rows, err = list(ctx, client)
    if not rows then
      return nil, err
    end
    return M.merge(def, rows)
  end

  function spec.prepare(ctx, info, config)
    local client, err = M.client(ctx, def, config)
    if not client then
      return nil, err
    end
    local model = M.describe(def, info)
    local instance = {
      key_count = client.pool.count,
      rotate_key = function()
        return client.pool.rotate()
      end,
      stream = function(call_ctx, request, emit)
        return M.stream(def, client, model, call_ctx, request, emit)
      end,
    }
    if def.fetch_usage then
      function instance.fetch_usage(call_ctx)
        return def.fetch_usage(call_ctx, client)
      end
    end
    return instance, model
  end

  return spec
end

return M
