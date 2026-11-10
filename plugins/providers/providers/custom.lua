-- Providers that `providers.toml` defines: a section with a `protocol`
-- (`openai`, `openai-responses`, `anthropic` or `google`) and a `base_url`
-- (or `<ID>_BASE_URL`). The key comes from `api_key_env` (default
-- `<ID>_API_KEY`), the saved key or `api_key`, and is always sent as a bearer
-- token. Declared `models` win over discovered metadata field by field;
-- `discover_models = true` also lists the server's models.
-- `M.provider(id, section)` returns a `register_provider` spec, and
-- `M.creator(notes)` the spec whose login adds a section, with {notes}
-- after its own on the providers reference page.

local chat = require("providers.chat_completions")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local openai = require("providers.openai")
local responses = require("providers.responses")
local routes = require("providers.routes")
local thinking = require("providers.thinking")

local M = {}

M.CREATOR_ID = "custom"
M.EMPTY_NAME = "provider name cannot be empty"
M.EMPTY_URL = "base URL cannot be empty"

local CHAT_PATH = "/chat/completions"
local RESPONSES_PATH = "/responses"
local KEY_SUFFIX = "_API_KEY"
local DEFAULT_TIER = "medium"
local MODEL_FIELDS = {
  "context_window",
  "max_output_tokens",
  "supports_tool_examples",
  "supports_thinking",
  "requires_thinking",
  "supports_vision",
}
local PRICING_FIELDS = { "input", "output", "cache_write", "cache_read" }
local NOTES = [==[
Run `makima auth login custom` to add a provider of your own, or write its section in `providers.toml` (`~/.config/makima/providers.toml` on Linux and macOS, `%APPDATA%\makima\providers.toml` on Windows). A section with a `protocol` is a provider named after the section, used as the `provider/` part of its model specs:

```toml
[my-proxy]
display_name = "My Proxy"
protocol = "openai"            # openai | openai-responses | anthropic | google
base_url = "https://llm.example.com/v1"
api_key_env = "MY_PROXY_API_KEY"
discover_models = true         # also list the server's models

[[my-proxy.models]]
id = "fast-v1"
tier = "weak"
context_window = 128000
max_output_tokens = 16384
pricing_input = 0.5
pricing_output = 1.5
```

| Field | Notes |
|-------|-------|
| `display_name` | Shown in pickers and auth status |
| `protocol` | `openai`, `openai-responses`, `anthropic` or `google` |
| `base_url` | Origin of the API. `<ID>_BASE_URL` wins over it |
| `api_key_env` | Env var that holds the key, by default `<ID>_API_KEY` |
| `api_key` | Inline key (prefer the env var or `makima auth login <id>`) |
| `headers` | Extra headers sent with every request; values expand `${VAR}` |
| `discover_models` | Also list the server's models (default false) |
| `models` | Declared models, which win over discovered metadata field by field |

Model fields: `id` (required), `tier` (`weak`, `medium`, `strong` or `compaction`, default `medium`), `context_window`, `max_output_tokens`, `supports_tool_examples`, `supports_thinking`, `requires_thinking` (for APIs that refuse thinking off; raises it to minimal effort, compaction included), `supports_vision` (false turns off image input and `view_image`), and `pricing_input`, `pricing_output`, `pricing_cache_write`, `pricing_cache_read`, `pricing_fast_input`, `pricing_fast_output` in USD per 1M tokens.

A section never takes over a built-in provider's id. A section that fails to register is logged and left out, and a file that does not parse stops Makima at startup with exit code 2.]==]

local CHOICES = {
  { id = "openai", label = "OpenAI-compatible chat completions" },
  { id = "anthropic", label = "Anthropic messages API" },
  { id = "google", label = "Google Gemini API" },
}

local function chat_stream(client, _, ctx, request, emit)
  local body = chat.build_request(request, { max_tokens_field = "max_tokens", include_stream_usage = true })
  if request.thinking.kind == "off" then
    body.thinking = { type = "disabled" }
    body.reasoning_effort = thinking.OFF
  else
    body.reasoning_effort =
      thinking.effort(request.thinking, thinking.dialects.standard, thinking.max_budget(request.model))
  end
  return http.post_stream(ctx, client.base .. CHAT_PATH, client.headers(), body, chat.stream, emit)
end

local function responses_stream(client, _, ctx, request, emit)
  local reasoning =
    responses.reasoning(request.thinking, thinking.dialects.standard, thinking.max_budget(request.model))
  local body = responses.build_request(request, reasoning)
  return http.post_stream(ctx, client.base .. RESPONSES_PATH, client.headers(), body, responses.stream, emit)
end

local OPENAI = {
  base_url = openai.def.base_url,
  defaults = { context_window = 200000, max_output_tokens = 100000 },
  stream = chat_stream,
  list_models = compat.list_models({}),
}

-- Each protocol's default base URL, limits for models nothing describes,
-- stream and listing.
M.PROTOCOLS = {
  openai = OPENAI,
  ["openai-responses"] = table.clone(OPENAI),
  anthropic = {
    base_url = routes.ROUTES.anthropic.def.base_url,
    defaults = { context_window = 200000, max_output_tokens = 128000 },
    stream = routes.ROUTES.anthropic.stream,
    list_models = routes.ROUTES.anthropic.list_models,
  },
  google = {
    base_url = routes.ROUTES.google.def.base_url,
    defaults = { context_window = 1000000, max_output_tokens = 65536 },
    stream = routes.ROUTES.google.stream,
    list_models = routes.ROUTES.google.list_models,
  },
}
M.PROTOCOLS["openai-responses"].stream = responses_stream

-- "My Cool Provider" -> "my-cool-provider".
function M.slugify(name)
  local words = {}
  for word in name:lower():gmatch("%w+") do
    words[#words + 1] = word
  end
  return table.concat(words, "-")
end

-- `<ID>_API_KEY`.
function M.key_env(id)
  return (id:upper():gsub("-", "_")) .. KEY_SUFFIX
end

-- A declared `[[<id>.models]]` entry as a descriptor.
function M.declared(entry)
  local model = { id = entry.id, tier = entry.tier or DEFAULT_TIER }
  for _, key in ipairs(MODEL_FIELDS) do
    model[key] = entry[key]
  end
  local priced = false
  for _, key in ipairs(PRICING_FIELDS) do
    priced = priced or entry["pricing_" .. key] ~= nil
  end
  if priced then
    model.pricing = {
      input = entry.pricing_input or 0,
      output = entry.pricing_output or 0,
      cache_write = entry.pricing_cache_write or 0,
      cache_read = entry.pricing_cache_read or 0,
    }
    if entry.pricing_fast_input ~= nil or entry.pricing_fast_output ~= nil then
      model.pricing.fast = { input = entry.pricing_fast_input or 0, output = entry.pricing_fast_output or 0 }
    end
  end
  return model
end

local function declared_models(section)
  local out = {}
  for _, entry in ipairs(section.models or {}) do
    out[#out + 1] = M.declared(entry)
  end
  return out
end

-- {row} with the declared descriptor of the same id over it.
local function over(row, declared)
  local out = table.clone(row)
  for _, model in ipairs(declared) do
    if model.id == row.id then
      for key, value in pairs(model) do
        out[key] = value
      end
    end
  end
  return out
end

-- The declared models with discovered {rows} under them, then the rows
-- nothing declares.
function M.merge(declared, rows)
  local out, seen = {}, {}
  for _, model in ipairs(declared) do
    local discovered
    for _, row in ipairs(rows) do
      discovered = discovered or (row.id == model.id and row)
    end
    out[#out + 1] = over(discovered or model, declared)
    seen[model.id] = true
  end
  for _, row in ipairs(rows) do
    if not seen[row.id] then
      out[#out + 1] = row
      seen[row.id] = true
    end
  end
  return out
end

-- {info} with the declared fields over it and the protocol's limits under
-- it. Thinking counts as supported unless something says otherwise.
function M.describe(protocol, section, info)
  local out = table.clone(protocol.defaults)
  out.supports_thinking = true
  for key, value in pairs(over(info, declared_models(section))) do
    out[key] = value
  end
  return out
end

local function client(ctx, id, protocol, section)
  return compat.client(ctx, { id = id, env = M.key_env(id), base_url = protocol.base_url }, section)
end

function M.provider(id, section)
  local protocol = assert(M.PROTOCOLS[section.protocol], "unknown protocol")
  local display_name = section.display_name or id
  local def = { id = id, display_name = display_name, env = M.key_env(id) }
  local spec = {
    id = id,
    display_name = display_name,
    models = declared_models(section),
    auth = compat.api_key_auth(def),
    auth_status = compat.auth_status(def),
  }

  function spec.discover(ctx, config)
    local declared = declared_models(config)
    local api = config.discover_models and client(ctx, id, protocol, config)
    if not api then
      return declared
    end
    local rows, err = protocol.list_models(ctx, api)
    if not rows then
      return nil, err
    end
    return M.merge(declared, rows)
  end

  function spec.prepare(ctx, info, config)
    local api, err = client(ctx, id, protocol, config)
    if not api then
      return nil, err
    end
    local model = M.describe(protocol, config, info)
    return {
      stream = function(call_ctx, request, emit)
        return protocol.stream(api, model, call_ctx, request, emit)
      end,
    },
      model
  end

  return spec
end

-- Asks for a name, protocol and base URL and saves them as a new section.
-- The provider is registered at the next start, so its key is set then.
local function create(ui)
  local name, err = ui:input("Provider name")
  if not name then
    return nil, err
  end
  local id = M.slugify(name)
  if id == "" then
    return nil, http.config_error(M.EMPTY_NAME)
  end
  local protocol, choice_err = ui:choice("Protocol", CHOICES)
  if not protocol then
    return nil, choice_err
  end
  local base_url, url_err = ui:input("Base URL")
  if not base_url then
    return nil, url_err
  end
  base_url = base_url:match("^%s*(.-)%s*$")
  if base_url == "" then
    return nil, http.config_error(M.EMPTY_URL)
  end
  local env = M.key_env(id)
  local _, save_err = makima.api.save_provider_config(id, {
    display_name = "Custom (" .. id .. ")",
    protocol = protocol,
    base_url = base_url,
    api_key_env = env,
    discover_models = true,
  })
  if save_err then
    return nil, save_err
  end
  ui:instructions(
    string.format(
      "Added %s. Restart makima, then set %s or run `makima auth login %s`; use it as %s/<model>.",
      id,
      env,
      id,
      id
    )
  )
  return id
end

function M.creator(notes)
  local created = false
  return {
    id = M.CREATOR_ID,
    display_name = "Custom provider...",
    docs = { notes = NOTES .. "\n\n" .. notes },
    auth = {
      {
        id = "create",
        label = "Create",
        login = function(_, ui)
          local id, err = create(ui)
          if not id then
            return nil, err
          end
          created = true
          return true
        end,
      },
    },
    auth_status = function()
      return { state = created and "configured" or "missing" }
    end,
    prepare = function()
      return nil, http.config_error("`custom` only adds providers; run `makima auth login custom`")
    end,
  }
end

return M
