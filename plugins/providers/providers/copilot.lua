-- GitHub Copilot: a GitHub token imported from the gh CLI, the Copilot
-- client or the system keyring, and per-model routing to Messages, Responses
-- or Chat Completions.

local anthropic = require("providers.anthropic")
local chat = require("providers.chat_completions")
local compat = require("providers.openai_compat")
local http = require("providers.http")
local responses = require("providers.responses")
local thinking = require("providers.thinking")

local M = {}

M.DEFAULT_ENDPOINT = "https://api.githubcopilot.com"
M.NOT_AUTHENTICATED = "not authenticated, run `makima auth login copilot` or set GH_COPILOT_TOKEN"
M.NOT_FOUND =
  "Copilot token not found. Run `gh auth login --web`, sign in with the Copilot client, or set GH_COPILOT_TOKEN."
M.ALREADY_AUTHENTICATED = "Already authenticated with Copilot."
M.KEYRING_LOCKED = "is unreadable; is the store locked?"

local DEFAULT_HOST = "github.com"
local TOKEN_ENVS = { "GH_COPILOT_TOKEN", "COPILOT_GITHUB_TOKEN" }
local GRAPHQL_QUERY = "query { viewer { copilotEndpoints { api } } }"
local API_VERSION = "2025-10-01"
local ANTHROPIC_VERSION = "2023-06-01"
local EDITOR = "Maki/"
local INTERACTION = "conversation-agent"
local INITIATOR = "agent"
local CHAT_PATH = "/chat/completions"
local RESPONSES_PATH = "/responses"
local MESSAGES_PATH = "/v1/messages"
local MODELS_PATH = "/models"
local CHAT_TYPE = "chat"
local ENABLED = "enabled"
local CLAUDE_PREFIX = "claude-"
local RESPONSES_MARKERS = { "gpt-5", "codex" }
local TEMPERATURE = 0.1
-- `/models` prices tokens in AI credits (1 credit = $0.01) per billing batch.
local AIC_TO_USD_PER_MILLION = 10000
local KEYRING_SERVICE_PREFIX = "gh:"
local GO_KEYRING_PREFIX = "go-keyring-base64:"
local GHE_SUFFIX = ".ghe.com"
local GITHUB_SUFFIX = ".github.com"
local TIERS = { lightweight = "weak", versatile = "medium", powerful = "strong" }
local METHOD = { id = "import", label = "Import GitHub token" }

local function pricing(input, output, cache_write, cache_read)
  return { input = input, output = output, cache_write = cache_write, cache_read = cache_read }
end

local function row(id, tier, price, max_output_tokens, aliases)
  return { id = id, tier = tier, pricing = price, max_output_tokens = max_output_tokens, aliases = aliases }
end

-- Fallback rates until `/models` reports its own, at the default context
-- tier. The first row of a tier is its default.
M.def = {
  id = "copilot",
  display_name = "Copilot",
  env = "GH_COPILOT_TOKEN",
  login_url = "https://github.com/settings/copilot",
  model = "gpt-5.6-terra",
  defaults = {
    context_window = 200000,
    max_output_tokens = 100000,
    supports_thinking = false,
    supports_vision = true,
    supports_tool_examples = false,
  },
  models = {
    row("gpt-5.6-luna", "weak", pricing(0.20, 1.20, 0.25, 0.02), 100000),
    row("gpt-5.6-terra", "medium", pricing(2.00, 12.00, 2.50, 0.20), 100000),
    row(
      "claude-opus-5",
      "strong",
      pricing(5.00, 25.00, 6.25, 0.50),
      64000,
      { "claude-opus-4.8", "claude-opus-4.7", "claude-opus-4.6", "claude-opus-4.5" }
    ),
    row("gpt-5-mini", "weak", pricing(0.25, 2.00, 0, 0.025), 100000),
    row("gpt-5.4-mini", "weak", pricing(0.75, 4.50, 0, 0.075), 100000),
    row("gpt-5.4-nano", "weak", pricing(0.20, 1.25, 0, 0.02), 100000),
    row("claude-haiku-4.5", "weak", pricing(1.00, 5.00, 1.25, 0.10), 64000),
    row("gemini-3.5-flash", "weak", pricing(1.50, 9.00, 0, 0.15), 65536),
    row("gemini-3.6-flash", "medium", pricing(0.75, 3.75, 0, 0.075), 65536),
    row("gemini-3.7-flash", "medium", pricing(0.75, 3.75, 0, 0.075), 65536),
    row("mai-code-1-flash-picker", "weak", pricing(0.75, 4.50, 0, 0.075), 100000),
    row("claude-sonnet-4.5", "medium", pricing(3.00, 15.00, 3.75, 0.30), 64000, { "claude-sonnet-4.6" }),
    row("claude-sonnet-5", "medium", pricing(2.00, 10.00, 2.50, 0.20), 100000),
    row("gpt-5.5", "strong", pricing(5.00, 30.00, 0, 0.50), 100000),
    row("kimi-k2.7-code", "medium", pricing(0.95, 4.00, 0, 0.19), 100000),
    row("kimi-k3", "strong", pricing(3.00, 15.00, 0, 0.30), 100000),
    row("gemini-3.1-pro-preview", "medium", pricing(2.00, 12.00, 0, 0.20), 65536),
    row("gpt-5.4", "strong", pricing(2.50, 15.00, 0, 0.25), 100000),
    row("gpt-5.6-sol", "strong", pricing(5.00, 30.00, 6.25, 0.50), 100000),
    row("gpt-5.3-codex", "strong", pricing(1.75, 14.00, 0, 0.175), 100000),
    row("claude-opus-4.8-fast", "strong", pricing(10.00, 50.00, 12.50, 1.00), 100000, { "claude-fable-5" }),
    row("grok-4.5", "medium", pricing(2.00, 6.00, 0, 0.50), 100000),
    row("grok-4.6", "medium", pricing(2.00, 6.00, 0, 0.50), 100000),
  },
}

local function trim(text)
  return text:match("^%s*(.-)%s*$")
end

local function push_unique(list, value)
  if not table.find(list, value) then
    list[#list + 1] = value
  end
end

local function sorted_keys(map)
  local keys = {}
  for key in pairs(type(map) == "table" and map or {}) do
    if type(key) == "string" then
      keys[#keys + 1] = key
    end
  end
  table.sort(keys)
  return keys
end

function M.github_host(host)
  return host == DEFAULT_HOST or host:sub(-#GHE_SUFFIX) == GHE_SUFFIX or host:sub(-#GITHUB_SUFFIX) == GITHUB_SUFFIX
end

function M.graphql_url(host)
  if host == DEFAULT_HOST then
    return "https://api.github.com/graphql"
  end
  return "https://" .. host .. "/api/graphql"
end

-- The token in use: `GH_COPILOT_TOKEN` or `COPILOT_GITHUB_TOKEN`, then the
-- saved one. Nil when there is none, or nil plus a read error.
function M.token(ctx)
  for _, name in ipairs(TOKEN_ENVS) do
    local token = trim(maki.uv.os_getenv(name) or "")
    if token ~= "" then
      return { token = token, host = DEFAULT_HOST, env = name }
    end
  end
  local saved, err = ctx.credentials:read()
  if err then
    return nil, err
  end
  if type(saved) == "table" and type(saved.api_key) == "string" then
    return { token = saved.api_key, host = type(saved.host) == "string" and saved.host or DEFAULT_HOST }
  end
end

-- The gh CLI writes keyring entries through go-keyring, which may store them
-- base64-encoded behind a prefix.
function M.decode_go_keyring(raw)
  if raw:sub(1, #GO_KEYRING_PREFIX) ~= GO_KEYRING_PREFIX then
    return raw
  end
  local ok, decoded = pcall(maki.base64.decode, raw:sub(#GO_KEYRING_PREFIX + 1))
  return ok and utf8.len(decoded) and decoded or raw
end

-- The Copilot client's `hosts.json` and `apps.json`, then the gh CLI's
-- `hosts.yml`, decoded. YAML is a superset of JSON, so one parser reads all.
function M.config_files()
  local base = maki.uv.os_getenv("XDG_CONFIG_HOME")
  if not base or base == "" then
    local home = maki.uv.os_homedir()
    if not home then
      return {}
    end
    base = maki.fs.joinpath(home, ".config")
  end
  local files = {}
  for _, path in ipairs({
    maki.fs.joinpath(base, "github-copilot", "hosts.json"),
    maki.fs.joinpath(base, "github-copilot", "apps.json"),
    maki.fs.joinpath(base, "gh", "hosts.yml"),
  }) do
    local contents = maki.fs.read(path)
    local value = contents and maki.yaml.decode(contents)
    if type(value) == "table" then
      files[#files + 1] = value
    end
  end
  return files
end

local function keyring_accounts(files, host)
  local accounts = {}
  for _, file in ipairs(files) do
    local entry = file[host]
    if type(entry) == "table" then
      if type(entry.user) == "string" then
        push_unique(accounts, entry.user)
      end
      for _, user in ipairs(sorted_keys(entry.users)) do
        push_unique(accounts, user)
      end
    end
  end
  push_unique(accounts, "")
  return accounts
end

-- A token from the config {files}, then from the gh entries in the system
-- keyring. Unreadable keyring entries add a line to {hints}.
function M.import(files, hints)
  for _, file in ipairs(files) do
    for _, host in ipairs(sorted_keys(file)) do
      local entry = file[host]
      if M.github_host(host) and type(entry) == "table" and type(entry.oauth_token) == "string" then
        return { token = entry.oauth_token, host = host }
      end
    end
  end
  local hosts = { DEFAULT_HOST }
  for _, file in ipairs(files) do
    for _, host in ipairs(sorted_keys(file)) do
      if M.github_host(host) then
        push_unique(hosts, host)
      end
    end
  end
  for _, host in ipairs(hosts) do
    for _, account in ipairs(keyring_accounts(files, host)) do
      local raw, err = maki.host.keyring_get(KEYRING_SERVICE_PREFIX .. host, account)
      if raw then
        return { token = M.decode_go_keyring(raw), host = host }
      elseif err then
        hints[#hints + 1] = string.format("gh keyring entry for %s/%s %s (%s)", host, account, M.KEYRING_LOCKED, err)
      end
    end
  end
end

local function login(ctx, ui)
  local current, err = M.token(ctx)
  if err then
    return nil, err
  end
  if current then
    return ui:instructions(M.ALREADY_AUTHENTICATED)
  end
  local hints = {}
  local found = M.import(M.config_files(), hints)
  if not found then
    hints[#hints + 1] = M.NOT_FOUND
    return nil, table.concat(hints, "\n")
  end
  local _, write_err = ctx.credentials:write({
    api_key = found.token,
    host = found.host ~= DEFAULT_HOST and found.host or nil,
  })
  if write_err then
    return nil, write_err
  end
  return ui:instructions("Copilot token imported from gh CLI / Copilot client / system keyring (" .. found.host .. ").")
end

local function auth_status(ctx)
  local auth, err = M.token(ctx)
  if err then
    return nil, err
  end
  if not auth then
    return { state = "missing" }
  elseif auth.env then
    return { state = "env", detail = "via " .. auth.env }
  end
  return { state = "saved", detail = auth.host ~= DEFAULT_HOST and auth.host or nil }
end

local function headers(state, auth, interaction, extra)
  local out = table.clone(extra or {})
  out.Authorization = "Bearer " .. auth.token
  out["Editor-Version"] = EDITOR .. maki.version().current
  out["X-GitHub-Api-Version"] = API_VERSION
  if interaction then
    out["X-Initiator"] = INITIATOR
    out["X-Interaction-Type"] = INTERACTION
    out["OpenAI-Intent"] = INTERACTION
  end
  for name, value in pairs(state.configured) do
    http.set_header(out, name, value)
  end
  return out
end

-- The account's Copilot API endpoint, from GitHub's GraphQL API, or the
-- public endpoint when that lookup fails.
local function discover_endpoint(ctx, auth)
  local response = http.send(ctx, {
    url = M.graphql_url(auth.host),
    method = "POST",
    headers = { Authorization = "Bearer " .. auth.token, ["Content-Type"] = "application/json" },
    body = assert(maki.json.encode({ query = GRAPHQL_QUERY })),
  })
  local body = response and http.read_json(response)
  local data = type(body) == "table" and type(body.data) == "table" and body.data or {}
  local viewer = type(data.viewer) == "table" and data.viewer or {}
  local endpoints = type(viewer.copilotEndpoints) == "table" and viewer.copilotEndpoints or {}
  return type(endpoints.api) == "string" and endpoints.api or M.DEFAULT_ENDPOINT
end

-- Token and endpoint, looked up once per instance until `reload_auth`.
local function connect(ctx, state)
  if state.auth then
    return state.auth
  end
  local auth, err = M.token(ctx)
  if not auth then
    return nil, err or http.config_error(M.NOT_AUTHENTICATED), err == nil
  end
  auth.endpoint = discover_endpoint(ctx, auth)
  state.auth = auth
  return auth
end

local function enabled_chat_model(entry)
  local capabilities = type(entry.capabilities) == "table" and entry.capabilities or {}
  local policy = entry.policy
  return type(entry.id) == "string"
    and entry.model_picker_enabled == true
    and capabilities.type == CHAT_TYPE
    and (type(policy) ~= "table" or policy.state == ENABLED)
end

-- The account's enabled chat models, the chat default first.
local function list(ctx, state, auth)
  local response, err = http.send(ctx, { url = auth.endpoint .. MODELS_PATH, headers = headers(state, auth) })
  if not response then
    return nil, err
  end
  local body, read_err = http.read_json(response)
  if read_err then
    return nil, read_err
  end
  local models = {}
  for _, entry in ipairs(type(body) == "table" and type(body.data) == "table" and body.data or {}) do
    if type(entry) == "table" and enabled_chat_model(entry) then
      table.insert(models, entry.is_chat_default == true and 1 or #models + 1, M.descriptor(entry))
    end
  end
  return models
end

local function route(endpoints)
  if table.find(endpoints, MESSAGES_PATH) then
    return "messages"
  elseif table.find(endpoints, RESPONSES_PATH) then
    return "responses"
  end
  return "chat"
end

local function discovered_pricing(entry)
  local billing = type(entry.billing) == "table" and entry.billing or {}
  local prices = type(billing.token_prices) == "table" and billing.token_prices or {}
  local default = prices.default
  if type(default) ~= "table" or type(prices.batch_size) ~= "number" or prices.batch_size == 0 then
    return nil
  end
  local scale = AIC_TO_USD_PER_MILLION / prices.batch_size
  local function price(value)
    return (type(value) == "number" and value or 0) * scale
  end
  -- `/models` has no cache-write rate, so it comes from the curated row.
  local curated = compat.describe(M.def, { id = entry.id }).pricing
  return {
    input = price(default.input_price),
    output = price(default.output_price),
    cache_read = price(default.cache_price),
    cache_write = curated and curated.cache_write or 0,
  }
end

-- One `/models` row as a descriptor. `provider_data` keeps the route and the
-- reasoning levels the model declares.
function M.descriptor(entry)
  local capabilities = type(entry.capabilities) == "table" and entry.capabilities or {}
  local limits = type(capabilities.limits) == "table" and capabilities.limits or {}
  local supports = type(capabilities.supports) == "table" and capabilities.supports or {}
  local declared = type(supports.reasoning_effort) == "table" and supports.reasoning_effort or {}
  local efforts, off = thinking.parse_levels(declared)
  local data = {
    route = route(type(entry.supported_endpoints) == "table" and entry.supported_endpoints or {}),
    efforts = efforts,
    off = off,
    adaptive_thinking = supports.adaptive_thinking == true,
  }
  -- The Chat Completions body carries no reasoning field, so thinking there
  -- would be a setting the request drops.
  local thinks = #declared > 0
    or data.adaptive_thinking
    or type(supports.max_thinking_budget) == "number"
    or type(supports.min_thinking_budget) == "number"
  return {
    id = entry.id,
    context_window = compat.limit(limits.max_context_window_tokens),
    max_output_tokens = compat.limit(limits.max_output_tokens),
    pricing = discovered_pricing(entry),
    supports_thinking = data.route ~= "chat" and thinks,
    supports_vision = supports.vision == true,
    tier = TIERS[entry.model_picker_category],
    provider_data = data,
  }
end

-- Where a model without discovered data goes, from its id.
function M.guess_route(id)
  if id:sub(1, #CLAUDE_PREFIX) == CLAUDE_PREFIX then
    return "messages"
  end
  for _, marker in ipairs(RESPONSES_MARKERS) do
    if id:find(marker, 1, true) then
      return "responses"
    end
  end
  return "chat"
end

-- The route data for {model}: discovered, else from this instance's own
-- listing, else guessed.
local function model_data(ctx, state, auth, model)
  if type(model.provider_data) == "table" and model.provider_data.route then
    return model.provider_data
  end
  if not state.listing then
    local models, err = list(ctx, state, auth)
    if not models then
      return nil, err
    end
    state.listing = {}
    for _, listed in ipairs(models) do
      state.listing[listed.id] = listed.provider_data
    end
  end
  return state.listing[model.id] or { route = M.guess_route(model.id) }
end

-- The Responses dialect a model declares; without declared levels it is
-- low to high with adaptive at high.
function M.dialect(data)
  local fallback = thinking.dialects.prefer_high
  return {
    supported = (data.efforts and #data.efforts > 0) and data.efforts or fallback.supported,
    adaptive = not data.adaptive_thinking and fallback.adaptive or nil,
    off = data.off and thinking.OFF or nil,
  }
end

local function stream(state, model, ctx, request, emit)
  local auth, err = connect(ctx, state)
  if not auth then
    return nil, err
  end
  local data, data_err = model_data(ctx, state, auth, model)
  if not data then
    return nil, data_err
  end
  request.fast = false
  if data.route == "messages" then
    local extra = { ["anthropic-version"] = ANTHROPIC_VERSION }
    return http.post_stream(
      ctx,
      auth.endpoint .. MESSAGES_PATH,
      headers(state, auth, true, extra),
      anthropic.build_request(request),
      anthropic.stream,
      emit
    )
  elseif data.route == "responses" then
    local reasoning = responses.reasoning(request.thinking, M.dialect(data), thinking.max_budget(request.model))
    local body = responses.build_request(request, reasoning)
    return http.post_stream(
      ctx,
      auth.endpoint .. RESPONSES_PATH,
      headers(state, auth, true),
      body,
      responses.stream,
      emit
    )
  end
  local body = chat.build_request(request, {})
  body.n = 1
  body.temperature = TEMPERATURE
  return http.post_stream(ctx, auth.endpoint .. CHAT_PATH, headers(state, auth, true), body, chat.stream, emit)
end

-- State for {client} (`base` and `headers()`), whose headers carry
-- credentials that come from elsewhere and whose base is the endpoint. The
-- model listing stays with the client.
local function delegated(client)
  client.copilot = client.copilot or {}
  local state = client.copilot
  state.configured = client.headers()
  state.auth = { token = "", endpoint = client.base }
  return state
end

function M.route_stream(client, model, ctx, request, emit)
  return stream(delegated(client), model, ctx, request, emit)
end

function M.route_list(ctx, client)
  local state = delegated(client)
  return list(ctx, state, state.auth)
end

local function new_state(config)
  local configured, err = http.headers(M.def.id, {}, config.headers)
  if not configured then
    return nil, err
  end
  return { configured = configured }
end

function M.provider()
  return {
    id = M.def.id,
    display_name = M.def.display_name,
    docs = {
      env = TOKEN_ENVS,
      endpoint = M.DEFAULT_ENDPOINT,
      notes = "`makima auth login copilot` imports a token from the gh CLI, the Copilot client or the system keyring. Each model streams through the API its endpoint names: Messages, Responses or Chat Completions.",
    },
    models = compat.descriptors(M.def),
    auth = { { id = METHOD.id, label = METHOD.label, login = login } },
    auth_status = auth_status,
    discover = function(ctx, config)
      local state, err = new_state(config)
      if not state then
        return nil, err
      end
      local auth, auth_err, missing = connect(ctx, state)
      if not auth then
        if not missing then
          return nil, auth_err
        end
        return compat.descriptors(M.def)
      end
      local models, list_err = list(ctx, state, auth)
      if not models then
        return nil, list_err
      end
      return compat.merge(M.def, models)
    end,
    prepare = function(ctx, info, config)
      local state, err = new_state(config)
      if not state then
        return nil, err
      end
      local auth, auth_err = M.token(ctx)
      if not auth then
        return nil, auth_err or http.config_error(M.NOT_AUTHENTICATED)
      end
      local model = compat.describe(M.def, info)
      return {
        stream = function(call_ctx, request, emit)
          return stream(state, model, call_ctx, request, emit)
        end,
        -- A new login may change the token, its host and the models it
        -- grants.
        reload_auth = function()
          state.auth, state.listing = nil, nil
        end,
      },
        model
    end,
  }
end

return M
