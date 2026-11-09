-- The Anthropic provider: the Messages API with API keys, or Bedrock when
-- `CLAUDE_CODE_USE_BEDROCK=1`. Bedrock is a mode of this provider, not a
-- provider of its own.

local bedrock = require("providers.bedrock")
local clock = require("providers.clock")
local codec = require("providers.anthropic")
local compat = require("providers.openai_compat")
local http = require("providers.http")

local M = {}

M.LONG_CONTEXT_WINDOW = 1000000

M.API_VERSION = "2023-06-01"
local MESSAGES_PATH = "/v1/messages"
local MODELS_PATH = "/v1/models?limit=1000"
local USAGE_PATH = "/api/oauth/usage"
local OAUTH_BETA = "oauth-2025-04-20"
local JSON_TYPE = "application/json"
local AUTHORIZATION = "authorization"
local LONG_CONTEXT_SUFFIX = "-1m"
local FIVE_HOURS = 5
local MONEY_EXPONENT = 2
local USD = "USD"

local function pricing(input, output, cache_write, cache_read, fast)
  return { input = input, output = output, cache_write = cache_write, cache_read = cache_read, fast = fast }
end

local SONNET = pricing(3.00, 15.00, 3.75, 0.30)
local OPUS = pricing(5.00, 25.00, 6.25, 0.50)
local FAST_OPUS = pricing(5.00, 25.00, 6.25, 0.50, { input = 10.00, output = 50.00 })

local NOTES = [[
Add `-1m` to any Claude model, like `claude-sonnet-4-6-1m`, to use the 1M token context window.

#### Amazon Bedrock

If you already use Claude through AWS Bedrock, set `CLAUDE_CODE_USE_BEDROCK=1` and every Anthropic request goes through Bedrock instead, with the same models and features.

You need `AWS_REGION` and one of these for auth:

| Method | Env vars |
|--------|----------|
| IAM credentials | `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_SESSION_TOKEN`) |
| Credentials file | `AWS_PROFILE` (defaults to `default`), reads `~/.aws/credentials` |
| Bearer token | `AWS_BEARER_TOKEN_BEDROCK` |
| Container credentials | `AWS_CONTAINER_CREDENTIALS_FULL_URI` |
| Gateway proxy | `CLAUDE_CODE_SKIP_BEDROCK_AUTH=1` + `ANTHROPIC_BEDROCK_BASE_URL` (skips signing, for a proxy that handles auth) |

`ANTHROPIC_MODEL` overrides the model and `ANTHROPIC_BEDROCK_BASE_URL` the endpoint. These names match Claude Code, so a Bedrock setup from there works here as is.]]

-- The first row of a tier is its default.
M.def = {
  id = "anthropic",
  display_name = "Anthropic",
  env = "ANTHROPIC_API_KEY",
  base_url = "https://api.anthropic.com",
  login_url = "https://console.anthropic.com/settings/keys",
  model = "claude-sonnet-4-6",
  notes = NOTES,
  defaults = {
    context_window = 200000,
    max_output_tokens = 128000,
    supports_thinking = true,
    supports_vision = true,
    supports_tool_examples = true,
    supports_fast = false,
  },
  models = {
    { id = "claude-haiku-4-5", tier = "weak", max_output_tokens = 64000, pricing = pricing(1.00, 5.00, 1.25, 0.10) },
    -- Introductory rates until 2026-09-01, then 3.00 / 15.00 / 3.75 / 0.30.
    { id = "claude-sonnet-5", tier = "medium", pricing = pricing(2.00, 10.00, 2.50, 0.20) },
    { id = "claude-sonnet-4-6", tier = "medium", max_output_tokens = 64000, pricing = SONNET },
    { id = "claude-sonnet-4-5", tier = "medium", max_output_tokens = 64000, pricing = SONNET },
    { id = "claude-sonnet-4", tier = "medium", max_output_tokens = 64000, pricing = SONNET },
    { id = "claude-opus-5", tier = "strong", supports_fast = true, pricing = FAST_OPUS },
    { id = "claude-opus-4-8", tier = "strong", supports_fast = true, pricing = FAST_OPUS },
    -- Fast mode withdrawn on 2026-07-24.
    { id = "claude-opus-4-7", tier = "strong", pricing = OPUS },
    -- Fast mode withdrawn on 2026-06-29.
    { id = "claude-opus-4-6", tier = "strong", pricing = OPUS },
    { id = "claude-opus-4-5", tier = "strong", max_output_tokens = 64000, pricing = OPUS },
    { id = "claude-fable-5", tier = "strong", pricing = pricing(10.00, 50.00, 12.50, 1.00) },
    {
      id = "claude-opus-4-0",
      aliases = { "claude-opus-4-1" },
      tier = "strong",
      max_output_tokens = 32000,
      pricing = pricing(15.00, 75.00, 18.75, 1.50),
    },
  },
  -- A `-1m` id is its base row with the long context window.
  adjust = function(model)
    if select(2, codec.strip_long_context(model.id)) then
      model.context_window = M.LONG_CONTEXT_WINDOW
    end
    return model
  end,
}

-- The origin of a base URL, which may still carry `/v1/messages` or a query.
function M.origin(base_url)
  local origin = base_url:match("^[^?#]*"):gsub("/+$", "")
  if origin:sub(-#MESSAGES_PATH) == MESSAGES_PATH then
    return origin:sub(1, -#MESSAGES_PATH - 1)
  end
  return origin
end

-- Keys, origin and headers from `providers.toml` {config}, or nil plus an
-- error.
local function client(ctx, config)
  local keys, err = http.api_keys(ctx, config, M.def.env)
  if not keys then
    return nil, err
  end
  local configured, header_err = http.headers(M.def.id, {}, config.headers)
  if not configured then
    return nil, header_err
  end
  local api = { base = M.origin(compat.base_url(M.def, config)), pool = http.key_pool(keys) }
  -- {extra} plus the key, with configured headers replacing any of them.
  function api.headers(extra)
    local headers = table.clone(extra or {})
    headers["x-api-key"] = api.pool.current()
    for name, value in pairs(configured) do
      http.set_header(headers, name, value)
    end
    return headers
  end
  function api.get_json(call_ctx, url, extra)
    local headers = table.clone(extra or {})
    headers["anthropic-version"] = M.API_VERSION
    local response, send_err = http.send(call_ctx, { url = url, headers = api.headers(headers) })
    if not response then
      return nil, send_err
    end
    return http.read_json(response)
  end
  return api
end

-- Every listed model, plus a `-1m` row for those whose input window reaches
-- the long context. {api} has `base` and `get_json(ctx, url, extra)`.
function M.list_models(ctx, api)
  local rows, after = {}, nil
  repeat
    local url = api.base .. MODELS_PATH .. (after and "&after_id=" .. after or "")
    local page, err = api.get_json(ctx, url)
    if err then
      return nil, err
    end
    page = type(page) == "table" and page or {}
    for _, model in ipairs(type(page.data) == "table" and page.data or {}) do
      if type(model) == "table" and type(model.id) == "string" then
        if type(model.max_input_tokens) == "number" and model.max_input_tokens >= M.LONG_CONTEXT_WINDOW then
          rows[#rows + 1] = { id = model.id .. LONG_CONTEXT_SUFFIX }
        end
        rows[#rows + 1] = { id = model.id }
      end
    end
    after = type(page.last_id) == "string" and page.last_id or nil
  until not page.has_more or not after
  table.sort(rows, function(a, b)
    return a.id < b.id
  end)
  return rows
end

local function percent(value)
  return type(value) == "number" and math.max(0, math.floor(value + 0.5)) or nil
end

local function spent(minor_units, exponent, currency)
  local amount = minor_units / 10 ^ (type(exponent) == "number" and exponent or MONEY_EXPONENT)
  if currency == nil or currency == USD then
    return string.format("$%.2f spent", amount)
  end
  return string.format("%.2f %s spent", amount, currency)
end

local function object(value)
  return type(value) == "table" and value or nil
end

local function window(limit)
  if limit.kind == "session" then
    return { Hours = FIVE_HOURS }
  elseif limit.kind == "weekly_all" then
    return { Weekly = {} }
  elseif limit.kind == "weekly_scoped" then
    local model = object(object(limit.scope) and limit.scope.model)
    local name = model and model.display_name
    return { Weekly = { model = type(name) == "string" and name or nil } }
  end
  return { Other = limit.kind }
end

local function credits(body)
  local spend, extra = object(body.spend), object(body.extra_usage)
  local percentage, detail
  if spend and spend.enabled == true then
    local used = object(spend.used)
    percentage = percent(spend.percent) or 0
    detail = used and type(used.amount_minor) == "number" and spent(used.amount_minor, used.exponent, used.currency)
  elseif not spend and extra and extra.is_enabled == true and percent(extra.utilization) then
    percentage = percent(extra.utilization)
    detail = type(extra.used_credits) == "number" and spent(extra.used_credits, extra.decimal_places, extra.currency)
  else
    return nil
  end
  return { kind = "Credits", percentage = percentage, detail = detail or nil }
end

-- The OAuth usage response: its `limits` rows, or the older per-window
-- fields without them, then the extra-usage credits.
function M.parse_usage(body)
  body = object(body) or {}
  local limits = {}
  for _, limit in ipairs(object(body.limits) or {}) do
    if object(limit) and type(limit.kind) == "string" and percent(limit.percent) then
      limits[#limits + 1] = {
        kind = window(limit),
        percentage = percent(limit.percent),
        reset_at = clock.parse_rfc3339(limit.resets_at),
      }
    end
  end
  if #limits == 0 then
    for _, lane in ipairs({
      { { Hours = FIVE_HOURS }, body.five_hour },
      { { Weekly = {} }, body.seven_day },
      { { Weekly = { model = "Sonnet" } }, body.seven_day_sonnet },
      { { Weekly = { model = "Opus" } }, body.seven_day_opus },
    }) do
      local usage = object(lane[2])
      if usage and percent(usage.utilization) then
        limits[#limits + 1] =
          { kind = lane[1], percentage = percent(usage.utilization), reset_at = clock.parse_rfc3339(usage.resets_at) }
      end
    end
  end
  limits[#limits + 1] = credits(body)
  return { limits = limits }
end

local function authorized(headers)
  for name in pairs(headers) do
    if name:lower() == AUTHORIZATION then
      return true
    end
  end
  return false
end

-- Subscription quota for {api}. It exists only for OAuth tokens, which
-- arrive as an `Authorization` header; API keys have none.
function M.fetch_usage(ctx, api)
  if not authorized(api.headers()) then
    return nil
  end
  local body, err = api.get_json(ctx, api.base .. USAGE_PATH, { ["anthropic-beta"] = OAUTH_BETA })
  if err then
    return nil, err
  end
  return M.parse_usage(body)
end

-- Streams {request} to the Messages API at {api} (`base` origin and
-- `headers(extra)`) for descriptor {model}. {system_prefix} goes before the
-- system prompt as its own block.
function M.stream(api, model, ctx, request, emit, system_prefix)
  -- A stale fast flag must never bill an ineligible model at the fast rate.
  request.fast = request.fast and model.supports_fast == true
  local response, err = http.send(ctx, {
    url = api.base .. MESSAGES_PATH,
    method = "POST",
    headers = api.headers({
      ["Content-Type"] = JSON_TYPE,
      ["anthropic-beta"] = codec.beta_header(request),
      ["anthropic-version"] = M.API_VERSION,
    }),
    body = assert(maki.json.encode(codec.build_request(request, system_prefix))),
  })
  if not response then
    return nil, err
  end
  local result, stream_err = codec.stream(response, emit)
  response:close()
  return result, stream_err
end

local function api_instance(ctx, model, config)
  local api, err = client(ctx, config)
  if not api then
    return nil, err
  end
  return {
    key_count = function()
      return api.pool.count
    end,
    rotate_key = function()
      return api.pool.rotate()
    end,
    reload_auth = function(call_ctx)
      local keys, key_err = http.api_keys(call_ctx, config, M.def.env)
      if not keys then
        return nil, key_err
      end
      api.pool = http.key_pool(keys)
    end,
    stream = function(call_ctx, request, emit)
      return M.stream(api, model, call_ctx, request, emit)
    end,
    fetch_usage = function(call_ctx)
      return M.fetch_usage(call_ctx, api)
    end,
  }
end

function M.provider()
  local spec = compat.provider(M.def)
  local key_status = spec.auth_status

  function spec.auth_status(ctx, config)
    if bedrock.enabled() then
      return bedrock.auth_status(ctx)
    end
    return key_status(ctx, config)
  end

  function spec.discover(ctx, config)
    local api = not bedrock.enabled() and client(ctx, config)
    if not api then
      return compat.descriptors(M.def)
    end
    local rows, err = M.list_models(ctx, api)
    if not rows then
      return nil, err
    end
    return compat.merge(M.def, rows)
  end

  function spec.prepare(ctx, info, config)
    local model = compat.describe(M.def, info)
    local instance, err
    if bedrock.enabled() then
      instance, err = bedrock.instance(ctx)
    else
      instance, err = api_instance(ctx, model, config)
    end
    if not instance then
      return nil, err
    end
    return instance, model
  end

  return spec
end

return M
