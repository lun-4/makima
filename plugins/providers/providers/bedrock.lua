-- Bedrock, the mode of the Anthropic provider that `CLAUDE_CODE_USE_BEDROCK=1`
-- turns on: AWS credentials, SigV4 over the exact body sent, and the
-- EventStream response. The env var names match Claude Code.

local anthropic = require("providers.anthropic")
local clock = require("providers.clock")
local eventstream = require("providers.bedrock_eventstream")
local http = require("providers.http")
local sigv4 = require("providers.sigv4")

local M = {}

M.MISSING_REGION = "AWS_REGION must be set when using Bedrock"
M.NO_CREDENTIALS =
  "no AWS credentials found: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, AWS_PROFILE, AWS_CONTAINER_CREDENTIALS_FULL_URI, or AWS_BEARER_TOKEN_BEDROCK"

local API_VERSION = "bedrock-2023-05-31"
local TOOL_EXAMPLES_BETA = "tool-examples-2025-10-29"
local SERVICE = "bedrock"
local JSON_TYPE = "application/json"
local ENABLED = "1"
local DEFAULT_PROFILE = "default"
local CREDENTIALS_FILE = "/.aws/credentials"
local CONTAINER_TIMEOUT_SECS = 5
local REFRESH_MARGIN_SECS = 300
local MILLIS = 1000
local SECONDS_PER_DAY = 86400
local STATUS_DETAIL = "via Bedrock"

local function env(name)
  return maki.uv.os_getenv(name)
end

local function trim(text)
  return text:match("^%s*(.-)%s*$")
end

function M.enabled()
  return env("CLAUDE_CODE_USE_BEDROCK") == ENABLED
end

-- The keys of {profile} in an `~/.aws/credentials` file, or nil.
function M.parse_credentials_file(content, profile)
  local section = "[" .. profile .. "]"
  local inside, found = false, {}
  for line in content:gmatch("[^\n]+") do
    line = trim(line)
    if line:sub(1, 1) == "[" then
      inside = line == section
    elseif inside then
      local key, value = line:match("^([^=]*)=(.*)$")
      if key then
        found[trim(key)] = trim(value)
      end
    end
  end
  if not found.aws_access_key_id or not found.aws_secret_access_key then
    return nil
  end
  return {
    access_key = found.aws_access_key_id,
    secret_key = found.aws_secret_access_key,
    session_token = found.aws_session_token,
  }
end

-- A container credentials endpoint response, with `expires_at` in epoch
-- seconds when it names an expiry.
function M.parse_container_credentials(body)
  body = type(body) == "table" and body or {}
  if type(body.AccessKeyId) ~= "string" then
    return nil, http.config_error("container creds response missing AccessKeyId")
  end
  if type(body.SecretAccessKey) ~= "string" then
    return nil, http.config_error("container creds response missing SecretAccessKey")
  end
  local expires = clock.parse_rfc3339(body.Expiration)
  return {
    access_key = body.AccessKeyId,
    secret_key = body.SecretAccessKey,
    session_token = type(body.Token) == "string" and body.Token or nil,
    expires_at = expires and math.floor(expires / MILLIS),
  }
end

-- The token file rotates, so it is read on every fetch.
local function container_credentials(ctx, url)
  local token = env("AWS_CONTAINER_AUTHORIZATION_TOKEN")
  local token_file = env("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
  if token_file then
    local content, err = maki.fs.read(token_file)
    if not content then
      return nil, http.config_error(string.format("read container auth token file %s: %s", token_file, tostring(err)))
    end
    token = content
  end
  local response, err = ctx.send({
    url = url,
    headers = token and { Authorization = trim(token) } or {},
    connect_timeout = CONTAINER_TIMEOUT_SECS,
    low_speed_timeout = CONTAINER_TIMEOUT_SECS,
    idle_timeout = CONTAINER_TIMEOUT_SECS,
  })
  if not response then
    return nil, http.config_error("container creds request: " .. tostring(err))
  end
  if response.status ~= 200 then
    local failure = http.error(response)
    return nil,
      http.config_error(string.format("container creds endpoint returned %d: %s", failure.status, failure.message))
  end
  local body, read_err = http.read_json(response)
  if read_err then
    return nil, read_err
  end
  return M.parse_container_credentials(body)
end

-- Credentials in Claude Code's order: bearer token, skipped auth for a
-- gateway that signs itself, env keys, the container endpoint, then the
-- shared credentials file. {offline} only notes the container endpoint
-- instead of calling it. Returns `{ region, method, bearer?, access_key?,
-- secret_key?, session_token?, expires_at? }` or nil plus an error.
function M.resolve(ctx, offline)
  local region = env("AWS_REGION")
  if not region then
    return nil, http.config_error(M.MISSING_REGION)
  end
  local credentials, err
  local bearer = env("AWS_BEARER_TOKEN_BEDROCK")
  local access_key, secret_key = env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY")
  local container = env("AWS_CONTAINER_CREDENTIALS_FULL_URI")
  if bearer then
    credentials = { method = "bearer", bearer = bearer }
  elseif env("CLAUDE_CODE_SKIP_BEDROCK_AUTH") == ENABLED then
    credentials = { method = "gateway" }
  elseif access_key and secret_key then
    credentials = {
      method = "env",
      access_key = access_key,
      secret_key = secret_key,
      session_token = env("AWS_SESSION_TOKEN"),
    }
  elseif container and offline then
    credentials = { method = "container" }
  elseif container then
    credentials, err = container_credentials(ctx, container)
    if not credentials then
      return nil, err
    end
    credentials.method = "container"
  else
    local home = env("HOME")
    local content = home and maki.fs.read(home .. CREDENTIALS_FILE)
    credentials = content and M.parse_credentials_file(content, env("AWS_PROFILE") or DEFAULT_PROFILE)
    if not credentials then
      return nil, http.config_error(M.NO_CREDENTIALS)
    end
    credentials.method = "profile"
  end
  credentials.region = region
  return credentials
end

function M.auth_status(ctx)
  local credentials, err = M.resolve(ctx, true)
  if not credentials then
    return { state = "missing", detail = http.message(err) }
  end
  return { state = "env", detail = STATUS_DETAIL .. " (" .. credentials.method .. ")" }
end

local function has_examples(tools)
  for _, tool in ipairs(tools or {}) do
    if tool.input_examples ~= nil then
      return true
    end
  end
  return false
end

-- The invoke URL and body for {request}. `ANTHROPIC_MODEL` replaces the model
-- id sent, and `ANTHROPIC_BEDROCK_BASE_URL` the regional endpoint. Fast mode
-- exists only on the direct API, so `speed` is never sent.
function M.build_request(request, region)
  local model_id, long_context = anthropic.strip_long_context(env("ANTHROPIC_MODEL") or request.model.id)
  local body = anthropic.build_body(request)
  body.anthropic_version = API_VERSION
  local betas = {}
  if has_examples(request.tools) then
    betas[#betas + 1] = TOOL_EXAMPLES_BETA
  end
  if long_context then
    betas[#betas + 1] = anthropic.LONG_CONTEXT_BETA
  end
  if #betas > 0 then
    body.anthropic_beta = betas
  end
  local base = env("ANTHROPIC_BEDROCK_BASE_URL") or string.format("https://bedrock-runtime.%s.amazonaws.com", region)
  return base .. "/model/" .. http.urlencode(model_id) .. "/invoke-with-response-stream", body
end

-- `YYYYMMDDTHHMMSSZ` for {secs}.
function M.amz_timestamp(secs)
  local of_day = secs % SECONDS_PER_DAY
  return string.format(
    "%sT%02d%02d%02dZ",
    (clock.utc_date(secs):gsub("-", "")),
    math.floor(of_day / 3600),
    math.floor(of_day % 3600 / 60),
    of_day % 60
  )
end

-- The headers for one POST of {body} to {url}, signed when the credentials
-- are keys.
function M.headers(credentials, url, body)
  local host = url:match("^https?://([^/?]+)")
  local headers = { ["Content-Type"] = JSON_TYPE, Host = host }
  if credentials.bearer then
    headers.Authorization = "Bearer " .. credentials.bearer
  elseif credentials.access_key then
    local signed = sigv4.sign({
      method = "POST",
      url = url,
      headers = { { "content-type", JSON_TYPE }, { "host", host } },
      body = body,
      access_key = credentials.access_key,
      secret_key = credentials.secret_key,
      session_token = credentials.session_token,
      region = credentials.region,
      service = SERVICE,
      timestamp = M.amz_timestamp(maki.time.now().secs),
    })
    for _, header in ipairs(signed) do
      headers[header[1]] = header[2]
    end
  end
  return headers
end

-- A provider instance. Credentials with an expiry are resolved again shortly
-- before it.
function M.instance(ctx)
  local credentials, err = M.resolve(ctx)
  if not credentials then
    return nil, err
  end
  local instance = {}

  function instance.reload_auth(call_ctx)
    local fresh, reload_err = M.resolve(call_ctx)
    if not fresh then
      return nil, reload_err
    end
    credentials = fresh
  end

  function instance.stream(call_ctx, request, emit)
    if credentials.expires_at and maki.time.now().secs + REFRESH_MARGIN_SECS >= credentials.expires_at then
      local _, refresh_err = instance.reload_auth(call_ctx)
      if refresh_err then
        return nil, refresh_err
      end
    end
    local url, body = M.build_request(request, credentials.region)
    local encoded = assert(maki.json.encode(body))
    local response, send_err = http.send(call_ctx, {
      url = url,
      method = "POST",
      headers = M.headers(credentials, url, encoded),
      body = encoded,
    })
    if not response then
      return nil, send_err
    end
    local result, stream_err = eventstream.stream(response, emit)
    response:close()
    return result, stream_err
  end

  return instance
end

return M
