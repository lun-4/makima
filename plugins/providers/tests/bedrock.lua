local bedrock = require("providers.bedrock")
local http = require("providers.http")
local sigv4 = require("providers.sigv4")
local t = require("makima.test_helpers")

local REGION = "us-east-1"
local HOST = "bedrock-runtime.us-east-1.amazonaws.com"
local BEDROCK_MODEL = "anthropic.claude-3-5-sonnet-20241022-v2:0"
local MODEL_URL = "https://" .. HOST .. "/model/anthropic.claude-3-5-sonnet-20241022-v2%3A0/invoke-with-response-stream"
local CONTAINER_URL = "http://169.254.170.23/v1/credentials"
local NOW = 1704067200
local NOW_STAMP = "20240101T000000Z"
local EXPIRY = "2024-01-01T00:04:00Z"
local LATER_EXPIRY = "2024-01-01T06:00:00Z"

local CREDENTIALS_FILE = [[
[default]
aws_access_key_id = AKID
aws_secret_access_key = SECRET

[myprofile]
aws_access_key_id = MYKEY
aws_secret_access_key = MYSECRET
aws_session_token = MYTOKEN
]]

local function uint(value, bytes)
  local out = {}
  for i = bytes, 1, -1 do
    out[i] = string.char(value % 256)
    value = math.floor(value / 256)
  end
  return table.concat(out)
end

local function chunk(event)
  local payload = assert(makima.json.encode({ bytes = makima.base64.encode(assert(makima.json.encode(event))) }))
  local name, value = ":event-type", "chunk"
  local headers = string.char(#name) .. name .. "\7" .. uint(#value, 2) .. value
  local total = 12 + #headers + #payload + 4
  return uint(total, 4) .. uint(#headers, 4) .. uint(0, 4) .. headers .. payload .. uint(0, 4)
end

local function eventstream_text(text)
  return t.response({
    chunk({ type = "content_block_start", index = 0, content_block = { type = "text" } })
      .. chunk({ type = "content_block_delta", index = 0, delta = { type = "text_delta", text = text } })
      .. chunk({ type = "message_stop" }),
  })
end

local function json(value)
  return t.response({ assert(makima.json.encode(value)) })
end

local function request(id, tools)
  return {
    model = {
      id = id,
      output_tokens = 8000,
      max_output_tokens = 64000,
      context_window = 200000,
    },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = tools or {},
    thinking = { kind = "off" },
    fast = true,
  }
end

local function at(secs, fn)
  local now = makima.time.now
  makima.time.now = function()
    return { secs = secs, nanosecs = 0 }
  end
  local ok, err = pcall(fn)
  makima.time.now = now
  if not ok then
    error(err, 0)
  end
end

local function container(expiration)
  return json({ AccessKeyId = "ASIA", SecretAccessKey = "secret", Token = "session", Expiration = expiration })
end

local function resolve(env, responses)
  local ctx = t.provider_ctx(responses or {})
  local credentials, err
  t.with_env(env, function()
    credentials, err = bedrock.resolve(ctx)
  end)
  return credentials, err, ctx
end

t.case("credentials_file_profiles", function()
  t.same(bedrock.parse_credentials_file(CREDENTIALS_FILE, "default"), { access_key = "AKID", secret_key = "SECRET" })
  t.same(
    bedrock.parse_credentials_file(CREDENTIALS_FILE, "myprofile"),
    { access_key = "MYKEY", secret_key = "MYSECRET", session_token = "MYTOKEN" }
  )
  t.eq(bedrock.parse_credentials_file(CREDENTIALS_FILE, "nonexistent"), nil)
end)

t.case("container_credentials_response", function()
  t.same(
    bedrock.parse_container_credentials({
      AccessKeyId = "ASIA123",
      SecretAccessKey = "secret456",
      Token = "session789",
      Expiration = "2026-05-11T18:00:00.123Z",
    }),
    { access_key = "ASIA123", secret_key = "secret456", session_token = "session789", expires_at = 1778522400 }
  )
  t.same(bedrock.parse_container_credentials({ AccessKeyId = "AKIA1", SecretAccessKey = "s1" }), {
    access_key = "AKIA1",
    secret_key = "s1",
  })
  local missing, err = bedrock.parse_container_credentials({ SecretAccessKey = "s1" })
  t.eq(missing, nil)
  t.eq(err.kind, http.CONFIG_KIND)
  t.has(err.message, "AccessKeyId")
end)

t.case("resolve_follows_claude_code_precedence", function()
  local keys = { AWS_ACCESS_KEY_ID = "AKID", AWS_SECRET_ACCESS_KEY = "SECRET", AWS_SESSION_TOKEN = "TOK" }
  local function with(extra)
    local env = { AWS_REGION = REGION }
    for name, value in pairs(extra) do
      env[name] = value
    end
    return env
  end
  t.same(select(2, resolve({})), http.config_error(bedrock.MISSING_REGION))
  t.same(
    resolve(with({ AWS_BEARER_TOKEN_BEDROCK = "bt", CLAUDE_CODE_SKIP_BEDROCK_AUTH = "1" })),
    { region = REGION, method = "bearer", bearer = "bt" }
  )
  t.same(
    resolve(with({ CLAUDE_CODE_SKIP_BEDROCK_AUTH = "1", AWS_ACCESS_KEY_ID = "AKID" })),
    { region = REGION, method = "gateway" }
  )
  keys.AWS_CONTAINER_CREDENTIALS_FULL_URI = CONTAINER_URL
  t.same(
    resolve(with(keys)),
    { region = REGION, method = "env", access_key = "AKID", secret_key = "SECRET", session_token = "TOK" }
  )
  t.same(select(2, resolve(with({ HOME = "/nonexistent" }))), http.config_error(bedrock.NO_CREDENTIALS))
end)

t.case("resolve_reads_the_profile_from_home", function()
  local home = t.mktmpdir("bedrock_home")
  makima.fs.mkdir(home .. "/.aws")
  makima.fs.write(home .. "/.aws/credentials", CREDENTIALS_FILE)
  local credentials = resolve({ AWS_REGION = REGION, HOME = home, AWS_PROFILE = "myprofile" })
  t.rmtree(home)
  t.same(credentials, {
    region = REGION,
    method = "profile",
    access_key = "MYKEY",
    secret_key = "MYSECRET",
    session_token = "MYTOKEN",
  })
end)

t.case("container_token_file_wins_and_is_trimmed", function()
  local dir = t.mktmpdir("bedrock_token")
  makima.fs.write(dir .. "/token", "file-token\n")
  local credentials, _, ctx = resolve({
    AWS_REGION = REGION,
    AWS_CONTAINER_CREDENTIALS_FULL_URI = CONTAINER_URL,
    AWS_CONTAINER_AUTHORIZATION_TOKEN = "env-token",
    AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE = dir .. "/token",
  }, { container(LATER_EXPIRY) })
  t.rmtree(dir)
  t.eq(ctx.requests[1].url, CONTAINER_URL)
  t.eq(ctx.requests[1].headers.Authorization, "file-token")
  t.eq(credentials.method, "container")
  t.eq(credentials.session_token, "session")
end)

t.case("container_failure_names_the_status", function()
  local _, err = resolve(
    { AWS_REGION = REGION, AWS_CONTAINER_CREDENTIALS_FULL_URI = CONTAINER_URL },
    { t.response({ "denied" }, { status = 403 }) }
  )
  t.same(err, http.config_error("container creds endpoint returned 403: denied"))
end)

t.case("missing_region_status_shows_the_message", function()
  local status
  t.with_env({}, function()
    status = bedrock.auth_status(t.provider_ctx({}))
  end)
  t.same(status, { state = "missing", detail = bedrock.MISSING_REGION })
end)

t.case("offline_status_never_calls_the_container", function()
  local ctx = t.provider_ctx({})
  local status
  t.with_env({ AWS_REGION = REGION, AWS_CONTAINER_CREDENTIALS_FULL_URI = CONTAINER_URL }, function()
    status = bedrock.auth_status(ctx)
  end)
  t.same(status, { state = "env", detail = "via Bedrock (container)" })
  t.eq(#ctx.requests, 0)
end)

t.case("body_and_url_differ_from_the_direct_api", function()
  local url, body
  t.with_env({}, function()
    url, body = bedrock.build_request(request("claude-opus-4-8-1m", { { name = "x", input_examples = {} } }), REGION)
  end)
  t.eq(url, "https://" .. HOST .. "/model/claude-opus-4-8/invoke-with-response-stream")
  t.eq(body.anthropic_version, "bedrock-2023-05-31")
  t.same(body.anthropic_beta, { "tool-examples-2025-10-29", "context-1m-2025-08-07" })
  t.eq(body.model, nil)
  t.eq(body.stream, nil)
  t.eq(body.speed, nil)
  t.eq(body.max_tokens, 64000)
  t.eq(body.system[1].text, "sys")
end)

t.case("model_and_endpoint_overrides", function()
  local url, body
  t.with_env({ ANTHROPIC_MODEL = BEDROCK_MODEL }, function()
    url, body = bedrock.build_request(request("claude-sonnet-4-5"), REGION)
  end)
  t.eq(url, MODEL_URL)
  t.eq(body.anthropic_beta, nil)
  t.with_env({ ANTHROPIC_BEDROCK_BASE_URL = "http://localhost:8080" }, function()
    url = bedrock.build_request(request("claude-sonnet-4-5"), REGION)
  end)
  t.eq(url, "http://localhost:8080/model/claude-sonnet-4-5/invoke-with-response-stream")
end)

t.case("amz_timestamp", function()
  t.eq(bedrock.amz_timestamp(NOW), NOW_STAMP)
  t.eq(bedrock.amz_timestamp(NOW + 3723), "20240101T010203Z")
end)

local function streamed(env, responses)
  local ctx = t.provider_ctx(responses)
  local result
  at(NOW, function()
    t.with_env(env, function()
      local instance = assert(bedrock.instance(ctx))
      result = assert(instance.stream(ctx, request("claude-sonnet-4-5"), t.recorder()))
    end)
  end)
  t.eq(result.message.content[1].text, "ok")
  return ctx.requests
end

t.case("signs_the_exact_bytes_sent", function()
  local env = { AWS_REGION = REGION, ANTHROPIC_MODEL = BEDROCK_MODEL }
  env.AWS_ACCESS_KEY_ID, env.AWS_SECRET_ACCESS_KEY, env.AWS_SESSION_TOKEN = "AKID", "SECRET", "TOK"
  local sent = streamed(env, { eventstream_text("ok") })[1]
  t.eq(sent.url, MODEL_URL)
  t.eq(sent.headers.Host, HOST)
  t.eq(sent.headers["Content-Type"], "application/json")
  t.eq(sent.headers["x-amz-security-token"], "TOK")
  local expected = sigv4.sign({
    method = "POST",
    url = MODEL_URL,
    headers = { { "content-type", "application/json" }, { "host", HOST } },
    body = sent.body,
    access_key = "AKID",
    secret_key = "SECRET",
    session_token = "TOK",
    region = REGION,
    service = "bedrock",
    timestamp = NOW_STAMP,
  })
  t.eq(sent.headers.Authorization, expected[1][2])
  t.eq(sent.headers["x-amz-date"], NOW_STAMP)
end)

t.case("bearer_and_gateway_skip_signing", function()
  local sent = streamed({ AWS_REGION = REGION, AWS_BEARER_TOKEN_BEDROCK = "bt" }, { eventstream_text("ok") })[1]
  t.eq(sent.headers.Authorization, "Bearer bt")
  t.eq(sent.headers["x-amz-date"], nil)
  sent = streamed({ AWS_REGION = REGION, CLAUDE_CODE_SKIP_BEDROCK_AUTH = "1" }, { eventstream_text("ok") })[1]
  t.eq(sent.headers.Authorization, nil)
end)

t.case("expiring_container_credentials_refresh_before_the_request", function()
  local env = { AWS_REGION = REGION, AWS_CONTAINER_CREDENTIALS_FULL_URI = CONTAINER_URL }
  local requests = streamed(env, { container(EXPIRY), container(LATER_EXPIRY), eventstream_text("ok") })
  t.eq(#requests, 3)
  t.eq(requests[2].url, CONTAINER_URL)
  t.has(requests[3].headers.Authorization, "Credential=ASIA/20240101/us-east-1/bedrock")

  requests = streamed(env, { container(LATER_EXPIRY), eventstream_text("ok") })
  t.eq(#requests, 2)
end)

t.case("failed_refresh_stops_the_request", function()
  local ctx = t.provider_ctx({ container(EXPIRY), t.response({ "gone" }, { status = 500 }) })
  local env = { AWS_REGION = REGION, AWS_CONTAINER_CREDENTIALS_FULL_URI = CONTAINER_URL }
  at(NOW, function()
    t.with_env(env, function()
      local instance = assert(bedrock.instance(ctx))
      local result, err = instance.stream(ctx, request("claude-sonnet-4-5"), t.recorder())
      t.eq(result, nil)
      t.eq(err.kind, http.CONFIG_KIND)
      t.has(err.message, "returned 500")
    end)
  end)
  t.eq(#ctx.requests, 2)
end)

t.report()
