local openai = require("maki.openai")
local auth = require("maki.openai_auth")

do
  local requests, replies, closed = {}, {}, 0
  local ctx = {
    send = function(_, request)
      requests[#requests + 1] = request
      local reply = table.remove(replies, 1)
      if not reply then
        return nil, "network failure"
      end
      local body = reply.body or ""
      return {
        status = reply.status,
        read = function()
          local chunk = body
          body = ""
          return chunk
        end,
        close = function()
          closed = closed + 1
        end,
      }
    end,
  }
  replies[1] = { status = 200, body = [[{"device_auth_id":"device","user_code":"user","interval":"7"}]] }
  local session = assert(auth.start(ctx))
  assert(session.interval == 10 and session.user_code == "user" and session.verification_url == auth.verification_url)
  assert(requests[1].body:find("client_id", 1, true))
  replies[1] = { status = 403 }
  local value, err = auth.poll(ctx, session)
  assert(not value and err.kind == "pending" and err.status == 403)
  replies[1] = { status = 200, body = [[{"authorization_code":"a+b","code_verifier":"x/y"}]] }
  local device = assert(auth.poll(ctx, session))
  local payload = maki.base64
    .encode([[{"https://api.openai.com/auth":{"chatgpt_account_id":"account"}}]])
    :gsub("=+$", "")
    :gsub("%+", "-")
    :gsub("/", "_")
  local jwt = "header." .. payload .. ".signature"
  replies[1] = {
    status = 200,
    body = assert(
      maki.json.encode({ access_token = "access", refresh_token = "refresh", id_token = jwt, expires_in = 20 })
    ),
  }
  local tokens = assert(auth.exchange(ctx, device, 1000))
  assert(tokens.account_id == "account" and tokens.expires == 21000 and tokens.refresh == "refresh")
  assert(requests[4].body:find("code=a%%2Bb") and requests[4].body:find("code_verifier=x%%2Fy"))
  replies[1] = { status = 200, body = [[{"access_token":"new","refresh_token":"rotated"}]] }
  local refreshed = assert(auth.refresh(ctx, tokens, 2000))
  assert(refreshed.refresh == "rotated" and refreshed.expires == 3602000)
  replies[1] = { status = 200, body = [[{"access_token":"secret"}]] }
  value, err = auth.refresh(ctx, tokens, 2000)
  assert(not value and err.kind == "api")
  replies[1] = { status = 500, body = "secret" }
  value, err = auth.poll(ctx, session)
  assert(not value and err.kind == "api" and not err.message:find("secret"))
  assert(auth.account_id("bad.token") == nil and closed == 7)
end

do
  local previous = maki.uv.os_getenv
  maki.uv.os_getenv = function()
    return nil
  end
  local instance = assert(
    openai.prepare(nil, { id = "gpt-test" }, { api_key = "first, second", base_url = "https://example.test/v1/" })
  )
  local sent, closed
  local ctx = {
    send = function(_, request)
      sent = request
      return {
        status = 401,
        close = function()
          closed = true
        end,
      }
    end,
  }
  local request =
    { model = { id = "gpt-test", max_output_tokens = 1000 }, messages = {}, tools = {}, thinking = { mode = "off" } }
  local _, err = instance.stream(ctx, request, function() end)
  assert(err.status == 401 and closed and sent.url == "https://example.test/v1/chat/completions")
  assert(sent.headers.Authorization == "Bearer first")
  assert(instance.rotate_key())
  request.model.id = "gpt-test-codex"
  instance.stream(ctx, request, function() end)
  assert(sent.url == "https://example.test/v1/responses" and sent.headers.Authorization == "Bearer second")
  maki.uv.os_getenv = previous
end

local usage = assert(
  openai.parse_usage(
    [[{"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":12.5,"limit_window_seconds":18000,"reset_at":1760000000},"secondary_window":{"used_percent":150,"limit_window_seconds":604800}}}]]
  )
)
assert(usage.plan == "pro" and #usage.limits == 2)
assert(usage.limits[1].kind.type == "hours" and usage.limits[1].kind.hours == 5)
assert(usage.limits[1].percentage == 13 and usage.limits[1].reset_at == 1760000000000)
assert(usage.limits[2].kind.type == "weekly" and usage.limits[2].percentage == 100)

for _, case in ipairs({ { 86400, "days" }, { 0, "days" }, { 17, "other" } }) do
  local result = assert(openai.parse_usage(assert(maki.json.encode({
    rate_limit = {
      primary_window = {
        used_percent = -2,
        limit_window_seconds = case[1],
        reset_at = 18446744073709552,
      },
    },
  }))))
  assert(result.limits[1].kind.type == case[2] and result.limits[1].percentage == 0)
  assert(result.limits[1].reset_at == nil)
end
assert(assert(openai.parse_usage([[{"plan_type":"free"}]])).plan == "free")
for _, response in ipairs({ "{}", [[{"rate_limit":{}}]], [[{"rate_limit":{"primary_window":{"used_percent":5}}}]] }) do
  local result, err = openai.parse_usage(response)
  assert(not result and err.kind == "config")
end
for _, response in ipairs({
  "{",
  [[{"plan_type":1}]],
  [[{"rate_limit":{"primary_window":{"limit_window_seconds":-1}}}]],
}) do
  local result, err = openai.parse_usage(response)
  assert(not result and err.kind == "api")
end
