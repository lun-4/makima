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
  assert(requests[1].url == "https://auth.openai.com/api/accounts/deviceauth/usercode")
  assert(requests[1].method == "POST" and requests[1].headers["Content-Type"] == "application/json")
  local start_body = assert(maki.json.decode(requests[1].body))
  assert(start_body.client_id == "app_EMoamEEZ73f0CkXaXp7hrann")
  replies[1] = { status = 403 }
  local value, err = auth.poll(ctx, session)
  assert(not value and err.kind == "pending" and err.status == 403)
  replies[1] = { status = 200, body = [[{"authorization_code":"a+b","code_verifier":"x/y"}]] }
  local device = assert(auth.poll(ctx, session))
  assert(requests[3].url == "https://auth.openai.com/api/accounts/deviceauth/token")
  local poll_body = assert(maki.json.decode(requests[3].body))
  assert(poll_body.device_auth_id == "device" and poll_body.user_code == "user")
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
  assert(requests[4].url == "https://auth.openai.com/oauth/token")
  assert(requests[4].headers["Content-Type"] == "application/x-www-form-urlencoded")
  assert(requests[4].body:find("grant_type=authorization_code", 1, true))
  assert(requests[4].body:find("code=a%%2Bb") and requests[4].body:find("code_verifier=x%%2Fy"))
  assert(requests[4].body:find("redirect_uri=https%%3A%%2F%%2Fauth.openai.com%%2Fdeviceauth%%2Fcallback"))
  replies[1] = { status = 200, body = [[{"access_token":"new","refresh_token":"rotated"}]] }
  local refreshed = assert(auth.refresh(ctx, tokens, 2000))
  assert(refreshed.access == "new" and refreshed.refresh == "rotated" and refreshed.expires == 3602000)
  assert(requests[5].url == requests[4].url)
  assert(requests[5].body:find("grant_type=refresh_token", 1, true))
  assert(requests[5].body:find("refresh_token=refresh", 1, true))
  replies[1] = { status = 200, body = [[{"access_token":"secret"}]] }
  value, err = auth.refresh(ctx, tokens, 2000)
  assert(not value and err.kind == "api")
  replies[1] = { status = 500, body = "secret" }
  value, err = auth.poll(ctx, session)
  assert(not value and err.kind == "api" and not err.message:find("secret"))
  assert(auth.account_id("bad.token") == nil and closed == 7)
end

do
  local sent = 0
  local ctx = {
    send = function()
      sent = sent + 1
      return nil, "sensitive network detail"
    end,
  }
  local value, err = auth.start(ctx)
  assert(not value and err.kind == "network" and not err.message:find("sensitive"))
  value, err = auth.exchange(ctx, { authorization_code = "", code_verifier = "verifier" }, 0)
  assert(not value and err.kind == "config" and sent == 1)
  value, err = auth.refresh(ctx, {}, 0)
  assert(not value and err.kind == "config" and sent == 1)
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

do
  local stored = { access = "old-access", refresh = "old-refresh", expires = 9999999999999, account_id = "account" }
  local requests, replies, closed = {}, {}, 0
  local credentials = {
    read = function(_, key)
      assert(key == "chatgpt")
      return stored
    end,
    refresh = function(_, key, callback)
      assert(key == "chatgpt")
      local updated, err = callback(stored)
      if not updated then
        return nil, err
      end
      stored = updated
      return stored
    end,
  }
  local ctx = {
    credentials = credentials,
    send = function(_, request)
      requests[#requests + 1] = request
      local reply = assert(table.remove(replies, 1), "unexpected request")
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
  local model = { id = "gpt-5.6-luna", max_output_tokens = 1000 }
  local provider =
    assert(openai.prepare(ctx, model, { api_key = "fallback", headers = { Authorization = "override" } }))
  local request = { model = model, messages = {}, tools = {}, thinking = { mode = "off" } }
  replies[1] = {
    status = 200,
    body = 'event: response.output_text.delta\ndata: {"delta":"hello"}\n\nevent: response.completed\ndata: {"response":{"status":"completed"}}\n\n',
  }
  local result = assert(provider.stream(ctx, request, function()
    return true
  end))
  assert(result.message.content[1].text == "hello")
  assert(requests[1].url == "https://chatgpt.com/backend-api/codex/responses")
  assert(requests[1].headers.Authorization == "Bearer old-access")
  assert(requests[1].headers["chatgpt-account-id"] == "account")
  assert(assert(maki.json.decode(requests[1].body)).model == model.id)

  replies[1] = {
    status = 200,
    body = [[{"models":[{"slug":"discovered","visibility":"list","service_tiers":[{"id":"priority"}]}]}]],
  }
  local discovered = assert(openai.discover(ctx, { api_key = "fallback" }))
  assert(discovered[1].id == "discovered" and discovered[1].fast)
  assert(discovered[1].provider_data.account_id == "account")
  assert(requests[2].url:find("https://chatgpt.com/backend-api/codex/models?client_version=", 1, true) == 1)
  assert(requests[2].headers.Authorization == "Bearer old-access")
  assert(requests[2].headers["chatgpt-account-id"] == "account")

  local payload = maki.base64.encode([[{"chatgpt_account_id":"account"}]]):gsub("=+$", "")
  replies[1] = {
    status = 200,
    body = assert(maki.json.encode({
      access_token = "new-access",
      refresh_token = "new-refresh",
      id_token = "header." .. payload .. ".signature",
      expires_in = 3600,
    })),
  }
  assert(provider.refresh_auth(ctx))
  assert(stored.access == "new-access" and stored.refresh == "new-refresh")
  assert(requests[3].url == "https://auth.openai.com/oauth/token")
  assert(requests[3].body:find("refresh_token=old-refresh", 1, true))
  replies[1] = {
    status = 200,
    body = [[{"plan_type":"pro","rate_limit":{"primary_window":{"used_percent":20,"limit_window_seconds":18000}}}]],
  }
  local quota = assert(provider.fetch_usage(ctx))
  assert(quota.plan == "pro" and quota.limits[1].percentage == 20)
  assert(requests[4].url == "https://chatgpt.com/backend-api/wham/usage" and requests[4].method == "GET")
  assert(requests[4].headers.Authorization == "Bearer new-access")

  stored = { access = "reloaded", refresh = "refresh", account_id = "other", expires = 9999999999999 }
  assert(provider.reload_auth(ctx))
  replies[1] = { status = 500, body = "secret" }
  local _, err = provider.stream(ctx, request, function() end)
  assert(err.status == 500 and not err.message:find("secret"))
  assert(requests[5].headers.Authorization == "Bearer reloaded")
  assert(requests[5].headers["chatgpt-account-id"] == "other")
  stored = nil
  local ok, reload_err = provider.reload_auth(ctx)
  assert(not ok and reload_err.kind == "config")
  assert(closed == 5)
end

do
  local previous = maki.uv.os_getenv
  local env = {}
  maki.uv.os_getenv = function(name)
    return env[name]
  end
  local stored = { api_key = "saved" }
  local oauth
  local refresh_error = { kind = "network", message = "refresh failed" }
  local ctx = {
    credentials = {
      read = function(_, key)
        return key == "chatgpt" and oauth or (key == "api_key" and stored or nil)
      end,
      refresh = function()
        return nil, refresh_error
      end,
    },
  }
  local sent
  ctx.send = function(_, request)
    sent = request
    return { status = 401, close = function() end }
  end
  local model = { id = "gpt-test", max_output_tokens = 1000 }
  local request = { model = model, messages = {}, tools = {}, thinking = { mode = "off" } }
  local function check(config, expected)
    local provider = assert(openai.prepare(ctx, model, config or {}))
    provider.stream(ctx, request, function() end)
    assert(sent.headers.Authorization == "Bearer " .. expected)
  end
  check(nil, "saved")
  check({ api_key = "configured" }, "configured")
  env.OPENAI_API_KEY = "environment"
  check({ api_key = "configured" }, "environment")
  oauth = { access = "expired", refresh = "refresh", expires = 0 }
  check(nil, "environment")
  env.OPENAI_API_KEY = nil
  check(nil, "saved")
  oauth = { access = "valid", refresh = "refresh", expires = 9999999999999 }
  local live = assert(openai.prepare(ctx, model, {}))
  live.stream(ctx, request, function() end)
  assert(sent.headers.Authorization == "Bearer valid")
  oauth.expires = 0
  live.stream(ctx, request, function() end)
  assert(sent.headers.Authorization == "Bearer saved")
  oauth = { access = "expired", refresh = "refresh", expires = 0 }
  stored = nil
  local provider, err = openai.prepare(ctx, model, {})
  assert(not provider and err == refresh_error)
  stored = { api_key = "saved" }
  local models = require("maki.openai_models")
  local previous_discover = models.discover
  models.discover = function(_, options)
    assert(options.kind == "platform" and options.headers.Authorization == "Bearer saved")
    return { { id = "fixture" } }
  end
  assert(openai.discover(ctx, {})[1].id == "fixture")
  models.discover = previous_discover
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
