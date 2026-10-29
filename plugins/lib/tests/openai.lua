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
  assert(refreshed.account_id == "account")
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

do
  local previous = maki.uv.os_getenv
  maki.uv.os_getenv = function()
    return nil
  end
  local values = { chatgpt = { access = "access", expires = 12345 }, api_key = { api_key = "saved" } }
  local fail
  local ctx = {
    credentials = {
      read = function(_, key)
        return values[key]
      end,
      delete = function(_, key)
        if fail == key then
          return nil, { kind = "config", message = "delete failed" }
        end
        values[key] = nil
        return true
      end,
    },
  }
  local status = assert(openai.auth_status(ctx, {}))
  assert(status.configured and status.source == "stored" and status.method == "chatgpt" and status.expires_at == 12345)
  assert(not status.access)
  values.chatgpt = nil
  assert(openai.auth_status(ctx, {}).method == "api_key")
  assert(openai.auth_status(ctx, { api_key = "configured" }).source == "config")
  fail = "api_key"
  local ok, err = openai.logout(ctx)
  assert(not ok and err.message == "delete failed" and values.api_key)
  fail = nil
  assert(openai.logout(ctx) and not values.api_key)
  assert(not openai.auth_status(ctx, {}).configured)
  maki.uv.os_getenv = previous
end

do
  local previous_now, previous_await, previous_defer, previous_cancel =
    maki.time.now, maki.async.await, maki.defer_fn, maki.async.on_cancel
  local millis, waits, written, cancel = 0, {}, nil, nil
  maki.async.on_cancel = function(callback)
    cancel = callback
  end
  maki.time.now = function()
    return { secs = math.floor(millis / 1000), nanosecs = (millis % 1000) * 1000000 }
  end
  maki.async.await = function(_, schedule)
    local result, err
    schedule(function(value, failure)
      result, err = value, failure
    end)
    return result, err
  end
  maki.defer_fn = function(callback, delay)
    waits[#waits + 1] = delay
    millis = millis + delay
    callback()
    return { stop = function() end }
  end
  local replies = {
    { status = 200, body = [[{"device_auth_id":"device","user_code":"user","interval":"7"}]] },
    { status = 403 },
    { status = 200, body = [[{"authorization_code":"code","code_verifier":"verifier"}]] },
    { status = 200, body = [[{"access_token":"access","refresh_token":"refresh","expires_in":3600}]] },
  }
  local ctx = {
    credentials = {
      write = function(_, key, value)
        assert(key == "chatgpt")
        written = value
        return true
      end,
    },
    send = function()
      local reply = assert(table.remove(replies, 1))
      local body = reply.body or ""
      return {
        status = reply.status,
        read = function()
          local chunk = body
          body = ""
          return chunk
        end,
        close = function() end,
      }
    end,
  }
  local ui = {
    device_challenge = function(_, url, code)
      assert(url == auth.verification_url and code == "user")
      return true
    end,
  }
  assert(auth.login(ctx, ui))
  assert(written.access == "access" and written.refresh == "refresh" and written.expires == 3620000)
  assert(#waits == 2 and waits[1] == 10000 and waits[2] == 10000)
  written = nil
  replies = { { status = 200, body = [[{"device_auth_id":"device","user_code":"user"}]] } }
  ui.device_challenge = function()
    return false
  end
  local ok, err = auth.login(ctx, ui)
  assert(not ok and err.kind == "config" and written == nil)
  ui.device_challenge = function()
    return true
  end
  replies = {
    { status = 200, body = [[{"device_auth_id":"device","user_code":"user"}]] },
    { status = 403 },
  }
  local stopped = false
  maki.defer_fn = function(callback)
    local timer = {
      stop = function()
        stopped = true
      end,
    }
    cancel()
    callback()
    return timer
  end
  ok = auth.login(ctx, ui)
  assert(not ok and stopped and written == nil and #replies == 1)
  maki.defer_fn = function(callback, delay)
    millis = millis + delay
    callback()
    return { stop = function() end }
  end
  replies = {
    { status = 200, body = [[{"device_auth_id":"device","user_code":"user"}]] },
    { status = 200, body = [[{"authorization_code":"code","code_verifier":"verifier"}]] },
  }
  local original_send = ctx.send
  local count = 0
  ctx.send = function(...)
    count = count + 1
    local response = original_send(...)
    if count == 2 then
      cancel()
    end
    return response
  end
  ok = auth.login(ctx, ui)
  assert(not ok and written == nil and count == 2)
  maki.time.now, maki.async.await, maki.defer_fn, maki.async.on_cancel =
    previous_now, previous_await, previous_defer, previous_cancel
end

do
  local saved, writes = { access = "existing" }, 0
  local conflict = { kind = "config", message = "credential file contains a different credential type" }
  local ctx = {
    credentials = {
      write = function(_, key, value)
        writes = writes + 1
        assert(key == "api_key" and value.api_key == "new-key")
        if saved.access then
          return nil, conflict
        end
        saved = value
        return true
      end,
    },
  }
  local ui = {
    input = function(_, prompt, secret, optional)
      assert(prompt == "OpenAI API key" and secret and not optional)
      return "  new-key  "
    end,
  }
  local ui = {
    input = function(_, prompt, secret, optional)
      assert(prompt == "OpenAI API key" and secret and not optional)
      return "  new-key  "
    end,
  }
  local ok, err = openai.login_api_key(ctx, ui)
  assert(not ok and err == conflict and saved.access == "existing")
  saved = {}
  assert(openai.login_api_key(ctx, ui) and saved.api_key == "new-key")
  ui.input = function()
    return "  "
  end
  ok, err = openai.login_api_key(ctx, ui)
  assert(not ok and err.kind == "config" and writes == 2 and saved.api_key == "new-key")
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
