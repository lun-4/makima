local zai = require("maki.zai")
assert(#zai.models == 10)
assert(zai.supports_thinking("glm-5.2"))
assert(zai.supports_thinking("glm-6.0"))
assert(not zai.supports_thinking("glm-5-code"))
assert(not zai.supports_thinking("glm-4.7"))
local request = { model = { id = "glm-5.3", max_output_tokens = 131072 }, messages = {}, thinking = { mode = "off" } }
local body = assert(zai.build_request(request))
assert(body.reasoning_effort == "none" and body.max_tokens == 131072 and body.stream_options == nil)
request.thinking = { mode = "adaptive" }
assert(zai.build_request(request).reasoning_effort == "high")
request.thinking = { mode = "effort", level = "max" }
assert(zai.build_request(request).reasoning_effort == "xhigh")
request.model.id = "glm-5-code"
assert(zai.build_request(request).reasoning_effort == nil)
local usage = assert(
  zai.parse_quotas(
    '{"data":{"level":"Pro","limits":[{"type":"TOKENS_LIMIT","unit":3,"percentage":25,"nextResetTime":123},{"type":"TOKENS_LIMIT","unit":6,"percentage":50},{"type":"TIME_LIMIT","unit":1,"percentage":10}]}}'
  )
)
assert(usage.plan == "Pro" and usage.limits[1].kind.hours == 5 and usage.limits[1].reset_at == 123)
assert(usage.limits[2].kind.type == "weekly" and usage.limits[3].kind.type == "subscription")
assert(not zai.parse_quotas('{"data":{"limits":[{"type":"TIME_LIMIT"}]}}'))
assert(not zai.parse_quotas('{"data":{"limits":[{"type":"TIME_LIMIT","unit":-1,"percentage":2}]}}'))
assert(not zai.parse_quotas('{"data":{"limits":[{"type":"TIME_LIMIT","unit":1,"percentage":1.5}]}}'))
local unknown = assert(zai.parse_quotas('{"data":{"limits":[{"type":"X","unit":9,"percentage":50}]}}'))
assert(unknown.limits[1].kind.label == "X #9")
local provider = assert(
  zai.prepare(
    nil,
    {},
    { api_key_env = "MAKI_TEST_NO_ZAI_KEY", api_key = "first, second", base_url = "https://example.test/" }
  )
)
assert(provider.key_count == 2 and provider.rotate_key())
local sent
local ctx = {
  send = function(_, req)
    sent = req
    local read = false
    return {
      status = 429,
      headers = {},
      read = function()
        if read then
          return ""
        end
        read = true
        return "1113 insufficient balance"
      end,
      close = function() end,
    }
  end,
}
local _, err = provider.stream(ctx, request, function() end)
assert(sent.url == "https://example.test/chat/completions" and sent.headers.Authorization == "Bearer second")
assert(err.status == 402 and err.message == "1113 insufficient balance" and err.retry_after_ms == nil)

local saved
local credentials = {
  read = function()
    return saved
  end,
  write = function(_, key, value)
    assert(key == "api_key")
    saved = value
    return true
  end,
  delete = function(_, key)
    assert(key == "api_key")
    saved = nil
    return true
  end,
}
local auth_ctx = { credentials = credentials }
local config = { api_key_env = "MAKI_TEST_NO_ZAI_KEY", api_key = "fallback" }
assert(zai.auth_status(auth_ctx, config).source == "config")
assert(zai.login(auth_ctx, {
  input = function()
    return "  saved-key  "
  end,
}))
assert(saved.api_key == "saved-key" and zai.auth_status(auth_ctx, config).source == "stored")
assert(zai.prepare(auth_ctx, {}, config).key_count == 1)
assert(zai.logout(auth_ctx) and not saved)
assert(zai.auth_status(auth_ctx, { api_key_env = "MAKI_TEST_NO_ZAI_KEY" }).configured == false)
assert(zai.login(auth_ctx, {
  input = function()
    return nil
  end,
}) == false)
local _, blank = zai.login(auth_ctx, {
  input = function()
    return "   "
  end,
})
assert(blank.kind == "config")
saved = { api_key = false }
local _, invalid = zai.auth_status(auth_ctx, config)
assert(invalid.message == "Invalid Z.AI credentials")
