local anthropic = require("maki.anthropic")
local direct = require("maki.anthropic_direct")

local frames = {
  'event: message_start\ndata: {"message":{"usage":{"input_tokens":12,"cache_read_input_tokens":3}}}\n\n',
  'event: content_block_start\ndata: {"index":0,"content_block":{"type":"thinking"}}\n\n',
  'event: content_block_delta\ndata: {"index":0,"delta":{"type":"thinking_delta","thinking":"reason"}}\n\n',
  'event: content_block_delta\ndata: {"index":0,"delta":{"type":"signature_delta","signature":"sig"}}\n\n',
  'event: content_block_start\ndata: {"index":1,"content_block":{"type":"tool_use","id":"id","name":"tool"}}\n\n',
  'event: content_block_delta\ndata: {"index":1,"delta":{"type":"input_json_delta","partial_json":"{\\"x\\":1}"}}\n\n',
  'event: content_block_stop\ndata: {"index":1}\n\n',
  'event: message_delta\ndata: {"delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}\n\n',
  "event: message_stop\ndata: {}\n\n",
}
local position, emitted = 0, {}
local response = {
  read = function()
    position = position + 1
    return frames[position] or ""
  end,
}
local result = assert(direct.stream(response, function(event)
  emitted[#emitted + 1] = event
  return true
end))
assert(result.message.content[1].thinking == "reason" and result.message.content[1].signature == "sig")
assert(result.message.content[2].input.x == 1 and emitted[2].type == "tool_use_start")
assert(result.usage.input == 12 and result.usage.cache_read == 3 and result.usage.output == 7)
assert(result.stop_reason == "tool_use")
assert(#direct.models == 13 and direct.models[1].id == "claude-haiku-4-5")
local failed = {
  read = function()
    return 'event: error\ndata: {"error":{"type":"overloaded_error","message":"busy"}}\n\n'
  end,
}
local _, stream_error = direct.stream(failed, function()
  return true
end)
assert(stream_error.status == 529 and stream_error.message == "busy")
local prepared = assert(
  direct.prepare(nil, { id = "claude-sonnet-5" }, { api_key = "first, second", base_url = "https://fixture.invalid" })
)
assert(prepared.key_count == 2 and prepared.rotate_key())
local sent
local ctx = {
  send = function(_, request)
    sent = request
    local read = false
    return {
      status = 429,
      headers = { ["retry-after"] = "3" },
      read = function()
        if read then
          return ""
        end
        read = true
        return "first"
      end,
      close = function() end,
    }
  end,
}
local _, api_error = prepared.stream(ctx, { model = { id = "claude-sonnet-5" }, messages = {} }, function()
  return true
end)
assert(sent.headers["x-api-key"] == "second")
assert(api_error.status == 429 and api_error.retry_after_ms == 3000 and api_error.message == "Anthropic request failed")
local saved_key = "stored-key"
local credential_ctx = {
  credentials = {
    read = function(_, name)
      assert(name == "api_key")
      return { api_key = saved_key }
    end,
  },
}
local stored = assert(
  direct.prepare(
    credential_ctx,
    { id = "claude-sonnet-5" },
    { api_key_env = "MAKI_TEST_MISSING_ANTHROPIC_KEY", api_key = "config-key" }
  )
)
local function current_key()
  local _, err = stored.stream(ctx, { model = { id = "claude-sonnet-5" }, messages = {} }, function()
    return true
  end)
  assert(err.status == 429)
  return sent.headers["x-api-key"]
end
assert(current_key() == "stored-key")
saved_key = "replacement-key"
assert(stored.reload_auth(credential_ctx))
assert(current_key() == "replacement-key")
local _, invalid = direct.prepare(
  { credentials = {
    read = function()
      return { api_key = false }
    end,
  } },
  {},
  { api_key_env = "MAKI_TEST_MISSING_ANTHROPIC_KEY" }
)
assert(invalid.kind == "config" and invalid.message == "Invalid Anthropic credentials")
local saved
local auth_ctx = {
  credentials = {
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
  },
}
local auth_config = { api_key_env = "MAKI_TEST_MISSING_ANTHROPIC_KEY", api_key = "config-key" }
assert(direct.auth_status(auth_ctx, auth_config).source == "config")
assert(direct.login(auth_ctx, {
  input = function(_, prompt, secret, optional)
    assert(prompt == "Anthropic API key" and secret and not optional)
    return "  logged-in-key  "
  end,
}))
assert(saved.api_key == "logged-in-key" and direct.auth_status(auth_ctx, auth_config).source == "stored")
assert(direct.logout(auth_ctx) and direct.auth_status(auth_ctx, auth_config).source == "config")
assert(direct.login(auth_ctx, {
  input = function()
    return nil
  end,
}) == false)
local _, empty_login = direct.login(auth_ctx, {
  input = function()
    return "  "
  end,
})
assert(empty_login.kind == "config" and saved == nil)
local denied = {
  credentials = {
    read = function()
      return nil, { kind = "config", message = "credential access denied" }
    end,
  },
}
assert(direct.auth_status(denied, auth_config).source == "config")
local request = {
  model = { id = "claude-opus-4-7-1m", max_output_tokens = 8192, fast = true },
  system = "system",
  messages = {
    { role = "user", content = { { type = "text", text = "first" } } },
    { role = "assistant", content = { { type = "text", text = "  " } } },
    {
      role = "user",
      content = {
        { type = "image", source = { media_type = "image/png", data = "abc" } },
        { type = "thinking", thinking = "thought" },
        { type = "redacted_thinking", data = "secret" },
      },
    },
  },
  tools = { { name = "tool", description = "test", input_schema = { type = "object" } } },
  thinking = { mode = "effort", level = "xhigh" },
  fast = true,
}
local body = anthropic.build_request(request, { system_prefix = "prefix" })
assert(body.model == "claude-opus-4-7" and body.max_tokens == 8192 and body.stream and body.speed == "fast")
assert(body.system[1].text == "prefix" and body.system[1].cache_control == nil)
assert(body.system[2].cache_control.type == "ephemeral")
assert(body.messages[1].content[1].cache_control == nil)
assert(body.messages[2].content[1].text == "(empty)" and body.messages[2].content[1].cache_control)
assert(body.messages[3].content[1].source.type == "base64" and body.messages[3].content[1].cache_control)
assert(body.messages[3].content[2].cache_control == nil and body.messages[3].content[3].cache_control == nil)
assert(body.tools[1].cache_control.type == "ephemeral" and request.tools[1].cache_control == nil)
assert(request.messages[3].content[1].source.type == nil)
assert(body.thinking.type == "adaptive" and body.thinking.display == "summarized")
assert(body.output_config.effort == "high")
local empty =
  anthropic.build_request({ model = { id = "model" }, messages = {}, tools = assert(maki.json.decode("[]")) })
local encoded = assert(maki.json.encode(empty))
assert(encoded:find('"messages":%[%]') and encoded:find('"tools":%[%]'))
assert(empty.max_tokens == 32000 and empty.speed == nil)
local legacy = anthropic.build_request({
  model = { id = "claude-sonnet-4-5", max_output_tokens = 8192 },
  messages = {},
  tools = {},
  thinking = { mode = "budget", tokens = 9000 },
})
assert(legacy.thinking.type == "enabled" and legacy.thinking.budget_tokens == 4096)
local only_thinking =
  anthropic.convert_messages({ { role = "assistant", content = { { type = "thinking", thinking = "x" } } } })
assert(only_thinking[1].content[1].cache_control == nil)
local result = anthropic.convert_messages({
  { role = "user", content = { { type = "tool_result", tool_use_id = "id", content = "ok", is_error = false } } },
})
assert(result[1].content[1].is_error == nil)

local shared = assert(anthropic.build_shared_body(request))
assert(shared.model == nil and shared.stream == nil and shared.speed == nil)
assert(request.model.id == "claude-opus-4-7-1m")
assert(anthropic.beta_header(request) == "fast-mode-2026-02-01,context-1m-2025-08-07")
assert(anthropic.beta_header({ model = { id = "claude-1m", fast = false }, fast = true }) == "context-1m-2025-08-07")
assert(anthropic.beta_header({ model = { id = "claude", fast = true }, fast = true }) == "fast-mode-2026-02-01")
assert(anthropic.beta_header({ model = { id = "claude-1m-extra", fast = true }, fast = false }) == nil)
local object_tools = { custom = true }
assert(
  anthropic.build_shared_body({ model = request.model, messages = {}, tools = object_tools }).tools == object_tools
)
assert(anthropic.convert_tools("opaque") == "opaque")
assert(anthropic.convert_tools(false) == false)
for _, id in ipairs({ "claude-3-5-sonnet-20241022", "vendor/claude-3-5", "claude-opus-4x7" }) do
  local wire = anthropic.build_shared_body({
    model = { id = id },
    messages = {},
    tools = {},
    thinking = { mode = "budget", tokens = 2048 },
  })
  assert(wire.thinking.type == "enabled" and wire.thinking.budget_tokens == 2048)
end
local modern = anthropic.build_shared_body({
  model = { id = "vendor/claude-opus-4.7" },
  messages = {},
  tools = {},
  thinking = { mode = "adaptive" },
})
assert(modern.thinking.type == "adaptive" and modern.thinking.display == "summarized")

for _, text in ipairs({ "", "\t\r\n ", "\194\133\194\160", "\226\128\131\227\128\128" }) do
  local messages = anthropic.convert_messages({ { role = "user", content = { { type = "text", text = text } } } })
  assert(messages[1].content[1].text == "(empty)")
end
local nonblank = anthropic.convert_messages({
  {
    role = "user",
    content = {
      { type = "text", text = "\226\128\131hello\194\160" },
      { type = "text", text = "\226\128\139" },
    },
  },
})
assert(nonblank[1].content[1].text == "\226\128\131hello\194\160")
assert(nonblank[1].content[2].text == "\226\128\139")

for _, case in ipairs({
  { 409, "low" },
  { 410, "low" },
  { 860, "low" },
  { 861, "medium" },
  { 1679, "medium" },
  { 1680, "high" },
  { 4096, "high" },
}) do
  local wire = anthropic.build_shared_body({
    model = { id = "claude-opus-4-7", max_output_tokens = 8192 },
    messages = {},
    thinking = { mode = "budget", tokens = case[1] },
  })
  assert(wire.output_config.effort == case[2])
end

local usage = assert(direct.parse_usage({
  limits = {
    { kind = "session", percent = 13.6, resets_at = "reset" },
    { kind = "weekly_scoped", percent = 2.5, scope = { model = { display_name = "Sonnet" } } },
    { kind = "custom", percent = 4 },
  },
  five_hour = { utilization = 99 },
  spend = { enabled = true, used = { amount_minor = 233 } },
}, function(timestamp)
  return timestamp == "reset" and 1234 or nil
end))
assert(#usage.limits == 4 and usage.limits[1].percentage == 14 and usage.limits[1].reset_at == 1234)
assert(usage.limits[2].kind.model == "Sonnet" and usage.limits[2].percentage == 3)
assert(usage.limits[3].kind.label == "custom")
assert(usage.limits[4].percentage == 0 and usage.limits[4].detail == "$2.33 spent")
local fallback = assert(direct.parse_usage({
  limits = { { kind = "session" } },
  five_hour = { utilization = 35 },
  seven_day_opus = { utilization = 3 },
  extra_usage = { is_enabled = true, utilization = 2, used_credits = 233 },
}))
assert(#fallback.limits == 3 and fallback.limits[2].kind.model == "Opus")
assert(fallback.limits[3].detail == "$2.33 spent")
assert(
  #assert(direct.parse_usage({ spend = { enabled = false }, extra_usage = { is_enabled = true, utilization = 2 } })).limits
    == 0
)
assert(not direct.parse_usage({ limits = { {} } }))

local usage_request, closed
local usage_ctx = {
  send = function(_, request)
    usage_request = request
    local done = false
    return {
      status = 200,
      read = function()
        if done then
          return ""
        end
        done = true
        return '{"five_hour":{"utilization":12}}'
      end,
      close = function()
        closed = true
      end,
    }
  end,
}
assert(direct.fetch_usage(usage_ctx, "https://proxy.invalid", { Authorization = "Bearer token" }) == nil)
assert(direct.fetch_usage(usage_ctx, nil, { ["x-api-key"] = "key" }) == nil)
local fetched = assert(
  direct.fetch_usage(
    usage_ctx,
    "https://proxy.invalid/v1/messages?x=1",
    { Authorization = "Bearer token" },
    "https://proxy.invalid"
  )
)
assert(closed and fetched.limits[1].percentage == 12)
assert(usage_request.url == "https://proxy.invalid/api/oauth/usage" and usage_request.method == "GET")
assert(
  usage_request.headers.Authorization == "Bearer token"
    and usage_request.headers["anthropic-beta"] == "oauth-2025-04-20"
)
