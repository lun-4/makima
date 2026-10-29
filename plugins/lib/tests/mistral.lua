local mistral = require("maki.mistral")
local messages = {
  { role = "assistant", content = "answer", reasoning_content = "thought" },
  { role = "assistant", content = "", reasoning_content = "only" },
  { role = "assistant", content = "unchanged" },
}
mistral.convert_messages(messages)
assert(messages[1].reasoning_content == nil)
assert(messages[1].content[1].thinking[1].text == "thought")
assert(messages[1].content[2].text == "answer")
assert(#messages[2].content == 1 and messages[2].content[1].thinking[1].text == "only")
assert(messages[3].content == "unchanged")

local request = {
  model = { id = "mistral-medium-latest", thinking = { mode = "adaptive" } },
  messages = {},
  thinking = { mode = "effort", level = "low" },
}
local body = assert(mistral.build_request(request))
assert(body.reasoning_effort == "high" and body.stream_options.include_usage)
request.thinking = { mode = "off" }
assert(mistral.build_request(request).reasoning_effort == nil)
request.model.id = "ministral-14b-latest"
request.thinking = { mode = "adaptive" }
assert(mistral.build_request(request).reasoning_effort == nil)

local saved
local ctx = {
  credentials = {
    read = function()
      return saved
    end,
    write = function(_, _, value)
      saved = value
      return true
    end,
    delete = function()
      saved = nil
      return true
    end,
  },
}
local config = { api_key_env = "MAKI_MISTRAL_TEST_MISSING_KEY", api_key = "config-key" }
assert(mistral.login(ctx, {
  input = function()
    return "saved-key"
  end,
}))
assert(mistral.auth_status(ctx, config).configured)
local instance = assert(mistral.prepare(ctx, request.model, config))
assert(instance.key_count == 1)
assert(saved.api_key == "saved-key")
assert(mistral.logout(ctx) and saved == nil)
local empty, err = mistral.login(ctx, {
  input = function()
    return "  "
  end,
})
assert(not empty and err.kind == "config")
assert(mistral.auth_status(ctx, config).source == "config")

local response_closed = false
local response = {
  status = 200,
  read = function(self)
    if self.done then
      return ""
    end
    self.done = true
    return [[{"data":[{"id":"yes","max_context_length":8192,"capabilities":{"completion_chat":true,"reasoning":false,"vision":true}},{"id":"no","capabilities":{"completion_chat":false}},{"id":"unknown","capabilities":{"completion_chat":true}}]}]]
  end,
  close = function()
    response_closed = true
  end,
}
ctx.send = function(_, req)
  assert(req.url:match("/models$") and req.headers.Authorization == "Bearer config-key")
  return response
end
local models = assert(mistral.discover(ctx, config))
assert(response_closed and #models == 2)
assert(models[1].id == "unknown" and models[1].supports_thinking == nil and models[1].supports_vision == false)
assert(models[2].id == "yes" and models[2].context_window == 8192)
assert(models[2].supports_thinking == false and models[2].supports_vision == true)

local failure = { kind = "config", message = "credential store failed" }
ctx.credentials.read = function()
  return nil, failure
end
local denied, credential_err = mistral.prepare(ctx, request.model, config)
assert(denied == nil and credential_err == failure)

local sent = false
local stream_ctx = {
  send = function(_, wire)
    sent = true
    assert(wire.headers["x-affinity"] == nil)
    return nil, { kind = "network", message = "fixture" }
  end,
}
request.session_id = maki.json.null()
local _, stream_err = instance.stream(stream_ctx, request, function() end)
assert(sent and stream_err.kind == "network")
sent = false
request.session_id = 42
local _, invalid_err = instance.stream(stream_ctx, request, function() end)
assert(not sent and invalid_err.kind == "config" and invalid_err.message == "Invalid Mistral session ID")
