local gemini = require("maki.gemini")
local schema = {
  type = "object",
  additionalProperties = false,
  properties = { x = { type = "object", additionalProperties = false, properties = { y = { type = "string" } } } },
}
local body = assert(gemini.build_request({
  model = { id = "gemini-flash", max_output_tokens = 10000 },
  system = "system",
  thinking = { mode = "effort", level = "high" },
  tools = { { name = "f", description = "desc", input_schema = schema }, { name = "empty", description = "" } },
  messages = {
    {
      role = "assistant",
      content = {
        { type = "thinking", thinking = "thought", signature = "sig" },
        { type = "tool_use", id = "id", name = "f", input = { x = 1 }, thought_signature = "call-sig" },
        { type = "redacted_thinking", data = "hidden" },
      },
    },
    {
      role = "user",
      content = {
        { type = "image", source = { media_type = "image/png", data = "abc" } },
        { type = "tool_result", tool_use_id = "id", content = '{"ok":true}', is_error = true },
        { type = "tool_result", tool_use_id = "missing", content = "null" },
        { type = "tool_result", tool_use_id = "missing", content = "plain" },
      },
    },
  },
}))
assert(body.systemInstruction.parts[1].text == "system")
assert(body.generationConfig.thinkingConfig.thinkingBudget == 3000)
assert(body.generationConfig.maxOutputTokens == 10000)
assert(body.contents[1].role == "model" and body.contents[1].parts[1].thoughtSignature == "sig")
assert(body.contents[1].parts[2].thoughtSignature == "call-sig")
assert(body.contents[2].parts[1].functionResponse.name == "f")
assert(body.contents[2].parts[1].functionResponse.response.error.ok)
assert(body.contents[2].parts[2].functionResponse.name == "unknown")
assert(assert(maki.json.encode(body.contents[2].parts[2].functionResponse.response)) == '{"result":null}')
assert(body.contents[2].parts[3].functionResponse.response.result == "plain")
assert(body.contents[3].parts[1].inlineData.mimeType == "image/png")
assert(body.tools[1].functionDeclarations[1].parameters.properties.x.additionalProperties == nil)
assert(schema.properties.x.additionalProperties == false)
assert(body.tools[1].functionDeclarations[2].parameters == nil)
local empty = assert(gemini.build_request({ model = { id = "m" }, messages = {}, thinking = { mode = "off" } }))
assert(assert(maki.json.encode(empty.contents)) == "[]")
assert(empty.generationConfig == nil and empty.systemInstruction == nil and empty.tools == nil)
local adaptive = assert(gemini.build_request({ model = { id = "m" }, messages = {}, thinking = { mode = "adaptive" } }))
assert(adaptive.generationConfig.thinkingConfig.includeThoughts)
assert(adaptive.generationConfig.thinkingConfig.thinkingBudget == nil)
local capped = assert(
  gemini.build_request({ model = { id = "flash" }, messages = {}, thinking = { mode = "budget", tokens = 99999 } })
)
assert(capped.generationConfig.thinkingConfig.thinkingBudget == 24576)

local tools = assert(gemini.convert_tools({
  { name = "missing_description" },
  { name = "non_string_description", description = false },
  {
    name = "nested",
    description = "",
    input_schema = {
      type = "object",
      properties = { items = { type = "array", items = { type = "object", additionalProperties = false } } },
    },
  },
}))
assert(#tools == 2 and tools[1].name == "non_string_description" and tools[1].description == "")
assert(tools[2].parameters.properties.items.items.additionalProperties == nil)

local results = gemini.convert_messages({
  {
    role = "user",
    content = {
      { type = "tool_result", tool_use_id = "id", content = "[1,2]", is_error = false },
      { type = "tool_result", tool_use_id = "id", content = "42", is_error = true },
    },
  },
})
assert(assert(maki.json.encode(results[1].parts[1].functionResponse.response)) == '{"result":[1,2]}')
assert(results[1].parts[2].functionResponse.response.error.result == 42)

local empty_schemas = assert(gemini.convert_tools({
  { name = "empty_object", description = "", input_schema = { type = "object", properties = {} } },
  {
    name = "empty_array",
    description = "",
    input_schema = { type = "object", properties = assert(maki.json.decode("[]")) },
  },
  { name = "missing", description = "", input_schema = { type = "object" } },
}))
for _, tool in ipairs(empty_schemas) do
  assert(tool.parameters == nil)
end

local function body_for(chunks)
  local index = 0
  return {
    read = function()
      index = index + 1
      return chunks[index] or ""
    end,
  }
end
local events = {}
local response = assert(gemini.stream(
  body_for({
    'data: {"candidates":[{"content":{"parts":[{"text":"a","thought":true},{"text":"b","thought":true,"thoughtSignature":"sig"},{"text":"hello"}]}}]}\n\n',
    'data: {"candidates":[{"content":{"parts":[{"text":" world"},{"functionCall":{"name":"f","args":{"x":1}},"thoughtSignature":"call-sig"}]}}],"usageMetadata":{"promptTokenCount":10,"cachedContentTokenCount":3,"candidatesTokenCount":4,"thoughtsTokenCount":2}}\n\n',
  }),
  function(event)
    events[#events + 1] = event
    return true
  end
))
assert(events[1].type == "thinking_delta" and events[3].type == "thinking_block_end")
assert(events[4].type == "text_delta" and events[6].type == "tool_use_start")
assert(response.message.content[1].thinking == "ab" and response.message.content[1].signature == "sig")
assert(response.message.content[2].text == "hello world")
assert(response.message.content[3].input.x == 1 and response.message.content[3].thought_signature == "call-sig")
assert(response.message.content[3].id == events[6].id and response.stop_reason == "tool_use")
assert(response.usage.input == 7 and response.usage.output == 6 and response.usage.cache_read == 3)
local failure = { kind = "closed", message = "closed" }
local failed, err = gemini.stream(
  body_for({ 'data: {"candidates":[{"content":{"parts":[{"text":"x"}]}}]}\n\n' }),
  function()
    return nil, failure
  end
)
assert(failed == nil and err == failure)
local finished = assert(gemini.stream(
  body_for({
    'data: invalid\n\ndata: {"candidates":[{"finishReason":"MAX_TOKENS"}]}\n\n',
  }),
  function()
    return true
  end
))
assert(finished.stop_reason == "max_tokens")

assert(#gemini.models == 3)
local getenv = maki.uv.os_getenv
maki.uv.os_getenv = function(name)
  if name == "GEMINI_API_KEY" then
    return "first, second"
  end
end
local instance = assert(gemini.prepare(nil, { id = "gemini-2.5-pro" }, {
  api_key = "config-key",
  base_url = "http://localhost:8080/v1/",
}))
local sent, closed
local ctx = {
  send = function(_, request)
    sent = request
    local response_body =
      body_for({ 'data: {"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}\n\n' })
    response_body.status = 200
    response_body.close = function()
      closed = true
    end
    return response_body
  end,
}
local request = { model = { id = "gemini/test" }, messages = {}, thinking = { mode = "off" } }
local streamed = assert(instance.stream(ctx, request, function()
  return true
end))
assert(sent.url == "http://localhost:8080/v1/models/gemini%2Ftest:streamGenerateContent?alt=sse")
assert(sent.headers["x-goog-api-key"] == "first" and closed)
assert(streamed.message.content[1].text == "ok")
assert(instance.rotate_key())
assert(instance.stream(ctx, request, function()
  return true
end))
assert(sent.headers["x-goog-api-key"] == "second")
maki.uv.os_getenv = function(name)
  if name == "GEMINI_API_KEY" then
    return "replacement"
  end
end
assert(instance.reload_auth())
assert(instance.stream(ctx, request, function()
  return true
end))
assert(sent.headers["x-goog-api-key"] == "replacement")
local error_ctx = {
  send = function()
    local result = body_for({ '{"error":{"message":"too many tokens"}}' })
    result.status = 429
    result.headers = { ["retry-after"] = "3" }
    result.close = function()
      closed = true
    end
    return result
  end,
}
closed = false
local failed_request, api_err = instance.stream(error_ctx, request, function()
  return true
end)
assert(not failed_request and api_err.status == 429 and api_err.retry_after_ms == 3000)
assert(api_err.message == '{"error":{"message":"too many tokens"}}' and closed)
local oversized_ctx = {
  send = function()
    local result = body_for({ string.rep("x", 65536), "y" })
    result.status = 400
    result.close = function()
      closed = true
    end
    return result
  end,
}
closed = false
local _, oversized = instance.stream(oversized_ctx, request, function()
  return true
end)
assert(oversized.message == "Google error response too large" and closed)

maki.uv.os_getenv = function(name)
  if name == "GEMINI_API_KEY" then
    return " , "
  end
end
local saved_key = "saved-key"
local auth_ctx = {
  credentials = {
    read = function(_, key)
      assert(key == "api_key")
      return saved_key and { api_key = saved_key }
    end,
    write = function(_, _, value)
      saved_key = value.api_key
      return true
    end,
    delete = function()
      saved_key = nil
      return true
    end,
  },
}
local saved = assert(gemini.prepare(auth_ctx, { id = "m" }, { api_key = "config-key" }))
assert(saved.key_count == 1)
assert(saved.stream(ctx, request, function()
  return true
end))
assert(sent.headers["x-goog-api-key"] == "saved-key")
saved_key = "changed-key"
assert(saved.reload_auth(auth_ctx))
assert(saved.stream(ctx, request, function()
  return true
end))
assert(sent.headers["x-goog-api-key"] == "changed-key")
assert(gemini.auth_status(auth_ctx, {}).source == "stored")
assert(gemini.login(auth_ctx, {
  input = function()
    return " login-key "
  end,
}))
assert(saved_key == "login-key")
assert(gemini.logout(auth_ctx) and saved_key == nil)
local configured = assert(gemini.prepare(nil, { id = "m" }, { api_key = "config-key" }))
assert(configured.stream(ctx, request, function()
  return true
end))
assert(sent.headers["x-goog-api-key"] == "config-key")
local discovery_ctx = {
  send = function(_, outgoing)
    sent = outgoing
    local response = body_for({
      '{"models":[{"name":"models/z","supportedGenerationMethods":["generateContent"]},{"name":"models/a","supportedGenerationMethods":["generateContent"]},{"name":"models/no","supportedGenerationMethods":["embedContent"]}]}',
    })
    response.status = 200
    response.close = function()
      closed = true
    end
    return response
  end,
}
closed = false
local discovered =
  assert(gemini.discover(discovery_ctx, { api_key = "config-key", base_url = "http://localhost:8080/v1" }))
assert(#discovered == 2 and discovered[1].id == "a" and discovered[2].id == "z")
assert(sent.url == "http://localhost:8080/v1/models?key=config-key&pageSize=1000" and closed)
maki.uv.os_getenv = getenv
