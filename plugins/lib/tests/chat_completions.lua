local chat = require("maki.chat_completions")
local request = {
  model = { id = "model", max_output_tokens = 100, turn_output_tokens = 20 },
  system = "system",
  messages = {
    {
      role = "assistant",
      content = {
        { type = "thinking", thinking = "think" },
        { type = "tool_use", id = "id", name = "search", input = { query = "x" } },
      },
    },
    {
      role = "user",
      content = {
        { type = "text", text = "first" },
        { type = "image", source = { media_type = "image/png", data = "abc" } },
        { type = "tool_result", tool_use_id = "id", content = "result", is_error = true },
        { type = "text", text = "second" },
      },
    },
  },
  tools = { { name = "search", description = "find", input_schema = { "invalid" } } },
}
local body, err =
  chat.build_request(request, { max_tokens_field = "max_completion_tokens", include_stream_usage = true })
assert(body, err)
assert(body.max_completion_tokens == 100 and body.stream_options.include_usage)
assert(body.messages[1].content == "system")
assert(body.messages[2].content == "" and body.messages[2].reasoning_content == "think")
assert(body.messages[2].tool_calls[1]["function"].arguments == '{"query":"x"}')
assert(body.messages[3].role == "tool" and body.messages[3].content == "result")
assert(body.messages[4].content[1].image_url.url == "data:image/png;base64,abc")
assert(body.messages[4].content[2].text == "first\nsecond")
local wire = assert(maki.json.decode(assert(maki.json.encode(body))))
assert(wire.messages[2].tool_calls[1]["function"].arguments == '{"query":"x"}')
assert(wire.tools[1]["function"].parameters.type == "object")
assert(assert(maki.json.encode(wire.tools[1]["function"].parameters.properties)) == "{}")
assert(body.tools[1]["function"].parameters.type == "object")
assert(next(body.tools[1]["function"].parameters.properties) == nil)

local empty =
  assert(chat.build_request({ model = { id = "m" }, messages = {}, tools = {} }, { max_tokens_field = "max_tokens" }))
assert(empty.messages[1].content == "" and empty.tools == nil and empty.stream_options == nil)
local converted = assert(chat.convert_messages({
  {
    role = "assistant",
    content = {
      { type = "text", text = "a" },
      { type = "text", text = "b" },
      { type = "redacted_thinking", data = "secret" },
    },
  },
}, ""))
assert(converted[2].content == "ab" and converted[2].reasoning_content == nil)
local result, encoding_error = chat.convert_messages(
  { { role = "assistant", content = {
    { type = "tool_use", id = "id", name = "f", input = function() end },
  } } },
  ""
)
assert(result == nil and encoding_error ~= nil)
for _, tools in ipairs({ false, "invalid", 42 }) do
  assert(#assert(chat.convert_tools(tools)) == 0)
end
assert(#assert(chat.convert_tools(nil)) == 0)
local no_tools =
  assert(chat.build_request({ model = { id = "m" }, messages = {} }, { max_tokens_field = "max_tokens" }))
assert(no_tools.tools == nil)
local missing_schema = assert(chat.convert_tools({ { name = "f", description = "test" } }))
local parameters = assert(maki.json.decode(assert(maki.json.encode(missing_schema[1]["function"].parameters))))
assert(parameters.type == "object" and assert(maki.json.encode(parameters.properties)) == "{}")

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
local response = assert(chat.stream(
  body_for({
    'data: {"choices":[{"delta":{"reasoning":"think","content":" hello","tool_calls":[{"index":0,"id":"id","function":{"name":"f","arguments":"{\\"x\\":"}}]}}]}\n\n',
    'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"42}"}}]},"finish_reason":"tool_calls"}]}\n\n',
    'data: {"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":3}}}\n\ndata: [DONE]\n\n',
  }),
  function(event)
    events[#events + 1] = event
    return true
  end
))
assert(events[1].type == "thinking_delta" and events[2].text == "hello" and events[3].id == "id")
assert(response.message.content[3].input.x == 42 and response.message.content[3].id == "id")
assert(response.usage.input == 7 and response.usage.output == 4 and response.usage.cache_read == 3)
assert(response.stop_reason == "tool_use")
local failure = { kind = "closed", message = "closed" }
local failed, emit_err = chat.stream(body_for({ 'data: {"choices":[{"delta":{"content":"x"}}]}\n\n' }), function()
  return nil, failure
end)
assert(failed == nil and emit_err == failure)

local function sse(value)
  return "data: " .. assert(maki.json.encode(value)) .. "\n\n"
end

local wire = sse({
  choices = {
    {
      delta = {
        tool_calls = {
          { index = 1, id = "second", ["function"] = { name = "two", arguments = '{"b":' } },
        },
      },
    },
  },
}) .. sse({
  choices = {
    {
      delta = {
        tool_calls = {
          { index = 0, ["function"] = { name = "one", arguments = '{"a":1}' } },
          { index = 1, id = "", ["function"] = { name = "", arguments = "2}" } },
          { index = 512, ["function"] = { name = "ignored", arguments = "{}" } },
        },
      },
      finish_reason = "length",
    },
  },
}) .. sse({
  choices = {},
  usage = {
    prompt_tokens = 4,
    completion_tokens = 2,
    prompt_tokens_details = { cached_tokens = 3 },
    prompt_cache_hit_tokens = 5,
  },
}) .. "data: [DONE]\n\n"
local chunks = {}
for i = 1, #wire, 7 do
  chunks[#chunks + 1] = wire:sub(i, i + 6)
end
local starts = {}
local interleaved = assert(chat.stream(body_for(chunks), function(value)
  starts[#starts + 1] = value
  return true
end))
local first, second = interleaved.message.content[1], interleaved.message.content[2]
assert(#interleaved.message.content == 2 and first.name == "one" and first.input.a == 1)
assert(second.name == "two" and second.id == "second" and second.input.b == 2)
assert(#starts == 2 and starts[1].id == second.id and starts[2].id == first.id)
assert(interleaved.stop_reason == "max_tokens")
assert(interleaved.usage.input == 0 and interleaved.usage.cache_read == 5 and interleaved.usage.output == 2)

local malformed, malformed_err = chat.stream(
  body_for({
    sse({
      choices = {
        {
          message = {
            tool_calls = {
              { index = 0, ["function"] = { name = "broken", arguments = "{" } },
            },
          },
        },
      },
    }),
  }),
  function()
    return true
  end
)
assert(malformed == nil and malformed_err.kind == "api")
assert(malformed_err.message == "Provider returned invalid JSON tool arguments")

local non_streaming = assert(chat.stream(
  body_for({
    sse({
      choices = { { message = { tool_calls = { { id = "full", ["function"] = { name = "f", arguments = "{}" } } } } } },
    }),
  }),
  function()
    return true
  end
))
assert(non_streaming.message.content[1].id == "full" and non_streaming.message.content[1].name == "f")

local api_result, api_err = chat.stream(body_for({ sse({ error = { message = "upstream failed" } }) }), function()
  return true
end)
assert(api_result == nil and api_err.kind == "api" and api_err.message == "upstream failed")
local recovery = assert(chat.stream(
  body_for({
    "data: {broken\n\n"
      .. sse({
        choices = {
          {
            delta = {
              content = "ok",
              tool_calls = {
                { index = 0, ["function"] = { name = "f", arguments = '{"items":[null,1],"key":null}' } },
                { index = 1, ["function"] = { name = "g", arguments = "null" } },
              },
            },
          },
        },
      })
      .. sse({ choices = { { delta = { tool_calls = { { index = 0, id = "late" } } } } } }),
  }),
  function()
    return true
  end
))
assert(recovery.message.content[1].text == "ok")
assert(recovery.message.content[2].id == "late")
assert(recovery.message.content[2].input.key == maki.json.null())
assert(recovery.message.content[2].input.items[1] == maki.json.null())
assert(recovery.message.content[3].input == maki.json.null())
for _, detail in ipairs({
  { code = 429 },
  { type = "rate_limit_error" },
  { metadata = { error_type = "rate_limit_exceeded" } },
}) do
  local _, status_err = chat.stream(body_for({ sse({ error = detail }) }), function()
    return true
  end)
  assert(status_err.status == 429 and status_err.kind == "api")
end
local structured = { kind = "timeout", message = "timed out" }
local _, structured_err = chat.stream({
  read = function()
    return nil, structured
  end,
}, function()
  return true
end)
assert(structured_err == structured)
local broken_body = {
  read = function()
    return nil, "connection lost"
  end,
}
local network_result, network_err = chat.stream(broken_body, function()
  return true
end)
assert(network_result == nil and network_err.kind == "network" and network_err.message == "connection lost")
local _, closed_err = chat.stream({
  read = function()
    return nil, "response is closed"
  end,
}, function()
  return true
end)
assert(closed_err.kind == "closed" and closed_err.message == "response is closed")

for _, delta in ipairs({
  { reasoning = "thinking" },
  { content = "text" },
  { tool_calls = { { index = 0, ["function"] = { name = "f" } } } },
}) do
  local reads = 0
  local stream_body = {
    read = function()
      reads = reads + 1
      assert(reads == 1)
      return sse({ choices = { { delta = delta } } })
    end,
  }
  local cancelled = { kind = "cancelled", message = "cancelled" }
  local value, emit_failure = chat.stream(stream_body, function()
    return nil, cancelled
  end)
  assert(value == nil and emit_failure == cancelled and reads == 1)
end
