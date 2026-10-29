local responses = require("maki.responses")
local request = {
  model = { id = "model" },
  system = "system",
  messages = {
    {
      role = "user",
      content = {
        { type = "text", text = "hello" },
        { type = "image", source = { media_type = "image/png", data = "abc" } },
        { type = "tool_result", tool_use_id = "id", content = "result" },
      },
    },
    {
      role = "assistant",
      content = {
        { type = "text", text = "a" },
        { type = "tool_use", id = "id", name = "f", input = { x = 1 } },
        { type = "text", text = "b" },
        { type = "thinking", thinking = "hidden" },
      },
    },
  },
  tools = { { name = "f", description = "test", input_schema = { type = "object" } } },
}
local body = assert(responses.build_request(request, { reasoning = { effort = "high", summary = "auto" } }))
assert(body.instructions == "system" and body.stream and not body.store and body.reasoning.effort == "high")
assert(body.input[2].content[1].image_url == "data:image/png;base64,abc")
assert(body.input[3].output == "result" and body.input[4].content[1].text == "ab")
assert(body.input[5].arguments == '{"x":1}' and body.tools[1].strict == false)
local empty = assert(responses.build_request({ model = { id = "m" }, messages = {}, tools = {} }))
assert(assert(maki.json.encode(empty)):find('"input":%[%]'))
assert(empty.tools == nil)
local bad, err =
  responses.convert_input({ { role = "assistant", content = { { type = "tool_use", input = function() end } } } })
assert(not bad and err)

local function wire(kind, value)
  return "event: " .. kind .. "\ndata: " .. assert(maki.json.encode(value)) .. "\n\n"
end
local function stream(data, emit)
  local position = 0
  return responses.stream({
    read = function()
      position = position + 1
      if position <= #data then
        return data:sub(position, position)
      end
      return ""
    end,
  }, emit or function()
    return true
  end)
end
local events = {}
local result = assert(
  stream(
    wire("response.reasoning_summary_text.delta", { delta = "one" })
      .. wire("response.reasoning_summary_part.added", {})
      .. wire("response.reasoning_text.delta", { delta = "two" })
      .. wire("response.output_text.delta", { delta = " hello" })
      .. wire(
        "response.output_item.added",
        { output_index = 2, item = { type = "function_call", call_id = "id", name = "f" } }
      )
      .. wire("response.function_call_arguments.delta", { output_index = 2, delta = '{"x":' })
      .. wire("response.function_call_arguments.delta", { output_index = 2, delta = "1}" })
      .. wire(
        "response.output_item.done",
        { output_index = 2, item = { type = "function_call", arguments = '{"x":2}' } }
      )
      .. wire("response.completed", {
        response = {
          status = "completed",
          usage = { input_tokens = 10, output_tokens = 4, input_tokens_details = { cached_tokens = 3 } },
        },
      }),
    function(event)
      events[#events + 1] = event
      return true
    end
  )
)
assert(events[2].type == "thinking_block_end" and events[4].text == "hello" and events[5].id == "id")
assert(result.message.content[1].thinking == "one" and result.message.content[2].thinking == "two")
assert(result.message.content[3].text == "hello" and result.message.content[4].input.x == 2)
assert(result.stop_reason == "tool_use" and result.usage.input == 7 and result.usage.cache_read == 3)
local incomplete = assert(stream(wire("response.incomplete", { response = {} })))
assert(incomplete.stop_reason == "max_tokens")
local failed, failure =
  stream(wire("response.failed", { response = { error = { code = "rate_limit_exceeded", message = "slow" } } }))
assert(not failed and failure.status == 429 and failure.message == "slow")
local marker = { kind = "cancelled", message = "cancelled" }
local cancelled, cancel_err = stream(wire("response.output_text.delta", { delta = "x" }), function()
  return nil, marker
end)
assert(not cancelled and cancel_err == marker)

local recovered = assert(
  stream(
    "event: response.completed\ndata: {broken\n\n"
      .. wire("response.output_item.done", { output_index = 7, item = { type = "function_call", name = "f" } })
      .. wire("response.function_call_arguments.delta", { output_index = 0, delta = '{"x":3}' })
      .. wire(
        "response.completed",
        { response = { status = maki.json.null(), usage = { input_tokens = "9", output_tokens = -1 } } }
      )
  )
)
assert(recovered.stop_reason == "tool_use" and recovered.message.content[1].input.x == 3)
assert(recovered.usage.input == 0 and recovered.usage.output == 0)
local _, failed_type = stream(wire("response.failed", { response = { error = { type = "rate_limit_error" } } }))
assert(failed_type.status == 500 and failed_type.message == "response generation failed")

local _, coded =
  stream(wire("error", { error = { type = "api_error", code = "rate_limit_exceeded", message = "slow" } }))
assert(coded.status == 429 and coded.message == "slow")
local _, numeric = stream(wire("error", { error = { code = 503, message = " " } }))
assert(numeric.status == 503 and numeric.message == "provider sent an error frame with no detail")
local _, metadata = stream(wire("error", { error = { metadata = { error_type = "invalid_api_key" } } }))
assert(metadata.status == 401 and metadata.message == "provider sent an error frame with no detail")
local _, unmapped = stream(wire("error", { error = {} }))
assert(unmapped.status == 400)
local _, malformed = stream(wire("error", { error = { type = 1 }, message = "fallback" }))
assert(malformed.status == 500 and malformed.message == "fallback")

local progress = {}
assert(
  stream(
    wire("response.in_progress", { prompt_progress = { processed = -1, total = "9", cache = 4294967297 } }),
    function(event)
      progress[#progress + 1] = event
      return true
    end
  )
)
assert(progress[1].processed == 0 and progress[1].total == 0 and progress[1].cache == 1)
