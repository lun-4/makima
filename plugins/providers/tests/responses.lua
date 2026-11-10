local responses = require("providers.responses")
local t = require("makima.test_helpers")

local function sse(events)
  local frames = {}
  for _, event in ipairs(events) do
    frames[#frames + 1] = "event: " .. event.type .. "\ndata: " .. assert(makima.json.encode(event)) .. "\n\n"
  end
  return t.response({ table.concat(frames) })
end

local function completed(status, usage)
  return { type = "response.completed", response = { status = status, usage = usage } }
end

t.case("build_request", function()
  local body = responses.build_request({
    model = { id = "gpt", output_tokens = 100, max_output_tokens = 4096 },
    system = "sys",
    messages = {
      {
        role = "user",
        content = {
          { type = "text", text = "hi" },
          { type = "image", source = { media_type = "image/png", data = "abc" } },
          { type = "tool_result", tool_use_id = "c0", content = "out" },
        },
      },
      {
        role = "assistant",
        content = {
          { type = "thinking", thinking = "dropped" },
          { type = "text", text = "ok" },
          { type = "tool_use", id = "c1", name = "f", input = { a = 1 } },
        },
      },
    },
    tools = { { name = "f", description = "d", input_schema = { type = "object" } } },
  }, { effort = "high" })
  t.same(body, {
    model = "gpt",
    instructions = "sys",
    stream = true,
    store = false,
    reasoning = { effort = "high" },
    input = {
      { type = "message", role = "user", content = { { type = "input_text", text = "hi" } } },
      {
        type = "message",
        role = "user",
        content = { { type = "input_image", image_url = "data:image/png;base64,abc" } },
      },
      { type = "function_call_output", call_id = "c0", output = "out" },
      { type = "message", role = "assistant", content = { { type = "output_text", text = "ok" } } },
      { type = "function_call", call_id = "c1", name = "f", arguments = '{"a":1}' },
    },
    tools = { { type = "function", name = "f", description = "d", parameters = { type = "object" }, strict = false } },
  })
  t.eq(assert(makima.json.encode(responses.build_request({ model = { id = "m" }, messages = {} }).input)), "[]")
end)

t.case("stream_reasoning_text_and_tools", function()
  local emit, events = t.recorder()
  local result = assert(responses.stream(
    sse({
      { type = "response.in_progress", prompt_progress = { processed = 5, total = 10, cache = 2 } },
      { type = "response.reasoning_summary_text.delta", delta = "one" },
      { type = "response.reasoning_summary_part.added" },
      { type = "response.reasoning_summary_text.delta", delta = "two" },
      { type = "response.output_text.delta", delta = " Hi" },
      {
        type = "response.output_item.added",
        output_index = 3,
        item = { type = "function_call", call_id = "c", name = "f" },
      },
      { type = "response.function_call_arguments.delta", output_index = 3, delta = '{"x":' },
      { type = "response.function_call_arguments.delta", output_index = 3, delta = "1}" },
      completed("completed", { input_tokens = 9, output_tokens = 3, input_tokens_details = { cached_tokens = 4 } }),
    }),
    emit
  ))
  t.same(events, {
    { type = "prompt_progress", processed = 5, total = 10, cache = 2 },
    { type = "thinking_delta", text = "one" },
    { type = "thinking_block_end" },
    { type = "thinking_delta", text = "two" },
    { type = "text_delta", text = "Hi" },
    { type = "tool_use_start", id = "c", name = "f" },
  })
  t.same(result, {
    message = {
      role = "assistant",
      content = {
        { type = "thinking", thinking = "one" },
        { type = "thinking", thinking = "two" },
        { type = "text", text = "Hi" },
        { type = "tool_use", id = "c", name = "f", input = { x = 1 } },
      },
    },
    usage = { input_tokens = 5, output_tokens = 3, cache_creation_input_tokens = 0, cache_read_input_tokens = 4 },
    stop_reason = "tool_use",
  })
end)

t.case("output_item_done_carries_full_arguments", function()
  local result = assert(responses.stream(
    sse({
      {
        type = "response.output_item.done",
        output_index = 0,
        item = { type = "function_call", call_id = "c", name = "f", arguments = '{"y":2}' },
      },
      { type = "response.incomplete", response = {} },
    }),
    t.recorder()
  ))
  t.same(result.message.content, { { type = "tool_use", id = "c", name = "f", input = { y = 2 } } })
  t.eq(result.stop_reason, "max_tokens")
end)

t.case("failed_response_maps_its_code", function()
  local _, err = responses.stream(
    sse({ { type = "response.failed", response = { error = { code = "rate_limit_exceeded", message = "slow down" } } } }),
    t.recorder()
  )
  t.same(err, { status = 429, message = "slow down", retry = "rate_limit" })
  _, err = responses.stream(sse({ { type = "response.failed", response = {} } }), t.recorder())
  t.same(err, { status = 500, message = "response generation failed", retry = "transient" })
end)

t.report()
