local chat = require("providers.chat_completions")
local t = require("maki.test_helpers")

local OVERLOADED = "server overloaded"

local function sse(chunks)
  local frames = {}
  for _, chunk in ipairs(chunks) do
    frames[#frames + 1] = "data: " .. (type(chunk) == "string" and chunk or assert(maki.json.encode(chunk))) .. "\n\n"
  end
  return t.response({ table.concat(frames) })
end

t.case("build_request", function()
  local body = chat.build_request({
    model = { id = "m", output_tokens = 100, max_output_tokens = 4096 },
    system = "sys",
    messages = {
      {
        role = "assistant",
        content = {
          { type = "thinking", thinking = "think" },
          { type = "text", text = "said" },
          { type = "tool_use", id = "c1", name = "search", input = { query = "x" } },
        },
      },
      {
        role = "user",
        content = {
          { type = "tool_result", tool_use_id = "c1", content = "found" },
          { type = "text", text = "first" },
          { type = "image", source = { media_type = "image/png", data = "abc" } },
          { type = "text", text = "second" },
        },
      },
    },
    tools = {
      { name = "search", description = "find", input_schema = maki.json.decode('["x"]') },
      { name = "empty", description = "", input_schema = { type = "object" } },
      { input_schema = { type = "object" } },
    },
  }, { max_tokens_field = "max_completion_tokens", include_stream_usage = true })
  t.same(body, {
    model = "m",
    stream = true,
    max_completion_tokens = 4096,
    stream_options = { include_usage = true },
    messages = {
      { role = "system", content = "sys" },
      {
        role = "assistant",
        content = "said",
        reasoning_content = "think",
        tool_calls = {
          { id = "c1", type = "function", ["function"] = { name = "search", arguments = '{"query":"x"}' } },
        },
      },
      { role = "tool", tool_call_id = "c1", content = "found" },
      {
        role = "user",
        content = {
          { type = "image_url", image_url = { url = "data:image/png;base64,abc" } },
          { type = "text", text = "first\nsecond" },
        },
      },
    },
    tools = {
      {
        type = "function",
        ["function"] = { name = "search", description = "find", parameters = { type = "object", properties = {} } },
      },
      {
        type = "function",
        ["function"] = { name = "empty", description = "", parameters = { type = "object" } },
      },
    },
  })
end)

t.case("stream_text_reasoning_and_usage", function()
  local emit, events = t.recorder()
  local result = assert(chat.stream(
    sse({
      { choices = { { delta = { reasoning_content = "hmm" } } } },
      { choices = { { delta = { content = "  Hello" } } } },
      { choices = { { delta = { content = " world" }, finish_reason = "stop" } } },
      { usage = { prompt_tokens = 10, completion_tokens = 4, prompt_tokens_details = { cached_tokens = 3 } } },
      "[DONE]",
    }),
    emit
  ))
  t.same(events, {
    { type = "thinking_delta", text = "hmm" },
    { type = "text_delta", text = "Hello" },
    { type = "text_delta", text = " world" },
  })
  t.same(result, {
    message = {
      role = "assistant",
      content = { { type = "thinking", thinking = "hmm" }, { type = "text", text = "Hello world" } },
    },
    usage = { input_tokens = 7, output_tokens = 4, cache_creation_input_tokens = 0, cache_read_input_tokens = 3 },
    stop_reason = "end_turn",
  })
end)

t.case("done_marker_allows_whitespace", function()
  local result = assert(chat.stream(t.response({ "data: [DONE]  \n\n" }), t.recorder()))
  t.same(result.message.content, {})
end)

t.case("stream_tool_calls_by_wire_index", function()
  local emit, events = t.recorder()
  local result = assert(chat.stream(
    sse({
      { choices = { { delta = { tool_calls = { { index = 2, id = "b", ["function"] = { name = "bash" } } } } } } },
      { choices = { { delta = { tool_calls = { { index = 0, id = "a", ["function"] = { name = "read" } } } } } } },
      { choices = { { delta = { tool_calls = { { index = 2, ["function"] = { arguments = '{"cmd":' } } } } } } },
      { choices = { { delta = { tool_calls = { { index = 2, ["function"] = { arguments = '"ls"}' } } } } } } },
      { choices = { { delta = { tool_calls = { { index = 0, ["function"] = { arguments = "{broken" } } } } } } },
      { choices = { { finish_reason = "tool_calls", delta = {} } } },
    }),
    emit
  ))
  t.same(events, {
    { type = "tool_use_start", id = "b", name = "bash" },
    { type = "tool_use_start", id = "a", name = "read" },
  })
  t.same(result.message.content, {
    { type = "tool_use", id = "a", name = "read", input = {} },
    { type = "tool_use", id = "b", name = "bash", input = { cmd = "ls" } },
  })
  t.eq(result.stop_reason, "tool_use")
end)

t.case("unnamed_tool_call_gets_minted_id", function()
  local result = assert(
    chat.stream(
      sse({ { choices = { { message = { tool_calls = { { ["function"] = { name = "x", arguments = "{}" } } } } } } } }),
      t.recorder()
    )
  )
  local call = result.message.content[1]
  t.eq(call.name, "x")
  assert(call.id:match("^call_%x+$"), call.id)
end)

t.case("error_frame_is_classified", function()
  for _, case in ipairs({
    { { code = "server_is_overloaded", message = OVERLOADED }, 529, "transient" },
    { { code = 503, message = OVERLOADED }, 503, "transient" },
    { { type = "rate_limit_error", message = OVERLOADED }, 429, "rate_limit" },
    { { metadata = { error_type = "invalid_api_key" }, message = OVERLOADED }, 401 },
    { { code = "unknown", message = OVERLOADED }, 400 },
  }) do
    local result, err = chat.stream(sse({ { error = case[1] } }), t.recorder())
    t.eq(result, nil)
    t.same(err, { status = case[2], message = OVERLOADED, retry = case[3] })
  end
  local _, err = chat.stream(sse({ { error = { message = " " } } }), t.recorder())
  t.same(err, { status = 400, message = "provider sent an error frame with no detail" })
end)

t.report()
