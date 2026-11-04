local gemini = require("providers.gemini")
local t = require("maki.test_helpers")

local function sse(chunks)
  local frames = {}
  for _, chunk in ipairs(chunks) do
    frames[#frames + 1] = "data: " .. assert(maki.json.encode(chunk)) .. "\r\n\r\n"
  end
  return t.response({ table.concat(frames) })
end

local function parts(...)
  return { candidates = { { content = { parts = { ... } } } } }
end

t.case("convert_messages", function()
  local contents = gemini.convert_messages({
    {
      role = "assistant",
      content = {
        { type = "thinking", thinking = "hmm", signature = "s1" },
        { type = "tool_use", id = "t1", name = "read", input = { path = "a" }, thought_signature = "s2" },
        { type = "tool_use", id = "t2", name = "bash", input = { cmd = "ls" } },
      },
    },
    {
      role = "user",
      content = {
        { type = "tool_result", tool_use_id = "t1", content = '{"lines":3}' },
        { type = "tool_result", tool_use_id = "t2", content = "denied", is_error = true },
        { type = "image", source = { media_type = "image/png", data = "abc" } },
      },
    },
  })
  t.same(contents, {
    {
      role = "model",
      parts = {
        { text = "hmm", thought = true, thoughtSignature = "s1" },
        { functionCall = { name = "read", args = { path = "a" } }, thoughtSignature = "s2" },
        { functionCall = { name = "bash", args = { cmd = "ls" } } },
      },
    },
    {
      role = "user",
      parts = {
        { functionResponse = { name = "read", response = { lines = 3 } } },
        { functionResponse = { name = "bash", response = { error = { result = "denied" } } } },
      },
    },
    { role = "user", parts = { { inlineData = { mimeType = "image/png", data = "abc" } } } },
  })
end)

t.case("build_request", function()
  local body = gemini.build_request({
    model = { id = "gemini-2.5-flash", output_tokens = 8000, max_output_tokens = 65536 },
    system = "sys",
    messages = {},
    thinking = { kind = "effort", level = "max" },
    tools = {
      {
        name = "read",
        description = "r",
        input_schema = { type = "object", additionalProperties = false, properties = { p = { type = "string" } } },
      },
      { name = "noop", description = "n", input_schema = { type = "object", properties = {} } },
      { name = "hidden", input_schema = { type = "object", properties = {} } },
    },
  })
  t.same(body, {
    contents = {},
    systemInstruction = { parts = { { text = "sys" } } },
    generationConfig = { maxOutputTokens = 65536, thinkingConfig = { thinkingBudget = 24576, includeThoughts = true } },
    tools = {
      {
        functionDeclarations = {
          {
            name = "read",
            description = "r",
            parameters = { type = "object", properties = { p = { type = "string" } } },
          },
          { name = "noop", description = "n" },
          { name = "hidden", description = "" },
        },
      },
    },
  })
  local adaptive =
    gemini.build_request({ model = { id = "gemini-2.5-pro" }, messages = {}, thinking = { kind = "adaptive" } })
  t.same(adaptive.generationConfig, { thinkingConfig = { includeThoughts = true } })
end)

t.case("thinking_budget_scales_off_the_declared_window", function()
  local body = gemini.build_request({
    model = { id = "gemini-2.5-pro", output_tokens = 4000, max_output_tokens = 8192 },
    messages = {},
    thinking = { kind = "effort", level = "max" },
  })
  t.same(
    body.generationConfig,
    { maxOutputTokens = 8192, thinkingConfig = { thinkingBudget = 4096, includeThoughts = true } }
  )
end)

t.case("stream_merges_parts_and_ends_thinking", function()
  local emit, events = t.recorder()
  local result = assert(gemini.stream(
    sse({
      parts({ text = "plan", thought = true }),
      parts({ text = "ned", thought = true, thoughtSignature = "sig" }),
      parts({ text = "Hello" }, { text = " there" }),
      parts({ functionCall = { name = "read", args = { path = "a" } }, thoughtSignature = "fsig" }),
      {
        candidates = { { finishReason = "STOP" } },
        usageMetadata = {
          promptTokenCount = 10,
          cachedContentTokenCount = 4,
          candidatesTokenCount = 3,
          thoughtsTokenCount = 2,
        },
      },
    }),
    emit
  ))
  local call = result.message.content[3]
  assert(call.id:match("^call_read_%x+$"), call.id)
  t.same(events, {
    { type = "thinking_delta", text = "plan" },
    { type = "thinking_delta", text = "ned" },
    { type = "thinking_block_end" },
    { type = "text_delta", text = "Hello" },
    { type = "text_delta", text = " there" },
    { type = "tool_use_start", id = call.id, name = "read" },
  })
  t.same(result, {
    message = {
      role = "assistant",
      content = {
        { type = "thinking", thinking = "planned", signature = "sig" },
        { type = "text", text = "Hello there" },
        { type = "tool_use", id = call.id, name = "read", input = { path = "a" }, thought_signature = "fsig" },
      },
    },
    usage = { input_tokens = 6, output_tokens = 5, cache_creation_input_tokens = 0, cache_read_input_tokens = 4 },
    stop_reason = "end_turn",
  })
end)

t.case("tool_results_are_always_objects_and_unknown_names_fall_back", function()
  local function response(content)
    local contents = gemini.convert_messages({
      { role = "user", content = { { type = "tool_result", tool_use_id = "missing", content = content } } },
    })
    return contents[1].parts[1].functionResponse
  end
  t.same(response("not json"), { name = "unknown", response = { result = "not json" } })
  t.same(response('"a json string"').response, { result = "a json string" })
  t.same(response("42").response, { result = 42 })
  t.same(response("null").response, { result = "null" })
  t.same(response("[1]").response, { result = { 1 } })
  t.same(response('{"out":"ok"}').response, { out = "ok" })
end)

t.case("pasted_images_stay_in_their_turn", function()
  local contents = gemini.convert_messages({
    {
      role = "user",
      content = {
        { type = "text", text = "what is this?" },
        { type = "image", source = { media_type = "image/webp", data = "abc" } },
      },
    },
  })
  t.same(contents, {
    {
      role = "user",
      parts = { { text = "what is this?" }, { inlineData = { mimeType = "image/webp", data = "abc" } } },
    },
  })
end)

t.case("additional_properties_are_stripped_at_every_depth", function()
  local tools = gemini.convert_tools({
    {
      name = "t",
      input_schema = {
        type = "object",
        additionalProperties = false,
        properties = {
          inner = { type = "object", additionalProperties = false, properties = { x = { type = "number" } } },
          list = { type = "array", items = { type = "string", additionalProperties = false } },
        },
      },
    },
  })
  t.same(tools[1], {
    name = "t",
    description = "",
    parameters = {
      type = "object",
      properties = {
        inner = { type = "object", properties = { x = { type = "number" } } },
        list = { type = "array", items = { type = "string" } },
      },
    },
  })
end)

t.case("null_stream_event_is_ignored", function()
  local result = assert(gemini.stream(t.response({ "data: null\n\n" }), t.recorder()))
  t.same(result.message.content, {})
end)

t.case("stream_call_ids_signatures_and_max_tokens", function()
  local call = { functionCall = { name = "bash", args = { cmd = "ls" }, thoughtSignature = "nested" } }
  local result = assert(
    gemini.stream(
      sse({ parts(call, { functionCall = { name = "bash" } }), { candidates = { { finishReason = "MAX_TOKENS" } } } }),
      t.recorder()
    )
  )
  local first, second = result.message.content[1], result.message.content[2]
  t.eq(first.thought_signature, "nested")
  t.same(second.input, {})
  assert(first.id ~= second.id, "parallel same-name calls need distinct ids")
  t.eq(result.stop_reason, "max_tokens")
end)

t.report()
