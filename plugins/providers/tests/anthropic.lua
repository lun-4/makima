local anthropic = require("providers.anthropic")
local t = require("makima.test_helpers")

local EPHEMERAL = { type = "ephemeral" }
local UNPARSED_ERROR = "upstream exploded"
local FALLBACK_MAX_TOKENS = 32000
local FALLBACK_HIGH_BUDGET = 19660

local function sse(events)
  local frames = {}
  for _, event in ipairs(events) do
    frames[#frames + 1] = "event: " .. event.type .. "\ndata: " .. assert(makima.json.encode(event)) .. "\n\n"
  end
  return t.response({ table.concat(frames) })
end

local function request(id, thinking, extra)
  local base = {
    model = { id = id, output_tokens = 16000, max_output_tokens = 64000 },
    system = "sys",
    messages = {},
    thinking = thinking or { kind = "off" },
    fast = false,
  }
  for key, value in pairs(extra or {}) do
    base[key] = value
  end
  return base
end

t.case("requires_adaptive", function()
  for id, expected in pairs({
    ["claude-opus-4-7"] = true,
    ["claude-opus-4-6"] = false,
    ["claude-sonnet-5"] = true,
    ["claude-sonnet-4-5-20250929"] = false,
    ["claude-3-5-sonnet-20241022"] = false,
    ["anthropic/claude-opus-4.7"] = true,
    ["claude-haiku-5-1m"] = true,
  }) do
    t.eq(anthropic.requires_adaptive(id), expected, id)
  end
end)

t.case("messages_cache_the_tail_and_drop_blank_text", function()
  local messages = anthropic.convert_messages({
    { role = "user", content = { { type = "text", text = "first" } } },
    {
      role = "assistant",
      content = {
        { type = "text", text = " \n" },
        { type = "tool_use", id = "t1", name = "read", input = { path = "a" } },
        { type = "thinking", thinking = "hmm", signature = "sig" },
      },
    },
    {
      role = "user",
      content = {
        { type = "tool_result", tool_use_id = "t1", content = "ok" },
        { type = "image", source = { media_type = "image/png", data = "abc" } },
      },
    },
    { role = "assistant", content = { { type = "text", text = "  " } } },
  })
  t.same(messages, {
    { role = "user", content = { { type = "text", text = "first" } } },
    {
      role = "assistant",
      content = {
        { type = "tool_use", id = "t1", name = "read", input = { path = "a" } },
        { type = "thinking", thinking = "hmm", signature = "sig" },
      },
    },
    {
      role = "user",
      content = {
        { type = "tool_result", tool_use_id = "t1", content = "ok" },
        {
          type = "image",
          source = { type = "base64", media_type = "image/png", data = "abc" },
          cache_control = EPHEMERAL,
        },
      },
    },
    { role = "assistant", content = { { type = "text", text = "(empty)", cache_control = EPHEMERAL } } },
  })
end)

t.case("build_request", function()
  local body = anthropic.build_request(request("claude-sonnet-4-5-1m", { kind = "effort", level = "high" }, {
    fast = true,
    tools = { { name = "a", input_schema = {} }, { name = "b", input_schema = {} } },
  }))
  t.eq(body.model, "claude-sonnet-4-5")
  t.eq(body.stream, true)
  t.eq(body.speed, "fast")
  t.eq(body.max_tokens, 64000)
  t.same(body.system, { { type = "text", text = "sys", cache_control = EPHEMERAL } })
  t.same(body.thinking, { type = "enabled", budget_tokens = 19200 })
  t.same(body.tools[2], { name = "b", input_schema = {}, cache_control = EPHEMERAL })
  t.eq(body.tools[1].cache_control, nil)
  t.eq(
    anthropic.beta_header(request("claude-sonnet-4-5-1m", nil, { fast = true })),
    "fast-mode-2026-02-01,context-1m-2025-08-07"
  )
  t.eq(anthropic.beta_header(request("claude-sonnet-4-5")), nil)
end)

t.case("unknown_window_falls_back", function()
  local body = anthropic.build_body(request("claude-sonnet-4-5", { kind = "effort", level = "high" }, {
    model = { id = "claude-sonnet-4-5", output_tokens = 8000 },
  }))
  t.eq(body.max_tokens, FALLBACK_MAX_TOKENS)
  t.same(body.thinking, { type = "enabled", budget_tokens = FALLBACK_HIGH_BUDGET })
end)

t.case("adaptive_models_send_effort", function()
  for _, case in ipairs({
    { { kind = "off" }, nil, nil },
    { { kind = "adaptive" }, { type = "adaptive", display = "summarized" }, nil },
    { { kind = "effort", level = "xhigh" }, { type = "adaptive", display = "summarized" }, { effort = "high" } },
    { { kind = "budget", tokens = 1500 }, { type = "adaptive", display = "summarized" }, { effort = "low" } },
  }) do
    local body = anthropic.build_body(request("claude-opus-5", case[1]))
    t.same(body.thinking, case[2], case[1].kind)
    t.same(body.output_config, case[3], case[1].kind)
  end
  t.same(anthropic.build_body(request("claude-opus-4-6", { kind = "adaptive" })).thinking, { type = "adaptive" })
end)

t.case("stream_decodes_blocks", function()
  local emit, events = t.recorder()
  local result = assert(anthropic.stream(
    sse({
      {
        type = "message_start",
        message = { usage = { input_tokens = 10, cache_creation_input_tokens = 2, cache_read_input_tokens = 3 } },
      },
      { type = "content_block_start", index = 0, content_block = { type = "thinking", thinking = "" } },
      { type = "content_block_delta", index = 0, delta = { type = "thinking_delta", thinking = "let me" } },
      { type = "content_block_delta", index = 0, delta = { type = "thinking_delta", thinking = " see" } },
      { type = "content_block_delta", index = 0, delta = { type = "signature_delta", signature = "sig" } },
      { type = "content_block_start", index = 1, content_block = { type = "redacted_thinking", data = "xyz" } },
      { type = "content_block_start", index = 2, content_block = { type = "text", text = "" } },
      { type = "content_block_delta", index = 2, delta = { type = "text_delta", text = "Hi" } },
      { type = "content_block_start", index = 3, content_block = { type = "tool_use", id = "tu", name = "bash" } },
      { type = "content_block_delta", index = 3, delta = { type = "input_json_delta", partial_json = '{"cmd":' } },
      { type = "content_block_delta", index = 3, delta = { type = "input_json_delta", partial_json = '"ls"}' } },
      { type = "content_block_stop", index = 3 },
      { type = "message_delta", delta = { stop_reason = "tool_use" }, usage = { output_tokens = 7 } },
      { type = "message_stop" },
    }),
    emit
  ))
  t.same(events, {
    { type = "thinking_delta", text = "let me" },
    { type = "thinking_delta", text = " see" },
    { type = "text_delta", text = "Hi" },
    { type = "tool_use_start", id = "tu", name = "bash" },
  })
  t.same(result, {
    message = {
      role = "assistant",
      content = {
        { type = "thinking", thinking = "let me see", signature = "sig" },
        { type = "redacted_thinking", data = "xyz" },
        { type = "text", text = "Hi" },
        { type = "tool_use", id = "tu", name = "bash", input = { cmd = "ls" } },
      },
    },
    usage = { input_tokens = 10, output_tokens = 7, cache_creation_input_tokens = 2, cache_read_input_tokens = 3 },
    stop_reason = "tool_use",
  })
end)

t.case("error_events", function()
  local _, err =
    anthropic.stream(sse({ { type = "error", error = { type = "overloaded_error", message = "busy" } } }), t.recorder())
  t.same(err, { status = 529, message = "busy", retry = "transient" })
  _, err = anthropic.stream(t.response({ "event: error\ndata: " .. UNPARSED_ERROR .. "\n\n" }), t.recorder())
  t.same(err, { status = 400, message = UNPARSED_ERROR })
  local raw = '{"message":"raw upstream error"}'
  _, err = anthropic.stream(t.response({ "event: error\ndata: " .. raw .. "\n\n" }), t.recorder())
  t.same(err, { status = 400, message = raw })
end)

t.report()
