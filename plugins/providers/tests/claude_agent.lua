local claude_agent = require("providers.claude_agent")
local t = require("maki.test_helpers")

local SOCKET = "/run/user/1000/bridge.sock"
local CONFIG = { socket = SOCKET }
local CONNECT_FAILED = "could not connect to claude-agent: connection refused"
local REQUEST = {
  model = { id = "sonnet" },
  messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
  invocation = { conversation_id = "c1", operation_id = "o1", attempt = 0, purpose = "agent" },
}
local DONE = {
  message = { role = "assistant", content = { { type = "text", text = "hello" } } },
  usage = { input_tokens = 3, output_tokens = 1, cache_creation_input_tokens = 0, cache_read_input_tokens = 0 },
  stop_reason = "end_turn",
}

local spec = claude_agent.provider()

local function frame(event, data)
  return "event: " .. event .. "\ndata: " .. assert(maki.json.encode(data)) .. "\n\n"
end

local function json(value, status)
  return t.response({ assert(maki.json.encode(value)) }, { status = status })
end

local function failing_ctx(err)
  local ctx = t.provider_ctx({})
  ctx.send = function(request)
    ctx.requests[#ctx.requests + 1] = request
    return nil, err
  end
  return ctx
end

local function stream(responses, emit)
  local ctx = t.provider_ctx(responses)
  local instance = assert(spec.prepare(ctx, { id = "sonnet" }, CONFIG))
  local result, err = instance.stream(ctx, REQUEST, emit or t.recorder())
  return ctx, result, err
end

t.case("socket_comes_from_config", function()
  t.eq(claude_agent.socket(CONFIG), SOCKET)
  local default = claude_agent.socket({ socket = "" })
  if default then
    t.has(default, "claude-agent/bridge.sock")
  end
end)

t.case("aliases_are_named_by_the_model_they_resolve_to", function()
  t.eq(claude_agent.describe({ value = "default", resolved_model = "claude-opus-5-5" }), nil)
  t.same(
    claude_agent.describe({ value = "opus", resolved_model = "claude-opus-5-5", supports_adaptive_thinking = true }),
    {
      id = "claude-opus-5-5",
      tier = "strong",
      context_window = claude_agent.DEFAULT_CONTEXT,
      supports_thinking = true,
      supports_vision = true,
    }
  )
  local older = claude_agent.describe({ value = "claude-opus-4-8", resolved_model = "claude-opus-4-8" })
  t.eq(older.id, "claude-opus-4-8")
  t.eq(older.tier, nil)
  t.eq(claude_agent.describe({ value = "sonnet" }).id, "sonnet")
  local long = claude_agent.describe({ value = "sonnet[1m]", resolved_model = "claude-sonnet-5-5[1m]" })
  t.eq(long.context_window, claude_agent.LONG_CONTEXT)
  t.eq(long.tier, nil)
  t.eq(claude_agent.describe({ value = "haiku", supports_adaptive_thinking = false }).supports_thinking, false)
  t.eq(claude_agent.describe({ value = "opus", context_window = 1000000 }).context_window, 1000000)
end)

t.case("a_step_posts_the_request_and_streams_its_events", function()
  local emit, events = t.recorder()
  local ctx, result = stream({
    t.response({
      frame("event", { type = "text_delta", text = "hel" }),
      ": heartbeat\n\n" .. frame("event", { type = "text_delta", text = "lo" }),
      frame("done", DONE),
    }),
  }, emit)
  local sent = ctx.requests[1]
  t.eq(sent.unix_socket, SOCKET)
  t.eq(sent.method, "POST")
  t.has(sent.url, "/v1/steps")
  local body = maki.json.decode(sent.body)
  t.eq(body.protocol_version, claude_agent.PROTOCOL_VERSION)
  t.same(body.request, REQUEST)
  t.same(events, { { type = "text_delta", text = "hel" }, { type = "text_delta", text = "lo" } })
  t.same(result, DONE)
  t.eq(ctx.delivered_after, 0)
end)

t.case("the_bridge_error_table_is_returned_as_is", function()
  local failure = { status = 529, message = "overloaded", retry = "none" }
  local _, result, err = stream({ t.response({ frame("error", failure) }) })
  t.eq(result, nil)
  t.same(err, failure)
end)

t.case("a_refused_step_returns_the_bridge_table", function()
  local conflict = { message = "step o1/0 was already started", retry = "none" }
  local ctx, _, err = stream({ json(conflict, 409) })
  t.same(err, conflict)
  t.eq(ctx.delivered_after, 0)
end)

t.case("a_retired_call_sends_no_step", function()
  local ctx = t.provider_ctx({})
  ctx.retired = true
  local instance = assert(spec.prepare(ctx, { id = "sonnet" }, CONFIG))
  t.eq(instance.request_delivery_marker, "manual")
  local result, err = instance.stream(ctx, REQUEST, t.recorder())
  t.eq(result, nil)
  t.eq(err.retry, "none")
  t.eq(#ctx.requests, 0)
end)

t.case("a_stream_without_a_result_is_not_retried", function()
  local _, _, err = stream({ t.response({ frame("event", { type = "text_delta", text = "x" }) }) })
  t.eq(err.retry, "none")
  local _, _, read_err = stream({ t.response({}, { read_error = "idle timeout waiting for response body" }) })
  t.eq(read_err.retry, "none")
end)

t.case("a_finished_call_stops_reading", function()
  local response = t.response({ frame("event", { type = "text_delta", text = "x" }), frame("done", DONE) })
  local _, result, err = stream({ response }, function()
    return false
  end)
  t.eq(result, nil)
  t.eq(err.retry, "none")
  t.eq(response.closed, true)
end)

t.case("a_bridge_that_is_not_running_is_a_config_error", function()
  local ctx = failing_ctx(CONNECT_FAILED)
  local instance = assert(spec.prepare(ctx, { id = "sonnet" }, CONFIG))
  local _, err = instance.stream(ctx, REQUEST, t.recorder())
  t.eq(err.kind, "config")
  t.has(err.message, SOCKET)
  local _, other = instance.stream(failing_ctx("connection reset"), REQUEST, t.recorder())
  t.eq(other, "connection reset")
end)

t.case("auth_status_follows_the_bridge", function()
  t.eq(spec.auth_status(failing_ctx(CONNECT_FAILED), CONFIG).state, "missing")
  local function status(body)
    return spec.auth_status(t.provider_ctx({ json(body) }), CONFIG)
  end
  t.same(
    status({ ready = false, error = "claude did not start" }),
    { state = "failed", detail = "claude did not start" }
  )
  t.eq(status({ ready = true, logged_in = false }).state, "missing")
  t.same(
    status({ ready = true, logged_in = true, account = { email = "a@example.com" } }),
    { state = "configured", detail = "a@example.com" }
  )
end)

t.case("discovery_lists_each_model_once", function()
  local ctx = t.provider_ctx({
    json({
      models = {
        { value = "default", resolved_model = "claude-opus-5-5" },
        { value = "opus", resolved_model = "claude-opus-5-5" },
        { value = "claude-opus-5-5", resolved_model = "claude-opus-5-5" },
        { value = "haiku", resolved_model = "claude-haiku-5-5" },
      },
    }),
  })
  local models = spec.discover(ctx, CONFIG)
  t.eq(ctx.requests[1].method, "GET")
  t.has(ctx.requests[1].url, "/v1/models")
  t.eq(#models, 2)
  t.eq(models[1].id, "claude-opus-5-5")
  t.eq(models[1].tier, "strong")
  t.eq(models[2].tier, "weak")
end)

t.case("discovery_is_cached_for_the_next_start", function()
  local dir = t.mktmpdir("claude-agent")
  local cache = maki.fs.joinpath(dir, claude_agent.CACHE_FILE)
  t.same(claude_agent.provider(cache).models, claude_agent.MODELS)
  local cached = claude_agent.provider(cache)
  cached.discover(
    t.provider_ctx({ json({ models = { { value = "opus", resolved_model = "claude-opus-5-5" } } }) }),
    CONFIG
  )
  local models = claude_agent.provider(cache).models
  t.eq(models[1].id, "claude-opus-5-5")
  t.eq(models[1].supports_thinking, true)
  t.rmtree(dir)
end)

t.case("aliases_can_think_before_any_discovery", function()
  for _, model in ipairs(claude_agent.MODELS) do
    t.eq(model.supports_thinking, true)
  end
end)

t.case("ending_a_conversation_posts_the_reason", function()
  local ctx = t.provider_ctx({ json({}) })
  local instance = assert(spec.prepare(ctx, { id = "sonnet" }, CONFIG))
  instance.end_conversation(ctx, "c/1", "cancelled")
  t.has(ctx.requests[1].url, "/v1/conversations/c%2F1/end")
  t.same(maki.json.decode(ctx.requests[1].body), { reason = "cancelled" })
end)

t.report()
