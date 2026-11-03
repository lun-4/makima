local http = require("providers.http")
local llama_cpp = require("providers.llama_cpp")
local local_server = require("providers.local_server")
local ollama = require("providers.ollama")
local t = require("maki.test_helpers")

local HOST = "http://box:1234"
local CONFIG_HOST = "http://config:4321"
local CLOUD = "https://ollama.com/v1"
local DONE = "data: [DONE]\n\n"
local RESPONSES_DONE =
  'event: response.completed\ndata: {"response":{"usage":{"input_tokens":1,"output_tokens":1}}}\n\n'

local ollama_spec = local_server.provider(ollama.def)
local llama_spec = local_server.provider(llama_cpp.def)

local function sse()
  return t.response({ 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n' .. DONE })
end

local function request(thinking)
  return {
    model = { id = "qwen3", max_output_tokens = 16384, context_window = 128000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = thinking or { kind = "off" },
  }
end

local function prepare(spec, env, config, responses, saved)
  local ctx = t.provider_ctx(responses or { sse() }, saved)
  local instance, model
  t.with_env(env, function()
    instance, model = spec.prepare(ctx, { id = "qwen3" }, config or {})
  end)
  return ctx, instance, model
end

local function sent(spec, env, config, saved, thinking)
  local ctx, instance = prepare(spec, env, config, nil, saved)
  assert(instance, "prepare failed")
  assert(instance.stream(ctx, request(thinking), t.recorder()))
  return ctx.requests[1]
end

t.case("host_from_config_then_env", function()
  t.eq(sent(ollama_spec, { OLLAMA_HOST = HOST }).url, HOST .. "/v1/chat/completions")
  t.eq(sent(ollama_spec, { OLLAMA_HOST = HOST }, { base_url = CONFIG_HOST }).url, CONFIG_HOST .. "/v1/chat/completions")
  t.eq(sent(llama_spec, { LLAMA_CPP_HOST = HOST }).url, HOST .. "/v1/chat/completions")
end)

t.case("base_url_env_does_not_name_the_host", function()
  local _, instance, err = prepare(ollama_spec, { OLLAMA_BASE_URL = HOST })
  t.eq(instance, nil)
  t.eq(err.kind, http.CONFIG_KIND)
  t.eq(err.message, "OLLAMA_HOST not set")
end)

t.case("no_key_sends_no_authorization", function()
  local req = sent(ollama_spec, { OLLAMA_HOST = HOST })
  t.eq(req.headers.Authorization, nil)
  local _, instance = prepare(ollama_spec, { OLLAMA_HOST = HOST })
  t.eq(instance.key_count, nil)
  t.eq(instance.rotate_key, nil)
end)

t.case("key_with_host_keeps_host", function()
  local req = sent(ollama_spec, { OLLAMA_HOST = HOST, OLLAMA_API_KEY = "k" })
  t.eq(req.url, HOST .. "/v1/chat/completions")
  t.eq(req.headers.Authorization, "Bearer k")
end)

t.case("ollama_key_without_host_uses_cloud", function()
  local req = sent(ollama_spec, { OLLAMA_API_KEY = "k" })
  t.eq(req.url, CLOUD .. "/chat/completions")
  req = sent(ollama_spec, {}, {}, { api_key = "saved" })
  t.eq(req.url, CLOUD .. "/chat/completions")
  t.eq(req.headers.Authorization, "Bearer saved")
end)

t.case("llama_cpp_has_no_cloud_fallback", function()
  for _, env in ipairs({ {}, { LLAMA_CPP_API_KEY = "k" } }) do
    local _, instance, err = prepare(llama_spec, env)
    t.eq(instance, nil)
    t.eq(err.kind, http.CONFIG_KIND)
    t.eq(err.message, "LLAMA_CPP_HOST not set")
  end
end)

t.case("configured_headers_expand_and_win", function()
  local req = sent(ollama_spec, { OLLAMA_HOST = HOST, OLLAMA_API_KEY = "k", TOKEN = "gw" }, {
    headers = { authorization = "Token ${TOKEN}", ["X-Extra"] = "1" },
  })
  t.eq(req.headers.Authorization, nil)
  t.eq(req.headers.authorization, "Token gw")
  t.eq(req.headers["X-Extra"], "1")
  local _, instance, err = prepare(ollama_spec, { OLLAMA_HOST = HOST }, { headers = { X = "${MISSING}" } })
  t.eq(instance, nil)
  t.eq(err.kind, http.CONFIG_KIND)
end)

t.case("keys_rotate", function()
  local ctx, instance = prepare(llama_spec, { LLAMA_CPP_HOST = HOST, LLAMA_CPP_API_KEY = "a,b" })
  t.eq(instance.key_count, 2)
  assert(instance.rotate_key())
  assert(instance.stream(ctx, request(), t.recorder()))
  t.eq(ctx.requests[1].headers.Authorization, "Bearer b")
end)

t.case("chat_body_and_local_thinking", function()
  local body = maki.json.decode(sent(ollama_spec, { OLLAMA_HOST = HOST }, {}, nil, { kind = "adaptive" }).body)
  t.eq(body.max_tokens, 16384)
  t.eq(body.stream_options.include_usage, true)
  t.eq(body.thinking_budget_tokens, nil)
  local env = { LLAMA_CPP_HOST = HOST }
  t.eq(maki.json.decode(sent(llama_spec, env).body).thinking_budget_tokens, 0)
  t.eq(maki.json.decode(sent(llama_spec, env, {}, nil, { kind = "adaptive" }).body).thinking_budget_tokens, -1)
  local budget = sent(llama_spec, env, {}, nil, { kind = "budget", tokens = 4096 })
  t.eq(maki.json.decode(budget.body).thinking_budget_tokens, 4096)
end)

t.case("responses_protocol_streams_with_progress", function()
  local config = { protocol = "openai-responses" }
  local ctx, instance = prepare(llama_spec, { LLAMA_CPP_HOST = HOST }, config, {
    t.response({
      'event: response.in_progress\ndata: {"prompt_progress":{"processed":5,"total":10,"cache":1}}\n\n',
      RESPONSES_DONE,
    }),
  })
  local emit, events = t.recorder()
  assert(instance.stream(ctx, request({ kind = "adaptive" }), emit))
  local req = ctx.requests[1]
  t.eq(req.url, HOST .. "/v1/responses")
  local body = maki.json.decode(req.body)
  t.eq(body.return_progress, true)
  t.eq(body.reasoning.effort, "medium")
  t.eq(body.thinking_budget_tokens, nil)
  t.same(events[1], { type = "prompt_progress", processed = 5, total = 10, cache = 1 })
end)

t.case("prepare_fills_local_defaults", function()
  local _, _, model = prepare(ollama_spec, { OLLAMA_HOST = HOST })
  t.same(model, {
    id = "qwen3",
    context_window = 128000,
    max_output_tokens = 16384,
    supports_thinking = false,
    pricing = local_server.PRICING_FREE,
  })
  _, _, model = prepare(llama_spec, { LLAMA_CPP_HOST = HOST })
  t.eq(model.max_output_tokens, nil)
  t.eq(model.supports_thinking, true)
end)

t.case("discover_without_host_is_empty", function()
  t.with_env({}, function()
    t.same(llama_spec.discover(t.provider_ctx({}), {}), {})
  end)
end)

t.case("auth_status_counts_a_configured_host", function()
  t.with_env({}, function()
    t.eq(ollama_spec.auth_status(t.provider_ctx({}), {}).state, "missing")
    t.eq(ollama_spec.auth_status(t.provider_ctx({}), { base_url = HOST }).state, "configured")
    t.eq(ollama_spec.auth_status(t.provider_ctx({}, { api_key = "saved-key-1" }), {}).state, "saved")
  end)
  t.with_env({ OLLAMA_API_KEY = "k" }, function()
    t.same(
      ollama_spec.auth_status(t.provider_ctx({}), { base_url = HOST }),
      { state = "env", detail = "via OLLAMA_API_KEY" }
    )
  end)
end)

t.case("login_asks_host_then_optional_key", function()
  t.same(ollama_spec.config, { { key = "base_url", prompt = "Ollama host URL", default = "http://localhost:11434" } })
  t.eq(llama_spec.config[1].default, "http://localhost:8080")
  local prompts = {}
  local ui = {
    input = function(_, prompt, secret)
      prompts[#prompts + 1] = { prompt, secret }
      return ""
    end,
  }
  local ctx = t.provider_ctx({})
  assert(ollama_spec.auth[1].login(ctx, ui, { base_url = HOST }))
  t.same(prompts, { { "Ollama API key (or Enter to skip)", true } })
  t.eq(ctx.saved, nil)
end)

t.report()
