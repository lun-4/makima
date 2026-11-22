local local_server = require("providers.local_server")
local ollama = require("providers.ollama")
local t = require("makima.test_helpers")

local HOST = "http://box:11434"
local CLOUD_ROOT = "https://ollama.com"
local CONTEXT_LENGTH = 128000

local spec = local_server.provider(ollama.def)

local function json(value, opts)
  return t.response({ assert(makima.json.encode(value)) }, opts)
end

-- Shaped like a real `POST /api/show` body.
local function show(capabilities)
  return {
    license = "Apache License Version 2.0, January 2004",
    parameters = 'stop "<|im_start|>"\nnum_ctx 2048',
    details = { family = "qwen25vl", families = { "qwen25vl", "clip" } },
    model_info = {
      ["general.architecture"] = "qwen25vl",
      ["qwen25vl.context_length"] = CONTEXT_LENGTH,
      ["qwen25vl.embedding_length"] = 3584,
    },
    capabilities = capabilities,
  }
end

local function discover(env, responses, config)
  local ctx = t.provider_ctx(responses)
  local models, err
  t.with_env(env, function()
    models, err = spec.discover(ctx, config or {})
  end)
  return ctx, models, err
end

t.case("show_details", function()
  t.eq(ollama.vision(show({ "completion", "vision" })), true)
  t.eq(ollama.vision(show({ "completion", "tools" })), false)
  t.eq(ollama.vision(show(nil)), nil)
  t.eq(ollama.context_window(show(nil)), CONTEXT_LENGTH)
end)

t.case("context_window_sources", function()
  local function window(model_info, parameters)
    return ollama.context_window({ model_info = model_info, parameters = parameters })
  end
  t.eq(window({ ["llama.context_length"] = 8192 }), 8192)
  t.eq(window({ context_length = 4096 }), 4096)
  t.eq(window({ ["llama.context_length"] = 8192, context_length = 4096 }), 8192)
  t.eq(window({ ["llama.context_length"] = "8192" }), nil)
  t.eq(window({ ["llama.embedding_length"] = 4096 }), nil)
  t.eq(window({}, "temperature 0.7\nnum_ctx 4096\nseed 0"), 4096)
  t.eq(window({}, "  num_ctx  8192  "), 8192)
  t.eq(window({}, "num_ctx 0"), nil)
  t.eq(window({}, "num_ctx abc"), nil)
  t.eq(window({}, ""), nil)
end)

t.case("discovery_shows_each_model", function()
  local ctx, models = discover({ OLLAMA_HOST = HOST, OLLAMA_API_KEY = "k" }, {
    json({ data = { { id = "zeta" }, { id = "alpha" } } }),
    json(show({ "completion", "vision" })),
    json({ model_info = { context_length = 4096 }, capabilities = { "completion" } }),
  })
  t.eq(ctx.requests[1].url, HOST .. "/v1/models")
  t.eq(ctx.requests[1].headers.Authorization, "Bearer k")
  t.eq(ctx.requests[2].url, HOST .. "/api/show")
  t.eq(ctx.requests[2].method, "POST")
  t.eq(ctx.requests[2].headers.Authorization, "Bearer k")
  t.eq(ctx.requests[2].headers["Content-Type"], "application/json")
  t.eq(makima.json.decode(ctx.requests[2].body).model, "alpha")
  t.eq(makima.json.decode(ctx.requests[3].body).model, "zeta")
  t.same(models, {
    {
      id = "alpha",
      context_window = CONTEXT_LENGTH,
      max_output_tokens = 16384,
      supports_thinking = false,
      supports_vision = true,
      pricing = local_server.PRICING_FREE,
    },
    {
      id = "zeta",
      context_window = 4096,
      max_output_tokens = 16384,
      supports_thinking = false,
      supports_vision = false,
      pricing = local_server.PRICING_FREE,
    },
  })
end)

t.case("failed_show_keeps_the_model", function()
  local _, models = discover({ OLLAMA_HOST = HOST }, {
    json({ data = { { id = "a" }, { id = "b" } } }),
    t.response({ "boom" }, { status = 500 }),
    t.response({ "not json" }),
  })
  t.eq(#models, 2)
  for _, model in ipairs(models) do
    t.eq(model.context_window, 128000)
    t.eq(model.supports_vision, nil)
  end
end)

t.case("listing_failure_fails_discovery", function()
  local _, models, err = discover({ OLLAMA_HOST = HOST }, { t.response({ "down" }, { status = 503 }) })
  t.eq(models, nil)
  t.eq(err.status, 503)
end)

t.case("cloud_discovery_uses_cloud_root", function()
  local ctx = discover({ OLLAMA_API_KEY = "k" }, { json({ data = {} }) })
  t.eq(ctx.requests[1].url, CLOUD_ROOT .. "/v1/models")
end)

t.report()
