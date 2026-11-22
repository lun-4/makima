local llama_cpp = require("providers.llama_cpp")
local local_server = require("providers.local_server")
local t = require("makima.test_helpers")

local HOST = "http://box:8080"
local DEFAULT = llama_cpp.DEFAULT_CONTEXT

local spec = local_server.provider(llama_cpp.def)

local function json(value)
  return t.response({ assert(makima.json.encode(value)) })
end

local function window(row, mode, props_n_ctx)
  return llama_cpp.context_window(row, mode, props_n_ctx or 0)
end

t.case("router_context_window", function()
  t.eq(window({ meta = { n_ctx = 32768 } }, "router"), 32768)
  t.eq(window({ status = { args = { "--model", "foo.gguf", "--ctx-size", "16384" } } }, "router"), 16384)
  t.eq(window({ status = { args = { "--fit-ctx", "24000" } } }, "router"), 24000)
  t.eq(window({ status = { args = { "--ctx-size", "8192", "--fit-ctx", "16384" } } }, "router"), 8192)
  t.eq(window({ meta = { n_ctx = 4096 }, status = { args = { "--ctx-size", "65536" } } }, "router"), 4096)
  t.eq(window({}, "router"), DEFAULT)
end)

t.case("single_and_legacy_context_window", function()
  t.eq(window({ meta = { n_ctx = 131072 } }, "single"), 131072)
  t.eq(window({ meta = { n_ctx = 0 } }, "single"), DEFAULT)
  t.eq(window({ status = { args = { "--ctx-size", "8192" } } }, "single"), DEFAULT)
  t.eq(window({ max_model_len = 4096 }, "legacy"), 4096)
  t.eq(window({}, "legacy", 8192), 8192)
  t.eq(window({ max_model_len = 2048 }, "legacy", 8192), 2048)
  t.eq(window({ max_model_len = 0 }, "legacy", 4096), 4096)
  t.eq(window({}, "legacy"), DEFAULT)
end)

t.case("flag_value", function()
  t.eq(llama_cpp.flag_value({ "--ctx-size", "4096" }, "--ctx-size"), 4096)
  t.eq(llama_cpp.flag_value({ "--model", "foo.gguf" }, "--ctx-size"), nil)
  t.eq(llama_cpp.flag_value({ "--ctx-size" }, "--ctx-size"), nil)
  t.eq(llama_cpp.flag_value({ "--ctx-size", "abc" }, "--ctx-size"), nil)
end)

t.case("props_vision", function()
  t.eq(llama_cpp.props_vision({ modalities = { vision = true, audio = false } }, "single"), true)
  t.eq(llama_cpp.props_vision({ modalities = { vision = false } }, "legacy"), false)
  t.eq(llama_cpp.props_vision({ total_slots = 4 }, "single"), nil)
  t.eq(llama_cpp.props_vision({ role = "router", modalities = { vision = true } }, "router"), nil)
end)

t.case("modes", function()
  t.eq(llama_cpp.mode({ role = "router" }, { { id = "a", max_model_len = 1 } }), "router")
  t.eq(llama_cpp.mode({}, { { id = "a", max_model_len = 1 } }), "legacy")
  t.eq(llama_cpp.mode({}, { { id = "a" } }), "single")
  t.eq(llama_cpp.mode({}, {}), "single")
end)

t.case("parse_filters_non_text_models_and_reads_vision", function()
  local models = llama_cpp.parse_models({ modalities = { vision = true } }, {
    data = {
      { id = "zeta", meta = { n_ctx = 4096 } },
      { id = "embed", architecture = { input_modalities = { "text" }, output_modalities = { "embedding" } } },
      { id = "alpha", architecture = { input_modalities = { "text", "image" }, output_modalities = { "text" } } },
      { id = "mid", architecture = { input_modalities = { "text" }, output_modalities = { "text" } } },
    },
  })
  t.same(models, {
    { id = "alpha", context_window = DEFAULT, supports_vision = true, pricing = local_server.PRICING_FREE },
    { id = "mid", context_window = DEFAULT, supports_vision = false, pricing = local_server.PRICING_FREE },
    { id = "zeta", context_window = 4096, supports_vision = true, pricing = local_server.PRICING_FREE },
  })
end)

t.case("discovery_reads_props_then_models", function()
  local ctx = t.provider_ctx({
    json({ role = "router" }),
    json({ data = { { id = "m", status = { args = { "--ctx-size", "2048" } } } } }),
  })
  local models
  t.with_env({ LLAMA_CPP_HOST = HOST, LLAMA_CPP_API_KEY = "k" }, function()
    models = assert(spec.discover(ctx, {}))
  end)
  t.eq(ctx.requests[1].url, HOST .. "/props?autoload=false")
  t.eq(ctx.requests[1].headers.Authorization, "Bearer k")
  t.eq(ctx.requests[2].url, HOST .. "/v1/models")
  t.same(models, {
    { id = "m", context_window = 2048, supports_thinking = true, pricing = local_server.PRICING_FREE },
  })
end)

t.case("props_failure_fails_discovery", function()
  local ctx = t.provider_ctx({ t.response({ "nope" }, { status = 404 }) })
  t.with_env({ LLAMA_CPP_HOST = HOST }, function()
    local models, err = spec.discover(ctx, {})
    t.eq(models, nil)
    t.eq(err.status, 404)
  end)
end)

t.report()
