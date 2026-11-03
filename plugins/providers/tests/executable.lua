local executable = require("providers.executable")
local http = require("providers.http")
local routes = require("providers.routes")
local t = require("maki.test_helpers")

local PATH = "/scripts/corp"
local BASE = "https://corp.example.invalid/v1"
local DONE = "data: [DONE]\n\n"
local PREFIX = "You are Corp."
local INFO = '{"display_name": "Corp", "base": "openai", "has_auth": true}'
local BASES = { openai = true, anthropic = true, ["llama-cpp"] = true }

-- A fake script: {outputs} maps a subcommand to its output, or to a function
-- returning it. Every run is recorded in `runs`.
local function script(outputs)
  local runs = {}
  local function run(_, command)
    runs[#runs + 1] = command
    local output = outputs[command]
    if type(output) == "function" then
      output = output()
    end
    if output == nil then
      return nil, http.config_error(PATH .. " " .. command .. " exited with 1")
    end
    return output
  end
  return run, runs
end

local function auth(token, base_url)
  return maki.json.encode({ base_url = base_url, headers = { authorization = "Bearer " .. token } })
end

local function provider(route, run, info, models)
  local described = {
    slug = "corp",
    path = PATH,
    info = info or { display_name = "Corp", base = "openai", has_auth = true },
    models = models or {},
  }
  return executable.provider(described, routes.ROUTES[route], run)
end

local function request(id)
  return {
    model = { id = id or "gpt-4.1", max_output_tokens = 16384, context_window = 128000 },
    system = "sys",
    messages = { { role = "user", content = { { type = "text", text = "hi" } } } },
    tools = {},
    thinking = { kind = "off" },
  }
end

local function chat_sse()
  return t.response({ 'data: {"choices":[{"delta":{"content":"ok"},"finish_reason":"stop"}]}\n\n' .. DONE })
end

local function unauthorized()
  return t.response({ "expired" }, { status = 401 })
end

local function stream(spec, responses, id)
  local ctx = t.provider_ctx(responses)
  local instance = assert(spec.prepare(ctx, { id = id or "gpt-4.1" }, {}))
  local emit, events = t.recorder()
  local result, err = instance.stream(ctx, request(id), emit)
  return ctx, result, err, events, instance
end

t.case("info_needs_fields_and_a_known_base", function()
  local info = assert(executable.parse_info(PATH, INFO, BASES))
  t.eq(info.display_name, "Corp")
  t.eq(info.system_prefix, nil)
  local prefixed = executable.parse_info(
    PATH,
    '{"display_name": "T", "base": "anthropic", "has_auth": false, "system_prefix": "' .. PREFIX .. '"}',
    BASES
  )
  t.eq(prefixed.system_prefix, PREFIX)
  t.eq(
    executable.parse_info(
      PATH,
      '{"display_name": "T", "base": "openai", "has_auth": false, "system_prefix": ""}',
      BASES
    ).system_prefix,
    nil
  )
  local _, missing = executable.parse_info(PATH, '{"display_name": "T", "base": "openai"}', BASES)
  t.eq(missing.kind, http.CONFIG_KIND)
  local _, unknown = executable.parse_info(PATH, '{"display_name": "T", "base": "vertex", "has_auth": false}', BASES)
  t.has(unknown.message, "unknown base provider 'vertex'")
end)

t.case("model_defaults", function()
  local model = executable.model({ id = "custom-v1" })
  t.same(model, { id = "custom-v1", tier = "medium", max_output_tokens = 16384, context_window = 128000 })
  local full = executable.model({
    id = "m",
    tier = "strong",
    supports_tool_examples = true,
    requires_thinking = true,
    pricing = { input = 3, output = 15, cache_write = 3.75, cache_read = 0.3 },
    thinking_fields = { adaptive = { reasoning_effort = "medium" }, low = { reasoning_effort = "low" } },
  })
  t.eq(full.tier, "strong")
  t.eq(full.supports_tool_examples, true)
  t.eq(full.requires_thinking, true)
  t.eq(full.pricing.cache_read, 0.3)
  t.eq(full.provider_data.thinking_fields.low.reasoning_effort, "low")
  t.eq(executable.model({ id = "m", tier = 7 }), nil)
  t.eq(executable.model({ id = "m", pricing = { input = 1 } }), nil)
end)

t.case("bad_entries_cost_only_themselves", function()
  local models = executable.parse_models(
    "corp",
    '[{"id": "typo", "thinking_fields": {"hight": {"reasoning_effort": "high"}}}, {"id": "broken", "tier": 7}, {"id": "good"}]',
    "llama-cpp"
  )
  t.eq(#models, 2)
  t.eq(models[1].id, "typo")
  t.eq(models[1].provider_data, nil)
  t.eq(models[2].id, "good")
  t.same(executable.parse_models("corp", "not json", "openai"), {})
end)

t.case("auth_output", function()
  local with_base = assert(executable.parse_auth(PATH, "resolve", auth("tok", BASE)))
  t.eq(with_base.base_url, BASE)
  t.eq(with_base.headers.authorization, "Bearer tok")
  t.eq(assert(executable.parse_auth(PATH, "resolve", auth("x"))).base_url, nil)
  local _, err = executable.parse_auth(PATH, "refresh", '{"headers": {"authorization": 1}}')
  t.has(err.message, PATH .. " refresh")
end)

t.case("scan_caches_descriptions_until_the_script_changes", function()
  local dir = t.mktmpdir("executable-scan")
  local cache = maki.fs.joinpath(dir, "cache.json")
  local scripts = maki.fs.joinpath(dir, "providers")
  maki.fs.mkdir(scripts)
  for _, name in ipairs({ "corp", "has.dot", "weird", "plain" }) do
    maki.fs.write(maki.fs.joinpath(scripts, name), "#!/bin/sh\n")
  end
  local executable_check = maki.fn.executable
  maki.fn.executable = function(path)
    return path:find("plain", 1, true) and 0 or 1
  end
  local runs = {}
  local function run(path, command)
    runs[#runs + 1] = command
    if path:find("weird", 1, true) then
      return '{"display_name": "Weird", "base": "unknown-provider", "has_auth": false}'
    end
    return command == "info" and INFO or '[{"id": "corp-1", "tier": "strong"}]'
  end
  local found = executable.scan(scripts, cache, BASES, run)
  local again = executable.scan(scripts, cache, BASES, run)
  maki.fn.executable = executable_check
  t.rmtree(dir)
  t.eq(#found, 1)
  t.eq(found[1].slug, "corp")
  t.eq(found[1].info.base, "openai")
  t.eq(found[1].models[1].tier, "strong")
  t.eq(#again, 1)
  -- corp and weird each ran info and models once; the cache served the rest.
  t.same(runs, { "info", "models", "info", "models" })
end)

t.case("prepare_resolves_and_streams_with_script_credentials", function()
  local run, runs = script({ resolve = auth("resolved", BASE) })
  local ctx = stream(provider("openai", run), { chat_sse() })
  local req = ctx.requests[1]
  t.eq(req.url, BASE .. "/chat/completions")
  t.eq(req.headers.authorization, "Bearer resolved")
  t.same(runs, { "resolve" })
end)

t.case("base_default_and_configured_headers", function()
  local run = script({ resolve = auth("resolved") })
  local spec = provider("openai", run)
  local ctx = t.provider_ctx({ chat_sse() })
  t.with_env({ GATEWAY = "gw" }, function()
    local instance = assert(spec.prepare(ctx, { id = "gpt-4.1" }, { headers = { authorization = "${GATEWAY}" } }))
    assert(instance.stream(ctx, request(), t.recorder()))
  end)
  t.eq(ctx.requests[1].url, "https://api.openai.com/v1/chat/completions")
  t.eq(ctx.requests[1].headers.authorization, "gw")
end)

t.case("unauthorized_before_events_refreshes_once_and_replays", function()
  local run, runs = script({ resolve = auth("stale", BASE), refresh = auth("fresh") })
  local ctx, result = stream(provider("openai", run), { unauthorized(), chat_sse() })
  assert(result)
  t.same(runs, { "resolve", "refresh" })
  t.eq(ctx.requests[2].headers.authorization, "Bearer fresh")
  -- A refresh without base_url keeps the one in use.
  t.eq(ctx.requests[2].url, BASE .. "/chat/completions")
end)

t.case("unauthorized_after_events_is_not_replayed", function()
  local run, runs = script({ resolve = auth("stale", BASE), refresh = auth("fresh") })
  local partial = t.response({
    'data: {"choices":[{"delta":{"content":"par"}}]}\n\n',
    'data: {"error":{"type":"authentication_error","message":"expired"}}\n\n',
  })
  local ctx, result, err, events = stream(provider("openai", run), { partial })
  t.eq(result, nil)
  t.eq(err.status, 401)
  t.eq(#events > 0, true)
  t.eq(#ctx.requests, 1)
  t.same(runs, { "resolve" })
end)

t.case("failed_refresh_returns_the_original_error", function()
  local run = script({ resolve = auth("stale", BASE) })
  local ctx, result, err = stream(provider("openai", run), { unauthorized() })
  t.eq(result, nil)
  t.eq(err.status, 401)
  t.eq(#ctx.requests, 1)
end)

t.case("instances_share_refreshed_credentials", function()
  local run, runs = script({ resolve = auth("stale", BASE), refresh = auth("fresh") })
  local spec = provider("openai", run)
  local ctx = t.provider_ctx({ chat_sse() })
  local first = assert(spec.prepare(ctx, { id = "gpt-4.1" }, {}))
  local second = assert(spec.prepare(ctx, { id = "gpt-4.1" }, {}))
  first.refresh_auth(ctx)
  assert(second.stream(ctx, request(), t.recorder()))
  t.eq(ctx.requests[1].headers.authorization, "Bearer fresh")
  t.same(runs, { "resolve", "resolve", "refresh" })
end)

t.case("reload_keeps_base_url", function()
  local run, runs = script({ resolve = auth("old", BASE), reload = auth("reloaded") })
  local ctx = t.provider_ctx({ chat_sse() })
  local instance = assert(provider("openai", run).prepare(ctx, { id = "gpt-4.1" }, {}))
  instance.reload_auth(ctx)
  assert(instance.stream(ctx, request(), t.recorder()))
  t.eq(ctx.requests[1].headers.authorization, "Bearer reloaded")
  t.eq(ctx.requests[1].url, BASE .. "/chat/completions")
  t.same(runs, { "resolve", "reload" })
end)

t.case("system_prefix_is_a_block_for_anthropic_and_prepended_elsewhere", function()
  local messages = t.response({
    'data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}\n\n',
  })
  local info = { display_name = "Corp", base = "anthropic", has_auth = false, system_prefix = PREFIX }
  local ctx =
    stream(provider("anthropic", script({ resolve = auth("oauth") }), info), { messages }, "claude-sonnet-4-5")
  local body = maki.json.decode(ctx.requests[1].body)
  t.eq(ctx.requests[1].url, "https://api.anthropic.com/v1/messages")
  t.eq(body.system[1].text, PREFIX)
  t.eq(body.system[2].text, "sys")

  info.base = "openai"
  ctx = stream(provider("openai", script({ resolve = auth("k") }), info), { chat_sse() })
  body = maki.json.decode(ctx.requests[1].body)
  t.eq(body.messages[1].content, PREFIX .. "\n\nsys")
end)

t.case("script_models_match_by_longest_prefix_else_base_models", function()
  local models = {
    { id = "corp", tier = "weak", context_window = 1000 },
    { id = "corp-large", tier = "strong", context_window = 2000 },
  }
  local spec = provider("openai", script({ resolve = auth("k") }), nil, models)
  t.eq(#spec.models, 2)
  local _, model = spec.prepare(t.provider_ctx({}), { id = "corp-large-v2" }, {})
  t.eq(model.context_window, 2000)
  t.eq(model.tier, "strong")
  local base = provider("openai", script({ resolve = auth("k") }))
  t.eq(base.models[1].id, routes.ROUTES.openai.def.models[1].id)
end)

t.case("thinking_fields_reach_llama_cpp_bodies", function()
  local models = executable.parse_models(
    "corp",
    '[{"id": "qwen", "thinking_fields": {"off": {"chat_template_kwargs": {"enable_thinking": false}}}}]',
    "llama-cpp"
  )
  local info = { display_name = "Corp", base = "llama-cpp", has_auth = false }
  local ctx = stream(provider("llama-cpp", script({ resolve = auth("k") }), info, models), { chat_sse() }, "qwen")
  t.eq(ctx.requests[1].url, "http://localhost:8080/v1/chat/completions")
  t.eq(maki.json.decode(ctx.requests[1].body).chat_template_kwargs.enable_thinking, false)
end)

t.case("login_runs_on_the_terminal_and_logout_runs_quietly", function()
  local run, runs = script({ logout = "" })
  local spec = provider("openai", run)
  local ran
  local ui = {
    run = function(_, argv)
      ran = argv
      return true
    end,
  }
  t.eq(spec.auth[1].login(t.provider_ctx({}), ui), true)
  t.same(ran, { PATH, "login" })
  spec.logout(t.provider_ctx({}))
  t.same(runs, { "logout" })
  local keyless = provider("openai", run, { display_name = "Corp", base = "openai", has_auth = false })
  t.eq(keyless.auth, nil)
  t.eq(keyless.logout, nil)
end)

t.report()
