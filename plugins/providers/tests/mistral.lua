local mistral = require("providers.mistral")
local compat = require("providers.openai_compat")
local t = require("maki.test_helpers")

local function thinking_block(text)
  return { type = "thinking", thinking = { { type = "text", text = text } } }
end

t.case("reasoning_becomes_a_thinking_block", function()
  local messages = {
    { role = "system", content = "sys" },
    { role = "assistant", content = "text", reasoning_content = "a" },
    { role = "assistant", content = "", reasoning_content = "b" },
    { role = "assistant", reasoning_content = "c" },
    { role = "assistant", content = "only text" },
  }
  mistral.structure_reasoning(messages)
  t.same(messages, {
    { role = "system", content = "sys" },
    { role = "assistant", content = { thinking_block("a"), { type = "text", text = "text" } } },
    { role = "assistant", content = { thinking_block("b") } },
    { role = "assistant", content = { thinking_block("c") } },
    { role = "assistant", content = "only text" },
  })
end)

t.case("high_only_effort_and_affinity", function()
  local body = { messages = {} }
  local headers = mistral.def.encode(body, {
    thinking = { kind = "effort", level = "minimal" },
    model = {},
    session_id = "s1",
  })
  t.eq(body.reasoning_effort, "high")
  t.same(headers, { ["x-affinity"] = "s1" })
  t.same(mistral.def.encode({ messages = {} }, { thinking = { kind = "off" }, model = {} }), {})
end)

t.case("ministral_never_thinks", function()
  t.eq(compat.describe(mistral.def, { id = "ministral-14b-latest" }).supports_thinking, false)
  t.eq(compat.describe(mistral.def, { id = "mistral-medium-latest" }).supports_thinking, true)
end)

t.case("aliases_share_their_row", function()
  local alias = compat.describe(mistral.def, { id = "mistral-medium-2604" })
  t.eq(alias.tier, "strong")
  t.eq(alias.context_window, 262144)
  t.eq(alias.supports_vision, true)
  t.eq(compat.describe(mistral.def, { id = "zai-glm-5-2" }).pricing.cache_read, 0.14)
  t.eq(compat.describe(mistral.def, { id = "mistral-vibe-cli-latest" }).context_window, 128000)
end)

t.case("discovery_keeps_chat_models", function()
  t.same(
    mistral.parse_model({
      id = "magistral",
      max_context_length = 40960,
      capabilities = { completion_chat = true, reasoning = true, vision = false },
    }),
    { id = "magistral", context_window = 40960, supports_thinking = true, supports_vision = false }
  )
  t.same(mistral.parse_model({ id = "m", capabilities = { completion_chat = true } }), {
    id = "m",
    supports_vision = false,
  })
  t.eq(mistral.parse_model({ id = "embed", capabilities = { completion_chat = false } }), nil)
  t.eq(mistral.parse_model({ id = "bare" }), nil)
end)

t.case("coding_plan_defaults_to_vibe", function()
  local ctx = t.provider_ctx({})
  local opened
  local ui = {
    open_browser = function(_, url)
      opened = url
      return true
    end,
    input = function()
      return "key"
    end,
  }
  t.same(compat.provider(mistral.def).auth[1].login(ctx, ui, { plan = "coding" }), {
    model = "mistral-vibe-cli-latest",
  })
  t.eq(opened, "https://console.mistral.ai/codestral/cli")
end)

t.report()
