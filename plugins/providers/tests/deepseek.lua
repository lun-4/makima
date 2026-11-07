local deepseek = require("providers.deepseek")
local compat = require("providers.openai_compat")
local t = require("maki.test_helpers")

local FLASH = "deepseek-v4-flash"
local REASONER = "deepseek-reasoner"

local function tool_call_body()
  return {
    tools = { { type = "function" } },
    messages = {
      { role = "system", content = "sys" },
      { role = "user", content = "hi" },
      { role = "assistant", content = "ok", reasoning_content = "kept" },
      { role = "assistant", content = "", tool_calls = { { id = "c1" } } },
      { role = "tool", tool_call_id = "c1", content = "out" },
    },
  }
end

local function encode(kind, id, body)
  body = body or tool_call_body()
  local thinking = kind == "effort" and { kind = kind, level = "minimal" } or { kind = kind }
  deepseek.def.encode(body, { thinking = thinking, model = {} }, { id = id or FLASH })
  return body
end

t.case("pads_only_assistant_turns_without_reasoning", function()
  local messages = encode("adaptive").messages
  t.eq(messages[3].reasoning_content, "kept")
  t.eq(messages[4].reasoning_content, "")
  for _, i in ipairs({ 1, 2, 5 }) do
    t.eq(messages[i].reasoning_content, nil)
  end
end)

t.case("reasoner_and_toolless_bodies_are_not_padded", function()
  t.same(encode("adaptive", REASONER).messages, tool_call_body().messages)
  local body = tool_call_body()
  body.tools = nil
  t.same(encode("adaptive", FLASH, body).messages, tool_call_body().messages)
end)

t.case("only_the_reasoner_is_outside_v4", function()
  t.eq(deepseek.uses_v4_thinking_protocol(FLASH), true)
  t.eq(deepseek.uses_v4_thinking_protocol("deepseek-v9-turbo"), true)
  t.eq(deepseek.uses_v4_thinking_protocol(REASONER), false)
end)

t.case("thinking_toggle_and_max_effort", function()
  local off = encode("off")
  t.same(off.thinking, { type = "disabled" })
  t.eq(off.reasoning_effort, nil)
  t.eq(off.messages[4].reasoning_content, nil)
  local adaptive = encode("adaptive")
  t.same(adaptive.thinking, { type = "enabled" })
  t.eq(adaptive.reasoning_effort, nil)
  t.eq(encode("effort").reasoning_effort, "max")
end)

t.case("descriptors_carry_peak_pricing", function()
  for _, model in ipairs(compat.provider(deepseek.def).models) do
    t.same(model.pricing_schedule, {
      windows = { { start = 1, ["end"] = 4 }, { start = 6, ["end"] = 10 } },
      weekdays_only = true,
      multiplier = 2,
    })
  end
  local unknown = compat.describe(deepseek.def, { id = "my-custom-model" })
  t.eq(unknown.pricing, nil)
  t.eq(unknown.context_window, 1000000)
end)

t.case("balance", function()
  t.same(
    deepseek.parse_balance({
      balance_infos = {
        { currency = "USD", total_balance = "10.00", granted_balance = "1.00", topped_up_balance = "9.00" },
        { currency = "EUR", total_balance = "1", granted_balance = "0", topped_up_balance = "1" },
      },
    }),
    {
      limits = {
        { kind = "Credits", detail = "total: $10.00, topped-up: $9.00, granted: $1.00" },
        { kind = "Credits", detail = "total: 1, topped-up: 1, granted: 0" },
      },
    }
  )
end)

t.report()
