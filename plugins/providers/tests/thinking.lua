local t = require("makima.test_helpers")
local thinking = require("providers.thinking")

local OFF = { kind = "off" }
local ADAPTIVE = { kind = "adaptive" }

local function effort(level)
  return { kind = "effort", level = level }
end

local function budget(tokens)
  return { kind = "budget", tokens = tokens }
end

t.case("max_budget_is_half_the_declared_window_at_least_the_minimum", function()
  t.eq(thinking.max_budget({ max_output_tokens = 1000 }), thinking.MIN_BUDGET)
  t.eq(thinking.max_budget({ max_output_tokens = 8193, output_tokens = 2000 }), 4096)
  t.eq(thinking.max_budget({ output_tokens = 8192 }), nil)
end)

t.case("budget", function()
  for _, case in ipairs({
    { OFF, 10000, "off" },
    { ADAPTIVE, 10000, "adaptive" },
    { effort("high"), nil, 19660 },
    { effort("minimal"), 5000, thinking.MIN_BUDGET },
    { budget(500), 10000, thinking.MIN_BUDGET },
    { budget(50000), 10000, 10000 },
    { budget(50000), nil, 50000 },
  }) do
    t.eq(thinking.budget(case[1], case[2]), case[3], case[1].kind)
  end
end)

t.case("effort_snaps_to_dialect", function()
  local dialects = thinking.dialects
  for _, case in ipairs({
    { ADAPTIVE, dialects.standard, "medium" },
    { OFF, dialects.standard, nil },
    { OFF, dialects.glm, "none" },
    { effort("xhigh"), dialects.standard, "high" },
    { effort("low"), dialects.glm, "high" },
    { budget(3000), dialects.standard, "medium" },
    { ADAPTIVE, dialects.deepseek, nil },
  }) do
    t.eq(thinking.effort(case[1], case[2], 10000), case[3], case[1].kind)
  end
end)

t.case("apply_local_picks_declared_fragment", function()
  local fields = {
    off = { think = false },
    adaptive = { think = true },
    low = { options = { effort = "low" } },
    high = { options = { effort = "high" } },
  }
  local body = { options = { temperature = 1 } }
  thinking.apply_local(body, effort("medium"), fields, 10000)
  t.same(body, { options = { temperature = 1, effort = "low" } })
  body = {}
  thinking.apply_local(body, OFF, fields, 10000)
  t.same(body, { think = false })
end)

t.case("apply_local_falls_back_to_budget_field", function()
  local body = {}
  thinking.apply_local(body, budget(2000), { adaptive = { think = true } }, 10000)
  t.same(body, { think = true, thinking_budget_tokens = 2000 })
  for _, case in ipairs({ { OFF, 0 }, { ADAPTIVE, -1 }, { effort("low"), 2000 } }) do
    body = {}
    thinking.apply_local(body, case[1], nil, 10000)
    t.same(body, { thinking_budget_tokens = case[2] })
  end
end)

t.report()
