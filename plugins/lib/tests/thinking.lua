local thinking = require("maki.thinking")

local function resolve(request, profile, constraints)
  local result, err = thinking.resolve(request, profile, constraints)
  assert(result, err)
  return result
end

local profile = { mode = "effort", levels = { "high", "minimal", "medium" } }
local snapped = resolve({ mode = "effort", level = "low" }, profile)
assert(snapped.status == "adjusted" and snapped.effective.level == "minimal")
local exact = resolve({ mode = "effort", level = "high" }, profile)
assert(exact.status == "exact" and exact.effective.level == "high")
local adaptive = resolve({ mode = "adaptive" }, { mode = "adaptive", levels = { "medium" } })
assert(adaptive.status == "exact" and adaptive.effective.mode == "adaptive")
local converted = resolve({ mode = "budget", tokens = 12000 }, profile)
assert(converted.approximate and converted.status == "adjusted" and converted.effective.level == "medium")
local clamped = resolve(
  { mode = "budget", tokens = 9000 },
  { mode = "budget", min_tokens = 1000, max_tokens = 10000 },
  { parent = { mode = "budget", tokens = 5000 } }
)
assert(clamped.effective.tokens == 5000 and clamped.status == "adjusted")
local none = resolve({ mode = "effort", level = "high" }, { mode = "none" })
assert(none.effective.mode == "off" and none.status == "adjusted")
local required = resolve({ mode = "off" }, { mode = "budget", min_tokens = 1000, max_tokens = 10000, required = true })
assert(required.effective.tokens == 10000 and required.status == "adjusted")
local unknown = resolve({ mode = "adaptive" }, nil)
assert(unknown.status == "unsupported" and unknown.effective.mode == "adaptive")
local result, err = thinking.resolve({ mode = "off" }, { mode = "effort", levels = { "low", "low" } })
assert(result == nil and err == "invalid thinking profile")

local request = { mode = "adaptive" }
local bounded = resolve(request, profile, { parent = { mode = "budget", tokens = 7000 } })
assert(bounded.requested.mode == "adaptive" and bounded.effective.level == "minimal")
assert(bounded.status == "adjusted" and bounded.approximate)
request.mode = "off"
assert(bounded.requested.mode == "adaptive")
local adaptive_parent = resolve(
  { mode = "adaptive" },
  { mode = "adaptive", levels = { "medium" } },
  { parent = { mode = "adaptive" } }
)
assert(adaptive_parent.status == "exact" and adaptive_parent.effective.mode == "adaptive")
local impossible = resolve(
  { mode = "adaptive" },
  { mode = "effort", levels = { "high" } },
  { parent = { mode = "budget", tokens = 1000 } }
)
assert(impossible.status == "unsupported" and impossible.approximate)
local required_parent_off = resolve(
  { mode = "adaptive" },
  { mode = "budget", min_tokens = 1000, max_tokens = 10000, required = true },
  { parent = { mode = "off" } }
)
assert(required_parent_off.status == "unsupported" and required_parent_off.effective.mode == "off")

local minimum = resolve({ mode = "effort", level = "minimal" }, { mode = "effort", levels = { "high", "medium" } })
assert(minimum.status == "adjusted" and minimum.effective.level == "medium" and not minimum.approximate)
local budget_profile = { mode = "budget", min_tokens = 1000, max_tokens = 10000 }
for _, test in ipairs({
  { 500, 1000, "adjusted" },
  { 1000, 1000, "exact" },
  { 10000, 10000, "exact" },
  { 12000, 10000, "adjusted" },
}) do
  local value = resolve({ mode = "budget", tokens = test[1] }, budget_profile)
  assert(value.effective.mode == "budget" and value.effective.tokens == test[2] and value.status == test[3])
  assert(not value.approximate)
end
local ceiling = resolve({ mode = "budget", tokens = 9000 }, budget_profile, { max_tokens = 4000 })
assert(ceiling.effective.tokens == 4000 and ceiling.status == "adjusted")
local below_minimum = resolve({ mode = "budget", tokens = 9000 }, budget_profile, { max_tokens = 500 })
assert(below_minimum.status == "unsupported")

local preference = { mode = "effort", level = "high" }
local limited = resolve(
  preference,
  { mode = "effort", levels = { "low", "high" } },
  { parent = { mode = "effort", level = "low" } }
)
assert(preference.mode == "effort" and preference.level == "high")
assert(limited.requested.level == "high" and limited.effective.level == "low" and limited.status == "adjusted")
preference.level = "medium"
assert(limited.requested.level == "high")

local parent_minimum = resolve(
  { mode = "budget", tokens = 500 },
  budget_profile,
  { parent = { mode = "budget", tokens = 500 } }
)
assert(parent_minimum.status == "unsupported")
local parent_effort_minimum = resolve(
  { mode = "effort", level = "minimal" },
  { mode = "effort", levels = { "high" } },
  { parent = { mode = "effort", level = "low" } }
)
assert(parent_effort_minimum.status == "unsupported")
local unknown_parent = { mode = "budget", tokens = 1000 }
local unknown_bounded = resolve({ mode = "adaptive" }, nil, { parent = unknown_parent })
unknown_parent.tokens = 2000
assert(unknown_bounded.effective.tokens == 1000)

local boundary = resolve({ mode = "budget", tokens = 3277 }, { mode = "effort", levels = { "minimal", "low" } })
assert(boundary.effective.level == "low" and boundary.approximate)
local referenced = resolve(
  { mode = "effort", level = "high" },
  { mode = "effort", levels = { "high" } },
  { max_tokens = 10000 }
)
assert(referenced.effective.level == "high" and referenced.status == "exact")
local referenced_budget = resolve({ mode = "effort", level = "high" }, budget_profile, { max_tokens = 5000 })
assert(referenced_budget.effective.tokens == 3000 and referenced_budget.approximate)
local invalid_constraints, constraints_err = thinking.resolve({ mode = "off" }, nil, false)
assert(invalid_constraints == nil and constraints_err == "invalid thinking constraints")

for _, invalid in ipairs({
  { mode = "effort", levels = { [1] = "low", [3] = "high" } },
  { mode = "effort", levels = {} },
  { mode = "budget", min_tokens = 10000, max_tokens = 1000 },
  { mode = "none", required = true },
}) do
  local value, failure = thinking.resolve({ mode = "off" }, invalid)
  assert(value == nil and failure == "invalid thinking profile")
end
