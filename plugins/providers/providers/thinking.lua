-- Maps the requested thinking setting (`request.thinking`: `{ kind = "off" }`,
-- `{ kind = "adaptive" }`, `{ kind = "effort", level }` or
-- `{ kind = "budget", tokens }`) onto what one API can express. Providers
-- encode the result as returned.

local M = {}

M.MIN_BUDGET = 1024
M.FALLBACK_MAX_BUDGET = 32768
M.LEVELS = { "minimal", "low", "medium", "high", "xhigh", "max" }
M.OFF = "none"

local BUDGET_FIELD = "thinking_budget_tokens"
local PERCENT = { minimal = 10, low = 20, medium = 40, high = 60, xhigh = 80, max = 100 }
local RANK = {}
for rank, level in ipairs(M.LEVELS) do
  RANK[level] = rank
end

-- How an API spells effort: the levels it accepts (ascending), what adaptive
-- maps to (nil sends nothing and lets the API decide), and an explicit off.
M.dialects = {
  standard = { supported = { "minimal", "low", "medium", "high" }, adaptive = "medium" },
  prefer_high = { supported = { "low", "medium", "high" }, adaptive = "high" },
  high_only = { supported = { "high" }, adaptive = "high" },
  glm = { supported = { "high", "xhigh" }, adaptive = "high", off = "none" },
  deepseek = { supported = { "max" } },
  anthropic_adaptive = { supported = { "low", "medium", "high" } },
  tensorx = { supported = { "low", "medium", "high" }, adaptive = "high", off = "none" },
}

-- The most thinking one request may spend: half the model's declared output
-- window, at least `MIN_BUDGET`. Nil when the window is unknown.
function M.max_budget(model)
  local window = model.max_output_tokens
  return window and math.max(math.floor(window / 2), M.MIN_BUDGET)
end

function M.level_budget(level, max)
  max = math.max(max, M.MIN_BUDGET)
  return math.clamp(math.floor(max * PERCENT[level] / 100), M.MIN_BUDGET, max)
end

function M.budget_level(tokens, max)
  local percent = math.floor(tokens * 100 / math.max(max, 1))
  for _, level in ipairs(M.LEVELS) do
    if PERCENT[level] >= percent then
      return level
    end
  end
  return "max"
end

-- The effort levels among {values}, ascending and without repeats, and
-- whether {values} also names `none`, an explicit off.
function M.parse_levels(values)
  local levels = {}
  for _, level in ipairs(M.LEVELS) do
    if table.find(values, level) then
      levels[#levels + 1] = level
    end
  end
  return levels, table.find(values, M.OFF) ~= nil
end

-- The closest of {supported} at or below {level}, else the lowest supported.
function M.snap(level, supported)
  if #supported == 0 or table.find(supported, level) then
    return level
  end
  for i = #supported, 1, -1 do
    if RANK[supported[i]] < RANK[level] then
      return supported[i]
    end
  end
  return supported[1]
end

-- For token-budget APIs: "off", "adaptive" or a token count clamped to
-- `[MIN_BUDGET, max]`. An unknown {max} never caps a requested budget.
function M.budget(thinking, max)
  if thinking.kind == "off" or thinking.kind == "adaptive" then
    return thinking.kind
  elseif thinking.kind == "effort" then
    return M.level_budget(thinking.level, max or M.FALLBACK_MAX_BUDGET)
  elseif max then
    return math.clamp(thinking.tokens, M.MIN_BUDGET, math.max(max, M.MIN_BUDGET))
  end
  return math.max(thinking.tokens, M.MIN_BUDGET)
end

-- For effort APIs: the level to send for {dialect}, or nil to send nothing.
function M.effort(thinking, dialect, max)
  local level
  if thinking.kind == "off" then
    return dialect.off
  elseif thinking.kind == "adaptive" then
    level = dialect.adaptive
    if not level then
      return nil
    end
  elseif thinking.kind == "effort" then
    level = thinking.level
  else
    level = M.budget_level(thinking.tokens, max or M.FALLBACK_MAX_BUDGET)
  end
  return M.snap(level, dialect.supported)
end

-- Merges {fragment} into {body}, recursing into tables both sides hold.
function M.merge(body, fragment)
  for key, value in pairs(fragment) do
    if type(body[key]) == "table" and type(value) == "table" then
      M.merge(body[key], value)
    else
      body[key] = value
    end
  end
end

-- Local servers declare per-model `thinking_fields`: a body fragment for
-- `off`, `adaptive` and each effort level they accept. Merges the matching
-- fragment into {body}. Without one, `thinking_budget_tokens` carries the
-- setting (0 off, -1 adaptive), so a request never says nothing.
function M.apply_local(body, thinking, fields, max)
  local fragment, keep_budget
  if fields then
    if thinking.kind == "off" or thinking.kind == "adaptive" then
      fragment = fields[thinking.kind]
    else
      local declared = {}
      for _, level in ipairs(M.LEVELS) do
        if fields[level] then
          declared[#declared + 1] = level
        end
      end
      if #declared == 0 and thinking.kind == "budget" then
        fragment, keep_budget = fields.adaptive, true
      else
        local level = thinking.level or M.budget_level(thinking.tokens, max or M.FALLBACK_MAX_BUDGET)
        fragment = fields[M.snap(level, declared)] or fields.adaptive
      end
    end
  end
  local budget = M.budget(thinking, max)
  if fragment then
    M.merge(body, fragment)
    if keep_budget then
      body[BUDGET_FIELD] = budget
    end
    return
  end
  body[BUDGET_FIELD] = budget == "off" and 0 or budget == "adaptive" and -1 or budget
end

return M
