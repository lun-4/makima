local M = {}

local levels = { "minimal", "low", "medium", "high", "xhigh", "max" }
local percentages = { 10, 20, 40, 60, 80, 100 }
local fallback_max = 32768
local minimum_budget = 1024

local function level_index(level)
  for i, name in ipairs(levels) do
    if name == level then
      return i
    end
  end
end

local function positive_integer(value)
  return type(value) == "number" and value > 0 and value < math.huge and value % 1 == 0
end

local function validate_intent(intent)
  if type(intent) ~= "table" then
    return false
  end
  return intent.mode == "off"
    or intent.mode == "adaptive"
    or (intent.mode == "effort" and level_index(intent.level) ~= nil)
    or (intent.mode == "budget" and positive_integer(intent.tokens))
end

local function budget(intent, maximum)
  if intent.mode == "budget" then
    return intent.tokens
  end
  local amount = math.floor(maximum * percentages[level_index(intent.level)] / 100)
  return math.min(maximum, math.max(math.min(minimum_budget, maximum), amount))
end

local function valid_profile(profile)
  if type(profile) ~= "table" then
    return false
  end
  if profile.required ~= nil and type(profile.required) ~= "boolean" then
    return false
  end
  if profile.mode == "none" then
    return not profile.required
  elseif profile.mode == "budget" then
    return positive_integer(profile.min_tokens)
      and positive_integer(profile.max_tokens)
      and profile.min_tokens <= profile.max_tokens
  elseif profile.mode == "effort" or profile.mode == "adaptive" then
    if type(profile.levels) ~= "table" or #profile.levels == 0 then
      return false
    end
    local seen = {}
    local count = 0
    for key, level in pairs(profile.levels) do
      if
        type(key) ~= "number"
        or key % 1 ~= 0
        or key < 1
        or key > #profile.levels
        or not level_index(level)
        or seen[level]
      then
        return false
      end
      seen[level] = true
      count = count + 1
    end
    return count == #profile.levels
  end
  return false
end

---@class ThinkingConstraints
---@field parent? ThinkingPreference
---@field max_tokens? number Conversion reference and ceiling.
---@class ThinkingResolution
---@field requested ThinkingPreference
---@field effective ThinkingPreference
---@field status 'exact'|'adjusted'|'unsupported'
---@field approximate boolean
---@param requested ThinkingPreference
---@param profile ThinkingCapabilities|nil
---@param constraints ThinkingConstraints|nil
---@return ThinkingResolution|nil result
---@return string|nil error
function M.resolve(requested, profile, constraints)
  if constraints == nil then
    constraints = {}
  end
  if not validate_intent(requested) then
    return nil, "invalid thinking preference"
  end
  if profile ~= nil and not valid_profile(profile) then
    return nil, "invalid thinking profile"
  end
  if
    type(constraints) ~= "table"
    or (constraints.parent ~= nil and not validate_intent(constraints.parent))
    or (constraints.max_tokens ~= nil and not positive_integer(constraints.max_tokens))
  then
    return nil, "invalid thinking constraints"
  end

  local original = { mode = requested.mode, level = requested.level, tokens = requested.tokens }
  local function result(effective, status, approximate)
    return { requested = original, effective = effective, status = status, approximate = approximate }
  end

  local maximum = constraints.max_tokens or (profile and profile.max_tokens) or fallback_max
  local intent = requested
  local adjusted, approximate = false, false
  local parent = constraints.parent
  if parent and intent.mode ~= "off" then
    if parent.mode == "off" or (parent.mode ~= "adaptive" and intent.mode == "adaptive") then
      intent, adjusted = parent, true
    elseif parent.mode ~= "adaptive" then
      local exceeds
      if intent.mode == "effort" and parent.mode == "effort" then
        exceeds = level_index(intent.level) > level_index(parent.level)
      else
        exceeds = budget(intent, maximum) > budget(parent, maximum)
        approximate = intent.mode ~= parent.mode
      end
      if exceeds then
        intent, adjusted = parent, true
      end
    end
  end

  if not profile then
    return result({ mode = intent.mode, level = intent.level, tokens = intent.tokens }, "unsupported", approximate)
  end
  if profile.mode == "none" then
    return result({ mode = "off" }, intent.mode == "off" and not adjusted and "exact" or "adjusted", approximate)
  end
  if intent.mode == "off" then
    if not profile.required then
      return result({ mode = "off" }, adjusted and "adjusted" or "exact", approximate)
    end
    if parent and parent.mode == "off" then
      return result({ mode = "off" }, "unsupported", approximate)
    end
    intent, adjusted = { mode = "adaptive" }, true
  end

  local effective
  if profile.mode == "adaptive" and intent.mode == "adaptive" and not constraints.max_tokens then
    effective = { mode = "adaptive" }
  elseif profile.mode == "budget" then
    local ceiling = math.min(profile.max_tokens, maximum)
    if parent and parent.mode ~= "adaptive" and parent.mode ~= "off" then
      ceiling = math.min(ceiling, budget(parent, maximum))
      approximate = approximate or parent.mode == "effort"
    end
    if ceiling < profile.min_tokens then
      return result({ mode = "off" }, "unsupported", approximate)
    end
    local tokens
    if intent.mode == "adaptive" then
      tokens, adjusted = ceiling, true
    else
      tokens = budget(intent, maximum)
      if intent.mode == "effort" then
        approximate, adjusted = true, true
      end
    end
    local clamped = math.max(profile.min_tokens, math.min(ceiling, tokens))
    adjusted = adjusted or clamped ~= tokens
    effective = { mode = "budget", tokens = clamped }
  else
    local index
    if intent.mode == "adaptive" then
      index, adjusted = level_index("medium"), true
    elseif intent.mode == "budget" then
      local percent = intent.tokens * 100 / maximum
      index = #levels
      for i, value in ipairs(percentages) do
        if value >= percent then
          index = i
          break
        end
      end
      approximate, adjusted = true, true
    else
      index = level_index(intent.level)
    end
    local best, lowest
    for _, level in ipairs(profile.levels) do
      local candidate = level_index(level)
      if not lowest or candidate < lowest then
        lowest = candidate
      end
      if candidate <= index and (not best or candidate > best) then
        best = candidate
      end
    end
    best = best or lowest
    if parent and parent.mode == "effort" and best > level_index(parent.level) then
      return result({ mode = "off" }, "unsupported", approximate)
    end
    local effort_ceiling = constraints.max_tokens
    if parent and parent.mode == "budget" then
      effort_ceiling = math.min(effort_ceiling or parent.tokens, parent.tokens)
    end
    if effort_ceiling and budget({ mode = "effort", level = levels[best] }, maximum) > effort_ceiling then
      local constrained
      for _, level in ipairs(profile.levels) do
        local candidate = level_index(level)
        if
          candidate <= index
          and budget({ mode = "effort", level = level }, maximum) <= effort_ceiling
          and (not constrained or candidate > constrained)
        then
          constrained = candidate
        end
      end
      if not constrained then
        return result({ mode = "off" }, "unsupported", true)
      end
      best, adjusted, approximate = constrained, true, true
    end
    adjusted = adjusted or best ~= index
    effective = { mode = "effort", level = levels[best] }
  end
  return result(effective, adjusted and "adjusted" or "exact", approximate)
end

return M
