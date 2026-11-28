local M = {}
local WINDOW = 10
local MAX_COUNT = 1000000000
local MAX_TEXT = 2048
local states = { active = true, paused = true, complete = true, blocked = true, capped = true, cleared = true }

function M.trim(text)
  return (text:gsub("^%s+", ""):gsub("%s+$", ""))
end

function M.text(value)
  if type(value) == "string" and #value <= MAX_TEXT and utf8.len(value) and M.trim(value) ~= "" then
    return value
  end
  return nil
end

local function identifier(value)
  return type(value) == "string" and #value >= 16 and #value <= 22 and value:match("^[1-9A-HJ-NP-Za-km-z]+$") ~= nil
end

local function count(value)
  return type(value) == "number" and value >= 0 and value <= MAX_COUNT and value % 1 == 0
end

function M.validate(record)
  if
    type(record) ~= "table"
    or record.version ~= 1
    or not identifier(record.revision)
    or not identifier(record.scope)
    or not states[record.state]
    or not count(record.count)
    or not count(record.ceiling)
    or record.ceiling < WINDOW
    or record.ceiling % WINDOW ~= 0
    or record.count > record.ceiling
    or record.count < record.ceiling - WINDOW
    or (record.reason ~= nil and not M.text(record.reason))
    or (record.last_counted ~= nil and not identifier(record.last_counted))
    or (record.state == "active" and record.count == record.ceiling)
    or (record.state == "capped" and record.count ~= record.ceiling)
  then
    return nil, "invalid or unsupported goal record"
  end
  if record.state == "cleared" then
    if record.objective ~= nil or record.count ~= 0 or record.ceiling ~= WINDOW or record.last_counted ~= nil then
      return nil, "invalid cleared goal record"
    end
  elseif not M.text(record.objective) then
    return nil, "invalid goal objective"
  end
  -- Discard unknown fields rather than copying them into subsequent records.
  return {
    version = 1,
    revision = record.revision,
    scope = record.scope,
    state = record.state,
    objective = record.objective,
    reason = record.reason,
    count = record.count,
    ceiling = record.ceiling,
    last_counted = record.last_counted,
  }
end

function M.progress(record)
  return WINDOW - (record.ceiling - record.count)
end

local function transition(record, id, state, reason)
  local next_record = table.clone(record)
  next_record.state, next_record.reason, next_record.revision = state, reason, id()
  return next_record
end

function M.pause(record, id, reason)
  if record and record.state == "active" then
    return transition(record, id, "paused", reason)
  end
  return record
end

function M.command(record, id, argument, plan)
  if argument == "clear" then
    return {
      version = 1,
      revision = id(),
      scope = id(),
      state = "cleared",
      count = 0,
      ceiling = WINDOW,
    }
  end
  if argument == "pause" then
    if not record or record.state == "cleared" then
      return nil, "No goal is set."
    end
    return M.pause(record, id, "paused by user")
  end
  if plan then
    return nil, "Leave plan mode before setting or resuming a goal."
  end
  if argument == "resume" then
    if not record or record.state == "cleared" then
      return nil, "No goal is set."
    end
    if record.state == "active" or record.state == "complete" then
      return nil, "Cannot resume a goal that is " .. record.state .. "."
    end
    local next_record = transition(record, id, "active", nil)
    if next_record.count == next_record.ceiling then
      if next_record.ceiling > MAX_COUNT - WINDOW then
        return nil, "Goal lifetime limit reached. Clear it before starting another allowance."
      end
      next_record.ceiling = next_record.ceiling + WINDOW
    end
    return next_record, "resumed"
  end
  if not M.text(argument) then
    return nil, "Goal objective must be nonblank UTF-8 text (at most " .. MAX_TEXT .. " bytes)."
  end
  local verb = "changed"
  if not record or record.state == "cleared" then
    verb = "set"
    record = { version = 1, scope = id(), count = 0, ceiling = WINDOW }
  end
  local capped = record.count >= record.ceiling
  local next_record = transition(record, id, capped and "capped" or "active", capped and "allowance exhausted" or nil)
  next_record.objective = argument
  return next_record, verb
end

function M.claim(record, existing, attribution, op, evidence)
  if
    not record
    or record.state ~= "active"
    or not attribution
    or not attribution.current
    or attribution.activation ~= record.revision
  then
    return nil, "This request was not offered the current goal activation."
  end
  if op ~= "complete" and op ~= "blocked" then
    return nil, "Expected op = status, complete or blocked."
  end
  if not M.text(evidence) then
    return nil, "Provide nonblank evidence (at most " .. MAX_TEXT .. " bytes)."
  end
  if existing and existing.revision == record.revision and existing.turn == attribution.turn_id then
    if existing.state == op and existing.evidence == evidence then
      return existing
    end
    return nil, "This turn has already reported an outcome for this goal revision."
  end
  return { turn = attribution.turn_id, revision = record.revision, state = op, evidence = evidence }
end

-- Only the actor produces these ordered facts. An exposure is process-only
-- until its exact turn retires. A changed revision may keep the same scope.
function M.reduce(record, id, facts, exposed, claim)
  local eligible
  for _, fact in ipairs(facts) do
    if fact.kind == "exposed" then
      exposed = { turn = fact.turn, scope = fact.scope }
    elseif fact.kind == "stopped" then
      record = M.pause(record, id, fact.reason)
      claim, eligible = nil, nil
    elseif fact.kind == "settled" then
      local qualifies = exposed
        and exposed.turn == fact.turn
        and record
        and exposed.scope == record.scope
        and record.state ~= "cleared"
        and record.last_counted ~= fact.turn
      if exposed and exposed.turn == fact.turn then
        exposed = nil
      end
      if qualifies then
        record = table.clone(record)
        record.count = record.count + 1
        record.last_counted = fact.turn
      end
      eligible = nil
      if record and record.state == "active" then
        if
          fact.outcome == "finished"
          and qualifies
          and claim
          and claim.turn == fact.turn
          and claim.revision == record.revision
        then
          record = transition(record, id, claim.state, claim.evidence)
        elseif fact.outcome ~= "finished" then
          record = M.pause(record, id, fact.outcome)
        elseif record.count >= record.ceiling then
          record = transition(record, id, "capped", "allowance exhausted")
        elseif qualifies then
          eligible = fact.turn
        end
      end
      if claim and claim.turn == fact.turn then
        claim = nil
      end
    end
  end
  return record, exposed, claim, eligible
end

return M
