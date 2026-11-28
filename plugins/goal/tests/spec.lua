local command, tool
local register_command, register_tool = makima.api.register_command, makima.api.register_tool
makima.api.register_command = function(spec)
  command = spec
  register_command(spec)
end
makima.api.register_tool = function(spec)
  tool = spec
  register_tool(spec)
end
load_plugin()
assert(command.name == "/goal" and command.tui_only and command.arguments.raw)
assert(tool.name == "goal" and not tool.enabled_by_default and tool.audiences[1] == "main")

local n = 0
local function id()
  n = n + 1
  return string.rep("A", 15) .. string.rep("a", n % 7 + 1)
end
local function new_goal()
  local record, verb = reducer.command(nil, id, "Run the regression suite", false)
  assert(record.state == "active" and verb == "set" and reducer.progress(record) == 0)
  return record
end
local function exposure(record, turn)
  return { kind = "exposed", turn = turn, scope = record.scope }
end
local function settlement(turn, outcome)
  return { kind = "settled", turn = turn, outcome = outcome or "finished" }
end
local function report(record, turn, op, existing, evidence)
  return reducer.claim(
    record,
    existing,
    { turn_id = turn, activation = record.revision, current = true },
    op,
    evidence or "Verified with the regression suite"
  )
end

local function goal_reducer_confirms_only_first_matching_normal_claim()
  local record, turn = new_goal(), id()
  local claim = assert(report(record, turn, "complete"))
  assert(report(record, turn, "complete", claim) == claim)
  assert(not report(record, turn, "blocked", claim))
  assert(not report(record, turn, "complete", nil, "   "))
  assert(not reducer.claim(record, nil, { turn_id = turn, activation = id(), current = true }, "complete", "stale"))
  for _, outcome in ipairs({ "failed", "cancelled", "max_tokens", "max_turns" }) do
    local result = reducer.reduce(record, id, { exposure(record, turn), settlement(turn, outcome) }, nil, claim)
    assert(result.state == "paused" and result.count == 1 and result.reason == outcome)
  end
  local complete = reducer.reduce(record, id, { exposure(record, turn), settlement(turn) }, nil, claim)
  assert(complete.state == "complete" and complete.count == 1)
  local next_record, _, _, eligible = reducer.reduce(record, id, { exposure(record, turn), settlement(turn) })
  assert(next_record.state == "active" and eligible == turn)
  local stopped = reducer.reduce(
    record,
    id,
    { exposure(record, turn), { kind = "stopped", reason = "plan mode" }, settlement(turn) },
    nil,
    claim
  )
  assert(stopped.state == "paused" and stopped.count == 1)
end

local function goal_reducer_counts_once_preserves_replacement_and_resets_on_clear()
  local record, turn = new_goal(), id()
  local claim = assert(report(record, turn, "complete"))
  local newer = assert(reducer.command(record, id, "Refined objective", false))
  assert(newer.scope == record.scope and newer.revision ~= record.revision)
  local facts = { exposure(record, turn), exposure(newer, turn), settlement(turn), settlement(turn) }
  local counted = reducer.reduce(newer, id, facts, nil, claim)
  assert(counted.state == "active" and counted.count == 1)
  local cleared = assert(reducer.command(counted, id, "clear", true))
  assert(cleared.scope ~= counted.scope and cleared.state == "cleared")
  local fresh = assert(reducer.command(cleared, id, "New objective", false))
  assert(reducer.reduce(fresh, id, facts).count == 0)
end

local function goal_reducer_pause_resume_and_allowance()
  local record = new_goal()
  assert(reducer.validate(record))
  assert(not reducer.validate({ version = 99 }))
  assert(not reducer.command(record, id, "Different objective", true))
  local paused = assert(reducer.command(record, id, "pause", true))
  assert(paused.state == "paused" and not reducer.command(paused, id, "resume", true))
  record = assert(reducer.command(paused, id, "resume", false))
  for i = 1, 9 do
    local turn = "turn-" .. i
    record = reducer.reduce(record, id, { exposure(record, turn), settlement(turn) })
    assert(record.count == i and record.state == "active")
  end
  local turn = "tenth"
  local facts = { exposure(record, turn), settlement(turn) }
  local complete = reducer.reduce(record, id, facts, nil, assert(report(record, turn, "complete")))
  assert(complete.state == "complete" and not reducer.command(complete, id, "resume", false))
  local blocked = reducer.reduce(record, id, facts, nil, assert(report(record, turn, "blocked")))
  assert(blocked.state == "blocked" and blocked.count == 10)
  local capped = reducer.reduce(record, id, facts)
  local replaced = assert(reducer.command(capped, id, "Refinement at cap", false))
  assert(replaced.state == "capped" and replaced.count == 10)
  local resumed = assert(reducer.command(replaced, id, "resume", false))
  assert(resumed.state == "active" and resumed.ceiling == 20 and resumed.count == 10 and reducer.progress(resumed) == 0)
end

goal_reducer_confirms_only_first_matching_normal_claim()
goal_reducer_counts_once_preserves_replacement_and_resets_on_clear()
goal_reducer_pause_resume_and_allowance()
