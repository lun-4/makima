local originals = {
  command = makima.api.register_command,
  tool = makima.api.register_tool,
  register = makima.state.register,
  get = makima.agent.get,
}
local POLICY_ERROR = "task policy unavailable"
local STATUS_ERROR = "status unavailable"
local context = {}
local tools = {}
local policy_error
local tasks
local agents = {}

local function eq(actual, expected)
  assert(actual == expected, "expected " .. tostring(expected) .. ", got " .. tostring(actual))
end

makima.api.register_command = function() end
makima.api.register_tool = function(spec)
  tools[spec.name] = spec
end
makima.state.register = function(name, spec)
  eq(name, "policy")
  eq(spec.scope, "session")
  eq(spec.version, 1)
  return {
    get = function(_, ctx)
      eq(ctx, context)
      return tasks, policy_error or 0
    end,
  }
end
makima.agent.get = function(ctx, id)
  eq(ctx, context)
  assert(tasks[id], "only resolve tasks from the policy")
  local status = agents[id]
  if not status then
    return nil, "agent unavailable"
  end
  return {
    status = function(_, status_ctx)
      eq(status_ctx, context)
      if status == "unavailable" then
        return nil, STATUS_ERROR
      end
      return status
    end,
  }
end

local ok, err = pcall(function()
  load_plugin()
  local tool = tools.task_list
  assert(tool, "task_list must be registered")
  eq(tool.schema.additionalProperties, false)
  local validator, validation_error = makima.json.schema_validator(tool.schema)
  eq(validation_error, nil)
  eq(validator:validate({}), nil)
  assert(validator:validate({ task_id = "unexpected" }))

  local function output()
    local result = tool.handler({}, context)
    assert(not result.is_error, result.llm_output)
    return result.llm_output
  end

  eq(output(), "No tasks.")
  tasks = {
    running = { description = "Active task" },
    closed = { description = "Closed task" },
    idle = { description = "Waiting task" },
    queued = { description = "Queued task" },
    missing = { description = "Missing agent" },
    dormant = { description = "Dormant task" },
  }
  agents = {
    running = { status = "working", queued = 0 },
    closed = { status = "closed", queued = 1 },
    idle = { status = "idle", live = true, queued = 0 },
    queued = { status = "idle", queued = 1 },
    unreadable = "unavailable",
    dormant = { status = "idle", live = false, queued = 0 },
    unrelated = { status = "working", queued = 0 },
  }
  eq(
    output(),
    table.concat({
      "closed: Closed task (done)",
      "dormant: Dormant task (idle)",
      "idle: Waiting task (idle)",
      "missing: Missing agent (done)",
      "queued: Queued task (running)",
      "running: Active task (running)",
    }, "\n")
  )

  tasks = { running = tasks.running }
  agents.running = { status = "idle", queued = 0 }
  eq(output(), "running: Active task (idle)")
  tasks.running.description = "Updated description"
  agents.running.status = "closed"
  eq(output(), "running: Updated description (done)")
  tasks = { unreadable = { description = "Unavailable status" } }
  local unreadable = tool.handler({}, context)
  eq(unreadable.is_error, true)
  eq(unreadable.llm_output, STATUS_ERROR)
  tasks = {}
  eq(output(), "No tasks.")
  policy_error = POLICY_ERROR
  local unavailable = tool.handler({}, context)
  eq(unavailable.is_error, true)
  eq(unavailable.llm_output, POLICY_ERROR)
end)

makima.api.register_command = originals.command
makima.api.register_tool = originals.tool
makima.state.register = originals.register
makima.agent.get = originals.get
assert(ok, tostring(err))
