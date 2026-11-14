local failures = {}

local function case(name, fn)
  local ok, err = pcall(fn)
  if not ok then
    table.insert(failures, name .. ": " .. tostring(err))
  end
end

local function eq(actual, expected, msg)
  if actual ~= expected then
    error((msg or "") .. "\nexpected: " .. tostring(expected) .. "\n  actual: " .. tostring(actual))
  end
end

case("makima_agent_has_expected_functions", function()
  assert(type(makima.agent) == "table", "makima.agent must be a table")
  local expected = {
    "resolve_model",
    "system_prompt",
    "tools",
    "call_tool",
    "spawn",
    "root",
    "list",
    "get",
  }
  for _, fn_name in ipairs(expected) do
    eq(type(makima.agent[fn_name]), "function", "makima.agent." .. fn_name .. " must be a function")
  end
end)

case("schema_validator_compiles_and_validates", function()
  local validator, err = makima.json.schema_validator({
    type = "object",
    properties = { answer = { type = "string" } },
    required = { "answer" },
  })
  eq(err, nil, "valid schema must compile")
  eq(validator:validate({ answer = "42" }), nil, "matching value must produce no errors")
  local errors = validator:validate({ answer = 42 })
  assert(type(errors) == "table" and #errors > 0, "mismatch must produce error list")
end)

case("schema_validator_rejects_bad_schema", function()
  local validator, err = makima.json.schema_validator({ type = 42 })
  eq(validator, nil, "bad schema must not compile")
  assert(err ~= nil, "bad schema must return an error")
end)

case("task_policy_uses_durable_ids_and_current_authority", function()
  local originals = {
    command = makima.api.register_command,
    tool = makima.api.register_tool,
    spawn = makima.agent.spawn,
    get = makima.agent.get,
    register = makima.state.register,
    model = makima.agent.resolve_model,
    prompt = makima.agent.system_prompt,
    tools = makima.agent.tools,
  }
  local tools, command, stored
  tools = {}
  local context = {
    agent = function()
      return {
        id = function()
          return "owner-durable"
        end,
      }
    end,
    mode = function()
      return "build"
    end,
    tool_output_lines = function()
      return {}
    end,
  }
  local agent_id = "agent-retire-durable"
  local access_error = "retire target denied"
  local commit_error = "state_value_too_large"
  local accessible, reject_commit = true, false
  local retired, closed, admitted = 0, 0, 0
  local ticket = {
    id = function()
      return "retire-turn"
    end,
    wait = function()
      return { status = "completed", text = "retained" }
    end,
  }
  local agent = {
    id = function()
      return agent_id
    end,
    send = function()
      assert(stored[agent_id], "policy must precede admission")
      admitted = admitted + 1
      return ticket
    end,
    retire = function(_, ctx)
      eq(ctx, context)
      retired = retired + 1
      return true
    end,
    close = function()
      closed = closed + 1
      return true
    end,
    turn = function()
      return ticket
    end,
  }
  makima.state.register = function(name, spec)
    eq(name, "policy")
    eq(spec.scope, "session")
    eq(spec.version, 1)
    return {
      get = function()
        return stored, 0
      end,
      update = function(_, ctx, fn)
        eq(ctx, context)
        local next_value = fn(stored and makima.json.decode(makima.json.encode(stored)))
        if reject_commit then
          return nil, commit_error
        end
        stored = next_value
        return stored, 1
      end,
    }
  end
  makima.api.register_command = function(spec)
    command = spec
  end
  makima.api.register_tool = function(spec)
    tools[spec.name] = spec
  end
  makima.agent.spawn = function()
    return agent
  end
  makima.agent.get = function(ctx, id)
    eq(ctx, context)
    eq(id, agent_id)
    if not accessible then
      return nil, access_error
    end
    return agent
  end
  makima.agent.resolve_model = function()
    return { spec = "retire/model" }
  end
  makima.agent.system_prompt = function()
    return "retire prompt"
  end
  makima.agent.tools = function()
    return {}
  end
  local ok, err = pcall(function()
    load_plugin()
    reject_commit = true
    local rejected = tools.task_spawn.handler({ description = "retirement", prompt = "" }, context)
    eq(rejected.is_error, true)
    eq(rejected.llm_output, commit_error)
    eq(closed, 1)
    eq(retired, 1)
    eq(admitted, 0)
    eq(stored, nil)
    reject_commit = false
    local spawned = tools.task_spawn.handler({ description = "retirement", prompt = "" }, context)
    assert(not spawned.is_error, spawned.llm_output)
    eq(makima.json.decode(spawned.llm_output).task_id, agent_id)
    eq(stored[agent_id].owner, "owner-durable")
    eq(stored[agent_id].writes, false)
    accessible = false
    local denied = tools.task_stop.handler({ task_id = agent_id }, context)
    eq(denied.is_error, true)
    eq(denied.llm_output, access_error)
    eq(closed, 1)
    accessible = true
    local legacy = tools.task_despawn.handler({ task_id = "old-transcript-key" }, context)
    eq(legacy.llm_output, "unknown task_id")
    local despawned = tools.task_despawn.handler({ task_id = agent_id }, context)
    assert(not despawned.is_error, despawned.llm_output)
    eq(closed, 2)
    eq(retired, 1)
    command.handler({ values = { task_or_agent_id = agent_id } }, context)
    eq(retired, 2)
    eq(pcall(command.handler, { values = { task_or_agent_id = agent_id } }, nil), false)
    eq(tools.task_wait.header({ task_id = agent_id }), agent_id:sub(-8))
  end)
  makima.api.register_command = originals.command
  makima.api.register_tool = originals.tool
  makima.agent.spawn = originals.spawn
  makima.agent.get = originals.get
  makima.state.register = originals.register
  makima.agent.resolve_model = originals.model
  makima.agent.system_prompt = originals.prompt
  makima.agent.tools = originals.tools
  assert(ok, tostring(err))
end)

if #failures > 0 then
  error(#failures .. " case(s) failed:\n\n" .. table.concat(failures, "\n\n"))
end
