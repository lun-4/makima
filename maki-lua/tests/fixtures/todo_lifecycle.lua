local fixture = {
  tools = {},
  callbacks = {},
  windows = {},
  queries = {},
  pending = {},
  commits = 0,
}

local function resume(thread)
  local ok, err = coroutine.resume(thread)
  assert(ok, err)
end

maki = {
  json = fixture_json,
  api = {
    register_prompt_hint = function() end,
    register_tool = function(tool)
      fixture.tools[tool.name] = tool
    end,
    create_autocmd = function(events, spec)
      if type(events) == "string" then
        events = { events }
      end
      for _, event in ipairs(events) do
        fixture.callbacks[event] = spec.callback
      end
    end,
  },
  keymap = {
    set = function(_, key, callback)
      assert(key == "<C-t>")
      fixture.toggle = callback
    end,
  },
  ui = {
    buf = function()
      return {
        set_lines = function(self, lines)
          self.lines = lines
        end,
      }
    end,
    open_win = function(buf, config)
      local win = { buf = buf, visible = config.visible, session = fixture.session }
      function win:is_open()
        return true
      end
      function win:is_visible()
        return self.visible
      end
      function win:set_config(value)
        self.config = value
      end
      function win:show()
        self.visible = true
      end
      function win:hide()
        self.visible = false
      end
      fixture.windows[#fixture.windows + 1] = win
      fixture.window = win
      return win
    end,
    set_status_hint = function(hint)
      fixture.hint = hint
    end,
  },
}

function fixture.agent(id, items)
  local agent = { id = id, items = items }
  function agent:history_get(ctx, namespace)
    assert(ctx == fixture.ctx)
    assert(namespace == "todo_write.todos")
    fixture.queries[#fixture.queries + 1] = self.id
    local value = self.items
    if self.delayed then
      fixture.pending[#fixture.pending + 1] = coroutine.running()
      coroutine.yield()
    end
    return value, self.err
  end
  return agent
end

fixture.ctx = {
  history_get = function(_, namespace)
    assert(namespace == "todo_write.todos")
    return fixture.tool_items
  end,
  history_set = function(_, namespace, items)
    assert(namespace == "todo_write.todos")
    fixture.commits = fixture.commits + 1
    fixture.tool_items = items
    return true
  end,
}

function fixture.emit(event, agent, namespace)
  local session = agent and agent.session or "shared-session"
  if event == "AgentFocusChanged" then
    fixture.session = session
  end
  local callback = fixture.callbacks[event]
  if callback then
    resume(coroutine.create(function()
      callback({
        agent = agent,
        data = { agent_id = agent and agent.id, session_id = session, namespace = namespace },
      }, fixture.ctx)
    end))
  end
end

function fixture.release()
  local thread = table.remove(fixture.pending, 1)
  assert(thread, "no delayed query")
  resume(thread)
  assert(coroutine.status(thread) == "dead")
end

function fixture.text(body)
  local lines = {}
  for _, line in ipairs(body.lines) do
    local spans = {}
    for _, span in ipairs(line) do
      spans[#spans + 1] = span[1]
    end
    lines[#lines + 1] = table.concat(spans)
  end
  return table.concat(lines, "\n")
end

return fixture
