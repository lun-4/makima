local command
local callbacks
local messages
local checker
local main
local ctx
local verdict
local wait_error
local sent
local latest_id
local on_wait
local busy
local send_error
local cancellations
local pending
local on_send
local on_spawn
local config
local spawn_options
local spawns
local checks
local closed_subscriptions

maki = {
  api = {
    register_command = function(spec)
      command = spec.handler
    end,
    register_options = function(spec)
      config = {}
      for key, option in pairs(spec) do
        config[key] = option.default
      end
      return config
    end,
  },
  agent = {
    resolve_model = function()
      return { spec = "test" }
    end,
    spawn = function(_, opts)
      spawns = spawns + 1
      spawn_options = opts
      if on_spawn then
        on_spawn()
      end
      return checker
    end,
  },
  json = {
    encode = function()
      return "evidence"
    end,
  },
  ui = {
    post = function(_, text)
      messages[#messages + 1] = text
      return true
    end,
  },
  notify = function() end,
}

local goal = load_plugin()
assert(goal.setup == nil)
local function reset(setup_hook, initial_pending)
  callbacks, messages, sent = {}, {}, {}
  spawns, checks, closed_subscriptions = 0, 0, 0
  config.profile = "verdict"
  verdict = { status = "completed", captured = { complete = false, reason = "open", next_step = "work" } }
  wait_error = nil
  latest_id = "turn"
  on_wait = nil
  busy = false
  send_error = nil
  cancellations = 0
  pending = initial_pending ~= false
  on_send = nil
  on_spawn = setup_hook
  checker = {
    send = function(_, _, _, opts)
      checks = checks + 1
      assert(opts.when == "queue")
      return {
        wait = function(_, _, options)
          assert(options.timeout == 1)
          if on_wait then
            on_wait()
          end
          return verdict, wait_error
        end,
      }
    end,
    cancel = function()
      cancellations = cancellations + 1
    end,
    close = function(self)
      self.closed = true
    end,
  }
  local function subscribe(name)
    return function(_, _, callback)
      callbacks[name] = callback
      return {
        close = function()
          closed_subscriptions = closed_subscriptions + 1
        end,
      }
    end
  end
  main = {
    id = function()
      return "main"
    end,
    latest_turn = function()
      return {
        id = function()
          return latest_id
        end,
        result = function()
          if not pending then
            return { status = "completed", turn_id = latest_id }
          end
        end,
      }
    end,
    status = function()
      return { status = busy and "working" or "idle", queued = 0, active_turn = busy and latest_id or nil }
    end,
    transcript = function(_, _, opts)
      assert(opts.through_turn == latest_id)
      return {}
    end,
    send = function(_, _, _, opts)
      if send_error then
        return nil, send_error
      end
      sent[#sent + 1] = opts
      if on_send then
        on_send()
      end
      return {}
    end,
    on_turn_end = subscribe("end"),
    on_idle = subscribe("idle"),
    on_turn_start = subscribe("start"),
    on_close = subscribe("close"),
  }
  ctx = {
    agent = function()
      return main
    end,
  }
  config.timeout_secs = 1
  config.max_continuations = 1
  assert(command({ args = "test goal" }, ctx))
end

local function finish()
  pending = false
  local previous = #sent
  callbacks["end"]({ data = { status = "completed", turn_id = "turn" } }, ctx)
  assert(#sent >= previous)
end

reset()
finish()
assert(#sent == 1 and sent[1].when == "idle" and sent[1].after_turn == "turn")
verdict.captured.complete = true
latest_id = "next"
finish()
assert(not goal.status("main").active and checker.closed)
assert(spawns == 1 and checks == 2 and closed_subscriptions == 4)
assert(messages[#messages]:find("Goal complete", 1, true))

reset()
finish()
latest_id = "next"
finish()
assert(#sent == 1 and not goal.status("main").active and checker.closed)
assert(messages[#messages]:find("continuation limit", 1, true))

reset()
verdict, wait_error = nil, "agent turn wait timed out"
finish()
assert(not goal.status("main").active and checker.closed)

reset()
on_wait = function()
  latest_id = "new"
end
finish()
assert(#sent == 0 and goal.status("main").active)
command({ args = "off" }, ctx)

reset()
latest_id = "new"
finish()
assert(#sent == 1 and sent[1].after_turn == "new")
command({ args = "off" }, ctx)

reset()
latest_id = "new"
on_wait = function()
  callbacks["end"]({ data = { status = "completed", turn_id = "turn" } }, ctx)
end
pending = false
callbacks["end"]({ data = { status = "completed", turn_id = "new" } }, ctx)
assert(#sent == 1 and sent[1].after_turn == "new")
command({ args = "off" }, ctx)

reset()
busy = true
finish()
assert(#sent == 0)
busy = false
pending = false
callbacks.idle({ data = { settled = "not-a-turn-id" } }, ctx)
callbacks["end"]({ data = { turn_id = "old", status = "completed" } }, ctx)
assert(#sent == 1 and sent[1].after_turn == "turn")
command({ args = "off" }, ctx)

reset()
send_error = "agent has pending work"
finish()
assert(#sent == 0 and goal.status("main").active)
send_error = nil
callbacks.idle({}, ctx)
assert(#sent == 1)
command({ args = "off" }, ctx)

reset()
on_wait = function()
  on_wait = nil
  latest_id = "new"
  callbacks.start({ data = { turn_id = "new" } }, ctx)
end
finish()
assert(cancellations == 1 and #sent == 0)
callbacks.idle({}, ctx)
assert(#sent == 1 and sent[1].after_turn == "new")
command({ args = "off" }, ctx)

reset()
config.max_continuations = 2
command({ args = "two turns" }, ctx)
finish()
latest_id = "second"
finish()
assert(#sent == 2 and sent[1].after_turn == "turn" and sent[2].after_turn == "second")
assert(goal.status("main").continuations == 2)
command({ args = "off" }, ctx)

reset()
on_wait = function()
  on_wait = nil
  callbacks.start({ data = { turn_id = "old" } }, ctx)
end
finish()
assert(cancellations == 0 and #sent == 1)
command({ args = "off" }, ctx)

reset()
on_send = function()
  on_send = nil
  latest_id = "next"
  callbacks.start({ data = { turn_id = "next" } }, ctx)
  callbacks.idle({}, ctx)
end
finish()
assert(#sent == 1 and not goal.status("main").active)
assert(messages[#messages]:find("continuation limit", 1, true))
command({ args = "off" }, ctx)

reset(function()
  pending = false
end)
assert(#sent == 1 and sent[1].after_turn == "turn")
command({ args = "off" }, ctx)

reset(nil, false)
assert(#sent == 0)
callbacks.idle({}, ctx)
assert(#sent == 0)
command({ args = "off" }, ctx)

reset()
assert(command({ args = "status" }, ctx))
assert(messages[#messages] == "Goal (verdict, 0 continuations): test goal")
local status = goal.status("main")
assert(status.active and status.text == "test goal" and status.profile == "verdict")
assert(status.continuations == 0 and not status.checking)
assert(spawn_options.tools and next(spawn_options.tools) == nil)
assert(spawn_options.audience == "general_sub" and spawn_options.auto_deliver == false)
assert(command({ args = "off" }, ctx))
assert(messages[#messages] == "Goal cleared.")
assert(not goal.status("main").active and checker.closed and closed_subscriptions == 4)
assert(command({ args = "status" }, ctx))
assert(messages[#messages] == "No goal is set.")
assert(command({ args = "off" }, ctx))
assert(closed_subscriptions == 4)

for _, args in ipairs({ "test goal", "status", "off" }) do
  local ok, err = command({ args = args }, nil)
  assert(not ok and err == "no agent is available for /goal")
end
local ok, err = command({ args = "status" }, {
  agent = function()
    return nil, "agent closed"
  end,
})
assert(not ok and err == "agent closed")

reset()
config.profile = "verify"
config.max_continuations = 2
local previous_checker = checker
checker = {}
for key, value in pairs(previous_checker) do
  checker[key] = value
end
assert(command({ args = "verify goal" }, ctx))
assert(previous_checker.closed and spawns == 2 and not checker.closed)
assert(spawn_options.tools == nil)
assert(spawn_options.system:find("gather observable evidence", 1, true))
assert(goal.status("main").profile == "verify")
config.profile = "verdict"
finish()
latest_id = "second"
finish()
assert(spawns == 2 and checks == 2 and not checker.closed)
assert(goal.status("main").profile == "verify")
assert(goal.status("main").continuations == 2)
assert(command({ args = "off" }, ctx))
assert(checker.closed)
