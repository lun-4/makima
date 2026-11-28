local reducer = require("goal_reducer")
local KEY = "goal"
local sessions = {}

local function root_session(ctx)
  local root = ctx and ctx:agent()
  local graph_root = root and makima.agent.root(ctx)
  if not root or not graph_root or graph_root:id() ~= root:id() then
    return nil, "Goals are available only on the pinned session root."
  end
  return root
end

local function describe(record)
  if not record or record.state == "cleared" then
    return "No goal is set."
  end
  local status = string.format(
    "Goal (%s, %d/10, %d lifetime): %s",
    record.state,
    reducer.progress(record),
    record.count,
    record.objective
  )
  if record.reason then
    return status .. "\nReason: " .. record.reason
  end
  return status
end

local function update_badge(ctx, state)
  local record = state and state.record
  local spans = {}
  if record and record.state == "active" then
    local progress = reducer.progress(record)
    if state.exposed and state.exposed.scope == record.scope then
      progress = progress + 1
    end
    spans[1] = { string.format("goal:%d/10", progress), "#FFD700" }
  end
  local ok, err = makima.ui.set_root_status_content(ctx, spans)
  if not ok then
    makima.log.warn("Goal status: " .. tostring(err))
  end
end

local function reconcile(root, ctx, trigger, argument)
  local root_id = root:id()
  local state = sessions[root_id] or {}
  local function new_id()
    return root:new_revision_id()
  end
  local verb, notice, boundary_turn
  local result, err = root:tool_transition(ctx, KEY, function(boundary)
    boundary_turn = boundary.latest
    local record = state.record
    local original = record
    if not state.loaded then
      local decoded, read_err = boundary.record:read(KEY)
      if read_err then
        error(read_err)
      end
      if decoded then
        local validation_err
        record, validation_err = reducer.validate(decoded)
        if validation_err then
          error(validation_err)
        end
        original = record
        record = reducer.pause(record, new_id, "reopened")
      end
    end
    local updated, exposed, claim, eligible = reducer.reduce(record, new_id, boundary.facts, state.exposed, state.claim)
    if boundary.faulted then
      updated = reducer.pause(updated, new_id, "controller state uncertain")
      notice = "Goal controller is disabled; reopen the session before reactivating it."
    elseif boundary.closing then
      updated = reducer.pause(updated, new_id, "session closed or reloaded")
    elseif argument and argument ~= "" then
      local candidate, action = reducer.command(updated, new_id, argument, boundary.plan)
      if candidate then
        verb = action
        if candidate ~= updated then
          updated, claim = candidate, nil
        end
        eligible = nil
      else
        notice = action
      end
    end
    if updated ~= original then
      local written, write_err = boundary.record:write(KEY, updated)
      if not written then
        error(write_err)
      end
    end
    if updated ~= state.record and (not state.record or not updated or state.record.revision ~= updated.revision) then
      state.eligible = nil
    end
    state.record, state.exposed, state.claim, state.loaded = updated, exposed, claim, true
    if #boundary.facts > 0 then
      state.eligible = eligible
    end
    sessions[root_id] = state
    if
      updated
      and updated.state == "active"
      and not boundary.plan
      and not boundary.closing
      and not boundary.faulted
    then
      return { revision = updated.revision, scope = updated.scope }
    end
    return nil
  end)
  if not result then
    state.claim, state.eligible = nil, nil
    -- A durable write may already have committed; the next inspection must
    -- recover the authoritative record, not reuse a potentially older cache.
    sessions[root_id] = nil
    update_badge(ctx, nil)
    return nil, err
  end
  if trigger == "closing" then
    sessions[root_id] = nil
    update_badge(ctx, nil)
  elseif not verb then
    update_badge(ctx, state)
  end
  if trigger == "idle" and result.wake and state.eligible and state.eligible == boundary_turn then
    state.eligible = nil
    root:wake_with_tool(
      ctx,
      result.wake,
      "[system: continue toward the active goal "
        .. string.format("%q", state.record.objective)
        .. "; use the goal tool to report completion or blocking when verified]"
    )
  end
  return state, notice, verb
end

makima.api.register_tool({
  name = "goal",
  enabled_by_default = false,
  audiences = { "main" },
  description = "Read the active goal with status. Report complete only with concrete verification, or blocked only with evidence of a concrete external dependency that prevents further progress. Reports are provisional until the current turn finishes normally.",
  schema = {
    type = "object",
    properties = {
      op = { type = "string", enum = { "status", "complete", "blocked" }, required = true },
      evidence = { type = "string", description = "Concrete evidence, required for complete and blocked." },
    },
  },
  lifecycle = function(trigger, ctx)
    local root, err = root_session(ctx)
    if not root then
      error(err)
    end
    local state, reconcile_err = reconcile(root, ctx, trigger)
    if not state then
      error(reconcile_err)
    end
    if reconcile_err then
      makima.log.warn(reconcile_err)
    end
  end,
  handler = function(input, ctx)
    local root = root_session(ctx)
    local state = root and sessions[root:id()]
    local attribution = ctx:tool_claim()
    local record = state and state.record
    if
      not record
      or record.state ~= "active"
      or not attribution
      or not attribution.current
      or attribution.activation ~= record.revision
    then
      return { llm_output = "This request was not offered the current goal activation.", is_error = true }
    end
    if input.op == "status" then
      return { llm_output = describe(record) }
    end
    local claim, err = reducer.claim(record, state.claim, attribution, input.op, input.evidence)
    if not claim then
      return { llm_output = err, is_error = true }
    end
    state.claim = claim
    return {
      llm_output = "Provisional "
        .. claim.state
        .. " report accepted. It is confirmed only if this turn finishes normally.",
    }
  end,
})

makima.api.register_command({
  name = "/goal",
  description = "Show, set, pause, resume, or clear the root session goal",
  argument_hint = "[<objective>|pause|resume|clear]",
  arguments = { raw = true },
  tui_only = true,
  handler = function(opts, ctx)
    local root, root_err = root_session(ctx)
    if not root then
      makima.ui.flash(root_err)
      return
    end
    local argument = reducer.trim(opts.args or "")
    local state, notice, verb = reconcile(root, ctx, "command", argument)
    if not state or notice then
      makima.ui.flash("Goal: " .. tostring(notice))
      return
    end
    if not verb then
      if argument == "pause" or argument == "clear" then
        root:remove_tool(ctx, KEY)
      end
      makima.ui.flash(describe(state.record))
      return
    end
    local record = state.record
    if record.state == "active" then
      local attached, attach_err = root:add_tool(ctx, KEY)
      if not attached then
        root:remove_tool(ctx, KEY)
        local paused, pause_err = reconcile(root, ctx, "command", "pause")
        local message = "Goal was not attached: " .. tostring(attach_err)
        if not paused then
          message = message .. "; could not pause the record: " .. tostring(pause_err)
        end
        makima.ui.flash(message)
        return
      end
    else
      root:remove_tool(ctx, KEY)
    end
    update_badge(ctx, state)
    local announcement = "[system: goal " .. verb .. " to " .. string.format("%q", record.objective)
    if record.state == "active" then
      announcement = announcement .. "; use the goal tool]"
    else
      announcement = announcement .. "; the turn allowance is exhausted; use /goal resume to extend it]"
    end
    -- No turn or queue entry is reserved by attachment or record publication.
    local sent, send_err = root:send(ctx, announcement, { when = "join" })
    if not sent then
      makima.ui.flash("Goal is " .. record.state .. " but its announcement was not queued: " .. tostring(send_err))
    elseif record.state == "capped" then
      makima.ui.flash("Goal allowance exhausted; use /goal resume to extend it.")
    end
  end,
})
