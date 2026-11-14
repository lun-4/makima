local ToolView = require("makima.tool_view")
local output_limits = require("makima.output_limits")
local plan_spec = require("makima.plan_spec")

local TURN_UNAVAILABLE_ERR = "turn_result_unavailable"
local GENERAL_SEND_IN_PLAN_ERR =
  "general subagents cannot take new work in plan mode; despawn the task or switch to build mode"
local GENERAL_BLOCKED_IN_PLAN_ERR =
  "general subagents are blocked in plan mode; use research or plan_reviewer (read-only)"
local UNKNOWN_TASK_ERR = "unknown task_id"
local TASK_ACCESS_ERR = "task is owned by another agent branch"
local RETIRE_CONTEXT_ERR = "task-retire requires an agent context"
local TURN_WAIT_TIMEOUT = "agent turn wait timed out"
local QUOTE_BAR = "│ "
local SEND_FAILED_PREFIX = "Send failed: "
local SPAWN_FAILED_PREFIX = "Spawn failed: "
local DESPAWN_FAILED_PREFIX = "Despawn failed: "
local DETAILS_HINT = "Details (click to expand)"
local BODY_INDENT_COLS = 4
local MIN_MD_WIDTH = 20
local DEFAULT_OUTPUT_LINES = 5

local description = [[Launch an autonomous subagent to perform tasks independently. Best combined with batch.

Subagent types (set via `subagent_type`):
- `research` (default): Read-only tools. For codebase exploration or gathering context.
- `general`: Full tool access. For delegating implementation work.
- `plan_reviewer`: Read-only audit of a finished plan. Only available in plan mode. Evaluates shape, test-to-acceptance-criteria coverage, and severity of risks, and answers with VERDICT: pass|fail.

Use `task_spawn` to start work and `task_wait` to observe its exact turn. A
waiting agent yields its turn capacity while its descendants run. Submit
follow-up instructions with `task_send` and close the child subtree with `task_despawn`.

Notes:
1. Launch multiple tasks concurrently when possible.
2. The agent's result is not visible to the user. Summarize it in your response.
3. Each invocation starts fresh - inline any needed context into the prompt.
4. Tell it to return concise summaries with file:line refs, not full file contents.
5. If a `@model:` reference next to a subagent requests an exact model but the `model` parameter is not in this tool's schema (the `allow_model` option is off), the model is unavailable. Do not silently substitute another model: if the `question` tool is loaded, ask the user how to proceed; otherwise reject the request and ask the user to clarify.
]]

-- Read-only plan reviewer directive: a verbatim port of pi-luna's reviewer
-- prompt (subagents.ts), with makima's embedded-spec mechanic.
-- The mode gate keeps this spawnable only in plan.
local PLAN_REVIEWER_PROMPT = [[You are the **plan_reviewer** subagent: a read-only plan reviewer spawned by the primary agent. Your only tools are read, grep, glob, list — no shell, no writes, no subagent spawns. Your task message names the plan file path; the plan specification (a markdown document defining the required plan shape) is embedded in your system prompt. Read the plan file yourself and judge it against the embedded specification — the files are the truth. When the caller included the user's original reque...

## Review work
1. Verify the plan follows the specification's shape: all required sections present (Goal, Implementation Summary, Implementation Plan, Acceptance Criteria, Test Strategy, Review Strategy, Documentation Strategy, Risks/Blockers/Required Decisions); acceptance criteria named AC.1, AC.2, …; each criterion observable, not a restatement of steps; implementation unknowns resolved; no plan-of-plans.
2. Audit test-to-acceptance-criteria coverage — a core responsibility, not a nice-to-have. Every criterion needs at least one named test that would fail if the behavior regressed. Flag criteria with no mapped test (at least medium; high for core behavior), euphemisms such as "code inspection" / "implicitly verified" / "covered by existing tests", pre-existing tests that would pass regardless, and test-layer mismatches (pure logic tested only at integration level, or full-stack behavior mocked ...
3. Assess test infrastructure adequacy. If a behavior cannot be adequately tested because the harness does not exist, the plan should include a phase to build the missing infrastructure (implementation phase + acceptance criteria + tests for the infrastructure itself). High if a core behavior is untestable and the plan neither includes the infrastructure nor acknowledges the gap; medium if the gap is acknowledged but not built; low if coverage could simply be stronger.
4. Orient yourself in the relevant repository code. Inspect enough to judge whether the plan reflects the real implementation surfaces and likely contracts.
5. Assess replace-vs-edit trade-offs. Flag incremental editing when replacement would be cleaner: high-churn edits (>~60% of a module's substantive lines), accumulated complexity, contract changes (signature/return type/error model/invariants), wrong underlying structure, or workarounds around code that should be replaced. High when edits would likely produce bugs or unmaintainable code; medium for quality concerns.

## Severity guide
- critical: unsafe to hand off; execution would likely fail badly, corrupt state, violate an explicit instruction, or miss the core goal.
- high: major gap — missing required contract, wrong file/module, missing review loop, or likely test failure.
- medium: executable but with a meaningful quality, coverage, sequencing, or maintainability issue.
- low: minor improvement, clarity issue, or small risk that does not block handoff.

Report findings classified by severity (critical / high / medium / low), quoting the plan where relevant, and end with a final line `VERDICT: pass` or `VERDICT: fail` (fail when any critical or high finding remains). If there are no findings, say so and pass. Your FINAL message is the findings report — it is delivered back to the primary automatically when you settle.

]] .. plan_spec

local opts = makima.api.register_options({
  allow_model = {
    default = false,
    desc = "Expose a `model` input that overrides the subagent model. Only enable if you trust callers to pick an exact model themselves.",
  },
})

local spawn_schema = {
  type = "object",
  required = { "description", "prompt" },
  additionalProperties = false,
  properties = {
    description = {
      type = "string",
      description = "Short (3-5 words) description of the task",
    },
    prompt = {
      type = "string",
      description = "Detailed task prompt for the agent",
    },
    subagent_type = {
      type = "string",
      description = 'Subagent type: "research" (read-only, default), "general" (can modify files), or "plan_reviewer" (read-only plan audit, plan mode only)',
    },
    model_tier = {
      type = "string",
      description = 'Model tier (optional, omit to use current model, capped at current tier):\n- "strong" (e.g. Opus): Deep reasoning, complex architecture, subtle bugs, most critical sections. ~5x cost of medium.\n- "medium" (e.g. Sonnet): Balanced. Refactors, features, multi-file changes.\n- "weak" (e.g. Haiku): Fast/cheap. Search, summarize, boilerplate, simple edits.',
    },
    output_schema = {
      type = "object",
      properties = {},
      additionalProperties = true,
      description = "JSON Schema the subagent's final result must match. task_wait returns the validated object in result.captured; result.text contains assistant text.",
    },
    thinking = {
      description = "Thinking: off|adaptive|minimal|low|medium|high|xhigh|max|int budget. Omit to inherit parent; capped at parent.",
    },
  },
}

-- Only advertise `model` when the plugin opts in: it costs tokens in every
-- task schema, and an off-by-default flag keeps the common path lean.
if opts.allow_model then
  spawn_schema.properties.model = {
    type = "string",
    description = 'Exact model spec, e.g. "ollama/glm-5.2". You tell makima the model; makima will not guess. Overrides model_tier.',
  }
end

local policy = makima.state.register("policy", { scope = "session", version = 1 })

local function task_policy(ctx)
  local value, revision = policy:get(ctx)
  if type(revision) == "string" then
    return nil, revision
  end
  return value or {}
end

-- The executing agent's own mode, never the focused session's. Plan-specific
-- gating stays here until plan mode becomes a guest plugin.
local function current_mode(ctx)
  return ctx:mode()
end

-- Validates the shared setup for a subagent and returns
-- `spec, err` where spec holds the model/system/tools/audience/local_tools.
local function prepare(input, ctx)
  local subagent_type = input.subagent_type or "research"
  if subagent_type ~= "research" and subagent_type ~= "general" and subagent_type ~= "plan_reviewer" then
    return nil, { llm_output = "unknown subagent type: " .. subagent_type, is_error = true }
  end
  local mode, mode_err = current_mode(ctx)
  if mode_err then
    return nil, { llm_output = mode_err, is_error = true }
  end
  if subagent_type == "plan_reviewer" and mode ~= "plan" then
    return nil, { llm_output = "plan_reviewer is only available in plan mode", is_error = true }
  end
  if subagent_type == "general" and mode == "plan" then
    return nil, { llm_output = GENERAL_BLOCKED_IN_PLAN_ERR, is_error = true }
  end

  local model, model_err = makima.agent.resolve_model(ctx, {
    tier = input.model_tier,
    spec = opts.allow_model and input.model or nil,
  })
  if model_err then
    return nil, { llm_output = model_err, is_error = true }
  end

  local audience = subagent_type == "general" and "general_sub" or "research_sub"
  local system
  local system_err
  if subagent_type == "plan_reviewer" then
    system = PLAN_REVIEWER_PROMPT
  else
    local prompt_id = subagent_type == "research" and "research" or "general"
    system, system_err = makima.agent.system_prompt(ctx, {
      prompt_id = prompt_id,
      instructions = true,
    })
  end
  if system_err then
    return nil, { llm_output = system_err, is_error = true }
  end

  local tool_defs, tools_err = makima.agent.tools(ctx, {
    audience = audience,
    spec = model.spec,
  })
  if tools_err then
    return nil, { llm_output = tools_err, is_error = true }
  end

  return {
    model = model,
    system = system,
    tools = tool_defs,
    audience = audience,
    output_schema = input.output_schema,
    subagent_type = subagent_type,
  },
    nil
end

local function spawn_options(spec, input)
  return {
    model = { spec = spec.model.spec, thinking = input.thinking },
    system = spec.system,
    tools = spec.tools,
    output_schema = spec.output_schema,
    audience = spec.audience,
    name = input.description,
  }
end

local function project_result(result)
  local output = result.output or {}
  return {
    text = result.text,
    duration_ms = result.duration_ms,
    input_tokens = result.input_tokens,
    output_tokens = result.output_tokens,
    captured = result.captured or output[#output],
    error = result.error or (result.status == "cancelled" and (result.cancellation_reason or "cancelled")),
    user_message = result.user_message,
    retryable = result.retryable,
    turn_id = result.turn_id,
    status = result.status,
    cancellation_reason = result.cancellation_reason,
  }
end

local function enqueue(agent, message, ctx)
  local ok, ticket, err = pcall(agent.send, agent, ctx, message, { when = "join" })
  if not ok then
    return nil, tostring(ticket)
  end
  return ticket and ticket:id(), err
end

local function spawn(spec, input, ctx)
  local owner, owner_err = ctx:agent()
  if not owner then
    return nil, owner_err
  end
  local agent, spawn_err = makima.agent.spawn(ctx, spawn_options(spec, input))
  if not agent then
    return nil, spawn_err
  end
  local task_id = agent:id()
  local committed = false
  local ok, admitted, err = pcall(function()
    local value, revision = policy:update(ctx, function(tasks)
      tasks = tasks or {}
      if tasks[task_id] then
        error("duplicate task_id")
      end
      tasks[task_id] = {
        description = input.description,
        writes = spec.subagent_type == "general",
        owner = owner:id(),
      }
      return tasks
    end)
    if not value then
      return nil, revision
    end
    committed = true
    return enqueue(agent, input.prompt, ctx)
  end)
  if not ok or not admitted then
    local failure = tostring(ok and err or admitted)
    local closed, close_err = agent:close(ctx)
    if not closed then
      failure = failure .. "; cleanup close failed for " .. task_id .. ": " .. tostring(close_err)
    else
      local retired, retire_err = agent:retire(ctx)
      if not retired then
        failure = failure .. "; cleanup retire failed for " .. task_id .. ": " .. tostring(retire_err)
      end
    end
    if committed then
      local removed, remove_err = policy:update(ctx, function(tasks)
        tasks[task_id] = nil
        return tasks
      end)
      if not removed then
        failure = failure .. "; policy cleanup failed: " .. tostring(remove_err)
      end
    end
    return nil, failure
  end
  return { task_id = task_id, turn_id = admitted }
end

local function rendered_reply(rendered, output, is_error, state)
  rendered.llm_output = output
  rendered.is_error = is_error
  rendered.state = state
  return rendered
end

local function label_buf(text)
  local buf = makima.ui.buf()
  buf:line(text)
  return buf
end

local function resolve_task(task_id, ctx)
  local tasks, policy_err = task_policy(ctx)
  if not tasks then
    return nil, policy_err
  end
  local task = tasks[task_id]
  if not task then
    return nil, UNKNOWN_TASK_ERR
  end
  local agent, err = makima.agent.get(ctx, task_id)
  if not agent then
    return nil, err
  end
  return task, nil, agent
end

makima.api.register_command({
  name = "/task-retire",
  description = "Retire an accessible quiescent leaf agent, preserving its history and results.",
  tui_only = false,
  argument_hint = "<task_or_agent_id>",
  arguments = {
    { name = "task_or_agent_id", type = "string" },
  },
  handler = function(opts, ctx)
    if not ctx then
      error(RETIRE_CONTEXT_ERR)
    end
    local id = opts.values.task_or_agent_id
    local agent, err = makima.agent.get(ctx, id)
    if not agent then
      error(err)
    end
    local ok
    ok, err = agent:retire(ctx)
    if not ok then
      error(err)
    end
  end,
})

-- task_despawn ---------------------------------------------------------------

local despawn_schema = {
  type = "object",
  required = { "task_id" },
  additionalProperties = false,
  properties = {
    task_id = { type = "string", description = "Task id returned by task_spawn." },
  },
}

local stop_schema = {
  type = "object",
  required = { "task_id" },
  additionalProperties = false,
  properties = {
    task_id = {
      type = "string",
      description = "Task id returned by task_spawn, or the call id of a tool moved to the background.",
    },
  },
}

local wait_schema = {
  type = "object",
  required = { "task_id" },
  additionalProperties = false,
  properties = {
    task_id = { type = "string", description = "Task id returned by task_spawn." },
    turn_id = { type = "string", description = "Exact admitted turn. Omit to snapshot the latest retained turn once." },
    timeout = {
      type = "integer",
      description = "Observation timeout in seconds. Zero checks without waiting; timeout never cancels work.",
    },
  },
}

local function wait_label(input, state)
  if state and state.description then
    return state.description
  end
  return input.task_id:sub(-8)
end

-- Shows {text} as plain lines at once and swaps in its Markdown rendering
-- from a background task, then calls {on_rendered} so the caller redraws.
-- Rendering hops to the highlight thread, which a busy machine can make slow,
-- so a tool call must never wait on it. Unfinished so callers can append a
-- fallback line before `view:finish()`. A `prefix` span starts every line,
-- narrowing the wrap width to fit it.
local function markdown_preview(text, ctx, prefix, on_rendered)
  local tol = ctx and ctx:tool_output_lines()
  local preview = makima.ui.buf()
  local view = ToolView.new(preview, {
    max_lines = (tol and tol.task_spawn) or DEFAULT_OUTPUT_LINES,
    keep = "head",
    max_line_bytes = output_limits.DEFAULT_MAX_LINE_BYTES,
  })
  if type(text) ~= "string" or text == "" then
    return preview, view
  end
  local function append_lines(lines)
    for _, line in ipairs(lines) do
      if prefix then
        table.insert(line, 1, { prefix, "dim" })
      end
      view:append(line)
    end
  end
  local plain = {}
  for _, line in ipairs(makima.split(text, "\n")) do
    plain[#plain + 1] = { { line } }
  end
  append_lines(plain)
  local prefix_cols = prefix and utf8.len(prefix) or 0
  local width = math.max(makima.ui.terminal_size().cols - BODY_INDENT_COLS - prefix_cols, MIN_MD_WIDTH)
  makima.async.run(function()
    local ok, lines = pcall(makima.ui.markdown, text, width)
    if not ok then
      return
    end
    view:clear()
    append_lines(lines)
    view:finish()
    on_rendered()
  end)
  return preview, view
end

local function details_view(status, style, preview, view, details)
  local buf = makima.ui.buf()
  local function render()
    buf:set_lines({ { { status, style } } })
    if preview:len() > 0 then
      buf:line("")
      buf:lines(preview:get_lines())
    end
    if view.expanded then
      buf:line("")
      for _, detail in ipairs(details) do
        buf:line({ { detail, "dim" } })
      end
    elseif view.skipped < 2 then
      buf:line({ { DETAILS_HINT, "dim" } })
    end
  end
  render()
  buf:on("click", function()
    view:toggle()
    render()
  end)
  return buf, render
end

local function wait_view(input, output, is_error, ctx, state)
  local decoded = not is_error and makima.json.decode(output) or nil
  local envelope = type(decoded) == "table" and decoded or {}
  local result = type(envelope.result) == "table" and envelope.result or {}
  local status, style, report = "Completed", "dim", result.text
  if is_error then
    local errors = {
      [TURN_UNAVAILABLE_ERR] = "Cannot retrieve this turn: its result is no longer retained.",
      [UNKNOWN_TASK_ERR] = "Cannot retrieve this task: it is no longer available.",
      [TASK_ACCESS_ERR] = "Cannot retrieve this task: it belongs to another agent branch.",
    }
    status, style = errors[output] and (errors[output] .. " (" .. output .. ")") or ("Wait failed: " .. output), "error"
  elseif envelope.status == "running" then
    status = "Still running when checked. The wait timed out; the task was not cancelled."
  elseif result.status == "cancelled" then
    status, style =
      "Subagent cancelled: " .. tostring(result.error or result.cancellation_reason or "cancelled"), "error"
  elseif envelope.error or result.error or result.status == "failed" then
    local err = result.user_message or result.error or envelope.error or "unknown error"
    status, style = "Subagent failed: " .. tostring(err), "error"
  elseif envelope.status ~= "done" then
    status, report = "Wait result", output
  end
  if type(result.duration_ms) == "number" then
    status = status .. " · " .. makima.ui.humantime(result.duration_ms / 1000)
  end

  local redraw
  local preview, view = markdown_preview(report, ctx, nil, function()
    redraw()
  end)
  if (type(report) ~= "string" or report == "") and envelope.status == "done" and style ~= "error" then
    view:append({
      { result.captured ~= nil and "Structured result available in details." or "No report returned.", "dim" },
    })
  end
  view:finish()

  local details = { "Task: " .. input.task_id }
  local turn_id = result.turn_id or input.turn_id
  if turn_id then
    details[#details + 1] = "Turn: " .. turn_id
  end
  if result.input_tokens or result.output_tokens then
    details[#details + 1] = "Tokens: "
      .. tostring(result.input_tokens or 0)
      .. " input, "
      .. tostring(result.output_tokens or 0)
      .. " output"
  end
  if result.captured ~= nil then
    details[#details + 1] = "Captured: "
      .. makima.text.truncate_line(makima.json.encode(result.captured), output_limits.DEFAULT_MAX_LINE_BYTES)
  end
  local body
  body, redraw = details_view(status, style, preview, view, details)
  return { body = body, header = label_buf(wait_label(input, state)) }
end

local function send_label(input, state)
  if state and state.description then
    return 'To "' .. state.description .. '"'
  end
  return "To " .. input.task_id:sub(-8)
end

-- Builds the quoted message before the handler admits any work and returns a
-- finisher for the result. A child that runs while its parent is still busy
-- can finish and have its result delivered ahead of the parent's next
-- request. The Markdown may land while the handler waits on admission, so
-- the body exists from the start. Clicks toggle the truncated view, and a
-- failure line follows the quote so the user still sees what was sent.
local function quote_view(text, ctx, failed_prefix, header_text)
  local body = makima.ui.buf()
  local quote, view, failure
  local function render()
    body:set_lines(quote:get_lines())
    if failure then
      body:line({ { failure, "error" } })
    end
  end
  quote, view = markdown_preview(text, ctx, QUOTE_BAR, render)
  view:finish()
  render()
  body:on("click", function()
    view:toggle()
    render()
  end)
  return function(output, is_error)
    failure = is_error and failed_prefix .. output or nil
    render()
    return { body = body, header = label_buf(header_text) }
  end
end

local function send_view(input, ctx, state)
  return quote_view(input.message, ctx, SEND_FAILED_PREFIX, send_label(input, state))
end

local function spawn_view(input, ctx)
  return quote_view(input.prompt, ctx, SPAWN_FAILED_PREFIX, input.description)
end

local function despawn_view(input, output, is_error, state)
  local body = makima.ui.buf()
  if is_error then
    body:line({ { DESPAWN_FAILED_PREFIX .. output, "error" } })
  end
  return { body = body, header = label_buf(wait_label(input, state)) }
end

-- task_send ------------------------------------------------------------------

local send_schema = {
  type = "object",
  required = { "task_id", "message" },
  additionalProperties = false,
  properties = {
    task_id = { type = "string", description = "Task id returned by task_spawn." },
    message = {
      type = "string",
      description = "Follow-up instructions for the subagent.",
    },
  },
}

local function send_handler(input, ctx)
  local task, access_err, agent = resolve_task(input.task_id, ctx)
  local state = task and { description = task.description } or nil
  local finish_view = send_view(input, ctx, state)
  local function reply(output, is_error)
    return rendered_reply(finish_view(output, is_error), output, is_error, state)
  end
  if not task then
    return reply(access_err, true)
  end
  local mode, mode_err = current_mode(ctx)
  if mode_err then
    return reply(mode_err, true)
  end
  if task.writes and mode == "plan" then
    return reply(GENERAL_SEND_IN_PLAN_ERR, true)
  end
  local ok, err = enqueue(agent, input.message, ctx)
  if not ok then
    return reply(err, true)
  end
  return reply(makima.json.encode({ queued = true, turn_id = ok }))
end

local function spawn_handler(input, ctx)
  local finish_view = spawn_view(input, ctx)
  local function reply(output, is_error)
    return rendered_reply(finish_view(output, is_error), output, is_error)
  end
  local spec, err = prepare(input, ctx)
  if err then
    return reply(err.llm_output, true)
  end
  local ok, task_id, spawn_err = pcall(spawn, spec, input, ctx)
  if not ok then
    return reply(tostring(task_id), true)
  end
  if spawn_err then
    return reply(spawn_err, true)
  end
  return reply(makima.json.encode(task_id))
end

local function despawn_handler(input, ctx)
  local task, access_err, agent = resolve_task(input.task_id, ctx)
  local state = task and { description = task.description } or nil
  local function reply(output, is_error)
    return rendered_reply(despawn_view(input, output, is_error, state), output, is_error, state)
  end
  if not task then
    return reply(access_err, true)
  end
  local closed, err = agent:close(ctx)
  if not closed then
    return reply(err, true)
  end
  return reply(makima.json.encode({ ok = true }))
end

local function stop_handler(input, ctx)
  local task, access_err, agent = resolve_task(input.task_id, ctx)
  if task then
    local closed, err = agent:close(ctx)
    if not closed then
      return { llm_output = err, is_error = true }
    end
    return makima.json.encode({ ok = true, task_id = input.task_id })
  end
  if access_err ~= UNKNOWN_TASK_ERR then
    return { llm_output = access_err, is_error = true }
  end
  local ok, err = ctx:stop_background(input.task_id)
  if not ok then
    return { llm_output = "Stop failed: " .. tostring(err), is_error = true }
  end
  return makima.json.encode({ ok = true, task_id = input.task_id })
end

local function restore_stop(input, output, is_error, ctx)
  local body = makima.ui.buf()
  if is_error then
    body:line({ { "Stop failed: " .. output, "error" } })
  else
    body:line({ { "Stopped " .. input.task_id, "dim" } })
  end
  return { body = body, header = label_buf(input.task_id) }
end

local function wait_handler(input, ctx)
  local task, access_err, agent = resolve_task(input.task_id, ctx)
  local state = task and { description = task.description } or nil
  local function reply(output, is_error)
    return rendered_reply(wait_view(input, output, is_error, ctx, state), output, is_error, state)
  end
  if not task then
    return reply(access_err, true)
  end
  local waiting = makima.ui.buf()
  waiting:line({ { wait_label(input, state), "dim" } })
  waiting:line({ { "Waiting for the subagent…", "dim" } })
  ctx:live_buf(waiting)
  makima.async.on_cancel(function(reason)
    waiting:set_lines({ { { "Wait failed: " .. reason, "error" } } })
  end)
  local ticket, turn_err
  if input.turn_id then
    ticket, turn_err = agent:turn(ctx, input.turn_id)
  else
    ticket, turn_err = agent:latest_turn(ctx)
  end
  if not ticket then
    if turn_err and turn_err:sub(1, #TURN_UNAVAILABLE_ERR) == TURN_UNAVAILABLE_ERR then
      turn_err = TURN_UNAVAILABLE_ERR
    end
    return reply(turn_err or TURN_UNAVAILABLE_ERR, true)
  end
  local result, wait_err = ticket:wait(ctx, { timeout = input.timeout })
  if result then
    result = project_result(result)
  end
  if wait_err == TURN_WAIT_TIMEOUT then
    return reply(makima.json.encode({ status = "running" }))
  end
  if wait_err then
    return reply(tostring(wait_err), true)
  end
  return reply(makima.json.encode(result and {
    status = "done",
    result = result,
    error = result.error,
  } or { status = "running" }))
end

local function restore_wait(input, output, is_error, ctx)
  return wait_view(input, output, is_error, ctx, ctx:state())
end

local function restore_send(input, output, is_error, ctx)
  return send_view(input, ctx, ctx:state())(output, is_error)
end

local function restore_spawn(input, output, is_error, ctx)
  return spawn_view(input, ctx)(output, is_error)
end

local function task_header(input)
  return wait_label(input)
end
local function header(input)
  return input.description
end

local function restore_despawn(input, output, is_error, ctx)
  return despawn_view(input, output, is_error, ctx:state())
end

makima.api.register_tool({
  name = "task_spawn",
  description = "Start a background subagent and return {task_id, turn_id} immediately. Observe results with task_wait, which yields the caller's turn capacity while it waits. Steer work with task_send and finish with task_despawn. Also callable from code_execution.\n\n"
    .. description,
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = spawn_schema,
  handler = spawn_handler,
  header = header,
  restore = restore_spawn,
})

makima.api.register_tool({
  name = "task_wait",
  description = "Wait for one exact task turn. Omit turn_id to select the latest turn once. Messages joining that turn extend its work; the wait never follows a later turn. Returns {status, result?, error?}, with per-turn usage and duration_ms. Timeout returns running without cancelling work; zero checks without waiting. Unavailable or evicted turn IDs return turn_result_unavailable. The caller releases its turn capacity while waiting. Also callable from code_execution. See [Agent work](/docs/agent-work/#task-waits) for completion and retention rules.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  schema = wait_schema,
  handler = wait_handler,
  header = task_header,
  restore = restore_wait,
})

makima.api.register_tool({
  name = "task_send",
  description = "Submit follow-up instructions to a background subagent and return { queued = true, turn_id }, or an admission error. Compatible messages join open work and share its turn_id; otherwise they start a new turn. Messages wait for the next model request, without interrupting a running request or tool. Acceptance does not mean processing. Overflow rejects only the new message. Also callable from code_execution. See [Agent work](/docs/agent-work/#messages-and-turns) for queue rules.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = send_schema,
  handler = send_handler,
  header = function(input)
    return send_label(input)
  end,
  restore = restore_send,
})

makima.api.register_tool({
  name = "task_despawn",
  description = "Permanently close a background subagent and its descendants, discard pending work, flush its chat transcript, and release active turn permits. This does not retire their durable identities. Returns { ok = true }. Also callable from a code_execution script as a Python async function.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = despawn_schema,
  handler = despawn_handler,
  header = task_header,
  restore = restore_despawn,
})

makima.api.register_tool({
  name = "task_stop",
  description = "Stop a background subagent or a tool call moved to the background. A moved call's final result still arrives as a message. Returns { ok = true }.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = stop_schema,
  handler = stop_handler,
  header = function(input)
    return input.task_id
  end,
  restore = restore_stop,
})

local SUBAGENT_TYPES = {
  research = "Read-only search and summarize",
  general = "Can modify files",
  plan_reviewer = "Read-only plan audit (plan mode)",
}
local SUBAGENT_ORDER = { "research", "general", "plan_reviewer" }

makima.api.register_completion_source("subagent", {
  get_items = function(ctx)
    local mode = ctx.mode or "build"
    local items = {}
    for _, name in ipairs(SUBAGENT_ORDER) do
      local include
      if mode == "plan" then
        include = name ~= "general"
      else
        include = name ~= "plan_reviewer"
      end
      if include then
        items[#items + 1] = {
          label = "subagent:" .. name,
          kind = "subagent",
          insertion = "@subagent:" .. name .. " ",
          description = SUBAGENT_TYPES[name],
        }
      end
    end
    return items
  end,
})

local function valid_subagent_type(value)
  return value == "research" or value == "general" or value == "plan_reviewer"
end

local function expand_subagent(ref)
  local value = ref.value
  if not valid_subagent_type(value) then
    return nil, "unknown subagent type: " .. value
  end
  return "<subagent:" .. value .. ">", nil
end

makima.api.register_expander("subagent", expand_subagent)
makima.api.register_expander("a", expand_subagent)

-- `after_instructions` is a system-only slot, so this teaches the main agent
-- what the token means without costing subagent prompts any tokens.
makima.api.register_prompt_hint({
  slot = "after_instructions",
  content = "A `<subagent:type>` token in a user message (typed as `@subagent:type`) is a delegation request: launch a subagent with task_spawn and observe it with task_wait, setting `subagent_type` to `type`.",
})
