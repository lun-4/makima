-- Async tasks retain exact turn tickets, not invocation authority.
-- Completion delivery belongs to the runtime; a wait in the parent's own turn withdraws it.
local ToolView = require("maki.tool_view")
local output_limits = require("maki.output_limits")
local plan_spec = require("maki.plan_spec")

local MAX_RETAINED_TASKS = 128
local MAX_RETAINED_TURNS = 64
local TURN_UNAVAILABLE_ERR = "turn_result_unavailable"
local TASK_LIMIT_ERR = "task record limit reached; despawn unused tasks"
local GENERAL_SEND_IN_PLAN_ERR =
  "general subagents cannot take new work in plan mode; despawn the task or switch to build mode"
local GENERAL_BLOCKED_IN_PLAN_ERR =
  "general subagents are blocked in plan mode; use research or plan_reviewer (read-only)"
local UNKNOWN_TASK_ERR = "unknown task_id"
local TASK_ACCESS_ERR = "task is owned by another agent branch"
local TURN_WAIT_TIMEOUT = "agent turn wait timed out"
local TASK_CLOSED_ERR = "task was despawned before its message was admitted"
local BODY_INDENT_COLS = 4
local MIN_MD_WIDTH = 20
local DEFAULT_OUTPUT_LINES = 5

local description = [[Launch an autonomous subagent to perform tasks independently. Best combined with batch.

Subagent types (set via `subagent_type`):
- `research` (default): Read-only tools. For codebase exploration or gathering context.
- `general`: Full tool access. For delegating implementation work.
- `plan_reviewer`: Read-only audit of a finished plan. Only available in plan mode. Evaluates shape, test-to-acceptance-criteria coverage, and severity of risks, and answers with VERDICT: pass|fail.

Use `task_spawn` to start work and `task_wait` to observe its exact turn. A
waiting agent yields its turn capacity while its descendants run. Queue
follow-up work with `task_send` and release the child with `task_despawn`.

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

local opts = maki.api.register_options({
  max_concurrent = {
    default = 8,
    min = 1,
    desc = "Process-wide subagent turn limit for unmanaged frontends; managed TUI sessions use agent.max_concurrent_agent_turns.",
  },
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

-- Live tasks keyed by task_id.
local tasks = {}
local task_order = {}

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

  local model, model_err = maki.agent.resolve_model(ctx, {
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
    system, system_err = maki.agent.system_prompt(ctx, {
      prompt_id = prompt_id,
      instructions = true,
    })
  end
  if system_err then
    return nil, { llm_output = system_err, is_error = true }
  end

  local tool_defs, tools_err = maki.agent.tools(ctx, {
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

-- Process-wide cap on concurrent subagents, released on despawn or gc.
local semaphore = maki.async.semaphore(opts.max_concurrent)

local function spawn_options(spec, input)
  return {
    model = { spec = spec.model.spec, thinking = input.thinking },
    system = spec.system,
    tools = spec.tools,
    output_schema = spec.output_schema,
    audience = spec.audience,
    name = input.description,
    semaphore = semaphore,
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

local function create_session(ctx, options)
  local agent, err = maki.agent.spawn(ctx, options)
  if not agent then
    return nil, err
  end
  local latest
  local tickets = {}
  local order = {}
  local adapter = {}
  function adapter:session_id()
    return agent:_maki_session_id()
  end
  function adapter:_maki_task_owner()
    return agent:_maki_task_owner(ctx)
  end
  function adapter:close(call_ctx)
    return agent:close(call_ctx)
  end
  local function make_ticket_room(call_ctx)
    for index, id in ipairs(order) do
      if #order < MAX_RETAINED_TURNS then
        break
      end
      if tickets[id]:result(call_ctx) then
        tickets[id] = nil
        table.remove(order, index)
        break
      end
    end
  end
  local function retain_ticket(ticket, call_ctx)
    local id = ticket:id()
    if not tickets[id] then
      make_ticket_room(call_ctx)
      if #order >= MAX_RETAINED_TURNS then
        return nil
      end
      tickets[id] = ticket
      order[#order + 1] = id
    end
    return ticket
  end
  function adapter:send(message, call_ctx)
    local authorized, auth_err = agent:status(call_ctx)
    if not authorized then
      return nil, auth_err, true
    end
    make_ticket_room(call_ctx)
    local ticket, send_err = agent:submit(call_ctx, message, { existing_only = #order >= MAX_RETAINED_TURNS })
    if not ticket then
      return nil, send_err, true
    end
    if not retain_ticket(ticket, call_ctx) then
      return nil, TURN_UNAVAILABLE_ERR, true
    end
    latest = ticket:id()
    return latest
  end
  function adapter:wait(call_ctx, turn_id, timeout)
    -- Without an id, wait on the child's latest turn, which may be one the
    -- runtime admitted to hand it a result from its own subagents.
    local ticket
    if turn_id then
      ticket = tickets[turn_id]
    else
      ticket = agent:latest_turn(call_ctx) or tickets[latest]
      if ticket then
        ticket = retain_ticket(ticket, call_ctx)
      end
    end
    if not ticket then
      return nil, TURN_UNAVAILABLE_ERR
    end
    local result, wait_err = ticket:wait(call_ctx, { timeout = timeout })
    if not result then
      return nil, wait_err
    end
    return project_result(result)
  end
  function adapter:idle(call_ctx)
    local status = agent:status(call_ctx)
    return status ~= nil and status.status ~= "running" and status.queued == 0
  end
  return adapter
end
local function fail_task(task, err, ctx)
  if task.closed then
    return
  end
  task.closed = true
  task.error = err
  task.sess:close(ctx)
end

local function enqueue(task, message, ctx)
  if task.closed then
    return nil, task.error or TASK_CLOSED_ERR
  end
  local ok, admitted, admission_err, nonfatal = pcall(function()
    return task.sess:send(message, ctx)
  end)
  if not ok then
    fail_task(task, admitted or TASK_CLOSED_ERR, ctx)
    return nil, admitted or TASK_CLOSED_ERR
  end
  if not admitted then
    if not nonfatal then
      fail_task(task, admission_err or TASK_CLOSED_ERR, ctx)
    end
    return nil, admission_err or TASK_CLOSED_ERR
  end
  return admitted, nil
end

local function spawn(spec, input, ctx)
  local i = 1
  while i <= #task_order do
    local id = task_order[i]
    local task = tasks[id]
    if not task or task.owner:_maki_retired() then
      tasks[id] = nil
      table.remove(task_order, i)
    elseif #task_order >= MAX_RETAINED_TASKS and task.owner:_maki_can_control() then
      if task.closed or task.sess:idle(ctx) then
        task.sess:close(ctx)
        tasks[id] = nil
        table.remove(task_order, i)
      else
        i = i + 1
      end
    else
      i = i + 1
    end
  end
  if #task_order >= MAX_RETAINED_TASKS then
    return nil, TASK_LIMIT_ERR
  end
  local ok, sess, sess_err = pcall(function()
    return create_session(ctx, spawn_options(spec, input))
  end)
  if not ok then
    return nil, tostring(sess)
  end
  if sess_err then
    return nil, sess_err
  end
  local task_id = sess:session_id()
  if tasks[task_id] then
    sess:close(ctx)
    return nil, "duplicate task_id"
  end
  local task = {
    sess = sess,
    owner = sess:_maki_task_owner(),
    closed = false,
    writes = spec.subagent_type == "general",
  }
  tasks[task_id] = task
  task_order[#task_order + 1] = task_id
  local admitted, enqueue_err = enqueue(task, input.prompt, ctx)
  if not admitted then
    fail_task(task, enqueue_err, ctx)
    tasks[task_id] = nil
    table.remove(task_order)
    return nil, enqueue_err
  end
  return { task_id = task_id, turn_id = admitted }, nil
end

-- task_spawn -----------------------------------------------------------------

local function spawn_handler(input, ctx)
  local spec, err = prepare(input, ctx)
  if err then
    return err
  end
  local ok, task_id, spawn_err = pcall(spawn, spec, input, ctx)
  if not ok then
    return { llm_output = tostring(task_id), is_error = true }
  end
  if spawn_err then
    return { llm_output = spawn_err, is_error = true }
  end
  return { llm_output = maki.json.encode(task_id) }
end

local function resolve_task(task_id)
  local task = tasks[task_id]
  if not task then
    return nil, UNKNOWN_TASK_ERR
  end
  if not task.owner:_maki_can_control() then
    return nil, TASK_ACCESS_ERR
  end
  return task, nil
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
      description = "Instructions for the subagent. Busy sends join its open work turn; idle sends start a new turn.",
    },
  },
}

local function send_handler(input, ctx)
  local task, access_err = resolve_task(input.task_id)
  if not task then
    return { llm_output = access_err, is_error = true }
  end
  local mode, mode_err = current_mode(ctx)
  if mode_err then
    return { llm_output = mode_err, is_error = true }
  end
  if task.writes and mode == "plan" then
    return { llm_output = GENERAL_SEND_IN_PLAN_ERR, is_error = true }
  end
  local ok, err = enqueue(task, input.message, ctx)
  if not ok then
    return { llm_output = err, is_error = true }
  end
  return { llm_output = maki.json.encode({ queued = true, turn_id = ok }) }
end

-- task_despawn ---------------------------------------------------------------

local despawn_schema = {
  type = "object",
  required = { "task_id" },
  additionalProperties = false,
  properties = {
    task_id = { type = "string", description = "Task id returned by task_spawn." },
  },
}

local function despawn_handler(input, ctx)
  local task, access_err = resolve_task(input.task_id)
  if not task then
    return { llm_output = access_err, is_error = true }
  end
  fail_task(task, TASK_CLOSED_ERR, ctx)
  tasks[input.task_id] = nil
  for i, id in ipairs(task_order) do
    if id == input.task_id then
      table.remove(task_order, i)
      break
    end
  end
  return { llm_output = maki.json.encode({ ok = true }) }
end

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

local function wait_handler(input, ctx)
  local task, access_err = resolve_task(input.task_id)
  if not task then
    return { llm_output = access_err, is_error = true }
  end
  local result, wait_err = task.sess:wait(ctx, input.turn_id, input.timeout)
  if wait_err == TURN_WAIT_TIMEOUT then
    return { llm_output = maki.json.encode({ status = "running" }) }
  end
  if wait_err then
    return { llm_output = tostring(wait_err), is_error = true }
  end
  return {
    llm_output = maki.json.encode(result and {
      status = "done",
      result = result,
      error = result.error,
    } or { status = "running" }),
  }
end
local function header(input)
  return input.description
end

-- Standalone runs render markdown on the Rust side (format = "markdown");
-- this mirrors that for restore and batch children, which build the body here.
local function restore(_input, output, is_error, ctx)
  local tol = ctx:tool_output_lines()
  return ToolView.restore_markdown(output, is_error, {
    max_lines = (tol and tol.task_spawn) or DEFAULT_OUTPUT_LINES,
    keep = "head",
    max_line_bytes = output_limits.DEFAULT_MAX_LINE_BYTES,
    width = math.max(maki.ui.terminal_size().cols - BODY_INDENT_COLS, MIN_MD_WIDTH),
  })
end

maki.api.register_tool({
  name = "task_spawn",
  description = "Start a background subagent and return {task_id, turn_id} immediately. Observe results with task_wait, which yields the caller's turn capacity while it waits. Steer work with task_send and finish with task_despawn. Also callable from code_execution.\n\n"
    .. description,
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = spawn_schema,
  handler = spawn_handler,
  header = header,
  restore = restore,
})

maki.api.register_tool({
  name = "task_wait",
  description = "Wait for one exact task turn. Omitted turn_id snapshots the task's latest turn once, including a turn it ran to handle results from its own subagents. A subagent's turn does not end while its own subagents are still running, so their results arrive within that turn unless its request budget runs out; follow-up turns it runs for later reports are not delivered on their own. Later messages accepted into this same turn extend its work; a successor never retargets an existing wait. Child reports arrive at safe request boundaries for active shared-actor parents, or in a batched idle continuation. Returns {status, result?, error?}, with per-turn usage and duration_ms. Each task retains up to 64 turn tickets; settled tickets are evicted oldest-first when admitting new turns. Evicted or unavailable IDs return turn_result_unavailable. At 64 unresolved unique tickets, new turns reject without cancelling accepted work; further messages may reuse an open ticket. Timeout only stops observation, never cancels the child. The waiting agent releases its turn capacity while it waits. Also callable from code_execution.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  schema = wait_schema,
  handler = wait_handler,
  header = function(input)
    return "task_wait → " .. input.task_id
  end,
  restore = restore,
})

maki.api.register_tool({
  name = "task_send",
  description = "Submit instructions to a background subagent and return immediately. Busy sends join open work and return the same turn_id; idle or admission-closed sends start a new turn with a fresh ID. Instructions reach the next safe model request, not an in-flight request or blocked tool. Acceptance is not proof of processing. The bounded inbox rejects overflow without cancelling the child or earlier instructions. Returns { queued = true, turn_id }, or an admission error. Also callable from a code_execution script as a Python async function.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = send_schema,
  handler = send_handler,
  header = function(input)
    return "task_send → " .. input.task_id
  end,
  restore = restore,
})

maki.api.register_tool({
  name = "task_despawn",
  description = "Cancel a background subagent, discard messages not yet admitted, flush its chat transcript, and release active turn permits. Returns { ok = true }. Also callable from a code_execution script as a Python async function.",
  kind = "execute",
  audiences = { "main", "general_sub", "interpreter", "workflow" },
  examples = {},
  schema = despawn_schema,
  handler = despawn_handler,
  header = function(input)
    return "task_despawn → " .. input.task_id
  end,
  restore = restore,
})

local SUBAGENT_TYPES = {
  research = "Read-only search and summarize",
  general = "Can modify files",
  plan_reviewer = "Read-only plan audit (plan mode)",
}
local SUBAGENT_ORDER = { "research", "general", "plan_reviewer" }

maki.api.register_completion_source("subagent", {
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

maki.api.register_expander("subagent", expand_subagent)
maki.api.register_expander("a", expand_subagent)

-- `after_instructions` is a system-only slot, so this teaches the main agent
-- what the token means without costing subagent prompts any tokens.
maki.api.register_prompt_hint({
  slot = "after_instructions",
  content = "A `<subagent:type>` token in a user message (typed as `@subagent:type`) is a delegation request: launch a subagent with task_spawn and observe it with task_wait, setting `subagent_type` to `type`.",
})
