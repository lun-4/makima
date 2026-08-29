-- Structured-output story: the subagent gets a session-local structured_output
-- tool whose validated input is captured by the Rust driver.
-- Invalid input is an inline tool error the model can fix in the same run.
--
-- Subagents run on a background driver, so the main agent is not blocked while
-- one runs. Four tools split the lifecycle:
--   task_spawn   -> { task_id }                 (kickoff)
--   task_get     -> { status, result?, error? } (manual polling)
--   task_send    -> { queued = true }           (queue a message / nudge)
--   task_despawn -> { ok = true }               (cancel + flush history)
-- A spawned subagent's result is returned automatically when it finishes, so the
-- agent waits for that reply; task_get is only for manual polling. The unified
-- `task` tool remains as a blocking composite over the four.
--
-- Rust exposes primitives only (`maki.agent.session`, `maki.json.schema_validator`,
-- `maki.async.semaphore`).

local ToolView = require("maki.tool_view")
local output_limits = require("maki.output_limits")
local plan_spec = require("maki.plan_spec")

local STRUCTURED_OUTPUT_NAME = "structured_output"
local STRUCTURED_OUTPUT_DESCRIPTION = "Report your final result. Call it exactly once when your task is complete."
local STRUCTURED_OUTPUT_ACK = "Output recorded."
local STRUCTURED_OUTPUT_PROMPT_SUFFIX = "\n\nWhen finished, call the structured_output tool with your final result."
local MAX_NUDGES = 2
local MAX_SCHEMA_ERRORS = 3
local SCHEMA_COMPILE_ERROR = "invalid output_schema"
local SCHEMA_ROOT_ERROR = "output_schema must have type object"
local STRUCTURED_MISSING_ERROR = "subagent finished without calling structured_output"
local STRUCTURED_INVALID_ERROR = "subagent result does not match output_schema"
local SUMMARY_MISSING_ERROR = "subagent finished without providing a summary"
local GENERAL_BLOCKED_IN_PLAN_ERR =
  "general subagents are blocked in plan mode; use research or plan_reviewer (read-only)"
local NUDGE_MISSING =
  "You did not call the structured_output tool. Call it now with your final result matching its input schema."
local NUDGE_SUMMARY =
  "You finished your work but did not provide a summary. Reply with a concise summary of what you did and found."
local INVALID_INPUT_PREFIX =
  "Input does not match the required schema. Fix the errors and call structured_output again:\n"
local UNKNOWN_TASK_ERR = "unknown task_id"
local TASK_CLOSED_ERR = "task was despawned before its message was admitted"
local BODY_INDENT_COLS = 4
local MIN_MD_WIDTH = 20
local DEFAULT_OUTPUT_LINES = 5

local description = [[Launch an autonomous subagent to perform tasks independently. Best combined with batch.

Subagent types (set via `subagent_type`):
- `research` (default): Read-only tools. For codebase exploration or gathering context.
- `general`: Full tool access. For delegating implementation work.
- `plan_reviewer`: Read-only audit of a finished plan. Only available in plan mode. Evaluates shape, test-to-acceptance-criteria coverage, and severity of risks, and answers with VERDICT: pass|fail.

Subagents run in the background, so the main agent is never blocked by one. Use
`task_spawn` to start a subagent; its result is returned automatically when it
finishes, so wait for that reply rather than polling `task_get`. Use `task_send`
to queue more work and `task_despawn` to cancel a running subagent. The unified
`task` tool is a blocking composite over those four and keeps working for one-shot use.

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
  max_concurrent = { default = 8, min = 1, desc = "Max concurrently running subagents." },
  allow_model = {
    default = false,
    desc = "Expose a `model` input that overrides the subagent model. Only enable if you trust callers to pick an exact model themselves.",
  },
})

local schema = {
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
      description = "JSON Schema (object) the subagent's final result must match. When set, the result is returned as a validated JSON string.",
    },
  },
}

-- Only advertise `model` when the plugin opts in: it costs tokens in every
-- task schema, and an off-by-default flag keeps the common path lean.
if opts.allow_model then
  schema.properties.model = {
    type = "string",
    description = 'Exact model spec, e.g. "ollama/glm-5.2". You tell makima the model; makima will not guess. Overrides model_tier.',
  }
end

local examples = {
  {
    description = "Find auth middleware",
    prompt = "Search the codebase for authentication middleware. Return file paths and a summary of how auth is implemented.",
    model_tier = "weak",
  },
}

-- Process-wide cap on concurrent subagents, released on despawn or gc.
local semaphore = maki.async.semaphore(opts.max_concurrent)

-- Live tasks keyed by task_id.
local tasks = {}

local function bounded_errors(errors)
  local out = {}
  for i = 1, math.min(#errors, MAX_SCHEMA_ERRORS) do
    out[i] = errors[i]
  end
  return table.concat(out, "\n")
end

local function current_mode()
  local mode, err = maki.api.mode.get()
  if err then
    return nil
  end
  return mode
end

-- Validates the shared setup for a subagent and returns
-- `spec, err` where spec holds the model/system/tools/audience/local_tools.
local function prepare(input, ctx)
  local subagent_type = input.subagent_type or "research"
  if subagent_type ~= "research" and subagent_type ~= "general" and subagent_type ~= "plan_reviewer" then
    return nil, { llm_output = "unknown subagent type: " .. subagent_type, is_error = true }
  end
  if subagent_type == "plan_reviewer" and current_mode() ~= "plan" then
    return nil, { llm_output = "plan_reviewer is only available in plan mode", is_error = true }
  end
  if subagent_type == "general" and current_mode() == "plan" then
    return nil, { llm_output = GENERAL_BLOCKED_IN_PLAN_ERR, is_error = true }
  end

  local validator
  if input.output_schema then
    if type(input.output_schema) ~= "table" or input.output_schema.type ~= "object" then
      return nil, { llm_output = SCHEMA_ROOT_ERROR, is_error = true }
    end
    local compile_err
    validator, compile_err = maki.json.schema_validator(input.output_schema)
    if compile_err then
      return nil, { llm_output = SCHEMA_COMPILE_ERROR .. ": " .. compile_err, is_error = true }
    end
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

  local local_tools
  if validator then
    -- Invalid input is an inline tool error the model can fix in the same run;
    -- the last validation errors are kept on the tool spec so the composite
    -- can report them if the subagent never commits.
    local_tools = {
      [STRUCTURED_OUTPUT_NAME] = {
        description = STRUCTURED_OUTPUT_DESCRIPTION,
        input_schema = input.output_schema,
        last_errors = nil,
        capture_input = true,
        handler = function(value)
          local errs = validator:validate(value)
          if errs then
            local spec = local_tools[STRUCTURED_OUTPUT_NAME]
            spec.last_errors = bounded_errors(errs)
            return nil, INVALID_INPUT_PREFIX .. spec.last_errors
          end
          return STRUCTURED_OUTPUT_ACK
        end,
      },
    }
  end

  return {
    model = model,
    system = system,
    tools = tool_defs,
    audience = audience,
    local_tools = local_tools,
    subagent_type = subagent_type,
  },
    nil
end

local function ctx_opts(spec, input, turn_semaphore)
  return {
    model_spec = spec.model.spec,
    system = spec.system,
    tools = spec.tools,
    local_tools = spec.local_tools,
    audience = spec.audience,
    name = input.description,
    semaphore = turn_semaphore,
  }
end

local function fail_task(task, err)
  if task.closed then
    return
  end
  task.closed = true
  task.error = err
  task.sess:close()
end

local function enqueue(task, message)
  if task.closed then
    return nil, task.error or TASK_CLOSED_ERR
  end
  local ok, admitted, admission_err = pcall(function()
    return task.sess:send(message)
  end)
  if not ok then
    fail_task(task, admitted or TASK_CLOSED_ERR)
    return nil, admitted or TASK_CLOSED_ERR
  end
  if not admitted then
    fail_task(task, admission_err or TASK_CLOSED_ERR)
    return nil, admission_err or TASK_CLOSED_ERR
  end
  return true, nil
end

local function spawn(spec, input, ctx)
  local ok, sess, sess_err = pcall(function()
    return maki.agent.session(ctx, ctx_opts(spec, input, semaphore))
  end)
  if not ok then
    return nil, sess_err
  end
  if sess_err then
    return nil, sess_err
  end
  local task_id = sess:session_id()
  local task = {
    sess = sess,
    closed = false,
    validator = spec.local_tools ~= nil,
  }
  tasks[task_id] = task
  local message = input.prompt
  if spec.local_tools then
    message = message .. STRUCTURED_OUTPUT_PROMPT_SUFFIX
  end
  local admitted, enqueue_err = enqueue(task, message)
  if not admitted then
    tasks[task_id] = nil
    return nil, enqueue_err
  end
  return task_id, nil
end

-- task_spawn -----------------------------------------------------------------

local spawn_schema = {
  type = "object",
  required = { "description", "prompt" },
  additionalProperties = false,
  properties = schema.properties,
}

local function spawn_handler(input, ctx)
  local spec, err = prepare(input, ctx)
  if err then
    return err
  end
  local ok, task_id, spawn_err = pcall(spawn, spec, input, ctx)
  if not ok then
    return { llm_output = tostring(spawn_err), is_error = true }
  end
  if spawn_err then
    return { llm_output = spawn_err, is_error = true }
  end
  return { llm_output = maki.json.encode({ task_id = task_id }) }
end

-- task_get -------------------------------------------------------------------

local get_schema = {
  type = "object",
  required = { "task_id" },
  additionalProperties = false,
  properties = {
    task_id = { type = "string", description = "Task id returned by task_spawn." },
  },
}

local function get_handler(input)
  local task = tasks[input.task_id]
  if not task then
    return { llm_output = UNKNOWN_TASK_ERR, is_error = true }
  end
  local status, err = task.sess:status()
  if err then
    return { llm_output = err, is_error = true }
  end
  if task.closed and task.error then
    status.status = "closed"
    status.error = task.error
  end
  return { llm_output = maki.json.encode(status) }
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
      description = "Message to queue to the subagent. A done subagent restarts on the next turn.",
    },
  },
}

local function send_handler(input)
  local task = tasks[input.task_id]
  if not task then
    return { llm_output = UNKNOWN_TASK_ERR, is_error = true }
  end
  local ok, err = enqueue(task, input.message)
  if not ok then
    return { llm_output = err, is_error = true }
  end
  return { llm_output = maki.json.encode({ queued = true }) }
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

local function despawn_handler(input)
  local task = tasks[input.task_id]
  if not task then
    return { llm_output = UNKNOWN_TASK_ERR, is_error = true }
  end
  fail_task(task, TASK_CLOSED_ERR)
  tasks[input.task_id] = nil
  return { llm_output = maki.json.encode({ ok = true }) }
end

-- task (blocking composite) --------------------------------------------------
local function handler(input, ctx)
  local spec, err = prepare(input, ctx)
  if err then
    return err
  end

  local permit = semaphore:acquire()

  -- pcall so a raised error cannot leak the permit or leave a session open.
  local ok, out = pcall(function()
    local ok, sess, sess_err = pcall(function()
      return maki.agent.session(ctx, ctx_opts(spec, input))
    end)
    if not ok then
      error(sess_err, 0)
    end
    if sess_err then
      error(sess_err, 0)
    end

    local message = input.prompt
    if spec.local_tools then
      message = message .. STRUCTURED_OUTPUT_PROMPT_SUFFIX
    end

    local result = {}
    local prompt_err
    local retries = 0
    result, prompt_err = sess:prompt(message)
    result = result or {}
    while not prompt_err and retries < MAX_NUDGES do
      if spec.local_tools then
        if result.captured then
          break
        end
        retries = retries + 1
        result, prompt_err = sess:prompt(NUDGE_MISSING)
      elseif result.text == "" then
        retries = retries + 1
        result, prompt_err = sess:prompt(NUDGE_SUMMARY)
      else
        break
      end
      result = result or {}
    end

    sess:close()

    if prompt_err then
      -- A result alongside the error means the run was cut short after
      -- streaming some text, and half a transcript beats a bare error.
      if result and result.text and result.text ~= "" then
        return {
          llm_output = "sub-agent interrupted (" .. prompt_err .. "). Partial output:\n" .. result.text,
          is_error = true,
        }
      end
      return { llm_output = "sub-agent error: " .. prompt_err, is_error = true }
    end
    if spec.local_tools and not result.captured then
      local last_errors = spec.local_tools[STRUCTURED_OUTPUT_NAME].last_errors
      local msg = last_errors and (STRUCTURED_INVALID_ERROR .. ":\n" .. last_errors) or STRUCTURED_MISSING_ERROR
      return { llm_output = msg, is_error = true }
    end
    if not spec.local_tools and result.text == "" then
      return { llm_output = SUMMARY_MISSING_ERROR, is_error = true }
    end
    return {
      llm_output = result.captured and maki.json.encode(result.captured) or result.text,
      format = "markdown",
    }
  end)

  permit:release()
  if not ok then
    error(out, 0)
  end
  return out
end

local function header(input)
  return input.description
end

-- Standalone runs render markdown on the Rust side (format = "markdown");
-- this mirrors that for restore and batch children, which build the body here.
local function restore(_input, output, is_error, ctx)
  local tol = ctx:tool_output_lines()
  return ToolView.restore_markdown(output, is_error, {
    max_lines = (tol and tol.task) or DEFAULT_OUTPUT_LINES,
    keep = "head",
    max_line_bytes = output_limits.DEFAULT_MAX_LINE_BYTES,
    width = math.max(maki.ui.terminal_size().cols - BODY_INDENT_COLS, MIN_MD_WIDTH),
  })
end

maki.api.register_tool({
  name = "task_spawn",
  description = "Start a background subagent and return its task_id immediately. Each task's messages run FIFO, acquiring concurrency capacity only when each turn starts. The result is returned automatically when the subagent finishes, so wait for the reply instead of polling task_get. Queue messages with task_send and finish with task_despawn. Also callable from a code_execution script as a Python async function.",
  kind = "execute",
  audiences = { "main", "interpreter", "workflow" },
  examples = {},
  schema = spawn_schema,
  handler = spawn_handler,
  header = header,
  restore = restore,
})

maki.api.register_tool({
  name = "task_get",
  description = 'Poll a background subagent. Returns { status = "running" | "done" | "closed", result?, error? }. Normally unnecessary: a spawned subagent\'s result arrives automatically, so wait for that reply instead of polling task_get. Does not block the main agent. Also callable from a code_execution script as a Python async function.',
  kind = "execute",
  audiences = { "main", "interpreter", "workflow" },
  examples = {},
  schema = get_schema,
  handler = get_handler,
  header = function()
    return "task_get"
  end,
  restore = restore,
})

maki.api.register_tool({
  name = "task_send",
  description = "Queue a message to a background subagent in per-task FIFO order and return immediately. A done subagent processes it as a new turn, acquiring concurrency capacity when the turn starts. Returns { queued = true }, or a session error if queueing fails. Also callable from a code_execution script as a Python async function.",
  kind = "execute",
  audiences = { "main", "interpreter", "workflow" },
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
  audiences = { "main", "interpreter", "workflow" },
  examples = {},
  schema = despawn_schema,
  handler = despawn_handler,
  header = function(input)
    return "task_despawn → " .. input.task_id
  end,
  restore = restore,
})

maki.api.register_tool({
  name = "task",
  description = description,
  kind = "execute",
  audiences = { "main", "workflow" },
  examples = examples,
  schema = schema,
  handler = handler,
  header = header,
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
  content = "A `<subagent:type>` token in a user message (typed as `@subagent:type`) is a delegation request: launch a subagent with the task tool, setting `subagent_type` to `type`.",
})
