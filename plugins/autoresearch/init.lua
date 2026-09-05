local helpers = require("autoresearch_helpers")

local MODE = "autoresearch"
local BENCHMARK_COMMAND = "bash autoresearch.sh"
local CONTINUE_PROMPT =
  "If autoresearch mode is still active, continue its loop. Make one coherent change, run the benchmark, then keep or discard it with research_log. Otherwise stop immediately."
local DEFAULT_MAX_ITERATIONS = 20
local NANOS_PER_SECOND = 1000000000
local STATE_VERSION = 1

local sessions = {}
local focused_session

local opts = maki.api.register_options({
  max_iterations = {
    default = DEFAULT_MAX_ITERATIONS,
    min = 1,
    desc = "Maximum benchmark runs in one autoresearch session.",
  },
  timeout_secs = {
    default = 120,
    min = 5,
    desc = "Kill autoresearch.sh after this many seconds of execution.",
  },
})

local DIRECTIVE = [[
You are in **Autoresearch mode**. Work autonomously in a bounded experiment loop.

The experiment history lives on a dedicated `autoresearch/<goal>` git branch. Every accepted experiment is one commit. Rejected experiments are reset to the last accepted commit.

## Protocol

1. Before editing anything, call `research_init` with the goal, primary metric, and whether to minimize or maximize it. Initialization requires a clean worktree and creates the dedicated branch.
2. If `autoresearch.sh` does not exist, build it as the first experiment. It must run a representative deterministic workload, exit non-zero on failure, and print numeric results as `METRIC name=value`. Do not use a live network or time-dependent input. For noisy workloads, the script owns statistical validity: warm up, collect enough samples, report a robust aggregate such as a median or trimmed mean, and reject unacceptable variance. It must also fail when correctness checks or secondary guardrails regress, and clean up any ignored artifacts it creates.
3. Make one coherent change at a time.
4. Call `research_run`. Never invoke `autoresearch.sh` another way.
5. Call `research_log` exactly once for every run:
   - `keep` for a correct result worth retaining after considering the primary metric, secondary metrics, and stated constraints. It commits all experiment changes.
   - `discard` for a valid result not worth retaining.
   - `crash` when the benchmark fails.
   - `checks_failed` when separate correctness checks fail.
6. `research_log` rolls rejected experiments back and queues the next iteration. Set `continue_loop` to false when no useful experiment remains. Otherwise continue until the configured limit or the user interrupts.

The primary metric is the default decision signal, not a hard gate. You may keep a run with a flat or worse primary metric when a secondary improvement or constraint tradeoff justifies it; explain that tradeoff in the description. Never game the benchmark, weaken correctness checks, or combine unrelated changes. Inspect `git status` before keeping a run so generated files are not committed accidentally.
]]

local ok, mode_err = maki.api.mode.define({
  name = MODE,
  label = "[RESEARCH]",
  system_prompt = DIRECTIVE,
  tools = {
    "read",
    "grep",
    "glob",
    "list",
    "index",
    "webfetch",
    "websearch",
    "bash",
    "write",
    "edit",
    "question",
    "task",
    "research_init",
    "research_run",
    "research_log",
  },
})
if not ok then
  maki.log.warn("autoresearch: mode definition failed: " .. mode_err)
end

local function current_session(ctx)
  local sid, err = ctx:session_id()
  if err then
    return nil, err
  end
  if not sid then
    return nil, "autoresearch requires a persistent session"
  end
  return sid
end

local function require_mode(ctx)
  local mode, err = ctx:mode()
  if err then
    return nil, err
  end
  if mode ~= MODE then
    return nil, "switch to autoresearch mode with /autoresearch first"
  end
  return true
end

local function call_bash(ctx, command, description, timeout)
  local input = {
    command = command,
    description = description,
  }
  if timeout then
    input.timeout = timeout
  end
  return maki.agent.call_tool(ctx, "bash", input)
end

local function run_git(ctx, command, description)
  return call_bash(ctx, command, description)
end

local function tool_error(message, body)
  return { llm_output = message, is_error = true, body = body }
end

local function repaint_status(sid, state)
  if focused_session == sid then
    maki.ui.set_status_content(helpers.status_content(state))
  end
end

local function load_session(sid)
  if sessions[sid] then
    return sessions[sid]
  end
  local stored, load_error = maki.session.get_data(sid)
  if load_error then
    return nil, load_error
  end
  if not stored then
    return nil
  end
  local state, validation_error = helpers.restore_state(stored)
  if not state then
    return nil, validation_error
  end
  sessions[sid] = state
  return state
end

local function persist_session(sid, state)
  sessions[sid] = state
  local saved, save_error = maki.session.set_data(sid, state)
  if not saved then
    return nil, save_error
  end
  repaint_status(sid, state)
  return true
end

local function finish_loop(sid, summary, reason)
  local cleared, clear_error = maki.session.set_data(sid, nil)
  if not cleared then
    return summary .. "\n" .. reason .. "; could not clear state: " .. clear_error
  end
  sessions[sid] = nil
  repaint_status(sid, nil)
  local switched, switch_error = maki.session.set_mode(sid, "build")
  if not switched then
    return summary .. "\n" .. reason .. "; state cleared but could not return to build mode: " .. switch_error
  end
  return summary .. "\n" .. reason .. "; returned to build mode."
end

local function resume_session(ctx, sid, require_clean)
  local state, load_error = load_session(sid)
  if not state then
    return state, load_error
  end
  local command = helpers.guarded_command(state, ":", require_clean and state.pending == nil)
  local _, verify_error = run_git(ctx, command, "Verify autoresearch state")
  if verify_error then
    return nil,
      "autoresearch state does not match the current Git checkout; restore branch "
        .. state.branch
        .. " at "
        .. state.accepted_commit
        .. " and retry: "
        .. verify_error
  end
  repaint_status(sid, state)
  return state
end

local function now_seconds()
  local timestamp = maki.time.now()
  return timestamp.secs + timestamp.nanosecs / NANOS_PER_SECOND
end

local function new_trace(ctx)
  local trace = {
    buf = maki.ui.buf(),
    lines = { "starting research_init" },
  }
  trace.buf:set_lines(trace.lines)
  ctx:live_buf(trace.buf)
  return trace
end

local function traced_git(trace, ctx, command, description, display)
  local line = #trace.lines + 1
  local started = now_seconds()
  trace.lines[line] = "running: " .. (display or command)
  trace.buf:set_lines(trace.lines)

  local output, err = run_git(ctx, command, description)
  local elapsed = now_seconds() - started
  if err then
    local detail = tostring(err):match("[^\r\n]+") or "unknown error"
    trace.lines[line] = string.format("failed (%.1fs): %s: %s", elapsed, display or command, detail)
  else
    trace.lines[line] = string.format("ok (%.1fs): %s", elapsed, display or command)
  end
  trace.buf:set_lines(trace.lines)
  return output, err
end

local function format_metrics(metrics)
  local names = {}
  for name in pairs(metrics) do
    names[#names + 1] = name
  end
  table.sort(names)
  local values = {}
  for _, name in ipairs(names) do
    values[#values + 1] = string.format("%s=%s", name, metrics[name])
  end
  return table.concat(values, ", ")
end

maki.api.create_autocmd({ "SessionFocusChanged", "SessionReset" }, {
  callback = function(event)
    if event.event == "SessionReset" then
      focused_session = nil
      maki.ui.set_status_content({})
      return
    end
    focused_session = event.data.session_id
    local state, load_error = load_session(focused_session)
    if load_error then
      maki.log.warn("autoresearch: failed to load session state: " .. load_error)
    end
    maki.ui.set_status_content(helpers.status_content(state))
  end,
})

maki.api.register_command({
  name = "/autoresearch",
  description = "Start or stop a bounded benchmark-driven research loop",
  argument_hint = "[off | reset | goal]",
  nargs = "*",
  tui_only = true,
  handler = function(command)
    local args = tostring(command.args or ""):gsub("^%s+", ""):gsub("%s+$", "")
    if args == "off" then
      local switched, err = maki.api.mode.set("build")
      if not switched then
        maki.ui.flash("autoresearch: " .. err)
      end
      maki.ui.set_status_content({})
      return
    end
    if args == "reset" then
      local sid, current_error = maki.session.current()
      if not sid then
        maki.ui.flash("autoresearch: " .. current_error)
        return
      end
      local cleared, clear_error = maki.session.set_data(sid, nil)
      if not cleared then
        maki.ui.flash("autoresearch: " .. clear_error)
        return
      end
      sessions[sid] = nil
      maki.ui.set_status_content({})
      maki.ui.flash("autoresearch state reset")
      return
    end

    local switched, err = maki.api.mode.set(MODE)
    if not switched then
      maki.ui.flash("autoresearch: " .. err)
      return
    end
    if args == "" then
      maki.ui.flash("autoresearch enabled: describe the optimization goal")
      return
    end
    local _, prompt_err = maki.session.prompt(args)
    if prompt_err then
      maki.ui.flash("autoresearch: " .. prompt_err)
    end
  end,
})

maki.api.register_tool({
  name = "research_init",
  description = "Initialize a bounded experiment loop on a new dedicated git branch. Call before making any changes.",
  schema = {
    type = "object",
    additionalProperties = false,
    required = { "goal", "primary_metric", "direction" },
    properties = {
      goal = { type = "string", description = "What to optimize" },
      primary_metric = { type = "string", description = "METRIC name used to judge experiments" },
      direction = { type = "string", enum = { "minimize", "maximize" } },
    },
  },
  audiences = { "main" },
  handler = function(input, ctx)
    local trace = new_trace(ctx)
    local function fail(message)
      return tool_error(message, trace.buf)
    end

    local in_mode, mode_error = require_mode(ctx)
    if not in_mode then
      return fail(mode_error)
    end
    local sid, session_error = current_session(ctx)
    if not sid then
      return fail(session_error)
    end
    local existing, resume_error = resume_session(ctx, sid, true)
    if resume_error then
      return fail(resume_error)
    end
    if existing then
      return {
        llm_output = string.format(
          "Resumed %s at run %d/%d. Continue the experiment loop.",
          existing.branch,
          existing.run_count,
          existing.max_iterations
        ),
        body = trace.buf,
      }
    end

    local branch = "autoresearch/" .. helpers.slugify(input.goal)
    local output, initialization_error = traced_git(
      trace,
      ctx,
      helpers.initialization_command(branch),
      "Initialize autoresearch Git branch",
      "Git repository checks and branch creation"
    )
    if not output then
      return fail(initialization_error)
    end
    local commit, parse_error = helpers.parse_initialization(output)
    if not commit then
      return fail(parse_error)
    end

    local state = {
      version = STATE_VERSION,
      branch = branch,
      primary_metric = input.primary_metric,
      direction = input.direction,
      max_iterations = opts.max_iterations,
      run_count = 0,
      accepted_commit = commit,
      accepted_metric = nil,
      best_metric = nil,
      pending = nil,
    }
    local persisted, persist_error = persist_session(sid, state)
    if not persisted then
      return fail("could not persist autoresearch state: " .. persist_error)
    end
    return {
      llm_output = string.format(
        "Initialized %s at %s. Build autoresearch.sh, then establish the baseline.",
        branch,
        commit:sub(1, 8)
      ),
      body = trace.buf,
    }
  end,
})

maki.api.register_tool({
  name = "research_run",
  description = "Run the fixed autoresearch.sh benchmark and parse its METRIC lines.",
  schema = {
    type = "object",
    additionalProperties = false,
    properties = {},
  },
  audiences = { "main" },
  handler = function(_, ctx)
    local in_mode, mode_error = require_mode(ctx)
    if not in_mode then
      return tool_error(mode_error)
    end
    local sid, session_error = current_session(ctx)
    if not sid then
      return tool_error(session_error)
    end
    local state, state_error = resume_session(ctx, sid)
    if not state then
      return tool_error(state_error or "call research_init before running the benchmark")
    end
    if state.pending then
      return tool_error("log the pending benchmark result before starting another run")
    end
    if state.run_count >= state.max_iterations then
      return tool_error("maximum autoresearch iterations reached")
    end

    state.run_count = state.run_count + 1
    local output, run_error = call_bash(ctx, BENCHMARK_COMMAND, "Run research benchmark", opts.timeout_secs)
    if not output then
      state.pending = { run = state.run_count, error = run_error }
      local persisted, persist_error = persist_session(sid, state)
      if not persisted then
        return tool_error("could not persist failed benchmark state: " .. persist_error)
      end
      return string.format("Run #%d failed: %s\nCall research_log with status crash.", state.run_count, run_error)
    end

    local metrics, metric_error = helpers.parse_metrics(output)
    if not metrics then
      state.pending = { run = state.run_count, error = metric_error }
      local persisted, persist_error = persist_session(sid, state)
      if not persisted then
        return tool_error("could not persist invalid benchmark state: " .. persist_error)
      end
      return string.format(
        "Run #%d produced invalid metrics: %s\nCall research_log with status crash.",
        state.run_count,
        metric_error
      )
    end
    local primary = metrics[state.primary_metric]
    if primary == nil then
      local error_message = "missing primary metric " .. state.primary_metric
      state.pending = { run = state.run_count, error = error_message }
      local persisted, persist_error = persist_session(sid, state)
      if not persisted then
        return tool_error("could not persist invalid benchmark state: " .. persist_error)
      end
      return string.format(
        "Run #%d %s (%s). Call research_log with status crash.",
        state.run_count,
        error_message,
        format_metrics(metrics)
      )
    end

    state.pending = {
      run = state.run_count,
      metrics = metrics,
      primary = primary,
    }
    local persisted, persist_error = persist_session(sid, state)
    if not persisted then
      return tool_error("could not persist benchmark state: " .. persist_error)
    end
    return string.format("Run #%d passed: %s", state.run_count, format_metrics(metrics))
  end,
})

maki.api.register_tool({
  name = "research_log",
  description = "Keep and commit a selected run, or discard and roll back a rejected run. Queues the next bounded iteration unless stopped.",
  schema = {
    type = "object",
    additionalProperties = false,
    required = { "status", "description" },
    properties = {
      status = {
        type = "string",
        enum = { "keep", "discard", "crash", "checks_failed" },
      },
      description = { type = "string", description = "Concise experiment result" },
      continue_loop = {
        type = "boolean",
        description = "Queue another iteration. Defaults to true.",
      },
    },
  },
  audiences = { "main" },
  handler = function(input, ctx)
    local in_mode, mode_error = require_mode(ctx)
    if not in_mode then
      return tool_error(mode_error)
    end
    local sid, session_error = current_session(ctx)
    if not sid then
      return tool_error(session_error)
    end
    local state, state_error = resume_session(ctx, sid)
    if not state or not state.pending then
      return tool_error(state_error or "there is no pending benchmark result")
    end
    local pending = state.pending

    if input.status == "keep" then
      if pending.error then
        return tool_error("a failed benchmark cannot be kept")
      end
      local status, status_error = run_git(
        ctx,
        helpers.guarded_command(state, helpers.worktree_status_command(), false),
        "Inspect experiment changes"
      )
      if not status then
        return tool_error(status_error)
      end
      local has_changes, change_error = helpers.has_changes(status)
      if has_changes == nil then
        return tool_error(change_error)
      end
      if not has_changes and state.accepted_metric ~= nil then
        return tool_error("kept experiments must contain a change to commit")
      end
      if has_changes then
        local message = helpers.commit_message(
          pending.run,
          input.description,
          state.primary_metric,
          pending.primary,
          state.accepted_metric
        )
        local command =
          helpers.guarded_command(state, "git add -A && git commit -m " .. helpers.shell_quote(message), false)
        local _, commit_error = run_git(ctx, command, "Commit accepted experiment")
        if commit_error then
          return tool_error(commit_error)
        end
        local commit, revision_error = run_git(ctx, "git rev-parse HEAD", "Read experiment commit")
        if not commit then
          return tool_error(revision_error)
        end
        state.accepted_commit = commit:match("([0-9a-fA-F]+)")
        if not state.accepted_commit then
          return tool_error("could not read experiment commit")
        end
      end
      if helpers.improves(state.direction, pending.primary, state.best_metric) then
        state.best_metric = pending.primary
      end
      state.accepted_metric = pending.primary
    else
      local command = helpers.guarded_command(
        state,
        "git reset --hard " .. helpers.shell_quote(state.accepted_commit) .. " && git clean -fd",
        false
      )
      local _, rollback_error = run_git(ctx, command, "Roll back rejected experiment")
      if rollback_error then
        return tool_error("rollback failed; pending run retained for retry: " .. rollback_error)
      end
    end

    state.pending = nil
    local persisted, persist_error = persist_session(sid, state)
    if not persisted then
      return tool_error("could not persist autoresearch result: " .. persist_error)
    end
    local summary = string.format("Run #%d %s: %s", pending.run, input.status, input.description)
    if state.run_count >= state.max_iterations then
      return finish_loop(sid, summary, "Maximum iterations reached")
    end
    if input.continue_loop == false then
      return finish_loop(sid, summary, "Autoresearch stopped")
    end

    local notified, notify_error = maki.session.notify(CONTINUE_PROMPT, { session = sid, wake = true })
    if not notified then
      return summary .. "\nCould not queue the next iteration: " .. notify_error
    end
    return summary .. "\nNext iteration queued."
  end,
})
