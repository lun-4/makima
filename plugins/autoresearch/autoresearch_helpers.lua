local M = {}

local STATE_VERSION = 1

function M.slugify(value)
  local slug = value:lower():gsub("[^%w]+", "-"):gsub("^%-+", ""):gsub("%-+$", "")
  if slug == "" then
    slug = "experiment"
  end
  return slug:sub(1, 40):gsub("%-+$", "")
end

function M.shell_quote(value)
  return "'" .. value:gsub("'", "'\\''") .. "'"
end

function M.has_output(value)
  return value ~= "" and not value:match("^%s*Exit code: 0%s*$")
end

function M.initialization_command(branch)
  local quoted_branch = M.shell_quote(branch)
  return table.concat({
    "set -eu",
    'fail() { printf "AUTORESEARCH_ERROR=%s\\n" "$1" >&2; exit 1; }',
    'repository=$(git rev-parse --is-inside-work-tree 2>&1) || fail "autoresearch requires a Git repository: $repository"',
    '[ "$repository" = true ] || fail "autoresearch requires a Git repository"',
    'commit=$(git rev-parse --verify HEAD 2>&1) || fail "autoresearch requires a repository with at least one commit: $commit"',
    'status=$(git status --porcelain=v1 --untracked-files=all 2>&1) || fail "could not read Git status: $status"',
    '[ -z "$status" ] || fail "autoresearch requires a clean worktree before initialization"',
    'current_branch=$(git branch --show-current 2>&1) || fail "could not read current Git branch: $current_branch"',
    '[ "$current_branch" = ' .. quoted_branch .. " ] || {",
    "  switch_output=$(git switch -c "
      .. quoted_branch
      .. ' 2>&1) || fail "could not create autoresearch branch: $switch_output"',
    "}",
    'printf "AUTORESEARCH_COMMIT=%s\\n" "$commit"',
  }, "\n")
end

function M.parse_initialization(output)
  local commit = output:match("AUTORESEARCH_COMMIT=([0-9a-fA-F]+)")
  if not commit then
    return nil, "could not read baseline commit"
  end
  return commit
end

function M.restore_state(value)
  if type(value) ~= "table" or value.version ~= STATE_VERSION then
    return nil, "unsupported autoresearch session state"
  end
  if
    type(value.branch) ~= "string"
    or value.branch == ""
    or type(value.primary_metric) ~= "string"
    or value.primary_metric == ""
    or (value.direction ~= "minimize" and value.direction ~= "maximize")
    or type(value.max_iterations) ~= "number"
    or value.max_iterations < 1
    or value.max_iterations % 1 ~= 0
    or type(value.run_count) ~= "number"
    or value.run_count < 0
    or value.run_count % 1 ~= 0
    or type(value.accepted_commit) ~= "string"
    or not value.accepted_commit:match("^[0-9a-fA-F]+$")
    or (value.accepted_metric ~= nil and type(value.accepted_metric) ~= "number")
    or (value.best_metric ~= nil and type(value.best_metric) ~= "number")
    or (value.pending ~= nil and type(value.pending) ~= "table")
  then
    return nil, "invalid autoresearch session state"
  end
  if value.accepted_metric == nil then
    value.accepted_metric = value.best_metric
  end
  return value
end

function M.status_content(state)
  if not state then
    return {}
  end
  local text = string.format("AR %d/%d", state.run_count, state.max_iterations)
  if state.pending then
    text = text .. " pending"
  elseif state.best_metric ~= nil then
    text = text .. string.format(" best %s=%s", state.primary_metric, state.best_metric)
  end
  return { { text, "status_info" } }
end

function M.commit_message(run, description, metric, candidate, incumbent)
  local result = metric .. "=" .. string.format("%.12g", candidate)
  if incumbent == nil then
    result = result .. " baseline"
  else
    result = result .. " delta=" .. string.format("%.12g", candidate - incumbent)
  end
  description = description:gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")
  return string.format("autoresearch: run %d %s %s", run, result, description)
end

function M.parse_metrics(output)
  local metrics = {}
  for line in output:gmatch("[^\r\n]+") do
    local name, raw = line:match("^%s*METRIC%s+([%w_.-]+)%s*=%s*(%S+)%s*$")
    if name then
      local value = tonumber(raw)
      if not value then
        return nil, "metric " .. name .. " is not numeric: " .. raw
      end
      metrics[name] = value
    end
  end
  if next(metrics) == nil then
    return nil, "benchmark emitted no METRIC name=value lines"
  end
  return metrics
end

function M.improves(direction, candidate, incumbent)
  if incumbent == nil then
    return true
  end
  if direction == "minimize" then
    return candidate < incumbent
  end
  return candidate > incumbent
end

return M
