local helpers = require("autoresearch_helpers")
local th = require("maki.test_helpers")

local case = th.case
local eq = th.eq

case("slugify_goal", function()
  eq(helpers.slugify("Reduce API latency!"), "reduce-api-latency")
  eq(helpers.slugify("!!!"), "experiment")
end)

case("shell_quote_apostrophe", function()
  eq(helpers.shell_quote("don't"), "'don'\\''t'")
end)

case("detect_empty_bash_output", function()
  eq(helpers.has_output("Exit code: 0"), false)
  eq(helpers.has_output(" M src/main.rs\n"), true)
end)

case("initialization_is_one_shell_transaction", function()
  local command = helpers.initialization_command("autoresearch/don't-break")
  assert(command:find("git rev%-parse %-%-is%-inside%-work%-tree"))
  assert(command:find("git status %-%-porcelain=v1 %-%-untracked%-files=all"))
  assert(command:find("git switch %-c 'autoresearch/don'\\''t%-break'"))
  assert(command:find('printf "AUTORESEARCH_COMMIT=%%s\\n"'))
end)

case("guarded_command_checks_branch_head_and_cleanliness", function()
  local command = helpers.guarded_command({
    branch = "autoresearch/parser",
    accepted_commit = "abc123",
  }, "git reset --hard 'abc123'", true)
  assert(command:find("git branch --show-current", 1, true))
  assert(command:find("autoresearch/parser", 1, true))
  assert(command:find("git rev-parse HEAD", 1, true))
  assert(command:find("abc123", 1, true))
  assert(command:find("git status --porcelain=v1 --untracked-files=all", 1, true))
  assert(command:find("git reset --hard 'abc123'", 1, true))
end)

case("parse_initialization_commit", function()
  local commit, err = helpers.parse_initialization("AUTORESEARCH_COMMIT=abc123\nExit code: 0")
  eq(err, nil)
  eq(commit, "abc123")

  commit, err = helpers.parse_initialization("Exit code: 0")
  eq(commit, nil)
  assert(err:match("baseline commit"))
end)

case("restore_and_render_session_state", function()
  local state = {
    version = 1,
    branch = "autoresearch/latency",
    primary_metric = "latency",
    direction = "minimize",
    max_iterations = 20,
    run_count = 3,
    accepted_commit = "abc123",
    best_metric = 12.5,
  }
  eq(helpers.restore_state(state), state)
  eq(state.accepted_metric, 12.5)
  eq(helpers.status_content(state)[1][1], "AR 3/20 best latency=12.5")
  state.pending = { run = 4 }
  eq(helpers.status_content(state)[1][1], "AR 3/20 pending")
  assert(select(2, helpers.restore_state({ version = 1 })):match("invalid"))
end)

case("commit_message_records_metric_evidence", function()
  eq(
    helpers.commit_message(1, " establish baseline ", "latency_ms", 12.5, nil),
    "autoresearch: run 1 latency_ms=12.5 baseline establish baseline"
  )
  eq(
    helpers.commit_message(2, "remove\nallocation", "latency_ms", 10.25, 12.5),
    "autoresearch: run 2 latency_ms=10.25 delta=-2.25 remove allocation"
  )
end)

case("parse_metrics", function()
  local metrics, err = helpers.parse_metrics("noise\nMETRIC latency_ms=12.5\nMETRIC throughput=4\n")
  eq(err, nil)
  eq(metrics.latency_ms, 12.5)
  eq(metrics.throughput, 4)
end)

case("reject_non_numeric_metric", function()
  local metrics, err = helpers.parse_metrics("METRIC latency=fast")
  eq(metrics, nil)
  assert(err:match("not numeric"))
end)

case("reject_missing_metrics", function()
  local metrics, err = helpers.parse_metrics("benchmark complete")
  eq(metrics, nil)
  assert(err:match("no METRIC"))
end)

case("compare_directions", function()
  eq(helpers.improves("minimize", 9, 10), true)
  eq(helpers.improves("minimize", 10, 10), false)
  eq(helpers.improves("maximize", 11, 10), true)
  eq(helpers.improves("maximize", 9, 10), false)
  eq(helpers.improves("maximize", 1, nil), true)
end)
