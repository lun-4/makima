---
name: lunamaki-sync-with-upstream
description: Sync the lunamaki fork with upstream maki additively, never dropping fork features
---

# Syncing makima with upstream maki

This is makima, a fork of maki (the Rust AI coding agent). Every so often you
must pull upstream changes in without breaking fork features. The golden rule:
**additive only. Never remove a lunamaki feature; bring in every upstream
improvement.**

Doing this is fiddly. This skill records the setup, the process, and the sharp
edges so a future iteration does not re-derive them.

## Repo and remotes

- Working branch is a sync branch (e.g. `luna.sync-makima-048-v2`), created on
  top of the fork's line of work.
- `upstream` is `tontinton/maki`. `origin` is `lun-4/makima` (this repo).
- `git fetch upstream` first.

NOTE: confirm those origins are valid before continuing, as different clones
may have different setups.

## The principle: additive + defer to fork

Classify every difference between `upstream/main` and the current branch. For
each intersection, decide: keep both, keep fork, or combine. Two rules are
absolute:

1. **Never drop fork features.** Async subagents, plan mode / overrides /
   plan_reviewer / plan_submit, `/thinking` Pi-effort mapping, automode,
   `--append-system-prompt` in TUI, openai-codex `/login`, bundled-plugins-at-
   will, OpenRouter inventory, warm model catalog always.
2. **Always defer to fork on identity:**
   - `Cargo.toml` `[workspace.package] version` matches the upstream version with
     a `-luna` suffix. Sub-crates use `version.workspace = true`.
   - `install.sh` and `install.ps1`: `REPO="lun-4/makima"`.
   - `makima-storage/src/version.rs`: `RELEASES_URL` points at `lun-4`.
   - `README.md`, `banner.png`, `splash.rs` branding (`LOGO="makima"`).

## Process

1. `git fetch upstream && git merge-tree --write-tree --name-only HEAD upstream/main`
   (dry-run). List the content conflicts.
2. `git merge upstream/main` (no-edit). Inspect `git status` / `git diff --name-only --diff-filter=U`.
3. Resolve each conflict by combining both sides. For code, read
   `git show HEAD:<file>` and `git show upstream/main:<file>` and merge the two
   semantics; do not drop fork code to make the merge easy.
4. `git add -A`, commit the merge with a message listing notable merges.
5. Verify identity files still point at fork.
6. **Build and test on a fast box.** The dev VM is too slow/constrained; the
   human runs the verification commands on their build machine. Do not fight a
   local build past a little `luac`/syntax checking.

## Verification commands (human runs these after you commit)

```bash
cargo check --workspace
cargo clippy --all --tests -- -D warnings
cargo nextest run --workspace
```

`-D warnings` makes every warning fail. Clippy and the Lua tests catch most
sync mistakes. Do targeted suites (`-p makima-lua --test <name>`) to iterate.

## Sharp edges found the hard way (do not rediscover these)

### Lua plugin merges can silently corrupt syntax
`plugins/task/init.lua` got an **extra `end`** during a partial-output merge,
which un-balanced the `pcall(function() ... end)` and broke ~50 tests (the task
plugin is `include_str!`d by the task_policy suite). Always syntax-check every
plugin after merging:

```bash
sudo apt install -y lua5.1   # if needed
for f in $(find plugins -name '*.lua'); do luac -p "$f" || echo "BROKE $f"; done
```

### A subagent's cancel must NOT derive from the parent run's cancel
If `makima.agent.session` uses `agent_ctx.cancel.child()`, then at the end of a
normal run the UI drops the run's `CancelTrigger`, and `CancelTrigger::drop`
**fires** it — closing every in-flight subagent (`task_get` reports
`closed`, never appears in `/tasks`). Fix: create an independent
`CancelToken` whose trigger lives only in the `subagent_cancels` map
(`cancel.rs:94`).

### A completed subagent's reply must be delivered to the main agent
The driver only sent `SubagentHistory` on close, but an async subagent never
closes while idle. Emit `SubagentHistory` after every completed run, guarded by
a `replied` flag so `close()` does not re-queue the same text.

### Poll methods return must-be-used `Dirty`
Upstream made `tick_edge_scroll` etc. return `Dirty`. A merged `tick()` that
calls them with `let _ =` both warns (unused_must_use) and misses repaints.
OR the result into the returned `Dirty`.

### Tests: each `load_source` has its own require cache
A second `load_source` cannot mutate a module another plugin already required
(`bash_helpers`). If you must stub a shared helper, compose the stub and the
plugin into **one** `load_source` so they share the per-env require cache.

### Tests: never hit a real model API
`stub_ctx`'s provider is `NullProvider` (panics on call). For a session that
must actually run, pass **no model_spec** so it inherits the parent's mock
provider, or build a `CannedProvider` returning canned `StreamResponse`s. Build
a production-like `ToolContext` from `stub_ctx` then override `cancel`,
`subagent_cancels`, `event_tx`, `provider`, `model`.

### Rust API sharp edges
- `Role` has no `PartialEq`; use `matches!(m.role, Role::Assistant)`.
- `ToolOutput` is at `makima_agent::ToolOutput` (root), not `tools::`.
- `SessionRef` is `makima_storage::id::SessionRef`.
- Upstream signature drifts you must reconcile: `run_lua_command(name, args,
  depth)`, `methods::new_session_response(id, &modes)`,
  `load_session_response(&modes)`, and the `Server` struct's `modes` field.

## Commit hygiene
Add **new commits** for fixes (the human prefers no rebase/amend mid-session).
Merge-integration fixes can stay in the merge commit; pre-existing fork breakage
goes in its own commit so history stays interpretable.

## Reporting
Report: merge commit hash, one line per conflict (how resolved), build/test
status, an explicit "zero fork features dropped" check, and any upstream change
that conflicts semantically with fork behavior so the human can eyeball it.
