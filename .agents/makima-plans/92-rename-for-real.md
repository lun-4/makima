# Full `maki` -> `makima` rename

## Goal

Remove every remaining identity-bearing `maki` token from the fork, including the Rust workspace, Lua API, environment and log names, UI role prefixes, bridge, docs, site, and tooling. Keep only factual references to the upstream project `tontinton/maki`. Make UI role prefixes configurable so tests never assert the product name.

## Implementation Summary

The fork is already partially renamed: the binary is `makima`, the remote is `lun-4/makima`, the site is `makima.ln4.net`, XDG directories are `makima`, the project directory is `.makima`, the ACP implementation name is `makima`, and the default theme is `makima`. What remains is the crate layer, the Lua API, a handful of environment/log/UI strings, and the docs/site/tooling that describe them.

Measured with `rg -o --pcre2 '(?i)maki(?!ma)'` on the visible tree: about 9.5k tokens across 554 files, plus about 2k in historical `.agents/makima-plans/` documents and about 50 in `acp-command-effect-projection-handover.md`. The generated `site/docs/content/lua-api/_index.md` alone holds 1,140; `site/demo.cast` holds 189; `plugins/` holds 1,195; `maki-lua/` holds 2,948; `maki-ui/` holds 1,656.

### Blast-radius categories

| Category | Surface | Examples | Scope | Treatment |
| --- | --- | --- | --- | --- |
| Workspace identity | 16 crate dirs and manifests, Rust module paths, public types | `maki-agent`, `maki_agent::`, `MakiId`, `MakiIdParseError`, `maki_thinking` | ~6.3k tokens, 301 Rust files, 16 `Cargo.toml`, `Cargo.lock` | Rename dirs with `git mv`, rename packages/modules/types, regenerate lock |
| Lua API and plugins | Global table, doc names, `require` namespace, bundled helper lib, skill name | `maki.api`, `maki.ui`, `require("maki.tool_view")`, `maki.test_helpers`, `maki-plugin-dev` | 1,195 Lua tokens, 1,140 generated lua-api tokens, ~76 UI test snippets | Rename to `makima.*`, `plugins/lib/makima`, `makima-plugin-dev`; regenerate docs |
| Environment, logs, wire identity | Env vars, log filenames, user agents, provider referer | `MAKI_LOG`, `MAKI_TEST_*`, `TEST_MAKI_*`, `maki.log`, `maki.1.log`, `maki.sh` referer | Small, scattered | Rename to `MAKIMA_*`, `makima.log`, `makima.ln4.net`; add tests |
| UI display identity | Assistant role prefix, temp-file prefix, test/bench fixtures | `maki> `, `maki-input-` | ~30 occurrences | Parameterize role names; tests use synthetic prefixes |
| Docs and site | Generated pages, hand-written docs, README/AGENTS, marketing page, demo, banner | `maki.api` anchors, `maki.setup()`, `maki>`, banner wordmark | ~1.5k visible tokens plus binary assets | Rewrite/regenerate; re-record demo; replace banner |
| Tooling and release | CI paths and package names, justfile, bridge package, Python scripts | `maki-*/src/**`, `-p maki-agent`, `maki-claude-agent-bridge`, `tbench_maki_agent.py` | Small | Rename; regenerate lockfile |
| Upstream and historical | Upstream links, fork attribution, historical plans and handover | `tontinton/maki`, `_maki is good`, `.agents/makima-plans/`, `acp-command-effect-projection-handover.md` | ~2k tokens | Keep unchanged; allowlist in the guard test |
| Already renamed | Binary, repo, domain, XDG dirs, project dir, ACP name, theme | `makima`, `lun-4/makima`, `~/.config/makima`, `.makima`, `makima.toml` | — | Verify only; do not double-replace |

### Non-goals

- No backward compatibility. User `init.lua` files, plugins, env vars, log files, and `maki.*` scripts must be updated by their owners.
- No git history rewrite. Directory renames use `git mv` so blame follows.
- No rewriting of historical plans under `.agents/makima-plans/` or `acp-command-effect-projection-handover.md`.
- No change to upstream attribution links (`github.com/tontinton/maki`) or to `lunamaki-sync-with-upstream`.
- No editing of `site/_build/`, `target/`, or `.worktrees/`; they are generated or untracked.
- No Lua API exposure of the role-prefix parameter in this change. Add the internal seam only; a later Lua API will construct it.
- No push, PR, force push, or history rewrite unless the operator asks after the branch is reviewed.

### Branch and commit checkpoints

Create `rename/makima` from the current `mistress` HEAD before any rename work. The operator has authorized scoped autocommits for this task: commit at each checkpoint below without asking again, but stage exactly that checkpoint's files and report each commit. Do not bundle unrelated changes.

| # | Checkpoint | Commit summary | Scope | Verification before commit |
| --- | --- | --- | --- | --- |
| 1 | Phase 1 code | `Rename workspace crates to makima` | 16 `git mv` directories, root/workspace `Cargo.toml`, every crate manifest, `use maki_*` paths, `MakiId`/`MakiIdParseError`, `maki_thinking`, `clippy.toml`, `.gitattributes`, `Cargo.lock`, and in parallel mode `pub const APP_NAME` | `cargo check -p makima --tests`, `just check` |
| 2 | Phase 1 prose | `Rename Rust doc comments to makima` | Rust doc comments and prose only; no behavior | `cargo check`, Rust comment findings from the guard empty |
| 3 | Phase 2 | `Rename environment and log identity to makima` | `MAKIMA_LOG`, test env vars, `makima.log` and rotation, user agents, requesty referer, config/prompt copy, uses the shared `APP_NAME`, associated tests | `cargo nextest run -p makima-storage -p makima-agent -p makima`, relevant Lua specs |
| 4 | Phase 3 core | `Rename the Lua API to makima` | `makima-lua` runtime global, doc names, loader init owners, macro docs, examples/benches | `cargo check -p makima-lua`. Lua tests are expected red until checkpoint 5 because bundled plugins still call `maki.*`; disclose this in the commit body. |
| 5 | Phase 3 plugins | `Migrate bundled plugins to the makima Lua API` | `plugins/**/*.lua`, `plugins/lib/makima`, requires, test helpers, provider specs, task `_makima_*` methods and callers, skill name | `stylua --check plugins/`, `cargo nextest run -p makima-lua`, all plugin specs |
| 6 | Phase 4 | `Parameterize UI role prefixes` | `RoleNames`, `set_role_names`, message/search tests, benches, `MAKI_PREFIX_LEN` removal, UI meta-test | `cargo nextest run -p makima-ui`, `cargo check -p makima-ui --benches` |
| 7 | Phase 5 bridge | `Rename the Claude Agent bridge to makima` | bridge `package.json`/lockfile, `SERVER_NAME`, options/conversations/convert/server strings, tests, bridge README/design | `just bridge-check` |
| 8 | Phase 5 tooling | `Update CI, justfile, and scripts for makima` | workflows, justfile, `tbench_makima_agent.py`, memprobe/collect/tool-token scripts, provider script paths, architecture-test paths | `just check`, `just lint`, `cargo machete` |
| 9 | Phase 6 docs | `Rename hand-written docs and site copy to makima` | README/CONTRIBUTING/AGENTS, `docs/*.md`, nested AGENTS, bridge docs, site index and hand-written pages, `.agents/skills/**` | `just fmt-check`; manual Markdown review |
| 10 | Phase 6 generated | `Regenerate makima docs` | the eight `site/docs/content/*/_index.md` generated pages | `just gen-docs-check` |
| 11 | Phase 6 assets | `Refresh demo and branding assets` | `site/demo.cast`, `banner.png`, favicons/icons, doom videos, any `site/build.sh`/`site/index.html` reference changes | `just site-check`; human visual check. If the operator defers an asset, record the deferral in the body and do not stage a stale file. |
| 12 | Phase 7 cleanup | `Fix remaining makima identity references` | any residual fixes the guard finds, excluding the guard test itself | guard run locally; scoped tests |
| 13 | Phase 7 guard | `Add the residual identity guard` | `tests/rename_identity.rs` only | the full final-state checks: `just ci`, `just bridge-check`, `nix flake check`, `nix build`, `just site-check` |

Rules for every checkpoint:

- Run `cargo fmt` for Rust checkpoints and `stylua plugins/` for Lua checkpoints before staging, so each commit is readable even when it is not CI-green.
- Stage with explicit paths (`git add <path>`), never `git add -A` or `git add .`; inspect `git diff --cached` before committing.
- Commit message: imperative summary under ~72 characters, blank line, then 2-3 short paragraphs explaining what changed and why. Name breaking changes (Lua global, env/log names, init owner ids) in the body.
- Run `git show --stat HEAD` after each commit and report it.
- Intermediate commits need not be CI-green; the final commit (checkpoint 13) must pass the full checks above against the exact staged content. Checkpoint 4 is the known-red intermediate.
- If a checkpoint has no changes (checkpoint 12 is the likely no-op, and checkpoint 11 may be deferred), skip its commit and say why; do not create an empty commit. AC.11 counts the non-empty checkpoints.
- If a checkpoint needs to split further, keep the same granularity and report the split; if it would mix unrelated concerns, ask before committing.
- Do not amend, rebase, force push, or push the branch unless the operator asks.

### Parallel execution (optional)

The linear checkpoint sequence is the default and remains the source of truth. Parallel execution trades merge coordination for wall-clock time and is worthwhile only after the foundation is in place. Cap it at 3-4 concurrent agents; the mechanical crate rename and the generated docs are faster serial.

Foundation, done by the integrator before any agent starts:

1. Phase 0 baseline and `rename/makima`.
2. Phase 1 crate and directory rename, Rust identifier rewrite, and the Phase 1 prose sweep (checkpoints 1-2).
3. Add `pub const APP_NAME` in `makima-storage/src/paths.rs` and fix the contract every agent will target: global `makima`, `plugins/lib/makima`, `makima.test_helpers`, `makima-plugin-dev`, `makima_init.global`/`makima_init.project`. In parallel mode this one-line change moves from Phase 2 into checkpoint 1, so Wave 1 branches from a foundation that already exposes `APP_NAME`.
4. Do not spawn agents until `cargo check -p makima --tests` is green and the contract is written in the agent prompts.

Wave 1, after the foundation:

| Agent | Owned paths | Checkpoint | Depends on | Must not touch |
| --- | --- | --- | --- | --- |
| A | `src/setup.rs`, `makima-storage/src/{log,version}.rs`, `makima-lua/src/api/net.rs`, `plugins/providers/providers/requesty.lua` and its spec, `makima-config/src/{lib,project,providers}.rs`, `makima-agent/src/prompt.rs`, `src/project_trust.rs`, `makima-ui/src/components/mcp_picker.rs`, `makima-ui/src/terminal.rs`, `makima-ui/src/themes/*.toml` | 3 | Foundation | `makima-ui/src/components/messages/**`, `tool_display.rs`, `search_modal.rs`, `makima-ui/benches/**` (D) |
| B | `makima-lua/src/**` except `tests/**` and `api/net.rs`, `makima-lua/examples/**`, `makima-lua/benches/**`, `makima-lua-macro/**` | 4 | Foundation | `makima-lua/tests/**` (H/integrator), `makima-lua/src/api/net.rs` (A) |
| C | `claude-agent-bridge/**` | 7 | Foundation | nothing in Rust |
| D | `makima-ui/src/components/{tool_display,messages,search_modal,streaming_content,btw_modal}.rs`, `makima-ui/benches/{wrap,reflow}.rs` | 6 | Foundation, `APP_NAME` | `mcp_picker.rs`, `terminal.rs` (A) |
| E (optional) | `.github/workflows/**`, `justfile`, `scripts/**`, `src/architecture_tests.rs`, `makima-commands/tests/thinking_architecture.rs` | 8 | Foundation | `Cargo.toml`, `Cargo.lock` (integrator) |

Wave 2, after B's API contract is merged and the Wave 1 branches are integrated or at least contract-stable:

| Agent | Owned paths | Checkpoint | Depends on | Must not touch |
| --- | --- | --- | --- | --- |
| F | `plugins/lib/makima/**` and the non-provider tool plugins (`bash`, `read`, `write`, `edit`, `glob`, `grep`, `list`, `index`, `batch`, `todo_write`, `question`, `memory`, `code_execution`, `view_image`, `webfetch`, `websearch`, `options`, `perf`, `model`, `thinking`, `mode_plan_override`, `plan_submit_tool`, `splashes`, `splashes_default`) | 5a | B | `plugins/providers/**` (G), `sessions`/`task`/`skill`/`usage` (H) |
| G | `plugins/providers/**` | 5b | F's helper path; A's requesty change integrated | `plugins/lib/makima/**` (F) |
| H | `plugins/{sessions,task,skill,usage}/**`, `makima-lua/tests/**` | 5c | F, B's `_makima_*` method names | `plugins/lib/makima/**` (F), `plugins/providers/**` (G) |
| I | `README.md`, `CONTRIBUTING.md`, `AGENTS.md`, `docs/**`, `makima-lua/src/api/AGENTS.md`, `.agents/skills/**` | 9a | B, D, final names | `site/**` (J) |
| J | `site/index.html`, hand-written `site/docs/content/**` (not generated pages) | 9b | B, final anchors | `site/docs/content/{tools,plugins,providers,configuration,lua-api,keybindings,commands,folder-trust}/_index.md` (integrator) |

Run at most three agents concurrently. A sensible schedule is A/B/C, then D/E, then F/I/J (F establishes the shared plugin helper path while the docs agents work), then G/H. The integrator handles checkpoints 1-2, 10-13, and all conflict resolution.

When the parallel checkpoints split, use these commit summaries: 5a `Migrate shared plugin helpers and core tools to makima`, 5b `Migrate provider plugins to makima`, 5c `Migrate remaining plugins to makima`, 9a `Rename repository docs and skills to makima`, 9b `Rename hand-written site docs to makima`.

Shared-file rules:

- `Cargo.toml`, `Cargo.lock`, `tests/rename_identity.rs`, and the generated `site/docs/content/*/_index.md` pages are integrator-only.
- `makima-ui/src/components/messages/tests.rs` belongs to D. A must not edit it.
- `plugins/lib/makima/**` belongs to F. G and H wait for F's helper path before starting.
- `makima-lua/tests/**` belongs to H. B and F must not edit Rust test harness files. `makima-lua/src/api/net.rs` belongs to A; B must not edit it.
- `src/architecture_tests.rs` belongs to E; `justfile` and workflows also belong to E.
- Docs cross-links and anchors are a contract from B; I and J use the final anchors and do not rename them independently.
- If an agent needs a file outside its ownership, it stops and reports; it does not edit the file or run a repo-wide replacement.

Worktree and branch mechanics:

- Each agent gets its own worktree and branch from the integration branch, for example:
  ```
  git worktree add /tmp/makima-rename/agent-a -b rename/agent-a rename/makima
  ```
- Each worktree builds in its own `target/` directory. Do not share `CARGO_TARGET_DIR` across concurrent builds; cargo lock contention and test temp dirs make results unreliable.
- Agents may commit on their own branch with the checkpoint summary. They do not commit to `rename/makima`, push, rebase, or amend.
- The integrator reviews `git log --oneline rename/makima..rename/agent-a` and `git diff --stat`, runs the checkpoint's scoped checks, then integrates with `git merge --squash rename/agent-a` or a cherry-pick and commits with the checkpoint message. Keep `rename/makima` linear; do not create merge commits there.
- If a parallel checkpoint splits, use 5a/5b/5c for the plugin migration and 9a/9b for docs. AC.11 counts those non-empty sub-checkpoints.
- Intermediate agent branches may be red. The integrator still verifies each integrated checkpoint where feasible and runs the full checks only on checkpoint 13.
- The residual guard runs once at the end, by the integrator. Agents may run scoped `rg` checks but must not run the full guard or fix files outside their ownership.

Parallelism risks:

- Merge conflicts concentrate in shared imports, `Cargo.toml`, test harness files, and docs cross-links. The ownership table is the mitigation; a repo-wide replacement by any agent is a defect.
- Agent context drift is the other cost: each prompt must repeat the target names, the owned paths, the checkpoint message, the verification command, and the no-touch list.
- The plugin sweep is large but highly repetitive; a single script plus one agent may beat three agents once integration time is counted. Use parallelism where the work is genuinely independent (bridge, UI, docs), not to split a mechanical replacement.

## Implementation Plan

### Phase 0: Baseline

Run the cheap checks before touching anything and record any pre-existing failures:

- `just check`
- `just lint`
- `just test`
- `just gen-docs-check`
- `just bridge-check`

Work on a clean worktree. If a check is already red, record it so the rename is not blamed for it. Then create the branch with `git switch -c rename/makima` from the current `mistress` HEAD. If the worktree is dirty, stop and ask; do not stash unrelated work without approval.

### Phase 1: Crates, directories, and Rust identifiers

Rename these directories with `git mv`:

| Old | New |
| --- | --- |
| `maki-acp` | `makima-acp` |
| `maki-agent` | `makima-agent` |
| `maki-commands` | `makima-commands` |
| `maki-config` | `makima-config` |
| `maki-config-macro` | `makima-config-macro` |
| `maki-docgen` | `makima-docgen` |
| `maki-domain` | `makima-domain` |
| `maki-highlight` | `makima-highlight` |
| `maki-interpreter` | `makima-interpreter` |
| `maki-lua` | `makima-lua` |
| `maki-lua-macro` | `makima-lua-macro` |
| `maki-markdown` | `makima-markdown` |
| `maki-match` | `makima-match` |
| `maki-providers` | `makima-providers` |
| `maki-storage` | `makima-storage` |
| `maki-ui` | `makima-ui` |

Also `git mv maki-domain/src/maki_thinking.rs maki-domain/src/makima_thinking.rs`.

After this phase the directories are `makima-*`. The file paths in Phases 2-6 are written with the pre-rename names for readability; translate them through the table above when executing. Root paths such as `src/`, `tests/`, `plugins/`, `site/`, `docs/`, and `scripts/` keep their names.

Update:

- Root `Cargo.toml`: package `name = "makima"`, `[workspace] members`, `[workspace.dependencies]`, root dependencies and dev-dependencies, and the version comment that points at `maki-ui/src/splash_golden.txt`.
- Every crate manifest: `name`, workspace dependencies, dev-dependencies, descriptions that say "for maki" or "maki-ui"/"maki-lua", and `maki-docgen`'s `[[bin]] name = "makima-docgen"`.
- Rust module paths: `maki_<crate>` -> `makima_<crate>` for all 16 crates, including `maki_config_macro` and `maki_lua_macro`.
- Crate-name strings and paths: `maki-<crate>` -> `makima-<crate>`.
- Public types: `MakiId` -> `MakimaId`, `MakiIdParseError` -> `MakimaIdParseError`, `as_maki_id` -> `as_makima_id`.
- `maki-domain/src/lib.rs`: `pub mod makima_thinking;` and the re-export.
- Internal identifiers: `create_maki_global` -> `create_makima_global`; `maki_to_claude_tool_name`/`claude_to_maki_tool_name` -> `makima_*`; `maki_commands_dependency_direction_is_neutral` -> `makima_commands_dependency_direction_is_neutral`; `maki_dir` locals -> `makima_dir`.
- `maki-commands/tests/thinking_architecture.rs` path to `makima-domain/src/makima_thinking.rs`.
- `clippy.toml` reasons to `makima_storage::id::MakimaId`.
- `.gitattributes` comment to `makima-docgen`.

Regenerate `Cargo.lock` by running `cargo check` (or `cargo generate-lockfile`) and commit the result. Verify `cargo check -p makima --tests` and `just check` before moving on.

Phase 1 also owns the Rust prose and doc-comment sweep. A green `cargo check` is not the end: doc comments such as `maki.agent.call_tool` (`makima-agent/src/tools/mod.rs`), `maki index` (`tools/mod.rs`), `maki-ui`/`maki-lua` (`makima-markdown/src/render.rs`), `maki.match` (`makima-match/src/lib.rs`), and "Every tool call in maki" (`agent/tool_dispatch.rs`) are part of this phase. Use the Phase 7 guard as the enumeration mechanism for Phase 1's doc comments; Phase 1's doc-comment findings are empty before moving on. The full guard cannot be green until Phases 2-4 have renamed the env, log, UI, and Lua strings.

Add the package and CLI identity tests now:

- `package_name_is_makima` in `src/main.rs` or `src/lib.rs`: `assert_eq!(env!("CARGO_PKG_NAME"), "makima")`.
- `cli_name_is_makima` in `src/cli.rs`: `assert_eq!(Cli::command().get_name(), "makima")`. This one already passes today (`src/cli.rs` already sets `#[command(name = "makima")]`); it is a regression guard. `package_name_is_makima` is the AC.3 test that fails before the change.

Do not blind-replace. `makima` already exists in many strings, and `maki` is a substring of `making`; use token-boundary rules and review the diff.

### Phase 2: Environment, logs, paths, and user-facing strings

- `src/setup.rs`: `MAKI_LOG` -> `MAKIMA_LOG`; update the doc comment and the `MAKIMA_LOG` examples in `maki-lua/src/docs_render.rs` and `site/docs/content/plugins/_index.md`.
- Test-only env vars: `MAKI_TEST_*` -> `MAKIMA_TEST_*`; `TEST_MAKI_*` -> `TEST_MAKIMA_*`.
- `maki-storage/src/log.rs`: `maki.log` -> `makima.log`, `maki.log.lock` -> `makima.log.lock`, `maki.N.log` -> `makima.N.log`, the `maki:` stderr prefix, and all rotation comments/tests. `file_path` currently builds `format!("maki.{index}.log")` independently of `ROTATED_PREFIX`; make it use `ROTATED_PREFIX`/`ROTATED_SUFFIX`, and update the existing `"maki.7.log", Some(7)` case to `"makima.7.log"`. Old `maki.*.log` files must not be treated as ours.
- `maki-lua/src/api/net.rs`: `FALLBACK_USER_AGENT = "maki"` -> `APP_NAME`.
- `plugins/providers/providers/requesty.lua`: `REFERER = "https://maki.sh"` -> `https://makima.ln4.net`; update `plugins/providers/tests/requesty.lua`.
- `src/architecture_tests.rs`: remove `"maki.sh"` from `SHARED_HOSTS`; rename `FRONTEND_ROOTS`, the `maki-commands` dependency test and its crate list, and all `maki-*` paths.
- User-facing messages:
  - `maki-config/src/project.rs`: `.maki` -> `.makima` in `NO_SHARED_FILES_YET`, comments, and test names; `maki trust add/remove/list` -> `makima trust ...` in `HOW_TO_TRUST`, `CLEAR_THE_REJECTION`, comments, and tests. Also rename the user-facing `Maki` copy: "restart Maki" in `HOW_TO_TRUST`, the gated-file descriptions ("for Maki and every process it starts", "inside Maki's own process"), `"Maki can load shared project configuration from {}."`, and the related `warn!` at line 495. While here, update `TRUST_DOCS` from the stale `https://lun-4.github.io/makima/docs/folder-trust/` to `https://makima.ln4.net/docs/folder-trust/`.
  - `maki-config/src/lib.rs`: `maki.setup` -> `makima.setup` in error messages.
  - `maki-ui/src/components/mcp_picker.rs`: `'maki mcp auth ...'` -> `'makima mcp auth ...'`.
  - `maki-storage/src/trusted_folders.rs`, `maki-agent/src/permissions.rs`, `maki-acp/src/permissions.rs`: stale `.maki/permissions.toml` comments -> `.makima/permissions.toml`; `maki trust` prose -> `makima trust`.
  - `maki-storage/src/lock.rs`: `maki-pack` -> `makima-pack` (or rephrase to "the makima pack process").
  - `src/project_trust.rs`: test argv `"maki"` -> `"makima"`.
  - `maki-acp/src/server.rs`: test path `/tmp/maki-acp-test` -> `/tmp/makima-acp-test`.
  - `maki-ui/src/event_loop.rs`: test path `/nonexistent/maki-plan-test.md` -> `/nonexistent/makima-plan-test.md`.
  - `maki-ui/src/terminal.rs`: temp prefix `maki-input-` -> `makima-input-`; test title and OSC2 expectation -> `makima`.
- `maki-ui/src/themes/*.toml`: comments "for Maki" -> "for Makima" (15 files).
- `maki-agent/src/prompt.rs`: `DEFAULT_IDENTITY` ("You are Maki, an interactive CLI coding agent...") -> "You are Makima, ..."; update the four `"You are Maki"` test assertions. This changes the model's system-prompt self-identity, not just a label.
- Remaining guard-caught identifiers with explicit new values: `__MAKI_REFERENCE_PATH__` -> `__MAKIMA_REFERENCE_PATH__` in `maki-lua/src/docs_render.rs`; `"maki"` sample values in `api/json.rs`/`api/yaml.rs` -> `"makima"`; `~/projects/maki:main` in `maki-ui/src/components/status_bar.rs` -> `~/projects/makima:main` and update the width-10 expected truncation in the same test; `/home/user/.local/state/maki/plans/test.md` and `__maki_test_*` in `maki-agent/src/permissions.rs` -> `.../makima/...` and `__makima_test_*`; `MCP_ARGS = "{\"q\":\"maki\"}"` -> `"{\"q\":\"makima\"}"` in the same file; `..._code_maki_does_not_own` test-case labels in `maki-agent/src/agent/tool_dispatch.rs` -> `..._code_makima_does_not_own`; `maki-ui/src/text_buffer.rs` comment in `plugins/lib/maki/text_input.lua` -> `makima-ui/src/text_buffer.rs`; `maki_to_claude_roundtrip` -> `makima_to_claude_roundtrip` in `src/sdk_mode.rs`.
- Introduce one shared app-name constant:
  - Make `APP_NAME` public in `maki-storage/src/paths.rs` (`pub const APP_NAME: &str = "makima";`).
  - Use it in `maki-lua/src/api/net.rs` fallback UA, `maki-storage/src/version.rs` User-Agent, `tests/frontend_parity.rs` `APP_DIR`, and the role-prefix default in Phase 4.
  - Keep literals where a const cannot be used (log filenames), and guard them with tests.

Add tests:

- `log_file_names_use_app_name` in `maki-storage/src/log.rs`: assert `LOG_FILE_NAME == format!("{APP_NAME}.log")`, `LOCK_FILE_NAME == format!("{APP_NAME}.log.lock")`, `ROTATED_PREFIX == format!("{APP_NAME}.")`.
- `rotated_index_rejects_legacy_names` in the same module: build the legacy name with `concat!("ma", "ki", ".7.log")` so the source does not contain the forbidden token, and assert `rotated_index(&legacy) == None`.
- `log_env_is_the_app_name_upper` in `src/setup.rs`: `LOG_ENV == format!("{}_LOG", APP_NAME.to_uppercase())`.
- `fallback_user_agent_uses_app_name` in `maki-lua/src/api/net.rs`.

### Phase 3: Lua API and plugin namespace

Rename the global table and every doc name:

- `maki-lua/src/api/mod.rs`: `create_maki_global` -> `create_makima_global`, local `maki` -> `makima`, `maki.set(...)` -> `makima.set(...)`.
- `maki-lua/src/runtime.rs`: `build_env` parameter and `env.set("maki", maki)` -> `env.set("makima", makima)`; thread name `"maki-lua"` -> `"makima-lua"`; splash test glyphs `'maki'` -> `'makima'`.
- `maki-lua/src/loader.rs`: `GLOBAL_INIT_OWNER`/`PROJECT_INIT_OWNER` `maki_init.global`/`maki_init.project` -> `makima_init.global`/`makima_init.project`; update `api/session_option.rs`, tests, and docs. This is a user-facing `init.lua` session-option id and `plugins.<owner>` opts key, so treat it as a breaking config change. Also update `PERMISSION_NAME_WARNING` ("inherits maki's permission rules...") to `makima's`.
- `maki-lua/src/docs.rs`: `resolve_table` parameter/local, `name == "maki"` -> `"makima"`, module-name examples.
- `maki-lua/src/api/top.rs`: `extend "maki"` -> `extend "makima"`; tests.
- `maki-lua/src/splash.rs`, `ctx.rs`, `credentials.rs`, `login_ui.rs`, `session_snapshot.rs`, `agent_autocmd.rs`: doc names, examples, parameter names.
- `maki-lua/examples/splash_bench.rs`, `maki-lua/benches/splash_perf.rs`, and `maki-lua/src/api/split.rs` test setup: replace the `maki` global with `makima`.
- Every `lua_table!` / `lua_class!` string name and every `#[lua_fn]` doc example: `maki.` -> `makima.`.
- `maki-lua-macro/src/lib.rs` doc examples: `maki.x` -> `makima.x`.

Plugin namespace:

- Across `plugins/**/*.lua`, replace every `maki.` global reference with `makima.` and every `require("maki.*")` with `require("makima.*")`. This includes the tool plugins, `plugins/providers`, `plugins/splashes`, `plugins/splashes_default`, and every spec.
- Rename the Rust-registered Lua methods `_maki_retired`, `_maki_can_control`, `_maki_task_owner`, and `_maki_session_id` in `maki-lua/src/api/agent.rs` and `api/agent/handles/agent_methods.rs`, and update every Lua caller/stub in `plugins/task/init.lua`, `maki-lua/tests/task_policy.rs`, and the `api/agent/handles/mod.rs` snippets. Both sides must change together; a one-sided rename breaks the bundled task plugin and its tests.
- Update the hard path in `maki-lua/tests/plan_mode_clone.rs` (`include_str!("../../plugins/lib/maki/plan_spec.lua")` -> `.../makima/plan_spec.lua`) and the comment in `maki-lua/tests/plugin_host.rs` that names `plugins/lib/maki/partial.lua`.
- `git mv plugins/lib/maki plugins/lib/makima`.
- Every `require("maki.*")` -> `require("makima.*")` in bundled plugins, specs, and docs.
- `maki.test_helpers` -> `makima.test_helpers` in `plugins/lib/makima/test_helpers.lua`, all specs, `AGENTS.md`, and `site/docs/content/plugins/_index.md`.
- Test temp prefixes `/tmp/maki_*` -> `/tmp/makima_*` in `plugins/lib/makima/test_helpers.lua`, `plugins/memory/tests/spec.lua`, and `plugins/write/tests/spec.lua`.
- `maki-plugin-dev` -> `makima-plugin-dev` in `maki-lua/src/docs_render.rs`, `maki-lua/src/write_lock_regression.rs`, `maki-lua/tests/completion_plugins.rs`, `plugins/skill/init.lua`, `plugins/skill/tests/spec.lua`, and docs.
- `maki-lua/src/docs_render.rs`: `get_dir("maki")` -> `get_dir("makima")`, `maki.{stem}` -> `makima.{stem}`, the `expect("plugins/lib/maki embedded")` message, and all guide text/examples.
- `maki-docgen/src/lua_util.rs`: static extraction path `["maki", "api", "register_command"]` -> `["makima", ...]`; tests.
- `maki-docgen/src/gen_lua_api.rs`, `gen_plugins.rs`, `gen_tools.rs`, `gen_config.rs`, `gen_commands.rs`, `gen_keybindings.rs`, `gen_folder_trust.rs`: all `maki.`/`Maki` prose and examples.
- `plugins/providers/types.lua` and all provider modules/tests: `maki.*` -> `makima.*`; `plugins/providers/providers/http.lua` comment `maki_config::expand_env` -> `makima_config::expand_env`.

Update every Rust test that embeds Lua: `maki-lua/tests/*.rs` (including the Lua snippets and fixture strings in `in_memory_host.rs`, `plugin_host.rs`, `events_slots.rs`, `agent_handles.rs`, and `common/mod.rs`), `maki-ui/src/event_loop/focus_tests.rs`, `maki-ui/src/event_loop/approval/tests.rs`, `maki-ui/src/event_loop.rs`, `maki-acp/src/server.rs`, `src/print.rs`, `src/sdk_mode.rs`, `src/cmd/subcmd.rs`, `tests/frontend_parity.rs`.

Add `makima_global_installed_and_legacy_global_absent` in `maki-lua/tests/runtime_alignment.rs` (or `in_memory_host.rs`):

- `PluginHost` exposes no Lua handle, so drive the assertion from Lua through `PluginHost::load_source`: load a chunk containing `assert(makima ~= nil, "makima global missing")`, `assert(maki == nil, "legacy maki global present")`, and `assert(type(makima.version) == "function")`.
- Assert the chunk loads successfully. Do not try to call `lua.globals()` on `PluginHost`; that will not compile.

Verify with `cargo nextest run -p makima-lua` and the plugin specs in `makima-lua/tests/spec.rs`.

### Phase 4: Parameterize UI role prefixes

The current `maki> ` assistant prefix is hardcoded in `maki-ui/src/components/tool_display.rs` and asserted literally in tests and benches. Replace it with an internal parameter so no test encodes the product name and a later Lua API can drive it.

Design:

- Add `RoleNames` in `maki-ui/src/components/tool_display.rs` (or a small sibling module), deriving `Clone`:
  ```rust
  #[derive(Clone)]
  pub struct RoleNames {
      pub assistant: Arc<str>,
      pub user: Arc<str>,
      pub thinking: Arc<str>,
  }
  ```
  `Default` derives `assistant` from `makima_storage::paths::APP_NAME` with `format!("{APP_NAME}> ")`; `user` is `"you> "`; `thinking` is `"thinking> "`.
- Change `RoleStyle.prefix` from `&'static str` to `Arc<str>`.
- Change constructors to `assistant_style(names: &RoleNames)`, `user_style(names: &RoleNames)`, `thinking_style(names: &RoleNames)`. `error_style`, `done_style`, and `post_style` construct `Arc::from("")` / `Arc::from(TOOL_INDICATOR)`.
- Change `message_style(role, names)`, `message_prefix` to return `&str`, `build_message_lines(msg, width, names)`, and `message_search_text(msg, names)`. Update every call site, including the test-only call at `maki-ui/src/components/messages/tests.rs:1226`, to pass `&self.role_names` / `&names`.
- `MessagesPanel` stores `role_names: RoleNames` and constructs it with `RoleNames::default()`. Add `pub fn set_role_names(&mut self, names: RoleNames)` that updates both `StreamingContent` prefixes, invalidates the line cache, and rebuilds on the next render. This is the seam a future Lua API will call; do not expose it to Lua now, and do not wire it to `UiConfig` in this change. It will have no production caller until the Lua API lands, which is expected. Add `#[allow(dead_code)]` with a short comment (precedent: `maki-ui/src/components/permission_prompt.rs:118`); `maki-ui/src/lib.rs` keeps `mod components;` private, so an uncalled `pub fn` is dead code and `just lint` runs clippy with `-D warnings`.
- `StreamingContent.prefix` becomes `Arc<str>`; `new`/`set_style` accept `impl Into<Arc<str>>` so existing literals still compile.
- Replace `THINKING_HIDDEN_HEADER` with `thinking_indicator(line_count, names)` using `names.thinking`; update both call sites in `maki-ui/src/components/messages/mod.rs`.

Test changes:

- Add a test-only `test_role_names()` with synthetic prefixes (`"assistant> "`, `"user> "`, `"thinking> "`) in `maki-ui/src/components/messages/tests.rs`.
- `default_assistant_prefix_comes_from_app_name`: assert `RoleNames::default().assistant.as_ref() == format!("{APP_NAME}> ")`, referencing the shared constant rather than a literal.
- `assistant_prefix_is_configurable`: render an assistant message with `test_role_names()` and assert the synthetic prefix; call `set_role_names(other_names)`; render again and assert the already-rendered message now shows `other_names`. This proves cache invalidation, not just that a newly pushed message uses the new names.
- `search_text_includes_role_prefix`: use `test_role_names()` and assert against its fields, not `makima>`.
- `search_role_prefix_matches` in `search_modal.rs`: replace `maki>`/`you>` literals with synthetic test constants and rename the `maki_prefix` test-case label to `assistant_prefix`.
- `thinking_indicator_uses_configured_prefix`: assert the collapsed header uses the configured thinking prefix.
- Add `ui_tests_do_not_hardcode_product_prefix`: walk `maki-ui/src/components/**/*.rs` and `maki-ui/benches/*.rs`, scan for `format!("{APP_NAME}>")`, and fail with path:line. This is the executable enforcement of "tests never assert the product name"; referencing `APP_NAME` keeps a future rename from silently disabling the check, and the test source contains no product-name literal.
- `MAKI_PREFIX_LEN` in `maki-ui/src/components/messages/tests.rs` is a test constant equal to 6 (the old `maki> ` length), used by selection tests at lines 1019, 1069, 1089, and 1090. Remove it. `panel_with_msgs` at line 1002 must call `set_role_names(test_role_names())` before building, and the selection tests derive offsets from `test_role_names().assistant.len()`; do not hardcode 8 or mix the default and synthetic prefixes.
- Benches `maki-ui/benches/wrap.rs` and `reflow.rs`: use a neutral `"assistant> "` constant, not the product name.

Verify with `cargo nextest run -p makima-ui` and `cargo check -p makima-ui --benches`.

### Phase 5: Bridge, scripts, CI, Nix

Bridge:

- `claude-agent-bridge/package.json`: `name` and `bin` -> `makima-claude-agent-bridge`; description "Serves Makima's...".
- Regenerate `package-lock.json` with `npm install --package-lock-only` or `npm install --omit=optional`.
- `src/tools.ts`: `SERVER_NAME = "makima"`. Replace every literal server name with `SERVER_NAME`: `src/options.ts` (`{ maki: server(...) }`), `src/conversations.ts` connection check, `test/fake.ts`, `test/conversations.test.ts`, and `test/server.test.ts`. Update `src/main.ts` usage text and the `src/protocol.ts`/`src/main.ts` path comments (`maki-lua/src/api/provider.rs`, `maki-storage paths.rs`).
- Rename identity-bearing runtime strings: `src/convert.ts:10` conversation-checkpoint prompt "history from Maki" -> "from Makima"; `src/server.ts:102` error "Maki closed the step" -> "Makima closed the step"; the remaining `Maki` prose in `src/records.ts`, `src/options.ts`, and `src/convert.ts` -> Makima; `test/convert.test.ts` test names "stream events become Maki events" and "a failed turn maps to a Maki error" -> Makima.
- Add `server_name_is_makima` in the existing `claude-agent-bridge/test/server.test.ts`: assert `SERVER_NAME === "makima"` and that the `mcpServers` key built by `options.ts` is `SERVER_NAME`. This is the test that fails if either side reverts.
- `README.md` and `claude-agent-bridge-design.md`: rename prose and `maki-lua/src/api/provider.rs` path references; update the design doc's sample `"session_id": "maki-session-1"` to `"makima-session-1"`.

Scripts:

- `git mv scripts/tbench_maki_agent.py scripts/tbench_makima_agent.py`.
- Rename `MakiAgent` -> `MakimaAgent`, update `--agent-import-path` examples and docstrings. While renaming, correct its `~/.makima/auth` and `~/.makima/providers` mounts to the current state/config locations (`~/.local/state/makima/auth`, `~/.config/makima/providers`) if the benchmark still needs them.
- `scripts/memprobe.py`: example paths `./base/maki`, `./target/debug/maki` -> `makima`.
- `scripts/tool_token_analysis.py`: `MAKI SESSION TOOL TOKEN ANALYSIS` -> `MAKIMA ...`.
- `scripts/collect.py`: docstring "runs Maki" -> "runs Makima".
- `scripts/providers/openai-oauth`: while touching it, correct stale `~/.makima/providers` and `~/.makima/auth` paths to the current XDG locations (`~/.config/makima/providers`, `~/.local/state/makima/auth`).

CI and build:

- `.github/workflows/rust.yml`: path filters `maki-*/...` -> `makima-*/...`; Windows `cargo test -p maki-agent`/`-p maki-storage` -> `makima-*`.
- `.github/workflows/pages.yml`: keep the upstream `maki.sh`/`tontinton/maki` guards; add a generated-doc check for stale `maki.` if the residual test does not already cover it.
- `justfile`: `-p maki-docgen` -> `-p makima-docgen`; `-p maki-lua` -> `-p makima-lua`; the `maki-lua` bench comment -> `makima-lua`; add the `site-check` recipe described in Phase 7.
- `flake.nix`: no literal change; `packageName` derives from `Cargo.toml`. Verify `nix build` produces `result/bin/makima`.
- `Cargo.lock`: regenerated in Phase 1.
- `src/architecture_tests.rs`: `FRONTEND_ROOTS`, `SHARED_HOSTS`, and the dependency-direction test. `workspace_source_roots` derives from `fs::read_dir(".")` and needs no edit.
- `maki-commands/tests/thinking_architecture.rs`: updated path.
- `maki-storage/src/lock.rs`: `maki-pack` prose.

### Phase 6: Docs and site

Hand-written documentation:

- `README.md`: update `cargo install --locked --git ... maki` -> `makima`; remove the "package is still named `maki`" note; update `maki` API references. Keep the upstream `tontinton/maki` attribution lines.
- `CONTRIBUTING.md`: "contributing to maki" -> "Makima".
- `AGENTS.md`: crate names, Lua API names, `maki-docgen`, `maki.test_helpers`, and `maki-plugin-dev`.
- `maki-lua/src/api/AGENTS.md`: `maki.fs`/`maki.uv`/`maki.treesitter`, crate names, `cargo nextest -p makima-lua`.
- `docs/agent-system.md` and `docs/agent-invariants.md`: crate paths and Lua API names.
- `claude-agent-bridge/README.md` and `claude-agent-bridge-design.md`.
- `site/docs/DESIGN.md`: "maki's TUI heritage" -> "Makima's TUI heritage"; line 37's "Custom `maki` syntax theme" -> "Custom `makima` syntax theme" (the theme is `makima` in `site/docs/config.toml`). Do not use "upstream maki's" here; it would leave a token that is not on the allowlist.
- `.agents/skills/aesthetic-splash-screens/**`: update the skill text, `references/template.lua`, `references/shader-porting.md`, `scripts/bench.lua`, `scripts/smoke_test.lua`, and `scripts/dump_frame.lua` to `makima.*`, `_G.makima`, `-p makima-lua`, `~/.config/makima/lua`, and Makima prose. This is developer-facing API documentation and must not rot.
- `.agents/skills/lunamaki-sync-with-upstream/SKILL.md`: update our-identity references (`maki-storage` -> `makima-storage`, `maki-lua` -> `makima-lua`, `maki_agent`/`maki_storage` paths, `lun-4/maki` -> `lun-4/makima`, `luna.sync-maki-*` -> `luna.sync-makima-*`, `LOGO="luna-maki"` -> `LOGO="makima"`). Keep the upstream references (`upstream maki`, `tontinton/maki`) and the `lunamaki` skill name; add those lines to the guard allowlist.
- `site/docs/content/_index.md` and `docs/README.md` already say Makima; verify only. (`rg` finds no true `maki` tokens there; the apparent hits are `makima`.)

Hand-written site pages:

- `site/index.html`: update the Lua code samples (`makima.api.*`), prose, and keep the upstream `tontinton/maki` attribution.
- `site/docs/content/quick-start/_index.md`: `cargo install ... makima`; remove the old package-name note; `makima.setup()`.
- `site/docs/content/headless/_index.md`, `hooks`, `notifications`, `modes`, `permissions`, `references`, `skills`, `splash`, `agent-work`, `token-economy`, `acp`, `cli`, `mcp`, `context`: replace `maki.*` with `makima.*` and update anchors.
- `site/docs/content/skills/_index.md`: `makima-plugin-dev`.
- `site/docs/content/permissions/_index.md`: `makima trust`, `makima.net`, `.makima/permissions.toml`.

Generated pages: run `just gen-docs` and commit the outputs for `tools`, `plugins`, `providers`, `configuration`, `lua-api`, `keybindings`, `commands`, and `folder-trust`. The lua-api page's anchors change from `#maki-*` to `#makima-*`; regenerate all cross-links by rewriting the hand-written docs and then checking the build.

Add `lua_api_page_uses_makima_names` in `maki-docgen/src/gen_lua_api.rs`: generate the page, assert it contains `makima.api.register_command`, and assert it does not contain `concat!("ma", "ki", ".")` (the whole legacy module prefix, not only `.api`). Construct the legacy token dynamically so the test source does not trip the residual guard. This is the named test for AC.8, not a manual grep.

Site identity check: add the `just site-check` recipe and the `site_build_is_clean` test described in Phase 7. This gives AC.9 a named, executable check instead of an ad-hoc grep.

Assets:

- `site/demo.cast`: re-record with the new binary. It must show the `makima` shell prompt, `makima>` assistant prefix, `makima-plugin-dev`, and `makima.ui.*` docs. If re-recording is impossible in the execution environment, stop and ask the operator. If the operator defers the cast, remove `site/demo.cast` and its `site/build.sh`/`site/index.html` references rather than shipping stale frames, and record the deferral; the cast tests return early when the file is absent.
- `banner.png`: replace the wordmark with `makima`. This is a binary asset with no OCR/render harness; validate visually. If the operator cannot supply a new banner, stop and ask; do not leave the old wordmark in a renamed project. AC.9's banner clause is operator-provided or explicitly deferred.
- Visual site assets: review `favicon.ico`, `favicon-16x16.png`, `favicon-32x32.png`, `apple-touch-icon.png`, `android-chrome-192x192.png`, `android-chrome-512x512.png`, and `hero-cards*.svg`. Replace any that carry the old `maki` wordmark or logo; the same operator-provided-or-deferred rule applies.
- `doom.mp4` and `doom-av1.mp4`: review the recordings. If the TUI shows the old `maki>` prefix or wordmark, re-record them as well; if re-recording is not possible, flag them for the operator rather than shipping a stale recording.
- Keep `.agents/makima-plans/*` and `acp-command-effect-projection-handover.md` untouched.

### Phase 7: Residual identity guard and verification

Add `no_stale_identity_tokens` to `tests/rename_identity.rs`, not `src/architecture_tests.rs`. The guard file contains the matcher fixtures and the upstream allowlist data, so it is the only file skipped wholesale; keeping `src/architecture_tests.rs` scanned preserves coverage of `FRONTEND_ROOTS`, `SHARED_HOSTS`, and the dependency list. This test is the single enumeration mechanism for the rename, so its matcher must be stronger than a word-boundary scan.

File discovery and path check:

- Get tracked files with `git ls-files -z`, and also `git ls-files --others --exclude-standard -z` so newly created test/fixture files are scanned before they are staged. Fail with a clear message if git is unavailable; this is a repository test, not a packaged-crate test.
- Apply the token rules to each tracked path as well as its contents. This catches a file left named `scripts/tbench_maki_agent.py` or a directory left named `plugins/lib/maki`.
- Skip `target/`, `site/_build/`, `.agents/makima-plans/`, `acp-command-effect-projection-handover.md`, `.github/workflows/pages.yml`, and the guard file `tests/rename_identity.rs` for the content pass. The guard file is skipped because `identity_matcher_cases` and the allowlist data intentionally contain the forbidden tokens as fixtures. The path pass still sees `.agents/makima-plans` (it contains `makima`, not `maki`, so it is harmless). `pages.yml` is skipped because it deliberately keeps the upstream-origin guards (`https://maki.sh`, `tontinton/maki`); review that file manually and keep only those upstream strings.
- Scan text extensions: `.rs`, `.lua`, `.md`, `.toml`, `.yml`, `.yaml`, `.ts`, `.py`, `.sh`, `.ps1`, `.html`, `.json`, `.txt`, `.nix`, `.svg`, `.webmanifest`, plus the extensionless `justfile`, `Cargo.lock`, `clippy.toml`, `.gitattributes`, `.gitignore`, `.githooks/pre-commit`, `LICENSE`, and `scripts/providers/openai-oauth`.

Token rules:

- `(?i:(?<![A-Za-z])maki(?![A-Za-z]))` catches `maki`, `MAKI`, `maki_`, `_maki_`, `maki-`, `maki.`, `maki>`, `tontinton/maki`, `TEST_MAKI_GLOBAL_ONLY`, and `create_maki_global`. It does not catch `makima`, `making`, or `lunamaki`.
- `(?<![A-Za-z])Maki(?![a-z])` catches `MakiId`, `MakiAgent`, and standalone `Maki`, but not `Makima`. Keep this rule outside the `(?i:...)` scope, or it will also become case-insensitive and stop matching the CamelCase forms.
- Assert the tree contains no `makimama` (the signature of a double replacement).

Matcher implementation: Rust's `regex` crate has no lookaround, and the root crate does not depend on it. Implement a small hand-rolled scanner instead of translating these patterns to `\b` (which treats `_` as a word character and reintroduces the underscore miss):

- Iterate over `match_indices` of `"maki"` after ASCII-lowercasing the haystack (ASCII case folding preserves byte offsets), and over `match_indices` of `"Maki"` on the original haystack.
- A match counts when the preceding character is not alphabetic and the following character is not alphabetic (`char::is_alphabetic`). `_`, `.`, `-`, `/`, `>`, quotes, and punctuation are all non-alphabetic, so `_maki_`, `create_maki_global`, `maki.fn`, and `maki>` match; `makima` and `making` are excluded by the following-character check; `lunamaki` is excluded by the preceding-character check.
- Add `identity_matcher_cases` as a table-driven unit test with must-match cases (`maki`, `MAKI`, `maki_`, `_maki_`, `maki.fn`, `maki>`, `tontinton/maki`, `MakiId`, `MakiAgent`, `scripts/tbench_maki_agent.py`, `plugins/lib/maki`) and must-not-match cases (`makima`, `making`, `lunamaki`, `makiconf`, `makimama`). This is the named unit test for AC.1's matcher.

`site/demo.cast` needs its own pass:

- Parse the asciicast v2 ndjson: the first line is the header, each later line is `[time, "o", payload]`.
- Concatenate the `"o"` payloads in order, strip CSI/OSC escape sequences, and apply `\b`/`\r` handling so erased intermediate keystrokes do not produce false positives. Then apply the token rules to the resulting terminal text. A raw line scan sees some occurrences but misses the split/backspaced cases; the decoder reconstructs the terminal text and catches them.
- Expose this as the separate named test `cast_text_is_clean` so AC.9 has a real automated half and the AC mapping stays explicit.

`site/_build` needs a pass when present:

- Add `site_build_is_clean`, which walks `site/_build` directly (it is gitignored), scans text files with the same rules, text extension list, cast decoder, and upstream allowlist, and skips binary assets. Map `_build` copies to their source paths for the allowlist (for example `_build/index.html` -> `site/index.html`), or apply fragment-level exemptions to HTML too. For single-line JSON such as `site/_build/docs/search.json`, apply the allowlist to individual string values, not whole lines, so one allowed upstream phrase cannot mask the rest of the line.
- Add `just site-check` with this body:
  ```
  site-check:
      cd site && ./build.sh
      MAKIMA_SITE_CHECK=1 cargo test -p makima site_build_is_clean
  ```
- The test fails if `site/_build` is absent when `MAKIMA_SITE_CHECK=1` is set, and returns early otherwise so a plain `cargo test` does not require a site build.

Allowlist:

- Do not allowlist general substrings like `top of maki`; they mask live `maki.fn` references (for example `plugins/lib/maki/scroll.lua` and the generated lua-api page). The allowlist is a `contains` check on the specific upstream-attribution fragments listed below, scoped to the named files/lines. Add a separate global assertion that the token matcher finds no `lun-4/maki` (the stale fork origin); `lun-4/makima` is the current origin and is excluded by the following-character check. Do not implement this as a raw `contains("lun-4/maki")`, which would also match `lun-4/makima`.
- Allow only these upstream-attribution fragments, scoped to `README.md:3,7,9,166` and `site/index.html:830,977,1221`:
  - `a fucked up fork of [maki](https://github.com/tontinton/maki):`
  - `_maki is good!`
  - `## things makima does on top of maki`
  - `> DISCLAIMER: >90% of code in maki was written by maki,`
  - `a fork of <a href="https://github.com/tontinton/maki"`
  - `Everything upstream maki does,`
  - `makima is a fork of <a href="https://github.com/tontinton/maki"`
- For `.agents/skills/lunamaki-sync-with-upstream/SKILL.md`, allow only the upstream lines (`Sync the lunamaki fork with upstream maki`, `# Syncing makima with upstream maki`, `a fork of maki`, `` `upstream` is `tontinton/maki` ``). Update every other line in that file.
- Add a short, reviewed negative-fixture exception for the AC.4 Lua chunk in `maki-lua/tests/runtime_alignment.rs` (`assert(maki == nil, ...)` and its error message). If the test is placed in `in_memory_host.rs` instead, move the exception with it. Keep it to those exact lines; do not generalize it to `_maki_` or `maki` anywhere else.
- Report `path:line: snippet` for every finding and assert the list is empty.

Then run the full verification:

- `just check`
- `just lint`
- `just test`
- `just ci` (fmt-check, lint, pylint, test, gen-docs-check, machete)
- `just gen-docs-check`
- `just bridge-check`
- `cargo machete`
- `nix flake check`
- `nix build` and `./result/bin/makima --version`
- `cargo run -- --version` and `cargo run -- --help`
- `just site-check` (builds `site/_build` and runs `site_build_is_clean`)
- Manual TUI check: prompt, assistant prefix, splash, skill picker
- Manual banner and demo-cast visual check

### Phase 8: Review

Dispatch a `general` subagent to review the completed work for missed identity tokens, test quality, documentation consistency, and commit boundaries. Focus areas: the `RoleNames` seam, generated docs, CI path filters, the residual-test allowlist, and whether each checkpoint commit is atomic and its body explains the change. Fix or explicitly rebut every finding; repeat until no critical findings remain.

## Acceptance Criteria

**AC.1** No stale `maki` identity tokens remain in tracked text outside the explicit upstream allowlist, historical documents, the retained `pages.yml` upstream guards, and the guard's own matcher fixtures. Verified by `no_stale_identity_tokens` and `identity_matcher_cases`.

**AC.2** Each renamed crate compiles and lints; the full workspace suite passes before handoff. Verified during implementation by scoped `cargo check -p 'makima-*' --tests`, `just lint`, and scoped `cargo nextest run -p 'makima-*'`; the full suite is AC.10's `just test`. Quote the glob so the shell does not expand it into sixteen positional arguments.

**AC.3** The Cargo package is `makima`, the CLI is named `makima`, and the Nix build exposes a `makima` binary. Verified by `package_name_is_makima`, `cli_name_is_makima`, and `nix build` + `./result/bin/makima --version`.

**AC.4** The Lua global is `makima`; `maki` is absent; bundled plugins use `makima.*` and `require("makima.*")`. Verified by `makima_global_installed_and_legacy_global_absent` (a Lua chunk loaded through `PluginHost::load_source` that asserts `makima ~= nil` and `maki == nil`) and the updated plugin specs (`lib_spec`, `providers_*_spec`, and the rest of `makima-lua/tests/spec.rs`).

**AC.5** Environment, log, user-agent, and provider-referer identity use `makima`; legacy `maki` log names are not treated as current. Verified by `log_env_is_the_app_name_upper`, `log_file_names_use_app_name`, `rotated_index_rejects_legacy_names`, `fallback_user_agent_uses_app_name`, and the updated `requesty` provider spec.

**AC.6** UI role prefixes are parameterized; tests use synthetic names and never assert the product name; the default assistant prefix derives from `APP_NAME`. Verified by `default_assistant_prefix_comes_from_app_name`, `assistant_prefix_is_configurable`, `search_text_includes_role_prefix`, `search_role_prefix_matches`, `thinking_indicator_uses_configured_prefix`, and `ui_tests_do_not_hardcode_product_prefix`.

**AC.7** The Claude Agent bridge package and MCP server name are `makima`. Verified by `server_name_is_makima` and `just bridge-check`.

**AC.8** Generated documentation and anchors use `makima`, and generated files are current. Verified by `lua_api_page_uses_makima_names` and `just gen-docs-check`.

**AC.9** The site builds without stale identity tokens, and the demo cast, banner, icons, and doom videos show `makima` or are explicitly deferred to the operator. Verified by `site_build_is_clean` (after `just site-check`), `cast_text_is_clean`, and a human visual check of the banner, icons, demo, and doom videos.

**AC.10** The full local CI and Nix checks pass. Verified by `just ci`, `nix flake check`, and `nix build`.

**AC.11** The work is on branch `rename/makima`, and `git log --oneline --reverse mistress..rename/makima` shows one commit per non-empty checkpoint in the table (or per non-empty 5a/5b/5c and 9a/9b sub-checkpoint when parallel execution is used), each scoped to its phase; the final commit is CI-green and the worktree is clean. Verified by `git branch --show-current`, `git log --oneline --reverse mistress..rename/makima`, `git show --stat <sha>` for each checkpoint, and `git status --short`.

## Test Strategy

| Criterion | Test or check | Layer |
| --- | --- | --- |
| AC.1 | `no_stale_identity_tokens` + `identity_matcher_cases` + `cast_text_is_clean` + `site_build_is_clean`; includes the tracked-path check | Repository integration |
| AC.2 | Scoped `cargo check -p 'makima-*' --tests` + `just lint`; a runnable build/lint gate, not a unit test; full suite in AC.10 | Workspace |
| AC.3 | `package_name_is_makima` (`env!("CARGO_PKG_NAME")`), `cli_name_is_makima` (`Cli::command().get_name()`), `nix build` + binary version | Unit + build |
| AC.4 | `makima_global_installed_and_legacy_global_absent` (Lua chunk via `PluginHost::load_source`), `lib_spec`, `providers_*_spec` | Lua host integration |
| AC.5 | `log_env_is_the_app_name_upper`, `log_file_names_use_app_name`, `rotated_index_rejects_legacy_names`, `fallback_user_agent_uses_app_name`, `requesty` spec | Unit + Lua spec |
| AC.6 | `default_assistant_prefix_comes_from_app_name`, `assistant_prefix_is_configurable`, `search_text_includes_role_prefix`, `search_role_prefix_matches`, `thinking_indicator_uses_configured_prefix`, `ui_tests_do_not_hardcode_product_prefix` | UI unit/render |
| AC.7 | `server_name_is_makima`, `just bridge-check` | TypeScript integration |
| AC.8 | `lua_api_page_uses_makima_names`, `just gen-docs-check` | Docgen unit + generated output |
| AC.9 | `site_build_is_clean` after `just site-check`; `cast_text_is_clean`; human visual check for `banner.png`, icons, demo, and doom videos | Build + manual |
| AC.10 | `just ci`, `nix flake check`, `nix build` | Full CI |
| AC.11 | `git branch --show-current`, `git log --oneline --reverse mistress..rename/makima`, `git show --stat <sha>` per checkpoint or sub-checkpoint, `git status --short` | Git process check |

Test-layer notes:

- Unit tests cover env/log/UA constants, role names, docgen, and the residual-token matcher.
- Integration tests cover the Lua host, plugin specs, the CLI, and the tracked-file scan.
- Render tests cover the messages panel, search modal, and collapsed thinking indicator.
- Full-stack checks cover the TypeScript bridge, Nix build, and site build.
- The asciicast decoder and the `site/_build` scanner are new test infrastructure built as part of this plan; they are not external dependencies.
- Manual checks cover the banner, icons, demo cast, and doom videos. There is no OCR or video/terminal-recording harness in the repository; flag these as manual rather than pretending they are automated.

## Review Strategy

Before submission, run a `plan_reviewer` pass against this file and fix or rebut every finding. After implementation and automated checks, dispatch a `general` subagent to review the work. Review commit by commit first (`git log --reverse --stat mistress..rename/makima`, then `git show <sha>` for each checkpoint) and flag any commit that mixes phases, omits its 2-3 paragraph body, or stages unrelated files. Then review the cumulative diff. Focus the review on missed `maki` identity tokens, test quality (especially the role-prefix tests and the residual guard), generated-doc consistency, and CI/Nix coverage. Fix or explicitly rebut all findings; if critical findings remain, run another review pass.

## Documentation Strategy

Repository-facing:

- Update `AGENTS.md` crate and Lua API descriptions, the nested `maki-lua/src/api/AGENTS.md` (moving to `makima-lua/...`), and `docs/agent-system.md`/`docs/agent-invariants.md`.
- Update `CONTRIBUTING.md` and the README install instructions.
- Keep upstream attribution intact and explain that `tontinton/maki` is the upstream project, not this fork's identity.

User-facing:

- Update `site/index.html` and every hand-written site page.
- Regenerate the eight docgen pages.
- Re-record the demo and replace the banner.
- There is no changelog or migration guide in the repository. Because backward compatibility is explicitly not a constraint, record the breaking Lua/env/log changes in the relevant checkpoint commit bodies rather than adding a migration document. The commit sequence is the reviewable record of the change.

## Risks, Blockers, and Required Decisions

- **Manual assets:** `site/demo.cast` requires an interactive re-record; `banner.png`, the favicon/apple-touch/android icons, and `doom*.mp4` require visual replacement or review. The asciicast decoder verifies the cast's text, but a human must confirm the animation. There is no OCR or video harness for the banner, icons, or videos. If the execution environment cannot record a terminal, the operator must record or approve leaving the cast out of scope; the same operator-provided-or-deferred rule applies to the banner, icons, and doom videos.
- **Replacement hazards:** a blind `s/maki/makima/g` corrupts `makima` into `makimama`, `making` into `makimang`, and upstream `tontinton/maki` links. Use token-boundary rules and the explicit allowlist; review the diff rather than trusting the script.
- **Generated docs and anchors:** the lua-api anchors and cross-links change. Regenerate with `just gen-docs` and grep the built site; a stale anchor is a silent 404.
- **Nix derivation name:** the package name now comes from `Cargo.toml`, so the derivation changes from `maki-*` to `makima-*`. The local `result` symlink and any external cache keys may change. `flake.nix` itself needs no literal edit.
- **Lockfile and installs:** `Cargo.lock` must be regenerated and committed. The README and quick-start must stop telling users to install the package as `maki`; `cargo install --locked --git ... makima` must work.
- **Breaking Lua API:** user `init.lua` files, plugins, and scripts using `maki.*` or `require("maki.*")` break by design. The bundled plugins and tests must be fully migrated in the same change.
- **Role-prefix refactor:** changing `RoleStyle.prefix` to `Arc<str>` and threading `RoleNames` through message rendering touches a hot UI path. The prefix is already materialized per render, so do not chase an allocation-free claim; keep `RoleNames` shared as `Arc`s and run the full `makima-ui` suite.
- **Residual-test brittleness:** the allowlist is a short, scoped list of upstream-attribution fragments plus the AC.4 negative fixture; it is not a general substring or `_maki_` allowlist. If a legitimate future upstream reference is added, the test will fail until the allowlist is extended; that is the desired tradeoff for a fork identity guard. The path check, asciicast decoder, guard-file skip, and `pages.yml` exception are part of the guard, not optional extras.
- **Historical documents:** `.agents/makima-plans/` and `acp-command-effect-projection-handover.md` are left untouched. If the operator wants them rewritten too, the scope grows by roughly 2k tokens and the diff becomes much noisier.
- **Site-check dependency:** `just site-check` runs `site/build.sh`, which downloads Zola if it is not on `PATH`. In an offline environment, install Zola first or run only the non-site checks and flag AC.9 as blocked.
- **Commit granularity:** checkpoint 4 is intentionally red because the core Lua global changes before the bundled plugins; checkpoint 5 makes the Lua suite green. Only checkpoint 13 must pass the full checks. Generated files (`Cargo.lock`, generated docs) belong with their phase, not a later catch-all commit.
- **Branch base:** branch from the current `mistress` HEAD. If `mistress` moves during the work, do not rebase or force push unless the operator asks; nothing is pushed by default.
- **Parallel integration:** parallel mode adds worktree and branch management, merge-conflict resolution, and agent context drift. Cap it at 3-4 concurrent agents, keep the ownership table authoritative, and remember that the linear checkpoint sequence remains the default. The crate rename and generated docs are not parallel work.
- **Test-suite duration:** `just test` is a full workspace run; run the scoped crate tests while iterating (`cargo nextest run -p makima-lua`, `-p makima-ui`, `-p makima-storage`, `-p makima`) and the full suite before handoff.
