# Remove Executable Providers (#233)

## Goal

Delete the executable-provider feature: a script in `<config dir>/providers/` no longer registers a provider. Remove its adapter, tests, example script, and every doc/tooling reference, while keeping custom `providers.toml` providers unchanged.

## Implementation Summary

Issue #233 asks to remove executable providers now that providers live in `plugins/providers/`. The feature is self-contained in `plugins/providers/providers/executable.lua` and its registration block in `plugins/providers/init.lua:92-104`. Nothing in Rust scans the config `providers/` directory; `makima-config` only handles `providers.toml`.

What stays:

- Custom providers from `providers.toml` sections with `protocol` (`plugins/providers/providers/custom.lua`), the supported first-party replacement.
- `providers.routes.ROUTES` (`plugins/providers/providers/routes.lua`), still used by `custom.lua` and `aperture.lua`.
- `ui:run` / `InteractionRequest::Terminal`, still used by `claude_agent.lua`'s login.
- `makima.fn.job*` and `makima.fn.executable`, general Neovim-style plugin APIs.

What is dead after removal and goes too:

- `aperture.delegated()` (`plugins/providers/providers/aperture.lua:292-313`) and `opencode.delegated()` (`plugins/providers/providers/opencode.lua:124-144`). `delegated()` is a route factory: its returned route hands an executable script's traffic to another provider implementation, using the script-supplied base URL and headers. `aperture.delegated()` routes through Aperture's gateway logic, and `opencode.delegated(cache)` routes `<catalog provider>/<model>` ids through that catalog provider and everything else through Zen. They existed so a script could set `base = "aperture"` or `base = "opencode"` instead of naming a plain wire protocol. With scripts gone, nothing can name those bases.
- This does **not** remove the built-in routing. `opencode/<catalog provider>/<model>` is implemented by `M.provider`'s `prepare`/`delegate` (`opencode.lua:106-122,178-183`) and keeps working. Aperture keeps its upstream routing through `M.provider` → `M.instance`/`M.describe` (`aperture.lua:234-257,283-287`). Only the script-facing route factories are deleted.
- `scripts/providers/openai-oauth`, an executable-provider example now redundant with the built-in `openai` ChatGPT login (`plugins/providers/providers/openai.lua`, `openai-codex` alias).
- The tbench `providers/` mount and install copy, which only supplied executable scripts.

Scope boundaries: do not remove any built-in provider, the models.dev catalog, the Claude Agent bridge/provider, or custom `providers.toml` support. No migration shim or fallback warning; this is an accepted breaking removal per the issue.

Per the operator, the final diff adds no durable regression tests. A throwaway check is allowed while implementing and must be removed before handoff; final code additions should be minimal.

## Implementation Plan

### Phase 1: Temporary verification, then remove runtime support

0. Before deleting, set up a throwaway check (not committed) that a script in a temp config `providers/` directory registers a provider. A temporary test file or a short probe using the real plugin host is fine. Keep it until after the deletion to confirm the provider no longer registers, then delete the check in Phase 5.
1. Delete `plugins/providers/providers/executable.lua`.
2. Delete `plugins/providers/tests/executable.lua`.
3. Remove the `providers_executable` entry from `makima-lua/tests/spec.rs:28`.
4. Edit `plugins/providers/init.lua`:
   - Remove `local executable = require("providers.executable")` (line 5).
   - Remove `local routes = require("providers.routes")` (line 9); no longer used in this file.
   - Replace lines 67-71 (`bases`, `aperture.delegated`, `opencode.delegated`, `executable.notes`) with `register(custom.creator(catalog.NOTES))`.
   - Remove the whole config-dir scan block, lines 92-104.
   - Update the comment at lines 73-75 to say "Sections with a protocol each win over..." without "then scripts".
5. Delete `aperture.delegated()` at `plugins/providers/providers/aperture.lua:292-313`.
6. Delete `opencode.delegated()` at `plugins/providers/providers/opencode.lua:124-144`.
7. Update `plugins/providers/providers/routes.lua:1-3` so the header says routes serve credentials from elsewhere such as the Aperture gateway or a custom `providers.toml` section, not an executable script.
8. Edit `plugins/providers/providers/catalog.lua:28`: "A built-in provider, a `providers.toml` section or a script with the same id wins." becomes "A built-in provider or a `providers.toml` section with the same id wins."

### Phase 2: Remove the example and tooling

1. Delete `scripts/providers/openai-oauth` (the directory becomes empty and disappears from git).
2. In `scripts/tbench_makima_agent.py`:
   - Remove `"~/.config/makima/providers:/mnt/makima-providers:ro"` from the `MOUNTS` example in the module docstring.
   - Remove the `exec_as_root` call that copies `/mnt/makima-providers` into `/root/.config/makima/providers` (around line 149).

### Phase 3: Update hand-written docs

1. `makima-docgen/src/gen_config.rs:431`: remove `providers/` (executable providers) from the "Config holds ..." sentence, leaving `providers.toml` and `commands/`.
2. `makima-lua/src/login_ui.rs:133`: change the `ui:run` example from the provider script path to a neutral command, e.g. `ui:run({ "gh", "auth", "login" })`. Keep the function and the `Terminal` interaction: `claude_agent.lua` uses them.
3. `README.md:76`: delete the "**Dynamic providers**" bullet.
4. `AGENTS.md:64`: change "custom and executable providers from `providers.toml` and `providers/`" to "custom providers from `providers.toml`".
5. `site/docs/content/cli/_index.md:102`: delete the sentence about an executable provider's login running on the terminal; keep "Pick **Custom provider...** to add an endpoint of your own."
6. `site/docs/content/cli/_index.md:117`: change "Built-in and script providers are listed live" to "Built-in and custom providers are listed live" (same file, different section).
7. `site/index.html:1217`: replace "Write a short provider script ... OpenAI oauth example included" with custom-provider wording that links to `/docs/providers/#providers-toml`; the existing GitHub `scripts/providers` link becomes dead after Phase 2.
8. `claude-agent-bridge-design.md:82`: replace "as executable providers start their scripts" with "as the managed bridge will do on first use once the protocol is stable" (line 202 says the managed bridge does not exist yet).
9. `makima-storage/src/paths.rs:206-208`: the doc comment on `paths::init()` uses "a provider script cache" as its example of a transitive directory read. Replace it with a still-true example, such as the models.dev catalog cache.
10. Leave historical files under `.agents/makima-plans/` and `site/demo.cast` alone; they describe past state or are a recording.

### Phase 4: Regenerate generated docs

Run `just gen-docs` from the repo root. Expected changed files:

- `site/docs/content/providers/_index.md`: the `#### Executable providers` section disappears and the models.dev note no longer mentions scripts.
- `site/docs/content/configuration/_index.md`: the directory-layout sentence no longer lists `providers/`.
- `site/docs/content/lua-api/_index.md`: the `ui:run` example is updated.

If unrelated pages change, fix the source and regenerate; never hand-edit generated pages, or `just gen-docs-check` will fail. The generated pages must match sources exactly.

### Phase 5: Verify, clean up, and remove the throwaway check

1. Run the temporary check from Phase 1 step 0 and confirm the config-dir script no longer registers. Delete the check; `git status` must show no new test file.
2. Run the reference scan below and confirm it returns nothing outside `.agents/makima-plans/` and `site/demo.cast`:
   ```sh
   rg -niE 'executable providers?|provider scripts?|script providers?|script with the same id|provider-scripts|executable file in the config|scripts/providers|/mnt/makima-providers|makima/providers/' \
     plugins/providers makima-docgen/src makima-lua/src makima-storage/src site/docs/content scripts \
     site/index.html README.md AGENTS.md CONTRIBUTING.md claude-agent-bridge-design.md
   ```
   Also confirm the deleted paths are absent: `plugins/providers/providers/executable.lua`, `plugins/providers/tests/executable.lua`, `scripts/providers/openai-oauth`.
3. `just fmt` (Rust + stylua).
4. `just gen-docs-check` (after `just gen-docs`).
5. `.ssh/remote-ci.sh` (runs `just ci`: fmt-check, lint, pylint, nextest, gen-docs-check, machete).
6. Optionally `just bridge-check`; the bridge is unchanged, so it should stay green.
7. Inspect `git diff --stat`: it should be deletions plus a handful of small doc edits, with no added tests.

## Acceptance Criteria

- **AC.1** An executable file placed in the config `providers/` directory does not appear in the provider registry. Verified during implementation with the throwaway check from Phase 1, run before and after deletion, then removed: the script registers before and is absent after.
- **AC.2** The generated Providers page no longer documents executable providers or offers scripts as a provider source, and the custom provider section remains. Verified by `just gen-docs-check` and an empty search for `Executable providers`, `script with the same id`, and `provider script` in `site/docs/content/providers/_index.md`.
- **AC.3** The generated Configuration page no longer lists `providers/` as a config entry. Verified by `just gen-docs-check` and an empty search for the backticked `` `providers/` `` token in `site/docs/content/configuration/_index.md`.
- **AC.4** The generated Lua API `ui:run` example does not point at a config provider script. Verified by `just gen-docs-check` and an empty search for `.config/makima/providers` in `site/docs/content/lua-api/_index.md`.
- **AC.5** No source, generated doc, hand-written doc, landing page, or tooling file references executable provider scripts, and the removed files are absent. Verified by the Phase 5 `rg` scan and `test ! -e` checks.
- **AC.6** The workspace still formats, lints, tests, and has current generated docs, and the final diff adds no tests. Verified by `just ci` exiting 0 and `git diff --stat` showing only deletions and small doc edits.

## Test Strategy

Per operator instruction, this plan adds no durable tests. The behavior is a removal, verified now with a throwaway check that is deleted before handoff, plus existing suites:

| Criterion | Verification | Layer |
|-----------|--------------|-------|
| AC.1 | Throwaway config-dir script check, red before deletion, green after, then deleted | Temporary integration check |
| AC.2 | `just gen-docs-check` + targeted `rg` on the Providers page | Generated output check |
| AC.3 | `just gen-docs-check` + targeted `rg` on the Configuration page | Generated output check |
| AC.4 | `just gen-docs-check` + targeted `rg` on the Lua API page | Generated output check |
| AC.5 | Phase 5 `rg` scan + `test ! -e` on deleted paths | Repository reference check |
| AC.6 | `.ssh/remote-ci.sh` (`just ci`) + `git diff --stat` | Full workspace CI |

The existing `providers_*` Lua specs (`makima-lua/tests/spec.rs`) and the Rust tests in `makima-lua/tests/provider_auth.rs` that compile the bundled `init.lua` (`bundled_providers_keep_auto_detect_order`, `malformed_provider_section_is_skipped_without_aborting_bundled_plugin`) still run in `just ci`; they prove the rewrite did not disturb built-in providers or the custom `providers.toml` flow.

Gap, accepted by the operator: with no committed test, a future reintroduction of config-dir scripts would not fail CI. The deletion is verified during this change only.

## Review Strategy

Plan-mode review with `plan_reviewer` after this file is written. Fix or rebut all findings; rerun if any critical or high findings remain.

After implementation, run a `general` subagent review of the diff, focusing on dangling references, dead code left behind, and whether any built-in provider or custom `providers.toml` behavior changed. Fix or rebut all findings. Then run `.ssh/remote-ci.sh` again until green.

## Documentation Strategy

All user-facing references are covered above: generated Providers/Configuration/Lua API pages, hand-written CLI reference, the site landing page, README, AGENTS.md, paths.rs rationale, and the bridge design note. No new documentation is needed for the removal itself: the surviving path is the already-documented `providers.toml` custom provider flow. Do not add a deprecation page; the feature is gone in one release, and there is no existing changelog to update.

## Risks, Blockers, and Required Decisions

- **Accepted breaking change.** Users with scripts in `~/.config/makima/providers/` lose those providers with no warning. The issue accepts this. `providers.toml` cannot replace script-driven credential resolution (short-lived tokens, custom refresh); such users must run a proxy instead. If a deprecation warning is desired, that is a separate decision.
- **No durable test guard.** The operator explicitly declined committed regression tests, so a future reintroduction would not fail CI. Accepted.
- **Stale cache.** `provider-scripts.json` may remain in users' state directories. It is inert; do not add cleanup code.
- **Internal modules removed.** `aperture.delegated` and `opencode.delegated` are bundled-plugin internals, not a public API. `grep` confirms no other callers. A user plugin deep-requiring `providers.aperture` could break, acceptable for internal modules.
- **Generated doc drift.** `just gen-docs` rewrites all generated pages. Investigate unrelated churn in its source and regenerate; never hand-edit a generated page. `gen-docs-check` gates correctness.

No unresolved operator decisions; the issue defines the scope.
