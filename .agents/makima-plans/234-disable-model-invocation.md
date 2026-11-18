# Support `disable-model-invocation` in skills

## Goal

Skills whose `SKILL.md` frontmatter sets `disable-model-invocation` to a true value must disappear from the skill list the model sees, while still being loadable by name through `@skill:<name>` and the `skill` tool.

## Implementation Summary

The model discovers skills through the `skill` tool's description, which embeds the output of `build_skill_list` (`plugins/skill/skill_helpers.lua`). The plugin also exposes a completion source and an expander (`plugins/skill/init.lua`) that resolve skills by name from the same `discover_skills` map, and the tool handler looks the name up in that map.

So the feature reduces to one derived boolean on each discovered skill plus a filter in the description builder:

- `plugins/skill/skill_helpers.lua`: a new `parse_bool(value)` helper normalizes boolean frontmatter values, and `build_skill_list` skips entries marked `disable_model_invocation`, including in the "No skills available." branch.
- `plugins/skill/init.lua`: `scan_skill_dir` stores `disable_model_invocation = parse_bool(fm["disable-model-invocation"])` on each skill entry. The entry stays in the map, so completion, the `@skill:` expander, and the tool handler still find it. The builtin `makima-plugin-dev` skill stays visible.
- `plugins/skill/tests/spec.lua`: unit cases for value normalization and the list filter.
- `makima-lua/tests/skill_visibility.rs` (new): end-to-end cases that boot the real plugin on an in-memory FS and check the description the model receives, completion, expansion, name lookup, and the error listing.
- `site/docs/content/skills/_index.md`: document the flag, its accepted spellings, and the `@skill:` workflow, and fix the now-inaccurate "lists every skill it found" sentence.

Non-goals: `metadata.opencode/autoinvoke`, any other invocation policy, and marking hidden skills differently in the `@` popup (they must remain visible and selectable for the user). The flag is top-level frontmatter only, matching the issue.

## Implementation Plan

### Phase 1: Normalize and apply the flag

In `plugins/skill/skill_helpers.lua`:

- Add a module const near the top, e.g. `local TRUTHY = { ["true"] = true, ["yes"] = true, ["on"] = true, ["1"] = true }`.
- Add `M.parse_bool(value) -> boolean`: returns true for boolean `true` and number `1`, and for strings whose lowercased value is in `TRUTHY`; false for everything else (including `nil`, `false`, `0`, `"no"`, `"off"`, `"false"`). Keep it generic rather than naming the flag, so other boolean frontmatter fields can use it later. This matches the Claude Code convention that boolean frontmatter fields accept `yes`, `no`, `on`, `off`, `1`, and `0` in any letter case. YAML 1.2 gives `yes` and `on` to us as strings, so the normalization must happen here rather than relying on `makima.yaml.decode`.
- `build_skill_list(skills)`: filter out entries where `s.disable_model_invocation` is truthy before deciding the empty case, sort the survivors alphabetically, and keep rendering one line each. A set of only hidden skills renders the existing `No skills available.` block.

In `plugins/skill/init.lua`, `scan_skill_dir`:

- Import `parse_bool` from the helpers module next to `parse_frontmatter` and `build_skill_list`.
- Add `disable_model_invocation = parse_bool(fm["disable-model-invocation"])` to the entry built for each skill directory. `fm` is always a table (`parse_frontmatter` returns `{}` on missing or invalid frontmatter), so no nil guard is needed.
- Do not filter at discovery time: the map is shared by the description, the handler, the completion source, and the expander.
- The builtin entry in `discover_skills` gets no flag and stays visible.

The handler's not-found path already calls `build_skill_list(skills)`, so hidden skills also stay out of that error listing. This is deliberate: the model should learn about the skill only from an explicit `<skill:name>` token in the user message, not from an error.

### Phase 2: Unit tests for normalization and the filter

Add to `plugins/skill/tests/spec.lua`:

- Under the existing `parse_frontmatter`/`build_skill_list` sections, a `parse_bool_spellings` case looping over `true`, `1`, `"true"`, `"YES"`, `"on"`, `"1"` → true, and `false`, `nil`, `0`, `"false"`, `"no"`, `"off"`, `"0"`, `""` → false.
- `build_skill_list_skips_model_disabled_skill`: two entries, one with `disable_model_invocation = true`; assert the visible name and description appear and the hidden name does not.
- `build_skill_list_only_model_disabled_skills`: single hidden entry; assert the text contains `No skills available` and not the hidden name.

### Phase 3: End-to-end test with the real plugin

Create `makima-lua/tests/skill_visibility.rs`. Seed the in-memory FS first, then boot the plugin (the tool description is computed once at load, so seeding after boot would make AC.2 fail for the wrong reason). Bind both handles: `let (handle, guard) = makima_lua::test_support::spawn_host_with_fs_for_tests(&["skill"], Arc::clone(&fs), None);`.

Seed three skills under `std::env::current_dir().join(".agents/skills/...")`, mirroring `project_skill_from_cwd_is_offered_and_expands` in `makima-lua/tests/completion_plugins.rs`:

- `visible-skill/SKILL.md`: `name: visible-skill`, a description, and a body marker.
- `hidden-skill/SKILL.md`: same plus `disable-model-invocation: true`.
- `spelling-skill/SKILL.md`: same plus `disable-model-invocation: "yes"`, covering the Claude Code spelling end to end.

Use `guard.host().registry()` for the `ToolRegistry`, `handle` for completion/expansion, and `makima_agent::tools::test_support::stub_ctx(&AgentMode::Build)` with `ctx.registry` set to the host registry for invoking the tool through `entry.tool.parse(...)` and `inv.execute(&ctx)` (same shape as `run_tool` in `makima-lua/tests/code_execution_policy.rs`).

To read the description the model receives, build definitions like `tool_description` in `makima-lua/tests/plugin_host.rs`: `ToolFilter::from_config(&AgentConfig::default(), &Model::from_spec(...), &[])`, a `DescriptionContext { audience: ToolAudience::MAIN, workflow: false, mcp: false, .. }`, then `registry.definitions(&Vars::new(), &ctx, false)` and select the `skill` entry. Keep this helper local to the new file, matching `plugin_host.rs`; do not refactor the existing test module for this change.

Tests:

- `description_lists_visible_skill_only`: description contains `visible-skill` and its description; contains neither `hidden-skill` nor `spelling-skill`.
- `completion_and_expansion_keep_disabled_skill`: `collect_completion_items` contains `skill:hidden-skill`; `expand_references("@skill:hidden-skill")` returns `<skill:hidden-skill>`. This guards the "filter only in the description" decision; it passes on today's plugin by design.
- `skill_tool_loads_disabled_skill_by_name`: invoking the tool with `{"name": "hidden-skill"}` succeeds and the output contains the body marker. Same non-regression role.
- `unknown_skill_error_hides_disabled_skills`: invoking with `{"name": "nope"}` returns an error whose text contains `skill not found: nope`, `visible-skill`, and `<available_skills>`, and does not contain `hidden-skill` or `spelling-skill`. The positive assertions keep the test from passing if the listing itself breaks.

### Phase 4: Docs

In `site/docs/content/skills/_index.md`:

- Add a short section after "Writing one": what the flag does (skill disappears from the list the model reads, so it costs no tokens and the model never picks it on its own), how to run it (type `@skill:<name>`; completion still offers it), a frontmatter fragment, the accepted true spellings (`true`, `yes`, `on`, `1`, in any case), and that it must be a top-level key. Show the fragment with `disable-model-invocation: true`.
- Amend the existing line "The `skill` tool lists every skill it found" to say it lists every skill that has not opted out.
- Leave `site/docs/content/context/_index.md` alone: its token-economy explanation stays true, since hidden skills cost nothing at all.

Do not change generated pages: `makima-docgen` reads the first paragraph of the static tool description (`Load a skill that provides...`) and does not include discovered skills.

### Phase 5: Verification

- `cargo nextest run -p makima-lua` (runs `skill_plugin_spec` and the new integration file).
- Run `just test` once at the end: the plugin file is bundled into other suites and `makima-docgen` loads plugins, so the workspace run catches anything outside `makima-lua`.
- `just check`, `just lint`, `just fmt-check` (includes `stylua --check plugins/`), `just gen-docs-check`.
- Bound the CPU for every build and test command to 600% (six cores) so a heavy workspace build cannot starve the machine: run them as `systemd-run --user --scope -p CPUQuota=600% -- <command>` (for example `systemd-run --user --scope -p CPUQuota=600% -- just test`). If `systemd-run` is unavailable, say so and use the closest equivalent (`taskset` on six cores) in the same commands.

## Acceptance Criteria

- AC.1: `parse_bool` accepts `true`, `1`, `"true"`, `"yes"`, `"on"`, `"1"` in any case and rejects `false`, `nil`, `0`, `"false"`, `"no"`, `"off"`, `"0"`, and `""`. Verified by `parse_bool_spellings`.
- AC.2: `build_skill_list` omits entries with `disable_model_invocation = true` and reports `No skills available` when all entries are hidden. Verified by the two `build_skill_list_*` unit cases.
- AC.3: The `skill` tool description given to the main audience contains visible skill names and descriptions and contains neither a `true` nor a `"yes"` hidden skill's name. Verified by `description_lists_visible_skill_only`.
- AC.4: Hidden skills remain in `@` completion and expand from `@skill:<name>` to `<skill:name>`. Verified by `completion_and_expansion_keep_disabled_skill` (non-regression guard).
- AC.5: The `skill` tool loads a hidden skill when called by name and returns its body. Verified by `skill_tool_loads_disabled_skill_by_name` (non-regression guard).
- AC.6: The unknown-skill error lists visible skills and `<available_skills>` but not hidden skills. Verified by `unknown_skill_error_hides_disabled_skills`.
- AC.7: `site/docs/content/skills/_index.md` documents the flag, the accepted spellings, the `@skill:` workflow, and the corrected "every skill it found" sentence. Verified by a human reading the rendered section and `grep -n "disable-model-invocation" site/docs/content/skills/_index.md`.
- AC.8: `cargo nextest run -p makima-lua`, `just test`, `just check`, `just lint`, `just fmt-check`, and `just gen-docs-check` all pass, each run under the 600% CPU cap described in Phase 5.

## Test Strategy

| Criterion | Test |
| --- | --- |
| AC.1, AC.2 | `plugins/skill/tests/spec.lua`: `parse_bool_spellings`, `build_skill_list_skips_model_disabled_skill`, `build_skill_list_only_model_disabled_skills` (loaded by `skill_plugin_spec` in `makima-lua/tests/spec.rs`) |
| AC.3 | `makima-lua/tests/skill_visibility.rs::description_lists_visible_skill_only` |
| AC.4 | `makima-lua/tests/skill_visibility.rs::completion_and_expansion_keep_disabled_skill` |
| AC.5 | `makima-lua/tests/skill_visibility.rs::skill_tool_loads_disabled_skill_by_name` |
| AC.6 | `makima-lua/tests/skill_visibility.rs::unknown_skill_error_hides_disabled_skills` |
| AC.7 | Human review of the docs section; grep for the key |
| AC.8 | The named commands above, each wrapped in `systemd-run --user --scope -p CPUQuota=600% -- ...` |

Unit tests cover the pure normalization and filter. The integration test is required because the flag is applied in `init.lua` (not reachable from the standalone Lua spec) and because the contract is what the model actually receives as a tool description. The in-memory FS host harness already exists (`makima_lua::test_support::spawn_host_with_fs_for_tests`), so no new infrastructure is needed. The tests must stay hermetic: seed only at `current_dir()`, assert positives on the seeded names, and never depend on skills on the developer's real disk (the in-memory backend hides global/config skills).

Genuine new-behavior coverage is AC.1, AC.2, AC.3, and AC.6. AC.4 and AC.5 pin the deliberate choice to keep hidden skills in the shared lookup map; they pass on today's plugin and would fail if a future refactor filtered discovery.

## Review Strategy

Before handoff, run a `plan_reviewer` pass on this file and fix or rebut every finding; repeat if any critical or high finding appears.

After implementation, review the diff for: flag parsing only in `scan_skill_dir`, no filtering of the shared skills map, no provider or architecture-test implications, docs tone, and the amended sentence on the skills page. Run a review pass with a `general` subagent if one is available, then fix or rebut findings before reporting completion.

## Documentation Strategy

`site/docs/content/skills/_index.md` is the canonical user doc for skills and gets the new subsection plus the correction. No `AGENTS.md` change is needed because the plugin architecture is unchanged. No generated docs change.

## Risks, Blockers, and Required Decisions

- The tool description is built once at plugin load, so a newly edited `disable-model-invocation` value takes effect on the next start, like other skill discovery today. Acceptable and consistent; do not build a refresh path for this change.
- Hiding skills from the not-found error listing is a deliberate reading of "removes the skill from the system prompt / tool description". Mention it in the PR description so a maintainer can weigh in; if they want the error to show them, only the call site changes, not the flag.
- The truthiness spellings are wider than YAML's `true`, matching Claude Code. Documented in the docs section and pinned by the unit case, so a future refactor cannot silently narrow them.
- No blockers requiring operator decisions.
