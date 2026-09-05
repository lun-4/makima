# Fix: `/thinking` default not applied to new sessions (GitHub issue #103)

## Goal

Make the persisted global thinking default (`Prefs.default_thinking`, pinned by `/thinking` / `maki.session.set_thinking({set_default=true})`) take effect on every session created without an explicit stored thinking level — including sessions created mid-run, which today silently fall back to `Off` and display as `minimal` effort on models that require thinking (the reporter's glm-5.3-flash symptom).

## Implementation Summary

**Corrected diagnosis (after review):** the preference is *not* write-only. The startup path already applies it: `src/cmd/tui.rs:353-364` reads `read_prefs(&storage).default_thinking.or(stack.config.always_thinking)` once, and the loop at `tui.rs:358-366` stamps `session.meta.thinking` on every message-empty tab before `maki_ui::run` (startup, picker, and `/reload` tabs alike, since the loop re-runs). So a plain quit/relaunch flow already inherits the pinned default.

The genuinely broken path is **mid-session new-session creation**:
- `SessionRequest::New` (`maki-ui/src/event_loop.rs:1534-1548`) — sessions-picker new tab (Ctrl+N), `maki.session.new` — builds `AppSession::new(...)` directly and spawns a runtime, bypassing the `tui.rs` stamping entirely.
- Any other future entry point that creates a session without passing through `tui.rs` has the same hole.

Both paths converge on `SessionState::from_session` (`maki-ui/src/app/session_state.rs:35-110`), the single resolution point for live state: `App::new` at `maki-ui/src/app/mod.rs:447` and `apply_loaded_session` at `maki-ui/src/app/session.rs:348-358`. Its thinking expression (lines 93-98) reads only `session.meta.thinking`, falling back to `ThinkingConfig::default()` = `Off` (`maki-providers/src/types.rs:547-554`); on requiring-thinking models `RequestOptions::clamped` (`maki-providers/src/types.rs:753-764`) raises `Off` to `Effort(Minimal)` — the exact "minimal" the reporter saw.

**Fix:** in `from_session`, when `session.meta.thinking` is `None`, fall back to `read_prefs(storage).default_thinking` before the default. `Option::or_else` keeps the file read off the hot path: persisted sessions always carry `Some(...)` (`build_meta` at `maki-ui/src/app/session.rs:116-135` mirrors `state.thinking` unconditionally), so the fallback fires only for genuinely new/unsaved sessions — matching the plugin's "default for new sessions" wording and the `Prefs` doc comment ("default thinking overlay", `maki-storage/src/sessions.rs:412-414`). The existing `model.supports_thinking()` filter applies to the pref fallback too, and the applied value is baked into `meta.thinking` by the first checkpoint.

**Keep `tui.rs` stamping untouched** (`src/cmd/tui.rs:353-366`): it is the only place `always_thinking` config is applied (from_session has no `AgentConfig`), and its now-redundant pref application is harmless (from_session's fallback only fires when `meta.thinking` is None, which the stamping prevents). Do not delete it.

## Implementation Plan

All code changes in `maki-ui/src/app/session_state.rs`, plus tests in the same file. No changes to `src/cmd/tui.rs`.

### Phase 1 — core fix

1. Extend the import at `session_state.rs:8`:
   `use maki_storage::sessions::{StoredEffect, StoredMode, StoredRule};` → add `Prefs, read_prefs`.
2. Replace the `thinking` expression at `session_state.rs:93-98` with:
   ```rust
   thinking: session
       .meta
       .thinking
       .or_else(|| read_prefs(storage).default_thinking)
       .map(Into::into)
       .filter(|_| model.supports_thinking())
       .unwrap_or_default(),
   ```

### Phase 2 — unit tests in the `#[cfg(test)]` module (`session_state.rs:209+`)

Test imports to add: `use maki_storage::sessions::{Effort, Prefs, write_prefs};` next to the existing `StoredThinking` import (`session_state.rs:214`) — `Effort` is needed for `StoredThinking::Effort { level: Effort::High }` and is the same type `maki_providers` re-exports — plus `ThinkingSupport` from maki_providers, and `write_prefs(...)` returns `Result`, so it needs `.unwrap()`. Reuse the existing tempdir pattern (`resumed()` helper, `session_state.rs:232-236`).

- `new_session_uses_pref_thinking_default`: tempdir `StateDir`, `write_prefs(&storage, &Prefs { default_thinking: Some(StoredThinking::Effort { level: Effort::High }) })`, then fresh `AppSession::new("test-model", "/tmp")` (has `meta.thinking == None`, see `Session::new`, `maki-storage/src/sessions.rs:1398-1423`) through `from_session` → assert `state.thinking == ThinkingConfig::Effort(Effort::High)`.
- `explicit_session_thinking_beats_pref`: `session.meta.thinking = Some(StoredThinking::Budget { tokens: 4096 })` + pref High → assert `ThinkingConfig::Budget(4096)`.
- `pref_thinking_ignored_when_model_unsupported`: model `Model { thinking_override: Some(ThinkingSupport::No), ..test_model() }` + pref High → assert `ThinkingConfig::Off`.
- `new_session_without_pref_stays_off`: empty tempdir (no `prefs.json`) → assert `ThinkingConfig::Off`.

Note: `test_model()` (`maki-ui/src/components/mod.rs:402`) supports thinking (anthropic manifest), so the positive cases exercise the fallback, not the clamp.

Only `new_session_uses_pref_thinking_default` fails pre-fix (no fallback → `Off`); the other three pass pre-fix and act as regression guards for the precedence/filter/default rules.

### Phase 3 — verification

- `just check` then `just lint`; run tests with `just test -p maki-ui`.
- Manual (primary — exercises the changed path): run `makima`, run `/thinking high` (plugin runs, writes `prefs.json`); then, **inside the same session**, create a new session via `maki.session.new` (or the sessions-picker new-tab action); the new tab's thinking badge must read `high`. Confirm `prefs.json` in the `StateDir` contains `default_thinking = {kind=effort, level=high}`.
- Manual (sanity — pre-existing behavior, must not regress): quit, relaunch fresh (no `--last`) → startup tab still shows `high` (this path was already handled by `tui.rs:353-366`; it validates we didn't break the stamping).

## Acceptance Criteria

- **AC.1** Fresh session (`meta.thinking` None) starts at the pref default, via the real `from_session` path. → `new_session_uses_pref_thinking_default`
- **AC.2** Explicit per-session stored thinking wins over the pref. → `explicit_session_thinking_beats_pref`
- **AC.3** Pref default is dropped when the model does not support thinking. → `pref_thinking_ignored_when_model_unsupported`
- **AC.4** With no pref, new sessions keep the previous default (`Off`). → `new_session_without_pref_stays_off`
- **AC.5** Observable: a session created mid-run (sessions-picker new tab / `maki.session.new`) inherits the pinned pref. Manual check (no TUI driver harness exists; see Test Strategy gap).
- **AC.6** Observable regression guard: the quit/relaunch fresh-session path (stamped by `tui.rs:353-366`) still inherits the pref. Manual sanity check.

## Test Strategy

Layer: pure state resolution → unit tests in `maki-ui/src/app/session_state.rs`; `from_session` is the exact function every runtime spawn (new and resumed, via `App::new` / `apply_loaded_session`) calls, so the tests validate the real path without a TUI harness. `write_prefs`/`read_prefs` round-trip is already covered in `maki-storage/src/sessions.rs:3630-3657`.

Existing tests that must keep passing and double as regression guards for the precedence rules: `from_session_applies_provider_adjust_model` (`session_state.rs:363-382`) and `thinking_restored_from_session_meta` (`maki-ui/src/app/tests.rs:5283-5295`).

Gap: there is no TUI/process-restart driver test infrastructure in the repo, so AC.5/AC.6 rely on the documented manual procedures. This is a pre-existing infra gap, not introduced by this change; the unit tests cover the state-resolution end of the flow.

| Criterion | Test |
| --- | --- |
| AC.1 | `new_session_uses_pref_thinking_default` |
| AC.2 | `explicit_session_thinking_beats_pref` |
| AC.3 | `pref_thinking_ignored_when_model_unsupported` |
| AC.4 | `new_session_without_pref_stays_off` |
| AC.5 | manual procedure (Phase 3, primary) |
| AC.6 | manual procedure (Phase 3, sanity) |

## Review Strategy

Plan-mode review via `plan_reviewer` before `plan_submit`; fix or rebut findings, re-run until no critical/high findings. After implementation and `just test`, dispatch a `general` subagent to review the diff (AGENTS.md has no separate review guidance); fix or rebut findings, repeat until no critical findings.

## Documentation Strategy

No doc changes needed. The behavior being restored is already documented: Lua API docs at `maki-lua/src/api/session.rs:230-239` (`set_default` "also persist as the default for new sessions") and `plugins/thinking/init.lua:1-4`. `always_thinking` config remains applied at startup only (see Risks) — the configuration table (`site/docs/content/configuration/_index.md:68`) says "Start every session", which stays accurate for startup sessions; decide separately whether to extend it mid-session.

## Risks, Blockers, and Required Decisions

- **Asymmetry (decision for luna):** after this fix, startup tabs get `pref.or(always_thinking)` (`tui.rs:353-355`) while mid-session new tabs get `pref` only (`always_thinking` config is invisible to `from_session` — no `AgentConfig` is plumbed there). Mid-session previously got neither, so this is a strict improvement; the residual gap is a follow-up (e.g. stamping `always_thinking` in `SessionRequest::New` like `tui.rs` does, or plumbing a default into `from_session`). Recommended: ship this fix as-is, track the asymmetry as a follow-up.
- **Do-not-touch guard:** `src/cmd/tui.rs:353-366` must not be "simplified away" — it is the only consumer of `stack.config.always_thinking` and covers the startup path; the `from_session` fallback is additive.
- **`read_prefs` cost:** one small file read, only on the `meta.thinking == None` path; negligible.
- **Resume semantics unchanged:** persisted sessions always carry `Some(...)` in `meta.thinking` (`build_meta` mirrors unconditionally), so resumed sessions keep their stored value; the pref applies only to sessions without an explicit level. `apply_loaded_session` (`app/session.rs:358`) also funnels through `from_session`; a meta-less loaded session (never checkpointed) would get the pref too — consistent with the "overlay" doc.
- **Manual-only verification gap:** AC.5/AC.6 have no automated test — the repo has no TUI/process-restart driver harness. The state-resolution logic is fully unit-tested (AC.1-4); the manual procedures cover the remaining observable behavior. Building a TUI driver harness is out of scope for this issue.
- **Non-goal:** SDK mode (`src/sdk_mode.rs:883`) and ACP (`maki-acp/src/server.rs:759`) pass `thinking: Default::default()` and don't restore stored thinking on resume — separate pre-existing gap, out of scope for this issue.

No blockers. The only decision is the scope of the `always_thinking` mid-session follow-up (recommended: separate effort).