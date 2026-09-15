## Goal

Implement issue #123: let users choose an implementation model from plan approval, then use it for either implementation path. Selection is staged until approval; after approval it remains the normal session model, as explicitly confirmed by the user.

## Implementation Summary

Reuse the existing model picker in `maki-ui`, adding a selectable `Implementation model: <spec>` row to the plan form and its existing Ctrl+M shortcut. Default dynamically to the current model; display any staged override without switching models until implementation is confirmed.

Keep changes within the TUI form, picker routing, and runtime approval boundary. Reuse normal policy checks, provider creation, model persistence, and recents. No new dependencies, temporary model lifecycle, plugin APIs, or issue #24 host/plugin migration.

## Implementation Plan

### 1. Plan form and staged selection

- In `maki-ui/src/components/plan_form.rs`, append a fourth selectable model row, preserving existing Refine/Clear/Implement indices and default selection. Enter on this row emits a picker-open action, never implementation. Render the effective model (override or current) and distinguish the default/current case from an explicit selection. Pass current model into rendering rather than storing a redundant default snapshot.
- Add `Option<String>` staged implementation model to the form. Preserve it through picker cancellation, editing, refining, dismiss/reopen, and plan rewrites within the same planning lifecycle. Clear it on form reset, session replacement and successful implementation. Preserve existing parallel-option semantics; do not append its indicator to the model row.
- Handle `key::MODEL_PICKER` before generic key-code handling (Ctrl+M may arrive as a modified character). Include its platform-aware hint. Update form height/layout for the additional row.
- Track picker purpose explicitly (normal session or plan implementation), clearing that routing state when the picker closes or the session resets. Open with the effective staged/current spec using `ModelPicker::open` and the existing refresh action. Reuse search, paste, recents, refresh anchoring and selection behavior.
- In `maki-ui/src/app/mod.rs`, route plan-picker selection to staged state only, with no ChangeModel action, preference/recents update, or implementation start. Escape returns to the form unchanged, including selected row and parallel setting. Ordinary picker invocation continues to switch immediately. Existing tier-assignment shortcuts retain their separate global semantics.
- Fix key and paste routing: the form currently precedes the picker (`app/mod.rs:923`, `:1064`, `:2778`). An open picker must receive its input while permission prompts retain priority. Update keybinding context precedence in `app/view.rs:583`, and form rendering at `app/view.rs:283`.

### 2. Gate approval on successful model application

- Have both approval actions emit a single `Action::ImplementPlan { clear_context }` without consuming the plan, resetting the form/session, or creating implementation input. Keep the existing `implement_plan` logic (`app/mod.rs:2843`) as the continuation, accessible to runtime handling.
- At the runtime action boundary, resolve the form's override. Without one, continue using the current model without reconstructing its provider. With one, run the existing normal switch operation before the continuation.
- Reuse `event_loop.rs:1986` semantics: policy validation, parsing, provider initialization, App model update/persistence, recents and shared ProviderSlot installation. On failure flash the existing helpful error, retain Plan mode, form, staged choice and session, and do not produce send/reset actions. Validation/provider initialization must precede model or preference mutation.
- Only after success call the implementation continuation and dispatch its actions in order. Retain existing plan snapshot, parallel prompt, Build transition, same-context history, and clear-context NewSession behavior. The installed model must survive the fresh-session path and remain selected afterward.
- Do not prepend ChangeModel to the old action list: old approval eagerly constructs input, while ChangeModel failure does not stop subsequent actions. Input captures thinking/fast settings (`app/mode.rs:197`), so model update and slot installation must precede input creation.

### 3. Testable runtime boundary and automated coverage

- Existing event-loop tests do not instantiate a complete terminal/runtime. Extract narrow terminal-independent switch/approval helpers used by production dispatch, operating on the real App, ProviderSlot and policy. Inject provider construction so tests can deterministically return a fake provider or an error, without credentials, live APIs or a terminal. Ordinary ChangeModel must use the same switch helper to avoid divergent semantics.
- Reuse `app/tests.rs` test App/plan fixtures and temporary storage. Add a narrow continuation callback boundary if needed to assert slot installation before implementation input construction, not merely after helper return. Exercise the actual implementation continuation from tests.
- Add parameterized `#[test_case]` tests for both implementation paths, default/override selections, failure classes, and preserved controls. Assert resulting actions/input, session identity/history, plan state, persisted model/recents and slot, not just helper success values. Test the fresh-session consumer path with a terminal-free dispatch fixture around existing NewSession/send handling if needed, so a reset losing the installed model is caught. This fixture is in scope; do not substitute action-shape checks for fresh-runtime verification.
- Add ratatui TestBackend render assertions for the model row/default/override, shortcut hint and form height, plus App key/paste scenarios for the nested picker.

## Acceptance Criteria

- **AC.1:** Approval visibly shows the current implementation model by default and a selected override afterward. Selecting its row or Ctrl+M opens the existing searchable picker without approving the plan.
- **AC.2:** Picker selection only stages a model; cancellation preserves the previous choice and returns to the form. Normal session model, preferences and recents do not change until approval. A normal picker outside approval still switches immediately.
- **AC.3:** Both implementation options start using the staged model, or the current model if unchanged. Successful override remains the session/persisted model; clear-context implementation uses it in the fresh runtime. Input settings are normalized for that model before construction.
- **AC.4:** Policy, model-parse or provider-initialization failure prevents implementation and session reset, preserves the staged choice and plan for retry, and visibly reports an error without changing active/persisted model or recents.
- **AC.5:** Refine, edit, dismiss/reopen, parallel toggle and ordinary plan navigation continue working; staging survives this planning lifecycle but cannot leak to another session or later plan after reset. Picker input takes precedence over form input, but not permissions.

## Test Strategy

Named tests to implement (parameterized cases where appropriate):

| Criteria | Tests and layer |
| --- | --- |
| AC.1 | `plan_form_renders_implementation_model` (TestBackend default/override and height); `plan_form_opens_model_picker` (App row Enter/Ctrl+M scenarios). |
| AC.2 | `plan_picker_stages_without_switching`, `plan_picker_cancel_preserves_selection`, `normal_picker_still_changes_model` (App scenarios with persistence/recents assertions). |
| AC.3 | `plan_approval_uses_effective_model` (production runtime helper plus real continuation, clear/keep and default/override); `plan_approval_installs_model_before_input` (continuation boundary and normalized settings); `clear_plan_implementation_retains_model_in_fresh_runtime` (terminal-free runtime dispatch fixture verifying the provider/model observed by the implementation run); `plan_approval_persists_selected_model` (temporary storage integration). |
| AC.4 | `plan_model_failure_preserves_approval` (injected provider failure, parse failure and policy denial, both paths; assert no continuation/reset/send and unchanged state/storage); `plan_model_failure_renders_error` (render scenario). |
| AC.5 | `plan_model_selection_lifecycle` (refine/edit/dismiss/rewrite/reset/session replacement); `plan_form_existing_controls_with_model_selection` (navigation/editor/parallel); `plan_picker_receives_keys_and_paste` and `permission_prompt_precedes_plan_picker` (App scenarios). |

The production helper and terminal-free dispatch fixture supply the missing runtime testing seam in this work. Fake providers and temporary storage avoid network, credentials, sleeps and persistence leakage. Existing tests in `components/plan_form.rs`, `components/model_picker.rs`, `app/tests.rs:4768` onward and `event_loop.rs:2200` provide reusable patterns; update old approval tests for intent/continuation separation.

Run cheapest first: `cargo check -p maki-ui --tests`, `cargo clippy -p maki-ui --tests -- -D warnings`, `cargo nextest run -p maki-ui`. Then run repository-wide `just lint`, `just test`, and documentation generation/check commands from `justfile`. No builds or tests have been run during planning.

## Review Strategy

Before handoff, use a read-only plan_reviewer and resolve its findings. After any critical or high plan-review findings, fix or explicitly rebut all findings and run another plan-review pass before submission. Initial plan review passed; its low-severity request to document this retry requirement has been incorporated. After implementation and all automatable checks, dispatch a general subagent to review the completed changes, concentrating on routing cleanup, deferred model mutation, error gating and fresh-session ordering. Fix or explicitly rebut all findings; repeat review for critical findings until none remain or escalate a blocker.

## Documentation Strategy

Update the handwritten approval workflow in `site/docs/content/modes/_index.md:109` to describe the selectable model row, Ctrl+M, staged cancellation and persistent switch on approval. Update contextual keybinding help in `maki-ui/src/components/keybindings.rs`; regenerate generated keybindings using `just gen-docs`, and validate `just gen-docs-check`. Do not manually edit generated docs. No AGENTS.md change is needed because this adds no repository-wide architectural contract.

## Risks, Blockers, and Required Decisions

- User decision resolved: implementation model defaults to current, can be overridden via a pressable form control, appears in the dialog, and remains selected after approval. Use a selectable row plus existing Ctrl+M, not a temporary per-run override.
- The runtime currently combines model state/persistence with provider installation, and approval eagerly mutates state. Deferred approval must preserve the required ordering and must not partially approve on errors.
- Fresh-context resets and popup routing can erase or leak state. Explicit lifecycle and fresh-runtime tests are required.
- There is no complete terminal event-loop test harness. Building the narrow production helper/terminal-free runtime fixture above is part of this plan, not an accepted test gap.
- Issue #24 may later relocate plan behavior to plugins. Keep this feature in current UI/action boundaries without adding host/plugin contracts.
- No unresolved product decisions or external blockers.
