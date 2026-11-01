# Simplify shared model options and plan approval

## Goal

Keep what #172 asks for, and what PR #182 delivers on top of it: one model picker that edits a model and its thinking and fast options, used for `/model`, Ctrl+M and the plan form's implementation model. Approval still commits the selection and the Build mode together. The difference is the amount of machinery. The PR added about 9,250 lines, with roughly 6,800 of them tests, many of which pin implementation details. This plan removes the parts that are not needed, fixes the bugs the review found, and leaves code that reads in one pass.

## What the review found

The descriptor registry (`ModelOptionSpec`) is a table of function pointers for two options. It round-trips typed values through strings, and the option specifics leak back out anyway: `SessionOptionDefinition::model_option` matches `"true"` and `"false"`, maps any free-input option to `FreeValueDomain::ThinkingSetting`, and docgen enumerates thinking and fast capabilities by hand. Adding a third option would still touch every layer the registry claims to hide.

The capability resolver in `maki-ui/src/model_capabilities.rs` and `sync_model_capabilities` in the event loop use five independent staleness mechanisms, a waiter list, a 10 ms polling loop, a FIFO cache keyed by target and config generation, and two internal events. Even so they have bugs:

- A failed preparation is cached as "unresolved" with no end.
- Capabilities set at open are reset to unresolved on the next tick, so option rows flicker and focus drops.
- Moving the highlight from A to B and back to A can leave A unresolved.
- Each preparation holds two blocking-pool threads.

The picker's retained-conflict state blocks Enter for drafts the backend accepts (`fast=false` on a model without fast) and for drafts whose capabilities are still loading, and the footer then suggests Ctrl+R, which deletes them. The plan form shows inherited option values using the current model's capabilities rather than the staged model's.

Around this sit a 496-line architecture test containing a hand-written Rust tokenizer, a `set_model_with_claim` that copies `commit_patch` so the tokenizer finds the right call, tests that assert constants equal their own literals, docgen tests that look for sentences the same PR wrote, and the model picker described in three separate doc pages.

## Design

### Model options are an enum

`maki-domain/src/selection.rs` defines:

```rust
pub enum ModelOption { Thinking, Fast }
```

`ModelOption::ALL` lists them. Methods give the key, label, description, presets, whether free input is accepted, and typed `get`/`set` on a `ModelSelection`. Each method is one exhaustive `match`, so a new option is a compile error in exactly the places that need a decision, and nowhere else. `ModelSelection::set_option` and the derived deserializer go back to the shape on `mistress`, with `set_option` delegating to the enum. The `True`/`False` preset labels go; presets are the canonical values.

Capabilities belong next to `Model`, so `maki-providers/src/selection.rs` defines `ModelCapabilities { thinking: ThinkingSupport, fast: FastSupport }`, which is `Copy`, built with `From<&Model>`, and has `availability(option) -> Availability`. "Not resolved yet" is `Option<ModelCapabilities>::None` in the one place that needs it, the picker. A preparation failure falls back to the static capabilities of `Model::from_spec`, so failure never looks like loading, and commit-time validation stays authoritative.

Session options go back to their own definitions on `mistress` (`thinking_definition`, `toggle_definition`). They have their own vocabulary (`enabled`/`disabled`) and gain nothing from an adapter.

### One capability cache, pulled by the picker

`ModelCapabilityCache` holds `HashMap<Arc<str>, ModelCapabilities>` keyed by spec, plus the set of specs in flight and an epoch. `get(spec)` returns a cached value, or starts one resolution on a blocking thread and returns `None`. When a resolution finishes it stores its result, if the epoch has not moved, and calls a wake callback. The event loop wires that callback to `InternalEvent::ModelCapabilitiesResolved`, which refreshes open pickers. Discovery and provider changes call `clear()`, which bumps the epoch.

The picker asks the cache from `refresh()`, which the app already polls. The current model's capabilities come synchronously from its prepared `Model` at open, so the common case never waits. This removes `ModelCapabilityRequest`, `model_picker_generation`, the picker fields of `OrchestrationRevision`, `sync_model_capabilities`, and the ready and invalidated events.

Command completion iterates `ModelOption::ALL` and offers the options the named model supports, resolved through the same cache with the shared `resolve_model` identity resolution, or those of the target's current model when no model is named. It needs no projection identities or revisions: a stale suggestion is harmless because applying the selection validates again.

### Picker drafts without conflicts

The picker keeps one draft per option. Option rows are shown only for options the highlighted model supports. Enter submits the spec plus the drafts of the visible options; drafts for hidden options stay in their editors, so moving back to a supporting model restores them, but they are never submitted. While capabilities are unresolved, Enter waits only if a draft exists, and the footer says why. The conflict list, Ctrl+R, the width-dependent hint variants and the footer budget arithmetic go; `ListPicker` already clamps footer rows. `current_spec` folds into `current`, and the test-only `specs` field goes.

### Plan form shows what is staged

The plan form shows the staged model, or the current one, and one line of staged overrides such as `thinking=high fast=false`. It no longer predicts inherited values, which needs the staged model's capabilities that the form does not have. `plan_form.height` and `view` go back to taking the current spec, and `focused_model_selection` and the `ViewLayout` plumbing go. The plan picker takes its current selection from the agent that owns the plan form, which removes the double clearing against two different agents.

The "focused form, or the saved one in `plan_forms`" lookup is written six times. One `plan_form_entry(key)` helper replaces those copies.

### Agent side

`set_model_with_claim` calls `commit_patch`, and `commit_patch` takes a `ModelSelection`. `prepare_selection` stays as the single conversion used by approval, the TUI and both Lua paths. The approval's pending-fast handling (`ApprovalPendingFast`, the wrapped transaction finalizer) becomes a plain flag if the success path can clear the saved `/fast` wish directly; otherwise it stays and gets a comment explaining the race.

### Tests and docs

Delete the architecture test, the constant-equals-literal test, the docgen prose tests, and the resolver and event-loop tests that assert internal counters and requests. Keep and adapt the behaviour tests: atomic approval, omitted versus explicit options, staging kept on cancel and failure, stale approval rejected, pinned-target policy, picker submit and cancel.

The model picker gets one canonical section on the providers page. The commands page keeps a short `/model` entry that links to it. The keybindings page relies on the `KEYBINDS` table it already generates, without a hand-written duplicate. The prose follows the docs tone in `AGENTS.md`.

## Steps

The repository's pre-commit hook runs `just ci`, so every commit has to build and pass on its own. The descriptor registry, the capability resolver and the picker share types, so they landed together.

1. Commit this plan.
2. Replace the descriptor registry with `ModelOption`, move capabilities to providers, replace the resolver with `ModelCapabilityCache`, simplify picker drafts and the plan form, add the plan-form lookup helper, and remove the architecture test and the tests of removed machinery.
3. Treat approval overrides like other model selections for the saved `/fast` wish: drop it when approval starts, which removes `ApprovalPendingFast`. `set_model_with_claim` delegates to `commit_patch`.
4. Consolidate the docs and regenerate them.
5. Rename leftovers and record what landed.
6. Make completion follow the named or current model's capabilities again, through the cache.

An approval with an explicit model or fast override now drops a restored `/fast` wish even if the approval later fails or is cancelled. That matches ordinary model selection, and it is the one behaviour change from the PR as reviewed.
