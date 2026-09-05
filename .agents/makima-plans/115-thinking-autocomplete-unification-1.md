## Goal

Centralize thinking semantics in one shared crate and remove duplicate `ThinkingConfig`/`Effort` representations and string lists. Use the canonical data for `/thinking` autocomplete, host-provided Lua selector options, parsing, persistence-compatible serialization, provider integration, and generated documentation.

## Implementation Summary

Add a dependency-leaf workspace crate, `maki-domain`, owning the domain types and metadata. `ThinkingConfig` will be the single configuration enum with `Off`, `Adaptive`, `Effort(Effort)`, and `Budget(u32)` variants. `Effort` remains a nested enum because an effort level is independently used for provider capabilities, ordering, snapping, and budget percentages; it is not a second configuration enum. Thus the two existing semantic models are merged into one type identity without conflating configuration modes and effort levels.

The shared crate owns canonical effort names/order, finite selector options, aliases, parsing errors, `Display`/`FromStr`, a custom serde representation compatible with the existing `kind`/`level`/`tokens` persistence schema, budget math, and formatting helpers. `StoredThinking` is removed rather than retained as a second semantic enum. Storage fields use the shared `ThinkingConfig` directly, with serialization compatibility implemented by that type. Storage, commands, providers, config, UI, Lua, and root consumers migrate to the shared type. Provider-specific dialects, capability clamping, adaptive model detection, and wire translation remain in `maki-providers`.

The existing `maki.session.thinking()` host response will include a canonical ordered `options: string[]` list. This is an intentional Lua UX boundary: the host creates it from typed shared metadata, and the plugin consumes it without any list or descriptions of its own. The thinking plugin will contain no hardcoded ladder. `/thinking` completion will use the same shared metadata. Numeric budgets remain accepted at UX boundaries but are not finite selector options. The argument hint and duplicated usage list are removed once completion and generated messaging are in place. Generated docs use the shared metadata; provider JSON examples remain literal because they document external wire contracts.

## Implementation Plan

### Phase 1: Create the canonical domain crate

1. Add `maki-domain` to the workspace and workspace dependencies. Use only lightweight domain dependencies such as `serde` and `thiserror`. Keep the crate intentionally domain-focused rather than a general-purpose type bucket: begin with `thinking.rs`, and add future modules only for cross-cutting, behavior-rich concepts that cannot belong to an existing crate.
2. Make `maki-thinking::ThinkingConfig` and `maki-thinking::Effort` the sole public semantic types. `Effort` is a public nested value type used by `ThinkingConfig::Effort`, provider capability lists, ordering, and snapping. `StoredThinking` is deleted; the shared `ThinkingConfig` owns a custom serde implementation that emits and reads exactly the legacy tagged schema. Storage may re-export the shared types from compatibility modules, but may not define wrappers or alternate parsers.
3. Move the semantic behavior currently split between `maki-commands/src/spec.rs`, `maki-storage/src/sessions.rs`, and `maki-providers/src/types.rs` into the new crate:
   - `ThinkingConfig::{Off, Adaptive, Effort(Effort), Budget(u32)}`.
   - `Effort::{Minimal, Low, Medium, High, XHigh, Max}` in the existing intensity order.
   - Canonical option metadata for `off`, `adaptive`, and all effort levels, including aliases and descriptions.
   - `ThinkingParseError`, `MIN_THINKING_BUDGET`, effort ordering, names, percentage/budget conversion, inverse conversion, and snapping.
3. Preserve ordinary parsing behavior: trim whitespace; accept canonical modes, six effort values, and positive numeric budgets; reject zero budgets distinctly; report unknown values consistently. Make `on` and `true` aliases for `adaptive`, and `false` an alias for `off`, in the shared parser so slash commands, config, and Lua string settings have one contract. Keep `toggle` as a current-value-dependent command operation, not a stateless parse.
4. Implement custom serde for canonical `ThinkingConfig` with exact representations: `{"kind":"off"}`, `{"kind":"adaptive"}`, `{"kind":"effort","level":"minimal"|"low"|"medium"|"high"|"xhigh"|"max"}`, and `{"kind":"budget","tokens":<positive u32>}`. Unknown kinds, missing required fields, zero budgets, and invalid effort names remain errors.
5. Add focused unit tests for canonical option ordering/metadata, parsing and aliases, zero/unknown errors, display/parse round trips, serde fixtures, effort ordering, percentages, budget conversion, inverse conversion, and snapping.

### Phase 2: Migrate commands and autocomplete

1. Remove the flattened `maki-commands::ThinkingConfig`; use/re-export `maki_thinking::ThinkingConfig` in `BuiltinOperation::SetThinking` and `HostContextResponse`.
2. Register `/thinking` completion and implement its provider from `ThinkingConfig::options()`. Completion labels/insertions must be canonical values and descriptions must come from shared metadata.
3. Remove `/thinking`'s `argument_hint`; retain optional arity and numeric-budget support.
4. Refactor `maki-agent/src/command.rs` to use shared parsing plus a command-level toggle helper. Remove the local effort-name match and derive any accepted-value error text from shared metadata.
5. Update command tests for nested config variants, toggle behavior, aliases, budgets, no hint, and exact completion output. Confirm headless/ACP/SDK command registration remains valid without a UI completion provider.

### Phase 3: Migrate storage and preserve old files

1. Make `maki-storage` depend on `maki-domain`. Change `SessionMeta::thinking`, `Prefs::default_thinking`, and related APIs to use `maki_thinking::ThinkingConfig` directly.
2. Remove storage's semantic `Effort`, `StoredThinking`, parser, and duplicate config. Re-export `maki_thinking::{Effort, ThinkingConfig, ThinkingParseError}` from compatibility paths only where required by downstream API stability; every re-export must resolve to the same underlying type.
3. Preserve and test existing session JSONL and `prefs.json` forms, including `kind=off`, `kind=adaptive`, `kind=effort` with `level`, and `kind=budget` with `tokens`. Keep field attributes and omission behavior unchanged.
4. Migrate storage tests so legacy fixtures load and current writes round-trip byte/structure-equivalently where existing tests require it.

### Phase 4: Migrate providers and all internal consumers

1. Make `maki-providers` depend directly on `maki-domain`. Remove provider conversions through storage `StoredThinking`; re-export `Effort` and `ThinkingConfig` from `maki-providers` if that avoids unnecessary downstream API churn.
2. Keep provider-only behavior in place and typed with the shared enums: `EffortDialect`, model supported-level declarations, snapping, budget clamping, adaptive-version detection, local thinking fragments, and provider request-body mapping.
3. Migrate `maki-config`, `maki-ui`, `maki-agent`, `maki-lua`, ACP, SDK, and root TUI consumers. Delete conversion functions whose only purpose was translating between duplicate config enums.
4. Convert strings/numbers to enums exactly at UX/config/API boundaries. Internal state, command operations, host messages, and provider abstractions use typed values. Strings remain only for UX serialization, persisted/protocol compatibility, provider JSON, tests, and generated documentation.
5. Preserve model capability clamping, session-over-global preference precedence, Lua error behavior, and provider request outputs.

### Phase 5: Source autocomplete and the Lua selector from host data

The existing Lua test host can already stub `SessionRequest::GetThinking` and inspect the returned Lua table, but it cannot currently execute the bundled thinking plugin with a reordered response. Extend the test host in `maki-lua/src/api/session.rs` test support and the bundled-plugin test setup under `maki-lua/tests` (using the existing plugin host/test-support path) to load the thinking plugin with injected `GetThinking` options. The new infrastructure test is `plugin_harness_can_override_host_thinking_options`; only after it passes is the plugin scenario test run.

1. Extend `maki.session.thinking()` with exactly `options: string[]`, generated from `ThinkingConfig::options()` in canonical order. Include `off` and `adaptive`; exclude arbitrary budgets. The plugin does not need descriptions because it already renders values.
2. Generate the response in the UI host/event-loop path from shared metadata. Do not add a UI-local list. The exact Lua-visible response is always `{ mode = string, supports_thinking = boolean, options = string[] }` on success; `options` is required and is never absent. For a focused session whose model does not support thinking, return `supports_thinking = false` with the same canonical options array. Preserve the existing `(nil, error)` response for unavailable/no-UI cases. If the canonical array is unexpectedly empty, the plugin reports the selector as unavailable; it does not invent fallback options. If the current mode is absent, selection starts at the first host option.
3. Change `plugins/thinking/init.lua` to read `info.options`, pass that list to indexing/wrapping/rendering, and remove `THINKING_LEVELS`.
4. Keep selected values converted to strings only when calling `maki.session.set_thinking()`. The host-side request and internal handling remain typed.
5. Add a Lua API field/documentation update and tests for response shape/order. Add a plugin/runtime scenario test using a deliberately distinctive host option order so hardcoded plugin lists regress visibly.

### Phase 6: Remove stale hints and generate documentation

1. Add shared formatting helpers for accepted settings and effort lists, including the budget placeholder. Use them in command/provider/config error text rather than maintaining literal lists.
2. Remove `THINKING_USAGE` entirely after completion works. The `/thinking` command metadata has no argument hint, and invalid-input errors use the shared parser error/formatter.
3. Update `maki-docgen` to consume shared thinking metadata directly for provider and command prose. Preserve literal provider request JSON examples that describe external APIs.
4. Update Lua API docs for `thinking().options` and numeric budgets. Remove duplicated effort lists from hand-written plugin/API prose where generated references can be linked.
5. Reconcile duplicate built-in/plugin `/thinking` command documentation according to runtime precedence. Regenerate checked-in docs with `just gen-docs`; ensure `just gen-docs-check` passes.

### Phase 7: Review and validation

1. Run focused checks and tests in dependency order, then workspace check/lint/test commands from `AGENTS.md`.
2. Dispatch a `general` implementation reviewer after implementation and automated testing. Fix or explicitly rebut all findings; rerun review if any critical findings remain.

## Acceptance Criteria

- **AC.1**: One shared crate contains the only semantic `ThinkingConfig` and `Effort` definitions, with no competing command/storage/provider copies. Add `maki-commands/tests/thinking_architecture.rs` with `shared_thinking_types_have_single_owner`: scan `maki-thinking/src` and the source trees of `maki-commands`, `maki-storage`, and `maki-providers`, assert the canonical enum declarations occur in `maki-thinking/src` exactly once each, assert no enum declarations occur in the other three trees, and compile compatibility imports through generic identity helpers. Run exactly with `cargo test -p maki-commands --test thinking_architecture` plus `cargo check --workspace`.
- **AC.2**: Shared parsing preserves canonical modes, aliases, whitespace, positive budgets, zero-budget rejection, and unknown-value errors. Verify with `maki-domain` tests `parse_canonical_modes`, `parse_aliases`, `parse_budgets`, and `parse_invalid_values`.
- **AC.3**: Effort ordering, names, percentages, budget mapping, inverse mapping, and snapping are unchanged. Verify with `maki-domain` tests `effort_order_and_names`, `effort_budget_mapping`, `effort_from_budget`, and `effort_snap`.
- **AC.4**: Existing session and preference JSON remains readable/writable with the prior tagged schema. Verify with separate storage tests `legacy_session_meta_thinking_loads`, `legacy_prefs_thinking_loads`, `session_meta_thinking_serializes_legacy_shape`, and `prefs_thinking_serializes_legacy_shape`, covering all four variants and omitted/default fields.
- **AC.5**: `/thinking` dispatch uses shared typed parsing, preserves toggle/alias/budget behavior, and has no argument hint. Verify with agent tests `thinking_command_parsing_and_toggle` and `thinking_command_metadata`.
- **AC.6**: `/thinking` completion returns the shared canonical ordered options and metadata. Verify with UI test `thinking_completion_matches_shared_options`.
- **AC.7**: `maki.session.thinking()` exposes host-generated canonical options in order, including `off` and `adaptive` and excluding arbitrary budgets. Verify with UI host test `thinking_host_response_uses_shared_options` and Lua API test `thinking_response_contains_options`.
- **AC.8**: The thinking plugin has no hardcoded option ladder and follows host-supplied order/values. Verify with plugin scenario test `thinking_plugin_uses_host_options` using a non-default option order. If the harness needs extension, verify the infrastructure with `plugin_harness_can_override_host_thinking_options` before this scenario.
- **AC.9**: Provider wire behavior and capability clamping remain unchanged. Verify with named migrated tests `thinking_apply_reasoning_effort`, `thinking_apply_to_body`, `thinking_apply_google_thinking`, `thinking_apply_local_thinking`, `thinking_budget_resolver`, and `thinking_clamped_to_model_capabilities`.
- **AC.10**: Generated command, Lua API, provider, and relevant config documentation reflects shared metadata and contains no stale manually maintained thinking list. Verify with named docgen test `generated_thinking_docs_match_shared_options`, which asserts every canonical option is present, the removed argument hint is absent, and no duplicate `/thinking` command row is emitted, plus `just gen-docs-check`.
- **AC.11**: Internal consumers use enums/configs, with conversion ownership fixed as follows: `maki-domain` owns canonical `Display`/`FromStr` and serde conversion; `maki-agent`, `maki-config`, and `maki-lua` perform conversion only when accepting external command/config/Lua values; `maki-ui` performs conversion only when constructing the Lua `options: string[]` response; `maki-storage` performs only the legacy persistence serde boundary; `maki-providers` performs only provider JSON/wire conversion; plugins and docs consume strings. Add `maki-commands/tests/thinking_architecture.rs::internal_thinking_paths_use_shared_types`: scan `maki-agent/src`, `maki-config/src`, `maki-lua/src`, `maki-ui/src`, `maki-storage/src`, and `maki-providers/src` and fail on `enum ThinkingConfig`, `enum Effort`, `enum StoredThinking`, `StoredThinking::parse_setting`, effort-name match arms outside parser/wire/docs test modules, or string-valued internal fields named `thinking`/`mode` outside explicitly listed boundary structs. Also compile typed command/host/storage/provider paths.

## Test Strategy

- AC.2 and AC.3: pure unit tests in `maki-domain`.
- AC.4: storage tests load and serialize the explicit legacy fixtures `{"kind":"off"}`, `{"kind":"adaptive"}`, `{"kind":"effort","level":"high"}`, and `{"kind":"budget","tokens":4096}`; assert exact structural equality of serialized JSON, absent optional fields remain absent, and malformed/missing required fields retain rejection behavior.
- AC.5: real command registry/host-recording tests in `maki-agent`.
- AC.6: completion provider tests in `maki-ui` comparing returned items to `ThinkingConfig::options()`.
- AC.7: UI event-loop host response tests plus Lua API round-trip tests asserting the exact required schema `{mode:string,supports_thinking:boolean,options:string[]}`, canonical order, unsupported-model behavior, and no-UI error behavior.
- AC.8: Add or extend the existing Lua test harness to execute the bundled thinking plugin with host options intentionally reordered. If extension is necessary, first add the harness capability and test it with `plugin_harness_can_override_host_thinking_options`, then run `thinking_plugin_uses_host_options`.
- AC.9: named provider request-body and policy tests listed under AC.9, migrated to shared types.
- AC.10: `generated_thinking_docs_match_shared_options`, which invokes the docgen render functions in memory and asserts every canonical option appears exactly once in generated command/provider/Lua sections, the removed argument hint is absent, and `/thinking` has one effective command row, plus `just gen-docs-check` for checked-in artifacts.
- AC.1 and AC.11: `maki-commands/tests/thinking_architecture.rs`, run with `cargo test -p maki-commands --test thinking_architecture`; AC.1 scans `maki-thinking/src`, `maki-commands/src`, `maki-storage/src`, and `maki-providers/src` for the two canonical enum declarations, while AC.11 scans the explicitly listed consumer source trees for the forbidden definitions, parser calls, effort-name match arms, and untyped internal fields described in AC.11. Compile-time typed-path coverage supplements these source assertions.

The execution phase must first add the plugin-host override seam and pass `plugin_harness_can_override_host_thinking_options` in the existing `maki-lua` test-support path, then add and pass `thinking_plugin_uses_host_options`; AC.8 cannot be claimed without both tests.

## Review Strategy

The planning session should run a `plan_reviewer` audit before handoff, using this user request and the inspected crate/dependency structure as context. Fix all critical/high findings in this plan and rerun the reviewer if necessary. After implementation, run the repository's normal checks and dispatch a `general` reviewer; resolve all critical/high implementation findings before completion.

## Documentation Strategy

Update Lua API documentation for the new `thinking().options` response and numeric-budget boundary. Regenerate command/provider/Lua docs through `maki-docgen`. Remove stale hardcoded effort lists and usage hints. No AGENTS or architecture-document change is needed unless the new crate introduces a repository convention not already covered by existing guidance.

## Risks, Blockers, and Required Decisions

- The canonical public model is exactly `maki_thinking::ThinkingConfig` plus its nested public `maki_thinking::Effort`; there is no third semantic type. `StoredThinking` is deleted. `ThinkingConfig` custom serde owns the legacy persistence representation, while `Display`/`FromStr` own canonical UX conversion. `maki-commands` and `maki-providers` may re-export these names solely for downstream compatibility, never wrap or redefine them.
- Persistence compatibility is non-negotiable. The canonical type or storage adapter must retain the existing tagged JSON shape and receive fixture tests before old types are removed.
- `maki-commands` is intentionally frontend-neutral, so the shared crate avoids a dependency on storage or providers.
- Provider-specific wire strings and test fixtures remain intentional string boundaries and must not be replaced with internal enum plumbing.
- The Lua plugin option response is a host contract. The plugin must fail clearly when options are unavailable rather than reintroducing a duplicate fallback list.
- The builtin `/thinking` registration is authoritative for command reference because it is the standard command contract and owns argument completion. The bundled plugin's `/thinking` command is an override implementation detail and must not produce a second command row. Generated command docs will emit one `/thinking` row using the effective runtime registration: builtin metadata plus a note that the plugin supplies the interactive selector where applicable.
