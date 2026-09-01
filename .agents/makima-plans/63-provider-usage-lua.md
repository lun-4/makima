# Goal

Replace issue #63’s Rust-owned provider quota meter and `/usage` modal with a bundled Lua `usage` plugin, while retaining provider I/O and authoritative session accounting in Rust. Expose reusable Lua primitives for provider usage, focused-session usage, and styled status-bar content, with coalesced requests, account-generation safety, no headless quota traffic, and deterministic automated coverage.

# Implementation Summary

Implement three host-facing primitives:

1. `maki.usage` reads a Lua-thread mirror of one event-loop-owned provider-usage coordinator. `maki-ui` remains the only place that calls `Provider::fetch_usage()`, owns request generations and waiter groups, and rejects stale completions.
2. `maki.session.usage()` returns the focused session’s already-recorded token/cache/cost accounting without repricing it.
3. `maki.ui.set_status_content(...)` publishes plugin-scoped styled spans through a generation-tracked snapshot parallel to `HintStore`/`HintWriter`; `set_status_hint` keeps its key-label semantics.

Add a generation-tracked provider slot/wrapper around the active `ModelSlot`. Every provider installation receives a new instance generation. Successful `reload_auth()`, `refresh_auth()`, and `rotate_key() == true` calls increment its auth generation and wake the event loop. Usage fetches capture the combined provider identity and cannot publish after either provider replacement or a tracked account mutation. Background model discovery must return a candidate to the event loop instead of writing `model_slot` directly.

Bundle `plugins/usage/init.lua`. It registers the TUI-only `/usage` command, owns a 30-second `maki.timer`, formats compact quota lanes into generic status content, and renders provider quota plus focused-session accounting in a Lua float. It registers safely in every host but does not subscribe, fetch, or arm its timer until the TUI-only `SessionFocusChanged` event arrives.

Remove `maki-ui/src/components/usage_modal.rs`, the per-`App` quota slot/poller/readout, `Action::RefreshUsage`, and the standard Rust `/usage` command (`BuiltinId::Usage` / `BuiltinOperation::ToggleUsage`). Keep `ProviderUsage`, `UsageLimit`, `UsageWindow`, and all provider-specific `fetch_usage()` implementations; remove only presentation-only Rust helpers whose callers disappear.

Primary touch points:

- `maki-lua/src/api/mod.rs`, new `maki-lua/src/api/usage.rs`, `maki-lua/src/api/session.rs`, `maki-lua/src/api/ui/mod.rs`, and `maki-lua/src/api/util/command.rs`
- `maki-lua/src/runtime.rs`, `maki-lua/src/loader.rs`, `maki-lua/src/lib.rs`, and test support
- new `maki-ui/src/provider_usage.rs` (or an equivalently isolated coordinator), `maki-ui/src/event_loop.rs`, and the generation-tracked provider slot near `maki-ui/src/agent/mod.rs`
- `maki-ui/src/app/mod.rs`, `maki-ui/src/app/view.rs`, `maki-ui/src/components/status_bar.rs`, `maki-ui/src/components/mod.rs`, and `maki-ui/src/theme.rs`
- `maki-commands/src/spec.rs` and `maki-agent/src/command.rs`
- new `plugins/usage/init.lua` plus pure Lua and real-host integration tests
- generated Lua API/plugin/command documentation under `site/docs/`

Scope boundaries:

- Quota is global to the single active provider slot, not per focused session.
- Session usage is local accounting only and never triggers provider I/O.
- Lua owns refresh policy and all quota/session presentation; Rust exposes data and generic UI primitives.
- Do not change provider-specific quota HTTP/parsing implementations or retroactively calculate historical cost.
- Account-generation tracking covers provider installations and all host/agent calls through the `Provider` trait’s `reload_auth`, `refresh_auth`, and `rotate_key` methods. Audit direct self-calls inside provider implementations; any usage-capable provider that mutates quota credentials internally must be routed through the same generation notifier. Current direct Bedrock self-reload is acceptable only while Bedrock has no usage endpoint.

# Implementation Plan

## Phase 0: Build deterministic test seams

1. Extract the quota coordinator as a pure state machine in `maki-ui/src/provider_usage.rs` before wiring network tasks.
   - Inputs: provider installation/mutation, ordinary or forced fetch request with reply sender, fetch completion tagged with request and provider generations, and shutdown.
   - Outputs: start-fetch command, canonical-state publication, waiter replies, or no-op.
   - Tests inject completions explicitly; no network, executor timing, or sleep is involved.

2. Add internal event-loop completion/wake channels and an injectable provider-usage driver seam.
   - A fetch task sends `{request_generation, provider_identity, result}` to this channel.
   - The event loop compares the captured identity directly with `ProviderSlot::current_identity()` before feeding a completion to the coordinator; it does not rely on a mutation wake having been drained first.
   - Tasks never mutate an `ArcSwap`, Lua state, or coordinator directly.
   - Route background model-discovery candidates and provider-auth mutation notices through event-loop wake channels too, so every installation/invalidation happens on the event-loop owner.
   - Let coordinator/adapter tests inject an `Arc<dyn Provider>` or fetch-launch closure whose call count and pending completion are externally controlled. Production still constructs providers through `from_model`; tests do not touch the provider registry or network.

3. Add `maki-lua` test-support hooks for real plugin tests.
   - Implement a `ScriptedUiHarness` around a real `PluginHost` that continuously drains `ui_action_rx`, queues deterministic `Usage` and `Session::Usage` replies, captures `OpenWin { buf, config, event_tx, cmd_rx, ... }`, exposes buffer/span snapshots, and injects key, resize, close, and timeout events.
   - Add test-only timer inspection/manual-fire requests on the Lua thread. Tests can assert plugin owner and interval and trigger a registered callback without `Instant::now()` or wall-clock sleeps.
   - Add a status-content writer/reader pair parallel to `hint_writer_pair()`.
   - Keep these surfaces behind `test-support` so production builds do not expose them.

4. Prove the infrastructure itself with `coordinator_accepts_controlled_completion`, `scripted_ui_harness_replies_and_delivers_window_events`, `manual_timer_fire_runs_callback_without_wall_clock`, and `timer_inspection_reports_owner_and_interval`.

## Phase 1: Define provider identity, coordinator ownership, and Lua usage contracts

1. Replace widely writable raw provider installation with a generation-tracked provider slot.
   - Wrap the active provider in a local `TrackedProvider` that implements `Provider` and delegates every method.
   - Give each installed provider a monotonically increasing instance generation and an auth generation starting at zero. Expose one opaque `provider_id` derived from both; do not expose credential material.
   - After every `Ok(())` from `reload_auth()` or `refresh_auth()`, and after `rotate_key()` returns `true`, increment auth generation and send a provider-mutation wake. The unit-returning methods cannot report whether credentials materially changed, so conservative invalidation is the only implementable safe contract; only errors and `rotate_key() == false` preserve identity.
   - Increment the atomic auth generation before sending the wake. When any fetch completion is drained, compare its captured identity directly with `ProviderSlot::current_identity()` before feeding the coordinator, so a racing completion is rejected even if its mutation wake is still queued. Drain the mutation wake before rendering, transition canonical usage, then publish notifications.
   - Audit all direct auth mutation calls. Calls in `maki-ui/src/agent/mod.rs` and `maki-agent/src/agent/{run,streaming}.rs` must operate through the tracked provider. Any direct internal mutation in a usage-capable provider must gain the same notifier or be refactored through the tracked path.

2. Make the provider slot’s install method the only replacement path.
   - Use it for initial construction, `/model` and model-picker changes, loaded-session model adoption, same-model/provider credential refresh, and model-discovery replacement.
   - Change `spawn_model_fetch` to return a candidate model/provider to the event loop; it must no longer call `model_slot.store(...)` from its detached task.
   - Atomic event-loop installation order is: resolve the candidate successfully; install it and bump identity; transition the coordinator to that installed identity and resolve old waiters; immediately suppress plugin status content for all Apps; enqueue the new provider-tagged loading snapshot with a delivery acknowledgement; then enqueue `ProviderChanged` on the same Lua request sender. FIFO guarantees the Lua mirror/subscribers process invalidation before the autocmd.
   - Keep plugin status content suppressed until the Lua dispatcher acknowledges that it updated the mirror and synchronously ran usage subscribers. The acknowledgement carries the invalidated provider identity/invalidation generation and the status-content publication generation observed after callbacks. Ignore an acknowledgement unless it matches the currently gated invalidation.
   - Before reopening the gate, load the shared `StatusContentReader` snapshot at or beyond the acknowledged generation and synchronously reconcile every App’s cached `StatusContentSnapshot`; only then may a draw use plugin content. This covers the `App::tick()` -> ack drain -> draw interleaving without waiting for another poll.
   - The event loop continues rendering all other TUI content while suppressed. If the Lua request cannot be sent or the host disappears, do not block: keep plugin status hidden until a matching later acknowledgement, which is safer than displaying old-account quota.
   - Document `ProviderChanged` data as `{provider_id, provider, model}`. It is global and does not imply focus changed.

3. Make `ProviderUsageCoordinator` wholly event-loop-owned.
   - It owns canonical state, request generations, the one physically active fetch, active waiters, and at most one pending fetch group.
   - `UiAction::UsageFetch { force, reply_tx }` is the only Lua-to-coordinator request. `maki.usage.get()` does not cross to the UI; it reads the Lua mirror.
   - An ordinary request joins the active request when identities match. The first forced request during an active same-provider request creates one pending forced group; further forced requests join it. Ordinary requests arriving after that join the pending group rather than causing a third call.
   - A forced caller never resolves from the older active result; it resolves from the forced follow-up.
   - At most one network call is physically active. A pending group starts only after the active completion is consumed.
   - On provider transition, resolve all active and pending old-identity waiters with `(nil, "provider changed during fetch")`. Keep the obsolete physical request marked active until its completion arrives and discard that result. A new-provider fetch requested meanwhile becomes the sole pending group and starts after the obsolete call finishes.
   - On event-loop shutdown, resolve all waiters with a documented UI-dropped error so no Lua task remains parked.
   - Provider endpoint failures and unsupported responses are canonical terminal `error`/`unsupported` states and return `(state, nil)`; only transport, transition, or shutdown failures use `(nil, err)`.

4. Define the transport-neutral usage snapshot in `maki-lua`, using only primitives so `maki-lua` need not own provider objects.
   - Every state contains required string fields `{status, provider_id, provider, model}` where `status` is `"loading"`, `"ready"`, `"unsupported"`, or `"error"`.
   - Initial provider installation publishes `loading` before the first TUI focus event; in this state, loading means quota is unresolved/invalidated and does not imply a request is already active.
   - `error` additionally contains required string field `error`. `ready` always contains `limits` as a sequence, possibly empty, and optionally contains string `plan`.
   - Each limit always contains `window`. Optional `percentage` (number), `reset_at_ms` (number), and `detail` (string) keys are omitted when absent and therefore read as nil in Lua. `reset_at_ms` is Unix epoch milliseconds.
   - Window is explicitly tagged: `{kind="hours", value=N}`, `{kind="days", value=N}`, `{kind="monthly"}`, `{kind="weekly", model=nil|string}`, `{kind="credits"}`, `{kind="subscription"}`, or `{kind="other", label=string}`.
   - Preserve `ProviderUsage.plan` and every `UsageLimit` field; Rust does not preformat labels or relative times.

5. Add `maki.usage` in `maki-lua/src/api/usage.rs`.
   - `maki.usage.get()` synchronously returns `(current_snapshot, nil)` from the mirror and performs no I/O.
   - `maki.usage.fetch({force = boolean})` is async, publishes/observes loading, and returns the terminal snapshot for its request group as `(snapshot, nil)` or a runtime transport/transition error as `(nil, err)`.
   - `maki.usage.on_change(callback)` registers a plugin-owned synchronous callback and returns an idempotent unsubscribe function. The callback receives the new snapshot and must not yield; async follow-up is scheduled with `maki.async.run`.
   - Invalid argument/table types throw as programmer errors. Runtime failures follow the project pair convention.
   - Store callback registry keys by plugin. Clear them on unload/reload. Invoke only after canonical state equality changes; update the mirror before callbacks and isolate each callback error.

6. Add the reverse event-loop-to-Lua path.
   - Extend `Request`/`EventHandle` with a typed `ProviderUsageChanged { snapshot, invalidation, ack }` message. The invalidation token identifies the provider identity/generation being installed or mutated. An acknowledgement is required for invalidations and optional for routine refresh publications.
   - The Lua dispatcher updates the mirror, invokes current subscriber callbacks on the Lua thread under an isolated delivery scope, then reads the `StatusContentWriter` publication generation after all callback-driven clears and acknowledges `{invalidation, status_generation}`. No network task or event-loop thread calls `mlua::Function` directly.
   - The event loop publishes this request for loading, ready, unsupported, error, and provider invalidation states only when the canonical snapshot changes. Invalidations set the status-content render gate before enqueueing. A matching acknowledgement is allowed to re-enable plugin status only after every App cache is synchronously reconciled to at least `status_generation`; stale acknowledgements are ignored. A failed/disconnected send leaves content suppressed without blocking the TUI.

## Phase 2: Expose focused-session accounting without repricing

1. Add `SessionRequest::Usage` and `maki.session.usage()`.

2. Handle it synchronously in `maki-ui/src/event_loop.rs::handle_session_request` from the focused runtime and return:
   - `total = {input, output, cache_creation, cache_read, cost}` from `SessionState::token_usage` and its already-settled `SessionState::cost`.
   - `models = { {model, input, output, cache_creation, cache_read, cost}, ... }` from `AppSession::usage_by_model()` / `StoredTokenUsage`.
   - `cost` is `number|nil`; nil/absent means unpriced legacy data and must never become zero.
   - Sort models by total recorded tokens descending, then model spec ascending as the deterministic tie-breaker.

3. Return recorded cost unchanged. Do not call `model_cost`, `session_cost`, `settle_session`, or another pricing helper in this API or the plugin. Do not mutate the session, reconstruct a provider, or make provider requests.

## Phase 3: Add generic styled status content

1. Implement `StatusContentStore`, writer, reader, and generation snapshot alongside the hint implementation.
   - Store `BTreeMap<plugin_id, Vec<(text, style_name)>>` for deterministic owner order.
   - `nil`, an empty list, or a list reduced to empty after dropping empty text clears only the caller’s entry.
   - Publish an `ArcSwap` snapshot with an increasing generation only when content changes.
   - Clear and publish on plugin unload/reload, mirroring `HintStore` cleanup.

2. Expose `maki.ui.set_status_content({ {text, style_name}, ... })`.
   - Require both fields to be strings; malformed shapes throw a programmer error.
   - Keep names unresolved in the snapshot. Resolve through `theme::style_by_name` on every TUI render so theme changes apply immediately; unknown names use the existing `Style::default()` fallback.
   - Keep `set_status_hint` unchanged for key-label hints.

3. Thread `StatusContentReader` through `LuaThread`/`PluginHost`, TUI setup context, `RuntimeContext`, and each `App`, parallel to `HintReader`.
   - Seed `Watch` from the initial snapshot and poll it in `App::tick()`.
   - A changed generation owes a frame; polling the same generation remains clean.

4. Render status content after Rust-owned context and cost spans with deterministic spacing and bounded priority.
   - Flatten plugin owners in `BTreeMap` order with one space between non-empty contributions.
   - Rust context/cost remains mandatory. Reserve a named minimum tail width for the model when possible; plugin content receives only the remaining width and is display-width clipped while preserving span styles. If no width remains, hide plugin content rather than breaking layout or crowding out all model identity.
   - Honor the event loop’s provider-invalidation render gate: while an invalidation acknowledgement is pending, render no plugin status content for any App. Preserve existing behavior when mandatory Rust spans alone exceed the terminal width.

## Phase 4: Build and bundle the Lua usage plugin

1. Add `plugins/usage/init.lua`, include it in `maki-lua/src/loader.rs`, and add it to the normal built-in set.

2. Gate activation on the first `SessionFocusChanged` autocmd.
   - Top-level load only registers `/usage` and autocmds. It does not subscribe to usage, fetch, or register a timer.
   - First focus registers one usage subscription, starts one 30-second recurring `maki.timer`, and schedules an immediate ordinary fetch with `maki.async.run`.
   - Timer callbacks call `maki.usage.fetch({force=false})`; coordinator coalescing is the overlap guard.
   - Later `SessionFocusChanged` events update modal-local session data only and do not fetch global quota.
   - On `ProviderChanged`, synchronously clear old presentation, then schedule an ordinary fetch with `maki.async.run` when activated.

3. Define unload behavior to match existing timer semantics.
   - Unload removes the recurring schedule, subscriber keys, autocmds, command, and status content. No future manual/scheduled timer fire is possible.
   - A timer callback or fetch already running at unload may finish and update the process-wide canonical cache, but removed subscriber keys prevent it from republishing that plugin’s status/modal content. Do not claim stronger cancellation without changing timer task ownership.

4. Own compact status rendering in Lua.
   - The synchronous usage callback reads only its snapshot and updates status content or modal dirty state; it never performs an async round trip directly.
   - Render ready limits with meaningful percentage data, preserving compact hours/days/week/month/credits/subscription/other distinctions and named styles such as `accent` and `dim`.
   - Clear for unsupported and ready-with-no-percentage-lanes.
   - Retain the last useful same-provider ready spans while a routine refresh is loading or errors, but always clear when `provider_id` changes.

5. Implement `/usage` as a TUI-only focused Lua float.
   - Create the buffer/window before starting I/O, render an immediate placeholder from `maki.usage.get()`, then launch forced quota fetch and focused-session read as generation-scoped `maki.async.run` tasks.
   - Maintain one module-level modal instance token/generation. Reinvocation closes/replaces the prior float and increments the generation. Every async completion checks that the same generation is still open before mutating buffers.
   - Render provider plan, limits, percentages, reset-relative times, details, loading/unsupported/error states, total token/cache rows, optional recorded total cost, and deterministic per-model rows. Render nil cost as a dim `unpriced` marker, never `$0`.
   - Port compact token, window label, relative reset, and cost formatting into small tested Lua helpers. Remove duplicate Rust presentation helpers.
   - Handle Esc/Ctrl+C/close, scrolling via `plugins/lib/maki/scroll.lua`, resize, and `Ctrl+R` in `win:recv(timeout)`; Ctrl+R schedules a forced fetch and keeps the float open.
   - The usage subscription rerenders provider data from the local mirror without yielding. `TurnEnd`, `TurnError`, and `SessionFocusChanged` callbacks synchronously capture the current modal generation and enqueue `maki.session.usage()` through `maki.async.run`; late replies after close/reopen are discarded.
   - A parked `win:recv()` must not block reverse usage notifications or async session completions; they update the shared buffer directly on the Lua dispatcher.

## Phase 5: Remove superseded Rust presentation and command ownership

1. Delete `maki-ui/src/components/usage_modal.rs` and remove its module export.

2. Remove from `maki-ui`:
   - `UsageModal`, `UsageFetchState`, per-`App` `usage_slot`, `usage_readout_watch`, initialization, overlay registration, polling, rendering, scrolling, key handling, and Rust-modal tests.
   - `Action::RefreshUsage` and its dispatcher branch.
   - `refresh_usage`, `refresh_usage_into`, startup quota refresh, and old per-session model/provider refresh calls.
   - `App::usage_readout()`, the native status-bar quota field, and Rust compact quota formatting.
   - The native quota-only `usage_color` endpoints/helper/tests when removal search confirms no unrelated caller remains.

3. Remove the standard command surfaces:
   - `BuiltinId::Usage`, `BuiltinOperation::ToggleUsage`, and the `/usage` entry in `maki-commands/src/spec.rs`.
   - The matching `maki-agent/src/command.rs` mapping and `maki-ui` built-in dispatch branch.
   - Replace old command tests with bundled-plugin registration/dispatch tests.

4. Retain provider-neutral quota data and provider fetch implementations.
   - Keep `ProviderUsage`, `UsageLimit`, `UsageWindow`, `Provider::fetch_usage()`, and provider parser/eligibility tests.
   - Remove `UsageWindow::label()` and `short()` if searches confirm their only callers were the deleted Rust UI; Lua owns that vocabulary.

5. Add a named architecture regression test, `usage_plugin_migration_has_no_rust_ui_owners`, under the existing architecture-test pattern. It must fail if removed modal/action/readout/built-in symbols or presentation-only window helpers remain, fail if the standard command table still owns `/usage`, and assert the usage plugin is bundled. This makes removal executable rather than review-only grep.

## Phase 6: Documentation, generated artifacts, and validation

1. Add rustdoc/Lua-doc annotations for the exact `maki.usage` state tables and callback yieldability, `maki.session.usage`, `maki.ui.set_status_content`, `ProviderChanged`, and the bundled plugin/command.

2. Run `just gen-docs` and commit generated Lua API, plugin, autocmd, and command references. `/usage` must appear only under its plugin owner.

3. Format Rust/Lua and run the scoped then workspace checks listed in Test Strategy. No new dependency is expected; run `nix flake check` only if dependency/flake metadata changes.

# Acceptance Criteria

- **AC.1** `maki.usage.get()` and `fetch()` expose the exact documented provider-tagged loading/ready/unsupported/error schema, including plan, every limit field, all window variants, and epoch-millisecond reset values.
- **AC.2** At most one provider quota network call is active; ordinary callers coalesce, forced callers share exactly one follow-up, stale/transitioned and shutdown waiters always resolve, and no obsolete completion publishes.
- **AC.3** Provider identity changes on every installation, every successful tracked auth reload/refresh, and successful key rotation; initial, model, loaded-session, discovery, credential-refresh, and account-mutation paths suppress old plugin status immediately and cannot expose stale quota in any later frame/event or completion, including ack-after-poll and stale-ack interleavings.
- **AC.4** Event-loop canonical changes reach the Lua mirror in order; subscribers run on the Lua thread only after real changes, are error-isolated, and are removed on plugin unload.
- **AC.5** `maki.session.usage()` returns focused totals and deterministically ordered per-model counters with recorded `number|nil` costs unchanged and performs no provider I/O or repricing.
- **AC.6** `maki.ui.set_status_content` is plugin-scoped and deterministic, repaints only on generation change, resolves styles at render time, falls back safely for unknown styles, preserves Rust context/cost and minimum model space at narrow widths, and clears on unload.
- **AC.7** The bundled plugin produces no quota action or timer before first TUI focus; afterward it performs one immediate fetch and owns one 30-second timer. Unload removes future fires/subscriptions/content while safely tolerating an already-running callback.
- **AC.8** Compact Lua status presentation shows meaningful ready lanes, clears for unsupported/empty/new-provider states, and retains only a useful same-provider readout through routine loading/error transitions.
- **AC.9** `/usage` is a bundled TUI-only Lua command whose focused float shows provider quota plus total/per-model local token, cache, and optional recorded-cost accounting and handles close, scroll, resize, and Ctrl+R.
- **AC.10** Turn/focus changes refresh modal-local accounting without provider I/O; usage notifications update while `recv()` is parked; late session/quota work cannot mutate a closed or replaced float.
- **AC.11** Rust modal/meter/poller/action/standard-command and presentation-only helpers are absent, the plugin owns `/usage`, and provider quota contracts/implementations remain intact.
- **AC.12** Deterministic coordinator, scripted UI/window, manual timer, fake provider, and status-content test seams exist behind test support and pass without timing sleeps or network calls.
- **AC.13** Generated references are current, and all scoped/workspace checks pass without warnings or regressions.

# Test Strategy

Use controlled channels and explicit completions for all ordering tests. Do not use wall-clock sleeps for coalescing, stale-result, timer, or modal-generation behavior.

| Acceptance criterion | Named test(s) and layer |
|---|---|
| AC.1 | `maki-lua` serialization tests `usage_state_serializes_all_variants`, `ready_usage_preserves_plan_and_limit_fields`, `usage_window_tags_are_stable`, and `usage_get_reads_initial_provider_loading_mirror`; API round-trip `usage_fetch_returns_terminal_snapshot`. |
| AC.2 | Pure coordinator tests `ordinary_fetches_coalesce`, `forced_fetch_queues_one_follow_up`, `repeated_forced_fetches_share_one_follow_up`, `ordinary_after_forced_joins_pending`, `stale_active_waiters_resolve_on_transition`, `stale_pending_waiters_resolve_on_second_transition`, `obsolete_completion_is_discarded`, and `shutdown_resolves_usage_waiters`; thin adapter test `fetch_completion_reenters_event_loop`. |
| AC.3 | Provider-slot/event-loop tests `initial_install_has_provider_identity`, `model_change_uses_transition_pipeline`, `loaded_session_replacement_uses_transition_pipeline`, `model_discovery_replacement_uses_transition_pipeline`, `provider_refresh_uses_transition_pipeline`, `same_model_reconstruction_changes_instance_generation`, `successful_reload_and_refresh_bump_auth_generation`, `successful_key_rotation_bumps_auth_generation`, `failed_auth_and_false_key_rotation_keep_generation`, `auth_generation_bump_before_wake_rejects_racing_completion`, `auth_mutation_wake_invalidates_before_render`, `provider_transition_cannot_render_old_usage_content`, `auth_mutation_cannot_render_old_usage_content`, `ack_after_app_poll_cannot_render_pre_invalidation_content`, `stale_invalidation_ack_does_not_reopen_render_gate`, and `disconnected_lua_keeps_status_suppressed_without_blocking`; executable audit `usage_capable_providers_have_no_untracked_auth_self_mutations`. |
| AC.4 | Lua runtime tests `provider_usage_change_updates_mirror_before_callbacks`, `unchanged_usage_does_not_notify`, `usage_subscriber_errors_are_isolated`, `usage_unsubscribe_is_idempotent`, and `plugin_unload_removes_usage_subscribers`; event-order test `invalidation_precedes_provider_changed_autocmd`. |
| AC.5 | Event-loop/API tests `session_usage_returns_totals_and_per_model_records`, `session_usage_orders_equal_totals_by_model`, `session_usage_preserves_unpriced_cost_as_nil`, `session_usage_keeps_stored_cost_when_current_price_differs`, and `session_usage_does_not_fetch_provider`; float assertion `unpriced_cost_renders_without_zero_dollars`. |
| AC.6 | Store tests `status_content_is_deterministically_ordered`, `status_content_clear_is_plugin_scoped`, `status_content_unload_removes_owner`, `status_content_same_value_does_not_publish`, and shape-validation cases; TUI tests `status_content_publish_owes_one_frame`, `same_generation_owes_no_frame`, `status_content_resolves_style_after_theme_change`, `status_content_unknown_style_uses_default`, `status_content_renders_after_context_and_cost`, and `status_content_narrow_width_preserves_layout`. |
| AC.7 | Real-host tests `usage_waits_for_tui_focus_event`, `first_focus_starts_one_fetch_and_one_timer`, `later_focus_does_not_fetch_quota`, `usage_timer_has_thirty_second_interval`, and `plugin_unload_removes_schedule_subscription_and_content`; race test `already_started_timer_fetch_after_unload_cannot_republish_plugin_content`. |
| AC.8 | Pure Lua plugin tests `ready_lanes_publish_status_content`, `all_window_kinds_have_consistent_labels`, `unsupported_and_empty_ready_clear_status`, `routine_loading_and_error_keep_last_same_provider_readout`, and `provider_change_clears_old_readout`. |
| AC.9 | Registry/integration tests `usage_command_is_bundled_and_tui_only`, `usage_command_opens_float_with_quota_and_session_rows`, `usage_float_ctrl_r_forces_refresh`, and `usage_float_scroll_resize_and_close`; structured buffer assertions cover plan/detail/reset, cache rows, deterministic model order, and optional cost. |
| AC.10 | Scripted-host tests `usage_change_renders_while_window_recv_is_parked`, `turn_end_rereads_session_usage_without_fetch`, `focus_change_rereads_session_usage_without_quota_fetch`, `close_discards_late_session_reply`, and `reopen_discards_older_forced_fetch_and_session_reply`. |
| AC.11 | Architecture test `usage_plugin_migration_has_no_rust_ui_owners`, positive `usage_command_is_bundled_and_tui_only`, provider contract test `provider_usage_contract_remains_available`, and existing representative Anthropic/OpenAI/Synthetic/Z.AI/catalog fetch/parser tests. |
| AC.12 | Infrastructure tests `coordinator_accepts_controlled_completion`, `scripted_ui_harness_replies_and_delivers_window_events`, `manual_timer_fire_runs_callback_without_wall_clock`, `timer_inspection_reports_owner_and_interval`, `fake_provider_exposes_call_and_completion_control`, and `status_content_writer_pair_publishes_snapshot`. |
| AC.13 | `just gen-docs-check`; cheapest first: `cargo check -p maki-lua --tests`, `cargo check -p maki-ui --tests`, `cargo check -p maki-agent --tests`, `cargo nextest run -p maki-lua`, `cargo nextest run -p maki-ui`, relevant `maki-agent`/`maki-providers` scoped tests, `just lint`, and `just test`. Run `nix flake check` only if dependency/flake metadata changes. |

# Review Strategy

Before handoff, run `plan-reviewer`; resolve every critical/high finding and re-review until none remain.

After implementation and all automatable checks, dispatch `nat-code-reviewer` in change-review mode, focused on:

- coordinator ownership, waiter resolution, and completion-channel races;
- provider installation and auth-generation coverage, including discovery and agent key rotation;
- Lua mirror/subscriber thread confinement and unload cleanup;
- headless silence and timer in-flight semantics;
- modal generation checks around parked `recv()`, close, and reopen;
- unchanged optional historical cost semantics;
- status-content width/style/order behavior;
- removal completeness and generated-doc drift.

Fix or explicitly rebut all findings. Re-run review after any critical finding until none remain.

# Documentation Strategy

Update generated reference documentation through existing annotations and `just gen-docs`:

- exact Lua API schemas for `maki.usage`, `maki.session.usage`, and `maki.ui.set_status_content`;
- synchronous/non-yielding `on_change` callback semantics and use of `maki.async.run`;
- `ProviderChanged` event payload and ordering relative to usage invalidation;
- built-in `usage` plugin and TUI-only `/usage` command.

No new hand-written guide is needed: these are reference-level plugin/API contracts. Keep one canonical generated entry per surface and link rather than duplicating prose.

# Risks, Blockers, and Required Decisions

- **Provider mutation coverage:** A wrapper observes trait calls made through the installed provider, including current UI reload, agent refresh, and key rotation paths. Direct provider self-mutation must be audited; a usage-capable bypass must be connected to the generation notifier before claiming account safety.
- **Obsolete physical request:** Provider fetch futures are not assumed cancellable. A transitioned request may keep the single network slot until it completes, but its waiters resolve immediately with a transition error and its result is discarded. A new-provider request queues behind it.
- **Callback yieldability:** Usage subscribers and autocmds are synchronous. They may update local buffers/state and enqueue `maki.async.run`; they must not await `maki.session.usage()` or `maki.usage.fetch()` directly.
- **Timer unload semantics:** Existing timer tasks already started at unload are allowed to finish. Correctness depends on generation checks and removed subscriber/content ownership, not impossible retroactive cancellation.
- **Global versus focused state:** Provider quota follows the global provider slot. Only local accounting follows session focus.
- **No unresolved blocker or operator decision remains:** the revised design specifies coordinator ownership, reverse messaging, stale waiter outcomes, account generations, async callback handling, width priority, optional costs, and the deterministic harness needed to verify them.

