# Goal

Replace the current lossy command-to-ACP behavior with frontend-neutral session state, operation, feedback, and isolated-turn contracts so ACP and the TUI execute commands with the same semantics and expose every representable state change. Ship a default bundled `/options` Lua plugin for TUI discovery while keeping ACP schema and picker presentation outside the core state model.

# Implementation Summary

The implementation will keep `maki-commands` focused on command discovery and dispatch, move persistent session-option ownership into `maki-agent`, and adapt that state independently in `maki-acp`, `maki-lua`, and existing TUI controls. A global session-option catalog will contain built-in and plugin-owned definitions. Each live session will be owned by one agent-side `SessionCoordinator` actor, addressed by stable session ID, with a serialized mailbox for mutations and read-only versioned snapshot handles for projection. Model/provider adoption, permissions, Workflow, cwd, option values, history replacement, and acknowledged persistence will execute through that actor; ACP, Lua, and the TUI must not mutate their former mirrors or channels directly.

Slash commands, ACP `session/set_config_option`, existing TUI controls, and the new bundled `/options` plugin will all submit the same coordinator operations and await typed acknowledgements. A shared coordinator directory maps live session IDs to mailboxes for explicit Lua/tool targeting, removes entries on close, and returns stale-session errors after teardown.

The work deliberately keeps separate contracts for separate lifecycles:

- persistent selectable state: agent-owned option definitions, per-session values, full snapshots, and subscriptions;
- transient frontend feedback: typed successful results such as canonical cwd confirmation;
- local asynchronous work: attributable compaction progress and completion;
- primary model work: the existing normal agent turn;
- isolated model work: a new frontend-neutral one-turn provider service used by `/btw`.

Primary touch points are `maki-commands/src/{spec,dispatch}.rs`, `maki-agent/src/{command,headless,types}.rs` plus new focused agent modules, `maki-acp/src/{server,methods,translate}.rs`, `maki-lua/src/{runtime,loader,api}.rs`, `maki-storage/src/sessions.rs`, current TUI command/control paths, `plugins/bash`, and a new bundled options plugin. ACP schema types must remain confined to `maki-acp`; Lua callback objects must remain confined to `maki-lua`.

Confirmed product decisions:

- ACP does not advertise or execute `/new` or `/clear`; TUI behavior remains unchanged.
- Model, cwd, YOLO, Fast, Workflow, and Bash auto mode persist per session. Old sessions use safe defaults for newly stored values.
- Fast always exposes Enabled and Disabled; enabling it on an unsupported model fails without changing state.
- Plugin replacement preserves compatible per-session values transactionally, rejects stale handles, and retains the previous valid registration if replacement validation fails.
- The TUI option picker ships as a default bundled `/options` plugin without a keybinding.
- No client-specific Zed protocol extension, transcript clearing, or displayed-directory mutation is in scope.

# Implementation Plan

## Phase 1: Correct command availability and execution contracts

1. Split `TargetCapability::SessionControl` in `maki-commands/src/spec.rs` into semantic capabilities that distinguish history compaction from session replacement. Assign `/compact` to the portable compaction capability and `/new` plus `/clear` to session replacement. Update `TargetCapabilities::ALL`, tests, and all target capability declarations.
2. Give the TUI target both capabilities but give `maki_agent::command::portable_capabilities()` only compaction. Preserve the registry invariant that projection and dispatch use the same capability filter: ACP initial/dynamic projections omit both `/new` and `/clear`, and manually typed unavailable forms follow the registry's unknown-command policy. (That policy changed under this branch: see [Merge with the slash-command parsing change](#merge-with-the-slash-command-parsing-change).)
3. Extend `CommandOutcome`/`HostResponse` only with narrow execution distinctions required by frontends: retain local completion, retain the primary `AgentTurn`, add an explicit isolated-turn request for `/btw`, and add a typed frontend-feedback result for successful cwd changes. Do not add a generic vector or union of arbitrary command effects.
4. Keep command metadata and dispatch in `maki-commands`; put provider-, permission-, storage-, and session-specific behavior in `maki-agent`.

## Phase 2: Establish the authoritative session coordinator and option state

1. Add an agent-side `SessionCoordinator` actor and handle. The actor owns the live provider/model, mutable history, canonical cwd, permission state, Workflow/Fast values, session-option values, and storage checkpoint handle. Its mailbox serializes all mutations and returns typed acknowledgements. A read handle exposes immutable versioned snapshots without allowing mutation.
2. Make `Agent` an operation-scoped coordinator-internal execution worker rather than a second session owner. To start a primary turn, the coordinator grants an exclusive execution lease that transfers access to the current provider/model and mutable history into the worker; while leased, external ordinary mutations queue and only cancellation/control messages may interrupt. Completion returns the resulting history/provider state to the coordinator, which checkpoints and publishes it before releasing the lease. Manual compaction and history replacement use the same exclusive lease. Model changes cannot commit against an in-flight old-model turn; their serialized operation runs after cancellation/completion. Tool execution receives the lease’s committed session snapshot and explicit session ID.

   Define lease reentrancy explicitly: a tool/background callback executing inside a session lease may read its committed snapshot, but a mutating request targeting that same session must return an immediate typed `SESSION_BUSY` error and must never enter the ordinary queue or await lease release. Mutations targeting another live session use that session’s coordinator normally. Command/front-end mutations issued outside the lease may queue. Add a deterministic provider-tool test proving same-session `set_option` fails promptly without deadlock and the turn has one terminal response; Bash classification only reads its session value and is unaffected.
3. Replace the process-global `SessionMailbox` registry with one canonical live-session coordinator directory keyed by stable session ID. Preserve `SessionMailbox::notify` as a compatibility facade that resolves the coordinator and submits a typed notification operation; it must not retain separate ownership or registration lifetime. Define duplicate-ID rejection, registration at new/load, last-handle behavior, explicit close/unregister ordering, and stale errors for late command, Lua, tool, and notification requests.
4. Move model transitions, permission changes, Workflow/Fast changes, cwd changes, history replacement/compaction, notifications, and persistence requests behind coordinator operations. ACP and TUI code must stop sending directly to `model_tx`, toggling `PermissionManager`, or writing option mirrors. Existing event/input channels become private coordinator-to-worker implementation details.
5. Define operation serialization and failure semantics: one mutation or execution lease is active at a time; reads identify the committed snapshot version they observe; shutdown rejects new mutations, cancels/drains the active lease, acknowledges the final checkpoint, unregisters the session, and then closes runtime channels. Add two-session, active-turn/mutation, active-turn/compaction, duplicate-registration, close, and stale-routing integration tests before adapting frontends.
4. Add a focused module such as `maki-agent/src/session_options.rs`. Define ACP-free types for:
   - stable option ID and owner (`Builtin` or plugin generation);
   - name, description, category (`Model` or `Mode`), ordered selectable values, and persistence declaration;
   - ordered full snapshot containing definitions and the current values for one session;
   - domain errors for unknown IDs, invalid values, unavailable owners, policy rejection, unsupported Fast, callback failure, and stale handles;
   - version/change subscription that wakes only after a committed definition or value transition.
2. Separate global definitions from per-session values. The Lua VM is shared by concurrent TUI sessions, so plugin registration must never carry one global `current_value`. Each live session gets a `SessionOptions` instance/value map initialized from persisted state or definition defaults. The global catalog supplies definitions and plugin setter behavior.
3. Use stable built-in IDs and order: `model`, `yolo`, `fast`, `workflow`, then plugin options in deterministic registration order, including `bash.auto_mode`. Reserve unqualified IDs for core; require plugin IDs to be namespaced by the owning plugin (for example `bash.auto_mode`); reject cross-owner collisions and invalid IDs at registration.
4. Implement explicit, typed built-in setters as coordinator operations behind the generic lookup surface:
   - Model validates `ModelPolicy`, constructs the candidate model/provider, requests runtime adoption, and waits for an acknowledgement. Only then does the coordinator commit the model spec, atomically clear Fast when unsupported, checkpoint the result, and publish one snapshot. Provider construction failure, adoption rejection, channel closure, or checkpoint failure leaves the prior committed snapshot active; the runtime adoption protocol must either remain reversible until commit or acknowledge only after it can honor coordinator rollback.
   - YOLO commits through coordinator-owned permission state and an explicit `PermissionManager` setter rather than toggle-only mutation.
   - Fast validates the coordinator’s current model support before committing Enabled; Disabled always succeeds. Serialized mailbox handling defines deterministic order for concurrent Model/Fast requests.
   - Workflow updates the committed value consumed when constructing future `AgentInput` and tool definitions.
   - A transaction publishes one snapshot only after underlying runtime adoption and the required acknowledged checkpoint succeed. Failed operations publish nothing and return a domain error.
5. Convert `/model`, `/yolo`, `/fast`, and `/workflow` into conveniences that read the invoking session’s current value and call the same explicit setters used by frontends. Preserve current slash-command parsing, fuzzy model resolution, policy errors, and Fast validation.
6. Replace `SessionCommandState` mirrors with the new session state/option handles. Ensure normal command context, normal turn construction, permissions, and model channels read committed values from that single owner.
7. Keep Fast’s definition stable with both `enabled` and `disabled` values for every model. An incompatible model transition atomically sets Fast to disabled; a direct request to enable it returns the shared `FAST_UNSUPPORTED` domain error.

## Phase 3: Persist and restore every projected option

1. Extend `maki-storage/src/sessions.rs::SessionMeta` with safe, serde-defaulted fields for YOLO and persisted plugin session-option values. Continue using the existing header fields for Model/cwd and existing `SessionMeta.fast`/`workflow` fields. Use an ordered or deterministic serialized representation for plugin values and ignore/remove values whose definitions are unavailable at load without corrupting the session.
2. Define backward-compatible defaults: old sessions restore YOLO disabled unless an explicit session value exists; Fast and Workflow continue defaulting false; Bash auto mode defaults from its plugin configuration when no persisted value exists. A process/config YOLO request may seed a new session, but a stored session value wins when loading that session.
3. Update headless/ACP session setup and load in `maki-agent/src/headless.rs` and `maki-acp/src/server.rs` to restore Model, cwd, YOLO, Fast, Workflow, and available persistent plugin values before initial option snapshots are returned. Remove `RESTORED_FAST` and the forced false Workflow path.
4. Add a dependency-safe acknowledged checkpoint abstraction in `maki-storage` (a per-session writer handle/trait plus request and acknowledgement types) that understands session revision/epoch semantics but not UI state. Inject one implementation into every `SessionCoordinator` construction path: synchronous/background storage for headless and ACP, and a TUI adapter that preserves current coalescing behavior. The coordinator’s authoritative checkpoint operation writes current history, model/cwd header fields, and option metadata and returns success only after the implementation acknowledges the session log save. Refactor `maki-ui/src/storage_writer.rs` into an adapter over this abstraction rather than a second authority; coalescing must retain and complete every accepted acknowledgement, and shutdown drains all accepted checkpoints before reporting completion.
5. Define mutation durability: option/model/cwd state is not published as committed until its required checkpoint succeeds. Stage candidate state, write it through the acknowledged checkpoint, then expose it to snapshots/runtime observers; if a runtime transition must occur first, retain enough prior state to compensate on checkpoint failure. Do not wait for a later normal turn: selector and slash-command changes must survive immediate shutdown/reload.
6. Update TUI checkpoint construction in `maki-ui/src/app/session.rs` and restore in `maki-ui/src/app/session_state.rs` to use coordinator snapshots while preserving existing mode, thinking, drafts, queue, rules, and pricing behavior. Existing specialized TUI controls and indicators become read/adapt surfaces over the coordinator; do not retain separate mutable option fields that can diverge.
7. Add storage/checkpoint tests for coalesced acknowledged writes, save failure, immediate close after an option change, shutdown while a save is active, and final reload state. Use deterministic channels/fake storage boundaries rather than sleeps.

## Phase 4: Add deterministic ACP orchestration tests and project full snapshots

1. Before changing ACP behavior, refactor `maki-acp/src/server.rs` around a production `ServerDeps`/`SessionFactory` boundary used by the normal request loop and by tests. Inject session/coordinator construction, provider/runtime event sources, acknowledged storage, model discovery, MCP startup, cancellation handles, and the output sink; production adapters retain current stdin/stdout/provider/storage behavior. Build a deterministic test harness with fake implementations and a protocol collector that drives the same request dispatch path for new/load, normal prompts, local operations, isolated turns, cancellation, and close without real providers, stdio, clocks, or sleeps, and asserts exact wire order and terminal counts.
2. Require every ACP session-bearing request and notification (`prompt`, `set_mode`, `set_config_option`, cancel, close-related paths) to extract and validate its `SessionId` through the active coordinator binding/directory before dispatch. Unknown, closed, or replaced IDs return a defined stale/resource error and never fall back to `srv.session`. Add old-session prompt, set-option, and cancel scenarios after new/load replacement.
3. Replace ACP’s single undifferentiated `pending.prompt` with a per-session operation coordinator. Each primary turn, manual compaction, and isolated turn receives an operation ID and owns a compare-and-set terminal state. Runtime events carry or are routed with that operation identity; only the owner can emit its terminal progress and `PromptResponse`. Serialize outbound state updates and terminal responses through this coordinator, including an explicit barrier that sends/acknowledges the committed `ConfigOptionUpdate` version before successful slash-command completion. Define cancellation, provider error, runtime channel closure, late events, and `close_session` transitions.
3. Add interleaving harness tests before feature adaptation: primary completion cannot finish compact/BTW, late Done cannot double-complete, cancellation races have one terminal response, close terminates each operation once, and a required snapshot update precedes prompt completion.
4. Replace `maki-acp/src/methods.rs::model_config_option` with an adapter from the agent snapshot to ordered ACP `SessionConfigOption` values. Map Model to ACP category `model`; map YOLO, Fast, Workflow, and Bash auto mode to `mode`. Use stable `enabled`/`disabled` values and human-readable labels.
2. Include the full ordered snapshot in `session/new` and `session/load` responses. The response must reflect restored committed values, not launch defaults.
3. Replace the model-only `handle_set_config` path with `SessionOptions::set(config_id, value)` for the active session. Return the resulting full snapshot in `SetSessionConfigOptionResponse` and use common domain-to-ACP error mapping. No ACP handler may send directly on `model_tx` or mutate permission/state mirrors.
4. Add a per-session snapshot watcher next to the existing available-command watcher. Emit `ConfigOptionUpdate` containing the complete ordered snapshot after successful slash-command changes, selector changes, model discovery, plugin definition registration/replacement/removal, and dependent changes such as Fast being cleared.
5. Model discovery updates only the Model definition’s selectable values, preserves all committed current values, and publishes one full snapshot. If the current model is absent from a transient discovery result, retain it as a selectable/current entry rather than creating an invalid ACP snapshot.
6. Enforce update ordering for slash commands: committed state and its `ConfigOptionUpdate` must be emitted before the final successful `PromptResponse`. Direct `session/set_config_option` returns its full snapshot after commit; watcher deduplication/versioning must prevent inconsistent duplicate snapshots.
7. Add explicit cleanup of option-watcher tasks in `close_session`, matching command-projection task cleanup.

## Phase 5: Expose transactional plugin-owned session options to Lua

1. Add a distinct Lua API module for `maki.api.register_session_option`; do not overload static `register_options`. Parse and validate ID, name, description, category, ordered non-empty unique values, initial value, persistence, and optional change callback as programmer-error boundaries.
2. Inject the agent option catalog/session bridge through `maki-lua::runtime::SpawnConfig` and Lua app data. Keep Rust agent definitions callback-agnostic by representing plugin operations through a narrow asynchronous adapter implemented by `maki-lua`; never make `maki-agent` depend on `mlua`. Separate stable plugin identity and option ID (used for persistence/compatibility) from an ephemeral generation token (used to reject stale handles).
3. Refactor plugin reload into an explicit prepare/commit transaction over the complete plugin generation. Execute replacement source in isolated pending stores/environment state and stage Lua registry keys, tools, permission rules, commands, jobs/hooks, and session-option definitions without calling `drop_plugin_keys` or mutating the active generation. Validate all staged registrations before touching active state.
4. Add a catalog transaction coordinator for cross-session definition replacement. It reserves the candidate generation, asks every live `SessionCoordinator` to prepare against a specific snapshot version, computes/validates candidate values, and holds those session reservations until commit or abort. A session close participates by aborting/removing its reservation before it unregisters; concurrent option sets queue behind the reservation. Only after every participant acknowledges prepare does the catalog issue one commit token that swaps the global generation and applies each prepared per-session state before any new snapshot is observable. Any prepare/checkpoint/publication failure aborts all reservations and leaves the complete old generation and all old session snapshots active.
5. Make option validators execute in a restricted Lua validation environment that exposes only the proposed value and pure normalization helpers, not filesystem, jobs, registration, session mutation, UI, tools, autocmds, or other side-effecting APIs. The Lua load coroutine itself orchestrates validation before returning from replacement: capture/clone the candidate generation’s function references and immutable per-session candidate data, release all app-data/coordinator borrows, run each validator as a directly spawned local-executor Lua thread/task, never through the request queue currently awaiting `LoadSource`, and collect typed results through a transaction-local channel. A transaction-owned cancellation token is triggered out of band by plugin unload or session close; cancellation aborts catalog prepare. After each result, recheck the candidate generation and reserved session snapshot version before prepare acknowledgement. Reload/set APIs are absent from the restricted environment; indirect reentrancy returns a typed validation error rather than queueing another load.
6. On successful replacement:
   - preserve each session’s current value if the new definition still accepts it;
   - otherwise choose the new initial/configured value;
   - run the new generation’s restricted validator for every changed effective value during prepare;
   - checkpoint persistent candidate values while reservations prevent competing commits;
   - if any validation, checkpoint, registration, or publication fails, abort the catalog transaction and retain the complete old generation;
   - expose one new catalog generation and one coherent definition/value snapshot per affected session after the commit token.
6. On explicit unload, remove the plugin definitions and their live values, retain persisted raw values only if needed for later compatible re-registration, publish updated snapshots, and invalidate generation-bound handles. Stale handles return `(nil, err)` and cannot reach a newer generation. Define renamed IDs as new options; changed value sets preserve only still-valid values; cross-plugin claims of a persisted ID are rejected.
7. Return a generation-bound Lua handle supporting `get` and explicit `set`. Command handlers may default to the invoking command’s session. Tool handlers and background/plugin code must pass an explicit session ID. Extend the permission-scope/classification callback context so every tool invocation carries its owning session ID; Bash must read only `ctx:session_id()` because the focused TUI session can differ from the tool’s session. Runtime failures follow the project `(value, err)` convention, with `(true, nil)` on successful sets.
7. Add generic `maki.session.options(opts?)` and `maki.session.set_option(...)` primitives for the focused or explicitly named live session. They return frontend-neutral snapshots and call the same setters. Under ACP/headless contexts without an interactive UI, explicit live-session targeting must work through a session registry/mailbox rather than incorrectly returning `NO_UI_ERR`.
8. Migrate `plugins/bash/init.lua` and `bash_helpers.lua`: register persistent `bash.auto_mode`, remove the global `auto_mode_on` source of truth, make `/automode` toggle through the handle, and make permission classification read the option for `ctx:session_id()`. Initialize sessions from `plugins.bash.auto_mode` only when no persisted value exists.

## Phase 6: Ship the bundled TUI `/options` plugin

1. Add `plugins/options/init.lua` plus pure helper/test modules following existing bundled plugin patterns. Register it in `maki-lua/src/loader.rs::BUNDLED_PLUGINS` and `maki_config::DEFAULT_BUILTINS` in sorted order. Ship no default keybinding.
2. Implement `/options` as two `maki.ui.open_list_picker` calls:
   - first picker reads the focused session snapshot and shows option name, current value, and description/category detail in deterministic snapshot order;
   - second picker shows the selected definition’s ordered values with the current value selected;
   - confirmation calls `maki.session.set_option` and reports errors through `maki.ui.flash` without optimistic local mutation.
3. Re-read the snapshot whenever the picker is opened and before applying a selected value, so plugin reloads or model-dependent Fast changes cannot make a stale row mutate the wrong definition. A stale/missing option closes or refreshes with a useful message.
4. Keep existing specialized model/thinking pickers and status controls. `/options` is an additional generic discovery surface, not a replacement or source of state.
5. Add a deterministic plugin test fixture that mocks `maki.ui.open_list_picker`, captures both picker specifications, supplies chosen/dismissed results, and backs `maki.session.options`/`set_option` with a versioned fake bridge. Use it to prove the second-stage snapshot re-read, current-value cursor, stale-definition handling, no optimistic mutation, and flash errors.

## Phase 7: Make cwd canonical, shared, and visibly confirmed

1. Change `InteractiveControl::ChangeDirectory` to reply with `Result<PathBuf, String>`. Route the request through the session coordinator: canonicalize and validate, stage the headless cwd, permission rules, session header, and shared live cwd, persist through the acknowledged checkpoint, then publish the canonical value and return it. On failure, compensate any staged runtime change and expose none of it.
2. Have `SessionCommandHost` use only the coordinator acknowledgement’s canonical path and produce typed frontend feedback. A failed change updates none of the committed values.
3. Replace the startup cwd captured by `maki-acp::start_event_pump` with the coordinator’s read-only committed cwd handle, read for each `ToolStart`/`ToolDone` translation. Serialization defines `/cd` versus `ToolStart`: events before commit resolve against the old path and events after commit against the new path; no event observes a staged path.
4. Translate successful cwd feedback to an ACP agent-message chunk such as `Working directory: /canonical/path`, then complete the prompt. Do not append it to provider history. Document that ACP cannot alter Zed’s displayed session directory field.

## Phase 8: Give manual compaction attributable progress and immediate persistence

1. Introduce a manual local-operation identity/lifecycle in `maki-agent` rather than inferring command ownership from ambient `AutoCompacting`/`CompactionDone` events. Keep automatic compaction behavior distinct.
2. On `/compact`, reserve the active ACP prompt, emit an attributable start event, run compaction, immediately checkpoint the replaced history and current model/options, then emit completion. A persistence failure makes the operation fail rather than reporting a durable success.
3. Translate the lifecycle in ACP to a stable `ToolCall`/`ToolCallUpdate` sequence titled `Compact context`: pending/in-progress, completed on durable success, or failed with the error. Complete or fail the ACP prompt only after the terminal progress update.
4. Route cancellation through the operation’s cancel token. Ensure cancellation/failure leaves a valid history and terminates the ACP prompt exactly once.
5. Do not claim that the visible ACP transcript was removed; progress text refers only to model context.

## Phase 9: Extract and route frontend-neutral isolated `/btw` turns

1. Add a focused agent service such as `maki-agent/src/agent/isolated_turn.rs`. It accepts provider/model, copied history, resolved system text, images, session ID, cancellation, and an output sink. The service itself closes dangling tool calls in the copy, appends the side-question reminder/question/images, adapts images, enforces an empty tool array, executes one provider request, and reports text/thinking/completion/error without returning history mutations.
2. Factor common Build-mode system-prompt preparation from TUI/headless code so both frontends can produce the same current prompt from cwd-sensitive instructions, live prompt slots, overrides/appends, modes, and current model. Recompute ACP’s isolated-turn system text at invocation rather than preserving a startup-only value. Keep `/btw` pinned to Build semantics as the TUI currently does.
3. Change the `/btw` builtin to return an isolated-turn request rather than `AgentTurn`. Preserve command image attachments.
4. Refactor `maki-ui/src/app/btw.rs` to call the shared service and continue rendering its modal; remove duplicated provider mechanics but preserve current TUI presentation.
5. Route ACP isolated turns through a dedicated pending operation, stream text as ACP agent-message chunks, handle thinking according to existing ACP thought/text policy, and complete/fail/cancel the active prompt exactly once. Never submit the isolated request to the primary `input_tx`.
6. Prove that the original primary history remains byte-for-byte unchanged after isolated success, provider failure, and cancellation, and that the next normal turn does not include the side question or answer.

## Phase 10: Documentation, client verification, and cleanup

1. Update handwritten ACP and command documentation (`site/docs/content/acp/_index.md`, `site/docs/content/commands/_index.md`) with representable effects, `/new`/`/clear` exclusion, full session-option behavior, `/cd` and transcript limitations, compaction progress, and isolated `/btw` semantics.
2. Document the Lua registration/handle API, persistence and reload rules, explicit session targeting, collision rules, and the bundled `/options` plugin in generated Lua API/plugin documentation inputs. Regenerate docs and keep one canonical explanation per topic.
3. Remove obsolete model-only ACP helpers, state mirrors, old Bash global state, `RESTORED_FAST`, and dead duplicated BTW provider code.
4. Perform a recorded Zed scenario after automated tests: capture initial/load config options, slash and direct selector updates, Fast clearing on model change, `/options` TUI behavior separately, `/cd` confirmation and subsequent tool locations, compaction progress, `/new` non-advertisement and its local guidance response, and isolated `/btw` output. Record that Zed’s displayed cwd and old transcript remain unchanged by protocol limitation.

# Acceptance Criteria

- **AC.1:** ACP initial and dynamic command projections omit `/new` and `/clear`, retain `/compact`, and a literal ACP `/new` neither clears primary history nor changes the session ID; TUI `/new` and `/clear` retain current behavior.
- **AC.2:** Every live session has an ordered option snapshot with IDs `model`, `yolo`, `fast`, `workflow`, and available plugin options such as `bash.auto_mode`; concurrent TUI sessions can hold different values without cross-session leakage.
- **AC.3:** Slash commands, existing TUI controls, ACP `session/set_config_option`, Lua handles, and `/options` use the same explicit setters; invalid IDs/values, policy failures, unsupported Fast, stale handles, and callback failures leave all state unchanged and return useful errors.
- **AC.4:** New/load ACP responses and every `ConfigOptionUpdate` contain the complete ordered snapshot with Model in category `model` and all other projected controls in category `mode`; successful slash-command updates arrive before prompt completion.
- **AC.5:** A model change and any required Fast disablement commit and publish as one coherent snapshot; Fast remains visible with both values and rejects Enabled for unsupported models.
- **AC.6:** Model, canonical cwd, YOLO, Fast, Workflow, and Bash auto mode survive session reload, including when changed immediately before shutdown and without a later normal model turn; old sessions load with safe documented defaults.
- **AC.7:** Plugin option registration, compatible replacement, incompatible replacement, unload, failed reload, callback failure, and stale handles follow the transactional generation policy and publish only committed full snapshots.
- **AC.8:** The default bundled `/options` command displays current session options and their values through Lua list pickers, changes the focused session through the shared setter, handles stale definitions/errors, and has no default keybinding.
- **AC.9:** `/automode` and `bash.auto_mode` selector changes affect Bash classification for the invoking/tool session only; two concurrent sessions may use different modes, and the persisted value overrides the plugin default on reload.
- **AC.10:** Successful `/cd` canonicalizes once, updates command context, the headless agent, permission rules, persistence, and later ACP relative tool locations to the same path, and emits visible non-history feedback; failure changes none of them.
- **AC.11:** ACP `/compact` emits attributable in-progress and completed/failed tool updates, terminates its prompt exactly once, and persists compacted history before success; reloading without a later turn uses the compacted history.
- **AC.12:** `/btw` uses copied history, closes dangling calls only in the copy, sends current provider/model/common Build system text/images with no tools, streams through the active frontend, handles failure/cancellation, and leaves primary history byte-for-byte unchanged.
- **AC.13:** Existing command precedence, custom Lua commands, MCP prompts, image preservation, unknown/unavailable slash handling, stale-target protections, TUI model/toggle/new/compact/cd/btw behavior, and session pricing/history persistence continue to pass.
- **AC.14:** A deterministic ACP server/session harness can inject provider/runtime events and cancellation, collect ordered wire messages, and prove terminal-response counts without real providers, stdio processes, clocks, or sleeps; generated documentation is current, and a recorded Zed verification demonstrates client-visible option snapshots/updates, compaction progress, cwd confirmation/tool locations, `/new` exclusion, and isolated `/btw`, while explicitly recording unsupported transcript and displayed-cwd changes.

# Test Strategy

Every named test below must be added or updated so it fails when the corresponding behavior is removed.

| Acceptance criterion | Named automated/manual coverage |
|---|---|
| AC.1 | `maki-commands`: `portable_target_separates_compaction_from_session_replacement`; `maki-acp`: `acp_projection_omits_session_replacement`, `literal_new_preserves_history_and_session_id`; `maki-ui`: `tui_new_and_clear_reset_session` |
| AC.2 | `maki-agent`: `session_option_values_are_isolated_per_session`, `snapshot_order_is_stable`; `maki-lua`: `shared_runtime_targets_distinct_session_values` |
| AC.3 | `maki-agent`: `all_frontends_share_explicit_option_setters`, `failed_option_set_is_atomic`, `model_adoption_failure_preserves_committed_state`, `model_channel_close_preserves_committed_state`, `checkpoint_failure_preserves_committed_state`, `concurrent_model_and_fast_requests_serialize`, `same_session_mutation_during_lease_returns_session_busy_without_deadlock`, `cross_session_mutation_during_lease_routes_normally`; `maki-acp`: `slash_and_selector_produce_same_state`, `acp_rejects_stale_session_ids_for_prompt_set_option_and_cancel`; `maki-lua`: `stale_handle_returns_error_pair` |
| AC.4 | `maki-acp`: `new_and_load_return_full_ordered_options`, `config_update_contains_full_snapshot`, `slash_update_precedes_prompt_response`, `model_discovery_preserves_non_model_options`, `plugin_reload_emits_one_full_config_update_per_affected_session`, `failed_plugin_reload_emits_no_config_update` |
| AC.5 | `maki-agent`: `model_change_atomically_disables_fast`, `unsupported_fast_enable_preserves_state`; `maki-acp`: `model_and_fast_publish_in_one_snapshot` |
| AC.6 | `maki-storage`/`maki-agent`: `projected_options_round_trip_session_storage`, `option_set_checkpoints_without_agent_turn`, `headless_and_acp_coordinators_use_acknowledged_checkpoint`; `maki-ui`: `old_session_defaults_projected_options_safely`, `tui_checkpoint_adapter_completes_coalesced_acknowledgements`, `shutdown_drains_all_accepted_checkpoints`; `maki-acp`: `load_restores_all_projected_options` |
| AC.7 | `maki-lua/src/loader.rs`: `compatible_reload_preserves_session_values`, `incompatible_reload_applies_initial_value_transactionally`, `failed_reload_retains_complete_previous_generation`, `unload_removes_owned_options`, `validator_failure_preserves_all_sessions`, `validator_side_effect_is_rejected_and_old_generation_is_untouched`, `reentrant_validator_is_rejected`, `validator_cancellation_aborts_prepare_without_dispatcher_deadlock`, `validator_unload_or_session_close_keeps_old_generation`, `renamed_option_is_new_identity`, `changed_values_preserve_only_compatible_values`, `cross_plugin_persisted_id_claim_is_rejected`, `reload_racing_session_close_is_atomic`, `reload_racing_option_set_is_serialized`, `prepare_checkpoint_failure_exposes_no_mixed_generation` |
| AC.8 | `plugins/options/tests/spec.lua`: `renders_options_with_current_values`, `opens_values_at_current_selection`, `sets_selected_value`, `reports_stale_option`, `reports_setter_error`; config/loader test `options_plugin_is_default_without_keybinding` |
| AC.9 | `plugins/bash/tests/spec.lua`: `automode_reads_explicit_session`, `automode_sessions_do_not_leak`, `automode_persisted_value_wins_over_default`; integration test `automode_command_and_selector_share_setter` |
| AC.10 | `maki-agent`: `change_directory_returns_and_commits_canonical_path`, `failed_change_directory_is_atomic`; `maki-acp`: `cd_feedback_is_visible_not_history`, `tool_locations_follow_live_cwd`; persistence reload test `canonical_cwd_round_trips` |
| AC.11 | `maki-agent`: `manual_compaction_persists_before_reporting_success`, `failed_persistence_reports_failure_and_rolls_history_back`, `a_failed_compaction_is_never_persisted`, `cancelled_persistence_reports_cancellation_not_failure`; `maki-acp`: `compact_tool_progress_success_sequence`, `compact_tool_progress_failure_sequence`, `compact_prompt_cancellation_completes_once`; reload test `compacted_history_loads_without_followup_turn` |
| AC.12 | `maki-agent::agent::isolated_turn`: `isolated_turn_uses_copy_and_empty_tools` (empty tools and the Build system text), `isolated_turn_closes_dangling_tool_calls_in_its_copy`, `isolated_turn_preserves_primary_history_on_failure`, `..._on_cancel`; `maki-acp`: `btw_streams_and_completes_active_prompt`, `isolated_btw_is_not_forwarded_to_primary_agent`. The primary history is untouched by construction rather than by assertion -- `IsolatedTurnRequest.history` is an owned `Vec<Message>`, so the turn has nothing to mutate |
| AC.13 | Retain or add individually named regressions: `command_precedence_prefers_expected_producer`, `registry_generation_invalidates_stale_target`, `custom_lua_command_dispatches_on_portable_target`, `mcp_prompt_preserves_reference_and_arguments`, `normal_command_preserves_image_attachments`, `unknown_slash_prompt_is_rejected`, `unavailable_interactive_command_is_rejected`, `escaped_slash_prompt_is_sent_literal`, `stale_session_option_handle_is_rejected`, `tui_model_selector_uses_coordinator`, `tui_toggle_controls_use_coordinator`, `tui_new_and_clear_reset_session`, `tui_compact_preserves_expected_presentation`, `tui_cd_uses_canonical_feedback`, `tui_btw_uses_shared_isolated_service`, `session_restore_preserves_pricing_and_history` |
| AC.14 | `maki-acp`: harness self-tests `collector_preserves_wire_order`, `operation_terminal_is_compare_and_set`, `fake_provider_drives_stream_without_stdio`, plus end-to-end scenarios `server_session_harness_drives_new_load_prompt_compact_and_btw_with_ordered_terminals` and `server_session_harness_close_cancel_races_complete_once`; `just gen-docs-check`; manual record `zed-acp-command-effects` containing the requested message/order and visible-behavior checks |

Run cheapest checks while iterating:

```text
cargo check -p maki-commands --tests
cargo nextest run -p maki-commands
cargo check -p maki-agent --tests
cargo nextest run -p maki-agent
cargo check -p maki-acp --tests
cargo nextest run -p maki-acp
cargo check -p maki-lua --tests
cargo nextest run -p maki-lua
cargo check -p maki-ui --tests
cargo nextest run -p maki-ui
just gen-docs-check
```

After focused suites pass, run the repository-required checks:

```text
just check
just lint
just test
```

The Zed check is manual because Rust wire tests cannot prove client layout/presentation. It supplements rather than replaces protocol assertions.

# Review Strategy

Before execution handoff, run the `plan-reviewer` subagent against this plan and resolve or explicitly rebut every critical/high finding; repeat review if any critical/high issue was found.

After implementation and automated checks, follow repository review guidance if present. Otherwise dispatch a `general-purpose` implementation reviewer, or the configured code-review specialist when available, covering correctness, ownership/concurrency, rollback, protocol ordering, persistence compatibility, Lua stale callbacks, and primary-history isolation. Fix or explicitly rebut all findings. If any critical finding appears, repeat review after fixes until none remain or the operator must decide a blocker.

Review should pay special attention to:

- avoiding a second current-value owner in ACP, Lua module state, or TUI fields;
- multi-session behavior under one shared Lua runtime;
- transactional ordering between model/provider channels and committed snapshots;
- no mutex/app-data borrow held across async Lua callbacks;
- cleanup of watcher tasks and stale plugin generations;
- exactly-once ACP prompt completion under success, failure, cancellation, and session close.

# Documentation Strategy

Update user-facing ACP and command pages for command availability and protocol limits. Add generated Lua API documentation for registration, handles, focused versus explicit session targeting, persistence, callbacks, collisions, and reload semantics. Document `/options` as the TUI discovery surface and Bash auto mode as a persistent per-session option. Keep details canonical: ACP limitations belong in ACP docs; Lua contracts belong in generated Lua API reference; task-oriented use of `/options` belongs in commands/plugins docs. Run `just gen-docs-check` and commit generated changes required by the repository’s doc workflow.

# Risks, Blockers, and Required Decisions

- **State ownership risk:** `SessionCommandState`, `PermissionManager`, TUI state, storage, and Lua modules currently duplicate values. Execution must remove or reduce these to adapters over one committed per-session owner rather than synchronize caches opportunistically.
- **Multi-session Lua risk:** one Lua runtime serves concurrent sessions. Definitions/callbacks are plugin-generation state; current values are per-session. Every command/tool/background API must resolve the correct session explicitly and tests must exercise non-focused tool sessions.
- **Transactional callback risk:** Lua callbacks are async and app-data borrows are not sendable. Stage definitions and values without holding locks/borrows across callback execution, then compare generation/version and commit atomically or retry/fail safely.
- **YOLO persistence risk:** the operator explicitly chose persistence for permission bypass. Use a serde-defaulted disabled value for old sessions, make the resumed value visible, and ensure it cannot be accidentally inherited from another session or process default.
- **Plugin persistence risk:** persisted plugin values may outlive unavailable definitions. Preserve them as inert session metadata only under a bounded, deterministic representation, and never project or execute them until a matching owner/definition validates them.
- **Model transition risk:** sending a model change and then failing state commit can split the agent from projection. Design one authoritative transition/acknowledgement path so commit follows confirmed runtime adoption; do not rely on fire-and-forget `model_tx.send` as proof.
- **Protocol ordering risk:** ACP updates and prompt responses use the same output channel, but independent watcher tasks can race. Carry/await committed snapshot versions or emit through a serialized session projection path so the required update precedes prompt completion.
- **Compaction attribution risk:** ambient compaction events can belong to auto-compaction or another run. Manual operation IDs and pending prompt ownership must make terminal events unambiguous.
- **Client limitation:** ACP v1 cannot clear Zed’s transcript, replace the active session ID from a slash prompt, or change Zed’s displayed session cwd. Documentation and progress text must not claim those effects.
- **Client verification risk:** Zed may rank or hide several `mode` options. Full wire snapshots are the portable guarantee; the manual Zed record must note actual presentation without adding client-specific protocol behavior.
- **Scope boundary:** no generic Rust TUI selector, no new keybinding, no ACP dependency upgrade solely for this work, no monolithic command-effect protocol, and no attempt to make old transcript messages disappear.
# Rebase onto the shared agent actor

The branch was implemented against `145541ff`. `mistress` has since replaced the TUI's `AgentLoop` with the shared agent actor in `maki-agent/src/actor/`, replaced `ModelSlot` with `ProviderSlot` plus provider identity generations, moved provider usage into a Lua-driven state machine, and changed Bash auto mode so a classifier error falls back to permission enforcement instead of failing closed. Merging `635a6317` required re-seating this plan's lease and coordinator work onto machinery that no longer exists.

## Merge resolution

| Area | Resolution |
|---|---|
| `maki-lua/src/docs.rs`, `api/session.rs` | Both sides added list entries. Kept both. |
| `maki-lua/src/api/agent.rs` | `mistress` deleted the `SubagentDriver` block this plan had touched. The deletion wins; `lease_committer: None` moved to the five surviving `AgentInput` literals. A sub-session must not commit into its parent's coordinator. |
| `plugins/bash/init.lua` | Kept the session-option auto-mode gate. Dropped `ctx:set_deadline` from that position: `mistress` moved it to immediately before `jobstart`, so approval time no longer counts against the execution timeout. |
| `maki-lua/tests/real_plugins_restore.rs` | Took the renamed tests and the inverted classifier-error semantics, threaded `SessionCoordinatorHandle` through them. `exec_verdict` is gone; its last caller uses `exec_verdict_prompt`. |
| `maki-ui/src/agent/agent_loop.rs` | Lease acquisition moved from the deleted `AgentLoop::process_entry` into `TuiActorBackend::acquire_lease`, called from `execute_agent` and `run_compact`. History commits on `TurnOutcome::Completed` and after a successful idle compaction. |
| `maki-ui/src/agent/mod.rs` | Kept `mistress`'s `ProviderSlot` rewrite plus this plan's `cwd` slot. `SessionMailbox` is minted once per session and threaded through respawn. |
| `maki-ui/src/event_loop.rs` | Per-session model slots retyped to `ProviderSlot`. The provider-usage readers follow the focused session. Coordinator calls dispatch off the event-loop thread. |

Provider usage remains one coordinator listening on one channel. Session slots share the event loop's change channel through `ProviderSlot::with_change_tx`, and instance generations come from a process-wide counter so identities stay unique across slots.

## Defects found and corrected

An adversarial review of the merged branch, followed by manual testing through the TUI, found fifteen defects. Three came from the merge resolution; the rest were already present in the plan's implementation and were exposed by the new base or by testing.

### Introduced by the merge

- Respawn minted a fresh `SessionMailbox`. The coordinator hands out the mailbox it was registered with and cannot learn about a replacement, so every notification after a respawn reached an instance nobody polled. The mailbox is now created once in `AgentHandles::spawn` and reused.
- A lease failure returned `SetupFailed`, which the runner discards for a root turn. TUI prompts are root turns, so the prompt vanished with no feedback. `report_setup_failure` emits `ControlError` before returning.
- `change_model` installed an already-tracked provider into the event loop's slot, double-wrapping `TrackedProvider` so a session-local re-auth never reached the usage coordinator. The install is gone; session slots report through the shared channel instead.

### Already present in the implementation

- `headless::spawn` registered no coordinator, so every option read and every mailbox notification in print mode failed with `session not live`. Bash could not run under `--print`. SDK mode passed an empty option catalog with the same effect. Both now receive the real catalog, and `auto_mode_enabled` falls back to the configured default when there is no session at all.
- The per-tick model sync wrote the event loop's global model into every tab's `App` and persisted it, so a session restored on its own model displayed and saved the wrong one while inference used the right one. Each tab now follows its own slot.
- Coordinator operations ran under `smol::block_on` on the event-loop thread. A turn holds the session lease for its whole duration, so `/model` during a turn froze the UI, and a turn parked on a permission prompt deadlocked the process. Operations dispatch off-thread and apply through `InternalEvent::SessionOp`.
- The model option's value list was fixed at registration, but providers discover models in the background. A session registered before discovery could only select the model it started on. `sync_model_values` republishes through `update_model_values`, which ACP already did.
- `/new` rotates the tab onto a new session id but left the coordinator registered under the old one. Restoring the previous session failed with `session already live`, the new session never acquired a lease, and `replace_history(Vec::new())` on the retired coordinator checkpointed the previous conversation away. `rotate_session` retires the old coordinator and registers one for the new id, and the mailbox rotates with it.
- A coordinator was registered before its session had a snapshot in the storage writer, so its first checkpoint had no base to merge into. Seeding moved into `register_coordinator`, where every registration path gets it.
- The cleanup that removes an empty session's files also forgot its writer snapshot, which is the base a coordinator checkpoint merges into. After `/new` the fresh session is empty, so the cleanup ran, and the model change on it failed with `session snapshot is unavailable`. The two deletes meant different things through one function: a delete from the picker still forgets everything, while the cleanup keeps the snapshot and lets the files come back if the coordinator later has something worth saving. This one was invisible to reading either subsystem, because each is correct alone: the cleanup predates the plan and the merge-into-snapshot checkpoint comes from it. Logging the storage writer's map found it in one reproduction after three wrong diagnoses from code.
- Restoring a session no longer reused an idle empty tab, stranding one tab per restore. The path is restored by pushing the new runtime and removing the stale one; a tab owns a coordinator and mailbox keyed to its session id and cannot be repointed in place.
- `run_compact` returned `ControlDone` where the runner expects `CompactDone`, warning on every idle compaction.
- The TUI wrote `input.lease_committer` and then committed through a separately acquired lease, leaving the assignment dead.

## Verification

`just ci` passes: `fmt-check`, `lint`, `test`, `gen-docs-check`, `machete`.

Every defect above has a regression test, each confirmed to fail with its fix reverted:

| Test | Defect |
|---|---|
| `maki-ui`: `respawn_keeps_the_mailbox_the_coordinator_hands_out` | Mailbox replaced on respawn |
| `maki-ui`: `session_slot_reports_into_the_shared_change_channel` | Session-local re-auth lost; colliding instance generations |
| `maki-ui`: `rotate_mailbox_repoints_the_agent_at_the_new_session` | Mailbox left on the retired session after `/new` |
| `maki-ui`: `rotating_a_session_moves_its_coordinator_and_mailbox` | Coordinator left on the retired session id after `/new` |
| `maki-ui`: `each_tab_follows_its_own_model_slot` | Per-tab model overwritten from the global slot |
| `maki-ui`: `a_setup_failure_reaches_the_user` | A turn dying in setup vanished silently |
| `maki-ui`: `cleaning_up_an_empty_session_keeps_its_coordinator_base` | Empty-session cleanup dropped the checkpoint base |
| `maki-ui`: `deleting_a_session_forgets_its_coordinator_base` | The other half: a deleted session must not be resurrected |
| `maki-agent`: `spawn_registers_a_resolvable_coordinator` | No coordinator in print mode |
| `maki-agent`: `a_model_discovered_after_registration_becomes_selectable` | Model list frozen at registration |

Reaching the three `EventLoop`-bound defects meant moving the logic off a type that owns a terminal. `register_coordinator` took only four things from `SpawnCtx`, now a `CoordinatorDeps`; `sync_session_models` takes only the sessions, which makes reading one shared slot for every tab unrepresentable rather than merely fixed; and `rotate_session_coordinator` holds the rotation invariant. An `EventLoop` still cannot be constructed in a test, so logic that needs one should be moved out rather than left uncovered.

## Flaky tests found while verifying

Three tests failed intermittently under full-suite contention, each asserting an outcome that was only usually true.

| Test | Cause |
|---|---|
| `file_refresh_uses_lexical_source_order_for_ties` | Fixed hundred-iteration budget polling an async matcher; uses the deadline helper `91-flaky-tests` added |
| `failed_subagent_turn_resolves_and_same_session_recovers` | Assumed the first envelope on the parent channel was the history one, then assumed `status` had settled when `prompt` returned |
| `live_event_handle_does_not_hang_after_begin_shutdown` | Raced the dispatch loop over whether a shutting-down host still serves requests |

The third was corrected in the product rather than the test: `EventHandle` carries the host's shutdown flag and `collect_prompt_slots` returns defaults without sending into a dying host, which is what the test's own comment already claimed. Fifty consecutive full-suite runs pass.

## Lease scope

The lease was first implemented as "nothing else may touch this session while a
turn runs": `AcquireLease` parked the coordinator's whole operation loop until
the turn released it, so every operation queued behind it. That is broader than
what the lease is for, and it changed behaviour the TUI had before this plan.
On `mistress` `/yolo` flipped a `PermissionManager` flag directly, `/fast` set a
field, and `/model` installed into the model slot, all synchronously; each took
effect the moment it was typed. Toggling YOLO mid-turn is how a user escapes a
permission prompt they are looking at, so deferring it removes the reason to
reach for it.

The lease now guards history alone. An operation that leaves history untouched
runs while the lease is held; `ReplaceHistory`, `ChangeDirectory`,
`PreparePluginOptions`, `Close` and a nested `AcquireLease` queue until the turn
releases. Operation handling moved out of the loop into `handle_operation` so
`hold_lease` can serve from the same set.

Two consequences worth stating. A turn's history commit travels through the same
loop, so a slow operation served during a lease delays that commit; the trade is
deliberate, because the alternative is the UI waiting on the turn. And `fast` and
`workflow` are captured into a turn's input when it is admitted, so toggling
either mid-turn updates the option snapshot while the running turn keeps the old
value -- the same behaviour `mistress` had, not something the lease change
introduces.

Commands that still defer say so rather than appearing ignored.

## Model changes mid-run

Narrowing the lease made an option change reach the coordinator during a
running turn, but a run still captured its provider and model when it started,
so a model change applied only to the following turn. For an agentic loop of a
dozen requests that is a long wait, and it reads as the change having been
ignored -- the status bar showed the new name while every request kept using
the old model.

A run now polls an optional `ModelSource` at the top of each request. That
point is safe because every tool call already has its result in history, so a
changed tool schema cannot orphan an outstanding one; everything else derived
from the model is recomputed per request already. Cost attribution improves as a
side effect, each request billing against the model that served it.

Two properties made this smaller than it looks. History is stored
provider-neutral -- `ContentBlock` is a shared representation each adapter
renders, and a reasoning block's signature is used by the provider that issued
it and ignored elsewhere -- so a history built under one provider is not
malformed for another. And `resolve_compaction_model` already substituted a
different provider and model for a request inside a live run, so the precedent
existed.

One claim made when this landed was wrong and is corrected here. Not everything
derived from the model is recomputed per request: `request_tools` filters and
extends a base schema the frontend built once for the turn, so a run that
switched model carried the previous model's tool descriptions. The base is
rebuilt now, through a builder the frontend supplies.

ACP and SDK adopt through a shared source rather than asking the session loop to
swap its locals. That loop reads its control channel only between turns, so the
round-trip could not adopt mid-run at all; worse, once the coordinator served
option changes during a lease, it deadlocked. The adopter waited for the turn,
the turn's history commit waited for the coordinator, and the coordinator waited
for the adopter. Installing into the source is a store, so nothing waits.
`ChangeDirectory` was never exposed to this because it defers behind the lease.

Between a change and the request that picks it up, the status bar shows the
model the run is on with a queued marker. Showing the new name would claim a
switch that has not happened; showing only the old one looks like the change was
dropped.

`fast` and `workflow` travel the same path. The coordinator owns both as
session options, so the source reads them from there rather than mirroring them
into a second place, and a toggle reaches the run at its next request. Workflow
needed the schema rebuild to be worth anything: the flag admits `task` into the
interpreter's tool list, which is decided when the schema is built, so adopting
the flag alone would have changed subagent behaviour while the interpreter kept
its old tools.

Thinking was the exception, so it became a session option too. It differs from
the others in one way: its value list is not its domain. A client picks from the
named efforts, but a token budget is any number, so the definition carries an
open domain alongside the list, and `accepts` consults both. Everything else is
the same shape as fast -- the coordinator owns it, a model change clamps it off
when the new model cannot think, asking for it on such a model is refused rather
than stored, and the run settings source reads it so `/thinking` mid-turn lands
on the next request.

Storage keeps the field it has always had. `SessionMeta.thinking` predates
session options, the checkpoint projection writes there rather than into the
generic map, and `Prefs::default_thinking` still seeds a new session. What
changed is who owns the value while a session is live: the TUI's `state.thinking`
is now a mirror of the option, written when the coordinator commits, and the
`/thinking` plugin keeps its picker and reaches the coordinator through
`maki.session.set_thinking` as before. ACP gets the option for free -- it is one
more `select`, and a loaded session restores the effort it was saved with.

## What the lease still had to guard

Narrowing the lease to history replacement was not enough on its own, because
two operations it kept serving write history on the way past. `set_option` and
`update_model_values` snapshot `state.history` for their checkpoint, and that
field is written in exactly one place -- a replacement -- so while a lease is
held it is the last committed history, from before the running turn. The TUI's
writer merges a checkpoint by replacing the stored messages with the ones it
carries, so an option change mid-turn rewound the saved session to before the
turn, and the history-base guard then preserved the rewind against the next
correct save.

The unattended path was the worse one: republishing discovered models calls
`update_model_values` on every session, so a tab with a turn in flight lost its
persisted history seconds after startup with no user action.

A checkpoint's history is optional now, and only a replacement carries it. An
option, model or directory change leaves the stored messages alone, which is
what those operations always meant.

## The interactive loop and the shared model

Pointing the ACP and SDK adopters at the shared source left
`InteractiveControl::AdoptModel` with no producer, and the interactive session
loop kept its own `provider` and `model` locals. Those locals were never
updated again, so after a model change everything the loop builds from them was
stale: the advertised tool set, which gates tools on model capabilities, the
system prompt, and -- worst -- manual compaction and isolated turns, which ran
on the old provider entirely. The SDK's `set_model` control request rebuilt a
provider into those locals and was then discarded, because the turn reads the
shared source.

The loop refreshes its locals from the shared source each iteration, so
controls and turns alike see the current model, and `set_model` installs there.
The dead control is gone rather than left as an invitation for the two to
diverge again. Interactive runs also carry a tool builder now; without one they
adopted a workflow toggle while keeping the schema built for the old flag,
which is the half-applied state the change set out to avoid.

## Smaller corrections from the same review

A session cleaned up as empty keeps its writer snapshot so its coordinator can
still checkpoint, but nothing forgot it afterwards, so `/new` on an untouched
session left one behind every time. Retiring a coordinator forgets it.

Rotating a tab onto a new session mutated the mailbox before registration could
fail, and a failure left the tab on a session id nothing could resolve while
every later operation checkpointed into the retired session's file. Registration
happens first now, and the mailbox moves only once it succeeds.

The guard against awaiting a coordinator operation on the event-loop thread
listed the wrong operations after the lease was narrowed: it still named the two
the lease now serves, and omitted `close`, which the lease defers. Correcting it
caught a real one at shutdown. That await is gone -- dropping the handle ends the
coordinator's loop and unregisters on the way out, and a close persists nothing,
so awaiting it bought only the risk of hanging exit behind a turn that never
released its lease.

## Exit latency

Retiring coordinators by dropping their handles rather than awaiting a close
made exit wait three seconds. The storage writer's thread stops when its wake
channel closes, but every coordinator's checkpoint writer holds a clone of that
sender, and those live in tasks that exit when next polled rather than when the
handle drops. So the channel usually still had a live sender at exit and the
drain ran to its timeout -- intermittently, which is why it looked like a hang
rather than a constant cost.

The thread stops on an explicit flag now instead of inferring it from the last
sender going away. Nothing that merely holds a wake clone can delay exit.

## Known ordering wart

`settle_turn` resolves a turn's ticket inside `finalize_turn`, which runs before the actor flips to idle. A plugin that calls `sess:prompt()` and then `sess:status()` can see `running`, with no error, for a turn that has already returned. The test that caught this waits for the status to settle; the ordering is unchanged. Correcting it means resolving the ticket after the idle flip rather than inside `finalize_turn`, which has eight call sites, and setting idle first is wrong because `status` reads the retained outcome and would then see a stale one. This is upstream of the plan and left for a separate change.

# Merge with the slash-command parsing change

`mistress` merged #133 (`lua-command-parsing`), which changed what the registry
does with a leading slash it cannot resolve. `classify_input` now splits input
into a command, an escaped literal, and plain text: an unresolvable
`SlashClass::Command` returns `CommandOutcome::Failed(UnknownCommand)` instead
of falling through as model text, and a doubled slash is the escape that sends
the text literally (`//help` sends `/help`). This plan was written against the
old forwarding policy, so the statements above that described unavailable
commands becoming literal input have been corrected in place.

The behaviour this plan owns is unaffected. `/new` and `/clear` are hidden from
ACP through `bind_target_with_presentation`, which narrows presentation without
narrowing resolution, so they still resolve and still return
`NEW_SESSION_GUIDANCE`. Only genuinely unresolvable input changed.

## Merge resolution

| Area | Resolution |
|---|---|
| `maki-commands/src/lib.rs` | Both sides added exports. Kept both. |
| `maki-acp/src/server.rs` | Took `mistress`'s rejection semantics and its two tests, re-seated onto this branch's `dispatch_prompt` helper: `prompt_request` now needs the live session id, because prompts validate against the coordinator binding. |
| `maki-ui/src/event_loop.rs` | Kept this plan's push-then-remove restore path, with `mistress`'s `set_focused(idx)` in place of the bare `self.focused = idx`. |
| `maki-ui/src/components/file_completion.rs` | Both sides replaced the same fixed-iteration poll with `wait_for_matcher`. Kept this branch's predicate, which does not `unwrap` the session. |
| `maki-lua/tests/real_plugins_restore.rs` | Both sides added imports. Kept both. |
| `site/docs/content/acp/_index.md`, `commands/_index.md`, `maki-docgen/src/gen_commands.rs` | Combined: this branch's hidden-`/new` paragraph plus `mistress`'s error-and-escape sentence. `maki-docgen --check` confirms the handwritten and generated halves agree. |

## Defects found and corrected

Two semantic conflicts that merged cleanly and failed afterwards:

- `src/print.rs` constructed an `AgentInput` for `InputDispatch::LiteralInput` without `lease_committer`. Print mode's literal path arrived on this branch; the field arrived on the other side of it.
- `maki-agent`: `portable_target_separates_compaction_from_session_replacement` asserted `/new` on a portable target falls through as literal input. Under the new policy an unresolvable command fails, so the assertion now expects `CommandOutcome::Failed(UnknownCommand)`. The ACP guidance path is unchanged and its test still passes.

## Verification

`just ci` passes: `fmt-check`, `lint`, `test` (5017 tests), `gen-docs-check`, `machete`.

`maki-docgen`'s `generation_failure_prevents_output` fails intermittently under
`cargo test --workspace`: it shares the `CHECK_CALLS`/`WRITE_CALLS` atomics with
the other tests in its binary. It came in with `db1a54f9` and nextest does not
hit it, since each test gets its own process. Upstream of this plan.

## Follow-ups from the merge audit

Three agents audited the merged branch: the resolution itself, the acceptance
criteria, and whether anything from `mistress` had been quietly undone. The
merge came back clean on all three counts -- no side's changes were lost, the
`//` escape and unknown-command rejection reach every frontend, and the
`self.focused` invariant from `4b12e80b` survives the rewritten restore path.
What the audit did find was one interaction bug and a set of criteria whose
tests do not pin what their names claim.

### Fixed

- `maki-lua/src/api/tool.rs`: `nested_dispatch_result` matched `Dispatched(_)`
  as success. `mistress` wrote it when the only outcomes were `Completed` and
  `AgentTurn`; this plan added `IsolatedTurn`, `ManualCompaction` and
  `FrontendFeedback`, and a nested dispatch has no frontend to execute any of
  them. So `maki.api.run_command("/compact")` from inside another command's
  handler returned `true` to Lua having done nothing, and `/new`'s guidance text
  was discarded. Only `Completed` is finished by the time it returns; everything
  else now reports that it needs a frontend. `AgentTurn` is included -- it was
  swallowed on `mistress` too, for the same reason.
- `maki-agent/src/headless.rs`: the manual-compaction arm committed history
  before reporting `Completed`, correctly, but nothing tested it, and
  `headless.rs` has no seam to test it through. The persist-then-decide step is
  now `settle_manual_compaction`, taking the persistence step as a parameter.
  Reversing commit and terminal, or persisting a compaction that failed, breaks
  three of its four tests.
- `maki-agent/src/agent/isolated_turn.rs`: `run_with_outcome` asserted that a
  locally-owned `Vec` was unchanged after passing a `.clone()` into the callee.
  That cannot fail, and it was standing in for AC.12's history guarantee. The
  guarantee is real but structural, so the assertion is gone and the tests now
  pin what the provider is handed: empty tools, the Build system text, and
  dangling tool calls closed in the copy.
- `maki-docgen/src/gen_commands.rs`: the built-ins table's `TUI-only` column
  reported only the interactive-UI capability, so `/new` read as `no` directly
  above prose saying it is not portable. The column is now `Frontends`, computed
  against `portable_capabilities()`, which asks the question the reader has.

### Left open

- **AC.7's restricted validator environment was never built.** `session_option.rs`
  runs validators with the plugin's full environment; the only guard is a
  reentrancy flag with no test coverage. This is an unbuilt phase, not a
  regression.
- **AC.8, AC.10, AC.14 remain partly unpinned**: `/options`' command body is
  uncovered beyond three pure helpers, the ACP half of `/cd` (feedback rendering
  and live-cwd tool locations) has no test, and AC.14's `ServerDeps`/
  `SessionFactory` refactor and Zed record do not exist -- though the harness
  capability the AC asks for is genuinely present.
- **`maki-commands/src/registry.rs`: `resolves_input_for` has no callers.** Dead
  on both sides of the merge.
- **Three shared-process-state test defects**, all upstream of this plan and all
  invisible under nextest, which gives each test its own process:
  `maki-storage/src/lib.rs`'s `wait_at_replacement_boundary` clones `release_tx`
  into every waiter so the channel never disconnects, deadlocking
  `cargo test -p maki-storage` outright; `maki-docgen/src/main.rs` shares
  `CHECK_CALLS`/`WRITE_CALLS` across tests that run in parallel; and
  `maki-providers`' `seed_catalog_for_tests` races on a `OnceLock`.
