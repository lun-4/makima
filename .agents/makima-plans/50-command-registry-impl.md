# Goal

Add a neutral `maki-commands` crate as the authoritative registry for command metadata, name resolution, aliases, precedence, execution, and argument completion.

Make `maki-ui::CommandPalette` a projection-only component. It displays registry metadata, manages selection and completion UI state, and returns an opaque `CommandId` with arguments. It does not execute commands or select provider-specific behavior.

Route palette and headless command execution through the same registry dispatch path. Preserve current command behavior. Keep `/theme` as a `maki-ui` producer. Keep `/model` in `maki-ui/src/app/mod.rs` until issue #27 moves model configuration into the agent layer.

Do not add an unused `maki-commands` dependency to `maki-acp` in this change.

No commit is created automatically. The implementation uses the commit stages below when the user explicitly authorizes commits.

# Current-state inventory

Before API design, inspect and record the current command sources and consumers.

- List every branch in `maki-ui/src/app/mod.rs::App::execute_command`.
- Record each command's canonical name, aliases, argument rules, effects, host state access, error behavior, and nested execution behavior.
- Record builtin command definitions, custom command expansion, MCP prompt registration, Lua command snapshots, and their refresh triggers.
- Record all completion producers, their reader/event types, cancellation paths, and tests.
- Record `CommandPalette` constructors, public methods, internal state, and all test fixtures.
- Record docgen command metadata inputs and generated output files.
- Record application initialization and the lifecycle points for producer registration, refresh, palette open, palette close, and session shutdown.

Store the inventory in the implementation issue or working notes. Add a parity matrix to the plan before implementation. Each existing command branch must map to one registered behavior, one typed host request, one neutral effect, or explicit argument validation.

# Architecture

Dependency direction:

```text
maki-commands <- maki-ui
maki-commands <- maki-docgen
maki-commands <- maki
```

`maki-commands` has no dependency on `maki-ui`, `maki-agent`, `maki-lua`, ratatui, provider state, or the Lua runtime. `maki-lua` remains independent of `maki-commands` in this change.

The registry owns command identity, registration, aliases, precedence, projection, resolution, handler lifetime, and completion session validation. Producers own their registrations and handler implementations. `maki-ui::App` owns `UiCommandHost`, which translates neutral requests and effects to UI state and actions. `CommandPalette` owns only projection and interaction state.

The registry must not contain provider or UI policy hidden behind generic JSON or state escape hatches. Add typed operations only when the inventory maps an existing command branch to that operation. Do not add `Json` or `SetState` variants unless a concrete parity case requires them and the payload schema is documented and tested.

# Contract design

## Execution traits and runtime model

Use boxed futures so the registry can store handlers as trait objects and dispatch them on the existing `smol` runtime.

The contract must use signatures equivalent to the following. Exact names may follow repository conventions after dependency inspection.

```rust
pub trait CommandBehavior: Send + Sync {
    fn execute<'a>(
        &'a self,
        context: CommandContext,
        host: &'a dyn CommandHost,
        arguments: &'a str,
    ) -> BoxFuture<'a, Result<CommandResult, CommandError>>;
}

pub trait CommandHost: Send + Sync {
    fn request<'a>(
        &'a self,
        request: HostRequest,
    ) -> BoxFuture<'a, Result<HostResponse, CommandError>>;
}
```

Use the workspace's existing boxed-future and error conventions where available. Do not add a runtime dependency to `maki-commands` solely to obtain an executor.

`CommandBehavior` and `CommandHost` are object-safe. Handler implementations do not borrow the registry. Handler execution may call the host and may return a nested `CommandResult::Run`, but it may not mutate registry registrations during the same dispatch call.

`CommandContext` contains the current command ID, producer ID, recursion depth, cancellation token, and host-independent invocation metadata. It does not contain `App`, ratatui, provider state, Lua values, or a registry reference.

`CommandResult` contains:

- `Complete(Vec<Effect>)` for ordered neutral effects.
- `Run(CommandId, String)` for one nested command invocation.

The registry executes nested commands recursively. It increments depth before dispatch. It rejects a command when the current depth reaches `MAX_EXECUTION_DEPTH`. The returned error is `DepthExceeded`. Nested arguments pass unchanged to the nested behavior. The registry preserves effect order and returns nested effects in execution order.

`CommandError` contains `InvalidArguments`, `Host`, `DepthExceeded`, `Cancelled`, and `Handler`. Unknown command IDs return a distinct `UnknownCommand` error. Host response type mismatches return `Host` with an actionable description.

## IDs and handler lifetime

Define registry-scoped `CommandId`, `BehaviorId`, `CompletionId`, `ProducerId`, and `CompletionSessionId` as opaque non-serialized values.

IDs are never reused during the lifetime of a registry. A command registration receives a new `CommandId` when a producer is replaced. A resolved command owns an `Arc` to its handler record. An owned resolution remains executable after producer removal or replacement, using the handler captured by that resolution. A newly resolved name cannot find a removed or replaced command.

Retired handler records remain alive through owned resolutions and in-flight executions. The registry drops retired records after no references remain. Executing a command from an owned resolution does not consult the current name map.

## Registration and resolution

Define `CommandSpec` with canonical name, description, maximum argument count, aliases, producer, priority, behavior ID, and optional completion ID. Define `PaletteCommand` with display name, canonical name, description, maximum argument count, command ID, and optional completion ID.

`register_producer` replaces all registrations for one producer atomically. Registration validates every canonical name and alias before changing the registry. Duplicate canonical names or aliases within one producer return an error. Registration input order is stable and determines the first-registration tie rule within one producer.

`remove_producer` is an empty replacement. Both replacement and removal cancel that producer's active completion sessions before changing the registration map.

Normalize names with ASCII-insensitive lowercasing. Preserve non-ASCII characters. Canonical names take precedence over aliases at equal priority. Across producers, resolution uses priority Lua, MCP, application, builtin. At equal priority, the producer registration order is stable and explicit. A canonical name in a lower-priority producer does not override an alias in a higher-priority producer. A canonical name and alias at the same priority use canonical status as the tie-breaker, then stable registration order.

Reject alias collisions that are ambiguous after applying these rules. Do not silently produce different results for projection and resolution. Projection and resolution use the same winner map.

Define and test `resolve`, `resolve_alias`, `palette_commands`, `projection_generation`, and execution from an owned resolved command.

## Completion protocol

Define `ArgumentContext`, `ArgumentItem`, `ArgumentLifecycle`, `CancelToken`, `CompletionSession`, and `CompletionBatch`.

The registry allocates globally unique completion session IDs. `ArgumentCompletion::collect` receives a `CompletionSession` and returns an optional `flume::Receiver<CompletionBatch>`. Completion providers and lifecycle callbacks are stored as `Send + Sync` trait objects and use boxed futures where they can perform asynchronous work.

Every batch contains producer ID, producer generation, session ID, `done`, and items. The registry accepts a batch only when all three identity fields match the active session. It ignores stale or mismatched batches without adding items. `done` terminates the session. A closed receiver terminates it without adding items. Repeated done batches are ignored.

Cancellation is idempotent. It sets the token and sends exactly one cancel lifecycle event. The registry removes the active session after cancellation. Batches and lifecycle events that arrive later are ignored. The application calls the registry's explicit `cancel_completion_session` method when the palette closes, when the selected command changes, and when a producer is replaced or removed.

Use deterministic test channels and barriers for cancellation and replacement tests. Do not use sleeps.

# Implementation stages

## Stage 0: Inventory and parity fixture

Inspect the current implementation and write the command parity matrix in the working notes or issue.

Add no runtime code. Add focused characterization tests only where existing behavior lacks coverage. The tests must cover unknown commands, malformed arguments, maximum arguments, aliases, nested commands, every builtin branch, custom commands, MCP prompts, Lua dispatch, theme lifecycle, model behavior, and completion cancellation.

Commit boundary:

```text
chore: inventory command dispatch and parity cases
```

This commit contains only characterization tests and inventory artifacts. It must not change command behavior.

## Stage 1: Add neutral contracts and registry

Add `maki-commands` to the workspace and root dependency table. Add the crate to `maki-ui`, `maki-docgen`, and root `maki` only when the next migration stage consumes it. Do not add it to `maki-acp` yet.

Implement the contracts, opaque IDs, registration validation, precedence, projection generation, owned resolutions, handler lifetime, execution, nested depth, errors, and completion session validation.

Add tests for unknown commands, malformed arguments, max-argument enforcement, duplicate canonical names, duplicate aliases, canonical/alias collision matrices, priority ties, Unicode normalization, replacement, removal, stable IDs, owned resolution execution, stale completion batches, idempotent cancellation, and deterministic cancellation races.

Verification:

```text
just check
cargo test -p maki-commands
cargo check -p maki-commands
cargo fmt --all -- --check
```

Commit boundary:

```text
feat: add neutral command registry
```

## Stage 2: Register one builtin and preserve old dispatch

Add the application adapter and register one representative builtin through `CommandRegistry`. Keep the existing dispatch path for all other commands. Route the representative command through a typed behavior and host/effect contract.

Verify that the old and new paths produce equivalent effects, errors, argument validation, and nested execution results. Keep registration initialization in one application-owned function called during `App` construction. Define the refresh function and call it at each existing command-source refresh point.

Commit boundary:

```text
refactor: route one builtin through command registry
```

## Stage 3: Make the palette a projection

Refactor `maki-ui/src/components/command.rs` to store generic `PaletteCommand` rows, projection generation, opaque completion IDs, and completion sessions. Remove provider snapshots, source kinds, readers, execution lookup, and UI action construction.

Define a palette confirmation result containing only `CommandId` and argument text. Define palette-close cancellation as an explicit application callback that cancels the active session and invokes theme-session cleanup when applicable.

Migrate palette fixtures and tests. Prove projection-only behavior through API tests. Keep the old application execution path while the palette adapter changes.

Prefer compile-time separation. If a source scan remains necessary, place it in a temporary architecture test with an explicit removal condition and scope it to forbidden imports and execution APIs, not arbitrary symbol names.

Commit boundary:

```text
refactor: make command palette projection-only
```

## Stage 4: Migrate command producers

Move builtin and custom command registrations to the application-owned registry adapter. Preserve Lua and MCP ownership in their existing crates. Convert their snapshots and event handles to neutral registrations without making `maki-lua` depend on `maki-commands`.

Register `/theme` from `maki-ui`. Keep preview-session state neutral. Let `UiCommandHost` apply previews, persist commits, and restore the committed theme captured at session start. Wire cleanup to palette close, cancellation, producer replacement, and normal completion.

Register `/model` from its current owner in `maki-ui/src/app/mod.rs`. Use the current model state, picker, and adapter. Do not move model ownership before issue #27.

Add parity tests for every inventory row. Add tests for producer initialization, refresh, replacement, removal, palette close, session shutdown, and application construction.

Commit boundaries:

```text
refactor: register builtin and custom commands
refactor: register MCP and Lua commands
refactor: move theme command behind neutral host
refactor: register model command without changing ownership
```

Each commit must build and pass the tests for the migrated producer. Do not combine unrelated producer migrations.

## Stage 5: Route all execution through the registry

Replace `App::execute_command` with name resolution followed by registry execution. Palette confirmation passes its `CommandId` to the registry. Headless input resolves the name once and passes the same ID to the registry. Remove provider and command-name dispatch branches from `App::execute_command`.

`UiCommandHost` is the only adapter allowed to access `App` state during command execution. It maps typed host requests and neutral effects to existing UI operations. It preserves effect ordering, error messages, picker behavior, usage refresh, queue focus, help, model switching, theme persistence, MCP operations, Lua dispatch, and nested depth behavior.

Add a parity test that invokes each command through both palette and headless entry points and compares the resulting registry dispatch and application effects. Add a source-level check only if the API boundary cannot enforce the absence of provider dispatch.

Commit boundary:

```text
refactor: route application command execution through registry
```

## Stage 6: Update docgen and remaining consumers

Make `maki-docgen` consume central command metadata. Preserve canonical names, aliases, descriptions, ordering, and argument metadata. Record expected generated-file changes. If output is unchanged, require a byte-identical generated-output check.

Run `just gen-docs-check`. Add `maki-commands` to `maki-acp` only in a later commit when ACP consumes a contract or exposes a tested API. Do not add an unused dependency.

Commit boundary:

```text
refactor: generate command docs from registry metadata
```

# Required tests

Registry tests cover registration, validation, aliases, precedence, Unicode normalization, projection generation, replacement, removal, stable IDs, owned resolutions, handler lifetime, execution errors, host response mismatches, effect ordering, nested arguments, depth limits, completion dispatch, stale batches, cancellation, and deterministic replacement races.

Palette tests cover generic projection, canonical and alias display, opaque completion IDs, generation updates, missing completion state, stale batches, cancellation on close, and confirmation without behavior execution.

Application tests cover every row in the command parity matrix, builtin/custom/MCP/Lua dispatch, theme preview/restore/commit, model behavior and ownership, help/usage/Btw/queue effects, nested Lua commands, palette/headless parity, producer refresh, and removal/replacement behavior.

Architecture tests verify that `maki-commands` manifests do not depend on UI or producer crates and that `CommandPalette` exposes no execution or provider-specific API. Prefer compile-time API checks over source scans.

# Acceptance criteria

- `maki-commands` has no dependency on `maki-ui`, ratatui, `maki-agent`, `maki-lua`, or provider crates.
- All contracts have explicit async, object-safety, `Send`/`Sync`, ownership, cancellation, error, and effect-order semantics.
- Registration validates atomically and applies deterministic canonical, alias, priority, and registration-order rules.
- Owned resolutions remain executable after producer replacement or removal. New lookups do not resolve retired registrations.
- Stale completion batches and lifecycle events cannot update a replacement session.
- Palette confirmation returns only `CommandId` and arguments. Palette code does not execute behavior or construct UI actions.
- Palette close cancels its active completion session and restores an active theme preview through the application-owned cleanup path.
- Every existing command branch appears in the parity matrix and has a passing parity test.
- Palette and headless entry points dispatch the same registry command ID and behavior.
- `/model` remains application-owned until issue #27.
- Generated command documentation preserves metadata and ordering, with expected output changes recorded.
- `maki-acp` has no unused dependency on `maki-commands`.
- Each commit stage builds independently and passes its scoped tests.
- Final verification passes `cargo fmt --all -- --check`, `just check`, `just lint`, targeted crate tests, `just gen-docs-check`, and the full workspace test command from the repository `justfile`.

# Verification sequence

Run the cheapest applicable check after each edit. Run the stage-specific checks before each commit. Run `just check` and `just lint` after integration stages. Run `just gen-docs-check` after docgen changes. Run the full workspace test command from `justfile` before any commit that combines crates or before handoff.

Never use sleeps in concurrency tests. Use barriers, channels, and explicit generation/session assertions.

# Review gates

After Stage 1, request a plan and contract review. Resolve every critical and high finding before Stage 2.

After Stage 5, request an adversarial implementation review. The review must cover dependency direction, contract completeness, registration lifecycle, producer replacement, completion cancellation, ID semantics, execution parity, palette purity, theme cleanup, model ownership, and documentation output.

Repeat the review after fixes until no critical or high finding remains.

# Commit policy

The implementation does not create commits unless the user authorizes commits. When authorized, create only the commit stages listed above. Verify the scoped tests before each commit. Stage files by commit purpose. Do not mix formatting-only changes, generated output, or unrelated fixes into a migration commit. Do not amend existing commits. Do not force push.
