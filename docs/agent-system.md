# Agent system

The reliable turn lifecycle, shared actor, and per-outer-session agent manager below describe the current implementation. See the [Lua API reference](../site/docs/content/lua-api/_index.md) and [plugin guide](../site/docs/content/plugins/_index.md) for the public contract.

## Vocabulary

- **Outer session**: one frontend session (TUI, print, SDK or ACP) that owns one agent manager and one rooted agent graph.
- **Agent**: one graph node with a stable `AgentId`, persistent actor, history, FIFO queue, and reusable lifecycle.
- **Root agent**: the graph node with no parent. It uses the same actor implementation as descendants.
- **Turn**: one accepted model/tool run, identified by a `TurnId` and ending in one `TurnOutcome`.
- **Actor**: the per-agent scheduler and state owner.
- **Manager**: the per-outer-session graph, factory, authority, task, and cross-agent concurrency owner.
- **Subagent** and **task**: product-facing names for descendant agents. They are not separate core runtime types.
- **Correlation id**: a frontend or protocol value such as a headless cancel key, task id, or provider tool-use id. It is not agent identity or authority.

## Architecture

```text
Outer session (TUI, print, SDK or ACP)
└── AgentManagerHandle
    ├── graph, limits, runner tasks, turn permits, wait watchers
    └── root AgentActorHandle
        ├── history, FIFO queue, lifecycle, retained outcomes
        └── Lua-created child AgentActorHandle
            └── Lua-created grandchild AgentActorHandle

AgentActorHandle
└── ActorBackend (TUI, headless or Lua adapter)
    └── transient Agent
        └── Agent::run(turn_id, input) -> TurnOutcome
```

The layers deliberately own different state:

| Layer | Owns | Must not own |
|---|---|---|
| `Agent::run` | One model/tool run, invocation-local usage and model-turn count, terminal outcome emission | The persistent queue, graph topology, or frontend lifecycle |
| `AgentActorHandle` | One agent's stable identity, history, FIFO scheduling, active cancellation, tickets, outcomes, cumulative usage | Parent-child topology, graph limits, or a second copy of frontend state |
| `AgentManagerHandle` | One outer session's rooted graph, actor factory, runner tasks, topology, spawn authority, global active-turn limit, subtree lifecycle | A second per-agent queue, history, or outcome store |
| TUI, headless and Lua backends | Dynamic provider/model/tool/prompt preparation and presentation | Lifecycle truth or graph authority derived from presentation ids |

The core is dependency-neutral. `maki-agent` does not depend on `maki-ui` or `maki-lua`; those crates implement `ActorBackend` to prepare and execute their own turns against actor-owned history. `maki-agent::headless` holds the print, SDK and ACP backend.

## Turn lifecycle

`AgentId` and `TurnId` are nominal wrappers around `MakiId`. They use the same compact UUIDv7/base58 representation but cannot be interchanged by the Rust type system.

Every accepted turn ends in exactly one variant of `TurnOutcome`:

- `Completed`: usage, model-turn count, and `DoneReason`
- `Failed`: usage, model-turn count, and a durable `TurnFailure`
- `Cancelled`: usage, model-turn count, and `TurnCancellationReason`

`TurnFailure` snapshots provider/runtime errors into a stable public taxonomy, preserving a full diagnostic, user-facing message, and retryability. Cancellation is not failure and is not successful completion.

`Agent::run` is the low-level terminalization boundary. It resets invocation-local accounting, runs the provider/tool loop, determines one outcome, makes one `AgentEvent::TurnOutcome` delivery attempt, and returns the same outcome. A failed event send does not change the result and is not retried.

This is producer-side exactly-once terminalization, not durable exactly-once event delivery. The returned or actor-retained outcome is authoritative; an event sink is presentation transport.

Standalone controls and compaction are not turns. They have no `TurnId`, create no retained outcome, and use control-specific events such as `ControlComplete`, `ControlError`, or `CompactionDone`.

## Configuration ownership

Managed actors own the committed `EffectiveAgentConfig`. Typed changes and admissions share one FIFO; materialized work runs through the separate execution queue. Preparation runs outside actor locks, and completion checks the reserved entry identity before committing or enqueueing work. Active execution does not wait for later configuration preparation.

Every frontend, the coordinator and Lua commit a config change through one `config_change::ConfigChangeTicket`. Reserving takes the durable mutation (for a root, the session option lease) and then the FIFO slot, both synchronously, so changes hold the lease in dispatch order. A change that meets a held lease or idle guard joins a per-actor wait queue and takes its slot once the holder lets go, so waiting changes keep dispatch order too; it fails only when the target closes or the change is rejected. A model a change switches to is re-resolved against discovery when the change commits, under the actor's state lock, so discovery that lands while the change is in flight still reaches it. Every frontend fills one `orchestration::ModelCatalog` through `ModelCatalog::discover`. Each provider's batch merges into the catalog at once; its refresh then commits rediscovered metadata to the agents of every attached orchestration, reading their committed configs only after the provider's metadata is written (so a change in flight is caught by either side), and republishes their sessions' model values when the catalog grew. Print waits up to `ACTIVE_DISCOVERY_BOUND` for its own provider's batch before the first turn. A coordinator option that needs no actor change (yolo, plugin options) takes the same queue place through `config_change::reserve_in_turn`, so it waits rather than failing busy. Errors are a typed `ConfigChangeError`; `is_busy()` means an idle guard or the lease refused a change, which only plugin option replacement across sessions still reports.

Every mode name goes through one `modes::resolve`, against the registry the Lua host owns (`EventHandle::mode_registry`), and every frontend and Lua commit a mode with `ConfigChangeTicket::commit_mode`. `plan` keeps the current plan file or allocates one under the state directory. A mode is durable config committed once, like any other change: headless frontends admit turns without a mode of their own.

An actor commit publishes its identity, generation, and full configuration to frontend projections. Provider slots and coordinator reads do not supply managed execution settings. Configuration checkpoints use the existing session format and project onto stored metadata without replacing history. The user-visible behaviour is described in [Modes: Configuration changes](../site/docs/content/modes/_index.md#configuration-changes).

### Durable publication

Durable configuration changes commit through `PreparedOperationTicket::commit_durable`. History edits made under an idle guard (rewind, plan approval that clears context) commit through `ActorIdleGuard::commit_prepared_transaction_held`. In both, the actor captures a candidate snapshot, the host stages it on disk, and the actor commits only after the host has published it. One `CheckpointPublication` per request arbitrates between cancellation and the final replacement:

```text
writer guard      SessionPublicationGuard: session lock held, owner still ours
  plugin gate     PluginAuthority::publication_gate: plugin generation alive
    task scope    TaskCell: invocation live, same plugin origin
      cancel      CancelToken publication gate: not cancelled
        CAS       CheckpointPublication PENDING -> PUBLISHING
          rename  staged file replaces the session file
```

The claim runs synchronously inside the writer guard and takes the locks in this order (`LuaCtx::publication_claim`). Hosts outside a Lua invocation use `session_coordinator::publication_claim`, which is the CAS alone. The claim callback must not take actor, manager, or option-catalog locks.

Invalidation before the claim wins. Plugin revocation, the invocation ending, cancellation, or a dropped waiter moves the publication to `CANCELLED`. The staged file is discarded, the operation fails, and the finalizer never runs, so the actor, public options, host snapshot and disk all keep their previous state. A lost session lock or a failed rename has the same effect.

After the claim, nothing can cancel the publication. The commit runs on an actor-owned task or the actor runner, so dropping the waiter does not drop it. Once the rename succeeds the actor commits, and the `PreparedFinalization` installs reserved public options and the host snapshot before observers see the new configuration. A publish callback that returns success without claiming is treated as a failure.

## The actor

The actor API is exported from `maki-agent::actor` and re-exported from `maki-agent`.

### Main types

- `AgentActorHandle`: cloneable handle to one persistent actor.
- `ActorBackend`: object-safe async adapter implemented by a frontend.
- `TurnContext`: stable identity, cancellation, correlation, interrupt source, and optional managed-turn capability for one execution.
- `TurnTicket`: exact waiter allocated when an explicit turn is admitted.
- `ActorSnapshot`: lifecycle, run status, queue projection, latest outcome, and cumulative usage.
- `ActorLifecycle`: `Open`, `Closed`, or `Shutdown`.
- `ActorStatus`: `Idle` or `Running(TurnId)`.
- `BackendResult`: distinguishes a real entered run, setup failure, and non-turn control results.

### Explicit turn admission

```rust
let ticket = actor.admit_turn(input, event_sender, correlation)?;
let turn_id = ticket.turn_id();
let outcome = ticket.wait().await;
```

`admit_turn` allocates a `TurnId` and ticket immediately, then places the work at the back of the actor's single FIFO. Admission and permanent closure linearize under the actor state lock. Every successful explicit admission is terminalized even when it is removed, cleared, cancelled before execution, or drained by close/shutdown.

Outcomes remain queryable with `outcome(turn_id)`. `wait_outcome(turn_id)` either observes the retained result or waits on the already-registered ticket without a lookup/finalization race. A late waiter returns the retained result.

The actor returns to `Idle` before publishing the retained outcome and resolving its ticket. Therefore completion of a ticket means the actor is immediately reusable.

### Shared ordinary-message submission

TUI main input, child input, Lua `Agent:send` with `when = "join"`, and headless prompts (print, SDK, ACP, through `headless::RootSubmission`) use `AgentActorHandle::submit`, so a message sent while a turn runs joins it in every frontend. SDK and ACP submit under their own bookkeeping lock and record the turn's presentation before its events can reach their pump; an ACP prompt that joins is answered with the turn it joined, and one that gets a new turn waits behind the running one. An ordinary message starts work or joins compatible work whose instruction admission remains open. The call returns the `TurnTicket` covering that message. Several submissions can share one turn; each pending message has its own stable `SubmissionId` for queue projection, consumption, and removal.

The actor orders submissions with configuration, working-directory changes, controls, and explicit turns. Those operations prevent later messages from joining earlier work across the operation. Every backend consumes pending inputs before an eligible model request, preserving separate messages, images, preambles, and presentation metadata. Admission and finish share actor-state synchronization so an input joins the open turn or receives a new ticket.

`admit_turn` still queues a distinct turn, and idle-only admission rejects a busy actor. These policies do not require a separate root scheduler. Controls and compact requests use the FIFO but never become turns.

`CompletionPolicy::Independent` lets the main agent finish while children run. `CompletionPolicy::JoinReportingChildren` makes non-main agents wait for reporting children before completing. Both policies use the same submission mechanism. See [Agent work](../site/docs/content/agent-work/_index.md) for user-visible queue, cancellation, and late-report behavior.

### Backend contract

`ActorBackend` has three methods:

- `run_turn`: execute an admitted turn against actor-owned history.
- `run_control`: execute a standalone control with no turn identity.
- `run_compact`: compact history with no turn identity.

A turn backend returns `BackendResult::EnteredRun(outcome)` only after `Agent::run` has already attempted terminal event delivery. The actor retains that outcome but must not emit it again. A failed terminal checkpoint turns that outcome into a failure that keeps the turn's usage and cost. If setup fails before entering `Agent::run`, the backend reports `SetupFailed`, optionally with its specific failure; the actor synthesizes and delivers the sole failed outcome, and the turn has no history.

The `AgentInput` is moved exactly once from queue to backend. It is intentionally not cloned to paper over ownership or lifetime problems.

### Cancellation and closure

Actor operations have different semantics:

| Operation | Effect | Actor reusable? |
|---|---|---|
| `cancel_turn(turn_id)` | Cancels one queued, active, or pop/install-gap turn | Yes |
| `cancel_existing()` / `cancel_all()` | Atomically cuts active and currently queued work | Yes |
| `cancel_correlation(...)` | Cancels work admitted under a correlation key (headless cancel requests) and supports precancellation | Yes |
| `remove` / `clear` | Removes queued work; explicit admissions terminalize as cancelled | Yes |
| `close()` | Rejects admission, cancels active work as `Closed`, drains queue, exits runner | No |
| `shutdown()` | Rejects admission, cancels active work as `Shutdown`, drains queue, exits runner | No |

The reusable cancellation cut spans active removal and queue draining under the actor state/queue linearization. Work admitted after the cut survives. The first installed terminal lifecycle and cancellation reason wins; repeated close/shutdown calls are no-ops.

Ordinary completion, failure, and user cancellation do not close an actor.

## The manager and graph

`AgentManagerHandle` owns one graph for one outer session. It is the production factory for that root and all descendants created from managed Lua tool execution or trusted plugin handlers.

The graph stores each node's:

- `AgentId`
- optional parent
- immutable root id and depth
- child ids
- graph lifecycle
- actor handle and runner task
- metadata: display label and spawning tool-use id

Graph lifecycle is distinct from actor lifecycle:

- `Reserved`: capacity and edge are reserved while the factory runs.
- `Live`: actor committed and usable.
- `Closing`: permanently closing but may still have owned runtime work.
- `Closed`: runner settled.
- `Removed`: retained tombstone state.

Closed nodes remain inspectable for the manager lifetime and do not consume live-node or direct-child capacity. `Removed` is present in the public enum and is also non-capacity-consuming, but the current manager has no observed transition into it; reservation rollback removes the node from the map instead. A turn ending does not change graph lifecycle.

### Construction APIs

```rust
let manager = AgentManagerHandle::new(limits)?;
let root = manager.create_root(initial_messages, mirror, backend)?;

// Only from a currently active managed turn:
let child = manager.spawn_child(&current, metadata, initial_messages, mirror, backend)?;
```

`create_root` succeeds once; frontends use `create_root_deferred_with_config` so the root factory can run after the manager exists. `spawn_child` requires `CurrentManagedTurn`, a manager-issued capability for a specific active `(AgentId, TurnId)` and manager generation/nonce. `spawn_child_trusted_with_config` is the host-only path for trusted plugin handlers; it validates a live parent reference instead and must not be exposed as tool authority. The manager reserves topology and capacity under its graph lock, runs the caller's factory without that lock, then commits or rolls back. Returned factory errors and panics restore the reservation and capacity.

No graph lock may be held across an await, actor call, backend call, or factory call that can re-enter agent execution.

### Inspection and handles

- `limits()` and `generation()` expose manager metadata.
- `root_id()` and `root()` return the committed/reserved root.
- `lookup(id)` and `list()` return `AgentRef`s.
- `node(id)` returns one `AgentNodeSnapshot`.
- `snapshot()` returns all nodes sorted deterministically by depth and id.
- `actor(id)` returns the actor only for a live node.
- `AgentRef` bundles manager provenance with an id and provides `id`, `actor`, `snapshot`, `cancel`, and `close_subtree`.

`AgentNodeSnapshot` and graph ids describe state. They do not grant delegated spawn or wait authority.

### Limits

The manager enforces these per-outer-session limits:

| Configuration key | Default | Meaning |
|---|---:|---|
| `agent.max_concurrent_agent_turns` | 8 | Managed turns executing across the graph |
| `agent.max_agent_depth` | 4 | Maximum child-edge depth; root depth is 0 |
| `agent.max_children_per_agent` | 16 | Live or reserved direct children of one node |
| `agent.max_live_agents` | 64 | Live, reserved, or closing nodes including root |

Every value must be at least one. Limit checks and graph reservation are atomic. The manager's `max_concurrent_agent_turns` limiter is the only cross-agent turn limit.

### Managed turn authority

A manager-created actor receives an internal managed-admission hook. Before its backend executes, the runner obtains global capacity and activates a unique guard. The resulting `CurrentManagedTurn` carries:

- manager identity/generation
- current `AgentId`
- current `TurnId`
- an unforgeable active-turn nonce
- the turn's permit lease

Clones carry a restricted token, not the unique activation guard. Validation checks the manager and the still-active exact turn. A token retained after the turn ends becomes unusable. Agent ids, parent links, TUI focus, task ids, and tool-use ids are never bearer authority.

This context propagates through `AgentParams` and `ToolContext` into the Lua invocation scope. `maki.agent.spawn(ctx, ...)` requires the invocation's current managed authority to exactly match the supplied `ctx`; stale, context-free, or wrong-manager attempts fail before child creation. A managed child receives its own context when it later executes, so it can create a grandchild subject to limits.

### Cross-agent concurrency and child waits

The manager semaphore counts managed turns actively executing across the whole graph, not actors or queued work. Per-agent FIFO still belongs to each actor.

A parent that blocks on its own descendant must not consume the last permit and deadlock that child. `TurnPermitLease` therefore supports validated descendant waits:

1. Validate active parent authority, descendant topology, actor identity, and ticket ownership.
2. Register an independently driven watcher for the exact child ticket.
3. Suspend the parent lease. The first suspension releases physical capacity; concurrent child waits share the suspension count.
4. Let the child execute under the freed capacity.
5. When the last suspension ends, require the parent execution poll gate to reacquire capacity.
6. Resume the waiting Lua continuation only after logical ownership is restored.

Production waits use `TurnPermitLease::observe_descendant`: Lua `AgentTurn:wait`, `Agent:prompt` and the reporting-children completion join. A timeout or dropped wait ends the watcher, never the child. Parent cancellation signals its watchers and still reacquires capacity before polling backend cleanup, so cleanup cannot run outside the global limit. Ticket ownership is checked against the actor identity the ticket was issued by, so a child that finishes before the watcher registers is still observed.

A wait without a current managed invocation (a trusted plugin handler) uses an ordinary exact-ticket wait. The child remains subject to manager capacity, but there is no parent permit to yield.

### Graph cancellation and shutdown

| Manager operation | Scope | Permanent? |
|---|---|---|
| `cancel_agent(id)` | Existing work on one actor | No |
| `cancel_subtree(id)` | Existing work on the captured node and descendants | No |
| `close_descendants(id)` | Descendants, not the selected node | Yes |
| `close_subtree(id)` | Selected node and descendants | Yes |
| `shutdown(timeout)` | Entire graph and manager admission | Yes |

Reusable subtree cancellation captures a graph revision, then applies one actor cancellation cut outside the graph lock. A child reserved before the graph cut is marked for cancellation when it commits; a child created after the cut survives. Permanent close marks nodes before actor closure so no descendant can commit beneath a closing branch.

`shutdown` stops graph admission, marks nodes closing, signals actor shutdown, and waits up to one caller-provided aggregate deadline. It returns disjoint `ShutdownReport { joined, timed_out }` lists. If work times out, a detached reaper retains manager, limiter, watchers, and runner ownership until cleanup settles.

Dropping ordinary `AgentRef` or actor-handle clones does not cancel work.

## Frontend integration

### TUI

Each prepared TUI session runtime creates an `AgentManagerHandle`, creates exactly one root through it, and obtains the root actor from the returned `AgentRef`. The root backend still owns dynamic TUI preparation such as provider/model selection, prompts, tools, MCP state, cwd/instruction refresh, event stamping, and control handling.

The TUI queue is a presentation facade over the actor's one scheduling queue. It must not become a shadow scheduler. Every envelope carries the actor's `ActorWorkIdentity`, so `run_id` values are presentation and cancellation fences only.

Live child routing uses `AgentId`; task ids and tool-use ids remain labels and history correlation. Restored child tabs without a live `AgentId` are display-only. The UI is a flat list of agents, not a graph navigator.

Session runtime replacement is prepared inertly and activated as a transaction. The new manager/root, mailbox, command target, provider, permissions, and session-lock lease are not partially published before activation. Old manager shutdown starts only after the replacement is active.

### Lua agent API

`maki.agent.spawn(ctx, opts)` is the only way to create a child agent. It requires an active managed turn or a trusted manager-backed invocation and fails closed otherwise.

- Spawn creates a child through the manager and returns an `Agent` handle.
- `Agent:send` admits a turn only when the agent has no active, preparing or queued work; `Agent:prompt` does the same and waits for that exact turn.
- `Agent:enqueue` queues a distinct turn; `Agent:submit` uses the shared submission path above.
- `Agent:close` permanently closes the managed subtree.
- Interactive child cancellation uses reusable actor cancellation and keeps the agent available for later turns.

A descendant's cancellation token is independent of the spawning parent turn. Background children can outlive that parent turn. Structured outcomes and history are stamped for TUI routing, while presentation notifications must not be inserted into provider history.

`maki.async.run` inherits the originating task's exact managed authority, so its worker may act only while that turn remains active. Timer fires intentionally start without registration-time managed authority because they may run after the turn ends.

The bundled task plugin's ids are labels, not graph identity.

### Frontend roots

TUI, print, ACP and SDK all execute through a manager-owned root created by the configured or deferred root factory. No frontend constructs an unmanaged actor or a bare low-level agent for prompt execution. Print, SDK and ACP bind their root through `maki_lua::orchestration::bind_frontend_root`, and their root configuration and history commit through the session coordinator's durable publication. SDK and ACP coordinators write the session log; print's writes nothing (`accept_all`), so the same commits apply without reaching disk. The SDK and ACP report a turn's terminal result only after its history checkpoint settles.

## Known implementation caveats

These are current code observations, not intended invariants:

- `ActorBackend::run_compact` is documented and checked by the runner as returning `BackendResult::CompactDone`, but the TUI backend returns `ControlFailed` on the error path. The user-facing compaction event comes from `agent::compact`, but the actor runner logs the backend result as unexpected.
- `GraphLifecycle::Removed` is defined but is not currently assigned by the manager.
- Manager shutdown uses a bounded 10 ms poll for runner completion before joining or handing ownership to the reaper.

Keep these visible when touching the relevant code. Fixing them should update or remove the caveat and add focused regression coverage.

## Deferred work

The following work is not part of the current implementation contract:

- agent graph, outcome, and pending-delivery persistence, and restart recovery of live children (root and child configuration and history are saved; restored child tabs are display-only)
- guest-defined agent presets and complete guest-defined mode bundles
- guest implementations of build and plan workflows
- graph/tree UI, arbitrary-agent focus, and a uniform full promptbox for every focused agent
- removal of the native mode enum and remaining host special cases
- queue coalescing and contextual automode

Runtime `AgentId` and `TurnId` values are not yet restart-durable. Graph snapshots and tombstones are in-memory only. Do not document proposal examples from `.agents/` as available APIs until their delivery lands.

## Source map

| Concern | Primary implementation |
|---|---|
| IDs, outcomes, failure taxonomy, events | `maki-agent/src/types.rs` |
| Low-level provider/tool run | `maki-agent/src/agent/run.rs` |
| Actor API and retained state | `maki-agent/src/actor/mod.rs` |
| Queue and interrupt extraction | `maki-agent/src/actor/queue.rs` |
| Actor execution state machine | `maki-agent/src/actor/runner.rs` |
| Tickets and exact waits | `maki-agent/src/actor/tickets.rs` |
| Backend and work types | `maki-agent/src/actor/types.rs` |
| Manager graph and permit machinery | `maki-agent/src/manager/mod.rs` |
| Manager public projections | `maki-agent/src/manager/types.rs` |
| Manager errors | `maki-agent/src/manager/manager_error.rs` |
| TUI root construction/backend | `maki-ui/src/agent/mod.rs`, `maki-ui/src/agent/agent_loop.rs` |
| TUI cancellation routing | `maki-ui/src/agent/command_router.rs` |
| Lua agent backend and API | `maki-lua/src/api/agent.rs`, `maki-lua/src/api/agent/handles/` |
| Lua orchestration, targets and plugin authority | `maki-lua/src/orchestration.rs`, `maki-lua/src/orchestration/targets.rs` |
| Prepared operations and durable commit | `maki-agent/src/actor/prepared.rs`, `maki-agent/src/actor/history_edit.rs` |
| Publication state and session lock guard | `maki-storage/src/checkpoint.rs`, `maki-storage/src/session_lock.rs` |
| Session coordinator and checkpoint projection | `maki-agent/src/session_coordinator.rs`, `maki-agent/src/session_checkpoint.rs` |
| Print, SDK and ACP backend | `maki-agent/src/headless.rs` |
| Lua turn-scoped authority | `maki-lua/src/runtime.rs`, `maki-lua/src/api/util/ctx.rs` |
| Configured graph limits | `maki-config/src/lib.rs` |

For the non-negotiable maintenance rules derived from this design, read [Agent invariants](agent-invariants.md).
