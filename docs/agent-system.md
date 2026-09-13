# Agent system

This document describes the agent implementation that exists now. It covers the reliable turn lifecycle, shared actor, and per-outer-session agent manager introduced by the first three Issue 24 deliveries.

It does not describe the intended final form of Issue 24 as if it already exists. See [Deferred work](#deferred-work) for the current boundary.

## Vocabulary

- **Outer session**: the TUI session runtime that owns one agent manager and one rooted agent graph.
- **Agent**: one graph node with a stable `AgentId`, persistent actor, history, FIFO queue, and reusable lifecycle.
- **Root agent**: the graph node with no parent. It uses the same actor implementation as descendants.
- **Turn**: one accepted model/tool run, identified by a `TurnId` and ending in one `TurnOutcome`.
- **Actor**: the per-agent scheduler and state owner.
- **Manager**: the per-outer-session graph, factory, authority, task, and cross-agent concurrency owner.
- **Subagent** and **task**: compatibility and product-facing names for descendant agents. They are not separate core runtime types.
- **Correlation id**: a frontend or protocol value such as a TUI `run_id`, task id, or provider tool-use id. It is not agent identity or authority.

## Architecture

```text
TUI outer session
└── AgentManagerHandle
    ├── graph, limits, runner tasks, turn permits, wait watchers
    └── root AgentActorHandle
        ├── history, FIFO queue, lifecycle, retained outcomes
        └── Lua-created child AgentActorHandle
            └── Lua-created grandchild AgentActorHandle

AgentActorHandle
└── ActorBackend (TUI or Lua adapter)
    └── transient Agent
        └── Agent::run(turn_id, input) -> TurnOutcome
```

The layers deliberately own different state:

| Layer | Owns | Must not own |
|---|---|---|
| `Agent::run` | One model/tool run, invocation-local usage and model-turn count, terminal outcome emission | The persistent queue, graph topology, or frontend lifecycle |
| `AgentActorHandle` | One agent's stable identity, history, FIFO scheduling, active cancellation, tickets, outcomes, cumulative usage | Parent-child topology, graph limits, or a second copy of frontend state |
| `AgentManagerHandle` | One outer session's rooted graph, actor factory, runner tasks, topology, spawn authority, global active-turn limit, subtree lifecycle | A second per-agent queue, history, or outcome store |
| TUI and Lua backends | Dynamic provider/model/tool/prompt preparation and compatibility presentation | Lifecycle truth or graph authority derived from presentation ids |

The core is dependency-neutral. `maki-agent` does not depend on `maki-ui` or `maki-lua`; those crates implement `ActorBackend` to prepare and execute their own turns against actor-owned history.

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

### Root queue compatibility path

The TUI root uses `rush(RootWork)` rather than explicit admission. A queued root input has no externally visible `TurnId` or ticket. When idle, the scheduler assigns an internal turn id and starts it. While another root turn is active, the same queue is exposed as an `InterruptSource`, allowing the input to fold into the active turn instead of creating an orphan turn.

For this compatibility path, `Agent::run` still emits the authoritative terminal event, but the actor does not retain a separately waitable root outcome. Do not convert root queue entries into explicit admissions without redesigning interrupt and queue-panel semantics.

Controls and compact requests also use the FIFO, but never become turns.

### Backend contract

`ActorBackend` has three methods:

- `run_turn`: execute an explicit admission or started root against actor-owned history.
- `run_control`: execute a standalone control with no turn identity.
- `run_compact`: compact history with no turn identity.

A turn backend returns `BackendResult::EnteredRun(outcome)` only after `Agent::run` has already attempted terminal event delivery. The actor retains that outcome but must not emit it again. If setup fails before entering `Agent::run`, the backend reports `SetupFailed`; the actor synthesizes and delivers the sole failed outcome.

The `AgentInput` is moved exactly once from queue to backend. It is intentionally not cloned to paper over ownership or lifetime problems.

### Cancellation and closure

Actor operations have different semantics:

| Operation | Effect | Actor reusable? |
|---|---|---|
| `cancel_turn(turn_id)` | Cancels one queued, active, or pop/install-gap turn | Yes |
| `cancel_existing()` / `cancel_all()` | Atomically cuts active and currently queued work | Yes |
| `cancel_correlation(...)` | Cancels matching compatibility work and supports precancellation | Yes |
| `remove` / `clear` | Removes queued work; explicit admissions terminalize as cancelled | Yes |
| `close()` | Rejects admission, cancels active work as `Closed`, drains queue, exits runner | No |
| `shutdown()` | Rejects admission, cancels active work as `Shutdown`, drains queue, exits runner | No |

The reusable cancellation cut spans active removal and queue draining under the actor state/queue linearization. Work admitted after the cut survives. The first installed terminal lifecycle and cancellation reason wins; repeated close/shutdown calls are no-ops.

Ordinary completion, failure, and user cancellation do not close an actor.

## The manager and graph

`AgentManagerHandle` owns one graph for one managed TUI outer session. It is the production factory for that root and all descendants created from managed Lua tool execution.

The graph stores each node's:

- `AgentId`
- optional parent
- immutable root id and depth
- child ids
- graph lifecycle
- actor handle and runner task
- optional compatibility metadata

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
let child = current.spawn_child(metadata, initial_messages, mirror, backend)?;
```

`create_root` succeeds once. `spawn_child` requires `CurrentManagedTurn`, a manager-issued capability for a specific active `(AgentId, TurnId)` and manager generation/nonce. The manager reserves topology and capacity under its graph lock, runs the caller's factory without that lock, then commits or rolls back. Returned factory errors and panics restore the reservation and capacity.

No graph lock may be held across an await, actor call, backend call, or factory call that can re-enter agent execution.

### Inspection and handles

- `limits()` and `generation()` expose manager metadata.
- `root_id()` returns the committed/reserved root identity.
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

Every value must be at least one. Limit checks and graph reservation are atomic. The deprecated `plugins.task.max_concurrent` is only an unmanaged-frontend compatibility fallback; managed TUI sessions do not apply both limits.

### Managed turn authority

A manager-created actor receives an internal managed-admission hook. Before its backend executes, the runner obtains global capacity and activates a unique guard. The resulting `CurrentManagedTurn` carries:

- manager identity/generation
- current `AgentId`
- current `TurnId`
- an unforgeable active-turn nonce
- the turn's permit lease

Clones carry a restricted token, not the unique activation guard. Validation checks the manager and the still-active exact turn. A token retained after the turn ends becomes unusable. Agent ids, parent links, TUI focus, task ids, and tool-use ids are never bearer authority.

This context propagates through `AgentParams` and `ToolContext` into the Lua invocation scope. `maki.agent.session(ctx, ...)` requires the invocation's current managed authority to exactly match the supplied `ctx`; stale, context-free, or wrong-manager attempts fail before child creation. A managed child receives its own context when it later executes, so it can create a grandchild subject to limits.

### Cross-agent concurrency and blocking child waits

The manager semaphore counts managed turns actively executing across the whole graph, not actors or queued work. Per-agent FIFO still belongs to each actor.

A parent that blocks on its own descendant must not consume the last permit and deadlock that child. `TurnPermitLease` therefore supports validated descendant waits:

1. Validate active parent authority, descendant topology, actor identity, and ticket ownership.
2. Register an independently driven watcher for the exact child ticket.
3. Suspend the parent lease. The first suspension releases physical capacity; concurrent child waits share the suspension count.
4. Let the child execute under the freed capacity.
5. When the last suspension ends, require the parent execution poll gate to reacquire capacity.
6. Resume the waiting Lua continuation only after logical ownership is restored.

Dropping a prompt wait cancels the watcher, not the child. A prompt timeout closes the child's subtree. Parent cancellation signals its watchers and still reacquires capacity before polling backend cleanup, so cleanup cannot run outside the global limit.

This machinery is an internal compatibility primitive, not the future public agent wait API.

There is a current race to account for when changing this path: turn retirement removes the actor's ticket registration before resolving the ticket, while managed wait registration verifies ownership through that registration. A sufficiently fast child can therefore terminalize between `admit_and_wait_for_descendant` admission and watcher registration, producing `TicketActorMismatch` even though the passed ticket itself belongs to the actor. The actor's direct `TurnTicket::wait` and `wait_outcome` paths do not have this race. Do not describe managed admit-and-wait as fully race-free until registration accepts an already-retained matching outcome or otherwise closes this window.

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

The TUI queue is a presentation facade over the actor's one scheduling queue. It must not become a shadow scheduler. Root `run_id` values remain stale-event and cancellation correlation only.

Live child routing uses `AgentId`; compatibility strings remain labels and protocol/history correlation. Restored child tabs without a live `AgentId` are display-only. The current UI remains a flat compatibility view, not a graph navigator.

Session runtime replacement is prepared inertly and activated as a transaction. The new manager/root, mailbox, command target, provider, permissions, and session-lock lease are not partially published before activation. Old manager shutdown starts only after the replacement is active.

### Lua compatibility sessions

`maki.agent.session(ctx, opts)` remains the existing public compatibility API.

- In a managed TUI turn, it creates a child through `CurrentManagedTurn::spawn_child`.
- In deferred unmanaged frontends, it still creates an unmanaged actor directly.
- `prompt` admits one exact child turn and waits for that result.
- `send` admits asynchronously through the same actor FIFO.
- `status` adapts actor state and cumulative usage to the existing `running | done | closed` shape.
- `close` and `task_despawn` permanently close the managed subtree.
- Interactive child cancellation uses reusable actor cancellation and keeps the session available for later turns.

A descendant's cancellation token is independent of the spawning parent turn. Background children can outlive that parent turn. Structured outcomes and history are stamped for TUI routing, while presentation notifications must not be inserted into provider history.

The bundled task API and current Session result shapes remain compatibility surfaces. Their ids are not graph identity.

### Deferred unmanaged frontends

Headless, ACP, SDK, and print paths still construct low-level agents or unmanaged actors directly where applicable. This is an explicit migration exception, not permission for managed TUI roots or descendants to bypass the manager.

## Known implementation caveats

These are current code observations, not intended invariants:

- Managed admit-and-wait has the fast-child ticket-registration race described above.
- `ActorBackend::run_compact` is documented and checked by the runner as returning `BackendResult::CompactDone`, but the TUI backend currently returns `ControlDone` or `ControlFailed`. The user-facing compaction event comes from `agent::compact`, but the actor runner logs the backend result as unexpected.
- `GraphLifecycle::Removed` is defined but is not currently assigned by the manager.
- Manager shutdown uses a bounded 10 ms poll for runner completion before joining or handing ownership to the reaper.

Keep these visible when touching the relevant code. Fixing them should update or remove the caveat and add focused regression coverage.

## Deferred work

The following Issue 24 work is not part of the current implementation contract:

- moving model, provider binding, prompt, tools, permissions, mode, and request options into durable per-agent state
- immutable queued policy snapshots and FIFO model/mode setter barriers
- first-class public Lua `Agent` and visibility-only `AgentRef` userdata
- public spawn, send, exact-turn wait, inspect, subscribe, and graph APIs
- replacing task polling and all compatibility Session/task adapters
- agent graph, outcome, pending-delivery, and policy persistence or restart recovery
- guest-defined agent presets and complete guest-defined mode bundles
- guest implementations of build and plan workflows
- graph/tree UI, arbitrary-agent focus, and a uniform full promptbox for every focused agent
- migration of headless, ACP, SDK, and print onto the manager
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
| Lua Session adapter | `maki-lua/src/api/agent.rs` |
| Lua turn-scoped authority | `maki-lua/src/runtime.rs`, `maki-lua/src/api/util/ctx.rs` |
| Configured graph limits | `maki-config/src/lib.rs` |

For the non-negotiable maintenance rules derived from this design, read [Agent invariants](agent-invariants.md).
