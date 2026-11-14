# Agent system

The agent runtime uses a per-session journal, durable agent identities, resident actors, and a per-outer-session manager. See the [Lua API reference](../site/docs/content/lua-api/_index.md) and [plugin guide](../site/docs/content/plugins/_index.md) for the public contract.

## Vocabulary

- Outer session: one frontend session (TUI, print, SDK or ACP) that owns one agent manager and one rooted agent graph.
- Agent: one durable graph node with an `AgentId`, history, configuration, and lifecycle. An open node can have a resident actor or remain dormant.
- Root agent: the graph node with no parent. It uses the same actor implementation as descendants.
- Turn: one accepted model/tool run, identified by a `TurnId` and ending in one `TurnOutcome`.
- Actor: the per-agent scheduler and state owner.
- Manager: the per-outer-session graph, factory, authority, task, and cross-agent concurrency owner.
- Subagent and task: product-facing names for descendant agents. They are not separate core runtime types.
- Correlation id: a frontend or protocol value such as a headless cancel key or provider tool-use id. It is not agent identity or authority. The bundled task plugin uses the durable agent id as its task id.

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

The core is dependency-neutral. `makima-agent` does not depend on `makima-ui` or `makima-lua`; those crates implement `ActorBackend` to prepare and execute their own turns against actor-owned history. `makima-agent::headless` holds the print, SDK and ACP backend.

## Turn lifecycle

`AgentId` and `TurnId` are nominal wrappers around `MakimaId`. They use the same compact UUIDv7/base58 representation but cannot be interchanged by the Rust type system.

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

An actor commit publishes its identity, generation, and full configuration to frontend projections. Provider slots and coordinator reads do not supply managed execution settings. Configuration checkpoints record per-agent Config state and project compatible header and metadata fields without replacing history. The user-visible behaviour is described in [Modes: Configuration changes](../site/docs/content/modes/_index.md#configuration-changes).

### Session state audit

| State | Authority and restoration |
|---|---|
| Agent identity, ancestry, lifecycle, spawn specification and usage | Ordered agent revisions. Reservations, resident actors and runner tasks are rebuilt. |
| Transcripts, tool outputs, observations and posts | Session message, child-message, output and post records. Conversation continuation metadata and compaction metadata belong to the matching transcript epoch. |
| Turn admissions, outcomes and deliveries | Journal records. Recovery interrupts admitted unfinished turns without executing them again. Terminal delivery records prevent duplicate observations after restart and compaction. |
| Model, thinking, effective fast setting, workflow, cwd and resolved mode | Per-agent `Config` records. Provider instances, model capabilities, runtime generations and caches are rebuilt. The spawn ceiling remains separate from mutable mode configuration. |
| Deferred fast intent | A model-pinned internal Config option. Discovery consumes the intent with the committed setting change. |
| Plugin options | All current values belong to Config history. Resume applies values whose current definitions declare persistence. Full-state rewind restores all recorded values. |
| Permission grants and explicit YOLO intent | Session-scoped `Rules` records. Invocation-only YOLO, config/project/plugin rules and cwd-derived boundary checks are rebuilt. Allow-once answers and pending prompts are discarded. |
| Plan path, written status, approved content and staged implementation selection | The session-scoped `plan/state` slot records per-agent entries. Legacy metadata migrates into this slot. Approval that clears context creates an implementation session with a durable parent link. |
| Draft, inactive root text queue, context display, title and billed usage | Session metadata or the existing usage records. Draft and queued attachments are not retained. Pending compaction requests are discarded. |
| Completed shell output not yet submitted to a turn | Not retained. Once submitted, it belongs to the transcript. |
| Provider sessions, MCP transports, background invocations and session locks | Rebuilt or interrupted. No live resource handle is deserialized. |
| Plugin registrations, callbacks, timers and VM tables | Rebuilt when plugins load. `makima.state` stores registered, versioned JSON values in agent or session scope. VM tables, callbacks, timers and registrations are not stored. |
| Focus, panels, scroll position and expanded rows | Transient UI state, except the existing persisted tool presentation state. Input history retains its separate storage. |

Runtime configuration epochs and provider metadata are not independent durable settings. A restored actor receives its resolved mode definition with the configuration before admission. Saved mode snapshots do not consult changed or removed registry definitions. An unusable saved plan file requires a visible Build fallback; the fallback must pass through the normal configuration commit path.

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

## Session journal and recovery

`SessionJournal` gives agent revisions and state changes one monotonic session sequence. Config, rules, recorded context, plugin slots, turn admissions and results, delivery disposition, file changes, and restoration boundaries share that order. Agent scope and session scope are explicit. Actor histories remain the mutable transcript authority; journal snapshots include the matching histories for checkpoint publication.

Session JSONL format v3 stores agent revisions, state records, branch descriptors, restore references, and transcript records. The reader imports v2, but every save writes v3. There is no v2 downgrade path. Grouped boundary payloads use the `boundary-pool-v1` marker and require the supported `boundary-pools-v1` header feature. Writers emit this feature only when grouped payloads are present. Legacy inline snapshots remain readable. Older v3 binaries without this feature reject pooled checkpoints; this does not make every v3 checkpoint incompatible with every older v3 binary. Legacy import assigns durable identities when the migrated checkpoint is published; a transient imported identity must not be used as a stable restore endpoint before that publication. Unknown format versions, unsupported header features, and complete records with unsupported schemas fail loading rather than being silently discarded. Torn non-JSON appends can be skipped. Later schema additions require a named feature gate so older v3 binaries reject files they cannot preserve.

Recovery sanitizes transcripts, repairs unavailable tool results, restores retained boundary snapshots, and records `Interrupted` cancellation for admitted turns without terminal results. It never reissues interrupted model requests, tools, shell commands, or queued input. Inactive root text returns to the draft. Recovery publishes its checkpoint before exposing the runtime or admitting work. Root and child Config records, saved mode snapshots, Rules, registered state slots, and recorded instructions supply restored state; runtime providers, tools, permissions, and authority are rebuilt. Instruction-file changes produce a reload notice but do not replace recorded content. `/reload-context` journals the new root content; children retain the system prompt resolved at spawn.

Child result and delivery disposition records use `(AgentId, TurnId)`. Queued, consumed, withdrawn, and dropped deliveries survive restart and compaction. Recovery reconstructs pending delivery work from the journal. A consumed or withdrawn delivery cannot become eligible again because its presentation event was lost. This deduplication is separate from the best-effort event transport and applies only to state retained by a successful checkpoint.

### History epochs and restoration points

A restoration boundary names an agent, journal sequence, journal identity, and per-agent history epoch and length. Pinned boundary snapshots preserve the history needed across compaction. Conversation rewind replaces the selected agent and its recorded descendants, their configurations and state, and the applicable session-scoped state. Branch checkpoints preserve the displaced state for later restoration. Missing or pruned branch files are reported as expired.

Grouped snapshots store a shared canonical state and agent-revision prefix, raw histories, and an ordered list of covered boundary sequences. Each boundary lens records an epoch and exact prefix length for every identity retained at that boundary, including closed and retired agents. A later cleanup append cannot extend that cutoff. Old boundaries without an unambiguous durable cutoff are not added to a group. Pools contain no nested boundary snapshots or external pool/history dependencies. Copied state can still contain blob or offload references.

Recovery validates grouped snapshots without eagerly sanitizing every covered prefix. Restoring a selected boundary first truncates raw histories to its recorded cutoffs, then sanitizes those exact prefixes and remaps their offsets and epochs. Sanitizing a full shared history before truncation would incorrectly retain a later tool result or omit the unavailable-result repair required by an earlier boundary. Legacy inline singleton snapshots retain their existing exact-prefix recovery path.

History epochs are opaque identities with an order within one history namespace. Their numeric values are not portable identifiers. Import remints every distinct saved epoch into a fresh local epoch while preserving order, equality, and references from histories, boundaries, compactions, and nested pinned snapshots. Independent imported namespaces must not be merged by comparing their raw epoch numbers. Restore endpoint digests use ordered epoch ranks so a valid import does not change the semantic digest. Agent ids, journal state, messages, tool outputs, and retained history relationships still participate in that digest.

### File restore and publication recovery

Conversation-only, conversation-and-files, and files-only restores share the publication boundary. Files-only restore keeps the conversation revision. Cross-session rewind follows recorded parent links through planning and implementation sessions; it does not include unrelated branches. The preview lists affected paths and shell command count. Shell side effects are not undone.

Mutable-path tools capture an explicit Absent state or a content-addressed pre-image after acquiring the canonical write lock. Recording captures the actual post-image even when the tool reports failure. File restore holds the same locks and affected-agent transition barriers. Before any write, it verifies required blobs, source and target checkpoint sequence and semantic digest, path anchors, and expected content. Symlink, hardlink, path-escape, or unrelated content substitutions fail or become reported skips rather than overwrites. Linux and macOS support secure file restoration through atomic no-replace manifest publication. Other platforms reject file restoration; metadata-free compatibility startup and deletion require conservative read-only inspection. File-restoring choices fail explicitly on unsupported platforms.

A transition manifest records the source and target endpoints, before/after states, skipped paths, and an optional inverse reference. Blob references and the manifest are staged and synced before file writes. Per-path Applied, Conflict, RollbackIntent, RolledBack, and Complete records describe progress. A later Applied or RolledBack outcome resolves an earlier conflict for that path. Complete rejects unresolved conflicts and unfinished work; the engine verifies content under lock before recording outcomes. Redo inverts only applied paths, so skipped paths remain untouched.

The writer first confirms baseline checkpoint durability. File changes then precede publication of the target checkpoint and its transition reference. A failure before target publication aborts toward the recorded before-state. A visible target checkpoint requires forward completion even if directory sync fails or the waiter is cancelled. Target durability and file completion precede runtime activation and new admission. Sync failure or cancellation after publication retains the receipt, file locks, graph barriers, and actual renewing session-lease owners; it never silently rolls files back beneath the published conversation.

Manifest publication has its own failure boundary. A manifest renamed into place remains a pending transition if a later sync or identity check fails. Storage returns its reference in a publication-aware error. The core constructs an unresolved receipt with the acquired locks, and the caller attaches its transition barriers and lease owners before attempting settlement or retaining it.

Startup discovers pending transitions through both endpoints and validates references in the current checkpoint and retained branch checkpoints, including completed transitions needed for redo. Missing or corrupt referenced manifests fail closed. Reading Complete confirms progress-file durability before excluding the transition from pending repair. Startup claims or reuses owned leases for the manifest owner, source, and target in sorted order. An unresolved repair retains those owners and blocks admission. Session deletion holds the restore namespace guard and rejects unfinished or cross-session dependencies, including inverse references; deleting a referenced owner cannot discard its blobs or manifests.

## The actor

The actor API is exported from `makima-agent::actor` and re-exported from `makima-agent`.

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
- durable lifecycle and current graph lifecycle
- residency, incarnation reservation, actor handle and runner task
- metadata: display label and spawning tool-use id

Durable lifecycle is `Open`, `Closed`, or `Retired`. Explicit close and `task_despawn` record `Closed`; `Agent:retire` and `/task-retire` retire eligible accessible quiescent leaf identities and release retained capacity. Runtime teardown, detachment, and process shutdown release resident resources without changing an open durable lifecycle.

Graph lifecycle describes runtime state: `Reserved`, `Dormant`, `Live`, `Closing`, `Closed`, `Retired`, or `Removed`. `Removed` is defined but is not assigned by the manager. Residency is separate: `Dormant`, `Activating`, or `Resident`. Closed and retired records remain inspectable. Ordinary turn completion does not close the durable agent.

### Residency and reactivation

Recovery rebuilds open child records as dormant nodes. Reactivation reserves one incarnation, prepares the backend outside the graph lock, then revalidates the reservation before installing it. Concurrent callers share the activation result. Retired, closed, stale, or unrestorable records cannot reactivate. Tools rebind by recorded name against current handlers; missing handlers or providers fail before admission. The immutable spawn mode ceiling still limits later configuration.

A resident idle actor remains allocated during normal execution. The active-work limits do not count idle resident actors. The retained-identity limit bounds open and closed records; automatic idle actor eviction remains deferred. Detachment revokes the old incarnation and drains owned tool completions before releasing resident resources. Cleanup must not make the node appear reusable while detachment remains pending.

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
- `actor(id)` returns the actor only for a live resident node; reactivation is a separate prepared operation.
- `AgentRef` bundles manager provenance with an id and provides `id`, `actor`, `snapshot`, `cancel`, and `close_subtree`.

`AgentNodeSnapshot` and graph ids describe state. They do not grant delegated spawn or wait authority.

### Limits

The manager enforces these per-outer-session limits:

| Configuration key | Default | Meaning |
|---|---:|---|
| `agent.max_concurrent_agent_turns` | 8 | Managed turns executing across the graph |
| `agent.max_agent_depth` | 4 | Maximum child-edge depth; root depth is 0 |
| `agent.max_children_per_agent` | 16 | Direct children with outstanding work |
| `agent.max_active_agents` | 64 | Agents with outstanding work, including the root when active |
| `agent.max_retained_agents` | 1024 | Non-retired durable identities; imported legacy agents are exempt |

Every value must be at least one. The work ledger reserves active-agent and direct-child capacity atomically at admission and retains it through queued, running, delivery, and cleanup work. Reporting descendant work also retains capacity for its reporting ancestors. Idle resident and dormant agents consume neither limit. Retired identities release retained capacity. `max_live_agents` is an input alias for `max_active_agents`; new configuration uses the latter name. The manager's `max_concurrent_agent_turns` limiter is the only cross-agent turn limit.

### Managed turn authority

A manager-created actor receives an internal managed-admission hook. Before its backend executes, the runner obtains global capacity and activates a unique guard. The resulting `CurrentManagedTurn` carries:

- manager identity/generation
- current `AgentId`
- current `TurnId`
- an unforgeable active-turn nonce
- the turn's permit lease

Clones carry a restricted token, not the unique activation guard. Validation checks the manager and the still-active exact turn. A token retained after the turn ends becomes unusable. Agent ids, parent links, TUI focus, task ids, and tool-use ids are never bearer authority.

This context propagates through `AgentParams` and `ToolContext` into the Lua invocation scope. `makima.agent.spawn(ctx, ...)` requires the invocation's current managed authority to exactly match the supplied `ctx`; stale, context-free, or wrong-manager attempts fail before child creation. A managed child receives its own context when it later executes, so it can create a grandchild subject to limits.

### Cross-agent concurrency and child waits

The manager semaphore counts managed turns actively executing across the whole graph, not actors or queued work. Per-agent FIFO still belongs to each actor.

A parent that blocks on its own descendant must not consume the last permit and deadlock that child. `TurnPermitLease` therefore supports validated descendant waits:

1. Validate active parent authority, descendant topology, actor identity, and ticket ownership.
2. Register an independently driven watcher for the exact child ticket.
3. Suspend the parent lease. The first suspension releases physical capacity; concurrent child waits share the suspension count.
4. Let the child execute under the freed capacity.
5. When the last suspension ends, require the parent execution poll gate to reacquire capacity.
6. Resume the waiting Lua continuation only after logical ownership is restored.

Production waits use `TurnPermitLease::observe_descendant`: Lua `AgentTurn:wait` and the reporting-children completion join. A timeout or dropped wait ends the watcher, never the child. Parent cancellation signals its watchers and still reacquires capacity before polling backend cleanup, so cleanup cannot run outside the global limit. Ticket ownership is checked against the actor identity the ticket was issued by, so a child that finishes before the watcher registers is still observed.

A wait without a current managed invocation (a trusted plugin handler) uses an ordinary exact-ticket wait. The child remains subject to manager capacity, but there is no parent permit to yield.

### Graph cancellation and shutdown

| Manager operation | Scope | Permanent? |
|---|---|---|
| `cancel_agent(id)` | Existing work on one actor | No |
| `cancel_subtree(id)` | Existing work on the captured node and descendants | No |
| `close_descendants(id)` | Descendants, not the selected node | Yes |
| `close_subtree(id)` | Selected node and descendants | Yes |
| `shutdown(timeout)` | Entire runtime graph and manager admission | Runtime only; durable open agents remain open |

Reusable subtree cancellation captures a graph revision, then applies one actor cancellation cut outside the graph lock. A child reserved before the graph cut is marked for cancellation when it commits; a child created after the cut survives. Permanent close marks nodes before actor closure so no descendant can commit beneath a closing branch.

`shutdown` stops graph admission, marks nodes closing, signals actor shutdown, and waits up to one caller-provided aggregate deadline. It returns disjoint `ShutdownReport { joined, timed_out }` lists. If work times out, a detached reaper retains manager, limiter, watchers, and runner ownership until cleanup settles.

Dropping ordinary `AgentRef` or actor-handle clones does not cancel work.

## Frontend integration

### TUI

Each prepared TUI session runtime creates an `AgentManagerHandle`, creates exactly one root through it, and obtains the root actor from the returned `AgentRef`. The root backend still owns dynamic TUI preparation such as provider/model selection, prompts, tools, MCP state, cwd/instruction refresh, event stamping, and control handling.

The TUI queue is a presentation facade over the actor's one scheduling queue. It must not become a shadow scheduler. Every envelope carries the actor's `ActorWorkIdentity`, so `run_id` values are presentation and cancellation fences only.

Child routing uses durable `AgentId`. The bundled task plugin returns that id as `task_id` and an exact `turn_id` for each admitted turn. Restorable dormant children can reactivate; imported legacy children without a complete spawn specification remain display-only. Tool-use ids remain history correlation. The UI is a flat list of agents, not a graph navigator.

Session runtime replacement is prepared inertly and activated as a transaction. The new manager/root, mailbox, command target, provider, permissions, and session-lock lease are not partially published before activation. Old manager shutdown starts only after the replacement is active.

### Lua agent API

`makima.agent.spawn(ctx, opts)` is the only way to create a child agent. It requires an active managed turn or a trusted manager-backed invocation and fails closed otherwise.

- Spawn creates a child through the manager and returns an `Agent` handle.
- `Agent:send` requires `opts.when`: `idle` admits only when no work is active, preparing, or queued; `join` joins compatible work or starts a turn; `queue` admits a distinct queued turn.
- Accepted sends return an exact `AgentTurn`. `Agent:turn(id)` and `Agent:latest_turn()` resolve retained results without following future turns.
- `Agent:close` permanently closes the managed subtree.
- Interactive child cancellation uses reusable actor cancellation and keeps the agent available for later turns.

A descendant's cancellation token is independent of the spawning parent turn. Background children can outlive that parent turn. Structured outcomes and history are stamped for TUI routing, while presentation notifications must not be inserted into provider history.

`makima.async.run` inherits the originating task's exact managed authority, so its worker may act only while that turn remains active. Timer fires intentionally start without registration-time managed authority because they may run after the turn ends.

The bundled task plugin resolves durable task ids through the current manager. Exact-turn waits do not follow later turns, and result retention or session deletion can make an old turn unavailable.

### Frontend roots

TUI, print, ACP and SDK all execute through a manager-owned root created by the configured or deferred root factory. No frontend constructs an unmanaged actor or a bare low-level agent for prompt execution. Print, SDK and ACP bind their root through `makima_lua::orchestration::bind_frontend_root`, and their root configuration and history commit through the session coordinator's durable publication. SDK and ACP coordinators write the session log; print's writes nothing (`accept_all`), so the same commits apply without reaching disk. The SDK and ACP report a turn's terminal result only after its history checkpoint settles.

## Boundary snapshot measurements

The ignored `manager::rewind_tests::boundary_pin_profile::boundary_pin_growth_profile` fixture measures one child rewind while preserving 10, 100, or 500 boundaries from two outside agents. Each outside-agent turn contributes 1 KiB of response text. These are single debug-profile CPU runs on the same remote build box, with debug assertions enabled, not release-build latency targets. Log sizes and RSS use decimal MB.

| Outside boundaries | Initial log MB | Inline pins log MB | Grouped pool log MB | Inline peak RSS MB | Grouped peak RSS MB | Inline transition ms | Grouped transition ms | Inline load + recovery ms | Grouped load + recovery ms |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | 0.039 | 0.271 | 0.070 | 44.1 | 40.4 | 15.2 | 6.0 | 11.2 | 3.7 |
| 100 | 0.289 | 14.552 | 0.557 | 280.2 | 50.3 | 800.6 | 40.5 | 579.4 | 26.0 |
| 500 | 1.401 | 335.835 | 2.723 | 5677.9 | 92.8 | 18212.6 | 195.1 | 13723.0 | 128.3 |

Inline pins serialize one prefix per covered boundary; the grouped fixture writes one shared pool. Peak RSS is the maximum sampled process-lifetime peak and includes fixture construction and earlier phases. Transition time covers preparation plus publication/install and excludes measurement reporting. Load + recovery time sums only those named phases. The grouped run also publishes recovery and reloads the checkpoint to verify digest stability; its complete restart measurements are 17.7, 132.7, and 647.9 ms. The inline run omits those extra publication/reload checks, so total restart times are not directly comparable. These measurements show the retained-prefix growth cost for this fixture, not a bound for arbitrary session content.

## Known implementation caveats

These are current code observations, not intended invariants:

- `ActorBackend::run_compact` is documented and checked by the runner as returning `BackendResult::CompactDone`, but the TUI backend returns `ControlFailed` on the error path. The user-facing compaction event comes from `agent::compact`, but the actor runner logs the backend result as unexpected.
- `GraphLifecycle::Removed` is defined but is not currently assigned by the manager.
- Manager shutdown uses a bounded 10 ms poll for runner completion before joining or handing ownership to the reaper.

Keep these visible when touching the relevant code. Fixing them should update or remove the caveat and add focused regression coverage.

## Deferred work

The following work is not part of the current implementation contract:

- automatic eviction of idle resident actors
- guest-defined agent presets and complete guest-defined mode bundles
- guest implementations of build and plan workflows
- graph/tree UI, arbitrary-agent focus, and a uniform full promptbox for every focused agent
- removal of the native mode enum and remaining host special cases
- queue coalescing and contextual automode

Live execution resources, accepted input payloads, pending permission prompts, shell processes, and plugin VM objects do not survive restart. Durable ids and recorded results do not preserve a live invocation or grant authority in a new manager.

## Source map

| Concern | Primary implementation |
|---|---|
| IDs, outcomes, failure taxonomy, events | `makima-agent/src/types.rs` |
| Low-level provider/tool run | `makima-agent/src/agent/run.rs` |
| Actor API and retained state | `makima-agent/src/actor/mod.rs` |
| Queue and interrupt extraction | `makima-agent/src/actor/queue.rs` |
| Actor execution state machine | `makima-agent/src/actor/runner.rs` |
| Tickets and exact waits | `makima-agent/src/actor/tickets.rs` |
| Backend and work types | `makima-agent/src/actor/types.rs` |
| Manager graph and permit machinery | `makima-agent/src/manager/mod.rs` |
| Manager public projections | `makima-agent/src/manager/types.rs` |
| Work capacity and graph transitions | `makima-agent/src/manager/work.rs`, `makima-agent/src/manager/transition.rs`, `makima-agent/src/manager/rewind.rs` |
| Session journal and durable state fold | `makima-agent/src/journal.rs`, `makima-storage/src/session_state.rs` |
| Checkpoint-qualified restart recovery | `makima-agent/src/session_recovery.rs`, `makima-agent/src/session_checkpoint.rs` |
| Plugin state and recorded instructions | `makima-agent/src/state.rs`, `makima-lua/src/api/state.rs`, `makima-agent/src/context.rs` |
| Restoration boundaries and plan slots | `makima-agent/src/rewind.rs`, `makima-agent/src/parent_rewind.rs`, `makima-agent/src/plan_state.rs` |
| Secure file recording and restoration | `makima-agent/src/file_restore/`, `makima-storage/src/blobs.rs`, `makima-storage/src/restore.rs` |
| Session format, branches and semantic digests | `makima-storage/src/sessions.rs`, `makima-storage/src/restore_digest.rs` |
| TUI restore publication and activation | `makima-ui/src/event_loop/rewind.rs`, `makima-ui/src/storage_writer.rs` |
| Manager errors | `makima-agent/src/manager/manager_error.rs` |
| TUI root construction/backend | `makima-ui/src/agent/mod.rs`, `makima-ui/src/agent/agent_loop.rs` |
| TUI cancellation routing | `makima-ui/src/agent/command_router.rs` |
| Lua agent backend and API | `makima-lua/src/api/agent.rs`, `makima-lua/src/api/agent/handles/` |
| Lua orchestration, targets and plugin authority | `makima-lua/src/orchestration.rs`, `makima-lua/src/orchestration/targets.rs` |
| Prepared operations and durable commit | `makima-agent/src/actor/prepared.rs`, `makima-agent/src/actor/history_edit.rs` |
| Publication state and session lock guard | `makima-storage/src/checkpoint.rs`, `makima-storage/src/session_lock.rs` |
| Session coordinator and checkpoint projection | `makima-agent/src/session_coordinator.rs`, `makima-agent/src/session_checkpoint.rs` |
| Print, SDK and ACP backend | `makima-agent/src/headless.rs` |
| Lua turn-scoped authority | `makima-lua/src/runtime.rs`, `makima-lua/src/api/util/ctx.rs` |
| Configured graph limits | `makima-config/src/lib.rs` |

For the non-negotiable maintenance rules derived from this design, read [Agent invariants](agent-invariants.md).
