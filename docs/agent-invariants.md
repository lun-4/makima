# Agent invariants

These invariants cover the actor, manager and agent frontends as they are currently implemented. A change to an invariant should update its implementation, regression coverage, and applicable contract documentation.

## Identity and authority

1. `AgentId` and `TurnId` are nominal, distinct types. The bundled task plugin returns the durable `AgentId` as `task_id`; provider tool-use ids, TUI `run_id`, `parent_tool_use_id`, labels, and chat indexes remain correlation or presentation values.
2. One durable agent has at most one resident actor. Its `AgentId` survives restart. Every explicit accepted turn receives one fresh `TurnId`.
3. Graph topology is descriptive, not authority. Knowing an id or parent edge must not grant spawn, wait, cancellation, or ancestor/sibling control.
4. Delegated managed operations require a manager-issued capability for the exact active `(manager generation, AgentId, TurnId, nonce)`.
5. A retained capability becomes invalid as soon as its managed turn guard ends. Lua invocation scope must validate the current task's authority, not creation-time context cached in a handle.
6. TUI routing keys descendants by durable `AgentId`, including dormant restorable children. Tool-use ids may correlate output but may collide. Resolving a task id must still validate current manager provenance and authority.
7. A missing, closed, or foreign target is an error. Never fall back from the executing agent to the focused agent, the root, or a sole headless session.

## Turn terminalization

8. Every explicit turn successfully admitted to an actor reaches exactly one retained `TurnOutcome`: completed, failed, or cancelled.
9. Cancellation is a top-level terminal state, never a successful `DoneReason` or a `TurnFailure`.
10. `Agent::run` determines one authoritative outcome, attempts one terminal event delivery, and returns the same value even if the sink rejects it.
11. The actor must not re-emit `BackendResult::EnteredRun`; `Agent::run` already attempted delivery. The actor only synthesizes terminal delivery for work that never entered the run.
12. Event delivery is not lifecycle truth. Retention and ticket resolution must succeed independently of event-sink availability.
13. Duplicate finalization is inert. It must not change the retained outcome, cumulative usage, latest outcome, waiter result, or public delivery count.
14. Outcome usage and model-turn count are local to one turn. Actor cumulative usage is a separate sum of terminal outcomes.
15. The actor becomes `Idle` before it publishes retention and resolves the ticket. A completed exact wait therefore observes a reusable actor.
16. Ordinary completed, failed, and user-cancelled turns leave the actor and graph node live.
17. `TurnFailure` is the durable error snapshot. Do not reconstruct lifecycle failure from display events or assistant text.

## Admission, waiting, and work kinds

18. Explicit admission allocates and registers the ticket before work can finalize. Exact waits must handle both finalization-before-wait and wait-before-finalization.
19. Removing, clearing, closing, or shutting down must terminalize every affected queued explicit admission and resolve its waiter. Never drop accepted turn work silently.
20. TUI main input, child input, Lua `when = "join"` sends and headless prompts (print, SDK, ACP) use `AgentActorHandle::submit`, so input sent while a turn runs joins it in every frontend. Compatible submissions share the ticket for their execution; a closed admission or intervening operation requires a new turn.
21. A pending ordinary message has a stable `SubmissionId` distinct from its `TurnId`. Consumption or removal addresses that submission without changing another message or creating an extra terminal result for a shared turn.
22. Controls and compaction are not turns. They have no `TurnId`, no managed turn admission, and no retained `TurnOutcome`.
23. `AgentInput` has one ownership transfer from queue to backend. Do not clone input payloads to avoid fixing ownership or lifetime design.
24. Each actor orders configuration and admission through one operation FIFO and executes materialized work through its execution queue. UI queue projections and interrupt extraction must use actor-owned work, not a shadow deque.
25. Settlement publication must linearize with concurrent push so `QueueSettled` cannot race ahead of newly accepted work. The actor checks queue and ticket emptiness under its own lock before emitting the settled identity.

## Cancellation and closure

26. Reusable cancellation and permanent closure are different operations.
    - `cancel_turn`, `cancel_existing`, `cancel_agent`, and `cancel_subtree` leave actors/nodes reusable.
    - `close` and `close_subtree` durably reject future work. Runtime `shutdown` rejects admission in that runtime without closing an open durable agent.
27. A reusable actor cancellation is one atomic cut across active work, queued work, and the queue-pop/active-install gap. Work admitted after the cut survives.
28. A reusable graph cancellation captures one graph cut. Reserved descendants before the cut are cancelled when committed; descendants created after it survive.
29. Permanent graph closure marks topology closing before invoking actor closure, preventing a child factory from committing beneath the closed branch.
30. The first cancellation reason or permanent actor lifecycle wins. Later close/shutdown/cancel races must not rewrite an outcome already observable or emitted.
31. Close and shutdown are idempotent, reject later admission, cancel active work, terminalize queued explicit turns, and wake the runner.
32. Child closure never closes ancestors or siblings. Dropping ordinary handles never cancels work.
33. A descendant's runtime cancellation is independent of the parent turn. A background child may outlive the turn that spawned it.
34. Interactive child cancellation is reusable. `task_despawn`, `Agent:close`, and subtree close durably close the subtree. `/task-retire` and `Agent:retire` retire eligible accessible quiescent leaf identities and release retained capacity. Outer-session teardown and shutdown stop execution without recording durable closure.

## Manager graph and factories

35. Each outer session (TUI, print, SDK or ACP) owns exactly one manager and exactly one root. Every managed descendant, including grandchildren, is created through that manager.
36. The manager owns topology, runner tasks, wait watchers, limits, and global turn permits. It must not duplicate actor queue, history, lifecycle scheduling, tickets, or outcome storage.
37. Root depth is zero. Parent, root, and depth relationships are immutable after reservation.
38. Work admission reserves active-agent and direct-child capacity atomically. Queued, executing, delivery, and cleanup work retain capacity until settlement. Idle resident and dormant actors consume neither active-work limit; non-retired identities consume retained capacity except imported legacy records.
39. Actor/backend factories run outside the graph lock. Returned errors, panics, concurrent closure, and failed commit all roll reservations back exactly once.
40. The graph lock may enclose synchronous actor state operations such as cut capture. Drop it before applying a cut, awaiting, invoking a factory or backend, joining a task, or re-entering tool dispatch.
41. Durable agent records retain topology, spawn specification, lifecycle, usage, and identity across restart. Runtime residency and incarnation reservations are separate. Reactivation installs at most one resident actor and must reject a stale reservation or closed, retired, or unrestorable record.
    - `ResidentInitialization::finish_with` acquires the activation-completion read guard before graph validation. Once the attempt is marked finishing, manager-owned work retains that guard through publication and activation commit or rollback, even if the initiating waiter is dropped. Fallible host registration precedes this work.
    - Shutdown and detach acquire activation completion before journal publication. When these guards overlap the graph lock, the order is activation completion, journal publication, graph. No path may await activation completion or journal publication while holding the graph lock. The journal publication lock is distinct from its state mutex.
    - Shutdown waits for finishing activation. Detach defers cleanup when the completion guard is unavailable and never blocks a destructor. Close refuses an affected finishing subtree with `BusySubtree` before mutating it.
42. Root and descendant construction must not fall back to direct `AgentActorHandle::spawn`. Direct construction is allowed only in tests.

## Managed concurrency and descendant waits

43. `max_concurrent_agent_turns` limits executing managed turns across one graph. It does not count live agents or merely queued turns.
44. Per-agent serialization stays in the actor. The manager limiter must not become a second scheduler.
45. Managed permit acquisition happens after actor active cancellation is installed and before backend execution. Cancellation while waiting for capacity must settle without entering the backend.
46. While unsuspended, every opaque backend poll holds physical permit ownership for the entire poll. Capacity must not be released from underneath cleanup or a concurrent suspension race.
47. A blocking parent wait may yield capacity only after validating active authority, descendant relationship, child actor identity, and ticket ownership.
48. Multiple descendant waits from one parent share a reference-counted suspension. The first releases capacity; only the last retirement requires reacquisition.
49. Suspension guard drop is synchronous and non-blocking. The managed execution poll gate is the only authority that reacquires the semaphore.
50. A completed child waiter must not resume Lua/model/tool continuation until the parent lease reports ownership restored.
51. Dropping or timing out a wait ends its watcher, never the child.
52. A wait without a current managed invocation uses an ordinary exact-ticket wait and yields no parent permit.
53. Parent cancellation signals all registered watchers but does not bypass the global limit for backend cleanup.
54. Abnormal managed-execution drop closes lease registration, signals watchers, releases physical capacity, and revokes authority without creating an ownership cycle.
55. Manager shutdown owns and eventually joins or reaps actor runners and wait watchers. Shutdown waits for finishing activation before starting its timeout budget. The timeout bounds publication-guard acquisition and runner drain after activation completes, not the full shutdown call. A stuck protected initialization can therefore delay shutdown indefinitely. Once that wait completes, a timeout transfers remaining cleanup to the reaper rather than leaking tasks. `ShutdownReport` describes node joins at return; it is not a guarantee that retained cleanup has finished. Reaping requires a running executor and cannot guarantee completion after process exit. Frontend forced-exit deadlines remain best-effort and may interrupt cleanup; only the published journal prefix is recoverable.

## History, events, and persistence

56. The actor exclusively owns mutable conversation history. Optional `SharedMessages` is a mirror, not a competing owner.
57. Restored history is sanitized and published synchronously before an actor handle escapes.
58. Presentation-only failure/notification text must not be inserted into provider history as model output.
59. Structured child outcomes and history must retain the live child's `AgentId`; stale presentation filtering must not discard later events from a reusable child.
60. Terminal child deduplication keys by `(AgentId, TurnId)`, not a task id alone. Reusing a child must allow later distinct turns through.
61. `ActorWorkIdentity` carries runtime identity through admission, events and settlement. A frontend `run_id` is presentation only and must not become runtime identity, topology, or authorization.
62. Agents use only the manager's per-outer-session concurrency limit. Do not add a second process-wide turn semaphore.
63. `makima.async.run` inherits only the originating turn's managed authority. Timer fires intentionally inherit none and must fail closed for managed operations.
64. The Lua Agent API and the task plugin may adapt actor state, result text, capture values, and cumulative usage, but they must not become a second lifecycle source.
65. Durable configuration and history edits follow [durable publication](agent-system.md#durable-publication): the actor commits only after the claim, invalidation before the claim leaves actor, options, host and disk unchanged, and nothing suppresses finalization after it.
66. SDK and ACP send a turn's terminal result only after its history checkpoint settles. Print publishes through the same path into a writer that saves nothing: its root model, mode and cwd changes apply, and nothing reaches disk.

## Current scope boundary

67. Actor config covers model, prompt, tools, permissions, modes, and request options. Do not add a second writable copy of those values in a frontend or coordinator; read them from the actor.
68. Durable graph records, ids, retained results, configuration, state slots, and delivery disposition survive checkpointed restart. Active invocations, accepted input payloads, runtime resources, and permission prompts do not. Recovery terminalizes unfinished admissions as Interrupted and never reexecutes them.
69. Do not weaken these current invariants to imitate a future design. Land the future ownership model first, migrate all affected callers, and update these docs with the new verified contract.

## Journal and recovery

70. Agent revisions and state changes share one session journal sequence. Config, rules, context, plugin state, admissions, outcomes, and deliveries must not acquire independent competing orders.
71. Teardown never records Closed or Retired for an open durable agent. Detachment revokes the old incarnation and retains ownership through tool completion and cleanup before making resources reusable.
72. Recovery publishes sanitized history, interrupted results, and state before admission or observer activation. Configuration, permissions, plan state, registered plugin state, and recorded context must match that checkpoint.
73. Delivery deduplication keys by `(AgentId, TurnId)` and records consumed, withdrawn, or dropped disposition durably. Restart and compaction must not redeliver a terminally disposed result from retained state.
74. Plugin state uses registered, versioned JSON slots in explicit agent or session scope. Mutations validate authority and expected revision. Migration changes publish with the same journal transaction before observers run.
75. Session saves write v3; v2 import has no downgrade path. The first v3 release writes an empty header feature set. Unknown versions, unsupported feature names, and complete unsupported record schemas fail closed.
76. History epochs are opaque ordered identities within one namespace. Import remints distinct saved epochs while preserving order, equality, and all retained references. Independent imported namespaces must never be merged by equal raw epoch numbers.

## Lock order

The order applies to locks held at the same time. A synchronous state mutex must not be held across an await.

- Acquire activation completion before journal publication, and publication before the journal boundary when both are required. Acquire these guards before taking graph, actor, or journal state. History mutations that only need the boundary do not acquire publication while holding the boundary.
- Acquire manager graph state before actor state. Either may acquire journal state. Capture a close cut under the graph lock and apply it after releasing the graph lock. Terminal delivery releases source actor state before looking up the target in the graph. A journal commit builder runs under journal state and must not acquire graph, actor, or session coordinator state.
- Catalog publication reserves its journal publication and boundary guards before taking `DIRECTORY`. When both directory and catalog state are needed, acquire `DIRECTORY` first. Catalog decisions then acquire participant option state and journal state in that order. Batch publication acquires journal guards in journal address order. An operation must release the directory and catalog mutexes before awaiting a participant or publication guard.
- Journal state may enqueue writer work. The writer releases its status and attached-journal mutexes before changing journal state. It does not hold either mutex during writes, fsync, or receipt waits. The writer registry lock only protects the registry and is released before opening or closing a writer.

A terminal guard keeps the corresponding journal entry staged until delivery has been attempted. Dropping the guard releases it for conversion without acquiring publication or boundary guards, so a preparation waiting on that entry can proceed.

## Verification expectations

Changes in this area should use deterministic channels, barriers, and fake providers. Avoid sleeps and polling in lifecycle/race tests. Test the layer whose invariant changed, and run crate-scoped checks before the project-wide lint, test, formatting, and generated-document checks.
