# Agent invariants

These invariants cover the actor, manager and agent frontends as they are currently implemented. A change to an invariant should update its implementation, regression coverage, and applicable contract documentation.

## Identity and authority

1. `AgentId` and `TurnId` are nominal, distinct types. Never substitute task ids, provider tool-use ids, TUI `run_id`, `parent_tool_use_id`, labels, or chat indexes.
2. One actor keeps one stable `AgentId` for its lifetime. Every explicit accepted turn receives one fresh `TurnId`.
3. Graph topology is descriptive, not authority. Knowing an id or parent edge must not grant spawn, wait, cancellation, or ancestor/sibling control.
4. Delegated managed operations require a manager-issued capability for the exact active `(manager generation, AgentId, TurnId, nonce)`.
5. A retained capability becomes invalid as soon as its managed turn guard ends. Lua invocation scope must validate the current task's authority, not creation-time context cached in a handle.
6. Live TUI routing keys descendants by `AgentId`. Task ids and tool-use ids may label or correlate output but may collide.
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
    - `close`, `close_subtree`, and `shutdown` reject future work.
27. A reusable actor cancellation is one atomic cut across active work, queued work, and the queue-pop/active-install gap. Work admitted after the cut survives.
28. A reusable graph cancellation captures one graph cut. Reserved descendants before the cut are cancelled when committed; descendants created after it survive.
29. Permanent graph closure marks topology closing before invoking actor closure, preventing a child factory from committing beneath the closed branch.
30. The first cancellation reason or permanent actor lifecycle wins. Later close/shutdown/cancel races must not rewrite an outcome already observable or emitted.
31. Close and shutdown are idempotent, reject later admission, cancel active work, terminalize queued explicit turns, and wake the runner.
32. Child closure never closes ancestors or siblings. Dropping ordinary handles never cancels work.
33. A descendant's runtime cancellation is independent of the parent turn. A background child may outlive the turn that spawned it.
34. Interactive child cancellation is reusable. `task_despawn`, `Agent:close`, subtree close, and outer-session shutdown are permanent.

## Manager graph and factories

35. Each outer session (TUI, print, SDK or ACP) owns exactly one manager and exactly one root. Every managed descendant, including grandchildren, is created through that manager.
36. The manager owns topology, runner tasks, wait watchers, limits, and global turn permits. It must not duplicate actor queue, history, lifecycle scheduling, tickets, or outcome storage.
37. Root depth is zero. Parent, root, and depth relationships are immutable after reservation.
38. Limit checks and graph reservation are atomic. Reserved and closing nodes consume capacity; settled closed/removed nodes do not.
39. Actor/backend factories run outside the graph lock. Returned errors, panics, concurrent closure, and failed commit all roll reservations back exactly once.
40. Never hold the graph lock across an await, actor call, backend call, factory invocation, task join, or any path that can re-enter tool dispatch.
41. Graph snapshots may include closed tombstones for the manager lifetime. They are in-memory observations, not persisted identity.
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
55. Manager shutdown owns and eventually joins or reaps actor runners and wait watchers. A timeout transfers ownership to the reaper rather than leaking tasks.

## History, events, and persistence

56. The actor exclusively owns mutable conversation history. Optional `SharedMessages` is a mirror, not a competing owner.
57. Restored history is sanitized and published synchronously before an actor handle escapes.
58. Presentation-only failure/notification text must not be inserted into provider history as model output.
59. Structured child outcomes and history must retain the live child's `AgentId`; stale presentation filtering must not discard later events from a reusable child.
60. Terminal child deduplication keys by `(AgentId, TurnId)`, not a task id alone. Reusing a child must allow later distinct turns through.
61. `ActorWorkIdentity` carries runtime identity through admission, events and settlement. A frontend `run_id` is presentation only and must not become runtime identity, topology, or authorization.
62. Agents use only the manager's per-outer-session concurrency limit. Do not add a second process-wide turn semaphore.
63. `maki.async.run` inherits only the originating turn's managed authority. Timer fires intentionally inherit none and must fail closed for managed operations.
64. The Lua Agent API and the task plugin may adapt actor state, result text, capture values, and cumulative usage, but they must not become a second lifecycle source.
65. Durable configuration and history edits follow [durable publication](agent-system.md#durable-publication): the actor commits only after the claim, invalidation before the claim leaves actor, options, host and disk unchanged, and nothing suppresses finalization after it.
66. SDK and ACP send a turn's terminal result only after its history checkpoint settles. Print publishes through the same path into a writer that saves nothing: its root model, mode and cwd changes apply, and nothing reaches disk.

## Current scope boundary

67. Actor config covers model, prompt, tools, permissions, modes, and request options. Do not add a second writable copy of those values in a frontend or coordinator; read them from the actor.
68. Do not assume agent graph topology, ids, queued work, outcomes, or notifications survive process restart.
69. Do not weaken these current invariants to imitate a future design. Land the future ownership model first, migrate all affected callers, and update these docs with the new verified contract.

## Verification expectations

Changes in this area should use deterministic channels, barriers, and fake providers. Avoid sleeps and polling in lifecycle/race tests. Test the layer whose invariant changed, and run crate-scoped checks before the project-wide lint, test, formatting, and generated-document checks.
