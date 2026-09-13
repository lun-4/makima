# Agent invariants

This checklist describes current correctness requirements. A change that intentionally alters one of these rules should update the implementation, deterministic regression tests, and [`agent-system.md`](agent-system.md) together.

## Identity and authority

1. `AgentId` and `TurnId` are nominal, distinct types. Never substitute task ids, provider tool-use ids, TUI `run_id`, `parent_tool_use_id`, labels, or chat indexes.
2. One actor keeps one stable `AgentId` for its lifetime. Every explicit accepted turn receives one fresh `TurnId`.
3. Graph topology is descriptive, not authority. Knowing an id or parent edge must not grant spawn, wait, cancellation, or ancestor/sibling control.
4. Delegated managed operations require a manager-issued capability for the exact active `(manager generation, AgentId, TurnId, nonce)`.
5. A retained capability becomes invalid as soon as its managed turn guard ends. Lua invocation scope must validate the current task's authority, not creation-time context cached in a session.
6. Live TUI routing keys descendants by `AgentId`. Compatibility strings may label or correlate output but may collide.

## Turn terminalization

7. Every explicit turn successfully admitted to an actor reaches exactly one retained `TurnOutcome`: completed, failed, or cancelled.
8. Cancellation is a top-level terminal state, never a successful `DoneReason` or a `TurnFailure`.
9. `Agent::run` determines one authoritative outcome, attempts one terminal event delivery, and returns the same value even if the sink rejects it.
10. The actor must not re-emit `BackendResult::EnteredRun`; `Agent::run` already attempted delivery. The actor only synthesizes terminal delivery for work that never entered the run.
11. Event delivery is not lifecycle truth. Retention and ticket resolution must succeed independently of event-sink availability.
12. Duplicate finalization is inert. It must not change the retained outcome, cumulative usage, latest outcome, waiter result, or public delivery count.
13. Outcome usage and model-turn count are local to one turn. Actor cumulative usage is a separate sum of terminal outcomes.
14. The actor becomes `Idle` before it publishes retention and resolves the ticket. A completed exact wait therefore observes a reusable actor.
15. Ordinary completed, failed, and user-cancelled turns leave the actor and graph node live.
16. `TurnFailure` is the durable error snapshot. Do not reconstruct lifecycle failure from display events or assistant text.

## Admission, waiting, and work kinds

17. Explicit admission allocates and registers the ticket before work can finalize. Exact waits must handle both finalization-before-wait and wait-before-finalization.
18. Removing, clearing, closing, or shutting down must terminalize every affected queued explicit admission and resolve its waiter. Never drop accepted turn work silently.
19. Root queue work is a deliberate compatibility exception: it has no external `TurnId` or ticket before execution and may fold into the active root turn.
20. Root input folded by `InterruptSource` belongs to the active turn. It must not create another ticket, retained outcome, or terminal event.
21. Controls and compaction are not turns. They have no `TurnId`, no managed turn admission, and no retained `TurnOutcome`.
22. `AgentInput` has one ownership transfer from queue to backend. Do not clone input payloads to avoid fixing ownership or lifetime design.
23. Each actor has one FIFO scheduling queue. UI queue projections and interrupt extraction must read and mutate that same queue, not a shadow deque.
24. Queue-empty publication must linearize with concurrent push so `QueueDrained` cannot race ahead of newly accepted work.

## Cancellation and closure

25. Reusable cancellation and permanent closure are different operations.
    - `cancel_turn`, `cancel_existing`, `cancel_agent`, and `cancel_subtree` leave actors/nodes reusable.
    - `close`, `close_subtree`, and `shutdown` reject future work.
26. A reusable actor cancellation is one atomic cut across active work, queued work, and the queue-pop/active-install gap. Work admitted after the cut survives.
27. A reusable graph cancellation captures one graph cut. Reserved descendants before the cut are cancelled when committed; descendants created after it survive.
28. Permanent graph closure marks topology closing before invoking actor closure, preventing a child factory from committing beneath the closed branch.
29. The first cancellation reason or permanent actor lifecycle wins. Later close/shutdown/cancel races must not rewrite an outcome already observable or emitted.
30. Close and shutdown are idempotent, reject later admission, cancel active work, terminalize queued explicit turns, and wake the runner.
31. Child closure never closes ancestors or siblings. Dropping ordinary handles never cancels work.
32. A descendant's runtime cancellation is independent of the parent turn. A background child may outlive the turn that spawned it.
33. Interactive child cancellation is reusable. `task_despawn`, Session close, subtree close, and outer-session shutdown are permanent.

## Manager graph and factories

34. A managed TUI outer session owns exactly one manager and exactly one root. Every managed descendant, including grandchildren, is created through that manager.
35. The manager owns topology, runner tasks, wait watchers, limits, and global turn permits. It must not duplicate actor queue, history, lifecycle scheduling, tickets, or outcome storage.
36. Root depth is zero. Parent, root, and depth relationships are immutable after reservation.
37. Limit checks and graph reservation are atomic. Reserved and closing nodes consume capacity; settled closed/removed nodes do not.
38. Actor/backend factories run outside the graph lock. Returned errors, panics, concurrent closure, and failed commit all roll reservations back exactly once.
39. Never hold the graph lock across an await, actor call, backend call, factory invocation, task join, or any path that can re-enter tool dispatch.
40. Graph snapshots may include closed tombstones for the manager lifetime. They are in-memory observations, not persisted identity. `GraphLifecycle::Removed` currently has no observed transition and must not be presented as active behavior without implementing it.
41. Managed TUI root and descendant construction must not fall back to direct `AgentActorHandle::spawn`. Direct construction is currently allowed only for actor/manager tests and explicitly deferred unmanaged frontends.

## Managed concurrency and descendant waits

42. `max_concurrent_agent_turns` limits executing managed turns across one graph. It does not count live agents or merely queued turns.
43. Per-agent serialization stays in the actor. The manager limiter must not become a second scheduler.
44. Managed permit acquisition happens after actor active cancellation is installed and before backend execution. Cancellation while waiting for capacity must settle without entering the backend.
45. While unsuspended, every opaque backend poll holds physical permit ownership for the entire poll. Capacity must not be released from underneath cleanup or a concurrent suspension race.
46. A blocking parent wait may yield capacity only after validating active authority, descendant relationship, child actor identity, and ticket ownership. The current admission-to-watcher ticket race documented in `agent-system.md` is a known defect to close, not behavior to preserve.
47. Multiple descendant waits from one parent share a reference-counted suspension. The first releases capacity; only the last retirement requires reacquisition.
48. Suspension guard drop is synchronous and non-blocking. The managed execution poll gate is the only authority that reacquires the semaphore.
49. A completed child waiter must not resume Lua/model/tool continuation until the parent lease reports ownership restored.
50. Dropping a wait cancels its watcher, not the child. Timeout closes the child subtree according to current Session semantics.
51. Parent cancellation signals all registered watchers but does not bypass the global limit for backend cleanup.
52. Abnormal managed-execution drop closes lease registration, signals watchers, releases physical capacity, and revokes authority without creating an ownership cycle.
53. Manager shutdown owns and eventually joins or reaps actor runners and wait watchers. A timeout transfers ownership to the reaper rather than leaking tasks.

## History, events, and compatibility

54. The actor exclusively owns mutable conversation history. Optional `SharedMessages` is a mirror, not a competing owner.
55. Restored history is sanitized and published synchronously before an actor handle escapes.
56. Presentation-only failure/notification text must not be inserted into provider history as model output.
57. Structured child outcomes and history must retain the live child's `AgentId`; stale root `run_id` filtering must not discard later events from a reusable child.
58. Terminal child deduplication keys by `(AgentId, TurnId)`, not a task id alone. Reusing a child must allow later distinct turns through.
59. Root `run_id` remains presentation and stale-event correlation. It must not become runtime identity, topology, or authorization.
60. Managed sessions use only the manager's per-outer-session concurrency limit. The deprecated process-wide task semaphore applies only to unmanaged compatibility sessions; never enforce both.
61. Session/task APIs may adapt actor state, result text, capture values, and cumulative usage, but they must not become a second lifecycle source.

## Current scope boundary

62. Do not assume that model, prompt, tools, permissions, modes, or request options are manager-owned per-agent state yet.
63. Do not assume agent graph topology, ids, queued work, outcomes, or notifications survive process restart.
64. Do not expose proposal-only first-class Lua agent, graph navigation, preset, subscription, or public wait APIs as implemented behavior.
65. Do not weaken these current invariants to imitate a future design. Land the future ownership model first, migrate all affected callers, and update these docs with the new verified contract.

## Verification expectations

Changes in this area should use deterministic channels, barriers, and fake providers. Avoid sleeps and polling in lifecycle/race tests. At minimum, test the layer whose invariant changed:

- `maki-agent::agent::run` for terminal outcomes and provider/tool-loop behavior
- `maki-agent::actor::tests` for FIFO, exact waits, cancellation cuts, closure, and retention
- `maki-agent::manager::tests` for authority, limits, factory rollback, graph cuts, permits, waits, and reaping
- `maki-lua` tests for managed invocation scope and Session/task compatibility
- `maki-ui` tests for root queue behavior, replacement, child routing, and presentation correlation

Run crate-scoped checks first, then the repository lint, test, formatting, and generated-document checks required by the project.
