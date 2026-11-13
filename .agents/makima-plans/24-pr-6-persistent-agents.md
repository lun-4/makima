# Agent Revamp PR 6: Persistence

## Goal

Make a makima session fully durable: the agent graph, per-agent configuration, transcripts, turn outcomes, pending child deliveries, plugin state and recorded context all live in one append-only session state log (JSONL v3), so that resume restores the latest state and rewind restores any earlier restoration point, including across clear-context plan approval and with optional file restore.

Agents become durable logical records (`AgentId` survives restart, residency is a property), which closes #41, #148 and #177, unblocks #14, #166 and #213, and removes the "restored child tabs are display-only" limitation.

## Implementation Summary

Tracking source: issue #24 tracking comment (PR 6 row) and the "Durable agent identity" comment; #41 persistence proposal; #148; #177 design direction; #166 design findings; #14; #213; #173 (boundary only). Local mirrors: `.agents/makima-issues/` (stale, prefer GitHub), prior stage plans in `.agents/makima-plans/24-pr-*.md`, contracts in `docs/agent-system.md` and `docs/agent-invariants.md`.

Operator decisions already taken:

- One plan covers the whole PR 6 row, delivered as ordered phases on one branch. Each phase must pass the relevant checks before the next phase starts. Do not commit or push without an explicit request. The phases may be split into stacked PRs without changing their contracts.
- `AgentId` itself becomes durable. No `ActorId` rename. Incarnations stay fenced by `Node.reservation` and the actor `Arc` identity (`makima-agent/src/manager/mod.rs:998-1019`, `actor/mod.rs:1819`).
- Format bumps to `LOG_FORMAT_VERSION = 3`. The loader reads v2 and v3 and always writes v3. Old binaries refuse v3 files and leave them untouched (existing behaviour, `makima-storage/src/sessions.rs:993-998`). No downgrade path. The header carries a `features` set for later additions (see Compatibility within v3).
- Work that was queued or running when the process died is recorded as cancelled with reason `Interrupted` and is never re-run. Pending child deliveries are kept and consumed exactly once.
- v2 child transcripts load display-only: each gets a generated durable id on first v3 write, lifecycle `Closed`, parent = root with `ancestry_unknown = true`. They cannot receive follow-ups.
- #14, #166 and #213 are unblocked and not implemented here. Each is left with a small, defined remainder.
- Plan state lives in a state slot, not in a host record type, so PR 8A changes its owner and deletes no storage code.

Corrections to the issue texts that this plan relies on (verified in code):

- #41/#148 say older binaries skip a new record type. They do not: any undecodable record is `UnreadableRecord` (`sessions.rs:966-974`). Hence the v3 bump.
- #148 says clear-context approval starts a new session. Today it replaces history in place (`makima-ui/src/event_loop/approval.rs:270-275`). Phase 9 changes approval to mint a new session with a parent link, as #148 specifies.
- `policy_generation` restarts at 0 per actor (`actor/mod.rs:932`), so records are keyed by a durable per-session `seq`, with generation kept as information.
- `TurnId` and actor `coverage` are in-memory, so restoration points are anchored by a durable boundary index, not by turn id alone.

Crates touched: `makima-storage` (format, session model, blob and branch stores), `makima-agent` (journal, manager residency, actor persistence hooks, history edit, checkpoint), `makima-lua` (agent API, `makima.state`, strict converter), `makima-ui` (storage writer, restore, rewind UI, link resolver, approval), `makima-acp`, `src/` (SDK, print, tui cmd), `makima-providers` (`ObservationSource` unchanged in shape), `plugins/task`, `plugins/write`/`edit` (no change expected; pre-images are captured in Rust), `makima-docgen` output, `docs/`, `site/docs/`.

Non-goals: SQLite (#43); byte-bounded result memory policy (#173, only the stored-result cap below); event-callback admission (#171); guest modes/presets (PR 7); UI graph view (PR 8B); tracking shell side effects for file restore; persisting background tool calls (#315), which are treated as interrupted work; original input (#14); tool-row agent links (#213); the todo plugin, including the agent focus-change event and the origin-session ownership of queued windows that it needs (#166); release of idle actors within a process; contextual automode, whose state fits agent-scoped or session-scoped slots.

## Implementation Plan

### Design contract (applies to all phases)

**Records (v3).** `LogRecord` (`sessions.rs:410-443`) gains three tags. Tag names must not start with `msg` (the scan counts messages by prefix, `sessions.rs:1238-1290`).

```rust
Header { v: 3, id, model, cwd, created_at, parent: Option<ParentLink>, features: BTreeSet<String> }
struct ParentLink { session: MakimaId, message_count: usize, seq: u64 }

State  { d: StoredStateRecord }   // "state", append-only, ordered by seq
Agent  { seq: u64, d: StoredAgentRecord } // "agent", ordered revisions; latest map is derived
Branch { d: StoredBranch }        // "branch", {id, created_at, at_seq, agent, offset}

struct StoredStateRecord { seq: u64, agent: Option<AgentId>, generation: u64, change: StateChange }
enum StateChange {                       // #[serde(tag = "kind")]
  Boundary { offset: usize, lens: BTreeMap<AgentId, (u64, usize)> }, // full transcript epoch/length map
  Admitted { turn: TurnId, origin },     // every accepted turn, including queued work
  Config   { cwd, model, thinking, fast, workflow, mode: StoredMode, options: BTreeMap<String,String> },
  Rules    { rules: Vec<StoredRule>, yolo: Option<bool> }, // None = not set explicitly
  Context  { source: String, hash: String, content: String },
  Plugin   { key: String, version: u32, value: Option<serde_json::Value> }, // None = deleted
  Turn     { turn: TurnId, result: StoredTurnResult },
  Delivery { id: (AgentId, TurnId), op: Queued(StoredDelivery) | Consumed | Withdrawn | Dropped },
  File     { path, pre_blob: Option<String>, post_hash: String },
}
struct StoredAgentRecord {
  id: AgentId, parent: Option<AgentId>, depth: u32, transcript_key: String,
  lifecycle: Open | Closed | Retired, ancestry_unknown: bool, label, spawned_by_tool_use_id,
  spec: Option<StoredAgentSpec>,       // plain-data spawn options; None = not reactivatable
  restorable: Result<(), String>,      // e.g. "agent uses plugin-local tools"
  usage: TokenUsage, created_seq: u64, updated_seq: u64,
}
```

`agent: None` on a state record means session scope. `SessionMeta` (`sessions.rs:151-183`) keeps only UI state (draft, root text queue, context size, title, usage); its config, plan and rule fields are read on v2 load only and are converted into seed `Config` and `Rules` records and a plan slot value (Phase 6).

Single ordering. New `makima-agent/src/journal.rs`: `SessionJournal`, owned by the `AgentManager`, allocates `seq` (seeded on load from max + 1), holds an opaque `state_epoch` (re-minted on load, rewind, reset, compaction) and buffers records. Every state mutation goes actor/manager commit → journal → writer. The journal sits above a `GraphStore` persistence adapter. `MemoryGraphStore` is the test adapter; the production adapter uses the existing storage checkpoint writers. `AgentManager::restore_graph(SessionStateView)` rebuilds logical nodes from that adapter. Neither writer drains records destructively: checkpoints capture an immutable journal prefix and its matching transcript snapshots, tagged with revision and epoch. Successful publication advances a persisted high-water mark; failure retains the prefix for retry. Append transcript changes and state changes through the same commit gate, so a checkpoint never pairs sampled history with a different journal revision. UI and storage are projections; they never author config.

Restoration point. Immediately before appending a rewindable user message (`is_boundary_message`, `actor/history_edit.rs:82-107`), capture a boundary with its prefix offset and the full transcript epoch/length map of all open agents, including empty transcripts. Fold records through that boundary to restore the state before that message, which remains the rewind draft. Mid-turn changes have later sequence numbers. Persist admission separately for every turn origin; boundaries are not admission records. Recovery sanitation must preserve boundary identities and remap offsets when it removes or inserts messages; test unmatched-tool recovery explicitly. Agent lifecycle updates also carry `updated_seq` and remain in the ordered history, so historical graph reconstruction does not depend on the latest agent map alone.

**Durability.** Write-behind (#41): a change is effective at commit and saved afterwards, with retry and a visible "applied but not saved" warning. Paths that are already publish-then-commit stay so (`PreparedOperationTicket::commit_durable`, `commit_prepared_transaction_held`). SDK/ACP keep reporting a terminal result only after the checkpoint settles (invariant 66). Every other consumer observes a terminal result at commit. A result lost before publication reads as `Interrupted` if its admission was persisted; an entirely unpublished turn has no recoverable record. Its unpublished delivery is lost with it. Recovery only guarantees state through the last published checkpoint. Within that durable prefix, `Consumed`/`Withdrawn`/`Dropped` delivery records are authoritative. Consumption and the observation append share one checkpoint transaction. A history marker is a compatibility fallback, not the primary deduplication record; compaction retains terminal delivery identities. Exactly-once refers to observation insertion, not to provider execution after an uncertain request.

**Bounds.** Stored turn results keep the existing count limit of 128 per agent; `text`/`output` above 64 KiB are written through `tools::offload` and referenced. Delivery bounds stay 64 entries / 256 KiB (`actor/delivery.rs:14-18`). Plugin values: depth 64, 256 KiB encoded, rejected without mutation. All limits are named consts. Offloaded results live in the session's offload directory (`sessions/offload/<id>/`) and are removed with the session, not earlier, so branches and held tickets can resolve references while the owning session exists. Session deletion invalidates those references; subsequent reads fail with `result_unavailable` or `branch_expired`. No cross-session blob retention system is added. Bounding that directory is #173.

**Locks.** Journal appends take only the journal mutex; never hold the graph lock across an await (invariant 40) and never take actor/manager locks inside a publication claim (`docs/agent-system.md:85`).

**Compatibility within v3.** The header carries `features`, a set of feature names. A binary refuses to load a file that names a feature it does not implement, through the same error path as a version mismatch, and leaves the file untouched. A later change defines a feature name when a binary without the change would misread the file or drop data on a full rewrite; that covers new record tags and new fields that carry state. A writer adds the name to a session when it first writes such data. The first v3 release defines no features. There is no unknown-field preservation and no skippable record tag: a binary either implements everything in the file or refuses it. #14's `source_text` on message records and the PR 7 and PR 8A additions use this rule and need no version bump.

**Stored mode.** `StoredMode` is a snapshot of the resolved `ModeDef` (`makima-agent/src/modes.rs:49-62`): `id`, `label`, `system_prompt`, `restrict_write_to` and `tools`, with an absent tool list kept distinct from an empty one. `Config` stores the snapshot. A resumed or reactivated agent therefore runs under the prompt text, toolset and write restriction it had, and a mode whose definition has changed or disappeared is not looked up again by name. PR 7 replaces the snapshot contents with its guest bundle under a feature name.

**Stored agent spec.** `StoredAgentSpec` holds the spawn options that are plain data (`SPAWN_OPTIONS`, `makima-lua/src/api/agent.rs:78-91`): `model` selection, resolved `system` text, `tools` as the definitions sent to the provider, `audience`, `mcp`, `output_schema`, `name`, `silent`, `auto_deliver` and `inherit_provider`, plus the mode ceiling (`ModeCeiling`) and the owning plugin. `show` is a creation effect and is not stored. `local_tools` are closures and are not stored. The spec seeds creation only; at reactivation the latest folded `Config` supplies model, mode, cwd and options, and the ceiling is revalidated.

**Reactivation.** Capabilities are rebound by name, never deserialized. Provider and model go through current discovery. Each stored tool definition is bound to the currently registered tool of the same name; a missing tool fails with `tool_unavailable`, and a missing MCP server with `mcp_unavailable`. The stored definitions remain the ones sent to the provider, so the transcript and the tool list stay consistent. No definition fingerprint is stored. Reactivation uses the currently registered handler by name while preserving saved provider-facing definitions. Schema/behavior changes are an accepted compatibility limitation; handler-side validation is not guaranteed.

**State audit (#41).** Each kind of session state is logged, rebuilt, or deliberately not stored:

| State | Treatment |
|---|---|
| Agent graph: identity, parent, depth, lifecycle, label, spawn spec | Logged as `Agent` revisions. Reservations and actor handles are rebuilt. |
| Transcripts, tool outputs, posts | Logged in the existing `msg`, `sub_msg`, `out` and `post` records. |
| Turn outcomes and pending deliveries | Logged as `Turn` and `Delivery`. Live tickets are rebuilt from stored results. |
| Admitted turns, queued or running | Logged as `Admitted` and recovered as `Interrupted`. The pinned policy is discarded with the turn. Root queue text stays in `SessionMeta` as inactive drafts. |
| Model, thinking, fast, workflow, mode | Logged in `Config` per agent. |
| cwd | Logged in `Config`. The header keeps the latest root cwd for listing only. Rewind restores the cwd of the boundary and calls `PermissionManager::set_cwd`. |
| Plugin options | All current values are logged in `Config.options`. Rewind restores all of them. Resume applies the options declared `persistent`, as today; the others take their initial value. |
| Session permission rules, including allow-for-session grants, and explicit yolo | Logged in `Rules`. Config, project and plugin rules are read again, not stored. Allow-once answers and pending prompts are dropped with the interrupted turn. |
| Plan path, status, approved content, staged implementation model | The plan slot (Phase 6). |
| Instructions and prompt slots | Logged in `Context` (Phase 10). |
| Plugin state | Registered slots only. `makima.store`, VM tables, timers, autocmds and callback registrations are not stored. |
| Draft, root text queue, title, context size, usage | `SessionMeta`, as today. |
| Provider sessions, MCP transports, background tool calls (#315), session locks | Not stored. Rebound at activation or reported as interrupted. |
| UI state: focus, panel visibility, scroll, expanded rows | Not stored, apart from what tool `state` records today. |

The table is the starting point of the audit, not its result. Phase 5 checks every field of `SessionMeta`, `EffectiveAgentConfig`, the `App` session state and `PermissionManager` against it, and adds a row for any field it does not cover before the phase closes.

### Phase 0: test infrastructure

There is no restart harness today. Build it first; later phases depend on it.

- `makima-agent`: `GraphStore` trait (`put_agent`, `append_state`, `load`) with `MemoryGraphStore` under the `test-util` feature, and `restart_manager(store) -> AgentManager` in `manager/tests.rs`, reusing `Gate`/`TestBackend` (`manager/tests.rs:1636-1671`).
- `makima-lua/tests/common/mod.rs`: `restart_host(state_dir)` that drops the VM and manager and rebuilds from disk on `InMemoryFs` (`makima-lua/src/lib.rs:392`).
- `makima-ui`: `reopen_app(session_id)` helper beside `with_event_loop` (`event_loop.rs:6149`).
- Prepare reusable gates from discarded PR #178 at `0bd60a46` (reference only, old `maki-*` paths). Port its state API regression cases in Phase 6, not before that API exists. Add disk-backed tests using temporary real directories as well as in-memory tests. Restart helpers must disable graceful flush and discard unpersisted buffers when simulating a crash.
- Harness self-tests: `restart_manager_round_trips_an_empty_graph`, `restart_host_reloads_plugins_and_session`. Map both harness self-tests to AC.3; later crash tests must exercise disk-backed publication, not only the memory adapter.

### Phase 1: storage v3

- `sessions.rs`: add the record types, `Session.state: Vec<StoredStateRecord>`, `Session.agent_revisions: Vec<(u64, StoredAgentRecord)>`, derived `Session.agents` latest-value index, `Session.branches`, header `parent` and `features`. Follow the posts pattern exactly: `push_state` calls `touch`, `write_full_session` re-emits (`:875-924`), `append` uses new `saved_state_count` and `saved_agent_revision_count` cursors; every rewrite retains ordered agent revisions alongside state records, `cursor_from`/`cursor_ahead` cover them (`:700-835`).
- Loader accepts `v ∈ {2, 3}`; `scan_jsonl_header` accepts both; bump `SCAN_CACHE_VERSION`; `ScannedHeader` gains `parent`. Session count semantics (#98) unchanged. A header that names an unknown feature refuses the load like a version mismatch.
- `Session::state_at(seq)` and `Session::latest_state()` fold helpers returning a typed `SessionStateView` (per-agent config, rules, context, plugin map, results, pending deliveries).
- Compaction is per agent. Seed only that agent's effective state in its new transcript epoch; preserve session-scoped values, other agents' records, and terminal delivery identities. Keep a compaction barrier with its sequence number. Root rewind refuses boundaries that would cross any affected agent's compaction; child rewind refuses compaction within its subtree. Other agents' boundaries remain indexed, even when a root boundary is now unavailable.
- v2 conversion: seed records from `SessionMeta`; each `StoredSubagent` (`:329-345`) becomes a `StoredAgentRecord` per the legacy decision; `ambiguous` entries get distinct generated ids. Legacy records carry a flag and do not count toward `max_retained_agents`.

### Phase 2: durable identity

- `makima-agent/src/types.rs:493-526`: redocument `AgentId` as the durable agent identity. Add `AgentManager::create_root_with_id` and thread a supplied id through `create_root_inner` (`manager/mod.rs:490`).
- Persist the root record; TUI (`makima-ui/src/agent/mod.rs:556`), SDK (`src/sdk_mode.rs:1288`), ACP (`makima-acp/src/server.rs:1212`) and print pass the stored root id on resume. SDK `--fork-session` mints a new root id and sets header `parent`.
- Child spawn (`manager/mod.rs:667`) writes a `StoredAgentRecord` at commit, close and metadata change. `transcript_key` keeps the existing `tool_use_id`/`session-<id>` string for `sub_msg` addressing; it is no longer identity.
- Update invariant 2 to "one durable agent has at most one resident actor".

### Phase 3: outcomes and deliveries without resident actors

- Derive serde on `TurnResult`, `TurnProvenance`, `TurnOrigin` (`actor/tickets.rs:16-40`); add `TurnCancellationReason::Interrupted`.
- `retire_and_publish` (`actor/mod.rs:1134-1190`) journals `Turn` after seal, before resolve. Invariant 15 (Idle before resolve) is preserved. The `Turn` record and the transcript and delivery changes of that turn belong to one checkpoint. Tickets, `TurnOutcome` events and parent deliveries resolve at commit and do not wait for publication, so a failing disk cannot keep an accepted turn from terminating. SDK and ACP keep their existing wait on the checkpoint (invariant 66), with their current behaviour when it fails. Print uses the existing explicit nonpersistent adapter.
- Replace `ParentDelivery.parent: Weak<ActorInner>` (`actor/delivery.rs:24-59`) with routing by id through the manager: `manager.deliver(parent, ChildDelivery)`. Journal `Delivery::Queued` whenever acceptance succeeds, for resident and non-resident parents, under the same bounds. Resident parents then use `admit_delivery`; non-resident parents retain a logical queue. `commit`, `withdraw_child_delivery`, discard journal `Consumed`/`Withdrawn`/`Dropped`. Existing semantics hold: a result consumed via `AgentTurn:wait` is withdrawn; closed-child turns are not delivered to the closer; a failed root turn keeps queued deliveries.
- Shutdown no longer discards pending deliveries; it leaves them queued in the journal.
- Journal `Admitted { turn, origin }` at admission for every turn origin: user, plugin, child-result continuation and queued sends. Joined input joins an existing turn and writes no record. No input or policy snapshot is stored, because interrupted work is never re-run and nothing would read it. Phase 4 enables recovery: each durable `Admitted` without a terminal result becomes `Cancelled { Interrupted }`. Publish recovery sanitation and the synthesized outcomes as one idempotent recovery checkpoint before exposing results or activating actors. A second restart observes those terminal records and does not synthesize them again. Do not flush restored root queues automatically: retain their text as inactive drafts that require explicit resubmission. This applies the never-rerun decision consistently to root and child work.

### Phase 4: residency, limits and the Lua agent API (#177, durable-identity comment)

- `Node` (`manager/mod.rs:107-121`) gains `residency: Resident | Dormant`; `GraphLifecycle::Dormant` does not consume capacity. Nodes loaded from disk start `Dormant`; the root is reactivated immediately. A node also becomes `Dormant` when its owning VM is torn down by a plugin reload or by process shutdown. Teardown journals no lifecycle change; only an explicit close or despawn journals `Closed`. `LuaSession::drop` closes subtrees today and has to distinguish the two (`makima-lua/src/api/agent/handles/mod.rs:81-133`).
- `ensure_resident(id)` runs the reserve → factory → commit path of `spawn_child_inner` with the stored id, a new reservation, no second push into `children`, and mode-ceiling validation. A host `Reactivator` rebuilds live parts from `StoredAgentSpec` (provider, model via discovery, fresh MCP session, event senders, permissions, host-generated `structured_output` tool). Rebinding follows the reactivation contract above.
- No release of idle actors within a process. An idle resident actor stays resident until it is closed, retired or its VM is torn down, as today. #177 states that hibernation is not yet a requirement and asks for measurement first. Measure retained bytes per idle agent (history, sealed results, Lua session, MCP session, transcript relay copies) at 33 and 128 sequential tasks and with large outputs, and record the numbers in the PR. Release, with its eligibility rule and a resident limit, is a follow-up if the numbers require it. One consequence: an agent with local tools is never unloaded while its owner is loaded, so follow-ups to it keep working within the process.
- Limits (`manager/types.rs:14-20`, checks at `mod.rs:698-715`): `max_children_per_agent` (default 16) counts children with outstanding work (queued, executing, pending delivery) instead of retained children. `max_live_agents` (default 64) becomes `max_active_agents` and counts agents with outstanding work in the session; the old key stays as an alias. `max_agent_depth` is unchanged. New `max_retained_agents` (default 1024) is an identity quota. The retention limit rejects new identities with `retention_limit` until explicit retirement; it never invalidates an open follow-up target merely because unrelated tasks were created. `Agent:retire(ctx)` and an authorized task-retire command explicitly retire a quiescent leaf. Persist `lifecycle: Retired` as an ordered agent revision before acknowledging retirement; replay and historical folds distinguish it from quota-consuming `Closed`. Retirement closes admission but preserves readable history and tombstones. The quota counts non-retired logical identities, including closed records; retired records do not consume identity-admission quota. This is an admission quota, not a disk-size bound. No implicit history deletion or bounded-disk claim is made. Follow-up sends and automatic continuations reserve outstanding-work capacity atomically, not only spawn. Joined input does not double-count a turn. Suspended parents release execution permits but keep one work reservation; reserved child-result completion/settlement paths must drain without obtaining another ordinary work slot. On saturation reject new ordinary admissions explicitly, preserve pending reports, and avoid blocking the dispatcher. Regenerate docs.
- Lua (`makima-lua/src/api/agent/`): `makima.agent.get(ctx, AgentRef | string)` resolves resident and dormant agents with the existing topology authorization (invariant 3); `Agent:is_live()` and `status.live`; `Agent:turn(ctx, turn_id)` returns a resolved handle from stored results; `send` on a dormant agent calls `ensure_resident` then admits. Errors are typed: `not_restorable` (local Lua tools, with the reason from the record), `provider_unavailable`, `model_unavailable`, `tool_unavailable`, `mcp_unavailable`, `retired`. A failed reactivation leaves the record unchanged. Old incarnation callbacks, permissions, owner cleanup and checkpoints must match both durable id and current reservation/epoch; a stale actor cannot close or mutate its replacement.
- `build_session` (`api/agent.rs:1668-1836`) captures `StoredAgentSpec`. An agent whose final local-tool map contains Lua closures, supplied by the caller or inherited from the parent (`inherited_local_tools`, `api/agent.rs:1390`), is stored with `restorable = Err(..)`. The host-generated `structured_output` tool is rebuilt from `output_schema` and does not count. Such an agent works as today while its owner is loaded. After a restart or an owner reload it is dormant and readable, and `send` fails with `not_restorable`. A plugin that registers again cannot attach new closures to it.
- Record committed root/child config changes in this phase, including post-spawn model, mode, options and cwd. Reactivation uses the latest Config fold. Keep existing metadata projections until Phase 5 cleanup.
- Restart activation and recovery are enabled in this phase after dormant-node reconstruction exists. Phase 3 tests serialization and delivery routing with fake records; Phase 4 adds actual drop-and-rebuild tests. Parent links for rendering use `StoredAgentRecord.parent`.

### Phase 5: state log as the single source of configuration

- Actor config commits (`ConfigCommit` observers, `prepared.rs:500-505`) journal `Config`; child config uses the same path. Delete `project_selected_config`, `preserve_child_config`, the config half of `apply_session_checkpoint` (`session_checkpoint.rs:470-522`), `same_coordinator_metadata` and the config copy in `merge_tui_snapshot` (`storage_writer.rs:1256-1287`); `build_meta` (`app/session.rs:149-173`) stops writing config. Keep #149's history-identity merge. `Config` includes cwd, and `/cd` journals a `Config` record.
- Permission rules and yolo: `PermissionManager` (`makima-agent/src/permissions.rs:259-336`) reports rule changes to the journal as session-scoped `Rules`.
- Plan metadata stays in `SessionMeta` during this phase. Phase 6 moves it to the plan slot and removes the fields.
- Resume in all frontends builds initial config from `latest_state()`, through the one mode setter; a plan session whose file cannot be made still resumes in build. Plugin options follow the audit table: persistent options come from the record, the others take their initial value.

### Phase 6: plugin state API (#41, #166 generic part)

New `makima-lua/src/api/state.rs` (`makima.store` keeps its meaning and is not reused):

```lua
local slot = makima.state.register("example", { scope = "agent" | "session", version = 1, migrate = fn })
local value, rev = slot:get(ctx)
local rev = slot:set(ctx, value, { expect = rev })     -- expect optional
local rev = slot:delete(ctx, { expect = rev })
local value, rev = slot:update(ctx, function(cur) return next end)  -- non-yielding read-modify-write
local value, rev = agent:state(ctx, "<plugin>/<name>") -- retained read, no actor needed
makima.state.null                                      -- JSON null sentinel
slot:on_change(function(ctx, event) end)               -- event = { agent, revision, reason }
```

- Key is `<plugin>/<name>`. Slots of unloaded plugins are carried forward opaque. A stored `version` lower than the registered one calls the non-yielding `migrate` on first read after load, and the result is journaled as a new `Plugin` record at the current sequence. A rewind that installs an older-version value migrates it the same way before it notifies. A failed migration leaves the stored value and fails the read. A higher stored version fails the read with a specific error and leaves the record.
- Authority from the invocation (`makima-lua/src/api/util/ctx.rs:97-288`): `Handler` writes `execution_agent_id` validated against the live managed turn; `Trusted` writes its pinned target; session scope writes through the root coordinator; `Restore` and `Start` are read-only. Never UI focus, never root fallback.
- Commit is serialized by the journal commit gate for resident and dormant records; it does not require an actor. The actor uses the same gate for history/state publication. `update` executes a non-yielding callback against a copied value, without holding host locks, then performs a revision-checked commit; a racing commit yields `state_conflict` and does not rerun the callback implicitly. At the logical commit linearization point validate plugin-generation authority, invocation/task liveness, cancellation, target authorization, state epoch and expected revision. Reuse the invocation claim gates rather than deferring them to disk publication. Acquire plugin-generation gate, invocation/task liveness gate, then journal commit gate; no await or plugin callback occurs under them. Existing writer claims keep their writer-before-plugin order and never acquire the journal gate. Cancel/end/revoke either wins before commit or observes the completed commit. A handle from an older `state_epoch` fails with `stale_state`. Nested successful commits survive a later outer failure.
- Strict converter in `makima-lua/src/api/util/convert.rs` (do not reuse lenient `lua_to_json`, `:88-198`): rejects cycles, NaN/Inf, functions/userdata, sparse arrays, non-string object keys; keeps tagged empty arrays; copies values; verifies a round trip through the storage decoder inside a full record.
- A state-only change bumps `content_revision` so checkpoints are not throttled or coalesced away.
- `agent:state` and `slot:get` for a dormant agent read from the folded session state. A trusted callback can commit to its pinned dormant record without reactivation, subject to the same authority and epoch checks. Reset starts a new session/root state scope with absent agent and session slots; historical values remain in the previous session. Compaction preserves current values. Port the #178 nested dispatch, cancellation, concurrent edit and state-only checkpoint tests here. Callback registrations do not survive a restart or a reload: a plugin registers again and resolves its agents by durable id under a new invocation.
- Host handle. `StateSlot<T>` in `makima-agent` gives host code the same registration, revision check, commit gate and `Plugin` records as the Lua API. It exists for host-owned slots and is the only other entry point.
- Plan slot. One session-scoped slot holds the plan path, the written flag, the approved content with its hash, and the staged implementation model. Its key is the one the PR 8A plan plugin will register (`plan/state` proposed) and is reserved for the host until then. The native plan code (`components/plan_form.rs:92`, `event_loop/approval.rs`) writes it at path allocation, plan write, staged-model change and approval, and the plan fields leave `SessionMeta`. PR 8A changes the owner and migrates no data.
- Notification. `on_change` receives `reason = "commit" | "resume" | "rewind"`. Resume and rewind install the folded values first and notify afterwards, once per slot that has a value, including when the value is unchanged. A notification cannot veto the transition. The slot is authoritative and a file derived from it is a projection: on `resume` or `rewind` the plan owner rewrites the plan file from the approved content, and a failed write posts a warning and leaves the slot as installed. On both resume and rewind, failed plan-file restoration blocks further turn admission until the existing mode setter applies the visible Build fallback. Never execute Plan mode with a stale or missing file. Keep the installed slot for repair.
- Tests use a fixture plugin registered in the test host. The todo plugin is not a consumer in this PR.

### Phase 7: task plugin and durable links

- `plugins/task/init.lua`: `task_id` becomes the durable agent id; task policy (`description`, `writes`, owner) moves to a session-scoped slot keyed by agent id; `resolve_task` uses `makima.agent.get`; `task_wait` uses `Agent:turn`; the plugin's own 128-task eviction is removed, and retention is the manager's `max_retained_agents` with explicit retirement. Old transcript-key task ids from v2 sessions resolve to `unknown task_id` as today.
- Links: `Post.agent` and `result_agent` store the durable id, and restored chats set `agent_id`. One UI map `AgentId -> chat` and one resolver in `app/mouse.rs`; delete `historical_chat_index`, `result_historical` and the `ambiguous` handling. A missing agent flashes `FLASH_NO_AGENT_CHAT` on both surfaces. Tool-row links are #213, which then needs only the `agent` field on tool replies and the header hit test.

### Phase 8: full-state rewind (#148)

- `ConfigChange::Restore(StoredAgentConfig)` (`actor/config.rs:80`); `HistoryEditAction` and `PreparedTransactionSnapshot` (`prepared.rs:37`) gain `state: Option<AgentStateSnapshot>`, installed in `HistoryEditWork::execute` under the same lock as `replace_snapshot` (`history_edit.rs:233-261`), re-minting `state_epoch`.
- `dispatch_rewind` (`event_loop.rs:4097-4152`) becomes a heavy transition: pause queue → `cancel_subtree` → `prepare_idle_subtree` → one `commit_prepared_transaction_held` carrying history, `Restore` config and state; session-scoped parts (rules, session plugin slots, graph) apply in the coordinator `finalize`. Queued messages are dropped. Never a sequence of FIFO setters.
- Scope: rewinding the root restores the whole session as of the boundary: agents created after it are closed and removed from the active log, surviving agents return to their record and transcript length (`lens`), pending deliveries and outcomes fold back. Rewinding a child restores that agent, its agent-scoped state and its descendants only.
- Compaction checks use the agent-scoped barriers from Phase 1. Root rewind refuses if any agent in the boundary map compacted later. Child compaction keeps unrelated root boundaries. Child rewind preserves all later session and outside-subtree records; rebuild the active log from the target subtree prefix plus unchanged outside records. Already-consumed reports in outside parents remain consumed. Drop unconsumed reports originating in removed subtree turns; restore other queued reports as held until the next explicit user turn, never as an automatic continuation immediately after rewind. Sequence numbers remain monotonic after replacement. Revision fences cover the entire replacement even when content is identical.
- Branches: before the rewrite, `link_archive` (`makima-storage/src/sessions.rs:580`, a hard link with a copy fallback) preserves the current log at `sessions/branches/<session>/<branch>.jsonl`, and a `Branch` record is appended to the new log. The writer holds its publication barrier from the link to the rename, so no append reaches the preserved file. Branch retention follows the archive pattern with its own consts (count and total bytes, oldest pruned first). A `Branch` record whose file was pruned stays listed as expired; redo or restore of it fails with `branch_expired` and changes nothing. Redo and "restore branch" load the file through the same heavy transition, which itself saves the replaced state as a branch.
- The rewind picker gains a branch list. File choices are added in Phase 12 when they work end-to-end.

### Phase 9: parent link across plan approval

- Clear-context approval (`approval.rs:409-531`) mints a new session via `Action::ReplaceSession` with a new `SessionReplacementKind::Linked`, header `parent = { session, message_count, seq }`, and carries rules/options like reset. The kind names no mode and no plan. The planning session keeps its plan slot value with the approved content and staged model.
- The rewind picker in a session with `parent` lists parent turns. Choosing one claims the parent lock, loads the parent, and rewinds it to the chosen boundary (default: the approval point). If the parent is open elsewhere (`session_lock::open_elsewhere`) or its cwd differs, fail with an error and change nothing. The implementation session stays on disk. The picker offers the parent boundaries that are still in the parent's active log at or before `parent.seq`. Sequence numbers are stable across rewrites, so the link needs no snapshot of the parent. If the parent was deleted or rewound past all of those boundaries, the picker offers none and says why. The transition saves the parent's replaced state as a branch in the parent, like any rewind. Redo in the reopened planning session restores that branch and does not switch sessions: the implementation session is unchanged and is resumed from the session picker.
- Plan mode returns through the restored `Config`. The plan content and staged model return through the plan slot, and the plan owner rewrites the plan file on the `rewind` notification, regardless of the file-restore choice.

### Phase 10: recorded context injections (#41)

- At `prepare_prompt_inputs` (`makima-agent/src/agent/admission.rs:161`), hash instructions and resolved prompt slots per source. First sight journals `Context`. Later admissions use the recorded content. A differing on-disk hash posts one "instructions changed on disk, run /reload-context" notice per change and keeps the recorded text.
- New `/reload-context` command (through the central command registry) journals new `Context` records via `reload_instructions` (`makima-ui/src/agent/agent_loop.rs:428`).
- Resume and rewind build the root prompt from recorded content. `makima.agent.system_prompt(ctx, { instructions = true })` reads the recorded content as well, so a child spawned after an unreloaded change matches its parent. A child's system prompt is resolved at spawn and stored in `StoredAgentSpec`; the task plugin builds research and general prompts with instructions enabled (`plugins/task/init.lua`, `prepare`). `/reload-context` and the change notice apply to the root only, and children spawned afterwards get the current text.

### Phase 11: removed

Original input (#14) is not part of this PR. It adds `source_text` to `AgentInput` and to message records under a feature name (see Compatibility within v3). The later phase numbers are unchanged.

### Phase 12: file restore (#148)

- Persist a `RestoreTransition` manifest keyed by a transition id: source/target session revision and branch, each applied path's before/after blob or explicit Absent state, expected hashes, and skipped/conflict outcomes. Stage and sync manifest/blob references before writes. Mark per-path progress and transition completion durably; a crash resumes reconciliation only if current content matches the expected before or after hash, otherwise reports a conflict. A branch log references its transition manifest. Redo and branch restore invert only paths actually applied by the transition; skipped paths remain untouched. Files-only transitions retain the same conversation revision but still have a reversible manifest.
- `makima-storage/src/blobs.rs`: content-addressed store at `sessions/blobs/<session>/<hash>`, deleted with the session.
- `makima-agent/src/agent/tool_dispatch.rs:810-840`: after `FileWriteLocks::acquire` and before `invocation.execute`, store the pre-image; after execute, hash the result; journal `File`. Covers every `mutable_path` tool. Represent missing pre-images as Absent, so undoing file creation removes the file and redo recreates it. Capture actual post-state even when a tool reports failure after writing. Restore holds the same canonical path locks as mutable tools, rechecks hash under lock, and rejects symlink/path-escape substitutions. Hold the affected-agent transition barrier throughout; never restore against an active writer.
- Add conversation-only, conversation-and-files and files-only choices now. Cross-session rewind defaults to conversation-and-files and previews affected paths. Gather file records along the actual parent-link path from the selected planning boundary through the implementation session; do not include unrelated branches. Restore: for each file changed after the boundary, restore the earliest pre-image only if the current hash equals the last recorded `post_hash`; otherwise skip and report. Before overwriting, store current content so redo can reapply. The dialog lists affected files and the number of shell commands run after the boundary with a "not reverted" note.

### Phase 13: docs and cleanup

See Documentation Strategy. Remove dead code named above; run `just gen-docs`.

## Acceptance Criteria

- **AC.1** A v3 log containing messages, state, agent and branch records round-trips semantically unchanged through append, full rewrite, staged checkpoint and shrink archive.
- **AC.2** A v2 log loads, is rewritten as v3 with seed state and display-only child records (`Closed`, `ancestry_unknown`), and an undecodable v3 record still refuses the load leaving the file untouched. Session listing and message counts are unchanged for v2 and v3.
- **AC.3** Root and child `AgentId`s are identical before and after a process restart in TUI, SDK and ACP; `Agent:id()` returns that id.
- **AC.4** After restart, a turn result included in the last published checkpoint is readable by `Agent:turn(ctx, id)` without a resident actor; turns that were queued or running read `Cancelled { Interrupted }` and no model or tool call is re-issued.
- **AC.5** A child result included in the last published checkpoint, queued for a resident or non-resident parent at crash or shutdown, is delivered to the parent exactly once after restart, including when the crash fell between the history write and the delivery acknowledgement. Withdrawn results are not delivered.
- **AC.6** 33 sequential tasks without despawn succeed under the default 16-child limit, and 65 under the default 64 active-agent limit; earlier tasks still accept `task_send` and return earlier results. Retained bytes per idle agent are measured and recorded in the PR.
- **AC.7** `makima.agent.get(ctx, id)` succeeds for resident and dormant agents and `is_live()` distinguishes them; `send` to a dormant agent restores its transcript and config and admits the turn with exactly one resident actor under concurrent sends.
- **AC.8** Reactivation fails with `not_restorable`, `provider_unavailable`, `model_unavailable`, `tool_unavailable`, `mcp_unavailable` or `retired` as applicable and leaves the stored record byte-identical.
- **AC.9** After restart `task_send` and `task_wait` resolve a task spawned before the restart.
- **AC.10** Cwd, model, thinking, fast, workflow, the mode snapshot, session permission rules, explicit yolo and plan state (path, written, approved content, staged implementation model) of root and children are restored on resume from state records and the plan slot, with no config fields authored by the TUI snapshot. Resume restores plugin options declared persistent; rewind restores all of them.
- **AC.11** `makima.state` values survive turn end, resume and compaction; root and child values are isolated; value rules hold (missing vs null vs delete, empty arrays, rejected values cause no mutation, later Lua table mutation does not alter stored state).
- **AC.12** A write from a handle created before a rewind, reset or compaction fails with `stale_state`, including when the replacement history is content-identical; a write cancelled before commit does not mutate; a nested committed write survives outer tool failure.
- **AC.13** A state-only change with no new message reaches disk for root and child, and an older queued snapshot cannot overwrite it. Dormant writes, explicit reset clearing, revision conflicts, migrations and state notifications follow the same contract. `on_change` fires with reason `commit`, `resume` or `rewind`, and a migration result is journaled.
- **AC.14** Removed (#166). The number is retired.
- **AC.15** A post row and a completion notice for one agent open the same chat before and after restore; a missing agent shows the one unavailable message.
- **AC.16** Rewinding the root to an earlier boundary restores conversation, mode, model, options, rules, plan, plugin state and the agent graph as of that boundary in one step, after cancelling active work; mid-turn changes made after the boundary are gone.
- **AC.17** Redo after a rewind returns the session to its pre-rewind state; a rewind racing a stale checkpoint writer does not resurrect discarded history. A pruned branch reports `branch_expired` and changes nothing.
- **AC.18** Rewinding an implementation session to before clear-context approval reopens the planning session in Plan mode with the approved plan content, staged implementation model and previous model and options; it fails without changes when the parent is open elsewhere or in another cwd; the implementation session remains resumable.
- **AC.19** After `AGENTS.md` changes on disk, the next turn and a resumed session still use the recorded content and one change notice is shown; `/reload-context` switches to the new content and is itself rewindable.
- **AC.20** Removed (#14). The number is retired.
- **AC.21** Rewind with files restores files written by write/edit only where the current hash matches the recorded post-hash, reports the rest, reports the count of later shell commands, and redo restores the overwritten content.
- **AC.22** `just ci` passes on the build box and `just gen-docs-check` is clean.
- **AC.23** A v3 file that names an unknown feature is refused and left untouched; the first v3 release writes an empty feature set.
- **AC.24** Teardown of a VM or of the process journals no lifecycle change: children spawned before a restart are dormant, not closed. A child with local tools, supplied or inherited, is readable after restart and fails `send` with `not_restorable`.

## Test Strategy

All tests use gates/barriers, no sleeps. Names use `#[test_case]` where parameterised; error strings are shared consts.

| AC | Layer | Tests |
|---|---|---|
| AC.1 | storage unit (`sessions.rs`) | `v3_records_round_trip_through_append_rewrite_and_stage`, extend `random_mutation_script_round_trips_through_the_log`, `shrink_archive_keeps_state_and_agent_records`, `agent_history_survives_rewrite_and_historical_fold` |
| AC.2 | storage unit + Lua spec | `v2_log_loads_and_rewrites_as_v3_with_seed_state`, `v2_children_become_closed_records_with_unknown_ancestry`, keep `record_another_version_wrote_refuses_the_load_and_stays_on_disk`, `scan_counts_messages_in_v2_and_v3`; `plugins/sessions/tests/spec.lua` |
| AC.3 | manager + frontend parity | `root_and_child_ids_survive_restart` (manager, `restart_manager`), `frontend_parity::agent_ids_survive_resume` (Tui/Sdk/Acp cases), Lua `agent_id_is_stable_across_restart_host` |
| AC.4 | actor + Lua | `stored_result_is_readable_without_resident_actor`, `queued_and_running_turns_restart_as_interrupted`, `restart_does_not_reissue_provider_requests` (scripted provider call count), `delivery_started_and_queued_root_turns_restart_interrupted`, `sdk_and_acp_terminal_reporting_waits_for_terminal_checkpoint`, `ordinary_ticket_settles_despite_failed_checkpoint`, `recovery_terminalization_is_durable_and_idempotent` |
| AC.5 | delivery tests + manager | `delivery_to_dormant_parent_is_consumed_once_after_restart`, `crash_between_history_and_ack_does_not_duplicate_delivery`, `withdrawn_delivery_stays_withdrawn_after_restart`, `shutdown_keeps_pending_deliveries`, `resident_parent_pending_delivery_survives_crash`, `compacted_parent_keeps_delivery_deduplication` |
| AC.6 | manager + `task_policy.rs` | `sequential_children_beyond_child_and_active_limits_succeed`, `task_spawn_33_sequential_without_despawn`, `earlier_task_accepts_send_after_restart`, `retirement_releases_identity_quota_and_preserves_reads_across_restart`, `saturated_parent_child_wait_and_delivery_drain`, `legacy_children_do_not_count_toward_retention` |
| AC.7 | manager + `agent_handles.rs` | `get_resolves_dormant_agent_and_reports_liveness`, `send_to_dormant_agent_restores_and_admits`, `concurrent_sends_reactivate_one_actor` (commit gate), `stale_incarnation_cannot_mutate_reactivated_agent`, `first_cut_reactivation_uses_latest_config` |
| AC.8 | `agent_handles.rs` | `reactivation_failure_is_typed_and_keeps_record` (test_case per error) |
| AC.9 | `real_plugins_restore.rs` | `task_send_and_wait_resolve_a_task_after_restart_host` |
| AC.10 | storage_writer + checkpoint + parity | `config_is_restored_from_state_records_for_root_and_child`, `tui_snapshot_cannot_author_config`, `rules_and_plan_state_survive_resume`, extend `sdk_resume_restores_the_saved_mode`, `rewind_restores_earlier_cwd`, `resume_applies_persistent_options_only`, `stored_mode_snapshot_survives_definition_change` |
| AC.11 | Lua host (`tests/state.rs`) + convert unit | `state_survives_turn_end_resume_and_compaction`, `agent_scoped_state_is_isolated`, `strict_converter_value_rules` (test_case), `stored_value_is_a_copy` |
| AC.12 | ported `state_regression.rs` | `stale_handle_fails_after_rewind_reset_compaction` (incl. identical content), `cancel_before_commit_does_not_mutate`, `nested_commit_survives_outer_failure`, `ended_invocation_cannot_commit_state`, `revoked_plugin_cannot_commit_state` |
| AC.13 | `session_checkpoint.rs`, `storage_writer.rs` | `state_only_checkpoint_round_trips_for_root_and_child`, `older_snapshot_cannot_overwrite_newer_state`, `trusted_write_to_dormant_agent_does_not_reactivate`, `reset_clears_agent_and_session_state`, `concurrent_disjoint_updates_preserve_both_edits`, `state_migration_and_notifications_are_ordered`, `stale_expected_revision_returns_conflict_without_mutation`, `fixture_plugin_receives_commit_resume_rewind_notifications`, `migration_result_is_journaled`, `host_slot_handle_shares_records_with_lua` |
| AC.15 | `app/tests.rs` | `post_and_notice_links_open_same_chat_before_and_after_restore`, `missing_agent_link_flashes_unavailable` |
| AC.16 | `history_edit.rs` + event_loop | `rewind_restores_config_state_and_graph_in_one_commit`, `rewind_cancels_active_subtree_first`, `mid_turn_changes_roll_back_with_their_turn`, `child_rewind_leaves_session_state`, `child_compaction_keeps_root_boundaries`, `root_rewind_refuses_across_child_compaction`, `child_rewind_keeps_later_session_records`, `rewind_does_not_autostart_turn_from_requeued_delivery`, `sanitation_remaps_boundary_offsets` |
| AC.17 | event_loop + storage | `redo_restores_pre_rewind_session`, extend `stale_snapshot_after_a_rewind_is_rewritten_not_spliced` for state records, `pruned_branch_reports_expired`, `no_append_reaches_branch_file` |
| AC.18 | `approval/tests` + event_loop | `clear_context_mints_session_with_parent_link`, `rewind_across_approval_reopens_planning_session`, `cross_session_rewind_fails_when_parent_locked_or_other_cwd`, `plan_file_is_rewritten_from_slot_on_rewind`, `plan_file_failure_applies_build_before_admission`, `parent_boundaries_offered_only_at_or_before_link` |
| AC.19 | admission unit + event_loop | `changed_instructions_keep_recorded_content_and_notify_once`, `reload_context_records_and_is_rewindable`, `resume_uses_recorded_context`, `child_prompt_is_fixed_at_spawn` |
| AC.21 | tool_dispatch + event_loop | `pre_image_and_post_hash_recorded_under_write_lock`, `file_restore_skips_user_edited_files`, `redo_restores_overwritten_files`, `rewind_dialog_reports_shell_commands`, `cross_session_rewind_restores_implementation_files`, `file_redo_survives_restart`, `redo_does_not_modify_paths_skipped_by_rewind`, `file_restore_and_redo_round_trip_created_files`, `restore_crash_manifest_reconciles_partial_progress`, `deleted_session_result_and_branch_reads_fail_explicitly` |
| AC.22 | CI | `.ssh/remote-ci.sh`, `just gen-docs-check` |
| AC.23 | storage unit | `unknown_feature_refuses_the_load_and_stays_on_disk`, `first_v3_write_has_empty_feature_set` |
| AC.24 | manager + Lua | `teardown_leaves_children_dormant_not_closed`, `local_tool_child_is_readable_and_not_restorable_after_restart`, `inherited_local_tools_mark_child_not_restorable` |

Iterate with `just fmt-check` locally and crate-scoped checks on the build box; run `.ssh/remote-ci.sh` at the end of every phase.

## Review Strategy

Plan review: use `openai/gpt-6.1-sol` `plan_reviewer` subagents. Resolve findings and repeat until no critical or high findings remain before submission. This revision applies the review at https://gist.github.com/lun-4/2027c5a6a133ba37dab49d46188236dd?permalink_comment_id=6471207#gistcomment-6471207 and incorporates the agreed corrections to comment 6471232: first-cut config recording, checkpoint-qualified recovery, Build fallback, current-handler compatibility and session-deletion limitations. Implementation review: load the `code-review` skill and run it at the end of each phase and once on the full branch, with particular attention to invariants 15, 36, 40, 56, 60, 64-67 and lock ordering in publication claims. Fix or rebut every finding; repeat while critical findings remain.

## Documentation Strategy

- `docs/agent-system.md`: rewrite the deferred list (`:311-322`), node lifecycle and limits (`:181`, `:209-220`), add the journal, restoration-point and residency sections.
- `docs/agent-invariants.md`: revise invariants 2, 41, 68; add invariants for single journal ordering, exactly-once delivery across restart, and teardown-never-closes.
- `AGENTS.md`: storage line gains state log, branches and blobs; Lua line gains `makima.state`.
- `site/docs/`: `context.md` (recorded instructions, `/reload-context`), a persistence/rewind concept page or section (rewind choices, branches, cross-approval rewind), `headless.md`/`acp.md` resume notes. Generated pages (`lua-api`, `tools`, `commands`, `configuration`, `keybindings`) via `just gen-docs`. Tone rules from `AGENTS.md` apply.
- Post the format break (v3, no downgrade), the task-id break, the rename of `max_live_agents` to `max_active_agents` and the new meaning of `max_children_per_agent` (children with outstanding work, not retained children) in the PR description, and update the #24 tracking comment.

## Risks, Blockers, and Required Decisions

Decided by the operator: full scope in one plan; durable `AgentId`; v3 with v2 read; interrupted-never-rerun; legacy children display-only; #14, #166 and #213 unblocked, not implemented; plan state in a slot.

Decisions taken from issue text, flagged for the operator at approval:

- Clear-context approval mints a new session (per #148). The session picker will show planning and implementation sessions separately.
- Changed instruction files require an explicit `/reload-context` (per #41's suggestion).
- `task_id` becomes the durable agent id.

Decisions taken in this revision, flagged for the operator at approval:

- Idle actors are not released within a process. Phase 4 measures first, and release is a follow-up if needed.
- Tools rebind by name at reactivation, with no definition fingerprint.
- Plugin options that are not declared persistent are restored on rewind and not on resume.
- Later format additions define a feature name, and a binary refuses a file with an unknown feature. Unknown fields are not preserved.
- Redo after a cross-session rewind stays in the planning session.
- Session deletion invalidates offloaded results and branch data, including held-ticket references.
- Failed plan-file restoration applies Build fallback before turn admission.
- Outside SDK and ACP, terminal results are observed at commit and do not wait for the checkpoint.

Risks to watch:

- Size. This is the largest PR of the train. Stacked PRs: Phases 0-4 (durable graph, recovery and limits, which fixes #177), Phases 5-7 (configuration, state API, task plugin and links), Phases 8-10 (rewind, linked sessions, recorded context) and Phase 12 (file restore).
- Teardown versus close: `LuaSession::drop` closes subtrees today. If teardown journals `Closed`, every child is lost at exit.
- Resident agents are bounded only by `max_retained_agents` until release exists. The Phase 4 measurement decides whether that is acceptable.
- Reactivation races and re-validation (mode ceiling, vanished provider).
- Removing the three-writer meta merge can regress #149's protection; keep its tests green before deleting code.
- Write amplification from full-file staged checkpoints on every child turn in headless once state grows; measure, and move `save_child` to append if needed.
- Outcome journaling must not deadlock the actor lock or break Idle-before-resolve.
- #173 is not solved here: held tickets still retain full results in memory.
- #315 background tool calls and permission prompts do not survive restart; they surface as interrupted.
- In-flight overlap: #205 (goal plugin), #95 (autoresearch) and #89 (tier profiles) want plugin state; #316 touches compaction accounting. Rebase early.
- Gap: Windows and Nix builds are not exercised by `.ssh/remote-ci.sh`; `nix flake check` should be run once before merge.
