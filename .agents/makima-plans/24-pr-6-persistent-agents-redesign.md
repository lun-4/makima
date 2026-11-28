# Agent Revamp PR 6: Persistence, redesign

This document replaces the rewind, file restore and storage parts of `24-pr-6-persistent-agents.md`. It records the design agreed in review (questions Q1 to Q41), with the Q6 decision revised after review. Durable agents, the Lua agent API, `makima.state`, the task plugin changes and recorded context stay as planned there, except where this document says otherwise.

## Goal

Keep what makes agents persistent. Make rewind small and simple.

Rewind changes the selected idle agent's conversation and pending input. Other agents, mode, plan, config, rules, plugin state, and files are unaffected.

## Core rules

1. The session log is append-only. Normal saves only append records. Nothing is deleted or rewritten. (Q9, Q30)
2. Rewind changes only the selected idle agent's conversation and pending input. It leaves other agents, non-conversation state, and files unchanged. (Q2, Q6, Q8)
3. A quit or restart does not interrupt anything. On the next start, every agent continues as if nothing happened. (Q10, Q11)
4. All frontends write through one API, with one writer in `makima-agent`. (Q36) After review this became stricter: frontends are clients. They read the journal's projection and change the session only through agent-layer verbs. See [Frontends and the TUI](#frontends-and-the-tui-after-review).

## The log

One file per session, `sessions/<session-id>.jsonl`, as today. (Q27)

- Messages use one record type for every agent: `Msg { id, agent, d }`. `id` is stable (log `seq` or a `MakimaId`). It replaces both `Msg` (root, no id) and `SubMsg { sub }`. (Q20, Q31)
- `meta.subagents`, `meta.subagent_conversations` and `meta.history_epochs` are removed. The agent records are the only description of children. (Q31)
- Compaction is an event in the log. The old messages stay in the file. (Q9)
- A rewind appends a fork record for the selected agent. The record identifies the target by stable ID and stores the system note. (Q16, Q19, Q20)
- Posts and the planning-session link refer to messages by id, not by position. `Boundary` records are removed. (Q20)
- State records (Config, Rules, Context, plugin slots, turns, deliveries) stay. They are only read as "latest". The "fold to sequence N" reducer, per-boundary snapshots and rewind previews are removed. (Q32)
- Recorded context keys become fixed strings: `instruction:<path>` and `prompt:<plugin>:<slot>:<name>`. They are no longer serialized Rust enums or registration order. The v3 format is unreleased, so this needs no migration. (Q33)

### Writes

- Every save appends new records only. (Q30)
- The only full rewrite is the one-time migration of a v2 session when it is first opened. The original goes to `archive/<id>/`. (Q30)
- Deleting a session removes its files. Deletion goes through the writer. (Q36)

## Building an agent's context

The context builder reads the selected agent's events in log order (Q19):

```
Msg         append the message
Compaction  replace everything so far with the summary
Fork(id)    set the context back to what it was right after message `id`,
            then add the fork's system note
```

- A fork can target a message from before a compaction. The context then holds the original messages again. If they no longer fit (the model changed since, for example), the usual auto-compaction runs on the next turn. (Q19)
- A fork can target a message in a branch that an earlier fork cut off. That is how redo works. There is no separate redo command and no branch files. (Q21)

## Rewind

### Rules

- Any idle agent can be rewound from its chat, including a subagent. This replaces the root-only decision made in review. (Q6)
- Targets are the start of the selected agent's user turns. On commit, the picker returns the prompt to the input box and drops that agent's pending messages. (Q3, Q5)
- Files are never read, restored or written. (Q8)
- Mode, plan, config, rules, plugin state and recorded context are not rewound. (Q2, Q17)
- Rewind does not modify other agents' conversations or queues and does not interrupt their work. (Q1)
- Pending and future child reports remain eligible for delivery, including reports about tasks removed from the selected agent's conversation. (Q2)
- Rewind preserves delivery dispositions. Reports the parent has already received are not delivered again. (Q26)

### Agent API

Rewind lives in `makima-agent` as a two-step API. (Q28)

```
begin_rewind()          fails if the selected agent is not idle
                        acquires and returns that agent's idle guard
guard.targets()         user turns in the agent's current context and cut-off branches;
                        "back to planning session" in the root picker if a parent link exists
guard.commit(target)    appends the fork record, or switches to the planning session
drop(guard)             releases the lock and writes nothing
```

- The TUI calls `begin_rewind` on the viewed agent when the picker opens. The guard prevents that agent from starting a turn. On commit, the selected agent's pending messages are dropped. Pending child reports can be delivered after the guard is released. (Q15, Q26)
- Only the TUI uses the API in this PR. SDK and ACP get commands for it later. (Q35)

### System note

The runtime builds the system note at commit and stores it in the fork record so context replay does not need to rebuild it. (Q4, Q16) The note tells the selected agent that its conversation was rewound. It also includes the current mode, a warning that files may differ from the conversation, and all descendants of the selected agent present in the agent graph at commit. Each entry shows the agent ID, graph label (if set), and status (running, idle or done). The note does not read the task plugin's private state or include tasks whose agents are absent from the graph.

### UI

The selected agent's chat shows its current context. A divider row marks the fork ("rewound from turn X"). The system note appears under the divider. The rewind picker lists cut-off messages in "rewound" groups. (Q21, Q22)

### Back to the planning session

Clear-context plan approval still starts a new session with a link to the planning session. (Q18, Q25) The implementation session's root picker offers "back to planning session" as a separate navigation action. This action does not append a fork. It:

- suspends the implementation session the same way a quit does. Its agents continue when it is opened again. Nothing is recorded as `Interrupted`;
- opens the planning session as it is (Plan mode, plan ready). Its conversation is not cut;
- leaves files alone. The dialog says that files changed during implementation stay changed.

## Restart

- On the next start, every agent that was mid-turn continues, the root included. No interruption is reported to a parent. In-flight tool calls of a parent, such as `task_wait`, continue too. (Q10, Q11)
- A response that was cut off while streaming is thrown away and the same request is sent again. (Q29)
- Each in-flight tool call is handled by whether it is resumable (Q23):
  - Resumable tools run again or wait again (`task_wait`, `question`). The rule: stateless tools are resumable. Tools opt in with `resumable = true` on `register_tool`. The default is not resumable. Document this on `register_tool`.
  - All other tools (`bash`, `write`, `edit`, `webfetch`, MCP calls, `code_execution`) get the restart text (below), and the model decides.
- `MAKIMA_CONTINUE_ALL_AGENTS` (default `1`) turns this on or off for the root and all subagents. With `0`, unfinished turns are recorded as `Interrupted`, as in the original plan. When turns are continued, makima shows one line: "continuing N interrupted turns (set MAKIMA_CONTINUE_ALL_AGENTS=0 to stop this)". (Q13, Q24)
- A child that cannot continue (provider or model gone, private `local_tools`, the switch set to `0`) reports its turn to the parent as failed with the reason. This is the normal failure path. (Q38, Q41)
- If the mode cannot be restored on resume, the session falls back to a read-only mode with a warning, and the root does not continue by itself. It waits for the user. (Q39)

### Interruption text

`[Cancelled by user]` on unfinished tool calls is replaced by two texts (Q12):

- User pressed stop: "This tool call was interrupted by the user before it finished. It may have partly run."
- Restart: "This tool call was interrupted when makima stopped. It may have partly run. Check the current state before retrying, then continue your task."

`[Response cut off by user cancel]` stays for a reply the user stopped.

### User interrupts

Escape interrupts the agent the user is looking at. Interrupting a subagent this way is reported to its parent as usual. Only a quit and restart never interrupts. (Q14)

## Task plugin

- `task_list` shows the ID, description and status (running, idle or done) of the caller's descendant tasks. It reads the `policy` slot and the agent graph. If an agent is missing from the graph, `task_list` shows its task as done only if the caller appears in the task's stored ancestry. The rewind note uses the agent graph instead of the task plugin's private state. (This revises Q34.)

## Single writer

- One writer in `makima-agent` handles every save, for every frontend. (Q36)
- UI-only data (posts, input draft, queued messages) is sent to it as records.
- The TUI's `StorageWriter`, `CoordinatorCheckpointWriter` and their merge code are deleted.

### Shape (Q42)

- One writer task per open session, in `makima-agent`.
- Frontends and agents call `append(records)` and get a receipt they can wait on.
- The writer assigns every record its `seq` and appends in arrival order.
- The writer is the only code that opens the session file.
- It never holds its state lock across a write or fsync.

### Durability (Q43)

- The writer appends everything queued, then fsyncs once for the batch.
- These wait for their receipt (the fsync): turn end, rewind commit, config and mode changes, spawns.
- Streaming output and UI-only records (draft, posts) do not wait.

### Crash rule (Q44)

If makima dies in the middle of an append, the file ends in an incomplete line. On load, that line is ignored, and it is cut off before the first new append. Every complete line before it is kept.

### Write failures (Q45)

- On a failed write (disk full, permissions), the writer keeps the records in memory and retries with backoff.
- A warning stays visible until a write succeeds.
- Agents keep working.
- Receipts that are being waited on fail with the error, so a rewind commit or spawn reports it.
- On quit with unsaved records, makima warns before it exits.

### Testing (Q48)

The writer writes through a small file trait. Tests use a fake that can deterministically inject:

- disk full;
- a failed fsync;
- a torn write (part of a line written, then a crash).

These tests cover Q44 and Q45 and check that agents keep running and nothing panics.

## Opening a session (Q46)

SDK, ACP and TUI open sessions through one function in `makima-agent`. It claims the lock, loads (migrating v2 once), recovers, chooses the model (recorded model, else the startup model, with the policy check) and starts the writer. The hand-written copies are deleted:

- `src/sdk_mode.rs:1734-1846`;
- `makima-acp/src/server.rs:600-700`;
- `src/cmd/tui.rs` (`resolve_session`, `restored_session_model`).

The copies have drifted today: TUI ignores the recorded model when one is passed explicitly, ACP fails hard when the policy rejects it, and SDK has its own `startup_model`. Pick one behaviour.

After review this grew into the whole session pipeline, `makima_agent::session::{open, prepare}`, used by TUI, SDK, ACP and print. It also starts the shared backend, binds Lua, checkpoints the startup config and installs the observer. See below.

## Frontends and the TUI (after review)

Review ("current agent, not root") and hands-on testing of the TUI led to more work in this PR. The frontend part follows its own plan, [24-pr-6-frontend-interface.md](24-pr-6-frontend-interface.md), one commit per step.

### Current agent, not root

A reviewer asked to aim for "current agent" instead of "root only" wherever possible, so the PR does not build a root/guest split into the design.

- Any idle agent can be rewound, including a subagent. The `Fork` record names its agent. The note lists that agent's graph descendants. In the TUI, `Esc Esc` works in any tab and rewinds the agent you are looking at. (This revises Q6.)
- `task_list` shows a task whose agent is gone to any ancestor. Each task stores the chain of agents that started it.
- `/reload-context` runs from any agent and reloads the session's recorded context.
- What stays root-specific is structural: one parentless agent per graph, and ACP/SDK expose one conversation per session.

### One submit path, dormant agents wake

Typing into a tab and `task_send` are the same operation. Both call `AgentRef::submit`. A dormant agent is woken first with `AgentRef::wake`, through a reactivator that the Lua host installs once per runtime. So a restored subagent tab accepts input and opens its rewind picker. The input box is cleared only when the input is accepted, so a refused send keeps the text.

After a restart, only agents with unfinished admitted turns continue. An agent that was idle loads as dormant and is woken on demand.

### The TUI does not own subagents

The TUI used to rebuild a v2-style subagent list from its tabs and write it into its session copy. That made up agent records at the copy's own sequence numbers, which collided with the journal and broke the shutdown projection. Tabs now come from the journal's agent records and labels. `StoredSubagent`, `set_subagents` and `sync_subagents` are gone, and `legacy_child` lives only in the v2 migration.

### Frontends are clients

- **The journal owns every session fact and writes it once:** title (derived from the first root user text, or an explicit rename), usage and cost, tool outputs, posts (anchored to the agent's last message), input held after a failed turn (`StateChange::InputHold`), `!cmd` results waiting for the agent's next turn (`StateChange::Preamble`), and the draft.
- **Frontends read `SessionJournal::projection()`.** The TUI keeps no session copy. `App::checkpoint`, `AppSession::checkpoint`, `append_ui_record` and `project_session_journal` are gone.
- **One backend.** `HeadlessBackend` runs every agent for every frontend. `FrontendProfile` holds what differs (question mode, interactive reauth, tool output lines, holding input after a failure, excluded tools, owned or shared MCP, setup error reporting). `TuiActorBackend` is gone.
- **Shared verbs:** `AgentRef::{submit, wake, run_shell, ask_isolated, begin_rewind}`, `CancelScope` with `SessionCancellation`, one `AnswerRouter` per session, and one reset (`ResetSeed`).
- **Isolated questions** (`/btw`) run outside the actor queue for every frontend, from a snapshot of the agent's context and history, so they work during a busy turn.

### TUI fixes found by hands-on testing

- `/tasks` ages a row from its turn's admission, keeps the age and context size of restored subagents, and nests background tool calls under the agent that runs them.
- The session cost in the status bar grows during a run. It used to show only the total restored at startup (#168).
- A subagent's first prompt is shown once.
- Restore snapshots go only to the chat that holds the tool row, so other tabs no longer log `snapshot dropped`.
- Plugin callbacks on host events (`TurnEnd`, `SessionStatusChanged`) are no longer killed after an unrelated cancelled task (they were dispatched without their own scope).
- After a failed turn, queued messages are held instead of being lost when the next run starts.

## Removed from the PR

- File restore: `file_restore/*` (including `secure.rs` and `recording.rs`), `makima-storage/src/restore.rs`, `blobs.rs`, `restore_digest.rs`, the `File`, `ToolExecution` and `RestoreRef` records, `sessions/restores/`, `sessions/blobs/`, the restore form, the shell-command counter, and `shell_execution` on tools. (Q8, Q17)
- Boundary pins and `BoundarySnapshot`. (Q17)
- Rewind into the planning session as a state restore: `parent_rewind.rs`, the rewind half of `plan_state.rs`, and `ParentLink.message_count` as a position. (Q7, Q17, Q18)
- Folding Config, Rules, Context and plugin state back to a turn, including the rewindable `/reload-context`. (Q17, Q32)
- Graph reconciliation on rewind (`manager/rewind.rs` `reconcile`). (Q17)
- Branch files (`sessions/branches/`) and branch pruning. (Q21)
- The full-rewrite save path for normal checkpoints, and `SessionLog` cursor code that only served it. (Q30)
- `Boundary` records. (Q20)
- `meta.subagents`, `meta.subagent_conversations`, `meta.history_epochs`. (Q31)
- `makima-agent/benches/boundary_pin_profile.rs` and `makima-lua/tests/memory_profile.rs`. (Q40)

## Fixed in this branch

- Write a child's spawn and its spawn spec in one commit. With continue-on-resume, a gap between them hangs the parent. (Q37)
- Turn-result conversion retries forever and blocks every later commit (`journal.rs:1442-1475`, `1820`). (Q40)
- The journal incarnation is set before steps that can fail, during activation (`manager/mod.rs:657-670`). (Q40)
- The headless initializer publishes outside the activation guard (`headless.rs:1393-1474`). (Q40)
- One session activating makes option changes fail in every other session (`session_coordinator.rs:2151`). (Q40)
- An "always allow" grant whose config write fails is only logged (`permissions.rs:916-923`). (Q40)
- `makima.state` replay overwrites the revision and can migrate during a read-only access (`api/state.rs:32`, `agent state.rs:300-311`). (Q40)
- A checkpoint is confirmed before the journal is installed, so on an install error the disk is ahead of memory with no rollback (`session_coordinator.rs:2134-2146`, `2424-2440`; `session_checkpoint.rs:543-548` publishes, then commits). Install first, or make both one step that can be rolled back.
- The single writer must never hold its state lock across a write or fsync. The old TUI writer did (`storage_writer.rs:2802-2816`), and the UI thread stalled on every rewind, restore and config save.
- Bugs that existed before this PR and are fixed here:
  - `headless.rs:761` unwraps `session_id` in manual compaction, and print mode passes `None`, so `/compact` in print mode panics;
  - ACP `load_session` closes the current session before it locks and loads the new one (`makima-acp/src/server.rs:606`), so a failed load leaves no session;
  - `lua_to_json` (`makima-lua/src/api/util/convert.rs:362-384`) has no cycle or depth guard, so a self-referencing table overflows the stack. Use the depth limit `state_value_*` already has.
- Waking a dormant child calls `fetch_all(.., refresh = true)` (`makima-lua/src/api/agent/reactivation.rs:52-68`), which needs the network, and maps every `prepare_model` error to `provider_unavailable` (`:99-101`). Every agent now continues after a restart, so wake from the cached catalog first, refresh only on a miss, and keep the real error.
- One shared lock helper instead of 403 copies of `lock().unwrap_or_else(|e| e.into_inner())`. Dead branches removed. (Q40)
- Rules that are only kept by convention today (Q47):
  - an RAII guard pairs `record_terminal` with `release_terminal`. Today a missed `release_terminal` makes `prepare_reserved` wait forever while it holds the publication and boundary locks (`actor/mod.rs:1374-1386`, `journal.rs:1816-1882`);
  - `finishing` moves under the graph lock. Today it is an `AtomicBool` read outside the lock (`manager/mod.rs:79`, `127`, `699`, `1023`);
  - the lock order is written in `docs/agent-invariants.md`, including the session `DIRECTORY` and catalog locks;
  - `aborted_preparations` is pruned (`journal.rs:366`, `557`).
- Cheap cleanups (Q47):
  - the new writer has no `#[cfg(test)]` hooks in production code. Tests use the file trait;
  - the extra JSON round trips in `makima.state` go (`convert.rs:173-268`, `api/state.rs:207-218`, `agent state.rs:391-399`);
  - one copy of the Windows NT-API helpers (`makima-agent/src/tools/offload/windows.rs` and `makima-storage/src/offload_cleanup/windows.rs`).
- Shared test support per crate: one polling helper and one mock backend, instead of the current copies (Q48).

## Not in this PR

- Rewind for SDK and ACP. (Q35) The agent-layer call needs no frontend state, so it can be added later.
- Lua `makima.session.*` (list, create, focus, rename, delete) outside the TUI. It still goes through the TUI's UI channel.
- The TUI still schedules lease heartbeats, coordinates tab replacement and keeps `ProviderSlot` for model discovery and usage. SDK picks fork sources itself.
- Letting plugins provide their private `local_tools` again at load, so such agents can be woken. (Q41)
- Splitting the log per agent. (Q27)
- Session lock liveness depends on file mtime (`HEARTBEAT_INTERVAL` 2 s, `STALE_AFTER` 5 s in `makima-storage/src/session_lock.rs:27-28`), so clock skew and coarse mtime granularity affect who owns a session. Known hazard, unchanged here.
- The remaining `#[cfg(test)]` hooks in production code, `Option` + `expect` state machines, and full snapshot clones under locks. (Q47)
- Original input (#14), the todo plugin (#166), tool-row agent links (#213), byte-bounded result memory (#173), background tool calls across restart (#315), automatic eviction of idle agents.

## Docs to update

- Rewind: any idle agent is eligible. Only the selected agent's conversation and pending input change.
- Restart behaviour and `MAKIMA_CONTINUE_ALL_AGENTS`.
- `register_tool`: `resumable`, with the rule "stateless tools are resumable".
- `task_list`.
- Remove file restore, the three rewind choices and the shell-command counter from the docs.
- Storage: the session log keeps every message, including cut-off branches and pre-compaction messages, until the session is deleted.
