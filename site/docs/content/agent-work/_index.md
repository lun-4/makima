+++
title = "Agent work"
weight = 32
[extra]
group = "Concepts"
+++

# Agent work

The main agent and its subagents use the same message queue and input controls. Each agent has its own conversation and working directory. A turn is one execution with a fixed result: completed, failed, or cancelled. Several messages can belong to the same turn.

## Persistence and resume

A saved session retains the main agent and its subagents as a graph. Each agent has a durable ID, distinct from the session ID and its turn IDs. `task_id` is the durable agent ID. History, committed configuration, recorded context, retained turn results, and recorded child-report delivery state survive a restart.

An open agent without a running instance is dormant. Loading a session restores its graph. An agent that was idle stays dormant until it receives work. Missing providers, models, tools, or MCP servers can prevent reactivation. Children that depend on local Lua tool closures cannot reactivate after a restart. Legacy child transcripts remain readable but cannot resume work.

### Restart

When a saved session is reopened after a quit or crash, every unfinished turn continues by default, including the root turn and any subagent turns. Makima requests a reply again if it was cut off while streaming. A resumable tool call runs or waits again. For example, `question` asks again and `task_wait` resumes waiting. For other in-flight calls, Makima sends the model this result instead of retrying the call:

> This tool call was interrupted when makima stopped. It may have partly run. Check the current state before retrying, then continue your task.

`MAKIMA_CONTINUE_ALL_AGENTS` defaults to `1`. Set it to `0` to record unfinished turns as `Interrupted` instead. When turns continue, Makima shows `continuing N interrupted turns (set MAKIMA_CONTINUE_ALL_AGENTS=0 to stop this)`. A subagent that cannot continue reports the reason to its parent. If the saved root mode cannot be restored, Makima warns, switches to read-only mode, and waits for input instead of resuming the root turn automatically.

Pending child reports keep their delivery state. A report the parent already read is not delivered again. Reading a saved transcript or result does not rerun its turn.

Quitting releases running instances without permanently closing the saved agents. Explicit agent close through `task_despawn` or `Agent:close` remains permanent, but does not retire durable identities. `/task-retire <task_or_agent_id>` separately retires an accessible quiescent leaf agent. Close and retirement preserve saved history and retained results.

Instruction changes on disk do not silently replace the session's recorded context. See [Recorded context](/docs/context/#recorded-context) for `/reload-context`.

### Session storage

The session log is append-only. It keeps every saved message, including cut-off branches and the original messages from before compaction, until the session is deleted.

If a write fails, the terminal UI shows an unsaved warning. Makima keeps the records in memory and retries while agents keep working. If any records remain unsaved at quit, Makima warns before exiting. Unsaved records may be lost on exit or crash.

## Rewind

In the terminal UI, press `Esc Esc` in the idle root chat to open the rewind picker. If the root is running, stop it first. Only the root can be rewound.

- Current context lists user turns in the root's current conversation. Selecting one puts its prompt back in the input box and rewinds to just before that prompt. Makima drops the root's pending messages.
- Rewound lists prompts outside the current context, including cut-off branches and prompts from before compaction. Select one to return to that line of conversation. This is how redo works.
- Back to planning session is available after plan approval with clear context. It suspends the implementation session as a quit would and opens the planning session without cutting its conversation. Files changed during implementation stay changed. See [Restart](#restart) for what happens when the implementation session is reopened.

Selecting a prompt appends a fork to the session log. The fork changes only the root's conversation context. Subagents keep running and their new reports still reach the root. Mode, plan, configuration, rules, recorded context, plugin state, and files all keep their current values.

The root chat marks the fork and shows a system note with the current mode, a warning that files may differ from the conversation, and a list of existing subagents. The model sees the note too. The [`task_list` tool](/docs/tools/#task_list) can show the current task list later.

## Messages and turns

An ordinary message starts work on an idle agent or joins compatible work that still accepts instructions. The agent consumes pending messages in order before its next model request, after the current request and tools finish. Sending a message does not interrupt a request or tool already in progress.

A queued configuration change, working-directory change, control operation, or explicitly queued turn keeps later messages from joining earlier work. Those messages receive a new turn ID. Messages and attachments stay separate. Instructions consumed after a turn starts use the `<user-interrupt>` wrapper; its initial input remains an ordinary user message.

Acceptance does not mean the model has received the message. A pending message can be recalled or removed before consumption. Removing one message does not cancel its turn or other messages in that turn. A stale queue selection cannot remove a different message after consumption. Queue limits reject further input without cancelling accepted work.

## Focused controls

Main and subagent views share input, queue, status, and controls. The subagent label identifies the viewed agent. The draft, attachments, and input history remain shared across view changes.

Model, mode, plan, compact, shell commands, reference expansion, and file completion target the viewed agent. Each new child inherits its parent's working directory. Later directory changes affect only the target agent and take effect in queue order. Commands and pickers keep their original target when the view changes. A closed target returns an error rather than redirecting work to the main agent. Agent restrictions still apply.

Reset and new-session operations remain session-wide. The Lua `makima.session.*` APIs retain session scope.

## Cancellation

UI cancellation stops the viewed agent's current work and removes its ordinary messages already pending when cancellation is accepted. Descendants, siblings, pending child reports, queued configuration and control operations, and later messages survive. If no work is active, cancellation clears only pending ordinary messages.

Cancelling a managed turn through `ticket:cancel` also closes the child subtrees that turn delegated. UI cancellation does not close those children. Explicit close through `task_despawn` or `agent:close` permanently closes the selected agent and its descendants without retiring their durable identities. Children created by trusted plugin handlers have independent ownership; unrelated turn cancellation does not close them, but explicit close or revoking the plugin does.

## Child reports and completion

The main agent may finish while its children run. A subagent waits for children that report to it before completing, subject to execution limits. This completion rule does not change how it accepts messages.

A subagent spawned with an `output_schema` must end each turn with a valid structured report. Consuming new instructions invalidates earlier reports and captured results, so only a fresh report satisfies completion. Child observations do not invalidate reports.

Child reports are internal observations, not user instructions. An active parent receives them after outstanding tool results and before its next eligible model request. Queued work can consume pending reports at that boundary. When no earlier queued work remains, an idle parent receives a bounded batch in a new turn, called a continuation. The runtime does not start a separate turn for every report in that batch.

Late reports can resume an idle or locally cancelled parent. A continuation has a new turn ID and result. It can report that result once to its own parent. Earlier completed or cancelled results stay unchanged. Reports travel only from child to parent, and an acknowledged report does not start another continuation.

A failed continuation keeps reports it has not consumed, without retrying automatically. Explicit work or a newly completed child turn can trigger recovery. Cancellation also preserves pending reports and allows a later continuation. Recorded pending reports survive a restart with their delivery state.

## Task waits

`task_wait` observes one exact turn. Without a `turn_id`, it selects the task's latest turn once, including a continuation started to process child reports. Messages that join that turn extend its work. A wait never follows a later turn. A waiting agent releases its turn capacity so its descendants can run.

An exact wait in the parent's own turn claims the matching pending child report instead of also delivering it automatically. Reading a retained result or reaching an observation timeout does not acknowledge a report. A wait cannot retract a report already committed to model history.

The live runtime retains up to 64 turn tickets per agent. When new turns need room, settled tickets are evicted oldest-first. If all 64 tickets are unresolved, a new turn is rejected without cancelling accepted work; messages can still join an open turn. Saved state retains up to 128 terminal results per agent. Those results can be read after restart without reactivating the agent, even after their live tickets have been evicted. An unavailable result or a missing result artifact returns `turn_result_unavailable`.

A settled wait returns `{ status = "done", result = ..., error = ... }`, with per-turn usage and `duration_ms` in `result`. A timeout returns `{ status = "running" }` and does not cancel work. `timeout = 0` checks without waiting. See [Task tools](/docs/tools/#task_spawn) for parameters and [Plugins](/docs/plugins/#agent-context-and-lifetime) for Lua invocation authority.
