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

An open agent without a running instance is dormant. Loading a session restores its graph without restarting every child. Sending new work to an eligible dormant agent reactivates it from its saved configuration and capabilities. Missing providers, models, tools, or MCP servers can prevent reactivation. Children that depend on local Lua tool closures cannot reactivate after a restart. Legacy child transcripts remain readable but cannot resume work.

Unfinished admitted turns become cancelled with reason `interrupted` during recovery. Their model requests and tools are not replayed. Reading a transcript or retained result does not rerun the turn. Pending child reports retain their delivery state, so a report already consumed by the parent is not inserted again after restart.

Closing the application releases running instances without permanently closing the saved agents. Explicit agent close through `task_despawn` or `agent:close` remains permanent, but does not retire durable identities. `/task-retire <task_or_agent_id>` separately retires an accessible quiescent leaf agent. Close and retirement preserve saved history and retained results.

Instruction changes on disk do not silently replace the session's recorded context. See [Recorded context](/docs/context/#recorded-context) for `/reload-context`.

## Rewind and branches

In the TUI, `Esc Esc` on an idle agent opens rewind. Selecting a restoration point opens a preview of affected file paths and shell commands. The restore choices are:

| Choice | Effect |
|--------|--------|
| Conversation | Restores conversation history, configuration, and recorded state, including context and plan state. Does not undo ordinary file edits. |
| Conversation and files | Restores the conversation and recorded file edits. |
| Files only | Restores recorded file edits without changing conversation history, configuration, plan state, or the input draft. |

Conversation restore stops active work in the affected subtree before changing its history. Restored history does not automatically replay turns. Requeued child reports remain held until explicit user input allows delivery. A child rewind leaves session-wide settings and unrelated agent history unchanged.

Rewind retains the previous state as a branch. Selecting it under Branches in the rewind picker redoes the restore. Retained branches and file redo survive restart. A pruned branch is shown as expired and cannot be restored. Compaction can make older restoration points unavailable.

Approving a plan with clear context creates a separate implementation session linked to the planning session. Rewind can follow that link to an eligible planning boundary. It reopens the parent conversation and preserves the implementation session separately. Redo stays within the parent session; it does not switch back to the implementation session. A parent open elsewhere or with an incompatible working directory cannot be restored.

### File restore limits

File restore uses the before and after states recorded by mutating tools. It can undo a recorded creation, deletion, or edit. A file that no longer matches the expected state is skipped with a conflict warning, preserving user or external edits. Redo changes only paths that the earlier restore actually changed; it does not overwrite paths skipped because of conflicts.

Shell commands are listed in the preview but are not reverted. Their filesystem changes are not captured as ordinary tool edits. Writes that a tool does not declare and record are also outside file restore. Conversation restore can rewrite the plan file from recorded plan state even when ordinary file restoration is not selected.

File restore is supported on Linux and macOS. On Windows and other platforms, Conversation remains available, but file restore choices return an explicit unsupported error before changing files. If existing file-restore metadata cannot be inspected safely, session startup or deletion fails rather than ignoring that metadata.

An interrupted file restore is reconciled before new work can start. Unresolved restore conflicts can keep the affected sessions blocked; a failed or pending restore must not be bypassed by deleting its metadata.

## Messages and turns

An ordinary message starts work on an idle agent or joins compatible work that still accepts instructions. The agent consumes pending messages in order before its next model request, after the current request and tools finish. Sending a message does not interrupt a request or tool already in progress.

A queued configuration change, working-directory change, control operation, or explicitly queued turn keeps later messages from joining earlier work. Those messages receive a new turn ID. Messages and attachments stay separate. Instructions consumed after a turn starts use the `<user-interrupt>` wrapper; its initial input remains an ordinary user message.

Acceptance does not mean the model has received the message. A pending message can be recalled or removed before consumption. Removing one message does not cancel its turn or other messages in that turn. A stale queue selection cannot remove a different message after consumption. Queue limits reject further input without cancelling accepted work.

## Focused controls

Main and subagent views share input, queue, status, and controls. The subagent label identifies the viewed agent. The draft, attachments, and input history remain shared across view changes.

Model, mode, plan, compact, rewind, shell commands, reference expansion, and file completion target the viewed agent. Each new child inherits its parent's working directory. Later directory changes affect only the target agent and take effect in queue order. Commands and pickers keep their original target when the view changes. A closed target returns an error rather than redirecting work to the main agent. Agent restrictions still apply.

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
