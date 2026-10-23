+++
title = "Agent work"
weight = 32
[extra]
group = "Concepts"
+++

# Agent work

The main agent and its subagents use the same message queue and input controls. Each agent has its own conversation and working directory. A turn is one execution with a fixed result: completed, failed, or cancelled. Several messages can belong to the same turn.

## Messages and turns

An ordinary message starts work on an idle agent or joins compatible work that still accepts instructions. The agent consumes pending messages in order before its next model request, after the current request and tools finish. Sending a message does not interrupt a request or tool already in progress.

A queued configuration change, working-directory change, control operation, or explicitly queued turn keeps later messages from joining earlier work. Those messages receive a new turn ID. Messages and attachments stay separate. Instructions consumed after a turn starts use the `<user-interrupt>` wrapper; its initial input remains an ordinary user message.

Acceptance does not mean the model has received the message. A pending message can be recalled or removed before consumption. Removing one message does not cancel its turn or other messages in that turn. A stale queue selection cannot remove a different message after consumption. Queue limits reject further input without cancelling accepted work.

## Focused controls

Main and subagent views share input, queue, status, and controls. The subagent label identifies the viewed agent. The draft, attachments, and input history remain shared across view changes.

Model, mode, plan, compact, rewind, shell commands, reference expansion, and file completion target the viewed agent. Each new child inherits its parent's working directory. Later directory changes affect only the target agent and take effect in queue order. Commands and pickers keep their original target when the view changes. A closed target returns an error rather than redirecting work to the main agent. Agent restrictions still apply.

Reset and new-session operations remain session-wide. The Lua `maki.session.*` APIs retain session scope.

## Cancellation

UI cancellation stops the viewed agent's current work and removes its ordinary messages already pending when cancellation is accepted. Descendants, siblings, pending child reports, queued configuration and control operations, and later messages survive. If no work is active, cancellation clears only pending ordinary messages.

Cancelling a managed turn through `ticket:cancel` also closes the child subtrees that turn delegated. UI cancellation does not close those children. Explicit close through `task_despawn` or `agent:close` permanently closes the selected agent and its descendants. Children created by trusted plugin handlers have independent ownership; unrelated turn cancellation does not close them, but explicit close or revoking the plugin does.

## Child reports and completion

The main agent may finish while its children run. A subagent waits for children that report to it before completing, subject to execution limits. This completion rule does not change how it accepts messages.

A subagent spawned with an `output_schema` must end each turn with a valid structured report. Consuming new instructions invalidates earlier reports and captured results, so only a fresh report satisfies completion. Child observations do not invalidate reports.

Child reports are internal observations, not user instructions. An active parent receives them after outstanding tool results and before its next eligible model request. Queued work can consume pending reports at that boundary. When no earlier queued work remains, an idle parent receives a bounded batch in a new turn, called a continuation. The runtime does not start a separate turn for every report in that batch.

Late reports can resume an idle or locally cancelled parent. A continuation has a new turn ID and result. It can report that result once to its own parent. Earlier completed or cancelled results stay unchanged. Reports travel only from child to parent, and an acknowledged report does not start another continuation.

A failed continuation keeps reports it has not consumed, without retrying automatically. Explicit work or a newly completed child turn can trigger recovery. Cancellation also preserves pending reports and allows a later continuation. Pending reports are held in memory and do not survive a restart.

## Task waits

`task_wait` observes one exact turn. Without a `turn_id`, it selects the task's latest turn once, including a continuation started to process child reports. Messages that join that turn extend its work. A wait never follows a later turn. A waiting agent releases its turn capacity so its descendants can run.

An exact wait in the parent's own turn claims the matching pending child report instead of also delivering it automatically. Reading a retained result or reaching an observation timeout does not acknowledge a report. A wait cannot retract a report already committed to model history.

Each task retains up to 64 turn tickets. When new turns need room, settled tickets are evicted oldest-first. An unavailable or evicted ID returns `turn_result_unavailable`. If all 64 tickets are unresolved, a new turn is rejected without cancelling accepted work; messages can still join an open turn.

A settled wait returns `{ status = "done", result = ..., error = ... }`, with per-turn usage and `duration_ms` in `result`. A timeout returns `{ status = "running" }` and does not cancel work. `timeout = 0` checks without waiting. See [Task tools](/docs/tools/#task_spawn) for parameters and [Plugins](/docs/plugins/#agent-context-and-lifetime) for Lua invocation authority.
