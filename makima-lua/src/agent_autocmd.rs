//! One place that turns agent events into autocmds. Lifecycle events come
//! from the graph itself ([`observe`]), tool and compaction events from the
//! envelopes a frontend reads ([`dispatch`]), so a plugin sees the same events
//! for every agent, children included, whichever way makima was started.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use makima_agent::{
    ActorEvent, ActorWorkIdentity, AgentEvent, AgentId, AgentManagerHandle, AgentRef,
    CompactionStatus, DoneReason, Envelope, TurnId, TurnOutcome,
};
use serde_json::{Value, json};

use mlua::Lua;

use crate::EventHandle;
use crate::api::agent::{origin_json, result_json, selection_json};
use crate::runtime::{HostId, Request, RequestTx};

pub const TURN_START_EVENT: &str = "TurnStart";
pub const TURN_END_EVENT: &str = "TurnEnd";
pub const AGENT_IDLE_EVENT: &str = "AgentIdle";
pub const AGENT_CLOSED_EVENT: &str = "AgentClosed";
pub const CONFIG_CHANGED_EVENT: &str = "ConfigChanged";
pub const COMPACTION_DONE_EVENT: &str = "CompactionDone";
pub const SESSION_CWD_CHANGED_EVENT: &str = "SessionCwdChanged";

/// Fires the autocmd for one envelope a frontend read from {root}'s graph.
pub fn dispatch(handle: &EventHandle, root: &AgentRef, envelope: &Envelope) {
    let agent = match &envelope.subagent {
        Some(subagent) => root.manager().reference(subagent.agent_id),
        None => root.clone(),
    };
    if let Some((event, data)) = envelope_autocmd(
        &envelope.event,
        agent.id(),
        envelope.identity.and_then(|identity| identity.turn_id()),
        handle.tool_calls(),
    ) {
        handle.fire_agent_autocmd(event, agent, data);
    }
}

/// What `ToolDone` cannot carry itself: the input a call ran with and when the
/// frontend saw it start. `ToolStart` is the only event with both, so the host
/// holds them until the matching `ToolDone` reads them back. A call that never
/// reports done has neither.
#[derive(Clone, Default)]
pub struct ToolCalls(Arc<Mutex<HashMap<(AgentId, String), StartedCall>>>);

struct StartedCall {
    input: Option<Value>,
    started: Instant,
    turn_id: Option<TurnId>,
}

impl ToolCalls {
    fn start(&self, agent: AgentId, id: &str, turn_id: Option<TurnId>, input: Option<Value>) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(
                (agent, id.to_owned()),
                StartedCall {
                    input,
                    started: Instant::now(),
                    turn_id,
                },
            );
    }

    fn finish(&self, agent: AgentId, id: &str) -> Option<StartedCall> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(agent, id.to_owned()))
    }

    fn finish_turn(&self, agent: AgentId, turn_id: TurnId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(call_agent, _), call| *call_agent != agent || call.turn_id != Some(turn_id));
    }

    fn close_agent(&self, agent: AgentId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain(|(call_agent, _), _| *call_agent != agent);
    }
}

/// Fires the lifecycle autocmds of every agent in {manager}'s graph until
/// the graph is dropped or the host stops. Observing a graph twice is a no-op.
pub(crate) fn observe(lua: &Lua, manager: &AgentManagerHandle) {
    let (Some(host), Some(tx)) = (
        lua.app_data_ref::<HostId>(),
        lua.app_data_ref::<RequestTx>(),
    ) else {
        return;
    };
    let Some(events) = manager.subscribe_events(host.0) else {
        return;
    };
    let tx = tx.0.clone();
    smol::spawn(async move {
        let mut idle = IdleFilter::default();
        while let Some((agent, event)) = events.recv().await {
            if !idle.passes(&event) {
                continue;
            }
            let Some((name, data)) = actor_autocmd(event) else {
                continue;
            };
            let request = Request::FireAutocmd {
                event: name.to_owned(),
                data,
                agent: Some(agent),
            };
            if tx.send_async(request).await.is_err() {
                break;
            }
        }
    })
    .detach();
}

/// Lets an actor's idle through only when work settled since the last one.
/// A config-only commit passes through idle too, and an AgentIdle callback
/// that changes config would otherwise wake itself forever.
#[derive(Default)]
struct IdleFilter(HashMap<AgentId, ActorWorkIdentity>);

impl IdleFilter {
    fn passes(&mut self, event: &ActorEvent) -> bool {
        match event {
            ActorEvent::Idle { agent_id, settled } => {
                settled.is_some_and(|settled| self.0.insert(*agent_id, settled) != Some(settled))
            }
            ActorEvent::Close { agent_id, .. } => {
                self.0.remove(agent_id);
                true
            }
            _ => true,
        }
    }
}

fn envelope_autocmd(
    event: &AgentEvent,
    agent: AgentId,
    turn_id: Option<TurnId>,
    tool_calls: &ToolCalls,
) -> Option<(&'static str, Value)> {
    match event {
        AgentEvent::ToolStart(e) => {
            tool_calls.start(agent, &e.id, turn_id, e.raw_input.clone());
            Some(("ToolStart", json!({ "tool_id": e.id, "tool": e.tool })))
        }
        AgentEvent::ToolDone(e) => {
            let mut data = json!({
                "tool_id": e.id,
                "tool": e.tool,
                "is_error": e.is_error,
                "bytes": e.output.as_text().len(),
            });
            if let Some(call) = tool_calls.finish(agent, &e.id) {
                if let Some(input) = call.input {
                    data["input"] = input;
                }
                data["duration_ms"] = json!(call.started.elapsed().as_millis() as u64);
            }
            Some(("ToolDone", data))
        }
        AgentEvent::ToolMoved { id, .. } => {
            tool_calls.finish(agent, id);
            None
        }
        AgentEvent::TurnOutcome(outcome) => {
            tool_calls.finish_turn(agent, outcome.turn_id());
            None
        }
        AgentEvent::SubagentClosed => {
            tool_calls.close_agent(agent);
            None
        }
        AgentEvent::AutoCompacting {
            context_size,
            context_window,
        } => Some((
            "AutoCompacting",
            json!({ "context_size": context_size, "context_window": context_window }),
        )),
        AgentEvent::CompactionDone {
            context_size_before,
            context_size_after,
            context_window,
            summary,
        } => Some((
            COMPACTION_DONE_EVENT,
            json!({
                "status": compaction_status(CompactionStatus::Completed),
                "context_size_before": context_size_before,
                "context_size_after": context_size_after,
                "context_window": context_window,
                "summary": summary,
            }),
        )),
        _ => None,
    }
}

fn actor_autocmd(event: ActorEvent) -> Option<(&'static str, Value)> {
    match event {
        ActorEvent::Start {
            turn_id,
            provenance,
            config,
            text,
            ..
        } => Some((
            TURN_START_EVENT,
            json!({
                "turn_id": turn_id.to_string(),
                "origin": origin_json(&provenance),
                "selection": config.map(|commit| selection_json(&commit.config)),
                "text": text,
            }),
        )),
        ActorEvent::End(result) => {
            let mut data = result_json(&result);
            let outcome = &result.outcome;
            if let TurnOutcome::Completed { reason, .. } = outcome {
                data["reason"] = turn_end_reason(*reason)?.into();
            }
            data["cost"] = json!(outcome.cost());
            data["list_cost"] = json!(outcome.list_cost());
            data["context_size"] = outcome.context_size().into();
            data["context_window"] = outcome.context_window().into();
            data["num_turns"] = outcome.num_turns().into();
            Some((TURN_END_EVENT, data))
        }
        ActorEvent::Idle { settled, .. } => Some((
            AGENT_IDLE_EVENT,
            json!({ "settled": settled.map(|work| work.presentation_id) }),
        )),
        ActorEvent::Config(commit) => Some((
            CONFIG_CHANGED_EVENT,
            json!({
                "generation": commit.generation,
                "selection": selection_json(&commit.config),
                "mode": commit.config.mode.id().key(),
            }),
        )),
        ActorEvent::CompactionEnd(status) => Some((
            COMPACTION_DONE_EVENT,
            json!({ "status": compaction_status(status) }),
        )),
        ActorEvent::Close { lifecycle, .. } => Some((
            AGENT_CLOSED_EVENT,
            json!({ "lifecycle": format!("{lifecycle:?}").to_lowercase() }),
        )),
        _ => None,
    }
}

/// The status words of `TurnEnd`, so one check reads both events.
fn compaction_status(status: CompactionStatus) -> &'static str {
    match status {
        CompactionStatus::Completed => "completed",
        CompactionStatus::Failed => "failed",
        CompactionStatus::Cancelled => "cancelled",
    }
}

/// Nothing for a manual `/compact`: it closes a run without ending a turn, and
/// a goal loop hearing TurnEnd there would inject "continue" mid cleanup.
fn turn_end_reason(reason: DoneReason) -> Option<&'static str> {
    match reason {
        DoneReason::EndTurn => Some("finished"),
        DoneReason::MaxTokens => Some("max_tokens"),
        DoneReason::MaxTurns => Some("max_turns"),
        DoneReason::Compact => None,
    }
}

#[cfg(test)]
mod tests {
    use makima_agent::{
        AgentId, ToolDoneEvent, ToolOutput, ToolStartEvent, TurnId, TurnProvenance, TurnResult,
    };
    use makima_providers::TokenUsage;
    use test_case::test_case;

    use super::*;

    const TOOL_ID: &str = "t1";
    const TOOL_COMMAND: &str = "cargo test";
    const TOOL_OUTPUT: &str = "ok";
    const SUMMARY: &str = "the summary";
    const PROMPT: &str = "fix the build";

    fn tool_start() -> AgentEvent {
        AgentEvent::ToolStart(Box::new(ToolStartEvent {
            id: TOOL_ID.into(),
            tool: "bash".into(),
            summary: String::new(),
            render_header: None,
            annotation: None,
            input: None,
            raw_input: Some(json!({ "command": TOOL_COMMAND })),
            output: None,
        }))
    }

    fn tool_done(output: &str) -> AgentEvent {
        AgentEvent::ToolDone(Box::new(ToolDoneEvent {
            id: TOOL_ID.into(),
            tool: "bash".into(),
            output: ToolOutput::Plain(output.into()),
            is_error: false,
            annotation: None,
            written_path: None,
        }))
    }

    /// A plugin charging a budget off `ToolDone` needs the call itself, and
    /// only `ToolStart` ever carries it, so the pairing is what to pin.
    #[test]
    fn tool_done_carries_the_call_it_started() {
        let calls = ToolCalls::default();
        let agent = AgentId::generate();
        envelope_autocmd(&tool_start(), agent, None, &calls).unwrap();
        let (event, data) = envelope_autocmd(&tool_done(TOOL_OUTPUT), agent, None, &calls).unwrap();
        assert_eq!(event, "ToolDone");
        assert_eq!(data["input"]["command"], TOOL_COMMAND);
        assert_eq!(data["bytes"], TOOL_OUTPUT.len());
        assert_eq!(data["is_error"], false);
        assert!(data["duration_ms"].is_u64());
    }

    #[test]
    fn tool_done_without_a_start_carries_no_call() {
        let (_, data) = envelope_autocmd(
            &tool_done(TOOL_OUTPUT),
            AgentId::generate(),
            None,
            &ToolCalls::default(),
        )
        .unwrap();
        assert!(data.get("input").is_none() && data.get("duration_ms").is_none());
        assert_eq!(data["bytes"], TOOL_OUTPUT.len());
    }

    #[test]
    fn calls_with_the_same_id_are_scoped_to_the_agent() {
        let calls = ToolCalls::default();
        let first = AgentId::generate();
        let second = AgentId::generate();
        let mut other = tool_start();
        if let AgentEvent::ToolStart(start) = &mut other {
            start.raw_input = Some(json!({ "command": SUMMARY }));
        }
        envelope_autocmd(&tool_start(), first, None, &calls);
        envelope_autocmd(&other, second, None, &calls);
        let (_, first_done) =
            envelope_autocmd(&tool_done(TOOL_OUTPUT), first, None, &calls).unwrap();
        let (_, second_done) =
            envelope_autocmd(&tool_done(TOOL_OUTPUT), second, None, &calls).unwrap();
        assert_eq!(first_done["input"]["command"], TOOL_COMMAND);
        assert_eq!(second_done["input"]["command"], SUMMARY);
        assert!(first_done["duration_ms"].is_u64());
        assert!(second_done["duration_ms"].is_u64());
    }

    #[test]
    fn starting_another_call_does_not_expire_a_long_running_call() {
        let calls = ToolCalls::default();
        let agent = AgentId::generate();
        envelope_autocmd(&tool_start(), agent, None, &calls);
        calls
            .0
            .lock()
            .unwrap()
            .get_mut(&(agent, TOOL_ID.into()))
            .unwrap()
            .started -= std::time::Duration::from_secs(7200);
        calls.start(agent, SUMMARY, None, None);
        let (_, done) = envelope_autocmd(&tool_done(TOOL_OUTPUT), agent, None, &calls).unwrap();
        assert_eq!(done["input"]["command"], TOOL_COMMAND);
        assert!(done["duration_ms"].as_u64().unwrap() >= 7_200_000);
    }

    #[test]
    fn moved_calls_and_finished_turns_are_cleaned_up() {
        let calls = ToolCalls::default();
        let agent = AgentId::generate();
        let other = AgentId::generate();
        let turn = TurnId::generate();
        let next_turn = TurnId::generate();
        calls.start(agent, TOOL_ID, Some(turn), None);
        calls.start(agent, SUMMARY, Some(next_turn), None);
        calls.start(other, TOOL_ID, Some(turn), None);
        envelope_autocmd(
            &AgentEvent::ToolMoved {
                id: TOOL_ID.into(),
                tool: "bash".into(),
                summary: String::new(),
            },
            agent,
            Some(turn),
            &calls,
        );
        assert!(calls.finish(agent, TOOL_ID).is_none());
        calls.start(agent, TOOL_ID, Some(turn), None);
        let ActorEvent::End(mut result) = completed_turn(DoneReason::EndTurn) else {
            unreachable!();
        };
        if let TurnOutcome::Completed {
            agent_id, turn_id, ..
        } = &mut result.outcome
        {
            *agent_id = agent;
            *turn_id = turn;
        }
        envelope_autocmd(
            &AgentEvent::TurnOutcome(result.outcome),
            agent,
            Some(turn),
            &calls,
        );
        assert!(calls.finish(agent, TOOL_ID).is_none());
        assert!(calls.finish(agent, SUMMARY).is_some());
        assert!(calls.finish(other, TOOL_ID).is_some());
    }

    #[test]
    fn compaction_done_carries_the_summary() {
        let event = AgentEvent::CompactionDone {
            context_size_before: 100_000,
            context_size_after: 30_000,
            context_window: 200_000,
            summary: SUMMARY.into(),
        };
        let (name, data) =
            envelope_autocmd(&event, AgentId::generate(), None, &ToolCalls::default()).unwrap();
        assert_eq!(name, COMPACTION_DONE_EVENT);
        assert_eq!(data["summary"], SUMMARY);
        assert_eq!(data["context_size_after"], 30_000);
    }

    #[test]
    fn turn_start_carries_the_message_that_started_it() {
        let (name, data) = actor_autocmd(ActorEvent::Start {
            agent_id: AgentId::generate(),
            turn_id: TurnId::generate(),
            provenance: TurnProvenance::default(),
            config: None,
            text: PROMPT.into(),
        })
        .unwrap();
        assert_eq!(name, TURN_START_EVENT);
        assert_eq!(data["text"], PROMPT);
    }

    fn completed_turn(reason: DoneReason) -> ActorEvent {
        ActorEvent::End(TurnResult {
            outcome: TurnOutcome::Completed {
                agent_id: AgentId::generate(),
                turn_id: TurnId::generate(),
                usage: TokenUsage {
                    input: 1_000,
                    output: 500,
                    ..Default::default()
                },
                cost: Some(0.05),
                list_cost: Some(0.25),
                context_size: 12_000,
                context_window: 200_000,
                num_turns: 4,
                reason,
            },
            text: String::new(),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        })
    }

    #[test_case(DoneReason::EndTurn, Some("finished") ; "finished")]
    #[test_case(DoneReason::MaxTokens, Some("max_tokens") ; "max_tokens")]
    #[test_case(DoneReason::MaxTurns, Some("max_turns") ; "max_turns")]
    #[test_case(DoneReason::Compact, None ; "manual_compact_is_not_a_turn")]
    fn done_maps_to_a_turn_end_reason(reason: DoneReason, expected: Option<&str>) {
        let fired = actor_autocmd(completed_turn(reason));
        let got = fired
            .as_ref()
            .map(|(event, data)| (*event, data["reason"].as_str().unwrap_or_default()));
        assert_eq!(got, expected.map(|reason| (TURN_END_EVENT, reason)));
    }

    /// An idle `/compact` is no turn, so it reports through CompactionDone,
    /// failure included, as compaction inside a turn does on success.
    #[test_case(CompactionStatus::Completed, "completed" ; "completed")]
    #[test_case(CompactionStatus::Failed, "failed" ; "failed")]
    #[test_case(CompactionStatus::Cancelled, "cancelled" ; "cancelled")]
    fn idle_compaction_fires_compaction_done(status: CompactionStatus, expected: &str) {
        let (event, data) = actor_autocmd(ActorEvent::CompactionEnd(status)).unwrap();
        assert_eq!(event, COMPACTION_DONE_EVENT);
        assert_eq!(data["status"], expected);
    }

    /// Idle after a config-only commit settles no new work, so only the idle
    /// that follows new work fires AgentIdle.
    #[test]
    fn agent_idle_fires_once_per_settled_work() {
        let agent_id = AgentId::generate();
        let work = |presentation| {
            Some(ActorWorkIdentity::new(
                agent_id,
                makima_agent::ActorWorkKind::Control { operation_id: 0 },
                presentation,
            ))
        };
        let idle = |settled| ActorEvent::Idle { agent_id, settled };
        let mut filter = IdleFilter::default();
        let fired: Vec<_> = [idle(None), idle(work(1)), idle(work(1)), idle(work(2))]
            .iter()
            .map(|event| filter.passes(event))
            .collect();
        assert_eq!(fired, [false, true, false, true]);
    }

    /// A budget watchdog reads the whole bill off this one payload, so the keys
    /// it charges against have to survive refactors.
    #[test]
    fn turn_end_carries_the_turn_totals() {
        let (_, data) = actor_autocmd(completed_turn(DoneReason::EndTurn)).unwrap();
        assert_eq!(data["usage"]["input_tokens"], 1_000);
        assert_eq!(data["status"], "completed");
        assert_eq!(data["cost"], 0.05);
        assert_eq!(data["list_cost"], 0.25);
        assert_eq!(data["context_size"], 12_000);
        assert_eq!(data["context_window"], 200_000);
        assert_eq!(data["num_turns"], 4);
    }
}
