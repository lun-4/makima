//! One place that turns agent events into autocmds. Lifecycle events come
//! from the graph itself ([`observe`]), tool and compaction events from the
//! envelopes a frontend reads ([`dispatch`]), so a plugin sees the same events
//! for every agent, children included, whichever way maki was started.

use std::collections::HashMap;

use maki_agent::{
    ActorEvent, ActorWorkIdentity, AgentEvent, AgentId, AgentManagerHandle, AgentRef,
    CompactionStatus, DoneReason, Envelope, TurnOutcome,
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

/// Fires the autocmd for one envelope a frontend read from {root}'s graph.
pub fn dispatch(handle: &EventHandle, root: &AgentRef, envelope: &Envelope) {
    let Some((event, data)) = envelope_autocmd(&envelope.event) else {
        return;
    };
    let agent = match &envelope.subagent {
        Some(subagent) => root.manager().reference(subagent.agent_id),
        None => root.clone(),
    };
    handle.fire_agent_autocmd(event, agent, data);
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

fn envelope_autocmd(event: &AgentEvent) -> Option<(&'static str, Value)> {
    match event {
        AgentEvent::ToolStart(e) => Some(("ToolStart", json!({ "tool_id": e.id, "tool": e.tool }))),
        AgentEvent::ToolDone(e) => Some(("ToolDone", json!({ "tool_id": e.id, "tool": e.tool }))),
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
        } => Some((
            COMPACTION_DONE_EVENT,
            json!({
                "status": compaction_status(CompactionStatus::Completed),
                "context_size_before": context_size_before,
                "context_size_after": context_size_after,
                "context_window": context_window,
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
            ..
        } => Some((
            TURN_START_EVENT,
            json!({
                "turn_id": turn_id.to_string(),
                "origin": origin_json(&provenance),
                "selection": config.map(|commit| selection_json(&commit.config)),
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
    use maki_agent::{AgentId, TurnId, TurnProvenance, TurnResult};
    use maki_providers::TokenUsage;
    use test_case::test_case;

    use super::*;

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
                maki_agent::ActorWorkKind::Control { operation_id: 0 },
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
