use crate::{ActorStatus, AgentId, AgentManagerHandle, GraphLifecycle};
use std::fmt::Write;

const NO_SUBAGENTS: &str = "No subagents.";

pub(crate) fn format_subagents(manager: &AgentManagerHandle, parent: AgentId) -> String {
    let mut output = String::new();
    for node in manager.descendant_snapshots(parent) {
        let status = if matches!(
            node.graph_lifecycle,
            GraphLifecycle::Closed | GraphLifecycle::Retired | GraphLifecycle::Removed
        ) {
            "done"
        } else if node.actor.as_ref().is_some_and(|actor| {
            matches!(actor.status, ActorStatus::Running(_)) || actor.queued != 0
        }) {
            "running"
        } else {
            "idle"
        };
        let _ = writeln!(
            output,
            "{}: {} ({status})",
            node.agent_id,
            node.metadata.label.as_deref().unwrap_or_default()
        );
    }
    if output.is_empty() {
        NO_SUBAGENTS.into()
    } else {
        output.pop();
        output
    }
}
