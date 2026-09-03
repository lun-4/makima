use std::sync::Arc;

use maki_agent::actor::AgentActorHandle;
use maki_agent::{CancelMap, CancelTrigger, TurnCancellationReason};

use super::AgentCommand;
use super::shared_queue::correlation;

/// Routes commands from the UI to the actor's cancellation APIs.
///
/// The actor owns all cancellation state; there is no coordinator or shadow
/// cancel map here. `AgentCommand::Cancel { run_id }` maps to the actor's
/// run_id-correlated cancel, which targets only that run's active/queued/
/// pre-admission work. `CancelAll` also cancels compatibility subagents,
/// matching the old single coordinator, and fires the startup cancel token so
/// an early CancelAll aborts MCP readiness.
pub(super) fn spawn_command_router(
    cmd_rx: flume::Receiver<AgentCommand>,
    actor: Arc<AgentActorHandle>,
    subagent_cancels: Arc<CancelMap<String>>,
    init_trigger: CancelTrigger,
) {
    // `CancelTrigger` is single-fire and not `Clone`: one startup trigger is
    // consumed by the first CancelAll (aborting MCP readiness) while later
    // CancelAll calls still cancel the actor and subagents.
    let mut init_trigger = Some(init_trigger);
    smol::spawn(async move {
        while let Ok(cmd) = cmd_rx.recv_async().await {
            match cmd {
                AgentCommand::Cancel { run_id } => {
                    actor.cancel_correlation(&correlation(run_id), TurnCancellationReason::User);
                }
                AgentCommand::CancelAll => {
                    if let Some(trigger) = init_trigger.take() {
                        trigger.cancel();
                    }
                    actor.cancel_all();
                    subagent_cancels.cancel_all();
                }
                AgentCommand::CancelSubagent { tool_use_id } => {
                    subagent_cancels.cancel_or_precancel(tool_use_id);
                }
            }
        }
    })
    .detach();
}
