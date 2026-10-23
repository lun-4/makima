//! The one shape `maki.session.read()` returns. The UI and the headless
//! drivers all serialize this struct, so a plugin sees the same keys wherever
//! it runs and a new field is one edit instead of three `json!` blobs.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use maki_agent::{AgentEvent, AgentRef, Envelope};
use maki_providers::{TokenUsage, add_cost};
use maki_storage::id::MakiId;
use maki_storage::sessions::StoredTokenUsage;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{EventHandle, agent_autocmd};

pub const STATUS_IDLE: &str = "idle";
pub const STATUS_WORKING: &str = "working";
pub const STATUS_NEEDS_INPUT: &str = "needs_input";
/// Agents only: a session that closed is no longer live.
pub const STATUS_CLOSED: &str = "closed";

#[derive(Serialize)]
pub struct SessionSnapshot {
    pub id: String,
    /// Left out when the host has no title to give. Never filled with the
    /// session id, so a plugin can tell "no title" from a real one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// [`STATUS_IDLE`], [`STATUS_WORKING`], or [`STATUS_NEEDS_INPUT`].
    pub status: &'static str,
    /// Unix seconds.
    pub updated_at: u64,
    /// From [`session_usage`]; subagent turns included, their tokens are real
    /// spend on this session too.
    pub usage: Value,
}

/// The `usage` of a session snapshot: `{total, models}`, models sorted by
/// total tokens descending, then spec. Costs are what each turn billed, `None`
/// when unpriced. A host that does not split usage by model passes none.
pub fn session_usage(
    total: &TokenUsage,
    cost: Option<f64>,
    models: &HashMap<String, StoredTokenUsage>,
) -> Value {
    let mut models: Vec<_> = models.iter().collect();
    models.sort_by(|(a_model, a_usage), (b_model, b_usage)| {
        b_usage
            .total()
            .cmp(&a_usage.total())
            .then_with(|| a_model.cmp(b_model))
    });
    let models: Vec<_> = models
        .into_iter()
        .map(|(model, usage)| {
            json!({
                "model": model,
                "input": usage.input,
                "output": usage.output,
                "cache_creation": usage.cache_creation,
                "cache_read": usage.cache_read,
                "cost": usage.cost,
            })
        })
        .collect();
    json!({
        "total": {
            "input": total.input,
            "output": total.output,
            "cache_creation": total.cache_creation,
            "cache_read": total.cache_read,
            "cost": cost,
        },
        "models": models,
    })
}

#[derive(Default)]
struct Totals {
    usage: TokenUsage,
    cost: Option<f64>,
    working: bool,
}

/// A poisoned lock only means some other run panicked mid-update. The
/// numbers are still whole, so read them instead of losing the session's
/// whole accounting over it.
fn lock(totals: &Mutex<Totals>) -> MutexGuard<'_, Totals> {
    totals.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a headless driver (print, SDK, ACP) feeds every envelope of its one
/// session to: the totals `maki.session.read` answers from and the Lua turn
/// autocmds, so plugins see the same events and numbers as under the TUI. The
/// driver writes while the Lua thread reads, hence the lock, though the two
/// rarely meet.
#[derive(Clone)]
pub struct HeadlessObserver {
    handle: EventHandle,
    session_id: MakiId,
    totals: Arc<Mutex<Totals>>,
    root: Arc<OnceLock<AgentRef>>,
}

impl HeadlessObserver {
    pub fn new(handle: EventHandle, session_id: MakiId) -> Self {
        Self {
            handle,
            session_id,
            totals: Arc::default(),
            root: Arc::default(),
        }
    }

    /// Folds one envelope into the totals, then fires its autocmd.
    pub fn observe(&self, envelope: &Envelope) {
        self.fold(envelope);
        if let Some(root) = self.root.get() {
            agent_autocmd::dispatch(&self.handle, root, envelope);
        }
    }

    fn fold(&self, envelope: &Envelope) {
        let event = &envelope.event;
        let mut totals = lock(&self.totals);
        // Envelopes only flow while a run is alive, so anything that is not
        // an ending means the agent is busy. Sdk mode runs many turns and no
        // driver has to remember to clear a flag between them.
        totals.working = !matches!(
            event,
            AgentEvent::TurnOutcome(_)
                | AgentEvent::ControlComplete { .. }
                | AgentEvent::ControlError { .. }
        );
        if let AgentEvent::TurnOutcome(outcome) = event {
            totals.usage += outcome.usage();
            add_cost(&mut totals.cost, outcome.cost());
        }
    }

    /// Installs the provider `maki.session.read` answers from. There is no
    /// rename path under the headless drivers, so `title` stays nil rather
    /// than echoing the id back as a fake one, and usage has no per-model
    /// split.
    pub fn install(&self, root: AgentRef) {
        self.handle.observe_agents(&root.manager());
        let _ = self.root.set(root);
        let totals = Arc::clone(&self.totals);
        let id = self.session_id;
        self.handle.install_session_snapshot(Box::new(move |want| {
            // One session here, so any other id names a tab that is not ours.
            if want != id {
                return Err(format!("session {want} not live"));
            }
            let totals = lock(&totals);
            serde_json::to_value(SessionSnapshot {
                id: id.to_string(),
                title: None,
                status: if totals.working {
                    STATUS_WORKING
                } else {
                    STATUS_IDLE
                },
                updated_at: maki_storage::now_epoch(),
                usage: session_usage(&totals.usage, totals.cost, &HashMap::new()),
            })
            .map_err(|e| e.to_string())
        }));
    }
}

#[cfg(test)]
mod tests {
    use maki_agent::{AgentId, DoneReason, TurnCompleteEvent, TurnId, TurnOutcome};
    use maki_providers::Message;

    use super::*;

    const FIRST_TURN_INPUT: u32 = 1_000;
    const SECOND_TURN_INPUT: u32 = 400;
    const TURN_COST: f64 = 0.25;
    const WINDOW: u32 = 200_000;
    const GROWS_MSG: &str = "session totals add up across turns, they never restart at one turn";
    const WORKING_MSG: &str = "a second turn has to read as working again";

    fn envelope(event: AgentEvent) -> Envelope {
        Envelope {
            event,
            subagent: None,
            identity: None,
        }
    }

    /// Carries no usage or cost of its own, so a fold that summed these instead
    /// of `Done` would leave the totals empty and fail below.
    fn turn_complete(context_size: u32) -> Envelope {
        envelope(AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
            message: Message::user("go".into()),
            usage: TokenUsage::default(),
            model: String::new(),
            cost: None,
            context_size: Some(context_size),
            context_window: WINDOW,
        })))
    }

    fn done(input: u32) -> Envelope {
        envelope(AgentEvent::TurnOutcome(TurnOutcome::Completed {
            agent_id: AgentId::generate(),
            turn_id: TurnId::generate(),
            usage: TokenUsage {
                input,
                ..Default::default()
            },
            cost: Some(TURN_COST),
            list_cost: None,
            context_size: input,
            context_window: WINDOW,
            num_turns: 1,
            reason: DoneReason::EndTurn,
        }))
    }

    /// Sdk mode serves many turns on one session, so `Done` is a turn boundary
    /// and not the end. Totals have to keep climbing and the status has to come
    /// back to life on turn two, with no driver clearing anything by hand.
    #[test]
    fn totals_grow_and_status_revives_across_turns() {
        let observer =
            HeadlessObserver::new(EventHandle::disconnected_for_test(), MakiId::generate());

        observer.fold(&turn_complete(FIRST_TURN_INPUT));
        assert!(lock(&observer.totals).working);
        observer.fold(&done(FIRST_TURN_INPUT));
        assert!(!lock(&observer.totals).working);

        observer.fold(&turn_complete(SECOND_TURN_INPUT));
        assert!(lock(&observer.totals).working, "{WORKING_MSG}");
        observer.fold(&done(SECOND_TURN_INPUT));

        let totals = lock(&observer.totals);
        assert!(!totals.working);
        assert_eq!(
            totals.usage.input,
            FIRST_TURN_INPUT + SECOND_TURN_INPUT,
            "{GROWS_MSG}"
        );
        assert_eq!(totals.cost, Some(TURN_COST * 2.0), "{GROWS_MSG}");
    }

    #[test]
    fn session_usage_preserves_costs_and_sorts_models_deterministically() {
        let total = TokenUsage {
            input: 10,
            output: 20,
            cache_creation: 30,
            cache_read: 40,
        };
        let usage_by_model = HashMap::from([
            (
                "z/model".to_owned(),
                StoredTokenUsage {
                    input: 5,
                    output: 5,
                    cache_creation: 0,
                    cache_read: 0,
                    cost: None,
                },
            ),
            (
                "b/model".to_owned(),
                StoredTokenUsage {
                    input: 14,
                    output: 1,
                    cache_creation: 2,
                    cache_read: 3,
                    cost: Some(0.25),
                },
            ),
            (
                "a/model".to_owned(),
                StoredTokenUsage {
                    input: 10,
                    output: 0,
                    cache_creation: 0,
                    cache_read: 0,
                    cost: Some(0.5),
                },
            ),
        ]);

        assert_eq!(
            session_usage(&total, None, &usage_by_model),
            json!({
                "total": {
                    "input": 10,
                    "output": 20,
                    "cache_creation": 30,
                    "cache_read": 40,
                    "cost": null,
                },
                "models": [
                    {
                        "model": "b/model",
                        "input": 14,
                        "output": 1,
                        "cache_creation": 2,
                        "cache_read": 3,
                        "cost": 0.25,
                    },
                    {
                        "model": "a/model",
                        "input": 10,
                        "output": 0,
                        "cache_creation": 0,
                        "cache_read": 0,
                        "cost": 0.5,
                    },
                    {
                        "model": "z/model",
                        "input": 5,
                        "output": 5,
                        "cache_creation": 0,
                        "cache_read": 0,
                        "cost": null,
                    },
                ],
            })
        );
    }
}
