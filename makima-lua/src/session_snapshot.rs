//! The one shape `makima.session.read()` returns. The UI and the headless
//! drivers all serialize this struct, so a plugin sees the same keys wherever
//! it runs and a new field is one edit instead of three `json!` blobs.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use makima_agent::{AgentEvent, AgentRef, Envelope};
use makima_providers::TokenUsage;
use makima_storage::id::MakimaId;
use makima_storage::sessions::StoredTokenUsage;
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
/// when unpriced.
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

/// Tracks headless session status and dispatches Lua autocmds.
#[derive(Clone)]
pub struct HeadlessObserver {
    handle: EventHandle,
    session_id: MakimaId,
    working: Arc<AtomicBool>,
    root: Arc<OnceLock<AgentRef>>,
}

impl HeadlessObserver {
    pub fn new(handle: EventHandle, session_id: MakimaId) -> Self {
        Self {
            handle,
            session_id,
            working: Arc::default(),
            root: Arc::default(),
        }
    }

    pub fn observe(&self, envelope: &Envelope) {
        self.working.store(
            !matches!(
                envelope.event,
                AgentEvent::TurnOutcome(_)
                    | AgentEvent::ControlComplete { .. }
                    | AgentEvent::ControlError { .. }
            ),
            Ordering::Relaxed,
        );
        if let Some(root) = self.root.get() {
            agent_autocmd::dispatch(&self.handle, root, envelope);
        }
    }

    /// Installs the provider for `makima.session.read`.
    pub fn install(&self, root: AgentRef) {
        let manager = root.manager();
        self.handle.observe_agents(&manager);
        let journal = manager.journal();
        let _ = self.root.set(root);
        let working = Arc::clone(&self.working);
        let id = self.session_id;
        self.handle.install_session_snapshot(Box::new(move |want| {
            // One session here, so any other id names a tab that is not ours.
            if want != id {
                return Err(format!("session {want} not live"));
            }
            let projection = journal.projection();
            let usage = projection.usage();
            serde_json::to_value(SessionSnapshot {
                id: id.to_string(),
                title: Some(projection.title().to_owned()),
                status: if working.load(Ordering::Relaxed) {
                    STATUS_WORKING
                } else {
                    STATUS_IDLE
                },
                updated_at: projection.updated_at,
                usage: session_usage(&usage.tokens, usage.cost, &usage.by_model),
            })
            .map_err(|e| e.to_string())
        }));
    }
}

#[cfg(test)]
mod tests {
    use makima_agent::{AgentId, DoneReason, TurnCompleteEvent, TurnId, TurnOutcome};
    use makima_providers::Message;

    use test_case::test_case;

    use super::{AgentEvent, Envelope, EventHandle, HeadlessObserver, MakimaId, Ordering};
    use super::{HashMap, StoredTokenUsage, TokenUsage, json, session_usage};

    #[test_case((); "consecutive_turns")]
    fn status_tracks_turn_boundaries(_: ()) {
        let observer =
            HeadlessObserver::new(EventHandle::disconnected_for_test(), MakimaId::generate());
        let observe = |event| {
            observer.observe(&Envelope {
                event,
                subagent: None,
                identity: None,
            })
        };
        for _ in 0..2 {
            observe(AgentEvent::TurnComplete(Box::new(TurnCompleteEvent {
                message: Message::user("go".into()),
                usage: TokenUsage::default(),
                model: String::new(),
                cost: None,
                context_size: None,
                context_window: 0,
            })));
            assert!(observer.working.load(Ordering::Relaxed));
            observe(AgentEvent::TurnOutcome(TurnOutcome::completed(
                AgentId::generate(),
                TurnId::generate(),
                TokenUsage::default(),
                0,
                DoneReason::EndTurn,
            )));
            assert!(!observer.working.load(Ordering::Relaxed));
        }
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
