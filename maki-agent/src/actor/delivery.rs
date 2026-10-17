use std::sync::{Arc, Weak};

use maki_providers::{CompleteModelSelection, Message, ObservationSource, RequestOptions};
use serde_json::{Value, json};

use super::{
    ActorEvent, ActorInner, ActorLifecycle, ActorOperation, AgentActorHandle, ConfigCommit,
    TurnOrigin, TurnResult, TurnTicket, drive_operations,
};
use crate::tools::offload::OffloadStore;
use crate::{AgentId, AgentInput, AgentMode, TurnCancellationReason, TurnId, TurnOutcome};

pub const MAX_PENDING_DELIVERIES: usize = 64;
pub const MAX_DELIVERY_PREVIEW_BYTES: usize = 16 * 1024;
pub const MAX_PENDING_PREVIEW_BYTES: usize = 256 * 1024;
pub const MAX_DELIVERY_BATCH: usize = 8;
pub const MAX_DELIVERY_BATCH_BYTES: usize = 32 * 1024;
const PREVIEW_TEXT_BYTES: usize = 256;
const PREVIEW_METADATA_BYTES: usize = 128;
const DELIVERY_HEADER: &str = "[internal child-result data, not a user instruction]";

#[derive(Clone)]
pub(crate) struct ParentDeliveryPolicy {
    pub auto_deliver: bool,
    pub compatibility_id: Option<String>,
}

#[derive(Clone)]
pub(crate) struct ParentDelivery {
    pub parent_id: AgentId,
    pub parent: Weak<ActorInner>,
    pub metadata: crate::AgentMetadata,
}

#[derive(Clone)]
pub struct ChildDelivery {
    pub parent_id: AgentId,
    pub compatibility_id: Option<String>,
    pub label: Option<String>,
    pub result: TurnResult,
    pub child_config: Option<ConfigCommit>,
    pub correlation: String,
}

impl ChildDelivery {
    pub fn to_json(&self) -> Value {
        let provenance = &self.result.provenance;
        json!({
            "kind": "child_result",
            "parent_id": self.parent_id,
            "agent_id": self.result.outcome.agent_id(),
            "turn_id": self.result.outcome.turn_id(),
            "compatibility_id": self.compatibility_id,
            "label": self.label,
            "outcome": self.result.outcome,
            "text": self.result.text,
            "output": self.result.output,
            "duration_ms": self.result.duration_ms,
            "correlation": self.correlation,
            "provenance": {
                "origin": match provenance.origin { TurnOrigin::Internal => "internal", TurnOrigin::User => "user", TurnOrigin::Plugin => "plugin" },
                "plugin": provenance.plugin,
                "plugin_generation": provenance.plugin_generation,
                "source_agent": provenance.source_agent,
                "source_turn": provenance.source_turn,
            },
            "configuration": self.child_config.as_ref().map(|commit| json!({
                "generation": commit.generation,
                "selection": CompleteModelSelection::from_model(&commit.config.model, RequestOptions {
                    thinking: commit.config.thinking,
                    fast: commit.config.fast,
                }),
                "mode": commit.config.mode.id().key(),
                "workflow": commit.config.workflow,
            })),
        })
    }

    fn preview(&self, store: Option<&OffloadStore>) -> Self {
        let serialized = self.to_json().to_string();
        if serialized.len() + DELIVERY_HEADER.len() < MAX_DELIVERY_PREVIEW_BYTES {
            return self.clone();
        }
        let mut preview = self.clone();
        {
            preview.result.text = bounded(&self.result.text, PREVIEW_TEXT_BYTES);
            let mut reference = json!({
                "truncated": true,
                "full_result": {
                    "agent_id": self.result.outcome.agent_id(),
                    "turn_id": self.result.outcome.turn_id(),
                    "lookup": "retained turn result",
                },
            });
            if let Some(store) = store {
                match store.put(&serialized) {
                    Ok(saved) => {
                        let path = store.path_of(&saved);
                        if path.as_os_str().len() <= PREVIEW_TEXT_BYTES {
                            reference["offload"] = json!({
                                "path": path,
                                "original_bytes": saved.original_bytes,
                                "saved_bytes": saved.saved_bytes,
                                "complete": saved.original_bytes == saved.saved_bytes,
                                "lookup": "read or grep",
                            });
                            if saved.saved_bytes < saved.original_bytes {
                                reference["diagnostic"] = json!(
                                    "offload file capped; use retained result lookup for the full result"
                                );
                            }
                        } else {
                            reference["diagnostic"] = json!(
                                "offload path exceeds preview budget; use retained result lookup"
                            );
                        }
                    }
                    Err(error) => {
                        reference["diagnostic"] = json!(format!(
                            "file offload failed: {}; use retained result lookup",
                            bounded(&error.to_string(), PREVIEW_METADATA_BYTES),
                        ));
                    }
                }
            } else {
                reference["diagnostic"] = json!(
                    "file offload unavailable: no session offload store; use retained result lookup"
                );
            }
            preview.result.output = vec![reference];
            if let TurnOutcome::Failed { failure, .. } = &mut preview.result.outcome {
                failure.diagnostic = bounded(&failure.diagnostic, PREVIEW_TEXT_BYTES);
                failure.user_message = bounded(&failure.user_message, PREVIEW_TEXT_BYTES);
            }
        }
        preview.label = preview
            .label
            .map(|text| bounded(&text, PREVIEW_METADATA_BYTES));
        preview.compatibility_id = preview
            .compatibility_id
            .map(|text| bounded(&text, PREVIEW_METADATA_BYTES));
        preview.correlation = bounded(&preview.correlation, PREVIEW_METADATA_BYTES);
        preview.result.provenance.plugin = preview
            .result
            .provenance
            .plugin
            .map(|text| bounded(&text, PREVIEW_METADATA_BYTES));
        preview
    }

    fn input(&self) -> AgentInput {
        let mut envelope = self.to_json();
        if envelope.to_string().len() + DELIVERY_HEADER.len() + 1 > MAX_DELIVERY_PREVIEW_BYTES
            && let Some(configuration) = envelope.get_mut("configuration")
        {
            bound_json_strings(configuration);
        }
        AgentInput {
            message: String::new(),
            mode: AgentMode::Build,
            images: Vec::new(),
            preamble: vec![Message {
                observation_source: Some(ObservationSource::ChildResult {
                    agent_id: self.result.outcome.agent_id().to_string(),
                    turn_id: self.result.outcome.turn_id().to_string(),
                }),
                ..Message::observation(format!("{DELIVERY_HEADER}\n{}", without_nulls(envelope)))
            }],
            thinking: Default::default(),
            fast: false,
            workflow: false,
            prompt: None,
            cancel: None,
            lease_committer: None,
        }
    }
}

fn bounded(text: &str, limit: usize) -> String {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

fn bound_json_strings(value: &mut Value) {
    match value {
        Value::String(text) => *text = bounded(text, PREVIEW_METADATA_BYTES),
        Value::Object(fields) => fields.values_mut().for_each(bound_json_strings),
        _ => {}
    }
}

/// Absent and null read the same to the model, so the rendered envelope
/// drops null fields rather than spending tokens on them.
fn without_nulls(value: Value) -> Value {
    match value {
        Value::Object(fields) => fields
            .into_iter()
            .filter(|(_, field)| !field.is_null())
            .map(|(key, field)| (key, without_nulls(field)))
            .collect(),
        other => other,
    }
}

/// One line for the user describing a child-result delivery input, or `None`
/// when the input carries no delivery.
pub fn delivery_summary(input: &AgentInput) -> Option<String> {
    let body = input
        .preamble
        .iter()
        .find(|message| {
            matches!(
                message.observation_source,
                Some(ObservationSource::ChildResult { .. })
            )
        })?
        .first_text_content()?
        .strip_prefix(DELIVERY_HEADER)?;
    let delivery: Value = serde_json::from_str(body.trim()).ok()?;
    let source = delivery["label"]
        .as_str()
        .or_else(|| delivery["agent_id"].as_str())?;
    let status = delivery["outcome"]["status"].as_str().unwrap_or_default();
    let text = delivery["text"].as_str().unwrap_or_default();
    Some(format!("[{status} result from {source}] {text}"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRejection {
    ParentUnavailable,
    ParentClosed,
    Overflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStatus {
    Accepted,
    Rejected(DeliveryRejection),
    Discarded(TurnCancellationReason),
    Executed { parent_turn_id: TurnId },
}

#[derive(Clone)]
pub struct DeliveryEvent {
    pub delivery: Arc<ChildDelivery>,
    pub status: DeliveryStatus,
}

pub(crate) struct PendingDelivery {
    pub delivery: Arc<ChildDelivery>,
    pub message: Message,
    pub bytes: usize,
}

pub(super) fn admit_delivery(
    inner: &Arc<ActorInner>,
    delivery: Arc<ChildDelivery>,
) -> Result<(), DeliveryRejection> {
    let delivery = Arc::new(delivery.preview(inner.offload.as_deref()));
    let message = delivery.input().preamble.remove(0);
    let bytes = message.first_text_content().map_or(0, str::len);
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if state.lifecycle != ActorLifecycle::Open {
        return Err(DeliveryRejection::ParentClosed);
    }
    let id = delivery.result.outcome.turn_id();
    if state.deliveries.contains_key(&id) || state.delivery_seen.contains(&id) {
        return Ok(());
    }
    if bytes > MAX_DELIVERY_PREVIEW_BYTES
        || state.deliveries.len() >= MAX_PENDING_DELIVERIES
        || state
            .deliveries
            .values()
            .map(|pending| pending.bytes)
            .sum::<usize>()
            + bytes
            > MAX_PENDING_PREVIEW_BYTES
    {
        return Err(DeliveryRejection::Overflow);
    }
    state.delivery_seen.push_back(id);
    if state.delivery_seen.len() > super::MAX_RETAINED_TURN_RESULTS {
        state.delivery_seen.pop_front();
    }
    state.delivery_order.push_back(id);
    state.deliveries.insert(
        id,
        PendingDelivery {
            delivery: Arc::clone(&delivery),
            message,
            bytes,
        },
    );
    state.emit(ActorEvent::Delivery(DeliveryEvent {
        delivery,
        status: DeliveryStatus::Accepted,
    }));
    inner.queue.notify();
    Ok(())
}

pub(super) fn record(
    inner: &ActorInner,
    turn_id: TurnId,
    generation: u64,
    policy: Option<Arc<super::EffectiveAgentConfig>>,
    correlation: String,
) {
    if inner
        .managed_admission
        .as_ref()
        .is_some_and(|managed| managed.parent_delivery.is_some())
    {
        let config = policy.map(|config| ConfigCommit {
            identity: Arc::clone(&inner.identity),
            generation,
            config,
        });
        inner
            .delivery_records
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(turn_id, (config, correlation));
    }
}

pub(super) fn publish(
    inner: &ActorInner,
    ticket: &TurnTicket,
    admission: Option<&super::TurnAdmission>,
) {
    let Some(target) = inner
        .managed_admission
        .as_ref()
        .and_then(|managed| managed.parent_delivery.as_ref())
    else {
        return;
    };
    let recorded = inner
        .delivery_records
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&ticket.turn_id());
    // Closing a child is the owner's own decision, so the turns it cuts short
    // carry nothing the parent does not already know.
    if matches!(
        ticket.peek_result().map(|result| result.outcome),
        Some(TurnOutcome::Cancelled {
            reason: TurnCancellationReason::Closed | TurnCancellationReason::Shutdown,
            ..
        })
    ) {
        return;
    }
    let policy = {
        let state = inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.delivery_runs.contains_key(&ticket.turn_id()) {
            return;
        }
        state.parent_delivery_policy.clone()
    };
    let Some(policy) = policy.filter(|policy| policy.auto_deliver) else {
        return;
    };
    let (child_config, correlation) = recorded.unwrap_or_else(|| {
        let config = admission.and_then(|admission| {
            admission.policy.clone().map(|config| ConfigCommit {
                identity: Arc::clone(&inner.identity),
                generation: admission.generation,
                config,
            })
        });
        (
            config,
            admission
                .map(|admission| admission.correlation.clone())
                .unwrap_or_default(),
        )
    });
    let Some(result) = ticket.peek_result() else {
        return;
    };
    let delivery = Arc::new(
        ChildDelivery {
            parent_id: target.parent_id,
            compatibility_id: policy.compatibility_id,
            label: target.metadata.label.clone(),
            result,
            child_config,
            correlation,
        }
        .preview(inner.offload.as_deref()),
    );
    let result = target
        .parent
        .upgrade()
        .ok_or(DeliveryRejection::ParentUnavailable)
        .and_then(|parent| admit_delivery(&parent, Arc::clone(&delivery)));
    if let Err(reason) = result {
        inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .emit(ActorEvent::Delivery(DeliveryEvent {
                delivery,
                status: DeliveryStatus::Rejected(reason),
            }));
    }
}

impl AgentActorHandle {
    pub fn withdraw_child_delivery(&self, child_turn: TurnId) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(pending) = state.deliveries.remove(&child_turn) else {
            return false;
        };
        state.delivery_order.retain(|id| *id != child_turn);
        state.emit(ActorEvent::Delivery(DeliveryEvent {
            delivery: pending.delivery,
            status: DeliveryStatus::Discarded(TurnCancellationReason::Withdrawn),
        }));
        true
    }
}

pub(super) fn discard(state: &mut super::ActorState, reason: TurnCancellationReason) {
    state.delivery_order.clear();
    let pending: Vec<_> = state
        .deliveries
        .drain()
        .map(|(_, pending)| pending.delivery)
        .collect();
    for delivery in pending {
        state.emit(ActorEvent::Delivery(DeliveryEvent {
            delivery,
            status: DeliveryStatus::Discarded(reason),
        }));
    }
}

pub(super) fn settle(inner: &ActorInner, turn_id: TurnId, reason: Option<TurnCancellationReason>) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if let Some(batch) = state.delivery_runs.remove(&turn_id)
        && batch.iter().any(|id| state.deliveries.contains_key(id))
    {
        state.delivery_suspended = true;
        state.emit(ActorEvent::DeliveryContinuationFailed {
            parent_turn_id: turn_id,
            reason,
        });
    }
}

pub(super) fn drain(inner: &Arc<ActorInner>, turn_id: TurnId) -> Vec<Message> {
    let mut messages = Vec::new();
    commit(inner, turn_id, &mut |message| messages.push(message));
    messages
}

pub(super) fn commit(inner: &Arc<ActorInner>, turn_id: TurnId, commit: &mut dyn FnMut(Message)) {
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if state.lifecycle != ActorLifecycle::Open
        || state
            .processing
            .as_ref()
            .is_some_and(|work| work.cancellation_reason.is_some())
    {
        return;
    }
    let eligible = state.delivery_runs.get(&turn_id).cloned();
    if eligible.is_none()
        && state
            .instructions
            .get(&turn_id)
            .is_none_or(|inbox| !inbox.open)
    {
        return;
    }
    let mut committed = 0;
    let mut bytes = 0;
    while let Some(id) = state.delivery_order.front().copied() {
        if committed == MAX_DELIVERY_BATCH {
            break;
        }
        if eligible.as_ref().is_some_and(|batch| !batch.contains(&id)) {
            break;
        }
        let Some(pending) = state.deliveries.get(&id) else {
            state.delivery_order.pop_front();
            continue;
        };
        if bytes + pending.bytes > MAX_DELIVERY_BATCH_BYTES {
            break;
        }
        let pending = state
            .deliveries
            .remove(&id)
            .expect("head checked under actor lock");
        state.delivery_order.pop_front();
        bytes += pending.bytes;
        commit(pending.message);
        committed += 1;
        state.emit(ActorEvent::Delivery(DeliveryEvent {
            delivery: pending.delivery,
            status: DeliveryStatus::Executed {
                parent_turn_id: turn_id,
            },
        }));
    }
}

/// Reserve only after explicit FIFO work has drained. Records are not claimed
/// until the model loop inserts them into history at a safe request boundary.
pub(super) fn schedule(inner: &Arc<ActorInner>, state: &mut super::ActorState) {
    if state.lifecycle != ActorLifecycle::Open
        || state.delivery_suspended
        || state.deliveries.is_empty()
        || !state.delivery_runs.is_empty()
        || state.has_pending_work(&inner.queue)
        || !state.operations.is_empty()
    {
        return;
    }
    let turn_id = TurnId::generate();
    let ticket = TurnTicket::new(turn_id, Arc::clone(&inner.identity));
    inner.admit_ticket(turn_id, ticket.clone());
    let mut bytes = 0;
    let batch: Vec<_> = state
        .delivery_order
        .iter()
        .copied()
        .take(MAX_DELIVERY_BATCH)
        .take_while(|id| {
            bytes += state.deliveries.get(id).map_or(0, |pending| pending.bytes);
            bytes <= MAX_DELIVERY_BATCH_BYTES
        })
        .collect();
    state.delivery_runs.insert(turn_id, batch);
    state.next_operation_id = state.next_operation_id.wrapping_add(1);
    let id = state.next_operation_id;
    state.operations.push_back(ActorOperation::Turn {
        id,
        input: AgentInput::from_defaults(
            String::new(),
            AgentMode::Build,
            Vec::new(),
            Default::default(),
        ),
        event_sender: None,
        correlation: "child-results".into(),
        ticket,
        policy_constraint: None,
    });
    drive_operations(inner, state);
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    use maki_providers::TokenUsage;
    use test_case::test_case;

    use super::{
        ChildDelivery, DeliveryRejection, DeliveryStatus, MAX_PENDING_DELIVERIES, admit_delivery,
    };
    use crate::actor::{
        ActorBackend, ActorEvent, BackendResult, ControlWork, TurnContext, TurnProvenance,
        TurnResult, WorkKind,
    };
    use crate::{
        AgentActorHandle, AgentId, AgentInput, History, TurnCancellationReason, TurnId, TurnOutcome,
    };

    const CHILD_TEXT: &str = "child result";
    const LABEL: &str = "research";
    const CORRELATION: &str = "child correlation";

    struct UnusedBackend;

    impl ActorBackend for UnusedBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
    }

    fn delivery(parent_id: AgentId) -> Arc<ChildDelivery> {
        Arc::new(ChildDelivery {
            parent_id,
            compatibility_id: None,
            label: None,
            child_config: None,
            correlation: CORRELATION.into(),
            result: TurnResult {
                outcome: TurnOutcome::cancelled(
                    AgentId::generate(),
                    TurnId::generate(),
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text: CHILD_TEXT.into(),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            },
        })
    }

    #[test_case(false; "saved")]
    #[test_case(true; "closed_store")]
    fn oversized_completion_offload_preserves_envelope(closed: bool) {
        let directory = tempfile::tempdir().unwrap();
        let store = crate::tools::offload::OffloadStore::on_disk(directory.path().to_path_buf());
        if closed {
            store.request_close();
        }
        let mut envelope = (*delivery(AgentId::generate())).clone();
        envelope.result.text = "\u{0001}".repeat(super::MAX_DELIVERY_PREVIEW_BYTES);
        envelope.result.provenance.plugin = Some(LABEL.into());
        envelope.label = Some(LABEL.into());
        envelope.child_config = Some(super::ConfigCommit {
            identity: Arc::new(()),
            generation: 3,
            config: Arc::new(crate::EffectiveAgentConfig::new(
                super::super::tests::policy(false),
                crate::AgentMode::Build,
            )),
        });
        let canonical = envelope.to_json();
        let preview = envelope.preview(Some(&store));
        let rendered = preview.input().preamble.remove(0);
        assert!(rendered.first_text_content().unwrap().len() <= super::MAX_DELIVERY_PREVIEW_BYTES);
        let value = preview.to_json();
        for field in [
            "configuration",
            "provenance",
            "outcome",
            "correlation",
            "label",
        ] {
            assert_eq!(value[field], canonical[field]);
        }
        let reference = &value["output"][0];
        assert_eq!(reference["truncated"], true);
        assert_eq!(reference["full_result"]["turn_id"], canonical["turn_id"]);
        if closed {
            const DIAGNOSTIC: &str = "file offload failed: the session's offload store is closed; use retained result lookup";
            assert_eq!(reference["diagnostic"], DIAGNOSTIC);
            assert!(reference.get("offload").is_none());
        } else {
            let body =
                std::fs::read_to_string(reference["offload"]["path"].as_str().unwrap()).unwrap();
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&body).unwrap(),
                canonical
            );
            assert_eq!(reference["offload"]["complete"], true);
            assert_eq!(preview.preview(Some(&store)).to_json(), value);
        }
    }

    #[test]
    fn delivery_summary_names_source_status_and_text() {
        let mut envelope = (*delivery(AgentId::generate())).clone();
        let agent_id = envelope.result.outcome.agent_id();
        assert_eq!(
            super::delivery_summary(&envelope.input()),
            Some(format!("[cancelled result from {agent_id}] {CHILD_TEXT}"))
        );
        envelope.label = Some(LABEL.into());
        assert_eq!(
            super::delivery_summary(&envelope.input()),
            Some(format!("[cancelled result from {LABEL}] {CHILD_TEXT}"))
        );
        assert_eq!(
            super::delivery_summary(&AgentInput {
                preamble: Vec::new(),
                ..envelope.input()
            }),
            None
        );
    }

    #[test]
    fn envelope_configuration_uses_normalized_selection_and_mode_key() {
        let mut envelope = (*delivery(AgentId::generate())).clone();
        let config = super::super::tests::policy(false);
        let expected_spec = config.model.spec();
        envelope.child_config = Some(super::ConfigCommit {
            identity: Arc::new(()),
            generation: 3,
            config: Arc::new(crate::EffectiveAgentConfig::new(
                config,
                crate::AgentMode::Build,
            )),
        });
        let configuration = &envelope.to_json()["configuration"];
        assert_eq!(configuration["generation"], 3);
        assert_eq!(configuration["mode"], "build");
        assert_eq!(configuration["selection"]["spec"], expected_spec);
        assert_eq!(configuration["selection"]["thinking"], "off");
        assert_eq!(configuration["selection"]["fast"], false);
        assert!(configuration.get("provider").is_none());
        assert!(configuration.get("thinking").is_none());
    }

    #[test]
    fn clearing_user_work_keeps_child_deliveries() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let user = actor
                .admit_turn(
                    AgentInput::from_defaults(
                        String::new(),
                        crate::AgentMode::Build,
                        Vec::new(),
                        Default::default(),
                    ),
                    None,
                    CORRELATION.into(),
                )
                .unwrap();
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            actor.clear_user_work();
            assert!(matches!(user.wait().await, TurnOutcome::Cancelled { .. }));
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
            actor.close();
            task.await;
        });
    }

    #[test]
    fn completion_admission_does_not_allocate_ticket() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            assert!(actor.latest_ticket().is_none());
            let user = actor
                .admit_turn(
                    AgentInput::from_defaults(
                        String::new(),
                        crate::AgentMode::Build,
                        Vec::new(),
                        Default::default(),
                    ),
                    None,
                    CORRELATION.into(),
                )
                .unwrap();
            assert_eq!(actor.latest_ticket().unwrap().turn_id(), user.turn_id());
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            assert_eq!(actor.latest_ticket().unwrap().turn_id(), user.turn_id());
            actor.close();
            task.await;
        });
    }

    #[test]
    fn withdrawn_delivery_settles_once_without_running() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let events = actor.subscribe_events();
            let envelope = delivery(actor.agent_id());
            admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            let child_turn = envelope.result.outcome.turn_id();
            assert!(!actor.withdraw_child_delivery(TurnId::generate()));
            assert!(actor.withdraw_child_delivery(child_turn));
            assert!(!actor.withdraw_child_delivery(child_turn));
            let statuses: Vec<_> = events
                .try_iter()
                .filter_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event.status),
                    _ => None,
                })
                .collect();
            assert_eq!(
                statuses,
                vec![
                    DeliveryStatus::Accepted,
                    DeliveryStatus::Discarded(TurnCancellationReason::Withdrawn),
                ]
            );
            assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
            actor.close();
            task.await;
        });
    }

    #[test_case(false; "clear")]
    #[test_case(true; "close")]
    fn queued_delivery_discards_once(close: bool) {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let events = actor.subscribe_events();
            let envelope = delivery(actor.agent_id());
            admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            if close {
                actor.close();
            } else {
                actor.clear();
            }
            let statuses: Vec<_> = events
                .try_iter()
                .filter_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event.status),
                    _ => None,
                })
                .collect();
            assert_eq!(
                statuses,
                vec![
                    DeliveryStatus::Accepted,
                    DeliveryStatus::Discarded(if close {
                        TurnCancellationReason::Closed
                    } else {
                        TurnCancellationReason::User
                    })
                ]
            );
            assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
            actor.close();
            task.await;
        });
    }

    #[derive(Clone, Copy)]
    enum Discard {
        Clear,
        Close,
        Withdraw,
    }

    #[test_case(Discard::Clear; "clear")]
    #[test_case(Discard::Close; "close")]
    #[test_case(Discard::Withdraw; "withdraw")]
    fn discarded_notification_does_not_notify_ancestor(discard: Discard) {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (ancestor, ancestor_task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let mut managed = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            managed.parent_delivery = Some(super::ParentDelivery {
                parent_id: ancestor.agent_id(),
                parent: Arc::downgrade(&ancestor.inner),
                metadata: Default::default(),
            });
            let (_start, rx) = flume::bounded(1);
            let (parent, parent_task) = AgentActorHandle::spawn_inner(
                managed.agent_id,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                Some(managed),
                None,
                Some(rx),
            );
            parent.configure_parent_delivery(true, None).unwrap();
            let ancestor_events = ancestor.subscribe_events();
            let envelope = delivery(parent.agent_id());
            admit_delivery(&parent.inner, Arc::clone(&envelope)).unwrap();
            match discard {
                Discard::Clear => {
                    parent.clear();
                }
                Discard::Close => parent.close(),
                Discard::Withdraw => {
                    assert!(parent.withdraw_child_delivery(envelope.result.outcome.turn_id()));
                }
            }
            assert!(
                ancestor_events
                    .try_iter()
                    .all(|event| !matches!(event, ActorEvent::Delivery(_)))
            );
            assert_eq!(ancestor.snapshot().queued, 0);
            assert!(ancestor.inner.tickets.lock().unwrap().is_empty());
            parent.close();
            ancestor.close();
            parent_task.await;
            ancestor_task.await;
        });
    }

    #[test]
    fn deferred_cancel_preserves_admission_configuration_and_correlation() {
        smol::block_on(async {
            const CORRELATION: &str = "cancelled before materialization";
            let (_start, rx) = flume::bounded(1);
            let (parent, parent_task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let mut managed = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            managed.parent_delivery = Some(super::ParentDelivery {
                parent_id: parent.agent_id(),
                parent: Arc::downgrade(&parent.inner),
                metadata: Default::default(),
            });
            let (_start, rx) = flume::bounded(1);
            let config = crate::EffectiveAgentConfig::new(
                super::super::tests::policy(false),
                crate::AgentMode::Build,
            );
            let (child, child_task) = AgentActorHandle::spawn_inner(
                managed.agent_id,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                Some(managed),
                Some(Arc::new(config)),
                Some(rx),
            );
            child.configure_parent_delivery(true, None).unwrap();
            let barrier = child.reserve_config_update().unwrap();
            let events = parent.subscribe_events();
            let ticket = child
                .admit_turn(
                    delivery(parent.agent_id()).input(),
                    None,
                    CORRELATION.into(),
                )
                .unwrap();
            child.cancel_turn(ticket.turn_id()).unwrap();
            let event = events
                .try_iter()
                .find_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event),
                    _ => None,
                })
                .unwrap();
            assert_eq!(event.delivery.correlation, CORRELATION);
            let config = event.delivery.child_config.as_ref().unwrap();
            assert_eq!(config.generation, 0);
            assert_eq!(config.config.mode, crate::AgentMode::Build);
            assert!(child.inner.delivery_records.lock().unwrap().is_empty());
            drop(barrier);
            child.close();
            parent.close();
            child_task.await;
            parent_task.await;
        });
    }

    #[test_case(true; "enabled")]
    #[test_case(false; "disabled")]
    fn closed_parent_rejection_uses_creation_policy(auto_deliver: bool) {
        smol::block_on(async {
            const COMPATIBILITY_ID: &str = "explicit compatibility";
            const TOOL_ID: &str = "tool metadata is not routing identity";
            let (_parent_start, parent_rx) = flume::bounded(1);
            let (parent, parent_task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(parent_rx),
            );
            let mut admission = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            admission.parent_delivery = Some(super::ParentDelivery {
                parent_id: parent.agent_id(),
                parent: Arc::downgrade(&parent.inner),
                metadata: crate::AgentMetadata {
                    spawned_by_tool_use_id: Some(TOOL_ID.into()),
                    ..Default::default()
                },
            });
            let (_child_start, child_rx) = flume::bounded(1);
            let (child, child_task) = AgentActorHandle::spawn_inner(
                admission.agent_id,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                Some(admission),
                None,
                Some(child_rx),
            );
            child
                .configure_parent_delivery(auto_deliver, Some(COMPATIBILITY_ID.into()))
                .unwrap();
            let events = child.subscribe_events();
            parent.close();
            let ticket = crate::actor::TurnTicket::new(TurnId::generate(), child.identity());
            ticket.resolve(TurnOutcome::cancelled(
                child.agent_id(),
                ticket.turn_id(),
                TokenUsage::default(),
                0,
                TurnCancellationReason::User,
            ));
            super::publish(&child.inner, &ticket, None);
            let deliveries: Vec<_> = events
                .try_iter()
                .filter_map(|event| match event {
                    ActorEvent::Delivery(event) => Some(event),
                    _ => None,
                })
                .collect();
            if auto_deliver {
                assert_eq!(deliveries.len(), 1);
                assert_eq!(
                    deliveries[0].status,
                    DeliveryStatus::Rejected(DeliveryRejection::ParentClosed)
                );
                assert_eq!(
                    deliveries[0].delivery.compatibility_id.as_deref(),
                    Some(COMPATIBILITY_ID)
                );
            } else {
                assert!(deliveries.is_empty());
            }
            assert!(parent.inner.state.lock().unwrap().deliveries.is_empty());
            child.close();
            child_task.await;
            parent_task.await;
        });
    }

    #[test]
    fn rendered_envelope_omits_null_fields() {
        let envelope = delivery(AgentId::generate());
        assert!(envelope.to_json().to_string().contains("null"));
        let input = envelope.input();
        let rendered = input.preamble[0].first_text_content().unwrap();
        assert!(!rendered.contains("null"), "{rendered}");
        assert!(super::delivery_summary(&input).is_some());
    }

    #[test]
    fn completion_identity_deduplicates_only_same_turn() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let envelope = delivery(actor.agent_id());
            admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            admit_delivery(&actor.inner, Arc::clone(&envelope)).unwrap();
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 2);
            assert!(actor.withdraw_child_delivery(envelope.result.outcome.turn_id()));
            admit_delivery(&actor.inner, envelope).unwrap();
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
            assert!(actor.latest_ticket().is_none());
            actor.close();
            task.await;
        });
    }

    #[test]
    fn completion_preview_and_pending_byte_bounds() {
        smol::block_on(async {
            const LARGE_TEXT: &str = "\u{0001}";
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let mut envelope = (*delivery(actor.agent_id())).clone();
            envelope.result.text = LARGE_TEXT.repeat(super::MAX_DELIVERY_PREVIEW_BYTES);
            envelope
                .result
                .output
                .push(serde_json::json!({ "large": envelope.result.text }));
            let canonical_text = envelope.result.text.clone();
            admit_delivery(&actor.inner, Arc::new(envelope)).unwrap();
            {
                let state = actor.inner.state.lock().unwrap();
                let pending = state.deliveries.values().next().unwrap();
                assert!(pending.bytes <= super::MAX_DELIVERY_PREVIEW_BYTES);
                assert_ne!(pending.delivery.result.text, canonical_text);
                assert_eq!(pending.delivery.to_json()["output"][0]["truncated"], true);
                let text = pending
                    .message
                    .first_text_content()
                    .unwrap()
                    .strip_prefix(super::DELIVERY_HEADER)
                    .unwrap();
                assert!(serde_json::from_str::<serde_json::Value>(text.trim()).is_ok());
            }
            actor.close();
            task.await;
        });
    }

    #[test]
    fn delivery_budget_preserves_fifo_and_explicit_work() {
        smol::block_on(async {
            const INSTRUCTION: &str = "ordinary work";
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let ticket = actor
                .submit(
                    AgentInput::from_defaults(
                        INSTRUCTION.into(),
                        crate::AgentMode::Build,
                        Vec::new(),
                        Default::default(),
                    ),
                    None,
                    INSTRUCTION.into(),
                    Default::default(),
                )
                .unwrap();
            let mut ids = Vec::new();
            for _ in 0..super::MAX_DELIVERY_BATCH + 1 {
                let envelope = delivery(actor.agent_id());
                ids.push(envelope.result.outcome.turn_id());
                admit_delivery(&actor.inner, envelope).unwrap();
            }
            let source =
                super::super::inbox::ActorTurnInbox::source(&actor.inner, ticket.turn_id(), None);
            let events = actor.subscribe_events();
            let mut first = Vec::new();
            source.commit_completions(&mut |message| {
                assert!(actor.inner.state.try_lock().is_err());
                let executed = events
                    .try_iter()
                    .filter(|event| {
                        matches!(
                            event,
                            ActorEvent::Delivery(super::DeliveryEvent {
                                status: DeliveryStatus::Executed { .. },
                                ..
                            })
                        )
                    })
                    .count();
                assert_eq!(executed, usize::from(!first.is_empty()));
                first.push(message);
            });
            assert_eq!(first.len(), super::MAX_DELIVERY_BATCH);
            assert!(events.try_iter().any(|event| matches!(
                event,
                ActorEvent::Delivery(super::DeliveryEvent {
                    status: DeliveryStatus::Executed { .. },
                    ..
                })
            )));
            assert_eq!(
                actor.inner.state.lock().unwrap().delivery_order.front(),
                ids.last()
            );
            assert_eq!(source.drain_completions().len(), 1);
            assert!(source.drain_completions().is_empty());
            assert!(!actor.withdraw_child_delivery(ids[0]));
            assert_eq!(actor.latest_ticket().unwrap().turn_id(), ticket.turn_id());
            actor.close();
            task.await;
        });
    }

    #[test]
    fn pending_completion_blocks_finish_but_not_instruction_close() {
        smol::block_on(async {
            const INSTRUCTION: &str = "ordinary work";
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let ticket = actor
                .submit(
                    AgentInput::from_defaults(
                        INSTRUCTION.into(),
                        crate::AgentMode::Build,
                        Vec::new(),
                        Default::default(),
                    ),
                    None,
                    INSTRUCTION.into(),
                    Default::default(),
                )
                .unwrap();
            let source =
                super::super::inbox::ActorTurnInbox::source(&actor.inner, ticket.turn_id(), None);
            source.instructions_submitted();
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            assert!(!source.finish());
            assert!(source.finish_instructions());
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
            actor.close();
            task.await;
        });
    }

    #[test]
    fn idle_child_batch_uses_one_continuation() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            for _ in 0..super::MAX_DELIVERY_BATCH + 1 {
                admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            }
            let id = {
                let mut state = actor.inner.state.lock().unwrap();
                super::schedule(&actor.inner, &mut state);
                super::schedule(&actor.inner, &mut state);
                assert_eq!(state.delivery_runs.len(), 1);
                assert_eq!(state.deliveries.len(), super::MAX_DELIVERY_BATCH + 1);
                *state.delivery_runs.keys().next().unwrap()
            };
            assert_eq!(actor.inner.tickets.lock().unwrap().len(), 1);
            assert_eq!(
                super::drain(&actor.inner, id).len(),
                super::MAX_DELIVERY_BATCH
            );
            assert!(super::drain(&actor.inner, id).is_empty());
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
            actor.close();
            task.await;
        });
    }

    #[test]
    fn submit_during_delivery_continuation_starts_work() {
        smol::block_on(async {
            const INSTRUCTION: &str = "ordinary instruction";
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            let continuation = {
                let mut state = actor.inner.state.lock().unwrap();
                super::schedule(&actor.inner, &mut state);
                *state.delivery_runs.keys().next().unwrap()
            };
            let ticket = actor
                .submit(
                    AgentInput::from_defaults(
                        INSTRUCTION.into(),
                        crate::AgentMode::Build,
                        Vec::new(),
                        Default::default(),
                    ),
                    None,
                    INSTRUCTION.into(),
                    Default::default(),
                )
                .unwrap();
            assert_ne!(ticket.turn_id(), continuation);
            assert_eq!(
                actor
                    .submit(
                        AgentInput::from_defaults(
                            INSTRUCTION.into(),
                            crate::AgentMode::Build,
                            Vec::new(),
                            Default::default()
                        ),
                        None,
                        INSTRUCTION.into(),
                        Default::default()
                    )
                    .unwrap()
                    .turn_id(),
                ticket.turn_id()
            );
            actor.close();
            task.await;
        });
    }

    #[test]
    fn failed_delivery_continuation_waits_for_recovery_trigger() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            let id = {
                let mut state = actor.inner.state.lock().unwrap();
                super::schedule(&actor.inner, &mut state);
                *state.delivery_runs.keys().next().unwrap()
            };
            actor.cancel_turn(id).unwrap();
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            {
                let mut state = actor.inner.state.lock().unwrap();
                assert!(state.delivery_suspended);
                super::schedule(&actor.inner, &mut state);
                assert!(state.delivery_runs.is_empty());
                assert_eq!(state.deliveries.len(), 2);
            }
            actor.close();
            task.await;
        });
    }

    #[test]
    fn bounded_admission_and_internal_observation() {
        smol::block_on(async {
            let (_start, rx) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                AgentId::generate(),
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(rx),
            );
            let envelope = delivery(actor.agent_id());
            let input = envelope.input();
            assert!(input.message.is_empty());
            assert!(input.preamble[0].is_observation());
            assert_eq!(envelope.to_json()["text"], CHILD_TEXT);
            for _ in 0..MAX_PENDING_DELIVERIES {
                admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            }
            assert!(matches!(
                admit_delivery(&actor.inner, delivery(actor.agent_id())),
                Err(DeliveryRejection::Overflow)
            ));
            actor.close();
            assert!(matches!(
                admit_delivery(&actor.inner, envelope),
                Err(DeliveryRejection::ParentClosed)
            ));
            task.await;
        });
    }
}
