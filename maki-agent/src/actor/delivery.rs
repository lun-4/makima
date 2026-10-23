use std::sync::{Arc, Weak};

use maki_providers::{Message, ObservationSource};
use serde_json::{Value, json};

use super::{
    ActorEvent, ActorInner, ActorLifecycle, ActorOperation, AgentActorHandle, TurnOrigin,
    TurnResult, TurnTicket, drive_operations,
};
use crate::tools::offload::OffloadStore;
use crate::types::{ActorWorkIdentity, ActorWorkKind};
use crate::{AgentInput, TurnCancellationReason, TurnId, TurnOutcome};

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
}

#[derive(Clone)]
pub(crate) struct ParentDelivery {
    pub parent: Weak<ActorInner>,
    pub metadata: crate::AgentMetadata,
}

#[derive(Clone)]
pub struct ChildDelivery {
    pub label: Option<String>,
    pub result: TurnResult,
}

impl ChildDelivery {
    pub fn to_json(&self) -> Value {
        let provenance = &self.result.provenance;
        json!({
            "kind": "child_result",
            "agent_id": self.result.outcome.agent_id(),
            "turn_id": self.result.outcome.turn_id(),
            "label": self.label,
            "outcome": self.result.outcome,
            "text": self.result.text,
            "output": self.result.output,
            "duration_ms": self.result.duration_ms,
            "provenance": {
                "origin": match provenance.origin { TurnOrigin::Internal => "internal", TurnOrigin::User => "user", TurnOrigin::Plugin => "plugin" },
                "plugin": provenance.plugin,
                "plugin_generation": provenance.plugin_generation,
                "source_agent": provenance.source_agent,
                "source_turn": provenance.source_turn,
            },
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
        preview.result.provenance.plugin = preview
            .result
            .provenance
            .plugin
            .map(|text| bounded(&text, PREVIEW_METADATA_BYTES));
        preview
    }

    fn input(&self) -> AgentInput {
        let envelope = self.to_json();
        AgentInput {
            message: String::new(),
            images: Vec::new(),
            preamble: vec![Message {
                observation_source: Some(ObservationSource::ChildResult {
                    agent_id: self.result.outcome.agent_id().to_string(),
                    turn_id: self.result.outcome.turn_id().to_string(),
                }),
                ..Message::observation(format!("{DELIVERY_HEADER}\n{}", without_nulls(envelope)))
            }],
            prompt: None,
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
    cancellation_retried: bool,
    arrived_during_run: bool,
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
    state.delivery_suspended = false;
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
            cancellation_retried: false,
            arrived_during_run: true,
        },
    );
    state.emit(ActorEvent::Delivery(DeliveryEvent {
        delivery,
        status: DeliveryStatus::Accepted,
    }));
    inner.queue.notify();
    Ok(())
}

pub(super) fn publish(inner: &ActorInner, ticket: &TurnTicket) {
    let Some(target) = inner
        .managed_admission
        .as_ref()
        .and_then(|managed| managed.parent_delivery.as_ref())
    else {
        return;
    };
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
        state.parent_delivery_policy.clone()
    };
    if !policy.is_some_and(|policy| policy.auto_deliver) {
        return;
    }
    let Some(result) = ticket.peek_result() else {
        return;
    };
    let delivery = Arc::new(
        ChildDelivery {
            label: target.metadata.label.clone(),
            result,
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

pub(super) fn take_pending(state: &mut super::ActorState) -> Vec<Arc<ChildDelivery>> {
    state.delivery_order.clear();
    state
        .deliveries
        .drain()
        .map(|(_, pending)| pending.delivery)
        .collect()
}

pub(super) fn discard(state: &mut super::ActorState, reason: TurnCancellationReason) {
    for delivery in take_pending(state) {
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
        let new_completion = state
            .deliveries
            .values()
            .any(|pending| pending.arrived_during_run);
        let retry_cancellation = reason.is_some_and(|reason| {
            !matches!(
                reason,
                TurnCancellationReason::Closed | TurnCancellationReason::Shutdown
            )
        }) && batch.iter().any(|id| {
            state
                .deliveries
                .get(id)
                .is_some_and(|pending| !pending.cancellation_retried)
        });
        if retry_cancellation {
            for id in &batch {
                if let Some(pending) = state.deliveries.get_mut(id) {
                    pending.cancellation_retried = true;
                }
            }
        }
        state.delivery_suspended = !new_completion && !retry_cancellation;
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
        || state.idle.is_some()
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
    for pending in state.deliveries.values_mut() {
        pending.arrived_during_run = false;
    }
    state.delivery_runs.insert(turn_id, batch);
    state.next_operation_id = state.next_operation_id.wrapping_add(1);
    let id = state.next_operation_id;
    let presentation_id = state.reserve_presentation(0);
    let identity = ActorWorkIdentity::new(
        inner.agent_id,
        ActorWorkKind::Turn {
            operation_id: id,
            turn_id,
        },
        presentation_id,
    );
    state.operations.push_back(ActorOperation::Turn {
        id,
        input: Box::new(AgentInput::new(String::new(), Vec::new())),
        event_sender: None,
        correlation: "child-results".into(),
        ticket,
        policy_constraint: None,
        identity,
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

    fn delivery(agent_id: AgentId) -> Arc<ChildDelivery> {
        Arc::new(ChildDelivery {
            label: None,
            result: TurnResult {
                outcome: TurnOutcome::cancelled(
                    agent_id,
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
        envelope.result.provenance.origin = crate::actor::TurnOrigin::Plugin;
        envelope.result.provenance.plugin_generation = Some(3);
        envelope.result.provenance.source_agent = Some(AgentId::generate());
        envelope.result.provenance.source_turn = Some(TurnId::generate());
        let canonical = envelope.to_json();
        let preview = envelope.preview(Some(&store));
        let rendered = preview.input().preamble.remove(0);
        assert!(rendered.first_text_content().unwrap().len() <= super::MAX_DELIVERY_PREVIEW_BYTES);
        let value = preview.to_json();
        for field in [
            "kind",
            "agent_id",
            "turn_id",
            "provenance",
            "outcome",
            "duration_ms",
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
                    AgentInput::new(String::new(), Vec::new()),
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
                    AgentInput::new(String::new(), Vec::new()),
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
            parent.configure_parent_delivery(true).unwrap();
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
    fn deferred_cancel_delivers_result_to_actual_parent() {
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
            child.configure_parent_delivery(true).unwrap();
            let barrier = child.reserve_prepared_operation(None).unwrap();
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
            assert_eq!(event.status, DeliveryStatus::Accepted);
            assert_eq!(event.delivery.result.outcome.agent_id(), child.agent_id());
            assert_eq!(event.delivery.result.outcome.turn_id(), ticket.turn_id());
            assert!(matches!(
                event.delivery.result.outcome,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ));
            assert_eq!(parent.inner.state.lock().unwrap().deliveries.len(), 1);
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
                parent: Arc::downgrade(&parent.inner),
                metadata: crate::AgentMetadata {
                    label: Some(LABEL.into()),
                    spawned_by_tool_use_id: Some(TOOL_ID.into()),
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
            child.configure_parent_delivery(auto_deliver).unwrap();
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
            super::publish(&child.inner, &ticket);
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
                let delivery = &deliveries[0].delivery;
                assert_eq!(delivery.label.as_deref(), Some(LABEL));
                assert_eq!(delivery.result.outcome.agent_id(), child.agent_id());
                assert_eq!(delivery.result.outcome.turn_id(), ticket.turn_id());
                assert!(matches!(
                    delivery.result.outcome,
                    TurnOutcome::Cancelled {
                        reason: TurnCancellationReason::User,
                        ..
                    }
                ));
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
        assert!(matches!(
            input.preamble[0].observation_source,
            Some(maki_providers::ObservationSource::ChildResult { .. })
        ));
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
                    AgentInput::new(INSTRUCTION.into(), Vec::new()),
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
                    AgentInput::new(INSTRUCTION.into(), Vec::new()),
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
                    AgentInput::new(INSTRUCTION.into(), Vec::new()),
                    None,
                    INSTRUCTION.into(),
                    Default::default(),
                )
                .unwrap();
            assert_ne!(ticket.turn_id(), continuation);
            assert_eq!(
                actor
                    .submit(
                        AgentInput::new(INSTRUCTION.into(), Vec::new()),
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

    #[test_case(false; "failure_without_new_report")]
    #[test_case(true; "failure_with_new_report_during_finalization")]
    fn failed_continuation_recovery_is_bounded(arrival_during_finalization: bool) {
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
            let later = delivery(actor.agent_id());
            if arrival_during_finalization {
                admit_delivery(&actor.inner, Arc::clone(&later)).unwrap();
            }
            super::settle(&actor.inner, id, None);
            {
                let state = actor.inner.state.lock().unwrap();
                assert_eq!(state.delivery_suspended, !arrival_during_finalization);
                assert!(state.delivery_runs.is_empty());
            }
            if !arrival_during_finalization {
                let first = {
                    let state = actor.inner.state.lock().unwrap();
                    Arc::clone(&state.deliveries.values().next().unwrap().delivery)
                };
                admit_delivery(&actor.inner, first).unwrap();
                assert!(actor.inner.state.lock().unwrap().delivery_suspended);
                admit_delivery(&actor.inner, later).unwrap();
            }
            assert!(!actor.inner.state.lock().unwrap().delivery_suspended);
            assert!(super::drain(&actor.inner, id).is_empty());
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 2);
            actor.cancel_turn(id).unwrap();
            let recovery = schedule_ticket(&actor);
            assert_ne!(recovery.turn_id(), id);
            assert_eq!(super::drain(&actor.inner, recovery.turn_id()).len(), 2);
            super::settle(&actor.inner, recovery.turn_id(), None);
            assert!(!actor.inner.state.lock().unwrap().delivery_suspended);
            actor.close();
            task.await;
        });
    }

    fn deferred_actor(
        parent: Option<&AgentActorHandle>,
    ) -> (AgentActorHandle, smol::Task<()>, flume::Sender<()>) {
        let managed = parent.map(|parent| {
            let mut managed = crate::actor::types::ManagedTurnAdmission::new(
                std::sync::Weak::new(),
                AgentId::generate(),
            );
            managed.parent_delivery = Some(super::ParentDelivery {
                parent: Arc::downgrade(&parent.inner),
                metadata: Default::default(),
            });
            managed
        });
        let id = managed
            .as_ref()
            .map_or_else(AgentId::generate, |managed| managed.agent_id);
        let (start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            id,
            Vec::new(),
            None,
            Box::new(UnusedBackend),
            managed,
            None,
            Some(rx),
        );
        if parent.is_some() {
            actor.configure_parent_delivery(true).unwrap();
        }
        (actor, task, start)
    }

    fn schedule_ticket(actor: &AgentActorHandle) -> crate::actor::TurnTicket {
        let mut state = actor.inner.state.lock().unwrap();
        super::schedule(&actor.inner, &mut state);
        let turn_id = *state.delivery_runs.keys().next().unwrap();
        actor.turn_ticket(turn_id).unwrap()
    }

    #[test]
    fn nested_continuations_report_once_and_quiesce() {
        smol::block_on(async {
            let (ancestor, ancestor_task, _ancestor_start) = deferred_actor(None);
            let (parent, parent_task, _parent_start) = deferred_actor(Some(&ancestor));
            let (child, child_task, _child_start) = deferred_actor(Some(&parent));
            let ancestor_events = ancestor.subscribe_events();
            let parent_events = parent.subscribe_events();
            let child_ticket = child
                .admit_turn(
                    delivery(parent.agent_id()).input(),
                    None,
                    CORRELATION.into(),
                )
                .unwrap();
            child.cancel_turn(child_ticket.turn_id()).unwrap();
            let parent_ticket = schedule_ticket(&parent);
            assert_eq!(
                super::drain(&parent.inner, parent_ticket.turn_id()).len(),
                1
            );
            parent.cancel_turn(parent_ticket.turn_id()).unwrap();
            super::publish(&parent.inner, &parent_ticket);
            let ancestor_ticket = schedule_ticket(&ancestor);
            assert_eq!(
                super::drain(&ancestor.inner, ancestor_ticket.turn_id()).len(),
                1
            );
            ancestor.cancel_turn(ancestor_ticket.turn_id()).unwrap();
            for actor in [&ancestor, &parent, &child] {
                let mut state = actor.inner.state.lock().unwrap();
                super::schedule(&actor.inner, &mut state);
                super::schedule(&actor.inner, &mut state);
                assert!(state.deliveries.is_empty());
                assert!(state.delivery_runs.is_empty());
                assert!(!state.has_pending_work(&actor.inner.queue));
                assert!(state.operations.is_empty());
            }
            for (events, turn_id) in [
                (&parent_events, parent_ticket.turn_id()),
                (&ancestor_events, ancestor_ticket.turn_id()),
            ] {
                let statuses: Vec<_> = events
                    .try_iter()
                    .filter_map(|event| match event {
                        ActorEvent::Delivery(event) => Some(event.status),
                        _ => None,
                    })
                    .collect();
                assert_eq!(
                    statuses,
                    [
                        DeliveryStatus::Accepted,
                        DeliveryStatus::Executed {
                            parent_turn_id: turn_id
                        }
                    ]
                );
            }
            child.close();
            parent.close();
            ancestor.close();
            child_task.await;
            parent_task.await;
            ancestor_task.await;
        });
    }

    #[test]
    fn cancelled_parent_resumes_and_reports_upward() {
        smol::block_on(async {
            let (ancestor, ancestor_task, _ancestor_start) = deferred_actor(None);
            let (parent, parent_task, _parent_start) = deferred_actor(Some(&ancestor));
            let original = parent
                .admit_turn(
                    delivery(parent.agent_id()).input(),
                    None,
                    CORRELATION.into(),
                )
                .unwrap();
            parent.cancel_turn(original.turn_id()).unwrap();
            let original_result = original.wait_result().await;
            assert!(ancestor.withdraw_child_delivery(original.turn_id()));
            let child_report = delivery(parent.agent_id());
            admit_delivery(&parent.inner, Arc::clone(&child_report)).unwrap();
            let continuation = schedule_ticket(&parent);
            assert_ne!(continuation.turn_id(), original.turn_id());
            assert_eq!(super::drain(&parent.inner, continuation.turn_id()).len(), 1);
            continuation.resolve(TurnOutcome::completed(
                parent.agent_id(),
                continuation.turn_id(),
                TokenUsage::default(),
                1,
                crate::DoneReason::EndTurn,
            ));
            super::publish(&parent.inner, &continuation);
            super::publish(&parent.inner, &continuation);
            super::settle(&parent.inner, continuation.turn_id(), None);
            admit_delivery(&parent.inner, child_report).unwrap();
            assert!(parent.inner.state.lock().unwrap().deliveries.is_empty());
            assert_eq!(
                original.wait_result().await.outcome,
                original_result.outcome
            );
            {
                let state = ancestor.inner.state.lock().unwrap();
                assert_eq!(state.deliveries.len(), 1);
                assert!(matches!(
                    state.deliveries[&continuation.turn_id()]
                        .delivery
                        .result
                        .outcome,
                    TurnOutcome::Completed { .. }
                ));
            }
            parent.close();
            ancestor.close();
            parent_task.await;
            ancestor_task.await;
        });
    }

    #[test]
    fn failed_batch_does_not_treat_existing_backlog_as_recovery() {
        smol::block_on(async {
            let (actor, task, _start) = deferred_actor(None);
            for _ in 0..super::MAX_DELIVERY_BATCH + 1 {
                admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            }
            let ticket = schedule_ticket(&actor);
            super::settle(&actor.inner, ticket.turn_id(), None);
            {
                let mut state = actor.inner.state.lock().unwrap();
                assert!(state.delivery_suspended);
                assert_eq!(state.deliveries.len(), super::MAX_DELIVERY_BATCH + 1);
                super::schedule(&actor.inner, &mut state);
                assert!(state.delivery_runs.is_empty());
            }
            actor.close();
            task.await;
        });
    }

    #[test]
    fn consumed_report_is_acknowledged_after_provider_failure() {
        smol::block_on(async {
            let (actor, task, _start) = deferred_actor(None);
            let report = delivery(actor.agent_id());
            admit_delivery(&actor.inner, Arc::clone(&report)).unwrap();
            let ticket = schedule_ticket(&actor);
            assert_eq!(super::drain(&actor.inner, ticket.turn_id()).len(), 1);
            super::settle(&actor.inner, ticket.turn_id(), None);
            admit_delivery(&actor.inner, report).unwrap();
            {
                let state = actor.inner.state.lock().unwrap();
                assert!(state.deliveries.is_empty());
                assert!(state.delivery_runs.is_empty());
                assert!(!state.delivery_suspended);
            }
            actor.close();
            task.await;
        });
    }

    #[test]
    fn cancellation_retries_each_unconsumed_report_once() {
        smol::block_on(async {
            let (actor, task, _start) = deferred_actor(None);
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            let first = schedule_ticket(&actor);
            actor.cancel_turn(first.turn_id()).unwrap();
            assert!(!actor.inner.state.lock().unwrap().delivery_suspended);
            let retry = schedule_ticket(&actor);
            assert_ne!(first.turn_id(), retry.turn_id());
            actor.cancel_turn(retry.turn_id()).unwrap();
            {
                let mut state = actor.inner.state.lock().unwrap();
                assert!(state.delivery_suspended);
                super::schedule(&actor.inner, &mut state);
                assert!(state.delivery_runs.is_empty());
                assert_eq!(state.deliveries.len(), 1);
            }
            admit_delivery(&actor.inner, delivery(actor.agent_id())).unwrap();
            let recovered = schedule_ticket(&actor);
            assert_eq!(super::drain(&actor.inner, recovered.turn_id()).len(), 2);
            actor.cancel_turn(recovered.turn_id()).unwrap();
            assert!(actor.inner.state.lock().unwrap().deliveries.is_empty());
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
