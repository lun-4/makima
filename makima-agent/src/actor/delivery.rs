use std::collections::BTreeMap;
use std::sync::Arc;

use crate::manager::WorkReservation;
use makima_providers::{Message, ObservationSource};
use makima_storage::session_state::{StateChange, StoredDeliveryOp};
use serde_json::{Value, json};

use super::{
    ActorEvent, ActorInner, ActorLifecycle, ActorOperation, AgentActorHandle, TurnOrigin,
    TurnResult, TurnTicket, drive_operations,
};
use crate::journal::{JournalChange, JournalCommit, JournalIncarnation, with_history_changes};
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
    pub parent_id: crate::AgentId,
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

    pub(crate) fn preview(&self, store: Option<&OffloadStore>) -> Self {
        let serialized = self.to_json().to_string();
        if serialized.len() + DELIVERY_HEADER.len() < MAX_DELIVERY_PREVIEW_BYTES {
            return self.clone();
        }
        self.truncated_preview(store, &serialized)
    }

    pub(crate) fn truncated_preview(&self, store: Option<&OffloadStore>, serialized: &str) -> Self {
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
                match store.put(serialized) {
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

    pub(crate) fn input(&self) -> AgentInput {
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
    Journal(String),
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
    pub(crate) work: Vec<WorkReservation>,
    cancellation_retried: bool,
    arrived_during_run: bool,
}

pub(super) fn restore_pending(
    state: &mut super::ActorState,
    delivery: Arc<ChildDelivery>,
    work: Vec<WorkReservation>,
) {
    let id = delivery.result.outcome.turn_id();
    if state.deliveries.contains_key(&id) {
        return;
    }
    let preview = Arc::new(delivery.preview(None));
    let message = preview.input().preamble.remove(0);
    let bytes = message.first_text_content().map_or(0, str::len);
    if !state.delivery_seen.contains(&id) {
        state.delivery_seen.push_back(id);
    }
    state.delivery_order.push_back(id);
    state.deliveries.insert(
        id,
        PendingDelivery {
            delivery: preview,
            message,
            bytes,
            work,
            cancellation_retried: false,
            arrived_during_run: false,
        },
    );
}

#[cfg(test)]
pub(crate) fn admit_delivery(
    inner: &Arc<ActorInner>,
    delivery: Arc<ChildDelivery>,
) -> Result<(), DeliveryRejection> {
    admit_delivery_reserved(inner, delivery, Vec::new())
}

#[cfg(test)]
pub(crate) fn admit_delivery_reserved(
    inner: &Arc<ActorInner>,
    delivery: Arc<ChildDelivery>,
    work: Vec<WorkReservation>,
) -> Result<(), DeliveryRejection> {
    admit_delivery_bound(inner, delivery, work, None)
}

pub(crate) fn admit_delivery_bound(
    inner: &Arc<ActorInner>,
    delivery: Arc<ChildDelivery>,
    work: Vec<WorkReservation>,
    source: Option<JournalIncarnation>,
) -> Result<(), DeliveryRejection> {
    let bindings = source
        .into_iter()
        .chain(inner.journal_incarnation.clone())
        .collect::<Vec<_>>();
    let store = if inner.journal.is_some() {
        None
    } else {
        inner.offload.as_deref()
    };
    let preview = Arc::new(delivery.preview(store));
    let message = preview.input().preamble.remove(0);
    let bytes = message.first_text_content().map_or(0, str::len);
    let mut state = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    if state.lifecycle == ActorLifecycle::Shutdown {
        let journal = inner
            .journal
            .as_ref()
            .ok_or(DeliveryRejection::ParentClosed)?;
        if journal.queue_delivery_for(inner.agent_id, Arc::clone(&delivery), bytes, &bindings)? {
            retain_work(inner, &delivery, work);
        }
        return Ok(());
    }
    if state.lifecycle != ActorLifecycle::Open {
        return Err(DeliveryRejection::ParentClosed);
    }
    let id = delivery.result.outcome.turn_id();
    if state.deliveries.contains_key(&id) || state.delivery_seen.contains(&id) {
        return Ok(());
    }
    if let Some(journal) = &inner.journal {
        if !journal.queue_delivery_for(inner.agent_id, Arc::clone(&delivery), bytes, &bindings)? {
            return Ok(());
        }
    } else if bytes > MAX_DELIVERY_PREVIEW_BYTES
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
    let delivery = preview;
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
            work,
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
    let delivery = Arc::new(ChildDelivery {
        label: target.metadata.label.clone(),
        result,
    });
    let work = inner
        .state
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .turn_work
        .get_mut(&ticket.turn_id())
        .into_iter()
        .flatten()
        .filter_map(WorkReservation::take_report)
        .collect();
    let result = inner
        .managed_admission
        .as_ref()
        .and_then(|managed| managed.manager.upgrade())
        .ok_or(DeliveryRejection::ParentUnavailable)
        .and_then(|manager| {
            crate::AgentManagerHandle::from_inner(manager).deliver_reserved_for(
                target.parent_id,
                Arc::clone(&delivery),
                work,
                inner.journal_incarnation.clone(),
            )
        });
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
    pub(crate) fn reconcile_rewind_delivery_work(
        &self,
        mut work: impl FnMut(crate::AgentId, TurnId) -> Vec<WorkReservation>,
    ) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(journal) = &self.inner.journal {
            let snapshot = journal.snapshot();
            let mut deliveries = BTreeMap::new();
            for record in &snapshot.state {
                if record.agent == Some(self.agent_id())
                    && let StateChange::Delivery { id, op } = &record.change
                {
                    if deliveries.get(id).is_some_and(|(_, previous)| {
                        !matches!(previous, StoredDeliveryOp::Queued(_))
                    }) {
                        continue;
                    }
                    deliveries.insert(*id, (record.seq, op.clone()));
                }
            }
            state.deliveries.retain(|turn, pending| {
                matches!(
                    deliveries.get(&(pending.delivery.result.outcome.agent_id(), *turn)),
                    Some((_, StoredDeliveryOp::Queued(_)))
                )
            });
            let mut order = state
                .deliveries
                .iter()
                .filter_map(|(turn, pending)| {
                    deliveries
                        .get(&(pending.delivery.result.outcome.agent_id(), *turn))
                        .map(|(seq, _)| (*seq, *turn))
                })
                .collect::<Vec<_>>();
            order.sort_unstable();
            state.delivery_order = order.into_iter().map(|(_, turn)| turn).collect();
            let pending = state.delivery_order.iter().copied().collect::<Vec<_>>();
            state.delivery_runs.retain(|_, batch| {
                batch.retain(|turn| pending.contains(turn));
                !batch.is_empty()
            });
            state.delivery_seen = deliveries.keys().map(|(_, turn)| *turn).collect();
            state.delivery_held = deliveries
                .values()
                .any(|(_, op)| matches!(op, StoredDeliveryOp::Queued(delivery) if delivery.held));
            state.delivery_suspended = state.delivery_held;
        }
        for pending in state.deliveries.values_mut() {
            pending.work = work(
                pending.delivery.result.outcome.agent_id(),
                pending.delivery.result.outcome.turn_id(),
            );
        }
    }

    pub(crate) fn reconcile_pending_deliveries(
        &self,
        mut work: impl FnMut(crate::AgentId, TurnId) -> Vec<WorkReservation>,
    ) -> Result<(), super::ActorError> {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(journal) = &self.inner.journal else {
            return Ok(());
        };
        let pending = journal
            .pending_deliveries(self.inner.agent_id)
            .map_err(super::ActorError::HistoryCheckpoint)?;
        if pending
            .iter()
            .any(|(_, turn, delivery)| delivery.is_none() && !state.deliveries.contains_key(turn))
        {
            return Err(super::ActorError::HistoryCheckpoint(
                "stored delivery was not hydrated before initialization".into(),
            ));
        }
        let ids = pending.iter().map(|(_, turn, _)| *turn).collect::<Vec<_>>();
        state.deliveries.retain(|turn, _| ids.contains(turn));
        state.delivery_order.clear();
        for (source, turn, delivery) in pending {
            if !state.deliveries.contains_key(&turn) {
                let delivery = delivery.ok_or_else(|| {
                    super::ActorError::HistoryCheckpoint(
                        "stored delivery was not hydrated before initialization".into(),
                    )
                })?;
                restore_pending(&mut state, delivery, work(source, turn));
            }
            if let Some(pending) = state.deliveries.get_mut(&turn)
                && pending.work.is_empty()
            {
                pending.work = work(source, turn);
            }
            if !state.delivery_order.contains(&turn) {
                state.delivery_order.push_back(turn);
            }
        }
        Ok(())
    }

    pub fn withdraw_child_delivery(&self, child_turn: TurnId) -> bool {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(pending) = state.deliveries.get(&child_turn) else {
            return false;
        };
        if !record_operation(&self.inner, &pending.delivery, StoredDeliveryOp::Withdrawn) {
            return false;
        }
        let pending = state
            .deliveries
            .remove(&child_turn)
            .expect("checked under actor lock");
        state.delivery_order.retain(|id| *id != child_turn);
        state.emit(ActorEvent::Delivery(DeliveryEvent {
            delivery: pending.delivery,
            status: DeliveryStatus::Discarded(TurnCancellationReason::Withdrawn),
        }));
        true
    }
}

pub(crate) fn change(
    parent_id: crate::AgentId,
    delivery: &ChildDelivery,
    op: StoredDeliveryOp,
) -> JournalChange {
    JournalChange {
        agent: Some(parent_id),
        generation: 0,
        change: StateChange::Delivery {
            id: (
                delivery.result.outcome.agent_id(),
                delivery.result.outcome.turn_id(),
            ),
            op,
        },
    }
}

pub(super) fn record_drop(inner: &ActorInner, delivery: &ChildDelivery) {
    record_operation(inner, delivery, StoredDeliveryOp::Dropped);
}

fn record_operation(inner: &ActorInner, delivery: &ChildDelivery, op: StoredDeliveryOp) -> bool {
    let Some(journal) = &inner.journal else {
        return true;
    };
    match journal.commit(JournalCommit {
        incarnations: inner.journal_incarnation.clone().into_iter().collect(),
        changes: vec![change(inner.agent_id, delivery, op)],
        ..Default::default()
    }) {
        Ok(_) => true,
        Err(error) => {
            tracing::warn!(%error, agent_id = %inner.agent_id, "delivery transition could not be journaled");
            false
        }
    }
}

fn retain_work(inner: &ActorInner, delivery: &ChildDelivery, work: Vec<WorkReservation>) {
    if let Some(manager) = inner
        .managed_admission
        .as_ref()
        .and_then(|managed| managed.manager.upgrade())
    {
        crate::AgentManagerHandle::from_inner(manager).retain_delivery_work(
            inner.agent_id,
            delivery.result.outcome.agent_id(),
            delivery.result.outcome.turn_id(),
            work,
        );
    }
}

pub(super) fn take_pending(
    inner: &ActorInner,
    state: &mut super::ActorState,
    preserve: bool,
) -> Vec<Arc<ChildDelivery>> {
    state.delivery_order.clear();
    state
        .deliveries
        .drain()
        .map(|(_, pending)| {
            if preserve {
                retain_work(inner, &pending.delivery, pending.work);
            }
            pending.delivery
        })
        .collect()
}

pub(super) fn discard(
    inner: &ActorInner,
    state: &mut super::ActorState,
    reason: TurnCancellationReason,
) {
    for delivery in take_pending(inner, state, reason == TurnCancellationReason::Shutdown) {
        if reason != TurnCancellationReason::Shutdown {
            record_drop(inner, &delivery);
        }
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
        || held(inner, &state)
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
        state
            .turn_work
            .entry(turn_id)
            .or_default()
            .extend(pending.work);
        bytes += pending.bytes;
        with_history_changes(
            vec![change(
                inner.agent_id,
                &pending.delivery,
                StoredDeliveryOp::Consumed,
            )],
            || commit(pending.message),
        );
        committed += 1;
        state.emit(ActorEvent::Delivery(DeliveryEvent {
            delivery: pending.delivery,
            status: DeliveryStatus::Executed {
                parent_turn_id: turn_id,
            },
        }));
    }
}

pub(super) fn held(inner: &ActorInner, state: &super::ActorState) -> bool {
    let Some(journal) = &inner.journal else {
        return state.delivery_held;
    };
    let snapshot = journal.snapshot();
    let mut deliveries = BTreeMap::new();
    for record in &snapshot.state {
        if record.agent == Some(inner.agent_id)
            && let StateChange::Delivery { id, op } = &record.change
        {
            deliveries.insert(*id, op);
        }
    }
    deliveries
        .values()
        .any(|op| matches!(op, StoredDeliveryOp::Queued(delivery) if delivery.held))
}

/// Reserve only after explicit FIFO work has drained. Records are not claimed
/// until the model loop inserts them into history at a safe request boundary.
pub(super) fn schedule(inner: &Arc<ActorInner>, state: &mut super::ActorState) {
    if state.lifecycle != ActorLifecycle::Open
        || state.idle.is_some()
        || state.delivery_suspended
        || held(inner, state)
        || state.deliveries.is_empty()
        || !state.delivery_runs.is_empty()
        || state.has_pending_work(&inner.queue)
        || !state.operations.is_empty()
    {
        return;
    }
    let turn_id = TurnId::generate();
    let ticket = TurnTicket::new(turn_id, Arc::clone(&inner.identity));
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
    let work = batch
        .iter()
        .filter_map(|id| state.deliveries.get(id))
        .flat_map(|pending| pending.work.iter().map(WorkReservation::hold_only))
        .collect::<Vec<_>>();
    state.turn_work.insert(turn_id, work);
    inner.admit_ticket(turn_id, ticket.clone());
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

    use makima_providers::TokenUsage;
    use makima_storage::session_state::{SessionStateView, StoredDeliveryOp};
    use test_case::test_case;

    use super::{
        ChildDelivery, DeliveryRejection, DeliveryStatus, MAX_PENDING_DELIVERIES, admit_delivery,
    };
    use crate::actor::{
        ActorBackend, ActorEvent, ActorHistory, BackendResult, ControlWork, TurnContext,
        TurnProvenance, TurnResult, WorkKind,
    };
    use crate::{
        AgentActorHandle, AgentId, AgentInput, History, TurnCancellationReason, TurnId, TurnOutcome,
    };

    const CHILD_TEXT: &str = "child result";
    const LABEL: &str = "research";
    const CORRELATION: &str = "child correlation";
    const CONVERSION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const CONVERSION_TIMEOUT_MESSAGE: &str = "delivery conversion did not settle";

    struct UnusedBackend;

    impl ActorBackend for UnusedBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(async { unreachable!("deferred runner must not execute") })
        }
        fn run_compact<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
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

    #[test_case(None, false; "absent_report_pruned_unheld_outside_preserved")]
    #[test_case(Some(StoredDeliveryOp::Dropped), true; "dropped_child_report_pruned_held_outside_preserved")]
    #[test_case(Some(StoredDeliveryOp::Consumed), false; "consumed_child_report_pruned")]
    #[test_case(Some(StoredDeliveryOp::Withdrawn), true; "withdrawn_child_report_pruned")]
    fn rewind_reconciles_pending_reports_against_installed_journal(
        terminal: Option<StoredDeliveryOp>,
        held: bool,
    ) {
        smol::block_on(async {
            use crate::journal::{JournalChange, JournalCommit};
            use makima_storage::id::MakimaId;
            use makima_storage::session_state::{StateChange, StoredDelivery};

            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let kept = delivery(AgentId::generate());
            let removed = delivery(AgentId::generate());
            let kept_turn = kept.result.outcome.turn_id();
            let removed_turn = removed.result.outcome.turn_id();
            {
                let mut state = parent.inner.state.lock().unwrap();
                super::restore_pending(&mut state, Arc::clone(&removed), Vec::new());
                super::restore_pending(&mut state, Arc::clone(&kept), Vec::new());
                state
                    .delivery_runs
                    .insert(TurnId::generate(), vec![removed_turn, kept_turn]);
                state.delivery_suspended = !held;
                state.delivery_held = !held;
            }
            let mut changes = vec![JournalChange {
                agent: Some(parent.agent_id()),
                generation: 0,
                change: StateChange::Delivery {
                    id: (kept.result.outcome.agent_id(), kept_turn),
                    op: StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                        label: kept.label.clone(),
                        result: kept.result.to_stored(MakimaId::generate(), None).unwrap(),
                        preview_bytes: None,
                        held,
                    })),
                },
            }];
            if let Some(op) = terminal {
                changes.push(JournalChange {
                    agent: Some(parent.agent_id()),
                    generation: 0,
                    change: StateChange::Delivery {
                        id: (removed.result.outcome.agent_id(), removed_turn),
                        op,
                    },
                });
            }
            let journal = manager.journal();
            journal
                .commit(JournalCommit {
                    changes,
                    ..Default::default()
                })
                .unwrap();
            let revision = journal.snapshot().revision;
            let mut reconciled = Vec::new();
            parent.reconcile_rewind_delivery_work(|source, turn| {
                reconciled.push((source, turn));
                Vec::new()
            });
            assert_eq!(
                reconciled,
                vec![(kept.result.outcome.agent_id(), kept_turn)]
            );
            assert_eq!(journal.snapshot().revision, revision);
            {
                let state = parent.inner.state.lock().unwrap();
                assert_eq!(state.deliveries.len(), 1);
                assert!(state.deliveries.contains_key(&kept_turn));
                assert_eq!(
                    state.delivery_order.iter().copied().collect::<Vec<_>>(),
                    vec![kept_turn]
                );
                assert!(
                    state
                        .delivery_runs
                        .values()
                        .all(|batch| batch == &[kept_turn])
                );
                assert_eq!(state.delivery_held, held);
                assert_eq!(state.delivery_suspended, held);
                assert_eq!(super::held(&parent.inner, &state), held);
            }
            parent.shutdown();
            task.await;
        });
    }

    #[test_case(false; "saved")]
    #[test_case(true; "closed_store")]
    fn oversized_completion_offload_preserves_envelope(closed: bool) {
        let directory = tempfile::tempdir().unwrap();
        let store =
            crate::tools::offload::OffloadStore::on_disk(directory.path().canonicalize().unwrap());
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

    async fn settle_conversions(journal: &crate::journal::SessionJournal) {
        futures_lite::future::or(
            async {
                while journal.pending_revision().is_some() {
                    smol::future::yield_now().await;
                }
            },
            async {
                smol::Timer::after(CONVERSION_TIMEOUT).await;
                panic!("{CONVERSION_TIMEOUT_MESSAGE}");
            },
        )
        .await;
    }

    async fn delivery_view(manager: &crate::AgentManagerHandle) -> SessionStateView {
        let journal = manager.journal();
        settle_conversions(&journal).await;
        let snapshot = journal.snapshot();
        SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq).unwrap()
    }

    #[test_case(false; "persisted_parent")]
    #[test_case(true; "resident_parent")]
    fn manager_delivery_routes_once_and_preserves_queue_bound(resident: bool) {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let router = if resident {
                manager.clone()
            } else {
                crate::AgentManagerHandle::new_with_journal(Default::default(), manager.journal())
                    .unwrap()
            };
            let envelope = delivery(AgentId::generate());
            let id = (
                envelope.result.outcome.agent_id(),
                envelope.result.outcome.turn_id(),
            );
            router
                .deliver(parent.agent_id(), Arc::clone(&envelope))
                .unwrap();
            router
                .deliver(parent.agent_id(), Arc::clone(&envelope))
                .unwrap();
            for _ in 1..MAX_PENDING_DELIVERIES {
                router.deliver(parent.agent_id(), delivery(id.0)).unwrap();
            }
            assert_eq!(
                router.deliver(parent.agent_id(), delivery(id.0)),
                Err(DeliveryRejection::Overflow),
            );
            router
                .deliver(parent.agent_id(), Arc::clone(&envelope))
                .unwrap();
            let view = delivery_view(&router).await;
            let pending = &view.scopes[&Some(parent.agent_id())].deliveries;
            assert_eq!(pending.len(), MAX_PENDING_DELIVERIES);
            assert!(matches!(&pending[&id], StoredDeliveryOp::Queued(stored)
                if stored.result.outcome.agent_id == id.0
                    && stored.result.outcome.turn_id == id.1));
            assert_eq!(
                parent.inner.state.lock().unwrap().deliveries.len(),
                if resident { MAX_PENDING_DELIVERIES } else { 0 },
            );
            parent.shutdown();
            task.await;
        });
    }

    #[test_case(false; "persisted_parent")]
    #[test_case(true; "resident_parent")]
    fn manager_delivery_preserves_preview_byte_bound(resident: bool) {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let router = if resident {
                manager.clone()
            } else {
                crate::AgentManagerHandle::new_with_journal(Default::default(), manager.journal())
                    .unwrap()
            };
            let mut envelope = (*delivery(AgentId::generate())).clone();
            envelope.result.text =
                CHILD_TEXT.repeat(super::MAX_DELIVERY_PREVIEW_BYTES / CHILD_TEXT.len() / 2);
            let bytes = envelope.input().preamble[0]
                .first_text_content()
                .unwrap()
                .len();
            let count = super::MAX_PENDING_PREVIEW_BYTES / bytes;
            assert!(count < MAX_PENDING_DELIVERIES);
            for _ in 0..count {
                envelope.result.outcome = TurnOutcome::cancelled(
                    envelope.result.outcome.agent_id(),
                    TurnId::generate(),
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                );
                router
                    .deliver(parent.agent_id(), Arc::new(envelope.clone()))
                    .unwrap();
            }
            envelope.result.outcome = TurnOutcome::cancelled(
                envelope.result.outcome.agent_id(),
                TurnId::generate(),
                TokenUsage::default(),
                0,
                TurnCancellationReason::User,
            );
            assert_eq!(
                router.deliver(parent.agent_id(), Arc::new(envelope)),
                Err(DeliveryRejection::Overflow),
            );
            parent.shutdown();
            task.await;
        });
    }

    #[test_case(false; "closed")]
    #[test_case(true; "retired")]
    fn persisted_terminal_parent_rejects_without_queue_mutation(retired: bool) {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let journal = manager.journal();
            let mut record = journal.agent(parent.agent_id()).unwrap();
            record.lifecycle = if retired {
                makima_storage::session_state::StoredAgentLifecycle::Retired
            } else {
                makima_storage::session_state::StoredAgentLifecycle::Closed
            };
            journal
                .commit(crate::journal::JournalCommit {
                    agents: vec![record],
                    ..Default::default()
                })
                .unwrap();
            let router =
                crate::AgentManagerHandle::new_with_journal(Default::default(), journal).unwrap();
            let before = router.journal().snapshot().revision;
            assert_eq!(
                router.deliver(parent.agent_id(), delivery(AgentId::generate())),
                Err(DeliveryRejection::ParentClosed),
            );
            assert_eq!(router.journal().snapshot().revision, before);
            assert!(parent.inner.state.lock().unwrap().deliveries.is_empty());
            parent.shutdown();
            task.await;
        });
    }

    #[test_case(false; "explicit_close")]
    #[test_case(true; "shutdown")]
    fn manager_delivery_close_distinguishes_shutdown(shutdown: bool) {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let envelope = delivery(AgentId::generate());
            let id = (
                envelope.result.outcome.agent_id(),
                envelope.result.outcome.turn_id(),
            );
            manager
                .deliver(parent.agent_id(), Arc::clone(&envelope))
                .unwrap();
            if shutdown {
                parent.shutdown();
            } else {
                parent.close();
            }
            task.await;
            let view = delivery_view(&manager).await;
            let op = &view.scopes[&Some(parent.agent_id())].deliveries[&id];
            if shutdown {
                assert!(matches!(op, StoredDeliveryOp::Queued(_)));
            } else {
                assert_eq!(op, &StoredDeliveryOp::Dropped);
            }
            assert!(parent.inner.state.lock().unwrap().deliveries.is_empty());
        });
    }

    #[test]
    fn consumed_delivery_and_observation_share_journal_commit() {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let envelope = delivery(AgentId::generate());
            let id = (
                envelope.result.outcome.agent_id(),
                envelope.result.outcome.turn_id(),
            );
            manager.deliver(parent.agent_id(), envelope).unwrap();
            let ticket = schedule_ticket(&parent);
            let mut history = History::new(Vec::new())
                .with_mirror(Arc::clone(&parent.inner.history))
                .with_journal(parent.agent_id(), manager.journal());
            let journal = manager.journal();
            settle_conversions(&journal).await;
            let before = journal.snapshot();
            assert!(before.histories[&parent.agent_id()].messages.is_empty());
            let inbox =
                super::super::inbox::ActorTurnInbox::source(&parent.inner, ticket.turn_id(), None);
            inbox.commit_completions(&mut |message| {
                history.push(message);
                let snapshot = manager.journal().snapshot();
                let view = SessionStateView::fold(
                    &snapshot.state,
                    &snapshot.agent_revisions,
                    snapshot.seq,
                )
                .unwrap();
                assert_eq!(
                    view.scopes[&Some(parent.agent_id())].deliveries[&id],
                    StoredDeliveryOp::Consumed,
                );
                assert_eq!(snapshot.histories[&parent.agent_id()].messages.len(), 1);
                assert_eq!(snapshot.revision, before.revision + 1);
                assert_eq!(snapshot.seq, before.seq + 1);
            });
            let snapshot = journal.snapshot();
            let mut session = makima_storage::sessions::Session::<
                makima_providers::Message,
                TokenUsage,
                crate::ToolOutput,
            >::new("test/model", ".");
            snapshot.apply_to(&mut session).unwrap();
            let directory = tempfile::tempdir().unwrap();
            session.save_to(directory.path()).unwrap();
            let loaded = makima_storage::sessions::Session::<
                makima_providers::Message,
                TokenUsage,
                crate::ToolOutput,
            >::load_from(session.id, directory.path())
            .unwrap();
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(
                loaded.latest_state().unwrap().scopes[&Some(parent.agent_id())].deliveries[&id],
                StoredDeliveryOp::Consumed
            );
            let restored = crate::journal::SessionJournal::from_session(&loaded).unwrap();
            assert_eq!(
                restored.snapshot().histories[&parent.agent_id()]
                    .messages
                    .len(),
                1
            );
            assert!(!parent.withdraw_child_delivery(id.1));
            parent.shutdown();
            task.await;
        });
    }

    #[test]
    fn manager_delivery_withdrawal_is_terminal() {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (parent, task, _start) = deferred_actor(&manager, None);
            let envelope = delivery(AgentId::generate());
            let id = (
                envelope.result.outcome.agent_id(),
                envelope.result.outcome.turn_id(),
            );
            manager
                .deliver(parent.agent_id(), Arc::clone(&envelope))
                .unwrap();
            assert!(parent.withdraw_child_delivery(id.1));
            assert!(!parent.withdraw_child_delivery(id.1));
            manager.deliver(parent.agent_id(), envelope).unwrap();
            let view = delivery_view(&manager).await;
            assert_eq!(
                view.scopes[&Some(parent.agent_id())].deliveries[&id],
                StoredDeliveryOp::Withdrawn,
            );
            assert!(parent.inner.state.lock().unwrap().deliveries.is_empty());
            parent.shutdown();
            task.await;
        });
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            manager.register_delivery_actor_for_test(&ancestor);
            let managed =
                manager.delivery_admission_for_test(AgentId::generate(), ancestor.agent_id());
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            manager.register_delivery_actor_for_test(&parent);
            let managed =
                manager.delivery_admission_for_test(AgentId::generate(), parent.agent_id());
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            manager.register_delivery_actor_for_test(&parent);
            let mut admission =
                manager.delivery_admission_for_test(AgentId::generate(), parent.agent_id());
            admission.parent_delivery = Some(super::ParentDelivery {
                parent_id: parent.agent_id(),
                metadata: crate::AgentMetadata {
                    transcript_key: Some(TOOL_ID.into()),
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
            Some(makima_providers::ObservationSource::ChildResult { .. })
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

    #[test_case(false; "available_report_blocks_finish")]
    #[test_case(true; "held_report_allows_unrelated_turn_to_finish")]
    fn pending_completion_blocks_finish_but_not_instruction_close(held: bool) {
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
            actor.inner.state.lock().unwrap().delivery_held = held;
            if held {
                assert!(source.drain_completions().is_empty());
            }
            assert_eq!(source.finish(), held);
            assert!(source.finish_instructions());
            assert_eq!(actor.inner.state.lock().unwrap().deliveries.len(), 1);
            actor.close();
            task.await;
        });
    }

    #[test]
    fn unrelated_plugin_turn_completes_with_restored_held_reports() {
        const INSTRUCTION: &str = "ordinary plugin work";
        const TURN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
        const TURN_TIMEOUT_MESSAGE: &str = "plugin turn did not finish with held reports";

        struct FinishingBackend;

        impl ActorBackend for FinishingBackend {
            fn run_turn<'a>(
                &'a mut self,
                _: ActorHistory<'a>,
                context: TurnContext,
                input: AgentInput,
                work: WorkKind,
            ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
                Box::pin(async move {
                    assert_eq!(work, WorkKind::Turn);
                    assert_eq!(input.message, INSTRUCTION);
                    assert!(input.preamble.is_empty());
                    let inbox = context.inbox.as_ref().unwrap();
                    inbox.instructions_submitted();
                    assert!(inbox.drain_completions().is_empty());
                    inbox.join_children().await;
                    assert!(inbox.drain_completions().is_empty());
                    assert!(inbox.finish());
                    BackendResult::EnteredRun(TurnOutcome::completed(
                        context.agent_id,
                        context.turn_id.unwrap(),
                        TokenUsage::default(),
                        1,
                        crate::DoneReason::EndTurn,
                    ))
                })
            }

            fn run_control<'a>(
                &'a mut self,
                _: ActorHistory<'a>,
                _: TurnContext,
                _: &'a ControlWork,
            ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
                Box::pin(async { unreachable!("plugin turn must not run control work") })
            }

            fn run_compact<'a>(
                &'a mut self,
                _: ActorHistory<'a>,
                _: TurnContext,
                _: Option<&'a str>,
            ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
                Box::pin(async { unreachable!("plugin turn must not compact") })
            }
        }

        smol::block_on(futures_lite::future::or(
            async {
                let (start, rx) = flume::bounded(1);
                let (actor, task) = AgentActorHandle::spawn_inner(
                    AgentId::generate(),
                    Vec::new(),
                    None,
                    Box::new(FinishingBackend),
                    None,
                    None,
                    Some(rx),
                );
                let reports = [delivery(AgentId::generate()), delivery(AgentId::generate())];
                let ids = reports
                    .iter()
                    .map(|report| report.result.outcome.turn_id())
                    .collect::<Vec<_>>();
                {
                    let mut state = actor.inner.state.lock().unwrap();
                    for report in &reports {
                        super::restore_pending(&mut state, Arc::clone(report), Vec::new());
                    }
                    state.delivery_held = true;
                    state.delivery_suspended = true;
                }
                let events = actor.subscribe_events();
                let provenance = TurnProvenance {
                    origin: crate::actor::TurnOrigin::Plugin,
                    plugin: Some(LABEL.into()),
                    ..Default::default()
                };
                let ticket = actor
                    .submit(
                        AgentInput::new(INSTRUCTION.into(), Vec::new()),
                        None,
                        CORRELATION.into(),
                        crate::actor::TurnAdmissionOptions {
                            provenance: provenance.clone(),
                            ..Default::default()
                        },
                    )
                    .unwrap();
                start.send(()).unwrap();
                let result = ticket.wait_result().await;
                assert!(matches!(result.outcome, TurnOutcome::Completed { .. }));
                assert_eq!(result.provenance, provenance);
                actor.runtime_barrier().await.unwrap();
                {
                    let state = actor.inner.state.lock().unwrap();
                    assert!(state.delivery_held);
                    assert!(state.delivery_runs.is_empty());
                    assert_eq!(state.deliveries.len(), reports.len());
                    assert_eq!(
                        state.delivery_order.iter().copied().collect::<Vec<_>>(),
                        ids
                    );
                    assert_eq!(state.delivery_seen.iter().copied().collect::<Vec<_>>(), ids);
                    for report in &reports {
                        let pending = &state.deliveries[&report.result.outcome.turn_id()];
                        assert_eq!(pending.delivery.to_json(), report.to_json());
                        assert!(!pending.cancellation_retried);
                        assert!(!pending.arrived_during_run);
                    }
                    assert!(!state.has_pending_work(&actor.inner.queue));
                    assert!(state.operations.is_empty());
                }
                assert_eq!(actor.latest_ticket().unwrap().turn_id(), ticket.turn_id());
                assert!(
                    events
                        .try_iter()
                        .all(|event| !matches!(event, ActorEvent::Delivery(_)))
                );
                actor.close();
                task.await;
            },
            async {
                smol::Timer::after(TURN_TIMEOUT).await;
                panic!("{TURN_TIMEOUT_MESSAGE}");
            },
        ));
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
        manager: &crate::AgentManagerHandle,
        parent: Option<&AgentActorHandle>,
    ) -> (AgentActorHandle, smol::Task<()>, flume::Sender<()>) {
        let id = AgentId::generate();
        let mut managed =
            manager.delivery_admission_for_test(id, parent.map_or(id, AgentActorHandle::agent_id));
        if parent.is_none() {
            managed.parent_delivery = None;
        }
        let (start, rx) = flume::bounded(1);
        let (actor, task) = AgentActorHandle::spawn_inner(
            id,
            Vec::new(),
            None,
            Box::new(UnusedBackend),
            Some(managed),
            None,
            Some(rx),
        );
        manager.register_delivery_actor_for_test(&actor);
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
    fn continuation_inherits_all_report_chains_at_capacity() {
        smol::block_on(async {
            let ledger = crate::manager::WorkLedger::new(crate::AgentLimits {
                max_active_agents: 4,
                ..Default::default()
            });
            let ancestor = AgentId::generate();
            let parent = AgentId::generate();
            ledger.register(ancestor, None, 0, true);
            ledger.register(parent, Some(ancestor), 0, true);
            let mut reports = Vec::new();
            for _ in 0..2 {
                let child = AgentId::generate();
                let admission = ledger.register(child, Some(parent), 0, true);
                let mut turn = admission.try_reserve_reporting(true).unwrap();
                reports.push((delivery(child), turn.take_report().unwrap()));
            }
            assert_eq!(ledger.active_agents(), 4);
            let (start, ready) = flume::bounded(1);
            let (actor, task) = AgentActorHandle::spawn_inner(
                parent,
                Vec::new(),
                None,
                Box::new(UnusedBackend),
                None,
                None,
                Some(ready),
            );
            for (delivery, work) in reports {
                super::admit_delivery_reserved(&actor.inner, delivery, vec![work]).unwrap();
            }
            let ticket = schedule_ticket(&actor);
            assert!(
                actor
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .turn_work
                    .get_mut(&ticket.turn_id())
                    .unwrap()
                    .iter_mut()
                    .all(|work| work.take_report().is_none())
            );
            assert_eq!(
                actor.inner.state.lock().unwrap().turn_work[&ticket.turn_id()].len(),
                2
            );
            assert_eq!(super::drain(&actor.inner, ticket.turn_id()).len(), 2);
            assert_eq!(ledger.active_agents(), 4);
            let mut work = actor
                .inner
                .state
                .lock()
                .unwrap()
                .turn_work
                .remove(&ticket.turn_id())
                .unwrap();
            let reports = work
                .iter_mut()
                .filter_map(crate::manager::WorkReservation::take_report)
                .collect::<Vec<_>>();
            assert_eq!(reports.len(), 2);
            assert!(reports.iter().all(|report| report.agent_id() == ancestor));
            drop(work);
            assert!(ledger.is_busy(parent));
            drop(reports);
            assert_eq!(ledger.active_agents(), 0);
            actor.shutdown();
            drop(start);
            task.await;
        });
    }

    #[test]
    fn nested_continuations_report_once_and_quiesce() {
        smol::block_on(async {
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (ancestor, ancestor_task, _ancestor_start) = deferred_actor(&manager, None);
            let (parent, parent_task, _parent_start) = deferred_actor(&manager, Some(&ancestor));
            let (child, child_task, _child_start) = deferred_actor(&manager, Some(&parent));
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (ancestor, ancestor_task, _ancestor_start) = deferred_actor(&manager, None);
            let (parent, parent_task, _parent_start) = deferred_actor(&manager, Some(&ancestor));
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (actor, task, _start) = deferred_actor(&manager, None);
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (actor, task, _start) = deferred_actor(&manager, None);
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
            let manager = crate::AgentManagerHandle::new(Default::default()).unwrap();
            let (actor, task, _start) = deferred_actor(&manager, None);
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
