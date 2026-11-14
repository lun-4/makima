use crate::{
    AgentId, HistorySnapshot,
    journal::{JournalError, SessionJournalSnapshot},
};
use makima_providers::Message;
use makima_storage::{
    id::MakimaId,
    session_state::{
        SessionStateView, StateChange, StoredAgentRecord, StoredDeliveryOp, StoredStateRecord,
    },
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewindBoundary {
    pub agent: AgentId,
    pub seq: u64,
    pub epoch: MakimaId,
}

#[derive(Debug, Error)]
pub enum RewindError {
    #[error("rewind boundary is no longer in the active journal")]
    StaleBoundary,
    #[error("rewind boundary crosses compaction for agent {0}")]
    Compacted(AgentId),
    #[error("rewind history is unavailable for agent {0}")]
    MissingHistory(AgentId),
    #[error("rewind publication no longer owns the journal")]
    LostReservation,
    #[error("invalid pinned rewind boundary: {0}")]
    InvalidPin(String),
    #[error(transparent)]
    State(#[from] crate::state::StateError),
    #[error(transparent)]
    Journal(#[from] JournalError),
}

#[derive(Deserialize, Serialize)]
struct BoundaryPin {
    state: Vec<StoredStateRecord>,
    agents: Vec<(u64, StoredAgentRecord)>,
    histories: BTreeMap<AgentId, PinnedHistory>,
}

#[derive(Deserialize, Serialize)]
struct PinnedHistory {
    epoch: u64,
    compacted: bool,
    messages: Vec<Message>,
}

pub use crate::journal::JournalReplacement;

pub(crate) fn subtree(view: &SessionStateView, root: AgentId) -> BTreeSet<AgentId> {
    let mut ids = BTreeSet::from([root]);
    loop {
        let before = ids.len();
        for agent in view.agents.values() {
            if agent.parent.is_some_and(|parent| ids.contains(&parent)) {
                ids.insert(agent.id);
            }
        }
        if ids.len() == before {
            return ids;
        }
    }
}

pub(crate) fn pin_outside_boundaries(
    current: &SessionJournalSnapshot,
    candidate: &mut SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
) -> Result<(), RewindError> {
    let view = SessionStateView::fold(&current.state, &current.agent_revisions, current.seq)
        .map_err(JournalError::from)?;
    for (seq, boundary) in &view.boundaries {
        if boundary
            .agent
            .is_some_and(|agent| affected.contains(&agent))
        {
            continue;
        }
        if !candidate.state.iter().any(|record| {
            record.seq == *seq && matches!(record.change, StateChange::Boundary { .. })
        }) {
            continue;
        }
        if let Some(record) = current.state.iter().rev().find(|record| matches!(&record.change, StateChange::BoundarySnapshot { boundary, .. } if boundary == seq)) {
            if !candidate.state.iter().any(|saved| saved.seq == record.seq) {
                candidate.state.push(record.clone());
            }
            continue;
        }
        if boundary.lens.iter().any(|(agent, (epoch, _))| {
            current
                .histories
                .get(agent)
                .is_some_and(|history| history.epoch != *epoch)
                || view
                    .compactions
                    .get(agent)
                    .is_some_and(|(barrier, _)| barrier > seq)
        }) {
            continue;
        }
        let mut histories = BTreeMap::new();
        let historical = SessionStateView::fold(&current.state, &current.agent_revisions, *seq)
            .map_err(JournalError::from)?;
        for agent in historical.agents.keys() {
            let history = current
                .histories
                .get(agent)
                .ok_or(RewindError::MissingHistory(*agent))?;
            let len = boundary
                .lens
                .get(agent)
                .map_or(history.messages.len(), |(_, len)| *len);
            if history.messages.len() < len {
                return Err(RewindError::MissingHistory(*agent));
            }
            histories.insert(
                *agent,
                PinnedHistory {
                    epoch: history.epoch,
                    compacted: history.compacted,
                    messages: history.messages[..len].to_vec(),
                },
            );
        }
        let pin = BoundaryPin {
            state: current
                .state
                .iter()
                .filter(|record| {
                    record.seq <= *seq
                        && !matches!(record.change, StateChange::BoundarySnapshot { .. })
                })
                .cloned()
                .collect(),
            agents: current
                .agent_revisions
                .iter()
                .filter(|(record_seq, _)| record_seq <= seq)
                .cloned()
                .collect(),
            histories,
        };
        let snapshot = serde_json::to_value(pin)
            .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
        push(
            candidate,
            None,
            0,
            StateChange::BoundarySnapshot {
                boundary: *seq,
                snapshot,
            },
        )?;
    }
    candidate.state.sort_by_key(|record| record.seq);
    Ok(())
}

fn pinned_snapshot(
    current: &SessionJournalSnapshot,
    seq: u64,
) -> Result<Option<SessionJournalSnapshot>, RewindError> {
    let Some(snapshot) = current
        .state
        .iter()
        .rev()
        .find_map(|record| match &record.change {
            StateChange::BoundarySnapshot { boundary, snapshot } if *boundary == seq => {
                Some(snapshot)
            }
            _ => None,
        })
    else {
        return Ok(None);
    };
    let pin: BoundaryPin = serde_json::from_value(snapshot.clone())
        .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
    if pin.state.iter().any(|record| {
        record.seq > seq || matches!(record.change, StateChange::BoundarySnapshot { .. })
    }) || pin.agents.iter().any(|(record_seq, _)| *record_seq > seq)
    {
        return Err(RewindError::InvalidPin(String::from(
            "pinned prefix exceeds its boundary",
        )));
    }
    let view = SessionStateView::fold(&pin.state, &pin.agents, seq).map_err(JournalError::from)?;
    let boundary = view
        .boundaries
        .get(&seq)
        .ok_or_else(|| RewindError::InvalidPin(String::from("pinned boundary is missing")))?;
    for (agent, (epoch, len)) in &boundary.lens {
        if pin
            .histories
            .get(agent)
            .is_none_or(|history| history.epoch != *epoch || history.messages.len() != *len)
        {
            return Err(RewindError::InvalidPin(String::from(
                "pinned history does not match boundary lens",
            )));
        }
    }
    if view
        .agents
        .keys()
        .any(|agent| !pin.histories.contains_key(agent))
    {
        return Err(RewindError::InvalidPin(String::from(
            "pinned agent history is missing",
        )));
    }
    let mut snapshot = current.clone();
    snapshot.state = pin.state;
    snapshot.agent_revisions = pin.agents;
    snapshot.histories = pin
        .histories
        .into_iter()
        .map(|(agent, history)| {
            let mut restored = HistorySnapshot::new(history.messages);
            restored.epoch = history.epoch;
            restored.compacted = history.compacted;
            (agent, restored)
        })
        .collect();
    snapshot.seq = seq;
    Ok(Some(snapshot))
}

pub(crate) fn recover_boundary_pins(
    state: &mut [StoredStateRecord],
) -> Result<BTreeSet<u64>, RewindError> {
    let mut pinned = BTreeSet::new();
    for record in state {
        let StateChange::BoundarySnapshot { boundary, snapshot } = &mut record.change else {
            continue;
        };
        let mut pin: BoundaryPin = serde_json::from_value(snapshot.clone())
            .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
        let view = SessionStateView::fold(&pin.state, &pin.agents, *boundary)
            .map_err(JournalError::from)?;
        let saved = view
            .boundaries
            .get(boundary)
            .ok_or_else(|| RewindError::InvalidPin("pinned boundary is missing".into()))?;
        if saved.lens.iter().any(|(agent, (epoch, len))| {
            pin.histories
                .get(agent)
                .is_none_or(|history| history.epoch != *epoch || history.messages.len() != *len)
        }) {
            return Err(RewindError::InvalidPin(
                "pinned history does not match boundary lens".into(),
            ));
        }
        let mut maps = BTreeMap::new();
        for (agent, history) in &mut pin.histories {
            let previous_epoch = history.epoch;
            let mut source = HistorySnapshot::new(std::mem::take(&mut history.messages));
            source.compacted = history.compacted;
            let (sanitized, offsets) = crate::session_recovery::sanitize(source);
            history.messages = sanitized.messages.as_ref().clone();
            history.epoch = sanitized.epoch;
            maps.insert(*agent, (previous_epoch, history.epoch, offsets));
        }
        for record in &mut pin.state {
            if let StateChange::Boundary { offset, lens } = &mut record.change {
                if let Some(agent) = record.agent
                    && let Some((previous, _, offsets)) = maps.get(&agent)
                    && lens.get(&agent).is_some_and(|(epoch, _)| epoch == previous)
                    && let Some(restored) = offsets.get(*offset)
                {
                    *offset = *restored;
                }
                for (agent, (epoch, len)) in lens {
                    if let Some((previous, restored, offsets)) = maps.get(agent)
                        && epoch == previous
                        && let Some(offset) = offsets.get(*len)
                    {
                        *epoch = *restored;
                        *len = *offset;
                    }
                }
            }
        }
        *snapshot = serde_json::to_value(pin)
            .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
        pinned.insert(*boundary);
    }
    Ok(pinned)
}

pub(crate) fn prepare_boundary(
    current: &SessionJournalSnapshot,
    boundary: RewindBoundary,
) -> Result<(SessionJournalSnapshot, BTreeSet<AgentId>, bool), RewindError> {
    if current.epoch != boundary.epoch {
        return Err(RewindError::StaleBoundary);
    }
    let view = SessionStateView::fold(&current.state, &current.agent_revisions, current.seq)
        .map_err(JournalError::from)?;
    view.boundaries
        .get(&boundary.seq)
        .filter(|saved| saved.agent == Some(boundary.agent))
        .ok_or(RewindError::StaleBoundary)?;
    let session_scope = view
        .agents
        .get(&boundary.agent)
        .ok_or(RewindError::StaleBoundary)?
        .parent
        .is_none();
    let affected = if session_scope {
        view.agents.keys().copied().collect()
    } else {
        subtree(&view, boundary.agent)
    };
    let pinned = pinned_snapshot(current, boundary.seq)?;
    let source = pinned.as_ref().unwrap_or(current);
    let historical = SessionStateView::fold(&source.state, &source.agent_revisions, boundary.seq)
        .map_err(JournalError::from)?;
    let saved = historical
        .boundaries
        .get(&boundary.seq)
        .filter(|saved| saved.agent == Some(boundary.agent))
        .ok_or(RewindError::StaleBoundary)?;
    let mut affected = affected;
    if session_scope {
        affected.extend(historical.agents.keys().copied());
    } else {
        affected.extend(subtree(&historical, boundary.agent));
    }
    let surviving: BTreeSet<_> = historical
        .agents
        .keys()
        .copied()
        .filter(|id| affected.contains(id))
        .collect();
    for agent in saved
        .lens
        .keys()
        .filter(|id| session_scope || affected.contains(id))
    {
        if pinned.is_none()
            && view
                .compactions
                .get(agent)
                .is_some_and(|(seq, _)| *seq > boundary.seq)
        {
            return Err(RewindError::Compacted(*agent));
        }
    }
    let mut candidate = current.clone();
    if pinned.is_some() {
        candidate.state.retain(|record| {
            !session_scope && record.agent.is_none_or(|id| !affected.contains(&id))
        });
        candidate.state.extend(
            source
                .state
                .iter()
                .filter(|record| {
                    record.seq <= boundary.seq
                        && (session_scope || record.agent.is_some_and(|id| affected.contains(&id)))
                })
                .cloned(),
        );
        candidate.state.sort_by_key(|record| record.seq);
        candidate
            .agent_revisions
            .retain(|(_, record)| !session_scope && !affected.contains(&record.id));
        candidate.agent_revisions.extend(
            source
                .agent_revisions
                .iter()
                .filter(|(seq, record)| *seq <= boundary.seq && affected.contains(&record.id))
                .cloned(),
        );
        candidate.agent_revisions.sort_by_key(|(seq, _)| *seq);
    } else {
        candidate.state.retain(|record| {
            record.seq <= boundary.seq
                || (!session_scope && record.agent.is_none_or(|id| !affected.contains(&id)))
        });
        candidate.agent_revisions.retain(|(seq, record)| {
            *seq <= boundary.seq || (!session_scope && !affected.contains(&record.id))
        });
    }
    if !session_scope {
        pin_outside_boundaries(current, &mut candidate, &affected)?;
    }
    let retained_boundaries: BTreeSet<_> = candidate
        .state
        .iter()
        .filter_map(|record| {
            matches!(record.change, StateChange::Boundary { .. }).then_some(record.seq)
        })
        .collect();
    for record in &current.state {
        if matches!(&record.change, StateChange::BoundarySnapshot { boundary, .. } if retained_boundaries.contains(boundary))
            && !candidate.state.iter().any(|saved| saved.seq == record.seq)
        {
            candidate.state.push(record.clone());
        }
    }
    candidate.state.sort_by_key(|record| record.seq);
    candidate
        .histories
        .retain(|agent, _| !affected.contains(agent) || surviving.contains(agent));
    candidate
        .managed_histories
        .retain(|agent| candidate.histories.contains_key(agent));
    for agent in surviving {
        let history = source
            .histories
            .get(&agent)
            .ok_or(RewindError::MissingHistory(agent))?;
        let len = saved
            .lens
            .get(&agent)
            .map(|(_, len)| *len)
            .unwrap_or(history.messages.len());
        if saved
            .lens
            .get(&agent)
            .is_some_and(|(epoch, _)| history.epoch != *epoch)
            || history.messages.len() < len
        {
            return Err(RewindError::Compacted(agent));
        }
        let mut restored = HistorySnapshot::new(history.messages[..len].to_vec());
        restored.compacted = history.compacted;
        restored.conversation = None;
        let previous_epoch = history.epoch;
        candidate.histories.insert(agent, restored);
        rebase_history_epoch(&mut candidate, agent, previous_epoch);
        candidate.managed_histories.insert(agent);
    }
    finalize_candidate(
        current,
        &mut candidate,
        &affected,
        session_scope,
        boundary.agent,
    )?;
    Ok((candidate, affected, session_scope))
}

pub(crate) fn rebase_history_epoch(
    candidate: &mut SessionJournalSnapshot,
    agent: AgentId,
    previous_epoch: u64,
) {
    let Some(history) = candidate.histories.get(&agent) else {
        return;
    };
    for record in &mut candidate.state {
        if let StateChange::Boundary { lens, .. } = &mut record.change
            && let Some((epoch, _)) = lens.get_mut(&agent)
            && *epoch == previous_epoch
        {
            *epoch = history.epoch;
        }
    }
}

pub(crate) fn finalize_candidate(
    current: &SessionJournalSnapshot,
    candidate: &mut SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
    session_scope: bool,
    target: AgentId,
) -> Result<(), RewindError> {
    let retained: BTreeSet<_> = candidate
        .state
        .iter()
        .filter_map(|record| {
            matches!(record.change, StateChange::Boundary { .. }).then_some(record.seq)
        })
        .collect();
    candidate.state.retain(|record| !matches!(&record.change, StateChange::BoundarySnapshot { boundary, .. } if !retained.contains(boundary)));
    candidate.epoch = MakimaId::generate();
    candidate.authoritative = true;
    candidate.revision = current
        .revision
        .checked_add(1)
        .ok_or(JournalError::SequenceExhausted)?;
    candidate.seq = current.seq.max(candidate.seq);
    let candidate_view =
        SessionStateView::fold(&candidate.state, &candidate.agent_revisions, u64::MAX)
            .map_err(JournalError::from)?;
    let current_view = SessionStateView::fold(&current.state, &current.agent_revisions, u64::MAX)
        .map_err(JournalError::from)?;
    let retained_turns: BTreeSet<_> = candidate
        .state
        .iter()
        .filter_map(|record| match &record.change {
            StateChange::Turn { turn, .. } => record.agent.map(|agent| (agent, *turn)),
            _ => None,
        })
        .collect();
    let mut changes = Vec::new();
    let addresses: BTreeSet<_> = current_view
        .scopes
        .iter()
        .chain(&candidate_view.scopes)
        .filter(|(agent, _)| session_scope || agent.is_some_and(|id| affected.contains(&id)))
        .flat_map(|(agent, scope)| scope.plugins.keys().map(move |key| (*agent, key.clone())))
        .collect();
    for (agent, key) in addresses {
        let saved = candidate_view
            .scopes
            .get(&agent)
            .and_then(|scope| scope.plugins.get(&key));
        let previous = current_view
            .scopes
            .get(&agent)
            .and_then(|scope| scope.plugins.get(&key));
        let version = saved.or(previous).map_or(1, |value| value.version);
        let value = saved.and_then(|value| value.value.clone());
        let generation = candidate_view
            .scopes
            .get(&agent)
            .map_or(0, |scope| scope.generation);
        changes.push((
            agent,
            generation,
            StateChange::Plugin {
                key,
                version,
                value,
            },
        ));
    }
    for (recipient, scope) in &candidate_view.scopes {
        for (id, op) in &scope.deliveries {
            let StoredDeliveryOp::Queued(delivery) = op else {
                continue;
            };
            let consumed_outside = !session_scope
                && recipient.is_some_and(|id| !affected.contains(&id))
                && current_view
                    .scopes
                    .get(recipient)
                    .and_then(|scope| scope.deliveries.get(id))
                    .is_some_and(|op| matches!(op, StoredDeliveryOp::Consumed));
            let removed_source = affected.contains(&id.0) && !retained_turns.contains(id);
            if consumed_outside || removed_source {
                changes.push((
                    *recipient,
                    scope.generation,
                    StateChange::Delivery {
                        id: *id,
                        op: if consumed_outside {
                            StoredDeliveryOp::Consumed
                        } else {
                            StoredDeliveryOp::Dropped
                        },
                    },
                ));
            } else if session_scope
                || recipient.is_some_and(|id| affected.contains(&id))
                || affected.contains(&id.0)
            {
                let mut delivery = delivery.clone();
                delivery.held = true;
                changes.push((
                    *recipient,
                    scope.generation,
                    StateChange::Delivery {
                        id: *id,
                        op: StoredDeliveryOp::Queued(delivery),
                    },
                ));
            }
        }
    }
    for (agent, generation, change) in changes {
        push(candidate, agent, generation, change)?;
    }
    let unmatched: BTreeSet<_> = candidate_view
        .scopes
        .iter()
        .filter(|(agent, _)| session_scope || agent.is_some_and(|id| affected.contains(&id)))
        .flat_map(|(agent, scope)| scope.admitted.keys().map(move |turn| (*agent, *turn)))
        .collect();
    candidate.state.retain(|record| !matches!(&record.change, StateChange::Admitted { turn, .. } if unmatched.contains(&(record.agent, *turn))));
    let lens = candidate
        .histories
        .iter()
        .map(|(agent, history)| (*agent, (history.epoch, history.messages.len())))
        .collect();
    let offset = candidate
        .histories
        .get(&target)
        .map_or(0, |history| history.messages.len());
    let generation = candidate_view
        .scopes
        .get(&Some(target))
        .map_or(0, |scope| scope.generation);
    push(
        candidate,
        Some(target),
        generation,
        StateChange::Boundary { offset, lens },
    )?;
    SessionStateView::fold(&candidate.state, &candidate.agent_revisions, candidate.seq)
        .map_err(JournalError::from)?;
    Ok(())
}

pub(crate) fn push(
    candidate: &mut SessionJournalSnapshot,
    agent: Option<AgentId>,
    generation: u64,
    change: StateChange,
) -> Result<(), JournalError> {
    candidate.seq = candidate
        .seq
        .checked_add(1)
        .ok_or(JournalError::SequenceExhausted)?;
    candidate.state.push(StoredStateRecord {
        seq: candidate.seq,
        agent,
        generation,
        change,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{RewindBoundary, RewindError};
    use crate::{
        AgentId, HistorySnapshot,
        actor::ActorHistory,
        agent::History,
        journal::{JournalChange, JournalCommit, SessionJournal},
        state::{StateChangeReason, StateError, StateScope, StateSlotSpec},
    };
    use makima_providers::Message;
    use makima_storage::session_state::{
        SessionStateView, StateChange, StoredAgentLifecycle, StoredAgentRecord,
    };
    use serde_json::{Value, json};
    use std::sync::Arc;
    use test_case::test_case;

    const SLOT: &str = "rewind/value";
    const BEFORE: &str = "before";
    const AFTER: &str = "after";
    const SUMMARY: &str = "compacted child summary";
    const CONTINUATION: &str = "child continuation";

    fn agent(id: AgentId, parent: Option<AgentId>) -> StoredAgentRecord {
        StoredAgentRecord {
            id,
            parent,
            depth: u32::from(parent.is_some()),
            transcript_key: parent.map_or_else(String::new, |_| id.to_string()),
            lifecycle: StoredAgentLifecycle::Open,
            ancestry_unknown: false,
            label: None,
            spawned_by_tool_use_id: None,
            spec: None,
            restorable: Ok(()),
            usage: Default::default(),
            created_seq: 0,
            updated_seq: 0,
            legacy: false,
        }
    }

    fn plugin(agent: Option<AgentId>, value: &str) -> JournalChange {
        JournalChange {
            agent,
            generation: 0,
            change: StateChange::Plugin {
                key: SLOT.into(),
                version: 1,
                value: Some(json!(value)),
            },
        }
    }

    fn fixture() -> (
        Arc<SessionJournal>,
        AgentId,
        AgentId,
        AgentId,
        RewindBoundary,
    ) {
        let journal = Arc::new(SessionJournal::default());
        let root = AgentId::generate();
        let child = AgentId::generate();
        let sibling = AgentId::generate();
        journal
            .commit(JournalCommit {
                agents: vec![
                    agent(root, None),
                    agent(child, Some(root)),
                    agent(sibling, Some(root)),
                ],
                histories: [root, child, sibling]
                    .into_iter()
                    .map(|id| (id, HistorySnapshot::new(vec![Message::user(BEFORE.into())])))
                    .collect(),
                changes: vec![
                    plugin(None, BEFORE),
                    plugin(Some(root), BEFORE),
                    plugin(Some(child), BEFORE),
                    plugin(Some(sibling), BEFORE),
                ],
                ..Default::default()
            })
            .unwrap();
        let lens = journal
            .snapshot()
            .histories
            .iter()
            .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
            .collect();
        journal
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(child),
                    generation: 0,
                    change: StateChange::Boundary { offset: 1, lens },
                }],
                ..Default::default()
            })
            .unwrap();
        let snapshot = journal.snapshot();
        let boundary = RewindBoundary {
            agent: child,
            seq: snapshot.seq,
            epoch: snapshot.epoch,
        };
        journal
            .commit(JournalCommit {
                changes: vec![
                    plugin(None, AFTER),
                    plugin(Some(root), AFTER),
                    plugin(Some(child), AFTER),
                    plugin(Some(sibling), AFTER),
                ],
                ..Default::default()
            })
            .unwrap();
        (journal, root, child, sibling, boundary)
    }

    #[test_case(false; "child_preserves_outside")]
    #[test_case(true; "root_replaces_whole_session")]
    fn replacement_is_exact_scoped_and_repeatable(root_scope: bool) {
        smol::block_on(async {
            let (journal, root, child, sibling, mut boundary) = fixture();
            if root_scope {
                let mut state = journal.lock();
                let record = state
                    .snapshot
                    .state
                    .iter_mut()
                    .find(|record| record.seq == boundary.seq)
                    .unwrap();
                record.agent = Some(root);
                boundary.agent = root;
            }
            let before = journal.snapshot();
            let outside: Vec<_> = before
                .state
                .iter()
                .filter(|record| record.seq > boundary.seq && record.agent != Some(child))
                .cloned()
                .collect();
            let mut replacement = journal.prepare_rewind(boundary.clone()).await.unwrap();
            let candidate = replacement.snapshot().clone();
            assert_eq!(journal.snapshot().seq, before.seq);
            assert!(candidate.seq > before.seq);
            assert_ne!(candidate.epoch, before.epoch);
            let view =
                SessionStateView::fold(&candidate.state, &candidate.agent_revisions, candidate.seq)
                    .unwrap();
            assert_eq!(
                view.scopes[&Some(child)].plugins[SLOT].value,
                Some(json!(BEFORE))
            );
            assert_eq!(
                view.scopes[&Some(sibling)].plugins[SLOT].value,
                Some(json!(if root_scope { BEFORE } else { AFTER }))
            );
            if !root_scope {
                for record in outside {
                    assert!(candidate.state.contains(&record));
                }
            }
            replacement.install().unwrap();
            let epoch = journal.snapshot().epoch;
            drop(replacement);
            boundary.epoch = epoch;
            let second = journal.prepare_rewind(boundary).await.unwrap();
            assert!(second.snapshot().seq > candidate.seq);
        });
    }

    #[test]
    fn failed_preparation_and_dropped_reservation_leave_live_state_unchanged() {
        smol::block_on(async {
            let (journal, _, _, _, boundary) = fixture();
            let before = journal.snapshot();
            let failed = journal
                .prepare_rewind_with(boundary.clone(), |_| Err(RewindError::LostReservation))
                .await;
            assert!(failed.is_err());
            assert_eq!(journal.snapshot().seq, before.seq);
            let replacement = journal.prepare_rewind(boundary).await.unwrap();
            assert!(
                journal
                    .commit(JournalCommit {
                        changes: vec![plugin(None, AFTER)],
                        ..Default::default()
                    })
                    .is_err()
            );
            drop(replacement);
            assert_eq!(journal.snapshot().epoch, before.epoch);
            journal
                .commit(JournalCommit {
                    changes: vec![plugin(None, AFTER)],
                    ..Default::default()
                })
                .unwrap();
        });
    }

    #[test_case(false; "unrelated_compaction_allowed")]
    #[test_case(true; "subtree_compaction_rejected")]
    fn scoped_compaction_barriers(affected: bool) {
        smol::block_on(async {
            let (journal, _, child, sibling, boundary) = fixture();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(if affected { child } else { sibling }),
                        generation: 0,
                        change: StateChange::Compaction { epoch: 2 },
                    }],
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(journal.prepare_rewind(boundary).await.is_err(), affected);
        });
    }

    #[test_case(false; "boundary_callback")]
    #[test_case(true; "branch_callback")]
    fn migration_callback_runs_outside_journal_mutex(branch: bool) {
        smol::block_on(async {
            let (journal, _, _, _, boundary) = fixture();
            let migrate = |_: &crate::journal::SessionJournalSnapshot| {
                assert!(journal.try_lock_for_rewind_test());
                Ok(Vec::new())
            };
            if branch {
                journal
                    .prepare_branch_restore_with(boundary.agent, journal.snapshot(), migrate)
                    .await
                    .unwrap();
            } else {
                journal
                    .prepare_rewind_with(boundary, migrate)
                    .await
                    .unwrap();
            }
        });
    }

    #[test]
    fn candidate_projection_is_scoped_atomic_and_does_not_publish() {
        smol::block_on(async {
            let (journal, _, child, sibling, boundary) = fixture();
            let mut replacement = journal.prepare_rewind(boundary).await.unwrap();
            let config = makima_storage::session_state::StoredAgentConfig {
                cwd: String::from("."),
                model: String::from(BEFORE),
                thinking: None,
                fast: false,
                workflow: false,
                mode: makima_storage::session_state::StoredModeSnapshot {
                    id: String::from(BEFORE),
                    label: String::from(BEFORE),
                    system_prompt: None,
                    restrict_write_to: None,
                    tools: None,
                },
                options: Default::default(),
            };
            let change = |agent| JournalChange {
                agent: Some(agent),
                generation: 1,
                change: StateChange::Config(config.clone()),
            };
            let before = replacement.snapshot().seq;
            assert!(
                replacement
                    .apply_configs(vec![change(child), change(sibling)])
                    .is_err()
            );
            assert_eq!(replacement.snapshot().seq, before);
            replacement.apply_configs(vec![change(child)]).unwrap();
            let view = SessionStateView::fold(
                &replacement.snapshot().state,
                &replacement.snapshot().agent_revisions,
                replacement.snapshot().seq,
            )
            .unwrap();
            assert_eq!(view.scopes[&Some(child)].config.as_ref(), Some(&config));
            assert!(
                !journal
                    .snapshot()
                    .state
                    .iter()
                    .any(|record| matches!(record.change, StateChange::Config(_)))
            );
            let before = replacement.snapshot().seq;
            assert!(
                replacement
                    .apply_migrations(vec![
                        plugin(Some(child), BEFORE),
                        plugin(Some(sibling), BEFORE)
                    ])
                    .is_err()
            );
            assert_eq!(replacement.snapshot().seq, before);
            replacement
                .apply_migrations(vec![plugin(Some(child), BEFORE)])
                .unwrap();
            let events = journal.subscribe_state_changes();
            replacement.install().unwrap();
            assert!(events.try_recv().is_err());
            replacement.notify();
            assert_eq!(events.try_recv().unwrap().agent, Some(child));
        });
    }

    #[test_case(false; "root_rewind_after_divergent_child")]
    #[test_case(true; "root_rewind_after_child_branch_and_restart")]
    fn preserved_root_boundary_restores_exact_child_timeline(branch_restore: bool) {
        use crate::actor::{TurnProvenance, TurnResult};
        use crate::{ToolOutput, TurnCancellationReason, TurnId, TurnOutcome};
        use makima_providers::TokenUsage;
        use makima_storage::id::MakimaId;
        use makima_storage::session_state::{StoredAgentConfig, StoredModeSnapshot};
        use makima_storage::sessions::Session;
        smol::block_on(async {
            let (journal, root, child, _, child_boundary) = fixture();
            let historical_config = StoredAgentConfig {
                cwd: String::from("/"),
                model: BEFORE.into(),
                thinking: None,
                fast: false,
                workflow: true,
                mode: StoredModeSnapshot {
                    id: BEFORE.into(),
                    label: BEFORE.into(),
                    system_prompt: None,
                    restrict_write_to: None,
                    tools: None,
                },
                options: Default::default(),
            };
            let turn = TurnId::generate();
            let result = TurnResult {
                outcome: TurnOutcome::cancelled(
                    child,
                    turn,
                    TokenUsage::default(),
                    1,
                    TurnCancellationReason::User,
                ),
                text: AFTER.into(),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 1,
            }
            .to_stored(MakimaId::generate(), None)
            .unwrap();
            let mut historical_agent = journal.agent(child).unwrap();
            historical_agent.label = Some(AFTER.into());
            historical_agent.lifecycle = StoredAgentLifecycle::Closed;
            let mut historical_history = HistorySnapshot::new(vec![
                Message::user(BEFORE.into()),
                Message::user(AFTER.into()),
            ]);
            historical_history.epoch = journal.snapshot().histories[&child].epoch;
            journal
                .commit(JournalCommit {
                    agents: vec![historical_agent],
                    histories: [(child, historical_history.clone())].into(),
                    changes: vec![
                        plugin(Some(child), AFTER),
                        JournalChange {
                            agent: Some(child),
                            generation: 0,
                            change: StateChange::Config(historical_config.clone()),
                        },
                        JournalChange {
                            agent: Some(child),
                            generation: 0,
                            change: StateChange::Turn {
                                turn,
                                result: result.clone(),
                            },
                        },
                    ],
                    ..Default::default()
                })
                .unwrap();
            let snapshot = journal.snapshot();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 1,
                            lens: snapshot
                                .histories
                                .iter()
                                .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                                .collect(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let root_boundary = journal.snapshot().seq;
            let branch = journal.snapshot();
            let mut child_rewind = journal.prepare_rewind(child_boundary).await.unwrap();
            child_rewind.install().unwrap();
            drop(child_rewind);
            journal
                .commit(JournalCommit {
                    histories: [(
                        child,
                        HistorySnapshot::new(vec![Message::user(String::from(SLOT))]),
                    )]
                    .into(),
                    changes: vec![plugin(Some(child), SLOT)],
                    ..Default::default()
                })
                .unwrap();
            if branch_restore {
                let mut restored = journal.prepare_branch_restore(child, branch).await.unwrap();
                restored.install().unwrap();
                drop(restored);
            }
            let dir = tempfile::tempdir().unwrap();
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(BEFORE, "/");
            journal.snapshot().apply_to(&mut session).unwrap();
            session.save_to(dir.path()).unwrap();
            let session = Session::load_from(session.id, dir.path()).unwrap();
            let recovered = crate::session_recovery::RecoveryCandidate::prepare(session)
                .unwrap()
                .finalize_after_publication();
            let journal = recovered.journal();
            let mut restored = journal
                .prepare_rewind(RewindBoundary {
                    agent: root,
                    seq: root_boundary,
                    epoch: journal.snapshot().epoch,
                })
                .await
                .unwrap();
            let view = SessionStateView::fold(
                &restored.snapshot().state,
                &restored.snapshot().agent_revisions,
                restored.snapshot().seq,
            )
            .unwrap();
            assert!(view.agents.contains_key(&child));
            assert_eq!(view.agents[&child].label.as_deref(), Some(AFTER));
            assert_eq!(view.agents[&child].lifecycle, StoredAgentLifecycle::Closed);
            assert_eq!(
                view.scopes[&Some(child)].config.as_ref(),
                Some(&historical_config)
            );
            assert_eq!(
                view.scopes[&Some(child)].results.back(),
                Some(&(turn, result))
            );
            assert_eq!(
                view.scopes[&Some(child)].plugins[SLOT].value,
                Some(json!(AFTER))
            );
            assert_eq!(
                restored.snapshot().histories[&child].messages,
                historical_history.messages
            );
            restored.install().unwrap();
        });
    }

    #[test]
    fn root_rewind_discards_orphan_boundary_pins() {
        smol::block_on(async {
            let (journal, root, _, _, _) = fixture();
            let snapshot = journal.snapshot();
            let lens = || {
                snapshot
                    .histories
                    .iter()
                    .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                    .collect()
            };
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 1,
                            lens: lens(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let early = journal.snapshot();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 1,
                            lens: lens(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let later = journal.snapshot().seq;
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::BoundarySnapshot {
                            boundary: later,
                            snapshot: json!({}),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let replacement = journal
                .prepare_rewind(RewindBoundary {
                    agent: root,
                    seq: early.seq,
                    epoch: early.epoch,
                })
                .await
                .unwrap();
            assert!(!replacement.snapshot().state.iter().any(|record| matches!(&record.change, StateChange::BoundarySnapshot { boundary, .. } if *boundary == later)));
        });
    }

    #[test]
    fn child_branch_preserves_revision_order_before_restored_boundaries() {
        smol::block_on(async {
            let (journal, _, child, _, _) = fixture();
            let branch = journal.snapshot();
            let mut replacement = journal.prepare_branch_restore(child, branch).await.unwrap();
            let snapshot = replacement.snapshot().clone();
            let boundary = snapshot
                .state
                .iter()
                .find(|record| {
                    record.agent == Some(child)
                        && matches!(record.change, StateChange::Boundary { .. })
                })
                .unwrap();
            let view =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, boundary.seq)
                    .unwrap();
            assert!(view.agents.contains_key(&child));
            replacement.install().unwrap();
            drop(replacement);
            let boundary = RewindBoundary {
                agent: child,
                seq: boundary.seq,
                epoch: snapshot.epoch,
            };
            let prepared = journal.prepare_rewind(boundary).await.unwrap();
            let view = SessionStateView::fold(
                &prepared.snapshot().state,
                &prepared.snapshot().agent_revisions,
                prepared.snapshot().seq,
            )
            .unwrap();
            assert!(view.agents.contains_key(&child));
            assert_eq!(
                view.scopes[&Some(child)].plugins[SLOT].value,
                Some(json!(BEFORE))
            );
        });
    }

    #[test_case(false; "compaction_barrier")]
    #[test_case(true; "epoch_mismatch_without_barrier")]
    fn root_boundary_rejects_actual_child_compaction(without_barrier: bool) {
        smol::block_on(async {
            let (journal, root, child, _, _) = fixture();
            let mut history = History::restored_exact(journal.snapshot().histories[&child].clone())
                .with_journal(child, Arc::clone(&journal));
            ActorHistory::new(&mut history)
                .push(Message::observation(AFTER.into()))
                .await
                .unwrap();
            let snapshot = journal.snapshot();
            let original = snapshot.histories[&child].messages.clone();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: snapshot.histories[&root].messages.len(),
                            lens: snapshot
                                .histories
                                .iter()
                                .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                                .collect(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let branch = journal.snapshot();
            let boundary = RewindBoundary {
                agent: root,
                seq: branch.seq,
                epoch: branch.epoch,
            };
            ActorHistory::new(&mut history)
                .replace_compacted(vec![
                    Message::user(BEFORE.into()),
                    Message::observation(SUMMARY.into()),
                ])
                .await
                .unwrap();
            let compacted = journal.snapshot();
            let barriers: Vec<_> = compacted.state.iter().filter(|record| {
                record.agent == Some(child)
                    && matches!(record.change, StateChange::Compaction { epoch } if epoch == compacted.histories[&child].epoch)
            }).collect();
            assert_eq!(barriers.len(), 1);
            assert_eq!(compacted.revision, branch.revision + 1);
            assert_ne!(
                compacted.histories[&child].epoch,
                branch.histories[&child].epoch
            );
            ActorHistory::new(&mut history)
                .push(Message::observation(CONTINUATION.into()))
                .await
                .unwrap();
            if without_barrier {
                let mut without_barrier = journal.snapshot();
                without_barrier.state.retain(|record| {
                    record.agent != Some(child)
                        || !matches!(record.change, StateChange::Compaction { .. })
                });
                let error =
                    super::prepare_boundary(&without_barrier, boundary.clone()).unwrap_err();
                assert!(
                    matches!(error, RewindError::Compacted(id) if id == child),
                    "{error:?}"
                );
            }
            let current = journal.snapshot();
            assert!(
                matches!(journal.preview_rewind(boundary.clone()), Err(RewindError::Compacted(id)) if id == child)
            );
            assert!(
                matches!(journal.prepare_rewind(boundary.clone()).await, Err(RewindError::Compacted(id)) if id == child)
            );
            assert_eq!(journal.snapshot().state, current.state);
            let mut restored = journal.prepare_branch_restore(root, branch).await.unwrap();
            restored.install().unwrap();
            drop(restored);
            let restored = journal.snapshot();
            assert_eq!(restored.histories[&child].messages, original);
            let rewound = journal
                .preview_rewind(RewindBoundary {
                    epoch: restored.epoch,
                    ..boundary
                })
                .unwrap();
            assert_eq!(rewound.histories[&child].messages, original);
        });
    }

    #[test]
    fn subtree_rewind_does_not_rebase_stale_unpinned_root_boundary() {
        smol::block_on(async {
            let (journal, root, child, _, _) = fixture();
            let snapshot = journal.snapshot();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: snapshot.histories[&root].messages.len(),
                            lens: snapshot
                                .histories
                                .iter()
                                .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                                .collect(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let root_seq = journal.snapshot().seq;
            let mut history = History::restored_exact(snapshot.histories[&child].clone())
                .with_journal(child, Arc::clone(&journal));
            ActorHistory::new(&mut history)
                .replace(vec![Message::observation(AFTER.into())])
                .await;
            ActorHistory::new(&mut history)
                .push(Message::user(CONTINUATION.into()))
                .await
                .unwrap();
            let current = journal.snapshot();
            let child_seq = current
                .state
                .iter()
                .rev()
                .find(|record| {
                    record.agent == Some(child)
                        && matches!(record.change, StateChange::Boundary { .. })
                })
                .unwrap()
                .seq;
            let mut replacement = journal
                .prepare_rewind(RewindBoundary {
                    agent: child,
                    seq: child_seq,
                    epoch: current.epoch,
                })
                .await
                .unwrap();
            replacement.install().unwrap();
            drop(replacement);
            let restored = journal.snapshot();
            assert!(!restored.state.iter().any(|record| matches!(record.change,
                StateChange::BoundarySnapshot { boundary, .. } if boundary == root_seq)));
            assert!(matches!(journal.preview_rewind(RewindBoundary {
                agent: root,
                seq: root_seq,
                epoch: restored.epoch,
            }), Err(RewindError::Compacted(id)) if id == child));
        });
    }

    #[test_case(false; "authoritative_pin_survives_compaction")]
    #[test_case(true; "pin_epoch_must_match_its_lens")]
    fn pinned_root_boundary_uses_original_child_history_after_compaction(corrupt_pin: bool) {
        smol::block_on(async {
            let (journal, root, child, _, child_boundary) = fixture();
            let mut history = History::restored_exact(journal.snapshot().histories[&child].clone())
                .with_journal(child, Arc::clone(&journal));
            ActorHistory::new(&mut history)
                .push(Message::observation(AFTER.into()))
                .await
                .unwrap();
            let snapshot = journal.snapshot();
            let original = snapshot.histories[&child].messages.clone();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: snapshot.histories[&root].messages.len(),
                            lens: snapshot
                                .histories
                                .iter()
                                .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                                .collect(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let root_seq = journal.snapshot().seq;
            let mut replacement = journal.prepare_rewind(child_boundary).await.unwrap();
            replacement.install().unwrap();
            drop(replacement);
            let mut history = History::restored_exact(journal.snapshot().histories[&child].clone())
                .with_journal(child, Arc::clone(&journal));
            ActorHistory::new(&mut history)
                .replace_compacted(vec![
                    Message::user(BEFORE.into()),
                    Message::observation(SUMMARY.into()),
                ])
                .await
                .unwrap();
            if corrupt_pin {
                let snapshot = journal.snapshot();
                let pin = snapshot
                    .state
                    .iter()
                    .find_map(|record| match &record.change {
                        StateChange::BoundarySnapshot { boundary, snapshot }
                            if *boundary == root_seq =>
                        {
                            Some(snapshot)
                        }
                        _ => None,
                    })
                    .unwrap();
                let mut decoded: super::BoundaryPin = serde_json::from_value(pin.clone()).unwrap();
                assert_ne!(decoded.histories[&child].epoch, history.snapshot().epoch);
                decoded.histories.get_mut(&child).unwrap().epoch = history.snapshot().epoch;
                journal
                    .commit(JournalCommit {
                        changes: vec![JournalChange {
                            agent: None,
                            generation: 0,
                            change: StateChange::BoundarySnapshot {
                                boundary: root_seq,
                                snapshot: serde_json::to_value(decoded).unwrap(),
                            },
                        }],
                        ..Default::default()
                    })
                    .unwrap();
            }
            let result = journal.preview_rewind(RewindBoundary {
                agent: root,
                seq: root_seq,
                epoch: journal.snapshot().epoch,
            });
            if corrupt_pin {
                let error = result.unwrap_err();
                assert!(matches!(error, RewindError::InvalidPin(_)), "{error:?}");
                let mut state = journal.snapshot().state;
                assert!(matches!(
                    super::recover_boundary_pins(&mut state),
                    Err(RewindError::InvalidPin(_))
                ));
            } else {
                assert_eq!(result.unwrap().histories[&child].messages, original);
            }
        });
    }

    #[test]
    fn child_rewind_preserves_consumed_drops_removed_and_holds_retained_reports() {
        use crate::actor::{TurnProvenance, TurnResult};
        use crate::{TurnCancellationReason, TurnId, TurnOutcome};
        use makima_providers::TokenUsage;
        use makima_storage::id::MakimaId;
        use makima_storage::session_state::{StoredDelivery, StoredDeliveryOp};
        smol::block_on(async {
            let (journal, root, child, _, _) = fixture();
            let stored = |turn| {
                TurnResult {
                    outcome: TurnOutcome::cancelled(
                        child,
                        turn,
                        TokenUsage::default(),
                        0,
                        TurnCancellationReason::User,
                    ),
                    text: BEFORE.into(),
                    output: Vec::new(),
                    provenance: TurnProvenance::default(),
                    duration_ms: 0,
                }
                .to_stored(MakimaId::generate(), None)
                .unwrap()
            };
            let retained = TurnId::generate();
            let consumed = TurnId::generate();
            let removed = TurnId::generate();
            let delivery = |turn| JournalChange {
                agent: Some(root),
                generation: 0,
                change: StateChange::Delivery {
                    id: (child, turn),
                    op: StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                        label: None,
                        result: stored(turn),
                        preview_bytes: None,
                        held: false,
                    })),
                },
            };
            journal
                .commit(JournalCommit {
                    changes: vec![
                        JournalChange {
                            agent: Some(child),
                            generation: 0,
                            change: StateChange::Turn {
                                turn: retained,
                                result: stored(retained),
                            },
                        },
                        delivery(retained),
                    ],
                    ..Default::default()
                })
                .unwrap();
            let snapshot = journal.snapshot();
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(child),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 1,
                            lens: snapshot
                                .histories
                                .iter()
                                .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                                .collect(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let snapshot = journal.snapshot();
            journal
                .commit(JournalCommit {
                    changes: vec![
                        delivery(consumed),
                        JournalChange {
                            agent: Some(root),
                            generation: 0,
                            change: StateChange::Delivery {
                                id: (child, consumed),
                                op: StoredDeliveryOp::Consumed,
                            },
                        },
                        delivery(removed),
                    ],
                    ..Default::default()
                })
                .unwrap();
            let mut prepared = journal
                .prepare_rewind(RewindBoundary {
                    agent: child,
                    seq: snapshot.seq,
                    epoch: snapshot.epoch,
                })
                .await
                .unwrap();
            prepared.install().unwrap();
            let installed = journal.snapshot();
            let view =
                SessionStateView::fold(&installed.state, &installed.agent_revisions, installed.seq)
                    .unwrap();
            let deliveries = &view.scopes[&Some(root)].deliveries;
            assert!(matches!(
                deliveries[&(child, consumed)],
                StoredDeliveryOp::Consumed
            ));
            assert!(matches!(
                deliveries[&(child, removed)],
                StoredDeliveryOp::Dropped
            ));
            assert!(
                matches!(&deliveries[&(child, retained)], StoredDeliveryOp::Queued(delivery) if delivery.held)
            );
        });
    }

    #[test_case(false; "notify_installed_journal_after_publication")]
    #[test_case(true; "dropped_handoff_emits_nothing")]
    fn offline_rewind_handoff_transfers_notices_without_installing(abort: bool) {
        use crate::{ToolOutput, session_recovery::RecoveryCandidate};
        use makima_providers::TokenUsage;
        use makima_storage::sessions::Session;
        smol::block_on(async {
            let (offline, _, child, _, boundary) = fixture();
            let before = offline.snapshot();
            let offline_events = offline.subscribe_state_changes();
            let replacement = offline.prepare_rewind(boundary).await.unwrap();
            let candidate = replacement.snapshot().clone();
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(BEFORE, "/");
            candidate.apply_to(&mut session).unwrap();
            let installed = RecoveryCandidate::from_exact(session, candidate.clone())
                .unwrap()
                .finalize_after_publication()
                .journal();
            let events = installed.subscribe_state_changes();
            let notice = replacement.prepare_handoff(Arc::clone(&installed)).unwrap();
            assert_eq!(offline.snapshot().state, before.state);
            assert_eq!(offline.snapshot().epoch, before.epoch);
            assert!(offline_events.try_recv().is_err());
            assert!(events.try_recv().is_err());
            assert_eq!(installed.snapshot().state, candidate.state);
            assert!(offline.try_publication_read().is_some());
            if abort {
                drop(notice);
                assert!(events.try_recv().is_err());
            } else {
                notice.notify();
                let event = events.try_recv().unwrap();
                assert_eq!(event.agent, Some(child));
                assert_eq!(event.reason, StateChangeReason::Rewind);
                assert!(events.try_recv().is_err());
            }
        });
    }

    #[test]
    fn mismatched_exact_journal_rejects_handoff_without_offline_install() {
        use crate::{ToolOutput, session_recovery::RecoveryCandidate};
        use makima_providers::TokenUsage;
        use makima_storage::sessions::Session;
        smol::block_on(async {
            let (offline, _, child, _, boundary) = fixture();
            let before = offline.snapshot();
            let replacement = offline.prepare_rewind(boundary).await.unwrap();
            let candidate = replacement.snapshot().clone();
            let mut session = Session::<Message, TokenUsage, ToolOutput>::new(BEFORE, "/");
            candidate.apply_to(&mut session).unwrap();
            let installed = RecoveryCandidate::from_exact(session, candidate)
                .unwrap()
                .finalize_after_publication()
                .journal();
            installed
                .commit(JournalCommit {
                    changes: vec![plugin(Some(child), AFTER)],
                    ..Default::default()
                })
                .unwrap();
            assert!(replacement.prepare_handoff(installed).is_err());
            assert_eq!(offline.snapshot().state, before.state);
            assert!(offline.try_publication_read().is_some());
        });
    }

    #[test]
    fn identical_rewind_fences_state_access_and_defers_scoped_notices() {
        smol::block_on(async {
            let (journal, _, child, sibling, boundary) = fixture();
            let slot = journal
                .register_state::<Value>(StateSlotSpec {
                    key: SLOT.into(),
                    scope: StateScope::Agent,
                    version: 1,
                })
                .unwrap();
            let child_access = journal.state_access(child, None);
            let sibling_access = journal.state_access(sibling, None);
            let events = journal.subscribe_state_changes();
            let mut replacement = journal.prepare_rewind(boundary).await.unwrap();
            replacement.install().unwrap();
            assert!(events.try_recv().is_err());
            assert!(matches!(
                slot.get(&child_access),
                Err(StateError::StaleAccess)
            ));
            assert_eq!(slot.get(&sibling_access).unwrap().value, Some(json!(AFTER)));
            replacement.notify();
            let event = events.try_recv().unwrap();
            assert_eq!(event.agent, Some(child));
            assert_eq!(event.reason, StateChangeReason::Rewind);
            assert!(events.try_recv().is_err());
        });
    }
}
