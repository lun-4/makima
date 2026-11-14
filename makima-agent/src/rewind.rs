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

const BOUNDARY_POOL_FORMAT: &str = "boundary-pool-v1";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    format: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    boundaries: Vec<u64>,
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
    prune_boundary_pins(candidate)?;
    carry_boundary_pins(current, candidate)?;
    let view = SessionStateView::fold(&current.state, &current.agent_revisions, current.seq)
        .map_err(JournalError::from)?;
    let retained = boundary_sequences(candidate);
    let covered = covered_boundaries(&candidate.state)?;
    let boundaries: Vec<_> = view
        .boundaries
        .iter()
        .filter_map(|(seq, boundary)| {
            if boundary
                .agent
                .is_some_and(|agent| affected.contains(&agent))
                || !retained.contains(seq)
                || covered.contains(seq)
                || current.agent_revisions.iter().any(|(revision, agent)| {
                    *revision <= *seq && !boundary.lens.contains_key(&agent.id)
                })
                || boundary.lens.iter().any(|(agent, (epoch, _))| {
                    current
                        .histories
                        .get(agent)
                        .is_some_and(|history| history.epoch != *epoch)
                        || view
                            .compactions
                            .get(agent)
                            .is_some_and(|(barrier, _)| barrier > seq)
                })
            {
                None
            } else {
                Some(*seq)
            }
        })
        .collect();
    let Some(&through) = boundaries.last() else {
        return Ok(());
    };
    let histories = current
        .histories
        .iter()
        .map(|(agent, history)| {
            (
                *agent,
                PinnedHistory {
                    epoch: history.epoch,
                    compacted: history.compacted,
                    messages: history.messages.as_ref().clone(),
                },
            )
        })
        .collect();
    let pin = BoundaryPin {
        format: Some(BOUNDARY_POOL_FORMAT.into()),
        boundaries,
        state: current
            .state
            .iter()
            .filter(|record| {
                record.seq <= through
                    && !matches!(&record.change, StateChange::BoundarySnapshot { .. })
            })
            .cloned()
            .collect(),
        agents: current
            .agent_revisions
            .iter()
            .filter(|(seq, _)| *seq <= through)
            .cloned()
            .collect(),
        histories,
    };
    validate_pool(&pin)?;
    let boundary = pin.boundaries[0];
    let snapshot =
        serde_json::to_value(pin).map_err(|error| RewindError::InvalidPin(error.to_string()))?;
    push(
        candidate,
        None,
        0,
        StateChange::BoundarySnapshot { boundary, snapshot },
    )?;
    candidate.state.sort_by_key(|record| record.seq);
    Ok(())
}

fn boundary_sequences(snapshot: &SessionJournalSnapshot) -> BTreeSet<u64> {
    snapshot
        .state
        .iter()
        .filter_map(|record| {
            matches!(&record.change, StateChange::Boundary { .. }).then_some(record.seq)
        })
        .collect()
}

fn pin_boundaries(boundary: u64, snapshot: &serde_json::Value) -> Result<Vec<u64>, RewindError> {
    match snapshot.get("format") {
        None => Ok(vec![boundary]),
        Some(format) if format.as_str() == Some(BOUNDARY_POOL_FORMAT) => {
            let boundaries: Vec<u64> =
                serde_json::from_value(snapshot.get("boundaries").cloned().ok_or_else(|| {
                    RewindError::InvalidPin("pool boundaries are missing".into())
                })?)
                .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
            if boundaries.first() != Some(&boundary)
                || boundaries.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(RewindError::InvalidPin(
                    "pool boundaries are not ordered or representative is invalid".into(),
                ));
            }
            Ok(boundaries)
        }
        Some(_) => Err(RewindError::InvalidPin(
            "unsupported boundary pool format".into(),
        )),
    }
}

pub(crate) fn has_boundary_pin(state: &[StoredStateRecord], seq: u64) -> Result<bool, RewindError> {
    for record in state {
        if let StateChange::BoundarySnapshot { boundary, snapshot } = &record.change
            && pin_boundaries(*boundary, snapshot)?.contains(&seq)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn covered_boundaries(state: &[StoredStateRecord]) -> Result<BTreeSet<u64>, RewindError> {
    let mut covered = BTreeSet::new();
    for record in state {
        if let StateChange::BoundarySnapshot { boundary, snapshot } = &record.change {
            covered.extend(pin_boundaries(*boundary, snapshot)?);
        }
    }
    Ok(covered)
}

pub(crate) fn carry_boundary_pins(
    source: &SessionJournalSnapshot,
    candidate: &mut SessionJournalSnapshot,
) -> Result<(), RewindError> {
    prune_boundary_pins(candidate)?;
    let retained = boundary_sequences(candidate);
    let mut covered = covered_boundaries(&candidate.state)?;
    for record in &source.state {
        let StateChange::BoundarySnapshot { boundary, snapshot } = &record.change else {
            continue;
        };
        let wanted: Vec<_> = pin_boundaries(*boundary, snapshot)?
            .into_iter()
            .filter(|seq| retained.contains(seq) && !covered.contains(seq))
            .collect();
        if wanted.is_empty() {
            continue;
        }
        let mut change = record.change.clone();
        if let StateChange::BoundarySnapshot { boundary, snapshot } = &mut change {
            *boundary = wanted[0];
            if snapshot.get("format").is_some() {
                snapshot["boundaries"] = serde_json::to_value(&wanted)
                    .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
            }
        }
        push(candidate, None, 0, change)?;
        covered.extend(wanted);
    }
    Ok(())
}

fn prune_boundary_pins(candidate: &mut SessionJournalSnapshot) -> Result<(), RewindError> {
    let retained = boundary_sequences(candidate);
    let mut keep = Vec::with_capacity(candidate.state.len());
    for mut record in std::mem::take(&mut candidate.state) {
        if let StateChange::BoundarySnapshot { boundary, snapshot } = &mut record.change {
            let wanted: Vec<_> = pin_boundaries(*boundary, snapshot)?
                .into_iter()
                .filter(|seq| retained.contains(seq))
                .collect();
            if wanted.is_empty() {
                continue;
            }
            *boundary = wanted[0];
            if snapshot.get("format").is_some() {
                snapshot["boundaries"] = serde_json::to_value(wanted)
                    .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
            }
        }
        keep.push(record);
    }
    candidate.state = keep;
    Ok(())
}

fn validate_pool(pin: &BoundaryPin) -> Result<(), RewindError> {
    let through = *pin
        .boundaries
        .last()
        .ok_or_else(|| RewindError::InvalidPin("pool is empty".into()))?;
    if pin.format.as_deref() != Some(BOUNDARY_POOL_FORMAT)
        || pin.boundaries.windows(2).any(|pair| pair[0] >= pair[1])
        || pin
            .state
            .iter()
            .any(|record| matches!(&record.change, StateChange::BoundarySnapshot { .. }))
    {
        return Err(RewindError::InvalidPin(
            "invalid nonrecursive boundary pool".into(),
        ));
    }
    let view =
        SessionStateView::fold(&pin.state, &pin.agents, u64::MAX).map_err(JournalError::from)?;
    for seq in &pin.boundaries {
        let saved = view
            .boundaries
            .get(seq)
            .ok_or_else(|| RewindError::InvalidPin("pooled boundary is missing".into()))?;
        if pin
            .agents
            .iter()
            .any(|(revision, agent)| *revision <= *seq && !saved.lens.contains_key(&agent.id))
        {
            return Err(RewindError::InvalidPin(
                "pooled boundary history cutoff is missing".into(),
            ));
        }
        if saved.lens.iter().any(|(agent, (epoch, len))| {
            pin.histories
                .get(agent)
                .is_none_or(|history| history.epoch != *epoch || history.messages.len() < *len)
        }) {
            return Err(RewindError::InvalidPin(
                "pooled history does not match boundary lens".into(),
            ));
        }
        if saved.agent.is_some_and(|agent| {
            saved
                .lens
                .get(&agent)
                .is_none_or(|(_, len)| saved.offset > *len)
        }) {
            return Err(RewindError::InvalidPin(
                "pooled boundary agent lens or offset is invalid".into(),
            ));
        }
    }
    if !view.boundaries.contains_key(&through)
        || view
            .agents
            .keys()
            .any(|agent| !pin.histories.contains_key(agent))
    {
        return Err(RewindError::InvalidPin(
            "pool boundary or agent history is missing".into(),
        ));
    }
    Ok(())
}

fn pinned_snapshot(
    current: &SessionJournalSnapshot,
    seq: u64,
) -> Result<Option<SessionJournalSnapshot>, RewindError> {
    let mut selected = None;
    for record in current.state.iter().rev() {
        if let StateChange::BoundarySnapshot { boundary, snapshot } = &record.change
            && pin_boundaries(*boundary, snapshot)?.contains(&seq)
        {
            selected = Some(snapshot);
            break;
        }
    }
    let Some(snapshot) = selected else {
        return Ok(None);
    };
    let mut pin: BoundaryPin = serde_json::from_value(snapshot.clone())
        .map_err(|error| RewindError::InvalidPin(error.to_string()))?;
    if pin.format.is_some() {
        validate_pool(&pin)?;
        pin.state.retain(|record| record.seq <= seq);
        pin.agents.retain(|(record_seq, _)| *record_seq <= seq);
        let view =
            SessionStateView::fold(&pin.state, &pin.agents, seq).map_err(JournalError::from)?;
        let saved = view
            .boundaries
            .get(&seq)
            .ok_or(RewindError::StaleBoundary)?;
        pin.histories
            .retain(|agent, _| view.agents.contains_key(agent));
        let mut remaps = BTreeMap::new();
        for (agent, history) in &mut pin.histories {
            let (epoch, len) = saved.lens.get(agent).copied().ok_or_else(|| {
                RewindError::InvalidPin("pooled boundary history cutoff is missing".into())
            })?;
            history.messages.truncate(len);
            let mut raw = HistorySnapshot::new(std::mem::take(&mut history.messages));
            raw.epoch = epoch;
            raw.compacted = history.compacted;
            let (sanitized, offsets) = crate::session_recovery::sanitize(raw);
            history.messages = sanitized.messages.as_ref().clone();
            history.epoch = sanitized.epoch;
            remaps.insert(*agent, (epoch, sanitized.epoch, offsets));
        }
        for record in &mut pin.state {
            if let StateChange::Boundary { offset, lens } = &mut record.change {
                if let Some(agent) = record.agent
                    && let Some((previous, _, offsets)) = remaps.get(&agent)
                    && lens.get(&agent).is_some_and(|(epoch, _)| epoch == previous)
                    && let Some(mapped) = offsets.get(*offset)
                {
                    *offset = *mapped;
                }
                for (agent, (epoch, len)) in lens {
                    if let Some((previous, restored, offsets)) = remaps.get(agent)
                        && epoch == previous
                        && let Some(mapped) = offsets.get(*len)
                    {
                        *epoch = *restored;
                        *len = *mapped;
                    }
                }
            }
        }
    }
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
        let covered = pin_boundaries(*boundary, snapshot)?;
        if pin.format.is_some() {
            validate_pool(&pin)?;
            pinned.extend(covered);
            continue;
        }
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
    prune_boundary_pins(&mut candidate)?;
    carry_boundary_pins(current, &mut candidate)?;
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
            .or_else(|| pinned.as_ref().map(|_| history.messages.len()))
            .ok_or(RewindError::MissingHistory(agent))?;
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
    prune_boundary_pins(candidate)?;
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
        journal::{JournalChange, JournalCommit, JournalError, SessionJournal},
        state::{StateChangeReason, StateError, StateScope, StateSlotSpec},
    };
    use makima_providers::{ContentBlock, Message, Role};
    use makima_storage::session_state::{
        SessionStateView, StateChange, StateFoldError, StoredAgentLifecycle, StoredAgentRecord,
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, sync::Arc};
    use test_case::test_case;

    const SLOT: &str = "rewind/value";
    const BEFORE: &str = "before";
    const AFTER: &str = "after";
    const SUMMARY: &str = "compacted child summary";
    const CONTINUATION: &str = "child continuation";
    const TOOL_A: &str = "call-a";
    const TOOL_B: &str = "call-b";

    #[test_case(false, false; "one_call")]
    #[test_case(true, false; "multiple_calls")]
    #[test_case(false, true; "dropped_empty_message")]
    #[test_case(true, true; "multiple_calls_and_dropped_empty_message")]
    fn grouped_pool_sanitizes_each_exact_raw_prefix(multiple_calls: bool, empty_message: bool) {
        let (journal, root, child, _, _) = fixture();
        let mut current = journal.snapshot();
        let calls = if multiple_calls {
            vec![TOOL_A, TOOL_B]
        } else {
            vec![TOOL_A]
        };
        let mut raw = Vec::new();
        if empty_message {
            raw.push(Message {
                role: Role::User,
                ..Default::default()
            });
        }
        raw.push(Message {
            role: Role::Assistant,
            content: calls
                .iter()
                .map(|id| ContentBlock::ToolUse {
                    id: (*id).into(),
                    name: SLOT.into(),
                    input: json!({}),
                    thought_signature: None,
                })
                .collect(),
            ..Default::default()
        });
        let first_len = raw.len();
        raw.push(Message {
            role: Role::User,
            content: calls
                .iter()
                .map(|id| ContentBlock::ToolResult {
                    tool_use_id: (*id).into(),
                    content: AFTER.into(),
                    is_error: false,
                })
                .collect(),
            ..Default::default()
        });
        current
            .histories
            .insert(root, HistorySnapshot::new(raw.clone()));
        let mut boundaries = Vec::new();
        for len in [first_len, raw.len()] {
            let mut lens: BTreeMap<_, _> = current
                .histories
                .iter()
                .map(|(agent, history)| (*agent, (history.epoch, history.messages.len())))
                .collect();
            lens.get_mut(&root).unwrap().1 = len;
            super::push(
                &mut current,
                Some(root),
                0,
                StateChange::Boundary { offset: len, lens },
            )
            .unwrap();
            boundaries.push(current.seq);
        }
        let mut candidate = current.clone();
        super::pin_outside_boundaries(&current, &mut candidate, &[child].into()).unwrap();
        assert_eq!(
            candidate
                .state
                .iter()
                .filter(|record| matches!(record.change, StateChange::BoundarySnapshot { .. }))
                .count(),
            1
        );
        let original = candidate.state.clone();
        let recovered = super::recover_boundary_pins(&mut candidate.state).unwrap();
        assert_eq!(candidate.state, original);
        assert_eq!(recovered, boundaries.iter().copied().collect());
        for (seq, len) in boundaries.iter().zip([first_len, raw.len()]) {
            let selected = super::pinned_snapshot(&candidate, *seq).unwrap().unwrap();
            let (expected, _) =
                crate::session_recovery::sanitize(HistorySnapshot::new(raw[..len].to_vec()));
            assert_eq!(selected.histories[&root].messages, expected.messages);
            let results = selected.histories[&root].messages.last().unwrap();
            for block in &results.content {
                let ContentBlock::ToolResult {
                    content, is_error, ..
                } = block
                else {
                    panic!("expected tool result");
                };
                assert_eq!(*is_error, len == first_len);
                assert_eq!(
                    content,
                    if len == first_len {
                        crate::agent::UNAVAILABLE_RESULT
                    } else {
                        AFTER
                    }
                );
            }
        }
        let mut carried = current.clone();
        carried.state.retain(|record| record.seq != boundaries[0]);
        super::carry_boundary_pins(&candidate, &mut carried).unwrap();
        assert!(!super::has_boundary_pin(&carried.state, boundaries[0]).unwrap());
        assert!(super::has_boundary_pin(&carried.state, boundaries[1]).unwrap());
        assert!(
            super::pinned_snapshot(&carried, boundaries[1])
                .unwrap()
                .is_some()
        );
        super::carry_boundary_pins(&candidate, &mut carried).unwrap();
        assert_eq!(
            carried
                .state
                .iter()
                .filter(|record| matches!(record.change, StateChange::BoundarySnapshot { .. }))
                .count(),
            1
        );
        carried.state.retain(|record| record.seq != boundaries[1]);
        super::prune_boundary_pins(&mut carried).unwrap();
        assert!(
            !carried
                .state
                .iter()
                .any(|record| matches!(record.change, StateChange::BoundarySnapshot { .. }))
        );
    }

    #[test_case(StoredAgentLifecycle::Closed; "closed_history")]
    #[test_case(StoredAgentLifecycle::Retired; "retired_history")]
    fn grouped_pool_freezes_terminal_history_before_late_cleanup_after_restart(
        lifecycle: StoredAgentLifecycle,
    ) {
        use crate::file_restore::StoredSession;
        smol::block_on(async {
            let (journal, root, _, sibling, child_boundary) = fixture();
            let mut terminal = journal.agent(sibling).unwrap();
            terminal.lifecycle = lifecycle.clone();
            let terminal_history = HistorySnapshot::new(vec![
                Message::user(BEFORE.into()),
                Message::observation(AFTER.into()),
            ]);
            journal
                .commit(JournalCommit {
                    agents: vec![terminal],
                    histories: [(sibling, terminal_history.clone())].into(),
                    ..Default::default()
                })
                .unwrap();
            let mut outside = Vec::new();
            let mut expected = Vec::new();
            let mut root_history =
                History::restored_exact(journal.snapshot().histories[&root].clone())
                    .with_journal(root, Arc::clone(&journal));
            for turn in 0..2 {
                expected.push(journal.snapshot().histories[&sibling].messages.clone());
                ActorHistory::new(&mut root_history)
                    .push(Message::user(CONTINUATION.into()))
                    .await
                    .unwrap();
                let snapshot = journal.snapshot();
                let boundary = snapshot
                    .state
                    .iter()
                    .rev()
                    .find(|record| {
                        record.agent == Some(root)
                            && matches!(record.change, StateChange::Boundary { .. })
                    })
                    .unwrap();
                let StateChange::Boundary { lens, .. } = &boundary.change else {
                    unreachable!()
                };
                assert_eq!(
                    lens[&sibling],
                    (terminal_history.epoch, expected[turn].len())
                );
                outside.push(boundary.seq);
                let mut late = snapshot.histories[&sibling].clone();
                Arc::make_mut(&mut late.messages).push(Message::observation(AFTER.into()));
                journal
                    .commit(JournalCommit {
                        histories: [(sibling, late)].into(),
                        ..Default::default()
                    })
                    .unwrap();
            }
            let mut replacement = journal.prepare_rewind(child_boundary).await.unwrap();
            assert!(super::has_boundary_pin(&replacement.snapshot().state, outside[1]).unwrap());
            replacement.install().unwrap();
            drop(replacement);
            let directory = tempfile::tempdir().unwrap();
            let mut session = StoredSession::new(BEFORE, "/");
            journal.snapshot().apply_to(&mut session).unwrap();
            session.save_to(directory.path()).unwrap();
            let loaded = StoredSession::load_from(session.id, directory.path()).unwrap();
            let recovery = crate::session_recovery::RecoveryCandidate::prepare(loaded).unwrap();
            let mut published = recovery.session().clone();
            published.save_to(directory.path()).unwrap();
            let reloaded = StoredSession::load_from(session.id, directory.path()).unwrap();
            let reloaded_journal = SessionJournal::from_session(&reloaded).unwrap();
            assert!(
                super::has_boundary_pin(&reloaded_journal.snapshot().state, outside[1]).unwrap()
            );
            let recovered = recovery.finalize_after_publication();
            let restarted = recovered.journal();
            let earlier = outside[0];
            let later = outside[1];
            for (seq, expected) in outside.into_iter().zip(expected).rev() {
                let mut replacement = restarted
                    .prepare_rewind(RewindBoundary {
                        agent: root,
                        seq,
                        epoch: restarted.snapshot().epoch,
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    replacement.snapshot().histories[&sibling].messages,
                    expected
                );
                replacement.install().unwrap();
                replacement.notify();
                drop(replacement);
                let selected = restarted.snapshot();
                let view = SessionStateView::fold(
                    &selected.state,
                    &selected.agent_revisions,
                    selected.seq,
                )
                .unwrap();
                assert_eq!(view.agents[&sibling].lifecycle, lifecycle);
                assert_eq!(selected.histories[&sibling].messages, expected);
                assert!(super::has_boundary_pin(&selected.state, earlier).unwrap());
                assert_eq!(
                    super::has_boundary_pin(&selected.state, later).unwrap(),
                    seq == later
                );
            }
        });
    }

    #[test_case(false; "ambiguous_legacy_boundary_is_unavailable")]
    #[test_case(true; "exact_legacy_singleton_stays_available")]
    fn missing_terminal_cutoff_never_uses_late_history(exact_legacy_pin: bool) {
        let (journal, root, child, sibling, _) = fixture();
        let mut terminal = journal.agent(sibling).unwrap();
        terminal.lifecycle = StoredAgentLifecycle::Closed;
        journal
            .commit(JournalCommit {
                agents: vec![terminal],
                ..Default::default()
            })
            .unwrap();
        let mut current = journal.snapshot();
        let lens = current
            .histories
            .iter()
            .filter(|(agent, _)| **agent != sibling)
            .map(|(agent, history)| (*agent, (history.epoch, history.messages.len())))
            .collect();
        super::push(
            &mut current,
            Some(root),
            0,
            StateChange::Boundary { offset: 1, lens },
        )
        .unwrap();
        let seq = current.seq;
        let exact = current.clone();
        Arc::make_mut(&mut current.histories.get_mut(&sibling).unwrap().messages)
            .push(Message::observation(AFTER.into()));
        let mut candidate = current.clone();
        super::pin_outside_boundaries(&current, &mut candidate, &[child].into()).unwrap();
        assert!(!super::has_boundary_pin(&candidate.state, seq).unwrap());
        if exact_legacy_pin {
            let pin = super::BoundaryPin {
                format: None,
                boundaries: Vec::new(),
                state: exact.state,
                agents: exact.agent_revisions,
                histories: exact
                    .histories
                    .iter()
                    .map(|(agent, history)| {
                        (
                            *agent,
                            super::PinnedHistory {
                                epoch: history.epoch,
                                compacted: history.compacted,
                                messages: history.messages.as_ref().clone(),
                            },
                        )
                    })
                    .collect(),
            };
            super::push(
                &mut candidate,
                None,
                0,
                StateChange::BoundarySnapshot {
                    boundary: seq,
                    snapshot: serde_json::to_value(pin).unwrap(),
                },
            )
            .unwrap();
            super::recover_boundary_pins(&mut candidate.state).unwrap();
        }
        let result = super::prepare_boundary(
            &candidate,
            RewindBoundary {
                agent: root,
                seq,
                epoch: candidate.epoch,
            },
        );
        if exact_legacy_pin {
            assert_eq!(result.unwrap().0.histories[&sibling].messages.len(), 1);
        } else {
            assert!(matches!(result, Err(RewindError::MissingHistory(agent)) if agent == sibling));
        }
    }

    #[test]
    fn legacy_singleton_pin_remains_readable_and_recoverable() {
        let (journal, root, child, _, _) = fixture();
        let mut current = journal.snapshot();
        let lens = current
            .histories
            .iter()
            .map(|(agent, history)| (*agent, (history.epoch, history.messages.len())))
            .collect();
        super::push(
            &mut current,
            Some(root),
            0,
            StateChange::Boundary { offset: 1, lens },
        )
        .unwrap();
        let seq = current.seq;
        let mut candidate = current.clone();
        super::pin_outside_boundaries(&current, &mut candidate, &[child].into()).unwrap();
        for record in &mut candidate.state {
            if let StateChange::BoundarySnapshot { snapshot, .. } = &mut record.change {
                let object = snapshot.as_object_mut().unwrap();
                object.remove("format");
                object.remove("boundaries");
            }
        }
        assert_eq!(
            super::pinned_snapshot(&candidate, seq)
                .unwrap()
                .unwrap()
                .histories[&root]
                .messages,
            current.histories[&root].messages
        );
        assert_eq!(
            super::recover_boundary_pins(&mut candidate.state).unwrap(),
            [seq].into()
        );
        assert!(super::pinned_snapshot(&candidate, seq).unwrap().is_some());
    }

    #[test_case(false; "out_of_range_lens")]
    #[test_case(true; "out_of_range_owner_offset")]
    fn grouped_pool_rejects_invalid_raw_prefix_before_sanitation(invalid_offset: bool) {
        let (journal, root, child, _, _) = fixture();
        let mut current = journal.snapshot();
        let lens = current
            .histories
            .iter()
            .map(|(agent, history)| (*agent, (history.epoch, history.messages.len())))
            .collect();
        super::push(
            &mut current,
            Some(root),
            0,
            StateChange::Boundary { offset: 1, lens },
        )
        .unwrap();
        let seq = current.seq;
        let mut candidate = current.clone();
        super::pin_outside_boundaries(&current, &mut candidate, &[child].into()).unwrap();
        for record in &mut candidate.state {
            if let StateChange::BoundarySnapshot { snapshot, .. } = &mut record.change {
                let mut pin: super::BoundaryPin = serde_json::from_value(snapshot.clone()).unwrap();
                let saved = pin
                    .state
                    .iter_mut()
                    .find(|record| record.seq == seq)
                    .unwrap();
                let StateChange::Boundary { offset, lens } = &mut saved.change else {
                    unreachable!()
                };
                if invalid_offset {
                    *offset += 1;
                } else {
                    lens.get_mut(&root).unwrap().1 += 1;
                }
                *snapshot = serde_json::to_value(pin).unwrap();
            }
        }
        assert!(matches!(
            super::pinned_snapshot(&candidate, seq),
            Err(RewindError::InvalidPin(_))
        ));
        assert!(matches!(
            super::recover_boundary_pins(&mut candidate.state),
            Err(RewindError::InvalidPin(_))
        ));
    }

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
            let recovery = crate::session_recovery::RecoveryCandidate::prepare(session).unwrap();
            let mut published = recovery.session().clone();
            published.save_to(dir.path()).unwrap();
            let reloaded =
                Session::<Message, TokenUsage, ToolOutput>::load_from(published.id, dir.path())
                    .unwrap();
            assert!(super::has_boundary_pin(reloaded.state(), root_boundary).unwrap());
            let recovered = recovery.finalize_after_publication();
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
                let change = StateChange::BoundarySnapshot {
                    boundary: root_seq,
                    snapshot: serde_json::to_value(decoded).unwrap(),
                };
                let error = journal
                    .commit(JournalCommit {
                        changes: vec![JournalChange {
                            agent: None,
                            generation: 0,
                            change: change.clone(),
                        }],
                        ..Default::default()
                    })
                    .unwrap_err();
                assert!(
                    matches!(error, JournalError::InvalidState(StateFoldError::InvalidBoundarySnapshot { boundary, .. }) if boundary == root_seq)
                );
                let mut corrupted = journal.snapshot();
                assert_eq!(corrupted.state, snapshot.state);
                super::push(&mut corrupted, None, 0, change).unwrap();
                assert!(matches!(
                    super::pinned_snapshot(&corrupted, root_seq),
                    Err(RewindError::InvalidPin(_))
                ));
                assert!(matches!(
                    super::recover_boundary_pins(&mut corrupted.state),
                    Err(RewindError::InvalidPin(_))
                ));
            }
            let result = journal.preview_rewind(RewindBoundary {
                agent: root,
                seq: root_seq,
                epoch: journal.snapshot().epoch,
            });
            assert_eq!(result.unwrap().histories[&child].messages, original);
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
