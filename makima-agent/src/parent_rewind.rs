use crate::ToolOutput;
use crate::journal::SessionJournal;
use crate::rewind::{RewindBoundary, RewindError};
use makima_providers::{Message, TokenUsage};
use makima_storage::session_state::{ParentLink, StateChange};
use makima_storage::sessions::Session;
use thiserror::Error;

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

const APPROVAL_POINT: &str = "Approval point";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentRewindEntry {
    pub seq: u64,
    pub offset: usize,
    pub preview: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ParentRewindError {
    #[error("Parent link does not identify this session")]
    SessionMismatch,
    #[error("Parent session has no root agent")]
    MissingRoot,
    #[error("Parent session has no retained root boundaries")]
    NoBoundaries,
    #[error("Parent session has no retained boundaries at or before approval")]
    NoBoundariesBeforeLink,
    #[error("Parent boundaries at or before approval cross compaction")]
    Compacted,
    #[error("Parent boundaries at or before approval no longer have available histories")]
    Unavailable,
    #[error("Cannot read parent rewind state: {0}")]
    Invalid(String),
}

pub fn boundaries(
    parent: &StoredSession,
    link: &ParentLink,
) -> Result<Vec<ParentRewindEntry>, ParentRewindError> {
    if parent.id != link.session {
        return Err(ParentRewindError::SessionMismatch);
    }
    let root = parent
        .root_agent_id()
        .ok_or(ParentRewindError::MissingRoot)?;
    let view = parent
        .latest_state()
        .map_err(|error| ParentRewindError::Invalid(error.to_string()))?;
    let root_boundaries: Vec<_> = view
        .boundaries
        .iter()
        .filter(|(_, boundary)| boundary.agent == Some(root))
        .collect();
    if root_boundaries.is_empty() {
        return Err(ParentRewindError::NoBoundaries);
    }
    let eligible: Vec<_> = root_boundaries
        .into_iter()
        .filter(|(seq, _)| **seq <= link.seq)
        .collect();
    if eligible.is_empty() {
        return Err(ParentRewindError::NoBoundariesBeforeLink);
    }
    let journal = SessionJournal::from_session(parent)
        .map_err(|error| ParentRewindError::Invalid(error.to_string()))?;
    let epoch = journal.snapshot().epoch;
    let mut entries = Vec::new();
    let mut compacted = 0;
    for (seq, boundary) in eligible.iter().rev().copied() {
        let candidate = match journal.preview_rewind(RewindBoundary {
            agent: root,
            seq: *seq,
            epoch,
        }) {
            Ok(candidate) => candidate,
            Err(RewindError::Compacted(agent)) => {
                if view
                    .compactions
                    .get(&agent)
                    .is_some_and(|(barrier, _)| *barrier > *seq)
                {
                    compacted += 1;
                }
                continue;
            }
            Err(RewindError::MissingHistory(_) | RewindError::StaleBoundary) => continue,
            Err(error) => return Err(ParentRewindError::Invalid(error.to_string())),
        };
        let Some(history) = candidate.histories.get(&root) else {
            continue;
        };
        let pinned = parent.state().iter().any(|record| {
            matches!(record.change,
            StateChange::BoundarySnapshot { boundary, .. } if boundary == *seq)
        });
        let preview = (!pinned)
            .then(|| {
                parent
                    .messages()
                    .get(boundary.offset)
                    .and_then(Message::user_text)
            })
            .flatten()
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| {
                if *seq == link.seq {
                    APPROVAL_POINT.into()
                } else {
                    format!("Boundary #{seq}")
                }
            });
        entries.push(ParentRewindEntry {
            seq: *seq,
            offset: history.messages.len(),
            preview,
        });
    }
    if entries.is_empty() {
        return Err(if compacted == eligible.len() {
            ParentRewindError::Compacted
        } else {
            ParentRewindError::Unavailable
        });
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::{APPROVAL_POINT, ParentRewindError, StoredSession, boundaries};
    use crate::{AgentId, HistorySnapshot};
    use makima_providers::Message;
    use makima_storage::id::MakimaId;
    use makima_storage::session_state::{ParentLink, StateChange, StoredStateRecord};
    use serde_json::json;
    use test_case::test_case;

    const FIRST: &str = "first planning turn";
    const SECOND: &str = "second planning turn";

    fn fixture() -> (StoredSession, ParentLink, AgentId) {
        let mut session = StoredSession::new("test-model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message::user(FIRST.into()));
        session.push_message(Message::user(SECOND.into()));
        for (seq, offset) in [(1, 0), (2, 1), (3, 2), (4, 2)] {
            session.push_state(StoredStateRecord {
                seq,
                agent: Some(root),
                generation: 0,
                change: StateChange::Boundary {
                    offset,
                    lens: [(root, (session.history_snapshot().epoch, offset))].into(),
                },
            });
        }
        let link = ParentLink {
            session: session.id,
            message_count: session.messages().len(),
            seq: 3,
        };
        (session, link, root)
    }

    #[test_case(false; "live")]
    #[test_case(true; "restart")]
    fn inclusive_ceiling_keeps_distinct_sequences_and_end_anchor(restart: bool) {
        let (mut session, mut link, _) = fixture();
        link.seq = 4;
        let directory = tempfile::tempdir().unwrap();
        if restart {
            session.save_to(directory.path()).unwrap();
            session = StoredSession::load_from(session.id, directory.path()).unwrap();
        }
        let entries = boundaries(&session, &link).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.seq, entry.offset))
                .collect::<Vec<_>>(),
            [(4, 2), (3, 2), (2, 1), (1, 0)]
        );
        assert_eq!(entries[0].preview, APPROVAL_POINT);
        assert_eq!(entries[1].preview, "Boundary #3");
        assert_eq!(entries[2].preview, SECOND);
        link.seq = 2;
        assert_eq!(
            boundaries(&session, &link)
                .unwrap()
                .iter()
                .map(|entry| entry.seq)
                .collect::<Vec<_>>(),
            [2, 1]
        );
    }

    #[test_case(false; "rewound_parent")]
    #[test_case(true; "later_root_boundary_only")]
    fn missing_retained_boundary_reasons_are_distinct(later: bool) {
        let (mut session, link, _) = fixture();
        session.replace_state(
            session
                .state()
                .iter()
                .filter(|record| later && record.seq > link.seq)
                .cloned()
                .collect(),
        );
        assert_eq!(
            boundaries(&session, &link),
            Err(if later {
                ParentRewindError::NoBoundariesBeforeLink
            } else {
                ParentRewindError::NoBoundaries
            })
        );
    }

    #[test_case(false; "missing_child_transcript")]
    #[test_case(true; "short_child_transcript")]
    fn missing_child_history_is_unavailable_not_compacted(short: bool) {
        let (mut session, mut link, root) = fixture();
        let child = AgentId::generate();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = child.to_string();
        record.created_seq = 1;
        record.updated_seq = 1;
        session.push_agent_revision(1, record);
        link.seq += 1;
        let state = session
            .state()
            .iter()
            .cloned()
            .map(|mut record| {
                record.seq += 1;
                if let StateChange::Boundary { lens, .. } = &mut record.change {
                    lens.insert(child, (0, 2));
                }
                record
            })
            .collect();
        session.replace_state(state);
        if short {
            session.set_subagent_messages(child.to_string(), vec![Message::user(FIRST.into())]);
        }
        assert!(session.latest_state().unwrap().compactions.is_empty());
        assert_eq!(
            boundaries(&session, &link),
            Err(ParentRewindError::Unavailable)
        );
    }

    #[test_case(false; "restart_preserves_stale_epoch")]
    #[test_case(true; "recovery_preserves_stale_epoch")]
    fn replaced_history_cannot_revalidate_old_lenses_after_restart(recover: bool) {
        let (mut session, link, _) = fixture();
        let mut history = HistorySnapshot::new(session.messages().to_vec());
        history.compacted = true;
        session.adopt_history(&history);
        let directory = tempfile::tempdir().unwrap();
        session.save_to(directory.path()).unwrap();
        let mut loaded = StoredSession::load_from(session.id, directory.path()).unwrap();
        assert_ne!(loaded.history_snapshot().epoch, history.epoch);
        assert_ne!(
            loaded.latest_state().unwrap().boundaries[&link.seq].lens
                [&session.root_agent_id().unwrap()]
                .0,
            loaded.history_snapshot().epoch
        );
        if recover {
            loaded = crate::session_recovery::RecoveryCandidate::prepare(loaded)
                .unwrap()
                .session()
                .clone();
        }
        assert_eq!(
            boundaries(&loaded, &link),
            Err(ParentRewindError::Unavailable)
        );
    }

    #[test]
    fn compaction_is_checked_against_current_parent() {
        let (mut session, link, root) = fixture();
        session.push_state(StoredStateRecord {
            seq: 5,
            agent: Some(root),
            generation: 0,
            change: StateChange::Compaction { epoch: 1 },
        });
        assert_eq!(
            boundaries(&session, &link),
            Err(ParentRewindError::Compacted)
        );
    }

    #[test_case(false; "valid_pin")]
    #[test_case(true; "malformed_pin")]
    fn pinned_boundary_uses_preserved_history_after_restart(malformed: bool) {
        let (mut session, mut link, root) = fixture();
        link.seq = 3;
        let original = session
            .state()
            .iter()
            .filter(|record| record.seq <= link.seq)
            .cloned()
            .collect::<Vec<_>>();
        let snapshot = if malformed {
            json!({})
        } else {
            json!({
                "state":original,"agents":session.agent_revisions(),
                "histories":{root.to_string():{"epoch":session.history_snapshot().epoch,"compacted":false,"messages":session.messages()}}
            })
        };
        session.replace_shared_messages(HistorySnapshot::new(Vec::new()).messages);
        session.push_state(StoredStateRecord {
            seq: 5,
            agent: None,
            generation: 0,
            change: StateChange::BoundarySnapshot {
                boundary: link.seq,
                snapshot,
            },
        });
        let directory = tempfile::tempdir().unwrap();
        session.save_to(directory.path()).unwrap();
        let session = StoredSession::load_from(session.id, directory.path()).unwrap();
        if malformed {
            assert!(matches!(
                boundaries(&session, &link),
                Err(ParentRewindError::Invalid(_))
            ));
        } else {
            let entries = boundaries(&session, &link).unwrap();
            assert_eq!((entries[0].seq, entries[0].offset), (3, 2));
            assert_eq!(entries[0].preview, APPROVAL_POINT);
        }
    }

    #[test]
    fn child_boundaries_are_not_parent_turns_and_enumeration_is_read_only() {
        let (mut session, link, _) = fixture();
        session.push_state(StoredStateRecord {
            seq: 5,
            agent: Some(AgentId::generate()),
            generation: 0,
            change: StateChange::Boundary {
                offset: 0,
                lens: Default::default(),
            },
        });
        let state = session.state().to_vec();
        let revisions = session.agent_revisions().to_vec();
        let revision = session.revision();
        let entries = boundaries(&session, &ParentLink { seq: 5, ..link }).unwrap();
        assert_eq!(
            entries.iter().map(|entry| entry.seq).collect::<Vec<_>>(),
            [4, 3, 2, 1]
        );
        assert_eq!(session.state(), state);
        assert_eq!(session.agent_revisions(), revisions);
        assert_eq!(session.revision(), revision);
    }

    #[test]
    fn missing_root_is_distinct_from_missing_boundaries() {
        let session = StoredSession::new("test-model", "/project");
        let link = ParentLink {
            session: session.id,
            message_count: 0,
            seq: 0,
        };
        assert_eq!(
            boundaries(&session, &link),
            Err(ParentRewindError::MissingRoot)
        );
    }

    #[test]
    fn mismatched_parent_is_rejected() {
        let (session, mut link, _) = fixture();
        link.session = MakimaId::generate();
        assert_eq!(
            boundaries(&session, &link),
            Err(ParentRewindError::SessionMismatch)
        );
    }
}
