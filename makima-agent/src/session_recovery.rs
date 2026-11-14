use crate::{
    AgentMode, ModeId, ModeRegistry, ToolOutput,
    agent::UNAVAILABLE_RESULT,
    journal::{SessionJournal, SessionJournalSnapshot},
    modes, plan_state,
};
use makima_providers::{ContentBlock, Message, Role, TokenUsage};
use makima_storage::{
    StateDir,
    checkpoint::CheckpointError,
    session_state::{
        StateChange, StoredCancellationReason, StoredPayload, StoredStateRecord, StoredTurnOutcome,
        StoredTurnProvenance, StoredTurnResult, StoredTurnStatus,
    },
    sessions::{HistorySnapshot, Session, next_epoch},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::take,
    sync::Arc,
};

const SEQUENCE_EXHAUSTED: &str = "session recovery exhausted the state sequence";
const DRAFT_SEPARATOR: &str = "\n\n";

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

pub struct RecoveryCandidate {
    session: StoredSession,
    journal: Arc<SessionJournal>,
}

pub struct RecoveredSession {
    session: StoredSession,
    journal: Arc<SessionJournal>,
}

struct HistoryRemap {
    epoch: u64,
    offsets: Vec<usize>,
}

pub fn resolve_legacy_mode(
    session: &mut StoredSession,
    registry: &ModeRegistry,
    state_dir: &StateDir,
) -> Result<Option<String>, CheckpointError> {
    let session_id = session.id;
    let failure = |message: String| CheckpointError::Save {
        session_id,
        message: message.into(),
    };
    let view = session
        .latest_state()
        .map_err(|error| failure(error.to_string()))?;
    if view
        .scopes
        .get(&session.root_agent_id())
        .is_some_and(|scope| scope.config.is_some())
    {
        return Ok(None);
    }
    let plans = plan_state::plan_entries(&view).map_err(failure)?;
    let mode = modes::saved(&session.meta).map(|(key, _)| key);
    let restored = if mode == Some(ModeId::Plan.key()) {
        let current = session
            .root_agent_id()
            .and_then(|root| plans.get(&root))
            .map_or(AgentMode::Build, |entry| {
                AgentMode::Plan(entry.path.clone())
            });
        modes::resolve(registry, ModeId::Plan.key(), &current, state_dir)
    } else {
        modes::restored(registry, &session.meta, state_dir)
    };
    let (mode, definition, warning) = match restored {
        Ok((mode, definition)) => (mode, definition, None),
        Err(error) => (
            AgentMode::Build,
            registry.current(&AgentMode::Build),
            Some(error.to_string()),
        ),
    };
    let restored = modes::prepare_restore(&definition.stored_snapshot(&mode), registry)
        .map_err(|error| failure(error.to_string()))?;
    session
        .resolve_legacy_mode(restored.definition.stored_snapshot(&restored.mode))
        .map_err(|error| failure(error.to_string()))?;
    Ok(restored.warning.or(warning))
}

impl RecoveryCandidate {
    pub fn from_exact(
        session: StoredSession,
        snapshot: SessionJournalSnapshot,
    ) -> Result<Self, CheckpointError> {
        let journal = SessionJournal::from_exact_snapshot(&session, snapshot).map_err(|error| {
            CheckpointError::Save {
                session_id: session.id,
                message: Arc::from(error.to_string()),
            }
        })?;
        Ok(Self {
            session,
            journal: Arc::new(journal),
        })
    }

    pub fn prepare(mut session: StoredSession) -> Result<Self, CheckpointError> {
        let session_id = session.id;
        let failure = |message: String| CheckpointError::Save {
            session_id,
            message: Arc::from(message),
        };
        for queued in take(&mut session.meta.queued_messages) {
            let draft = session.meta.input_draft.get_or_insert_with(String::new);
            if !draft.is_empty() {
                draft.push_str(DRAFT_SEPARATOR);
            }
            draft.push_str(&queued);
        }
        let view = session
            .latest_state()
            .map_err(|error| failure(error.to_string()))?;
        let epochs: BTreeMap<_, _> = view
            .agents
            .values()
            .filter_map(|agent| {
                let history = if agent.transcript_key.is_empty() {
                    Some(session.history_snapshot())
                } else {
                    session.subagent_history_snapshot(&agent.transcript_key)
                }?;
                Some((agent.id, history.epoch))
            })
            .collect();
        let mut histories = BTreeMap::new();
        let (history, offsets) = sanitize(session.history_snapshot());
        session.adopt_history(&history);
        if let Some(root) = session.root_agent_id() {
            histories.insert(
                root,
                HistoryRemap {
                    epoch: history.epoch,
                    offsets,
                },
            );
        }
        let keys: BTreeSet<_> = session
            .subagent_messages()
            .keys()
            .cloned()
            .chain(
                view.agents
                    .values()
                    .filter(|agent| !agent.transcript_key.is_empty())
                    .map(|agent| agent.transcript_key.clone()),
            )
            .collect();
        for key in keys {
            let history = session.subagent_history_snapshot(&key).unwrap_or_default();
            let (history, offsets) = sanitize(history);
            session.adopt_subagent_history(key.clone(), &history);
            if let Some(agent) = view
                .agents
                .values()
                .find(|agent| agent.transcript_key == key)
            {
                histories.insert(
                    agent.id,
                    HistoryRemap {
                        epoch: history.epoch,
                        offsets,
                    },
                );
            }
        }
        let mut state = session.state().to_vec();
        let pinned = crate::rewind::recover_boundary_pins(&mut state)
            .map_err(|error| failure(error.to_string()))?;
        for record in &mut state {
            if pinned.contains(&record.seq) {
                continue;
            }
            if let StateChange::Boundary { offset, lens } = &mut record.change {
                if let Some(agent) = record.agent
                    && lens
                        .get(&agent)
                        .is_some_and(|(epoch, _)| epochs.get(&agent) == Some(epoch))
                    && let Some(remap) = histories.get(&agent)
                    && *offset < remap.offsets.len()
                {
                    *offset = remap.offset(*offset);
                }
                for (agent, (epoch, len)) in lens {
                    if epochs.get(agent) == Some(epoch)
                        && let Some(remap) = histories.get(agent)
                        && *len < remap.offsets.len()
                    {
                        *epoch = remap.epoch;
                        *len = remap.offset(*len);
                    }
                }
            } else if let StateChange::Compaction { epoch } = &mut record.change
                && let Some(agent) = record.agent
                && epochs.get(&agent) == Some(epoch)
                && let Some(remap) = histories.get(&agent)
            {
                *epoch = remap.epoch;
            }
        }
        session.replace_state(state);
        let mut seq = session
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
            .max()
            .unwrap_or(0);
        for (agent, scope) in view.scopes {
            let Some(agent) = agent else { continue };
            for (turn, origin) in scope.admitted {
                seq = seq
                    .checked_add(1)
                    .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.to_owned()))?;
                session.push_state(StoredStateRecord {
                    seq,
                    agent: Some(agent),
                    generation: scope.generation,
                    change: StateChange::Turn {
                        turn,
                        result: StoredTurnResult {
                            outcome: StoredTurnOutcome {
                                agent_id: agent,
                                turn_id: turn,
                                usage: Default::default(),
                                num_turns: 0,
                                cost: None,
                                list_cost: None,
                                context_size: 0,
                                context_window: 0,
                                status: StoredTurnStatus::Cancelled {
                                    reason: StoredCancellationReason::Interrupted,
                                },
                            },
                            text: StoredPayload::Inline(String::new()),
                            output: StoredPayload::Inline(Vec::new()),
                            provenance: StoredTurnProvenance {
                                origin,
                                plugin: None,
                                plugin_generation: None,
                                source_agent: None,
                                source_turn: None,
                            },
                            duration_ms: 0,
                        },
                    },
                });
            }
        }
        let journal = Arc::new(
            SessionJournal::from_session(&session).map_err(|error| failure(error.to_string()))?,
        );
        Ok(Self { session, journal })
    }

    pub fn session(&self) -> &StoredSession {
        &self.session
    }

    pub fn finalize_after_publication(self) -> RecoveredSession {
        RecoveredSession {
            session: self.session,
            journal: self.journal,
        }
    }
}

impl RecoveredSession {
    pub fn session(&self) -> &StoredSession {
        &self.session
    }

    pub fn journal(&self) -> Arc<SessionJournal> {
        Arc::clone(&self.journal)
    }

    pub fn into_parts(self) -> (StoredSession, Arc<SessionJournal>) {
        (self.session, self.journal)
    }
}

impl HistoryRemap {
    fn offset(&self, offset: usize) -> usize {
        self.offsets
            .get(offset)
            .copied()
            .unwrap_or_else(|| *self.offsets.last().unwrap_or(&0))
    }
}

pub(crate) fn sanitize(
    mut history: HistorySnapshot<Message>,
) -> (HistorySnapshot<Message>, Vec<usize>) {
    let mut messages: Vec<Message> = Vec::with_capacity(history.messages.len());
    let mut offsets = Vec::with_capacity(history.messages.len() + 1);
    for mut message in history.messages.iter().cloned() {
        let previous = messages.last();
        let mut calls: Vec<_> = previous
            .filter(|message| matches!(message.role, Role::Assistant))
            .into_iter()
            .flat_map(Message::tool_uses)
            .map(|(id, _, _)| id.to_owned())
            .collect();
        if matches!(message.role, Role::User) {
            let had_results = message
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolResult { .. }));
            if !had_results && !calls.is_empty() {
                messages.push(results(take(&mut calls)));
            }
            message.content.retain(|block| !matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if !calls.contains(tool_use_id)));
            if had_results
                && !message
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolResult { .. }))
            {
                message
                    .content
                    .retain(|block| !matches!(block, ContentBlock::Image { .. }));
            }
            if message.content.is_empty() {
                offsets.push(messages.len());
                continue;
            }
            let missing: Vec<_> = calls.into_iter().filter(|id| !message.content.iter().any(|block| matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == id))).collect();
            message.content.extend(missing.into_iter().map(unavailable));
        } else if !calls.is_empty() {
            messages.push(results(calls));
        }
        offsets.push(messages.len());
        if !message.content.is_empty() {
            messages.push(message);
        }
    }
    if let Some(last) = messages.last()
        && matches!(last.role, Role::Assistant)
        && last.has_tool_calls()
    {
        let calls = last.tool_uses().map(|(id, _, _)| id.to_owned()).collect();
        messages.push(results(calls));
    }
    offsets.push(messages.len());
    if history.messages.as_ref() != &messages {
        history.epoch = next_epoch();
        history.messages = Arc::new(messages);
    }
    (history, offsets)
}

fn unavailable(tool_use_id: String) -> ContentBlock {
    ContentBlock::ToolResult {
        tool_use_id,
        content: UNAVAILABLE_RESULT.to_owned(),
        is_error: true,
    }
}

fn results(calls: Vec<String>) -> Message {
    Message {
        role: Role::User,
        content: calls.into_iter().map(unavailable).collect(),
        display_text: Some(String::new()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentLimits, AgentManagerHandle,
        rewind::{RewindBoundary, RewindError},
        session_checkpoint::SessionLogCheckpoint,
    };
    use makima_storage::{
        StateDir,
        checkpoint::{CheckpointClaim, CheckpointPublication},
        id::{AgentId, TurnId},
        plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot},
        session_lock,
        session_state::StoredTurnOrigin,
        sessions::SESSIONS_DIR,
    };
    use serde_json::json;
    use tempfile::TempDir;
    use test_case::test_case;

    const MODEL: &str = "test/model";
    const CWD: &str = "/tmp";
    const CHILD_KEY: &str = "child";
    const TOOL: &str = "tool";
    const ORPHAN: &str = "orphan";
    const CLAIM_REJECTED: &str = "recovery claim rejected";
    const DRAFT: &str = "inactive draft";

    #[test_case(false; "exact_candidate_preserves_history_coordinates")]
    #[test_case(true; "mismatched_candidate_is_rejected")]
    fn exact_candidate_does_not_sanitize_or_remint(mismatch: bool) {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message::user(DRAFT.into()));
        let journal = SessionJournal::from_session(&session).unwrap();
        let mut snapshot = journal.snapshot();
        snapshot.revision = 17;
        snapshot.histories.get_mut(&root).unwrap().epoch = 73;
        snapshot.managed_histories.insert(root);
        if mismatch {
            snapshot.histories.get_mut(&root).unwrap().messages = Arc::new(Vec::new());
        }
        let expected = snapshot.clone();
        let result = RecoveryCandidate::from_exact(session, snapshot);
        if mismatch {
            assert!(result.is_err());
        } else {
            let recovered = result.unwrap().finalize_after_publication();
            let actual = recovered.journal().snapshot();
            assert_eq!(actual.epoch, expected.epoch);
            assert_eq!(actual.seq, expected.seq);
            assert_eq!(actual.revision, expected.revision);
            assert_eq!(actual.state, expected.state);
            assert_eq!(
                actual.histories[&root].epoch,
                expected.histories[&root].epoch
            );
            assert!(Arc::ptr_eq(
                &actual.histories[&root].messages,
                &expected.histories[&root].messages
            ));
        }
    }

    #[test_case(0; "valid_empty_prefix")]
    #[test_case(1; "valid_end_prefix_includes_missing_result")]
    #[test_case(2; "invalid_prefix_stays_invalid_after_growth")]
    fn sanitization_growth_rebases_only_originally_valid_lenses(offset: usize) {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
            ..Default::default()
        });
        let original_epoch = session.history_snapshot().epoch;
        let seq = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq,
            agent: Some(root),
            generation: 0,
            change: StateChange::Boundary {
                offset,
                lens: [(root, (original_epoch, offset))].into(),
            },
        });
        let candidate = RecoveryCandidate::prepare(session).unwrap();
        let snapshot = candidate.journal.snapshot();
        let history = &snapshot.histories[&root];
        assert_eq!(history.messages.len(), 2);
        assert_ne!(history.epoch, original_epoch);
        let view = candidate.session.latest_state().unwrap();
        let boundary = &view.boundaries[&seq];
        assert_eq!(
            candidate
                .session
                .state()
                .iter()
                .find(|record| record.seq == seq)
                .unwrap()
                .generation,
            0
        );
        let result = candidate.journal.preview_rewind(RewindBoundary {
            agent: root,
            seq,
            epoch: snapshot.epoch,
        });
        if offset > 1 {
            assert_eq!(boundary.lens[&root], (original_epoch, offset));
            assert!(matches!(result, Err(RewindError::Compacted(id)) if id == root));
        } else {
            let expected = if offset == 0 { 0 } else { 2 };
            assert_eq!(boundary.offset, expected);
            assert_eq!(boundary.lens[&root], (history.epoch, expected));
            assert_eq!(result.unwrap().histories[&root].messages.len(), expected);
        }
        let recovered_epoch = history.epoch;
        let again = RecoveryCandidate::prepare(candidate.session).unwrap();
        assert_eq!(again.session.history_snapshot().epoch, recovered_epoch);
        assert_eq!(
            again.session.latest_state().unwrap().boundaries[&seq],
            *boundary
        );
    }

    #[test]
    fn interrupted_admission_keeps_policy_generation_after_history_sanitization() {
        const GENERATION: u64 = 37;
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        let turn = TurnId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
            ..Default::default()
        });
        let epoch = session.history_snapshot().epoch;
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(root),
            generation: GENERATION,
            change: StateChange::Admitted {
                turn,
                origin: StoredTurnOrigin::User,
            },
        });
        let candidate = RecoveryCandidate::prepare(session).unwrap();
        assert_ne!(candidate.session.history_snapshot().epoch, epoch);
        let terminal = candidate.session.state().iter().find(|record| {
            matches!(&record.change, StateChange::Turn { turn: id, .. } if *id == turn)
        }).unwrap();
        assert_eq!(terminal.generation, GENERATION);
        assert_eq!(
            candidate.session.latest_state().unwrap().scopes[&Some(root)].generation,
            GENERATION
        );
    }

    #[test]
    fn legacy_plan_config_uses_the_migrated_slot_path() {
        let directory = TempDir::new().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let path = directory.path().join(CHILD_KEY);
        std::fs::write(&path, DRAFT).unwrap();
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.meta.mode = Some(makima_storage::sessions::StoredMode::Plan);
        let plans: PlanSlot = [(
            root,
            PlanEntry {
                path: path.clone(),
                written: true,
                approved: None,
                staged_model: None,
            },
        )]
        .into();
        session.push_state(StoredStateRecord {
            seq: session.agent_revisions().last().unwrap().0 + 1,
            agent: None,
            generation: 0,
            change: StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(serde_json::to_value(plans).unwrap()),
            },
        });
        assert!(
            resolve_legacy_mode(&mut session, &ModeRegistry::builtin(), &storage)
                .unwrap()
                .is_none()
        );
        let view = session.latest_state().unwrap();
        assert_eq!(
            view.scopes[&Some(root)]
                .config
                .as_ref()
                .unwrap()
                .mode
                .restrict_write_to
                .as_ref(),
            Some(&path)
        );
        assert!(plan_state::plan_entries(&view).unwrap()[&root].written);
        assert_eq!(std::fs::read_to_string(path).unwrap(), DRAFT);
    }

    #[test_case(false; "known_mode")]
    #[test_case(true; "removed_mode_falls_back_once")]
    fn legacy_mode_resolution_is_once_and_preserves_existing_config(removed: bool) {
        const REMOVED_MODE: &str = "removed_mode";
        const LEGACY_OPTION: &str = "legacy.option";
        let directory = TempDir::new().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let registry = ModeRegistry::builtin();
        let mut session = StoredSession::new(MODEL, CWD);
        session.meta.fast = true;
        session.meta.workflow = true;
        session.meta.thinking = Some(makima_storage::sessions::StoredThinking::Off);
        session
            .meta
            .session_options
            .insert(LEGACY_OPTION.into(), DRAFT.into());
        if removed {
            session.meta.mode = Some(makima_storage::sessions::StoredMode::Custom(
                REMOVED_MODE.into(),
            ));
        }
        let warning = resolve_legacy_mode(&mut session, &registry, &storage).unwrap();
        assert_eq!(warning.is_some(), removed);
        let before = session.state().to_vec();
        let state = session.latest_state().unwrap();
        let config = state.scopes[&session.root_agent_id()]
            .config
            .as_ref()
            .unwrap();
        assert_eq!(config.model, MODEL);
        assert_eq!(config.cwd, CWD);
        assert!(config.fast && config.workflow);
        assert_eq!(config.options[LEGACY_OPTION], DRAFT);
        assert_eq!(config.mode.id, AgentMode::Build.id().key());
        registry
            .define(crate::ModeDefSpec {
                name: AgentMode::Build.id().key().into(),
                system_prompt: Some(REMOVED_MODE.into()),
                ..Default::default()
            })
            .unwrap();
        assert!(
            resolve_legacy_mode(&mut session, &registry, &storage)
                .unwrap()
                .is_none()
        );
        assert_eq!(session.state(), before);
    }

    fn fixture() -> (StoredSession, AgentId, AgentId, TurnId, TurnId) {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        let child = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = CHILD_KEY.into();
        record.created_seq = 2;
        record.updated_seq = 2;
        session.push_agent_revision(2, record);
        let calls = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: TOOL.into(),
                name: TOOL.into(),
                input: json!({}),
                thought_signature: None,
            }],
            ..Default::default()
        };
        let orphan = Message {
            role: Role::User,
            content: vec![unavailable(ORPHAN.into())],
            ..Default::default()
        };
        session.adopt_history(&HistorySnapshot::new(vec![
            orphan.clone(),
            calls.clone(),
            Message::user(DRAFT.into()),
        ]));
        session
            .adopt_subagent_history(CHILD_KEY.into(), &HistorySnapshot::new(vec![orphan, calls]));
        let epoch = session.history_snapshot().epoch;
        let child_epoch = session.subagent_history_snapshot(CHILD_KEY).unwrap().epoch;
        let lens = BTreeMap::from([(root, (epoch, 2)), (child, (child_epoch, 1))]);
        session.push_state(StoredStateRecord {
            seq: 3,
            agent: Some(root),
            generation: 0,
            change: StateChange::Boundary { offset: 2, lens },
        });
        let root_turn = TurnId::generate();
        let child_turn = TurnId::generate();
        for (seq, agent, turn, origin) in [
            (4, root, root_turn, StoredTurnOrigin::User),
            (5, child, child_turn, StoredTurnOrigin::Internal),
        ] {
            session.push_state(StoredStateRecord {
                seq,
                agent: Some(agent),
                generation: 0,
                change: StateChange::Admitted { turn, origin },
            });
        }
        session.meta.input_draft = Some(ORPHAN.into());
        session.meta.queued_messages.push(DRAFT.into());
        (session, root, child, root_turn, child_turn)
    }

    fn claim() -> CheckpointClaim {
        Arc::new(|publication| {
            publication
                .try_publish()
                .then_some(())
                .ok_or_else(|| Arc::from(CLAIM_REJECTED))
        })
    }

    #[test]
    fn orphan_result_before_actual_result_preserves_root_and_child_results() {
        let (mut session, root, child, _, _) = fixture();
        let calls = Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: TOOL.into(),
                name: TOOL.into(),
                input: json!({}),
                thought_signature: None,
            }],
            ..Default::default()
        };
        let orphan = Message {
            role: Role::User,
            content: vec![unavailable(ORPHAN.into())],
            ..Default::default()
        };
        let actual = Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: TOOL.into(),
                content: DRAFT.into(),
                is_error: false,
            }],
            ..Default::default()
        };
        let original = vec![calls, orphan, actual];
        let expected = crate::agent::History::restored(original.clone()).into_vec();
        let mut history = session.history_snapshot();
        history.messages = Arc::new(original.clone());
        session.adopt_history(&history);
        let mut history = session.subagent_history_snapshot(CHILD_KEY).unwrap();
        history.messages = Arc::new(original);
        session.adopt_subagent_history(CHILD_KEY.into(), &history);
        let candidate = RecoveryCandidate::prepare(session).unwrap();
        assert_eq!(candidate.session().messages(), &expected);
        assert_eq!(
            candidate.session().subagent_messages()[CHILD_KEY].as_ref(),
            &expected
        );
        let boundary = &candidate.session().latest_state().unwrap().boundaries[&3];
        assert_eq!(boundary.offset, 1);
        assert_eq!(boundary.lens[&root].1, 1);
        assert_eq!(boundary.lens[&child].1, 1);
    }

    #[test]
    fn recovery_terminalization_is_durable_and_idempotent() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (mut session, root, child, root_turn, child_turn) = fixture();
            let id = session.id;
            session.save(&storage).unwrap();
            let sessions = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let lease = session_lock::claim(&sessions, &id).unwrap().unwrap();
            let loaded = StoredSession::load(id, &storage).unwrap();
            let writer = SessionLogCheckpoint::open_with_session(
                storage.clone(),
                loaded,
                Some(lease.publication_guard()),
            )
            .unwrap();
            let recovered = writer
                .recover(CheckpointPublication::default(), claim())
                .await
                .unwrap();
            let view = recovered.session().latest_state().unwrap();
            for (agent, turn) in [(root, root_turn), (child, child_turn)] {
                let scope = &view.scopes[&Some(agent)];
                assert!(scope.admitted.is_empty());
                let result = scope.results.iter().find(|(id, _)| *id == turn).unwrap();
                assert_eq!(
                    result.1.outcome.status,
                    StoredTurnStatus::Cancelled {
                        reason: StoredCancellationReason::Interrupted
                    }
                );
                assert!(scope.deliveries.is_empty());
            }
            assert_eq!(recovered.session().messages().len(), 3);
            assert_eq!(recovered.session().subagent_messages()[CHILD_KEY].len(), 2);
            let boundary = &view.boundaries[&3];
            assert_eq!(boundary.offset, 2);
            assert_eq!(
                boundary.lens[&root],
                (recovered.session().history_snapshot().epoch, 2)
            );
            assert_eq!(
                boundary.lens[&child],
                (
                    recovered
                        .session()
                        .subagent_history_snapshot(CHILD_KEY)
                        .unwrap()
                        .epoch,
                    0
                )
            );
            assert!(recovered.session().meta.queued_messages.is_empty());
            assert_eq!(
                recovered.session().meta.input_draft.as_deref(),
                Some(format!("{ORPHAN}{DRAFT_SEPARATOR}{DRAFT}").as_str())
            );
            let journal = recovered.journal();
            let snapshot = journal.snapshot();
            let manager = AgentManagerHandle::restore_graph(
                AgentLimits::default(),
                journal,
                view,
                snapshot.histories,
            )
            .unwrap();
            assert!(manager.lookup(root).unwrap().actor().is_err());
            let durable_scopes = recovered.session().latest_state().unwrap().scopes;
            let durable_state = recovered.session().state().to_vec();
            drop(writer);
            let loaded = StoredSession::load(id, &storage).unwrap();
            let writer = SessionLogCheckpoint::open_with_session(
                storage.clone(),
                loaded,
                Some(lease.publication_guard()),
            )
            .unwrap();
            let again = writer
                .recover(CheckpointPublication::default(), claim())
                .await
                .unwrap();
            let after = again.session().latest_state().unwrap();
            assert_eq!(durable_state.len(), again.session().state().len());
            assert_eq!(durable_scopes, after.scopes);
            assert_eq!(again.session().meta, recovered.session().meta);
            assert_eq!(again.session().messages(), recovered.session().messages());
            assert_eq!(
                again.session().subagent_messages(),
                recovered.session().subagent_messages()
            );
        });
    }

    #[test_case(0 ; "claim_rejected")]
    #[test_case(1 ; "lease_lost")]
    #[test_case(2 ; "rename_failed_after_claim")]
    fn failed_recovery_does_not_publish_or_expose_results(failure: u8) {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (mut session, root, _, _, _) = fixture();
            let id = session.id;
            session.save(&storage).unwrap();
            let sessions = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let path = sessions.join(format!("{id}.jsonl"));
            let original = std::fs::read(&path).unwrap();
            let lease = session_lock::claim(&sessions, &id).unwrap().unwrap();
            let guard = lease.publication_guard();
            let replacement = if failure == 1 {
                lease.release().unwrap();
                Some(session_lock::claim(&sessions, &id).unwrap().unwrap())
            } else {
                None
            };
            let writer =
                SessionLogCheckpoint::open_with_session(storage.clone(), session, Some(guard))
                    .unwrap();
            let claim = match failure {
                1 => claim(),
                2 => {
                    let path = path.clone();
                    Arc::new(move |publication: &CheckpointPublication| {
                        assert!(publication.try_publish());
                        std::fs::remove_file(&path).unwrap();
                        std::fs::create_dir(&path).unwrap();
                        Ok(())
                    }) as CheckpointClaim
                }
                _ => Arc::new(|_: &CheckpointPublication| Err(Arc::from(CLAIM_REJECTED)))
                    as CheckpointClaim,
            };
            assert!(
                writer
                    .recover(CheckpointPublication::default(), claim)
                    .await
                    .is_err()
            );
            if failure == 2 {
                std::fs::remove_dir(&path).unwrap();
                std::fs::write(&path, &original).unwrap();
            }
            assert_eq!(std::fs::read(&path).unwrap(), original);
            let stored = StoredSession::load(id, &storage).unwrap();
            assert!(
                stored.latest_state().unwrap().scopes[&Some(root)]
                    .results
                    .is_empty()
            );
            drop(replacement);
        });
    }
}
