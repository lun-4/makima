use crate::{
    AgentMode, ModeId, ModeRegistry, ToolOutput,
    agent::UNAVAILABLE_RESULT,
    journal::{SessionJournal, SessionJournalSnapshot},
    modes, plan_state,
    session_writer::SessionWriter,
};
use makima_providers::{ContentBlock, Message, Role, TokenUsage};
use makima_storage::{
    StateDir,
    checkpoint::{CheckpointClaim, CheckpointError, CheckpointPublication},
    id::MakimaId,
    session_state::{
        StateChange, StoredCancellationReason, StoredPayload, StoredStateRecord, StoredTurnOutcome,
        StoredTurnProvenance, StoredTurnResult, StoredTurnStatus,
    },
    sessions::{HistorySnapshot, Session, SessionRecord, next_epoch},
};
use std::{collections::BTreeSet, mem::take, sync::Arc};

const SEQUENCE_EXHAUSTED: &str = "session recovery exhausted the state sequence";
const DRAFT_SEPARATOR: &str = "\n\n";
const WRONG_WRITER: &str = "recovery writer belongs to another session";

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

pub struct RecoveryCandidate {
    session: StoredSession,
    journal: Arc<SessionJournal>,
    records: Vec<SessionRecord<Message, TokenUsage, ToolOutput>>,
}

pub struct RecoveredSession {
    session: StoredSession,
    journal: Arc<SessionJournal>,
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
            records: Vec::new(),
        })
    }

    pub fn prepare(mut session: StoredSession) -> Result<Self, CheckpointError> {
        let session_id = session.id;
        let failure = |message: String| CheckpointError::Save {
            session_id,
            message: Arc::from(message),
        };
        let mut records = Vec::new();
        let restore_draft = !session.meta.queued_messages.is_empty();
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
        let mut seq = session
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
            .chain(session.sequence_high_water())
            .max()
            .unwrap_or(0);
        let previous_epoch = session.history_snapshot().epoch;
        let covered = session.last_message_id();
        let history = sanitize(session.history_snapshot());
        session.adopt_history(&history);
        if history.epoch != previous_epoch
            && let Some(root) = session.root_agent_id()
        {
            records.push(SessionRecord::Compaction {
                agent: root,
                messages: history.messages.as_ref().clone(),
                ids: history.ids.as_ref().clone(),
                covered,
            });
            seq = seq
                .checked_add(1)
                .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.into()))?;
            let record = StoredStateRecord {
                seq,
                agent: Some(root),
                generation: view
                    .scopes
                    .get(&Some(root))
                    .map_or(0, |scope| scope.generation),
                change: StateChange::Compaction {
                    epoch: history.epoch,
                },
            };
            session.push_state(record.clone());
            records.push(SessionRecord::State { d: record });
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
            let previous_epoch = history.epoch;
            let covered = session.chat_message_ids(Some(&key)).last().copied();
            let history = sanitize(history);
            session.adopt_subagent_history(key.clone(), &history);
            if let Some(agent) = view
                .agents
                .values()
                .find(|agent| agent.transcript_key == key)
                && history.epoch != previous_epoch
            {
                records.push(SessionRecord::Compaction {
                    agent: agent.id,
                    messages: history.messages.as_ref().clone(),
                    ids: history.ids.as_ref().clone(),
                    covered,
                });
                seq = seq
                    .checked_add(1)
                    .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.into()))?;
                let record = StoredStateRecord {
                    seq,
                    agent: Some(agent.id),
                    generation: view
                        .scopes
                        .get(&Some(agent.id))
                        .map_or(0, |scope| scope.generation),
                    change: StateChange::Compaction {
                        epoch: history.epoch,
                    },
                };
                session.push_state(record.clone());
                records.push(SessionRecord::State { d: record });
            }
        }
        for (agent, scope) in view.scopes {
            let Some(agent) = agent else { continue };
            for (turn, origin) in scope.admitted {
                seq = seq
                    .checked_add(1)
                    .ok_or_else(|| failure(SEQUENCE_EXHAUSTED.to_owned()))?;
                let record = StoredStateRecord {
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
                };
                session.push_state(record.clone());
                records.push(SessionRecord::State { d: record });
            }
        }
        if restore_draft {
            records.push(SessionRecord::Draft {
                text: session.meta.input_draft.clone(),
            });
            records.push(SessionRecord::Queued {
                messages: session.meta.queued_messages.clone(),
            });
        }
        let journal = Arc::new(
            SessionJournal::from_session(&session).map_err(|error| failure(error.to_string()))?,
        );
        Ok(Self {
            session,
            journal,
            records,
        })
    }

    pub async fn publish(
        self,
        writer: &SessionWriter,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<RecoveredSession, CheckpointError> {
        let session_id = self.session.id;
        if writer.session_id() != session_id {
            return Err(CheckpointError::Save {
                session_id,
                message: Arc::from(WRONG_WRITER),
            });
        }
        claim(&publication).map_err(|message| CheckpointError::Save {
            session_id,
            message,
        })?;
        writer
            .append(self.records)
            .await
            .map_err(|error| CheckpointError::Save {
                session_id,
                message: Arc::from(error.to_string()),
            })?;
        writer.bind_journal(Arc::clone(&self.journal)).await?;
        Ok(RecoveredSession {
            session: self.session,
            journal: self.journal,
        })
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

impl SessionWriter {
    pub async fn recover(
        &self,
        session: StoredSession,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> Result<RecoveredSession, CheckpointError> {
        RecoveryCandidate::prepare(session)?
            .publish(self, publication, claim)
            .await
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

pub(crate) fn sanitize(mut history: HistorySnapshot<Message>) -> HistorySnapshot<Message> {
    let mut messages: Vec<Message> = Vec::with_capacity(history.messages.len());
    let mut ids = Vec::with_capacity(history.messages.len());
    for (index, mut message) in history.messages.iter().cloned().enumerate() {
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
                ids.push(MakimaId::generate());
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
                continue;
            }
            let missing: Vec<_> = calls.into_iter().filter(|id| !message.content.iter().any(|block| matches!(block, ContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == id))).collect();
            message.content.extend(missing.into_iter().map(unavailable));
        } else if !calls.is_empty() {
            messages.push(results(calls));
            ids.push(MakimaId::generate());
        }
        if !message.content.is_empty() {
            messages.push(message);
            ids.push(history.ids[index]);
        }
    }
    if let Some(last) = messages.last()
        && matches!(last.role, Role::Assistant)
        && last.has_tool_calls()
    {
        let calls = last.tool_uses().map(|(id, _, _)| id.to_owned()).collect();
        messages.push(results(calls));
        ids.push(MakimaId::generate());
    }
    if history.messages.as_ref() != &messages {
        history.epoch = next_epoch();
        history.covered = history.ids.last().copied();
        history.ids = Arc::new(ids);
        history.messages = Arc::new(messages);
    }
    history
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
    use crate::{AgentLimits, AgentManagerHandle};
    use makima_storage::{
        StateDir,
        checkpoint::{CheckpointClaim, CheckpointPublication},
        id::{AgentId, TurnId},
        plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot},
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

    #[test]
    fn sanitization_repairs_missing_results_once() {
        let mut session = StoredSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_message(Message {
            role: Role::Assistant,
            content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
            ..Default::default()
        });
        let original_epoch = session.history_snapshot().epoch;
        let candidate = RecoveryCandidate::prepare(session).unwrap();
        let history = candidate.session.history_snapshot();
        assert_eq!(history.messages.len(), 2);
        assert_ne!(history.epoch, original_epoch);
        let again = RecoveryCandidate::prepare(candidate.session).unwrap();
        assert_eq!(again.session.history_snapshot().epoch, history.epoch);
        assert_eq!(again.session.messages(), history.messages.as_ref());
    }

    #[test_case(false; "inserted_result_gets_a_new_id")]
    #[test_case(true; "repaired_result_keeps_its_id")]
    fn sanitization_preserves_surviving_message_ids(repair_existing: bool) {
        let mut messages = vec![
            Message {
                role: Role::User,
                content: vec![unavailable(ORPHAN.into())],
                ..Default::default()
            },
            Message {
                role: Role::Assistant,
                content: vec![ContentBlock::tool_use(TOOL, TOOL, json!({}))],
                ..Default::default()
            },
        ];
        if repair_existing {
            messages.push(Message {
                role: Role::User,
                content: vec![
                    unavailable(ORPHAN.into()),
                    ContentBlock::Text { text: DRAFT.into() },
                ],
                ..Default::default()
            });
        } else {
            messages.push(Message::user(DRAFT.into()));
        }
        let original = HistorySnapshot::new(messages);
        let history = sanitize(original.clone());
        assert_ne!(history.epoch, original.epoch);
        assert_eq!(history.covered, original.ids.last().copied());
        assert_eq!(history.ids.len(), history.messages.len());
        assert_eq!(history.ids[0], original.ids[1]);
        if repair_existing {
            assert_eq!(history.ids[1], original.ids[2]);
        } else {
            assert!(!original.ids.contains(&history.ids[1]));
            assert_eq!(history.ids[2], original.ids[2]);
        }
        let again = sanitize(history.clone());
        assert_eq!(again.epoch, history.epoch);
        assert!(Arc::ptr_eq(&again.ids, &history.ids));
        assert!(Arc::ptr_eq(&again.messages, &history.messages));
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
        let (mut session, _, _, _, _) = fixture();
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
        history.ids = Arc::new(original.iter().map(|_| MakimaId::generate()).collect());
        history.messages = Arc::new(original.clone());
        session.adopt_history(&history);
        let mut history = session.subagent_history_snapshot(CHILD_KEY).unwrap();
        history.ids = Arc::new(original.iter().map(|_| MakimaId::generate()).collect());
        history.messages = Arc::new(original);
        session.adopt_subagent_history(CHILD_KEY.into(), &history);
        let candidate = RecoveryCandidate::prepare(session).unwrap();
        assert_eq!(candidate.session().messages(), &expected);
        assert_eq!(
            candidate.session().subagent_messages()[CHILD_KEY].as_ref(),
            &expected
        );
    }

    #[test]
    fn recovery_terminalization_is_durable_and_idempotent() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (mut session, root, child, root_turn, child_turn) = fixture();
            let id = session.id;
            session.save(&storage).unwrap();
            let original_state = session.state().to_vec();
            let sessions = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let path = sessions.join(format!("{id}.jsonl"));
            let original = std::fs::read(&path).unwrap();
            let loaded = StoredSession::load(id, &storage).unwrap();
            let writer = SessionWriter::open_session(&sessions, &loaded).unwrap();
            let recovered = writer
                .recover(loaded, CheckpointPublication::default(), claim())
                .await
                .unwrap();
            assert!(std::fs::read(&path).unwrap().starts_with(&original));
            assert_eq!(
                &recovered.session().state()[..original_state.len()],
                &original_state
            );
            let stored = StoredSession::load(id, &storage).unwrap();
            for chat in [None, Some(CHILD_KEY)] {
                assert_eq!(
                    stored.chat_message_ids(chat),
                    recovered.session().chat_message_ids(chat)
                );
            }
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
            for agent in [root, child] {
                assert!(
                    recovered.session().state()[original_state.len()..]
                        .iter()
                        .any(|record| {
                            record.agent == Some(agent)
                                && matches!(record.change, StateChange::Compaction { .. })
                        })
                );
            }
            let durable_bytes = std::fs::read(&path).unwrap();
            let durable_scopes = recovered.session().latest_state().unwrap().scopes;
            let durable_state = recovered.session().state().to_vec();
            drop(writer);
            let loaded = StoredSession::load(id, &storage).unwrap();
            let writer = SessionWriter::open_session(&sessions, &loaded).unwrap();
            let again = writer
                .recover(loaded, CheckpointPublication::default(), claim())
                .await
                .unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), durable_bytes);
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

    #[test]
    fn recovery_rejects_a_writer_for_another_session() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let writer_session = StoredSession::new(MODEL, CWD);
            let writer = SessionWriter::open_session(directory.path(), &writer_session).unwrap();
            let (session, _, _, _, _) = fixture();
            let result = writer
                .recover(
                    session,
                    CheckpointPublication::default(),
                    Arc::new(|_| unreachable!()),
                )
                .await;
            assert!(
                matches!(result, Err(CheckpointError::Save { message, .. }) if message.as_ref() == WRONG_WRITER)
            );
        });
    }

    #[test]
    fn rejected_recovery_does_not_publish_or_expose_results() {
        smol::block_on(async {
            let directory = TempDir::new().unwrap();
            let storage = StateDir::from_path(directory.path().to_path_buf());
            let (mut session, root, _, _, _) = fixture();
            let id = session.id;
            session.save(&storage).unwrap();
            let sessions = storage.ensure_subdir(SESSIONS_DIR).unwrap();
            let path = sessions.join(format!("{id}.jsonl"));
            let original = std::fs::read(&path).unwrap();
            let writer = SessionWriter::open_session(&sessions, &session).unwrap();
            let claim: CheckpointClaim = Arc::new(|_| Err(Arc::from(CLAIM_REJECTED)));
            assert!(
                writer
                    .recover(session, CheckpointPublication::default(), claim)
                    .await
                    .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), original);
            let stored = StoredSession::load(id, &storage).unwrap();
            assert!(
                stored.latest_state().unwrap().scopes[&Some(root)]
                    .results
                    .is_empty()
            );
        });
    }
}
