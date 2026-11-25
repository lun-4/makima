use crate::{
    AgentRef, History, HistorySnapshot, ToolOutput,
    actor::{ActorError, ActorIdleGuard},
    journal::{JournalCommit, JournalError, SessionJournal},
    session_writer::WriterError,
    subagents::format_subagents,
};
use makima_providers::{Message, TokenUsage};
use makima_storage::{
    conversation::ConversationState,
    id::{AgentId, MakimaId},
    session_state::ParentLink,
    sessions::{ContextEntry, Session, SessionRecord, context_entries, next_epoch},
};
use std::{collections::HashSet, sync::Arc};
use thiserror::Error;

const REWIND_NOTE: &str = "This conversation was rewound.";
const FILE_WARNING: &str = "Files may differ from what the conversation says. Files, tasks, mode, and session state have not been restored.";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewindTargetId {
    Message(MakimaId),
    PlanningSession,
}

#[derive(Debug, Clone)]
pub struct RewindTarget {
    pub id: RewindTargetId,
    pub prompt: Option<Message>,
    pub cut_off: bool,
}

#[derive(Debug, Error)]
pub enum RewindError {
    #[error("the agent is not idle; interrupt its turn before rewinding")]
    Busy,
    #[error("the rewind target is not available")]
    InvalidTarget,
    #[error("session does not belong to this agent")]
    WrongSession,
    #[error(transparent)]
    Actor(#[from] ActorError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error("rewind context: {0}")]
    Context(String),
    #[error("rewind could not be saved: {0}")]
    Persistence(String),
}

pub enum RewindOutcome {
    Fork {
        history: HistorySnapshot,
        draft: String,
        warning: Option<WriterError>,
    },
    PlanningSession(ParentLink),
}

pub struct RewindGuard {
    agent: AgentRef,
    idle: ActorIdleGuard,
    journal: Arc<SessionJournal>,
    events: Vec<serde_json::Value>,
    targets: Vec<RewindTarget>,
    parent: Option<ParentLink>,
}

impl AgentRef {
    pub fn begin_rewind(
        &self,
        session: &Session<Message, TokenUsage, ToolOutput>,
    ) -> Result<RewindGuard, RewindError> {
        let journal = self.manager().journal();
        if journal.session_id().is_some_and(|id| id != session.id) {
            return Err(RewindError::WrongSession);
        }
        let actor = self
            .actor()
            .map_err(|error| RewindError::Context(error.to_string()))?;
        let idle = actor.prepare_rewind().map_err(|error| match error {
            ActorError::Busy => RewindError::Busy,
            error => RewindError::Actor(error),
        })?;
        let mut events = journal.context_events();
        if events.is_empty() {
            events = session.context_events().to_vec();
        }
        let active = context_entries(&events, self.id())
            .map_err(|error| RewindError::Context(error.to_string()))?;
        let active: HashSet<_> = active.iter().map(ContextEntry::id).collect();
        let mut targets = Vec::new();
        let mut seen = HashSet::new();
        for event in &events {
            if event["t"] != "msg" || event["agent"] != serde_json::json!(self.id()) {
                continue;
            }
            let Ok(prompt) = serde_json::from_value::<Message>(event["d"].clone()) else {
                continue;
            };
            if !History::is_user_input_message(&prompt) {
                continue;
            }
            let id = serde_json::from_value(event["id"].clone())
                .map_err(|error| RewindError::Context(error.to_string()))?;
            if seen.insert(id) {
                targets.push(RewindTarget {
                    id: RewindTargetId::Message(id),
                    prompt: Some(prompt),
                    cut_off: !active.contains(&id),
                });
            }
        }
        let parent = (self.manager().root_id().ok() == Some(self.id()))
            .then(|| session.parent().cloned())
            .flatten();
        if parent.is_some() {
            targets.push(RewindTarget {
                id: RewindTargetId::PlanningSession,
                prompt: None,
                cut_off: false,
            });
        }
        Ok(RewindGuard {
            agent: self.clone(),
            idle,
            journal,
            events,
            targets,
            parent,
        })
    }
}

impl RewindGuard {
    pub fn agent_id(&self) -> AgentId {
        self.agent.id()
    }

    pub fn targets(&self) -> &[RewindTarget] {
        &self.targets
    }

    pub async fn commit(self, target: RewindTargetId) -> Result<RewindOutcome, RewindError> {
        let selected = self
            .targets
            .iter()
            .find(|entry| entry.id == target)
            .ok_or(RewindError::InvalidTarget)?;
        let RewindTargetId::Message(prompt_id) = target else {
            return self
                .parent
                .clone()
                .map(RewindOutcome::PlanningSession)
                .ok_or(RewindError::InvalidTarget);
        };
        let draft = selected
            .prompt
            .as_ref()
            .ok_or(RewindError::InvalidTarget)?
            .user_text()
            .unwrap_or_default()
            .to_owned();
        let agent = self.agent.id();
        let agent_value = serde_json::json!(agent);
        let position = self
            .events
            .iter()
            .position(|event| {
                event["t"] == "msg"
                    && event["agent"] == agent_value
                    && event["id"] == serde_json::json!(prompt_id)
            })
            .ok_or(RewindError::InvalidTarget)?;
        let before = context_entries(&self.events[..position], agent)
            .map_err(|error| RewindError::Context(error.to_string()))?;
        let predecessor = before.last().map(ContextEntry::id);
        let actor = self
            .agent
            .actor()
            .map_err(|error| RewindError::Context(error.to_string()))?;
        let mode = actor
            .effective_config()
            .map(|config| config.mode.id().to_string())
            .unwrap_or_else(|| "build".into());
        let subagents = format_subagents(&self.agent.manager(), agent);
        let note = format!(
            "{REWIND_NOTE} Current mode: {mode}.\n{FILE_WARNING}\n\nExisting subagents:\n{subagents}"
        );
        let conversation = ConversationState::fresh(None);
        let cleared_turns = self.idle.cut_rewind_queue(&self.journal)?;
        let record: SessionRecord<Message, TokenUsage, ToolOutput> = SessionRecord::Fork {
            agent,
            target: predecessor,
            note_id: MakimaId::generate(),
            note,
            conversation: conversation.clone(),
            input_draft: Some(draft.clone()),
            cleared_turns: cleared_turns.clone(),
        };
        let event = serde_json::to_value(&record)
            .map_err(|error| RewindError::Context(error.to_string()))?;
        let mut events = self.journal.context_events();
        if events.is_empty() {
            events = self.events.clone();
        }
        events.push(event.clone());
        let entries = context_entries(&events, agent)
            .map_err(|error| RewindError::Context(error.to_string()))?;
        let mut messages = Vec::new();
        let mut ids = Vec::new();
        for entry in entries {
            ids.push(entry.id());
            messages.push(match entry {
                ContextEntry::Message { d, .. } => serde_json::from_value(d)
                    .map_err(|error| RewindError::Context(error.to_string()))?,
                ContextEntry::Note { note, .. } => Message::observation(note),
            });
        }
        let mut history = self.idle.rewind_history();
        history.epoch = next_epoch();
        history.messages = Arc::new(messages);
        history.ids = Arc::new(ids);
        history.compacted = false;
        history.covered = None;
        history.conversation = Some(conversation);
        let prepared = self
            .journal
            .prepare(JournalCommit {
                incarnations: vec![self.journal.incarnation(agent)],
                histories: [(agent, history.clone())].into(),
                managed_histories: [agent].into(),
                ..Default::default()
            })
            .await?;
        let warning = if let Some(writer) = self.journal.writer() {
            writer
                .append_fork(record)
                .await
                .map_err(|error| RewindError::Persistence(error.to_string()))?
                .warning
        } else if prepared.is_persistent() {
            return Err(RewindError::Persistence(
                "session journal has no writer".into(),
            ));
        } else {
            None
        };
        prepared.install_fork(agent, event)?;
        self.idle.install_rewind(history.clone());
        for turn in cleared_turns {
            let _ = actor.remove(turn);
        }
        Ok(RewindOutcome::Fork {
            history,
            draft,
            warning,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{REWIND_NOTE, RewindError, RewindOutcome, RewindTargetId};
    use crate::{
        AgentInput, AgentLimits, AgentManagerHandle, BackendResult, DoneReason, HistorySnapshot,
        TurnCancellationReason, TurnOutcome, TurnProvenance, TurnResult,
        actor::TurnAdmissionOptions,
        journal::{JournalChange, JournalCommit, SessionJournal},
        session_writer::{Record, SessionIo, SessionWriter, StoredSession},
        test_util::MockBackend,
    };
    use futures_lite::future::poll_once;
    use makima_providers::{Message, TokenUsage};
    use makima_storage::{
        id::{AgentId, MakimaId, TurnId},
        session_state::{ParentLink, StateChange, StoredDeliveryOp},
        sessions::initialize_session_log,
    };
    use std::{
        fs::{File, OpenOptions, read_to_string},
        io::{self, Write},
        sync::Arc,
        time::Duration,
    };
    use test_case::test_case;

    const FIRST: &str = "first user turn";
    const SECOND: &str = "second user turn";
    const REPORT: &str = "consumed child report";

    const REPLACEMENT: &str = "replacement user turn";
    const OLD_DRAFT: &str = "old draft";
    const OLD_QUEUE: &str = "old queued prompt";
    const FORK: &str = "fork";
    const TORN_DRAFT: &[u8] = br#"{"t":"draft","text":"incomplete"#;
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
    const LATE_PROMPT_CANCELLED: &str = "prompt submitted during fork write did not run";

    struct ForkGateIo {
        file: File,
        entered: flume::Sender<()>,
        release: flume::Receiver<()>,
    }

    impl SessionIo for ForkGateIo {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.file.write(bytes)
        }

        fn sync(&mut self) -> io::Result<()> {
            self.file.sync_all()
        }

        fn position(&mut self) -> io::Result<u64> {
            self.entered.send(()).map_err(io::Error::other)?;
            self.release.recv().map_err(io::Error::other)?;
            Ok(self.file.metadata()?.len())
        }
    }

    #[test_case(false; "ready_admission")]
    #[test_case(true; "staged_admission")]
    fn batchable_prompt_submitted_during_fork_write_survives_rewind(block_conversion: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let session = StoredSession::new("test", "/");
            initialize_session_log(directory.path(), &session).unwrap();
            let path = directory.path().join(format!("{}.jsonl", session.id));
            let (entered, writing) = flume::bounded(1);
            let (release, blocked) = flume::bounded(1);
            let writer = SessionWriter::with_io(
                session.id,
                ForkGateIo {
                    file: OpenOptions::new().append(true).open(path).unwrap(),
                    entered,
                    release: blocked,
                },
                session.next_state_seq(),
            );
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let journal = manager.journal();
            journal.bind_writer(writer.clone()).unwrap();
            let (started, running) = flume::bounded(1);
            let (finish, running_gate) = flume::bounded(1);
            let root = manager
                .create_root(
                    vec![Message::user(FIRST.into())],
                    None,
                    Box::new(MockBackend::STRICT.with_turn(move |_, ctx, input, _| {
                        let started = started.clone();
                        let running_gate = running_gate.clone();
                        Box::pin(async move {
                            started.send((ctx.turn_id.unwrap(), input.message)).unwrap();
                            running_gate.recv_async().await.unwrap();
                            BackendResult::EnteredRun(TurnOutcome::completed(
                                ctx.agent_id,
                                ctx.turn_id.unwrap(),
                                TokenUsage::default(),
                                1,
                                DoneReason::EndTurn,
                            ))
                        })
                    })),
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let target = journal.snapshot().histories[&root.id()].ids[0];
            let guard = root.begin_rewind(&session).unwrap();
            let submit = |prompt: &str| {
                actor
                    .submit(
                        AgentInput::new(prompt.into(), Vec::new()),
                        None,
                        String::new(),
                        TurnAdmissionOptions::default(),
                    )
                    .unwrap()
            };
            let conversion = block_conversion.then(|| {
                journal
                    .record_terminal(
                        TurnResult {
                            outcome: TurnOutcome::cancelled(
                                AgentId::generate(),
                                TurnId::generate(),
                                TokenUsage::default(),
                                0,
                                TurnCancellationReason::User,
                            ),
                            text: String::new(),
                            output: Vec::new(),
                            provenance: TurnProvenance::default(),
                            duration_ms: 0,
                        },
                        None,
                    )
                    .unwrap()
                    .unwrap()
            });
            let old = submit(OLD_QUEUE);
            let mut fork = Box::pin(guard.commit(RewindTargetId::Message(target)));
            if let Some(conversion) = conversion {
                let ready = journal.snapshot().fold().unwrap();
                assert!(
                    !ready
                        .scopes
                        .get(&Some(root.id()))
                        .is_some_and(|scope| { scope.admitted.contains_key(&old.turn_id()) })
                );
                assert!(poll_once(fork.as_mut()).await.is_none());
                drop(conversion);
            }
            let fork = smol::spawn(fork);
            writing.recv_async().await.unwrap();
            let late = submit(REPLACEMENT);
            release.send(()).unwrap();
            fork.await.unwrap();
            assert_ne!(old.turn_id(), late.turn_id());
            assert!(matches!(
                old.wait().await,
                TurnOutcome::Cancelled {
                    reason: TurnCancellationReason::User,
                    ..
                }
            ));
            let active = smol::future::race(async { running.recv_async().await.unwrap() }, async {
                late.wait().await;
                panic!("{LATE_PROMPT_CANCELLED}");
            })
            .await;
            assert_eq!(active, (late.turn_id(), REPLACEMENT.into()));

            writer.append(Vec::new()).await.unwrap();
            let reopened = StoredSession::load_from(session.id, directory.path()).unwrap();
            let fork = reopened
                .context_events()
                .iter()
                .find(|event| event["t"] == FORK)
                .unwrap();
            assert_eq!(fork["cleared_turns"], serde_json::json!([old.turn_id()]));
            let state = reopened.latest_state().unwrap();
            let scope = &state.scopes[&Some(root.id())];
            assert!(!scope.admitted.contains_key(&old.turn_id()));
            assert!(!scope.turn_inputs.contains_key(&old.turn_id()));
            assert!(scope.admitted.contains_key(&active.0));
            let replayed: AgentInput =
                serde_json::from_value(scope.turn_inputs[&active.0].clone()).unwrap();
            assert_eq!(replayed.message, active.1);

            finish.send(()).unwrap();
            late.wait().await;
            assert!(
                manager
                    .shutdown(SHUTDOWN_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
            writer.close().await.unwrap();
        });
    }

    #[test_case(false, false; "log_ends_at_fork")]
    #[test_case(false, true; "two_rewinds")]
    #[test_case(true, false; "torn_line_after_fork")]
    fn durable_fork_restores_history_and_input(torn_tail: bool, redo: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let session = StoredSession::new("test", "/");
            let path = directory.path().join(format!("{}.jsonl", session.id));
            let writer = SessionWriter::open_session(directory.path(), &session).unwrap();
            writer
                .append(vec![
                    Record::Draft {
                        text: Some(OLD_DRAFT.into()),
                    },
                    Record::Queued {
                        messages: vec![OLD_QUEUE.into()],
                    },
                ])
                .await
                .unwrap();
            let manager = AgentManagerHandle::new(AgentLimits::default()).unwrap();
            let journal = manager.journal();
            journal.bind_writer(writer.clone()).unwrap();
            let root = manager
                .create_root(
                    vec![Message::user(FIRST.into()), Message::user(SECOND.into())],
                    None,
                    Box::new(MockBackend::STRICT.with_turn(|mut history, ctx, input, _| {
                        Box::pin(async move {
                            history.push(Message::user(input.message)).await.unwrap();
                            BackendResult::EnteredRun(TurnOutcome::completed(
                                ctx.agent_id,
                                ctx.turn_id.unwrap(),
                                TokenUsage::default(),
                                1,
                                DoneReason::EndTurn,
                            ))
                        })
                    })),
                )
                .unwrap();
            let actor = root.actor().unwrap();
            let mut compacted = actor.committed_history_snapshot();
            compacted.compacted = true;
            journal
                .commit(JournalCommit {
                    histories: [(root.id(), compacted)].into(),
                    ..Default::default()
                })
                .unwrap();
            let target = journal.snapshot().histories[&root.id()].ids[1];
            let RewindOutcome::Fork { mut history, .. } = root
                .begin_rewind(&session)
                .unwrap()
                .commit(RewindTargetId::Message(target))
                .await
                .unwrap()
            else {
                panic!("expected fork");
            };
            if redo {
                actor
                    .admit_turn(
                        AgentInput::new(REPLACEMENT.into(), Vec::new()),
                        None,
                        String::new(),
                    )
                    .unwrap()
                    .wait()
                    .await;
                actor.runtime_barrier().await.unwrap();
                let before = journal.snapshot();
                let replacement = &before.histories[&root.id()];
                assert_eq!(
                    replacement.messages.last().unwrap().user_text(),
                    Some(REPLACEMENT)
                );
                let RewindOutcome::Fork {
                    history: second, ..
                } = root
                    .begin_rewind(&session)
                    .unwrap()
                    .commit(RewindTargetId::Message(*replacement.ids.last().unwrap()))
                    .await
                    .unwrap()
                else {
                    panic!("expected fork");
                };
                assert_eq!(&second.ids[..history.ids.len()], history.ids.as_slice());
                history = second;
            }
            assert_eq!(actor.committed_history_snapshot().ids, history.ids);
            let log = read_to_string(&path).unwrap();
            let last: serde_json::Value =
                serde_json::from_str(log.lines().last().unwrap()).unwrap();
            assert_eq!(last["t"], FORK);
            let reopened = StoredSession::load_from(session.id, directory.path()).unwrap();
            assert_eq!(
                reopened.meta.input_draft.as_deref(),
                Some(if redo { REPLACEMENT } else { SECOND })
            );
            assert!(reopened.meta.queued_messages.is_empty());
            assert!(
                manager
                    .shutdown(SHUTDOWN_TIMEOUT)
                    .await
                    .timed_out
                    .is_empty()
            );
            writer.append(Vec::new()).await.unwrap();
            writer.close().await.unwrap();
            if torn_tail {
                let mut file = OpenOptions::new().append(true).open(&path).unwrap();
                file.write_all(TORN_DRAFT).unwrap();
                file.sync_all().unwrap();
            }
            let reopened = StoredSession::load_from(session.id, directory.path()).unwrap();
            let restored = reopened.history_snapshot();
            assert_eq!(restored.messages, history.messages);
            assert_eq!(restored.ids, history.ids);
            assert_eq!(restored.conversation, history.conversation);
            assert!(!restored.compacted);
            let forks = reopened
                .context_events()
                .iter()
                .filter(|event| event["t"] == "fork")
                .count();
            assert_eq!(forks, if redo { 2 } else { 1 });
            assert_eq!(
                reopened.meta.input_draft.as_deref(),
                Some(if redo { REPLACEMENT } else { SECOND })
            );
            assert!(reopened.meta.queued_messages.is_empty());
        });
    }

    fn fixture() -> (AgentManagerHandle, crate::AgentRef, StoredSession) {
        let journal = Arc::new(SessionJournal::nonpersistent());
        let manager =
            AgentManagerHandle::new_with_journal(AgentLimits::default(), journal).unwrap();
        let root = manager
            .create_root(Vec::new(), None, Box::new(MockBackend::STRICT))
            .unwrap();
        let mut session = StoredSession::new("test", "/");
        session.bind_root_id(root.id()).unwrap();
        session.push_message(Message::user(FIRST.into()));
        session.push_message(Message::observation(REPORT.into()));
        session.push_message(Message::user(SECOND.into()));
        let mut history = session.history_snapshot();
        if let Some(previous) = manager.journal().snapshot().histories.get(&root.id()) {
            history.epoch = previous.epoch;
        }
        manager
            .journal()
            .commit(JournalCommit {
                histories: [(root.id(), history)].into(),
                ..Default::default()
            })
            .unwrap();
        (manager, root, session)
    }

    #[test]
    fn drop_and_planning_write_nothing() {
        smol::block_on(async {
            let (manager, root, mut session) = fixture();
            let parent = ParentLink {
                session: MakimaId::generate(),
                message: None,
            };
            session.set_parent(Some(parent.clone()));
            let before = manager.journal().snapshot();
            let events = manager.journal().context_events();
            let guard = root.begin_rewind(&session).unwrap();
            assert_eq!(guard.targets().len(), 3);
            assert!(matches!(
                root.begin_rewind(&session),
                Err(RewindError::Busy)
            ));
            drop(guard);
            assert_eq!(manager.journal().snapshot().revision, before.revision);
            assert_eq!(manager.journal().context_events(), events);
            let outcome = root
                .begin_rewind(&session)
                .unwrap()
                .commit(RewindTargetId::PlanningSession)
                .await
                .unwrap();
            assert!(matches!(outcome, RewindOutcome::PlanningSession(link) if link == parent));
            assert_eq!(manager.journal().snapshot().revision, before.revision);
            root.actor().unwrap().close();
        });
    }

    #[test_case(0; "first_user_turn")]
    #[test_case(2; "later_user_turn")]
    fn fork_cuts_before_prompt_preserves_state_and_supports_redo(index: usize) {
        smol::block_on(async {
            let (manager, root, session) = fixture();
            let child = AgentId::generate();
            let turn = TurnId::generate();
            manager
                .journal()
                .commit(JournalCommit {
                    histories: [(
                        child,
                        HistorySnapshot::new(vec![Message::user(REPORT.into())]),
                    )]
                    .into(),
                    changes: vec![JournalChange {
                        agent: Some(root.id()),
                        generation: 0,
                        change: StateChange::Delivery {
                            id: (child, turn),
                            op: StoredDeliveryOp::Consumed,
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let before = manager.journal().snapshot();
            let selected = session.message_id(index).unwrap();
            let outcome = root
                .begin_rewind(&session)
                .unwrap()
                .commit(RewindTargetId::Message(selected))
                .await
                .unwrap();
            let RewindOutcome::Fork { history, draft, .. } = outcome else {
                panic!("expected fork")
            };
            assert_eq!(draft, if index == 0 { FIRST } else { SECOND });
            assert_eq!(history.messages.len(), index + 1);
            assert!(
                history
                    .messages
                    .last()
                    .unwrap()
                    .first_text_content()
                    .unwrap()
                    .contains(REWIND_NOTE)
            );
            assert_ne!(
                history.conversation,
                session.history_snapshot().conversation
            );
            let after = manager.journal().snapshot();
            assert_eq!(before.state.to_vec(), after.state.to_vec());
            assert!(
                manager
                    .journal()
                    .pending_deliveries(root.id())
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                before.histories[&child].messages,
                after.histories[&child].messages
            );
            let guard = root.begin_rewind(&session).unwrap();
            assert!(
                guard
                    .targets()
                    .iter()
                    .any(|target| target.id == RewindTargetId::Message(selected) && target.cut_off)
            );
            let redo = guard
                .commit(RewindTargetId::Message(session.message_id(2).unwrap()))
                .await
                .unwrap();
            assert!(matches!(redo, RewindOutcome::Fork { draft, .. } if draft == SECOND));
            assert_eq!(
                manager.journal().snapshot().state.to_vec(),
                before.state.to_vec()
            );
            root.actor().unwrap().close();
        });
    }

    #[test]
    fn queued_messages_wait_and_nonidle_rewind_fails() {
        smol::block_on(async {
            let (_, root, session) = fixture();
            let guard = root.begin_rewind(&session).unwrap();
            let actor = root.actor().unwrap();
            let ticket = actor
                .admit_turn(
                    crate::AgentInput::new(FIRST.into(), Vec::new()),
                    None,
                    String::new(),
                )
                .unwrap();
            actor.runtime_barrier().await.unwrap();
            assert!(ticket.peek().is_none());
            assert_eq!(actor.snapshot().queued, 1);
            assert!(matches!(
                root.begin_rewind(&session),
                Err(RewindError::Busy)
            ));
            actor.close();
            drop(guard);
            ticket.wait().await;
        });
    }
}
