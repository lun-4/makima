use crate::{
    AgentRef, History, HistorySnapshot, ToolOutput,
    actor::{ActorError, ActorIdleGuard},
    journal::{JournalError, SessionJournal},
    subagents::format_subagents,
};
use makima_providers::{Message, TokenUsage};
use makima_storage::{
    conversation::ConversationState,
    id::MakimaId,
    session_state::ParentLink,
    sessions::{ContextEntry, Session, SessionRecord, context_entries, next_epoch},
};
use std::{collections::HashSet, sync::Arc};
use thiserror::Error;

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
    #[error("only the root agent can be rewound")]
    NotRoot,
    #[error("the root is not idle; interrupt its turn before rewinding")]
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
        draft: Message,
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
        if self.manager().root_id().map_err(|_| RewindError::NotRoot)? != self.id() {
            return Err(RewindError::NotRoot);
        }
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
        let parent = session.parent().cloned();
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
        let draft = selected.prompt.clone().ok_or(RewindError::InvalidTarget)?;
        let position = self
            .events
            .iter()
            .position(|event| event["t"] == "msg" && event["id"] == serde_json::json!(prompt_id))
            .ok_or(RewindError::InvalidTarget)?;
        let before = context_entries(&self.events[..position], self.agent.id())
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
        let subagents = format_subagents(&self.agent.manager());
        let note = format!(
            "The session was rewound. Current mode: {mode}.\n{FILE_WARNING}\n\nExisting subagents:\n{subagents}"
        );
        let record: SessionRecord<Message, TokenUsage, ToolOutput> = SessionRecord::Fork {
            target: predecessor,
            note,
        };
        let mut events = self.journal.context_events();
        if events.is_empty() {
            events = self.events.clone();
        }
        events.push(
            serde_json::to_value(&record)
                .map_err(|error| RewindError::Context(error.to_string()))?,
        );
        let entries = context_entries(&events, self.agent.id())
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
        history.conversation = Some(ConversationState::fresh(None));
        let _publication = self.journal.reserve_publication().await;
        self.journal.install_rewind(
            self.journal.incarnation(self.agent.id()),
            history.clone(),
            record,
        )?;
        self.idle.install_rewind(history.clone())?;
        self.journal
            .flush()
            .await
            .map_err(RewindError::Persistence)?;
        Ok(RewindOutcome::Fork { history, draft })
    }
}

#[cfg(test)]
mod tests {
    use super::{RewindError, RewindOutcome, RewindTargetId};
    use crate::{
        AgentLimits, AgentManagerHandle, HistorySnapshot,
        journal::{JournalChange, JournalCommit, SessionJournal},
        session_writer::StoredSession,
        test_util::MockBackend,
    };
    use makima_providers::Message;
    use makima_storage::{
        id::{AgentId, MakimaId, TurnId},
        session_state::{ParentLink, StateChange, StoredDeliveryOp},
    };
    use std::sync::Arc;
    use test_case::test_case;

    const FIRST: &str = "first user turn";
    const SECOND: &str = "second user turn";
    const REPORT: &str = "consumed child report";

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
            let RewindOutcome::Fork { history, draft } = outcome else {
                panic!("expected fork")
            };
            assert_eq!(
                draft.user_text(),
                Some(if index == 0 { FIRST } else { SECOND })
            );
            assert_eq!(history.messages.len(), index + 1);
            assert!(
                history
                    .messages
                    .last()
                    .unwrap()
                    .first_text_content()
                    .unwrap()
                    .contains("session was rewound")
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
            assert!(
                matches!(redo, RewindOutcome::Fork { draft, .. } if draft.user_text() == Some(SECOND))
            );
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
