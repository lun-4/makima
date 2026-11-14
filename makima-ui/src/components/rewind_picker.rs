use std::collections::BTreeSet;

use crate::click::{HitTarget, PickerId};
use crate::components::Overlay;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::repaint::Cadence;

use crossterm::event::KeyEvent;
use makima_agent::AgentRef;
use makima_agent::actor::HistoryBoundary;
use makima_agent::parent_rewind::ParentRewindEntry;
use makima_agent::rewind::RewindBoundary;
use makima_storage::id::MakimaId;
use makima_storage::session_state::{SessionStateView, StateChange};
use ratatui::Frame;
use ratatui::layout::{Position, Rect};

const TITLE: &str = " Rewind ";
const PREVIEW_MAX_LEN: usize = 80;
pub(crate) const NO_TURNS_MSG: &str = "No user turns to rewind to";

pub enum RewindPickerAction {
    Consumed,
    Select(RewindEntry),
    Close,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewindSelection {
    Boundary {
        boundary: HistoryBoundary,
        durable: Option<RewindBoundary>,
    },
    Parent {
        session: MakimaId,
        seq: u64,
    },
    Branch {
        id: MakimaId,
        expired: bool,
    },
}

#[derive(Clone)]
pub struct RewindEntry {
    pub selection: RewindSelection,
    pub prompt_preview: String,
}

impl RewindEntry {
    fn new(selection: RewindSelection, turn_number: usize, text: &str) -> Self {
        let first_line = text.lines().next().unwrap_or("");
        let preview = &first_line[..first_line.floor_char_boundary(PREVIEW_MAX_LEN)];
        let suffix = if preview.len() < first_line.len() {
            "..."
        } else {
            ""
        };
        Self {
            selection,
            prompt_preview: format!("{turn_number}: {preview}{suffix}"),
        }
    }
}

impl PickerItem for RewindEntry {
    fn label(&self) -> &str {
        &self.prompt_preview
    }

    fn section(&self) -> Option<&str> {
        Some(match self.selection {
            RewindSelection::Boundary { .. } => "Turns",
            RewindSelection::Parent { .. } => "Parent turns",
            RewindSelection::Branch { .. } => "Branches",
        })
    }
}

pub struct RewindPicker {
    picker: ListPicker<RewindEntry>,
    target: Option<AgentRef>,
}

impl RewindPicker {
    pub fn new() -> Self {
        Self {
            picker: ListPicker::new().with_picker(PickerId::Rewind),
            target: None,
        }
    }

    pub fn open_actor(&mut self, target: AgentRef) -> Result<(), String> {
        self.target = None;
        self.picker.close();
        let journal = target.manager().journal();
        let durable = journal.snapshot();
        let view = SessionStateView::fold(&durable.state, &durable.agent_revisions, u64::MAX)
            .map_err(|error| error.to_string())?;
        let pinned: BTreeSet<_> = durable
            .state
            .iter()
            .filter_map(|record| match record.change {
                StateChange::BoundarySnapshot { boundary, .. } => Some(boundary),
                _ => None,
            })
            .collect();
        let mut snapshot: Vec<_> = if journal.session_id().is_some() {
            let history = durable.histories.get(&target.id()).ok_or(NO_TURNS_MSG)?;
            view.boundaries
                .iter()
                .filter_map(|(seq, stored)| {
                    if stored.agent != Some(target.id()) {
                        return None;
                    }
                    let offset = if pinned.contains(seq) {
                        journal
                            .preview_rewind(RewindBoundary {
                                agent: target.id(),
                                seq: *seq,
                                epoch: durable.epoch,
                            })
                            .ok()?
                            .histories
                            .get(&target.id())?
                            .messages
                            .len()
                    } else {
                        let (epoch, _) = stored.lens.get(&target.id())?;
                        if *epoch != history.epoch {
                            return None;
                        }
                        stored.offset
                    };
                    let message = history.messages.get(offset).cloned();
                    Some((
                        HistoryBoundary {
                            agent_id: target.id(),
                            epoch: history.epoch,
                            offset,
                        },
                        message,
                        Some(*seq),
                    ))
                })
                .collect()
        } else {
            target
                .actor()
                .map_err(|error| error.to_string())?
                .history_boundary_messages()
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|(boundary, message)| (boundary, Some(message), None))
                .collect()
        };
        snapshot.sort_by_key(|(boundary, _, _)| boundary.offset);
        snapshot.dedup_by_key(|(boundary, _, _)| boundary.offset);
        let mut affected = BTreeSet::from([target.id()]);
        loop {
            let before = affected.len();
            for record in view.agents.values() {
                if record
                    .parent
                    .is_some_and(|parent| affected.contains(&parent))
                {
                    affected.insert(record.id);
                }
            }
            if affected.len() == before {
                break;
            }
        }
        let session_scope = view
            .agents
            .get(&target.id())
            .is_some_and(|record| record.parent.is_none());
        let mut entries = Vec::with_capacity(snapshot.len());
        for (boundary, message, seq) in snapshot {
            let Some(text) = message
                .as_ref()
                .and_then(|message| message.user_text())
                .map(str::to_owned)
                .or_else(|| {
                    seq.filter(|seq| pinned.contains(seq))
                        .map(|seq| format!("Boundary #{seq}"))
                })
            else {
                continue;
            };
            let seq = seq.filter(|seq| {
                view.boundaries[seq]
                    .lens
                    .keys()
                    .filter(|agent| session_scope || affected.contains(agent))
                    .all(|agent| {
                        view.compactions
                            .get(agent)
                            .is_none_or(|(barrier, _)| *barrier <= *seq)
                    })
            });
            if journal.session_id().is_some() && seq.is_none() {
                continue;
            }
            entries.push(RewindEntry::new(
                RewindSelection::Boundary {
                    boundary,
                    durable: seq.map(|seq| RewindBoundary {
                        agent: target.id(),
                        seq,
                        epoch: durable.epoch,
                    }),
                },
                entries.len() + 1,
                &text,
            ));
        }
        let empty = entries.is_empty();
        entries.reverse();
        self.target = Some(target);
        self.picker.open(entries, TITLE);
        if empty {
            self.picker.close();
            Err(NO_TURNS_MSG.into())
        } else {
            Ok(())
        }
    }

    pub fn append_parent(&mut self, session: MakimaId, entries: Vec<ParentRewindEntry>) {
        let mut items = self.picker.items().unwrap_or_default().to_vec();
        items.extend(entries.into_iter().map(|entry| RewindEntry {
            selection: RewindSelection::Parent {
                session,
                seq: entry.seq,
            },
            prompt_preview: entry.preview,
        }));
        self.picker.open(items, TITLE);
    }

    pub fn append_branches(&mut self, branches: impl IntoIterator<Item = (MakimaId, bool)>) {
        if self.target.is_none() {
            return;
        }
        let mut entries = self.picker.items().unwrap_or_default().to_vec();
        for (id, expired) in branches {
            entries.push(RewindEntry {
                selection: RewindSelection::Branch { id, expired },
                prompt_preview: format!("Branch {id}{}", if expired { " (expired)" } else { "" }),
            });
        }
        self.picker.open(entries, TITLE);
    }

    #[cfg(test)]
    pub fn entry_at_offset(&self, offset: usize) -> Option<RewindEntry> {
        self.picker.items()?.iter().find(|entry| {
            matches!(&entry.selection, RewindSelection::Boundary { boundary, .. } if boundary.offset == offset)
        }).cloned()
    }

    pub fn target(&self) -> Option<AgentRef> {
        self.target.clone()
    }

    pub fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub fn close(&mut self) {
        self.picker.close();
    }

    pub fn select(&mut self, index: usize) -> bool {
        self.picker.select(index)
    }

    pub fn click_input(&mut self, pos: Position) -> bool {
        self.picker.click_input(pos)
    }

    pub fn scroll(&mut self, delta: i32) {
        self.picker.scroll(delta);
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        self.picker.handle_paste(text)
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> RewindPickerAction {
        match self.picker.handle_key(key) {
            PickerAction::Consumed => RewindPickerAction::Consumed,
            PickerAction::Select(entry) => RewindPickerAction::Select(entry),
            PickerAction::Close => RewindPickerAction::Close,
            PickerAction::Toggle(..) | PickerAction::Delete(..) => RewindPickerAction::Consumed,
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        self.picker.view(frame, area)
    }

    pub fn hits(&self) -> Vec<HitTarget> {
        if self.is_open() {
            self.picker.hits()
        } else {
            Vec::new()
        }
    }

    pub fn scroll_line(&self) -> Option<u16> {
        self.is_open().then(|| self.picker.scroll_line())
    }

    pub fn content_len(&self) -> Option<u16> {
        self.is_open().then(|| self.picker.content_len()).flatten()
    }

    pub fn scroll_to_line(&mut self, line: u16) {
        self.picker.scroll_to_line(line);
    }
}

impl Overlay for RewindPicker {
    fn is_open(&self) -> bool {
        self.is_open()
    }

    fn close(&mut self) {
        self.close()
    }

    fn cadence(&self) -> Cadence {
        self.picker.cadence()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::{Future, pending};
    use std::pin::Pin;
    use std::slice;
    use std::time::Duration;

    use makima_agent::actor::ActorHistory;
    use makima_agent::journal::{JournalChange, JournalCommit};
    use makima_agent::parent_rewind::ParentRewindEntry;
    use makima_agent::rewind::RewindBoundary;
    use makima_agent::session_recovery::RecoveryCandidate;
    use makima_agent::{
        ActorBackend, AgentInput, AgentManagerHandle, AgentRef, BackendResult, ControlWork,
        HistorySnapshot, TurnContext, WorkKind,
    };
    use makima_providers::{ContentBlock, Message, Role};
    use makima_storage::id::MakimaId;
    use makima_storage::session_state::{SessionStateView, StateChange, StoredStateRecord};
    use test_case::test_case;

    use super::{NO_TURNS_MSG, PREVIEW_MAX_LEN, RewindPicker, RewindSelection};
    use crate::AppSession;
    use crate::components::list_picker::PickerItem;

    const RESPONSE: &str = "response";
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(pending())
        }

        fn run_control<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(pending())
        }

        fn run_compact<'a>(
            &'a mut self,
            _: ActorHistory<'a>,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(pending())
        }
    }

    fn with_actor(messages: &[Message], test: impl FnOnce(AgentRef)) {
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let target = manager
            .create_root(messages.to_vec(), None, Box::new(IdleBackend))
            .unwrap();
        test(target);
        assert!(
            smol::block_on(manager.shutdown(SHUTDOWN_TIMEOUT))
                .timed_out
                .is_empty()
        );
    }

    fn user_msg(text: &str) -> Message {
        Message::user(text.into())
    }

    fn assistant_msg() -> Message {
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: RESPONSE.into(),
            }],
            ..Default::default()
        }
    }

    #[test_case(&[]; "empty_messages")]
    #[test_case(&[assistant_msg()]; "no_user_turns")]
    #[test_case(&[Message::synthetic("continue".into())]; "only_synthetic")]
    fn open_without_user_turns_returns_error(messages: &[Message]) {
        with_actor(messages, |target| {
            let mut picker = RewindPicker::new();
            assert_eq!(picker.open_actor(target), Err(NO_TURNS_MSG.into()));
        });
    }

    #[test]
    fn entries_are_in_reverse_order() {
        let messages = vec![
            user_msg("first"),
            assistant_msg(),
            user_msg("second"),
            assistant_msg(),
            user_msg("third"),
        ];
        with_actor(&messages, |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target.clone()).unwrap();
            let item = picker.picker.selected_item().unwrap();
            assert_eq!(item.label(), "3: third");
            let RewindSelection::Boundary { boundary, .. } = &item.selection else {
                panic!("expected boundary");
            };
            assert_eq!(boundary.offset, 4);
            assert_eq!(picker.target().unwrap().id(), target.id());
            assert_eq!(boundary.agent_id, target.id());
            assert!(picker.entry_at_offset(1).is_none());
        });
    }

    #[test_case("a".repeat(120), format!("1: {}...", "a".repeat(PREVIEW_MAX_LEN)); "long_prompt")]
    #[test_case("é".repeat(41), format!("1: {}...", "é".repeat(PREVIEW_MAX_LEN / 2)); "unicode_prompt")]
    #[test_case("a".repeat(PREVIEW_MAX_LEN), format!("1: {}", "a".repeat(PREVIEW_MAX_LEN)); "exact_limit")]
    #[test_case("first line\nsecond line".into(), "1: first line".into(); "multiline_prompt")]
    fn preview_uses_first_line_and_truncates_at_character_boundary(text: String, expected: String) {
        let message = user_msg(&text);
        with_actor(slice::from_ref(&message), |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            let item = picker.picker.selected_item().unwrap();
            assert_eq!(item.label(), expected);
            let selected = picker.target().unwrap();
            let RewindSelection::Boundary { boundary, .. } = &item.selection else {
                panic!("expected boundary");
            };
            assert_eq!(
                serde_json::to_value(
                    selected
                        .actor()
                        .unwrap()
                        .history_boundary_message(boundary)
                        .unwrap()
                )
                .unwrap(),
                serde_json::to_value(&message).unwrap(),
            );
        });
    }

    #[test]
    fn display_text_overrides_content() {
        let message = Message::user_display("ai sees this".into(), "user typed this".into());
        with_actor(&[message], |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            assert_eq!(
                picker.picker.selected_item().unwrap().label(),
                "1: user typed this"
            );
        });
    }

    #[test]
    fn synthetic_messages_and_observations_are_excluded() {
        let messages = vec![
            Message::observation("build failed".into()),
            user_msg("real prompt"),
            assistant_msg(),
            Message::synthetic("[Cancelled by user]".into()),
        ];
        with_actor(&messages, |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            let item = picker.picker.selected_item().unwrap();
            assert_eq!(item.label(), "1: real prompt");
            assert!(
                matches!(&item.selection, RewindSelection::Boundary { boundary, .. } if boundary.offset == 1)
            );
        });
    }

    #[test]
    fn branches_remain_selectable_without_turns_and_keep_expired_identity() {
        with_actor(&[], |target| {
            let mut picker = RewindPicker::new();
            assert_eq!(picker.open_actor(target), Err(NO_TURNS_MSG.into()));
            let available = MakimaId::generate();
            let expired = MakimaId::generate();
            picker.append_branches([(available, false), (expired, true)]);
            assert!(picker.is_open());
            assert_eq!(
                picker.picker.items().unwrap()[0].selection,
                RewindSelection::Branch {
                    id: available,
                    expired: false
                },
            );
            assert_eq!(
                picker.picker.items().unwrap()[1].selection,
                RewindSelection::Branch {
                    id: expired,
                    expired: true
                },
            );
            assert!(
                picker.picker.items().unwrap()[1]
                    .label()
                    .ends_with(" (expired)")
            );
        });
    }

    #[test]
    fn parent_entries_preserve_distinct_sequence_identity_at_the_same_offset() {
        const FIRST_SEQ: u64 = 11;
        const APPROVAL_SEQ: u64 = 12;
        const APPROVAL: &str = "Approval point";
        with_actor(&[], |target| {
            let session = MakimaId::generate();
            let mut picker = RewindPicker::new();
            assert!(picker.open_actor(target).is_err());
            picker.append_parent(
                session,
                vec![
                    ParentRewindEntry {
                        seq: APPROVAL_SEQ,
                        offset: 0,
                        preview: APPROVAL.into(),
                    },
                    ParentRewindEntry {
                        seq: FIRST_SEQ,
                        offset: 0,
                        preview: RESPONSE.into(),
                    },
                ],
            );
            let entries = picker.picker.items().unwrap();
            assert_eq!(entries.len(), 2);
            assert_eq!(
                entries[0].selection,
                RewindSelection::Parent {
                    session,
                    seq: APPROVAL_SEQ
                }
            );
            assert_eq!(
                entries[1].selection,
                RewindSelection::Parent {
                    session,
                    seq: FIRST_SEQ
                }
            );
            assert_eq!(entries[0].section(), Some("Parent turns"));
        });
    }

    #[test]
    fn durable_selection_pins_sequence_and_journal_epoch() {
        with_actor(&[user_msg("first")], |target| {
            let actor = target.actor().unwrap();
            let journal = target.manager().journal();
            let session = MakimaId::generate();
            journal.bind_session(session).unwrap();
            let history = actor.history_boundaries().unwrap().remove(0);
            journal
                .commit(JournalCommit {
                    histories: [(
                        target.id(),
                        HistorySnapshot {
                            epoch: history.epoch,
                            compacted: false,
                            conversation: None,
                            messages: actor.committed_history(),
                        },
                    )]
                    .into(),
                    changes: vec![JournalChange {
                        agent: Some(target.id()),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 0,
                            lens: [(target.id(), (history.epoch, 0))].into(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let snapshot = journal.snapshot();
            let mut picker = RewindPicker::new();
            picker.open_actor(target.clone()).unwrap();
            let entry = picker.picker.selected_item().unwrap();
            let RewindSelection::Boundary {
                durable: Some(boundary),
                ..
            } = &entry.selection
            else {
                panic!("expected durable boundary");
            };
            assert_eq!(boundary.agent, target.id());
            assert_eq!(boundary.seq, snapshot.seq);
            assert_eq!(boundary.epoch, snapshot.epoch);
        });
    }

    #[test_case(false; "active_owner_prompt")]
    #[test_case(true; "missing_owner_prompt")]
    fn pinned_root_boundary_stays_selectable_after_child_rewind_and_disk_recovery(
        missing_prompt: bool,
    ) {
        const FIRST: &str = "root before child rewind";
        const SECOND: &str = "root boundary after child work";
        const CHILD: &str = "child before rewind";
        const LATER_CHILD: &str = "child work retained by root pin";
        let messages = [user_msg(FIRST), assistant_msg(), user_msg(SECOND)];
        with_actor(&messages, |root| {
            let journal = root.manager().journal();
            journal.bind_session(MakimaId::generate()).unwrap();
            let mut child_record = journal.agent(root.id()).unwrap();
            let child = makima_agent::AgentId::generate();
            child_record.id = child;
            child_record.parent = Some(root.id());
            child_record.depth = 1;
            child_record.transcript_key = CHILD.into();
            let root_history = HistorySnapshot::new(if missing_prompt {
                messages[..2].to_vec()
            } else {
                messages.to_vec()
            });
            let child_history = HistorySnapshot::new(vec![user_msg(CHILD), user_msg(LATER_CHILD)]);
            journal
                .commit(JournalCommit {
                    agents: vec![child_record],
                    histories: [
                        (root.id(), root_history.clone()),
                        (child, child_history.clone()),
                    ]
                    .into(),
                    changes: vec![JournalChange {
                        agent: Some(child),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 1,
                            lens: [
                                (root.id(), (root_history.epoch, 2)),
                                (child, (child_history.epoch, 1)),
                            ]
                            .into(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let child_boundary = journal.snapshot().seq;
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(root.id()),
                        generation: 0,
                        change: StateChange::Boundary {
                            offset: 2,
                            lens: [
                                (root.id(), (root_history.epoch, 2)),
                                (child, (child_history.epoch, 2)),
                            ]
                            .into(),
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
            let root_boundary = journal.snapshot().seq;
            let mut replacement = smol::block_on(journal.prepare_rewind(RewindBoundary {
                agent: child,
                seq: child_boundary,
                epoch: journal.snapshot().epoch,
            }))
            .unwrap();
            replacement.install().unwrap();
            drop(replacement);
            let mut session = AppSession::new(FIRST, "/");
            journal.snapshot().apply_to(&mut session).unwrap();
            let directory = tempfile::tempdir().unwrap();
            session.save_to(directory.path()).unwrap();
            let loaded = AppSession::load_from(session.id, directory.path()).unwrap();
            let recovered = RecoveryCandidate::prepare(loaded)
                .unwrap()
                .finalize_after_publication();
            let recovered_journal = recovered.journal();
            let snapshot = recovered_journal.snapshot();
            let view =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                    .unwrap();
            assert_eq!(
                view.boundaries[&root_boundary].lens[&root.id()],
                (snapshot.histories[&root.id()].epoch, 2),
            );
            let pin = snapshot
                .state
                .iter()
                .rev()
                .find_map(|record| match &record.change {
                    StateChange::BoundarySnapshot { boundary, snapshot }
                        if *boundary == root_boundary =>
                    {
                        Some(snapshot)
                    }
                    _ => None,
                })
                .unwrap();
            let pinned_state: Vec<StoredStateRecord> =
                serde_json::from_value(pin["state"].clone()).unwrap();
            let pinned_lens = pinned_state
                .iter()
                .find_map(|record| match &record.change {
                    StateChange::Boundary { lens, .. } if record.seq == root_boundary => Some(lens),
                    _ => None,
                })
                .unwrap();
            for (agent, expected) in [
                (root.id(), &messages[..2]),
                (child, child_history.messages.as_slice()),
            ] {
                let history = &pin["histories"][agent.to_string()];
                assert_eq!(history["epoch"].as_u64(), Some(pinned_lens[&agent].0));
                assert_eq!(pinned_lens[&agent].1, expected.len());
                assert_eq!(history["messages"], serde_json::to_value(expected).unwrap());
            }
            assert_eq!(snapshot.histories[&child].messages.len(), 1);
            let manager =
                AgentManagerHandle::new_with_journal(Default::default(), recovered_journal)
                    .unwrap();
            let mut picker = RewindPicker::new();
            picker.open_actor(manager.reference(root.id())).unwrap();
            let entry = picker.entry_at_offset(2).unwrap();
            let label = if missing_prompt {
                format!("1: Boundary #{root_boundary}")
            } else {
                format!("1: {SECOND}")
            };
            assert_eq!(entry.prompt_preview, label);
            let RewindSelection::Boundary {
                boundary,
                durable: Some(durable),
            } = entry.selection
            else {
                panic!("expected recovered pinned boundary");
            };
            assert_eq!(boundary.epoch, snapshot.histories[&root.id()].epoch);
            assert_eq!(durable.seq, root_boundary);
            assert_eq!(durable.epoch, snapshot.epoch);
            let restored = smol::block_on(manager.journal().prepare_rewind(durable)).unwrap();
            assert_eq!(restored.snapshot().histories[&child].messages.len(), 2);
            drop(restored);
            assert!(
                smol::block_on(manager.shutdown(SHUTDOWN_TIMEOUT))
                    .timed_out
                    .is_empty()
            );
        });
    }

    #[test_case(false; "root_checks_child_compaction")]
    #[test_case(true; "child_ignores_root_compaction")]
    fn durable_picker_scopes_compaction_and_reads_dormant_history(child_selected: bool) {
        with_actor(&[user_msg(RESPONSE)], |root| {
            let journal = root.manager().journal();
            let child = makima_agent::AgentId::generate();
            let mut record = journal.agent(root.id()).unwrap();
            record.id = child;
            record.parent = Some(root.id());
            record.depth = 1;
            record.transcript_key = RESPONSE.into();
            let root_history = root
                .actor()
                .unwrap()
                .history_boundaries()
                .unwrap()
                .remove(0);
            let child_history = HistorySnapshot::new(vec![user_msg(RESPONSE)]);
            let lens: BTreeMap<_, _> = [
                (root.id(), (root_history.epoch, 0)),
                (child, (child_history.epoch, 0)),
            ]
            .into();
            journal
                .commit(JournalCommit {
                    agents: vec![record],
                    histories: [
                        (
                            root.id(),
                            HistorySnapshot {
                                epoch: root_history.epoch,
                                compacted: false,
                                conversation: None,
                                messages: root.actor().unwrap().committed_history(),
                            },
                        ),
                        (child, child_history),
                    ]
                    .into(),
                    changes: vec![
                        JournalChange {
                            agent: Some(root.id()),
                            generation: 0,
                            change: StateChange::Boundary {
                                offset: 0,
                                lens: lens.clone(),
                            },
                        },
                        JournalChange {
                            agent: Some(child),
                            generation: 0,
                            change: StateChange::Boundary { offset: 0, lens },
                        },
                        JournalChange {
                            agent: Some(if child_selected { root.id() } else { child }),
                            generation: 0,
                            change: StateChange::Compaction { epoch: 1 },
                        },
                    ],
                    ..Default::default()
                })
                .unwrap();
            journal.bind_session(MakimaId::generate()).unwrap();
            let mut picker = RewindPicker::new();
            let result = picker.open_actor(if child_selected {
                root.manager().reference(child)
            } else {
                root
            });
            if child_selected {
                result.unwrap();
                assert!(matches!(
                    picker.picker.selected_item().unwrap().selection,
                    RewindSelection::Boundary {
                        durable: Some(_),
                        ..
                    }
                ));
            } else {
                assert_eq!(result, Err(NO_TURNS_MSG.into()));
            }
        });
    }

    #[test]
    fn turn_numbers_skip_synthetic() {
        let messages = vec![
            user_msg("first"),
            assistant_msg(),
            Message::synthetic("continue".into()),
            assistant_msg(),
            user_msg("second"),
        ];
        with_actor(&messages, |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            assert_eq!(picker.picker.selected_item().unwrap().label(), "2: second");
        });
    }
}
