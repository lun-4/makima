use crate::{
    StorageError,
    blobs::BlobHash,
    conversation::ConversationState,
    id::MakimaId,
    session_state::{ParentLink, StateChange, StoredAgentRecord, StoredStateRecord},
    sessions::{Session, TitleSource},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Serialize)]
struct Endpoint<'a, M, T> {
    id: MakimaId,
    cwd: &'a str,
    model: &'a str,
    parent: Option<&'a ParentLink>,
    state: Vec<StoredStateRecord>,
    agents: &'a [(u64, StoredAgentRecord)],
    histories: BTreeMap<String, RetainedHistory>,
    messages: &'a [M],
    submessages: BTreeMap<&'a str, &'a [M]>,
    tool_outputs: BTreeMap<&'a str, &'a T>,
}

#[derive(Serialize)]
struct RetainedHistory {
    epoch: u64,
    compacted: bool,
    conversation: Option<ConversationState>,
}

fn visit_state_epochs(state: &mut [StoredStateRecord], visit: &mut impl FnMut(u64) -> u64) {
    for record in state {
        match &mut record.change {
            StateChange::Boundary { lens, .. } => {
                for (epoch, _) in lens.values_mut() {
                    *epoch = visit(*epoch);
                }
            }
            StateChange::Compaction { epoch } => *epoch = visit(*epoch),
            StateChange::BoundarySnapshot { snapshot, .. } => visit_pin_epochs(snapshot, visit),
            _ => {}
        }
    }
}

fn visit_epoch_value(value: &mut Value, visit: &mut impl FnMut(u64) -> u64) {
    if let Some(epoch) = value.as_u64() {
        *value = visit(epoch).into();
    }
}

fn visit_pin_epochs(snapshot: &mut Value, visit: &mut impl FnMut(u64) -> u64) {
    if let Some(state) = snapshot.get_mut("state").and_then(Value::as_array_mut) {
        for record in state {
            let Some(change) = record.get_mut("change") else {
                continue;
            };
            match change.get("kind").and_then(Value::as_str) {
                Some("boundary") => {
                    if let Some(lens) = change.get_mut("lens").and_then(Value::as_object_mut) {
                        for entry in lens.values_mut() {
                            if let Some(epoch) = entry.get_mut(0) {
                                visit_epoch_value(epoch, visit);
                            }
                        }
                    }
                }
                Some("compaction") => {
                    if let Some(epoch) = change.get_mut("epoch") {
                        visit_epoch_value(epoch, visit);
                    }
                }
                Some("boundary_snapshot") => {
                    if let Some(snapshot) = change.get_mut("snapshot") {
                        visit_pin_epochs(snapshot, visit);
                    }
                }
                _ => {}
            }
        }
    }
    if let Some(histories) = snapshot.get_mut("histories").and_then(Value::as_object_mut) {
        for history in histories.values_mut() {
            if let Some(epoch) = history.get_mut("epoch") {
                visit_epoch_value(epoch, visit);
            }
        }
    }
}

pub fn session_digest<M, U, T>(session: &Session<M, U, T>) -> Result<BlobHash, StorageError>
where
    M: Serialize + DeserializeOwned + TitleSource + Clone,
    U: Serialize + DeserializeOwned + Default,
    T: Serialize + DeserializeOwned,
{
    let mut state = session.state().to_vec();
    let root = session.history_snapshot();
    let mut histories = BTreeMap::from([(
        String::new(),
        RetainedHistory {
            epoch: root.epoch,
            compacted: root.compacted,
            conversation: root.conversation,
        },
    )]);
    let submessages: BTreeMap<_, _> = session
        .subagent_messages()
        .iter()
        .map(|(key, messages)| (key.as_str(), messages.as_slice()))
        .collect();
    for key in submessages.keys() {
        if let Some(history) = session.subagent_history_snapshot(key) {
            histories.insert(
                (*key).to_owned(),
                RetainedHistory {
                    epoch: history.epoch,
                    compacted: history.compacted,
                    conversation: history.conversation,
                },
            );
        }
    }
    let mut distinct = BTreeSet::new();
    visit_state_epochs(&mut state, &mut |epoch| {
        distinct.insert(epoch);
        epoch
    });
    distinct.extend(histories.values().map(|history| history.epoch));
    let ranks: BTreeMap<_, _> = distinct
        .into_iter()
        .enumerate()
        .map(|(rank, epoch)| (epoch, rank as u64))
        .collect();
    visit_state_epochs(&mut state, &mut |epoch| ranks[&epoch]);
    for history in histories.values_mut() {
        history.epoch = ranks[&history.epoch];
    }
    let endpoint = Endpoint {
        id: session.id,
        cwd: &session.cwd,
        model: &session.model,
        parent: session.parent(),
        state,
        agents: session.agent_revisions(),
        histories,
        messages: session.messages(),
        submessages,
        tool_outputs: session
            .tool_outputs()
            .iter()
            .map(|(key, output)| (key.as_str(), output.as_ref()))
            .collect(),
    };
    let mut value = serde_json::to_value(endpoint)?;
    sort_objects(&mut value);
    Ok(BlobHash::digest(&serde_json::to_vec(&value)?))
}

fn sort_objects(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.sort_keys();
            for value in object.values_mut() {
                sort_objects(value);
            }
        }
        Value::Array(array) => {
            for value in array {
                sort_objects(value);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::session_digest;
    use crate::{
        conversation::ConversationState,
        id::{AgentId, MakimaId},
        restore::TransitionRef,
        session_state::{
            ParentLink, StateChange, StoredAgentConfig, StoredBranch, StoredModeSnapshot,
            StoredStateRecord,
        },
        sessions::{HistorySnapshot, Session, next_epoch},
    };
    use serde_json::{Value, json};
    use std::{mem, sync::Arc};
    use tempfile::TempDir;
    use test_case::test_case;

    const MODEL: &str = "digest-model";
    const CWD: &str = "/digest-project";
    const CHILD: &str = "digest-child";
    const TOOL: &str = "digest-tool";
    const CONVERSATION: &str = "digest-conversation";
    const PIN_VERSION: u32 = 1;
    const GENERATION: u64 = 7;

    type TestSession = Session<Value, Value, Value>;

    fn push(session: &mut TestSession, agent: Option<AgentId>, change: StateChange) -> u64 {
        let seq = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq,
            agent,
            generation: GENERATION,
            change,
        });
        seq
    }

    fn fixture(pinned: bool) -> TestSession {
        let mut session = TestSession::new(MODEL, CWD);
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let child = AgentId::generate();
        let mut record = session.agents()[&root].clone();
        record.id = child;
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = CHILD.into();
        record.created_seq = session.next_state_seq();
        record.updated_seq = record.created_seq;
        session.push_agent_revision(record.created_seq, record);
        push(
            &mut session,
            Some(root),
            StateChange::Config(StoredAgentConfig {
                cwd: CWD.into(),
                model: MODEL.into(),
                thinking: None,
                fast: false,
                workflow: false,
                mode: StoredModeSnapshot {
                    id: "build".into(),
                    label: "Build".into(),
                    system_prompt: None,
                    restrict_write_to: None,
                    tools: None,
                },
                options: Default::default(),
            }),
        );
        let mut history = HistorySnapshot::new(vec![json!({"content": "root", "id": "root-id"})]);
        history.conversation = Some(ConversationState::new(Arc::from(CONVERSATION)));
        session.adopt_history(&history);
        let mut child_history =
            HistorySnapshot::new(vec![json!({"content": "child", "id": "child-id"})]);
        child_history.conversation = Some(ConversationState::new(Arc::from("child-conversation")));
        session.adopt_subagent_history(CHILD.into(), &child_history);
        session.insert_tool_output(TOOL.into(), json!({"output": "result"}));
        let old = next_epoch();
        let boundary = push(
            &mut session,
            Some(root),
            StateChange::Boundary {
                offset: 1,
                lens: [(root, (old, 1)), (child, (child_history.epoch, 1))].into(),
            },
        );
        push(
            &mut session,
            Some(root),
            StateChange::Compaction {
                epoch: history.epoch,
            },
        );
        if pinned {
            let nested = json!({
                "version": PIN_VERSION,
                "histories": {
                    root.to_string(): {"epoch": old, "compacted": false, "messages": []},
                    child.to_string(): {"epoch": child_history.epoch, "compacted": false, "messages": []}
                },
                "state": session.state(),
                "agents": session.agent_revisions()
            });
            let mut pin = nested.clone();
            pin["state"].as_array_mut().unwrap().push(json!({
                "seq": session.next_state_seq(),
                "agent": root,
                "generation": GENERATION,
                "change": {"kind": "boundary_snapshot", "boundary": boundary, "snapshot": nested}
            }));
            push(
                &mut session,
                Some(root),
                StateChange::BoundarySnapshot {
                    boundary,
                    snapshot: pin,
                },
            );
        }
        session
    }

    #[test_case(false; "boundary_and_compaction")]
    #[test_case(true; "nested_boundary_pins")]
    fn digest_survives_disk_epoch_remapping(pinned: bool) {
        let directory = TempDir::new().unwrap();
        let mut session = fixture(pinned);
        let expected = session_digest(&session).unwrap();
        session.save_to(directory.path()).unwrap();
        let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_ne!(
            session.history_snapshot().epoch,
            loaded.history_snapshot().epoch
        );
        assert_ne!(session.state(), loaded.state());
        assert_eq!(session_digest(&loaded).unwrap(), expected);
        loaded.save_to(directory.path()).unwrap();
        let reloaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_ne!(
            loaded.history_snapshot().epoch,
            reloaded.history_snapshot().epoch
        );
        assert_eq!(session_digest(&reloaded).unwrap(), expected);
    }

    #[test_case("message")]
    #[test_case("message_id")]
    #[test_case("submessage")]
    #[test_case("submessage_id")]
    #[test_case("agent_record")]
    #[test_case("child_conversation")]
    #[test_case("tool_output")]
    #[test_case("model")]
    #[test_case("cwd")]
    #[test_case("config")]
    #[test_case("conversation")]
    #[test_case("compacted")]
    #[test_case("parent")]
    #[test_case("epoch_partition")]
    #[test_case("current_epoch_partition")]
    #[test_case("pinned_epoch_partition")]
    fn semantic_changes_change_digest(change: &str) {
        let session = fixture(true);
        let expected = session_digest(&session).unwrap();
        let mut changed = session.clone();
        match change {
            "message" => changed.push_message(json!({"content": "changed"})),
            "message_id" => {
                let mut messages = changed.messages().to_vec();
                messages[0]["id"] = json!("changed");
                changed.replace_messages(messages);
            }
            "submessage_id" | "child_conversation" => {
                let mut history = changed.subagent_history_snapshot(CHILD).unwrap();
                if change == "submessage_id" {
                    Arc::make_mut(&mut history.messages)[0]["id"] = json!("changed");
                } else {
                    history.conversation = Some(ConversationState::new(Arc::from("changed")));
                }
                changed.adopt_subagent_history(CHILD.into(), &history);
            }
            "agent_record" => {
                let mut revisions = changed.agent_revisions().to_vec();
                revisions[0].1.label = Some("changed".into());
                changed.replace_agent_revisions(revisions);
            }
            "submessage" => changed.set_subagent_messages(CHILD.into(), vec![json!("changed")]),
            "tool_output" => changed.insert_tool_output(TOOL.into(), json!("changed")),
            "model" => changed.set_model("changed".into()),
            "cwd" => changed.set_cwd("/changed".into()),
            "config" => {
                let mut state = changed.state().to_vec();
                for record in &mut state {
                    if let StateChange::Config(config) = &mut record.change {
                        config.fast = !config.fast;
                    }
                }
                changed.replace_state(state);
            }
            "conversation" => {
                changed.meta.conversation = Some(ConversationState::new(Arc::from("changed")))
            }
            "compacted" => changed.meta.compacted = !changed.meta.compacted,
            "parent" => changed.set_parent(Some(ParentLink {
                session: MakimaId::generate(),
                message_count: 1,
                seq: 0,
            })),
            "current_epoch_partition" => {
                let mut history = changed.history_snapshot();
                history.epoch = next_epoch();
                changed.adopt_history(&history);
            }
            "epoch_partition" | "pinned_epoch_partition" => {
                let root = changed.root_agent_id().unwrap();
                let current = changed.history_snapshot().epoch;
                let mut state = changed.state().to_vec();
                for record in &mut state {
                    match &mut record.change {
                        StateChange::Boundary { lens, .. } if change == "epoch_partition" => {
                            lens.get_mut(&root).unwrap().0 = current;
                        }
                        StateChange::BoundarySnapshot { snapshot, .. }
                            if change == "pinned_epoch_partition" =>
                        {
                            snapshot["histories"][root.to_string()]["epoch"] = json!(current);
                        }
                        _ => {}
                    }
                }
                changed.replace_state(state);
            }
            _ => unreachable!(),
        }
        assert_ne!(session_digest(&changed).unwrap(), expected);
    }

    #[test_case(false; "monotonic_rename_is_stable")]
    #[test_case(true; "reversing_global_order_changes_digest")]
    fn epoch_renaming_preserves_global_order(reversed: bool) {
        let mut session = fixture(true);
        let expected = session_digest(&session).unwrap();
        let mut rename = |epoch| {
            if reversed {
                u64::MAX - epoch
            } else {
                const OFFSET: u64 = 1_000_000;
                epoch + OFFSET
            }
        };
        let mut history = session.history_snapshot();
        history.epoch = rename(history.epoch);
        session.adopt_history(&history);
        let mut history = session.subagent_history_snapshot(CHILD).unwrap();
        history.epoch = rename(history.epoch);
        session.replace_journal_histories(
            &[
                (session.root_agent_id().unwrap(), session.history_snapshot()),
                (
                    session
                        .agents()
                        .values()
                        .find(|agent| agent.transcript_key == CHILD)
                        .unwrap()
                        .id,
                    history,
                ),
            ]
            .into(),
        );
        let mut state = session.state().to_vec();
        super::visit_state_epochs(&mut state, &mut rename);
        session.replace_state(state);
        assert_eq!(session_digest(&session).unwrap() == expected, !reversed);
    }

    #[test]
    fn swapping_current_cross_agent_epoch_order_changes_digest() {
        let mut session = fixture(true);
        let expected = session_digest(&session).unwrap();
        let mut root = session.history_snapshot();
        let mut child = session.subagent_history_snapshot(CHILD).unwrap();
        let child_id = session
            .agents()
            .values()
            .find(|agent| agent.transcript_key == CHILD)
            .unwrap()
            .id;
        assert!(root.epoch < child.epoch);
        mem::swap(&mut root.epoch, &mut child.epoch);
        session.replace_journal_histories(
            &[(session.root_agent_id().unwrap(), root), (child_id, child)].into(),
        );
        assert_ne!(session_digest(&session).unwrap(), expected);
    }

    #[test]
    fn checkpoint_attachments_and_ui_metadata_do_not_change_digest() {
        let mut session = fixture(true);
        let expected = session_digest(&session).unwrap();
        session.title = "changed".into();
        session.updated_at = session.updated_at.wrapping_add(1);
        session.meta.input_draft = Some("changed".into());
        session.push_restore_ref(TransitionRef {
            session: session.id,
            transition: MakimaId::generate(),
        });
        session.push_branch(StoredBranch {
            id: MakimaId::generate(),
            created_at: session.created_at,
            at_seq: session.next_state_seq(),
            agent: session.root_agent_id().unwrap(),
            offset: session.messages().len(),
            transition: None,
        });
        assert_eq!(session_digest(&session).unwrap(), expected);
    }

    #[test]
    fn payload_object_order_does_not_change_digest() {
        let mut session = fixture(false);
        session.insert_tool_output(
            TOOL.into(),
            serde_json::from_str(r#"{"a":1,"z":{"b":2,"y":3}}"#).unwrap(),
        );
        let expected = session_digest(&session).unwrap();
        session.insert_tool_output(
            TOOL.into(),
            serde_json::from_str(r#"{"z":{"y":3,"b":2},"a":1}"#).unwrap(),
        );
        assert_eq!(session_digest(&session).unwrap(), expected);
    }
}
