use crate::HistorySnapshot;
use crate::agent::{InstructionSource, Instructions};
use crate::context::{ContextBinding, ContextPolicy, ContextUpdate, root_context};
use crate::journal::{JournalCommit, JournalError, SessionJournal};
use crate::prompt::ResolvedSlots;
use crate::state::{StateAccess, StateError, StateScope, StateSlotSpec};
use makima_providers::Message;
use makima_storage::id::AgentId;
use makima_storage::session_state::{StateChange, StoredAgentLifecycle, StoredAgentRecord};
use std::path::PathBuf;
use std::sync::Arc;
use test_case::test_case;

const INSTRUCTION_PATH: &str = "/recorded/project/AGENTS.md";
const INSTRUCTION_LABEL: &str = "Recorded project instructions";
const FIRST: &str = "first rules";
const SECOND: &str = "second rules";
const THIRD: &str = "third rules";
const PLUGIN_SLOT: &str = "context_test/unrelated";

fn root_fixture() -> (Arc<SessionJournal>, AgentId, StateAccess) {
    let journal = Arc::new(SessionJournal::default());
    let root = AgentId::generate();
    journal
        .commit(JournalCommit {
            agents: vec![StoredAgentRecord {
                id: root,
                parent: None,
                depth: 0,
                transcript_key: String::new(),
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
            }],
            histories: [(
                root,
                HistorySnapshot::new(vec![Message::user(FIRST.into())]),
            )]
            .into(),
            ..Default::default()
        })
        .unwrap();
    let access = journal.state_access(root, None);
    (journal, root, access)
}

fn proposal(access: &StateAccess, content: &str, policy: ContextPolicy) -> ContextUpdate {
    ContextBinding::capture(access.clone())
        .unwrap()
        .prepare(
            &Instructions::from_sources(vec![InstructionSource {
                path: PathBuf::from(INSTRUCTION_PATH),
                label: INSTRUCTION_LABEL.into(),
                content: content.into(),
            }]),
            &ResolvedSlots::default(),
            true,
            policy,
        )
        .unwrap()
        .1
}

fn context_commit(update: ContextUpdate) -> JournalCommit {
    JournalCommit {
        context_updates: vec![update],
        ..Default::default()
    }
}

fn assert_context(access: &StateAccess, content: &str) {
    let context = root_context(access).unwrap().unwrap();
    assert_eq!(
        context.instructions.text,
        format!("\n\n{INSTRUCTION_LABEL}:\n{content}")
    );
    assert_eq!(context.instructions.sources.len(), 1);
    assert_eq!(
        context.instructions.sources[0].path,
        PathBuf::from(INSTRUCTION_PATH)
    );
}

#[test]
fn context_proposals_from_same_baseline_use_compare_and_swap() {
    let (journal, _, access) = root_fixture();
    let first = proposal(&access, FIRST, ContextPolicy::Admission);
    let second = proposal(&access, SECOND, ContextPolicy::Admission);
    assert!(!first.changes.is_empty());
    assert!(!second.changes.is_empty());
    journal.commit(context_commit(first)).unwrap();
    let committed = journal.snapshot();
    assert!(matches!(
        journal.commit(context_commit(second)),
        Err(JournalError::State(StateError::Conflict))
    ));
    assert_eq!(journal.snapshot().state, committed.state);
    assert_eq!(journal.snapshot().revision, committed.revision);
    assert_context(&access, FIRST);
}

#[test]
fn unrelated_plugin_commit_does_not_conflict_with_context_proposal() {
    let (journal, _, access) = root_fixture();
    let update = proposal(&access, FIRST, ContextPolicy::Admission);
    let slot = journal
        .register_state::<String>(StateSlotSpec {
            key: PLUGIN_SLOT.into(),
            scope: StateScope::Agent,
            version: 1,
        })
        .unwrap();
    let before = journal.snapshot().revision;
    slot.set(&access, SECOND.into(), None).unwrap();
    assert!(journal.snapshot().revision > before);
    journal.commit(context_commit(update)).unwrap();
    assert_eq!(slot.get(&access).unwrap().value.as_deref(), Some(SECOND));
    assert_context(&access, FIRST);
}

#[test]
fn dropping_prepared_context_preserves_context_and_access_baseline() {
    smol::block_on(async {
        let (journal, root, access) = root_fixture();
        let update = proposal(&access, FIRST, ContextPolicy::Admission);
        let before = journal.snapshot();
        let mut commit = context_commit(update.clone());
        commit.boundary = Some((root, 0));
        commit.state_epoch_agents.insert(root);
        let prepared = journal.prepare(commit).await.unwrap();
        assert!(
            prepared
                .snapshot()
                .state
                .iter()
                .any(|record| matches!(record.change, StateChange::Context { .. }))
        );
        assert_eq!(journal.snapshot().state, before.state);
        assert!(root_context(&access).unwrap().is_none());
        drop(prepared);
        assert_eq!(journal.snapshot().state, before.state);
        assert!(journal.pending_revision().is_none());
        assert!(root_context(&access).unwrap().is_none());
        assert!(Arc::ptr_eq(
            &journal.snapshot().histories[&root].messages,
            &before.histories[&root].messages
        ));
        assert!(ContextBinding::capture(access.clone()).is_ok());
        journal.commit(context_commit(update)).unwrap();
        assert_context(&access, FIRST);
    });
}

#[test_case(false; "abort_releases_context_addresses")]
#[test_case(true; "install_releases_context_addresses")]
fn prepared_context_reserves_each_address_against_raw_context_commits(install: bool) {
    smol::block_on(async {
        let (journal, _, access) = root_fixture();
        let update = proposal(&access, FIRST, ContextPolicy::Admission);
        let changes = update.changes.clone();
        assert!(changes.len() > 1);
        let mut prepared = journal
            .prepare(context_commit(update.clone()))
            .await
            .unwrap();
        let reserved = journal.snapshot();
        for change in &changes {
            assert!(matches!(change.change, StateChange::Context { .. }));
            assert!(matches!(
                journal.commit(JournalCommit {
                    changes: vec![change.clone()],
                    ..Default::default()
                }),
                Err(JournalError::State(StateError::Conflict))
            ));
            assert_eq!(journal.snapshot().state, reserved.state);
            assert_eq!(journal.snapshot().revision, reserved.revision);
        }
        if install {
            prepared.install().unwrap();
        }
        drop(prepared);
        if !install {
            journal.commit(context_commit(update)).unwrap();
        }
        journal
            .commit(JournalCommit {
                changes,
                ..Default::default()
            })
            .unwrap();
        assert_context(&access, FIRST);
    });
}

#[test_case(false; "abort_releases_staged_context_collision")]
#[test_case(true; "install_releases_staged_context_collision")]
fn prepared_context_reservations_cover_stage_and_complete(install: bool) {
    smol::block_on(async {
        let (journal, _, access) = root_fixture();
        let update = proposal(&access, FIRST, ContextPolicy::Admission);
        let raw = update.changes[0].clone();
        assert!(matches!(raw.change, StateChange::Context { .. }));
        let mut prepared = journal
            .prepare(context_commit(update.clone()))
            .await
            .unwrap();
        let reserved = journal.snapshot();
        assert!(matches!(
            journal.stage(
                JournalCommit {
                    changes: vec![raw.clone()],
                    ..Default::default()
                },
                1,
            ),
            Err(JournalError::State(StateError::Conflict))
        ));
        assert!(matches!(
            journal.stage(context_commit(update.clone()), 1),
            Err(JournalError::InvalidPending)
        ));
        assert_eq!(journal.snapshot().state, reserved.state);
        assert_eq!(journal.snapshot().revision, reserved.revision);
        assert_eq!(journal.snapshot().seq, reserved.seq);

        let pending = journal.stage(JournalCommit::default(), 1).unwrap();
        let staged = journal.snapshot();
        assert!(matches!(
            journal.complete(pending.clone(), vec![raw.clone()]),
            Err(JournalError::State(StateError::Conflict))
        ));
        assert_eq!(journal.snapshot().state, staged.state);
        assert_eq!(journal.snapshot().revision, staged.revision);
        assert_eq!(journal.snapshot().seq, staged.seq);
        if install {
            prepared.install().unwrap();
            assert_context(&access, FIRST);
        }
        drop(prepared);
        journal.complete(pending, vec![raw]).unwrap();
        assert!(journal.pending_revision().is_none());
        if !install {
            journal.commit(context_commit(update)).unwrap();
        }
        assert_context(&access, FIRST);
    });
}

#[test]
fn no_write_context_proposal_still_validates_manifest_after_reload() {
    let (journal, _, access) = root_fixture();
    journal
        .commit(context_commit(proposal(
            &access,
            FIRST,
            ContextPolicy::Admission,
        )))
        .unwrap();
    let unchanged = proposal(&access, FIRST, ContextPolicy::Admission);
    assert!(unchanged.changes.is_empty());
    let baseline = journal.snapshot();
    assert_eq!(
        journal.commit(context_commit(unchanged.clone())).unwrap(),
        baseline.revision
    );
    assert_eq!(journal.snapshot().state, baseline.state);
    journal
        .commit(context_commit(proposal(
            &access,
            SECOND,
            ContextPolicy::Reload,
        )))
        .unwrap();
    let reloaded = journal.snapshot();
    assert!(matches!(
        journal.commit(context_commit(unchanged)),
        Err(JournalError::State(StateError::Conflict))
    ));
    assert_eq!(journal.snapshot().state, reloaded.state);
    assert_eq!(journal.snapshot().revision, reloaded.revision);
    assert_context(&access, SECOND);
}

#[test_case(false; "ordinary_commit_rejects_stale_content_baseline")]
#[test_case(true; "prepare_rejects_stale_content_baseline")]
fn content_only_reload_invalidates_no_write_and_competing_reload_proposals(prepared: bool) {
    smol::block_on(async {
        let (journal, _, access) = root_fixture();
        journal
            .commit(context_commit(proposal(
                &access,
                FIRST,
                ContextPolicy::Reload,
            )))
            .unwrap();
        let unchanged = proposal(&access, FIRST, ContextPolicy::Admission);
        assert!(unchanged.changes.is_empty());
        let competing = proposal(&access, THIRD, ContextPolicy::Reload);
        assert_eq!(competing.expected, unchanged.expected);
        assert!(!competing.changes.is_empty());
        let winner = proposal(&access, SECOND, ContextPolicy::Reload);
        assert_eq!(winner.expected, unchanged.expected);
        journal.commit(context_commit(winner)).unwrap();
        let reloaded = journal.snapshot();
        assert_context(&access, SECOND);
        let current = proposal(&access, SECOND, ContextPolicy::Admission);
        assert_ne!(current.expected, unchanged.expected);

        for stale in [unchanged, competing] {
            if prepared {
                assert!(matches!(
                    journal.prepare(context_commit(stale)).await,
                    Err(JournalError::State(StateError::Conflict))
                ));
            } else {
                assert!(matches!(
                    journal.commit(context_commit(stale)),
                    Err(JournalError::State(StateError::Conflict))
                ));
            }
            assert_eq!(journal.snapshot().state, reloaded.state);
            assert_eq!(journal.snapshot().revision, reloaded.revision);
            assert!(journal.pending_revision().is_none());
            assert_context(&access, SECOND);
        }
        journal.commit(context_commit(current)).unwrap();
        assert_context(&access, SECOND);
    });
}

#[test_case(false; "ordinary_commit")]
#[test_case(true; "prepared_install")]
fn invalidated_state_access_cannot_publish_context(prepared: bool) {
    smol::block_on(async {
        let (journal, _, access) = root_fixture();
        let update = proposal(&access, FIRST, ContextPolicy::Admission);
        let before = journal.snapshot();
        if prepared {
            let mut token = journal.prepare(context_commit(update)).await.unwrap();
            journal.invalidate_state_access();
            assert!(matches!(
                token.install(),
                Err(JournalError::State(StateError::StaleAccess))
            ));
            drop(token);
        } else {
            journal.invalidate_state_access();
            assert!(matches!(
                journal.commit(context_commit(update)),
                Err(JournalError::State(StateError::StaleAccess))
            ));
        }
        assert!(matches!(
            ContextBinding::capture(access.clone()),
            Err(StateError::StaleAccess)
        ));
        assert!(matches!(
            root_context(&access),
            Err(StateError::StaleAccess)
        ));
        assert_eq!(journal.snapshot().state, before.state);
        assert!(journal.pending_revision().is_none());
    });
}

#[test]
fn context_commit_for_rejects_stale_incarnation_without_consuming_proposal() {
    let (journal, root, access) = root_fixture();
    journal.set_incarnation(root, 1);
    let old = journal.incarnation(root);
    let update = proposal(&access, FIRST, ContextPolicy::Admission);
    journal.set_incarnation(root, 2);
    let before = journal.snapshot();
    assert!(matches!(
        journal.commit_for(old, context_commit(update.clone())),
        Err(JournalError::StaleIncarnation(agent)) if agent == root
    ));
    assert_eq!(journal.snapshot().state, before.state);
    assert_eq!(journal.snapshot().revision, before.revision);
    journal
        .commit_for(journal.incarnation(root), context_commit(update))
        .unwrap();
    assert_context(&access, FIRST);
}
