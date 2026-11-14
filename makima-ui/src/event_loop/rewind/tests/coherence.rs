use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use makima_agent::actor::{ActorLifecycle, ActorStatus, TurnAdmissionOptions};
use makima_agent::plan_state::PlanProjectionRollback;
use makima_agent::session_options::{DISABLED_VALUE, ENABLED_VALUE, WORKFLOW_OPTION_ID};
use makima_agent::state::{
    StateChangeEvent, StateChangeReason, StateRevision, StateScope, StateSlotSpec,
};
use makima_agent::{AgentInput, AgentRef, CancelToken};
use makima_commands::{CommandOutcome, InputDispatch};
use makima_storage::restore::RestoreChoice;
use makima_storage::session_state::{SessionStateView, StateChange};
use serde_json::json;

use super::super::super::tests::{pump_until, with_event_loop};
use super::super::super::{EventLoop, InternalEvent};
use super::{
    FIRST, SECOND, THIRD, WAIT, assert_persisted_state, boundary, checkpoint, setup, target, turn,
};
use crate::AppSession;
use crate::components::Action;

const PLUGIN: &str = "rewind-coherence";
const SPAWN: &str = "/spawn-rewind-later";
const CHILD: &str = "post-boundary child";
const SLOT: &str = "rewind_coherence/value";

fn spawn(event_loop: &mut EventLoop<'_>, index: usize, host: &makima_lua::PluginHost) -> AgentRef {
    host.load_source(
        PLUGIN,
        &format!(
            r#"
        makima.api.register_command({{
            name = "{SPAWN}", tui_only = false,
            handler = function(_, ctx)
                assert(makima.agent.spawn(ctx, {{ name = "{CHILD}", system = "child",
                    tools = makima.json.decode('[]'), inherit_provider = true,
                    auto_deliver = false, silent = true }}))
            end,
        }})
    "#
        ),
    )
    .unwrap();
    let command = event_loop.sessions[index].app.command_target.clone();
    let dispatched = smol::block_on(
        event_loop
            .ctx
            .command_runtime
            .registry
            .dispatch_input(&command, SPAWN.into()),
    );
    assert!(
        matches!(
            dispatched,
            InputDispatch::Dispatched(CommandOutcome::Completed)
        ),
        "spawn command failed: {dispatched:?}"
    );
    let root = target(event_loop, index);
    let mut child = None;
    pump_until(event_loop, |_| {
        child = root
            .manager()
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .and_then(|node| root.manager().lookup(node.agent_id).ok());
        child.is_some()
    });
    child.unwrap()
}

#[test]
fn runtimes_share_projection_and_tool_write_registry() {
    with_event_loop(|event_loop| {
        let (first, _first_host) = setup(event_loop);
        let (second, _second_host) = setup(event_loop);
        let shared = &event_loop.ctx.file_write_locks;
        for index in [first, second] {
            assert!(Arc::ptr_eq(
                shared,
                &event_loop.sessions[index]
                    .handles
                    .orchestration_template
                    .file_write_locks,
            ));
        }
    });
}

#[test]
fn retiring_runtime_cancels_blocked_projection_capture_without_writing() {
    const PLAN: &str = "approved plan before runtime retirement";
    with_event_loop(|event_loop| {
        let (index, _host) = setup(event_loop);
        let path = super::plan_fixture(event_loop, index, PLAN);
        let root = target(event_loop, index);
        let snapshot = root.manager().journal().snapshot();
        let affected = BTreeSet::from([root.id()]);
        let locks = Arc::clone(&event_loop.ctx.file_write_locks);
        let cancel = event_loop.sessions[index].lifetime_cancel.clone();
        smol::block_on(async {
            let held = PlanProjectionRollback::capture_locked(
                &snapshot,
                &affected,
                &locks,
                &CancelToken::none(),
            )
            .await
            .unwrap();
            let mut waiting = Box::pin(PlanProjectionRollback::capture_locked(
                &snapshot, &affected, &locks, &cancel,
            ));
            assert!(
                futures_lite::future::poll_once(waiting.as_mut())
                    .await
                    .is_none()
            );
            let runtime = event_loop.remove_runtime(index);
            assert!(cancel.is_cancelled());
            assert!(waiting.await.is_err());
            assert_eq!(fs::read_to_string(&path).unwrap(), PLAN);
            drop(held);
            event_loop.sessions.push(runtime);
        });
    });
}

#[test]
fn child_preview_projects_current_session_plan_slot_for_rewind_and_branch() {
    const PLAN_FILE: &str = "child-session-plan.md";
    const HISTORICAL_PLAN: &str = "historical child plan";
    const CURRENT_PLAN: &str = "current session child plan";
    const LATEST_PLAN: &str = "session plan after child rewind";
    const STALE_FILE: &str = "stale projection";
    with_event_loop(|event_loop| {
        let (index, host) = setup(event_loop);
        let root = target(event_loop, index);
        let child = spawn(event_loop, index, &host);
        let actor = child.actor().unwrap();
        pump_until(event_loop, |_| {
            root.actor().unwrap().prepare_idle().is_ok() && actor.prepare_idle().is_ok()
        });
        let path = PathBuf::from(&event_loop.session_cwd).join(PLAN_FILE);
        super::approve_plan(&child, &path, HISTORICAL_PLAN);
        for text in [FIRST, SECOND] {
            let ticket = actor
                .submit(
                    AgentInput {
                        message: text.into(),
                        images: Vec::new(),
                        preamble: Vec::new(),
                        prompt: None,
                    },
                    None,
                    text.into(),
                    TurnAdmissionOptions::default(),
                )
                .unwrap();
            pump_until(event_loop, |_| {
                ticket.peek().is_some() && actor.prepare_idle().is_ok()
            });
        }
        checkpoint(event_loop, index);
        super::approve_plan(&child, &path, CURRENT_PLAN);
        let (history, durable) = super::boundary_for(&child, SECOND);
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: child.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        super::finish_restore(event_loop, index);
        assert_eq!(fs::read_to_string(&path).unwrap(), CURRENT_PLAN);
        let branch = event_loop.sessions[index]
            .app
            .state
            .session
            .branches()
            .last()
            .unwrap()
            .id;
        assert!(
            event_loop.sessions[index]
                .app
                .state
                .session
                .load_branch_from(branch, &event_loop.ctx.sessions_dir)
                .is_ok()
        );
        super::approve_plan(&child, &path, LATEST_PLAN);
        fs::write(&path, STALE_FILE).unwrap();
        event_loop.handle_action(
            index,
            Action::RestoreBranch {
                target: child.clone(),
                id: branch,
            },
        );
        super::finish_restore(event_loop, index);
        assert_eq!(fs::read_to_string(&path).unwrap(), LATEST_PLAN);
        let disk =
            AppSession::load(event_loop.sessions[index].id(), &event_loop.ctx.storage).unwrap();
        let plans = makima_agent::plan_state::plan_entries(&disk.latest_state().unwrap()).unwrap();
        assert_eq!(
            plans[&child.id()].approved.as_ref().unwrap().content,
            LATEST_PLAN
        );
    });
}

#[test]
fn root_restore_removes_later_resident_child_and_releases_only_coherent_notices() {
    with_event_loop(|event_loop| {
        let (index, host) = setup(event_loop);
        let root = target(event_loop, index);
        let journal = root.manager().journal();
        let slot = journal
            .register_state::<serde_json::Value>(StateSlotSpec {
                key: SLOT.into(),
                scope: StateScope::Agent,
                version: 1,
            })
            .unwrap();
        slot.set(&root.state_access(), json!({"value": FIRST}), None)
            .unwrap();
        turn(event_loop, index, FIRST);
        turn(event_loop, index, SECOND);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = root.actor().unwrap().committed_history()[..history.offset].to_vec();
        let child = spawn(event_loop, index, &host);
        let child_actor = child.actor().unwrap();
        let child_access = child.state_access();
        let incarnation = child.incarnation().unwrap();
        let ticket = child_actor
            .submit(
                AgentInput {
                    message: CHILD.into(),
                    images: Vec::new(),
                    preamble: Vec::new(),
                    prompt: None,
                },
                None,
                CHILD.into(),
                TurnAdmissionOptions::default(),
            )
            .unwrap();
        pump_until(event_loop, |_| {
            ticket.peek().is_some() && child_actor.snapshot().status == ActorStatus::Idle
        });
        slot.set(&root.state_access(), json!({"value": THIRD}), None)
            .unwrap();
        slot.set(&child_access, json!({"value": CHILD}), None)
            .unwrap();
        turn(event_loop, index, THIRD);
        checkpoint(event_loop, index);
        let displaced_seq = journal.snapshot().seq;
        let notices = journal.subscribe_state_changes();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        super::confirm_choice(event_loop, RestoreChoice::ConversationOnly);
        let ready = loop {
            let event = event_loop.internal_rx.recv_timeout(WAIT).unwrap();
            if matches!(event, InternalEvent::RestoreReady { .. }) {
                break event;
            }
            event_loop.handle_internal(event);
        };
        assert!(notices.try_recv().is_err());
        event_loop.handle_internal(ready);
        assert!(!event_loop.sessions[index].app.restore_in_progress);
        let mut received: Vec<_> = notices.try_iter().collect();
        let restored_slot = slot.get(&root.state_access()).unwrap();
        let snapshot = journal.snapshot();
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .unwrap();
        let removed_slot = &view.scopes[&Some(child.id())].plugins[SLOT];
        assert!(removed_slot.value.is_none());
        assert!(removed_slot.revision > displaced_seq);
        let mut expected_notices = vec![
            StateChangeEvent {
                key: SLOT.into(),
                agent: Some(root.id()),
                revision: restored_slot.revision,
                reason: StateChangeReason::Rewind,
            },
            StateChangeEvent {
                key: SLOT.into(),
                agent: Some(child.id()),
                revision: StateRevision(removed_slot.revision),
                reason: StateChangeReason::Rewind,
            },
        ];
        received.sort_by_key(|event| event.agent);
        expected_notices.sort_by_key(|event| event.agent);
        assert_eq!(received, expected_notices);
        assert_eq!(restored_slot.value, Some(json!({"value": FIRST})));
        assert!(slot.get(&child_access).is_err());
        assert!(child.actor().is_err());
        assert_ne!(child_actor.snapshot().lifecycle, ActorLifecycle::Open);
        assert!(root.manager().validate_activation(&incarnation).is_err());
        assert!(
            !root
                .manager()
                .snapshot()
                .iter()
                .any(|node| node.agent_id == child.id())
        );
        assert!(
            !event_loop.sessions[index]
                .app
                .agent_chat_index
                .contains_key(&child.id())
        );
        assert!(
            !snapshot
                .agent_revisions
                .iter()
                .any(|(_, record)| record.id == child.id())
        );
        assert!(!snapshot.histories.contains_key(&child.id()));
        let child_records: Vec<_> = snapshot
            .state
            .iter()
            .filter(|record| record.agent == Some(child.id()))
            .collect();
        assert_eq!(child_records.len(), 1);
        assert_eq!(child_records[0].seq, removed_slot.revision);
        assert!(matches!(
            &child_records[0].change,
            StateChange::Plugin { key, version: 1, value: None } if key == SLOT
        ));
        assert_eq!(
            serde_json::to_value(root.actor().unwrap().committed_history().as_ref()).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        let runtime = &event_loop.sessions[index];
        assert_eq!(
            serde_json::to_value(runtime.app.state.session.messages()).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        let disk = AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap();
        assert_persisted_state(&disk, &snapshot);
        assert_eq!(disk.agent_revisions(), snapshot.agent_revisions);
        assert_eq!(
            serde_json::to_value(disk.messages()).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        let view = disk.latest_state().unwrap();
        let config = root.effective_config().unwrap().unwrap();
        let stored = view.scopes[&Some(root.id())].config.as_ref().unwrap();
        assert_eq!(stored.workflow, config.workflow);
        assert_eq!(stored.cwd, config.context.cwd().to_string_lossy());
        assert_eq!(
            runtime
                .coordinator
                .read()
                .options()
                .options
                .iter()
                .find(|option| option.definition.id.as_ref() == WORKFLOW_OPTION_ID)
                .unwrap()
                .current_value
                .as_ref(),
            if config.workflow {
                ENABLED_VALUE
            } else {
                DISABLED_VALUE
            }
        );
        assert!(Arc::ptr_eq(
            &runtime.handles.history.load_full().messages,
            &root.actor().unwrap().committed_history()
        ));
        assert!(
            snapshot
                .state
                .iter()
                .any(|record| matches!(record.change, StateChange::Plugin { .. }))
        );
        assert!(root.actor().unwrap().prepare_idle().is_ok());
    });
}
