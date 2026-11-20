mod coherence;
mod outside_snapshot;

use super::super::tests::{pump_until, sync_preparer, with_event_loop};
use super::super::{EventLoop, InternalEvent, PreparedProvider, reserve_config_change};
use crate::AppSession;
use crate::chat::Chat;
use crate::components::{Action, DisplayRole, Status};
use makima_agent::actor::{ActorStatus, ConfigChange, ConfigPatch, HistoryBoundary, PreparedModel};
use makima_agent::command::CONTEXT_RELOAD_ROOT_ONLY;
use makima_agent::context::root_context;
use makima_agent::journal::{JournalCommit, SessionJournal, SessionJournalSnapshot};
use makima_agent::plan_state::{approval_edit, plan_entries};
use makima_agent::rewind::RewindBoundary;
use makima_agent::session_options::{DISABLED_VALUE, ENABLED_VALUE, WORKFLOW_OPTION_ID};
use makima_agent::state::{StateScope, StateSlotSpec};
use makima_agent::tools::ToolRegistry;
use makima_agent::{AgentInput, AgentMode, AgentRef, Post, PostLevel, TurnOutcome};
use makima_commands::{CommandError, CommandOutcome, InputDispatch};
use makima_config::{Effect, PermissionRule, ToolKey};
use makima_lua::PluginHost;
use makima_providers::provider::{BoxFuture, Provider};
use makima_providers::{
    AgentError, ContentBlock, Invocation, Message, Model, ProviderEvent, RequestDelivery,
    RequestOptions, Role, StopReason, StreamResponse, TokenUsage,
};
use makima_storage::checkpoint::{CheckpointRequest, CheckpointVersion};
use makima_storage::id::SessionRef;
use makima_storage::session_lock;
use makima_storage::session_state::{ParentLink, StateChange};
use makima_storage::sessions::StoredRule;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use test_case::test_case;

const MODEL: &str = "anthropic/claude-opus-4-6";
const FIRST: &str = "first rewind prompt";
const SECOND: &str = "second rewind prompt";
const THIRD: &str = "third rewind prompt";
const RESPONSE: &str = "rewind test response";
const RELOAD_CONTEXT: &str = "/reload-context";
const WAIT: Duration = Duration::from_secs(10);

struct CompletingProvider {
    systems: Option<flume::Sender<String>>,
}

impl Provider for CompletingProvider {
    fn stream_message<'a>(
        &'a self,
        _: &'a Model,
        _: &'a [Message],
        system: &'a str,
        _: &'a serde_json::Value,
        _: &'a flume::Sender<ProviderEvent>,
        _: RequestOptions,
        _: Option<&'a SessionRef>,
        _: &'a Invocation,
        _: RequestDelivery,
    ) -> BoxFuture<'a, Result<StreamResponse, AgentError>> {
        Box::pin(async move {
            if let Some(systems) = &self.systems {
                systems.send(system.into()).unwrap();
            }
            Ok(StreamResponse {
                message: Message {
                    role: Role::Assistant,
                    content: vec![ContentBlock::Text {
                        text: RESPONSE.into(),
                    }],
                    ..Default::default()
                },
                usage: TokenUsage::default(),
                stop_reason: Some(StopReason::EndTurn),
            })
        })
    }
}

fn setup(event_loop: &mut EventLoop<'_>) -> (usize, PluginHost) {
    setup_with_provider(event_loop, Arc::new(CompletingProvider { systems: None }))
}

fn setup_with_provider(
    event_loop: &mut EventLoop<'_>,
    provider: Arc<dyn Provider>,
) -> (usize, PluginHost) {
    let host = PluginHost::with_command_registry(
        Arc::new(ToolRegistry::new()),
        event_loop.ctx.command_runtime.registry.clone(),
        false,
        makima_lua::ui_sink::HostUi::Frontend,
    )
    .unwrap();
    host.event_handle()
        .install_orchestration_services(event_loop.orchestration.clone());
    event_loop.ctx.lua_event_handle = host.event_handle();
    let prepared_provider = Arc::clone(&provider);
    event_loop.ctx.prepare_provider = sync_preparer(move |model| {
        Ok(PreparedModel {
            model,
            provider: Arc::clone(&prepared_provider),
        })
    });
    let runtime = event_loop
        .ctx
        .spawn_runtime_with_provider(
            AppSession::new(MODEL, &event_loop.session_cwd),
            Some(PreparedProvider {
                model: Model::from_spec(MODEL).unwrap(),
                provider,
            }),
        )
        .unwrap();
    let index = event_loop.sessions.len();
    event_loop.push_runtime(runtime);
    event_loop.set_focused(index);
    event_loop.sync_orchestration();
    (index, host)
}

fn target(event_loop: &EventLoop<'_>, index: usize) -> AgentRef {
    let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
    manager.lookup(root).unwrap()
}

fn turn(event_loop: &mut EventLoop<'_>, index: usize, text: &str) {
    event_loop.submit_text(index, text.into()).unwrap();
    pump_until(event_loop, |event_loop| {
        let actor = target(event_loop, index).actor().unwrap();
        actor.snapshot().status == ActorStatus::Idle
            && actor
                .committed_history()
                .iter()
                .any(|message| message.user_text() == Some(text))
            && event_loop.sessions[index].app.status == Status::Idle
    });
}

fn two_turn_fixture(event_loop: &mut EventLoop<'_>) -> (usize, PluginHost) {
    let (index, host) = setup(event_loop);
    turn(event_loop, index, FIRST);
    turn(event_loop, index, SECOND);
    (index, host)
}

fn reload_context_command(event_loop: &mut EventLoop<'_>, index: usize) -> InputDispatch {
    let registry = event_loop.ctx.command_runtime.registry.clone();
    let command_target = event_loop.sessions[index].app.command_target.clone();
    let (completed, result) = flume::bounded(1);
    smol::spawn(async move {
        let outcome = registry
            .dispatch_input(&command_target, RELOAD_CONTEXT.into())
            .await;
        completed.send(outcome).unwrap();
    })
    .detach();
    let command = event_loop
        .ctx
        .command_runtime
        .recv_timeout_for_test(WAIT)
        .unwrap();
    event_loop.handle_command(command);
    pump_until(event_loop, |_| !result.is_empty());
    result.try_recv().unwrap()
}

fn checkpoint(event_loop: &mut EventLoop<'_>, index: usize) -> AppSession {
    let runtime = &mut event_loop.sessions[index];
    runtime.app.checkpoint_now();
    let session = Arc::clone(&runtime.app.state.session);
    smol::block_on(event_loop.ctx.storage_writer.checkpoint_fenced(
        CheckpointRequest {
            session_id: session.id,
            version: CheckpointVersion {
                epoch: session.history_snapshot().epoch,
                revision: session.revision(),
            },
            snapshot: session,
        },
        &runtime.app.storage_fence,
    ))
    .unwrap();
    AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap()
}

fn boundary(
    event_loop: &EventLoop<'_>,
    index: usize,
    text: &str,
) -> (HistoryBoundary, RewindBoundary) {
    boundary_for(&target(event_loop, index), text)
}

fn boundary_for(target: &AgentRef, text: &str) -> (HistoryBoundary, RewindBoundary) {
    let actor = target.actor().unwrap();
    let (history, _) = actor
        .history_boundary_messages()
        .unwrap()
        .into_iter()
        .find(|(_, message)| message.user_text() == Some(text))
        .unwrap();
    let snapshot = target.manager().journal().snapshot();
    let view = snapshot.state.iter().find(|record| {
        record.agent == Some(target.id()) && matches!(&record.change,
            StateChange::Boundary { offset, lens }
                if *offset == history.offset && lens.get(&target.id()).is_some_and(|(epoch, _)| *epoch == history.epoch))
    }).unwrap();
    let durable = RewindBoundary {
        agent: target.id(),
        seq: view.seq,
        epoch: snapshot.epoch,
    };
    (history, durable)
}

fn finish_restore(event_loop: &mut EventLoop<'_>, index: usize) {
    assert!(event_loop.sessions[index].app.restore_in_progress);
    pump_until(event_loop, |event_loop| {
        !event_loop.sessions[index].app.restore_in_progress
    });
}

fn assert_history(event_loop: &EventLoop<'_>, index: usize, expected: &[Message]) {
    let target = target(event_loop, index);
    let actor = target.actor().unwrap();
    let journal = target.manager().journal().snapshot();
    let loaded =
        AppSession::load(event_loop.sessions[index].id(), &event_loop.ctx.storage).unwrap();
    for messages in [
        actor.committed_history(),
        Arc::clone(&journal.histories[&target.id()].messages),
    ] {
        assert_eq!(
            serde_json::to_value(messages).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }
    assert_eq!(
        serde_json::to_value(loaded.messages()).unwrap(),
        serde_json::to_value(expected).unwrap()
    );
    assert_persisted_state(&loaded, &journal);
    assert_eq!(loaded.agent_revisions(), journal.agent_revisions.to_vec());
}

#[track_caller]
pub(crate) fn assert_persisted_state(loaded: &AppSession, journal: &SessionJournalSnapshot) {
    let restored = SessionJournal::from_session(loaded).unwrap().snapshot();
    let normalized = |snapshot: &SessionJournalSnapshot| {
        let histories: BTreeMap<_, _> = snapshot
            .histories
            .iter()
            .map(|(id, history)| (*id, json!({"epoch": history.epoch})))
            .collect();
        let mut value = json!({"histories": histories, "state": snapshot.state.to_vec()});
        normalize_history_epochs(&mut value);
        value
    };
    assert_eq!(normalized(&restored), normalized(journal));
}

fn normalize_history_epochs(snapshot: &mut Value) {
    let mut epochs = BTreeMap::new();
    visit_history_epochs(snapshot, &mut |epoch| {
        epochs.insert(epoch.as_u64().unwrap(), 0);
    });
    for (index, label) in epochs.values_mut().enumerate() {
        *label = index;
    }
    visit_history_epochs(snapshot, &mut |epoch| {
        *epoch = json!(epochs[&epoch.as_u64().unwrap()]);
    });
}

fn visit_history_epochs(snapshot: &mut Value, visit: &mut impl FnMut(&mut Value)) {
    if let Some(histories) = snapshot.get_mut("histories").and_then(Value::as_object_mut) {
        for history in histories.values_mut() {
            visit(history.get_mut("epoch").unwrap());
        }
    }
    if let Some(state) = snapshot.get_mut("state").and_then(Value::as_array_mut) {
        for record in state {
            let change = record.get_mut("change").unwrap();
            match change["kind"].as_str().unwrap() {
                "boundary" => {
                    for entry in change["lens"].as_object_mut().unwrap().values_mut() {
                        visit(entry.get_mut(0).unwrap());
                    }
                }
                "compaction" => visit(change.get_mut("epoch").unwrap()),
                "boundary_snapshot" => {
                    visit_history_epochs(change.get_mut("snapshot").unwrap(), visit);
                }
                _ => {}
            }
        }
    }
}

fn plan_fixture(event_loop: &mut EventLoop<'_>, index: usize, content: &str) -> PathBuf {
    const PLAN_FILE: &str = "rewind-plan.md";
    let root = target(event_loop, index);
    let path = PathBuf::from(&event_loop.session_cwd).join(PLAN_FILE);
    fs::write(&path, content).unwrap();
    let mode = AgentMode::Plan(path.clone());
    let modes = event_loop.ctx.lua_event_handle.mode_registry();
    smol::block_on(
        reserve_config_change(&root, &event_loop.sessions[index].coordinator, None)
            .unwrap()
            .commit_change(ConfigChange::Mode {
                mode: mode.clone(),
                mode_def: Some(modes.current(&mode)),
            }),
    )
    .unwrap();
    approve_plan(&root, &path, content);
    path
}

fn approve_plan(root: &AgentRef, path: &Path, content: &str) {
    let edit = approval_edit(root, path, content.into(), Default::default()).unwrap();
    root.manager()
        .journal()
        .commit(JournalCommit {
            state_edits: vec![edit],
            ..Default::default()
        })
        .unwrap();
    fs::write(path, content).unwrap();
}

#[test]
fn preparation_failure_repairs_current_plan_without_installing_any_plane() {
    const HISTORICAL_PLAN: &str = "# Historical approved plan\n";
    const CURRENT_PLAN: &str = "# Current approved plan\n";
    const PREPARATION_FAILED: &str = "injected rewind model preparation failure";
    with_event_loop(|event_loop| {
        let (index, _host) = setup(event_loop);
        let root = target(event_loop, index);
        let path = plan_fixture(event_loop, index, HISTORICAL_PLAN);
        turn(event_loop, index, FIRST);
        turn(event_loop, index, SECOND);
        approve_plan(&root, &path, CURRENT_PLAN);
        smol::block_on(
            event_loop.sessions[index]
                .coordinator
                .set_option(WORKFLOW_OPTION_ID, ENABLED_VALUE),
        )
        .unwrap();
        let original = checkpoint(event_loop, index);
        let journal = root.manager().journal();
        let before = journal.snapshot();
        let actor = root.actor().unwrap();
        let config = actor.effective_config().unwrap();
        let options = event_loop.sessions[index].coordinator.read().options();
        let permissions = event_loop.sessions[index]
            .coordinator
            .permissions()
            .unwrap()
            .snapshot();
        let graph = format!("{:?}", root.manager().snapshot());
        let slot = Arc::clone(&*event_loop.sessions[index].model_slot.load());
        let (entered, projected) = flume::bounded(1);
        let (release, released) = flume::bounded(1);
        let observed_path = path.clone();
        event_loop.ctx.prepare_provider = sync_preparer(move |_| {
            entered
                .send(fs::read_to_string(&observed_path).unwrap())
                .unwrap();
            released.recv().unwrap();
            Err(Arc::from(PREPARATION_FAILED))
        });
        let (history, durable) = boundary(event_loop, index, SECOND);
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        let mut projection = None;
        pump_until(event_loop, |_| {
            projection = projected.try_recv().ok();
            projection.is_some()
        });
        assert_eq!(projection.unwrap(), HISTORICAL_PLAN);
        assert!(actor.prepare_idle().is_err());
        assert!(event_loop.sessions[index].app.restore_in_progress);
        release.send(()).unwrap();
        finish_restore(event_loop, index);
        assert!(
            event_loop.sessions[index]
                .app
                .status_bar
                .flash_text()
                .unwrap()
                .contains(PREPARATION_FAILED)
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), CURRENT_PLAN);
        let after = journal.snapshot();
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(after.revision, before.revision);
        assert_eq!(after.seq, before.seq);
        assert_eq!(after.state, before.state);
        assert_eq!(after.agent_revisions, before.agent_revisions);
        assert_eq!(
            plan_entries(&original.latest_state().unwrap()).unwrap(),
            plan_entries(
                &AppSession::load(original.id, &event_loop.ctx.storage)
                    .unwrap()
                    .latest_state()
                    .unwrap()
            )
            .unwrap()
        );
        assert_history(event_loop, index, original.messages());
        assert!(Arc::ptr_eq(&actor.effective_config().unwrap(), &config));
        assert_eq!(
            event_loop.sessions[index].coordinator.read().options(),
            options
        );
        assert_eq!(format!("{:?}", root.manager().snapshot()), graph);
        let rules = event_loop.sessions[index]
            .coordinator
            .permissions()
            .unwrap()
            .snapshot();
        assert_eq!(rules.yolo, permissions.yolo);
        assert_eq!(
            format!("{:?}", rules.rules),
            format!("{:?}", permissions.rules)
        );
        assert_eq!(
            event_loop.sessions[index].model_slot.load().model.spec(),
            slot.model.spec()
        );
        assert!(Arc::ptr_eq(
            &event_loop.sessions[index].model_slot.load().provider,
            &slot.provider
        ));
        assert!(actor.prepare_idle().is_ok());
        assert!(root.validate_live().is_ok());
        assert!(event_loop.sessions[index].coordinator.permissions().is_ok());
        assert_eq!(
            AppSession::load(original.id, &event_loop.ctx.storage)
                .unwrap()
                .branches(),
            original.branches()
        );
    });
}

#[test]
fn failed_historical_plan_projection_publishes_build_fallback() {
    const PLAN: &str = "# Historical plan\n";
    const BUILD: &str = "build";
    const FALLBACK: &str = "Resuming in Build mode";
    with_event_loop(|event_loop| {
        let (index, _host) = setup(event_loop);
        let root = target(event_loop, index);
        let path = plan_fixture(event_loop, index, PLAN);
        turn(event_loop, index, FIRST);
        turn(event_loop, index, SECOND);
        let original = checkpoint(event_loop, index);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        finish_restore(event_loop, index);
        assert_history(event_loop, index, &expected);
        assert_eq!(
            root.effective_config().unwrap().unwrap().mode,
            AgentMode::Build
        );
        assert!(
            event_loop.sessions[index]
                .app
                .status_bar
                .flash_text()
                .unwrap()
                .contains(FALLBACK)
        );
        let saved = AppSession::load(original.id, &event_loop.ctx.storage).unwrap();
        assert_eq!(
            saved.latest_state().unwrap().scopes[&Some(root.id())]
                .config
                .as_ref()
                .unwrap()
                .mode
                .id,
            BUILD
        );
        assert_eq!(
            event_loop.sessions[index].app.root_mode(),
            crate::app::mode::Mode::Build
        );
        smol::block_on(
            event_loop.sessions[index]
                .coordinator
                .set_option(WORKFLOW_OPTION_ID, ENABLED_VALUE),
        )
        .unwrap();
        checkpoint(event_loop, index);
        assert!(root.actor().unwrap().prepare_idle().is_ok());
    });
}

#[test_case(false; "candidate_migration_published")]
#[test_case(true; "candidate_migration_rejected")]
fn plugin_state_migration_is_part_of_restore_publication(fail: bool) {
    const PLUGIN: &str = "rewind-state";
    const SLOT: &str = "rewind-state/value";
    const MIGRATION_ERROR: &str = "injected rewind migration failure";
    with_event_loop(|event_loop| {
        let (index, host) = setup(event_loop);
        let root = target(event_loop, index);
        host.load_source(
            PLUGIN,
            r#"makima.state.register("value", {scope = "agent", version = 1})"#,
        )
        .unwrap();
        let journal = root.manager().journal();
        let slot = journal
            .register_state::<Value>(StateSlotSpec {
                key: SLOT.into(),
                scope: StateScope::Agent,
                version: 1,
            })
            .unwrap();
        slot.set(&root.state_access(), json!({"count": 1}), None)
            .unwrap();
        turn(event_loop, index, FIRST);
        turn(event_loop, index, SECOND);
        slot.set(&root.state_access(), json!({"count": 9}), None)
            .unwrap();
        let original = checkpoint(event_loop, index);
        let before = journal.snapshot();
        let migrate = if fail {
            format!("function(value) value.count = 99; error('{MIGRATION_ERROR}') end")
        } else {
            "function(value) return {count = value.count + 1} end".into()
        };
        host.load_source(PLUGIN, &format!(
            "makima.state.register('value', {{scope = 'agent', version = 2, migrate = {migrate}}})"
        )).unwrap();
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        finish_restore(event_loop, index);
        if fail {
            assert_history(event_loop, index, original.messages());
            assert_eq!(journal.snapshot().state, before.state);
            assert_eq!(journal.snapshot().epoch, before.epoch);
            assert!(
                event_loop.sessions[index]
                    .app
                    .status_bar
                    .flash_text()
                    .unwrap()
                    .contains(MIGRATION_ERROR)
            );
        } else {
            assert_history(event_loop, index, &expected);
        }
        let saved = AppSession::load(original.id, &event_loop.ctx.storage).unwrap();
        let state = saved.latest_state().unwrap();
        let value = &state.scopes[&Some(root.id())].plugins[SLOT];
        assert_eq!(value.version, if fail { 1 } else { 2 });
        assert_eq!(value.value, Some(json!({"count": if fail {9} else {2}})));
        assert!(root.actor().unwrap().prepare_idle().is_ok());
    });
}

#[test]
fn context_reload_routes_root_and_rewinds_recorded_instructions_despite_changed_disk() {
    const INSTRUCTION_FILE: &str = "AGENTS.md";
    const ORIGINAL: &str = "original rewind project instructions";
    const CHANGED: &str = "reloaded rewind project instructions";
    with_event_loop(|event_loop| {
        let path = PathBuf::from(&event_loop.session_cwd).join(INSTRUCTION_FILE);
        fs::write(&path, ORIGINAL).unwrap();
        let canonical_path = path.canonicalize().unwrap();
        let (systems, requests) = flume::unbounded();
        let (index, _host) = setup_with_provider(
            event_loop,
            Arc::new(CompletingProvider {
                systems: Some(systems),
            }),
        );
        let root = target(event_loop, index);
        let assert_context = |expected: &str, absent: &str| {
            let recorded = root_context(&root.state_access()).unwrap().unwrap();
            let source = recorded
                .instructions
                .sources
                .iter()
                .find(|source| source.path == canonical_path)
                .unwrap();
            assert_eq!(source.content, expected);
            assert!(recorded.instructions.text.contains(expected));
            assert!(!recorded.instructions.text.contains(absent));
        };
        let assert_system = |expected: &str, absent: &str| {
            let system = requests.try_recv().unwrap();
            assert!(system.contains(expected));
            assert!(!system.contains(absent));
            assert!(requests.is_empty());
        };
        turn(event_loop, index, FIRST);
        assert_context(ORIGINAL, CHANGED);
        assert_system(ORIGINAL, CHANGED);
        turn(event_loop, index, SECOND);
        assert_system(ORIGINAL, CHANGED);
        let original = checkpoint(event_loop, index);
        let original_context = original.latest_state().unwrap().scopes[&Some(root.id())]
            .context
            .clone();
        assert!(!original_context.is_empty());
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        fs::write(&path, CHANGED).unwrap();
        let dispatched = reload_context_command(event_loop, index);
        assert!(
            matches!(
                dispatched,
                InputDispatch::Dispatched(CommandOutcome::Completed)
            ),
            "reload command failed: {dispatched:?}"
        );
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index]
                .handles
                .btw_system
                .load()
                .contains(CHANGED)
        });
        assert_context(CHANGED, ORIGINAL);
        let reloaded = checkpoint(event_loop, index);
        assert_ne!(
            reloaded.latest_state().unwrap().scopes[&Some(root.id())].context,
            original_context
        );
        assert_persisted_state(&reloaded, &root.manager().journal().snapshot());
        turn(event_loop, index, THIRD);
        assert_system(CHANGED, ORIGINAL);
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        finish_restore(event_loop, index);
        assert_history(event_loop, index, &expected);
        assert_context(ORIGINAL, CHANGED);
        let restored = checkpoint(event_loop, index);
        assert_eq!(
            restored.latest_state().unwrap().scopes[&Some(root.id())].context,
            original_context
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), CHANGED);
        turn(event_loop, index, SECOND);
        assert_context(ORIGINAL, CHANGED);
        assert_system(ORIGINAL, CHANGED);
        assert_eq!(fs::read_to_string(&path).unwrap(), CHANGED);
    });
}

#[test_case(false; "root_rewind_and_redo")]
#[test_case(true; "repeated_rewind_and_redo")]
fn root_rewind_and_branch_redo_publish_actor_journal_and_disk(repeated: bool) {
    const NEXT_MODEL: &str = "openai/gpt-5";
    const ORIGINAL_SCOPE: &str = "original rewind rule *";
    const LATER_SCOPE: &str = "later rewind rule *";
    with_event_loop(|event_loop| {
        let (index, _host) = setup(event_loop);
        let root = target(event_loop, index);
        let permissions = event_loop.sessions[index]
            .coordinator
            .permissions()
            .unwrap();
        permissions.add_session_rule(PermissionRule {
            tool: ToolKey::native("bash"),
            scope: Some(ORIGINAL_SCOPE.into()),
            effect: Effect::Deny,
        });
        let historical_rules = permissions.snapshot().stored_rules();
        assert_eq!(historical_rules.len(), 1);
        turn(event_loop, index, FIRST);
        turn(event_loop, index, SECOND);
        let assert_config = |event_loop: &EventLoop<'_>, model: &str, rules: &[StoredRule]| {
            assert_eq!(
                root.effective_config().unwrap().unwrap().model.spec(),
                model
            );
            assert_eq!(permissions.snapshot().stored_rules(), rules);
            let journal = root.manager().journal().snapshot();
            let view = journal.fold(journal.seq).unwrap();
            let loaded =
                AppSession::load(event_loop.sessions[index].id(), &event_loop.ctx.storage).unwrap();
            for state in [view, loaded.latest_state().unwrap()] {
                assert_eq!(
                    state.scopes[&Some(root.id())]
                        .config
                        .as_ref()
                        .unwrap()
                        .model,
                    model
                );
                assert_eq!(state.scopes[&None].rules, rules);
            }
            assert_persisted_state(&loaded, &journal);
        };
        checkpoint(event_loop, index);
        assert_config(event_loop, MODEL, &historical_rules);
        let reservation =
            reserve_config_change(&root, &event_loop.sessions[index].coordinator, None).unwrap();
        smol::block_on(reservation.commit_change(ConfigChange::Patch(ConfigPatch {
            model: Some(PreparedModel {
                model: Model::from_spec(NEXT_MODEL).unwrap(),
                provider: Arc::clone(&root.effective_config().unwrap().unwrap().provider),
            }),
            ..Default::default()
        })))
        .unwrap();
        permissions.add_session_rule(PermissionRule {
            tool: ToolKey::native("bash"),
            scope: Some(LATER_SCOPE.into()),
            effect: Effect::Allow,
        });
        let later_rules = permissions.snapshot().stored_rules();
        assert_eq!(later_rules.len(), historical_rules.len() + 1);
        smol::block_on(
            event_loop.sessions[index]
                .coordinator
                .set_option(WORKFLOW_OPTION_ID, ENABLED_VALUE),
        )
        .unwrap();
        turn(event_loop, index, THIRD);
        let original = checkpoint(event_loop, index);
        assert_config(event_loop, NEXT_MODEL, &later_rules);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        finish_restore(event_loop, index);
        assert_history(event_loop, index, &expected);
        assert_eq!(
            event_loop.sessions[index].app.input_box.buffer.value(),
            SECOND
        );
        assert!(!root.effective_config().unwrap().unwrap().workflow);
        assert_config(event_loop, MODEL, &historical_rules);
        let branch = event_loop.sessions[index]
            .app
            .state
            .session
            .branches()
            .last()
            .unwrap()
            .id;
        if repeated {
            let (history, durable) = boundary(event_loop, index, FIRST);
            let expected = expected[..history.offset].to_vec();
            event_loop.handle_action(
                index,
                Action::RewindAgent {
                    target: root.clone(),
                    boundary: history,
                    durable: Some(durable),
                },
            );
            finish_restore(event_loop, index);
            assert_history(event_loop, index, &expected);
            assert_config(event_loop, MODEL, &historical_rules);
            assert_eq!(
                event_loop.sessions[index].app.input_box.buffer.value(),
                FIRST
            );
        }
        event_loop.handle_action(
            index,
            Action::RestoreBranch {
                target: root.clone(),
                id: branch,
            },
        );
        finish_restore(event_loop, index);
        assert_history(event_loop, index, original.messages());
        assert!(root.effective_config().unwrap().unwrap().workflow);
        assert_config(event_loop, NEXT_MODEL, &later_rules);
        smol::block_on(
            event_loop.sessions[index]
                .coordinator
                .set_option(WORKFLOW_OPTION_ID, DISABLED_VALUE),
        )
        .unwrap();
        let saved = checkpoint(event_loop, index);
        assert!(
            !saved.latest_state().unwrap().scopes[&Some(root.id())]
                .config
                .as_ref()
                .unwrap()
                .workflow
        );
    });
}

#[test_case(false; "child_selected")]
#[test_case(true; "outside_root_selected")]
fn child_restore_preserves_outside_root_history_config_scroll_and_draft(outside_selected: bool) {
    const PLUGIN: &str = "rewind-child";
    const COMMAND: &str = "/spawn-rewind-child";
    const CHILD: &str = "rewind child";
    const OUTSIDE_DRAFT: &str = "outside unsent draft";
    const ROOT_SCROLL: u16 = 4;
    with_event_loop(|event_loop| {
        let (index, host) = setup(event_loop);
        turn(event_loop, index, FIRST);
        host.load_source(
            PLUGIN,
            &format!(
                r#"
            makima.api.register_command({{
                name = "{COMMAND}", tui_only = false,
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
        let root = target(event_loop, index);
        let manager = root.manager();
        let command_target = event_loop.sessions[index].app.command_target.clone();
        let dispatched = smol::block_on(
            event_loop
                .ctx
                .command_runtime
                .registry
                .dispatch_input(&command_target, COMMAND.into()),
        );
        assert!(
            matches!(
                dispatched,
                InputDispatch::Dispatched(CommandOutcome::Completed)
            ),
            "spawn command failed: {dispatched:?}"
        );
        let child = manager
            .snapshot()
            .into_iter()
            .find(|node| node.parent_id == Some(root.id()))
            .unwrap()
            .agent_id;
        let child = manager.lookup(child).unwrap();
        let actor = child.actor().unwrap();
        pump_until(event_loop, |_| {
            manager
                .actor(root.id())
                .is_ok_and(|actor| actor.prepare_idle().is_ok())
                && actor.prepare_idle().is_ok()
        });
        for text in [SECOND, THIRD] {
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
                    Default::default(),
                )
                .unwrap();
            pump_until(event_loop, |_| {
                ticket.peek().is_some() && actor.snapshot().status == ActorStatus::Idle
            });
            assert!(matches!(ticket.peek(), Some(TurnOutcome::Completed { .. })));
        }
        let snapshot = manager.journal().snapshot();
        let key = snapshot
            .agent_revisions
            .iter()
            .rev()
            .find(|(_, record)| record.id == child.id())
            .unwrap()
            .1
            .transcript_key
            .clone();
        {
            let app = &mut event_loop.sessions[index].app;
            let mut chat = Chat::new(
                CHILD.into(),
                app.ui_config.clone(),
                app.lua_event_handle.clone(),
                Arc::clone(&app.theme_provider),
            );
            chat.agent_id = Some(child.id());
            chat.parent_agent_id = Some(root.id());
            chat.subagent_id = Some(key.clone());
            chat.push_user_message(SECOND);
            chat.push_user_message(THIRD);
            let child_index = app.chats.len();
            app.agent_chat_index.insert(child.id(), child_index);
            app.chat_index.insert(key.clone(), child_index);
            app.chats.push(chat);
            app.chats[0].restore_scroll(ROOT_SCROLL, false);
            app.chats[0].push_post(&Post {
                text: OUTSIDE_DRAFT.into(),
                level: PostLevel::Info,
                agent: None,
            });
            app.active_chat = if outside_selected { 0 } else { child_index };
            app.input_box.set_input(OUTSIDE_DRAFT.into());
        }
        event_loop.sync_orchestration();
        if !outside_selected {
            let before = manager.journal().snapshot();
            let dispatched = reload_context_command(event_loop, index);
            assert!(matches!(
                dispatched,
                InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(message)))
                    if message.as_ref() == CONTEXT_RELOAD_ROOT_ONLY
            ));
            assert_eq!(manager.journal().snapshot().state, before.state);
        }
        event_loop.sessions[index]
            .app
            .seed_recoverable_queue_for_test(vec![OUTSIDE_DRAFT.into()]);
        let original = checkpoint(event_loop, index);
        assert_eq!(original.meta.queued_messages, [OUTSIDE_DRAFT]);
        assert!(event_loop.sessions[index].app.holds_recovery_text());
        let root_history = root.actor().unwrap().committed_history();
        let root_config = root.effective_config().unwrap().unwrap();
        let (history, durable) = boundary_for(&child, THIRD);
        let expected = actor.committed_history()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: child.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        finish_restore(event_loop, index);
        assert_history(event_loop, index, &root_history);
        assert!(Arc::ptr_eq(
            &root.effective_config().unwrap().unwrap(),
            &root_config
        ));
        assert_eq!(
            serde_json::to_value(actor.committed_history()).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        let app = &event_loop.sessions[index].app;
        assert_eq!(app.chats[0].scroll_top(), ROOT_SCROLL);
        assert!(!app.chats[0].auto_scroll());
        assert_eq!(app.chats[0].last_message_text(), OUTSIDE_DRAFT);
        assert_eq!(
            app.chats[0].last_message_role(),
            Some(&DisplayRole::Post(PostLevel::Info))
        );
        assert_eq!(
            app.input_box.buffer.value(),
            if outside_selected {
                OUTSIDE_DRAFT
            } else {
                THIRD
            }
        );
        assert_eq!(
            app.selected_agent_id(),
            Some(if outside_selected {
                root.id()
            } else {
                child.id()
            })
        );
        let loaded = AppSession::load(original.id, &event_loop.ctx.storage).unwrap();
        assert_eq!(loaded.meta.queued_messages, original.meta.queued_messages);
        assert!(!loaded.meta.queued_messages.is_empty());
        assert_eq!(
            serde_json::to_value(&loaded.subagent_messages()[&key]).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
    });
}

#[test]
fn published_rewind_holds_actor_until_event_loop_installs_display() {
    with_event_loop(|event_loop| {
        let (index, _host) = two_turn_fixture(event_loop);
        let root = target(event_loop, index);
        let original = checkpoint(event_loop, index);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        let event = loop {
            let event = event_loop.internal_rx.recv_timeout(WAIT).unwrap();
            if let InternalEvent::RestoreReady {
                result,
                session,
                runtime,
            } = event
            {
                assert!(result.is_ok(), "{}", result.as_ref().err().unwrap());
                let event = InternalEvent::RestoreReady {
                    result,
                    session,
                    runtime,
                };
                break event;
            }
            event_loop.handle_internal(event);
        };
        assert_history(event_loop, index, &expected);
        assert!(root.actor().unwrap().prepare_idle().is_err());
        assert!(event_loop.sessions[index].app.restore_in_progress);
        assert_eq!(
            event_loop.sessions[index]
                .app
                .state
                .session
                .messages()
                .len(),
            original.messages().len()
        );
        event_loop.handle_internal(event);
        assert!(root.actor().unwrap().prepare_idle().is_ok());
        assert!(!event_loop.sessions[index].app.restore_in_progress);
        assert_eq!(
            event_loop.sessions[index].app.input_box.buffer.value(),
            SECOND
        );
    });
}

const UNTOUCHED_FILE: &str = "rewind-untouched.txt";
const UNTOUCHED_CONTENT: &[u8] = b"user-owned edit";
const SYNC_FAILURE: &str = "injected restore directory sync failure";

#[test_case(None, false; "parent_runtime_replaced")]
#[test_case(Some(false), false; "parent_sync_recovers")]
#[test_case(Some(false), true; "parent_baseline_sync_recovers")]
#[test_case(Some(true), true; "parent_baseline_sync_failure_keeps_implementation")]
fn parent_restore_replaces_runtime_before_admission(sync_failure: Option<bool>, baseline: bool) {
    with_event_loop(|event_loop| {
        let (parent_index, _parent_host) = two_turn_fixture(event_loop);
        let parent_id = event_loop.sessions[parent_index].id();
        let (history, selected) = boundary(event_loop, parent_index, SECOND);
        let parent = checkpoint(event_loop, parent_index);
        let expected = parent.messages()[..history.offset].to_vec();
        let old_parent = event_loop.remove_runtime(parent_index);
        super::super::tests::release_runtime(old_parent);
        let (index, _host) = setup(event_loop);
        let implementation_id = event_loop.sessions[index].id();
        Arc::make_mut(&mut event_loop.sessions[index].app.state.session).set_parent(Some(
            ParentLink {
                session: parent_id,
                message_count: parent.messages().len(),
                seq: parent.sequence_high_water().unwrap(),
            },
        ));
        turn(event_loop, index, THIRD);
        checkpoint(event_loop, index);
        let identity = event_loop.sessions[index].handles.identity();
        let writer = Arc::clone(&event_loop.ctx.storage_writer);
        let sync = sync_failure.map(|_| {
            if baseline {
                writer.hold_current_sync(SYNC_FAILURE)
            } else {
                writer.hold_restore_sync(SYNC_FAILURE)
            }
        });
        event_loop.handle_action(
            index,
            Action::RewindParent {
                session: parent_id,
                seq: selected.seq,
            },
        );
        if let Some((entered, release)) = sync {
            let mut held = None;
            pump_until(event_loop, |_| {
                held = entered.try_recv().ok();
                held.is_some()
            });
            assert_eq!(
                held,
                Some(if baseline {
                    implementation_id
                } else {
                    parent_id
                })
            );
            assert_eq!(event_loop.sessions[index].id(), implementation_id);
            assert!(event_loop.sessions[index].app.restore_in_progress);
            assert!(event_loop.submit_text(index, FIRST.into()).is_err());
            let fail = sync_failure.unwrap();
            release
                .send(if fail {
                    Err(SYNC_FAILURE.into())
                } else {
                    Ok(())
                })
                .unwrap();
            writer.release_restore_sync();
            if fail {
                pump_until(event_loop, |event_loop| {
                    event_loop.sessions[index].lock_lost
                });
                assert_eq!(event_loop.sessions[index].id(), implementation_id);
                assert_eq!(
                    AppSession::load(parent_id, &event_loop.ctx.storage)
                        .unwrap()
                        .messages(),
                    parent.messages()
                );
                return;
            }
        }
        finish_restore(event_loop, index);
        assert_eq!(event_loop.sessions[index].id(), parent_id);
        assert!(!Arc::ptr_eq(
            &identity,
            &event_loop.sessions[index].handles.identity()
        ));
        assert_history(event_loop, index, &expected);
        let restored = AppSession::load(parent_id, &event_loop.ctx.storage).unwrap();
        assert_persisted_state(
            &restored,
            &target(event_loop, index).manager().journal().snapshot(),
        );
        assert!(
            session_lock::claim(&event_loop.ctx.sessions_dir, &parent_id)
                .unwrap()
                .is_none()
        );
        turn(event_loop, index, FIRST);
    });
}

#[test_case(false; "sync_recovers")]
#[test_case(true; "sync_failure_stays_forward")]
fn visible_restore_waits_for_durability_without_rollback(fail: bool) {
    with_event_loop(|event_loop| {
        let (index, _host) = two_turn_fixture(event_loop);
        let root = target(event_loop, index);
        let id = event_loop.sessions[index].id();
        let original = checkpoint(event_loop, index);
        let writer = Arc::clone(&event_loop.ctx.storage_writer);
        let (entered, release) = writer.hold_restore_sync(SYNC_FAILURE);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root.clone(),
                boundary: history,
                durable: Some(durable),
            },
        );
        let mut published = None;
        pump_until(event_loop, |_| {
            published = entered.try_recv().ok();
            published.is_some()
        });
        assert_eq!(published, Some(id));
        let visible = AppSession::load(id, &event_loop.ctx.storage).unwrap();
        assert!(event_loop.sessions[index].app.restore_in_progress);
        assert!(event_loop.submit_text(index, THIRD.into()).is_err());
        assert!(root.actor().unwrap().prepare_idle().is_err());
        assert_eq!(
            serde_json::to_value(event_loop.sessions[index].app.state.session.messages()).unwrap(),
            serde_json::to_value(original.messages()).unwrap()
        );
        assert_eq!(
            serde_json::to_value(visible.messages()).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        release
            .send(if fail {
                Err(SYNC_FAILURE.into())
            } else {
                Ok(())
            })
            .unwrap();
        writer.release_restore_sync();
        if fail {
            pump_until(event_loop, |event_loop| {
                event_loop.sessions[index].lock_lost
            });
            assert!(event_loop.sessions[index].app.restore_in_progress);
            assert_eq!(
                serde_json::to_value(
                    AppSession::load(id, &event_loop.ctx.storage)
                        .unwrap()
                        .messages()
                )
                .unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
        } else {
            finish_restore(event_loop, index);
            assert_history(event_loop, index, &expected);
        }
    });
}

#[test_case(false; "baseline_sync_recovers")]
#[test_case(true; "baseline_sync_failure_never_publishes")]
fn ordinary_restore_waits_for_baseline_durability(fail: bool) {
    with_event_loop(|event_loop| {
        let (index, _host) = two_turn_fixture(event_loop);
        let root = target(event_loop, index);
        let id = event_loop.sessions[index].id();
        let original = checkpoint(event_loop, index);
        let writer = Arc::clone(&event_loop.ctx.storage_writer);
        let (entered, release) = writer.hold_current_sync(SYNC_FAILURE);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root,
                boundary: history,
                durable: Some(durable),
            },
        );
        let mut baseline = None;
        pump_until(event_loop, |_| {
            baseline = entered.try_recv().ok();
            baseline.is_some()
        });
        assert_eq!(baseline, Some(id));
        assert!(event_loop.sessions[index].app.restore_in_progress);
        assert!(event_loop.submit_text(index, THIRD.into()).is_err());
        assert_history(event_loop, index, original.messages());
        release
            .send(if fail {
                Err(SYNC_FAILURE.into())
            } else {
                Ok(())
            })
            .unwrap();
        writer.release_restore_sync();
        if fail {
            pump_until(event_loop, |event_loop| {
                event_loop.sessions[index].lock_lost
            });
            let disk = AppSession::load(id, &event_loop.ctx.storage).unwrap();
            assert_eq!(
                serde_json::to_value(disk.messages()).unwrap(),
                serde_json::to_value(original.messages()).unwrap()
            );
            assert_eq!(
                serde_json::to_value(event_loop.sessions[index].app.state.session.messages())
                    .unwrap(),
                serde_json::to_value(original.messages()).unwrap()
            );
        } else {
            finish_restore(event_loop, index);
            assert_history(event_loop, index, &expected);
        }
    });
}

#[test_case(true; "baseline_cancel")]
#[test_case(false; "published_cancel")]
fn runtime_cancellation_abandons_directory_sync_wait(baseline: bool) {
    with_event_loop(|event_loop| {
        let (index, _host) = two_turn_fixture(event_loop);
        let root = target(event_loop, index);
        let id = event_loop.sessions[index].id();
        checkpoint(event_loop, index);
        let writer = Arc::clone(&event_loop.ctx.storage_writer);
        let (entered, _release) = if baseline {
            writer.hold_current_sync(SYNC_FAILURE)
        } else {
            writer.hold_restore_sync(SYNC_FAILURE)
        };
        let (history, durable) = boundary(event_loop, index, SECOND);
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root,
                boundary: history,
                durable: Some(durable),
            },
        );
        let mut waiting = None;
        pump_until(event_loop, |_| {
            waiting = entered.try_recv().ok();
            waiting.is_some()
        });
        assert_eq!(waiting, Some(id));
        assert!(event_loop.submit_text(index, THIRD.into()).is_err());
        event_loop.sessions[index].cancel_background_work();
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].lock_lost
        });
        writer.release_restore_sync();
        let source_lease = match event_loop.sessions[index].session_lock.as_ref().unwrap() {
            super::super::SessionLockState::Shared(lease) => Arc::downgrade(lease),
            _ => panic!("restore must retain a shared source lease"),
        };
        let retired = event_loop.remove_runtime(index);
        super::super::tests::release_runtime(retired);
        pump_until(event_loop, |_| source_lease.upgrade().is_none());
        let mut claimed = None;
        pump_until(event_loop, |event_loop| {
            claimed = session_lock::claim(&event_loop.ctx.sessions_dir, &id).unwrap();
            claimed.is_some()
        });
        claimed.unwrap().release().unwrap();
    });
}

#[test]
fn conversation_rewind_leaves_files_untouched() {
    with_event_loop(|event_loop| {
        let (index, _host) = two_turn_fixture(event_loop);
        let root = target(event_loop, index);
        let path = PathBuf::from(&event_loop.session_cwd).join(UNTOUCHED_FILE);
        fs::write(&path, UNTOUCHED_CONTENT).unwrap();
        let original = checkpoint(event_loop, index);
        let (history, durable) = boundary(event_loop, index, SECOND);
        let expected = original.messages()[..history.offset].to_vec();
        event_loop.handle_action(
            index,
            Action::RewindAgent {
                target: root,
                boundary: history,
                durable: Some(durable),
            },
        );
        finish_restore(event_loop, index);
        assert_history(event_loop, index, &expected);
        assert_eq!(fs::read(path).unwrap(), UNTOUCHED_CONTENT);
        turn(event_loop, index, THIRD);
    });
}
