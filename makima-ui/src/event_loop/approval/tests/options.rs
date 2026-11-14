use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use makima_agent::ModeId;
use makima_agent::actor::{
    AgentActorHandle, ConfigChange, ConfigCommit, ConfigPatch, PreparedModel,
};
use makima_agent::session_checkpoint::stored_agent_config;
use makima_agent::session_options::SessionOptionError;
use makima_domain::{Effort, ModelSelection, ThinkingConfig};
use makima_providers::model::FastSupport;
use makima_providers::{RequestOptions, ThinkingSupport};
use makima_storage::{id::MakimaId, session_state::StateChange};
use test_case::test_case;

use super::{
    APPROVAL_CHANGED, ActorStatus, AppSession, ApprovalStep, EventLoop, HISTORY, InternalEvent,
    Mode, PLAN, RecordingProvider, SOURCE_MODEL, Status, TARGET_MODEL, WAIT, approve,
    assert_preserved, capable_model, checkpoint, final_ready, implementation_options,
    implementation_selection, next_ready, pump_until, read_model, read_prefs, read_recents, setup,
    sync_preparer, with_event_loop,
};

const CHECKPOINT_FAILURE: &str = "approval checkpoint failed";
const CHECKPOINT_FLASH: &str = "history checkpoint failed: approval checkpoint failed";

pub(super) fn apply_source_patch(
    event_loop: &mut EventLoop<'_>,
    index: usize,
    patch: ConfigPatch,
) -> ConfigCommit {
    let runtime = &event_loop.sessions[index];
    let (manager, root) = runtime.handles.manager_and_root();
    let ticket =
        super::reserve_config_change(&manager.lookup(root).unwrap(), &runtime.coordinator, None)
            .unwrap();
    let commit = smol::block_on(ticket.commit_change(ConfigChange::Patch(patch))).unwrap();
    assert_eq!(
        super::root_stored_config(event_loop, index),
        stored_agent_config(
            &commit.config,
            runtime.coordinator.read().options().config_values(),
        )
        .unwrap()
    );
    event_loop.sessions[index].project_config(&commit);
    commit
}

pub(super) fn implementation_index(
    event_loop: &EventLoop<'_>,
    source: MakimaId,
    linked: bool,
) -> Option<usize> {
    event_loop.sessions.iter().position(|runtime| {
        if linked {
            runtime
                .app
                .state
                .session
                .parent()
                .is_some_and(|parent| parent.session == source)
        } else {
            runtime.id() == source
        }
    })
}

pub(super) fn assert_linked_parent(
    event_loop: &EventLoop<'_>,
    implementation: usize,
    source: MakimaId,
    source_actor: &AgentActorHandle,
    message_count: usize,
) {
    let runtime = &event_loop.sessions[implementation];
    assert_ne!(runtime.id(), source);
    let parent = runtime.app.state.session.parent().unwrap();
    assert_eq!(parent.session, source);
    assert_eq!(parent.message_count, message_count);
    let saved = AppSession::load(source, &event_loop.ctx.storage).unwrap();
    let root = saved.root_agent_id().unwrap();
    assert_eq!(root, source_actor.agent_id());
    assert_eq!(
        saved.messages(),
        source_actor.committed_history().as_slice()
    );
    assert!(saved.state().iter().any(|record| {
        record.seq == parent.seq
            && record.agent == Some(root)
            && matches!(&record.change, StateChange::Boundary { offset, .. } if *offset == message_count)
    }));
    assert_ne!(runtime.handles.manager_and_root().1, root);
}

#[test_case(false, false, false, false; "model_only")]
#[test_case(true, true, false, false; "options_only_clear_context")]
#[test_case(false, false, true, false; "explicit_disabled")]
#[test_case(true, false, true, true; "required_thinking_clear_context")]
fn approval_selection_uses_predecessor_and_explicit_disables(
    clear: bool,
    options_only: bool,
    disable: bool,
    required: bool,
) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let selection = ModelSelection {
            spec: (!options_only).then(|| TARGET_MODEL.into()),
            thinking: disable.then_some(ThinkingConfig::Off),
            fast: (disable || options_only).then_some(false),
        };
        event_loop.sessions[index]
            .app
            .stage_plan_selection(selection.clone());
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let (record, recorded) = flume::unbounded();
        apply_source_patch(
            event_loop,
            index,
            ConfigPatch {
                thinking: Some(ThinkingConfig::Effort(Effort::High)),
                fast: Some(true),
                model: Some(PreparedModel {
                    model: capable_model(SOURCE_MODEL),
                    provider: Arc::new(RecordingProvider {
                        requests: record.clone(),
                        release: None,
                    }),
                }),
                ..Default::default()
            },
        );
        let preparations = Arc::new(AtomicUsize::new(0));
        let preparation_count = Arc::clone(&preparations);
        event_loop.ctx.prepare_provider = sync_preparer(move |model| {
            preparation_count.fetch_add(1, Ordering::SeqCst);
            let mut model = capable_model(&model.spec());
            if required {
                model.thinking_support = Some(ThinkingSupport::Required);
            }
            Ok(PreparedModel {
                model,
                provider: Arc::new(RecordingProvider {
                    requests: record.clone(),
                    release: None,
                }),
            })
        });
        let source = event_loop.sessions[index].id();
        let source_config = actor.effective_config().unwrap();
        let source_history = actor.committed_history();
        let source_stored_config = super::root_stored_config(event_loop, index);
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            implementation_index(event_loop, source, clear).is_some_and(|implementation| {
                let runtime = &event_loop.sessions[implementation];
                let (manager, root) = runtime.handles.manager_and_root();
                !runtime.app.plan_approval_pending
                    && runtime.app.status == Status::Idle
                    && manager.actor(root).unwrap().snapshot().status == ActorStatus::Idle
                    && !recorded.is_empty()
            })
        });
        let implementation = implementation_index(event_loop, source, clear).unwrap();
        let (implementation_manager, implementation_root) = event_loop.sessions[implementation]
            .handles
            .manager_and_root();
        let implementation_actor = implementation_manager.actor(implementation_root).unwrap();
        let (spec, options, messages, _) = recorded.recv_timeout(WAIT).unwrap();
        let expected = RequestOptions {
            thinking: if required {
                ThinkingConfig::Effort(Effort::Minimal)
            } else if disable {
                ThinkingConfig::Off
            } else {
                ThinkingConfig::Effort(Effort::High)
            },
            fast: !disable && !options_only,
        };
        let expected_spec = if options_only {
            SOURCE_MODEL
        } else {
            TARGET_MODEL
        };
        assert_eq!(spec, expected_spec);
        assert_eq!(options, expected);
        super::assert_actor_selection(&implementation_actor, expected_spec, expected);
        if clear {
            assert_linked_parent(
                event_loop,
                implementation,
                source,
                &actor,
                source_history.len(),
            );
            assert!(Arc::ptr_eq(
                &source_config,
                &actor.effective_config().unwrap()
            ));
            assert!(Arc::ptr_eq(&source_history, &actor.committed_history()));
            let parent = AppSession::load(source, &event_loop.ctx.storage).unwrap();
            let view = parent.latest_state().unwrap();
            let config = view.scopes[&Some(root)].config.as_ref().unwrap();
            assert_eq!(config, &source_stored_config);
        }
        assert_eq!(
            messages
                .iter()
                .any(|message| message.user_text() == Some(HISTORY)),
            !clear
        );
        assert_eq!(
            preparations.load(Ordering::SeqCst),
            usize::from(!options_only)
        );
        checkpoint(event_loop, implementation);
        let restored = AppSession::load(
            event_loop.sessions[implementation].id(),
            &event_loop.ctx.storage,
        )
        .unwrap();
        let view = restored.latest_state().unwrap();
        let config = view.scopes[&Some(implementation_root)]
            .config
            .as_ref()
            .unwrap();
        assert_eq!(config.model, expected_spec);
        assert_eq!(config.thinking, Some(expected.thinking.into()));
        assert_eq!(config.fast, expected.fast);
        assert_eq!(config.mode.id, ModeId::Build.key());
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
    });
}

#[test]
fn approval_option_only_change_makes_request_stale() {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = actor.committed_history();
        let recents = read_recents(&event_loop.ctx.storage);
        let model = read_model(&event_loop.ctx.storage);
        approve(event_loop, index, false);
        let ready = final_ready(event_loop, &requests);
        let mut selection = implementation_selection();
        selection.fast = Some(true);
        event_loop.sessions[index]
            .app
            .stage_plan_selection(selection.clone());
        event_loop.handle_internal(ready);
        assert_eq!(
            event_loop.sessions[index].app.status_bar.flash_text(),
            Some(APPROVAL_CHANGED)
        );
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_selection(),
            &selection
        );
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert_eq!(read_recents(&event_loop.ctx.storage), recents);
        assert_eq!(read_model(&event_loop.ctx.storage), model);
        assert!(requests.is_empty());
    });
}

#[test_case(false, false; "thinking")]
#[test_case(true, false; "fast")]
#[test_case(false, true; "linked_thinking")]
#[test_case(true, true; "linked_fast")]
fn approval_rejects_unsupported_explicit_options(fast: bool, clear: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = actor.committed_history();
        let recents = read_recents(&event_loop.ctx.storage);
        let model = read_model(&event_loop.ctx.storage);
        let prefs = serde_json::to_value(read_prefs(&event_loop.ctx.storage)).unwrap();
        let mut selection = implementation_selection();
        if fast {
            selection.fast = Some(true);
        }
        event_loop.sessions[index]
            .app
            .stage_plan_selection(selection.clone());
        let prepare = event_loop.ctx.prepare_provider.clone();
        event_loop.ctx.prepare_provider = Arc::new(move |spec| {
            let prepared = prepare(spec);
            Box::pin(async move {
                let mut prepared = prepared.await?;
                if fast {
                    prepared.model.fast_support = Some(FastSupport::Unsupported);
                } else {
                    prepared.model.thinking_support = Some(ThinkingSupport::No);
                }
                Ok(prepared)
            })
        });
        let source = event_loop.sessions[index].id();
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        assert!(implementation_index(event_loop, source, true).is_none());
        let expected = if fast {
            SessionOptionError::FastUnsupported
        } else {
            SessionOptionError::ThinkingUnsupported
        }
        .to_string();
        assert!(
            event_loop.sessions[index]
                .app
                .status_bar
                .flash_text()
                .unwrap()
                .contains(&expected)
        );
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_selection(),
            &selection
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert_eq!(read_recents(&event_loop.ctx.storage), recents);
        assert_eq!(read_model(&event_loop.ctx.storage), model);
        assert_eq!(
            serde_json::to_value(read_prefs(&event_loop.ctx.storage)).unwrap(),
            prefs
        );
        assert!(requests.is_empty());
    });
}

#[test_case(false; "keep_context")]
#[test_case(true; "clear_context")]
fn approval_checkpoint_failure_retains_stage_for_retry(clear: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = actor.committed_history();
        let source = event_loop.sessions[index].id();
        let run = event_loop.sessions[index].app.run_id;
        approve(event_loop, index, clear);
        let mut ready = final_ready(event_loop, &requests);
        let InternalEvent::PlanApprovalReady {
            result: Ok(ApprovalStep::Prepared(prepared)),
            ..
        } = &mut ready
        else {
            unreachable!()
        };
        prepared.transaction.publish =
            Box::new(|_, _| Box::pin(async { Err(CHECKPOINT_FAILURE.into()) }));
        event_loop.handle_internal(ready);
        let failed = next_ready(event_loop);
        event_loop.handle_internal(failed);
        assert_preserved(event_loop, &host, index, &path, run, Some(CHECKPOINT_FLASH));
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        assert!(requests.is_empty());
        assert!(implementation_index(event_loop, source, true).is_none());
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            implementation_index(event_loop, source, clear).is_some_and(|implementation| {
                let app = &event_loop.sessions[implementation].app;
                app.root_mode() == Mode::Build && app.status == Status::Idle && !requests.is_empty()
            })
        });
        let implementation = implementation_index(event_loop, source, clear).unwrap();
        if clear {
            assert_linked_parent(event_loop, implementation, source, &actor, history.len());
            assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
            assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        }
        let (_, options, _, _) = requests.recv_timeout(WAIT).unwrap();
        assert_eq!(options, implementation_options());
        assert_eq!(fs::read_to_string(path).unwrap(), PLAN);
        assert!(requests.is_empty());
    });
}
