use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use maki_agent::actor::{ConfigPatch, PreparedModel};
use maki_agent::session_options::{MODEL_OPTION_ID, SessionOptionError};
use maki_domain::{Effort, ModelSelection, ThinkingConfig};
use maki_providers::model::FastSupport;
use maki_providers::{RequestOptions, ThinkingSupport};
use maki_storage::sessions::StoredMode;
use test_case::test_case;

use super::{
    APPROVAL_CHANGED, ActorStatus, AppSession, ApprovalStep, HISTORY, InternalEvent, Mode, PLAN,
    RecordingProvider, SOURCE_MODEL, Status, TARGET_MODEL, WAIT, apply_patch, approve,
    assert_preserved, capable_model, checkpoint, final_ready, implementation_options,
    implementation_selection, next_ready, pump_until, read_model, read_prefs, read_recents, setup,
    with_event_loop,
};

const CHECKPOINT_FAILURE: &str = "approval checkpoint failed";
const CHECKPOINT_FLASH: &str = "history checkpoint failed: approval checkpoint failed";
const MALFORMED_MODEL: &str = "missing-provider";

#[test_case(false; "keep_context")]
#[test_case(true; "clear_context")]
fn approval_rejects_malformed_identity_without_mutation(clear: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = actor.committed_history();
        let selection = ModelSelection {
            spec: Some(MALFORMED_MODEL.into()),
            ..implementation_selection()
        };
        event_loop.sessions[index]
            .app
            .plan_form
            .set_implementation_selection(selection.clone(), SOURCE_MODEL);
        event_loop.ctx.prepare_provider =
            Arc::new(|_, _| panic!("malformed identity must not prepare provider"));
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
        let expected = SessionOptionError::InvalidValue {
            id: MODEL_OPTION_ID.into(),
            value: MALFORMED_MODEL.into(),
        }
        .to_string();
        assert_eq!(
            event_loop.sessions[index].app.status_bar.flash_text(),
            Some(expected.as_str())
        );
        assert_eq!(
            event_loop.sessions[index]
                .app
                .plan_form
                .implementation_selection(),
            &selection
        );
        assert_eq!(
            event_loop.sessions[index].app.state.plan.path(),
            Some(path.as_path())
        );
        assert!(Arc::ptr_eq(&config, &actor.effective_config().unwrap()));
        assert!(Arc::ptr_eq(&history, &actor.committed_history()));
        assert!(requests.is_empty());
    });
}

#[test_case(false, false, false, false; "model_only_keep")]
#[test_case(true, false, false, false; "model_only_clear")]
#[test_case(false, true, false, false; "options_only_keep")]
#[test_case(true, true, false, false; "options_only_clear")]
#[test_case(false, false, true, false; "disabled_keep")]
#[test_case(true, false, true, false; "disabled_clear")]
#[test_case(false, false, true, true; "required_thinking_keep")]
#[test_case(true, false, true, true; "required_thinking_clear")]
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
            .plan_form
            .set_implementation_selection(selection.clone(), SOURCE_MODEL);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let (record, recorded) = flume::unbounded();
        apply_patch(
            &actor,
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
        event_loop.ctx.prepare_provider = Arc::new(move |model, _| {
            preparation_count.fetch_add(1, Ordering::SeqCst);
            let mut model = capable_model(&model.spec());
            if required {
                model.thinking_override = Some(ThinkingSupport::Required);
            }
            Ok(PreparedModel {
                model,
                provider: Arc::new(RecordingProvider {
                    requests: record.clone(),
                    release: None,
                }),
            })
        });
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
                && event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
                && !recorded.is_empty()
        });
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
        super::assert_actor_selection(&actor, expected_spec, expected);
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
        checkpoint(event_loop, index);
        let restored =
            AppSession::load(event_loop.sessions[index].id(), &event_loop.ctx.storage).unwrap();
        assert_eq!(restored.model, expected_spec);
        assert_eq!(restored.meta.thinking, Some(expected.thinking.into()));
        assert_eq!(restored.meta.fast, expected.fast);
        assert_eq!(restored.meta.mode, Some(StoredMode::Build));
        assert!(recorded.is_empty());
        assert!(requests.is_empty());
    });
}

#[test_case(false; "keep_context")]
#[test_case(true; "clear_context")]
fn approval_model_only_normalizes_unsupported_inherited_options(clear: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        apply_patch(
            &actor,
            ConfigPatch {
                thinking: Some(ThinkingConfig::Effort(Effort::High)),
                ..Default::default()
            },
        );
        event_loop.sessions[index]
            .app
            .plan_form
            .set_implementation_selection(
                ModelSelection {
                    spec: Some(TARGET_MODEL.into()),
                    ..Default::default()
                },
                SOURCE_MODEL,
            );
        let prepare = event_loop.ctx.prepare_provider.clone();
        event_loop.ctx.prepare_provider = Arc::new(move |model, timeouts| {
            let mut prepared = prepare(model, timeouts)?;
            prepared.model.thinking_override = Some(ThinkingSupport::No);
            prepared.model.supports_fast_override = Some(FastSupport::Unsupported);
            Ok(prepared)
        });
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
                && !requests.is_empty()
        });
        let (_, options, _, _) = requests.recv_timeout(WAIT).unwrap();
        assert_eq!(
            options,
            RequestOptions {
                thinking: ThinkingConfig::Off,
                fast: false
            }
        );
        super::assert_actor_selection(&actor, TARGET_MODEL, options);
        checkpoint(event_loop, index);
        let restored =
            AppSession::load(event_loop.sessions[index].id(), &event_loop.ctx.storage).unwrap();
        assert_eq!(restored.meta.thinking, Some(options.thinking.into()));
        assert_eq!(restored.meta.fast, options.fast);
    });
}

#[test_case(false; "keep_context")]
#[test_case(true; "clear_context")]
fn approval_option_only_change_makes_request_stale(clear: bool) {
    with_event_loop(|event_loop| {
        let (index, path, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let config = actor.effective_config().unwrap();
        let history = actor.committed_history();
        let recents = read_recents(&event_loop.ctx.storage);
        let model = read_model(&event_loop.ctx.storage);
        approve(event_loop, index, clear);
        let ready = final_ready(event_loop, &requests);
        let mut selection = implementation_selection();
        selection.fast = Some(true);
        event_loop.sessions[index]
            .app
            .plan_form
            .set_implementation_selection(selection.clone(), SOURCE_MODEL);
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

#[test_case(false, false; "thinking_keep")]
#[test_case(true, false; "thinking_clear")]
#[test_case(false, true; "fast_keep")]
#[test_case(true, true; "fast_clear")]
fn approval_rejects_unsupported_explicit_options(clear: bool, fast: bool) {
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
            .plan_form
            .set_implementation_selection(selection.clone(), SOURCE_MODEL);
        let prepare = event_loop.ctx.prepare_provider.clone();
        event_loop.ctx.prepare_provider = Arc::new(move |model, timeouts| {
            let mut prepared = prepare(model, timeouts)?;
            if fast {
                prepared.model.supports_fast_override = Some(FastSupport::Unsupported);
            } else {
                prepared.model.thinking_override = Some(ThinkingSupport::No);
            }
            Ok(prepared)
        });
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
        });
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
        approve(event_loop, index, clear);
        pump_until(event_loop, |event_loop| {
            event_loop.sessions[index].app.root_mode() == Mode::Build
                && event_loop.sessions[index].app.status == Status::Idle
                && !requests.is_empty()
        });
        let (_, options, _, _) = requests.recv_timeout(WAIT).unwrap();
        assert_eq!(options, implementation_options());
        assert_eq!(fs::read_to_string(path).unwrap(), PLAN);
        assert!(requests.is_empty());
    });
}
