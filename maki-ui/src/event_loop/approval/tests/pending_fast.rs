use std::sync::Arc;

use maki_agent::actor::{AgentActorHandle, ConfigCommit, ConfigPatch, PreparedModel};
use maki_domain::ModelSelection;
use maki_providers::model::FastSupport;

use test_case::test_case;

use super::{
    Action, ActorStatus, AppSession, ApprovalStep, EventLoop, InternalEvent, SOURCE_MODEL, Status,
    TARGET_MODEL, WAIT, apply_patch, approve, capable_model, final_ready, next_ready, pump_until,
    setup, with_event_loop,
};

const CHECKPOINT_FAILURE: &str = "pending-fast approval checkpoint failed";

fn set_fast_support(
    event_loop: &mut EventLoop<'_>,
    index: usize,
    actor: &AgentActorHandle,
    support: FastSupport,
) -> ConfigCommit {
    let provider = Arc::clone(&actor.effective_config().unwrap().provider);
    let mut model = capable_model(&actor.effective_config().unwrap().model.spec());
    model.supports_fast_override = Some(support);
    let commit = apply_patch(
        actor,
        ConfigPatch {
            fast: Some(false),
            model: Some(PreparedModel { model, provider }),
            ..Default::default()
        },
    );
    event_loop.sessions[index].project_config(&commit);
    commit
}

fn settled_fast(event_loop: &EventLoop<'_>, index: usize) -> bool {
    let runtime = &event_loop.sessions[index];
    let (manager, root) = runtime.handles.manager_and_root();
    let ticket =
        super::reserve_config_change(&manager.lookup(root).unwrap(), &runtime.coordinator, None)
            .unwrap();
    let fast =
        smol::block_on(ticket.commit_change(super::ConfigChange::Patch(ConfigPatch::default())))
            .unwrap()
            .config
            .fast;
    let stored = AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap();
    assert_eq!(stored.meta.fast, fast);
    fast
}

fn committed_config(event: &InternalEvent) -> ConfigCommit {
    let InternalEvent::PlanApprovalReady {
        result: Ok(ApprovalStep::Committed(committed)),
        ..
    } = event
    else {
        panic!("expected committed plan approval");
    };
    committed.config.clone()
}

#[test_case(false, false, false; "identity_supported_ready_first")]
#[test_case(false, false, true; "identity_supported_subscription_first")]
#[test_case(false, true, false; "identity_pending_ready_first")]
#[test_case(false, true, true; "identity_pending_subscription_first")]
#[test_case(true, false, false; "fast_supported_ready_first")]
#[test_case(true, false, true; "fast_supported_subscription_first")]
#[test_case(true, true, false; "fast_pending_ready_first")]
#[test_case(true, true, true; "fast_pending_subscription_first")]
fn explicit_root_approval_supersedes_saved_fast(
    fast_only: bool,
    pending: bool,
    subscription_first: bool,
) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let support = if pending {
            FastSupport::Pending
        } else {
            FastSupport::Supported
        };
        set_fast_support(event_loop, index, &actor, support);
        event_loop.sessions[index].app.state.fast_pending = true;
        event_loop.sessions[index]
            .app
            .plan_form
            .set_implementation_selection(
                ModelSelection {
                    spec: (!fast_only).then(|| TARGET_MODEL.into()),
                    fast: fast_only.then_some(false),
                    ..Default::default()
                },
                SOURCE_MODEL,
            );
        let prepare = Arc::clone(&event_loop.ctx.prepare_provider);
        event_loop.ctx.prepare_provider = Arc::new(move |model, timeouts| {
            let mut prepared = prepare(model, timeouts)?;
            prepared.model.supports_fast_override = Some(support);
            Ok(prepared)
        });
        approve(event_loop, index, false);
        let prepared = final_ready(event_loop, &requests);
        assert!(event_loop.sessions[index].app.state.fast_pending);
        event_loop.handle_internal(prepared);
        let committed = next_ready(event_loop);
        let config = committed_config(&committed);
        assert!(!config.config.fast);
        if subscription_first {
            assert!(event_loop.sessions[index].project_config(&config));
            assert!(!event_loop.sessions[index].app.state.fast_pending);
        }
        event_loop.handle_internal(committed);
        assert!(!event_loop.sessions[index].app.state.fast_pending);
        assert!(!event_loop.sessions[index].project_config(&config));
        if pending {
            let discovered = set_fast_support(event_loop, index, &actor, FastSupport::Supported);
            assert!(!discovered.config.fast);
        }
        assert!(!settled_fast(event_loop, index));
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
                && event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
                && !requests.is_empty()
        });
        assert!(!requests.recv_timeout(WAIT).unwrap().1.fast);
        assert!(!event_loop.sessions[index].app.state.fast_pending);
        assert!(!settled_fast(event_loop, index));
    });
}

#[test_case(false, false; "cancel_pending")]
#[test_case(false, true; "cancel_supported")]
#[test_case(true, false; "checkpoint_failure_pending")]
#[test_case(true, true; "checkpoint_failure_supported")]
fn unsuccessful_root_approval_preserves_saved_fast(checkpoint_failure: bool, supported: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        let projected = event_loop.sessions[index].projected_config.clone();
        let source = set_fast_support(
            event_loop,
            index,
            &actor,
            if supported {
                FastSupport::Supported
            } else {
                FastSupport::Pending
            },
        );
        event_loop.sessions[index].app.state.fast_pending = true;
        event_loop.sessions[index]
            .app
            .plan_form
            .set_implementation_selection(
                ModelSelection {
                    fast: Some(false),
                    ..Default::default()
                },
                SOURCE_MODEL,
            );
        approve(event_loop, index, false);
        let mut prepared = final_ready(event_loop, &requests);
        if supported {
            event_loop.sessions[index].projected_config = projected;
            assert!(event_loop.sessions[index].project_config(&source));
        }
        assert!(event_loop.sessions[index].app.state.fast_pending);
        if checkpoint_failure {
            let InternalEvent::PlanApprovalReady {
                result: Ok(ApprovalStep::Prepared(approval)),
                ..
            } = &mut prepared
            else {
                unreachable!()
            };
            approval.transaction.publish =
                Box::new(|_, _| Box::pin(async { Err(CHECKPOINT_FAILURE.into()) }));
            event_loop.handle_internal(prepared);
            let failed = next_ready(event_loop);
            assert!(
                matches!(&failed, InternalEvent::PlanApprovalReady { result: Err(error), .. }
                if error.contains(CHECKPOINT_FAILURE))
            );
            event_loop.handle_internal(failed);
        } else {
            event_loop.dispatch(index, vec![Action::CancelPlanApproval]);
            event_loop.handle_internal(prepared);
        }
        assert!(!event_loop.sessions[index].app.plan_approval_pending);
        assert_eq!(
            event_loop.sessions[index].app.state.fast_pending,
            !supported
        );
        if !supported {
            set_fast_support(event_loop, index, &actor, FastSupport::Supported);
        }
        assert!(settled_fast(event_loop, index));
        assert!(!event_loop.sessions[index].app.state.fast_pending);
        assert!(requests.is_empty());
    });
}

#[test_case(false; "supported")]
#[test_case(true; "pending")]
fn omitted_root_fast_retains_saved_fast_restore(pending: bool) {
    with_event_loop(|event_loop| {
        let (index, _, requests, _host) = setup(event_loop);
        let (manager, root) = event_loop.sessions[index].handles.manager_and_root();
        let actor = manager.actor(root).unwrap();
        set_fast_support(
            event_loop,
            index,
            &actor,
            if pending {
                FastSupport::Pending
            } else {
                FastSupport::Supported
            },
        );
        event_loop.sessions[index].app.state.fast_pending = true;
        event_loop.sessions[index]
            .app
            .plan_form
            .set_implementation_selection(ModelSelection::default(), SOURCE_MODEL);
        approve(event_loop, index, false);
        let prepared = final_ready(event_loop, &requests);
        event_loop.handle_internal(prepared);
        let committed = next_ready(event_loop);
        event_loop.handle_internal(committed);
        if pending {
            assert!(event_loop.sessions[index].app.state.fast_pending);
            set_fast_support(event_loop, index, &actor, FastSupport::Supported);
        }
        assert!(!event_loop.sessions[index].app.state.fast_pending);
        assert!(settled_fast(event_loop, index));
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
                && event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
                && !requests.is_empty()
        });
        assert!(!requests.recv_timeout(WAIT).unwrap().1.fast);
    });
}
