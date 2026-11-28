use std::sync::Arc;

use makima_agent::actor::{
    AgentActorHandle, ConfigChange, ConfigCommit, ConfigPatch, PreparedModel,
};
use makima_domain::ModelSelection;
use makima_providers::model::FastSupport;

use test_case::test_case;

use super::options::{apply_source_patch, assert_linked_parent, implementation_index};
use super::{
    ActorStatus, AppSession, ApprovalStep, EventLoop, InternalEvent, Status, TARGET_MODEL, WAIT,
    approve, assert_pending_fast, capable_model, final_ready, next_ready, pump_until,
    seed_pending_fast, setup, with_event_loop,
};

fn set_fast_support(
    event_loop: &mut EventLoop<'_>,
    index: usize,
    actor: &AgentActorHandle,
    support: FastSupport,
) -> ConfigCommit {
    let provider = Arc::clone(&actor.effective_config().unwrap().provider);
    let mut model = capable_model(&actor.effective_config().unwrap().model.spec());
    model.fast_support = Some(support);
    apply_source_patch(
        event_loop,
        index,
        ConfigPatch {
            fast: Some(false),
            model: Some(PreparedModel { model, provider }),
            ..Default::default()
        },
    )
}

fn settled_fast(event_loop: &EventLoop<'_>, index: usize) -> bool {
    let runtime = &event_loop.sessions[index];
    let (manager, root) = runtime.handles.manager_and_root();
    let mut ticket =
        super::reserve_config_change(&manager.lookup(root).unwrap(), &runtime.coordinator, None)
            .unwrap();
    let config = smol::block_on(ticket.wait_predecessor()).unwrap();
    let stored = AppSession::load(runtime.id(), &event_loop.ctx.storage).unwrap();
    let folded = stored.latest_state().unwrap();
    let stored_config = folded.scopes[&Some(root)].config.as_ref().unwrap();
    assert_eq!(stored_config.fast, config.fast);
    assert_eq!(
        stored_config
            .options
            .get(super::FAST_PENDING_MODEL_OPTION_ID),
        None
    );
    config.fast
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

#[test_case(false, false, false; "identity_supported")]
#[test_case(false, true, false; "identity_pending")]
#[test_case(true, false, false; "fast_supported")]
#[test_case(true, true, false; "fast_pending")]
#[test_case(false, false, true; "linked_identity_supported")]
#[test_case(false, true, true; "linked_identity_pending")]
#[test_case(true, false, true; "linked_fast_supported")]
#[test_case(true, true, true; "linked_fast_pending")]
fn explicit_root_approval_supersedes_saved_fast(fast_only: bool, pending: bool, linked: bool) {
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
        let pending_fast_model = seed_pending_fast(event_loop, index);
        event_loop.sessions[index]
            .app
            .stage_plan_selection(ModelSelection {
                spec: (!fast_only).then(|| TARGET_MODEL.into()),
                fast: fast_only.then_some(false),
                ..Default::default()
            });
        let prepare = Arc::clone(&event_loop.ctx.prepare_provider);
        event_loop.ctx.prepare_provider = Arc::new(move |spec| {
            let prepared = prepare(spec);
            Box::pin(async move {
                let mut prepared = prepared.await?;
                prepared.model.fast_support = Some(support);
                Ok(prepared)
            })
        });
        let source = event_loop.sessions[index].id();
        let source_config = actor.effective_config().unwrap();
        let source_history = actor.committed_history();
        let source_stored_config = super::root_stored_config(event_loop, index);
        approve(event_loop, index, linked);
        assert_pending_fast(event_loop, index, Some(&pending_fast_model));
        let prepared = final_ready(event_loop, &requests);
        assert_pending_fast(event_loop, index, Some(&pending_fast_model));
        event_loop.handle_internal(prepared);
        let committed = next_ready(event_loop);
        assert!(!committed_config(&committed).config.fast);
        assert_pending_fast(
            event_loop,
            index,
            linked.then_some(pending_fast_model.as_str()),
        );
        event_loop.handle_internal(committed);
        pump_until(event_loop, |event_loop| {
            implementation_index(event_loop, source, linked).is_some_and(|implementation| {
                let runtime = &event_loop.sessions[implementation];
                let (manager, root) = runtime.handles.manager_and_root();
                !runtime.app.plan_approval_pending
                    && runtime.app.status == Status::Idle
                    && manager.actor(root).unwrap().snapshot().status == ActorStatus::Idle
                    && !requests.is_empty()
            })
        });
        let implementation = implementation_index(event_loop, source, linked).unwrap();
        let (implementation_manager, implementation_root) = event_loop.sessions[implementation]
            .handles
            .manager_and_root();
        let implementation_actor = implementation_manager.actor(implementation_root).unwrap();
        if linked {
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
            assert_eq!(
                view.scopes[&Some(root)].config.as_ref().unwrap(),
                &source_stored_config
            );
        }
        assert_pending_fast(event_loop, implementation, None);
        if pending {
            let current = implementation_actor.effective_config().unwrap();
            let mut model = current.model.clone();
            model.fast_support = Some(FastSupport::Supported);
            let discovered = smol::block_on(
                implementation_actor
                    .reserve_prepared_operation(None)
                    .unwrap()
                    .commit_when_ready(Some(ConfigChange::Refresh {
                        expected: PreparedModel {
                            model: current.model.clone(),
                            provider: Arc::clone(&current.provider),
                        },
                        replacement: PreparedModel {
                            model,
                            provider: Arc::clone(&current.provider),
                        },
                    })),
            )
            .unwrap()
            .config;
            assert!(!discovered.config.fast);
        }
        assert!(!requests.recv_timeout(WAIT).unwrap().1.fast);
        assert!(!settled_fast(event_loop, implementation));
    });
}
