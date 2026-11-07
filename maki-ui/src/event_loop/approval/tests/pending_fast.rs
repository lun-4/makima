use std::sync::Arc;

use maki_agent::actor::{AgentActorHandle, ConfigCommit, ConfigPatch, PreparedModel};
use maki_domain::ModelSelection;
use maki_providers::model::FastSupport;

use test_case::test_case;

use super::{
    ActorStatus, AppSession, ApprovalStep, EventLoop, InternalEvent, Status, TARGET_MODEL, WAIT,
    apply_patch, approve, capable_model, final_ready, next_ready, pump_until, setup,
    with_event_loop,
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

/// An explicit model or fast choice replaces a restored `/fast` wish as soon
/// as approval starts, the same as an ordinary model selection does.
#[test_case(false, false; "identity_supported")]
#[test_case(false, true; "identity_pending")]
#[test_case(true, false; "fast_supported")]
#[test_case(true, true; "fast_pending")]
fn explicit_root_approval_supersedes_saved_fast(fast_only: bool, pending: bool) {
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
            .set_implementation_selection(ModelSelection {
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
        approve(event_loop, index, false);
        assert!(!event_loop.sessions[index].app.state.fast_pending);
        let prepared = final_ready(event_loop, &requests);
        event_loop.handle_internal(prepared);
        let committed = next_ready(event_loop);
        assert!(!committed_config(&committed).config.fast);
        event_loop.handle_internal(committed);
        if pending {
            let discovered = set_fast_support(event_loop, index, &actor, FastSupport::Supported);
            assert!(!discovered.config.fast);
        }
        pump_until(event_loop, |event_loop| {
            !event_loop.sessions[index].app.plan_approval_pending
                && event_loop.sessions[index].app.status == Status::Idle
                && actor.snapshot().status == ActorStatus::Idle
                && !requests.is_empty()
        });
        assert!(!requests.recv_timeout(WAIT).unwrap().1.fast);
        assert!(!settled_fast(event_loop, index));
    });
}
