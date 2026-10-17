mod common;

use std::sync::Arc;

use maki_agent::cancel::{CancelMap, CancelToken};
use maki_agent::tools::ToolRegistry;
use maki_agent::{AgentId, AgentMode};
use maki_lua::agent_errors::{ORCHESTRATION_CONTEXT_EXPIRED, UNMANAGED_AUTHORITY_INACTIVE};
use maki_lua::host_root::HostRootServices;
use maki_lua::test_support::InMemoryFs;
use maki_lua::{PluginHost, UiAction};
use serde_json::json;
use test_case::test_case;

const OWNER: &str = "invocation_owner";
const CONTROLLER: &str = "invocation_controller";
const PARK: &str = "/invocation_park";
const RELEASE: &str = "/invocation_release";
const PUBLISHED: &str = "/invocation_published";
const READY: &str = "invocation parked";
const UNAVAILABLE: &str = "command handler is unavailable";
const HOST_RETIRED: &str = "host root reference is stale";
const PROBE: &str = "invocation_child_probe";

fn host(registry: Arc<ToolRegistry>) -> PluginHost {
    PluginHost::with_session_provider_preparer(
        registry,
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
    )
    .unwrap()
}

#[test_case(true; "accepted_reload_revokes_effects")]
#[test_case(false; "rejected_reload_preserves_effects")]
fn suspended_command_effects_use_captured_owner(accepted: bool) {
    let host = host(Arc::clone(ToolRegistry::global_arc()));
    let (ctx, _trigger) = common::production_like_ctx();
    host.event_handle()
        .install_host_root_services(Arc::new(HostRootServices::fixed(
            AgentId::generate(),
            &ctx.model,
            ctx.opts,
            ctx.model_policy.clone(),
        )));
    host.wait_for_worker_barrier_for_test();
    host.load_source(
        CONTROLLER,
        r#"
        local gate = {}
        maki.store.register('invocation_gate', 'gate', gate)
        maki.api.register_command({
            name = '/invocation_release', description = 'release parked handler', tui_only = false,
            handler = function() gate.release() end,
        })
        "#,
    )
    .unwrap();
    host.load_source(
        OWNER,
        r#"
        maki.api.register_command({
            name = '/invocation_park', description = 'exercise captured authority', tui_only = false,
            handler = function(_, ctx)
                assert(ctx ~= nil)
                maki.async.await(1, function(callback)
                    maki.store.collect('invocation_gate').gate.release = callback
                    maki.ui.flash('invocation parked')
                end)
                local ok, err = maki.api.register_command({
                    name = '/invocation_published', description = 'must retain original authority',
                    tui_only = false, handler = function() end,
                })
                if not ok then
                    assert(err == expected_error)
                    local ran, run_err = maki.api.run_command('/invocation_release')
                    assert(not ran and run_err == expected_error)
                    local timer, timer_err = maki.timer.set(1, function() end)
                    assert(not timer and timer_err == expected_error)
                    local autocmd, autocmd_err = maki.api.create_autocmd('User', { callback = function() end })
                    assert(not autocmd and autocmd_err == expected_error)
                    for _, effect in ipairs({
                        function() return maki.api.mode.define({ name = 'revoked_mode' }) end,
                        function() return maki.api.mode.reset('build') end,
                        function() return maki.api.mode.set('build') end,
                        function() return maki.model.set({ fast = false }) end,
                        function() return maki.session.set_thinking({ mode = 'off' }) end,
                        function() return maki.session.set_default_thinking('off') end,
                        function() return maki.session.set_option('fast', 'disabled') end,
                    }) do
                        local value, effect_err = effect()
                        assert(not value and effect_err == expected_error, effect_err)
                    end
                else
                    assert(err == nil)
                    assert(maki.api.mode.define({ name = 'live_mode' }))
                    assert(maki.api.mode.reset('build'))
                end
            end,
        })
        "#
        .replace("expected_error", &format!("{ORCHESTRATION_CONTEXT_EXPIRED:?}"))
        .as_str(),
    )
    .unwrap();
    smol::block_on(common::bounded(async {
        let parked = host.event_handle().run_command_for_test(
            Arc::from(OWNER),
            Arc::from(PARK),
            String::new(),
            0,
        );
        assert!(
            matches!(host.ui_action_rx().recv_async().await.unwrap(), UiAction::Flash(text) if text == READY)
        );
        if accepted {
            host.load_source(OWNER, "").unwrap();
        } else {
            assert!(
                host.load_source(OWNER, "error('reject candidate')")
                    .is_err()
            );
        }
        let release = host.event_handle().run_command_for_test(
            Arc::from(CONTROLLER),
            Arc::from(RELEASE),
            String::new(),
            0,
        );
        assert_eq!(release.recv_async().await.unwrap(), Ok(()));
        assert_eq!(parked.recv_async().await.unwrap(), Ok(()));
        let published = host.event_handle().run_command_for_test(
            Arc::from(OWNER),
            Arc::from(PUBLISHED),
            String::new(),
            0,
        );
        let expected = if accepted {
            Err(UNAVAILABLE.to_string())
        } else {
            Ok(())
        };
        assert_eq!(published.recv_async().await.unwrap(), expected);
        assert!(host.ui_action_rx().try_recv().is_err());
    }));
}

#[test]
fn standalone_control_and_mode_are_bound_to_live_root_invocations() {
    let registry = Arc::clone(ToolRegistry::global_arc());
    let host = host(registry.clone());
    host.load_source(
        OWNER,
        &format!(
            r#"
            local child, first
            maki.api.register_tool({{
                name = '{PROBE}', description = 'exercise standalone control across turns',
                schema = {{ type = 'object', properties = {{ stage = {{ type = 'string' }} }} }},
                handler = function(input, ctx)
                    assert(ctx:mode() == input.stage or input.stage ~= 'plan' and input.stage ~= 'build')
                    if input.stage == 'plan' then
                        child = assert(maki.agent.spawn(ctx, {{ inherit_provider = true, silent = true, tools = {{}} }}))
                        first = assert(child:send(ctx, 'first'))
                        assert(first:wait(ctx))
                    elseif input.stage == 'foreign' then
                        local value, err = child:status(ctx)
                        assert(not value and err == '{UNMANAGED_AUTHORITY_INACTIVE}')
                    elseif input.stage == 'retired' then
                        local value, err = child:status(ctx)
                        assert(not value and err == '{HOST_RETIRED}')
                    else
                        assert(first:wait(ctx))
                        local turn = assert(child:send(ctx, 'second'))
                        assert(turn:wait(ctx))
                        assert(child:close(ctx))
                    end
                    return 'ok'
                end,
            }})
            "#
        ),
    )
    .unwrap();
    let (mut first, _events, _trigger) = common::ctx_with_canned_provider();
    first.mode = AgentMode::Plan("/plan.md".into());
    first.registry = registry.clone();
    let root_id = AgentId::generate();
    first.execution_agent_id = Some(root_id);
    let services = Arc::new(HostRootServices::fixed(
        root_id,
        &first.model,
        first.opts,
        first.model_policy.clone(),
    ));
    host.event_handle()
        .install_host_root_services(services.clone());
    host.wait_for_worker_barrier_for_test();
    assert_eq!(
        common::exec_tool_text(&registry, &first, PROBE, json!({"stage": "plan"})).unwrap(),
        "ok"
    );
    let mut foreign = first.clone();
    foreign.subagent_cancels = Arc::new(CancelMap::new());
    assert_eq!(
        common::exec_tool_text(&registry, &foreign, PROBE, json!({"stage": "foreign"})).unwrap(),
        "ok"
    );
    let mut second = first.clone();
    second.cancel = CancelToken::none();
    second.mode = AgentMode::Build;
    assert_eq!(
        common::exec_tool_text(&registry, &second, PROBE, json!({"stage": "build"})).unwrap(),
        "ok"
    );
    services.retire();
    assert_eq!(
        common::exec_tool_text(&registry, &second, PROBE, json!({"stage": "retired"})).unwrap(),
        "ok"
    );
}
