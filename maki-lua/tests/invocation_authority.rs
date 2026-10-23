mod common;

use std::sync::Arc;

use maki_agent::tools::ToolRegistry;
use maki_commands::ResolutionError;
use maki_lua::agent_errors::ORCHESTRATION_CONTEXT_EXPIRED;
use maki_lua::test_support::{InMemoryFs, run_command};
use maki_lua::{PluginHost, UiAction};
use test_case::test_case;

const OWNER: &str = "invocation_owner";
const CONTROLLER: &str = "invocation_controller";
const PARK: &str = "/invocation_park";
const RELEASE: &str = "/invocation_release";
const PUBLISHED: &str = "/invocation_published";
const READY: &str = "invocation parked";

fn host(registry: Arc<ToolRegistry>) -> PluginHost {
    PluginHost::with_session_provider_preparer(
        registry,
        Default::default(),
        true,
        Arc::new(InMemoryFs::new()),
        None,
        None,
        maki_lua::ui_sink::HostUi::Frontend,
    )
    .unwrap()
}

#[test_case(true; "accepted_reload_revokes_effects")]
#[test_case(false; "rejected_reload_preserves_effects")]
fn suspended_command_effects_use_captured_owner(accepted: bool) {
    let host = host(Arc::clone(ToolRegistry::global_arc()));
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
                    local ran, run_err = maki.api.run_command(ctx, '/invocation_release')
                    assert(not ran and run_err == expected_error)
                    local timer, timer_err = maki.timer.set(1, function() end)
                    assert(not timer and timer_err == expected_error)
                    local autocmd, autocmd_err = maki.api.create_autocmd('User', { callback = function() end })
                    assert(not autocmd and autocmd_err == expected_error)
                    for _, effect in ipairs({
                        function() return maki.api.mode.define({ name = 'revoked_mode' }) end,
                        function() return maki.api.mode.reset('build') end,
                        function() return maki.session.set_default_thinking('off') end,
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
        let parked = run_command(&host.command_registry(), PARK, 0, None);
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
        let release = run_command(&host.command_registry(), RELEASE, 0, None);
        assert_eq!(release.recv_async().await.unwrap(), Ok(()));
        assert_eq!(parked.recv_async().await.unwrap(), Ok(()));
        let published = run_command(&host.command_registry(), PUBLISHED, 0, None);
        let expected = if accepted {
            Err(ResolutionError::UnknownCommand(Arc::from(PUBLISHED)).to_string())
        } else {
            Ok(())
        };
        assert_eq!(published.recv_async().await.unwrap(), expected);
        assert!(host.ui_action_rx().try_recv().is_err());
    }));
}
