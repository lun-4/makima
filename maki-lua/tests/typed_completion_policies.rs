use std::sync::Arc;

use maki_lua::{PluginError, PluginHost};

fn registry() -> Arc<maki_agent::tools::ToolRegistry> {
    Arc::new(maki_agent::tools::ToolRegistry::new())
}

#[test]
fn typed_completion_policies_accept_documented_forms() {
    let host = PluginHost::new(registry()).unwrap();
    host.load_source(
        "policies",
        r#"
        local function handler() end
        maki.api.register_command({
            name = "/policies",
            tui_only = false,
            arguments = {
                { name = "disabled", type = "string", completion = false },
                { name = "replace", type = "string", completion = {
                    mode = "replace",
                    items = {{ label = "x", insertion = "x" }},
                } },
                { name = "extend", type = "string", completion = {
                    mode = "extend",
                    items = {{ label = "x", insertion = "x" }},
                } },
            },
            handler = handler,
        })
        "#,
    )
    .unwrap();

    let registry = host.command_registry();
    let target = registry.bind_target(maki_commands::TargetCapabilities::ALL, Arc::new(NoopHost));
    let command = registry.resolve_for(&target, "/policies").unwrap();
    let maki_commands::CommandArguments::Positional(arguments) = &command.spec().arguments else {
        panic!("expected typed arguments");
    };
    assert_eq!(
        arguments[0].completion,
        maki_commands::CompletionPolicy::Disabled
    );
    assert_eq!(
        arguments[1].completion,
        maki_commands::CompletionPolicy::Replace
    );
    assert_eq!(
        arguments[2].completion,
        maki_commands::CompletionPolicy::Extend
    );
}

#[test]
fn typed_completion_policy_rejects_conflicting_mode_and_policy() {
    let host = PluginHost::new(registry()).unwrap();
    let error = host
        .load_source(
            "conflicting_policy",
            r#"
            maki.api.register_command({
                name = "/conflict",
                tui_only = false,
                arguments = {
                    {
                        name = "value",
                        type = "string",
                        completion = {
                            mode = "replace",
                            policy = "extend",
                            items = {{ label = "x", insertion = "x" }},
                        },
                    },
                },
                handler = function() end,
            })
            "#,
        )
        .expect_err("expected conflicting policy rejection");
    assert!(matches!(error, PluginError::Lua { .. }));
    assert!(
        error.to_string().contains("only one of 'mode' or 'policy'"),
        "unexpected error: {error}"
    );
}

struct NoopHost;

impl maki_commands::CommandHost for NoopHost {
    fn request(
        &self,
        _request: maki_commands::HostRequest,
    ) -> maki_commands::CommandFuture<
        Result<maki_commands::HostResponse, maki_commands::CommandError>,
    > {
        Box::pin(async { Ok(maki_commands::HostResponse::Completed) })
    }
}
