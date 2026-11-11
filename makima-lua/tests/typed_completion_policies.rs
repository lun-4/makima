use std::sync::Arc;

use makima_lua::{PluginError, PluginHost};

fn registry() -> Arc<makima_agent::tools::ToolRegistry> {
    Arc::new(makima_agent::tools::ToolRegistry::new())
}

#[test]
fn typed_completion_policies_accept_documented_forms() {
    let host = PluginHost::new(registry()).unwrap();
    host.load_source(
        "policies",
        r#"
        local function handler() end
        makima.api.register_command({
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
    let target = registry.bind_target(makima_commands::TargetCapabilities::ALL, Arc::new(NoopHost));
    let command = registry.resolve_for(&target, "/policies").unwrap();
    let makima_commands::CommandArguments::Positional(arguments) = &command.spec().arguments else {
        panic!("expected typed arguments");
    };
    assert_eq!(
        arguments[0].completion,
        makima_commands::CompletionPolicy::Disabled
    );
    assert_eq!(
        arguments[1].completion,
        makima_commands::CompletionPolicy::Replace
    );
    assert_eq!(
        arguments[2].completion,
        makima_commands::CompletionPolicy::Extend
    );
}

#[test]
fn typed_completion_policy_rejects_conflicting_mode_and_policy() {
    let host = PluginHost::new(registry()).unwrap();
    let error = host
        .load_source(
            "conflicting_policy",
            r#"
            makima.api.register_command({
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

impl makima_commands::CommandHost for NoopHost {
    fn request(
        &self,
        _request: makima_commands::HostRequest,
    ) -> makima_commands::CommandFuture<
        Result<makima_commands::HostResponse, makima_commands::CommandError>,
    > {
        Box::pin(async { Ok(makima_commands::HostResponse::Completed) })
    }
}
