use std::sync::Arc;
use std::time::Duration;

use maki_commands::{
    CommandError, CommandHost, CommandOutcome, CommandRegistry, HostResponse, InputDispatch,
};
use maki_lua::{EventHandle, PluginHost, UiAction};

const COMMAND_PLUGIN: &str = r#"
    maki.api.register_command({
        name = "/typed",
        tui_only = false,
        arguments = {
            { name = "label", type = "string" },
            { name = "count", type = "integer" },
            { name = "source", type = "file" },
            { name = "destination", type = "directory" },
            { name = "tag", type = "string", optional = true },
            { name = "extras", type = "file", optional = true, variadic = true },
        },
        handler = function(opts)
            local values = opts.values
            local tag = values.tag == nil and "<nil>" or values.tag
            maki.ui.flash(table.concat({
                opts.args,
                table.concat(opts.fargs, ","),
                values.label,
                tostring(values.count),
                values.source,
                values.destination,
                tag,
                table.concat(values.extras, ","),
            }, "|"))
        end,
    })
"#;

const GENERATION_A_PLUGIN: &str = r#"
    maki.api.register_command({
        name = "/typed",
        tui_only = false,
        arguments = { { name = "value", type = "string" } },
        handler = function(opts) maki.ui.flash("A:" .. opts.values.value) end,
    })
"#;

const GENERATION_B_PLUGIN: &str = r#"
    maki.api.register_command({
        name = "/typed",
        tui_only = false,
        arguments = { { name = "value", type = "string" } },
        handler = function(opts) maki.ui.flash("B:" .. opts.values.value) end,
    })
"#;

struct TestCommandHost;

impl CommandHost for TestCommandHost {
    fn request(
        &self,
        _request: maki_commands::HostRequest,
    ) -> maki_commands::CommandFuture<Result<HostResponse, maki_commands::CommandError>> {
        Box::pin(async { Ok(HostResponse::Completed) })
    }
}

fn setup() -> (
    PluginHost,
    CommandRegistry,
    maki_commands::TargetHandle,
    flume::Receiver<UiAction>,
) {
    let host = PluginHost::new(Arc::new(maki_agent::tools::ToolRegistry::new())).unwrap();
    host.load_source("typed_routes", COMMAND_PLUGIN).unwrap();
    let registry = host.command_registry();
    let target = registry.bind_target(
        maki_commands::TargetCapabilities::ALL,
        Arc::new(TestCommandHost),
    );
    let actions = host.ui_action_rx();
    (host, registry, target, actions)
}

fn next_flash(actions: &flume::Receiver<UiAction>) -> String {
    match actions
        .recv_timeout(Duration::from_secs(5))
        .expect("typed command handler did not run")
    {
        UiAction::Flash(message) => message,
        _ => panic!("expected a flash from typed command handler"),
    }
}

fn dispatch_via_registry(
    registry: &CommandRegistry,
    target: &maki_commands::TargetHandle,
    actions: &flume::Receiver<UiAction>,
    raw_args: &str,
) -> String {
    let input = format!("   /typed {raw_args}   ");
    let outcome = smol::block_on(registry.dispatch_input(target, input.as_str().into()));
    assert!(matches!(
        outcome,
        InputDispatch::Dispatched(CommandOutcome::Completed)
    ));
    next_flash(actions)
}

#[test]
fn typed_registry_route_reaches_the_handler_with_parsed_values() {
    let cases = [
        (
            r#""hello world" 42 "src/input file.txt" "build output""#,
            r#""hello world" 42 "src/input file.txt" "build output"|hello world,42,src/input file.txt,build output|hello world|42|src/input file.txt|build output|<nil>|"#,
        ),
        (
            r#""hello world" 42 "src/input file.txt" "build output" "tag value" "extra one" extra-two"#,
            r#""hello world" 42 "src/input file.txt" "build output" "tag value" "extra one" extra-two|hello world,42,src/input file.txt,build output,tag value,extra one,extra-two|hello world|42|src/input file.txt|build output|tag value|extra one,extra-two"#,
        ),
    ];

    for (raw_args, expected) in cases {
        let (_host, registry, target, actions) = setup();
        assert_eq!(
            dispatch_via_registry(&registry, &target, &actions, raw_args),
            expected
        );
    }
}

fn assert_registry_rejects(raw_args: &str) {
    let (_host, registry, target, actions) = setup();
    let input = format!(" /typed {raw_args} ");
    let outcome = smol::block_on(registry.dispatch_input(&target, input.as_str().into()));
    assert!(matches!(
        outcome,
        InputDispatch::Dispatched(CommandOutcome::Failed(
            maki_commands::CommandError::TypedArguments { .. }
        ))
    ));
    assert!(matches!(
        actions.try_recv(),
        Err(flume::TryRecvError::Empty)
    ));
}

#[test]
fn queued_execute_command_rejects_retired_generation_before_worker_consumes_it() {
    let host = PluginHost::new(Arc::new(maki_agent::tools::ToolRegistry::new())).unwrap();
    host.load_source("generation", GENERATION_A_PLUGIN).unwrap();
    let registry = host.command_registry();
    let target = registry.bind_target(
        maki_commands::TargetCapabilities::ALL,
        Arc::new(TestCommandHost),
    );
    let actions = host.ui_action_rx();
    let release = host.pause_worker_for_test();

    let old_outcome = smol::spawn(registry.dispatch_input(&target, "/typed generation-a".into()));

    let replacement = host
        .queue_load_source_for_test("generation", GENERATION_B_PLUGIN)
        .unwrap();
    release.send(()).unwrap();
    replacement
        .recv()
        .expect("replacement load reply")
        .expect("replacement load failed");
    host.wait_for_worker_barrier_for_test();
    assert!(matches!(
        smol::block_on(old_outcome),
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::Producer(error)))
            if error.as_ref() == maki_lua::COMMAND_HANDLER_UNAVAILABLE
    ));

    assert!(matches!(
        actions.try_recv(),
        Err(flume::TryRecvError::Empty)
    ));

    let new_outcome =
        smol::block_on(registry.dispatch_input(&target, "/typed generation-b".into()));
    assert!(matches!(
        new_outcome,
        InputDispatch::Dispatched(CommandOutcome::Completed)
    ));
    assert_eq!(next_flash(&actions), "B:generation-b");
}

#[test]
fn callbacks_none_lifecycle_rejects_retired_generation() {
    let host = PluginHost::new(Arc::new(maki_agent::tools::ToolRegistry::new())).unwrap();
    host.load_source(
        "lifecycle_generation",
        r#"
        maki.api.register_command({
            name = "/deploy",
            tui_only = false,
            arguments = { { name = "value", type = "string", completion = {
                items = {},
                on_cancel = function() maki.ui.flash("old-generation") end,
            } } },
            handler = function() end,
        })
        "#,
    )
    .unwrap();
    let registry = host.command_registry();
    let target = registry.bind_target(
        maki_commands::TargetCapabilities::ALL,
        Arc::new(TestCommandHost),
    );
    let command = registry.resolve_for(&target, "/deploy").unwrap();
    let session = registry.open_completion(command, target.id()).unwrap();
    let _ = smol::block_on(session.complete(Arc::from(""), Arc::from(""), 0, Arc::from("build")));
    let context = maki_lua::CommandArgumentContext {
        command: Arc::from("/deploy"),
        plugin: Arc::from("lifecycle_generation"),
        args: String::new(),
        arg: String::new(),
        index: 0,
        mode: "build".into(),
        session: 1,
        generation: 0,
        command_generation: host
            .event_handle()
            .command_generation_for_test("lifecycle_generation", "/deploy"),
        argument_name: Some(Arc::from("value")),
        argument_kind: Some("string".into()),
        preceding_arguments: Arc::from([]),
    };
    host.load_source(
        "lifecycle_generation",
        r#"
        maki.api.register_command({
            name = "/deploy",
            tui_only = false,
            arguments = { { name = "value", type = "string", completion = {
                items = {},
                on_cancel = function() maki.ui.flash("new-generation") end,
            } } },
            handler = function() end,
        })
        "#,
    )
    .unwrap();
    host.wait_for_worker_barrier_for_test();
    let new_generation = host
        .event_handle()
        .command_generation_for_test("lifecycle_generation", "/deploy");
    assert_ne!(context.command_generation, new_generation);
    let flashes = host.ui_action_rx();
    while flashes.try_recv().is_ok() {}
    handle_lifecycle_for_test(&host.event_handle(), context);
    let result = flashes.recv_timeout(Duration::from_millis(250));
    assert!(result.is_err(), "unexpected lifecycle action received");
}

fn handle_lifecycle_for_test(handle: &EventHandle, context: maki_lua::CommandArgumentContext) {
    handle.command_argument_lifecycle(
        context,
        maki_lua::CommandArgumentLifecycle::Cancel,
        None,
        maki_agent::CancelToken::none(),
    );
}

#[test]
fn invalid_typed_invocations_never_reach_the_handler() {
    let invalid_inputs = [
        (
            r#""hello world" nope "src/input file.txt" "build output""#,
            "invalid integer",
        ),
        (r#""hello world 42"#, "unterminated quote"),
        (r#""hello world" 42"#, "missing required paths"),
    ];

    for (raw_args, _case_name) in invalid_inputs {
        assert_registry_rejects(raw_args);
    }
}
