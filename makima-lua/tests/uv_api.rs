use std::sync::Arc;

use makima_agent::tools::ToolRegistry;
use makima_lua::{PluginHost, PluginPermissions};

fn setup() -> PluginHost {
    let reg = Arc::new(ToolRegistry::new());
    PluginHost::new(reg).unwrap()
}

#[test]
fn os_getenv_reads_existing_var() {
    let host = setup();
    host.load_source(
        "getenv_existing",
        r#"
        local home = makima.uv.os_getenv("HOME")
        assert(home ~= nil, "HOME should be set")
        assert(type(home) == "string", "HOME should be a string")
        "#,
    )
    .unwrap();
}

#[test]
fn os_getenv_returns_nil_for_missing_var() {
    let host = setup();
    host.load_source(
        "getenv_missing",
        r#"
        local val = makima.uv.os_getenv("MAKIMA_TEST_VAR_DOES_NOT_EXIST_12345")
        assert(val == nil, "unset var should return nil, got: " .. tostring(val))
        "#,
    )
    .unwrap();
}

#[test]
fn os_getenv_denied_without_env_permission() {
    let host = setup();
    host.load_source_with_permissions(
        "getenv_denied",
        r#"
        local ok, err = pcall(function()
            return makima.uv.os_getenv("HOME")
        end)
        assert(not ok, "should fail without env permission")
        assert(
            tostring(err):find("permission denied"),
            "should mention permission denied, got: " .. tostring(err)
        )
        "#,
        PluginPermissions::denied(),
    )
    .unwrap();
}
