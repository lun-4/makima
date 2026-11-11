use std::sync::Arc;

use makima_agent::tools::ToolRegistry;
use makima_lua::PluginHost;

const COLLISION: &str = "store entry 'renderers.main' is already registered by plugin 'first'";

fn host() -> PluginHost {
    PluginHost::new(Arc::new(ToolRegistry::new())).unwrap()
}

#[test]
fn same_owner_replaces_and_collect_returns_all_entries() {
    let host = host();
    host.load_source(
        "owner",
        r#"
        makima.store.register("renderers", "main", { version = 1 })
        makima.store.register("renderers", "other", "second")
        makima.store.register("renderers", "main", { version = 2 })
        local values = makima.store.collect("renderers")
        assert(values.main.version == 2)
        assert(values.other == "second")
        assert(next(makima.store.collect("missing")) == nil)
        "#,
    )
    .unwrap();
}

#[test]
fn foreign_owner_collision_fails_without_removing_original() {
    let host = host();
    host.load_source(
        "first",
        r#"makima.store.register("renderers", "main", "first")"#,
    )
    .unwrap();

    let error = host
        .load_source(
            "second",
            r#"makima.store.register("renderers", "main", "second")"#,
        )
        .unwrap_err();
    assert!(error.to_string().contains(COLLISION));

    host.load_source(
        "probe",
        r#"assert(makima.store.collect("renderers").main == "first")"#,
    )
    .unwrap();
}

#[test]
fn entries_are_cleared_on_unload_and_reload() {
    let host = host();
    host.load_source(
        "owner",
        r#"
        makima.store.register("renderers", "old", true)
        makima.store.register("other", "old", true)
        "#,
    )
    .unwrap();
    host.load_source(
        "owner",
        r#"
        assert(makima.store.collect("renderers").old == nil)
        assert(makima.store.collect("other").old == nil)
        makima.store.register("renderers", "new", true)
        "#,
    )
    .unwrap();
    host.unload("owner").unwrap();

    host.load_source(
        "probe",
        r#"assert(next(makima.store.collect("renderers")) == nil)"#,
    )
    .unwrap();
}

#[test]
fn failed_load_clears_entries_registered_during_load() {
    let host = host();
    let error = host
        .load_source(
            "broken",
            r#"
            makima.store.register("renderers", "leaked", true)
            error("load failed")
            "#,
        )
        .unwrap_err();
    assert!(error.to_string().contains("load failed"));

    host.load_source(
        "probe",
        r#"assert(makima.store.collect("renderers").leaked == nil)"#,
    )
    .unwrap();
}

#[test]
fn store_changed_fires_on_register_with_registry_and_key() {
    let host = host();
    host.load_source(
        "watcher",
        r#"
        makima.api.create_autocmd("StoreChanged", {
          callback = function(ev)
            if ev.data and ev.data.key and ev.data.registry ~= "events" then
              makima.store.register("events", ev.data.registry, ev.data.key)
            end
          end,
        })
        "#,
    )
    .unwrap();
    host.load_source(
        "owner",
        r#"
        makima.store.register("renderers", "main", 1)
        makima.store.register("other", "main", 2)
        makima.store.register("renderers", "main", 3)
        "#,
    )
    .unwrap();
    host.load_source(
        "probe",
        r#"
        local events = makima.store.collect("events")
        assert(events.renderers == "main")
        assert(events.other == "main")
        "#,
    )
    .unwrap();
}

#[test]
fn store_changed_fires_when_owner_unloads() {
    let host = host();
    host.load_source(
        "watcher",
        r#"
        makima.api.create_autocmd("StoreChanged", {
          callback = function(ev)
            -- Clears carry no key; registrations do.
            if ev.data and ev.data.registry == "renderers" and ev.data.key == nil then
              makima.store.register("marker", "cleared", true)
            end
          end,
        })
        "#,
    )
    .unwrap();
    host.load_source(
        "owner",
        r#"makima.store.register("renderers", "main", true)"#,
    )
    .unwrap();
    host.load_source(
        "probe_before",
        r#"assert(makima.store.collect("marker").cleared == nil)"#,
    )
    .unwrap();
    host.unload("owner").unwrap();
    host.load_source(
        "probe_after",
        r#"assert(makima.store.collect("marker").cleared == true)"#,
    )
    .unwrap();
}
