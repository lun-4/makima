use std::sync::Arc;

use maki_agent::tools::ToolRegistry;
use maki_lua::PluginHost;

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
        maki.store.register("renderers", "main", { version = 1 })
        maki.store.register("renderers", "other", "second")
        maki.store.register("renderers", "main", { version = 2 })
        local values = maki.store.collect("renderers")
        assert(values.main.version == 2)
        assert(values.other == "second")
        assert(next(maki.store.collect("missing")) == nil)
        "#,
    )
    .unwrap();
}

#[test]
fn foreign_owner_collision_fails_without_removing_original() {
    let host = host();
    host.load_source(
        "first",
        r#"maki.store.register("renderers", "main", "first")"#,
    )
    .unwrap();

    let error = host
        .load_source(
            "second",
            r#"maki.store.register("renderers", "main", "second")"#,
        )
        .unwrap_err();
    assert!(error.to_string().contains(COLLISION));

    host.load_source(
        "probe",
        r#"assert(maki.store.collect("renderers").main == "first")"#,
    )
    .unwrap();
}

#[test]
fn entries_are_cleared_on_unload_and_reload() {
    let host = host();
    host.load_source(
        "owner",
        r#"
        maki.store.register("renderers", "old", true)
        maki.store.register("other", "old", true)
        "#,
    )
    .unwrap();
    host.load_source(
        "owner",
        r#"
        assert(maki.store.collect("renderers").old == nil)
        assert(maki.store.collect("other").old == nil)
        maki.store.register("renderers", "new", true)
        "#,
    )
    .unwrap();
    host.unload("owner").unwrap();

    host.load_source(
        "probe",
        r#"assert(next(maki.store.collect("renderers")) == nil)"#,
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
            maki.store.register("renderers", "leaked", true)
            error("load failed")
            "#,
        )
        .unwrap_err();
    assert!(error.to_string().contains("load failed"));

    host.load_source(
        "probe",
        r#"assert(maki.store.collect("renderers").leaked == nil)"#,
    )
    .unwrap();
}

#[test]
fn store_changed_fires_on_register_with_registry_and_key() {
    let host = host();
    host.load_source(
        "watcher",
        r#"
        maki.api.create_autocmd("StoreChanged", {
          callback = function(ev)
            if ev.data and ev.data.key and ev.data.registry ~= "events" then
              maki.store.register("events", ev.data.registry, ev.data.key)
            end
          end,
        })
        "#,
    )
    .unwrap();
    host.load_source(
        "owner",
        r#"
        maki.store.register("renderers", "main", 1)
        maki.store.register("other", "main", 2)
        maki.store.register("renderers", "main", 3)
        "#,
    )
    .unwrap();
    host.load_source(
        "probe",
        r#"
        local events = maki.store.collect("events")
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
        maki.api.create_autocmd("StoreChanged", {
          callback = function(ev)
            -- Clears carry no key; registrations do.
            if ev.data and ev.data.registry == "renderers" and ev.data.key == nil then
              maki.store.register("marker", "cleared", true)
            end
          end,
        })
        "#,
    )
    .unwrap();
    host.load_source("owner", r#"maki.store.register("renderers", "main", true)"#)
        .unwrap();
    host.load_source(
        "probe_before",
        r#"assert(maki.store.collect("marker").cleared == nil)"#,
    )
    .unwrap();
    host.unload("owner").unwrap();
    host.load_source(
        "probe_after",
        r#"assert(maki.store.collect("marker").cleared == true)"#,
    )
    .unwrap();
}
