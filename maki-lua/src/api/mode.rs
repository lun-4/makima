//! `maki.api.mode`: define and override agent modes. Definitions land in the
//! shared `ModeRegistry` (built-ins `build`/`plan` included), so every
//! frontend sees the same modes. `Agent:set_mode` switches an agent's mode.

use std::sync::Arc;

use maki_agent::{ModeDefSpec, ModeId, ModeRegistry};
use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table, Value};

use crate::agent_errors::DELEGATED_HOST_MUTATION;
use crate::api::util::pair::{Pair, err_pair, try_pair};

const NO_REGISTRY_ERR: &str = "mode registry not available";

fn registry(lua: &Lua) -> Result<Arc<ModeRegistry>, mlua::Error> {
    lua.app_data_ref::<Arc<ModeRegistry>>()
        .map(|r| Arc::clone(&r))
        .ok_or_else(|| mlua::Error::runtime(NO_REGISTRY_ERR))
}

/// Evaluate a Lua `system_prompt` function (or pass a plain string through)
/// with the `{ cwd, plan_path }` context.
fn eval_system_prompt(lua: &Lua, value: Value) -> LuaResult<Option<String>> {
    match value {
        Value::Nil => Ok(None),
        Value::String(s) => Ok(Some(s.to_str()?.to_owned())),
        Value::Function(f) => {
            let cwd = std::env::current_dir()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default();
            let ctx = lua.create_table()?;
            ctx.set("cwd", cwd)?;
            ctx.set("plan_path", Value::Nil)?;
            match f.call::<mlua::Value>(ctx)? {
                Value::Nil => Ok(None),
                Value::String(s) => Ok(Some(s.to_str()?.to_owned())),
                other => Err(mlua::Error::runtime(format!(
                    "system_prompt function must return a string or nil, got {:?}",
                    other.type_name()
                ))),
            }
        }
        other => Err(mlua::Error::runtime(format!(
            "system_prompt must be a string, function, or nil, got {:?}",
            other.type_name()
        ))),
    }
}

fn spec_from_table(lua: &Lua, opts: Table) -> LuaResult<ModeDefSpec> {
    let name: String = opts.get("name")?;
    let label = opts.get::<Option<String>>("label")?.map(Arc::from);
    let system_prompt = eval_system_prompt(lua, opts.get("system_prompt")?)?;
    let restrict_write_to = opts
        .get::<Option<String>>("restrict_write_to")?
        .map(Into::into);
    let tools = opts.get::<Option<Vec<String>>>("tools")?;
    Ok(ModeDefSpec {
        name,
        label,
        system_prompt,
        restrict_write_to,
        tools,
    })
}

/// Defines a new mode or fully overrides an existing one (built-ins included).
///
/// @param opts table { name = "audit", label = "[AUDIT]", system_prompt = string|fn(ctx)->string, restrict_write_to = string, tools = string[] }
/// @return (boolean, string|nil) `true` on success, or nil and an error.
/// @example
/// maki.api.mode.define({ name = "plan", label = "[PLAN]", tools = { "read", "write" } })
#[lua_fn]
fn define(lua: &Lua, opts: Table) -> LuaResult<Pair<bool>> {
    if crate::runtime::in_delegated_tool(lua) {
        return Ok(err_pair(DELEGATED_HOST_MUTATION));
    }
    try_pair!(crate::runtime::validate_plugin_effect(lua));
    let spec = spec_from_table(lua, opts)?;
    let reg = registry(lua)?;
    match reg.define(spec) {
        Ok(()) => Ok((Some(true), None)),
        Err(e) => Ok(err_pair(e.to_string())),
    }
}

/// Lists all known modes as `{ name, label }` pairs (built-ins first).
///
/// @return (table|nil, string|nil) Array of `{name, label}`, or nil and an error.
/// @example
/// for _, m in ipairs(maki.api.mode.list()) do print(m.name) end
#[lua_fn]
fn list(lua: &Lua) -> LuaResult<Pair<Table>> {
    let reg = registry(lua)?;
    let out = lua.create_table()?;
    for (i, def) in reg.list().iter().enumerate() {
        let row = lua.create_table()?;
        row.set("name", def.id.key())?;
        row.set("label", def.label.as_ref())?;
        out.set(i + 1, row)?;
    }
    Ok((Some(out), None))
}

/// Restores a built-in's default definition, dropping any plugin override.
/// With no argument, resets every built-in.
///
/// @param name string|nil Mode id to reset ("build" or "plan").
/// @return (boolean, string|nil) `true` on success, or nil and an error.
/// @example
/// maki.api.mode.reset("plan")
#[lua_fn]
fn reset(lua: &Lua, name: Option<String>) -> LuaResult<Pair<bool>> {
    if crate::runtime::in_delegated_tool(lua) {
        return Ok(err_pair(DELEGATED_HOST_MUTATION));
    }
    try_pair!(crate::runtime::validate_plugin_effect(lua));
    let reg = registry(lua)?;
    let result = match name {
        Some(name) => reg.reset(&ModeId::parse(&name)),
        None => reg.reset(&ModeId::Build).and(reg.reset(&ModeId::Plan)),
    };
    match result {
        Ok(()) => Ok((Some(true), None)),
        Err(e) => Ok(err_pair(e.to_string())),
    }
}

lua_table! {
    /// `maki.api.mode`: define, override and list agent modes. Built-in
    /// `build` and `plan` are pre-registered, so overrides use the same call
    /// as defining a custom mode. Switch an agent with `Agent:set_mode`.
    "maki.api.mode" => pub(crate) fn create_mode_table(),
    DOCS [define, list, reset]
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn prep(reg: Option<Arc<ModeRegistry>>) -> Lua {
        let lua = Lua::new();
        let reg = reg.unwrap_or_else(|| Arc::new(ModeRegistry::builtin()));
        lua.set_app_data(reg);
        let t = create_mode_table(&lua).unwrap();
        lua.globals().set("mode", t).unwrap();
        lua
    }

    #[test]
    fn define_custom_mode_lands_in_registry() {
        let lua = prep(None);
        smol::block_on(
            lua.load(r#"
                local ok, err = mode.define({ name = "audit", label = "[AUDIT]", system_prompt = "Audit directive", tools = { "read", "grep" } })
                assert(ok, err)
            "#)
            .exec_async(),
        )
        .unwrap();
        let reg = lua.app_data_ref::<Arc<ModeRegistry>>().unwrap();
        let def = reg.get(&ModeId::Custom("audit".into())).unwrap();
        assert_eq!(def.label.as_ref(), "[AUDIT]");
        assert_eq!(def.system_prompt.as_deref(), Some("Audit directive"));
        assert_eq!(
            def.tools.as_deref(),
            Some(&["read".into(), "grep".into()][..])
        );
    }

    #[test]
    fn function_system_prompt_is_evaluated() {
        let lua = prep(None);
        smol::block_on(
            lua.load(r#"
                local ok, err = mode.define({ name = "plan", system_prompt = function(ctx) return "FUNC_" .. tostring(ctx.cwd ~= nil) end })
                assert(ok, err)
            "#)
            .exec_async(),
        )
        .unwrap();
        let reg = lua.app_data_ref::<Arc<ModeRegistry>>().unwrap();
        let def = reg.get(&ModeId::Plan).unwrap();
        let prompt = def.system_prompt.unwrap();
        assert!(prompt.starts_with("FUNC_true"), "got: {prompt}");
    }

    #[test]
    fn define_overrides_builtin_and_list_reports() {
        let lua = prep(None);
        smol::block_on(
            lua.load(
                r#"
                local ok, err = mode.define({ name = "plan", label = "[CUSTOM_PLAN]" })
                assert(ok, err)
                local list = mode.list()
                assert(#list == 2, "expected two modes")
                local named = {}
                for _, m in ipairs(list) do named[m.name] = m end
                assert(named.plan.label == "[CUSTOM_PLAN]")
            "#,
            )
            .exec_async(),
        )
        .unwrap();
    }

    #[test]
    fn reset_restores_builtin_plan() {
        let lua = prep(None);
        smol::block_on(
            lua.load(
                r#"
                mode.define({ name = "plan", system_prompt = "Override" })
                assert(mode.reset("plan"), "reset failed")
                local d = mode.define({ name = "plan", system_prompt = "Again" })
                assert(d, "define failed")
                mode.reset()
            "#,
            )
            .exec_async(),
        )
        .unwrap();
    }

    #[test]
    fn mode_id_parse_roundtrips() {
        assert_eq!(ModeId::parse("build"), ModeId::Build);
        assert_eq!(ModeId::parse("plan"), ModeId::Plan);
        assert_eq!(
            ModeId::parse("audit"),
            ModeId::Custom(Arc::from("audit" as &str))
        );
        assert_eq!(ModeId::parse("audit").key(), "audit");
        assert_eq!(ModeId::parse("audit").to_string(), "audit");
    }

    #[test]
    fn spec_from_table_parses_all_fields() {
        let lua = Lua::new();
        lua.set_app_data(Arc::new(ModeRegistry::builtin()));
        let t = lua
            .load(r#"{ name = "x", label = "[X]", system_prompt = "p", restrict_write_to = "out.md", tools = {"a"} }"#)
            .eval::<Table>()
            .unwrap();
        let spec = spec_from_table(&lua, t).unwrap();
        assert_eq!(spec.name, "x");
        assert_eq!(spec.label.as_deref(), Some("[X]"));
        assert_eq!(spec.system_prompt.as_deref(), Some("p"));
        assert_eq!(
            spec.restrict_write_to
                .as_deref()
                .map(|p| p.to_string_lossy().into_owned()),
            Some("out.md".into())
        );
        assert_eq!(spec.tools.as_deref(), Some(&["a".to_owned()][..]));
    }

    #[test]
    fn define_rejects_empty_name() {
        let lua = prep(None);
        let (val, err): (Option<bool>, Option<String>) =
            smol::block_on(lua.load("return mode.define({ name = '' })").eval_async()).unwrap();
        assert!(val.is_none());
        assert_eq!(err.as_deref(), Some("mode name must be non-empty"));
    }

    #[test]
    fn json_list_shapes_match_sdk_contract() {
        let reg = ModeRegistry::builtin();
        let shapes: Vec<serde_json::Value> = reg
            .list()
            .iter()
            .map(|d| json!({"name": d.id.key(), "label": d.label.as_ref()}))
            .collect();
        assert_eq!(shapes.len(), 2);
        assert_eq!(shapes[0]["name"], "build");
    }
}
