//! `maki.model`: model catalog reads that address no agent. Reading or
//! changing an agent's model goes through its `Agent` handle.

use maki_lua_macro::{lua_fn, lua_table};
use maki_providers::{Model, ThinkingConfig};
use mlua::{Lua, Result as LuaResult, Table};

use crate::api::util::pair::{Pair, err_pair};

/// Describes a model by spec without addressing any agent. Unknown specs
/// return an error.
///
/// @param spec string Model spec, e.g. `"anthropic/claude-sonnet-4-20250514"`.
/// @return (table|nil, string|nil) `{spec, provider, id, tier, supports_thinking,
///   supports_fast, context_window, max_output_tokens, pricing}`, or nil and an
///   error. `max_output_tokens` is nil when unknown. `pricing` is
///   `{input, output, cache_write, cache_read}` in USD per million tokens.
/// @example
/// local info = assert(maki.model.info("anthropic/claude-sonnet-4-20250514"))
#[lua_fn]
fn info(lua: &Lua, spec: String) -> LuaResult<Pair<Table>> {
    let model = match Model::from_spec(&spec) {
        Ok(model) => model,
        Err(error) => return Ok(err_pair(error.to_string())),
    };
    Ok((Some(model_table(lua, &model)?), None))
}

/// The one Lua shape of a model, shared by `maki.model.info` and
/// `maki.agent.resolve_model`.
pub(crate) fn model_table(lua: &Lua, model: &Model) -> LuaResult<Table> {
    let table = lua.create_table()?;
    table.set("spec", model.spec())?;
    table.set("provider", model.provider.to_string())?;
    table.set("id", model.id.clone())?;
    table.set("tier", model.tier.to_string())?;
    table.set("supports_thinking", model.supports_thinking())?;
    table.set("supports_fast", model.supports_fast())?;
    table.set("context_window", model.context_window)?;
    table.set("max_output_tokens", model.max_output_tokens)?;
    let pricing = lua.create_table()?;
    pricing.set("input", model.pricing.input)?;
    pricing.set("output", model.pricing.output)?;
    pricing.set("cache_write", model.pricing.cache_write)?;
    pricing.set("cache_read", model.pricing.cache_read)?;
    table.set("pricing", pricing)?;
    Ok(table)
}

/// Lists the thinking levels a selection accepts, from `off` to `max`. A
/// token budget is also accepted.
///
/// @return table Array of thinking level names.
/// @example
/// local levels = maki.model.thinking_levels()
#[lua_fn]
fn thinking_levels(lua: &Lua) -> LuaResult<Table> {
    lua.create_sequence_from(ThinkingConfig::options().iter().copied())
}

lua_table! {
    /// Model catalog reads that address no agent. `Agent:config`,
    /// `Agent:set_model` and `Agent:available_models` act on an agent.
    "maki.model" => pub(crate) fn create_model_table(),
    DOCS [info, thinking_levels]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::util::convert::lua_to_json;
    use mlua::Value;

    const SPEC: &str = "anthropic/claude-opus-4-6";

    #[test]
    fn info_describes_the_model_registry_entry() {
        let lua = Lua::new();
        lua.globals()
            .set("model", create_model_table(&lua).unwrap())
            .unwrap();
        let model = Model::from_spec(SPEC).unwrap();
        let (info, err): (Value, Option<String>) = smol::block_on(
            lua.load(format!("return model.info({SPEC:?})"))
                .eval_async(),
        )
        .unwrap();
        let info = lua_to_json(&lua, &info).unwrap();
        assert_eq!(err, None);
        assert_eq!(info["supports_fast"], model.supports_fast());
        assert_eq!(info["context_window"], model.context_window);
        assert_eq!(info["pricing"]["input"], model.pricing.input);
        assert_eq!(info["pricing"]["cache_read"], model.pricing.cache_read);
    }
}
