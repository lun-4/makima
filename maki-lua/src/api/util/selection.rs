//! Lua input for the shared model selection: every API that takes a model
//! accepts a spec string or a `{spec, thinking, fast}` table and validates it
//! the same way `/model` does.

use maki_providers::ModelSelection;
use mlua::{Error, Lua, LuaSerdeExt, Value};

pub(crate) const SELECTION_ARG_ERR: &str = "model selection must be a spec string or a table";

pub(crate) fn selection_from_lua(lua: &Lua, value: Value) -> Result<ModelSelection, String> {
    let table = match value {
        Value::String(spec) => {
            let table = lua.create_table().map_err(|error| error.to_string())?;
            table
                .set(maki_providers::MODEL_SPEC_KEY, spec)
                .map_err(|error| error.to_string())?;
            table
        }
        Value::Table(table) => table,
        _ => return Err(SELECTION_ARG_ERR.into()),
    };
    lua.from_value(Value::Table(table))
        .map_err(|error| match error {
            Error::DeserializeError(message) => message,
            other => other.to_string(),
        })
}
