use makima_storage::session_state::{
    MAX_PLUGIN_VALUE_BYTES, MAX_PLUGIN_VALUE_DEPTH, StateChange, StoredStateRecord,
};
use mlua::{Lua, LuaSerdeExt, Result as LuaResult, Table, Value};
use serde_json::Value as JsonValue;
use std::{collections::HashSet, ffi::c_void};

use crate::api::util::memory::unlimited;

pub(crate) const NIL_TOOL_RESULT_ERR: &str = "tool returned nil without an error message";

/// How many nulls an array encoding may invent for keys the table does not
/// hold. A JSON null arrives as an absent Lua key, so a round trip has to be
/// able to put a few back, but nothing bounds the largest key a table carries.
/// Past this the table is a sparse map, not an array, and the object encoding
/// keeps every key while allocating per entry.
const MAX_ARRAY_HOLES: usize = 4096;

pub(crate) fn state_value_from_lua(lua: &Lua, value: &Value) -> LuaResult<JsonValue> {
    let mut path = HashSet::new();
    let mut remaining = MAX_PLUGIN_VALUE_BYTES;
    let value = state_value_from_lua_inner(
        lua,
        value,
        &lua.array_metatable(),
        &mut path,
        0,
        &mut remaining,
    )?;
    validate_state_value(&value)?;
    Ok(value)
}

fn state_value_from_lua_inner(
    lua: &Lua,
    value: &Value,
    array_metatable: &Table,
    path: &mut HashSet<*const c_void>,
    depth: usize,
    remaining: &mut usize,
) -> LuaResult<JsonValue> {
    if depth > MAX_PLUGIN_VALUE_DEPTH {
        return Err(mlua::Error::runtime(
            "state value exceeds maximum nesting depth",
        ));
    }

    let cost = match value {
        Value::String(value) => value.as_bytes().len().max(1),
        _ => 1,
    };
    *remaining = remaining
        .checked_sub(cost)
        .ok_or_else(|| mlua::Error::runtime("state value exceeds maximum encoded size"))?;
    match value {
        Value::Nil => Ok(JsonValue::Null),
        Value::LightUserData(value) if value.0.is_null() => Ok(JsonValue::Null),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => {
            if (*value as f64) as i128 != i128::from(*value) {
                return Err(mlua::Error::runtime(
                    "state value integer cannot be represented losslessly in Lua",
                ));
            }
            Ok(JsonValue::Number((*value).into()))
        }
        Value::Number(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| mlua::Error::runtime("state value numbers must be finite")),
        Value::String(value) => Ok(JsonValue::String(value.to_str()?.to_owned())),
        Value::Table(table) => {
            let pointer = table.to_pointer();
            if !path.insert(pointer) {
                return Err(mlua::Error::runtime("state value contains a table cycle"));
            }
            let result =
                state_value_table_to_json(lua, table, array_metatable, path, depth, remaining);
            path.remove(&pointer);
            result
        }
        _ => Err(mlua::Error::runtime(format!(
            "state value does not support Lua {} values",
            value.type_name()
        ))),
    }
}

fn state_value_table_to_json(
    lua: &Lua,
    table: &Table,
    array_metatable: &Table,
    path: &mut HashSet<*const c_void>,
    depth: usize,
    remaining: &mut usize,
) -> LuaResult<JsonValue> {
    let tagged_array = table.metatable().as_ref() == Some(array_metatable);
    let mut integer_entries = Vec::new();
    let mut object_entries = Vec::new();
    let mut saw_integer = false;
    let mut saw_string = false;
    let mut max_index = 0usize;

    for pair in table.pairs::<Value, Value>() {
        if integer_entries.len() + object_entries.len() >= MAX_PLUGIN_VALUE_BYTES {
            return Err(mlua::Error::runtime(
                "state value exceeds maximum encoded size",
            ));
        }
        let (key, value) = pair?;
        match key {
            Value::Integer(index) if index > 0 => {
                if saw_string {
                    return Err(mlua::Error::runtime(
                        "state value tables cannot mix array and object keys",
                    ));
                }
                saw_integer = true;
                let index = usize::try_from(index)
                    .map_err(|_| mlua::Error::runtime("state value array index is too large"))?;
                max_index = max_index.max(index);
                integer_entries.push((index, value));
            }
            Value::String(key) => {
                if saw_integer {
                    return Err(mlua::Error::runtime(
                        "state value tables cannot mix array and object keys",
                    ));
                }
                saw_string = true;
                *remaining = remaining.checked_sub(key.as_bytes().len()).ok_or_else(|| {
                    mlua::Error::runtime("state value exceeds maximum encoded size")
                })?;
                object_entries.push((key.to_str()?.to_owned(), value));
            }
            _ => {
                return Err(mlua::Error::runtime(
                    "state value table keys must be strings or positive integers",
                ));
            }
        }
    }

    if tagged_array || saw_integer {
        if saw_string || integer_entries.len() != max_index {
            return Err(mlua::Error::runtime(
                "state value arrays must have dense positive integer keys",
            ));
        }
        let mut items = vec![JsonValue::Null; integer_entries.len()];
        for (index, value) in integer_entries {
            items[index - 1] = state_value_from_lua_inner(
                lua,
                &value,
                array_metatable,
                path,
                depth + 1,
                remaining,
            )?;
        }
        return Ok(JsonValue::Array(items));
    }

    let mut object = serde_json::Map::new();
    for (key, value) in object_entries {
        object.insert(
            key,
            state_value_from_lua_inner(lua, &value, array_metatable, path, depth + 1, remaining)?,
        );
    }
    Ok(JsonValue::Object(object))
}

pub(crate) fn state_value_to_lua(lua: &Lua, value: &JsonValue) -> LuaResult<Value> {
    validate_state_value(value)?;
    let converted = state_value_to_lua_inner(lua, value, 0)?;
    if state_value_from_lua(lua, &converted)? != *value {
        return Err(mlua::Error::runtime(
            "state value cannot be represented losslessly in Lua",
        ));
    }
    Ok(converted)
}

fn state_value_to_lua_inner(lua: &Lua, value: &JsonValue, depth: usize) -> LuaResult<Value> {
    if depth > MAX_PLUGIN_VALUE_DEPTH {
        return Err(mlua::Error::runtime(
            "state value exceeds maximum nesting depth",
        ));
    }

    Ok(match value {
        JsonValue::Null => Value::NULL,
        JsonValue::Bool(value) => Value::Boolean(*value),
        JsonValue::Number(json_number) => {
            if let Some(integer) = json_number.as_i64() {
                if (integer as f64) as i128 != i128::from(integer) {
                    return Err(mlua::Error::runtime(
                        "state value integer cannot be represented losslessly in Lua",
                    ));
                }
                Value::Integer(integer)
            } else {
                let number = json_number.as_f64().ok_or_else(|| {
                    mlua::Error::runtime("state value number cannot be represented in Lua")
                })?;
                let roundtrip = serde_json::Number::from_f64(number).ok_or_else(|| {
                    mlua::Error::runtime("state value number cannot be represented in Lua")
                })?;
                if roundtrip != *json_number {
                    return Err(mlua::Error::runtime(
                        "state value number cannot be represented losslessly in Lua",
                    ));
                }
                Value::Number(number)
            }
        }
        JsonValue::String(value) => Value::String(lua.create_string(value)?),
        JsonValue::Array(items) => {
            let table = lua.create_table_with_capacity(items.len(), 0)?;
            for (index, item) in items.iter().enumerate() {
                table.set(index + 1, state_value_to_lua_inner(lua, item, depth + 1)?)?;
            }
            table.set_metatable(Some(lua.array_metatable()))?;
            Value::Table(table)
        }
        JsonValue::Object(items) => {
            let table = lua.create_table_with_capacity(0, items.len())?;
            for (key, item) in items {
                table.set(
                    key.as_str(),
                    state_value_to_lua_inner(lua, item, depth + 1)?,
                )?;
            }
            Value::Table(table)
        }
    })
}

fn validate_state_value(value: &JsonValue) -> LuaResult<()> {
    let mut pending = vec![(value, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > MAX_PLUGIN_VALUE_DEPTH {
            return Err(mlua::Error::runtime(
                "state value exceeds maximum nesting depth",
            ));
        }
        match value {
            JsonValue::Array(values) => {
                pending.extend(values.iter().map(|value| (value, depth + 1)))
            }
            JsonValue::Object(values) => {
                pending.extend(values.values().map(|value| (value, depth + 1)))
            }
            _ => {}
        }
    }
    if serde_json::to_vec(value)
        .map_err(mlua::Error::external)?
        .len()
        > MAX_PLUGIN_VALUE_BYTES
    {
        return Err(mlua::Error::runtime(
            "state value exceeds maximum encoded size",
        ));
    }
    let record = StoredStateRecord {
        seq: 1,
        agent: None,
        generation: 0,
        change: StateChange::Plugin {
            key: "state/validation".into(),
            version: 1,
            value: Some(value.clone()),
        },
    };
    let encoded = serde_json::to_vec(&record).map_err(mlua::Error::external)?;
    let decoded: StoredStateRecord =
        serde_json::from_slice(&encoded).map_err(mlua::Error::external)?;
    if decoded != record {
        return Err(mlua::Error::runtime(
            "state value storage round trip is lossy",
        ));
    }
    Ok(())
}

/// mlua reads `bool` by Lua truthiness and never fails, so `get::<bool>`
/// answers `Ok(false)` for a missing key and quietly kills whatever
/// `unwrap_or(true)` sat behind it. `Option<bool>` keeps absent apart from
/// an explicit `false`.
pub(crate) fn opt_bool(tbl: &Table, key: &str) -> Option<bool> {
    tbl.get::<Option<bool>>(key).ok().flatten()
}

pub(crate) fn bytes_arg(val: &Value, what: &str) -> LuaResult<Vec<u8>> {
    match val {
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        Value::Buffer(b) => Ok(b.to_vec()),
        _ => Err(mlua::Error::runtime(format!(
            "{what}: expected string or buffer, got {}",
            val.type_name()
        ))),
    }
}

pub(crate) fn lua_tool_result(values: mlua::MultiValue) -> Result<String, String> {
    let mut iter = values.into_iter();
    match iter.next() {
        Some(Value::String(s)) => Ok(s.to_string_lossy()),
        Some(Value::Nil) | None => match iter.next() {
            Some(Value::String(err)) => Err(err.to_string_lossy()),
            _ => Err(NIL_TOOL_RESULT_ERR.into()),
        },
        Some(other) => Err(format!(
            "tool returned {} (expected string)",
            other.type_name()
        )),
    }
}

/// Convert a [`serde_json::Value`] into a Lua value by hand.
///
/// mlua's `to_value` looks like the easy path, but monty turns on serde_json's
/// `arbitrary_precision` feature for the whole workspace. With it, a number
/// serializes as a little tagged struct instead of a plain scalar, so plugins
/// end up with a Lua table where they asked for a number. We walk the tree
/// ourselves to keep numbers as numbers.
pub(crate) fn json_to_lua(lua: &Lua, value: &JsonValue) -> LuaResult<Value> {
    unlimited(lua, || build_lua(lua, value))
}

fn build_lua(lua: &Lua, value: &JsonValue) -> LuaResult<Value> {
    Ok(match value {
        JsonValue::Null => Value::Nil,
        JsonValue::Bool(b) => Value::Boolean(*b),
        JsonValue::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => Value::Integer(i),
            (_, Some(f)) => Value::Number(f),
            _ => Value::Nil,
        },
        JsonValue::String(s) => Value::String(lua.create_string(s)?),
        JsonValue::Array(items) => {
            let table = lua.create_table_with_capacity(items.len(), 0)?;
            for (idx, item) in items.iter().enumerate() {
                table.set(idx + 1, build_lua(lua, item)?)?;
            }
            table.set_metatable(Some(lua.array_metatable()))?;
            Value::Table(table)
        }
        JsonValue::Object(map) => {
            let table = lua.create_table_with_capacity(0, map.len())?;
            for (key, val) in map {
                table.set(key.as_str(), build_lua(lua, val)?)?;
            }
            Value::Table(table)
        }
    })
}

/// Symmetric counterpart to [`json_to_lua`]. We avoid mlua's `from_value`
/// for the same `arbitrary_precision` reason documented above.
pub(crate) fn lua_to_json(lua: &Lua, val: &Value) -> LuaResult<JsonValue> {
    within_template(lua, val, None)
}

/// [`lua_to_json`], guided by the JSON that [`json_to_lua`] built `val` from:
/// whatever the Lua side produced wins, and a null the Lua side left absent is
/// restored from `template`.
///
/// A JSON null arrives as a Lua nil, and assigning nil to a table key is a
/// no-op, so a layer never sees a null and cannot hand one back. They have to
/// be carried across instead. Guiding stops wherever the two sides stop being
/// the same container kind, so a layer that swapped a subtree for a scalar owns
/// it outright. The price: a layer cannot delete a key whose value is null, it
/// comes back, while deleting a key holding a real value works.
pub(crate) fn lua_to_json_within(
    lua: &Lua,
    val: &Value,
    template: &JsonValue,
) -> LuaResult<JsonValue> {
    within_template(lua, val, Some(template))
}

fn within_template(lua: &Lua, val: &Value, template: Option<&JsonValue>) -> LuaResult<JsonValue> {
    Ok(match val {
        Value::Nil => JsonValue::Null,
        Value::Boolean(b) => JsonValue::Bool(*b),
        Value::Integer(n) => JsonValue::Number((*n).into()),
        Value::Number(n) => serde_json::Number::from_f64(*n)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::String(s) => JsonValue::String(s.to_str()?.to_owned()),
        Value::Table(tbl) => {
            // An untagged table serializes as a JSON array only when every key
            // is a positive integer and they are dense from 1 (count == max),
            // so no string key silently disappears and sparse tables like
            // `{ [1] = "a", [3] = "c" }` deterministically become objects
            // (`lua_rawlen` borders are implementation-defined for those).
            // The array metatable outranks that, since `json_to_lua` writes it
            // on every JSON array and a null element leaves an absent key:
            // density cannot be re-derived, and the gaps are holes to fill with
            // null. Keys an array cannot express still fall back to the object
            // encoding, which keeps all of them.
            let mut has_non_int = false;
            let mut int_count = 0;
            let mut max_int = 0;
            let mut entries = Vec::new();
            for pair in tbl.pairs::<Value, Value>() {
                let (k, v) = pair?;
                match k {
                    Value::Integer(i) if i > 0 => {
                        int_count += 1;
                        max_int = max_int.max(i as usize);
                    }
                    _ => has_non_int = true,
                }
                entries.push((k, v));
            }

            let tagged = tbl.metatable().as_ref() == Some(&lua.array_metatable());
            // Slots the entries do not pay for. The array encoding has to
            // materialize every one of them, and the largest key alone decides
            // how many, so this is the only thing standing between
            // `decoded[os.time()] = 1` and an allocation the size of a clock.
            let holes = max_int - int_count;
            let is_array = !has_non_int
                && if tagged {
                    holes <= MAX_ARRAY_HOLES
                } else {
                    int_count > 0 && holes == 0
                };
            if is_array {
                let template = template.and_then(JsonValue::as_array);
                // A trailing null never moved `max_int`, so only the template
                // knows the array ran on past the last key the Lua side kept.
                // A trailing non-null there was a real value the layer dropped.
                let mut len = max_int;
                while template.is_some_and(|t| t.get(len).is_some_and(JsonValue::is_null)) {
                    len += 1;
                }
                let mut arr = vec![JsonValue::Null; len];
                for (k, v) in entries {
                    let Value::Integer(i) = k else { unreachable!() };
                    let idx = i as usize - 1;
                    arr[idx] = within_template(lua, &v, template.and_then(|t| t.get(idx)))?;
                }
                return Ok(JsonValue::Array(arr));
            }

            let template = template.and_then(JsonValue::as_object);
            let mut map = serde_json::Map::new();
            for (k, v) in entries {
                let key = match k {
                    Value::String(s) => s.to_str()?.to_owned(),
                    Value::Integer(i) => i.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::Boolean(b) => b.to_string(),
                    _ => continue,
                };
                let child = template.and_then(|t| t.get(&key));
                map.insert(key, within_template(lua, &v, child)?);
            }
            for (key, _) in template.into_iter().flatten().filter(|(_, v)| v.is_null()) {
                map.entry(key.as_str()).or_insert(JsonValue::Null);
            }
            JsonValue::Object(map)
        }
        _ => JsonValue::Null,
    })
}

#[cfg(test)]
mod tests {
    use mlua::{Lua, LuaSerdeExt, Value};
    use serde_json::Value as JsonValue;
    use test_case::test_case;

    use super::{json_to_lua, lua_to_json, opt_bool, state_value_from_lua, state_value_to_lua};
    use makima_storage::session_state::{MAX_PLUGIN_VALUE_BYTES, MAX_PLUGIN_VALUE_DEPTH};
    use mlua::UserData;
    use std::ffi::c_void;

    const FLAG_KEY: &str = "flag";

    #[test_case(None ; "missing_key_is_none")]
    #[test_case(Some(true) ; "explicit_true")]
    #[test_case(Some(false) ; "explicit_false_is_not_absent")]
    fn opt_bool_distinguishes_absent_from_false(value: Option<bool>) {
        let lua = Lua::new();
        let tbl = lua.create_table().unwrap();
        if let Some(value) = value {
            tbl.raw_set(FLAG_KEY, value).unwrap();
        }
        assert_eq!(opt_bool(&tbl, FLAG_KEY), value);
    }

    #[test_case(Value::Nil, JsonValue::Null ; "nil_to_null")]
    #[test_case(Value::Boolean(true), JsonValue::Bool(true) ; "bool_true")]
    #[test_case(Value::Boolean(false), JsonValue::Bool(false) ; "bool_false")]
    #[test_case(Value::Integer(42), serde_json::json!(42) ; "integer")]
    #[test_case(Value::Number(1.5), serde_json::json!(1.5) ; "float")]
    fn lua_to_json_scalars(input: Value, expected: JsonValue) {
        let lua = Lua::new();
        let result = lua_to_json(&lua, &input).unwrap();
        assert_eq!(result, expected);
    }

    #[test_case(f64::NAN ; "nan")]
    #[test_case(f64::INFINITY ; "positive_infinity")]
    #[test_case(f64::NEG_INFINITY ; "negative_infinity")]
    fn lua_to_json_non_finite_floats_become_null(n: f64) {
        let lua = Lua::new();
        let result = lua_to_json(&lua, &Value::Number(n)).unwrap();
        assert_eq!(result, JsonValue::Null);
    }

    #[test_case(i64::MAX ; "i64_max")]
    #[test_case(i64::MIN ; "i64_min")]
    #[test_case(0 ; "zero")]
    fn lua_to_json_integer_boundaries(n: i64) {
        let lua = Lua::new();
        let result = lua_to_json(&lua, &Value::Integer(n)).unwrap();
        assert_eq!(result, serde_json::json!(n));
    }

    #[test]
    fn lua_to_json_string() {
        let lua = Lua::new();
        let s = lua.create_string("hello").unwrap();
        let result = lua_to_json(&lua, &Value::String(s)).unwrap();
        assert_eq!(result, serde_json::json!("hello"));
    }

    #[test]
    fn lua_to_json_array_table() {
        let lua = Lua::new();
        let tbl = lua.create_table().unwrap();
        tbl.raw_set(1, 10).unwrap();
        tbl.raw_set(2, 20).unwrap();
        tbl.raw_set(3, 30).unwrap();

        let result = lua_to_json(&lua, &Value::Table(tbl)).unwrap();
        assert_eq!(result, serde_json::json!([10, 20, 30]));
    }

    #[test]
    fn lua_to_json_object_table() {
        let lua = Lua::new();
        let tbl = lua.create_table().unwrap();
        tbl.set("key", "value").unwrap();

        let result = lua_to_json(&lua, &Value::Table(tbl)).unwrap();
        assert_eq!(result, serde_json::json!({"key": "value"}));
    }

    #[test]
    fn lua_to_json_empty_table_is_empty_object() {
        let lua = Lua::new();
        let tbl = lua.create_table().unwrap();

        let result = lua_to_json(&lua, &Value::Table(tbl)).unwrap();
        assert_eq!(result, serde_json::json!({}));
    }

    #[test]
    fn lua_to_json_nested_table() {
        let lua = Lua::new();

        let inner_obj = lua.create_table().unwrap();
        inner_obj.set("z", true).unwrap();

        let inner_arr = lua.create_table().unwrap();
        inner_arr.raw_set(1, 1).unwrap();
        inner_arr.raw_set(2, inner_obj).unwrap();

        let outer = lua.create_table().unwrap();
        outer.set("items", inner_arr).unwrap();

        let result = lua_to_json(&lua, &Value::Table(outer)).unwrap();
        assert_eq!(result, serde_json::json!({"items": [1, {"z": true}]}));
    }

    #[test]
    fn lua_to_json_sparse_table_becomes_object() {
        let lua = Lua::new();
        let tbl = lua.create_table().unwrap();
        tbl.raw_set(1, "a").unwrap();
        tbl.raw_set(3, "c").unwrap();

        let result = lua_to_json(&lua, &Value::Table(tbl)).unwrap();
        assert_eq!(result, serde_json::json!({"1": "a", "3": "c"}));
    }

    #[test]
    fn lua_to_json_array_metatable_with_string_key_becomes_object() {
        let lua = Lua::new();
        let arr = json_to_lua(&lua, &serde_json::json!([10, 20])).unwrap();
        let tbl = arr.as_table().unwrap();
        tbl.set("total", 5).unwrap();

        let result = lua_to_json(&lua, &arr).unwrap();
        assert_eq!(result, serde_json::json!({"1": 10, "2": 20, "total": 5}));
    }

    #[test]
    fn lua_to_json_mixed_table_becomes_object() {
        let lua = Lua::new();
        let tbl = lua.create_table().unwrap();
        tbl.raw_set(1, "first").unwrap();
        tbl.set("pattern", "grep").unwrap();

        let result = lua_to_json(&lua, &Value::Table(tbl)).unwrap();
        assert_eq!(result, serde_json::json!({"1": "first", "pattern": "grep"}));
    }

    #[test]
    fn lua_to_json_function_becomes_null() {
        let lua = Lua::new();
        let func = lua.create_function(|_, ()| Ok(())).unwrap();
        let result = lua_to_json(&lua, &Value::Function(func)).unwrap();
        assert_eq!(result, JsonValue::Null);
    }

    #[test]
    fn lua_to_json_thread_becomes_null() {
        let lua = Lua::new();
        let thread = lua
            .create_thread(lua.create_function(|_, ()| Ok(())).unwrap())
            .unwrap();
        let result = lua_to_json(&lua, &Value::Thread(thread)).unwrap();
        assert_eq!(result, JsonValue::Null);
    }

    const ROUNDTRIP_CASES: &[&str] = &[
        "null",
        "true",
        "42",
        "3.14",
        r#""hello""#,
        "[1,2,3]",
        "[]",
        r#"{}"#,
        r#"{"a":1,"b":[true,"x"]}"#,
    ];

    #[test_case(0 ; "null")]
    #[test_case(1 ; "bool")]
    #[test_case(2 ; "integer")]
    #[test_case(3 ; "float")]
    #[test_case(4 ; "string")]
    #[test_case(5 ; "array")]
    #[test_case(6 ; "empty_array")]
    #[test_case(7 ; "empty_object")]
    #[test_case(8 ; "nested_object")]
    fn lua_to_json_roundtrip(idx: usize) {
        let original: JsonValue = serde_json::from_str(ROUNDTRIP_CASES[idx]).unwrap();
        let lua = Lua::new();
        let lua_val = json_to_lua(&lua, &original).unwrap();
        let back = lua_to_json(&lua, &lua_val).unwrap();
        assert_eq!(back, original);
    }
    const STATE_CASES: &[&str] = &[
        "null",
        "true",
        "42",
        "3.14",
        r#""hello""#,
        "[]",
        "[null,1,null]",
        "{}",
        r#"{"a":null,"b":[true,"x"]}"#,
    ];

    #[test_case(0 ; "null")]
    #[test_case(1 ; "bool")]
    #[test_case(2 ; "integer")]
    #[test_case(3 ; "float")]
    #[test_case(4 ; "string")]
    #[test_case(5 ; "empty_array")]
    #[test_case(6 ; "array_with_nulls")]
    #[test_case(7 ; "empty_object")]
    #[test_case(8 ; "nested_object_with_nulls")]
    fn state_value_roundtrip(idx: usize) {
        let original: JsonValue = serde_json::from_str(STATE_CASES[idx]).unwrap();
        let lua = Lua::new();
        let lua_value = state_value_to_lua(&lua, &original).unwrap();
        let back = state_value_from_lua(&lua, &lua_value).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn state_value_rejects_function_and_thread() {
        let lua = Lua::new();
        let function = lua.create_function(|_, ()| Ok(())).unwrap();
        let thread = lua.create_thread(function.clone()).unwrap();
        assert!(state_value_from_lua(&lua, &Value::Function(function)).is_err());
        assert!(state_value_from_lua(&lua, &Value::Thread(thread)).is_err());
    }

    struct TestUserData;

    impl UserData for TestUserData {}

    #[test]
    fn state_value_rejects_userdata_and_non_null_lightuserdata() {
        let lua = Lua::new();
        let userdata = Value::UserData(lua.create_userdata(TestUserData).unwrap());
        let mut byte = 0;
        let pointer = std::ptr::addr_of_mut!(byte).cast::<c_void>();
        let light_userdata = Value::LightUserData(mlua::LightUserData(pointer));
        assert!(state_value_from_lua(&lua, &userdata).is_err());
        assert!(state_value_from_lua(&lua, &light_userdata).is_err());
    }

    #[test_case(f64::NAN ; "nan")]
    #[test_case(f64::INFINITY ; "positive_infinity")]
    #[test_case(f64::NEG_INFINITY ; "negative_infinity")]
    fn state_value_rejects_non_finite_numbers(number: f64) {
        let lua = Lua::new();
        assert!(state_value_from_lua(&lua, &Value::Number(number)).is_err());
    }

    #[test]
    fn state_value_accepts_explicit_null_and_preserves_shared_tables() {
        let lua = Lua::new();
        let shared = lua.create_table().unwrap();
        shared.set("value", Value::NULL).unwrap();
        let root = lua.create_table().unwrap();
        root.set("left", shared.clone()).unwrap();
        root.set("right", shared).unwrap();
        assert_eq!(
            state_value_from_lua(&lua, &Value::Table(root)).unwrap(),
            serde_json::json!({"left": {"value": null}, "right": {"value": null}})
        );
    }

    #[test]
    fn state_value_rejects_cycles_without_mutating_table() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        table.set("self", table.clone()).unwrap();
        assert!(state_value_from_lua(&lua, &Value::Table(table.clone())).is_err());
        assert_eq!(table.get::<Value>("self").unwrap().as_table(), Some(&table));
    }

    #[test]
    fn state_value_rejects_mixed_and_invalid_object_keys_without_mutation() {
        let lua = Lua::new();
        let mixed = lua.create_table().unwrap();
        mixed.set(1, "first").unwrap();
        mixed.set("second", "value").unwrap();
        assert!(state_value_from_lua(&lua, &Value::Table(mixed.clone())).is_err());
        assert_eq!(mixed.get::<String>("second").unwrap(), "value");

        let invalid = lua.create_table().unwrap();
        invalid.set(false, "value").unwrap();
        assert!(state_value_from_lua(&lua, &Value::Table(invalid)).is_err());
    }

    #[test]
    fn state_value_rejects_sparse_and_malformed_tagged_arrays() {
        let lua = Lua::new();
        let sparse = lua.create_table().unwrap();
        sparse.set(1, "first").unwrap();
        sparse.set(3, "third").unwrap();
        assert!(state_value_from_lua(&lua, &Value::Table(sparse)).is_err());

        let tagged = lua.create_table().unwrap();
        tagged.set(1, "first").unwrap();
        tagged.set("extra", "value").unwrap();
        tagged.set_metatable(Some(lua.array_metatable())).unwrap();
        assert!(state_value_from_lua(&lua, &Value::Table(tagged)).is_err());
    }

    #[test]
    fn state_value_at_depth_limit_fits_stored_session_record() {
        let lua = Lua::new();
        let mut value = Value::Boolean(false);
        for _ in 0..MAX_PLUGIN_VALUE_DEPTH {
            let table = lua.create_table().unwrap();
            table.set("nested", value).unwrap();
            value = Value::Table(table);
        }
        let state = state_value_from_lua(&lua, &value).unwrap();
        assert_eq!(
            state_value_to_lua(&lua, &state)
                .and_then(|copy| state_value_from_lua(&lua, &copy))
                .unwrap(),
            state
        );
    }

    #[test]
    fn state_value_rejects_excessive_depth() {
        let lua = Lua::new();
        let mut value = Value::Boolean(false);
        for _ in 0..=MAX_PLUGIN_VALUE_DEPTH {
            let table = lua.create_table().unwrap();
            table.set("nested", value).unwrap();
            value = Value::Table(table);
        }
        assert!(state_value_from_lua(&lua, &value).is_err());
    }
    #[test_case("9007199254740993" ; "integer_rounding")]
    #[test_case("18446744073709551615" ; "unsigned_rounding")]
    fn state_value_rejects_lossy_stored_numbers(encoded: &str) {
        let lua = Lua::new();
        let value: JsonValue = serde_json::from_str(encoded).unwrap();
        assert!(state_value_to_lua(&lua, &value).is_err());
    }

    #[test]
    fn state_value_preserves_storage_decoded_decimal() {
        let lua = Lua::new();
        let stored: JsonValue = serde_json::from_str("1.00000000000000000001").unwrap();
        assert_eq!(stored, serde_json::json!(1.0));
        let converted = state_value_to_lua(&lua, &stored).unwrap();
        assert_eq!(state_value_from_lua(&lua, &converted).unwrap(), stored);
    }

    #[test]
    fn storage_decoder_rejects_unrepresentable_exponent_before_state_conversion() {
        let error = serde_json::from_str::<JsonValue>("1e999").unwrap_err();
        assert!(error.to_string().starts_with("number out of range"));
    }

    #[test_case(f64::MIN_POSITIVE ; "smallest_normal")]
    #[test_case(f64::MAX ; "largest_finite")]
    #[test_case(f64::from_bits(1) ; "smallest_subnormal")]
    #[test_case(-0.0 ; "negative_zero")]
    #[test_case(1.0000000000000002 ; "next_float_after_one")]
    fn state_value_preserves_stored_float_bits(number: f64) {
        let lua = Lua::new();
        let stored = JsonValue::Number(serde_json::Number::from_f64(number).unwrap());
        let converted = state_value_to_lua(&lua, &stored).unwrap();
        let restored = state_value_from_lua(&lua, &converted).unwrap();
        assert_eq!(restored.as_f64().unwrap().to_bits(), number.to_bits());
    }

    #[test_case(MAX_PLUGIN_VALUE_BYTES - 2, true ; "size_limit")]
    #[test_case(MAX_PLUGIN_VALUE_BYTES - 1, false ; "over_size_limit")]
    fn state_value_checks_encoded_size_both_ways(bytes: usize, accepted: bool) {
        let lua = Lua::new();
        let text = "x".repeat(bytes);
        let value = Value::String(lua.create_string(&text).unwrap());
        assert_eq!(state_value_from_lua(&lua, &value).is_ok(), accepted);
        assert_eq!(
            state_value_to_lua(&lua, &JsonValue::String(text)).is_ok(),
            accepted
        );
    }

    #[test]
    fn state_value_rejects_non_utf8_strings_and_keys() {
        let lua = Lua::new();
        let invalid = lua.create_string([0xff]).unwrap();
        assert!(state_value_from_lua(&lua, &Value::String(invalid.clone())).is_err());
        let table = lua.create_table().unwrap();
        table.set(invalid, true).unwrap();
        assert!(state_value_from_lua(&lua, &Value::Table(table)).is_err());
    }
}
