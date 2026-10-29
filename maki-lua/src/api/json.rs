use maki_lua_macro::{lua_fn, lua_table};
use mlua::{
    AnyUserData, Lua, LuaSerdeExt, Result as LuaResult, Table, UserData, UserDataMethods, Value,
};

use super::util::convert::{json_to_lua_with_nulls, lua_to_json};
use super::util::pair::{Pair, pair, try_pair};

pub(crate) const VALIDATOR_DOCS: crate::docs::ModuleDoc = crate::docs::ModuleDoc {
    name: "maki.json.SchemaValidator",
    kind: crate::docs::DocKind::Class,
    desc: "A compiled JSON Schema validator. Create one with \
`maki.json.schema_validator()` and reuse it to validate many values \
without recompiling the schema each time.",
    fns: &[crate::docs::FnDoc {
        name: "validate",
        args: "{value}",
        desc: "Check {value} against the compiled schema. Returns nil \
when the value is valid. When validation fails, returns a list of \
human-readable error strings.",
        params: &[crate::docs::ParamDoc {
            name: "{value}",
            ty: "any",
            desc: "The Lua value to validate.",
        }],
        returns: "(table?) Array of error strings, or nil if valid.",
        guard: None,
        example: "local errs = validator:validate({ name = 123 })\n\
if errs then\n\
  for _, msg in ipairs(errs) do print(msg) end\n\
end",
    }],
};

/// Schema compile errors surface at creation time, before spending any tokens.
struct LuaSchemaValidator {
    validator: jsonschema::Validator,
}

impl UserData for LuaSchemaValidator {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("validate", |lua, this, value: Value| {
            let json = lua_to_json(lua, &value)?;
            let errors: Vec<String> = this
                .validator
                .iter_errors(&json)
                .map(|e| {
                    let path = e.instance_path.to_string();
                    if path.is_empty() {
                        e.to_string()
                    } else {
                        format!("at {path}: {e}")
                    }
                })
                .collect();
            if errors.is_empty() {
                return Ok(Value::Nil);
            }
            let tbl = lua.create_table()?;
            for (i, err) in errors.into_iter().enumerate() {
                tbl.set(i + 1, err)?;
            }
            Ok(Value::Table(tbl))
        });
    }
}

/// Turn a Lua value into a JSON string. Tables, strings, numbers,
/// booleans, nil, and the JSON null sentinel all work. Functions and
/// other userdata cannot be serialized.
///
/// @param value any Lua value to encode.
/// @return (string?, string?) JSON string, or nil plus an error.
/// @example
/// local s, err = maki.json.encode({ name = "maki", version = 1 })
/// print(s) -- {"name":"maki","version":1}
#[lua_fn]
fn encode(lua: &Lua, value: Value) -> LuaResult<Pair<String>> {
    let serde_val: serde_json::Value = try_pair!(lua.from_value(value));
    Ok(pair(serde_json::to_string(&serde_val)))
}

/// Return the JSON null sentinel. It compares equal to preserved nulls and
/// encodes as JSON null. The sentinel is truthy; compare it explicitly
/// rather than using a truthiness check. Assign nil to remove a field.
///
/// @return any JSON null sentinel.
#[lua_fn]
fn null(lua: &Lua) -> LuaResult<Value> {
    Ok(lua.null())
}

/// Parse a JSON string into a Lua value. Objects become tables and
/// arrays become 1-indexed tables. By default null becomes nil, removing
/// object fields and leaving holes in arrays. Set `preserve_null` to
/// represent null as `maki.json.null()` instead.
///
/// @param str string JSON string to decode.
/// @param opts table? Optional settings: `preserve_null` (boolean, default false).
/// @return (any?, string?) Decoded value and nil, or nil plus an error. A root null decodes to nil by default.
/// @example
/// local t, err = maki.json.decode('{"x": 42}')
/// print(t.x) -- 42
#[lua_fn]
fn decode(lua: &Lua, str: String, opts: Option<Table>) -> LuaResult<Pair<Value>> {
    let preserve_null = match opts {
        Some(opts) => opts.get::<Option<bool>>("preserve_null")?.unwrap_or(false),
        None => false,
    };
    let value = try_pair!(serde_json::from_str::<serde_json::Value>(&str));
    Ok((
        Some(json_to_lua_with_nulls(lua, &value, preserve_null)?),
        None,
    ))
}

/// Compile a JSON Schema into a reusable validator object. Supports
/// draft-07, 2019-09, and 2020-12. Schema errors show up right away so
/// you catch mistakes before doing any real work.
///
/// @param schema table JSON Schema as a Lua table.
/// @return (maki.json.SchemaValidator?, string?) Validator, or nil plus an error.
/// @example
/// local v, err = maki.json.schema_validator({
///   type = "object",
///   properties = { name = { type = "string" } },
///   required = { "name" },
/// })
/// local errs = v:validate({ name = "maki" })
/// assert(errs == nil)
#[lua_fn]
fn schema_validator(lua: &Lua, schema: Value) -> LuaResult<Pair<AnyUserData>> {
    let schema_json = try_pair!(lua_to_json(lua, &schema));
    let validator = try_pair!(jsonschema::validator_for(&schema_json));
    Ok((
        Some(lua.create_userdata(LuaSchemaValidator { validator })?),
        None,
    ))
}

lua_table! {
    /// JSON encoding, decoding, and schema validation. Encode Lua
    /// tables to JSON strings, decode JSON back into tables, and
    /// optionally validate data against a JSON Schema.
    ///
    /// ```lua
    /// local s = maki.json.encode({ ok = true })
    /// local t = maki.json.decode(s)
    /// ```
    "maki.json" => pub(crate) fn create_json_table(), DOCS [
        encode, decode, null, schema_validator,
    ]
}

#[cfg(test)]
mod tests {
    use mlua::Lua;

    fn lua_with_json() -> Lua {
        let lua = Lua::new();
        let json = super::create_json_table(&lua).unwrap();
        lua.globals().set("json", json).unwrap();
        lua
    }

    #[test]
    fn encode_table() {
        let lua = lua_with_json();
        let result: String = lua
            .load(r#"local s, err = json.encode({a = 1}); return s"#)
            .eval()
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["a"], 1);
    }

    #[test]
    fn decode_string() {
        let lua = lua_with_json();
        let result: i64 = lua
            .load(r#"local t, err = json.decode('{"x":42}'); return t.x"#)
            .eval()
            .unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn encode_error_returns_nil_and_message() {
        let lua = lua_with_json();
        let (is_nil, has_err): (bool, bool) = lua
            .load(r#"local s, err = json.encode(json.encode); return s == nil, err ~= nil"#)
            .eval()
            .unwrap();
        assert!(is_nil);
        assert!(has_err);
    }

    #[test]
    fn decode_error_returns_nil_and_message() {
        let lua = lua_with_json();
        let (is_nil, has_err): (bool, bool) = lua
            .load(r#"local t, err = json.decode("{invalid}"); return t == nil, err ~= nil"#)
            .eval()
            .unwrap();
        assert!(is_nil);
        assert!(has_err);
    }

    #[test]
    fn roundtrip() {
        let lua = lua_with_json();
        let result: String = lua
            .load(
                r#"
                local t = {name = "test", count = 3}
                local s = json.encode(t)
                local t2 = json.decode(s)
                return t2.name .. ":" .. tostring(t2.count)
                "#,
            )
            .eval()
            .unwrap();
        assert_eq!(result, "test:3");
    }

    #[test]
    fn decode_array() {
        let lua = lua_with_json();
        let result: i64 = lua
            .load(r#"local t = json.decode('[10,20,30]'); return #t"#)
            .eval()
            .unwrap();
        assert_eq!(result, 3);
    }

    #[test]
    fn chat_request_encodes_empty_history_and_fallback_schema() {
        let lua = lua_with_json();
        let empty: String = lua.load("return assert(json.encode({}))").eval().unwrap();
        assert_eq!(empty, "{}");
        let chat: mlua::Table = lua
            .load(include_str!(
                "../../../plugins/lib/maki/chat_completions.lua"
            ))
            .eval()
            .unwrap();
        let maki = lua.create_table().unwrap();
        maki.set("json", lua.globals().get::<mlua::Table>("json").unwrap())
            .unwrap();
        lua.globals().set("maki", maki).unwrap();
        lua.globals().set("chat", chat).unwrap();
        let encoded: String = lua
            .load(
                r#"
                local request = {model = {id = 'model'}, messages = {}, tools = {
                    {name = 'f', description = 'test', input_schema = json.decode('[]')}
                }}
                local body = assert(chat.build_request(request, {max_tokens_field = 'max_tokens'}))
                return assert(json.encode(body))
                "#,
            )
            .eval()
            .unwrap();
        let body: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            body["messages"],
            serde_json::json!([{ "role": "system", "content": "" }])
        );
        assert_eq!(
            body["tools"][0]["function"]["parameters"],
            serde_json::json!({"type": "object", "properties": {}})
        );
    }

    #[test]
    fn encode_decode_empty_array_roundtrips() {
        let lua = lua_with_json();
        let result: String = lua
            .load(r#"local s = json.encode(json.decode('[]')); return s"#)
            .eval()
            .unwrap();
        assert_eq!(result, "[]");
    }

    #[test]
    fn decode_preserves_null_when_requested() {
        let lua = lua_with_json();
        lua.load(
            r#"
            local value, err = json.decode('{"a":null,"nested":[null,{"x":null},null]}', { preserve_null = true })
            local null = json.null()
            assert(err == nil and value.a == null)
            assert(#value.nested == 3 and value.nested[1] == null)
            assert(value.nested[2].x == null and value.nested[3] == null)
            local root, root_err = json.decode('null', { preserve_null = true })
            assert(root_err == nil and root == null)
            assert(json.encode(root) == 'null')
            local encoded = assert(json.encode(value))
            local again = assert(json.decode(encoded, { preserve_null = true }))
            assert(again.a == null and again.nested[3] == null)
            local collision = assert(json.decode('{"__json_null":true}', { preserve_null = true }))
            assert(collision ~= null and collision.__json_null == true)
            assert(json.encode(collision) == '{"__json_null":true}')
            local validator = assert(json.schema_validator({type = 'null'}))
            assert(validator:validate(root) == nil and validator:validate(collision) ~= nil)
            local legacy = json.decode('{"a":null}', { preserve_null = false })
            assert(legacy.a == nil and json.decode('null') == nil)
            local invalid, invalid_err = json.decode('{', { preserve_null = true })
            assert(invalid == nil and invalid_err ~= nil)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn decode_null_roundtrips() {
        let lua = lua_with_json();
        let result: String = lua
            .load(
                r#"
                local t = json.decode('{"a":null,"b":1}')
                local s = json.encode(t)
                local t2 = json.decode(s)
                return tostring(t2.b)
                "#,
            )
            .eval()
            .unwrap();
        assert_eq!(result, "1");
    }
}
