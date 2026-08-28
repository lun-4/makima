//! Fuzzy matching backed by nucleo, the same matcher makima's built-in
//! pickers use. Plugins get consistent type-ahead search without
//! re-implementing subsequence logic.

use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Table, Value as LuaValue};

fn string_arg(val: &LuaValue, what: &str) -> LuaResult<String> {
    match val {
        LuaValue::String(s) => String::from_utf8(s.as_bytes().to_vec())
            .map_err(|_| mlua::Error::runtime(format!("{what}: expected valid UTF-8 string"))),
        _ => Err(mlua::Error::runtime(format!(
            "{what}: expected string, got {}",
            val.type_name()
        ))),
    }
}

/// Fuzzy match {query} against {text} with nucleo, the same matcher makima's
/// built-in pickers use. Every whitespace-separated word in {query} must
/// match somewhere in {text}; word order does not matter. An empty or
/// whitespace-only query matches everything.
///
/// @param query string Search words, whitespace separated.
/// @param text string Text to search in.
/// @return (table|nil) nil when no word matches. On a match: {score = number, indices = {…}} where indices are the 1-based codepoint offsets of the matched characters, ascending.
/// @example
/// local m = maki.match.fuzzy("gh pr", "gh pr 441 review")
/// if m then
///   print(m.score) -- matched codepoint offsets in m.indices
/// end
#[lua_fn]
fn fuzzy(lua: &Lua, query: LuaValue, text: LuaValue) -> LuaResult<Option<Table>> {
    let query = string_arg(&query, "match.fuzzy: query")?;
    let text = string_arg(&text, "match.fuzzy: text")?;
    let Some((score, indices)) = maki_match::fuzzy_match(&query, &text) else {
        return Ok(None);
    };
    let t = lua.create_table()?;
    t.set("score", score)?;
    t.set("indices", indices)?;
    Ok(Some(t))
}

lua_table! {
    /// Fuzzy matching via nucleo, the same matcher makima's built-in pickers use.
    ///
    /// Use it for type-ahead search over a plugin's own item list.
    ///
    /// ```lua
    /// local m = maki.match.fuzzy("gh pr", "gh pr 441 review")
    /// if m then
    ///   print(m.score) -- matched codepoint offsets in m.indices
    /// end
    /// ```
    "maki.match" => pub(crate) fn create_match_table(), DOCS [
        fuzzy,
    ]
}

#[cfg(test)]
mod tests {
    #[test]
    fn non_string_args_error_names_the_arg() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let fuzzy: mlua::Function = t.get("fuzzy").unwrap();
        lua.globals().set("f", &fuzzy).unwrap();
        let msg: String = lua
            .load(
                r#"local ok, err = pcall(f, 123, "hello")
            assert(not ok)
            return tostring(err)"#,
            )
            .eval()
            .unwrap();
        assert!(msg.contains("match.fuzzy: query"), "{msg}");
        let msg: String = lua
            .load(
                r#"local ok, err = pcall(f, "hello", {})
            assert(not ok)
            return tostring(err)"#,
            )
            .eval()
            .unwrap();
        assert!(msg.contains("match.fuzzy: text"), "{msg}");
    }
}
