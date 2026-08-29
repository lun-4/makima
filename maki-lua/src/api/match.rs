//! Fuzzy matching backed by nucleo, the same matcher makima's built-in
//! pickers use. Plugins get consistent type-ahead search without
//! re-implementing subsequence logic.

use maki_lua_macro::{lua_fn, lua_table};
use maki_match::completion_match_default;
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

/// Match {query} against a completion {label}, returning one-based Lua
/// indices and the ranking metadata used by built-in completion.
///
/// @param query string Search pattern.
/// @param label string Completion label.
/// @return (table|nil) A match with one-based codepoint `indices` and a
/// `ranking` table, or nil when the label does not match.
#[lua_fn]
fn completion(lua: &Lua, query: LuaValue, label: LuaValue) -> LuaResult<Option<Table>> {
    let query = string_arg(&query, "match.completion: query")?;
    let label = string_arg(&label, "match.completion: label")?;
    let Some(matched) = completion_match_default(&query, &label) else {
        return Ok(None);
    };
    let result = lua.create_table()?;
    let indices: Vec<u32> = matched.indices.into_iter().map(|index| index + 1).collect();
    result.set("indices", indices)?;
    let ranking = lua.create_table()?;
    ranking.set("quality_rank", matched.ranking.quality_rank)?;
    ranking.set("boundary_rank", matched.ranking.boundary_rank)?;
    ranking.set("start_index", matched.ranking.start_index + 1)?;
    ranking.set("gap_count", matched.ranking.gap_count)?;
    ranking.set("span_length", matched.ranking.span_length)?;
    ranking.set("unmatched_suffix", matched.ranking.unmatched_suffix)?;
    ranking.set("fuzzy_score", matched.ranking.fuzzy_score)?;
    result.set("ranking", ranking)?;
    Ok(Some(result))
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
    "maki.match" => pub(crate) fn create_match_table(),     DOCS [
        fuzzy,
        completion,
    ]

}

#[cfg(test)]
mod tests {
    #[test]
    fn completion_returns_one_based_indices_and_ranking() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let completion: mlua::Function = t.get("completion").unwrap();
        lua.globals().set("completion", completion).unwrap();
        lua.load(
            r#"
            local m = completion("ap", "apple")
            assert(m)
            assert(m.indices[1] == 1 and m.indices[2] == 2)
            assert(m.ranking.quality_rank == 1)
            assert(m.ranking.start_index == 1)
            assert(m.ranking.fuzzy_score ~= nil)
            assert(completion("zzz", "apple") == nil)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn completion_non_string_args_error_names_the_arg() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let completion: mlua::Function = t.get("completion").unwrap();
        lua.globals().set("completion", &completion).unwrap();
        let msg: String = lua
            .load(r#"local ok, err = pcall(completion, 123, "hello") return tostring(err)"#)
            .eval()
            .unwrap();
        assert!(msg.contains("match.completion: query"), "{msg}");
    }

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
