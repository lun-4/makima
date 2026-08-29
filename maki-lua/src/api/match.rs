//! Fuzzy matching backed by nucleo, the same matcher makima's built-in
//! pickers use. Plugins get consistent type-ahead search without
//! re-implementing subsequence logic.

use std::cmp::Ordering;

use maki_lua_macro::{lua_fn, lua_table};
use maki_match::{
    CompletionMatchOptions, CompletionRanking, compare_completion_rankings, completion_match,
};
use mlua::{Lua, Result as LuaResult, Table, Value as LuaValue};
use nucleo_matcher::pattern::{CaseMatching, Normalization};

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

/// Compatibility/raw-score fuzzy matching for {query} against {text} with
/// nucleo, the same matcher makima's built-in pickers use. Every
/// whitespace-separated word in {query} must match somewhere in {text}; word
/// order does not matter. An empty or whitespace-only query matches everything.
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

fn parse_case(value: LuaValue) -> LuaResult<CaseMatching> {
    match value {
        LuaValue::Nil => Ok(CaseMatching::Smart),
        LuaValue::String(value) => match value
            .to_str()
            .map_err(|_| {
                mlua::Error::runtime("match.completion: opts.case: expected valid UTF-8 string")
            })?
            .as_ref()
        {
            "smart" => Ok(CaseMatching::Smart),
            "ignore" => Ok(CaseMatching::Ignore),
            "respect" => Ok(CaseMatching::Respect),
            other => Err(mlua::Error::runtime(format!(
                "match.completion: opts.case: unsupported value {other:?} (expected smart, ignore, or respect)"
            ))),
        },
        other => Err(mlua::Error::runtime(format!(
            "match.completion: opts.case: expected string or nil, got {} ({other:?})",
            other.type_name()
        ))),
    }
}

fn parse_normalization(value: LuaValue) -> LuaResult<Normalization> {
    match value {
        LuaValue::Nil => Ok(Normalization::Smart),
        LuaValue::String(value) => match value
            .to_str()
            .map_err(|_| {
                mlua::Error::runtime(
                    "match.completion: opts.normalization: expected valid UTF-8 string",
                )
            })?
            .as_ref()
        {
            "smart" => Ok(Normalization::Smart),
            "never" => Ok(Normalization::Never),
            other => Err(mlua::Error::runtime(format!(
                "match.completion: opts.normalization: unsupported value {other:?} (expected smart or never)"
            ))),
        },
        other => Err(mlua::Error::runtime(format!(
            "match.completion: opts.normalization: expected string or nil, got {} ({other:?})",
            other.type_name()
        ))),
    }
}

fn parse_completion_options(value: Option<LuaValue>) -> LuaResult<CompletionMatchOptions> {
    let Some(value) = value else {
        return Ok(CompletionMatchOptions::default());
    };
    let LuaValue::Table(opts) = value else {
        return Err(mlua::Error::runtime(format!(
            "match.completion: opts: expected table or nil, got {} ({value:?})",
            value.type_name()
        )));
    };
    Ok(CompletionMatchOptions {
        case_matching: parse_case(opts.get("case")?)?,
        normalization: parse_normalization(opts.get("normalization")?)?,
    })
}

fn set_ranking(lua: &Lua, result: &Table, ranking: CompletionRanking) -> LuaResult<()> {
    let ranking_table = lua.create_table()?;
    ranking_table.set("quality_rank", ranking.quality_rank)?;
    ranking_table.set("boundary_rank", ranking.boundary_rank)?;
    ranking_table.set("start_index", ranking.start_index + 1)?;
    ranking_table.set("gap_count", ranking.gap_count)?;
    ranking_table.set("span_length", ranking.span_length)?;
    ranking_table.set("unmatched_suffix", ranking.unmatched_suffix)?;
    ranking_table.set("fuzzy_score", ranking.fuzzy_score)?;
    result.set("ranking", ranking_table)
}

/// Match {query} against a completion {label}, returning one-based Lua
/// indices and the textual ranking metadata used by built-in completion.
///
/// The optional {opts} table accepts `case = "smart" | "ignore" | "respect"`
/// and `normalization = "smart" | "never"`; omitted or nil fields use the
/// defaults. Indices and `ranking.start_index` are one-based Unicode codepoint
/// positions. The other ranking fields are non-negative integers. The result
/// shape is `{ indices, ranking = { quality_rank, boundary_rank, start_index,
/// gap_count, span_length, unmatched_suffix, fuzzy_score } }`.
///
/// @param query string Search pattern.
/// @param label string Completion label.
/// @param opts table? Optional matching options.
/// @return (table|nil) A match with one-based codepoint `indices` and a
/// `ranking` table, or nil when the label does not match.
#[lua_fn]
fn completion(
    lua: &Lua,
    query: LuaValue,
    label: LuaValue,
    opts: Option<LuaValue>,
) -> LuaResult<Option<Table>> {
    let query = string_arg(&query, "match.completion: query")?;
    let label = string_arg(&label, "match.completion: label")?;
    let options = parse_completion_options(opts)?;
    let Some(matched) = completion_match(&query, &label, options) else {
        return Ok(None);
    };
    let result = lua.create_table()?;
    let indices: Vec<u32> = matched.indices.into_iter().map(|index| index + 1).collect();
    result.set("indices", indices)?;
    set_ranking(lua, &result, matched.ranking)?;
    Ok(Some(result))
}

fn nonnegative_integer(table: &Table, field: &str, side: &str) -> LuaResult<u64> {
    let value: LuaValue = table.get(field)?;
    match value {
        LuaValue::Integer(value) if value >= 0 => Ok(value as u64),
        other => Err(mlua::Error::runtime(format!(
            "match.compare: {side}.ranking.{field}: expected a non-negative integer, got {} ({other:?})",
            other.type_name()
        ))),
    }
}

fn ranking_from_table(table: &Table, side: &str) -> LuaResult<CompletionRanking> {
    let ranking: LuaValue = table.get("ranking")?;
    let LuaValue::Table(ranking) = ranking else {
        return Err(mlua::Error::runtime(format!(
            "match.compare: {side}.ranking: expected table, got {} ({ranking:?})",
            ranking.type_name()
        )));
    };
    let quality_rank = u8::try_from(nonnegative_integer(&ranking, "quality_rank", side)?)
        .ok()
        .filter(|value| *value <= 4)
        .ok_or_else(|| {
            mlua::Error::runtime(format!(
                "match.compare: {side}.ranking.quality_rank: expected an integer from 0 through 4"
            ))
        })?;
    let boundary_rank = u8::try_from(nonnegative_integer(&ranking, "boundary_rank", side)?)
        .ok()
        .filter(|value| *value <= 1)
        .ok_or_else(|| {
            mlua::Error::runtime(format!(
                "match.compare: {side}.ranking.boundary_rank: expected an integer from 0 through 1"
            ))
        })?;
    let start_index = nonnegative_integer(&ranking, "start_index", side)?;
    if start_index == 0 {
        return Err(mlua::Error::runtime(format!(
            "match.compare: {side}.ranking.start_index: expected a one-based positive integer"
        )));
    }
    let start_index = usize::try_from(start_index - 1).map_err(|_| {
        mlua::Error::runtime(format!(
            "match.compare: {side}.ranking.start_index: value is out of range"
        ))
    })?;
    let gap_count =
        usize::try_from(nonnegative_integer(&ranking, "gap_count", side)?).map_err(|_| {
            mlua::Error::runtime(format!(
                "match.compare: {side}.ranking.gap_count: value is out of range"
            ))
        })?;
    let span_length = usize::try_from(nonnegative_integer(&ranking, "span_length", side)?)
        .map_err(|_| {
            mlua::Error::runtime(format!(
                "match.compare: {side}.ranking.span_length: value is out of range"
            ))
        })?;
    let unmatched_suffix =
        usize::try_from(nonnegative_integer(&ranking, "unmatched_suffix", side)?).map_err(
            |_| {
                mlua::Error::runtime(format!(
                    "match.compare: {side}.ranking.unmatched_suffix: value is out of range"
                ))
            },
        )?;
    let fuzzy_score =
        u32::try_from(nonnegative_integer(&ranking, "fuzzy_score", side)?).map_err(|_| {
            mlua::Error::runtime(format!(
                "match.compare: {side}.ranking.fuzzy_score: value is out of range"
            ))
        })?;
    Ok(CompletionRanking {
        quality_rank,
        boundary_rank,
        start_index,
        gap_count,
        span_length,
        unmatched_suffix,
        fuzzy_score,
    })
}

/// Compare the textual ranking of two results from {completion}.
///
/// Returns -1 when {left} sorts first, 0 for equal textual ranking, and 1
/// when {right} sorts first. Source priority, provider order, list grouping,
/// and other caller-owned policy are intentionally not compared. Use the
/// result as a boolean predicate for `table.sort`:
/// `function(a, b) return maki.match.compare(a, b) < 0 end`.
///
/// @param left table Match result returned by `maki.match.completion`.
/// @param right table Match result returned by `maki.match.completion`.
/// @return (integer) -1, 0, or 1.
#[lua_fn]
fn compare(_lua: &Lua, left: LuaValue, right: LuaValue) -> LuaResult<i32> {
    let left = match left {
        LuaValue::Table(table) => ranking_from_table(&table, "left")?,
        other => {
            return Err(mlua::Error::runtime(format!(
                "match.compare: left: expected table, got {} ({other:?})",
                other.type_name()
            )));
        }
    };
    let right = match right {
        LuaValue::Table(table) => ranking_from_table(&table, "right")?,
        other => {
            return Err(mlua::Error::runtime(format!(
                "match.compare: right: expected table, got {} ({other:?})",
                other.type_name()
            )));
        }
    };
    Ok(match compare_completion_rankings(&left, &right) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    })
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
        compare,
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
    fn completion_options_control_case_and_normalization() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let completion: mlua::Function = t.get("completion").unwrap();
        lua.globals().set("completion", completion).unwrap();
        lua.load(
            r#"
            assert(completion("APPLE", "apple", { case = "ignore" }), "ignore case")
            assert(completion("APPLE", "apple", { case = "smart" }) == nil, "smart case")
            assert(completion("a", "A", { case = "respect" }) == nil, "respect case")
            assert(completion("e", "bë", { normalization = "smart" }), "smart normalization")
            assert(completion("e", "bë", { normalization = "never" }) == nil, "never normalization")
            assert(completion("ap", "Apple", { case = nil, normalization = nil }), "nil fields")
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn completion_unicode_indices_match_between_rust_and_lua() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let completion: mlua::Function = t.get("completion").unwrap();
        let cases = [
            ("好世", "你好世界", vec![2, 3]),
            ("e", "éclair", vec![1]),
            ("b", "🇺🇸ab", vec![4]),
            ("a", "👍🏽abc", vec![3]),
        ];
        for (query, label, expected) in cases {
            let rust = maki_match::completion_match_default(query, label).unwrap();
            let lua_result = completion
                .call::<Option<mlua::Table>>((query, label))
                .unwrap()
                .expect("Lua completion should match");
            let lua_indices: Vec<u32> = lua_result.get("indices").unwrap();
            let expected_rust: Vec<u32> = expected.iter().map(|index| index - 1).collect();
            assert_eq!(rust.indices, expected_rust);
            assert_eq!(lua_indices, expected);
        }
    }

    #[test]
    fn completion_rejects_invalid_options() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let completion: mlua::Function = t.get("completion").unwrap();
        lua.globals().set("completion", completion).unwrap();
        for (call, expected) in [
            (r#"completion("a", "a", 1)"#, "match.completion: opts"),
            (r#"completion("a", "a", { case = 1 })"#, "opts.case"),
            (r#"completion("a", "a", { case = "wrong" })"#, "opts.case"),
            (
                r#"completion("a", "a", { normalization = true })"#,
                "opts.normalization",
            ),
            (
                r#"completion("a", "a", { normalization = "wrong" })"#,
                "opts.normalization",
            ),
            (
                r#"completion("a", "a", { case = string.char(255) })"#,
                "opts.case",
            ),
        ] {
            let msg: String = lua
                .load(format!(
                    r#"local ok, err = pcall(function() {call} end) return tostring(err)"#
                ))
                .eval()
                .unwrap();
            assert!(msg.contains(expected), "{call}: {msg}");
        }
    }

    #[test]
    fn compare_completion_results_matches_textual_order() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let completion: mlua::Function = t.get("completion").unwrap();
        let compare: mlua::Function = t.get("compare").unwrap();
        lua.globals().set("completion", completion).unwrap();
        lua.globals().set("compare", compare).unwrap();
        lua.load(
            r#"
            local exact = completion("app", "app")
            local prefix = completion("app", "apple")
            local substring = completion("app", "xapp")
            assert(compare(exact, prefix) < 0)
            assert(compare(prefix, exact) > 0)
            assert(compare(exact, exact) == 0)
            assert(compare(prefix, prefix) == 0)
            local equal_copy = { indices = {}, ranking = {} }
            for _, field in ipairs({ "quality_rank", "boundary_rank", "start_index", "gap_count", "span_length", "unmatched_suffix", "fuzzy_score" }) do
                equal_copy.ranking[field] = prefix.ranking[field]
            end
            assert(compare(prefix, equal_copy) == 0)
            local results = { substring, exact, prefix }
            table.sort(results, function(a, b) return compare(a, b) < 0 end)
            assert(results[1] == exact)
            assert(results[2] == prefix)
            assert(results[3] == substring)
            "#,
        )
        .exec()
        .unwrap();
    }

    #[test]
    fn compare_rejects_malformed_results() {
        let lua = mlua::Lua::new();
        let t = super::create_match_table(&lua).unwrap();
        let compare: mlua::Function = t.get("compare").unwrap();
        lua.globals().set("compare", compare).unwrap();
        let cases = [
            (r#"compare(1, {})"#, "match.compare: left"),
            (r#"compare({}, {})"#, "left.ranking"),
            (
                r#"compare({ ranking = {} }, {})"#,
                "left.ranking.quality_rank",
            ),
            (
                r#"compare({ ranking = { quality_rank = 1 } }, {})"#,
                "left.ranking.boundary_rank",
            ),
            (
                r#"compare({ ranking = { quality_rank = 1, boundary_rank = 0, start_index = 0 } }, {})"#,
                "left.ranking.start_index",
            ),
            (
                r#"compare({ ranking = { quality_rank = 1, boundary_rank = 0, start_index = 1, gap_count = -1 } }, {})"#,
                "left.ranking.gap_count",
            ),
            (
                r#"compare({ score = 1, indices = {} }, {})"#,
                "left.ranking",
            ),
        ];
        for (call, expected) in cases {
            let msg: String = lua
                .load(format!(
                    r#"local ok, err = pcall(function() {call} end) return tostring(err)"#
                ))
                .eval()
                .unwrap();
            assert!(msg.contains(expected), "{call}: {msg}");
        }
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
