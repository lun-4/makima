use makima_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, String as LuaString, Value};

use makima_agent::tools::offload;
use makima_agent::tools::{
    trailing_truncation_marker, truncate_file as truncate_file_text,
    truncate_line as truncate_line_text,
};

use super::util::pair::{Pair, pair};

/// Convert an HTML string to Markdown.
/// Useful for cleaning up web content fetched with `makima.webfetch`.
///
/// @param html string HTML source text.
/// @return (string?, string?) Markdown text on success, or nil plus an error message.
/// @example
/// local md, err = makima.text.html_to_markdown("<h1>Hello</h1><p>world</p>")
/// if err then return end
/// print(md) -- "# Hello\n\nworld"
#[lua_fn]
fn html_to_markdown(_lua: &Lua, html: String) -> LuaResult<Pair<String>> {
    Ok(pair(
        htmd::convert(&html).map_err(|e| format!("html_to_markdown: {e}")),
    ))
}

/// Return {s} as valid UTF-8, with every invalid byte sequence replaced by
/// U+FFFD. Lua strings are bytes, and `makima.net` and `makima.fs` hand them
/// over as-is, so decode before measuring or cutting a body as text. An
/// invalid byte can grow to 3 bytes, so a cut made before decoding can triple
/// in size.
///
/// @param s string Bytes to decode.
/// @return (string) Valid UTF-8 text, `s` itself when it already is.
/// @example
/// local resp = makima.net.request(url)
/// local text = makima.text.to_utf8(resp.body)
#[lua_fn]
fn to_utf8(lua: &Lua, s: LuaString) -> LuaResult<LuaString> {
    if s.to_str().is_ok() {
        return Ok(s);
    }
    lua.create_string(s.to_string_lossy())
}

/// Truncate one line while preserving a UTF-8 boundary and ending it with
/// `[line truncated, +N bytes]`, where N is the number of bytes cut.
///
/// @param text string The line to truncate.
/// @param max_bytes integer Maximum bytes of the result, marker included; a cap smaller than the marker yields the marker alone.
/// @return string The truncated line.
#[lua_fn]
fn truncate_line(_lua: &Lua, text: String, max_bytes: usize) -> LuaResult<String> {
    Ok(truncate_line_text(&text, max_bytes))
}

/// Truncate file output by line and byte limits, adding `[file truncated]` when needed.
///
/// @param text string The file output to truncate.
/// @param max_lines integer Maximum lines to retain.
/// @param max_bytes integer Maximum bytes to retain.
/// @param remaining_lines integer? Number of source lines remaining after the output.
/// @return string The truncated file output.
#[lua_fn]
fn truncate_file(
    _lua: &Lua,
    text: String,
    max_lines: usize,
    max_bytes: usize,
    remaining_lines: Value,
) -> LuaResult<String> {
    let remaining_lines = match remaining_lines {
        Value::Integer(lines) if lines >= 0 => Some(lines as usize),
        Value::Integer(_) | Value::Nil => None,
        value => {
            return Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: "integer or nil".to_owned(),
                message: None,
            });
        }
    };
    Ok(truncate_file_text(
        &text,
        max_lines,
        max_bytes,
        remaining_lines,
    ))
}

/// Name the tool output a line's trailing truncation marker came from, if any.
/// Mutation guards use it to reject markers pasted back as file content.
///
/// @param line string One line, without its newline.
/// @return string? `"read"` for a read or grep line marker (current or legacy format), `"preview"` for a line cut in an offload preview, or nil.
#[lua_fn]
fn truncation_marker(_lua: &Lua, line: String) -> LuaResult<Option<&'static str>> {
    Ok(trailing_truncation_marker(&line).map(|marker| marker.as_str()))
}

/// Whether a line is the saved-output footer in a tool result.
///
/// @param line string One line of tool output.
/// @return boolean
#[lua_fn]
fn is_offload_notice(_lua: &Lua, line: String) -> LuaResult<bool> {
    Ok(offload::is_offload_notice(&line))
}

lua_table! {
    /// Text transformation utilities.
    ///
    /// Helper functions for converting between text formats.
    ///
    /// ```lua
    /// local md = makima.text.html_to_markdown(html)
    /// local text = makima.text.to_utf8(bytes)
    /// ```
    "makima.text" => pub(crate) fn create_text_table(), DOCS [
        html_to_markdown,
        to_utf8,
        truncate_line,
        truncate_file,
        truncation_marker,
        is_offload_notice,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlua::{Function, Table};
    use test_case::test_case;

    const LATIN1_TEXT: &[u8] = b"caf\xe9 cr\xe8me";
    const LOSSY_TEXT: &str = "caf\u{fffd} cr\u{fffd}me";

    fn text_table(lua: &Lua) -> Table {
        create_text_table(lua).unwrap()
    }

    #[test_case(b"caf\xc3\xa9", "caf\u{e9}" ; "valid_utf8_is_unchanged")]
    #[test_case(LATIN1_TEXT, LOSSY_TEXT ; "invalid_bytes_become_replacement_chars")]
    fn to_utf8_decodes_lossily(input: &[u8], expected: &str) {
        let lua = Lua::new();
        let table = text_table(&lua);
        let to_utf8: Function = table.get("to_utf8").unwrap();
        let decoded: LuaString = to_utf8.call(lua.create_string(input).unwrap()).unwrap();
        assert_eq!(decoded.to_str().unwrap(), expected);
    }
}
