use maki_lua_macro::{lua_fn, lua_table};
use mlua::{Lua, Result as LuaResult, Value};

use maki_agent::tools::{
    trailing_truncation_marker, truncate_file as truncate_file_text,
    truncate_line as truncate_line_text,
};

use super::util::pair::{Pair, pair};

/// Convert an HTML string to Markdown.
/// Useful for cleaning up web content fetched with `maki.webfetch`.
///
/// @param html string HTML source text.
/// @return (string?, string?) Markdown text on success, or nil plus an error message.
/// @example
/// local md, err = maki.text.html_to_markdown("<h1>Hello</h1><p>world</p>")
/// if err then return end
/// print(md) -- "# Hello\n\nworld"
#[lua_fn]
fn html_to_markdown(_lua: &Lua, html: String) -> LuaResult<Pair<String>> {
    Ok(pair(
        htmd::convert(&html).map_err(|e| format!("html_to_markdown: {e}")),
    ))
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
/// @return string? `"read"` for a read or grep line marker (current or legacy format), or nil.
#[lua_fn]
fn truncation_marker(_lua: &Lua, line: String) -> LuaResult<Option<&'static str>> {
    Ok(trailing_truncation_marker(&line).map(|marker| marker.as_str()))
}

lua_table! {
    /// Text transformation utilities.
    ///
    /// Helper functions for converting between text formats.
    ///
    /// ```lua
    /// local md = maki.text.html_to_markdown(html)
    /// ```
    "maki.text" => pub(crate) fn create_text_table(), DOCS [
        html_to_markdown,
        truncate_line,
        truncate_file,
        truncation_marker,
    ]
}
