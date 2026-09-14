use std::fs;
use std::path::Path;
use std::sync::Arc;

use color_eyre::eyre::{Context, Result, eyre};
use maki_commands::{ArgumentKind, CommandArguments, CompletionPolicy, PositionalArgument};
use tree_sitter::{Node, Parser};

const REQUIRED_FIELDS: [&str; 3] = ["name", "description", "tui_only"];
const DOCUMENTED_FIELDS: [&str; 5] = [
    "name",
    "description",
    "argument_hint",
    "arguments",
    "tui_only",
];
const REMOVED_FIELDS: [&str; 4] = ["nargs", "completion", "argument_completion", "completions"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LuaPluginCommand {
    pub name: String,
    pub description: String,
    pub argument_hint: Option<String>,
    pub arguments: CommandArguments,
    pub tui_only: bool,
}

pub fn parse_lua_commands(source: &str) -> Result<Vec<LuaPluginCommand>> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_lua::LANGUAGE.into())
        .map_err(|error| eyre!("could not load Lua grammar: {error}"))?;
    let tree = parser
        .parse(source, None)
        .ok_or_else(|| eyre!("Lua parser did not produce a syntax tree"))?;
    let root = tree.root_node();

    if root.has_error() {
        let error_node = find_syntax_error(root).unwrap_or(root);
        return Err(node_error(
            error_node,
            source,
            "unsupported or malformed Lua syntax",
        ));
    }

    let mut commands = Vec::new();
    collect_commands(root, source, &mut commands)?;
    Ok(commands)
}

fn collect_commands(
    node: Node<'_>,
    source: &str,
    commands: &mut Vec<LuaPluginCommand>,
) -> Result<()> {
    if node.kind() == "function_call" && is_register_command(node, source) {
        commands.push(parse_command(node, source)?);
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_commands(child, source, commands)?;
    }
    Ok(())
}

fn find_syntax_error(node: Node<'_>) -> Option<Node<'_>> {
    if node.is_error() || node.is_missing() {
        return Some(node);
    }

    (0..node.child_count())
        .filter_map(|index| node.child(index as u32))
        .find_map(find_syntax_error)
}

fn is_register_command(node: Node<'_>, source: &str) -> bool {
    let Some(name) = node.child_by_field_name("name") else {
        return false;
    };
    expression_path(name, source).is_some_and(|path| path == ["maki", "api", "register_command"])
}

fn expression_path(node: Node<'_>, source: &str) -> Option<Vec<String>> {
    match node.kind() {
        "identifier" => Some(vec![node_text(node, source).to_owned()]),
        "parenthesized_expression" => node
            .named_child(0)
            .and_then(|child| expression_path(child, source)),
        "dot_index_expression" => {
            let table = node.child_by_field_name("table")?;
            let field = node.child_by_field_name("field")?;
            let mut path = expression_path(table, source)?;
            if field.kind() != "identifier" {
                return None;
            }
            path.push(node_text(field, source).to_owned());
            Some(path)
        }
        "bracket_index_expression" => {
            let table = node.child_by_field_name("table")?;
            let field = node.child_by_field_name("field")?;
            let mut path = expression_path(table, source)?;
            let value = decode_string(field, source).ok()?;
            path.push(value);
            Some(path)
        }
        _ => None,
    }
}

fn parse_command(node: Node<'_>, source: &str) -> Result<LuaPluginCommand> {
    let arguments = node
        .child_by_field_name("arguments")
        .ok_or_else(|| node_error(node, source, "registration call has no arguments"))?;
    let children: Vec<_> = arguments.named_children(&mut arguments.walk()).collect();
    let [table] = children.as_slice() else {
        return Err(node_error(
            arguments,
            source,
            "register_command requires exactly one inline table argument",
        ));
    };
    if table.kind() != "table_constructor" {
        return Err(node_error(
            *table,
            source,
            "register_command requires an inline table argument",
        ));
    }

    let mut name = None;
    let mut description = None;
    let mut argument_hint = None;
    let mut arguments = None;
    let mut tui_only = None;
    let mut seen = [false; DOCUMENTED_FIELDS.len()];

    let mut cursor = table.walk();
    for field in table.named_children(&mut cursor) {
        let Some(key) = field.child_by_field_name("name") else {
            return Err(node_error(
                field,
                source,
                "unkeyed registration-table entries are unsupported",
            ));
        };
        let key = match key.kind() {
            "identifier" if key.start_byte() == field.start_byte() => {
                node_text(key, source).to_owned()
            }
            "identifier" => {
                return Err(node_error(
                    key,
                    source,
                    "computed registration-table keys are unsupported",
                ));
            }
            "string" => decode_string(key, source)
                .map_err(|error| node_error(key, source, &error.to_string()))?,
            _ => {
                return Err(node_error(
                    key,
                    source,
                    "computed registration-table keys are unsupported",
                ));
            }
        };
        let Some(index) = DOCUMENTED_FIELDS.iter().position(|field| *field == key) else {
            if REMOVED_FIELDS.contains(&key.as_str()) {
                return Err(node_error(
                    key_node(field),
                    source,
                    &format!("registration field `{key}` is obsolete; use `arguments`"),
                ));
            }
            continue;
        };
        if seen[index] {
            return Err(node_error(
                key_node(field),
                source,
                &format!("duplicate registration field `{key}`"),
            ));
        }
        seen[index] = true;

        let value = field.child_by_field_name("value").ok_or_else(|| {
            node_error(
                field,
                source,
                &format!("registration field `{key}` has no value"),
            )
        })?;
        match key.as_str() {
            "name" => name = Some(string_field(value, source, &key)?),
            "description" => description = Some(string_field(value, source, &key)?),
            "argument_hint" => {
                argument_hint = if value.kind() == "nil" {
                    None
                } else {
                    Some(string_field(value, source, &key)?)
                };
            }
            "arguments" => {
                if arguments.is_some() {
                    return Err(node_error(
                        value,
                        source,
                        "duplicate registration field `arguments`",
                    ));
                }
                arguments = Some(parse_arguments(value, source)?);
            }
            "tui_only" => {
                tui_only = match value.kind() {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => {
                        return Err(node_error(
                            value,
                            source,
                            "registration field `tui_only` must be a literal boolean",
                        ));
                    }
                };
            }
            _ => unreachable!(),
        }
    }

    for field in REQUIRED_FIELDS {
        let index = DOCUMENTED_FIELDS
            .iter()
            .position(|documented| *documented == field)
            .expect("required field is documented");
        if !seen[index] {
            return Err(node_error(
                *table,
                source,
                &format!("registration is missing required field `{field}`"),
            ));
        }
    }

    Ok(LuaPluginCommand {
        name: name.expect("required field checked"),
        description: description.expect("required field checked"),
        argument_hint,
        arguments: arguments.unwrap_or_else(|| CommandArguments::Positional(Arc::from([]))),
        tui_only: tui_only.expect("required field checked"),
    })
}

fn table_fields<'tree>(node: Node<'tree>) -> impl Iterator<Item = Node<'tree>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .filter(|child| !child.is_extra())
        .collect::<Vec<_>>()
        .into_iter()
}

fn parse_arguments(node: Node<'_>, source: &str) -> Result<CommandArguments> {
    if node.kind() != "table_constructor" {
        return Err(node_error(
            node,
            source,
            "registration field `arguments` must be an inline table",
        ));
    }
    let fields: Vec<_> = table_fields(node).collect();
    if fields.iter().any(|field| {
        field
            .child_by_field_name("name")
            .is_some_and(|key| node_text(key, source) == "raw")
    }) {
        let mut raw = None;
        let mut required = false;
        for field in fields {
            let key = table_field_name(field, source)?;
            let value = field.child_by_field_name("value").ok_or_else(|| {
                node_error(
                    field,
                    source,
                    &format!("raw argument field `{key}` has no value"),
                )
            })?;
            match key.as_str() {
                "raw" => {
                    if raw.replace(value.kind() == "true").is_some() || value.kind() != "true" {
                        return Err(node_error(
                            value,
                            source,
                            "raw arguments require `raw = true`",
                        ));
                    }
                }
                "required" => required = boolean_field(value, source, "raw required")?,
                _ => {
                    return Err(node_error(
                        key_node(field),
                        source,
                        "raw arguments accept only `raw` and `required`",
                    ));
                }
            }
        }
        return Ok(CommandArguments::Raw { required });
    }
    let mut arguments = Vec::new();
    for field in fields {
        let value = table_entry_value(field).ok_or_else(|| {
            node_error(
                field,
                source,
                "registration `arguments` entries must be inline tables",
            )
        })?;
        if value.kind() != "table_constructor" {
            return Err(node_error(
                value,
                source,
                "registration `arguments` entries must be inline tables",
            ));
        }
        arguments.push(parse_argument(value, source)?);
    }
    Ok(CommandArguments::Positional(arguments.into()))
}

fn parse_argument(node: Node<'_>, source: &str) -> Result<PositionalArgument> {
    let fields = table_fields(node)
        .map(|field| Ok((table_field_name(field, source)?, field)))
        .collect::<Result<Vec<_>>>()?;
    let type_name = fields
        .iter()
        .find(|(key, _)| key == "type")
        .map(|(_, field)| {
            field
                .child_by_field_name("value")
                .ok_or_else(|| {
                    node_error(
                        *field,
                        source,
                        "registration argument field `type` has no value",
                    )
                })
                .and_then(|value| string_field(value, source, "argument type"))
        })
        .transpose()?
        .ok_or_else(|| node_error(node, source, "registration argument is missing `type`"))?;
    let allowed = if type_name == "enum" {
        &[
            "name",
            "type",
            "choices",
            "optional",
            "variadic",
            "completion",
        ][..]
    } else {
        &["name", "type", "optional", "variadic", "completion"][..]
    };
    let mut name = None;
    let mut choices = None;
    let mut optional = false;
    let mut variadic = false;
    for (key, field) in fields {
        if !allowed.contains(&key.as_str()) {
            return Err(node_error(
                key_node(field),
                source,
                &format!("unknown registration argument field `{key}`"),
            ));
        }
        let value = field.child_by_field_name("value").ok_or_else(|| {
            node_error(
                field,
                source,
                &format!("registration argument field `{key}` has no value"),
            )
        })?;
        match key.as_str() {
            "name" => name = Some(string_field(value, source, "argument name")?),
            "type" | "completion" => {}
            "choices" => choices = Some(parse_choices(value, source)?),
            "optional" => optional = boolean_field(value, source, "argument optional")?,
            "variadic" => variadic = boolean_field(value, source, "argument variadic")?,
            _ => unreachable!("allowed argument field"),
        }
    }
    let name =
        name.ok_or_else(|| node_error(node, source, "registration argument is missing `name`"))?;
    let kind = match type_name.as_str() {
        "string" if choices.is_none() => ArgumentKind::String,
        "integer" if choices.is_none() => ArgumentKind::Integer,
        "file" if choices.is_none() => ArgumentKind::File,
        "directory" if choices.is_none() => ArgumentKind::Directory,
        "enum" => ArgumentKind::Enum(
            choices
                .ok_or_else(|| {
                    node_error(node, source, "enum arguments require literal `choices`")
                })?
                .into_iter()
                .map(Arc::<str>::from)
                .collect::<Vec<_>>()
                .into(),
        ),
        _ => {
            return Err(node_error(
                node,
                source,
                "registration argument `type` must be string, integer, enum, file, or directory",
            ));
        }
    };
    Ok(PositionalArgument {
        name: Arc::from(name),
        kind,
        optional,
        variadic,
        completion: CompletionPolicy::Default,
    })
}

fn parse_choices(node: Node<'_>, source: &str) -> Result<Vec<String>> {
    if node.kind() != "table_constructor" {
        return Err(node_error(
            node,
            source,
            "enum argument `choices` must be an inline array",
        ));
    }
    let mut choices = Vec::new();
    for field in table_fields(node) {
        let value = table_entry_value(field)
            .ok_or_else(|| node_error(field, source, "enum choices must contain strings"))?;
        choices.push(string_field(value, source, "enum choice")?);
    }
    if choices.is_empty() {
        return Err(node_error(
            node,
            source,
            "enum argument `choices` must not be empty",
        ));
    }
    Ok(choices)
}

fn table_entry_value(field: Node<'_>) -> Option<Node<'_>> {
    field
        .child_by_field_name("value")
        .or_else(|| field.named_child(0))
}

fn table_field_name(field: Node<'_>, source: &str) -> Result<String> {
    let key = field.child_by_field_name("name").ok_or_else(|| {
        node_error(
            field,
            source,
            "registration argument fields must use literal keys",
        )
    })?;
    match key.kind() {
        "identifier" if key.start_byte() == field.start_byte() => {
            Ok(node_text(key, source).to_owned())
        }
        "string" => {
            decode_string(key, source).map_err(|error| node_error(key, source, &error.to_string()))
        }
        _ => Err(node_error(
            key,
            source,
            "computed registration argument keys are unsupported",
        )),
    }
}

fn boolean_field(node: Node<'_>, source: &str, field: &str) -> Result<bool> {
    match node.kind() {
        "true" => Ok(true),
        "false" | "nil" => Ok(false),
        _ => Err(node_error(
            node,
            source,
            &format!("registration field `{field}` must be a literal boolean"),
        )),
    }
}

fn key_node(field: Node<'_>) -> Node<'_> {
    field.child_by_field_name("name").unwrap_or(field)
}

fn string_field(node: Node<'_>, source: &str, field: &str) -> Result<String> {
    if node.kind() != "string" {
        return Err(node_error(
            node,
            source,
            &format!("registration field `{field}` must be a literal string"),
        ));
    }
    decode_string(node, source).map_err(|error| node_error(node, source, &error.to_string()))
}

fn decode_string(node: Node<'_>, source: &str) -> Result<String> {
    let raw = node_text(node, source);
    if raw.starts_with('[') {
        decode_long_string(raw)
    } else {
        decode_quoted_string(raw)
    }
}

fn decode_quoted_string(raw: &str) -> Result<String> {
    let quote = raw
        .as_bytes()
        .first()
        .copied()
        .ok_or_else(|| eyre!("empty Lua string"))?;
    if !matches!(quote, b'\'' | b'"') || raw.as_bytes().last() != Some(&quote) {
        return Err(eyre!("invalid quoted Lua string"));
    }

    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len().saturating_sub(2));
    let mut index = 1;
    while index + 1 < bytes.len() {
        let byte = bytes[index];
        if byte != b'\\' {
            append_normalized_byte(bytes, &mut index, &mut decoded);
            continue;
        }
        index += 1;
        if index + 1 >= bytes.len() {
            return Err(eyre!("trailing escape in Lua string"));
        }
        match bytes[index] {
            b'a' => {
                decoded.push(7);
                index += 1;
            }
            b'b' => {
                decoded.push(8);
                index += 1;
            }
            b'f' => {
                decoded.push(12);
                index += 1;
            }
            b'n' => {
                decoded.push(b'\n');
                index += 1;
            }
            b'r' => {
                decoded.push(b'\r');
                index += 1;
            }
            b't' => {
                decoded.push(b'\t');
                index += 1;
            }
            b'v' => {
                decoded.push(11);
                index += 1;
            }
            b'\\' | b'\'' | b'"' => {
                decoded.push(bytes[index]);
                index += 1;
            }
            b'\n' => {
                decoded.push(b'\n');
                index += 1;
            }
            b'\r' => {
                decoded.push(b'\n');
                index += 1;
                if bytes.get(index) == Some(&b'\n') {
                    index += 1;
                }
            }
            b'z' => {
                index += 1;
                while index + 1 < bytes.len() && is_lua_whitespace(bytes[index]) {
                    index += 1;
                }
            }
            b'x' => {
                let end = index + 3;
                if end >= bytes.len() {
                    return Err(eyre!("incomplete hexadecimal escape in Lua string"));
                }
                let value = parse_hex(&bytes[index + 1..end])?;
                decoded.push(value);
                index = end;
            }
            b'u' => {
                let mut end = index + 1;
                if bytes.get(end) != Some(&b'{') {
                    return Err(eyre!("invalid Unicode escape in Lua string"));
                }
                end += 1;
                let start = end;
                while bytes.get(end).is_some_and(|byte| byte.is_ascii_hexdigit()) {
                    end += 1;
                }
                if start == end || bytes.get(end) != Some(&b'}') {
                    return Err(eyre!("invalid Unicode escape in Lua string"));
                }
                let value = u32::from_str_radix(&raw[start..end], 16)
                    .map_err(|_| eyre!("invalid Unicode escape in Lua string"))?;
                let character = char::from_u32(value)
                    .ok_or_else(|| eyre!("Unicode escape is outside the valid range"))?;
                let mut buffer = [0; 4];
                decoded.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
                index = end + 1;
            }
            byte if byte.is_ascii_digit() => {
                let start = index;
                let mut end = index;
                while end < bytes.len() - 1 && end - start < 3 && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                let value = raw[start..end]
                    .parse::<u16>()
                    .map_err(|_| eyre!("invalid decimal escape in Lua string"))?;
                if value > u8::MAX as u16 {
                    return Err(eyre!("decimal escape is outside the byte range"));
                }
                decoded.push(value as u8);
                index = end;
            }
            _ => return Err(eyre!("unsupported escape in Lua string")),
        }
    }

    String::from_utf8(decoded).map_err(|_| eyre!("Lua string is not valid UTF-8"))
}

fn append_normalized_byte(bytes: &[u8], index: &mut usize, output: &mut Vec<u8>) {
    match bytes[*index] {
        b'\r' => {
            output.push(b'\n');
            *index += 1;
            if bytes.get(*index) == Some(&b'\n') {
                *index += 1;
            }
        }
        byte => {
            output.push(byte);
            *index += 1;
        }
    }
}

fn decode_long_string(raw: &str) -> Result<String> {
    let bytes = raw.as_bytes();
    if bytes.len() < 4 || bytes[0] != b'[' {
        return Err(eyre!("invalid long Lua string"));
    }
    let mut opening_end = 1;
    while bytes.get(opening_end) == Some(&b'=') {
        opening_end += 1;
    }
    if bytes.get(opening_end) != Some(&b'[') {
        return Err(eyre!("invalid long Lua string delimiter"));
    }
    let level = opening_end - 1;
    let closing_start = bytes
        .len()
        .checked_sub(level + 2)
        .ok_or_else(|| eyre!("invalid long Lua string"))?;
    if bytes.get(closing_start) != Some(&b']')
        || bytes
            .get(closing_start + 1..bytes.len() - 1)
            .is_none_or(|delimiter| {
                delimiter.len() != level || delimiter.iter().any(|byte| *byte != b'=')
            })
        || bytes.last() != Some(&b']')
    {
        return Err(eyre!("invalid long Lua string delimiter"));
    }

    let mut content = &bytes[opening_end + 1..closing_start];
    if content.starts_with(b"\r\n") {
        content = &content[2..];
    } else if content.starts_with(b"\n") || content.starts_with(b"\r") {
        content = &content[1..];
    }
    let mut normalized = Vec::with_capacity(content.len());
    let mut index = 0;
    while index < content.len() {
        append_normalized_byte(content, &mut index, &mut normalized);
    }
    String::from_utf8(normalized).map_err(|_| eyre!("Lua string is not valid UTF-8"))
}

fn parse_hex(bytes: &[u8]) -> Result<u8> {
    let text = std::str::from_utf8(bytes).map_err(|_| eyre!("invalid hexadecimal escape"))?;
    u8::from_str_radix(text, 16).map_err(|_| eyre!("invalid hexadecimal escape"))
}

fn is_lua_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn node_text<'a>(node: Node<'_>, source: &'a str) -> &'a str {
    &source[node.byte_range()]
}

fn node_error(node: Node<'_>, _source: &str, reason: &str) -> color_eyre::Report {
    let position = node.start_position();
    eyre!(
        "line {} column {}: {reason}",
        position.row + 1,
        position.column + 1
    )
}

pub fn load_builtin_plugin_commands() -> Result<Vec<LuaPluginCommand>> {
    let plugin_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../plugins");
    load_plugin_commands(&plugin_dir)
}

fn load_plugin_commands(plugin_dir: &Path) -> Result<Vec<LuaPluginCommand>> {
    let entries = fs::read_dir(plugin_dir).wrap_err_with(|| {
        format!(
            "could not enumerate plugin directory {}",
            plugin_dir.display()
        )
    })?;
    let mut commands = Vec::new();
    for entry in entries {
        let entry = entry.wrap_err_with(|| {
            format!(
                "could not read an entry in plugin directory {}",
                plugin_dir.display()
            )
        })?;
        let file_type = entry.file_type().wrap_err_with(|| {
            format!("could not inspect plugin path {}", entry.path().display())
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let path = entry.path().join("init.lua");
        let source = match fs::read_to_string(&path) {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(error).wrap_err_with(|| format!("could not read {}", path.display()));
            }
        };
        commands.extend(
            parse_lua_commands(&source)
                .wrap_err_with(|| format!("could not parse {}", path.display()))?,
        );
    }
    commands.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(commands)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;
    use test_case::test_case;

    use maki_commands::CommandArguments;

    use super::{decode_string, load_plugin_commands, parse_lua_commands};

    #[test_case("'hello'" => "hello"; "single quoted")]
    #[test_case("\"line\\nnext\"" => "line\nnext"; "control escape")]
    #[test_case("'quote\\\' slash\\\\'" => "quote' slash\\"; "escaped delimiters")]
    #[test_case("'\\6'" => "\u{06}"; "one digit escape")]
    #[test_case("'\\65'" => "A"; "two digit escape")]
    #[test_case("'\\065'" => "A"; "three digit escape")]
    #[test_case("'\\0654'" => "A4"; "decimal escape followed by digit")]
    #[test_case("'\\065\\x42\\u{43}'" => "ABC"; "byte escapes")]
    #[test_case("'a\\z  \n  b'" => "ab"; "whitespace escape")]
    #[test_case("'a\\\nb'" => "a\nb"; "escaped newline")]
    #[test_case("[=[\nhello\r\nworld]=]" => "hello\nworld"; "long string")]
    #[test_case("[==[\nhello]==]" => "hello"; "long string delimiter level")]
    fn decodes_lua_string_literals(source: &str) -> String {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_lua::LANGUAGE.into())
            .expect("language");
        let tree = parser.parse(source, None).expect("tree");
        let node = tree.root_node().named_child(0).expect("string node");
        decode_string(node, source).expect("valid literal")
    }

    #[test_case(r#"'\u{110000}'"# => "Unicode escape is outside the valid range"; "invalid unicode scalar")]
    #[test_case(r#"'\u{}'"# => "invalid Unicode escape in Lua string"; "empty unicode escape")]
    fn rejects_invalid_string_literals(source: &str) -> String {
        let mut parser = tree_sitter::Parser::new();
        parser
            .set_language(&tree_sitter_lua::LANGUAGE.into())
            .expect("language");
        let tree = parser.parse(source, None).expect("tree");
        decode_string(
            tree.root_node().named_child(0).expect("string node"),
            source,
        )
        .expect_err("invalid literal")
        .to_string()
    }

    #[test]
    fn parses_static_command_forms() {
        let source = r#"
            (maki["api"].register_command) {
                ["name"] = "/one",
                description = [[first\nsecond]],
                tui_only = false,
                arguments = {},
                handler = function() end,
            }
            maki.api.register_command({
                name = "/two", description = "Two", tui_only = true, arguments = { raw = true },
            })
        "#;
        let commands = parse_lua_commands(source).expect("commands");
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].name, "/one");
        assert_eq!(commands[0].description, r"first\nsecond");
        assert!(matches!(
            &commands[0].arguments,
            CommandArguments::Positional(arguments) if arguments.is_empty()
        ));
        assert!(commands[1].tui_only);
        assert!(matches!(
            &commands[1].arguments,
            CommandArguments::Raw { required: false }
        ));
    }

    #[test]
    fn parses_typed_and_raw_arguments() {
        let source = r#"
            maki.api.register_command({
                name = "/typed",
                description = "Typed",
                tui_only = false,
                arguments = {
                    -- Source path
                    { name = "source", type = "file" },
                    {
                        name = "mode",
                        -- Supported modes
                        type = "enum",
                        choices = {
                            "fast",
                            -- Safest mode
                            "safe",
                        },
                        optional = true,
                    },
                    { name = "paths", type = "directory", variadic = true },
                },
            })
            maki.api.register_command({
                name = "/raw",
                description = "Raw",
                tui_only = false,
                arguments = { raw = true },
            })
        "#;
        let commands = parse_lua_commands(source).expect("commands");
        assert!(matches!(
            &commands[0].arguments,
            CommandArguments::Positional(arguments)
                if arguments.len() == 3
                    && arguments[0].name.as_ref() == "source"
                    && arguments[1].optional
                    && arguments[2].variadic
        ));
        assert!(matches!(
            &commands[1].arguments,
            CommandArguments::Raw { required: false }
        ));
    }

    #[test_case(
        "{ name = 'value', type = 'string', typo = true }",
        "unknown registration argument field `typo`"
        ; "unknown_scalar_field"
    )]
    #[test_case(
        "{ name = 'value', type = 'string', choices = { 'x' } }",
        "unknown registration argument field `choices`"
        ; "choices_on_scalar"
    )]
    #[test_case(
        "{ name = 'value', type = 'enum', choices = { 'x' }, typo = true }",
        "unknown registration argument field `typo`"
        ; "unknown_enum_field"
    )]
    fn rejects_unknown_typed_argument_fields(descriptor: &str, expected: &str) {
        let source = format!(
            r#"maki.api.register_command({{
                name = "/typed",
                description = "Typed",
                tui_only = false,
                arguments = {{ {descriptor} }},
            }})"#
        );
        let error = parse_lua_commands(&source).expect_err("unknown field");
        assert!(error.to_string().contains(expected), "{error}");
    }

    #[test]
    fn accepts_completion_field_in_typed_argument_docs() {
        let source = r#"maki.api.register_command({
            name = "/typed",
            description = "Typed",
            tui_only = false,
            arguments = {
                { name = "value", type = "string", completion = { get_items = provider } },
            },
        })"#;
        assert_eq!(parse_lua_commands(source).expect("command").len(), 1);
    }

    #[test]
    fn ignores_non_registration_syntax() {
        let source = r#"
            -- maki.api.register_command({ name = "/comment" })
            local text = "maki.api.register_command({ name = '/string' })"
            other.api.register_command({ name = "/other", description = "x", tui_only = true })
        "#;
        assert!(parse_lua_commands(source).expect("source").is_empty());
    }

    #[test]
    fn rejects_missing_outer_metadata() {
        let source = r#"
            maki.api.register_command({
                handler = function()
                    maki.api.register_command({ name = "/inner", description = "inner", tui_only = true })
                end,
            })
        "#;
        let error = parse_lua_commands(source).expect_err("missing metadata");
        assert!(error.to_string().contains("missing required field `name`"));
    }

    #[test]
    fn rejects_invalid_source() {
        let error = parse_lua_commands("maki.api.register_command({").expect_err("invalid source");
        assert!(error.to_string().contains("line 1 column"));
        assert!(error.to_string().contains("malformed Lua syntax"));
    }

    #[test_case("maki.api.register_command(command)"; "variable argument")]
    #[test_case("maki.api.register_command({ name = name, description = \"x\", tui_only = true })"; "computed string")]
    #[test_case("maki.api.register_command({ name = \"x\", description = \"x\", tui_only = enabled })"; "computed boolean")]
    #[test_case("maki.api.register_command({ [key] = \"x\", name = \"x\", description = \"x\", tui_only = true })"; "computed key")]
    #[test_case("maki.api.register_command({ name = \"x\", name = \"y\", description = \"x\", tui_only = true })"; "duplicate field")]
    #[test_case("maki.api.register_command({ name = \"x\", description = \"x\" })"; "missing field")]
    fn rejects_unsupported_command_metadata(source: &str) {
        let error = parse_lua_commands(source).expect_err("unsupported declaration");
        assert!(error.to_string().contains("line 1 column"));
    }

    #[test]
    fn loads_plugin_directories() {
        let root = tempdir().expect("tempdir");
        fs::create_dir(root.path().join("first")).expect("first");
        fs::create_dir(root.path().join("second")).expect("second");
        fs::create_dir(root.path().join("empty")).expect("empty");
        fs::write(
            root.path().join("first/init.lua"),
            "maki.api.register_command({name=\"/z\",description=\"z\",tui_only=false,arguments={}})",
        )
        .expect("first source");
        fs::write(
            root.path().join("second/init.lua"),
            "maki.api.register_command({name=\"/a\",description=\"a\",tui_only=true,arguments={raw=true}})",
        )
        .expect("second source");
        let commands = load_plugin_commands(root.path()).expect("commands");
        assert_eq!(
            commands
                .iter()
                .map(|command| command.name.as_str())
                .collect::<Vec<_>>(),
            ["/a", "/z"]
        );
    }

    #[test]
    fn loader_reports_source_path() {
        let root = tempdir().expect("tempdir");
        let plugin = root.path().join("broken");
        fs::create_dir(&plugin).expect("plugin");
        fs::write(plugin.join("init.lua"), "maki.api.register_command({").expect("source");
        let error = load_plugin_commands(root.path()).expect_err("invalid source");
        assert!(error.to_string().contains("broken/init.lua"));
    }

    #[test]
    fn loader_rejects_invalid_utf8() {
        let root = tempdir().expect("tempdir");
        let plugin = root.path().join("invalid");
        fs::create_dir(&plugin).expect("plugin");
        fs::write(plugin.join("init.lua"), [0xff]).expect("source");
        let error = load_plugin_commands(root.path()).expect_err("invalid utf8");
        assert!(error.to_string().contains("invalid/init.lua"));
    }

    #[test]
    fn loader_rejects_missing_root() {
        let root = tempdir().expect("tempdir");
        let error = load_plugin_commands(&root.path().join("missing")).expect_err("missing root");
        assert!(error.to_string().contains("missing"));
    }
}
