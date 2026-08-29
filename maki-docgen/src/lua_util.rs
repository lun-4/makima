pub fn find_matching_brace(s: &str, open: usize) -> Option<usize> {
    let mut depth = 0;
    for (i, ch) in s[open..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(open + i);
                }
            }
            _ => {}
        }
    }
    None
}

pub fn extract_lua_field(s: &str, field: &str) -> Option<String> {
    let dq = format!("{field} = \"");
    let sq = format!("{field} = '");
    if let Some(start) = s.find(&dq) {
        let after = &s[start + dq.len()..];
        let end = after.find('"')?;
        Some(unescape_lua_string(&after[..end]))
    } else {
        let start = s.find(&sq)?;
        let after = &s[start + sq.len()..];
        let end = after.find('\'')?;
        Some(unescape_lua_string(&after[..end]))
    }
}

pub fn extract_lua_bool(s: &str, field: &str) -> Option<bool> {
    let marker = format!("{field} = ");
    let value = s.get(s.find(&marker)? + marker.len()..)?;
    if value.starts_with("true") {
        Some(true)
    } else if value.starts_with("false") {
        Some(false)
    } else {
        None
    }
}

fn unescape_lua_string(s: &str) -> String {
    s.replace("\\n", "\n")
}

pub struct LuaPluginCommand {
    pub name: String,
    pub description: String,
    pub argument_hint: Option<String>,
    pub tui_only: bool,
}

pub fn parse_lua_commands(source: &str) -> Vec<LuaPluginCommand> {
    let mut commands = Vec::new();
    let marker = "register_command({";
    let mut search = source;
    while let Some(start) = search.find(marker) {
        let block = &search[start + marker.len() - 1..];
        if let Some(end) = find_matching_brace(block, 0) {
            let inner = &block[1..end];
            let name = extract_lua_field(inner, "name");
            let desc = extract_lua_field(inner, "description");
            let Some(tui_only) = extract_lua_bool(inner, "tui_only") else {
                search = &block[end..];
                continue;
            };
            if let (Some(name), Some(description)) = (name, desc) {
                commands.push(LuaPluginCommand {
                    name,
                    description,
                    argument_hint: extract_lua_field(inner, "argument_hint"),
                    tui_only,
                });
            }
            search = &block[end..];
        } else {
            break;
        }
    }
    commands
}

pub fn load_builtin_plugin_commands() -> Vec<LuaPluginCommand> {
    let plugin_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../plugins");
    let Ok(entries) = std::fs::read_dir(plugin_dir) else {
        return Vec::new();
    };
    let mut commands: Vec<LuaPluginCommand> = entries
        .filter_map(|e| e.ok())
        .flat_map(|e| {
            let path = e.path().join("init.lua");
            let source = std::fs::read_to_string(&path).ok()?;
            Some(parse_lua_commands(&source))
        })
        .flatten()
        .collect();
    commands.sort_by(|a, b| a.name.cmp(&b.name));
    commands
}
