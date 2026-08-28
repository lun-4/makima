use std::fs;
use std::path::{Path, PathBuf};

use tree_sitter::{Node, Parser};

const FRONTEND_ROOTS: &[&str] = &["maki-ui/src", "maki-acp/src", "src"];
const FORBIDDEN_FRONTEND_CALLS: &[&str] = &["create_producer", "register_commands"];
const FORBIDDEN_FRONTEND_IDENTIFIERS: &[&str] = &[
    "BUILTIN_COMMANDS",
    "BuiltinId",
    "InvocationLifecycle",
    "McpPromptSink",
    "PromptSink",
    "resolve_model_arg",
    "resolve_theme_arg",
    "cmd_cd",
];
const FORBIDDEN_ACP_IDENTIFIERS: &[&str] = &[
    "BuiltinId",
    "BuiltinOperation",
    "ProducerId",
    "CommandMailbox",
    "InvocationLifecycle",
];
const FORBIDDEN_ACP_COMMANDS: &[&str] = &[
    "/compact",
    "/new",
    "/clear",
    "/model",
    "/cd",
    "/btw",
    "/yolo",
    "/fast",
    "/workflow",
    "/tasks",
    "/help",
    "/theme",
    "/mcp",
    "/login",
    "/exit",
    "/reload",
    "/sessions",
];

#[test]
fn frontends_cannot_register_standard_commands() {
    for source in production_sources(FRONTEND_ROOTS) {
        let path = source.path.to_string_lossy();
        for call in FORBIDDEN_FRONTEND_CALLS {
            assert!(
                !source.calls.iter().any(|name| name == call),
                "{path} calls forbidden standard-registration API `{call}`"
            );
        }
        for identifier in FORBIDDEN_FRONTEND_IDENTIFIERS {
            assert!(
                !source.identifiers.iter().any(|name| name == identifier),
                "{path} contains forbidden standard-registration identifier `{identifier}`"
            );
        }
    }
}

#[test]
fn acp_is_command_agnostic_proxy() {
    for source in production_sources(&["maki-acp/src"]) {
        let path = source.path.to_string_lossy();
        for identifier in FORBIDDEN_ACP_IDENTIFIERS {
            assert!(
                !source.identifiers.iter().any(|name| name == identifier),
                "{path} contains command-specific identifier `{identifier}`"
            );
        }
        for command in FORBIDDEN_ACP_COMMANDS {
            assert!(
                !source.strings.iter().any(|value| value == command),
                "{path} contains command-specific literal `{command}`"
            );
        }
    }
}

#[test]
fn maki_commands_dependency_direction_is_neutral() {
    let manifest = fs::read_to_string("maki-commands/Cargo.toml").unwrap();
    for dependency in [
        "maki-agent",
        "maki-ui",
        "maki-acp",
        "maki-lua",
        "maki-providers",
        "maki-config",
        "maki-storage",
    ] {
        assert!(
            !manifest.lines().any(|line| {
                line.split_once('=')
                    .is_some_and(|(name, _)| name.trim() == dependency)
            }),
            "maki-commands depends on `{dependency}`"
        );
    }
}

struct Source {
    path: PathBuf,
    identifiers: Vec<String>,
    calls: Vec<String>,
    strings: Vec<String>,
}

fn production_sources(roots: &[&str]) -> Vec<Source> {
    roots
        .iter()
        .flat_map(|root| rust_files(Path::new(root)))
        .filter_map(parse_source)
        .collect()
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return files;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files.extend(rust_files(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && path
                .file_name()
                .is_none_or(|name| name != "architecture_tests.rs")
        {
            files.push(path);
        }
    }
    files
}

fn parse_source(path: PathBuf) -> Option<Source> {
    let text = fs::read_to_string(&path).ok()?;
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .ok()?;
    let tree = parser.parse(&text, None)?;
    let mut source = Source {
        path,
        identifiers: Vec::new(),
        calls: Vec::new(),
        strings: Vec::new(),
    };
    collect(tree.root_node(), text.as_bytes(), false, &mut source);
    Some(source)
}

fn collect(node: Node<'_>, text: &[u8], in_test: bool, source: &mut Source) {
    let in_test = in_test || is_test_item(node, text);
    if in_test {
        return;
    }
    match node.kind() {
        "identifier" | "type_identifier" => {
            if let Ok(identifier) = node.utf8_text(text) {
                source.identifiers.push(identifier.to_owned());
            }
        }
        "call_expression" => {
            if let Some(function) = node.child_by_field_name("function")
                && let Some(name) = terminal_identifier(function, text)
            {
                source.calls.push(name);
            }
        }
        "string_literal" => {
            if let Ok(literal) = node.utf8_text(text)
                && let Some(value) = literal.strip_prefix('"').and_then(|s| s.strip_suffix('"'))
            {
                source.strings.push(value.to_owned());
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect(child, text, in_test, source);
    }
}

fn is_test_item(node: Node<'_>, text: &[u8]) -> bool {
    if !matches!(node.kind(), "mod_item" | "function_item") {
        return false;
    }
    let mut sibling = node.prev_named_sibling();
    while let Some(attribute) = sibling {
        if attribute.kind() != "attribute_item" {
            break;
        }
        if attribute
            .utf8_text(text)
            .is_ok_and(|value| value.contains("test"))
        {
            return true;
        }
        sibling = attribute.prev_named_sibling();
    }
    false
}

fn terminal_identifier(node: Node<'_>, text: &[u8]) -> Option<String> {
    if node.kind() == "identifier" {
        return node.utf8_text(text).ok().map(str::to_owned);
    }
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .filter_map(|child| terminal_identifier(child, text))
        .last()
}
