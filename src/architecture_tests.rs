use std::fs;
use std::path::{Path, PathBuf};

use tree_sitter::{Node, Parser};

const FRONTEND_ROOTS: &[&str] = &["maki-ui/src", "maki-acp/src", "src"];
const SOURCE_SCAN_ERROR: &str = "failed to scan production Rust sources";
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
const FORBIDDEN_RUST_USAGE_UI_IDENTIFIERS: &[&str] = &[
    "UsageWindow::label",
    "UsageWindow::short",
    "RefreshUsage",
    "ToggleUsage",
    "UsageFetchState",
    "UsageModal",
    "UsageModalContext",
    "usage_modal",
    "usage_readout",
    "usage_slot",
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

/// Coordinator operations a running turn's lease defers, mirroring
/// `defers_behind_lease` in the coordinator. One of these issued while a turn
/// holds the lease waits for the turn to end, so awaiting it on the
/// event-loop thread blocks every frame behind that turn.
///
/// `set_option` and `update_model_values` are deliberately absent: the lease
/// serves them, so awaiting one does not wait for the turn.
const LEASE_BOUND_COORDINATOR_OPS: &[&str] = &[
    "acquire_lease",
    "change_directory",
    "close",
    "replace_history",
];

/// Awaiting a lease-bound coordinator operation on the event-loop thread
/// freezes every frame until the running turn ends, and deadlocks outright
/// when that turn is itself waiting on the UI -- a permission prompt or a
/// question is answered by the very loop that is blocked. The loop dispatches
/// these through `dispatch_session_op` and applies the result when it lands.
///
/// This has been reintroduced twice, in call sites a reviewer would have to
/// notice by eye, so it is enforced rather than remembered.
#[test]
fn the_ui_never_blocks_on_a_lease_bound_coordinator_operation() {
    let mut offenders = Vec::new();
    for path in rust_files(Path::new("maki-ui/src")).expect(SOURCE_SCAN_ERROR) {
        let text = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_rust::LANGUAGE.into())
            .expect("failed to initialize Rust parser");
        let tree = parser
            .parse(&text, None)
            .unwrap_or_else(|| panic!("failed to parse {}", path.display()));
        let mut found = Vec::new();
        blocking_coordinator_calls(tree.root_node(), text.as_bytes(), false, &mut found);
        for op in found {
            offenders.push(format!("{}: block_on(.. {op} ..)", path.display()));
        }
    }
    assert!(
        offenders.is_empty(),
        "the event loop must not await a coordinator operation; dispatch it \
         off-thread instead:\n  {}",
        offenders.join("\n  ")
    );
}

/// Collects lease-bound coordinator operations awaited inside a `block_on`.
fn blocking_coordinator_calls(node: Node<'_>, text: &[u8], in_test: bool, out: &mut Vec<String>) {
    let in_test = in_test || is_test_item(node, text);
    if in_test {
        return;
    }
    if node.kind() == "call_expression"
        && let Some(function) = node.child_by_field_name("function")
        && function
            .utf8_text(text)
            .is_ok_and(|name| name.rsplit("::").next() == Some("block_on"))
        && let Some(arguments) = node.child_by_field_name("arguments")
        && let Some(op) = awaited_coordinator_op(arguments, text)
    {
        out.push(op);
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        blocking_coordinator_calls(child, text, in_test, out);
    }
}

fn awaited_coordinator_op(node: Node<'_>, text: &[u8]) -> Option<String> {
    if node.kind() == "call_expression"
        && let Some(function) = node.child_by_field_name("function")
        && let Ok(name) = function.utf8_text(text)
        && let Some(method) = name.rsplit('.').next()
        && LEASE_BOUND_COORDINATOR_OPS.contains(&method)
    {
        return Some(method.to_owned());
    }
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .find_map(|child| awaited_coordinator_op(child, text))
}

#[test]
fn frontends_cannot_register_standard_commands() {
    for source in production_sources(FRONTEND_ROOTS).expect(SOURCE_SCAN_ERROR) {
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
fn rust_frontends_do_not_own_usage_presentation() {
    assert!(
        Path::new("plugins/usage/init.lua").is_file(),
        "bundled usage plugin is missing"
    );
    for source in production_sources(&["maki-ui/src", "maki-agent/src", "maki-commands/src"])
        .expect(SOURCE_SCAN_ERROR)
    {
        let path = source.path.to_string_lossy();
        for identifier in FORBIDDEN_RUST_USAGE_UI_IDENTIFIERS {
            assert!(
                !source.identifiers.iter().any(|name| name == identifier),
                "{path} contains retired Rust usage UI identifier `{identifier}`"
            );
        }
        assert!(
            !source.strings.iter().any(|value| value == "/usage"),
            "{path} contains retired Rust `/usage` command literal"
        );
    }
}

#[test]
fn acp_is_command_agnostic_proxy() {
    for source in production_sources(&["maki-acp/src"]).expect(SOURCE_SCAN_ERROR) {
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

fn production_sources(roots: &[&str]) -> Result<Vec<Source>, String> {
    let mut sources = Vec::new();
    for root in roots {
        for path in rust_files(Path::new(root))? {
            sources.push(parse_source(path)?);
        }
    }
    Ok(sources)
}

fn rust_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = fs::read_dir(root)
        .map_err(|error| format!("failed to read {}: {error}", root.display()))?;
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|error| format!("failed to read an entry in {}: {error}", root.display()))?;
        let path = entry.path();
        let metadata = fs::metadata(&path)
            .map_err(|error| format!("failed to inspect {}: {error}", path.display()))?;
        if metadata.is_dir() {
            files.extend(rust_files(&path)?);
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && path
                .file_name()
                .is_none_or(|name| name != "architecture_tests.rs")
        {
            files.push(path);
        }
    }
    Ok(files)
}

fn parse_source(path: PathBuf) -> Result<Source, String> {
    let text = fs::read_to_string(&path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    parse_source_text(path, &text)
}

fn parse_source_text(path: PathBuf, text: &str) -> Result<Source, String> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .map_err(|error| format!("failed to initialize Rust parser: {error}"))?;
    let tree = parser
        .parse(text, None)
        .ok_or_else(|| format!("failed to parse {}", path.display()))?;
    if tree.root_node().has_error() {
        return Err(format!("syntax error while parsing {}", path.display()));
    }
    let mut source = Source {
        path,
        identifiers: Vec::new(),
        calls: Vec::new(),
        strings: Vec::new(),
    };
    collect(tree.root_node(), text.as_bytes(), false, &mut source);
    Ok(source)
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

#[test]
fn source_scan_rejects_missing_roots() {
    let Err(error) = production_sources(&["missing-architecture-test-root"]) else {
        panic!("missing root was silently skipped");
    };

    assert!(error.contains("missing-architecture-test-root"), "{error}");
}

#[test]
fn source_scan_rejects_syntax_errors() {
    let path = PathBuf::from("broken.rs");
    let Err(error) = parse_source_text(path.clone(), "fn broken(") else {
        panic!("syntax error was silently accepted");
    };

    assert!(error.contains(path.to_str().unwrap()), "{error}");
}
