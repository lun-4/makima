use std::fs;
use std::path::{Path, PathBuf};

use tree_sitter::{Node, Parser};

const FRONTEND_ROOTS: &[&str] = &["makima-ui/src", "makima-acp/src", "src"];
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
/// Built-in provider ids, which Rust must not name: every provider is
/// registered from Lua by `plugins/providers`.
const PROVIDER_IDS: &[&str] = &[
    "anthropic",
    "aperture",
    "bedrock",
    "copilot",
    "deepseek",
    "google",
    "llama-cpp",
    "mistral",
    "ollama",
    "openai",
    "openai-codex",
    "opencode",
    "opencode-go",
    "openrouter",
    "regolo",
    "requesty",
    "synthetic",
    "tensorx",
    "vertex",
    "zai",
];
const VENDOR_MODEL_PREFIXES: &[&str] = &["claude-", "gpt-", "gemini-"];
const VENDOR_ENV_PREFIXES: &[&str] = &["AWS_", "CLAUDE_", "GEMINI_"];
const PROVIDER_MODULES: &str = "plugins/providers/providers";
const SHARED_HOSTS: &[&str] = &["127.0.0.1", "localhost", "makima.ln4.net"];
const NON_PROVIDER_HOST_VALUES: &[&str] = &[
    "Rewrite bash commands with [rtk](https://github.com/rtk-ai/rtk) when it is installed",
    "https://api.github.com/repos/lun-4/makima/releases/latest",
];
const URL_SCHEMES: &[&str] = &["https://", "http://"];
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
    for source in production_sources(&["makima-ui/src", "makima-agent/src", "makima-commands/src"])
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
    for source in production_sources(&["makima-acp/src"]).expect(SOURCE_SCAN_ERROR) {
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

/// Every host a bundled provider module names, less the shared ones.
fn provider_hosts() -> Vec<String> {
    let mut hosts = Vec::new();
    for entry in fs::read_dir(PROVIDER_MODULES).unwrap() {
        let module = fs::read_to_string(entry.unwrap().path()).unwrap();
        for rest in URL_SCHEMES
            .iter()
            .flat_map(|scheme| module.split(scheme).skip(1))
        {
            let host: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
                .collect();
            if host.contains('.')
                && !host.ends_with('.')
                && !SHARED_HOSTS.iter().any(|shared| host.ends_with(shared))
            {
                hosts.push(host);
            }
        }
    }
    hosts.sort();
    hosts.dedup();
    hosts
}

/// The `src` directory of the root package and of every workspace member.
fn workspace_source_roots() -> Vec<String> {
    let mut roots = vec!["src".to_owned()];
    for entry in fs::read_dir(".").unwrap() {
        let path = entry.unwrap().path();
        if path.join("Cargo.toml").is_file() && path.join("src").is_dir() {
            roots.push(path.join("src").to_string_lossy().into_owned());
        }
    }
    roots
}

fn provider_knowledge(value: &str, hosts: &[String]) -> Option<String> {
    let env_prefixes = PROVIDER_IDS
        .iter()
        .map(|id| format!("{}_", id.to_uppercase().replace('-', "_")))
        .chain(VENDOR_ENV_PREFIXES.iter().map(|prefix| prefix.to_string()));
    if let Some(id) = PROVIDER_IDS
        .iter()
        .find(|id| value == **id || value.starts_with(&format!("{id}/")))
    {
        Some(format!("provider id `{id}`"))
    } else if let Some(prefix) = VENDOR_MODEL_PREFIXES
        .iter()
        .find(|p| value.starts_with(**p))
    {
        Some(format!("model prefix `{prefix}`"))
    } else if let Some(prefix) = env_prefixes
        .into_iter()
        .find(|p| value.contains(p.as_str()))
    {
        Some(format!("env var prefix `{prefix}`"))
    } else if NON_PROVIDER_HOST_VALUES.contains(&value) {
        None
    } else {
        hosts
            .iter()
            .find(|host| value.contains(host.as_str()))
            .map(|host| format!("endpoint host `{host}`"))
    }
}

#[test]
fn rust_source_names_no_provider() {
    let hosts = provider_hosts();
    assert!(!hosts.is_empty(), "no hosts found in {PROVIDER_MODULES}");
    let roots = workspace_source_roots();
    let roots: Vec<&str> = roots.iter().map(String::as_str).collect();
    let mut found = Vec::new();
    for source in production_sources(&roots).expect(SOURCE_SCAN_ERROR) {
        for value in &source.strings {
            if let Some(knowledge) = provider_knowledge(value, &hosts) {
                found.push(format!(
                    "{}: {knowledge} in {value:?}",
                    source.path.display()
                ));
            }
        }
    }
    assert!(
        found.is_empty(),
        "provider knowledge in Rust:\n{}",
        found.join("\n")
    );
}

#[test]
fn makima_commands_dependency_direction_is_neutral() {
    let manifest = fs::read_to_string("makima-commands/Cargo.toml").unwrap();
    for dependency in [
        "makima-agent",
        "makima-ui",
        "makima-acp",
        "makima-lua",
        "makima-providers",
        "makima-config",
        "makima-storage",
    ] {
        assert!(
            !manifest.lines().any(|line| {
                line.split_once('=')
                    .is_some_and(|(name, _)| name.trim() == dependency)
            }),
            "makima-commands depends on `{dependency}`"
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
                .is_none_or(|name| name != "architecture_tests.rs" && name != "tests.rs")
            && !path
                .components()
                .any(|component| component.as_os_str() == "tests")
            && !path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("_tests.rs"))
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
        "string_literal" | "raw_string_literal" => {
            if let Ok(literal) = node.utf8_text(text) {
                let value = literal.trim_start_matches('r').trim_matches('#');
                if let Some(value) = value.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
                    source.strings.push(value.to_owned());
                }
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
    if !node.kind().ends_with("_item") || node.kind() == "attribute_item" {
        return false;
    }
    let mut sibling = node.prev_named_sibling();
    while let Some(attribute) = sibling {
        if attribute.kind() != "attribute_item" {
            break;
        }
        if attribute.utf8_text(text).is_ok_and(attribute_is_test) {
            return true;
        }
        sibling = attribute.prev_named_sibling();
    }
    false
}

fn attribute_is_test(attribute: &str) -> bool {
    let Some(attribute) = attribute.trim().strip_prefix("#[") else {
        return false;
    };
    let name_end = attribute.find(['(', ']', ' ']).unwrap_or(attribute.len());
    match &attribute[..name_end] {
        "test" | "test_case" => true,
        "cfg" => attribute[name_end..]
            .trim()
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(")]"))
            .is_some_and(|predicate| predicate.trim() == "test"),
        "cfg_attr" => attribute[name_end..]
            .trim()
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(")]"))
            .and_then(|value| value.split_once(','))
            .is_some_and(|(predicate, applied)| {
                predicate.trim() == "test" && matches!(applied.trim(), "test" | "test_case")
            }),
        _ => false,
    }
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

#[test_case::test_case("#[test]\nfn test_only() { \"provider/model\"; }", false ; "test")]
#[test_case::test_case("#[test_case(1)]\nfn test_only(_: u8) { \"provider/model\"; }", false ; "test_case")]
#[test_case::test_case("#[cfg(test)]\nmod tests { fn test_only() { \"provider/model\"; } }", false ; "cfg_test")]
#[test_case::test_case("#[cfg_attr(test, test)]\nfn test_only() { \"provider/model\"; }", false ; "cfg_attr_test")]
#[test_case::test_case("#[cfg(not(test))]\nfn production() { \"provider/model\"; }", true ; "cfg_not_test")]
#[test_case::test_case("#[cfg(any(test, feature = \"extra\"))]\nfn production() { \"provider/model\"; }", true ; "cfg_may_be_production")]
#[test_case::test_case("#[cfg_attr(not(test), test)]\nfn production() { \"provider/model\"; }", true ; "cfg_attr_not_test")]
#[test_case::test_case("#[cfg_attr(test, inline)]\nfn production() { \"provider/model\"; }", true ; "cfg_attr_non_test")]
#[test_case::test_case("#[doc = \"test\"]\nfn production() { \"provider/model\"; }", true ; "non_test_attribute")]
fn source_scan_filters_only_test_attributes(source: &str, collected: bool) {
    let source = parse_source_text(PathBuf::from("attestation.rs"), source).unwrap();
    assert_eq!(
        source.strings.contains(&"provider/model".to_owned()),
        collected
    );
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
