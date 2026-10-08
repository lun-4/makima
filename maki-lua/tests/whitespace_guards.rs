use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use maki_agent::tools::{ToolRegistry, test_support::stub_ctx};
use maki_agent::{AgentMode, ToolOutput};
use maki_config::PluginsConfig;
use maki_lua::PluginHost;
use serde_json::{Value, json};
use test_case::test_case;

const CAP: usize = 80;
const BLANK_REPEATS: usize = CAP + 1;
const SEED: &str = "first\nold\nend\n";
const READ_MARKER: &str = "abc[line truncated, +42 bytes]";
const READ_MARKER_ERROR: &str = "marker from truncated read or grep output";
const PREVIEW_MARKER: &str = "abc[line cut: first 3 of 900 bytes]";
const PREVIEW_MARKER_ERROR: &str = "marker from a cut tool-output preview";
const NO_MATCH: &str = "old_string not found in file";
const FAILED_SECOND_EDIT: &str = "edits[1]";

fn edit_tools_host() -> (Arc<ToolRegistry>, PluginHost) {
    let reg = Arc::new(ToolRegistry::new());
    let mut host = PluginHost::new(Arc::clone(&reg)).unwrap();
    let mut config = PluginsConfig::from_plugins(HashMap::new());
    config.opts.insert(
        "edit".to_owned(),
        json!({ "edit_lines": true, "insert_lines": true })
            .as_object()
            .unwrap()
            .clone(),
    );
    host.load_builtins(&config).unwrap();
    (reg, host)
}

fn execute(reg: &ToolRegistry, tool: &str, input: Value) -> Result<ToolOutput, String> {
    let invocation = reg.get(tool).unwrap().tool.parse(&input).unwrap();
    let mut ctx = stub_ctx(&AgentMode::Build);
    ctx.config.max_line_bytes = CAP;
    smol::block_on(invocation.execute(&ctx)).output
}

fn marker_input(tool: &str, path: &Path, marker: &str) -> (&'static str, Value) {
    match tool {
        "write_new" => ("write", json!({"path": path, "content": marker})),
        "write" => (
            "write",
            json!({"path": path, "content": format!("first\n{marker}\nend\n")}),
        ),
        "edit" => (
            "edit",
            json!({"path": path, "old_string": "old", "new_string": marker}),
        ),
        "multiedit" => (
            "multiedit",
            json!({"path": path, "edits": [
                {"old_string": "first", "new_string": "FIRST"},
                {"old_string": "old", "new_string": marker},
            ]}),
        ),
        "edit_lines" => (
            "edit_lines",
            json!({"path": path, "start": 2, "end": 2, "new_string": marker}),
        ),
        "insert_lines" => (
            "insert_lines",
            json!({"path": path, "line": 1, "new_string": marker}),
        ),
        _ => panic!("unknown mutation tool: {tool}"),
    }
}

#[test_case("write_new" ; "new_file_write")]
#[test_case("write" ; "replacement_write")]
#[test_case("edit" ; "edit")]
#[test_case("multiedit" ; "multiedit_second_edit")]
#[test_case("edit_lines" ; "edit_lines")]
#[test_case("insert_lines" ; "insert_lines")]
fn marker_additions_are_rejected_without_mutation(tool: &str) {
    let (reg, _host) = edit_tools_host();
    for (marker, expected_error) in [
        (READ_MARKER, READ_MARKER_ERROR),
        (PREVIEW_MARKER, PREVIEW_MARKER_ERROR),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("guard.txt");
        if tool != "write_new" {
            std::fs::write(&path, SEED).unwrap();
        }
        let (name, input) = marker_input(tool, &path, marker);
        let result = execute(&reg, name, input);
        if tool == "write_new" {
            assert!(!path.exists(), "rejected write created the file");
        } else {
            assert_eq!(std::fs::read(&path).unwrap(), SEED.as_bytes());
        }
        let err = result.unwrap_err();
        assert!(err.contains(expected_error), "{tool}: {err}");
    }
}

#[test]
fn fuzzy_blank_rejection_is_atomic_and_crlf_edit_is_allowed() {
    let (reg, _host) = edit_tools_host();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("blank.txt");
    let blank = " ".repeat(BLANK_REPEATS);
    let before = format!("outside\nBEGIN\n{blank}\nEND\n");
    std::fs::write(&path, &before).unwrap();
    let result = execute(
        &reg,
        "multiedit",
        json!({"path": path, "edits": [
            {"old_string": "outside", "new_string": "OUTSIDE"},
            {"old_string": " BEGIN\n\n END", "new_string": "replacement"},
        ]}),
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
    let err = result.unwrap_err();
    assert!(err.contains(NO_MATCH), "{err}");
    assert!(err.contains(FAILED_SECOND_EDIT), "{err}");

    let path = dir.path().join("crlf.txt");
    let tabs = "\t".repeat(BLANK_REPEATS);
    let before = format!("outside\r\nBEGIN\r\n{tabs}\r\nEND\r\n");
    std::fs::write(&path, &before).unwrap();
    execute(
        &reg,
        "edit",
        json!({"path": path, "old_string": format!(" BEGIN\n{tabs}\n END"), "new_string": "replacement"}),
    )
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "outside\r\nreplacement\r\n"
    );
}
