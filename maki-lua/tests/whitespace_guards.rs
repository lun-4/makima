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
const READ_MARKER_ERROR: &str = "marker from truncated read or grep output";
const PREVIEW_MARKER_ERROR: &str = "marker from a cut tool-output preview";
const NO_MATCH: &str = "old_string not found in file";
const FAILED_SECOND_EDIT: &str = "edits[1]";
const MARKERS: [(&str, &str); 3] = [
    ("abc[line truncated, +42 bytes]", READ_MARKER_ERROR),
    ("abc[line truncated]", READ_MARKER_ERROR),
    ("abc[line cut: first 3 of 900 bytes]", PREVIEW_MARKER_ERROR),
];
const SUFFIXES: [&str; 4] = [
    "\u{a0}",
    "\u{2003}",
    "\u{202f}",
    " \t\u{a0}\u{2003}\u{202f}\t ",
];

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

fn mutation_input(
    tool: &str,
    path: &Path,
    before: &str,
    replacement: &str,
) -> (&'static str, Value) {
    match tool {
        "write_new" | "write" => (
            "write",
            json!({"path": path, "content": before.replace("\nold\n", &format!("\n{replacement}\n"))}),
        ),
        "edit" => (
            "edit",
            json!({"path": path, "old_string": "old", "new_string": replacement}),
        ),
        "multiedit" => (
            "multiedit",
            json!({"path": path, "edits": [
                {"old_string": "first\n", "new_string": "FIRST\n"},
                {"old_string": "old", "new_string": replacement},
            ]}),
        ),
        "edit_lines" => {
            let line = before.lines().position(|line| line == "old").unwrap() + 1;
            (
                "edit_lines",
                json!({"path": path, "start": line, "end": line, "new_string": replacement}),
            )
        }
        "insert_lines" => (
            "insert_lines",
            json!({"path": path, "line": 1, "new_string": replacement}),
        ),
        _ => panic!("unknown mutation tool: {tool}"),
    }
}

fn expected_mutation(tool: &str, before: &str, replacement: &str) -> String {
    if tool == "insert_lines" {
        let (first, rest) = before.split_once('\n').unwrap();
        format!("{first}\n{replacement}\n{rest}")
    } else {
        let after = before.replace("\nold\n", &format!("\n{replacement}\n"));
        if tool == "multiedit" {
            after.replace("first\n", "FIRST\n")
        } else {
            after
        }
    }
}

#[test_case("write_new" ; "new_write")]
#[test_case("write" ; "replacement_write")]
#[test_case("edit" ; "edit")]
#[test_case("multiedit" ; "multiedit_atomic")]
#[test_case("edit_lines" ; "edit_lines")]
#[test_case("insert_lines" ; "insert_lines")]
fn unicode_marker_addition_is_rejected_without_mutation(tool: &str) {
    let (reg, _host) = edit_tools_host();
    for (marker, error) in MARKERS {
        for suffix in SUFFIXES {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("guard.txt");
            if tool != "write_new" {
                std::fs::write(&path, SEED).unwrap();
            }
            let line = format!("{marker}{suffix}");
            let (name, input) = mutation_input(tool, &path, SEED, &line);
            let result = execute(&reg, name, input);
            if tool == "write_new" {
                assert!(
                    !path.exists(),
                    "{tool} created a file for {line:?}: {result:?}"
                );
            } else {
                assert_eq!(
                    std::fs::read(&path).unwrap(),
                    SEED.as_bytes(),
                    "{tool} mutated the file for {line:?}: {result:?}"
                );
            }
            let err = result.unwrap_err();
            assert!(err.contains(error), "{tool}, {line:?}: {err}");
        }
    }
}

#[test_case("write" ; "write")]
#[test_case("edit" ; "edit")]
#[test_case("multiedit" ; "multiedit")]
#[test_case("edit_lines" ; "edit_lines")]
#[test_case("insert_lines" ; "insert_lines")]
fn existing_literal_markers_are_preserved_but_cannot_be_duplicated(tool: &str) {
    let (reg, _host) = edit_tools_host();
    for (marker, error) in MARKERS {
        for suffix in SUFFIXES {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("literal.txt");
            let line = format!("{marker}{suffix}");
            let before = format!("{line}\n{line}\n{SEED}");
            std::fs::write(&path, &before).unwrap();
            let (name, input) = mutation_input(tool, &path, &before, "new");
            execute(&reg, name, input).unwrap();
            let preserved = expected_mutation(tool, &before, "new");
            assert_eq!(std::fs::read_to_string(&path).unwrap(), preserved);

            std::fs::write(&path, &before).unwrap();
            let (name, input) = mutation_input(tool, &path, &before, &line);
            let result = execute(&reg, name, input);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
            let err = result.unwrap_err();
            assert!(err.contains(error), "{tool}, {line:?}: {err}");

            if tool != "insert_lines" {
                let changed = format!("{marker}{suffix} ");
                let (name, input) = mutation_input(tool, &path, SEED, &changed);
                let result = execute(&reg, name, input);
                assert_eq!(std::fs::read_to_string(&path).unwrap(), before);
                let err = result.unwrap_err();
                assert!(err.contains(error), "{tool}, {changed:?}: {err}");
            }
        }
    }
}

#[test_case("write_new" ; "new_write")]
#[test_case("write" ; "replacement_write")]
#[test_case("edit" ; "edit")]
#[test_case("multiedit" ; "multiedit")]
#[test_case("edit_lines" ; "edit_lines")]
#[test_case("insert_lines" ; "insert_lines")]
fn unrecognized_brackets_are_not_rejected(tool: &str) {
    let (reg, _host) = edit_tools_host();
    for bracket in [
        "abc[line truncated, +oops bytes]",
        "abc[ordinary brackets]",
        "abc[line truncated] still content",
    ] {
        for suffix in SUFFIXES {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("brackets.txt");
            if tool != "write_new" {
                std::fs::write(&path, SEED).unwrap();
            }
            let line = format!("{bracket}{suffix}");
            let (name, input) = mutation_input(tool, &path, SEED, &line);
            execute(&reg, name, input).unwrap();
            assert_eq!(
                std::fs::read_to_string(&path).unwrap(),
                expected_mutation(tool, SEED, &line)
            );
        }
    }
}

#[test_case("write" ; "write")]
#[test_case("edit" ; "edit")]
#[test_case("multiedit" ; "multiedit")]
#[test_case("edit_lines" ; "edit_lines")]
fn removing_literal_markers_is_allowed(tool: &str) {
    let (reg, _host) = edit_tools_host();
    for (marker, _) in MARKERS {
        for suffix in SUFFIXES {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("remove.txt");
            let before = format!("first\n{marker}{suffix}\nend\n");
            std::fs::write(&path, &before).unwrap();
            let (name, input) = match tool {
                "write" => ("write", json!({"path": path, "content": SEED})),
                "edit" => (
                    "edit",
                    json!({"path": path, "old_string": format!("{marker}{suffix}"), "new_string": "old"}),
                ),
                "multiedit" => (
                    "multiedit",
                    json!({"path": path, "edits": [
                        {"old_string": format!("{marker}{suffix}"), "new_string": "old"},
                    ]}),
                ),
                _ => (
                    "edit_lines",
                    json!({"path": path, "start": 2, "end": 2, "new_string": "old"}),
                ),
            };
            execute(&reg, name, input).unwrap();
            assert_eq!(std::fs::read_to_string(&path).unwrap(), SEED);
        }
    }
}

fn fuzzy_input(tool: &str, path: &Path, old: &str, replacement: &str) -> Value {
    if tool == "multiedit" {
        json!({"path": path, "edits": [
            {"old_string": "outside", "new_string": "OUTSIDE"},
            {"old_string": old, "new_string": replacement},
        ]})
    } else {
        json!({"path": path, "old_string": old, "new_string": replacement})
    }
}

#[test_case("edit", " " ; "edit_spaces")]
#[test_case("edit", "\t" ; "edit_tabs")]
#[test_case("edit", " \t" ; "edit_mixed")]
#[test_case("multiedit", " " ; "multiedit_spaces_atomic")]
#[test_case("multiedit", "\t" ; "multiedit_tabs_atomic")]
#[test_case("multiedit", " \t" ; "multiedit_mixed_atomic")]
fn fuzzy_blank_coverage_rejection_is_atomic(tool: &str, unit: &str) {
    let (reg, _host) = edit_tools_host();
    let blank = unit.repeat(BLANK_REPEATS);
    let different = format!("{blank} ");
    let scenarios = [
        (blank.clone(), String::new()),
        (blank.clone(), " \t".to_owned()),
        (blank.clone(), different),
        (format!("{blank}\n{blank}"), blank.clone()),
        (
            format!("{blank}\n{}", "\t ".repeat(BLANK_REPEATS)),
            format!("{blank}\n{blank}"),
        ),
    ];
    for (file_lines, coverage) in scenarios {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blank.txt");
        let before = format!("outside\nBEGIN\n{file_lines}\nEND\n");
        let old = format!(" BEGIN\n{coverage}\n END");
        std::fs::write(&path, &before).unwrap();
        let result = execute(&reg, tool, fuzzy_input(tool, &path, &old, "replacement"));
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before.as_bytes(),
            "{tool}, {unit:?}, {coverage:?}: {result:?}"
        );
        let err = result.unwrap_err();
        assert!(err.contains(NO_MATCH), "{err}");
        if tool == "multiedit" {
            assert!(err.contains(FAILED_SECOND_EDIT), "{err}");
        }
    }
}

#[test_case("edit", " " ; "edit_spaces")]
#[test_case("edit", "\t" ; "edit_tabs")]
#[test_case("edit", " \t" ; "edit_mixed")]
#[test_case("multiedit", " " ; "multiedit_spaces")]
#[test_case("multiedit", "\t" ; "multiedit_tabs")]
#[test_case("multiedit", " \t" ; "multiedit_mixed")]
fn exact_blank_substrings_and_complete_fuzzy_coverage_are_allowed(tool: &str, unit: &str) {
    let (reg, _host) = edit_tools_host();
    let blank = unit.repeat(BLANK_REPEATS);
    for copies in [1, 2] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("covered.txt");
        let lines = vec![blank.clone(); copies].join("\n");
        let before = format!("outside\nBEGIN\n{lines}\nEND\n");
        let old = format!(" BEGIN\n{lines}\n END");
        std::fs::write(&path, &before).unwrap();
        execute(&reg, tool, fuzzy_input(tool, &path, &old, "replacement")).unwrap();
        let outside = if tool == "multiedit" {
            "OUTSIDE"
        } else {
            "outside"
        };
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            format!("{outside}\nreplacement\n")
        );
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("substring.txt");
    let prefix = unit.repeat(CAP / 2);
    let old = format!("BEGIN\n{prefix}");
    let before = format!("outside\nBEGIN\n{blank}\nEND\n");
    std::fs::write(&path, &before).unwrap();
    execute(&reg, tool, fuzzy_input(tool, &path, &old, "changed\nX")).unwrap();
    let outside = if tool == "multiedit" {
        "OUTSIDE"
    } else {
        "outside"
    };
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        format!("{outside}\nchanged\nX{}\nEND\n", &blank[prefix.len()..])
    );
}

#[test_case("edit" ; "edit")]
#[test_case("multiedit" ; "multiedit")]
fn fuzzy_guard_preserves_nonblank_short_crlf_and_escape_compatibility(tool: &str) {
    let (reg, _host) = edit_tools_host();
    let nonblank = "x".repeat(BLANK_REPEATS);
    let tabs = "\t".repeat(BLANK_REPEATS);
    let cases = [
        (
            format!("BEGIN\n  {nonblank} \nEND"),
            format!(" BEGIN\n{nonblank}\n END"),
            "\n",
        ),
        (
            "BEGIN\n \t\nEND".to_owned(),
            " BEGIN\n\n END".to_owned(),
            "\n",
        ),
        (
            format!("BEGIN\r\n{tabs}\r\nEND"),
            format!(" BEGIN\n{tabs}\n END"),
            "\r\n",
        ),
        (
            format!("BEGIN\n{tabs}\nEND"),
            format!("BEGIN\\n{}\\nEND", "\\t".repeat(BLANK_REPEATS)),
            "\n",
        ),
        (
            format!("BEGIN\n{nonblank}\nEND"),
            format!("BEGIN\\n{nonblank}\\nEND"),
            "\n",
        ),
    ];
    for (block, old, newline) in cases {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compatible.txt");
        let before = format!("outside{newline}{block}{newline}");
        std::fs::write(&path, &before).unwrap();
        execute(&reg, tool, fuzzy_input(tool, &path, &old, "replacement")).unwrap();
        let outside = if tool == "multiedit" {
            "OUTSIDE"
        } else {
            "outside"
        };
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            format!("{outside}{newline}replacement{newline}")
        );
    }
}
