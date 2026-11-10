use std::fs;
use std::path::Path;
use std::process::Command;

const TEXT_EXTENSIONS: &[&str] = &[
    "rs",
    "lua",
    "md",
    "toml",
    "yml",
    "yaml",
    "ts",
    "py",
    "sh",
    "ps1",
    "html",
    "json",
    "txt",
    "nix",
    "svg",
    "webmanifest",
];
const SPECIAL_FILES: &[&str] = &[
    "justfile",
    "Cargo.lock",
    "clippy.toml",
    ".gitattributes",
    ".gitignore",
    ".githooks/pre-commit",
    "LICENSE",
    "scripts/providers/openai-oauth",
];
const SKIP_PREFIXES: &[&str] = &["target/", "site/_build/", ".agents/makima-plans/"];
const SKIP_CONTENT_FILES: &[&str] = &[
    "acp-command-effect-projection-handover.md",
    ".github/workflows/pages.yml",
    "tests/rename_identity.rs",
];
const UPSTREAM_ALLOWLIST: &[(&str, usize, &str)] = &[
    (
        "README.md",
        3,
        "a fucked up fork of [maki](https://github.com/tontinton/maki):",
    ),
    ("README.md", 7, "_maki is good!"),
    ("README.md", 9, "## things makima does on top of maki"),
    (
        "README.md",
        166,
        "> DISCLAIMER: >90% of code in maki was written by maki,",
    ),
    (
        "site/index.html",
        830,
        "a fork of <a href=\"https://github.com/tontinton/maki\"",
    ),
    ("site/index.html", 977, "Everything upstream maki does,"),
    (
        "site/index.html",
        1221,
        "makima is a fork of <a href=\"https://github.com/tontinton/maki\"",
    ),
];
const SYNC_SKILL: &str = ".agents/skills/lunamaki-sync-with-upstream/SKILL.md";
const NEGATIVE_FIXTURE: &str = "makima-lua/tests/runtime_alignment.rs";
const NEGATIVE_FIXTURE_LINE: &str = "assert(maki == nil, \"legacy maki global present\")";

fn identity_tokens(text: &str) -> Vec<usize> {
    let lower = text.to_ascii_lowercase();
    let mut positions: Vec<usize> = lower
        .match_indices("maki")
        .filter_map(|(index, _)| {
            let before = text[..index].chars().next_back();
            let after = text[index + 4..].chars().next();
            (before.is_none_or(|c| !c.is_alphabetic()) && after.is_none_or(|c| !c.is_alphabetic()))
                .then_some(index)
        })
        .collect();
    positions.extend(text.match_indices("Maki").filter_map(|(index, _)| {
        let before = text[..index].chars().next_back();
        let after = text[index + 4..].chars().next();
        (before.is_none_or(|c| !c.is_alphabetic()) && !after.is_some_and(char::is_lowercase))
            .then_some(index)
    }));
    positions.sort_unstable();
    positions.dedup();
    positions
}

fn has_identity_token(text: &str) -> bool {
    !identity_tokens(text).is_empty()
}

fn has_double_replacement(text: &str) -> bool {
    text.to_ascii_lowercase().contains("makimama")
}

fn is_text_file(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    SPECIAL_FILES.contains(&name)
        || path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| TEXT_EXTENSIONS.contains(&extension))
}

fn is_skipped(path: &str) -> bool {
    SKIP_PREFIXES.iter().any(|prefix| path.starts_with(prefix))
        || SKIP_CONTENT_FILES.contains(&path)
}

fn is_allowed(path: &str, line_number: usize, line: &str) -> bool {
    if path == NEGATIVE_FIXTURE && line.contains(NEGATIVE_FIXTURE_LINE) {
        return true;
    }
    if path == SYNC_SKILL
        && (line.contains("upstream maki")
            || line.contains("a fork of maki")
            || line.contains("tontinton/maki"))
    {
        return true;
    }
    UPSTREAM_ALLOWLIST
        .iter()
        .any(|(allowed_path, allowed_line, fragment)| {
            path == *allowed_path && line_number == *allowed_line && line.contains(fragment)
        })
}

fn git_files(args: &[&str]) -> Vec<String> {
    let output = Command::new("git")
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("git {} failed: {error}", args.join(" ")));
    assert!(
        output.status.success(),
        "git {} failed with {}",
        args.join(" "),
        output.status
    );
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect()
}

fn repository_files() -> Vec<String> {
    let mut files = git_files(&["ls-files", "-z"]);
    files.extend(git_files(&[
        "ls-files",
        "--others",
        "--exclude-standard",
        "-z",
    ]));
    files.sort();
    files.dedup();
    files
}

fn scan_line(path: &str, line_number: usize, line: &str, findings: &mut Vec<String>) {
    if is_allowed(path, line_number, line) {
        return;
    }
    if has_identity_token(line) || has_double_replacement(line) {
        findings.push(format!("{path}:{line_number}: {}", line.trim()));
    }
    if line.contains("lun-4/maki") && !line.contains("lun-4/makima") {
        findings.push(format!("{path}:{line_number}: stale fork origin"));
    }
}

fn scan_repository() -> Vec<String> {
    let mut findings = Vec::new();
    for path in repository_files() {
        if is_skipped(&path) {
            continue;
        }
        if has_identity_token(&path) {
            findings.push(format!("{path}: stale identity token in path"));
        }
        let file = Path::new(&path);
        if !file.is_file() || !is_text_file(file) {
            continue;
        }
        let Ok(source) = fs::read_to_string(file) else {
            continue;
        };
        for (index, line) in source.lines().enumerate() {
            scan_line(&path, index + 1, line, &mut findings);
        }
    }
    findings
}

fn strip_escapes(source: &str) -> String {
    let mut out = String::new();
    let mut chars = source.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\u{1b}' => match chars.next() {
                Some('[') => {
                    for next in chars.by_ref() {
                        if ('@'..='~').contains(&next) {
                            break;
                        }
                    }
                }
                Some(']') => {
                    while let Some(next) = chars.next() {
                        if next == '\u{7}' {
                            break;
                        }
                        if next == '\u{1b}' && chars.peek() == Some(&'\\') {
                            chars.next();
                            break;
                        }
                    }
                }
                Some(_) => {}
                None => break,
            },
            '\u{8}' => {
                out.pop();
            }
            '\r' => out.truncate(out.rfind('\n').map_or(0, |index| index + 1)),
            '\n' => out.push('\n'),
            '\t' => out.push(' '),
            ch if !ch.is_control() => out.push(ch),
            _ => {}
        }
    }
    out
}

fn cast_terminal_text(source: &str) -> String {
    let mut out = String::new();
    for line in source.lines().skip(1) {
        let Ok(event) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(event) = event.as_array() else {
            continue;
        };
        if event.get(1).and_then(|kind| kind.as_str()) == Some("o")
            && let Some(payload) = event.get(2).and_then(|payload| payload.as_str())
        {
            out.push_str(&strip_escapes(payload));
        }
    }
    out
}

fn scan_cast(path: &Path, findings: &mut Vec<String>) {
    let Ok(source) = fs::read_to_string(path) else {
        return;
    };
    let text = cast_terminal_text(&source);
    if has_identity_token(&text) || has_double_replacement(&text) {
        findings.push(format!(
            "{}: cast text contains stale identity",
            path.display()
        ));
    }
}

fn scan_json_strings(path: &Path, value: &serde_json::Value, findings: &mut Vec<String>) {
    match value {
        serde_json::Value::String(text)
            if has_identity_token(text) || has_double_replacement(text) =>
        {
            findings.push(format!(
                "{}: JSON string contains stale identity",
                path.display()
            ));
        }
        serde_json::Value::Array(values) => {
            for value in values {
                scan_json_strings(path, value, findings);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                scan_json_strings(path, value, findings);
            }
        }
        _ => {}
    }
}

fn source_for_build_path(path: &Path) -> String {
    let relative = path.strip_prefix("site/_build").unwrap_or(path);
    if relative == Path::new("index.html") {
        return "site/index.html".to_owned();
    }
    format!("site/_build/{}", relative.display())
}

fn scan_build_tree(root: &Path) -> Vec<String> {
    let mut findings = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if metadata.is_dir() {
            if let Ok(entries) = fs::read_dir(&path) {
                pending.extend(entries.flatten().map(|entry| entry.path()));
            }
            continue;
        }
        if path.extension().and_then(|extension| extension.to_str()) == Some("cast") {
            scan_cast(&path, &mut findings);
            continue;
        }
        if !is_text_file(&path) {
            continue;
        }
        let Ok(source) = fs::read_to_string(&path) else {
            continue;
        };
        if path.file_name().and_then(|name| name.to_str()) == Some("search.json") {
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&source) {
                scan_json_strings(&path, &value, &mut findings);
            }
            continue;
        }
        let source_path = source_for_build_path(&path);
        for (index, line) in source.lines().enumerate() {
            scan_line(&source_path, index + 1, line, &mut findings);
        }
    }
    findings
}

#[test]
fn identity_matcher_cases() {
    for case in [
        "maki",
        "MAKI",
        "maki_",
        "_maki_",
        "maki.fn",
        "maki>",
        "tontinton/maki",
        "MakiId",
        "MakiAgent",
        "scripts/tbench_maki_agent.py",
        "plugins/lib/maki",
    ] {
        assert!(has_identity_token(case), "{case:?} must match");
    }
    for case in ["makima", "making", "lunamaki", "makiconf", "makimama"] {
        assert!(!has_identity_token(case), "{case:?} must not match");
    }
}

#[test]
fn no_stale_identity_tokens() {
    let findings = scan_repository();
    assert!(
        findings.is_empty(),
        "stale identity tokens remain:\n{}",
        findings.join("\n")
    );
}

#[test]
fn cast_text_is_clean() {
    let path = Path::new("site/demo.cast");
    if !path.exists() {
        return;
    }
    let mut findings = Vec::new();
    scan_cast(path, &mut findings);
    assert!(
        findings.is_empty(),
        "cast text contains stale identity:\n{}",
        findings.join("\n")
    );
}

#[test]
fn site_build_is_clean() {
    let root = Path::new("site/_build");
    let required = std::env::var("MAKIMA_SITE_CHECK").as_deref() == Ok("1");
    if !root.exists() {
        assert!(!required, "MAKIMA_SITE_CHECK=1 requires site/_build");
        return;
    }
    let findings = scan_build_tree(root);
    assert!(
        findings.is_empty(),
        "site build contains stale identity:\n{}",
        findings.join("\n")
    );
}

#[test]
fn no_stale_fork_origin() {
    let findings = scan_repository()
        .into_iter()
        .filter(|finding| finding.contains("lun-4/maki"))
        .collect::<Vec<_>>();
    assert!(
        findings.is_empty(),
        "stale lun-4/maki origin remains:\n{}",
        findings.join("\n")
    );
}
