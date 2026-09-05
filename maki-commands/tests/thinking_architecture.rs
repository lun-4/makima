use std::fs;
use std::path::{Path, PathBuf};

use maki_domain::{Effort, ThinkingConfig};

fn source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                files.push(path);
            }
        }
    }
    files
}

fn declares(source: &str, type_name: &str) -> bool {
    source.lines().any(|line| {
        let words = line.split_whitespace().collect::<Vec<_>>();
        words.windows(2).any(|window| {
            matches!(window[0], "enum" | "struct" | "type")
                && window[1].trim_matches(|character: char| {
                    !character.is_ascii_alphanumeric() && character != '_'
                }) == type_name
        })
    })
}

fn identity<T>(value: T) -> T {
    value
}

#[test]
fn thinking_types_have_one_source_of_truth() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut declarations = Vec::new();

    for source_root in fs::read_dir(workspace)
        .unwrap()
        .map(|entry| entry.unwrap().path())
    {
        if source_root.file_name().is_some_and(|name| name == "target") {
            continue;
        }
        let source_root = source_root.join("src");
        if !source_root.is_dir() {
            continue;
        }
        for file in source_files(&source_root) {
            let source = fs::read_to_string(&file).unwrap();
            for type_name in ["ThinkingConfig", "Effort"] {
                if declares(&source, type_name) {
                    declarations.push((type_name, file.clone()));
                }
            }
        }
    }

    declarations.sort();
    assert_eq!(
        declarations,
        vec![
            ("Effort", workspace.join("maki-domain/src/maki_thinking.rs")),
            (
                "ThinkingConfig",
                workspace.join("maki-domain/src/maki_thinking.rs")
            ),
        ]
    );
}

#[test]
fn compatibility_imports_share_the_domain_types() {
    let config: ThinkingConfig = identity(ThinkingConfig::Off);
    let effort: Effort = identity(Effort::High);
    assert_eq!(config, ThinkingConfig::Off);
    assert_eq!(effort, Effort::High);
}
