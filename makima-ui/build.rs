use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let themes_dir = Path::new("src/themes");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed={}", themes_dir.display());

    let mut files: Vec<PathBuf> = fs::read_dir(themes_dir)
        .expect("read src/themes")
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "toml"))
        .collect();
    files.sort();

    let mut out = String::from("pub const BUNDLED_THEMES: &[ThemeEntry] = &[\n");
    for path in &files {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("theme file name");
        out.push_str(&format!(
            r#"    ThemeEntry {{ name: {name:?}, toml: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/themes/{name}.toml")) }},
"#
        ));
    }
    out.push_str("];\n");

    let dest = Path::new(&std::env::var("OUT_DIR").expect("OUT_DIR")).join("bundled_themes.rs");
    fs::write(dest, out).expect("write bundled_themes.rs");
}
