use maki_lua::docs_render;

const FRONTMATTER: &str = r#"+++
title = "Lua API"
weight = 10
[extra]
group = "Reference"
+++

"#;

pub fn generate() -> String {
    format!("{FRONTMATTER}{}", docs_render::site_page())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MUTABLE_PATH_CONTRACT: &str = "same-path mutation is already in progress";
    const SERIALIZATION_CONTRACT: &str = "same-process per-path mutation serialization";

    #[test]
    fn generated_docs_contain_mutable_path_reentry_contract() {
        let page = generate();
        assert!(
            page.contains(SERIALIZATION_CONTRACT),
            "generated mutable_path docs must state the serialization contract"
        );
        assert!(
            page.contains(MUTABLE_PATH_CONTRACT) && page.contains("unsupported"),
            "generated mutable_path docs must state reentry is unsupported and name its error"
        );
    }
}
