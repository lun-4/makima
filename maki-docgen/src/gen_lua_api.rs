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

    #[test]
    fn generated_docs_contain_typed_arguments_contract() {
        let page = generate();
        let start = page
            .find("### `maki.api.register_command()`")
            .expect("generated docs must render register_command");
        let end = page[start..]
            .find("\n---\n")
            .map(|offset| start + offset)
            .expect("register_command section must have a separator");
        let section = &page[start..end];
        let rendered = section.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            !section.contains("| Completion is declared"),
            "completion prose must remain part of the arguments parameter"
        );
        let required = [
            "A required scalar consumes exactly one value.",
            "An optional scalar consumes zero or one value.",
            "A required variadic consumes one or more values.",
            "An optional variadic consumes zero or more values.",
            "The command parser outer-trims the argument remainder",
            "Unicode whitespace separates tokens outside quotes.",
            "adjacent quoted and unquoted fragments concatenate",
            "Backslashes are literal outside quotes and inside single quotes.",
            r#"only `\\"` and `\\\\` decode to a"#,
            "the outer-trimmed original argument remainder",
            "Raw commands do not set `opts.fargs` or `opts.values`.",
            "A missing optional scalar is `nil`.",
            "A missing optional variadic is an empty array.",
            "The inclusive exact range is `-9007199254740991` to",
            "Defaults are enum choices in the core",
            "`completion = false`,",
            "may set `mode` to `\"replace\"` or",
            "`\"extend\"` (`\"replace\"` is the default).",
            "Provider callbacks receive the typed argument name",
            "Raw commands do not have argument completion providers.",
            "Documentation and test example. It is not bundled and never copies files.",
            "name = \"/copy\"",
            "choices = { \"skip\", \"overwrite\" }",
            "records decoded values.",
        ];
        for text in required {
            assert!(
                rendered.contains(text),
                "rendered register_command docs must contain {text:?}"
            );
        }
        let order = [
            "A required scalar consumes exactly one value.",
            "The command parser outer-trims the argument remainder",
            "the outer-trimmed original argument remainder",
            "Defaults are enum choices in the core",
            "Documentation and test example. It is not bundled and never copies files.",
        ];
        let mut previous = 0;
        for text in order {
            let position = rendered
                .find(text)
                .unwrap_or_else(|| panic!("missing ordered rendered text {text:?}"));
            assert!(
                position >= previous,
                "rendered register_command text {text:?} is out of order"
            );
            previous = position;
        }
    }
}
