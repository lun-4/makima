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
    use test_case::test_case;

    const SECTION_SEPARATOR: &str = "\n---\n";
    const MODULE_HEADING_PREFIX: &str = "\n## ";

    fn normalized(text: &str) -> String {
        text.split_whitespace().collect::<Vec<_>>().join(" ")
    }

    #[test_case("maki.api.register_command()", "maki.api.register_command({spec})"; "register_command")]
    #[test_case("maki.keymap.set()", "maki.keymap.set({mode}, {lhs}, {rhs}, {opts?})"; "keymap_set")]
    #[test_case("maki.api.run_command()", "maki.api.run_command({ctx?}, {cmdline})"; "run_command")]
    #[test_case("maki.agent.spawn()", "maki.agent.spawn({ctx}, {opts})"; "agent_spawn")]
    #[test_case("Agent:status()", "Agent:status({ctx})"; "agent_status")]
    #[test_case("Agent:config()", "Agent:config({ctx})"; "agent_config")]
    #[test_case("Agent:available_models()", "Agent:available_models({ctx})"; "agent_available_models")]
    #[test_case("Agent:transcript()", "Agent:transcript({ctx}, {opts})"; "agent_transcript")]
    #[test_case("Agent:send()", "Agent:send({ctx}, {message}, {opts})"; "agent_send")]
    #[test_case("Agent:on_turn_start()", "Agent:on_turn_start({ctx}, {handler})"; "agent_on_turn_start")]
    #[test_case("Agent:on_turn_end()", "Agent:on_turn_end({ctx}, {handler})"; "agent_on_turn_end")]
    #[test_case("Agent:on_idle()", "Agent:on_idle({ctx}, {handler})"; "agent_on_idle")]
    #[test_case("Agent:on_config_change()", "Agent:on_config_change({ctx}, {handler})"; "agent_on_config_change")]
    #[test_case("Agent:on_close()", "Agent:on_close({ctx}, {handler})"; "agent_on_close")]
    #[test_case("LuaCtx:agent()", "LuaCtx:agent()"; "context_agent")]
    #[test_case("LuaCtx:model()", "LuaCtx:model()"; "context_model")]
    fn generated_docs_render_agent_api_sections(heading: &str, signature: &str) {
        let page = generate();
        let start = page
            .find(&format!("### `{heading}`"))
            .expect("API section must be rendered");
        let end = page[start..]
            .find(SECTION_SEPARATOR)
            .map(|offset| start + offset)
            .expect("API section must have a separator");
        assert!(
            page[start..end].contains(&format!("```lua\n{signature}\n```")),
            "{heading} section must render its signature {signature:?}"
        );
    }

    #[test_case("maki.agent.AgentTurn"; "agent_turn")]
    #[test_case("maki.agent.AgentSubscription"; "agent_subscription")]
    fn generated_docs_render_agent_class_description(name: &str) {
        let module = maki_lua::docs::api_docs()
            .into_iter()
            .find(|module| module.name == name)
            .expect("class metadata must be registered");
        let page = generate();
        let start = page
            .find(&format!("{MODULE_HEADING_PREFIX}{name} "))
            .expect("class section must be rendered");
        let end = page[start + MODULE_HEADING_PREFIX.len()..]
            .find(MODULE_HEADING_PREFIX)
            .map_or(page.len(), |offset| {
                start + MODULE_HEADING_PREFIX.len() + offset
            });
        assert!(
            normalized(&page[start..end]).contains(&normalized(module.desc)),
            "{name} section must render the class description"
        );
    }

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
            .find(SECTION_SEPARATOR)
            .map(|offset| start + offset)
            .expect("register_command section must have a separator");
        let section = &page[start..end];
        let rendered = normalized(section);
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
