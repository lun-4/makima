use maki_agent::session_options::{
    DISABLED_VALUE, ENABLED_VALUE, SessionOptionCategory, SessionOptionsSnapshot,
    THINKING_OPTION_ID,
};
use maki_agent::{ModeId, ModeRegistry};

use agent_client_protocol_schema::{
    AgentCapabilities, Implementation, InitializeResponse, LoadSessionResponse, McpCapabilities,
    NewSessionResponse, PromptCapabilities, ProtocolVersion, SessionConfigOption,
    SessionConfigOptionCategory, SessionConfigSelectOption, SessionMode, SessionModeId,
    SessionModeState,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

const MODE_BUILD: &str = "build";
const MODE_PLAN: &str = "plan";

pub const MODEL_CONFIG_ID: &str = "model";

pub fn initialize_response() -> InitializeResponse {
    InitializeResponse::new(ProtocolVersion::V1)
        .agent_capabilities(
            AgentCapabilities::new()
                .load_session(true)
                .prompt_capabilities(PromptCapabilities::new().image(true).embedded_context(true))
                .mcp_capabilities(McpCapabilities::new().http(true)),
        )
        .auth_methods(vec![])
        .agent_info(Implementation::new("makima", VERSION))
}

pub fn mode_state(current: &str, modes: &ModeRegistry) -> SessionModeState {
    let mut list: Vec<SessionMode> = modes
        .list()
        .into_iter()
        .filter(|d| !matches!(d.id, ModeId::Custom(_)))
        .map(|d| {
            SessionMode::new(
                SessionModeId::from(d.id.key().to_string()),
                d.label.to_string(),
            )
        })
        .collect();
    for def in modes.list() {
        if let ModeId::Custom(name) = def.id {
            list.push(SessionMode::new(
                SessionModeId::from(name.to_string()),
                def.label.to_string(),
            ));
        }
    }
    SessionModeState::new(SessionModeId::from(current.to_string()), list)
}

pub fn new_session_response(session_id: &str, modes: &ModeRegistry) -> NewSessionResponse {
    NewSessionResponse::new(session_id.to_string()).modes(mode_state(MODE_BUILD, modes))
}

pub fn load_session_response(modes: &ModeRegistry) -> LoadSessionResponse {
    LoadSessionResponse::new().modes(mode_state(MODE_BUILD, modes))
}

pub fn session_config_options(
    snapshot: &SessionOptionsSnapshot,
    supports_boolean: bool,
) -> Vec<SessionConfigOption> {
    snapshot
        .options
        .iter()
        .map(|option| {
            let definition = &option.definition;
            let category = if definition.id.as_ref() == THINKING_OPTION_ID {
                SessionConfigOptionCategory::ThoughtLevel
            } else {
                match definition.category {
                    SessionOptionCategory::Model => SessionConfigOptionCategory::Model,
                    SessionOptionCategory::Mode => SessionConfigOptionCategory::Mode,
                }
            };
            let projected = if supports_boolean
                && definition.free_value.is_none()
                && definition.values.len() == 2
                && definition
                    .values
                    .iter()
                    .any(|value| value.value.as_ref() == ENABLED_VALUE)
                && definition
                    .values
                    .iter()
                    .any(|value| value.value.as_ref() == DISABLED_VALUE)
            {
                SessionConfigOption::boolean(
                    definition.id.to_string(),
                    definition.name.to_string(),
                    option.current_value.as_ref() == ENABLED_VALUE,
                )
            } else {
                let values: Vec<SessionConfigSelectOption> = definition
                    .values
                    .iter()
                    .map(|value| {
                        SessionConfigSelectOption::new(
                            value.value.to_string(),
                            value.name.to_string(),
                        )
                    })
                    .collect();
                SessionConfigOption::select(
                    definition.id.to_string(),
                    definition.name.to_string(),
                    option.current_value.to_string(),
                    values,
                )
            };
            projected
                .category(category)
                .description(definition.description.to_string())
        })
        .collect()
}

pub fn mode_id_to_agent_mode(mode_id: &str, modes: &ModeRegistry) -> Option<maki_agent::AgentMode> {
    match mode_id {
        MODE_BUILD => Some(maki_agent::AgentMode::Build),
        MODE_PLAN => {
            let storage = maki_storage::StateDir::resolve().ok()?;
            let plan_path = maki_storage::plans::new_plan_path(&storage).ok()?;
            Some(maki_agent::AgentMode::Plan(plan_path))
        }
        name if modes.contains(name) => {
            Some(maki_agent::AgentMode::Custom(ModeId::Custom(name.into())))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_snapshot_maps_order_and_categories() {
        let definitions = maki_agent::session_coordinator::builtin_option_definitions(
            "test/model",
            ["test/model".into()],
            true,
            false,
            true,
            maki_agent::ThinkingConfig::Off,
        );
        let options =
            maki_agent::session_options::SessionOptions::new(definitions, &Default::default())
                .unwrap();

        let projected = session_config_options(&options.snapshot(), false);

        let wire = serde_json::to_value(&projected).unwrap();
        let options = wire.as_array().unwrap();
        assert_eq!(
            options
                .iter()
                .map(|option| option["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["model", "yolo", "fast", "workflow", "thinking"]
        );
        assert_eq!(options[0]["category"], "model");
        assert!(
            options[1..4]
                .iter()
                .all(|option| option["category"] == "mode")
        );
        assert_eq!(
            projected[4].category,
            Some(SessionConfigOptionCategory::ThoughtLevel)
        );
    }

    #[test]
    fn supported_toggle_options_project_as_boolean() {
        let definitions = maki_agent::session_coordinator::builtin_option_definitions(
            "test/model",
            ["test/model".into()],
            true,
            false,
            true,
            maki_agent::ThinkingConfig::Off,
        );
        let options =
            maki_agent::session_options::SessionOptions::new(definitions, &Default::default())
                .unwrap();

        let projected = session_config_options(&options.snapshot(), true);
        let wire = serde_json::to_value(projected).unwrap();
        let options = wire.as_array().unwrap();

        assert_eq!(options[0]["type"], "select");
        assert!(options[1..4].iter().all(|option| {
            option["type"] == "boolean"
                && option.get("options").is_none()
                && option["currentValue"].is_boolean()
        }));
        assert_eq!(options[4]["type"], "select");
    }

    #[test]
    fn legacy_projection_keeps_toggle_select_domains() {
        let definitions = maki_agent::session_coordinator::builtin_option_definitions(
            "test/model",
            ["test/model".into()],
            true,
            false,
            true,
            maki_agent::ThinkingConfig::Off,
        );
        let options =
            maki_agent::session_options::SessionOptions::new(definitions, &Default::default())
                .unwrap();

        let wire =
            serde_json::to_value(session_config_options(&options.snapshot(), false)).unwrap();
        let options = wire.as_array().unwrap();

        assert!(options.iter().all(|option| option["type"] == "select"));
        assert_eq!(
            options[1]["options"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value["value"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [ENABLED_VALUE, DISABLED_VALUE]
        );
    }

    #[test]
    fn agent_info_names_makima() {
        let info = initialize_response().agent_info.unwrap();
        assert_eq!(info.name, "makima");
    }
}
