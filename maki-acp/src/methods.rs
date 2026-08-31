use maki_agent::session_options::{SessionOptionCategory, SessionOptionsSnapshot};
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

pub fn session_config_options(snapshot: &SessionOptionsSnapshot) -> Vec<SessionConfigOption> {
    snapshot
        .options
        .iter()
        .map(|option| {
            let definition = &option.definition;
            let values: Vec<SessionConfigSelectOption> = definition
                .values
                .iter()
                .map(|value| {
                    SessionConfigSelectOption::new(value.value.to_string(), value.name.to_string())
                })
                .collect();
            let category = match definition.category {
                SessionOptionCategory::Model => SessionConfigOptionCategory::Model,
                SessionOptionCategory::Mode => SessionConfigOptionCategory::Mode,
            };
            SessionConfigOption::select(
                definition.id.to_string(),
                definition.name.to_string(),
                option.current_value.to_string(),
                values,
            )
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
        );
        let options =
            maki_agent::session_options::SessionOptions::new(definitions, &Default::default())
                .unwrap();

        let projected = session_config_options(&options.snapshot());

        let wire = serde_json::to_value(projected).unwrap();
        let options = wire.as_array().unwrap();
        assert_eq!(
            options
                .iter()
                .map(|option| option["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["model", "yolo", "fast", "workflow"]
        );
        assert_eq!(options[0]["category"], "model");
        assert!(
            options[1..]
                .iter()
                .all(|option| option["category"] == "mode")
        );
    }

    #[test]
    fn agent_info_names_makima() {
        let info = initialize_response().agent_info.unwrap();
        assert_eq!(info.name, "makima");
    }
}
