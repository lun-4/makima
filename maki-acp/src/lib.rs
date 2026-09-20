pub mod elicitation;
pub mod methods;
pub mod permissions;
pub mod server;
pub mod translate;

use std::path::PathBuf;
use std::sync::Arc;

use maki_agent::permissions::PluginRuleStore;
use maki_agent::prompt::ResolvedSlots;
use maki_agent::{AgentConfig, ModeRegistry, PermissionsConfig};
use maki_commands::CommandRegistry;
use maki_config::{ModelPolicy, TrustConfig, project::TrustMode};
use maki_providers::Timeouts;
use maki_providers::model::Model;

pub struct AcpParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub initial_wd: PathBuf,
    pub storage: maki_storage::StateDir,
    pub prompt_slots: Arc<ResolvedSlots>,
    pub modes: Arc<ModeRegistry>,
    pub yolo: bool,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    pub model_policy: Arc<ModelPolicy>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub lua_event_handle: maki_lua::EventHandle,
    pub command_registry: CommandRegistry,
    pub trust_mode: TrustMode,
    pub trust_policy: Arc<TrustConfig>,
}

pub fn run(params: AcpParams) -> color_eyre::Result<()> {
    smol::block_on(server::serve(params))
}
