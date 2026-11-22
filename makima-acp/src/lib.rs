pub mod elicitation;
pub mod methods;
pub mod permissions;
pub mod server;
pub mod translate;

use std::path::PathBuf;
use std::sync::Arc;

use makima_agent::agent::PromptSlotSource;
use makima_agent::permissions::PluginRuleStore;
use makima_agent::{AgentConfig, PermissionsConfig};
use makima_commands::CommandRegistry;
use makima_config::{ModelPolicy, SessionDefaults, TrustConfig, project::TrustMode};
use makima_providers::model::Model;
use makima_providers::{ProviderRegistry, Timeouts};

pub struct AcpParams {
    pub model: Model,
    pub config: AgentConfig,
    pub permissions_config: PermissionsConfig,
    pub timeouts: Timeouts,
    pub initial_wd: PathBuf,
    pub storage: makima_storage::StateDir,
    pub prompt_slots: PromptSlotSource,
    pub yolo: bool,
    pub defaults: SessionDefaults,
    pub system_prompt_override: Option<String>,
    pub append_system_prompt: Option<String>,
    pub model_policy: Arc<ModelPolicy>,
    pub providers: Arc<ProviderRegistry>,
    pub plugin_rules: Arc<PluginRuleStore>,
    pub lua_event_handle: makima_lua::EventHandle,
    pub command_registry: CommandRegistry,
    pub trust_mode: TrustMode,
    pub trust_policy: Arc<TrustConfig>,
}

pub fn run(params: AcpParams) -> color_eyre::Result<()> {
    smol::block_on(server::serve(params))
}
