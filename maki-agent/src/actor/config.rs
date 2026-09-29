use std::sync::Arc;

use maki_providers::provider::Provider;
use maki_providers::{Model, RequestOptions, ThinkingConfig, ThinkingConfigExt};

use crate::{AgentMode, ModeDef};

use super::{ActorError, EffectiveAgentConfig};

#[derive(Clone)]
pub struct PreparedModel {
    pub provider: Arc<dyn Provider>,
    pub model: Model,
}

#[derive(Clone, Default)]
pub struct ConfigPatch {
    pub model: Option<PreparedModel>,
    pub thinking: Option<ThinkingConfig>,
    pub toggle_thinking: bool,
    pub fast: Option<bool>,
    pub workflow: Option<bool>,
}

#[derive(Clone)]
pub enum ConfigChange {
    Patch(ConfigPatch),
    Mode {
        mode: AgentMode,
        mode_def: Option<ModeDef>,
    },
    ToggleFast,
    ToggleWorkflow,
    ToggleThinking,
    Refresh {
        expected: PreparedModel,
        replacement: PreparedModel,
    },
}

#[derive(Clone)]
pub struct ConfigCommit {
    pub identity: Arc<()>,
    pub generation: u64,
    pub config: Arc<EffectiveAgentConfig>,
}

impl ConfigChange {
    pub(super) fn apply(
        self,
        current: &EffectiveAgentConfig,
    ) -> Result<EffectiveAgentConfig, ActorError> {
        let mut config = current.clone();
        let mut patch = match self {
            Self::Patch(patch) => patch,
            Self::Mode { mode, mode_def } => {
                if matches!(mode, AgentMode::Custom(_)) && mode_def.is_none() {
                    return Err(ActorError::InvalidConfig(
                        "custom mode requires a resolved definition".into(),
                    ));
                }
                config.mode = mode;
                config.mode_def = mode_def;
                return Ok(config);
            }
            Self::ToggleThinking => ConfigPatch {
                toggle_thinking: true,
                ..Default::default()
            },
            Self::ToggleFast => ConfigPatch {
                fast: Some(!current.fast),
                ..Default::default()
            },
            Self::ToggleWorkflow => ConfigPatch {
                workflow: Some(!current.workflow),
                ..Default::default()
            },
            Self::Refresh {
                expected,
                replacement,
            } => {
                if !Arc::ptr_eq(&expected.provider, &current.provider)
                    || expected.model.spec() != current.model.spec()
                {
                    return Ok(config);
                }
                ConfigPatch {
                    model: Some(replacement),
                    ..Default::default()
                }
            }
        };
        if patch.toggle_thinking {
            if patch.thinking.is_some() {
                return Err(ActorError::InvalidConfig(
                    "thinking value and toggle are mutually exclusive".into(),
                ));
            }
            patch.thinking = Some(
                ThinkingConfig::parse("", current.thinking)
                    .map_err(|error| ActorError::InvalidConfig(error.into()))?,
            );
        }
        if let Some(model) = patch.model {
            config.settings.provider = model.provider;
            config.settings.model = model.model;
        }
        if patch.fast == Some(true) && !config.model.supports_fast() {
            return Err(crate::session_options::SessionOptionError::FastUnsupported.into());
        }
        if patch.thinking.is_some_and(|thinking| thinking.enabled())
            && !config.model.supports_thinking()
        {
            return Err(crate::session_options::SessionOptionError::ThinkingUnsupported.into());
        }
        let options = RequestOptions {
            thinking: patch.thinking.unwrap_or(config.thinking),
            fast: patch.fast.unwrap_or(config.fast),
        }
        .clamped(&config.model);
        config.settings.thinking = options.thinking;
        config.settings.fast = options.fast;
        if let Some(workflow) = patch.workflow {
            config.settings.workflow = workflow;
        }
        Ok(config)
    }
}

fn same_model(left: &Model, right: &Model) -> bool {
    left.id == right.id
        && left.provider == right.provider
        && left.tier == right.tier
        && left.family == right.family
        && left.supports_tool_examples_override == right.supports_tool_examples_override
        && left.thinking_override == right.thinking_override
        && left.supports_vision_override == right.supports_vision_override
        && left.supports_fast_override == right.supports_fast_override
        && left.max_output_tokens == right.max_output_tokens
        && left.turn_output_tokens == right.turn_output_tokens
        && left.context_window == right.context_window
        && left.thinking_fields == right.thinking_fields
        && left.pricing.input == right.pricing.input
        && left.pricing.output == right.pricing.output
        && left.pricing.cache_write == right.pricing.cache_write
        && left.pricing.cache_read == right.pricing.cache_read
        && left
            .pricing
            .fast
            .as_ref()
            .map(|pricing| (pricing.input, pricing.output))
            == right
                .pricing
                .fast
                .as_ref()
                .map(|pricing| (pricing.input, pricing.output))
}

pub(super) fn equivalent(left: &EffectiveAgentConfig, right: &EffectiveAgentConfig) -> bool {
    Arc::ptr_eq(&left.provider, &right.provider)
        && same_model(&left.model, &right.model)
        && left.fast == right.fast
        && left.workflow == right.workflow
        && left.thinking == right.thinking
        && left.mode == right.mode
        && left.mode_def == right.mode_def
}
