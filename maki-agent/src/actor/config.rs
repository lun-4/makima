use std::path::PathBuf;
use std::sync::Arc;

use maki_providers::provider::Provider;
use maki_providers::{Model, RequestOptions, ThinkingConfig, ThinkingConfigExt, model_registry};

use crate::{AgentMode, ModeDef};

use super::{ActorError, EffectiveAgentConfig};

#[derive(Clone, Default)]
pub struct ModeCeiling {
    restrict_write_to: Option<PathBuf>,
    tools: Option<Vec<String>>,
}

impl ModeCeiling {
    pub(super) fn new(config: &EffectiveAgentConfig) -> Self {
        config
            .mode_authority
            .clone()
            .unwrap_or_else(|| Self::from_mode(&config.mode, config.mode_def.as_ref()))
    }

    pub fn from_mode(mode: &AgentMode, mode_def: Option<&ModeDef>) -> Self {
        Self {
            restrict_write_to: mode_def
                .and_then(|definition| definition.restrict_write_to.clone())
                .or_else(|| mode.plan_path().map(PathBuf::from)),
            tools: mode_def.and_then(|definition| definition.tools.clone()),
        }
    }

    pub fn validate(&self, mode: &AgentMode, mode_def: Option<&ModeDef>) -> Result<(), ActorError> {
        let path = mode_def
            .and_then(|definition| definition.restrict_write_to.as_deref())
            .or_else(|| mode.plan_path());
        if self.restrict_write_to.is_some() && self.restrict_write_to.as_deref() != path {
            return Err(ActorError::InvalidConfig(
                "mode exceeds the child's initial write restriction".into(),
            ));
        }
        if let Some(allowed) = &self.tools
            && !mode_def
                .and_then(|definition| definition.tools.as_ref())
                .is_some_and(|tools| tools.iter().all(|tool| allowed.contains(tool)))
        {
            return Err(ActorError::InvalidConfig(
                "mode exceeds the child's initial tool permissions".into(),
            ));
        }
        Ok(())
    }
}

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
    PatchAndMode {
        patch: ConfigPatch,
        mode: AgentMode,
        mode_def: ModeDef,
    },
    Mode {
        mode: AgentMode,
        mode_def: Option<ModeDef>,
    },
    WorkingDirectory(PathBuf),
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

/// Re-resolves `model` against what discovery knows now. `None` when that
/// changes nothing, so an already-correct agent is left alone.
pub fn rediscovered_model(provider: &dyn Provider, model: &Model) -> Option<Model> {
    let mut refined = Model::from_spec(&model.spec()).ok()?;
    provider.adjust_model(&mut refined);
    (!same_model(&refined, model)).then_some(refined)
}

/// {candidate} as it commits over {current}: a model it switched to takes
/// what discovery knows by now, so discovery that lands after the change
/// materialized is not lost.
pub(super) fn with_discovery(
    current: Option<&EffectiveAgentConfig>,
    candidate: &Arc<EffectiveAgentConfig>,
) -> Arc<EffectiveAgentConfig> {
    let switched = current.is_none_or(|current| {
        !Arc::ptr_eq(&current.provider, &candidate.provider)
            || current.model.spec() != candidate.model.spec()
    });
    let Some(model) = switched
        .then(|| model_registry::discovered(&candidate.model.provider, &candidate.model.id))
        .flatten()
        .and_then(|_| rediscovered_model(&*candidate.provider, &candidate.model))
    else {
        return Arc::clone(candidate);
    };
    let mut config = (**candidate).clone();
    config.settings.model = model;
    Arc::new(config)
}

impl ConfigChange {
    /// A metadata-only refresh: the provider stays, so a concurrent model
    /// switch makes the compare-and-swap a no-op.
    pub fn rediscovered(config: &EffectiveAgentConfig) -> Option<Self> {
        let model = rediscovered_model(&*config.provider, &config.model)?;
        Some(Self::Refresh {
            expected: PreparedModel {
                provider: Arc::clone(&config.provider),
                model: config.model.clone(),
            },
            replacement: PreparedModel {
                provider: Arc::clone(&config.provider),
                model,
            },
        })
    }

    pub(super) fn apply(
        self,
        current: &EffectiveAgentConfig,
    ) -> Result<EffectiveAgentConfig, ActorError> {
        let mut config = current.clone();
        let mut patch = match self {
            Self::WorkingDirectory(cwd) => {
                if !cwd.is_absolute() {
                    return Err(ActorError::InvalidConfig(
                        "working directory must be absolute".into(),
                    ));
                }
                config.context = current.context.change_directory(cwd);
                return Ok(config);
            }
            Self::Patch(patch) => patch,
            Self::PatchAndMode {
                patch,
                mode,
                mode_def,
            } => {
                let config = Self::Mode {
                    mode,
                    mode_def: Some(mode_def),
                }
                .apply(current)?;
                return Self::Patch(patch).apply(&config);
            }
            Self::Mode { mode, mode_def } => {
                if mode_def.is_none() {
                    return Err(ActorError::InvalidConfig(
                        "mode change requires a resolved definition".into(),
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
        && left.context == right.context
}

#[cfg(test)]
mod context_tests {
    use std::path::PathBuf;

    use super::ConfigChange;
    use crate::AgentMode;
    use crate::actor::{AgentContextSnapshot, EffectiveAgentConfig};

    #[test]
    fn cwd_config_captures_context_generation_and_rejects_relative_paths() {
        let original =
            EffectiveAgentConfig::new(crate::actor::tests::policy(false), AgentMode::Build)
                .with_context(AgentContextSnapshot::new(PathBuf::from("/original")));
        let changed = ConfigChange::WorkingDirectory(PathBuf::from("/changed"))
            .apply(&original)
            .unwrap();
        assert_eq!(original.context.cwd(), PathBuf::from("/original"));
        assert_eq!(original.context.generation(), 0);
        assert_eq!(changed.context.cwd(), PathBuf::from("/changed"));
        assert_eq!(changed.context.generation(), 1);
        assert!(!super::equivalent(&original, &changed));
        assert!(
            ConfigChange::WorkingDirectory(PathBuf::from("relative"))
                .apply(&original)
                .is_err()
        );
    }
}
