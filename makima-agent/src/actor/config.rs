use std::path::PathBuf;
use std::sync::Arc;

use makima_domain::ModelSelection;
use makima_providers::{
    Model, PreparedProvider, ProviderRegistry, RequestOptions, ThinkingConfig, ThinkingConfigExt,
};

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

pub type PreparedModel = PreparedProvider;

#[derive(Clone, Default)]
pub struct ConfigPatch {
    pub model: Option<PreparedModel>,
    pub thinking: Option<ThinkingConfig>,
    pub toggle_thinking: bool,
    pub fast: Option<bool>,
    pub workflow: Option<bool>,
}

impl ConfigPatch {
    pub fn from_model_selection(selection: &ModelSelection) -> Self {
        Self {
            thinking: selection.thinking,
            fast: selection.fast,
            ..Default::default()
        }
    }
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

/// Re-prepares `model` against what discovery knows now. `None` when that
/// changes nothing or fails, so an already-correct agent is left alone.
pub async fn rediscovered_model(providers: &ProviderRegistry, model: &Model) -> Option<Model> {
    let resolved = providers.model(&model.spec()).ok()?;
    let refined = providers.prepare(resolved).await.ok()?.model;
    (!same_model(&refined, model)).then_some(refined)
}

impl ConfigChange {
    /// A metadata-only refresh: the provider stays, so a concurrent model
    /// switch makes the compare-and-swap a no-op.
    pub async fn rediscovered(
        config: &EffectiveAgentConfig,
        providers: &ProviderRegistry,
    ) -> Option<Self> {
        let model = rediscovered_model(providers, &config.model).await?;
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
            && config.model.supports_thinking() != Some(true)
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
        && left.tool_examples == right.tool_examples
        && left.thinking_support == right.thinking_support
        && left.vision == right.vision
        && left.fast_support == right.fast_support
        && left.max_output_tokens == right.max_output_tokens
        && left.context_window == right.context_window
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
        let original_cwd = std::env::temp_dir().join("original");
        let changed_cwd = std::env::temp_dir().join("changed");
        let original =
            EffectiveAgentConfig::new(crate::actor::tests::policy(false), AgentMode::Build)
                .with_context(AgentContextSnapshot::new(original_cwd.clone()));
        let changed = ConfigChange::WorkingDirectory(changed_cwd.clone())
            .apply(&original)
            .unwrap();
        assert_eq!(original.context.cwd(), original_cwd.as_path());
        assert_eq!(original.context.generation(), 0);
        assert_eq!(changed.context.cwd(), changed_cwd.as_path());
        assert_eq!(changed.context.generation(), 1);
        assert!(!super::equivalent(&original, &changed));
        assert!(
            ConfigChange::WorkingDirectory(PathBuf::from("relative"))
                .apply(&original)
                .is_err()
        );
    }
}
