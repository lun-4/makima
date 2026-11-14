use std::path::PathBuf;
use std::sync::Arc;

use makima_domain::ModelSelection;
use makima_providers::{
    Model, PreparedProvider, ProviderRegistry, RequestOptions, ThinkingConfig, ThinkingConfigExt,
};

use makima_storage::session_state::{StoredAgentConfig, StoredModeCeiling};

use crate::{AgentMode, ModeDef, RunSettings};

use super::{ActorError, AgentContextSnapshot, EffectiveAgentConfig};

impl EffectiveAgentConfig {
    pub fn from_stored(
        stored: &StoredAgentConfig,
        prepared: PreparedModel,
        (mode, definition): (AgentMode, ModeDef),
    ) -> Result<Self, ActorError> {
        if prepared.model.spec() != stored.model {
            return Err(ActorError::InvalidConfig(
                "prepared model does not match stored configuration".into(),
            ));
        }
        let cwd = PathBuf::from(&stored.cwd);
        if !cwd.is_absolute() {
            return Err(ActorError::InvalidConfig(
                "stored working directory must be absolute".into(),
            ));
        }
        if definition.id != mode.id() {
            return Err(ActorError::InvalidConfig(
                "resolved mode definition does not match restored mode".into(),
            ));
        }
        let options = RequestOptions {
            thinking: stored.thinking.into(),
            fast: stored.fast,
        }
        .clamped(&prepared.model);
        Ok(Self::new(
            RunSettings {
                provider: prepared.provider,
                model: prepared.model,
                fast: options.fast,
                thinking: options.thinking,
                workflow: stored.workflow,
            },
            mode,
        )
        .with_mode_def(Some(definition))
        .with_context(AgentContextSnapshot::new(cwd)))
    }
}

#[derive(Clone, Default)]
pub struct ModeCeiling {
    restrict_write_to: Option<PathBuf>,
    tools: Option<Vec<String>>,
}

impl From<StoredModeCeiling> for ModeCeiling {
    fn from(ceiling: StoredModeCeiling) -> Self {
        Self {
            restrict_write_to: ceiling.restrict_write_to,
            tools: ceiling.tools,
        }
    }
}

impl From<&ModeCeiling> for StoredModeCeiling {
    fn from(ceiling: &ModeCeiling) -> Self {
        Self {
            restrict_write_to: ceiling.restrict_write_to.clone(),
            tools: ceiling.tools.clone(),
        }
    }
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
    Restore(StoredAgentConfig),
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
    pub(super) fn resolved_restore(
        stored: &StoredAgentConfig,
        prepared: PreparedModel,
        mode: (AgentMode, ModeDef),
        current: &EffectiveAgentConfig,
    ) -> Result<EffectiveAgentConfig, ActorError> {
        let mut config = EffectiveAgentConfig::from_stored(stored, prepared, mode)?;
        config.context = current.context.change_directory(PathBuf::from(&stored.cwd));
        config.mode_authority = current.mode_authority.clone();
        if let Some(ceiling) = &config.mode_authority {
            ceiling.validate(&config.mode, config.mode_def.as_ref())?;
        }
        Ok(config)
    }

    pub(super) fn restores_pending_fast(&self, current: &EffectiveAgentConfig) -> bool {
        matches!(self, Self::Refresh { expected, replacement }
            if Arc::ptr_eq(&expected.provider, &current.provider)
                && expected.model.spec() == current.model.spec()
                && replacement.model.supports_fast())
    }

    pub(super) fn clears_pending_fast(&self, current: &EffectiveAgentConfig) -> bool {
        match self {
            Self::Patch(patch) | Self::PatchAndMode { patch, .. } => {
                patch.model.is_some() || patch.fast.is_some()
            }
            Self::ToggleFast => true,
            Self::Refresh {
                expected,
                replacement,
            } => {
                Arc::ptr_eq(&expected.provider, &current.provider)
                    && expected.model.spec() == current.model.spec()
                    && !replacement.model.fast_pending()
            }
            _ => false,
        }
    }

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
            Self::Restore(_) => {
                return Err(ActorError::InvalidConfig(
                    "restore requires resources resolved against current discovery".into(),
                ));
            }
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

    use super::{ConfigChange, ModeCeiling};
    use crate::AgentMode;
    use crate::actor::{AgentContextSnapshot, EffectiveAgentConfig};
    use crate::modes::{ModeDef, ModeId};
    use makima_storage::session_state::StoredModeCeiling;
    use test_case::test_case;

    #[test_case(None; "unrestricted_tools")]
    #[test_case(Some(Vec::new()); "no_tools")]
    #[test_case(Some(vec!["read".into()]); "read_only_tools")]
    fn restored_ceiling_preserves_tool_authority(tools: Option<Vec<String>>) {
        let stored = StoredModeCeiling {
            restrict_write_to: Some(PathBuf::from("/allowed")),
            tools: tools.clone(),
        };
        let ceiling = ModeCeiling::from(stored.clone());
        assert_eq!(StoredModeCeiling::from(&ceiling), stored);
        let mut definition = ModeDef::default_for(ModeId::parse("build"));
        definition.restrict_write_to = Some(PathBuf::from("/allowed"));
        definition.tools = tools;
        assert!(
            ceiling
                .validate(&AgentMode::Build, Some(&definition))
                .is_ok()
        );
        definition.restrict_write_to = Some(PathBuf::from("/outside"));
        assert!(
            ceiling
                .validate(&AgentMode::Build, Some(&definition))
                .is_err()
        );
        definition.restrict_write_to = Some(PathBuf::from("/allowed"));
        definition.tools = Some(vec!["write".into()]);
        assert_eq!(
            ceiling
                .validate(&AgentMode::Build, Some(&definition))
                .is_ok(),
            stored.tools.is_none()
        );
    }

    #[test_case(false; "root")]
    #[test_case(true; "child")]
    fn post_spawn_policy_commit_records_once_before_observation(child: bool) {
        use crate::actor::tests::{ScriptedBackend, policy};
        use crate::manager::AgentManagerHandle;
        use crate::session_checkpoint::stored_agent_config;
        use makima_storage::session_state::{SessionStateView, StateChange};
        use std::collections::BTreeMap;

        smol::block_on(async {
            let manager = AgentManagerHandle::new(Default::default()).unwrap();
            let config = EffectiveAgentConfig::new(policy(false), AgentMode::Build)
                .with_context(AgentContextSnapshot::new(PathBuf::from("/original")))
                .with_mode_def(Some(ModeDef::default_for(ModeId::parse("build"))));
            let root = manager
                .create_root_with_config(Some(config.clone()), Vec::new(), None, |_| {
                    Ok::<Box<dyn crate::actor::ActorBackend>, String>(Box::new(
                        ScriptedBackend::new(),
                    ))
                })
                .unwrap();
            let target = if child {
                manager
                    .spawn_child_trusted_with_config(
                        &root,
                        config,
                        Default::default(),
                        Vec::new(),
                        None,
                        |_| {
                            Ok::<Box<dyn crate::actor::ActorBackend>, String>(Box::new(
                                ScriptedBackend::new(),
                            ))
                        },
                    )
                    .unwrap()
            } else {
                root.clone()
            };
            let actor = target.actor().unwrap();
            let journal = manager.journal();
            let options = BTreeMap::from([("plugin.transient".into(), "selected".into())]);
            let original =
                stored_agent_config(&actor.effective_config().unwrap(), options.clone()).unwrap();
            journal
                .record_config(target.id(), 0, original, Some(options.clone()))
                .unwrap();
            let before = journal.snapshot();
            let observer = actor.subscribe_config_commits();
            while observer.try_recv().is_ok() {}
            let committed = actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::ToggleFast))
                .await
                .unwrap();
            assert!(committed.config.config.fast);
            let changed = journal.snapshot();
            assert_eq!(changed.state.len(), before.state.len() + 1);
            let folded =
                SessionStateView::fold(&changed.state, &changed.agent_revisions, changed.seq)
                    .unwrap();
            let saved = folded.scopes[&Some(target.id())].config.as_ref().unwrap();
            assert!(saved.fast);
            assert_eq!(saved.options, options);
            assert_eq!(saved.cwd, "/original");
            assert_eq!(saved.mode.id, "build");
            assert_eq!(
                observer.try_recv().unwrap().generation,
                committed.config.generation
            );
            actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(None)
                .await
                .unwrap();
            assert_eq!(journal.snapshot().state, changed.state);
            let cancelled = actor.reserve_prepared_operation(None).unwrap();
            drop(cancelled);
            assert_eq!(journal.snapshot().state, changed.state);
            assert!(matches!(
                changed.state.last().unwrap().change,
                StateChange::Config(_)
            ));
            root.close_subtree().unwrap();
        });
    }

    #[test_case(true; "supported")]
    #[test_case(false; "unsupported")]
    fn discovery_consumes_model_pinned_fast_intent_atomically(supported: bool) {
        use crate::actor::PreparedModel;
        use crate::actor::tests::{ScriptedBackend, policy};
        use crate::manager::AgentManagerHandle;
        use crate::session_checkpoint::stored_agent_config;
        use crate::session_options::FAST_PENDING_MODEL_OPTION_ID;
        use makima_providers::model::FastSupport;
        use makima_storage::session_state::StateChange;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        smol::block_on(async {
            let manager = AgentManagerHandle::new(Default::default()).unwrap();
            let mut settings = policy(false);
            settings.model.fast_support = Some(FastSupport::Pending);
            let config = EffectiveAgentConfig::new(settings, AgentMode::Build)
                .with_mode_def(Some(ModeDef::default_for(ModeId::Build)));
            let root = manager
                .create_root_with_config(Some(config), Vec::new(), None, |_| {
                    Ok::<Box<dyn crate::actor::ActorBackend>, String>(Box::new(
                        ScriptedBackend::new(),
                    ))
                })
                .unwrap();
            let actor = root.actor().unwrap();
            let before = actor.effective_config().unwrap();
            let options =
                BTreeMap::from([(FAST_PENDING_MODEL_OPTION_ID.into(), before.model.spec())]);
            manager
                .journal()
                .record_config(
                    root.id(),
                    0,
                    stored_agent_config(&before, options.clone()).unwrap(),
                    Some(options),
                )
                .unwrap();
            let mut replacement = before.model.clone();
            replacement.fast_support = Some(if supported {
                FastSupport::Supported
            } else {
                FastSupport::Unsupported
            });
            let commit = actor
                .reserve_prepared_operation(None)
                .unwrap()
                .commit_when_ready(Some(ConfigChange::Refresh {
                    expected: PreparedModel {
                        provider: Arc::clone(&before.provider),
                        model: before.model.clone(),
                    },
                    replacement: PreparedModel {
                        provider: Arc::clone(&before.provider),
                        model: replacement,
                    },
                }))
                .await
                .unwrap();
            assert_eq!(commit.config.config.fast, supported);
            let snapshot = manager.journal().snapshot();
            let StateChange::Config(stored) = &snapshot.state.last().unwrap().change else {
                panic!("expected configuration record")
            };
            assert_eq!(stored.fast, supported);
            assert!(!stored.options.contains_key(FAST_PENDING_MODEL_OPTION_ID));
            root.close_subtree().unwrap();
        });
    }

    #[test_case(true; "supported")]
    #[test_case(false; "unsupported")]
    fn restored_config_uses_stored_values_and_clamps_capabilities(supported: bool) {
        use crate::actor::PreparedModel;
        use crate::actor::tests::policy;
        use crate::session_checkpoint::stored_agent_config;
        use makima_providers::model::FastSupport;
        use makima_providers::{Effort, ThinkingConfig, ThinkingSupport};
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let mut definition = ModeDef::default_for(ModeId::parse("saved_custom"));
        definition.system_prompt = Some("exact saved prompt".into());
        definition.tools = Some(Vec::new());
        let mode = AgentMode::Custom(definition.id.clone());
        let original = EffectiveAgentConfig::new(policy(true), mode.clone())
            .with_context(AgentContextSnapshot::new(PathBuf::from("/saved")))
            .with_mode_def(Some(definition.clone()));
        let mut stored = stored_agent_config(&original, BTreeMap::new()).unwrap();
        stored.thinking = Some(ThinkingConfig::Effort(Effort::High).into());
        stored.workflow = true;
        let mut model = original.model.clone();
        model.fast_support = Some(if supported {
            FastSupport::Supported
        } else {
            FastSupport::Unsupported
        });
        model.thinking_support = Some(if supported {
            ThinkingSupport::Yes
        } else {
            ThinkingSupport::No
        });
        let restored = EffectiveAgentConfig::from_stored(
            &stored,
            PreparedModel {
                provider: Arc::clone(&original.provider),
                model,
            },
            (mode.clone(), definition.clone()),
        )
        .unwrap();
        assert_eq!(restored.context.cwd(), PathBuf::from("/saved"));
        assert_eq!(restored.model.spec(), stored.model);
        assert_eq!(restored.fast, supported);
        assert_eq!(
            restored.thinking,
            if supported {
                ThinkingConfig::Effort(Effort::High)
            } else {
                ThinkingConfig::Off
            }
        );
        assert!(restored.workflow);
        assert_eq!(restored.mode, mode);
        assert_eq!(restored.mode_def, Some(definition));
    }

    #[test_case(false; "rejects_changed_permissions")]
    #[test_case(true; "restores_inside_existing_ceiling")]
    fn restore_preserves_current_mode_authority(allowed: bool) {
        use crate::actor::{ModeCeiling, PreparedModel};
        use crate::session_checkpoint::stored_agent_config;
        use std::collections::BTreeMap;
        use std::sync::Arc;

        const RESTRICTED_TOOL: &str = "read";
        let mut definition = ModeDef::default_for(ModeId::Build);
        definition.tools = Some(vec![RESTRICTED_TOOL.into()]);
        let current =
            EffectiveAgentConfig::new(crate::actor::tests::policy(false), AgentMode::Build)
                .with_context(AgentContextSnapshot::new(PathBuf::from("/current")))
                .with_mode_def(Some(definition.clone()))
                .with_mode_authority(ModeCeiling::from_mode(&AgentMode::Build, Some(&definition)));
        let mut saved = stored_agent_config(&current, BTreeMap::new()).unwrap();
        if !allowed {
            saved.mode.tools = None;
        }
        let mode = crate::modes::from_snapshot(&saved.mode).unwrap();
        let result = ConfigChange::resolved_restore(
            &saved,
            PreparedModel {
                provider: Arc::clone(&current.provider),
                model: current.model.clone(),
            },
            mode,
            &current,
        );
        assert_eq!(result.is_ok(), allowed);
        if let Ok(config) = result {
            assert!(config.mode_authority().is_some());
            assert_eq!(
                config.context.generation(),
                current.context.generation() + 1
            );
        }
    }

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
