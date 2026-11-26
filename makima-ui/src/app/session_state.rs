use color_eyre::Result;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use makima_agent::ModeId;
use makima_agent::actor::EffectiveAgentConfig;
#[cfg(test)]
use makima_agent::actor::{AgentContextSnapshot, PreparedModel};
use makima_agent::journal::{SessionJournal, SessionJournalSnapshot};
use makima_agent::plan_state;
#[cfg(test)]
use makima_agent::{AgentMode, ModeRegistry, RunSettings, SessionDefaults};
#[cfg(test)]
use makima_agent::{modes, session_defaults};
#[cfg(test)]
use makima_providers::Model;
#[cfg(test)]
use makima_providers::provider::Provider;
#[cfg(test)]
use makima_storage::StateDir;
#[cfg(test)]
use makima_storage::session_state::StoredAgentConfig;
use makima_storage::sessions::StoredMode;
#[cfg(test)]
use makima_storage::sessions::read_prefs;

use crate::AppSession;

use super::mode::{Mode, PlanState};

/// Where the status bar's request options come from: an agent's committed
/// config, or a subagent chat's own settings once its agent is gone. Resolving
/// both together keeps a frame from mixing one source's thinking with
/// another's fast flag.
pub(crate) enum ConfigSource {
    Actor(Arc<makima_agent::EffectiveAgentConfig>),
    Chat(makima_providers::RequestOptions),
}

impl ConfigSource {
    pub(crate) fn options(&self) -> makima_providers::RequestOptions {
        match self {
            Self::Actor(config) => makima_providers::RequestOptions {
                thinking: config.thinking,
                fast: config.fast,
            },
            Self::Chat(options) => *options,
        }
    }

    pub(crate) fn workflow(&self) -> bool {
        match self {
            Self::Actor(config) => config.workflow,
            Self::Chat(_) => false,
        }
    }
}

pub(crate) struct SessionState {
    pub(crate) projection: Arc<SessionJournalSnapshot>,
    pub plan: PlanState,
    pub warnings: Vec<String>,
}

const PLAN_FILE_MISSING_WARNING: &str = "Plan file was deleted \u{2014} started a new plan";

impl SessionState {
    pub fn from_session(session: &AppSession) -> Result<Self> {
        Self::from_projection(SessionJournal::from_session(session)?.projection())
    }

    fn from_projection(projection: Arc<SessionJournalSnapshot>) -> Result<Self> {
        let view = projection.fold()?;
        let root = projection.root_agent_id();
        let stored = root
            .and_then(|root| view.scopes.get(&Some(root)))
            .and_then(|scope| scope.config.as_ref());
        let mut warnings = Vec::new();
        let entries = plan_state::plan_entries(&view).unwrap_or_default();
        let entry = root.and_then(|root| entries.get(&root));
        let plan_path = stored.map_or_else(
            || entry.map(|entry| entry.path.as_path()),
            |config| {
                (ModeId::parse(&config.mode.id) == ModeId::Plan)
                    .then_some(config.mode.restrict_write_to.as_deref())
                    .flatten()
            },
        );
        let plan = match plan_path {
            Some(p) if p.exists() => {
                if entry.is_some_and(|entry| entry.written) {
                    PlanState::Ready(PathBuf::from(p))
                } else {
                    PlanState::Drafting(PathBuf::from(p))
                }
            }
            Some(_) => {
                if stored.is_none() {
                    warnings.push(PLAN_FILE_MISSING_WARNING.into());
                }
                PlanState::None
            }
            None => PlanState::None,
        };

        Ok(Self {
            projection,
            plan,
            warnings,
        })
    }

    /// The configuration the agent is seeded with: the session being resumed,
    /// or {defaults} for a blank one. The app shows it until the agent
    /// answers, then the agent's committed config.
    #[cfg(test)]
    pub fn starting_config(
        &mut self,
        provider: Arc<dyn Provider>,
        model: &Model,
        cwd: PathBuf,
        registry: &ModeRegistry,
        storage: &StateDir,
        defaults: SessionDefaults,
    ) -> Result<EffectiveAgentConfig> {
        let view = self.projection.fold()?;
        let stored = self.projection.root_agent_id().and_then(|root| {
            view.scopes
                .get(&Some(root))
                .and_then(|scope| scope.config.as_ref())
        });
        let root = self.projection.root_agent_id();
        let (mode, mode_def) = match (stored, root) {
            (Some(config), Some(root)) => {
                let restored =
                    plan_state::prepare_restore_for_agent(&view, root, &config.mode, registry)?;
                self.warnings.extend(restored.warning);
                (restored.mode, restored.definition)
            }
            _ => {
                let current = self
                    .plan
                    .path()
                    .map(|path| AgentMode::Plan(path.to_path_buf()))
                    .unwrap_or(AgentMode::Build);
                let restored = modes::saved(self.projection.seed_meta()).map_or_else(
                    || Ok((AgentMode::Build, registry.current(&AgentMode::Build))),
                    |(key, _)| modes::resolve(registry, key, &current, storage),
                );
                restored.unwrap_or_else(|error| {
                    tracing::warn!(%error, "resuming in read-only mode");
                    let fallback = modes::read_only_fallback(error);
                    self.warnings.extend(fallback.warning);
                    (fallback.mode, fallback.definition)
                })
            }
        };
        if let Some(config) = stored {
            let mut selected = config.clone();
            selected.model = model.spec();
            return EffectiveAgentConfig::from_stored(
                &selected,
                PreparedModel {
                    provider,
                    model: model.clone(),
                },
                (mode, mode_def),
            )
            .map_err(Into::into);
        }
        let settings = session_defaults::resolve(
            defaults,
            &read_prefs(storage),
            self.projection.seed_meta(),
            self.projection
                .root_history()
                .is_none_or(|history| history.messages.is_empty()),
            model,
        );
        Ok(EffectiveAgentConfig::new(
            RunSettings {
                provider,
                model: model.clone(),
                thinking: settings.thinking.into(),
                fast: settings.fast,
                workflow: settings.workflow,
            },
            mode,
        )
        .with_mode_def(Some(mode_def))
        .with_context(AgentContextSnapshot::new(cwd)))
    }

    /// Adopts the plan path of the config the agent was seeded with.
    pub fn adopt(&mut self, config: &EffectiveAgentConfig) {
        match config.mode.plan_path() {
            Some(path) if self.plan.path() != Some(path) => {
                let ready = self.projection.fold().ok().is_some_and(|view| {
                    let Some(root) = self.projection.root_agent_id() else {
                        return false;
                    };
                    plan_state::plan_entries(&view)
                        .ok()
                        .and_then(|plans| plans.get(&root).cloned())
                        .is_some_and(|entry| {
                            entry.written
                                && plan_state::resolved_plan_path(&view, root, &entry.path)
                                    .is_ok_and(|resolved| resolved == path)
                        })
                });
                self.plan = if ready {
                    PlanState::Ready(path.to_path_buf())
                } else {
                    PlanState::Drafting(path.to_path_buf())
                };
            }
            None => self.plan = PlanState::None,
            Some(_) => {}
        }
    }
}

impl From<Mode> for StoredMode {
    fn from(mode: Mode) -> Self {
        match mode {
            Mode::Build => StoredMode::Build,
            Mode::Plan => StoredMode::Plan,
            Mode::Custom(name) => StoredMode::Custom(name.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::test_model;
    use makima_agent::ModeDef;
    use makima_agent::session_options::FAST_PENDING_MODEL_OPTION_ID;
    use makima_providers::ThinkingConfig;
    use makima_providers::model::FastSupport;
    use makima_storage::id::AgentId;
    use makima_storage::plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot};
    use makima_storage::session_state::{StateChange, StoredStateRecord};
    use makima_storage::sessions::{Effort, Prefs, StoredThinking, write_prefs};
    use test_case::test_case;

    fn resumed(session: AppSession) -> SessionState {
        SessionState::from_session(&session).unwrap()
    }

    fn make_plan_session(mode: Option<StoredMode>, plan_path: Option<String>) -> AppSession {
        let mut session = AppSession::new("test-model", "/tmp");
        session.meta.mode = mode;
        if let Some(path) = plan_path {
            let root = AgentId::generate();
            session.bind_root_id(root).unwrap();
            session.push_state(StoredStateRecord {
                seq: 1,
                agent: None,
                generation: 0,
                change: StateChange::Plugin {
                    key: PLAN_STATE_KEY.into(),
                    version: PLAN_STATE_VERSION,
                    value: Some(
                        serde_json::to_value(PlanSlot::from([(
                            root,
                            PlanEntry {
                                path: path.into(),
                                written: false,
                                approved: None,
                                staged_model: None,
                            },
                        )]))
                        .unwrap(),
                    ),
                },
            });
        }
        session
    }

    #[test_case(FastSupport::Pending, false ; "pending_disables_fast")]
    #[test_case(FastSupport::Supported, true ; "supported_restores_fast")]
    #[test_case(FastSupport::Unsupported, false ; "unsupported_disables_fast")]
    fn resume_fast_support(support: FastSupport, expected_fast: bool) {
        let mut session = AppSession::new("test-model", "/tmp");
        session.meta.fast = true;
        let mut model = test_model();
        model.fast_support = Some(support);
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut state = SessionState::from_session(&session).unwrap();
        let seeded = state
            .starting_config(
                makima_agent::tools::cli_tool_ctx(std::env::current_dir().unwrap()).provider,
                &model,
                tmp.path().to_path_buf(),
                &ModeRegistry::builtin(),
                &storage,
                SessionDefaults::default(),
            )
            .unwrap();
        assert_eq!(seeded.fast, expected_fast);
    }

    fn stored_config(model: &Model) -> StoredAgentConfig {
        let cwd = std::env::temp_dir().join("durable-cwd");
        StoredAgentConfig {
            cwd: cwd.to_string_lossy().into_owned(),
            model: model.spec(),
            thinking: Some(StoredThinking::Off),
            fast: false,
            workflow: false,
            mode: ModeDef::default_for(makima_agent::ModeId::Build)
                .stored_snapshot(&AgentMode::Build),
            options: Default::default(),
        }
    }

    #[test_case(false ; "durable_disabled_over_defaults_enabled")]
    #[test_case(true ; "durable_enabled_over_defaults_disabled")]
    fn starting_config_uses_durable_settings_cwd_and_exact_mode(enabled: bool) {
        const MODE: &str = "unregistered-mode";
        const PROMPT: &str = "saved mode prompt";
        const TOOL: &str = "read";
        let mut model = test_model();
        model.fast_support = Some(FastSupport::Supported);
        model.thinking_support = Some(makima_providers::ThinkingSupport::Yes);
        let mut session = AppSession::new("test-model", "/tmp");
        let mut stored = stored_config(&model);
        stored.fast = enabled;
        stored.workflow = enabled;
        stored.thinking = Some(if enabled {
            StoredThinking::Effort {
                level: Effort::High,
            }
        } else {
            StoredThinking::Off
        });
        stored.mode.id = MODE.into();
        stored.mode.label = MODE.into();
        stored.mode.system_prompt = Some(PROMPT.into());
        stored.mode.tools = Some(vec![TOOL.into()]);
        session.resolve_legacy_config(stored.clone()).unwrap();
        session.meta.fast = !enabled;
        session.meta.workflow = !enabled;
        session.meta.mode = Some(StoredMode::Plan);
        session.meta.thinking = Some(StoredThinking::Adaptive);
        let mut state = resumed(session);
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let config = state
            .starting_config(
                makima_agent::tools::cli_tool_ctx(std::env::temp_dir()).provider,
                &model,
                tmp.path().to_path_buf(),
                &ModeRegistry::builtin(),
                &storage,
                SessionDefaults {
                    thinking: Some(StoredThinking::Adaptive),
                    fast: !enabled,
                    workflow: !enabled,
                },
            )
            .unwrap();
        state.adopt(&config);
        assert_eq!(config.fast, stored.fast);
        assert_eq!(config.workflow, stored.workflow);
        assert_eq!(config.thinking, stored.thinking.into());
        assert_eq!(config.context.cwd(), Path::new(&stored.cwd));
        assert_eq!(
            config
                .mode_def
                .as_ref()
                .unwrap()
                .stored_snapshot(&config.mode),
            stored.mode
        );
        assert!(state.warnings.is_empty());
    }

    #[test_case(false ; "durable_build_ignores_legacy_plan")]
    #[test_case(true ; "invalid_durable_plan_warns_before_adoption")]
    fn durable_mode_controls_fallback_warnings(invalid_plan: bool) {
        const MISSING_SLOT_WARNING: &str = "Could not restore the saved mode: Cannot restore plan file: Active Plan configuration has no plan slot entry. Resumed in read-only mode; waiting for user input.";
        let model = test_model();
        let mut session = AppSession::new("test-model", "/tmp");
        let mut stored = stored_config(&model);
        if invalid_plan {
            stored.mode.id = ModeId::Plan.key().into();
        }
        session.resolve_legacy_config(stored).unwrap();
        session.meta.mode = Some(StoredMode::Plan);
        let mut state = resumed(session);
        let tmp = tempfile::tempdir().unwrap();
        let config = state.starting_config(
            makima_agent::tools::cli_tool_ctx(std::env::temp_dir()).provider,
            &model,
            tmp.path().to_path_buf(),
            &ModeRegistry::builtin(),
            &StateDir::from_path(tmp.path().to_path_buf()),
            SessionDefaults::default(),
        );
        let config = config.unwrap();
        if invalid_plan {
            assert!(modes::is_read_only_fallback(&config.mode));
            assert_eq!(state.warnings, [MISSING_SLOT_WARNING]);
            assert!(state.plan.path().is_none());
            state.adopt(&config);
            assert_eq!(state.warnings, [MISSING_SLOT_WARNING]);
            assert!(state.plan.path().is_none());
            return;
        }
        assert_eq!(config.mode, AgentMode::Build);
        assert!(state.warnings.is_empty());
        state.adopt(&config);
        assert!(state.warnings.is_empty());
        assert!(state.plan.path().is_none());
    }

    #[test]
    fn removed_legacy_mode_restores_without_tools() {
        let directory = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let mut session = AppSession::new("test-model", "/tmp");
        session.meta.mode = Some(StoredMode::Custom("removed_mode".into()));
        let (state, config) = restore(session, &storage);
        assert!(modes::is_read_only_fallback(&config.mode));
        assert_eq!(config.mode_def.unwrap().tools, Some(Vec::new()));
        assert!(!state.warnings.is_empty());
    }

    #[test]
    fn missing_written_plan_resumes_read_only_without_recreating_ready_file() {
        let directory = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let model = test_model();
        let path = directory.path().join("missing-written.md");
        let mut session = AppSession::new("test-model", "/tmp");
        let mut stored = stored_config(&model);
        let mode = AgentMode::Plan(path.clone());
        stored.mode = ModeRegistry::builtin()
            .current(&mode)
            .stored_snapshot(&mode);
        session.resolve_legacy_config(stored).unwrap();
        let root = session.root_agent_id().unwrap();
        let seq = session
            .state()
            .iter()
            .map(|record| record.seq)
            .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
            .max()
            .unwrap()
            + 1;
        let entry = PlanEntry {
            path: path.clone(),
            written: true,
            approved: None,
            staged_model: None,
        };
        session.push_state(StoredStateRecord {
            seq,
            agent: None,
            generation: 0,
            change: StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(serde_json::to_value(PlanSlot::from([(root, entry.clone())])).unwrap()),
            },
        });
        let (state, config) = restore(session, &storage);
        assert!(modes::is_read_only_fallback(&config.mode));
        assert!(state.plan.path().is_none());
        assert!(!path.exists());
        assert!(!state.warnings.is_empty());
        assert_eq!(
            plan_state::plan_entries(&state.projection.fold().unwrap()).unwrap()[&root],
            entry
        );
    }

    #[test]
    fn model_projection_preserves_durable_fast_intent() {
        let model = test_model();
        let mut session = AppSession::new("test-model", "/tmp");
        let mut stored = stored_config(&model);
        stored
            .options
            .insert(FAST_PENDING_MODEL_OPTION_ID.into(), model.spec());
        session.resolve_legacy_config(stored.clone()).unwrap();
        let state = resumed(session);
        assert_eq!(state.projection.model(), model.spec());
        let view = state.projection.fold().unwrap();
        assert_eq!(
            view.scopes[&state.projection.root_agent_id()]
                .config
                .as_ref(),
            Some(&stored)
        );
    }

    const MISSING_CUSTOM_MODE: &str = "review";

    /// Resumes {session} the way the event loop does: seed the agent's config,
    /// then let the app adopt it.
    fn restore(session: AppSession, storage: &StateDir) -> (SessionState, EffectiveAgentConfig) {
        let model = test_model();
        let mut state = SessionState::from_session(&session).unwrap();
        let config = state
            .starting_config(
                makima_agent::tools::cli_tool_ctx(std::env::temp_dir()).provider,
                &model,
                std::env::temp_dir(),
                &ModeRegistry::builtin(),
                storage,
                SessionDefaults::default(),
            )
            .unwrap();
        state.adopt(&config);
        (state, config)
    }

    /// A catalog provider registers no models until discovery answers, so
    /// startup seeds a model that cannot say yet whether it thinks. That must
    /// not throw away the saved default.
    #[test]
    fn undiscovered_model_keeps_the_saved_thinking_default() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        write_prefs(
            &storage,
            &Prefs {
                default_thinking: Some(StoredThinking::Effort {
                    level: Effort::High,
                }),
            },
        )
        .unwrap();
        let mut model = test_model();
        model.thinking_support = None;
        let mut state = resumed(AppSession::new("test-model", "/tmp"));
        let config = state
            .starting_config(
                makima_agent::tools::cli_tool_ctx(std::env::temp_dir()).provider,
                &model,
                std::env::temp_dir(),
                &ModeRegistry::builtin(),
                &storage,
                SessionDefaults::default(),
            )
            .unwrap();
        assert_eq!(config.thinking, ThinkingConfig::Effort(Effort::High));
    }

    #[test]
    fn plan_mode_without_path_allocates_path() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let (state, config) = restore(make_plan_session(Some(StoredMode::Plan), None), &storage);
        assert!(state.plan.path().is_some(), "plan path should be allocated");
        assert_eq!(state.plan.path(), config.mode.plan_path());
    }

    #[test_case(Some(StoredMode::Plan), true ; "unallocatable_plan_path")]
    #[test_case(Some(StoredMode::Custom(MISSING_CUSTOM_MODE.into())), false ; "missing_custom_mode")]
    fn unrestorable_mode_resumes_read_only_and_warns(mode: Option<StoredMode>, blocked: bool) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("state");
        if blocked {
            std::fs::write(&dir, "").unwrap();
        }
        let (state, config) = restore(
            make_plan_session(mode.clone(), None),
            &StateDir::from_path(dir),
        );
        assert!(modes::is_read_only_fallback(&config.mode));
        assert_eq!(config.mode_def.unwrap().tools, Some(Vec::new()));
        assert!(state.plan.path().is_none());
        assert_eq!(state.projection.seed_meta().mode, mode);
        assert_eq!(state.warnings.len(), 1);
    }

    #[test]
    fn plan_mode_with_missing_file_allocates_new_path_and_warns() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let session =
            make_plan_session(Some(StoredMode::Plan), Some("/nonexistent/plan.md".into()));
        let (state, _) = restore(session, &storage);
        let path = state.plan.path().expect("plan path should be allocated");
        assert_ne!(path, Path::new("/nonexistent/plan.md"));
        assert_eq!(state.warnings, [PLAN_FILE_MISSING_WARNING]);
    }

    #[test]
    fn plan_mode_with_existing_file_preserves_path() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let plan_file = tmp.path().join("existing-plan.md");
        std::fs::write(&plan_file, "# Plan").unwrap();
        let session = make_plan_session(
            Some(StoredMode::Plan),
            Some(plan_file.to_string_lossy().into_owned()),
        );
        let (state, _) = restore(session, &storage);
        assert_eq!(state.plan.path(), Some(plan_file.as_path()));
    }

    #[test]
    fn build_mode_does_not_allocate_path() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let (state, _) = restore(make_plan_session(Some(StoredMode::Build), None), &storage);
        assert!(state.plan.path().is_none());
        assert!(state.warnings.is_empty());
    }
}
