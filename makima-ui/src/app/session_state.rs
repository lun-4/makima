use color_eyre::Result;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use makima_agent::actor::{AgentContextSnapshot, EffectiveAgentConfig, PreparedModel};
use makima_agent::{AgentMode, ModeId, ModeRegistry, RunSettings, SessionDefaults};
use makima_agent::{modes, plan_state, session_defaults};
use makima_config::ModelPolicy;
use makima_providers::provider::Provider;
use makima_providers::{Model, ProviderRegistry, TokenUsage, settle_session};
use makima_storage::StateDir;
use makima_storage::session_state::StoredAgentConfig;
use makima_storage::sessions::{StoredMode, read_prefs};

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
    /// Shared with the writer thread, so a checkpoint is just a refcount bump.
    pub session: Arc<AppSession>,
    pub token_usage: TokenUsage,
    /// What the session has billed so far: the restored total plus every turn
    /// since. Kept running, because re-deriving it from the counters would
    /// re-price history at today's rates. `None` while nothing was priced.
    pub cost: Option<f64>,
    pub context_size: u32,
    pub plan: PlanState,
    pub warnings: Vec<String>,
}

const PLAN_FILE_MISSING_WARNING: &str = "Plan file was deleted \u{2014} started a new plan";
const MODE_RESTORE_FAILED_WARNING: &str = "Could not restore the saved mode, resumed in build mode";

impl SessionState {
    pub fn from_session(
        mut session: AppSession,
        fallback_model: &Model,
        model_policy: &ModelPolicy,
        providers: &ProviderRegistry,
    ) -> Self {
        let stored = match session.latest_state() {
            Ok(view) => session
                .root_agent_id()
                .and_then(|root| view.scopes.get(&Some(root)))
                .and_then(|scope| scope.config.clone()),
            Err(error) => {
                tracing::error!(%error, "could not fold session state for presentation");
                None
            }
        };
        let model = resumed_model(
            &mut session,
            stored.as_ref(),
            fallback_model,
            model_policy,
            providers,
        );
        session.set_model(model.spec());

        let mut warnings = Vec::new();
        let entries = session
            .latest_state()
            .ok()
            .and_then(|view| plan_state::plan_entries(&view).ok())
            .unwrap_or_default();
        let entry = session.root_agent_id().and_then(|root| entries.get(&root));
        let plan_path = stored.as_ref().map_or_else(
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

        let token_usage = session.token_usage;
        let fast = stored
            .as_ref()
            .map_or(session.meta.fast, |config| config.fast)
            && model.supports_fast();
        let cost = settle_session(
            &token_usage,
            session.usage_by_model_mut(),
            &model,
            fast,
            providers,
        );
        let context_size = session.meta.context_size;

        Self {
            session: Arc::new(session),
            token_usage,
            cost,
            context_size,
            plan,
            warnings,
        }
    }

    pub fn session_mut(&mut self) -> &mut AppSession {
        Arc::make_mut(&mut self.session)
    }

    /// The configuration the agent is seeded with: the session being resumed,
    /// or {defaults} for a blank one. The app shows it until the agent
    /// answers, then the agent's committed config.
    pub fn starting_config(
        &mut self,
        provider: Arc<dyn Provider>,
        model: &Model,
        cwd: PathBuf,
        registry: &ModeRegistry,
        storage: &StateDir,
        defaults: SessionDefaults,
    ) -> Result<EffectiveAgentConfig> {
        let view = self.session.latest_state()?;
        let stored = self.session.root_agent_id().and_then(|root| {
            view.scopes
                .get(&Some(root))
                .and_then(|scope| scope.config.as_ref())
        });
        let root = self.session.root_agent_id();
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
                let restored = modes::saved(&self.session.meta).map_or_else(
                    || Ok((AgentMode::Build, registry.current(&AgentMode::Build))),
                    |(key, _)| modes::resolve(registry, key, &current, storage),
                );
                restored.unwrap_or_else(|error| {
                    tracing::warn!(%error, "resuming in build mode");
                    self.warnings.push(MODE_RESTORE_FAILED_WARNING.into());
                    let build = AgentMode::Build;
                    (build.clone(), registry.current(&build))
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
            &self.session.meta,
            self.session.messages().is_empty(),
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
                let ready = self.session.latest_state().ok().is_some_and(|view| {
                    let Some(root) = self.session.root_agent_id() else {
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

    /// Updates the presentation header from the root's committed model.
    pub fn update_model(&mut self, model: &Model) {
        self.session_mut().set_model(model.spec());
    }
}

/// The model a resumed session is accounted against.
fn resumed_model(
    session: &mut AppSession,
    stored: Option<&StoredAgentConfig>,
    fallback_model: &Model,
    model_policy: &ModelPolicy,
    providers: &ProviderRegistry,
) -> Model {
    // A caller that resolved the session's own model hands in a value that
    // has already been through its provider, so it is richer than anything a
    // rebuild from the spec could produce. Only a saved model the caller did
    // not resolve gets rebuilt.
    let spec = stored.map_or(session.model.as_str(), |config| config.model.as_str());
    if model_policy.allows(spec) && fallback_model.spec() == spec {
        fallback_model.clone()
    } else {
        match model_policy
            .allows(spec)
            .then(|| providers.model(spec))
            .transpose()
            .ok()
            .flatten()
        {
            Some(model) => model,
            None => {
                session.model = fallback_model.spec();
                fallback_model.clone()
            }
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
    use crate::components::{test_model, test_pricing};
    use makima_agent::ModeDef;
    use makima_agent::session_options::FAST_PENDING_MODEL_OPTION_ID;
    use makima_providers::model::FastSupport;
    use makima_providers::{FastPricing, ModelPricing, ThinkingConfig};
    use makima_storage::id::AgentId;
    use makima_storage::plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot};
    use makima_storage::session_state::{StateChange, StoredStateRecord};
    use makima_storage::sessions::{Effort, Prefs, StoredThinking, write_prefs};
    use test_case::test_case;

    const RECORDED_COST: f64 = 0.42;
    /// A round million, so a per-million rate reads straight off the bill.
    const MILLION_INPUT: TokenUsage = TokenUsage {
        input: 1_000_000,
        output: 0,
        cache_creation: 0,
        cache_read: 0,
    };
    /// [`MILLION_INPUT`] at `test_pricing`'s standard input rate.
    const LIST_PRICE: f64 = 3.0;
    /// Twice the standard rate, so a resume that ignores `fast` bills half.
    const FAST_INPUT_RATE: f64 = 6.0;
    const UNRESOLVABLE_MODEL: &str = "a-model-no-table-has-ever-heard-of";

    fn resumed(session: AppSession, model: &Model) -> SessionState {
        SessionState::from_session(
            session,
            model,
            &ModelPolicy::default(),
            &ProviderRegistry::default(),
        )
    }

    /// An old session: counters, no per-model breakdown.
    fn session_with_counters() -> AppSession {
        let mut session = AppSession::new("test-model", "/tmp");
        session.token_usage = MILLION_INPUT;
        session
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

    /// A resumed session opens on the bill it ran up, not on its counters
    /// re-priced with whatever model is selected now. The model that recorded
    /// this one prices to nothing, so only the recorded cost can answer.
    #[test]
    fn resumed_session_opens_on_the_cost_its_turns_recorded() {
        let mut session = session_with_counters();
        session.add_model_usage(
            UNRESOLVABLE_MODEL,
            session.token_usage.billed(Some(RECORDED_COST)),
        );
        let state = resumed(session, &test_model());
        assert_eq!(state.cost, Some(RECORDED_COST));
    }

    /// Older sessions kept counters only, and those are priced with the
    /// session's own clamped `fast` flag. A hardcoded `false` would open a
    /// resumed fast session on half its bill.
    #[test_case(false => Some(LIST_PRICE)     ; "standard_rates")]
    #[test_case(true  => Some(FAST_INPUT_RATE) ; "fast_rates")]
    fn resume_without_a_breakdown_prices_the_counters(fast: bool) -> Option<f64> {
        let mut session = session_with_counters();
        session.meta.fast = fast;
        let model = Model {
            fast_support: Some(FastSupport::Supported),
            pricing: ModelPricing {
                fast: Some(FastPricing {
                    input: FAST_INPUT_RATE,
                    output: test_pricing().output,
                }),
                ..test_pricing()
            },
            ..test_model()
        };

        resumed(session, &model).cost
    }

    #[test_case(FastSupport::Pending, false ; "pending_prices_standard")]
    #[test_case(FastSupport::Supported, true ; "supported_restores_fast")]
    #[test_case(FastSupport::Unsupported, false ; "unsupported_prices_standard")]
    fn resume_fast_support(support: FastSupport, fast_rates: bool) {
        let mut session = session_with_counters();
        session.meta.fast = true;
        let mut model = test_model();
        model.fast_support = Some(support);
        model.pricing.fast = Some(FastPricing {
            input: FAST_INPUT_RATE,
            output: test_pricing().output,
        });
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let mut state = SessionState::from_session(
            session,
            &model,
            &ModelPolicy::default(),
            &ProviderRegistry::default(),
        );
        assert_eq!(
            state.cost,
            Some(if fast_rates {
                FAST_INPUT_RATE
            } else {
                LIST_PRICE
            })
        );
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
        assert_eq!(seeded.fast, fast_rates);
    }

    fn stored_config(model: &Model) -> StoredAgentConfig {
        StoredAgentConfig {
            cwd: "/durable-cwd".into(),
            model: model.spec(),
            thinking: Some(StoredThinking::Off),
            fast: false,
            workflow: false,
            mode: ModeDef::default_for(makima_agent::ModeId::Build)
                .stored_snapshot(&AgentMode::Build),
            options: Default::default(),
        }
    }

    #[test_case(false ; "durable_standard_over_legacy_fast")]
    #[test_case(true ; "durable_fast_over_legacy_standard")]
    fn folded_config_prices_the_saved_model_and_fast(fast: bool) {
        let mut model = test_model();
        model.fast_support = Some(FastSupport::Supported);
        model.pricing.fast = Some(FastPricing {
            input: FAST_INPUT_RATE,
            output: test_pricing().output,
        });
        let mut session = session_with_counters();
        let mut stored = stored_config(&model);
        stored.fast = !fast;
        session.resolve_legacy_config(stored.clone()).unwrap();
        stored.fast = fast;
        session.push_state(StoredStateRecord {
            seq: session
                .state()
                .iter()
                .map(|record| record.seq)
                .chain(session.agent_revisions().iter().map(|(seq, _)| *seq))
                .max()
                .unwrap_or(0)
                + 1,
            agent: session.root_agent_id(),
            generation: 0,
            change: StateChange::Config(stored),
        });
        session.model = UNRESOLVABLE_MODEL.into();
        session.meta.fast = !fast;
        let mut state = resumed(session, &model);
        assert_eq!(
            state.cost,
            Some(if fast { FAST_INPUT_RATE } else { LIST_PRICE })
        );
        assert!(state.session.usage_by_model().contains_key(&model.id));
        assert_eq!(state.session.model, model.spec());
        state.update_model(&Model {
            id: UNRESOLVABLE_MODEL.into(),
            ..model
        });
        assert_eq!(
            state.cost,
            Some(if fast { FAST_INPUT_RATE } else { LIST_PRICE })
        );
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
        let mut session = session_with_counters();
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
        let mut state = resumed(session, &model);
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
        const MISSING_SLOT_WARNING: &str = "Cannot restore plan file: Active Plan configuration has no plan slot entry. Resuming in Build mode";
        let model = test_model();
        let mut session = session_with_counters();
        let mut stored = stored_config(&model);
        if invalid_plan {
            stored.mode.id = ModeId::Plan.key().into();
        }
        session.resolve_legacy_config(stored).unwrap();
        session.meta.mode = Some(StoredMode::Plan);
        let mut state = resumed(session, &model);
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
        assert_eq!(config.mode, AgentMode::Build);
        if invalid_plan {
            assert_eq!(state.warnings, [MISSING_SLOT_WARNING]);
            assert!(state.plan.path().is_none());
            state.adopt(&config);
            assert_eq!(state.warnings, [MISSING_SLOT_WARNING]);
            assert!(state.plan.path().is_none());
            return;
        }
        assert!(state.warnings.is_empty());
        state.adopt(&config);
        assert!(state.warnings.is_empty());
        assert!(state.plan.path().is_none());
    }

    #[test]
    fn missing_written_plan_resumes_build_without_recreating_ready_file() {
        let directory = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(directory.path().to_path_buf());
        let model = test_model();
        let path = directory.path().join("missing-written.md");
        let mut session = session_with_counters();
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
        assert_eq!(config.mode, AgentMode::Build);
        assert!(state.plan.path().is_none());
        assert!(!path.exists());
        assert!(!state.warnings.is_empty());
        assert_eq!(
            plan_state::plan_entries(&state.session.latest_state().unwrap()).unwrap()[&root],
            entry
        );
    }

    #[test]
    fn model_presentation_update_does_not_change_durable_fast_intent() {
        let model = test_model();
        let mut session = session_with_counters();
        let mut stored = stored_config(&model);
        stored
            .options
            .insert(FAST_PENDING_MODEL_OPTION_ID.into(), model.spec());
        session.resolve_legacy_config(stored.clone()).unwrap();
        let mut state = resumed(session, &model);
        let replacement = Model {
            id: UNRESOLVABLE_MODEL.into(),
            ..model
        };
        state.update_model(&replacement);
        assert_eq!(state.session.model, replacement.spec());
        let view = state.session.latest_state().unwrap();
        assert_eq!(
            view.scopes[&state.session.root_agent_id()].config.as_ref(),
            Some(&stored)
        );
    }

    const MISSING_CUSTOM_MODE: &str = "review";

    /// Resumes {session} the way the event loop does: seed the agent's config,
    /// then let the app adopt it.
    fn restore(session: AppSession, storage: &StateDir) -> (SessionState, EffectiveAgentConfig) {
        let model = test_model();
        let mut state = SessionState::from_session(
            session,
            &model,
            &ModelPolicy::default(),
            &ProviderRegistry::default(),
        );
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
        let mut state = resumed(AppSession::new("test-model", "/tmp"), &model);
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
    fn unrestorable_mode_resumes_in_build_mode_and_warns(mode: Option<StoredMode>, blocked: bool) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("state");
        if blocked {
            std::fs::write(&dir, "").unwrap();
        }
        let (state, config) = restore(
            make_plan_session(mode.clone(), None),
            &StateDir::from_path(dir),
        );
        assert_eq!(config.mode, AgentMode::Build);
        assert_eq!(config.mode_def.unwrap().id, makima_agent::ModeId::Build);
        assert!(state.plan.path().is_none());
        assert_eq!(state.session.meta.mode, mode);
        assert_eq!(state.warnings, [MODE_RESTORE_FAILED_WARNING]);
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
    fn disallowed_restored_model_uses_fallback() {
        let fallback = test_model();
        let mut session = make_plan_session(Some(StoredMode::Build), None);
        session.model = "openai/gpt-5".into();
        let raw: makima_config::RawConfig = serde_json::from_value(serde_json::json!({
            "provider": {"allowed_models": [fallback.spec()]}
        }))
        .unwrap();
        let policy = raw.into_config().unwrap().provider.model_policy;

        let model = resumed_model(
            &mut session,
            None,
            &fallback,
            &policy,
            &ProviderRegistry::default(),
        );

        assert_eq!(model.spec(), fallback.spec());
        assert_eq!(session.model, fallback.spec());
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
