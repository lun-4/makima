use std::path::{Path, PathBuf};
use std::sync::Arc;

use maki_agent::actor::{AgentContextSnapshot, EffectiveAgentConfig};
use maki_agent::{AgentMode, ModeRegistry, RunSettings, SessionDefaults};
use maki_agent::{modes, session_defaults};
use maki_config::{Effect, ModelPolicy};
use maki_providers::provider::{Provider, adjust_model};
use maki_providers::{Model, Timeouts, TokenUsage, settle_session};
use maki_storage::StateDir;
use maki_storage::sessions::{StoredEffect, StoredMode, StoredRule, read_prefs};

use crate::AppSession;

use super::mode::{Mode, PlanState};

/// Where the status bar's request options come from: an agent's committed
/// config, or a subagent chat's own settings once its agent is gone. Resolving
/// both together keeps a frame from mixing one source's thinking with
/// another's fast flag.
pub(crate) enum ConfigSource {
    Actor(Arc<maki_agent::EffectiveAgentConfig>),
    Chat(maki_providers::RequestOptions),
}

impl ConfigSource {
    pub(crate) fn options(&self) -> maki_providers::RequestOptions {
        match self {
            Self::Actor(config) => maki_providers::RequestOptions {
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
    /// UI-owned plan bookkeeping. The agent's committed mode carries the plan
    /// path; whether the plan is drafted or ready is only tracked here.
    pub plan: PlanState,
    pub warnings: Vec<String>,
    /// A saved `/fast` the model cannot honour until discovery says whether it
    /// supports fast. The agent runs without fast meanwhile; this keeps the
    /// wish persisted.
    pub fast_pending: bool,
}

const PLAN_FILE_MISSING_WARNING: &str = "Plan file was deleted \u{2014} started a new plan";
const MODE_RESTORE_FAILED_WARNING: &str = "Could not restore the saved mode, resumed in build mode";

impl SessionState {
    pub fn from_session(
        mut session: AppSession,
        fallback_model: &Model,
        model_policy: &ModelPolicy,
    ) -> Self {
        let model = resumed_model(&mut session, fallback_model, model_policy);

        let mut warnings = Vec::new();

        let plan = match &session.meta.plan_path {
            Some(p) if Path::new(p).exists() => {
                if session.meta.plan_written {
                    PlanState::Ready(PathBuf::from(p))
                } else {
                    PlanState::Drafting(PathBuf::from(p))
                }
            }
            Some(_) => {
                warnings.push(PLAN_FILE_MISSING_WARNING.into());
                PlanState::None
            }
            None => PlanState::None,
        };

        let fast_pending = session.meta.fast && !model.supports_fast() && model.fast_pending();
        let token_usage = session.token_usage;
        let fast = session.meta.fast && model.supports_fast();
        let cost = settle_session(&token_usage, session.usage_by_model_mut(), &model, fast);
        let context_size = session.meta.context_size;

        Self {
            fast_pending,
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
        &self,
        provider: Arc<dyn Provider>,
        model: &Model,
        cwd: PathBuf,
        registry: &ModeRegistry,
        storage: &StateDir,
        defaults: SessionDefaults,
    ) -> EffectiveAgentConfig {
        let build = AgentMode::Build;
        let (mode, mode_def) = modes::restored(registry, &self.session.meta, storage)
            .unwrap_or_else(|error| {
                tracing::warn!(%error, "resuming in build mode");
                (build.clone(), registry.current(&build))
            });
        let settings = session_defaults::resolve(
            defaults,
            &read_prefs(storage),
            &self.session.meta,
            self.session.messages().is_empty(),
            model,
        );
        EffectiveAgentConfig::new(
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
        .with_context(AgentContextSnapshot::new(cwd))
    }

    /// Adopts the plan path of the config the agent was seeded with, and
    /// says so when the saved mode could not be restored.
    pub fn adopt(&mut self, config: &EffectiveAgentConfig) {
        if config.mode == AgentMode::Build && modes::saved(&self.session.meta).is_some() {
            self.warnings.push(MODE_RESTORE_FAILED_WARNING.into());
            self.session_mut().meta.mode = Some(StoredMode::Build);
        }
        if let Some(path) = config.mode.plan_path()
            && self.plan.path() != Some(path)
        {
            self.plan = PlanState::Drafting(path.to_path_buf());
        }
    }

    /// Records the root's committed model on the session. A model switch drops
    /// a pending fast wish; discovery answering for the same model keeps it
    /// while the model can still honour it.
    pub fn update_model(&mut self, model: &Model) {
        let spec = model.spec();
        self.fast_pending &=
            self.session.model == spec && (model.fast_pending() || model.supports_fast());
        self.session_mut().set_model(spec);
    }

    /// Whether a pending fast wish can now be committed on `model`. A
    /// committed fast settles the wish.
    pub fn resolve_pending_fast(&mut self, fast: bool, model: &Model) -> bool {
        self.fast_pending &= !fast;
        self.fast_pending && model.supports_fast()
    }
}

/// The model a resumed session is accounted against (cost, a pending /fast).
fn resumed_model(
    session: &mut AppSession,
    fallback_model: &Model,
    model_policy: &ModelPolicy,
) -> Model {
    // A caller that resolved the session's own model hands in a value that
    // has already been through the provider (and its `adjust_model`), so it
    // is richer than anything a rebuild from the spec could produce. Only a
    // saved model the caller did not resolve gets rebuilt and adjusted.
    if model_policy.allows(&session.model) && fallback_model.spec() == session.model {
        fallback_model.clone()
    } else {
        match model_policy
            .allows(&session.model)
            .then(|| Model::from_spec(&session.model))
            .transpose()
            .ok()
            .flatten()
        {
            Some(mut model) => {
                // Apply the provider's per-model adjustments (e.g. ZAI's
                // glm-5.2 thinking support) so a resumed session matches one
                // started fresh.
                if let Err(e) = adjust_model(&mut model, Timeouts::default()) {
                    tracing::warn!(model = %model.id, error = %e, "failed to adjust resumed model");
                }
                model
            }
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

pub(crate) fn rules_to_stored(rules: &[maki_config::PermissionRule]) -> Vec<StoredRule> {
    rules
        .iter()
        .map(|r| {
            let effect = match r.effect {
                Effect::Allow => StoredEffect::Allow,
                Effect::Deny => StoredEffect::Deny,
            };
            StoredRule {
                tool: r.tool.to_string(),
                scope: r.scope.clone(),
                effect,
            }
        })
        .collect()
}

fn parse_stored_tool_key(s: &str) -> Option<maki_config::ToolKey> {
    match maki_config::ToolKey::parse(s) {
        Ok(key) => Some(key),
        Err(e) => {
            tracing::error!(key = s, error = %e, "malformed stored tool key — rule DROPPED; a deny rule may have been lost");
            None
        }
    }
}

pub(crate) fn stored_to_rules(stored: &[StoredRule]) -> Vec<maki_config::PermissionRule> {
    stored
        .iter()
        .filter_map(|r| {
            let tool = match parse_stored_tool_key(&r.tool) {
                Some(t) => t,
                None => {
                    if matches!(r.effect, StoredEffect::Deny) {
                        tracing::error!(
                            key = %r.tool,
                            "SECURITY: stored DENY rule dropped — tool may now be accessible. \
                             Re-add this rule manually in permissions.toml"
                        );
                    }
                    return None;
                }
            };
            let effect = match r.effect {
                StoredEffect::Allow => Effect::Allow,
                StoredEffect::Deny => Effect::Deny,
            };
            Some(maki_config::PermissionRule {
                tool,
                scope: r.scope.clone(),
                effect,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::components::{test_model, test_pricing};
    use maki_providers::model::FastSupport;
    use maki_providers::{FastPricing, ModelPricing};
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
        SessionState::from_session(session, model, &ModelPolicy::default())
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
        session.meta.plan_path = plan_path;
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

    #[test_case(FastSupport::Pending, true, false ; "pending_preserves_saved_intent")]
    #[test_case(FastSupport::Supported, false, true ; "supported_restores_fast")]
    #[test_case(FastSupport::Unsupported, false, false ; "unsupported_clears_saved_intent")]
    fn resume_fast_support(support: FastSupport, pending: bool, fast_rates: bool) {
        let mut session = session_with_counters();
        session.meta.fast = true;
        let mut model = test_model();
        model.supports_fast_override = Some(support);
        model.pricing.fast = Some(FastPricing {
            input: FAST_INPUT_RATE,
            output: test_pricing().output,
        });
        let tmp = tempfile::tempdir().unwrap();
        let storage = StateDir::from_path(tmp.path().to_path_buf());
        let state = SessionState::from_session(session, &model, &ModelPolicy::default());
        assert_eq!(state.fast_pending, pending);
        assert_eq!(
            state.cost,
            Some(if fast_rates {
                FAST_INPUT_RATE
            } else {
                LIST_PRICE
            })
        );
        let seeded = state.starting_config(
            maki_agent::tools::cli_tool_ctx(std::env::current_dir().unwrap()).provider,
            &model,
            tmp.path().to_path_buf(),
            &ModeRegistry::builtin(),
            &storage,
            SessionDefaults::default(),
        );
        assert_eq!(seeded.fast, fast_rates);
    }

    #[test_case(FastSupport::Supported, false, true ; "discovery_answers_supported")]
    #[test_case(FastSupport::Unsupported, false, false ; "discovery_rejects_fast")]
    #[test_case(FastSupport::Pending, false, false ; "still_pending_keeps_intent")]
    #[test_case(FastSupport::Supported, true, false ; "switch_answers_pending_intent")]
    fn pending_fast_model_update(support: FastSupport, switch: bool, ready: bool) {
        let mut session = session_with_counters();
        session.meta.fast = true;
        let mut model = test_model();
        model.supports_fast_override = Some(FastSupport::Pending);
        let mut state = resumed(session, &model);
        assert!(state.fast_pending);
        if switch {
            model.id = UNRESOLVABLE_MODEL.into();
        }
        model.supports_fast_override = Some(support);
        state.update_model(&model);
        assert_eq!(state.fast_pending, ready || support == FastSupport::Pending);
        assert_eq!(state.resolve_pending_fast(false, &model), ready);
        assert!(!state.resolve_pending_fast(true, &model));
        assert!(!state.fast_pending);
    }

    const MISSING_CUSTOM_MODE: &str = "review";

    /// Resumes {session} the way the event loop does: seed the agent's config,
    /// then let the app adopt it.
    fn restore(session: AppSession, storage: &StateDir) -> (SessionState, EffectiveAgentConfig) {
        let model = test_model();
        let mut state = SessionState::from_session(session, &model, &ModelPolicy::default());
        let config = state.starting_config(
            maki_agent::tools::cli_tool_ctx(std::env::temp_dir()).provider,
            &model,
            std::env::temp_dir(),
            &ModeRegistry::builtin(),
            storage,
            SessionDefaults::default(),
        );
        state.adopt(&config);
        (state, config)
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
        let (state, config) = restore(make_plan_session(mode, None), &StateDir::from_path(dir));
        assert_eq!(config.mode, AgentMode::Build);
        assert_eq!(config.mode_def.unwrap().id, maki_agent::ModeId::Build);
        assert!(state.plan.path().is_none());
        assert_eq!(state.session.meta.mode, Some(StoredMode::Build));
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
        let raw: maki_config::RawConfig = serde_json::from_value(serde_json::json!({
            "provider": {"allowed_models": [fallback.spec()]}
        }))
        .unwrap();
        let policy = raw.into_config().unwrap().provider.model_policy;

        let model = resumed_model(&mut session, &fallback, &policy);

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

    #[test]
    fn from_session_applies_provider_adjust_model() {
        // SAFETY: this test runs single-threaded; no other thread reads the env.
        unsafe { std::env::set_var("APERTURE_HOST", "https://example.com") };
        let mut session = AppSession::new("aperture/zai/glm-5.2", "/tmp");
        let model = resumed_model(&mut session, &test_model(), &ModelPolicy::default());
        assert!(
            model.supports_thinking(),
            "resumed aperture/zai/glm-5.2 should inherit thinking support from adjust_model",
        );
    }
}
