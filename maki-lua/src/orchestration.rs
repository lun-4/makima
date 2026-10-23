use crate::agent_errors::{
    NO_ORCHESTRATION_SERVICES, ORCHESTRATION_CONTEXT_EXPIRED, ORCHESTRATION_SERVICES_EXPIRED,
};
use crate::runtime::{TaskHandle, active_task_id, lock_cell};
use maki_agent::actor::{ConfigChange, ConfigCommit, EffectiveAgentConfig, PreparedTransaction};
use maki_agent::cancel::{CancelToken, CancelTrigger, PublicationRegistration};
use maki_agent::command::command_input;
use maki_agent::config_change::{ConfigChangeError, ConfigChangeTicket};
use maki_agent::headless::{InteractiveControl, InteractiveHandle, RootBinding};
use maki_agent::permissions::PermissionManager;
use maki_agent::session_checkpoint::SessionLogCheckpoint;
use maki_agent::session_coordinator::{
    ConfigMutation, ModelPreparer, SessionCoordinatorHandle, publication_claim,
};
use maki_agent::session_options::YOLO_OPTION_ID;
use maki_agent::{
    AgentRef, ModeRegistry, TurnAdmissionOptions, TurnOrigin, TurnProvenance, modes,
    tools::ToolContext,
};
use maki_commands::{
    CommandContent, CommandError, CommandInvocation, CommandOutcome, CommandRegistry,
    InputDispatch, ResolutionError, TargetHandle,
};
use maki_providers::Message;
use maki_storage::StateDir;
use maki_storage::checkpoint::{CheckpointClaim, CheckpointPublication};
use maki_storage::sessions::{HistorySnapshot, SessionMeta, StoredSubagent};
use mlua::Lua;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

mod catalog;
mod command_host;
mod targets;
pub use catalog::{ACTIVE_DISCOVERY_BOUND, Discovery, ModelCatalog};
pub use command_host::PinnedCommandHost;
pub use targets::AgentOrchestration;

const WRONG_TASK: &str = "orchestration context belongs to another task";
pub const DURABLE_CONFIG_UNAVAILABLE: &str = "durable configuration adapter is unavailable";
static NEXT_PLUGIN_GENERATION: AtomicU64 = AtomicU64::new(1);

pub(crate) struct PluginAuthority {
    alive: AtomicBool,
    generation: u64,
    publication_gate: Mutex<()>,
    publication_cancel: CancelToken,
    publication_trigger: Mutex<Option<CancelTrigger>>,
}

impl PluginAuthority {
    fn new(alive: bool, generation: u64) -> Self {
        let (trigger, cancel) = CancelToken::new();
        let authority = Self {
            alive: AtomicBool::new(alive),
            generation,
            publication_gate: Mutex::new(()),
            publication_cancel: cancel,
            publication_trigger: Mutex::new(Some(trigger)),
        };
        if !alive {
            authority.revoke();
        }
        authority
    }

    fn revoke(&self) {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.alive.store(false, Ordering::Release);
        self.publication_trigger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
    }

    pub(crate) fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> PublicationRegistration {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.publication_cancel.register_publication(publication)
    }

    pub(crate) fn claim_publication(
        &self,
        claim: impl FnOnce() -> Result<(), Arc<str>>,
    ) -> Result<(), Arc<str>> {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !self.is_alive() {
            return Err(Arc::from(crate::agent_errors::INVOCATION_EXPIRED));
        }
        claim()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
}

#[derive(Clone)]
pub struct TrustedTarget {
    pub target: AgentRef,
    pub template: ToolContext,
}

pub type ConfigMutationReserver = Arc<
    dyn Fn(&AgentRef, CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError>
        + Send
        + Sync,
>;

pub type TurnCompletionFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// Runs a slash command a plugin asked for outside a command invocation,
/// pinned to the given target, at the given hop depth. Resolves once the
/// handler has finished and the frontend has taken its outcome; a model turn
/// or `/compact` it started may still be running.
pub type CommandRunner =
    Arc<dyn Fn(String, u8, TrustedTarget) -> TurnCompletionFuture + Send + Sync>;

/// Runs the outcome of a plugin-run command that is neither finished nor
/// failed: a model turn, compaction, an isolated turn or feedback.
pub type OutcomeExecutor =
    Arc<dyn Fn(String, CommandOutcome, TrustedTarget) -> TurnCompletionFuture + Send + Sync>;

pub(crate) const NEEDS_FRONTEND: &str = "needs a frontend to run it";
pub const CLOSED_TARGET_ERR: &str = "the agent this command was pinned to has closed";

/// The one runner every frontend binds: {cmdline} resolves through {target},
/// runs on the agent the caller pinned at its hop depth, and waits for the
/// handler; a handler error fails the call, and every other outcome goes to
/// the frontend's {execute}.
pub fn command_runner(
    registry: CommandRegistry,
    target: TargetHandle,
    execute: OutcomeExecutor,
) -> CommandRunner {
    Arc::new(move |cmdline, depth, admission| {
        let dispatch = admission
            .target
            .validate_live()
            .map_err(|_| CLOSED_TARGET_ERR.to_owned())
            .and_then(|()| {
                registry
                    .resolve_input_for(&target, &cmdline)
                    .map_err(|error| match error {
                        ResolutionError::UnknownCommand(name) => {
                            format!("unknown command '{name}'")
                        }
                        ResolutionError::StaleTarget => error.to_string(),
                    })
            })
            .map(|resolved| {
                registry.dispatch_command_with_admission(
                    &target,
                    resolved.command,
                    resolved.arguments,
                    CommandContent::default(),
                    depth.into(),
                    Arc::new(admission.clone()),
                )
            });
        let execute = Arc::clone(&execute);
        Box::pin(async move {
            match dispatch?.await {
                CommandOutcome::Completed => Ok(()),
                CommandOutcome::Failed(error) => detached_outcome(
                    &cmdline,
                    InputDispatch::Dispatched(CommandOutcome::Failed(error)),
                ),
                outcome => execute(cmdline, outcome, admission).await,
            }
        })
    })
}

/// How print, SDK and ACP run a plugin-run command's outcome on the agent it
/// was pinned to, as the TUI does: model turns are admitted on it and
/// compaction is queued on it (through the frontend's own paths when that is
/// the root), and feedback has nowhere to show. An isolated turn's answer
/// belongs to the prompt that asked for it, and a plugin-run one has no
/// prompt to answer.
pub fn headless_outcomes(handle: &InteractiveHandle) -> OutcomeExecutor {
    let root = handle.root.clone();
    let admitter = handle.input_tx.admitter();
    let control = handle.control_tx.clone();
    Arc::new(move |cmdline, outcome, admission| {
        let root = root.clone();
        let admitter = admitter.clone();
        let control = control.clone();
        Box::pin(async move {
            let root = root.get().await?;
            let target = admission.target;
            let child = (target.id() != root.id() || !target.same_manager(&root))
                .then(|| target.actor().map_err(|error| error.to_string()))
                .transpose()?;
            match (outcome, child) {
                (CommandOutcome::FrontendFeedback(_), _) => Ok(()),
                (CommandOutcome::AgentTurn(turn), None) => {
                    admitter.admit(command_input(turn)?).await.map(|_| ())
                }
                (CommandOutcome::AgentTurn(turn), Some(actor)) => actor
                    .admit_turn_with_options(
                        command_input(turn)?,
                        None,
                        String::new(),
                        TurnAdmissionOptions {
                            provenance: TurnProvenance {
                                origin: TurnOrigin::User,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    )
                    .map(|_| ())
                    .map_err(|error| error.to_string()),
                (CommandOutcome::ManualCompaction(instructions), None) => {
                    let (output, _) = flume::unbounded();
                    control
                        .send(InteractiveControl::ManualCompaction {
                            instructions,
                            output,
                            cancel: CancelToken::none(),
                        })
                        .await
                }
                (CommandOutcome::ManualCompaction(instructions), Some(actor)) => {
                    let run_id = actor.reserve_presentation(actor.presentation_id());
                    actor
                        .push_compact(run_id, instructions)
                        .map_err(|error| error.to_string())
                }
                (outcome, _) => detached_outcome(&cmdline, InputDispatch::Dispatched(outcome)),
            }
        })
    })
}

/// Only `Completed` is finished by the time a detached dispatch returns, so
/// every outcome that still needs a frontend loop to run it is an error
/// rather than a `true` the caller would read as "it ran".
pub(crate) fn detached_outcome(cmdline: &str, result: InputDispatch) -> Result<(), String> {
    match result {
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(_)))
        | InputDispatch::LiteralInput(_) => Err("unknown command".to_owned()),
        InputDispatch::Dispatched(CommandOutcome::Failed(error)) => Err(error.to_string()),
        InputDispatch::Dispatched(CommandOutcome::Completed) => Ok(()),
        InputDispatch::Dispatched(
            CommandOutcome::AgentTurn(_)
            | CommandOutcome::IsolatedTurn(_)
            | CommandOutcome::ManualCompaction(_)
            | CommandOutcome::FrontendFeedback(_),
        ) => Err(format!("{cmdline} {NEEDS_FRONTEND}")),
    }
}

pub type TurnCompletion =
    Arc<dyn Fn(&AgentRef, HistorySnapshot<Message>) -> TurnCompletionFuture + Send + Sync>;

/// Builds the child history/config persistence hooks every non-UI frontend binds
/// to its root graph. `label` names the frontend in the closed-graph error.
pub fn child_persistence(
    label: &'static str,
    services: Weak<AgentOrchestration>,
    checkpoint: Arc<SessionLogCheckpoint>,
) -> (ConfigMutationReserver, TurnCompletion) {
    let destination = move |target: &AgentRef| -> Result<(String, String), String> {
        let services = services
            .upgrade()
            .ok_or_else(|| format!("{label} orchestration is closed"))?;
        let key = services.history_key(target)?;
        let name = target
            .snapshot()
            .map_err(|error| error.to_string())?
            .metadata
            .label
            .unwrap_or_default();
        Ok((key, name))
    };
    let config_destination = destination.clone();
    let completion_destination = destination;
    let config_checkpoint = Arc::clone(&checkpoint);

    let reserver: ConfigMutationReserver = Arc::new(move |target, publication| {
        let (key, name) = config_destination(target)?;
        let checkpoint = Arc::clone(&config_checkpoint);
        Ok(Box::new(move |_, claim| {
            Ok(PreparedTransaction {
                publication,
                publish: Box::new(move |ready, publication| {
                    Box::pin(async move {
                        checkpoint
                            .checkpoint_child_config(
                                child_metadata(key, name, &ready.config),
                                publication,
                                claim,
                            )
                            .await
                            .map_err(|error| error.to_string())
                    })
                }),
                finalize: Box::new(|_| {}),
            })
        }))
    });

    let completion: TurnCompletion = Arc::new(move |target, history| {
        let child = (|| {
            let (key, name) = completion_destination(target)?;
            let config = target
                .effective_config()
                .map_err(|error| error.to_string())?
                .ok_or("child agent configuration is unavailable")?;
            Ok::<_, String>(child_metadata(key, name, &config))
        })();
        let checkpoint = Arc::clone(&checkpoint);
        Box::pin(async move {
            checkpoint
                .checkpoint_child(child?, history)
                .await
                .map_err(|error| error.to_string())
        })
    });

    (reserver, completion)
}

pub fn child_metadata(key: String, name: String, config: &EffectiveAgentConfig) -> StoredSubagent {
    StoredSubagent {
        tool_use_id: key,
        name,
        model: Some(config.model.spec()),
        thinking: Some(config.thinking.into()),
        fast: config.fast,
    }
}

/// A root binding that hands the bound root and its template to the frontend.
pub fn root_binding() -> (RootBinding, flume::Receiver<TrustedTarget>) {
    let (ready, root) = flume::bounded(1);
    let binding: RootBinding = Box::new(move |_, target, template| {
        Box::pin(async move {
            ready
                .send(TrustedTarget { target, template })
                .map_err(|error| error.to_string())
        })
    });
    (binding, root)
}

/// Binds a headless frontend's root actor to {coordinator}, so committed
/// config installs the model and cwd on {handle} and the YOLO option drives
/// its permissions, and returns the root that {root_binding} delivered.
pub async fn bind_frontend_root(
    handle: &InteractiveHandle,
    coordinator: &SessionCoordinatorHandle,
    root_binding: &flume::Receiver<TrustedTarget>,
    prepare_model: ModelPreparer,
) -> Result<TrustedTarget, String> {
    let root = handle.initialized_root().await?;
    let model = handle.model.clone();
    project_yolo(coordinator, Arc::clone(&handle.permissions));
    let permissions = Arc::clone(&handle.permissions);
    coordinator
        .bind_actor(
            root,
            prepare_model,
            Arc::new(move |commit| {
                model.install(
                    Arc::clone(&commit.config.provider),
                    commit.config.model.clone(),
                );
                permissions.set_cwd(commit.config.context.cwd().to_path_buf());
            }),
        )
        .map_err(|error| error.to_string())?;
    root_binding
        .recv_async()
        .await
        .map_err(|error| error.to_string())
}

/// Keeps {permissions}' YOLO state on {coordinator}'s committed option,
/// inside the commit. An unchanged value leaves a session that never chose
/// YOLO on its seed.
pub fn project_yolo(coordinator: &SessionCoordinatorHandle, permissions: Arc<PermissionManager>) {
    coordinator.read().project_options(move |options| {
        let enabled = options.enabled(YOLO_OPTION_ID);
        if permissions.is_yolo() != enabled {
            permissions.set_yolo(enabled);
        }
    });
}

/// The orchestration a non-UI frontend binds to its root graph: the root's
/// config commits through {coordinator} and its children persist to
/// {checkpoint}, and {catalog}'s discovery refreshes it. `label` names the
/// frontend in the closed-graph error.
pub fn durable_frontend(
    label: &'static str,
    catalog: &ModelCatalog,
    root: TrustedTarget,
    coordinator: SessionCoordinatorHandle,
    checkpoint: Arc<SessionLogCheckpoint>,
) -> Arc<AgentOrchestration> {
    let orchestration = Arc::new_cyclic(|services| {
        let (child_config, completion) = child_persistence(label, services.clone(), checkpoint);
        frontend_root(catalog, root, child_config, coordinator).with_completion(completion)
    });
    catalog.attach(&orchestration);
    orchestration
}

/// The root half of [`durable_frontend`]: root config goes through
/// {coordinator}, children through {child_config}.
pub fn frontend_root(
    catalog: &ModelCatalog,
    target: TrustedTarget,
    child_config: ConfigMutationReserver,
    coordinator: SessionCoordinatorHandle,
) -> AgentOrchestration {
    let root_id = target.target.id();
    let root = target.target.clone();
    let services = AgentOrchestration::new(catalog.specs());
    services.register_root(
        target,
        Some(Arc::new(move |target, publication| {
            if target.id() != root_id {
                return child_config(target, publication);
            }
            Ok(coordinator
                .reserve_transaction(publication)?
                .into_config_mutation())
        })),
    );
    services.select(Some(root));
    services
}

/// Children apply config changes without persistence; a root needs a durable
/// adapter.
fn default_config_mutation(
    target: &AgentRef,
    publication: CheckpointPublication,
) -> Result<ConfigMutation, ConfigChangeError> {
    if target.snapshot()?.parent_id.is_none() {
        return Err(DURABLE_CONFIG_UNAVAILABLE.into());
    }
    Ok(nonpersistent_config_mutation(publication))
}

/// Reserves {target}'s next slot in its config FIFO.
pub fn reserve_config_change<'a>(
    services: &'a (impl OrchestrationServices + ?Sized),
    target: &'a AgentRef,
) -> Result<ConfigChangeTicket<'a>, ConfigChangeError> {
    ConfigChangeTicket::reserve(target, CheckpointPublication::default(), |publication| {
        services.reserve_config_mutation(target, publication)
    })
}

/// Commits one config change through {target}'s FIFO; see
/// [`ConfigChangeTicket::commit`] for what `prepare` receives.
pub async fn commit_config_change(
    services: &(impl OrchestrationServices + ?Sized),
    target: &AgentRef,
    prepare: impl AsyncFnOnce(
        CheckpointPublication,
        Arc<EffectiveAgentConfig>,
    ) -> Result<(CheckpointClaim, ConfigChange), ConfigChangeError>,
) -> Result<ConfigCommit, ConfigChangeError> {
    reserve_config_change(services, target)?
        .commit(prepare)
        .await
}

/// Sets mode {key} on {target}; see [`ConfigChangeTicket::commit_mode`].
pub async fn set_mode(
    services: &(impl OrchestrationServices + ?Sized),
    target: &AgentRef,
    registry: &ModeRegistry,
    key: &str,
    state_dir: &StateDir,
) -> Result<ConfigCommit, ConfigChangeError> {
    reserve_config_change(services, target)?
        .commit_mode(target, registry, key, state_dir, None, async |_, _| {
            Ok(publication_claim())
        })
        .await
}

/// Puts a resumed session's root back in the mode it was saved in, keeping a
/// plan's file while it still exists. Every frontend that resumes calls it.
pub async fn restore_mode(
    services: &(impl OrchestrationServices + ?Sized),
    target: &AgentRef,
    registry: &ModeRegistry,
    meta: &SessionMeta,
    state_dir: &StateDir,
) -> Result<(), ConfigChangeError> {
    let Some((key, saved_plan)) = modes::saved(meta) else {
        return Ok(());
    };
    reserve_config_change(services, target)?
        .commit_mode(
            target,
            registry,
            key,
            state_dir,
            saved_plan,
            async |_, _| Ok(publication_claim()),
        )
        .await?;
    Ok(())
}

pub fn nonpersistent_config_mutation(publication: CheckpointPublication) -> ConfigMutation {
    Box::new(move |_, claim| {
        Ok(PreparedTransaction {
            publication,
            publish: Box::new(move |_, publication| {
                Box::pin(async move { claim(&publication).map_err(|error| error.to_string()) })
            }),
            finalize: Box::new(|_| {}),
        })
    })
}

pub trait OrchestrationServices: Send + Sync + 'static {
    fn resolve_target(
        &self,
        invocation: Option<&CommandInvocation>,
    ) -> Result<Option<TrustedTarget>, String>;

    fn validate_target(&self, target: &AgentRef) -> Result<(), String> {
        self.template(target).map(|_| ())
    }

    fn reserve_config_mutation(
        &self,
        target: &AgentRef,
        publication: CheckpointPublication,
    ) -> Result<ConfigMutation, ConfigChangeError> {
        self.validate_target(target)?;
        default_config_mutation(target, publication)
    }

    /// Completes the host's persistence attempt before the backend returns.
    /// The callback must not await work on the target's occupied runner.
    fn complete_turn(
        &self,
        _target: &AgentRef,
        _history: HistorySnapshot<Message>,
    ) -> TurnCompletionFuture {
        Box::pin(async { Ok(()) })
    }

    fn register_child_target(
        &self,
        _target: TrustedTarget,
        _history_key: String,
    ) -> Result<(), String> {
        Err("host target registration is unavailable".into())
    }

    fn template(&self, _target: &AgentRef) -> Result<ToolContext, String> {
        Err("host target template is unavailable".into())
    }

    fn available_models(&self, _target: &AgentRef) -> Result<Vec<String>, String> {
        Err("host model catalog is unavailable".into())
    }

    fn command_runner(&self, _target: &AgentRef) -> Result<CommandRunner, String> {
        Err("host command runner is unavailable".into())
    }
}

#[derive(Clone, Default)]
pub struct OrchestrationServicesSlot(pub Option<Arc<dyn OrchestrationServices>>);

/// The orchestration services the host installed, if any.
pub(crate) fn services(lua: &Lua) -> Option<Arc<dyn OrchestrationServices>> {
    lua.app_data_ref::<OrchestrationServicesSlot>()
        .and_then(|slot| slot.0.clone())
}

#[derive(Default)]
struct PluginAuthorities(HashMap<Arc<str>, Arc<PluginAuthority>>);

pub(crate) fn revoke_plugin(lua: &Lua, plugin: &str) {
    if let Some(mut owners) = lua.app_data_mut::<PluginAuthorities>()
        && let Some(alive) = owners.0.remove(plugin)
    {
        alive.revoke();
    }
    crate::api::agent::close_plugin_agents(lua, plugin);
    if let Some(mut store) = lua.app_data_mut::<crate::api::autocmd::AutocmdStore>() {
        store.revoke_plugin(plugin);
    }
}

/// The plugin's current authority. A plugin with no committed load, such as
/// one being unloaded, gets an authority that is already revoked, so nothing
/// minted or validated against it can act.
pub(crate) fn plugin_authority(lua: &Lua, plugin: Arc<str>) -> Arc<PluginAuthority> {
    lua.app_data_ref::<PluginAuthorities>()
        .and_then(|owners| owners.0.get(&plugin).cloned())
        .unwrap_or_else(|| Arc::new(PluginAuthority::new(false, 0)))
}

/// Whether the running code belongs to {plugin} at {generation} and the
/// plugin's live authority is still {owner}.
pub(crate) fn holds_plugin_grant(
    lua: &Lua,
    plugin: &Arc<str>,
    generation: u64,
    owner: &Arc<PluginAuthority>,
) -> bool {
    crate::runtime::current_plugin(lua).is_some_and(|(current, current_generation)| {
        current == *plugin && current_generation == generation
    }) && Arc::ptr_eq(owner, &plugin_authority(lua, plugin.clone()))
}

/// Grants a fresh authority when a plugin load commits; the previous one
/// must already be revoked.
pub(crate) fn install_plugin_authority(lua: &Lua, plugin: Arc<str>) -> Arc<PluginAuthority> {
    if lua.app_data_ref::<PluginAuthorities>().is_none() {
        lua.set_app_data(PluginAuthorities::default());
    }
    let authority = Arc::new(PluginAuthority::new(
        true,
        NEXT_PLUGIN_GENERATION.fetch_add(1, Ordering::Relaxed),
    ));
    lua.app_data_mut::<PluginAuthorities>()
        .expect("authority map installed")
        .0
        .insert(plugin, Arc::clone(&authority));
    authority
}

#[derive(Clone)]
pub(crate) struct TrustedContext {
    pub(crate) target: AgentRef,
    pub(crate) template: ToolContext,
    pub(crate) plugin: Arc<str>,
    pub(crate) generation: u64,
    services: Option<Arc<dyn OrchestrationServices>>,
    authority: ScopeAuthority,
}

#[derive(Clone)]
struct ScopeAuthority {
    task: Weak<Mutex<crate::runtime::TaskCell>>,
    owner: Arc<PluginAuthority>,
}

impl TrustedContext {
    pub(crate) fn new(
        lua: &Lua,
        target: TrustedTarget,
        task: &TaskHandle,
        plugin: Arc<str>,
        owner: Arc<PluginAuthority>,
    ) -> Self {
        Self {
            target: target.target,
            template: target.template,
            plugin,
            generation: owner.generation,
            services: services(lua),
            authority: ScopeAuthority {
                task: Arc::downgrade(task),
                owner,
            },
        }
    }

    /// Checks plugin and host authority only. The pinned target may have
    /// closed since; operations that address it check it themselves.
    pub(crate) fn validate(&self, lua: &Lua) -> Result<(), String> {
        self.authority.validate(lua)?;
        let captured = self.services.as_ref().ok_or(NO_ORCHESTRATION_SERVICES)?;
        let current = services(lua).ok_or(ORCHESTRATION_SERVICES_EXPIRED)?;
        if !Arc::ptr_eq(captured, &current) {
            return Err("agent target does not belong to this host scope".into());
        }
        Ok(())
    }

    pub(crate) fn validate_target(&self, lua: &Lua, target: &AgentRef) -> Result<(), String> {
        self.validate(lua)?;
        self.services
            .as_ref()
            .ok_or(NO_ORCHESTRATION_SERVICES)?
            .validate_target(target)
    }
}

impl ScopeAuthority {
    fn validate(&self, lua: &Lua) -> Result<(), String> {
        let task = self.task.upgrade().ok_or(ORCHESTRATION_CONTEXT_EXPIRED)?;
        let current_id = active_task_id(lua);
        let cell = lock_cell(&task);
        if !cell.is_live() || !self.owner.alive.load(Ordering::Acquire) {
            return Err(ORCHESTRATION_CONTEXT_EXPIRED.into());
        }
        if current_id != Some(cell.id) {
            return Err(WRONG_TASK.into());
        }
        Ok(())
    }
}

pub(crate) fn resolve_target(
    lua: &Lua,
    invocation: Option<&CommandInvocation>,
) -> Result<Option<TrustedTarget>, String> {
    let services = services(lua);
    let Some(services) = services else {
        return Ok(None);
    };
    let target = services.resolve_target(invocation)?;
    if let Some(target) = &target {
        services.validate_target(&target.target)?;
    }
    Ok(target)
}

/// Revalidates a target the host pinned before the request reached Lua; a
/// target that closed in between resolves to none rather than to focus.
pub(crate) fn validate_pinned_target(
    lua: &Lua,
    target: Option<TrustedTarget>,
) -> Result<Option<TrustedTarget>, String> {
    let Some(target) = target.filter(|target| target.target.validate_live().is_ok()) else {
        return Ok(None);
    };
    services(lua)
        .ok_or(NO_ORCHESTRATION_SERVICES)?
        .validate_target(&target.target)?;
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::{
        CommandContent, CommandError, CommandOutcome, InputDispatch, NEEDS_FRONTEND,
        ScopeAuthority, WRONG_TASK, detached_outcome, install_plugin_authority, plugin_authority,
        revoke_plugin,
    };
    use crate::agent_errors::ORCHESTRATION_CONTEXT_EXPIRED;
    use crate::runtime::{TaskCell, TaskScope};
    use maki_agent::cancel::CancelToken;
    use mlua::Lua;
    use std::sync::Arc;

    #[test]
    fn authority_rejects_sibling_scope_reload_and_scope_exit() {
        let lua = Lua::new();
        let first = TaskScope::detached(&lua);
        let mut authority = ScopeAuthority {
            task: Arc::downgrade(first.handle()),
            owner: install_plugin_authority(&lua, Arc::from("owner")),
        };
        assert!(authority.validate(&lua).is_ok());
        let sibling = TaskScope::detached(&lua);
        assert_eq!(authority.validate(&lua).unwrap_err(), WRONG_TASK);
        drop(sibling);
        assert!(authority.validate(&lua).is_ok());
        revoke_plugin(&lua, "owner");
        assert_eq!(
            authority.validate(&lua).unwrap_err(),
            ORCHESTRATION_CONTEXT_EXPIRED
        );
        assert!(!plugin_authority(&lua, Arc::from("owner")).is_alive());
        authority.owner = install_plugin_authority(&lua, Arc::from("owner"));
        assert!(authority.validate(&lua).is_ok());
        let retained = first.handle().clone();
        drop(first);
        assert_eq!(
            authority.validate(&lua).unwrap_err(),
            ORCHESTRATION_CONTEXT_EXPIRED
        );
        drop(retained);
    }

    #[test]
    fn cancellation_expires_scope() {
        let lua = Lua::new();
        let (trigger, cancel) = CancelToken::new();
        let scope = TaskScope::new(&lua, TaskCell::new(cancel, None, None));
        let authority = ScopeAuthority {
            task: Arc::downgrade(scope.handle()),
            owner: install_plugin_authority(&lua, Arc::from("owner")),
        };
        drop(trigger);
        assert_eq!(
            authority.validate(&lua).unwrap_err(),
            ORCHESTRATION_CONTEXT_EXPIRED
        );
    }

    #[test_case::test_case(
        InputDispatch::Dispatched(CommandOutcome::Failed(CommandError::UnknownCommand(
            Arc::from("/bogus")
        ))),
        Err("unknown command".to_owned()) ;
        "unknown command is rejected"
    )]
    #[test_case::test_case(
        InputDispatch::Dispatched(CommandOutcome::Completed),
        Ok(()) ;
        "dispatched command succeeds"
    )]
    #[test_case::test_case(
        InputDispatch::LiteralInput(CommandContent::from("prose")),
        Err("unknown command".to_owned()) ;
        "literal input is not a command"
    )]
    #[test_case::test_case(
        InputDispatch::Dispatched(CommandOutcome::AgentTurn(maki_commands::AgentTurn {
            content: CommandContent::from("review this"),
            prompt: None,
        })),
        Err(format!("/nested {NEEDS_FRONTEND}")) ;
        "an agent turn has no frontend to run it"
    )]
    #[test_case::test_case(
        InputDispatch::Dispatched(CommandOutcome::IsolatedTurn(maki_commands::IsolatedTurn {
            content: CommandContent::from("why?"),
        })),
        Err(format!("/nested {NEEDS_FRONTEND}")) ;
        "an isolated turn has no frontend to run it"
    )]
    #[test_case::test_case(
        InputDispatch::Dispatched(CommandOutcome::ManualCompaction(None)),
        Err(format!("/nested {NEEDS_FRONTEND}")) ;
        "manual compaction has no frontend to run it"
    )]
    #[test_case::test_case(
        InputDispatch::Dispatched(CommandOutcome::FrontendFeedback(
            maki_commands::FrontendFeedback::Text(Arc::from("use session/new")),
        )),
        Err(format!("/nested {NEEDS_FRONTEND}")) ;
        "frontend feedback has no frontend to show it"
    )]
    fn detached_outcome_maps_outcomes(result: InputDispatch, expected: Result<(), String>) {
        assert_eq!(detached_outcome("/nested", result), expected);
    }
}
