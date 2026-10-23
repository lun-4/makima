use std::any::Any;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use maki_agent::AgentRef;
use maki_agent::actor::{ConfigChange, ConfigPatch, EffectiveAgentConfig};
use maki_agent::command::SessionCommandHost;
use maki_agent::config_change::{ConfigChangeError, prepare_patch};
use maki_agent::session_coordinator::{ModelPreparer, canonical_directory, publication_claim};
use maki_commands::{
    BuiltinOperation, CommandError, CommandFuture, CommandHost, FrontendFeedback,
    HostContextRequest, HostContextResponse, HostRequest, HostResponse, ModelSelection,
};

use super::{AgentOrchestration, OrchestrationServices, TrustedTarget, reserve_config_change};

const ORCHESTRATION_CLOSED: &str = "the frontend's orchestration has closed";

/// The command host print, SDK and ACP bind for their root. Builtins that act
/// on one agent (model, fast, workflow, working directory and what they read)
/// act on the agent a command was pinned to, as the TUI's do; the rest, and
/// everything pinned to the root, go to the session.
pub struct PinnedCommandHost {
    session: SessionCommandHost,
    root: AgentRef,
    services: Weak<AgentOrchestration>,
    prepare_model: ModelPreparer,
}

impl PinnedCommandHost {
    pub fn new(
        session: SessionCommandHost,
        root: TrustedTarget,
        services: &Arc<AgentOrchestration>,
        prepare_model: ModelPreparer,
    ) -> Self {
        Self {
            root: root.target.clone(),
            session: session.with_admission_context(root),
            services: Arc::downgrade(services),
            prepare_model,
        }
    }

    /// The agent other than the root that {admission} pins a command to.
    fn pinned(&self, admission: Option<&Arc<dyn Any + Send + Sync>>) -> Option<AgentRef> {
        admission
            .and_then(|admission| admission.downcast_ref::<TrustedTarget>())
            .map(|pinned| pinned.target.clone())
            .filter(|target| target.id() != self.root.id() || !target.same_manager(&self.root))
    }

    /// Runs {request} on {target}, or hands it back when it is the session's.
    fn agent_request(
        &self,
        target: AgentRef,
        request: HostRequest,
    ) -> Result<CommandFuture<Result<HostResponse, CommandError>>, HostRequest> {
        let services = self.services.clone();
        let prepare_model = Arc::clone(&self.prepare_model);
        Ok(match request {
            HostRequest::Context(HostContextRequest::WorkingDirectory) => Box::pin(async move {
                let config = pinned_config(&target)?;
                Ok(HostResponse::Context(
                    HostContextResponse::WorkingDirectory(config.context.cwd().to_path_buf()),
                ))
            }),
            HostRequest::Context(HostContextRequest::FastModeSupported) => Box::pin(async move {
                let config = pinned_config(&target)?;
                Ok(HostResponse::Context(
                    HostContextResponse::FastModeSupported(
                        config.model.supports_fast() || config.model.fast_pending(),
                    ),
                ))
            }),
            HostRequest::Context(HostContextRequest::ModelSpecs) => Box::pin(async move {
                let specs = live(&services)?
                    .available_models(&target)
                    .map_err(|_| CommandError::StaleTarget)?;
                Ok(HostResponse::Context(HostContextResponse::Values(
                    specs.iter().map(|spec| Arc::from(spec.as_str())).collect(),
                )))
            }),
            HostRequest::Builtin(BuiltinOperation::ToggleFast) => {
                Box::pin(commit_change(services, target, ConfigChange::ToggleFast))
            }
            HostRequest::Builtin(BuiltinOperation::ToggleWorkflow) => Box::pin(commit_change(
                services,
                target,
                ConfigChange::ToggleWorkflow,
            )),
            HostRequest::Builtin(BuiltinOperation::ChangeDirectory { path }) => {
                Box::pin(change_directory(services, target, path))
            }
            HostRequest::Builtin(BuiltinOperation::SelectModel(selection)) => {
                Box::pin(select_model(services, target, selection, prepare_model))
            }
            HostRequest::Builtin(BuiltinOperation::SetModel { spec }) => Box::pin(select_model(
                services,
                target,
                ModelSelection {
                    spec: Some(spec),
                    ..Default::default()
                },
                prepare_model,
            )),
            request => return Err(request),
        })
    }
}

impl CommandHost for PinnedCommandHost {
    fn admission_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.session.admission_context()
    }

    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        self.session.request(request)
    }

    fn request_with_admission(
        &self,
        request: HostRequest,
        admission: Option<Arc<dyn Any + Send + Sync>>,
    ) -> CommandFuture<Result<HostResponse, CommandError>> {
        match self.pinned(admission.as_ref()) {
            Some(target) => self
                .agent_request(target, request)
                .unwrap_or_else(|request| self.session.request(request)),
            None => self.session.request(request),
        }
    }
}

fn producer_error(error: impl ToString) -> CommandError {
    CommandError::Producer(Arc::from(error.to_string()))
}

fn live(services: &Weak<AgentOrchestration>) -> Result<Arc<AgentOrchestration>, CommandError> {
    services
        .upgrade()
        .ok_or_else(|| producer_error(ORCHESTRATION_CLOSED))
}

fn pinned_config(target: &AgentRef) -> Result<Arc<EffectiveAgentConfig>, CommandError> {
    target
        .effective_config()
        .ok()
        .flatten()
        .ok_or(CommandError::StaleTarget)
}

async fn commit_change(
    services: Weak<AgentOrchestration>,
    target: AgentRef,
    change: ConfigChange,
) -> Result<HostResponse, CommandError> {
    let services = live(&services)?;
    reserve_config_change(&*services, &target)
        .map_err(producer_error)?
        .commit_change(change)
        .await
        .map_err(producer_error)?;
    Ok(HostResponse::Completed)
}

async fn change_directory(
    services: Weak<AgentOrchestration>,
    target: AgentRef,
    path: PathBuf,
) -> Result<HostResponse, CommandError> {
    let services = live(&services)?;
    let commit = reserve_config_change(&*services, &target)
        .map_err(producer_error)?
        .commit(async |_, predecessor| {
            let path = canonical_directory(predecessor.context.cwd(), path).await?;
            Ok((publication_claim(), ConfigChange::WorkingDirectory(path)))
        })
        .await
        .map_err(producer_error)?;
    Ok(HostResponse::FrontendFeedback(
        FrontendFeedback::WorkingDirectory(commit.config.context.cwd().to_path_buf()),
    ))
}

async fn select_model(
    services: Weak<AgentOrchestration>,
    target: AgentRef,
    selection: ModelSelection,
    prepare_model: ModelPreparer,
) -> Result<HostResponse, CommandError> {
    let services = live(&services)?;
    let policy = services
        .template(&target)
        .map_err(|_| CommandError::StaleTarget)?
        .model_policy;
    reserve_config_change(&*services, &target)
        .map_err(producer_error)?
        .commit(async |_, _| {
            let patch = ConfigPatch {
                thinking: selection.thinking,
                fast: selection.fast,
                ..Default::default()
            };
            let patch =
                prepare_patch(patch, selection.spec.as_deref(), &policy, &prepare_model).await?;
            Ok::<_, ConfigChangeError>((publication_claim(), ConfigChange::Patch(patch)))
        })
        .await
        .map_err(producer_error)?;
    Ok(HostResponse::Completed)
}
