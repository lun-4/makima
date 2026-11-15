use std::any::Any;
use std::path::PathBuf;
use std::sync::{Arc, Weak};

use makima_agent::AgentActorHandle;
use makima_agent::actor::{ConfigChange, EffectiveAgentConfig};
use makima_agent::command::SessionCommandHost;
use makima_agent::config_change::{ConfigChangeError, ConfigChangeTicket, prepare_selection};
use makima_agent::session_coordinator::{ModelPreparer, canonical_directory, publication_claim};
use makima_commands::{
    BuiltinOperation, CommandError, CommandFuture, CommandHost, FrontendFeedback,
    HostContextRequest, HostContextResponse, HostRequest, HostResponse, ModelSelection,
};
use makima_storage::checkpoint::CheckpointPublication;

use super::{
    AgentOrchestration, CLOSED_TARGET_ERR, OrchestrationServices, PinnedCommandTarget,
    TrustedTarget,
};

const ORCHESTRATION_CLOSED: &str = "the frontend's orchestration has closed";
const CHILD_SESSION_RESET: &str = "resetting the session is unsupported for child agents";

/// The command host print, SDK and ACP bind for their root. Agent-local
/// operations use the captured actor, including for the root. Session effects
/// delegate with the captured root actor and an admission validator.
pub struct PinnedCommandHost {
    session: SessionCommandHost,
    root: TrustedTarget,
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
            root,
            session,
            services: Arc::downgrade(services),
            prepare_model,
        }
    }

    /// Runs {request} on {target}, or hands it back when it is the session's.
    fn agent_request(
        &self,
        target: PinnedCommandTarget,
        request: HostRequest,
    ) -> Result<CommandFuture<Result<HostResponse, CommandError>>, HostRequest> {
        let actor = match target.actor() {
            Ok(actor) => actor,
            Err(_) => return Ok(Box::pin(async { Err(CommandError::StaleTarget) })),
        };
        let services = self.services.clone();
        let prepare_model = Arc::clone(&self.prepare_model);
        Ok(match request {
            HostRequest::Context(HostContextRequest::WorkingDirectory) => Box::pin(async move {
                let config = pinned_config(&actor)?;
                Ok(HostResponse::Context(
                    HostContextResponse::WorkingDirectory(config.context.cwd().to_path_buf()),
                ))
            }),
            HostRequest::Context(HostContextRequest::FastModeSupported) => Box::pin(async move {
                let config = pinned_config(&actor)?;
                Ok(HostResponse::Context(
                    HostContextResponse::FastModeSupported(
                        config.model.supports_fast() || config.model.fast_pending(),
                    ),
                ))
            }),
            HostRequest::Context(HostContextRequest::ModelSpecs) => Box::pin(async move {
                let specs = target
                    .template
                    .providers
                    .offline_specs(&target.template.model_policy);
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
        Some(match PinnedCommandTarget::pin(self.root.clone()) {
            Ok(target) => Arc::new(target),
            Err(_) => Arc::new(()),
        })
    }

    fn request(&self, request: HostRequest) -> CommandFuture<Result<HostResponse, CommandError>> {
        self.request_with_admission(request, self.admission_context())
    }

    fn request_with_admission(
        &self,
        request: HostRequest,
        admission: Option<Arc<dyn Any + Send + Sync>>,
    ) -> CommandFuture<Result<HostResponse, CommandError>> {
        let Some(target) = admission
            .as_ref()
            .and_then(|admission| admission.downcast_ref::<PinnedCommandTarget>())
            .cloned()
        else {
            return Box::pin(async { Err(CommandError::StaleTarget) });
        };
        if target.validate().is_err() {
            return Box::pin(async { Err(CommandError::StaleTarget) });
        }
        let response = match self.agent_request(target.clone(), request) {
            Ok(response) => response,
            Err(request) => {
                let child = target.target.id() != self.root.target.id()
                    || !target.target.same_manager(&self.root.target);
                if child
                    && matches!(
                        request,
                        HostRequest::Builtin(BuiltinOperation::ResetSession)
                    )
                {
                    return Box::pin(async { Err(producer_error(CHILD_SESSION_RESET)) });
                }
                let root = if child {
                    PinnedCommandTarget::pin(self.root.clone())
                } else {
                    Ok(target.clone())
                };
                let Ok(root) = root else {
                    return Box::pin(async { Err(CommandError::StaleTarget) });
                };
                let Ok(actor) = root.actor() else {
                    return Box::pin(async { Err(CommandError::StaleTarget) });
                };
                let publication = CheckpointPublication::default();
                let registrations = target.actor().and_then(|target_actor| {
                    let mut registrations = target_actor
                        .register_publication(&publication)
                        .map_err(|error| error.to_string())?;
                    registrations.extend(
                        actor
                            .register_publication(&publication)
                            .map_err(|error| error.to_string())?,
                    );
                    Ok(registrations)
                });
                let Ok(registrations) = registrations else {
                    return Box::pin(async { Err(CommandError::StaleTarget) });
                };
                let admission = target.clone();
                let response = self.session.request_for_actor(
                    request,
                    actor,
                    move || {
                        admission.validate().map_err(ConfigChangeError::from)?;
                        root.validate().map_err(ConfigChangeError::from)
                    },
                    publication,
                    publication_claim(),
                );
                Box::pin(async move {
                    let _registrations = registrations;
                    response.await
                })
            }
        };
        Box::pin(async move {
            target.validate().map_err(|_| CommandError::StaleTarget)?;
            let response = response.await;
            target.validate().map_err(|_| CommandError::StaleTarget)?;
            response
        })
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

fn pinned_config(actor: &AgentActorHandle) -> Result<Arc<EffectiveAgentConfig>, CommandError> {
    actor.effective_config().ok_or(CommandError::StaleTarget)
}

fn reserve_pinned_change<'a>(
    services: &'a AgentOrchestration,
    target: &'a PinnedCommandTarget,
) -> Result<ConfigChangeTicket<'a>, ConfigChangeError> {
    let actor = target
        .actor()
        .map_err(|_| ConfigChangeError::from(CLOSED_TARGET_ERR))?;
    ConfigChangeTicket::reserve_for_actor(actor, CheckpointPublication::default(), |publication| {
        target
            .validate()
            .map_err(|_| ConfigChangeError::from(CLOSED_TARGET_ERR))?;
        let mutation = services.reserve_config_mutation(&target.target, publication)?;
        target
            .validate()
            .map_err(|_| ConfigChangeError::from(CLOSED_TARGET_ERR))?;
        let target = target.clone();
        Ok(Box::new(move |config, claim| {
            target
                .validate()
                .map_err(|_| ConfigChangeError::from(CLOSED_TARGET_ERR))?;
            let transaction = mutation(config, claim)?;
            target
                .validate()
                .map_err(|_| ConfigChangeError::from(CLOSED_TARGET_ERR))?;
            Ok(transaction)
        }))
    })
}

async fn commit_change(
    services: Weak<AgentOrchestration>,
    target: PinnedCommandTarget,
    change: ConfigChange,
) -> Result<HostResponse, CommandError> {
    let services = live(&services)?;
    reserve_pinned_change(&services, &target)
        .map_err(producer_error)?
        .commit_change(change)
        .await
        .map_err(producer_error)?;
    Ok(HostResponse::Completed)
}

async fn change_directory(
    services: Weak<AgentOrchestration>,
    target: PinnedCommandTarget,
    path: PathBuf,
) -> Result<HostResponse, CommandError> {
    let services = live(&services)?;
    let commit = reserve_pinned_change(&services, &target)
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
    target: PinnedCommandTarget,
    selection: ModelSelection,
    prepare_model: ModelPreparer,
) -> Result<HostResponse, CommandError> {
    let services = live(&services)?;
    let policy = Arc::clone(&target.template.model_policy);
    reserve_pinned_change(&services, &target)
        .map_err(producer_error)?
        .commit(async |_, _| {
            let patch = prepare_selection(&selection, &policy, &prepare_model).await?;
            Ok::<_, ConfigChangeError>((publication_claim(), ConfigChange::Patch(patch)))
        })
        .await
        .map_err(producer_error)?;
    Ok(HostResponse::Completed)
}
