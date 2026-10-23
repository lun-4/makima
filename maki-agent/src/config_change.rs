//! The one sequence every agent config change commits through.

use std::collections::VecDeque;
use std::path::Path;
use std::sync::{Arc, MutexGuard};

use maki_config::ModelPolicy;
use maki_providers::Model;
use maki_storage::StateDir;
use maki_storage::checkpoint::{CheckpointClaim, CheckpointPublication};
use thiserror::Error;

use crate::AgentMode;
use crate::actor::{
    ConfigChange, ConfigCommit, ConfigPatch, EffectiveAgentConfig, PreparedModel,
    PreparedOperationTicket,
};
use crate::modes::{self, ModeDef, ModeError, ModeRegistry};
use crate::session_coordinator::{
    ConfigMutation, ModelPreparer, SessionCoordinatorError, SessionCoordinatorHandle,
    publication_claim,
};
use crate::session_options::{MODEL_OPTION_ID, SessionOptionError};
use crate::{ActorError, AgentActorHandle, AgentRef, ManagerError};

#[derive(Debug, Clone, PartialEq, Error)]
pub enum ConfigChangeError {
    #[error(transparent)]
    Target(#[from] ManagerError),
    #[error(transparent)]
    Actor(#[from] ActorError),
    #[error(transparent)]
    Coordinator(#[from] SessionCoordinatorError),
    #[error(transparent)]
    Option(#[from] SessionOptionError),
    #[error(transparent)]
    Mode(#[from] ModeError),
    #[error("model preparation failed: {0}")]
    ModelPreparation(Arc<str>),
    #[error("directory preparation failed: {0}")]
    DirectoryPreparation(Arc<str>),
    #[error("{0}")]
    Rejected(Arc<str>),
}

impl From<String> for ConfigChangeError {
    fn from(error: String) -> Self {
        Self::Rejected(error.into())
    }
}

impl From<&str> for ConfigChangeError {
    fn from(error: &str) -> Self {
        Self::Rejected(error.into())
    }
}

impl ConfigChangeError {
    /// Refused because an idle guard or the session option lease was held.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            Self::Actor(ActorError::Busy)
                | Self::Coordinator(SessionCoordinatorError::SessionBusy(_))
        )
    }

    async fn wait_released(&self, actor: &AgentActorHandle) {
        match self {
            Self::Actor(ActorError::Busy) => actor.wait_unguarded().await,
            Self::Coordinator(SessionCoordinatorError::SessionBusy(id)) => {
                if let Ok(coordinator) = SessionCoordinatorHandle::resolve(*id) {
                    coordinator.read().wait_unreserved().await;
                }
            }
            _ => {}
        }
    }
}

/// A change that met a busy target, in the actor's dispatch-ordered wait
/// queue. Only the head retries, so a later change never overtakes an
/// earlier waiting one.
struct QueuedChange {
    actor: AgentActorHandle,
    place: Arc<()>,
}

impl QueuedChange {
    fn enqueue(actor: AgentActorHandle) -> Self {
        let place = Arc::new(());
        waiting(&actor).push_back(Arc::clone(&place));
        Self { actor, place }
    }

    async fn wait_turn(&self) {
        loop {
            let listener = self.actor.inner.policy_changed.listen();
            if waiting(&self.actor)
                .front()
                .is_some_and(|head| Arc::ptr_eq(head, &self.place))
            {
                return;
            }
            listener.await;
        }
    }
}

impl Drop for QueuedChange {
    fn drop(&mut self) {
        waiting(&self.actor).retain(|place| !Arc::ptr_eq(place, &self.place));
        self.actor.inner.policy_changed.notify(usize::MAX);
    }
}

/// Takes {target}'s place in its config FIFO for a change that holds only the
/// durable lease, not an actor operation: it waits behind changes already
/// queued and retries {reserve} after each release, so it neither fails Busy
/// nor overtakes a waiting change.
pub async fn reserve_in_turn<T>(
    target: &AgentRef,
    mut reserve: impl FnMut() -> Result<T, ConfigChangeError>,
) -> Result<T, ConfigChangeError> {
    let actor = target.actor()?;
    if waiting(&actor).is_empty() {
        match reserve() {
            Err(error) if error.is_busy() => {}
            result => return result,
        }
    }
    let place = QueuedChange::enqueue(actor);
    place.wait_turn().await;
    loop {
        match reserve() {
            Err(error) if error.is_busy() => error.wait_released(&place.actor).await,
            result => return result,
        }
    }
}

fn waiting(actor: &AgentActorHandle) -> MutexGuard<'_, VecDeque<Arc<()>>> {
    actor
        .inner
        .config_waiters
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// A slot in an agent's config FIFO. Reserving takes the durable mutation and
/// then the actor operation synchronously, so changes hold the session lease
/// in dispatch order. A change that meets a held lease or idle guard queues
/// and takes its slot once the holder lets go; it fails only if the target
/// closes or the change is rejected.
pub struct ConfigChangeTicket<'a> {
    publication: CheckpointPublication,
    reserve: Box<
        dyn FnMut(CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError> + Send + 'a,
    >,
    queued: Option<QueuedChange>,
    reserved: Option<(ConfigMutation, PreparedOperationTicket)>,
}

impl<'a> ConfigChangeTicket<'a> {
    pub fn reserve(
        target: &AgentRef,
        publication: CheckpointPublication,
        reserve: impl FnMut(CheckpointPublication) -> Result<ConfigMutation, ConfigChangeError>
        + Send
        + 'a,
    ) -> Result<Self, ConfigChangeError> {
        target.validate_live()?;
        let actor = target.actor()?;
        let mut ticket = Self {
            publication,
            reserve: Box::new(reserve),
            queued: None,
            reserved: None,
        };
        if waiting(&actor).is_empty() {
            match ticket.try_reserve(&actor) {
                Err(error) if error.is_busy() => {}
                result => return result.map(|()| ticket),
            }
        }
        ticket.queued = Some(QueuedChange::enqueue(actor));
        Ok(ticket)
    }

    /// A refused attempt rolls its publication back, so the next attempt
    /// takes a fresh one.
    fn try_reserve(&mut self, actor: &AgentActorHandle) -> Result<(), ConfigChangeError> {
        let attempt = (|| {
            let mutation = (self.reserve)(self.publication.clone())?;
            let operation = actor.reserve_prepared_operation(None)?;
            operation.register_publication(&self.publication)?;
            Ok((mutation, operation))
        })();
        match attempt {
            Ok(reserved) => {
                self.reserved = Some(reserved);
                Ok(())
            }
            Err(error) => {
                self.publication = CheckpointPublication::default();
                Err(error)
            }
        }
    }

    async fn reserved(&mut self) -> Result<&PreparedOperationTicket, ConfigChangeError> {
        if let Some(place) = &self.queued {
            place.wait_turn().await;
            let actor = place.actor.clone();
            while let Err(error) = self.try_reserve(&actor) {
                if !error.is_busy() {
                    return Err(error);
                }
                error.wait_released(&actor).await;
            }
            self.queued = None;
        }
        Ok(&self
            .reserved
            .as_ref()
            .expect("a ticket is reserved or queued")
            .1)
    }

    pub async fn wait_predecessor(
        &mut self,
    ) -> Result<Arc<EffectiveAgentConfig>, ConfigChangeError> {
        Ok(self.reserved().await?.wait_predecessor().await?)
    }

    /// Cancelling it before the claim rolls the change back. A change that
    /// queued publishes through a fresh one, so take it once reserved.
    pub fn publication(&self) -> CheckpointPublication {
        self.publication.clone()
    }

    /// Once the predecessor settles, `prepare` derives the change from its
    /// config and returns the claim the durable publication must pass.
    pub async fn commit(
        mut self,
        prepare: impl AsyncFnOnce(
            CheckpointPublication,
            Arc<EffectiveAgentConfig>,
        ) -> Result<(CheckpointClaim, ConfigChange), ConfigChangeError>,
    ) -> Result<ConfigCommit, ConfigChangeError> {
        let predecessor = self.wait_predecessor().await?;
        let (mutation, operation) = self.reserved.take().expect("waiting reserves the change");
        let (claim, change) = prepare(self.publication, predecessor).await?;
        operation.resolve(Some(change))?;
        operation.wait_ready().await?;
        let transaction = mutation(operation.ready_config()?, claim)?;
        Ok(operation.commit_durable(transaction).await?.config)
    }

    /// The one mode setter: once the predecessor settles, resolves {key} and
    /// commits it. Plan mode keeps {kept_plan} when given (a resumed
    /// session's file), else the predecessor's. {claim} sees the definition
    /// before it yields the claim.
    pub async fn commit_mode(
        self,
        target: &AgentRef,
        registry: &ModeRegistry,
        key: &str,
        state_dir: &StateDir,
        kept_plan: Option<&Path>,
        claim: impl AsyncFnOnce(
            CheckpointPublication,
            &ModeDef,
        ) -> Result<CheckpointClaim, ConfigChangeError>,
    ) -> Result<ConfigCommit, ConfigChangeError> {
        let actor = target.actor()?;
        self.commit(async |publication, predecessor| {
            let current = kept_plan.map_or_else(
                || predecessor.mode.clone(),
                |plan| AgentMode::Plan(plan.to_path_buf()),
            );
            let (mode, mode_def) = modes::resolve(registry, key, &current, state_dir)?;
            actor.validate_mode_change(&mode, &mode_def)?;
            let claim = claim(publication, &mode_def).await?;
            let change = ConfigChange::Mode {
                mode,
                mode_def: Some(mode_def),
            };
            Ok((claim, change))
        })
        .await
    }

    /// Commits a change fixed at dispatch under the default claim.
    pub async fn commit_change(
        self,
        change: ConfigChange,
    ) -> Result<ConfigCommit, ConfigChangeError> {
        self.commit(async |_, _| Ok((publication_claim(), change)))
            .await
    }
}

/// Resolves {spec} under {policy} and prepares its provider.
pub async fn prepare_model(
    spec: &str,
    policy: &ModelPolicy,
    prepare: &ModelPreparer,
) -> Result<PreparedModel, ConfigChangeError> {
    if !policy.allows(spec) {
        return Err(SessionOptionError::PolicyRejected(spec.into()).into());
    }
    let model = Model::from_spec(spec).map_err(|_| SessionOptionError::InvalidValue {
        id: MODEL_OPTION_ID.into(),
        value: spec.into(),
    })?;
    prepare(model)
        .await
        .map_err(ConfigChangeError::ModelPreparation)
}

/// Fills {patch}'s model from {spec}, when one is given.
pub async fn prepare_patch(
    mut patch: ConfigPatch,
    spec: Option<&str>,
    policy: &ModelPolicy,
    prepare: &ModelPreparer,
) -> Result<ConfigPatch, ConfigChangeError> {
    if let Some(spec) = spec {
        patch.model = Some(prepare_model(spec, policy, prepare).await?);
    }
    Ok(patch)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use maki_config::ModelPolicy;

    use super::{ConfigChangeError, ModelPreparer, SessionOptionError, prepare_model};

    const ALLOWED_SPEC: &str = "anthropic/claude-sonnet-4-20250514";
    const DISALLOWED_SPEC: &str = "openai/gpt-5";

    #[test]
    fn prepare_model_rejects_disallowed_spec_before_preparing() {
        let policy = ModelPolicy::new(&[ALLOWED_SPEC.into()], &[]).unwrap();
        let prepare: ModelPreparer = Arc::new(|_| unreachable!());
        let result = smol::block_on(prepare_model(DISALLOWED_SPEC, &policy, &prepare));
        assert!(matches!(
            result,
            Err(ConfigChangeError::Option(SessionOptionError::PolicyRejected(spec)))
                if *spec == *DISALLOWED_SPEC
        ));
    }
}
