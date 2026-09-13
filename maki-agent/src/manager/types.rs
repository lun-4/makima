use std::sync::Arc;
use std::time::Duration;

use crate::{
    ActorSnapshot, AgentActorHandle, AgentId, AgentInput, EventSender, TurnId, TurnTicket,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLimits {
    pub max_concurrent_agent_turns: usize,
    pub max_agent_depth: usize,
    pub max_children_per_agent: usize,
    pub max_live_agents: usize,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_concurrent_agent_turns: 8,
            max_agent_depth: 4,
            max_children_per_agent: 16,
            max_live_agents: 64,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLifecycle {
    Reserved,
    Live,
    Closing,
    Closed,
    Removed,
}

impl GraphLifecycle {
    pub(crate) fn consumes_capacity(self) -> bool {
        matches!(self, Self::Reserved | Self::Live | Self::Closing)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMetadata {
    pub label: Option<String>,
    pub spawned_by_tool_use_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AgentNodeSnapshot {
    pub agent_id: AgentId,
    pub parent_id: Option<AgentId>,
    pub root_id: AgentId,
    pub depth: usize,
    pub children: Vec<AgentId>,
    pub graph_lifecycle: GraphLifecycle,
    pub actor: Option<ActorSnapshot>,
    pub metadata: AgentMetadata,
}

#[derive(Clone)]
pub struct AgentRef {
    pub(crate) manager: super::AgentManagerHandle,
    pub(crate) agent_id: AgentId,
}

impl std::fmt::Debug for AgentRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("AgentRef")
            .field(&self.agent_id)
            .finish()
    }
}

impl AgentRef {
    pub fn id(&self) -> AgentId {
        self.agent_id
    }

    pub fn actor(&self) -> Result<AgentActorHandle, super::ManagerError> {
        self.manager.actor(self.agent_id)
    }

    pub fn snapshot(&self) -> Result<AgentNodeSnapshot, super::ManagerError> {
        self.manager.node(self.agent_id)
    }

    pub fn cancel(&self) -> Result<(), super::ManagerError> {
        self.manager.cancel_agent(self.agent_id)
    }

    pub fn close_subtree(&self) -> Result<(), super::ManagerError> {
        self.manager.close_subtree(self.agent_id)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShutdownReport {
    pub joined: Vec<AgentId>,
    pub timed_out: Vec<AgentId>,
}

#[derive(Clone)]
pub struct CurrentManagedTurn {
    pub(crate) token: ManagedTurnToken,
    pub(crate) lease: TurnPermitLease,
}

impl CurrentManagedTurn {
    pub fn agent_id(&self) -> AgentId {
        self.token.agent_id
    }

    pub fn turn_id(&self) -> TurnId {
        self.token.turn_id
    }

    pub fn node_snapshot(&self) -> Result<AgentNodeSnapshot, super::ManagerError> {
        self.token.manager.node(self.token.agent_id)
    }

    pub fn spawn_child(
        &self,
        metadata: AgentMetadata,
        initial_messages: Vec<maki_providers::Message>,
        shared_messages: Option<crate::SharedMessages>,
        backend: Box<dyn crate::ActorBackend>,
    ) -> Result<AgentRef, super::ManagerError> {
        self.token
            .manager
            .spawn_child(self, metadata, initial_messages, shared_messages, backend)
    }

    pub fn lease(&self) -> TurnPermitLease {
        self.lease.clone()
    }

    pub fn validate_active(&self) -> Result<(), super::ManagerError> {
        self.token.manager.validate_active(self)
    }

    pub fn validate_descendant(&self, child_id: AgentId) -> Result<(), super::ManagerError> {
        self.token.manager.validate_descendant(self, child_id)
    }

    pub fn same_authority(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.token.manager.0, &other.token.manager.0)
            && self.token.generation == other.token.generation
            && self.token.agent_id == other.token.agent_id
            && self.token.turn_id == other.token.turn_id
            && self.token.nonce == other.token.nonce
    }
}

#[derive(Clone)]
pub(crate) struct ManagedTurnToken {
    pub(crate) manager: super::AgentManagerHandle,
    pub(crate) generation: u64,
    pub(crate) agent_id: AgentId,
    pub(crate) turn_id: TurnId,
    pub(crate) nonce: u64,
}

pub struct PromptAdmission {
    pub input: AgentInput,
    pub event_sender: Option<EventSender>,
    pub correlation: String,
}

#[derive(Clone)]
pub struct TurnPermitLease {
    pub(crate) inner: Arc<super::LeaseInner>,
}

impl TurnPermitLease {
    pub fn agent_id(&self) -> AgentId {
        self.inner.agent_id
    }

    pub fn turn_id(&self) -> TurnId {
        self.inner.turn_id
    }

    pub fn wait_for_descendant(
        &self,
        current: &CurrentManagedTurn,
        child_id: AgentId,
        actor: &AgentActorHandle,
        ticket: TurnTicket,
        timeout: Option<Duration>,
    ) -> Result<ManagedPromptWait, super::ManagerError> {
        current
            .token
            .manager
            .register_prompt_wait(current, self, child_id, actor, ticket, timeout)
    }

    pub fn admit_and_wait_for_descendant(
        &self,
        current: &CurrentManagedTurn,
        child_id: AgentId,
        actor: &AgentActorHandle,
        admission: PromptAdmission,
        timeout: Option<Duration>,
    ) -> Result<Option<(TurnId, ManagedPromptWait)>, super::ManagerError> {
        if !Arc::ptr_eq(&self.inner, &current.lease.inner) {
            return Err(super::ManagerError::WrongManager);
        }
        let managed_actor = current
            .token
            .manager
            .validated_descendant_actor(current, child_id)?;
        if !managed_actor.same_actor(actor) {
            return Err(super::ManagerError::ActorMismatch {
                expected_id: child_id,
                actual_id: actor.agent_id(),
            });
        }
        let Ok(ticket) = managed_actor.admit_turn(
            admission.input,
            admission.event_sender,
            admission.correlation,
        ) else {
            return Ok(None);
        };
        let turn_id = ticket.turn_id();
        #[cfg(test)]
        current.token.manager.wait_at_prompt_admission_gate(turn_id);
        match current.token.manager.register_prompt_wait(
            current,
            self,
            child_id,
            &managed_actor,
            ticket,
            timeout,
        ) {
            Ok(wait) => Ok(Some((turn_id, wait))),
            Err(error) => {
                let _ = managed_actor.cancel_turn(turn_id);
                Err(error)
            }
        }
    }
}

pub struct ManagedPromptWait {
    pub(crate) inner: Arc<super::PromptWaitInner>,
}

impl ManagedPromptWait {
    pub async fn wait(self) -> Result<crate::TurnOutcome, PromptWaitError> {
        let result = self.inner.wait_result().await;
        self.inner.lease.wait_until_owned().await;
        result
    }
}

impl Drop for ManagedPromptWait {
    fn drop(&mut self) {
        self.inner.cancel();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptWaitError {
    Cancelled,
    Timeout,
}
