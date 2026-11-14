use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use makima_storage::session_state::{StoredAgentConfig, StoredAgentRecord};

use crate::actor::{EffectiveAgentConfig, TranscriptRequest, TranscriptSnapshot};
use crate::state::StateAccess;
use crate::{
    ActorBackend, ActorSnapshot, AgentActorHandle, AgentId, HistorySnapshot, SharedMessages,
    TurnId, TurnTicket,
};

use super::ReactivationError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentLimits {
    pub max_concurrent_agent_turns: usize,
    pub max_agent_depth: usize,
    pub max_children_per_agent: usize,
    pub max_active_agents: usize,
    pub max_retained_agents: usize,
}

impl Default for AgentLimits {
    fn default() -> Self {
        Self {
            max_concurrent_agent_turns: 8,
            max_agent_depth: 4,
            max_children_per_agent: 16,
            max_active_agents: 64,
            max_retained_agents: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphLifecycle {
    Reserved,
    Dormant,
    Live,
    Closing,
    Closed,
    Retired,
    Removed,
}

impl GraphLifecycle {
    pub(crate) fn consumes_capacity(self) -> bool {
        matches!(self, Self::Reserved | Self::Live | Self::Closing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentResidency {
    Dormant,
    Activating,
    Resident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentIncarnation {
    pub agent_id: AgentId,
    pub manager_generation: u64,
    pub reservation: u64,
}

#[derive(Clone)]
pub struct ReactivationInput {
    pub record: StoredAgentRecord,
    pub config: Option<StoredAgentConfig>,
    pub history: HistorySnapshot,
}

pub type ResidentInitializer = Box<
    dyn FnOnce(
            ResidentInitialization,
        ) -> Pin<Box<dyn Future<Output = Result<(), ReactivationError>> + Send>>
        + Send,
>;

pub struct ResidentAgent {
    pub backend: Box<dyn ActorBackend>,
    pub config: EffectiveAgentConfig,
    pub shared_messages: Option<SharedMessages>,
    pub start: Option<flume::Receiver<()>>,
    pub initialize: Option<ResidentInitializer>,
}

pub struct ResidentInitialization {
    pub agent: AgentRef,
    pub actor: AgentActorHandle,
    pub incarnation: AgentIncarnation,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentMetadata {
    pub label: Option<String>,
    pub spawned_by_tool_use_id: Option<String>,
    pub transcript_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AgentNodeSnapshot {
    pub agent_id: AgentId,
    pub parent_id: Option<AgentId>,
    pub root_id: AgentId,
    pub depth: usize,
    pub children: Vec<AgentId>,
    pub graph_lifecycle: GraphLifecycle,
    pub residency: AgentResidency,
    pub actor: Option<ActorSnapshot>,
    pub metadata: AgentMetadata,
}

#[derive(Clone)]
pub struct AgentRef {
    pub(crate) manager: super::AgentManagerHandle,
    pub(crate) agent_id: AgentId,
    pub(crate) terminal: Arc<Mutex<Option<AgentNodeSnapshot>>>,
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
    pub fn state_access(&self) -> StateAccess {
        self.manager.journal().state_access(self.agent_id, None)
    }
    pub fn manager(&self) -> super::AgentManagerHandle {
        self.manager.clone()
    }

    pub fn same_manager(&self, other: &Self) -> bool {
        self.manager.same_manager(&other.manager)
    }

    pub fn transcript(
        &self,
        request: TranscriptRequest,
    ) -> Result<TranscriptSnapshot, crate::ActorError> {
        self.manager.transcript(self.agent_id, request)
    }

    pub fn turn_ticket(&self, turn_id: TurnId) -> Result<TurnTicket, super::ManagerError> {
        self.manager.retained_turn_ticket(self, turn_id)
    }

    pub fn is_live(&self) -> bool {
        self.manager.is_live(self.agent_id)
    }

    pub fn incarnation(&self) -> Result<AgentIncarnation, super::ManagerError> {
        self.manager.incarnation(self.agent_id)
    }

    pub fn retire(&self, current: &CurrentManagedTurn) -> Result<(), super::ManagerError> {
        self.manager.retire(current, self.agent_id)
    }

    pub fn validate_live(&self) -> Result<(), super::ManagerError> {
        self.manager.validate_live_ref(self)
    }

    pub fn id(&self) -> AgentId {
        self.agent_id
    }

    pub fn actor(&self) -> Result<AgentActorHandle, super::ManagerError> {
        self.manager.actor(self.agent_id)
    }

    pub fn snapshot(&self) -> Result<AgentNodeSnapshot, super::ManagerError> {
        if let Some(snapshot) = self
            .terminal
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
        {
            return Ok(snapshot);
        }
        self.manager.node(self.agent_id).or_else(|error| {
            self.terminal
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
                .ok_or(error)
        })
    }

    pub fn effective_config(
        &self,
    ) -> Result<Option<Arc<crate::actor::EffectiveAgentConfig>>, super::ManagerError> {
        self.manager.effective_config(self.agent_id)
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
    pub(crate) state_access: StateAccess,
    pub(crate) token: ManagedTurnToken,
    pub(crate) lease: TurnPermitLease,
    pub(crate) policy: Option<Arc<crate::RunSettings>>,
    pub(crate) mode: Option<crate::AgentMode>,
    pub(crate) mode_def: Option<crate::ModeDef>,
    pub(crate) context: Option<crate::actor::AgentContextSnapshot>,
}

impl CurrentManagedTurn {
    pub fn capture_new_invocation_state_access(&self) -> StateAccess {
        self.token.manager.journal().state_access(
            self.token.agent_id,
            Some((self.token.turn_id, self.token.nonce)),
        )
    }

    pub fn state_access(&self) -> StateAccess {
        self.state_access.clone()
    }
    pub fn manager(&self) -> super::AgentManagerHandle {
        self.token.manager.clone()
    }

    pub fn agent_ref(&self) -> Result<AgentRef, super::ManagerError> {
        self.token.manager.lookup(self.agent_id())
    }

    pub fn agent_id(&self) -> AgentId {
        self.token.agent_id
    }

    pub fn turn_id(&self) -> TurnId {
        self.token.turn_id
    }

    pub fn policy_snapshot(&self) -> Option<&Arc<crate::RunSettings>> {
        self.policy.as_ref()
    }

    pub fn mode_definition(&self) -> Option<&crate::ModeDef> {
        self.mode_def.as_ref()
    }

    pub fn node_snapshot(&self) -> Result<AgentNodeSnapshot, super::ManagerError> {
        self.token.manager.node(self.token.agent_id)
    }

    pub fn spawn_child(
        &self,
        metadata: AgentMetadata,
        initial_messages: Vec<makima_providers::Message>,
        shared_messages: Option<crate::SharedMessages>,
        backend: Box<dyn crate::ActorBackend>,
    ) -> Result<AgentRef, super::ManagerError> {
        self.token
            .manager
            .spawn_child(self, metadata, initial_messages, shared_messages, backend)
    }

    pub fn spawn_child_with_config(
        &self,
        config: crate::actor::EffectiveAgentConfig,
        metadata: AgentMetadata,
        initial_messages: Vec<makima_providers::Message>,
        shared_messages: Option<crate::SharedMessages>,
        backend: Box<dyn crate::ActorBackend>,
    ) -> Result<AgentRef, super::ManagerError> {
        self.token.manager.spawn_child_with_config(
            self,
            config,
            metadata,
            initial_messages,
            shared_messages,
            |_| Ok::<_, String>(backend),
        )
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
            && self.token.agent_id == other.token.agent_id
            && self.token.turn_id == other.token.turn_id
            && self.token.nonce == other.token.nonce
    }
}

#[derive(Clone)]
pub(crate) struct ManagedTurnToken {
    pub(crate) manager: super::AgentManagerHandle,
    pub(crate) agent_id: AgentId,
    pub(crate) turn_id: TurnId,
    pub(crate) nonce: u64,
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

    pub fn observe_descendant(
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
            .register_turn_wait(current, self, child_id, actor, ticket, timeout)
    }
}

pub struct ManagedPromptWait {
    pub(crate) inner: Arc<super::PromptWaitInner>,
    pub(crate) ticket: TurnTicket,
}

impl ManagedPromptWait {
    pub async fn wait_result(self) -> Result<crate::TurnResult, PromptWaitError> {
        let result = self.inner.wait_result().await;
        self.inner.lease.wait_until_owned().await;
        result?;
        Ok(self.ticket.wait_result().await)
    }

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
