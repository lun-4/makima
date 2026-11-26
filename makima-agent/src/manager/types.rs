use makima_domain::LockExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_lite::future::or;
use makima_storage::session_state::{StoredAgentConfig, StoredAgentRecord, StoredAgentSpec};

use crate::actor::{
    EffectiveAgentConfig, TranscriptRequest, TranscriptSnapshot, TurnAdmissionOptions,
};
use crate::state::StateAccess;
use crate::{
    ActorBackend, ActorError, ActorSnapshot, AgentActorHandle, AgentId, AgentInput, CallScope,
    EventSender, HistorySnapshot, ReasonedCancelTrigger, SharedMessages, TurnId, TurnOutcome,
    TurnTicket,
};

use super::{ReactivationError, SubmitError, WakeError};

#[derive(Clone)]
pub enum RetainedTurn {
    Ticket(TurnTicket),
    Restoring(TurnId),
}

impl RetainedTurn {
    pub fn turn_id(&self) -> TurnId {
        match self {
            Self::Ticket(ticket) => ticket.turn_id(),
            Self::Restoring(turn) => *turn,
        }
    }
}

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

pub struct SpawnRestoration {
    pub spec: StoredAgentSpec,
    pub restorable: Result<(), String>,
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
    pub(super) start: flume::Sender<()>,
}

impl ResidentInitialization {
    /// Owns durable initialization and activation completion even if its waiter is dropped.
    /// Complete fallible host registration first. `work` must not invalidate the actor;
    /// its publication must remain bound to the caller's cancellation authority.
    /// Shutdown and detach wait for completion; close returns `BusySubtree`.
    pub async fn finish_with(
        self,
        work: impl Future<Output = Result<(), ReactivationError>> + Send + 'static,
    ) -> Result<(), ReactivationError> {
        self.agent.manager().finish_initialization(self, work).await
    }
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SubmitWhen {
    /// Joins compatible work or admits a new turn.
    #[default]
    Join,
    /// Admits a distinct turn behind existing work.
    Queue,
    /// Admits a distinct turn only when the actor is idle.
    Idle,
}

#[derive(Clone, Default)]
pub struct SubmitOptions {
    pub when: SubmitWhen,
    pub admission: TurnAdmissionOptions,
    pub presentation: Option<EventSender>,
    pub correlation: String,
}

impl SubmitOptions {
    pub(crate) fn submit_to(
        mut self,
        actor: &AgentActorHandle,
        input: AgentInput,
    ) -> Result<(TurnTicket, u64), ActorError> {
        match self.when {
            SubmitWhen::Join => {
                actor.submit_presented(input, self.presentation, self.correlation, self.admission)
            }
            SubmitWhen::Queue | SubmitWhen::Idle => {
                if self.admission.existing_only {
                    return Err(ActorError::Busy);
                }
                self.admission.idle_only |= self.when == SubmitWhen::Idle;
                actor.admit_turn_presented(
                    input,
                    self.presentation,
                    self.correlation,
                    self.admission,
                )
            }
        }
    }
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

    pub async fn retire(&self, current: &CurrentManagedTurn) -> Result<(), super::ManagerError> {
        self.manager.retire(current, self.agent_id).await
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

    /// Ensures the agent has a resident actor, reactivating it if needed.
    pub async fn wake(&self) -> Result<(), WakeError> {
        self.manager.wake_agent(self.agent_id).await
    }

    pub fn input_held(&self) -> bool {
        self.actor().is_ok_and(|actor| actor.input_held())
    }

    /// Admits input on the resident actor without starting reactivation.
    pub fn submit(
        &self,
        input: AgentInput,
        options: SubmitOptions,
    ) -> Result<(TurnTicket, u64), SubmitError> {
        let actor = self.manager.resident_actor(self.agent_id)?;
        Ok(options.submit_to(&actor, input)?)
    }

    pub fn snapshot(&self) -> Result<AgentNodeSnapshot, super::ManagerError> {
        if let Some(snapshot) = self.terminal.lock_unpoisoned().clone() {
            return Ok(snapshot);
        }
        self.manager
            .node(self.agent_id)
            .or_else(|error| self.terminal.lock_unpoisoned().clone().ok_or(error))
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
    pub(crate) tool_call: Option<Arc<CallScope>>,
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

    pub async fn spawn_child(
        &self,
        metadata: AgentMetadata,
        initial_messages: Vec<makima_providers::Message>,
        shared_messages: Option<crate::SharedMessages>,
        backend: Box<dyn crate::ActorBackend>,
    ) -> Result<AgentRef, super::ManagerError> {
        self.token
            .manager
            .spawn_child(self, metadata, initial_messages, shared_messages, backend)
            .await
    }

    pub async fn spawn_child_with_config(
        &self,
        config: crate::actor::EffectiveAgentConfig,
        metadata: AgentMetadata,
        initial_messages: Vec<makima_providers::Message>,
        shared_messages: Option<crate::SharedMessages>,
        backend: Box<dyn crate::ActorBackend>,
    ) -> Result<AgentRef, super::ManagerError> {
        self.token
            .manager
            .spawn_child_with_config(
                self,
                config,
                metadata,
                initial_messages,
                shared_messages,
                |_| Ok::<_, String>(backend),
            )
            .await
    }

    pub fn tool_call(&self) -> Option<&CallScope> {
        self.tool_call.as_deref()
    }

    pub fn lease(&self) -> TurnPermitLease {
        self.lease.clone()
    }

    pub fn validate_active(&self) -> Result<(), super::ManagerError> {
        self.token.manager.validate_active(self)
    }

    pub fn validate_descendant(&self, child_id: AgentId) -> Result<(), super::ManagerError> {
        self.token
            .manager
            .validate_descendant(self, child_id)
            .map(drop)
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

    /// Wait for an owned descendant ticket, including one settling after close.
    pub fn observe_descendant(
        &self,
        current: &CurrentManagedTurn,
        child_id: AgentId,
        ticket: TurnTicket,
        timeout: Option<Duration>,
    ) -> Result<ManagedPromptWait, super::ManagerError> {
        current
            .token
            .manager
            .register_turn_wait(current, self, child_id, ticket, timeout)
    }
}

pub struct ManagedPromptWait {
    pub(crate) inner: Arc<super::PromptWaitInner>,
    pub(crate) ticket: TurnTicket,
    pub(crate) _cancel: ReasonedCancelTrigger,
}

impl ManagedPromptWait {
    pub async fn wait_result(self) -> Result<crate::TurnResult, PromptWaitError> {
        self.wait_owned().await?;
        Ok(self.ticket.wait_result().await)
    }

    pub async fn wait(self) -> Result<TurnOutcome, PromptWaitError> {
        self.wait_owned().await
    }

    async fn wait_owned(&self) -> Result<TurnOutcome, PromptWaitError> {
        let result = self.inner.wait_result().await;
        self.inner.lease.wait_until_owned().await;
        result
    }

    /// Waits with the parent lease until the call moves. A moved call keeps the
    /// original watcher and timeout, but no longer waits for the parent permit.
    pub async fn wait_for_call(self, call: &CallScope) -> Result<TurnOutcome, PromptWaitError> {
        if let Some(result) = or(
            async {
                call.moved().await;
                None
            },
            async { Some(self.wait_owned().await) },
        )
        .await
        {
            return result;
        }
        // Dropping the parent's trigger here would cancel the watcher on handoff.
        let _parent_cancel = self.inner.lease.retire(self.inner.watcher_id);
        or(
            async {
                call.cancel().cancelled().await;
                Err(PromptWaitError::Cancelled)
            },
            self.inner.wait_result(),
        )
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptWaitError {
    Cancelled,
    Timeout,
}
