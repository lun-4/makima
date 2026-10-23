//! Backend trait and the dependency-neutral work shapes the scheduler moves.

use std::any::Any;
use std::future::Future;
use std::pin::Pin;

use maki_providers::TokenUsage;
use std::sync::Arc;

use crate::cancel::{CancelToken, ReasonedCancelToken};
use crate::manager::{CurrentManagedTurn, ManagerInner};
use crate::tools::offload::OffloadStore;
use crate::types::{ActorWorkIdentity, AgentId, TurnFailure, TurnId, TurnOutcome};
use crate::{AgentMode, InterruptSource, RunSettings};

#[derive(Clone)]
pub(crate) struct ManagedTurnAdmission {
    pub(crate) manager: std::sync::Weak<ManagerInner>,
    pub(crate) agent_id: AgentId,
    pub(crate) is_child: bool,
    pub(crate) parent_delivery: Option<super::delivery::ParentDelivery>,
}

/// The actor's immutable per-admission configuration snapshot.
#[derive(Clone)]
pub struct EffectiveAgentConfig {
    pub settings: RunSettings,
    pub mode: AgentMode,
    pub mode_def: Option<crate::ModeDef>,
    pub context: super::AgentContextSnapshot,
    pub(super) mode_authority: Option<super::ModeCeiling>,
}

impl EffectiveAgentConfig {
    pub fn new(settings: RunSettings, mode: AgentMode) -> Self {
        Self {
            settings,
            mode,
            mode_def: None,
            mode_authority: None,
            context: super::AgentContextSnapshot::new(".".into()),
        }
    }

    /// Sets creation authority independently of the initial runtime mode.
    pub fn with_mode_authority(mut self, authority: super::ModeCeiling) -> Self {
        self.mode_authority = Some(authority);
        self
    }

    pub fn mode_authority(&self) -> Option<super::ModeCeiling> {
        self.mode_authority.clone()
    }

    pub fn with_context(mut self, context: super::AgentContextSnapshot) -> Self {
        self.context = context;
        self
    }

    pub fn with_mode_def(mut self, mode_def: Option<crate::ModeDef>) -> Self {
        self.mode_def = mode_def;
        self
    }
}

impl std::ops::Deref for EffectiveAgentConfig {
    type Target = RunSettings;

    fn deref(&self) -> &Self::Target {
        &self.settings
    }
}

impl ManagedTurnAdmission {
    pub(crate) fn new(manager: std::sync::Weak<ManagerInner>, agent_id: AgentId) -> Self {
        Self {
            manager,
            agent_id,
            is_child: false,
            parent_delivery: None,
        }
    }
}

/// What category of work the backend executes. Only turns produce a [`TurnOutcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkKind {
    Turn,
    Control,
    Compact,
}

/// Stable information about the actor and the current turn, passed to the
/// backend on every execution so it can build correlated events and cancel
/// cooperatively. The reasoned token records the first cancellation reason;
/// `Agent::run` reads it once when constructing its terminal outcome. The
/// interrupt source processes compact commands between model requests.
///
/// `work` is the identity of the admitted work this execution is running,
/// stamped at admission and never re-derived, so every event it emits carries
/// the presentation it was admitted under.
#[derive(Clone)]
pub struct TurnContext {
    pub agent_id: AgentId,
    pub turn_id: Option<TurnId>,
    pub work: ActorWorkIdentity,
    pub output: super::TurnOutput,
    pub provenance: super::TurnProvenance,
    pub cancel: CancelToken,
    pub cancel_reason: ReasonedCancelToken,
    pub generation: u64,
    pub policy: Option<Arc<EffectiveAgentConfig>>,
    /// Extracts compatible compaction work while the turn is active.
    pub interrupt: Option<std::sync::Arc<dyn InterruptSource>>,
    pub inbox: Option<Arc<dyn super::TurnInbox>>,
    pub managed_turn: Option<CurrentManagedTurn>,
    pub admission: Option<crate::agent::TurnAdmissionSnapshot>,
}

/// The terminal result of one backend execution. `EnteredRun` is the only
/// variant that represents an admitted turn's real outcome; it is delivered
/// exactly once to the admission's sink and never again. `SetupFailed`
/// reports a turn that could not even start and is synthesized into exactly
/// one `TurnOutcome::Failed` by the actor. Control and compact variants
/// belong to no turn.
#[derive(Debug)]
pub enum BackendResult {
    /// The backend entered the run and produced a real outcome.
    EnteredRun(TurnOutcome),
    /// Setup failed before the run entered; the actor synthesizes one
    /// `TurnOutcome::Failed` for the admission, carrying the backend's
    /// failure when it has a specific one, and delivers it once.
    SetupFailed(Option<TurnFailure>),
    ControlDone,
    ControlFailed,
    CompactDone,
    /// A control that compacted the history ended this way.
    ControlCompacted(super::CompactionStatus),
}

/// A standalone control operation. Carries the actor admission that identified
/// it so its events settle under the same identity.
#[derive(Debug, Clone)]
pub struct ControlWork {
    pub name: String,
    pub correlation: String,
    pub work: ActorWorkIdentity,
    /// Host data for the backend; dropped with the work if it never runs.
    pub payload: Option<Arc<dyn Any + Send + Sync>>,
}

/// A turn admitted by the host. Holds the agent input plus the admission's
/// event-sender metadata so the single terminal delivery can reach the sink
/// that admitted it. The input is taken by the runner when the turn starts.
pub struct TurnAdmission {
    pub turn_id: TurnId,
    pub input: Option<crate::AgentInput>,
    pub event_sender: Option<crate::EventSender>,
    pub correlation: String,
    pub work: ActorWorkIdentity,
    pub(crate) generation: u64,
    pub(crate) policy: Option<Arc<EffectiveAgentConfig>>,
    pub(crate) ticket: super::TurnTicket,
    pub(crate) admission: Option<crate::agent::TurnAdmissionSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionPolicy {
    Independent,
    JoinReportingChildren,
}

/// The actor's lifecycle. `Closed` and `Shutdown` are terminal and reject
/// new admissions; `Open` stays reusable across completed, failed, and
/// cancelled turns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActorLifecycle {
    #[default]
    Open,
    Closed,
    Shutdown,
}

/// Run status of the actor, including the identity of the active turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActorStatus {
    Idle,
    Running(TurnId),
}

/// A point-in-time projection of every actor lens: lifecycle, status, active
/// turn id, queued user work count and (for the TUI) the queue's neutral
/// messages, the latest retained outcome, and cumulative usage.
#[derive(Debug, Clone)]
pub struct ActorSnapshot {
    pub lifecycle: ActorLifecycle,
    pub status: ActorStatus,
    pub active_turn: Option<TurnId>,
    pub queued: usize,
    pub queue: Vec<super::queue::QueueProjection>,
    pub latest: Option<TurnOutcome>,
    pub cumulative_usage: TokenUsage,
}

/// Captures external admission dependencies on the blocking pool, without the
/// actor state lock. The supplied configuration is the committed FIFO predecessor;
/// implementations must use its resolved mode definition when present. Cancellation
/// can retire the admission before this callback returns.
pub type AdmissionPreparation = Arc<
    dyn Fn(&crate::AgentInput, Option<&EffectiveAgentConfig>) -> crate::agent::TurnAdmissionSnapshot
        + Send
        + Sync,
>;

pub type PreparedReadiness = Arc<
    dyn Fn(
            crate::agent::TurnAdmissionSnapshot,
        ) -> Pin<
            Box<
                dyn Future<Output = Result<crate::agent::TurnAdmissionSnapshot, super::ActorError>>
                    + Send,
            >,
        > + Send
        + Sync,
>;

pub trait ActorBackend: Send {
    fn offload_store(&self) -> Option<Arc<OffloadStore>> {
        None
    }

    fn prepared_readiness(&self) -> Option<PreparedReadiness> {
        None
    }

    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        None
    }

    /// Executes one accepted turn. Returns `EnteredRun` with the authoritative
    /// outcome, or `SetupFailed` for a turn that never entered.
    fn run_turn<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        input: crate::AgentInput,
        work: WorkKind,
    ) -> std::pin::Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>>;

    /// Executes a standalone control operation. Never carries a [`TurnId`]
    /// and must not produce a [`TurnOutcome`].
    fn run_control<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        control: &'a ControlWork,
    ) -> std::pin::Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>>;

    /// Executes a compact operation. No [`TurnId`], no outcome.
    fn run_compact<'a>(
        &'a mut self,
        history: &'a mut crate::History,
        context: TurnContext,
        instructions: Option<&'a str>,
    ) -> std::pin::Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>>;
}
