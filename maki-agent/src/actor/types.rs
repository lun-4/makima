//! Backend trait and the dependency-neutral work shapes the scheduler moves.

use std::future::Future;

use maki_providers::{ImageSource, TokenUsage};
use std::sync::Arc;

use crate::cancel::{CancelToken, ReasonedCancelToken};
use crate::manager::{CurrentManagedTurn, ManagerInner};
use crate::types::{AgentId, TurnId, TurnOutcome};
use crate::{InterruptSource, RunSettings};

#[derive(Clone)]
pub(crate) struct ManagedTurnAdmission {
    pub(crate) manager: std::sync::Weak<ManagerInner>,
    pub(crate) agent_id: AgentId,
    pub(crate) ceiling: Option<RunSettings>,
    pub(crate) mode_ceiling: Option<crate::AgentMode>,
}

/// The actor's immutable per-admission settings shape.
pub type EffectiveAgentConfig = RunSettings;

impl ManagedTurnAdmission {
    pub(crate) fn new(
        manager: std::sync::Weak<ManagerInner>,
        agent_id: AgentId,
        ceiling: Option<RunSettings>,
        mode_ceiling: Option<crate::AgentMode>,
    ) -> Self {
        Self {
            manager,
            agent_id,
            ceiling,
            mode_ceiling,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EarlierRoot {
    pub run_id: u64,
    pub displayed: bool,
    pub text: String,
    pub images: Vec<ImageSource>,
    pub correlation: String,
}

/// What category of work the backend is asked to execute. Controls and
/// compacts never produce a [`TurnOutcome`]; turns and started roots settle
/// into one. A started root carries the neutral display metadata the host
/// queued (`run_id`, displayed, text, images) so the backend can emit
/// `QueueItemConsumed` only for roots that were not yet drawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkKind {
    Turn,
    Root {
        run_id: u64,
        displayed: bool,
        text: String,
        images: Vec<ImageSource>,
        earlier: Vec<EarlierRoot>,
    },
    Control,
    Compact,
}

/// Stable information about the actor and the current turn, passed to the
/// backend on every execution so it can build correlated events and cancel
/// cooperatively. The reasoned token records the first cancellation reason;
/// `Agent::run` reads it once when constructing its terminal outcome. The
/// interrupt source lets the running agent fold queued roots into its own
/// turn and process compact commands between model turns.
#[derive(Clone)]
pub struct TurnContext {
    pub agent_id: AgentId,
    pub turn_id: Option<TurnId>,
    pub cancel: CancelToken,
    pub cancel_reason: ReasonedCancelToken,
    /// Adapter-local correlation (the sink's run id or the control's key).
    pub correlation: String,
    pub generation: u64,
    pub policy: Option<Arc<EffectiveAgentConfig>>,
    /// Extracts root/compact work out of the actor's queue while the run is
    /// active, so it can fold them instead of waiting for the turn to end.
    pub interrupt: Option<std::sync::Arc<dyn InterruptSource>>,
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
    /// `TurnOutcome::Failed` for the admission and delivers it once.
    SetupFailed {
        agent_id: AgentId,
        turn_id: TurnId,
    },
    ControlDone,
    ControlFailed,
    CompactDone,
}

/// A root input queued by the host. It carries neutral display metadata
/// (`run_id`, displayed, text, image count) so the TUI can project the queue
/// without importing UI types into the core.
pub struct RootWork {
    pub input: crate::AgentInput,
    pub run_id: u64,
    pub displayed: bool,
    pub text: String,
    pub images: Vec<ImageSource>,
    pub correlation: String,
    pub earlier: Vec<EarlierRoot>,
    pub generation: u64,
    pub(crate) policy: Option<Arc<EffectiveAgentConfig>>,
    pub(crate) admission: Option<crate::agent::TurnAdmissionSnapshot>,
}

impl RootWork {
    pub fn new(
        input: crate::AgentInput,
        run_id: u64,
        displayed: bool,
        text: String,
        images: Vec<ImageSource>,
        correlation: String,
    ) -> Self {
        Self {
            input,
            run_id,
            displayed,
            text,
            images,
            correlation,
            earlier: Vec::new(),
            generation: 0,
            policy: None,
            admission: None,
        }
    }
}

/// A standalone control operation. Compact correlation only: the host's
/// control key, enough to route the result back.
#[derive(Debug, Clone)]
pub struct ControlWork {
    pub name: String,
    pub correlation: String,
}

/// A turn admitted by the host. Holds the agent input plus the admission's
/// event-sender metadata so the single terminal delivery can reach the sink
/// that admitted it. The input is taken by the runner when the turn starts.
pub struct TurnAdmission {
    pub turn_id: TurnId,
    pub input: Option<crate::AgentInput>,
    pub event_sender: Option<crate::EventSender>,
    pub correlation: String,
    /// True when this admission was synthesized from a queued root input.
    /// A root-started turn that is cancelled before entering produces no
    /// retained outcome and no terminal delivery.
    pub(crate) root: bool,
    pub(crate) generation: u64,
    pub(crate) policy: Option<Arc<EffectiveAgentConfig>>,
    pub(crate) ticket: super::TurnTicket,
    pub(crate) admission: Option<crate::agent::TurnAdmissionSnapshot>,
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
/// turn id, queued count and (for the TUI) the queue's neutral messages, the
/// latest retained outcome, and cumulative usage.
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

/// The adapter owns its mutable configuration and executes work against the
/// actor's shared history. Object-safe: every execution method returns a
/// boxed future, so the TUI and Lua can share one `Box<dyn ActorBackend>`.
/// Captures the external state pinned to an admission. The actor always calls
/// this before taking its state lock, so implementations may perform blocking
/// work but must tolerate the actor closing before the prepared snapshot commits.
pub type AdmissionPreparation =
    Arc<dyn Fn(&crate::AgentInput) -> crate::agent::TurnAdmissionSnapshot + Send + Sync>;

pub trait ActorBackend: Send {
    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        None
    }

    /// Executes one accepted turn or a started root. `turn_id` is `Some`;
    /// `work` distinguishes `Turn` from `Root`. Returns `EnteredRun` with
    /// the authoritative outcome, or `SetupFailed` for a turn that never
    /// entered.
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
