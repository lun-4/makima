use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::mem::take;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_lock::{RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use event_listener::Event;
use makima_providers::{Message, TokenUsage};
use makima_storage::id::{AgentId, MakimaId, TurnId};
use makima_storage::session_state::{
    SessionStateView, StateChange, StateFoldError, StoredAgentConfig, StoredAgentRecord,
    StoredDelivery, StoredDeliveryOp, StoredPayload, StoredStateRecord, StoredTurnResult,
};
use makima_storage::sessions::{Session, StoredRule};
use thiserror::Error;

use crate::actor::{
    ChildDelivery, DeliveryRejection, MAX_DELIVERY_PREVIEW_BYTES, MAX_PENDING_DELIVERIES,
    MAX_PENDING_PREVIEW_BYTES, StoredTurnProgress, TurnResult, TurnResultStorageError,
};
use crate::context::{CONTEXT_MANIFEST, ContextUpdate, manifest_hash};
use crate::rewind::{
    RewindBoundary, RewindError, finalize_candidate, prepare_boundary, rebase_history_epoch,
    subtree,
};
use crate::state::{
    StateChangeEvent, StateChangeReason, StateEdit, StateError, StateRevision, StateRuntime,
    StateScope, read_slot, validate_access, validate_record,
};
use crate::tools::offload::OffloadStore;
use crate::{HistorySnapshot, ToolOutput, TurnOutcome};

const CONVERSION_RETRY_DELAY: Duration = Duration::from_secs(1);
const REPLACEMENT_RESERVED: &str = "rewind publication is reserved";

fn context_address(source: &str) -> String {
    format!("\0context:{source}")
}

thread_local! {
    static HISTORY_CHANGES: RefCell<Vec<JournalChange>> = const { RefCell::new(Vec::new()) };
}

pub(crate) fn with_history_changes<T>(
    changes: Vec<JournalChange>,
    commit: impl FnOnce() -> T,
) -> T {
    struct Reset(Vec<JournalChange>);
    impl Drop for Reset {
        fn drop(&mut self) {
            HISTORY_CHANGES.with(|slot| {
                slot.replace(take(&mut self.0));
            });
        }
    }
    let _reset = Reset(HISTORY_CHANGES.with(|slot| slot.replace(changes)));
    commit()
}

pub(crate) fn take_history_changes() -> Vec<JournalChange> {
    HISTORY_CHANGES.with(|slot| slot.take())
}

fn restored_preview_bytes(delivery: &StoredDelivery) -> usize {
    if let Some(bytes) = delivery.preview_bytes {
        return bytes;
    }
    let StoredPayload::Inline(text) = &delivery.result.text else {
        return MAX_DELIVERY_PREVIEW_BYTES;
    };
    let offloaded_output = matches!(&delivery.result.output, StoredPayload::Offloaded(_));
    let output = match &delivery.result.output {
        StoredPayload::Inline(output) => output.clone(),
        StoredPayload::Offloaded(_) => Vec::new(),
    };
    let delivery = ChildDelivery {
        label: delivery.label.clone(),
        result: TurnResult {
            outcome: delivery.result.outcome.clone().into(),
            text: text.clone(),
            output,
            provenance: delivery.result.provenance.clone().into(),
            duration_ms: delivery.result.duration_ms,
        },
    };
    let preview = if offloaded_output {
        delivery.truncated_preview(None, "")
    } else {
        delivery.preview(None)
    };
    preview.input().preamble[0]
        .first_text_content()
        .map_or(0, str::len)
}

type StoredSession = Session<Message, TokenUsage, ToolOutput>;
type PendingDeliveryEnvelope = (AgentId, TurnId, Option<Arc<ChildDelivery>>);

#[derive(Debug, Error)]
pub enum JournalError {
    #[error(transparent)]
    InvalidState(#[from] StateFoldError),
    #[error("session journal sequence {0} conflicts with the checkpoint")]
    Diverged(u64),
    #[error("session journal sequence exhausted")]
    SequenceExhausted,
    #[error("journal checkpoint belongs to another incarnation or a future revision")]
    StaleCheckpoint,
    #[error("stale actor incarnation for agent {0}")]
    StaleIncarnation(AgentId),
    #[error("journal preparation failed: {0}")]
    Preparation(String),
    #[error("prepared journal commits cannot add agent identities")]
    PreparedAgents,
    #[error("pending journal conversion does not match its reserved records")]
    InvalidPending,
    #[error(transparent)]
    State(#[from] StateError),
}

#[derive(Debug, Clone)]
pub struct JournalChange {
    pub agent: Option<AgentId>,
    pub generation: u64,
    pub change: StateChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalIncarnation {
    pub agent: AgentId,
    pub reservation: u64,
}

#[derive(Debug, Clone, Default)]
pub struct JournalCommit {
    pub boundary: Option<(AgentId, u64)>,
    pub state_edits: Vec<StateEdit>,
    pub context_updates: Vec<ContextUpdate>,
    pub state_epoch_agents: BTreeSet<AgentId>,
    pub incarnations: Vec<JournalIncarnation>,
    pub agents: Vec<StoredAgentRecord>,
    pub changes: Vec<JournalChange>,
    pub histories: BTreeMap<AgentId, HistorySnapshot>,
    pub managed_histories: BTreeSet<AgentId>,
}

#[derive(Debug, Clone)]
pub struct SessionJournalSnapshot {
    pub epoch: MakimaId,
    pub revision: u64,
    pub seq: u64,
    pub state: Vec<StoredStateRecord>,
    pub agent_revisions: Vec<(u64, StoredAgentRecord)>,
    pub histories: BTreeMap<AgentId, HistorySnapshot>,
    pub managed_histories: BTreeSet<AgentId>,
    pub authoritative: bool,
}

impl SessionJournalSnapshot {
    fn empty() -> Self {
        Self {
            epoch: MakimaId::generate(),
            revision: 0,
            seq: 0,
            state: Vec::new(),
            agent_revisions: Vec::new(),
            histories: BTreeMap::new(),
            managed_histories: BTreeSet::new(),
            authoritative: false,
        }
    }

    pub fn apply_to(&self, session: &mut StoredSession) -> Result<(), JournalError> {
        if self.authoritative {
            SessionStateView::fold(&self.state, &self.agent_revisions, self.seq)?;
            session.replace_state(self.state.clone());
            session.replace_agent_revisions(self.agent_revisions.clone());
            session.replace_journal_histories(&self.histories);
            return Ok(());
        }
        let mut state: BTreeMap<_, _> = session
            .state()
            .iter()
            .cloned()
            .map(|record| (record.seq, record))
            .collect();
        let mut agents: BTreeMap<_, _> = session.agent_revisions().iter().cloned().collect();
        for record in &self.state {
            if agents.contains_key(&record.seq)
                || state.get(&record.seq).is_some_and(|saved| saved != record)
            {
                return Err(JournalError::Diverged(record.seq));
            }
            state.insert(record.seq, record.clone());
        }
        for (seq, record) in &self.agent_revisions {
            if state.contains_key(seq) || agents.get(seq).is_some_and(|saved| saved != record) {
                return Err(JournalError::Diverged(*seq));
            }
            agents.insert(*seq, record.clone());
        }
        let state: Vec<_> = state.into_values().collect();
        let agents: Vec<_> = agents.into_iter().collect();
        let view = SessionStateView::fold(&state, &agents, u64::MAX)?;
        session.replace_state(state);
        session.replace_agent_revisions(agents);
        for (agent, history) in &self.histories {
            let Some(record) = view.agents.get(agent) else {
                continue;
            };
            if self.managed_histories.contains(agent) {
                if record.transcript_key.is_empty() {
                    session.adopt_history(history);
                } else {
                    session.adopt_subagent_history(record.transcript_key.clone(), history);
                }
            } else if !record.transcript_key.is_empty()
                && !session
                    .subagent_messages()
                    .contains_key(&record.transcript_key)
            {
                session.adopt_subagent_history(record.transcript_key.clone(), history);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalPending {
    epoch: MakimaId,
    revision: u64,
    first_seq: u64,
    count: usize,
}

#[derive(Clone)]
struct StagedCommit {
    snapshot: SessionJournalSnapshot,
    pending: Option<JournalPending>,
}

struct RuntimeResult {
    result: TurnResult,
    progress: StoredTurnProgress,
    stored: Option<StoredTurnResult>,
    store: Option<Arc<OffloadStore>>,
}

#[derive(Clone)]
struct PendingResult {
    conversion: Arc<Mutex<RuntimeResult>>,
    terminal: Option<JournalPending>,
    deliveries: Vec<(JournalPending, AgentId, Option<String>, usize)>,
    released: bool,
    outcome: Option<TurnOutcome>,
    incarnation: Option<JournalIncarnation>,
}

#[derive(Clone)]
pub(crate) struct JournalState {
    pub(crate) snapshot: SessionJournalSnapshot,
    pub(crate) plugin_state: StateRuntime,
    ready: SessionJournalSnapshot,
    staged: VecDeque<StagedCommit>,
    published_revision: u64,
    session_id: Option<MakimaId>,
    results: BTreeMap<(AgentId, TurnId), PendingResult>,
    delivery_bytes: BTreeMap<(AgentId, AgentId, TurnId), usize>,
    delivery_envelopes: BTreeMap<(AgentId, AgentId, TurnId), (u64, Arc<ChildDelivery>)>,
    checkpoint_error: Option<String>,
    conversion_errors: BTreeMap<(AgentId, TurnId), String>,
    aborted_preparations: BTreeSet<u64>,
    incarnations: BTreeMap<AgentId, u64>,
    replacement: Option<MakimaId>,
}

pub struct JournalCheckpoint {
    snapshot: SessionJournalSnapshot,
    _publication: RwLockReadGuardArc<()>,
}

impl JournalCheckpoint {
    pub fn snapshot(&self) -> &SessionJournalSnapshot {
        &self.snapshot
    }
}

/// An owned publication lane, acquired before reading runtime state that the
/// prepared journal candidate must publish and finalize without an ordinary checkpoint.
pub struct JournalPublication {
    journal: Arc<SessionJournal>,
    publication: RwLockWriteGuardArc<()>,
}

impl JournalPublication {
    pub async fn prepare(self, commit: JournalCommit) -> Result<JournalPrepared, JournalError> {
        self.prepare_with(|_| commit).await
    }

    pub async fn prepare_with(
        self,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
    ) -> Result<JournalPrepared, JournalError> {
        self.journal.prepare_reserved(self.publication, build).await
    }
}

pub struct JournalPrepared {
    journal: Arc<SessionJournal>,
    pending: Option<JournalPending>,
    commit: JournalCommit,
    candidate: SessionJournalSnapshot,
    _boundary: RwLockWriteGuardArc<()>,
    _publication: RwLockWriteGuardArc<()>,
}

impl JournalPrepared {
    pub fn commit(mut self) -> Result<u64, JournalError> {
        self.install()
    }

    pub fn is_persistent(&self) -> bool {
        self.journal.is_persistent()
    }

    pub fn snapshot(&self) -> &SessionJournalSnapshot {
        &self.candidate
    }

    pub fn install(&mut self) -> Result<u64, JournalError> {
        Self::install_batch(&mut [&mut *self])?;
        Ok(self.candidate.revision)
    }

    pub fn install_batch(tokens: &mut [&mut Self]) -> Result<(), JournalError> {
        let mut order = (0..tokens.len()).collect::<Vec<_>>();
        order.sort_unstable_by_key(|index| Arc::as_ptr(&tokens[*index].journal));
        if order
            .windows(2)
            .any(|pair| Arc::ptr_eq(&tokens[pair[0]].journal, &tokens[pair[1]].journal))
        {
            return Err(JournalError::InvalidPending);
        }
        let journals = order
            .iter()
            .map(|index| Arc::clone(&tokens[*index].journal))
            .collect::<Vec<_>>();
        let mut guards = journals
            .iter()
            .map(|journal| journal.lock())
            .collect::<Vec<_>>();
        let mut candidates = guards
            .iter()
            .map(|state| (**state).clone())
            .collect::<Vec<_>>();
        for (index, state) in order.iter().zip(&mut candidates) {
            tokens[*index].install_locked(state)?;
        }
        for (guard, candidate) in guards.iter_mut().zip(candidates) {
            **guard = candidate;
        }
        drop(guards);
        for token in tokens {
            if token.pending.is_none() {
                continue;
            }
            token.pending = None;
            token.journal.conversion_changed.notify(usize::MAX);
            token.journal.drain_state_notices();
        }
        Ok(())
    }

    fn install_locked(&self, state: &mut JournalState) -> Result<u64, JournalError> {
        if self.pending.is_none() {
            return Ok(self.candidate.revision);
        }
        SessionJournal::validate_incarnations(state, &self.commit.incarnations)?;
        for update in &self.commit.context_updates {
            validate_access(
                &state.plugin_state,
                &state.snapshot,
                &update.access,
                StateScope::Agent,
                true,
                None,
            )?;
            if manifest_hash(&state.snapshot, update.access.target) != update.expected {
                return Err(StateError::Conflict.into());
            }
        }
        let pending = self
            .pending
            .as_ref()
            .ok_or(JournalError::InvalidPending)?
            .clone();
        let index = state
            .staged
            .iter()
            .position(|entry| entry.pending.as_ref() == Some(&pending))
            .ok_or(JournalError::InvalidPending)?;
        if self.candidate.epoch != state.snapshot.epoch {
            return Err(JournalError::StaleCheckpoint);
        }
        let history_epochs = SessionJournal::changed_history_epochs(&state.snapshot, &self.commit);
        SessionJournal::complete_locked(state, pending, self.commit.changes.clone())?;
        let history_changes: BTreeMap<_, _> = self
            .commit
            .histories
            .iter()
            .filter(|(agent, history)| {
                state.staged[index]
                    .snapshot
                    .histories
                    .get(agent)
                    .is_none_or(|before| {
                        before.epoch != history.epoch
                            || before.compacted != history.compacted
                            || before.conversation != history.conversation
                            || !Arc::ptr_eq(&before.messages, &history.messages)
                    })
            })
            .map(|(agent, history)| (*agent, history.clone()))
            .collect();
        for entry in state.staged.iter_mut().skip(index) {
            entry.snapshot.histories.extend(history_changes.clone());
            entry
                .snapshot
                .managed_histories
                .extend(self.commit.managed_histories.clone());
        }
        state.snapshot.histories.extend(history_changes);
        state
            .snapshot
            .managed_histories
            .extend(self.commit.managed_histories.clone());
        for agent in history_epochs {
            state
                .plugin_state
                .agent_epochs
                .insert(agent, MakimaId::generate());
        }
        for address in SessionJournal::state_addresses(&self.commit) {
            state.plugin_state.reservations.remove(&address);
        }
        SessionJournal::advance_ready(state);
        state.published_revision = state.published_revision.max(self.candidate.revision);
        state
            .plugin_state
            .enqueue_notices(SessionJournal::state_events(
                &self.candidate,
                &self.commit.changes,
            ));
        Ok(self.candidate.revision)
    }
}

impl Drop for JournalPrepared {
    fn drop(&mut self) {
        let Some(pending) = self.pending.take() else {
            return;
        };
        let mut state = self.journal.lock();
        if let Some(index) = state
            .staged
            .iter()
            .position(|entry| entry.pending.as_ref() == Some(&pending))
        {
            state.staged.remove(index);
        }
        for address in SessionJournal::state_addresses(&self.commit) {
            state.plugin_state.reservations.remove(&address);
        }
        state.aborted_preparations.insert(self.candidate.revision);
        SessionJournal::advance_ready(&mut state);
        self.journal.conversion_changed.notify(usize::MAX);
    }
}

pub struct PreparedRewindNotification {
    journal: Arc<SessionJournal>,
    notices: Vec<StateChangeEvent>,
}

impl PreparedRewindNotification {
    pub fn notify(self) {
        self.journal
            .lock()
            .plugin_state
            .enqueue_notices(self.notices);
        self.journal.drain_state_notices();
    }
}

pub struct JournalReplacement {
    journal: Arc<SessionJournal>,
    candidate: SessionJournalSnapshot,
    affected: BTreeSet<AgentId>,
    session_scope: bool,
    reservation: MakimaId,
    previous_revision: u64,
    notices: Vec<StateChangeEvent>,
    installed: bool,
    _boundary: Option<RwLockWriteGuardArc<()>>,
    _publication: Option<RwLockWriteGuardArc<()>>,
}

impl JournalReplacement {
    pub fn snapshot(&self) -> &SessionJournalSnapshot {
        &self.candidate
    }

    pub fn affected_agents(&self) -> &BTreeSet<AgentId> {
        &self.affected
    }

    pub fn session_scope(&self) -> bool {
        self.session_scope
    }

    pub fn apply_configs(&mut self, changes: Vec<JournalChange>) -> Result<(), RewindError> {
        if self.installed {
            return Err(RewindError::LostReservation);
        }
        let mut candidate = self.candidate.clone();
        for change in changes {
            if !change
                .agent
                .is_some_and(|agent| self.affected.contains(&agent))
                || !matches!(change.change, StateChange::Config(_))
            {
                return Err(StateError::ReadOnly.into());
            }
            crate::rewind::push(
                &mut candidate,
                change.agent,
                change.generation,
                change.change,
            )?;
        }
        SessionStateView::fold(&candidate.state, &candidate.agent_revisions, candidate.seq)
            .map_err(JournalError::from)?;
        self.candidate = candidate;
        Ok(())
    }

    pub fn apply_migrations(&mut self, changes: Vec<JournalChange>) -> Result<(), RewindError> {
        if self.installed {
            return Err(RewindError::LostReservation);
        }
        let mut candidate = self.candidate.clone();
        SessionJournal::migrate_replacement(
            &mut candidate,
            &self.affected,
            self.session_scope,
            changes,
        )?;
        let mut notices: BTreeMap<_, _> = self
            .notices
            .iter()
            .map(|notice| ((notice.agent, notice.key.clone()), notice.clone()))
            .collect();
        for record in &candidate.state {
            if (self.session_scope
                || record
                    .agent
                    .is_some_and(|agent| self.affected.contains(&agent)))
                && let StateChange::Plugin { key, version, .. } = &record.change
            {
                notices.insert(
                    (record.agent, key.clone()),
                    StateChangeEvent {
                        agent: record.agent,
                        key: key.clone(),
                        revision: read_slot(&candidate, record.agent, key, *version).revision,
                        reason: StateChangeReason::Rewind,
                    },
                );
            }
        }
        self.candidate = candidate;
        self.notices = notices.into_values().collect();
        Ok(())
    }

    pub fn with_prepared_actors(
        mut self,
        actors: &[crate::actor::PreparedActorRewind],
    ) -> Result<Self, RewindError> {
        if self.installed {
            return Err(RewindError::LostReservation);
        }
        for actor in actors {
            let agent = actor.agent_id();
            if !self.affected.contains(&agent) {
                return Err(StateError::ReadOnly.into());
            }
            let previous_epoch = self
                .candidate
                .histories
                .get(&agent)
                .map(|history| history.epoch);
            self.candidate
                .histories
                .insert(agent, actor.history().clone());
            if let Some(previous_epoch) = previous_epoch {
                rebase_history_epoch(&mut self.candidate, agent, previous_epoch);
            }
            let config = actor.config();
            let options = self
                .candidate
                .state
                .iter()
                .rev()
                .find_map(|record| match &record.change {
                    StateChange::Config(config) if record.agent == Some(agent) => {
                        Some(config.options.clone())
                    }
                    _ => None,
                })
                .unwrap_or_default();
            let stored = crate::session_checkpoint::stored_agent_config(&config.config, options)
                .map_err(|error| JournalError::Preparation(error.to_string()))?;
            crate::rewind::push(
                &mut self.candidate,
                Some(agent),
                config.generation,
                StateChange::Config(stored),
            )?;
        }
        let lens = self
            .candidate
            .histories
            .iter()
            .map(|(agent, history)| (*agent, (history.epoch, history.messages.len())))
            .collect();
        if let Some(record) = self
            .candidate
            .state
            .iter_mut()
            .rev()
            .find(|record| matches!(record.change, StateChange::Boundary { .. }))
            && let StateChange::Boundary { lens: saved, .. } = &mut record.change
        {
            *saved = lens;
        }
        Ok(self)
    }

    pub fn prepare_handoff(
        mut self,
        installed: Arc<SessionJournal>,
    ) -> Result<PreparedRewindNotification, RewindError> {
        if self.installed || Arc::ptr_eq(&self.journal, &installed) {
            return Err(RewindError::LostReservation);
        }
        {
            let state = self.journal.lock();
            if state.replacement != Some(self.reservation)
                || state.snapshot.revision != self.previous_revision
                || !state.staged.is_empty()
            {
                return Err(RewindError::LostReservation);
            }
        }
        {
            let state = installed.lock();
            let candidate = &self.candidate;
            let actual = &state.snapshot;
            if actual.epoch != candidate.epoch
                || actual.seq != candidate.seq
                || actual.revision != candidate.revision
                || actual.state != candidate.state
                || actual.agent_revisions != candidate.agent_revisions
                || actual.managed_histories != candidate.managed_histories
                || actual.histories.len() != candidate.histories.len()
                || actual.histories.iter().any(|(agent, history)| {
                    candidate.histories.get(agent).is_none_or(|saved| {
                        history.epoch != saved.epoch
                            || history.compacted != saved.compacted
                            || history.conversation != saved.conversation
                            || history.messages != saved.messages
                    })
                })
                || !state.staged.is_empty()
            {
                return Err(RewindError::LostReservation);
            }
        }
        Ok(PreparedRewindNotification {
            journal: installed,
            notices: take(&mut self.notices),
        })
    }

    pub fn install(&mut self) -> Result<(), RewindError> {
        if self.installed {
            return Ok(());
        }
        let mut state = self.journal.lock();
        if state.replacement != Some(self.reservation)
            || state.snapshot.revision != self.previous_revision
            || !state.staged.is_empty()
        {
            return Err(RewindError::LostReservation);
        }
        state
            .results
            .retain(|(agent, _), _| !self.affected.contains(agent));
        state
            .conversion_errors
            .retain(|(agent, _), _| !self.affected.contains(agent));
        state.delivery_envelopes.retain(|(parent, source, _), _| {
            !self.session_scope
                && !self.affected.contains(parent)
                && !self.affected.contains(source)
        });
        state.delivery_bytes.retain(|(parent, source, _), _| {
            !self.session_scope
                && !self.affected.contains(parent)
                && !self.affected.contains(source)
        });
        state
            .plugin_state
            .active_turns
            .retain(|(agent, _), _| !self.affected.contains(agent));
        for agent in &self.affected {
            state
                .plugin_state
                .agent_epochs
                .insert(*agent, MakimaId::generate());
        }
        if self.session_scope {
            state.plugin_state.session_epoch = MakimaId::generate();
        }
        state.snapshot = self.candidate.clone();
        state.ready = self.candidate.clone();
        state.published_revision = self.candidate.revision;
        state.replacement = None;
        self.installed = true;
        drop(state);
        self._boundary.take();
        self._publication.take();
        self.journal.conversion_changed.notify(usize::MAX);
        Ok(())
    }

    pub fn notify(&mut self) {
        if !self.installed {
            return;
        }
        self.journal
            .lock()
            .plugin_state
            .enqueue_notices(take(&mut self.notices));
        self.journal.drain_state_notices();
    }
}

impl Drop for JournalReplacement {
    fn drop(&mut self) {
        let mut state = self.journal.lock();
        if state.replacement == Some(self.reservation) {
            state.replacement = None;
        }
        self.journal.conversion_changed.notify(usize::MAX);
    }
}

pub struct SessionJournal {
    boundaries: Arc<RwLock<()>>,
    publication: Arc<RwLock<()>>,
    persistent: bool,
    state: Mutex<JournalState>,
    conversion_changed: Event,
}

impl Default for SessionJournal {
    fn default() -> Self {
        let snapshot = SessionJournalSnapshot::empty();
        Self {
            persistent: true,
            boundaries: Arc::new(RwLock::new(())),
            publication: Arc::new(RwLock::new(())),
            state: Mutex::new(JournalState {
                snapshot: snapshot.clone(),
                plugin_state: StateRuntime::default(),
                ready: snapshot,
                staged: VecDeque::new(),
                published_revision: 0,
                session_id: None,
                results: BTreeMap::new(),
                delivery_bytes: BTreeMap::new(),
                delivery_envelopes: BTreeMap::new(),
                checkpoint_error: None,
                conversion_errors: BTreeMap::new(),
                aborted_preparations: BTreeSet::new(),
                incarnations: BTreeMap::new(),
                replacement: None,
            }),
            conversion_changed: Event::new(),
        }
    }
}

impl SessionJournal {
    pub fn nonpersistent() -> Self {
        Self {
            persistent: false,
            ..Self::default()
        }
    }

    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    pub fn from_session(session: &StoredSession) -> Result<Self, JournalError> {
        SessionStateView::fold(session.state(), session.agent_revisions(), u64::MAX)?;
        let journal = Self::default();
        {
            let mut state = journal.lock();
            let snapshot = &mut state.snapshot;
            snapshot.state = session.state().to_vec();
            snapshot.agent_revisions = session.agent_revisions().to_vec();
            snapshot.seq = snapshot
                .state
                .iter()
                .map(|record| record.seq)
                .chain(snapshot.agent_revisions.iter().map(|(seq, _)| *seq))
                .chain(session.sequence_high_water())
                .max()
                .unwrap_or(0);
            for record in session.agents().values() {
                let history = if record.transcript_key.is_empty() {
                    session.history_snapshot()
                } else {
                    session
                        .subagent_history_snapshot(&record.transcript_key)
                        .unwrap_or_default()
                };
                snapshot.histories.insert(record.id, history);
            }
            state.ready = state.snapshot.clone();
            state.session_id = Some(session.id);
        }
        Ok(journal)
    }

    pub(crate) fn from_exact_snapshot(
        session: &StoredSession,
        snapshot: SessionJournalSnapshot,
    ) -> Result<Self, JournalError> {
        let view =
            SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)?;
        if session.state() != snapshot.state
            || session.agent_revisions() != snapshot.agent_revisions
            || session
                .sequence_high_water()
                .is_some_and(|seq| seq > snapshot.seq)
            || view
                .agents
                .keys()
                .any(|agent| !snapshot.histories.contains_key(agent))
            || snapshot
                .histories
                .keys()
                .any(|agent| !view.agents.contains_key(agent))
        {
            return Err(JournalError::Preparation(
                "exact runtime candidate does not match session state".into(),
            ));
        }
        for (agent, history) in &snapshot.histories {
            let record = &view.agents[agent];
            let saved = if record.transcript_key.is_empty() {
                session.history_snapshot()
            } else {
                session
                    .subagent_history_snapshot(&record.transcript_key)
                    .ok_or_else(|| {
                        JournalError::Preparation(
                            "exact runtime candidate has no stored history".into(),
                        )
                    })?
            };
            if saved.messages != history.messages
                || saved.compacted != history.compacted
                || saved.conversation != history.conversation
            {
                return Err(JournalError::Preparation(
                    "exact runtime history differs from published candidate".into(),
                ));
            }
        }
        let journal = Self::default();
        {
            let mut state = journal.lock();
            state.snapshot = snapshot.clone();
            state.ready = snapshot;
            state.session_id = Some(session.id);
        }
        Ok(journal)
    }

    pub fn set_incarnation(&self, agent: AgentId, reservation: u64) {
        let mut state = self.lock();
        if state.incarnations.get(&agent) != Some(&reservation) {
            state
                .plugin_state
                .active_turns
                .retain(|(id, _), _| *id != agent);
        }
        state.incarnations.insert(agent, reservation);
        let stale: Vec<_> = state
            .results
            .iter()
            .filter(|(_, result)| {
                result.incarnation.as_ref().is_some_and(|binding| {
                    binding.agent == agent && binding.reservation != reservation
                })
            })
            .map(|(key, _)| *key)
            .collect();
        for key in stale {
            if let Some(result) = state.results.remove(&key) {
                state
                    .delivery_envelopes
                    .retain(|(_, child, turn), _| (*child, *turn) != key);
                state
                    .delivery_bytes
                    .retain(|(_, child, turn), _| (*child, *turn) != key);
                let pending: Vec<_> = result
                    .terminal
                    .into_iter()
                    .chain(
                        result
                            .deliveries
                            .into_iter()
                            .map(|(pending, _, _, _)| pending),
                    )
                    .collect();
                state.staged.retain(|entry| {
                    !entry
                        .pending
                        .as_ref()
                        .is_some_and(|candidate| pending.contains(candidate))
                });
                state.conversion_errors.remove(&key);
            }
        }
        Self::advance_ready(&mut state);
        self.conversion_changed.notify(usize::MAX);
    }

    pub fn incarnation(&self, agent: AgentId) -> JournalIncarnation {
        JournalIncarnation {
            agent,
            reservation: self.lock().incarnations.get(&agent).copied().unwrap_or(0),
        }
    }

    fn validate_incarnations(
        state: &JournalState,
        bindings: &[JournalIncarnation],
    ) -> Result<(), JournalError> {
        for binding in bindings {
            if state.incarnations.get(&binding.agent).copied().unwrap_or(0) != binding.reservation {
                return Err(JournalError::StaleIncarnation(binding.agent));
            }
        }
        Ok(())
    }

    pub fn commit_for(
        &self,
        binding: JournalIncarnation,
        mut commit: JournalCommit,
    ) -> Result<u64, JournalError> {
        commit.incarnations.push(binding);
        self.commit(commit)
    }

    pub fn commit_with_for(
        &self,
        binding: JournalIncarnation,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
    ) -> Result<u64, JournalError> {
        self.commit_with(|snapshot| {
            let mut commit = build(snapshot);
            commit.incarnations.push(binding);
            commit
        })
    }

    pub fn bind_session(&self, session: MakimaId) -> Result<(), JournalError> {
        let mut state = self.lock();
        if state.session_id.is_some_and(|id| id != session) {
            return Err(JournalError::StaleCheckpoint);
        }
        state.session_id = Some(session);
        Ok(())
    }

    pub fn session_id(&self) -> Option<MakimaId> {
        self.lock().session_id
    }

    pub fn snapshot(&self) -> SessionJournalSnapshot {
        self.lock().ready.clone()
    }

    pub fn checkpoint_snapshot(&self) -> Result<SessionJournalSnapshot, String> {
        let state = self.lock();
        if let Some(error) = state.conversion_errors.values().next() {
            return Err(error.clone());
        }
        Ok(state.ready.clone())
    }

    pub fn persistence_error(&self) -> Option<String> {
        let state = self.lock();
        state
            .conversion_errors
            .values()
            .next()
            .cloned()
            .or_else(|| state.checkpoint_error.clone())
    }

    pub fn set_checkpoint_error(&self, error: Option<String>) {
        self.lock().checkpoint_error = error;
    }

    pub async fn wait_terminal_conversion(
        &self,
        agent: AgentId,
        turn: TurnId,
    ) -> Result<(), String> {
        loop {
            let listener = self.conversion_changed.listen();
            {
                let state = self.lock();
                if let Some(error) = state.conversion_errors.get(&(agent, turn)) {
                    return Err(error.clone());
                }
                if !state.results.contains_key(&(agent, turn))
                    && state.ready.state.iter().rev().any(|record| record.agent == Some(agent)
                        && matches!(&record.change, StateChange::Turn { turn: saved, .. } if *saved == turn))
                {
                    return Ok(());
                }
                if let Some(error) = state.conversion_errors.values().next() {
                    return Err(error.clone());
                }
            }
            listener.await;
        }
    }

    pub fn pending_revision(&self) -> Option<u64> {
        self.lock()
            .staged
            .front()
            .map(|entry| entry.snapshot.revision)
    }

    pub fn transcript_key_in_use(&self, key: &str, except: Option<AgentId>) -> bool {
        self.lock()
            .snapshot
            .agent_revisions
            .iter()
            .any(|(_, record)| Some(record.id) != except && record.transcript_key == key)
    }

    pub fn agent(&self, id: AgentId) -> Option<StoredAgentRecord> {
        self.lock()
            .snapshot
            .agent_revisions
            .iter()
            .rev()
            .find(|(_, record)| record.id == id)
            .map(|(_, record)| record.clone())
    }

    pub fn replace_terminal_outcome(&self, outcome: &TurnOutcome) -> Result<(), JournalError> {
        self.replace_terminal_outcome_for(None, outcome)
    }

    pub fn replace_terminal_outcome_for(
        &self,
        incarnation: Option<JournalIncarnation>,
        outcome: &TurnOutcome,
    ) -> Result<(), JournalError> {
        let mut state = self.lock();
        Self::validate_incarnations(&state, &incarnation.into_iter().collect::<Vec<_>>())?;
        let key = (outcome.agent_id(), outcome.turn_id());
        if let Some(pending) = state.results.get_mut(&key) {
            pending.outcome = Some(outcome.clone());
            return Ok(());
        }
        let result = state
            .snapshot
            .state
            .iter()
            .rev()
            .find_map(|record| match &record.change {
                StateChange::Turn { turn, result }
                    if record.agent == Some(key.0) && *turn == key.1 =>
                {
                    Some(result.clone())
                }
                _ => None,
            });
        let Some(mut result) = result else {
            return Err(JournalError::InvalidPending);
        };
        result.outcome = outcome.clone().into();
        let candidate = Self::apply_commit(
            &state.snapshot,
            JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(key.0),
                    generation: 0,
                    change: StateChange::Turn {
                        turn: key.1,
                        result,
                    },
                }],
                ..Default::default()
            },
        )?;
        state.snapshot = candidate.clone();
        state.staged.push_back(StagedCommit {
            snapshot: candidate,
            pending: None,
        });
        Self::advance_ready(&mut state);
        Ok(())
    }

    pub fn record_terminal(
        self: &Arc<Self>,
        result: TurnResult,
        store: Option<Arc<OffloadStore>>,
    ) -> Result<(), JournalError> {
        self.record_terminal_for(None, result, store)
    }

    pub fn record_terminal_for(
        self: &Arc<Self>,
        incarnation: Option<JournalIncarnation>,
        result: TurnResult,
        store: Option<Arc<OffloadStore>>,
    ) -> Result<(), JournalError> {
        if !self.persistent {
            return Ok(());
        }
        let key = (result.outcome.agent_id(), result.outcome.turn_id());
        let mut state = self.lock();
        Self::validate_incarnations(&state, &incarnation.clone().into_iter().collect::<Vec<_>>())?;
        if state.results.contains_key(&key) {
            return Ok(());
        }
        let history = state.snapshot.histories.get(&key.0).cloned();
        let pending = Self::stage_locked(
            &mut state,
            JournalCommit {
                histories: history
                    .map(|history| (key.0, history))
                    .into_iter()
                    .collect(),
                managed_histories: [key.0].into(),
                ..Default::default()
            },
            1,
        )?;
        state.results.insert(
            key,
            PendingResult {
                conversion: Arc::new(Mutex::new(RuntimeResult {
                    result,
                    progress: StoredTurnProgress::default(),
                    stored: None,
                    store,
                })),
                terminal: Some(pending),
                deliveries: Vec::new(),
                released: false,
                outcome: None,
                incarnation,
            },
        );
        Ok(())
    }

    pub fn release_terminal(self: &Arc<Self>, agent: AgentId, turn: TurnId) {
        let key = (agent, turn);
        let mut state = self.lock();
        let Some(result) = state.results.get_mut(&key) else {
            return;
        };
        if result.released {
            return;
        }
        result.released = true;
        drop(state);
        self.spawn_conversion(key);
    }

    pub(crate) fn pending_deliveries(
        &self,
        parent: AgentId,
    ) -> Result<Vec<PendingDeliveryEnvelope>, String> {
        let state = self.lock();
        let view = SessionStateView::fold(
            &state.snapshot.state,
            &state.snapshot.agent_revisions,
            state.snapshot.seq,
        )
        .map_err(|error| error.to_string())?;
        let scope = view.scopes.get(&Some(parent));
        let mut deliveries = Vec::new();
        if let Some(scope) = scope {
            for (id, op) in &scope.deliveries {
                if !matches!(op, StoredDeliveryOp::Queued(_)) {
                    continue;
                }
                let sequence = state.snapshot.state.iter().rev().find_map(|record| {
                    (record.agent == Some(parent) && matches!(&record.change, StateChange::Delivery { id: saved, .. } if saved == id)).then_some(record.seq)
                }).unwrap_or(0);
                let envelope = state
                    .delivery_envelopes
                    .get(&(parent, id.0, id.1))
                    .map(|(_, delivery)| Arc::clone(delivery));
                deliveries.push((sequence, id.0, id.1, envelope));
            }
        }
        for ((target, child, turn), (sequence, delivery)) in &state.delivery_envelopes {
            if *target == parent
                && scope.is_none_or(|scope| !scope.deliveries.contains_key(&(*child, *turn)))
            {
                deliveries.push((*sequence, *child, *turn, Some(Arc::clone(delivery))));
            }
        }
        deliveries.sort_by_key(|(sequence, _, _, _)| *sequence);
        Ok(deliveries
            .into_iter()
            .map(|(_, child, turn, delivery)| (child, turn, delivery))
            .collect())
    }

    pub fn queue_delivery(
        self: &Arc<Self>,
        parent: AgentId,
        delivery: Arc<ChildDelivery>,
        preview_bytes: usize,
    ) -> Result<bool, DeliveryRejection> {
        self.queue_delivery_for(parent, delivery, preview_bytes, &[])
    }

    pub fn queue_delivery_for(
        self: &Arc<Self>,
        parent: AgentId,
        delivery: Arc<ChildDelivery>,
        preview_bytes: usize,
        incarnations: &[JournalIncarnation],
    ) -> Result<bool, DeliveryRejection> {
        if !self.persistent {
            return Err(DeliveryRejection::ParentUnavailable);
        }
        let child = delivery.result.outcome.agent_id();
        let turn = delivery.result.outcome.turn_id();
        let key = (child, turn);
        let mut state = self.lock();
        Self::validate_incarnations(&state, incarnations)
            .map_err(|error| DeliveryRejection::Journal(error.to_string()))?;
        let view = SessionStateView::fold(
            &state.snapshot.state,
            &state.snapshot.agent_revisions,
            state.snapshot.seq,
        )
        .map_err(|_| DeliveryRejection::ParentUnavailable)?;
        if state.delivery_bytes.contains_key(&(parent, child, turn))
            || view
                .scopes
                .get(&Some(parent))
                .is_some_and(|scope| scope.deliveries.contains_key(&key))
        {
            return Ok(false);
        }
        let mut sizes = state.delivery_bytes.clone();
        if let Some(scope) = view.scopes.get(&Some(parent)) {
            for ((child, turn), op) in &scope.deliveries {
                if matches!(op, StoredDeliveryOp::Queued(_)) {
                    sizes.entry((parent, *child, *turn)).or_insert_with(|| {
                        let StoredDeliveryOp::Queued(delivery) = op else {
                            return 0;
                        };
                        restored_preview_bytes(delivery)
                    });
                }
            }
        }
        let (count, bytes) = sizes
            .iter()
            .filter(|((owner, source, id), _)| {
                *owner == parent
                    && !view
                        .scopes
                        .get(&Some(parent))
                        .and_then(|scope| scope.deliveries.get(&(*source, *id)))
                        .is_some_and(|op| !matches!(op, StoredDeliveryOp::Queued(_)))
            })
            .fold((0usize, 0usize), |(count, bytes), (_, size)| {
                (count + 1, bytes.saturating_add(*size))
            });
        if preview_bytes > MAX_DELIVERY_PREVIEW_BYTES
            || count >= MAX_PENDING_DELIVERIES
            || bytes.saturating_add(preview_bytes) > MAX_PENDING_PREVIEW_BYTES
        {
            return Err(DeliveryRejection::Overflow);
        }
        let pending = Self::stage_locked(&mut state, JournalCommit::default(), 1)
            .map_err(|_| DeliveryRejection::ParentUnavailable)?;
        let spawn;
        if let Some(result) = state.results.get_mut(&key) {
            result
                .deliveries
                .push((pending, parent, delivery.label.clone(), preview_bytes));
            spawn = false;
        } else {
            let stored = view
                .scopes
                .get(&Some(child))
                .and_then(|scope| scope.results.iter().find(|(id, _)| *id == turn))
                .map(|(_, result)| result.clone());
            state.results.insert(
                key,
                PendingResult {
                    conversion: Arc::new(Mutex::new(RuntimeResult {
                        result: delivery.result.clone(),
                        progress: StoredTurnProgress::default(),
                        stored,
                        store: None,
                    })),
                    terminal: None,
                    deliveries: vec![(pending, parent, delivery.label.clone(), preview_bytes)],
                    released: true,
                    outcome: None,
                    incarnation: None,
                },
            );
            spawn = true;
        }
        state
            .delivery_bytes
            .insert((parent, child, turn), preview_bytes);
        let sequence = state.snapshot.seq;
        state
            .delivery_envelopes
            .insert((parent, child, turn), (sequence, delivery));
        drop(state);
        if spawn {
            self.spawn_conversion(key);
        }
        Ok(true)
    }

    fn spawn_conversion(self: &Arc<Self>, key: (AgentId, TurnId)) {
        let conversion = self
            .lock()
            .results
            .get(&key)
            .map(|result| Arc::clone(&result.conversion));
        if let Some(conversion) = conversion {
            let inline = {
                let mut conversion = conversion.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(stored) = &conversion.stored {
                    Some(stored.clone())
                } else {
                    let RuntimeResult {
                        result, progress, ..
                    } = &mut *conversion;
                    result
                        .to_stored_with_progress(progress, MakimaId::generate(), None)
                        .ok()
                }
            };
            if let Some(stored) = inline
                && self.complete_conversion(key, &conversion, stored).is_ok()
            {
                return;
            }
        }
        let journal = Arc::clone(self);
        smol::spawn(async move {
            loop {
                let (conversion, session, inline_session) = {
                    let state = journal.lock();
                    let Some(result) = state.results.get(&key) else { return };
                    (Arc::clone(&result.conversion), state.session_id, state.snapshot.epoch)
                };
                let expected = Arc::clone(&conversion);
                let converted = smol::unblock(move || {
                    let mut conversion = conversion.lock().unwrap_or_else(|error| error.into_inner());
                    if let Some(stored) = &conversion.stored {
                        return Ok(stored.clone());
                    }
                    let RuntimeResult { result, progress, store, .. } = &mut *conversion;
                    let stored = result.to_stored_with_progress(progress, session.unwrap_or(inline_session), session.and(store.as_deref()))?;
                    conversion.stored = Some(stored.clone());
                    Ok::<_, TurnResultStorageError>(stored)
                }).await;
                match converted {
                    Ok(stored) => {
                        if journal.complete_conversion(key, &expected, stored).is_ok() {
                            return;
                        }
                        smol::Timer::after(CONVERSION_RETRY_DELAY).await;
                    }
                    Err(error) => {
                        if !journal.record_conversion_error(key, &expected, error.to_string()) {
                            return;
                        }
                        tracing::warn!(%error, agent_id = %key.0, turn_id = %key.1, "turn result is unsaved; persistence will retry");
                        smol::Timer::after(CONVERSION_RETRY_DELAY).await;
                    }
                }
            }
        }).detach();
    }

    fn record_conversion_error(
        &self,
        key: (AgentId, TurnId),
        expected: &Arc<Mutex<RuntimeResult>>,
        error: String,
    ) -> bool {
        let mut state = self.lock();
        if state
            .results
            .get(&key)
            .is_none_or(|result| !Arc::ptr_eq(&result.conversion, expected))
        {
            return false;
        }
        state.conversion_errors.insert(key, error);
        self.conversion_changed.notify(usize::MAX);
        true
    }

    fn complete_conversion(
        &self,
        key: (AgentId, TurnId),
        expected: &Arc<Mutex<RuntimeResult>>,
        mut stored: StoredTurnResult,
    ) -> Result<(), JournalError> {
        let mut state = self.lock();
        let Some(result) = state.results.get(&key) else {
            return Ok(());
        };
        if !Arc::ptr_eq(&result.conversion, expected) {
            return Ok(());
        }
        if let Some(binding) = &result.incarnation
            && let Err(error) = Self::validate_incarnations(&state, std::slice::from_ref(binding))
        {
            let pending = result
                .terminal
                .iter()
                .chain(result.deliveries.iter().map(|(pending, _, _, _)| pending))
                .cloned()
                .collect::<Vec<_>>();
            state.staged.retain(|entry| {
                !entry
                    .pending
                    .as_ref()
                    .is_some_and(|candidate| pending.contains(candidate))
            });
            state.results.remove(&key);
            state.conversion_errors.remove(&key);
            Self::advance_ready(&mut state);
            self.conversion_changed.notify(usize::MAX);
            return Err(error);
        }
        if let Some(outcome) = &result.outcome {
            stored.outcome = outcome.clone().into();
        }
        let mut commits = Vec::new();
        if let Some(pending) = result.terminal.clone() {
            commits.push((
                pending,
                JournalChange {
                    agent: Some(key.0),
                    generation: 0,
                    change: StateChange::Turn {
                        turn: key.1,
                        result: stored.clone(),
                    },
                },
            ));
        }
        for (pending, parent, label, preview_bytes) in result.deliveries.clone() {
            commits.push((
                pending,
                JournalChange {
                    agent: Some(parent),
                    generation: 0,
                    change: StateChange::Delivery {
                        id: key,
                        op: StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                            label,
                            preview_bytes: Some(preview_bytes),
                            held: false,
                            result: stored.clone(),
                        })),
                    },
                },
            ));
        }
        let previous_snapshot = state.snapshot.clone();
        let previous_staged = state.staged.clone();
        for (pending, change) in commits {
            if let Err(error) = Self::complete_locked(&mut state, pending, vec![change]) {
                state.snapshot = previous_snapshot;
                state.staged = previous_staged;
                tracing::warn!(%error, agent_id = %key.0, turn_id = %key.1, "terminal journal conversion could not commit");
                state.conversion_errors.insert(key, error.to_string());
                self.conversion_changed.notify(usize::MAX);
                return Err(error);
            }
        }
        state.results.remove(&key);
        state.conversion_errors.remove(&key);
        Self::advance_ready(&mut state);
        self.conversion_changed.notify(usize::MAX);
        Ok(())
    }

    pub fn try_publication_read(&self) -> Option<RwLockReadGuardArc<()>> {
        self.publication.try_read_arc()
    }

    pub async fn publication_read(&self) -> RwLockReadGuardArc<()> {
        self.publication.read_arc().await
    }

    pub async fn capture_checkpoint(&self) -> Result<JournalCheckpoint, String> {
        let publication = self.publication_read().await;
        let snapshot = self.checkpoint_snapshot()?;
        Ok(JournalCheckpoint {
            snapshot,
            _publication: publication,
        })
    }

    pub async fn boundary_read(&self) -> RwLockReadGuardArc<()> {
        self.boundaries.read_arc().await
    }

    pub fn preview_rewind(
        &self,
        boundary: RewindBoundary,
    ) -> Result<SessionJournalSnapshot, RewindError> {
        let current = self.snapshot();
        let (candidate, _, _) = prepare_boundary(&current, boundary)?;
        Ok(candidate)
    }

    pub async fn prepare_rewind(
        self: &Arc<Self>,
        boundary: RewindBoundary,
    ) -> Result<JournalReplacement, RewindError> {
        self.prepare_rewind_with(boundary, |_| Ok(Vec::new())).await
    }

    pub async fn prepare_rewind_with(
        self: &Arc<Self>,
        boundary: RewindBoundary,
        migrate: impl FnOnce(&SessionJournalSnapshot) -> Result<Vec<JournalChange>, RewindError>,
    ) -> Result<JournalReplacement, RewindError> {
        let mut replacement = self
            .prepare_replacement(move |current| prepare_boundary(current, boundary))
            .await?;
        let changes = migrate(replacement.snapshot())?;
        replacement.apply_migrations(changes)?;
        Ok(replacement)
    }

    fn migrate_replacement(
        candidate: &mut SessionJournalSnapshot,
        affected: &BTreeSet<AgentId>,
        session_scope: bool,
        changes: Vec<JournalChange>,
    ) -> Result<(), RewindError> {
        for change in changes {
            if change.agent.is_none() && !session_scope
                || change.agent.is_some_and(|agent| !affected.contains(&agent))
                || !matches!(change.change, StateChange::Plugin { .. })
            {
                return Err(StateError::ReadOnly.into());
            }
            validate_record(&change, candidate.seq)?;
            crate::rewind::push(candidate, change.agent, change.generation, change.change)?;
        }
        Ok(())
    }

    pub async fn prepare_branch_restore(
        self: &Arc<Self>,
        agent: AgentId,
        restored: SessionJournalSnapshot,
    ) -> Result<JournalReplacement, RewindError> {
        self.prepare_branch_restore_with(agent, restored, |_| Ok(Vec::new()))
            .await
    }

    pub async fn prepare_branch_restore_with(
        self: &Arc<Self>,
        agent: AgentId,
        restored: SessionJournalSnapshot,
        migrate: impl FnOnce(&SessionJournalSnapshot) -> Result<Vec<JournalChange>, RewindError>,
    ) -> Result<JournalReplacement, RewindError> {
        let mut replacement = self
            .prepare_replacement(move |current| {
                let view =
                    SessionStateView::fold(&current.state, &current.agent_revisions, current.seq)
                        .map_err(JournalError::from)?;
                let session_scope = view
                    .agents
                    .get(&agent)
                    .ok_or(RewindError::StaleBoundary)?
                    .parent
                    .is_none();
                let saved = SessionStateView::fold(
                    &restored.state,
                    &restored.agent_revisions,
                    restored.seq,
                )
                .map_err(JournalError::from)?;
                if !saved.agents.contains_key(&agent) {
                    return Err(RewindError::StaleBoundary);
                }
                let mut affected = subtree(&view, agent);
                affected.extend(subtree(&saved, agent));
                let mut candidate = if session_scope {
                    restored.clone()
                } else {
                    let mut candidate = current.clone();
                    candidate.seq = current.seq.max(restored.seq);
                    candidate
                        .state
                        .retain(|record| record.agent.is_none_or(|id| !affected.contains(&id)));
                    candidate
                        .agent_revisions
                        .retain(|(_, record)| !affected.contains(&record.id));
                    candidate.state.extend(
                        restored
                            .state
                            .iter()
                            .filter(|record| record.agent.is_some_and(|id| affected.contains(&id)))
                            .cloned(),
                    );
                    candidate.agent_revisions.extend(
                        restored
                            .agent_revisions
                            .iter()
                            .filter(|(_, record)| affected.contains(&record.id))
                            .cloned(),
                    );
                    crate::rewind::carry_boundary_pins(&restored, &mut candidate)?;
                    candidate.state.sort_by_key(|record| record.seq);
                    candidate.agent_revisions.sort_by_key(|(seq, _)| *seq);
                    candidate.histories.retain(|id, _| !affected.contains(id));
                    candidate.histories.extend(
                        restored
                            .histories
                            .iter()
                            .filter(|(id, _)| affected.contains(id))
                            .map(|(id, history)| (*id, history.clone())),
                    );
                    candidate
                        .managed_histories
                        .retain(|id| !affected.contains(id));
                    candidate.managed_histories.extend(
                        restored
                            .managed_histories
                            .iter()
                            .filter(|id| affected.contains(id)),
                    );
                    candidate
                };
                if !session_scope {
                    crate::rewind::pin_outside_boundaries(current, &mut candidate, &affected)?;
                }
                let mut previous_epochs = Vec::new();
                for (id, history) in &mut candidate.histories {
                    if affected.contains(id) {
                        previous_epochs.push((*id, history.epoch));
                        let mut restored = HistorySnapshot::new(history.messages.as_ref().clone());
                        restored.compacted = history.compacted;
                        *history = restored;
                    }
                }
                for (agent, epoch) in previous_epochs {
                    rebase_history_epoch(&mut candidate, agent, epoch);
                }
                finalize_candidate(current, &mut candidate, &affected, session_scope, agent)?;
                Ok((candidate, affected, session_scope))
            })
            .await?;
        let changes = migrate(replacement.snapshot())?;
        replacement.apply_migrations(changes)?;
        Ok(replacement)
    }

    async fn prepare_replacement(
        self: &Arc<Self>,
        build: impl FnOnce(
            &SessionJournalSnapshot,
        )
            -> Result<(SessionJournalSnapshot, BTreeSet<AgentId>, bool), RewindError>,
    ) -> Result<JournalReplacement, RewindError> {
        let publication = self.publication.write_arc().await;
        let boundary = self.boundaries.write_arc().await;
        let mut build = Some(build);
        loop {
            let listener = self.conversion_changed.listen();
            {
                let mut state = self.lock();
                if let Some(error) = state.conversion_errors.values().next() {
                    return Err(JournalError::Preparation(error.clone()).into());
                }
                if state.staged.is_empty() {
                    if state.replacement.is_some() {
                        return Err(RewindError::LostReservation);
                    }
                    let (candidate, affected, session_scope) =
                        build.take().ok_or(RewindError::LostReservation)?(&state.snapshot)?;
                    let mut addresses = BTreeSet::new();
                    for record in state.snapshot.state.iter().chain(&candidate.state) {
                        if (session_scope || record.agent.is_some_and(|id| affected.contains(&id)))
                            && let StateChange::Plugin { key, version, .. } = &record.change
                        {
                            addresses.insert((record.agent, key.clone(), *version));
                        }
                    }
                    let notices = addresses
                        .into_iter()
                        .map(|(agent, key, version)| StateChangeEvent {
                            revision: read_slot(&candidate, agent, &key, version).revision,
                            agent,
                            key,
                            reason: StateChangeReason::Rewind,
                        })
                        .collect();
                    let reservation = MakimaId::generate();
                    state.replacement = Some(reservation);
                    return Ok(JournalReplacement {
                        journal: Arc::clone(self),
                        previous_revision: state.snapshot.revision,
                        candidate,
                        affected,
                        session_scope,
                        reservation,
                        notices,
                        installed: false,
                        _boundary: Some(boundary),
                        _publication: Some(publication),
                    });
                }
            }
            listener.await;
        }
    }

    pub async fn prepare(
        self: &Arc<Self>,
        commit: JournalCommit,
    ) -> Result<JournalPrepared, JournalError> {
        self.prepare_with(|_| commit).await
    }

    pub async fn reserve_publication(self: &Arc<Self>) -> JournalPublication {
        JournalPublication {
            journal: Arc::clone(self),
            publication: self.publication.write_arc().await,
        }
    }

    pub async fn prepare_with(
        self: &Arc<Self>,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
    ) -> Result<JournalPrepared, JournalError> {
        self.reserve_publication().await.prepare_with(build).await
    }

    async fn prepare_reserved(
        self: &Arc<Self>,
        publication: RwLockWriteGuardArc<()>,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
    ) -> Result<JournalPrepared, JournalError> {
        let mut build = Some(build);
        let boundary = self.boundaries.write_arc().await;
        loop {
            let listener = self.conversion_changed.listen();
            {
                let mut state = self.lock();
                if let Some(error) = state.conversion_errors.values().next() {
                    return Err(JournalError::Preparation(error.clone()));
                }
                if state.staged.is_empty() {
                    if state.replacement.is_some() {
                        return Err(JournalError::Preparation(REPLACEMENT_RESERVED.into()));
                    }
                    let builder = build.take().ok_or(JournalError::InvalidPending)?;
                    let mut commit = builder(&state.snapshot);
                    if commit.changes.is_empty()
                        && commit.state_edits.is_empty()
                        && commit.context_updates.is_empty()
                        && commit.histories.is_empty()
                        && commit.state_epoch_agents.is_empty()
                        && commit.managed_histories.is_empty()
                        && commit.boundary.is_none()
                    {
                        return Err(JournalError::InvalidPending);
                    }
                    if !commit.agents.is_empty() {
                        return Err(JournalError::PreparedAgents);
                    }
                    Self::validate_incarnations(&state, &commit.incarnations)?;
                    self.resolve_state_edits(&mut state, &mut commit)?;
                    let candidate = Self::apply_commit(&state.snapshot, commit.clone())?;
                    let pending = if commit.changes.is_empty() {
                        let mut staged = state.snapshot.clone();
                        staged.revision = candidate.revision;
                        let pending = JournalPending {
                            epoch: staged.epoch,
                            revision: staged.revision,
                            first_seq: staged.seq,
                            count: 0,
                        };
                        state.snapshot = staged.clone();
                        state.staged.push_back(StagedCommit {
                            snapshot: staged,
                            pending: Some(pending.clone()),
                        });
                        pending
                    } else {
                        Self::stage_locked(
                            &mut state,
                            JournalCommit::default(),
                            commit.changes.len(),
                        )?
                    };
                    state.plugin_state.reservations.extend(
                        Self::state_addresses(&commit)
                            .into_iter()
                            .map(|address| (address, pending.revision)),
                    );
                    return Ok(JournalPrepared {
                        journal: Arc::clone(self),
                        pending: Some(pending),
                        commit,
                        candidate,
                        _boundary: boundary,
                        _publication: publication,
                    });
                }
            }
            listener.await;
        }
    }

    pub fn record_config(
        &self,
        agent: AgentId,
        generation: u64,
        config: StoredAgentConfig,
        options: Option<BTreeMap<String, String>>,
    ) -> Result<u64, JournalError> {
        self.record_config_bound(None, agent, generation, config, options, Vec::new())
    }

    pub fn record_config_for(
        &self,
        incarnation: JournalIncarnation,
        generation: u64,
        config: StoredAgentConfig,
        options: Option<BTreeMap<String, String>>,
    ) -> Result<u64, JournalError> {
        self.record_config_with_context_for(incarnation, generation, config, options, Vec::new())
    }

    pub(crate) fn record_config_with_context_for(
        &self,
        incarnation: JournalIncarnation,
        generation: u64,
        config: StoredAgentConfig,
        options: Option<BTreeMap<String, String>>,
        context_updates: Vec<ContextUpdate>,
    ) -> Result<u64, JournalError> {
        self.record_config_bound(
            Some(incarnation.clone()),
            incarnation.agent,
            generation,
            config,
            options,
            context_updates,
        )
    }

    fn record_config_bound(
        &self,
        incarnation: Option<JournalIncarnation>,
        agent: AgentId,
        generation: u64,
        mut config: StoredAgentConfig,
        options: Option<BTreeMap<String, String>>,
        context_updates: Vec<ContextUpdate>,
    ) -> Result<u64, JournalError> {
        self.commit_with(|snapshot| {
            let latest = snapshot
                .state
                .iter()
                .rev()
                .find_map(|record| match &record.change {
                    StateChange::Config(config) if record.agent == Some(agent) => Some(config),
                    _ => None,
                });
            config.options = options.unwrap_or_else(|| {
                latest
                    .map(|config| config.options.clone())
                    .unwrap_or(config.options.clone())
            });
            if latest == Some(&config) {
                return JournalCommit {
                    incarnations: incarnation.into_iter().collect(),
                    context_updates,
                    ..Default::default()
                };
            }
            JournalCommit {
                incarnations: incarnation.into_iter().collect(),
                context_updates,
                changes: vec![JournalChange {
                    agent: Some(agent),
                    generation,
                    change: StateChange::Config(config),
                }],
                ..Default::default()
            }
        })
    }

    pub(crate) fn record_rules_with_apply(
        &self,
        incarnation: Option<JournalIncarnation>,
        rules: Vec<StoredRule>,
        yolo: Option<bool>,
        apply: impl FnOnce(),
    ) -> Result<u64, JournalError> {
        self.commit_with_apply(
            |snapshot| {
                let latest = snapshot
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Rules { rules, yolo } if record.agent.is_none() => {
                            Some((rules, *yolo))
                        }
                        _ => None,
                    });
                let unchanged = latest == Some((&rules, yolo));
                JournalCommit {
                    incarnations: incarnation.into_iter().collect(),
                    changes: if unchanged {
                        Vec::new()
                    } else {
                        vec![JournalChange {
                            agent: None,
                            generation: 0,
                            change: StateChange::Rules { rules, yolo },
                        }]
                    },
                    ..Default::default()
                }
            },
            apply,
        )
    }

    pub fn commit(&self, commit: JournalCommit) -> Result<u64, JournalError> {
        self.commit_with(|_| commit)
    }

    pub fn commit_with(
        &self,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
    ) -> Result<u64, JournalError> {
        self.commit_with_apply(build, || ())
    }

    pub(crate) fn commit_with_apply(
        &self,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
        apply: impl FnOnce(),
    ) -> Result<u64, JournalError> {
        self.commit_with_receipts(build, apply)
            .map(|(revision, _)| revision)
    }

    pub(crate) fn commit_state_edit(&self, edit: StateEdit) -> Result<StateRevision, JournalError> {
        let (_, events) = self.commit_with_receipts(
            |_| JournalCommit {
                state_edits: vec![edit],
                ..Default::default()
            },
            || (),
        )?;
        events
            .first()
            .map(|event| event.revision)
            .ok_or(JournalError::InvalidPending)
    }

    fn commit_with_receipts(
        &self,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
        apply: impl FnOnce(),
    ) -> Result<(u64, Vec<StateChangeEvent>), JournalError> {
        let mut state = self.lock();
        if state.replacement.is_some() {
            return Err(JournalError::Preparation(REPLACEMENT_RESERVED.into()));
        }
        let mut commit = build(&state.snapshot);
        Self::validate_incarnations(&state, &commit.incarnations)?;
        self.resolve_state_edits(&mut state, &mut commit)?;
        let history_epochs = Self::changed_history_epochs(&state.snapshot, &commit);
        let changes = commit.changes.clone();
        let retired_deliveries: Vec<_> = commit
            .changes
            .iter()
            .filter_map(|change| {
                if let StateChange::Delivery {
                    id: (child, turn),
                    op,
                } = &change.change
                    && !matches!(op, StoredDeliveryOp::Queued(_))
                {
                    change.agent.map(|parent| (parent, *child, *turn))
                } else {
                    None
                }
            })
            .collect();
        let candidate = Self::apply_commit(&state.snapshot, commit)?;
        for key in retired_deliveries {
            state.delivery_bytes.remove(&key);
            state.delivery_envelopes.remove(&key);
        }
        if candidate.revision == state.snapshot.revision {
            apply();
            return Ok((candidate.revision, Vec::new()));
        }
        let revision = candidate.revision;
        let events = Self::state_events(&candidate, &changes);
        for agent in history_epochs {
            state
                .plugin_state
                .agent_epochs
                .insert(agent, MakimaId::generate());
        }
        state.snapshot = candidate.clone();
        state.staged.push_back(StagedCommit {
            snapshot: candidate,
            pending: None,
        });
        Self::advance_ready(&mut state);
        apply();
        state.plugin_state.enqueue_notices(events.clone());
        drop(state);
        self.drain_state_notices();
        Ok((revision, events))
    }

    fn resolve_context_updates(
        &self,
        state: &JournalState,
        commit: &mut JournalCommit,
    ) -> Result<(), JournalError> {
        let mut roots = BTreeSet::new();
        for update in &commit.context_updates {
            if !std::ptr::eq(self, update.access.journal.as_ref()) {
                return Err(StateError::StaleAccess.into());
            }
            validate_access(
                &state.plugin_state,
                &state.snapshot,
                &update.access,
                StateScope::Agent,
                true,
                None,
            )?;
            if !roots.insert(update.access.target)
                || state.plugin_state.reservations.contains_key(&(
                    Some(update.access.target),
                    context_address(CONTEXT_MANIFEST),
                ))
                || manifest_hash(&state.snapshot, update.access.target) != update.expected
            {
                return Err(StateError::Conflict.into());
            }
        }
        let mut addresses = BTreeSet::new();
        for change in commit.changes.iter().chain(
            commit
                .context_updates
                .iter()
                .flat_map(|update| &update.changes),
        ) {
            if let StateChange::Context { source, .. } = &change.change {
                let address = (change.agent, context_address(source.as_str()));
                if !addresses.insert(address.clone())
                    || state.plugin_state.reservations.contains_key(&address)
                {
                    return Err(StateError::Conflict.into());
                }
            }
        }
        for update in &commit.context_updates {
            let generation = commit
                .changes
                .iter()
                .rev()
                .find(|change| change.agent == Some(update.access.target))
                .map(|change| change.generation)
                .or_else(|| {
                    state
                        .snapshot
                        .state
                        .iter()
                        .rev()
                        .find(|record| record.agent == Some(update.access.target))
                        .map(|record| record.generation)
                })
                .unwrap_or_default();
            commit
                .changes
                .extend(update.changes.iter().cloned().map(|mut change| {
                    change.generation = generation;
                    change
                }));
        }
        Ok(())
    }

    fn resolve_state_edits(
        &self,
        state: &mut JournalState,
        commit: &mut JournalCommit,
    ) -> Result<(), JournalError> {
        self.resolve_context_updates(state, commit)?;
        let mut addresses = BTreeSet::new();
        for change in &commit.changes {
            if let StateChange::Plugin { key, .. } = &change.change {
                if state
                    .plugin_state
                    .reservations
                    .contains_key(&(change.agent, key.clone()))
                {
                    return Err(StateError::Conflict.into());
                }
                if !addresses.insert((change.agent, key.clone())) {
                    return Err(StateError::Conflict.into());
                }
                validate_record(change, 1)?;
            }
        }
        for edit in &commit.state_edits {
            if !std::ptr::eq(self, edit.access.journal.as_ref()) {
                return Err(StateError::StaleAccess.into());
            }
            validate_access(
                &state.plugin_state,
                &state.snapshot,
                &edit.access,
                edit.spec.scope,
                true,
                commit
                    .agents
                    .iter()
                    .rev()
                    .find(|record| record.id == edit.access.target),
            )?;
            let address = edit.address();
            if !addresses.insert(address.clone())
                || state.plugin_state.reservations.contains_key(&address)
            {
                return Err(StateError::Conflict.into());
            }
            let current = read_slot(&state.snapshot, address.0, &address.1, edit.spec.version);
            if edit
                .expect
                .is_some_and(|expected| expected != current.revision)
            {
                return Err(StateError::Conflict.into());
            }
            if current.version > edit.spec.version {
                return Err(StateError::NewerVersion {
                    stored: current.version,
                    registered: edit.spec.version,
                }
                .into());
            }
            let change = edit.change();
            validate_record(&change, 1)?;
            commit.changes.push(change);
        }
        if let Some((agent, generation)) = commit.boundary.take() {
            let history = commit
                .histories
                .get(&agent)
                .or_else(|| state.snapshot.histories.get(&agent))
                .ok_or_else(|| {
                    JournalError::Preparation("boundary requires an agent history".into())
                })?;
            let mut lens: BTreeMap<_, _> = state
                .snapshot
                .histories
                .iter()
                .map(|(id, history)| (*id, (history.epoch, history.messages.len())))
                .collect();
            lens.extend(
                commit
                    .histories
                    .iter()
                    .map(|(id, history)| (*id, (history.epoch, history.messages.len()))),
            );
            commit.changes.push(JournalChange {
                agent: Some(agent),
                generation,
                change: StateChange::Boundary {
                    offset: history.messages.len(),
                    lens,
                },
            });
        }
        Ok(())
    }

    fn state_addresses(commit: &JournalCommit) -> BTreeSet<(Option<AgentId>, String)> {
        commit
            .changes
            .iter()
            .filter_map(|change| match &change.change {
                StateChange::Plugin { key, .. } => Some((change.agent, key.clone())),
                StateChange::Context { source, .. } => {
                    Some((change.agent, context_address(source.as_str())))
                }
                _ => None,
            })
            .chain(commit.context_updates.iter().map(|update| {
                (
                    Some(update.access.target),
                    context_address(CONTEXT_MANIFEST),
                )
            }))
            .collect()
    }

    fn changed_history_epochs(
        snapshot: &SessionJournalSnapshot,
        commit: &JournalCommit,
    ) -> BTreeSet<AgentId> {
        let mut agents = commit.state_epoch_agents.clone();
        agents.extend(commit.histories.iter().filter_map(|(agent, history)| {
            snapshot
                .histories
                .get(agent)
                .filter(|before| {
                    before.epoch != history.epoch || before.compacted != history.compacted
                })
                .map(|_| *agent)
        }));
        agents
    }

    fn state_events(
        snapshot: &SessionJournalSnapshot,
        changes: &[JournalChange],
    ) -> Vec<StateChangeEvent> {
        changes
            .iter()
            .filter_map(|change| match &change.change {
                StateChange::Plugin { key, version, .. } => Some(StateChangeEvent {
                    key: key.clone(),
                    agent: change.agent,
                    revision: read_slot(snapshot, change.agent, key, *version).revision,
                    reason: StateChangeReason::Commit,
                }),
                _ => None,
            })
            .collect()
    }

    pub fn stage(
        &self,
        commit: JournalCommit,
        pending_change_count: usize,
    ) -> Result<JournalPending, JournalError> {
        let mut state = self.lock();
        Self::stage_locked(&mut state, commit, pending_change_count)
    }

    fn stage_locked(
        state: &mut JournalState,
        commit: JournalCommit,
        pending_change_count: usize,
    ) -> Result<JournalPending, JournalError> {
        if state.replacement.is_some() {
            return Err(JournalError::Preparation(REPLACEMENT_RESERVED.into()));
        }
        Self::validate_incarnations(state, &commit.incarnations)?;
        if !commit.state_edits.is_empty()
            || !commit.context_updates.is_empty()
            || commit.boundary.is_some()
        {
            return Err(JournalError::InvalidPending);
        }
        for address in Self::state_addresses(&commit) {
            if state.plugin_state.reservations.contains_key(&address) {
                return Err(StateError::Conflict.into());
            }
        }
        for change in &commit.changes {
            if let StateChange::Plugin { .. } = &change.change {
                validate_record(change, 1)?;
            }
        }
        if pending_change_count == 0 {
            return Err(JournalError::InvalidPending);
        }
        let history_epochs = Self::changed_history_epochs(&state.snapshot, &commit);
        let mut candidate = Self::apply_commit(&state.snapshot, commit)?;
        if candidate.revision == state.snapshot.revision {
            candidate.revision = candidate
                .revision
                .checked_add(1)
                .ok_or(JournalError::SequenceExhausted)?;
        }
        let first_seq = candidate
            .seq
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        candidate.seq = candidate
            .seq
            .checked_add(
                u64::try_from(pending_change_count).map_err(|_| JournalError::SequenceExhausted)?,
            )
            .ok_or(JournalError::SequenceExhausted)?;
        let pending = JournalPending {
            epoch: candidate.epoch,
            revision: candidate.revision,
            first_seq,
            count: pending_change_count,
        };
        for agent in history_epochs {
            state
                .plugin_state
                .agent_epochs
                .insert(agent, MakimaId::generate());
        }
        state.snapshot = candidate.clone();
        state.staged.push_back(StagedCommit {
            snapshot: candidate,
            pending: Some(pending.clone()),
        });
        Ok(pending)
    }

    pub fn complete(
        &self,
        pending: JournalPending,
        changes: Vec<JournalChange>,
    ) -> Result<(), JournalError> {
        let mut state = self.lock();
        Self::complete_locked(&mut state, pending, changes)?;
        Self::advance_ready(&mut state);
        self.conversion_changed.notify(usize::MAX);
        Ok(())
    }

    fn complete_locked(
        state: &mut JournalState,
        pending: JournalPending,
        changes: Vec<JournalChange>,
    ) -> Result<(), JournalError> {
        let index = state
            .staged
            .iter()
            .position(|entry| entry.pending.as_ref() == Some(&pending))
            .ok_or(JournalError::InvalidPending)?;
        if pending.epoch != state.snapshot.epoch || changes.len() != pending.count {
            return Err(JournalError::InvalidPending);
        }
        for change in &changes {
            let address = match &change.change {
                StateChange::Plugin { key, .. } => Some((change.agent, key.clone())),
                StateChange::Context { source, .. } => {
                    Some((change.agent, context_address(source)))
                }
                _ => None,
            };
            if address.as_ref().is_some_and(|address| {
                state
                    .plugin_state
                    .reservations
                    .get(address)
                    .is_some_and(|revision| *revision != pending.revision)
            }) {
                return Err(StateError::Conflict.into());
            }
            if matches!(&change.change, StateChange::Plugin { .. }) {
                validate_record(change, pending.first_seq)?;
            }
        }
        let records: Vec<_> = changes
            .into_iter()
            .enumerate()
            .map(|(offset, change)| StoredStateRecord {
                seq: pending.first_seq + offset as u64,
                agent: change.agent,
                generation: change.generation,
                change: change.change,
            })
            .collect();
        let install = |snapshot: &mut SessionJournalSnapshot| -> Result<(), JournalError> {
            snapshot.state.extend(records.iter().cloned());
            snapshot.state.sort_by_key(|record| record.seq);
            SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)?;
            Ok(())
        };
        let mut snapshots: Vec<_> = state
            .staged
            .iter()
            .skip(index)
            .map(|entry| entry.snapshot.clone())
            .collect();
        for snapshot in &mut snapshots {
            install(snapshot)?;
        }
        let mut logical = state.snapshot.clone();
        install(&mut logical)?;
        for (entry, snapshot) in state.staged.iter_mut().skip(index).zip(snapshots) {
            entry.snapshot = snapshot;
        }
        state.staged[index].pending = None;
        state.snapshot = logical;
        Ok(())
    }

    fn advance_ready(state: &mut JournalState) {
        while state
            .staged
            .front()
            .is_some_and(|entry| entry.pending.is_none())
        {
            if let Some(entry) = state.staged.pop_front() {
                state.ready = entry.snapshot;
            }
        }
    }

    fn apply_commit(
        snapshot: &SessionJournalSnapshot,
        commit: JournalCommit,
    ) -> Result<SessionJournalSnapshot, JournalError> {
        if commit.agents.is_empty()
            && commit.changes.is_empty()
            && commit.histories.is_empty()
            && commit.state_epoch_agents.is_empty()
            && commit.managed_histories.is_empty()
        {
            return Ok(snapshot.clone());
        }
        let mut candidate = snapshot.clone();
        candidate.revision = candidate
            .revision
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        for mut record in commit.agents {
            candidate.seq = candidate
                .seq
                .checked_add(1)
                .ok_or(JournalError::SequenceExhausted)?;
            record.created_seq = candidate
                .agent_revisions
                .iter()
                .rev()
                .find(|(_, saved)| saved.id == record.id)
                .map_or(candidate.seq, |(_, saved)| saved.created_seq);
            record.updated_seq = candidate.seq;
            candidate.agent_revisions.push((candidate.seq, record));
        }
        for change in commit.changes {
            candidate.seq = candidate
                .seq
                .checked_add(1)
                .ok_or(JournalError::SequenceExhausted)?;
            candidate.state.push(StoredStateRecord {
                seq: candidate.seq,
                agent: change.agent,
                generation: change.generation,
                change: change.change,
            });
        }
        candidate.histories.extend(commit.histories);
        candidate.managed_histories.extend(commit.managed_histories);
        SessionStateView::fold(&candidate.state, &candidate.agent_revisions, candidate.seq)?;
        Ok(candidate)
    }

    pub fn acknowledge(&self, snapshot: &SessionJournalSnapshot) -> Result<(), JournalError> {
        let mut state = self.lock();
        if snapshot.epoch != state.ready.epoch
            || snapshot.revision > state.ready.revision
            || state.aborted_preparations.contains(&snapshot.revision)
        {
            return Err(JournalError::StaleCheckpoint);
        }
        state.published_revision = state.published_revision.max(snapshot.revision);
        Ok(())
    }

    pub fn published_revision(&self) -> u64 {
        self.lock().published_revision
    }

    #[cfg(test)]
    pub(crate) fn try_lock_for_rewind_test(&self) -> bool {
        self.state.try_lock().is_ok()
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, JournalState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::{JournalChange, JournalCommit, SessionJournal, restored_preview_bytes};
    use crate::actor::{ChildDelivery, MAX_PENDING_DELIVERIES, TurnProvenance, TurnResult};
    use crate::tools::offload::OffloadStore;
    use crate::{HistorySnapshot, ToolOutput, TurnCancellationReason, TurnOutcome};
    use makima_providers::{Message, TokenUsage};
    use makima_storage::id::{AgentId, TurnId};
    use makima_storage::session_state::{
        MAX_INLINE_TURN_RESULT_BYTES, StateChange, StoredAgentConfig, StoredDelivery,
        StoredDeliveryOp, StoredModeSnapshot, StoredTurnOrigin,
    };
    use makima_storage::sessions::Session;
    use serde_json::json;
    use std::sync::Arc;
    use test_case::test_case;

    const FIRST: &str = "first";
    const SECOND: &str = "second";

    #[test_case(false; "cancelled_anchor_keeps_approval_unpublished")]
    #[test_case(true; "anchor_folds_resolved_approval_edit")]
    fn prepared_boundary_follows_state_edit_without_changing_history(install: bool) {
        use crate::state::{StateScope, StateSlotSpec};
        use makima_storage::session_state::{
            SessionStateView, StoredAgentLifecycle, StoredAgentRecord,
        };
        use serde_json::Value;
        smol::block_on(async {
            const SLOT: &str = "approval/plan";
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            let history = HistorySnapshot::new(vec![Message::user(FIRST.into())]);
            journal
                .commit(JournalCommit {
                    agents: vec![StoredAgentRecord {
                        id: agent,
                        parent: None,
                        depth: 0,
                        transcript_key: String::new(),
                        lifecycle: StoredAgentLifecycle::Open,
                        ancestry_unknown: false,
                        label: None,
                        spawned_by_tool_use_id: None,
                        spec: None,
                        restorable: Ok(()),
                        usage: Default::default(),
                        created_seq: 0,
                        updated_seq: 0,
                        legacy: false,
                    }],
                    histories: [(agent, history.clone())].into(),
                    ..Default::default()
                })
                .unwrap();
            let slot = journal
                .register_state::<Value>(StateSlotSpec {
                    key: SLOT.into(),
                    scope: StateScope::Session,
                    version: 1,
                })
                .unwrap();
            let access = journal.state_access(agent, None);
            let before = journal.snapshot();
            let edit = slot.edit(&access, Some(json!(SECOND)), None).unwrap();
            let mut prepared = journal
                .prepare(JournalCommit {
                    state_edits: vec![edit],
                    boundary: Some((agent, 0)),
                    ..Default::default()
                })
                .await
                .unwrap();
            let snapshot = prepared.snapshot();
            let boundary = snapshot.state.last().unwrap();
            let reserved_sequence = boundary.seq;
            assert!(
                matches!(&boundary.change, StateChange::Boundary { offset, lens } if *offset == history.messages.len() && lens[&agent] == (history.epoch, history.messages.len()))
            );
            assert!(matches!(
                snapshot.state[snapshot.state.len() - 2].change,
                StateChange::Plugin { .. }
            ));
            let view =
                SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, boundary.seq)
                    .unwrap();
            assert_eq!(view.scopes[&None].plugins[SLOT].value, Some(json!(SECOND)));
            assert_eq!(journal.snapshot().state, before.state);
            assert!(Arc::ptr_eq(
                &snapshot.histories[&agent].messages,
                &history.messages
            ));
            if install {
                prepared.install().unwrap();
            }
            drop(prepared);
            assert_eq!(
                slot.get(&access).unwrap().value,
                install.then(|| json!(SECOND))
            );
            if !install {
                let next = journal
                    .prepare(JournalCommit {
                        boundary: Some((agent, 0)),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                assert!(next.snapshot().state.last().unwrap().seq > reserved_sequence);
                drop(next);
            }
            assert!(Arc::ptr_eq(
                &journal.snapshot().histories[&agent].messages,
                &history.messages
            ));
        });
    }

    #[test]
    fn stale_incarnation_rejects_commit_and_prepared_install_without_blocking_prefix() {
        smol::block_on(async {
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            journal.set_incarnation(agent, 1);
            let old = journal.incarnation(agent);
            let mut token = journal
                .prepare(JournalCommit {
                    incarnations: vec![old.clone()],
                    changes: vec![admitted(agent)],
                    ..Default::default()
                })
                .await
                .unwrap();
            journal.set_incarnation(agent, 2);
            assert!(
                journal
                    .commit_for(
                        old,
                        JournalCommit {
                            changes: vec![admitted(agent)],
                            ..Default::default()
                        }
                    )
                    .is_err()
            );
            assert!(token.install().is_err());
            drop(token);
            journal
                .commit_for(
                    journal.incarnation(agent),
                    JournalCommit {
                        changes: vec![admitted(agent)],
                        ..Default::default()
                    },
                )
                .unwrap();
            assert!(journal.pending_revision().is_none());
            assert_eq!(journal.snapshot().state.len(), 1);
        });
    }

    #[test]
    fn pending_delivery_view_includes_unconverted_runtime_envelopes_and_respects_tombstones() {
        let journal = Arc::new(SessionJournal::default());
        let parent = AgentId::generate();
        let child = AgentId::generate();
        let turn = TurnId::generate();
        let delivery = Arc::new(ChildDelivery {
            label: None,
            result: TurnResult {
                outcome: TurnOutcome::cancelled(
                    child,
                    turn,
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text: FIRST.into(),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            },
        });
        journal
            .record_terminal(delivery.result.clone(), None)
            .unwrap();
        journal
            .queue_delivery(parent, Arc::clone(&delivery), FIRST.len())
            .unwrap();
        assert!(journal.snapshot().state.is_empty());
        let pending = journal.pending_deliveries(parent).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!((pending[0].0, pending[0].1), (child, turn));
        assert!(Arc::ptr_eq(pending[0].2.as_ref().unwrap(), &delivery));
        journal
            .commit(JournalCommit {
                changes: vec![JournalChange {
                    agent: Some(parent),
                    generation: 0,
                    change: StateChange::Delivery {
                        id: (child, turn),
                        op: StoredDeliveryOp::Withdrawn,
                    },
                }],
                ..Default::default()
            })
            .unwrap();
        assert!(journal.pending_deliveries(parent).unwrap().is_empty());
    }

    #[test_case(false; "stale_source")]
    #[test_case(true; "stale_recipient")]
    fn delivery_queue_rejects_stale_binding_before_reserving_state(recipient: bool) {
        let journal = Arc::new(SessionJournal::default());
        let parent = AgentId::generate();
        let child = AgentId::generate();
        let turn = TurnId::generate();
        let delivery = Arc::new(ChildDelivery {
            label: None,
            result: TurnResult {
                outcome: TurnOutcome::cancelled(
                    child,
                    turn,
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text: FIRST.into(),
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            },
        });
        let agent = if recipient { parent } else { child };
        journal.set_incarnation(agent, 1);
        let stale = journal.incarnation(agent);
        journal.set_incarnation(agent, 2);
        let before = journal.snapshot();
        assert!(matches!(
            journal.queue_delivery_for(parent, Arc::clone(&delivery), FIRST.len(), &[stale]),
            Err(crate::actor::DeliveryRejection::Journal(_))
        ));
        assert_eq!(journal.snapshot().revision, before.revision);
        assert!(journal.pending_revision().is_none());
        assert!(journal.lock().delivery_bytes.is_empty());
        assert!(
            journal
                .queue_delivery_for(parent, delivery, FIRST.len(), &[journal.incarnation(agent)])
                .unwrap()
        );
        assert!(journal.pending_revision().is_none());
        assert_eq!(journal.snapshot().state.len(), 1);
    }

    #[test]
    fn detached_conversion_cannot_poison_or_complete_replacement_result() {
        const CONVERSION_ERROR: &str = "detached conversion failed";
        let journal = Arc::new(SessionJournal::default());
        let agent = AgentId::generate();
        let turn = TurnId::generate();
        let result = TurnResult {
            outcome: TurnOutcome::cancelled(
                agent,
                turn,
                TokenUsage::default(),
                0,
                TurnCancellationReason::User,
            ),
            text: FIRST.into(),
            output: Vec::new(),
            provenance: TurnProvenance::default(),
            duration_ms: 0,
        };
        journal.set_incarnation(agent, 1);
        journal
            .record_terminal_for(Some(journal.incarnation(agent)), result.clone(), None)
            .unwrap();
        let expected = Arc::clone(&journal.lock().results[&(agent, turn)].conversion);
        let stored = result
            .to_stored(makima_storage::id::MakimaId::generate(), None)
            .unwrap();
        journal.set_incarnation(agent, 2);
        assert!(!journal.record_conversion_error(
            (agent, turn),
            &expected,
            CONVERSION_ERROR.into()
        ));
        assert!(journal.checkpoint_snapshot().is_ok());
        journal
            .record_terminal_for(Some(journal.incarnation(agent)), result, None)
            .unwrap();
        assert!(!journal.record_conversion_error(
            (agent, turn),
            &expected,
            CONVERSION_ERROR.into()
        ));
        journal
            .complete_conversion((agent, turn), &expected, stored)
            .unwrap();
        assert!(journal.lock().results.contains_key(&(agent, turn)));
        assert!(journal.persistence_error().is_none());
        journal.release_terminal(agent, turn);
        assert!(journal.pending_revision().is_none());
    }

    #[test_case(false; "reverse_input_order")]
    #[test_case(true; "stale_token_leaves_all_journals_uninstalled")]
    fn batch_install_validates_every_journal_before_mutation(stale: bool) {
        smol::block_on(async {
            let journals = [
                Arc::new(SessionJournal::default()),
                Arc::new(SessionJournal::default()),
            ];
            let agents = [AgentId::generate(), AgentId::generate()];
            let mut tokens = Vec::new();
            for (journal, agent) in journals.iter().zip(agents) {
                journal.set_incarnation(agent, 1);
                tokens.push(
                    journal
                        .prepare(JournalCommit {
                            incarnations: vec![journal.incarnation(agent)],
                            changes: vec![admitted(agent)],
                            ..Default::default()
                        })
                        .await
                        .unwrap(),
                );
            }
            if stale {
                journals[1].set_incarnation(agents[1], 2);
            }
            let mut references = tokens.iter_mut().rev().collect::<Vec<_>>();
            let result = super::JournalPrepared::install_batch(&mut references);
            assert_eq!(result.is_err(), stale);
            for journal in &journals {
                assert_eq!(journal.snapshot().state.len(), usize::from(!stale));
                assert_eq!(journal.published_revision() == 0, stale);
            }
            drop(references);
            assert!(tokens.iter().all(|token| token.pending.is_some() == stale));
            drop(tokens);
            assert!(
                journals
                    .iter()
                    .all(|journal| journal.pending_revision().is_none())
            );
        });
    }

    #[test]
    fn incarnation_replacement_waits_for_claimed_candidate_and_runtime_finalization() {
        smol::block_on(async {
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            journal.set_incarnation(agent, 1);
            let mut token = journal
                .prepare(JournalCommit {
                    incarnations: vec![journal.incarnation(agent)],
                    changes: vec![admitted(agent)],
                    ..Default::default()
                })
                .await
                .unwrap();
            let publication = makima_storage::checkpoint::CheckpointPublication::default();
            assert!(publication.try_publish());
            assert!(journal.try_publication_read().is_none());
            let mut detach = Box::pin(async {
                let _publication = journal.publication_read().await;
                journal.set_incarnation(agent, 2);
            });
            assert!(futures_lite::future::poll_once(&mut detach).await.is_none());
            token.install().unwrap();
            assert_eq!(journal.snapshot().state.len(), 1);
            assert_eq!(journal.incarnation(agent).reservation, 1);
            assert!(futures_lite::future::poll_once(&mut detach).await.is_none());
            drop(token);
            detach.await;
            assert_eq!(journal.incarnation(agent).reservation, 2);
            assert_eq!(journal.snapshot().state.len(), 1);
        });
    }

    #[test_case(false; "abort_releases_ordinary_publication")]
    #[test_case(true; "install_retains_lane_until_runtime_finalization")]
    fn prepared_publication_excludes_ordinary_capture_through_token_drop(install: bool) {
        smol::block_on(async {
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            let ordinary = journal.capture_checkpoint().await.unwrap();
            let before = ordinary.snapshot().revision;
            let mut reserve = Box::pin(journal.reserve_publication());
            assert!(
                futures_lite::future::poll_once(&mut reserve)
                    .await
                    .is_none()
            );
            assert_eq!(journal.snapshot().revision, before);
            drop(ordinary);
            let permit = reserve.await;
            assert!(journal.try_publication_read().is_none());
            let boundary = journal.boundary_read().await;
            let mut prepare = Box::pin(permit.prepare_with(|snapshot| {
                assert_eq!(snapshot.revision, before);
                JournalCommit {
                    changes: vec![admitted(agent)],
                    ..Default::default()
                }
            }));
            assert!(
                futures_lite::future::poll_once(&mut prepare)
                    .await
                    .is_none()
            );
            drop(boundary);
            let mut token = prepare.await.unwrap();
            let candidate = token.snapshot().clone();
            let mut capture = Box::pin(journal.capture_checkpoint());
            assert!(
                futures_lite::future::poll_once(&mut capture)
                    .await
                    .is_none()
            );
            if install {
                token.install().unwrap();
                assert!(
                    futures_lite::future::poll_once(&mut capture)
                        .await
                        .is_none()
                );
            }
            drop(token);
            let ordinary = capture.await.unwrap();
            assert_eq!(ordinary.snapshot().state.len(), usize::from(install));
            if install {
                assert_eq!(ordinary.snapshot().revision, candidate.revision);
            } else {
                assert_eq!(ordinary.snapshot().revision, before);
            }
        });
    }

    #[test]
    fn config_options_preserve_logical_values_and_deduplicate_full_config() {
        const MODEL: &str = "test/model";
        const OPTION: &str = "custom";
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        let config = StoredAgentConfig {
            cwd: ".".into(),
            model: MODEL.into(),
            thinking: None,
            fast: false,
            workflow: false,
            mode: StoredModeSnapshot {
                id: "build".into(),
                label: "Build".into(),
                system_prompt: None,
                restrict_write_to: None,
                tools: None,
            },
            options: [(OPTION.into(), FIRST.into())].into(),
        };
        journal
            .record_config(agent, 1, config.clone(), None)
            .unwrap();
        let pending = journal.stage(JournalCommit::default(), 1).unwrap();
        let mut changed = config.clone();
        changed.fast = true;
        changed.options.clear();
        let revision = journal
            .record_config(agent, 2, changed.clone(), None)
            .unwrap();
        assert_eq!(
            journal
                .record_config(agent, 3, changed.clone(), None)
                .unwrap(),
            revision
        );
        let options = [(OPTION.into(), SECOND.into())].into();
        journal
            .record_config(agent, 4, changed, Some(options))
            .unwrap();
        journal.complete(pending, vec![admitted(agent)]).unwrap();
        let snapshot = journal.snapshot();
        let configs: Vec<_> = snapshot
            .state
            .iter()
            .filter_map(|record| match &record.change {
                StateChange::Config(config) => Some(config),
                _ => None,
            })
            .collect();
        assert_eq!(configs.len(), 3);
        assert_eq!(configs[1].options[OPTION], FIRST);
        assert_eq!(configs[2].options[OPTION], SECOND);
    }

    #[test_case(false; "abort_preserves_sibling")]
    #[test_case(true; "install_preserves_sibling")]
    fn prepared_candidate_is_private_and_orders_sibling_commits(install: bool) {
        smol::block_on(async {
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            let before = journal.snapshot();
            let change = admitted(agent);
            let mut token = journal
                .prepare(JournalCommit {
                    changes: vec![change.clone()],
                    ..Default::default()
                })
                .await
                .unwrap();
            let candidate = token.snapshot().clone();
            assert_eq!(candidate.state.len(), 1);
            assert_eq!(journal.snapshot().revision, before.revision);
            let mut read = Box::pin(journal.boundary_read());
            assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            journal
                .commit(JournalCommit {
                    changes: vec![admitted(agent)],
                    ..Default::default()
                })
                .unwrap();
            assert!(journal.snapshot().state.is_empty());
            if install {
                token.install().unwrap();
                assert_eq!(journal.published_revision(), candidate.revision);
                assert_eq!(journal.snapshot().state.len(), 2);
                assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            }
            drop(token);
            drop(read.await);
            let ready = journal.snapshot();
            assert_eq!(ready.state.len(), if install { 2 } else { 1 });
            assert!(ready.seq > candidate.seq);
            if !install {
                assert!(ready.state.iter().all(|record| record.seq > candidate.seq));
                assert_eq!(journal.published_revision(), before.revision);
                assert!(journal.acknowledge(&candidate).is_err());
            }
            let before_history = journal.snapshot();
            let history_token = journal
                .prepare(JournalCommit {
                    histories: [(
                        agent,
                        HistorySnapshot::new(vec![Message::user(FIRST.into())]),
                    )]
                    .into(),
                    managed_histories: [agent].into(),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(history_token.snapshot().seq, before_history.seq);
            history_token.commit().unwrap();
            assert_eq!(journal.snapshot().histories[&agent].messages.len(), 1);
        });
    }

    #[test]
    fn checkpoint_snapshot_reports_conversion_error_without_hiding_readable_prefix() {
        const UNSAVED: &str = "offload unavailable";
        let journal = SessionJournal::default();
        let key = (AgentId::generate(), TurnId::generate());
        let before = journal.checkpoint_snapshot().unwrap();
        journal.lock().conversion_errors.insert(key, UNSAVED.into());
        assert_eq!(journal.checkpoint_snapshot().unwrap_err(), UNSAVED);
        assert_eq!(journal.snapshot().revision, before.revision);
        journal.lock().conversion_errors.remove(&key);
        assert_eq!(
            journal.checkpoint_snapshot().unwrap().revision,
            before.revision
        );
    }

    #[test_case(false; "offloaded_output_inline_text")]
    #[test_case(true; "offloaded_text_and_output")]
    fn restored_offloaded_deliveries_keep_exact_preview_capacity(offload_text: bool) {
        const MODEL: &str = "test/model";
        const CWD: &str = ".";
        const RELOADED_COUNT: usize = 17;
        let mut session = Session::<Message, TokenUsage, ToolOutput>::new(MODEL, CWD);
        let parent = AgentId::generate();
        session.bind_root_id(parent).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let store = OffloadStore::on_disk(directory.path().join("offload"));
        let text = if offload_text {
            FIRST.repeat(MAX_INLINE_TURN_RESULT_BYTES)
        } else {
            FIRST.into()
        };
        let delivery = |text: String, output| ChildDelivery {
            label: None,
            result: TurnResult {
                outcome: TurnOutcome::cancelled(
                    AgentId::generate(),
                    TurnId::generate(),
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text,
                output,
                provenance: TurnProvenance::default(),
                duration_ms: 0,
            },
        };
        let envelope = delivery(
            text,
            vec![json!(SECOND.repeat(MAX_INLINE_TURN_RESULT_BYTES))],
        );
        let preview_bytes = envelope.preview(None).input().preamble[0]
            .first_text_content()
            .unwrap()
            .len();
        let mut stored = StoredDelivery {
            label: None,
            result: envelope.result.to_stored(session.id, Some(&store)).unwrap(),
            preview_bytes: Some(preview_bytes),
            held: false,
        };
        assert_eq!(restored_preview_bytes(&stored), preview_bytes);
        let encoded = serde_json::to_value(&stored).unwrap();
        let decoded: StoredDelivery = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.preview_bytes, Some(preview_bytes));
        if !offload_text {
            let mut legacy = serde_json::to_value(&stored).unwrap();
            legacy.as_object_mut().unwrap().remove("preview_bytes");
            stored = serde_json::from_value(legacy).unwrap();
            assert_eq!(stored.preview_bytes, None);
            assert_eq!(restored_preview_bytes(&stored), preview_bytes);
        }
        let journal = SessionJournal::from_session(&session).unwrap();
        journal
            .commit(JournalCommit {
                changes: (0..RELOADED_COUNT)
                    .map(|_| {
                        let mut queued = stored.clone();
                        let child = AgentId::generate();
                        let turn = TurnId::generate();
                        queued.result.outcome.agent_id = child;
                        queued.result.outcome.turn_id = turn;
                        JournalChange {
                            agent: Some(parent),
                            generation: 0,
                            change: StateChange::Delivery {
                                id: (child, turn),
                                op: StoredDeliveryOp::Queued(Box::new(queued)),
                            },
                        }
                    })
                    .collect(),
                ..Default::default()
            })
            .unwrap();
        journal.snapshot().apply_to(&mut session).unwrap();
        session.save_to(directory.path()).unwrap();
        let loaded =
            Session::<Message, TokenUsage, ToolOutput>::load_from(session.id, directory.path())
                .unwrap();
        let restored = Arc::new(SessionJournal::from_session(&loaded).unwrap());
        for _ in RELOADED_COUNT..MAX_PENDING_DELIVERIES {
            let next = Arc::new(delivery(FIRST.into(), Vec::new()));
            let bytes = next.preview(None).input().preamble[0]
                .first_text_content()
                .unwrap()
                .len();
            assert!(restored.queue_delivery(parent, next, bytes).unwrap());
        }
        let next = Arc::new(delivery(FIRST.into(), Vec::new()));
        assert!(
            restored
                .queue_delivery(parent, next, preview_bytes)
                .is_err()
        );
    }

    fn admitted(agent: AgentId) -> JournalChange {
        JournalChange {
            agent: Some(agent),
            generation: 0,
            change: StateChange::Admitted {
                turn: TurnId::generate(),
                origin: StoredTurnOrigin::User,
            },
        }
    }

    #[test_case(false; "complete_in_order")]
    #[test_case(true; "complete_out_of_order")]
    fn pending_conversion_holds_exact_prefix(complete_later_first: bool) {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        journal
            .commit(JournalCommit {
                histories: [(agent, HistorySnapshot::new(Vec::new()))].into(),
                ..Default::default()
            })
            .unwrap();
        let before = journal.snapshot();
        let first = journal
            .stage(
                JournalCommit {
                    histories: [(
                        agent,
                        HistorySnapshot::new(vec![Message::user(FIRST.into())]),
                    )]
                    .into(),
                    ..Default::default()
                },
                1,
            )
            .unwrap();
        let second = journal
            .stage(
                JournalCommit {
                    histories: [(
                        agent,
                        HistorySnapshot::new(vec![Message::user(SECOND.into())]),
                    )]
                    .into(),
                    ..Default::default()
                },
                1,
            )
            .unwrap();
        journal
            .commit_with(|logical| {
                assert_eq!(
                    logical.histories[&agent].messages[0].user_text(),
                    Some(SECOND)
                );
                JournalCommit {
                    changes: vec![admitted(agent)],
                    ..Default::default()
                }
            })
            .unwrap();
        let second_change = admitted(agent);
        if complete_later_first {
            journal
                .complete(second.clone(), vec![second_change.clone()])
                .unwrap();
        }
        assert_eq!(journal.snapshot().revision, before.revision);
        assert!(journal.snapshot().histories[&agent].messages.is_empty());
        assert!(journal.pending_revision().is_some());
        journal
            .complete(first.clone(), vec![admitted(agent)])
            .unwrap();
        if !complete_later_first {
            let prefix = journal.snapshot();
            assert_eq!(
                prefix.histories[&agent].messages[0].user_text(),
                Some(FIRST)
            );
            assert_eq!(prefix.state.len(), 1);
            journal.complete(second, vec![second_change]).unwrap();
        }
        let ready = journal.snapshot();
        assert_eq!(ready.state.len(), 3);
        assert!(ready.state.windows(2).all(|pair| pair[0].seq < pair[1].seq));
        assert_eq!(
            ready.histories[&agent].messages[0].user_text(),
            Some(SECOND)
        );
        assert!(journal.pending_revision().is_none());
        assert!(journal.complete(first, vec![admitted(agent)]).is_err());
        journal.acknowledge(&before).unwrap();
        assert_eq!(journal.published_revision(), before.revision);
    }

    #[test]
    fn failed_pending_completion_keeps_prefix_retryable() {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        let pending = journal.stage(JournalCommit::default(), 1).unwrap();
        assert!(journal.complete(pending.clone(), Vec::new()).is_err());
        assert_eq!(journal.snapshot().seq, 0);
        assert!(journal.pending_revision().is_some());
        journal.complete(pending, vec![admitted(agent)]).unwrap();
        assert_eq!(journal.snapshot().state.len(), 1);
    }
}

#[cfg(any(test, feature = "test-util"))]
use crate::ManagerError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphMutation<A, S> {
    Agent(A),
    State(S),
}

#[derive(Debug, Clone)]
pub struct GraphCheckpoint<A, S> {
    epoch: MakimaId,
    records: Arc<[GraphMutation<A, S>]>,
}

impl<A, S> GraphCheckpoint<A, S> {
    pub fn new(epoch: MakimaId, records: Vec<GraphMutation<A, S>>) -> Self {
        Self {
            epoch,
            records: records.into(),
        }
    }

    pub fn epoch(&self) -> MakimaId {
        self.epoch
    }

    pub fn records(&self) -> &[GraphMutation<A, S>] {
        &self.records
    }

    pub fn revision(&self) -> usize {
        self.records.len()
    }
}

pub trait GraphStore: Send + Sync {
    type AgentRecord: Clone + Send + Sync;
    type StateRecord: Clone + Send + Sync;
    type Error: Error + Send + Sync + 'static;

    fn put_agent(&self, record: Self::AgentRecord) -> Result<usize, Self::Error>;
    fn append_state(&self, record: Self::StateRecord) -> Result<usize, Self::Error>;
    fn snapshot(
        &self,
    ) -> Result<GraphCheckpoint<Self::AgentRecord, Self::StateRecord>, Self::Error>;
    fn publish(
        &self,
        checkpoint: &GraphCheckpoint<Self::AgentRecord, Self::StateRecord>,
    ) -> Result<(), Self::Error>;
    fn load(&self) -> Result<GraphCheckpoint<Self::AgentRecord, Self::StateRecord>, Self::Error>;
}

#[cfg(any(test, feature = "test-util"))]
#[derive(Debug, Error)]
pub enum GraphRestartError<E: Error + 'static> {
    #[error("could not load the graph checkpoint: {0}")]
    Store(#[source] E),
    #[error("nonempty graph restoration requires dormant-node reconstruction")]
    NonemptyGraph,
    #[error(transparent)]
    Manager(#[from] ManagerError),
}

#[cfg(any(test, feature = "test-util"))]
pub use memory::{MemoryGraphStore, MemoryGraphStoreError};

#[cfg(any(test, feature = "test-util"))]
mod memory {
    use std::sync::{Mutex, MutexGuard};

    use super::{GraphCheckpoint, GraphMutation, GraphStore};
    use makima_storage::id::MakimaId;
    use thiserror::Error;

    #[derive(Debug, Error, PartialEq, Eq)]
    pub enum MemoryGraphStoreError {
        #[error("graph store mutex poisoned")]
        Poisoned,
        #[error("graph checkpoint belongs to a previous store incarnation")]
        StaleCheckpoint,
        #[error("graph checkpoint does not match the buffered prefix")]
        InvalidCheckpoint,
        #[error("injected graph publication failure")]
        PublicationFailed,
    }

    struct MemoryState<A, S> {
        epoch: MakimaId,
        records: Vec<GraphMutation<A, S>>,
        persisted: usize,
        fail_next_publication: bool,
    }

    pub struct MemoryGraphStore<A, S> {
        state: Mutex<MemoryState<A, S>>,
    }

    impl<A, S> Default for MemoryGraphStore<A, S> {
        fn default() -> Self {
            Self {
                state: Mutex::new(MemoryState {
                    epoch: MakimaId::generate(),
                    records: Vec::new(),
                    persisted: 0,
                    fail_next_publication: false,
                }),
            }
        }
    }

    impl<A, S> MemoryGraphStore<A, S> {
        fn lock(&self) -> Result<MutexGuard<'_, MemoryState<A, S>>, MemoryGraphStoreError> {
            self.state
                .lock()
                .map_err(|_| MemoryGraphStoreError::Poisoned)
        }

        pub fn fail_next_publication(&self) -> Result<(), MemoryGraphStoreError> {
            self.lock()?.fail_next_publication = true;
            Ok(())
        }

        pub fn restart(&self) -> Result<(), MemoryGraphStoreError> {
            let mut state = self.lock()?;
            let persisted = state.persisted;
            state.records.truncate(persisted);
            state.epoch = MakimaId::generate();
            state.fail_next_publication = false;
            Ok(())
        }
    }

    impl<A: Clone + PartialEq + Send + Sync, S: Clone + PartialEq + Send + Sync> GraphStore
        for MemoryGraphStore<A, S>
    {
        type AgentRecord = A;
        type StateRecord = S;
        type Error = MemoryGraphStoreError;

        fn put_agent(&self, record: A) -> Result<usize, Self::Error> {
            let mut state = self.lock()?;
            state.records.push(GraphMutation::Agent(record));
            Ok(state.records.len())
        }

        fn append_state(&self, record: S) -> Result<usize, Self::Error> {
            let mut state = self.lock()?;
            state.records.push(GraphMutation::State(record));
            Ok(state.records.len())
        }

        fn snapshot(&self) -> Result<GraphCheckpoint<A, S>, Self::Error> {
            let state = self.lock()?;
            Ok(GraphCheckpoint {
                epoch: state.epoch,
                records: state.records.clone().into(),
            })
        }

        fn publish(&self, checkpoint: &GraphCheckpoint<A, S>) -> Result<(), Self::Error> {
            let mut state = self.lock()?;
            if checkpoint.epoch != state.epoch {
                return Err(MemoryGraphStoreError::StaleCheckpoint);
            }
            if !state.records.starts_with(checkpoint.records()) {
                return Err(MemoryGraphStoreError::InvalidCheckpoint);
            }
            if checkpoint.revision() <= state.persisted {
                return Ok(());
            }
            if state.fail_next_publication {
                state.fail_next_publication = false;
                return Err(MemoryGraphStoreError::PublicationFailed);
            }
            state.persisted = checkpoint.revision();
            Ok(())
        }

        fn load(&self) -> Result<GraphCheckpoint<A, S>, Self::Error> {
            let state = self.lock()?;
            Ok(GraphCheckpoint {
                epoch: state.epoch,
                records: state.records[..state.persisted].to_vec().into(),
            })
        }
    }
}
