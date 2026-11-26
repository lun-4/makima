use makima_domain::LockExt;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::mem::take;
use std::slice::from_ref;
use std::sync::{
    Arc, Mutex, MutexGuard,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use async_lock::{RwLock, RwLockReadGuardArc, RwLockWriteGuardArc};
use event_listener::{Event, EventListener};
use imbl::{OrdMap, OrdSet};
use makima_providers::{Message, TokenUsage, add_cost};
use makima_storage::conversation::ConversationState;
use makima_storage::id::{AgentId, MakimaId, TurnId};
use makima_storage::now_epoch;
use makima_storage::session_state::{
    ParentLink, SessionStateReducer, SessionStateView, StateChange, StateFoldError,
    StoredAgentConfig, StoredAgentRecord, StoredDelivery, StoredDeliveryOp, StoredPayload,
    StoredPluginValue, StoredStateRecord, StoredTurnFailure, StoredTurnFailureKind,
    StoredTurnOutcome, StoredTurnResult, StoredTurnStatus,
};
use makima_storage::sessions::{
    DEFAULT_TITLE, Session, SessionMeta, SessionRecord, StoredPost, StoredRule, StoredTokenUsage,
    append_context_events, generate_title, normalize_title,
};
use thiserror::Error;

use crate::actor::{
    ChildDelivery, DeliveryRejection, MAX_DELIVERY_PREVIEW_BYTES, MAX_PENDING_DELIVERIES,
    MAX_PENDING_PREVIEW_BYTES, StoredTurnProgress, TurnResult, TurnResultStorageError,
};
use crate::context::{CONTEXT_MANIFEST, ContextUpdate, manifest_hash};
use crate::session_writer::SessionWriter;
use crate::state::{
    StateChangeEvent, StateChangeReason, StateEdit, StateError, StateRevision, StateRuntime,
    StateScope, read_slot, validate_access, validate_record,
};
use crate::tools::offload::OffloadStore;
use crate::{HistorySnapshot, Post, ToolOutput, TurnOutcome};

mod records;
pub use records::{JournalRecord, JournalRecords};

const CONVERSION_RETRY_DELAYS: [Duration; 2] =
    [Duration::from_millis(100), Duration::from_millis(200)];
const CONVERSION_FAILED: &str = "turn result could not be saved: ";

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

#[derive(Default)]
pub(crate) struct DeliveryInboxUsage {
    count: usize,
    bytes: usize,
}

impl DeliveryInboxUsage {
    pub(crate) fn from_sizes(sizes: impl IntoIterator<Item = usize>) -> Self {
        let mut usage = Self::default();
        for bytes in sizes {
            usage.add(bytes);
        }
        usage
    }

    fn add(&mut self, bytes: usize) {
        self.count += 1;
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub(crate) fn admit(&mut self, bytes: usize) -> Result<(), DeliveryRejection> {
        if bytes > MAX_DELIVERY_PREVIEW_BYTES
            || self.count >= MAX_PENDING_DELIVERIES
            || self.bytes.saturating_add(bytes) > MAX_PENDING_PREVIEW_BYTES
        {
            return Err(DeliveryRejection::Overflow);
        }
        self.add(bytes);
        Ok(())
    }
}

pub(crate) fn restored_preview_bytes(delivery: &StoredDelivery) -> usize {
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

pub(crate) type StoredSession = Session<Message, TokenUsage, ToolOutput>;
type JournalSessionRecord = SessionRecord<Message, TokenUsage, ToolOutput>;
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
    #[error("cannot publish spawn while an earlier journal change is pending")]
    SpawnPublicationPending,
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
    pub state_edits: Vec<StateEdit>,
    pub context_updates: Vec<ContextUpdate>,
    pub state_epoch_agents: BTreeSet<AgentId>,
    pub incarnations: Vec<JournalIncarnation>,
    pub agents: Vec<StoredAgentRecord>,
    pub changes: Vec<JournalChange>,
    pub histories: BTreeMap<AgentId, HistorySnapshot>,
    pub managed_histories: BTreeSet<AgentId>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionUsage {
    pub tokens: TokenUsage,
    pub cost: Option<f64>,
    pub by_model: HashMap<String, StoredTokenUsage>,
}

impl SessionUsage {
    fn from_session(session: &StoredSession) -> Self {
        let by_model = session.usage_by_model().clone();
        Self {
            tokens: session.token_usage,
            cost: by_model
                .values()
                .filter_map(|usage| usage.cost)
                .reduce(|a, b| a + b),
            by_model,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionJournalSnapshot {
    pub id: MakimaId,
    pub updated_at: u64,
    pub epoch: MakimaId,
    pub revision: u64,
    pub seq: u64,
    pub state: JournalRecords<StoredStateRecord>,
    pub agent_revisions: JournalRecords<(u64, StoredAgentRecord)>,
    pub histories: OrdMap<AgentId, HistorySnapshot>,
    pub managed_histories: OrdSet<AgentId>,
    pub authoritative: bool,
    pub tool_outputs: Arc<HashMap<String, Arc<ToolOutput>>>,
    pub posts: Arc<Vec<StoredPost>>,
    title: Option<Arc<str>>,
    cwd: Arc<str>,
    model: Arc<str>,
    parent: Option<ParentLink>,
    root: Option<AgentId>,
    rootless_history: Option<HistorySnapshot>,
    context_events: Arc<Vec<serde_json::Value>>,
    seed_meta: Arc<SessionMeta>,
    draft: Option<Arc<str>>,
    draft_changed: bool,
    usage: Arc<SessionUsage>,
    reducer: Option<ReducerCache>,
    prepared_installed: Option<Arc<AtomicBool>>,
}

#[derive(Debug, Clone)]
struct ReducerCache {
    state: JournalRecords<StoredStateRecord>,
    agents: JournalRecords<(u64, StoredAgentRecord)>,
    reduced: SessionStateReducer,
}

impl SessionJournalSnapshot {
    pub fn title(&self) -> &str {
        self.title.as_deref().unwrap_or(DEFAULT_TITLE)
    }

    pub fn cwd(&self) -> &str {
        &self.cwd
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn parent(&self) -> Option<&ParentLink> {
        self.parent.as_ref()
    }

    pub fn root_agent_id(&self) -> Option<AgentId> {
        self.root
    }

    pub fn root_history(&self) -> Option<&HistorySnapshot> {
        self.root
            .and_then(|root| self.histories.get(&root))
            .or(self.rootless_history.as_ref())
    }

    pub fn context_events(&self) -> &[serde_json::Value] {
        &self.context_events
    }

    pub fn seed_meta(&self) -> &SessionMeta {
        &self.seed_meta
    }

    pub fn draft(&self) -> Option<&str> {
        self.draft.as_deref()
    }

    fn update_draft(&mut self, draft: Option<&str>) {
        self.draft = draft.map(Arc::from);
        self.draft_changed = true;
    }

    pub fn usage(&self) -> &SessionUsage {
        &self.usage
    }

    fn seed_session_fields(&mut self, session: &StoredSession) {
        self.id = session.id;
        self.updated_at = session.updated_at;
        self.title = (!session.title_is_default()).then(|| Arc::from(session.title.as_str()));
        self.cwd = Arc::from(session.cwd.as_str());
        self.model = Arc::from(session.model.as_str());
        self.parent = session.parent().cloned();
        self.root = session.root_agent_id();
        self.rootless_history = self.root.is_none().then(|| session.history_snapshot());
        self.context_events = Arc::new(session.context_events().to_vec());
        self.seed_meta = Arc::new(session.meta.clone());
        self.draft = session.meta.input_draft.as_deref().map(Arc::from);
        self.draft_changed = false;
        self.usage = Arc::new(SessionUsage::from_session(session));
        self.tool_outputs = Arc::new(session.tool_outputs().clone());
        self.posts = Arc::new(session.posts().to_vec());
        self.refresh_root_header();
    }

    fn refresh_root_header(&mut self) {
        let Some(root) = self.root else { return };
        if let Some(config) = self
            .state
            .iter()
            .rev()
            .find_map(|record| match &record.change {
                StateChange::Config(config) if record.agent == Some(root) => Some(config),
                _ => None,
            })
        {
            self.cwd = Arc::from(config.cwd.as_str());
            self.model = Arc::from(config.model.as_str());
        }
    }

    fn set_root_header(&mut self, config: &StoredAgentConfig) {
        self.cwd = Arc::from(config.cwd.as_str());
        self.model = Arc::from(config.model.as_str());
    }

    fn reduced(&self) -> Result<SessionStateReducer, StateFoldError> {
        if let Some(cache) = &self.reducer
            && self.state.same_root(&cache.state)
            && self.agent_revisions.same_root(&cache.agents)
        {
            return Ok(cache.reduced.clone());
        }
        SessionStateReducer::from_ordered(&self.state.to_vec(), &self.agent_revisions.to_vec())
    }

    fn cache_reducer(&mut self, reduced: SessionStateReducer) {
        self.reducer = Some(ReducerCache {
            state: self.state.clone(),
            agents: self.agent_revisions.clone(),
            reduced,
        });
    }

    pub fn fold(&self) -> Result<SessionStateView, StateFoldError> {
        Ok(self.reduced()?.materialize())
    }

    pub(crate) fn plugin(&self, agent: Option<AgentId>, key: &str) -> Option<StoredPluginValue> {
        if let Some(cache) = &self.reducer
            && self.state.same_root(&cache.state)
            && self.agent_revisions.same_root(&cache.agents)
        {
            return cache.reduced.plugin(agent, key).cloned();
        }
        self.state
            .iter()
            .rev()
            .find_map(|record| match &record.change {
                StateChange::Plugin {
                    key: saved,
                    version,
                    value,
                } if record.agent == agent && saved == key => Some(StoredPluginValue {
                    revision: record.seq,
                    version: *version,
                    value: value.clone(),
                }),
                _ => None,
            })
    }

    pub(crate) fn latest_agent(&self, agent: AgentId) -> Option<StoredAgentRecord> {
        if let Some(cache) = &self.reducer
            && self.state.same_root(&cache.state)
            && self.agent_revisions.same_root(&cache.agents)
        {
            return cache
                .reduced
                .agents
                .get(&agent)
                .map(|record| record.as_ref().clone());
        }
        self.agent_revisions
            .iter()
            .rev()
            .find(|(_, record)| record.id == agent)
            .map(|(_, record)| record.clone())
    }

    fn empty() -> Self {
        let epoch = MakimaId::generate();
        Self {
            id: epoch,
            updated_at: now_epoch(),
            epoch,
            revision: 0,
            seq: 0,
            state: JournalRecords::default(),
            agent_revisions: JournalRecords::default(),
            histories: OrdMap::new(),
            managed_histories: OrdSet::new(),
            authoritative: false,
            tool_outputs: Arc::default(),
            posts: Arc::default(),
            title: None,
            cwd: Arc::default(),
            model: Arc::default(),
            parent: None,
            root: None,
            rootless_history: None,
            context_events: Arc::default(),
            seed_meta: Arc::default(),
            draft: None,
            draft_changed: false,
            usage: Arc::default(),
            reducer: None,
            prepared_installed: None,
        }
    }

    pub fn apply_to(&self, session: &mut StoredSession) -> Result<(), JournalError> {
        if self.authoritative {
            self.fold()?;
            self.apply_session_fields(session);
            session.replace_state(self.state.to_vec());
            session.replace_agent_revisions(self.agent_revisions.to_vec());
            session.replace_journal_histories(
                &self
                    .histories
                    .iter()
                    .map(|(agent, history)| (*agent, history.clone()))
                    .collect(),
            );
            return Ok(());
        }
        SessionStateReducer::validate_structure(session.state(), session.agent_revisions())?;
        let mut state: BTreeMap<_, _> = session
            .state()
            .iter()
            .cloned()
            .map(|record| (record.seq, record))
            .collect();
        let mut agents: BTreeMap<_, _> = session.agent_revisions().iter().cloned().collect();
        for record in self.state.iter() {
            if agents.contains_key(&record.seq)
                || state.get(&record.seq).is_some_and(|saved| saved != record)
            {
                return Err(JournalError::Diverged(record.seq));
            }
            state.insert(record.seq, record.clone());
        }
        for (seq, record) in self.agent_revisions.iter() {
            if state.contains_key(seq) || agents.get(seq).is_some_and(|saved| saved != record) {
                return Err(JournalError::Diverged(*seq));
            }
            agents.insert(*seq, record.clone());
        }
        let state: Vec<_> = state.into_values().collect();
        let agents: Vec<_> = agents.into_iter().collect();
        let view = SessionStateView::fold(&state, &agents)?;
        self.apply_session_fields(session);
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

    fn apply_session_fields(&self, session: &mut StoredSession) {
        session.replace_tool_outputs(self.tool_outputs.as_ref().clone());
        session.replace_posts(self.posts.as_ref().clone());
        if session.token_usage != self.usage.tokens
            || session.usage_by_model() != &self.usage.by_model
        {
            session.token_usage = self.usage.tokens;
            session
                .usage_by_model_mut()
                .clone_from(&self.usage.by_model);
        }
        if let Some(title) = &self.title {
            session.set_title(title.to_string());
        }
        if self.draft_changed {
            session.meta.input_draft = self.draft().map(str::to_owned);
        }
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
    histories: BTreeSet<AgentId>,
}

#[derive(Clone)]
struct EmittedHistory {
    epoch: u64,
    len: usize,
    compacted: bool,
    conversation: Option<ConversationState>,
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
    pending: JournalPending,
    terminal_generation: Option<u64>,
    delivery: Option<(AgentId, Option<String>, usize)>,
    released: bool,
    outcome: Option<TurnOutcome>,
    incarnation: Option<JournalIncarnation>,
}

#[derive(Clone)]
pub(crate) struct JournalState {
    pub(crate) snapshot: SessionJournalSnapshot,
    pub(crate) plugin_state: StateRuntime,
    ready: Arc<SessionJournalSnapshot>,
    staged: VecDeque<StagedCommit>,
    published_revision: u64,
    session_id: Option<MakimaId>,
    results: BTreeMap<(AgentId, TurnId), PendingResult>,
    delivery_bytes: BTreeMap<(AgentId, AgentId, TurnId), usize>,
    delivery_envelopes: BTreeMap<(AgentId, AgentId, TurnId), (u64, Arc<ChildDelivery>)>,
    checkpoint_error: Option<String>,
    writer_error: Option<String>,
    conversion_errors: BTreeMap<(AgentId, TurnId), String>,
    last_failed_conversion: Option<((AgentId, TurnId), String)>,
    incarnations: BTreeMap<AgentId, u64>,
    writer: Option<SessionWriter>,
    unwritten: Vec<JournalSessionRecord>,
    emitted_histories: BTreeMap<AgentId, EmittedHistory>,
    captured_unwritten: usize,
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
    fork: Option<(AgentId, serde_json::Value)>,
    _boundary: RwLockWriteGuardArc<()>,
    _publication: RwLockWriteGuardArc<()>,
}

impl JournalPrepared {
    pub fn commit(mut self) -> Result<u64, JournalError> {
        self.install()
    }

    pub(crate) fn install_fork(
        mut self,
        agent: AgentId,
        event: serde_json::Value,
    ) -> Result<u64, JournalError> {
        self.fork = Some((agent, event));
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
            SessionJournal::emit_unwritten(guard);
        }
        for token in tokens.iter_mut() {
            if token.pending.take().is_none() {
                continue;
            }
            if let Some(installed) = &token.candidate.prepared_installed {
                installed.store(true, Ordering::Release);
            }
        }
        drop(guards);
        for token in tokens {
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
                    .get(*agent)
                    .is_none_or(|before| {
                        before.epoch != history.epoch
                            || before.compacted != history.compacted
                            || before.conversation != history.conversation
                            || !Arc::ptr_eq(&before.messages, &history.messages)
                    })
            })
            .map(|(agent, history)| (*agent, history.clone()))
            .collect();
        state.staged[index]
            .histories
            .extend(history_changes.keys().copied());
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
        if let Some((agent, event)) = &self.fork {
            let history = &self.commit.histories[agent];
            let snapshot = Arc::make_mut(&mut state.ready);
            Arc::make_mut(&mut snapshot.context_events).push(event.clone());
            if let Some(draft) = event["input_draft"].as_str() {
                snapshot.update_draft(Some(draft).filter(|draft| !draft.is_empty()));
            }
            state.staged[index].histories.remove(agent);
            state.emitted_histories.insert(
                *agent,
                EmittedHistory {
                    epoch: history.epoch,
                    len: history.messages.len(),
                    compacted: history.compacted,
                    conversation: history.conversation.clone(),
                },
            );
        }
        SessionJournal::collect_ready(state);
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
        SessionJournal::advance_ready(&mut state);
        self.journal.conversion_changed.notify(usize::MAX);
    }
}

#[must_use]
pub struct TerminalGuard {
    journal: Arc<SessionJournal>,
    key: (AgentId, TurnId),
    revision: u64,
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        self.journal.release_terminal(self.key, self.revision);
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
                ready: Arc::new(snapshot),
                staged: VecDeque::new(),
                published_revision: 0,
                session_id: None,
                results: BTreeMap::new(),
                delivery_bytes: BTreeMap::new(),
                delivery_envelopes: BTreeMap::new(),
                checkpoint_error: None,
                writer_error: None,
                conversion_errors: BTreeMap::new(),
                last_failed_conversion: None,
                incarnations: BTreeMap::new(),
                writer: None,
                unwritten: Vec::new(),
                emitted_histories: BTreeMap::new(),
                captured_unwritten: 0,
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

    #[cfg(test)]
    pub(crate) fn into_nonpersistent(mut self) -> Self {
        self.persistent = false;
        self
    }

    pub fn is_persistent(&self) -> bool {
        self.persistent
    }

    pub fn from_session(session: &StoredSession) -> Result<Self, JournalError> {
        let journal = Self::default();
        {
            let mut state = journal.lock();
            let snapshot = &mut state.snapshot;
            snapshot.state =
                JournalRecords::try_from_ordered(session.state().iter().cloned(), "state")?;
            snapshot.agent_revisions = JournalRecords::try_from_ordered(
                session.agent_revisions().iter().cloned(),
                "agent",
            )?;
            snapshot.cache_reducer(snapshot.reduced()?);
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
            snapshot.seed_session_fields(session);
            state.emitted_histories = Self::history_baseline(&state.snapshot);
            state.ready = Arc::new(state.snapshot.clone());
            state.session_id = Some(session.id);
        }
        Ok(journal)
    }

    pub(crate) fn from_exact_snapshot(
        session: &StoredSession,
        mut snapshot: SessionJournalSnapshot,
    ) -> Result<Self, JournalError> {
        let view = snapshot.fold()?;
        if !session.state().iter().eq(snapshot.state.iter())
            || !session
                .agent_revisions()
                .iter()
                .eq(snapshot.agent_revisions.iter())
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
            snapshot.seed_session_fields(session);
            state.emitted_histories = Self::history_baseline(&snapshot);
            state.snapshot = snapshot.clone();
            state.ready = Arc::new(snapshot);
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
                state
                    .staged
                    .retain(|entry| entry.pending.as_ref() != Some(&result.pending));
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
        Self::set_session_id(&mut state, session);
        Ok(())
    }

    pub(crate) fn take_unwritten(&self) -> Vec<JournalSessionRecord> {
        let mut state = self.lock();
        Self::emit_unwritten(&mut state);
        state.captured_unwritten = 0;
        take(&mut state.unwritten)
    }

    pub fn bind_writer(&self, writer: SessionWriter) -> Result<(), JournalError> {
        let mut state = self.lock();
        if state.session_id.is_some_and(|id| id != writer.session_id()) {
            return Err(JournalError::StaleCheckpoint);
        }
        Self::set_session_id(&mut state, writer.session_id());
        state.writer.get_or_insert(writer);
        Self::emit_unwritten(&mut state);
        Ok(())
    }

    #[doc(hidden)]
    pub fn replace_writer_for_test(&self, writer: SessionWriter) {
        self.lock().writer = Some(writer);
    }

    fn set_session_id(state: &mut JournalState, id: MakimaId) {
        if state.session_id.is_none() {
            state.session_id = Some(id);
            state.snapshot.id = id;
            Arc::make_mut(&mut state.ready).id = id;
        }
    }

    pub fn has_writer(&self) -> bool {
        self.lock().writer.is_some()
    }

    pub fn flush_spawn(&self, target: u64) -> Result<(), String> {
        let (writer, target) = {
            let mut state = self.lock();
            let Some(writer) = state.writer.clone() else {
                return Ok(());
            };
            if state.ready.revision < target {
                return Err(JournalError::SpawnPublicationPending.to_string());
            }
            Self::emit_unwritten(&mut state);
            (writer, state.ready.revision)
        };
        smol::block_on(writer.append(Vec::new())).map_err(|error| error.to_string())?;
        let mut state = self.lock();
        state.published_revision = state.published_revision.max(target);
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), String> {
        if !self.persistent {
            return Ok(());
        }
        let target = self.lock().snapshot.revision;
        loop {
            let listener = self.conversion_changed.listen();
            let ready = {
                let mut state = self.lock();
                if let Some(error) = state.conversion_errors.values().next() {
                    return Err(error.clone());
                }
                if state
                    .staged
                    .iter()
                    .all(|entry| entry.snapshot.revision > target)
                {
                    Self::emit_unwritten(&mut state);
                    Some(
                        state
                            .writer
                            .as_ref()
                            .map(|writer| writer.append(Vec::new())),
                    )
                } else {
                    None
                }
            };
            let Some(receipt) = ready else {
                listener.await;
                continue;
            };
            let receipt = receipt.ok_or_else(|| "session journal has no writer".to_owned())?;
            receipt.await.map_err(|error| error.to_string())?;
            let mut state = self.lock();
            state.published_revision = state.published_revision.max(target);
            return Ok(());
        }
    }

    fn history_baseline(snapshot: &SessionJournalSnapshot) -> BTreeMap<AgentId, EmittedHistory> {
        snapshot
            .histories
            .iter()
            .map(|(agent, history)| {
                (
                    *agent,
                    EmittedHistory {
                        epoch: history.epoch,
                        len: history.messages.len(),
                        compacted: history.compacted,
                        conversation: history.conversation.clone(),
                    },
                )
            })
            .collect()
    }

    pub fn context_events(&self) -> Vec<serde_json::Value> {
        self.projection().context_events().to_vec()
    }

    pub(crate) fn writer(&self) -> Option<SessionWriter> {
        self.lock().writer.clone()
    }

    pub fn session_id(&self) -> Option<MakimaId> {
        self.lock().session_id
    }

    pub fn title(&self) -> String {
        self.projection().title().to_owned()
    }

    pub fn set_title(&self, title: String) {
        let title = normalize_title(&title);
        let mut state = self.lock();
        if state.ready.title.as_deref() != Some(&title) {
            let snapshot = Arc::make_mut(&mut state.ready);
            snapshot.title = Some(Arc::from(title.as_str()));
            snapshot.updated_at = now_epoch();
            state.unwritten.push(SessionRecord::Title { title });
            Self::emit_unwritten(&mut state);
        }
    }

    pub fn set_draft(&self, draft: Option<String>) {
        let draft = draft.filter(|text| !text.is_empty());
        let mut state = self.lock();
        if state.ready.draft() == draft.as_deref() {
            return;
        }
        let snapshot = Arc::make_mut(&mut state.ready);
        snapshot.update_draft(draft.as_deref());
        snapshot.updated_at = now_epoch();
        if self.persistent {
            state.unwritten.push(SessionRecord::Draft { text: draft });
            Self::emit_unwritten(&mut state);
        }
    }

    pub fn usage(&self) -> SessionUsage {
        self.lock().ready.usage.as_ref().clone()
    }

    pub fn cost(&self) -> Option<f64> {
        self.lock().ready.usage.cost
    }

    pub(crate) fn record_usage(&self, model: &str, tokens: TokenUsage, cost: Option<f64>) {
        let mut state = self.lock();
        let snapshot = Arc::make_mut(&mut state.ready);
        snapshot.updated_at = now_epoch();
        let usage = Arc::make_mut(&mut snapshot.usage);
        usage.tokens += tokens;
        add_cost(&mut usage.cost, cost);
        *usage.by_model.entry(model.to_owned()).or_default() += tokens.billed(cost);
        if self.persistent {
            let record = SessionRecord::Usage {
                token_usage: usage.tokens,
                usage_by_model: usage.by_model.clone(),
            };
            state.unwritten.push(record);
            Self::emit_unwritten(&mut state);
        }
    }

    pub(crate) fn record_tool_output(&self, id: &str, output: &ToolOutput) {
        let mut state = self.lock();
        if state.ready.tool_outputs.contains_key(id) {
            return;
        }
        let snapshot = Arc::make_mut(&mut state.ready);
        snapshot.updated_at = now_epoch();
        Arc::make_mut(&mut snapshot.tool_outputs).insert(id.to_owned(), Arc::new(output.clone()));
        if self.persistent {
            state.unwritten.push(SessionRecord::Out {
                id: id.to_owned(),
                d: output.clone(),
            });
            Self::emit_unwritten(&mut state);
        }
    }

    pub fn record_post(&self, target: AgentId, post: Post) {
        let mut state = self.lock();
        let agent = state
            .ready
            .latest_agent(target)
            .and_then(|record| record.parent.map(|_| target));
        let after_message = state
            .ready
            .histories
            .get(&target)
            .and_then(|history| history.ids.last().copied());
        let stored = StoredPost {
            agent,
            after_message,
            post,
        };
        let snapshot = Arc::make_mut(&mut state.ready);
        snapshot.updated_at = now_epoch();
        Arc::make_mut(&mut snapshot.posts).push(stored.clone());
        if self.persistent {
            state.unwritten.push(SessionRecord::Post { d: stored });
            Self::emit_unwritten(&mut state);
        }
    }

    pub fn projection(&self) -> Arc<SessionJournalSnapshot> {
        Arc::clone(&self.lock().ready)
    }

    pub fn snapshot(&self) -> SessionJournalSnapshot {
        self.projection().as_ref().clone()
    }

    pub(crate) fn admitted_input_turns(&self, agent: AgentId) -> Result<Vec<TurnId>, JournalError> {
        let snapshot = self.lock().snapshot.clone();
        let view = snapshot.fold()?;
        Ok(view
            .scopes
            .get(&Some(agent))
            .map_or_else(Vec::new, |scope| {
                scope
                    .admitted
                    .keys()
                    .filter(|turn| scope.turn_inputs.contains_key(turn))
                    .copied()
                    .collect()
            }))
    }

    pub fn checkpoint_snapshot(&self) -> Result<SessionJournalSnapshot, String> {
        let state = self.lock();
        if let Some(error) = state.conversion_errors.values().next() {
            return Err(error.clone());
        }
        Ok(state.ready.as_ref().clone())
    }

    pub fn persistence_error(&self) -> Option<String> {
        let state = self.lock();
        state
            .conversion_errors
            .values()
            .next()
            .cloned()
            .or_else(|| {
                state
                    .writer
                    .as_ref()
                    .and_then(|writer| writer.status().error.map(|error| error.to_string()))
            })
            .or_else(|| state.writer_error.clone())
            .or_else(|| state.checkpoint_error.clone())
            .or_else(|| {
                state
                    .last_failed_conversion
                    .as_ref()
                    .map(|(_, error)| error.clone())
            })
    }

    pub(crate) fn set_writer_error(&self, error: Option<String>) {
        self.lock().writer_error = error;
    }

    pub fn set_checkpoint_error(&self, error: Option<String>) {
        self.lock().checkpoint_error = error;
    }

    pub(crate) fn listen_for_terminal_conversion(&self) -> EventListener {
        self.conversion_changed.listen()
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
                if let Some((key, error)) = &state.last_failed_conversion
                    && *key == (agent, turn)
                    && !state.results.contains_key(key)
                {
                    return Err(error.clone());
                }
                if !state.results.contains_key(&(agent, turn))
                    && let Some(result) =
                        state
                            .ready
                            .state
                            .iter()
                            .rev()
                            .find_map(|record| match &record.change {
                                StateChange::Turn {
                                    turn: saved,
                                    result,
                                } if *saved == turn && record.agent == Some(agent) => Some(result),
                                _ => None,
                            })
                {
                    return Self::failed_conversion(result)
                        .map_or(Ok(()), |error| Err(error.into()));
                }
                if let Some(error) = state.conversion_errors.values().next() {
                    return Err(error.clone());
                }
            }
            listener.await;
        }
    }

    pub fn transcript_key_in_use(&self, key: &str, except: Option<AgentId>) -> bool {
        self.lock()
            .snapshot
            .agent_revisions
            .iter()
            .any(|(_, record)| Some(record.id) != except && record.transcript_key == key)
    }

    pub fn agent(&self, id: AgentId) -> Option<StoredAgentRecord> {
        self.lock().snapshot.latest_agent(id)
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
        if Self::failed_conversion(&result).is_some() {
            return Ok(());
        }
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
            histories: BTreeSet::new(),
        });
        Self::advance_ready(&mut state);
        Ok(())
    }

    pub fn record_terminal(
        self: &Arc<Self>,
        result: TurnResult,
        store: Option<Arc<OffloadStore>>,
    ) -> Result<Option<TerminalGuard>, JournalError> {
        self.record_terminal_for(None, result, store)
    }

    pub fn record_terminal_for(
        self: &Arc<Self>,
        incarnation: Option<JournalIncarnation>,
        result: TurnResult,
        store: Option<Arc<OffloadStore>>,
    ) -> Result<Option<TerminalGuard>, JournalError> {
        self.stage_terminal(incarnation, result, store, 0)
    }

    pub(crate) fn stage_terminal(
        self: &Arc<Self>,
        incarnation: Option<JournalIncarnation>,
        result: TurnResult,
        store: Option<Arc<OffloadStore>>,
        generation: u64,
    ) -> Result<Option<TerminalGuard>, JournalError> {
        if !self.persistent {
            return Ok(None);
        }
        let key = (result.outcome.agent_id(), result.outcome.turn_id());
        let mut state = self.lock();
        Self::validate_incarnations(&state, &incarnation.clone().into_iter().collect::<Vec<_>>())?;
        if state.results.contains_key(&key) {
            return Ok(None);
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
            2,
        )?;
        let revision = pending.revision;
        state.results.insert(
            key,
            PendingResult {
                conversion: Arc::new(Mutex::new(RuntimeResult {
                    result,
                    progress: StoredTurnProgress::default(),
                    stored: None,
                    store,
                })),
                pending,
                terminal_generation: Some(generation),
                delivery: None,
                released: false,
                outcome: None,
                incarnation,
            },
        );
        Ok(Some(TerminalGuard {
            journal: Arc::clone(self),
            key,
            revision,
        }))
    }

    fn release_terminal(self: &Arc<Self>, key: (AgentId, TurnId), revision: u64) {
        let mut state = self.lock();
        let Some(result) = state.results.get_mut(&key) else {
            return;
        };
        if result.released || result.pending.revision != revision {
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
        let reduced = state
            .snapshot
            .reduced()
            .map_err(|error| error.to_string())?;
        let mut deliveries = Vec::new();
        for (id, op, sequence) in reduced.deliveries(parent) {
            if !matches!(op, StoredDeliveryOp::Queued(_)) {
                continue;
            }
            let envelope = state
                .delivery_envelopes
                .get(&(parent, id.0, id.1))
                .map(|(_, delivery)| Arc::clone(delivery));
            deliveries.push((sequence, id.0, id.1, envelope));
        }
        for ((target, child, turn), (sequence, delivery)) in &state.delivery_envelopes {
            if *target == parent && reduced.delivery(parent, (*child, *turn)).is_none() {
                deliveries.push((*sequence, *child, *turn, Some(Arc::clone(delivery))));
            }
        }
        deliveries.sort_by_key(|(sequence, _, _, _)| *sequence);
        Ok(deliveries
            .into_iter()
            .map(|(_, child, turn, delivery)| (child, turn, delivery))
            .collect())
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
        let reduced = state
            .snapshot
            .reduced()
            .map_err(|_| DeliveryRejection::ParentUnavailable)?;
        if state.delivery_bytes.contains_key(&(parent, child, turn))
            || reduced.delivery(parent, key).is_some()
        {
            return Ok(false);
        }
        let mut sizes: BTreeMap<_, _> = state
            .delivery_bytes
            .iter()
            .filter(|((owner, _, _), _)| *owner == parent)
            .map(|(key, size)| (*key, *size))
            .collect();
        for ((child, turn), op, _) in reduced.deliveries(parent) {
            if let StoredDeliveryOp::Queued(delivery) = op {
                sizes
                    .entry((parent, child, turn))
                    .or_insert_with(|| restored_preview_bytes(delivery));
            }
        }
        DeliveryInboxUsage::from_sizes(sizes.into_iter().filter_map(|((_, source, id), size)| {
            matches!(
                reduced.delivery(parent, (source, id)),
                None | Some(StoredDeliveryOp::Queued(_))
            )
            .then_some(size)
        }))
        .admit(preview_bytes)?;
        let target = (parent, delivery.label.clone(), preview_bytes);
        let (spawn, sequence) = if let Some(result) = state.results.get_mut(&key) {
            if result.delivery.is_some() {
                return Err(DeliveryRejection::ParentUnavailable);
            }
            result.delivery = Some(target);
            (false, result.pending.first_seq + 1)
        } else {
            let stored = reduced.result(child, turn).cloned();
            let pending = Self::stage_locked(&mut state, JournalCommit::default(), 1)
                .map_err(|_| DeliveryRejection::ParentUnavailable)?;
            let sequence = pending.first_seq;
            state.results.insert(
                key,
                PendingResult {
                    conversion: Arc::new(Mutex::new(RuntimeResult {
                        result: delivery.result.clone(),
                        progress: StoredTurnProgress::default(),
                        stored,
                        store: None,
                    })),
                    pending,
                    terminal_generation: None,
                    delivery: Some(target),
                    released: true,
                    outcome: None,
                    incarnation: None,
                },
            );
            (true, sequence)
        };
        state
            .delivery_bytes
            .insert((parent, child, turn), preview_bytes);
        state
            .delivery_envelopes
            .insert((parent, child, turn), (sequence, delivery));
        drop(state);
        if spawn {
            self.spawn_conversion(key);
        }
        Ok(true)
    }

    fn failed_conversion(result: &StoredTurnResult) -> Option<&str> {
        match &result.outcome.status {
            StoredTurnStatus::Failed { failure }
                if failure.diagnostic.starts_with(CONVERSION_FAILED) =>
            {
                Some(&failure.diagnostic)
            }
            _ => None,
        }
    }

    fn spawn_conversion(self: &Arc<Self>, key: (AgentId, TurnId)) {
        let Some(conversion) = self
            .lock()
            .results
            .get(&key)
            .map(|result| Arc::clone(&result.conversion))
        else {
            return;
        };
        let inline = {
            let mut runtime = conversion.lock_unpoisoned();
            if runtime.stored.is_none() {
                runtime.stored = runtime.result.to_stored(MakimaId::generate(), None).ok();
            }
            runtime.stored.clone()
        };
        if let Some(stored) = inline
            && self.complete_conversion(key, &conversion, stored).is_ok()
        {
            return;
        }

        let journal = Arc::clone(self);
        smol::spawn(async move {
            for retry in CONVERSION_RETRY_DELAYS.into_iter().map(Some).chain([None]) {
                let (session, inline_session) = {
                    let state = journal.lock();
                    if state.results.get(&key).is_none_or(|result| {
                        !Arc::ptr_eq(&result.conversion, &conversion)
                    }) {
                        return;
                    }
                    (state.session_id, state.snapshot.epoch)
                };
                let expected = Arc::clone(&conversion);
                let converted = smol::unblock(move || {
                    let mut runtime = expected.lock_unpoisoned();
                    if let Some(stored) = &runtime.stored {
                        return Ok(stored.clone());
                    }
                    let RuntimeResult { result, progress, store, .. } = &mut *runtime;
                    let stored = result.to_stored_with_progress(
                        progress, session.unwrap_or(inline_session), session.and(store.as_deref()),
                    )?;
                    runtime.stored = Some(stored.clone());
                    Ok::<_, TurnResultStorageError>(stored)
                }).await;
                let error = match converted {
                    Ok(stored) => match journal.complete_conversion(key, &conversion, stored) {
                        Ok(()) => return,
                        Err(error) => error.to_string(),
                    },
                    Err(error) => error.to_string(),
                };
                if let Some(delay) = retry {
                    if !journal.record_conversion_error(key, &conversion, error.clone()) {
                        return;
                    }
                    tracing::warn!(%error, agent_id = %key.0, turn_id = %key.1, "turn result is unsaved; persistence will retry");
                    smol::Timer::after(delay).await;
                } else {
                    journal.fail_conversion(key, &conversion, error);
                }
            }
        }).detach();
    }

    fn fail_conversion(
        &self,
        key: (AgentId, TurnId),
        expected: &Arc<Mutex<RuntimeResult>>,
        error: String,
    ) {
        let (mut outcome, provenance, duration_ms) = {
            let runtime = expected.lock_unpoisoned();
            (
                StoredTurnOutcome::from(runtime.result.outcome.clone()),
                runtime.result.provenance.clone().into(),
                runtime.result.duration_ms,
            )
        };
        let diagnostic = format!("{CONVERSION_FAILED}{error}");
        outcome.status = StoredTurnStatus::Failed {
            failure: StoredTurnFailure {
                kind: StoredTurnFailureKind::Internal,
                diagnostic: diagnostic.clone(),
                user_message: diagnostic.clone(),
                retryable: false,
            },
        };
        // A terminal record clears the admission on replay even though the payload was lost.
        let stored = StoredTurnResult {
            outcome,
            text: StoredPayload::Inline(String::new()),
            output: StoredPayload::Inline(Vec::new()),
            provenance,
            duration_ms,
        };
        let mut state = self.lock();
        if state
            .results
            .get(&key)
            .is_none_or(|result| !Arc::ptr_eq(&result.conversion, expected))
        {
            return;
        }
        tracing::error!(%error, agent_id = %key.0, turn_id = %key.1, "turn result persistence retries exhausted");
        state.last_failed_conversion = Some((key, diagnostic));
        if let Err(error) = self.complete_conversion_locked(&mut state, key, stored) {
            tracing::error!(%error, agent_id = %key.0, turn_id = %key.1, "failed to install terminal failure record");
            if let Some(result) = state.results.remove(&key)
                && let Some(entry) = state
                    .staged
                    .iter_mut()
                    .find(|entry| entry.pending.as_ref() == Some(&result.pending))
            {
                entry.pending = None;
            }
            state
                .delivery_envelopes
                .retain(|(_, child, turn), _| (*child, *turn) != key);
            state
                .delivery_bytes
                .retain(|(_, child, turn), _| (*child, *turn) != key);
            state.conversion_errors.remove(&key);
            Self::advance_ready(&mut state);
            self.conversion_changed.notify(usize::MAX);
        }
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
        stored: StoredTurnResult,
    ) -> Result<(), JournalError> {
        let mut state = self.lock();
        let Some(result) = state.results.get(&key) else {
            return Ok(());
        };
        if !Arc::ptr_eq(&result.conversion, expected) {
            return Ok(());
        }
        self.complete_conversion_locked(&mut state, key, stored)
    }

    fn complete_conversion_locked(
        &self,
        state: &mut JournalState,
        key: (AgentId, TurnId),
        mut stored: StoredTurnResult,
    ) -> Result<(), JournalError> {
        let result = &state.results[&key];
        if let Some(binding) = &result.incarnation {
            Self::validate_incarnations(state, from_ref(binding))?;
        }
        let failed = Self::failed_conversion(&stored).is_some();
        if !failed && let Some(outcome) = &result.outcome {
            stored.outcome = outcome.clone().into();
        }
        let mut changes = Vec::with_capacity(result.pending.count);
        if let Some(generation) = result.terminal_generation {
            changes.push(JournalChange {
                agent: Some(key.0),
                generation,
                change: StateChange::Turn {
                    turn: key.1,
                    result: stored.clone(),
                },
            });
        }
        if let Some((parent, label, preview_bytes)) = &result.delivery {
            changes.push(JournalChange {
                agent: Some(*parent),
                generation: 0,
                change: StateChange::Delivery {
                    id: key,
                    op: StoredDeliveryOp::Queued(Box::new(StoredDelivery {
                        label: label.clone(),
                        preview_bytes: Some(*preview_bytes),
                        held: false,
                        result: stored,
                    })),
                },
            });
        }
        if let Err(error) = Self::complete_locked(state, result.pending.clone(), changes) {
            tracing::warn!(%error, agent_id = %key.0, turn_id = %key.1, "terminal journal conversion could not commit");
            state.conversion_errors.insert(key, error.to_string());
            self.conversion_changed.notify(usize::MAX);
            return Err(error);
        }
        if failed {
            state
                .delivery_envelopes
                .retain(|(_, child, turn), _| (*child, *turn) != key);
        }
        state.results.remove(&key);
        state.conversion_errors.remove(&key);
        Self::advance_ready(state);
        self.conversion_changed.notify(usize::MAX);
        Ok(())
    }

    pub fn try_publication_read(&self) -> Option<RwLockReadGuardArc<()>> {
        self.publication.try_read_arc()
    }

    pub async fn publication_read(&self) -> RwLockReadGuardArc<()> {
        self.publication.read_arc().await
    }

    pub async fn boundary_read(&self) -> RwLockReadGuardArc<()> {
        self.boundaries.read_arc().await
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
                    let builder = build.take().ok_or(JournalError::InvalidPending)?;
                    let mut commit = builder(&state.snapshot);
                    if commit.changes.is_empty()
                        && commit.state_edits.is_empty()
                        && commit.context_updates.is_empty()
                        && commit.histories.is_empty()
                        && commit.state_epoch_agents.is_empty()
                        && commit.managed_histories.is_empty()
                    {
                        return Err(JournalError::InvalidPending);
                    }
                    if !commit.agents.is_empty() {
                        return Err(JournalError::PreparedAgents);
                    }
                    Self::validate_incarnations(&state, &commit.incarnations)?;
                    self.resolve_state_edits(&mut state, &mut commit)?;
                    let mut candidate = Self::apply_commit(&state.snapshot, commit.clone())?;
                    candidate.tool_outputs = Arc::clone(&state.ready.tool_outputs);
                    candidate.posts = Arc::clone(&state.ready.posts);
                    candidate.prepared_installed = Some(Arc::new(AtomicBool::new(false)));
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
                            histories: BTreeSet::new(),
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
                        fork: None,
                        _boundary: boundary,
                        _publication: publication,
                    });
                }
            }
            listener.await;
        }
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
        self.commit_with_receipts(false, build, apply)
            .map(|(revision, _)| revision)
    }

    pub(crate) fn commit_spawn(&self, commit: JournalCommit) -> Result<u64, JournalError> {
        self.commit_with_receipts(true, |_| commit, || ())
            .map(|(revision, _)| revision)
    }

    pub(crate) fn commit_state_edit(&self, edit: StateEdit) -> Result<StateRevision, JournalError> {
        let (_, events) = self.commit_with_receipts(
            false,
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
        spawn: bool,
        build: impl FnOnce(&SessionJournalSnapshot) -> JournalCommit,
        apply: impl FnOnce(),
    ) -> Result<(u64, Vec<StateChangeEvent>), JournalError> {
        let mut state = self.lock();
        if spawn && state.writer.is_some() && !state.staged.is_empty() {
            return Err(JournalError::SpawnPublicationPending);
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
        let histories = commit.histories.keys().copied().collect();
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
            histories,
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
        Self::validate_incarnations(state, &commit.incarnations)?;
        if !commit.state_edits.is_empty() || !commit.context_updates.is_empty() {
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
        let histories = commit.histories.keys().copied().collect();
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
            histories,
        });
        Ok(pending)
    }

    pub fn complete(
        &self,
        pending: JournalPending,
        changes: Vec<JournalChange>,
    ) -> Result<(), JournalError> {
        if changes.len() != pending.count {
            return Err(JournalError::InvalidPending);
        }
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
        if pending.epoch != state.snapshot.epoch || changes.len() > pending.count {
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
        let mut reduced = state.staged[index].snapshot.reduced()?;
        for record in &records {
            reduced.apply_record(record)?;
        }
        let mut through = state.staged[index].snapshot.seq;
        let mut snapshots = Vec::with_capacity(state.staged.len() - index);
        for entry in state.staged.iter().skip(index) {
            let mut snapshot = entry.snapshot.clone();
            let mut suffix: Vec<_> = snapshot
                .state
                .after(through)
                .map(|record| (record.seq, Some(record), None))
                .chain(
                    snapshot
                        .agent_revisions
                        .after(through)
                        .map(|(seq, agent)| (*seq, None, Some(agent))),
                )
                .collect();
            suffix.sort_by_key(|(seq, _, _)| *seq);
            for (seq, record, agent) in suffix {
                if let Some(record) = record {
                    reduced.apply_record(record)?;
                }
                if let Some(agent) = agent {
                    reduced.apply_agent(seq, agent)?;
                }
            }
            through = snapshot.seq;
            snapshot.state.extend(records.iter().cloned());
            snapshot.cache_reducer(reduced.clone());
            snapshots.push(snapshot);
        }
        let mut logical = state.snapshot.clone();
        logical.state.extend(records.iter().cloned());
        logical.cache_reducer(reduced);
        for (entry, snapshot) in state.staged.iter_mut().skip(index).zip(snapshots) {
            entry.snapshot = snapshot;
        }
        state.staged[index].pending = None;
        state.snapshot = logical;
        Ok(())
    }

    fn advance_ready(state: &mut JournalState) {
        Self::collect_ready(state);
        Self::emit_unwritten(state);
    }

    fn emit_unwritten(state: &mut JournalState) {
        for record in &state.unwritten[state.captured_unwritten.min(state.unwritten.len())..] {
            let records = match record {
                SessionRecord::Group { records } => records.as_slice(),
                _ => from_ref(record),
            };
            for record in records {
                if matches!(
                    record,
                    SessionRecord::Msg { .. }
                        | SessionRecord::Compaction { .. }
                        | SessionRecord::History { .. }
                ) && let Ok(event) = serde_json::to_value(record)
                {
                    let snapshot = Arc::make_mut(&mut state.ready);
                    append_context_events(Arc::make_mut(&mut snapshot.context_events), event);
                }
            }
        }
        state.captured_unwritten = state.unwritten.len();
        if let Some(writer) = &state.writer
            && !state.unwritten.is_empty()
        {
            drop(writer.append(take(&mut state.unwritten)));
            state.captured_unwritten = 0;
        }
    }

    fn collect_ready(state: &mut JournalState) {
        while state
            .staged
            .front()
            .is_some_and(|entry| entry.pending.is_none())
        {
            if let Some(mut entry) = state.staged.pop_front() {
                entry.snapshot.id = state.ready.id;
                entry.snapshot.updated_at = now_epoch();
                entry.snapshot.draft.clone_from(&state.ready.draft);
                entry.snapshot.draft_changed = state.ready.draft_changed;
                entry.snapshot.context_events = Arc::clone(&state.ready.context_events);
                entry.snapshot.title = state.ready.title.clone();
                entry.snapshot.cwd = Arc::clone(&state.ready.cwd);
                entry.snapshot.model = Arc::clone(&state.ready.model);
                entry.snapshot.root = state.ready.root;
                entry.snapshot.usage = Arc::clone(&state.ready.usage);
                entry.snapshot.tool_outputs = Arc::clone(&state.ready.tool_outputs);
                entry.snapshot.posts = Arc::clone(&state.ready.posts);
                let mut records: Vec<_> = entry
                    .snapshot
                    .state
                    .after(state.ready.seq)
                    .map(|record| (record.seq, SessionRecord::State { d: record.clone() }))
                    .chain(entry.snapshot.agent_revisions.after(state.ready.seq).map(
                        |(seq, agent)| {
                            (
                                *seq,
                                SessionRecord::Agent {
                                    seq: *seq,
                                    d: agent.clone(),
                                },
                            )
                        },
                    ))
                    .collect();
                records.sort_by_key(|(seq, _)| *seq);
                if let Some(root) = records.iter().rev().find_map(|(_, record)| match record {
                    SessionRecord::Agent { d, .. } if d.parent.is_none() => Some(d.id),
                    _ => None,
                }) && Some(root) != entry.snapshot.root
                {
                    entry.snapshot.root = Some(root);
                    entry.snapshot.refresh_root_header();
                } else if let Some(config) =
                    records.iter().rev().find_map(|(_, record)| match record {
                        SessionRecord::State { d }
                            if d.agent.is_some() && d.agent == entry.snapshot.root =>
                        {
                            match &d.change {
                                StateChange::Config(config) => Some(config),
                                _ => None,
                            }
                        }
                        _ => None,
                    })
                {
                    entry.snapshot.set_root_header(config);
                }
                let mut records: Vec<_> = records.into_iter().map(|(_, record)| record).collect();
                for agent in entry.histories {
                    let Some(history) = entry.snapshot.histories.get(&agent) else {
                        continue;
                    };
                    let previous = state.emitted_histories.get(&agent);
                    let appended = previous.is_none_or(|previous| {
                        previous.epoch == history.epoch && previous.len <= history.messages.len()
                    });
                    if appended {
                        let start = previous.map_or(0, |previous| previous.len);
                        let messages = history.ids.iter().zip(history.messages.iter()).skip(start);
                        records.extend(messages.map(|(id, message)| SessionRecord::Msg {
                            id: *id,
                            agent,
                            d: message.clone(),
                        }));
                    } else {
                        records.push(SessionRecord::Compaction {
                            agent,
                            messages: history.messages.as_ref().clone(),
                            ids: history.ids.as_ref().clone(),
                            covered: history.covered,
                        });
                    }
                    if previous.is_none_or(|previous| {
                        previous.compacted != history.compacted
                            || previous.conversation != history.conversation
                    }) {
                        records.push(SessionRecord::History {
                            agent,
                            compacted: history.compacted,
                            conversation: history.conversation.clone(),
                        });
                    }
                    if entry.snapshot.title.is_none()
                        && entry
                            .snapshot
                            .latest_agent(agent)
                            .is_some_and(|record| record.parent.is_none())
                        && let Some(title) = generate_title(&history.messages)
                    {
                        entry.snapshot.title = Some(Arc::from(title.as_str()));
                        records.push(SessionRecord::Title { title });
                    }
                    state.emitted_histories.insert(
                        agent,
                        EmittedHistory {
                            epoch: history.epoch,
                            len: history.messages.len(),
                            compacted: history.compacted,
                            conversation: history.conversation.clone(),
                        },
                    );
                }
                state.unwritten.extend(SessionRecord::group(records));
                state.ready = Arc::new(entry.snapshot);
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
        let mut reduced = snapshot.reduced()?;
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
            record.created_seq = reduced
                .agents
                .get(&record.id)
                .map_or(candidate.seq, |saved| saved.created_seq);
            record.updated_seq = candidate.seq;
            reduced.apply_agent(candidate.seq, &record)?;
            candidate.agent_revisions.push((candidate.seq, record));
        }
        for change in commit.changes {
            candidate.seq = candidate
                .seq
                .checked_add(1)
                .ok_or(JournalError::SequenceExhausted)?;
            let record = StoredStateRecord {
                seq: candidate.seq,
                agent: change.agent,
                generation: change.generation,
                change: change.change,
            };
            reduced.apply_record(&record)?;
            candidate.state.push(record);
        }
        candidate.histories.extend(commit.histories);
        candidate.managed_histories.extend(commit.managed_histories);
        candidate.cache_reducer(reduced);
        Ok(candidate)
    }

    pub fn acknowledge(&self, snapshot: &SessionJournalSnapshot) -> Result<(), JournalError> {
        let mut state = self.lock();
        if snapshot.epoch != state.ready.epoch
            || snapshot.revision > state.ready.revision
            || snapshot
                .prepared_installed
                .as_ref()
                .is_some_and(|installed| !installed.load(Ordering::Acquire))
        {
            return Err(JournalError::StaleCheckpoint);
        }
        state.published_revision = state.published_revision.max(snapshot.revision);
        Ok(())
    }

    pub fn published_revision(&self) -> u64 {
        self.lock().published_revision
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, JournalState> {
        self.state.lock_unpoisoned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        JournalChange, JournalCommit, JournalError, SessionJournal, SessionJournalSnapshot,
        restored_preview_bytes,
    };
    use crate::actor::{
        ChildDelivery, DeliveryRejection, MAX_PENDING_DELIVERIES, TurnProvenance, TurnResult,
    };
    use crate::session_writer::{Record, SessionWriter, StoredSession};
    use crate::tools::offload::OffloadStore;
    use crate::{HistorySnapshot, Post, ToolOutput, TurnCancellationReason, TurnOutcome};
    use makima_providers::{Message, TokenUsage};
    use makima_storage::id::{AgentId, MakimaId, TurnId};
    use makima_storage::session_state::{
        MAX_INLINE_TURN_RESULT_BYTES, ParentLink, StateChange, StoredAgentConfig, StoredDelivery,
        StoredDeliveryOp, StoredModeSnapshot, StoredPayload, StoredTurnOrigin,
    };
    use makima_storage::sessions::{DEFAULT_TITLE, Session, context_entries};
    use makima_storage::test_util::test_record;
    use serde_json::json;
    use std::fs::read_to_string;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::sync::Arc;

    use futures_lite::future::poll_once;
    use test_case::test_case;

    use async_lock::RwLockReadGuardArc;
    use std::collections::BTreeMap;

    const CHILD_POST: &str = "child update";

    #[test_case(false; "retained_draft")]
    #[test_case(true; "cleared_draft")]
    fn live_projection_matches_log_replay(clear_draft: bool) {
        const MODEL: &str = "test/changed";
        const CWD: &str = "/changed";
        const OUTPUT: &str = "tool_result";
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let root = AgentId::generate();
            let mut stored = StoredSession::new("test/seed", "/seed");
            stored.bind_root_id(root).unwrap();
            stored.set_parent(Some(ParentLink {
                session: MakimaId::generate(),
                message: None,
            }));
            let writer = SessionWriter::open_session(dir.path(), &stored).unwrap();
            let journal = SessionJournal::from_session(&stored).unwrap();
            journal.bind_writer(writer).unwrap();
            let before = journal.projection();
            journal.set_draft(Some(FIRST.into()));
            let typed = journal.projection();
            assert!(before.draft().is_none());
            journal.set_draft(Some(FIRST.into()));
            assert!(Arc::ptr_eq(&typed, &journal.projection()));
            let mut history = before.root_history().unwrap().clone();
            history.messages = Arc::new(vec![Message::user(SECOND.into())]);
            history.ids = Arc::new(vec![MakimaId::generate()]);
            let preamble = MakimaId::generate();
            journal
                .commit(JournalCommit {
                    histories: [(root, history)].into(),
                    changes: [
                        StateChange::Config(StoredAgentConfig {
                            cwd: CWD.into(),
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
                            options: BTreeMap::new(),
                        }),
                        StateChange::InputHold { held: true },
                        StateChange::Preamble {
                            id: preamble,
                            message: OUTPUT.into(),
                        },
                    ]
                    .into_iter()
                    .map(|change| JournalChange {
                        agent: Some(root),
                        generation: 1,
                        change,
                    })
                    .collect(),
                    ..Default::default()
                })
                .unwrap();
            journal.set_title(SECOND.into());
            journal.record_usage(
                MODEL,
                TokenUsage {
                    input: 2,
                    ..Default::default()
                },
                Some(0.5),
            );
            journal.record_tool_output(OUTPUT, &ToolOutput::TodoList(Vec::new()));
            journal.record_post(
                root,
                Post {
                    text: CHILD_POST.into(),
                    level: Default::default(),
                    agent: None,
                },
            );
            if clear_draft {
                journal.set_draft(None);
            }
            let live = journal.projection();
            assert_eq!(live.draft(), if clear_draft { None } else { Some(FIRST) });
            journal.flush().await.unwrap();
            let loaded = StoredSession::load_from(stored.id, dir.path()).unwrap();
            let replayed = SessionJournal::from_session(&loaded).unwrap().projection();
            assert_eq!(live.id, replayed.id);
            assert_eq!(live.parent(), replayed.parent());
            assert_eq!(live.root_agent_id(), replayed.root_agent_id());
            assert_eq!(live.title(), replayed.title());
            assert_eq!(live.cwd(), CWD);
            assert_eq!(live.cwd(), replayed.cwd());
            assert_eq!(live.model(), MODEL);
            assert_eq!(live.model(), replayed.model());
            let view = live.fold().unwrap();
            assert!(view.scopes[&Some(root)].input_held);
            assert_eq!(
                view.scopes[&Some(root)].preambles.as_slice(),
                &[(preamble, OUTPUT.into())]
            );
            assert_eq!(view, replayed.fold().unwrap());
            assert_eq!(
                context_entries(live.context_events(), root).unwrap(),
                context_entries(replayed.context_events(), root).unwrap()
            );
            assert_eq!(
                live.root_history().unwrap().ids,
                replayed.root_history().unwrap().ids
            );
            assert_eq!(
                live.root_history().unwrap().messages,
                replayed.root_history().unwrap().messages
            );
            assert_eq!(live.draft(), replayed.draft());
            assert_eq!(live.usage(), replayed.usage());
            assert_eq!(live.posts, replayed.posts);
            assert_eq!(
                serde_json::to_value(&*live.tool_outputs).unwrap(),
                serde_json::to_value(&*replayed.tool_outputs).unwrap()
            );
        });
    }

    #[test]
    fn child_post_uses_its_last_committed_message() {
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let mut session = StoredSession::new("test", "/test");
            let root = AgentId::generate();
            let child = AgentId::generate();
            session.bind_root_id(root).unwrap();
            let writer = SessionWriter::open_session(dir.path(), &session).unwrap();
            let journal = SessionJournal::from_session(&session).unwrap();
            journal.bind_writer(writer).unwrap();
            let history = HistorySnapshot::new(vec![Message::user(CHILD_POST.into())]);
            let child_message = history.ids[0];
            journal
                .commit(JournalCommit {
                    agents: vec![test_record(child, Some(root))],
                    histories: [(child, history)].into(),
                    ..Default::default()
                })
                .unwrap();
            journal.record_post(
                child,
                Post {
                    text: CHILD_POST.into(),
                    level: Default::default(),
                    agent: None,
                },
            );
            journal.flush().await.unwrap();
            let reloaded = StoredSession::load_from(session.id, dir.path()).unwrap();
            journal.snapshot().apply_to(&mut session).unwrap();
            assert_eq!(session.posts(), reloaded.posts());
            let post = &reloaded.posts()[0];
            assert_eq!(post.agent, Some(child));
            assert_eq!(post.after_message, Some(child_message));
        });
    }

    #[test_case(None; "derived_once_from_root")]
    #[test_case(Some(DEFAULT_TITLE); "explicit_default_survives_reopen")]
    fn title_is_written_once(explicit: Option<&str>) {
        const CHILD_TEXT: &str = "child request";
        const ROOT_TEXT: &str = "root request";
        const LATER_TEXT: &str = "later root request";
        smol::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let mut session = StoredSession::new("test", "/test");
            let root = AgentId::generate();
            let child = AgentId::generate();
            session.bind_root_id(root).unwrap();
            let writer = SessionWriter::open_session(dir.path(), &session).unwrap();
            let journal = SessionJournal::from_session(&session).unwrap();
            journal.bind_writer(writer.clone()).unwrap();
            journal
                .commit(JournalCommit {
                    agents: vec![test_record(child, Some(root))],
                    histories: [(
                        child,
                        HistorySnapshot::new(vec![Message::user(CHILD_TEXT.into())]),
                    )]
                    .into(),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(journal.title(), DEFAULT_TITLE);
            let journal = if let Some(title) = explicit {
                journal.set_title(title.into());
                journal.flush().await.unwrap();
                let session = StoredSession::load_from(session.id, dir.path()).unwrap();
                let journal = SessionJournal::from_session(&session).unwrap();
                journal.bind_writer(writer).unwrap();
                journal
            } else {
                journal
            };
            for text in [ROOT_TEXT, LATER_TEXT] {
                journal
                    .commit(JournalCommit {
                        histories: [(root, HistorySnapshot::new(vec![Message::user(text.into())]))]
                            .into(),
                        ..Default::default()
                    })
                    .unwrap();
            }
            journal.flush().await.unwrap();
            let log = read_to_string(dir.path().join(format!("{}.jsonl", session.id))).unwrap();
            let titles: Vec<_> = log
                .lines()
                .map(|line| serde_json::from_str::<Record>(line).unwrap())
                .flat_map(|record| match record {
                    Record::Group { records } => records,
                    _ => vec![record],
                })
                .filter_map(|record| match record {
                    Record::Title { title } => Some(title),
                    _ => None,
                })
                .collect();
            let expected = explicit.unwrap_or(ROOT_TEXT);
            assert_eq!(titles, [expected]);
            assert_eq!(
                StoredSession::load_from(session.id, dir.path())
                    .unwrap()
                    .title,
                expected
            );
        });
    }

    pub(crate) struct JournalCheckpoint {
        snapshot: SessionJournalSnapshot,
        _publication: RwLockReadGuardArc<()>,
    }

    impl JournalCheckpoint {
        pub(crate) fn snapshot(&self) -> &SessionJournalSnapshot {
            &self.snapshot
        }
    }

    impl SessionJournal {
        pub(crate) fn pending_revision(&self) -> Option<u64> {
            self.lock()
                .staged
                .front()
                .map(|entry| entry.snapshot.revision)
        }

        pub(crate) fn queue_delivery(
            self: &Arc<Self>,
            parent: AgentId,
            delivery: Arc<ChildDelivery>,
            preview_bytes: usize,
        ) -> Result<bool, DeliveryRejection> {
            self.queue_delivery_for(parent, delivery, preview_bytes, &[])
        }

        pub(crate) async fn capture_checkpoint(&self) -> Result<JournalCheckpoint, String> {
            let publication = self.publication_read().await;
            let snapshot = self.checkpoint_snapshot()?;
            Ok(JournalCheckpoint {
                snapshot,
                _publication: publication,
            })
        }

        pub(crate) fn record_config(
            &self,
            agent: AgentId,
            generation: u64,
            config: StoredAgentConfig,
            options: Option<BTreeMap<String, String>>,
        ) -> Result<u64, JournalError> {
            self.record_config_bound(None, agent, generation, config, options, Vec::new())
        }
    }

    const FIRST: &str = "first";
    const SECOND: &str = "second";
    const SLOT: &str = "journal/incremental";

    #[test_case(false; "scope_exit")]
    #[test_case(true; "unwind")]
    fn terminal_guard_drop_unblocks_preparation(unwind: bool) {
        smol::block_on(async {
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            let admission = admitted(agent);
            let StateChange::Admitted { turn, .. } = admission.change else {
                unreachable!();
            };
            journal
                .commit(JournalCommit {
                    changes: vec![admission],
                    ..Default::default()
                })
                .unwrap();
            let terminal = journal
                .record_terminal(
                    TurnResult {
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
                    },
                    None,
                )
                .unwrap()
                .unwrap();
            let mut prepared = Box::pin(journal.prepare(JournalCommit {
                changes: vec![admitted(agent)],
                ..Default::default()
            }));
            assert!(poll_once(&mut prepared).await.is_none());
            let exit = catch_unwind(AssertUnwindSafe(move || {
                let _terminal = terminal;
                assert!(!unwind, "{FIRST}");
            }));
            assert_eq!(exit.is_err(), unwind);
            let prepared = poll_once(&mut prepared).await.unwrap().unwrap();
            prepared.commit().unwrap();
            assert!(journal.pending_revision().is_none());
            assert_eq!(journal.snapshot().state.iter().len(), 3);
        });
    }

    #[test]
    fn spawn_flush_rejects_pending_changes_without_waiting_or_reordering() {
        let directory = tempfile::tempdir().unwrap();
        let session = Session::new(FIRST, "/");
        let journal = SessionJournal::from_session(&session).unwrap();
        let writer =
            crate::session_writer::SessionWriter::open_session(directory.path(), &session).unwrap();
        journal.bind_writer(writer).unwrap();
        let pending = journal.stage(JournalCommit::default(), 1).unwrap();
        assert_eq!(
            journal.flush_spawn(pending.revision).unwrap_err(),
            JournalError::SpawnPublicationPending.to_string()
        );
        journal
            .complete(pending, vec![admitted(AgentId::generate())])
            .unwrap();
        journal.flush_spawn(journal.snapshot().revision).unwrap();
        assert_eq!(journal.published_revision(), journal.snapshot().revision);
    }

    #[test]
    fn writer_binding_flushes_backlog_once_and_waits_for_pending_conversion() {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let session = Session::new(FIRST, "/");
            let journal = SessionJournal::from_session(&session).unwrap();
            let agent = AgentId::generate();
            journal
                .commit(JournalCommit {
                    changes: vec![admitted(agent)],
                    ..Default::default()
                })
                .unwrap();
            let writer =
                crate::session_writer::SessionWriter::open_session(directory.path(), &session)
                    .unwrap();
            journal.bind_writer(writer.clone()).unwrap();
            journal.bind_writer(writer).unwrap();
            let pending = journal.stage(JournalCommit::default(), 1).unwrap();
            let flush = journal.flush();
            futures_lite::pin!(flush);
            assert!(futures_lite::future::poll_once(&mut flush).await.is_none());
            journal.complete(pending, vec![admitted(agent)]).unwrap();
            flush.await.unwrap();
            let path = directory.path().join(format!("{}.jsonl", session.id));
            let log = std::fs::read_to_string(path).unwrap();
            let records: Vec<serde_json::Value> = log
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(
                records
                    .iter()
                    .filter(|record| record["t"] == "state")
                    .count(),
                2
            );
        });
    }

    #[test]
    fn ready_emissions_preserve_history_events_and_stable_ids() {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        let mut history = HistorySnapshot::new(vec![Message::user(FIRST.into())]);
        journal
            .commit(JournalCommit {
                histories: [(agent, history.clone())].into(),
                ..Default::default()
            })
            .unwrap();
        journal
            .commit(JournalCommit {
                histories: [(agent, history.clone())].into(),
                ..Default::default()
            })
            .unwrap();
        let first_id = history.ids[0];
        history = HistorySnapshot::new(vec![Message::user(SECOND.into())]);
        history.covered = Some(first_id);
        let summary_id = history.ids[0];
        journal
            .commit(JournalCommit {
                histories: [(agent, history)].into(),
                ..Default::default()
            })
            .unwrap();
        let state = journal.lock();
        let [super::SessionRecord::Group { records }, compaction] = state.unwritten.as_slice()
        else {
            panic!("expected grouped history and subsequent compaction");
        };
        assert!(
            matches!(&records[0], super::SessionRecord::Msg { id, agent: saved, .. } if *id == first_id && *saved == agent)
        );
        assert!(
            matches!(compaction, super::SessionRecord::Compaction { ids, covered, .. } if ids == &[summary_id] && *covered == Some(first_id))
        );
    }

    #[test]
    fn history_metadata_changes_emit_without_repeating_messages() {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        let mut history = HistorySnapshot::new(vec![Message::user(FIRST.into())]);
        journal
            .commit(JournalCommit {
                histories: [(agent, history.clone())].into(),
                ..Default::default()
            })
            .unwrap();
        journal.lock().unwritten.clear();
        history.compacted = true;
        history.conversation = Some(super::ConversationState::fresh(None));
        journal
            .commit(JournalCommit {
                histories: [(agent, history.clone())].into(),
                ..Default::default()
            })
            .unwrap();
        journal
            .commit(JournalCommit {
                histories: [(agent, history.clone())].into(),
                ..Default::default()
            })
            .unwrap();
        let state = journal.lock();
        assert_eq!(state.unwritten.len(), 1);
        assert!(
            matches!(&state.unwritten[0], super::SessionRecord::History { agent: saved, compacted: true, conversation } if *saved == agent && conversation == &history.conversation)
        );
    }

    #[test_case(false; "complete_in_order")]
    #[test_case(true; "complete_out_of_order")]
    fn ready_emissions_wait_for_contiguous_conversions(reverse: bool) {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        let first = journal.stage(JournalCommit::default(), 1).unwrap();
        let second = journal.stage(JournalCommit::default(), 1).unwrap();
        if reverse {
            journal
                .complete(second.clone(), vec![admitted(agent)])
                .unwrap();
            assert!(journal.lock().unwritten.is_empty());
        }
        journal.complete(first, vec![admitted(agent)]).unwrap();
        if !reverse {
            assert_eq!(journal.lock().unwritten.len(), 1);
            journal.complete(second, vec![admitted(agent)]).unwrap();
        }
        let state = journal.lock();
        let sequences: Vec<_> = state
            .unwritten
            .iter()
            .map(|record| match record {
                super::SessionRecord::State { d } => d.seq,
                _ => panic!("expected state record"),
            })
            .collect();
        assert_eq!(sequences, [1, 2]);
    }

    #[test]
    fn restored_baseline_emits_only_new_records() {
        let source = SessionJournal::default();
        let agent = AgentId::generate();
        source
            .commit(JournalCommit {
                changes: vec![admitted(agent)],
                ..Default::default()
            })
            .unwrap();
        let mut session = Session::new(FIRST, "/");
        source.snapshot().apply_to(&mut session).unwrap();
        let restored = SessionJournal::from_session(&session).unwrap();
        assert!(restored.lock().unwritten.is_empty());
        restored
            .commit(JournalCommit {
                changes: vec![admitted(agent)],
                ..Default::default()
            })
            .unwrap();
        let state = restored.lock();
        assert_eq!(state.unwritten.len(), 1);
        assert!(matches!(&state.unwritten[0], super::SessionRecord::State { d } if d.seq == 2));
    }

    fn assert_reducer_matches_fold(snapshot: &super::SessionJournalSnapshot) {
        let full = makima_storage::session_state::SessionStateView::fold(
            &snapshot.state.to_vec(),
            &snapshot.agent_revisions.to_vec(),
        )
        .unwrap();
        assert_eq!(snapshot.reduced().unwrap().materialize(), full);
        assert_eq!(snapshot.fold().unwrap(), full);
    }

    #[test_case(vec![1, 1]; "duplicate_durable_target")]
    #[test_case(vec![2, 1]; "unordered_durable_target")]
    fn nonauthoritative_publication_rejects_invalid_raw_stream_without_mutation(
        sequences: Vec<u64>,
    ) {
        let snapshot = super::SessionJournalSnapshot::empty();
        let mut session = super::StoredSession::new(FIRST, "/");
        let records: Vec<_> = sequences
            .into_iter()
            .map(|seq| makima_storage::session_state::StoredStateRecord {
                seq,
                agent: None,
                generation: 0,
                change: StateChange::Plugin {
                    key: SLOT.into(),
                    version: 1,
                    value: None,
                },
            })
            .collect();
        session.replace_state(records);
        let before = session.state().to_vec();
        let error = snapshot.apply_to(&mut session).unwrap_err();
        assert!(matches!(
            error,
            super::JournalError::InvalidState(
                makima_storage::session_state::StateFoldError::UnorderedSequence { .. }
            )
        ));
        assert_eq!(session.state(), before);
    }

    #[test_case(false; "empty_snapshot")]
    #[test_case(true; "snapshot_with_state")]
    fn incremental_commit_rejects_bad_tail_atomically_and_shares_old_records(with_state: bool) {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        journal
            .commit(JournalCommit {
                histories: [(agent, HistorySnapshot::new(Vec::new()))].into(),
                ..Default::default()
            })
            .unwrap();
        if with_state {
            journal
                .commit(JournalCommit {
                    changes: vec![JournalChange {
                        agent: Some(agent),
                        generation: 0,
                        change: StateChange::Admitted {
                            turn: TurnId::generate(),
                            origin: StoredTurnOrigin::User,
                        },
                    }],
                    ..Default::default()
                })
                .unwrap();
        }
        let before = journal.snapshot();
        let plugin = || JournalChange {
            agent: Some(agent),
            generation: 0,
            change: StateChange::Plugin {
                key: SLOT.into(),
                version: 1,
                value: Some(json!(FIRST)),
            },
        };
        let error = journal
            .commit(JournalCommit {
                changes: vec![
                    plugin(),
                    JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Compaction { epoch: 1 },
                    },
                ],
                ..Default::default()
            })
            .unwrap_err();
        assert!(matches!(
            error,
            super::JournalError::InvalidState(
                makima_storage::session_state::StateFoldError::MissingCompactionAgent { .. }
            )
        ));
        let rejected = journal.snapshot();
        assert_eq!(
            (rejected.revision, rejected.seq),
            (before.revision, before.seq)
        );
        assert!(rejected.state.same_root(&before.state));
        assert_eq!(rejected.fold().unwrap(), before.fold().unwrap());
        journal
            .commit(JournalCommit {
                changes: vec![plugin()],
                ..Default::default()
            })
            .unwrap();
        let accepted = journal.snapshot();
        assert_eq!(accepted.revision, before.revision + 1);
        assert_eq!(accepted.seq, before.seq + 1);
        for previous in before.state.iter() {
            assert!(std::ptr::eq(
                previous,
                accepted
                    .state
                    .iter()
                    .find(|record| record.seq == previous.seq)
                    .unwrap()
            ));
        }
        assert_eq!(
            accepted.plugin(Some(agent), SLOT).unwrap().value,
            Some(json!(FIRST))
        );
        assert_reducer_matches_fold(&accepted);
        let unchanged = journal.commit(JournalCommit::default()).unwrap();
        assert_eq!(unchanged, accepted.revision);
        assert!(journal.snapshot().state.same_root(&accepted.state));
    }

    #[test_case(false; "complete_earlier_first")]
    #[test_case(true; "complete_later_first")]
    fn pending_completion_replays_order_sensitive_suffix(complete_later_first: bool) {
        let journal = SessionJournal::default();
        let agent = AgentId::generate();
        let first = journal.stage(JournalCommit::default(), 1).unwrap();
        let second = journal.stage(JournalCommit::default(), 1).unwrap();
        let plugin = |value: &str| JournalChange {
            agent: Some(agent),
            generation: 0,
            change: StateChange::Plugin {
                key: SLOT.into(),
                version: 1,
                value: Some(json!(value)),
            },
        };
        journal
            .commit(JournalCommit {
                changes: vec![plugin(SECOND)],
                ..Default::default()
            })
            .unwrap();
        if complete_later_first {
            journal
                .complete(second.clone(), vec![plugin(FIRST)])
                .unwrap();
        }
        journal.complete(first, vec![plugin(FIRST)]).unwrap();
        if !complete_later_first {
            journal.complete(second, vec![plugin(FIRST)]).unwrap();
        }
        let snapshot = journal.snapshot();
        assert_eq!(
            snapshot.plugin(Some(agent), SLOT).unwrap().value,
            Some(json!(SECOND))
        );
        assert_eq!(
            snapshot.plugin(Some(agent), SLOT).unwrap().revision,
            snapshot.seq
        );
        assert_reducer_matches_fold(&snapshot);
    }

    #[test_case(false; "cancelled_anchor_keeps_approval_unpublished")]
    #[test_case(true; "anchor_folds_resolved_approval_edit")]
    fn prepared_state_edit_preserves_history(install: bool) {
        use crate::state::{StateScope, StateSlotSpec};
        use serde_json::Value;
        smol::block_on(async {
            const SLOT: &str = "approval/plan";
            let journal = Arc::new(SessionJournal::default());
            let agent = AgentId::generate();
            let history = HistorySnapshot::new(vec![Message::user(FIRST.into())]);
            journal
                .commit(JournalCommit {
                    agents: vec![test_record(agent, None)],
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
                    ..Default::default()
                })
                .await
                .unwrap();
            let snapshot = prepared.snapshot();
            let record = snapshot.state.iter().next_back().unwrap();
            let reserved_sequence = record.seq;
            assert!(matches!(record.change, StateChange::Plugin { .. }));
            let view = snapshot.fold().unwrap();
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
                        changes: vec![JournalChange {
                            agent: Some(agent),
                            generation: 0,
                            change: StateChange::Admitted {
                                turn: TurnId::generate(),
                                origin: StoredTurnOrigin::User,
                            },
                        }],
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                assert!(next.snapshot().state.iter().next_back().unwrap().seq > reserved_sequence);
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
            assert_eq!(journal.snapshot().state.iter().len(), 1);
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
        let _terminal = journal
            .record_terminal(delivery.result.clone(), None)
            .unwrap();
        journal
            .queue_delivery(parent, Arc::clone(&delivery), FIRST.len())
            .unwrap();
        assert!(journal.snapshot().state.iter().next().is_none());
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
        assert_eq!(journal.snapshot().state.iter().len(), 1);
    }

    #[derive(Default)]
    struct FailingOffload {
        failures: usize,
        attempts: std::sync::atomic::AtomicUsize,
    }

    impl crate::tools::offload::OffloadBackend for Arc<FailingOffload> {
        fn create_new(&self, _: &str, _: &[u8]) -> std::io::Result<bool> {
            let attempt = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if attempt < self.failures {
                return Err(std::io::Error::other(OFFLOAD_FAULT));
            }
            Ok(true)
        }

        fn total_bytes(&self) -> std::io::Result<u64> {
            Ok(0)
        }

        fn remove_all(&self) -> std::io::Result<()> {
            Ok(())
        }

        fn path(&self, name: &str) -> std::path::PathBuf {
            name.into()
        }
    }

    const OFFLOAD_FAULT: &str = "injected result offload failure";

    #[test_case(false; "transient")]
    #[test_case(true; "permanent")]
    fn offload_failure_retries_then_releases_ordered_prefix(permanent: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let mut session = Session::new(FIRST, "/");
            let agent = AgentId::generate();
            session.bind_root_id(agent).unwrap();
            let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
            let writer =
                crate::session_writer::SessionWriter::open_session(directory.path(), &session)
                    .unwrap();
            journal.bind_writer(writer).unwrap();
            let backend = Arc::new(FailingOffload {
                failures: if permanent { usize::MAX } else { 1 },
                ..Default::default()
            });
            let store = Arc::new(OffloadStore::new(Box::new(Arc::clone(&backend))));
            let turn = TurnId::generate();
            let later = TurnId::generate();
            let admission = |turn| JournalChange {
                agent: Some(agent),
                generation: 0,
                change: StateChange::Admitted {
                    turn,
                    origin: StoredTurnOrigin::User,
                },
            };
            let result = |turn, text| TurnResult {
                outcome: TurnOutcome::cancelled(
                    agent,
                    turn,
                    TokenUsage::default(),
                    0,
                    TurnCancellationReason::User,
                ),
                text,
                output: Vec::new(),
                provenance: TurnProvenance::default(),
                duration_ms: 1,
            };
            let prefix = journal
                .commit(JournalCommit {
                    changes: vec![admission(turn)],
                    ..Default::default()
                })
                .unwrap();
            let first_terminal = journal
                .record_terminal(
                    result(turn, FIRST.repeat(MAX_INLINE_TURN_RESULT_BYTES)),
                    Some(store),
                )
                .unwrap();
            journal
                .commit(JournalCommit {
                    changes: vec![admission(later)],
                    ..Default::default()
                })
                .unwrap();
            let later_terminal = journal
                .record_terminal(result(later, SECOND.into()), None)
                .unwrap();
            drop(later_terminal);
            assert_eq!(journal.snapshot().revision, prefix);
            drop(first_terminal);
            futures_lite::future::or(
                async {
                    loop {
                        let changed = journal.conversion_changed.listen();
                        if journal.pending_revision().is_none() {
                            break;
                        }
                        changed.await;
                    }
                },
                async {
                    smol::Timer::after(std::time::Duration::from_secs(5)).await;
                    panic!("failed conversion still blocks the journal");
                },
            )
            .await;
            let attempts = backend.attempts.load(std::sync::atomic::Ordering::Relaxed);
            assert_eq!(
                attempts,
                if permanent {
                    super::CONVERSION_RETRY_DELAYS.len() + 1
                } else {
                    2
                }
            );
            journal
                .wait_terminal_conversion(agent, later)
                .await
                .unwrap();
            if permanent {
                assert!(
                    journal
                        .wait_terminal_conversion(agent, turn)
                        .await
                        .unwrap_err()
                        .contains(OFFLOAD_FAULT)
                );
                assert!(journal.persistence_error().unwrap().contains(OFFLOAD_FAULT));
            } else {
                journal.wait_terminal_conversion(agent, turn).await.unwrap();
                assert!(journal.persistence_error().is_none());
            }
            journal.flush().await.unwrap();
            let reloaded =
                Session::<Message, TokenUsage, ToolOutput>::load_from(session.id, directory.path())
                    .unwrap();
            let view = reloaded.latest_state().unwrap();
            let scope = &view.scopes[&Some(agent)];
            assert!(scope.admitted.is_empty());
            assert_eq!(
                scope.results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                [turn, later]
            );
            let stored = &scope.results[0].1;
            assert_eq!(stored.duration_ms, 1);
            if permanent {
                assert!(
                    SessionJournal::failed_conversion(stored)
                        .unwrap()
                        .contains(OFFLOAD_FAULT)
                );
                assert!(matches!(&stored.text, StoredPayload::Inline(text) if text.is_empty()));
            } else {
                assert!(matches!(&stored.text, StoredPayload::Offloaded(_)));
            }
        });
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
        let stale_terminal = journal
            .record_terminal_for(Some(journal.incarnation(agent)), result.clone(), None)
            .unwrap();
        let expected = Arc::clone(&journal.lock().results[&(agent, turn)].conversion);
        let stored = result.to_stored(MakimaId::generate(), None).unwrap();
        journal.set_incarnation(agent, 2);
        assert!(!journal.record_conversion_error(
            (agent, turn),
            &expected,
            CONVERSION_ERROR.into()
        ));
        assert!(journal.checkpoint_snapshot().is_ok());
        let terminal = journal
            .record_terminal_for(Some(journal.incarnation(agent)), result, None)
            .unwrap();
        assert!(!journal.record_conversion_error(
            (agent, turn),
            &expected,
            CONVERSION_ERROR.into()
        ));
        drop(stale_terminal);
        assert!(journal.pending_revision().is_some());
        journal
            .complete_conversion((agent, turn), &expected, stored)
            .unwrap();
        assert!(journal.lock().results.contains_key(&(agent, turn)));
        assert!(journal.persistence_error().is_none());
        drop(terminal);
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
                assert_eq!(journal.snapshot().state.iter().len(), usize::from(!stale));
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
            assert_eq!(journal.snapshot().state.iter().len(), 1);
            assert_eq!(journal.incarnation(agent).reservation, 1);
            assert!(futures_lite::future::poll_once(&mut detach).await.is_none());
            drop(token);
            detach.await;
            assert_eq!(journal.incarnation(agent).reservation, 2);
            assert_eq!(journal.snapshot().state.iter().len(), 1);
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
            assert_eq!(ordinary.snapshot().state.iter().len(), usize::from(install));
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
            assert_eq!(candidate.state.iter().len(), 1);
            assert_eq!(journal.snapshot().revision, before.revision);
            let mut read = Box::pin(journal.boundary_read());
            assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            journal
                .commit(JournalCommit {
                    changes: vec![admitted(agent)],
                    ..Default::default()
                })
                .unwrap();
            assert!(journal.snapshot().state.iter().next().is_none());
            if install {
                token.install().unwrap();
                assert_eq!(journal.published_revision(), candidate.revision);
                assert_eq!(journal.snapshot().state.iter().len(), 2);
                assert!(futures_lite::future::poll_once(&mut read).await.is_none());
            }
            drop(token);
            drop(read.await);
            let ready = journal.snapshot();
            assert_eq!(ready.state.iter().len(), if install { 2 } else { 1 });
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
            assert_eq!(journal.acknowledge(&candidate).is_ok(), install);
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
        let store = OffloadStore::on_disk(directory.path().canonicalize().unwrap().join("offload"));
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
            assert_eq!(prefix.state.iter().len(), 1);
            journal.complete(second, vec![second_change]).unwrap();
        }
        let ready = journal.snapshot();
        assert_eq!(ready.state.iter().len(), 3);
        assert!(
            ready
                .state
                .iter()
                .zip(ready.state.iter().skip(1))
                .all(|(left, right)| left.seq < right.seq)
        );
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
        assert_eq!(journal.snapshot().state.iter().len(), 1);
    }
}
