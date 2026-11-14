use crate::{
    blobs::FileState,
    id::{AgentId, MakimaId, TurnId},
    restore::TransitionRef,
    sessions::StoredRule,
};
use imbl::{OrdMap, OrdSet};
use makima_domain::{ModelSelection, StoredThinking, TokenUsage};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::Arc,
};
use thiserror::Error;

pub const BOUNDARY_POOL_FORMAT: &str = "boundary-pool-v1";
pub const MAX_STORED_TURN_RESULTS: usize = 128;
pub const MAX_PLUGIN_VALUE_DEPTH: usize = 64;
pub const MAX_PLUGIN_VALUE_BYTES: usize = 256 * 1024;
pub const MAX_INLINE_TURN_RESULT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredStateRecord {
    pub seq: u64,
    pub agent: Option<AgentId>,
    pub generation: u64,
    pub change: StateChange,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateChange {
    Boundary {
        offset: usize,
        lens: BTreeMap<AgentId, (u64, usize)>,
    },
    BoundarySnapshot {
        boundary: u64,
        #[serde(deserialize_with = "Value::deserialize")]
        snapshot: Value,
    },
    Admitted {
        turn: TurnId,
        origin: StoredTurnOrigin,
    },
    Config(StoredAgentConfig),
    Rules {
        rules: Vec<StoredRule>,
        yolo: Option<bool>,
    },
    Context {
        source: String,
        hash: String,
        content: String,
    },
    Plugin {
        key: String,
        version: u32,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "plugin_value"
        )]
        value: Option<Value>,
    },
    Turn {
        turn: TurnId,
        result: StoredTurnResult,
    },
    Delivery {
        id: (AgentId, TurnId),
        op: StoredDeliveryOp,
    },
    File {
        path: PathBuf,
        before: FileState,
        after: FileState,
    },
    ToolExecution {
        #[serde(rename = "tool_kind")]
        kind: String,
    },
    Compaction {
        epoch: u64,
    },
}

fn plugin_value<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredAgentConfig {
    pub cwd: String,
    pub model: String,
    pub thinking: Option<StoredThinking>,
    pub fast: bool,
    pub workflow: bool,
    pub mode: StoredModeSnapshot,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredModeSnapshot {
    pub id: String,
    pub label: String,
    pub system_prompt: Option<String>,
    pub restrict_write_to: Option<PathBuf>,
    pub tools: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredAgentLifecycle {
    Open,
    Closed,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredAgentRecord {
    pub id: AgentId,
    pub parent: Option<AgentId>,
    pub depth: u32,
    pub transcript_key: String,
    pub lifecycle: StoredAgentLifecycle,
    pub ancestry_unknown: bool,
    pub label: Option<String>,
    pub spawned_by_tool_use_id: Option<String>,
    pub spec: Option<StoredAgentSpec>,
    pub restorable: Result<(), String>,
    pub usage: TokenUsage,
    pub created_seq: u64,
    pub updated_seq: u64,
    pub legacy: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredAgentSpec {
    pub model: ModelSelection,
    pub system: String,
    pub tools: Vec<Value>,
    pub audience: String,
    pub mcp: bool,
    pub output_schema: Option<Value>,
    pub name: Option<String>,
    pub silent: bool,
    pub auto_deliver: bool,
    pub inherit_provider: bool,
    pub mode_ceiling: StoredModeCeiling,
    pub plugin: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredModeCeiling {
    pub restrict_write_to: Option<PathBuf>,
    pub tools: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentLink {
    pub session: MakimaId,
    pub message_count: usize,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredBranch {
    pub id: MakimaId,
    pub created_at: u64,
    pub at_seq: u64,
    pub agent: AgentId,
    pub offset: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transition: Option<TransitionRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredTurnOrigin {
    Internal,
    User,
    Plugin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTurnProvenance {
    pub origin: StoredTurnOrigin,
    pub plugin: Option<String>,
    pub plugin_generation: Option<u64>,
    pub source_agent: Option<AgentId>,
    pub source_turn: Option<TurnId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredOffloadRef {
    pub session: MakimaId,
    pub path: PathBuf,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "storage", content = "value", rename_all = "snake_case")]
pub enum StoredPayload<T> {
    Inline(T),
    Offloaded(StoredOffloadRef),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTurnResult {
    pub outcome: StoredTurnOutcome,
    pub text: StoredPayload<String>,
    pub output: StoredPayload<Vec<Value>>,
    pub provenance: StoredTurnProvenance,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTurnOutcome {
    pub agent_id: AgentId,
    pub turn_id: TurnId,
    pub usage: TokenUsage,
    pub num_turns: u32,
    pub cost: Option<f64>,
    pub list_cost: Option<f64>,
    pub context_size: u32,
    pub context_window: u32,
    #[serde(flatten)]
    pub status: StoredTurnStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StoredTurnStatus {
    Completed { reason: StoredDoneReason },
    Failed { failure: StoredTurnFailure },
    Cancelled { reason: StoredCancellationReason },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredDoneReason {
    EndTurn,
    MaxTokens,
    MaxTurns,
    Compact,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredCancellationReason {
    User,
    Closed,
    Shutdown,
    Interrupted,
    Withdrawn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredTurnFailureKind {
    Provider,
    Authentication,
    Timeout,
    Tool,
    Transport,
    InvalidResponse,
    Internal,
    Compaction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTurnFailure {
    pub kind: StoredTurnFailureKind,
    pub diagnostic: String,
    pub user_message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredDelivery {
    pub label: Option<String>,
    pub result: StoredTurnResult,
    #[serde(default)]
    pub held: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_bytes: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", content = "delivery", rename_all = "snake_case")]
pub enum StoredDeliveryOp {
    Queued(Box<StoredDelivery>),
    Consumed,
    Withdrawn,
    Dropped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBoundary {
    pub agent: Option<AgentId>,
    pub generation: u64,
    pub offset: usize,
    pub lens: BTreeMap<AgentId, (u64, usize)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContext {
    pub hash: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredPluginValue {
    pub revision: u64,
    pub version: u32,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredFileState {
    pub before: FileState,
    pub after: FileState,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StoredScopeState {
    pub generation: u64,
    pub config: Option<StoredAgentConfig>,
    pub rules: Vec<StoredRule>,
    pub yolo: Option<bool>,
    pub context: BTreeMap<String, StoredContext>,
    pub plugins: BTreeMap<String, StoredPluginValue>,
    pub admitted: BTreeMap<TurnId, StoredTurnOrigin>,
    pub results: VecDeque<(TurnId, StoredTurnResult)>,
    pub deliveries: BTreeMap<(AgentId, TurnId), StoredDeliveryOp>,
    pub files: BTreeMap<PathBuf, StoredFileState>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SessionStateView {
    pub agents: BTreeMap<AgentId, StoredAgentRecord>,
    pub scopes: BTreeMap<Option<AgentId>, StoredScopeState>,
    pub boundaries: BTreeMap<u64, StoredBoundary>,
    pub compactions: BTreeMap<AgentId, (u64, u64)>,
}

#[derive(Debug, Clone, Default)]
pub struct SessionStateReducer {
    pub agents: OrdMap<AgentId, Arc<StoredAgentRecord>>,
    scopes: OrdMap<Option<AgentId>, ReducedScope>,
    boundaries: OrdMap<u64, Arc<StoredBoundary>>,
    compactions: OrdMap<AgentId, (u64, u64)>,
    state_sequences: OrdSet<u64>,
    agent_sequences: OrdSet<u64>,
}

#[derive(Debug, Clone, Default)]
struct ReducedScope {
    generation: u64,
    config: Option<Arc<StoredAgentConfig>>,
    rules: Arc<Vec<StoredRule>>,
    yolo: Option<bool>,
    context: OrdMap<String, Arc<StoredContext>>,
    plugins: OrdMap<String, Arc<StoredPluginValue>>,
    admitted: OrdMap<TurnId, Arc<StoredTurnOrigin>>,
    results: Arc<VecDeque<(TurnId, StoredTurnResult)>>,
    deliveries: OrdMap<(AgentId, TurnId), Arc<StoredDeliveryOp>>,
    delivery_sequences: OrdMap<(AgentId, TurnId), u64>,
    files: OrdMap<PathBuf, Arc<StoredFileState>>,
}

impl ReducedScope {
    fn materialize(&self) -> StoredScopeState {
        StoredScopeState {
            generation: self.generation,
            config: self.config.as_deref().cloned(),
            rules: self.rules.as_ref().clone(),
            yolo: self.yolo,
            context: self
                .context
                .iter()
                .map(|(key, value)| (key.clone(), value.as_ref().clone()))
                .collect(),
            plugins: self
                .plugins
                .iter()
                .map(|(key, value)| (key.clone(), value.as_ref().clone()))
                .collect(),
            admitted: self
                .admitted
                .iter()
                .map(|(key, value)| (*key, value.as_ref().clone()))
                .collect(),
            results: self.results.as_ref().clone(),
            deliveries: self
                .deliveries
                .iter()
                .map(|(key, value)| (*key, value.as_ref().clone()))
                .collect(),
            files: self
                .files
                .iter()
                .map(|(key, value)| (key.clone(), value.as_ref().clone()))
                .collect(),
        }
    }
}

impl SessionStateReducer {
    pub fn from_ordered(
        state: &[StoredStateRecord],
        revisions: &[(u64, StoredAgentRecord)],
    ) -> Result<Self, StateFoldError> {
        Self::through(state, revisions, u64::MAX)
    }

    fn through(
        state: &[StoredStateRecord],
        revisions: &[(u64, StoredAgentRecord)],
        through: u64,
    ) -> Result<Self, StateFoldError> {
        Self::validate_structure(state, revisions)?;
        let mut reducer = Self::default();
        for (seq, agent) in revisions.iter().take_while(|(seq, _)| *seq <= through) {
            reducer.apply_agent(*seq, agent)?;
        }
        for record in state.iter().take_while(|record| record.seq <= through) {
            reducer.apply_record(record)?;
        }
        Ok(reducer)
    }

    pub fn validate_structure(
        state: &[StoredStateRecord],
        revisions: &[(u64, StoredAgentRecord)],
    ) -> Result<(), StateFoldError> {
        validate_sequence("state", state.iter().map(|record| record.seq))?;
        validate_sequence("agent", revisions.iter().map(|(seq, _)| *seq))?;
        let state_sequences: BTreeSet<_> = state.iter().map(|record| record.seq).collect();
        for (seq, agent) in revisions {
            if state_sequences.contains(seq) {
                return Err(StateFoldError::DuplicateSequence { seq: *seq });
            }
            if agent.updated_seq != *seq || agent.created_seq > agent.updated_seq {
                return Err(StateFoldError::InvalidAgentRevision { seq: *seq });
            }
        }
        Ok(())
    }

    pub fn materialize(&self) -> SessionStateView {
        SessionStateView {
            agents: self
                .agents
                .iter()
                .map(|(key, value)| (*key, value.as_ref().clone()))
                .collect(),
            scopes: self
                .scopes
                .iter()
                .map(|(key, value)| (*key, value.materialize()))
                .collect(),
            boundaries: self
                .boundaries
                .iter()
                .map(|(key, value)| (*key, value.as_ref().clone()))
                .collect(),
            compactions: self
                .compactions
                .iter()
                .map(|(key, value)| (*key, *value))
                .collect(),
        }
    }

    pub fn plugin(&self, agent: Option<AgentId>, key: &str) -> Option<&StoredPluginValue> {
        self.scopes.get(&agent)?.plugins.get(key).map(Arc::as_ref)
    }

    pub fn deliveries(
        &self,
        parent: AgentId,
    ) -> impl Iterator<Item = ((AgentId, TurnId), &StoredDeliveryOp, u64)> {
        self.scopes
            .get(&Some(parent))
            .into_iter()
            .flat_map(|scope| {
                scope.deliveries.iter().map(|(id, op)| {
                    (
                        *id,
                        op.as_ref(),
                        scope.delivery_sequences.get(id).copied().unwrap_or(0),
                    )
                })
            })
    }

    pub fn delivery(&self, parent: AgentId, id: (AgentId, TurnId)) -> Option<&StoredDeliveryOp> {
        self.scopes
            .get(&Some(parent))?
            .deliveries
            .get(&id)
            .map(Arc::as_ref)
    }

    pub fn result(&self, agent: AgentId, turn: TurnId) -> Option<&StoredTurnResult> {
        self.scopes
            .get(&Some(agent))?
            .results
            .iter()
            .find(|(id, _)| *id == turn)
            .map(|(_, result)| result)
    }

    pub fn apply_agent(
        &mut self,
        seq: u64,
        agent: &StoredAgentRecord,
    ) -> Result<(), StateFoldError> {
        if self.state_sequences.contains(&seq) || self.agent_sequences.contains(&seq) {
            return Err(StateFoldError::DuplicateSequence { seq });
        }
        if agent.updated_seq != seq || agent.created_seq > agent.updated_seq {
            return Err(StateFoldError::InvalidAgentRevision { seq });
        }
        self.agent_sequences.insert(seq);
        self.agents.insert(agent.id, Arc::new(agent.clone()));
        Ok(())
    }

    pub fn apply_record(&mut self, record: &StoredStateRecord) -> Result<(), StateFoldError> {
        let seq = record.seq;
        if self.state_sequences.contains(&seq) || self.agent_sequences.contains(&seq) {
            return Err(StateFoldError::DuplicateSequence { seq });
        }
        validate_record(record)?;
        let mut candidate = self.clone();
        candidate.apply_change(record)?;
        candidate.state_sequences.insert(seq);
        *self = candidate;
        Ok(())
    }

    fn apply_change(&mut self, record: &StoredStateRecord) -> Result<(), StateFoldError> {
        let seq = record.seq;
        if let StateChange::BoundarySnapshot { boundary, snapshot } = &record.change {
            return if *boundary < seq
                && self.boundaries.contains_key(boundary)
                && snapshot.is_object()
                && valid_boundary_pool(*boundary, seq, snapshot)
            {
                Ok(())
            } else {
                Err(StateFoldError::InvalidBoundarySnapshot {
                    seq,
                    boundary: *boundary,
                })
            };
        }
        let mut scope = self.scopes.get(&record.agent).cloned().unwrap_or_default();
        scope.generation = record.generation;
        match &record.change {
            StateChange::Boundary { offset, lens } => {
                self.boundaries.insert(
                    seq,
                    Arc::new(StoredBoundary {
                        agent: record.agent,
                        generation: record.generation,
                        offset: *offset,
                        lens: lens.clone(),
                    }),
                );
            }
            StateChange::BoundarySnapshot { .. } => {}
            StateChange::Admitted { turn, origin } => {
                if !scope.results.iter().any(|(id, _)| id == turn) {
                    scope.admitted.insert(*turn, Arc::new(origin.clone()));
                }
            }
            StateChange::Config(config) => scope.config = Some(Arc::new(config.clone())),
            StateChange::Rules { rules, yolo } => {
                scope.rules = Arc::new(rules.clone());
                scope.yolo = *yolo;
            }
            StateChange::Context {
                source,
                hash,
                content,
            } => {
                scope.context.insert(
                    source.clone(),
                    Arc::new(StoredContext {
                        hash: hash.clone(),
                        content: content.clone(),
                    }),
                );
            }
            StateChange::Plugin {
                key,
                version,
                value,
            } => {
                scope.plugins.insert(
                    key.clone(),
                    Arc::new(StoredPluginValue {
                        revision: seq,
                        version: *version,
                        value: value.clone(),
                    }),
                );
            }
            StateChange::Turn { turn, result } => {
                scope.admitted.remove(turn);
                let results = Arc::make_mut(&mut scope.results);
                results.retain(|(id, _)| id != turn);
                results.push_back((*turn, result.clone()));
                if results.len() > MAX_STORED_TURN_RESULTS {
                    results.pop_front();
                }
            }
            StateChange::Delivery { id, op } => {
                scope.delivery_sequences.insert(*id, seq);
                if !scope.deliveries.get(id).is_some_and(|previous| {
                    !matches!(previous.as_ref(), StoredDeliveryOp::Queued(_))
                }) {
                    scope.deliveries.insert(*id, Arc::new(op.clone()));
                }
            }
            StateChange::File {
                path,
                before,
                after,
            } => {
                scope.files.insert(
                    path.clone(),
                    Arc::new(StoredFileState {
                        before: before.clone(),
                        after: after.clone(),
                    }),
                );
            }
            StateChange::ToolExecution { .. } => {}
            StateChange::Compaction { epoch } => {
                let agent = record
                    .agent
                    .ok_or(StateFoldError::MissingCompactionAgent { seq })?;
                self.compactions.insert(agent, (seq, *epoch));
            }
        }
        self.scopes.insert(record.agent, scope);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StateFoldError {
    #[error("{stream} sequence {seq} does not follow {previous}")]
    UnorderedSequence {
        stream: &'static str,
        previous: u64,
        seq: u64,
    },
    #[error("sequence {seq} is used by both a state and agent record")]
    DuplicateSequence { seq: u64 },
    #[error("agent revision at sequence {seq} has invalid creation/update sequences")]
    InvalidAgentRevision { seq: u64 },
    #[error(
        "boundary snapshot at sequence {seq} has no earlier retained boundary {boundary} or object payload"
    )]
    InvalidBoundarySnapshot { seq: u64, boundary: u64 },
    #[error("compaction at sequence {seq} requires an agent")]
    MissingCompactionAgent { seq: u64 },
    #[error("turn result at sequence {seq} does not match its agent and turn identity")]
    TurnIdentityMismatch { seq: u64 },
    #[error("delivery at sequence {seq} does not match its result identity")]
    DeliveryIdentityMismatch { seq: u64 },
    #[error("plugin value at sequence {seq} exceeds depth {MAX_PLUGIN_VALUE_DEPTH}")]
    PluginDepth { seq: u64 },
    #[error("plugin value at sequence {seq} exceeds {MAX_PLUGIN_VALUE_BYTES} encoded bytes")]
    PluginSize { seq: u64 },
}

impl SessionStateView {
    pub fn fold(
        state: &[StoredStateRecord],
        revisions: &[(u64, StoredAgentRecord)],
        through: u64,
    ) -> Result<Self, StateFoldError> {
        Ok(SessionStateReducer::through(state, revisions, through)?.materialize())
    }

    #[cfg(test)]
    pub fn boundary_available(&self, seq: u64) -> bool {
        self.boundaries.get(&seq).is_some_and(|boundary| {
            boundary.lens.keys().all(|agent| {
                self.compactions
                    .get(agent)
                    .is_none_or(|(barrier, _)| *barrier <= seq)
            })
        })
    }
}

pub fn validate_record(record: &StoredStateRecord) -> Result<(), StateFoldError> {
    let seq = record.seq;
    match &record.change {
        StateChange::Plugin {
            value: Some(value), ..
        } => validate_plugin_value(value, seq),
        StateChange::Turn { turn, result }
            if result.outcome.turn_id != *turn || record.agent != Some(result.outcome.agent_id) =>
        {
            Err(StateFoldError::TurnIdentityMismatch { seq })
        }
        StateChange::Delivery {
            id,
            op: StoredDeliveryOp::Queued(delivery),
        } if (
            delivery.result.outcome.agent_id,
            delivery.result.outcome.turn_id,
        ) != *id =>
        {
            Err(StateFoldError::DeliveryIdentityMismatch { seq })
        }
        StateChange::Compaction { .. } if record.agent.is_none() => {
            Err(StateFoldError::MissingCompactionAgent { seq })
        }
        _ => Ok(()),
    }
}

fn valid_boundary_pool(boundary: u64, seq: u64, snapshot: &Value) -> bool {
    if snapshot["format"].as_str() != Some(BOUNDARY_POOL_FORMAT) {
        return false;
    }
    let Ok(boundaries) = serde_json::from_value::<Vec<u64>>(snapshot["boundaries"].clone()) else {
        return false;
    };
    let Some(state) = snapshot["state"].as_array() else {
        return false;
    };
    if boundaries.first() != Some(&boundary)
        || boundaries.last().is_none_or(|boundary| *boundary >= seq)
        || boundaries.windows(2).any(|pair| pair[0] >= pair[1])
        || state
            .iter()
            .any(|record| record["change"]["kind"] == "boundary_snapshot")
    {
        return false;
    }
    let Ok(state) = serde_json::from_value::<Vec<StoredStateRecord>>(snapshot["state"].clone())
    else {
        return false;
    };
    let Ok(agents) =
        serde_json::from_value::<Vec<(u64, StoredAgentRecord)>>(snapshot["agents"].clone())
    else {
        return false;
    };
    let Some(histories) = snapshot["histories"].as_object() else {
        return false;
    };
    if histories.iter().any(|(agent, history)| {
        agent.parse::<AgentId>().is_err()
            || history["epoch"].as_u64().is_none()
            || history["compacted"].as_bool().is_none()
            || !history["messages"].is_array()
    }) {
        return false;
    }
    let Ok(view) = SessionStateView::fold(&state, &agents, u64::MAX) else {
        return false;
    };
    let mut revisions = agents.iter().peekable();
    let mut retained = BTreeSet::new();
    boundaries.iter().all(|seq| {
        while revisions
            .peek()
            .is_some_and(|(revision, _)| revision <= seq)
        {
            if let Some((_, agent)) = revisions.next() {
                retained.insert(agent.id);
            }
        }
        let Some(saved) = view.boundaries.get(seq) else {
            return false;
        };
        saved.lens.iter().all(|(agent, (epoch, len))| {
            histories.get(&agent.to_string()).is_some_and(|history| {
                history["epoch"].as_u64() == Some(*epoch)
                    && history["messages"]
                        .as_array()
                        .is_some_and(|messages| messages.len() >= *len)
            })
        }) && saved.agent.is_none_or(|agent| {
            saved
                .lens
                .get(&agent)
                .is_some_and(|(_, len)| saved.offset <= *len)
        }) && retained.iter().all(|agent| saved.lens.contains_key(agent))
    })
}

fn validate_sequence(
    stream: &'static str,
    seqs: impl Iterator<Item = u64>,
) -> Result<(), StateFoldError> {
    let mut previous = None;
    for seq in seqs {
        if let Some(previous) = previous
            && seq <= previous
        {
            return Err(StateFoldError::UnorderedSequence {
                stream,
                previous,
                seq,
            });
        }
        previous = Some(seq);
    }
    Ok(())
}

fn validate_plugin_value(value: &Value, seq: u64) -> Result<(), StateFoldError> {
    let mut pending = vec![(value, 0)];
    while let Some((value, depth)) = pending.pop() {
        if depth > MAX_PLUGIN_VALUE_DEPTH {
            return Err(StateFoldError::PluginDepth { seq });
        }
        match value {
            Value::Array(values) => pending.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => {
                pending.extend(values.values().map(|value| (value, depth + 1)))
            }
            _ => {}
        }
    }
    if serde_json::to_vec(value).map_or(true, |encoded| encoded.len() > MAX_PLUGIN_VALUE_BYTES) {
        return Err(StateFoldError::PluginSize { seq });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_PLUGIN_VALUE_BYTES, MAX_PLUGIN_VALUE_DEPTH, MAX_STORED_TURN_RESULTS,
        SessionStateReducer, SessionStateView, StateChange, StateFoldError, StoredAgentConfig,
        StoredAgentLifecycle, StoredAgentRecord, StoredDelivery, StoredDeliveryOp,
        StoredStateRecord, StoredTurnResult,
    };
    use crate::{
        blobs::FileState,
        id::{AgentId, MakimaId, TurnId},
    };
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::slice::from_ref;
    use test_case::test_case;

    const SLOT: &str = "test.slot";
    fn config() -> Value {
        json!({"cwd":"/saved", "model":"test/model", "thinking":{"kind":"off"},
            "fast":true, "workflow":false, "options":{"key":"value"},
            "mode":{"id":"custom", "label":"Custom", "system_prompt":"saved prompt",
                "restrict_write_to":"plan.md", "tools":[]}})
    }
    fn agent(id: AgentId, seq: u64) -> StoredAgentRecord {
        serde_json::from_value(json!({
            "id":id, "parent":null, "depth":0, "transcript_key":"root", "lifecycle":"open",
            "ancestry_unknown":false, "label":"Root", "spawned_by_tool_use_id":null,
            "restorable":{"Ok":null}, "usage":{}, "created_seq":seq, "updated_seq":seq,
            "legacy":false, "spec":{"model":{}, "system":"prompt", "tools":[{"name":"read"}],
                "audience":"general", "mcp":true, "output_schema":{"type":"string"},
                "name":"worker", "silent":false, "auto_deliver":true, "inherit_provider":false,
                "mode_ceiling":{"restrict_write_to":null,"tools":[]}, "plugin":"test"}
        }))
        .unwrap()
    }
    fn result(agent: AgentId, turn: TurnId) -> Value {
        json!({"outcome":{"agent_id":agent, "turn_id":turn, "usage":{"input":12,"output":3,"cache_creation":0,"cache_read":0},
                "num_turns":1, "cost":0.01, "list_cost":0.02, "context_size":20,
                "context_window":100, "status":"completed", "reason":"end_turn"},
            "text":{"storage":"inline","value":"done"},
            "output":{"storage":"inline","value":[{"ok":true}]},
            "provenance":{"origin":"plugin", "plugin":"test", "plugin_generation":2,
                "source_agent":agent, "source_turn":turn}, "duration_ms":7})
    }
    fn state(seq: u64, agent: Option<AgentId>, change: Value) -> StoredStateRecord {
        let record =
            serde_json::from_value(json!({"seq":seq,"agent":agent,"generation":1,"change":change}))
                .unwrap();
        assert_eq!(
            serde_json::from_slice::<StoredStateRecord>(&serde_json::to_vec(&record).unwrap())
                .unwrap(),
            record
        );
        record
    }
    fn fold(state: &[StoredStateRecord]) -> SessionStateView {
        SessionStateView::fold(state, &[], u64::MAX).unwrap()
    }

    #[test]
    fn boundary_snapshot_round_trips_without_mutating_scope_or_generation() {
        let agent = AgentId::generate();
        let prefix = [state(
            1,
            Some(agent),
            json!({"kind":"boundary","offset":0,"lens":{agent.to_string():[7,0]}}),
        )];
        let before = fold(&prefix);
        let payload = json!({"format":super::BOUNDARY_POOL_FORMAT,"boundaries":[1],
            "state":prefix,"agents":[],"histories":{agent.to_string():{"epoch":7,"compacted":false,"messages":[]}}});
        let auxiliary = state(
            2,
            None,
            json!({"kind":"boundary_snapshot","boundary":1,"snapshot":payload}),
        );
        let mut records = prefix.to_vec();
        records.push(StoredStateRecord {
            generation: u64::MAX,
            ..auxiliary
        });
        assert_eq!(fold(&records), before);
        assert_eq!(SessionStateView::fold(&records, &[], 1).unwrap(), before);
    }

    fn pooled_boundary(agent: AgentId) -> (Vec<StoredStateRecord>, Value) {
        let prefix = vec![
            state(
                1,
                Some(agent),
                json!({"kind":"boundary","offset":0,"lens":{agent.to_string():[7,0]}}),
            ),
            state(
                2,
                Some(agent),
                json!({"kind":"boundary","offset":1,"lens":{agent.to_string():[7,1]}}),
            ),
        ];
        let payload = json!({"format":super::BOUNDARY_POOL_FORMAT,"boundaries":[1,2],
            "state":prefix,"agents":[],"histories":{agent.to_string():{"epoch":7,"compacted":false,"messages":["message"]}}});
        (prefix, payload)
    }

    #[test]
    fn grouped_boundary_prefix_lenses_can_be_shorter_than_shared_history() {
        let agent = AgentId::generate();
        let (mut prefix, payload) = pooled_boundary(agent);
        prefix.push(state(
            3,
            None,
            json!({"kind":"boundary_snapshot","boundary":1,"snapshot":payload}),
        ));
        assert_eq!(fold(&prefix).boundaries.len(), 2);
    }

    #[test_case(super::StoredAgentLifecycle::Closed, true; "closed_with_cutoff")]
    #[test_case(super::StoredAgentLifecycle::Closed, false; "closed_without_cutoff")]
    #[test_case(super::StoredAgentLifecycle::Retired, true; "retired_with_cutoff")]
    #[test_case(super::StoredAgentLifecycle::Retired, false; "retired_without_cutoff")]
    fn grouped_pools_require_durable_cutoffs_for_closed_and_retired_histories(
        lifecycle: super::StoredAgentLifecycle,
        cutoff: bool,
    ) {
        let root = AgentId::generate();
        let closed = AgentId::generate();
        let revisions = vec![
            (0, agent(root, 0)),
            (1, agent(closed, 1)),
            (2, {
                let mut record = agent(closed, 2);
                record.created_seq = 1;
                record.lifecycle = lifecycle;
                record
            }),
        ];
        let mut lens = BTreeMap::from([(root, (7, 0))]);
        if cutoff {
            lens.insert(closed, (8, 1));
        }
        let prefix = vec![state(
            3,
            Some(root),
            json!({"kind":"boundary","offset":0,"lens":lens}),
        )];
        let payload = json!({"format":super::BOUNDARY_POOL_FORMAT,"boundaries":[3],
        "state":prefix,"agents":revisions,"histories":{
            root.to_string():{"epoch":7,"compacted":false,"messages":[]},
            closed.to_string():{"epoch":8,"compacted":false,"messages":["at boundary","late cleanup"]}
        }});
        let mut records = prefix;
        records.push(state(
            4,
            None,
            json!({"kind":"boundary_snapshot","boundary":3,"snapshot":payload}),
        ));
        assert_eq!(
            SessionStateView::fold(&records, &revisions, u64::MAX).is_ok(),
            cutoff
        );
    }

    #[test]
    fn grouped_pool_does_not_require_cutoffs_for_agents_created_after_covered_boundary() {
        let root = AgentId::generate();
        let future = AgentId::generate();
        let revisions = vec![(0, agent(root, 0)), (2, agent(future, 2))];
        let prefix = vec![state(
            1,
            Some(root),
            json!({"kind":"boundary","offset":0,"lens":{root.to_string():[7,0]}}),
        )];
        let payload = json!({"format":super::BOUNDARY_POOL_FORMAT,"boundaries":[1],
        "state":prefix,"agents":revisions,"histories":{
            root.to_string():{"epoch":7,"compacted":false,"messages":[]},
            future.to_string():{"epoch":8,"compacted":false,"messages":["later"]}
        }});
        let mut records = prefix;
        records.push(state(
            3,
            None,
            json!({"kind":"boundary_snapshot","boundary":1,"snapshot":payload}),
        ));
        assert!(SessionStateView::fold(&records, &revisions, u64::MAX).is_ok());
    }

    #[test_case("format"; "unknown_format")]
    #[test_case("missing_format"; "missing_format")]
    #[test_case("format_type"; "non_string_format")]
    #[test_case("missing_boundaries"; "missing_boundaries")]
    #[test_case("boundaries_type"; "non_array_boundaries")]
    #[test_case("empty"; "empty_boundaries")]
    #[test_case("order"; "unordered_boundaries")]
    #[test_case("representative"; "wrong_representative")]
    #[test_case("missing"; "missing_covered_boundary")]
    #[test_case("history"; "short_history")]
    #[test_case("epoch"; "wrong_epoch")]
    #[test_case("offset"; "invalid_offset")]
    #[test_case("nested"; "recursive_snapshot")]
    fn malformed_grouped_boundary_payload_is_rejected(damage: &str) {
        let agent = AgentId::generate();
        let (mut prefix, mut payload) = pooled_boundary(agent);
        match damage {
            "format" => payload["format"] = json!("unknown"),
            "missing_format" => {
                payload.as_object_mut().unwrap().remove("format");
            }
            "format_type" => payload["format"] = json!(1),
            "missing_boundaries" => {
                payload.as_object_mut().unwrap().remove("boundaries");
            }
            "boundaries_type" => payload["boundaries"] = json!(1),
            "empty" => payload["boundaries"] = json!([]),
            "order" => payload["boundaries"] = json!([2, 1]),
            "representative" => payload["boundaries"] = json!([2]),
            "missing" => payload["boundaries"] = json!([1, 9]),
            "history" => payload["histories"][agent.to_string()]["messages"] = json!([]),
            "epoch" => payload["histories"][agent.to_string()]["epoch"] = json!(8),
            "offset" => payload["state"][1]["change"]["offset"] = json!(2),
            "nested" => {
                payload["state"][1]["change"] =
                    json!({"kind":"boundary_snapshot","boundary":1,"snapshot":{}})
            }
            _ => unreachable!(),
        }
        prefix.push(state(
            3,
            None,
            json!({"kind":"boundary_snapshot","boundary":1,"snapshot":payload}),
        ));
        assert!(matches!(
            SessionStateView::fold(&prefix, &[], u64::MAX),
            Err(super::StateFoldError::InvalidBoundarySnapshot { .. })
        ));
    }

    #[test_case(0, json!({}); "missing_boundary")]
    #[test_case(2, json!({}); "same_sequence")]
    #[test_case(3, json!({}); "future_boundary")]
    #[test_case(1, Value::Null; "null_snapshot")]
    #[test_case(1, json!([]); "array_snapshot")]
    fn malformed_boundary_snapshot_is_rejected(boundary: u64, snapshot: Value) {
        let agent = AgentId::generate();
        let records = [
            state(
                1,
                Some(agent),
                json!({"kind":"boundary","offset":0,"lens":{}}),
            ),
            state(
                2,
                None,
                json!({"kind":"boundary_snapshot","boundary":boundary,"snapshot":snapshot}),
            ),
        ];
        assert_eq!(
            SessionStateView::fold(&records, &[], u64::MAX),
            Err(StateFoldError::InvalidBoundarySnapshot { seq: 2, boundary })
        );
    }

    #[test_case(json!({"kind":"boundary_snapshot","boundary":1}); "missing_payload")]
    #[test_case(json!({"kind":"boundary_snapshot","boundary":"one","snapshot":{}}); "invalid_boundary_type")]
    fn malformed_boundary_snapshot_record_does_not_deserialize(change: Value) {
        assert!(serde_json::from_value::<StateChange>(change).is_err());
    }

    #[test_case(false ; "released")]
    #[test_case(true ; "held_until_user_turn")]
    fn delivery_hold_survives_serde_and_fold(held: bool) {
        let agent = AgentId::generate();
        let turn = TurnId::generate();
        let mut delivery: StoredDelivery =
            serde_json::from_value(json!({"label":null,"result":result(agent, turn)})).unwrap();
        assert!(!delivery.held);
        delivery.held = held;
        let record = state(
            1,
            Some(agent),
            json!({"kind":"delivery", "id":[agent, turn],
            "op":{"op":"queued", "delivery":delivery}}),
        );
        let view = fold(&[record]);
        let StoredDeliveryOp::Queued(restored) =
            &view.scopes[&Some(agent)].deliveries[&(agent, turn)]
        else {
            panic!("queued delivery missing")
        };
        assert_eq!(restored.held, held);
    }

    #[test_case(0 ; "empty_prefix")]
    #[test_case(2 ; "inclusive_config")]
    #[test_case(3 ; "inclusive_boundary")]
    #[test_case(4 ; "admitted")]
    #[test_case(6 ; "inclusive_terminal")]
    #[test_case(u64::MAX ; "latest")]
    fn historical_fold(through: u64) {
        let root = AgentId::generate();
        let turn = TurnId::generate();
        let first = agent(root, 1);
        let mut closed = first.clone();
        closed.updated_seq = 5;
        closed.lifecycle = StoredAgentLifecycle::Closed;
        let mut config = config();
        config["kind"] = json!("config");
        let records = [
            state(2, Some(root), config),
            state(
                3,
                Some(root),
                json!({"kind":"boundary","offset":4,"lens":{root.to_string():[1,4]}}),
            ),
            state(
                4,
                Some(root),
                json!({"kind":"admitted","turn":turn,"origin":"user"}),
            ),
            state(
                6,
                Some(root),
                json!({"kind":"turn","turn":turn,"result":result(root,turn)}),
            ),
        ];
        assert_eq!(
            serde_json::from_value::<StoredAgentRecord>(serde_json::to_value(&first).unwrap())
                .unwrap(),
            first
        );
        let view = SessionStateView::fold(&records, &[(1, first), (5, closed)], through).unwrap();
        assert_eq!(view.agents.contains_key(&root), through >= 1);
        if let Some(agent) = view.agents.get(&root) {
            assert_eq!(agent.updated_seq, if through >= 5 { 5 } else { 1 });
        }
        assert_eq!(view.boundaries.contains_key(&3), through >= 3);
        if let Some(scope) = view.scopes.get(&Some(root)) {
            assert_eq!(scope.config.as_ref().unwrap().cwd, "/saved");
            assert_eq!(!scope.results.is_empty(), through >= 6);
            assert_eq!(
                scope.admitted.contains_key(&turn),
                (4..6).contains(&through)
            );
        }
    }

    #[test_case("consumed" ; "consumed")]
    #[test_case("withdrawn" ; "withdrawn")]
    #[test_case("dropped" ; "dropped")]
    fn compaction_preserves_other_scopes_boundaries_and_tombstones(terminal: &str) {
        let root = AgentId::generate();
        let child = AgentId::generate();
        let turn = TurnId::generate();
        let queued = json!({"kind":"delivery","id":[child,turn],"op":{"op":"queued","delivery":{"label":null,"result":result(child,turn)}}});
        let records = [
            (None, json!({"kind":"rules","rules":[{"tool":"write","effect":"deny"}],"yolo":false})),
            (Some(child), json!({"kind":"context","source":"instructions","hash":"old","content":"old"})),
            (Some(root), json!({"kind":"file","path":"src/lib.rs","before":{"state":"absent"},"after":{"state":"absent"}})),
            (Some(root), queued.clone()),
            (Some(root), json!({"kind":"boundary","offset":0,"lens":{root.to_string():[1,0],child.to_string():[1,0]}})),
            (Some(child), json!({"kind":"boundary","offset":0,"lens":{child.to_string():[1,0]}})),
            (Some(root), json!({"kind":"delivery","id":[child,turn],"op":{"op":terminal}})),
            (Some(root), json!({"kind":"compaction","epoch":2})),
            (Some(root), queued),
            (Some(child), json!({"kind":"context","source":"instructions","hash":"new","content":"new"})),
        ].into_iter().enumerate().map(|(index, (agent, change))| state(index as u64 + 1, agent, change)).collect::<Vec<_>>();
        let view = fold(&records);
        assert_eq!(view.scopes[&None].rules.len(), 1);
        assert_eq!(view.scopes[&None].yolo, Some(false));
        assert_eq!(
            view.scopes[&Some(child)].context["instructions"].hash,
            "new"
        );
        assert_eq!(
            view.scopes[&Some(root)].files[Path::new("src/lib.rs")].before,
            FileState::Absent
        );
        assert_eq!(
            serde_json::to_value(&view.scopes[&Some(root)].deliveries[&(child, turn)]).unwrap(),
            json!({"op":terminal})
        );
        assert_eq!(view.compactions[&root], (8, 2));
        assert!(view.boundaries.contains_key(&5));
        assert!(!view.boundary_available(5));
        assert!(view.boundary_available(6));
        assert!(!view.boundary_available(999));
        assert!(
            SessionStateView::fold(&records, &[], 5)
                .unwrap()
                .boundary_available(5)
        );
    }

    #[test_case(None ; "deletion")]
    #[test_case(Some(Value::Null) ; "explicit_null")]
    #[test_case(Some(json!({"items":[1,true]})) ; "object")]
    fn plugin_null_deletion_and_scope(value: Option<Value>) {
        let root = AgentId::generate();
        let mut last = json!({"kind":"plugin","key":SLOT,"version":2});
        if let Some(value) = &value {
            last["value"] = value.clone();
        }
        let records = [
            state(
                1,
                None,
                json!({"kind":"plugin","key":SLOT,"version":1,"value":"session"}),
            ),
            state(
                2,
                Some(root),
                json!({"kind":"plugin","key":SLOT,"version":1,"value":"old"}),
            ),
            state(3, Some(root), last),
        ];
        let view = fold(&records);
        assert_eq!(
            view.scopes[&None].plugins[SLOT].value,
            Some(json!("session"))
        );
        let stored = &view.scopes[&Some(root)].plugins[SLOT];
        assert_eq!(stored.value, value);
        assert_eq!((stored.revision, stored.version), (3, 2));
    }

    #[test]
    fn turn_results_evict_by_commit_order_per_agent() {
        let root = AgentId::generate();
        let child = AgentId::generate();
        let child_turn = TurnId::generate();
        let mut records = vec![state(
            0,
            Some(child),
            json!({"kind":"turn","turn":child_turn,"result":result(child,child_turn)}),
        )];
        let turns: Vec<_> = (0..=MAX_STORED_TURN_RESULTS)
            .map(|_| TurnId::generate())
            .collect();
        for (index, turn) in turns.iter().enumerate() {
            records.push(state(
                index as u64 + 1,
                Some(root),
                json!({"kind":"turn","turn":turn,"result":result(root,*turn)}),
            ));
        }
        let view = fold(&records);
        let results = &view.scopes[&Some(root)].results;
        assert_eq!(results.len(), MAX_STORED_TURN_RESULTS);
        assert_eq!(results.front().unwrap().0, turns[1]);
        assert_eq!(
            view.scopes[&Some(child)].results.front().unwrap().0,
            child_turn
        );
    }

    #[test_case(vec![2, 1] ; "decreasing")]
    #[test_case(vec![1, 1] ; "duplicate")]
    fn invalid_sequence_order_rejected_even_beyond_cutoff(seqs: Vec<u64>) {
        let root = AgentId::generate();
        let records: Vec<_> = seqs
            .iter()
            .map(|seq| state(*seq, Some(root), json!({"kind":"compaction","epoch":2})))
            .collect();
        let revisions: Vec<_> = seqs.iter().map(|seq| (*seq, agent(root, *seq))).collect();
        for (stream, folded) in [
            ("state", SessionStateView::fold(&records, &[], 0)),
            ("agent", SessionStateView::fold(&[], &revisions, 0)),
        ] {
            let expected = StateFoldError::UnorderedSequence {
                stream,
                previous: seqs[0],
                seq: seqs[1],
            };
            assert_eq!(folded, Err(expected));
        }
    }

    #[test_case(false; "state_suffix")]
    #[test_case(true; "agent_suffix")]
    fn cutoff_still_validates_raw_suffix_order(agent_stream: bool) {
        let root = AgentId::generate();
        let state_records = [
            state(2, Some(root), json!({"kind":"compaction","epoch":2})),
            state(1, Some(root), json!({"kind":"compaction","epoch":1})),
        ];
        let revisions = [(2, agent(root, 2)), (1, agent(root, 1))];
        let (state, agents) = if agent_stream {
            (&[][..], &revisions[..])
        } else {
            (&state_records[..], &[][..])
        };
        assert_eq!(
            SessionStateView::fold(state, agents, 0),
            Err(StateFoldError::UnorderedSequence {
                stream: if agent_stream { "agent" } else { "state" },
                previous: 2,
                seq: 1
            })
        );
    }

    #[test]
    fn cutoff_ignores_invalid_payload_but_reducer_failure_is_atomic() {
        let valid = state(
            1,
            None,
            json!({"kind":"plugin","key":SLOT,"version":1,"value":true}),
        );
        let invalid = state(2, None, json!({"kind":"compaction","epoch":2}));
        let mut reducer = SessionStateReducer::from_ordered(from_ref(&valid), &[]).unwrap();
        let expected = reducer.materialize();
        assert_eq!(
            SessionStateView::fold(&[valid, invalid.clone()], &[], 1).unwrap(),
            expected
        );
        assert_eq!(
            reducer.apply_record(&invalid),
            Err(StateFoldError::MissingCompactionAgent { seq: 2 })
        );
        assert_eq!(reducer.materialize(), expected);
    }

    #[test]
    fn invalid_records_return_typed_errors() {
        let root = AgentId::generate();
        let child = AgentId::generate();
        let turn = TurnId::generate();
        let cases = [
            (
                None,
                json!({"kind":"compaction","epoch":2}),
                StateFoldError::MissingCompactionAgent { seq: 1 },
            ),
            (
                Some(root),
                json!({"kind":"turn","turn":turn,"result":result(child,turn)}),
                StateFoldError::TurnIdentityMismatch { seq: 1 },
            ),
            (
                Some(root),
                json!({"kind":"delivery","id":[child,turn],"op":{"op":"queued","delivery":{"label":null,"result":result(root,turn)}}}),
                StateFoldError::DeliveryIdentityMismatch { seq: 1 },
            ),
        ];
        for (agent, change, error) in cases {
            let record = state(1, agent, change);
            assert_eq!(super::validate_record(&record), Err(error.clone()));
            assert_eq!(SessionStateView::fold(&[record], &[], 1), Err(error));
        }
        let records = [state(1, Some(root), json!({"kind":"compaction","epoch":2}))];
        assert_eq!(
            SessionStateView::fold(&records, &[(1, agent(root, 1))], 0),
            Err(StateFoldError::DuplicateSequence { seq: 1 })
        );
        let mut revision = agent(root, 2);
        revision.updated_seq = 1;
        for seq in [1, 2] {
            let expected = StateFoldError::InvalidAgentRevision { seq };
            assert_eq!(
                SessionStateView::fold(&[], &[(seq, revision.clone())], 0),
                Err(expected)
            );
        }
    }

    #[test_case(MAX_PLUGIN_VALUE_DEPTH, true, true ; "depth_limit")]
    #[test_case(MAX_PLUGIN_VALUE_DEPTH + 1, true, false ; "depth_over_limit")]
    #[test_case(MAX_PLUGIN_VALUE_BYTES - 2, false, true ; "encoded_byte_limit")]
    #[test_case(MAX_PLUGIN_VALUE_BYTES - 1, false, false ; "encoded_byte_over_limit")]
    fn plugin_limits(size: usize, depth: bool, accepted: bool) {
        let mut value = if depth {
            Value::Null
        } else {
            json!("x".repeat(size))
        };
        if depth {
            for _ in 0..size {
                value = Value::Array(vec![value]);
            }
        }
        let record = StoredStateRecord {
            seq: 1,
            agent: None,
            generation: 1,
            change: StateChange::Plugin {
                key: SLOT.into(),
                version: 1,
                value: Some(value),
            },
        };
        let validation = super::validate_record(&record);
        let folded = SessionStateView::fold(&[record], &[], 1);
        if accepted {
            assert!(validation.is_ok());
            assert!(folded.is_ok());
        } else {
            let expected = if depth {
                StateFoldError::PluginDepth { seq: 1 }
            } else {
                StateFoldError::PluginSize { seq: 1 }
            };
            assert_eq!(validation, Err(expected.clone()));
            assert_eq!(folded, Err(expected));
        }
    }

    #[test_case(None ; "unrestricted")]
    #[test_case(Some(vec![]) ; "no_tools")]
    #[test_case(Some(vec!["read".into()]) ; "selected_tools")]
    fn required_mode_snapshot_preserves_optional_tools(tools: Option<Vec<String>>) {
        let mut encoded = config();
        encoded["mode"]["tools"] = json!(tools);
        let config: StoredAgentConfig = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(config.mode.tools, tools);
        assert_eq!(serde_json::to_value(config).unwrap(), encoded);
        encoded.as_object_mut().unwrap().remove("mode");
        assert!(serde_json::from_value::<StoredAgentConfig>(encoded).is_err());
    }

    #[test_case(json!({"status":"completed","reason":"max_turns"}) ; "completed")]
    #[test_case(json!({"status":"cancelled","reason":"interrupted"}) ; "interrupted")]
    #[test_case(json!({"status":"failed","failure":{"kind":"provider","diagnostic":"failed","user_message":"failed","retryable":true}}) ; "failed")]
    fn typed_results_and_offload_round_trip(status: Value) {
        let root = AgentId::generate();
        let mut encoded = result(root, TurnId::generate());
        encoded["outcome"].as_object_mut().unwrap().remove("reason");
        encoded["outcome"]
            .as_object_mut()
            .unwrap()
            .extend(status.as_object().unwrap().clone());
        let offload = json!({"storage":"offloaded","value":{"session":MakimaId::generate(),"path":"result.json","bytes":100000}});
        encoded["text"] = offload.clone();
        encoded["output"] = offload;
        let result: StoredTurnResult = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(serde_json::to_value(result).unwrap(), encoded);
    }
}
