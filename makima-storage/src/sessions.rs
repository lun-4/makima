//! Session persistence with append-only JSONL log format.
//!
//! Each session is stored as `{id}.jsonl`, one JSON record per line. An incomplete
//! final line is ignored on load and truncated before append. Complete records,
//! including replaced contexts, remain in the log.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

use tracing::warn;

use crate::conversation::ConversationState;
use crate::id::{AgentId, MakimaId};
use crate::offload_cleanup::Root;
use crate::plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot};
use crate::session_state::{
    ParentLink, SessionStateView, StateChange, StateFoldError, StoredAgentConfig,
    StoredAgentLifecycle, StoredAgentRecord, StoredDeliveryOp, StoredModeSnapshot,
    StoredStateRecord, StoredTurnResult,
};
use makima_domain::TokenUsage;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub use makima_domain::{
    Effort, MIN_THINKING_BUDGET, Post, PostLevel, StoredThinking, ThinkingParseError,
};

use crate::{StateDir, StorageError, atomic_write, now_epoch, session_lock};

const LOG_FORMAT_VERSION: u32 = 3;
const LEGACY_LOG_FORMAT_VERSION: u32 = 2;
const LEGACY_CHILD_UNRESTORABLE: &str = "legacy child has no stored spawn specification";
const LEGACY_PLAN_SLOT: &str = "makima.plan";
pub const SESSIONS_DIR: &str = "sessions";
const CWD_INDEX_FILE: &str = "cwd_latest.json";
const SCAN_CACHE_FILE: &str = "scan_cache.json";
const SCAN_CACHE_VERSION: u32 = 3;
#[cfg(test)]
const TAIL_BUF: u64 = 4096;
const DEFAULT_TITLE: &str = "New session";
const MAX_TITLE_LEN: usize = 60;
const FILE_CHANGED_UNDERNEATH: &str = "file changed underneath";
/// Original v2 logs retained during one-time migration.
const ARCHIVE_DIR: &str = "archive";
/// Full tool output saved instead of being cut, as `offload/<id>/`. It goes
/// with the session, so references to it in the history stay valid.
pub const OFFLOAD_DIR: &str = "offload";
const MSG_PREFIX: &[u8] = br#"{"t":"msg""#;

/// Hands out the token that tags one append-only run of a message list.
/// Process wide, so two runs never pick the same number.
static EPOCH: AtomicU64 = AtomicU64::new(1);

pub fn next_epoch() -> u64 {
    EPOCH.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("incompatible session version {found} (expected {expected})")]
    VersionMismatch { found: u32, expected: u32 },
    #[error("session requires unsupported features: {features:?}")]
    UnsupportedFeatures { features: BTreeSet<String> },
    #[error(transparent)]
    InvalidState(#[from] StateFoldError),
    #[error("session root ID mismatch: stored {stored_id}, got {given_id}")]
    RootIdMismatch {
        stored_id: AgentId,
        given_id: AgentId,
    },
    #[error("session ID mismatch: log owns {log_id}, got {given_id}")]
    IdMismatch {
        log_id: MakimaId,
        given_id: MakimaId,
    },
    #[error("session log {path} has no valid header")]
    MissingHeader { path: String },
    #[error("session log diverged ({reason}); rewrite required")]
    LogDiverged { reason: &'static str },
    #[error("fork target {target} is not an earlier root message")]
    InvalidForkTarget { target: MakimaId },
    #[error("session is open in another terminal; close it there first")]
    OpenElsewhere,
    #[error(
        "session log {path} line {line} holds a record this makima cannot read ({reason}); \
         the session is not loaded so that saving it cannot drop that record"
    )]
    UnreadableRecord {
        path: String,
        line: usize,
        reason: String,
    },
}

/// Per-model token breakdown entry. Mirrors the four usage counters tracked by
/// the active provider; kept storage-local to avoid a circular dependency on
/// `makima-providers`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct StoredTokenUsage {
    #[serde(default)]
    pub input: u32,
    #[serde(default)]
    pub output: u32,
    #[serde(default)]
    pub cache_creation: u32,
    #[serde(default)]
    pub cache_read: u32,
    /// What the turns billed, in USD. Prices move (some providers by the hour),
    /// so re-pricing these counters later would be fiction. `None` on unpriced
    /// models, and on entries written before we recorded it until the next load
    /// settles an estimate into them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
}

impl StoredTokenUsage {
    pub fn total_input(&self) -> u32 {
        self.input
            .saturating_add(self.cache_read)
            .saturating_add(self.cache_creation)
    }

    pub fn total(&self) -> u32 {
        self.total_input().saturating_add(self.output)
    }
}

impl std::ops::AddAssign for StoredTokenUsage {
    fn add_assign(&mut self, rhs: Self) {
        self.input = self.input.saturating_add(rhs.input);
        self.output = self.output.saturating_add(rhs.output);
        self.cache_creation = self.cache_creation.saturating_add(rhs.cache_creation);
        self.cache_read = self.cache_read.saturating_add(rhs.cache_read);
        add_cost(&mut self.cost, rhs.cost);
    }
}

/// The one way costs are summed, re-exported by `makima-providers` so every
/// running total agrees: `None` until the first priced turn shows up, and from
/// there it only grows.
pub fn add_cost(total: &mut Option<f64>, addend: Option<f64>) {
    if let Some(addend) = addend {
        *total = Some(total.unwrap_or_default() + addend);
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionMeta {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub compacted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<ConversationState>,
    #[serde(default, skip_serializing)]
    pub mode: Option<StoredMode>,
    #[serde(default, skip_serializing)]
    pub session_rules: Vec<StoredRule>,
    #[serde(default)]
    pub context_size: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_draft: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub queued_messages: Vec<String>,
    #[serde(default, skip_serializing)]
    pub thinking: Option<StoredThinking>,
    #[serde(default, skip_serializing)]
    pub fast: bool,
    #[serde(default, skip_serializing)]
    pub workflow: bool,
    /// `None` when the user never set yolo for this session, which is what
    /// makes `--yolo` a property of the invocation rather than of the log.
    #[serde(default, skip_serializing)]
    pub yolo: Option<bool>,
    #[serde(default, skip_serializing)]
    pub session_options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct LegacyPlan {
    #[serde(default, skip_serializing)]
    plan_path: Option<PathBuf>,
    #[serde(default, skip_serializing)]
    plan_written: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyConfig {
    mode: Option<StoredMode>,
    thinking: Option<StoredThinking>,
    fast: bool,
    workflow: bool,
    session_options: BTreeMap<String, String>,
    session_rules: Vec<StoredRule>,
    yolo: Option<bool>,
}

impl LegacyConfig {
    fn from_meta(meta: &SessionMeta) -> Self {
        Self {
            mode: meta.mode.clone(),
            thinking: meta.thinking,
            fast: meta.fast,
            workflow: meta.workflow,
            session_options: meta.session_options.clone(),
            session_rules: meta.session_rules.clone(),
            yolo: meta.yolo,
        }
    }

    fn restore(&self, meta: &mut SessionMeta) {
        meta.mode = self.mode.clone();
        meta.thinking = self.thinking;
        meta.fast = self.fast;
        meta.workflow = self.workflow;
        meta.session_options = self.session_options.clone();
        meta.session_rules = self.session_rules.clone();
        meta.yolo = self.yolo;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryDestination {
    Main,
    Child(String),
}

/// Messages plus the token of the run they belong to. Comparing tokens tells
/// an append from a rewrite, with no need to diff the lists.
#[derive(Debug, Clone)]
pub struct HistorySnapshot<M> {
    pub epoch: u64,
    pub compacted: bool,
    pub conversation: Option<ConversationState>,
    pub messages: Arc<Vec<M>>,
    pub ids: Arc<Vec<MakimaId>>,
    pub covered: Option<MakimaId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryIdentity {
    epoch: u64,
    len: usize,
}

impl<M> HistorySnapshot<M> {
    pub fn new(messages: Vec<M>) -> Self {
        Self {
            epoch: next_epoch(),
            compacted: false,
            conversation: None,
            ids: Arc::new(messages.iter().map(|_| MakimaId::generate()).collect()),
            covered: None,
            messages: Arc::new(messages),
        }
    }
}

impl<M> Default for HistorySnapshot<M> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// Active context projections and retained append-only session events.
#[derive(Debug, Clone)]
pub struct Session<M, U, T> {
    pub id: MakimaId,
    pub title: String,
    pub cwd: String,
    pub model: String,
    messages: Arc<Vec<M>>,
    message_ids: Vec<MakimaId>,
    context_events: Vec<serde_json::Value>,
    history_error: Option<String>,
    pub token_usage: U,
    tool_outputs: HashMap<String, Arc<T>>,
    subagent_messages: HashMap<String, Arc<Vec<M>>>,
    subagent_history_epochs: HashMap<String, u64>,
    subagent_conversations: HashMap<String, Arc<str>>,
    subagents: Vec<StoredSubagent>,
    posts: Vec<StoredPost>,
    state: Vec<StoredStateRecord>,
    sequence_high_water: Option<u64>,
    agent_revisions: Vec<(u64, StoredAgentRecord)>,
    agents: BTreeMap<AgentId, StoredAgentRecord>,
    legacy_root_unpublished: bool,
    legacy_config: Option<LegacyConfig>,
    parent: Option<ParentLink>,
    features: BTreeSet<String>,
    usage_by_model: HashMap<String, StoredTokenUsage>,
    pub meta: SessionMeta,
    pub created_at: u64,
    pub updated_at: u64,
    /// Bumped by every mutation, so a checkpoint knows if there is anything
    /// to write.
    revision: u64,
    /// Bumped by every mutation except `meta`, so a checkpoint can tell a tool
    /// result, which has to reach disk now, from a keystroke in the draft,
    /// which can wait for the keystrokes behind it.
    content_revision: u64,
    /// Transient producer lineage used to reject stale history projections.
    epoch: u64,
    rewrites: u64,
    checkpoint_required: Arc<AtomicBool>,
}

#[derive(Serialize)]
pub struct SessionSummary {
    pub id: MakimaId,
    pub title: String,
    pub updated_at: u64,
    pub cwd: String,
    pub message_count: usize,
    pub open_elsewhere: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredEffect {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StoredMode {
    Build,
    Plan,
    Custom(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRule {
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub effect: StoredEffect,
}

/// Global user preferences persisted across sessions, distinct from per-session
/// meta. The default thinking overlay lets `/thinking <mode>` at runtime pin a
/// global default without editing `init.lua`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Prefs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_thinking: Option<StoredThinking>,
}

const PREFS_FILE: &str = "prefs.json";

pub fn read_prefs(state_dir: &StateDir) -> Prefs {
    let path = state_dir.path().join(PREFS_FILE);
    fs::read_to_string(&path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

pub fn write_prefs(state_dir: &StateDir, prefs: &Prefs) -> Result<(), StorageError> {
    let path = state_dir.path().join(PREFS_FILE);
    atomic_write(&path, &serde_json::to_vec_pretty(prefs)?)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSubagent {
    pub tool_use_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub ambiguous: bool,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Absent on subagents written before this was recorded, and the one flag
    /// that says whether `fast` below is the subagent's or just a default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking: Option<StoredThinking>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fast: bool,
}

fn mark_duplicate_subagent_identities(subagents: &mut [StoredSubagent]) {
    let mut keys = HashMap::new();
    let mut ids = HashMap::new();
    for child in subagents.iter() {
        *keys.entry(child.tool_use_id.clone()).or_insert(0usize) += 1;
        if let Some(id) = &child.agent_id {
            *ids.entry(id.clone()).or_insert(0usize) += 1;
        }
    }
    for child in subagents {
        if child.ambiguous
            || keys[&child.tool_use_id] > 1
            || child.agent_id.as_ref().is_some_and(|id| ids[id] > 1)
        {
            child.ambiguous = true;
            child.agent_id = None;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredPost {
    /// Transcript id of the chat holding the post; `None` is the main chat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chat: Option<String>,
    #[serde(default, deserialize_with = "post_message_id")]
    pub after_message: Option<MakimaId>,
    #[serde(flatten)]
    pub post: Post,
}

fn post_message_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<MakimaId>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(id) => id.parse().map(Some).map_err(serde::de::Error::custom),
        serde_json::Value::Null | serde_json::Value::Number(_) => Ok(None),
        _ => Err(serde::de::Error::custom("invalid post message id")),
    }
}

pub trait TitleSource {
    fn first_user_text(&self) -> Option<&str>;
}

/// A pasted code block bakes `\n` into a title and skews width-based padding
/// in single-line UI like the picker, so every title entry point calls this.
pub fn normalize_title(title: &str) -> String {
    title.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn generate_title<M: TitleSource>(messages: &[M]) -> String {
    let first_user_text = messages.iter().find_map(|m| m.first_user_text());

    let Some(text) = first_user_text.map(str::trim).filter(|t| !t.is_empty()) else {
        return DEFAULT_TITLE.into();
    };
    let text = normalize_title(text);

    if text.len() <= MAX_TITLE_LEN {
        return text;
    }

    let boundary = text.floor_char_boundary(MAX_TITLE_LEN);
    let truncated = &text[..boundary];
    match truncated.rfind(' ') {
        Some(pos) if pos > MAX_TITLE_LEN / 2 => format!("{}…", &truncated[..pos]),
        _ => format!("{truncated}…"),
    }
}

// -- JSONL record types --

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "t")]
pub enum SessionRecord<M, U, T> {
    #[serde(rename = "header")]
    Header {
        v: u32,
        id: MakimaId,
        model: String,
        cwd: String,
        created_at: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent: Option<ParentLink>,
        #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
        features: BTreeSet<String>,
    },
    #[serde(rename = "state")]
    State { d: StoredStateRecord },
    #[serde(rename = "agent")]
    Agent { seq: u64, d: StoredAgentRecord },
    #[serde(rename = "msg")]
    Msg {
        #[serde(default = "MakimaId::generate")]
        id: MakimaId,
        #[serde(default = "AgentId::generate")]
        agent: AgentId,
        d: M,
    },
    #[serde(rename = "input")]
    Input {
        d: StoredStateRecord,
        messages: Vec<(MakimaId, M)>,
    },
    #[serde(rename = "compaction")]
    Compaction {
        agent: AgentId,
        messages: Vec<M>,
        #[serde(default)]
        ids: Vec<MakimaId>,
        #[serde(default)]
        covered: Option<MakimaId>,
    },
    #[serde(rename = "fork")]
    Fork {
        target: Option<MakimaId>,
        note_id: MakimaId,
        note: String,
        conversation: ConversationState,
    },
    #[serde(rename = "history")]
    History {
        agent: AgentId,
        compacted: bool,
        conversation: Option<ConversationState>,
    },
    #[serde(rename = "out")]
    Out { id: String, d: T },
    #[serde(rename = "sub_msg")]
    SubMsg { sub: String, d: M },
    #[serde(rename = "post")]
    Post { d: StoredPost },
    #[serde(rename = "draft")]
    Draft { text: Option<String> },
    #[serde(rename = "queued")]
    Queued { messages: Vec<String> },
    #[serde(rename = "title")]
    Title { title: String },
    #[serde(rename = "usage")]
    Usage {
        token_usage: U,
        usage_by_model: HashMap<String, StoredTokenUsage>,
    },
    #[serde(rename = "meta")]
    Meta {
        title: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent: Option<ParentLink>,
        #[serde(default)]
        parent_recorded: bool,
        #[serde(default)]
        message_count: usize,
        token_usage: U,
        updated_at: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence_high_water: Option<u64>,
        #[serde(default, skip_serializing)]
        history_epochs: BTreeMap<AgentId, u64>,
        #[serde(default, skip_serializing)]
        subagents: Vec<StoredSubagent>,
        #[serde(default, skip_serializing)]
        subagent_conversations: HashMap<String, Arc<str>>,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
        usage_by_model: HashMap<String, StoredTokenUsage>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        legacy_config: Option<LegacyConfig>,
        #[serde(flatten)]
        meta: Box<SessionMeta>,
        #[serde(flatten)]
        legacy_plan: LegacyPlan,
    },
}

fn sync_dir_and_ancestors(path: &Path) -> Result<(), StorageError> {
    #[cfg(unix)]
    {
        File::open(path)?.sync_all()?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
            if let Some(ancestor) = parent.parent() {
                File::open(ancestor)?.sync_all()?;
            }
        }
    }
    #[cfg(not(unix))]
    crate::sync_parent_dir(path);
    Ok(())
}

/// Initializes a new log or migrates v2. Existing v3 logs are never changed here.
pub fn initialize_session_log<M: Serialize, U: Serialize, T: Serialize>(
    directory: &Path,
    session: &Session<M, U, T>,
) -> Result<(), SessionError> {
    let path = jsonl_path(directory, session.id);
    match fs::metadata(&path) {
        Ok(metadata) if metadata.len() > 0 => migrate_v2_before_append(directory, session),
        Ok(_) => initialize_empty_log(directory, session),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            initialize_empty_log(directory, session)
        }
        Err(error) => Err(StorageError::from(error).into()),
    }
}

fn initialize_empty_log<M: Serialize, U: Serialize, T: Serialize>(
    directory: &Path,
    session: &Session<M, U, T>,
) -> Result<(), SessionError> {
    fs::create_dir_all(directory).map_err(StorageError::from)?;
    let mut bytes = Vec::new();
    write_full_session(&mut bytes, session)?;
    atomic_write(&jsonl_path(directory, session.id), &bytes)?;
    update_cwd_index(directory, &session.cwd, session.id)?;
    sync_dir_and_ancestors(directory)?;
    Ok(())
}

/// Performs the only supported full rewrite, archiving an existing v2 log first.
pub fn migrate_v2_before_append<M: Serialize, U: Serialize, T: Serialize>(
    directory: &Path,
    session: &Session<M, U, T>,
) -> Result<(), SessionError> {
    let path = jsonl_path(directory, session.id);
    let existing = match fs::read(&path) {
        Ok(existing) => existing,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(StorageError::from(error).into()),
    };
    let legacy = complete_prefix(&existing)
        .split(|byte| *byte == b'\n')
        .next()
        .and_then(|line| serde_json::from_slice::<serde_json::Value>(line).ok())
        .is_some_and(|header| header["v"] == LEGACY_LOG_FORMAT_VERSION);
    if !legacy {
        return Ok(());
    }
    let archive = directory.join(ARCHIVE_DIR).join(session.id.to_string());
    fs::create_dir_all(&archive).map_err(StorageError::from)?;
    let archive_path = archive.join("v2.jsonl");
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&archive_path)
    {
        Ok(mut archived) => archived
            .write_all(&existing)
            .and_then(|()| archived.sync_all())
            .map_err(StorageError::from)?,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            if fs::read(&archive_path).map_err(StorageError::from)? != existing {
                return Err(SessionError::LogDiverged {
                    reason: FILE_CHANGED_UNDERNEATH,
                });
            }
        }
        Err(error) => return Err(StorageError::from(error).into()),
    }
    sync_dir_and_ancestors(&archive)?;
    let mut bytes = Vec::new();
    write_full_session(&mut bytes, session)?;
    atomic_write(&path, &bytes)?;
    sync_dir_and_ancestors(directory)?;
    Ok(())
}

fn complete_prefix(data: &[u8]) -> &[u8] {
    &data[..data
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |index| index + 1)]
}

/// Encodes a newline-terminated record for the session writer.
pub fn encode_record<R: Serialize>(record: &R) -> Result<Vec<u8>, SessionError> {
    let mut bytes = Vec::new();
    append_record(&mut bytes, record)?;
    Ok(bytes)
}

pub fn encode_records<R: Serialize>(records: &[R]) -> Result<Vec<u8>, SessionError> {
    let mut bytes = Vec::new();
    for record in records {
        append_record(&mut bytes, record)?;
    }
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContextEntry {
    Message { id: MakimaId, d: serde_json::Value },
    Note { id: MakimaId, note: String },
}

impl ContextEntry {
    pub fn id(&self) -> MakimaId {
        match self {
            Self::Message { id, .. } | Self::Note { id, .. } => *id,
        }
    }

    fn into_message<M: DeserializeOwned>(self) -> Result<M, serde_json::Error> {
        serde_json::from_value(match self {
            Self::Message { d, .. } => d,
            Self::Note { note, .. } => serde_json::json!({
                "role": "user",
                "kind": "observation",
                "content": [{"type": "text", "text": note}],
            }),
        })
    }
}

/// Replays a root's events, retaining the historical line at each message for redo.
pub fn context_entries(
    events: &[serde_json::Value],
    root: AgentId,
) -> Result<Vec<ContextEntry>, SessionError> {
    let agent = serde_json::json!(root);
    let mut nodes: Vec<(Option<usize>, ContextEntry)> = Vec::new();
    let mut head = None;
    let mut lines = HashMap::new();
    for event in events {
        if event["t"] == "fork" {
            let target: Option<MakimaId> =
                serde_json::from_value(event["target"].clone()).map_err(StorageError::from)?;
            head = target
                .map(|target| {
                    lines
                        .get(&target)
                        .copied()
                        .ok_or(SessionError::InvalidForkTarget { target })
                })
                .transpose()?;
            let id =
                serde_json::from_value(event["note_id"].clone()).map_err(StorageError::from)?;
            let note = serde_json::from_value(event["note"].clone()).map_err(StorageError::from)?;
            nodes.push((head, ContextEntry::Note { id, note }));
            head = Some(nodes.len() - 1);
            lines.insert(id, nodes.len() - 1);
            continue;
        }
        if event["agent"] != agent {
            continue;
        }
        match event["t"].as_str() {
            Some("msg") => {
                let id = serde_json::from_value(event["id"].clone()).map_err(StorageError::from)?;
                nodes.push((
                    head,
                    ContextEntry::Message {
                        id,
                        d: event["d"].clone(),
                    },
                ));
                head = Some(nodes.len() - 1);
                lines.insert(id, nodes.len() - 1);
            }
            Some("compaction") => {
                head = None;
                let ids: Vec<MakimaId> =
                    serde_json::from_value(event["ids"].clone()).map_err(StorageError::from)?;
                if let Some(messages) = event["messages"].as_array() {
                    for (id, d) in ids.into_iter().zip(messages) {
                        nodes.push((head, ContextEntry::Message { id, d: d.clone() }));
                        head = Some(nodes.len() - 1);
                        lines.insert(id, nodes.len() - 1);
                    }
                }
            }
            _ => {}
        }
    }
    let mut entries = Vec::new();
    while let Some(index) = head {
        let (previous, entry) = &nodes[index];
        entries.push(entry.clone());
        head = *previous;
    }
    entries.reverse();
    Ok(entries)
}

/// Builds active ids. Child event streams must exclude root fork records.
pub fn context_message_ids(events: &[serde_json::Value], agent: AgentId) -> Vec<MakimaId> {
    context_entries(events, agent)
        .map(|entries| entries.iter().map(ContextEntry::id).collect())
        .unwrap_or_default()
}

fn meta_record<M, U, T>(session: &Session<M, U, T>) -> Result<Vec<u8>, SessionError>
where
    M: Serialize,
    U: Serialize,
    T: Serialize,
{
    let history_epochs = BTreeMap::new();
    let mut buf = Vec::new();
    append_record(
        &mut buf,
        &SessionRecord::<&M, &U, &T>::Meta {
            title: session.title.clone(),
            parent: session.parent.clone(),
            parent_recorded: true,
            message_count: session.messages.len(),
            token_usage: &session.token_usage,
            updated_at: session.updated_at,
            sequence_high_water: session.sequence_high_water(),
            history_epochs,
            subagents: Vec::new(),
            subagent_conversations: HashMap::new(),
            usage_by_model: session.usage_by_model.clone(),
            legacy_config: if session.root_config()?.is_none() {
                session.legacy_config.clone()
            } else {
                None
            },
            meta: Box::new(session.meta.clone()),
            legacy_plan: LegacyPlan::default(),
        },
    )?;
    Ok(buf)
}

fn write_full_session<M, U, T>(
    file: &mut impl Write,
    session: &Session<M, U, T>,
) -> Result<(), SessionError>
where
    M: Serialize,
    U: Serialize,
    T: Serialize,
{
    if let Some(error) = &session.history_error {
        return Err(StorageError::Io(io::Error::other(error.clone())).into());
    }
    session.latest_state()?;
    let (model, cwd) = session.header_cache()?;
    let mut buf = Vec::new();
    append_record(
        &mut buf,
        &SessionRecord::<&M, &U, &T>::Header {
            v: LOG_FORMAT_VERSION,
            id: session.id,
            model,
            cwd,
            created_at: session.created_at,
            parent: session.parent.clone(),
            features: session.features.clone(),
        },
    )?;
    if session.context_events.is_empty() {
        for (index, msg) in session.messages.iter().enumerate() {
            append_record(
                &mut buf,
                &SessionRecord::<&M, &U, &T>::Msg {
                    id: session
                        .message_ids
                        .get(index)
                        .copied()
                        .unwrap_or_else(MakimaId::generate),
                    agent: session.message_agent(),
                    d: msg,
                },
            )?;
        }
    } else {
        for event in &session.context_events {
            append_record(&mut buf, event)?;
        }
    }
    for (id, output) in &session.tool_outputs {
        append_record(
            &mut buf,
            &SessionRecord::<&M, &U, &T>::Out {
                id: id.clone(),
                d: output,
            },
        )?;
    }
    for post in &session.posts {
        append_record(
            &mut buf,
            &SessionRecord::<&M, &U, &T>::Post { d: post.clone() },
        )?;
    }
    append_journal_records(&mut buf, &session.state, &session.agent_revisions)?;
    append_record(
        &mut buf,
        &SessionRecord::<&M, &U, &T>::History {
            agent: session.message_agent(),
            compacted: session.meta.compacted,
            conversation: session.meta.conversation.clone(),
        },
    )?;
    buf.extend_from_slice(&meta_record(session)?);
    file.write_all(&buf).map_err(StorageError::from)?;
    Ok(())
}

fn append_journal_records(
    buf: &mut Vec<u8>,
    state: &[StoredStateRecord],
    agents: &[(u64, StoredAgentRecord)],
) -> Result<(), SessionError> {
    let mut records: Vec<_> = state
        .iter()
        .map(|record| {
            (
                record.seq,
                SessionRecord::<(), (), ()>::State { d: record.clone() },
            )
        })
        .chain(agents.iter().map(|(seq, record)| {
            (
                *seq,
                SessionRecord::<(), (), ()>::Agent {
                    seq: *seq,
                    d: record.clone(),
                },
            )
        }))
        .collect();
    records.sort_by_key(|(seq, _)| *seq);
    for (_, record) in records {
        append_record(buf, &record)?;
    }
    Ok(())
}

fn rekey_result(result: &mut StoredTurnResult, previous: AgentId, id: AgentId) {
    if result.outcome.agent_id == previous {
        result.outcome.agent_id = id;
    }
    if result.provenance.source_agent == Some(previous) {
        result.provenance.source_agent = Some(id);
    }
}

fn root_record(id: AgentId, seq: u64) -> StoredAgentRecord {
    StoredAgentRecord {
        id,
        parent: None,
        depth: 0,
        transcript_key: String::new(),
        lifecycle: StoredAgentLifecycle::Open,
        ancestry_unknown: false,
        label: None,
        spawned_by_tool_use_id: None,
        spec: None,
        restorable: Ok(()),
        usage: TokenUsage::default(),
        created_seq: seq,
        updated_seq: seq,
        legacy: false,
    }
}

fn legacy_child(
    root: AgentId,
    transcript_key: String,
    label: Option<String>,
    spawned_by_tool_use_id: Option<String>,
    seq: u64,
) -> StoredAgentRecord {
    StoredAgentRecord {
        id: AgentId::generate(),
        parent: Some(root),
        depth: 1,
        transcript_key,
        lifecycle: StoredAgentLifecycle::Closed,
        ancestry_unknown: true,
        label,
        spawned_by_tool_use_id,
        spec: None,
        restorable: Err(LEGACY_CHILD_UNRESTORABLE.to_owned()),
        usage: TokenUsage::default(),
        created_seq: seq,
        updated_seq: seq,
        legacy: true,
    }
}

fn latest_agents(revisions: &[(u64, StoredAgentRecord)]) -> BTreeMap<AgentId, StoredAgentRecord> {
    let mut ordered: Vec<_> = revisions.iter().collect();
    ordered.sort_by_key(|(seq, _)| *seq);
    ordered
        .into_iter()
        .map(|(_, record)| (record.id, record.clone()))
        .collect()
}

pub fn append_record<R: Serialize>(buf: &mut Vec<u8>, record: &R) -> Result<(), SessionError> {
    let value = serde_json::to_value(record).map_err(StorageError::from)?;
    if let serde_json::Value::Object(mut object) = value {
        if let Some(tag) = object.remove("t") {
            buf.extend_from_slice(b"{\"t\":");
            serde_json::to_writer(&mut *buf, &tag).map_err(StorageError::from)?;
            for (key, value) in object {
                buf.push(b',');
                serde_json::to_writer(&mut *buf, &key).map_err(StorageError::from)?;
                buf.push(b':');
                serde_json::to_writer(&mut *buf, &value).map_err(StorageError::from)?;
            }
            buf.push(b'}');
        } else {
            serde_json::to_writer(&mut *buf, &object).map_err(StorageError::from)?;
        }
    } else {
        serde_json::to_writer(&mut *buf, &value).map_err(StorageError::from)?;
    }
    buf.push(b'\n');
    Ok(())
}

fn decode_log_record(mut record: serde_json::Value) -> serde_json::Value {
    let is_agent = record["t"] == "agent";
    if let Some(object) = record.as_object_mut() {
        if is_agent {
            if let Some(journal_seq) = object.remove("journal_seq") {
                object.insert("seq".into(), journal_seq);
            }
        } else {
            object.remove("seq");
        }
    }
    record
}

pub fn append_context_events(events: &mut Vec<serde_json::Value>, event: serde_json::Value) {
    match event["t"].as_str() {
        Some("msg" | "compaction" | "history" | "fork") => events.push(event),
        Some("input") => {
            if let Some(messages) = event["messages"].as_array() {
                for message in messages {
                    events.push(serde_json::json!({
                        "t": "msg", "agent": event["d"]["agent"], "id": message[0], "d": message[1],
                    }));
                }
            }
        }
        _ => {}
    }
}

fn load_jsonl<M, U, T>(data: &[u8], display_path: &str) -> Result<Session<M, U, T>, SessionError>
where
    M: DeserializeOwned,
    U: DeserializeOwned + Default,
    T: DeserializeOwned,
{
    let mut line_count = 0usize;

    let mut id: Option<MakimaId> = None;
    let mut model = String::new();
    let mut cwd = String::new();
    let mut created_at = 0u64;
    let mut messages: Vec<M> = Vec::new();
    let mut message_ids = Vec::new();
    let mut context_events = Vec::new();
    let mut agent_messages: BTreeMap<AgentId, Vec<M>> = BTreeMap::new();
    let mut agent_message_ids: BTreeMap<AgentId, Vec<MakimaId>> = BTreeMap::new();
    let mut tool_outputs = HashMap::new();
    let mut subagent_messages: HashMap<String, Vec<M>> = HashMap::new();
    let mut title = DEFAULT_TITLE.to_string();
    let mut token_usage = U::default();
    let mut updated_at = 0u64;
    let mut subagents = Vec::new();
    let mut subagent_conversations = HashMap::new();
    let mut posts = Vec::new();
    let mut usage_by_model = HashMap::new();
    let mut meta = SessionMeta::default();
    let mut legacy_config = None;
    let mut legacy_plan = LegacyPlan::default();
    let mut version = LOG_FORMAT_VERSION;
    let mut parent = None;
    let mut features = BTreeSet::new();
    let mut state = Vec::new();
    let mut agent_revisions = Vec::new();
    let mut sequence_high_water = None;
    let mut history_epochs = BTreeMap::new();
    for line in complete_prefix(data).split(|&b| b == b'\n') {
        line_count += 1;
        if line.is_empty() {
            continue;
        }
        if id.is_none()
            && let Ok(header) = serde_json::from_slice::<HeaderCompatibility>(line)
            && header.t == "header"
        {
            if !matches!(header.v, LEGACY_LOG_FORMAT_VERSION | LOG_FORMAT_VERSION) {
                return Err(SessionError::VersionMismatch {
                    found: header.v,
                    expected: LOG_FORMAT_VERSION,
                });
            }
            if !header.features.is_empty() {
                return Err(SessionError::UnsupportedFeatures {
                    features: header.features,
                });
            }
        }
        let record: SessionRecord<M, U, T> = match serde_json::from_slice::<serde_json::Value>(line)
            .and_then(|value| serde_json::from_value(decode_log_record(value)))
        {
            Ok(r) => r,
            Err(e) if id.is_some() => {
                return Err(SessionError::UnreadableRecord {
                    path: display_path.to_owned(),
                    line: line_count,
                    reason: e.to_string(),
                });
            }
            Err(e) => {
                warn!(
                    path = display_path,
                    error = %e,
                    line = line_count,
                    "skipping torn JSONL record",
                );
                continue;
            }
        };
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) {
            sequence_high_water = sequence_high_water.max(value["seq"].as_u64());
        }
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) {
            append_context_events(&mut context_events, decode_log_record(value));
        }
        match record {
            SessionRecord::Input { d, messages } => {
                let agent = d.agent.ok_or_else(|| SessionError::UnreadableRecord {
                    path: display_path.to_owned(),
                    line: line_count,
                    reason: "input record has no agent".into(),
                })?;
                for (id, message) in messages {
                    agent_messages.entry(agent).or_default().push(message);
                    agent_message_ids.entry(agent).or_default().push(id);
                }
                state.push(d);
            }
            SessionRecord::Header {
                v,
                id: h_id,
                model: h_model,
                cwd: h_cwd,
                created_at: h_created,
                parent: h_parent,
                features: h_features,
            } => {
                if !matches!(v, LEGACY_LOG_FORMAT_VERSION | LOG_FORMAT_VERSION) {
                    return Err(SessionError::VersionMismatch {
                        found: v,
                        expected: LOG_FORMAT_VERSION,
                    });
                }
                if !h_features.is_empty() {
                    return Err(SessionError::UnsupportedFeatures {
                        features: h_features,
                    });
                }
                version = v;
                parent = h_parent;
                features = h_features;
                id = Some(h_id);
                model = h_model;
                cwd = h_cwd;
                created_at = h_created;
            }
            SessionRecord::State { d } => state.push(d),
            SessionRecord::Agent { seq, d } => agent_revisions.push((seq, d)),
            SessionRecord::Msg {
                id: message_id,
                agent,
                d,
            } => {
                let unified = serde_json::from_slice::<serde_json::Value>(line)
                    .ok()
                    .is_some_and(|v| v.get("agent").is_some());
                if unified {
                    agent_messages.entry(agent).or_default().push(d);
                    agent_message_ids.entry(agent).or_default().push(message_id);
                } else {
                    if version == LOG_FORMAT_VERSION {
                        let root = latest_agents(&agent_revisions)
                            .values()
                            .find(|agent| agent.parent.is_none())
                            .map(|agent| agent.id)
                            .or_else(|| id.and_then(|id| id.to_string().parse().ok()))
                            .unwrap_or(agent);
                        agent_messages.entry(root).or_default().push(d);
                        agent_message_ids.entry(root).or_default().push(message_id);
                    } else {
                        messages.push(d);
                        message_ids.push(message_id);
                    }
                }
            }
            SessionRecord::Compaction {
                agent,
                messages: summary,
                ids,
                covered: _,
            } => {
                agent_message_ids.insert(
                    agent,
                    if ids.len() == summary.len() {
                        ids
                    } else {
                        summary.iter().map(|_| MakimaId::generate()).collect()
                    },
                );
                agent_messages.insert(agent, summary);
            }
            SessionRecord::History { .. } | SessionRecord::Fork { .. } => {}
            SessionRecord::Out { id: out_id, d } => {
                tool_outputs.insert(out_id, Arc::new(d));
            }
            SessionRecord::SubMsg { sub, d } => {
                subagent_messages.entry(sub).or_default().push(d);
            }
            SessionRecord::Post { d } => posts.push(d),
            SessionRecord::Draft { text } => meta.input_draft = text,
            SessionRecord::Queued { messages } => meta.queued_messages = messages,
            SessionRecord::Title { title: next_title } => title = next_title,
            SessionRecord::Usage {
                token_usage: usage,
                usage_by_model: by_model,
            } => {
                token_usage = usage;
                usage_by_model = by_model;
            }
            SessionRecord::Meta {
                title: m_title,
                parent: m_parent,
                parent_recorded,
                message_count: _,
                token_usage: m_usage,
                updated_at: m_updated,
                sequence_high_water: m_sequence_high_water,
                history_epochs: m_history_epochs,
                subagents: m_subagents,
                subagent_conversations: m_subagent_conversations,
                usage_by_model: m_usage_by_model,
                meta: m_meta,
                legacy_config: m_legacy_config,
                legacy_plan: m_legacy_plan,
            } => {
                title = m_title;
                if parent_recorded {
                    parent = m_parent;
                }
                token_usage = m_usage;
                updated_at = m_updated;
                sequence_high_water = sequence_high_water.max(m_sequence_high_water);
                history_epochs = m_history_epochs;
                subagents = m_subagents;
                subagent_conversations = m_subagent_conversations;
                usage_by_model = m_usage_by_model;
                meta = *m_meta;
                legacy_config = m_legacy_config;
                legacy_plan = m_legacy_plan;
            }
        }
    }

    let id = id.ok_or_else(|| SessionError::MissingHeader {
        path: display_path.to_string(),
    })?;

    mark_duplicate_subagent_identities(&mut subagents);
    let agents = latest_agents(&agent_revisions);
    let root = agents
        .values()
        .find(|agent| agent.parent.is_none())
        .map(|agent| agent.id)
        .or_else(|| {
            (version == LEGACY_LOG_FORMAT_VERSION)
                .then(|| {
                    context_events.iter().find_map(|event| {
                        event["agent"].as_str().and_then(|agent| agent.parse().ok())
                    })
                })
                .flatten()
        })
        .unwrap_or_else(|| id.to_string().parse().expect("matching id representation"));
    if let Some(root_messages) = agent_messages.remove(&root) {
        messages = root_messages;
        message_ids = agent_message_ids.remove(&root).unwrap_or_default();
    }
    if context_events.iter().any(|event| event["t"] == "fork") {
        let entries = context_entries(&context_events, root)?;
        message_ids = entries.iter().map(ContextEntry::id).collect();
        messages = entries
            .into_iter()
            .map(ContextEntry::into_message)
            .collect::<Result<_, _>>()
            .map_err(StorageError::from)?;
    }
    for agent in agents.values().filter(|agent| agent.parent.is_some()) {
        subagent_messages
            .entry(agent.transcript_key.clone())
            .or_default()
            .extend(agent_messages.remove(&agent.id).unwrap_or_default());
    }
    if version != LEGACY_LOG_FORMAT_VERSION {
        subagents = agents
            .values()
            .filter(|agent| agent.parent.is_some())
            .map(|agent| StoredSubagent {
                tool_use_id: agent.transcript_key.clone(),
                agent_id: Some(agent.id.to_string()),
                ambiguous: false,
                name: agent.label.clone().unwrap_or_default(),
                model: None,
                thinking: None,
                fast: false,
            })
            .collect();
    }
    let root_value = serde_json::json!(root);
    for event in &context_events {
        match event["t"].as_str() {
            Some("fork") => {
                meta.compacted = false;
                meta.conversation = Some(
                    serde_json::from_value(event["conversation"].clone())
                        .map_err(StorageError::from)?,
                );
            }
            Some("history") if event["agent"] == root_value => {
                meta.compacted = event["compacted"].as_bool().unwrap_or_default();
                meta.conversation = serde_json::from_value(event["conversation"].clone())
                    .map_err(StorageError::from)?;
            }
            _ => {}
        }
    }
    let mut session = Session {
        id,
        title,
        cwd,
        model,
        messages: Arc::new(messages),
        message_ids,
        context_events,
        history_error: None,
        token_usage,
        tool_outputs,
        subagent_messages: subagent_messages
            .into_iter()
            .map(|(id, msgs)| (id, Arc::new(msgs)))
            .collect(),
        subagent_history_epochs: HashMap::new(),
        subagent_conversations,
        subagents,
        posts,
        state,
        sequence_high_water,
        agents: latest_agents(&agent_revisions),
        legacy_root_unpublished: version == LEGACY_LOG_FORMAT_VERSION,
        legacy_config,
        agent_revisions,
        parent,
        features,
        usage_by_model,
        meta,
        created_at,
        updated_at,
        revision: 0,
        content_revision: 0,
        epoch: next_epoch(),
        rewrites: 0,
        checkpoint_required: Arc::new(AtomicBool::new(true)),
    };
    if version == LEGACY_LOG_FORMAT_VERSION {
        session.legacy_config = Some(LegacyConfig::from_meta(&session.meta));
        session.migrate_legacy();
    }
    if version == LOG_FORMAT_VERSION {
        session.refresh_children();
    }
    session.migrate_legacy_plan(legacy_plan)?;
    if session.root_config()?.is_none() {
        if let Some(legacy) = &session.legacy_config {
            legacy.restore(&mut session.meta);
        }
    } else {
        session.legacy_config = None;
    }
    if version == LEGACY_LOG_FORMAT_VERSION {
        let migrated_root = session.message_agent();
        let old_root = root;
        let root = migrated_root;
        let mut root_index = 0;
        for event in &mut session.context_events {
            if event["agent"] == serde_json::json!(old_root) {
                event["agent"] = serde_json::json!(root);
            }
            if event["t"] == "msg" && event.get("agent").is_none() {
                event["id"] = serde_json::json!(session.message_ids[root_index]);
                event["agent"] = serde_json::json!(root);
                root_index += 1;
            }
        }
        for line in complete_prefix(data).split(|byte| *byte == b'\n') {
            if let Ok(mut event) = serde_json::from_slice::<serde_json::Value>(line)
                && event["t"] == "sub_msg"
                && let Some(key) = event["sub"].as_str()
                && let Some(agent) = session
                    .agents
                    .values()
                    .find(|agent| agent.transcript_key == key)
            {
                event["t"] = serde_json::json!("msg");
                event["id"] = serde_json::json!(MakimaId::generate());
                event["agent"] = serde_json::json!(agent.id);
                if let Some(object) = event.as_object_mut() {
                    object.remove("sub");
                }
                session.context_events.push(event);
            }
        }
    }
    let legacy_post_positions: Vec<_> = complete_prefix(data)
        .split(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice::<serde_json::Value>(line).ok())
        .filter(|record| record["t"] == "post")
        .map(|record| record["d"]["after_message"].as_u64())
        .collect();
    for (index, position) in legacy_post_positions.into_iter().enumerate() {
        if let Some(position) = position.and_then(|position| usize::try_from(position).ok()) {
            let ids = session.chat_message_ids(session.posts[index].chat.as_deref());
            session.posts[index].after_message = position
                .checked_sub(1)
                .and_then(|index| ids.get(index).copied());
        }
    }
    session.restore_loaded_history_epochs(&history_epochs);
    (session.model, session.cwd) = session.header_cache()?;
    Ok(session)
}

#[cfg(test)]
fn remap_loaded_history_epochs(
    state: &mut [StoredStateRecord],
    canonical: &mut BTreeMap<AgentId, u64>,
    mut allocate: impl FnMut() -> u64,
) {
    let mut saved = BTreeSet::new();
    visit_history_epochs(state, canonical, &mut |epoch| {
        saved.insert(epoch);
        epoch
    });
    let mapping: BTreeMap<_, _> = saved.into_iter().map(|epoch| (epoch, allocate())).collect();
    visit_history_epochs(state, canonical, &mut |epoch| mapping[&epoch]);
}

#[cfg(test)]
fn visit_history_epochs(
    state: &mut [StoredStateRecord],
    canonical: &mut BTreeMap<AgentId, u64>,
    visit: &mut impl FnMut(u64) -> u64,
) {
    for epoch in canonical.values_mut() {
        *epoch = visit(*epoch);
    }
    for record in state {
        if let StateChange::Compaction { epoch } = &mut record.change {
            *epoch = visit(*epoch);
        }
    }
}

// -- CWD index --

fn load_cwd_index(dir: &Path) -> HashMap<String, String> {
    fs::read(dir.join(CWD_INDEX_FILE))
        .ok()
        .and_then(|data| serde_json::from_slice(&data).ok())
        .unwrap_or_default()
}

fn update_cwd_index(dir: &Path, cwd: &str, session_id: MakimaId) -> Result<(), StorageError> {
    let mut index = load_cwd_index(dir);
    let id_str = session_id.to_string();
    if index.get(cwd).is_some_and(|v| *v == id_str)
        && !index
            .iter()
            .any(|(key, value)| key != cwd && *value == id_str)
    {
        return Ok(());
    }
    index.retain(|key, value| key == cwd || *value != id_str);
    index.insert(cwd.to_string(), id_str);
    atomic_write(&dir.join(CWD_INDEX_FILE), &serde_json::to_vec(&index)?)
}

pub fn offload_dir(sessions_dir: &Path, id: MakimaId) -> PathBuf {
    sessions_dir.join(OFFLOAD_DIR).join(id.to_string())
}

fn jsonl_path(dir: &Path, id: MakimaId) -> PathBuf {
    dir.join(format!("{id}.jsonl"))
}

fn is_jsonl(path: &Path) -> bool {
    path.extension().is_some_and(|e| e == "jsonl")
}

fn try_remove(path: &Path) -> Result<bool, StorageError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e.into()),
    }
}

fn remove_from_cwd_index(dir: &Path, session_id: MakimaId) -> Result<(), StorageError> {
    let mut index = load_cwd_index(dir);
    let before = index.len();
    let id = session_id.to_string();
    index.retain(|_, v| *v != id);
    if index.len() != before {
        atomic_write(&dir.join(CWD_INDEX_FILE), &serde_json::to_vec(&index)?)?;
    }
    Ok(())
}

// -- Header scanning for session list --

#[derive(Deserialize)]
struct HeaderCompatibility {
    t: String,
    v: u32,
    #[serde(default)]
    features: BTreeSet<String>,
}

#[derive(Deserialize)]
struct JsonlHeader {
    v: u32,
    id: MakimaId,
    cwd: String,
    #[serde(default)]
    parent: Option<ParentLink>,
    #[serde(default)]
    features: BTreeSet<String>,
}

#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum ScanRecord {
    Msg,
    Input {
        messages: Vec<serde::de::IgnoredAny>,
    },
    Title {
        title: String,
    },
    Meta {
        title: String,
        updated_at: u64,
        #[serde(default)]
        message_count: Option<usize>,
    },
    #[serde(other)]
    Other,
}

/// Cached scan result for one session file, keyed by file name and validated
/// by (size, mtime): stale entries are rescanned, deleted files pruned.
/// `header: None` marks files that failed to scan (wrong version, foreign
/// format), so they are not re-read on every list either.
#[derive(Serialize, Deserialize)]
struct ScanCacheEntry {
    size: u64,
    mtime_ms: u64,
    header: Option<ScannedHeader>,
}

#[derive(Serialize, Deserialize)]
struct ScanCacheFile {
    version: u32,
    entries: ScanCache,
}

#[derive(Serialize, Deserialize)]
struct ScannedHeader {
    id: MakimaId,
    parent: Option<ParentLink>,
    cwd: String,
    title: String,
    updated_at: u64,
    message_count: usize,
}

type ScanCache = HashMap<String, ScanCacheEntry>;

fn load_scan_cache(dir: &Path) -> (ScanCache, bool) {
    let Some(data) = fs::read(dir.join(SCAN_CACHE_FILE)).ok() else {
        return (ScanCache::new(), false);
    };
    match serde_json::from_slice::<ScanCacheFile>(&data) {
        Ok(cache) if cache.version == SCAN_CACHE_VERSION => (cache.entries, false),
        _ => (ScanCache::new(), true),
    }
}

fn file_signature(path: &Path) -> Option<(u64, u64)> {
    let meta = fs::metadata(path).ok()?;
    let mtime_ms = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)?;
    Some((meta.len(), mtime_ms))
}

fn scan_headers(cwd: Option<&str>, dir: &Path) -> Result<Vec<SessionSummary>, StorageError> {
    let (mut cache, cache_needs_rewrite) = load_scan_cache(dir);
    let mut fresh = ScanCache::new();
    let mut dirty = cache_needs_rewrite;
    let mut out = Vec::new();
    for path in session_entries(dir)? {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        let Some((size, mtime_ms)) = file_signature(&path) else {
            continue;
        };
        let entry = match cache.remove(name) {
            Some(e) if e.size == size && e.mtime_ms == mtime_ms => e,
            _ => {
                dirty = true;
                ScanCacheEntry {
                    size,
                    mtime_ms,
                    header: scan_jsonl_header(&path),
                }
            }
        };
        if let Some(h) = &entry.header
            && cwd.is_none_or(|c| crate::paths::dirs_equal(&h.cwd, c))
        {
            out.push(SessionSummary {
                id: h.id,
                title: normalize_title(&h.title),
                updated_at: h.updated_at,
                cwd: h.cwd.clone(),
                // Recomputed from the lock file on every scan: the summary
                // cache must not pin a stale open-elsewhere flag.
                message_count: h.message_count,
                open_elsewhere: session_lock::open_elsewhere(dir, &h.id),
            });
        }
        fresh.insert(name.to_owned(), entry);
    }
    // Leftover cache entries belong to deleted files; rewriting prunes them.
    if (dirty || !cache.is_empty())
        && let Ok(data) = serde_json::to_vec(&ScanCacheFile {
            version: SCAN_CACHE_VERSION,
            entries: fresh,
        })
        && let Err(e) = atomic_write(&dir.join(SCAN_CACHE_FILE), &data)
    {
        warn!(error = %e, "failed to write session scan cache");
    }
    Ok(out)
}

fn scan_jsonl_header(path: &Path) -> Option<ScannedHeader> {
    let file = File::open(path).ok()?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line).ok()?;
    let header: JsonlHeader = serde_json::from_slice(line.trim_ascii_end()).ok()?;
    if !matches!(header.v, LEGACY_LOG_FORMAT_VERSION | LOG_FORMAT_VERSION)
        || !header.features.is_empty()
    {
        return None;
    }

    let mut title = DEFAULT_TITLE.to_string();
    let mut updated_at = 0;
    let mut message_count = 0;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => {}
            // A partial read would cache a wrong count and a stale title, so
            // an unreadable file fails the scan: the session is left unlisted
            // rather than appearing with a bogus count.
            Err(_) => return None,
        }
        if line.starts_with(MSG_PREFIX) && line.ends_with(b"\n") {
            message_count += 1;
        } else if line.ends_with(b"\n")
            && let Ok(ScanRecord::Meta {
                title: next_title,
                updated_at: next_updated_at,
                message_count: next_message_count,
            }) = serde_json::from_slice(line.trim_ascii_end())
        {
            title = next_title;
            updated_at = next_updated_at;
            if let Some(count) = next_message_count {
                message_count = count;
            }
        } else if line.ends_with(b"\n")
            && let Ok(ScanRecord::Title { title: next_title }) =
                serde_json::from_slice(line.trim_ascii_end())
        {
            title = normalize_title(&next_title);
        } else if line.ends_with(b"\n") {
            match serde_json::from_slice::<ScanRecord>(&line) {
                Ok(ScanRecord::Msg) => message_count += 1,
                Ok(ScanRecord::Input { messages }) => message_count += messages.len(),
                _ => {}
            }
        }
    }

    Some(ScannedHeader {
        id: header.id,
        parent: header.parent,
        cwd: header.cwd,
        title,
        updated_at,
        message_count,
    })
}

fn session_entries(dir: &Path) -> Result<Vec<PathBuf>, StorageError> {
    Ok(fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|p| is_jsonl(p))
        .collect())
}

fn load_session_at<M, U, T>(path: &Path) -> Result<Session<M, U, T>, SessionError>
where
    M: DeserializeOwned,
    U: DeserializeOwned + Default,
    T: DeserializeOwned,
{
    let data = fs::read(path).map_err(StorageError::from)?;
    // Decodes the same image payload once per record that mentions it.
    let _intern = crate::intern::Scope::enter();
    let mut session: Session<M, U, T> = load_jsonl(&data, &path.display().to_string())?;
    session.title = normalize_title(&session.title);
    Ok(session)
}

// -- Session impl --

impl<M, U, T> Session<M, U, T> {
    fn refresh_children(&mut self) {
        self.subagents = self
            .agents
            .values()
            .filter(|agent| agent.parent.is_some())
            .map(|agent| {
                let config = self
                    .state
                    .iter()
                    .filter(|record| record.agent == Some(agent.id))
                    .filter_map(|record| match &record.change {
                        StateChange::Config(config) => Some((record.seq, config)),
                        _ => None,
                    })
                    .max_by_key(|(seq, _)| *seq)
                    .map(|(_, config)| config);
                let spec = agent.spec.as_ref();
                StoredSubagent {
                    tool_use_id: agent.transcript_key.clone(),
                    agent_id: Some(agent.id.to_string()),
                    ambiguous: false,
                    name: agent
                        .label
                        .clone()
                        .or_else(|| spec.and_then(|spec| spec.name.clone()))
                        .unwrap_or_default(),
                    model: config.map(|config| config.model.clone()).or_else(|| {
                        spec.and_then(|spec| spec.model.spec.as_ref().map(ToString::to_string))
                    }),
                    thinking: config.map_or_else(
                        || spec.and_then(|spec| spec.model.thinking.map(StoredThinking::from)),
                        |config| config.thinking,
                    ),
                    fast: config.map_or_else(
                        || spec.and_then(|spec| spec.model.fast).unwrap_or(false),
                        |config| config.fast,
                    ),
                }
            })
            .collect();
        for agent in self.agents.values().filter(|agent| agent.parent.is_some()) {
            self.subagent_messages
                .entry(agent.transcript_key.clone())
                .or_default();
        }
    }

    fn message_agent(&self) -> AgentId {
        self.root_agent_id().unwrap_or_else(|| {
            self.id
                .to_string()
                .parse()
                .expect("matching id representation")
        })
    }

    pub fn message_id(&self, index: usize) -> Option<MakimaId> {
        self.message_ids.get(index).copied()
    }
    pub fn last_message_id(&self) -> Option<MakimaId> {
        self.message_ids.last().copied()
    }
    pub fn chat_message_ids(&self, chat: Option<&str>) -> Vec<MakimaId> {
        let Some(key) = chat else {
            return self.message_ids.clone();
        };
        let Some(agent) = self
            .agents
            .values()
            .find(|agent| agent.transcript_key == key)
            .map(|agent| agent.id)
        else {
            return Vec::new();
        };
        let events: Vec<_> = self
            .context_events
            .iter()
            .filter(|event| event["t"] != "fork")
            .cloned()
            .collect();
        context_message_ids(&events, agent)
    }

    pub fn context_events(&self) -> &[serde_json::Value] {
        &self.context_events
    }

    pub fn replace_context_events(&mut self, events: Vec<serde_json::Value>) {
        self.context_events = events;
    }

    #[cfg(test)]
    fn canonical_history_epochs(&self) -> BTreeMap<AgentId, u64> {
        self.agents
            .values()
            .filter_map(|agent| {
                let epoch = if agent.transcript_key.is_empty() {
                    self.epoch
                } else {
                    if !self.subagent_messages.contains_key(&agent.transcript_key) {
                        return None;
                    }
                    self.subagent_history_epochs
                        .get(&agent.transcript_key)
                        .copied()
                        .unwrap_or_default()
                };
                Some((agent.id, epoch))
            })
            .collect()
    }

    fn restore_loaded_history_epochs(&mut self, saved: &BTreeMap<AgentId, u64>) {
        for agent in self.agents.values() {
            if agent.transcript_key.is_empty() {
                if let Some(epoch) = saved.get(&agent.id) {
                    self.epoch = *epoch;
                }
            } else {
                let epoch = saved.get(&agent.id).copied().unwrap_or_else(next_epoch);
                self.subagent_history_epochs
                    .insert(agent.transcript_key.clone(), epoch);
                if saved.contains_key(&agent.id) {
                    self.subagent_messages
                        .entry(agent.transcript_key.clone())
                        .or_default();
                }
            }
        }
    }

    pub fn sequence_high_water(&self) -> Option<u64> {
        self.state
            .iter()
            .map(|record| record.seq)
            .chain(self.agent_revisions.iter().map(|(seq, _)| *seq))
            .chain(self.sequence_high_water)
            .max()
    }

    pub fn next_state_seq(&self) -> u64 {
        self.sequence_high_water()
            .map_or(0, |seq| seq.saturating_add(1))
    }

    pub fn root_agent_id(&self) -> Option<AgentId> {
        self.agents
            .values()
            .find(|agent| agent.parent.is_none())
            .map(|agent| agent.id)
    }

    fn root_config(&self) -> Result<Option<StoredAgentConfig>, SessionError> {
        let mut view = self.latest_state()?;
        Ok(self.root_agent_id().and_then(|root| {
            view.scopes
                .remove(&Some(root))
                .and_then(|scope| scope.config)
        }))
    }

    fn header_cache(&self) -> Result<(String, String), SessionError> {
        Ok(self.root_config()?.map_or_else(
            || (self.model.clone(), self.cwd.clone()),
            |config| (config.model, config.cwd),
        ))
    }

    fn migrate_legacy(&mut self) {
        let root = AgentId::generate();
        let mut seq = self.next_state_seq();
        self.agent_revisions.push((seq, root_record(root, seq)));
        seq += 1;
        self.state.push(StoredStateRecord {
            seq,
            agent: None,
            generation: 0,
            change: StateChange::Rules {
                rules: self.meta.session_rules.clone(),
                yolo: self.meta.yolo,
            },
        });
        seq += 1;
        let mut transcripts = HashSet::new();
        for child in &self.subagents {
            transcripts.insert(child.tool_use_id.clone());
            self.agent_revisions.push((
                seq,
                legacy_child(
                    root,
                    child.tool_use_id.clone(),
                    Some(child.name.clone()),
                    Some(child.tool_use_id.clone()),
                    seq,
                ),
            ));
            seq += 1;
        }
        let mut orphans: Vec<_> = self
            .subagent_messages
            .keys()
            .filter(|key| !transcripts.contains(*key))
            .cloned()
            .collect();
        orphans.sort();
        for transcript in orphans {
            self.agent_revisions
                .push((seq, legacy_child(root, transcript, None, None, seq)));
            seq += 1;
        }
        self.agents = latest_agents(&self.agent_revisions);
        for stored in &mut self.posts {
            let Some(key) = stored.post.agent.as_deref() else {
                continue;
            };
            let mut matches = self
                .agents
                .values()
                .filter(|record| record.parent.is_some() && record.transcript_key == key);
            stored.post.agent = matches
                .next()
                .filter(|_| {
                    matches.next().is_none()
                        && !self
                            .subagents
                            .iter()
                            .any(|child| child.tool_use_id == key && child.ambiguous)
                })
                .map(|record| record.id.to_string());
        }
    }

    fn migrate_legacy_plan(&mut self, legacy: LegacyPlan) -> Result<(), SessionError> {
        let view = self.latest_state()?;
        let plugins = view.scopes.get(&None).map(|scope| &scope.plugins);
        if plugins.is_some_and(|plugins| plugins.contains_key(PLAN_STATE_KEY)) {
            return Ok(());
        }
        let entry = match plugins.and_then(|plugins| plugins.get(LEGACY_PLAN_SLOT)) {
            Some(stored) => stored.value.as_ref().and_then(|value| {
                let path = value.get("path")?.as_str()?;
                Some(PlanEntry {
                    path: PathBuf::from(path),
                    written: value
                        .get("written")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                    approved: None,
                    staged_model: None,
                })
            }),
            None => legacy.plan_path.map(|path| PlanEntry {
                path,
                written: legacy.plan_written,
                approved: None,
                staged_model: None,
            }),
        };
        let Some(entry) = entry else {
            return Ok(());
        };
        let root = match self.root_agent_id() {
            Some(root) => root,
            None => {
                let root = AgentId::generate();
                let seq = self.next_state_seq();
                self.agent_revisions.push((seq, root_record(root, seq)));
                self.agents = latest_agents(&self.agent_revisions);
                self.legacy_root_unpublished = true;
                root
            }
        };
        let value =
            serde_json::to_value(PlanSlot::from([(root, entry)])).map_err(StorageError::from)?;
        self.state.push(StoredStateRecord {
            seq: self.next_state_seq(),
            agent: None,
            generation: 0,
            change: StateChange::Plugin {
                key: PLAN_STATE_KEY.to_owned(),
                version: PLAN_STATE_VERSION,
                value: Some(value),
            },
        });
        self.latest_state()?;
        Ok(())
    }

    pub fn state(&self) -> &[StoredStateRecord] {
        &self.state
    }

    pub fn agent_revisions(&self) -> &[(u64, StoredAgentRecord)] {
        &self.agent_revisions
    }

    pub fn agents(&self) -> &BTreeMap<AgentId, StoredAgentRecord> {
        &self.agents
    }

    pub fn parent(&self) -> Option<&ParentLink> {
        self.parent.as_ref()
    }

    pub fn latest_state(&self) -> Result<SessionStateView, SessionError> {
        SessionStateView::fold(&self.state, &self.agent_revisions)
            .map_err(SessionError::InvalidState)
    }
}

impl<M, U, T> Session<M, U, T>
where
    M: Serialize + DeserializeOwned + TitleSource + Clone,
    U: Serialize + DeserializeOwned + Default,
    T: Serialize + DeserializeOwned,
{
    pub fn new(model: &str, cwd: &str) -> Self {
        let now = now_epoch();
        Self {
            id: MakimaId::generate(),
            title: DEFAULT_TITLE.into(),
            cwd: cwd.into(),
            model: model.into(),
            messages: Arc::default(),
            message_ids: Vec::new(),
            context_events: Vec::new(),
            history_error: None,
            token_usage: U::default(),
            tool_outputs: HashMap::new(),
            subagent_messages: HashMap::new(),
            subagent_history_epochs: HashMap::new(),
            subagent_conversations: HashMap::new(),
            subagents: Vec::new(),
            posts: Vec::new(),
            state: Vec::new(),
            sequence_high_water: None,
            agent_revisions: Vec::new(),
            agents: BTreeMap::new(),
            legacy_root_unpublished: false,
            legacy_config: None,
            parent: None,
            features: BTreeSet::new(),
            usage_by_model: HashMap::new(),
            meta: SessionMeta {
                mode: Some(StoredMode::Build),
                ..Default::default()
            },
            created_at: now,
            updated_at: now,
            revision: 0,
            content_revision: 0,
            epoch: next_epoch(),
            rewrites: 0,
            checkpoint_required: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn checkpoint_required(&self) -> bool {
        self.checkpoint_required.load(Ordering::Acquire)
    }

    fn rebind_message_agent(&mut self, id: AgentId) {
        let previous = serde_json::json!(self.message_agent());
        for event in &mut self.context_events {
            if event["agent"] == previous {
                event["agent"] = serde_json::json!(id);
            }
        }
    }

    pub fn bind_root_id(&mut self, id: AgentId) -> Result<(), SessionError> {
        let Some(previous) = self.root_agent_id() else {
            self.rebind_message_agent(id);
            self.push_agent_revision(
                self.next_state_seq(),
                root_record(id, self.next_state_seq()),
            );
            self.legacy_root_unpublished = false;
            return Ok(());
        };
        if previous == id {
            return Ok(());
        }
        if !self.legacy_root_unpublished
            || self.agents.contains_key(&id)
            || self
                .agent_revisions
                .iter()
                .any(|(_, agent)| agent.id == previous && agent.spec.is_some())
        {
            return Err(SessionError::RootIdMismatch {
                stored_id: previous,
                given_id: id,
            });
        }
        self.latest_state()?;
        self.rebind_message_agent(id);
        for (_, agent) in &mut self.agent_revisions {
            if agent.id == previous {
                agent.id = id;
            }
            if agent.parent == Some(previous) {
                agent.parent = Some(id);
            }
        }
        for record in &mut self.state {
            if record.agent == Some(previous) {
                record.agent = Some(id);
            }
            match &mut record.change {
                StateChange::Plugin {
                    key,
                    value: Some(serde_json::Value::Object(entries)),
                    ..
                } if record.agent.is_none() && key == PLAN_STATE_KEY => {
                    if let Some(entry) = entries.remove(&previous.to_string()) {
                        entries.insert(id.to_string(), entry);
                    }
                }
                StateChange::Turn { result, .. } => rekey_result(result, previous, id),
                StateChange::Delivery {
                    id: delivery_id,
                    op,
                } => {
                    if delivery_id.0 == previous {
                        delivery_id.0 = id;
                    }
                    if let StoredDeliveryOp::Queued(delivery) = op {
                        rekey_result(&mut delivery.result, previous, id);
                    }
                }
                _ => {}
            }
        }
        self.agents = latest_agents(&self.agent_revisions);
        self.legacy_root_unpublished = false;
        self.rewrite();
        Ok(())
    }

    pub fn resolve_legacy_mode(
        &mut self,
        snapshot: StoredModeSnapshot,
    ) -> Result<(), SessionError> {
        let legacy = self
            .legacy_config
            .clone()
            .unwrap_or_else(|| LegacyConfig::from_meta(&self.meta));
        self.resolve_legacy_config(StoredAgentConfig {
            cwd: self.cwd.clone(),
            model: self.model.clone(),
            thinking: legacy.thinking,
            fast: legacy.fast,
            workflow: legacy.workflow,
            mode: snapshot,
            options: legacy.session_options,
        })
    }

    pub fn resolve_legacy_config(&mut self, config: StoredAgentConfig) -> Result<(), SessionError> {
        let view = self.latest_state()?;
        if self.root_agent_id().is_some_and(|root| {
            view.scopes
                .get(&Some(root))
                .is_some_and(|scope| scope.config.is_some())
        }) {
            return Ok(());
        }
        if self.root_agent_id().is_none() {
            self.migrate_legacy();
        }
        let root = self.root_agent_id();
        self.push_state(StoredStateRecord {
            seq: self.next_state_seq(),
            agent: root,
            generation: 0,
            change: StateChange::Config(config),
        });
        Ok(())
    }

    pub fn replace_authoritative(&mut self, mut replacement: Self) -> Result<(), SessionError> {
        if self.id != replacement.id {
            return Err(SessionError::IdMismatch {
                log_id: self.id,
                given_id: replacement.id,
            });
        }
        replacement.latest_state()?;
        if replacement.checkpoint_required() {
            self.checkpoint_required.store(true, Ordering::Release);
        }
        replacement.checkpoint_required = Arc::clone(&self.checkpoint_required);
        replacement.sequence_high_water = self
            .sequence_high_water()
            .max(replacement.sequence_high_water());
        replacement.revision = self.revision;
        replacement.content_revision = self.content_revision;
        replacement.rewrites = self.rewrites;
        let removed_keys: BTreeSet<_> = self
            .subagent_history_epochs
            .keys()
            .chain(self.subagent_messages.keys())
            .chain(replacement.subagent_history_epochs.keys())
            .filter(|key| !replacement.subagent_messages.contains_key(*key))
            .cloned()
            .collect();
        for key in removed_keys {
            replacement
                .subagent_history_epochs
                .insert(key, next_epoch());
        }
        replacement.created_at = self.created_at;
        replacement.rewrite();
        *self = replacement;
        Ok(())
    }

    pub fn push_state(&mut self, record: StoredStateRecord) {
        let reorders = self
            .state
            .iter()
            .map(|record| record.seq)
            .chain(self.agent_revisions.iter().map(|(seq, _)| *seq))
            .any(|seq| seq >= record.seq);
        self.state.push(record);
        self.refresh_children();
        if reorders {
            self.rewrite();
        } else {
            self.touch();
        }
    }

    pub fn replace_state(&mut self, state: Vec<StoredStateRecord>) {
        self.sequence_high_water = self.sequence_high_water();
        if self.state != state {
            self.state = state;
            self.refresh_children();
            self.rewrite();
        }
    }

    pub fn push_agent_revision(&mut self, seq: u64, record: StoredAgentRecord) {
        let reorders = self.state.iter().any(|record| record.seq >= seq)
            || self.agent_revisions.iter().any(|(saved, _)| *saved >= seq);
        self.agent_revisions.push((seq, record));
        self.agents = latest_agents(&self.agent_revisions);
        self.refresh_children();
        if reorders {
            self.rewrite();
        } else {
            self.touch();
        }
    }

    pub fn replace_agent_revisions(&mut self, revisions: Vec<(u64, StoredAgentRecord)>) {
        self.sequence_high_water = self.sequence_high_water();
        if self.agent_revisions != revisions {
            self.agents = latest_agents(&revisions);
            self.refresh_children();
            self.agent_revisions = revisions;
            self.rewrite();
        }
    }

    pub fn set_parent(&mut self, parent: Option<ParentLink>) {
        if self.parent != parent {
            self.parent = parent;
            self.rewrite();
        }
    }

    pub fn messages(&self) -> &[M] {
        &self.messages
    }

    pub fn history_identity(&self) -> HistoryIdentity {
        HistoryIdentity {
            epoch: self.epoch,
            len: self.messages.len(),
        }
    }

    pub fn history_snapshot(&self) -> HistorySnapshot<M> {
        HistorySnapshot {
            epoch: self.epoch,
            compacted: self.meta.compacted,
            conversation: self.meta.conversation.clone(),
            messages: Arc::clone(&self.messages),
            ids: Arc::new(self.message_ids.clone()),
            covered: None,
        }
    }

    pub fn take_messages(self) -> Vec<M> {
        Arc::unwrap_or_clone(self.messages)
    }

    pub fn tool_outputs(&self) -> &HashMap<String, Arc<T>> {
        &self.tool_outputs
    }

    pub fn subagent_messages(&self) -> &HashMap<String, Arc<Vec<M>>> {
        &self.subagent_messages
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn content_revision(&self) -> u64 {
        self.content_revision
    }

    fn touch(&mut self) {
        self.content_revision += 1;
        self.touch_soft();
    }

    /// Only UI state moved, so the write can wait for company. Everything a
    /// crash would lose for good goes through `touch`, which is the default a
    /// new mutator gets by not thinking about it.
    fn touch_soft(&mut self) {
        self.updated_at = now_epoch();
        self.revision += 1;
    }

    fn rewrite(&mut self) {
        self.rewrites += 1;
        self.touch();
    }

    fn rewrite_messages(&mut self) {
        self.epoch = next_epoch();
        self.rewrite();
    }

    fn record_history(&mut self, messages: &[M]) {
        let previous = self
            .messages
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>();
        let next = messages
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>();
        let (previous, next) = match (previous, next) {
            (Ok(previous), Ok(next)) => (previous, next),
            (Err(error), _) | (_, Err(error)) => {
                self.history_error = Some(error.to_string());
                return;
            }
        };
        let prefix = previous.len() <= next.len() && previous == next[..previous.len()];
        if prefix {
            for value in &next[previous.len()..] {
                let id = MakimaId::generate();
                self.message_ids.push(id);
                self.context_events.push(serde_json::json!({"t":"msg", "id": id, "agent":self.message_agent(), "d":value}));
            }
        } else {
            let covered = self.last_message_id();
            self.message_ids = next.iter().map(|_| MakimaId::generate()).collect();
            self.context_events.push(serde_json::json!({"t":"compaction", "agent":self.message_agent(), "messages": next, "ids": self.message_ids, "covered": covered}));
        }
    }

    pub fn fork(&mut self, target: Option<MakimaId>, note: String) -> Result<(), SessionError> {
        let conversation = ConversationState::fresh(None);
        let event = serde_json::to_value(SessionRecord::<M, U, T>::Fork {
            target,
            note_id: MakimaId::generate(),
            note,
            conversation: conversation.clone(),
        })
        .map_err(StorageError::from)?;
        self.context_events.push(event);
        let rebuilt =
            context_entries(&self.context_events, self.message_agent()).and_then(|entries| {
                let ids = entries.iter().map(ContextEntry::id).collect();
                let messages = entries
                    .into_iter()
                    .map(ContextEntry::into_message)
                    .collect::<Result<Vec<M>, _>>()
                    .map_err(StorageError::from)?;
                Ok((ids, messages))
            });
        let (ids, messages) = match rebuilt {
            Ok(context) => context,
            Err(error) => {
                self.context_events.pop();
                return Err(error);
            }
        };
        self.message_ids = ids;
        self.messages = Arc::new(messages);
        self.meta.compacted = false;
        self.meta.conversation = Some(conversation);
        self.rewrite_messages();
        Ok(())
    }

    pub fn push_message(&mut self, msg: M) {
        let id = MakimaId::generate();
        match serde_json::to_value(&msg) {
            Ok(d) => self
                .context_events
                .push(serde_json::json!({"t":"msg", "id":id, "agent":self.message_agent(), "d":d})),
            Err(error) => self.history_error = Some(error.to_string()),
        }
        self.message_ids.push(id);
        Arc::make_mut(&mut self.messages).push(msg);
        self.touch();
    }

    pub fn replace_messages(&mut self, messages: Vec<M>) {
        self.replace_shared_messages(Arc::new(messages));
    }

    pub fn replace_shared_messages(&mut self, messages: Arc<Vec<M>>) {
        self.record_history(&messages);
        self.messages = messages;
        self.rewrite_messages();
    }

    pub fn truncate_messages(&mut self, len: usize) {
        if len >= self.messages.len() {
            return;
        }
        let messages = self.messages[..len].to_vec();
        self.record_history(&messages);
        Arc::make_mut(&mut self.messages).truncate(len);
        self.rewrite_messages();
    }

    /// Adopting a producer's snapshot inherits its run token, so the log's
    /// cursors survive exactly when the snapshot was an append.
    pub fn adopt_history(&mut self, snapshot: &HistorySnapshot<M>) {
        let agent = self.message_agent();
        let previous = self.message_ids.clone();
        self.record_snapshot(agent, &previous, self.epoch == snapshot.epoch, snapshot);
        self.message_ids = snapshot.ids.as_ref().clone();
        self.messages = Arc::clone(&snapshot.messages);
        self.epoch = snapshot.epoch;
        self.meta.compacted = snapshot.compacted;
        self.meta.conversation = snapshot.conversation.clone();
        self.touch();
    }

    /// Applies everything the owner mirrors from live state. It takes an `Arc`
    /// and checks for a real change first because `Arc::make_mut` deep-copies
    /// the whole session while the writer still holds the last snapshot, and an
    /// idle session should not pay for that every frame.
    pub fn checkpoint(
        this: &mut Arc<Self>,
        history: Option<&HistorySnapshot<M>>,
        mut meta: SessionMeta,
        token_usage: U,
    ) where
        M: Clone,
        U: PartialEq + Clone,
        T: Clone,
    {
        if let Some(snapshot) = history {
            meta.compacted = snapshot.compacted;
            meta.conversation = snapshot.conversation.clone();
        } else {
            meta.compacted = this.meta.compacted;
            meta.conversation = this.meta.conversation.clone();
        }
        if let Some(legacy) = &this.legacy_config
            && this.root_config().is_ok_and(|config| config.is_none())
        {
            legacy.restore(&mut meta);
        }
        let history = history.filter(|h| {
            !Arc::ptr_eq(&this.messages, &h.messages)
                || h.compacted != this.meta.compacted
                || h.conversation != this.meta.conversation
        });
        if history.is_none() && this.meta == meta && this.token_usage == token_usage {
            return;
        }
        let session = Arc::make_mut(this);
        if let Some(snapshot) = history {
            session.adopt_history(snapshot);
            // The title comes from the messages, so it goes stale exactly when
            // they move.
            session.update_title_if_default();
        }
        session.set_meta(meta);
        session.set_token_usage(token_usage);
    }

    /// A change under an existing id is not expressible as an append, so it
    /// voids the cursors; a new id is a pure append.
    pub fn insert_tool_output(&mut self, id: String, output: T) {
        if self.tool_outputs.insert(id, Arc::new(output)).is_some() {
            self.rewrite();
        } else {
            self.touch();
        }
    }

    pub fn subagent_history_snapshot(&self, id: &str) -> Option<HistorySnapshot<M>> {
        self.subagent_messages.get(id).map(|messages| {
            let agent = self
                .agents
                .values()
                .find(|agent| agent.transcript_key == id)
                .map(|agent| agent.id);
            let history = self.context_events.iter().rev().find(|event| {
                event["t"] == "history"
                    && agent.is_some_and(|agent| event["agent"] == serde_json::json!(agent))
            });
            HistorySnapshot {
                epoch: self
                    .subagent_history_epochs
                    .get(id)
                    .copied()
                    .unwrap_or_default(),
                compacted: history
                    .is_some_and(|event| event["compacted"].as_bool().unwrap_or_default()),
                conversation: history
                    .map(|event| {
                        serde_json::from_value(event["conversation"].clone())
                            .ok()
                            .flatten()
                    })
                    .unwrap_or_else(|| {
                        self.subagent_conversations
                            .get(id)
                            .cloned()
                            .map(ConversationState::new)
                    }),
                messages: Arc::clone(messages),
                ids: Arc::new(self.chat_message_ids(Some(id))),
                covered: None,
            }
        })
    }

    /// Rejects projections from a run superseded by a child history replacement.
    pub fn adopt_subagent_history(&mut self, id: String, snapshot: &HistorySnapshot<M>) -> bool {
        if self
            .subagent_history_epochs
            .get(&id)
            .is_some_and(|epoch| *epoch > snapshot.epoch)
        {
            return false;
        }
        let previous = self.chat_message_ids(Some(&id));
        let same_epoch = self.subagent_history_epochs.get(&id) == Some(&snapshot.epoch);
        let agent = self
            .agents
            .values()
            .find(|agent| agent.transcript_key == id)
            .map(|agent| agent.id);
        if let Some(agent) = agent {
            self.record_snapshot(agent, &previous, same_epoch, snapshot);
        } else {
            let event_count = self.context_events.len();
            self.record_child_history(&id, &snapshot.messages);
            self.context_events.truncate(event_count);
            if let Some(agent) = self
                .agents
                .values()
                .find(|agent| agent.transcript_key == id)
                .map(|agent| agent.id)
            {
                self.record_snapshot(agent, &previous, same_epoch, snapshot);
            }
        }
        self.subagent_history_epochs
            .insert(id.clone(), snapshot.epoch);
        // Frontend replays of a child transcript carry no id; keep the last known one.
        if let Some(conversation) = &snapshot.conversation {
            self.subagent_conversations
                .insert(id.clone(), Arc::clone(&conversation.id));
        }
        if self
            .subagent_messages
            .insert(id, Arc::clone(&snapshot.messages))
            .is_some()
        {
            self.rewrite();
        } else {
            self.touch();
        }
        true
    }

    pub fn replace_journal_histories(&mut self, histories: &BTreeMap<AgentId, HistorySnapshot<M>>) {
        let epoch = next_epoch();
        self.epoch = epoch;
        self.subagent_history_epochs
            .values_mut()
            .for_each(|saved| *saved = epoch);
        for key in self.subagent_messages.keys() {
            self.subagent_history_epochs.insert(key.clone(), epoch);
        }
        self.meta.compacted = false;
        self.meta.conversation = None;
        self.subagent_conversations.clear();
        for (agent, history) in histories {
            let Some(record) = self.agents.get(agent).cloned() else {
                continue;
            };
            if record.transcript_key.is_empty() {
                self.adopt_history(history);
            } else {
                let key = record.transcript_key.clone();
                self.subagent_history_epochs
                    .insert(key.clone(), history.epoch);
                self.adopt_subagent_history(key, history);
            }
        }
        let keys: HashSet<_> = self
            .agents
            .values()
            .map(|agent| agent.transcript_key.as_str())
            .collect();
        self.subagents
            .retain(|child| keys.contains(child.tool_use_id.as_str()));
        self.rewrite();
    }

    fn record_snapshot(
        &mut self,
        agent: AgentId,
        previous: &[MakimaId],
        same_epoch: bool,
        snapshot: &HistorySnapshot<M>,
    ) {
        let append = same_epoch && snapshot.ids.starts_with(previous);
        let result = if append {
            snapshot
                .messages
                .iter()
                .zip(snapshot.ids.iter())
                .skip(previous.len())
                .map(|(message, id)| {
                    serde_json::to_value(SessionRecord::<&M, U, T>::Msg {
                        id: *id,
                        agent,
                        d: message,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        } else {
            serde_json::to_value(SessionRecord::<&M, U, T>::Compaction {
                agent,
                messages: snapshot.messages.iter().collect(),
                ids: snapshot.ids.as_ref().clone(),
                covered: snapshot.covered.or_else(|| previous.last().copied()),
            })
            .map(|record| vec![record])
        };
        match result {
            Ok(records) => {
                self.context_events.extend(records);
                match serde_json::to_value(SessionRecord::<M, U, T>::History {
                    agent,
                    compacted: snapshot.compacted,
                    conversation: snapshot.conversation.clone(),
                }) {
                    Ok(record) => self.context_events.push(record),
                    Err(error) => self.history_error = Some(error.to_string()),
                }
            }
            Err(error) => self.history_error = Some(error.to_string()),
        }
    }

    fn record_child_history(&mut self, key: &str, messages: &[M]) {
        let agent = self
            .agents
            .values()
            .find(|agent| agent.transcript_key == key)
            .map(|agent| agent.id)
            .unwrap_or_else(|| {
                let root = self.message_agent();
                if self.root_agent_id().is_none() {
                    let seq = self.next_state_seq();
                    self.push_agent_revision(seq, root_record(root, seq));
                }
                let seq = self.next_state_seq();
                let label = self
                    .subagents
                    .iter()
                    .find(|child| child.tool_use_id == key)
                    .map(|child| child.name.clone());
                let child = legacy_child(root, key.to_owned(), label, None, seq);
                let id = child.id;
                self.push_agent_revision(seq, child);
                id
            });
        let next = match serde_json::to_value(messages) {
            Ok(next) => next,
            Err(error) => {
                self.history_error = Some(error.to_string());
                return;
            }
        };
        let previous = match self
            .subagent_messages
            .get(key)
            .map(|messages| serde_json::to_value(messages.as_ref()))
            .transpose()
        {
            Ok(previous) => previous,
            Err(error) => {
                self.history_error = Some(error.to_string());
                return;
            }
        };
        let previous = previous.as_ref().and_then(|value| value.as_array());
        let Some(next) = next.as_array() else {
            return;
        };
        let prefix = previous.is_none_or(|previous| {
            previous.len() <= next.len() && previous == &next[..previous.len()]
        });
        if prefix {
            for d in &next[previous.map_or(0, Vec::len)..] {
                self.context_events.push(
                    serde_json::json!({"t":"msg","id":MakimaId::generate(),"agent":agent,"d":d}),
                );
            }
        } else {
            let ids: Vec<_> = next.iter().map(|_| MakimaId::generate()).collect();
            let covered = self.chat_message_ids(Some(key)).last().copied();
            self.context_events.push(
                serde_json::json!({"t":"compaction","agent":agent,"messages":next,"ids":ids,"covered":covered}),
            );
        }
    }

    pub fn set_subagent_messages(&mut self, id: String, msgs: Vec<M>) {
        self.record_child_history(&id, &msgs);
        self.subagent_history_epochs.remove(&id);
        if self.subagent_messages.insert(id, Arc::new(msgs)).is_some() {
            self.rewrite();
        } else {
            self.touch();
        }
    }

    fn set_token_usage(&mut self, usage: U)
    where
        U: PartialEq,
    {
        if self.token_usage == usage {
            return;
        }
        self.token_usage = usage;
        self.touch();
    }

    fn set_meta(&mut self, meta: SessionMeta) {
        if self.meta == meta {
            return;
        }
        self.meta = meta;
        self.touch_soft();
    }

    pub fn subagents(&self) -> &[StoredSubagent] {
        &self.subagents
    }

    pub fn set_subagents(&mut self, mut subagents: Vec<StoredSubagent>) {
        mark_duplicate_subagent_identities(&mut subagents);
        if self.root_agent_id().is_none() && !subagents.is_empty() {
            let seq = self.next_state_seq();
            self.push_agent_revision(seq, root_record(self.message_agent(), seq));
        }
        for child in subagents {
            let existing = self
                .agents
                .values()
                .find(|agent| agent.parent.is_some() && agent.transcript_key == child.tool_use_id)
                .cloned();
            let mut record = if let Some(record) = existing {
                record
            } else {
                let seq = self.next_state_seq();
                let mut record = legacy_child(
                    self.message_agent(),
                    child.tool_use_id.clone(),
                    None,
                    Some(child.tool_use_id.clone()),
                    seq,
                );
                if !child.ambiguous
                    && let Some(id) = child.agent_id.as_deref().and_then(|id| id.parse().ok())
                    && !self.agents.contains_key(&id)
                {
                    record.id = id;
                }
                record
            };
            if !child.name.is_empty() {
                record.label = Some(child.name.clone());
            }
            if self.agents.get(&record.id) != Some(&record) {
                let seq = self.next_state_seq();
                record.updated_seq = seq;
                self.push_agent_revision(seq, record.clone());
            }
            let has_config = self.state.iter().any(|state| {
                state.agent == Some(record.id) && matches!(state.change, StateChange::Config(_))
            });
            if record.legacy
                && !has_config
                && let Some(model) = child.model
            {
                let root_config = self.root_config().ok().flatten();
                let config = StoredAgentConfig {
                    cwd: self.cwd.clone(),
                    model,
                    thinking: child.thinking,
                    fast: child.fast,
                    workflow: false,
                    options: BTreeMap::new(),
                    mode: root_config.map(|config| config.mode).unwrap_or_else(|| {
                        StoredModeSnapshot {
                            id: "build".into(),
                            label: "Build".into(),
                            system_prompt: None,
                            restrict_write_to: None,
                            tools: None,
                        }
                    }),
                };
                self.push_state(StoredStateRecord {
                    seq: self.next_state_seq(),
                    agent: Some(record.id),
                    generation: 0,
                    change: StateChange::Config(config),
                });
            }
        }
        self.refresh_children();
    }

    pub fn posts(&self) -> &[StoredPost] {
        &self.posts
    }

    pub fn push_post(&mut self, post: StoredPost) {
        self.posts.push(post);
        self.touch();
    }

    pub fn prune_posts(&mut self, _chat: Option<&str>, _new_len: usize) {}

    pub fn usage_by_model(&self) -> &HashMap<String, StoredTokenUsage> {
        &self.usage_by_model
    }

    /// For settling costs on load; every other write goes through
    /// [`Self::add_model_usage`].
    pub fn usage_by_model_mut(&mut self) -> &mut HashMap<String, StoredTokenUsage> {
        self.touch();
        &mut self.usage_by_model
    }

    pub fn set_title(&mut self, title: String) {
        if self.title == title {
            return;
        }
        self.title = title;
        self.touch();
    }

    pub fn set_cwd(&mut self, cwd: String) {
        if self.cwd == cwd {
            return;
        }
        self.cwd = cwd;
        self.rewrite();
    }

    /// Header field, see [`Self::set_cwd`].
    pub fn set_model(&mut self, model: String) {
        if self.model == model {
            return;
        }
        self.model = model;
        self.rewrite();
    }

    pub fn add_model_usage(&mut self, model: &str, usage: StoredTokenUsage) {
        *self.usage_by_model.entry(model.to_owned()).or_default() += usage;
        self.touch();
    }

    /// Removes state unreachable from the main history, its posts, or live
    /// children. Child messages and posts can retain further descendants.
    /// Call only for explicit history edits, not compaction.
    pub fn prune_orphans(&mut self, tool_ids: impl Fn(&M) -> Vec<String>) {
        self.prune_orphans_preserving(tool_ids, &[]);
    }

    pub fn prune_orphans_preserving(
        &mut self,
        tool_ids: impl Fn(&M) -> Vec<String>,
        live_ids: &[String],
    ) {
        self.prune_posts(None, self.messages.len());
        let previous_counts = (
            self.subagent_messages.len(),
            self.subagent_history_epochs.len(),
            self.subagents.len(),
            self.posts.len(),
            self.tool_outputs.len(),
        );
        let mut post_agents: HashMap<Option<&str>, Vec<&str>> = HashMap::new();
        for stored in &self.posts {
            if let Some(agent) = stored.post.agent.as_deref() {
                let transcript_key = agent
                    .parse::<AgentId>()
                    .ok()
                    .and_then(|id| self.agents.get(&id))
                    .map_or(agent, |record| record.transcript_key.as_str());
                post_agents
                    .entry(stored.chat.as_deref())
                    .or_default()
                    .push(transcript_key);
            }
        }
        let mut pending: Vec<String> = self
            .messages
            .iter()
            .flat_map(&tool_ids)
            .chain(live_ids.iter().cloned())
            .chain(
                post_agents
                    .get(&None)
                    .into_iter()
                    .flatten()
                    .map(|id| (*id).to_owned()),
            )
            .collect();
        let mut reachable = HashSet::new();
        while let Some(id) = pending.pop() {
            if !reachable.insert(id.clone()) {
                continue;
            }
            if let Some(messages) = self.subagent_messages.get(&id) {
                pending.extend(messages.iter().flat_map(&tool_ids));
            }
            if let Some(agents) = post_agents.get(&Some(id.as_str())) {
                pending.extend(agents.iter().map(|id| (*id).to_owned()));
            }
        }
        self.subagent_messages
            .retain(|id, _| reachable.contains(id));
        self.subagent_conversations
            .retain(|id, _| reachable.contains(id));
        self.subagents
            .retain(|sa| reachable.contains(&sa.tool_use_id));
        self.posts.retain(|stored| {
            stored
                .chat
                .as_ref()
                .is_none_or(|chat| reachable.contains(chat))
        });
        self.tool_outputs.retain(|id, _| reachable.contains(id));
        if previous_counts
            != (
                self.subagent_messages.len(),
                self.subagent_history_epochs.len(),
                self.subagents.len(),
                self.posts.len(),
                self.tool_outputs.len(),
            )
        {
            self.rewrite();
        }
    }

    #[cfg(any(test, feature = "test-util"))]
    pub fn save(&mut self, dir: &StateDir) -> Result<(), SessionError> {
        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR)?;
        self.save_to(&sessions_dir)
    }

    /// Writes a complete fixture. Runtime persistence uses the agent's session writer.
    #[cfg(any(test, feature = "test-util"))]
    pub fn save_to(&mut self, dir: &Path) -> Result<(), SessionError> {
        fs::create_dir_all(dir).map_err(StorageError::from)?;
        let mut bytes = Vec::new();
        write_full_session(&mut bytes, self)?;
        atomic_write(&jsonl_path(dir, self.id), &bytes)?;
        update_cwd_index(dir, &self.cwd, self.id)?;
        self.checkpoint_required.store(true, Ordering::Release);
        Ok(())
    }

    pub fn load(id: MakimaId, dir: &StateDir) -> Result<Self, SessionError> {
        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR)?;
        Self::load_from(id, &sessions_dir)
    }

    pub fn load_from(id: MakimaId, dir: &Path) -> Result<Self, SessionError> {
        let path = jsonl_path(dir, id);
        if !path.exists() {
            return Err(StorageError::NotFound(id.to_string()).into());
        }
        load_session_at::<M, U, T>(&path)
    }

    pub fn list(cwd: &str, dir: &StateDir) -> Result<Vec<SessionSummary>, SessionError> {
        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR)?;
        Self::list_in(cwd, &sessions_dir)
    }

    pub fn list_in(cwd: &str, dir: &Path) -> Result<Vec<SessionSummary>, SessionError> {
        let mut summaries = scan_headers(Some(cwd), dir)?;
        summaries.sort_unstable_by_key(|s| Reverse(s.updated_at));
        Ok(summaries)
    }

    pub fn list_all(dir: &StateDir) -> Result<Vec<SessionSummary>, SessionError> {
        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR)?;
        Self::list_all_in(&sessions_dir)
    }

    pub fn list_all_in(dir: &Path) -> Result<Vec<SessionSummary>, SessionError> {
        let mut summaries = scan_headers(None, dir)?;
        summaries.sort_unstable_by_key(|s| Reverse(s.updated_at));
        Ok(summaries)
    }

    pub fn latest(cwd: &str, dir: &StateDir) -> Result<Option<Self>, SessionError> {
        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR)?;
        Self::latest_in(cwd, &sessions_dir)
    }

    pub fn latest_in(cwd: &str, dir: &Path) -> Result<Option<Self>, SessionError> {
        let cached = load_cwd_index(dir)
            .remove(cwd)
            .and_then(|s| match s.parse::<MakimaId>() {
                Ok(id) => Some(id),
                Err(e) => {
                    warn!(error = %e, cwd, "indexed session id unparseable; rescanning");
                    None
                }
            });
        if let Some(id) = cached {
            match Self::load_from(id, dir) {
                Ok(s) if crate::paths::dirs_equal(&s.cwd, cwd) => return Ok(Some(s)),
                Ok(_) => {}
                Err(e) => warn!(error = %e, cwd, "indexed session missing on disk; rescanning"),
            }
        }

        scan_headers(Some(cwd), dir)?
            .into_iter()
            .max_by_key(|s| s.updated_at)
            .map(|s| Self::load_from(s.id, dir).map(Some))
            .unwrap_or(Ok(None))
    }

    pub fn update_title_if_default(&mut self) {
        if self.title == DEFAULT_TITLE {
            self.set_title(generate_title(&self.messages));
        }
    }

    pub fn delete(id: MakimaId, dir: &StateDir) -> Result<(), SessionError> {
        fs::create_dir_all(dir.path()).map_err(StorageError::from)?;
        let state_root = fs::canonicalize(dir.path()).map_err(StorageError::from)?;
        let root = Root::open(&state_root).map_err(StorageError::from)?;
        let cleanup_root = match root.directory(Path::new(SESSIONS_DIR)) {
            Ok(root) => Ok(root),
            Err(error) if error.kind() == ErrorKind::NotFound => Err(error),
            Err(error) => return Err(StorageError::from(error).into()),
        };
        Self::delete_from_with_cleanup(id, &state_root.join(SESSIONS_DIR), cleanup_root)
    }

    pub fn delete_from(id: MakimaId, dir: &Path) -> Result<(), SessionError> {
        Self::delete_from_with_cleanup(id, dir, Root::open(dir))
    }

    fn delete_from_with_cleanup(
        id: MakimaId,
        dir: &Path,
        cleanup_root: io::Result<Root>,
    ) -> Result<(), SessionError> {
        if session_lock::open_elsewhere(dir, &id) {
            return Err(SessionError::OpenElsewhere);
        }
        let removed = try_remove(&jsonl_path(dir, id))?;
        // Backups, not the session: failing to sweep them must not fail a
        // delete whose log is already gone, and their presence alone does not
        // make a session exist.
        let session_id = id.to_string();
        let paths = [
            (Path::new(ARCHIVE_DIR).join(&session_id), "archives"),
            (
                Path::new(OFFLOAD_DIR).join(&session_id),
                "offloaded tool output",
            ),
        ];
        match &cleanup_root {
            Ok(root) => {
                for (path, what) in paths {
                    if let Err(error) = root.remove(&path)
                        && error.kind() != ErrorKind::NotFound
                    {
                        warn!(error = %error, session_id = %id, what, "session data remains after delete");
                    }
                }
            }
            Err(error) if error.kind() != ErrorKind::NotFound => {
                for (_, what) in paths {
                    warn!(error = %error, session_id = %id, what, "session data remains after delete");
                }
            }
            Err(_) => {}
        }
        if !removed {
            return Err(StorageError::NotFound(id.to_string()).into());
        }
        remove_from_cwd_index(dir, id)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Effort;
    use super::StoredThinking;
    use super::ThinkingParseError;
    use super::{
        ARCHIVE_DIR, CWD_INDEX_FILE, DEFAULT_TITLE, MAX_TITLE_LEN, SESSIONS_DIR, StoredSubagent,
        TAIL_BUF, generate_title, jsonl_path, load_cwd_index, offload_dir, root_record,
        update_cwd_index, write_full_session,
    };
    use super::{
        HistorySnapshot, SCAN_CACHE_FILE, Session, SessionError, SessionMeta, SessionRecord,
        StorageError, TitleSource, context_entries, encode_records,
    };
    use super::{LEGACY_CHILD_UNRESTORABLE, LEGACY_PLAN_SLOT, StoredMode};
    use super::{Post, PostLevel, StoredPost};
    use super::{Prefs, read_prefs, write_prefs};
    use crate::StateDir;
    use crate::conversation::ConversationState;
    use crate::id::{AgentId, MakimaId};
    use crate::plans::{PLAN_STATE_KEY, PlanSlot};
    use crate::session_lock;
    use crate::session_state::{
        StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredModeSnapshot, StoredStateRecord,
    };
    use serde_json::Value;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::fs::symlink as symlink_dir;
    #[cfg(windows)]
    use std::os::windows::fs::symlink_dir;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use tempfile::TempDir;
    use test_case::test_case;

    type TestSession = Session<Value, Value, Value>;

    #[cfg(any(unix, windows))]
    const OFFLOAD_SENTINEL: &str = "external offload sentinel";
    #[cfg(any(unix, windows))]
    const OFFLOAD_SENTINEL_FILE: &str = "sentinel.txt";
    const FIXTURE_ID: &str = "CNK1hV6GWoysH3KQMm5wu";
    const UNSUPPORTED_FEATURE: &str = "future-state-format";
    const FUTURE_FORMAT_VERSION: u32 = 99;
    const SONNET_COST: f64 = 0.42;
    const HAIKU_COST: f64 = 0.08;
    const TAMPERED_TITLE: &str = "tampered cached title";
    const PENDING_DRAFT: &str = "half typed thought";
    const POST_TEXT: &str = "reviewer has findings";
    const POST_AGENT: &str = "session-reviewer";
    const POST_APPEND_COUNT: usize = 64;
    const POST_DESCENDANT: &str = "post-descendant";
    const MESSAGE_DESCENDANT: &str = "message-descendant";
    const LIVE_CHILD: &str = "live-child";
    const ORPHAN_CHILD: &str = "orphan-child";
    /// A pid no live process on this machine has.
    const FAKE_LOCK_PID: u32 = u32::MAX - 1;

    #[test_case(false; "ordinary_branch")]
    #[test_case(true; "precompaction_branch")]
    fn fork_restores_historical_context_and_redo_after_reload(compact: bool) {
        const NOTE: &str = "rewound; files may differ";
        const REDO_NOTE: &str = "redo selected branch";
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let first = user_message("first");
        let second = user_message("second");
        session.push_message(first.clone());
        let first_id = session.last_message_id().unwrap();
        session.push_message(second.clone());
        let second_id = session.last_message_id().unwrap();
        if compact {
            session.replace_messages(vec![user_message("summary")]);
            session.meta.compacted = true;
            session.meta.conversation = Some(ConversationState::fresh(None));
        }
        session.save_to(directory.path()).unwrap();
        let historical_events = session.context_events().to_vec();
        let previous = session.meta.conversation.clone();
        session.fork(Some(first_id), NOTE.into()).unwrap();
        assert!(!session.meta.compacted);
        assert_ne!(session.meta.conversation, previous);
        assert_eq!(session.messages()[0], first);
        assert_eq!(session.messages()[1]["content"][0]["text"], NOTE);
        assert_eq!(session.messages()[1]["kind"], "observation");
        session.push_message(user_message("new branch"));
        session.save_to(directory.path()).unwrap();
        assert!(session.context_events().starts_with(&historical_events));
        let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert!(loaded.context_events().starts_with(&historical_events));
        assert_eq!(loaded.messages(), session.messages());
        assert_eq!(
            loaded.chat_message_ids(None),
            session.chat_message_ids(None)
        );
        loaded.fork(Some(second_id), REDO_NOTE.into()).unwrap();
        assert_eq!(&loaded.messages()[..2], &[first, second]);
        assert_eq!(loaded.messages()[2]["content"][0]["text"], REDO_NOTE);
        loaded.save_to(directory.path()).unwrap();
        let reloaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(reloaded.messages(), loaded.messages());
        assert_eq!(
            reloaded.chat_message_ids(None),
            loaded.chat_message_ids(None)
        );
        assert_eq!(
            reloaded
                .context_events()
                .iter()
                .filter(|event| event["t"] == "fork")
                .count(),
            2
        );
    }

    #[test_case(false; "fork_is_latest_root_history")]
    #[test_case(true; "later_root_history_supersedes_fork")]
    fn replay_fork_metadata_and_note_id(later_history: bool) {
        const NOTE: &str = "rewound to session start";
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.meta.compacted = true;
        session.meta.conversation = Some(ConversationState::fresh(None));
        session.save_to(directory.path()).unwrap();

        let note_id = MakimaId::generate();
        let conversation = ConversationState::fresh(None);
        let mut records = vec![
            SessionRecord::<Value, Value, Value>::Fork {
                target: None,
                note_id,
                note: NOTE.into(),
                conversation: conversation.clone(),
            },
            SessionRecord::History {
                agent: AgentId::generate(),
                compacted: true,
                conversation: Some(ConversationState::fresh(None)),
            },
        ];
        let expected = if later_history {
            let next = ConversationState::fresh(None);
            records.push(SessionRecord::History {
                agent: root,
                compacted: true,
                conversation: Some(next.clone()),
            });
            next
        } else {
            conversation
        };
        OpenOptions::new()
            .append(true)
            .open(jsonl_path(directory.path(), session.id))
            .unwrap()
            .write_all(&encode_records(&records).unwrap())
            .unwrap();

        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        let history = loaded.history_snapshot();
        assert_eq!(history.compacted, later_history);
        assert_eq!(history.conversation, Some(expected));
        assert_eq!(history.covered, None);
        assert_eq!(loaded.chat_message_ids(None), [note_id]);
        let transcript_events: Vec<_> = loaded
            .context_events()
            .iter()
            .filter(|event| event["t"] != "history")
            .cloned()
            .collect();
        assert_eq!(
            context_entries(&transcript_events, root).unwrap()[0].id(),
            note_id
        );
    }

    #[test]
    fn fork_retains_prior_notes_and_does_not_rewind_children() {
        const FIRST_NOTE: &str = "first rewind";
        const SECOND_NOTE: &str = "second rewind";
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.push_message(user_message("first"));
        let first_id = session.last_message_id().unwrap();
        session.set_subagent_messages("child".into(), vec![user_message("child work")]);
        let child_ids = session.chat_message_ids(Some("child"));
        let child_messages = session.subagent_messages()["child"].clone();
        let events = session.context_events().to_vec();
        let child_target = child_ids[0];
        assert!(
            matches!(session.fork(Some(child_target), FIRST_NOTE.into()),
            Err(SessionError::InvalidForkTarget { target }) if target == child_target)
        );
        assert_eq!(session.context_events(), events);
        session.fork(Some(first_id), FIRST_NOTE.into()).unwrap();
        let first_note = session.last_message_id().unwrap();
        session.fork(Some(first_note), SECOND_NOTE.into()).unwrap();
        assert_eq!(session.messages().len(), 3);
        session.fork(Some(first_note), FIRST_NOTE.into()).unwrap();
        session.push_message(user_message("continued"));
        let continued = session.last_message_id().unwrap();
        session.push_message(user_message("discarded"));
        session.fork(Some(continued), SECOND_NOTE.into()).unwrap();
        assert_eq!(session.messages().len(), 5);
        assert_eq!(session.messages()[1]["content"][0]["text"], FIRST_NOTE);
        assert_eq!(session.messages()[4]["content"][0]["text"], SECOND_NOTE);
        assert_eq!(session.subagent_messages()["child"], child_messages);
        assert_eq!(session.chat_message_ids(Some("child")), child_ids);
        session.save_to(directory.path()).unwrap();
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(loaded.messages(), session.messages());
        assert_eq!(loaded.subagent_messages()["child"], child_messages);
        assert_eq!(loaded.chat_message_ids(Some("child")), child_ids);
    }

    #[test]
    fn fork_before_first_message_preserves_redo_after_reload() {
        const NOTE: &str = "rewound to session start";
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let first = user_message("first");
        session.push_message(first.clone());
        let first_id = session.last_message_id().unwrap();
        session.fork(None, NOTE.into()).unwrap();
        assert_eq!(session.messages().len(), 1);
        assert_eq!(session.messages()[0]["content"][0]["text"], NOTE);
        session.save_to(directory.path()).unwrap();
        let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(loaded.messages(), session.messages());
        assert_eq!(
            loaded.chat_message_ids(None),
            session.chat_message_ids(None)
        );
        loaded.fork(Some(first_id), NOTE.into()).unwrap();
        assert_eq!(loaded.messages()[0], first);
    }

    #[test]
    fn invalid_fork_target_leaves_context_and_log_unchanged() {
        let mut session = TestSession::new("model", "/project");
        session.push_message(user_message("first"));
        let messages = session.messages().to_vec();
        let events = session.context_events().to_vec();
        let target = MakimaId::generate();
        assert!(matches!(session.fork(Some(target), String::new()),
            Err(SessionError::InvalidForkTarget { target: found }) if found == target));
        assert_eq!(session.messages(), messages);
        assert_eq!(session.context_events(), events);
    }

    #[test]
    fn state_compaction_epoch_survives_repeated_load_and_save() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(root),
            generation: 0,
            change: StateChange::Compaction {
                epoch: session.history_snapshot().epoch,
            },
        });
        session.save_to(directory.path()).unwrap();
        let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(loaded.state(), session.state());
        loaded.save_to(directory.path()).unwrap();
        let reloaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(reloaded.state(), session.state());
        let after = fs::read(jsonl_path(directory.path(), session.id)).unwrap();
        assert!(!after.is_empty());
        let records = records(directory.path(), session.id);
        assert_eq!(
            records
                .iter()
                .filter(|record| record["t"] == "state")
                .count(),
            session.state().len()
        );
    }

    #[test]
    fn parent_link_changes_are_latest_values_without_replacing_the_header() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.save_to(directory.path()).unwrap();
        let path = jsonl_path(directory.path(), session.id);
        let parent = crate::session_state::ParentLink {
            session: MakimaId::generate(),
            message: None,
        };
        session.set_parent(Some(parent.clone()));
        session.save_to(directory.path()).unwrap();
        assert!(!fs::read(&path).unwrap().is_empty());
        assert_eq!(
            TestSession::load_from(session.id, directory.path())
                .unwrap()
                .parent(),
            Some(&parent)
        );
        session.set_parent(None);
        session.save_to(directory.path()).unwrap();
        assert!(
            TestSession::load_from(session.id, directory.path())
                .unwrap()
                .parent()
                .is_none()
        );
    }

    #[test]
    fn child_metadata_cannot_replace_canonical_configuration() {
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let seq = session.next_state_seq();
        let mut child = root_record(AgentId::generate(), seq);
        child.parent = Some(root);
        child.transcript_key = POST_AGENT.into();
        let id = child.id;
        session.push_agent_revision(seq, child);
        let config = StoredAgentConfig {
            cwd: "/project".into(),
            model: POST_TEXT.into(),
            thinking: None,
            fast: true,
            workflow: false,
            mode: resolved_mode(),
            options: BTreeMap::new(),
        };
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(id),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        session.set_subagents(vec![StoredSubagent {
            tool_use_id: POST_AGENT.into(),
            agent_id: Some(id.to_string()),
            ambiguous: true,
            name: POST_AGENT.into(),
            model: Some(PENDING_DRAFT.into()),
            thinking: Some(StoredThinking::Off),
            fast: false,
        }]);
        assert_eq!(
            session.latest_state().unwrap().scopes[&Some(id)].config,
            Some(config)
        );
        let projected = &session.subagents()[0];
        assert_eq!(projected.agent_id.as_deref(), Some(id.to_string().as_str()));
        assert_eq!(projected.model.as_deref(), Some(POST_TEXT));
        assert!(projected.thinking.is_none());
        assert!(projected.fast);
    }

    #[test]
    fn v2_header_with_unified_messages_restores_the_root_context() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.bind_root_id(AgentId::generate()).unwrap();
        session.push_message(user_message(PENDING_DRAFT));
        session.save_to(directory.path()).unwrap();
        let mut fixture = records(directory.path(), session.id);
        fixture.retain(|record| !matches!(record["t"].as_str(), Some("agent" | "state")));
        fixture[0]["v"] = serde_json::json!(2);
        fs::write(
            jsonl_path(directory.path(), session.id),
            super::encode_records(&fixture).unwrap(),
        )
        .unwrap();
        let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(loaded.messages(), session.messages());
        loaded.save_to(directory.path()).unwrap();
        assert_eq!(
            TestSession::load_from(session.id, directory.path())
                .unwrap()
                .messages(),
            session.messages()
        );
    }

    #[derive(Clone, serde::Deserialize)]
    struct UnserializableMessage;

    impl serde::Serialize for UnserializableMessage {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom(POST_TEXT))
        }
    }

    impl TitleSource for UnserializableMessage {
        fn first_user_text(&self) -> Option<&str> {
            None
        }
    }

    #[test_case(false; "append_message")]
    #[test_case(true; "replace_history")]
    fn failed_history_serialization_refuses_publication(replace: bool) {
        let directory = TempDir::new().unwrap();
        let mut session: Session<UnserializableMessage, Value, Value> =
            Session::new("model", "/project");
        session.save_to(directory.path()).unwrap();
        let path = jsonl_path(directory.path(), session.id);
        let before = fs::read(&path).unwrap();
        if replace {
            session.replace_messages(vec![UnserializableMessage]);
        } else {
            session.push_message(UnserializableMessage);
        }
        let error = session.save_to(directory.path()).unwrap_err();
        assert!(error.to_string().contains(POST_TEXT));
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn agent_record_populates_an_empty_child_transcript() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let seq = session.next_state_seq();
        let record =
            super::legacy_child(root, POST_AGENT.into(), Some(POST_TEXT.into()), None, seq);
        session.push_agent_revision(seq, record);
        session.save_to(directory.path()).unwrap();
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert!(
            loaded
                .subagent_messages()
                .get(POST_AGENT)
                .unwrap()
                .is_empty()
        );
        assert_eq!(loaded.subagents()[0].name, POST_TEXT);
    }

    fn resolved_mode() -> StoredModeSnapshot {
        StoredModeSnapshot {
            id: "custom-review".into(),
            label: "Review".into(),
            system_prompt: Some("Inspect without changing files".into()),
            restrict_write_to: Some(PathBuf::from("review.md")),
            tools: Some(Vec::new()),
        }
    }

    fn plugin_record(seq: u64, value: Option<Value>) -> StoredStateRecord {
        StoredStateRecord {
            seq,
            agent: None,
            generation: 0,
            change: StateChange::Plugin {
                key: "test.slot".into(),
                version: 1,
                value,
            },
        }
    }

    fn saved_fixture() -> (TempDir, TestSession) {
        let dir = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.push_message(user_message(POST_TEXT));
        session.save_to(dir.path()).unwrap();
        (dir, session)
    }

    #[test]
    fn authoritative_replacement_removes_discarded_collections_and_fences_identical_content() {
        let (dir, mut current) = saved_fixture();
        let replacement = current.clone();
        let child = AgentId::generate();
        let mut record = root_record(child, current.next_state_seq());
        record.parent = current.root_agent_id();
        current.push_agent_revision(current.next_state_seq(), record);
        current.push_state(plugin_record(
            current.next_state_seq(),
            Some(Value::Bool(true)),
        ));
        current.insert_tool_output(POST_AGENT.into(), Value::Bool(true));
        current.set_subagent_messages(POST_AGENT.into(), vec![user_message(PENDING_DRAFT)]);
        current.push_post(stored_post(None, 0, None));
        let high_water = current.sequence_high_water();
        let revision = current.revision();
        current.replace_authoritative(replacement).unwrap();
        assert!(current.revision() > revision);
        assert!(!current.agents().contains_key(&child));
        assert!(current.tool_outputs().is_empty());
        assert!(current.subagent_messages().is_empty());
        assert!(current.posts().is_empty());
        assert_eq!(current.sequence_high_water(), high_water);
        let revision = current.revision();
        let different = TestSession::new("other", "/other");
        assert!(matches!(
            current.replace_authoritative(different),
            Err(SessionError::IdMismatch { .. })
        ));
        assert_eq!(current.revision(), revision);
        current.save_to(dir.path()).unwrap();
        let revision = current.revision();
        let history = current.history_identity();
        current.replace_authoritative(current.clone()).unwrap();
        assert!(current.revision() > revision);
        assert_eq!(current.history_identity(), history);
        current.save_to(dir.path()).unwrap();
        let loaded = TestSession::load_from(current.id, dir.path()).unwrap();
        assert_eq!(loaded.sequence_high_water(), high_water);
        assert!(loaded.next_state_seq() > high_water.unwrap());
        assert_same_session(&loaded, &current);
    }

    #[test]
    fn journal_history_replacement_removes_absent_children_and_fences_identical_history() {
        let (dir, mut session) = saved_fixture();
        let root = session.root_agent_id().unwrap();
        let child = AgentId::generate();
        let mut record = root_record(child, session.next_state_seq());
        record.parent = Some(root);
        record.transcript_key = POST_AGENT.into();
        session.push_agent_revision(record.created_seq, record);
        let stale = HistorySnapshot::new(vec![user_message(POST_TEXT)]);
        session.adopt_subagent_history(ORPHAN_CHILD.into(), &stale);
        let histories = [
            (
                root,
                HistorySnapshot::new(vec![user_message(PENDING_DRAFT)]),
            ),
            (child, HistorySnapshot::new(vec![user_message(POST_TEXT)])),
        ]
        .into();
        session.save_to(dir.path()).unwrap();
        session.replace_journal_histories(&histories);
        assert_eq!(session.history_snapshot().epoch, histories[&root].epoch);
        assert_eq!(
            session.subagent_history_snapshot(POST_AGENT).unwrap().epoch,
            histories[&child].epoch
        );
        assert_eq!(session.messages(), &[user_message(PENDING_DRAFT)]);
        assert_eq!(session.subagent_messages().len(), 2);
        assert_eq!(
            session.subagent_messages()[POST_AGENT].as_ref(),
            &[user_message(POST_TEXT)]
        );
        assert!(!session.adopt_subagent_history(ORPHAN_CHILD.into(), &stale));
        session.save_to(dir.path()).unwrap();
        let revision = session.revision();
        session.replace_journal_histories(&histories);
        assert!(session.revision() > revision);
        let key = session.agents()[&child].transcript_key.clone();
        let mut pending = histories[&child].clone();
        Arc::make_mut(&mut pending.messages).push(user_message(PENDING_DRAFT));
        assert!(session.adopt_subagent_history(key, &pending));
        session.replace_journal_histories(&histories);
        session.save_to(dir.path()).unwrap();
        assert_same_session(
            &TestSession::load_from(session.id, dir.path()).unwrap(),
            &session,
        );
    }

    #[test]
    fn authoritative_replacement_keeps_removed_child_epoch_tombstones() {
        let (dir, mut session) = saved_fixture();
        let replacement = session.clone();
        let stale = HistorySnapshot::new(vec![user_message(POST_TEXT)]);
        assert!(session.adopt_subagent_history(POST_AGENT.into(), &stale));
        session.replace_authoritative(replacement).unwrap();
        assert!(!session.adopt_subagent_history(POST_AGENT.into(), &stale));
        session.prune_orphans(|_| Vec::new());
        session.replace_authoritative(session.clone()).unwrap();
        assert!(!session.adopt_subagent_history(POST_AGENT.into(), &stale));
        assert!(session.subagent_messages().is_empty());
        session.save_to(dir.path()).unwrap();
        assert!(
            TestSession::load_from(session.id, dir.path())
                .unwrap()
                .subagent_messages()
                .is_empty()
        );
        let fresh = HistorySnapshot::new(vec![user_message(PENDING_DRAFT)]);
        assert!(session.adopt_subagent_history(POST_AGENT.into(), &fresh));
    }

    #[test]
    fn individual_prefix_replacements_preserve_sequence_high_water_after_restart() {
        let (dir, mut session) = saved_fixture();
        let high_water = session.next_state_seq() + POST_APPEND_COUNT as u64;
        session.push_state(plugin_record(high_water, Some(Value::Bool(true))));
        session.replace_state(Vec::new());
        session.replace_agent_revisions(Vec::new());
        session.save_to(dir.path()).unwrap();
        let session = TestSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(session.sequence_high_water(), Some(high_water));
        assert_eq!(session.next_state_seq(), high_water + 1);
    }

    fn records(dir: &Path, id: MakimaId) -> Vec<Value> {
        fs::read(jsonl_path(dir, id))
            .unwrap()
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect()
    }

    fn assert_retired_meta_absent(records: &[Value]) {
        for record in records
            .iter()
            .rev()
            .filter(|record| record["t"] == "meta")
            .take(1)
        {
            for key in [
                "plan_path",
                "plan_written",
                "mode",
                "thinking",
                "fast",
                "workflow",
                "session_options",
                "session_rules",
                "yolo",
            ] {
                assert!(
                    record.get(key).is_none(),
                    "unexpected metadata field: {key}"
                );
            }
        }
    }

    #[test]
    fn unresolved_v2_config_survives_save_and_resolution() {
        let tmp = TempDir::new().unwrap();
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let header = serde_json::json!({"t":"header", "v":2, "id":id, "model":"legacy-model", "cwd":"/legacy", "created_at":1});
        let meta = serde_json::json!({
            "t":"meta", "title":"legacy", "token_usage":null, "updated_at":2,
            "mode":{"custom":"removed-mode"}, "thinking":{"kind":"off"},
            "fast":true, "workflow":true, "session_options":{"custom":"option"},
            "session_rules":[{"tool":"bash", "effect":"deny"}], "yolo":false,
            "plan_path":"plan.md", "plan_written":true
        });
        fs::write(jsonl_path(tmp.path(), id), format!("{header}\n{meta}\n")).unwrap();
        let mut session = Arc::new(TestSession::load_from(id, tmp.path()).unwrap());
        let legacy = session.meta.clone();
        TestSession::checkpoint(
            &mut session,
            None,
            SessionMeta {
                input_draft: Some(PENDING_DRAFT.into()),
                ..Default::default()
            },
            Value::Null,
        );
        Arc::make_mut(&mut session).save_to(tmp.path()).unwrap();
        let mut loaded = TestSession::load_from(id, tmp.path()).unwrap();
        assert_eq!(loaded.meta.mode, legacy.mode);
        assert_eq!(loaded.meta.thinking, legacy.thinking);
        assert_eq!(loaded.meta.session_options, legacy.session_options);
        assert!(loaded.meta.fast && loaded.meta.workflow);
        assert_retired_meta_absent(&records(tmp.path(), id));
        assert!(
            records(tmp.path(), id)
                .last()
                .unwrap()
                .get("legacy_config")
                .is_some()
        );
        loaded.save_to(tmp.path()).unwrap();
        loaded.push_message(user_message("before resolution"));
        loaded.save_to(tmp.path()).unwrap();
        let mut loaded = TestSession::load_from(id, tmp.path()).unwrap();
        loaded.resolve_legacy_mode(resolved_mode()).unwrap();
        let count = loaded.state().len();
        loaded.resolve_legacy_mode(resolved_mode()).unwrap();
        assert_eq!(loaded.state().len(), count);
        loaded.save_to(tmp.path()).unwrap();
        loaded.push_message(user_message("after resolution"));
        loaded.save_to(tmp.path()).unwrap();
        let appended_records = records(tmp.path(), id);
        assert_retired_meta_absent(&appended_records);
        assert!(
            appended_records
                .last()
                .unwrap()
                .get("legacy_config")
                .is_none()
        );
        loaded.save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(id, tmp.path()).unwrap();
        let view = loaded.latest_state().unwrap();
        let config = view.scopes[&loaded.root_agent_id()]
            .config
            .as_ref()
            .unwrap();
        assert_eq!(config.thinking, legacy.thinking);
        assert_eq!(config.options, legacy.session_options);
        assert!(config.fast && config.workflow);
        assert_eq!(config.mode, resolved_mode());
        assert_eq!(view.scopes[&None].rules, legacy.session_rules);
        assert_eq!(view.scopes[&None].yolo, legacy.yolo);
        let plans: PlanSlot = serde_json::from_value(
            view.scopes[&None].plugins[PLAN_STATE_KEY]
                .value
                .clone()
                .unwrap(),
        )
        .unwrap();
        let plan = &plans[&loaded.root_agent_id().unwrap()];
        assert_eq!(plan.path, Path::new("plan.md"));
        assert!(plan.written);
        assert!(plan.approved.is_none());
        assert_eq!(loaded.meta.input_draft.as_deref(), Some(PENDING_DRAFT));
        let records = records(tmp.path(), id);
        assert_retired_meta_absent(&records);
        assert!(records.last().unwrap().get("legacy_config").is_none());
    }

    #[test]
    fn config_and_rules_append_ignore_stale_legacy_payload_and_seed_only_once() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        let root = session.root_agent_id().unwrap();
        session.save_to(tmp.path()).unwrap();
        let mut config = session.root_config().unwrap().unwrap();
        config.fast = true;
        config
            .options
            .insert("persistent".into(), "committed".into());
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: None,
            generation: 0,
            change: StateChange::Rules {
                rules: Vec::new(),
                yolo: Some(false),
            },
        });
        session.save_to(tmp.path()).unwrap();
        let count = session.state().len();
        let stale_meta = serde_json::json!({
            "t":"meta", "title":"stale", "token_usage":null, "updated_at":3,
            "legacy_config": {
                "mode":"plan", "thinking":{"kind":"off"}, "fast":false, "workflow":true,
                "session_options":{"persistent":"stale"}, "session_rules":[], "yolo":true
            }
        });
        writeln!(
            OpenOptions::new()
                .append(true)
                .open(jsonl_path(tmp.path(), session.id))
                .unwrap(),
            "{stale_meta}"
        )
        .unwrap();
        let mut loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert!(loaded.legacy_config.is_none());
        loaded.resolve_legacy_mode(resolved_mode()).unwrap();
        assert_eq!(loaded.state().len(), count);
        let view = loaded.latest_state().unwrap();
        assert_eq!(view.scopes[&Some(root)].config.as_ref(), Some(&config));
        assert_eq!(view.scopes[&None].yolo, Some(false));
        loaded.save_to(tmp.path()).unwrap();
        let records = records(tmp.path(), session.id);
        assert_retired_meta_absent(&records);
        assert!(records.last().unwrap().get("legacy_config").is_none());
    }

    #[test]
    fn fresh_root_binding_is_idempotent_and_not_legacy() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let revision = session.revision();
        session.bind_root_id(root).unwrap();
        assert_eq!(session.revision(), revision);
        let agent = &session.agents()[&root];
        assert_eq!(agent.lifecycle, StoredAgentLifecycle::Open);
        assert!(agent.parent.is_none());
        assert!(!agent.legacy && !agent.ancestry_unknown);
        assert!(session.state().is_empty());
        session.save_to(tmp.path()).unwrap();
        assert_same_session(
            &TestSession::load_from(session.id, tmp.path()).unwrap(),
            &session,
        );
    }

    #[test_case(false; "fresh_root")]
    #[test_case(true; "published_root")]
    fn established_root_rebind_refuses_without_mutation_or_disk_changes(reload: bool) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let original = AgentId::generate();
        session.bind_root_id(original).unwrap();
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.save_to(tmp.path()).unwrap();
        if reload {
            session = TestSession::load_from(session.id, tmp.path()).unwrap();
        }
        let before = session.clone();
        let bytes = fs::read(jsonl_path(tmp.path(), session.id)).unwrap();
        let replacement = AgentId::generate();
        assert!(matches!(session.bind_root_id(replacement),
            Err(SessionError::RootIdMismatch { stored_id, given_id })
                if stored_id == original && given_id == replacement));
        assert_eq!(session.revision(), before.revision());
        assert_same_session(&session, &before);
        assert_eq!(fs::read(jsonl_path(tmp.path(), session.id)).unwrap(), bytes);
    }

    #[test]
    fn legacy_config_resolution_seeds_host_config_once_and_preserves_metadata() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("legacy-model", "/legacy");
        let legacy_meta = session.meta.clone();
        let config = StoredAgentConfig {
            cwd: "/restored".into(),
            model: "discovered-model".into(),
            thinking: Some(StoredThinking::Adaptive),
            fast: true,
            workflow: true,
            mode: resolved_mode(),
            options: [("host.option".to_owned(), "restored".to_owned())].into(),
        };
        session.resolve_legacy_config(config.clone()).unwrap();
        let root = session.root_agent_id().unwrap();
        let revision = session.revision();
        let mut different = config.clone();
        different.model = "must-not-overwrite".into();
        session.resolve_legacy_config(different).unwrap();
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        assert_eq!(session.revision(), revision);
        assert_eq!(session.meta, legacy_meta);
        session.save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_same_session(&loaded, &session);
        assert_eq!(
            loaded.latest_state().unwrap().scopes[&Some(root)]
                .config
                .as_ref(),
            Some(&config)
        );
    }

    #[test_case(false, false; "legacy_slot")]
    #[test_case(true, false; "canonical_value_wins")]
    #[test_case(true, true; "canonical_tombstone_wins")]
    fn legacy_plan_migration_respects_canonical_presence(canonical: bool, tombstone: bool) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/tmp");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut record = plugin_record(
            session.next_state_seq(),
            Some(serde_json::json!({"path":"legacy.md", "written":true})),
        );
        if let StateChange::Plugin { key, .. } = &mut record.change {
            *key = LEGACY_PLAN_SLOT.into();
        }
        session.push_state(record);
        if canonical {
            let mut record = plugin_record(
                session.next_state_seq(),
                (!tombstone).then(|| serde_json::json!({})),
            );
            if let StateChange::Plugin { key, .. } = &mut record.change {
                *key = PLAN_STATE_KEY.into();
            }
            session.push_state(record);
        }
        session.save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        let view = loaded.latest_state().unwrap();
        let value = &view.scopes[&None].plugins[PLAN_STATE_KEY].value;
        if canonical {
            assert_eq!(*value, (!tombstone).then(|| serde_json::json!({})));
        } else {
            let plans: PlanSlot = serde_json::from_value(value.clone().unwrap()).unwrap();
            assert_eq!(plans[&root].path, Path::new("legacy.md"));
            assert!(plans[&root].written);
            assert!(plans[&root].approved.is_none());
        }
        let mut loaded = loaded;
        loaded.save_to(tmp.path()).unwrap();
        let reloaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(loaded.state(), reloaded.state());
    }

    #[test_case(2; "v2")]
    #[test_case(3; "v3")]
    fn plan_metadata_is_load_only_and_root_rekey_preserves_plan_map(version: u32) {
        let tmp = TempDir::new().unwrap();
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let header = serde_json::json!({"t":"header", "v":version, "id":id, "model":"model", "cwd":"/tmp", "created_at":1});
        let meta = serde_json::json!({"t":"meta", "title":"plan", "token_usage":null, "updated_at":2, "plan_path":"legacy.md", "plan_written":true});
        fs::write(jsonl_path(tmp.path(), id), format!("{header}\n{meta}\n")).unwrap();
        let mut session = TestSession::load_from(id, tmp.path()).unwrap();
        let previous = session.root_agent_id().unwrap();
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let view = session.latest_state().unwrap();
        let plans: PlanSlot = serde_json::from_value(
            view.scopes[&None].plugins[PLAN_STATE_KEY]
                .value
                .clone()
                .unwrap(),
        )
        .unwrap();
        assert!(!plans.contains_key(&previous));
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[&root].path, Path::new("legacy.md"));
        assert!(plans[&root].written);
        assert!(plans[&root].approved.is_none());
        session.save_to(tmp.path()).unwrap();
        assert_retired_meta_absent(&records(tmp.path(), id));
        let loaded = TestSession::load_from(id, tmp.path()).unwrap();
        assert_eq!(loaded.state(), session.state());
    }

    #[test_case("unique", true; "unique_child")]
    #[test_case("orphan", true; "unlisted_transcript")]
    #[test_case("duplicate", false; "duplicate_transcript_key")]
    #[test_case("ambiguous", false; "ambiguous_identity")]
    #[test_case("missing", false; "missing_transcript")]
    fn v2_post_links_migrate_to_durable_ids_and_survive_rewrite(case: &str, available: bool) {
        const KEY: &str = "legacy-post-child";
        let tmp = TempDir::new().unwrap();
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let header = serde_json::json!({"t":"header", "v":2, "id":id, "model":"m", "cwd":"/tmp", "created_at":1});
        let child =
            serde_json::json!({"tool_use_id":KEY, "name":KEY, "ambiguous":case == "ambiguous"});
        let children = match case {
            "orphan" | "missing" => Vec::new(),
            "duplicate" => vec![child.clone(), child],
            _ => vec![child],
        };
        let meta = serde_json::json!({"t":"meta", "title":"legacy", "token_usage":null, "updated_at":2, "subagents":children});
        let post = serde_json::json!({"t":"post", "d":{"chat":null, "after_message":0, "text":POST_TEXT, "agent":KEY}});
        let history = if case == "missing" {
            String::new()
        } else {
            serde_json::json!({"t":"sub_msg", "sub":KEY, "d":user_message(POST_TEXT)}).to_string()
        };
        fs::write(
            jsonl_path(tmp.path(), id),
            format!("{header}\n{meta}\n{post}\n{history}\n"),
        )
        .unwrap();
        let mut loaded = TestSession::load_from(id, tmp.path()).unwrap();
        let target = loaded.posts()[0].post.agent.clone();
        assert_eq!(target.is_some(), available);
        if let Some(target) = &target {
            let target = target.parse::<AgentId>().unwrap();
            assert_eq!(loaded.agents()[&target].transcript_key, KEY);
            assert_eq!(
                loaded.agents()[&target].lifecycle,
                StoredAgentLifecycle::Closed
            );
        }
        loaded.save_to(tmp.path()).unwrap();
        let rewritten = TestSession::load_from(id, tmp.path()).unwrap();
        assert_eq!(rewritten.posts()[0].post.agent, target);
        assert_eq!(rewritten.posts()[0].post.text, POST_TEXT);
    }

    #[test]
    fn v2_migration_preserves_legacy_metadata_and_resolves_mode_only_on_request() {
        let tmp = TempDir::new().unwrap();
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let header = serde_json::json!({"t":"header", "v":2, "id":id, "model":"legacy-model", "cwd":"/legacy", "created_at":1});
        let child =
            serde_json::json!({"tool_use_id":"duplicate", "agent_id":"old-id", "name":"legacy"});
        let meta = serde_json::json!({
            "t":"meta", "title":"legacy", "token_usage":null, "updated_at":2,
            "subagents":[child.clone(),child], "mode":{"custom":"removed-mode"},
            "thinking":{"kind":"off"}, "fast":true, "workflow":true,
            "session_options":{"custom":"option"}, "session_rules":[{"tool":"bash", "effect":"deny"}],
            "yolo":false, "plan_path":"plan.md", "plan_written":true
        });
        let orphan =
            serde_json::json!({"t":"sub_msg", "sub":"orphan", "d":user_message("orphan history")});
        let path = jsonl_path(tmp.path(), id);
        fs::write(&path, format!("{header}\n{meta}\n{orphan}\n")).unwrap();
        let mut session = TestSession::load_from(id, tmp.path()).unwrap();
        let root = session.root_agent_id().unwrap();
        assert_eq!(
            session.meta.mode,
            Some(StoredMode::Custom("removed-mode".into()))
        );
        let view = session.latest_state().unwrap();
        assert!(
            view.scopes
                .get(&Some(root))
                .is_none_or(|scope| scope.config.is_none())
        );
        assert_eq!(view.scopes[&None].rules, session.meta.session_rules);
        assert_eq!(view.scopes[&None].yolo, Some(false));
        let plans: PlanSlot = serde_json::from_value(
            view.scopes[&None].plugins[PLAN_STATE_KEY]
                .value
                .clone()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(plans[&root].path, Path::new("plan.md"));
        assert!(plans[&root].written);
        assert!(plans[&root].approved.is_none());
        let children: Vec<_> = session
            .agents()
            .values()
            .filter(|agent| agent.parent.is_some())
            .collect();
        assert_eq!(children.len(), 3);
        assert_eq!(
            children
                .iter()
                .map(|agent| agent.id)
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
        for agent in children {
            assert_eq!(agent.parent, Some(root));
            assert!(agent.legacy && agent.ancestry_unknown);
            assert_eq!(agent.lifecycle, StoredAgentLifecycle::Closed);
            assert_eq!(agent.restorable, Err(LEGACY_CHILD_UNRESTORABLE.to_owned()));
            assert!(agent.spec.is_none());
        }
        let legacy_meta = session.meta.clone();
        session.save_to(tmp.path()).unwrap();
        let mut session = TestSession::load_from(id, tmp.path()).unwrap();
        assert_eq!(session.meta, legacy_meta);
        assert_eq!(session.root_agent_id(), Some(root));
        assert!(
            session
                .latest_state()
                .unwrap()
                .scopes
                .get(&Some(root))
                .is_none_or(|scope| scope.config.is_none())
        );
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        let revision = session.revision();
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        assert_eq!(session.revision(), revision);
        session.save_to(tmp.path()).unwrap();
        let reloaded = TestSession::load_from(id, tmp.path()).unwrap();
        assert_same_session(&reloaded, &session);
        let config = reloaded.latest_state().unwrap().scopes[&Some(root)]
            .config
            .clone()
            .unwrap();
        assert_eq!(config.mode, resolved_mode());
        assert_eq!(config.model, "legacy-model");
        assert_eq!(config.cwd, "/legacy");
        assert_eq!(config.thinking, Some(StoredThinking::Off));
        assert!(config.fast && config.workflow);
        assert_eq!(config.options, legacy_meta.session_options);
        assert_eq!(
            reloaded.subagent_messages()["orphan"][0],
            user_message("orphan history")
        );
    }

    #[test_case(b"{\"t\":\"state\",\"d\":{}}\n"; "malformed_state")]
    #[test_case(b"{\"t\":\"agent\",\"seq\":1,\"d\":{}}\n"; "malformed_agent")]
    fn malformed_v3_records_refuse_load_without_modifying_disk(record: &[u8]) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.save_to(tmp.path()).unwrap();
        let path = jsonl_path(tmp.path(), session.id);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(record)
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        assert!(matches!(
            TestSession::load_from(session.id, tmp.path()),
            Err(SessionError::UnreadableRecord { .. })
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    impl TitleSource for Value {
        fn first_user_text(&self) -> Option<&str> {
            if self.get("role")?.as_str()? != "user" {
                return None;
            }
            self.get("content")?.as_array()?.iter().find_map(|b| {
                if b.get("type")?.as_str()? == "text" {
                    let text = b.get("text")?.as_str()?;
                    (!text.is_empty()).then_some(text)
                } else {
                    None
                }
            })
        }
    }

    fn user_message(text: &str) -> Value {
        text_message("user", text)
    }

    fn assistant_message(text: &str) -> Value {
        text_message("assistant", text)
    }

    fn text_message(role: &str, text: &str) -> Value {
        serde_json::json!({
            "role": role,
            "content": [{"type": "text", "text": text}]
        })
    }

    fn write_jsonl(path: &Path, session: &TestSession) {
        let mut file = std::fs::File::create(path).unwrap();
        write_full_session(&mut file, session).unwrap();
    }

    fn append_raw_msg(path: &Path, message: Value) {
        // Tag first, matching the `SessionRecord` layout the real appends write:
        // the scan classifies lines by that prefix.
        let record = format!(r#"{{"t":"msg","d":{message}}}"#);
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(record.as_bytes()).unwrap();
        file.write_all(b"\n").unwrap();
    }

    #[test]
    fn prune_orphans_drops_unreachable_tool_state() {
        fn ids(m: &Value) -> Vec<String> {
            vec![m.as_str().unwrap().to_owned()]
        }
        fn subagent(id: &str) -> StoredSubagent {
            StoredSubagent {
                tool_use_id: id.into(),
                agent_id: None,
                ambiguous: false,
                name: "sub".into(),
                model: None,
                thinking: None,
                fast: false,
            }
        }

        let mut session: TestSession = Session::new("model", "/p");
        session.push_message("task-live".into());
        session
            .subagent_messages
            .insert("task-live".into(), Arc::new(vec!["sub-tool".into()]));
        session
            .subagent_messages
            .insert("task-stale".into(), Arc::new(vec!["stale-sub-tool".into()]));
        session.set_subagents(vec![subagent("task-live"), subagent("task-stale")]);
        for id in ["task-live", "sub-tool", "stale-sub-tool", "orphan"] {
            session.insert_tool_output(id.into(), Value::Null);
        }

        session.prune_orphans(ids);

        assert_eq!(
            session.subagent_messages().keys().collect::<Vec<_>>(),
            ["task-live"]
        );
        let subagent_ids: Vec<_> = session
            .subagents()
            .iter()
            .map(|sa| sa.tool_use_id.as_str())
            .collect();
        assert_eq!(subagent_ids, ["task-live"]);
        let mut outputs: Vec<_> = session.tool_outputs().keys().cloned().collect();
        outputs.sort();
        assert_eq!(outputs, ["sub-tool", "task-live"]);
    }

    #[test]
    fn roundtrip_save_load() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession =
            Session::new("anthropic/claude-sonnet-4", "/home/test/project");
        session.push_message(user_message("hello"));
        session.set_subagent_messages(
            "tool-1".into(),
            vec![user_message("sub-prompt"), assistant_message("sub-reply")],
        );
        session.save_to(dir).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.id, session.id);
        assert_eq!(loaded.model, "anthropic/claude-sonnet-4");
        assert_eq!(loaded.cwd, "/home/test/project");
        assert_eq!(loaded.messages().len(), 1);
        assert_eq!(loaded.subagent_messages["tool-1"].len(), 2);
    }

    #[test_case(true; "duplicate_key")]
    #[test_case(false; "duplicate_agent_id")]
    fn duplicate_metadata_projects_to_canonical_child_records(duplicate_key: bool) {
        let child = StoredSubagent {
            tool_use_id: "first".into(),
            agent_id: Some(AgentId::generate().to_string()),
            ambiguous: false,
            name: POST_AGENT.into(),
            model: None,
            thinking: None,
            fast: false,
        };
        let mut other = child.clone();
        if !duplicate_key {
            other.tool_use_id = "second".into();
        }
        let mut session = TestSession::new("model", "/project");
        session.set_subagents(vec![child, other]);
        let directory = TempDir::new().unwrap();
        session.save_to(directory.path()).unwrap();
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(loaded.subagents().len(), if duplicate_key { 1 } else { 2 });
        assert!(
            loaded
                .subagents()
                .iter()
                .all(|child| child.name == POST_AGENT)
        );
        assert_eq!(loaded.subagents(), session.subagents());
    }

    #[test]
    fn roundtrip_usage_by_model() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("anthropic/claude-sonnet-4", "/project");
        session.add_model_usage(
            "claude-sonnet-4",
            super::StoredTokenUsage {
                input: 100,
                output: 20,
                cache_creation: 5,
                cache_read: 40,
                cost: Some(SONNET_COST),
            },
        );
        session.add_model_usage(
            "claude-haiku-4",
            super::StoredTokenUsage {
                input: 30,
                output: 10,
                cost: Some(HAIKU_COST),
                ..Default::default()
            },
        );
        session.save_to(dir).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        let sonnet = &loaded.usage_by_model()["claude-sonnet-4"];
        assert_eq!(sonnet.input, 100);
        assert_eq!(sonnet.output, 20);
        assert_eq!(sonnet.cache_read, 40);
        assert_eq!(sonnet.total_input(), 145);
        assert_eq!(sonnet.cost, Some(SONNET_COST));
        assert_eq!(loaded.usage_by_model()["claude-haiku-4"].total(), 40);
        assert_eq!(
            loaded.usage_by_model()["claude-haiku-4"].cost,
            Some(HAIKU_COST)
        );
    }

    /// A turn that reports no price must not erase what was already billed.
    #[test_case(None, None, None ; "unpriced_stays_unpriced")]
    #[test_case(None, Some(SONNET_COST), Some(SONNET_COST) ; "first_price_starts_the_total")]
    #[test_case(Some(SONNET_COST), Some(HAIKU_COST), Some(SONNET_COST + HAIKU_COST) ; "priced_turns_accumulate")]
    #[test_case(Some(SONNET_COST), None, Some(SONNET_COST) ; "unpriced_turn_keeps_the_total")]
    fn add_cost_only_grows_a_total(
        mut total: Option<f64>,
        addend: Option<f64>,
        expected: Option<f64>,
    ) {
        super::add_cost(&mut total, addend);
        assert_eq!(total, expected);
    }

    fn usage(input: u32, cost: Option<f64>) -> super::StoredTokenUsage {
        super::StoredTokenUsage {
            input,
            cost,
            ..Default::default()
        }
    }

    fn saved_usage_by_model(dir: &Path, id: MakimaId) -> Value {
        let text = fs::read_to_string(jsonl_path(dir, id)).unwrap();
        let meta = text
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .rfind(|record| record["t"] == "meta")
            .expect("saved session has a meta record");
        meta["usage_by_model"].clone()
    }

    /// An entry without `cost` loads unpriced with its counters intact, and
    /// saving it back must not mint one.
    #[test]
    fn unpriced_usage_entry_stays_unpriced_on_disk() {
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let json = format!(
            r#"{{"t":"header","v":2,"id":"{FIXTURE_ID}","model":"m","cwd":"/","created_at":0}}
{{"t":"meta","title":"t","token_usage":null,"updated_at":0,"usage_by_model":{{"m":{{"input":7,"output":3}}}}}}"#
        );
        let tmp = TempDir::new().unwrap();
        fs::write(
            tmp.path().join(format!("{FIXTURE_ID}.jsonl")),
            format!("{json}\n"),
        )
        .unwrap();

        let mut loaded = TestSession::load_from(id, tmp.path()).unwrap();
        let entry = loaded.usage_by_model()["m"];
        assert_eq!(entry.cost, None, "no key means unpriced, not free");
        assert_eq!((entry.input, entry.output), (7, 3));

        let dir = tmp.path().join("rewritten");
        fs::create_dir(&dir).unwrap();
        loaded.save_to(&dir).unwrap();
        assert!(
            saved_usage_by_model(&dir, id)["m"].get("cost").is_none(),
            "an unpriced entry writes no cost key"
        );
    }

    /// What a turn billed is written verbatim, read back verbatim, and keeps
    /// adding up after a reload. A later unpriced turn must not throw away what
    /// the earlier ones paid.
    #[test]
    fn recorded_costs_survive_a_reload_and_keep_adding_up() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("anthropic/claude-sonnet-4", "/project");
        session.add_model_usage("claude-sonnet-4", usage(100, Some(SONNET_COST)));
        session.add_model_usage("claude-haiku-4", usage(30, None));
        session.save_to(dir).unwrap();

        let on_disk = saved_usage_by_model(dir, session.id);
        assert_eq!(on_disk["claude-sonnet-4"]["cost"], Value::from(SONNET_COST));

        let mut loaded = TestSession::load_from(session.id, dir).unwrap();
        loaded.add_model_usage("claude-sonnet-4", usage(50, None));
        loaded.add_model_usage("claude-haiku-4", usage(10, Some(HAIKU_COST)));

        let sonnet = loaded.usage_by_model()["claude-sonnet-4"];
        assert_eq!((sonnet.input, sonnet.cost), (150, Some(SONNET_COST)));
        let haiku = loaded.usage_by_model()["claude-haiku-4"];
        assert_eq!((haiku.input, haiku.cost), (40, Some(HAIKU_COST)));
    }

    #[test]
    fn usage_by_model_absent_when_meta_omits_it() {
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let json = format!(
            r#"{{"t":"header","v":2,"id":"{FIXTURE_ID}","model":"m","cwd":"/","created_at":0}}
{{"t":"meta","title":"t","token_usage":null,"updated_at":0}}"#
        );
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(format!("{FIXTURE_ID}.jsonl"));
        fs::write(&path, json).unwrap();
        let loaded = TestSession::load_from(id, tmp.path()).unwrap();
        assert!(loaded.usage_by_model().is_empty());
    }

    #[test]
    fn roundtrip_jsonl_incremental() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));

        session.save_to(dir).unwrap();

        session.push_message(assistant_message("reply"));
        session.push_message(user_message("second"));
        session.tool_outputs.insert(
            "tool-1".into(),
            Arc::new(serde_json::json!({"result": "ok"})),
        );
        session.set_subagent_messages("sub-1".into(), vec![user_message("sub-prompt")]);
        session.save_to(tmp.path()).unwrap();

        session.set_subagent_messages(
            "sub-1".into(),
            vec![user_message("sub-prompt"), assistant_message("sub-reply")],
        );
        session.set_subagent_messages("sub-2".into(), vec![user_message("sub-2-prompt")]);
        session.save_to(tmp.path()).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 3);
        assert_eq!(loaded.tool_outputs().len(), 1);
        assert!(loaded.tool_outputs().contains_key("tool-1"));
        assert_eq!(loaded.subagent_messages["sub-1"].len(), 2);
        assert_eq!(loaded.subagent_messages["sub-2"].len(), 1);
    }

    fn stored_post(chat: Option<&str>, _after_message: usize, agent: Option<&str>) -> StoredPost {
        StoredPost {
            chat: chat.map(str::to_owned),
            after_message: None,
            post: Post {
                text: POST_TEXT.into(),
                level: PostLevel::Warn,
                agent: agent.map(str::to_owned),
            },
        }
    }

    #[test_case(false; "in_memory")]
    #[test_case(true; "reloaded")]
    fn durable_post_references_retain_transcript_keys(reload: bool) {
        const CHILD_KEY: &str = "post-child-transcript";
        const DESCENDANT_KEY: &str = "post-descendant-transcript";
        const ORPHAN_KEY: &str = "post-orphan-transcript";
        let mut session = TestSession::new("m", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut ids = Vec::new();
        for key in [CHILD_KEY, DESCENDANT_KEY, ORPHAN_KEY] {
            let mut record = root_record(
                AgentId::generate(),
                session.agent_revisions().last().unwrap().0 + 1,
            );
            record.parent = Some(root);
            record.depth = 1;
            record.transcript_key = key.into();
            ids.push(record.id);
            session.push_agent_revision(record.created_seq, record);
            session.set_subagent_messages(key.into(), vec![user_message(key)]);
        }
        let kept = stored_post(None, 0, Some(&ids[0].to_string()));
        let nested = stored_post(Some(CHILD_KEY), 0, Some(&ids[1].to_string()));
        session.push_post(kept.clone());
        session.push_post(nested.clone());
        session.push_post(stored_post(Some(ORPHAN_KEY), 0, Some(&ids[2].to_string())));
        let tmp = TempDir::new().unwrap();
        if reload {
            session.save_to(tmp.path()).unwrap();
            session = TestSession::load_from(session.id, tmp.path()).unwrap();
        }
        assert_eq!(session.subagent_messages().len(), ids.len());
        session.prune_orphans(|_| Vec::new());
        assert_eq!(
            session.subagent_messages()[CHILD_KEY].as_slice(),
            [user_message(CHILD_KEY)]
        );
        assert_eq!(
            session.subagent_messages()[DESCENDANT_KEY].as_slice(),
            [user_message(DESCENDANT_KEY)]
        );
        assert!(!session.subagent_messages().contains_key(ORPHAN_KEY));
        assert_eq!(session.posts(), [kept, nested]);
    }

    #[test]
    fn prune_orphans_retains_live_children_and_recursive_message_and_post_references() {
        fn ids(message: &Value) -> Vec<String> {
            message
                .as_str()
                .map(|id| vec![id.to_owned()])
                .unwrap_or_default()
        }
        let mut session = TestSession::new("m", "/project");
        session.push_message(Value::String(POST_AGENT.into()));
        for (id, references) in [
            (POST_AGENT, vec![Value::String(MESSAGE_DESCENDANT.into())]),
            (MESSAGE_DESCENDANT, vec![Value::String(POST_AGENT.into())]),
            (POST_DESCENDANT, Vec::new()),
            (LIVE_CHILD, Vec::new()),
            (ORPHAN_CHILD, Vec::new()),
        ] {
            session.set_subagent_messages(id.into(), references);
            session.insert_tool_output(id.into(), Value::Null);
        }
        session.push_post(stored_post(
            Some(MESSAGE_DESCENDANT),
            0,
            Some(POST_DESCENDANT),
        ));
        session.push_post(stored_post(Some(ORPHAN_CHILD), 0, Some(ORPHAN_CHILD)));
        session.prune_orphans_preserving(ids, &[LIVE_CHILD.into()]);
        for id in [POST_AGENT, MESSAGE_DESCENDANT, POST_DESCENDANT, LIVE_CHILD] {
            assert!(session.subagent_messages().contains_key(id));
            assert!(session.tool_outputs().contains_key(id));
        }
        assert!(!session.subagent_messages().contains_key(ORPHAN_CHILD));
        assert!(!session.tool_outputs().contains_key(ORPHAN_CHILD));
        assert_eq!(
            session.posts(),
            [stored_post(
                Some(MESSAGE_DESCENDANT),
                0,
                Some(POST_DESCENDANT)
            )]
        );
        let revision = session.revision();
        session.prune_orphans_preserving(ids, &[LIVE_CHILD.into()]);
        assert_eq!(session.revision(), revision);
    }

    /// The mirror re-adopts the run's snapshot on every checkpoint; that must
    /// not erase the void minted by a same-frame in-place replacement, or the
    /// writer appends onto a stale prefix and persists a mixed transcript.
    #[test]
    fn bootstrap_history_uses_current_root_metadata_over_retained_events() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let mut snapshot = HistorySnapshot::new(vec![user_message(PENDING_DRAFT)]);
        snapshot.conversation = Some(super::ConversationState::new("old-conversation".into()));
        session.adopt_history(&snapshot);
        session.meta.compacted = true;
        let current = super::ConversationState::new("current-conversation".into());
        session.meta.conversation = Some(current.clone());
        session.save_to(directory.path()).unwrap();
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert!(loaded.history_snapshot().compacted);
        assert_eq!(loaded.history_snapshot().conversation, Some(current));
    }

    #[test]
    fn compacted_history_metadata_round_trips_and_survives_meta_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mut session: Arc<TestSession> = Arc::new(Session::new("m", "/project"));
        let mut snapshot = HistorySnapshot::new(vec![user_message("summary")]);
        snapshot.compacted = true;
        Session::checkpoint(
            &mut session,
            Some(&snapshot),
            SessionMeta::default(),
            Value::Null,
        );
        Session::checkpoint(&mut session, None, SessionMeta::default(), Value::Null);
        assert!(session.history_snapshot().compacted);
        Arc::make_mut(&mut session).save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert!(loaded.history_snapshot().compacted);
    }

    #[test]
    fn crash_recovery_truncated_line() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("survives"));
        session.save_to(dir).unwrap();

        let path = jsonl_path(dir, session.id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"t\":\"msg\",\"d\":{\"trun").unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 1);
    }

    #[test]
    fn rewind_compact() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        for i in 0..10 {
            session.push_message(user_message(&format!("msg-{i}")));
        }
        session.set_subagent_messages(
            "sub-1".into(),
            vec![user_message("sub-prompt"), assistant_message("sub-reply")],
        );
        session.save_to(dir).unwrap();

        session.truncate_messages(5);
        session.tool_outputs.clear();
        session.subagent_messages.remove("sub-1");
        session.save_to(dir).unwrap();

        session.push_message(user_message("after-compact-1"));
        session.push_message(user_message("after-compact-2"));
        session.push_message(user_message("after-compact-3"));
        session.save_to(tmp.path()).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 8);
        assert_eq!(loaded.subagent_messages()["sub-1"].len(), 2);
    }

    fn archive_dir_for(dir: &Path, id: MakimaId) -> PathBuf {
        dir.join(ARCHIVE_DIR).join(id.to_string())
    }

    #[test]
    fn rewrite_without_shrink_does_not_archive() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.save_to(dir).unwrap();

        session.push_message(user_message("two"));
        session.save_to(dir).unwrap();

        assert!(!archive_dir_for(dir, session.id).exists());
    }

    #[test]
    fn delete_removes_offload_dir() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.save_to(dir).unwrap();
        let offload = offload_dir(dir, session.id);
        fs::create_dir_all(&offload).unwrap();
        fs::write(offload.join("0123456789abcdef.txt"), "output").unwrap();

        TestSession::delete_from(session.id, dir).unwrap();
        assert!(!offload.exists());
    }

    #[cfg(any(unix, windows))]
    fn prepare_delete_targets(dir: &Path) -> (TestSession, PathBuf, PathBuf) {
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.push_message(user_message("two"));
        session.save_to(dir).unwrap();
        session.replace_messages(vec![user_message("summary")]);
        session.save_to(dir).unwrap();

        let archive = archive_dir_for(dir, session.id);
        let offload = offload_dir(dir, session.id);
        fs::create_dir_all(&offload).unwrap();
        fs::write(offload.join(OFFLOAD_SENTINEL_FILE), OFFLOAD_SENTINEL).unwrap();
        (session, archive, offload)
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn delete_supports_trusted_root_aliases() {
        let tmp = TempDir::new().unwrap();
        let actual_root = tmp.path().join("trusted-root");
        let actual_sessions = actual_root.join(SESSIONS_DIR);
        fs::create_dir_all(&actual_sessions).unwrap();
        let (session, archive, offload) = prepare_delete_targets(&actual_sessions);
        let alias = tmp.path().join("trusted-alias");
        symlink_dir(&actual_root, &alias).unwrap();

        TestSession::delete(session.id, &StateDir::from_path(alias)).unwrap();

        assert!(!jsonl_path(&actual_sessions, session.id).exists());
        assert!(!archive.exists());
        assert!(!offload.exists());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn delete_does_not_follow_replaced_sessions_directory() {
        let tmp = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let state_root = tmp.path().join("trusted-root");
        let sessions_dir = state_root.join(SESSIONS_DIR);
        fs::create_dir_all(&state_root).unwrap();
        let external_sessions = external.path().join(SESSIONS_DIR);
        fs::create_dir_all(&external_sessions).unwrap();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.save_to(&external_sessions).unwrap();
        let external_jsonl = jsonl_path(&external_sessions, session.id);
        let sentinel = external_sessions.join(OFFLOAD_SENTINEL_FILE);
        fs::write(&sentinel, OFFLOAD_SENTINEL).unwrap();
        symlink_dir(&external_sessions, &sessions_dir).unwrap();

        let result = TestSession::delete(session.id, &StateDir::from_path(state_root));

        assert!(external_jsonl.exists());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), OFFLOAD_SENTINEL);
        assert!(result.is_err());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn delete_removes_nested_offload_links_without_following_them() {
        let tmp = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.save_to(dir).unwrap();
        let offload = offload_dir(dir, session.id);
        fs::create_dir_all(offload.join("nested")).unwrap();
        fs::write(offload.join("nested/output.txt"), "local output").unwrap();
        let sentinel = external.path().join(OFFLOAD_SENTINEL_FILE);
        fs::write(&sentinel, OFFLOAD_SENTINEL).unwrap();
        symlink_dir(external.path(), offload.join("nested/link")).unwrap();

        TestSession::delete_from(session.id, dir).unwrap();
        assert!(!offload.exists());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), OFFLOAD_SENTINEL);
        assert_eq!(fs::read_dir(external.path()).unwrap().count(), 1);
    }

    #[test]
    fn load_nonexistent_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let id = MakimaId::generate();
        let err = TestSession::load_from(id, tmp.path()).unwrap_err();
        assert!(matches!(
            err,
            SessionError::Storage(StorageError::NotFound(_))
        ));
    }

    #[test]
    fn list_filters_by_cwd() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s1: TestSession = Session::new("m", "/project-a");
        let mut s2: TestSession = Session::new("m", "/project-b");
        let mut s3: TestSession = Session::new("m", "/project-a");
        s1.save_to(dir).unwrap();
        s2.save_to(dir).unwrap();
        s3.save_to(dir).unwrap();

        let list = TestSession::list_in("/project-a", dir).unwrap();
        assert_eq!(list.len(), 2);
        assert!(list.iter().all(|s| s.id != s2.id));
        assert!(list.iter().all(|s| s.cwd == "/project-a"));
        assert!(list.iter().all(|s| s.message_count == 0));
    }

    #[test]
    fn list_all_in_returns_every_cwd_with_directory() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s1: TestSession = Session::new("m", "/a");
        s1.updated_at = 100;
        s1.save_to(dir).unwrap();
        let mut s2: TestSession = Session::new("m", "/b");
        s2.updated_at = 200;
        s2.save_to(dir).unwrap();

        let list = TestSession::list_all_in(dir).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].id, s2.id);
        assert_eq!(list[0].cwd, "/b");
        assert_eq!(list[1].id, s1.id);
        assert_eq!(list[1].cwd, "/a");
    }

    #[test]
    fn list_in_flags_open_elsewhere() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.save_to(dir).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert!(!list[0].open_elsewhere, "no lock yet");

        fs::write(
            session_lock::lock_path(dir, &s.id),
            FAKE_LOCK_PID.to_string(),
        )
        .unwrap();
        let list = TestSession::list_in("/project", dir).unwrap();
        assert!(list[0].open_elsewhere, "fresh foreign lock");
        let list = TestSession::list_all_in(dir).unwrap();
        assert!(list[0].open_elsewhere);
    }

    #[test]
    fn delete_from_is_not_blocked_by_own_session_lock() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.save_to(dir).unwrap();
        let lease = session_lock::claim(dir, &s.id).unwrap().unwrap();

        TestSession::delete_from(s.id, dir).unwrap();
        lease.release().unwrap();
    }

    /// Rewrites the scan-cache title of `id` without touching the session
    /// file, so a later list showing [`TAMPERED_TITLE`] proves it was served
    /// from the cache instead of re-reading the file.
    fn tamper_cached_title(dir: &Path, id: MakimaId) {
        let cache_path = dir.join(SCAN_CACHE_FILE);
        let mut cache: Value = serde_json::from_slice(&fs::read(&cache_path).unwrap()).unwrap();
        let entry = cache
            .get_mut("entries")
            .and_then(Value::as_object_mut)
            .unwrap()
            .get_mut(&format!("{id}.jsonl"))
            .expect("session missing from scan cache");
        entry["header"]["title"] = TAMPERED_TITLE.into();
        fs::write(&cache_path, serde_json::to_vec(&cache).unwrap()).unwrap();
    }

    /// One scan must cache headers of every cwd, so reopening the picker
    /// here or in another project never re-reads unchanged files.
    #[test]
    fn list_serves_all_cwds_from_cache_after_one_scan() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut a: TestSession = Session::new("m", "/project-a");
        a.save_to(dir).unwrap();
        let mut b: TestSession = Session::new("m", "/project-b");
        b.save_to(dir).unwrap();
        TestSession::list_in("/project-a", dir).unwrap();

        tamper_cached_title(dir, a.id);
        tamper_cached_title(dir, b.id);
        let list_a = TestSession::list_in("/project-a", dir).unwrap();
        assert_eq!(list_a[0].title, TAMPERED_TITLE);
        let list_b = TestSession::list_in("/project-b", dir).unwrap();
        assert_eq!(list_b[0].title, TAMPERED_TITLE);
    }

    #[test]
    fn list_rescans_changed_file_and_prunes_deleted() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s1: TestSession = Session::new("m", "/project");
        s1.push_message(user_message("hi"));
        s1.save_to(dir).unwrap();
        let mut s2: TestSession = Session::new("m", "/project");
        s2.save_to(dir).unwrap();
        TestSession::list_in("/project", dir).unwrap();

        s1.title = "renamed".into();
        s1.save_to(dir).unwrap();
        TestSession::delete_from(s2.id, dir).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].title, "renamed");
        let cache: Value =
            serde_json::from_slice(&fs::read(dir.join(SCAN_CACHE_FILE)).unwrap()).unwrap();
        assert_eq!(cache["version"], super::SCAN_CACHE_VERSION);
        assert_eq!(
            cache["entries"].as_object().unwrap().len(),
            1,
            "deleted entry pruned"
        );
    }

    #[test]
    fn dirty_persisted_title_normalized_on_list_and_load() {
        const NORMALIZED: &str = "line one line two";
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.push_message(user_message("hi"));
        s.save_to(dir).unwrap();
        s.title = "line one\n\n\tline two".into();
        s.save_to(tmp.path()).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list[0].title, NORMALIZED);
        assert_eq!(list[0].message_count, 1);
        assert_eq!(TestSession::load_from(s.id, dir).unwrap().title, NORMALIZED);
    }

    #[test_case(Some(b"{ not json".as_slice()) ; "corrupt_cache")]
    #[test_case(None ; "missing_cache")]
    fn list_survives_bad_scan_cache(content: Option<&[u8]>) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.save_to(dir).unwrap();
        if let Some(content) = content {
            fs::write(dir.join(SCAN_CACHE_FILE), content).unwrap();
        }

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, s.id);
    }

    fn save_with_time(session: &mut TestSession, dir: &Path, time: u64) {
        session.updated_at = time;
        session.save_to(dir).unwrap();
        update_cwd_index(dir, &session.cwd, session.id).unwrap();
    }

    #[test]
    fn latest_returns_most_recent_for_cwd() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s1: TestSession = Session::new("m", "/project");
        s1.title = "first".into();
        save_with_time(&mut s1, dir, 1000);

        let mut s2: TestSession = Session::new("m", "/other");
        save_with_time(&mut s2, dir, 2000);

        let mut s3: TestSession = Session::new("m", "/project");
        s3.title = "latest".into();
        save_with_time(&mut s3, dir, 3000);

        let latest = TestSession::latest_in("/project", dir).unwrap().unwrap();
        assert_eq!(latest.title, "latest");
    }

    #[test]
    fn latest_falls_back_when_index_stale() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.save_to(dir).unwrap();

        let index_path = dir.join(CWD_INDEX_FILE);
        let stale: HashMap<String, String> = [("/project".into(), "deleted-id".into())].into();
        fs::write(&index_path, serde_json::to_vec(&stale).unwrap()).unwrap();

        let latest = TestSession::latest_in("/project", dir).unwrap().unwrap();
        assert_eq!(latest.id, session.id);
    }

    #[test_case("short title", "short title" ; "short_passthrough")]
    #[test_case("", DEFAULT_TITLE ; "empty_defaults")]
    #[test_case(
        "This is a very long title that exceeds the sixty character limit and should be truncated at a word boundary",
        "This is a very long title that exceeds the sixty character…"
        ; "long_truncates_at_word"
    )]
    #[test_case("one\n\ntwo\t three", "one two three" ; "whitespace_collapses")]
    fn title_extraction(input: &str, expected: &str) {
        let messages: Vec<Value> = if input.is_empty() {
            vec![]
        } else {
            vec![user_message(input)]
        };
        assert_eq!(generate_title(&messages), expected);
    }

    #[test]
    fn delete_removes_file_and_cwd_index() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s1: TestSession = Session::new("m", "/project");
        s1.save_to(dir).unwrap();
        let mut s2: TestSession = Session::new("m", "/other");
        s2.save_to(dir).unwrap();

        TestSession::delete_from(s1.id, dir).unwrap();
        assert!(!jsonl_path(dir, s1.id).exists());
        let index = load_cwd_index(dir);
        assert!(!index.values().any(|v| *v == s1.id.to_string()));
        assert_eq!(index.get("/other"), Some(&s2.id.to_string()));
    }

    #[test]
    fn delete_nonexistent_returns_not_found() {
        let tmp = TempDir::new().unwrap();
        let id = MakimaId::generate();
        let err = TestSession::delete_from(id, tmp.path()).unwrap_err();
        assert!(matches!(
            err,
            SessionError::Storage(StorageError::NotFound(_))
        ));
    }

    #[test_case(true; "state_root_exists")]
    #[test_case(false; "state_root_missing")]
    fn delete_missing_sessions_directory_returns_not_found(state_root_exists: bool) {
        let tmp = TempDir::new().unwrap();
        let state_root = tmp.path().join("state");
        if state_root_exists {
            fs::create_dir(&state_root).unwrap();
        }
        let state_dir = StateDir::from_path(state_root);
        let id = MakimaId::generate();

        let err = TestSession::delete(id, &state_dir).unwrap_err();
        assert!(matches!(
            err,
            SessionError::Storage(StorageError::NotFound(missing_id)) if missing_id == id.to_string()
        ));
    }

    #[test]
    fn load_with_corrupt_header_id_reports_missing_header() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let id = MakimaId::generate();
        let mut session: TestSession = Session::new("m", "/project");
        session.id = id;

        let path = jsonl_path(dir, id);
        write_jsonl(&path, &session);

        let corrupted =
            fs::read_to_string(&path)
                .unwrap()
                .replacen(&id.to_string(), "not-a-valid-id", 1);
        fs::write(&path, corrupted).unwrap();

        let err = TestSession::load_from(id, dir).unwrap_err();
        assert!(matches!(err, SessionError::MissingHeader { .. }));
    }

    #[test]
    fn title_unicode_safe() {
        let input = "あ".repeat(100);
        let title = generate_title(&[user_message(&input)]);
        assert!(title.len() <= MAX_TITLE_LEN * 4);
        assert!(title.is_char_boundary(title.len()));
    }

    #[test]
    fn scan_counts_msg_records_and_ignores_other_tags() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.push_message(user_message("one"));
        s.push_message(user_message("two"));
        s.set_subagent_messages("sub".into(), vec![user_message("hidden")]);
        s.insert_tool_output("t1".into(), Value::String("output".into()));
        write_jsonl(&jsonl_path(dir, s.id), &s);

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].message_count, 2);
    }

    #[test]
    fn scan_counts_appended_messages_after_cache_invalidation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.push_message(user_message("one"));
        let path = jsonl_path(dir, s.id);
        write_jsonl(&path, &s);
        assert_eq!(
            TestSession::list_in("/project", dir).unwrap()[0].message_count,
            1
        );

        append_raw_msg(&path, user_message("two"));
        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list[0].message_count, 2, "changed file rescans its count");
    }

    #[test]
    fn scan_ignores_msg_shaped_lines_that_are_not_msg_records() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.push_message(user_message("one"));
        let path = jsonl_path(dir, s.id);
        write_jsonl(&path, &s);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        for noise in [
            r#"{"t":"msgx","d":{}}"#,
            r#"{"t":"sub_msg","d":{}}"#,
            r#"{"t":"msg2","d":{}}"#,
            "not json at all",
        ] {
            writeln!(file, "{noise}").unwrap();
        }
        drop(file);

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].message_count, 1);
    }

    #[test]
    fn scan_ignores_torn_msg_tail_like_the_loader() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.push_message(user_message("one"));
        let path = jsonl_path(dir, s.id);
        write_jsonl(&path, &s);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(br#"{"t":"msg","d":{"trun"#).unwrap();
        drop(file);

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list[0].message_count, 1, "a torn tail does not count");
        assert_eq!(
            TestSession::load_from(s.id, dir).unwrap().messages().len(),
            1
        );
    }

    #[test]
    fn session_meta_default_yolo_is_none() {
        let meta = SessionMeta::default();
        assert!(meta.yolo.is_none());
    }

    #[test]
    fn legacy_yolo_seeds_rules_without_metadata_authoring() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("test/model", "/tmp");
        session.meta.yolo = Some(true);
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.save_to(dir).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(
            loaded.latest_state().unwrap().scopes[&None].yolo,
            Some(true)
        );
        assert_eq!(loaded.meta.yolo, None);
    }

    #[test]
    fn load_wrong_version_jsonl_returns_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let bad_header = serde_json::json!({
            "t": "header",
            "v": 999,
            "id": FIXTURE_ID,
            "model": "m",
            "cwd": "/tmp",
            "created_at": 0
        });
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let path = jsonl_path(dir, id);
        fs::write(&path, format!("{}\n", bad_header)).unwrap();

        let err = TestSession::load_from(id, dir).unwrap_err();
        assert!(matches!(
            err,
            SessionError::VersionMismatch { found: 999, .. }
        ));
    }

    #[test_case(StoredThinking::Off ; "off")]
    #[test_case(StoredThinking::Adaptive ; "adaptive")]
    #[test_case(StoredThinking::Effort { level: Effort::XHigh } ; "effort")]
    #[test_case(StoredThinking::Budget { tokens: 4096 } ; "budget")]
    fn stored_thinking_serde_round_trip(variant: StoredThinking) {
        let json = serde_json::to_string(&variant).unwrap();
        let parsed: StoredThinking = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, variant);
    }

    #[test_case("off", Ok(StoredThinking::Off) ; "off")]
    #[test_case("adaptive", Ok(StoredThinking::Adaptive) ; "adaptive")]
    #[test_case(" adaptive ", Ok(StoredThinking::Adaptive) ; "trims_whitespace")]
    #[test_case("4096", Ok(StoredThinking::Budget { tokens: 4096 }) ; "valid_budget")]
    #[test_case("1", Ok(StoredThinking::Budget { tokens: 1 }) ; "minimum_budget")]
    #[test_case("0", Err(ThinkingParseError::BudgetZero) ; "budget_zero")]
    #[test_case("fast", Err(ThinkingParseError::Unknown("fast".into())) ; "garbage")]
    #[test_case("high", Ok(StoredThinking::Effort { level: Effort::High }) ; "effort_level")]
    fn parse_setting(input: &str, expected: Result<StoredThinking, ThinkingParseError>) {
        assert_eq!(StoredThinking::parse_setting(input), expected);
    }

    // Six ascending values in a six-variant enum also proves ALL is complete.
    #[test]
    fn effort_all_ascending_with_increasing_percent() {
        for pair in Effort::ALL.windows(2) {
            assert!(pair[0] < pair[1], "ALL must be ascending");
            assert!(
                pair[0].percent() < pair[1].percent(),
                "percent must be strictly increasing"
            );
        }
    }

    #[test]
    fn effort_wire_strings_round_trip() {
        let expected = ["minimal", "low", "medium", "high", "xhigh", "max"];
        for (e, s) in Effort::ALL.into_iter().zip(expected) {
            assert_eq!(e.as_str(), s);
            assert_eq!(s.parse::<Effort>(), Ok(e));
        }
    }

    #[test_case(Effort::High, &[Effort::Low, Effort::Medium, Effort::High], Effort::High ; "exact_match")]
    #[test_case(Effort::Max, &[Effort::Low, Effort::Medium, Effort::High], Effort::High ; "downgrade_to_nearest_lower")]
    #[test_case(Effort::Minimal, &[Effort::Low, Effort::Medium], Effort::Low ; "below_lowest_takes_lowest")]
    #[test_case(Effort::Medium, &[], Effort::Medium ; "empty_supported_keeps_self")]
    #[test_case(Effort::Max, &[Effort::High, Effort::XHigh], Effort::XHigh ; "glm_max_snaps_to_xhigh")]
    fn effort_snap(level: Effort, supported: &[Effort], expected: Effort) {
        assert_eq!(level.snap(supported), expected);
    }

    #[test_case(Effort::Minimal, 32_768, 3_276 ; "minimal_ten_percent")]
    #[test_case(Effort::Medium, 32_768, 13_107 ; "medium_forty_percent")]
    #[test_case(Effort::Max, 32_768, 32_768 ; "max_full_budget")]
    #[test_case(Effort::Minimal, 4_096, 1_024 ; "small_max_floors_at_min")]
    #[test_case(Effort::Max, 512, 1_024 ; "tiny_max_raised_to_floor")]
    fn effort_budget(level: Effort, max: u32, expected: u32) {
        assert_eq!(level.budget(max), expected);
    }

    #[test_case(32_768, 32_768, Effort::Max ; "full_budget_is_max")]
    #[test_case(64_000, 32_768, Effort::Max ; "above_max_is_max")]
    #[test_case(0, 32_768, Effort::Minimal ; "zero_is_minimal")]
    #[test_case(13_107, 32_768, Effort::Medium ; "forty_percent_is_medium")]
    #[test_case(1_024, 0, Effort::Max ; "zero_max_saturates")]
    fn effort_from_budget(n: u32, max: u32, expected: Effort) {
        assert_eq!(Effort::from_budget(n, max), expected);
    }

    #[test]
    fn effort_budget_round_trips_at_realistic_max() {
        const MAX: u32 = 32_768;
        for e in Effort::ALL {
            assert_eq!(Effort::from_budget(e.budget(MAX), MAX), e);
        }
    }

    #[test]
    fn session_meta_defaults_absent_fields() {
        let json = r#"{"mode":"build"}"#;
        let meta: super::SessionMeta = serde_json::from_str(json).unwrap();
        assert!(meta.thinking.is_none());
        assert!(!meta.fast);
        assert!(!meta.workflow);
    }

    #[test]
    fn legacy_config_seeds_state_without_metadata_authoring() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.meta.thinking = Some(StoredThinking::Budget { tokens: 8192 });
        session.meta.fast = true;
        session.meta.workflow = true;
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.save_to(dir).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        let view = loaded.latest_state().unwrap();
        let config = view.scopes[&loaded.root_agent_id()]
            .config
            .as_ref()
            .unwrap();
        assert_eq!(
            config.thinking,
            Some(StoredThinking::Budget { tokens: 8192 })
        );
        assert!(config.fast && config.workflow);
        assert!(loaded.meta.thinking.is_none());
        assert!(!loaded.meta.fast && !loaded.meta.workflow);
    }

    #[test]
    fn corrupt_header_line_only_reports_missing_header() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let path = jsonl_path(dir, id);
        fs::write(&path, "NOT_A_HEADER\n").unwrap();

        let err = TestSession::load_from(id, dir).unwrap_err();
        assert!(matches!(err, SessionError::MissingHeader { .. }));
    }

    #[test]
    fn empty_lines_in_jsonl_are_skipped() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("msg"));
        session.save_to(dir).unwrap();

        let path = jsonl_path(dir, session.id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"\n\n\n").unwrap();
        drop(file);
        append_raw_msg(&path, user_message("after"));

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 2);
    }

    #[test_case(b"{\"t\":\"future_type\",\"d\":{}}\n" ; "unknown_record_type")]
    #[test_case(b"{\"t\":\"meta\",\"title\":7}\n" ; "record_with_a_field_of_the_wrong_shape")]
    fn record_another_version_wrote_refuses_the_load_and_stays_on_disk(record: &[u8]) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));
        session.save_to(dir).unwrap();

        let path = jsonl_path(dir, session.id);
        let bad_record_line = fs::read(&path)
            .unwrap()
            .iter()
            .filter(|byte| **byte == b'\n')
            .count()
            + 1;
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(record).unwrap();
        drop(file);
        append_raw_msg(&path, user_message("second"));
        let before = fs::read(&path).unwrap();

        let error = TestSession::load_from(session.id, dir).unwrap_err();
        assert!(
            matches!(error, SessionError::UnreadableRecord { line, .. } if line == bad_record_line),
            "{error}"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test_case(true; "unsupported_feature")]
    #[test_case(false; "unsupported_version")]
    fn incompatible_header_refuses_load_and_scan_without_touching_session(feature: bool) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.push_message(user_message("preserved"));
        session.save_to(tmp.path()).unwrap();
        let path = jsonl_path(tmp.path(), session.id);
        let bytes = fs::read_to_string(&path).unwrap();
        let mut lines = bytes.lines();
        let mut header: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        if feature {
            header["features"] = serde_json::json!([UNSUPPORTED_FEATURE]);
        } else {
            header["v"] = FUTURE_FORMAT_VERSION.into();
        }
        let changed = format!("{header}\n{}\n", lines.collect::<Vec<_>>().join("\n"));
        fs::write(&path, changed.as_bytes()).unwrap();
        let error = TestSession::load_from(session.id, tmp.path()).unwrap_err();
        if feature {
            assert!(
                matches!(error, SessionError::UnsupportedFeatures { features }
                if features == BTreeSet::from([UNSUPPORTED_FEATURE.to_owned()]))
            );
        } else {
            assert!(matches!(
                error,
                SessionError::VersionMismatch {
                    found: FUTURE_FORMAT_VERSION,
                    ..
                }
            ));
        }
        assert!(TestSession::list_all_in(tmp.path()).unwrap().is_empty());
        assert_eq!(fs::read(&path).unwrap(), changed.as_bytes());
    }

    #[test_case(false; "populated")]
    #[test_case(true; "cleared")]
    fn ui_records_restore_independent_fields_and_refresh_session_listing(clear: bool) {
        const CWD: &str = "/project";
        const PROMPT: &str = "original prompt";
        const DRAFT: &str = "unfinished input";
        const QUEUED: &str = "next prompt";
        const TITLE: &str = "renamed session";
        const CONTEXT_SIZE: u32 = 42;
        let tmp = TempDir::new().unwrap();
        let mut session: TestSession = Session::new("model", CWD);
        session.push_message(user_message(PROMPT));
        session.meta.context_size = CONTEXT_SIZE;
        session.save_to(tmp.path()).unwrap();
        let before = TestSession::list_in(CWD, tmp.path()).unwrap();
        let mut updates: Vec<SessionRecord<Value, Value, Value>> = vec![
            SessionRecord::Draft {
                text: Some(DRAFT.into()),
            },
            SessionRecord::Queued {
                messages: vec![QUEUED.into()],
            },
            SessionRecord::Title {
                title: TITLE.into(),
            },
        ];
        if clear {
            updates.extend([
                SessionRecord::Draft { text: None },
                SessionRecord::Queued {
                    messages: Vec::new(),
                },
            ]);
        }
        let path = jsonl_path(tmp.path(), session.id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&encode_records(&updates).unwrap()).unwrap();
        file.sync_all().unwrap();
        let restored = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(
            restored.meta.input_draft.as_deref(),
            (!clear).then_some(DRAFT)
        );
        assert_eq!(
            restored.meta.queued_messages,
            if clear {
                Vec::<String>::new()
            } else {
                vec![QUEUED.to_owned()]
            }
        );
        assert_eq!(restored.title, TITLE);
        assert_eq!(restored.messages(), session.messages());
        assert_eq!(restored.state(), session.state());
        assert_eq!(restored.meta.context_size, session.meta.context_size);
        let after = TestSession::list_in(CWD, tmp.path()).unwrap();
        assert_eq!(after[0].title, TITLE);
        assert_eq!(after[0].message_count, before[0].message_count);
        assert_eq!(after[0].updated_at, before[0].updated_at);
        file.write_all(br#"{"t":"title","title":"torn"}"#).unwrap();
        file.sync_all().unwrap();
        assert_eq!(
            TestSession::load_from(session.id, tmp.path())
                .unwrap()
                .title,
            TITLE
        );
        assert_eq!(
            TestSession::list_in(CWD, tmp.path()).unwrap()[0].title,
            TITLE
        );
    }

    #[test]
    fn usage_records_restore_latest_aggregate_totals() {
        const MODEL: &str = "usage-model";
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new(MODEL, "/project");
        session.save_to(tmp.path()).unwrap();
        let path = jsonl_path(tmp.path(), session.id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        for input in [10, 20] {
            let totals = super::StoredTokenUsage {
                input,
                cost: Some(SONNET_COST),
                ..Default::default()
            };
            let usage = serde_json::json!({"input": input});
            let record = SessionRecord::<Value, Value, Value>::Usage {
                token_usage: usage.clone(),
                usage_by_model: HashMap::from([(MODEL.to_owned(), totals)]),
            };
            file.write_all(&encode_records(&[record]).unwrap()).unwrap();
            file.sync_all().unwrap();
            let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
            assert_eq!(loaded.token_usage, usage);
            assert_eq!(
                loaded.usage_by_model(),
                &HashMap::from([(MODEL.to_owned(), totals)])
            );
        }
    }

    #[test]
    fn scan_returns_latest_title_after_multiple_appends() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));
        session.save_to(dir).unwrap();

        session.title = "v1".into();
        session.push_message(assistant_message("reply"));
        session.save_to(tmp.path()).unwrap();

        session.title = "v2".into();
        session.push_message(user_message("second"));
        session.save_to(tmp.path()).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].title, "v2");
    }

    #[test]
    fn scan_returns_default_title_for_header_only_file() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let session: TestSession = Session::new("m", "/project");
        let path = jsonl_path(dir, session.id);
        let header = serde_json::json!({"t":"header","v":2,"id":session.id,"model":"m","cwd":"/project","created_at":0});
        fs::write(&path, format!("{}\n", header)).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].title, DEFAULT_TITLE);
    }

    #[test]
    fn scan_handles_large_meta_record() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("msg"));
        session.save_to(dir).unwrap();

        session.title = "big-meta".into();
        session.meta.input_draft = Some("x".repeat(TAIL_BUF as usize * 2));
        session.push_message(assistant_message("reply"));
        session.save_to(tmp.path()).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].title, "big-meta");
    }

    fn normalized_epoch_state(session: &TestSession) -> Vec<StoredStateRecord> {
        let mut state = session.state().to_vec();
        let mut canonical = session.canonical_history_epochs();
        let mut epoch = 0;
        super::remap_loaded_history_epochs(&mut state, &mut canonical, || {
            epoch += 1;
            epoch
        });
        state
    }

    #[track_caller]
    fn assert_same_session(loaded: &TestSession, expected: &TestSession) {
        assert_eq!(loaded.messages(), expected.messages(), "messages");
        assert_eq!(loaded.tool_outputs(), expected.tool_outputs(), "outputs");
        assert_eq!(
            loaded.subagent_messages(),
            expected.subagent_messages(),
            "subagent messages",
        );
        assert_eq!(loaded.title, expected.title, "title");
        assert_eq!(
            serde_json::to_value(&loaded.meta).unwrap(),
            serde_json::to_value(&expected.meta).unwrap(),
            "meta"
        );
        assert_eq!(loaded.updated_at, expected.updated_at, "updated_at");
        assert_eq!(loaded.id, expected.id, "id");
        let (model, cwd) = expected.header_cache().unwrap();
        assert_eq!(loaded.cwd, cwd, "cwd");
        assert_eq!(loaded.model, model, "model");
        assert_eq!(loaded.created_at, expected.created_at, "created_at");
        assert_eq!(loaded.token_usage, expected.token_usage, "usage");
        assert_eq!(loaded.subagents(), expected.subagents(), "subagents");
        assert_eq!(loaded.posts(), expected.posts(), "posts");
        assert_eq!(
            loaded.usage_by_model(),
            expected.usage_by_model(),
            "model usage"
        );
        assert_eq!(
            loaded.subagent_conversations,
            expected.subagent_conversations
        );
        let loaded_state = normalized_epoch_state(loaded);
        let expected_state = normalized_epoch_state(expected);
        assert_eq!(loaded_state, expected_state, "state");
        assert_eq!(
            loaded.agent_revisions(),
            expected.agent_revisions(),
            "agent revisions"
        );
        assert_eq!(
            loaded.sequence_high_water(),
            expected.sequence_high_water(),
            "sequence high water"
        );
        assert_eq!(loaded.agents(), expected.agents(), "agents");
        assert_eq!(loaded.parent(), expected.parent(), "parent");
        assert_eq!(loaded.features, expected.features, "features");
        assert_eq!(
            crate::session_state::SessionStateView::fold(&loaded_state, loaded.agent_revisions())
                .unwrap(),
            crate::session_state::SessionStateView::fold(
                &expected_state,
                expected.agent_revisions(),
            )
            .unwrap(),
            "folded state"
        );
    }

    /// `Arc::make_mut` deep-copies the session while the writer holds the last
    /// snapshot, and a checkpoint that changes nothing must not pay for it.
    #[test]
    fn unchanged_checkpoint_does_not_clone_the_session() {
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("hello"));
        let snapshot = HistorySnapshot::new(session.messages().to_vec());
        let mut session = Arc::new(session);
        let meta = session.meta.clone();
        Session::checkpoint(&mut session, Some(&snapshot), meta.clone(), Value::Null);

        let held = Arc::clone(&session);
        Session::checkpoint(&mut session, Some(&snapshot), meta.clone(), Value::Null);
        assert!(Arc::ptr_eq(&held, &session), "no change, no clone");

        Session::checkpoint(
            &mut session,
            Some(&snapshot),
            SessionMeta {
                input_draft: Some("draft".into()),
                ..meta
            },
            Value::Null,
        );
        assert!(!Arc::ptr_eq(&held, &session));
        assert_eq!(session.meta.input_draft.as_deref(), Some("draft"));
        assert!(session.revision() > held.revision());
    }

    /// What the owner types sits in `meta`, so `content_revision` is what tells
    /// a keystroke, which can wait for the ones behind it, from a tool result,
    /// which has to be on disk before the next crash.
    #[test]
    fn a_meta_only_change_leaves_content_revision_alone() {
        let mut session: TestSession = Session::new("m", "/project");
        let (revision, content) = (session.revision(), session.content_revision());

        session.set_meta(SessionMeta {
            input_draft: Some(PENDING_DRAFT.into()),
            ..session.meta.clone()
        });
        assert!(session.revision() > revision, "still needs writing");
        assert_eq!(session.content_revision(), content, "but it can wait");

        session.push_message(user_message("hello"));
        assert!(session.content_revision() > content);
    }

    #[test]
    fn prefs_overlay_round_trips_and_defaults_on_missing_file() {
        let dir = TempDir::new().unwrap();
        let state = StateDir::from_path(dir.path().to_path_buf());

        assert_eq!(read_prefs(&state).default_thinking, None);

        write_prefs(
            &state,
            &Prefs {
                default_thinking: Some(StoredThinking::Effort {
                    level: Effort::Medium,
                }),
            },
        )
        .unwrap();

        assert_eq!(
            read_prefs(&state).default_thinking,
            Some(StoredThinking::Effort {
                level: Effort::Medium
            })
        );

        write_prefs(
            &state,
            &Prefs {
                default_thinking: Some(StoredThinking::Off),
            },
        )
        .unwrap();
        assert_eq!(
            read_prefs(&state).default_thinking,
            Some(StoredThinking::Off)
        );
    }
}
