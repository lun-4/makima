//! Session persistence with append-only JSONL log format.
//!
//! Each session is stored as `{uuid}.jsonl`, one JSON record per line. The format is
//! crash-safe: on load, any trailing run of unparseable lines is discarded (a partial
//! flush may corrupt multiple trailing records). `SessionLog` tracks cursor state to
//! enable O(delta) incremental saves.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::UNIX_EPOCH;

use tempfile::NamedTempFile;
use tracing::{info, warn};

use crate::conversation::ConversationState;
use crate::id::{AgentId, MakimaId};
use crate::offload_cleanup::Root;
use crate::plans::{PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot};
use crate::restore::{RestoreError, RestoreStore, TransitionRef};
use crate::session_lock::SessionPublicationGuard;
use crate::session_state::{
    BOUNDARY_POOL_FORMAT, BOUNDARY_POOLS_FEATURE, ParentLink, SessionStateView, StateChange,
    StateFoldError, StoredAgentConfig, StoredAgentLifecycle, StoredAgentRecord, StoredBranch,
    StoredDeliveryOp, StoredModeSnapshot, StoredStateRecord, StoredTurnResult,
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
const BRANCH_DIR: &str = "branches";
const BLOBS_DIR: &str = "blobs";
const RESTORES_DIR: &str = "restores";
const BRANCH_KEEP: usize = 20;
const BRANCH_MAX_BYTES: u64 = 256 * 1024 * 1024;
type Publication = Arc<Mutex<u64>>;
type PublicationRegistry = Mutex<HashMap<PathBuf, Weak<Mutex<u64>>>>;
static PUBLICATIONS: OnceLock<PublicationRegistry> = OnceLock::new();
const LEGACY_CHILD_UNRESTORABLE: &str = "legacy child has no stored spawn specification";
const LEGACY_PLAN_SLOT: &str = "makima.plan";
pub const SESSIONS_DIR: &str = "sessions";
const CWD_INDEX_FILE: &str = "cwd_latest.json";
const SCAN_CACHE_FILE: &str = "scan_cache.json";
const SCAN_CACHE_VERSION: u32 = 2;
#[cfg(test)]
const TAIL_BUF: u64 = 4096;
const DEFAULT_TITLE: &str = "New session";
const MAX_TITLE_LEN: usize = 60;
const EPOCH_CHANGED: &str = "messages were rewritten";
const HEADER_CHANGED: &str = "root configuration changed header cache";
const FILE_CHANGED_UNDERNEATH: &str = "file changed underneath";
const CURSOR_AHEAD: &str = "cursor ahead of session";
const LOG_BLOATED: &str = "too many stale meta records";
/// Every append leaves a whole meta record behind and only the last one is ever
/// read. Past this many, the log is rewritten and they all go away at once.
const MAX_APPENDS: usize = 512;
/// Where a shrink rewrite parks the log it is about to drop, as `archive/<id>/`.
const ARCHIVE_DIR: &str = "archive";
/// Full tool output saved instead of being cut, as `offload/<id>/`. It goes
/// with the session, so references to it in the history stay valid.
pub const OFFLOAD_DIR: &str = "offload";
/// Archives kept per session. The extra ones go on the next archive, not on a
/// timer.
const ARCHIVE_KEEP: usize = 3;
/// Three copies of a log full of tool output add up fast, so the bytes get a
/// budget of their own. The newest archive always survives, whatever it weighs.
const ARCHIVE_MAX_BYTES: u64 = 32 * 1024 * 1024;
/// A `msg` line starts with this. Matching the prefix beats parsing the log.
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
    #[error(transparent)]
    Restore(#[from] RestoreError),
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
    #[error("session is open in another terminal; close it there first")]
    OpenElsewhere,
    #[error("branch_expired")]
    BranchExpired { branch: MakimaId },
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
struct LegacyPlan {
    #[serde(default, skip_serializing)]
    plan_path: Option<PathBuf>,
    #[serde(default, skip_serializing)]
    plan_written: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyConfig {
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
            messages: Arc::new(messages),
        }
    }
}

impl<M> Default for HistorySnapshot<M> {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

/// The conversation collections are private so every change goes through a
/// mutator that classifies itself: `revision` says "this needs writing",
/// `epoch` says "append cursors into the log are void". The other fields stay
/// public because the meta record is rewritten in full on every append, so
/// they hold no cursor to spoil. `cwd` and `model` are the exception: they
/// live in the header record, which only a rewrite touches, so changes must
/// go through their setters.
///
/// [`SessionMeta`] is the part the owner mirrors from its own live state and
/// hands over whole on every checkpoint. Whatever the session maintains itself
/// gets a field of its own instead, so a checkpoint never copies it out and
/// back in only to compare it against itself.
#[derive(Debug, Clone)]
pub struct Session<M, U, T> {
    pub id: MakimaId,
    pub title: String,
    pub cwd: String,
    pub model: String,
    messages: Arc<Vec<M>>,
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
    branches: Vec<StoredBranch>,
    restore_refs: Vec<TransitionRef>,
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
    /// The append-only run `messages` belongs to, adopted from the producer's
    /// snapshot or minted fresh when this session rewrites them itself. Once
    /// it changes, every append cursor into the log is void.
    epoch: u64,
    /// Bumped when this session rewrites a collection in place (replaced
    /// messages, tool outputs or subagent histories). Kept apart from
    /// `epoch` so `set_history` adopting a producer's snapshot can never
    /// erase a locally minted void: cursor validity is the pair.
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
    /// How many committed messages that chat had when the post arrived.
    pub after_message: usize,
    #[serde(flatten)]
    pub post: Post,
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

#[derive(Serialize, Deserialize)]
#[serde(tag = "t")]
enum LogRecord<M, U, T> {
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
    #[serde(rename = "branch")]
    Branch { d: StoredBranch },
    #[serde(rename = "restore_ref")]
    RestoreRef { d: TransitionRef },
    #[serde(rename = "msg")]
    Msg { d: M },
    #[serde(rename = "out")]
    Out { id: String, d: T },
    #[serde(rename = "sub_msg")]
    SubMsg { sub: String, d: M },
    #[serde(rename = "post")]
    Post { d: StoredPost },
    #[serde(rename = "meta")]
    Meta {
        title: String,
        token_usage: U,
        updated_at: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sequence_high_water: Option<u64>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        history_epochs: BTreeMap<AgentId, u64>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        subagents: Vec<StoredSubagent>,
        #[serde(default, skip_serializing_if = "HashMap::is_empty")]
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

// -- SessionLog: append-only persistence --

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchAvailability {
    pub branch: StoredBranch,
    pub available: bool,
}

pub struct SessionLog {
    session_id: MakimaId,
    publication: Publication,
    saved_generation: u64,
    file: File,
    /// The session's `(epoch, rewrites)` at the last write. Appending is
    /// sound only while both stay the same.
    saved_epoch: u64,
    saved_rewrites: u64,
    /// Length of the file after the last write. Anything else means someone
    /// truncated, deleted or wrote it, and an append would corrupt it.
    saved_len: u64,
    saved_msg_count: usize,
    saved_post_count: usize,
    saved_state_count: usize,
    saved_agent_revision_count: usize,
    saved_branch_count: usize,
    saved_restore_ref_count: usize,
    appends: usize,
    saved_tool_ids: HashSet<String>,
    saved_sub_msg_counts: HashMap<String, usize>,
    /// Serialized trailing meta record; lets `append` persist meta-only
    /// changes (title, draft, updated_at) instead of dropping them.
    saved_meta: Vec<u8>,
    saved_header: (String, String),
    saved_features: BTreeSet<String>,
}

#[derive(Debug)]
pub enum PublicationOutcome {
    NotPublished,
    Published,
    PublishedUnsynced(StorageError),
}

impl PublicationOutcome {
    pub fn is_published(&self) -> bool {
        !matches!(self, Self::NotPublished)
    }
}

pub struct StagedCheckpoint {
    temporary: NamedTempFile,
    publication: Publication,
    branch: Option<StoredBranch>,
    branches: Vec<StoredBranch>,
    path: PathBuf,
    cwd: String,
    session_id: MakimaId,
    checkpoint_required: Arc<AtomicBool>,
}

impl StagedCheckpoint {
    pub fn publish(
        self,
        guard: &SessionPublicationGuard,
        claim_publication: impl FnOnce() -> bool,
    ) -> Result<bool, SessionError> {
        self.publish_outcome(guard, claim_publication)
            .map(|outcome| outcome.is_published())
    }

    pub fn publish_outcome(
        self,
        guard: &SessionPublicationGuard,
        claim_publication: impl FnOnce() -> bool,
    ) -> Result<PublicationOutcome, SessionError> {
        guard
            .publish(|| self.publish_unowned_outcome(claim_publication))
            .map_err(StorageError::from)?
            .unwrap_or(Ok(PublicationOutcome::NotPublished))
    }

    pub fn publish_unowned(
        self,
        claim_publication: impl FnOnce() -> bool,
    ) -> Result<bool, SessionError> {
        self.publish_unowned_outcome(claim_publication)
            .map(|outcome| outcome.is_published())
    }

    pub fn publish_unowned_outcome(
        self,
        claim_publication: impl FnOnce() -> bool,
    ) -> Result<PublicationOutcome, SessionError> {
        self.publish_with_sync(claim_publication, |path| {
            if let Some(parent) = path.parent() {
                SessionLog::sync_publication(parent)?;
            }
            Ok(())
        })
    }

    fn publish_with_sync(
        self,
        claim_publication: impl FnOnce() -> bool,
        sync_directory: impl FnOnce(&Path) -> Result<(), StorageError>,
    ) -> Result<PublicationOutcome, SessionError> {
        let mut generation = self
            .publication
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !claim_publication() {
            return Ok(PublicationOutcome::NotPublished);
        }
        let preserved = if let Some(branch) = &self.branch {
            let dir = self
                .path
                .parent()
                .ok_or_else(|| StorageError::Io(io::Error::other(FILE_CHANGED_UNDERNEATH)))?;
            validate_branch_source(&self.path, self.session_id)?;
            let destination = branch_path(dir, self.session_id, branch.id);
            let parent = destination
                .parent()
                .ok_or_else(|| StorageError::Io(io::Error::other(FILE_CHANGED_UNDERNEATH)))?;
            fs::create_dir_all(parent).map_err(StorageError::from)?;
            sync_branch_directory(parent)?;
            link_archive(&self.path, &destination).map_err(StorageError::from)?;
            if let Err(error) = sync_branch_file(&destination) {
                let _ = fs::remove_file(&destination);
                return Err(error.into());
            }
            Some(destination)
        } else {
            None
        };
        if let Err(error) = self.temporary.persist(&self.path) {
            if let Some(path) = preserved {
                let _ = fs::remove_file(path);
            }
            return Err(StorageError::from(error.error).into());
        }
        self.checkpoint_required.store(true, Ordering::Release);
        *generation = generation.wrapping_add(1);
        if let Err(error) = sync_directory(&self.path) {
            warn!(%error, session_id = %self.session_id, published = true, durability_confirmed = false, "session replacement is live but directory sync failed; retaining all branches");
            return Ok(PublicationOutcome::PublishedUnsynced(error));
        }
        if let Some(branch) = &self.branch {
            prune_branches(
                self.path.parent().unwrap_or(Path::new(".")),
                self.session_id,
                branch.id,
                &self.branches,
            );
        }
        if let Some(dir) = self.path.parent()
            && let Err(error) = update_cwd_index(dir, &self.cwd, self.session_id)
        {
            warn!(error = %error, session_id = %self.session_id, cwd = %self.cwd, "failed to update published session cwd index");
        }
        Ok(PublicationOutcome::Published)
    }
}

fn publication_for(dir: &Path, id: MakimaId) -> Result<Publication, StorageError> {
    let path = jsonl_path(&fs::canonicalize(dir)?, id);
    let mut publications = PUBLICATIONS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    publications.retain(|_, publication| publication.strong_count() > 0);
    if let Some(publication) = publications.get(&path).and_then(Weak::upgrade) {
        return Ok(publication);
    }
    let publication = Arc::new(Mutex::new(0));
    publications.insert(path, Arc::downgrade(&publication));
    Ok(publication)
}

fn branch_path(dir: &Path, session: MakimaId, branch: MakimaId) -> PathBuf {
    dir.join(BRANCH_DIR)
        .join(session.to_string())
        .join(format!("{branch}.jsonl"))
}

fn validate_branch_source(path: &Path, session_id: MakimaId) -> Result<(), SessionError> {
    let file = File::open(path).map_err(StorageError::from)?;
    let mut header = String::new();
    BufReader::new(file)
        .read_line(&mut header)
        .map_err(StorageError::from)?;
    let header: JsonlHeader = serde_json::from_str(&header).map_err(StorageError::from)?;
    if header.id != session_id {
        return Err(SessionError::IdMismatch {
            log_id: session_id,
            given_id: header.id,
        });
    }
    Ok(())
}

fn sync_branch_directory(path: &Path) -> Result<(), StorageError> {
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

fn sync_branch_file(path: &Path) -> Result<(), StorageError> {
    File::open(path)?.sync_all()?;
    if let Some(parent) = path.parent() {
        sync_branch_directory(parent)?;
    }
    Ok(())
}

fn prune_branches(dir: &Path, session: MakimaId, newest: MakimaId, metadata: &[StoredBranch]) {
    let branch_dir = dir.join(BRANCH_DIR).join(session.to_string());
    let Ok(entries) = fs::read_dir(branch_dir) else {
        return;
    };
    let mut branches: Vec<_> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path();
            if !is_jsonl(&path) {
                return None;
            }
            let id: MakimaId = path.file_stem()?.to_str()?.parse().ok()?;
            let metadata = entry.metadata().ok()?;
            Some((id == newest, id, metadata.len(), path))
        })
        .collect();
    let created_at: HashMap<_, _> = metadata
        .iter()
        .map(|branch| (branch.id, branch.created_at))
        .collect();
    branches.sort_unstable_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| created_at.get(&b.1).cmp(&created_at.get(&a.1)))
            .then_with(|| b.1.cmp(&a.1))
    });
    let mut bytes = 0u64;
    for (index, (_, _, size, path)) in branches.into_iter().enumerate() {
        bytes = bytes.saturating_add(size);
        if index > 0
            && (index >= BRANCH_KEEP || bytes > BRANCH_MAX_BYTES)
            && let Err(error) = fs::remove_file(&path)
        {
            warn!(%error, path = %path.display(), "failed to prune session branch");
        }
    }
}

fn sub_msg_snapshot<M>(map: &HashMap<String, Arc<Vec<M>>>) -> HashMap<String, usize> {
    map.iter().map(|(k, v)| (k.clone(), v.len())).collect()
}

/// Compaction and rewind hand the log a shorter message list, and writing that
/// out would take the dropped turns with it, so the old file gets a second name
/// under `archive/<id>/` first. Its own path is untouched until the rename, so
/// [`SessionLog::rewrite`] keeps its crash-safety promise.
fn archive_if_shrinking<M, U, T>(dir: &Path, session: &Session<M, U, T>) {
    let path = jsonl_path(dir, session.id);
    let new_count = session.messages.len();
    if !log_msg_count_exceeds(&path, new_count) {
        return;
    }

    let archive_dir = dir.join(ARCHIVE_DIR).join(session.id.to_string());
    if let Err(e) = fs::create_dir_all(&archive_dir) {
        warn!(error = %e, session_id = %session.id, "cannot create session archive dir");
        return;
    }
    let existing = archives_newest_first(&archive_dir);
    let next = existing.first().map_or(0, |a| a.seq) + 1;
    let archive_path = archive_dir.join(format!("{next}.jsonl"));
    let bytes = match link_archive(&path, &archive_path) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                error = %e,
                session_id = %session.id,
                "cannot archive session log before shrink rewrite"
            );
            return;
        }
    };
    prune_archives(existing, bytes);
    // The live log is about to be renamed away. If the archive's directory
    // entry is not durable by then, a crash frees the only inode holding the
    // dropped turns, which is the loss this whole function exists to prevent.
    crate::sync_parent_dir(&archive_path);
    info!(
        session_id = %session.id,
        new_msgs = new_count,
        archive = %archive_path.display(),
        "archived session log before shrink rewrite"
    );
}

/// Whether the log holds more than `limit` messages, which is the whole
/// question a shrink asks, so the scan stops at the first line past it. A
/// growing log never trips that, so this still walks to EOF once per rewrite:
/// it reads bytes into one reused buffer rather than allocating and
/// UTF-8-validating a `String` per line, which is what made the pass show up
/// next to the write it rides along with. A missing or unreadable file answers
/// no and the rewrite goes ahead as before: nobody should lose a save because
/// the old file would not count.
fn log_msg_count_exceeds(path: &Path, limit: usize) -> bool {
    let Ok(file) = fs::File::open(path) else {
        return false;
    };
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut count = 0;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) | Err(_) => return false,
            Ok(_) => {}
        }
        if line.starts_with(MSG_PREFIX) {
            count += 1;
            if count > limit {
                return true;
            }
        }
    }
}

/// The archive is a second name for the log's current inode. The rewrite
/// renames a fresh file over the path, so the old bytes stay whole under the
/// new name and not one of them is copied. Filesystems with no links (FAT32 on
/// a stick) fall back to a plain copy. The size is for the byte budget.
fn link_archive(from: &Path, to: &Path) -> Result<u64, io::Error> {
    link_archive_with(
        from,
        to,
        |source, destination| fs::hard_link(source, destination),
        io::copy,
    )
}

fn link_archive_with(
    from: &Path,
    to: &Path,
    hard_link: impl FnOnce(&Path, &Path) -> io::Result<()>,
    copy: impl FnOnce(&mut File, &mut File) -> io::Result<u64>,
) -> Result<u64, io::Error> {
    if hard_link(from, to).is_err() {
        let mut destination = OpenOptions::new().write(true).create_new(true).open(to)?;
        let result = File::open(from)
            .and_then(|mut source| copy(&mut source, &mut destination))
            .and_then(|bytes| destination.sync_all().map(|()| bytes));
        result.inspect_err(|_| {
            let _ = fs::remove_file(to);
        })?;
    }
    Ok(fs::metadata(to)?.len())
}

/// `<seq>.jsonl`, counting up. A number cannot step back the way a clock does
/// after an NTP fix or a suspend, so the order is always the truth and pruning
/// can never mistake the newest archive for the oldest. The mtime says when.
struct Archive {
    seq: u64,
    size: u64,
    path: PathBuf,
}

/// Newest first: the next name comes off the front, pruning walks to the back.
fn archives_newest_first(archive_dir: &Path) -> Vec<Archive> {
    let Ok(entries) = fs::read_dir(archive_dir) else {
        return Vec::new();
    };
    let mut archives: Vec<Archive> = entries
        .flatten()
        .filter_map(|entry| {
            let path = entry.path();
            if !is_jsonl(&path) {
                return None;
            }
            Some(Archive {
                seq: path.file_stem()?.to_str()?.parse().ok()?,
                size: entry.metadata().ok()?.len(),
                path,
            })
        })
        .collect();
    archives.sort_unstable_by_key(|a| Reverse(a.seq));
    archives
}

/// Walks from the newest and keeps what both budgets allow, so the rest go.
/// `new_bytes` is the archive we just made: it is not in `existing`, so it can
/// never be the one dropped.
fn prune_archives(existing: Vec<Archive>, new_bytes: u64) {
    let mut total = new_bytes;
    let mut room = ARCHIVE_KEEP.saturating_sub(1);
    for archive in existing {
        total += archive.size;
        if room > 0 && total <= ARCHIVE_MAX_BYTES {
            room -= 1;
            continue;
        }
        let _ = fs::remove_file(&archive.path);
    }
}

impl SessionLog {
    pub fn sync_publication(dir: &Path) -> Result<(), StorageError> {
        sync_branch_directory(dir)
    }

    /// Starts the file over: writes the whole log through a rename, so a crash
    /// mid-write leaves the old one intact, then claims the cwd index and
    /// sweeps pre-jsonl leftovers. A rewrite that drops messages (compaction,
    /// rewind) parks the previous file under `archive/<id>/` first, keeping the
    /// newest [`ARCHIVE_KEEP`] of them within [`ARCHIVE_MAX_BYTES`]. The only
    /// way to get a usable cursor onto a file this process did not write: a
    /// cursor read back from disk describes the session that was loaded, never
    /// the live one.
    pub fn rewrite<M, U, T>(dir: &Path, session: &Session<M, U, T>) -> Result<Self, SessionError>
    where
        M: Serialize,
        U: Serialize,
        T: Serialize,
    {
        fs::create_dir_all(dir).map_err(StorageError::from)?;
        let publication = publication_for(dir, session.id)?;
        let mut generation = publication
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let path = jsonl_path(dir, session.id);
        let tmp = path.with_extension("jsonl.tmp");

        let mut tmp_file = File::create(&tmp).map_err(StorageError::from)?;
        write_full_session(&mut tmp_file, session)?;
        tmp_file.sync_data().map_err(StorageError::from)?;
        // Last thing before the rename: a write that never lands must not
        // spend an archive slot, since taking one prunes the oldest.
        archive_if_shrinking(dir, session);
        fs::rename(&tmp, &path).map_err(StorageError::from)?;
        session.checkpoint_required.store(true, Ordering::Release);
        *generation = generation.wrapping_add(1);
        crate::sync_parent_dir(&path);
        let file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(StorageError::from)?;
        update_cwd_index(dir, &session.header_cache()?.1, session.id)?;
        Ok(Self::cursor_from(
            session,
            file,
            Arc::clone(&publication),
            *generation,
        ))
    }

    /// Stages a complete session without changing its durable file or cwd index.
    pub fn stage_checkpoint<M, U, T>(
        dir: &Path,
        session: &Session<M, U, T>,
    ) -> Result<StagedCheckpoint, SessionError>
    where
        M: Serialize,
        U: Serialize,
        T: Serialize,
    {
        fs::create_dir_all(dir).map_err(StorageError::from)?;
        let path = jsonl_path(dir, session.id);
        let mut temporary = NamedTempFile::new_in(dir).map_err(StorageError::from)?;
        write_full_session(temporary.as_file_mut(), session)?;
        temporary
            .as_file()
            .sync_data()
            .map_err(StorageError::from)?;
        Ok(StagedCheckpoint {
            temporary,
            publication: publication_for(dir, session.id)?,
            branch: None,
            branches: Vec::new(),
            path,
            cwd: session.header_cache()?.1,
            session_id: session.id,
            checkpoint_required: Arc::clone(&session.checkpoint_required),
        })
    }

    pub fn stage_checkpoint_with_branch<M, U, T>(
        dir: &Path,
        session: &Session<M, U, T>,
        branch: StoredBranch,
    ) -> Result<StagedCheckpoint, SessionError>
    where
        M: Serialize,
        U: Serialize,
        T: Serialize,
    {
        let mut staged = Self::stage_checkpoint(dir, session)?;
        if !session
            .branches
            .iter()
            .any(|existing| existing.id == branch.id)
        {
            let mut buf = Vec::new();
            append_record(
                &mut buf,
                &LogRecord::<&M, &U, &T>::Branch { d: branch.clone() },
            )?;
            staged
                .temporary
                .as_file_mut()
                .write_all(&buf)
                .map_err(StorageError::from)?;
            staged
                .temporary
                .as_file()
                .sync_data()
                .map_err(StorageError::from)?;
        }
        staged.branches = session.branches.clone();
        staged.branch = Some(branch);
        Ok(staged)
    }

    pub fn session_id(&self) -> MakimaId {
        self.session_id
    }

    pub fn append<M, U, T>(&mut self, session: &Session<M, U, T>) -> Result<(), SessionError>
    where
        M: Serialize,
        U: Serialize,
        T: Serialize,
    {
        self.require_same_id(session)?;
        let publication = Arc::clone(&self.publication);
        let generation = publication
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if *generation != self.saved_generation {
            return Err(SessionError::LogDiverged {
                reason: FILE_CHANGED_UNDERNEATH,
            });
        }
        session.latest_state()?;
        self.ensure_appendable(session)?;
        if session.header_cache()? != self.saved_header
            || session_features(session) != self.saved_features
        {
            return Err(SessionError::LogDiverged {
                reason: HEADER_CHANGED,
            });
        }

        let mut buf = Vec::new();
        let mut new_msg_count = self.saved_msg_count;
        let mut new_tool_ids = Vec::new();

        for msg in &session.messages[self.saved_msg_count..] {
            append_record(&mut buf, &LogRecord::<&M, &U, &T>::Msg { d: msg })?;
            new_msg_count += 1;
        }

        for (id, output) in &session.tool_outputs {
            if !self.saved_tool_ids.contains(id) {
                append_record(
                    &mut buf,
                    &LogRecord::<&M, &U, &T>::Out {
                        id: id.clone(),
                        d: output,
                    },
                )?;
                new_tool_ids.push(id.clone());
            }
        }

        let mut new_sub_counts: Vec<(String, usize)> = Vec::new();
        for (sub_id, msgs) in &session.subagent_messages {
            let saved = self.saved_sub_msg_counts.get(sub_id).copied().unwrap_or(0);
            for msg in &msgs[saved..] {
                append_record(
                    &mut buf,
                    &LogRecord::<&M, &U, &T>::SubMsg {
                        sub: sub_id.clone(),
                        d: msg,
                    },
                )?;
            }
            if msgs.len() > saved {
                new_sub_counts.push((sub_id.clone(), msgs.len()));
            }
        }

        for post in &session.posts[self.saved_post_count..] {
            append_record(&mut buf, &LogRecord::<&M, &U, &T>::Post { d: post.clone() })?;
        }

        append_journal_records(
            &mut buf,
            &session.state[self.saved_state_count..],
            &session.agent_revisions[self.saved_agent_revision_count..],
        )?;
        for branch in &session.branches[self.saved_branch_count..] {
            append_record(
                &mut buf,
                &LogRecord::<&M, &U, &T>::Branch { d: branch.clone() },
            )?;
        }

        for reference in &session.restore_refs[self.saved_restore_ref_count..] {
            append_record(
                &mut buf,
                &LogRecord::<&M, &U, &T>::RestoreRef {
                    d: reference.clone(),
                },
            )?;
        }
        let meta = meta_record(session)?;
        if buf.is_empty() && meta == self.saved_meta {
            return Ok(());
        }
        buf.extend_from_slice(&meta);

        if let Err(e) = self
            .file
            .write_all(&buf)
            .and_then(|()| self.file.sync_data())
        {
            // A failed write can leave partial bytes; roll back to the last
            // record boundary so the file matches the unadvanced cursors and
            // a retry appends cleanly instead of duplicating records.
            let _ = self.file.set_len(self.saved_len);
            return Err(StorageError::from(e).into());
        }

        self.saved_len += buf.len() as u64;
        self.appends += 1;
        self.saved_msg_count = new_msg_count;
        self.saved_post_count = session.posts.len();
        self.saved_state_count = session.state.len();
        self.saved_agent_revision_count = session.agent_revisions.len();
        self.saved_branch_count = session.branches.len();
        self.saved_restore_ref_count = session.restore_refs.len();
        self.saved_tool_ids.extend(new_tool_ids);
        for (sub_id, count) in new_sub_counts {
            self.saved_sub_msg_counts.insert(sub_id, count);
        }
        self.saved_meta = meta;

        Ok(())
    }

    fn cursor_from<M, U, T>(
        session: &Session<M, U, T>,
        file: File,
        publication: Publication,
        saved_generation: u64,
    ) -> Self
    where
        M: Serialize,
        U: Serialize,
        T: Serialize,
    {
        let saved_len = file.metadata().map(|m| m.len()).unwrap_or_default();
        Self {
            session_id: session.id,
            publication,
            saved_generation,
            file,
            saved_epoch: session.epoch,
            saved_rewrites: session.rewrites,
            saved_len,
            saved_msg_count: session.messages.len(),
            saved_post_count: session.posts.len(),
            saved_state_count: session.state.len(),
            saved_agent_revision_count: session.agent_revisions.len(),
            saved_branch_count: session.branches.len(),
            saved_restore_ref_count: session.restore_refs.len(),
            appends: 0,
            saved_tool_ids: session.tool_outputs.keys().cloned().collect(),
            saved_sub_msg_counts: sub_msg_snapshot(&session.subagent_messages),
            saved_meta: meta_record(session).unwrap_or_default(),
            saved_header: session.header_cache().unwrap_or_default(),
            saved_features: session_features(session),
        }
    }

    fn require_same_id<M, U, T>(&self, session: &Session<M, U, T>) -> Result<(), SessionError> {
        if session.id != self.session_id {
            return Err(SessionError::IdMismatch {
                log_id: self.session_id,
                given_id: session.id,
            });
        }
        Ok(())
    }

    /// Ok only while every cursor still describes the file, which is what makes
    /// `saved_len` the file length and the rest of the cursors its content, and
    /// while appending is still cheaper than starting the file over.
    fn ensure_appendable<M, U, T>(&self, session: &Session<M, U, T>) -> Result<(), SessionError> {
        let reason = if session.epoch != self.saved_epoch || session.rewrites != self.saved_rewrites
        {
            EPOCH_CHANGED
        } else if self.file.metadata().map_err(StorageError::from)?.len() != self.saved_len {
            FILE_CHANGED_UNDERNEATH
        } else if self.appends >= MAX_APPENDS {
            LOG_BLOATED
        } else if self.cursor_ahead(session) {
            // Nothing shrinks a session without minting a new epoch, so this
            // should never fire. It stays because the slices in `append` would
            // panic instead of corrupting if it ever does.
            CURSOR_AHEAD
        } else {
            return Ok(());
        };
        Err(SessionError::LogDiverged { reason })
    }

    fn cursor_ahead<M, U, T>(&self, session: &Session<M, U, T>) -> bool {
        self.saved_msg_count > session.messages.len()
            || self.saved_post_count > session.posts.len()
            || self.saved_state_count > session.state.len()
            || self.saved_agent_revision_count > session.agent_revisions.len()
            || self.saved_branch_count > session.branches.len()
            || self.saved_restore_ref_count > session.restore_refs.len()
            || self
                .saved_tool_ids
                .iter()
                .any(|id| !session.tool_outputs.contains_key(id))
            || self.saved_sub_msg_counts.iter().any(|(sub, &count)| {
                session
                    .subagent_messages
                    .get(sub)
                    .is_none_or(|msgs| count > msgs.len())
            })
    }
}

fn meta_record<M, U, T>(session: &Session<M, U, T>) -> Result<Vec<u8>, SessionError>
where
    M: Serialize,
    U: Serialize,
    T: Serialize,
{
    let history_epochs = session.canonical_history_epochs();
    let mut buf = Vec::new();
    append_record(
        &mut buf,
        &LogRecord::<&M, &U, &T>::Meta {
            title: session.title.clone(),
            token_usage: &session.token_usage,
            updated_at: session.updated_at,
            sequence_high_water: session.sequence_high_water(),
            history_epochs,
            subagents: session.subagents.clone(),
            subagent_conversations: session.subagent_conversations.clone(),
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

fn session_features<M, U, T>(session: &Session<M, U, T>) -> BTreeSet<String> {
    let mut features = session.features.clone();
    features.remove(BOUNDARY_POOLS_FEATURE);
    if session.state.iter().any(|record| matches!(&record.change,
        StateChange::BoundarySnapshot { snapshot, .. }
            if snapshot.get("format").and_then(serde_json::Value::as_str) == Some(BOUNDARY_POOL_FORMAT)))
    {
        features.insert(BOUNDARY_POOLS_FEATURE.into());
    }
    features
}

fn write_full_session<M, U, T>(
    file: &mut File,
    session: &Session<M, U, T>,
) -> Result<(), SessionError>
where
    M: Serialize,
    U: Serialize,
    T: Serialize,
{
    let (model, cwd) = session.header_cache()?;
    let mut buf = Vec::new();
    append_record(
        &mut buf,
        &LogRecord::<&M, &U, &T>::Header {
            v: LOG_FORMAT_VERSION,
            id: session.id,
            model,
            cwd,
            created_at: session.created_at,
            parent: session.parent.clone(),
            features: session_features(session),
        },
    )?;
    for msg in session.messages.iter() {
        append_record(&mut buf, &LogRecord::<&M, &U, &T>::Msg { d: msg })?;
    }
    for (id, output) in &session.tool_outputs {
        append_record(
            &mut buf,
            &LogRecord::<&M, &U, &T>::Out {
                id: id.clone(),
                d: output,
            },
        )?;
    }
    for (sub_id, msgs) in &session.subagent_messages {
        for msg in msgs.iter() {
            append_record(
                &mut buf,
                &LogRecord::<&M, &U, &T>::SubMsg {
                    sub: sub_id.clone(),
                    d: msg,
                },
            )?;
        }
    }
    for post in &session.posts {
        append_record(&mut buf, &LogRecord::<&M, &U, &T>::Post { d: post.clone() })?;
    }
    append_journal_records(&mut buf, &session.state, &session.agent_revisions)?;
    for branch in &session.branches {
        append_record(
            &mut buf,
            &LogRecord::<&M, &U, &T>::Branch { d: branch.clone() },
        )?;
    }
    for reference in &session.restore_refs {
        append_record(
            &mut buf,
            &LogRecord::<&M, &U, &T>::RestoreRef {
                d: reference.clone(),
            },
        )?;
    }
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
                LogRecord::<(), (), ()>::State { d: record.clone() },
            )
        })
        .chain(agents.iter().map(|(seq, record)| {
            (
                *seq,
                LogRecord::<(), (), ()>::Agent {
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

fn append_record<R: Serialize>(buf: &mut Vec<u8>, record: &R) -> Result<(), SessionError> {
    serde_json::to_writer(&mut *buf, record).map_err(StorageError::from)?;
    buf.push(b'\n');
    Ok(())
}

/// Tag-only probe used to classify a line that failed the strict `LogRecord`
/// parse: distinguishes a header with a bad id from a genuinely unknown record.
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
    let mut branches = Vec::new();
    let mut restore_refs = Vec::new();
    let mut sequence_high_water = None;
    let mut history_epochs = BTreeMap::new();
    for line in data.split(|&b| b == b'\n') {
        line_count += 1;
        if line.is_empty() {
            continue;
        }
        // A crash can tear an append, so a line that is not JSON at all is
        // skipped, and so is a bad header, which leaves the log headerless.
        // Whole JSON after the header that does not decode came from another
        // version; skipping it would let the next save drop it from disk.
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
            if header
                .features
                .iter()
                .any(|feature| feature != BOUNDARY_POOLS_FEATURE)
            {
                return Err(SessionError::UnsupportedFeatures {
                    features: header.features,
                });
            }
        }
        let record: LogRecord<M, U, T> = match serde_json::from_slice(line) {
            Ok(r) => r,
            Err(e) if e.is_data() && id.is_some() => {
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
        match record {
            LogRecord::Header {
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
                if h_features
                    .iter()
                    .any(|feature| feature != BOUNDARY_POOLS_FEATURE)
                {
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
            LogRecord::State { d } => state.push(d),
            LogRecord::Agent { seq, d } => agent_revisions.push((seq, d)),
            LogRecord::Branch { d } => branches.push(d),
            LogRecord::RestoreRef { d } => {
                if !restore_refs.contains(&d) {
                    restore_refs.push(d);
                }
            }
            LogRecord::Msg { d } => messages.push(d),
            LogRecord::Out { id: out_id, d } => {
                tool_outputs.insert(out_id, Arc::new(d));
            }
            LogRecord::SubMsg { sub, d } => {
                subagent_messages.entry(sub).or_default().push(d);
            }
            LogRecord::Post { d } => posts.push(d),
            LogRecord::Meta {
                title: m_title,
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

    if state.iter().any(|record| matches!(&record.change,
        StateChange::BoundarySnapshot { snapshot, .. }
            if snapshot.get("format").and_then(serde_json::Value::as_str) == Some(BOUNDARY_POOL_FORMAT)))
        && !features.contains(BOUNDARY_POOLS_FEATURE)
    {
        return Err(SessionError::UnsupportedFeatures {
            features: BTreeSet::from([BOUNDARY_POOLS_FEATURE.into()]),
        });
    }
    remap_loaded_history_epochs(&mut state, &mut history_epochs, next_epoch);
    mark_duplicate_subagent_identities(&mut subagents);
    let mut session = Session {
        id,
        title,
        cwd,
        model,
        messages: Arc::new(messages),
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
        branches,
        restore_refs,
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
    session.migrate_legacy_plan(legacy_plan)?;
    if session.root_config()?.is_none() {
        if let Some(legacy) = &session.legacy_config {
            legacy.restore(&mut session.meta);
        }
    } else {
        session.legacy_config = None;
    }
    session.restore_loaded_history_epochs(&history_epochs);
    (session.model, session.cwd) = session.header_cache()?;
    Ok(session)
}

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

fn visit_history_epochs(
    state: &mut [StoredStateRecord],
    canonical: &mut BTreeMap<AgentId, u64>,
    visit: &mut impl FnMut(u64) -> u64,
) {
    for epoch in canonical.values_mut() {
        *epoch = visit(*epoch);
    }
    for record in state {
        match &mut record.change {
            StateChange::Boundary { lens, .. } => {
                for (epoch, _) in lens.values_mut() {
                    *epoch = visit(*epoch);
                }
            }
            StateChange::Compaction { epoch } => *epoch = visit(*epoch),
            StateChange::BoundarySnapshot { snapshot, .. } => visit_pin_epochs(snapshot, visit),
            _ => {}
        }
    }
}

fn visit_pin_epochs(snapshot: &mut serde_json::Value, visit: &mut impl FnMut(u64) -> u64) {
    let mut pending = vec![snapshot];
    while let Some(snapshot) = pending.pop() {
        if let Some(histories) = snapshot
            .get_mut("histories")
            .and_then(serde_json::Value::as_object_mut)
        {
            for history in histories.values_mut() {
                if let Some(epoch) = history.get_mut("epoch") {
                    visit_epoch_value(epoch, visit);
                }
            }
        }
        if let Some(state) = snapshot
            .get_mut("state")
            .and_then(serde_json::Value::as_array_mut)
        {
            for record in state {
                let Some(change) = record.get_mut("change") else {
                    continue;
                };
                match change.get("kind").and_then(serde_json::Value::as_str) {
                    Some("boundary") => {
                        if let Some(lens) = change
                            .get_mut("lens")
                            .and_then(serde_json::Value::as_object_mut)
                        {
                            for entry in lens.values_mut() {
                                if let Some(epoch) = entry.get_mut(0) {
                                    visit_epoch_value(epoch, visit);
                                }
                            }
                        }
                    }
                    Some("compaction") => {
                        if let Some(epoch) = change.get_mut("epoch") {
                            visit_epoch_value(epoch, visit);
                        }
                    }
                    Some("boundary_snapshot") => {
                        if let Some(snapshot) = change.get_mut("snapshot") {
                            pending.push(snapshot);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

fn visit_epoch_value(value: &mut serde_json::Value, visit: &mut impl FnMut(u64) -> u64) {
    if let Some(epoch) = value.as_u64() {
        *value = visit(epoch).into();
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

/// A `meta` line starts with this. Only meta lines get parsed during a scan;
/// everything else is classified by prefix.
const META_PREFIX: &[u8] = br#"{"t":"meta""#;

#[derive(Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
enum ScanRecord {
    Meta {
        title: String,
        updated_at: u64,
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
        || header
            .features
            .iter()
            .any(|feature| feature != BOUNDARY_POOLS_FEATURE)
    {
        return None;
    }

    let mut title = DEFAULT_TITLE.to_string();
    let mut updated_at = 0;
    let mut message_count = 0;
    // Messages are counted by prefix, the same trick the shrink check uses:
    // parsing every payload just to count it made the picker scan allocate
    // through megabytes of tool output. Only meta lines get parsed.
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
        // Every record the writer emits is one line of complete JSON followed
        // by a newline, so a newline-terminated prefix match is always a
        // complete msg record. Only the final line can be torn by a crash and
        // still match the prefix, so just that line needs a parse to stay in
        // step with the loader, which skips malformed records.
        if line.starts_with(MSG_PREFIX) && (line.ends_with(b"\n") || is_complete_msg_record(&line))
        {
            message_count += 1;
        } else if line.starts_with(META_PREFIX)
            && let Ok(ScanRecord::Meta {
                title: next_title,
                updated_at: next_updated_at,
            }) = serde_json::from_slice(line.trim_ascii_end())
        {
            title = next_title;
            updated_at = next_updated_at;
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

/// A torn tail can match the msg prefix without being a record the loader
/// would keep, so an unterminated final line has to parse as a well-formed
/// `t: "msg"` record with a `d` field before it counts.
fn is_complete_msg_record(line: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) else {
        return false;
    };
    matches!(value.get("t").and_then(|t| t.as_str()), Some("msg")) && value.get("d").is_some()
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

    pub fn branches(&self) -> &[StoredBranch] {
        &self.branches
    }

    pub fn parent(&self) -> Option<&ParentLink> {
        self.parent.as_ref()
    }

    pub fn features(&self) -> &BTreeSet<String> {
        &self.features
    }

    pub fn state_at(&self, seq: u64) -> Result<SessionStateView, SessionError> {
        SessionStateView::fold(&self.state, &self.agent_revisions, seq)
            .map_err(SessionError::InvalidState)
    }

    pub fn latest_state(&self) -> Result<SessionStateView, SessionError> {
        self.state_at(u64::MAX)
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
            branches: Vec::new(),
            restore_refs: Vec::new(),
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

    pub fn bind_root_id(&mut self, id: AgentId) -> Result<(), SessionError> {
        let Some(previous) = self.root_agent_id() else {
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
                StateChange::Boundary { lens, .. } => {
                    if let Some(value) = lens.remove(&previous) {
                        lens.insert(id, value);
                    }
                }
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
        for branch in &mut self.branches {
            if branch.agent == previous {
                branch.agent = id;
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
        let mut branches = self.branches.clone();
        for branch in &replacement.branches {
            if !branches.iter().any(|existing| existing.id == branch.id) {
                branches.push(branch.clone());
            }
        }
        replacement.branches = branches;
        let mut restore_refs = self.restore_refs.clone();
        for reference in &replacement.restore_refs {
            if !restore_refs.contains(reference) {
                restore_refs.push(reference.clone());
            }
        }
        replacement.restore_refs = restore_refs;
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

    pub fn list_branches_in(&self, dir: &Path) -> Result<Vec<BranchAvailability>, SessionError> {
        Ok(self
            .branches
            .iter()
            .map(|branch| BranchAvailability {
                branch: branch.clone(),
                available: self.load_branch_from(branch.id, dir).is_ok(),
            })
            .collect())
    }

    pub fn load_branch_from(&self, branch: MakimaId, dir: &Path) -> Result<Self, SessionError> {
        let expired = || SessionError::BranchExpired { branch };
        if !self.branches.iter().any(|stored| stored.id == branch) {
            return Err(expired());
        }
        let path = branch_path(dir, self.id, branch);
        let data = fs::read(&path).map_err(|_| expired())?;
        if !data.ends_with(b"\n")
            || data
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .any(|line| serde_json::from_slice::<serde_json::Value>(line).is_err())
        {
            return Err(expired());
        }
        let restored: Self =
            load_jsonl(&data, &path.display().to_string()).map_err(|_| expired())?;
        if restored.id != self.id {
            return Err(expired());
        }
        restored.latest_state().map_err(|_| expired())?;
        Ok(restored)
    }

    pub fn push_state(&mut self, record: StoredStateRecord) {
        let reorders = self
            .state
            .iter()
            .map(|record| record.seq)
            .chain(self.agent_revisions.iter().map(|(seq, _)| *seq))
            .any(|seq| seq >= record.seq);
        self.state.push(record);
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
            self.rewrite();
        }
    }

    pub fn push_agent_revision(&mut self, seq: u64, record: StoredAgentRecord) {
        let reorders = self.state.iter().any(|record| record.seq >= seq)
            || self.agent_revisions.iter().any(|(saved, _)| *saved >= seq);
        self.agent_revisions.push((seq, record));
        self.agents = latest_agents(&self.agent_revisions);
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
            self.agent_revisions = revisions;
            self.rewrite();
        }
    }

    pub fn restore_refs(&self) -> &[TransitionRef] {
        &self.restore_refs
    }

    pub fn push_restore_ref(&mut self, reference: TransitionRef) {
        if !self.restore_refs.contains(&reference) {
            self.restore_refs.push(reference);
            self.touch();
        }
    }

    pub fn push_branch(&mut self, branch: StoredBranch) {
        self.branches.push(branch);
        self.touch();
    }

    pub fn replace_branches(&mut self, branches: Vec<StoredBranch>) {
        if self.branches != branches {
            self.branches = branches;
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

    /// Every append cursor into the log is void from here on. Counted in
    /// `rewrites`, which snapshot adoption never touches, so a same-frame
    /// `set_history` cannot erase the void before the writer sees it.
    fn rewrite(&mut self) {
        self.rewrites += 1;
        self.touch();
    }

    /// [`Self::rewrite`] for local changes to `messages`: they also leave the
    /// producer's run, so the epoch is minted fresh. Once this state is
    /// saved, re-adopting a stale run snapshot keeps diverging instead of
    /// splicing its tail onto a rewound log.
    fn rewrite_messages(&mut self) {
        self.epoch = next_epoch();
        self.rewrite();
    }

    pub fn push_message(&mut self, msg: M) {
        Arc::make_mut(&mut self.messages).push(msg);
        self.touch();
    }

    pub fn replace_messages(&mut self, messages: Vec<M>) {
        self.replace_shared_messages(Arc::new(messages));
    }

    pub fn replace_shared_messages(&mut self, messages: Arc<Vec<M>>) {
        self.messages = messages;
        self.rewrite_messages();
    }

    pub fn truncate_messages(&mut self, len: usize) {
        if len >= self.messages.len() {
            return;
        }
        Arc::make_mut(&mut self.messages).truncate(len);
        self.rewrite_messages();
    }

    /// Adopting a producer's snapshot inherits its run token, so the log's
    /// cursors survive exactly when the snapshot was an append.
    pub fn adopt_history(&mut self, snapshot: &HistorySnapshot<M>) {
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
        self.subagent_messages
            .get(id)
            .map(|messages| HistorySnapshot {
                epoch: self
                    .subagent_history_epochs
                    .get(id)
                    .copied()
                    .unwrap_or_default(),
                compacted: false,
                conversation: self
                    .subagent_conversations
                    .get(id)
                    .cloned()
                    .map(ConversationState::new),
                messages: Arc::clone(messages),
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
        self.messages = Arc::default();
        self.meta.compacted = false;
        self.meta.conversation = None;
        self.subagent_messages.clear();
        self.subagent_conversations.clear();
        for (agent, history) in histories {
            let Some(record) = self.agents.get(agent) else {
                continue;
            };
            if record.transcript_key.is_empty() {
                self.epoch = history.epoch;
                self.messages = Arc::clone(&history.messages);
                self.meta.compacted = history.compacted;
                self.meta.conversation = history.conversation.clone();
            } else {
                let key = record.transcript_key.clone();
                self.subagent_history_epochs
                    .insert(key.clone(), history.epoch);
                self.subagent_messages
                    .insert(key.clone(), Arc::clone(&history.messages));
                if let Some(conversation) = &history.conversation {
                    self.subagent_conversations
                        .insert(key, Arc::clone(&conversation.id));
                }
            }
        }
        let keys: HashSet<_> = self
            .agents
            .values()
            .map(|agent| agent.transcript_key.as_str())
            .collect();
        self.subagents
            .retain(|child| keys.contains(child.tool_use_id.as_str()));
        self.posts.retain(|post| match &post.chat {
            None => post.after_message <= self.messages.len(),
            Some(key) => self
                .subagent_messages
                .get(key)
                .is_some_and(|messages| post.after_message <= messages.len()),
        });
        self.rewrite();
    }

    pub fn set_subagent_messages(&mut self, id: String, msgs: Vec<M>) {
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
        if self.subagents == subagents {
            return;
        }
        self.subagents = subagents;
        self.touch();
    }

    pub fn posts(&self) -> &[StoredPost] {
        &self.posts
    }

    pub fn push_post(&mut self, post: StoredPost) {
        self.posts.push(post);
        self.touch();
    }

    pub fn prune_posts(&mut self, chat: Option<&str>, new_len: usize) {
        let previous_len = self.posts.len();
        self.posts
            .retain(|stored| stored.chat.as_deref() != chat || stored.after_message <= new_len);
        if self.posts.len() != previous_len {
            self.rewrite();
        }
    }

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

    /// Header field: appends never rewrite the header, so the change voids
    /// the cursors to force a full rewrite.
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

    pub fn save(&mut self, dir: &StateDir) -> Result<(), SessionError> {
        let sessions_dir = dir.ensure_subdir(SESSIONS_DIR)?;
        self.save_to(&sessions_dir)
    }

    pub fn save_to(&mut self, dir: &Path) -> Result<(), SessionError> {
        self.updated_at = now_epoch();
        SessionLog::rewrite(dir, self)?;
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
        let _restore_guard = RestoreStore::guard_session_deletion_in(dir, id).map_err(|error| {
            if matches!(&error, RestoreError::Io(error) if error.kind() == ErrorKind::NotFound) {
                SessionError::Storage(StorageError::NotFound(id.to_string()))
            } else {
                SessionError::Restore(error)
            }
        })?;
        let removed = try_remove(&jsonl_path(dir, id))?;
        // Backups, not the session: failing to sweep them must not fail a
        // delete whose log is already gone, and their presence alone does not
        // make a session exist.
        let session_id = id.to_string();
        let paths = [
            (Path::new(ARCHIVE_DIR).join(&session_id), "archives"),
            (Path::new(BRANCH_DIR).join(&session_id), "branches"),
            (Path::new(BLOBS_DIR).join(&session_id), "file blobs"),
            (
                Path::new(RESTORES_DIR).join(&session_id),
                "file restore transitions",
            ),
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
        ARCHIVE_DIR, ARCHIVE_KEEP, ARCHIVE_MAX_BYTES, CWD_INDEX_FILE, DEFAULT_TITLE, LOG_BLOATED,
        MAX_APPENDS, MAX_TITLE_LEN, MSG_PREFIX, SESSIONS_DIR, StoredSubagent, TAIL_BUF,
        generate_title, jsonl_path, load_cwd_index, next_epoch, offload_dir, root_record,
        update_cwd_index, write_full_session,
    };
    use super::{
        BRANCH_DIR, BRANCH_KEEP, BRANCH_MAX_BYTES, branch_path, link_archive_with, prune_branches,
    };
    use super::{
        HistorySnapshot, PublicationOutcome, SCAN_CACHE_FILE, Session, SessionError, SessionLog,
        SessionMeta, StorageError, TitleSource,
    };
    use super::{LEGACY_CHILD_UNRESTORABLE, LEGACY_PLAN_SLOT, StoredMode};
    use super::{Post, PostLevel, StoredPost};
    use super::{Prefs, read_prefs, write_prefs};
    use crate::StateDir;
    use crate::conversation::{ConversationLink, ConversationRelation, ConversationState};
    use crate::id::{AgentId, MakimaId};
    use crate::plans::{PLAN_STATE_KEY, PlanSlot};
    use crate::restore::TransitionRef;
    use crate::session_lock;
    use crate::session_state::{
        ParentLink, StateChange, StoredAgentConfig, StoredAgentLifecycle, StoredBranch,
        StoredModeSnapshot, StoredStateRecord,
    };
    use serde_json::Value;
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::fs::{self, OpenOptions};
    use std::io::{self, Write};
    #[cfg(unix)]
    use std::os::unix::fs::symlink as symlink_dir;
    #[cfg(windows)]
    use std::os::windows::fs::symlink_dir;
    use std::path::{Path, PathBuf};
    use std::slice::from_ref;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::thread;
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
    /// Two of these already break the byte budget.
    const FAKE_ARCHIVE_BYTES: u64 = ARCHIVE_MAX_BYTES / 2;
    const EXISTING_ARCHIVE_SEQ: u64 = 7;
    /// A pid no live process on this machine has.
    const FAKE_LOCK_PID: u32 = u32::MAX - 1;

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

    fn branch(session: &TestSession) -> StoredBranch {
        StoredBranch {
            id: MakimaId::generate(),
            created_at: session.updated_at,
            at_seq: session.next_state_seq().saturating_sub(1),
            agent: session.root_agent_id().unwrap(),
            offset: session.messages().len(),
            transition: None,
        }
    }

    fn branch_fixture() -> (TempDir, TestSession) {
        let dir = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.push_message(user_message(POST_TEXT));
        session.save_to(dir.path()).unwrap();
        (dir, session)
    }

    #[test_case(false; "saved_origin")]
    #[test_case(true; "staged_publication_origin")]
    fn checkpoint_existence_is_shared_across_clones_and_authoritative_replacement(staged: bool) {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let earlier_clone = session.clone();
        assert!(!session.checkpoint_required());
        if staged {
            let pending = SessionLog::stage_checkpoint(directory.path(), &session).unwrap();
            assert!(!session.checkpoint_required());
            pending.publish_unowned(|| true).unwrap();
        } else {
            session.save_to(directory.path()).unwrap();
        }
        assert!(session.checkpoint_required());
        assert!(earlier_clone.checkpoint_required());
        let mut replacement = TestSession::new("model", "/project");
        replacement.id = session.id;
        session.replace_authoritative(replacement).unwrap();
        assert!(session.checkpoint_required());
        assert!(session.clone().checkpoint_required());
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert!(loaded.checkpoint_required());
        let mut fresh = TestSession::new("model", "/project");
        fresh.id = session.id;
        let fresh_clone = fresh.clone();
        fresh.replace_authoritative(loaded).unwrap();
        assert!(fresh.checkpoint_required());
        assert!(fresh_clone.checkpoint_required());
    }

    #[test_case(false, false; "cancelled")]
    #[test_case(true, false; "failed_rename")]
    #[test_case(true, true; "published_unsynced")]
    fn checkpoint_origin_changes_only_after_successful_rename(publish: bool, sync_failure: bool) {
        let directory = TempDir::new().unwrap();
        let session = TestSession::new("model", "/project");
        let earlier_clone = session.clone();
        let staged = SessionLog::stage_checkpoint(directory.path(), &session).unwrap();
        if publish && !sync_failure {
            fs::remove_file(staged.temporary.path()).unwrap();
        }
        let outcome = staged.publish_with_sync(
            || publish,
            |_| Err(StorageError::Io(io::Error::other(POST_TEXT))),
        );
        if sync_failure {
            assert!(matches!(
                outcome.unwrap(),
                PublicationOutcome::PublishedUnsynced(_)
            ));
        } else if publish {
            assert!(outcome.is_err());
        } else {
            assert!(matches!(outcome.unwrap(), PublicationOutcome::NotPublished));
        }
        assert_eq!(session.checkpoint_required(), sync_failure);
        assert_eq!(earlier_clone.checkpoint_required(), sync_failure);
        assert_eq!(
            jsonl_path(directory.path(), session.id).exists(),
            sync_failure
        );
    }

    #[test]
    fn restore_references_round_trip_append_deduplicate_and_survive_replacement() {
        let (dir, mut session) = branch_fixture();
        let original = session.clone();
        let first = TransitionRef {
            session: session.id,
            transition: MakimaId::generate(),
        };
        let second = TransitionRef {
            session: session.id,
            transition: MakimaId::generate(),
        };
        session.push_restore_ref(first.clone());
        let mut log = SessionLog::rewrite(dir.path(), &session).unwrap();
        let loaded = TestSession::load_from(session.id, dir.path()).unwrap();
        assert!(session.checkpoint_required());
        assert!(loaded.checkpoint_required());
        assert!(loaded.clone().checkpoint_required());
        assert_eq!(loaded.restore_refs(), from_ref(&first));
        let revision = session.revision();
        session.push_restore_ref(first.clone());
        assert_eq!(session.revision(), revision);
        session.push_restore_ref(second.clone());
        log.append(&session).unwrap();
        let size = fs::metadata(jsonl_path(dir.path(), session.id))
            .unwrap()
            .len();
        log.append(&session).unwrap();
        assert_eq!(
            fs::metadata(jsonl_path(dir.path(), session.id))
                .unwrap()
                .len(),
            size
        );
        let loaded = TestSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(loaded.restore_refs(), &[first.clone(), second.clone()]);
        assert_eq!(loaded.messages(), original.messages());
        assert_eq!(loaded.state(), original.state());

        let mut replacement = original;
        replacement.push_restore_ref(second.clone());
        replacement.replace_messages(Vec::new());
        session.replace_authoritative(replacement).unwrap();
        session.save_to(dir.path()).unwrap();
        let loaded = TestSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(loaded.restore_refs(), &[first, second]);
        assert!(loaded.messages().is_empty());
    }

    #[test_case(false, false ; "cancelled")]
    #[test_case(true, false ; "published")]
    #[test_case(true, true ; "published_unsynced")]
    fn restore_reference_is_committed_only_by_checkpoint_publication(
        publish: bool,
        sync_failure: bool,
    ) {
        let (dir, session) = branch_fixture();
        let reference = TransitionRef {
            session: session.id,
            transition: MakimaId::generate(),
        };
        let mut replacement = session.clone();
        replacement.push_restore_ref(reference.clone());
        let staged = SessionLog::stage_checkpoint(dir.path(), &replacement).unwrap();
        assert!(
            TestSession::load_from(session.id, dir.path())
                .unwrap()
                .restore_refs()
                .is_empty()
        );
        let outcome = staged
            .publish_with_sync(
                || publish,
                |_| {
                    if sync_failure {
                        Err(StorageError::Io(io::Error::other(POST_TEXT)))
                    } else {
                        Ok(())
                    }
                },
            )
            .unwrap();
        assert_eq!(outcome.is_published(), publish);
        assert_eq!(
            matches!(outcome, PublicationOutcome::PublishedUnsynced(_)),
            sync_failure
        );
        let loaded = TestSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(
            loaded.restore_refs(),
            if publish { vec![reference] } else { Vec::new() }
        );
        assert_eq!(loaded.messages(), session.messages());
        assert_eq!(loaded.state(), session.state());
    }

    #[test]
    fn branch_transition_reference_round_trips_without_publishing_it() {
        let (dir, mut session) = branch_fixture();
        let reference = TransitionRef {
            session: session.id,
            transition: MakimaId::generate(),
        };
        let mut saved = branch(&session);
        saved.transition = Some(reference);
        session.push_branch(saved.clone());
        SessionLog::stage_checkpoint_with_branch(dir.path(), &session, saved.clone())
            .unwrap()
            .publish_unowned(|| true)
            .unwrap();
        let loaded = TestSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(loaded.branches(), &[saved]);
        assert!(loaded.restore_refs().is_empty());
    }

    #[test]
    fn branch_publication_excludes_concurrent_and_stale_append_cursors() {
        let (dir, session) = branch_fixture();
        let cursor = SessionLog::rewrite(dir.path(), &session).unwrap();
        let before = fs::read(jsonl_path(dir.path(), session.id)).unwrap();
        let saved = branch(&session);
        let mut replacement = session.clone();
        replacement.replace_messages(Vec::new());
        replacement.push_branch(saved.clone());
        let staged =
            SessionLog::stage_checkpoint_with_branch(dir.path(), &replacement, saved.clone())
                .unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let publication = Arc::clone(&staged.publication);
        let mut worker = None;
        assert!(
            staged
                .publish_unowned(|| {
                    worker = Some(thread::spawn(move || {
                        let mut cursor = cursor;
                        let mut pending = session;
                        pending.push_message(user_message(PENDING_DRAFT));
                        assert!(publication.try_lock().is_err());
                        started_tx.send(()).unwrap();
                        cursor.append(&pending)
                    }));
                    started_rx.recv().unwrap();
                    true
                })
                .unwrap()
        );
        assert!(matches!(
            worker.unwrap().join().unwrap(),
            Err(SessionError::LogDiverged { .. })
        ));
        let archive = branch_path(dir.path(), replacement.id, saved.id);
        assert_eq!(fs::read(&archive).unwrap(), before);
        let mut live_cursor = SessionLog::rewrite(dir.path(), &replacement).unwrap();
        replacement.push_message(user_message(PENDING_DRAFT));
        live_cursor.append(&replacement).unwrap();
        assert_eq!(fs::read(archive).unwrap(), before);
        let restored = replacement.load_branch_from(saved.id, dir.path()).unwrap();
        assert_eq!(restored.messages(), &[user_message(POST_TEXT)]);
        assert_eq!(
            replacement.list_branches_in(dir.path()).unwrap()[0].branch,
            saved
        );
        assert!(replacement.list_branches_in(dir.path()).unwrap()[0].available);
    }

    #[test]
    fn restoring_a_branch_preserves_the_displaced_state_as_another_branch() {
        let (dir, mut session) = branch_fixture();
        let original = branch(&session);
        let mut rewound = session.clone();
        rewound.replace_messages(Vec::new());
        rewound.push_branch(original.clone());
        SessionLog::stage_checkpoint_with_branch(dir.path(), &rewound, original.clone())
            .unwrap()
            .publish_unowned(|| true)
            .unwrap();
        let restored = rewound.load_branch_from(original.id, dir.path()).unwrap();
        let displaced = branch(&rewound);
        rewound.replace_authoritative(restored).unwrap();
        rewound.push_branch(displaced.clone());
        let lease = session_lock::claim(dir.path(), &rewound.id)
            .unwrap()
            .unwrap();
        SessionLog::stage_checkpoint_with_branch(dir.path(), &rewound, displaced.clone())
            .unwrap()
            .publish(&lease.publication_guard(), || true)
            .unwrap();
        session = TestSession::load_from(rewound.id, dir.path()).unwrap();
        assert_eq!(session.messages(), &[user_message(POST_TEXT)]);
        assert_eq!(session.branches(), &[original.clone(), displaced.clone()]);
        assert!(
            session
                .load_branch_from(displaced.id, dir.path())
                .unwrap()
                .messages()
                .is_empty()
        );
        assert_eq!(
            session
                .load_branch_from(original.id, dir.path())
                .unwrap()
                .messages(),
            session.messages()
        );
    }

    #[test_case(false ; "hard_link")]
    #[test_case(true ; "copy_fallback")]
    fn branch_preservation_round_trips_and_copy_failure_removes_partial_file(copy_fallback: bool) {
        let (dir, session) = branch_fixture();
        let source = jsonl_path(dir.path(), session.id);
        let destination = dir.path().join("preserved.jsonl");
        link_archive_with(
            &source,
            &destination,
            |from, to| {
                if copy_fallback {
                    Err(io::Error::other(POST_TEXT))
                } else {
                    fs::hard_link(from, to)
                }
            },
            io::copy,
        )
        .unwrap();
        assert_eq!(fs::read(&source).unwrap(), fs::read(&destination).unwrap());
        let before = fs::read(&destination).unwrap();
        assert!(
            link_archive_with(
                &source,
                &destination,
                |_, _| Err(io::Error::other(POST_TEXT)),
                |_, _| panic!("existing branch must not be copied over"),
            )
            .is_err()
        );
        assert_eq!(fs::read(&destination).unwrap(), before);
        fs::remove_file(&destination).unwrap();
        assert!(
            link_archive_with(
                &source,
                &destination,
                |_, _| Err(io::Error::other(POST_TEXT)),
                |_, to| {
                    to.write_all(POST_TEXT.as_bytes())?;
                    Err(io::Error::other(POST_TEXT))
                },
            )
            .is_err()
        );
        assert!(!destination.exists());
        assert_eq!(
            TestSession::load_from(session.id, dir.path())
                .unwrap()
                .messages(),
            session.messages()
        );
    }

    #[test_case(false ; "cancelled")]
    #[test_case(true ; "preserve_failure")]
    fn branch_publication_aborts_without_mutating_live_log_or_branch_metadata(
        preserve_failure: bool,
    ) {
        let (dir, session) = branch_fixture();
        let before = fs::read(jsonl_path(dir.path(), session.id)).unwrap();
        let saved = branch(&session);
        let mut replacement = session.clone();
        replacement.replace_messages(Vec::new());
        let staged =
            SessionLog::stage_checkpoint_with_branch(dir.path(), &replacement, saved.clone())
                .unwrap();
        if preserve_failure {
            fs::write(dir.path().join(BRANCH_DIR), POST_TEXT).unwrap();
            assert!(staged.publish_unowned(|| true).is_err());
        } else {
            assert!(!staged.publish_unowned(|| false).unwrap());
        }
        assert_eq!(
            fs::read(jsonl_path(dir.path(), session.id)).unwrap(),
            before
        );
        let current = TestSession::load_from(session.id, dir.path()).unwrap();
        assert!(current.branches().is_empty());
        assert!(!branch_path(dir.path(), session.id, saved.id).exists());
    }

    #[test]
    fn branch_rename_failure_cleans_preservation_and_never_prunes() {
        let (dir, session) = branch_fixture();
        let saved = branch(&session);
        let staged =
            SessionLog::stage_checkpoint_with_branch(dir.path(), &session, saved.clone()).unwrap();
        let before = fs::read(jsonl_path(dir.path(), session.id)).unwrap();
        let branch_dir = branch_path(dir.path(), session.id, saved.id)
            .parent()
            .unwrap()
            .to_path_buf();
        fs::create_dir_all(&branch_dir).unwrap();
        let older = branch(&session);
        let older_path = branch_path(dir.path(), session.id, older.id);
        fs::write(&older_path, POST_TEXT).unwrap();
        fs::remove_file(staged.temporary.path()).unwrap();
        assert!(staged.publish_unowned(|| true).is_err());
        assert_eq!(
            fs::read(jsonl_path(dir.path(), session.id)).unwrap(),
            before
        );
        assert!(!branch_path(dir.path(), session.id, saved.id).exists());
        assert_eq!(fs::read_to_string(older_path).unwrap(), POST_TEXT);
    }

    #[test_case(false ; "not_published")]
    #[test_case(true ; "published")]
    fn typed_publication_reports_claim_outcome(publish: bool) {
        let (dir, session) = branch_fixture();
        let staged = SessionLog::stage_checkpoint(dir.path(), &session).unwrap();
        let lease = session_lock::claim(dir.path(), &session.id)
            .unwrap()
            .unwrap();
        let outcome = staged
            .publish_outcome(&lease.publication_guard(), || publish)
            .unwrap();
        assert_eq!(outcome.is_published(), publish);
        assert!(matches!(
            (publish, outcome),
            (false, PublicationOutcome::NotPublished) | (true, PublicationOutcome::Published)
        ));
    }

    #[test]
    fn post_rename_sync_failure_reports_live_publication_and_preserves_retention() {
        let (dir, mut session) = branch_fixture();
        let cursor = SessionLog::rewrite(dir.path(), &session).unwrap();
        let previous = branch(&session);
        let previous_path = branch_path(dir.path(), session.id, previous.id);
        fs::create_dir_all(previous_path.parent().unwrap()).unwrap();
        fs::write(&previous_path, POST_TEXT).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&previous_path)
            .unwrap()
            .set_len(BRANCH_MAX_BYTES)
            .unwrap();
        session.push_branch(previous);
        let saved = branch(&session);
        let before = fs::read(jsonl_path(dir.path(), session.id)).unwrap();
        session.replace_messages(Vec::new());
        session.push_branch(saved.clone());
        let staged =
            SessionLog::stage_checkpoint_with_branch(dir.path(), &session, saved.clone()).unwrap();
        let outcome = staged
            .publish_with_sync(
                || true,
                |path| {
                    assert!(
                        TestSession::load_from(session.id, path.parent().unwrap())
                            .unwrap()
                            .messages()
                            .is_empty()
                    );
                    Err(StorageError::Io(io::Error::other(POST_TEXT)))
                },
            )
            .unwrap();
        assert!(outcome.is_published());
        assert!(
            matches!(outcome, PublicationOutcome::PublishedUnsynced(ref error) if error.to_string() == POST_TEXT)
        );
        let live = fs::read(jsonl_path(dir.path(), session.id)).unwrap();
        SessionLog::sync_publication(dir.path()).unwrap();
        assert_eq!(fs::read(jsonl_path(dir.path(), session.id)).unwrap(), live);
        assert_eq!(fs::metadata(previous_path).unwrap().len(), BRANCH_MAX_BYTES);
        assert_eq!(
            fs::read(branch_path(dir.path(), session.id, saved.id)).unwrap(),
            before
        );
        assert_same_session(
            &TestSession::load_from(session.id, dir.path()).unwrap(),
            &session,
        );
        let mut cursor = cursor;
        assert!(matches!(
            cursor.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
    }

    #[test_case(false ; "count_budget")]
    #[test_case(true ; "byte_budget")]
    fn branch_retention_leaves_expired_metadata_and_restore_is_read_only(byte_budget: bool) {
        let (dir, mut session) = branch_fixture();
        let mut saved = Vec::new();
        for _ in 0..=BRANCH_KEEP {
            let mut branch = branch(&session);
            branch.created_at = saved.len() as u64;
            SessionLog::stage_checkpoint_with_branch(dir.path(), &session, branch.clone())
                .unwrap()
                .publish_unowned(|| true)
                .unwrap();
            session.push_branch(branch.clone());
            saved.push(branch);
        }
        let latest = saved.last().unwrap();
        if byte_budget {
            let newest_path = branch_path(dir.path(), session.id, latest.id);
            let original = fs::read(&newest_path).unwrap();
            OpenOptions::new()
                .write(true)
                .open(&newest_path)
                .unwrap()
                .set_len(BRANCH_MAX_BYTES)
                .unwrap();
            prune_branches(dir.path(), session.id, latest.id, session.branches());
            fs::write(newest_path, original).unwrap();
        }
        let availability = session.list_branches_in(dir.path()).unwrap();
        assert_eq!(availability.len(), BRANCH_KEEP + 1);
        assert!(!availability[0].available);
        let loaded = TestSession::load_from(session.id, dir.path()).unwrap();
        assert_eq!(loaded.branches(), session.branches());
        assert!(!loaded.list_branches_in(dir.path()).unwrap()[0].available);
        let before = fs::read(jsonl_path(dir.path(), session.id)).unwrap();
        assert!(
            matches!(session.load_branch_from(saved[0].id, dir.path()), Err(SessionError::BranchExpired { branch }) if branch == saved[0].id)
        );
        assert_eq!(
            fs::read(jsonl_path(dir.path(), session.id)).unwrap(),
            before
        );
        let remaining = fs::read_dir(
            branch_path(dir.path(), session.id, latest.id)
                .parent()
                .unwrap(),
        )
        .unwrap()
        .count();
        assert_eq!(remaining, if byte_budget { 1 } else { BRANCH_KEEP });
    }

    #[test]
    fn branch_restore_validates_session_and_complete_records() {
        let (dir, mut session) = branch_fixture();
        let saved = branch(&session);
        SessionLog::stage_checkpoint_with_branch(dir.path(), &session, saved.clone())
            .unwrap()
            .publish_unowned(|| true)
            .unwrap();
        session.push_branch(saved.clone());
        let path = branch_path(dir.path(), session.id, saved.id);
        let original = fs::read(&path).unwrap();
        fs::remove_file(&path).unwrap();
        let other = TestSession::new("other", "/other");
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        write_full_session(&mut file, &other).unwrap();
        assert!(matches!(
            session.load_branch_from(saved.id, dir.path()),
            Err(SessionError::BranchExpired { .. })
        ));
        fs::write(&path, &original[..original.len() - 1]).unwrap();
        assert!(matches!(
            session.load_branch_from(saved.id, dir.path()),
            Err(SessionError::BranchExpired { .. })
        ));
        fs::write(&path, &original).unwrap();
        assert!(session.load_branch_from(saved.id, dir.path()).is_ok());
    }

    #[test]
    fn authoritative_replacement_removes_discarded_collections_and_fences_identical_content() {
        let (dir, mut current) = branch_fixture();
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
        let saved = branch(&current);
        current.push_branch(saved.clone());
        let high_water = current.sequence_high_water();
        let revision = current.revision();
        current.replace_authoritative(replacement).unwrap();
        assert!(current.revision() > revision);
        assert!(!current.agents().contains_key(&child));
        assert!(current.tool_outputs().is_empty());
        assert!(current.subagent_messages().is_empty());
        assert!(current.posts().is_empty());
        assert_eq!(current.branches(), &[saved]);
        assert_eq!(current.sequence_high_water(), high_water);
        let revision = current.revision();
        let different = TestSession::new("other", "/other");
        assert!(matches!(
            current.replace_authoritative(different),
            Err(SessionError::IdMismatch { .. })
        ));
        assert_eq!(current.revision(), revision);
        let mut cursor = SessionLog::rewrite(dir.path(), &current).unwrap();
        let revision = current.revision();
        let history = current.history_identity();
        current.replace_authoritative(current.clone()).unwrap();
        assert!(current.revision() > revision);
        assert_eq!(current.history_identity(), history);
        assert!(matches!(
            cursor.append(&current),
            Err(SessionError::LogDiverged { .. })
        ));
        current.save_to(dir.path()).unwrap();
        let loaded = TestSession::load_from(current.id, dir.path()).unwrap();
        assert_eq!(loaded.sequence_high_water(), high_water);
        assert!(loaded.next_state_seq() > high_water.unwrap());
        assert_same_session(&loaded, &current);
    }

    #[test]
    fn journal_history_replacement_removes_absent_children_and_fences_identical_history() {
        let (dir, mut session) = branch_fixture();
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
        let mut cursor = SessionLog::rewrite(dir.path(), &session).unwrap();
        session.replace_journal_histories(&histories);
        assert_eq!(session.history_snapshot().epoch, histories[&root].epoch);
        assert_eq!(
            session.subagent_history_snapshot(POST_AGENT).unwrap().epoch,
            histories[&child].epoch
        );
        assert_eq!(session.messages(), &[user_message(PENDING_DRAFT)]);
        assert_eq!(session.subagent_messages().len(), 1);
        assert_eq!(
            session.subagent_messages()[POST_AGENT].as_ref(),
            &[user_message(POST_TEXT)]
        );
        assert!(!session.adopt_subagent_history(ORPHAN_CHILD.into(), &stale));
        assert!(matches!(
            cursor.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
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

    #[test_case(false; "matching_destination_namespace")]
    #[test_case(true; "different_destination_namespace")]
    fn authoritative_replacement_preserves_supplied_history_lineages_and_fences_cursor(
        different: bool,
    ) {
        const GENERATION: u64 = 41;
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        let child = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut record = super::root_record(child, session.next_state_seq());
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = POST_AGENT.into();
        session.push_agent_revision(record.created_seq, record);
        let stale_root = session.history_snapshot().epoch;
        let root_history = HistorySnapshot::new(Vec::new());
        let child_history = HistorySnapshot::new(Vec::new());
        session.adopt_history(&root_history);
        session.adopt_subagent_history(POST_AGENT.into(), &child_history);
        let stale = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq: stale,
            agent: Some(root),
            generation: GENERATION,
            change: StateChange::Boundary {
                offset: 0,
                lens: [(root, (stale_root, 0)), (child, (child_history.epoch, 0))].into(),
            },
        });
        let pinned = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq: pinned,
            agent: Some(root),
            generation: GENERATION,
            change: StateChange::Boundary {
                offset: 0,
                lens: [
                    (root, (root_history.epoch, 0)),
                    (child, (child_history.epoch, 0)),
                ]
                .into(),
            },
        });
        let pin = serde_json::json!({
            "state": session.state(), "agents": session.agent_revisions(),
            "histories": {
                root.to_string(): {"epoch": root_history.epoch, "compacted": false, "messages": []},
                child.to_string(): {"epoch": child_history.epoch, "compacted": false, "messages": []}
            }
        });
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: None,
            generation: 0,
            change: StateChange::BoundarySnapshot {
                boundary: pinned,
                snapshot: pin.clone(),
            },
        });
        let current = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq: current,
            agent: Some(root),
            generation: GENERATION,
            change: StateChange::Boundary {
                offset: 0,
                lens: [
                    (root, (root_history.epoch, 0)),
                    (child, (child_history.epoch, 0)),
                ]
                .into(),
            },
        });
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(child),
            generation: GENERATION,
            change: StateChange::Compaction {
                epoch: child_history.epoch,
            },
        });
        let replacement = session.clone();
        if different {
            session.adopt_history(&HistorySnapshot::new(Vec::new()));
            session.adopt_subagent_history(POST_AGENT.into(), &HistorySnapshot::new(Vec::new()));
        }
        let mut cursor = SessionLog::rewrite(directory.path(), &session).unwrap();
        session.replace_authoritative(replacement).unwrap();
        let root_epoch = session.history_snapshot().epoch;
        let child_epoch = session.subagent_history_snapshot(POST_AGENT).unwrap().epoch;
        assert_eq!(root_epoch, root_history.epoch);
        assert_eq!(child_epoch, child_history.epoch);
        assert!(matches!(
            cursor.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
        let view = session.latest_state().unwrap();
        assert_eq!(view.boundaries[&current].lens[&root].0, root_epoch);
        assert_eq!(view.boundaries[&current].lens[&child].0, child_epoch);
        assert_eq!(view.boundaries[&stale].lens[&root].0, stale_root);
        assert_eq!(view.boundaries[&stale].lens[&child].0, child_epoch);
        assert_eq!(view.boundaries[&pinned].lens[&root].0, root_history.epoch);
        assert_eq!(view.compactions[&child].1, child_epoch);
        assert!(
            session
                .state()
                .iter()
                .filter(|record| record.agent.is_some())
                .all(|record| record.generation == GENERATION)
        );
        assert!(session.state().iter().any(|record| matches!(&record.change, StateChange::BoundarySnapshot { snapshot, .. } if snapshot == &pin)));
        session.save_to(directory.path()).unwrap();
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        let view = loaded.latest_state().unwrap();
        assert_eq!(
            view.boundaries[&current].lens[&root].0,
            loaded.history_snapshot().epoch
        );
        assert_ne!(
            view.boundaries[&stale].lens[&root].0,
            loaded.history_snapshot().epoch
        );
        assert_eq!(
            view.boundaries[&pinned].lens[&root].0,
            loaded.history_snapshot().epoch
        );
    }

    #[test]
    fn authoritative_replacement_keeps_removed_child_epoch_tombstones() {
        let (dir, mut session) = branch_fixture();
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
        let (dir, mut session) = branch_fixture();
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
        for record in records.iter().filter(|record| record["t"] == "meta") {
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

    #[test_case(false; "rewrite_before_resolution")]
    #[test_case(true; "stage_before_resolution")]
    fn unresolved_v2_config_survives_checkpoint_save_append_and_resolution(staged: bool) {
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
        if staged {
            assert!(
                SessionLog::stage_checkpoint(tmp.path(), &session)
                    .unwrap()
                    .publish_unowned(|| true)
                    .unwrap()
            );
        } else {
            Arc::make_mut(&mut session).save_to(tmp.path()).unwrap();
        }
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
        let mut log = SessionLog::rewrite(tmp.path(), &loaded).unwrap();
        loaded.push_message(user_message("before resolution"));
        log.append(&loaded).unwrap();
        let mut loaded = TestSession::load_from(id, tmp.path()).unwrap();
        loaded.resolve_legacy_mode(resolved_mode()).unwrap();
        let count = loaded.state().len();
        loaded.resolve_legacy_mode(resolved_mode()).unwrap();
        assert_eq!(loaded.state().len(), count);
        let mut log = SessionLog::rewrite(tmp.path(), &loaded).unwrap();
        loaded.push_message(user_message("after resolution"));
        log.append(&loaded).unwrap();
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

    #[test_case(false; "rewrite")]
    #[test_case(true; "staged_publication")]
    fn root_config_updates_header_and_listing_cache_without_metadata_authority(staged: bool) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("old-model", "/old");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        let root = session.root_agent_id().unwrap();
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        assert_eq!(TestSession::list_in("/old", tmp.path()).unwrap().len(), 1);
        let config = StoredAgentConfig {
            cwd: "/new".into(),
            model: "new-model".into(),
            thinking: Some(StoredThinking::Off),
            fast: true,
            workflow: true,
            mode: resolved_mode(),
            options: BTreeMap::new(),
        };
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(root),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        let bytes = fs::read(jsonl_path(tmp.path(), session.id)).unwrap();
        assert!(
            matches!(log.append(&session), Err(SessionError::LogDiverged { reason }) if reason == super::HEADER_CHANGED)
        );
        assert_eq!(fs::read(jsonl_path(tmp.path(), session.id)).unwrap(), bytes);
        if staged {
            let staged = SessionLog::stage_checkpoint(tmp.path(), &session).unwrap();
            assert!(TestSession::list_in("/new", tmp.path()).unwrap().is_empty());
            assert!(staged.publish_unowned(|| true).unwrap());
        } else {
            log = SessionLog::rewrite(tmp.path(), &session).unwrap();
            session.push_message(user_message("after config"));
            log.append(&session).unwrap();
        }
        assert_eq!(records(tmp.path(), session.id)[0]["model"], config.model);
        assert!(TestSession::list_in("/old", tmp.path()).unwrap().is_empty());
        assert_eq!(
            TestSession::list_in("/new", tmp.path()).unwrap()[0].id,
            session.id
        );
        assert!(
            TestSession::latest_in("/old", tmp.path())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            TestSession::latest_in("/new", tmp.path())
                .unwrap()
                .unwrap()
                .id,
            session.id
        );
        assert!(!load_cwd_index(tmp.path()).contains_key("/old"));
        let conflicting = serde_json::json!({
            "t":"meta", "title":"conflict", "token_usage":null, "updated_at":3,
            "mode":"build", "thinking":{"kind":"adaptive"}, "fast":false,
            "workflow":false, "session_options":{"stale":"value"}, "yolo":true,
            "session_rules":[{"tool":"bash", "effect":"allow"}]
        });
        writeln!(
            OpenOptions::new()
                .append(true)
                .open(jsonl_path(tmp.path(), session.id))
                .unwrap(),
            "{conflicting}"
        )
        .unwrap();
        let mut loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        loaded.resolve_legacy_mode(resolved_mode()).unwrap();
        let view = loaded.latest_state().unwrap();
        assert_eq!(view.scopes[&Some(root)].config.as_ref(), Some(&config));
        assert!(view.scopes[&None].rules.is_empty());
        assert_eq!(view.scopes[&None].yolo, None);
        loaded.save_to(tmp.path()).unwrap();
        assert_retired_meta_absent(&records(tmp.path(), session.id));
    }

    #[test]
    fn config_and_rules_append_ignore_stale_legacy_payload_and_seed_only_once() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        let root = session.root_agent_id().unwrap();
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
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
        log.append(&session).unwrap();
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
    fn staged_config_publication_succeeds_when_auxiliary_index_write_fails() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("old-model", "/old");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.push_message(user_message("before config"));
        session.save_to(tmp.path()).unwrap();
        let mut config = session.root_config().unwrap().unwrap();
        config.cwd = "/new".into();
        config.model = "new-model".into();
        config.fast = true;
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: session.root_agent_id(),
            generation: 0,
            change: StateChange::Config(config.clone()),
        });
        session.push_message(user_message("after config"));
        let staged = SessionLog::stage_checkpoint(tmp.path(), &session).unwrap();
        let index_path = tmp.path().join(CWD_INDEX_FILE);
        fs::remove_file(&index_path).unwrap();
        fs::create_dir(&index_path).unwrap();

        assert!(staged.publish_unowned(|| true).unwrap());
        assert!(index_path.is_dir());
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(loaded.root_config().unwrap().as_ref(), Some(&config));
        assert_same_session(&loaded, &session);
    }

    #[test]
    fn rejected_staged_config_publication_preserves_header_and_cwd_index() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("old-model", "/old");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.save_to(tmp.path()).unwrap();
        let before = fs::read(jsonl_path(tmp.path(), session.id)).unwrap();
        let index = load_cwd_index(tmp.path());
        let mut config = session.root_config().unwrap().unwrap();
        config.cwd = "/new".into();
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: session.root_agent_id(),
            generation: 0,
            change: StateChange::Config(config),
        });
        assert!(
            !SessionLog::stage_checkpoint(tmp.path(), &session)
                .unwrap()
                .publish_unowned(|| false)
                .unwrap()
        );
        assert_eq!(
            fs::read(jsonl_path(tmp.path(), session.id)).unwrap(),
            before
        );
        assert_eq!(load_cwd_index(tmp.path()), index);
    }

    #[test]
    fn colliding_staged_configs_publish_only_the_claimed_snapshot() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/old");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.save_to(tmp.path()).unwrap();
        let stale = SessionLog::stage_checkpoint(tmp.path(), &session).unwrap();
        let mut config = session.root_config().unwrap().unwrap();
        config.cwd = "/winner".into();
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: session.root_agent_id(),
            generation: 0,
            change: StateChange::Config(config),
        });
        let winner = SessionLog::stage_checkpoint(tmp.path(), &session).unwrap();
        assert_ne!(stale.temporary.path(), winner.temporary.path());
        assert!(winner.publish_unowned(|| true).unwrap());
        let bytes = fs::read(jsonl_path(tmp.path(), session.id)).unwrap();
        let index = load_cwd_index(tmp.path());
        assert!(!stale.publish_unowned(|| false).unwrap());
        assert_eq!(fs::read(jsonl_path(tmp.path(), session.id)).unwrap(), bytes);
        assert_eq!(load_cwd_index(tmp.path()), index);
        assert_eq!(
            TestSession::load_from(session.id, tmp.path()).unwrap().cwd,
            "/winner"
        );
        assert_same_session(
            &TestSession::load_from(session.id, tmp.path()).unwrap(),
            &session,
        );
    }

    #[test_case(false, false; "rewrite")]
    #[test_case(true, false; "staged_checkpoint")]
    #[test_case(false, true; "empty_child_rewrite")]
    #[test_case(true, true; "empty_child_staged_checkpoint")]
    fn canonical_history_epochs_survive_load_without_rebasing_stale_lenses(
        staged: bool,
        empty: bool,
    ) {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        let child = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut record = super::root_record(child, session.next_state_seq());
        record.parent = Some(root);
        record.depth = 1;
        record.transcript_key = POST_AGENT.into();
        session.push_agent_revision(record.created_seq, record);
        let root_history = HistorySnapshot::new(vec![user_message(PENDING_DRAFT)]);
        let child_history = HistorySnapshot::new(vec![user_message(POST_TEXT)]);
        session.adopt_history(&root_history);
        session.adopt_subagent_history(POST_AGENT.into(), &child_history);
        let seq = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq,
            agent: Some(root),
            generation: root_history.epoch,
            change: StateChange::Boundary {
                offset: 1,
                lens: [
                    (root, (root_history.epoch, 1)),
                    (child, (child_history.epoch, 1)),
                ]
                .into(),
            },
        });
        let replacement = HistorySnapshot::new(if empty {
            Vec::new()
        } else {
            vec![user_message(POST_TEXT)]
        });
        session.adopt_subagent_history(POST_AGENT.into(), &replacement);
        let seq = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq,
            agent: Some(child),
            generation: replacement.epoch,
            change: StateChange::Compaction {
                epoch: replacement.epoch,
            },
        });
        if staged {
            SessionLog::stage_checkpoint(directory.path(), &session)
                .unwrap()
                .publish_unowned(|| true)
                .unwrap();
        } else {
            session.save_to(directory.path()).unwrap();
        }
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        let loaded_root = loaded.history_snapshot().epoch;
        let loaded_child = loaded.subagent_history_snapshot(POST_AGENT).unwrap().epoch;
        let view = loaded.latest_state().unwrap();
        let boundary = view.boundaries.values().next().unwrap();
        assert_ne!(loaded_root, root_history.epoch);
        assert_eq!(boundary.lens[&root].0, loaded_root);
        assert_ne!(boundary.lens[&child].0, loaded_child);
        assert!(boundary.lens[&child].0 < loaded_child);
        assert_eq!(view.compactions[&child].1, loaded_child);
        assert!(super::next_epoch() > loaded_child);
    }

    #[test_case(false; "absent_transcript")]
    #[test_case(true; "explicit_empty_zero_epoch_transcript")]
    fn child_epoch_metadata_preserves_transcript_presence_across_repeated_loads(present: bool) {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        let child = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut child_record = super::root_record(child, session.next_state_seq());
        child_record.parent = Some(root);
        child_record.depth = 1;
        child_record.transcript_key = POST_AGENT.into();
        session.push_agent_revision(child_record.created_seq, child_record);
        if present {
            let history = HistorySnapshot {
                epoch: 0,
                ..Default::default()
            };
            session.adopt_subagent_history(POST_AGENT.into(), &history);
        }
        for _ in 0..3 {
            assert_eq!(
                session.canonical_history_epochs().contains_key(&child),
                present
            );
            session.save_to(directory.path()).unwrap();
            session = TestSession::load_from(session.id, directory.path()).unwrap();
            assert_eq!(
                session.subagent_messages().contains_key(POST_AGENT),
                present
            );
            assert_eq!(
                session.subagent_history_snapshot(POST_AGENT).is_some(),
                present
            );
        }
    }

    #[test]
    fn absent_canonical_epoch_is_not_reconstructed_from_zero_lens() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        session.push_state(StoredStateRecord {
            seq: session.next_state_seq(),
            agent: Some(root),
            generation: 0,
            change: StateChange::Boundary {
                offset: 0,
                lens: [(root, (0, 0))].into(),
            },
        });
        session.save_to(directory.path()).unwrap();
        let path = jsonl_path(directory.path(), session.id);
        let raw = fs::read_to_string(&path).unwrap();
        let mut without_metadata = String::new();
        for line in raw.lines() {
            let mut record: Value = serde_json::from_str(line).unwrap();
            if record["t"] == "meta" {
                record.as_object_mut().unwrap().remove("history_epochs");
            }
            without_metadata.push_str(&record.to_string());
            without_metadata.push('\n');
        }
        fs::write(&path, without_metadata).unwrap();
        let loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        let view = loaded.latest_state().unwrap();
        assert_ne!(
            view.boundaries.values().next().unwrap().lens[&root].0,
            loaded.history_snapshot().epoch
        );
    }

    #[test]
    fn loaded_epoch_mapping_is_complete_ordered_and_independent_of_saved_magnitude() {
        const START: u64 = 11;
        const FIRST: u64 = 0;
        const SECOND: u64 = 37;
        const LAST: u64 = u64::MAX;
        let root = AgentId::generate();
        let child = AgentId::generate();
        let mut canonical = [(root, LAST), (child, FIRST)].into();
        let mut state = vec![
            StoredStateRecord {
                seq: 1,
                agent: Some(root),
                generation: LAST,
                change: StateChange::Boundary {
                    offset: 0,
                    lens: [(root, (SECOND, 0)), (child, (FIRST, 0))].into(),
                },
            },
            StoredStateRecord {
                seq: 2,
                agent: Some(root),
                generation: LAST,
                change: StateChange::Compaction { epoch: LAST },
            },
            StoredStateRecord {
                seq: 3,
                agent: None,
                generation: 0,
                change: StateChange::BoundarySnapshot {
                    boundary: 1,
                    snapshot: serde_json::json!({
                        "histories": {"saved": {"epoch": LAST}, "malformed": {"epoch": "invalid"}},
                        "state": [
                            {"generation": LAST, "change": {"kind": "boundary", "lens": {"saved": [SECOND, 0]}}},
                            {"change": {"kind": "boundary_snapshot", "snapshot": {
                                "histories": {"saved": {"epoch": FIRST}},
                                "state": [{"change": {"kind": "compaction", "epoch": LAST}}]
                            }}}
                        ]
                    }),
                },
            },
        ];
        let allocator = AtomicU64::new(START);
        super::remap_loaded_history_epochs(&mut state, &mut canonical, || {
            allocator.fetch_add(1, Ordering::Relaxed)
        });
        assert_eq!(allocator.load(Ordering::Relaxed), START + 3);
        assert_eq!(canonical[&child], START);
        assert_eq!(canonical[&root], START + 2);
        if let StateChange::Boundary { lens, .. } = &state[0].change {
            assert_eq!(lens[&root].0, START + 1);
            assert_eq!(lens[&child].0, canonical[&child]);
            assert!(lens[&root].0 < canonical[&root]);
        } else {
            panic!("expected boundary")
        }
        assert_eq!(state[0].generation, LAST);
        assert!(
            matches!(state[1].change, StateChange::Compaction { epoch } if epoch == canonical[&root])
        );
        let StateChange::BoundarySnapshot { snapshot, .. } = &state[2].change else {
            panic!("expected pin")
        };
        assert_eq!(snapshot["histories"]["saved"]["epoch"], START + 2);
        assert_eq!(snapshot["histories"]["malformed"]["epoch"], "invalid");
        assert_eq!(snapshot["state"][0]["generation"], LAST);
        assert_eq!(
            snapshot["state"][0]["change"]["lens"]["saved"][0],
            START + 1
        );
        assert_eq!(
            snapshot["state"][1]["change"]["snapshot"]["histories"]["saved"]["epoch"],
            START
        );
        assert_eq!(
            snapshot["state"][1]["change"]["snapshot"]["state"][0]["change"]["epoch"],
            START + 2
        );
    }

    #[test_case(false; "rewrite")]
    #[test_case(true; "staged_checkpoint")]
    fn maximal_saved_epoch_import_remains_writable_and_reloads_consistently(staged: bool) {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let mut history = session.history_snapshot();
        history.epoch = u64::MAX;
        session.adopt_history(&history);
        let boundary = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq: boundary,
            agent: Some(root),
            generation: 0,
            change: StateChange::Boundary {
                offset: 0,
                lens: [(root, (u64::MAX, 0))].into(),
            },
        });
        if staged {
            SessionLog::stage_checkpoint(directory.path(), &session)
                .unwrap()
                .publish_unowned(|| true)
                .unwrap();
        } else {
            session.save_to(directory.path()).unwrap();
        }
        for _ in 0..3 {
            let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
            let runtime_epoch = loaded.history_snapshot().epoch;
            assert_ne!(runtime_epoch, u64::MAX);
            assert_eq!(
                loaded.latest_state().unwrap().boundaries[&boundary].lens[&root].0,
                runtime_epoch
            );
            assert!(super::next_epoch() > runtime_epoch);
            loaded.push_message(user_message(POST_TEXT));
            loaded.save_to(directory.path()).unwrap();
        }
    }

    #[test]
    fn v3_journal_round_trips_append_rewrite_stage_and_archive() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        let root = session.root_agent_id().unwrap();
        session.set_parent(Some(ParentLink {
            session: MakimaId::generate(),
            message_count: 7,
            seq: 11,
        }));
        session.push_message(user_message("first"));
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        let historical_seq = session.next_state_seq() - 1;
        let before = session.state_at(historical_seq).unwrap();
        let seq = session.next_state_seq();
        session.push_state(plugin_record(seq, Some(Value::Null)));
        let mut revision = session.agents()[&root].clone();
        revision.updated_seq = seq + 1;
        revision.label = Some("renamed".into());
        session.push_agent_revision(seq + 1, revision);
        session.push_state(plugin_record(
            seq + 2,
            Some(serde_json::json!({"value": 2})),
        ));
        session.push_branch(branch(&session));
        session.push_message(assistant_message("second"));
        log.append(&session).unwrap();
        let first_append = fs::read(jsonl_path(tmp.path(), session.id)).unwrap();
        log.append(&session).unwrap();
        assert_eq!(
            fs::read(jsonl_path(tmp.path(), session.id)).unwrap(),
            first_append
        );
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_same_session(&loaded, &session);
        assert_eq!(loaded.state_at(historical_seq).unwrap(), before);
        assert_eq!(
            loaded.latest_state().unwrap().agents[&root]
                .label
                .as_deref(),
            Some("renamed")
        );
        let records: Vec<Value> = first_append
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(records[0]["v"], 3);
        let sequences: Vec<_> = records
            .iter()
            .filter_map(|record| match record["t"].as_str() {
                Some("state") => record["d"]["seq"].as_u64(),
                Some("agent") => record["seq"].as_u64(),
                _ => None,
            })
            .collect();
        assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            TestSession::list_all_in(tmp.path()).unwrap()[0].message_count,
            2
        );
        let scanned = super::scan_jsonl_header(&jsonl_path(tmp.path(), session.id)).unwrap();
        assert_eq!(scanned.parent.as_ref(), session.parent());
        let staged = SessionLog::stage_checkpoint(tmp.path(), &session).unwrap();
        session.push_state(plugin_record(seq + 3, None));
        assert!(staged.publish_unowned(|| true).unwrap());
        assert_same_session(
            &TestSession::load_from(session.id, tmp.path()).unwrap(),
            &loaded,
        );
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        session.truncate_messages(1);
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        assert_same_session(
            &TestSession::load_from(session.id, tmp.path()).unwrap(),
            &session,
        );
        let archived: TestSession =
            super::load_session_at(&archive_paths(tmp.path(), session.id)[0]).unwrap();
        assert_eq!(archived.messages().len(), 2);
        assert_eq!(
            normalized_epoch_state(&archived),
            normalized_epoch_state(&session)
        );
        assert_eq!(archived.agent_revisions(), session.agent_revisions());
        assert_eq!(archived.branches(), session.branches());
    }

    #[test_case(0; "state_replacement")]
    #[test_case(1; "agent_revision_replacement")]
    #[test_case(2; "branch_replacement")]
    fn same_length_journal_replacements_invalidate_append(collection: usize) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        session.push_state(plugin_record(
            session.next_state_seq(),
            Some(Value::Bool(true)),
        ));
        session.push_branch(branch(&session));
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        let bytes = fs::read(jsonl_path(tmp.path(), session.id)).unwrap();
        match collection {
            0 => {
                let mut records = session.state().to_vec();
                let last = records.last_mut().unwrap();
                *last = plugin_record(last.seq, None);
                session.replace_state(records);
            }
            1 => {
                let mut records = session.agent_revisions().to_vec();
                records[0].1.label = Some("changed".into());
                session.replace_agent_revisions(records);
            }
            _ => {
                let mut records = session.branches().to_vec();
                records[0].offset += 1;
                session.replace_branches(records);
            }
        }
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
        assert_eq!(fs::read(jsonl_path(tmp.path(), session.id)).unwrap(), bytes);
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        assert_same_session(
            &TestSession::load_from(session.id, tmp.path()).unwrap(),
            &session,
        );
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

    #[test]
    fn v2_double_load_can_bind_root_before_publication_and_rekey_seeded_state() {
        let tmp = TempDir::new().unwrap();
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let header = serde_json::json!({"t":"header", "v":2, "id":id, "model":"legacy", "cwd":"/project", "created_at":0});
        let meta = serde_json::json!({"t":"meta", "title":"legacy", "token_usage":null, "updated_at":0,
            "subagents":[{"tool_use_id":"child", "name":"legacy child"}]});
        fs::write(jsonl_path(tmp.path(), id), format!("{header}\n{meta}\n")).unwrap();
        let first = TestSession::load_from(id, tmp.path()).unwrap();
        let expected = first.root_agent_id().unwrap();
        let mut second = TestSession::load_from(id, tmp.path()).unwrap();
        let previous = second.root_agent_id().unwrap();
        assert_ne!(expected, previous);
        assert!(second.legacy_root_unpublished);
        second.resolve_legacy_mode(resolved_mode()).unwrap();
        let seq = second.next_state_seq();
        second.push_state(StoredStateRecord {
            seq,
            agent: Some(previous),
            generation: 0,
            change: StateChange::Boundary {
                offset: 0,
                lens: BTreeMap::from([(previous, (0, 0))]),
            },
        });
        second.push_branch(branch(&second));
        second.bind_root_id(expected).unwrap();
        assert!(!second.legacy_root_unpublished);
        assert_eq!(second.root_agent_id(), Some(expected));
        assert!(!second.agents().contains_key(&previous));
        assert!(
            second
                .agents()
                .values()
                .filter(|agent| agent.parent.is_some())
                .all(|agent| agent.parent == Some(expected))
        );
        let view = second.latest_state().unwrap();
        assert!(!view.scopes.contains_key(&Some(previous)));
        assert_eq!(
            view.scopes[&Some(expected)].config.as_ref().unwrap().mode,
            resolved_mode()
        );
        assert_eq!(view.boundaries[&seq].agent, Some(expected));
        assert_eq!(
            view.boundaries[&seq].lens,
            BTreeMap::from([(expected, (0, 0))])
        );
        assert_eq!(second.branches()[0].agent, expected);
        second.save_to(tmp.path()).unwrap();
        let reloaded = TestSession::load_from(id, tmp.path()).unwrap();
        assert_same_session(&reloaded, &second);
        assert!(!reloaded.legacy_root_unpublished);
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
    #[test_case(b"{\"t\":\"branch\",\"d\":{}}\n"; "malformed_branch")]
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

    #[test]
    fn inserting_an_earlier_cross_stream_revision_requires_rewrite() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.resolve_legacy_mode(resolved_mode()).unwrap();
        let seq = session.next_state_seq();
        session.push_state(plugin_record(seq + 1, Some(Value::Bool(true))));
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        let mut revision = session.agents()[&session.root_agent_id().unwrap()].clone();
        revision.updated_seq = seq;
        revision.label = Some("inserted earlier".into());
        session.push_agent_revision(seq, revision);
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        assert_same_session(
            &TestSession::load_from(session.id, tmp.path()).unwrap(),
            &session,
        );
    }

    #[test]
    fn invalid_journal_is_rejected_before_append_or_publication() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        session.push_state(plugin_record(1, Some(Value::Bool(true))));
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        let path = jsonl_path(tmp.path(), session.id);
        let bytes = fs::read(&path).unwrap();
        session.push_state(plugin_record(1, None));
        assert!(matches!(
            session.latest_state(),
            Err(SessionError::InvalidState(_))
        ));
        assert!(matches!(
            log.append(&session),
            Err(SessionError::InvalidState(_))
        ));
        assert!(matches!(
            SessionLog::rewrite(tmp.path(), &session),
            Err(SessionError::InvalidState(_))
        ));
        assert!(matches!(
            SessionLog::stage_checkpoint(tmp.path(), &session),
            Err(SessionError::InvalidState(_))
        ));
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn child_history_epochs_fence_reconstructed_events_without_changing_disk_format() {
        let mut session = TestSession::new("model", "/tmp");
        let earlier = HistorySnapshot::new(vec![serde_json::json!("earlier")]);
        let latest = HistorySnapshot::new(vec![serde_json::json!("latest")]);
        assert!(session.adopt_subagent_history(POST_AGENT.into(), &earlier));
        assert!(session.adopt_subagent_history(POST_AGENT.into(), &latest));
        let reconstructed = HistorySnapshot {
            messages: Arc::new(earlier.messages.as_ref().clone()),
            ..earlier
        };
        assert!(!session.adopt_subagent_history(POST_AGENT.into(), &reconstructed));
        let current = session.subagent_history_snapshot(POST_AGENT).unwrap();
        assert_eq!(current.epoch, latest.epoch);
        assert_eq!(current.messages, latest.messages);
        let tmp = TempDir::new().unwrap();
        session.save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(loaded.subagent_messages(), session.subagent_messages());
        assert_eq!(
            loaded.subagent_history_snapshot(POST_AGENT).unwrap().epoch,
            0
        );
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
        // Tag first, matching the `LogRecord` layout the real appends write:
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

    #[test]
    fn subagent_identity_roundtrip_and_legacy_defaults() {
        let legacy: StoredSubagent =
            serde_json::from_str(r#"{"tool_use_id":"tool","name":"child"}"#).unwrap();
        assert_eq!(legacy.agent_id, None);
        assert!(!legacy.ambiguous);

        let tmp = TempDir::new().unwrap();
        let mut session: TestSession = Session::new("model", "/project");
        let mut child = legacy;
        child.agent_id = Some("agent".into());
        session.set_subagents(vec![child.clone()]);
        session.save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(loaded.subagents(), &[child]);
    }

    #[test_case(true; "duplicate_key")]
    #[test_case(false; "duplicate_agent_id")]
    fn duplicate_subagent_identities_remain_ambiguous_after_removal(duplicate_key: bool) {
        let child = StoredSubagent {
            tool_use_id: "first".into(),
            agent_id: Some("agent".into()),
            ambiguous: false,
            name: "child".into(),
            model: None,
            thinking: None,
            fast: false,
        };
        let mut other = child.clone();
        if !duplicate_key {
            other.tool_use_id = "second".into();
        }
        let mut session: TestSession = Session::new("model", "/project");
        session.subagents = vec![child, other];
        let raw = TempDir::new().unwrap();
        session.save_to(raw.path()).unwrap();
        let mut session = TestSession::load_from(session.id, raw.path()).unwrap();
        assert!(
            session
                .subagents()
                .iter()
                .all(|child| child.ambiguous && child.agent_id.is_none())
        );
        session.set_subagents(vec![session.subagents()[0].clone()]);
        let tmp = TempDir::new().unwrap();
        session.save_to(tmp.path()).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert!(loaded.subagents()[0].ambiguous);
        assert!(loaded.subagents()[0].agent_id.is_none());
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
            .find(|record| record["t"] == "meta")
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
        fs::write(tmp.path().join(format!("{FIXTURE_ID}.jsonl")), json).unwrap();

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

    /// `subagents` and `usage_by_model` moved off `SessionMeta` onto the
    /// session, which must not move them in the file: they were flattened into
    /// the meta record and they still sit there.
    #[test]
    fn session_owned_fields_keep_their_place_in_the_meta_record() {
        let id: MakimaId = FIXTURE_ID.parse().unwrap();
        let meta_line = concat!(
            r#"{"t":"meta","title":"t","token_usage":null,"updated_at":0,"fast":true,"#,
            r#""subagents":[{"tool_use_id":"t1","name":"child"}],"#,
            r#""usage_by_model":{"m":{"input":7,"output":3}}}"#,
        );
        let json = format!(
            r#"{{"t":"header","v":2,"id":"{FIXTURE_ID}","model":"m","cwd":"/","created_at":0}}
{meta_line}"#
        );
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join(format!("{FIXTURE_ID}.jsonl")), json).unwrap();

        let mut loaded = TestSession::load_from(id, tmp.path()).unwrap();
        assert_eq!(loaded.subagents()[0].name, "child");
        assert_eq!(loaded.usage_by_model()["m"].total(), 10);
        assert!(loaded.meta.fast, "flattened meta still parses alongside");

        let dir = tmp.path().join("rewritten");
        fs::create_dir(&dir).unwrap();
        loaded.save_to(&dir).unwrap();
        let reloaded = TestSession::load_from(id, &dir).unwrap();
        assert_same_session(&reloaded, &loaded);
        assert_eq!(reloaded.subagents(), loaded.subagents());
        assert_eq!(reloaded.usage_by_model(), loaded.usage_by_model());
    }

    #[test]
    fn roundtrip_jsonl_incremental() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));

        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        session.push_message(assistant_message("reply"));
        session.push_message(user_message("second"));
        session.tool_outputs.insert(
            "tool-1".into(),
            Arc::new(serde_json::json!({"result": "ok"})),
        );
        session
            .subagent_messages
            .insert("sub-1".into(), Arc::new(vec![user_message("sub-prompt")]));
        log.append(&session).unwrap();

        Arc::make_mut(session.subagent_messages.get_mut("sub-1").unwrap())
            .push(assistant_message("sub-reply"));
        session
            .subagent_messages
            .insert("sub-2".into(), Arc::new(vec![user_message("sub-2-prompt")]));
        log.append(&session).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 3);
        assert_eq!(loaded.tool_outputs().len(), 1);
        assert!(loaded.tool_outputs().contains_key("tool-1"));
        assert_eq!(loaded.subagent_messages["sub-1"].len(), 2);
        assert_eq!(loaded.subagent_messages["sub-2"].len(), 1);
    }

    fn stored_post(chat: Option<&str>, after_message: usize, agent: Option<&str>) -> StoredPost {
        StoredPost {
            chat: chat.map(str::to_owned),
            after_message,
            post: Post {
                text: POST_TEXT.into(),
                level: PostLevel::Warn,
                agent: agent.map(str::to_owned),
            },
        }
    }

    #[test_case(None; "main")]
    #[test_case(Some(POST_AGENT); "child")]
    fn prune_posts_only_changes_the_selected_chat(chat: Option<&str>) {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("m", "/project");
        for selected in [None, Some(POST_AGENT)] {
            session.push_post(stored_post(selected, 1, None));
            session.push_post(stored_post(selected, 2, None));
        }
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        session.prune_posts(chat, 1);
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
        let expected: Vec<_> = [None, Some(POST_AGENT)]
            .into_iter()
            .flat_map(|selected| {
                let mut posts = vec![stored_post(selected, 1, None)];
                if selected != chat {
                    posts.push(stored_post(selected, 2, None));
                }
                posts
            })
            .collect();
        assert_eq!(session.posts(), expected);
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(loaded.posts(), expected);
    }

    #[test]
    fn posts_are_written_once_instead_of_repeated_in_meta() {
        let tmp = TempDir::new().unwrap();
        let mut session = TestSession::new("m", "/project");
        session.push_message(user_message("first"));
        let mut log = SessionLog::rewrite(tmp.path(), &session).unwrap();
        for index in 0..POST_APPEND_COUNT {
            let post = if index == 0 {
                stored_post(None, 1, Some(POST_AGENT))
            } else {
                stored_post(None, 0, None)
            };
            session.push_post(post);
            log.append(&session).unwrap();
        }
        let data = fs::read_to_string(jsonl_path(tmp.path(), session.id)).unwrap();
        let records: Vec<Value> = data
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let post_records: Vec<_> = records
            .iter()
            .filter(|record| record["t"] == "post")
            .collect();
        assert_eq!(post_records.len(), POST_APPEND_COUNT);
        assert_eq!(post_records[0]["d"]["after_message"], 1);
        assert_eq!(post_records[0]["d"]["agent"], POST_AGENT);
        assert!(
            records
                .iter()
                .filter(|record| record["t"] == "meta")
                .all(|record| record.get("posts").is_none())
        );
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(loaded.posts(), session.posts());
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        let rewritten = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert_eq!(rewritten.posts(), session.posts());
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

    #[test]
    fn prune_orphans_drops_rewound_posts_and_keeps_their_agents() {
        fn ids(m: &Value) -> Vec<String> {
            vec![m.as_str().unwrap().to_owned()]
        }
        const STALE_AGENT: &str = "session-stale";

        let mut session: TestSession = Session::new("model", "/p");
        session.push_message("task-live".into());
        for agent in [POST_AGENT, STALE_AGENT] {
            session
                .subagent_messages
                .insert(agent.into(), Arc::new(Vec::new()));
        }
        let kept = stored_post(None, 1, Some(POST_AGENT));
        let nested = stored_post(Some(POST_AGENT), 0, None);
        session.push_post(kept.clone());
        session.push_post(stored_post(None, 2, Some(STALE_AGENT)));
        session.push_post(nested.clone());
        session.push_post(stored_post(Some(STALE_AGENT), 0, None));

        session.prune_orphans(ids);

        assert_eq!(session.posts(), [kept, nested]);
        assert_eq!(
            session.subagent_messages().keys().collect::<Vec<_>>(),
            [POST_AGENT]
        );
    }

    /// The mirror re-adopts the run's snapshot on every checkpoint; that must
    /// not erase the void minted by a same-frame in-place replacement, or the
    /// writer appends onto a stale prefix and persists a mixed transcript.
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
        SessionLog::rewrite(tmp.path(), &session).unwrap();
        let loaded = TestSession::load_from(session.id, tmp.path()).unwrap();
        assert!(loaded.history_snapshot().compacted);
    }

    #[test]
    fn conversations_round_trip_and_a_rotation_alone_reaches_disk() {
        const FIRST: &str = "conversation-1";
        const ROTATED: &str = "conversation-2";
        const CHILD: &str = "child-conversation";
        const CHILD_KEY: &str = "sub-1";
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: Arc<TestSession> = Arc::new(Session::new("m", "/project"));
        let mut snapshot = HistorySnapshot::new(vec![user_message("hi")]);
        snapshot.conversation = Some(ConversationState::new(FIRST.into()));
        let mut child = HistorySnapshot::new(vec![user_message("child")]);
        child.conversation = Some(ConversationState::new(CHILD.into()));
        let meta = session.meta.clone();
        Session::checkpoint(&mut session, Some(&snapshot), meta.clone(), Value::Null);
        let writable = Arc::make_mut(&mut session);
        assert!(writable.adopt_subagent_history(CHILD_KEY.into(), &child));
        child.conversation = None;
        assert!(writable.adopt_subagent_history(CHILD_KEY.into(), &child));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        snapshot.conversation = Some(ConversationState {
            previous: Some(ConversationLink {
                id: FIRST.into(),
                relation: ConversationRelation::Continues,
                shared: 1,
            }),
            sent: Some(1),
            ..ConversationState::new(ROTATED.into())
        });
        let revision = session.revision();
        Session::checkpoint(&mut session, Some(&snapshot), meta, Value::Null);
        assert_ne!(session.revision(), revision);
        write_through(&mut log, dir, &session);

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(
            loaded.history_snapshot().conversation,
            snapshot.conversation
        );
        assert_eq!(
            loaded
                .subagent_history_snapshot(CHILD_KEY)
                .and_then(|child| child.conversation)
                .map(|conversation| conversation.id)
                .as_deref(),
            Some(CHILD)
        );
    }

    #[test]
    fn snapshot_adoption_does_not_erase_a_local_rewrite() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: Arc<TestSession> = Arc::new(Session::new("m", "/project"));
        let run = HistorySnapshot {
            epoch: next_epoch(),
            compacted: false,
            conversation: None,
            messages: Arc::new(vec![user_message("hi")]),
        };
        let meta = session.meta.clone();
        Session::checkpoint(&mut session, Some(&run), meta.clone(), Value::Null);
        Arc::make_mut(&mut session)
            .set_subagent_messages("sub-1".into(), vec![user_message("old")]);
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        Arc::make_mut(&mut session)
            .set_subagent_messages("sub-1".into(), vec![user_message("new")]);
        let advanced = HistorySnapshot {
            epoch: run.epoch,
            compacted: run.compacted,
            conversation: None,
            messages: Arc::new(vec![user_message("hi"), assistant_message("reply")]),
        };
        Session::checkpoint(&mut session, Some(&advanced), meta, Value::Null);
        write_through(&mut log, dir, &session);

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
    }

    #[test]
    fn replacing_subagent_messages_rewrites_the_log() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        session.set_subagent_messages("sub-1".into(), vec![user_message("old")]);
        write_through(&mut log, dir, &session);

        session.set_subagent_messages("sub-1".into(), vec![user_message("new")]);
        write_through(&mut log, dir, &session);

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
    }

    #[test]
    fn log_asks_for_a_rewrite_once_appends_pile_up() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        for i in 0..MAX_APPENDS {
            session.push_message(user_message(&format!("m{i}")));
            log.append(&session).unwrap();
        }
        session.push_message(user_message("one too many"));

        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged {
                reason: LOG_BLOATED
            })
        ));
    }

    /// `cwd` and `model` live in the header record, so changing them must
    /// diverge the log into a rewrite, which also refreshes the cwd index.
    #[test]
    fn cwd_and_model_changes_survive_reload_through_a_rewrite() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/old");
        session.push_message(user_message("hi"));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        session.set_model("m2".into());
        session.set_cwd("/new".into());
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged { .. })
        ));
        drop(SessionLog::rewrite(dir, &session).unwrap());

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.model, "m2");
        assert_eq!(loaded.cwd, "/new");
        assert_eq!(
            load_cwd_index(dir).get("/new"),
            Some(&session.id.to_string())
        );
    }

    #[test]
    fn append_wrong_session_returns_id_mismatch() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let session_a: TestSession = Session::new("m", "/project");
        let session_b: TestSession = Session::new("m", "/project");
        let mut log = SessionLog::rewrite(dir, &session_a).unwrap();

        let err = log.append(&session_b).unwrap_err();
        assert!(matches!(err, SessionError::IdMismatch { .. }));
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
        drop(SessionLog::rewrite(dir, &session).unwrap());

        session.truncate_messages(5);
        session.tool_outputs.clear();
        session.subagent_messages.remove("sub-1");
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        session.push_message(user_message("after-compact-1"));
        session.push_message(user_message("after-compact-2"));
        session.push_message(user_message("after-compact-3"));
        log.append(&session).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 8);
        assert!(loaded.subagent_messages().is_empty());
    }

    fn archive_dir_for(dir: &Path, id: MakimaId) -> PathBuf {
        dir.join(ARCHIVE_DIR).join(id.to_string())
    }

    fn archive_paths(dir: &Path, id: MakimaId) -> Vec<PathBuf> {
        let mut paths = fs::read_dir(archive_dir_for(dir, id))
            .unwrap()
            .flatten()
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }

    fn msg_line_count(path: &Path) -> usize {
        fs::read(path)
            .unwrap()
            .split(|&b| b == b'\n')
            .filter(|line| line.starts_with(MSG_PREFIX))
            .count()
    }

    #[test]
    fn rewrite_dropping_messages_archives_the_old_file() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        for i in 0..5 {
            session.push_message(user_message(&format!("turn {i}")));
        }
        session.save_to(dir).unwrap();

        session.replace_messages(vec![user_message("summary")]);
        session.save_to(dir).unwrap();

        let live = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(live.messages().len(), 1);
        let archives = archive_paths(dir, session.id);
        assert_eq!(archives.len(), 1);
        assert_eq!(msg_line_count(&archives[0]), 5);
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
    fn archived_file_round_trips() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        let pre: Vec<Value> = (0..3).map(|i| user_message(&format!("turn {i}"))).collect();
        for msg in &pre {
            session.push_message(msg.clone());
        }
        session.save_to(dir).unwrap();

        session.replace_messages(vec![assistant_message("summary")]);
        session.save_to(dir).unwrap();

        let scratch = TempDir::new().unwrap();
        let archives = archive_paths(dir, session.id);
        assert_eq!(archives.len(), 1);
        fs::copy(
            &archives[0],
            scratch.path().join(format!("{}.jsonl", session.id)),
        )
        .unwrap();
        let archived = TestSession::load_from(session.id, scratch.path()).unwrap();
        assert_eq!(archived.messages(), pre.as_slice());
    }

    #[test]
    fn archive_retention_keeps_newest_three() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("seed"));
        session.save_to(dir).unwrap();
        for round in 1..=5 {
            for _ in 0..round {
                session.push_message(user_message(&format!("turn {round}")));
            }
            session.save_to(dir).unwrap();
            session.replace_messages(vec![user_message(&format!("summary {round}"))]);
            session.save_to(dir).unwrap();
        }

        let archives = archive_paths(dir, session.id);
        assert_eq!(archives.len(), ARCHIVE_KEEP);
        let mut msg_counts: Vec<usize> = archives.iter().map(|p| msg_line_count(p)).collect();
        msg_counts.sort_unstable();
        assert_eq!(msg_counts, [4, 5, 6]);
    }

    /// A new name has to beat every name already there, or pruning would read
    /// the fresh archive as the oldest and eat it.
    #[test]
    fn archive_names_count_up_from_the_newest() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.push_message(user_message("two"));
        session.save_to(dir).unwrap();

        let archive_dir = archive_dir_for(dir, session.id);
        fs::create_dir_all(&archive_dir).unwrap();
        let existing = archive_dir.join(format!("{EXISTING_ARCHIVE_SEQ}.jsonl"));
        fs::write(&existing, "").unwrap();

        session.replace_messages(vec![user_message("summary")]);
        session.save_to(dir).unwrap();

        let fresh = archive_dir.join(format!("{}.jsonl", EXISTING_ARCHIVE_SEQ + 1));
        assert_eq!(
            archive_paths(dir, session.id),
            vec![existing, fresh.clone()]
        );
        assert_eq!(msg_line_count(&fresh), 2);
    }

    #[test]
    fn archive_retention_honors_the_byte_budget() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.push_message(user_message("two"));
        session.save_to(dir).unwrap();

        let archive_dir = archive_dir_for(dir, session.id);
        fs::create_dir_all(&archive_dir).unwrap();
        let fakes: Vec<PathBuf> = (1..=3)
            .map(|ms| {
                let path = archive_dir.join(format!("{ms}.jsonl"));
                // Sparse: the length is all the budget looks at.
                fs::File::create(&path)
                    .unwrap()
                    .set_len(FAKE_ARCHIVE_BYTES)
                    .unwrap();
                path
            })
            .collect();

        session.replace_messages(vec![user_message("summary")]);
        session.save_to(dir).unwrap();

        let archives = archive_paths(dir, session.id);
        assert_eq!(archives.len(), 2);
        assert!(archives.contains(&fakes[2]));
        assert!(!fakes[0].exists());
        assert!(!fakes[1].exists());
    }

    #[test]
    fn delete_removes_archive_dir() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("model", "/p");
        session.push_message(user_message("one"));
        session.push_message(user_message("two"));
        session.save_to(dir).unwrap();
        session.replace_messages(vec![user_message("summary")]);
        session.save_to(dir).unwrap();
        let archive_dir = archive_dir_for(dir, session.id);
        assert!(archive_dir.exists());

        TestSession::delete_from(session.id, dir).unwrap();
        assert!(!archive_dir.exists());
        assert!(!jsonl_path(dir, session.id).exists());
    }

    #[test]
    fn delete_removes_branch_directory_without_touching_other_sessions() {
        let (dir, session) = branch_fixture();
        let saved = branch(&session);
        SessionLog::stage_checkpoint_with_branch(dir.path(), &session, saved.clone())
            .unwrap()
            .publish_unowned(|| true)
            .unwrap();
        let other = MakimaId::generate();
        let other_path = branch_path(dir.path(), other, saved.id);
        fs::create_dir_all(other_path.parent().unwrap()).unwrap();
        fs::write(&other_path, POST_TEXT).unwrap();
        TestSession::delete_from(session.id, dir.path()).unwrap();
        assert!(
            !branch_path(dir.path(), session.id, saved.id)
                .parent()
                .unwrap()
                .exists()
        );
        assert!(!jsonl_path(dir.path(), session.id).exists());
        assert_eq!(fs::read_to_string(other_path).unwrap(), POST_TEXT);
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

    /// A rename with no new messages must survive restart, while a no-op
    /// append must not grow the file.
    #[test]
    fn append_writes_meta_only_when_it_changed() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("hi"));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        let path = jsonl_path(dir, session.id);
        let size_before = fs::metadata(&path).unwrap().len();
        log.append(&session).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), size_before);

        session.title = "renamed".into();
        session.updated_at = 42;
        log.append(&session).unwrap();

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.title, "renamed");
        assert_eq!(loaded.updated_at, 42);
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
        SessionLog::rewrite(dir, &s1).unwrap();
        let mut s2: TestSession = Session::new("m", "/b");
        s2.updated_at = 200;
        SessionLog::rewrite(dir, &s2).unwrap();

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
        let mut log = SessionLog::rewrite(dir, &s1).unwrap();
        let s2: TestSession = Session::new("m", "/project");
        SessionLog::rewrite(dir, &s2).unwrap();
        TestSession::list_in("/project", dir).unwrap();

        s1.title = "renamed".into();
        log.append(&s1).unwrap();
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
        let mut log = SessionLog::rewrite(dir, &s).unwrap();
        s.title = "line one\n\n\tline two".into();
        log.append(&s).unwrap();

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
        SessionLog::rewrite(dir, session).unwrap();
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
    fn scan_counts_complete_unterminated_final_record_like_the_loader() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut s: TestSession = Session::new("m", "/project");
        s.push_message(user_message("one"));
        let path = jsonl_path(dir, s.id);
        write_jsonl(&path, &s);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        // The crash happened between the record's closing brace and the
        // newline the writer emits after it. Written literally, because the
        // scan matches MSG_PREFIX against the head of the line: a `json!`
        // round-trip orders keys by whether serde_json has `preserve_order`,
        // which is a workspace-wide feature this crate does not ask for, so
        // building the record that way passes or fails with the build graph.
        file.write_all(br#"{"t":"msg","d":{"role":"user"}}"#)
            .unwrap();
        drop(file);

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list[0].message_count, 2);
        assert_eq!(
            TestSession::load_from(s.id, dir).unwrap().messages().len(),
            2
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

    /// A torn tail is what a crash mid-append leaves behind, and the file is
    /// the only thing standing between the user and their conversation, so the
    /// cursor `rewrite` hands back must describe the file it just wrote.
    #[test]
    fn rewrite_replaces_a_torn_file_and_keeps_appending() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));
        drop(SessionLog::rewrite(dir, &session).unwrap());

        let path = jsonl_path(dir, session.id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"t\":\"msg\",\"d\":{\"trun").unwrap();
        drop(file);

        session.push_message(assistant_message("reply"));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();
        session.push_message(user_message("second"));
        log.append(&session).unwrap();
        drop(log);

        let reloaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&reloaded, &session);
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
    fn crash_recovery_preserves_tool_outputs_around_corrupt_line() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));
        session
            .tool_outputs
            .insert("t1".into(), Arc::new(serde_json::json!({"result": "ok"})));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();
        log.append(&session).unwrap();

        let path = jsonl_path(dir, session.id);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"CORRUPT\n").unwrap();
        drop(file);
        append_raw_msg(&path, user_message("second"));

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_eq!(loaded.messages().len(), 2);
        assert!(loaded.tool_outputs().contains_key("t1"));
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
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(record).unwrap();
        drop(file);
        append_raw_msg(&path, user_message("second"));
        let before = fs::read(&path).unwrap();

        let error = TestSession::load_from(session.id, dir).unwrap_err();
        assert!(
            matches!(error, SessionError::UnreadableRecord { line: 4, .. }),
            "{error}"
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn pooled_boundary_feature_is_emitted_only_for_actual_payloads_and_forces_header_rewrite() {
        let directory = TempDir::new().unwrap();
        let mut session = TestSession::new("model", "/project");
        let root = AgentId::generate();
        session.bind_root_id(root).unwrap();
        let boundary = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq: boundary,
            agent: Some(root),
            generation: 0,
            change: StateChange::Boundary {
                offset: 0,
                lens: BTreeMap::from([(root, (7, 0))]),
            },
        });
        let mut log = SessionLog::rewrite(directory.path(), &session).unwrap();
        assert!(
            TestSession::load_from(session.id, directory.path())
                .unwrap()
                .features()
                .is_empty()
        );
        let snapshot = serde_json::json!({
            "format":super::BOUNDARY_POOL_FORMAT,"boundaries":[boundary],
            "state":session.state(),"agents":session.agent_revisions(),
            "histories":{root.to_string():{"epoch":7,"compacted":false,"messages":[]}}
        });
        let sequence = session.next_state_seq();
        session.push_state(StoredStateRecord {
            seq: sequence,
            agent: None,
            generation: 0,
            change: StateChange::BoundarySnapshot { boundary, snapshot },
        });
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged {
                reason: super::HEADER_CHANGED
            })
        ));
        session.save_to(directory.path()).unwrap();
        let mut loaded = TestSession::load_from(session.id, directory.path()).unwrap();
        assert_eq!(
            loaded.features(),
            &BTreeSet::from([super::BOUNDARY_POOLS_FEATURE.to_owned()])
        );
        assert_eq!(TestSession::list_all_in(directory.path()).unwrap().len(), 1);
        let path = jsonl_path(directory.path(), session.id);
        let before = fs::read_to_string(&path).unwrap();
        let mut lines = before.lines();
        let mut header: Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        header["features"] = serde_json::json!([]);
        fs::write(
            &path,
            format!("{header}\n{}\n", lines.collect::<Vec<_>>().join("\n")),
        )
        .unwrap();
        assert!(matches!(
            TestSession::load_from(session.id, directory.path()),
            Err(SessionError::UnsupportedFeatures { .. })
        ));
        let state = loaded
            .state()
            .iter()
            .filter(|record| !matches!(record.change, StateChange::BoundarySnapshot { .. }))
            .cloned()
            .collect();
        loaded.replace_state(state);
        loaded.save_to(directory.path()).unwrap();
        assert!(
            TestSession::load_from(session.id, directory.path())
                .unwrap()
                .features()
                .is_empty()
        );
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

    #[test]
    fn scan_returns_latest_title_after_multiple_appends() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("first"));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        session.title = "v1".into();
        session.push_message(assistant_message("reply"));
        log.append(&session).unwrap();

        session.title = "v2".into();
        session.push_message(user_message("second"));
        log.append(&session).unwrap();

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
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        session.title = "big-meta".into();
        session.meta.input_draft = Some("x".repeat(TAIL_BUF as usize * 2));
        session.push_message(assistant_message("reply"));
        log.append(&session).unwrap();

        let list = TestSession::list_in("/project", dir).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].title, "big-meta");
    }

    // -- The log never guesses --

    const PROPERTY_SEED: u64 = 0x2545_F491_4F6C_DD1D;
    const PROPERTY_STEPS: usize = 500;
    const MUTATION_KINDS: u64 = 15;
    const EXTERNAL_TRUNCATION: u64 = 12;

    /// Deterministic xorshift so a failure is always the same failure.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn tool_message(id: &str) -> Value {
        serde_json::json!({ "role": "assistant", "tool": id })
    }

    fn tool_ids(m: &Value) -> Vec<String> {
        m.get("tool")
            .and_then(Value::as_str)
            .map(|s| vec![s.to_owned()])
            .unwrap_or_default()
    }

    /// What the storage writer does: append while the epoch holds, rewrite the
    /// whole file otherwise.
    fn write_through(log: &mut SessionLog, dir: &Path, session: &TestSession) {
        match log.append(session) {
            Err(SessionError::LogDiverged { .. }) => {
                *log = SessionLog::rewrite(dir, session).unwrap()
            }
            other => other.unwrap(),
        }
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
        assert_eq!(loaded.branches(), expected.branches(), "branches");
        assert_eq!(loaded.parent(), expected.parent(), "parent");
        assert_eq!(loaded.features(), expected.features(), "features");
        assert_eq!(
            crate::session_state::SessionStateView::fold(
                &loaded_state,
                loaded.agent_revisions(),
                u64::MAX
            )
            .unwrap(),
            crate::session_state::SessionStateView::fold(
                &expected_state,
                expected.agent_revisions(),
                u64::MAX
            )
            .unwrap(),
            "folded state"
        );
    }

    fn mutate(session: &mut TestSession, rng: &mut Rng, step: usize) {
        let slot = format!("t{}", rng.below(4));
        match rng.below(MUTATION_KINDS) {
            0 => session.push_message(user_message(&format!("msg-{step}"))),
            1 => {
                session.push_message(tool_message(&slot));
                session.push_message(assistant_message("reply"));
            }
            2 => session.insert_tool_output(slot, Value::from(format!("out-{step}"))),
            3 => {
                let len = rng.below(4) as usize;
                let msgs = (0..len)
                    .map(|i| user_message(&format!("sub-{i}")))
                    .collect();
                session.set_subagent_messages(slot, msgs);
            }
            4 => {
                let len = session.messages().len();
                session.truncate_messages(len.saturating_sub(1 + rng.below(3) as usize));
            }
            5 => session.replace_messages(vec![user_message(&format!("fresh-{step}"))]),
            6 => session.prune_orphans(tool_ids),
            8 => session.push_state(plugin_record(
                session.next_state_seq(),
                Some(Value::from(step)),
            )),
            9 => session.push_state(plugin_record(session.next_state_seq(), None)),
            10 => {
                let mut agent = session.agents()[&session.root_agent_id().unwrap()].clone();
                agent.updated_seq = session.next_state_seq();
                agent.label = Some(format!("agent-{step}"));
                session.push_agent_revision(agent.updated_seq, agent);
            }
            11 => session.push_branch(branch(session)),
            12 => {
                let mut records = session.branches().to_vec();
                records.pop();
                session.replace_branches(records);
            }
            13 => session.set_parent(Some(ParentLink {
                session: session.id,
                message_count: session.messages().len(),
                seq: session.next_state_seq().saturating_sub(1),
            })),
            14 => session.push_state(StoredStateRecord {
                seq: session.next_state_seq(),
                agent: session.root_agent_id(),
                generation: 0,
                change: StateChange::Context {
                    source: slot,
                    hash: format!("hash-{step}"),
                    content: format!("context-{step}"),
                },
            }),
            _ => {
                session.set_title(format!("title-{step}"));
                session.set_meta(SessionMeta {
                    input_draft: Some(format!("draft-{step}")),
                    ..session.meta.clone()
                });
            }
        }
    }

    /// Every mutation kind in random order, snapshots dropped here and there
    /// like the writer coalescing them, and the file clobbered now and then.
    /// Whatever the script, reloading must give back the live session.
    #[test]
    fn random_mutation_script_round_trips_through_the_log() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        let mut log = SessionLog::rewrite(dir, &session).unwrap();
        let mut rng = Rng(PROPERTY_SEED);
        session.resolve_legacy_mode(resolved_mode()).unwrap();

        for step in 0..PROPERTY_STEPS {
            mutate(&mut session, &mut rng, step);
            // Dropping a snapshot is what coalescing does, and the next write
            // must still land on a file that matches.
            if rng.below(3) == 0 {
                continue;
            }
            if rng.below(EXTERNAL_TRUNCATION) == 0 {
                let path = jsonl_path(dir, session.id);
                let len = std::fs::metadata(&path).unwrap().len();
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(len / 2)
                    .unwrap();
            }
            write_through(&mut log, dir, &session);
        }
        write_through(&mut log, dir, &session);

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
    }

    #[test]
    fn externally_truncated_log_is_rewritten_not_appended() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("hello"));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        let path = jsonl_path(dir, session.id);
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(0)
            .unwrap();

        session.push_message(assistant_message("reply"));
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged { .. }),
        ));
        drop(SessionLog::rewrite(dir, &session).unwrap());

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
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

    /// A mutator called with the value already there is not a change, and a
    /// truncate that cuts nothing must leave the epoch alone or every open
    /// cursor into the log dies for nothing.
    #[test]
    fn no_op_mutators_leave_the_session_and_its_cursors_alone() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut session: TestSession = Session::new("m", "/project");
        session.push_message(user_message("a"));
        session.push_message(assistant_message("b"));
        let mut log = SessionLog::rewrite(dir, &session).unwrap();
        let (revision, updated_at, epoch) = (session.revision(), session.updated_at, session.epoch);

        session.set_title(session.title.clone());
        session.set_meta(session.meta.clone());
        session.truncate_messages(session.messages().len());
        session.truncate_messages(session.messages().len() + 1);

        assert_eq!(session.revision(), revision);
        assert_eq!(session.updated_at, updated_at);
        assert_eq!(session.epoch, epoch);

        session.push_message(user_message("c"));
        log.append(&session).unwrap();
        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
    }

    /// Every frame checkpoints, so a checkpoint that only grew the message list
    /// must stay a small append instead of rewriting the whole file.
    #[test]
    fn successive_checkpoints_from_one_run_stay_appendable() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut produced = HistorySnapshot::new(vec![user_message("a")]);
        let mut session: Arc<TestSession> = Arc::new(Session::new("m", "/project"));
        let meta = session.meta.clone();
        Session::checkpoint(&mut session, Some(&produced), meta.clone(), Value::Null);

        let mut log = SessionLog::rewrite(dir, &session).unwrap();
        for step in 0..3 {
            Arc::make_mut(&mut produced.messages).push(assistant_message(&format!("reply-{step}")));
            Session::checkpoint(&mut session, Some(&produced), meta.clone(), Value::Null);
            log.append(&session).unwrap();
        }

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
    }

    /// The writer keeps the previous snapshot alive, so the UI ends up mutating
    /// a deep copy from `Arc::make_mut`. The copy inherits the run token, so the
    /// cursors the writer holds still describe it.
    #[test]
    fn append_cursor_survives_the_clone_arc_make_mut_hands_the_ui() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let mut base: TestSession = Session::new("m", "/project");
        base.push_message(user_message("a"));
        let mut log = SessionLog::rewrite(dir, &base).unwrap();

        let mut session = Arc::new(base);
        let held = Arc::clone(&session);
        let live = Arc::make_mut(&mut session);
        live.push_message(assistant_message("b"));
        live.insert_tool_output("t1".into(), Value::from("out"));
        live.set_subagent_messages("s1".into(), vec![user_message("sub")]);

        log.append(&session).unwrap();

        assert_eq!(held.messages().len(), 1, "the writer's snapshot is frozen");
        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
    }

    /// The corruption the epoch exists for. A rewind mints a new run, so a
    /// snapshot still in flight carries the pre-rewind messages: longer than
    /// the rewritten log, yet sharing only its head. Going by length alone
    /// would splice its tail on and leave disk holding `[a, d, c]` while the
    /// session holds `[a, b, c]`.
    #[test]
    fn stale_snapshot_after_a_rewind_is_rewritten_not_spliced() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let produced = HistorySnapshot::new(vec![
            user_message("a"),
            assistant_message("b"),
            user_message("c"),
        ]);
        let mut session: Arc<TestSession> = Arc::new(Session::new("m", "/project"));
        let meta = session.meta.clone();
        Session::checkpoint(&mut session, Some(&produced), meta.clone(), Value::Null);
        let mut log = SessionLog::rewrite(dir, &session).unwrap();

        let live = Arc::make_mut(&mut session);
        live.truncate_messages(1);
        live.push_message(user_message("d"));
        write_through(&mut log, dir, &session);

        Session::checkpoint(&mut session, Some(&produced), meta, Value::Null);
        let path = jsonl_path(dir, session.id);
        let size_before = fs::metadata(&path).unwrap().len();
        assert!(matches!(
            log.append(&session),
            Err(SessionError::LogDiverged { .. }),
        ));
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            size_before,
            "a refused append must not have written half of itself"
        );
        drop(SessionLog::rewrite(dir, &session).unwrap());

        let loaded = TestSession::load_from(session.id, dir).unwrap();
        assert_same_session(&loaded, &session);
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
