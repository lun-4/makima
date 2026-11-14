pub(crate) mod recording;
pub(crate) mod secure;

use self::secure::AnchoredPath;
use crate::{
    CancelToken, ModeRegistry, ToolOutput,
    journal::{JournalChange, SessionJournalSnapshot},
    plan_state,
    tools::{FileWriteLocks, file_locks::WriteLockGuard},
};
use makima_providers::{Message, TokenUsage};
use makima_storage::{
    StateDir, StorageError,
    blobs::{BlobError, BlobHash, BlobStore, FileState},
    id::{AgentId, MakimaId},
    restore::{
        PathConflict, PathRestore, RestoreChoice, RestoreEndpoint,
        RestoreError as RestoreStorageError, RestoreProgress, RestoreSnapshot, RestoreStore,
        RestoreTransition, TransitionRef,
    },
    restore_digest::session_digest,
    session_lock::{LockHeartbeat, SessionPublicationGuard, claim},
    session_state::{SessionStateView, StateChange},
    sessions::{SESSIONS_DIR, Session, SessionError},
};
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};
use thiserror::Error;

pub const SUPPORTED: bool = cfg!(any(target_os = "linux", target_os = "macos"));

const SHELL_KIND: &str = "execute";
const CANCELLED: &str = "file restore cancelled";
const CONTENT_CONFLICT: &str = "current content does not match the recorded post-image";
const DISCONTINUOUS: &str = "recorded file changes are not a continuous history";
const UNPUBLISHED: &str = "restore transition is not present in the durable target checkpoint";
const PUBLISHED: &str = "published restore cannot be rolled back";
const INCOMPLETE: &str = "restore transition still requires repair";

static RETAINED: Mutex<Vec<RestoreLease>> = Mutex::new(Vec::new());
static STARTUP_LEASES: Mutex<Vec<(MakimaId, StartupLeases)>> = Mutex::new(Vec::new());

type FileBytes = Option<Vec<u8>>;
type PathBytes = (FileBytes, FileBytes);
type ProjectionPaths = (BTreeMap<PathBuf, Vec<u8>>, BTreeSet<PathBuf>);

pub type StoredSession = Session<Message, TokenUsage, ToolOutput>;

pub struct RestoreSegment {
    pub session: StoredSession,
    pub after_seq: u64,
    pub through_seq: u64,
    pub affected: BTreeSet<AgentId>,
}

#[derive(Debug, Clone)]
pub struct PlannedFile {
    pub path: PathBuf,
    pub expected: FileState,
    pub desired: FileState,
    pub warning: Option<String>,
    pub inverse_of: Option<TransitionRef>,
}

#[derive(Debug, Clone, Default)]
pub struct RestorePreview {
    pub paths: Vec<PlannedFile>,
    pub shell_commands: usize,
}

pub struct PlanProjection {
    pub snapshot: SessionJournalSnapshot,
    pub affected: BTreeSet<AgentId>,
}

pub struct RestoreRequest {
    pub sessions_dir: PathBuf,
    pub owner: MakimaId,
    pub source: RestoreEndpoint,
    pub target: RestoreEndpoint,
    pub choice: RestoreChoice,
    pub preview: RestorePreview,
    pub projection: Option<PlanProjection>,
    pub locks: Arc<FileWriteLocks>,
    pub cancel: CancelToken,
}

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("secure file restoration is not supported on this platform")]
    Unsupported,
    #[error("invalid file restore: {0}")]
    Invalid(String),
    #[error("file restore I/O: {0}")]
    Io(#[from] io::Error),
    #[error("file restore storage: {0}")]
    Storage(#[from] RestoreStorageError),
    #[error("file restore blob: {0}")]
    Blob(#[from] BlobError),
    #[error("file restore session: {0}")]
    Session(#[from] SessionError),
    #[error("file restore digest: {0}")]
    Digest(#[from] StorageError),
    #[error("file restore conflict at {}: {reason}", path.display())]
    Conflict { path: PathBuf, reason: String },
    #[error("file restore unresolved: {reason}")]
    Unresolved {
        reason: String,
        receipt: Box<PreparedRestore>,
    },
}

pub struct AppliedRestore {
    pub changes: Vec<JournalChange>,
    pub warnings: Vec<String>,
}

#[must_use]
pub struct PreparedRestore {
    lease: Option<RestoreLease>,
}

struct RestoreLease {
    sessions_dir: PathBuf,
    store: RestoreStore,
    blobs: BlobStore,
    reference: TransitionRef,
    manifest: RestoreTransition,
    anchors: Vec<Option<AnchoredPath>>,
    projection: Option<PlanProjection>,
    paths: BTreeSet<PathBuf>,
    read_only: BTreeMap<PathBuf, Result<AnchoredPath, String>>,
    cancel: CancelToken,
    _guards: Vec<WriteLockGuard>,
    barriers: Vec<Box<dyn Any + Send>>,
}

impl fmt::Debug for PreparedRestore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedRestore")
            .field(
                "transition",
                &self.lease.as_ref().map(|lease| &lease.reference),
            )
            .finish_non_exhaustive()
    }
}

pub fn endpoint_digest(session: &StoredSession) -> Result<BlobHash, RestoreError> {
    Ok(session_digest(session)?)
}

pub fn plan(segments: &[RestoreSegment]) -> Result<RestorePreview, RestoreError> {
    validate_segments(segments)?;
    let mut paths: BTreeMap<PathBuf, PlannedFile> = BTreeMap::new();
    let mut shell_commands = 0;
    for segment in segments {
        for record in segment.session.state().iter().filter(|record| {
            record.seq > segment.after_seq
                && record.seq <= segment.through_seq
                && record
                    .agent
                    .is_some_and(|agent| segment.affected.contains(&agent))
        }) {
            match &record.change {
                StateChange::File {
                    path,
                    before,
                    after,
                } => {
                    validate_path(path)?;
                    match paths.get_mut(path) {
                        Some(planned) => {
                            if !same_state(&planned.expected, before) {
                                planned.warning = Some(DISCONTINUOUS.into());
                            }
                            planned.expected = after.clone();
                        }
                        None => {
                            paths.insert(
                                path.clone(),
                                PlannedFile {
                                    path: path.clone(),
                                    expected: after.clone(),
                                    desired: before.clone(),
                                    warning: None,
                                    inverse_of: None,
                                },
                            );
                        }
                    }
                }
                StateChange::ToolExecution { kind } if kind == SHELL_KIND => {
                    shell_commands += 1;
                }
                _ => {}
            }
        }
    }
    Ok(RestorePreview {
        paths: paths.into_values().collect(),
        shell_commands,
    })
}

fn validate_segments(segments: &[RestoreSegment]) -> Result<(), RestoreError> {
    let mut sessions = BTreeSet::new();
    for (index, segment) in segments.iter().enumerate() {
        let session = &segment.session;
        if !sessions.insert(session.id) {
            return Err(RestoreError::Invalid("duplicate ancestry session".into()));
        }
        let cwd = Path::new(&session.cwd);
        if !cwd.is_absolute()
            || cwd
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
        {
            return Err(RestoreError::Invalid(
                "session cwd must be absolute and normalized".into(),
            ));
        }
        let ceiling = session.sequence_high_water().unwrap_or_default();
        if segment.after_seq > segment.through_seq || segment.through_seq > ceiling {
            return Err(RestoreError::Invalid(
                "segment sequence range is unavailable".into(),
            ));
        }
        let view = session.latest_state()?;
        if segment.after_seq != 0
            && !session
                .state()
                .iter()
                .any(|record| record.seq == segment.after_seq)
            && !session
                .agent_revisions()
                .iter()
                .any(|(seq, _)| *seq == segment.after_seq)
        {
            return Err(RestoreError::Invalid(
                "segment starting sequence is no longer retained".into(),
            ));
        }
        if segment
            .affected
            .iter()
            .any(|agent| !view.agents.contains_key(agent))
        {
            return Err(RestoreError::Invalid(
                "segment names an unknown affected agent".into(),
            ));
        }
        if let Some(previous) = index.checked_sub(1).map(|index| &segments[index]) {
            let link = session
                .parent()
                .ok_or_else(|| RestoreError::Invalid("unrelated ancestry segment".into()))?;
            if link.session != previous.session.id
                || link.seq != previous.through_seq
                || link.message_count > previous.session.messages().len()
                || segment.after_seq != 0
                || cwd != Path::new(&previous.session.cwd)
            {
                return Err(RestoreError::Invalid(
                    "ancestry route does not match its parent link ceiling or cwd".into(),
                ));
            }
        }
    }
    Ok(())
}

pub fn plan_inverse(
    sessions_dir: &Path,
    reference: &TransitionRef,
) -> Result<RestorePreview, RestoreError> {
    if !SUPPORTED {
        return Err(RestoreError::Unsupported);
    }
    let store = RestoreStore::new(state_dir(sessions_dir)?);
    let snapshot = store.load(reference)?;
    if !snapshot.progress.contains(&RestoreProgress::Complete)
        || snapshot.progress.contains(&RestoreProgress::RollbackIntent)
    {
        return Err(RestoreError::Invalid(
            "only a completed forward transition can be inverted".into(),
        ));
    }
    if !published(sessions_dir, &snapshot.transition, reference)? {
        return Err(RestoreError::Invalid(UNPUBLISHED.into()));
    }
    let paths = snapshot
        .applied_paths()
        .map(|index| {
            let path = &snapshot.transition.paths[index];
            PlannedFile {
                path: path.path.clone(),
                expected: path.after.clone(),
                desired: path.before.clone(),
                warning: None,
                inverse_of: Some(reference.clone()),
            }
        })
        .collect();
    Ok(RestorePreview {
        paths,
        shell_commands: 0,
    })
}

pub async fn prepare(request: RestoreRequest) -> Result<PreparedRestore, RestoreError> {
    prepare_with(request, RestoreStore::prepare).await
}

async fn prepare_with(
    mut request: RestoreRequest,
    publish: impl FnOnce(
        &RestoreStore,
        MakimaId,
        RestoreTransition,
    ) -> Result<TransitionRef, RestoreStorageError>
    + Send
    + 'static,
) -> Result<PreparedRestore, RestoreError> {
    if !SUPPORTED {
        return Err(RestoreError::Unsupported);
    }
    match (&request.choice, &request.projection) {
        (RestoreChoice::FilesOnly, Some(_)) => {
            return Err(RestoreError::Invalid(
                "files-only restore cannot project a conversation".into(),
            ));
        }
        (RestoreChoice::ConversationOnly | RestoreChoice::ConversationAndFiles, None) => {
            return Err(RestoreError::Invalid(
                "conversation restore requires a plan projection".into(),
            ));
        }
        _ => {}
    }
    if request.choice == RestoreChoice::ConversationOnly {
        request.preview.paths.clear();
    }
    let state = state_dir(&request.sessions_dir)?;
    let store = RestoreStore::new(state.clone());
    let blobs = BlobStore::new(state);
    let mut planned = BTreeMap::new();
    let mut inverse_of = None;
    for path in std::mem::take(&mut request.preview.paths) {
        validate_path(&path.path)?;
        if let Some(reference) = &path.inverse_of {
            if inverse_of
                .as_ref()
                .is_some_and(|previous| previous != reference)
            {
                return Err(RestoreError::Invalid(
                    "preview mixes inverse transitions".into(),
                ));
            }
            inverse_of = Some(reference.clone());
        }
        if planned.insert(path.path.clone(), path).is_some() {
            return Err(RestoreError::Invalid("preview repeats a path".into()));
        }
    }
    let (request, planned, store, projected, protected, paths, keys) = smol::unblock(move || {
        validate_inverse(&store, &request.sessions_dir, &planned)?;
        let (projected, protected) = projection_paths(request.projection.as_ref())?;
        let paths: BTreeSet<_> = planned.keys().chain(protected.iter()).cloned().collect();
        let keys = lock_keys(&paths)?;
        Ok::<_, RestoreError>((request, planned, store, projected, protected, paths, keys))
    })
    .await?;
    let guards = request
        .locks
        .acquire_all(keys, &request.cancel)
        .await
        .map_err(RestoreError::Invalid)?;
    smol::unblock(move || {
        check_cancel(&request.cancel)?;
        validate_endpoint(&request.sessions_dir, &request.source)?;
        validate_endpoint(&request.sessions_dir, &request.target)?;
        let required = planned
            .iter()
            .filter(|(path, _)| !protected.contains(*path))
            .map(|(path, file)| {
                Ok((
                    path.clone(),
                    (
                        read_state(&blobs, &file.expected)?,
                        read_state(&blobs, &file.desired)?,
                    ),
                ))
            })
            .collect::<Result<BTreeMap<_, _>, RestoreError>>()?;
        let mut manifest = RestoreTransition {
            id: MakimaId::generate(),
            choice: request.choice,
            source: request.source,
            target: request.target,
            inverse_of,
            paths: Vec::new(),
            skipped: Vec::new(),
        };
        let mut anchors = Vec::new();
        let mut read_only = BTreeMap::new();
        for path in paths.iter().cloned() {
            check_cancel(&request.cancel)?;
            if protected.contains(&path) && !projected.contains_key(&path) {
                read_only.insert(
                    path.clone(),
                    AnchoredPath::capture(&path).map_err(|error| error.to_string()),
                );
                if planned.contains_key(&path) {
                    manifest.skipped.push(PathConflict {
                        path,
                        reason: "path is owned by the restored plan slot".into(),
                        actual: None,
                    });
                }
                continue;
            }
            let anchor = match AnchoredPath::capture(&path) {
                Ok(anchor) => anchor,
                Err(error) => {
                    manifest.skipped.push(PathConflict {
                        path,
                        reason: error.to_string(),
                        actual: None,
                    });
                    continue;
                }
            };
            let actual = match anchor.read() {
                Ok(actual) => actual,
                Err(error) => {
                    manifest.skipped.push(PathConflict {
                        path,
                        reason: error.to_string(),
                        actual: None,
                    });
                    continue;
                }
            };
            let before = save_state(&blobs, request.owner, actual.as_deref())?;
            let desired = if let Some(bytes) = projected.get(&path) {
                Ok(Some(bytes.clone()))
            } else {
                let file = &planned[&path];
                if let Some(warning) = &file.warning {
                    Err(warning.clone())
                } else {
                    let (expected, desired) = &required[&path];
                    if *expected == actual {
                        Ok(desired.clone())
                    } else {
                        Err(CONTENT_CONFLICT.into())
                    }
                }
            };
            match desired {
                Ok(desired) => {
                    let after = save_state(&blobs, request.owner, desired.as_deref())?;
                    manifest.paths.push(PathRestore {
                        path,
                        before,
                        after,
                    });
                    anchors.push(Some(anchor));
                }
                Err(reason) => manifest.skipped.push(PathConflict {
                    path,
                    reason,
                    actual: Some(before),
                }),
            }
        }
        check_cancel(&request.cancel)?;
        let (reference, preparation_error) = match publish(&store, request.owner, manifest.clone())
        {
            Ok(reference) => (reference, None),
            Err(RestoreStorageError::Published { reference, source }) => {
                (reference, Some(source.to_string()))
            }
            Err(error) => return Err(error.into()),
        };
        let receipt = PreparedRestore {
            lease: Some(RestoreLease {
                sessions_dir: request.sessions_dir,
                store,
                blobs,
                reference,
                manifest,
                anchors,
                projection: request.projection,
                paths,
                read_only,
                cancel: request.cancel,
                _guards: guards,
                barriers: Vec::new(),
            }),
        };
        match preparation_error {
            Some(reason) => Err(RestoreError::Unresolved {
                reason,
                receipt: Box::new(receipt),
            }),
            None => Ok(receipt),
        }
    })
    .await
}

impl PreparedRestore {
    pub fn transition_ref(&self) -> TransitionRef {
        self.inner().reference.clone()
    }

    pub fn paths(&self) -> BTreeSet<PathBuf> {
        self.inner().paths.clone()
    }

    pub fn retain_barrier(&mut self, guard: impl Any + Send) {
        self.inner_mut().barriers.push(Box::new(guard));
    }

    pub fn retained() -> Vec<TransitionRef> {
        RETAINED
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|lease| lease.reference.clone())
            .collect()
    }

    pub fn take_retained(reference: &TransitionRef) -> Option<Self> {
        let mut retained = RETAINED.lock().unwrap_or_else(|error| error.into_inner());
        let index = retained
            .iter()
            .position(|lease| &lease.reference == reference)?;
        Some(Self {
            lease: Some(retained.swap_remove(index)),
        })
    }

    pub fn apply(&mut self, registry: &ModeRegistry) -> Result<AppliedRestore, RestoreError> {
        let lease = self.inner_mut();
        if published(&lease.sessions_dir, &lease.manifest, &lease.reference)? {
            return Err(RestoreError::Invalid(PUBLISHED.into()));
        }
        validate_endpoint(&lease.sessions_dir, &lease.manifest.source)?;
        validate_endpoint(&lease.sessions_dir, &lease.manifest.target)?;
        lease.forward(true)?;
        let mut warnings: Vec<_> = lease
            .manifest
            .skipped
            .iter()
            .map(|path| format!("{}: {}", path.path.display(), path.reason))
            .collect();
        let mut changes = Vec::new();
        if let Some(projection) = &lease.projection {
            let result = plan_state::prepare_rewind_projection_with(
                &projection.snapshot,
                &projection.affected,
                registry,
                |view, agent, entry, mode| {
                    let path = plan_state::validated_plan_path(view, agent, entry, Some(mode))?;
                    let Some(approved) = &entry.approved else {
                        if entry.written {
                            let anchor = lease
                                .read_only
                                .get(&path)
                                .ok_or_else(|| "written plan path was not captured".to_owned())?
                                .as_ref()
                                .map_err(Clone::clone)?;
                            if anchor.read().map_err(|error| error.to_string())?.is_none() {
                                return Err("written plan file is missing".into());
                            }
                        }
                        return Ok(());
                    };
                    if plan_state::content_sha256(&approved.content) != approved.sha256 {
                        return Err("approved plan content hash does not match".into());
                    }
                    let Some(index) = lease
                        .manifest
                        .paths
                        .iter()
                        .position(|file| file.path == path)
                    else {
                        return Err(lease
                            .manifest
                            .skipped
                            .iter()
                            .find(|file| file.path == path)
                            .map_or_else(
                                || "plan file was not staged".into(),
                                |file| file.reason.clone(),
                            ));
                    };
                    let actual = lease.anchors[index]
                        .as_ref()
                        .ok_or_else(|| "plan file anchor unavailable".to_owned())?
                        .read()
                        .map_err(|error| error.to_string())?;
                    if actual.as_deref() != Some(approved.content.as_bytes()) {
                        return Err("plan file does not match the approved content".into());
                    }
                    Ok(())
                },
            )
            .map_err(RestoreError::Invalid)?;
            changes = result.0;
            warnings.extend(result.1);
        }
        Ok(AppliedRestore { changes, warnings })
    }

    pub fn abort(mut self) -> Result<(), RestoreError> {
        let result = (|| {
            let lease = self.inner_mut();
            if published(&lease.sessions_dir, &lease.manifest, &lease.reference)? {
                return Err(RestoreError::Invalid(PUBLISHED.into()));
            }
            validate_endpoint(&lease.sessions_dir, &lease.manifest.source)?;
            validate_endpoint(&lease.sessions_dir, &lease.manifest.target)?;
            lease.rollback()?;
            Ok(())
        })();
        self.finish(result)
    }

    pub fn complete(mut self) -> Result<(), RestoreError> {
        let result = (|| {
            let lease = self.inner_mut();
            if !published(&lease.sessions_dir, &lease.manifest, &lease.reference)? {
                return Err(RestoreError::Invalid(UNPUBLISHED.into()));
            }
            lease.forward(false)?;
            lease
                .store
                .append(&lease.reference, RestoreProgress::Complete)?;
            Ok(())
        })();
        self.finish(result)
    }

    fn finish(mut self, result: Result<(), RestoreError>) -> Result<(), RestoreError> {
        match result {
            Ok(()) => {
                self.lease.take();
                Ok(())
            }
            Err(error) => Err(RestoreError::Unresolved {
                reason: error.to_string(),
                receipt: Box::new(self),
            }),
        }
    }

    fn inner(&self) -> &RestoreLease {
        self.lease.as_ref().expect("restore receipt is owned")
    }

    fn inner_mut(&mut self) -> &mut RestoreLease {
        self.lease.as_mut().expect("restore receipt is owned")
    }
}

impl Drop for PreparedRestore {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            tracing::error!(session = %lease.reference.session, transition = %lease.reference.transition,
                "unresolved file restore retained with write locks and transition barriers");
            RETAINED
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(lease);
        }
    }
}

impl RestoreLease {
    fn snapshot(&self) -> Result<RestoreSnapshot, RestoreError> {
        Ok(self.store.load(&self.reference)?)
    }

    fn adopt_created_directories(&self, index: usize) -> Result<(), RestoreError> {
        let anchor = self.anchors[index]
            .as_ref()
            .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?;
        for (other_index, other) in self.anchors.iter().enumerate() {
            if index != other_index
                && let Some(other) = other
            {
                anchor.adopt_created_from(other)?;
            }
        }
        Ok(())
    }

    fn preflight(&mut self) -> Result<Vec<PathBytes>, RestoreError> {
        let mut bytes = Vec::with_capacity(self.manifest.paths.len());
        for (index, path) in self.manifest.paths.iter().enumerate() {
            bytes.push((
                read_state(&self.blobs, &path.before)?,
                read_state(&self.blobs, &path.after)?,
            ));
            if self.anchors[index].is_none() {
                self.anchors[index] = Some(AnchoredPath::capture(&path.path)?);
            }
        }
        Ok(bytes)
    }

    fn forward(&mut self, cancellable: bool) -> Result<(), RestoreError> {
        let snapshot = self.snapshot()?;
        if snapshot.progress.contains(&RestoreProgress::RollbackIntent) {
            return Err(RestoreError::Invalid(
                "forward repair contradicts durable rollback intent".into(),
            ));
        }
        if snapshot.progress.contains(&RestoreProgress::Complete) {
            return Ok(());
        }
        let bytes = self.preflight()?;
        for (index, (before, after)) in bytes.iter().enumerate() {
            if cancellable {
                check_cancel(&self.cancel)?;
            }
            self.adopt_created_directories(index)?;
            let anchor = self.anchors[index]
                .as_ref()
                .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?;
            let actual = anchor.read()?;
            let applied = matches!(
                path_outcome(&snapshot, index),
                Some(RestoreProgress::Applied { .. })
            );
            if actual != *before && actual != *after {
                let actual = save_state(&self.blobs, self.reference.session, actual.as_deref())?;
                self.store.append(
                    &self.reference,
                    RestoreProgress::Conflict {
                        path: index,
                        actual,
                    },
                )?;
                return Err(RestoreError::Conflict {
                    path: self.manifest.paths[index].path.clone(),
                    reason: CONTENT_CONFLICT.into(),
                });
            }
            anchor.replace(actual.as_deref(), after.as_deref())?;
            if !applied {
                self.store
                    .append(&self.reference, RestoreProgress::Applied { path: index })?;
            }
        }
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), RestoreError> {
        let mut snapshot = self.snapshot()?;
        if snapshot.progress.contains(&RestoreProgress::Complete) {
            return if snapshot.progress.contains(&RestoreProgress::RollbackIntent) {
                self.store
                    .append(&self.reference, RestoreProgress::Complete)?;
                Ok(())
            } else {
                Err(RestoreError::Invalid(
                    "completed forward transition cannot be aborted".into(),
                ))
            };
        }
        let bytes = self.preflight()?;
        if !snapshot.progress.contains(&RestoreProgress::RollbackIntent) {
            for (index, (before, after)) in bytes.iter().enumerate() {
                self.adopt_created_directories(index)?;
                let anchor = self.anchors[index]
                    .as_ref()
                    .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?;
                let actual = anchor.read()?;
                if actual == *after && before != after {
                    self.store
                        .append(&self.reference, RestoreProgress::Applied { path: index })?;
                } else if actual != *before {
                    let actual =
                        save_state(&self.blobs, self.reference.session, actual.as_deref())?;
                    self.store.append(
                        &self.reference,
                        RestoreProgress::Conflict {
                            path: index,
                            actual,
                        },
                    )?;
                }
            }
            self.store
                .append(&self.reference, RestoreProgress::RollbackIntent)?;
            snapshot = self.snapshot()?;
        }
        for (index, (before, after)) in bytes.iter().enumerate().rev() {
            self.adopt_created_directories(index)?;
            let anchor = self.anchors[index]
                .as_ref()
                .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?;
            let actual = anchor.read()?;
            if actual != *before && actual != *after {
                let actual = save_state(&self.blobs, self.reference.session, actual.as_deref())?;
                self.store.append(
                    &self.reference,
                    RestoreProgress::Conflict {
                        path: index,
                        actual,
                    },
                )?;
                return Err(RestoreError::Conflict {
                    path: self.manifest.paths[index].path.clone(),
                    reason: CONTENT_CONFLICT.into(),
                });
            }
            anchor.replace(actual.as_deref(), before.as_deref())?;
            if path_outcome(&snapshot, index).is_some() {
                self.store
                    .append(&self.reference, RestoreProgress::RolledBack { path: index })?;
            }
        }
        self.store
            .append(&self.reference, RestoreProgress::Complete)?;
        Ok(())
    }
}

fn path_outcome(snapshot: &RestoreSnapshot, index: usize) -> Option<&RestoreProgress> {
    snapshot.progress.iter().rev().find(|event| {
        matches!(event,
        RestoreProgress::Applied { path }
        | RestoreProgress::Conflict { path, .. }
        | RestoreProgress::RolledBack { path } if *path == index)
    })
}

#[derive(Clone)]
pub struct OwnedRestoreLease {
    pub guard: SessionPublicationGuard,
    pub keepalive: Arc<dyn Any + Send + Sync>,
}

type StartupLeases = BTreeMap<MakimaId, OwnedRestoreLease>;

struct StartupRepair {
    session: MakimaId,
    leases: Option<StartupLeases>,
}

impl Drop for StartupRepair {
    fn drop(&mut self) {
        if let Some(leases) = self.leases.take() {
            STARTUP_LEASES
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push((self.session, leases));
        }
    }
}

pub async fn reconcile_session_guarded(
    sessions_dir: &Path,
    session: MakimaId,
    supplied: BTreeMap<MakimaId, OwnedRestoreLease>,
    locks: Arc<FileWriteLocks>,
    cancel: CancelToken,
) -> Result<(), RestoreError> {
    let sessions_dir = sessions_dir.to_path_buf();
    let store = RestoreStore::new(state_dir(&sessions_dir)?);
    let mut leases = {
        let mut retained = STARTUP_LEASES
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        retained
            .iter()
            .position(|(id, _)| *id == session)
            .map(|index| retained.swap_remove(index).1)
            .unwrap_or_default()
    };
    leases.extend(supplied);
    let repair = StartupRepair {
        session,
        leases: Some(leases),
    };
    let acquire_dir = sessions_dir.clone();
    let (mut repair, pending) = smol::unblock(move || {
        let mut repair = repair;
        let pending = store.pending_for_session(session)?;
        let mut endpoints = BTreeSet::from([session]);
        for reference in &pending {
            let snapshot = store.load(reference)?;
            endpoints.extend([
                reference.session,
                snapshot.transition.source.session,
                snapshot.transition.target.session,
            ]);
        }
        let leases = repair
            .leases
            .as_mut()
            .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?;
        leases.retain(|id, _| endpoints.contains(id));
        if !pending.is_empty() {
            for endpoint in endpoints {
                if let Some(lease) = leases.get(&endpoint) {
                    if lease.guard.publish(|| ())?.is_some() {
                        continue;
                    }
                    leases.remove(&endpoint);
                }
                let lease = claim(&acquire_dir, &endpoint)?.ok_or_else(|| {
                    RestoreError::Invalid(format!(
                        "restore endpoint session {endpoint} is open elsewhere"
                    ))
                })?;
                let heartbeat = Arc::new(LockHeartbeat::spawn(lease, || {}));
                leases.insert(
                    endpoint,
                    OwnedRestoreLease {
                        guard: heartbeat.publication_guard(),
                        keepalive: heartbeat,
                    },
                );
            }
        }
        Ok::<_, RestoreError>((repair, pending))
    })
    .await?;
    for reference in pending {
        check_cancel(&cancel)?;
        validate_startup_leases(
            repair
                .leases
                .as_ref()
                .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?,
        )?;
        reconcile(
            &sessions_dir,
            &reference,
            Arc::clone(&locks),
            cancel.clone(),
        )
        .await?;
    }
    validate_startup_leases(
        repair
            .leases
            .as_ref()
            .ok_or_else(|| RestoreError::Invalid(INCOMPLETE.into()))?,
    )?;
    drop(repair.leases.take());
    Ok(())
}

fn validate_startup_leases(leases: &StartupLeases) -> Result<(), RestoreError> {
    for lease in leases.values() {
        if lease.guard.publish(|| ())?.is_none() {
            return Err(RestoreError::Invalid(
                "restore endpoint session lease was lost".into(),
            ));
        }
    }
    Ok(())
}

pub async fn reconcile_session(
    sessions_dir: &Path,
    session: MakimaId,
    locks: Arc<FileWriteLocks>,
    cancel: CancelToken,
) -> Result<(), RestoreError> {
    let store = RestoreStore::new(state_dir(sessions_dir)?);
    let pending = smol::unblock(move || store.pending_for_session(session)).await?;
    for reference in pending {
        reconcile(sessions_dir, &reference, Arc::clone(&locks), cancel.clone()).await?;
    }
    Ok(())
}

pub async fn reconcile(
    sessions_dir: &Path,
    reference: &TransitionRef,
    locks: Arc<FileWriteLocks>,
    cancel: CancelToken,
) -> Result<(), RestoreError> {
    if let Some(receipt) = PreparedRestore::take_retained(reference) {
        return smol::unblock(move || reconcile_receipt(receipt)).await;
    }
    let sessions_dir = sessions_dir.to_path_buf();
    let reference = reference.clone();
    let state = state_dir(&sessions_dir)?;
    let store = RestoreStore::new(state.clone());
    let (store, snapshot, keys) = {
        let reference = reference.clone();
        smol::unblock(move || {
            let snapshot = store.load(&reference)?;
            let paths = snapshot
                .transition
                .paths
                .iter()
                .map(|file| file.path.clone())
                .chain(
                    snapshot
                        .transition
                        .skipped
                        .iter()
                        .map(|file| file.path.clone()),
                )
                .collect();
            let keys = lock_keys(&paths)?;
            Ok::<_, RestoreError>((store, snapshot, keys))
        })
        .await?
    };
    let guards = locks
        .acquire_all(keys, &cancel)
        .await
        .map_err(RestoreError::Invalid)?;
    let receipt = PreparedRestore {
        lease: Some(RestoreLease {
            sessions_dir: sessions_dir.to_path_buf(),
            store,
            blobs: BlobStore::new(state),
            reference: reference.clone(),
            anchors: (0..snapshot.transition.paths.len()).map(|_| None).collect(),
            paths: snapshot
                .transition
                .paths
                .iter()
                .map(|path| path.path.clone())
                .chain(
                    snapshot
                        .transition
                        .skipped
                        .iter()
                        .map(|path| path.path.clone()),
                )
                .collect(),
            read_only: BTreeMap::new(),
            manifest: snapshot.transition,
            projection: None,
            cancel,
            _guards: guards,
            barriers: Vec::new(),
        }),
    };
    smol::unblock(move || reconcile_receipt(receipt)).await
}

fn reconcile_receipt(receipt: PreparedRestore) -> Result<(), RestoreError> {
    match published(
        &receipt.inner().sessions_dir,
        &receipt.inner().manifest,
        &receipt.inner().reference,
    ) {
        Ok(true) => receipt.complete(),
        Ok(false) => receipt.abort(),
        Err(error) => Err(RestoreError::Unresolved {
            reason: error.to_string(),
            receipt: Box::new(receipt),
        }),
    }
}

fn projection_paths(projection: Option<&PlanProjection>) -> Result<ProjectionPaths, RestoreError> {
    let Some(projection) = projection else {
        return Ok((BTreeMap::new(), BTreeSet::new()));
    };
    let files = plan_state::projection_files(&projection.snapshot, &projection.affected)
        .map_err(RestoreError::Invalid)?;
    let view = SessionStateView::fold(
        &projection.snapshot.state,
        &projection.snapshot.agent_revisions,
        projection.snapshot.seq,
    )
    .map_err(|error| RestoreError::Invalid(error.to_string()))?;
    let entries = plan_state::plan_entries(&view).map_err(RestoreError::Invalid)?;
    let mut paths: BTreeSet<_> = files.iter().map(|(path, _)| path.clone()).collect();
    for agent in &projection.affected {
        if let Some(entry) = entries.get(agent) {
            let path = plan_state::resolved_plan_path(&view, *agent, &entry.path)
                .map_err(RestoreError::Invalid)?;
            validate_path(&path)?;
            paths.insert(path);
        }
    }
    Ok((files.into_iter().collect(), paths))
}

fn validate_inverse(
    store: &RestoreStore,
    sessions_dir: &Path,
    planned: &BTreeMap<PathBuf, PlannedFile>,
) -> Result<(), RestoreError> {
    let Some(reference) = planned.values().find_map(|file| file.inverse_of.as_ref()) else {
        return Ok(());
    };
    let snapshot = store.load(reference)?;
    if !snapshot.progress.contains(&RestoreProgress::Complete)
        || snapshot.progress.contains(&RestoreProgress::RollbackIntent)
        || !published(sessions_dir, &snapshot.transition, reference)?
    {
        return Err(RestoreError::Invalid(
            "inverse transition is not completed and published".into(),
        ));
    }
    let applied: BTreeMap<_, _> = snapshot
        .applied_paths()
        .map(|index| {
            let path = &snapshot.transition.paths[index];
            (&path.path, path)
        })
        .collect();
    for file in planned.values() {
        let original = applied.get(&file.path).ok_or_else(|| {
            RestoreError::Invalid("inverse names a path that was not applied".into())
        })?;
        if file.inverse_of.as_ref() != Some(reference)
            || !same_state(&file.expected, &original.after)
            || !same_state(&file.desired, &original.before)
        {
            return Err(RestoreError::Invalid(
                "inverse path does not match its applied transition".into(),
            ));
        }
    }
    Ok(())
}

fn validate_endpoint(sessions_dir: &Path, endpoint: &RestoreEndpoint) -> Result<(), RestoreError> {
    let current = StoredSession::load_from(endpoint.session, sessions_dir)?;
    let checkpoint = if let Some(branch) = endpoint.branch {
        current.load_branch_from(branch, sessions_dir)?
    } else {
        current
    };
    if checkpoint.sequence_high_water().unwrap_or_default() != endpoint.seq
        || endpoint_digest(&checkpoint)? != endpoint.digest
    {
        return Err(RestoreError::Invalid(
            "restore endpoint no longer matches its checkpoint".into(),
        ));
    }
    Ok(())
}

fn published(
    sessions_dir: &Path,
    manifest: &RestoreTransition,
    reference: &TransitionRef,
) -> Result<bool, RestoreError> {
    let checkpoint = StoredSession::load_from(manifest.target.session, sessions_dir)?;
    Ok(checkpoint.restore_refs().contains(reference))
}

fn state_dir(sessions_dir: &Path) -> Result<StateDir, RestoreError> {
    if sessions_dir
        .file_name()
        .is_none_or(|name| name != SESSIONS_DIR)
    {
        return Err(RestoreError::Invalid(
            "sessions_dir must name the sessions directory".into(),
        ));
    }
    let parent = sessions_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .ok_or_else(|| RestoreError::Invalid("sessions_dir has no state directory".into()))?;
    Ok(StateDir::from_path(parent.to_path_buf()))
}

fn validate_path(path: &Path) -> Result<(), RestoreError> {
    let normalized: PathBuf = path.components().collect();
    if !path.is_absolute()
        || path.file_name().is_none()
        || path.as_os_str() != normalized.as_os_str()
        || path
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
    {
        return Err(RestoreError::Invalid(format!(
            "path must be absolute and normalized: {}",
            path.display()
        )));
    }
    Ok(())
}

fn lock_keys(paths: &BTreeSet<PathBuf>) -> Result<BTreeSet<PathBuf>, RestoreError> {
    paths
        .iter()
        .map(|path| {
            validate_path(path)?;
            let path = path.to_str().ok_or_else(|| {
                RestoreError::Invalid("non-UTF-8 restore paths cannot share tool locks".into())
            })?;
            FileWriteLocks::lock_key(path, Path::new("/")).map_err(RestoreError::Invalid)
        })
        .collect()
}

fn read_state(blobs: &BlobStore, state: &FileState) -> Result<Option<Vec<u8>>, RestoreError> {
    match state {
        FileState::Absent => Ok(None),
        FileState::Blob(reference) => Ok(Some(blobs.read(reference)?)),
    }
}

fn save_state(
    blobs: &BlobStore,
    owner: MakimaId,
    bytes: Option<&[u8]>,
) -> Result<FileState, RestoreError> {
    match bytes {
        None => Ok(FileState::Absent),
        Some(bytes) => Ok(FileState::Blob(blobs.put(owner, bytes)?)),
    }
}

fn same_state(left: &FileState, right: &FileState) -> bool {
    match (left, right) {
        (FileState::Absent, FileState::Absent) => true,
        (FileState::Blob(left), FileState::Blob(right)) => left.hash == right.hash,
        _ => false,
    }
}

fn check_cancel(cancel: &CancelToken) -> Result<(), RestoreError> {
    if cancel.is_cancelled() {
        Err(RestoreError::Invalid(CANCELLED.into()))
    } else {
        Ok(())
    }
}

#[cfg(all(test, not(any(target_os = "linux", target_os = "macos"))))]
mod unsupported_tests {
    use super::{RestoreError, RestoreRequest, prepare};
    use crate::CancelToken;
    use crate::tools::FileWriteLocks;
    use makima_storage::{
        blobs::BlobHash,
        id::MakimaId,
        restore::{RestoreChoice, RestoreEndpoint},
        sessions::SESSIONS_DIR,
    };
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(RestoreChoice::FilesOnly; "files_only")]
    #[test_case(RestoreChoice::ConversationAndFiles; "conversation_and_files")]
    #[test_case(RestoreChoice::ConversationOnly; "legacy_conversation_path_required")]
    fn secure_restore_rejects_unsupported_platform_before_storage_effects(choice: RestoreChoice) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let sessions_dir = directory.path().join(SESSIONS_DIR);
            let owner = MakimaId::generate();
            let endpoint = RestoreEndpoint {
                session: owner,
                branch: None,
                seq: 0,
                digest: BlobHash::digest(&[]),
            };
            let error = prepare(RestoreRequest {
                sessions_dir: sessions_dir.clone(),
                owner,
                source: endpoint.clone(),
                target: endpoint,
                choice,
                preview: Default::default(),
                projection: None,
                locks: Arc::new(FileWriteLocks::new()),
                cancel: CancelToken::none(),
            })
            .await
            .unwrap_err();
            assert!(matches!(error, RestoreError::Unsupported));
            assert!(!sessions_dir.exists());
        });
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::{
        CONTENT_CONFLICT, DISCONTINUOUS, OwnedRestoreLease, PlanProjection, PlannedFile,
        PreparedRestore, RestorePreview, RestoreRequest, RestoreSegment, SHELL_KIND, StoredSession,
        endpoint_digest, plan, plan_inverse, prepare, prepare_with, reconcile,
        reconcile_session_guarded, save_state, state_dir,
    };
    use crate::journal::SessionJournal;
    use crate::tools::FileWriteLocks;
    use crate::{CancelToken, ModeRegistry};
    use makima_storage::{
        blobs::{BlobHash, BlobRef, BlobStore, FileState},
        id::AgentId,
        plans::{ApprovedPlan, PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry},
        restore::{
            RestoreChoice, RestoreEndpoint, RestoreError as RestoreStorageError, RestoreProgress,
            RestoreStore,
        },
        session_lock::{LockHeartbeat, claim},
        session_state::{
            ParentLink, StateChange, StoredAgentConfig, StoredModeSnapshot, StoredStateRecord,
        },
    };
    use std::{collections::BTreeMap, fs, io, path::PathBuf, sync::Arc};
    use tempfile::TempDir;
    use test_case::test_case;

    const MODEL: &str = "restore-test";
    const BEFORE: &[u8] = b"before";
    const AFTER: &[u8] = b"after";
    const EXTERNAL: &[u8] = b"external";
    const FILE: &str = "file.txt";
    const SESSION_DIRECTORY: &str = "sessions";
    const PREPARE_FINISH_ERROR: &str = "manifest published before preparation confirmation failed";

    struct Fixture {
        _directory: TempDir,
        sessions_dir: PathBuf,
        session: StoredSession,
        path: PathBuf,
        blobs: BlobStore,
        locks: Arc<FileWriteLocks>,
    }

    impl Fixture {
        fn new() -> Self {
            let directory = TempDir::new().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let sessions_dir = root.join(SESSION_DIRECTORY);
            fs::create_dir(&sessions_dir).unwrap();
            let mut session = StoredSession::new(MODEL, root.to_str().unwrap());
            session.bind_root_id(AgentId::generate()).unwrap();
            session.save_to(&sessions_dir).unwrap();
            let blobs = BlobStore::new(state_dir(&sessions_dir).unwrap());
            Self {
                _directory: directory,
                sessions_dir,
                session,
                path: root.join(FILE),
                blobs,
                locks: Arc::new(FileWriteLocks::new()),
            }
        }

        fn state(&self, bytes: Option<&[u8]>) -> FileState {
            save_state(&self.blobs, self.session.id, bytes).unwrap()
        }

        fn endpoint(&self) -> RestoreEndpoint {
            RestoreEndpoint {
                session: self.session.id,
                branch: None,
                seq: self.session.sequence_high_water().unwrap_or_default(),
                digest: endpoint_digest(&self.session).unwrap(),
            }
        }

        fn request(&self, expected: Option<&[u8]>, desired: Option<&[u8]>) -> RestoreRequest {
            RestoreRequest {
                sessions_dir: self.sessions_dir.clone(),
                owner: self.session.id,
                source: self.endpoint(),
                target: self.endpoint(),
                choice: RestoreChoice::FilesOnly,
                preview: RestorePreview {
                    paths: vec![PlannedFile {
                        path: self.path.clone(),
                        expected: self.state(expected),
                        desired: self.state(desired),
                        warning: None,
                        inverse_of: None,
                    }],
                    shell_commands: 0,
                },
                projection: None,
                locks: self.locks.clone(),
                cancel: CancelToken::none(),
            }
        }

        fn store(&self) -> RestoreStore {
            RestoreStore::new(state_dir(&self.sessions_dir).unwrap())
        }

        fn publish(&mut self, receipt: &PreparedRestore) {
            self.session.push_restore_ref(receipt.transition_ref());
            self.session.save_to(&self.sessions_dir).unwrap();
        }

        fn change(&mut self, change: StateChange) {
            self.session.push_state(StoredStateRecord {
                seq: self.session.next_state_seq(),
                agent: self.session.root_agent_id(),
                generation: 0,
                change,
            });
        }
    }

    #[test_case(false; "continuous")]
    #[test_case(true; "discontinuous")]
    fn planner_uses_earliest_preimage_and_last_postimage(discontinuous: bool) {
        let mut fixture = Fixture::new();
        let first = fixture.state(Some(BEFORE));
        let middle = fixture.state(Some(AFTER));
        let last = fixture.state(Some(EXTERNAL));
        let start = fixture.session.sequence_high_water().unwrap();
        fixture.change(StateChange::File {
            path: fixture.path.clone(),
            before: first.clone(),
            after: middle.clone(),
        });
        fixture.change(StateChange::ToolExecution {
            kind: SHELL_KIND.into(),
        });
        fixture.change(StateChange::File {
            path: fixture.path.clone(),
            before: if discontinuous {
                FileState::Absent
            } else {
                middle
            },
            after: last.clone(),
        });
        let preview = plan(&[RestoreSegment {
            after_seq: start,
            through_seq: fixture.session.sequence_high_water().unwrap(),
            affected: [fixture.session.root_agent_id().unwrap()].into(),
            session: fixture.session,
        }])
        .unwrap();
        assert_eq!(preview.shell_commands, 1);
        assert_eq!(preview.paths.len(), 1);
        assert_eq!(preview.paths[0].desired, first);
        assert_eq!(preview.paths[0].expected, last);
        assert_eq!(
            preview.paths[0].warning.as_deref(),
            discontinuous.then_some(DISCONTINUOUS)
        );
    }

    #[test_case(false; "existing_file")]
    #[test_case(true; "created_file")]
    fn restore_and_applied_only_inverse_round_trip(created: bool) {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let desired = (!created).then_some(BEFORE);
            let mut receipt = prepare(fixture.request(Some(AFTER), desired))
                .await
                .unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            assert_eq!(fs::read(&fixture.path).ok().as_deref(), desired);
            fixture.publish(&receipt);
            let reference = receipt.transition_ref();
            receipt.complete().unwrap();
            let mut request = fixture.request(desired, Some(AFTER));
            request.preview = plan_inverse(&fixture.sessions_dir, &reference).unwrap();
            let mut inverse = prepare(request).await.unwrap();
            inverse.apply(&ModeRegistry::builtin()).unwrap();
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            fixture.publish(&inverse);
            inverse.complete().unwrap();
        });
    }

    #[test_case(false; "missing_expected")]
    #[test_case(true; "missing_desired")]
    fn unavailable_required_blob_fails_before_any_write(desired: bool) {
        smol::block_on(async {
            let fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut request = fixture.request(Some(AFTER), Some(BEFORE));
            let missing = FileState::Blob(BlobRef {
                session: fixture.session.id,
                hash: BlobHash::digest(EXTERNAL),
            });
            if desired {
                request.preview.paths[0].desired = missing;
            } else {
                request.preview.paths[0].expected = missing;
            }
            assert!(prepare(request).await.is_err());
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            assert!(
                fixture
                    .store()
                    .pending(fixture.session.id)
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn conversation_only_projects_plans_without_restoring_generic_files() {
        smol::block_on(async {
            let fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut request = fixture.request(Some(AFTER), Some(BEFORE));
            request.choice = RestoreChoice::ConversationOnly;
            request.projection = Some(PlanProjection {
                snapshot: SessionJournal::from_session(&fixture.session)
                    .unwrap()
                    .snapshot(),
                affected: [fixture.session.root_agent_id().unwrap()].into(),
            });
            let mut receipt = prepare(request).await.unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            assert!(receipt.inner().manifest.paths.is_empty());
            assert_eq!(
                receipt.inner().manifest.choice,
                RestoreChoice::ConversationOnly
            );
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            receipt.abort().unwrap();
        });
    }

    #[test]
    fn preflight_conflict_is_skipped_and_excluded_from_redo() {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, EXTERNAL).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            let applied = receipt.apply(&ModeRegistry::builtin()).unwrap();
            assert_eq!(applied.warnings.len(), 1);
            let reference = receipt.transition_ref();
            assert_eq!(
                fixture.store().load(&reference).unwrap().transition.skipped[0].reason,
                CONTENT_CONFLICT
            );
            fixture.publish(&receipt);
            receipt.complete().unwrap();
            assert!(
                plan_inverse(&fixture.sessions_dir, &reference)
                    .unwrap()
                    .paths
                    .is_empty()
            );
            assert_eq!(fs::read(&fixture.path).unwrap(), EXTERNAL);
        });
    }

    #[test_case(false; "unpublished_rolls_back")]
    #[test_case(true; "published_finishes_forward")]
    fn recovery_uses_checkpoint_reference_after_write_before_applied(publish: bool) {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            let reference = receipt.transition_ref();
            receipt.inner().anchors[0]
                .as_ref()
                .unwrap()
                .replace(Some(AFTER), Some(BEFORE))
                .unwrap();
            if publish {
                fixture.publish(&receipt);
            }
            let mut receipt = receipt;
            drop(receipt.lease.take());
            reconcile(
                &fixture.sessions_dir,
                &reference,
                fixture.locks.clone(),
                CancelToken::none(),
            )
            .await
            .unwrap();
            assert_eq!(
                fs::read(&fixture.path).unwrap(),
                if publish { BEFORE } else { AFTER }
            );
            let snapshot = fixture.store().load(&reference).unwrap();
            assert!(snapshot.progress.contains(&RestoreProgress::Complete));
            assert_eq!(
                snapshot.progress.contains(&RestoreProgress::RollbackIntent),
                !publish
            );
        });
    }

    #[test]
    fn startup_repair_requires_both_endpoint_session_leases() {
        smol::block_on(async {
            let fixture = Fixture::new();
            let mut source = StoredSession::new(MODEL, &fixture.session.cwd);
            source.bind_root_id(AgentId::generate()).unwrap();
            source.save_to(&fixture.sessions_dir).unwrap();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut request = fixture.request(Some(AFTER), Some(BEFORE));
            request.source = RestoreEndpoint {
                session: source.id,
                branch: None,
                seq: source.sequence_high_water().unwrap_or_default(),
                digest: endpoint_digest(&source).unwrap(),
            };
            let mut receipt = prepare(request).await.unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            drop(receipt.lease.take());
            let current = Arc::new(LockHeartbeat::spawn(
                claim(&fixture.sessions_dir, &fixture.session.id)
                    .unwrap()
                    .unwrap(),
                || {},
            ));
            let supplied = BTreeMap::from([(
                fixture.session.id,
                OwnedRestoreLease {
                    guard: current.publication_guard(),
                    keepalive: current.clone(),
                },
            )]);
            let source_lease = claim(&fixture.sessions_dir, &source.id).unwrap().unwrap();
            assert!(
                reconcile_session_guarded(
                    &fixture.sessions_dir,
                    fixture.session.id,
                    supplied.clone(),
                    fixture.locks.clone(),
                    CancelToken::none(),
                )
                .await
                .is_err()
            );
            assert_eq!(fs::read(&fixture.path).unwrap(), BEFORE);
            drop(source_lease);
            reconcile_session_guarded(
                &fixture.sessions_dir,
                fixture.session.id,
                supplied.clone(),
                fixture.locks.clone(),
                CancelToken::none(),
            )
            .await
            .unwrap();
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
        });
    }

    #[test_case(false; "before_rollback_write")]
    #[test_case(true; "after_rollback_write_before_progress")]
    fn recovery_resumes_durable_rollback_intent(already_reverted: bool) {
        smol::block_on(async {
            let fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            let reference = receipt.transition_ref();
            fixture
                .store()
                .append(&reference, RestoreProgress::RollbackIntent)
                .unwrap();
            if already_reverted {
                receipt.inner().anchors[0]
                    .as_ref()
                    .unwrap()
                    .replace(Some(BEFORE), Some(AFTER))
                    .unwrap();
            }
            drop(receipt.lease.take());
            reconcile(
                &fixture.sessions_dir,
                &reference,
                fixture.locks.clone(),
                CancelToken::none(),
            )
            .await
            .unwrap();
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            assert!(
                fixture
                    .store()
                    .load(&reference)
                    .unwrap()
                    .progress
                    .contains(&RestoreProgress::Complete)
            );
        });
    }

    #[test_case(false; "valid_approval")]
    #[test_case(true; "invalid_approval_is_not_generic_write")]
    fn plan_projection_owns_overlap_and_rolls_back_once(invalid_hash: bool) {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            let agent = fixture.session.root_agent_id().unwrap();
            fixture.change(StateChange::Config(StoredAgentConfig {
                cwd: fixture.session.cwd.clone(),
                model: MODEL.into(),
                thinking: None,
                fast: false,
                workflow: false,
                mode: StoredModeSnapshot {
                    id: "plan".into(),
                    label: "Plan".into(),
                    system_prompt: None,
                    restrict_write_to: Some(fixture.path.clone()),
                    tools: None,
                },
                options: Default::default(),
            }));
            let content = String::from_utf8(BEFORE.to_vec()).unwrap();
            let entry = PlanEntry {
                path: fixture.path.clone(),
                written: true,
                staged_model: None,
                approved: Some(ApprovedPlan {
                    sha256: crate::plan_state::content_sha256(if invalid_hash {
                        "invalid"
                    } else {
                        &content
                    }),
                    content,
                }),
            };
            fixture.session.push_state(StoredStateRecord {
                seq: fixture.session.next_state_seq(),
                agent: None,
                generation: 0,
                change: StateChange::Plugin {
                    key: PLAN_STATE_KEY.into(),
                    version: PLAN_STATE_VERSION,
                    value: Some(
                        serde_json::to_value(
                            [(agent, entry)].into_iter().collect::<BTreeMap<_, _>>(),
                        )
                        .unwrap(),
                    ),
                },
            });
            fixture.session.save_to(&fixture.sessions_dir).unwrap();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut request = fixture.request(Some(AFTER), Some(EXTERNAL));
            request.choice = RestoreChoice::ConversationAndFiles;
            request.projection = Some(PlanProjection {
                snapshot: SessionJournal::from_session(&fixture.session)
                    .unwrap()
                    .snapshot(),
                affected: [agent].into(),
            });
            let mut receipt = prepare(request).await.unwrap();
            assert!(receipt.paths().contains(&fixture.path));
            let applied = receipt.apply(&ModeRegistry::builtin()).unwrap();
            assert_eq!(
                fs::read(&fixture.path).unwrap(),
                if invalid_hash { AFTER } else { BEFORE }
            );
            assert_eq!(!applied.warnings.is_empty(), invalid_hash);
            assert_eq!(
                fixture
                    .store()
                    .load(&receipt.transition_ref())
                    .unwrap()
                    .transition
                    .paths
                    .len(),
                usize::from(!invalid_hash)
            );
            receipt.abort().unwrap();
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
        });
    }

    #[test_case(false; "related")]
    #[test_case(true; "unrelated")]
    fn planner_requires_actual_parent_route(unrelated: bool) {
        let mut fixture = Fixture::new();
        let root = fixture.session.root_agent_id().unwrap();
        let seq = fixture.session.sequence_high_water().unwrap();
        let mut child = StoredSession::new(MODEL, &fixture.session.cwd);
        child.bind_root_id(AgentId::generate()).unwrap();
        child.set_parent(Some(ParentLink {
            session: if unrelated {
                child.id
            } else {
                fixture.session.id
            },
            message_count: 0,
            seq,
        }));
        fixture.session.save_to(&fixture.sessions_dir).unwrap();
        let segments = [
            RestoreSegment {
                session: fixture.session,
                after_seq: 0,
                through_seq: seq,
                affected: [root].into(),
            },
            RestoreSegment {
                affected: [child.root_agent_id().unwrap()].into(),
                through_seq: child.sequence_high_water().unwrap(),
                after_seq: 0,
                session: child,
            },
        ];
        assert_eq!(plan(&segments).is_err(), unrelated);
    }

    #[test]
    fn runtime_conflict_retains_receipt_for_explicit_repair() {
        smol::block_on(async {
            let fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            fs::write(&fixture.path, EXTERNAL).unwrap();
            let reference = receipt.transition_ref();
            let error = receipt.abort().unwrap_err();
            let super::RestoreError::Unresolved { mut receipt, .. } = error else {
                panic!("abort must return ownership of an unresolved restore");
            };
            assert_eq!(fs::read(&fixture.path).unwrap(), EXTERNAL);
            assert_eq!(receipt.transition_ref(), reference);
            fs::write(&fixture.path, BEFORE).unwrap();
            let restored = receipt.lease.take().unwrap();
            let receipt = PreparedRestore {
                lease: Some(restored),
            };
            receipt.abort().unwrap();
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
        });
    }

    #[test]
    fn sibling_creations_share_restore_owned_parent_directories() {
        smol::block_on(async {
            let fixture = Fixture::new();
            let parent = fixture.path.parent().unwrap().join("new");
            let mut request = fixture.request(None, Some(BEFORE));
            request.preview.paths[0].path = parent.join("a");
            request.preview.paths.push(PlannedFile {
                path: parent.join("b"),
                expected: FileState::Absent,
                desired: fixture.state(Some(AFTER)),
                warning: None,
                inverse_of: None,
            });
            let mut receipt = prepare(request).await.unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            assert_eq!(fs::read(parent.join("a")).unwrap(), BEFORE);
            assert_eq!(fs::read(parent.join("b")).unwrap(), AFTER);
            receipt.abort().unwrap();
            assert!(!parent.join("a").exists());
            assert!(!parent.join("b").exists());
        });
    }

    #[test_case(false; "retry_forward")]
    #[test_case(true; "retry_rollback")]
    fn historical_conflict_resolves_after_current_content_is_repaired(rollback: bool) {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            fs::write(&fixture.path, EXTERNAL).unwrap();
            assert!(receipt.apply(&ModeRegistry::builtin()).is_err());
            fs::write(&fixture.path, AFTER).unwrap();
            if rollback {
                receipt.abort().unwrap();
                assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            } else {
                receipt.apply(&ModeRegistry::builtin()).unwrap();
                fixture.publish(&receipt);
                receipt.complete().unwrap();
                assert_eq!(fs::read(&fixture.path).unwrap(), BEFORE);
            }
        });
    }

    #[test]
    fn unpublished_restore_rejects_changed_endpoint_before_writing() {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            fixture.change(StateChange::ToolExecution {
                kind: SHELL_KIND.into(),
            });
            fixture.session.save_to(&fixture.sessions_dir).unwrap();
            assert!(receipt.apply(&ModeRegistry::builtin()).is_err());
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            let error = receipt.abort().unwrap_err();
            let super::RestoreError::Unresolved { mut receipt, .. } = error else {
                panic!("changed endpoint must keep repair unresolved");
            };
            drop(receipt.lease.take());
        });
    }

    #[test]
    fn published_reference_remains_authoritative_after_later_checkpoint() {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            fixture.publish(&receipt);
            fixture.change(StateChange::ToolExecution {
                kind: SHELL_KIND.into(),
            });
            fixture.session.save_to(&fixture.sessions_dir).unwrap();
            receipt.complete().unwrap();
            assert_eq!(fs::read(&fixture.path).unwrap(), BEFORE);
        });
    }

    #[test]
    fn published_restore_rejects_reapplication_without_changing_files() {
        smol::block_on(async {
            let mut fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            receipt.apply(&ModeRegistry::builtin()).unwrap();
            fixture.publish(&receipt);
            assert!(receipt.apply(&ModeRegistry::builtin()).is_err());
            assert_eq!(fs::read(&fixture.path).unwrap(), BEFORE);
            receipt.complete().unwrap();
        });
    }

    #[test_case(false; "attach_barrier_and_abort")]
    #[test_case(true; "drop_retains_receipt_until_abort")]
    fn published_prepare_error_retains_real_manifest_locks_and_attached_lease(drop_receipt: bool) {
        smol::block_on(async {
            let fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let source = RestoreStorageError::Io(io::Error::other(PREPARE_FINISH_ERROR));
            let expected_reason = source.to_string();
            let error = prepare_with(
                fixture.request(Some(AFTER), Some(BEFORE)),
                move |store, owner, manifest| {
                    let reference = store.prepare(owner, manifest)?;
                    Err(RestoreStorageError::Published {
                        reference,
                        source: Box::new(source),
                    })
                },
            )
            .await
            .unwrap_err();
            let super::RestoreError::Unresolved {
                reason,
                mut receipt,
            } = error
            else {
                panic!("published manifest must return an owned repair receipt");
            };
            assert_eq!(reason, expected_reason);
            let reference = receipt.transition_ref();
            assert_eq!(
                fixture
                    .store()
                    .pending_for_session(fixture.session.id)
                    .unwrap(),
                vec![reference.clone()]
            );
            let snapshot = fixture.store().load(&reference).unwrap();
            assert_eq!(snapshot.transition.paths[0].path, fixture.path);
            assert!(snapshot.progress.is_empty());
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            let lease = Arc::new(LockHeartbeat::spawn(
                claim(&fixture.sessions_dir, &fixture.session.id)
                    .unwrap()
                    .unwrap(),
                || {},
            ));
            receipt.retain_barrier(lease.clone());
            drop(lease);
            let cancel = CancelToken::none();
            let mut writer = Box::pin(
                fixture
                    .locks
                    .acquire_all([fixture.path.clone()].into(), &cancel),
            );
            assert!(
                futures_lite::future::poll_once(writer.as_mut())
                    .await
                    .is_none()
            );
            let receipt = if drop_receipt {
                drop(receipt);
                assert!(
                    futures_lite::future::poll_once(writer.as_mut())
                        .await
                        .is_none()
                );
                PreparedRestore::take_retained(&reference).unwrap()
            } else {
                *receipt
            };
            assert!(
                claim(&fixture.sessions_dir, &fixture.session.id)
                    .unwrap()
                    .is_none()
            );
            receipt.abort().unwrap();
            drop(writer.await.unwrap());
            assert!(
                fixture
                    .store()
                    .pending_for_session(fixture.session.id)
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(fs::read(&fixture.path).unwrap(), AFTER);
            assert!(
                claim(&fixture.sessions_dir, &fixture.session.id)
                    .unwrap()
                    .is_some()
            );
        });
    }

    #[test]
    fn dropped_receipt_retains_locks_and_barrier_until_explicit_abort() {
        smol::block_on(async {
            let fixture = Fixture::new();
            fs::write(&fixture.path, AFTER).unwrap();
            let mut receipt = prepare(fixture.request(Some(AFTER), Some(BEFORE)))
                .await
                .unwrap();
            let barrier = Arc::new(());
            receipt.retain_barrier(barrier.clone());
            let reference = receipt.transition_ref();
            drop(receipt);
            assert_eq!(Arc::strong_count(&barrier), 2);
            let receipt = PreparedRestore::take_retained(&reference).unwrap();
            receipt.abort().unwrap();
            assert_eq!(Arc::strong_count(&barrier), 1);
        });
    }
}
