use crate::{
    StateDir,
    blobs::{BlobHash, BlobRef, BlobStore, FileState},
    id::MakimaId,
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::cell::Cell;
use std::{
    collections::HashSet,
    fs::File,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};
use thiserror::Error;

const RESTORES_DIR: &str = "restores";
const BRANCHES_DIR: &str = "branches";
const SESSIONS_DIR: &str = "sessions";
const MANIFEST_FILE: &str = "manifest.json";
const STAGING_PREFIX: &str = ".prepare-";
const PROGRESS_FILE: &str = "progress.jsonl";
const LOCK_FILE: &str = ".lock";
const NAMESPACE_LOCK_FILE: &str = ".restore.lock";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionRef {
    pub session: MakimaId,
    pub transition: MakimaId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreEndpoint {
    pub session: MakimaId,
    pub branch: Option<MakimaId>,
    pub seq: u64,
    pub digest: BlobHash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathRestore {
    pub path: PathBuf,
    pub before: FileState,
    pub after: FileState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathConflict {
    pub path: PathBuf,
    pub reason: String,
    pub actual: Option<FileState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreChoice {
    ConversationOnly,
    ConversationAndFiles,
    FilesOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreTransition {
    pub id: MakimaId,
    pub choice: RestoreChoice,
    pub source: RestoreEndpoint,
    pub target: RestoreEndpoint,
    pub inverse_of: Option<TransitionRef>,
    pub paths: Vec<PathRestore>,
    pub skipped: Vec<PathConflict>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RestoreProgress {
    Applied { path: usize },
    Conflict { path: usize, actual: FileState },
    RollbackIntent,
    RolledBack { path: usize },
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestoreSnapshot {
    pub transition: RestoreTransition,
    pub progress: Vec<RestoreProgress>,
}

impl RestoreSnapshot {
    pub fn applied_paths(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.transition.paths.len()).filter(|path| {
            matches!(
                path_outcome(&self.progress, *path),
                Some(RestoreProgress::Applied { .. })
            )
        })
    }
}

#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("restore storage I/O: {0}")]
    Io(#[from] io::Error),
    #[error("invalid restore JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("restore owner session does not exist: {0}")]
    MissingOwner(MakimaId),
    #[error("invalid restore manifest: {0}")]
    InvalidManifest(String),
    #[error("restore transition already exists with a different manifest")]
    ManifestMismatch,
    #[error("invalid restore progress: {0}")]
    InvalidProgress(String),
    #[error("session has unfinished or cross-session restore references: {0}")]
    Pending(MakimaId),
    #[error("restore blob validation failed: {0}")]
    Blob(String),
    #[error("restore manifest is visible but preparation failed: {reference:?}: {source}")]
    Published {
        reference: TransitionRef,
        #[source]
        source: Box<RestoreError>,
    },
}

#[derive(Debug, Clone)]
pub struct RestoreStore {
    dir: StateDir,
}

#[derive(Debug)]
pub struct RestoreDeletionGuard {
    #[cfg(unix)]
    _lock: File,
}

impl RestoreStore {
    pub fn new(dir: StateDir) -> Self {
        Self { dir }
    }

    pub fn prepare(
        &self,
        owner: MakimaId,
        manifest: RestoreTransition,
    ) -> Result<TransitionRef, RestoreError> {
        validate_manifest(owner, &manifest)?;
        let _namespace = Self::namespace_lock(&self.dir.path().join(SESSIONS_DIR))?;
        let locked = self.owner(owner, true)?;
        self.validate_blobs(&manifest)?;
        let reference = TransitionRef {
            session: owner,
            transition: manifest.id,
        };
        let name = manifest.id.to_string();
        match locked.directory.child(&name, false) {
            Ok(directory) => {
                let mut file = directory.file(MANIFEST_FILE, false)?;
                let stored: RestoreTransition = read_json(&mut file)?;
                if stored != manifest {
                    return Err(RestoreError::ManifestMismatch);
                }
                let result = (|| {
                    read_snapshot(&directory, &reference)?;
                    self.validate_owner(owner, &locked.directory)?;
                    locked
                        .directory
                        .validate_child(&manifest.id.to_string(), &directory)?;
                    directory.validate_file(MANIFEST_FILE, &file)?;
                    file.sync_all()?;
                    directory.file(PROGRESS_FILE, false)?.sync_all()?;
                    directory.sync()?;
                    locked.directory.sync()?;
                    Ok::<_, RestoreError>(())
                })();
                result.map_err(|source| RestoreError::Published {
                    reference: reference.clone(),
                    source: Box::new(source),
                })?;
                return Ok(reference);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let staged_name = format!("{STAGING_PREFIX}{}", MakimaId::generate());
        let directory = locked.directory.stage(&staged_name)?;
        let progress = directory.file(PROGRESS_FILE, true)?;
        progress.sync_all()?;
        let mut file = directory.file(MANIFEST_FILE, true)?;
        file.write_all(&serde_json::to_vec(&manifest)?)?;
        file.sync_all()?;
        directory.sync()?;
        self.validate_owner(owner, &locked.directory)?;
        locked.directory.validate_file(LOCK_FILE, &locked._lock)?;
        locked.directory.validate_child(&staged_name, &directory)?;
        directory.validate_file(MANIFEST_FILE, &file)?;
        directory.validate_file(PROGRESS_FILE, &progress)?;
        locked.directory.publish(&staged_name, &name)?;
        let result = (|| {
            #[cfg(test)]
            fail_prepare_finish(0)?;
            locked.directory.sync()?;
            #[cfg(test)]
            fail_prepare_finish(1)?;
            self.validate_owner(owner, &locked.directory)?;
            #[cfg(test)]
            fail_prepare_finish(2)?;
            locked.directory.validate_child(&name, &directory)?;
            Ok::<_, RestoreError>(())
        })();
        result.map_err(|source| RestoreError::Published {
            reference: reference.clone(),
            source: Box::new(source),
        })?;
        Ok(reference)
    }

    pub fn load(&self, reference: &TransitionRef) -> Result<RestoreSnapshot, RestoreError> {
        let locked = self.owner(reference.session, false)?;
        let directory = locked
            .directory
            .child(&reference.transition.to_string(), false)?;
        let snapshot = read_snapshot(&directory, reference)?;
        self.validate_owner(reference.session, &locked.directory)?;
        locked
            .directory
            .validate_child(&reference.transition.to_string(), &directory)?;
        Ok(snapshot)
    }

    pub fn append(
        &self,
        reference: &TransitionRef,
        event: RestoreProgress,
    ) -> Result<(), RestoreError> {
        let locked = self.owner(reference.session, false)?;
        let directory = locked
            .directory
            .child(&reference.transition.to_string(), false)?;
        let mut manifest_file = directory.file(MANIFEST_FILE, false)?;
        let manifest = read_manifest(&mut manifest_file, reference)?;
        let mut progress_file = directory.file(PROGRESS_FILE, false)?;
        let (progress, valid_len) =
            read_progress(&mut progress_file, reference.session, &manifest)?;
        let append = validate_event(reference.session, &manifest, &progress, &event)?;
        if let RestoreProgress::Conflict { actual, .. } = &event {
            self.validate_blob(actual)?;
        }
        self.validate_owner(reference.session, &locked.directory)?;
        locked
            .directory
            .validate_child(&reference.transition.to_string(), &directory)?;
        locked.directory.validate_file(LOCK_FILE, &locked._lock)?;
        directory.validate_file(MANIFEST_FILE, &manifest_file)?;
        directory.validate_file(PROGRESS_FILE, &progress_file)?;
        if progress_file.metadata()?.len() != valid_len {
            progress_file.set_len(valid_len)?;
            sync_progress(&progress_file)?;
        }
        if append {
            progress_file.seek(SeekFrom::End(0))?;
            let mut bytes = serde_json::to_vec(&event)?;
            bytes.push(b'\n');
            progress_file.write_all(&bytes)?;
        }
        sync_progress(&progress_file)?;
        directory.validate_file(PROGRESS_FILE, &progress_file)?;
        self.validate_owner(reference.session, &locked.directory)?;
        locked
            .directory
            .validate_child(&reference.transition.to_string(), &directory)?;
        Ok(())
    }

    #[cfg(unix)]
    pub fn pending(&self, owner: MakimaId) -> Result<Vec<TransitionRef>, RestoreError> {
        let locked = match self.owner(owner, false) {
            Ok(locked) => locked,
            Err(RestoreError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        let references = pending_in(&locked.directory, owner)?;
        self.validate_owner(owner, &locked.directory)?;
        Ok(references)
    }

    #[cfg(unix)]
    pub fn pending_for_session(
        &self,
        session: MakimaId,
    ) -> Result<Vec<TransitionRef>, RestoreError> {
        let root = Directory::open(self.dir.path())?;
        let sessions = root.child(SESSIONS_DIR, false)?;
        self.validate_checkpoint_references(&sessions, session)?;
        let restores = match sessions.child(RESTORES_DIR, false) {
            Ok(restores) => restores,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };
        let mut references = Vec::new();
        for name in restores.entries()? {
            let owner = name.parse().map_err(|_| {
                RestoreError::InvalidManifest(format!("invalid restore owner directory: {name}"))
            })?;
            let locked = self.owner(owner, false)?;
            for reference in pending_in(&locked.directory, owner)? {
                let directory = locked
                    .directory
                    .child(&reference.transition.to_string(), false)?;
                let snapshot = read_snapshot(&directory, &reference)?;
                if snapshot.transition.source.session == session
                    || snapshot.transition.target.session == session
                {
                    references.push(reference);
                }
            }
            self.validate_owner(owner, &locked.directory)?;
        }
        references.sort_by_key(|reference| (reference.session, reference.transition));
        Ok(references)
    }

    #[cfg(unix)]
    fn validate_checkpoint_references(
        &self,
        sessions: &Directory,
        session: MakimaId,
    ) -> Result<(), RestoreError> {
        for reference in checkpoint_references(sessions, session)? {
            self.load(&reference)?;
        }
        Ok(())
    }

    pub fn guard_session_deletion(
        &self,
        session: MakimaId,
    ) -> Result<RestoreDeletionGuard, RestoreError> {
        Self::guard_session_deletion_in(&self.dir.path().join(SESSIONS_DIR), session)
    }

    #[cfg(unix)]
    pub fn guard_session_deletion_in(
        sessions_root: &Path,
        session: MakimaId,
    ) -> Result<RestoreDeletionGuard, RestoreError> {
        let guard = Self::namespace_lock(sessions_root)?;
        let sessions = Directory::open(sessions_root)?;
        let references = checkpoint_references(&sessions, session)?;
        let mut checkpoints: HashSet<MakimaId> = sessions
            .entries()?
            .into_iter()
            .filter_map(|name| {
                name.strip_suffix(".jsonl")
                    .and_then(|name| name.parse().ok())
            })
            .collect();
        match sessions.child(BRANCHES_DIR, false) {
            Ok(branches) => checkpoints.extend(
                branches
                    .entries()?
                    .into_iter()
                    .filter_map(|name| name.parse::<MakimaId>().ok()),
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        for checkpoint in checkpoints.into_iter().filter(|id| *id != session) {
            if let Ok(references) = checkpoint_references(&sessions, checkpoint)
                && references
                    .iter()
                    .any(|reference| reference.session == session)
            {
                return Err(RestoreError::Pending(session));
            }
        }
        let restores = match sessions.child(RESTORES_DIR, false) {
            Ok(restores) => restores,
            Err(error) if error.kind() == io::ErrorKind::NotFound && references.is_empty() => {
                return Ok(guard);
            }
            Err(error) => return Err(error.into()),
        };
        for reference in references {
            let owner = restores.child(&reference.session.to_string(), false)?;
            let lock = owner.file(LOCK_FILE, true)?;
            FileExt::lock_exclusive(&lock)?;
            owner.validate_file(LOCK_FILE, &lock)?;
            read_snapshot(
                &owner.child(&reference.transition.to_string(), false)?,
                &reference,
            )?;
        }
        for name in restores.entries()? {
            let owner: MakimaId = name.parse().map_err(|_| {
                RestoreError::InvalidManifest(format!("invalid restore owner directory: {name}"))
            })?;
            match sessions.read_file(&format!("{owner}.jsonl")) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(RestoreError::MissingOwner(owner));
                }
                Err(error) => return Err(error.into()),
            }
            let directory = restores.child(&name, false)?;
            let lock = directory.file(LOCK_FILE, true)?;
            FileExt::lock_exclusive(&lock)?;
            directory.validate_file(LOCK_FILE, &lock)?;
            for reference in references_in(&directory, owner, false)? {
                let snapshot = read_snapshot(
                    &directory.child(&reference.transition.to_string(), false)?,
                    &reference,
                )?;
                let transition = &snapshot.transition;
                let pending = snapshot.progress.last() != Some(&RestoreProgress::Complete);
                let cross_session = transition.source.session != owner
                    || transition.target.session != owner
                    || transition
                        .inverse_of
                        .as_ref()
                        .is_some_and(|reference| reference.session != owner);
                if (pending || cross_session)
                    && (owner == session
                        || transition.source.session == session
                        || transition.target.session == session
                        || transition
                            .inverse_of
                            .as_ref()
                            .is_some_and(|reference| reference.session == session))
                {
                    return Err(RestoreError::Pending(session));
                }
            }
        }
        Ok(guard)
    }

    #[cfg(not(unix))]
    pub fn pending(&self, _: MakimaId) -> Result<Vec<TransitionRef>, RestoreError> {
        ensure_no_restore_metadata(&self.dir.path().join(SESSIONS_DIR))?;
        Ok(Vec::new())
    }

    #[cfg(not(unix))]
    pub fn pending_for_session(&self, _: MakimaId) -> Result<Vec<TransitionRef>, RestoreError> {
        ensure_no_restore_metadata(&self.dir.path().join(SESSIONS_DIR))?;
        Ok(Vec::new())
    }

    #[cfg(not(unix))]
    pub fn guard_session_deletion_in(
        sessions_root: &Path,
        _: MakimaId,
    ) -> Result<RestoreDeletionGuard, RestoreError> {
        ensure_no_restore_metadata(sessions_root)?;
        Ok(RestoreDeletionGuard {})
    }

    #[cfg(unix)]
    fn namespace_lock(sessions_root: &Path) -> Result<RestoreDeletionGuard, RestoreError> {
        let sessions = Directory::open(sessions_root)?;
        let lock = sessions.file(NAMESPACE_LOCK_FILE, true)?;
        FileExt::lock_exclusive(&lock)?;
        sessions.validate_file(NAMESPACE_LOCK_FILE, &lock)?;
        lock.sync_all()?;
        sessions.sync()?;
        Ok(RestoreDeletionGuard { _lock: lock })
    }

    #[cfg(not(unix))]
    fn namespace_lock(_: &Path) -> Result<RestoreDeletionGuard, RestoreError> {
        Err(unsupported_restore_metadata().into())
    }

    fn owner(&self, owner: MakimaId, create: bool) -> Result<LockedOwner, RestoreError> {
        let root = match Directory::open(self.dir.path()) {
            Ok(root) => root,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RestoreError::MissingOwner(owner));
            }
            Err(error) => return Err(error.into()),
        };
        let sessions = match root.child(SESSIONS_DIR, false) {
            Ok(sessions) => sessions,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RestoreError::MissingOwner(owner));
            }
            Err(error) => return Err(error.into()),
        };
        let session = match sessions.read_file(&format!("{owner}.jsonl")) {
            Ok(session) => session,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RestoreError::MissingOwner(owner));
            }
            Err(error) => return Err(error.into()),
        };
        session.sync_all()?;
        sessions.sync()?;
        let restores = sessions.child(RESTORES_DIR, create)?;
        let directory = restores.child(&owner.to_string(), create)?;
        let lock = directory.file(LOCK_FILE, true)?;
        FileExt::lock_exclusive(&lock)?;
        directory.validate_file(LOCK_FILE, &lock)?;
        lock.sync_all()?;
        directory.sync()?;
        restores.sync()?;
        match sessions.read_file(&format!("{owner}.jsonl")) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RestoreError::MissingOwner(owner));
            }
            Err(error) => return Err(error.into()),
        }
        let locked = LockedOwner {
            directory,
            _lock: lock,
        };
        self.validate_owner(owner, &locked.directory)?;
        Ok(locked)
    }

    fn validate_owner(&self, owner: MakimaId, expected: &Directory) -> Result<(), RestoreError> {
        let root = Directory::open(self.dir.path())?;
        let sessions = root.child(SESSIONS_DIR, false)?;
        match sessions.read_file(&format!("{owner}.jsonl")) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RestoreError::MissingOwner(owner));
            }
            Err(error) => return Err(error.into()),
        }
        let actual = sessions
            .child(RESTORES_DIR, false)?
            .child(&owner.to_string(), false)?;
        if !expected.same_file(&actual)? {
            return Err(io::Error::other("restore owner directory was replaced").into());
        }
        Ok(())
    }

    fn validate_blobs(&self, manifest: &RestoreTransition) -> Result<(), RestoreError> {
        for state in manifest
            .paths
            .iter()
            .flat_map(|path| [&path.before, &path.after])
            .chain(
                manifest
                    .skipped
                    .iter()
                    .filter_map(|path| path.actual.as_ref()),
            )
        {
            self.validate_blob(state)?;
        }
        Ok(())
    }

    fn validate_blob(&self, state: &FileState) -> Result<(), RestoreError> {
        if let FileState::Blob(reference) = state {
            let blobs = BlobStore::new(self.dir.clone());
            let bytes = blobs
                .read(reference)
                .map_err(|error| RestoreError::Blob(error.to_string()))?;
            blobs
                .put(reference.session, &bytes)
                .map_err(|error| RestoreError::Blob(error.to_string()))?;
        }
        Ok(())
    }
}

#[cfg(any(not(unix), test))]
mod compatibility {
    use super::{BRANCHES_DIR, NAMESPACE_LOCK_FILE, RESTORES_DIR};
    use serde_json::Value;
    #[cfg(windows)]
    use std::os::windows::fs::MetadataExt;
    use std::{fs, io, path::Path};

    #[cfg(windows)]
    const REPARSE_POINT: u32 = 0x400;
    const BLOBS_DIR: &str = "blobs";
    const UNSUPPORTED: &str = "file restore metadata cannot be inspected safely on this platform";

    pub(super) fn unsupported_restore_metadata() -> io::Error {
        io::Error::new(io::ErrorKind::Unsupported, UNSUPPORTED)
    }

    pub(super) fn ensure_no_restore_metadata(root: &Path) -> io::Result<()> {
        inspect(root).map_err(|_| unsupported_restore_metadata())
    }

    fn safe_metadata(path: &Path) -> io::Result<fs::Metadata> {
        let metadata = fs::symlink_metadata(path)?;
        #[cfg(windows)]
        let reparse = metadata.file_attributes() & REPARSE_POINT != 0;
        #[cfg(not(windows))]
        let reparse = false;
        if metadata.is_symlink() || reparse {
            return Err(unsupported_restore_metadata());
        }
        Ok(metadata)
    }

    fn inspect(root: &Path) -> io::Result<()> {
        match safe_metadata(root) {
            Ok(metadata) if metadata.is_dir() => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            _ => return Err(unsupported_restore_metadata()),
        }
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = safe_metadata(&path)?;
            let name = entry.file_name();
            if [RESTORES_DIR, BLOBS_DIR, NAMESPACE_LOCK_FILE]
                .iter()
                .any(|artifact| name == *artifact)
            {
                return Err(unsupported_restore_metadata());
            }
            if name == BRANCHES_DIR {
                if !metadata.is_dir() {
                    return Err(unsupported_restore_metadata());
                }
                for session in fs::read_dir(path)? {
                    let session = session?;
                    if !safe_metadata(&session.path())?.is_dir() {
                        return Err(unsupported_restore_metadata());
                    }
                    for checkpoint in fs::read_dir(session.path())? {
                        inspect_checkpoint(&checkpoint?.path())?;
                    }
                }
            } else if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                inspect_checkpoint(&path)?;
            }
        }
        Ok(())
    }

    fn inspect_checkpoint(path: &Path) -> io::Result<()> {
        if !safe_metadata(path)?.is_file() {
            return Err(unsupported_restore_metadata());
        }
        for line in fs::read(path)?
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
        {
            let record: Value =
                serde_json::from_slice(line).map_err(|_| unsupported_restore_metadata())?;
            if record["t"] == "restore_ref"
                || (record["t"] == "branch" && !record["d"]["transition"].is_null())
            {
                return Err(unsupported_restore_metadata());
            }
        }
        Ok(())
    }
}

#[cfg(not(unix))]
use compatibility::{ensure_no_restore_metadata, unsupported_restore_metadata};

#[cfg(unix)]
fn checkpoint_references(
    sessions: &Directory,
    session: MakimaId,
) -> Result<Vec<TransitionRef>, RestoreError> {
    let mut references = Vec::new();
    match sessions.read_file(&format!("{session}.jsonl")) {
        Ok(mut file) => read_checkpoint_references(&mut file, &mut references)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    match sessions
        .child(BRANCHES_DIR, false)
        .and_then(|branches| branches.child(&session.to_string(), false))
    {
        Ok(branches) => {
            for name in branches.entries()? {
                if name.ends_with(".jsonl") {
                    read_checkpoint_references(&mut branches.read_file(&name)?, &mut references)?;
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(references)
}

#[cfg(unix)]
fn read_checkpoint_references(
    file: &mut File,
    references: &mut Vec<TransitionRef>,
) -> Result<(), RestoreError> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    for line in bytes
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: serde_json::Value = match serde_json::from_slice(line) {
            Ok(record) => record,
            Err(error) if error.is_syntax() || error.is_eof() => continue,
            Err(error) => return Err(error.into()),
        };
        let reference = match record["t"].as_str() {
            Some("restore_ref") => Some(serde_json::from_value(record["d"].clone())?),
            Some("branch") if !record["d"]["transition"].is_null() => {
                Some(serde_json::from_value(record["d"]["transition"].clone())?)
            }
            _ => None,
        };
        if let Some(reference) = reference
            && !references.contains(&reference)
        {
            references.push(reference);
        }
    }
    Ok(())
}

#[cfg(unix)]
fn pending_in(directory: &Directory, owner: MakimaId) -> Result<Vec<TransitionRef>, RestoreError> {
    references_in(directory, owner, true)
}

#[cfg(unix)]
fn references_in(
    directory: &Directory,
    owner: MakimaId,
    pending_only: bool,
) -> Result<Vec<TransitionRef>, RestoreError> {
    let mut references = Vec::new();
    for name in directory.entries()? {
        if name == LOCK_FILE
            || name
                .strip_prefix(STAGING_PREFIX)
                .is_some_and(|suffix| suffix.parse::<MakimaId>().is_ok())
        {
            continue;
        }
        let transition = name.parse().map_err(|_| {
            RestoreError::InvalidManifest(format!("invalid transition directory: {name}"))
        })?;
        let reference = TransitionRef {
            session: owner,
            transition,
        };
        let directory = directory.child(&name, false)?;
        let snapshot = read_snapshot(&directory, &reference)?;
        if !pending_only || snapshot.progress.last() != Some(&RestoreProgress::Complete) {
            references.push(reference);
        }
    }
    references.sort_by_key(|reference| reference.transition);
    Ok(references)
}

fn validate_manifest(owner: MakimaId, manifest: &RestoreTransition) -> Result<(), RestoreError> {
    let mut paths = HashSet::new();
    for path in manifest
        .paths
        .iter()
        .map(|path| &path.path)
        .chain(manifest.skipped.iter().map(|path| &path.path))
    {
        let normalized: PathBuf = path.components().collect();
        if !path.is_absolute()
            || path.file_name().is_none()
            || path
                .components()
                .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
            || path.as_os_str() != normalized.as_os_str()
            || !paths.insert(path)
        {
            return Err(RestoreError::InvalidManifest(format!(
                "path must be unique, absolute and normalized: {}",
                path.display()
            )));
        }
    }
    for state in manifest
        .paths
        .iter()
        .flat_map(|path| [&path.before, &path.after])
        .chain(
            manifest
                .skipped
                .iter()
                .filter_map(|path| path.actual.as_ref()),
        )
    {
        validate_state_owner(owner, state)?;
    }
    Ok(())
}

fn validate_state_owner(owner: MakimaId, state: &FileState) -> Result<(), RestoreError> {
    if let FileState::Blob(BlobRef { session, .. }) = state
        && *session != owner
    {
        return Err(RestoreError::InvalidManifest(
            "file-state blobs must belong to the restore owner".into(),
        ));
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(file: &mut File) -> Result<T, RestoreError> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn read_manifest(
    file: &mut File,
    reference: &TransitionRef,
) -> Result<RestoreTransition, RestoreError> {
    let manifest: RestoreTransition = read_json(file)?;
    if manifest.id != reference.transition {
        return Err(RestoreError::InvalidManifest(
            "transition id does not match its directory".into(),
        ));
    }
    validate_manifest(reference.session, &manifest)?;
    Ok(manifest)
}

fn read_snapshot(
    directory: &Directory,
    reference: &TransitionRef,
) -> Result<RestoreSnapshot, RestoreError> {
    let transition = read_manifest(&mut directory.file(MANIFEST_FILE, false)?, reference)?;
    let mut progress_file = directory.file(PROGRESS_FILE, false)?;
    let (progress, _) = read_progress(&mut progress_file, reference.session, &transition)?;
    if progress.last() == Some(&RestoreProgress::Complete) {
        sync_progress(&progress_file)?;
        directory.validate_file(PROGRESS_FILE, &progress_file)?;
    }
    Ok(RestoreSnapshot {
        transition,
        progress,
    })
}

fn read_progress(
    file: &mut File,
    owner: MakimaId,
    manifest: &RestoreTransition,
) -> Result<(Vec<RestoreProgress>, u64), RestoreError> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut progress = Vec::new();
    let mut valid_len = 0;
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if line.last() != Some(&b'\n') {
            match serde_json::from_slice::<serde_json::Value>(line) {
                Err(error) if error.is_eof() => break,
                Err(error) => return Err(error.into()),
                Ok(_) => {
                    return Err(RestoreError::InvalidProgress(
                        "complete progress event has no line terminator".into(),
                    ));
                }
            }
        }
        let event: RestoreProgress = serde_json::from_slice(line)?;
        if validate_event(owner, manifest, &progress, &event)? {
            progress.push(event);
        }
        valid_len += line.len() as u64;
    }
    Ok((progress, valid_len))
}

fn path_outcome(progress: &[RestoreProgress], path: usize) -> Option<&RestoreProgress> {
    progress.iter().rev().find(|event| match event {
        RestoreProgress::Applied { path: previous }
        | RestoreProgress::Conflict { path: previous, .. }
        | RestoreProgress::RolledBack { path: previous } => *previous == path,
        _ => false,
    })
}

fn validate_event(
    owner: MakimaId,
    manifest: &RestoreTransition,
    progress: &[RestoreProgress],
    event: &RestoreProgress,
) -> Result<bool, RestoreError> {
    let invalid = |reason: &str| RestoreError::InvalidProgress(reason.into());
    if progress.last() == Some(&RestoreProgress::Complete) {
        return if *event == RestoreProgress::Complete {
            Ok(false)
        } else {
            Err(invalid("event follows completion"))
        };
    }
    let rollback = progress.contains(&RestoreProgress::RollbackIntent);
    match event {
        RestoreProgress::Applied { path }
        | RestoreProgress::Conflict { path, .. }
        | RestoreProgress::RolledBack { path } => {
            if *path >= manifest.paths.len() {
                return Err(invalid("path index is out of range"));
            }
            let previous = path_outcome(progress, *path);
            if previous == Some(event) {
                return Ok(false);
            }
            match event {
                RestoreProgress::Applied { .. } if rollback => {
                    return Err(invalid("new application follows rollback intent"));
                }
                RestoreProgress::RolledBack { .. } if !rollback => {
                    return Err(invalid("rollback requires intent"));
                }
                RestoreProgress::Conflict { actual, .. } => {
                    validate_state_owner(owner, actual)?;
                }
                _ => {}
            }
        }
        RestoreProgress::RollbackIntent if rollback => return Ok(false),
        RestoreProgress::RollbackIntent => {}
        RestoreProgress::Complete => {
            for path in 0..manifest.paths.len() {
                match path_outcome(progress, path) {
                    Some(RestoreProgress::Conflict { .. }) => {
                        return Err(invalid("completion has unresolved runtime conflicts"));
                    }
                    Some(RestoreProgress::RolledBack { .. }) | None if rollback => {}
                    Some(RestoreProgress::Applied { .. }) if !rollback => {}
                    _ => return Err(invalid("completion has unresolved paths")),
                }
            }
        }
    }
    Ok(true)
}

fn sync_progress(file: &File) -> io::Result<()> {
    #[cfg(test)]
    if FAIL_PROGRESS_SYNC.with(|fail| fail.replace(false)) {
        return Err(io::Error::other("injected restore progress sync failure"));
    }
    file.sync_all()
}

#[cfg(test)]
thread_local! {
    static FAIL_PROGRESS_SYNC: Cell<bool> = const { Cell::new(false) };
    static FAIL_PREPARE_FINISH: Cell<Option<usize>> = const { Cell::new(None) };
}

#[cfg(test)]
fn fail_prepare_finish(stage: usize) -> io::Result<()> {
    FAIL_PREPARE_FINISH.with(|failure| {
        if failure.get() == Some(stage) {
            failure.set(None);
            Err(io::Error::other(
                "injected restore prepare publication failure",
            ))
        } else {
            Ok(())
        }
    })
}

struct LockedOwner {
    directory: Directory,
    _lock: File,
}

#[cfg(unix)]
use unix::Directory;

#[cfg(unix)]
mod unix {
    use rustix::{
        fs::{self, Dir, Mode, OFlags},
        io::Errno,
    };
    use std::{
        fs::File,
        io,
        os::unix::fs::MetadataExt,
        path::{Component, Path},
    };

    const DIRECTORY_FLAGS: OFlags = OFlags::RDONLY
        .union(OFlags::DIRECTORY)
        .union(OFlags::NOFOLLOW)
        .union(OFlags::CLOEXEC);
    const FILE_FLAGS: OFlags = OFlags::RDWR
        .union(OFlags::NOFOLLOW)
        .union(OFlags::NONBLOCK)
        .union(OFlags::CLOEXEC);
    const DIRECTORY_MODE: Mode = Mode::RUSR.union(Mode::WUSR).union(Mode::XUSR);
    const FILE_MODE: Mode = Mode::RUSR.union(Mode::WUSR);

    pub(super) struct Directory(File);

    impl Directory {
        pub(super) fn open(path: &Path) -> io::Result<Self> {
            let mut directory = Self(File::from(fs::open(
                if path.is_absolute() { "/" } else { "." },
                DIRECTORY_FLAGS,
                Mode::empty(),
            )?));
            for part in path.components() {
                match part {
                    Component::RootDir | Component::CurDir => {}
                    Component::Normal(name) => {
                        directory = Self(File::from(fs::openat(
                            &directory.0,
                            name,
                            DIRECTORY_FLAGS,
                            Mode::empty(),
                        )?));
                    }
                    _ => return Err(io::Error::other("invalid state directory path")),
                }
            }
            Ok(directory)
        }

        pub(super) fn child(&self, name: &str, create: bool) -> io::Result<Self> {
            if create {
                match fs::mkdirat(&self.0, name, DIRECTORY_MODE) {
                    Ok(()) => self.sync()?,
                    Err(Errno::EXIST) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(Self(File::from(fs::openat(
                &self.0,
                name,
                DIRECTORY_FLAGS,
                Mode::empty(),
            )?)))
        }

        pub(super) fn file(&self, name: &str, create: bool) -> io::Result<File> {
            let flags = if create {
                FILE_FLAGS | OFlags::CREATE
            } else {
                FILE_FLAGS
            };
            let file = File::from(fs::openat(&self.0, name, flags, FILE_MODE)?);
            let metadata = file.metadata()?;
            if !metadata.is_file() || metadata.nlink() != 1 {
                return Err(io::Error::other(
                    "restore entry is not a single-link regular file",
                ));
            }
            Ok(file)
        }

        pub(super) fn read_file(&self, name: &str) -> io::Result<File> {
            let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
            let file = File::from(fs::openat(&self.0, name, flags, Mode::empty())?);
            if !file.metadata()?.is_file() {
                return Err(io::Error::other("checkpoint entry is not a regular file"));
            }
            Ok(file)
        }

        pub(super) fn stage(&self, name: &str) -> io::Result<Self> {
            fs::mkdirat(&self.0, name, DIRECTORY_MODE)?;
            self.sync()?;
            self.child(name, false)
        }

        pub(super) fn publish(&self, from: &str, to: &str) -> io::Result<()> {
            #[cfg(target_os = "linux")]
            {
                fs::renameat_with(&self.0, from, &self.0, to, fs::RenameFlags::NOREPLACE)
                    .map_err(Into::into)
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = (from, to);
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "atomic restore publication",
                ))
            }
        }

        pub(super) fn validate_file(&self, name: &str, expected: &File) -> io::Result<()> {
            let actual = self.file(name, false)?.metadata()?;
            let expected = expected.metadata()?;
            if expected.nlink() != 1
                || expected.dev() != actual.dev()
                || expected.ino() != actual.ino()
            {
                return Err(io::Error::other("restore file was replaced"));
            }
            Ok(())
        }

        pub(super) fn validate_child(&self, name: &str, expected: &Self) -> io::Result<()> {
            if !expected.same_file(&self.child(name, false)?)? {
                return Err(io::Error::other("restore directory was replaced"));
            }
            Ok(())
        }

        pub(super) fn same_file(&self, other: &Self) -> io::Result<bool> {
            let expected = self.0.metadata()?;
            let actual = other.0.metadata()?;
            Ok(expected.dev() == actual.dev() && expected.ino() == actual.ino())
        }

        pub(super) fn sync(&self) -> io::Result<()> {
            self.0.sync_all()
        }

        pub(super) fn entries(&self) -> io::Result<Vec<String>> {
            let mut names = Vec::new();
            for entry in Dir::read_from(&self.0)? {
                let entry = entry?;
                let name = entry.file_name().to_bytes();
                if name == b"." || name == b".." {
                    continue;
                }
                names.push(
                    String::from_utf8(name.to_vec())
                        .map_err(|_| io::Error::other("non-UTF-8 restore entry"))?,
                );
            }
            Ok(names)
        }
    }
}

#[cfg(not(unix))]
struct Directory;

#[cfg(not(unix))]
impl Directory {
    fn open(_: &Path) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secure restore storage is unavailable on this platform",
        ))
    }

    fn child(&self, _: &str, _: bool) -> io::Result<Self> {
        Self::open(Path::new(""))
    }

    fn file(&self, _: &str, _: bool) -> io::Result<File> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secure restore storage",
        ))
    }

    fn read_file(&self, _: &str) -> io::Result<File> {
        Err(unsupported_restore_metadata())
    }

    fn stage(&self, _: &str) -> io::Result<Self> {
        Self::open(Path::new(""))
    }

    fn publish(&self, _: &str, _: &str) -> io::Result<()> {
        self.sync()
    }

    fn validate_file(&self, _: &str, _: &File) -> io::Result<()> {
        self.sync()
    }

    fn validate_child(&self, _: &str, _: &Self) -> io::Result<()> {
        self.sync()
    }

    fn same_file(&self, _: &Self) -> io::Result<bool> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secure restore storage",
        ))
    }

    fn sync(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "secure restore storage",
        ))
    }
}

#[cfg(test)]
mod compatibility_tests {
    #[cfg(not(unix))]
    use super::RestoreStore;
    use super::{
        BRANCHES_DIR, NAMESPACE_LOCK_FILE, RESTORES_DIR, SESSIONS_DIR,
        compatibility::ensure_no_restore_metadata,
    };
    #[cfg(not(unix))]
    use crate::{StateDir, id::MakimaId};
    use serde_json::json;
    use std::{fs, io};
    use tempfile::TempDir;
    use test_case::test_case;

    const CHECKPOINT: &str = "checkpoint.jsonl";
    const LEGACY_CHECKPOINT: &[u8] = b"{\"t\":\"header\",\"v\":2}\n{\"t\":\"branch\",\"d\":{}}";

    #[test_case(false; "missing_sessions")]
    #[test_case(true; "legacy_main_and_branch")]
    fn metadata_free_compatibility_inspection_allows_legacy_sessions(existing: bool) {
        let directory = TempDir::new().unwrap();
        let sessions = directory.path().join(SESSIONS_DIR);
        if existing {
            let branches = sessions.join(BRANCHES_DIR).join("session");
            fs::create_dir_all(&branches).unwrap();
            fs::write(sessions.join(CHECKPOINT), LEGACY_CHECKPOINT).unwrap();
            fs::write(branches.join(CHECKPOINT), LEGACY_CHECKPOINT).unwrap();
        }
        ensure_no_restore_metadata(&sessions).unwrap();
        #[cfg(not(unix))]
        {
            let state = StateDir::from_path(directory.path().to_path_buf());
            let store = RestoreStore::new(state);
            let id = MakimaId::generate();
            assert!(store.pending(id).unwrap().is_empty());
            assert!(store.pending_for_session(id).unwrap().is_empty());
            store.guard_session_deletion(id).unwrap();
        }
    }

    #[test_case(RESTORES_DIR; "restore_directory")]
    #[test_case("blobs"; "blob_directory")]
    #[test_case(NAMESPACE_LOCK_FILE; "restore_lock")]
    fn compatibility_artifacts_are_unsupported(artifact: &str) {
        let directory = TempDir::new().unwrap();
        let sessions = directory.path().join(SESSIONS_DIR);
        fs::create_dir(&sessions).unwrap();
        fs::write(sessions.join(artifact), LEGACY_CHECKPOINT).unwrap();
        assert_eq!(
            ensure_no_restore_metadata(&sessions).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[test_case(false, false; "main_restore_ref")]
    #[test_case(false, true; "main_branch_transition")]
    #[test_case(true, false; "retained_restore_ref")]
    #[test_case(true, true; "retained_branch_transition")]
    fn compatibility_checkpoint_references_are_unsupported(retained: bool, branch: bool) {
        let directory = TempDir::new().unwrap();
        let sessions = directory.path().join(SESSIONS_DIR);
        let checkpoint = if retained {
            sessions.join(BRANCHES_DIR).join("session")
        } else {
            sessions.clone()
        };
        fs::create_dir_all(&checkpoint).unwrap();
        let record = if branch {
            json!({"t":"branch","d":{"transition":{}}})
        } else {
            json!({"t":"restore_ref","d":{}})
        };
        fs::write(checkpoint.join(CHECKPOINT), record.to_string()).unwrap();
        assert_eq!(
            ensure_no_restore_metadata(&sessions).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[test]
    fn compatibility_inspection_errors_are_unsupported() {
        let directory = TempDir::new().unwrap();
        let sessions = directory.path().join(SESSIONS_DIR);
        fs::create_dir(&sessions).unwrap();
        fs::write(sessions.join(CHECKPOINT), b"{").unwrap();
        assert_eq!(
            ensure_no_restore_metadata(&sessions).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[cfg(unix)]
    #[test]
    fn compatibility_inspection_rejects_symlinked_checkpoints() {
        use std::os::unix::fs::symlink;
        let directory = TempDir::new().unwrap();
        let sessions = directory.path().join(SESSIONS_DIR);
        fs::create_dir(&sessions).unwrap();
        let outside = directory.path().join("outside");
        fs::write(&outside, LEGACY_CHECKPOINT).unwrap();
        symlink(&outside, sessions.join(CHECKPOINT)).unwrap();
        assert_eq!(
            ensure_no_restore_metadata(&sessions).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(fs::read(outside).unwrap(), LEGACY_CHECKPOINT);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::{
        FAIL_PREPARE_FINISH, FAIL_PROGRESS_SYNC, LOCK_FILE, MANIFEST_FILE, NAMESPACE_LOCK_FILE,
        PROGRESS_FILE, PathConflict, PathRestore, RESTORES_DIR, RestoreChoice, RestoreEndpoint,
        RestoreError, RestoreProgress, RestoreStore, RestoreTransition, SESSIONS_DIR,
        STAGING_PREFIX, TransitionRef,
    };
    use crate::{
        StateDir,
        blobs::{BlobHash, BlobRef, BlobStore, FileState},
        id::MakimaId,
        sessions::{Session, SessionError},
    };
    use fs2::FileExt;
    use serde_json::Value;
    use std::{
        fs::{self, OpenOptions},
        io::Write,
        os::unix::fs::symlink,
        path::PathBuf,
        slice::from_ref,
        sync::{Arc, Barrier},
        thread,
    };
    use tempfile::TempDir;
    use test_case::test_case;

    const BEFORE: &[u8] = b"before restore";
    const AFTER: &[u8] = b"after restore";
    const SKIP_REASON: &str = "outside selected paths";
    const TORN_EVENT: &[u8] = b"{\"kind\":\"applied\",\"path\":";

    type TestSession = Session<Value, Value, Value>;

    struct Fixture {
        root: TempDir,
        state: StateDir,
        owner: MakimaId,
        manifest: RestoreTransition,
    }

    impl Fixture {
        fn new() -> Self {
            let root = TempDir::new().unwrap();
            let state = StateDir::from_path(root.path().to_path_buf());
            let owner = MakimaId::generate();
            let sessions = root.path().join(SESSIONS_DIR);
            fs::create_dir(&sessions).unwrap();
            fs::write(sessions.join(format!("{owner}.jsonl")), b"{}\n").unwrap();
            let blobs = BlobStore::new(state.clone());
            let before = blobs.put(owner, BEFORE).unwrap();
            let after = blobs.put(owner, AFTER).unwrap();
            let endpoint = RestoreEndpoint {
                session: owner,
                branch: None,
                seq: 0,
                digest: BlobHash::digest(BEFORE),
            };
            let manifest = RestoreTransition {
                id: MakimaId::generate(),
                choice: RestoreChoice::ConversationAndFiles,
                source: endpoint.clone(),
                target: RestoreEndpoint {
                    seq: 1,
                    digest: BlobHash::digest(AFTER),
                    ..endpoint
                },
                inverse_of: None,
                paths: vec![PathRestore {
                    path: root.path().join("work/file"),
                    before: FileState::Blob(before),
                    after: FileState::Blob(after),
                }],
                skipped: vec![PathConflict {
                    path: root.path().join("work/skipped"),
                    reason: SKIP_REASON.into(),
                    actual: Some(FileState::Absent),
                }],
            };
            Self {
                root,
                state,
                owner,
                manifest,
            }
        }

        fn store(&self) -> RestoreStore {
            RestoreStore::new(self.state.clone())
        }

        fn prepare(&self) -> TransitionRef {
            self.store()
                .prepare(self.owner, self.manifest.clone())
                .unwrap()
        }

        fn directory(&self) -> PathBuf {
            self.root
                .path()
                .join(SESSIONS_DIR)
                .join(RESTORES_DIR)
                .join(self.owner.to_string())
                .join(self.manifest.id.to_string())
        }

        fn owner_log(&self) -> PathBuf {
            self.root
                .path()
                .join(SESSIONS_DIR)
                .join(format!("{}.jsonl", self.owner))
        }
    }

    #[test_case(false; "forward")]
    #[test_case(true; "rollback")]
    fn reopen_between_every_durable_operation(rollback: bool) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        assert_eq!(
            fixture.store().pending(fixture.owner).unwrap(),
            from_ref(&reference)
        );
        let applied = RestoreProgress::Applied { path: 0 };
        fixture.store().append(&reference, applied.clone()).unwrap();
        fixture.store().append(&reference, applied.clone()).unwrap();
        let snapshot = fixture.store().load(&reference).unwrap();
        assert_eq!(snapshot.transition, fixture.manifest);
        assert_eq!(snapshot.progress, [applied]);
        assert_eq!(snapshot.applied_paths().collect::<Vec<_>>(), [0]);
        if rollback {
            fixture
                .store()
                .append(&reference, RestoreProgress::RollbackIntent)
                .unwrap();
            assert!(
                fixture
                    .store()
                    .append(&reference, RestoreProgress::Complete)
                    .is_err()
            );
            fixture
                .store()
                .append(&reference, RestoreProgress::RolledBack { path: 0 })
                .unwrap();
            assert!(
                fixture
                    .store()
                    .load(&reference)
                    .unwrap()
                    .applied_paths()
                    .next()
                    .is_none()
            );
        }
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        assert!(fixture.store().pending(fixture.owner).unwrap().is_empty());
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::RollbackIntent)
                .is_err()
        );
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .is_err()
        );
    }

    #[test_case(false; "new_event")]
    #[test_case(true; "duplicate_event")]
    fn torn_tail_is_repaired_before_append(duplicate: bool) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        if duplicate {
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .unwrap();
        }
        OpenOptions::new()
            .append(true)
            .open(fixture.directory().join(PROGRESS_FILE))
            .unwrap()
            .write_all(TORN_EVENT)
            .unwrap();
        let snapshot = fixture.store().load(&reference).unwrap();
        assert_eq!(snapshot.progress.len(), usize::from(duplicate));
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        let bytes = fs::read(fixture.directory().join(PROGRESS_FILE)).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            fixture.store().load(&reference).unwrap().progress,
            [RestoreProgress::Applied { path: 0 }]
        );
    }

    #[test_case(b"{}\n"; "malformed_complete_line")]
    #[test_case(b"{\"kind\":\"unknown\"}\n"; "unknown_event")]
    #[test_case(b"{\"kind\":\"applied\",\"path\":99}\n"; "out_of_range")]
    #[test_case(b"{\"kind\":\"complete\"}\n"; "early_complete")]
    #[test_case(b"not json"; "non_torn_garbage")]
    #[test_case(b"{\"kind\":\"complete\"}"; "full_unterminated_event")]
    #[test_case(b"{\"kind\":\"applied\",\"path\":0}\n{}\n{\"kind\":\"complete\"}\n"; "malformed_middle")]
    fn malformed_progress_is_not_discarded(bytes: &[u8]) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let path = fixture.directory().join(PROGRESS_FILE);
        fs::write(&path, bytes).unwrap();
        assert!(fixture.store().load(&reference).is_err());
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .is_err()
        );
        assert!(fixture.store().pending(fixture.owner).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test_case(0; "empty_staging_directory")]
    #[test_case(1; "progress_only")]
    #[test_case(2; "partial_manifest")]
    #[test_case(3; "synced_manifest")]
    fn abandoned_prepare_staging_does_not_poison_admission(stage: usize) {
        let fixture = Fixture::new();
        let staged_name = format!("{STAGING_PREFIX}{}", MakimaId::generate());
        {
            let store = fixture.store();
            let locked = store.owner(fixture.owner, true).unwrap();
            let staged = locked.directory.stage(&staged_name).unwrap();
            if stage > 0 {
                staged
                    .file(PROGRESS_FILE, true)
                    .unwrap()
                    .sync_all()
                    .unwrap();
            }
            if stage > 1 {
                let mut manifest = staged.file(MANIFEST_FILE, true).unwrap();
                let bytes = serde_json::to_vec(&fixture.manifest).unwrap();
                let bytes = if stage == 2 {
                    &bytes[..bytes.len() / 2]
                } else {
                    &bytes
                };
                manifest.write_all(bytes).unwrap();
                manifest.sync_all().unwrap();
            }
            staged.sync().unwrap();
        }
        assert!(!fixture.directory().exists());
        assert!(fixture.store().pending(fixture.owner).unwrap().is_empty());
        assert!(
            fixture
                .store()
                .pending_for_session(fixture.owner)
                .unwrap()
                .is_empty()
        );
        let reference = fixture.prepare();
        assert_eq!(
            fixture.store().load(&reference).unwrap().transition,
            fixture.manifest
        );
        assert_eq!(
            fixture.store().pending(fixture.owner).unwrap(),
            from_ref(&reference)
        );
        assert_eq!(fixture.prepare(), reference);
    }

    #[test_case(0; "directory_sync")]
    #[test_case(1; "owner_validation")]
    #[test_case(2; "transition_identity_validation")]
    fn published_prepare_failure_keeps_discoverable_reference_for_repair(stage: usize) {
        let fixture = Fixture::new();
        let before = fs::read(fixture.owner_log()).unwrap();
        FAIL_PREPARE_FINISH.with(|failure| failure.set(Some(stage)));
        let result = fixture
            .store()
            .prepare(fixture.owner, fixture.manifest.clone());
        let Err(RestoreError::Published { reference, source }) = result else {
            panic!("post-publication failure must expose its transition reference");
        };
        assert!(matches!(*source, RestoreError::Io(_)));
        assert_eq!(reference.session, fixture.owner);
        assert_eq!(reference.transition, fixture.manifest.id);
        assert_eq!(
            fixture.store().load(&reference).unwrap().transition,
            fixture.manifest
        );
        assert_eq!(
            fixture.store().pending_for_session(fixture.owner).unwrap(),
            from_ref(&reference)
        );
        assert!(matches!(TestSession::delete(fixture.owner, &fixture.state),
            Err(SessionError::Restore(RestoreError::Pending(owner))) if owner == fixture.owner));
        assert_eq!(fs::read(fixture.owner_log()).unwrap(), before);
        assert!(!fixture.manifest.paths[0].path.exists());
        assert_eq!(fixture.prepare(), reference);
        fixture
            .store()
            .append(&reference, RestoreProgress::RollbackIntent)
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        assert!(
            fixture
                .store()
                .pending_for_session(fixture.owner)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn staged_publication_never_overwrites_existing_directory() {
        let fixture = Fixture::new();
        let store = fixture.store();
        let locked = store.owner(fixture.owner, true).unwrap();
        let staged_name = format!("{STAGING_PREFIX}{}", MakimaId::generate());
        let staged = locked.directory.stage(&staged_name).unwrap();
        let name = fixture.manifest.id.to_string();
        locked.directory.child(&name, true).unwrap();
        assert!(locked.directory.publish(&staged_name, &name).is_err());
        locked
            .directory
            .validate_child(&staged_name, &staged)
            .unwrap();
    }

    #[test]
    fn immutable_prepare_retry_and_mismatch() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let manifest_path = fixture.directory().join(MANIFEST_FILE);
        let original = fs::read(&manifest_path).unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        assert_eq!(fixture.prepare(), reference);
        let mut changed = fixture.manifest.clone();
        changed.target.seq += 1;
        assert!(matches!(
            fixture.store().prepare(fixture.owner, changed),
            Err(RestoreError::ManifestMismatch)
        ));
        assert_eq!(fs::read(manifest_path).unwrap(), original);
        assert_eq!(
            fixture.store().load(&reference).unwrap().progress,
            [RestoreProgress::Applied { path: 0 }]
        );
    }

    #[test_case("relative/file"; "relative")]
    #[test_case("/work/../file"; "parent")]
    #[test_case("/work/./file"; "dot")]
    #[test_case("/work//file"; "double_separator")]
    fn invalid_manifest_paths_are_rejected(path: &str) {
        let mut fixture = Fixture::new();
        fixture.manifest.paths[0].path = path.into();
        assert!(matches!(
            fixture
                .store()
                .prepare(fixture.owner, fixture.manifest.clone()),
            Err(RestoreError::InvalidManifest(_))
        ));
        assert!(!fixture.directory().exists());
    }

    #[test]
    fn duplicate_skipped_path_and_foreign_blob_are_rejected() {
        let fixture = Fixture::new();
        let mut manifest = fixture.manifest.clone();
        manifest.skipped[0].path = manifest.paths[0].path.clone();
        assert!(fixture.store().prepare(fixture.owner, manifest).is_err());
        let mut manifest = fixture.manifest.clone();
        manifest.paths[0].before = FileState::Blob(BlobRef {
            session: MakimaId::generate(),
            hash: BlobHash::digest(BEFORE),
        });
        assert!(fixture.store().prepare(fixture.owner, manifest).is_err());
    }

    #[test_case(false; "missing")]
    #[test_case(true; "corrupt")]
    fn unavailable_blob_prevents_prepare(corrupt: bool) {
        let fixture = Fixture::new();
        let blob = fixture
            .root
            .path()
            .join(SESSIONS_DIR)
            .join("blobs")
            .join(fixture.owner.to_string())
            .join(BlobHash::digest(BEFORE).as_str());
        if corrupt {
            fs::write(&blob, AFTER).unwrap();
        } else {
            fs::remove_file(&blob).unwrap();
        }
        assert!(matches!(
            fixture
                .store()
                .prepare(fixture.owner, fixture.manifest.clone()),
            Err(RestoreError::Blob(_))
        ));
        assert!(!fixture.directory().exists());
    }

    #[test]
    fn deleted_owner_fails_every_explicit_operation() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        fs::remove_file(fixture.owner_log()).unwrap();
        assert!(matches!(
            fixture.store().load(&reference),
            Err(RestoreError::MissingOwner(_))
        ));
        assert!(matches!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 }),
            Err(RestoreError::MissingOwner(_))
        ));
        assert!(matches!(
            fixture.store().pending(fixture.owner),
            Err(RestoreError::MissingOwner(_))
        ));
        assert!(matches!(
            fixture
                .store()
                .prepare(fixture.owner, fixture.manifest.clone()),
            Err(RestoreError::MissingOwner(_))
        ));
    }

    #[test_case("restores"; "restores_parent")]
    #[test_case("owner"; "owner_parent")]
    #[test_case("transition"; "transition_parent")]
    #[test_case("manifest"; "manifest_file")]
    #[test_case("progress"; "progress_file")]
    #[test_case("lock"; "lock_file")]
    fn symlinks_are_never_followed(component: &str) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let directory = fixture.directory();
        let path = match component {
            "restores" => directory.parent().unwrap().parent().unwrap().to_path_buf(),
            "owner" => directory.parent().unwrap().to_path_buf(),
            "transition" => directory.clone(),
            "manifest" => directory.join(MANIFEST_FILE),
            "progress" => directory.join(PROGRESS_FILE),
            "lock" => directory.parent().unwrap().join(LOCK_FILE),
            _ => unreachable!(),
        };
        let displaced = fixture.root.path().join("displaced");
        fs::rename(&path, &displaced).unwrap();
        symlink(&displaced, &path).unwrap();
        assert!(fixture.store().load(&reference).is_err());
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .is_err()
        );
        assert!(fixture.store().pending(fixture.owner).is_err());
    }

    #[test_case(false; "apply_resolves_conflict")]
    #[test_case(true; "rollback_resolves_conflict_without_applied_record")]
    fn latest_path_outcome_resolves_runtime_conflict(rollback: bool) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let conflict = RestoreProgress::Conflict {
            path: 0,
            actual: FileState::Absent,
        };
        fixture
            .store()
            .append(&reference, conflict.clone())
            .unwrap();
        fixture
            .store()
            .append(&reference, conflict.clone())
            .unwrap();
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Complete)
                .is_err()
        );
        assert_eq!(
            fixture.store().pending(fixture.owner).unwrap(),
            from_ref(&reference)
        );
        assert!(
            fixture
                .store()
                .load(&reference)
                .unwrap()
                .applied_paths()
                .next()
                .is_none()
        );
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::RolledBack { path: 0 })
                .is_err()
        );
        if rollback {
            fixture
                .store()
                .append(&reference, RestoreProgress::RollbackIntent)
                .unwrap();
            fixture
                .store()
                .append(&reference, RestoreProgress::RollbackIntent)
                .unwrap();
            assert!(
                fixture
                    .store()
                    .append(&reference, RestoreProgress::Applied { path: 0 })
                    .is_err()
            );
        }
        let resolved = if rollback {
            RestoreProgress::RolledBack { path: 0 }
        } else {
            RestoreProgress::Applied { path: 0 }
        };
        fixture
            .store()
            .append(&reference, resolved.clone())
            .unwrap();
        fixture
            .store()
            .append(&reference, resolved.clone())
            .unwrap();
        fixture.store().append(&reference, conflict).unwrap();
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Complete)
                .is_err()
        );
        fixture.store().append(&reference, resolved).unwrap();
        let snapshot = fixture.store().load(&reference).unwrap();
        assert_eq!(snapshot.progress.len(), if rollback { 5 } else { 4 });
        assert_eq!(
            snapshot.applied_paths().collect::<Vec<_>>(),
            if rollback { vec![] } else { vec![0] }
        );
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        assert!(fixture.store().pending(fixture.owner).unwrap().is_empty());
    }

    #[test]
    fn rollback_recovers_write_without_applied_progress() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Complete)
                .is_err()
        );
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 1 })
                .is_err()
        );
        fixture
            .store()
            .append(&reference, RestoreProgress::RollbackIntent)
            .unwrap();
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .is_err()
        );
        fixture
            .store()
            .append(&reference, RestoreProgress::RolledBack { path: 0 })
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        assert!(fixture.store().pending(fixture.owner).unwrap().is_empty());
    }

    #[test_case("load"; "load_requires_durable_complete")]
    #[test_case("pending"; "pending_requires_durable_complete")]
    #[test_case("global"; "global_pending_requires_durable_complete")]
    #[test_case("delete"; "delete_requires_durable_complete")]
    #[test_case("public_delete"; "public_delete_requires_durable_complete")]
    #[test_case("public_delete_from"; "public_delete_from_requires_durable_complete")]
    fn failed_complete_sync_cannot_silently_close_recovery(operation: &str) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        FAIL_PROGRESS_SYNC.with(|fail| fail.set(true));
        assert!(matches!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Complete),
            Err(RestoreError::Io(_))
        ));
        FAIL_PROGRESS_SYNC.with(|fail| fail.set(true));
        let result = match operation {
            "load" => fixture.store().load(&reference).map(|_| ()),
            "pending" => fixture.store().pending(fixture.owner).map(|_| ()),
            "global" => fixture
                .store()
                .pending_for_session(fixture.owner)
                .map(|_| ()),
            "delete" => fixture
                .store()
                .guard_session_deletion(fixture.owner)
                .map(|_| ()),
            "public_delete" | "public_delete_from" => {
                let result = if operation == "public_delete" {
                    TestSession::delete(fixture.owner, &fixture.state)
                } else {
                    TestSession::delete_from(fixture.owner, &fixture.root.path().join(SESSIONS_DIR))
                };
                match result {
                    Err(SessionError::Restore(error)) => Err(error),
                    other => panic!("expected restore durability error, got {other:?}"),
                }
            }
            _ => unreachable!(),
        };
        assert!(matches!(result, Err(RestoreError::Io(_))));
        assert!(fixture.owner_log().exists());
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        assert_eq!(
            fixture.store().load(&reference).unwrap().progress.last(),
            Some(&RestoreProgress::Complete)
        );
        assert!(fixture.store().pending(fixture.owner).unwrap().is_empty());
    }

    #[test_case(MANIFEST_FILE; "manifest")]
    #[test_case(PROGRESS_FILE; "progress")]
    fn hardlinked_restore_files_are_rejected_without_outside_writes(name: &str) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let outside = TempDir::new().unwrap();
        let alias = outside.path().join(name);
        fs::hard_link(fixture.directory().join(name), &alias).unwrap();
        let before = fs::read(&alias).unwrap();
        assert!(
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .is_err()
        );
        assert!(
            fixture
                .store()
                .prepare(fixture.owner, fixture.manifest.clone())
                .is_err()
        );
        assert!(fixture.store().load(&reference).is_err());
        assert!(fixture.store().pending(fixture.owner).is_err());
        assert_eq!(fs::read(&alias).unwrap(), before);
    }

    #[test_case(false, false; "delete_pending")]
    #[test_case(true, false; "delete_from_pending")]
    #[test_case(false, true; "delete_completed")]
    #[test_case(true, true; "delete_from_completed")]
    fn public_deletion_preserves_cross_session_restore_dependencies(
        delete_from: bool,
        complete: bool,
    ) {
        let mut fixture = Fixture::new();
        fixture.manifest.target.session = MakimaId::generate();
        let sessions = fixture.root.path().join(SESSIONS_DIR);
        let target_log = sessions.join(format!("{}.jsonl", fixture.manifest.target.session));
        fs::write(&target_log, BEFORE).unwrap();
        let reference = fixture.prepare();
        if complete {
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .unwrap();
            fixture
                .store()
                .append(&reference, RestoreProgress::Complete)
                .unwrap();
        }
        for session in [fixture.owner, fixture.manifest.target.session] {
            let result = if delete_from {
                TestSession::delete_from(session, &sessions)
            } else {
                TestSession::delete(session, &fixture.state)
            };
            assert!(
                matches!(result, Err(SessionError::Restore(RestoreError::Pending(id))) if id == session)
            );
        }
        assert!(fixture.owner_log().exists());
        assert_eq!(fs::read(target_log).unwrap(), BEFORE);
        assert_eq!(
            fixture.store().load(&reference).unwrap().transition,
            fixture.manifest
        );
        for path in &fixture.manifest.paths {
            for state in [&path.before, &path.after] {
                if let FileState::Blob(reference) = state {
                    BlobStore::new(fixture.state.clone())
                        .read(reference)
                        .unwrap();
                }
            }
        }
    }

    #[test_case(false; "delete")]
    #[test_case(true; "delete_from")]
    fn public_deletion_allows_completed_local_restore(delete_from: bool) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        if delete_from {
            TestSession::delete_from(fixture.owner, &fixture.root.path().join(SESSIONS_DIR))
                .unwrap();
        } else {
            TestSession::delete(fixture.owner, &fixture.state).unwrap();
        }
        assert!(!fixture.owner_log().exists());
        assert!(!fixture.directory().exists());
    }

    #[test_case(false, false, "transition"; "current_pending_missing_transition")]
    #[test_case(false, true, "transition"; "current_complete_missing_transition")]
    #[test_case(true, false, "transition"; "branch_pending_missing_transition")]
    #[test_case(true, true, "transition"; "branch_complete_missing_transition")]
    #[test_case(false, true, "owner"; "missing_entire_owner_directory")]
    #[test_case(false, true, "restores"; "missing_entire_restore_store")]
    #[test_case(false, true, "missing_manifest"; "current_missing_manifest")]
    #[test_case(true, true, "missing_manifest"; "branch_missing_manifest")]
    #[test_case(false, true, "missing_progress"; "current_missing_progress")]
    #[test_case(false, true, "manifest"; "current_corrupt_manifest")]
    #[test_case(true, true, "manifest"; "branch_corrupt_manifest")]
    #[test_case(false, true, "progress"; "current_corrupt_progress")]
    #[test_case(true, true, "progress"; "branch_corrupt_progress")]
    fn durable_checkpoint_references_require_valid_transitions(
        branch: bool,
        complete: bool,
        damage: &str,
    ) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        if complete {
            fixture
                .store()
                .append(&reference, RestoreProgress::Applied { path: 0 })
                .unwrap();
            fixture
                .store()
                .append(&reference, RestoreProgress::Complete)
                .unwrap();
        }
        let checkpoint = if branch {
            let directory = fixture
                .root
                .path()
                .join(SESSIONS_DIR)
                .join(super::BRANCHES_DIR)
                .join(fixture.owner.to_string());
            fs::create_dir_all(&directory).unwrap();
            let path = directory.join(format!("{}.jsonl", MakimaId::generate()));
            fs::copy(fixture.owner_log(), &path).unwrap();
            path
        } else {
            fixture.owner_log()
        };
        let record = serde_json::json!({"t": "restore_ref", "d": reference});
        writeln!(
            OpenOptions::new().append(true).open(&checkpoint).unwrap(),
            "{record}"
        )
        .unwrap();
        let before = fs::read(&checkpoint).unwrap();
        assert_eq!(
            fixture
                .store()
                .pending_for_session(fixture.owner)
                .unwrap()
                .is_empty(),
            complete
        );
        match damage {
            "transition" => fs::remove_dir_all(fixture.directory()).unwrap(),
            "owner" => fs::remove_dir_all(fixture.directory().parent().unwrap()).unwrap(),
            "restores" => {
                fs::remove_dir_all(fixture.directory().parent().unwrap().parent().unwrap()).unwrap()
            }
            "manifest" => fs::write(fixture.directory().join(MANIFEST_FILE), b"{}\n").unwrap(),
            "missing_manifest" => fs::remove_file(fixture.directory().join(MANIFEST_FILE)).unwrap(),
            "missing_progress" => fs::remove_file(fixture.directory().join(PROGRESS_FILE)).unwrap(),
            "progress" => fs::write(fixture.directory().join(PROGRESS_FILE), b"{}\n").unwrap(),
            _ => unreachable!(),
        }
        assert!(fixture.store().pending_for_session(fixture.owner).is_err());
        assert_eq!(fs::read(checkpoint).unwrap(), before);
    }

    #[test_case(false, false; "direct_terminated")]
    #[test_case(false, true; "direct_unterminated")]
    #[test_case(true, false; "branch_terminated")]
    #[test_case(true, true; "branch_unterminated")]
    fn checkpoint_reference_forms_block_startup_and_deletion_when_manifest_disappears(
        branch: bool,
        unterminated: bool,
    ) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        let record = if branch {
            serde_json::json!({"t":"branch", "d":{"transition":reference}})
        } else {
            serde_json::json!({"t":"restore_ref", "d":reference})
        };
        let mut bytes = fs::read(fixture.owner_log()).unwrap();
        bytes.extend_from_slice(record.to_string().as_bytes());
        if !unterminated {
            bytes.push(b'\n');
        }
        fs::write(fixture.owner_log(), bytes).unwrap();
        assert!(
            fixture
                .store()
                .pending_for_session(fixture.owner)
                .unwrap()
                .is_empty()
        );
        fs::remove_dir_all(fixture.root.path().join(SESSIONS_DIR).join(RESTORES_DIR)).unwrap();
        assert!(fixture.store().pending_for_session(fixture.owner).is_err());
        assert!(TestSession::delete(fixture.owner, &fixture.state).is_err());
        assert!(fixture.owner_log().exists());
    }

    #[test_case(false; "restore_ref")]
    #[test_case(true; "branch_transition")]
    fn damaged_cross_owner_reference_blocks_deletion_without_loading_unrelated_manifests(
        branch: bool,
    ) {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let other = MakimaId::generate();
        let checkpoint = fixture
            .root
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{other}.jsonl"));
        let record = if branch {
            serde_json::json!({"t":"branch", "d":{"transition":reference}})
        } else {
            serde_json::json!({"t":"restore_ref", "d":reference})
        };
        fs::write(&checkpoint, record.to_string()).unwrap();
        fs::remove_dir_all(fixture.root.path().join(SESSIONS_DIR).join(RESTORES_DIR)).unwrap();
        assert!(matches!(TestSession::delete(fixture.owner, &fixture.state),
            Err(SessionError::Restore(RestoreError::Pending(owner))) if owner == fixture.owner));
        assert!(fixture.owner_log().exists());
        assert!(checkpoint.exists());
        let unrelated = MakimaId::generate();
        let path = fixture
            .root
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{unrelated}.jsonl"));
        fs::write(&path, BEFORE).unwrap();
        TestSession::delete(unrelated, &fixture.state).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn torn_checkpoint_tail_does_not_hide_preceding_reference() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let record = serde_json::json!({"t":"restore_ref", "d":reference});
        let mut file = OpenOptions::new()
            .append(true)
            .open(fixture.owner_log())
            .unwrap();
        writeln!(file, "{record}").unwrap();
        file.write_all(TORN_EVENT).unwrap();
        assert_eq!(
            fixture.store().pending_for_session(fixture.owner).unwrap(),
            [reference]
        );
        fs::remove_dir_all(fixture.directory()).unwrap();
        assert!(fixture.store().pending_for_session(fixture.owner).is_err());
    }

    #[test]
    fn retained_branch_checkpoint_links_are_read_without_rejecting_hardlinks() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        let branches = fixture
            .root
            .path()
            .join(SESSIONS_DIR)
            .join(super::BRANCHES_DIR)
            .join(fixture.owner.to_string());
        fs::create_dir_all(&branches).unwrap();
        let branch = branches.join(format!("{}.jsonl", MakimaId::generate()));
        let record = serde_json::json!({"t": "restore_ref", "d": reference});
        writeln!(
            OpenOptions::new()
                .append(true)
                .open(fixture.owner_log())
                .unwrap(),
            "{record}"
        )
        .unwrap();
        fs::hard_link(fixture.owner_log(), &branch).unwrap();
        assert!(
            fixture
                .store()
                .pending_for_session(fixture.owner)
                .unwrap()
                .is_empty()
        );
        fs::remove_file(branch).unwrap();
        assert!(
            fixture
                .store()
                .pending_for_session(fixture.owner)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn referenced_transition_owned_by_another_session_must_exist() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let target = MakimaId::generate();
        let checkpoint = fixture
            .root
            .path()
            .join(SESSIONS_DIR)
            .join(format!("{target}.jsonl"));
        let record = serde_json::json!({"t": "restore_ref", "d": reference});
        fs::write(checkpoint, format!("{record}\n")).unwrap();
        fs::remove_dir_all(fixture.directory()).unwrap();
        assert!(fixture.store().pending_for_session(target).is_err());
    }

    #[test]
    fn global_pending_matches_both_endpoints_and_rejects_orphans() {
        let mut fixture = Fixture::new();
        let source = MakimaId::generate();
        let target = MakimaId::generate();
        for session in [source, target] {
            fs::write(
                fixture
                    .root
                    .path()
                    .join(SESSIONS_DIR)
                    .join(format!("{session}.jsonl")),
                b"{}\n",
            )
            .unwrap();
        }
        fixture.manifest.source.session = source;
        fixture.manifest.target.session = target;
        let reference = fixture.prepare();
        assert_eq!(
            fixture.store().pending_for_session(source).unwrap(),
            from_ref(&reference)
        );
        assert_eq!(
            fixture.store().pending_for_session(target).unwrap(),
            from_ref(&reference)
        );
        fs::remove_file(
            fixture
                .root
                .path()
                .join(SESSIONS_DIR)
                .join(format!("{source}.jsonl")),
        )
        .unwrap();
        assert_eq!(
            fixture.store().load(&reference).unwrap().transition,
            fixture.manifest
        );
        assert_eq!(
            fixture.store().pending_for_session(source).unwrap(),
            from_ref(&reference)
        );
        let orphan = fixture
            .directory()
            .parent()
            .unwrap()
            .join(MakimaId::generate().to_string());
        fs::create_dir(orphan).unwrap();
        assert!(fixture.store().pending(fixture.owner).is_err());
        assert!(fixture.store().pending_for_session(target).is_err());
        fs::remove_file(fixture.owner_log()).unwrap();
        assert!(matches!(
            fixture.store().pending_for_session(target),
            Err(RestoreError::MissingOwner(owner)) if owner == fixture.owner
        ));
    }

    #[test]
    fn deletion_guard_holds_stable_namespace_lock() {
        let fixture = Fixture::new();
        let sessions = fixture.root.path().join(SESSIONS_DIR);
        let guard = RestoreStore::guard_session_deletion_in(&sessions, fixture.owner).unwrap();
        let contender = OpenOptions::new()
            .read(true)
            .write(true)
            .open(sessions.join(NAMESPACE_LOCK_FILE))
            .unwrap();
        assert!(FileExt::try_lock_exclusive(&contender).is_err());
        fs::remove_file(fixture.owner_log()).unwrap();
        drop(guard);
        FileExt::try_lock_exclusive(&contender).unwrap();
        drop(contender);
        assert!(
            matches!(fixture.store().prepare(fixture.owner, fixture.manifest.clone()),
            Err(RestoreError::MissingOwner(owner)) if owner == fixture.owner)
        );
    }

    #[test]
    fn deletion_guard_retains_completed_cross_session_references() {
        let mut fixture = Fixture::new();
        fixture.manifest.source.session = MakimaId::generate();
        fixture.manifest.target.session = MakimaId::generate();
        let reference = fixture.prepare();
        for session in [
            fixture.owner,
            fixture.manifest.source.session,
            fixture.manifest.target.session,
        ] {
            assert!(matches!(fixture.store().guard_session_deletion(session),
                Err(RestoreError::Pending(id)) if id == session));
        }
        fixture
            .store()
            .append(&reference, RestoreProgress::Applied { path: 0 })
            .unwrap();
        fixture
            .store()
            .append(&reference, RestoreProgress::Complete)
            .unwrap();
        for session in [
            fixture.owner,
            fixture.manifest.source.session,
            fixture.manifest.target.session,
        ] {
            assert!(matches!(fixture.store().guard_session_deletion(session),
                Err(RestoreError::Pending(id)) if id == session));
        }
    }

    #[test]
    fn completed_inverse_reference_prevents_referenced_owner_deletion() {
        let fixture = Fixture::new();
        let original = fixture.prepare();
        fixture
            .store()
            .append(&original, RestoreProgress::Applied { path: 0 })
            .unwrap();
        fixture
            .store()
            .append(&original, RestoreProgress::Complete)
            .unwrap();
        let dependent = MakimaId::generate();
        fs::write(
            fixture
                .root
                .path()
                .join(SESSIONS_DIR)
                .join(format!("{dependent}.jsonl")),
            BEFORE,
        )
        .unwrap();
        let mut inverse = fixture.manifest.clone();
        inverse.id = MakimaId::generate();
        inverse.source.session = dependent;
        inverse.target.session = dependent;
        inverse.inverse_of = Some(original.clone());
        inverse.paths.clear();
        inverse.skipped.clear();
        let inverse = fixture.store().prepare(dependent, inverse).unwrap();
        fixture
            .store()
            .append(&inverse, RestoreProgress::Complete)
            .unwrap();
        assert!(matches!(TestSession::delete(fixture.owner, &fixture.state),
            Err(SessionError::Restore(RestoreError::Pending(owner))) if owner == fixture.owner));
        assert!(fixture.owner_log().exists());
        assert_eq!(
            fixture.store().load(&original).unwrap().transition,
            fixture.manifest
        );
    }

    #[test]
    fn deletion_guard_fails_closed_for_deleted_owner_and_malformed_transitions() {
        let fixture = Fixture::new();
        fixture.prepare();
        fs::write(fixture.directory().join(MANIFEST_FILE), b"{}").unwrap();
        assert!(
            fixture
                .store()
                .guard_session_deletion(MakimaId::generate())
                .is_err()
        );
        fs::remove_file(fixture.owner_log()).unwrap();
        assert!(
            matches!(fixture.store().guard_session_deletion(MakimaId::generate()),
            Err(RestoreError::MissingOwner(owner)) if owner == fixture.owner)
        );
    }

    #[test]
    fn concurrent_duplicate_appends_are_serialized() {
        let fixture = Fixture::new();
        let reference = fixture.prepare();
        let barrier = Arc::new(Barrier::new(3));
        let writers: Vec<_> = (0..2)
            .map(|_| {
                let store = fixture.store();
                let reference = reference.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    store
                        .append(&reference, RestoreProgress::Applied { path: 0 })
                        .unwrap();
                })
            })
            .collect();
        barrier.wait();
        for writer in writers {
            writer.join().unwrap();
        }
        assert_eq!(
            fixture.store().load(&reference).unwrap().progress,
            [RestoreProgress::Applied { path: 0 }]
        );
    }
}
