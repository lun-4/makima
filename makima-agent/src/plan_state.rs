use crate::AgentRef;
use crate::journal::{JournalChange, SessionJournalSnapshot};
use crate::modes::{self, ModeError, ModeId, RestoredMode};
use crate::state::{
    StateAccess, StateEdit, StateError, StateRevision, StateScope, StateSlot, StateSlotSpec,
};
use crate::tools::{FileWriteLocks, file_locks::WriteLockGuard};
use crate::{AgentMode, CancelToken, ModeRegistry};
use makima_domain::ModelSelection;
use makima_storage::session_state::{StateChange, StoredModeSnapshot};
#[cfg(not(unix))]
use makima_storage::{StorageError, atomic_write};
use makima_storage::{
    id::AgentId,
    plans::{ApprovedPlan, PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot},
    session_state::SessionStateView,
};
#[cfg(unix)]
use rustix::{
    fs::{self as anchored, AtFlags, FileType, Mode, OFlags},
    io::Errno,
};
use sha2::{Digest, Sha256};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
};
#[cfg(unix)]
use std::{
    ffi::OsString,
    fs::File,
    io::{Read, Write},
    path::Component,
};
#[cfg(not(unix))]
use std::{fs, path::Component};

#[cfg(windows)]
const REPARSE_POINT: u32 = 0x400;
#[cfg(not(unix))]
const UNSAFE_PLAN_PATH: &str = "Plan path contains an unsafe or non-regular component";

pub fn slot(access: &StateAccess) -> Result<StateSlot<PlanSlot>, StateError> {
    access.journal().register_state(StateSlotSpec {
        key: PLAN_STATE_KEY.into(),
        scope: StateScope::Session,
        version: PLAN_STATE_VERSION,
    })
}

pub fn plan_path_matches(target: &AgentRef, stored: &Path, requested: &Path) -> bool {
    same_plan_path(&target.state_access(), stored, requested)
}

fn same_plan_path(access: &StateAccess, stored: &Path, requested: &Path) -> bool {
    if stored == requested {
        return true;
    }
    let snapshot = access.journal().snapshot();
    let Ok(view) = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, u64::MAX)
    else {
        return false;
    };
    match (
        resolved_plan_path(&view, access.target(), stored),
        resolved_plan_path(&view, access.target(), requested),
    ) {
        (Ok(stored), Ok(requested)) => stored == requested,
        _ => false,
    }
}

pub fn allocation_edit(target: &AgentRef, path: &Path) -> Result<Option<StateEdit>, StateError> {
    allocation_edit_with_access(&target.state_access(), path)
}

pub(crate) fn allocation_edit_with_access(
    access: &StateAccess,
    path: &Path,
) -> Result<Option<StateEdit>, StateError> {
    let slot = slot(access)?;
    let current = slot.get(access)?;
    let mut plans = current.value.unwrap_or_default();
    if plans
        .get(&access.target())
        .is_some_and(|entry| same_plan_path(access, &entry.path, path))
    {
        return Ok(None);
    }
    plans.insert(
        access.target(),
        PlanEntry {
            path: path.to_path_buf(),
            written: false,
            approved: None,
            staged_model: None,
        },
    );
    slot.edit(access, Some(plans), Some(current.revision))
        .map(Some)
}

pub fn approval_edit(
    target: &AgentRef,
    path: &Path,
    content: String,
    selection: ModelSelection,
) -> Result<StateEdit, StateError> {
    let access = target.state_access();
    let slot = slot(&access)?;
    let current = slot.get(&access)?;
    let mut plans = current.value.unwrap_or_default();
    let mut entry = plans.remove(&target.id()).unwrap_or_else(|| PlanEntry {
        path: path.to_path_buf(),
        written: true,
        approved: None,
        staged_model: None,
    });
    if !same_plan_path(&access, &entry.path, path) {
        return Err(StateError::Conflict);
    }
    entry.written = true;
    entry.approved = Some(ApprovedPlan {
        sha256: content_sha256(&content),
        content,
    });
    entry.staged_model = (!selection.is_empty()).then_some(selection);
    plans.insert(target.id(), entry);
    slot.edit(&access, Some(plans), Some(current.revision))
}

pub fn set_written(
    target: &AgentRef,
    path: &Path,
    written: bool,
) -> Result<StateRevision, StateError> {
    set_written_with_access(&target.state_access(), path, written)
}

pub(crate) fn set_written_with_access(
    access: &StateAccess,
    path: &Path,
    written: bool,
) -> Result<StateRevision, StateError> {
    update_entry(access, path, |entry| {
        entry.written = written;
        entry.approved = None;
    })
}

pub fn set_staged_model(
    target: &AgentRef,
    path: &Path,
    selection: ModelSelection,
) -> Result<StateRevision, StateError> {
    update_entry(&target.state_access(), path, |entry| {
        entry.staged_model = (!selection.is_empty()).then_some(selection)
    })
}

fn update_entry(
    access: &StateAccess,
    path: &Path,
    update: impl FnOnce(&mut PlanEntry),
) -> Result<StateRevision, StateError> {
    let slot = slot(access)?;
    let current = slot.get(access)?;
    let mut plans = current.value.unwrap_or_default();
    let entry = plans.entry(access.target()).or_insert_with(|| PlanEntry {
        path: path.to_path_buf(),
        written: false,
        approved: None,
        staged_model: None,
    });
    if !same_plan_path(access, &entry.path, path) {
        return Err(StateError::Conflict);
    }
    update(entry);
    slot.set(access, plans, Some(current.revision))
}

pub fn plan_entries(view: &SessionStateView) -> Result<PlanSlot, String> {
    let Some(stored) = view
        .scopes
        .get(&None)
        .and_then(|scope| scope.plugins.get(PLAN_STATE_KEY))
    else {
        return Ok(PlanSlot::default());
    };
    if stored.version != PLAN_STATE_VERSION {
        return Err(format!("Unsupported plan state version {}", stored.version));
    }
    stored.value.clone().map_or_else(
        || Ok(PlanSlot::default()),
        |value| {
            serde_json::from_value(value).map_err(|error| format!("Invalid plan state: {error}"))
        },
    )
}

pub fn content_sha256(content: &str) -> String {
    format!("{:x}", Sha256::digest(content.as_bytes()))
}

pub struct PlanProjectionRollback {
    files: Vec<PlanProjectionFile>,
    rejected: BTreeSet<AgentId>,
    guards: Vec<WriteLockGuard>,
}

#[cfg(unix)]
struct PlanProjectionFile {
    path: PathBuf,
    parent: File,
    anchor: PathBuf,
    missing: Vec<OsString>,
    projected_parent: Option<File>,
    applied: bool,
    before: Option<Vec<u8>>,
    projected: Vec<u8>,
}

#[cfg(not(unix))]
struct PlanProjectionFile {
    path: PathBuf,
    applied: bool,
    before: Option<Vec<u8>>,
    projected: Vec<u8>,
}

impl PlanProjectionRollback {
    pub fn paths(
        snapshot: &SessionJournalSnapshot,
        affected: &BTreeSet<AgentId>,
    ) -> Result<BTreeSet<PathBuf>, String> {
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .map_err(|error| error.to_string())?;
        let plans = plan_entries(&view)?;
        let mut keys = BTreeSet::new();
        for agent in affected {
            let Some(entry) = plans.get(agent).filter(|entry| entry.approved.is_some()) else {
                continue;
            };
            let Some(config) = view
                .scopes
                .get(&Some(*agent))
                .and_then(|scope| scope.config.as_ref())
            else {
                continue;
            };
            let Ok(path) = validated_plan_path(&view, *agent, entry, Some(&config.mode)) else {
                continue;
            };
            keys.insert(FileWriteLocks::lock_key(
                &path.to_string_lossy(),
                Path::new("/"),
            )?);
        }
        Ok(keys)
    }

    pub async fn capture_locked(
        snapshot: &SessionJournalSnapshot,
        affected: &BTreeSet<AgentId>,
        locks: &FileWriteLocks,
        cancel: &CancelToken,
    ) -> Result<Self, String> {
        let guards = locks
            .acquire_all(Self::paths(snapshot, affected)?, cancel)
            .await?;
        if cancel.is_cancelled() {
            return Err(crate::tools::file_locks::CANCELLED.into());
        }
        let mut captured = Self::capture(snapshot, affected)?;
        captured.guards = guards;
        Ok(captured)
    }

    pub fn capture(
        snapshot: &SessionJournalSnapshot,
        affected: &BTreeSet<AgentId>,
    ) -> Result<Self, String> {
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .map_err(|error| error.to_string())?;
        let plans = plan_entries(&view)?;
        let mut files = Vec::new();
        let mut rejected = BTreeSet::new();
        for agent in affected {
            let Some(entry) = plans.get(agent) else {
                continue;
            };
            let Some(approved) = &entry.approved else {
                continue;
            };
            let Some(config) = view
                .scopes
                .get(&Some(*agent))
                .and_then(|scope| scope.config.as_ref())
            else {
                continue;
            };
            let Ok(path) = resolved_plan_path(&view, *agent, &entry.path) else {
                continue;
            };
            if content_sha256(&approved.content) != approved.sha256
                || (config.mode.id == ModeId::Plan.key()
                    && config
                        .mode
                        .restrict_write_to
                        .as_deref()
                        .and_then(|configured| resolved_plan_path(&view, *agent, configured).ok())
                        .as_ref()
                        != Some(&path))
            {
                continue;
            }
            #[cfg(unix)]
            {
                if files
                    .iter()
                    .any(|file: &PlanProjectionFile| file.path == path)
                {
                    continue;
                }
                let (parent, anchor, missing, name) = match capture_plan_parent(&path) {
                    Ok(parent) => parent,
                    Err(error) if unsafe_plan_path(&error) => {
                        rejected.insert(*agent);
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                };
                let before = if missing.is_empty() {
                    match read_plan_in(&parent, &name) {
                        Ok(before) => before,
                        Err(error) if unsafe_plan_path(&error) => {
                            rejected.insert(*agent);
                            continue;
                        }
                        Err(error) => return Err(error.to_string()),
                    }
                } else {
                    None
                };
                files.push(PlanProjectionFile {
                    path,
                    parent,
                    anchor,
                    missing,
                    projected_parent: None,
                    applied: false,
                    before,
                    projected: approved.content.as_bytes().to_vec(),
                });
            }
            #[cfg(not(unix))]
            {
                if files
                    .iter()
                    .any(|file: &PlanProjectionFile| file.path == path)
                {
                    continue;
                }
                let before = match read_legacy_plan_file(&path) {
                    Ok(before) => before,
                    Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                        rejected.insert(*agent);
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                };
                files.push(PlanProjectionFile {
                    path,
                    applied: false,
                    before,
                    projected: approved.content.as_bytes().to_vec(),
                });
            }
        }
        Ok(Self {
            files,
            rejected,
            guards: Vec::new(),
        })
    }

    pub fn project(
        &mut self,
        snapshot: &SessionJournalSnapshot,
        affected: &BTreeSet<AgentId>,
        registry: &ModeRegistry,
    ) -> Result<(Vec<JournalChange>, Vec<String>), String> {
        prepare_rewind_projection_with(snapshot, affected, registry, |view, agent, entry, mode| {
            let path = validated_plan_path(view, agent, entry, Some(mode))?;
            if self.rejected.contains(&agent) {
                return Err("Plan projection path was unsafe at capture".into());
            }
            if let Some(approved) = &entry.approved {
                let file = self
                    .files
                    .iter_mut()
                    .find(|file| file.path == path && file.projected == approved.content.as_bytes())
                    .ok_or_else(|| "Plan projection was not captured".to_owned())?;
                return file.project().map_err(|error| error.to_string());
            }
            let mut entry = entry.clone();
            entry.path = path;
            restore_plan_file(&entry)
        })
    }

    pub fn rollback(self) -> Result<(), String> {
        for file in self.files.into_iter().rev() {
            file.rollback().map_err(|error| error.to_string())?;
        }
        drop(self.guards);
        Ok(())
    }
}

#[cfg(not(unix))]
impl PlanProjectionFile {
    fn project(&mut self) -> io::Result<()> {
        let current = read_legacy_plan_file(&self.path)?;
        if self.applied {
            return if current.as_deref() == Some(self.projected.as_slice()) {
                Ok(())
            } else {
                Err(io::Error::other(
                    "Plan destination changed after projection",
                ))
            };
        }
        if current != self.before {
            return Err(io::Error::other("Plan destination changed after capture"));
        }
        self.applied = true;
        restore_approved_file(&self.path, &self.projected)
    }

    fn rollback(self) -> io::Result<()> {
        if !self.applied {
            return Ok(());
        }
        let current = read_legacy_plan_file(&self.path)?;
        if current == self.before {
            return Ok(());
        }
        if current.as_deref() != Some(self.projected.as_slice()) {
            return Err(io::Error::other(
                "Plan rollback destination changed after projection",
            ));
        }
        match self.before {
            Some(bytes) => restore_approved_file(&self.path, &bytes),
            None => {
                validate_legacy_plan_path(&self.path, false)?;
                fs::remove_file(&self.path)
            }
        }
    }
}

#[cfg(not(unix))]
fn read_legacy_plan_file(path: &Path) -> io::Result<Option<Vec<u8>>> {
    validate_legacy_plan_path(path, false)?;
    let bytes = match fs::read(path) {
        Ok(bytes) => Some(bytes),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    validate_legacy_plan_path(path, false)?;
    Ok(bytes)
}

#[cfg(unix)]
fn unsafe_plan_path(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::InvalidInput
        || error.raw_os_error().is_some_and(|code| {
            code == Errno::LOOP.raw_os_error() || code == Errno::NOTDIR.raw_os_error()
        })
}

#[cfg(unix)]
impl PlanProjectionFile {
    fn project(&mut self) -> io::Result<()> {
        let (anchor, _) = open_plan_parent(&self.anchor.join(".plan-anchor"), false)?
            .ok_or_else(|| io::Error::other("Plan projection ancestor disappeared"))?;
        require_same_directory(&self.parent, &anchor)?;
        if let Some(parent) = &self.projected_parent {
            let (current, _) = open_plan_parent(&self.path, false)?
                .ok_or_else(|| io::Error::other("Plan projection parent disappeared"))?;
            require_same_directory(parent, &current)?;
            let name = self
                .path
                .file_name()
                .ok_or_else(|| io::Error::other("Plan filename is missing"))?
                .to_owned();
            if read_plan_in(&current, &name)?.as_deref() != Some(self.projected.as_slice()) {
                return Err(io::Error::other(
                    "Plan destination changed after projection",
                ));
            }
            return Ok(());
        }
        let mut directory = self.parent.try_clone()?;
        for part in &self.missing {
            match anchored::mkdirat(&directory, part, Mode::RWXU) {
                Ok(()) => {}
                Err(error) => return Err(error.into()),
            }
            directory = File::from(anchored::openat(
                &directory,
                part,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?);
        }
        let name = self
            .path
            .file_name()
            .ok_or_else(|| io::Error::other("Plan filename is missing"))?
            .to_owned();
        if read_plan_in(&directory, &name)? != self.before {
            return Err(io::Error::other("Plan destination changed after capture"));
        }
        self.projected_parent = Some(directory.try_clone()?);
        self.applied = true;
        write_plan_in(&directory, &name, &self.projected)
    }

    fn rollback(self) -> io::Result<()> {
        if !self.applied {
            return Ok(());
        }
        let Some((directory, name)) = open_plan_parent(&self.path, false)? else {
            return Err(io::Error::other("Plan rollback parent disappeared"));
        };
        if let Some(parent) = &self.projected_parent {
            require_same_directory(parent, &directory)?;
        }
        let current = read_plan_in(&directory, &name)?;
        if current == self.before {
            return Ok(());
        }
        if current.as_deref() != Some(self.projected.as_slice()) {
            return Err(io::Error::other(
                "Plan rollback destination changed after projection",
            ));
        }
        match self.before {
            Some(bytes) => write_plan_in(&directory, &name, &bytes),
            None => {
                anchored::unlinkat(&directory, &name, AtFlags::empty())?;
                directory.sync_all()
            }
        }
    }
}

#[cfg(unix)]
fn require_same_directory(expected: &File, actual: &File) -> io::Result<()> {
    let expected = anchored::fstat(expected)?;
    let actual = anchored::fstat(actual)?;
    if expected.st_dev != actual.st_dev || expected.st_ino != actual.st_ino {
        return Err(io::Error::other("Plan projection directory was replaced"));
    }
    Ok(())
}

#[cfg(unix)]
fn capture_plan_parent(path: &Path) -> io::Result<(File, PathBuf, Vec<OsString>, OsString)> {
    let mut parts = path.components();
    if parts.next() != Some(Component::RootDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Plan path must be absolute",
        ));
    }
    let name = parts
        .next_back()
        .filter(|part| matches!(part, Component::Normal(_)))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Plan path has no regular filename",
            )
        })?
        .as_os_str()
        .to_owned();
    let mut directory = File::from(anchored::open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    let mut anchor = PathBuf::from("/");
    let mut missing = Vec::new();
    for part in parts {
        if !matches!(part, Component::Normal(_)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Plan ancestors must not contain parent traversal",
            ));
        }
        if !missing.is_empty() {
            missing.push(part.as_os_str().to_owned());
            continue;
        }
        match anchored::openat(
            &directory,
            part.as_os_str(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(next) => {
                directory = File::from(next);
                anchor.push(part.as_os_str());
            }
            Err(error) if error == Errno::NOENT => missing.push(part.as_os_str().to_owned()),
            Err(error) => return Err(error.into()),
        }
    }
    Ok((directory, anchor, missing, name))
}

#[cfg(unix)]
fn read_plan_in(directory: &File, name: &OsString) -> io::Result<Option<Vec<u8>>> {
    let mut file = match anchored::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(file) => File::from(file),
        Err(error) if error == Errno::NOENT => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if FileType::from_raw_mode(anchored::fstat(&file)?.st_mode) != FileType::RegularFile {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Plan destination is not a regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}

pub fn restore_plan_file(entry: &PlanEntry) -> Result<(), String> {
    if !entry.path.is_absolute() {
        return Err("Relative plan paths require the stored agent working directory".into());
    }
    let Some(approved) = &entry.approved else {
        #[cfg(not(unix))]
        if entry.written {
            validate_legacy_plan_path(&entry.path, false).map_err(|error| error.to_string())?;
        }
        if entry.written && !entry.path.is_file() {
            return Err("Written plan file is missing or is not a regular file".into());
        }
        return Ok(());
    };
    if content_sha256(&approved.content) != approved.sha256 {
        return Err("Approved plan content does not match its SHA256".into());
    }
    restore_approved_file(&entry.path, approved.content.as_bytes())
        .map_err(|error| error.to_string())
}

#[cfg(unix)]
fn restore_approved_file(path: &Path, content: &[u8]) -> io::Result<()> {
    let (directory, name) =
        open_plan_parent(path, true)?.ok_or_else(|| io::Error::other("Plan parent is missing"))?;
    write_plan_in(&directory, &name, content)
}

#[cfg(unix)]
fn open_plan_parent(path: &Path, create: bool) -> io::Result<Option<(File, OsString)>> {
    let mut parts = path.components();
    if parts.next() != Some(Component::RootDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Plan path must be absolute",
        ));
    }
    let name = parts
        .next_back()
        .filter(|part| matches!(part, Component::Normal(_)))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Plan path has no regular filename",
            )
        })?;
    let mut directory = File::from(anchored::open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    for part in parts {
        if !matches!(part, Component::Normal(_)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Plan ancestors must not contain parent traversal",
            ));
        }
        if create {
            match anchored::mkdirat(&directory, part.as_os_str(), Mode::RWXU) {
                Err(error) if error == Errno::EXIST => {}
                result => result?,
            }
        }
        directory = match anchored::openat(
            &directory,
            part.as_os_str(),
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(directory) => File::from(directory),
            Err(error) if error == Errno::NOENT && !create => return Ok(None),
            Err(error) => return Err(error.into()),
        };
    }
    Ok(Some((directory, name.as_os_str().to_owned())))
}

#[cfg(unix)]
fn write_plan_in(directory: &File, name: &OsString, content: &[u8]) -> io::Result<()> {
    let validate = || match anchored::statat(directory, name, AtFlags::SYMLINK_NOFOLLOW) {
        Ok(stat) if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile => Ok(()),
        Err(error) if error == Errno::NOENT => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Plan destination is not a regular file",
        )),
        Err(error) => Err(error.into()),
    };
    validate()?;
    let mut random = [0; 16];
    getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
    let temporary = format!(".plan-{:032x}", u128::from_ne_bytes(random));
    let mut file = File::from(anchored::openat(
        directory,
        temporary.as_str(),
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )?);
    let result = file
        .write_all(content)
        .and_then(|()| file.sync_all())
        .and_then(|()| validate())
        .and_then(|()| {
            anchored::renameat(directory, temporary.as_str(), directory, name)
                .map_err(io::Error::from)
        });
    let cleanup = anchored::unlinkat(directory, temporary.as_str(), AtFlags::empty());
    result?;
    if let Err(error) = cleanup
        && error != Errno::NOENT
    {
        return Err(error.into());
    }
    directory.sync_all()
}

#[cfg(not(unix))]
fn restore_approved_file(path: &Path, content: &[u8]) -> io::Result<()> {
    validate_legacy_plan_path(path, false)?;
    match fs::read(path) {
        Ok(current) if current == content => return validate_legacy_plan_path(path, false),
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    validate_legacy_plan_path(path, true)?;
    validate_legacy_plan_path(path, false)?;
    atomic_write(path, content).map_err(|error| match error {
        StorageError::Io(error) => error,
        error => io::Error::other(error.to_string()),
    })?;
    validate_legacy_plan_path(path, false)
}

#[cfg(not(unix))]
fn validate_legacy_plan_path(path: &Path, create: bool) -> io::Result<()> {
    if !path.is_absolute()
        || path.file_name().is_none()
        || path
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
        || path
            .to_string_lossy()
            .split(['/', '\\'])
            .any(|part| part == ".")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            UNSAFE_PLAN_PATH,
        ));
    }
    let mut current = PathBuf::new();
    for part in path.components() {
        current.push(part.as_os_str());
        if matches!(part, Component::Prefix(_)) {
            continue;
        }
        let destination = current == path;
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !create || destination {
                    continue;
                }
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                fs::symlink_metadata(&current)?
            }
            Err(error) => return Err(error),
        };
        #[cfg(windows)]
        let reparse = metadata.file_attributes() & REPARSE_POINT != 0;
        #[cfg(not(windows))]
        let reparse = false;
        if reparse
            || metadata.file_type().is_symlink()
            || if destination {
                !metadata.is_file()
            } else {
                !metadata.is_dir()
            }
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                UNSAFE_PLAN_PATH,
            ));
        }
    }
    Ok(())
}

pub fn resolved_plan_path(
    view: &SessionStateView,
    agent: AgentId,
    path: &Path,
) -> Result<PathBuf, String> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    let cwd = view
        .scopes
        .get(&Some(agent))
        .and_then(|scope| scope.config.as_ref())
        .map(|config| Path::new(&config.cwd))
        .filter(|cwd| cwd.is_absolute())
        .ok_or_else(|| {
            "Relative plan path has no absolute stored agent working directory".to_owned()
        })?;
    Ok(cwd.join(path))
}

fn project_plan_entry(
    view: &SessionStateView,
    agent: AgentId,
    entry: &PlanEntry,
    snapshot: Option<&StoredModeSnapshot>,
) -> Result<(), String> {
    let path = validated_plan_path(view, agent, entry, snapshot)?;
    let mut projection = entry.clone();
    projection.path = path;
    restore_plan_file(&projection)
}

pub(crate) fn projection_files(
    snapshot: &SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
) -> Result<Vec<(PathBuf, Vec<u8>)>, String> {
    let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
        .map_err(|error| error.to_string())?;
    let plans = plan_entries(&view)?;
    let mut files = BTreeMap::new();
    for agent in affected {
        let Some(entry) = plans.get(agent) else {
            continue;
        };
        let Some(approved) = &entry.approved else {
            continue;
        };
        let Some(config) = view
            .scopes
            .get(&Some(*agent))
            .and_then(|scope| scope.config.as_ref())
        else {
            continue;
        };
        let Ok(path) = validated_plan_path(&view, *agent, entry, Some(&config.mode)) else {
            continue;
        };
        if content_sha256(&approved.content) != approved.sha256 {
            continue;
        }
        let bytes = approved.content.as_bytes().to_vec();
        if files
            .insert(path, bytes.clone())
            .is_some_and(|existing| existing != bytes)
        {
            return Err("Approved plans disagree on the content of a shared path".into());
        }
    }
    Ok(files.into_iter().collect())
}

pub(crate) fn validated_plan_path(
    view: &SessionStateView,
    agent: AgentId,
    entry: &PlanEntry,
    snapshot: Option<&StoredModeSnapshot>,
) -> Result<PathBuf, String> {
    let path = resolved_plan_path(view, agent, &entry.path)?;
    if let Some(snapshot) = snapshot.filter(|snapshot| snapshot.id == ModeId::Plan.key()) {
        let configured = snapshot
            .restrict_write_to
            .as_deref()
            .ok_or_else(|| "Active Plan configuration has no plan path".to_owned())?;
        if resolved_plan_path(view, agent, configured)? != path {
            return Err("Plan slot path does not match the active Plan configuration".into());
        }
    }
    Ok(path)
}

pub fn prepare_restore_for_agent(
    view: &SessionStateView,
    agent: AgentId,
    snapshot: &StoredModeSnapshot,
    registry: &ModeRegistry,
) -> Result<RestoredMode, ModeError> {
    prepare_restore_for_agent_with(view, agent, snapshot, registry, |entry| {
        project_plan_entry(view, agent, entry, Some(snapshot))
    })
}

fn prepare_restore_for_agent_with(
    view: &SessionStateView,
    agent: AgentId,
    snapshot: &StoredModeSnapshot,
    registry: &ModeRegistry,
    project: impl FnOnce(&PlanEntry) -> Result<(), String>,
) -> Result<RestoredMode, ModeError> {
    let projection = plan_entries(view).and_then(|plans| {
        let Some(entry) = plans.get(&agent) else {
            return if snapshot.id == ModeId::Plan.key() {
                Err("Active Plan configuration has no plan slot entry".into())
            } else {
                Ok(())
            };
        };
        project(entry)
    });
    if let Err(error) = projection {
        return Ok(RestoredMode {
            mode: AgentMode::Build,
            definition: registry.current(&AgentMode::Build),
            warning: Some(format!(
                "Cannot restore plan file: {error}. Resuming in Build mode"
            )),
        });
    }
    let mut snapshot = snapshot.clone();
    if snapshot.id == ModeId::Plan.key()
        && let Some(path) = snapshot.restrict_write_to.as_deref()
    {
        match resolved_plan_path(view, agent, path) {
            Ok(path) => snapshot.restrict_write_to = Some(path),
            Err(error) => {
                return Ok(RestoredMode {
                    mode: AgentMode::Build,
                    definition: registry.current(&AgentMode::Build),
                    warning: Some(format!(
                        "Cannot restore plan file: {error}. Resuming in Build mode"
                    )),
                });
            }
        }
    }
    modes::prepare_restore(&snapshot, registry)
}

pub fn prepare_rewind_projection(
    snapshot: &SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
    registry: &ModeRegistry,
) -> Result<(Vec<JournalChange>, Vec<String>), String> {
    prepare_rewind_projection_with(snapshot, affected, registry, |view, agent, entry, mode| {
        project_plan_entry(view, agent, entry, Some(mode))
    })
}

pub(crate) fn prepare_rewind_projection_with(
    snapshot: &SessionJournalSnapshot,
    affected: &BTreeSet<AgentId>,
    registry: &ModeRegistry,
    mut project: impl FnMut(
        &SessionStateView,
        AgentId,
        &PlanEntry,
        &StoredModeSnapshot,
    ) -> Result<(), String>,
) -> Result<(Vec<JournalChange>, Vec<String>), String> {
    let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
        .map_err(|error| error.to_string())?;
    let mut changes = Vec::new();
    let mut warnings = Vec::new();
    for agent in affected {
        let Some(scope) = view.scopes.get(&Some(*agent)) else {
            continue;
        };
        let Some(config) = &scope.config else {
            continue;
        };
        let restored =
            prepare_restore_for_agent_with(&view, *agent, &config.mode, registry, |entry| {
                project(&view, *agent, entry, &config.mode)
            })
            .map_err(|error| error.to_string())?;
        if let Some(warning) = restored.warning {
            warnings.push(warning);
        }
        let mode = restored.definition.stored_snapshot(&restored.mode);
        if mode != config.mode {
            let mut config = config.clone();
            config.mode = mode;
            changes.push(JournalChange {
                agent: Some(*agent),
                generation: scope.generation,
                change: StateChange::Config(config),
            });
        }
    }
    Ok((changes, warnings))
}

pub fn restore_plan_files(view: &SessionStateView) -> Result<Vec<(AgentId, String)>, String> {
    Ok(plan_entries(view)?
        .into_iter()
        .filter_map(|(agent, entry)| {
            let snapshot = view
                .scopes
                .get(&Some(agent))
                .and_then(|scope| scope.config.as_ref())
                .map(|config| &config.mode);
            project_plan_entry(view, agent, &entry, snapshot)
                .err()
                .map(|error| {
                    (
                        agent,
                        format!(
                            "Cannot restore plan file {}: {error}. Resuming in Build mode",
                            entry.path.display()
                        ),
                    )
                })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::{content_sha256, plan_entries, restore_plan_file};
    #[cfg(any(unix, windows))]
    use crate::journal::SessionJournalSnapshot;
    use crate::{
        ToolOutput,
        journal::{JournalCommit, SessionJournal},
        state::StateError,
    };
    use makima_domain::{ModelSelection, ThinkingConfig};
    use makima_providers::{Message, TokenUsage};
    use makima_storage::session_state::StoredAgentConfig;
    use makima_storage::session_state::{
        MAX_PLUGIN_VALUE_BYTES, StoredAgentLifecycle, StoredAgentRecord,
    };
    use makima_storage::sessions::Session;
    use makima_storage::{
        id::AgentId,
        plans::{ApprovedPlan, PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot},
        session_state::{SessionStateView, StoredPluginValue, StoredScopeState},
    };
    #[cfg(any(unix, windows))]
    use std::path::Path;
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        sync::Arc,
    };
    use test_case::test_case;

    const APPROVED_CONTENT: &str = "# Approved\nPreserve these exact bytes.\n";
    const MUTATED_CONTENT: &str = "disk changed after approval";
    #[cfg(windows)]
    const EXTERNAL_PLAN_CONTENT: &str = "user changed projected plan";
    #[cfg(windows)]
    const PLAN_ROLLBACK_CONFLICT: &str = "Plan rollback destination changed after projection";

    #[cfg(any(unix, windows))]
    fn rollback_snapshot(path: &Path) -> (SessionJournalSnapshot, AgentId) {
        let agent = AgentId::generate();
        let registry = crate::ModeRegistry::builtin();
        let mode = crate::AgentMode::Plan(path.to_path_buf());
        let config = StoredAgentConfig {
            cwd: path.parent().unwrap().display().to_string(),
            model: "test-model".into(),
            thinking: None,
            fast: false,
            workflow: false,
            mode: registry.current(&mode).stored_snapshot(&mode),
            options: BTreeMap::new(),
        };
        let entry = PlanEntry {
            path: path.to_path_buf(),
            written: true,
            approved: Some(ApprovedPlan {
                content: APPROVED_CONTENT.into(),
                sha256: content_sha256(APPROVED_CONTENT),
            }),
            staged_model: None,
        };
        let mut session =
            Session::<Message, TokenUsage, ToolOutput>::new("test-model", &config.cwd);
        session.bind_root_id(agent).unwrap();
        session.push_state(makima_storage::session_state::StoredStateRecord {
            seq: 1,
            agent: Some(agent),
            generation: 0,
            change: makima_storage::session_state::StateChange::Config(config),
        });
        session.push_state(makima_storage::session_state::StoredStateRecord {
            seq: 2,
            agent: None,
            generation: 0,
            change: makima_storage::session_state::StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(serde_json::to_value(PlanSlot::from([(agent, entry)])).unwrap()),
            },
        });
        (
            SessionJournal::from_session(&session).unwrap().snapshot(),
            agent,
        )
    }

    #[cfg(unix)]
    #[test_case(false; "release_on_published_drop")]
    #[test_case(true; "release_after_rollback")]
    fn locked_projection_serializes_capture_and_retains_tool_guards(rollback: bool) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("plan.md");
            fs::write(&path, MUTATED_CONTENT).unwrap();
            let (snapshot, agent) = rollback_snapshot(&path);
            let affected = BTreeSet::from([agent]);
            let locks = crate::tools::FileWriteLocks::new();
            let cancel = crate::CancelToken::none();
            let key =
                crate::tools::FileWriteLocks::lock_key(&path.to_string_lossy(), directory.path())
                    .unwrap();
            let writer = locks
                .acquire(key.clone(), &[], &cancel, crate::tools::Deadline::None)
                .await
                .unwrap();
            let mut capture = Box::pin(super::PlanProjectionRollback::capture_locked(
                &snapshot, &affected, &locks, &cancel,
            ));
            assert!(
                futures_lite::future::poll_once(capture.as_mut())
                    .await
                    .is_none()
            );
            fs::write(&path, PLAN_STATE_KEY).unwrap();
            drop(writer);
            let mut token = capture.await.unwrap();
            assert_eq!(
                token.files[0].before.as_deref(),
                Some(PLAN_STATE_KEY.as_bytes())
            );
            let mut writer =
                Box::pin(locks.acquire(key, &[], &cancel, crate::tools::Deadline::None));
            assert!(
                futures_lite::future::poll_once(writer.as_mut())
                    .await
                    .is_none()
            );
            token
                .project(&snapshot, &affected, &crate::ModeRegistry::builtin())
                .unwrap();
            assert!(
                futures_lite::future::poll_once(writer.as_mut())
                    .await
                    .is_none()
            );
            if rollback {
                token.rollback().unwrap();
                assert_eq!(fs::read_to_string(&path).unwrap(), PLAN_STATE_KEY);
            } else {
                drop(token);
                assert_eq!(fs::read_to_string(&path).unwrap(), APPROVED_CONTENT);
            }
            let _writer = writer.await.unwrap();
        });
    }

    #[cfg(unix)]
    #[test]
    fn cancelled_locked_capture_does_not_capture_or_mutate() {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("plan.md");
            fs::write(&path, MUTATED_CONTENT).unwrap();
            let (snapshot, agent) = rollback_snapshot(&path);
            let affected = BTreeSet::from([agent]);
            let locks = crate::tools::FileWriteLocks::new();
            let (trigger, cancel) = crate::CancelToken::new();
            trigger.cancel();
            assert!(
                super::PlanProjectionRollback::capture_locked(
                    &snapshot, &affected, &locks, &cancel
                )
                .await
                .is_err()
            );
            assert_eq!(fs::read_to_string(path).unwrap(), MUTATED_CONTENT);
        });
    }

    #[cfg(unix)]
    #[test_case(false; "unapproved_draft_bytes")]
    #[test_case(true; "absent_file_and_parent")]
    fn aborted_projection_restores_actual_prior_file(missing: bool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/plan.md");
        if !missing {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, MUTATED_CONTENT).unwrap();
        }
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut token =
            super::PlanProjectionRollback::capture(&snapshot, &BTreeSet::from([agent])).unwrap();
        if missing {
            assert!(!path.parent().unwrap().exists());
        } else {
            assert_eq!(fs::read_to_string(&path).unwrap(), MUTATED_CONTENT);
        }
        token
            .project(
                &snapshot,
                &BTreeSet::from([agent]),
                &crate::ModeRegistry::builtin(),
            )
            .unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), APPROVED_CONTENT);
        token.rollback().unwrap();
        if missing {
            assert!(!path.exists());
        } else {
            assert_eq!(fs::read_to_string(&path).unwrap(), MUTATED_CONTENT);
        }
    }

    #[cfg(unix)]
    #[test_case(false; "destination_symlink")]
    #[test_case(true; "ancestor_symlink")]
    fn aborted_projection_rejects_unsafe_substitution(ancestor: bool) {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let external = tempfile::tempdir().unwrap();
        let parent = directory.path().join("nested");
        fs::create_dir(&parent).unwrap();
        let path = parent.join("plan.md");
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let sentinel = external.path().join("plan.md");
        fs::write(&sentinel, APPROVED_CONTENT).unwrap();
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut token =
            super::PlanProjectionRollback::capture(&snapshot, &BTreeSet::from([agent])).unwrap();
        token
            .project(
                &snapshot,
                &BTreeSet::from([agent]),
                &crate::ModeRegistry::builtin(),
            )
            .unwrap();
        if ancestor {
            fs::rename(&parent, directory.path().join("displaced")).unwrap();
            symlink(external.path(), &parent).unwrap();
        } else {
            fs::remove_file(&path).unwrap();
            symlink(&sentinel, &path).unwrap();
        }
        assert!(token.rollback().is_err());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), APPROVED_CONTENT);
    }

    #[cfg(unix)]
    #[test]
    fn aborted_projection_does_not_overwrite_new_external_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.md");
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut token =
            super::PlanProjectionRollback::capture(&snapshot, &BTreeSet::from([agent])).unwrap();
        token
            .project(
                &snapshot,
                &BTreeSet::from([agent]),
                &crate::ModeRegistry::builtin(),
            )
            .unwrap();
        fs::write(&path, PLAN_STATE_KEY).unwrap();
        assert!(token.rollback().is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), PLAN_STATE_KEY);
    }

    #[cfg(unix)]
    #[test_case(false; "directory_replaced_with_file")]
    #[test_case(true; "symlink_replaced_with_file")]
    fn unsafe_capture_cannot_become_untracked_projection(symlink: bool) {
        use std::os::unix::fs::symlink as create_symlink;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.md");
        let external = directory.path().join("external.md");
        fs::write(&external, MUTATED_CONTENT).unwrap();
        if symlink {
            create_symlink(&external, &path).unwrap();
        } else {
            fs::create_dir(&path).unwrap();
        }
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut token =
            super::PlanProjectionRollback::capture(&snapshot, &BTreeSet::from([agent])).unwrap();
        if symlink {
            fs::remove_file(&path).unwrap();
        } else {
            fs::remove_dir(&path).unwrap();
        }
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let (changes, warnings) = token
            .project(
                &snapshot,
                &BTreeSet::from([agent]),
                &crate::ModeRegistry::builtin(),
            )
            .unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(changes.len(), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), MUTATED_CONTENT);
        token.rollback().unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), MUTATED_CONTENT);
    }

    #[cfg(unix)]
    #[test_case(false; "before_projection")]
    #[test_case(true; "after_projection")]
    fn absent_parent_substitution_does_not_delete_unrelated_file(projected: bool) {
        let directory = tempfile::tempdir().unwrap();
        let parent = directory.path().join("nested");
        let path = parent.join("plan.md");
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut token =
            super::PlanProjectionRollback::capture(&snapshot, &BTreeSet::from([agent])).unwrap();
        if projected {
            token
                .project(
                    &snapshot,
                    &BTreeSet::from([agent]),
                    &crate::ModeRegistry::builtin(),
                )
                .unwrap();
            fs::rename(&parent, directory.path().join("displaced")).unwrap();
        }
        fs::create_dir(&parent).unwrap();
        fs::write(&path, APPROVED_CONTENT).unwrap();
        if projected {
            assert!(token.rollback().is_err());
        } else {
            let (_, warnings) = token
                .project(
                    &snapshot,
                    &BTreeSet::from([agent]),
                    &crate::ModeRegistry::builtin(),
                )
                .unwrap();
            assert_eq!(warnings.len(), 1);
            token.rollback().unwrap();
        }
        assert_eq!(fs::read_to_string(path).unwrap(), APPROVED_CONTENT);
    }

    #[cfg(unix)]
    #[test]
    fn rollback_capture_preserves_nonregular_candidate_build_fallback() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.md");
        fs::create_dir(&path).unwrap();
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut token =
            super::PlanProjectionRollback::capture(&snapshot, &BTreeSet::from([agent])).unwrap();
        let (changes, warnings) = token
            .project(
                &snapshot,
                &BTreeSet::from([agent]),
                &crate::ModeRegistry::builtin(),
            )
            .unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(changes.len(), 1);
        token.rollback().unwrap();
        assert!(path.is_dir());
    }

    #[cfg(windows)]
    #[test_case(false, false; "existing_preimage_checkpoint_failure")]
    #[test_case(true, false; "absent_preimage_checkpoint_failure")]
    #[test_case(false, true; "existing_preimage_user_conflict")]
    #[test_case(true, true; "absent_preimage_user_conflict")]
    fn windows_failed_projection_rolls_back_exact_preimage_or_preserves_user_edit(
        missing: bool,
        conflict: bool,
    ) {
        smol::block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("nested/plan.md");
            if !missing {
                fs::create_dir_all(path.parent().unwrap()).unwrap();
                fs::write(&path, MUTATED_CONTENT).unwrap();
            }
            let (snapshot, agent) = rollback_snapshot(&path);
            let affected = BTreeSet::from([agent]);
            let locks = crate::tools::FileWriteLocks::new();
            let cancel = crate::CancelToken::none();
            let mut rollback = super::PlanProjectionRollback::capture_locked(
                &snapshot, &affected, &locks, &cancel,
            )
            .await
            .unwrap();
            let key =
                crate::tools::FileWriteLocks::lock_key(&path.to_string_lossy(), directory.path())
                    .unwrap();
            let mut competing = Box::pin(locks.acquire_all(BTreeSet::from([key]), &cancel));
            assert!(
                futures_lite::future::poll_once(competing.as_mut())
                    .await
                    .is_none()
            );
            let (_, warnings) = rollback
                .project(&snapshot, &affected, &crate::ModeRegistry::builtin())
                .unwrap();
            assert!(warnings.is_empty());
            assert_eq!(fs::read_to_string(&path).unwrap(), APPROVED_CONTENT);
            assert!(
                futures_lite::future::poll_once(competing.as_mut())
                    .await
                    .is_none()
            );
            if conflict {
                fs::write(&path, EXTERNAL_PLAN_CONTENT).unwrap();
            }
            let result = rollback.rollback();
            if conflict {
                assert_eq!(result.unwrap_err(), PLAN_ROLLBACK_CONFLICT);
                assert_eq!(fs::read_to_string(&path).unwrap(), EXTERNAL_PLAN_CONTENT);
            } else {
                result.unwrap();
                assert_eq!(
                    fs::read(&path).ok(),
                    (!missing).then(|| MUTATED_CONTENT.as_bytes().to_vec())
                );
            }
            drop(competing.await.unwrap());
        });
    }

    #[cfg(windows)]
    #[test_case(Some(APPROVED_CONTENT); "matching_readonly_plan_is_not_rewritten")]
    #[test_case(None; "missing_approved_plan_is_recreated")]
    #[test_case(Some(MUTATED_CONTENT); "changed_approved_plan_is_restored")]
    fn windows_resume_preserves_approved_plan_authority(existing: Option<&str>) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/plan.md");
        if let Some(content) = existing {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, content).unwrap();
        }
        let matching = existing == Some(APPROVED_CONTENT);
        let before = matching.then(|| {
            let metadata = fs::metadata(&path).unwrap();
            let original = metadata.permissions();
            let mut permissions = original.clone();
            permissions.set_readonly(true);
            fs::set_permissions(&path, permissions).unwrap();
            (metadata.created().unwrap(), original)
        });
        let (snapshot, agent) = rollback_snapshot(&path);
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .unwrap();
        let config = view.scopes[&Some(agent)].config.as_ref().unwrap();
        let restored = super::prepare_restore_for_agent(
            &view,
            agent,
            &config.mode,
            &crate::ModeRegistry::builtin(),
        )
        .unwrap();
        assert_eq!(restored.mode.plan_path(), Some(path.as_path()));
        assert!(restored.warning.is_none());
        assert_eq!(fs::read_to_string(&path).unwrap(), APPROVED_CONTENT);
        if let Some((created, original)) = before {
            let metadata = fs::metadata(&path).unwrap();
            assert!(metadata.permissions().readonly());
            assert_eq!(metadata.created().unwrap(), created);
            fs::set_permissions(&path, original).unwrap();
        }
    }

    #[cfg(windows)]
    #[test_case(false; "invalid_approval_hash")]
    #[test_case(true; "configured_plan_path_disagrees")]
    fn windows_invalid_plan_projection_preserves_existing_files(path_mismatch: bool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.md");
        let other = directory.path().join("other.md");
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let (snapshot, agent) = rollback_snapshot(&path);
        let mut view =
            SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
                .unwrap();
        if path_mismatch {
            view.scopes
                .get_mut(&Some(agent))
                .unwrap()
                .config
                .as_mut()
                .unwrap()
                .mode
                .restrict_write_to = Some(other.clone());
        } else {
            let mut entries = plan_entries(&view).unwrap();
            entries
                .get_mut(&agent)
                .unwrap()
                .approved
                .as_mut()
                .unwrap()
                .sha256 = content_sha256(MUTATED_CONTENT);
            view.scopes
                .get_mut(&None)
                .unwrap()
                .plugins
                .get_mut(PLAN_STATE_KEY)
                .unwrap()
                .value = Some(serde_json::to_value(entries).unwrap());
        }
        let mode = view.scopes[&Some(agent)]
            .config
            .as_ref()
            .unwrap()
            .mode
            .clone();
        let restored =
            super::prepare_restore_for_agent(&view, agent, &mode, &crate::ModeRegistry::builtin())
                .unwrap();
        assert_eq!(restored.mode, crate::AgentMode::Build);
        assert!(restored.warning.is_some());
        assert_eq!(fs::read_to_string(&path).unwrap(), MUTATED_CONTENT);
        assert!(!other.exists());
    }

    #[cfg(windows)]
    #[test_case(false; "destination_symlink")]
    #[test_case(true; "parent_directory_symlink")]
    fn windows_plan_projection_rejects_reparse_paths(parent: bool) {
        use std::os::windows::fs::{symlink_dir, symlink_file};
        let directory = tempfile::tempdir().unwrap();
        let outside = directory.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("plan.md");
        fs::write(&sentinel, MUTATED_CONTENT).unwrap();
        let destination = directory.path().join("plans");
        let path = destination.join("plan.md");
        if parent {
            symlink_dir(&outside, &destination).unwrap();
        } else {
            fs::create_dir(&destination).unwrap();
            symlink_file(&sentinel, &path).unwrap();
        }
        let (snapshot, agent) = rollback_snapshot(&path);
        let view = SessionStateView::fold(&snapshot.state, &snapshot.agent_revisions, snapshot.seq)
            .unwrap();
        let mode = &view.scopes[&Some(agent)].config.as_ref().unwrap().mode;
        let restored =
            super::prepare_restore_for_agent(&view, agent, mode, &crate::ModeRegistry::builtin())
                .unwrap();
        assert_eq!(restored.mode, crate::AgentMode::Build);
        assert!(restored.warning.is_some());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), MUTATED_CONTENT);
    }

    #[cfg(unix)]
    #[test_case(false; "rewrites_mutated_file")]
    #[test_case(true; "recreates_missing_file")]
    fn restores_exact_approved_bytes(missing: bool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested/plan.md");
        if !missing {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, MUTATED_CONTENT).unwrap();
        }
        let entry = PlanEntry {
            path: path.clone(),
            written: true,
            approved: Some(ApprovedPlan {
                content: APPROVED_CONTENT.into(),
                sha256: content_sha256(APPROVED_CONTENT),
            }),
            staged_model: None,
        };
        restore_plan_file(&entry).unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), APPROVED_CONTENT);
    }

    #[test]
    fn invalid_hash_does_not_replace_disk_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.md");
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let entry = PlanEntry {
            path: path.clone(),
            written: true,
            approved: Some(ApprovedPlan {
                content: APPROVED_CONTENT.into(),
                sha256: content_sha256(MUTATED_CONTENT),
            }),
            staged_model: None,
        };
        assert!(restore_plan_file(&entry).is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), MUTATED_CONTENT);
    }

    #[test_case(false; "missing_active_plan_slot")]
    #[test_case(true; "missing_inactive_plan_slot")]
    fn missing_plan_slot_never_restores_plan_authority(build: bool) {
        let registry = crate::ModeRegistry::builtin();
        let mode = if build {
            crate::AgentMode::Build
        } else {
            crate::AgentMode::Plan("plan.md".into())
        };
        let snapshot = registry.current(&mode).stored_snapshot(&mode);
        let view = SessionStateView::default();
        let restored =
            super::prepare_restore_for_agent(&view, AgentId::generate(), &snapshot, &registry)
                .unwrap();
        assert_eq!(restored.mode, crate::AgentMode::Build);
        assert_eq!(restored.warning.is_some(), !build);
    }

    #[test_case(false; "active_plan_path_mismatch")]
    #[test_case(true; "failed_build_projection")]
    fn restore_failure_returns_visible_build_without_replacing_slot(build: bool) {
        let directory = tempfile::tempdir().unwrap();
        let root = AgentId::generate();
        let path = directory.path().join("plan.md");
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let entry = PlanEntry {
            path: path.clone(),
            written: true,
            approved: Some(ApprovedPlan {
                content: APPROVED_CONTENT.into(),
                sha256: content_sha256(MUTATED_CONTENT),
            }),
            staged_model: None,
        };
        let view = SessionStateView {
            scopes: BTreeMap::from([(
                None,
                StoredScopeState {
                    plugins: BTreeMap::from([(
                        PLAN_STATE_KEY.into(),
                        StoredPluginValue {
                            revision: 1,
                            version: PLAN_STATE_VERSION,
                            value: Some(
                                serde_json::to_value(PlanSlot::from([(root, entry)])).unwrap(),
                            ),
                        },
                    )]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let registry = crate::ModeRegistry::builtin();
        let mode = if build {
            crate::AgentMode::Build
        } else {
            crate::AgentMode::Plan(directory.path().join("other.md"))
        };
        let snapshot = registry.current(&mode).stored_snapshot(&mode);
        let before = view.clone();
        let restored = super::prepare_restore_for_agent(&view, root, &snapshot, &registry).unwrap();
        assert_eq!(restored.mode, crate::AgentMode::Build);
        assert!(restored.warning.is_some());
        assert_eq!(fs::read_to_string(path).unwrap(), MUTATED_CONTENT);
        assert_eq!(view, before);
    }

    #[test_case(false, true, false; "approved_relative_root")]
    #[test_case(true, true, false; "approved_relative_child")]
    #[test_case(false, false, false; "missing_unapproved_root")]
    #[test_case(true, false, false; "missing_unapproved_child")]
    #[test_case(false, true, true; "rewind_approved_relative_root")]
    #[test_case(true, true, true; "rewind_approved_relative_child")]
    #[test_case(false, false, true; "rewind_missing_unapproved_root")]
    #[test_case(true, false, true; "rewind_missing_unapproved_child")]
    fn disk_resume_resolves_agent_cwd_and_never_admits_missing_written_plan(
        child: bool,
        approved: bool,
        rewind: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let cwd = directory.path().join("agent-cwd");
        fs::create_dir_all(&cwd).unwrap();
        let root = AgentId::generate();
        let agent = if child { AgentId::generate() } else { root };
        let relative = "nested/relative-plan.md";
        let mode = crate::AgentMode::Plan(relative.into());
        let registry = crate::ModeRegistry::builtin();
        let stored = StoredAgentConfig {
            cwd: cwd.display().to_string(),
            model: "test-model".into(),
            thinking: None,
            fast: false,
            workflow: false,
            mode: registry.current(&mode).stored_snapshot(&mode),
            options: BTreeMap::new(),
        };
        let entry = PlanEntry {
            path: relative.into(),
            written: true,
            approved: approved.then(|| ApprovedPlan {
                content: APPROVED_CONTENT.into(),
                sha256: content_sha256(APPROVED_CONTENT),
            }),
            staged_model: None,
        };
        let mut session =
            Session::<Message, TokenUsage, ToolOutput>::new("test-model", &stored.cwd);
        session.bind_root_id(root).unwrap();
        if child {
            session.push_agent_revision(
                1,
                StoredAgentRecord {
                    id: agent,
                    parent: Some(root),
                    depth: 1,
                    transcript_key: agent.to_string(),
                    lifecycle: StoredAgentLifecycle::Open,
                    ancestry_unknown: false,
                    label: None,
                    spawned_by_tool_use_id: None,
                    spec: None,
                    restorable: Ok(()),
                    usage: Default::default(),
                    created_seq: 1,
                    updated_seq: 1,
                    legacy: false,
                },
            );
        }
        session.push_state(makima_storage::session_state::StoredStateRecord {
            seq: 2,
            agent: Some(agent),
            generation: 0,
            change: makima_storage::session_state::StateChange::Config(stored.clone()),
        });
        session.push_state(makima_storage::session_state::StoredStateRecord {
            seq: 3,
            agent: None,
            generation: 0,
            change: makima_storage::session_state::StateChange::Plugin {
                key: PLAN_STATE_KEY.into(),
                version: PLAN_STATE_VERSION,
                value: Some(
                    serde_json::to_value(PlanSlot::from([(agent, entry.clone())])).unwrap(),
                ),
            },
        });
        session.save_to(directory.path()).unwrap();
        let resumed =
            Session::<Message, TokenUsage, ToolOutput>::load_from(session.id, directory.path())
                .unwrap();
        let view = resumed.latest_state().unwrap();
        let restored = if rewind {
            let snapshot = SessionJournal::from_session(&resumed).unwrap().snapshot();
            let (changes, warnings) =
                super::prepare_rewind_projection(&snapshot, &BTreeSet::from([agent]), &registry)
                    .unwrap();
            assert_eq!(warnings.is_empty(), approved);
            assert_eq!(changes.len(), 1);
            let change = &changes[0];
            assert_eq!(change.agent, Some(agent));
            let makima_storage::session_state::StateChange::Config(config) = &change.change else {
                panic!("rewind projection must only change configuration");
            };
            let mut expected = stored.clone();
            expected.mode = config.mode.clone();
            assert_eq!(*config, expected);
            crate::modes::prepare_restore(&config.mode, &registry).unwrap()
        } else {
            super::prepare_restore_for_agent(&view, agent, &stored.mode, &registry).unwrap()
        };
        let absolute = cwd.join(relative);
        if approved {
            assert_eq!(restored.mode.plan_path(), Some(absolute.as_path()));
            assert_eq!(fs::read_to_string(&absolute).unwrap(), APPROVED_CONTENT);
            assert!(restored.warning.is_none());
        } else {
            assert_eq!(restored.mode, crate::AgentMode::Build);
            assert!(rewind || restored.warning.is_some());
            assert!(!absolute.exists());
        }
        assert_eq!(plan_entries(&view).unwrap()[&agent], entry);
    }

    #[cfg(unix)]
    #[test_case(false; "substituted_plan_symlink")]
    #[test_case(true; "substituted_ancestor_symlink")]
    fn symlink_substitution_returns_build_without_overwriting_unrelated_file(ancestor: bool) {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let unrelated = directory.path().join("unrelated");
        fs::create_dir(&unrelated).unwrap();
        let sentinel = unrelated.join("plan.md");
        fs::write(&sentinel, MUTATED_CONTENT).unwrap();
        let destination = directory.path().join("plans");
        let path = destination.join("plan.md");
        if ancestor {
            symlink(&unrelated, &destination).unwrap();
        } else {
            fs::create_dir(&destination).unwrap();
            symlink(&sentinel, &path).unwrap();
        }
        let root = AgentId::generate();
        let mode = crate::AgentMode::Plan(path.clone());
        let registry = crate::ModeRegistry::builtin();
        let snapshot = registry.current(&mode).stored_snapshot(&mode);
        let view = SessionStateView {
            scopes: BTreeMap::from([(
                None,
                StoredScopeState {
                    plugins: BTreeMap::from([(
                        PLAN_STATE_KEY.into(),
                        StoredPluginValue {
                            revision: 1,
                            version: PLAN_STATE_VERSION,
                            value: Some(
                                serde_json::to_value(PlanSlot::from([(
                                    root,
                                    PlanEntry {
                                        path,
                                        written: true,
                                        approved: Some(ApprovedPlan {
                                            content: APPROVED_CONTENT.into(),
                                            sha256: content_sha256(APPROVED_CONTENT),
                                        }),
                                        staged_model: None,
                                    },
                                )]))
                                .unwrap(),
                            ),
                        },
                    )]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        let before = view.clone();
        let restored = super::prepare_restore_for_agent(&view, root, &snapshot, &registry).unwrap();
        assert_eq!(restored.mode, crate::AgentMode::Build);
        assert!(restored.warning.is_some());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), MUTATED_CONTENT);
        assert_eq!(view, before);
    }

    #[test]
    fn oversized_full_plan_slot_is_rejected_without_mutation() {
        let journal = Arc::new(SessionJournal::default());
        let root = AgentId::generate();
        journal
            .commit(JournalCommit {
                agents: vec![StoredAgentRecord {
                    id: root,
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
                ..Default::default()
            })
            .unwrap();
        let access = journal.state_access(root, None);
        let slot = super::slot(&access).unwrap();
        let content = "x".repeat(MAX_PLUGIN_VALUE_BYTES);
        let plans = PlanSlot::from([(
            root,
            PlanEntry {
                path: "plan.md".into(),
                written: true,
                approved: Some(ApprovedPlan {
                    sha256: content_sha256(&content),
                    content,
                }),
                staged_model: None,
            },
        )]);
        let before = journal.snapshot().revision;
        assert!(matches!(
            slot.edit(&access, Some(plans), None),
            Err(StateError::InvalidValue(_))
        ));
        assert_eq!(journal.snapshot().revision, before);
        assert!(slot.get(&access).unwrap().value.is_none());
    }

    #[test]
    fn root_and_child_stages_roundtrip_independently() {
        let root = AgentId::generate();
        let child = AgentId::generate();
        let plans = PlanSlot::from([
            (
                root,
                PlanEntry {
                    path: "root.md".into(),
                    written: true,
                    approved: None,
                    staged_model: Some(ModelSelection {
                        spec: Some("provider/root".into()),
                        thinking: Some(ThinkingConfig::Off),
                        fast: Some(false),
                    }),
                },
            ),
            (
                child,
                PlanEntry {
                    path: "child.md".into(),
                    written: false,
                    approved: None,
                    staged_model: Some(ModelSelection {
                        spec: None,
                        thinking: None,
                        fast: Some(true),
                    }),
                },
            ),
        ]);
        let view = SessionStateView {
            scopes: BTreeMap::from([(
                None,
                StoredScopeState {
                    plugins: BTreeMap::from([(
                        PLAN_STATE_KEY.into(),
                        StoredPluginValue {
                            revision: 1,
                            version: PLAN_STATE_VERSION,
                            value: Some(serde_json::to_value(&plans).unwrap()),
                        },
                    )]),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        };
        assert_eq!(plan_entries(&view).unwrap(), plans);
    }
}
