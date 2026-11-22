use crate::AgentRef;
use crate::ModeRegistry;
use crate::modes::{self, ModeError, ModeId, RestoredMode};
use crate::state::{
    StateAccess, StateEdit, StateError, StateRevision, StateScope, StateSlot, StateSlotSpec,
};
use makima_domain::ModelSelection;
use makima_storage::session_state::StoredModeSnapshot;
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
#[cfg(unix)]
use std::{ffi::OsString, fs::File, io::Write, path::Component};
#[cfg(not(unix))]
use std::{fs, path::Component};
use std::{
    io,
    path::{Path, PathBuf},
};

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
    let Ok(view) = snapshot.fold() else {
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
        return Ok(modes::read_only_fallback(format!(
            "Cannot restore plan file: {error}"
        )));
    }
    let mut snapshot = snapshot.clone();
    if snapshot.id == ModeId::Plan.key()
        && let Some(path) = snapshot.restrict_write_to.as_deref()
    {
        match resolved_plan_path(view, agent, path) {
            Ok(path) => snapshot.restrict_write_to = Some(path),
            Err(error) => {
                return Ok(modes::read_only_fallback(format!(
                    "Cannot restore plan file: {error}"
                )));
            }
        }
    }
    modes::prepare_restore(&snapshot, registry)
}

#[cfg(test)]
mod tests {
    use super::{content_sha256, plan_entries, restore_plan_file};
    use crate::{
        ToolOutput,
        journal::{JournalCommit, SessionJournal},
        state::StateError,
    };
    #[cfg(windows)]
    use crate::{journal::SessionJournalSnapshot, test_util::test_symlink};
    use makima_domain::{ModelSelection, ThinkingConfig};
    use makima_providers::{Message, TokenUsage};
    use makima_storage::session_state::StoredAgentConfig;
    use makima_storage::session_state::{MAX_PLUGIN_VALUE_BYTES, StoredAgentRecord};
    use makima_storage::sessions::Session;
    use makima_storage::test_util::test_record;
    use makima_storage::{
        id::AgentId,
        plans::{ApprovedPlan, PLAN_STATE_KEY, PLAN_STATE_VERSION, PlanEntry, PlanSlot},
        session_state::{SessionStateView, StoredPluginValue, StoredScopeState},
    };
    #[cfg(windows)]
    use std::path::Path;
    use std::{collections::BTreeMap, fs, sync::Arc};
    use test_case::test_case;

    const APPROVED_CONTENT: &str = "# Approved\nPreserve these exact bytes.\n";
    const MUTATED_CONTENT: &str = "disk changed after approval";

    #[cfg(windows)]
    fn approved_plan_snapshot(path: &Path) -> (SessionJournalSnapshot, AgentId) {
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

    #[cfg(windows)]
    #[test_case(Some(APPROVED_CONTENT); "matching_readonly_plan_is_not_rewritten")]
    #[test_case(None; "missing_approved_plan_is_recreated")]
    #[test_case(Some(MUTATED_CONTENT); "changed_approved_plan_is_restored")]
    fn windows_resume_preserves_approved_plan_authority(existing: Option<&str>) {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let path = base.join("nested/plan.md");
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
        let (snapshot, agent) = approved_plan_snapshot(&path);
        let view = snapshot.fold().unwrap();
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
        let base = directory.path().canonicalize().unwrap();
        let path = base.join("plan.md");
        let other = base.join("other.md");
        fs::write(&path, MUTATED_CONTENT).unwrap();
        let (snapshot, agent) = approved_plan_snapshot(&path);
        let mut view = snapshot.fold().unwrap();
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
        assert!(crate::modes::is_read_only_fallback(&restored.mode));
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
        let base = directory.path().canonicalize().unwrap();
        let outside = base.join("outside");
        fs::create_dir(&outside).unwrap();
        let sentinel = outside.join("plan.md");
        fs::write(&sentinel, MUTATED_CONTENT).unwrap();
        let destination = base.join("plans");
        let path = destination.join("plan.md");
        let linked = if parent {
            test_symlink(
                |target, path| symlink_dir(target, path),
                &outside,
                &destination,
            )
        } else {
            fs::create_dir(&destination).unwrap();
            test_symlink(|target, path| symlink_file(target, path), &sentinel, &path)
        };
        if !linked {
            return;
        }
        let (snapshot, agent) = approved_plan_snapshot(&path);
        let view = snapshot.fold().unwrap();
        let mode = &view.scopes[&Some(agent)].config.as_ref().unwrap().mode;
        let restored =
            super::prepare_restore_for_agent(&view, agent, mode, &crate::ModeRegistry::builtin())
                .unwrap();
        assert!(crate::modes::is_read_only_fallback(&restored.mode));
        assert!(restored.warning.is_some());
        assert_eq!(fs::read_to_string(sentinel).unwrap(), MUTATED_CONTENT);
    }

    #[cfg(unix)]
    #[test_case(false; "rewrites_mutated_file")]
    #[test_case(true; "recreates_missing_file")]
    fn restores_exact_approved_bytes(missing: bool) {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let path = base.join("nested/plan.md");
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
        let base = directory.path().canonicalize().unwrap();
        let path = base.join("plan.md");
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
        assert_eq!(crate::modes::is_read_only_fallback(&restored.mode), !build);
        assert_eq!(restored.warning.is_some(), !build);
    }

    #[test_case(false; "active_plan_path_mismatch")]
    #[test_case(true; "failed_build_projection")]
    fn restore_failure_returns_visible_build_without_replacing_slot(build: bool) {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let root = AgentId::generate();
        let path = base.join("plan.md");
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
            crate::AgentMode::Plan(base.join("other.md"))
        };
        let snapshot = registry.current(&mode).stored_snapshot(&mode);
        let before = view.clone();
        let restored = super::prepare_restore_for_agent(&view, root, &snapshot, &registry).unwrap();
        assert!(crate::modes::is_read_only_fallback(&restored.mode));
        assert!(restored.warning.is_some());
        assert_eq!(fs::read_to_string(path).unwrap(), MUTATED_CONTENT);
        assert_eq!(view, before);
    }

    #[test_case(false, true; "approved_relative_root")]
    #[test_case(true, true; "approved_relative_child")]
    #[test_case(false, false; "missing_unapproved_root")]
    #[test_case(true, false; "missing_unapproved_child")]
    fn disk_resume_resolves_agent_cwd_and_never_admits_missing_written_plan(
        child: bool,
        approved: bool,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let cwd = base.join("agent-cwd");
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
                    created_seq: 1,
                    updated_seq: 1,
                    ..test_record(agent, Some(root))
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
        session.save_to(base.as_path()).unwrap();
        let resumed =
            Session::<Message, TokenUsage, ToolOutput>::load_from(session.id, base.as_path())
                .unwrap();
        let view = resumed.latest_state().unwrap();
        let restored =
            super::prepare_restore_for_agent(&view, agent, &stored.mode, &registry).unwrap();
        let absolute = cwd.join(relative);
        if approved {
            assert_eq!(restored.mode.plan_path(), Some(absolute.as_path()));
            assert_eq!(fs::read_to_string(&absolute).unwrap(), APPROVED_CONTENT);
            assert!(restored.warning.is_none());
        } else {
            assert!(crate::modes::is_read_only_fallback(&restored.mode));
            assert!(restored.warning.is_some());
            assert!(!absolute.exists());
        }
        assert_eq!(plan_entries(&view).unwrap()[&agent], entry);
    }

    #[cfg(unix)]
    #[test_case(false; "substituted_plan_symlink")]
    #[test_case(true; "substituted_ancestor_symlink")]
    fn symlink_substitution_returns_read_only_without_overwriting_unrelated_file(ancestor: bool) {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let base = directory.path().canonicalize().unwrap();
        let unrelated = base.join("unrelated");
        fs::create_dir(&unrelated).unwrap();
        let sentinel = unrelated.join("plan.md");
        fs::write(&sentinel, MUTATED_CONTENT).unwrap();
        let destination = base.join("plans");
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
        assert!(crate::modes::is_read_only_fallback(&restored.mode));
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
                agents: vec![test_record(root, None)],
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
