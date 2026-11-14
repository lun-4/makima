//! Registry of agent modes: built-in `build` and `plan` plus plugin-defined
//! modes and full overrides of the built-ins. The UI, Lua host, CLI, SDK and
//! ACP all share one `Arc<ModeRegistry>`.

#[cfg(unix)]
use libc::O_NONBLOCK;
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use makima_storage::StateDir;
use makima_storage::plans::new_plan_path;
use makima_storage::session_state::StoredModeSnapshot;
use makima_storage::sessions::{SessionMeta, StoredMode};

use crate::AgentMode;
use crate::prompt::PLAN_PROMPT;

const RESTORE_BUILD_FALLBACK: &str = "Resuming in Build mode.";
const PLAN_NOT_REGULAR: &str = "plan path is not a regular file";

/// Identity of an agent mode. Custom modes are keyed by a snake_case name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ModeId {
    Build,
    Plan,
    Custom(Arc<str>),
}

impl ModeId {
    /// Stable registry key: "build", "plan", or the custom name.
    pub fn key(&self) -> &str {
        match self {
            Self::Build => "build",
            Self::Plan => "plan",
            Self::Custom(name) => name,
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "build" => Self::Build,
            "plan" => Self::Plan,
            name => Self::Custom(Arc::from(name)),
        }
    }
}

impl std::fmt::Display for ModeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.key())
    }
}

/// A fully resolved mode definition. Label is the badge text; an override of
/// a built-in replaces the whole def.
#[derive(Debug, Clone, PartialEq)]
pub struct ModeDef {
    pub id: ModeId,
    pub label: Arc<str>,
    /// System-prompt snippet appended verbatim (like `PLAN_PROMPT`). Vars are
    /// applied at prompt-build time.
    pub system_prompt: Option<String>,
    /// Restricts bundled native mutation tools to this target path.
    pub restrict_write_to: Option<PathBuf>,
    /// When `Some`, the mode swaps in exactly this toolset. `None` inherits
    /// the default (build) set.
    pub tools: Option<Vec<String>>,
}

impl ModeDef {
    pub fn stored_snapshot(&self, mode: &AgentMode) -> StoredModeSnapshot {
        let mut snapshot = StoredModeSnapshot::from(self);
        snapshot.restrict_write_to = mode
            .plan_path()
            .map(Path::to_path_buf)
            .or(snapshot.restrict_write_to);
        snapshot
    }

    pub fn default_for(id: ModeId) -> Self {
        let (label, system_prompt) = match &id {
            ModeId::Build => ("[BUILD]".into(), None),
            ModeId::Plan => ("[PLAN]".into(), Some(PLAN_PROMPT.to_owned())),
            ModeId::Custom(name) => {
                let upper = name.to_ascii_uppercase();
                (format!("[{upper}]").into(), None)
            }
        };
        Self {
            id,
            label,
            system_prompt,
            restrict_write_to: None,
            tools: None,
        }
    }
}

impl From<&ModeDef> for StoredModeSnapshot {
    fn from(def: &ModeDef) -> Self {
        Self {
            id: def.id.key().to_owned(),
            label: def.label.to_string(),
            system_prompt: def.system_prompt.clone(),
            restrict_write_to: def.restrict_write_to.clone(),
            tools: def.tools.clone(),
        }
    }
}

impl From<&StoredModeSnapshot> for ModeDef {
    fn from(snapshot: &StoredModeSnapshot) -> Self {
        Self {
            id: ModeId::parse(&snapshot.id),
            label: snapshot.label.as_str().into(),
            system_prompt: snapshot.system_prompt.clone(),
            restrict_write_to: snapshot.restrict_write_to.clone(),
            tools: snapshot.tools.clone(),
        }
    }
}

/// The user-facing shape from Lua/SDK: name plus optional fields to define or
/// fully override a mode.
#[derive(Debug, Clone, Default)]
pub struct ModeDefSpec {
    pub name: String,
    pub label: Option<Arc<str>>,
    pub system_prompt: Option<String>,
    pub restrict_write_to: Option<PathBuf>,
    pub tools: Option<Vec<String>>,
}

impl ModeDefSpec {
    /// Builds a def, filling defaults for any absent field. Name is validated
    /// (snake_case, non-empty, not a reserved conflict).
    pub fn build(self) -> Result<ModeDef, ModeError> {
        if self.name.is_empty() {
            return Err(ModeError::EmptyName);
        }
        let id = ModeId::parse(&self.name);
        let mut def = ModeDef::default_for(id);
        if let Some(label) = self.label {
            def.label = label;
        }
        def.system_prompt = self.system_prompt;
        def.restrict_write_to = self.restrict_write_to;
        def.tools = self.tools;
        Ok(def)
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ModeError {
    #[error("mode name must be non-empty")]
    EmptyName,
    #[error("unknown mode '{0}'")]
    Unknown(String),
    #[error("cannot allocate a plan file: {0}")]
    PlanPath(Arc<str>),
    #[error("stored plan mode has no write restriction")]
    MissingPlanPath,
}

pub struct RestoredMode {
    pub mode: AgentMode,
    pub definition: ModeDef,
    pub warning: Option<String>,
}

pub fn prepare_restore(
    snapshot: &StoredModeSnapshot,
    registry: &ModeRegistry,
) -> Result<RestoredMode, ModeError> {
    let (mode, definition) = from_snapshot(snapshot)?;
    if let Some(path) = mode.plan_path()
        && let Err(error) = prepare_plan_file(path)
    {
        return Ok(RestoredMode {
            mode: AgentMode::Build,
            definition: registry.current(&AgentMode::Build),
            warning: Some(format!(
                "Cannot restore plan file {}: {error}. {RESTORE_BUILD_FALLBACK}",
                path.display()
            )),
        });
    }
    Ok(RestoredMode {
        mode,
        definition,
        warning: None,
    })
}

fn prepare_plan_file(path: &Path) -> io::Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let mut options = OpenOptions::new();
    options.write(true);
    #[cfg(unix)]
    options.custom_flags(O_NONBLOCK);
    let file = match options.clone().create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => options.open(path)?,
        Err(error) => return Err(error),
    };
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(ErrorKind::InvalidInput, PLAN_NOT_REGULAR));
    }
    Ok(())
}

pub fn from_snapshot(snapshot: &StoredModeSnapshot) -> Result<(AgentMode, ModeDef), ModeError> {
    let def = ModeDef::from(snapshot);
    let mode = match &def.id {
        ModeId::Build => AgentMode::Build,
        ModeId::Plan => AgentMode::Plan(
            def.restrict_write_to
                .clone()
                .ok_or(ModeError::MissingPlanPath)?,
        ),
        ModeId::Custom(_) => AgentMode::Custom(def.id.clone()),
    };
    Ok((mode, def))
}

/// The one step from a mode name to its definition, for every frontend and
/// Lua. `plan` keeps {current}'s plan file or allocates a fresh one under
/// {state_dir}. Guest modes (#24) replace the registry lookup here, and the
/// native build and plan arms are the only ones that move to plugins.
pub fn resolve(
    registry: &ModeRegistry,
    key: &str,
    current: &AgentMode,
    state_dir: &StateDir,
) -> Result<(AgentMode, ModeDef), ModeError> {
    let def = registry
        .get(&ModeId::parse(key))
        .ok_or_else(|| ModeError::Unknown(key.to_owned()))?;
    let mode = match &def.id {
        ModeId::Build => AgentMode::Build,
        ModeId::Plan => AgentMode::Plan(match current.plan_path() {
            Some(path) => path.to_path_buf(),
            None => new_plan_path(state_dir)
                .map_err(|error| ModeError::PlanPath(error.to_string().into()))?,
        }),
        ModeId::Custom(_) => AgentMode::Custom(def.id.clone()),
    };
    Ok((mode, def))
}

/// The mode key a session was saved in and its plan file while that still
/// exists; `None` for build.
pub fn saved(meta: &SessionMeta) -> Option<(&str, Option<&Path>)> {
    let key = match &meta.mode {
        Some(StoredMode::Plan) => ModeId::Plan.key(),
        Some(StoredMode::Custom(name)) => name.as_str(),
        Some(StoredMode::Build) | None => return None,
    };
    Some((key, None))
}

/// The mode a saved session resumes in, through [`resolve`]: a missing custom
/// mode is an error the caller answers by starting in build mode.
pub fn restored(
    registry: &ModeRegistry,
    meta: &SessionMeta,
    state_dir: &StateDir,
) -> Result<(AgentMode, ModeDef), ModeError> {
    let Some((key, plan)) = saved(meta) else {
        return Ok((AgentMode::Build, registry.current(&AgentMode::Build)));
    };
    let current = plan.map_or(AgentMode::Build, |plan| AgentMode::Plan(plan.to_path_buf()));
    resolve(registry, key, &current, state_dir)
}

/// Shared, thread-safe store of mode definitions.
#[derive(Debug)]
pub struct ModeRegistry {
    modes: RwLock<HashMap<String, ModeDef>>,
}

impl ModeRegistry {
    /// Registers the built-in `build` and `plan` modes.
    pub fn builtin() -> Self {
        let mut map = HashMap::new();
        for id in [ModeId::Build, ModeId::Plan] {
            let key = id.key().to_owned();
            map.insert(key, ModeDef::default_for(id));
        }
        Self {
            modes: RwLock::new(map),
        }
    }

    /// Insert or replace a mode (override). `build`/`plan` are replaced; a new
    /// name defines a custom mode.
    pub fn define(&self, spec: ModeDefSpec) -> Result<(), ModeError> {
        self.insert(spec.build()?);
        Ok(())
    }

    /// Restore a built-in's default def, dropping any plugin override.
    pub fn reset(&self, id: &ModeId) -> Result<(), ModeError> {
        if let ModeId::Custom(name) = id {
            return Err(ModeError::Unknown(name.to_string()));
        }
        self.insert(ModeDef::default_for(id.clone()));
        Ok(())
    }

    fn insert(&self, def: ModeDef) {
        self.modes
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(def.id.key().to_owned(), def);
    }

    pub fn get(&self, id: &ModeId) -> Option<ModeDef> {
        self.modes
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(id.key())
            .cloned()
    }

    /// Ordered list of all defined modes (built-ins first, then custom).
    pub fn list(&self) -> Vec<ModeDef> {
        let map = self.modes.read().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<ModeDef> = map.values().cloned().collect();
        out.sort_by_key(|d| match d.id {
            ModeId::Build => 0,
            ModeId::Plan => 1,
            ModeId::Custom(_) => 2,
        });
        out
    }

    /// Resolve the active mode's full def; falls back to `build` when a custom
    /// mode is not (re)defined.
    pub fn current(&self, active: &AgentMode) -> ModeDef {
        let id = active.id();
        self.get(&id).unwrap_or_else(|| {
            ModeDef::default_for(match id {
                ModeId::Custom(_) => ModeId::Build,
                builtin => builtin,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use crate::agent::build_mode_system_prompt;
    use crate::template::Vars;
    use crate::tools::test_support::stub_ctx;

    use super::*;

    fn reg() -> ModeRegistry {
        ModeRegistry::builtin()
    }

    #[test]
    fn builtin_registers_build_and_plan() {
        let r = reg();
        assert_eq!(r.list().len(), 2);
        assert!(r.get(&ModeId::Build).is_some());
        assert!(r.get(&ModeId::Plan).is_some());
    }

    #[test]
    fn builtin_plan_carries_plan_prompt() {
        let r = reg();
        let plan = r.get(&ModeId::Plan).unwrap();
        assert!(
            plan.system_prompt
                .as_deref()
                .unwrap_or("")
                .contains("Plan Mode")
        );
        assert_eq!(plan.label.as_ref(), "[PLAN]");
    }

    #[test]
    fn define_inserts_custom_mode() {
        let r = reg();
        r.define(ModeDefSpec {
            name: "audit".into(),
            label: Some("[AUDIT]".into()),
            system_prompt: Some("Audit rules".into()),
            ..Default::default()
        })
        .unwrap();
        let m = r.get(&ModeId::Custom("audit".into())).unwrap();
        assert_eq!(m.system_prompt.as_deref(), Some("Audit rules"));
        assert_eq!(m.label.as_ref(), "[AUDIT]");
    }

    #[test]
    fn define_overrides_builtin_plan() {
        let r = reg();
        r.define(ModeDefSpec {
            name: "plan".into(),
            system_prompt: Some("Override".into()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            r.get(&ModeId::Plan).unwrap().system_prompt.as_deref(),
            Some("Override")
        );
        assert_eq!(r.list().len(), 2);
    }

    #[test]
    fn reset_restores_builtin() {
        let r = reg();
        r.define(ModeDefSpec {
            name: "plan".into(),
            system_prompt: Some("Override".into()),
            ..Default::default()
        })
        .unwrap();
        r.reset(&ModeId::Plan).unwrap();
        assert!(
            r.get(&ModeId::Plan)
                .unwrap()
                .system_prompt
                .as_deref()
                .unwrap_or("")
                .contains("Plan Mode")
        );
    }

    #[test]
    fn current_falls_back_to_build_when_custom_missing() {
        let r = reg();
        let def = r.current(&AgentMode::Custom(ModeId::Custom("ghost".into())));
        assert_eq!(def.id, ModeId::Build);
    }

    const AUDIT_MODE: &str = "audit";
    const GHOST_MODE: &str = "ghost";
    const KEPT_PLAN: &str = "/kept/plan.md";
    const SNAPSHOT_LABEL: &str = "[SAVED]";
    const SNAPSHOT_PROMPT: &str = "Saved mode instructions";
    const SNAPSHOT_TARGET: &str = "/saved/target.md";
    const SNAPSHOT_TOOL: &str = "read";

    #[test_case("build", None ; "build_inherits_tools")]
    #[test_case("build", Some(Vec::new()) ; "build_no_tools")]
    #[test_case("plan", None ; "plan_inherits_tools")]
    #[test_case("plan", Some(Vec::new()) ; "plan_no_tools")]
    #[test_case(AUDIT_MODE, None ; "custom_inherits_tools")]
    #[test_case(AUDIT_MODE, Some(Vec::new()) ; "custom_no_tools")]
    #[test_case(AUDIT_MODE, Some(vec![SNAPSHOT_TOOL.into()]) ; "custom_restricted_tools")]
    fn stored_mode_snapshot_survives_definition_change(key: &str, tools: Option<Vec<String>>) {
        let registry = reg();
        registry
            .define(ModeDefSpec {
                name: key.into(),
                label: Some(SNAPSHOT_LABEL.into()),
                system_prompt: Some(SNAPSHOT_PROMPT.into()),
                restrict_write_to: Some(SNAPSHOT_TARGET.into()),
                tools: tools.clone(),
            })
            .unwrap();
        let def = registry.get(&ModeId::parse(key)).unwrap();
        let snapshot = StoredModeSnapshot::from(&def);
        registry
            .define(ModeDefSpec {
                name: key.into(),
                ..Default::default()
            })
            .unwrap();

        let (mode, restored) = from_snapshot(&snapshot).unwrap();
        assert_eq!(restored, def);
        assert_eq!(mode.id(), def.id);
        assert_ne!(registry.current(&mode), restored);
        assert_eq!(restored.tools, tools);
        assert_eq!(restored.system_prompt.as_deref(), Some(SNAPSHOT_PROMPT));
        assert_eq!(restored.label.as_ref(), SNAPSHOT_LABEL);
        assert_eq!(
            restored.restrict_write_to.as_deref(),
            Some(Path::new(SNAPSHOT_TARGET))
        );
        if key == "plan" {
            assert_eq!(mode.plan_path(), Some(Path::new(SNAPSHOT_TARGET)));
        }
    }

    #[test_case(ModeId::Build ; "build")]
    #[test_case(ModeId::Custom(AUDIT_MODE.into()) ; "custom")]
    fn stored_snapshot_preserves_absent_restrictions(id: ModeId) {
        let def = ModeDef::default_for(id.clone());
        let mode = match id {
            ModeId::Build => AgentMode::Build,
            _ => AgentMode::Custom(id),
        };
        let snapshot = def.stored_snapshot(&mode);
        let (restored_mode, restored_def) = from_snapshot(&snapshot).unwrap();

        assert_eq!(restored_mode, mode);
        assert_eq!(restored_def, def);
        assert_eq!(snapshot.restrict_write_to, None);
        assert_eq!(snapshot.tools, None);
    }

    #[test]
    fn stored_mode_snapshot_restores_deleted_definition() {
        let def = ModeDefSpec {
            name: GHOST_MODE.into(),
            system_prompt: Some(SNAPSHOT_PROMPT.into()),
            restrict_write_to: Some(SNAPSHOT_TARGET.into()),
            tools: Some(Vec::new()),
            ..Default::default()
        }
        .build()
        .unwrap();
        let snapshot = StoredModeSnapshot::from(&def);
        let registry = reg();
        assert!(registry.get(&def.id).is_none());

        let (mode, restored) = from_snapshot(&snapshot).unwrap();
        assert_eq!(mode, AgentMode::Custom(def.id.clone()));
        assert_eq!(restored, def);
        assert_eq!(registry.current(&mode).id, ModeId::Build);
    }

    #[test_case(None ; "uses_effective_plan_path")]
    #[test_case(Some(SNAPSHOT_TARGET.into()) ; "dynamic_plan_path_overrides_definition")]
    fn stored_snapshot_captures_effective_plan_restriction(restrict_write_to: Option<PathBuf>) {
        let mut def = ModeDef::default_for(ModeId::Plan);
        def.restrict_write_to = restrict_write_to;
        let mode = AgentMode::Plan(KEPT_PLAN.into());
        let mut original_ctx = stub_ctx(&mode);
        original_ctx.mode_def = Some(Arc::new(def.clone()));
        let vars = Vars::new();
        let original_prompt = build_mode_system_prompt(&vars, &def, &mode);
        let snapshot = def.stored_snapshot(&mode);
        let (restored_mode, restored_def) = from_snapshot(&snapshot).unwrap();
        let mut restored_ctx = stub_ctx(&restored_mode);
        restored_ctx.mode_def = Some(Arc::new(restored_def.clone()));
        let restored_prompt = build_mode_system_prompt(&vars, &restored_def, &restored_mode);

        assert_eq!(snapshot.restrict_write_to, Some(KEPT_PLAN.into()));
        assert_eq!(restored_mode, mode);
        assert_eq!(original_ctx.restrict_write_to(), Some(KEPT_PLAN.into()));
        assert_eq!(restored_ctx.restrict_write_to(), Some(KEPT_PLAN.into()));
        assert_eq!(restored_prompt, original_prompt);
        assert!(restored_prompt.contains(KEPT_PLAN));
        assert!(!restored_prompt.contains(SNAPSHOT_TARGET));
        assert_eq!(restored_def.restrict_write_to, Some(KEPT_PLAN.into()));
        assert_eq!(restored_def.system_prompt, def.system_prompt);
        assert_eq!(restored_def.tools, def.tools);
    }

    #[test]
    fn stored_plan_snapshot_does_not_reallocate_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing.md");
        let mode = AgentMode::Plan(path.clone());
        let def = ModeDef::default_for(ModeId::Plan);
        let snapshot = def.stored_snapshot(&mode);

        let (restored_mode, _) = from_snapshot(&snapshot).unwrap();
        assert_eq!(restored_mode, mode);
        assert!(!path.exists());
    }

    #[test_case(false; "missing_plan")]
    #[test_case(true; "existing_plan")]
    fn prepare_restore_keeps_saved_plan_and_preserves_contents(existing: bool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("nested").join("plan.md");
        if existing {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, SNAPSHOT_PROMPT).unwrap();
        }
        let mode = AgentMode::Plan(path.clone());
        let definition = ModeDef::default_for(ModeId::Plan);
        let snapshot = definition.stored_snapshot(&mode);
        let restored = prepare_restore(&snapshot, &reg()).unwrap();
        assert_eq!(restored.mode, mode);
        assert_eq!(restored.definition, ModeDef::from(&snapshot));
        assert_eq!(restored.warning, None);
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            if existing { SNAPSHOT_PROMPT } else { "" }
        );
    }

    #[test_case(false; "parent_is_file")]
    #[test_case(true; "plan_is_directory")]
    fn prepare_restore_falls_back_to_resolved_build_when_plan_unusable(directory_at_plan: bool) {
        let directory = tempfile::tempdir().unwrap();
        let blocked = directory.path().join("blocked");
        let path = if directory_at_plan {
            fs::create_dir(&blocked).unwrap();
            blocked
        } else {
            fs::write(&blocked, SNAPSHOT_PROMPT).unwrap();
            blocked.join("plan.md")
        };
        let mode = AgentMode::Plan(path.clone());
        let snapshot = ModeDef::default_for(ModeId::Plan).stored_snapshot(&mode);
        let registry = reg();
        registry
            .define(ModeDefSpec {
                name: "build".into(),
                label: Some(SNAPSHOT_LABEL.into()),
                system_prompt: Some(SNAPSHOT_PROMPT.into()),
                tools: Some(vec![SNAPSHOT_TOOL.into()]),
                ..Default::default()
            })
            .unwrap();
        let restored = prepare_restore(&snapshot, &registry).unwrap();
        assert_eq!(restored.mode, AgentMode::Build);
        assert_eq!(restored.definition, registry.current(&AgentMode::Build));
        let warning = restored.warning.unwrap();
        assert!(warning.contains(path.to_str().unwrap()));
        assert!(warning.contains(RESTORE_BUILD_FALLBACK));
    }

    #[cfg(unix)]
    #[test]
    fn prepare_restore_rejects_fifo_without_waiting_for_reader() {
        use rustix::fs::{CWD, Mode, mkfifoat};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.pipe");
        mkfifoat(CWD, &path, Mode::RUSR | Mode::WUSR).unwrap();
        let snapshot = ModeDef::default_for(ModeId::Plan).stored_snapshot(&AgentMode::Plan(path));
        let restored = prepare_restore(&snapshot, &reg()).unwrap();
        assert_eq!(restored.mode, AgentMode::Build);
        assert!(restored.warning.is_some());
    }

    #[test]
    fn prepare_restore_keeps_deleted_custom_definition() {
        let definition = ModeDefSpec {
            name: GHOST_MODE.into(),
            label: Some(SNAPSHOT_LABEL.into()),
            system_prompt: Some(SNAPSHOT_PROMPT.into()),
            restrict_write_to: Some(SNAPSHOT_TARGET.into()),
            tools: Some(Vec::new()),
        }
        .build()
        .unwrap();
        let snapshot = StoredModeSnapshot::from(&definition);
        let restored = prepare_restore(&snapshot, &reg()).unwrap();
        assert_eq!(restored.mode, AgentMode::Custom(definition.id.clone()));
        assert_eq!(restored.definition, definition);
        assert_eq!(restored.warning, None);
        assert!(matches!(
            prepare_restore(
                &StoredModeSnapshot::from(&ModeDef::default_for(ModeId::Plan)),
                &reg()
            ),
            Err(ModeError::MissingPlanPath)
        ));
    }

    #[test]
    fn stored_plan_snapshot_without_path_is_rejected() {
        let snapshot = StoredModeSnapshot::from(&ModeDef::default_for(ModeId::Plan));
        assert_eq!(from_snapshot(&snapshot), Err(ModeError::MissingPlanPath));
    }

    #[test_case("build", AgentMode::Build, ModeId::Build ; "build")]
    #[test_case(AUDIT_MODE, AgentMode::Build, ModeId::Custom(AUDIT_MODE.into()) ; "custom")]
    #[test_case("plan", AgentMode::Plan(KEPT_PLAN.into()), ModeId::Plan ; "plan_keeps_current_file")]
    fn resolve_maps_name_to_mode(key: &str, current: AgentMode, id: ModeId) {
        let r = reg();
        r.define(ModeDefSpec {
            name: AUDIT_MODE.into(),
            ..Default::default()
        })
        .unwrap();
        let state = tempfile::tempdir().unwrap();
        let (mode, def) =
            resolve(&r, key, &current, &StateDir::from_path(state.path().into())).unwrap();
        assert_eq!(def.id, id);
        assert_eq!(mode.id(), id);
        if let Some(path) = current.plan_path() {
            assert_eq!(mode.plan_path(), Some(path));
        }
    }

    #[test]
    fn resolve_allocates_plan_in_state_dir() {
        let state = tempfile::tempdir().unwrap();
        let (mode, _) = resolve(
            &reg(),
            "plan",
            &AgentMode::Build,
            &StateDir::from_path(state.path().into()),
        )
        .unwrap();
        let path = mode.plan_path().unwrap();
        assert!(path.starts_with(state.path().join(makima_storage::plans::PLANS_DIR)));
    }

    #[test]
    fn resolve_rejects_unknown_mode() {
        let state = tempfile::tempdir().unwrap();
        assert_eq!(
            resolve(
                &reg(),
                GHOST_MODE,
                &AgentMode::Build,
                &StateDir::from_path(state.path().into())
            ),
            Err(ModeError::Unknown(GHOST_MODE.into()))
        );
    }

    #[test]
    fn empty_name_is_rejected() {
        assert_eq!(
            ModeDefSpec {
                name: String::new(),
                ..Default::default()
            }
            .build(),
            Err(ModeError::EmptyName)
        );
        let r = reg();
        assert!(
            r.define(ModeDefSpec {
                name: String::new(),
                ..Default::default()
            })
            .is_err()
        );
    }

    #[test]
    fn list_is_build_then_plan_then_custom() {
        let r = reg();
        r.define(ModeDefSpec {
            name: "zed".into(),
            ..Default::default()
        })
        .unwrap();
        let names: Vec<String> = r
            .list()
            .into_iter()
            .map(|d| d.id.key().to_owned())
            .collect();
        assert_eq!(
            names,
            ["build".to_owned(), "plan".to_owned(), "zed".to_owned()]
        );
    }
}
