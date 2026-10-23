//! Registry of agent modes: built-in `build` and `plan` plus plugin-defined
//! modes and full overrides of the built-ins. The UI, Lua host, CLI, SDK and
//! ACP all share one `Arc<ModeRegistry>`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use maki_storage::StateDir;
use maki_storage::plans::new_plan_path;
use maki_storage::sessions::{SessionMeta, StoredMode};

use crate::AgentMode;
use crate::prompt::PLAN_PROMPT;

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
    /// When set, all non-matching writes are blocked (plan-file-style lock).
    pub restrict_write_to: Option<PathBuf>,
    /// When `Some`, the mode swaps in exactly this toolset. `None` inherits
    /// the default (build) set.
    pub tools: Option<Vec<String>>,
}

impl ModeDef {
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
    let plan = meta
        .plan_path
        .as_deref()
        .map(Path::new)
        .filter(|path| path.exists());
    Some((key, plan))
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
        assert!(path.starts_with(state.path().join(maki_storage::plans::PLANS_DIR)));
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
