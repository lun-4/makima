use std::path::PathBuf;
use std::sync::Arc;

use maki_agent::actor::{ConfigChange, ConfigPatch, PreparedModel};
use maki_agent::manager::ManagerError;
use maki_agent::{AgentMode, ModeDef};
use maki_config::ModelPolicy;
use maki_providers::{Model, Timeouts};

pub(crate) type PrepareProvider =
    Arc<dyn Fn(Model, Timeouts) -> Result<PreparedModel, Arc<str>> + Send + Sync>;

pub(crate) const APPROVAL_BUSY: &str =
    "Planning work is still running or queued. Retry approval when it finishes.";
pub(crate) const APPROVAL_CHANGED: &str =
    "The plan or session changed while preparing implementation. Retry approval.";
pub(crate) const APPROVAL_CANCELLED: &str = "Implementation preparation cancelled.";
pub(crate) const APPROVAL_LOCK_LOST: &str =
    "The session lock was lost to another process. Implementation cannot start.";
const APPROVAL_UNAVAILABLE: &str = "Implementation cannot start";
pub(crate) const IMPLEMENT_PARALLEL_HINT: &str = " Use batch+task to parallelize, assign each subagent a separate module and restrict its tests to that module to avoid interference.";

pub(crate) fn idle_error_message(error: ManagerError) -> String {
    match error {
        ManagerError::BusySubtree(_) => APPROVAL_BUSY.into(),
        error => format!("{APPROVAL_UNAVAILABLE}: {error}."),
    }
}

pub(crate) struct ApprovedPlan {
    pub(crate) path: PathBuf,
    pub(crate) content: String,
    pub(crate) message: String,
}

pub(crate) fn read_plan(path: PathBuf, parallel: bool) -> Result<ApprovedPlan, String> {
    let content = std::fs::read_to_string(&path)
        .map_err(|error| format!("Could not read plan {}: {error}", path.display()))?;
    let parallel = if parallel {
        IMPLEMENT_PARALLEL_HINT
    } else {
        ""
    };
    let message = format!(
        "Implement the approved plan below, captured from `{}`. Use this captured content, not a later revision of the file.{parallel}\n\n{content}",
        path.display()
    );
    Ok(ApprovedPlan {
        path,
        content,
        message,
    })
}

pub(crate) fn prepare_change(
    spec: Option<String>,
    policy: &ModelPolicy,
    timeouts: Timeouts,
    prepare: &PrepareProvider,
    mode_def: ModeDef,
) -> Result<ConfigChange, String> {
    let model = spec
        .map(|spec| {
            if !policy.allows(&spec) {
                return Err(format!("model is not allowed: {spec}"));
            }
            let model = Model::from_spec(&spec).map_err(|error| error.to_string())?;
            prepare(model, timeouts).map_err(|error| error.to_string())
        })
        .transpose()?;
    Ok(ConfigChange::PatchAndMode {
        patch: ConfigPatch {
            model,
            ..Default::default()
        },
        mode: AgentMode::Build,
        mode_def,
    })
}

#[cfg(test)]
mod tests {
    use super::{IMPLEMENT_PARALLEL_HINT, PrepareProvider, prepare_change, read_plan};
    use maki_agent::actor::ConfigChange;
    use maki_agent::{ModeDef, ModeId};
    use maki_config::ModelPolicy;
    use maki_providers::Timeouts;
    use std::sync::Arc;

    const PLAN: &str = "Implement the selected design.";
    const PROVIDER_ERROR: &str = "Provider preparation failed.";

    #[test_case::test_case(false; "sequential")]
    #[test_case::test_case(true; "parallel")]
    fn captures_absolute_plan_content(parallel: bool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plan.md");
        std::fs::write(&path, PLAN).unwrap();
        let plan = read_plan(path.clone(), parallel).unwrap();
        assert_eq!(plan.content, PLAN);
        assert_eq!(plan.path, path);
        assert!(plan.message.contains(&path.display().to_string()));
        assert_eq!(plan.message.contains(IMPLEMENT_PARALLEL_HINT), parallel);
    }

    #[test]
    fn missing_plan_is_an_error_not_empty_content() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("missing.md");
        let error = read_plan(path.clone(), false).err().unwrap();
        assert!(error.contains(&path.display().to_string()));
    }

    #[test]
    fn no_override_does_not_construct_provider() {
        let prepare: PrepareProvider =
            Arc::new(|_, _| panic!("unchanged model must reuse predecessor provider"));
        let change = prepare_change(
            None,
            &ModelPolicy::default(),
            Timeouts::default(),
            &prepare,
            ModeDef::default_for(ModeId::Build),
        )
        .unwrap();
        assert!(
            matches!(change, ConfigChange::PatchAndMode { patch, .. } if patch.model.is_none())
        );
    }

    #[test]
    fn failed_provider_preparation_returns_error() {
        let prepare: PrepareProvider = Arc::new(|_, _| Err(Arc::from(PROVIDER_ERROR)));
        let error = prepare_change(
            Some("zai/glm-5".into()),
            &ModelPolicy::default(),
            Timeouts::default(),
            &prepare,
            ModeDef::default_for(ModeId::Build),
        )
        .err()
        .unwrap();
        assert_eq!(error, PROVIDER_ERROR);
    }
}
