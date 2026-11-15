use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::AgentMode;
use crate::ModeRegistry;
use crate::command::find_project_ancestor_dirs;
use crate::template::Vars;

const INSTRUCTION_FILES: &[&str] = &[
    "AGENTS.md",
    "CLAUDE.md",
    ".github/copilot-instructions.md",
    "COPILOT.md",
    ".cursorrules",
    ".windsurfrules",
    ".clinerules",
    "CONVENTIONS.md",
    "GEMINI.md",
    "CODING_AGENT.md",
];

const LOCAL_INSTRUCTION_FILE: &str = "AGENTS.local.md";
const GLOBAL_INSTRUCTION_FILE: &str = "AGENTS.md";

#[derive(Clone, Default)]
pub struct LoadedInstructions(Arc<Mutex<HashSet<PathBuf>>>);

impl LoadedInstructions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains_or_insert(&self, path: PathBuf) -> bool {
        let mut set = self.0.lock().unwrap_or_else(|e| e.into_inner());
        !set.insert(path)
    }
}

#[derive(Clone)]
pub struct InstructionSource {
    pub path: PathBuf,
    pub label: String,
    pub content: String,
}

#[derive(Clone, Default)]
pub struct Instructions {
    pub text: String,
    pub loaded: LoadedInstructions,
    pub sources: Vec<InstructionSource>,
}

impl Instructions {
    pub fn from_sources(sources: Vec<InstructionSource>) -> Self {
        let mut text = String::new();
        let loaded = LoadedInstructions::new();
        for source in &sources {
            loaded.contains_or_insert(source.path.clone());
            text.push_str(&format!("\n\n{}:\n{}", source.label, source.content));
        }
        Self {
            text,
            loaded,
            sources,
        }
    }
}

pub fn is_instruction_file(name: &str) -> bool {
    name == LOCAL_INSTRUCTION_FILE
        || INSTRUCTION_FILES
            .iter()
            .any(|f| *f == name || Path::new(f).file_name().is_some_and(|n| n == name))
}

pub fn build_system_prompt(
    vars: &Vars,
    modes: &ModeRegistry,
    mode: &AgentMode,
    instructions: &str,
    slots: &crate::prompt::ResolvedSlots,
) -> String {
    build_system_prompt_with_def(vars, &modes.current(mode), mode, instructions, slots)
}

pub fn build_system_prompt_with_def(
    vars: &Vars,
    def: &crate::ModeDef,
    mode: &AgentMode,
    instructions: &str,
    slots: &crate::prompt::ResolvedSlots,
) -> String {
    let env = vars.apply(
        "\n\nEnvironment:\n- Working directory: {cwd}\n- Platform: {platform}\n- Date: {date}",
    );
    let instructions = format!("{env}{instructions}");
    let mut out = crate::prompt::assemble(crate::prompt::PromptId::System, slots, &instructions);

    out.push_str(&build_mode_system_prompt(vars, def, mode));
    out
}

pub fn build_mode_system_prompt(vars: &Vars, def: &crate::ModeDef, mode: &AgentMode) -> String {
    let Some(snippet) = &def.system_prompt else {
        return String::new();
    };
    let plan_path = mode
        .plan_path()
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    vars.clone()
        .with_custom("{plan_path}", plan_path)
        .apply(snippet)
        .into_owned()
}

fn read_instruction(path: &Path, loaded: &LoadedInstructions) -> Option<(PathBuf, String)> {
    let canonical = path.canonicalize().ok()?;
    if loaded.contains_or_insert(canonical.clone()) {
        return None;
    }
    let content = fs::read_to_string(&canonical).ok()?;
    Some((canonical, content))
}

fn collect_instruction_files(
    cwd: &str,
    global_config: Option<&Path>,
    loaded: &LoadedInstructions,
) -> Vec<InstructionSource> {
    let mut out = Vec::new();

    let ancestor_dirs: Vec<_> = find_project_ancestor_dirs(Path::new(cwd)).collect();
    let has_git_root = ancestor_dirs.iter().any(|dir| dir.join(".git").exists());
    let project_dirs = if has_git_root {
        ancestor_dirs
    } else {
        ancestor_dirs.into_iter().take(1).collect()
    };

    // Load root instructions first so cwd instructions come last and override on conflicts.
    for dir in project_dirs.into_iter().rev() {
        for filename in INSTRUCTION_FILES {
            if let Some((canonical, content)) = read_instruction(&dir.join(filename), loaded) {
                out.push(InstructionSource {
                    label: format!("Project instructions ({})", canonical.display()),
                    path: canonical,
                    content,
                });
                break;
            }
        }

        if let Some((canonical, content)) =
            read_instruction(&dir.join(LOCAL_INSTRUCTION_FILE), loaded)
        {
            out.push(InstructionSource {
                label: format!("Local instructions ({})", canonical.display()),
                path: canonical,
                content,
            });
        }
    }

    if let Some(dir) = global_config
        && let Some((canonical, content)) =
            read_instruction(&dir.join(GLOBAL_INSTRUCTION_FILE), loaded)
    {
        out.push(InstructionSource {
            label: format!("Global instructions ({})", canonical.display()),
            path: canonical,
            content,
        });
    }

    out
}

pub fn load_instruction_text(cwd: &str) -> String {
    load_instruction_text_with_global(cwd, makima_storage::paths::config_dir().ok().as_deref())
}

pub(crate) fn load_instruction_text_with_global(cwd: &str, global_config: Option<&Path>) -> String {
    load_instructions_with_global(cwd, global_config).text
}

pub fn load_instructions(cwd: &str) -> Instructions {
    load_instructions_with_global(cwd, makima_storage::paths::config_dir().ok().as_deref())
}

pub(crate) fn load_instructions_with_global(
    cwd: &str,
    global_config: Option<&Path>,
) -> Instructions {
    let loaded = LoadedInstructions::new();
    let sources = collect_instruction_files(cwd, global_config, &loaded);
    let mut instructions = Instructions::from_sources(sources);
    instructions.loaded = loaded;
    instructions
}

pub fn find_subdirectory_instructions(
    dir: &Path,
    cwd: &Path,
    loaded: &LoadedInstructions,
) -> Vec<(String, String)> {
    let Ok(cwd) = cwd.canonicalize() else {
        return Vec::new();
    };
    let Ok(dir) = dir.canonicalize() else {
        return Vec::new();
    };

    if !dir.starts_with(&cwd) || dir == cwd {
        return Vec::new();
    }

    let mut results = Vec::new();
    let mut current = dir.as_path();
    while current != cwd {
        for filename in INSTRUCTION_FILES {
            if let Some((canonical, content)) = read_instruction(&current.join(filename), loaded) {
                results.push((canonical.display().to_string(), content));
                break;
            }
        }
        current = match current.parent() {
            Some(p) => p,
            None => break,
        };
    }
    results
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use test_case::test_case;

    use super::*;

    const PLAN_PATH: &str = ".makima/plans/123.md";

    #[test_case(&AgentMode::Build, false ; "build_excludes_plan")]
    #[test_case(&AgentMode::Plan(PathBuf::from(PLAN_PATH)), true ; "plan_includes_plan")]
    fn plan_section_presence(mode: &AgentMode, expect_plan: bool) {
        let vars = Vars::new().set("{cwd}", "/tmp").set("{platform}", "linux");
        let slots = crate::prompt::ResolvedSlots::default();
        let modes = crate::ModeRegistry::builtin();
        let prompt = build_system_prompt(&vars, &modes, mode, "", &slots);
        assert_eq!(prompt.contains("Plan Mode"), expect_plan);
        if expect_plan {
            assert!(prompt.contains(PLAN_PATH));
        }
    }

    #[test]
    fn system_prompt_contains_generated_environment() {
        let vars = Vars::new()
            .set("{cwd}", "/tmp/project")
            .set("{platform}", "linux")
            .set("{date}", "2025-01-02");
        let prompt = build_system_prompt(
            &vars,
            &crate::ModeRegistry::builtin(),
            &AgentMode::Build,
            "",
            &crate::prompt::ResolvedSlots::default(),
        );

        assert!(prompt.contains("Environment:"));
        assert!(prompt.contains("- Working directory: /tmp/project"));
        assert!(prompt.contains("- Platform: linux"));
        assert!(prompt.contains("- Date: 2025-01-02"));
    }

    #[test_case(&AgentMode::Build, "[BUILD]", false ; "build_label")]
    #[test_case(&AgentMode::Plan(PathBuf::from("plan.md")), "[PLAN]", true ; "plan_label")]
    fn mode_label_from_registry(mode: &AgentMode, label: &str, expect_prompt: bool) {
        let def = crate::ModeRegistry::builtin().current(mode);
        assert_eq!(def.label.as_ref(), label);
        assert_eq!(def.system_prompt.is_some(), expect_prompt);
    }

    #[test]
    fn overridden_plan_replaces_prompt_text() {
        let modes = crate::ModeRegistry::builtin();
        modes
            .define(crate::ModeDefSpec {
                name: "plan".into(),
                system_prompt: Some("Polytoken directive for {plan_path}".into()),
                ..Default::default()
            })
            .unwrap();
        let vars = Vars::new().set("{cwd}", "/tmp");
        let slots = crate::prompt::ResolvedSlots::default();
        let prompt = build_system_prompt(
            &vars,
            &modes,
            &AgentMode::Plan(PathBuf::from("plan.md")),
            "",
            &slots,
        );
        assert!(prompt.contains("Polytoken directive for plan.md"));
        assert!(!prompt.contains("Plan Mode"));
    }

    #[test]
    fn custom_mode_prompt_lands_in_system() {
        let modes = crate::ModeRegistry::builtin();
        modes
            .define(crate::ModeDefSpec {
                name: "audit".into(),
                system_prompt: Some("AUDIT_DIRECTIVE".into()),
                ..Default::default()
            })
            .unwrap();
        let vars = Vars::new().set("{cwd}", "/tmp");
        let slots = crate::prompt::ResolvedSlots::default();
        let prompt = build_system_prompt(
            &vars,
            &modes,
            &AgentMode::Custom(crate::ModeId::Custom("audit".into())),
            "",
            &slots,
        );
        assert!(prompt.contains("AUDIT_DIRECTIVE"));
    }

    #[test]
    fn after_instructions_slot_lands_between_instructions_and_plan() {
        use std::sync::Arc;
        const INSTR: &str = "Project instructions here";
        const EXTRA: &str = "MEMORY_EXTRA";
        let vars = Vars::new().set("{cwd}", "/tmp").set("{platform}", "linux");
        let mut slots = crate::prompt::ResolvedSlots::default();
        slots.insert(
            crate::prompt::PromptId::System,
            crate::prompt::Slot::AfterInstructions,
            crate::prompt::SlotEntry {
                plugin: Arc::from("memory"),
                registration: 0,
                content: EXTRA.into(),
            },
        );
        let prompt = build_system_prompt(
            &vars,
            &crate::ModeRegistry::builtin(),
            &AgentMode::Plan(PathBuf::from("plan.md")),
            &format!("\n{INSTR}"),
            &slots,
        );
        let positions = [INSTR, EXTRA, "Plan Mode"].map(|n| prompt.find(n).unwrap());
        assert!(
            positions.is_sorted(),
            "expected order instructions < slot extra < plan section, got {positions:?}"
        );
    }

    #[test_case("AGENTS.md",                true  ; "direct_match")]
    #[test_case("CLAUDE.md",                true  ; "claude_md")]
    #[test_case("copilot-instructions.md",  true  ; "nested_path_filename")]
    #[test_case(".cursorrules",             true  ; "dotfile")]
    #[test_case("AGENTS.local.md",          true  ; "local_instruction_file")]
    #[test_case("random.md",                false ; "unrelated_file")]
    #[test_case("not-AGENTS.md",            false ; "partial_match")]
    fn is_instruction_file_cases(name: &str, expected: bool) {
        assert_eq!(is_instruction_file(name), expected);
    }

    #[test]
    fn from_sources_keeps_deleted_identities_and_exact_layout() {
        let dir = tempfile::tempdir().unwrap();
        let mut sources = Vec::new();
        for (filename, label, content) in [
            ("AGENTS.md", "recorded project", "first\n"),
            ("AGENTS.local.md", "recorded local", "\nsecond"),
        ] {
            let path = dir.path().join(filename);
            fs::write(&path, content).unwrap();
            let canonical = path.canonicalize().unwrap();
            fs::remove_file(&path).unwrap();
            sources.push(InstructionSource {
                path: canonical,
                label: label.into(),
                content: content.into(),
            });
        }
        let instructions = Instructions::from_sources(sources.clone());
        assert_eq!(
            instructions.text,
            "\n\nrecorded project:\nfirst\n\n\nrecorded local:\n\nsecond"
        );
        assert_eq!(instructions.sources.len(), sources.len());
        for (recorded, source) in instructions.sources.iter().zip(&sources) {
            assert_eq!(recorded.path, source.path);
            assert_eq!(recorded.label, source.label);
            assert_eq!(recorded.content, source.content);
            assert!(!source.path.exists());
            assert!(instructions.loaded.contains_or_insert(source.path.clone()));
        }
    }

    #[test_case(false ; "distinct_global")]
    #[test_case(true ; "global_deduplicates_project")]
    fn instruction_sources_preserve_load_order_and_canonical_identity(global_is_project: bool) {
        let root = tempfile::tempdir().unwrap();
        let global = tempfile::tempdir().unwrap();
        let sub = root.path().join("src");
        fs::create_dir(root.path().join(".git")).unwrap();
        fs::create_dir(&sub).unwrap();
        let mut expected = Vec::new();
        for (dir, filename, kind, content) in [
            (root.path(), "AGENTS.md", "Project", "root"),
            (root.path(), "AGENTS.local.md", "Local", "root local"),
            (sub.as_path(), "AGENTS.md", "Project", "child"),
            (sub.as_path(), "AGENTS.local.md", "Local", "child local"),
            (global.path(), "AGENTS.md", "Global", "global"),
        ] {
            let path = dir.join(filename);
            fs::write(&path, content).unwrap();
            let path = path.canonicalize().unwrap();
            let label = format!("{kind} instructions ({})", path.display());
            expected.push((path, label, content.to_string()));
        }
        fs::write(sub.join("CLAUDE.md"), "not selected").unwrap();
        let global_dir = if global_is_project {
            expected.pop();
            root.path()
        } else {
            global.path()
        };
        let instructions = load_instructions_with_global(sub.to_str().unwrap(), Some(global_dir));
        let actual: Vec<_> = instructions
            .sources
            .iter()
            .map(|source| {
                (
                    source.path.clone(),
                    source.label.clone(),
                    source.content.clone(),
                )
            })
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(
            instructions.text,
            expected
                .iter()
                .map(|(_, label, content)| format!("\n\n{label}:\n{content}"))
                .collect::<String>()
        );
        assert_eq!(
            load_instruction_text_with_global(sub.to_str().unwrap(), Some(global_dir)),
            instructions.text
        );
        for (path, _, _) in expected {
            assert!(instructions.loaded.contains_or_insert(path));
        }
    }

    #[test]
    fn load_instructions_merges_project_and_local() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("AGENTS.md"), "team rules").unwrap();
        fs::write(dir.path().join("AGENTS.local.md"), "my preferences").unwrap();

        let text = &load_instructions_with_global(dir.path().to_str().unwrap(), None).text;
        assert!(text.contains("team rules"));
        assert!(text.contains("my preferences"));
        assert!(
            text.find("team rules").unwrap() < text.find("my preferences").unwrap(),
            "project instructions should come before local instructions"
        );
    }

    #[test]
    fn load_instructions_local_without_project() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("AGENTS.local.md"), "solo preferences").unwrap();
        assert!(
            load_instructions_with_global(dir.path().to_str().unwrap(), None)
                .text
                .contains("solo preferences")
        );
    }

    #[test]
    fn load_instructions_empty_when_no_files() {
        let dir = tempfile::tempdir().unwrap();
        assert!(
            load_instructions_with_global(dir.path().to_str().unwrap(), None)
                .text
                .is_empty()
        );
    }

    #[test]
    fn load_instructions_empty_when_global_dir_has_no_file() {
        let cwd = tempfile::tempdir().unwrap();
        let global = tempfile::tempdir().unwrap();
        assert!(
            load_instructions_with_global(cwd.path().to_str().unwrap(), Some(global.path()))
                .text
                .is_empty()
        );
    }

    #[test]
    fn load_instructions_includes_global_file() {
        let cwd = tempfile::tempdir().unwrap();
        let global = tempfile::tempdir().unwrap();
        fs::write(global.path().join(GLOBAL_INSTRUCTION_FILE), "global rules").unwrap();

        let text =
            load_instructions_with_global(cwd.path().to_str().unwrap(), Some(global.path())).text;
        assert!(text.contains("global rules"));
    }

    #[test]
    fn load_instructions_includes_parent_directory_instructions() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".git")).unwrap();

        let sub = dir.path().join("crates").join("a");
        fs::create_dir_all(&sub).unwrap();

        fs::write(dir.path().join("AGENTS.md"), "root rules").unwrap();
        fs::write(sub.join("AGENTS.md"), "crate rules").unwrap();

        let text = load_instructions_with_global(sub.to_str().unwrap(), None).text;
        assert!(
            text.contains("crate rules"),
            "should load instructions from cwd"
        );
        assert!(
            text.contains("root rules"),
            "should load instructions from project root"
        );
        assert!(
            text.find("root rules").unwrap() < text.find("crate rules").unwrap(),
            "root instructions should come before closer instructions"
        );
    }

    #[test]
    fn find_subdirectory_instructions_discovers_agents_md() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("src").join("api");
        fs::create_dir_all(&sub).unwrap();
        fs::write(dir.path().join("src").join("AGENTS.md"), "api rules").unwrap();

        let loaded = LoadedInstructions::new();
        let results = find_subdirectory_instructions(&sub, dir.path(), &loaded);

        assert_eq!(results.len(), 1);
        assert!(results[0].0.ends_with("AGENTS.md"));
        assert_eq!(results[0].1, "api rules");
    }

    #[test]
    fn find_subdirectory_instructions_skips_root() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("AGENTS.md"), "root rules").unwrap();

        let loaded = LoadedInstructions::new();
        let from_root = find_subdirectory_instructions(dir.path(), dir.path(), &loaded);
        assert!(from_root.is_empty(), "should skip root-level directory");
    }

    #[test]
    fn find_subdirectory_instructions_deduplicates() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("src");
        fs::create_dir_all(&sub).unwrap();
        let agents_path = sub.join("AGENTS.md");
        fs::write(&agents_path, "rules").unwrap();

        let canonical = agents_path.canonicalize().unwrap();
        let loaded = LoadedInstructions::new();
        loaded.contains_or_insert(canonical);
        let pre_loaded = find_subdirectory_instructions(&sub, dir.path(), &loaded);
        assert!(pre_loaded.is_empty(), "should skip already-loaded files");

        let loaded = LoadedInstructions::new();
        let first = find_subdirectory_instructions(&sub, dir.path(), &loaded);
        let second = find_subdirectory_instructions(&sub, dir.path(), &loaded);
        assert_eq!(first.len(), 1);
        assert!(
            second.is_empty(),
            "should not return same file twice across calls"
        );
    }

    #[test]
    fn load_instructions_populates_loaded_set() {
        let dir = tempfile::tempdir().unwrap();
        let agents_path = dir.path().join("AGENTS.md");
        fs::write(&agents_path, "content").unwrap();

        let instr = load_instructions_with_global(dir.path().to_str().unwrap(), None);
        assert!(
            instr
                .loaded
                .contains_or_insert(agents_path.canonicalize().unwrap())
        );
    }
}
