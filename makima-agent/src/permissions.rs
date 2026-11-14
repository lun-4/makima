use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use makima_config::{
    DefaultEffect, Effect, FILE_WRITE_TOOLS, PermissionRule, PermissionTarget, PermissionsConfig,
    ProjectConfig, ToolKey, ToolKeyParseError, append_permission_rule,
};
use makima_storage::checkpoint::CheckpointPublication;
use makima_storage::session_state::StateChange;
use makima_storage::sessions::{StoredEffect, StoredRule};
use thiserror::Error;
use tracing::{info, warn};

use crate::journal::{
    JournalChange, JournalCommit, JournalError, JournalIncarnation, SessionJournal,
};
use crate::{AgentEvent, CancelToken, EventSender};

pub const DEFAULT_DENY_GUIDANCE: &str =
    "Do not retry. Try a different approach or ask the user for guidance.";

/// Tests assert on this exact prefix; a wording tweak here updates them in one place.
pub const PERMISSION_DENIED_PREFIX: &str = "Permission denied for";

const TASK_SPAWN_TOOL: &str = "task_spawn";
const BASH_TOOL: &str = "bash";

/// Words that open a block the bash plugin keeps as one scope. Their first
/// token names no program, so wildcarding it would cover every command the
/// block can hold. See [`generalize_bash_segment`].
const SHELL_KEYWORDS: [&str; 15] = [
    "if", "then", "elif", "else", "fi", "for", "while", "until", "do", "done", "case", "esac",
    "select", "function", "time",
];
fn builtin_rules(cwd: &Path) -> Vec<PermissionRule> {
    let cwd_glob = format!(
        "{}/**",
        makima_storage::paths::canonicalize_clean(cwd).display()
    );
    let allow = |tool: &str, scope: &str| PermissionRule {
        tool: ToolKey::native(tool),
        scope: Some(scope.into()),
        effect: Effect::Allow,
    };
    let mut rules: Vec<PermissionRule> = FILE_WRITE_TOOLS
        .iter()
        .map(|tool| allow(tool, &cwd_glob))
        .collect();
    rules.push(allow(TASK_SPAWN_TOOL, "*"));
    rules
}

pub const BOUNDARY_UNVERIFIABLE_PREFIX: &str = "Cannot verify project boundary for";

/// Whether the builtin defaults treat `tool` specially. File write tools get
/// the cwd allow and the plan mode allow, `task_spawn` gets a blanket allow, and an
/// "allow always" for `bash` is stored under the first word of the command.
/// Rules are keyed by name alone, so a plugin taking one of these names
/// inherits all of it.
pub fn carries_builtin_defaults(tool: &str) -> bool {
    FILE_WRITE_TOOLS.contains(&tool) || matches!(tool, TASK_SPAWN_TOOL | BASH_TOOL)
}

#[derive(Debug)]
pub enum PermissionCheck {
    Allowed,
    Denied,
    NeedsPrompt {
        tool: ToolKey,
        scopes: Vec<String>,
        force_prompt: bool,
    },
}

#[derive(Debug, Error)]
pub struct PermissionError {
    tool: String,
    scope: String,
    guidance: Option<String>,
}

impl std::fmt::Display for PermissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} `{}` ({}).",
            PERMISSION_DENIED_PREFIX, self.tool, self.scope
        )?;
        if let Some(g) = &self.guidance {
            write!(f, " User guidance: {}", g)
        } else {
            write!(f, " {}", DEFAULT_DENY_GUIDANCE)
        }
    }
}

impl PermissionError {
    fn new(tool: &str, scope: &str) -> Self {
        Self {
            tool: tool.to_string(),
            scope: scope.to_string(),
            guidance: None,
        }
    }

    fn with_guidance(tool: &str, scope: &str, guidance: String) -> Self {
        Self {
            tool: tool.to_string(),
            scope: scope.to_string(),
            guidance: Some(guidance),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionAnswer {
    AllowOnce,
    AllowSession,
    AllowAlwaysProject,
    AllowAlwaysLocal,
    AllowAlwaysGlobal,
    Deny,
    DenyWithGuidance(String),
    DenyAlwaysProject,
    DenyAlwaysLocal,
    DenyAlwaysGlobal,
}

impl PermissionAnswer {
    pub fn is_allow(&self) -> bool {
        matches!(
            self,
            Self::AllowOnce
                | Self::AllowSession
                | Self::AllowAlwaysProject
                | Self::AllowAlwaysLocal
                | Self::AllowAlwaysGlobal
        )
    }

    pub fn encode(&self) -> String {
        match self {
            Self::AllowOnce => "allow".to_string(),
            Self::AllowSession => "allow_session".to_string(),
            Self::AllowAlwaysProject => "allow_always_project".to_string(),
            Self::AllowAlwaysLocal => "allow_always_local".to_string(),
            Self::AllowAlwaysGlobal => "allow_always_global".to_string(),
            Self::Deny => "deny".to_string(),
            Self::DenyWithGuidance(g) => format!("deny:{g}"),
            Self::DenyAlwaysProject => "deny_always_project".to_string(),
            Self::DenyAlwaysLocal => "deny_always_local".to_string(),
            Self::DenyAlwaysGlobal => "deny_always_global".to_string(),
        }
    }

    pub fn decode(s: &str) -> Option<Self> {
        match s {
            "allow" => Some(Self::AllowOnce),
            "allow_session" => Some(Self::AllowSession),
            "allow_always_project" => Some(Self::AllowAlwaysProject),
            "allow_always_local" => Some(Self::AllowAlwaysLocal),
            "allow_always_global" => Some(Self::AllowAlwaysGlobal),
            "deny" => Some(Self::Deny),
            "deny_always_project" => Some(Self::DenyAlwaysProject),
            "deny_always_local" => Some(Self::DenyAlwaysLocal),
            "deny_always_global" => Some(Self::DenyAlwaysGlobal),
            _ if s.starts_with("deny:") => {
                let guidance = s.strip_prefix("deny:").unwrap();
                if guidance.is_empty() {
                    Some(Self::Deny)
                } else {
                    Some(Self::DenyWithGuidance(guidance.to_string()))
                }
            }
            _ => None,
        }
    }

    pub fn guidance(&self) -> Option<&str> {
        match self {
            Self::DenyWithGuidance(g) => Some(g),
            _ => None,
        }
    }
}

/// Splits the ask id from the encoded answer on the answer channel. A control
/// character cannot appear in a tool-use id, and the answer is the tail, so
/// deny guidance holding one still round-trips.
const ANSWER_ID_SEPARATOR: char = '\u{1f}';

/// An answer together with the id of the ask it answers.
///
/// The name is what makes a late answer harmless. The answer channel is a
/// queue with no receiver parked between turns, so an answer to a cancelled
/// turn's ask can sit there until the next turn's first permission wait pops
/// it. Untagged, that stale answer is applied to a different tool and scope,
/// and an `allow_always_*` then installs a rule for something the user never
/// saw. Tagged, the waiter can tell it apart and drop it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedAnswer {
    pub request_id: String,
    pub answer: PermissionAnswer,
}

impl TaggedAnswer {
    pub fn new(request_id: impl Into<String>, answer: PermissionAnswer) -> Self {
        Self {
            request_id: request_id.into(),
            answer,
        }
    }

    pub fn encode(&self) -> String {
        format!(
            "{}{ANSWER_ID_SEPARATOR}{}",
            self.request_id,
            self.answer.encode()
        )
    }

    pub fn decode(raw: &str) -> Option<Self> {
        let (request_id, answer) = raw.split_once(ANSWER_ID_SEPARATOR)?;
        Some(Self::new(request_id, PermissionAnswer::decode(answer)?))
    }
}

/// Permission rules declared by Lua plugins via
/// `makima.api.register_permission_rule`, keyed by plugin name. Shared between
/// the Lua runtime (writer, on plugin load/unload) and every
/// [`PermissionManager`] (reader).
#[derive(Default)]
pub struct PluginRuleStore(Mutex<HashMap<Arc<str>, Vec<PermissionRule>>>);

impl PluginRuleStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Arc<str>, Vec<PermissionRule>>> {
        self.0.lock().unwrap_or_else(|e| {
            warn!("plugin rule mutex was poisoned, recovering");
            e.into_inner()
        })
    }

    /// An empty `rules` removes the entry, so a reload that registers
    /// nothing clears the stale rules.
    pub fn replace(&self, plugin: &str, rules: Vec<PermissionRule>) {
        let mut map = self.lock();
        if rules.is_empty() {
            map.remove(plugin);
        } else {
            map.insert(Arc::from(plugin), rules);
        }
    }

    pub fn remove(&self, plugin: &str) {
        self.lock().remove(plugin);
    }

    pub fn snapshot(&self) -> Vec<PermissionRule> {
        self.lock().values().flatten().cloned().collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PermissionSnapshot {
    pub rules: Vec<PermissionRule>,
    pub yolo: Option<bool>,
}

impl PermissionSnapshot {
    pub fn from_stored(
        rules: &[StoredRule],
        yolo: Option<bool>,
    ) -> Result<Self, ToolKeyParseError> {
        Ok(Self {
            rules: rules
                .iter()
                .map(|rule| {
                    Ok(PermissionRule {
                        tool: ToolKey::parse(&rule.tool)?,
                        scope: rule.scope.clone(),
                        effect: match rule.effect {
                            StoredEffect::Allow => Effect::Allow,
                            StoredEffect::Deny => Effect::Deny,
                        },
                    })
                })
                .collect::<Result<_, ToolKeyParseError>>()?,
            yolo,
        })
    }

    pub fn stored_rules(&self) -> Vec<StoredRule> {
        self.rules
            .iter()
            .map(|rule| StoredRule {
                tool: rule.tool.to_string(),
                scope: rule.scope.clone(),
                effect: match rule.effect {
                    Effect::Allow => StoredEffect::Allow,
                    Effect::Deny => StoredEffect::Deny,
                },
            })
            .collect()
    }
}

#[derive(Clone)]
struct PermissionJournal {
    journal: Arc<SessionJournal>,
    incarnation: Option<JournalIncarnation>,
}

#[derive(Default)]
struct SessionPermissions {
    snapshot: PermissionSnapshot,
    binding: Option<PermissionJournal>,
}

pub struct PermissionManager {
    session: Mutex<SessionPermissions>,
    config_rules: Vec<PermissionRule>,
    builtin_rules: Mutex<Vec<PermissionRule>>,
    seed_yolo: bool,
    default: DefaultEffect,
    tool_defaults: HashMap<ToolKey, DefaultEffect>,
    cwd: Mutex<PathBuf>,
    project_config: ProjectConfig,
    plugin_rules: Arc<PluginRuleStore>,
}

impl PermissionManager {
    pub fn new(
        config: PermissionsConfig,
        cwd: PathBuf,
        project_config: ProjectConfig,
        plugin_rules: Arc<PluginRuleStore>,
    ) -> Self {
        let config_rules = config.rules;
        let builtin_rules = builtin_rules(&cwd);

        // Warn if wildcard deny is present — it blocks ALL tools including builtins.
        let has_wildcard_deny = config_rules
            .iter()
            .any(|r| matches!(r.tool, ToolKey::Wildcard) && r.effect == Effect::Deny);
        if has_wildcard_deny {
            warn!(
                "wildcard deny detected — this blocks ALL tools including \
                 builtins (write/edit/multiedit/task_spawn). Use per-tool rules \
                 instead if you want selective access."
            );
        }
        // Warn if wildcard allow is present — it permits ALL tools including write/edit/task_spawn.
        let has_wildcard_allow = config_rules
            .iter()
            .any(|r| matches!(r.tool, ToolKey::Wildcard) && r.effect == Effect::Allow);
        if has_wildcard_allow {
            warn!(
                "wildcard allow detected — this permits ALL tools including \
                 write/edit/multiedit/task_spawn. Use per-tool rules \
                 instead if you want selective access."
            );
        }

        Self {
            builtin_rules: Mutex::new(builtin_rules),
            session: Mutex::default(),
            config_rules,
            seed_yolo: config.yolo,
            default: config.default,
            tool_defaults: config.tool_defaults,
            cwd: Mutex::new(cwd),
            project_config,
            plugin_rules,
        }
    }

    /// Fresh manager for a new session runtime: shares config and builtin
    /// rules plus the current yolo state, but owns empty session rules so
    /// restoring one session never clobbers another's grants.
    pub fn fork(&self) -> Self {
        Self {
            session: Mutex::new(SessionPermissions {
                snapshot: PermissionSnapshot {
                    rules: Vec::new(),
                    yolo: self.persisted_yolo(),
                },
                binding: None,
            }),
            config_rules: self.config_rules.clone(),
            builtin_rules: Mutex::new(
                self.builtin_rules
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone(),
            ),
            seed_yolo: self.seed_yolo,
            default: self.default,
            tool_defaults: self.tool_defaults.clone(),
            cwd: Mutex::new(
                self.cwd
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .clone(),
            ),
            project_config: self.project_config.clone(),
            plugin_rules: Arc::clone(&self.plugin_rules),
        }
    }

    fn session(&self) -> MutexGuard<'_, SessionPermissions> {
        self.session.lock().unwrap_or_else(|e| {
            warn!("permission mutex was poisoned, recovering");
            e.into_inner()
        })
    }

    /// The order of the checks below is the policy itself, not an accident of
    /// how it was written: denies first, then yolo, then explicit allows, the
    /// plan file write, and last the defaults. Moving one moves the rules.
    fn check_inner(
        &self,
        tool: &ToolKey,
        scopes: &[&str],
        force_prompt: bool,
        plan_path: Option<&Path>,
    ) -> PermissionCheck {
        let session = self.session();
        let plugin = self.plugin_rules.snapshot();
        let builtin = self
            .builtin_rules
            .lock()
            .unwrap_or_else(|error| error.into_inner());

        let mut unclaimed_scopes: Vec<&str> = if force_prompt {
            Vec::new()
        } else {
            Vec::with_capacity(scopes.len())
        };

        for scope in scopes {
            let mut has_allow = false;
            for r in session
                .snapshot
                .rules
                .iter()
                .chain(&self.config_rules)
                .chain(builtin.iter())
                .chain(&plugin)
            {
                if !rule_matches_tool(&r.tool, tool) || !rule_matches_scope(r, scope) {
                    continue;
                }
                match r.effect {
                    Effect::Deny => {
                        info!(tool = %tool, scope = %scope, "permission denied");
                        return PermissionCheck::Denied;
                    }
                    Effect::Allow => has_allow = true,
                }
            }

            if has_allow {
                // allow wins for this scope (no deny matched)
            } else if !force_prompt {
                unclaimed_scopes.push(scope);
            }
            // force_prompt: all scopes will be prompted anyway
        }

        if session.snapshot.yolo.unwrap_or(self.seed_yolo) {
            return PermissionCheck::Allowed;
        }

        let pending: Vec<&str> = if force_prompt {
            scopes.to_vec()
        } else {
            unclaimed_scopes
        };

        if pending.is_empty() {
            return PermissionCheck::Allowed;
        }

        // Plan file auto-allow: fires AFTER deny rules have been evaluated.
        // Only triggers if ALL pending scopes match the plan file path.
        // A single non-plan scope means we must prompt for the rest.
        if !force_prompt && !pending.is_empty() {
            let is_plan_write = plan_path.is_some_and(|pp| {
                matches!(tool, ToolKey::Native(name) if FILE_WRITE_TOOLS.contains(&name.as_ref()))
                    && {
                        let normalized_plan = normalize_scope_path(&pp.display().to_string());
                        pending
                            .iter()
                            .all(|s| normalize_scope_path(s) == normalized_plan)
                    }
            });
            if is_plan_write {
                return PermissionCheck::Allowed;
            }
        }

        let eff = self
            .tool_defaults
            .get(tool)
            .copied()
            .or_else(|| {
                // McpTool falls back to McpServer-level default (Arc clone, ~2ns)
                let server = match tool {
                    ToolKey::McpTool { server, .. } => server,
                    _ => return None,
                };
                self.tool_defaults
                    .get(&ToolKey::McpServer {
                        server: server.clone(),
                    })
                    .copied()
            })
            .unwrap_or(self.default);
        match eff {
            DefaultEffect::Deny => {
                info!(tool = %tool, "denied by default");
                PermissionCheck::Denied
            }
            DefaultEffect::Allow => PermissionCheck::Allowed,
            DefaultEffect::Prompt => PermissionCheck::NeedsPrompt {
                tool: tool.clone(),
                scopes: pending.into_iter().map(|s| s.to_string()).collect(),
                force_prompt,
            },
        }
    }

    pub fn check(&self, tool: &ToolKey, scope: &str, plan_path: Option<&Path>) -> PermissionCheck {
        self.check_inner(tool, &[scope], false, plan_path)
    }

    pub fn check_multi(
        &self,
        tool: &ToolKey,
        scopes: &[&str],
        force_prompt: bool,
        plan_path: Option<&Path>,
    ) -> PermissionCheck {
        self.check_inner(tool, scopes, force_prompt, plan_path)
    }

    pub fn bind_journal(
        &self,
        journal: Arc<SessionJournal>,
        incarnation: Option<JournalIncarnation>,
    ) -> Result<(), JournalError> {
        let mut session = self.session();
        let incarnations = incarnation.clone().into_iter().collect();
        journal.commit_with_apply(
            |_| JournalCommit {
                incarnations,
                ..Default::default()
            },
            || {
                session.binding = Some(PermissionJournal {
                    journal: Arc::clone(&journal),
                    incarnation,
                });
            },
        )?;
        Ok(())
    }

    pub fn commit_startup(&self, mut commit: JournalCommit) -> Result<u64, JournalError> {
        let binding = self.session().binding.clone().ok_or_else(|| {
            JournalError::Preparation("session permissions journal is not bound".into())
        })?;
        let _publication = binding.journal.try_publication_read().ok_or_else(|| {
            JournalError::Preparation(
                "startup checkpoint conflicts with prepared publication".into(),
            )
        })?;
        let session = self.session();
        if !same_permission_binding(session.binding.as_ref(), Some(&binding)) {
            return Err(JournalError::Preparation(
                "permission journal binding changed".into(),
            ));
        }
        let rules = session.snapshot.stored_rules();
        let yolo = session.snapshot.yolo;
        commit.incarnations.extend(binding.incarnation.clone());
        binding.journal.commit_with_apply(
            |current| {
                commit.histories.retain(|agent, history| {
                    current.histories.get(agent).is_none_or(|stored| {
                        stored.epoch != history.epoch
                            || stored.compacted != history.compacted
                            || stored.conversation != history.conversation
                            || stored.messages != history.messages
                    })
                });
                commit
                    .managed_histories
                    .retain(|agent| !current.managed_histories.contains(agent));
                commit.changes.retain(|change| match &change.change {
                    StateChange::Config(config) => {
                        current
                            .state
                            .iter()
                            .rev()
                            .find_map(|record| match &record.change {
                                StateChange::Config(config) if record.agent == change.agent => {
                                    Some(config)
                                }
                                _ => None,
                            })
                            != Some(config)
                    }
                    StateChange::Rules { .. } => false,
                    _ => true,
                });
                let latest = current
                    .state
                    .iter()
                    .rev()
                    .find_map(|record| match &record.change {
                        StateChange::Rules { rules, yolo } if record.agent.is_none() => {
                            Some((rules, *yolo))
                        }
                        _ => None,
                    });
                if latest != Some((&rules, yolo)) {
                    commit.changes.push(JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Rules { rules, yolo },
                    });
                }
                commit
            },
            || (),
        )
    }

    pub fn snapshot(&self) -> PermissionSnapshot {
        self.session().snapshot.clone()
    }

    pub fn restore(&self, snapshot: PermissionSnapshot) {
        self.session().snapshot = snapshot;
    }

    pub fn restore_stored(
        &self,
        rules: &[StoredRule],
        yolo: Option<bool>,
    ) -> Result<(), ToolKeyParseError> {
        self.restore(PermissionSnapshot::from_stored(rules, yolo)?);
        Ok(())
    }

    fn update_session<T>(
        &self,
        update: impl FnOnce(&mut PermissionSnapshot) -> T,
    ) -> Result<T, JournalError> {
        let binding = self.session().binding.clone();
        let _publication = binding
            .as_ref()
            .map(|binding| {
                binding.journal.try_publication_read().ok_or_else(|| {
                    JournalError::Preparation(
                        "permission mutation conflicts with prepared publication".into(),
                    )
                })
            })
            .transpose()?;
        self.update_session_bound(binding.as_ref(), update)
    }

    fn update_session_bound<T>(
        &self,
        expected: Option<&PermissionJournal>,
        update: impl FnOnce(&mut PermissionSnapshot) -> T,
    ) -> Result<T, JournalError> {
        let mut session = self.session();
        if !same_permission_binding(session.binding.as_ref(), expected) {
            return Err(JournalError::Preparation(
                "permission journal binding changed".into(),
            ));
        }
        let mut snapshot = session.snapshot.clone();
        let result = update(&mut snapshot);
        if snapshot.yolo == session.snapshot.yolo
            && snapshot.rules.len() == session.snapshot.rules.len()
            && snapshot
                .rules
                .iter()
                .zip(&session.snapshot.rules)
                .all(|(a, b)| a.tool == b.tool && a.scope == b.scope && a.effect == b.effect)
        {
            if let Some(binding) = &session.binding {
                binding.journal.commit(JournalCommit {
                    incarnations: binding.incarnation.clone().into_iter().collect(),
                    ..Default::default()
                })?;
            }
            return Ok(result);
        }
        if let Some(binding) = &session.binding {
            let journal = Arc::clone(&binding.journal);
            journal.record_rules_with_apply(
                binding.incarnation.clone(),
                snapshot.stored_rules(),
                snapshot.yolo,
                || session.snapshot = snapshot,
            )?;
        } else {
            session.snapshot = snapshot;
        }
        Ok(result)
    }

    fn add_session_rules(&self, rules: impl IntoIterator<Item = PermissionRule>) -> bool {
        let result = self.update_session(|snapshot| insert_session_rules(snapshot, rules));
        if let Err(error) = result {
            warn!(%error, "failed to commit session permission rules");
            return false;
        }
        true
    }

    pub fn add_session_rule(&self, rule: PermissionRule) {
        self.add_session_rules([rule]);
    }

    pub fn try_add_session_rule(&self, rule: PermissionRule) -> Result<(), JournalError> {
        self.update_session(|snapshot| insert_session_rules(snapshot, [rule]))
    }

    pub fn try_toggle_yolo(&self) -> Result<bool, JournalError> {
        self.update_session(|snapshot| {
            let enabled = !snapshot.yolo.unwrap_or(self.seed_yolo);
            snapshot.yolo = Some(enabled);
            enabled
        })
    }

    pub fn try_set_yolo(&self, enabled: bool) -> Result<(), JournalError> {
        self.update_session(|snapshot| snapshot.yolo = Some(enabled))
    }

    pub fn toggle_yolo(&self) -> bool {
        match self.try_toggle_yolo() {
            Ok(enabled) => enabled,
            Err(error) => {
                warn!(%error, "failed to commit session yolo intent");
                self.is_yolo()
            }
        }
    }

    pub fn set_yolo(&self, enabled: bool) {
        if let Err(error) = self.try_set_yolo(enabled) {
            warn!(%error, "failed to commit session yolo intent");
        }
    }

    /// Replaces stored intent without journaling during session restoration.
    pub fn set_session_yolo(&self, stored: Option<bool>) {
        self.session().snapshot.yolo = stored;
    }

    pub fn is_yolo(&self) -> bool {
        self.session().snapshot.yolo.unwrap_or(self.seed_yolo)
    }

    /// What the session may persist. A one-shot `--yolo` is a property of the
    /// invocation, so on its own it stores nothing.
    pub fn persisted_yolo(&self) -> Option<bool> {
        self.session().snapshot.yolo
    }

    pub fn set_cwd(&self, cwd: PathBuf) {
        *self.cwd.lock().unwrap_or_else(|error| error.into_inner()) = cwd.clone();
        *self
            .builtin_rules
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = builtin_rules(&cwd);
    }

    /// Outside-cwd paths are not blocked here. They flow through the normal
    /// permission prompt (which uses the same canonicalization via
    /// [`scope_matches`]). Only unresolvable boundaries are hard-blocked.
    pub fn boundary_block_reason(&self, path: &Path) -> Option<String> {
        let cwd = self.cwd.lock().unwrap_or_else(|error| error.into_inner());
        match physical_boundary_check(&cwd, path) {
            Some(_) => None,
            None => Some(format!(
                "{BOUNDARY_UNVERIFIABLE_PREFIX} {} \
                 (project root could not be resolved)",
                path.display()
            )),
        }
    }

    pub fn session_rules_snapshot(&self) -> Vec<PermissionRule> {
        self.session().snapshot.rules.clone()
    }

    pub fn load_session_rules(&self, rules: Vec<PermissionRule>) {
        self.session().snapshot.rules = rules;
    }

    pub fn project_is_trusted(&self) -> bool {
        self.project_config.is_trusted()
    }

    /// Where an always-answer gets written, or `None` when it only holds for
    /// this session. An untrusted folder gets nothing, for a different reason
    /// on each side. An allow saved there is stripped on the next start, so it
    /// would only look forgotten. A deny would survive, because an untrusted
    /// project still contributes its deny scopes, but saving it means Makima
    /// writing `.makima/permissions.toml` into a checkout the user declined to
    /// trust, and the next start would then ask them about a gated file Makima
    /// created itself. The durable deny is the global one, which lands in the
    /// user's own config and no repository can touch.
    fn persist_target(&self, answer: &PermissionAnswer) -> Option<PermissionTarget> {
        match answer {
            PermissionAnswer::AllowAlwaysProject
            | PermissionAnswer::AllowAlwaysLocal
            | PermissionAnswer::DenyAlwaysProject
            | PermissionAnswer::DenyAlwaysLocal => self
                .project_config
                .is_trusted()
                .then(|| PermissionTarget::Project(self.project_config.clone())),
            _ => Some(PermissionTarget::Global),
        }
    }

    pub fn apply_decision(&self, tool: &ToolKey, scopes: &[String], answer: &PermissionAnswer) {
        if let Err(error) = self.try_apply_decision(tool, scopes, answer) {
            warn!(%error, "failed to commit permission decision");
        }
    }

    #[cfg(test)]
    fn apply_decision_checked(
        &self,
        tool: &ToolKey,
        scopes: &[String],
        answer: &PermissionAnswer,
    ) -> bool {
        self.try_apply_decision(tool, scopes, answer).is_ok()
    }

    pub fn try_apply_decision(
        &self,
        tool: &ToolKey,
        scopes: &[String],
        answer: &PermissionAnswer,
    ) -> Result<(), JournalError> {
        let binding = self.session().binding.clone();
        let _publication = binding
            .as_ref()
            .map(|binding| {
                binding.journal.try_publication_read().ok_or_else(|| {
                    JournalError::Preparation(
                        "permission mutation conflicts with prepared publication".into(),
                    )
                })
            })
            .transpose()?;
        self.apply_decision_bound(tool, scopes, answer, binding.as_ref())
    }

    fn apply_decision_bound(
        &self,
        tool: &ToolKey,
        scopes: &[String],
        answer: &PermissionAnswer,
        binding: Option<&PermissionJournal>,
    ) -> Result<(), JournalError> {
        let resolved = if answer.is_allow() || tool.is_mcp() {
            // MCP scopes are always wildcarded — both allow and deny generalize to "*".
            // This makes session and persisted rules consistent: a deny on an MCP tool
            // blocks the tool entirely, not just the specific input that triggered it.
            generalized_scopes(tool, scopes)
        } else {
            scopes.to_vec()
        };

        match answer {
            PermissionAnswer::AllowOnce
            | PermissionAnswer::Deny
            | PermissionAnswer::DenyWithGuidance(_) => self.update_session_bound(binding, |_| ()),
            PermissionAnswer::AllowSession => self.update_session_bound(binding, |snapshot| {
                insert_session_rules(
                    snapshot,
                    resolved.iter().map(|scope| PermissionRule {
                        tool: tool.clone(),
                        scope: Some(scope.clone()),
                        effect: Effect::Allow,
                    }),
                );
            }),
            PermissionAnswer::AllowAlwaysProject
            | PermissionAnswer::AllowAlwaysLocal
            | PermissionAnswer::AllowAlwaysGlobal
            | PermissionAnswer::DenyAlwaysProject
            | PermissionAnswer::DenyAlwaysLocal
            | PermissionAnswer::DenyAlwaysGlobal => {
                let effect = if answer.is_allow() {
                    Effect::Allow
                } else {
                    Effect::Deny
                };
                let target = self.persist_target(answer);
                if target.is_none() {
                    info!(
                        tool = %tool,
                        "project answer stays in this session because the folder is not trusted, the global answer is the one that lasts"
                    );
                }
                self.update_session_bound(binding, |snapshot| {
                    insert_session_rules(
                        snapshot,
                        resolved.iter().map(|scope| PermissionRule {
                            tool: tool.clone(),
                            scope: Some(scope.clone()),
                            effect,
                        }),
                    );
                })?;
                for s in &resolved {
                    if let Some(target) = &target
                        && let Err(e) = append_permission_rule(tool, Some(s), effect, target)
                    {
                        tracing::warn!(error = %e, "failed to persist permission rule");
                    }
                }
                Ok(())
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn enforce(
        &self,
        tool: &ToolKey,
        scopes: &crate::tools::PermissionScopes,
        event_tx: &EventSender,
        user_response_rx: Option<&async_lock::Mutex<flume::Receiver<String>>>,
        request_id: &str,
        cancel: &CancelToken,
        plan_path: Option<&Path>,
    ) -> Result<(), PermissionError> {
        let scope_refs: Vec<&str> = scopes.scopes.iter().map(|s| s.as_str()).collect();
        let tool_string = tool.to_string();
        let scope_display = || scopes.scopes.join("; ");
        let deny = |guidance: Option<String>| match guidance {
            Some(g) => PermissionError::with_guidance(&tool_string, &scope_display(), g),
            None => PermissionError::new(&tool_string, &scope_display()),
        };

        let (pt, ps, force_prompt) =
            match self.check_inner(tool, &scope_refs, scopes.force_prompt, plan_path) {
                PermissionCheck::Allowed => return Ok(()),
                PermissionCheck::Denied => return Err(deny(None)),
                PermissionCheck::NeedsPrompt {
                    tool,
                    scopes,
                    force_prompt,
                } => (tool, scopes, force_prompt),
            };

        let Some(rx) = user_response_rx else {
            warn!(tool = %tool, scope = %scope_display(), "no permission response channel");
            return Err(deny(None));
        };

        let guard = rx.lock().await;
        let refs: Vec<&str> = ps.iter().map(|s| s.as_str()).collect();
        let (t2, s2) = match self.check_inner(&pt, &refs, force_prompt, plan_path) {
            PermissionCheck::Allowed => return Ok(()),
            PermissionCheck::Denied => return Err(deny(None)),
            PermissionCheck::NeedsPrompt { tool, scopes, .. } => (tool, scopes),
        };

        let prompt_binding = self.session().binding.clone();
        let _ = event_tx.send(AgentEvent::PermissionRequest {
            id: request_id.to_owned(),
            tool: t2.clone(),
            scopes: s2.clone(),
        });
        // Only the answer naming this ask may be applied. Anything else is a
        // leftover from a cancelled or reassigned ask, so it is dropped and the
        // wait continues: consuming it would approve or deny this tool on a
        // decision the user made about another one.
        let answer = loop {
            let response = cancel.race(guard.recv_async()).await;
            match response {
                Ok(Ok(raw)) => match TaggedAnswer::decode(&raw) {
                    Some(tagged) if tagged.request_id == request_id => break tagged.answer,
                    tagged => warn!(
                        tool = %tool,
                        scope = %scope_display(),
                        request_id,
                        answered = tagged.map(|t| t.request_id).unwrap_or_default(),
                        "discarding a permission answer that does not name this request"
                    ),
                },
                Ok(Err(_)) => {
                    drop(guard);
                    warn!(tool = %tool, scope = %scope_display(), "permission channel closed");
                    return Err(deny(None));
                }
                Err(_) => {
                    drop(guard);
                    return Err(deny(None));
                }
            }
        };
        drop(guard);
        let journal = prompt_binding
            .as_ref()
            .map(|binding| Arc::clone(&binding.journal));
        let _publication = if let Some(journal) = &journal {
            match cancel.race(journal.publication_read()).await {
                Ok(guard) => Some(guard),
                Err(_) => return Err(deny(None)),
            }
        } else {
            None
        };
        {
            let session = self.session();
            if !same_permission_binding(session.binding.as_ref(), prompt_binding.as_ref()) {
                return Err(deny(None));
            }
            if let Some(binding) = &prompt_binding
                && let Err(error) = binding.journal.commit(JournalCommit {
                    incarnations: binding.incarnation.clone().into_iter().collect(),
                    ..Default::default()
                })
            {
                warn!(%error, "discarding permission answer from stale incarnation");
                return Err(deny(None));
            }
        }
        if !cancel.claim_publication(&CheckpointPublication::default())
            || self
                .apply_decision_bound(&t2, &s2, &answer, prompt_binding.as_ref())
                .is_err()
        {
            return Err(deny(None));
        }
        if answer.is_allow() {
            Ok(())
        } else {
            Err(deny(answer.guidance().map(String::from)))
        }
    }
}

fn insert_session_rules(
    snapshot: &mut PermissionSnapshot,
    rules: impl IntoIterator<Item = PermissionRule>,
) {
    for rule in rules {
        if !snapshot.rules.iter().any(|existing| {
            existing.tool == rule.tool
                && existing.scope == rule.scope
                && existing.effect == rule.effect
        }) {
            snapshot.rules.push(rule);
        }
    }
}

fn same_permission_binding(a: Option<&PermissionJournal>, b: Option<&PermissionJournal>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(&a.journal, &b.journal) && a.incarnation == b.incarnation,
        (None, None) => true,
        _ => false,
    }
}

fn rule_matches_tool(rule_key: &ToolKey, actual: &ToolKey) -> bool {
    match (rule_key, actual) {
        (ToolKey::Wildcard, _) => true,
        (ToolKey::McpServer { server: rs }, ToolKey::McpTool { server: as_, .. }) => rs == as_,
        _ => rule_key == actual,
    }
}

fn rule_matches_scope(rule: &PermissionRule, scope: &str) -> bool {
    match &rule.scope {
        None => true,
        Some(pattern) => scope_matches(pattern, scope),
    }
}

/// Absolutize first, then resolve symlinks in the leading components that
/// exist and append the rest as written. The order matters: a relative rule
/// like `dist/**` has to match before the dir exists, and
/// `incremental_canonicalize` leaves a relative path relative when the leading
/// component is missing.
fn normalize_scope_prefix(path: &str) -> PathBuf {
    let abs = std::path::absolute(path).unwrap_or_else(|_| PathBuf::from(path));
    makima_storage::paths::incremental_canonicalize(&abs)
        .unwrap_or_else(|| makima_storage::paths::normalize_path(&abs))
}

/// A pattern with nothing left once its trailing glob is taken off covers
/// every scope: `*` and `**`, but also `/*` and `/**`, which reduce to a
/// prefix every absolute path starts with. [`scope_matches`] short-circuits on
/// these and a plugin allow is refused for them, off the same answer.
///
/// A `/**` prefix is normalized before the answer, the way the matcher reads
/// it, not compared as text. `//**`, `/./**` and `/tmp/../**` all name the
/// root once normalized, and going by their spelling would let a plugin
/// smuggle in the everything rule this refuses.
pub fn is_universal_scope(pattern: &str) -> bool {
    match pattern.strip_suffix("/**") {
        Some(prefix) => is_root(&normalize_scope_prefix(prefix)),
        None => {
            let stem = pattern.trim_end_matches('*');
            stem.len() < pattern.len() && matches!(stem, "" | "/")
        }
    }
}

fn is_root(path: &Path) -> bool {
    path.parent().is_none()
}

/// Glob matcher for permission scopes. The boundary suffixes (`/**`, `" *"`)
/// must be tried before the bare `*`, otherwise a plain prefix would swallow
/// them. `" *"` is the bash form `<command> *`: it has to match the bare
/// command too (`pwd *` covers `pwd` and `pwd -L`, but not `pwdx`).
///
/// For the `/**` path pattern, `Path::starts_with` is used to compare
/// components rather than characters, which handles both `/` and `\`
/// transparently on all platforms.
pub fn scope_matches(pattern: &str, value: &str) -> bool {
    if let Some(prefix) = pattern.strip_suffix("/**") {
        let norm_prefix = normalize_scope_prefix(prefix);
        // A root prefix covers every scope, bash commands included. Those are
        // not paths, so a plain prefix test would miss them.
        if is_root(&norm_prefix) {
            return true;
        }
        let norm_value = normalize_scope_prefix(value);
        return norm_value == norm_prefix || norm_value.starts_with(&norm_prefix);
    }
    if is_universal_scope(pattern) {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix(" *") {
        return value == prefix || value.starts_with(&format!("{prefix} "));
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

/// Lexical normalization for scope paths. Resolves `..` and `.` without
/// hitting the filesystem and without producing `\\?\` prefixes on Windows.
/// Use this for display, logging, and scope matching.
///
/// For symlink-aware security checks, use [`physical_boundary_check`].
pub fn normalize_scope_path(path: &str) -> String {
    let resolved = crate::tools::resolve_path(path).unwrap_or_else(|_| path.to_string());
    makima_storage::paths::normalize_path(Path::new(&resolved))
        .to_string_lossy()
        .into_owned()
}

/// Check whether `child` is physically inside `parent`, following symlinks.
///
/// Uses incremental left-to-right canonicalization: each component is
/// resolved through the filesystem (including symlinks) *before* any
/// subsequent `..` component can act on it. This prevents symlink-based
/// boundary escapes where a symlink followed by `..` resolves to a
/// location outside the parent.
///
/// Returns `true` only when the resolved filesystem location of `child`
/// is under `parent`. Returns `None` if the parent itself cannot be resolved.
pub fn physical_boundary_check(parent: &Path, child: &Path) -> Option<bool> {
    let parent_canon = makima_storage::paths::incremental_canonicalize(parent)?;
    let child_canon = makima_storage::paths::incremental_canonicalize(child)
        .unwrap_or_else(|| child.to_path_buf());
    Some(child_canon.starts_with(&parent_canon))
}

/// A `<cmd> *` rule is only safe when `<cmd>` names one program. The bash
/// plugin keeps block forms whole on purpose, so `for f in *.rs; do wc -l $f;
/// done` arrives as one scope and its first token is `for`; wildcarding that
/// hands out every loop the model can write. Same for a bodiless `> log`,
/// whose first token is the redirect. Those scopes stay literal: remembering
/// one exact command is worth little, but it is never a blank cheque.
fn generalize_bash_segment(segment: &str) -> String {
    let first_token = segment.split_whitespace().next().unwrap_or(segment);
    let is_command_word = !first_token.is_empty()
        && first_token
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.:/@+-".contains(c))
        && !SHELL_KEYWORDS.contains(&first_token);
    if is_command_word {
        format!("{first_token} *")
    } else {
        segment.to_string()
    }
}

pub fn generalized_scopes(tool: &ToolKey, scopes: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    scopes
        .iter()
        .map(|s| generalize_scope(tool, s))
        .filter(|g| seen.insert(g.clone()))
        .collect()
}

fn generalize_scope(tool: &ToolKey, scope: &str) -> String {
    match tool {
        ToolKey::Native(name) if name.as_ref() == BASH_TOOL => generalize_bash_segment(scope),
        ToolKey::Native(name) if FILE_WRITE_TOOLS.contains(&name.as_ref()) => {
            let p = Path::new(scope);
            match p.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => {
                    format!("{}/**", parent.display())
                }
                _ => "**".to_string(),
            }
        }
        // MCP tool calls have a scope equal to the JSON-stringified input.
        // "Allow always" should whitelist the tool regardless of its arguments,
        // so generalize the scope to `*`. The rule's `tool` field still gates
        // which MCP tool it applies to, keeping distinct tools distinct.
        ToolKey::McpTool { .. } | ToolKey::McpServer { .. } => "*".to_string(),
        _ => scope.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::JournalChange;
    use makima_providers::{Message, TokenUsage};
    use makima_storage::id::AgentId;
    use makima_storage::session_state::StateChange;
    use makima_storage::sessions::Session;
    use std::sync::Barrier;
    use std::thread;
    use test_case::test_case;

    const PLAN_FILE: &str = "/home/user/.local/state/makima/plans/test.md";
    const TEST_CWD: &str = "/tmp";
    const MCP_SERVER: &str = "deepwiki";
    const MCP_TOOL: &str = "deepwiki.search";
    const MCP_ARGS: &str = "{\"q\":\"makima\"}";
    const READ_TOOL: &str = "read";
    const READ_SCOPE: &str = "/home/user/project/src/main.rs";

    const ALLOWED: &str = "allowed";
    const DENIED: &str = "denied";
    const PROMPTS: &str = "prompts";
    const PROJECT_DIR: &str = ".makima";
    const PROJECT_PERMISSIONS: &str = ".makima/permissions.toml";

    fn outcome(check: PermissionCheck) -> &'static str {
        match check {
            PermissionCheck::Allowed => ALLOWED,
            PermissionCheck::Denied => DENIED,
            PermissionCheck::NeedsPrompt { .. } => PROMPTS,
        }
    }

    fn mcp_tool_key() -> ToolKey {
        ToolKey::parse(MCP_TOOL).expect("test tool key parses")
    }

    fn mcp_server_key() -> ToolKey {
        ToolKey::McpServer {
            server: MCP_SERVER.into(),
        }
    }
    fn make_config(rules: Vec<PermissionRule>) -> PermissionsConfig {
        PermissionsConfig {
            rules,
            ..Default::default()
        }
    }

    fn allow_rule(scope: &str) -> PermissionRule {
        PermissionRule {
            tool: ToolKey::native("bash"),
            scope: Some(scope.into()),
            effect: Effect::Allow,
        }
    }

    fn deny_rule(scope: &str) -> PermissionRule {
        PermissionRule {
            tool: ToolKey::native("bash"),
            scope: Some(scope.into()),
            effect: Effect::Deny,
        }
    }

    fn mgr_with(config: PermissionsConfig, cwd: PathBuf) -> PermissionManager {
        let project_config = ProjectConfig::for_project(&cwd);
        PermissionManager::new(config, cwd, project_config, Arc::default())
    }

    fn default_mgr() -> PermissionManager {
        mgr_with(PermissionsConfig::default(), PathBuf::from("/tmp"))
    }

    fn plugin_edit_rule(scope: &str, effect: Effect) -> PermissionRule {
        PermissionRule {
            tool: ToolKey::native("edit"),
            scope: Some(scope.into()),
            effect,
        }
    }

    #[test_case("*", "anything" => true ; "star")]
    #[test_case("cargo *", "cargo test" => true ; "prefix")]
    #[test_case("cargo *", "git push" => false ; "prefix_no_match")]
    #[test_case("pwd *", "pwd" => true ; "space_star_matches_bare_command")]
    #[test_case("pwd *", "pwd -L" => true ; "space_star_matches_with_args")]
    #[test_case("pwd *", "pwdx" => false ; "space_star_no_partial_token")]
    #[test_case("src/**", "src/main.rs" => true ; "glob")]
    #[test_case("src/**", "src/deep/nested/file.rs" => true ; "glob_deep_nested")]
    #[test_case("src/**", "src" => true ; "glob_exact_prefix")]
    #[test_case("src/**", "srcfoo" => false ; "glob_no_bare_prefix")]
    #[test_case("src/**", "other/src/main.rs" => false ; "glob_no_inner_match")]
    fn scope_match(pattern: &str, value: &str) -> bool {
        scope_matches(pattern, value)
    }

    /// A pattern is universal only when the trailing glob is all it says.
    #[test_case("*" => true ; "star")]
    #[test_case("**" => true ; "double_star")]
    #[test_case("/*" => true ; "root_star")]
    #[test_case("/**" => true ; "root_double_star")]
    #[test_case("//**" => true ; "doubled_root_slash")]
    #[test_case("/./**" => true ; "root_dot")]
    #[test_case("/usr/../**" => true ; "root_by_parent")]
    #[test_case("/tmp/**" => false ; "directory_subtree")]
    #[test_case("cargo *" => false ; "bash_command")]
    #[test_case("/" => false ; "root_without_glob")]
    #[test_case("" => false ; "empty")]
    fn universal_scope(pattern: &str) -> bool {
        is_universal_scope(pattern)
    }
    #[test_case(vec!["cd /tmp", "cargo test"], vec!["cd *", "cargo *"], true ; "all_allowed")]
    #[test_case(vec!["cd /tmp", "cargo test"], vec!["cargo *"], false ; "missing_rule")]
    fn compound_check(scopes: Vec<&str>, rules: Vec<&str>, expect_allowed: bool) {
        let mgr = mgr_with(
            make_config(rules.into_iter().map(allow_rule).collect()),
            PathBuf::from("/tmp"),
        );
        let check = mgr.check_multi(&ToolKey::native("bash"), &scopes, false, None);
        assert_eq!(matches!(check, PermissionCheck::Allowed), expect_allowed);
    }

    #[test]
    fn compound_denied_if_any_segment_denied() {
        let mgr = mgr_with(
            make_config(vec![
                allow_rule("cd *"),
                allow_rule("cargo *"),
                deny_rule("rm *"),
            ]),
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check_multi(
                &ToolKey::native("bash"),
                &["cd /tmp", "cargo test", "rm -rf /"],
                false,
                None
            ),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn complex_constructs_force_prompt_even_with_allow_star() {
        let mgr = mgr_with(make_config(vec![allow_rule("*")]), PathBuf::from("/tmp"));
        assert!(matches!(
            mgr.check_multi(&ToolKey::native("bash"), &["echo $(whoami)"], true, None),
            PermissionCheck::NeedsPrompt { .. }
        ));
    }

    #[test_case("write", "/tmp/file.txt" => true ; "write_in_cwd")]
    #[test_case("write", "/etc/passwd" => false ; "write_outside_cwd")]
    #[test_case("task_spawn", "research" => true ; "task_spawn_allowed")]
    #[test_case("bash", "cargo test" => false ; "bash_prompts")]
    fn builtin_check(tool: &str, scope: &str) -> bool {
        matches!(
            default_mgr().check(&ToolKey::native(tool), scope, None),
            PermissionCheck::Allowed
        )
    }

    #[test]
    #[cfg(unix)]
    fn scope_matches_resolves_symlinked_parent() {
        let tmp = std::env::temp_dir();
        let real = tmp.join("__makima_test_scope_symlink_real");
        let link = tmp.join("__makima_test_scope_symlink_link");
        let _ = std::fs::remove_dir_all(&real);
        let _ = std::fs::remove_file(&link);
        std::fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let pattern = format!("{}/**", real.display());
        let value = format!("{}/new_file.txt", link.display());
        assert!(
            scope_matches(&pattern, &value),
            "symlinked parent should resolve: pattern={pattern}, value={value}"
        );

        let _ = std::fs::remove_dir_all(&real);
        let _ = std::fs::remove_file(&link);
    }

    #[test]
    fn scope_matches_relative_pattern_before_dir_exists() {
        // A relative rule like `dist/**` must match an absolute value even
        // before the directory exists.
        let cwd = std::env::current_dir().unwrap();
        let value = cwd.join("__makima_nonexistent_dist/file.txt");
        assert!(!value.exists(), "test dir must not exist");
        assert!(
            scope_matches("__makima_nonexistent_dist/**", &value.to_string_lossy()),
            "relative pattern should match absolute value: value={}",
            value.display()
        );
    }

    #[test]
    #[cfg(unix)]
    fn scope_matches_symlinked_parent_with_nonexistent_tail() {
        // Regression: symlinked leading component plus a non-existent tail
        // (`proj`). Both sides must resolve the symlink before appending the
        // lexical tail, else the prefix stays lexical and this returns false.
        let tmp = std::env::temp_dir();
        let real = tmp.join("__makima_test_scope_symlink_tail_real");
        let link = tmp.join("__makima_test_scope_symlink_tail_link");
        let _ = std::fs::remove_dir_all(&real);
        let _ = std::fs::remove_file(&link);
        std::fs::create_dir_all(&real).unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        // `proj` under the symlink does not exist.
        let pattern = format!("{}/proj/**", link.display());
        let value = format!("{}/proj/file.txt", link.display());
        assert!(
            scope_matches(&pattern, &value),
            "symlinked parent with non-existent tail should match: pattern={pattern}, value={value}"
        );

        let _ = std::fs::remove_dir_all(&real);
        let _ = std::fs::remove_file(&link);
    }

    #[test]
    fn path_traversal_prompts() {
        let path = normalize_scope_path("/tmp/../etc/passwd");
        assert!(matches!(
            default_mgr().check(&ToolKey::native("write"), &path, None),
            PermissionCheck::NeedsPrompt { .. }
        ));
    }

    #[test]
    fn session_rule_overrides_config() {
        let mgr = mgr_with(
            make_config(vec![allow_rule("cargo *")]),
            PathBuf::from("/tmp"),
        );
        mgr.add_session_rule(deny_rule("cargo *"));
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn deny_overrides_default_allow() {
        let mgr = mgr_with(
            PermissionsConfig {
                default: DefaultEffect::Allow,
                rules: vec![deny_rule("rm *")],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "rm -rf /", None),
            PermissionCheck::Denied
        ));
    }

    // When you allow "cargo test", we generalize to "cargo *" for convenience.
    // But denies stay exact, you probably have a good reason to block that specific thing.
    #[test]
    fn allow_decision_generalizes() {
        let mgr = default_mgr();
        mgr.apply_decision(
            &ToolKey::native("bash"),
            &["cargo test --all".into()],
            &PermissionAnswer::AllowSession,
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo build", None),
            PermissionCheck::Allowed
        ));
    }

    #[test]
    fn deny_decision_uses_exact() {
        let mgr = default_mgr();
        mgr.apply_decision(
            &ToolKey::native("bash"),
            &["cargo test".into()],
            &PermissionAnswer::DenyAlwaysLocal,
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Denied
        ));
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo build", None),
            PermissionCheck::NeedsPrompt { .. }
        ));
    }

    /// A trusted folder collects the answer in `.makima/permissions.toml`. An
    /// untrusted one keeps it for the session and gets no `.makima` directory,
    /// for deny as much as for allow, because declining to trust a checkout
    /// has to leave nothing behind in it. The deny that lasts is the global
    /// one.
    #[test_case(PermissionAnswer::AllowAlwaysProject, true, true ; "allow_is_written_when_trusted")]
    #[test_case(PermissionAnswer::DenyAlwaysProject, false, true ; "deny_is_written_when_trusted")]
    #[test_case(PermissionAnswer::AllowAlwaysProject, true, false ; "allow_stays_in_the_session_when_untrusted")]
    #[test_case(PermissionAnswer::DenyAlwaysProject, false, false ; "deny_stays_in_the_session_when_untrusted")]
    fn project_answer_is_written_only_in_a_trusted_folder(
        answer: PermissionAnswer,
        allowed: bool,
        trusted: bool,
    ) {
        let project = tempfile::tempdir().unwrap();
        let mgr = PermissionManager::new(
            PermissionsConfig::default(),
            project.path().to_path_buf(),
            if trusted {
                ProjectConfig::for_project(project.path())
            } else {
                ProjectConfig::discover(project.path())
            },
            Arc::default(),
        );

        mgr.apply_decision(&ToolKey::native("bash"), &["cargo test".into()], &answer);

        assert_eq!(mgr.session_rules_snapshot().len(), 1);
        assert_eq!(project.path().join(PROJECT_DIR).exists(), trusted);
        assert_eq!(project.path().join(PROJECT_PERMISSIONS).is_file(), trusted);
        let check = mgr.check(&ToolKey::native("bash"), "cargo test", None);
        assert_eq!(matches!(check, PermissionCheck::Allowed), allowed);
        assert_eq!(matches!(check, PermissionCheck::Denied), !allowed);
    }

    #[test]
    fn boundary_inside_proceeds() {
        let tmp = std::env::temp_dir();
        let mgr = mgr_with(PermissionsConfig::default(), tmp.clone());
        assert!(
            mgr.boundary_block_reason(&tmp.join("some_file.txt"))
                .is_none()
        );
    }

    #[test]
    fn boundary_outside_proceeds_via_prompt() {
        let tmp = std::env::temp_dir();
        let mgr = mgr_with(PermissionsConfig::default(), tmp);
        #[cfg(unix)]
        let outside = Path::new("/etc/hosts");
        #[cfg(windows)]
        let outside = Path::new(r"C:\Windows\System32\drivers\etc\hosts");
        assert!(mgr.boundary_block_reason(outside).is_none());
    }

    #[test]
    fn boundary_dotdot_smuggling_proceeds_via_prompt() {
        let tmp = std::env::temp_dir();
        let sub = tmp.join("__makima_test_boundary");
        std::fs::create_dir_all(&sub).unwrap();
        #[cfg(unix)]
        let attack = sub
            .join("x")
            .join("..")
            .join("..")
            .join("..")
            .join("etc")
            .join("passwd");
        #[cfg(windows)]
        let attack = sub
            .join("x")
            .join("..")
            .join("..")
            .join("..")
            .join("Windows")
            .join("System32");
        let mgr = mgr_with(PermissionsConfig::default(), sub.clone());
        assert!(
            mgr.boundary_block_reason(&attack).is_none(),
            "outside-cwd dotdot path should prompt, not hard-block: {}",
            attack.display()
        );
        let _ = std::fs::remove_dir_all(&sub);
    }

    #[test]
    #[cfg(unix)]
    fn boundary_symlink_escape_proceeds_via_prompt() {
        // Lexical normalization resolves this inside (/project/escape), but
        // incremental canonicalization follows the symlink first, so `..`
        // escapes outside. The permission prompt catches it, not this function.
        let tmp = std::env::temp_dir();
        let project = tmp.join("__makima_test_symlink_escape");
        let _ = std::fs::remove_dir_all(&project);
        std::fs::create_dir_all(&project).unwrap();
        let link = project.join("link");
        let _ = std::os::unix::fs::symlink(&tmp, &link);

        let attack = link.join("..").join("escape_target");
        let mgr = mgr_with(PermissionsConfig::default(), project.clone());
        assert!(
            mgr.boundary_block_reason(&attack).is_none(),
            "outside-boundary edits are gated by the prompt, not hard-blocked: {}",
            attack.display()
        );
        let _ = std::fs::remove_dir_all(&project);
    }

    #[test]
    fn boundary_nonexistent_cwd_proceeds_via_lexical_tail() {
        let missing = std::env::temp_dir().join("__makima_test_absent_cwd_xyz");
        let _ = std::fs::remove_dir_all(&missing);
        let mgr = mgr_with(PermissionsConfig::default(), missing.clone());
        assert!(
            mgr.boundary_block_reason(&missing.join("file.txt"))
                .is_none()
        );
    }

    #[test]
    fn permission_answer_roundtrip() {
        for a in [
            PermissionAnswer::AllowOnce,
            PermissionAnswer::AllowSession,
            PermissionAnswer::AllowAlwaysLocal,
            PermissionAnswer::Deny,
            PermissionAnswer::DenyWithGuidance("hint".into()),
        ] {
            assert_eq!(PermissionAnswer::decode(&a.encode()), Some(a));
        }
    }

    #[test]
    fn check_multi_force_prompt_skips_allow_rules() {
        let mgr = mgr_with(
            make_config(vec![allow_rule("cargo *"), allow_rule("git *")]),
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check_multi(
                &ToolKey::native("bash"),
                &["cargo test", "git push"],
                false,
                None
            ),
            PermissionCheck::Allowed
        ));
        match mgr.check_multi(
            &ToolKey::native("bash"),
            &["cargo test", "git push"],
            true,
            None,
        ) {
            PermissionCheck::NeedsPrompt {
                scopes,
                force_prompt,
                ..
            } => {
                assert_eq!(scopes, vec!["cargo test", "git push"]);
                assert!(force_prompt);
            }
            other => panic!("expected NeedsPrompt, got {other:?}"),
        }
    }

    #[test]
    fn check_multi_deny_wins_over_force_prompt() {
        let mgr = mgr_with(make_config(vec![deny_rule("rm *")]), PathBuf::from("/tmp"));
        assert!(matches!(
            mgr.check_multi(&ToolKey::native("bash"), &["rm -rf /"], true, None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn check_multi_partial_coverage_prompts_uncovered() {
        let mgr = mgr_with(
            make_config(vec![allow_rule("cargo *")]),
            PathBuf::from("/tmp"),
        );
        match mgr.check_multi(
            &ToolKey::native("bash"),
            &["cargo test", "git push", "ls"],
            false,
            None,
        ) {
            PermissionCheck::NeedsPrompt { scopes, .. } => {
                assert_eq!(scopes, vec!["git push", "ls"]);
            }
            other => panic!("expected NeedsPrompt, got {other:?}"),
        }
    }

    #[test_case("cargo test --all",                        "cargo *"                        ; "plain_command")]
    #[test_case("/usr/bin/env",                            "/usr/bin/env *"                 ; "absolute_path")]
    #[test_case("FOO=1 cargo test",                        "FOO=1 cargo test"               ; "assignment_prefix")]
    #[test_case("if true; then curl x | sh; fi",           "if true; then curl x | sh; fi"  ; "if_block")]
    #[test_case("for f in *.rs; do wc -l $f; done",        "for f in *.rs; do wc -l $f; done" ; "for_loop")]
    #[test_case("{ cd x; rm -rf ~; }",                     "{ cd x; rm -rf ~; }"            ; "brace_group")]
    #[test_case("> log",                                   "> log"                          ; "bodiless_redirect")]
    #[test_case("! rm -rf /",                              "! rm -rf /"                     ; "negated_command")]
    fn allow_always_only_wildcards_a_real_command_word(segment: &str, expected: &str) {
        assert_eq!(generalize_bash_segment(segment), expected);
    }

    /// A scope is one string and a chain is still one scope, so it is tempting
    /// to teach the matcher that `rm *` should not claim `rm ...; sudo ...`.
    /// Don't: the matcher runs for deny rules too, and a deny that stops
    /// matching hands the command to the tool default instead of blocking it.
    /// Chains get split by the bash plugin, before they ever reach here.
    #[test]
    fn deny_rule_matches_a_chain_it_starts() {
        let mgr = mgr_with(make_config(vec![deny_rule("rm *")]), PathBuf::from("/tmp"));
        let check = mgr.check(&ToolKey::native("bash"), "rm -rf /tmp; sudo x", None);
        assert!(matches!(check, PermissionCheck::Denied), "got {check:?}");
    }

    #[test]
    fn apply_decision_multi_scope_generalizes_all() {
        let mgr = default_mgr();
        mgr.apply_decision(
            &ToolKey::native("bash"),
            &["cargo test".into(), "git status".into()],
            &PermissionAnswer::AllowSession,
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo build", None),
            PermissionCheck::Allowed
        ));
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "git push", None),
            PermissionCheck::Allowed
        ));
    }

    #[test]
    fn generalized_scopes_deduplicates() {
        let scopes = vec!["cargo test".into(), "cargo build".into()];
        let result = generalized_scopes(&ToolKey::native("bash"), &scopes);
        assert_eq!(result, vec!["cargo *"]);
    }

    #[test]
    fn generalized_scopes_preserves_distinct() {
        let scopes = vec!["cargo test".into(), "git status".into()];
        let result = generalized_scopes(&ToolKey::native("bash"), &scopes);
        assert_eq!(result, vec!["cargo *", "git *"]);
    }

    #[test_case("webfetch", "some:scope" => "some:scope" ; "unknown_tool_preserves_exact")]
    #[test_case("myserver.fetch", "{\"url\":\"https://a\"}" => "*" ; "mcp_tool_generalizes_to_wildcard")]
    fn generalize_single_scope(tool: &str, scope: &str) -> String {
        generalized_scopes(&ToolKey::parse(tool).unwrap(), &[scope.into()])
            .into_iter()
            .next()
            .unwrap()
    }

    #[test]
    fn generalize_edit_uses_parent_dir() {
        let result = generalize_scope(&ToolKey::native("edit"), "/home/user/project/src/main.rs");
        let expected = format!(
            "{}/**",
            Path::new("/home/user/project/src/main.rs")
                .parent()
                .unwrap()
                .display()
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn generalize_edit_root_file() {
        let result = generalize_scope(&ToolKey::native("edit"), "/Cargo.toml");
        let expected = format!(
            "{}/**",
            Path::new("/Cargo.toml").parent().unwrap().display()
        );
        assert_eq!(result, expected);
    }

    /// "Allow always" stores a command's generalized scope as a rule, so the
    /// command must match the very rule it would create. When this broke, the
    /// bare `pwd` never matched its own `pwd *` rule and we reprompted forever.
    #[test_case("bash", "pwd" ; "bash_bare_command")]
    #[test_case("bash", "cargo test" ; "bash_command_with_args")]
    #[test_case("bash", "git status --short" ; "bash_command_with_flags")]
    #[test_case("edit", "/home/user/project/src/main.rs" ; "edit_path")]
    #[test_case("webfetch", "https://example.com" ; "unknown_tool_exact")]
    #[test_case("myfetch.search", "{\"url\":\"https://a\"}" ; "mcp_tool_call")]
    fn command_matches_its_own_generalized_rule(tool: &str, scope: &str) {
        let tool_key = ToolKey::parse(tool).unwrap();
        let rule = &generalized_scopes(&tool_key, &[scope.into()])[0];
        assert!(
            scope_matches(rule, scope),
            "{scope:?} does not match its generalized rule {rule:?}"
        );
    }

    /// "Allow always" on an MCP tool generalizes the stored scope to `*`, so a
    /// later call with different arguments matches the persisted rule instead of
    /// reprompting, while a different MCP tool is still gated by its `tool` name.
    #[test]
    fn mcp_allow_always_matches_any_args_but_stays_per_tool() {
        let mgr = default_mgr();
        mgr.apply_decision(
            &ToolKey::parse("myfetch.search").unwrap(),
            &["{\"url\":\"https://a\"}".into()],
            &PermissionAnswer::AllowSession,
        );
        // Same tool, different arguments -> allowed without reprompting.
        assert!(matches!(
            mgr.check(
                &ToolKey::parse("myfetch.search").unwrap(),
                "{\"url\":\"https://b\"}",
                None
            ),
            PermissionCheck::Allowed
        ));
        // A distinct MCP tool is not covered by the fetch rule.
        assert!(!matches!(
            mgr.check(
                &ToolKey::parse("myfetch.exec").unwrap(),
                "{\"cmd\":\"ls\"}",
                None
            ),
            PermissionCheck::Allowed
        ));
    }

    #[test]
    fn deny_rule_with_none_scope_blocks_everything() {
        let mgr = mgr_with(
            make_config(vec![PermissionRule {
                tool: ToolKey::native("bash"),
                scope: None,
                effect: Effect::Deny,
            }]),
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "anything", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn wildcard_deny_blocks_all_tools() {
        let mgr = mgr_with(
            make_config(vec![PermissionRule {
                tool: ToolKey::Wildcard,
                scope: None,
                effect: Effect::Deny,
            }]),
            PathBuf::from("/tmp"),
        );
        // Any deny wins: Wildcard deny blocks everything including builtins
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "ls", None),
            PermissionCheck::Denied
        ));
        assert!(matches!(
            mgr.check(&ToolKey::native("write"), "/tmp/x", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn mcp_deny_always_blocks_all_arguments() {
        let mgr = mgr_with(make_config(vec![]), PathBuf::from("/tmp"));
        let tool = ToolKey::McpTool {
            server: "deepwiki".into(),
            tool: "search".into(),
        };
        // User denies with specific arguments — should generalize to block all.
        mgr.apply_decision(
            &tool,
            &["{\"q\":\"dangerous\"}".into()],
            &PermissionAnswer::DenyAlwaysLocal,
        );
        // Different arguments: still denied.
        assert!(matches!(
            mgr.check(&tool, "{\"q\":\"safe\"}", None),
            PermissionCheck::Denied
        ));
        // Even wildcard scope: denied.
        assert!(matches!(
            mgr.check(&tool, "*", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn explicit_yolo_setter_is_idempotent() {
        let mgr = seeded_mgr(true);

        mgr.set_yolo(true);
        mgr.set_yolo(true);
        assert!(mgr.is_yolo());
        assert_eq!(mgr.persisted_yolo(), Some(true));
        mgr.set_yolo(false);
        mgr.set_yolo(false);
        assert!(!mgr.is_yolo());
        assert_eq!(mgr.persisted_yolo(), Some(false));
    }

    fn seeded_mgr(yolo: bool) -> PermissionManager {
        mgr_with(
            PermissionsConfig {
                yolo,
                ..Default::default()
            },
            PathBuf::from("/tmp"),
        )
    }

    fn yolo_state(mgr: &PermissionManager) -> (bool, Option<bool>) {
        let forked = mgr.fork();
        assert_eq!(
            (forked.is_yolo(), forked.persisted_yolo()),
            (mgr.is_yolo(), mgr.persisted_yolo()),
        );
        (mgr.is_yolo(), mgr.persisted_yolo())
    }

    #[test_case(false, None        => (false, None)        ; "no_flag_and_no_intent_stays_off")]
    #[test_case(true,  None        => (true,  None)        ; "the_flag_applies_but_is_never_stored")]
    #[test_case(false, Some(true)  => (true,  Some(true))  ; "stored_on_comes_back_without_the_flag")]
    #[test_case(true,  Some(true)  => (true,  Some(true))  ; "the_flag_does_not_wipe_stored_on")]
    #[test_case(true,  Some(false) => (false, Some(false)) ; "stored_off_overrides_the_flag")]
    #[test_case(false, Some(false) => (false, Some(false)) ; "stored_off_stays_off")]
    fn a_stored_yolo_intent_replaces_the_seed(
        seed: bool,
        stored: Option<bool>,
    ) -> (bool, Option<bool>) {
        let mgr = seeded_mgr(seed);
        mgr.set_session_yolo(stored);
        yolo_state(&mgr)
    }

    #[test_case(false => (true,  Some(true))  ; "toggling_on_claims_the_session")]
    #[test_case(true  => (false, Some(false)) ; "toggling_off_under_the_flag_claims_the_session")]
    fn toggling_yolo_records_the_intent(seed: bool) -> (bool, Option<bool>) {
        let mgr = seeded_mgr(seed);
        assert_eq!(mgr.toggle_yolo(), !seed);
        yolo_state(&mgr)
    }

    #[test]
    fn yolo_mode_allows_but_deny_still_blocks() {
        let mgr = mgr_with(make_config(vec![deny_rule("rm *")]), PathBuf::from("/tmp"));
        mgr.toggle_yolo();
        assert!(mgr.is_yolo());
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Allowed
        ));
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "rm -rf /", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn rules_journal_batches_decisions_and_ignores_noops_and_restore() {
        let manager = seeded_mgr(true);
        let journal = Arc::new(SessionJournal::default());
        manager.bind_journal(Arc::clone(&journal), None).unwrap();
        let restored = PermissionSnapshot {
            rules: vec![deny_rule("rm *")],
            yolo: Some(false),
        };
        manager.restore(restored.clone());
        manager.load_session_rules(restored.rules.clone());
        manager.set_session_yolo(restored.yolo);
        manager.set_yolo(false);
        for answer in [PermissionAnswer::AllowOnce, PermissionAnswer::Deny] {
            manager.apply_decision(&ToolKey::native(BASH_TOOL), &["cargo test".into()], &answer);
        }
        assert_eq!(journal.snapshot().revision, 0);

        let scopes = [
            "cargo test".into(),
            "git status".into(),
            "cargo check".into(),
        ];
        manager.apply_decision(
            &ToolKey::native(BASH_TOOL),
            &scopes,
            &PermissionAnswer::AllowSession,
        );
        let committed = journal.snapshot();
        assert_eq!(committed.revision, 1);
        assert_eq!(committed.state.len(), 1);
        assert!(committed.state[0].agent.is_none());
        match &committed.state[0].change {
            StateChange::Rules { rules, yolo } => {
                assert_eq!(rules, &manager.snapshot().stored_rules());
                assert_eq!(rules.len(), 3);
                assert_eq!(*yolo, Some(false));
            }
            other => panic!("unexpected permission change: {other:?}"),
        }
        manager.apply_decision(
            &ToolKey::native(BASH_TOOL),
            &scopes,
            &PermissionAnswer::AllowSession,
        );
        manager.set_yolo(false);
        assert_eq!(journal.snapshot().revision, committed.revision);
        manager.set_yolo(true);
        assert_eq!(journal.snapshot().revision, committed.revision + 1);
    }

    #[test]
    fn permission_snapshots_remain_coherent_during_concurrent_restore() {
        const ITERATIONS: usize = 256;
        let manager = Arc::new(default_mgr());
        let states = [
            PermissionSnapshot {
                rules: vec![deny_rule("rm *")],
                yolo: Some(false),
            },
            PermissionSnapshot {
                rules: vec![allow_rule("cargo *")],
                yolo: Some(true),
            },
        ];
        manager.restore(states[0].clone());
        let barrier = Arc::new(Barrier::new(2));
        thread::scope(|scope| {
            scope.spawn(|| {
                for index in 0..ITERATIONS {
                    barrier.wait();
                    manager.restore(states[index % states.len()].clone());
                    barrier.wait();
                }
            });
            for _ in 0..ITERATIONS {
                barrier.wait();
                let snapshot = manager.snapshot();
                let expected = &states[usize::from(snapshot.yolo.unwrap())];
                assert_eq!(snapshot.stored_rules(), expected.stored_rules());
                barrier.wait();
            }
        });
    }

    #[test]
    fn concurrent_permission_mutations_journal_complete_snapshots() {
        const WORKERS: usize = 8;
        let manager = Arc::new(default_mgr());
        let journal = Arc::new(SessionJournal::default());
        manager.bind_journal(Arc::clone(&journal), None).unwrap();
        let barrier = Barrier::new(WORKERS);
        thread::scope(|scope| {
            for index in 0..WORKERS {
                let barrier = &barrier;
                let manager = &manager;
                scope.spawn(move || {
                    barrier.wait();
                    manager.add_session_rule(allow_rule(&format!("command-{index} *")));
                    manager.toggle_yolo();
                });
            }
        });
        let snapshot = journal.snapshot();
        assert_eq!(snapshot.state.len(), WORKERS * 2);
        let mut previous_rule_count = 0;
        for record in &snapshot.state {
            let StateChange::Rules { rules, .. } = &record.change else {
                panic!("unexpected permission change");
            };
            assert!(rules.len() >= previous_rule_count);
            assert!(rules.len() <= previous_rule_count + 1);
            previous_rule_count = rules.len();
        }
        let StateChange::Rules { rules, yolo } = &snapshot.state.last().unwrap().change else {
            panic!("unexpected permission change");
        };
        assert_eq!(rules, &manager.snapshot().stored_rules());
        assert_eq!(rules.len(), WORKERS);
        assert_eq!(*yolo, Some(false));
        assert_eq!(manager.persisted_yolo(), *yolo);
    }

    #[test]
    fn forked_permissions_do_not_share_rules_or_journal_bindings() {
        let original = default_mgr();
        let journal = Arc::new(SessionJournal::default());
        original.bind_journal(Arc::clone(&journal), None).unwrap();
        original.add_session_rule(deny_rule("rm *"));
        original.set_yolo(true);
        let fork = original.fork();
        assert!(fork.snapshot().rules.is_empty());
        assert_eq!(fork.snapshot().yolo, Some(true));
        let original_rules = original.snapshot().stored_rules();
        let committed = journal.snapshot().revision;
        fork.add_session_rule(allow_rule("git *"));
        fork.set_yolo(false);
        assert_eq!(journal.snapshot().revision, committed);
        assert_eq!(original.snapshot().stored_rules(), original_rules);
        assert_eq!(original.persisted_yolo(), Some(true));
        assert_eq!(fork.persisted_yolo(), Some(false));
    }

    #[test]
    fn restore_rebuilds_ambient_permissions_instead_of_persisting_them() {
        let plugins = Arc::new(PluginRuleStore::default());
        plugins.replace("permission-test", vec![allow_rule("git *")]);
        let original = PermissionManager::new(
            make_config(vec![deny_rule("rm *")]),
            PathBuf::from(TEST_CWD),
            ProjectConfig::for_project(Path::new(TEST_CWD)),
            plugins,
        );
        original.add_session_rule(allow_rule("cargo *"));
        let stored = original.snapshot();
        assert_eq!(stored.rules.len(), 1);
        let restored_plugins = Arc::new(PluginRuleStore::default());
        restored_plugins.replace("permission-test", vec![deny_rule("git *")]);
        let directory = tempfile::tempdir().unwrap();
        let restored = PermissionManager::new(
            make_config(vec![deny_rule("cargo *")]),
            directory.path().to_path_buf(),
            ProjectConfig::for_project(directory.path()),
            restored_plugins,
        );
        restored.restore(stored);
        assert_eq!(
            outcome(restored.check(&ToolKey::native(BASH_TOOL), "cargo test", None)),
            DENIED
        );
        assert_eq!(
            outcome(restored.check(&ToolKey::native(BASH_TOOL), "git status", None)),
            DENIED
        );
        assert_eq!(
            outcome(restored.check(&ToolKey::native(BASH_TOOL), "rm file", None)),
            PROMPTS
        );
        let path = directory.path().join("restored.txt");
        assert_eq!(
            outcome(restored.check(&ToolKey::native("write"), &path.to_string_lossy(), None)),
            ALLOWED
        );
        assert_eq!(restored.snapshot().rules.len(), 1);
    }

    #[test]
    fn stale_permission_binding_cannot_mutate_runtime_or_journal() {
        let manager = default_mgr();
        let journal = Arc::new(SessionJournal::default());
        let agent = AgentId::generate();
        journal.set_incarnation(agent, 1);
        manager
            .bind_journal(Arc::clone(&journal), Some(journal.incarnation(agent)))
            .unwrap();
        journal.set_incarnation(agent, 2);
        manager.add_session_rule(allow_rule("cargo *"));
        manager.set_yolo(true);
        assert!(!manager.apply_decision_checked(
            &ToolKey::native(BASH_TOOL),
            &["git status".into()],
            &PermissionAnswer::AllowSession
        ));
        assert!(manager.snapshot().rules.is_empty());
        assert_eq!(manager.persisted_yolo(), None);
        assert_eq!(journal.snapshot().revision, 0);
    }

    #[test]
    fn prepared_rules_publish_before_runtime_and_reject_direct_mutations() {
        smol::block_on(async {
            let manager = default_mgr();
            let journal = Arc::new(SessionJournal::default());
            manager.bind_journal(Arc::clone(&journal), None).unwrap();
            let candidate = PermissionSnapshot {
                rules: vec![deny_rule("rm *")],
                yolo: Some(false),
            };
            let stored = candidate.stored_rules();
            let mut prepared = journal
                .prepare(JournalCommit {
                    changes: vec![crate::journal::JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Rules {
                            rules: stored.clone(),
                            yolo: candidate.yolo,
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(manager.try_add_session_rule(allow_rule("cargo *")).is_err());
            assert!(manager.try_set_yolo(true).is_err());
            assert!(manager.try_toggle_yolo().is_err());
            assert!(
                manager
                    .try_apply_decision(
                        &ToolKey::native(BASH_TOOL),
                        &["cargo test".into()],
                        &PermissionAnswer::AllowSession
                    )
                    .is_err()
            );
            assert!(manager.snapshot().rules.is_empty());
            assert_eq!(manager.persisted_yolo(), None);
            assert_eq!(prepared.snapshot().state.len(), 1);
            prepared.install().unwrap();
            manager.restore_stored(&stored, candidate.yolo).unwrap();
            assert_eq!(manager.snapshot().stored_rules(), stored);
            assert_eq!(manager.persisted_yolo(), candidate.yolo);
            drop(prepared);
            manager.set_yolo(true);
            assert_eq!(journal.snapshot().state.len(), 2);
        });
    }

    #[test]
    fn permission_answer_waits_for_prepared_publication_before_committing_grant() {
        smol::block_on(async {
            let manager = default_mgr();
            let journal = Arc::new(SessionJournal::default());
            manager.bind_journal(Arc::clone(&journal), None).unwrap();
            let (event_tx, events) = flume::unbounded();
            let event_tx = EventSender::new(event_tx);
            let (answers, receiver) = flume::unbounded();
            let receiver = async_lock::Mutex::new(receiver);
            let tool = ToolKey::native(BASH_TOOL);
            let scopes = crate::tools::PermissionScopes::single("cargo test".into());
            let cancel = CancelToken::none();
            let mut pending = Box::pin(manager.enforce(
                &tool,
                &scopes,
                &event_tx,
                Some(&receiver),
                "permission-test",
                &cancel,
                None,
            ));
            assert!(
                futures_lite::future::poll_once(pending.as_mut())
                    .await
                    .is_none()
            );
            assert!(!events.is_empty());
            let mut prepared = journal
                .prepare(JournalCommit {
                    changes: vec![JournalChange {
                        agent: None,
                        generation: 0,
                        change: StateChange::Rules {
                            rules: Vec::new(),
                            yolo: Some(false),
                        },
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            answers
                .send(TaggedAnswer::new("permission-test", PermissionAnswer::AllowSession).encode())
                .unwrap();
            assert!(
                futures_lite::future::poll_once(pending.as_mut())
                    .await
                    .is_none()
            );
            assert!(manager.snapshot().rules.is_empty());
            prepared.install().unwrap();
            manager.restore_stored(&[], Some(false)).unwrap();
            drop(prepared);
            pending.await.unwrap();
            assert_eq!(manager.snapshot().rules.len(), 1);
            assert_eq!(manager.persisted_yolo(), Some(false));
            assert_eq!(journal.snapshot().state.len(), 2);
        });
    }

    #[test_case(0; "cancelled")]
    #[test_case(1; "replaced_incarnation")]
    #[test_case(2; "rebound_manager")]
    fn interrupted_permission_prompt_discards_once_and_session_answers(interruption: usize) {
        smol::block_on(async {
            for answer in [PermissionAnswer::AllowOnce, PermissionAnswer::AllowSession] {
                let manager = default_mgr();
                let journal = Arc::new(SessionJournal::default());
                let agent = AgentId::generate();
                journal.set_incarnation(agent, 1);
                manager
                    .bind_journal(Arc::clone(&journal), Some(journal.incarnation(agent)))
                    .unwrap();
                let (trigger, cancel) = CancelToken::new();
                let (event_tx, events) = flume::unbounded();
                let event_tx = EventSender::new(event_tx);
                let (answers, receiver) = flume::unbounded();
                let receiver = async_lock::Mutex::new(receiver);
                let tool = ToolKey::native(BASH_TOOL);
                let scopes = crate::tools::PermissionScopes {
                    scopes: vec!["cargo test".into()],
                    force_prompt: false,
                };
                let mut pending = Box::pin(manager.enforce(
                    &tool,
                    &scopes,
                    &event_tx,
                    Some(&receiver),
                    "permission-test",
                    &cancel,
                    None,
                ));
                assert!(
                    futures_lite::future::poll_once(pending.as_mut())
                        .await
                        .is_none()
                );
                assert!(!events.is_empty());
                match interruption {
                    0 => trigger.cancel(),
                    1 => journal.set_incarnation(agent, 2),
                    2 => {
                        journal.set_incarnation(agent, 2);
                        manager
                            .bind_journal(Arc::clone(&journal), Some(journal.incarnation(agent)))
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
                answers
                    .send(TaggedAnswer::new("permission-test", answer).encode())
                    .unwrap();
                assert!(pending.await.is_err());
                assert!(manager.snapshot().rules.is_empty());
                assert_eq!(journal.snapshot().revision, 0);
            }
        });
    }

    #[test_case(None; "seed_only")]
    #[test_case(Some(false); "explicit_off")]
    #[test_case(Some(true); "explicit_on")]
    fn permission_rules_restart_from_disk_without_duplicate_records(yolo: Option<bool>) {
        let directory = tempfile::tempdir().unwrap();
        let mut session =
            Session::<Message, TokenUsage, crate::ToolOutput>::new("test/model", TEST_CWD);
        let manager = seeded_mgr(true);
        let journal = Arc::new(SessionJournal::from_session(&session).unwrap());
        manager.restore(PermissionSnapshot {
            rules: Vec::new(),
            yolo,
        });
        manager.bind_journal(Arc::clone(&journal), None).unwrap();
        manager.add_session_rule(allow_rule("cargo *"));
        journal.snapshot().apply_to(&mut session).unwrap();
        session.save_to(directory.path()).unwrap();
        let loaded = Session::<Message, TokenUsage, crate::ToolOutput>::load_from(
            session.id,
            directory.path(),
        )
        .unwrap();
        let folded = loaded.latest_state().unwrap();
        let restored = seeded_mgr(false);
        let folded = folded.scopes.get(&None).unwrap();
        restored.restore(PermissionSnapshot::from_stored(&folded.rules, folded.yolo).unwrap());
        let rebuilt = Arc::new(SessionJournal::from_session(&loaded).unwrap());
        restored.bind_journal(Arc::clone(&rebuilt), None).unwrap();
        assert_eq!(
            restored.snapshot().stored_rules(),
            manager.snapshot().stored_rules()
        );
        assert_eq!(restored.persisted_yolo(), yolo);
        assert_eq!(restored.is_yolo(), yolo.unwrap_or(false));
        restored.add_session_rule(allow_rule("cargo *"));
        assert_eq!(rebuilt.snapshot().revision, 0);
        assert_eq!(rebuilt.snapshot().state, journal.snapshot().state);
    }

    #[test]
    fn add_session_rule_is_idempotent() {
        let mgr = default_mgr();
        let rule = allow_rule("cargo *");
        mgr.add_session_rule(rule.clone());
        mgr.add_session_rule(rule.clone());
        mgr.add_session_rule(rule);
        assert_eq!(mgr.session_rules_snapshot().len(), 1);
    }

    #[test_case(PermissionAnswer::AllowOnce ; "allow_once")]
    #[test_case(PermissionAnswer::Deny ; "deny_once")]
    fn once_decisions_add_no_session_rules(answer: PermissionAnswer) {
        let mgr = default_mgr();
        mgr.apply_decision(&ToolKey::native("bash"), &["cargo test".into()], &answer);
        assert!(mgr.session_rules_snapshot().is_empty());
    }

    #[test_case(PermissionAnswer::AllowAlwaysLocal ; "allow")]
    #[test_case(PermissionAnswer::Deny ; "deny")]
    #[test_case(PermissionAnswer::DenyWithGuidance("no\u{1f}way".into()) ; "guidance_with_separator")]
    fn tagged_answer_roundtrip(answer: PermissionAnswer) {
        let tagged = TaggedAnswer::new("toolu_1", answer);
        assert_eq!(TaggedAnswer::decode(&tagged.encode()), Some(tagged));
    }

    #[test_case("" ; "empty")]
    #[test_case("allow" ; "untagged_answer")]
    #[test_case("toolu_1" ; "id_only")]
    #[test_case("{\"json\": true}" ; "elicitation_result")]
    fn untagged_payloads_never_decode_to_an_answer(raw: &str) {
        assert_eq!(TaggedAnswer::decode(raw), None);
    }

    #[test]
    fn default_deny_blocks_unmatched() {
        let mgr = mgr_with(
            PermissionsConfig {
                default: DefaultEffect::Deny,
                ..Default::default()
            },
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn default_deny_with_allow_rules() {
        let mgr = mgr_with(
            PermissionsConfig {
                default: DefaultEffect::Deny,
                rules: vec![allow_rule("cargo *")],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Allowed
        ));
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "rm -rf /", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn default_allow_allows_unmatched() {
        let mgr = mgr_with(
            PermissionsConfig {
                default: DefaultEffect::Allow,
                ..Default::default()
            },
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Allowed
        ));
    }

    #[test]
    fn default_prompt_is_default_behavior() {
        let mgr = mgr_with(PermissionsConfig::default(), PathBuf::from("/tmp"));
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::NeedsPrompt { .. }
        ));
    }

    #[test]
    fn mcp_server_wildcard_matches_all_server_tools() {
        let mgr = mgr_with(
            make_config(vec![PermissionRule {
                tool: ToolKey::McpServer {
                    server: "deepwiki".into(),
                },
                scope: None,
                effect: Effect::Allow,
            }]),
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(
                &ToolKey::McpTool {
                    server: "deepwiki".into(),
                    tool: "search".into()
                },
                "{}",
                None
            ),
            PermissionCheck::Allowed
        ));
        assert!(matches!(
            mgr.check(
                &ToolKey::McpTool {
                    server: "deepwiki".into(),
                    tool: "web_search".into()
                },
                "{}",
                None
            ),
            PermissionCheck::Allowed
        ));
    }

    #[test]
    fn mcp_server_wildcard_does_not_match_other_server() {
        let mgr = mgr_with(
            make_config(vec![PermissionRule {
                tool: ToolKey::McpServer {
                    server: "deepwiki".into(),
                },
                scope: None,
                effect: Effect::Allow,
            }]),
            PathBuf::from("/tmp"),
        );
        assert!(!matches!(
            mgr.check(
                &ToolKey::McpTool {
                    server: "github".into(),
                    tool: "search".into()
                },
                "{}",
                None
            ),
            PermissionCheck::Allowed
        ));
    }

    #[test]
    fn per_tool_default_overrides_global() {
        let mgr = mgr_with(
            PermissionsConfig {
                default: DefaultEffect::Deny,
                tool_defaults: HashMap::from([(ToolKey::native("bash"), DefaultEffect::Allow)]),
                rules: vec![],
                ..Default::default()
            },
            PathBuf::from("/tmp"),
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("bash"), "cargo test", None),
            PermissionCheck::Allowed
        ));
        assert!(matches!(
            mgr.check(&ToolKey::native("write"), "/etc/passwd", None),
            PermissionCheck::Denied
        ));
    }

    #[test_case("write", true ; "write_tool_allowed")]
    #[test_case("edit", true ; "edit_tool_allowed")]
    #[test_case("bash", false ; "non_write_tool_prompts")]
    fn plan_path_auto_allows_file_write_tools_only(tool: &str, expect_allowed: bool) {
        let plan = "/home/user/.local/state/makima/plans/test.md";
        let plan_path = Path::new(plan);
        let mgr = default_mgr();
        assert_eq!(
            matches!(
                mgr.check(&ToolKey::native(tool), plan, Some(plan_path)),
                PermissionCheck::Allowed
            ),
            expect_allowed,
        );
    }

    #[test_case(MCP_TOOL, MCP_ARGS, DefaultEffect::Prompt, true, true => ALLOWED ; "yolo_plan_mode_allows_mcp")]
    #[test_case(MCP_TOOL, MCP_ARGS, DefaultEffect::Allow, false, true => ALLOWED ; "default_allow_plan_mode_allows_mcp")]
    #[test_case(MCP_TOOL, MCP_ARGS, DefaultEffect::Prompt, false, true => PROMPTS ; "default_prompt_plan_mode_prompts_for_mcp")]
    #[test_case(MCP_TOOL, MCP_ARGS, DefaultEffect::Deny, false, true => DENIED ; "default_deny_plan_mode_denies_mcp")]
    #[test_case(MCP_TOOL, MCP_ARGS, DefaultEffect::Prompt, true, false => ALLOWED ; "yolo_build_mode_allows_mcp")]
    #[test_case(READ_TOOL, READ_SCOPE, DefaultEffect::Prompt, true, true => ALLOWED ; "yolo_plan_mode_allows_native_read")]
    fn plan_mode_uses_normal_permission_defaults(
        tool: &str,
        scope: &str,
        default: DefaultEffect,
        yolo: bool,
        plan_mode: bool,
    ) -> &'static str {
        let mgr = mgr_with(
            PermissionsConfig {
                yolo,
                default,
                ..Default::default()
            },
            PathBuf::from(TEST_CWD),
        );
        let plan_path = Path::new(PLAN_FILE);
        outcome(mgr.check(
            &ToolKey::parse(tool).expect("test tool key parses"),
            scope,
            plan_mode.then_some(plan_path),
        ))
    }

    #[test]
    fn plan_mode_keeps_an_explicit_allow_rule_for_an_mcp_tool() {
        let mgr = mgr_with(
            make_config(vec![PermissionRule {
                tool: ToolKey::parse(MCP_TOOL).expect("test tool key parses"),
                scope: Some("*".into()),
                effect: Effect::Allow,
            }]),
            PathBuf::from("/tmp"),
        );
        assert_eq!(
            outcome(mgr.check(
                &ToolKey::parse(MCP_TOOL).expect("test tool key parses"),
                MCP_ARGS,
                Some(Path::new(PLAN_FILE)),
            )),
            ALLOWED
        );
    }

    #[test_case(ToolKey::Wildcard,  Effect::Allow, true  => ALLOWED ; "wildcard_allow_in_plan_mode")]
    #[test_case(mcp_server_key(),   Effect::Allow, true  => ALLOWED ; "server_allow_in_plan_mode")]
    #[test_case(mcp_server_key(),   Effect::Deny, true  => DENIED ; "server_deny_in_plan_mode")]
    #[test_case(mcp_tool_key(),   Effect::Deny, true  => DENIED ; "tool_deny_in_plan_mode")]
    #[test_case(mcp_tool_key(),     Effect::Allow, true  => ALLOWED ; "exact_tool_allow_decides_in_plan_mode")]
    #[test_case(ToolKey::Wildcard,  Effect::Deny,  true  => DENIED  ; "wildcard_deny_denies_in_plan_mode")]
    #[test_case(ToolKey::Wildcard,  Effect::Allow, false => ALLOWED ; "wildcard_allow_outside_plan_mode")]
    #[test_case(mcp_server_key(),   Effect::Allow, false => ALLOWED ; "server_allow_outside_plan_mode")]
    #[test_case(ToolKey::Wildcard,  Effect::Deny,  false => DENIED  ; "wildcard_deny_outside_plan_mode")]
    fn plan_mode_uses_normal_mcp_permission_rules(
        rule_key: ToolKey,
        effect: Effect,
        plan_mode: bool,
    ) -> &'static str {
        let mgr = mgr_with(
            make_config(vec![PermissionRule {
                tool: rule_key,
                scope: None,
                effect,
            }]),
            PathBuf::from(TEST_CWD),
        );
        outcome(mgr.check(
            &mcp_tool_key(),
            MCP_ARGS,
            plan_mode.then_some(Path::new(PLAN_FILE)),
        ))
    }
    #[test]
    fn plugin_rules_apply_to_manager_and_forks() {
        let store = Arc::new(PluginRuleStore::default());
        let mgr = PermissionManager::new(
            PermissionsConfig::default(),
            PathBuf::from("/tmp"),
            ProjectConfig::for_project(Path::new("/tmp")),
            Arc::clone(&store),
        );
        let fork = mgr.fork();
        store.replace("memory", vec![plugin_edit_rule("/x/**", Effect::Allow)]);
        for m in [&mgr, &fork] {
            assert!(matches!(
                m.check(&ToolKey::native("edit"), "/x/f", None),
                PermissionCheck::Allowed
            ));
        }
    }

    #[test]
    fn config_deny_beats_plugin_allow() {
        let store = Arc::new(PluginRuleStore::default());
        store.replace("memory", vec![plugin_edit_rule("/x/**", Effect::Allow)]);
        let mgr = PermissionManager::new(
            make_config(vec![plugin_edit_rule("/x/**", Effect::Deny)]),
            PathBuf::from("/tmp"),
            ProjectConfig::for_project(Path::new("/tmp")),
            store,
        );
        assert!(matches!(
            mgr.check(&ToolKey::native("edit"), "/x/f", None),
            PermissionCheck::Denied
        ));
    }

    #[test]
    fn plan_path_multi_scope_all_must_match() {
        let plan = "/home/user/.local/state/makima/plans/test.md";
        let plan_path = Path::new(plan);
        let mgr = default_mgr();

        // All scopes match plan → allowed
        assert!(matches!(
            mgr.check_multi(
                &ToolKey::native("write"),
                &[plan, plan],
                false,
                Some(plan_path),
            ),
            PermissionCheck::Allowed
        ));

        // One scope is non-plan → needs prompt
        assert!(matches!(
            mgr.check_multi(
                &ToolKey::native("write"),
                &[plan, "/etc/passwd"],
                false,
                Some(plan_path),
            ),
            PermissionCheck::NeedsPrompt { .. }
        ));
    }
}
