use std::collections::HashMap;
use std::sync::Arc;

use makima_providers::Model;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

pub trait ValidNames: IntoEnumIterator + std::fmt::Display {
    fn valid_names() -> String {
        Self::iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

pub const SYSTEM_PROMPT: &str = include_str!("prompts/system.md");
pub const PLAN_PROMPT: &str = include_str!("prompts/plan.md");
pub const RESEARCH_PROMPT: &str = include_str!("prompts/research.md");
pub const GENERAL_PROMPT: &str = include_str!("prompts/general.md");
pub const COMPACTION_SYSTEM: &str = include_str!("prompts/compaction.md");
pub const COMPACTION_USER: &str = include_str!("prompts/compaction_user.md");

pub const DEFAULT_IDENTITY: &str = r#"You are Makima, an interactive CLI coding agent. Use the tools available to assist the user with software engineering tasks. Complete tasks successfully while minimizing token usage and tool calls to avoid context bloat.

You must NEVER generate or guess URLs unless they are for helping the user with programming."#;

pub const DEFAULT_TONE: &str = r#"- Be concise. Your output is displayed on a CLI rendered in monospace. Use GitHub-flavored markdown.
- Only use emojis if explicitly requested.
- Do not add comments to code unless asked.
- Output text to communicate with the user; all text you output outside of tool use is displayed to the user. Only use tools to complete tasks. NEVER use bash echo or other command-line tools to communicate thoughts, explanations, diagrams, or instructions to the user. Output all communication directly in your response text instead.
- NEVER create files unless absolutely necessary. ALWAYS prefer editing existing files."#;

const NATIVE_EFFICIENT_TOOLS: &[&str] = &["batch", "code_execution", "task_spawn"];
const INSTRUCTIONS_MARKER: &str = "{{instructions}}";

/// Singleton: alphabetically last plugin wins, discarding all prior content
/// and built-in defaults.  Used for slots with opinionated defaults where
/// multiple contributors would conflict (identity, tone).
///
/// Aggregate: all entries are joined.  Used for genuinely additive slots
/// where multiple plugins contributing is the point (tool usage hints,
/// efficient tools, after-instructions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display)]
#[strum(serialize_all = "snake_case")]
pub enum SlotKind {
    Singleton,
    Aggregate,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display, EnumIter, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
pub enum Slot {
    Identity,
    Tone,
    ToolUsage,
    EfficientTools,
    Conventions,
    AfterInstructions,
}

impl Slot {
    fn marker(self) -> &'static str {
        match self {
            Slot::Identity => "{{identity}}",
            Slot::Tone => "{{tone}}",
            Slot::ToolUsage => "{{tool_usage}}",
            Slot::EfficientTools => "{{efficient_tools}}",
            Slot::Conventions => "{{conventions}}",
            Slot::AfterInstructions => "{{after_instructions}}",
        }
    }

    pub fn kind(self) -> SlotKind {
        match self {
            Slot::Identity | Slot::Tone => SlotKind::Singleton,
            Slot::ToolUsage
            | Slot::EfficientTools
            | Slot::Conventions
            | Slot::AfterInstructions => SlotKind::Aggregate,
        }
    }

    /// Built-in default content for singleton slots.  When no plugin
    /// registers content for a singleton slot, the default is used.
    /// Aggregate slots have no default (the template carries the static
    /// text around the marker).
    pub fn default_content(self) -> Option<&'static str> {
        match self {
            Slot::Identity => Some(DEFAULT_IDENTITY),
            Slot::Tone => Some(DEFAULT_TONE),
            _ => None,
        }
    }

    pub fn names_for_kind(kind: SlotKind) -> String {
        Self::iter()
            .filter(|s| s.kind() == kind)
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, EnumString, Display, EnumIter, Serialize, Deserialize,
)]
#[strum(serialize_all = "snake_case")]
pub enum PromptId {
    System,
    Research,
    General,
}

impl PromptId {
    pub const ALL: &[PromptId] = &[PromptId::System, PromptId::Research, PromptId::General];
}

impl ValidNames for Slot {}
impl ValidNames for PromptId {}

#[derive(Clone)]
pub struct SlotEntry {
    pub plugin: Arc<str>,
    pub registration: usize,
    pub content: String,
}

#[derive(Clone)]
pub struct PromptSource {
    pub prompt: PromptId,
    pub slot: Slot,
    pub plugin: Arc<str>,
    pub registration: usize,
    pub content: Option<String>,
}

#[derive(Clone, Default)]
pub struct ResolvedSlots {
    entries: HashMap<(PromptId, Slot), Vec<SlotEntry>>,
    observations: HashMap<(PromptId, Slot), Vec<PromptSource>>,
}

impl ResolvedSlots {
    pub fn get(&self, prompt: PromptId, slot: Slot) -> &[SlotEntry] {
        self.entries
            .get(&(prompt, slot))
            .map(|v| v.as_slice())
            .unwrap_or_default()
    }

    pub fn insert(&mut self, prompt: PromptId, slot: Slot, entry: SlotEntry) {
        self.observe(PromptSource {
            prompt,
            slot,
            plugin: entry.plugin,
            registration: entry.registration,
            content: Some(entry.content),
        });
    }

    pub fn observe(&mut self, source: PromptSource) {
        let address = (source.prompt, source.slot);
        if let Some(content) = &source.content {
            self.entries.entry(address).or_default().push(SlotEntry {
                plugin: source.plugin.clone(),
                registration: source.registration,
                content: content.clone(),
            });
        }
        self.observations.entry(address).or_default().push(source);
    }

    pub fn sources(&self) -> Vec<PromptSource> {
        let mut sources = Vec::new();
        for &prompt in PromptId::ALL {
            for slot in Slot::iter() {
                if let Some(observations) = self.observations.get(&(prompt, slot)) {
                    sources.extend(observations.iter().cloned());
                }
            }
        }
        sources
    }
}

impl PromptId {
    fn template(self) -> &'static str {
        match self {
            PromptId::System => SYSTEM_PROMPT,
            PromptId::Research => RESEARCH_PROMPT,
            PromptId::General => GENERAL_PROMPT,
        }
    }

    /// A slot exists for this prompt iff its marker is present in the template.
    /// Markers that are absent get no content (and we warn at collection time
    /// when a plugin targets them explicitly).
    pub fn has_slot(self, slot: Slot) -> bool {
        self.template().contains(slot.marker())
    }
}

fn render_slot(slots: &ResolvedSlots, prompt: PromptId, slot: Slot) -> String {
    if slot == Slot::EfficientTools {
        return render_efficient_tools(slots, prompt);
    }
    let entries = slots.get(prompt, slot);
    match slot.kind() {
        SlotKind::Singleton => {
            if let Some(last) = entries.last() {
                last.content.clone()
            } else if let Some(default) = slot.default_content() {
                default.to_string()
            } else {
                String::new()
            }
        }
        // Aggregate slots have no built-in defaults; content comes entirely from plugins.
        SlotKind::Aggregate => {
            let mut parts = Vec::new();
            for entry in entries {
                parts.push(entry.content.as_str());
            }
            parts.join("\n")
        }
    }
}

fn render_efficient_tools(slots: &ResolvedSlots, prompt: PromptId) -> String {
    let extras = slots.get(prompt, Slot::EfficientTools);
    let names = NATIVE_EFFICIENT_TOOLS
        .iter()
        .copied()
        .chain(extras.iter().map(|e| e.content.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("Most efficient tools: {names}.")
}

/// Fill each `{{slot}}` marker in the template with its rendered content and
/// drop the project instructions (AGENTS.md and friends) into `{{instructions}}`.
pub fn assemble(id: PromptId, slots: &ResolvedSlots, instructions: &str) -> String {
    let mut out = id.template().to_string();
    for slot in Slot::iter() {
        out = fill_marker(&out, slot.marker(), &render_slot(slots, id, slot));
    }
    out.replace(INSTRUCTIONS_MARKER, instructions)
}

pub(crate) fn append_model_identity(system: &mut String, model: &Model) {
    if !system.is_empty() {
        system.push_str("\n\n");
    }
    system.push_str(&format!(
        "# Model identity\n- Model: {}\n- Provider: {}",
        model.id, model.provider
    ));
}

/// Replace a slot marker with its content. When the content is empty, also drop
/// the marker's own line (the trailing newline) so empty slots leave no blank
/// gap, without touching any other whitespace in the prompt.
fn fill_marker(template: &str, marker: &str, content: &str) -> String {
    if content.is_empty() {
        return template
            .replace(&format!("{marker}\n"), "")
            .replace(marker, "");
    }
    template.replace(marker, content)
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    const NATIVE_EFFICIENT_LINE: &str = "Most efficient tools: batch, code_execution, task_spawn";

    fn slots(prompt: PromptId, entries: &[(Slot, &str)]) -> ResolvedSlots {
        let mut slots = ResolvedSlots::default();
        for &(slot, content) in entries {
            slots.insert(
                prompt,
                slot,
                SlotEntry {
                    plugin: Arc::from("p"),
                    registration: 0,
                    content: content.into(),
                },
            );
        }
        slots
    }

    fn at(out: &str, needle: &str) -> usize {
        out.find(needle)
            .unwrap_or_else(|| panic!("missing: {needle}"))
    }

    #[test]
    fn empty_slots_emit_template_and_native_efficient_line() {
        let out = assemble(PromptId::System, &ResolvedSlots::default(), "");
        assert!(out.starts_with("You are Makima"));
        assert!(
            !out.contains("{{"),
            "unfilled marker left in output:\n{out}"
        );
        assert!(out.contains(&format!("{NATIVE_EFFICIENT_LINE}.")));
    }

    /// One test to pin the whole System layout: every slot shows up, in order,
    /// around the instructions. Covers presence and ordering for all of them.
    #[test]
    fn system_sections_land_in_layout_order() {
        let s = slots(
            PromptId::System,
            &[
                (Slot::ToolUsage, "TOOL_USAGE"),
                (Slot::EfficientTools, "EXTRA_TOOL"),
                (Slot::Conventions, "CONVENTIONS"),
                (Slot::AfterInstructions, "AFTER"),
            ],
        );
        let out = assemble(PromptId::System, &s, "INSTR");
        let positions = ["TOOL_USAGE", "EXTRA_TOOL", "CONVENTIONS", "INSTR", "AFTER"]
            .map(|needle| at(&out, needle));
        assert!(
            positions.is_sorted(),
            "sections out of layout order ({positions:?}):\n{out}"
        );
    }

    /// Regression: a `tool_usage` hint must land inside the `# Tool usage`
    /// section, not be appended after the rest of the prompt.
    #[test]
    fn tool_usage_hint_lands_inside_tool_usage_section() {
        const HINT: &str = "- HINT_LINE";
        let s = slots(PromptId::System, &[(Slot::ToolUsage, HINT)]);
        let out = assemble(PromptId::System, &s, "");
        let hint = at(&out, HINT);
        assert!(
            at(&out, "# Tool usage") < hint,
            "hint before its section:\n{out}"
        );
        assert!(
            hint < at(&out, "# Conventions"),
            "hint leaked past section:\n{out}"
        );
    }

    #[test]
    fn efficient_tools_extras_join_native_list() {
        let s = slots(
            PromptId::System,
            &[
                (Slot::EfficientTools, "index"),
                (Slot::EfficientTools, "foo"),
            ],
        );
        let out = assemble(PromptId::System, &s, "");
        assert!(out.contains(&format!("{NATIVE_EFFICIENT_LINE}, index, foo.")));
    }

    #[test]
    fn sources_preserve_registration_and_bucket_order_for_reconstruction() {
        let entries = [
            (PromptId::General, Slot::ToolUsage, "z", 4, "general"),
            (PromptId::System, Slot::ToolUsage, "z", 3, "first hint"),
            (PromptId::System, Slot::Identity, "z", 1, "first identity"),
            (PromptId::Research, Slot::ToolUsage, "a", 2, "research"),
            (PromptId::System, Slot::Identity, "a", 0, "second identity"),
            (PromptId::System, Slot::ToolUsage, "a", 5, "second hint"),
            (PromptId::System, Slot::Identity, "z", 7, "last identity"),
            (PromptId::System, Slot::ToolUsage, "z", 8, "last hint"),
            (PromptId::System, Slot::Tone, "a", 6, "tone"),
        ];
        let mut slots = ResolvedSlots::default();
        for (prompt, slot, plugin, registration, content) in entries {
            slots.insert(
                prompt,
                slot,
                SlotEntry {
                    plugin: Arc::from(plugin),
                    registration,
                    content: content.into(),
                },
            );
        }
        let sources = slots.sources();
        let projected: Vec<_> = sources
            .iter()
            .map(|source| {
                (
                    source.prompt,
                    source.slot,
                    source.plugin.as_ref(),
                    source.registration,
                    source.content.as_deref().unwrap(),
                )
            })
            .collect();
        assert_eq!(projected, [2, 4, 6, 8, 1, 5, 7, 3, 0].map(|i| entries[i]));

        let mut restored = ResolvedSlots::default();
        for source in sources {
            restored.observe(source);
        }
        for &prompt in PromptId::ALL {
            assert_eq!(
                assemble(prompt, &restored, ""),
                assemble(prompt, &slots, "")
            );
        }
        assert_eq!(
            render_slot(&restored, PromptId::System, Slot::Identity),
            entries[6].4
        );
        assert_eq!(
            render_slot(&restored, PromptId::System, Slot::ToolUsage),
            [entries[1].4, entries[5].4, entries[7].4].join("\n")
        );
    }

    #[test]
    fn observations_retain_omissions_without_rendering_or_duplicate_successes() {
        let mut slots = ResolvedSlots::default();
        let identity = "recorded identity";
        slots.observe(PromptSource {
            prompt: PromptId::General,
            slot: Slot::ToolUsage,
            plugin: Arc::from("missing"),
            registration: 0,
            content: None,
        });
        for (registration, content) in [(1, None), (2, Some(identity)), (3, None)] {
            slots.observe(PromptSource {
                prompt: PromptId::System,
                slot: Slot::Identity,
                plugin: Arc::from("plugin"),
                registration,
                content: content.map(String::from),
            });
        }
        let sources = slots.sources();
        assert_eq!(
            sources
                .iter()
                .map(|source| source.registration)
                .collect::<Vec<_>>(),
            [1, 2, 3, 0]
        );
        assert_eq!(
            sources
                .iter()
                .map(|source| source.content.as_deref())
                .collect::<Vec<_>>(),
            [None, Some(identity), None, None]
        );
        assert_eq!(slots.get(PromptId::System, Slot::Identity).len(), 1);
        assert!(slots.get(PromptId::General, Slot::ToolUsage).is_empty());
        assert_eq!(
            render_slot(&slots, PromptId::System, Slot::Identity),
            identity
        );
        let mut restored = ResolvedSlots::default();
        for source in sources {
            restored.observe(source);
        }
        assert_eq!(restored.sources().len(), 4);
        for &prompt in PromptId::ALL {
            assert_eq!(
                assemble(prompt, &restored, ""),
                assemble(prompt, &slots, "")
            );
        }
    }

    #[test]
    fn source_identifiers_round_trip_through_serde() {
        for &prompt in PromptId::ALL {
            let encoded = serde_json::to_string(&prompt).unwrap();
            assert_eq!(serde_json::from_str::<PromptId>(&encoded).unwrap(), prompt);
        }
        for slot in Slot::iter() {
            let encoded = serde_json::to_string(&slot).unwrap();
            assert_eq!(serde_json::from_str::<Slot>(&encoded).unwrap(), slot);
        }
    }

    #[test]
    fn same_slot_preserves_insertion_order() {
        let s = slots(
            PromptId::System,
            &[(Slot::ToolUsage, "FIRST"), (Slot::ToolUsage, "SECOND")],
        );
        let out = assemble(PromptId::System, &s, "");
        assert!(at(&out, "FIRST") < at(&out, "SECOND"));
    }

    /// Only System carries AfterInstructions, so the same content shows up there
    /// but never leaks into the subagent prompts.
    #[test]
    fn after_instructions_only_reaches_system() {
        let mut s = ResolvedSlots::default();
        for &pid in PromptId::ALL {
            s.insert(
                pid,
                Slot::AfterInstructions,
                SlotEntry {
                    plugin: Arc::from("p"),
                    registration: 0,
                    content: "AFTER".into(),
                },
            );
        }
        assert!(assemble(PromptId::System, &s, "").contains("AFTER"));
        assert!(!assemble(PromptId::Research, &s, "").contains("AFTER"));
        assert!(!assemble(PromptId::General, &s, "").contains("AFTER"));
    }

    #[test]
    fn research_drops_conventions_but_keeps_efficient_extras() {
        let s = slots(
            PromptId::Research,
            &[
                (Slot::Conventions, "DROPPED"),
                (Slot::EfficientTools, "EXTRA"),
            ],
        );
        let out = assemble(PromptId::Research, &s, "");
        assert!(!out.contains("DROPPED"));
        assert!(out.contains(&format!("{NATIVE_EFFICIENT_LINE}, EXTRA.")));
    }

    #[test_case(PromptId::System, Slot::ToolUsage, true ; "system_tool_usage")]
    #[test_case(PromptId::System, Slot::EfficientTools, true ; "system_efficient")]
    #[test_case(PromptId::System, Slot::Conventions, true ; "system_conventions")]
    #[test_case(PromptId::System, Slot::AfterInstructions, true ; "system_after")]
    #[test_case(PromptId::System, Slot::Identity, true ; "system_identity")]
    #[test_case(PromptId::System, Slot::Tone, true ; "system_tone")]
    #[test_case(PromptId::Research, Slot::Conventions, false ; "research_no_conventions")]
    #[test_case(PromptId::Research, Slot::AfterInstructions, false ; "research_no_after")]
    #[test_case(PromptId::Research, Slot::Identity, false ; "research_no_identity")]
    #[test_case(PromptId::Research, Slot::Tone, false ; "research_no_tone")]
    #[test_case(PromptId::General, Slot::AfterInstructions, false ; "general_no_after")]
    #[test_case(PromptId::General, Slot::Identity, false ; "general_no_identity")]
    #[test_case(PromptId::General, Slot::Tone, false ; "general_no_tone")]
    fn has_slot(prompt: PromptId, slot: Slot, expected: bool) {
        assert_eq!(prompt.has_slot(slot), expected);
    }

    #[test_case("after_instructions", Some(Slot::AfterInstructions) ; "valid_slot")]
    #[test_case("tool_usagee", None ; "typo_slot")]
    #[test_case("identity", Some(Slot::Identity) ; "identity_slot")]
    #[test_case("tone", Some(Slot::Tone) ; "tone_slot")]
    fn slot_parse_is_plugin_contract(input: &str, expected: Option<Slot>) {
        assert_eq!(input.parse::<Slot>().ok(), expected);
    }

    #[test_case("system", Some(PromptId::System) ; "valid_prompt")]
    #[test_case("systm", None ; "typo_prompt")]
    fn prompt_parse_is_plugin_contract(input: &str, expected: Option<PromptId>) {
        assert_eq!(input.parse::<PromptId>().ok(), expected);
    }

    #[test_case(Slot::Identity, SlotKind::Singleton ; "identity_singleton")]
    #[test_case(Slot::Tone, SlotKind::Singleton ; "tone_singleton")]
    #[test_case(Slot::Conventions, SlotKind::Aggregate ; "conventions_aggregate")]
    #[test_case(Slot::ToolUsage, SlotKind::Aggregate ; "tool_usage_aggregate")]
    #[test_case(Slot::EfficientTools, SlotKind::Aggregate ; "efficient_aggregate")]
    #[test_case(Slot::AfterInstructions, SlotKind::Aggregate ; "after_aggregate")]
    fn slot_kind_matches_expectations(slot: Slot, expected: SlotKind) {
        assert_eq!(slot.kind(), expected);
    }

    #[test]
    fn singleton_default_used_when_empty() {
        let out = assemble(PromptId::System, &ResolvedSlots::default(), "");
        assert!(out.starts_with("You are Makima"));
    }

    #[test]
    fn singleton_entry_replaces_default() {
        let mut s = ResolvedSlots::default();
        s.insert(
            PromptId::System,
            Slot::Identity,
            SlotEntry {
                plugin: Arc::from("user"),
                registration: 0,
                content: "Custom identity".into(),
            },
        );
        let out = assemble(PromptId::System, &s, "");
        assert!(out.contains("Custom identity"));
        assert!(!out.contains("You are Makima"));
    }

    #[test]
    fn singleton_last_entry_wins() {
        let mut s = ResolvedSlots::default();
        s.insert(
            PromptId::System,
            Slot::Identity,
            SlotEntry {
                plugin: Arc::from("first"),
                registration: 0,
                content: "FIRST".into(),
            },
        );
        s.insert(
            PromptId::System,
            Slot::Identity,
            SlotEntry {
                plugin: Arc::from("second"),
                registration: 0,
                content: "SECOND".into(),
            },
        );
        let out = assemble(PromptId::System, &s, "");
        assert!(out.contains("SECOND"));
        assert!(!out.contains("FIRST"));
        assert!(!out.contains("You are Makima"));
    }

    #[test]
    fn identity_only_in_system_not_subagents() {
        assert!(PromptId::System.has_slot(Slot::Identity));
        assert!(!PromptId::Research.has_slot(Slot::Identity));
        assert!(!PromptId::General.has_slot(Slot::Identity));
    }

    #[test]
    fn tone_only_in_system_not_subagents() {
        assert!(PromptId::System.has_slot(Slot::Tone));
        assert!(!PromptId::Research.has_slot(Slot::Tone));
        assert!(!PromptId::General.has_slot(Slot::Tone));
    }

    #[test]
    fn conventions_entry_appends_to_template_defaults() {
        let mut s = ResolvedSlots::default();
        s.insert(
            PromptId::System,
            Slot::Conventions,
            SlotEntry {
                plugin: Arc::from("plugin"),
                registration: 0,
                content: "- Extra rule".into(),
            },
        );
        let out = assemble(PromptId::System, &s, "");
        assert!(out.contains("Never assume a library is available"));
        assert!(out.contains("- Extra rule"));
    }
}
