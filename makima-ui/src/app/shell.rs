use std::collections::{HashMap, HashSet};

use makima_agent::{AgentId, CancelTrigger, ToolDoneEvent, ToolInput, ToolOutput, ToolStartEvent};

use super::App;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShellPrefix {
    pub prefix_len: usize,
    pub command: String,
    pub visible: bool,
}

pub(crate) fn parse_shell_prefix(text: &str) -> Option<ShellPrefix> {
    let (sigil_len, visible) = if text.starts_with("!!") {
        (2, false)
    } else if text.starts_with('!') {
        (1, true)
    } else {
        return None;
    };
    let rest = &text[sigil_len..];
    let prefix_len = if rest.starts_with(' ') {
        sigil_len + 1
    } else {
        sigil_len
    };
    let command = rest.trim();
    if command.is_empty() {
        return None;
    }
    Some(ShellPrefix {
        prefix_len,
        command: command.to_owned(),
        visible,
    })
}

pub(crate) enum ShellEvent {
    Start {
        id: String,
        command: String,
    },
    Output {
        id: String,
        content: String,
    },
    Done {
        id: String,
        output: String,
        is_error: bool,
    },
}

#[derive(Default)]
pub(crate) struct ShellState {
    cancel_triggers: HashMap<String, CancelTrigger>,
    targets: HashMap<String, AgentId>,
    id_counter: u64,
    active_ids: HashSet<String>,
}

impl ShellState {
    pub(crate) fn reserve_for(&mut self, target: AgentId) -> String {
        self.id_counter += 1;
        let id = format!("shell-{}", self.id_counter);
        self.active_ids.insert(id.clone());
        self.targets.insert(id.clone(), target);
        id
    }

    pub fn active_ids(&self) -> &HashSet<String> {
        &self.active_ids
    }

    pub fn release_id(&mut self, id: &str) {
        self.active_ids.remove(id);
        self.targets.remove(id);
        self.cancel_triggers.remove(id);
    }

    pub fn add_trigger_for(&mut self, id: String, trigger: CancelTrigger) {
        self.cancel_triggers.insert(id, trigger);
    }

    pub fn cancel_for(&mut self, target: AgentId) {
        let ids: Vec<_> = self
            .targets
            .iter()
            .filter(|(_, owner)| **owner == target)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            if let Some(trigger) = self.cancel_triggers.remove(&id) {
                trigger.cancel();
            }
        }
    }
}

impl App {
    pub(crate) fn handle_shell_event(&mut self, event: ShellEvent) {
        let id = match &event {
            ShellEvent::Start { id, .. }
            | ShellEvent::Output { id, .. }
            | ShellEvent::Done { id, .. } => id,
        };
        let Some(target) = self.shell.targets.get(id).copied() else {
            return;
        };
        if matches!(event, ShellEvent::Done { .. }) {
            self.shell.release_id(id);
        }
        let Some(chat_idx) = self
            .chats
            .iter()
            .position(|chat| chat.agent_id == Some(target))
        else {
            return;
        };
        match event {
            ShellEvent::Start { id, command } => {
                self.chats[chat_idx].shell_tool_start(ToolStartEvent {
                    id,
                    tool: "bash".into(),
                    summary: command.clone(),
                    annotation: None,
                    input: Some(ToolInput::Code {
                        language: "bash".into(),
                        code: command,
                    }),
                    raw_input: None,
                    output: None,
                    render_header: None,
                });
            }
            ShellEvent::Output { id, content } => {
                self.chats[chat_idx].shell_tool_output(&id, &content);
            }
            ShellEvent::Done {
                id,
                output,
                is_error,
            } => {
                self.chats[chat_idx].shell_tool_done(ToolDoneEvent {
                    id,
                    tool: "bash".into(),
                    output: ToolOutput::Plain(output.into()),
                    is_error,
                    annotation: None,
                    written_path: None,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ShellPrefix, parse_shell_prefix};
    use test_case::test_case;

    #[test_case("! ls",                     Some(ShellPrefix { prefix_len: 2, command: "ls".into(), visible: true })           ; "simple_visible")]
    #[test_case("!! ls",                    Some(ShellPrefix { prefix_len: 3, command: "ls".into(), visible: false })          ; "simple_anonymous")]
    #[test_case("! cargo test --release",   Some(ShellPrefix { prefix_len: 2, command: "cargo test --release".into(), visible: true })  ; "multi_word_command")]
    #[test_case("!! cargo build",           Some(ShellPrefix { prefix_len: 3, command: "cargo build".into(), visible: false }) ; "multi_word_anonymous")]
    #[test_case("! ",                       None                        ; "bang_space_only")]
    #[test_case("!",                        None                        ; "bang_alone")]
    #[test_case("!!",                       None                        ; "double_bang_alone")]
    #[test_case("!! ",                      None                        ; "double_bang_space_only")]
    #[test_case("hello ! world",            None                        ; "bang_mid_string")]
    #[test_case(" ! ls",                    None                        ; "leading_space")]
    #[test_case("!echo hi",                 Some(ShellPrefix { prefix_len: 1, command: "echo hi".into(), visible: true })      ; "no_space_after_bang")]
    #[test_case("!!echo hi",                Some(ShellPrefix { prefix_len: 2, command: "echo hi".into(), visible: false })     ; "no_space_after_double_bang")]
    #[test_case("!  ls",                    Some(ShellPrefix { prefix_len: 2, command: "ls".into(), visible: true })           ; "extra_spaces_trimmed")]
    fn parse_shell_prefix_cases(input: &str, expected: Option<ShellPrefix>) {
        assert_eq!(parse_shell_prefix(input), expected);
    }
}
