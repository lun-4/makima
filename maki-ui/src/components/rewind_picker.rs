use crate::components::Overlay;
use crate::components::list_picker::{ListPicker, PickerAction, PickerItem};
use crate::repaint::Cadence;

use crossterm::event::KeyEvent;
use maki_agent::AgentRef;
use maki_agent::actor::HistoryBoundary;
use ratatui::Frame;
use ratatui::layout::{Position, Rect};

const TITLE: &str = " Rewind ";
const PREVIEW_MAX_LEN: usize = 80;
pub(crate) const NO_TURNS_MSG: &str = "No user turns to rewind to";

pub enum RewindPickerAction {
    Consumed,
    Select(RewindEntry),
    Close,
}

pub struct RewindEntry {
    pub turn_index: usize,
    pub prompt_preview: String,
}

impl RewindEntry {
    fn new(turn_index: usize, turn_number: usize, text: &str) -> Self {
        let first_line = text.lines().next().unwrap_or("");
        let preview = &first_line[..first_line.floor_char_boundary(PREVIEW_MAX_LEN)];
        let suffix = if preview.len() < first_line.len() {
            "..."
        } else {
            ""
        };
        Self {
            turn_index,
            prompt_preview: format!("{turn_number}: {preview}{suffix}"),
        }
    }
}

impl PickerItem for RewindEntry {
    fn label(&self) -> &str {
        &self.prompt_preview
    }
}

pub struct RewindPicker {
    picker: ListPicker<RewindEntry>,
    target: Option<AgentRef>,
    boundaries: Vec<HistoryBoundary>,
}

impl RewindPicker {
    pub fn new() -> Self {
        Self {
            picker: ListPicker::new(),
            target: None,
            boundaries: Vec::new(),
        }
    }

    pub fn open_actor(&mut self, target: AgentRef) -> Result<(), String> {
        let actor = target.actor().map_err(|error| error.to_string())?;
        let snapshot = actor
            .history_boundary_messages()
            .map_err(|error| error.to_string())?;
        let mut entries = Vec::with_capacity(snapshot.len());
        let mut boundaries = Vec::with_capacity(snapshot.len());
        for (boundary, message) in snapshot {
            let Some(text) = message.user_text() else {
                continue;
            };
            entries.push(RewindEntry::new(boundary.offset, entries.len() + 1, text));
            boundaries.push(boundary);
        }
        if entries.is_empty() {
            return Err(NO_TURNS_MSG.into());
        }
        entries.reverse();
        self.target = Some(target);
        self.boundaries = boundaries;
        self.picker.open(entries, TITLE);
        Ok(())
    }

    pub fn target_boundary(&self, offset: usize) -> Option<(AgentRef, HistoryBoundary)> {
        Some((
            self.target.clone()?,
            self.boundaries
                .iter()
                .find(|boundary| boundary.offset == offset)?
                .clone(),
        ))
    }

    pub fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub fn close(&mut self) {
        self.picker.close();
    }

    pub fn contains(&self, pos: Position) -> bool {
        self.picker.contains(pos)
    }

    pub fn scroll(&mut self, delta: i32) {
        self.picker.scroll(delta);
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        self.picker.handle_paste(text)
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> RewindPickerAction {
        match self.picker.handle_key(key) {
            PickerAction::Consumed => RewindPickerAction::Consumed,
            PickerAction::Select(entry) => RewindPickerAction::Select(entry),
            PickerAction::Close => RewindPickerAction::Close,
            PickerAction::Toggle(..) | PickerAction::Delete(..) => RewindPickerAction::Consumed,
        }
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        self.picker.view(frame, area)
    }
}

impl Overlay for RewindPicker {
    fn is_open(&self) -> bool {
        self.is_open()
    }

    fn close(&mut self) {
        self.close()
    }

    fn cadence(&self) -> Cadence {
        self.picker.cadence()
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, pending};
    use std::pin::Pin;
    use std::slice;
    use std::time::Duration;

    use maki_agent::{
        ActorBackend, AgentInput, AgentManagerHandle, AgentRef, BackendResult, ControlWork,
        History, TurnContext, WorkKind,
    };
    use maki_providers::{ContentBlock, Message, Role};
    use test_case::test_case;

    use super::{NO_TURNS_MSG, PREVIEW_MAX_LEN, RewindPicker};
    use crate::components::list_picker::PickerItem;

    const RESPONSE: &str = "response";
    const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

    struct IdleBackend;

    impl ActorBackend for IdleBackend {
        fn run_turn<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: AgentInput,
            _: WorkKind,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(pending())
        }

        fn run_control<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: &'a ControlWork,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(pending())
        }

        fn run_compact<'a>(
            &'a mut self,
            _: &'a mut History,
            _: TurnContext,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
            Box::pin(pending())
        }
    }

    fn with_actor(messages: &[Message], test: impl FnOnce(AgentRef)) {
        let manager = AgentManagerHandle::new(Default::default()).unwrap();
        let target = manager
            .create_root(messages.to_vec(), None, Box::new(IdleBackend))
            .unwrap();
        test(target);
        assert!(
            smol::block_on(manager.shutdown(SHUTDOWN_TIMEOUT))
                .timed_out
                .is_empty()
        );
    }

    fn user_msg(text: &str) -> Message {
        Message::user(text.into())
    }

    fn assistant_msg() -> Message {
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::Text {
                text: RESPONSE.into(),
            }],
            ..Default::default()
        }
    }

    #[test_case(&[]; "empty_messages")]
    #[test_case(&[assistant_msg()]; "no_user_turns")]
    #[test_case(&[Message::synthetic("continue".into())]; "only_synthetic")]
    fn open_without_user_turns_returns_error(messages: &[Message]) {
        with_actor(messages, |target| {
            let mut picker = RewindPicker::new();
            assert_eq!(picker.open_actor(target), Err(NO_TURNS_MSG.into()));
        });
    }

    #[test]
    fn entries_are_in_reverse_order() {
        let messages = vec![
            user_msg("first"),
            assistant_msg(),
            user_msg("second"),
            assistant_msg(),
            user_msg("third"),
        ];
        with_actor(&messages, |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target.clone()).unwrap();
            let item = picker.picker.selected_item().unwrap();
            assert_eq!(item.label(), "3: third");
            assert_eq!(item.turn_index, 4);
            let (selected, boundary) = picker.target_boundary(item.turn_index).unwrap();
            assert_eq!(selected.id(), target.id());
            assert_eq!(boundary.offset, item.turn_index);
            assert_eq!(boundary.agent_id, target.id());
            assert!(picker.target_boundary(1).is_none());
        });
    }

    #[test_case("a".repeat(120), format!("1: {}...", "a".repeat(PREVIEW_MAX_LEN)); "long_prompt")]
    #[test_case("é".repeat(41), format!("1: {}...", "é".repeat(PREVIEW_MAX_LEN / 2)); "unicode_prompt")]
    #[test_case("a".repeat(PREVIEW_MAX_LEN), format!("1: {}", "a".repeat(PREVIEW_MAX_LEN)); "exact_limit")]
    #[test_case("first line\nsecond line".into(), "1: first line".into(); "multiline_prompt")]
    fn preview_uses_first_line_and_truncates_at_character_boundary(text: String, expected: String) {
        let message = user_msg(&text);
        with_actor(slice::from_ref(&message), |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            let item = picker.picker.selected_item().unwrap();
            assert_eq!(item.label(), expected);
            let (selected, boundary) = picker.target_boundary(item.turn_index).unwrap();
            assert_eq!(
                serde_json::to_value(
                    selected
                        .actor()
                        .unwrap()
                        .history_boundary_message(&boundary)
                        .unwrap()
                )
                .unwrap(),
                serde_json::to_value(&message).unwrap(),
            );
        });
    }

    #[test]
    fn display_text_overrides_content() {
        let message = Message::user_display("ai sees this".into(), "user typed this".into());
        with_actor(&[message], |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            assert_eq!(
                picker.picker.selected_item().unwrap().label(),
                "1: user typed this"
            );
        });
    }

    #[test]
    fn synthetic_messages_and_observations_are_excluded() {
        let messages = vec![
            Message::observation("build failed".into()),
            user_msg("real prompt"),
            assistant_msg(),
            Message::synthetic("[Cancelled by user]".into()),
        ];
        with_actor(&messages, |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            let item = picker.picker.selected_item().unwrap();
            assert_eq!(item.label(), "1: real prompt");
            assert_eq!(item.turn_index, 1);
        });
    }

    #[test]
    fn turn_numbers_skip_synthetic() {
        let messages = vec![
            user_msg("first"),
            assistant_msg(),
            Message::synthetic("continue".into()),
            assistant_msg(),
            user_msg("second"),
        ];
        with_actor(&messages, |target| {
            let mut picker = RewindPicker::new();
            picker.open_actor(target).unwrap();
            assert_eq!(picker.picker.selected_item().unwrap().label(), "2: second");
        });
    }
}
