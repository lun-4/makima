//! The host-side list-picker dialog a plugin opens via
//! `makima.ui.open_list_picker`. It wraps [`ListPicker`] and talks back to the
//! Lua thread by dialog `id` (the callbacks live in Lua app-data and cannot
//! cross the `UiAction` channel), replying with a single [`PickerResult`] on
//! the caller's channel. Filter edits keep the current row (native
//! [`ListPicker`] parity); the retired Lua dialog jumped the cursor to the
//! best match on every filter change.

use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use flume::Sender;
use makima_lua::{EventHandle, PickerConfig, PickerEvent, PickerItemSpec, PickerResult};
use ratatui::Frame;
use ratatui::layout::{Position, Rect};

use crate::click::{HitTarget, PickerId};
use crate::components::Overlay;
use crate::components::keybindings::key_display;
use crate::components::list_picker::{ClockSample, ListPicker, PickerAction, PickerItem};
use crate::components::lua_float::hint_footer;
use crate::repaint::{Cadence, Dirty};

/// The fixed delete key of the plugin dialog (old-Lua parity; not a config
/// option). Equals the native `SCROLL_HALF_DOWN` bind, which the picker's
/// delete check deliberately precedes.
const DELETE_KEY: KeyCode = KeyCode::Char('d');

/// One rendered entry of the dialog, decoded from a [`PickerItemSpec`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PickerEntry {
    label: String,
    detail: Option<String>,
    section: Option<String>,
    section_detail: Option<String>,
    /// Unix-epoch instant supplied by Lua; formatted at draw time.
    age: Option<Duration>,
}

impl PickerItem for PickerEntry {
    fn label(&self) -> &str {
        &self.label
    }
    fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }
    fn section(&self) -> Option<&str> {
        self.section.as_deref()
    }
    fn section_detail(&self) -> Option<&str> {
        self.section_detail.as_deref()
    }
    fn age(&self, now: &ClockSample) -> Option<Duration> {
        self.age.map(|at| now.unix.saturating_sub(at))
    }
}

/// What a key press of the plugin dialog resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LuaPickerAction {
    /// Consumed (navigation, search edit, first delete-press arm).
    Consumed,
    /// A delete key needs a second press; carries the flash message.
    Confirming(String),
    /// The chosen item, 0-based original index.
    Choice(usize),
    /// The deleted item, 0-based original index.
    Delete(usize),
    /// Dismissed without a choice.
    Close,
}

pub struct LuaPicker {
    picker: ListPicker<PickerEntry>,
    event_handle: EventHandle,
    id: Option<u64>,
    reply_tx: Option<Sender<PickerResult>>,
    prev_selected: Option<usize>,
    timeout_interval: Option<Duration>,
    next_timeout: Option<Instant>,
}

impl LuaPicker {
    pub fn new(event_handle: EventHandle) -> Self {
        Self {
            picker: ListPicker::new().with_picker(PickerId::Lua),
            event_handle,
            id: None,
            reply_tx: None,
            prev_selected: None,
            timeout_interval: None,
            next_timeout: None,
        }
    }

    /// Opens the dialog for `id`, wiring `reply_tx` for the terminal outcome.
    pub fn open(
        &mut self,
        id: u64,
        items: Vec<PickerItemSpec>,
        config: PickerConfig,
        reply_tx: Sender<PickerResult>,
    ) {
        let entries: Vec<PickerEntry> = items
            .iter()
            .map(|s| PickerEntry {
                label: s.label.clone(),
                detail: s.detail.clone(),
                section: s.section.clone(),
                section_detail: s.section_detail.clone(),
                age: s.age,
            })
            .collect();
        let mut picker = ListPicker::new()
            .with_picker(PickerId::Lua)
            .with_submit_keys(config.submit_keys.clone())
            .with_delete_key(KeyEvent::new(DELETE_KEY, KeyModifiers::CONTROL));
        if let Some(footer) = &config.footer {
            picker = picker.with_footer_line(hint_footer(footer));
        }
        picker.open(entries, config.title.clone().unwrap_or_default());
        if let Some(cursor) = config.cursor {
            picker.select(cursor);
        }

        // The dialog slot is single: a second open wins it. The replaced
        // call's reply_tx is dropped (it wakes with Close), so send its
        // Done to drain the callback entry instead of leaking it.
        if let Some(prev) = self.id.take() {
            self.event_handle.picker_event(prev, PickerEvent::Done);
        }
        self.picker = picker;
        self.id = Some(id);
        self.reply_tx = Some(reply_tx);
        self.timeout_interval = config.timeout;
        self.next_timeout = config.timeout.map(|d| Instant::now() + d);

        let initial = self.picker.selected_index();
        self.prev_selected = initial;
        if config.notify_initial
            && let Some(idx) = initial
        {
            self.event_handle
                .picker_event(id, PickerEvent::Change { index: idx });
        }
    }

    pub fn is_open(&self) -> bool {
        self.picker.is_open()
    }

    pub fn view(&mut self, frame: &mut Frame, area: Rect) -> Rect {
        self.picker.view(frame, area)
    }

    #[cfg(test)]
    pub fn view_at(&mut self, frame: &mut Frame, area: Rect, now: &ClockSample) -> Rect {
        self.picker.view_at(frame, area, now)
    }

    pub fn handle_paste(&mut self, text: &str) -> bool {
        let changed = self.picker.handle_paste(text);
        if changed {
            self.reset_timeout();
        }
        changed
    }

    pub fn handle_key(&mut self, key: KeyEvent) -> LuaPickerAction {
        let pre_index = self.picker.selected_index();
        let action = self.picker.handle_key(key);
        match action {
            PickerAction::Select(_) => {
                let idx = pre_index.expect("Select is only produced while a selection is open");
                self.finish(PickerResult::Choice(idx));
                LuaPickerAction::Choice(idx)
            }
            PickerAction::Delete(idx) => {
                self.finish(PickerResult::Delete(idx));
                LuaPickerAction::Delete(idx)
            }
            PickerAction::Close => {
                self.finish(PickerResult::Close);
                LuaPickerAction::Close
            }
            PickerAction::Consumed | PickerAction::Toggle(..) => {
                self.reset_timeout();
                if self.picker.delete_confirming()
                    && let Some(k) = self.picker.delete_key()
                {
                    return LuaPickerAction::Confirming(format!(
                        "Press {} again to delete",
                        key_display(k)
                    ));
                }
                self.notify_change_if_moved();
                LuaPickerAction::Consumed
            }
        }
    }

    /// Fires `Timeout` when the idle window has elapsed and re-arms it; the
    /// firing owes no frame on its own (the `Cadence::after` wake is a poll,
    /// and any visual effect of the Lua callback arrives as its own action).
    pub fn tick(&mut self) -> Dirty {
        let Some(next) = self.next_timeout else {
            return Dirty::NO;
        };
        if Instant::now() < next {
            return Dirty::NO;
        }
        if let Some(id) = self.id {
            self.event_handle.picker_event(id, PickerEvent::Timeout);
        }
        self.arm_timeout();
        Dirty::NO
    }

    fn reset_timeout(&mut self) {
        if self.picker.is_open() {
            self.arm_timeout();
        }
    }

    fn arm_timeout(&mut self) {
        self.next_timeout = self.timeout_interval.map(|d| Instant::now() + d);
    }

    fn notify_change_if_moved(&mut self) {
        let Some(post) = self.picker.selected_index() else {
            return;
        };
        if self.prev_selected == Some(post) {
            return;
        }
        self.prev_selected = Some(post);
        if let Some(id) = self.id {
            self.event_handle
                .picker_event(id, PickerEvent::Change { index: post });
        }
    }

    /// Terminal: report the outcome and `Done` exactly once, so a second
    /// close (Esc racing `close_all_overlays`) is a no-op.
    fn finish(&mut self, result: PickerResult) {
        self.picker.close();
        self.next_timeout = None;
        if let Some(tx) = self.reply_tx.take() {
            let _ = tx.send(result);
        }
        if let Some(id) = self.id.take() {
            self.event_handle.picker_event(id, PickerEvent::Done);
        }
    }
    pub fn select(&mut self, index: usize) -> bool {
        self.picker.select(index)
    }

    pub fn click_input(&mut self, pos: Position) -> bool {
        self.picker.click_input(pos)
    }

    pub fn scroll(&mut self, delta: i32) {
        self.picker.scroll(delta);
    }

    pub fn hits(&self) -> Vec<HitTarget> {
        if self.is_open() {
            self.picker.hits()
        } else {
            Vec::new()
        }
    }

    pub fn scroll_line(&self) -> Option<u16> {
        self.is_open().then(|| self.picker.scroll_line())
    }

    pub fn content_len(&self) -> Option<u16> {
        self.is_open().then(|| self.picker.content_len()).flatten()
    }

    pub fn scroll_to_line(&mut self, line: u16) {
        self.picker.scroll_to_line(line);
    }
}

impl Overlay for LuaPicker {
    fn is_open(&self) -> bool {
        self.is_open()
    }

    fn close(&mut self) {
        if self.is_open() {
            self.finish(PickerResult::Close);
        }
    }

    fn cadence(&self) -> Cadence {
        let timeout = match self.next_timeout {
            Some(next) => Cadence::after(next.saturating_duration_since(Instant::now())),
            None => Cadence::IDLE,
        };
        Cadence::any([self.picker.cadence(), timeout])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use makima_lua::test_support;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    const EXPECT_CONFIRM_FLASH: &str = "Press Ctrl+D again to delete";

    fn spec(label: &str) -> PickerItemSpec {
        PickerItemSpec {
            label: label.into(),
            detail: None,
            section: None,
            section_detail: None,
            age: None,
        }
    }

    fn spoke_at(unix_secs: u64) -> ClockSample {
        ClockSample {
            monotonic: Instant::now(),
            unix: Duration::from_secs(unix_secs),
        }
    }

    fn aged_spec(label: &str, age_secs: u64) -> PickerItemSpec {
        PickerItemSpec {
            label: label.into(),
            age: Some(Duration::from_secs(age_secs)),
            ..Default::default()
        }
    }

    fn key_event(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl_d() -> KeyEvent {
        KeyEvent::new(DELETE_KEY, KeyModifiers::CONTROL)
    }

    #[test]
    fn choice_reports_result_then_done() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx, rx) = flume::bounded(1);
        lp.open(7, vec![spec("A"), spec("B")], PickerConfig::default(), tx);
        assert!(lp.is_open());

        assert_eq!(
            lp.handle_key(key_event(KeyCode::Down)),
            LuaPickerAction::Consumed
        );
        assert_eq!(
            probe.try_recv_picker_event(),
            Some((7, PickerEvent::Change { index: 1 }))
        );

        assert_eq!(
            lp.handle_key(key_event(KeyCode::Enter)),
            LuaPickerAction::Choice(1)
        );
        assert_eq!(rx.try_recv().unwrap(), PickerResult::Choice(1));
        assert!(!lp.is_open());
        assert_eq!(probe.try_recv_picker_event(), Some((7, PickerEvent::Done)));
    }

    #[test]
    fn done_is_reported_exactly_once_on_double_close() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx, rx) = flume::bounded(1);
        lp.open(3, vec![spec("A")], PickerConfig::default(), tx);

        assert_eq!(
            lp.handle_key(key_event(KeyCode::Enter)),
            LuaPickerAction::Choice(0)
        );
        assert_eq!(rx.try_recv().unwrap(), PickerResult::Choice(0));

        // An Esc or close_all_overlays racing the choice must not re-send.
        lp.close();
        assert!(rx.try_recv().is_err());
        assert_eq!(probe.try_recv_picker_event(), Some((3, PickerEvent::Done)));
        assert!(probe.try_recv_picker_event().is_none());
    }

    #[test]
    fn esc_reports_close() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx, rx) = flume::bounded(1);
        lp.open(1, vec![spec("A")], PickerConfig::default(), tx);

        assert_eq!(
            lp.handle_key(key_event(KeyCode::Esc)),
            LuaPickerAction::Close
        );
        assert_eq!(rx.try_recv().unwrap(), PickerResult::Close);
        assert_eq!(probe.try_recv_picker_event(), Some((1, PickerEvent::Done)));
    }

    #[test]
    fn delete_key_arms_then_reports_delete() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx, rx) = flume::bounded(1);
        lp.open(2, vec![spec("A"), spec("B")], PickerConfig::default(), tx);

        assert_eq!(
            lp.handle_key(ctrl_d()),
            LuaPickerAction::Confirming(EXPECT_CONFIRM_FLASH.into())
        );
        assert_eq!(lp.handle_key(ctrl_d()), LuaPickerAction::Delete(0));
        assert_eq!(rx.try_recv().unwrap(), PickerResult::Delete(0));
        assert!(!lp.is_open());
        assert_eq!(probe.try_recv_picker_event(), Some((2, PickerEvent::Done)));
    }

    #[test]
    fn timeout_fires_once_per_idle_window_and_rearms() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let config = PickerConfig {
            timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let (tx, _rx) = flume::bounded(1);
        lp.open(4, vec![spec("A")], config, tx);

        // A pending window is a wake deadline that owes no motion.
        assert!(lp.cadence().frame().is_some());
        assert!(!lp.cadence().moves());

        lp.next_timeout = Some(Instant::now() - Duration::from_millis(1));
        assert_eq!(lp.tick(), Dirty::NO, "firing the event owes no frame");
        assert_eq!(
            probe.try_recv_picker_event(),
            Some((4, PickerEvent::Timeout))
        );

        // The window re-armed: nothing fires until the next deadline.
        assert_eq!(lp.tick(), Dirty::NO);
        assert!(probe.try_recv_picker_event().is_none());
        assert!(lp.next_timeout > Some(Instant::now()));

        // A key press resets the window instead of letting it fire.
        lp.next_timeout = Some(Instant::now() - Duration::from_millis(1));
        assert_eq!(
            lp.handle_key(key_event(KeyCode::Down)),
            LuaPickerAction::Consumed
        );
        assert_eq!(lp.tick(), Dirty::NO);
        assert!(probe.try_recv_picker_event().is_none());
    }

    #[test]
    fn lua_picker_live_age_reaches_visible_buffer() {
        let (handle, _probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx, rx) = flume::bounded(1);
        lp.open(
            1,
            vec![aged_spec("alpha", 991), aged_spec("beta", 997)],
            PickerConfig::default(),
            tx,
        );
        let area = Rect::new(0, 0, 60, 12);
        let mut terminal = Terminal::new(TestBackend::new(area.width, area.height)).unwrap();
        let mut render = |lp: &mut LuaPicker, now: ClockSample| {
            terminal
                .draw(|frame| {
                    lp.view_at(frame, area, &now);
                })
                .unwrap();
            let buf = terminal.backend().buffer();
            (0..area.height)
                .map(|y| {
                    (0..area.width)
                        .map(|x| {
                            buf.cell(Position::new(x, y))
                                .map(|c| c.symbol())
                                .unwrap_or(" ")
                        })
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
        };

        let rows = render(&mut lp, spoke_at(1000));
        assert!(rows.iter().any(|row| row.contains("9s")));
        assert!(rows.iter().any(|row| row.contains("3s")));

        let rows = render(&mut lp, spoke_at(1001));
        assert!(rows.iter().any(|row| row.contains("10s")));
        assert!(rows.iter().any(|row| row.contains("4s")));

        assert_eq!(
            lp.handle_key(key_event(KeyCode::Down)),
            LuaPickerAction::Consumed
        );
        assert_eq!(
            lp.handle_key(key_event(KeyCode::Enter)),
            LuaPickerAction::Choice(1),
            "age rendering preserves original callback indices"
        );
        assert_eq!(rx.try_recv().unwrap(), PickerResult::Choice(1));
    }

    #[test]
    fn lua_picker_age_is_independent_of_idle_timeout() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let config = PickerConfig {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        };
        let (tx, _rx) = flume::bounded(1);
        lp.open(2, vec![aged_spec("alpha", 990)], config, tx);

        let cadence = lp.cadence();
        assert_eq!(cadence.frame(), Some(crate::repaint::AGE_FRAME));
        assert!(cadence.moves(), "ages owe frames even with an idle timeout");
        assert_eq!(lp.tick(), Dirty::NO);
        assert!(
            probe.try_recv_picker_event().is_none(),
            "looking at a live age must not fire on_timeout"
        );
    }

    #[test]
    fn lua_picker_age_close_and_replacement() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx, _rx) = flume::bounded(1);
        lp.open(
            3,
            vec![aged_spec("alpha", 991)],
            PickerConfig::default(),
            tx,
        );
        assert!(lp.cadence().moves());

        lp.close();
        assert_eq!(lp.cadence(), Cadence::IDLE);
        assert_eq!(probe.try_recv_picker_event(), Some((3, PickerEvent::Done)));

        let (tx, _rx) = flume::bounded(1);
        lp.open(
            4,
            vec![aged_spec("alpha", 991)],
            PickerConfig::default(),
            tx,
        );
        let (tx, _rx) = flume::bounded(1);
        lp.open(5, vec![spec("static")], PickerConfig::default(), tx);
        assert_eq!(probe.try_recv_picker_event(), Some((4, PickerEvent::Done)));
        assert_eq!(
            lp.cadence(),
            Cadence::IDLE,
            "a static replacement drops the age animation"
        );
    }

    #[test]
    fn closed_dialog_is_idle_and_close_is_a_noop() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        assert_eq!(lp.cadence(), Cadence::IDLE);
        lp.close();
        assert!(probe.try_recv_picker_event().is_none());
    }

    #[test]
    fn notify_initial_fires_change_for_the_initial_item() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let config = PickerConfig {
            notify_initial: true,
            ..Default::default()
        };
        let (tx, _rx) = flume::bounded(1);
        lp.open(9, vec![spec("A"), spec("B")], config, tx);

        assert_eq!(
            probe.try_recv_picker_event(),
            Some((9, PickerEvent::Change { index: 0 }))
        );
        // The initial fire counts as the last change: a first Down then reports 1.
        assert_eq!(
            lp.handle_key(key_event(KeyCode::Down)),
            LuaPickerAction::Consumed
        );
        assert_eq!(
            probe.try_recv_picker_event(),
            Some((9, PickerEvent::Change { index: 1 }))
        );
    }

    #[test]
    fn a_second_open_sends_done_for_the_replaced_dialog() {
        let (handle, probe) = test_support::probed_event_handle();
        let mut lp = LuaPicker::new(handle);
        let (tx1, _rx1) = flume::bounded(1);
        lp.open(5, vec![spec("A")], PickerConfig::default(), tx1);

        let (tx2, _rx2) = flume::bounded(1);
        lp.open(6, vec![spec("B")], PickerConfig::default(), tx2);

        // The replaced dialog's entry is drained; the second dialog stays live.
        assert_eq!(probe.try_recv_picker_event(), Some((5, PickerEvent::Done)));
        assert!(lp.is_open());
        assert_eq!(
            lp.handle_key(key_event(KeyCode::Enter)),
            LuaPickerAction::Choice(0)
        );
        assert_eq!(probe.try_recv_picker_event(), Some((6, PickerEvent::Done)));
    }
}
