//! Where Lua UI requests end up. Every frontend gets the same [`UiAction`]s
//! and declares what it renders through its [`UiSink`]; an action a sink
//! declines is dropped, which closes its reply channel, so the Lua caller
//! gets `NO_UI_ERR` at once instead of waiting.

use crate::UiAction;

pub const NO_UI_ERR: &str = "no interactive UI attached";

pub trait UiSink {
    /// Shows a notification in the frontend's own idiom.
    fn notify(&mut self, text: String);

    /// Renders {action}. The default shows notifications and declines the
    /// rest.
    fn render(&mut self, action: UiAction) {
        if let UiAction::Flash(text) = action {
            self.notify(text);
        }
    }
}

/// Who renders a host's UI actions; every frontend names one when it spawns
/// its host, so none is left without a reader.
pub enum HostUi {
    /// The frontend drains `PluginHost::ui_action_rx` on its own loop.
    Frontend,
    /// The host renders on this sink from a thread of its own.
    Sink(Box<dyn UiSink + Send>),
}

/// The sink of a frontend without an interactive UI: notifications go to
/// stderr, which no headless protocol uses for its output.
pub struct HeadlessUi;

impl UiSink for HeadlessUi {
    fn notify(&mut self, text: String) {
        eprintln!("{text}");
    }
}

/// Renders every action from {actions} on {sink} until the host closes the
/// channel.
pub(crate) fn serve(actions: flume::Receiver<UiAction>, mut sink: Box<dyn UiSink + Send>) {
    while let Ok(action) = actions.recv() {
        sink.render(action);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Recording(Vec<String>);

    impl UiSink for Recording {
        fn notify(&mut self, text: String) {
            self.0.push(text);
        }
    }

    const NOTICE: &str = "saved";

    #[test]
    fn default_render_shows_notifications_and_declines_the_rest() {
        let (reply_tx, reply_rx) = flume::bounded::<crate::WinView>(1);
        let mut sink = Recording(Vec::new());
        sink.render(UiAction::Flash(NOTICE.into()));
        sink.render(UiAction::WinSaveView { reply_tx });
        assert_eq!(sink.0, [NOTICE]);
        assert!(reply_rx.recv().is_err());
    }
}
