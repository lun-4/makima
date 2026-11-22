use crate::terminal;
use arboard::Clipboard;
#[cfg(target_os = "linux")]
use arboard::{LinuxClipboardKind, SetExtLinux};

pub(crate) enum CopyResult {
    Copied,
    Noop,
}

pub(crate) struct ClipboardState {
    native: Option<Clipboard>,
}

impl ClipboardState {
    pub(crate) fn new() -> Self {
        Self {
            native: Clipboard::new().ok(),
        }
    }

    pub(crate) fn copy_text(&mut self, text: &str) -> Result<CopyResult, String> {
        Self::copy_text_impl(
            text,
            terminal::supports_osc52(),
            |t| self.copy_native(t),
            terminal::copy_to_clipboard,
        )
    }

    fn copy_text_impl<F, G>(
        text: &str,
        supports_osc52: bool,
        native: F,
        osc52: G,
    ) -> Result<CopyResult, String>
    where
        F: FnOnce(&str) -> Result<(), String>,
        G: FnOnce(&str) -> Result<(), String>,
    {
        if text.is_empty() {
            return Ok(CopyResult::Noop);
        }
        let native_err = match native(text) {
            Ok(()) => {
                if supports_osc52 && let Err(error) = osc52(text) {
                    tracing::debug!(%error, "supplemental OSC52 clipboard copy failed");
                }
                return Ok(CopyResult::Copied);
            }
            Err(e) => e,
        };
        match osc52(text) {
            Ok(()) => Ok(CopyResult::Copied),
            Err(osc52_err) => Err(format!("native ({native_err}); osc52 ({osc52_err})")),
        }
    }

    fn copy_native(&mut self, text: &str) -> Result<(), String> {
        let Some(clipboard) = &mut self.native else {
            return Err("native clipboard unavailable".into());
        };

        #[cfg(target_os = "linux")]
        {
            let primary = clipboard
                .set()
                .clipboard(LinuxClipboardKind::Primary)
                .text(text);
            let system = clipboard
                .set()
                .clipboard(LinuxClipboardKind::Clipboard)
                .text(text);
            match (primary, system) {
                (_, Ok(())) | (Ok(()), _) => Ok(()),
                (Err(p), Err(s)) => Err(format!("primary: {p}; clipboard: {s}")),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            clipboard.set_text(text).map_err(|e| e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ClipboardState, CopyResult};
    use std::cell::Cell;
    use test_case::test_case;

    const TEXT: &str = "hello";
    const NATIVE_ERROR: &str = "no display";
    const OSC52_ERROR: &str = "stdout closed";

    #[test_case(false; "unknown_terminal")]
    #[test_case(true; "supported_terminal")]
    fn empty_text_is_noop_and_skips_both_backends(supports_osc52: bool) {
        let native_called = Cell::new(false);
        let osc52_called = Cell::new(false);
        let result = ClipboardState::copy_text_impl(
            "",
            supports_osc52,
            |_| {
                native_called.set(true);
                Ok(())
            },
            |_| {
                osc52_called.set(true);
                Ok(())
            },
        );
        assert!(matches!(result, Ok(CopyResult::Noop)));
        assert!(!native_called.get(), "empty text must not reach native");
        assert!(!osc52_called.get(), "empty text must not reach osc52");
    }

    #[test_case(false, false; "unknown_terminal_skips_osc52")]
    #[test_case(true, false; "supported_terminal_also_copies_osc52")]
    #[test_case(true, true; "osc52_failure_preserves_native_success")]
    fn native_success(supports_osc52: bool, osc52_fails: bool) {
        let osc52_called = Cell::new(false);
        let result = ClipboardState::copy_text_impl(
            TEXT,
            supports_osc52,
            |text| {
                assert_eq!(text, TEXT);
                assert!(!osc52_called.get());
                Ok(())
            },
            |text| {
                assert_eq!(text, TEXT);
                osc52_called.set(true);
                if osc52_fails {
                    Err(OSC52_ERROR.into())
                } else {
                    Ok(())
                }
            },
        );
        assert!(matches!(result, Ok(CopyResult::Copied)));
        assert_eq!(osc52_called.get(), supports_osc52);
    }

    #[test_case(false; "unknown_terminal")]
    #[test_case(true; "supported_terminal")]
    fn native_failure_is_recovered_by_osc52(supports_osc52: bool) {
        let result = ClipboardState::copy_text_impl(
            TEXT,
            supports_osc52,
            |_| Err(NATIVE_ERROR.into()),
            |text| {
                assert_eq!(text, TEXT);
                Ok(())
            },
        );
        assert!(matches!(result, Ok(CopyResult::Copied)));
    }

    #[test_case(false; "unknown_terminal")]
    #[test_case(true; "supported_terminal")]
    fn both_failing_composes_errors_with_native_first(supports_osc52: bool) {
        let result = ClipboardState::copy_text_impl(
            TEXT,
            supports_osc52,
            |_| Err(NATIVE_ERROR.into()),
            |_| Err(OSC52_ERROR.into()),
        );
        let Err(msg) = result else {
            panic!("expected Err when both backends fail");
        };
        assert_eq!(
            msg,
            format!("native ({NATIVE_ERROR}); osc52 ({OSC52_ERROR})")
        );
    }

    #[test]
    fn whitespace_only_text_is_not_noop() {
        let native_called = Cell::new(false);
        let result = ClipboardState::copy_text_impl(
            "   \n\t",
            false,
            |_| {
                native_called.set(true);
                Ok(())
            },
            |_| Ok(()),
        );
        assert!(matches!(result, Ok(CopyResult::Copied)));
        assert!(
            native_called.get(),
            "whitespace-only text must still be copied"
        );
    }
}
