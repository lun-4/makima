use crate::{
    ActorBackend, AgentInput, BackendResult, ControlWork, TurnContext, WorkKind,
    actor::ActorHistory,
};
#[cfg(unix)]
use std::{ffi::CString, io, os::unix::ffi::OsStrExt, path::Path};
use std::{future::Future, pin::Pin};

const UNEXPECTED_OPERATION: &str = "StubBackend must not execute work";
#[cfg(unix)]
const FIFO_MODE: libc::mode_t = 0o600;

/// Create a FIFO at {path}. rustix has no `mkfifoat` on Apple targets.
#[cfg(unix)]
pub fn make_fifo(path: &Path) -> io::Result<()> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: `path` is a valid NUL-terminated string that outlives the call.
    if unsafe { libc::mkfifo(path.as_ptr(), FIFO_MODE) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// An inert backend for fixtures that must not execute turns, controls, or compaction.
pub struct StubBackend;

impl ActorBackend for StubBackend {
    fn run_turn<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: AgentInput,
        _: WorkKind,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("{UNEXPECTED_OPERATION}") })
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("{UNEXPECTED_OPERATION}") })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = BackendResult> + Send + 'a>> {
        Box::pin(async { panic!("{UNEXPECTED_OPERATION}") })
    }
}
