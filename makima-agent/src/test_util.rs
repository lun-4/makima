use crate::{
    ActorBackend, AgentInput, BackendResult, ControlWork, TurnContext, WorkKind,
    actor::{ActorHistory, AdmissionPreparation, PreparedReadiness},
    agent::TurnSetup,
    session_writer::{OpenedSessionIo, SessionIo, SessionWriter},
};
use makima_providers::provider::BoxFuture;
use makima_storage::id::MakimaId;
#[cfg(any(unix, all(test, windows)))]
use std::path::Path;
#[cfg(unix)]
use std::{ffi::CString, os::unix::ffi::OsStrExt};
use std::{io, time::Duration};

const UNEXPECTED_OPERATION: &str = "unexpected work in MockBackend";
#[cfg(unix)]
const FIFO_MODE: libc::mode_t = 0o600;

impl SessionWriter {
    pub fn with_io(session_id: MakimaId, io: impl SessionIo, next_seq: u64) -> Self {
        let mut opened: Option<OpenedSessionIo> = Some((Box::new(io), next_seq));
        Self::with_factory(session_id, move || {
            opened
                .take()
                .ok_or_else(|| io::Error::other("test I/O already opened"))
        })
    }
}

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

#[cfg(all(test, windows))]
pub(crate) fn test_symlink(
    link: impl FnOnce(&Path, &Path) -> io::Result<()>,
    target: &Path,
    path: &Path,
) -> bool {
    const ERROR_PRIVILEGE_NOT_HELD: i32 = 1314;
    match link(target, path) {
        Ok(()) => true,
        Err(error) if error.raw_os_error() == Some(ERROR_PRIVILEGE_NOT_HELD) => {
            eprintln!("skipping symlink assertions: {error}");
            false
        }
        Err(error) => panic!("could not create test symlink {path:?}: {error}"),
    }
}

/// Poll state that has no notification API. The timeout only guards against hangs.
pub async fn poll_until<T>(timeout: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    futures_lite::future::or(
        async {
            loop {
                if let Some(value) = probe() {
                    return Some(value);
                }
                smol::future::yield_now().await;
            }
        },
        async {
            smol::Timer::after(timeout).await;
            None
        },
    )
    .await
}

type MockTurn = dyn for<'a> FnMut(
        ActorHistory<'a>,
        TurnContext,
        AgentInput,
        WorkKind,
    ) -> BoxFuture<'a, BackendResult>
    + Send;

/// A fixture backend with an optional scripted turn and admission setup.
/// Unexpected work panics unless the fixture opts into no-op controls.
pub struct MockBackend {
    turn: Option<Box<MockTurn>>,
    setup: Option<TurnSetup>,
    noop_controls: bool,
}

impl MockBackend {
    pub const STRICT: Self = Self {
        turn: None,
        setup: None,
        noop_controls: false,
    };

    pub fn with_turn(
        mut self,
        turn: impl for<'a> FnMut(
            ActorHistory<'a>,
            TurnContext,
            AgentInput,
            WorkKind,
        ) -> BoxFuture<'a, BackendResult>
        + Send
        + 'static,
    ) -> Self {
        self.turn = Some(Box::new(turn));
        self
    }

    pub fn with_setup(mut self, setup: TurnSetup) -> Self {
        self.setup = Some(setup);
        self
    }

    pub fn with_noop_controls(mut self) -> Self {
        self.noop_controls = true;
        self
    }
}

impl ActorBackend for MockBackend {
    fn admission_preparation(&self) -> Option<AdmissionPreparation> {
        self.setup.clone().map(TurnSetup::preparation)
    }

    fn prepared_readiness(&self) -> Option<PreparedReadiness> {
        self.setup.as_ref().map(|_| TurnSetup::readiness())
    }

    fn run_turn<'a>(
        &'a mut self,
        history: ActorHistory<'a>,
        context: TurnContext,
        input: AgentInput,
        work: WorkKind,
    ) -> BoxFuture<'a, BackendResult> {
        match &mut self.turn {
            Some(turn) => turn(history, context, input, work),
            None => Box::pin(async { panic!("{UNEXPECTED_OPERATION}: turn") }),
        }
    }

    fn run_control<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: &'a ControlWork,
    ) -> BoxFuture<'a, BackendResult> {
        let enabled = self.noop_controls;
        Box::pin(async move {
            assert!(enabled, "{UNEXPECTED_OPERATION}: control");
            BackendResult::ControlDone
        })
    }

    fn run_compact<'a>(
        &'a mut self,
        _: ActorHistory<'a>,
        _: TurnContext,
        _: Option<&'a str>,
    ) -> BoxFuture<'a, BackendResult> {
        let enabled = self.noop_controls;
        Box::pin(async move {
            assert!(enabled, "{UNEXPECTED_OPERATION}: compact");
            BackendResult::CompactDone
        })
    }
}
