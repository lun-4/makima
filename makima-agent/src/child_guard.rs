use std::io;
use std::process::ExitStatus;
use std::time::Duration;

use async_process::Child;

const REAP_TIMEOUT: Duration = Duration::from_secs(5);

pub struct ChildGuard {
    pid: u32,
    child: Option<Child>,
}

impl ChildGuard {
    pub fn new(child: Child) -> Self {
        Self {
            pid: child.id(),
            child: Some(child),
        }
    }

    pub fn id(&self) -> u32 {
        self.pid
    }

    pub async fn status(&mut self) -> io::Result<ExitStatus> {
        match self.child.as_mut() {
            Some(child) => {
                let result = child.status().await;
                if result.is_ok() {
                    self.child = None;
                }
                result
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "child already reaped",
            )),
        }
    }

    pub async fn kill_and_reap(&mut self) {
        self.signal_kill();
        if let Some(mut child) = self.child.take() {
            futures_lite::future::or(
                async {
                    let _ = child.status().await;
                },
                async {
                    async_io::Timer::after(REAP_TIMEOUT).await;
                },
            )
            .await;
        }
    }

    #[cfg(unix)]
    fn signal_kill(&self) {
        if self.child.is_some() {
            unsafe {
                libc::killpg(self.pid as i32, libc::SIGKILL);
            }
        }
    }

    #[cfg(not(unix))]
    fn signal_kill(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.kill();
        }
    }

    // Best-effort non-blocking reap after SIGKILL to prevent zombie accumulation.
    // All normal paths call .status() or .kill_and_reap() first, so this only
    // fires on unexpected drops (panics, early returns). Uses WNOHANG to never
    // block the async executor.
    #[cfg(unix)]
    fn reap_nonblocking(&mut self) {
        if self.child.take().is_some() {
            unsafe {
                libc::waitpid(self.pid as i32, std::ptr::null_mut(), libc::WNOHANG);
            }
        }
    }

    #[cfg(not(unix))]
    fn reap_nonblocking(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.try_status();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.is_some() {
            self.signal_kill();
        }
        self.reap_nonblocking();
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::process::CommandExt;

    use async_process::Child;

    use super::ChildGuard;

    fn spawn_sleep() -> Child {
        let mut std_cmd = std::process::Command::new("sleep");
        std_cmd.arg("60");
        unsafe {
            std_cmd.pre_exec(|| {
                libc::setsid();
                Ok(())
            });
        }
        let mut cmd: async_process::Command = std_cmd.into();
        cmd.spawn().expect("failed to spawn sleep")
    }

    fn is_alive(pid: u32) -> bool {
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }

    /// Asserts the process was killed by attempting to reap it. The guard's own
    /// `Drop` may have won the race and reaped already, which reads as `ECHILD`.
    /// Otherwise the status must show `SIGKILL`: a `sleep` that outlived a guard
    /// which failed to kill it would reap here with a normal exit.
    fn assert_reaped(pid: u32) {
        let mut status = 0;
        let reaped = unsafe { libc::waitpid(pid as i32, &mut status, 0) };
        if reaped == -1 {
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.raw_os_error(),
                Some(libc::ECHILD),
                "process {pid} was neither reaped here nor already reaped by the guard"
            );
            return;
        }
        assert_eq!(reaped, pid as i32);
        assert!(
            libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGKILL,
            "process {pid} exited with status {status} instead of being killed"
        );
    }

    #[test]
    fn drop_kills_child_process() {
        let child = spawn_sleep();
        let pid = child.id();
        assert!(is_alive(pid));
        drop(ChildGuard::new(child));
        assert_reaped(pid);
    }

    #[test]
    fn kill_and_reap_kills_process() {
        smol::block_on(async {
            let child = spawn_sleep();
            let pid = child.id();
            assert!(is_alive(pid));
            let mut guard = ChildGuard::new(child);
            guard.kill_and_reap().await;
            assert_reaped(pid);
        });
    }

    #[test]
    fn status_after_reap_returns_error() {
        smol::block_on(async {
            let child = spawn_sleep();
            let mut guard = ChildGuard::new(child);
            guard.kill_and_reap().await;
            assert!(guard.status().await.is_err());
        });
    }
}
