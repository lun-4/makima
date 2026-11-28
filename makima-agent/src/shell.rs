#[cfg(windows)]
use std::env;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use async_process::{Command as AsyncCommand, Stdio};
use futures_lite::StreamExt;
use futures_lite::io::{AsyncBufReadExt, AsyncRead, BufReader};
use thiserror::Error;

use crate::{ActorError, AgentConfig, AgentRef, CancelToken, ChildGuard, ManagerError};

use makima_config::Shell;

const STREAM_FLUSH_INTERVAL: Duration = Duration::from_millis(100);
const SHELL_TIMEOUT: Duration = Duration::from_secs(300);

#[cfg(windows)]
const CMD_SCRIPT_FLAG: &str = "/C";
#[cfg(windows)]
const GIT_INSTALL_ROOT: &str = "GIT_INSTALL_ROOT";
/// Relative to `GIT_INSTALL_ROOT`. `bin/bash.exe` is the shim Git for Windows
/// ships so a plain spawn gets the same MSYS environment as its own terminal.
#[cfg(windows)]
const GIT_BASH: &str = "bin/bash.exe";

/// Builds the command that runs `script` under the configured shell. Unix
/// always gets `bash -c`.
pub fn command(shell: impl Into<ResolvedShell>, script: &str) -> Command {
    let shell = shell.into();
    let (program, flag) = program_and_flag(shell.shell, shell.git_bash.as_deref());
    let mut command = Command::new(program);
    #[cfg(windows)]
    if flag == CMD_SCRIPT_FLAG {
        command
            .args(["/D", "/S", flag])
            .raw_arg(format!(r#""{script}""#));
        return command;
    }
    #[cfg(windows)]
    if shell.shell == Shell::Powershell {
        command.args(["-NoProfile", "-NonInteractive"]);
    }
    command.arg(flag).arg(script);
    command
}

#[derive(Clone)]
pub struct ResolvedShell {
    shell: Shell,
    git_bash: Option<PathBuf>,
}

impl From<Shell> for ResolvedShell {
    fn from(shell: Shell) -> Self {
        let git_bash = if shell == Shell::Auto {
            git_bash()
        } else {
            None
        };
        Self {
            shell: resolved(shell, git_bash.as_deref()),
            git_bash,
        }
    }
}

impl ResolvedShell {
    pub fn name(&self) -> &'static str {
        name_of(self.shell)
    }
}

fn name_of(shell: Shell) -> &'static str {
    match shell {
        #[cfg(windows)]
        Shell::Powershell => "powershell",
        #[cfg(windows)]
        Shell::Cmd => "cmd",
        _ => "bash",
    }
}

/// `auto` is Git Bash when `GIT_INSTALL_ROOT` points at one, and cmd.exe
/// otherwise, so a machine without Git for Windows keeps working instead of
/// spawning a bash that is not there.
fn resolved(shell: Shell, git_bash: Option<&Path>) -> Shell {
    match (shell, git_bash) {
        #[cfg(windows)]
        (Shell::Auto, None) => Shell::Cmd,
        (shell, _) => shell,
    }
}

fn program_and_flag(shell: Shell, git_bash: Option<&Path>) -> (PathBuf, &'static str) {
    match resolved(shell, git_bash) {
        #[cfg(windows)]
        Shell::Powershell => ("powershell.exe".into(), "-Command"),
        #[cfg(windows)]
        Shell::Cmd => ("cmd.exe".into(), CMD_SCRIPT_FLAG),
        _ => (
            git_bash.map_or_else(|| PathBuf::from("bash"), Path::to_path_buf),
            "-c",
        ),
    }
}

/// Git for Windows exports `GIT_INSTALL_ROOT` to its children; a session
/// started from Explorer does not inherit it.
fn git_bash() -> Option<PathBuf> {
    #[cfg(not(windows))]
    {
        None
    }
    #[cfg(windows)]
    {
        let path = PathBuf::from(env::var_os(GIT_INSTALL_ROOT)?).join(GIT_BASH);
        path.is_file().then_some(path)
    }
}

#[derive(Debug, Error)]
pub enum ShellError {
    #[error(transparent)]
    Manager(#[from] ManagerError),
    #[error(transparent)]
    Actor(#[from] ActorError),
    #[error("agent working directory is unavailable")]
    MissingConfig,
    #[error("could not save shell result: {0}")]
    Persistence(String),
}

pub struct ShellOutput {
    pub output: String,
    pub is_error: bool,
}

impl AgentRef {
    /// Runs an explicit user command. Both forms report output to the caller;
    /// only `visible` commands add their result to the next user turn.
    pub async fn run_shell(
        &self,
        command: &str,
        visible: bool,
        config: &AgentConfig,
        cancel: &CancelToken,
        on_output: impl Fn(&str) + Send,
    ) -> Result<ShellOutput, ShellError> {
        self.validate_live()?;
        let actor = self.actor()?;
        let policy = actor.effective_config().ok_or(ShellError::MissingConfig)?;
        let (output, is_error) =
            match run_command(command, on_output, cancel, config, policy.context.cwd()).await {
                Ok(output) => (output, false),
                Err(output) => (output, true),
            };
        if visible {
            let label = if is_error { "Error" } else { "Output" };
            actor.record_preamble(format!("I ran: $ {command}\n\n{label}:\n{output}"))?;
            let journal = self.manager().journal();
            if journal.has_writer() {
                journal.flush().await.map_err(ShellError::Persistence)?;
            }
        }
        Ok(ShellOutput { output, is_error })
    }
}

async fn run_command(
    command: &str,
    on_output: impl Fn(&str),
    cancel: &CancelToken,
    config: &AgentConfig,
    cwd: &Path,
) -> Result<String, String> {
    let mut std_cmd = self::command(config.shell, command);
    std_cmd.current_dir(cwd).env("GIT_TERMINAL_PROMPT", "0");

    #[cfg(unix)]
    unsafe {
        std_cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }

    let mut cmd: AsyncCommand = std_cmd.into();
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().map_err(|e| format!("failed to spawn: {e}"))?;

    let (line_tx, line_rx) = flume::unbounded::<String>();
    if let Some(stdout) = child.stdout.take() {
        spawn_line_reader(BufReader::new(stdout), line_tx.clone());
    }
    if let Some(stderr) = child.stderr.take() {
        spawn_line_reader(BufReader::new(stderr), line_tx.clone());
    }
    let mut guard = ChildGuard::new(child);
    drop(line_tx);

    let mut output = String::new();
    let mut line_count: usize = 0;
    let mut truncated = false;
    let mut last_flush = Instant::now();
    let deadline = Instant::now() + SHELL_TIMEOUT;

    macro_rules! race_deadline {
        ($future:expr) => {
            futures_lite::future::race(
                $future,
                futures_lite::future::race(
                    async {
                        smol::Timer::at(deadline).await;
                        Err(format!("timed out after {}s", SHELL_TIMEOUT.as_secs()))
                    },
                    async {
                        cancel.cancelled().await;
                        Err("cancelled".to_string())
                    },
                ),
            )
            .await
        };
    }

    loop {
        let line = race_deadline!(async { Ok(line_rx.recv_async().await.ok()) });
        match line {
            Ok(Some(line)) => {
                if !truncated {
                    if !output.is_empty() {
                        output.push('\n');
                    }
                    output.push_str(&line);
                    line_count += 1;
                    if output.len() > config.max_output_bytes
                        || line_count >= config.max_output_lines
                    {
                        truncated = true;
                    }
                }
            }
            Ok(None) => break,
            Err(e) => {
                guard.kill_and_reap().await;
                return Err(e);
            }
        }

        if last_flush.elapsed() >= STREAM_FLUSH_INTERVAL && !output.is_empty() {
            on_output(&output);
            last_flush = Instant::now();
        }
    }

    let status =
        race_deadline!(async { guard.status().await.map_err(|e| format!("wait error: {e}")) });
    match status {
        Ok(status) => {
            on_output(&output);
            if truncated {
                output.push_str("\n[truncated]");
            }
            if !status.success() {
                if output.is_empty() {
                    return Err(format!("exited with code {}", status.code().unwrap_or(-1)));
                }
                return Err(output);
            }
            Ok(output)
        }
        Err(e) => {
            guard.kill_and_reap().await;
            Err(e)
        }
    }
}

fn spawn_line_reader<R: AsyncRead + Unpin + Send + 'static>(
    reader: BufReader<R>,
    tx: flume::Sender<String>,
) {
    smol::spawn(async move {
        let mut lines = reader.lines();
        while let Some(line) = lines.next().await {
            let Ok(line) = line else { break };
            if tx.send(line).is_err() {
                break;
            }
        }
    })
    .detach();
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use std::{fs, io};

    #[cfg(windows)]
    use test_case::test_case;

    #[cfg(windows)]
    use super::{Path, PathBuf, name_of, program_and_flag, resolved};
    use super::{Shell, command};

    #[cfg(windows)]
    const FAKE_GIT: &str = "/Program Files/Git/bin/bash.exe";
    #[cfg(windows)]
    const BATCH_SOURCE: &[u8] = b"@type \"%~1\"\r\n";
    #[cfg(windows)]
    const INPUT_CONTENT: &[u8] = b"hello from cmd\r\n";

    #[cfg(not(windows))]
    #[test]
    fn unix_always_uses_bash_dash_c() {
        let command = command(Shell::Cmd, "echo hi");
        assert_eq!(command.get_program(), "bash");
        let args: Vec<_> = command.get_args().collect();
        assert_eq!(args, ["-c", "echo hi"]);
    }

    #[cfg(windows)]
    #[test]
    fn each_shell_has_its_program_and_flag() {
        let git = Path::new(FAKE_GIT);
        assert_eq!(
            program_and_flag(Shell::Cmd, Some(git)),
            ("cmd.exe".into(), "/C")
        );
        assert_eq!(
            program_and_flag(Shell::Powershell, Some(git)),
            ("powershell.exe".into(), "-Command")
        );
        assert_eq!(
            program_and_flag(Shell::Auto, Some(git)),
            (PathBuf::from(git), "-c")
        );
        assert_eq!(name_of(Shell::Cmd), "cmd");
        assert_eq!(name_of(Shell::Powershell), "powershell");
    }

    #[cfg(windows)]
    #[test]
    fn powershell_disables_profile_and_interactive_prompts() {
        let script = "Write-Output 42";
        let command = command(Shell::Powershell, script);
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            ["-NoProfile", "-NonInteractive", "-Command", script]
        );
    }

    #[cfg(windows)]
    #[test_case("quoted command.cmd", "input file.txt", "output file.txt" ; "spaces")]
    #[test_case("quoted & command.cmd", "input & file.txt", "output & file.txt" ; "spaces_and_ampersands")]
    fn cmd_script_preserves_quotes_and_redirection(
        batch_file: &str,
        input_file: &str,
        output_file: &str,
    ) -> io::Result<()> {
        let dir = tempfile::tempdir()?;
        fs::write(dir.path().join(batch_file), BATCH_SOURCE)?;
        fs::write(dir.path().join(input_file), INPUT_CONTENT)?;

        let script = format!(r#"".\{batch_file}" "{input_file}" > "{output_file}""#);
        let output = command(Shell::Cmd, &script)
            .current_dir(dir.path())
            .output()?;
        assert!(
            output.status.success(),
            "cmd exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr),
        );
        assert_eq!(fs::read(dir.path().join(output_file))?, INPUT_CONTENT);
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn auto_falls_back_to_cmd_without_git_bash() {
        assert_eq!(
            program_and_flag(Shell::Auto, None),
            ("cmd.exe".into(), "/C")
        );
        assert_eq!(name_of(resolved(Shell::Auto, None)), "cmd");
    }
}
