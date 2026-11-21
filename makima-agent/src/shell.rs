#[cfg(windows)]
use std::env;
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use makima_config::Shell;

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
