use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use makima_config::Shell;

const GIT_INSTALL_ROOT: &str = "GIT_INSTALL_ROOT";
/// Relative to `GIT_INSTALL_ROOT`. `bin/bash.exe` is the shim Git for Windows
/// ships so a plain spawn gets the same MSYS environment as its own terminal.
const GIT_BASH: &str = "bin/bash.exe";

/// Builds the command that runs `script` under the configured shell. Unix
/// always gets `bash -c`.
pub fn command(shell: Shell, script: &str) -> Command {
    let (program, flag) = program_and_flag(shell, git_bash().as_deref());
    let mut command = Command::new(program);
    command.arg(flag).arg(script);
    command
}

/// The shell a plugin should assume when it builds command text. `auto` only
/// claims bash when Git for Windows is actually present.
pub fn name(shell: Shell) -> &'static str {
    name_of(resolved(shell, git_bash().as_deref()))
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
    match shell {
        #[cfg(windows)]
        Shell::Auto if git_bash.is_none() => Shell::Cmd,
        _ => shell,
    }
}

fn program_and_flag(shell: Shell, git_bash: Option<&Path>) -> (PathBuf, &'static str) {
    match resolved(shell, git_bash) {
        #[cfg(windows)]
        Shell::Powershell => ("powershell.exe".into(), "-Command"),
        #[cfg(windows)]
        Shell::Cmd => ("cmd.exe".into(), "/C"),
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
    use super::*;

    const FAKE_GIT: &str = "/Program Files/Git/bin/bash.exe";

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
    fn auto_falls_back_to_cmd_without_git_bash() {
        assert_eq!(
            program_and_flag(Shell::Auto, None),
            ("cmd.exe".into(), "/C")
        );
        assert_eq!(name_of(resolved(Shell::Auto, None)), "cmd");
    }
}
