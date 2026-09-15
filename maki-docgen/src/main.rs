mod gen_commands;
mod gen_config;
mod gen_keybindings;
mod gen_lua_api;
mod gen_plugins;
mod gen_providers;
mod gen_tools;
mod lua_util;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;

use color_eyre::eyre::{Context, Result, eyre};

const CONTENT_DIR: &str = "site/docs/content";

type Page = (&'static str, fn() -> Result<String>);

const PAGES: [Page; 7] = [
    ("tools", || Ok(gen_tools::generate())),
    ("plugins", || Ok(gen_plugins::generate())),
    ("providers", || Ok(gen_providers::generate())),
    ("configuration", || Ok(gen_config::generate())),
    ("lua-api", || Ok(gen_lua_api::generate())),
    ("keybindings", || Ok(gen_keybindings::generate())),
    ("commands", gen_commands::generate),
];

fn page_path(section: &str) -> PathBuf {
    Path::new(CONTENT_DIR).join(section).join("_index.md")
}

fn write_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .wrap_err_with(|| format!("could not create {}", parent.display()))?;
    }
    fs::write(path, content).wrap_err_with(|| format!("could not write {}", path.display()))?;
    println!("wrote {}", path.display());
    Ok(())
}

fn check_file(path: &Path, expected: &str) -> Result<bool> {
    match fs::read_to_string(path) {
        Ok(existing) if existing == expected => {
            println!("ok {}", path.display());
            Ok(true)
        }
        Ok(_) => {
            println!("mismatch {}", path.display());
            Ok(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!("missing {}", path.display());
            Ok(false)
        }
        Err(error) => Err(error).wrap_err_with(|| format!("could not read {}", path.display())),
    }
}

/// Generic over the sinks rather than taking `fn` pointers, so a test can pass
/// closures over its own counters instead of sharing statics with every other
/// test in the binary.
fn run_generation<C, W>(
    check: bool,
    pages: &[Page],
    check_output: C,
    write_output: W,
) -> Result<ExitCode>
where
    C: Fn(&Path, &str) -> Result<bool>,
    W: Fn(&Path, &str) -> Result<()>,
{
    let outputs = thread::scope(|scope| {
        let running: Vec<_> = pages
            .iter()
            .map(|(section, generate)| {
                let section = *section;
                let generate = *generate;
                (section, page_path(section), scope.spawn(generate))
            })
            .collect();
        running
            .into_iter()
            .map(|(section, path, page)| {
                let content = page
                    .join()
                    .map_err(|_| eyre!("documentation generator for `{section}` panicked"))??;
                Ok((path, content))
            })
            .collect::<Result<Vec<_>>>()
    })?;

    if check {
        let mut mismatches = 0;
        for (path, content) in &outputs {
            if !check_output(path, content)? {
                mismatches += 1;
            }
        }
        if mismatches == 0 {
            Ok(ExitCode::SUCCESS)
        } else {
            Err(eyre!("docs out of date, run `just gen-docs` to update"))
        }
    } else {
        for (path, content) in &outputs {
            write_output(path, content)?;
        }
        Ok(ExitCode::SUCCESS)
    }
}

fn main() -> ExitCode {
    let _ = color_eyre::install();
    let check = std::env::args().any(|argument| argument == "--check");
    match run_generation(check, &PAGES, check_file, write_file) {
        Ok(status) => status,
        Err(error) => {
            eprintln!("{error:?}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use color_eyre::eyre::{Result, eyre};

    use super::{Page, run_generation};

    const GENERATION_ERROR: &str = "generation failed";

    fn generated_page() -> Result<String> {
        Ok("generated".to_owned())
    }

    fn failed_page() -> Result<String> {
        Err(eyre!(GENERATION_ERROR))
    }

    fn panicking_page() -> Result<String> {
        panic!("test panic")
    }

    #[derive(Default)]
    struct Sinks {
        checks: AtomicUsize,
        writes: AtomicUsize,
    }

    impl Sinks {
        fn check(&self) -> impl Fn(&Path, &str) -> Result<bool> {
            move |_, _| {
                self.checks.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
        }

        fn write(&self) -> impl Fn(&Path, &str) -> Result<()> {
            move |_, _| {
                self.writes.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }

        fn counts(&self) -> (usize, usize) {
            (
                self.checks.load(Ordering::Relaxed),
                self.writes.load(Ordering::Relaxed),
            )
        }
    }

    #[test]
    fn generation_failure_prevents_output() {
        let sinks = Sinks::default();
        let pages: [Page; 2] = [("ok", generated_page), ("broken", failed_page)];

        for check in [false, true] {
            let error = run_generation(check, &pages, sinks.check(), sinks.write())
                .expect_err("generation failure");
            assert!(error.to_string().contains(GENERATION_ERROR));
            assert_eq!(sinks.counts(), (0, 0));
        }
    }

    #[test]
    fn worker_panic_reports_generation_failure() {
        let pages: [Page; 1] = [("panic", panicking_page)];
        let sinks = Sinks::default();
        let error =
            run_generation(false, &pages, sinks.check(), sinks.write()).expect_err("worker panic");
        assert!(error.to_string().contains("panic"));
        assert!(error.to_string().contains("documentation generator"));
    }

    #[test]
    fn generation_success_emits_all_pages() {
        let sinks = Sinks::default();
        let pages: [Page; 2] = [("one", generated_page), ("two", generated_page)];

        run_generation(false, &pages, sinks.check(), sinks.write()).expect("generation");
        assert_eq!(sinks.counts(), (0, 2));

        run_generation(true, &pages, sinks.check(), sinks.write()).expect("check");
        assert_eq!(sinks.counts(), (2, 2));
    }
}
