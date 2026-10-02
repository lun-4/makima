# Replace the positional prompt with `-p/--prompt` (issue #107, option B)

## Goal

`makima` no longer accepts a positional prompt. The prompt is passed with `-p, --prompt <PROMPT>`, `--print` becomes long-only, and an unknown first argument such as `makima resume` fails with clap's unrecognized-subcommand error. When the flag and piped stdin are both present, the prompt sent is the flag text followed by the stdin contents.

## Implementation Summary

Touch points, all in the root `makima` crate plus docs and scripts:

- `src/cli.rs`: `Cli` loses `initial_prompt`; gains `prompt: Option<String>` declared `#[arg(short, long)]`; `print` drops `short`. With no positionals left and `Option<Command>` subcommands, clap 4.6 (`clap_builder` `Parser::match_arg_error`) returns `ErrorKind::InvalidSubcommand` for any stray bare word, either via `invalid_subcommand` (when a subcommand name is close) or `unrecognized_subcommand` (otherwise). Both carry the same `ErrorKind`, and `main.rs` calls `Cli::parse()`, so a usage error with exit code 2 follows.
- `src/cmd/tui.rs`: `read_initial_prompt` is made testable (takes the stdin reader and a "stdin is piped" bool) and implements the combine rule. Both the print branch and the TUI branch of `run` call it. The SDK branch is untouched and must not read stdin, since stdin is its wire.
- `src/print.rs`: keeps its existing `None` fallback (read stdin until EOF), which now only triggers when stdin is a terminal and no flag was given, preserving the bare `makima --print` behaviour of reading a typed prompt until Ctrl+D. Module doc comment updated.
- Docs: `site/docs/content/headless/_index.md`, `site/docs/content/cli/_index.md` (hand-written), and the generated folder-trust page via `maki-docgen/src/gen_folder_trust.rs`.
- Scripts: `scripts/collect.py`, `scripts/tbench_maki_agent.py`.

Combine rule: `flag` and piped stdin `s`. If `s` is non-empty after trimming, the prompt is `format!("{flag}\n\n{s}")`. Otherwise the flag alone. With no flag, piped stdin is the prompt as today (including an empty string, unchanged behaviour). With neither, `None`. The flag text goes first so a leading slash command in `-p` still parses as a command in print mode.

Non-goals: rejecting `--prompt` in SDK mode or ACP (it is silently unused there today, as the positional was); any alias keeping the old positional; changing `-c`'s optional-value behaviour.

## Implementation Plan

### Phase 1: CLI surface (`src/cli.rs`)

1. Change `print` to `#[arg(long)]`. Keep its doc comment, noting Claude Code compatibility now applies to the long flag.
2. Remove `initial_prompt` and its `#[arg(value_name = "PROMPT")]`. Add, near `print`:
   ```rust
   /// Initial prompt. Seeds the TUI, or runs headless with --print. Piped stdin is appended after it, or used alone without it
   #[arg(short, long)]
   pub prompt: Option<String>,
   ```
3. Tests in the existing `mod tests` (use `clap::error::ErrorKind`):
   - `unknown_first_argument_is_rejected`, `#[test_case]` for `["makima", "resume"]` and `["makima", "fix the bug"]`: `Cli::try_parse_from` errs with `kind() == ErrorKind::InvalidSubcommand`.
   - `prompt_flag_parses`, cases `-p hello`, `--prompt hello`, `--prompt=hello`: `cli.prompt == Some("hello")` and `!cli.print`.
   - `print_flag_is_long_only`: `["makima", "--print"]` sets `print` with `prompt == None`; `["makima", "-p"]` fails to parse with the missing-value kind (expected `ErrorKind::InvalidValue`; confirm the exact kind when writing the test and assert it, not just `is_err()`).
   - `print_with_prompt_flag`: `["makima", "--print", "-p", "hello"]` sets both.
   Put `"hello"` in a test const per AGENTS.md.

### Phase 2: prompt resolution (`src/cmd/tui.rs`, `src/print.rs`)

1. Replace `read_initial_prompt` with:
   ```rust
   fn read_initial_prompt(flag: Option<String>, mut stdin: impl Read, piped: bool) -> Result<Option<String>>
   ```
   If `piped`, read stdin to a string (`.context("read stdin")`), then apply the combine rule above. If not piped, return `flag`. Name the separator a const (`PIPED_INPUT_SEPARATOR: &str = "\n\n"`) at the top of the file.
2. In `run`, call the resolver once, right after the SDK early return and before `if cli.print`: `let initial_prompt = read_initial_prompt(cli.prompt.take(), io::stdin(), !io::stdin().is_terminal())?;`. Pass it to `crate::print::run` (replacing `cli.initial_prompt`) and to `maki_ui::run` (the existing `let mut initial_prompt = read_initial_prompt(...)` line in the TUI branch goes away; keep the binding `mut` for the `.take()` in the loop). One wiring point instead of two. `cli` is already `mut`.
3. `src/print.rs`: leave the `None` fallback in `run`; update the `//!` line to `makima --print -p "prompt"`.
4. Unit tests in `tui.rs`'s `mod tests`, driving `read_initial_prompt` with `Cli::parse_from(...).prompt` and a `&[u8]` reader:
   - `#[test_case]` `initial_prompt_resolution` covering: flag, not piped → flag; no flag, piped `"data"` → `"data"`; flag + piped `"data"` → `"hello\n\ndata"` (built from the consts); flag + piped empty/whitespace → flag; no flag, not piped → `None`.
   - Build the inputs from `Cli::parse_from(["makima", "-p", PROMPT])` and `Cli::parse_from(["makima", "--print", "-p", PROMPT])` so the test fails if the flag stops reaching the resolver.

### Phase 3: docs and scripts

1. `site/docs/content/headless/_index.md`: intro becomes "with `--print`; pass the prompt with `-p`". Every example moves from `makima "..." --print` to `makima --print -p "..."`. Stdin example `echo "..." | makima -p` becomes `| makima --print`. Add one short paragraph under the stdin example: with `-p` and piped stdin, the prompt is the `-p` text, a blank line, then stdin, and a run whose stdin is an open pipe that never closes will wait on it (use `</dev/null`). The pipe examples (`cargo build 2>&1 | makima "Fix..." --print` and the two `git` ones) silently drop stdin today because the positional wins; under the combine rule they work as written once moved to `-p`, so keep them as pipes. The `grep -rl ... | while read file; do makima ...; done` example must gain `</dev/null` on the makima call, otherwise the first makima reads the rest of grep's file list and the loop stops after one file; use it as the motivating example in the `</dev/null` paragraph. Claude Code compatibility section: `claude -p "fix the bug" --output-format json` becomes `makima --print -p "fix the bug" --output-format json`, and drop "drop-in" wording that now overstates parity (the JSON output is still compatible; the flag spelling differs by `--print`).
2. `site/docs/content/cli/_index.md`: usage block becomes `makima [OPTIONS]` / `makima <COMMAND>`; the paragraph under it describes `-p` and stdin; flag table: `-p` row becomes `--print` (long only) and add `-p`, `--prompt <TEXT>` (noting `--prompt=<TEXT>` for text starting with `-`, and that piped stdin is appended); add `-p` to the run-path matrix (TUI yes, `--print` yes, SDK no); "Everyday examples" one-shot becomes `makima --print --yolo -m anthropic/claude-sonnet-4-6 -p "summarize the architecture"`. Add a one-line note that a bare word that is not a subcommand is an error.
3. `maki-docgen/src/gen_folder_trust.rs`: `makima --trust -p "run the test suite"` becomes `makima --trust --print -p "run the test suite"`, then `just gen-docs` to regenerate `site/docs/content/folder-trust/_index.md`.
4. `scripts/collect.py` `build_cmd_makima`: `["makima", "--print", "--verbose", "--output-format", "stream-json", f"--prompt={args.prompt}"]` (the `=` form survives prompts starting with `-`). Also pass `stdin=subprocess.DEVNULL` to the `Popen` at line 420, so makima never reads the harness's own stdin.
5. `scripts/tbench_maki_agent.py`: set `escaped = shlex.quote(f"--prompt={instruction}")` and replace `-- {escaped}` with `{escaped}`, so the quoted token survives instructions that start with `-`.
6. Comments that use `-p` as shorthand for print mode: `maki-lua/src/session_snapshot.rs:93`, `maki-lua/src/agent_autocmd.rs:2`, `maki-lua/src/api/top.rs:101` and `:292`, `src/print.rs:377`. Change them to `--print` (e.g. `makima --print`). `top.rs:101` feeds the generated `site/docs/content/lua-api/_index.md`, so regenerate with `just gen-docs`.
7. `README.md` line 56 already uses `--print` only; leave it.

### Phase 4: verify

`just check`, `just lint`, `cargo nextest run -p maki` (the root package is `maki`, binary `makima`), `just gen-docs-check`. Then the manual checks in Test Strategy.

## Acceptance Criteria

- AC.1: `makima resume` and `makima "fix the bug"` fail to parse with `ErrorKind::InvalidSubcommand` and never reach `cmd::dispatch`. Checked by `unknown_first_argument_is_rejected`, and manually by `cargo run -- resume` printing an unrecognized-subcommand usage error with exit code 2.
- AC.2: `-p`, `--prompt X`, and `--prompt=X` set `Cli::prompt`; `--print` is long-only and bare `-p` is a parse error. Checked by `prompt_flag_parses`, `print_flag_is_long_only`, `print_with_prompt_flag`.
- AC.3: The flag seeds the prompt in both the TUI and print paths, piped stdin seeds it without the flag, and both together produce flag text, blank line, stdin. Checked by `initial_prompt_resolution`; the call sites in `tui::run` are checked manually: `cargo run -- -p hi` opens the TUI with "hi" submitted, and `echo data | cargo run -- --print -p "repeat the text after this line"` returns output reflecting "data".
- AC.4: Docs and scripts no longer show the positional or `-p` as print. Checked by `just gen-docs-check` passing and `rg -n 'makima "|makima -p --|"-p", "--verbose"|-- \{escaped\}' site scripts maki-docgen` and `rg -n 'maki(ma)? -p\b' maki-lua src site` returning nothing that uses `-p` to mean print.
- AC.5: The workspace stays green: `just lint` and `just test` pass.

## Test Strategy

| AC | Test |
|----|------|
| AC.1 | `cli::tests::unknown_first_argument_is_rejected` (new) |
| AC.2 | `cli::tests::prompt_flag_parses`, `print_flag_is_long_only`, `print_with_prompt_flag` (new) |
| AC.3 | `cmd::tui::tests::initial_prompt_resolution` (new) plus the two manual runs |
| AC.4 | `just gen-docs-check`, the `rg` sweep |
| AC.5 | `just lint`, `just test` |

Layers: clap parsing and the resolver are unit-tested. There is no binary-level harness in the repo (no `tests/`, no `assert_cmd`), so the final hop from `tui::run` into `print::run` / `maki_ui::run` is covered by the manual runs (a known, accepted gap, also listed in Risks). The resolver is called once in `run` and its result passed to both runners, and the unit tests feed the resolver from parsed `Cli` values, so a renamed or dropped field fails to compile or fails the test. Building a binary harness for two one-line call sites is out of proportion to this change.

Existing tests that mention the positional: none (`rg initial_prompt` shows only the field, `tui.rs`, and `maki-ui`'s own `initial_prompt` parameter, which is unrelated and stays). `print_mode_bare_continue_errors` uses `--print -c` and still passes.

## Review Strategy

Planning: `plan-reviewer` pass before handoff. Implementation: no repo-specific review guidance beyond AGENTS.md, so dispatch a `general-purpose` (Opus) reviewer over the diff, checking AGENTS.md style rules (consts, no trivial comments, test_case naming) and the docs tone rules. Fix or rebut all findings; re-review on critical findings.

## Documentation Strategy

User docs as listed in Phase 3 (headless guide, CLI reference, generated folder-trust page). Docs tone per AGENTS.md: no em-dashes, plain, concise. `AGENTS.md` and `README.md` need no change. Close the issue's "Decision" section by noting option B with `-p/--prompt` and the stdin combination in the PR description (via the `pr-propose` skill if asked), not by editing the issue.

## Risks, Blockers, and Required Decisions

Decisions already made by the operator: option B; long spelling `--prompt`; flag and piped stdin are combined (flag first, blank line, stdin).

- Hang risk from combining: previously, giving a prompt meant stdin was never read. Now any non-terminal stdin is read to EOF, so a caller whose stdin is an open pipe that never closes (some CI runners, Node `child_process` defaults) will block. Mitigated by the docs note recommending `</dev/null`; `tbench_maki_agent.py` already redirects. No timeout, per the "no weird sleeps" spirit and because it would truncate slow pipes.
- Breaking change for users: `makima -p` (print from stdin) now errors with a missing-value message, which is loud. `makima -p "x"` (Claude Code muscle memory) opens the TUI instead of running headless. In a terminal that is visible. From an orchestrator whose stdin is an open pipe, the resolver reads stdin to EOF before the TUI starts, so the run can hang silently rather than failing on terminal setup. Call this out in the PR description as a migration note. A guard that bails from the TUI branch before reading stdin when stdout or stderr is not a terminal would turn the hang into an error; it is outside option B and is offered to the operator at handoff rather than planned here.
- The hop from `tui::run` into the two runners has no automated test (no binary harness); covered by the manual runs in AC.3.
- `--prompt` passed in SDK mode is ignored, as the positional was. Not addressed here.
