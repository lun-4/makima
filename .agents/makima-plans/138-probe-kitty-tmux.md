# Terminal graphics protocol handshake and automatic tmux passthrough detection (issue #138)

## Goal

Enable inline terminal images across direct terminals, nested tmux sessions, and remote SSH connections without requiring manual configuration. Discover both terminal graphics protocol support and tmux passthrough support via an in-band stdio handshake at startup.

## Implementation Summary

Inline image rendering currently fails under tmux and across SSH connections for three reasons:

1. Static multiplexer block: `maki-ui/src/terminal_image.rs` returns `None` whenever `TMUX`, `STY`, or `ZELLIJ` is present in the environment.
2. Environment variable loss over SSH: OpenSSH does not forward `TMUX` or `TERM_PROGRAM` by default. `TERM` defaults to `xterm-256color`. Environment checks cannot determine whether the host terminal supports Kitty graphics or whether tmux is in the display pipeline.
3. Escape encapsulation requirements: tmux drops Kitty graphics escape sequences (`\x1b_G...`) unless they are wrapped in Device Control String (DCS) passthrough envelopes: `\x1bPtmux;\x1b<escapes with doubled ESC>\x1b\\`. `ratatui-image` only generates these wrappers when `is_tmux` is true on the `Picker`.

The solution executes an in-band dual-probe handshake during terminal initialization inside `maki-ui/src/terminal.rs`, right after `ratatui::init()` enters raw mode and alternate screen, before `InputReader::spawn()` starts reading input events. The handshake writes a batched probe to standard output and reads responses from standard input using non-blocking polling (`libc::poll`) on file descriptor 0 with a bounded 100 millisecond deadline. The result is cached in a process-wide `OnceLock<Option<DetectedGraphics>>` consumed by `terminal_image::picker()`.

Important files:
- `maki-ui/src/terminal_image.rs`: Defines probe sequences, stream parser, detected graphics state, and picker construction with `is_tmux`.
- `maki-ui/src/terminal.rs`: Invokes the handshake during `TerminalGuard::init()` while holding exclusive access to standard input and output.
- `maki-ui/src/components/messages/mod.rs`: Consumes the configured picker during transcript rendering.

Scope boundaries:
- Kitty graphics and tmux passthrough are auto-probed.
- Probing is the authoritative mechanism for multiplexer passthrough. `protocol_from_env` retains its multiplexer block so that failed probes under tmux never emit raw escape bytes.
- If passthrough is disabled or unsupported, the UI falls back cleanly to text placeholders without emitting escape bytes.
- Runtime calls to `terminal_image::picker()` are strictly read-only and perform no environment modifications or standard input reads.

## Implementation Plan

### Phase 1: Probe query and response parser (`maki-ui/src/terminal_image.rs`)

1. Define the graphics record:
   ```rust
   #[derive(Debug, Clone, Copy, PartialEq, Eq)]
   pub(crate) struct DetectedGraphics {
       pub protocol: ProtocolType,
       pub is_tmux: bool,
   }
   ```
2. Define the probe query strings:
   - Direct Kitty query (`i=31`) followed by direct sentinel: `\x1b_Gi=31,s=1,v=1,a=q,t=d,f=24;AAAA\x1b\\\x1b[5n`
   - DCS-wrapped Kitty query (`i=32`) followed by DCS-wrapped sentinel: `\x1bPtmux;\x1b\x1b_Gi=32,s=1,v=1,a=q,t=d,f=24;AAAA\x07\x1b\x1b[5n\x1b\\`
   Terminating the Kitty query inside DCS with BEL (`\x07`) ensures no embedded String Terminator (`\x1b\`) causes non-tmux terminals to prematurely terminate DCS mode.
3. Implement `parse_probe_stream(buffer: &[u8], in_tmux: bool) -> ProbeResult`:
   - If bytes contain terminated `Gi=32;` reply (terminated with ST `\x1b\` or BEL `\x07`), return `ProbeResult::Detected(DetectedGraphics { protocol: ProtocolType::Kitty, is_tmux: true })`.
   - If bytes contain terminated `Gi=31;` reply, return `ProbeResult::Detected(DetectedGraphics { protocol: ProtocolType::Kitty, is_tmux: in_tmux })`.
   - If direct DSR sentinel `\x1b[0n` is observed with no Kitty reply and `!in_tmux`, return `ProbeResult::Unsupported` to abort immediately and avoid startup delay on non-Kitty terminals.
   - If under tmux (`in_tmux`), an initial `\x1b[0n` is expected from local tmux; wait for outer terminal DCS response or deadline.
4. Implement non-blocking stdio handshake:
   - Implement `probe_terminal_graphics(timeout: Duration) -> Option<DetectedGraphics>`:
     - On Unix platforms (`#[cfg(unix)]`):
       - Verify both `std::io::stdout().is_terminal()` and `std::io::stdin().is_terminal()`. Return `None` if either is false.
       - Write the probe sequences to standard output and flush.
       - Poll `libc::STDIN_FILENO` in 50ms slices up to 350ms deadline.
       - Read incoming chunks into an accumulation buffer.
       - Check `parse_probe_stream`. If `Detected` or `Unsupported`, break immediately.
       - Run `drain_stdin()` before returning to flush any trailing echoes or sentinels so crossterm's `InputReader` starts with a clean buffer.
     - On non-Unix platforms (`#[cfg(not(unix))]`): return `None`.
5. Retain the static multiplexer check in `protocol_from_env`. The handshake is the sole authority for enabling graphics under multiplexers.

### Phase 2: Single-threaded initialization seam (`maki-ui/src/terminal.rs`)

1. Define global detected state:
   ```rust
   static DETECTED_GRAPHICS: OnceLock<Option<DetectedGraphics>> = OnceLock::new();
   ```
2. In `TerminalGuard::init()`:
   - After `ratatui::init()` enables raw mode and alternate screen, call `terminal_image::init_graphics_detection()`.
   - If `DetectedGraphics { is_tmux: true, .. }` is detected, set `unsafe { std::env::set_var("TERM_PROGRAM", "tmux"); }`. Because this executes during single-threaded startup prior to spawning worker threads or the event loop, setting the environment variable is safe and allows `ratatui_image::Picker::from_fontsize` to initialize `is_tmux = true`.
   - Store the outcome in `DETECTED_GRAPHICS`.
   - Because `InputReader::spawn()` has not run, no background thread contends for standard input.

### Phase 3: Picker construction and testable decomposition (`maki-ui/src/terminal_image.rs`)

1. Decompose picker construction into a testable pure helper:
   ```rust
   fn create_picker(
       detected: Option<DetectedGraphics>,
       inline_images: bool,
       font: FontSize,
       env_fallback: impl Fn(&str) -> Option<String>,
   ) -> Option<Picker>
   ```
2. In `create_picker`:
   - Return `None` if `inline_images` is false.
   - If `detected` is `Some(detected)`:
     - Construct `picker = Picker::from_fontsize(font)`.
     - Set protocol to `detected.protocol` via `picker.set_protocol_type(...)`.
     - Return `Some(picker)`.
   - If `detected` is `None`:
     - Resolve protocol via `protocol_from_env(inline_images, env_fallback)`.
     - Return `None` if no protocol resolved.
     - Construct `picker = Picker::from_fontsize(font)` and set protocol.
3. In `terminal_image::picker(inline_images: bool)`:
   - If `!stdout().is_terminal()`, return `None`.
   - Compute font size from `crossterm::terminal::window_size()`.
   - Call `create_picker` passing `DETECTED_GRAPHICS.get().copied().flatten()`, font size, and environment lookup.
4. Runtime calls to `terminal_image::picker()` are strictly read-only and never mutate environment variables or access standard input.

## Acceptance Criteria

- AC.1: When standard input receives `\x1b_Gi=31;OK\x1b\` followed by `\x1b[0n`, `parse_probe_stream` returns `Some(DetectedGraphics { protocol: ProtocolType::Kitty, is_tmux: false })`.
- AC.2: When standard input receives `\x1b_Gi=32;OK\x1b\` followed by `\x1b[0n`, `parse_probe_stream` returns `Some(DetectedGraphics { protocol: ProtocolType::Kitty, is_tmux: true })`.
- AC.3: When standard input receives an isolated `\x1b[0n` (tmux local DSR response) before Ghostty's response, the parser does not terminate with `None` prematurely, and correctly resolves `_Gi=32;OK` once received.
- AC.4: When standard input receives only `\x1b[0n` and the deadline expires, `parse_probe_stream` returns `None`.
- AC.5: When standard input receives unrecognized DCS sequences before `\x1b_Gi=31;OK\x1b\`, the parser ignores the unknown DCS sequence and correctly identifies `ProtocolType::Kitty`.
- AC.6: The existing multiplexer block in `protocol_from_env` remains intact, ensuring that failed multiplexer probes do not fall back to emitting raw Kitty escapes.
- AC.7: `create_picker` constructs a `Picker` with the specified protocol when `DetectedGraphics` is provided, independent of `stdout().is_terminal()`.
- AC.8: When an image is encoded using a `Picker` where `is_tmux` is true, the resulting protocol payload contains the DCS passthrough wrapper `\x1bPtmux;`.
- AC.9: When `terminal_image::picker()` returns `None`, `InlineImage::height()` and `InlineImage::fallback()` return the text placeholder (`IMAGE_PLACEHOLDER`) without generating image protocol bytes.

## Test Strategy

### Test Mapping Table

| Acceptance Criterion | Verification / Named Test Case | File Location | Test Layer |
|---|---|---|---|
| AC.1 | `test_parse_probe_direct_kitty` | `maki-ui/src/terminal_image.rs` | Pure logic unit test |
| AC.2 | `test_parse_probe_tmux_kitty` | `maki-ui/src/terminal_image.rs` | Pure logic unit test |
| AC.3 | `test_parse_probe_delayed_dcs_response_after_local_dsr` | `maki-ui/src/terminal_image.rs` | Pure logic unit test |
| AC.4 | `test_parse_probe_unsupported_terminal_deadline` | `maki-ui/src/terminal_image.rs` | Pure logic unit test |
| AC.5 | `test_parse_probe_unknown_dcs_ignored` | `maki-ui/src/terminal_image.rs` | Pure logic unit test |
| AC.6 | `protocol_env_uses_safe_fallback` (existing test case `multiplexer`) | `maki-ui/src/terminal_image.rs` | Existing regression test |
| AC.7 | `test_create_picker_from_detected_graphics` | `maki-ui/src/terminal_image.rs` | Pure logic unit test |
| AC.8 | `test_inline_image_encode_emits_tmux_dcs_wrapper` | `maki-ui/src/terminal_image.rs` | Unit test |
| AC.9 | `test_disabled_or_unsupported_picker_falls_back_to_placeholder` | `maki-ui/src/terminal_image.rs` | Unit test |

### Verification Protocol

1. Local unit testing and linting:
   ```bash
   cargo check -p maki-ui --tests
   cargo test -p maki-ui
   just fmt-check
   ```
2. Remote workspace test suite:
   ```bash
   .ssh/remote-ci.sh
   ```

## Review Strategy

The review follows a two-stage process:

1. Plan review: An autonomous subagent audits this document against the plan specification rubric (structural compliance, test coverage for every acceptance criterion, replace-vs-edit evaluation, and risk mitigation). Any findings are corrected before execution starts.
2. Implementation review: Following local implementation and a passing remote CI run, a reviewer audits `git diff` against the acceptance criteria, verifying that no fork features regressed, no data races exist around standard input, and all test cases pass.

## Documentation Strategy

1. Code documentation: Document the dual-probe sequence rationale, DCS wrapping requirements, and polling semantics in `maki-ui/src/terminal_image.rs`.
2. Architecture memory: Record the tmux passthrough probe mechanism in project memory under tags `maki_ui` and `gotcha`.
3. User documentation: Update `site/docs/content/configuration/_index.md` under `ui.inline_images` to state that Kitty graphics and tmux passthrough are detected automatically without manual multiplexer flags.

## Risks, Blockers, and Required Decisions

### Risks

1. Stdin contention with input reader: If the probe executes after `InputReader::spawn()`, crossterm and the probe compete for input bytes.
   - Mitigation: Execute the probe strictly inside `TerminalGuard::init()` before `InputReader::spawn()` is invoked.
2. Indefinite hang on non-responsive terminals: Standard library `stdin().read()` blocks indefinitely if the terminal does not answer.
   - Mitigation: Use `libc::poll` with a bounded deadline (100 milliseconds) before every read attempt. Do not spawn orphaned background threads for reading.
3. Stdin desynchronization under tmux over SSH: Local tmux answers an unwrapped DSR (`\x1b[5n`) in under 1 millisecond before an in-flight DCS probe response reaches Ghostty over SSH.
   - Mitigation: Enclose the sentinel inside the DCS envelope (`\x1bPtmux;\x1b\x1b[5n\x1b\\`). Ghostty executes the probe and sentinel in order, guaranteeing that `_Gi=32;OK` precedes the sentinel. Continue polling until deadline or match rather than terminating on isolated `\x1b[0n`.
4. Race condition on `std::env::set_var`: Setting environment variables is unsafe in multi-threaded contexts.
   - Mitigation: Set `TERM_PROGRAM=tmux` strictly once during single-threaded startup in `TerminalGuard::init()`. Runtime calls to `terminal_image::picker()` are strictly read-only.

### Blockers and Required Decisions

None. The user confirmed tmux `allow-passthrough on` is enabled on the host terminal and requested automatic detection over SSH without configuration flags.
