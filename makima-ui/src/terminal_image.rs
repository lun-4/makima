use std::env;
#[cfg(unix)]
use std::io::Write;
use std::io::{Cursor, IsTerminal, stdout};
#[cfg(unix)]
use std::os::fd::RawFd;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;

use base64::{Engine, engine::general_purpose::STANDARD};
use color_eyre::eyre::{Result, ensure};
use image::{DynamicImage, ImageDecoder, ImageReader, Limits};
use makima_providers::ImageSource;
use ratatui::layout::Size;
use ratatui_image::{
    FontSize,
    picker::{Capability, Picker, ProtocolType},
    sliced::SlicedProtocol,
};

use crate::image::{MAX_IMAGE_BYTES, MAX_IMAGE_PIXELS};
use crate::repaint::Dirty;
use crate::terminal_da;
#[cfg(any(unix, test))]
use crate::terminal_da::Da1Feature;

const MAX_IMAGE_ROWS: u16 = 20;
const FALLBACK_FONT_SIZE: FontSize = FontSize::new(10, 20);
const DECODE_THREAD: &str = "inline-image";
const PROBE_TIMEOUT: Duration = Duration::from_millis(350);
#[cfg(unix)]
const PROBE_POLL_SLICE: Duration = Duration::from_millis(50);
/// Replies arriving after the probe stops reading become keypresses in the
/// input box, hence this quiet window.
#[cfg(unix)]
const PROBE_QUIET: Duration = Duration::from_millis(20);
#[cfg(unix)]
const PROBE_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(unix)]
const MAX_PROBE_BYTES: usize = 64 * 1024;
#[cfg(unix)]
const PROBE_KITTY_DIRECT: &[u8] = b"\x1b_Gi=31,s=1,v=1,a=q,t=d,f=24;AAAA\x1b\\";
#[cfg(unix)]
const PROBE_SENTINELS: &[u8] = b"\x1b[>c\x1b[c\x1b[5n";
/// Ghostty resumes escape parsing inside an ignored DCS, so the doubled ESC
/// makes it answer this wrapper even without tmux. The probe sends it only
/// after tmux detection.
#[cfg(unix)]
const PROBE_KITTY_TMUX: &[u8] =
    b"\x1bPtmux;\x1b\x1b_Gi=32,s=1,v=1,a=q,t=d,f=24;AAAA\x1b\x1b\\\x1b\x1b[5n\x1b\\";
#[cfg(any(unix, test))]
const DSR_RESPONSE: &[u8] = b"\x1b[0n";
#[cfg(any(unix, test))]
const TMUX_DA2_REPLY: &[u8] = b">84;";
#[cfg(any(unix, test))]
const KITTY_PROBE_TMUX_REPLY: &[u8] = b"Gi=32;";
#[cfg(any(unix, test))]
const KITTY_PROBE_DIRECT_REPLY: &[u8] = b"Gi=31;";
#[cfg(any(unix, test))]
const BEL_BYTE: u8 = 0x07;
#[cfg(any(unix, test))]
const ST_BYTES: &[u8] = b"\x1b\\";
static GENERATION: AtomicU64 = AtomicU64::new(0);
static DECODE_JOBS: OnceLock<flume::Sender<DecodeJob>> = OnceLock::new();
static DETECTED_GRAPHICS: OnceLock<Option<DetectedGraphics>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DetectedGraphics {
    pub protocol: ProtocolType,
    pub is_tmux: bool,
}

#[cfg(any(unix, test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProbeResult {
    Detected(DetectedGraphics),
    Unsupported,
    Pending,
}

type DecodeJob = Box<dyn FnOnce() + Send + 'static>;

/// Decoding gets its own thread rather than `makima_highlight::pool`: that one is
/// single threaded to bound syntect's regex cache, and a multi-megabyte decode
/// queued ahead of the transcript's highlight jobs stalls the whole UI.
fn decode_jobs() -> &'static flume::Sender<DecodeJob> {
    DECODE_JOBS.get_or_init(|| {
        let (tx, rx) = flume::unbounded::<DecodeJob>();
        thread::Builder::new()
            .name(DECODE_THREAD.into())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    if catch_unwind(AssertUnwindSafe(job)).is_err() {
                        tracing::error!("inline image decode panicked");
                    }
                }
            })
            .expect("failed to spawn the inline image thread");
        tx
    })
}

pub(crate) fn invalidate() {
    GENERATION.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn generation() -> u64 {
    GENERATION.load(Ordering::Relaxed)
}

#[cfg(any(unix, test))]
fn contains_subslice(buffer: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || buffer.len() < needle.len() {
        return false;
    }
    buffer.windows(needle.len()).any(|window| window == needle)
}

#[cfg(any(unix, test))]
fn count_subslice(buffer: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() || buffer.len() < needle.len() {
        return 0;
    }
    buffer
        .windows(needle.len())
        .filter(|window| *window == needle)
        .count()
}

#[cfg(any(unix, test))]
fn find_terminated_reply(buffer: &[u8], prefix: &[u8]) -> Option<(bool, bool)> {
    if prefix.is_empty() || buffer.len() < prefix.len() {
        return None;
    }
    let pos = buffer.windows(prefix.len()).position(|w| w == prefix)?;
    let rest = &buffer[pos + prefix.len()..];
    let is_terminated =
        rest.contains(&BEL_BYTE) || rest.windows(ST_BYTES.len()).any(|w| w == ST_BYTES);
    let has_sentinel = contains_subslice(rest, DSR_RESPONSE);
    Some((is_terminated, has_sentinel))
}

#[cfg(any(unix, test))]
fn is_tmux_detected(buffer: &[u8], env_tmux: bool) -> bool {
    env_tmux || contains_subslice(buffer, TMUX_DA2_REPLY)
}

#[cfg(any(unix, test))]
pub(crate) fn parse_probe_stream(buffer: &[u8], env_tmux: bool) -> ProbeResult {
    let in_tmux = is_tmux_detected(buffer, env_tmux);

    if let Some((terminated, has_sentinel)) =
        find_terminated_reply(buffer, KITTY_PROBE_DIRECT_REPLY)
    {
        return if terminated && has_sentinel {
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: in_tmux,
            })
        } else {
            ProbeResult::Pending
        };
    }

    if in_tmux
        && let Some((terminated, has_sentinel)) =
            find_terminated_reply(buffer, KITTY_PROBE_TMUX_REPLY)
    {
        return if terminated && has_sentinel {
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        } else {
            ProbeResult::Pending
        };
    }

    let sixel_sentinel = Da1Feature::Sixel
        .response_tail(buffer)
        .is_some_and(|tail| contains_subslice(tail, DSR_RESPONSE));
    let dsr_count = count_subslice(buffer, DSR_RESPONSE);
    if sixel_sentinel && (!in_tmux || dsr_count >= 2) {
        return ProbeResult::Detected(DetectedGraphics {
            protocol: ProtocolType::Sixel,
            is_tmux: in_tmux,
        });
    }

    if (!in_tmux && dsr_count > 0) || (in_tmux && dsr_count >= 2) {
        ProbeResult::Unsupported
    } else {
        ProbeResult::Pending
    }
}

#[cfg(unix)]
fn ensure_tmux_passthrough() {
    if env::var_os("TMUX").is_some() {
        let _ = std::process::Command::new("tmux")
            .args(["set", "-p", "allow-passthrough", "on"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .and_then(|mut child| child.wait());
    }
}

#[cfg(unix)]
fn write_probe(bytes: &[u8]) -> Option<()> {
    let mut out = stdout().lock();
    out.write_all(bytes).ok()?;
    out.flush().ok()
}

#[cfg(unix)]
fn drain_stdin(fd: RawFd, deadline: Instant, buffer: &mut Vec<u8>) {
    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        if !wait_readable(fd, remaining.min(PROBE_QUIET)) {
            break;
        }
        let mut discard = [0u8; 256];
        let n = unsafe { libc::read(fd, discard.as_mut_ptr().cast(), discard.len()) };
        if n <= 0 {
            break;
        }
        let retained = (n as usize).min(MAX_PROBE_BYTES.saturating_sub(buffer.len()));
        buffer.extend_from_slice(&discard[..retained]);
    }
}

#[cfg(unix)]
pub(crate) fn probe_terminal_graphics(timeout: Duration) -> Option<DetectedGraphics> {
    if !stdout().is_terminal() || !std::io::stdin().is_terminal() {
        return None;
    }
    let env_tmux = env::var_os("TMUX").is_some();
    let mut tmux_query_sent = env_tmux;
    if env_tmux {
        write_probe(PROBE_SENTINELS)?;
        write_probe(PROBE_KITTY_TMUX)?;
    } else {
        write_probe(PROBE_KITTY_DIRECT)?;
        write_probe(PROBE_SENTINELS)?;
    }

    let deadline = Instant::now() + timeout;
    let mut buf = Vec::with_capacity(256);
    let mut detected = None;

    while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
        let poll_timeout = remaining.min(PROBE_POLL_SLICE);
        if wait_readable(libc::STDIN_FILENO, poll_timeout) {
            let mut chunk = [0u8; 256];
            let n =
                unsafe { libc::read(libc::STDIN_FILENO, chunk.as_mut_ptr().cast(), chunk.len()) };
            if n > 0 {
                let retained = (n as usize).min(MAX_PROBE_BYTES.saturating_sub(buf.len()));
                buf.extend_from_slice(&chunk[..retained]);
                if buf.len() == MAX_PROBE_BYTES {
                    break;
                }
                if !tmux_query_sent && is_tmux_detected(&buf, env_tmux) {
                    tmux_query_sent = write_probe(PROBE_KITTY_TMUX).is_some();
                }
                match parse_probe_stream(&buf, env_tmux) {
                    ProbeResult::Detected(graphics) => {
                        detected = Some(graphics);
                        break;
                    }
                    ProbeResult::Unsupported => {
                        break;
                    }
                    ProbeResult::Pending => {}
                }
            } else if n == 0 {
                break;
            }
        }
    }

    drain_stdin(
        libc::STDIN_FILENO,
        Instant::now() + PROBE_DRAIN_TIMEOUT,
        &mut buf,
    );
    terminal_da::cache_features(&buf);
    detected
}

#[cfg(not(unix))]
pub(crate) fn probe_terminal_graphics(_timeout: Duration) -> Option<DetectedGraphics> {
    terminal_da::cache_features(&[]);
    None
}

#[cfg(unix)]
fn wait_readable(fd: RawFd, timeout: Duration) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    unsafe { libc::poll(&mut pfd, 1, ms) > 0 && pfd.revents & libc::POLLIN != 0 }
}

pub(crate) fn init_graphics_detection() {
    #[cfg(unix)]
    ensure_tmux_passthrough();

    let detected = probe_terminal_graphics(PROBE_TIMEOUT);
    if let Some(detected) = detected {
        tracing::info!(
            protocol = ?detected.protocol,
            is_tmux = detected.is_tmux,
            "terminal graphics protocol detected"
        );
    } else {
        tracing::warn!("terminal graphics protocol handshake timed out or unsupported");
    }
    let _ = DETECTED_GRAPHICS.set(detected);
}

pub(crate) fn detected_graphics() -> Option<DetectedGraphics> {
    DETECTED_GRAPHICS.get().copied().flatten()
}

pub(crate) fn picker(inline_images: bool) -> Option<Picker> {
    if !stdout().is_terminal() {
        return None;
    }
    let font = crossterm::terminal::window_size()
        .ok()
        .filter(|size| size.columns > 0 && size.rows > 0)
        .map(|size| FontSize::new(size.width / size.columns, size.height / size.rows))
        .filter(|font| font.width > 0 && font.height > 0)
        .unwrap_or(FALLBACK_FONT_SIZE);
    create_picker(
        DETECTED_GRAPHICS.get().copied().flatten(),
        inline_images,
        font,
        |key| env::var(key).ok(),
    )
}

fn create_picker(
    detected: Option<DetectedGraphics>,
    inline_images: bool,
    font: FontSize,
    env_fallback: impl Fn(&str) -> Option<String>,
) -> Option<Picker> {
    if !inline_images {
        return None;
    }
    let (protocol, is_tmux) = if let Some(detected) = detected {
        (detected.protocol, detected.is_tmux)
    } else {
        let is_tmux = env_fallback("TMUX").is_some()
            || env_fallback("TERM").is_some_and(|t| t.starts_with("tmux"))
            || env_fallback("TERM_PROGRAM").is_some_and(|p| p == "tmux");
        (protocol_from_env(inline_images, env_fallback)?, is_tmux)
    };
    #[allow(deprecated)]
    let mut picker = Picker::from_fontsize(font);
    picker.set_protocol_type(protocol);
    picker.set_is_tmux(is_tmux);
    if protocol == ProtocolType::Kitty {
        picker.add_capability(Capability::KittyCompression);
    }
    Some(picker)
}

/// An allowlist rather than trusting what the terminal advertises: one that
/// claims graphics it does not have paints escape bytes all over the UI. When
/// the guess is wrong anyway, `ui.inline_images = false` is the way out. An
/// unrecognized `TERM_PROGRAM` (tmux and some shell configs set it) falls
/// through to `TERM`, which kitty sets itself and forwards over ssh.
fn protocol_from_env(
    inline_images: bool,
    get: impl Fn(&str) -> Option<String>,
) -> Option<ProtocolType> {
    if !inline_images {
        return None;
    }
    if ["TMUX", "STY", "ZELLIJ"]
        .iter()
        .any(|key| get(key).is_some())
    {
        return None;
    }
    match get("TERM_PROGRAM").as_deref() {
        Some("ghostty" | "kitty") => return Some(ProtocolType::Kitty),
        Some("iTerm.app" | "WezTerm") => return Some(ProtocolType::Iterm2),
        Some("foot" | "mlterm") => return Some(ProtocolType::Sixel),
        _ => {}
    }
    match get("TERM").as_deref() {
        Some("xterm-kitty" | "xterm-ghostty") => Some(ProtocolType::Kitty),
        Some(t) if t.contains("sixel") || t.starts_with("foot") || t.starts_with("mlterm") => {
            Some(ProtocolType::Sixel)
        }
        _ => None,
    }
}

enum ImageState {
    Idle {
        height: u16,
    },
    Pending {
        width: u16,
        height: u16,
        result: flume::Receiver<Result<SlicedProtocol>>,
    },
    Ready {
        width: u16,
        protocol: SlicedProtocol,
    },
    Failed,
}

pub(crate) struct InlineImage {
    source: ImageSource,
    state: ImageState,
    /// The line a terminal without graphics gets instead of the pixels, or
    /// `None` where the text around the image already names it. Both the row
    /// count and the row itself read this, so layout and paint cannot disagree.
    fallback: Option<&'static str>,
}

impl InlineImage {
    pub fn new(source: ImageSource, fallback: Option<&'static str>) -> Self {
        Self {
            source,
            state: ImageState::Idle { height: 0 },
            fallback,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_prepared(source: ImageSource, picker: &Picker, width: u16) -> Result<Self> {
        let protocol = encode(&source, picker, width)?;
        Ok(Self {
            source,
            state: ImageState::Ready { width, protocol },
            fallback: None,
        })
    }

    pub fn source(&self) -> &ImageSource {
        &self.source
    }

    pub fn fallback(&self) -> Option<&'static str> {
        self.fallback
    }

    pub fn height(&self) -> u16 {
        let pixels = match &self.state {
            ImageState::Ready { protocol, .. } => protocol.size().height,
            ImageState::Pending { height, .. } | ImageState::Idle { height } => *height,
            ImageState::Failed => 0,
        };
        pixels.max(u16::from(self.fallback.is_some()))
    }

    pub fn release(&mut self) {
        if !matches!(self.state, ImageState::Failed) {
            self.state = ImageState::Idle {
                height: self.height(),
            };
        }
    }

    pub fn prepare(&mut self, picker: &Picker, width: u16) {
        if width == 0 {
            return;
        }
        match &self.state {
            ImageState::Pending { .. } | ImageState::Failed => return,
            ImageState::Ready {
                width: encoded_width,
                ..
            } if *encoded_width == width => return,
            _ => {}
        }
        let (tx, result) = flume::bounded(1);
        let source = self.source.clone();
        let picker = picker.clone();
        self.state = ImageState::Pending {
            width,
            height: self.height(),
            result,
        };
        let _ = decode_jobs().send(Box::new(move || {
            if !tx.is_disconnected() {
                let _ = tx.send(encode(&source, &picker, width));
            }
        }));
    }

    pub fn poll(&mut self) -> Dirty {
        let ImageState::Pending { width, result, .. } = &self.state else {
            return Dirty::NO;
        };
        let result = match result.try_recv() {
            Ok(result) => result,
            Err(flume::TryRecvError::Empty) => return Dirty::NO,
            Err(flume::TryRecvError::Disconnected) => {
                self.state = ImageState::Failed;
                return Dirty::YES;
            }
        };
        self.state = match result {
            Ok(protocol) => ImageState::Ready {
                width: *width,
                protocol,
            },
            Err(error) => {
                tracing::warn!(%error, "inline image unavailable");
                ImageState::Failed
            }
        };
        Dirty::YES
    }

    pub fn protocol(&self, width: u16) -> Option<&SlicedProtocol> {
        match &self.state {
            ImageState::Ready {
                width: encoded_width,
                protocol,
            } if *encoded_width == width => Some(protocol),
            _ => None,
        }
    }
}

fn encode(source: &ImageSource, picker: &Picker, width: u16) -> Result<SlicedProtocol> {
    ensure!(
        source.data.len() <= MAX_IMAGE_BYTES.div_ceil(3) * 4,
        "Image exceeds 20MB limit"
    );
    let bytes = STANDARD.decode(source.data.as_ref())?;
    let mut reader = ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
    let mut limits = Limits::default();
    limits.max_alloc = Some((MAX_IMAGE_PIXELS * 4) as u64);
    reader.limits(limits);
    let decoder = reader.into_decoder()?;
    let (w, h) = decoder.dimensions();
    ensure!(
        u64::from(w) * u64::from(h) <= MAX_IMAGE_PIXELS as u64,
        "Image exceeds pixel limit"
    );
    let image = DynamicImage::from_decoder(decoder)?;
    let font = picker.font_size();
    let size = Size::new(
        width.min(u16::MAX / font.width),
        MAX_IMAGE_ROWS.min(u16::MAX / font.height),
    );
    Ok(SlicedProtocol::new(picker, image, Some(size))?)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use base64::{Engine, engine::general_purpose::STANDARD};
    use color_eyre::eyre::Result;
    use image::{DynamicImage, ImageFormat};
    use makima_providers::{IMAGE_PLACEHOLDER, ImageMediaType, ImageSource};
    use ratatui::layout::Size;
    use ratatui_image::{
        FontSize,
        picker::{Picker, ProtocolType},
    };
    use test_case::test_case;

    use super::{
        DetectedGraphics, FALLBACK_FONT_SIZE, ImageState, InlineImage, ProbeResult, create_picker,
        encode, parse_probe_stream, protocol_from_env,
    };
    use crate::repaint::Dirty;
    use ratatui::buffer::Buffer;
    use ratatui::layout::Rect;
    use ratatui::widgets::Widget;
    use ratatui_image::sliced::{SignedPosition, SlicedImage};

    const EXPECTED_IDLE: &str = "nothing to encode with, so the image must stay idle";
    const EXPECTED_PENDING: &str = "image should be pending after prepare";
    const HEIGHT_KEPT: &str = "a released image keeps its height so the scroll does not jump";
    const MALFORMED_IMAGE: &[u8] = b"not an image";
    const IMAGE_WIDTH: u16 = 4;
    const IMAGE_HEIGHT: u16 = 6;
    const TERM_PROGRAM: &str = "TERM_PROGRAM";
    const TERM: &str = "TERM";
    const KITTY_PROGRAM: &str = "kitty";
    const KITTY_TERM: &str = "xterm-kitty";
    const UNKNOWN_PROGRAM: &str = "unknown";
    const TMUX_DCS_PREFIX: &str = "\x1bPtmux;";
    const DIRECT_PROBE_RESPONSE: &[u8] = b"\x1b_Gi=31;OK\x1b\\\x1b[>0;1;0c\x1b[0n";
    const TMUX_PROBE_RESPONSE: &[u8] = b"\x1b[>84;0;0c\x1b[0n\x1b_Gi=32;OK\x1b\\\x1b[0n";
    const TMUX_PROBE_RESPONSE_NO_UNDERSCORE: &[u8] = b"\x1b[>84;0;0c\x1b[0nGi=32;OK\x1b\\\x1b[0n";
    const DIRECT_DSR_RESPONSE: &[u8] = b"\x1b[>0;1;0c\x1b[0n";
    const TMUX_LOCAL_DSR_RESPONSE: &[u8] = b"\x1b[>84;0;0c\x1b[0n";
    const GHOSTTY_KITTY_RESPONSE: &[u8] = b"\x1b_Gi=32;OK\x1b\\\x1b[0n";
    const SIXEL_PROBE_RESPONSE: &[u8] = b"\x1b[?62;4;6c\x1b[0n";
    const TMUX_SIXEL_PROBE_RESPONSE: &[u8] = b"\x1b[>84;0;0c\x1b[0n\x1b[?62;4;6c\x1b[0n";
    const UNKNOWN_DCS_STREAM: &[u8] = b"\x1bPsomething_unknown\x1b\\\x1b_Gi=31;OK\x1b\\\x1b[0n";
    const BEL_TERMINATED_RESPONSE: &[u8] = b"\x1b_Gi=31;OK\x07\x1b[0n";
    const INCOMPLETE_KITTY_RESPONSE: &[u8] = b"\x1b_Gi=31;O";

    fn protocol(inline_images: bool, env: &[(&str, &str)]) -> Option<ProtocolType> {
        protocol_from_env(inline_images, |key| {
            env.iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).into())
        })
    }

    #[test_case(&[(TERM_PROGRAM, "iTerm.app")], Some(ProtocolType::Iterm2); "known_terminal")]
    #[test_case(&[(TERM, KITTY_TERM)], Some(ProtocolType::Kitty); "known_term")]
    #[test_case(&[(TERM_PROGRAM, "foot")], Some(ProtocolType::Sixel); "foot_terminal")]
    #[test_case(&[(TERM, "xterm-sixel")], Some(ProtocolType::Sixel); "sixel_term")]
    #[test_case(&[(TERM_PROGRAM, UNKNOWN_PROGRAM), (TERM, KITTY_TERM)], Some(ProtocolType::Kitty); "unsupported_program_falls_back_to_term")]
    #[test_case(&[(TERM_PROGRAM, UNKNOWN_PROGRAM), (TERM, "xterm-256color")], None; "unsupported_program_and_term")]
    #[test_case(&[], None; "missing_env")]
    #[test_case(&[(TERM_PROGRAM, KITTY_PROGRAM), ("TMUX", "mux")], None; "multiplexer")]
    fn protocol_env_uses_safe_fallback(env: &[(&str, &str)], expected: Option<ProtocolType>) {
        assert_eq!(protocol(true, env), expected);
    }

    #[test_case(&[(TERM_PROGRAM, KITTY_PROGRAM)]; "term_program")]
    #[test_case(&[(TERM, KITTY_TERM)]; "term")]
    fn disabled_inline_images_yields_no_protocol(env: &[(&str, &str)]) {
        assert_eq!(protocol(false, env), None);
    }

    #[test_case(false; "valid_png")]
    #[test_case(true; "malformed_image")]
    fn decode_result_drives_the_state_machine(malformed: bool) -> Result<()> {
        let mut png = Cursor::new(Vec::new());
        DynamicImage::new_rgb8(IMAGE_WIDTH.into(), IMAGE_HEIGHT.into())
            .write_to(&mut png, ImageFormat::Png)?;
        let source = ImageSource::new(
            ImageMediaType::Png,
            STANDARD
                .encode(if malformed {
                    MALFORMED_IMAGE
                } else {
                    png.get_ref()
                })
                .into(),
        );
        #[allow(deprecated)]
        let mut picker = Picker::from_fontsize(FontSize::new(1, 1));
        picker.set_protocol_type(ProtocolType::Halfblocks);
        let mut image = InlineImage::new(source.clone(), Some(IMAGE_PLACEHOLDER));
        image.prepare(&picker, 0);
        assert!(
            matches!(image.state, ImageState::Idle { .. }),
            "{EXPECTED_IDLE}"
        );
        assert_eq!(image.height(), 1);
        image.prepare(&picker, IMAGE_WIDTH);
        assert!(
            matches!(image.state, ImageState::Pending { .. }),
            "{EXPECTED_PENDING}"
        );

        // Encode here and hand the answer over instead of waiting on the real
        // decode thread, which is how this test would start failing at 3am on a
        // loaded machine.
        let (tx, result) = flume::bounded(1);
        tx.send(encode(&source, &picker, IMAGE_WIDTH))?;
        image.state = ImageState::Pending {
            width: IMAGE_WIDTH,
            height: 1,
            result,
        };
        assert_eq!(image.poll(), Dirty::YES);
        assert_eq!(image.poll(), Dirty::NO);
        if malformed {
            assert!(matches!(image.state, ImageState::Failed));
            assert_eq!(image.height(), 1);
            assert!(image.protocol(IMAGE_WIDTH).is_none());
            return Ok(());
        }
        assert_eq!(image.height(), IMAGE_HEIGHT);
        assert_eq!(
            image.protocol(IMAGE_WIDTH).map(|protocol| protocol.size()),
            Some(Size::new(IMAGE_WIDTH, IMAGE_HEIGHT))
        );
        image.release();
        assert_eq!(image.height(), IMAGE_HEIGHT, "{HEIGHT_KEPT}");
        assert!(image.protocol(IMAGE_WIDTH).is_none());
        image.prepare(&picker, IMAGE_WIDTH);
        assert!(
            matches!(image.state, ImageState::Pending { .. }),
            "{EXPECTED_PENDING}"
        );
        Ok(())
    }

    #[cfg(unix)]
    mod drain {
        use std::fs::File;
        use std::io::Write;
        use std::os::fd::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::mpsc;
        use std::thread;
        use std::time::{Duration, Instant};

        use color_eyre::eyre::Result;
        use test_case::test_case;

        use super::super::{PROBE_DRAIN_TIMEOUT, drain_stdin, wait_readable};
        use crate::terminal_da::Da1Feature;

        const CONTINUOUS_INPUT_PATH: &str = "/dev/zero";
        const HANG_GUARD: Duration = Duration::from_secs(5);
        const PENDING_REPLY: &[u8] = b"\x1b[0n";

        #[test_case(Duration::ZERO; "expired_deadline")]
        #[test_case(PROBE_DRAIN_TIMEOUT; "deadline_during_continuous_input")]
        fn continuous_input_cannot_extend_deadline(budget: Duration) -> Result<()> {
            let input = File::open(CONTINUOUS_INPUT_PATH)?;
            let (done_tx, done_rx) = mpsc::channel();
            let worker = thread::spawn(move || {
                drain_stdin(input.as_raw_fd(), Instant::now() + budget, &mut Vec::new());
                done_tx.send(()).unwrap();
            });

            done_rx.recv_timeout(HANG_GUARD)?;
            worker.join().unwrap();
            Ok(())
        }

        #[test_case(b"", b"\x1b[?62;52c"; "late_complete_reply")]
        #[test_case(b"\x1b[?62;", b"52c"; "split_reply")]
        fn clipboard_reply_survives_drain(prefix: &[u8], suffix: &[u8]) -> Result<()> {
            let (input, mut output) = UnixStream::pair()?;
            output.write_all(suffix)?;
            let mut buffer = prefix.to_vec();
            drain_stdin(input.as_raw_fd(), Instant::now() + HANG_GUARD, &mut buffer);
            assert!(Da1Feature::Osc52.response_tail(&buffer).is_some());
            Ok(())
        }

        #[test_case(false; "drains_pending_reply")]
        #[test_case(true; "expired_deadline_preserves_input")]
        fn pending_input_respects_deadline(expired: bool) -> Result<()> {
            let (input, mut output) = UnixStream::pair()?;
            output.write_all(PENDING_REPLY)?;
            assert!(wait_readable(input.as_raw_fd(), HANG_GUARD));
            let deadline = Instant::now() + if expired { Duration::ZERO } else { HANG_GUARD };

            let mut buffer = Vec::new();
            drain_stdin(input.as_raw_fd(), deadline, &mut buffer);
            assert_eq!(buffer, if expired { b"" } else { PENDING_REPLY });

            assert_eq!(wait_readable(input.as_raw_fd(), Duration::ZERO), expired);
            Ok(())
        }
    }

    #[test]
    fn test_parse_probe_direct_kitty() {
        assert_eq!(
            parse_probe_stream(DIRECT_PROBE_RESPONSE, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: false,
            })
        );
    }

    #[test]
    fn test_parse_probe_direct_reply_wins_over_wrapped() {
        let mut stream = DIRECT_PROBE_RESPONSE.to_vec();
        stream.extend_from_slice(GHOSTTY_KITTY_RESPONSE);
        assert_eq!(
            parse_probe_stream(&stream, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: false,
            })
        );
    }

    #[test]
    fn test_parse_probe_wrapped_reply_needs_tmux() {
        assert_eq!(
            parse_probe_stream(GHOSTTY_KITTY_RESPONSE, false),
            ProbeResult::Unsupported
        );
        assert_eq!(
            parse_probe_stream(GHOSTTY_KITTY_RESPONSE, true),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_tmux_kitty() {
        assert_eq!(
            parse_probe_stream(TMUX_PROBE_RESPONSE, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_tmux_kitty_waits_for_trailing_sentinel() {
        let stream_without_trailing_dsr = b"\x1b[>84;0;0c\x1b[0n\x1b_Gi=32;OK\x1b\\";
        assert_eq!(
            parse_probe_stream(stream_without_trailing_dsr, false),
            ProbeResult::Pending,
        );

        let stream_with_trailing_dsr = b"\x1b[>84;0;0c\x1b[0n\x1b_Gi=32;OK\x1b\\\x1b[0n";
        assert_eq!(
            parse_probe_stream(stream_with_trailing_dsr, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_tmux_kitty_without_leading_underscore() {
        assert_eq!(
            parse_probe_stream(TMUX_PROBE_RESPONSE_NO_UNDERSCORE, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_delayed_dcs_response_after_local_dsr() {
        let mut stream = TMUX_LOCAL_DSR_RESPONSE.to_vec();
        assert_eq!(parse_probe_stream(&stream, false), ProbeResult::Pending);
        stream.extend_from_slice(GHOSTTY_KITTY_RESPONSE);
        assert_eq!(
            parse_probe_stream(&stream, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_tmux_kitty_not_preempted_by_sixel_da1() {
        // Real tmux DA2 + DA1 with Sixel (1;2;4c) + local DSR (0n)
        let mut stream = b"\x1b[>84;0;0c\x1b[?1;2;4c\x1b[0n".to_vec();
        // Must stay Pending rather than falsely detecting Sixel before outer Kitty response arrives
        assert_eq!(parse_probe_stream(&stream, false), ProbeResult::Pending);
        stream.extend_from_slice(GHOSTTY_KITTY_RESPONSE);
        assert_eq!(
            parse_probe_stream(&stream, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_direct_sixel() {
        assert_eq!(
            parse_probe_stream(SIXEL_PROBE_RESPONSE, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Sixel,
                is_tmux: false,
            })
        );
    }

    #[test]
    fn test_parse_probe_tmux_sixel() {
        assert_eq!(
            parse_probe_stream(TMUX_SIXEL_PROBE_RESPONSE, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Sixel,
                is_tmux: true,
            })
        );
    }

    #[test]
    fn test_parse_probe_direct_unsupported_terminal_immediate() {
        assert_eq!(
            parse_probe_stream(DIRECT_DSR_RESPONSE, false),
            ProbeResult::Unsupported
        );
    }

    #[test]
    fn test_parse_probe_tmux_unsupported_terminal_both_dsrs() {
        let stream = b"\x1b[>84;0;0c\x1b[0n\x1b[0n";
        assert_eq!(parse_probe_stream(stream, false), ProbeResult::Unsupported);
    }

    #[test]
    fn test_parse_probe_unknown_dcs_ignored() {
        assert_eq!(
            parse_probe_stream(UNKNOWN_DCS_STREAM, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: false,
            })
        );
    }

    #[test]
    fn test_parse_probe_bel_terminated_response() {
        assert_eq!(
            parse_probe_stream(BEL_TERMINATED_RESPONSE, false),
            ProbeResult::Detected(DetectedGraphics {
                protocol: ProtocolType::Kitty,
                is_tmux: false,
            })
        );
    }

    #[test]
    fn test_parse_probe_incomplete_response_pending() {
        assert_eq!(
            parse_probe_stream(INCOMPLETE_KITTY_RESPONSE, false),
            ProbeResult::Pending
        );
    }

    #[test]
    fn test_create_picker_from_detected_graphics() {
        let detected = DetectedGraphics {
            protocol: ProtocolType::Kitty,
            is_tmux: true,
        };
        let picker = create_picker(Some(detected), true, FALLBACK_FONT_SIZE, |_| None).unwrap();
        assert_eq!(picker.protocol_type(), ProtocolType::Kitty);
        assert!(picker.tmux_detected());

        let detected_direct = DetectedGraphics {
            protocol: ProtocolType::Kitty,
            is_tmux: false,
        };
        let picker_direct =
            create_picker(Some(detected_direct), true, FALLBACK_FONT_SIZE, |_| None).unwrap();
        assert_eq!(picker_direct.protocol_type(), ProtocolType::Kitty);
        assert!(!picker_direct.tmux_detected());

        let picker_disabled = create_picker(Some(detected), false, FALLBACK_FONT_SIZE, |_| None);
        assert!(picker_disabled.is_none());

        let fallback_picker = create_picker(None, true, FALLBACK_FONT_SIZE, |key| {
            if key == TERM {
                Some(KITTY_TERM.into())
            } else {
                None
            }
        });
        assert!(fallback_picker.is_some());
        assert_eq!(
            fallback_picker.unwrap().protocol_type(),
            ProtocolType::Kitty
        );

        let none_picker = create_picker(None, true, FALLBACK_FONT_SIZE, |_| None);
        assert!(none_picker.is_none());
    }

    #[test]
    fn test_inline_image_encode_emits_tmux_dcs_wrapper() -> Result<()> {
        let mut png = Cursor::new(Vec::new());
        DynamicImage::new_rgb8(IMAGE_WIDTH.into(), IMAGE_HEIGHT.into())
            .write_to(&mut png, ImageFormat::Png)?;
        let source = ImageSource::new(ImageMediaType::Png, STANDARD.encode(png.get_ref()).into());
        #[allow(deprecated)]
        let mut picker = Picker::from_fontsize(FALLBACK_FONT_SIZE);
        picker.set_protocol_type(ProtocolType::Kitty);
        picker.set_is_tmux(true);
        assert!(picker.tmux_detected());
        let protocol = encode(&source, &picker, IMAGE_WIDTH)?;

        let mut buf = Buffer::empty(Rect::new(0, 0, IMAGE_WIDTH, IMAGE_HEIGHT));
        Widget::render(
            SlicedImage::new(&protocol, SignedPosition::from((0, 0))),
            Rect::new(0, 0, IMAGE_WIDTH, IMAGE_HEIGHT),
            &mut buf,
        );
        let contains_tmux_dcs = (0..IMAGE_HEIGHT)
            .any(|y| (0..IMAGE_WIDTH).any(|x| buf[(x, y)].symbol().contains(TMUX_DCS_PREFIX)));
        assert!(contains_tmux_dcs);
        Ok(())
    }

    #[test]
    fn test_disabled_or_unsupported_picker_falls_back_to_placeholder() {
        let source = ImageSource::new(ImageMediaType::Png, STANDARD.encode(MALFORMED_IMAGE).into());
        let image = InlineImage::new(source, Some(IMAGE_PLACEHOLDER));
        assert_eq!(image.fallback(), Some(IMAGE_PLACEHOLDER));
        assert_eq!(image.height(), 1);
        assert!(image.protocol(IMAGE_WIDTH).is_none());
    }
}
