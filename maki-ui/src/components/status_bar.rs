use std::borrow::Cow;
use std::env;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::{RetryInfo, Status};

use crate::animation::spinner_frame;
use crate::theme;

use maki_providers::format_tokens;
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::repaint::{Cadence, Dirty};

const TRUNCATE_PREFIX: &str = "..";
const FAST_LABEL: &str = " [fast]";
const WORKFLOW_LABEL: &str = " [workflow]";
const SUBAGENT_LABEL_PREFIX: &str = "\u{21b3} ";

/// Distinct footer badge for a focused subagent chat, contrasted with the
/// plain `[name]` used for the main chat.
pub(crate) fn subagent_label(name: &str) -> String {
    format!(" [{SUBAGENT_LABEL_PREFIX}{name}]")
}

pub struct UsageStats {
    /// The whole session's bill, drawn next to the focused chat's own once
    /// subagents make the two differ.
    pub global_cost: Option<f64>,
    pub context_size: u32,
    pub cost: Option<f64>,
    pub context_window: u32,
    pub show_global: bool,
}

pub struct StatusBarContext<'a> {
    pub status: &'a Status,
    pub mode_label: Cow<'static, str>,
    pub mode_style: Style,
    pub model_id: &'a str,
    pub stats: UsageStats,
    pub auto_scroll: bool,
    pub retry_info: Option<&'a RetryInfo>,
    /// Inline provider quota readout (`5h30% w50%`), drawn after the context
    /// length and cost. `None` for providers without a quota endpoint.
    pub usage: Option<Line<'static>>,
    pub thinking_label: Option<Cow<'static, str>>,
    pub fast: bool,
    pub workflow: bool,
    pub restoring: bool,
}

pub struct StatusBar {
    flash: Option<(String, Instant)>,
    started_at: Instant,
    cwd: PathBuf,
    cwd_branch: String,
    pub flash_duration: Duration,
    branch_update_rx: Option<flume::Receiver<()>>,
}

impl StatusBar {
    pub fn new(flash_duration: Duration) -> Self {
        let cwd = env::current_dir().unwrap_or_else(|_| ".".into());
        Self {
            flash: None,
            started_at: Instant::now(),
            cwd_branch: cwd_branch_label(&cwd),
            branch_update_rx: spawn_branch_watcher(&cwd),
            cwd,
            flash_duration,
        }
    }

    pub fn flash(&mut self, msg: String) {
        self.flash = Some((msg, Instant::now()));
    }

    #[cfg(test)]
    pub fn flash_text(&self) -> Option<&str> {
        self.flash.as_ref().map(|(s, _)| s.as_str())
    }

    pub fn set_cwd(&mut self, cwd: PathBuf) {
        self.cwd_branch = cwd_branch_label(&cwd);
        self.branch_update_rx = spawn_branch_watcher(&cwd);
        self.cwd = cwd;
    }

    pub(crate) fn cwd_branch(&self) -> &str {
        &self.cwd_branch
    }

    pub fn poll_branch_update(&mut self) -> Dirty {
        let Some(rx) = &self.branch_update_rx else {
            return Dirty::NO;
        };
        if rx.try_iter().next().is_none() {
            return Dirty::NO;
        }
        let branch = cwd_branch_label(&self.cwd);
        let changed = branch != self.cwd_branch;
        self.cwd_branch = branch;
        Dirty::from(changed)
    }

    pub fn clear_flash(&mut self) {
        self.flash = None;
    }

    pub fn clear_expired_hint(&mut self) -> Dirty {
        if self
            .flash
            .as_ref()
            .is_none_or(|(_, t)| t.elapsed() < self.flash_duration)
        {
            return Dirty::NO;
        }
        self.flash = None;
        Dirty::YES
    }

    /// The bar spins for a whole turn, again while a restore is in flight, and
    /// it counts a retry down by the second. It sits next to [`Self::view`] so
    /// a new moving span cannot forget to claim its frames.
    pub fn cadence(status: &Status, restoring: bool, retrying: bool) -> Cadence {
        Cadence::when(
            *status == Status::Streaming || restoring || retrying,
            Cadence::SPINNER,
        )
    }

    pub fn view(&self, frame: &mut Frame, area: Rect, ctx: &StatusBarContext) {
        let mut left_spans = Vec::new();

        if *ctx.status == Status::Streaming {
            let ch = spinner_frame(self.started_at.elapsed().as_millis());
            left_spans.push(Span::styled(format!(" {ch}"), theme::current().spinner));
        }

        if ctx.restoring {
            let ch = spinner_frame(self.started_at.elapsed().as_millis());
            left_spans.push(Span::styled(
                format!(" {ch}"),
                theme::current().status_notice,
            ));
        }

        left_spans.push(Span::styled(format!(" {}", ctx.mode_label), ctx.mode_style));

        if !ctx.auto_scroll {
            left_spans.push(Span::styled(
                " auto-scroll paused",
                theme::current().status_dim,
            ));
        }

        if let Some(retry) = ctx.retry_info {
            let secs = retry
                .deadline
                .saturating_duration_since(Instant::now())
                .as_secs();
            left_spans.push(Span::styled(
                format!(" {}", retry.message),
                theme::current().status_retry_error,
            ));
            left_spans.push(Span::styled(
                format!(" · retrying in {secs}s (#{})", retry.attempt),
                theme::current().status_retry_info,
            ));
        }

        let mut right_spans = Vec::new();

        match ctx.status {
            Status::Error { message: e, .. } => {
                left_spans.push(Span::styled(format!(" {e}"), theme::current().error));
            }
            _ => {
                let pct = if ctx.stats.context_window > 0 {
                    (ctx.stats.context_size as f64 / ctx.stats.context_window as f64 * 100.0) as u32
                } else {
                    0
                };

                let mut rest_spans = Vec::new();

                if let Some(ref label) = ctx.thinking_label {
                    rest_spans.push(Span::styled(
                        format!(" [{label}]"),
                        theme::current().status_dim,
                    ));
                }

                if ctx.fast {
                    rest_spans.push(Span::styled(FAST_LABEL, theme::current().status_dim));
                }
                if ctx.workflow {
                    rest_spans.push(Span::styled(WORKFLOW_LABEL, theme::current().status_dim));
                }

                let context_text = format!(
                    "  {}/{} ({}%)",
                    format_tokens(ctx.stats.context_size),
                    format_tokens(ctx.stats.context_window),
                    pct,
                );
                let rest_text = match ctx.stats.cost {
                    Some(cost) => format!("{context_text} ${cost:.3} "),
                    None => format!("{context_text} "),
                };
                rest_spans.push(Span::styled(
                    rest_text,
                    Style::new().fg(theme::current().foreground),
                ));

                if let Some(global) = ctx.stats.global_cost.filter(|_| ctx.stats.show_global) {
                    let global_text = format!(" \u{03a3}${global:.3} ");
                    rest_spans.push(Span::styled(
                        global_text,
                        Style::new().fg(theme::current().foreground),
                    ));
                }

                if let Some(usage) = ctx.usage.as_ref() {
                    rest_spans.push(Span::raw(" "));
                    rest_spans.extend(usage.spans.iter().cloned());
                }

                let left_width: usize = left_spans.iter().map(Span::width).sum();
                let rest_width: usize = rest_spans.iter().map(Span::width).sum();
                let available = (area.width as usize).saturating_sub(left_width + rest_width);
                let model = truncate_tail(ctx.model_id, available);

                right_spans.push(Span::styled(model, theme::current().status_dim));
                right_spans.append(&mut rest_spans);
            }
        }

        if let Some((ref msg, _)) = self.flash {
            left_spans.push(Span::styled(
                format!(" {msg}"),
                theme::current().status_notice,
            ));
        }

        let [left_area, right_area] = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(right_spans.iter().map(|s| s.width() as u16).sum()),
        ])
        .areas(area);

        frame.render_widget(Paragraph::new(Line::from(left_spans)), left_area);
        frame.render_widget(
            Paragraph::new(Line::from(right_spans)).alignment(Alignment::Right),
            right_area,
        );
    }
}

pub(crate) fn truncate_tail(s: &str, max_width: usize) -> Cow<'_, str> {
    if s.width() <= max_width {
        return Cow::Borrowed(s);
    }
    let budget = max_width.saturating_sub(TRUNCATE_PREFIX.width());
    let mut used = 0;
    let mut start = s.len();
    for (i, c) in s.char_indices().rev() {
        let w = c.width().unwrap_or(0);
        if used + w > budget {
            break;
        }
        used += w;
        start = i;
    }
    Cow::Owned(format!("{TRUNCATE_PREFIX}{}", &s[start..]))
}

fn collapse_home(path: &str) -> String {
    let Some(home) = maki_storage::paths::home() else {
        return path.to_string();
    };
    collapse_home_with(path, &home.to_string_lossy())
}

fn collapse_home_with(path: &str, home: &str) -> String {
    path.strip_prefix(home)
        .map(|rest| format!("~{rest}"))
        .unwrap_or_else(|| path.to_string())
}

fn cwd_branch_label(cwd: &Path) -> String {
    let cwd = cwd.to_string_lossy();
    let label = collapse_home(&cwd);
    match detect_branch(&cwd) {
        Some(branch) => format!("{label}:{branch}"),
        None => label,
    }
}

fn detect_branch(cwd: &str) -> Option<String> {
    let head = std::fs::read_to_string(find_git_dir(Path::new(cwd))?.join("HEAD")).ok()?;
    let head = head.trim();
    head.strip_prefix("ref: refs/heads/")
        .map(str::to_string)
        .or_else(|| Some(head.get(..7)?.to_string()))
}

fn find_git_dir(cwd: &Path) -> Option<std::path::PathBuf> {
    let mut dir = cwd;
    loop {
        let git = dir.join(".git");
        if git.is_dir() {
            return Some(git);
        }
        if git.is_file() {
            // Linked worktrees store a pointer here instead of a Git directory.
            let contents = std::fs::read_to_string(git).ok()?;
            let path = contents.trim().strip_prefix("gitdir: ")?;
            let path = Path::new(path);
            return Some(if path.is_absolute() {
                path.to_path_buf()
            } else {
                dir.join(path)
            });
        }
        dir = dir.parent()?;
    }
}

fn spawn_branch_watcher(cwd: &Path) -> Option<flume::Receiver<()>> {
    use notify::{RecursiveMode, Watcher};

    let git_dir = find_git_dir(cwd)?;
    let (tx, rx) = flume::bounded(1);

    std::thread::spawn(move || {
        let Ok(mut watcher) = notify::recommended_watcher(move |res: Result<notify::Event, _>| {
            if res.is_ok_and(|e| e.paths.iter().any(|p| p.ends_with("HEAD"))) {
                let _ = tx.try_send(());
            }
        }) else {
            return;
        };
        if watcher.watch(&git_dir, RecursiveMode::NonRecursive).is_ok() {
            std::thread::park();
        }
    });

    Some(rx)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::repaint::expect::QUIET;
    use tempfile::TempDir;
    use test_case::test_case;

    const FLASH_TTL: Duration = Duration::from_secs(3600);
    const FLASH_MSG: &str = "Copied";
    const STALE_BRANCH: &str = "/nowhere:gone";
    const BAR_WIDTH: u16 = 120;
    const MODEL_ID: &str = "test-model";
    const CONTEXT_SIZE: u32 = 12_000;
    const CHAT_COST: f64 = 0.25;
    const CHAT_COST_TEXT: &str = "$0.250";
    const SESSION_COST: f64 = 1.5;
    const SESSION_COST_TEXT: &str = "\u{03a3}$1.500";
    const SIGMA: char = '\u{03a3}';

    fn render(global_cost: Option<f64>, show_global: bool) -> String {
        let bar = StatusBar::new(FLASH_TTL);
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(BAR_WIDTH, 1)).unwrap();
        let ctx = StatusBarContext {
            status: &Status::Idle,
            mode_label: "build".into(),
            mode_style: Style::new(),
            model_id: MODEL_ID,
            stats: UsageStats {
                global_cost,
                context_size: CONTEXT_SIZE,
                cost: Some(CHAT_COST),
                context_window: crate::components::TEST_CONTEXT_WINDOW,
                show_global,
            },
            auto_scroll: true,
            retry_info: None,
            thinking_label: None,
            fast: false,
            workflow: false,
            restoring: false,
            usage: None,
        };
        terminal.draw(|f| bar.view(f, f.area(), &ctx)).unwrap();
        crate::components::buffer_text(terminal.backend().buffer())
    }

    /// The sigma is the whole session's bill, and only the session can hand it
    /// over. Pricing the focused chat's counters instead (what the bar used to
    /// do) tells the user a paid session was free, or bills another chat's
    /// tokens at this model's rates. A lone chat has nothing extra to show.
    #[test_case(Some(SESSION_COST), true  => true  ; "subagents_add_the_session_total")]
    #[test_case(Some(SESSION_COST), false => false ; "single_chat_shows_its_own_cost_only")]
    #[test_case(None,               true  => false ; "unpriced_session_claims_nothing")]
    fn session_total_appears_only_when_there_is_one_to_show(
        global_cost: Option<f64>,
        show_global: bool,
    ) -> bool {
        let text = render(global_cost, show_global);
        assert_eq!(text.matches(CHAT_COST_TEXT).count(), 1, "{text}");
        let shown = text.matches(SESSION_COST_TEXT).count() == 1;
        assert_eq!(
            text.contains(SIGMA),
            shown,
            "a sigma carrying another number is a misrender: {text}"
        );
        shown
    }

    #[test_case("/home/user/projects/app", "/home/user", "~/projects/app" ; "inside_home")]
    #[test_case("/tmp/other", "/home/user", "/tmp/other"                  ; "outside_home")]
    #[test_case("/home/user", "/home/user", "~"                           ; "exact_home")]
    fn collapse_home_cases(path: &str, home: &str, expected: &str) {
        assert_eq!(collapse_home_with(path, home), expected);
    }

    #[test_case("~/projects/maki:main", 30, "~/projects/maki:main" ; "fits_untouched")]
    #[test_case("~/projects/maki:main", 10, "..aki:main"           ; "ascii_tail")]
    #[test_case("~/文档/proj:分支", 8, "..j:分支"                  ; "cjk_path_and_branch")]
    #[test_case("release/🚀-v2", 6, "..-v2"                        ; "emoji_branch")]
    #[test_case("abc", 2, ".."                                     ; "prefix_only")]
    #[test_case("", 0, ""                                          ; "empty")]
    fn truncate_tail_cases(input: &str, max_width: usize, expected: &str) {
        assert_eq!(truncate_tail(input, max_width), expected);
    }

    fn tmp_with_head(content: Option<&str>) -> (TempDir, String) {
        let dir = TempDir::new().unwrap();
        if let Some(head) = content {
            let git = dir.path().join(".git");
            fs::create_dir(&git).unwrap();
            fs::write(git.join("HEAD"), head).unwrap();
        }
        let path = dir.path().to_string_lossy().into_owned();
        (dir, path)
    }

    #[test_case(Some("ref: refs/heads/feature/foo\n"), Some("feature/foo") ; "regular_ref")]
    #[test_case(Some("abc1234deadbeef\n"),            Some("abc1234")      ; "detached_head")]
    #[test_case(None,                                 None                 ; "no_git_dir")]
    fn detect_branch_cases(head: Option<&str>, expected: Option<&str>) {
        let (_dir, path) = tmp_with_head(head);
        assert_eq!(detect_branch(&path), expected.map(String::from));
    }

    #[test]
    fn detect_branch_from_subdirectory() {
        let (_dir, path) = tmp_with_head(Some("ref: refs/heads/main\n"));
        let sub = Path::new(&path).join("sub");
        fs::create_dir(&sub).unwrap();
        assert_eq!(
            detect_branch(&sub.to_string_lossy()),
            Some("main".to_string())
        );
    }

    #[test]
    fn detect_branch_from_worktree_gitfile() {
        let (_main, main_path) = tmp_with_head(None);
        let worktree = TempDir::new().unwrap();
        let git_dir = Path::new(&main_path).join(".git");
        fs::create_dir(&git_dir).unwrap();
        let worktree_git = git_dir.join("worktrees/feature");
        fs::create_dir_all(&worktree_git).unwrap();
        fs::write(worktree_git.join("HEAD"), "ref: refs/heads/feature\n").unwrap();
        fs::write(
            worktree.path().join(".git"),
            format!("gitdir: {}\n", worktree_git.display()),
        )
        .unwrap();

        assert_eq!(
            detect_branch(&worktree.path().to_string_lossy()),
            Some("feature".to_string())
        );
    }

    /// Once the flash is gone nothing clears the debt, so only the tick that
    /// removes it may report a change, or the loop never settles. The two
    /// lifetimes stand in for time passing: rewinding an `Instant` by an hour
    /// panics on a machine that booted less than an hour ago.
    #[test_case(false, FLASH_TTL      => Dirty::NO  ; "no_flash")]
    #[test_case(true,  FLASH_TTL      => Dirty::NO  ; "flash_still_visible")]
    #[test_case(true,  Duration::ZERO => Dirty::YES ; "flash_expired")]
    fn clear_expired_hint_owes_the_frame_only_once(flashing: bool, ttl: Duration) -> Dirty {
        let mut bar = StatusBar::new(ttl);
        if flashing {
            bar.flash(FLASH_MSG.into());
        }

        let first = bar.clear_expired_hint();
        assert_eq!(bar.clear_expired_hint(), Dirty::NO, "{QUIET}");
        first
    }

    /// The watcher fires for any write near `.git/HEAD`, most of which leave
    /// the branch alone, so repainting on each one means a repaint per commit,
    /// stash and index refresh while a build touches the repo. Either way the
    /// poll has to leave the bounded channel empty, or the watcher's
    /// `try_send` drops the next real switch.
    #[test_case(false => Dirty::NO  ; "unchanged_branch")]
    #[test_case(true  => Dirty::YES ; "switched_branch")]
    fn poll_branch_update_reports_only_real_changes(stale: bool) -> Dirty {
        let cwd = env::current_dir().unwrap();
        let label = cwd_branch_label(&cwd);
        let (tx, rx) = flume::bounded(1);
        let mut bar = StatusBar::new(FLASH_TTL);
        bar.cwd_branch = if stale {
            STALE_BRANCH.into()
        } else {
            label.clone()
        };
        bar.branch_update_rx = Some(rx);
        tx.send(()).unwrap();

        let dirty = bar.poll_branch_update();
        assert_eq!(bar.cwd_branch, label);
        assert!(
            tx.try_send(()).is_ok(),
            "a full channel makes the watcher drop the next switch"
        );
        dirty
    }

    #[test]
    fn clear_flash_removes_flash() {
        let mut bar = StatusBar::new(Duration::from_secs(999));
        bar.flash("Copied".into());
        bar.clear_flash();
        assert!(bar.flash.is_none());
    }

    #[test_case("task1", " [↳ task1]"            ; "named_subagent")]
    #[test_case("",       " [↳ ]"                ; "empty_name")]
    fn subagent_label_cases(name: &str, expected: &str) {
        assert_eq!(subagent_label(name), expected);
    }

    #[test]
    fn status_bar_no_longer_renders_chat_badge() {
        // The active-chat badge moved to the persistent top bar; the bottom
        // status bar must not duplicate it.
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;
        let mut term = Terminal::new(TestBackend::new(120, 1)).unwrap();
        let bar = StatusBar::new(Duration::from_secs(999));
        let status = Status::Idle;
        let ctx = StatusBarContext {
            status: &status,
            mode_label: Cow::Borrowed("NORMAL"),
            mode_style: Style::default(),
            model_id: "model",
            stats: UsageStats {
                global_cost: None,
                context_size: 0,
                cost: None,
                context_window: 0,
                show_global: false,
            },
            auto_scroll: false,
            retry_info: None,
            thinking_label: None,
            fast: false,
            workflow: false,
            restoring: false,
            usage: None,
        };
        term.draw(|frame| bar.view(frame, frame.area(), &ctx))
            .unwrap();
        let text: String = term
            .backend()
            .buffer()
            .content
            .iter()
            .map(|c| c.symbol())
            .collect();
        assert!(!text.contains('↳'), "no subagent badge: {text}");
        assert!(!text.contains("[task1]"), "no chat badge: {text}");
    }
}
