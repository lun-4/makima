use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use super::{RetryInfo, Status};
use makima_lua::StatusContentSnapshot;
#[cfg(test)]
use makima_lua::StatusEntry;

use crate::animation::spinner_frame;
use crate::click::{ClickTarget, HitTarget};
use crate::theme;

use makima_providers::format_tokens;
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::Paragraph;
use tracing::info;
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::repaint::{Cadence, Dirty};

const TRUNCATE_PREFIX: &str = "..";
const FAST_LABEL: &str = " [fast]";
const WORKFLOW_LABEL: &str = " [workflow]";
const RESTRICTED_LABEL: &str = " [restricted]";
const NORMAL_LABEL: &str = " [normal]";
const YOLO_LABEL: &str = " [yolo]";
const YOLO_DIM_FACTOR: f32 = 0.15;
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
    /// The model the visible turn is running on, or the session's model when
    /// idle.
    pub model_id: &'a str,
    /// Set when a model change has been adopted but the run has not reached
    /// its next request yet. Showing the new name would claim a switch that
    /// has not happened; showing only the old one would look like the change
    /// was ignored.
    pub model_switch_queued: bool,
    pub stats: UsageStats,
    pub auto_scroll: bool,
    pub retry_info: Option<&'a RetryInfo>,
    pub thinking_label: Option<Cow<'static, str>>,
    pub fast: bool,
    pub workflow: bool,
    pub yolo: bool,
    pub restricted: bool,
    pub restoring: bool,
    pub status_content: Option<&'a StatusContentSnapshot>,
    pub suppress_status_content: bool,
    pub hover: Option<ClickTarget>,
    pub thinking_supported: bool,
}

pub struct StatusBar {
    flash: Option<(String, Instant)>,
    started_at: Instant,
    cwd: PathBuf,
    cwd_branch: String,
    pub flash_duration: Duration,
    branch_watcher: Option<BranchWatcher>,
}

struct BranchWatcher {
    /// Nothing reads this field. It is here so it dies with the bar, and
    /// dropping it stops notify's event thread and closes the inotify fd.
    _watcher: RecommendedWatcher,
    rx: flume::Receiver<()>,
}

impl StatusBar {
    pub fn new(flash_duration: Duration, cwd: PathBuf) -> Self {
        Self {
            flash: None,
            started_at: Instant::now(),
            cwd_branch: cwd_branch_label(&cwd),
            branch_watcher: watch_branch(&cwd),
            cwd,
            flash_duration,
        }
    }

    /// The single place a flash is set. Some failures surface nowhere else,
    /// and a flash is gone within seconds, so mirror it to the log: tracing a
    /// reported error afterwards should not depend on catching the status bar
    /// before it clears.
    pub fn flash(&mut self, msg: String) {
        info!(flash = %msg, "status flash");
        self.flash = Some((msg, Instant::now()));
    }

    #[cfg(test)]
    pub fn flash_text(&self) -> Option<&str> {
        self.flash.as_ref().map(|(s, _)| s.as_str())
    }

    pub fn set_cwd(&mut self, cwd: PathBuf) {
        if self.cwd == cwd {
            return;
        }
        self.cwd_branch = cwd_branch_label(&cwd);
        drop(self.branch_watcher.take());
        self.branch_watcher = watch_branch(&cwd);
        self.cwd = cwd;
    }

    pub(crate) fn cwd_branch(&self) -> &str {
        &self.cwd_branch
    }

    pub fn poll_branch_update(&mut self) -> Dirty {
        let Some(watcher) = &self.branch_watcher else {
            return Dirty::NO;
        };
        if watcher.rx.try_iter().next().is_none() {
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

    pub fn view(&self, frame: &mut Frame, area: Rect, ctx: &StatusBarContext) -> Vec<HitTarget> {
        let hovered = |target: ClickTarget| ctx.hover == Some(target);
        let permission_label = if ctx.yolo { YOLO_LABEL } else { NORMAL_LABEL };
        let permission_width = (permission_label.width() as u16).min(area.width);
        let permission_area = Rect::new(
            area.right().saturating_sub(permission_width),
            area.y,
            permission_width,
            area.height,
        );
        let area = Rect::new(
            area.x,
            area.y,
            area.width.saturating_sub(permission_width),
            area.height,
        );
        let permission_style = if ctx.yolo {
            theme::dim_style(theme::current().error, YOLO_DIM_FACTOR)
        } else {
            theme::current().status_dim
        };
        let permission_style = if hovered(ClickTarget::PermissionChip) {
            permission_style.add_modifier(Modifier::UNDERLINED)
        } else {
            permission_style
        };
        frame.render_widget(
            Paragraph::new(Span::styled(permission_label, permission_style)),
            permission_area,
        );
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

        let mode_idx = left_spans.len();
        let mode_style = if hovered(ClickTarget::ModeChip) {
            ctx.mode_style.add_modifier(Modifier::UNDERLINED)
        } else {
            ctx.mode_style
        };
        left_spans.push(Span::styled(format!(" {}", ctx.mode_label), mode_style));

        let mut autoscroll_idx = None;
        if !ctx.auto_scroll {
            autoscroll_idx = Some(left_spans.len());
            let style = if hovered(ClickTarget::AutoScrollChip) {
                theme::current()
                    .status_dim
                    .add_modifier(Modifier::UNDERLINED)
            } else {
                theme::current().status_dim
            };
            left_spans.push(Span::styled(" auto-scroll paused", style));
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
        let mut model_idx = None;
        let mut thinking_idx = None;
        let mut status_base = None;
        let mut status_clicks: Vec<(usize, u32)> = Vec::new();

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
                    thinking_idx = Some(rest_spans.len());
                    rest_spans.push(Span::styled(
                        format!(" [{label}]"),
                        if hovered(ClickTarget::ThinkingChip) {
                            theme::current()
                                .status_dim
                                .add_modifier(Modifier::UNDERLINED)
                        } else {
                            theme::current().status_dim
                        },
                    ));
                } else if ctx.thinking_supported {
                    thinking_idx = Some(rest_spans.len());
                    rest_spans.push(Span::styled(
                        " [thinking: off]",
                        if hovered(ClickTarget::ThinkingChip) {
                            theme::current()
                                .status_dim
                                .add_modifier(Modifier::UNDERLINED)
                        } else {
                            theme::current().status_dim
                        },
                    ));
                }

                if ctx.fast {
                    rest_spans.push(Span::styled(FAST_LABEL, theme::current().status_dim));
                }
                if ctx.workflow {
                    rest_spans.push(Span::styled(WORKFLOW_LABEL, theme::current().status_dim));
                }
                if ctx.restricted {
                    rest_spans.push(Span::styled(RESTRICTED_LABEL, theme::current().status_dim));
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

                let left_width: usize = left_spans.iter().map(Span::width).sum();
                let rest_width: usize = rest_spans.iter().map(Span::width).sum();
                let available = (area.width as usize).saturating_sub(left_width + rest_width);
                let model_text = if ctx.model_switch_queued {
                    Cow::Owned(format!("{} {MODEL_SWITCH_QUEUED}", ctx.model_id))
                } else {
                    Cow::Borrowed(ctx.model_id)
                };
                let model_min_width = 8.min(model_text.width());
                let (plugin_spans, clicks) = if !ctx.suppress_status_content {
                    ctx.status_content
                        .map(|snapshot| {
                            status_content_spans(
                                snapshot,
                                available.saturating_sub(model_min_width),
                            )
                        })
                        .unwrap_or_default()
                } else {
                    (Vec::new(), Vec::new())
                };
                status_clicks = clicks;
                let plugin_width: usize = plugin_spans.iter().map(Span::width).sum();
                // `truncate_tail` keeps the tail, so a narrow bar collapses to
                // the incoming model, which is the half worth keeping.
                let model =
                    truncate_tail(&model_text, available.saturating_sub(plugin_width)).into_owned();

                model_idx = Some(right_spans.len());
                right_spans.push(Span::styled(
                    model,
                    if hovered(ClickTarget::ModelChip) {
                        theme::current()
                            .status_dim
                            .add_modifier(Modifier::UNDERLINED)
                    } else {
                        theme::current().status_dim
                    },
                ));
                let rest_base = right_spans.len();
                if let Some(idx) = thinking_idx {
                    thinking_idx = Some(rest_base + idx);
                }
                right_spans.append(&mut rest_spans);
                status_base = Some(right_spans.len());
                right_spans.extend(plugin_spans);
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

        let mut hits = Vec::new();
        if !permission_area.is_empty() {
            hits.push(HitTarget {
                rect: permission_area,
                target: ClickTarget::PermissionChip,
            });
        }
        if let Some(rect) = span_rect(left_area, &left_spans, mode_idx) {
            hits.push(HitTarget {
                rect,
                target: ClickTarget::ModeChip,
            });
        }
        if let Some(idx) = autoscroll_idx
            && let Some(rect) = span_rect(left_area, &left_spans, idx)
        {
            hits.push(HitTarget {
                rect,
                target: ClickTarget::AutoScrollChip,
            });
        }
        let model_rect = model_idx.and_then(|idx| span_rect(right_area, &right_spans, idx));
        let status_rects: Vec<(Rect, u32)> = status_base
            .map(|base| {
                status_clicks
                    .iter()
                    .filter_map(|(idx, id)| {
                        span_rect(right_area, &right_spans, base + idx).map(|rect| (rect, *id))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let thinking_rect = thinking_idx.and_then(|idx| span_rect(right_area, &right_spans, idx));
        frame.render_widget(Paragraph::new(Line::from(left_spans)), left_area);
        frame.render_widget(
            Paragraph::new(Line::from(right_spans)).alignment(Alignment::Right),
            right_area,
        );
        if let Some(rect) = model_rect {
            hits.push(HitTarget {
                rect,
                target: ClickTarget::ModelChip,
            });
        }
        if let Some(rect) = thinking_rect {
            hits.push(HitTarget {
                rect,
                target: ClickTarget::ThinkingChip,
            });
        }
        for (rect, id) in status_rects {
            hits.push(HitTarget {
                rect,
                target: ClickTarget::StatusEntry(id),
            });
        }
        hits
    }
}

fn span_rect(area: Rect, spans: &[Span], index: usize) -> Option<Rect> {
    let span = spans.get(index)?;
    let x = area.x + spans[..index].iter().map(Span::width).sum::<usize>() as u16;
    let right = x.saturating_add(span.width() as u16).min(area.right());
    (right > x && x < area.right()).then(|| Rect::new(x, area.y, right - x, 1))
}

fn status_content_spans(
    snapshot: &StatusContentSnapshot,
    max_width: usize,
) -> (Vec<Span<'static>>, Vec<(usize, u32)>) {
    let mut remaining = max_width;
    let mut spans = Vec::new();
    let mut clicks = Vec::new();
    let mut has_content = false;
    for (_, entries) in &snapshot.entries {
        let mut owner_has_content = false;
        for entry in entries {
            if entry.text.is_empty() {
                continue;
            }
            let separator = usize::from(has_content && !owner_has_content);
            if remaining <= separator {
                return (spans, clicks);
            }
            remaining -= separator;
            let clipped = truncate_head(&entry.text, remaining);
            let width = clipped.width();
            if width == 0 {
                continue;
            }
            if separator != 0 {
                spans.push(Span::raw(" "));
            }
            remaining -= width;
            if let Some(id) = entry.click {
                clicks.push((spans.len(), id));
            }
            spans.push(Span::styled(
                clipped.into_owned(),
                theme::style_by_name(&entry.style),
            ));
            has_content = true;
            owner_has_content = true;
        }
    }
    (spans, clicks)
}

fn truncate_head(s: &str, max_width: usize) -> Cow<'_, str> {
    if s.width() <= max_width {
        return Cow::Borrowed(s);
    }
    let mut width = 0;
    let end = s
        .char_indices()
        .take_while(|(_, ch)| {
            let next = width + ch.width().unwrap_or(0);
            if next > max_width {
                false
            } else {
                width = next;
                true
            }
        })
        .map(|(index, ch)| index + ch.len_utf8())
        .last()
        .unwrap_or(0);
    Cow::Owned(s[..end].to_owned())
}

/// Marks a model change adopted but not yet reached by the run, which picks
/// one up at its next request rather than mid-flight.
const MODEL_SWITCH_QUEUED: &str = "[switch queued]";

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
    let Some(home) = makima_storage::paths::home() else {
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

fn watch_branch(cwd: &Path) -> Option<BranchWatcher> {
    let git_dir = find_git_dir(cwd)?;
    let (tx, rx) = flume::bounded(1);
    let mut watcher = notify::recommended_watcher(move |res: Result<Event, _>| {
        if res.is_ok_and(|e| e.paths.iter().any(|p| p.ends_with("HEAD"))) {
            let _ = tx.try_send(());
        }
    })
    .ok()?;
    watcher.watch(&git_dir, RecursiveMode::NonRecursive).ok()?;
    Some(BranchWatcher {
        _watcher: watcher,
        rx,
    })
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::sync::Arc;

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
    const WATCHER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

    fn test_cwd() -> PathBuf {
        env::current_dir().unwrap()
    }

    fn render_context(status_content: Option<&StatusContentSnapshot>, width: u16) -> String {
        render_context_with(status_content, width, false)
    }

    fn render_context_with(
        status_content: Option<&StatusContentSnapshot>,
        width: u16,
        model_switch_queued: bool,
    ) -> String {
        let bar = StatusBar::new(FLASH_TTL, test_cwd());
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, 1)).unwrap();
        let status = Status::Idle;
        let ctx = StatusBarContext {
            status: &status,
            mode_label: Cow::Borrowed("build"),
            mode_style: Style::default(),
            model_id: MODEL_ID,
            model_switch_queued,
            stats: UsageStats {
                global_cost: None,
                context_size: CONTEXT_SIZE,
                cost: Some(CHAT_COST),
                context_window: crate::components::TEST_CONTEXT_WINDOW,
                show_global: false,
            },
            auto_scroll: true,
            retry_info: None,
            thinking_label: None,
            fast: false,
            workflow: false,
            yolo: false,
            restricted: false,
            restoring: false,
            status_content,
            suppress_status_content: false,
            hover: None,
            thinking_supported: false,
        };
        terminal
            .draw(|f| {
                bar.view(f, f.area(), &ctx);
            })
            .unwrap();
        crate::components::buffer_text(terminal.backend().buffer())
    }

    #[test]
    fn status_bar_orders_model_context_cost_then_plugin_at_right() {
        let snapshot = StatusContentSnapshot {
            entries: vec![(
                Arc::from("usage"),
                vec![StatusEntry {
                    text: "PLUGIN".into(),
                    style: "accent".into(),
                    click: None,
                }],
            )],
            generation: 1,
        };
        let text = render_context(Some(&snapshot), 120);
        let model = text.find(MODEL_ID).unwrap();
        let context = text.find("12.0k/200.0k (6%)").unwrap();
        let cost = text.find(CHAT_COST_TEXT).unwrap();
        let plugin = text.find("PLUGIN").unwrap();
        assert!(model < context && context < cost && cost < plugin, "{text}");
        assert!(text.trim_end().ends_with(NORMAL_LABEL.trim()), "{text}");
    }

    /// A run picks a changed model up at its next request, so between the
    /// change and that request the bar has to say the switch is pending: the
    /// new name alone would claim a switch that has not happened, and the old
    /// name alone looks like the change was ignored.
    #[test]
    fn status_bar_marks_a_queued_model_switch() {
        let text = render_context_with(None, 120, true);
        assert!(text.contains(MODEL_ID), "the running model stays: {text}");
        assert!(text.contains(MODEL_SWITCH_QUEUED), "{text}");
    }

    #[test]
    fn status_bar_without_a_queued_switch_shows_only_the_model() {
        let text = render_context_with(None, 120, false);
        assert!(text.contains(MODEL_ID), "{text}");
        assert!(!text.contains(MODEL_SWITCH_QUEUED), "{text}");
    }

    #[test]
    fn status_bar_narrow_width_keeps_plugin_and_model_minimum() {
        let snapshot = StatusContentSnapshot {
            entries: vec![(
                Arc::from("usage"),
                vec![StatusEntry {
                    text: "PLUGIN".into(),
                    style: "accent".into(),
                    click: None,
                }],
            )],
            generation: 1,
        };
        let text = render_context(Some(&snapshot), 56);
        assert!(text.contains("PLUGIN"), "{text}");
        assert!(text.contains("..-model"), "{text}");
    }

    #[test]
    fn status_content_preserves_authored_span_spacing() {
        let snapshot = StatusContentSnapshot {
            entries: vec![(
                Arc::from("usage"),
                vec![
                    StatusEntry {
                        text: "5h30%".into(),
                        style: "accent".into(),
                        click: None,
                    },
                    StatusEntry {
                        text: " ".into(),
                        style: "dim".into(),
                        click: None,
                    },
                    StatusEntry {
                        text: "w90%".into(),
                        style: "accent".into(),
                        click: None,
                    },
                ],
            )],
            generation: 1,
        };
        let (spans, _) = status_content_spans(&snapshot, 32);
        let text: String = spans.iter().map(|span| span.content.as_ref()).collect();
        assert_eq!(text, "5h30% w90%");
    }

    #[test]
    fn status_content_separates_plugin_entries_once() {
        let snapshot = StatusContentSnapshot {
            entries: vec![
                (
                    Arc::from("one"),
                    vec![StatusEntry {
                        text: "a".into(),
                        style: "accent".into(),
                        click: None,
                    }],
                ),
                (
                    Arc::from("two"),
                    vec![StatusEntry {
                        text: "b".into(),
                        style: "accent".into(),
                        click: None,
                    }],
                ),
            ],
            generation: 1,
        };
        let (spans, _) = status_content_spans(&snapshot, 32);
        let text: String = spans.iter().map(|span| span.content.as_ref()).collect();
        assert_eq!(text, "a b");
    }

    fn render(global_cost: Option<f64>, show_global: bool) -> String {
        let bar = StatusBar::new(FLASH_TTL, test_cwd());
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(BAR_WIDTH, 1)).unwrap();
        let ctx = StatusBarContext {
            status: &Status::Idle,
            mode_label: "build".into(),
            mode_style: Style::new(),
            model_id: MODEL_ID,
            model_switch_queued: false,
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
            yolo: false,
            restricted: false,
            restoring: false,
            status_content: None,
            suppress_status_content: false,
            hover: None,
            thinking_supported: false,
        };
        terminal
            .draw(|f| {
                bar.view(f, f.area(), &ctx);
            })
            .unwrap();
        crate::components::buffer_text(terminal.backend().buffer())
    }

    /// A restricted folder says so for the whole session: the startup card is
    /// long gone by the time the user wonders why their project config did
    /// nothing.
    #[test_case(true  => true  ; "a_restricted_folder_says_so")]
    #[test_case(false => false ; "a_trusted_folder_stays_quiet")]
    fn the_bar_advertises_restricted(restricted: bool) -> bool {
        let bar = StatusBar::new(FLASH_TTL, test_cwd());
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(BAR_WIDTH, 1)).unwrap();
        let ctx = StatusBarContext {
            status: &Status::Idle,
            mode_label: Cow::Borrowed("NORMAL"),
            mode_style: Style::default(),
            model_id: MODEL_ID,
            model_switch_queued: false,
            stats: UsageStats {
                global_cost: None,
                context_size: CONTEXT_SIZE,
                cost: Some(CHAT_COST),
                context_window: crate::components::TEST_CONTEXT_WINDOW,
                show_global: false,
            },
            auto_scroll: true,
            retry_info: None,
            thinking_label: None,
            fast: false,
            workflow: false,
            yolo: false,
            restricted,
            restoring: false,
            status_content: None,
            suppress_status_content: false,
            hover: None,
            thinking_supported: false,
        };
        terminal
            .draw(|f| {
                bar.view(f, f.area(), &ctx);
            })
            .unwrap();
        crate::components::buffer_text(terminal.backend().buffer())
            .contains(RESTRICTED_LABEL.trim())
    }

    #[test_case(true, false, BAR_WIDTH, false ; "yolo_idle")]
    #[test_case(false, false, BAR_WIDTH, false ; "normal_idle")]
    #[test_case(true, true, BAR_WIDTH, false ; "yolo_error")]
    #[test_case(false, true, BAR_WIDTH, false ; "normal_error")]
    #[test_case(true, false, 12, true ; "yolo_narrow_hovered")]
    #[test_case(false, true, 12, true ; "normal_narrow_hovered")]
    fn permission_chip_is_visible_and_clickable(yolo: bool, error: bool, width: u16, hover: bool) {
        let mut bar = StatusBar::new(FLASH_TTL, test_cwd());
        bar.flash(FLASH_MSG.into());
        let mut terminal =
            ratatui::Terminal::new(ratatui::backend::TestBackend::new(width, 1)).unwrap();
        let status = if error {
            Status::error(FLASH_MSG.into())
        } else {
            Status::Idle
        };
        let ctx = StatusBarContext {
            status: &status,
            mode_label: Cow::Borrowed("NORMAL"),
            mode_style: Style::default(),
            model_id: MODEL_ID,
            model_switch_queued: false,
            stats: UsageStats {
                global_cost: None,
                context_size: CONTEXT_SIZE,
                cost: Some(CHAT_COST),
                context_window: crate::components::TEST_CONTEXT_WINDOW,
                show_global: false,
            },
            auto_scroll: true,
            retry_info: None,
            thinking_label: None,
            fast: false,
            workflow: false,
            yolo,
            restricted: false,
            restoring: false,
            status_content: None,
            suppress_status_content: false,
            hover: hover.then_some(ClickTarget::PermissionChip),
            thinking_supported: false,
        };
        let mut hits = Vec::new();
        terminal
            .draw(|f| {
                hits = bar.view(f, f.area(), &ctx);
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let text = crate::components::buffer_text(buffer);
        let (label, other) = if yolo {
            (YOLO_LABEL, NORMAL_LABEL)
        } else {
            (NORMAL_LABEL, YOLO_LABEL)
        };
        assert!(text.ends_with(label), "{text}");
        assert!(!text.contains(other), "{text}");
        let hit = hits
            .iter()
            .find(|hit| hit.target == ClickTarget::PermissionChip)
            .unwrap();
        assert_eq!(
            hit.rect,
            Rect::new(width - label.width() as u16, 0, label.width() as u16, 1)
        );
        assert_eq!(
            buffer[(hit.rect.x + 1, 0)]
                .modifier
                .contains(Modifier::UNDERLINED),
            hover,
        );
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

    #[test_case("~/projects/makima:main", 30, "~/projects/makima:main" ; "fits_untouched")]
    #[test_case("~/projects/makima:main", 10, "..ima:main"          ; "ascii_tail")]
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
        let mut bar = StatusBar::new(ttl, test_cwd());
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
        let mut bar = StatusBar::new(FLASH_TTL, cwd);
        bar.cwd_branch = if stale {
            STALE_BRANCH.into()
        } else {
            label.clone()
        };
        bar.branch_watcher.as_mut().unwrap().rx = rx;
        tx.send(()).unwrap();

        let dirty = bar.poll_branch_update();
        assert_eq!(bar.cwd_branch, label);
        assert!(
            tx.try_send(()).is_ok(),
            "a full channel makes the watcher drop the next switch"
        );
        dirty
    }

    /// Dropping a watcher only asks notify's event thread to stop, and the
    /// sender dies on that thread, so wait for the channel to close instead of
    /// asserting on the spot.
    fn assert_watcher_dropped(rx: &flume::Receiver<()>) {
        let deadline = Instant::now() + WATCHER_SHUTDOWN_TIMEOUT;
        loop {
            match rx.recv_deadline(deadline) {
                Err(flume::RecvTimeoutError::Disconnected) => return,
                Ok(()) => {}
                Err(flume::RecvTimeoutError::Timeout) => {
                    panic!("the replaced watcher kept its event thread alive")
                }
            }
        }
    }

    /// A bar builds a watcher for its first cwd and another for every `/cd`,
    /// so a watcher that is only replaced leaves a thread and an inotify fd
    /// behind per directory the session ever visited.
    #[test]
    fn set_cwd_drops_the_previous_branch_watcher() {
        let (_first, first) = tmp_with_head(Some("ref: refs/heads/main\n"));
        let (_second, second) = tmp_with_head(Some("ref: refs/heads/main\n"));
        let (_third, third) = tmp_with_head(Some("ref: refs/heads/main\n"));

        let mut bar = StatusBar::new(FLASH_TTL, PathBuf::from(&first));
        let first_rx = bar.branch_watcher.as_ref().unwrap().rx.clone();
        bar.set_cwd(PathBuf::from(&second));
        let second_rx = bar.branch_watcher.as_ref().unwrap().rx.clone();
        bar.set_cwd(PathBuf::from(&third));

        assert_watcher_dropped(&first_rx);
        assert_watcher_dropped(&second_rx);
        assert!(
            !bar.branch_watcher.as_ref().unwrap().rx.is_disconnected(),
            "the bar must keep watching the cwd it moved to"
        );
    }

    #[test]
    fn clear_flash_removes_flash() {
        let mut bar = StatusBar::new(Duration::from_secs(999), test_cwd());
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
        let bar = StatusBar::new(Duration::from_secs(999), test_cwd());
        let status = Status::Idle;
        let ctx = StatusBarContext {
            status: &status,
            mode_label: Cow::Borrowed("NORMAL"),
            mode_style: Style::default(),
            model_id: "model",
            model_switch_queued: false,
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
            yolo: false,
            restricted: false,
            restoring: false,
            status_content: None,
            suppress_status_content: false,
            hover: None,
            thinking_supported: false,
        };
        term.draw(|frame| {
            bar.view(frame, frame.area(), &ctx);
        })
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
