use crossterm::event::KeyEvent;
use ratatui::layout::{Position, Rect};

/// One UI surface family that owns a vertical scrollbar.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PickerId {
    Model,
    Task,
    Theme,
    Rewind,
    Login,
    Mcp,
    Lua,
    Search,
    File,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScrollOwner {
    Messages,
    Input,
    Picker(PickerId),
    Help,
    Btw,
    Float(u32),
}

/// The palette's three selection domains plus the file completion grid.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompletionGrid {
    PaletteCommands,
    PaletteArguments,
    PaletteGrid,
    FileCompletion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Surface {
    Picker(PickerId),
    Help,
    Btw,
    Permission,
    Float(u32),
    Menu,
    Plan,
    Queue,
    Completion,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClickTarget {
    Surface(Surface),
    DismissSurface(Surface),
    PickerInput(PickerId),
    DeferInput,
    KeyHint(KeyEvent),
    PermissionChip,
    ModeChip,
    ThinkingChip,
    ModelChip,
    AgentChip,
    TasksHint,
    AutoScrollChip,
    StatusEntry(u32),
    MenuRow(u16),
    PickerRow { picker: PickerId, index: usize },
    PermissionHint { row: u16, cell: u16 },
    PermissionGuidance,
    PlanRow(u16),
    QueueRow(usize),
    CompletionRow { grid: CompletionGrid, index: usize },
    WindowContent(u32),
    WindowChrome(u32),
    WindowFooter { id: u32, index: usize },
    Scrollbar(ScrollOwner),
}

#[derive(Clone, Copy, Debug)]
pub struct HitTarget {
    pub rect: Rect,
    pub target: ClickTarget,
}

#[derive(Default)]
pub struct HitRegistry {
    hits: Vec<HitTarget>,
    modal: Option<(Surface, Rect, usize)>,
}

impl HitRegistry {
    pub fn clear(&mut self) {
        self.hits.clear();
        self.modal = None;
    }

    pub fn push(&mut self, rect: Rect, target: ClickTarget) {
        if rect.width == 0 || rect.height == 0 {
            return;
        }
        self.hits.push(HitTarget { rect, target });
    }

    pub fn target_at(&self, pos: Position) -> Option<ClickTarget> {
        self.hits[self.modal.map_or(0, |(_, _, start)| start)..]
            .iter()
            .rev()
            .find(|hit| hit.rect.contains(pos))
            .map(|hit| hit.target)
    }

    pub fn surface_at(&self, pos: Position) -> Option<Surface> {
        self.hits[self.modal.map_or(0, |(_, _, start)| start)..]
            .iter()
            .rev()
            .find_map(|hit| match hit.target {
                ClickTarget::Surface(surface) if hit.rect.contains(pos) => Some(surface),
                _ => None,
            })
    }

    pub fn scroll_surface_at(&self, pos: Position) -> Option<Surface> {
        self.hits.iter().rev().find_map(|hit| match hit.target {
            ClickTarget::Surface(surface) if hit.rect.contains(pos) => Some(surface),
            _ => None,
        })
    }

    pub fn rect_of(&self, target: ClickTarget) -> Option<Rect> {
        self.hits[self.modal.map_or(0, |(_, _, start)| start)..]
            .iter()
            .rev()
            .find(|hit| hit.target == target)
            .map(|hit| hit.rect)
    }

    pub fn surface(&mut self, surface: Surface, rect: Rect, modal: bool) {
        if rect.is_empty() {
            return;
        }
        if modal {
            self.modal = Some((surface, rect, self.hits.len()));
        }
        self.push(rect, ClickTarget::Surface(surface));
    }

    pub fn modal(&self) -> Option<(Surface, Rect)> {
        self.modal.map(|(surface, rect, _)| (surface, rect))
    }

    pub fn inside_modal(&self, pos: Position) -> bool {
        self.modal.is_some_and(|(_, rect, _)| rect.contains(pos))
    }
}
