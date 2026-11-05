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
pub enum ClickTarget {
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
    modal_rects: Vec<Rect>,
}

impl HitRegistry {
    pub fn clear(&mut self) {
        self.hits.clear();
        self.modal_rects.clear();
    }

    pub fn push(&mut self, rect: Rect, target: ClickTarget) {
        if rect.width == 0 || rect.height == 0 {
            return;
        }
        self.hits.push(HitTarget { rect, target });
    }

    pub fn target_at(&self, pos: Position) -> Option<ClickTarget> {
        self.hits
            .iter()
            .rev()
            .find(|hit| hit.rect.contains(pos))
            .map(|hit| hit.target)
    }

    pub fn rect_of(&self, target: ClickTarget) -> Option<Rect> {
        self.hits
            .iter()
            .rev()
            .find(|hit| hit.target == target)
            .map(|hit| hit.rect)
    }

    pub fn set_modal_rects(&mut self, rects: impl IntoIterator<Item = Rect>) {
        self.modal_rects.clear();
        self.modal_rects
            .extend(rects.into_iter().filter(|r| r.width > 0 && r.height > 0));
    }

    pub fn modal_rects(&self) -> &[Rect] {
        &self.modal_rects
    }

    pub fn inside_modal(&self, pos: Position) -> bool {
        self.modal_rects.iter().any(|rect| rect.contains(pos))
    }

    pub fn is_empty(&self) -> bool {
        self.hits.is_empty()
    }
}
