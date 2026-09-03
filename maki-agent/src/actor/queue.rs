//! The actor's sole FIFO deque.
//!
//! Every push, pop, interrupt extraction, removal, clear, snapshot, and drain
//! goes through this one lock-backed deque. `publish_if_empty` runs its
//! closure under the queue lock, so a drain publication can never interleave
//! with a concurrent push, matching the TUI's expectation.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{ActorWork, RootWork, TurnAdmission};
use crate::ExtractedCommand;
use crate::types::TurnId;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Neutral projection of one queued item, enough for the TUI to draw the
/// queue panel without importing UI types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueProjection {
    Message {
        text: String,
        image_count: usize,
        displayed: bool,
    },
    Compact,
    Control(String),
    Turn(String),
}

impl From<&RootWork> for QueueProjection {
    fn from(root: &RootWork) -> Self {
        Self::Message {
            text: root.text.clone(),
            image_count: root.image_count,
            displayed: root.displayed,
        }
    }
}

impl From<&ActorWork> for QueueProjection {
    fn from(work: &ActorWork) -> Self {
        match work {
            ActorWork::Root(root) => Self::from(root),
            ActorWork::Compact { .. } => Self::Compact,
            ActorWork::Control(control) => Self::Control(control.name.clone()),
            ActorWork::Turn(admission) => Self::Turn(admission.correlation.clone()),
        }
    }
}

/// One lock-backed FIFO deque of [`ActorWork`]. Shared through `Arc`.
pub struct ActorQueue {
    items: Mutex<VecDeque<ActorWork>>,
    notify_tx: flume::Sender<()>,
    notify_rx: Mutex<Option<flume::Receiver<()>>>,
}

impl ActorQueue {
    pub fn new() -> Self {
        let (notify_tx, notify_rx) = flume::bounded::<()>(1);
        Self {
            items: Mutex::new(VecDeque::new()),
            notify_tx,
            notify_rx: Mutex::new(Some(notify_rx)),
        }
    }

    /// Pushes `work` at the back and wakes the runner.
    pub fn push(&self, work: ActorWork) {
        lock(&self.items).push_back(work);
        self.notify();
    }

    /// Pops the front item, or `None` when the queue is empty.
    pub fn pop(&self) -> Option<ActorWork> {
        lock(&self.items).pop_front()
    }

    /// Extracts the given admitted turn from anywhere in the queue without
    /// disturbing the others. Returns `None` when it is not queued (already
    /// running or already consumed).
    pub fn remove_turn(&self, turn_id: TurnId) -> Option<TurnAdmission> {
        let mut items = lock(&self.items);
        let index = items
            .iter()
            .position(|w| matches!(w, ActorWork::Turn(a) if a.turn_id == turn_id))?;
        match items.remove(index) {
            Some(ActorWork::Turn(admission)) => Some(admission),
            _ => unreachable!("remove_turn position matched a Turn"),
        }
    }

    /// Removes the item at raw `index` (the same index [`snapshot`](Self::snapshot)
    /// uses) and returns it. `None` when out of bounds.
    pub fn remove_at(&self, index: usize) -> Option<ActorWork> {
        let mut items = lock(&self.items);
        if index >= items.len() {
            return None;
        }
        items.remove(index)
    }

    /// Correlation of one queued item. Roots and turns carry a host correlation;
    /// compacts are matched by their canonical `r{run_id}` encoding so a
    /// targeted cancel can drop them the way it drops a deferred root.
    fn correlation_of(work: &ActorWork) -> Option<std::borrow::Cow<'_, str>> {
        match work {
            ActorWork::Turn(a) => Some(std::borrow::Cow::Borrowed(a.correlation.as_str())),
            ActorWork::Root(r) => Some(std::borrow::Cow::Borrowed(r.correlation.as_str())),
            ActorWork::Compact { run_id } => {
                Some(std::borrow::Cow::Owned(super::run_correlation(*run_id)))
            }
            ActorWork::Control(_) => None,
        }
    }

    /// Removes every queued item whose correlation matches, returning them in
    /// FIFO order. Unrelated items stay untouched.
    pub fn remove_correlation(&self, correlation: &str) -> Vec<ActorWork> {
        let mut items = lock(&self.items);
        let matching: Vec<usize> = items
            .iter()
            .enumerate()
            .filter(|(_, w)| Self::correlation_of(w).as_deref() == Some(correlation))
            .map(|(i, _)| i)
            .collect();
        let mut removed = Vec::with_capacity(matching.len());
        for index in matching.into_iter().rev() {
            if let Some(work) = items.remove(index) {
                removed.push(work);
            }
        }
        removed.reverse();
        removed
    }

    /// Whether the item is shown in the TUI queue panel. Deferred roots
    /// (`displayed == false`) and compacts are visible; admitted turns,
    /// controls, and already-displayed roots are hidden rows.
    pub fn is_visible(work: &ActorWork) -> bool {
        match work {
            ActorWork::Root(root) => !root.displayed,
            ActorWork::Compact { .. } => true,
            ActorWork::Turn(_) | ActorWork::Control(_) => false,
        }
    }

    /// Removes the `visible_index`-th item in panel order and returns its
    /// projection plus the work. `None` when the panel has fewer rows.
    pub fn remove_visible_at(&self, visible_index: usize) -> Option<(ActorWork, QueueProjection)> {
        let mut items = lock(&self.items);
        let mut seen = 0usize;
        let mut target = None;
        for (i, w) in items.iter().enumerate() {
            if Self::is_visible(w) {
                if seen == visible_index {
                    target = Some(i);
                    break;
                }
                seen += 1;
            }
        }
        let index = target?;
        let work = items.remove(index)?;
        let projection = (&work).into();
        Some((work, projection))
    }

    pub fn len(&self) -> usize {
        lock(&self.items).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Removes every item and returns them in FIFO order.
    pub fn drain_all(&self) -> Vec<ActorWork> {
        let mut items = lock(&self.items);
        std::mem::take(&mut *items).into()
    }

    /// A neutral snapshot of the queue for the TUI projection.
    pub fn snapshot(&self) -> Vec<QueueProjection> {
        lock(&self.items).iter().map(Into::into).collect()
    }

    /// Runs `publish` under the queue lock, and only when the queue is empty,
    /// so a drain publication can never interleave with a concurrent push.
    pub fn publish_if_empty(&self, publish: impl FnOnce()) {
        let items = lock(&self.items);
        if items.is_empty() {
            publish();
        }
    }

    /// Wakes the runner. Debounced by the bounded channel: at most one token
    /// is pending while the runner drains everything in one pass.
    pub fn notify(&self) {
        let _ = self.notify_tx.try_send(());
    }

    /// Hands the runner its notify receiver. Called exactly once per queue.
    pub(crate) fn take_notify_rx(&self) -> flume::Receiver<()> {
        self.notify_rx
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("actor queue notify receiver taken once")
    }

    /// Extracts the front item only when it is interrupt-compatible. Roots
    /// fold into the active turn as `Interrupt`; compacts become `Compact`.
    /// Turns and controls are never popped here, so interrupt polling cannot
    /// discard incompatible FIFO entries.
    pub(crate) fn pop_interrupt(&self) -> Option<ExtractedCommand> {
        let mut items = lock(&self.items);
        match items.front() {
            Some(ActorWork::Root(_)) | Some(ActorWork::Compact { .. }) => {}
            _ => return None,
        }
        match items.pop_front()? {
            ActorWork::Root(root) => Some(ExtractedCommand::Interrupt(root.input, root.run_id)),
            ActorWork::Compact { run_id } => Some(ExtractedCommand::Compact(run_id)),
            _ => unreachable!("front was matched as root or compact"),
        }
    }
}

impl Default for ActorQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// A scheduler-side view of the queue, usable as the running agent's
/// [`InterruptSource`](crate::InterruptSource) so roots fold into the active
/// turn and compacts are handled between model turns.
#[derive(Clone)]
pub struct InterruptQueue {
    queue: Arc<ActorQueue>,
}

impl InterruptQueue {
    pub(crate) fn new(queue: Arc<ActorQueue>) -> Self {
        Self { queue }
    }
}

impl crate::InterruptSource for InterruptQueue {
    fn poll(&self) -> Option<ExtractedCommand> {
        self.queue.pop_interrupt()
    }
}
