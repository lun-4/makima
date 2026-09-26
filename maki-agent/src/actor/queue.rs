//! The actor's sole FIFO deque.
//!
//! Every push, pop, interrupt extraction, removal, clear, snapshot, and drain
//! goes through this one lock-backed deque. `publish_if_empty` runs its
//! closure under the queue lock, so a drain publication can never interleave
//! with a concurrent push, matching the TUI's expectation.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::types::EarlierRoot;
use super::{ActorWork, QueuedUiWork, RootWork, TurnAdmission};
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
    Compact(Option<String>),
    Control(String),
    Turn(String),
}

impl From<&RootWork> for QueueProjection {
    fn from(root: &RootWork) -> Self {
        Self::Message {
            text: root.text.clone(),
            image_count: root.images.len(),
            displayed: root.displayed,
        }
    }
}

impl From<&ActorWork> for QueueProjection {
    fn from(work: &ActorWork) -> Self {
        match work {
            ActorWork::Root(root) => Self::from(root),
            ActorWork::Compact { instructions, .. } => Self::Compact(instructions.clone()),
            ActorWork::Control(control) => Self::Control(control.name.clone()),
            ActorWork::Turn(admission) => Self::Turn(admission.correlation.clone()),
        }
    }
}

/// One lock-backed FIFO deque of [`ActorWork`]. Shared through `Arc`.
pub struct ActorQueue {
    state: Mutex<QueueState>,
    notify_tx: flume::Sender<()>,
    notify_rx: Mutex<Option<flume::Receiver<()>>>,
}

struct QueueState {
    items: VecDeque<ActorWork>,
    paused: bool,
}

impl ActorQueue {
    pub fn new() -> Self {
        let (notify_tx, notify_rx) = flume::bounded::<()>(1);
        Self {
            state: Mutex::new(QueueState {
                items: VecDeque::new(),
                paused: false,
            }),
            notify_tx,
            notify_rx: Mutex::new(Some(notify_rx)),
        }
    }

    /// Pushes `work` at the back and wakes the runner.
    pub fn push(&self, work: ActorWork) {
        lock(&self.state).items.push_back(work);
        self.notify();
    }

    /// Pauses or resumes consumption without changing queued work. Removals and
    /// lifecycle drains remain available while paused.
    pub fn set_paused(&self, paused: bool) {
        let mut state = lock(&self.state);
        state.paused = paused;
        drop(state);
        if !paused {
            self.notify();
        }
    }

    /// Pops the front item, or `None` when the queue is empty or paused.
    /// Consecutive plain root inputs that share a batch key are condensed into a single turn.
    pub fn pop(&self) -> Option<ActorWork> {
        let mut state = lock(&self.state);
        if state.paused {
            return None;
        }
        let items = &mut state.items;
        let first = items.pop_front()?;
        match first {
            ActorWork::Root(root) => {
                let key = crate::batch_key(&root.input);
                if key.is_none() {
                    return Some(ActorWork::Root(root));
                }
                let mut roots = vec![root];
                while items.front().and_then(|w| match w {
                    ActorWork::Root(r) => crate::batch_key(&r.input),
                    _ => None,
                }) == key
                {
                    let Some(ActorWork::Root(next)) = items.pop_front() else {
                        break;
                    };
                    roots.push(next);
                }
                if roots.len() == 1 {
                    return Some(ActorWork::Root(roots.pop().unwrap()));
                }
                let mut last = roots.pop().unwrap();
                let mut inputs = Vec::with_capacity(roots.len() + 1);
                let mut earlier = Vec::with_capacity(roots.len());
                for r in roots {
                    inputs.push(r.input);
                    earlier.push(EarlierRoot {
                        run_id: r.run_id,
                        displayed: r.displayed,
                        text: r.text,
                        images: r.images,
                        correlation: r.correlation,
                    });
                }
                inputs.push(last.input);
                last.input = crate::merge_inputs(inputs).expect("at least two inputs");
                last.earlier = earlier;
                Some(ActorWork::Root(last))
            }
            other => Some(other),
        }
    }

    /// Extracts the given admitted turn from anywhere in the queue without
    /// disturbing the others. Returns `None` when it is not queued (already
    /// running or already consumed).
    pub fn remove_turn(&self, turn_id: TurnId) -> Option<TurnAdmission> {
        let mut state = lock(&self.state);
        let items = &mut state.items;
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
        let mut state = lock(&self.state);
        let items = &mut state.items;
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
            ActorWork::Compact { run_id, .. } => {
                Some(std::borrow::Cow::Owned(super::run_correlation(*run_id)))
            }
            ActorWork::Control(_) => None,
        }
    }

    /// Removes every queued item whose correlation matches, returning them in
    /// FIFO order. Unrelated items stay untouched.
    pub fn remove_correlation(&self, correlation: &str) -> Vec<ActorWork> {
        let mut state = lock(&self.state);
        let items = &mut state.items;
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
        let mut state = lock(&self.state);
        let items = &mut state.items;
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
        lock(&self.state).items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Takes queued roots and compacts in FIFO order only while paused.
    /// Turns and controls remain queued for their normal lifecycle handling.
    pub(crate) fn take_paused_ui_work(&self) -> Vec<QueuedUiWork> {
        let mut state = lock(&self.state);
        if !state.paused {
            return Vec::new();
        }
        let mut kept = VecDeque::new();
        let mut taken = Vec::new();
        while let Some(work) = state.items.pop_front() {
            match work {
                ActorWork::Root(root) => taken.push(QueuedUiWork::Root(Box::new(root))),
                ActorWork::Compact {
                    run_id,
                    instructions,
                } => taken.push(QueuedUiWork::Compact {
                    run_id,
                    instructions,
                }),
                other => kept.push_back(other),
            }
        }
        state.items = kept;
        taken
    }

    /// Removes every item and returns them in FIFO order.
    pub fn drain_all(&self) -> Vec<ActorWork> {
        let mut state = lock(&self.state);
        let items = &mut state.items;
        std::mem::take(&mut *items).into()
    }

    /// A neutral snapshot of the queue for the TUI projection.
    pub fn snapshot(&self) -> Vec<QueueProjection> {
        lock(&self.state).items.iter().map(Into::into).collect()
    }

    /// Runs `publish` under the queue lock, and only when the queue is empty,
    /// so a drain publication can never interleave with a concurrent push.
    pub fn publish_if_empty(&self, publish: impl FnOnce()) {
        let state = lock(&self.state);
        if state.items.is_empty() {
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

    /// Extracts the front item only when it is interrupt-compatible and the
    /// queue is not paused. Roots fold into the active turn as `Interrupt`;
    /// compacts become `Compact`. Turns and controls stay in the queue.
    pub(crate) fn pop_interrupt(&self) -> Option<ExtractedCommand> {
        let mut state = lock(&self.state);
        if state.paused {
            return None;
        }
        let items = &mut state.items;
        match items.front() {
            Some(ActorWork::Root(_)) | Some(ActorWork::Compact { .. }) => {}
            _ => return None,
        }
        match items.pop_front()? {
            ActorWork::Root(first) => {
                let key = crate::batch_key(&first.input);
                let mut inputs = vec![first.input];
                while key.is_some()
                    && items.front().and_then(|w| match w {
                        ActorWork::Root(r) => crate::batch_key(&r.input),
                        _ => None,
                    }) == key
                {
                    let Some(ActorWork::Root(next)) = items.pop_front() else {
                        break;
                    };
                    inputs.push(next.input);
                }
                Some(ExtractedCommand::Interrupt(inputs))
            }
            ActorWork::Compact { instructions, .. } => {
                Some(ExtractedCommand::Compact(instructions))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentInput, AgentMode, SessionDefaults};
    use maki_providers::{ContentBlock, ImageMediaType, ImageSource};

    fn test_image() -> ImageSource {
        ImageSource::new(ImageMediaType::Png, Arc::from("dGVzdA=="))
    }

    fn test_input(message: &str) -> AgentInput {
        AgentInput::from_defaults(
            message.into(),
            AgentMode::Build,
            Vec::new(),
            SessionDefaults::default(),
        )
    }

    fn test_root(message: &str, run_id: u64, images: Vec<ImageSource>) -> RootWork {
        let mut input = test_input(message);
        input.images = images.clone();
        RootWork::new(
            input,
            run_id,
            true,
            message.into(),
            images,
            format!("r{run_id}"),
        )
    }

    #[test]
    fn test_actor_condenses_consecutive_plain_user_messages() {
        let queue = ActorQueue::new();
        queue.push(ActorWork::Root(test_root("first", 1, Vec::new())));
        queue.push(ActorWork::Root(test_root("second", 2, Vec::new())));
        queue.push(ActorWork::Root(test_root("third", 3, Vec::new())));

        let popped = queue.pop().expect("must pop work");
        let ActorWork::Root(merged) = popped else {
            panic!("expected Root work");
        };

        assert_eq!(merged.input.message, "third");
        assert_eq!(merged.run_id, 3);
        assert_eq!(merged.earlier.len(), 2);
        assert_eq!(merged.earlier[0].text, "first");
        assert_eq!(merged.earlier[0].run_id, 1);
        assert_eq!(merged.earlier[1].text, "second");
        assert_eq!(merged.earlier[1].run_id, 2);

        assert_eq!(merged.input.preamble.len(), 2);
        assert_eq!(merged.input.preamble[0].user_text(), Some("first"));
        assert_eq!(merged.input.preamble[1].user_text(), Some("second"));

        assert!(queue.pop().is_none());
    }

    #[test]
    fn test_actor_preserves_images_in_condensed_burst() {
        let queue = ActorQueue::new();
        let img1 = test_image();
        let img2 = test_image();
        queue.push(ActorWork::Root(test_root("first", 1, vec![img1.clone()])));
        queue.push(ActorWork::Root(test_root("second", 2, vec![img2.clone()])));
        queue.push(ActorWork::Root(test_root("third", 3, Vec::new())));

        let popped = queue.pop().expect("must pop work");
        let ActorWork::Root(merged) = popped else {
            panic!("expected Root work");
        };

        assert_eq!(merged.earlier[0].images.len(), 1);
        assert_eq!(merged.earlier[1].images.len(), 1);
        assert_eq!(merged.input.preamble.len(), 2);

        let img_count = |msg: &maki_providers::Message| {
            msg.content
                .iter()
                .filter(|b| matches!(b, ContentBlock::Image { .. }))
                .count()
        };
        assert_eq!(img_count(&merged.input.preamble[0]), 1);
        assert_eq!(img_count(&merged.input.preamble[1]), 1);
    }

    #[test]
    fn test_actor_does_not_condense_tool_results_or_slash_commands() {
        let queue = ActorQueue::new();
        queue.push(ActorWork::Root(test_root("first", 1, Vec::new())));
        queue.push(ActorWork::Compact {
            run_id: 2,
            instructions: None,
        });
        queue.push(ActorWork::Root(test_root("second", 3, Vec::new())));

        let first_pop = queue.pop().expect("first item");
        let ActorWork::Root(r1) = first_pop else {
            panic!("expected Root");
        };
        assert_eq!(r1.input.message, "first");
        assert_eq!(r1.earlier.len(), 0);

        let second_pop = queue.pop().expect("second item");
        assert!(matches!(second_pop, ActorWork::Compact { run_id: 2, .. }));

        let third_pop = queue.pop().expect("third item");
        let ActorWork::Root(r2) = third_pop else {
            panic!("expected Root");
        };
        assert_eq!(r2.input.message, "second");
        assert_eq!(r2.earlier.len(), 0);
    }
}
