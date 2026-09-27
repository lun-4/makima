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
    admission_event: event_listener::Event,
    pause_event: event_listener::Event,
}

struct QueueState {
    items: VecDeque<ActorWork>,
    paused: bool,
    admitting: bool,
}

impl ActorQueue {
    pub fn new() -> Self {
        let (notify_tx, notify_rx) = flume::bounded::<()>(1);
        Self {
            state: Mutex::new(QueueState {
                items: VecDeque::new(),
                paused: false,
                admitting: false,
            }),
            notify_tx,
            notify_rx: Mutex::new(Some(notify_rx)),
            admission_event: event_listener::Event::new(),
            pause_event: event_listener::Event::new(),
        }
    }

    /// Pushes `work` at the back and wakes the runner.
    pub fn push(&self, work: ActorWork) {
        lock(&self.state).items.push_back(work);
        self.notify();
    }

    pub(crate) fn finish_admission(&self) {
        let mut state = lock(&self.state);
        state.admitting = false;
        self.admission_event.notify(usize::MAX);
    }

    pub(crate) fn defer(&self, work: ActorWork) {
        let mut state = lock(&self.state);
        state.items.push_front(work);
        state.admitting = false;
        self.admission_event.notify(usize::MAX);
        drop(state);
        self.notify();
    }

    pub(crate) async fn wait_for_admission(&self) {
        loop {
            let listener = self.admission_event.listen();
            if !lock(&self.state).admitting {
                return;
            }
            listener.await;
        }
    }

    pub(crate) fn paused(&self) -> bool {
        lock(&self.state).paused
    }

    pub(crate) fn pause_listener(&self) -> event_listener::EventListener {
        self.pause_event.listen()
    }

    /// Pauses or resumes consumption without changing queued work. Removals and
    /// lifecycle drains remain available while paused.
    pub fn set_paused(&self, paused: bool) {
        let mut state = lock(&self.state);
        state.paused = paused;
        if paused {
            self.pause_event.notify(usize::MAX);
        }
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
        let first = state.items.pop_front()?;
        state.admitting = true;
        let items = &mut state.items;
        match first {
            ActorWork::Root(root) => {
                let key = crate::batch_key(&root.input);
                if key.is_none()
                    || !root.earlier.is_empty()
                    || !root.input.preamble.is_empty()
                    || (root.input.message.is_empty() && root.input.images.is_empty())
                {
                    return Some(ActorWork::Root(root));
                }
                let mut roots = vec![root];
                while items.front().and_then(|w| match w {
                    ActorWork::Root(r)
                        if r.earlier.is_empty()
                            && r.input.preamble.is_empty()
                            && (!r.input.message.is_empty() || !r.input.images.is_empty()) =>
                    {
                        crate::batch_key(&r.input)
                    }
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
                    earlier.push(EarlierRoot {
                        thinking: r.input.thinking,
                        fast: r.input.fast,
                        run_id: r.run_id,
                        displayed: r.displayed,
                        text: r.text,
                        images: r.images,
                        correlation: r.correlation,
                    });
                    inputs.push(r.input);
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
        for (index, work) in items.iter().enumerate() {
            if let ActorWork::Root(root) = work {
                for (earlier_index, earlier) in root.earlier.iter().enumerate() {
                    if !earlier.displayed {
                        if seen == visible_index {
                            target = Some((index, Some(earlier_index)));
                            break;
                        }
                        seen += 1;
                    }
                }
            }
            if target.is_some() {
                break;
            }
            if Self::is_visible(work) {
                if seen == visible_index {
                    target = Some((index, None));
                    break;
                }
                seen += 1;
            }
        }
        let (index, earlier_index) = target?;
        let work = if let Some(earlier_index) = earlier_index {
            let Some(ActorWork::Root(root)) = items.remove(index) else {
                unreachable!("earlier root belongs to root work");
            };
            let mut roots = Self::split_root(root);
            let removed = ActorWork::Root(roots.remove(earlier_index));
            for (offset, root) in roots.into_iter().enumerate() {
                items.insert(index + offset, ActorWork::Root(root));
            }
            removed
        } else if matches!(items.get(index), Some(ActorWork::Root(root)) if !root.earlier.is_empty())
        {
            let Some(ActorWork::Root(root)) = items.remove(index) else {
                unreachable!("matched root work");
            };
            let mut roots = Self::split_root(root);
            let removed = ActorWork::Root(roots.pop().expect("batched root"));
            for (offset, root) in roots.into_iter().enumerate() {
                items.insert(index + offset, ActorWork::Root(root));
            }
            removed
        } else {
            items.remove(index)?
        };
        let projection = (&work).into();
        Some((work, projection))
    }

    fn split_root(mut root: RootWork) -> Vec<RootWork> {
        let mut roots = Vec::with_capacity(root.earlier.len() + 1);
        let mut preamble = std::mem::take(&mut root.input.preamble).into_iter();
        for earlier in root.earlier.drain(..) {
            let message = preamble.next().expect("batched root input");
            let mut text = String::new();
            let mut images = Vec::new();
            for block in message.content {
                match block {
                    maki_providers::ContentBlock::Text { text: content } => text = content,
                    maki_providers::ContentBlock::Image { source } => images.push(source),
                    _ => {}
                }
            }
            let input = crate::AgentInput {
                message: text,
                images,
                mode: root.input.mode.clone(),
                preamble: Vec::new(),
                thinking: earlier.thinking,
                fast: earlier.fast,
                workflow: root.input.workflow,
                prompt: None,
                cancel: None,
                lease_committer: None,
            };
            roots.push(RootWork::new(
                input,
                earlier.run_id,
                earlier.displayed,
                earlier.text,
                earlier.images,
                earlier.correlation,
            ));
        }
        roots.push(root);
        roots
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
                ActorWork::Root(root) => {
                    taken.extend(
                        Self::split_root(root)
                            .into_iter()
                            .map(|root| QueuedUiWork::Root(Box::new(root))),
                    );
                }
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

    /// Takes admitted turns while paused, leaving all other work in place.
    pub(crate) fn take_paused_turns(&self) -> Vec<TurnAdmission> {
        let mut state = lock(&self.state);
        if !state.paused {
            return Vec::new();
        }
        let mut kept = VecDeque::new();
        let mut turns = Vec::new();
        while let Some(work) = state.items.pop_front() {
            match work {
                ActorWork::Turn(admission) => turns.push(admission),
                other => kept.push_back(other),
            }
        }
        state.items = kept;
        turns
    }

    /// Removes every item and returns them in FIFO order.
    pub fn drain_all(&self) -> Vec<ActorWork> {
        let mut state = lock(&self.state);
        let items = &mut state.items;
        std::mem::take(&mut *items).into()
    }

    /// A neutral snapshot of the queue for the TUI projection.
    pub fn snapshot(&self) -> Vec<QueueProjection> {
        lock(&self.state)
            .items
            .iter()
            .flat_map(|work| {
                let mut projected = Vec::new();
                if let ActorWork::Root(root) = work {
                    projected.extend(root.earlier.iter().map(|earlier| QueueProjection::Message {
                        text: earlier.text.clone(),
                        image_count: earlier.images.len(),
                        displayed: earlier.displayed,
                    }));
                }
                projected.push(work.into());
                projected
            })
            .collect()
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
    use test_case::test_case;

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
    fn paused_batched_roots_transfer_in_fifo_with_original_inputs() {
        const FIRST: &str = "first prompt";
        const SECOND: &str = "second prompt";
        const THIRD: &str = "";
        let queue = ActorQueue::new();
        let image = test_image();
        for (mut root, thinking, fast) in [
            (
                test_root(FIRST, 1, vec![image.clone()]),
                crate::ThinkingConfig::Budget(128),
                true,
            ),
            (
                test_root(SECOND, 2, Vec::new()),
                crate::ThinkingConfig::Adaptive,
                false,
            ),
            (
                test_root(THIRD, 3, vec![image.clone()]),
                crate::ThinkingConfig::Off,
                true,
            ),
        ] {
            root.input.thinking = thinking;
            root.input.fast = fast;
            queue.push(ActorWork::Root(RootWork {
                displayed: false,
                ..root
            }));
        }
        let batched = queue.pop().expect("batched roots");
        queue.set_paused(true);
        queue.defer(batched);
        queue.push(ActorWork::Compact {
            run_id: 4,
            instructions: None,
        });
        let projected = queue.snapshot();
        assert_eq!(projected.len(), 4);
        for (entry, text) in projected.iter().zip([FIRST, SECOND, THIRD]) {
            assert!(
                matches!(entry, QueueProjection::Message { text: projected, displayed: false, .. } if projected == text)
            );
        }

        let taken = queue.take_paused_ui_work();
        assert_eq!(taken.len(), 4);
        let mut taken = taken.into_iter();
        for (text, run_id, thinking, fast) in [
            (FIRST, 1, crate::ThinkingConfig::Budget(128), true),
            (SECOND, 2, crate::ThinkingConfig::Adaptive, false),
            (THIRD, 3, crate::ThinkingConfig::Off, true),
        ] {
            let work = taken.next().expect("root work");
            let QueuedUiWork::Root(root) = work else {
                panic!("expected root");
            };
            assert_eq!(root.text, text);
            assert_eq!(root.input.message, text);
            assert_eq!(root.run_id, run_id);
            assert_eq!(root.input.thinking, thinking);
            assert_eq!(root.input.fast, fast);
            assert!(!root.displayed);
            assert_eq!(root.correlation, format!("r{run_id}"));
            assert!(root.input.preamble.is_empty());
            assert!(root.earlier.is_empty());
            assert_eq!(root.images, root.input.images);
            if run_id == 1 || run_id == 3 {
                assert_eq!(root.input.images.as_slice(), std::slice::from_ref(&image));
            } else {
                assert!(root.input.images.is_empty());
            }
        }
        assert!(matches!(
            taken.next(),
            Some(QueuedUiWork::Compact { run_id: 4, .. })
        ));
        assert!(taken.next().is_none());
        assert!(queue.is_empty());
    }

    #[test_case(0, &["second prompt", "third prompt"] ; "remove_first")]
    #[test_case(1, &["first prompt", "third prompt"] ; "remove_middle")]
    #[test_case(2, &["first prompt", "second prompt"] ; "remove_last")]
    fn remove_visible_batched_row_preserves_other_prompts(
        removed_index: usize,
        remaining: &[&str],
    ) {
        const PROMPTS: [&str; 3] = ["first prompt", "second prompt", "third prompt"];
        let queue = ActorQueue::new();
        for (index, text) in PROMPTS.iter().enumerate() {
            queue.push(ActorWork::Root(RootWork {
                displayed: false,
                ..test_root(text, index as u64 + 1, Vec::new())
            }));
        }
        let batch = queue.pop().expect("batched roots");
        queue.defer(batch);
        queue.push(ActorWork::Compact {
            run_id: 4,
            instructions: None,
        });
        let (work, projection) = queue
            .remove_visible_at(removed_index)
            .expect("visible prompt");
        assert!(
            matches!(work, ActorWork::Root(root) if root.input.message == PROMPTS[removed_index])
        );
        assert!(
            matches!(projection, QueueProjection::Message { text, .. } if text == PROMPTS[removed_index])
        );
        let projected = queue.snapshot();
        let texts: Vec<_> = projected
            .iter()
            .filter_map(|entry| match entry {
                QueueProjection::Message { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(texts, remaining);
        assert!(matches!(
            projected.last(),
            Some(QueueProjection::Compact(None))
        ));
        queue.set_paused(true);
        let pending = queue.take_paused_ui_work();
        assert_eq!(pending.len(), 3);
        for (work, text) in pending.into_iter().zip(remaining.iter()) {
            let QueuedUiWork::Root(root) = work else {
                panic!("expected root");
            };
            assert_eq!(&root.input.message, text);
        }
    }

    #[test]
    fn remove_visible_batched_row_skips_hidden_roots_and_keeps_images() {
        let queue = ActorQueue::new();
        let image = test_image();
        for (index, text) in ["hidden", "visible", "last"].iter().enumerate() {
            queue.push(ActorWork::Root(RootWork {
                displayed: index == 0,
                ..test_root(text, index as u64 + 1, vec![image.clone()])
            }));
        }
        let batch = queue.pop().expect("batched roots");
        queue.defer(batch);
        let (work, projection) = queue.remove_visible_at(1).expect("last visible root");
        assert!(
            matches!(projection, QueueProjection::Message { text, image_count: 1, .. } if text == "last")
        );
        assert!(matches!(work, ActorWork::Root(root) if root.input.message == "last"));
        assert!(queue.remove_visible_at(1).is_none());
        queue.set_paused(true);
        let pending = queue.take_paused_ui_work();
        assert_eq!(pending.len(), 2);
        for (work, (text, displayed)) in pending
            .into_iter()
            .zip([("hidden", true), ("visible", false)])
        {
            let QueuedUiWork::Root(root) = work else {
                panic!("expected root");
            };
            assert_eq!(root.input.message, text);
            assert_eq!(root.input.images, vec![image.clone()]);
            assert_eq!(root.displayed, displayed);
        }
    }

    #[test]
    fn split_batch_restores_each_prompts_preferences() {
        let queue = ActorQueue::new();
        for (index, (thinking, fast)) in [
            (crate::ThinkingConfig::Budget(128), true),
            (crate::ThinkingConfig::Adaptive, false),
            (crate::ThinkingConfig::Off, true),
        ]
        .into_iter()
        .enumerate()
        {
            let mut root = test_root(&format!("prompt {index}"), index as u64, Vec::new());
            root.input.thinking = thinking;
            root.input.fast = fast;
            root.displayed = false;
            queue.push(ActorWork::Root(root));
        }
        let batch = queue.pop().expect("batch");
        queue.defer(batch);
        let (removed, _) = queue.remove_visible_at(1).expect("middle prompt");
        assert!(
            matches!(removed, ActorWork::Root(root) if root.input.thinking == crate::ThinkingConfig::Adaptive && !root.input.fast)
        );
        queue.set_paused(true);
        let remaining = queue.take_paused_ui_work();
        for (work, (thinking, fast)) in remaining.into_iter().zip([
            (crate::ThinkingConfig::Budget(128), true),
            (crate::ThinkingConfig::Off, true),
        ]) {
            let QueuedUiWork::Root(root) = work else {
                panic!("expected root");
            };
            assert_eq!(root.input.thinking, thinking);
            assert_eq!(root.input.fast, fast);
        }
    }

    #[test]
    fn deferred_batch_is_not_batched_again() {
        const FIRST: &str = "first prompt";
        const SECOND: &str = "second prompt";
        const THIRD: &str = "third prompt";
        let queue = ActorQueue::new();
        queue.push(ActorWork::Root(test_root(FIRST, 1, Vec::new())));
        queue.push(ActorWork::Root(test_root(SECOND, 2, Vec::new())));
        let batched = queue.pop().expect("batched roots");
        queue.defer(batched);
        queue.push(ActorWork::Root(test_root(THIRD, 3, Vec::new())));
        let Some(ActorWork::Root(first)) = queue.pop() else {
            panic!("expected deferred batch");
        };
        assert_eq!(first.earlier.len(), 1);
        assert_eq!(first.input.preamble[0].user_text(), Some(FIRST));
        assert_eq!(first.input.message, SECOND);
        queue.finish_admission();
        let Some(ActorWork::Root(next)) = queue.pop() else {
            panic!("expected following root");
        };
        assert_eq!(next.input.message, THIRD);
        assert!(next.earlier.is_empty());
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
