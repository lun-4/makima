//! The actor's sole FIFO deque.
//!
//! Every push, pop, interrupt extraction, removal, clear, snapshot, and drain
//! goes through this one lock-backed deque.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use super::{ActorInner, ActorWork, PendingQueueEntry, TurnAdmission};
use crate::types::TurnId;
use crate::{AgentInput, ExtractedCommand};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Neutral projection of one queued item, enough for the TUI to draw the
/// queue panel without importing UI types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueProjection {
    Compact(Option<String>),
    Control(String),
    Turn(String),
}

impl From<&ActorWork> for QueueProjection {
    fn from(work: &ActorWork) -> Self {
        match work {
            ActorWork::Compact { instructions, .. } => Self::Compact(instructions.clone()),
            ActorWork::Control(control) => Self::Control(control.name.clone()),
            ActorWork::HistoryEdit(_) => Self::Control("rewind".into()),
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
            Some(ActorWork::Turn(admission)) => Some(*admission),
            _ => unreachable!("remove_turn position matched a Turn"),
        }
    }

    pub(super) fn remove_compact(&self, operation_id: u64) -> Option<PendingQueueEntry> {
        let mut items = lock(&self.items);
        let index = items.iter().position(
            |work| matches!(work, ActorWork::Compact { operation_id: id, .. } if *id == operation_id),
        )?;
        match items.remove(index)? {
            ActorWork::Compact {
                run_id,
                instructions,
                ..
            } => Some(PendingQueueEntry::Compact {
                operation_id,
                run_id,
                instructions,
            }),
            _ => None,
        }
    }

    pub(super) fn has_prepared_turn(&self, turn_id: TurnId) -> bool {
        lock(&self.items).iter().any(|work| {
            matches!(work, ActorWork::Turn(admission)
                if admission.turn_id == turn_id && admission.admission.is_some())
        })
    }

    pub fn replace_turn_input(
        &self,
        turn_id: TurnId,
        input: AgentInput,
    ) -> Result<(), Box<AgentInput>> {
        let mut items = lock(&self.items);
        for work in items.iter_mut() {
            if let ActorWork::Turn(admission) = work
                && admission.turn_id == turn_id
            {
                admission.input = Some(input);
                return Ok(());
            }
        }
        Err(Box::new(input))
    }

    /// Turns carry a host correlation; compacts use their canonical `r{run_id}` encoding.
    fn correlation_of(work: &ActorWork) -> Option<Cow<'_, str>> {
        match work {
            ActorWork::Turn(a) => Some(Cow::Borrowed(a.correlation.as_str())),
            ActorWork::Compact { run_id, .. } => Some(Cow::Owned(super::run_correlation(*run_id))),
            ActorWork::Control(_) | ActorWork::HistoryEdit(_) => None,
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

    pub fn len(&self) -> usize {
        lock(&self.items).len()
    }

    pub fn is_empty(&self) -> bool {
        lock(&self.items).is_empty()
    }

    /// Removes every item and returns them in FIFO order.
    pub fn drain_all(&self) -> Vec<ActorWork> {
        let mut items = lock(&self.items);
        std::mem::take(&mut *items).into()
    }

    /// Drains every item `keep` does not select; kept items stay in order.
    pub fn drain_except(&self, keep: impl Fn(&ActorWork) -> bool) -> Vec<ActorWork> {
        let mut items = lock(&self.items);
        let mut drained = Vec::new();
        let mut kept = VecDeque::with_capacity(items.len());
        for work in std::mem::take(&mut *items) {
            if keep(&work) {
                kept.push_back(work);
            } else {
                drained.push(work);
            }
        }
        *items = kept;
        drained
    }

    /// A neutral snapshot of the queue for the TUI projection.
    pub(super) fn project_pending(
        &self,
        messages: &impl Fn(TurnId) -> Vec<PendingQueueEntry>,
    ) -> Vec<PendingQueueEntry> {
        lock(&self.items)
            .iter()
            .flat_map(|work| match work {
                ActorWork::Turn(turn) => messages(turn.turn_id),
                ActorWork::Compact {
                    operation_id,
                    run_id,
                    instructions,
                    ..
                } => vec![PendingQueueEntry::Compact {
                    operation_id: *operation_id,
                    run_id: *run_id,
                    instructions: instructions.clone(),
                }],
                _ => Vec::new(),
            })
            .collect()
    }

    pub fn snapshot(&self) -> Vec<QueueProjection> {
        lock(&self.items).iter().map(Into::into).collect()
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

    /// Extracts a same-policy compaction without crossing ordered work.
    pub(crate) fn pop_interrupt(&self, generation: u64) -> Option<ExtractedCommand> {
        let mut items = lock(&self.items);
        match items.front() {
            Some(ActorWork::Compact {
                generation: compact_generation,
                ..
            }) if *compact_generation == generation => {}
            _ => return None,
        }
        match items.pop_front()? {
            ActorWork::Compact { instructions, .. } => {
                Some(ExtractedCommand::Compact(instructions))
            }
            _ => unreachable!("front was matched as compact"),
        }
    }
}

impl Default for ActorQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// A scheduler-side [`InterruptSource`](crate::InterruptSource) for compaction
/// commands handled between model requests.
#[derive(Clone)]
pub struct InterruptQueue {
    inner: Arc<ActorInner>,
    cancellation_generation: u64,
    policy_generation: u64,
}

impl InterruptQueue {
    pub(crate) fn new(
        inner: Arc<ActorInner>,
        cancellation_generation: u64,
        policy_generation: u64,
    ) -> Self {
        Self {
            inner,
            cancellation_generation,
            policy_generation,
        }
    }
}

impl crate::InterruptSource for InterruptQueue {
    fn poll_ready(&self) -> Pin<Box<dyn Future<Output = Option<ExtractedCommand>> + Send + '_>> {
        Box::pin(async { self.poll() })
    }

    fn poll(&self) -> Option<ExtractedCommand> {
        let state = lock(&self.inner.state);
        (state.cancellation_generation == self.cancellation_generation)
            .then(|| self.inner.queue.pop_interrupt(self.policy_generation))
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::ActorQueue;
    use crate::ExtractedCommand;
    use crate::actor::{ActorWork, ControlWork};
    use crate::types::{ActorWorkIdentity, ActorWorkKind, AgentId};

    const INSTRUCTIONS: &str = "retain decisions";
    const CONTROL: &str = "boundary";

    fn compact(generation: u64) -> ActorWork {
        ActorWork::Compact {
            operation_id: generation,
            run_id: generation,
            instructions: Some(INSTRUCTIONS.into()),
            generation,
            policy: None,
            work: ActorWorkIdentity::new(
                AgentId::generate(),
                ActorWorkKind::Compact {
                    operation_id: generation,
                },
                generation,
            ),
        }
    }

    #[test_case::test_case(1, true; "same_generation")]
    #[test_case::test_case(2, false; "different_generation")]
    fn compaction_interrupt_respects_policy_generation(generation: u64, compatible: bool) {
        let queue = ActorQueue::new();
        queue.push(compact(1));
        let extracted = queue.pop_interrupt(generation);
        assert_eq!(extracted.is_some(), compatible);
        assert_eq!(queue.len(), usize::from(!compatible));
        if let Some(ExtractedCommand::Compact(instructions)) = extracted {
            assert_eq!(instructions.as_deref(), Some(INSTRUCTIONS));
        } else {
            assert!(!compatible);
        }
    }

    #[test]
    fn compaction_interrupt_does_not_cross_control_boundary() {
        let queue = ActorQueue::new();
        queue.push(ActorWork::Control(ControlWork {
            name: CONTROL.into(),
            correlation: CONTROL.into(),
            work: ActorWorkIdentity::new(
                AgentId::generate(),
                ActorWorkKind::Control { operation_id: 0 },
                0,
            ),
            payload: None,
        }));
        queue.push(compact(1));
        assert!(queue.pop_interrupt(1).is_none());
        assert_eq!(queue.len(), 2);
        assert!(matches!(queue.pop(), Some(ActorWork::Control(_))));
        assert!(matches!(
            queue.pop_interrupt(1),
            Some(ExtractedCommand::Compact(_))
        ));
        assert!(queue.is_empty());
    }

    #[test]
    fn compaction_interrupt_preserves_fifo_across_policy_boundary() {
        let queue = ActorQueue::new();
        queue.push(compact(1));
        queue.push(compact(2));
        assert!(queue.pop_interrupt(2).is_none());
        assert!(matches!(
            queue.pop_interrupt(1),
            Some(ExtractedCommand::Compact(_))
        ));
        assert!(queue.pop_interrupt(1).is_none());
        assert!(matches!(
            queue.pop_interrupt(2),
            Some(ExtractedCommand::Compact(_))
        ));
        assert!(queue.is_empty());
    }
}
