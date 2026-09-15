use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Coalescing work channel: at most one item is in flight. Submitted work
/// replaces pending items when `superseded_by` permits it; otherwise it queues
/// in order. By default every pending item is replaced (latest wins),
/// but callers can preserve terminal events from distinct work families.
pub(crate) struct CoalescedLatest<T> {
    inner: Arc<Inner<T>>,
}

struct Inner<T> {
    state: Mutex<State<T>>,
    dispatch: Box<dyn Fn(CoalescedWork<T>) -> bool + Send + Sync>,
    superseded_by: fn(&T, &T) -> bool,
}

struct State<T> {
    active: bool,
    pending: VecDeque<T>,
    closed: bool,
}

pub(crate) struct CoalescedWork<T> {
    value: Option<T>,
    inner: Arc<Inner<T>>,
}

impl<T> CoalescedLatest<T> {
    pub(crate) fn new(dispatch: impl Fn(CoalescedWork<T>) -> bool + Send + Sync + 'static) -> Self {
        Self::with_supersede(dispatch, |_, _| true)
    }

    pub(crate) fn with_supersede(
        dispatch: impl Fn(CoalescedWork<T>) -> bool + Send + Sync + 'static,
        superseded_by: fn(&T, &T) -> bool,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    active: false,
                    pending: VecDeque::new(),
                    closed: false,
                }),
                dispatch: Box::new(dispatch),
                superseded_by,
            }),
        }
    }

    pub(crate) fn submit(&self, value: T) -> bool {
        let mut state = self.inner.state.lock().unwrap();
        if state.closed {
            return false;
        }
        if state.active {
            state
                .pending
                .retain(|pending| !(self.inner.superseded_by)(pending, &value));
            state.pending.push_back(value);
            return true;
        }
        state.active = true;
        drop(state);
        let work = CoalescedWork {
            value: Some(value),
            inner: Arc::clone(&self.inner),
        };
        if (self.inner.dispatch)(work) {
            true
        } else {
            let mut state = self.inner.state.lock().unwrap();
            state.closed = true;
            state.active = false;
            state.pending.clear();
            false
        }
    }
}

impl<T> Clone for CoalescedLatest<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> CoalescedWork<T> {
    /// True when a pending item supersedes this one: the handler should
    /// skip it and the pending item takes over. Check before `finish`;
    /// once finished the value is taken and this panics.
    pub(crate) fn is_superseded(&self) -> bool {
        let state = self.inner.state.lock().unwrap();
        state
            .pending
            .iter()
            .any(|pending| (self.inner.superseded_by)(self.value(), pending))
    }

    pub(crate) fn value(&self) -> &T {
        self.value.as_ref().unwrap()
    }

    pub(crate) fn finish(mut self, deliver: impl FnOnce(T)) {
        let value = self.value.take().unwrap();
        let mut state = self.inner.state.lock().unwrap();
        let next = state.pending.pop_front();
        if next.is_none() {
            state.active = false;
        }
        drop(state);

        if let Some(next) = next {
            drop(value);
            let work = Self {
                value: Some(next),
                inner: Arc::clone(&self.inner),
            };
            if !(self.inner.dispatch)(work) {
                self.close();
            }
        } else {
            deliver(value);
        }
    }

    fn close(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.closed = true;
        state.active = false;
        state.pending.clear();
    }
}

impl<T> Drop for CoalescedWork<T> {
    fn drop(&mut self) {
        if self.value.is_some() {
            self.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_pending_replaces_older_and_stale_active_is_suppressed() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::new(move |work| tx.send(work).is_ok());

        assert!(latest.submit(1));
        assert!(latest.submit(2));
        assert!(latest.submit(3));
        let first = rx.recv().unwrap();
        let mut delivered = Vec::new();
        first.finish(|value| delivered.push(value));
        let last = rx.recv().unwrap();
        last.finish(|value| delivered.push(value));

        assert_eq!(delivered, vec![3]);
        assert!(rx.try_recv().is_err());
    }

    /// Models the runtime handler contract: run the work unless a pending
    /// item supersedes it, then hand the pending item off via `finish`.
    fn deliver_unless_superseded(work: CoalescedWork<u32>, delivered: &mut Vec<u32>) {
        if !work.is_superseded() {
            delivered.push(*work.value());
        }
        work.finish(drop);
    }

    #[test]
    fn superseding_pending_suppresses_active() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::with_supersede(
            move |work| tx.send(work).is_ok(),
            |this: &u32, pending: &u32| pending > this,
        );

        assert!(latest.submit(1));
        let first = rx.recv().unwrap();
        assert!(!first.is_superseded());
        assert!(latest.submit(2));
        assert!(first.is_superseded());
        let mut delivered = Vec::new();
        deliver_unless_superseded(first, &mut delivered);
        let second = rx.recv().unwrap();
        deliver_unless_superseded(second, &mut delivered);

        assert_eq!(delivered, vec![2]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn non_superseding_pending_still_runs_in_order() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::with_supersede(
            move |work| tx.send(work).is_ok(),
            |this: &u32, pending: &u32| pending > this,
        );

        assert!(latest.submit(2));
        let first = rx.recv().unwrap();
        assert!(!first.is_superseded());
        assert!(latest.submit(1));
        assert!(!first.is_superseded());
        let mut delivered = Vec::new();
        deliver_unless_superseded(first, &mut delivered);
        let second = rx.recv().unwrap();
        deliver_unless_superseded(second, &mut delivered);

        assert_eq!(delivered, vec![2, 1]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn multiple_non_superseding_items_run_in_order() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::with_supersede(
            move |work| tx.send(work).is_ok(),
            |this: &u32, pending: &u32| pending > this,
        );

        assert!(latest.submit(4));
        let first = rx.recv().unwrap();
        assert!(latest.submit(3));
        assert!(latest.submit(2));
        assert!(latest.submit(1));
        let mut delivered = Vec::new();
        deliver_unless_superseded(first, &mut delivered);
        for _ in 0..3 {
            let work = rx.recv().unwrap();
            deliver_unless_superseded(work, &mut delivered);
        }

        assert_eq!(delivered, vec![4, 3, 2, 1]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn superseding_item_replaces_matching_work_across_unrelated_queue_entries() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::with_supersede(
            move |work| tx.send(work).is_ok(),
            |this: &u32, pending: &u32| this % 2 == pending % 2,
        );

        assert!(latest.submit(2));
        let first = rx.recv().unwrap();
        assert!(latest.submit(1));
        assert!(latest.submit(4));
        assert!(first.is_superseded());
        let mut delivered = Vec::new();
        deliver_unless_superseded(first, &mut delivered);
        for _ in 0..2 {
            let work = rx.recv().unwrap();
            deliver_unless_superseded(work, &mut delivered);
        }

        assert_eq!(delivered, vec![1, 4]);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn clones_share_active_and_pending_slots() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::new(move |work| tx.send(work).is_ok());
        let clone = latest.clone();

        assert!(latest.submit(1));
        assert!(clone.submit(2));
        assert_eq!(rx.len(), 1);
        rx.recv().unwrap().finish(drop);
        assert_eq!(*rx.recv().unwrap().value(), 2);
    }

    #[test]
    fn dropped_active_work_closes_transport_and_drops_pending() {
        let (tx, rx) = flume::unbounded();
        let latest = CoalescedLatest::new(move |work| tx.send(work).is_ok());

        assert!(latest.submit(1));
        assert!(latest.submit(2));
        drop(rx.recv().unwrap());

        assert!(!latest.submit(3));
        assert!(rx.try_recv().is_err());
    }
}
