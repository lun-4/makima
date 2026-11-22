//! Whether a logical request reached the provider's server, reported while
//! the stream still runs, so lineage and cleanup only ever name a
//! conversation the server holds.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use flume::{Receiver, Sender};

/// Who decides when a request counts as delivered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RequestDeliveryMarker {
    /// The first accepted event, an explicit mark or a successful return.
    #[default]
    Auto,
    /// Only an explicit mark or a successful return.
    Manual,
}

#[derive(Debug, Default)]
struct RequestDeliveryState {
    delivered: bool,
    open_attempt: Option<u32>,
}

#[derive(Debug)]
struct RequestDeliveryShared {
    state: Mutex<RequestDeliveryState>,
    signal: Sender<()>,
}

impl RequestDeliveryShared {
    fn lock(&self) -> MutexGuard<'_, RequestDeliveryState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn deliver(&self, state: &mut RequestDeliveryState) {
        if !state.delivered {
            state.delivered = true;
            let _ = self.signal.try_send(());
        }
    }
}

/// One attempt's handle on the delivery state of its logical request.
#[derive(Debug, Clone)]
pub struct RequestDelivery {
    shared: Arc<RequestDeliveryShared>,
    attempt: u32,
}

impl RequestDelivery {
    /// A handle for a request whose delivery nobody tracks.
    pub fn untracked() -> Self {
        RequestDeliveryTracker::new().attempt(0)
    }

    /// Records delivery. Returns false, recording nothing, once the attempt
    /// has been retired: the caller must not start the request then.
    pub fn mark(&self) -> bool {
        let mut state = self.shared.lock();
        if state.open_attempt != Some(self.attempt) {
            return false;
        }
        self.shared.deliver(&mut state);
        true
    }

    pub fn is_delivered(&self) -> bool {
        self.shared.lock().delivered
    }
}

/// The owner side of one logical request: opens and retires attempts, and
/// is told the first time any of them is delivered.
#[derive(Debug)]
pub struct RequestDeliveryTracker {
    shared: Arc<RequestDeliveryShared>,
    delivered: Receiver<()>,
}

impl Default for RequestDeliveryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl RequestDeliveryTracker {
    pub fn new() -> Self {
        let (signal, delivered) = flume::bounded(1);
        Self {
            shared: Arc::new(RequestDeliveryShared {
                state: Mutex::default(),
                signal,
            }),
            delivered,
        }
    }

    /// Opens {attempt}, retiring any earlier one.
    pub fn attempt(&self, attempt: u32) -> RequestDelivery {
        self.shared.lock().open_attempt = Some(attempt);
        RequestDelivery {
            shared: Arc::clone(&self.shared),
            attempt,
        }
    }

    /// Retires the open attempt, so no handle can mark until the next one
    /// opens. A successful attempt counts as delivered even when its
    /// provider never marked: the server answered it.
    pub fn end_attempt(&self, succeeded: bool) {
        let mut state = self.shared.lock();
        if succeeded && state.open_attempt.is_some() {
            self.shared.deliver(&mut state);
        }
        state.open_attempt = None;
    }

    pub fn is_delivered(&self) -> bool {
        self.shared.lock().delivered
    }

    /// Resolves once, on the first delivery of any attempt.
    pub async fn delivered(&self) {
        let _ = self.delivered.recv_async().await;
    }
}

/// An owner dropped mid-attempt (its future cancelled) retires the attempt,
/// so a handle that outlives it cannot record a delivery nobody reads.
impl Drop for RequestDeliveryTracker {
    fn drop(&mut self) {
        self.shared.lock().open_attempt = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_mark_signals_once_and_later_marks_are_no_ops() {
        let tracker = RequestDeliveryTracker::new();
        let delivery = tracker.attempt(0);
        assert!(!tracker.is_delivered());
        assert!(delivery.mark());
        assert!(delivery.mark());
        assert!(tracker.is_delivered());
        assert!(tracker.delivered.try_recv().is_ok());
        assert!(tracker.delivered.try_recv().is_err());
    }

    #[test]
    fn retired_attempt_cannot_mark() {
        let tracker = RequestDeliveryTracker::new();
        let delivery = tracker.attempt(0);
        tracker.end_attempt(false);
        assert!(!delivery.mark());
        assert!(!tracker.is_delivered());
    }

    #[test]
    fn dropped_tracker_retires_its_open_attempt() {
        let tracker = RequestDeliveryTracker::new();
        let delivery = tracker.attempt(0);
        drop(tracker);
        assert!(!delivery.mark());
        assert!(!delivery.is_delivered());
    }

    #[test]
    fn successful_attempt_is_delivered_without_a_mark() {
        let tracker = RequestDeliveryTracker::new();
        let delivery = tracker.attempt(0);
        tracker.end_attempt(true);
        assert!(tracker.is_delivered());
        assert!(tracker.delivered.try_recv().is_ok());
        assert!(!delivery.mark());
    }

    #[test]
    fn earlier_attempt_cannot_mark_after_a_retry_opens() {
        let tracker = RequestDeliveryTracker::new();
        let first = tracker.attempt(0);
        tracker.end_attempt(false);
        let second = tracker.attempt(1);
        assert!(!first.mark());
        assert!(!tracker.is_delivered());
        assert!(second.mark());
        assert!(tracker.is_delivered());
    }

    #[test]
    fn delivery_survives_later_attempts() {
        let tracker = RequestDeliveryTracker::new();
        assert!(tracker.attempt(0).mark());
        tracker.end_attempt(false);
        tracker.attempt(1);
        tracker.end_attempt(false);
        assert!(tracker.is_delivered());
    }
}
