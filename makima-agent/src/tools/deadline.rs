//! The time a call has to finish by. Waiting on the user, like a permission
//! prompt, does not count as running, so [`Deadline::paused_while`] moves the
//! deadline later by however long the wait took.
//!
//! All clones share one instant: the output hook chain holds a copy of the
//! context made before the prompt and still has to see the new time.

use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

pub(crate) const DEADLINE_EXCEEDED: &str = "timeout exceeded";

#[derive(Clone, Debug, Default)]
pub struct Deadline(Option<Arc<Mutex<Instant>>>);

impl From<Option<Instant>> for Deadline {
    fn from(at: Option<Instant>) -> Self {
        Self(at.map(|at| Arc::new(Mutex::new(at))))
    }
}

impl Deadline {
    pub fn after(budget: Duration) -> Self {
        Self::at(Instant::now() + budget)
    }

    pub fn at(instant: Instant) -> Self {
        Self::from(Some(instant))
    }

    fn lock(&self) -> Option<MutexGuard<'_, Instant>> {
        self.0
            .as_deref()
            .map(|at| at.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// The absolute instant the call must finish by, if it has one.
    pub fn instant(&self) -> Option<Instant> {
        self.lock().as_deref().copied()
    }

    pub fn min(&self, other: &Self) -> Self {
        match (self.instant(), other.instant()) {
            (Some(left), Some(right)) => Self::at(left.min(right)),
            (Some(at), None) | (None, Some(at)) => Self::at(at),
            (None, None) => Self::default(),
        }
    }

    /// Only wrap waits on the user: real work should still spend the budget.
    pub async fn paused_while<F: Future>(&self, wait: F) -> F::Output {
        let _pause = self.pause();
        wait.await
    }

    /// Holds the deadline still until the guard drops, then pushes it back by
    /// however long the hold lasted.
    pub(crate) fn pause(&self) -> Pause<'_> {
        Pause {
            deadline: self,
            started: Instant::now(),
        }
    }

    fn remaining(&self) -> Option<Duration> {
        self.instant()
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    pub fn check(&self) -> Result<(), String> {
        match self.remaining() {
            Some(left) if left.is_zero() => Err(DEADLINE_EXCEEDED.into()),
            _ => Ok(()),
        }
    }

    pub fn cap_timeout(&self, timeout_secs: u64) -> Result<u64, String> {
        match self.remaining() {
            None => Ok(timeout_secs),
            Some(left) if left.is_zero() => Err(DEADLINE_EXCEEDED.into()),
            Some(left) => Ok(timeout_secs.min(left.as_secs().max(1))),
        }
    }
}

/// A pause in progress. Dropping it moves the deadline back by the time the
/// pause lasted, and every clone sees the move.
pub(crate) struct Pause<'a> {
    deadline: &'a Deadline,
    started: Instant,
}

impl Drop for Pause<'_> {
    fn drop(&mut self) {
        if let Some(mut at) = self.deadline.lock() {
            *at += self.started.elapsed();
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;

    const BUDGET: Duration = Duration::from_secs(60);
    const SHORT_BUDGET: Duration = Duration::from_millis(20);
    const PAST_DEADLINE: Duration = Duration::from_millis(25);

    #[test_case(Deadline::default(), 120, 120 ; "none_passes_through")]
    #[test_case(Deadline::after(BUDGET), 10, 10 ; "requested_under_remaining")]
    fn cap_timeout_ok(deadline: Deadline, requested: u64, expected: u64) {
        assert_eq!(deadline.cap_timeout(requested).unwrap(), expected);
    }

    #[test]
    fn cap_timeout_clamps_to_remaining() {
        let clamped = Deadline::after(BUDGET).cap_timeout(7200).unwrap();
        assert!(clamped <= BUDGET.as_secs(), "got {clamped}");
    }

    #[test]
    fn expired_deadline_is_rejected() {
        let expired = Deadline::after(Duration::ZERO);
        assert_eq!(expired.cap_timeout(120).unwrap_err(), DEADLINE_EXCEEDED);
        assert_eq!(expired.check().unwrap_err(), DEADLINE_EXCEEDED);
    }

    #[test]
    fn a_paused_wait_pushes_every_clone_back_by_its_length() {
        let deadline = Deadline::after(BUDGET);
        let before = deadline.instant().unwrap();
        let clone = deadline.clone();

        smol::block_on(deadline.paused_while(smol::Timer::after(PAST_DEADLINE)));

        assert!(clone.instant().unwrap() - before >= PAST_DEADLINE);
    }

    #[test]
    fn a_wait_outside_a_pause_still_spends_the_budget() {
        let deadline = Deadline::after(SHORT_BUDGET);

        smol::block_on(smol::Timer::after(PAST_DEADLINE));

        assert_eq!(deadline.check().unwrap_err(), DEADLINE_EXCEEDED);
    }

    #[test]
    fn a_pause_that_outlives_the_budget_keeps_the_call_alive() {
        let deadline = Deadline::after(SHORT_BUDGET);

        smol::block_on(deadline.paused_while(smol::Timer::after(PAST_DEADLINE)));

        assert!(
            deadline.check().is_ok(),
            "the wait must not spend the budget"
        );
    }
}
