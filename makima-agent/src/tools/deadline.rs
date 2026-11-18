//! The time a call has to finish by. Waiting on the user, like a permission
//! prompt, does not count as running, so [`Deadline::paused_while`] moves the
//! deadline later by however long the wait took.
//!
//! Clones and derived deadlines share their constituent budgets, so pauses in
//! nested calls also suspend the budgets inherited from their callers.

use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

pub(crate) const DEADLINE_EXCEEDED: &str = "timeout exceeded";

#[derive(Clone, Debug, Default)]
pub struct Deadline(Vec<Arc<Mutex<DeadlineState>>>);

#[derive(Debug)]
struct DeadlineState {
    at: Instant,
    pause_started: Option<Instant>,
    pauses: usize,
}

impl DeadlineState {
    fn instant(&self, now: Instant) -> Instant {
        self.at
            + self.pause_started.map_or(Duration::ZERO, |started| {
                now.saturating_duration_since(started)
            })
    }

    fn pause(&mut self, now: Instant) {
        if self.pauses == 0 {
            self.pause_started = Some(now);
        }
        self.pauses += 1;
    }

    fn resume(&mut self, now: Instant) {
        self.pauses -= 1;
        if self.pauses == 0
            && let Some(started) = self.pause_started.take()
        {
            self.at += now.saturating_duration_since(started);
        }
    }
}

impl From<Option<Instant>> for Deadline {
    fn from(at: Option<Instant>) -> Self {
        Self(
            at.into_iter()
                .map(|at| {
                    Arc::new(Mutex::new(DeadlineState {
                        at,
                        pause_started: None,
                        pauses: 0,
                    }))
                })
                .collect(),
        )
    }
}

impl Deadline {
    pub fn after(budget: Duration) -> Self {
        Self::at(Instant::now() + budget)
    }

    pub fn at(instant: Instant) -> Self {
        Self::from(Some(instant))
    }

    /// The current effective deadline, including elapsed time in an active pause.
    pub fn instant(&self) -> Option<Instant> {
        self.0
            .iter()
            .map(|state| lock(state).instant(Instant::now()))
            .min()
    }

    /// Combines shared budgets without snapshotting their current instants.
    /// Pausing the result pauses each constituent budget, including ancestors.
    pub fn min(&self, other: &Self) -> Self {
        let mut states = self.0.clone();
        for state in &other.0 {
            if !states.iter().any(|existing| Arc::ptr_eq(existing, state)) {
                states.push(Arc::clone(state));
            }
        }
        Self(states)
    }

    /// Only wrap waits on the user: real work should still spend the budget.
    pub async fn paused_while<F: Future>(&self, wait: F) -> F::Output {
        let _pause = self.pause();
        wait.await
    }

    /// Holds the deadline still until the guard drops, then pushes it back by
    /// however long the hold lasted.
    pub(crate) fn pause(&self) -> Pause<'_> {
        for state in &self.0 {
            lock(state).pause(Instant::now());
        }
        Pause { deadline: self }
    }

    #[cfg(test)]
    fn pause_at(&self, now: Instant) {
        for state in &self.0 {
            lock(state).pause(now);
        }
    }

    #[cfg(test)]
    fn resume_at(&self, now: Instant) {
        for state in &self.0 {
            lock(state).resume(now);
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

fn lock(state: &Mutex<DeadlineState>) -> MutexGuard<'_, DeadlineState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A pause in progress. The deadline advances while any pause is active.
pub(crate) struct Pause<'a> {
    deadline: &'a Deadline,
}

impl Drop for Pause<'_> {
    fn drop(&mut self) {
        for state in &self.deadline.0 {
            lock(state).resume(Instant::now());
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

    #[test_case(false ; "nested")]
    #[test_case(true ; "overlapping")]
    fn concurrent_pauses_count_only_the_union(overlapping: bool) {
        let start = Instant::now();
        let mut state = DeadlineState {
            at: start + BUDGET,
            pause_started: None,
            pauses: 0,
        };
        let second = start + Duration::from_secs(1);
        let first_end = start + Duration::from_secs(2);
        let end = start + Duration::from_secs(3);

        state.pause(start);
        state.pause(second);
        assert_eq!(
            state.instant(first_end),
            start + BUDGET + Duration::from_secs(2)
        );
        state.resume(first_end);
        assert_eq!(state.instant(end), start + BUDGET + Duration::from_secs(3));
        state.resume(end);
        assert_eq!(state.instant(end), start + BUDGET + Duration::from_secs(3));

        let deadline = Deadline::after(BUDGET);
        let clone = deadline.clone();
        let first = deadline.pause();
        let second = clone.pause();
        if overlapping {
            drop(first);
            assert_eq!(lock(&deadline.0[0]).pauses, 1);
            drop(second);
        } else {
            drop(second);
            assert_eq!(lock(&deadline.0[0]).pauses, 1);
            drop(first);
        }
        assert_eq!(lock(&deadline.0[0]).pauses, 0);
    }

    #[test]
    fn active_pause_freezes_remaining_time_and_expired_stays_expired() {
        let now = Instant::now();
        let deadline = Deadline::at(now + BUDGET);
        {
            let mut state = lock(&deadline.0[0]);
            state.pause(now - BUDGET);
        }
        assert!(deadline.remaining().unwrap() > BUDGET + BUDGET / 2);
        lock(&deadline.0[0]).resume(Instant::now());

        let expired = Deadline::at(now - BUDGET);
        let guard = expired.pause();
        assert_eq!(expired.check().unwrap_err(), DEADLINE_EXCEEDED);
        drop(guard);
        assert_eq!(expired.check().unwrap_err(), DEADLINE_EXCEEDED);
    }

    #[test]
    fn separate_pauses_do_not_count_the_gap() {
        let start = Instant::now();
        let mut state = DeadlineState {
            at: start + BUDGET,
            pause_started: None,
            pauses: 0,
        };
        state.pause(start);
        state.resume(start + Duration::from_secs(1));
        state.pause(start + Duration::from_secs(3));
        state.resume(start + Duration::from_secs(4));
        assert_eq!(
            state.instant(start + Duration::from_secs(4)),
            start + BUDGET + Duration::from_secs(2)
        );
    }

    #[test]
    fn derived_pause_propagates_to_all_ancestors_and_respects_each_budget() {
        let start = Instant::now();
        let parent = Deadline::at(start + BUDGET);
        let local = Deadline::at(start + BUDGET / 2);
        let child = parent.min(&local);
        let grandchild_limit = Deadline::at(start + BUDGET * 2);
        let grandchild = child.min(&grandchild_limit);
        let sibling = parent.min(&Deadline::at(start + BUDGET * 3));
        let end = start + Duration::from_secs(5);

        grandchild.pause_at(start);
        grandchild.resume_at(end);

        assert_eq!(
            parent.instant(),
            Some(start + BUDGET + Duration::from_secs(5))
        );
        assert_eq!(
            local.instant(),
            Some(start + BUDGET / 2 + Duration::from_secs(5))
        );
        assert_eq!(child.instant(), local.instant());
        assert_eq!(grandchild.instant(), local.instant());
        assert_eq!(sibling.instant(), parent.instant());
    }

    #[test]
    fn overlapping_parent_and_child_pauses_are_not_double_counted() {
        let start = Instant::now();
        let parent = Deadline::at(start + BUDGET);
        let limit = Deadline::at(start + BUDGET * 2);
        let child = parent.min(&limit).min(&parent);
        let second = start + Duration::from_secs(1);
        let third = start + Duration::from_secs(2);
        let end = start + Duration::from_secs(3);

        parent.pause_at(start);
        child.pause_at(second);
        parent.resume_at(third);
        child.resume_at(end);

        assert_eq!(
            parent.instant(),
            Some(start + BUDGET + Duration::from_secs(3))
        );
        assert_eq!(
            limit.instant(),
            Some(start + BUDGET * 2 + Duration::from_secs(2))
        );
        assert_eq!(child.instant(), parent.instant());
    }

    #[test]
    fn min_with_unbounded_deadline_keeps_parent_shared() {
        let start = Instant::now();
        let parent = Deadline::at(start + BUDGET);
        let child = Deadline::default().min(&parent).min(&Deadline::default());
        child.pause_at(start);
        child.resume_at(start + Duration::from_secs(1));
        assert_eq!(
            parent.instant(),
            Some(start + BUDGET + Duration::from_secs(1))
        );
        assert_eq!(
            Deadline::default().min(&Deadline::default()).instant(),
            None
        );
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
