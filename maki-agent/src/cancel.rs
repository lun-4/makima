//! Cooperative cancellation with parent-to-child propagation.
//!
//! `CancelTrigger` fires on Drop, so cleanup happens even if the trigger is forgotten.
//! `cancelled()` uses a double-check around the listener to close the TOCTOU window between flag read and listener registration.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use event_listener::Event;
use maki_storage::checkpoint::CheckpointPublication;

use crate::types::TurnCancellationReason;

#[derive(Default)]
struct CancellationState {
    publications: Vec<Weak<CheckpointPublication>>,
    children: Vec<Weak<Shared>>,
}

struct Shared {
    cancelled: AtomicBool,
    state: Mutex<CancellationState>,
    publication_gate: Arc<Mutex<()>>,
    event: Event,
    _parent_registration: Option<CancelRegistration>,
}

impl Shared {
    fn cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
            || self
                ._parent_registration
                .as_ref()
                .is_some_and(|registration| registration.source.cancelled())
    }

    fn fire(&self) {
        let _gate = self
            .publication_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.fire_locked();
    }

    fn fire_locked(&self) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        for publication in state.publications.iter().filter_map(Weak::upgrade) {
            publication.cancel();
        }
        let children: Vec<_> = state.children.iter().filter_map(Weak::upgrade).collect();
        drop(state);
        for child in children {
            child.fire_locked();
        }
        self.event.notify(usize::MAX);
    }
}

/// Keeps a request registered with its cancellation source until completion.
#[must_use]
pub struct PublicationRegistration {
    source: Weak<Shared>,
    publication: Arc<CheckpointPublication>,
}

impl PublicationRegistration {
    pub(crate) fn cancel(&self) {
        self.publication.cancel();
    }
}

impl Drop for PublicationRegistration {
    fn drop(&mut self) {
        if let Some(source) = self.source.upgrade() {
            let publication = Arc::downgrade(&self.publication);
            source
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .publications
                .retain(|registered| !registered.ptr_eq(&publication));
        }
    }
}

struct CancelRegistration {
    source: Arc<Shared>,
    child: Weak<Shared>,
}

impl Drop for CancelRegistration {
    fn drop(&mut self) {
        self.source
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .children
            .retain(|registered| !registered.ptr_eq(&self.child));
    }
}

/// Fires once, from the first trigger to drop, then keeps the winning reason.
struct ReasonedShared {
    cancelled: AtomicBool,
    reason: Mutex<Option<TurnCancellationReason>>,
    event: Event,
}

impl ReasonedShared {
    fn fire(&self, reason: TurnCancellationReason) {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        *self.reason.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
        self.event.notify(usize::MAX);
    }
}

#[derive(Clone)]
pub struct CancelToken(Arc<Shared>);

pub struct CancelTrigger(Arc<Shared>);

/// Single-fire cancellation that records the first reason to win.
///
/// Dropping any trigger fires the shared flag; the first reason stored wins
/// even when several sources race. `Agent::run` reads the winner once, when
/// it constructs the terminal [`TurnOutcome::Cancelled`], so the outcome is
/// never mutated after emission.
#[derive(Clone)]
pub struct ReasonedCancelToken(Arc<ReasonedShared>);

#[derive(Clone)]
pub struct ReasonedCancelTrigger(Arc<ReasonedShared>);

impl ReasonedCancelToken {
    pub fn new() -> (ReasonedCancelTrigger, Self) {
        let shared = Arc::new(ReasonedShared {
            cancelled: AtomicBool::new(false),
            reason: Mutex::new(None),
            event: Event::new(),
        });
        (ReasonedCancelTrigger(Arc::clone(&shared)), Self(shared))
    }

    pub fn none() -> Self {
        Self(Arc::new(ReasonedShared {
            cancelled: AtomicBool::new(false),
            reason: Mutex::new(None),
            event: Event::new(),
        }))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub fn reason(&self) -> Option<TurnCancellationReason> {
        *self.0.reason.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Waits until the token fires, returning the winning reason.
    pub async fn cancelled(&self) -> TurnCancellationReason {
        loop {
            if let Some(reason) = self.reason() {
                return reason;
            }
            let listener = self.0.event.listen();
            if let Some(reason) = self.reason() {
                return reason;
            }
            listener.await;
        }
    }

    pub async fn race<T>(
        &self,
        future: impl Future<Output = T>,
    ) -> Result<T, TurnCancellationReason> {
        if let Some(reason) = self.reason() {
            return Err(reason);
        }
        futures_lite::future::race(async { Ok(future.await) }, async {
            Err(self.cancelled().await)
        })
        .await
    }
}

impl ReasonedCancelTrigger {
    pub fn cancel(self, reason: TurnCancellationReason) {
        self.0.fire(reason);
    }
}

impl Drop for ReasonedCancelTrigger {
    fn drop(&mut self) {
        self.0.fire(crate::types::TurnCancellationReason::User);
    }
}

impl CancelToken {
    pub fn new() -> (CancelTrigger, Self) {
        let shared = Arc::new(Shared {
            cancelled: AtomicBool::new(false),
            state: Mutex::default(),
            publication_gate: Arc::default(),
            event: Event::new(),
            _parent_registration: None,
        });
        (CancelTrigger(Arc::clone(&shared)), Self(shared))
    }

    pub fn none() -> Self {
        Self(Arc::new(Shared {
            cancelled: AtomicBool::new(false),
            state: Mutex::default(),
            publication_gate: Arc::default(),
            event: Event::new(),
            _parent_registration: None,
        }))
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled()
    }

    pub async fn race<T>(&self, future: impl Future<Output = T>) -> Result<T, String> {
        if self.is_cancelled() {
            return Err("cancelled".into());
        }
        futures_lite::future::race(async { Ok(future.await) }, async {
            self.cancelled().await;
            Err("cancelled".into())
        })
        .await
    }

    pub async fn cancelled(&self) {
        loop {
            if self.is_cancelled() {
                return;
            }
            let listener = self.0.event.listen();
            if self.is_cancelled() {
                return;
            }
            listener.await;
        }
    }

    pub fn register_publication(
        &self,
        publication: &CheckpointPublication,
    ) -> PublicationRegistration {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let publication = Arc::new(publication.clone());
        if self.is_cancelled() {
            publication.cancel();
        } else {
            state.publications.push(Arc::downgrade(&publication));
        }
        PublicationRegistration {
            source: Arc::downgrade(&self.0),
            publication,
        }
    }

    /// Serializes the request CAS with cancellation of this source.
    pub fn claim_publication(&self, publication: &CheckpointPublication) -> bool {
        self.claim_publication_if(publication, || true)
    }

    /// The predicate must inspect captured cheap state only; it runs under the cancellation guard.
    pub fn claim_publication_if(
        &self,
        publication: &CheckpointPublication,
        valid: impl FnOnce() -> bool,
    ) -> bool {
        let _gate = self
            .0
            .publication_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self.is_cancelled() || !valid() {
            publication.cancel();
            return false;
        }
        publication.try_publish()
    }

    pub fn child(&self) -> (CancelTrigger, Self) {
        let _gate = self
            .0
            .publication_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let shared = Arc::new_cyclic(|child| Shared {
            cancelled: AtomicBool::new(false),
            state: Mutex::default(),
            publication_gate: Arc::clone(&self.0.publication_gate),
            event: Event::new(),
            _parent_registration: Some(CancelRegistration {
                source: Arc::clone(&self.0),
                child: child.clone(),
            }),
        });
        if self.is_cancelled() {
            shared.fire_locked();
        } else {
            state.children.push(Arc::downgrade(&shared));
        }
        (CancelTrigger(Arc::clone(&shared)), Self(shared))
    }
}

impl CancelTrigger {
    pub fn cancel(self) {
        self.0.fire();
    }
}

impl Drop for CancelTrigger {
    fn drop(&mut self) {
        self.0.fire();
    }
}

/// Names one registration inside a key's list so its owner can retire it
/// without disturbing the others registered under the same key.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CancelSlot(u64);

struct Slotted {
    slot: CancelSlot,
    trigger: Option<CancelTrigger>,
}

#[derive(Default)]
struct Entry {
    registrations: Vec<Slotted>,
    cancelled: bool,
}

pub struct CancelMap<K> {
    entries: Mutex<HashMap<K, Entry>>,
    next_slot: AtomicU64,
}

impl<K: Eq + std::hash::Hash> Default for CancelMap<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + std::hash::Hash> CancelMap<K> {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            next_slot: AtomicU64::new(0),
        }
    }

    /// Registers {trigger} under {id}, alongside any already there, and
    /// returns the slot to hand back to [`retire`](Self::retire).
    pub fn insert(&self, id: K, trigger: CancelTrigger) -> CancelSlot {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let slot = CancelSlot(self.next_slot.fetch_add(1, Ordering::Relaxed));
        let entry = map.entry(id).or_default();
        let trigger = if entry.cancelled {
            drop(trigger);
            None
        } else {
            Some(trigger)
        };
        entry.registrations.push(Slotted { slot, trigger });
        slot
    }

    /// Retires one registration, dropping its trigger when it is still
    /// active and leaving its siblings alone.
    pub fn retire(&self, id: &K, slot: CancelSlot) {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = map.get_mut(id) else {
            return;
        };
        entry
            .registrations
            .retain(|registration| registration.slot != slot);
        if entry.registrations.is_empty() {
            map.remove(id);
        }
    }

    /// Cancels everything under {id} and marks later siblings cancelled.
    /// The entry stays until every registered sibling retires.
    pub fn cancel_or_precancel(&self, id: K) {
        let mut map = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(id).or_default();
        entry.cancelled = true;
        for registration in &mut entry.registrations {
            drop(registration.trigger.take());
        }
    }

    pub fn remove(&self, id: &K) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
    }

    #[cfg(test)]
    fn has_key(&self, id: &K) -> bool {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(id)
    }

    pub fn cancel_all(&self) {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_wakes_token() {
        smol::block_on(async {
            let (trigger, token) = CancelToken::new();
            assert!(!token.is_cancelled());
            trigger.cancel();
            token.cancelled().await;
            assert!(token.is_cancelled());
        });
    }

    #[test]
    fn child_cancelled_by_parent() {
        let (parent_trigger, parent_token) = CancelToken::new();
        let (_child_trigger, child_token) = parent_token.child();
        let publication = CheckpointPublication::default();
        let _registration = child_token.register_publication(&publication);
        parent_trigger.cancel();
        assert!(child_token.is_cancelled());
        assert!(publication.is_cancelled());
        assert!(!publication.try_publish());
    }

    #[test_case::test_case(false; "cancel_before_claim")]
    #[test_case::test_case(true; "cancel_after_claim")]
    fn publication_registration_orders_cancellation(claim_first: bool) {
        let (trigger, token) = CancelToken::new();
        let publication = CheckpointPublication::default();
        let registration = token.register_publication(&publication);
        if claim_first {
            assert!(token.claim_publication(&publication));
        }
        trigger.cancel();
        assert_eq!(publication.is_cancelled(), !claim_first);
        assert!(!publication.try_publish());
        drop(registration);
        assert!(token.0.state.lock().unwrap().publications.is_empty());
    }

    #[test]
    fn publication_registration_rejects_cancelled_source_and_deregisters_on_abort() {
        let (trigger, token) = CancelToken::new();
        let abandoned = CheckpointPublication::default();
        drop(token.register_publication(&abandoned));
        trigger.cancel();
        assert!(!abandoned.is_cancelled());
        let publication = CheckpointPublication::default();
        let _registration = token.register_publication(&publication);
        assert!(publication.is_cancelled());
        assert!(!token.claim_publication(&publication));
    }

    #[test]
    fn child_cancelled_by_own_trigger() {
        smol::block_on(async {
            let (_parent_trigger, parent_token) = CancelToken::new();
            let (child_trigger, child_token) = parent_token.child();
            child_trigger.cancel();
            child_token.cancelled().await;
            assert!(child_token.is_cancelled());
            assert!(!parent_token.is_cancelled());
        });
    }

    #[test]
    fn drop_trigger_also_cancels() {
        smol::block_on(async {
            let (trigger, token) = CancelToken::new();
            drop(trigger);
            token.cancelled().await;
            assert!(token.is_cancelled());
        });
    }

    #[test]
    fn race_returns_value_when_not_cancelled() {
        smol::block_on(async {
            let (_trigger, token) = CancelToken::new();
            let result = token.race(async { 42 }).await;
            assert_eq!(result.unwrap(), 42);
        });
    }

    #[test]
    fn race_returns_error_when_already_cancelled() {
        smol::block_on(async {
            let (trigger, token) = CancelToken::new();
            trigger.cancel();
            let result = token.race(std::future::pending::<()>()).await;
            assert!(result.unwrap_err().contains("cancelled"));
        });
    }

    #[test]
    fn race_interrupted_by_concurrent_cancel() {
        smol::block_on(async {
            let (trigger, token) = CancelToken::new();
            smol::spawn(async move { trigger.cancel() }).detach();
            let result = token.race(std::future::pending::<()>()).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn cancel_map_insert_and_cancel() {
        let map = CancelMap::new();
        let (trigger, token) = CancelToken::new();
        map.insert("t1".to_owned(), trigger);
        assert!(!token.is_cancelled());
        map.cancel_or_precancel("t1".to_owned());
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_map_cancel_before_insert() {
        let map = CancelMap::new();
        map.cancel_or_precancel("t1".to_owned());
        let (trigger, token) = CancelToken::new();
        map.insert("t1".to_owned(), trigger);
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_map_remove_clears_cancelled() {
        let map: CancelMap<String> = CancelMap::new();
        map.cancel_or_precancel("t1".to_owned());
        map.remove(&"t1".to_owned());
        let (trigger, token) = CancelToken::new();
        map.insert("t1".to_owned(), trigger);
        assert!(!token.is_cancelled(), "remove should clear cancellation");
    }

    #[test]
    fn cancel_map_cancel_all() {
        let map = CancelMap::new();
        let (t1, tok1) = CancelToken::new();
        let (t2, tok2) = CancelToken::new();
        map.insert("a".to_owned(), t1);
        map.insert("b".to_owned(), t2);
        map.cancel_all();
        assert!(tok1.is_cancelled());
        assert!(tok2.is_cancelled());
    }

    #[test]
    fn cancel_map_cancel_all_clears_cancelled() {
        let map: CancelMap<String> = CancelMap::new();
        map.cancel_or_precancel("t1".to_owned());
        map.cancel_all();
        let (trigger, token) = CancelToken::new();
        map.insert("t1".to_owned(), trigger);
        assert!(
            !token.is_cancelled(),
            "cancel_all should clear cancelled entries"
        );
    }

    /// One tool call can open several subagents. They used to evict each
    /// other, so the first died the moment the second registered.
    #[test]
    fn cancel_map_keeps_siblings_under_one_key() {
        let map = CancelMap::new();
        let (t1, tok1) = CancelToken::new();
        let (t2, tok2) = CancelToken::new();
        map.insert("x".to_owned(), t1);
        map.insert("x".to_owned(), t2);
        assert!(!tok1.is_cancelled(), "a sibling must not evict the first");
        assert!(!tok2.is_cancelled());

        map.cancel_or_precancel("x".to_owned());
        assert!(tok1.is_cancelled(), "cancelling the key stops them all");
        assert!(tok2.is_cancelled());
    }

    #[test]
    fn cancel_map_retire_leaves_siblings_running() {
        let map = CancelMap::new();
        let (t1, tok1) = CancelToken::new();
        let (t2, tok2) = CancelToken::new();
        let slot1 = map.insert("x".to_owned(), t1);
        map.insert("x".to_owned(), t2);

        map.retire(&"x".to_owned(), slot1);
        assert!(tok1.is_cancelled(), "retiring drops that trigger");
        assert!(!tok2.is_cancelled(), "the sibling keeps running");

        map.cancel_or_precancel("x".to_owned());
        assert!(tok2.is_cancelled());
    }

    /// The last one out clears the key so it can be reused.
    #[test]
    fn cancel_map_retiring_the_last_registration_clears_the_key() {
        let map = CancelMap::new();
        let (t1, _tok1) = CancelToken::new();
        let slot = map.insert("x".to_owned(), t1);
        assert!(map.has_key(&"x".to_owned()));

        map.retire(&"x".to_owned(), slot);
        assert!(!map.has_key(&"x".to_owned()), "empty key must be dropped");
    }

    /// Cancelling before anything registers has to catch every session the
    /// tool call goes on to open, not just the first one through the door.
    #[test]
    fn cancel_map_precancel_catches_every_later_sibling() {
        let map: CancelMap<String> = CancelMap::new();
        map.cancel_or_precancel("x".to_owned());

        let (t1, tok1) = CancelToken::new();
        let (t2, tok2) = CancelToken::new();
        let slot1 = map.insert("x".to_owned(), t1);
        let slot2 = map.insert("x".to_owned(), t2);
        assert!(tok1.is_cancelled());
        assert!(
            tok2.is_cancelled(),
            "the mark must outlive the first insert"
        );

        map.retire(&"x".to_owned(), slot1);
        assert!(map.has_key(&"x".to_owned()));
        map.retire(&"x".to_owned(), slot2);
        assert!(!map.has_key(&"x".to_owned()));
    }

    /// Pressing esc while a fan-out is running must also stop the sibling
    /// that starts a moment later.
    #[test]
    fn cancel_map_cancel_catches_a_sibling_registered_after() {
        let map = CancelMap::new();
        let (t1, tok1) = CancelToken::new();
        let slot1 = map.insert("x".to_owned(), t1);

        map.cancel_or_precancel("x".to_owned());
        assert!(tok1.is_cancelled());

        let (t2, tok2) = CancelToken::new();
        let slot2 = map.insert("x".to_owned(), t2);
        assert!(tok2.is_cancelled(), "cancel left no mark for the sibling");

        map.retire(&"x".to_owned(), slot1);
        assert!(map.has_key(&"x".to_owned()));
        map.retire(&"x".to_owned(), slot2);
        assert!(!map.has_key(&"x".to_owned()));

        let (t3, tok3) = CancelToken::new();
        map.insert("x".to_owned(), t3);
        assert!(
            !tok3.is_cancelled(),
            "the completed call must not poison a reused tool id"
        );
    }

    #[test]
    fn cancel_map_insert_into_cancelled_returns_retirement_slot() {
        let map = CancelMap::new();
        map.cancel_or_precancel("x".to_owned());
        let (trigger, token) = CancelToken::new();
        let slot = map.insert("x".to_owned(), trigger);
        assert!(token.is_cancelled());
        map.retire(&"x".to_owned(), slot);
        assert!(!map.has_key(&"x".to_owned()));
    }

    #[test]
    fn reasoned_token_first_reason_wins() {
        smol::block_on(async {
            let (closed_trigger, closed_token) = ReasonedCancelToken::new();
            let (user_trigger, user_token) = ReasonedCancelToken::new();
            closed_trigger.cancel(TurnCancellationReason::Closed);
            user_trigger.cancel(TurnCancellationReason::User);
            assert_eq!(
                closed_token.cancelled().await,
                TurnCancellationReason::Closed
            );
            assert_eq!(user_token.cancelled().await, TurnCancellationReason::User);
        });
    }

    #[test]
    fn reasoned_token_second_fire_keeps_first_reason() {
        let (closed_trigger, token) = ReasonedCancelToken::new();
        let (user_trigger, _) = ReasonedCancelToken::new();
        closed_trigger.cancel(TurnCancellationReason::Closed);
        user_trigger.cancel(TurnCancellationReason::User);
        assert_eq!(token.reason(), Some(TurnCancellationReason::Closed));
    }

    #[test]
    fn reasoned_token_drop_uses_user_reason() {
        let (trigger, token) = ReasonedCancelToken::new();
        drop(trigger);
        let reason = smol::block_on(token.cancelled());
        assert_eq!(reason, TurnCancellationReason::User);
    }

    #[test]
    fn reasoned_token_race_returns_reason_when_cancelled() {
        smol::block_on(async {
            let (trigger, token) = ReasonedCancelToken::new();
            let (user_trigger, _) = ReasonedCancelToken::new();
            trigger.cancel(TurnCancellationReason::Shutdown);
            user_trigger.cancel(TurnCancellationReason::User);
            let result = token.race(std::future::pending::<()>()).await;
            assert_eq!(result.unwrap_err(), TurnCancellationReason::Shutdown);
        });
    }
}
