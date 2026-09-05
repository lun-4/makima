use nucleo::Status;
use nucleo::pattern::{Atom, Pattern};

use crate::repaint::Cadence;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Publication {
    Wait,
    Stream,
    Commit,
    Clear,
}

#[derive(Debug)]
struct Entry<K, T> {
    key: K,
    value: T,
}

#[derive(Debug)]
struct Pending<K> {
    generation: u64,
    key: K,
}

#[derive(Debug)]
pub(super) struct Published<K, T> {
    committed: Option<Entry<K, T>>,
    generation: u64,
    pending: Option<Pending<K>>,
}

impl<K, T> Default for Published<K, T> {
    fn default() -> Self {
        Self {
            committed: None,
            generation: 0,
            pending: None,
        }
    }
}

impl<K: PartialEq, T> Published<K, T> {
    pub(super) fn begin(&mut self, key: K) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.pending = Some(Pending {
            generation: self.generation,
            key,
        });
        self.generation
    }

    pub(super) fn commit(&mut self, generation: u64, key: K, value: T) -> Publication {
        if !matches!(&self.pending, Some(pending) if pending.generation == generation && pending.key == key)
        {
            return Publication::Wait;
        }
        self.committed = Some(Entry { key, value });
        self.pending = None;
        Publication::Commit
    }

    pub(super) fn stream(&mut self, key: K, value: T) -> Publication {
        if self.pending.is_some()
            || !matches!(&self.committed, Some(committed) if committed.key == key)
        {
            return Publication::Wait;
        }
        self.committed = Some(Entry { key, value });
        Publication::Stream
    }

    pub(super) fn commit_sync(&mut self, key: K, value: T) -> Publication {
        self.generation = self.generation.wrapping_add(1);
        self.pending = None;
        self.committed = Some(Entry { key, value });
        Publication::Commit
    }

    pub(super) fn clear(&mut self) -> Publication {
        self.generation = self.generation.wrapping_add(1);
        self.pending = None;
        if self.committed.take().is_some() {
            Publication::Clear
        } else {
            Publication::Wait
        }
    }

    pub(super) fn cancel(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.pending = None;
    }

    pub(super) fn value(&self) -> Option<&T> {
        self.committed.as_ref().map(|entry| &entry.value)
    }

    pub(super) fn can_accept(&self) -> bool {
        self.pending.is_none() && self.committed.is_some()
    }

    pub(super) fn is_pending(&self) -> bool {
        self.pending.is_some()
    }

    pub(super) fn cadence(&self) -> Cadence {
        Cadence::when(self.is_pending(), Cadence::PENDING)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PatternKey(Vec<Atom>);

impl PatternKey {
    fn new(pattern: &Pattern) -> Self {
        Self(pattern.atoms.clone())
    }
}

#[derive(Debug)]
pub(super) struct PatternPublication<T> {
    values: Published<PatternKey, T>,
    requested: Option<(u64, PatternKey)>,
}

impl<T> Default for PatternPublication<T> {
    fn default() -> Self {
        Self {
            values: Published::default(),
            requested: None,
        }
    }
}

impl<T> PatternPublication<T> {
    pub(super) fn begin(&mut self, pattern: &Pattern) {
        let key = PatternKey::new(pattern);
        let generation = self.values.begin(key.clone());
        self.requested = Some((generation, key));
    }

    pub(super) fn commit_sync(&mut self, pattern: &Pattern, value: T) -> Publication {
        self.requested = None;
        self.values.commit_sync(PatternKey::new(pattern), value)
    }

    pub(super) fn observe(
        &mut self,
        status: Status,
        snapshot_pattern: &Pattern,
        value: T,
    ) -> Publication {
        if !status.changed {
            return Publication::Wait;
        }
        let key = PatternKey::new(snapshot_pattern);
        if let Some((generation, requested)) = &self.requested {
            if requested != &key {
                return Publication::Wait;
            }
            let publication = self.values.commit(*generation, key, value);
            if publication == Publication::Commit {
                self.requested = None;
            }
            publication
        } else if self.values.committed.is_some() {
            self.values.stream(key, value)
        } else {
            self.values.commit_sync(key, value)
        }
    }

    pub(super) fn is_pending(&self) -> bool {
        self.values.is_pending()
    }
}

#[derive(Debug, Default)]
pub(super) struct CoherentCompletion {
    publication: PatternPublication<()>,
}

impl CoherentCompletion {
    pub(super) fn query_reparsed(&mut self, pattern: &Pattern, has_indexed_items: bool) {
        if has_indexed_items {
            self.publication.begin(pattern);
        } else {
            self.publication.commit_sync(pattern, ());
        }
    }

    pub(super) fn synchronous_commit(&mut self) {
        self.publication.values.pending = None;
        self.publication.requested = None;
    }

    pub(super) fn observe(&mut self, status: Status, snapshot_pattern: &Pattern) -> Publication {
        self.publication.observe(status, snapshot_pattern, ())
    }

    pub(super) fn ready(&self) -> bool {
        !self.publication.is_pending()
    }

    pub(super) fn pending(&self) -> bool {
        self.publication.is_pending()
    }

    pub(super) fn needs_repaint(&self, matcher_running: bool) -> bool {
        self.pending() || matcher_running
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nucleo::pattern::{CaseMatching, Normalization};

    fn pattern(query: &str) -> Pattern {
        Pattern::parse(query, CaseMatching::Smart, Normalization::Smart)
    }

    #[test]
    fn begin_retains_committed_value() {
        let mut state = Published::default();
        state.commit_sync("old", vec![1]);
        state.begin("new");
        assert_eq!(state.value(), Some(&vec![1]));
        assert!(!state.can_accept());
    }

    #[test]
    fn latest_generation_wins() {
        let mut state = Published::default();
        let old = state.begin("old");
        let new = state.begin("new");
        assert_eq!(state.commit(old, "old", 1), Publication::Wait);
        assert_eq!(state.commit(new, "new", 2), Publication::Commit);
        assert_eq!(state.value(), Some(&2));
    }

    #[test]
    fn newer_pending_blocks_stream() {
        let mut state = Published::default();
        state.commit_sync("old", 1);
        state.begin("new");
        assert_eq!(state.stream("old", 2), Publication::Wait);
        assert_eq!(state.value(), Some(&1));
    }

    #[test]
    fn clear_and_cancel_reject_late_results() {
        let mut state = Published::default();
        state.commit_sync("old", 1);
        let generation = state.begin("new");
        assert_eq!(state.clear(), Publication::Clear);
        assert_eq!(state.commit(generation, "new", 2), Publication::Wait);
        let generation = state.begin("later");
        state.cancel();
        assert_eq!(state.commit(generation, "later", 3), Publication::Wait);
    }

    #[test]
    fn current_pattern_commits_while_running_then_streams() {
        let requested = pattern("new");
        let mut state = PatternPublication::default();
        state.begin(&requested);
        let changed = Status {
            changed: true,
            running: true,
        };
        assert_eq!(state.observe(changed, &requested, 1), Publication::Commit);
        assert_eq!(state.observe(changed, &requested, 2), Publication::Stream);
        assert_eq!(state.values.value(), Some(&2));
    }

    #[test]
    fn stale_and_unchanged_patterns_wait() {
        let requested = pattern("new");
        let mut state = PatternPublication::default();
        state.begin(&requested);
        assert_eq!(
            state.observe(
                Status {
                    changed: true,
                    running: false,
                },
                &pattern("old"),
                1,
            ),
            Publication::Wait
        );
        assert_eq!(
            state.observe(
                Status {
                    changed: false,
                    running: true,
                },
                &requested,
                2,
            ),
            Publication::Wait
        );
        assert!(state.is_pending());
    }
}
