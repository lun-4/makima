//! User tier overrides (strong / medium / weak / compaction), one model per
//! tier, persisted across runs.
//!
//! The global lock never escapes this module: accessors lock internally and
//! return owned data. The module owns persistence: [`load_from_storage`] at
//! startup, [`set_and_persist`] on user edits. Callers never touch the on-disk
//! format directly.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard};

use makima_storage::{StateDir, atomic_write};
use tracing::warn;

use crate::model::ModelTier;

const TIERS_FILE: &str = "model-tiers";

/// Keyed by tier (not spec) so inserting a model automatically evicts the
/// previous holder.
static OVERRIDES: OnceLock<RwLock<Overrides>> = OnceLock::new();

type Overrides = BTreeMap<ModelTier, String>;

fn read() -> RwLockReadGuard<'static, Overrides> {
    overrides().read().unwrap()
}

fn write() -> RwLockWriteGuard<'static, Overrides> {
    overrides().write().unwrap()
}

fn overrides() -> &'static RwLock<Overrides> {
    OVERRIDES.get_or_init(RwLock::default)
}

/// The spec the user assigned to `tier`, whatever its provider.
pub fn tier_override(tier: ModelTier) -> Option<String> {
    read().get(&tier).cloned()
}

/// Tiers whose override points at `spec`, in descending tier order.
pub fn override_tiers(spec: &str) -> Vec<ModelTier> {
    override_tiers_in(&read(), spec)
}

/// A spec may hold several tiers; the strongest agent tier wins, and
/// Compaction only when it is the sole assignment. Without an override the
/// descriptor's tier stands, else Medium.
pub fn tier_for(spec: &str, declared: Option<ModelTier>) -> ModelTier {
    tier_for_in(&read(), spec, declared)
}

pub fn load_from_storage(dir: &StateDir) {
    *write() = read_overrides(dir.path().join(TIERS_FILE).as_path());
}

pub fn set_and_persist(spec: String, tier: ModelTier, dir: &StateDir) {
    update_and_persist(dir, |overrides| {
        overrides.insert(tier, spec);
    });
}

pub fn unset_and_persist(spec: &str, tier: ModelTier, dir: &StateDir) {
    update_and_persist(dir, |overrides| unset(overrides, spec, tier));
}

/// Snapshot under the lock, persist outside it: file IO must never run while
/// holding the lock.
fn update_and_persist(dir: &StateDir, update: impl FnOnce(&mut Overrides)) {
    let snapshot = {
        let mut overrides = write();
        update(&mut overrides);
        overrides.clone()
    };
    write_overrides(dir.path().join(TIERS_FILE).as_path(), &snapshot);
}

fn unset(overrides: &mut Overrides, spec: &str, tier: ModelTier) {
    if overrides.get(&tier).map(String::as_str) == Some(spec) {
        overrides.remove(&tier);
    }
}

fn override_tiers_in(overrides: &Overrides, spec: &str) -> Vec<ModelTier> {
    overrides
        .iter()
        .rev()
        .filter(|(_, s)| s.as_str() == spec)
        .map(|(&t, _)| t)
        .collect()
}

fn tier_for_in(overrides: &Overrides, spec: &str, declared: Option<ModelTier>) -> ModelTier {
    let mut tiers = override_tiers_in(overrides, spec).into_iter();
    match tiers.next() {
        Some(ModelTier::Compaction) => tiers.next().unwrap_or(ModelTier::Compaction),
        Some(tier) => tier,
        None => declared.unwrap_or(ModelTier::Medium),
    }
}

// On-disk format: { "tier": "spec", ... } keyed by tier, matching the in-memory
// map. Tier-keyed storage preserves a model assigned to multiple tiers; a
// spec-keyed file would collapse them to a single entry.

fn read_overrides(path: &Path) -> Overrides {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return Overrides::new();
    };
    if raw.trim().is_empty() {
        return Overrides::new();
    }
    match serde_json::from_str(&raw) {
        Ok(map) => map,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "failed to parse tier overrides, ignoring");
            Overrides::new()
        }
    }
}

fn write_overrides(path: &Path, overrides: &Overrides) {
    let json = match serde_json::to_vec_pretty(overrides) {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "failed to serialize tier overrides");
            return;
        }
    };
    if let Err(e) = atomic_write(path, &json) {
        warn!(path = %path.display(), error = %e, "failed to persist tier overrides");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use test_case::test_case;

    fn overrides_of(pairs: &[(ModelTier, &str)]) -> Overrides {
        pairs.iter().map(|(t, s)| (*t, s.to_string())).collect()
    }

    #[test_case("p/pinned", Some(ModelTier::Strong), ModelTier::Weak ; "override_beats_declared")]
    #[test_case("p/other", Some(ModelTier::Strong), ModelTier::Strong ; "declared_without_override")]
    #[test_case("p/other", None, ModelTier::Medium ; "medium_without_either")]
    #[test_case("p/multi", None, ModelTier::Strong ; "strongest_of_several")]
    #[test_case("p/compact", Some(ModelTier::Strong), ModelTier::Compaction ; "compaction_when_sole_override")]
    fn tier_for_resolution(spec: &str, declared: Option<ModelTier>, expected: ModelTier) {
        let overrides = overrides_of(&[
            (ModelTier::Weak, "p/pinned"),
            (ModelTier::Medium, "p/multi"),
            (ModelTier::Strong, "p/multi"),
            (ModelTier::Compaction, "p/compact"),
        ]);
        assert_eq!(tier_for_in(&overrides, spec, declared), expected);
    }

    #[test]
    fn persistence_round_trip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(TIERS_FILE);

        assert!(read_overrides(&path).is_empty());

        let mut m = Overrides::new();
        m.insert(ModelTier::Strong, "p/qwen3".into());
        m.insert(ModelTier::Medium, "p/qwen3:8b".into());
        write_overrides(&path, &m);

        let loaded = read_overrides(&path);
        assert_eq!(loaded.get(&ModelTier::Strong).unwrap(), "p/qwen3");
        assert_eq!(loaded.get(&ModelTier::Medium).unwrap(), "p/qwen3:8b");
    }

    #[test]
    fn persistence_handles_missing_or_invalid_input() {
        let tmp = TempDir::new().unwrap();
        assert!(read_overrides(&tmp.path().join("does-not-exist")).is_empty());

        for bad in [
            b"".as_slice(),
            b"   \n".as_slice(),
            b"not json at all".as_slice(),
        ] {
            let path = tmp.path().join(TIERS_FILE);
            std::fs::write(&path, bad).unwrap();
            assert!(read_overrides(&path).is_empty());
        }
    }

    #[test_case("p/a", ModelTier::Strong, true ; "matching_override_removed")]
    #[test_case("p/b", ModelTier::Strong, false ; "mismatched_spec_kept")]
    #[test_case("p/a", ModelTier::Weak, false ; "mismatched_tier_kept")]
    fn unset_removes_only_a_matching_override(spec: &str, tier: ModelTier, removed: bool) {
        let mut overrides = overrides_of(&[(ModelTier::Strong, "p/a")]);
        unset(&mut overrides, spec, tier);
        assert_eq!(overrides.is_empty(), removed);
    }

    #[test]
    fn write_then_read_preserves_multi_tier_assignment() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(TIERS_FILE);

        let mut m = Overrides::new();
        m.insert(ModelTier::Strong, "p/qwen3".into());
        m.insert(ModelTier::Medium, "p/qwen3".into());
        m.insert(ModelTier::Weak, "p/qwen3:8b".into());
        write_overrides(&path, &m);

        let loaded = read_overrides(&path);
        assert_eq!(loaded.get(&ModelTier::Strong).unwrap(), "p/qwen3");
        assert_eq!(loaded.get(&ModelTier::Medium).unwrap(), "p/qwen3");
        assert_eq!(loaded.get(&ModelTier::Weak).unwrap(), "p/qwen3:8b");
    }
}
