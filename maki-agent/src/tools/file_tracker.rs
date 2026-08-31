use std::collections::HashMap;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use sha2::{Digest, Sha256};
use similar::{DiffTag, TextDiff};

const STALE_READ_MSG: &str = "file changed since last read";
const SNAPSHOT_MISMATCH_MSG: &str = "file content does not match the last precise read";

#[derive(Default)]
struct FileState {
    mtime: Option<SystemTime>,
    fingerprint: Option<[u8; 32]>,
    covered: Vec<Range<usize>>,
    generation: u64,
}

pub struct FileReadTracker(Mutex<HashMap<PathBuf, FileState>>);

fn get_mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).and_then(|m| m.modified()).ok()
}

fn normalize_path(path: &Path) -> PathBuf {
    fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn fingerprint(content: &str) -> [u8; 32] {
    Sha256::digest(content.as_bytes()).into()
}

fn merge_ranges(mut ranges: Vec<Range<usize>>, size: usize) -> Result<Vec<Range<usize>>, String> {
    if ranges
        .iter()
        .any(|range| range.start > range.end || range.end > size)
    {
        return Err("read provenance contains an invalid source byte range".to_owned());
    }
    ranges.retain(|range| !range.is_empty());
    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(last) = merged.last_mut()
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    Ok(merged)
}

fn covered(ranges: &[Range<usize>], target: &Range<usize>) -> bool {
    target.is_empty()
        || ranges
            .iter()
            .any(|range| range.start <= target.start && range.end >= target.end)
}

fn char_boundaries(text: &str) -> Vec<usize> {
    text.char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(text.len()))
        .collect()
}

#[derive(Clone)]
struct Mapping {
    equal: Vec<(Range<usize>, Range<usize>)>,
    inserted: Vec<Range<usize>>,
}

fn mutation_mapping(
    before: &str,
    after: &str,
    coverage: &[Range<usize>],
) -> Result<Mapping, String> {
    let old_bytes = char_boundaries(before);
    let new_bytes = char_boundaries(after);
    let diff = TextDiff::from_chars(before, after);
    let mut mapping = Mapping {
        equal: Vec::new(),
        inserted: Vec::new(),
    };

    for op in diff.ops() {
        let old = op.old_range();
        let new = op.new_range();
        let old_range = old_bytes[old.start]..old_bytes[old.end];
        let new_range = new_bytes[new.start]..new_bytes[new.end];
        match op.tag() {
            DiffTag::Equal => mapping.equal.push((old_range, new_range)),
            DiffTag::Insert => mapping.inserted.push(new_range),
            DiffTag::Delete | DiffTag::Replace => {
                if !covered(coverage, &old_range) {
                    return Err(unseen_error(&old_range));
                }
                let deleted = &before[old_range.clone()];
                if !deleted.is_empty() {
                    for (start, _) in before.match_indices(deleted) {
                        let candidate = start..start + deleted.len();
                        if !covered(coverage, &candidate) {
                            return Err(unseen_error(&candidate));
                        }
                    }
                }
                if !new_range.is_empty() {
                    mapping.inserted.push(new_range);
                }
            }
        }
    }
    Ok(mapping)
}

fn unseen_error(range: &Range<usize>) -> String {
    format!(
        "mutation would delete or replace unseen source bytes {}-{}; recover them with read byte mode using byte_offset={} and byte_limit={}",
        range.start,
        range.end,
        range.start,
        range.end - range.start
    )
}

impl Default for FileReadTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FileReadTracker {
    pub fn new() -> Self {
        Self(Mutex::new(HashMap::new()))
    }

    pub fn fresh() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub fn begin_read(&self, path: &Path) -> u64 {
        self.0
            .lock()
            .unwrap()
            .entry(normalize_path(path))
            .or_default()
            .generation
    }

    pub fn record_freshness(&self, path: &Path) {
        let normalized = normalize_path(path);
        let mtime = get_mtime(&normalized);
        self.0.lock().unwrap().entry(normalized).or_default().mtime = mtime;
    }

    pub fn record_read(&self, path: &Path) {
        self.record_freshness(path);
    }

    pub fn record_observation(
        &self,
        path: &Path,
        content: &str,
        ranges: &[(usize, usize)],
        lease: u64,
    ) -> Result<bool, String> {
        let normalized = normalize_path(path);
        let observed = merge_ranges(
            ranges.iter().map(|&(start, end)| start..end).collect(),
            content.len(),
        )?;
        if observed.iter().any(|range| {
            !content.is_char_boundary(range.start) || !content.is_char_boundary(range.end)
        }) {
            return Err("read provenance range is not on a UTF-8 character boundary".to_owned());
        }
        let mut guard = self.0.lock().unwrap();
        let state = guard.entry(normalized.clone()).or_default();
        if state.generation != lease {
            return Ok(false);
        }
        let observed_fingerprint = fingerprint(content);
        if state.fingerprint == Some(observed_fingerprint) {
            state.covered.extend(observed);
            state.covered = merge_ranges(std::mem::take(&mut state.covered), content.len())?;
        } else {
            state.fingerprint = Some(observed_fingerprint);
            state.covered = observed;
        }
        state.mtime = get_mtime(&normalized);
        Ok(true)
    }

    pub fn check_before_edit(&self, path: &Path) -> Result<(), String> {
        let normalized = normalize_path(path);
        let mut guard = self.0.lock().unwrap();
        let Some(state) = guard.get_mut(&normalized) else {
            return Ok(());
        };
        let Some(recorded) = state.mtime else {
            return Ok(());
        };
        let Some(current) = get_mtime(&normalized) else {
            state.mtime = None;
            return Ok(());
        };
        if recorded != current {
            return Err(format!(
                "{STALE_READ_MSG}: {} - re-read using read tool before editing",
                path.display(),
            ));
        }
        Ok(())
    }

    pub fn validate_mutation(&self, path: &Path, before: &str, after: &str) -> Result<(), String> {
        let guard = self.0.lock().unwrap();
        let state = guard.get(&normalize_path(path)).ok_or_else(|| {
            format!(
                "{SNAPSHOT_MISMATCH_MSG}: {} - use the read tool before editing",
                path.display()
            )
        })?;
        if state.fingerprint != Some(fingerprint(before)) {
            return Err(format!(
                "{SNAPSHOT_MISMATCH_MSG}: {} - re-read the current file before editing",
                path.display()
            ));
        }
        mutation_mapping(before, after, &state.covered).map(|_| ())
    }

    pub fn commit_mutation(
        &self,
        path: &Path,
        before: &str,
        after: &str,
        known_insertions: &[String],
    ) -> Result<(), String> {
        let normalized = normalize_path(path);
        let mut guard = self.0.lock().unwrap();
        let state = guard
            .get_mut(&normalized)
            .ok_or_else(|| format!("{SNAPSHOT_MISMATCH_MSG}: {}", path.display()))?;
        if state.fingerprint != Some(fingerprint(before)) {
            return Err(format!("{SNAPSHOT_MISMATCH_MSG}: {}", path.display()));
        }
        let mapping = mutation_mapping(before, after, &state.covered)?;
        let mut rebased = mapping
            .inserted
            .into_iter()
            .filter(|range| {
                known_insertions
                    .iter()
                    .any(|known| !known.is_empty() && known.contains(&after[range.clone()]))
            })
            .collect::<Vec<_>>();
        for prior in &state.covered {
            for (old, new) in &mapping.equal {
                let start = prior.start.max(old.start);
                let end = prior.end.min(old.end);
                if start < end {
                    rebased.push(new.start + start - old.start..new.start + end - old.start);
                }
            }
        }
        state.covered = merge_ranges(rebased, after.len())?;
        state.fingerprint = Some(fingerprint(after));
        state.mtime = get_mtime(&normalized);
        state.generation = state.generation.wrapping_add(1);
        Ok(())
    }

    pub fn record_complete_write(&self, path: &Path, content: &str) {
        let normalized = normalize_path(path);
        let mut guard = self.0.lock().unwrap();
        let state = guard.entry(normalized.clone()).or_default();
        state.fingerprint = Some(fingerprint(content));
        state.covered = (!content.is_empty())
            .then_some(0..content.len())
            .into_iter()
            .collect();
        state.mtime = get_mtime(&normalized);
        state.generation = state.generation.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn future_mtime(path: &Path) {
        let future = SystemTime::now() + Duration::from_secs(10);
        fs::File::options()
            .write(true)
            .open(path)
            .unwrap()
            .set_modified(future)
            .unwrap();
    }

    #[test]
    fn untracked_file_allows_edit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("f.rs");
        fs::write(&path, "content").unwrap();

        let tracker = FileReadTracker::new();
        tracker.check_before_edit(&path).unwrap();
    }

    #[test]
    fn stale_read_rejects_edit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("f.rs");
        fs::write(&path, "original").unwrap();

        let tracker = FileReadTracker::new();
        tracker.record_read(&path);
        future_mtime(&path);
        let err = tracker.check_before_edit(&path).unwrap_err();
        assert!(err.contains(STALE_READ_MSG), "{err}");
    }

    #[test]
    fn deleted_file_allows_edit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("f.rs");
        fs::write(&path, "content").unwrap();

        let tracker = FileReadTracker::new();
        tracker.record_read(&path);
        fs::remove_file(&path).unwrap();
        tracker.check_before_edit(&path).unwrap();
    }

    #[test]
    fn re_read_after_change_allows_edit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("f.rs");
        fs::write(&path, "v1").unwrap();

        let tracker = FileReadTracker::new();
        tracker.record_read(&path);
        future_mtime(&path);
        tracker.record_read(&path);
        tracker.check_before_edit(&path).unwrap();
    }

    #[test]
    fn nonexistent_file_not_tracked() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ghost.rs");

        let tracker = FileReadTracker::new();
        tracker.record_read(&path);
        tracker.check_before_edit(&path).unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn symlink_resolves_to_canonical() {
        let dir = tempfile::TempDir::new().unwrap();
        let real = dir.path().join("real.rs");
        let link = dir.path().join("link.rs");
        fs::write(&real, "content").unwrap();
        std::os::unix::fs::symlink(&real, &link).unwrap();

        let tracker = FileReadTracker::new();
        tracker.record_read(&real);
        tracker.check_before_edit(&link).unwrap();
    }

    fn observe(tracker: &FileReadTracker, path: &Path, content: &str, ranges: &[(usize, usize)]) {
        let lease = tracker.begin_read(path);
        assert!(
            tracker
                .record_observation(path, content, ranges, lease)
                .unwrap()
        );
    }

    #[test]
    fn observations_merge_and_allow_only_covered_replacement() {
        let tracker = FileReadTracker::new();
        let path = Path::new("virtual.rs");
        observe(&tracker, path, "abcdef", &[(0, 2), (2, 4)]);

        tracker.validate_mutation(path, "abcdef", "abXYef").unwrap();
        let error = tracker
            .validate_mutation(path, "abcdef", "abcdXY")
            .unwrap_err();
        assert!(error.contains("unseen source bytes 4-6"), "{error}");
    }

    #[test]
    fn insertion_rebases_coverage_without_promoting_unseen_bytes() {
        let tracker = FileReadTracker::new();
        let path = Path::new("insert.rs");
        observe(&tracker, path, "known unseen", &[(0, 5)]);

        tracker
            .validate_mutation(path, "known unseen", "known! unseen")
            .unwrap();
        tracker
            .commit_mutation(path, "known unseen", "known! unseen", &["!".to_owned()])
            .unwrap();
        tracker
            .validate_mutation(path, "known! unseen", "KNOWN! unseen")
            .unwrap();
        let error = tracker
            .validate_mutation(path, "known! unseen", "known!")
            .unwrap_err();
        assert!(error.contains("unseen source bytes"), "{error}");
    }

    #[test]
    fn repeated_unseen_occurrence_is_not_favorably_aligned() {
        let tracker = FileReadTracker::new();
        let path = Path::new("repeat.rs");
        observe(&tracker, path, "same same", &[(0, 4)]);

        let error = tracker
            .validate_mutation(path, "same same", " same")
            .unwrap_err();
        assert!(error.contains("unseen source bytes 5-9"), "{error}");

        observe(&tracker, path, "same same", &[(5, 9)]);
        tracker
            .validate_mutation(path, "same same", " same")
            .unwrap();
    }

    #[test]
    fn snapshot_mismatch_is_enforced_without_mtime() {
        let tracker = FileReadTracker::new();
        let path = Path::new("memory.rs");
        observe(&tracker, path, "v1", &[(0, 2)]);

        let error = tracker.validate_mutation(path, "v2", "v3").unwrap_err();
        assert!(error.contains(SNAPSHOT_MISMATCH_MSG), "{error}");
    }

    #[test]
    fn late_observation_cannot_replace_committed_provenance() {
        let tracker = FileReadTracker::new();
        let path = Path::new("late.rs");
        observe(&tracker, path, "v1", &[(0, 2)]);
        let stale_lease = tracker.begin_read(path);
        tracker.validate_mutation(path, "v1", "v2").unwrap();
        tracker
            .commit_mutation(path, "v1", "v2", &["2".to_owned()])
            .unwrap();

        assert!(
            !tracker
                .record_observation(path, "v1", &[(0, 2)], stale_lease)
                .unwrap()
        );
        tracker.validate_mutation(path, "v2", "v3").unwrap();
    }

    #[test]
    fn multibyte_ranges_must_use_character_boundaries() {
        let tracker = FileReadTracker::new();
        let path = Path::new("utf8.rs");
        let lease = tracker.begin_read(path);
        let error = tracker
            .record_observation(path, "aé", &[(0, 2)], lease)
            .unwrap_err();
        assert!(error.contains("UTF-8 character boundary"), "{error}");

        observe(&tracker, path, "aé", &[(0, 3)]);
        tracker.validate_mutation(path, "aé", "Aé").unwrap();
    }
}
