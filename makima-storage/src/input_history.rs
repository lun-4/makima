use std::collections::VecDeque;
use std::fs;

use crate::{StateDir, StorageError, atomic_write};

const HISTORY_FILE: &str = "input_history.json";
pub const MAX_ENTRIES: usize = 100;

#[derive(Debug)]
pub struct InputHistory {
    entries: VecDeque<String>,
    max_entries: usize,
}

impl Default for InputHistory {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            max_entries: MAX_ENTRIES,
        }
    }
}

impl InputHistory {
    pub fn load(dir: &StateDir, max_entries: usize) -> Self {
        let path = dir.path().join(HISTORY_FILE);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => {
                return Self {
                    entries: VecDeque::new(),
                    max_entries,
                };
            }
        };
        let items: Vec<String> = serde_json::from_slice(&data).unwrap_or_default();
        let mut history = Self {
            entries: VecDeque::with_capacity(max_entries),
            max_entries,
        };
        for entry in items {
            history.push_inner(entry);
        }
        history
    }

    pub fn save(&self, dir: &StateDir) -> Result<(), StorageError> {
        let data = serde_json::to_vec(&self.entries)?;
        atomic_write(&dir.path().join(HISTORY_FILE), &data)
    }

    pub fn push(&mut self, entry: String) {
        let trimmed = entry.trim().to_string();
        if trimmed.is_empty() {
            return;
        }
        self.push_inner(trimmed);
    }

    fn push_inner(&mut self, entry: String) {
        if self.entries.back().is_some_and(|last| *last == entry) {
            return;
        }
        if self.entries.len() == self.max_entries {
            self.entries.pop_front();
        }
        self.entries.push_back(entry);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, index: usize) -> Option<&str> {
        self.entries.get(index).map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    fn tmp_dir() -> (tempfile::TempDir, StateDir) {
        let tmp = tempfile::tempdir().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        (tmp, dir)
    }

    #[test]
    fn roundtrip() {
        let (_tmp, dir) = tmp_dir();
        let mut history = InputHistory::load(&dir, MAX_ENTRIES);
        history.push("a".into());
        history.push("b".into());
        history.push("c".into());
        history.save(&dir).unwrap();
        let loaded = InputHistory::load(&dir, MAX_ENTRIES);
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.get(0), Some("a"));
        assert_eq!(loaded.get(2), Some("c"));
    }

    #[test]
    fn truncates_to_max_entries() {
        let mut history = InputHistory::default();
        for i in 0..150 {
            history.push(format!("entry{i}"));
        }
        assert_eq!(history.len(), MAX_ENTRIES);
        assert_eq!(history.get(0), Some("entry50"));
        assert_eq!(history.get(MAX_ENTRIES - 1), Some("entry149"));
    }

    #[test]
    fn rejects_consecutive_duplicates() {
        let mut history = InputHistory::default();
        history.push("a".into());
        history.push("a".into());
        history.push("b".into());
        history.push("b".into());
        history.push("a".into());
        assert_eq!(history.len(), 3);
        assert_eq!(history.get(0), Some("a"));
        assert_eq!(history.get(1), Some("b"));
        assert_eq!(history.get(2), Some("a"));
    }

    #[test]
    fn push_trims_and_rejects_blank() {
        let mut history = InputHistory::default();
        history.push("".into());
        history.push("   ".into());
        history.push("\n".into());
        assert!(history.is_empty());

        history.push("  hello  ".into());
        assert_eq!(history.get(0), Some("hello"));
    }

    #[test_case(None      ; "missing_file")]
    #[test_case(Some(b"not json" as &[u8]) ; "corrupt_file")]
    fn load_bad_state_returns_empty(content: Option<&[u8]>) {
        let (_tmp, dir) = tmp_dir();
        if let Some(data) = content {
            fs::write(dir.path().join(HISTORY_FILE), data).unwrap();
        }
        let history = InputHistory::load(&dir, MAX_ENTRIES);
        assert!(history.is_empty());
    }
}
