use std::path::PathBuf;
use std::sync::LazyLock;

use crate::{StateDir, StorageError};

pub const PLANS_DIR: &str = "plans";
const SLUG_RETRIES: usize = 10;

static ADJECTIVES: LazyLock<Vec<&str>> =
    LazyLock::new(|| load_words(include_str!("words/adjectives.txt")));
static NOUNS: LazyLock<Vec<&str>> = LazyLock::new(|| load_words(include_str!("words/nouns.txt")));

fn load_words(text: &'static str) -> Vec<&'static str> {
    let words: Vec<&str> = text.lines().filter(|l| !l.is_empty()).collect();
    assert!(!words.is_empty(), "word list must not be empty");
    words
}

pub fn new_plan_path(dir: &StateDir) -> Result<PathBuf, StorageError> {
    let plans_dir = dir.ensure_subdir(PLANS_DIR)?;
    for _ in 0..SLUG_RETRIES {
        let path = plans_dir.join(format!("{}.md", generate_slug()));
        if !path.exists() {
            return Ok(path);
        }
    }
    Err(StorageError::SlugCollision)
}

fn generate_slug() -> String {
    let mut buf = [0u8; 12];
    getrandom::fill(&mut buf).expect("rng failed");
    let adj1_idx = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize % ADJECTIVES.len();
    let mut adj2_idx =
        u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize % ADJECTIVES.len();
    let noun_idx = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]) as usize % NOUNS.len();
    if adj1_idx == adj2_idx {
        adj2_idx = (adj2_idx + 1) % ADJECTIVES.len();
    }
    format!(
        "{}-{}-{}",
        ADJECTIVES[adj1_idx], ADJECTIVES[adj2_idx], NOUNS[noun_idx]
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StateDir;

    #[test]
    fn slug_format_invariants() {
        let slug = generate_slug();
        let parts: Vec<&str> = slug.split('-').collect();
        assert_eq!(parts.len(), 3, "expected 3 parts: {slug}");
        assert!(
            parts
                .iter()
                .all(|p| !p.is_empty() && p.chars().all(|c| c.is_ascii_lowercase())),
            "invalid part in slug: {slug}",
        );
        assert_ne!(parts[0], parts[1], "duplicate adjective in slug: {slug}");
    }

    #[test]
    fn new_plan_path_under_plans_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let path = new_plan_path(&dir).unwrap();
        assert!(path.starts_with(tmp.path().join("plans")));
        assert_eq!(path.extension().and_then(|e| e.to_str()), Some("md"));
    }
}
