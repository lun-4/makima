use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentContextSnapshot {
    cwd: Arc<PathBuf>,
    generation: u64,
}

impl AgentContextSnapshot {
    pub fn new(cwd: PathBuf) -> Self {
        Self {
            cwd: Arc::new(cwd),
            generation: 0,
        }
    }

    pub fn cwd(&self) -> &Path {
        &self.cwd
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn change_directory(&self, cwd: PathBuf) -> Self {
        Self {
            cwd: Arc::new(cwd),
            generation: self.generation.wrapping_add(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AgentContextSnapshot;
    use std::path::PathBuf;

    #[test]
    fn context_change_does_not_mutate_captured_generation() {
        let original = AgentContextSnapshot::new(PathBuf::from("/original"));
        let inherited = original.clone();
        let changed = original.change_directory(PathBuf::from("/changed"));
        assert_eq!(inherited.cwd(), original.cwd());
        assert_eq!(inherited.generation(), 0);
        assert_eq!(changed.cwd(), PathBuf::from("/changed"));
        assert_eq!(changed.generation(), 1);
    }
}
