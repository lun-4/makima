use std::collections::HashMap;
use std::sync::Arc;

use crate::arguments::ArgumentKind;
use crate::completion::CommandCompletion;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CompletionKind {
    String,
    Integer,
    Enum,
    File,
    Directory,
}

impl From<&ArgumentKind> for CompletionKind {
    fn from(kind: &ArgumentKind) -> Self {
        match kind {
            ArgumentKind::String => Self::String,
            ArgumentKind::Integer => Self::Integer,
            ArgumentKind::Enum(_) | ArgumentKind::EnumWithDefault { .. } => Self::Enum,
            ArgumentKind::File => Self::File,
            ArgumentKind::Directory => Self::Directory,
        }
    }
}

/// Kind defaults supplied by a frontend when opening a completion session.
#[derive(Clone, Default)]
pub struct CompletionProviders(HashMap<CompletionKind, Arc<dyn CommandCompletion>>);

impl CompletionProviders {
    pub fn with(mut self, kind: CompletionKind, provider: Arc<dyn CommandCompletion>) -> Self {
        self.0.insert(kind, provider);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn get(&self, kind: &ArgumentKind) -> Option<Arc<dyn CommandCompletion>> {
        self.0.get(&CompletionKind::from(kind)).cloned()
    }
}
