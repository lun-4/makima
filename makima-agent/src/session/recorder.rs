use std::sync::Arc;

use crate::{Envelope, journal::SessionJournal};

pub(super) struct SessionRecorder {
    _journal: Arc<SessionJournal>,
    #[cfg(test)]
    pub(super) observed: usize,
}

impl SessionRecorder {
    pub(super) fn new(journal: Arc<SessionJournal>) -> Self {
        Self {
            _journal: journal,
            #[cfg(test)]
            observed: 0,
        }
    }

    pub(super) fn observe(&mut self, _: &Envelope) {
        #[cfg(test)]
        {
            self.observed += 1;
        }
    }
}
