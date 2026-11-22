//! The remote state a history is addressed under, and the conversation it
//! started from, so a provider that keeps state per conversation can carry it
//! over instead of rebuilding it.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::id::MakimaId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConversationRelation {
    /// The same agent goes on after the previous conversation ended.
    Continues,
    /// A copy starts beside a history that may still be in use.
    Branches,
}

/// What a new conversation starts from. `shared` leading messages of its
/// requests equal those of the last request sent under `id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationLink {
    pub id: Arc<str>,
    pub relation: ConversationRelation,
    pub shared: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationState {
    pub id: Arc<str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<ConversationLink>,
    /// Messages in the last request sent under `id`; `None` until one is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent: Option<usize>,
}

impl ConversationState {
    pub fn new(id: Arc<str>) -> Self {
        Self {
            id,
            previous: None,
            sent: None,
        }
    }

    /// A conversation under a newly minted id.
    pub fn fresh(previous: Option<ConversationLink>) -> Self {
        Self {
            previous,
            ..Self::new(MakimaId::generate().to_string().into())
        }
    }

    /// The link a conversation started from this one carries when the first
    /// `kept` messages of this history stay as they are. A conversation that
    /// never reached the provider passes its own link on, so no successor
    /// names an id the provider never saw.
    pub fn link(&self, relation: ConversationRelation, kept: usize) -> Option<ConversationLink> {
        match self.sent {
            Some(sent) => Some(ConversationLink {
                id: Arc::clone(&self.id),
                relation,
                shared: sent.min(kept),
            }),
            None => self.previous.as_ref().map(|previous| ConversationLink {
                id: Arc::clone(&previous.id),
                relation: match (previous.relation, relation) {
                    (ConversationRelation::Continues, ConversationRelation::Continues) => {
                        ConversationRelation::Continues
                    }
                    _ => ConversationRelation::Branches,
                },
                shared: previous.shared.min(kept),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;
    use ConversationRelation::{Branches, Continues};

    const CURRENT: &str = "current";
    const EARLIER: &str = "earlier";

    fn state(
        sent: Option<usize>,
        previous: Option<(ConversationRelation, usize)>,
    ) -> ConversationState {
        ConversationState {
            id: CURRENT.into(),
            previous: previous.map(|(relation, shared)| ConversationLink {
                id: EARLIER.into(),
                relation,
                shared,
            }),
            sent,
        }
    }

    #[test_case(Some(5), None, Continues, 8, Some((CURRENT, Continues, 5)) ; "sent_caps_shared")]
    #[test_case(Some(5), None, Branches, 3, Some((CURRENT, Branches, 3)) ; "kept_caps_shared")]
    #[test_case(None, None, Continues, 3, None ; "unsent_without_previous_has_no_link")]
    #[test_case(None, Some((Continues, 4)), Continues, 9, Some((EARLIER, Continues, 4)) ; "unsent_passes_its_link_on")]
    #[test_case(None, Some((Continues, 4)), Branches, 2, Some((EARLIER, Branches, 2)) ; "unsent_branch_of_continuation_branches")]
    #[test_case(None, Some((Branches, 4)), Continues, 9, Some((EARLIER, Branches, 4)) ; "unsent_continuation_of_branch_branches")]
    fn link(
        sent: Option<usize>,
        previous: Option<(ConversationRelation, usize)>,
        relation: ConversationRelation,
        kept: usize,
        expected: Option<(&str, ConversationRelation, usize)>,
    ) {
        let link = state(sent, previous).link(relation, kept);
        assert_eq!(
            link.map(|link| (link.id.to_string(), link.relation, link.shared)),
            expected.map(|(id, relation, shared)| (id.to_owned(), relation, shared))
        );
    }
}
