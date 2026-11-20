use crate::AgentId;
use makima_storage::id::MakimaId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewindBoundary {
    pub agent: AgentId,
    pub seq: u64,
    pub epoch: MakimaId,
}
