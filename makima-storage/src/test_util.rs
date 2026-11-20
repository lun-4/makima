use crate::{
    id::AgentId,
    session_state::{StoredAgentLifecycle, StoredAgentRecord},
};

pub fn test_record(id: AgentId, parent: Option<AgentId>) -> StoredAgentRecord {
    StoredAgentRecord {
        id,
        parent,
        depth: u32::from(parent.is_some()),
        transcript_key: parent.map_or_else(String::new, |_| id.to_string()),
        lifecycle: StoredAgentLifecycle::Open,
        ancestry_unknown: false,
        label: None,
        spawned_by_tool_use_id: None,
        spec: None,
        restorable: Ok(()),
        usage: Default::default(),
        created_seq: 0,
        updated_seq: 0,
        legacy: false,
    }
}
