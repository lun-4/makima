use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use flume::{Receiver, RecvError, Sender, TryRecvError};
use makima_domain::LockExt;
use makima_storage::id::{AgentId, MakimaId};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct AnswerKey {
    agent_id: AgentId,
    ask_id: MakimaId,
}

impl AnswerKey {
    pub fn agent_id(&self) -> AgentId {
        self.agent_id
    }
}

#[derive(Debug, Error)]
#[error("this ask is no longer pending in the session")]
pub struct AnswerError;

#[derive(Debug, Clone, Default)]
pub struct AnswerRouter(Arc<Mutex<HashMap<AnswerKey, Sender<String>>>>);

impl AnswerRouter {
    pub fn register(&self, agent_id: AgentId) -> PendingAnswer {
        let key = AnswerKey {
            agent_id,
            ask_id: MakimaId::generate(),
        };
        let (tx, rx) = flume::bounded(1);
        self.0.lock_unpoisoned().insert(key.clone(), tx);
        PendingAnswer {
            router: self.clone(),
            key,
            rx,
        }
    }

    pub fn answer(&self, key: AnswerKey, payload: String) -> Result<(), AnswerError> {
        let tx = self.0.lock_unpoisoned().remove(&key).ok_or(AnswerError)?;
        tx.try_send(payload).map_err(|_| AnswerError)
    }
}

#[derive(Debug)]
pub struct PendingAnswer {
    router: AnswerRouter,
    key: AnswerKey,
    rx: Receiver<String>,
}

impl PendingAnswer {
    pub fn key(&self) -> AnswerKey {
        self.key.clone()
    }

    pub async fn recv(&self) -> Result<String, RecvError> {
        self.rx.recv_async().await
    }

    pub fn try_recv(&self) -> Result<String, TryRecvError> {
        self.rx.try_recv()
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

impl Drop for PendingAnswer {
    fn drop(&mut self) {
        self.router.0.lock_unpoisoned().remove(&self.key);
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::AnswerRouter;
    use makima_storage::id::AgentId;

    const FIRST: &str = "first response";
    const SECOND: &str = "second response";

    #[test_case(true; "same_agent")]
    #[test_case(false; "different_agents")]
    fn concurrent_asks_receive_only_their_own_answer(same_agent: bool) {
        let router = AnswerRouter::default();
        let agent = AgentId::generate();
        let first = router.register(agent);
        let second = router.register(if same_agent {
            agent
        } else {
            AgentId::generate()
        });

        router.answer(second.key(), SECOND.into()).unwrap();
        assert!(first.is_empty());
        assert_eq!(second.try_recv().unwrap(), SECOND);
        assert!(router.answer(second.key(), FIRST.into()).is_err());
        router.answer(first.key(), FIRST.into()).unwrap();
        assert_eq!(first.try_recv().unwrap(), FIRST);
    }

    #[test]
    fn answers_cannot_cross_sessions_or_reach_a_replacement_ask() {
        let router = AnswerRouter::default();
        let other = AnswerRouter::default();
        let agent = AgentId::generate();
        let pending = router.register(agent);
        let stale = pending.key();
        assert!(other.answer(stale.clone(), FIRST.into()).is_err());
        drop(pending);
        let replacement = router.register(agent);
        assert!(router.answer(stale, FIRST.into()).is_err());
        assert!(replacement.is_empty());
    }
}
