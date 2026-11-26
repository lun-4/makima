use makima_agent::agent::isolated_turn::IsolatedTurnEvent;
use makima_agent::{AgentRef, CancelToken};
use makima_providers::ImageSource;

use crate::components::btw_modal::BtwEvent;

use super::App;

impl App {
    pub(crate) fn start_btw(&mut self, question: String, images: Vec<ImageSource>) {
        if let Some(root) = self.root_agent.clone() {
            self.start_btw_on(&root, question, images);
        }
    }

    pub(super) fn start_btw_on(
        &mut self,
        agent: &AgentRef,
        question: String,
        images: Vec<ImageSource>,
    ) {
        let events = match agent.ask_isolated(question.clone(), images, CancelToken::none()) {
            Ok(events) => events,
            Err(error) => return self.flash(error.to_string()),
        };
        let (modal_tx, modal_rx) = flume::bounded(64);
        self.btw_modal.open(&question, modal_rx);
        smol::spawn(async move {
            while let Ok(event) = events.recv_async().await {
                let event = match event {
                    IsolatedTurnEvent::TextDelta(text) | IsolatedTurnEvent::ThinkingDelta(text) => {
                        BtwEvent::TextDelta(text)
                    }
                    IsolatedTurnEvent::Done | IsolatedTurnEvent::Cancelled => BtwEvent::Done,
                    IsolatedTurnEvent::Error(error) => BtwEvent::Error(error),
                };
                let terminal = matches!(event, BtwEvent::Done | BtwEvent::Error(_));
                if modal_tx.send_async(event).await.is_err() || terminal {
                    break;
                }
            }
        })
        .detach();
    }
}
