use std::sync::Arc;

use maki_agent::agent::isolated_turn::{IsolatedTurnEvent, IsolatedTurnRequest, run_isolated_turn};
use maki_providers::provider::Provider;
use maki_providers::{ImageSource, Model};
use maki_storage::id::SessionRef;

use crate::components::btw_modal::BtwEvent;

use super::App;

const BTW_FALLBACK_SYSTEM: &str = "You are a helpful coding assistant. Answer concisely \
from the conversation context.";

impl App {
    pub(crate) fn start_btw(
        &mut self,
        question: String,
        images: Vec<ImageSource>,
        provider: Arc<dyn Provider>,
        model: Model,
    ) {
        let history = self
            .shared_history
            .as_ref()
            .map(|history| Vec::clone(&history.load().messages))
            .unwrap_or_default();
        let system = self
            .btw_system
            .as_ref()
            .map(|system| String::clone(&system.load()))
            .filter(|system| !system.is_empty())
            .unwrap_or_else(|| BTW_FALLBACK_SYSTEM.to_string());
        let (service_tx, service_rx) = flume::bounded(64);
        let (modal_tx, modal_rx) = flume::bounded(64);
        self.btw_modal.open(&question, modal_rx);

        smol::spawn(run_isolated_turn(
            IsolatedTurnRequest {
                provider,
                model,
                history,
                system,
                question,
                images,
                session_id: Some(SessionRef::from(self.state.session.id)),
                cancel: maki_agent::CancelToken::none(),
            },
            service_tx,
        ))
        .detach();
        smol::spawn(async move {
            while let Ok(event) = service_rx.recv_async().await {
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
