use std::path::Path;
use std::sync::Arc;

use makima_agent::agent::build_system_prompt_with_def;
use makima_agent::agent::isolated_turn::{
    IsolatedTurnEvent, IsolatedTurnRequest, run_isolated_turn,
};
use makima_agent::context::root_context;
use makima_agent::state::StateError;
use makima_agent::template::env_vars_for;
use makima_agent::{AgentMode, AgentRef, CancelToken};
use makima_providers::{ConversationRelation, ImageSource};
use makima_storage::id::SessionRef;

use crate::components::btw_modal::BtwEvent;

use super::App;

pub(super) const BTW_FALLBACK_SYSTEM: &str = "You are a helpful coding assistant. Answer concisely \
from the conversation context.";

impl App {
    pub(super) fn btw_system(&self, root: &AgentRef, cwd: &Path) -> Result<String, StateError> {
        let mut system = match &self.btw_prompt.override_text {
            Some(system) => system.clone(),
            None => match root_context(&root.state_access())? {
                Some(recorded) => {
                    let mode = AgentMode::Build;
                    build_system_prompt_with_def(
                        &env_vars_for(cwd),
                        &self.lua_event_handle.mode_registry().current(&mode),
                        &mode,
                        &recorded.instructions.text,
                        &recorded.slots,
                    )
                }
                None => BTW_FALLBACK_SYSTEM.into(),
            },
        };
        if let Some(append) = &self.btw_prompt.append_text {
            system.push('\n');
            system.push_str(append);
        }
        Ok(system)
    }

    pub(crate) fn start_btw(&mut self, question: String, images: Vec<ImageSource>) {
        let Some(root) = &self.root_agent else {
            return;
        };
        let config = self.root_config();
        let system = match self.btw_system(root, config.context.cwd()) {
            Ok(system) => system,
            Err(error) => return self.flash(error.to_string()),
        };
        let projection = self.session_projection();
        let live = self
            .shared_history
            .as_ref()
            .map(|history| history.load_full());
        let snapshot = live.as_deref().or_else(|| projection.root_history());
        let previous = snapshot.and_then(|snapshot| {
            snapshot
                .conversation
                .as_ref()?
                .link(ConversationRelation::Branches, snapshot.messages.len())
        });
        let history = snapshot.map_or_else(Vec::new, |snapshot| Vec::clone(&snapshot.messages));
        self.start_btw_request(IsolatedTurnRequest {
            provider: Arc::clone(&config.provider),
            model: config.model.clone(),
            history,
            previous,
            system,
            question,
            images,
            session_id: Some(SessionRef::from(projection.id)),
            cancel: CancelToken::none(),
        });
    }

    pub(super) fn start_btw_request(&mut self, request: IsolatedTurnRequest) {
        let (service_tx, service_rx) = flume::bounded(64);
        let (modal_tx, modal_rx) = flume::bounded(64);
        self.btw_modal.open(&request.question, modal_rx);
        smol::spawn(run_isolated_turn(request, service_tx)).detach();
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
