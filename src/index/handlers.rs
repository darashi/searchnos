use nostr_sdk::prelude::Event;
use std::sync::Arc;

use crate::app_state::AppState;
use crate::db_adapter::insert_event_json;

pub async fn handle_update(state: Arc<AppState>, event: &Event) -> anyhow::Result<()> {
    if event.kind.is_ephemeral() {
        event.verify()?;
        let _ = state.ephemeral_events.send(Arc::new(event.clone()));
        return Ok(());
    }

    let db = state.db.clone();
    let raw = event.as_json();
    tokio::task::spawn_blocking(move || insert_event_json(&db, &raw)).await??;
    Ok(())
}
