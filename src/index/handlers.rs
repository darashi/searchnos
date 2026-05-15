use nostr_sdk::{Event, JsonUtil};
use std::sync::Arc;

use crate::app_state::AppState;
use crate::db_adapter::insert_event_json;

pub async fn handle_update(state: Arc<AppState>, event: &Event) -> anyhow::Result<()> {
    let db = state.db.clone();
    let raw = event.as_json();
    tokio::task::spawn_blocking(move || insert_event_json(&db, &raw)).await??;
    Ok(())
}
