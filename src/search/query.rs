use std::sync::Arc;

use nostr_sdk::prelude::Event;
use nostr_sdk::{Filter, JsonUtil};
use searchnos_db::SearchnosDB;

pub async fn execute(
    db: Arc<SearchnosDB>,
    filters: Vec<Filter>,
) -> Result<Vec<Event>, anyhow::Error> {
    let filters_json = serde_json::to_string(&filters)?;

    let raw_events =
        tokio::task::spawn_blocking(move || -> Result<_, searchnos_db::SearchnosDBError> {
            db.flush()?;
            db.query(&filters_json)
        })
        .await??;

    raw_events
        .into_iter()
        .map(|raw| Event::from_json(raw.as_bytes()).map_err(|e| e.into()))
        .collect()
}
