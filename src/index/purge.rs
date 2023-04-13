use std::{collections::HashMap, sync::Arc, time::Duration};

use elasticsearch::indices::{IndicesDeleteParts, IndicesGetParts};
use serde_json::Value;
use tokio::task::JoinHandle;

use crate::{app_state::AppState, index::indexes::can_exist};

async fn purge_indices(state: Arc<AppState>) -> anyhow::Result<()> {
    log::info!(
        "Purging indices (TTL={}d)",
        state.index_ttl_days.unwrap_or(0)
    );
    let res = state
        .es_client
        .indices()
        .get(IndicesGetParts::Index(&[format!(
            "{}-*",
            state.index_name_prefix
        )
        .as_str()]))
        .send()
        .await?;
    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        return Err(anyhow::anyhow!(
            "Error getting indices: {} {}",
            status_code,
            body
        ));
    }
    let current_time = chrono::Utc::now();
    let indices = res.json::<HashMap<String, Value>>().await?;
    log::info!("Number of ondices available: {:?}", indices.len());
    for (name, _index_info) in indices {
        let can_exist = can_exist(
            &name,
            &current_time,
            state.index_ttl_days,
            state.index_allow_future_days,
        )?;
        if !can_exist {
            log::info!("Purging index: {}", name);
            let res = state
                .es_client
                .indices()
                .delete(IndicesDeleteParts::Index(&[name.as_str()]))
                .send()
                .await?;
            if !res.status_code().is_success() {
                let status_code = res.status_code();
                let body = res.text().await?;
                return Err(anyhow::anyhow!(
                    "Error purging index: {} {}",
                    status_code,
                    body
                ));
            }
        }
    }

    Ok(())
}

pub async fn spawn_index_purger(state: Arc<AppState>) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if let Err(e) = purge_indices(state.clone()).await {
                log::error!("Error purging index: {}", e);
            }
            tokio::time::sleep(Duration::from_secs(60 * 60)).await;
        }
    })
}
