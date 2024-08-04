use anyhow::bail;
use axum::extract::ws::{Message, WebSocket};
use chrono::{DateTime, Utc};
use futures::sink::SinkExt;
use nostr_sdk::prelude::{RelayMessage, SubscriptionId};
use std::collections::HashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;

use crate::app_state::AppState;
use crate::search::query::ElasticsearchQuery;

use super::query;
use nostr_sdk::{Filter, JsonUtil};

async fn send_events(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: &SubscriptionId,
    events: Vec<nostr_sdk::Event>,
) -> anyhow::Result<()> {
    for event in events {
        let relay_msg = RelayMessage::event(subscription_id.clone(), event);
        sender
            .lock()
            .await
            .send(Message::Text(relay_msg.as_json()))
            .await?;
    }

    Ok(())
}
async fn send_eose(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: &SubscriptionId,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::eose(subscription_id.clone());
    sender
        .lock()
        .await
        .send(Message::Text(relay_msg.as_json()))
        .await?;
    Ok(())
}

async fn query_then_send(
    addr: SocketAddr,
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: SubscriptionId,
    query: query::ElasticsearchQuery,
    cursor: Option<DateTime<Utc>>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let t0 = std::time::Instant::now();
    let (events, new_cursor) = query
        .execute(&state.es_client, &state.index_alias_name, cursor)
        .await?;
    let search_time = t0.elapsed().as_millis();
    let num_hits = events.len();
    send_events(sender.clone(), &subscription_id, events).await?;

    log::info!(
        "{} [{}] sent {} event(s), searched in {} ms",
        addr,
        subscription_id.to_string(),
        num_hits,
        search_time,
    );
    Ok(new_cursor)
}

#[derive(thiserror::Error, Debug)]
pub struct ClosedError {
    pub subscription_id: SubscriptionId,
    message: String,
}

impl std::fmt::Display for ClosedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl ClosedError {
    fn new(subscription_id: SubscriptionId, message: String) -> Self {
        Self {
            subscription_id,
            message,
        }
    }
}

pub async fn handle_req(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscriptions: Arc<Mutex<HashMap<SubscriptionId, Vec<Filter>>>>,
    addr: SocketAddr,
    subscription_id: &SubscriptionId,
    filters: Vec<nostr_sdk::Filter>,
) -> anyhow::Result<()> {
    log::info!(
        "{} [{}] req {:?}",
        addr,
        subscription_id.to_string(),
        filters
    );

    if !subscriptions.lock().await.contains_key(subscription_id) {
        let num_ongoing_subscriptions = subscriptions.lock().await.len();
        if num_ongoing_subscriptions + 1 > state.max_subscriptions {
            bail!(ClosedError::new(
                subscription_id.clone(),
                format!(
                    "error: too many ongoing subscriptions: {}",
                    num_ongoing_subscriptions
                )
            ));
        }
    }

    // check filter length
    if filters.len() > state.max_filters {
        bail!(ClosedError::new(
            subscription_id.clone(),
            format!("error: too many filters: {}", filters.len())
        ));
    }

    // do the first search
    for filter in filters.iter() {
        let query = ElasticsearchQuery::from_filter(filter.clone(), None);

        query_then_send(
            addr,
            state.clone(),
            sender.clone(),
            subscription_id.clone(),
            query,
            None,
        )
        .await?;
    }
    send_eose(sender.clone(), subscription_id).await?;

    subscriptions
        .lock()
        .await
        .insert(subscription_id.clone(), filters);

    Ok(())
}

pub async fn handle_close(
    subscriptions: Arc<Mutex<HashMap<SubscriptionId, Vec<Filter>>>>,
    addr: SocketAddr,
    subscription_id: &SubscriptionId,
) -> anyhow::Result<()> {
    log::info!("{} CLOSE {:?}", addr, subscription_id.to_string());
    subscriptions.lock().await.remove(subscription_id);

    Ok(())
}
