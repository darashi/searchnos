use anyhow::Context;
use axum::extract::ws::{Message, WebSocket};
use chrono::{DateTime, Utc};
use futures::sink::SinkExt;
use nostr_sdk::prelude::{RelayMessage, SubscriptionId};
use std::collections::HashMap;
use std::{net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::app_state::AppState;
use crate::search::filter::Filter;
use crate::search::query::ElasticsearchQuery;

use super::query;

async fn send_events(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: &SubscriptionId,
    events: Vec<nostr_sdk::Event>,
) -> anyhow::Result<()> {
    for event in events {
        let relay_msg = RelayMessage::new_event(subscription_id.clone(), event);
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
    let relay_msg = RelayMessage::new_eose(subscription_id.clone());
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

pub async fn handle_req(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if msg.len() < 3 {
        return Err(anyhow::anyhow!("too few arguments"));
    }
    let subscription_id: String =
        serde_json::from_value(msg[1].clone()).context("invalid subscription id")?;
    let subscription_id = SubscriptionId::new(subscription_id);
    let filters = msg[2..].to_vec();

    log::info!(
        "{} [{}] req {:?}",
        addr,
        subscription_id.to_string(),
        filters
    );

    let num_ongoing_subscriptions = join_handles.lock().await.len();
    if num_ongoing_subscriptions + 1 > state.max_subscriptions {
        return Err(anyhow::anyhow!(
            "too many ongoing subscriptions: {}",
            num_ongoing_subscriptions
        ));
    }

    // expire old subscription if exists
    stop_subscription(join_handles.clone(), &subscription_id.clone()).await;

    // prepare filters and cursors
    let filters: Vec<Filter> = filters
        .into_iter()
        .map(|f| serde_json::from_value::<Filter>(f).context("parsing filter"))
        .collect::<Result<_, _>>()?;

    // check filter length
    if filters.len() > state.max_filters {
        return Err(anyhow::anyhow!("too many filters: {}", filters.len()));
    }

    let filters: Vec<Filter> = filters
        .into_iter()
        .filter(|f| {
            if let Some(s) = &f.search {
                !s.is_empty()
            } else {
                false
            }
        })
        .collect();

    if filters.is_empty() {
        return Err(anyhow::anyhow!("only filter with search is supported"));
    }

    let mut cursors: Vec<Option<DateTime<Utc>>> = filters.iter().map(|_| None).collect();

    // do the first search
    for (filter, cursor) in filters.iter().zip(cursors.iter_mut()) {
        let query = ElasticsearchQuery::from_filter(filter.clone(), None);

        let new_cursor = query_then_send(
            addr,
            state.clone(),
            sender.clone(),
            subscription_id.clone(),
            query,
            None,
        );
        *cursor = new_cursor.await?;
    }
    send_eose(sender.clone(), &subscription_id).await?;

    let sid_ = subscription_id.clone();
    let join_handle = tokio::spawn(async move {
        let mut cursors = cursors;
        loop {
            let wait = 5.0 + (rand::random::<f64>() * 5.0); // TODO better scheduling
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(wait)).await;
            log::info!("{} [{}] cont. {:?}", addr, &sid_.to_string(), filters);

            for (filter, cursor) in filters.iter().zip(cursors.iter_mut()) {
                let query = ElasticsearchQuery::from_filter(filter.clone(), *cursor);

                let res = query_then_send(
                    addr,
                    state.clone(),
                    sender.clone(),
                    sid_.clone(),
                    query,
                    *cursor,
                )
                .await;
                match res {
                    Ok(new_cursor) => {
                        *cursor = new_cursor;
                    }
                    Err(e) => {
                        log::error!("error in continuing search: {:?}", e);
                    }
                }
            }
        }
    });
    join_handles
        .lock()
        .await
        .insert(subscription_id.to_string(), join_handle);

    Ok(())
}

async fn stop_subscription(
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    subscription_id: &SubscriptionId,
) {
    let removed = join_handles
        .lock()
        .await
        .remove(&subscription_id.to_string());
    if let Some(task) = removed {
        task.abort();
    }
}

pub async fn handle_close(
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if msg.len() != 2 {
        return Err(anyhow::anyhow!("invalid array length"));
    }

    let subscription_id = serde_json::from_value::<SubscriptionId>(msg[1].clone())
        .context("parsing subscription id")?;

    log::info!("{} CLOSE {:?}", addr, subscription_id.to_string());

    stop_subscription(join_handles, &subscription_id).await;

    Ok(())
}
