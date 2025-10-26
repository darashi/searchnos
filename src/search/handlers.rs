use anyhow::bail;
use futures::sink::SinkExt;
use nostr_sdk::prelude::{RelayMessage, SubscriptionId};
use nostr_sdk::{Filter, JsonUtil};
use searchnos_db::StreamItem;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::Instrument;
use yawc::{frame::FrameView, WebSocket as YawcWebSocket};

use crate::app_state::AppState;
use crate::client_addr::ClientAddr;

pub struct SubscriptionHandle {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl SubscriptionHandle {
    async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.cancel.send(true);
        self.task.await
    }
}

fn spawn_subscription_task(
    mut subscription: searchnos_db::Subscription,
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    subscription_id: SubscriptionId,
    mut cancel_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                maybe_item = subscription.next() => {
                    match maybe_item {
                        Some(StreamItem::Event(event_json)) => {
                            if let Err(err) = send_event_json(&sender, &subscription_id, &event_json).await {
                                tracing::warn!(
                                    error = %err,
                                    subscription = %subscription_id,
                                    "failed to deliver subscription event"
                                );
                                break;
                            }
                        }
                        Some(StreamItem::Eose) => {
                            tracing::debug!(subscription = %subscription_id, "unexpected EOSE after snapshot");
                        }
                        None => break,
                    }
                }
                result = cancel_rx.changed() => {
                    match result {
                        Ok(_) => {
                            if *cancel_rx.borrow() {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    })
}

fn make_event_message(subscription_id: &SubscriptionId, event_json: &str) -> String {
    format!("[\"EVENT\",\"{}\",{}]", subscription_id, event_json)
}

async fn send_event_json(
    sender: &Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    subscription_id: &SubscriptionId,
    event_json: &str,
) -> anyhow::Result<()> {
    let message = make_event_message(subscription_id, event_json);
    sender.lock().await.send(FrameView::text(message)).await?;
    Ok(())
}

async fn send_eose(
    sender: &Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    subscription_id: &SubscriptionId,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::eose(subscription_id.clone());
    sender
        .lock()
        .await
        .send(FrameView::text(relay_msg.as_json()))
        .await?;
    Ok(())
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
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    subscriptions: Arc<Mutex<HashMap<SubscriptionId, SubscriptionHandle>>>,
    subscription_id: &SubscriptionId,
    filters: Vec<Filter>,
) -> anyhow::Result<()> {
    let filter_count = filters.len();
    let req_span = tracing::info_span!("req", subscription = %subscription_id, filter_count);

    let subscription_id = subscription_id.clone();
    async move {
        {
            let guard = subscriptions.lock().await;
            if !guard.contains_key(&subscription_id) {
                let num_ongoing_subscriptions = guard.len();
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
        }

        if filters.len() > state.max_filters {
            bail!(ClosedError::new(
                subscription_id.clone(),
                format!("error: too many filters: {}", filters.len())
            ));
        }

        let filters_json = serde_json::to_string(&filters)?;
        let started_at = Instant::now();
        let mut subscription = state.db.clone().subscribe_async(&filters_json).await?;
        let mut hits = 0usize;

        loop {
            match subscription.next().await {
                Some(StreamItem::Event(event_json)) => {
                    send_event_json(&sender, &subscription_id, &event_json).await?;
                    hits += 1;
                }
                Some(StreamItem::Eose) => {
                    let elapsed_ms = started_at.elapsed().as_millis() as u64;
                    tracing::info!(filters = %filters_json, hits, elapsed_ms, "search results sent");
                    send_eose(&sender, &subscription_id).await?;
                    break;
                }
                None => {
                    tracing::warn!(subscription = %subscription_id, "subscription stream ended before EOSE");
                    return Ok(());
                }
            }
        }

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task = spawn_subscription_task(
            subscription,
            sender.clone(),
            subscription_id.clone(),
            cancel_rx,
        );

        let handle = SubscriptionHandle {
            cancel: cancel_tx,
            task,
        };

        let previous = {
            let mut guard = subscriptions.lock().await;
            guard.remove(&subscription_id)
        };

        if let Some(old_handle) = previous {
            if let Err(err) = old_handle.shutdown().await {
                tracing::debug!(
                    error = %err,
                    subscription = %subscription_id,
                    "previous subscription task terminated with error"
                );
            }
        }

        {
            let mut guard = subscriptions.lock().await;
            guard.insert(subscription_id.clone(), handle);
        }

        Ok(())
    }
    .instrument(req_span)
    .await
}

pub async fn handle_close(
    subscriptions: Arc<Mutex<HashMap<SubscriptionId, SubscriptionHandle>>>,
    addr: ClientAddr,
    subscription_id: &SubscriptionId,
) -> anyhow::Result<()> {
    let remote_addr = addr.socket_addr();
    if let Some(header) = addr.forwarded_raw() {
        tracing::info!(
            remote_ip = %remote_addr.ip(),
            remote_port = remote_addr.port(),
            forwarded = header,
            subscription = %subscription_id,
            "CLOSE received"
        );
    } else {
        tracing::info!(
            remote_ip = %remote_addr.ip(),
            remote_port = remote_addr.port(),
            subscription = %subscription_id,
            "CLOSE received"
        );
    }

    let handle = {
        let mut guard = subscriptions.lock().await;
        guard.remove(subscription_id)
    };

    if let Some(handle) = handle {
        if let Err(err) = handle.shutdown().await {
            tracing::debug!(
                error = %err,
                subscription = %subscription_id,
                "subscription task terminated with error"
            );
        }
    }

    Ok(())
}
