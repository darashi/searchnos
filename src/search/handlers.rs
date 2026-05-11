use anyhow::bail;
use futures::sink::SinkExt;
use nostr_sdk::prelude::{RelayMessage, SubscriptionId};
use nostr_sdk::{Filter, JsonUtil};
use searchnos_db::{QueryStats, StreamItem};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::Instrument;
use yawc::{frame::Frame, HttpWebSocket as YawcWebSocket};

use crate::app_state::AppState;
use crate::client_addr::ClientAddr;

pub struct SubscriptionHandle {
    cancel: watch::Sender<bool>,
    task: JoinHandle<()>,
}

enum InitialQueryItem {
    Event(String),
    Finished(QueryStats),
    Failed(String),
}

impl SubscriptionHandle {
    async fn shutdown(self) -> Result<(), tokio::task::JoinError> {
        let _ = self.cancel.send(true);
        self.task.await
    }
}

async fn wait_for_cancel(cancel_rx: &mut watch::Receiver<bool>) {
    loop {
        if *cancel_rx.borrow() {
            return;
        }

        if cancel_rx.changed().await.is_err() {
            return;
        }
    }
}

fn spawn_subscription_task(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
    subscription_id: SubscriptionId,
    filters_json: String,
    live_filters_json: String,
    mut cancel_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let started_at = Instant::now();

        let live_subscription_result = tokio::select! {
            result = state.db.clone().subscribe_async(&live_filters_json) => result,
            _ = wait_for_cancel(&mut cancel_rx) => return,
        };
        let mut subscription = match live_subscription_result {
            Ok(subscription) => subscription,
            Err(err) => {
                let message = format!("error: failed to subscribe: {err}");
                if let Err(send_err) = send_closed(&sender, &subscription_id, &message).await {
                    tracing::warn!(
                        error = %send_err,
                        subscription = %subscription_id,
                        "failed to deliver CLOSED"
                    );
                }
                return;
            }
        };

        let (initial_tx, mut initial_rx) = mpsc::channel(32);
        let db = state.db.clone();
        let query_filters_json = filters_json.clone();
        let initial_query_task = tokio::task::spawn_blocking(move || {
            match db.stream_query_with_stats(&query_filters_json, |event_json| {
                initial_tx
                    .blocking_send(InitialQueryItem::Event(event_json))
                    .is_ok()
            }) {
                Ok(stats) => {
                    let _ = initial_tx.blocking_send(InitialQueryItem::Finished(stats));
                }
                Err(err) => {
                    let _ = initial_tx.blocking_send(InitialQueryItem::Failed(err.to_string()));
                }
            }
        });

        let mut hits = 0usize;
        let mut sent_event_ids = HashSet::new();

        loop {
            tokio::select! {
                maybe_item = initial_rx.recv() => {
                    match maybe_item {
                        Some(InitialQueryItem::Event(event_json)) => {
                            if let Some(event_id) = event_id_from_json(&event_json) {
                                sent_event_ids.insert(event_id);
                            }
                            if let Err(err) = send_event_json(&sender, &subscription_id, &event_json).await {
                                tracing::warn!(
                                    error = %err,
                                    subscription = %subscription_id,
                                    "failed to deliver initial query event"
                                );
                                return;
                            }
                            hits += 1;
                        }
                        Some(InitialQueryItem::Finished(initial_query)) => {
                            let elapsed_ms = duration_to_ms(started_at.elapsed());
                            tracing::info!(
                                filters = %filters_json,
                                filter_count = initial_query.filters.len(),
                                hits,
                                elapsed_ms,
                                db_elapsed_ms = duration_to_ms(initial_query.total_elapsed),
                                "search results sent"
                            );
                            log_query_profile(&initial_query);

                            if let Err(err) = send_eose(&sender, &subscription_id).await {
                                tracing::warn!(
                                    error = %err,
                                    subscription = %subscription_id,
                                    "failed to deliver EOSE"
                                );
                                return;
                            }
                            break;
                        }
                        Some(InitialQueryItem::Failed(err)) => {
                            let message = format!("error: failed to query subscription: {err}");
                            if let Err(send_err) = send_closed(&sender, &subscription_id, &message).await {
                                tracing::warn!(
                                    error = %send_err,
                                    subscription = %subscription_id,
                                    "failed to deliver CLOSED"
                                );
                            }
                            return;
                        }
                        None => {
                            tracing::warn!(subscription = %subscription_id, "initial query stream ended before EOSE");
                            return;
                        }
                    }
                }
                _ = wait_for_cancel(&mut cancel_rx) => return,
            }
        }

        if let Err(err) = initial_query_task.await {
            tracing::warn!(
                error = %err,
                subscription = %subscription_id,
                "initial query task failed"
            );
            return;
        }

        loop {
            tokio::select! {
                maybe_item = subscription.next() => {
                    match maybe_item {
                        Some(StreamItem::Event(event_json)) => {
                            if let Some(event_id) = event_id_from_json(&event_json) {
                                if !sent_event_ids.insert(event_id) {
                                    continue;
                                }
                            }
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
                            tracing::debug!(subscription = %subscription_id, "ignored live subscription EOSE");
                        }
                        None => break,
                    }
                }
                _ = wait_for_cancel(&mut cancel_rx) => break,
            }
        }
    })
}

fn event_id_from_json(event_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(event_json).ok()?;
    value.get("id")?.as_str().map(ToOwned::to_owned)
}

fn make_event_message(subscription_id: &SubscriptionId, event_json: &str) -> String {
    format!("[\"EVENT\",\"{}\",{}]", subscription_id, event_json)
}

async fn send_event_json(
    sender: &Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
    subscription_id: &SubscriptionId,
    event_json: &str,
) -> anyhow::Result<()> {
    let message = make_event_message(subscription_id, event_json);
    sender.lock().await.send(Frame::text(message)).await?;
    tokio::task::yield_now().await;
    Ok(())
}

fn filters_json_with_limit(filters: &[Filter], limit: usize) -> anyhow::Result<String> {
    let mut filters = filters.to_vec();
    for filter in &mut filters {
        filter.limit = Some(limit);
    }
    serde_json::to_string(&filters).map_err(Into::into)
}

async fn send_eose(
    sender: &Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
    subscription_id: &SubscriptionId,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::eose(subscription_id.clone());
    sender
        .lock()
        .await
        .send(Frame::text(relay_msg.as_json()))
        .await?;
    Ok(())
}

async fn send_closed(
    sender: &Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
    subscription_id: &SubscriptionId,
    message: &str,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::closed(subscription_id.clone(), message);
    sender
        .lock()
        .await
        .send(Frame::text(relay_msg.as_json()))
        .await?;
    Ok(())
}

fn log_query_profile(stats: &QueryStats) {
    let total_candidates: usize = stats
        .filters
        .iter()
        .map(|filter| filter.candidate_count)
        .sum();

    let filter_details = stats
        .filters
        .iter()
        .enumerate()
        .map(|(index, filter_stats)| {
            format!(
                "#{}:matched={} candidates={} index_ms={} post_ms={}",
                index,
                filter_stats.matched_event_count,
                filter_stats.candidate_count,
                duration_to_ms(filter_stats.index_scan_duration),
                duration_to_ms(filter_stats.post_processing_duration)
            )
        })
        .collect::<Vec<_>>()
        .join(" | ");

    tracing::debug!(
        db_elapsed_ms = duration_to_ms(stats.total_elapsed),
        index_scan_ms = duration_to_ms(stats.index_scan_duration),
        post_processing_ms = duration_to_ms(stats.post_processing_duration),
        filter_count = stats.filters.len(),
        candidate_count = total_candidates,
        filter_details = %filter_details,
        "query profile"
    );
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
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

fn validate_search_filters(filters: &[Filter]) -> Result<(), String> {
    const SEARCH_FILTER_REQUIRED: &str = "error: search filter is required";

    if filters.is_empty() {
        return Err(SEARCH_FILTER_REQUIRED.to_string());
    }

    for filter in filters {
        match filter.search.as_deref() {
            Some(search) if !search.trim().is_empty() => {}
            Some(_) | None => return Err(SEARCH_FILTER_REQUIRED.to_string()),
        }
    }

    Ok(())
}

pub async fn handle_req(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
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

        if let Err(message) = validate_search_filters(&filters) {
            bail!(ClosedError::new(subscription_id.clone(), message));
        }

        let filters_json = serde_json::to_string(&filters)?;
        let live_filters_json = filters_json_with_limit(&filters, 0)?;
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

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task = spawn_subscription_task(
            state.clone(),
            sender.clone(),
            subscription_id.clone(),
            filters_json,
            live_filters_json,
            cancel_rx,
        );

        let handle = SubscriptionHandle {
            cancel: cancel_tx,
            task,
        };

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_search_filters_accepts_non_empty_search() {
        let filters = vec![Filter::new().search("nostr")];

        assert!(validate_search_filters(&filters).is_ok());
    }

    #[test]
    fn validate_search_filters_rejects_empty_filter_list() {
        let err = validate_search_filters(&[]).unwrap_err();

        assert_eq!(err, "error: search filter is required");
    }

    #[test]
    fn validate_search_filters_rejects_missing_search() {
        let filters = vec![Filter::new().limit(1)];
        let err = validate_search_filters(&filters).unwrap_err();

        assert_eq!(err, "error: search filter is required");
    }

    #[test]
    fn validate_search_filters_rejects_empty_search() {
        let filters = vec![Filter::new().search("   ")];
        let err = validate_search_filters(&filters).unwrap_err();

        assert_eq!(err, "error: search filter is required");
    }
}
