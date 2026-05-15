use crate::app_state::AppState;
use crate::client_addr::ClientAddr;
use crate::relay_sender::RelaySender;
use nostr_sdk::prelude::SubscriptionId;
use nostr_sdk::Filter;
use searchnos_db::StreamItem;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex};
use tokio::task::JoinHandle;
use tracing::Instrument;

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

#[derive(Clone, Default)]
pub struct SubscriptionManager {
    inner: Arc<Mutex<HashMap<SubscriptionId, SubscriptionHandle>>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self::default()
    }

    async fn len_if_new(&self, subscription_id: &SubscriptionId) -> Option<usize> {
        let guard = self.inner.lock().await;
        if guard.contains_key(subscription_id) {
            None
        } else {
            Some(guard.len())
        }
    }

    async fn replace(
        &self,
        subscription_id: SubscriptionId,
        handle: SubscriptionHandle,
    ) -> Result<(), tokio::task::JoinError> {
        let previous = {
            let mut guard = self.inner.lock().await;
            guard.remove(&subscription_id)
        };

        if let Some(old_handle) = previous {
            old_handle.shutdown().await?;
        }

        let mut guard = self.inner.lock().await;
        guard.insert(subscription_id, handle);
        Ok(())
    }

    pub async fn close(&self, addr: ClientAddr, subscription_id: &SubscriptionId) {
        log_close(&addr, subscription_id);

        let handle = {
            let mut guard = self.inner.lock().await;
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
    }

    pub async fn close_all(&self, addr: ClientAddr) {
        let ids = {
            let guard = self.inner.lock().await;
            guard.keys().cloned().collect::<Vec<_>>()
        };

        for subscription_id in ids {
            self.close(addr.clone(), &subscription_id).await;
        }
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
    sender: RelaySender,
    subscription_id: SubscriptionId,
    filters_json: String,
    mut cancel_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let started_at = Instant::now();

        let subscription_result = tokio::select! {
            result = async { state.db.clone().subscribe(&filters_json) } => result,
            _ = wait_for_cancel(&mut cancel_rx) => return,
        };
        let mut subscription = match subscription_result {
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

        let mut hits = 0usize;
        let mut initial_done = false;

        loop {
            tokio::select! {
                maybe_item = subscription.next() => {
                    match maybe_item {
                        Some(StreamItem::Event(event_json)) => {
                            if !initial_done {
                                hits += 1;
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
                            initial_done = true;
                            let elapsed_ms = duration_to_ms(started_at.elapsed());
                            tracing::info!(
                                filters = %filters_json,
                                hits,
                                elapsed_ms,
                                "search results sent"
                            );

                            if let Err(err) = send_eose(&sender, &subscription_id).await {
                                tracing::warn!(
                                    error = %err,
                                    subscription = %subscription_id,
                                    "failed to deliver EOSE"
                                );
                                break;
                            }
                        }
                        None => break,
                    }
                }
                _ = wait_for_cancel(&mut cancel_rx) => break,
            }
        }
    })
}

fn make_event_message(subscription_id: &SubscriptionId, event_json: &str) -> String {
    format!("[\"EVENT\",\"{}\",{}]", subscription_id, event_json)
}

async fn send_event_json(
    sender: &RelaySender,
    subscription_id: &SubscriptionId,
    event_json: &str,
) -> anyhow::Result<()> {
    let message = make_event_message(subscription_id, event_json);
    sender.text(message).await?;
    tokio::task::yield_now().await;
    Ok(())
}

async fn send_eose(sender: &RelaySender, subscription_id: &SubscriptionId) -> anyhow::Result<()> {
    sender.eose(subscription_id.clone()).await
}

async fn send_closed(
    sender: &RelaySender,
    subscription_id: &SubscriptionId,
    message: &str,
) -> anyhow::Result<()> {
    sender.closed(subscription_id.clone(), message).await
}

fn duration_to_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[derive(thiserror::Error, Debug)]
pub enum ClientMessageError {
    #[error("{message}")]
    Closed {
        subscription_id: SubscriptionId,
        message: String,
    },
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl ClientMessageError {
    fn closed(subscription_id: SubscriptionId, message: String) -> Self {
        Self::Closed {
            subscription_id,
            message,
        }
    }
}

fn log_close(addr: &ClientAddr, subscription_id: &SubscriptionId) {
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
    sender: RelaySender,
    subscriptions: SubscriptionManager,
    subscription_id: &SubscriptionId,
    filters: Vec<Filter>,
) -> Result<(), ClientMessageError> {
    let filter_count = filters.len();
    let req_span = tracing::info_span!("req", subscription = %subscription_id, filter_count);

    let subscription_id = subscription_id.clone();
    async move {
        if let Some(num_ongoing_subscriptions) = subscriptions.len_if_new(&subscription_id).await {
            if num_ongoing_subscriptions + 1 > state.max_subscriptions {
                return Err(ClientMessageError::closed(
                    subscription_id.clone(),
                    format!(
                        "error: too many ongoing subscriptions: {}",
                        num_ongoing_subscriptions
                    ),
                ));
            }
        }

        if filters.len() > state.max_filters {
            return Err(ClientMessageError::closed(
                subscription_id.clone(),
                format!("error: too many filters: {}", filters.len()),
            ));
        }

        if let Err(message) = validate_search_filters(&filters) {
            return Err(ClientMessageError::closed(subscription_id.clone(), message));
        }

        let filters_json = serde_json::to_string(&filters).map_err(anyhow::Error::from)?;

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let task = spawn_subscription_task(
            state.clone(),
            sender.clone(),
            subscription_id.clone(),
            filters_json,
            cancel_rx,
        );

        let handle = SubscriptionHandle {
            cancel: cancel_tx,
            task,
        };

        if let Err(err) = subscriptions.replace(subscription_id.clone(), handle).await {
            tracing::debug!(
                error = %err,
                subscription = %subscription_id,
                "previous subscription task terminated with error"
            );
        }

        Ok(())
    }
    .instrument(req_span)
    .await
}

pub async fn handle_close(
    subscriptions: SubscriptionManager,
    addr: ClientAddr,
    subscription_id: &SubscriptionId,
) {
    subscriptions.close(addr, subscription_id).await;
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
