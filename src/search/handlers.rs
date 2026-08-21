use crate::app_state::AppState;
use crate::client_addr::ClientAddr;
use crate::relay_sender::RelaySender;
use nostr_sdk::prelude::SubscriptionId;
use nostr_sdk::{Filter, Timestamp};
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{oneshot, watch, Mutex};
use tokio::task::JoinHandle;
use tracing::Instrument;

const DEFAULT_SEARCH_LIMIT: usize = 100;
const MAX_SEARCH_LIMIT: usize = 1000;
const SECONDS_PER_DAY: u64 = 86_400;

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

async fn wait_for_start(
    start_rx: &mut oneshot::Receiver<()>,
    cancel_rx: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        biased;
        _ = wait_for_cancel(cancel_rx) => false,
        result = start_rx => result.is_ok(),
    }
}

fn spawn_subscription_task(
    state: Arc<AppState>,
    sender: RelaySender,
    subscription_id: SubscriptionId,
    filters: Vec<Filter>,
    mut start_rx: oneshot::Receiver<()>,
    mut cancel_rx: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if !wait_for_start(&mut start_rx, &mut cancel_rx).await {
            return;
        }
        if *cancel_rx.borrow() {
            return;
        }
        let started_at = Instant::now();
        let filters_json = serde_json::to_string(&filters).unwrap_or_else(|_| "[]".to_string());
        let mut subscription = match state.db.clone().subscribe(&filters_json) {
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
        let mut snapshot_complete = false;

        loop {
            tokio::select! {
                item = subscription.next() => {
                    match item {
                        Some(searchnos_db::StreamItem::Event(event_json)) => {
                            if !snapshot_complete {
                                hits += 1;
                            }
                            if let Err(err) = send_event_json(&sender, &subscription_id, &event_json).await {
                                tracing::warn!(
                                    error = %err,
                                    subscription = %subscription_id,
                                    "failed to deliver subscription event"
                                );
                                return;
                            }
                        }
                        Some(searchnos_db::StreamItem::Eose) => {
                            snapshot_complete = true;
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
                        None if snapshot_complete => break,
                        None => {
                            let message = "error: failed to subscribe: subscription ended before EOSE";
                            if let Err(send_err) = send_closed(&sender, &subscription_id, message).await {
                                tracing::warn!(
                                    error = %send_err,
                                    subscription = %subscription_id,
                                    "failed to deliver CLOSED"
                                );
                            }
                            break;
                        }
                    }
                }
                _ = wait_for_cancel(&mut cancel_rx) => {
                    return;
                }
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

fn oldest_search_partition_start(now: u64, search_days: NonZeroU64) -> u64 {
    let current_day = now / SECONDS_PER_DAY;
    current_day
        .saturating_sub(search_days.get() - 1)
        .saturating_mul(SECONDS_PER_DAY)
}

fn normalize_search_filters(
    filters: Vec<Filter>,
    search_days: Option<NonZeroU64>,
    now: u64,
) -> Vec<Filter> {
    let minimum_since = search_days.map(|days| oldest_search_partition_start(now, days));

    filters
        .into_iter()
        .map(|mut filter| {
            filter.limit = Some(
                filter
                    .limit
                    .map_or(DEFAULT_SEARCH_LIMIT, |limit| limit)
                    .min(MAX_SEARCH_LIMIT),
            );
            if let Some(minimum_since) = minimum_since {
                let effective_since = filter
                    .since
                    .map_or(minimum_since, |since| since.as_secs().max(minimum_since));
                filter.since = Some(Timestamp::from_secs(effective_since));
            }
            filter
        })
        .collect()
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

        let filters =
            normalize_search_filters(filters, state.search_days, current_unix_timestamp());

        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (start_tx, start_rx) = oneshot::channel();
        let task = spawn_subscription_task(
            state.clone(),
            sender.clone(),
            subscription_id.clone(),
            filters,
            start_rx,
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
            return Ok(());
        }

        let _ = start_tx.send(());

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

    #[test]
    fn normalize_search_filters_limits_search_to_recent_utc_days() {
        let now = 1_000 * SECONDS_PER_DAY + 12_345;
        let search_days = NonZeroU64::new(365);
        let cutoff = (1_000 - 364) * SECONDS_PER_DAY;
        let filters = vec![
            Filter::new().search("unset"),
            Filter::new()
                .search("older")
                .since(Timestamp::from_secs(cutoff - 1)),
            Filter::new()
                .search("newer")
                .since(Timestamp::from_secs(cutoff + 1)),
        ];

        let normalized = normalize_search_filters(filters, search_days, now);

        assert_eq!(normalized[0].since, Some(Timestamp::from_secs(cutoff)));
        assert_eq!(normalized[1].since, Some(Timestamp::from_secs(cutoff)));
        assert_eq!(normalized[2].since, Some(Timestamp::from_secs(cutoff + 1)));
    }

    #[test]
    fn normalize_search_filters_does_not_add_since_without_search_days() {
        let filters = vec![Filter::new().search("nostr")];

        let normalized = normalize_search_filters(filters, None, 0);

        assert_eq!(normalized[0].since, None);
    }

    #[tokio::test]
    async fn wait_for_start_stops_waiting_when_cancelled() {
        let (_start_tx, mut start_rx) = oneshot::channel();
        let (cancel_tx, mut cancel_rx) = watch::channel(false);
        let task = tokio::spawn(async move { wait_for_start(&mut start_rx, &mut cancel_rx).await });

        cancel_tx.send(true).unwrap();

        assert!(!task.await.unwrap());
    }
}
