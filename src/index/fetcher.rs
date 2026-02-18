use std::sync::Arc;

use anyhow::anyhow;
use nostr_sdk::{Client, Filter, Keys, Kind, RelayPoolNotification, RelayUrl};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

use crate::app_state::AppState;
use crate::index::handlers::handle_update;

const LOG_TARGET: &str = "fetcher";

pub fn spawn_fetcher(
    state: Arc<AppState>,
    relays: Vec<RelayUrl>,
    kinds: Vec<Kind>,
) -> Option<JoinHandle<()>> {
    if relays.is_empty() {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(err) = run_fetcher(state, relays, kinds).await {
            tracing::error!(target: LOG_TARGET, error = %err, "terminated unexpectedly");
        }
    }))
}

async fn run_fetcher(
    state: Arc<AppState>,
    relays: Vec<RelayUrl>,
    kinds: Vec<Kind>,
) -> anyhow::Result<()> {
    if kinds.is_empty() {
        return Err(anyhow!("fetcher requires at least one kind"));
    }

    let keys = Keys::generate();
    let client = Client::new(keys);

    let relay_list: Vec<String> = relays.iter().map(|url| url.to_string()).collect();
    let kind_list: Vec<u16> = kinds.iter().map(|kind| u16::from(*kind)).collect();
    tracing::info!(target: LOG_TARGET, relays = ?relay_list, kinds = ?kind_list, "starting");

    for relay in &relays {
        let url = relay.to_string();
        if let Err(err) = client.add_relay(url.clone()).await {
            tracing::warn!(
                target: LOG_TARGET,
                relay = %url,
                error = %err,
                "failed to add source relay"
            );
        } else {
            tracing::info!(target: LOG_TARGET, relay = %url, "relay added");
        }
    }

    client.connect().await;
    tracing::info!(target: LOG_TARGET, "connected to relays");

    let subscription = Filter::new().limit(0).kinds(kinds.clone());
    if let Err(err) = client.subscribe(subscription, None).await {
        tracing::error!(target: LOG_TARGET, error = %err, "failed to subscribe to source relays");
        return Err(err.into());
    }
    tracing::info!(target: LOG_TARGET, "subscribed to source relays");

    let mut notifications = client.notifications();
    loop {
        let notification = match notifications.recv().await {
            Ok(notification) => notification,
            Err(RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    skipped,
                    "notification consumer lagged; continuing"
                );
                continue;
            }
            Err(RecvError::Closed) => {
                tracing::error!(
                    target: LOG_TARGET,
                    "notification channel closed; stopping fetcher"
                );
                return Err(anyhow!("notification channel closed"));
            }
        };

        match notification {
            RelayPoolNotification::Event {
                relay_url, event, ..
            } => {
                if let Err(err) = event.verify() {
                    tracing::warn!(target: LOG_TARGET, id = %event.id, error = %err, "event failed verification");
                    continue;
                }

                tracing::info!(
                    target: LOG_TARGET,
                    relay = %relay_url,
                    id = %event.id,
                    kind = event.kind.as_u16(),
                    "received event"
                );

                if let Err(err) = handle_update(state.clone(), &event).await {
                    tracing::error!(target: LOG_TARGET, id = %event.id, error = %err, "failed to ingest event");
                }
            }
            RelayPoolNotification::Message { .. } => {}
            RelayPoolNotification::Shutdown => {
                tracing::info!(target: LOG_TARGET, "received shutdown signal");
                return Ok(());
            }
        }
    }
}
