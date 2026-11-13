use std::sync::Arc;

use anyhow::anyhow;
use futures::StreamExt;
use nostr_sdk::prelude::{
    Client, ClientNotification, Filter, Keys, Kind, RelayUrl, SignerAuthenticator,
};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::app_state::AppState;
use crate::index::handlers::handle_update;

const LOG_TARGET: &str = "fetcher";
const RESTART_DELAY: Duration = Duration::from_secs(5);

pub fn spawn_fetcher(
    state: Arc<AppState>,
    relays: Vec<RelayUrl>,
    kinds: Vec<Kind>,
) -> Option<JoinHandle<()>> {
    if relays.is_empty() {
        return None;
    }

    Some(tokio::spawn(async move {
        loop {
            let result = run_fetcher(state.clone(), relays.clone(), kinds.clone()).await;

            match result {
                Ok(()) => {
                    tracing::warn!(
                        target: LOG_TARGET,
                        restart_delay_seconds = RESTART_DELAY.as_secs(),
                        "stopped; restarting"
                    );
                }
                Err(err) => {
                    tracing::error!(
                        target: LOG_TARGET,
                        error = %err,
                        restart_delay_seconds = RESTART_DELAY.as_secs(),
                        "terminated unexpectedly; restarting"
                    );
                }
            }

            sleep(RESTART_DELAY).await;
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

    let authenticator = SignerAuthenticator::new(Keys::generate());
    let client = Client::builder().authenticator(authenticator).build();

    let relay_list: Vec<String> = relays.iter().map(|url| url.to_string()).collect();
    let kind_list: Vec<u16> = kinds.iter().map(|kind| u16::from(*kind)).collect();
    tracing::info!(target: LOG_TARGET, relays = ?relay_list, kinds = ?kind_list, "starting");

    for relay in &relays {
        let url = relay.to_string();
        match client.add_relay(url.clone()).await {
            Err(err) => {
                tracing::warn!(
                    target: LOG_TARGET,
                    relay = %url,
                    error = %err,
                    "failed to add source relay"
                );
            }
            _ => {
                tracing::info!(target: LOG_TARGET, relay = %url, "relay added");
            }
        }
    }

    client.connect().await;
    tracing::info!(target: LOG_TARGET, "connected to relays");

    let subscription = Filter::new().limit(0).kinds(kinds.clone());
    if let Err(err) = client.subscribe(subscription).await {
        tracing::error!(target: LOG_TARGET, error = %err, "failed to subscribe to source relays");
        return Err(err.into());
    }
    tracing::info!(target: LOG_TARGET, "subscribed to source relays");

    let mut notifications = client.notifications();
    while let Some(notification) = notifications.next().await {
        match notification {
            ClientNotification::Event {
                relay_url, event, ..
            } => {
                if let Err(err) = event.verify() {
                    tracing::warn!(target: LOG_TARGET, id = %event.id, error = %err, "event failed verification");
                    continue;
                }

                tracing::debug!(
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
            ClientNotification::Message { .. } => {}
            ClientNotification::Shutdown => {
                tracing::info!(target: LOG_TARGET, "received shutdown signal");
                return Ok(());
            }
        }
    }

    tracing::error!(
        target: LOG_TARGET,
        "notification stream ended; stopping fetcher"
    );
    Err(anyhow!("notification stream ended"))
}
