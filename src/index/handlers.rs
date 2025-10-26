use futures::sink::SinkExt;
use nostr_sdk::prelude::*;
use nostr_sdk::Event;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::app_state::AppState;
use crate::client_addr::ClientAddr;
use yawc::{frame::FrameView, WebSocket as YawcWebSocket};

pub async fn handle_update(state: Arc<AppState>, event: &Event) -> anyhow::Result<()> {
    let db = state.db.clone();
    let raw = event.as_json();
    tokio::task::spawn_blocking(move || db.insert_event_json_owned(raw)).await??;
    Ok(())
}

pub async fn send_ok(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    event: &Event,
    status: bool,
    message: &str,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::ok(event.id, status, message);
    sender
        .lock()
        .await
        .send(FrameView::text(relay_msg.as_json()))
        .await?;
    Ok(())
}

pub async fn handle_event(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    state: Arc<AppState>,
    addr: ClientAddr,
    event: &nostr_sdk::Event,
    is_admin_connection: bool,
) -> anyhow::Result<()> {
    let remote_addr = addr.socket_addr();
    let forwarded_header = addr.forwarded_raw().map(str::to_owned);
    let event_span = if let Some(ref header) = forwarded_header {
        tracing::info_span!(
            "event",
            id = %event.id,
            kind = event.kind.as_u16(),
            pubkey = %event.pubkey,
            admin = is_admin_connection,
            remote_ip = %remote_addr.ip(),
            remote_port = remote_addr.port(),
            forwarded = header.as_str()
        )
    } else {
        tracing::info_span!(
            "event",
            id = %event.id,
            kind = event.kind.as_u16(),
            pubkey = %event.pubkey,
            admin = is_admin_connection,
            remote_ip = %remote_addr.ip(),
            remote_port = remote_addr.port()
        )
    };
    async move {
        if let Err(e) = event.verify() {
            tracing::warn!(error = %e, "signature verification failed");
            return Err(e.into());
        }

        if !is_admin_connection {
            tracing::info!("blocked: authentication required");
            return send_ok(sender, event, false, "restricted: authentication required").await;
        }

        // NIP-70: Check for protected event
        if event.is_protected() {
            tracing::info!("blocked: protected event (NIP-70)");
            return send_ok(sender, event, false, "blocked: protected event").await;
        }

        match handle_update(state, event).await {
            Ok(_) => {
                tracing::info!("accepted");
                send_ok(sender, event, true, "").await
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to handle event");
                send_ok(sender, event, false, "error: failed to handle EVENT").await
            }
        }
    }
    .instrument(event_span)
    .await
}
