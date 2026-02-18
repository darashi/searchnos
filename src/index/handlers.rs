use chrono::Utc;
use futures::sink::SinkExt;
use nostr_sdk::{prelude::*, Event};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::app_state::AppState;
use crate::client_addr::ClientAddr;
use crate::plugin::{PluginDecision, PluginSourceType};
use yawc::{frame::Frame, HttpWebSocket as YawcWebSocket};

pub async fn handle_update(state: Arc<AppState>, event: &Event) -> anyhow::Result<()> {
    let db = state.db.clone();
    let raw = event.as_json();
    tokio::task::spawn_blocking(move || db.insert_event_json_owned(raw)).await??;
    Ok(())
}

pub async fn send_ok(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
    event: &Event,
    status: bool,
    message: &str,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::ok(event.id, status, message);
    sender
        .lock()
        .await
        .send(Frame::text(relay_msg.as_json()))
        .await?;
    Ok(())
}

pub async fn handle_event(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, Frame>>>,
    state: Arc<AppState>,
    addr: ClientAddr,
    event: &nostr_sdk::Event,
    authenticated_pubkeys: Vec<String>,
) -> anyhow::Result<bool> {
    let authenticated_pubkeys_log = if authenticated_pubkeys.is_empty() {
        None
    } else {
        Some(authenticated_pubkeys.join(","))
    };
    let remote_addr = addr.socket_addr();
    let forwarded_header = addr.forwarded_raw().map(str::to_owned);
    let event_span = if let Some(ref header) = forwarded_header {
        tracing::info_span!(
            "event",
            id = %event.id,
            kind = event.kind.as_u16(),
            pubkey = %event.pubkey,
            authenticated_pubkeys = authenticated_pubkeys_log.as_deref().unwrap_or(""),
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
            authenticated_pubkeys = authenticated_pubkeys_log.as_deref().unwrap_or(""),
            remote_ip = %remote_addr.ip(),
            remote_port = remote_addr.port()
        )
    };
    async move {
        if state.block_event_message {
            tracing::info!("blocked: EVENT messages disabled");
            send_ok(sender, event, false, "blocked: writes disabled").await?;
            return Ok(false);
        }
        if let Err(e) = event.verify() {
            tracing::warn!(error = %e, "signature verification failed");
            return Err(e.into());
        }

        // NIP-70: Check for protected event
        if event.is_protected() {
            tracing::info!("blocked: protected event (NIP-70)");
            send_ok(sender, event, false, "blocked: protected event").await?;
            return Ok(false);
        }

        if let Some(plugin) = state.write_policy_plugin.as_ref() {
            let source_type = if remote_addr.ip().is_ipv4() {
                PluginSourceType::Ip4
            } else {
                PluginSourceType::Ip6
            };
            let source_info = addr
                .forwarded_raw()
                .map(|value| value.to_string())
                .unwrap_or_else(|| remote_addr.ip().to_string());
            let decision = plugin
                .evaluate_event(
                    event,
                    Utc::now().timestamp(),
                    source_type,
                    &source_info,
                    authenticated_pubkeys.as_slice(),
                )
                .await;
            match decision {
                Ok(PluginDecision::Accept) => {}
                Ok(PluginDecision::Reject { msg }) => {
                    let notice = msg.unwrap_or_else(|| "blocked: rejected by policy".to_string());
                    tracing::info!(message = notice.as_str(), "blocked by plugin");
                    send_ok(sender, event, false, &notice).await?;
                    return Ok(false);
                }
                Ok(PluginDecision::ShadowReject) => {
                    tracing::info!("blocked by plugin (shadow reject)");
                    send_ok(sender, event, true, "").await?;
                    return Ok(false);
                }
                Ok(PluginDecision::RejectAndChallenge { msg }) => {
                    let notice =
                        msg.unwrap_or_else(|| "blocked: authentication required".to_string());
                    tracing::info!(
                        message = notice.as_str(),
                        "plugin rejected and requested auth"
                    );
                    send_ok(sender, event, false, &notice).await?;
                    return Ok(true);
                }
                Err(err) => {
                    tracing::error!(error = %err, "write policy plugin failed");
                    send_ok(sender, event, false, "error: internal error").await?;
                    return Ok(false);
                }
            }
        }

        match handle_update(state, event).await {
            Ok(_) => {
                tracing::info!("accepted");
                send_ok(sender, event, true, "").await?;
                Ok(false)
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to handle event");
                send_ok(sender, event, false, "error: failed to handle EVENT").await?;
                Ok(false)
            }
        }
    }
    .instrument(event_span)
    .await
}
