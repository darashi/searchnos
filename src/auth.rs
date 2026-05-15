use anyhow::Context;
use nostr_sdk::{nips::nip42, prelude::ToBech32, Event, Kind, PublicKey};
use rand::{distr::Alphanumeric, RngExt};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::app_state::AppState;
use crate::relay_sender::RelaySender;

pub fn generate_auth_challenge() -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .map(char::from)
        .take(32)
        .collect()
}

pub struct ConnectionAuthState {
    challenge: String,
    challenge_sent: bool,
    authenticated_pubkeys: BTreeSet<PublicKey>,
}

impl ConnectionAuthState {
    pub fn new(challenge: String) -> Self {
        Self {
            challenge,
            challenge_sent: false,
            authenticated_pubkeys: BTreeSet::new(),
        }
    }

    pub fn authenticated_pubkeys(&self) -> Vec<PublicKey> {
        self.authenticated_pubkeys.iter().cloned().collect()
    }

    pub fn authenticated_pubkey_count(&self) -> usize {
        self.authenticated_pubkeys.len()
    }

    pub fn challenge(&self) -> String {
        self.challenge.clone()
    }

    pub fn ensure_challenge(&mut self) -> String {
        self.challenge_sent = true;
        self.challenge.clone()
    }

    pub fn challenge_sent(&self) -> bool {
        self.challenge_sent
    }

    fn register_authenticated_pubkey(&mut self, pubkey: PublicKey) {
        self.authenticated_pubkeys.insert(pubkey);
        self.challenge = generate_auth_challenge();
        self.challenge_sent = false;
    }
}

pub async fn handle_auth_message(
    state: Arc<AppState>,
    sender: RelaySender,
    event: Event,
    auth_state: Arc<Mutex<ConnectionAuthState>>,
) -> anyhow::Result<()> {
    let challenge = {
        let auth_state = auth_state.lock().await;
        auth_state.challenge()
    };

    let npub = event
        .pubkey
        .to_bech32()
        .unwrap_or_else(|_| event.pubkey.to_string());

    if let Err(e) = event.verify() {
        tracing::warn!(
            "authentication failed due to invalid signature for {}: {}",
            event.id,
            e
        );
        sender.notice("auth: invalid signature").await?;
        return Ok(());
    }

    let valid = if let Some(relay_url) = &state.public_relay_url {
        nip42::is_valid_auth_event(&event, relay_url, &challenge)
    } else {
        event.kind == Kind::Authentication
            && event
                .tags
                .challenge()
                .map(|c| c == challenge)
                .unwrap_or(false)
    };

    if !valid {
        tracing::warn!(
            "authentication failed due to invalid challenge or relay (pubkey {})",
            npub
        );
        sender.notice("auth: invalid challenge").await?;
        return Ok(());
    }

    let authed_count = {
        let mut auth_state = auth_state.lock().await;
        auth_state.register_authenticated_pubkey(event.pubkey);
        auth_state.authenticated_pubkey_count()
    };

    tracing::info!(
        auth_pubkey = %npub,
        total_authenticated_pubkeys = authed_count,
        "nip42 authentication verified"
    );
    sender
        .ok(&event, true, "")
        .await
        .context("failed to send OK for AUTH")?;

    Ok(())
}
