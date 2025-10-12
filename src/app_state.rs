use std::time::Duration;

use nostr_sdk::{PublicKey, RelayUrl};
use searchnos_db::SearchnosDB;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Debug)]
pub struct AppState {
    pub db: Arc<SearchnosDB>,
    pub relay_info: String,
    pub max_subscriptions: usize,
    pub max_filters: usize,
    pub admin_pubkeys: HashSet<PublicKey>,
    pub public_relay_url: Option<RelayUrl>,
    pub ping_interval: Duration,
    pub respect_forwarded_headers: bool,
    pub active_connections: AtomicUsize,
}
