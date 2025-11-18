use std::time::Duration;

use nostr_sdk::RelayUrl;
use searchnos_db::SearchnosDB;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::plugin::WritePolicyPlugin;

pub struct AppState {
    pub db: Arc<SearchnosDB>,
    pub relay_info: String,
    pub max_subscriptions: usize,
    pub max_filters: usize,
    pub public_relay_url: Option<RelayUrl>,
    pub ping_interval: Duration,
    pub respect_forwarded_headers: bool,
    pub active_connections: AtomicUsize,
    pub write_policy_plugin: Option<Arc<WritePolicyPlugin>>,
    pub block_event_message: bool,
}
