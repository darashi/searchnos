use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use nostr_sdk::prelude::Event;
use searchnos_db::SearchnosDB;
use tokio::sync::broadcast;

pub struct AppState {
    pub db: Arc<SearchnosDB>,
    pub ephemeral_events: broadcast::Sender<Arc<Event>>,
    pub allow_client_events: bool,
    pub relay_info: String,
    pub max_subscriptions: usize,
    pub max_filters: usize,
    pub search_days: Option<NonZeroU64>,
    pub ping_interval: Duration,
    pub respect_forwarded_headers: bool,
    pub active_connections: AtomicUsize,
    pub health_max_event_age: Duration,
}
