use std::num::NonZeroU64;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;

use searchnos_db::SearchnosDB;
use tokio::sync::Semaphore;

pub struct AppState {
    pub db: Arc<SearchnosDB>,
    pub relay_info: String,
    pub max_subscriptions: usize,
    pub max_filters: usize,
    pub search_days: Option<NonZeroU64>,
    pub search_permits: Arc<Semaphore>,
    pub ping_interval: Duration,
    pub respect_forwarded_headers: bool,
    pub active_connections: AtomicUsize,
    pub health_max_event_age: Duration,
}
