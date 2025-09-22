use std::time::Duration;

use elasticsearch::Elasticsearch;
use nostr_sdk::{PublicKey, RelayUrl};
use std::collections::HashSet;

#[derive(Debug)]
pub struct AppState {
    pub es_client: Elasticsearch,
    pub index_name_prefix: String,
    pub index_alias_name: String,
    pub relay_info: String,
    pub max_subscriptions: usize,
    pub max_filters: usize,
    pub admin_pubkeys: HashSet<PublicKey>,
    pub public_relay_url: Option<RelayUrl>,
    pub ping_interval: Duration,
    pub daily_index_ttl_days: Option<u64>,
    pub index_allow_future_days: u64,
    pub yearly_index_ttl_years: Option<u64>,
    pub yearly_index_kinds: HashSet<u16>,
    pub tx: tokio::sync::broadcast::Sender<nostr_sdk::Event>,
}
