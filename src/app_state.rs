use elasticsearch::Elasticsearch;

#[derive(Debug)]
pub struct AppState {
    pub es_client: Elasticsearch,
    pub index_name: String,
    pub relay_info: String,
    pub max_subscriptions: usize,
    pub max_filters: usize,
    pub api_key: String,
}
