use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    Elasticsearch,
};
use env_logger;
use log::info;
use nostr_sdk::prelude::*;
use searchnos::indexer::{
    handlers::handle_update,
    schema::{create_index_template, put_pipeline},
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    // env vars
    let es_url = env::var("ES_URL").expect("ES_URL is not set; set it to the URL of elasticsearch");
    let relays = env::var("NOSTR_RELAYS")
        .expect("NOSTR_RELAYS is not set; set it to the comma-separated URLs of relays");

    // prepare elasticsearch client
    let es_url = Url::parse(&es_url).expect("invalid elasticsearch url");
    let conn_pool = SingleNodeConnectionPool::new(es_url);
    let es_transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let es_client = Elasticsearch::new(es_transport);
    let pipeline_name = "nostr-pipeline";
    put_pipeline(&es_client, pipeline_name).await?;

    let index_template_name = "nostr";
    let alias_name = "nostr";

    create_index_template(&es_client, index_template_name, pipeline_name).await?;
    info!("elasticsearch index ready");

    // prepare nostr client
    let my_keys: Keys = Keys::generate();
    let nostr_client = Client::new(&my_keys);

    for relay in relays.split(',') {
        info!("adding relay: {}", relay);
        nostr_client.add_relay(relay, None).await?;
    }
    nostr_client.connect().await;
    info!("connected to relays");

    let subscription = Filter::new().limit(0).kinds(vec![
        Kind::Metadata,
        Kind::TextNote,
        Kind::EventDeletion,
        Kind::LongFormTextNote,
        Kind::ChannelCreation,
        Kind::ChannelMetadata,
        Kind::ChannelMessage,
        Kind::ChannelHideMessage,
        Kind::ChannelMuteUser,
    ]);

    nostr_client.subscribe(vec![subscription]).await;
    info!("ready to receive messages");

    // TODO periodically purge old indexes

    loop {
        let mut notifications = nostr_client.notifications();
        while let Ok(notification) = notifications.recv().await {
            if let RelayPoolNotification::Event(_url, event) = notification {
                handle_update(&es_client, &alias_name, &index_template_name, &event).await?;
            }
        }
    }
}
