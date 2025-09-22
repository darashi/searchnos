use nostr_sdk::prelude::*;
use std::{env, str::FromStr};
use tracing::info;

fn init_tracing() {
    let _ = tracing_log::LogTracer::init();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .try_init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    init_tracing();

    // env vars
    let src_relays = env::var("SRC_RELAYS")
        .expect("SRC_RELAYS is not set; set it to the comma-separated URLs of relays");
    let dest_relays = env::var("DEST_RELAYS")
        .expect("DEST_RELAYS is not set; set it to the comma-separated URLs of relays");
    let indexer_secret_key = env::var("INDEXER_SECRET_KEY")
        .expect("INDEXER_SECRET_KEY is not set; set it to the hex/bech32 encoded secret key used for destination authentication");

    let dest_keys = Keys::from_str(&indexer_secret_key)
        .expect("INDEXER_SECRET_KEY is invalid; expected hex or nsec format");
    let dest_pubkey = dest_keys.public_key();

    // prepare nostr clients
    let src_keys: Keys = Keys::generate();
    let src_client = Client::new(src_keys);

    for relay in src_relays.split(',') {
        info!("adding source relay: {}", relay);
        src_client.add_relay(relay).await?;
    }
    src_client.connect().await;

    info!(
        "adding destination relays with admin pubkey {}",
        dest_pubkey
    );
    let dest_client = Client::new(dest_keys);
    for relay in dest_relays.split(',') {
        info!("adding destination relay: {}", relay);
        dest_client.add_relay(relay).await?;
    }
    dest_client.connect().await;
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

    src_client.subscribe(subscription, None).await?;
    info!("ready to receive messages");

    loop {
        let mut notifications = src_client.notifications();
        while let Ok(notification) = notifications.recv().await {
            if let RelayPoolNotification::Event { event, .. } = notification {
                info!("received event: {}", event.as_json());
                // TODO check dates
                dest_client.send_event(&event).await?;
            }
        }
    }
}
