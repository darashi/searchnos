use env_logger;
use log::info;
use nostr_sdk::prelude::*;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    // env vars
    let src_relays = env::var("SRC_RELAYS")
        .expect("SRC_RELAYS is not set; set it to the comma-separated URLs of relays");
    let dest_relays = env::var("DEST_RELAYS")
        .expect("DEST_RELAYS is not set; set it to the comma-separated URLs of relays");

    // prepare nostr clients
    let my_keys: Keys = Keys::generate();
    let src_client = Client::new(&my_keys);

    for relay in src_relays.split(',') {
        info!("adding source relay: {}", relay);
        src_client.add_relay(relay, None).await?;
    }
    src_client.connect().await;

    let dest_client = Client::new(&my_keys);
    for relay in dest_relays.split(',') {
        info!("adding destination relay: {}", relay);
        dest_client.add_relay(relay, None).await?;
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

    src_client.subscribe(vec![subscription]).await;
    info!("ready to receive messages");

    loop {
        let mut notifications = src_client.notifications();
        while let Ok(notification) = notifications.recv().await {
            if let RelayPoolNotification::Event(_url, event) = notification {
                log::info!("received event: {}", event.as_json());
                // TODO check dates
                dest_client.send_event(event).await?;
            }
        }
    }
}
