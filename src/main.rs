use anyhow::{anyhow, Context};
use axum::extract::connect_info::ConnectInfo;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::Extension;
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use elasticsearch::{
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    Elasticsearch,
};
use futures::{sink::SinkExt, stream::StreamExt};
use nostr_sdk::{
    nips::nip42,
    prelude::{RelayInformationDocument, RelayMessage, ToBech32},
    JsonUtil, Kind, PublicKey, RelayUrl,
};
use rand::{distr::Alphanumeric, Rng};
use searchnos::app_state::AppState;
use searchnos::index::handlers::handle_event;
use searchnos::index::purge::spawn_index_purger;
use searchnos::index::schema::{create_index_template, put_pipeline};
use searchnos::search::handlers::{handle_close, handle_req, ClosedError};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use unicode_normalization::UnicodeNormalization;

fn generate_auth_challenge() -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .map(char::from)
        .take(32)
        .collect()
}

struct ConnectionAuthState {
    challenge: String,
    is_admin: bool,
    authenticated_pubkey: Option<PublicKey>,
}

impl ConnectionAuthState {
    fn new(challenge: String) -> Self {
        Self {
            challenge,
            is_admin: false,
            authenticated_pubkey: None,
        }
    }
}

async fn process_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscriptions: Arc<Mutex<HashMap<nostr_sdk::SubscriptionId, Vec<nostr_sdk::Filter>>>>,
    addr: SocketAddr,
    msg: Message,
    auth_state: Arc<Mutex<ConnectionAuthState>>,
) -> anyhow::Result<()> {
    match &msg {
        Message::Text(text) => {
            log::info!("{} RECEIVED {}", addr, text);
            let client_message = nostr_sdk::ClientMessage::from_json(text.as_bytes())?;
            match client_message {
                nostr_sdk::ClientMessage::Req {
                    subscription_id,
                    filter,
                } => {
                    handle_req(
                        state.clone(),
                        sender.clone(),
                        subscriptions.clone(),
                        addr,
                        &subscription_id,
                        vec![filter.into_owned()],
                    )
                    .await
                }
                nostr_sdk::ClientMessage::Close(subscription_id) => {
                    handle_close(subscriptions, addr, &subscription_id).await
                }
                nostr_sdk::ClientMessage::Event(event) => {
                    let is_admin_connection = {
                        let auth_state = auth_state.lock().await;
                        auth_state.is_admin
                    };

                    handle_event(
                        sender.clone(),
                        state.clone(),
                        addr,
                        &event,
                        is_admin_connection,
                    )
                    .await?;

                    if !is_admin_connection {
                        let challenge = {
                            let auth_state = auth_state.lock().await;
                            auth_state.challenge.clone()
                        };
                        if let Err(e) = send_auth_challenge(sender.clone(), &challenge).await {
                            log::warn!(
                                "{} failed to resend auth challenge after blocked EVENT: {}",
                                addr,
                                e
                            );
                        }
                    }

                    Ok(())
                }
                nostr_sdk::ClientMessage::Auth(event) => {
                    handle_auth_message(
                        state.clone(),
                        sender.clone(),
                        addr,
                        event.into_owned(),
                        auth_state.clone(),
                    )
                    .await?;
                    Ok(())
                }
                _ => Err(anyhow::anyhow!("invalid message type")),
            }?
        }
        Message::Close(_) => {
            log::info!("{} close message received", addr);
            return Ok(());
        }
        Message::Pong(_) => {}
        Message::Ping(_) => {}
        _ => {
            return Err(anyhow::anyhow!("non-text message {:?}", msg));
        }
    }
    Ok(())
}

async fn send_notice(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    msg: &str,
) -> anyhow::Result<()> {
    let notice = RelayMessage::notice(msg);
    sender
        .lock()
        .await
        .send(Message::Text(notice.as_json().into()))
        .await?;
    Ok(())
}

async fn send_auth_challenge(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    challenge: &str,
) -> anyhow::Result<()> {
    let auth = RelayMessage::auth(challenge.to_string());
    sender
        .lock()
        .await
        .send(Message::Text(auth.as_json().into()))
        .await?;
    Ok(())
}

async fn send_closed(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: nostr_sdk::SubscriptionId,
    msg: &str,
) -> anyhow::Result<()> {
    let closed = RelayMessage::closed(subscription_id, msg);
    sender
        .lock()
        .await
        .send(Message::Text(closed.as_json().into()))
        .await?;
    Ok(())
}

async fn handle_auth_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    addr: SocketAddr,
    event: nostr_sdk::Event,
    auth_state: Arc<Mutex<ConnectionAuthState>>,
) -> anyhow::Result<()> {
    let challenge = {
        let auth_state = auth_state.lock().await;
        auth_state.challenge.clone()
    };

    let npub = event
        .pubkey
        .to_bech32()
        .unwrap_or_else(|_| event.pubkey.to_string());

    if let Err(e) = event.verify() {
        log::warn!(
            "{} authentication failed due to invalid signature for {}: {}",
            addr,
            event.id,
            e
        );
        send_notice(sender.clone(), "auth: invalid signature").await?;
        send_auth_challenge(sender, &challenge).await?;
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
        log::warn!(
            "{} authentication failed due to invalid challenge or relay (pubkey {})",
            addr,
            npub
        );
        send_notice(sender.clone(), "auth: invalid challenge").await?;
        send_auth_challenge(sender, &challenge).await?;
        return Ok(());
    }

    if !state.admin_pubkeys.contains(&event.pubkey) {
        log::warn!(
            "{} authentication attempt with unauthorized pubkey {}",
            addr,
            npub
        );
        send_notice(sender.clone(), "auth: unauthorized pubkey").await?;
        send_auth_challenge(sender, &challenge).await?;
        return Ok(());
    }

    {
        let mut auth_state = auth_state.lock().await;
        auth_state.is_admin = true;
        auth_state.authenticated_pubkey = Some(event.pubkey);
    }

    log::info!("{} authenticated successfully as {}", addr, npub);
    send_notice(sender, "auth: success").await?;

    Ok(())
}

async fn spawn_pinger(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    addr: SocketAddr,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(state.ping_interval).await;
            log::info!("{} sending ping", addr);
            let res = sender.lock().await.send(Message::Ping(vec![].into())).await;
            if let Err(e) = res {
                log::warn!("{} error sending ping: {}", addr, e);
                return;
            }
        }
    })
}
fn match_search(normalized_content: &str, filter: &nostr_sdk::Filter) -> bool {
    match filter.search {
        Some(ref query) => {
            // TODO precompute this on REQ and stop doing this for every event
            let query = query.nfkc().collect::<String>().to_lowercase();
            let mut terms = query.split_whitespace();
            terms.all(|term| normalized_content.contains(term))
        }
        None => true,
    }
}

fn match_event(event: &nostr_sdk::Event, filters: &[nostr_sdk::Filter]) -> bool {
    let content = searchnos::index::text::extract_text(event)
        .nfkc()
        .collect::<String>()
        .to_lowercase(); // TODO precompute this on EVENT and stop doing this for every filter

    filters.iter().any(|filter| {
        filter.match_event(event, Default::default()) && match_search(&content, filter)
    })
}

async fn websocket(socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    log::info!("{} new websocket connection", addr);
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));
    let subscriptions = Arc::new(Mutex::new(HashMap::<
        nostr_sdk::SubscriptionId,
        Vec<nostr_sdk::Filter>,
    >::new()));
    let auth_state = Arc::new(Mutex::new(ConnectionAuthState::new(
        generate_auth_challenge(),
    )));

    // spawn pinger
    let pinger_handle = spawn_pinger(state.clone(), sender.clone(), addr).await;

    // span searcher
    let searcher_handle = tokio::spawn({
        let mut rx = state.tx.subscribe();
        let subscriptions = subscriptions.clone();
        let sender = sender.clone();
        async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        for (subscription_id, filters) in subscriptions.lock().await.iter() {
                            if match_event(&event, filters) {
                                let relay_msg =
                                    RelayMessage::event(subscription_id.clone(), event.clone());
                                sender
                                    .lock()
                                    .await
                                    .send(Message::Text(relay_msg.as_json().into()))
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("{} error receiving event: {}", addr, e);
                    }
                }
            }
        }
    });

    loop {
        match receiver.next().await {
            Some(Ok(msg)) => {
                let sender = sender.clone();
                let res = process_message(
                    state.clone(),
                    sender.clone(),
                    subscriptions.clone(),
                    addr,
                    msg,
                    auth_state.clone(),
                )
                .await;
                if let Err(e) = res {
                    log::warn!("{} error processing message: {}", addr, e);
                    if let Some(e) = e.downcast_ref::<ClosedError>() {
                        let res =
                            send_closed(sender, e.subscription_id.clone(), &e.to_string()).await;
                        if let Err(e) = res {
                            log::error!("{} error sending closed: {}", addr, e);
                            break;
                        }
                    } else {
                        let res = send_notice(sender, &format!("Error: {}", e)).await;
                        if let Err(e) = res {
                            log::error!("{} error sending notice: {}", addr, e);
                            break;
                        }
                    }
                }
            }
            Some(Err(e)) => {
                log::warn!("{} error receiving message: {}", addr, e);
            }
            None => {
                log::info!("{} websocket connection closed", addr);
                break;
            }
        }
    }

    // abort searcher and pinger
    searcher_handle.abort();
    pinger_handle.abort();
    let (was_admin, authed_pubkey) = {
        let auth_state = auth_state.lock().await;
        (auth_state.is_admin, auth_state.authenticated_pubkey)
    };
    match authed_pubkey {
        Some(pubkey) => {
            log::info!("{} disconnected (admin: true, pubkey: {})", addr, pubkey);
        }
        None => {
            log::info!("{} disconnected (admin: {})", addr, was_admin);
        }
    }
}

async fn ping() -> impl IntoResponse {
    println!("PING");

    StatusCode::OK
}

struct ReturnRelayInfoExtractor {}

impl<S> FromRequestParts<S> for ReturnRelayInfoExtractor
where
    S: Send + Sync,
{
    type Rejection = axum::response::Response;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(_upgrade) = parts.headers.get("upgrade") {
            Ok(ReturnRelayInfoExtractor {})
        } else {
            if let Some(accept) = parts.headers.get("accept") {
                if accept == "application/nostr+json" {
                    use axum::RequestPartsExt;
                    let Extension(state) = parts
                        .extract::<Extension<Arc<AppState>>>()
                        .await
                        .map_err(|err| err.into_response())?;
                    let relay_info = state.relay_info.clone();

                    let res = axum::response::Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .header("Access-Control-Allow-Origin", "*")
                        .body(relay_info)
                        .unwrap()
                        .into_response();

                    return Err(res);
                }
            }
            let res = axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain")
                .body("Please use a Nostr client to connect.".to_string())
                .unwrap()
                .into_response();

            Err(res)
        }
    }
}

async fn websocket_handler(
    _: ReturnRelayInfoExtractor,
    ws: WebSocketUpgrade,
    Extension(state): Extension<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket(socket, state, addr))
}

use clap::Parser;
#[derive(Parser, Debug)]
#[command(about,long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(long, env)]
    port: u16,

    /// URL of elasticsearch
    #[arg(long, env)]
    es_url: String,

    /// Comma-separated list of admin public keys allowed to publish events
    #[arg(long = "admin-pubkeys", env = "ADMIN_PUBKEYS", value_delimiter = ',')]
    admin_pubkeys: Vec<PublicKey>,

    /// Public relay URL used to validate AUTH events (optional)
    #[arg(long = "public-relay-url", env = "PUBLIC_RELAY_URL")]
    public_relay_url: Option<String>,

    /// Maximum number of subscriptions per client
    #[arg(long, env, default_value_t = 8)]
    max_subscriptions: usize,

    /// Maximum number of filters per subscription
    #[arg(long, env, default_value_t = 8)]
    max_filters: usize,

    /// Ping interval in seconds
    #[arg(long, env, default_value_t = 55)]
    ping_interval: u64,

    /// Daily index TTL in days
    #[arg(long = "daily-index-ttl", env = "DAILY_INDEX_TTL")]
    daily_index_ttl: Option<u64>,

    /// Maximum number of days in the future to accept events for
    #[arg(long, env, default_value_t = 1)]
    index_allow_future_days: u64,

    /// Comma-separated kinds to index yearly
    #[arg(long = "yearly-index-kinds", env = "YEARLY_INDEX_KINDS")]
    yearly_index_kinds: Option<String>,

    /// TTL for yearly indices (years)
    #[arg(long = "yearly-index-ttl", env = "YEARLY_INDEX_TTL")]
    yearly_index_ttl: Option<u64>,
}

async fn app(args: &Args) -> Result<Router, Box<dyn std::error::Error>> {
    let version = format!(
        "v{}-{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH").chars().take(7).collect::<String>()
    );
    let pkg_name = env!("CARGO_PKG_NAME").to_string();

    log::info!("{} {}", pkg_name, version);

    log::info!("connecting to elasticsearch");

    // prepare elasticsearch client
    let es_url = Url::parse(&args.es_url).expect("invalid elasticsearch url");
    let conn_pool = SingleNodeConnectionPool::new(es_url);
    let es_transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let es_client = Elasticsearch::new(es_transport);
    let index_name_prefix = "nostr";
    let index_alias_name = "nostr";
    let pipeline_name = "nostr-pipeline";
    let index_template_name = "nostr";
    put_pipeline(&es_client, pipeline_name).await?;
    create_index_template(
        &es_client,
        index_template_name,
        pipeline_name,
        index_name_prefix,
        index_template_name,
    )
    .await?;
    log::info!("elasticsearch index ready");

    let mut relay_info = RelayInformationDocument::new();
    relay_info.name = Some("searchnos".to_string()); // TODO make this configurable
    relay_info.description = Some("searchnos relay".to_string()); // TODO make this configurable
    relay_info.supported_nips = Some(vec![1, 9, 11, 22, 28, 42, 50, 70]);
    relay_info.software = Some(pkg_name);
    relay_info.version = Some(version);
    let relay_info = serde_json::to_string(&relay_info).unwrap();

    let (tx, mut rx) = tokio::sync::broadcast::channel(32);
    tokio::spawn(async move {
        loop {
            let _event = rx.recv().await;
            // just ignore the event
        }
    });

    // Use YEARLY_INDEX_KINDS/--yearly-index-kinds only (legacy removed)
    let yearly_index_kinds_raw = args.yearly_index_kinds.clone().unwrap_or_default();

    let yearly_index_kinds = yearly_index_kinds_raw
        .as_str()
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                match s.parse::<u16>() {
                    Ok(v) => Some(v),
                    Err(e) => {
                        log::warn!("Invalid YEARLY_INDEX_KINDS entry '{}': {}", s, e);
                        None
                    }
                }
            }
        })
        .collect::<std::collections::HashSet<u16>>();

    if !yearly_index_kinds.is_empty() {
        log::info!("Yearly index kinds enabled: {:?}", yearly_index_kinds);
    }

    let admin_pubkeys: HashSet<PublicKey> = args.admin_pubkeys.iter().copied().collect();

    if admin_pubkeys.is_empty() {
        return Err(anyhow!("no admin pubkeys configured").into());
    }

    log::info!("Configured {} admin pubkey(s)", admin_pubkeys.len());

    let public_relay_url = match &args.public_relay_url {
        Some(url) => {
            let url = url.trim();
            if url.is_empty() {
                None
            } else {
                Some(RelayUrl::parse(url).with_context(|| format!("invalid relay url '{}'", url))?)
            }
        }
        None => None,
    };

    let app_state = Arc::new(AppState {
        relay_info,
        es_client,
        index_name_prefix: index_name_prefix.to_string(),
        index_alias_name: index_alias_name.to_string(),
        max_subscriptions: args.max_subscriptions, // TODO include this in relay info
        max_filters: args.max_filters,             // TODO include this in relay info
        admin_pubkeys,
        public_relay_url,
        ping_interval: Duration::from_secs(args.ping_interval),
        daily_index_ttl_days: args.daily_index_ttl,
        index_allow_future_days: args.index_allow_future_days,
        yearly_index_ttl_years: args.yearly_index_ttl,
        yearly_index_kinds,
        tx,
    });

    if args.daily_index_ttl.is_some() {
        spawn_index_purger(app_state.clone()).await;
    } else {
        log::info!("index ttl is disabled");
    }

    let app = Router::new()
        .route("/ping", get(ping))
        .route("/", get(websocket_handler))
        .layer(Extension(app_state));

    Ok(app)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();
    let args = Args::parse();
    let app = app(&args).await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    log::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    fn find_available_port() -> Option<u16> {
        (32768..65535).find(|port| TcpListener::bind(format!("127.0.0.1:{}", port)).is_ok())
    }

    #[tokio::test]
    async fn somke_test() {
        env_logger::init();

        let port = find_available_port().expect("no available port found");
        let admin_pubkey = nostr_sdk::Keys::generate().public_key();
        let args = Args {
            port,
            es_url: "http://elastic:super-secret-nostaro@localhost:9200".to_string(),
            admin_pubkeys: vec![admin_pubkey],
            public_relay_url: None,
            max_subscriptions: 100,
            max_filters: 32,
            ping_interval: 55,
            daily_index_ttl: None,
            index_allow_future_days: 1,
            yearly_index_kinds: None,
            yearly_index_ttl: None,
        };
        let app = app(&args).await.unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], args.port));

        let join_handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap();
        });

        let keys = nostr_sdk::Keys::generate();
        let client = nostr_sdk::Client::new(keys);
        client
            .add_relay(format!("ws://localhost:{}", args.port))
            .await
            .unwrap();
        client.connect().await;

        let filter = nostr_sdk::Filter::new().search("nostr").limit(0);
        let relay_url = format!("ws://localhost:{}", args.port);
        let res = client
            .fetch_events_from([&relay_url], filter, Duration::from_secs(5))
            .await;
        assert!(res.is_ok());

        join_handle.abort();
    }
}
