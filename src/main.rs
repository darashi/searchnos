use axum::extract::connect_info::ConnectInfo;
use axum::extract::{FromRequestParts, Query};
use axum::http::request::Parts;
use axum::{async_trait, Extension};
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
use env_logger;
use futures::{sink::SinkExt, stream::StreamExt};
use nostr_sdk::prelude::{RelayInformationDocument, RelayMessage};
use nostr_sdk::JsonUtil;
use searchnos::app_state::AppState;
use searchnos::index::handlers::handle_event;
use searchnos::index::purge::spawn_index_purger;
use searchnos::index::schema::{create_index_template, put_pipeline};
use searchnos::search::handlers::{handle_close, handle_req};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use unicode_normalization::UnicodeNormalization;

#[derive(Deserialize, Debug)]
struct Parameter {
    api_key: Option<String>,
}

async fn process_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscriptions: Arc<Mutex<HashMap<nostr_sdk::SubscriptionId, Vec<nostr_sdk::Filter>>>>,
    addr: SocketAddr,
    msg: Message,
    is_admin_connection: bool,
) -> anyhow::Result<()> {
    match &msg {
        Message::Text(text) => {
            log::info!("{} RECEIVED {}", addr, text);
            let client_message = nostr_sdk::ClientMessage::from_json(text)?;
            match client_message {
                nostr_sdk::ClientMessage::Req {
                    subscription_id,
                    filters,
                } => {
                    handle_req(
                        state,
                        sender,
                        subscriptions,
                        addr,
                        &subscription_id,
                        filters,
                    )
                    .await
                }
                nostr_sdk::ClientMessage::Close(subscription_id) => {
                    handle_close(subscriptions, addr, &subscription_id).await
                }
                nostr_sdk::ClientMessage::Event(event) => {
                    handle_event(sender, state, addr, &event, is_admin_connection).await
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
        .send(Message::Text(notice.as_json()))
        .await?;
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
            let res = sender.lock().await.send(Message::Ping(vec![])).await;
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

fn match_event(event: &nostr_sdk::Event, filters: &Vec<nostr_sdk::Filter>) -> bool {
    let content = searchnos::index::text::extract_text(&event)
        .nfkc()
        .collect::<String>()
        .to_lowercase(); // TODO precompute this on EVENT and stop doing this for every filter

    filters
        .iter()
        .any(|filter| filter.match_event(event) && match_search(&content, filter))
}

async fn websocket(
    socket: WebSocket,
    state: Arc<AppState>,
    addr: SocketAddr,
    is_admin_connection: bool,
) {
    log::info!(
        "{} new websocket connection (admin: {})",
        addr,
        is_admin_connection
    );
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));
    let subscriptions = Arc::new(Mutex::new(HashMap::<
        nostr_sdk::SubscriptionId,
        Vec<nostr_sdk::Filter>,
    >::new()));

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
                                    .send(Message::Text(relay_msg.as_json()))
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
                    is_admin_connection,
                )
                .await;
                if let Err(e) = res {
                    log::warn!("{} error processing message: {}", addr, e);
                    let res = send_notice(sender, &format!("Error: {}", e)).await;
                    if let Err(e) = res {
                        log::error!("{} error sending notice: {}", addr, e);
                        break;
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
    log::info!("{} disconnected", addr);
}

async fn ping() -> impl IntoResponse {
    println!("PING");

    StatusCode::OK
}

struct ReturnRelayInfoExtractor {}

#[async_trait]
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

            return Err(res);
        }
    }
}

async fn websocket_handler(
    _: ReturnRelayInfoExtractor,
    params: Query<Parameter>,
    ws: WebSocketUpgrade,
    Extension(state): Extension<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> impl IntoResponse {
    let is_admin_connection = if let Some(api_key) = &params.api_key {
        state.api_key == *api_key
    } else {
        false
    };

    ws.on_upgrade(move |socket| websocket(socket, state, addr, is_admin_connection))
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

    /// API key for administrative connection to searchnos
    #[arg(long, env)]
    api_key: String,

    /// Maximum number of subscriptions per client
    #[arg(long, env, default_value_t = 8)]
    max_subscriptions: usize,

    /// Maximum number of filters per subscription
    #[arg(long, env, default_value_t = 8)]
    max_filters: usize,

    /// Ping interval in seconds
    #[arg(long, env, default_value_t = 55)]
    ping_interval: u64,

    /// Index TTL in days
    #[arg(long, env)]
    index_ttl_days: Option<u64>,

    /// Maximum number of days in the future to accept events for
    #[arg(long, env, default_value_t = 1)]
    index_allow_future_days: u64,
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
    relay_info.supported_nips = Some(vec![1, 9, 11, 22, 28, 50]);
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

    let app_state = Arc::new(AppState {
        relay_info,
        es_client,
        index_name_prefix: index_name_prefix.to_string(),
        index_alias_name: index_alias_name.to_string(),
        max_subscriptions: args.max_subscriptions, // TODO include this in relay info
        max_filters: args.max_filters,             // TODO include this in relay info
        api_key: args.api_key.to_string(),
        ping_interval: Duration::from_secs(args.ping_interval),
        index_ttl_days: args.index_ttl_days,
        index_allow_future_days: args.index_allow_future_days,
        tx,
    });

    if args.index_ttl_days.is_some() {
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
    axum::Server::bind(&addr)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    fn find_available_port() -> Option<u16> {
        (32768..65535).find(
            |port| match TcpListener::bind(format!("127.0.0.1:{}", port)) {
                Ok(_) => true,
                Err(_) => false,
            },
        )
    }

    #[tokio::test]
    async fn somke_test() {
        env_logger::init();

        let port = find_available_port().expect("no available port found");
        let args = Args {
            port,
            es_url: "http://elastic:super-secret-nostaro@localhost:9200".to_string(),
            api_key: "TEST_API_KEY".to_string(),
            max_subscriptions: 100,
            max_filters: 32,
            ping_interval: 55,
            index_ttl_days: None,
            index_allow_future_days: 1,
        };
        let app = app(&args).await.unwrap();
        let addr = SocketAddr::from(([127, 0, 0, 1], args.port));

        let join_handle = tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });

        let keys = nostr_sdk::Keys::generate();
        let client = nostr_sdk::Client::new(&keys);
        client
            .add_relay(format!("ws://localhost:{}", args.port))
            .await
            .unwrap();
        client.connect().await;

        let filter = nostr_sdk::Filter::new().search("nostr").limit(0);
        let res = client
            .get_events_of_with_opts(
                vec![filter],
                Some(Duration::from_secs(5)),
                nostr_sdk::FilterOptions::ExitOnEOSE,
            )
            .await;
        assert!(res.is_ok());

        join_handle.abort();
    }
}
