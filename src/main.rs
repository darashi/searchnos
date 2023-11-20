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

#[derive(Deserialize, Debug)]
struct Parameter {
    api_key: Option<String>,
}

async fn handle_text_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    text: &String,
    is_admin_connection: bool,
) -> anyhow::Result<()> {
    let msg: Vec<serde_json::Value> = serde_json::from_str(&text)?;
    if msg.len() < 1 {
        return Err(anyhow::anyhow!("invalid array length"));
    }

    match msg[0].as_str() {
        Some("REQ") => handle_req(state, sender, join_handles, addr, &msg).await?,
        Some("CLOSE") => handle_close(join_handles, addr, &msg).await?,
        Some("EVENT") => handle_event(sender, state, addr, &msg, is_admin_connection).await?,
        _ => {
            return Err(anyhow::anyhow!("invalid message type"));
        }
    }

    Ok(())
}

async fn process_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: Message,
    is_admin_connection: bool,
) -> anyhow::Result<()> {
    match &msg {
        Message::Text(text) => {
            handle_text_message(
                state,
                sender,
                join_handles,
                addr,
                &text,
                is_admin_connection,
            )
            .await?
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
    let notice = RelayMessage::new_notice(msg);
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
    let join_handles = Arc::new(Mutex::new(HashMap::<String, JoinHandle<()>>::new()));

    // spawn pinger
    let pinger_handle = spawn_pinger(state.clone(), sender.clone(), addr).await;

    tokio::spawn(async move {
        loop {
            tokio::select! {
                ws_next = receiver.next() => {
                    match ws_next {
                        Some(Ok(msg)) => {
                            let sender = sender.clone();
                            let res = process_message(state.clone(), sender.clone(), join_handles.clone(), addr, msg, is_admin_connection).await;
                            if let Err(e) = res {
                                log::warn!("{} error processing message: {}", addr, e);
                                let res = send_notice(sender, &format!("Error: {}", e)).await;
                                if let Err(e) = res {
                                    log::error!("{} error sending notice: {}", addr, e);
                                    return;
                                }
                            }
                        }
                        Some(Err(e)) => {
                            log::warn!("{} error receiving message: {}", addr, e);
                        }
                        None => {
                            log::info!("{} websocket connection closed", addr);

                            // abort all ongoing tasks
                            for join_handle in join_handles.lock().await.values() {
                                join_handle.abort();
                            }
                            pinger_handle.abort();
                            log::info!("{} disconnected", addr);
                            return;
                        }
                    }
                }
            }
        }
    });
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
        .await
        .unwrap();

    Ok(())
}
