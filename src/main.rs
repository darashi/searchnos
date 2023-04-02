use axum::extract::connect_info::ConnectInfo;
use axum::extract::FromRequestParts;
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
use searchnos::app_state::AppState;
use searchnos::searcher::handlers::{handle_close, handle_req};
use std::collections::HashMap;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

async fn handle_text_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    text: &String,
) -> anyhow::Result<()> {
    let msg: Vec<serde_json::Value> = serde_json::from_str(&text)?;
    if msg.len() < 1 {
        return Err(anyhow::anyhow!("invalid array length"));
    }

    match msg[0].as_str() {
        Some("REQ") => handle_req(state, sender, join_handles, addr, &msg).await?,
        Some("CLOSE") => handle_close(join_handles, addr, &msg).await?,
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
) -> anyhow::Result<()> {
    match &msg {
        Message::Text(text) => {
            handle_text_message(state, sender, join_handles, addr, &text).await?
        }
        Message::Close(_) => {
            log::info!("{} close message received", addr);
            return Ok(());
        }
        Message::Pong(_) => {}
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

async fn websocket(socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    log::info!("{} new websocket connection", addr);
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(Mutex::new(sender));
    let join_handles = Arc::new(Mutex::new(HashMap::<String, JoinHandle<()>>::new()));

    tokio::spawn(async move {
        loop {
            tokio::select! {
                ws_next = receiver.next() => {
                    match ws_next {
                        Some(Ok(msg)) => {
                            let sender = sender.clone();
                            let res = process_message(state.clone(), sender.clone(), join_handles.clone(), addr, msg).await;
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
                        .body(relay_info)
                        .unwrap()
                        .into_response();

                    return Err(res);
                }
            }
            let res = axum::response::Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/text")
                .body("Please use a Nostr client to connect.".to_string())
                .unwrap()
                .into_response();

            return Err(res);
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    // env vars
    let es_url = env::var("ES_URL").expect("ES_URL is not set; set it to the URL of elasticsearch");
    let port = env::var("PORT").expect("PORT is not set; set it to the port number to listen on");
    let port = port
        .parse::<u16>()
        .expect("PORT is not a valid port number");
    let max_subscriptions = if let Ok(max_subscriptions) = env::var("MAX_SUBSCRIPTIONS") {
        max_subscriptions
            .parse::<usize>()
            .expect("MAX_SUBSCRIPTIONS is not a valid number")
    } else {
        8
    };
    let max_filters = if let Ok(max_filters) = env::var("MAX_FILTERS") {
        max_filters
            .parse::<usize>()
            .expect("MAX_FILTERS is not a valid number")
    } else {
        8
    };

    log::info!("connecting to elasticsearch");

    // prepare elasticsearch client
    let es_url = Url::parse(&es_url).expect("invalid elasticsearch url");
    let conn_pool = SingleNodeConnectionPool::new(es_url);
    let es_transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let es_client = Elasticsearch::new(es_transport);
    let index_name = "nostr".to_string();
    log::info!("elasticsearch index ready");

    let mut relay_info = RelayInformationDocument::new();
    relay_info.name = Some("searchnos".to_string()); // TODO make this configurable
    relay_info.description = Some("searchnos relay".to_string()); // TODO make this configurable
    relay_info.supported_nips = Some(vec![1, 9, 11, 12, 15, 16, 22, 28, 33, 50]);
    relay_info.software = Some(env!("CARGO_PKG_NAME").to_string());
    relay_info.version = Some(env!("CARGO_PKG_VERSION").to_string());
    let relay_info = serde_json::to_string(&relay_info).unwrap();

    let app_state = Arc::new(AppState {
        relay_info,
        es_client,
        index_name,
        max_subscriptions, // TODO include this in relay info
        max_filters,       // TODO include this in relay info
    });

    let app = Router::new()
        .route("/ping", get(ping))
        .route("/", get(websocket_handler))
        .layer(Extension(app_state));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    log::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();

    Ok(())
}
