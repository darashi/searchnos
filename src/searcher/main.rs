#[macro_use]
extern crate serde_json;

mod filter;
mod search;

use anyhow::Context;
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
use chrono::{DateTime, Utc};
use elasticsearch::{
    http::{
        transport::{SingleNodeConnectionPool, TransportBuilder},
        Url,
    },
    Elasticsearch,
};
use env_logger;
use filter::Filter;
use futures::{sink::SinkExt, stream::StreamExt};
use nostr_sdk::prelude::{RelayInformationDocument, RelayMessage, SubscriptionId};
use search::ElasticsearchQuery;
use std::collections::HashMap;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Debug)]
struct AppState {
    es_client: Elasticsearch,
    index_name: String,
    relay_info: String,
}

async fn send_events(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: &SubscriptionId,
    events: Vec<nostr_sdk::Event>,
) -> anyhow::Result<()> {
    if events.is_empty() {
        sender.lock().await.send(Message::Ping(vec![])).await?;
    }

    for event in events {
        let relay_msg = RelayMessage::new_event(subscription_id.clone(), event);
        sender
            .lock()
            .await
            .send(Message::Text(relay_msg.as_json()))
            .await?;
    }

    Ok(())
}
async fn send_eose(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: &SubscriptionId,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::new_eose(subscription_id.clone());
    sender
        .lock()
        .await
        .send(Message::Text(relay_msg.as_json()))
        .await?;
    Ok(())
}

async fn query_then_send(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription_id: SubscriptionId,
    query: search::ElasticsearchQuery,
    cursor: Option<DateTime<Utc>>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let (events, new_cursor) = query
        .execute(&state.es_client, &state.index_name, cursor)
        .await?;
    send_events(sender.clone(), &subscription_id, events).await?;
    Ok(new_cursor)
}

async fn handle_req(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if msg.len() < 3 {
        return Err(anyhow::anyhow!("too few arguments"));
    }
    let subscription_id: String =
        serde_json::from_value(msg[1].clone()).context("invalid subscription id")?;
    let subscription_id = SubscriptionId::new(subscription_id);
    let filters = msg[2..].to_vec();

    log::info!(
        "{} REQ {:?} {:?}",
        addr,
        subscription_id.to_string(),
        filters
    );

    // expire old subscription if exists
    stop_subscription(join_handles.clone(), &subscription_id.clone()).await;

    // prepare filters and cursors
    // NOTE As `authors` is defined as `Option<Vec<XOnlyPublicKey>>` in `nostr_sdk::prelude::Filter`,
    // we cannot use shorter string than 64 charactors for prefix search condition.
    // We may need to use our own `Filter` type.
    let filters: Vec<Filter> = filters
        .into_iter()
        .map(|f| serde_json::from_value::<Filter>(f).context("parsing filter"))
        .collect::<Result<_, _>>()?;
    let mut cursors: Vec<Option<DateTime<Utc>>> = filters.iter().map(|_| None).collect();

    // do the first search
    for (filter, cursor) in filters.iter().zip(cursors.iter_mut()) {
        let query = ElasticsearchQuery::from_filter(filter.clone(), None);

        let new_cursor = query_then_send(
            state.clone(),
            sender.clone(),
            subscription_id.clone(),
            query,
            None,
        );
        *cursor = new_cursor.await?;
    }
    send_eose(sender.clone(), &subscription_id).await?;

    let sid_ = subscription_id.clone();
    let join_handle = tokio::spawn(async move {
        let mut cursors = cursors;
        loop {
            let wait = 5.0 + (rand::random::<f64>() * 5.0); // TODO better scheduling
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(wait)).await;
            log::info!(
                "{} continuing search for subscription {:?}",
                addr,
                &sid_.to_string()
            );

            for (filter, cursor) in filters.iter().zip(cursors.iter_mut()) {
                let query = ElasticsearchQuery::from_filter(filter.clone(), *cursor);

                let res =
                    query_then_send(state.clone(), sender.clone(), sid_.clone(), query, *cursor)
                        .await;
                match res {
                    Ok(new_cursor) => {
                        *cursor = new_cursor;
                    }
                    Err(e) => {
                        log::error!("error in continuing search: {:?}", e);
                    }
                }
            }
        }
    });
    join_handles
        .lock()
        .await
        .insert(subscription_id.to_string(), join_handle);

    Ok(())
}

async fn stop_subscription(
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    subscription_id: &SubscriptionId,
) {
    let removed = join_handles
        .lock()
        .await
        .remove(&subscription_id.to_string());
    if let Some(task) = removed {
        task.abort();
    }
}

async fn handle_close(
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if msg.len() != 2 {
        return Err(anyhow::anyhow!("invalid array length"));
    }

    let subscription_id = serde_json::from_value::<SubscriptionId>(msg[1].clone())
        .context("parsing subscription id")?;

    log::info!("{} CLOSE {:?}", addr, subscription_id.to_string());

    stop_subscription(join_handles, &subscription_id).await;

    Ok(())
}

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
    relay_info.supported_nips = Some(vec![1, 9, 11, 12, 15, 16, 22, 33, 50]);
    relay_info.software = Some(env!("CARGO_PKG_NAME").to_string());
    relay_info.version = Some(env!("CARGO_PKG_VERSION").to_string());
    let relay_info = serde_json::to_string(&relay_info).unwrap();

    let app_state = Arc::new(AppState {
        relay_info,
        es_client,
        index_name,
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
