#[macro_use]
extern crate serde_json;

mod condition;
mod engine;
mod search;

use anyhow::Context;
use axum::extract::connect_info::ConnectInfo;
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
use env_logger;
use futures::{sink::SinkExt, stream::StreamExt};
use nostr_sdk::prelude::{Filter, RelayMessage, SubscriptionId};
use std::collections::HashMap;
use std::time::SystemTime;
use std::{env, net::SocketAddr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tower_http::add_extension::AddExtensionLayer;

use crate::condition::Condition;
use crate::engine::Engine;

#[derive(Debug)]
struct AppState {
    engine: Engine,
}

#[derive(Debug, Clone)]
struct Subscription {
    id: SubscriptionId,
    filter: Filter,
    id_sent_at: Arc<Mutex<HashMap<String, SystemTime>>>,
}

impl From<&Vec<serde_json::Value>> for Subscription {
    fn from(msg: &Vec<serde_json::Value>) -> Self {
        let subscription_id = serde_json::from_value::<SubscriptionId>(msg[1].clone())
            .context("parsing subscription id")
            .unwrap();
        let filter = serde_json::from_value::<Filter>(msg[2].clone())
            .context("parsing filter")
            .unwrap();

        Subscription {
            id: subscription_id,
            filter,
            id_sent_at: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Subscription {
    fn to_condition(&self) -> Condition {
        Condition::from(&self.filter)
    }
}

async fn expire_old_ids(id_sent_at: Arc<Mutex<HashMap<String, SystemTime>>>) {
    let now = SystemTime::now();

    let ids_to_remove = id_sent_at
        .lock()
        .await
        .iter()
        .filter(|(_, sent_at)| now.duration_since(**sent_at).unwrap().as_secs() > 60)
        .map(|(id, _)| id.clone())
        .collect::<Vec<String>>();

    for id in ids_to_remove {
        id_sent_at.lock().await.remove(&id);
    }
}

async fn send_events(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription: Subscription,
    events: Vec<nostr_sdk::Event>,
) -> anyhow::Result<()> {
    for event in events {
        let event_id = &event.id.to_string();
        if subscription.id_sent_at.lock().await.contains_key(event_id) {
            continue;
        }

        let relay_msg = RelayMessage::new_event(subscription.id.clone(), event);

        sender
            .lock()
            .await
            .send(Message::Text(relay_msg.as_json()))
            .await?;
        subscription
            .id_sent_at
            .lock()
            .await
            .insert(event_id.clone(), SystemTime::now());
    }

    expire_old_ids(subscription.id_sent_at.clone()).await;

    Ok(())
}
async fn send_eose(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription: &Subscription,
) -> anyhow::Result<()> {
    let relay_msg = RelayMessage::new_eose(subscription.id.clone());
    sender
        .lock()
        .await
        .send(Message::Text(relay_msg.as_json()))
        .await?;
    Ok(())
}

async fn after_eose_transponder(
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    subscription: Subscription,
    mut broadcat_receiver: tokio::sync::broadcast::Receiver<Arc<Vec<nostr_sdk::Event>>>,
) {
    while let Ok(events) = broadcat_receiver.recv().await {
        send_events(sender.clone(), subscription.clone(), events.to_vec())
            .await
            .unwrap();
    }
}

async fn handle_req(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    let subscription = Subscription::from(msg);
    let condition: Condition = subscription.to_condition();

    // expire old subscription if exists
    stop_subscription(
        state.clone(),
        join_handles.clone(),
        addr,
        &subscription.id.clone(),
    )
    .await;

    // do the first search
    let limit = subscription.filter.limit;
    let notes = state.engine.search_once(&condition, &limit).await;
    match notes {
        Ok((notes, _latest)) => {
            send_events(sender.clone(), subscription.clone(), notes).await?;
            send_eose(sender.clone(), &subscription).await?;
        }
        Err(e) => {
            return Err(anyhow::anyhow!("error searching: {}", e));
        }
    }

    // spawn a task to handle the continuous search
    let broadcat_receiver = state
        .engine
        .subscribe(addr, subscription.id.to_string(), condition);
    let join_handle = tokio::spawn(after_eose_transponder(
        sender,
        subscription.clone(),
        broadcat_receiver,
    ));
    join_handles
        .lock()
        .await
        .insert(subscription.id.to_string(), join_handle);

    Ok(())
}

async fn stop_subscription(
    state: Arc<AppState>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    subscription_id: &SubscriptionId,
) {
    let subscription_id = subscription_id.to_string();

    state.engine.unsubscribe(addr, subscription_id.clone());
    let removed = join_handles.lock().await.remove(&subscription_id);
    if let Some(task) = removed {
        task.abort();
    }
}

async fn handle_close(
    state: Arc<AppState>,
    join_handles: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if msg.len() != 2 {
        return Err(anyhow::anyhow!("invalid array length"));
    }

    let subscription_id = serde_json::from_value::<SubscriptionId>(msg[1].clone())
        .context("parsing subscription id")?;

    stop_subscription(state, join_handles, addr, &subscription_id).await;

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
        Some("CLOSE") => handle_close(state, join_handles, addr, &msg).await?,
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
                            // unsubscribe all subscriptions
                            state.engine.leave(addr);

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
    StatusCode::OK
}

async fn websocket_handler(
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

    let engine = Engine::new(es_client, index_name);
    let app_state = Arc::new(AppState { engine });
    let state_ = app_state.clone();
    tokio::spawn(async move {
        state_.engine.searcher().await;
    });

    let app = Router::new()
        .route("/ping", get(ping))
        .route("/", get(websocket_handler))
        .layer(AddExtensionLayer::new(app_state));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    log::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();

    Ok(())
}
