#[macro_use]
extern crate serde_json;

mod condition;
mod engine;
mod search;

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
use log::{error, info};
use nostr_sdk::prelude::{Filter, RelayMessage, SubscriptionId};
use std::collections::HashMap;
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

fn make_notice(msg: String) -> String {
    let relay_message = RelayMessage::new_notice(msg);
    relay_message.as_json()
}

async fn handler(socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    let (sender, mut receiver) = socket.split();
    let sender = Arc::new(tokio::sync::Mutex::new(sender));
    info!("{}: connected", &addr.to_string());
    let tasks = Arc::new(Mutex::new(HashMap::<String, JoinHandle<_>>::new()));

    loop {
        tokio::select! {
            ws_next = receiver.next() => {
                match ws_next {
                    Some(Ok(msg)) => {
                        let msg = msg.to_text().expect("Can't get text");
                        let parsed = serde_json::from_str::<serde_json::Value>(msg);
                        if let Err(e) = parsed {
                            error!("{}: {:?}", addr, e);
                            let _ = sender.lock().await.send(Message::Text(make_notice("Can't parse to JSON".to_string()))).await;
                            continue;
                        }
                        let parsed = parsed.unwrap();
                        match parsed[0].as_str() {
                            None => {
                                error!("{}: failed to get event type", addr);
                                let _ = sender.lock().await.send(Message::Text(make_notice("Can't get event type".to_string()))).await;
                                continue;
                            }
                            Some(t) => {
                                match t {
                                    "REQ" => {
                                        let subscription_id = parsed[1].as_str();
                                        if subscription_id.is_none() {
                                            error!("{}: failed to get subscription id", addr);
                                            let _ = sender.lock().await.send(Message::Text(make_notice("Can't get subscription id".to_string()))).await;
                                            continue;
                                        }
                                        let subscription_id = subscription_id.unwrap().to_string();
                                        info!("{}: REQ {} received", addr, subscription_id);

                                        let filter = serde_json::from_value::<Filter>(parsed[2].clone());
                                        if filter.is_err() {
                                            error!("{}: {:?}", addr, filter);
                                            let _ = sender.lock().await.send(Message::Text(make_notice("Can't get filter".to_string()))).await;
                                            continue;
                                        }
                                        let filter = filter.unwrap();
                                        if filter.search.is_none() {
                                            error!("{}: search is empty", addr);
                                            let _ = sender.lock().await.send(Message::Text(make_notice("search must be specified for filter".to_string()))).await;
                                            continue;
                                        }
                                        let search = filter.search.unwrap();
                                        let condition = Condition::new(search);

                                        // remove old task
                                        let removed = tasks.lock().await.remove(&subscription_id);
                                        if let Some(task) = removed {
                                            task.abort();
                                        }

                                        // search older notes
                                        let notes = state.engine.search_once(&condition).await;
                                        if notes.is_err() {
                                            error!("{}: {:?}", addr, notes);
                                            let _ = sender.lock().await.send(Message::Text(make_notice("Can't search".to_string()))).await;
                                            continue;
                                        }
                                        let notes = notes.unwrap();
                                        for note in notes.iter() {
                                            let relay_message = RelayMessage::new_event(SubscriptionId::new(subscription_id.clone()), note.clone());
                                            let _ = sender.lock().await.send(Message::Text(relay_message.as_json())).await;
                                        }
                                        let latest_known = if notes.len() > 0 {
                                            Some(notes[notes.len() - 1].created_at.as_u64())
                                        } else {
                                            None
                                        };

                                        // send EOSE
                                        let eose = RelayMessage::new_eose(SubscriptionId::new(subscription_id.clone()));
                                        let _ = sender.lock().await.send(Message::Text(eose.as_json())).await;

                                        // setup subscription
                                        let receiver = state.engine.subscribe(addr, subscription_id.to_string(), condition.clone());
                                        let subscription_id = subscription_id.to_string();
                                        let subscription_id_ = subscription_id.clone();
                                        let sender_ = sender.clone();
                                        let task = tokio::spawn(async move {
                                            let mut receiver = receiver;
                                            while let Ok(events) = receiver.recv().await {
                                                for event in events.iter() {
                                                    if let Some(newest) = latest_known {
                                                        // prevent sending duplicate events
                                                        if event.created_at.as_u64() <= newest {
                                                            continue;
                                                        }
                                                    }
                                                    let relay_message = RelayMessage::new_event(SubscriptionId::new(subscription_id_.clone()), event.clone());
                                                    let _ = sender_.lock().await.send(Message::Text(relay_message.as_json())).await;
                                                }
                                            }
                                        });
                                        tasks.lock().await.insert(subscription_id, task);
                                    }
                                    "CLOSE" => {
                                        let subscription_id = parsed[1].as_str();
                                        if subscription_id.is_none() {
                                            error!("{}: failed to get subscription id", addr);
                                            let _ = sender.lock().await.send(Message::Text(make_notice("Can't get subscription id".to_string()))).await;
                                            continue;
                                        }
                                        let subscription_id = subscription_id.unwrap();
                                        info!("{}: CLOSE {}", addr, subscription_id);

                                        state.engine.unsubscribe(addr, subscription_id.to_string());

                                        let removed = tasks.lock().await.remove(subscription_id);
                                        if let Some(task) = removed {
                                            task.abort();
                                        }
                                    }
                                    _ => {
                                        error!("{}: unknown event type {}", addr, t);
                                        let _ = sender.lock().await.send(Message::Text(make_notice(format!("Unsupported event type {}", t)))).await;
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("{}: {:?}", addr, e);
                    }
                    None => {
                        state.engine.leave(addr);
                        for task in tasks.lock().await.values() {
                            task.abort();
                        }
                        info!("{}: disconnected", addr);
                        break;
                    }
                }
            }
        }
    }
    ()
}

async fn websocket(socket: WebSocket, state: Arc<AppState>, addr: SocketAddr) {
    tokio::spawn(handler(socket, state, addr));
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

    // prepare elasticsearch client
    let es_url = Url::parse(&es_url).expect("invalid elasticsearch url");
    let conn_pool = SingleNodeConnectionPool::new(es_url);
    let es_transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let es_client = Elasticsearch::new(es_transport);
    let index_name = "nostr".to_string();
    info!("elasticsearch index ready");

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
    info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();

    Ok(())
}
