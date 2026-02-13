use anyhow::{anyhow, Context};
use axum::{
    extract::{connect_info::ConnectInfo, FromRequestParts},
    http::{request::Parts, StatusCode},
    response::IntoResponse,
    routing::get,
    Extension, Router,
};
use futures::{sink::SinkExt, stream::StreamExt};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use nostr_sdk::{
    nips::nip42,
    prelude::{RelayInformationDocument, RelayMessage, ToBech32},
    Event, JsonUtil, Kind, PublicKey, RelayUrl,
};
use rand::{distr::Alphanumeric, Rng};
use searchnos::app_state::AppState;
use searchnos::client_addr::ClientAddr;
use searchnos::index::fetcher::spawn_fetcher;
use searchnos::index::handlers::{handle_event, send_ok};
use searchnos::plugin::WritePolicyPlugin;
use searchnos::search::handlers::{handle_close, handle_req, ClosedError, SubscriptionHandle};
use searchnos_db::{PurgePolicy as DbPurgePolicy, SearchnosDB, SearchnosDBOptions};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use std::{env, net::SocketAddr, str::FromStr, sync::Arc};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{Instrument, Span};
use yawc::{
    frame::{FrameView, OpCode},
    CompressionLevel, IncomingUpgrade, Options, WebSocket as YawcWebSocket,
};

fn generate_auth_challenge() -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .map(char::from)
        .take(32)
        .collect()
}

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

const DB_PURGE_TICK_INTERVAL_SECS: u64 = 60;
const DB_PURGE_BATCH_SIZE: usize = 1024;

const DEFAULT_FETCH_KINDS: [Kind; 9] = [
    Kind::Metadata,
    Kind::TextNote,
    Kind::EventDeletion,
    Kind::LongFormTextNote,
    Kind::ChannelCreation,
    Kind::ChannelMetadata,
    Kind::ChannelMessage,
    Kind::ChannelHideMessage,
    Kind::ChannelMuteUser,
];

fn parse_src_relays(values: &[String]) -> anyhow::Result<Vec<RelayUrl>> {
    let mut relays = Vec::new();
    for raw in values {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let relay =
            RelayUrl::parse(trimmed).with_context(|| format!("invalid relay url '{}'", trimmed))?;
        relays.push(relay);
    }
    Ok(relays)
}

fn parse_fetch_kinds(values: &[String]) -> anyhow::Result<Vec<Kind>> {
    let mut kinds = Vec::new();
    for raw in values {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let kind = Kind::from_str(trimmed)
            .map_err(|err| anyhow!("invalid fetch kind '{}': {}", trimmed, err))?;
        kinds.push(kind);
    }
    Ok(kinds)
}

fn spawn_purge_worker(db: Arc<SearchnosDB>) {
    tokio::spawn(async move {
        let tick_interval = Duration::from_secs(DB_PURGE_TICK_INTERVAL_SECS);
        let mut ticker = time::interval(tick_interval);
        let mut run_id: u64 = 0;
        loop {
            ticker.tick().await;
            run_id = run_id.saturating_add(1);
            let started_at = std::time::Instant::now();
            tracing::info!(
                run_id,
                batch_size = DB_PURGE_BATCH_SIZE,
                tick_interval_secs = DB_PURGE_TICK_INTERVAL_SECS,
                "starting purge run"
            );

            let db_for_run = db.clone();
            match tokio::task::spawn_blocking(move || {
                db_for_run.purge_stale_events(DB_PURGE_BATCH_SIZE)
            })
            .await
            {
                Ok(Ok(removed)) => {
                    tracing::info!(
                        run_id,
                        removed,
                        elapsed_ms = started_at.elapsed().as_millis() as u64,
                        "purge run completed"
                    );
                }
                Ok(Err(err)) => {
                    tracing::error!(error = %err, "failed to purge stale events");
                }
                Err(err) => {
                    tracing::error!(error = %err, "purge worker stopped unexpectedly");
                    break;
                }
            }
        }
    });
}

struct ConnectionAuthState {
    challenge: String,
    challenge_sent: bool,
    authenticated_pubkeys: std::collections::BTreeSet<PublicKey>,
}

impl ConnectionAuthState {
    fn new(challenge: String) -> Self {
        Self {
            challenge,
            challenge_sent: false,
            authenticated_pubkeys: std::collections::BTreeSet::new(),
        }
    }

    fn authenticated_pubkeys(&self) -> Vec<PublicKey> {
        self.authenticated_pubkeys.iter().cloned().collect()
    }

    fn ensure_challenge(&mut self) -> String {
        if !self.challenge_sent {
            self.challenge_sent = true;
        }
        self.challenge.clone()
    }

    fn register_authenticated_pubkey(&mut self, pubkey: PublicKey) {
        self.authenticated_pubkeys.insert(pubkey);
        self.challenge = generate_auth_challenge();
        self.challenge_sent = false;
    }
}

async fn process_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    subscriptions: Arc<Mutex<HashMap<nostr_sdk::SubscriptionId, SubscriptionHandle>>>,
    addr: ClientAddr,
    msg: FrameView,
    auth_state: Arc<Mutex<ConnectionAuthState>>,
) -> anyhow::Result<()> {
    match msg.opcode {
        OpCode::Text => {
            let payload = msg.payload;
            let text =
                std::str::from_utf8(&payload).context("received text frame with invalid UTF-8")?;
            tracing::info!("RECEIVED {}", text);
            let client_message = nostr_sdk::ClientMessage::from_json(payload.as_ref())?;
            match client_message {
                nostr_sdk::ClientMessage::Req {
                    subscription_id,
                    filters,
                } => {
                    let filters = filters
                        .into_iter()
                        .map(|filter| filter.into_owned())
                        .collect::<Vec<_>>();
                    handle_req(
                        state.clone(),
                        sender.clone(),
                        subscriptions.clone(),
                        &subscription_id,
                        filters,
                    )
                    .await
                }
                nostr_sdk::ClientMessage::Close(subscription_id) => {
                    handle_close(subscriptions.clone(), addr.clone(), &subscription_id).await
                }
                nostr_sdk::ClientMessage::Event(event) => {
                    let (challenge_to_send, authenticated_pubkeys) = {
                        let mut auth_state = auth_state.lock().await;
                        let authed_pubkeys = auth_state.authenticated_pubkeys();
                        let challenge = if authed_pubkeys.is_empty() && !auth_state.challenge_sent {
                            Some(auth_state.ensure_challenge())
                        } else {
                            None
                        };
                        (challenge, authed_pubkeys)
                    };

                    if let Some(challenge) = challenge_to_send {
                        send_auth_challenge(sender.clone(), &challenge).await?;
                    }

                    let authenticated_pubkeys_hex: Vec<String> = authenticated_pubkeys
                        .iter()
                        .map(|pk| pk.to_string())
                        .collect();

                    let should_send_auth = handle_event(
                        sender.clone(),
                        state.clone(),
                        addr.clone(),
                        &event,
                        authenticated_pubkeys_hex,
                    )
                    .await?;

                    if should_send_auth {
                        let challenge = {
                            let mut auth_state = auth_state.lock().await;
                            auth_state.ensure_challenge()
                        };
                        send_auth_challenge(sender.clone(), &challenge).await?;
                    }

                    Ok(())
                }
                nostr_sdk::ClientMessage::Auth(event) => {
                    handle_auth_message(
                        state.clone(),
                        sender.clone(),
                        event.into_owned(),
                        auth_state.clone(),
                    )
                    .await?;
                    Ok(())
                }
                other => Err(anyhow!("invalid message type: {:?}", other)),
            }?
        }
        OpCode::Close => {
            tracing::info!("close message received");
            return Ok(());
        }
        OpCode::Pong => {}
        OpCode::Ping => {}
        OpCode::Binary => {
            return Err(anyhow::anyhow!("binary websocket frames are not supported"));
        }
        OpCode::Continuation => {
            return Err(anyhow::anyhow!("continuation frames are not supported"));
        }
    }
    Ok(())
}

async fn send_notice(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    msg: &str,
) -> anyhow::Result<()> {
    let notice = RelayMessage::notice(msg);
    sender
        .lock()
        .await
        .send(FrameView::text(notice.as_json()))
        .await?;
    Ok(())
}

async fn send_auth_challenge(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    challenge: &str,
) -> anyhow::Result<()> {
    let auth = RelayMessage::auth(challenge.to_string());
    sender
        .lock()
        .await
        .send(FrameView::text(auth.as_json()))
        .await?;
    Ok(())
}

async fn send_closed(
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    subscription_id: nostr_sdk::SubscriptionId,
    msg: &str,
) -> anyhow::Result<()> {
    let closed = RelayMessage::closed(subscription_id, msg);
    sender
        .lock()
        .await
        .send(FrameView::text(closed.as_json()))
        .await?;
    Ok(())
}

async fn handle_auth_message(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
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
        tracing::warn!(
            "authentication failed due to invalid signature for {}: {}",
            event.id,
            e
        );
        send_notice(sender.clone(), "auth: invalid signature").await?;
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
        tracing::warn!(
            "authentication failed due to invalid challenge or relay (pubkey {})",
            npub
        );
        send_notice(sender.clone(), "auth: invalid challenge").await?;
        return Ok(());
    }

    let authed_count = {
        let mut auth_state = auth_state.lock().await;
        auth_state.register_authenticated_pubkey(event.pubkey);
        auth_state.authenticated_pubkeys.len()
    };

    tracing::info!(
        auth_pubkey = %npub,
        total_authenticated_pubkeys = authed_count,
        "nip42 authentication verified"
    );
    send_ok(sender.clone(), &event, true, "").await?;

    Ok(())
}

async fn spawn_pinger(
    state: Arc<AppState>,
    sender: Arc<Mutex<futures::stream::SplitSink<YawcWebSocket, FrameView>>>,
    span: Span,
) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            loop {
                tokio::time::sleep(state.ping_interval).await;
                tracing::info!("sending ping");
                let res = sender
                    .lock()
                    .await
                    .send(FrameView::ping(Vec::<u8>::new()))
                    .await;
                if let Err(e) = res {
                    tracing::warn!("error sending ping: {}", e);
                    return;
                }
            }
        }
        .instrument(span),
    )
}

async fn websocket(
    socket: YawcWebSocket,
    state: Arc<AppState>,
    addr: ClientAddr,
    compression_enabled: bool,
) {
    let conn_id = NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
    let socket_addr = addr.socket_addr();
    let forwarded_header = addr.forwarded_raw().map(str::to_owned);
    let connection_span = if let Some(ref header) = forwarded_header {
        tracing::info_span!(
            "connection",
            conn_id = conn_id,
            remote_ip = %socket_addr.ip(),
            remote_port = socket_addr.port(),
            forwarded = header.as_str(),
            compression = compression_enabled,
        )
    } else {
        tracing::info_span!(
            "connection",
            conn_id = conn_id,
            remote_ip = %socket_addr.ip(),
            remote_port = socket_addr.port(),
            compression = compression_enabled,
        )
    };
    let span_for_pinger = connection_span.clone();

    async move {
        let active_connections = state.active_connections.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!(active_connections, "new websocket connection");
        let (sender, mut receiver) = socket.split();
        let sender = Arc::new(Mutex::new(sender));
        let subscriptions = Arc::new(Mutex::new(HashMap::new()));
        let auth_state = Arc::new(Mutex::new(ConnectionAuthState::new(
            generate_auth_challenge(),
        )));

        // spawn pinger
        let pinger_handle = spawn_pinger(state.clone(), sender.clone(), span_for_pinger).await;

        while let Some(msg) = receiver.next().await {
            let res = process_message(
                state.clone(),
                sender.clone(),
                subscriptions.clone(),
                addr.clone(),
                msg,
                auth_state.clone(),
            )
            .await;

            if let Err(err) = res {
                tracing::warn!("error processing message: {}", err);
                if let Some(closed) = err.downcast_ref::<ClosedError>() {
                    if let Err(send_err) = send_closed(
                        sender.clone(),
                        closed.subscription_id.clone(),
                        &closed.to_string(),
                    )
                    .await
                    {
                        tracing::error!("error sending closed: {}", send_err);
                        break;
                    }
                } else if let Err(send_err) =
                    send_notice(sender.clone(), &format!("Error: {}", err)).await
                {
                    tracing::error!("error sending notice: {}", send_err);
                    break;
                }
            }
        }

        let active_connections = state
            .active_connections
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        tracing::info!(active_connections, "websocket connection closed");

        let pending_subscriptions: Vec<nostr_sdk::SubscriptionId> = {
            let guard = subscriptions.lock().await;
            guard.keys().cloned().collect()
        };
        for sub_id in pending_subscriptions {
            if let Err(err) = handle_close(subscriptions.clone(), addr.clone(), &sub_id).await {
                tracing::debug!(error = %err, subscription = %sub_id, "error during cleanup CLOSE");
            }
        }

        pinger_handle.abort();
        let authed_pubkeys = {
            let auth_state = auth_state.lock().await;
            auth_state.authenticated_pubkeys()
        };
        if authed_pubkeys.is_empty() {
            tracing::info!(active_connections, "disconnected");
        } else {
            let authed_list = authed_pubkeys
                .iter()
                .map(|pk| pk.to_bech32().unwrap_or_else(|_| pk.to_string()))
                .collect::<Vec<_>>()
                .join(",");
            tracing::info!(
                authenticated_pubkeys = authed_list.as_str(),
                active_connections,
                "disconnected"
            );
        }
    }
    .instrument(connection_span)
    .await;
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
    ws: IncomingUpgrade,
    Extension(state): Extension<Arc<AppState>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: axum::http::HeaderMap,
) -> axum::response::Response {
    let forwarded_header = if state.respect_forwarded_headers {
        headers
            .get(axum::http::header::FORWARDED)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    } else {
        None
    };
    let client_addr = ClientAddr::from_headers(addr, forwarded_header.as_deref());
    let options = Options::default()
        .with_compression_level(CompressionLevel::best())
        .with_utf8();

    let (response, upgrade) = match ws.upgrade(options) {
        Ok(parts) => parts,
        Err(err) => {
            tracing::warn!("failed to prepare websocket upgrade: {}", err);
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    let compression_enabled = response
        .headers()
        .get(axum::http::header::SEC_WEBSOCKET_EXTENSIONS)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("permessage-deflate"));

    tokio::spawn(async move {
        match upgrade.await {
            Ok(socket) => websocket(socket, state, client_addr, compression_enabled).await,
            Err(err) => tracing::warn!("failed to complete websocket upgrade: {}", err),
        }
    });

    response.into_response()
}

use clap::{Args as ClapArgs, Parser, Subcommand};

mod cmd;
#[derive(Parser, Debug)]
#[command(about,long_about = None)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(ClapArgs, Debug)]
pub struct CommonArgs {
    /// Path to searchnos-db storage directory
    #[arg(long, env = "SEARCHNOS_DB_PATH", default_value = "data")]
    db_path: String,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Display basic database statistics
    Stat,
    /// Export all events as newline-delimited JSON sorted by newest first
    Export,
    Serve(ServeArgs),
    Import(ImportArgs),
}

#[derive(ClapArgs, Debug)]
struct ServeArgs {
    /// Port to listen on
    #[arg(long, env, default_value_t = 3000)]
    port: u16,

    /// Number of events to batch before flushing to LMDB
    #[arg(
        long = "db-batch-size",
        env = "SEARCHNOS_DB_BATCH_SIZE",
        default_value_t = 4_096
    )]
    db_batch_size: usize,

    /// Maximum interval in milliseconds before pending events are flushed
    #[arg(
        long = "db-flush-interval-ms",
        env = "SEARCHNOS_DB_FLUSH_INTERVAL_MS",
        default_value_t = 100
    )]
    db_flush_interval_ms: u64,

    /// Public relay URL used to validate AUTH events (optional)
    #[arg(long = "public-relay-url", env = "PUBLIC_RELAY_URL")]
    public_relay_url: Option<String>,

    /// Comma-separated list of relays to fetch events from
    #[arg(
        long = "src-relays",
        env = "SRC_RELAYS",
        value_delimiter = ',',
        num_args = 0..
    )]
    src_relays: Vec<String>,

    /// Comma-separated list of event kind numbers to fetch
    #[arg(
        long = "fetch-kinds",
        env = "FETCH_KINDS",
        value_delimiter = ',',
        num_args = 0..
    )]
    fetch_kinds: Vec<String>,

    /// Maximum number of subscriptions per client
    #[arg(long, env, default_value_t = 20)]
    max_subscriptions: usize,

    /// Maximum number of filters per subscription
    #[arg(long, env, default_value_t = 8)]
    max_filters: usize,

    /// Ping interval in seconds
    #[arg(long, env, default_value_t = 55)]
    ping_interval: u64,

    /// Purge specifications for the database (e.g. "30d", "1:never", "2:3d", "3:purge")
    #[arg(
        long = "db-purge",
        env = "SEARCHNOS_DB_PURGE",
        value_delimiter = ',',
        num_args = 1..
    )]
    db_purge: Option<Vec<String>>,

    /// Use Forwarded header when logging client addresses
    #[arg(long = "respect-forwarded", env = "SEARCHNOS_RESPECT_FORWARDED")]
    respect_forwarded_headers: bool,

    /// Command executed for the write policy plugin
    #[arg(
        long = "write-policy-plugin",
        env = "WRITE_POLICY_PLUGIN",
        value_name = "CMD"
    )]
    write_policy_plugin: Option<String>,

    /// When set, reject all EVENT messages before invoking the plugin
    #[arg(long = "block-event-message", env = "BLOCK_EVENT_MESSAGE")]
    block_event_message: bool,
}

#[derive(ClapArgs, Debug)]
struct ImportArgs {
    /// Path to JSONL file containing events
    #[arg(value_name = "JSONL_PATH")]
    import_path: String,

    /// Number of events to batch before flushing to LMDB
    #[arg(
        long = "db-batch-size",
        env = "SEARCHNOS_DB_BATCH_SIZE",
        default_value_t = 4_096
    )]
    db_batch_size: usize,

    /// Maximum interval in milliseconds before pending events are flushed
    #[arg(
        long = "db-flush-interval-ms",
        env = "SEARCHNOS_DB_FLUSH_INTERVAL_MS",
        default_value_t = 100
    )]
    db_flush_interval_ms: u64,

    /// Comma-separated list of event kind numbers to import
    #[arg(
        long = "fetch-kinds",
        env = "FETCH_KINDS",
        value_delimiter = ',',
        num_args = 0..
    )]
    fetch_kinds: Vec<String>,
}

async fn app(common: &CommonArgs, args: &ServeArgs) -> Result<Router, Box<dyn std::error::Error>> {
    let version = format!(
        "v{}-{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH").chars().take(7).collect::<String>()
    );
    let pkg_name = env!("CARGO_PKG_NAME").to_string();

    tracing::info!("{} {}", pkg_name, version);

    if args.db_batch_size == 0 {
        return Err(anyhow!("db batch size must be greater than zero").into());
    }

    if args.db_flush_interval_ms == 0 {
        return Err(anyhow!("db flush interval must be greater than zero").into());
    }

    let src_relays = parse_src_relays(&args.src_relays)?;
    let mut fetch_kinds = parse_fetch_kinds(&args.fetch_kinds)?;
    if fetch_kinds.is_empty() && !src_relays.is_empty() {
        fetch_kinds = DEFAULT_FETCH_KINDS.to_vec();
    }
    if !fetch_kinds.is_empty() {
        fetch_kinds.sort();
        fetch_kinds.dedup();
    }

    let purge_policy = match args.db_purge.as_ref() {
        Some(specs) => {
            let policy = DbPurgePolicy::from_specs(specs.iter().map(|s| s.as_str()))
                .map_err(|err| anyhow!("invalid db purge spec: {err}"))?;
            tracing::info!(specs = ?specs, "configured database purge policy");
            Some(policy)
        }
        None => None,
    };

    tracing::info!(
        path = %common.db_path,
        batch_size = args.db_batch_size,
        flush_interval_ms = args.db_flush_interval_ms,
        purge_enabled = purge_policy.is_some(),
        "opening searchnos-db"
    );

    let db_options = SearchnosDBOptions {
        batch_size: args.db_batch_size,
        flush_interval: Duration::from_millis(args.db_flush_interval_ms),
        purge_policy: purge_policy.clone(),
        default_limit: Some(500),
        max_limit: Some(1000),
        ..SearchnosDBOptions::default()
    };

    let db = Arc::new(SearchnosDB::open_with_options(&common.db_path, db_options)?);

    if purge_policy.is_some() {
        spawn_purge_worker(db.clone());
    }

    let mut relay_info = RelayInformationDocument::new();
    relay_info.name = Some("searchnos".to_string()); // TODO make this configurable
    relay_info.description = Some("searchnos relay".to_string()); // TODO make this configurable
    relay_info.supported_nips = Some(vec![1, 9, 11, 22, 28, 40, 42, 50, 70]);
    relay_info.software = Some(pkg_name);
    relay_info.version = Some(version);
    let relay_info = serde_json::to_string(&relay_info).unwrap();

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

    let write_policy_plugin = args
        .write_policy_plugin
        .as_ref()
        .map(|cmd| cmd.trim())
        .filter(|cmd| !cmd.is_empty());
    let write_policy_plugin = if let Some(command) = write_policy_plugin {
        tracing::info!(command = %command, "write policy plugin configured");
        Some(Arc::new(WritePolicyPlugin::new(command.to_string())?))
    } else {
        tracing::info!("write policy plugin disabled");
        None
    };

    if args.block_event_message {
        tracing::info!("EVENT messages blocked by configuration");
    }

    let app_state = Arc::new(AppState {
        db,
        relay_info,
        max_subscriptions: args.max_subscriptions,
        max_filters: args.max_filters,
        public_relay_url,
        ping_interval: Duration::from_secs(args.ping_interval),
        respect_forwarded_headers: args.respect_forwarded_headers,
        active_connections: AtomicUsize::new(0),
        write_policy_plugin,
        block_event_message: args.block_event_message,
    });

    if !src_relays.is_empty() {
        let relay_list: Vec<String> = src_relays.iter().map(|url| url.to_string()).collect();
        let kind_list: Vec<u16> = fetch_kinds.iter().map(|kind| u16::from(*kind)).collect();
        tracing::info!(relays = ?relay_list, kinds = ?kind_list, "configured source fetching");
        let _fetch_handle = spawn_fetcher(app_state.clone(), src_relays, fetch_kinds);
    }

    let app = Router::new()
        .route("/ping", get(ping))
        .route("/", get(websocket_handler))
        .layer(Extension(app_state));

    Ok(app)
}

fn init_tracing() {
    let _ = tracing_log::LogTracer::init();
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .without_time()
        .try_init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "debug");
    init_tracing();
    let Cli { common, command } = Cli::parse();
    match command {
        Command::Stat => cmd::stat::run(common).await,
        Command::Export => cmd::export::run(common).await,
        Command::Serve(args) => run_serve(common, args).await,
        Command::Import(args) => run_import(common, args).await,
    }
}

async fn run_serve(common: CommonArgs, args: ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let app = app(&common, &args).await?;

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    tracing::info!("listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

#[derive(Debug)]
struct ImportSummary {
    inserted: usize,
    skipped_kind: usize,
}

async fn run_import(
    common: CommonArgs,
    args: ImportArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let summary = tokio::task::spawn_blocking(move || import_blocking(common, args)).await??;
    tracing::info!(
        inserted = summary.inserted,
        skipped_kind = summary.skipped_kind,
        "import completed"
    );
    Ok(())
}

fn import_blocking(common: CommonArgs, args: ImportArgs) -> Result<ImportSummary, anyhow::Error> {
    if args.db_batch_size == 0 {
        return Err(anyhow!("db batch size must be greater than zero"));
    }

    if args.db_flush_interval_ms == 0 {
        return Err(anyhow!("db flush interval must be greater than zero"));
    }

    let fetch_kinds = parse_fetch_kinds(&args.fetch_kinds)?;
    let allowed_kinds: Option<HashSet<Kind>> = if fetch_kinds.is_empty() {
        None
    } else {
        Some(fetch_kinds.into_iter().collect())
    };

    let total = count_non_empty_lines(&args.import_path)?;
    let progress_bar = if total > 0 {
        let pb = ProgressBar::new(total as u64);
        pb.set_style(default_progress_style());
        Some(pb)
    } else {
        None
    };

    let options = SearchnosDBOptions {
        batch_size: args.db_batch_size,
        flush_interval: Duration::from_millis(args.db_flush_interval_ms),
        ..SearchnosDBOptions::default()
    };

    let db = SearchnosDB::open_with_options(&common.db_path, options)?;

    let file = File::open(&args.import_path)
        .with_context(|| format!("failed to open {}", args.import_path))?;
    let reader = BufReader::new(file);

    let mut inserted = 0usize;
    let mut skipped_kind = 0usize;

    for (idx, line) in reader.lines().enumerate() {
        let raw_line = line.with_context(|| format!("failed to read line {}", idx + 1))?;
        if raw_line.trim().is_empty() {
            continue;
        }

        if let Some(ref kinds) = allowed_kinds {
            let event: Event = serde_json::from_str(&raw_line)
                .with_context(|| format!("failed to parse event at line {}", idx + 1))?;
            if !kinds.contains(&event.kind) {
                skipped_kind += 1;
                if let Some(pb) = progress_bar.as_ref() {
                    pb.inc(1);
                }
                continue;
            }
        }

        db.insert_event_json_owned(raw_line)
            .with_context(|| format!("failed to import event at line {}", idx + 1))?;
        inserted += 1;

        if let Some(pb) = progress_bar.as_ref() {
            pb.inc(1);
        }
    }

    db.flush()?;

    if let Some(pb) = progress_bar {
        pb.finish_with_message(format!(
            "Imported {inserted} events (skipped {skipped_kind}) into {}",
            common.db_path
        ));
    }

    Ok(ImportSummary {
        inserted,
        skipped_kind,
    })
}

fn default_progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{percent:>3}%|{bar:40}| {pos}/{len} [{elapsed_precise}<{eta_precise}, {per_sec_ev}]",
    )
    .unwrap()
    .with_key(
        "per_sec_ev",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = write!(w, "{:.2} ev/s", state.per_sec());
        },
    )
}

fn count_non_empty_lines(path: &str) -> Result<usize, anyhow::Error> {
    let file = File::open(path).with_context(|| format!("failed to open {path}"))?;
    let reader = BufReader::new(file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;

    fn find_available_port() -> std::io::Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }

    #[tokio::test]
    async fn somke_test() {
        init_tracing();

        let port = match find_available_port() {
            Ok(port) => port,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                println!("skipping somke_test: {err}");
                return;
            }
            Err(err) => panic!("failed to find available port: {err}"),
        };
        let db_path = std::env::temp_dir()
            .join(format!("searchnos-db-test-{}", port))
            .display()
            .to_string();
        let common = CommonArgs {
            db_path: db_path.clone(),
        };
        let args = ServeArgs {
            port,
            db_batch_size: 4_096,
            db_flush_interval_ms: 100,
            public_relay_url: None,
            src_relays: Vec::new(),
            fetch_kinds: Vec::new(),
            max_subscriptions: 100,
            max_filters: 32,
            ping_interval: 55,
            db_purge: None,
            respect_forwarded_headers: false,
            write_policy_plugin: None,
            block_event_message: false,
        };
        let app = app(&common, &args).await.unwrap();
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

    #[tokio::test]
    async fn import_respects_fetch_kinds() {
        use std::fs::{self, File};
        use std::io::Write;

        let temp_dir =
            std::env::temp_dir().join(format!("searchnos-import-test-{}", rand::random::<u64>()));
        fs::create_dir_all(&temp_dir).unwrap();

        let db_path = temp_dir.join("db").display().to_string();
        let import_path = temp_dir.join("events.jsonl");

        let keys = nostr_sdk::Keys::generate();
        let allowed_event = nostr_sdk::EventBuilder::new(Kind::TextNote, "hello")
            .sign_with_keys(&keys)
            .unwrap();
        let skipped_event = nostr_sdk::EventBuilder::new(Kind::Metadata, "{}")
            .sign_with_keys(&keys)
            .unwrap();

        let mut file = File::create(&import_path).unwrap();
        writeln!(file, "{}", allowed_event.as_json()).unwrap();
        writeln!(file, "{}", skipped_event.as_json()).unwrap();

        let summary = import_blocking(
            CommonArgs {
                db_path: db_path.clone(),
            },
            ImportArgs {
                import_path: import_path.display().to_string(),
                db_batch_size: 4_096,
                db_flush_interval_ms: 100,
                fetch_kinds: vec![Kind::TextNote.as_u16().to_string()],
            },
        )
        .unwrap();

        assert_eq!(summary.inserted, 1);
        assert_eq!(summary.skipped_kind, 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }
}
