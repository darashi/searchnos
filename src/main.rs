use anyhow::{anyhow, Context};
use axum::{
    extract::{connect_info::ConnectInfo, FromRequestParts},
    http::{request::Parts, StatusCode},
    response::IntoResponse,
    routing::get,
    Extension, Router,
};
use futures::stream::StreamExt;
use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};
use nostr_sdk::{prelude::RelayInformationDocument, Event, JsonUtil, Kind};
use searchnos::app_state::AppState;
use searchnos::client_addr::ClientAddr;
use searchnos::config::{parse_fetch_kinds, parse_src_relays, DEFAULT_FETCH_KINDS};
use searchnos::db_adapter::{insert_event_json, open_db_with_compact_workers};
use searchnos::index::fetcher::spawn_fetcher;
use searchnos::maintenance::{negentropy_relays, spawn_negentropy_signal_listener};
use searchnos::relay_sender::RelaySender;
use searchnos::search::handlers::{
    handle_close, handle_req, ClientMessageError, SubscriptionManager,
};
use searchnos_db::SearchnosDB;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{env, net::SocketAddr, sync::Arc};
use tokio::task::JoinHandle;
use tracing::{Instrument, Span};
use yawc::{
    frame::{Frame, OpCode},
    CompressionLevel, HttpWebSocket as YawcWebSocket, IncomingUpgrade, Options,
};

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

async fn process_message(
    state: Arc<AppState>,
    sender: RelaySender,
    subscriptions: SubscriptionManager,
    addr: ClientAddr,
    msg: Frame,
) -> Result<(), ClientMessageError> {
    match msg.opcode() {
        OpCode::Text => {
            let payload = msg.into_payload();
            let text =
                std::str::from_utf8(&payload).context("received text frame with invalid UTF-8")?;
            tracing::info!("RECEIVED {}", text);
            let client_message = nostr_sdk::ClientMessage::from_json(payload.as_ref())
                .map_err(anyhow::Error::from)?;
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
                    handle_close(subscriptions.clone(), addr.clone(), &subscription_id).await;
                    Ok(())
                }
                nostr_sdk::ClientMessage::Event(event) => {
                    tracing::info!(
                        id = %event.id,
                        kind = event.kind.as_u16(),
                        pubkey = %event.pubkey,
                        remote_ip = %addr.socket_addr().ip(),
                        remote_port = addr.socket_addr().port(),
                        "rejected EVENT message"
                    );
                    sender.ok(&event, false, "blocked: writes disabled").await?;
                    Ok(())
                }
                other => Err(anyhow!("invalid message type: {:?}", other).into()),
            }?
        }
        OpCode::Close => {
            tracing::info!("close message received");
            return Ok(());
        }
        OpCode::Pong => {}
        OpCode::Ping => {}
        OpCode::Binary => {
            return Err(anyhow::anyhow!("binary websocket frames are not supported").into());
        }
        OpCode::Continuation => {
            return Err(anyhow::anyhow!("continuation frames are not supported").into());
        }
    }
    Ok(())
}

async fn send_notice(sender: RelaySender, msg: &str) -> anyhow::Result<()> {
    sender.notice(msg).await
}

async fn send_closed(
    sender: RelaySender,
    subscription_id: nostr_sdk::SubscriptionId,
    msg: &str,
) -> anyhow::Result<()> {
    sender.closed(subscription_id, msg).await
}

async fn spawn_pinger(state: Arc<AppState>, sender: RelaySender, span: Span) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            loop {
                tokio::time::sleep(state.ping_interval).await;
                tracing::info!("sending ping");
                let res = sender.frame(Frame::ping(Vec::<u8>::new())).await;
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
        let sender = RelaySender::new(sender);
        let subscriptions = SubscriptionManager::new();

        // spawn pinger
        let pinger_handle = spawn_pinger(state.clone(), sender.clone(), span_for_pinger).await;

        while let Some(msg) = receiver.next().await {
            let res = process_message(
                state.clone(),
                sender.clone(),
                subscriptions.clone(),
                addr.clone(),
                msg,
            )
            .await;

            if let Err(err) = res {
                tracing::warn!("error processing message: {}", err);
                match err {
                    ClientMessageError::Closed {
                        subscription_id,
                        message,
                    } => {
                        if let Err(send_err) =
                            send_closed(sender.clone(), subscription_id, &message).await
                        {
                            tracing::error!("error sending closed: {}", send_err);
                            break;
                        }
                    }
                    ClientMessageError::Internal(err) => {
                        if let Err(send_err) =
                            send_notice(sender.clone(), &format!("Error: {}", err)).await
                        {
                            tracing::error!("error sending notice: {}", send_err);
                            break;
                        }
                    }
                }
            }
        }

        let active_connections = state
            .active_connections
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        tracing::info!(active_connections, "websocket connection closed");

        subscriptions.close_all(addr.clone()).await;

        pinger_handle.abort();
        tracing::info!(active_connections, "disconnected");
    }
    .instrument(connection_span)
    .await;
}

fn health_check_response(
    latest_event_created_at: Option<u64>,
    now: u64,
    max_event_age: Duration,
) -> (StatusCode, String) {
    let Some(latest_event_created_at) = latest_event_created_at else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "no events found".to_string(),
        );
    };

    let age_seconds = now.saturating_sub(latest_event_created_at);
    if age_seconds > max_event_age.as_secs() {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "latest event is stale: latest_event_age_seconds={age_seconds} max_age_seconds={}",
                max_event_age.as_secs()
            ),
        )
    } else {
        (
            StatusCode::OK,
            format!("OK latest_event_age_seconds={age_seconds}"),
        )
    }
}

async fn health_check(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    tracing::debug!("health check");

    let now = current_unix_timestamp();
    match latest_event_created_at(&state.db, now) {
        Ok(latest_event_created_at) => {
            health_check_response(latest_event_created_at, now, state.health_max_event_age)
        }
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("health check failed: {err}"),
        ),
    }
}

fn latest_event_created_at(db: &SearchnosDB, now: u64) -> anyhow::Result<Option<u64>> {
    let filters_json = format!(r#"[{{"limit":1,"until":{now}}}]"#);
    let Some(event_json) = db
        .query(&filters_json)
        .map_err(|err| anyhow::anyhow!("failed to query latest event: {err}"))?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };

    let created_at = serde_json::from_str::<serde_json::Value>(&event_json)
        .map_err(|err| anyhow::anyhow!("failed to parse latest event JSON: {err}"))?
        .get("created_at")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow::anyhow!("latest event JSON is missing created_at"))?;

    Ok(Some(created_at))
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

                    let res = (
                        StatusCode::OK,
                        [
                            ("Content-Type", "application/json"),
                            ("Access-Control-Allow-Origin", "*"),
                        ],
                        relay_info,
                    )
                        .into_response();

                    return Err(res);
                }
            }
            Err((StatusCode::OK, "Please use a Nostr client to connect.").into_response())
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
    /// Number of worker threads used by automatic compaction
    #[arg(long, env = "SEARCHNOS_COMPACT_WORKERS")]
    compact_workers: Option<NonZeroUsize>,
}

impl CommonArgs {
    pub fn open_db(&self) -> anyhow::Result<searchnos_db::SearchnosDB> {
        open_db_with_compact_workers(&self.db_path, self.compact_workers)
    }
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Display basic database statistics
    Stat,
    /// Compact the current hot event file into per-day partitions
    Compact,
    /// Rebuild partition search and visibility sidecars
    Reindex {
        /// Rebuild sidecars even when existing sidecar files are present
        #[arg(long)]
        force: bool,
    },
    /// Export all events as newline-delimited JSON sorted by newest first
    Export,
    /// Dump stored ndb notes as length-prefixed binary records
    Dump {
        /// Path to the output dump file
        output_path: PathBuf,
    },
    /// Load stored ndb notes from length-prefixed binary dumps
    Load {
        /// Paths to the input dump files
        #[arg(value_name = "INPUT_PATH", required = true, num_args = 1..)]
        input_paths: Vec<PathBuf>,
    },
    Serve(ServeArgs),
    Import(ImportArgs),
}

#[derive(ClapArgs, Debug)]
struct ServeArgs {
    /// Port to listen on
    #[arg(long, env, default_value_t = 3000)]
    port: u16,

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

    /// Comma-separated list of relays to reconcile with negentropy on SIGUSR2
    #[arg(
        long = "negentropy-relays",
        env = "NEGENTROPY_RELAYS",
        value_delimiter = ',',
        num_args = 0..
    )]
    negentropy_relays: Vec<String>,

    /// Number of recent UTC days to reconcile with negentropy
    #[arg(long = "negentropy-days", env = "NEGENTROPY_DAYS", default_value_t = 2)]
    negentropy_days: u64,

    /// Maximum number of subscriptions per client
    #[arg(long, env, default_value_t = 20)]
    max_subscriptions: usize,

    /// Maximum number of filters per subscription
    #[arg(long, env, default_value_t = 8)]
    max_filters: usize,

    /// Maximum number of recent UTC day partitions searched
    #[arg(long = "search-days", env = "SEARCH_DAYS")]
    search_days: Option<NonZeroU64>,

    /// Ping interval in seconds
    #[arg(long, env, default_value_t = 55)]
    ping_interval: u64,

    /// Maximum allowed age in seconds for the newest stored event
    #[arg(
        long = "health-max-event-age-seconds",
        env = "HEALTH_MAX_EVENT_AGE_SECONDS",
        default_value_t = 300
    )]
    health_max_event_age_seconds: u64,

    /// Use Forwarded header when logging client addresses
    #[arg(long = "respect-forwarded", env = "SEARCHNOS_RESPECT_FORWARDED")]
    respect_forwarded_headers: bool,

    /// Relay name returned in NIP-11 metadata
    #[arg(long = "relay-name", env = "RELAY_NAME", default_value = "searchnos")]
    relay_name: String,

    /// Relay description returned in NIP-11 metadata
    #[arg(
        long = "relay-description",
        env = "RELAY_DESCRIPTION",
        default_value = "searchnos relay"
    )]
    relay_description: String,
}

#[derive(ClapArgs, Debug)]
struct ImportArgs {
    /// Path to JSONL file containing events
    #[arg(value_name = "JSONL_PATH")]
    import_path: String,

    /// Comma-separated list of event kind numbers to import
    #[arg(
        long = "fetch-kinds",
        env = "FETCH_KINDS",
        value_delimiter = ',',
        num_args = 0..
    )]
    fetch_kinds: Vec<String>,
}

async fn app(common: &CommonArgs, args: &ServeArgs) -> anyhow::Result<Router> {
    let version = format!(
        "v{}-{}",
        env!("CARGO_PKG_VERSION"),
        env!("GIT_HASH").chars().take(7).collect::<String>()
    );
    let pkg_name = env!("CARGO_PKG_NAME").to_string();

    tracing::info!("{} {}", pkg_name, version);

    let src_relays = parse_src_relays(&args.src_relays)?;
    let negentropy_relays = negentropy_relays(args.negentropy_relays.clone());
    let mut fetch_kinds = parse_fetch_kinds(&args.fetch_kinds)?;
    if fetch_kinds.is_empty() && (!src_relays.is_empty() || !negentropy_relays.is_empty()) {
        fetch_kinds = DEFAULT_FETCH_KINDS.to_vec();
    }
    if !fetch_kinds.is_empty() {
        fetch_kinds.sort();
        fetch_kinds.dedup();
    }

    tracing::info!(
        path = %common.db_path,
        "opening searchnos-db"
    );

    let db = Arc::new(common.open_db()?);

    let mut relay_info = RelayInformationDocument::new();
    relay_info.name = Some(args.relay_name.clone());
    relay_info.description = Some(args.relay_description.clone());
    relay_info.supported_nips = Some(vec![1, 9, 11, 22, 28, 40, 50]);
    relay_info.software = Some(pkg_name);
    relay_info.version = Some(version);
    let relay_info = serde_json::to_string(&relay_info)?;

    let app_state = Arc::new(AppState {
        db,
        relay_info,
        max_subscriptions: args.max_subscriptions,
        max_filters: args.max_filters,
        search_days: args.search_days,
        ping_interval: Duration::from_secs(args.ping_interval),
        respect_forwarded_headers: args.respect_forwarded_headers,
        active_connections: AtomicUsize::new(0),
        health_max_event_age: Duration::from_secs(args.health_max_event_age_seconds),
    });

    if let Some(search_days) = args.search_days {
        tracing::info!(
            days = search_days.get(),
            "configured search partition limit"
        );
    }

    if !src_relays.is_empty() {
        let relay_list: Vec<String> = src_relays.iter().map(|url| url.to_string()).collect();
        let kind_list: Vec<u16> = fetch_kinds.iter().map(|kind| u16::from(*kind)).collect();
        tracing::info!(relays = ?relay_list, kinds = ?kind_list, "configured source fetching");
        let _fetch_handle = spawn_fetcher(app_state.clone(), src_relays, fetch_kinds.clone());
    }

    if !negentropy_relays.is_empty() {
        let kind_list: Vec<u16> = fetch_kinds.iter().map(|kind| u16::from(*kind)).collect();
        tracing::info!(
            relays = ?negentropy_relays,
            kinds = ?kind_list,
            days = args.negentropy_days,
            "configured negentropy reconcile"
        );
    }
    spawn_negentropy_signal_listener(
        app_state.db.clone(),
        negentropy_relays,
        args.negentropy_days,
        fetch_kinds,
    );

    let app = Router::new()
        .route("/healthz", get(health_check))
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
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let Cli { common, command } = Cli::parse();
    match command {
        Command::Stat => cmd::stat::run(common).await,
        Command::Compact => cmd::compact::run(common).await,
        Command::Reindex { force } => cmd::reindex::run(common, force).await,
        Command::Export => cmd::export::run(common).await,
        Command::Dump { output_path } => cmd::dump::run(common, output_path).await,
        Command::Load { input_paths } => cmd::load::run(common, input_paths).await,
        Command::Serve(args) => run_serve(common, args).await,
        Command::Import(args) => run_import(common, args).await,
    }
}

async fn run_serve(common: CommonArgs, args: ServeArgs) -> anyhow::Result<()> {
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

async fn run_import(common: CommonArgs, args: ImportArgs) -> anyhow::Result<()> {
    let summary = tokio::task::spawn_blocking(move || import_blocking(common, args)).await??;
    tracing::info!(
        inserted = summary.inserted,
        skipped_kind = summary.skipped_kind,
        "import completed"
    );
    Ok(())
}

fn import_blocking(common: CommonArgs, args: ImportArgs) -> Result<ImportSummary, anyhow::Error> {
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

    let db = common.open_db()?;

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

        insert_event_json(&db, &raw_line)
            .with_context(|| format!("failed to import event at line {}", idx + 1))?;
        inserted += 1;

        if let Some(pb) = progress_bar.as_ref() {
            pb.inc(1);
        }
    }

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
    .expect("default progress template must be valid")
    .with_key(
        "per_sec_ev",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = write!(w, "{:.2} ev/s", state.per_sec());
        },
    )
}

fn byte_progress_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{percent:>3}%|{bar:40}| {bytes}/{total_bytes} [{elapsed_precise}<{eta_precise}, {bytes_per_sec}]",
    )
    .expect("byte progress template must be valid")
    .with_key(
        "bytes",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = write!(w, "{}", HumanBytes(state.pos()));
        },
    )
    .with_key(
        "total_bytes",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = write!(w, "{}", HumanBytes(state.len().unwrap_or(0)));
        },
    )
    .with_key(
        "bytes_per_sec",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = write!(w, "{}/s", HumanBytes(state.per_sec() as u64));
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
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    use super::*;

    fn find_available_port() -> std::io::Result<u16> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let port = listener.local_addr()?.port();
        Ok(port)
    }

    fn default_serve_args(port: u16) -> ServeArgs {
        ServeArgs {
            port,
            src_relays: Vec::new(),
            fetch_kinds: Vec::new(),
            negentropy_relays: Vec::new(),
            negentropy_days: 2,
            max_subscriptions: 100,
            max_filters: 32,
            search_days: None,
            ping_interval: 55,
            health_max_event_age_seconds: 300,
            respect_forwarded_headers: false,
            relay_name: "searchnos".to_string(),
            relay_description: "searchnos relay".to_string(),
        }
    }

    fn read_http_response(port: u16, path: &str) -> std::io::Result<String> {
        let mut stream = loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => break stream,
                Err(err) if err.kind() == std::io::ErrorKind::ConnectionRefused => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Err(err) => return Err(err),
            }
        };
        write!(
            stream,
            "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
        )?;

        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        Ok(response)
    }

    #[test]
    fn health_check_reports_latest_event_age() {
        assert_eq!(
            health_check_response(None, 100, Duration::from_secs(60)),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "no events found".to_string()
            )
        );
        assert_eq!(
            health_check_response(Some(40), 100, Duration::from_secs(60)),
            (StatusCode::OK, "OK latest_event_age_seconds=60".to_string())
        );
        assert_eq!(
            health_check_response(Some(39), 100, Duration::from_secs(60)),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "latest event is stale: latest_event_age_seconds=61 max_age_seconds=60".to_string()
            )
        );
    }

    #[test]
    fn latest_event_created_at_ignores_future_events() {
        let temp_dir = std::env::temp_dir().join(format!(
            "searchnos-health-future-test-{}",
            rand::random::<u64>()
        ));
        let db_path = temp_dir.display().to_string();
        let db = searchnos::db_adapter::open_db(&db_path).unwrap();
        let keys = nostr_sdk::Keys::generate();
        let current_event = nostr_sdk::EventBuilder::text_note("current")
            .custom_created_at(nostr_sdk::Timestamp::from_secs(100))
            .sign_with_keys(&keys)
            .unwrap();
        let future_event = nostr_sdk::EventBuilder::text_note("future")
            .custom_created_at(nostr_sdk::Timestamp::from_secs(101))
            .sign_with_keys(&keys)
            .unwrap();

        insert_event_json(&db, &current_event.as_json()).unwrap();
        insert_event_json(&db, &future_event.as_json()).unwrap();

        assert_eq!(latest_event_created_at(&db, 100).unwrap(), Some(100));

        drop(db);
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn smoke_test() {
        init_tracing();

        let port = match find_available_port() {
            Ok(port) => port,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                println!("skipping smoke_test: {err}");
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
            compact_workers: None,
        };
        let args = default_serve_args(port);
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
    async fn health_check_endpoint_returns_ok() {
        init_tracing();

        let port = match find_available_port() {
            Ok(port) => port,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                println!("skipping health_check_endpoint_returns_ok: {err}");
                return;
            }
            Err(err) => panic!("failed to find available port: {err}"),
        };
        let db_path = std::env::temp_dir()
            .join(format!("searchnos-db-health-test-{}", port))
            .display()
            .to_string();
        let common = CommonArgs {
            db_path: db_path.clone(),
            compact_workers: None,
        };
        let keys = nostr_sdk::Keys::generate();
        let event = nostr_sdk::EventBuilder::text_note("fresh event")
            .custom_created_at(nostr_sdk::Timestamp::from_secs(current_unix_timestamp()))
            .sign_with_keys(&keys)
            .unwrap();
        {
            let db = common.open_db().unwrap();
            insert_event_json(&db, &event.as_json()).unwrap();
        }
        let args = default_serve_args(port);
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

        let response = tokio::task::spawn_blocking(move || read_http_response(port, "/healthz"))
            .await
            .unwrap()
            .unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("OK latest_event_age_seconds="));

        join_handle.abort();
    }

    #[tokio::test]
    async fn event_messages_are_rejected() {
        init_tracing();

        let port = match find_available_port() {
            Ok(port) => port,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                println!("skipping event_messages_are_rejected: {err}");
                return;
            }
            Err(err) => panic!("failed to find available port: {err}"),
        };
        let db_path = std::env::temp_dir()
            .join(format!("searchnos-db-event-test-{}", port))
            .display()
            .to_string();
        let common = CommonArgs {
            db_path: db_path.clone(),
            compact_workers: None,
        };
        let args = default_serve_args(port);
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

        let response = tokio::task::spawn_blocking(move || {
            let url = format!("ws://localhost:{port}");
            let (mut socket, _) = tungstenite::connect(url).unwrap();
            let keys = nostr_sdk::Keys::generate();
            let event = nostr_sdk::EventBuilder::text_note("hello")
                .sign_with_keys(&keys)
                .unwrap();
            socket
                .send(tungstenite::Message::Text(
                    format!("[\"EVENT\",{}]", event.as_json()).into(),
                ))
                .unwrap();
            socket.read().unwrap().to_string()
        })
        .await
        .unwrap();

        assert!(response.contains("\"OK\""));
        assert!(response.contains("false"));
        assert!(response.contains("blocked: writes disabled"));

        join_handle.abort();
    }

    #[tokio::test]
    async fn import_respects_fetch_kinds() {
        use std::fs::{self, File};

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
                compact_workers: None,
            },
            ImportArgs {
                import_path: import_path.display().to_string(),
                fetch_kinds: vec![Kind::TextNote.as_u16().to_string()],
            },
        )
        .unwrap();

        assert_eq!(summary.inserted, 1);
        assert_eq!(summary.skipped_kind, 1);

        fs::remove_dir_all(temp_dir).unwrap();
    }
}
