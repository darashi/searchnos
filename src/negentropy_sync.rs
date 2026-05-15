use std::collections::HashSet;
use std::error::Error;
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ndb::{NdbNote, NdbNoteBuf};
use negentropy::{Id, Negentropy, NegentropyStorageVector};
use nostr_sdk::Kind;
use searchnos_db::{InsertOptions, SearchnosDB};
use serde_json::Value;
use tracing::{info, warn};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

const NEGENTROPY_FRAME_SIZE_LIMIT: u64 = 60_000;
const FETCH_BATCH_SIZE: usize = 100;
const READ_TIMEOUT: Duration = Duration::from_millis(500);
const RELAY_RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
const FETCH_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
static SUBSCRIPTION_COUNTER: AtomicU64 = AtomicU64::new(0);

type RelaySocket = WebSocket<MaybeTlsStream<TcpStream>>;
type LocalNegentropyItem = (u64, [u8; 32]);

#[derive(Clone, Copy)]
struct UnixDayRange {
    since: u64,
    until: u64,
}

pub fn reconcile_unix_days(
    db: Arc<SearchnosDB>,
    relays: &[String],
    unix_days: &[u64],
    kinds: &[Kind],
) -> Result<(), Box<dyn Error>> {
    for relay in relays {
        for unix_day in unix_days {
            match reconcile_unix_day(db.clone(), relay, *unix_day, kinds) {
                Ok(stats) => {
                    info!(
                        %relay,
                        unix_day,
                        local = stats.local,
                        missing = stats.missing,
                        stored = stats.stored,
                        invalid = stats.invalid,
                        "reconciled day with negentropy"
                    );
                }
                Err(err) => {
                    warn!(%relay, unix_day, %err, "failed to reconcile day with negentropy");
                }
            }
        }
    }
    Ok(())
}

#[derive(Default)]
struct ReconcileStats {
    local: usize,
    missing: usize,
    stored: usize,
    invalid: usize,
}

fn reconcile_unix_day(
    db: Arc<SearchnosDB>,
    relay: &str,
    unix_day: u64,
    kinds: &[Kind],
) -> Result<ReconcileStats, Box<dyn Error>> {
    let since = unix_day
        .checked_mul(86_400)
        .ok_or("unix day is out of range")?;
    let until = since
        .checked_add(86_399)
        .ok_or("unix day is out of range")?;
    let range = UnixDayRange { since, until };
    let local_items = local_negentropy_items(db.as_ref(), range, kinds)?;
    let mut negentropy_storage = NegentropyStorageVector::with_capacity(local_items.len());
    for (created_at, id) in &local_items {
        negentropy_storage.insert(*created_at, Id::from_byte_array(*id))?;
    }
    negentropy_storage.seal()?;

    let mut negentropy = Negentropy::borrowed(&negentropy_storage, NEGENTROPY_FRAME_SIZE_LIMIT)?;
    let initial_message = negentropy.initiate()?;
    let mut socket = connect_socket(relay)?;
    let need_ids = reconcile_missing_ids(
        &mut socket,
        relay,
        unix_day,
        range,
        kinds,
        initial_message,
        &mut negentropy,
    )?;
    let missing = need_ids.len();
    let mut seen_ids: HashSet<String> = local_items.iter().map(|(_, id)| encode_hex(id)).collect();
    let mut stats = ReconcileStats {
        local: seen_ids.len(),
        missing,
        ..ReconcileStats::default()
    };

    if !need_ids.is_empty() {
        info!(
            %relay,
            unix_day,
            missing_total = need_ids.len(),
            batch_size = FETCH_BATCH_SIZE,
            batches = need_ids.len().div_ceil(FETCH_BATCH_SIZE),
            "fetching missing events after negentropy reconcile"
        );
    }

    let total_batches = need_ids.len().div_ceil(FETCH_BATCH_SIZE);
    let mut last_progress_log = Instant::now();
    for (batch_index, chunk) in need_ids.chunks(FETCH_BATCH_SIZE).enumerate() {
        let events = fetch_events_by_ids(&mut socket, relay, chunk)?;
        let fetched = events.len();
        for event_json in events {
            let note = match NdbNoteBuf::from_json(&event_json) {
                Ok(note) => note,
                Err(_) => {
                    stats.invalid += 1;
                    continue;
                }
            };
            let event = NdbNote::from_bytes(note.as_bytes())?;
            if event.created_at() < range.since || event.created_at() > range.until {
                stats.invalid += 1;
                continue;
            }
            if !is_allowed_kind(event.kind(), kinds) {
                stats.invalid += 1;
                continue;
            }
            let id = encode_hex(event.id());
            if seen_ids.insert(id) {
                db.insert_event_json(
                    &event_json,
                    InsertOptions {
                        notify_subscribers: false,
                    },
                )?;
                stats.stored += 1;
            }
        }
        let is_last_batch = batch_index + 1 == total_batches;
        if is_last_batch || last_progress_log.elapsed() >= FETCH_PROGRESS_INTERVAL {
            info!(
                %relay,
                unix_day,
                batch = batch_index + 1,
                batches = total_batches,
                requested = chunk.len(),
                fetched,
                stored_total = stats.stored,
                invalid_total = stats.invalid,
                "fetching missing events after negentropy reconcile"
            );
            last_progress_log = Instant::now();
        }
    }

    Ok(stats)
}

fn local_negentropy_items(
    db: &SearchnosDB,
    range: UnixDayRange,
    kinds: &[Kind],
) -> Result<Vec<LocalNegentropyItem>, Box<dyn Error>> {
    let filters_json = Value::Array(vec![event_filter(range, kinds)]).to_string();
    let mut items = Vec::new();
    db.stream_query(&filters_json, |event_json| {
        let Ok(note) = NdbNoteBuf::from_json(&event_json) else {
            return true;
        };
        let Ok(event) = NdbNote::from_bytes(note.as_bytes()) else {
            return true;
        };
        items.push((event.created_at(), *event.id()));
        true
    })?;
    items.sort_unstable();
    items.dedup_by_key(|(_, id)| *id);
    Ok(items)
}

fn reconcile_missing_ids(
    socket: &mut RelaySocket,
    relay: &str,
    unix_day: u64,
    range: UnixDayRange,
    kinds: &[Kind],
    initial_message: Vec<u8>,
    negentropy: &mut Negentropy<'_, NegentropyStorageVector>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let subscription = format!("searchnos-negentropy-{unix_day}-{}", unique_suffix());
    send_neg_open(socket, &subscription, range, kinds, &initial_message)?;
    let mut need_ids = Vec::new();
    let mut seen_need_ids = HashSet::new();
    let mut round = 0_u64;
    let mut response_deadline = Instant::now() + RELAY_RESPONSE_TIMEOUT;

    loop {
        let text = read_text_message(socket, response_deadline, "negentropy response")?;
        let Some(message) = parse_relay_message(&text)? else {
            continue;
        };
        let Some(kind) = message.first().and_then(Value::as_str) else {
            continue;
        };

        match kind {
            "NEG-MSG" if message.get(1).and_then(Value::as_str) == Some(subscription.as_str()) => {
                round += 1;
                let hex_message = message
                    .get(2)
                    .and_then(Value::as_str)
                    .ok_or("NEG-MSG is missing message")?;
                let query = decode_hex(hex_message)?;
                let mut have = Vec::new();
                let mut need = Vec::new();
                let response = negentropy.reconcile_with_ids(&query, &mut have, &mut need)?;
                let round_need = need.len();
                for id in need {
                    let id = encode_hex(id.as_bytes());
                    if seen_need_ids.insert(id.clone()) {
                        need_ids.push(id);
                    }
                }
                info!(
                    %relay,
                    unix_day,
                    round,
                    round_need,
                    missing_total = need_ids.len(),
                    "negentropy round"
                );
                if let Some(response) = response {
                    send_neg_msg(socket, &subscription, &response)?;
                    response_deadline = Instant::now() + RELAY_RESPONSE_TIMEOUT;
                } else {
                    send_neg_close(socket, &subscription)?;
                    return Ok(need_ids);
                }
            }
            "NEG-ERR" if message.get(1).and_then(Value::as_str) == Some(subscription.as_str()) => {
                let reason = message.get(2).and_then(Value::as_str).unwrap_or("");
                return Err(format!("relay returned NEG-ERR: {reason}").into());
            }
            "CLOSED" if message.get(1).and_then(Value::as_str) == Some(subscription.as_str()) => {
                let reason = message.get(2).and_then(Value::as_str).unwrap_or("");
                return Err(format!("negentropy subscription closed: {reason}").into());
            }
            _ => {}
        }
    }
}

fn fetch_events_by_ids(
    socket: &mut RelaySocket,
    relay: &str,
    ids: &[String],
) -> Result<Vec<String>, Box<dyn Error>> {
    let subscription = format!("searchnos-fetch-{}", unique_suffix());
    let request = Value::Array(vec![
        Value::String("REQ".to_owned()),
        Value::String(subscription.clone()),
        ids_filter(ids),
    ]);
    socket.send(Message::Text(request.to_string().into()))?;

    let mut events = Vec::new();
    let response_deadline = Instant::now() + RELAY_RESPONSE_TIMEOUT;
    loop {
        let text = read_text_message(socket, response_deadline, "fetch EOSE response")?;
        let Some(message) = parse_relay_message(&text)? else {
            continue;
        };
        let Some(kind) = message.first().and_then(Value::as_str) else {
            continue;
        };

        match kind {
            "EVENT" if message.get(1).and_then(Value::as_str) == Some(subscription.as_str()) => {
                if let Some(event) = message.get(2) {
                    events.push(event.to_string());
                }
            }
            "EOSE" if message.get(1).and_then(Value::as_str) == Some(subscription.as_str()) => {
                send_close(socket, &subscription)?;
                return Ok(events);
            }
            "CLOSED" if message.get(1).and_then(Value::as_str) == Some(subscription.as_str()) => {
                let reason = message.get(2).and_then(Value::as_str).unwrap_or("");
                return Err(format!("relay={relay} fetch subscription closed: {reason}").into());
            }
            _ => {}
        }
    }
}

fn connect_socket(relay: &str) -> Result<RelaySocket, Box<dyn Error>> {
    let (mut socket, _) = connect(relay)?;
    set_read_timeout(&mut socket)?;
    Ok(socket)
}

fn send_neg_open(
    socket: &mut RelaySocket,
    subscription: &str,
    range: UnixDayRange,
    kinds: &[Kind],
    message: &[u8],
) -> Result<(), Box<dyn Error>> {
    let request = Value::Array(vec![
        Value::String("NEG-OPEN".to_owned()),
        Value::String(subscription.to_owned()),
        event_filter(range, kinds),
        Value::String(encode_hex(message)),
    ]);
    socket.send(Message::Text(request.to_string().into()))?;
    Ok(())
}

fn send_neg_msg(
    socket: &mut RelaySocket,
    subscription: &str,
    message: &[u8],
) -> Result<(), Box<dyn Error>> {
    let request = Value::Array(vec![
        Value::String("NEG-MSG".to_owned()),
        Value::String(subscription.to_owned()),
        Value::String(encode_hex(message)),
    ]);
    socket.send(Message::Text(request.to_string().into()))?;
    Ok(())
}

fn send_neg_close(socket: &mut RelaySocket, subscription: &str) -> Result<(), Box<dyn Error>> {
    let request = Value::Array(vec![
        Value::String("NEG-CLOSE".to_owned()),
        Value::String(subscription.to_owned()),
    ]);
    socket.send(Message::Text(request.to_string().into()))?;
    Ok(())
}

fn send_close(socket: &mut RelaySocket, subscription: &str) -> Result<(), Box<dyn Error>> {
    let request = Value::Array(vec![
        Value::String("CLOSE".to_owned()),
        Value::String(subscription.to_owned()),
    ]);
    socket.send(Message::Text(request.to_string().into()))?;
    Ok(())
}

fn event_filter(range: UnixDayRange, kinds: &[Kind]) -> Value {
    let mut filter = serde_json::Map::new();
    filter.insert("since".to_owned(), Value::Number(range.since.into()));
    filter.insert("until".to_owned(), Value::Number(range.until.into()));
    if !kinds.is_empty() {
        filter.insert("kinds".to_owned(), kind_values(kinds));
    }
    Value::Object(filter)
}

fn ids_filter(ids: &[String]) -> Value {
    let ids = ids.iter().cloned().map(Value::String).collect();
    let mut filter = serde_json::Map::new();
    filter.insert("ids".to_owned(), Value::Array(ids));
    Value::Object(filter)
}

fn kind_values(kinds: &[Kind]) -> Value {
    Value::Array(
        kinds
            .iter()
            .map(|kind| Value::Number(u16::from(*kind).into()))
            .collect(),
    )
}

fn is_allowed_kind(kind: u32, kinds: &[Kind]) -> bool {
    if kinds.is_empty() {
        return true;
    }
    kinds
        .iter()
        .any(|allowed_kind| u32::from(u16::from(*allowed_kind)) == kind)
}

fn read_text_message(
    socket: &mut RelaySocket,
    deadline: Instant,
    context: &str,
) -> Result<String, Box<dyn Error>> {
    loop {
        match socket.read() {
            Ok(Message::Text(text)) => return Ok(text.to_string()),
            Ok(Message::Ping(payload)) => socket.send(Message::Pong(payload))?,
            Ok(Message::Close(frame)) => {
                return Err(format!("relay closed connection: {frame:?}").into());
            }
            Ok(_) => {}
            Err(tungstenite::Error::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                if Instant::now() >= deadline {
                    return Err(format!(
                        "{context} timed out after {}s",
                        RELAY_RESPONSE_TIMEOUT.as_secs()
                    )
                    .into());
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn parse_relay_message(text: &str) -> Result<Option<Vec<Value>>, Box<dyn Error>> {
    let value: Value = serde_json::from_str(text)?;
    Ok(value.as_array().cloned())
}

fn set_read_timeout(socket: &mut RelaySocket) -> Result<(), Box<dyn Error>> {
    match socket.get_mut() {
        MaybeTlsStream::Plain(stream) => stream.set_read_timeout(Some(READ_TIMEOUT))?,
        MaybeTlsStream::Rustls(stream) => stream.sock.set_read_timeout(Some(READ_TIMEOUT))?,
        _ => {}
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(value: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    if !value.len().is_multiple_of(2) {
        return Err("hex value has odd length".into());
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for index in (0..value.len()).step_by(2) {
        bytes.push(u8::from_str_radix(&value[index..index + 2], 16)?);
    }
    Ok(bytes)
}

fn unique_suffix() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    let counter = SUBSCRIPTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{millis}-{counter}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_filter_includes_kinds_when_configured() {
        let filter = event_filter(
            UnixDayRange {
                since: 100,
                until: 199,
            },
            &[Kind::TextNote, Kind::LongFormTextNote],
        );

        assert_eq!(
            filter,
            serde_json::json!({
                "since": 100,
                "until": 199,
                "kinds": [1, 30023]
            })
        );
    }

    #[test]
    fn event_filter_omits_empty_kinds() {
        let filter = event_filter(
            UnixDayRange {
                since: 100,
                until: 199,
            },
            &[],
        );

        assert_eq!(
            filter,
            serde_json::json!({
                "since": 100,
                "until": 199
            })
        );
    }

    #[test]
    fn allowed_kind_matches_configured_kinds() {
        assert!(is_allowed_kind(1, &[Kind::TextNote]));
        assert!(!is_allowed_kind(0, &[Kind::TextNote]));
        assert!(is_allowed_kind(0, &[]));
    }
}
