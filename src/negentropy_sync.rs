use std::collections::HashSet;
use std::error::Error;
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ndb::{NdbNote, NdbNoteBuf};
use negentropy::{Id, Negentropy, NegentropyStorageVector};
use searchnos_db::SearchnosDB;
use serde_json::Value;
use tracing::{info, warn};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

const NEGENTROPY_FRAME_SIZE_LIMIT: u64 = 60_000;
const FETCH_BATCH_SIZE: usize = 100;
const READ_TIMEOUT: Duration = Duration::from_millis(500);
static SUBSCRIPTION_COUNTER: AtomicU64 = AtomicU64::new(0);

type RelaySocket = WebSocket<MaybeTlsStream<TcpStream>>;
type LocalNegentropyItem = (u64, [u8; 32]);

pub fn reconcile_unix_days(
    db: Arc<SearchnosDB>,
    relays: &[String],
    unix_days: &[u64],
) -> Result<(), Box<dyn Error>> {
    for relay in relays {
        for unix_day in unix_days {
            match reconcile_unix_day(db.clone(), relay, *unix_day) {
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
) -> Result<ReconcileStats, Box<dyn Error>> {
    let since = unix_day
        .checked_mul(86_400)
        .ok_or("unix day is out of range")?;
    let until = since
        .checked_add(86_399)
        .ok_or("unix day is out of range")?;
    let local_items = local_negentropy_items(db.as_ref(), since, until)?;
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
        since,
        until,
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

    for chunk in need_ids.chunks(FETCH_BATCH_SIZE) {
        let events = fetch_events_by_ids(&mut socket, relay, chunk)?;
        for event_json in events {
            let note = match NdbNoteBuf::from_json(&event_json) {
                Ok(note) => note,
                Err(_) => {
                    stats.invalid += 1;
                    continue;
                }
            };
            let event = NdbNote::from_bytes(note.as_bytes())?;
            if event.created_at() < since || event.created_at() > until {
                stats.invalid += 1;
                continue;
            }
            let id = encode_hex(event.id());
            if seen_ids.insert(id) {
                db.insert_event_json(&event_json)?;
                stats.stored += 1;
            }
        }
    }

    Ok(stats)
}

fn local_negentropy_items(
    db: &SearchnosDB,
    since: u64,
    until: u64,
) -> Result<Vec<LocalNegentropyItem>, Box<dyn Error>> {
    let filters_json = serde_json::json!([{
        "since": since,
        "until": until
    }])
    .to_string();
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
    since: u64,
    until: u64,
    initial_message: Vec<u8>,
    negentropy: &mut Negentropy<'_, NegentropyStorageVector>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let subscription = format!("searchnos-negentropy-{unix_day}-{}", unique_suffix());
    send_neg_open(socket, &subscription, since, until, &initial_message)?;
    let mut need_ids = Vec::new();
    let mut seen_need_ids = HashSet::new();
    let mut round = 0_u64;

    loop {
        let text = read_text_message(socket)?;
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
    loop {
        let text = read_text_message(socket)?;
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
    since: u64,
    until: u64,
    message: &[u8],
) -> Result<(), Box<dyn Error>> {
    let request = Value::Array(vec![
        Value::String("NEG-OPEN".to_owned()),
        Value::String(subscription.to_owned()),
        day_filter(since, until),
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

fn day_filter(since: u64, until: u64) -> Value {
    let mut filter = serde_json::Map::new();
    filter.insert("since".to_owned(), Value::Number(since.into()));
    filter.insert("until".to_owned(), Value::Number(until.into()));
    Value::Object(filter)
}

fn ids_filter(ids: &[String]) -> Value {
    let ids = ids.iter().cloned().map(Value::String).collect();
    let mut filter = serde_json::Map::new();
    filter.insert("ids".to_owned(), Value::Array(ids));
    Value::Object(filter)
}

fn read_text_message(socket: &mut RelaySocket) -> Result<String, Box<dyn Error>> {
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
                ) => {}
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
