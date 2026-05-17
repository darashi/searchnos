use anyhow::Context;
use ndb::{NdbNote, NdbNoteBuf};
use searchnos_db::{InsertOptions, SearchnosDB, SearchnosDBOptions};
use std::num::NonZeroUsize;
use std::path::Path;

pub fn open_db(db_path: &str) -> anyhow::Result<SearchnosDB> {
    open_db_with_compact_workers(db_path, None)
}

pub fn open_db_with_compact_workers(
    db_path: &str,
    compact_workers: Option<NonZeroUsize>,
) -> anyhow::Result<SearchnosDB> {
    open_db_with_options(
        db_path,
        SearchnosDBOptions {
            compact_workers,
            ..SearchnosDBOptions::default()
        },
    )
}

pub fn open_db_with_hot_max_bytes(
    db_path: &str,
    hot_max_bytes: u64,
) -> anyhow::Result<SearchnosDB> {
    open_db_with_options(
        db_path,
        SearchnosDBOptions {
            hot_max_bytes,
            ..SearchnosDBOptions::default()
        },
    )
}

fn open_db_with_options(db_path: &str, options: SearchnosDBOptions) -> anyhow::Result<SearchnosDB> {
    let root = Path::new(db_path);
    std::fs::create_dir_all(root)?;
    SearchnosDB::open_with_options(root, options)
        .map_err(|err| anyhow::anyhow!("failed to open searchnos-db: {err}"))
}

pub fn event_json_to_packet(event_json: &str) -> anyhow::Result<Vec<u8>> {
    Ok(NdbNoteBuf::from_json(event_json)
        .context("failed to encode event as ndb note")?
        .into_bytes())
}

pub fn packet_to_event_json(packet: &[u8]) -> anyhow::Result<String> {
    NdbNote::from_bytes(packet)
        .context("failed to decode ndb note")?
        .to_json_string()
        .context("failed to convert ndb note to JSON")
}

pub fn packet_event_id(packet: &[u8]) -> anyhow::Result<[u8; 32]> {
    Ok(*NdbNote::from_bytes(packet)
        .context("failed to decode ndb note")?
        .id())
}

pub fn insert_event_json(db: &SearchnosDB, event_json: &str) -> anyhow::Result<()> {
    db.insert_event_json(event_json, InsertOptions::default())
        .map_err(|err| anyhow::anyhow!("failed to insert event: {err}"))
}
