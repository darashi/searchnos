use anyhow::{anyhow, Context};
use nostr_sdk::{Kind, RelayUrl};
use std::str::FromStr;
use std::time::Duration;

pub const DEFAULT_FETCH_KINDS: [Kind; 9] = [
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

pub struct DbRuntimeConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
}

pub fn parse_src_relays(values: &[String]) -> anyhow::Result<Vec<RelayUrl>> {
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

pub fn parse_fetch_kinds(values: &[String]) -> anyhow::Result<Vec<Kind>> {
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

pub fn validate_db_runtime_config(
    batch_size: usize,
    flush_interval_ms: u64,
) -> anyhow::Result<DbRuntimeConfig> {
    if batch_size == 0 {
        return Err(anyhow!("db batch size must be greater than zero"));
    }

    if flush_interval_ms == 0 {
        return Err(anyhow!("db flush interval must be greater than zero"));
    }

    Ok(DbRuntimeConfig {
        batch_size,
        flush_interval: Duration::from_millis(flush_interval_ms),
    })
}
