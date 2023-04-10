use anyhow::Context;
use chrono::Utc;
use elasticsearch::{DeleteByQueryParts, Elasticsearch, IndexParts};
use log::{error, info, warn};
use nostr_sdk::prelude::*;
use nostr_sdk::Event;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::app_state::AppState;
use crate::indexer::indexes::{can_exist, index_name_for_event};
use crate::indexer::text::extract_text;

#[derive(Debug, Serialize)]
struct Document {
    event: Event,
    text: String,
    tags: HashMap<String, HashSet<String>>,
    identifier_tag: String,
}

fn convert_tags(tags: &Vec<nostr_sdk::Tag>) -> HashMap<String, HashSet<String>> {
    let mut tag: HashMap<String, HashSet<String>> = HashMap::new();

    for t in tags {
        let t = t.as_vec();
        let mut it = t.iter();
        let tag_kind = it.next();
        let first_tag_value = it.next();
        if let (Some(tag_kind), Some(first_tag_value)) = (tag_kind, first_tag_value) {
            if tag_kind.len() != 1 {
                continue; // index only 1-char tags; See NIP-12
            }

            if let Some(values) = tag.get_mut(tag_kind) {
                values.insert(first_tag_value.clone());
            } else {
                let mut hs = HashSet::new();
                hs.insert(first_tag_value.clone());
                tag.insert(tag_kind.to_string(), hs);
            }
        }
    }

    tag
}

fn is_replaceable_event(event: &Event) -> bool {
    match event.kind {
        Kind::Replaceable(_) => true,
        Kind::Metadata | Kind::ContactList | Kind::ChannelMetadata => true,
        _ => false,
    }
}

fn is_ephemeral_event(event: &Event) -> bool {
    match event.kind {
        Kind::Ephemeral(_) => true,
        _ => false,
    }
}

fn is_parameterized_replaceable_event(event: &Event) -> bool {
    match event.kind {
        Kind::ParameterizedReplaceable(_) => true,
        _ => false,
    }
}

async fn delete_replaceable_event(
    es_client: &Elasticsearch,
    alias_name: &str,
    event: &Event,
) -> anyhow::Result<()> {
    let res = es_client
        .delete_by_query(DeleteByQueryParts::Index(&[alias_name]))
        .body(json!({
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "event.pubkey": event.pubkey.to_string()
                            }
                        },
                        {
                            "term": {
                                "event.kind": event.kind
                            }
                        },
                        {
                            "range": {
                                "event.created_at": {
                                    "lt": event.created_at.to_string()
                                }
                            }
                        }
                    ]
                }
            }
        }))
        .send()
        .await?;
    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        return Err(anyhow::anyhow!("failed to delete; received {}, {}", status_code, body).into());
    }
    let response_body = res.json::<serde_json::Value>().await?;
    info!(
        "replaceable event (kind {}): deleted {} event(s) of for pubkey {}",
        event.kind.as_u32(),
        response_body["deleted"],
        event.pubkey,
    );
    Ok(())
}

fn extract_identifier_tag(tags: &Vec<Tag>) -> String {
    tags.iter()
        .find_map(|tag| {
            if let Tag::Identifier(tag) = tag {
                Some(tag.to_string())
            } else {
                None
            }
        })
        .unwrap_or_default()
}

async fn delete_parameterized_replaceable_event(
    es_client: &Elasticsearch,
    alias_name: &str,
    event: &Event,
) -> anyhow::Result<()> {
    let identifier_tag = extract_identifier_tag(&event.tags);
    let res = es_client
        .delete_by_query(DeleteByQueryParts::Index(&[alias_name]))
        .body(json!({
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "event.pubkey": event.pubkey.to_string()
                            }
                        },
                        {
                            "term": {
                                "event.kind": event.kind
                            }
                        },
                        {
                            "range": {
                                "event.created_at": {
                                    "lt": event.created_at.to_string()
                                }
                            }
                        },
                        {
                            "term": {
                                "identifier_tag": identifier_tag
                            }
                        }
                    ]
                }
            }
        }))
        .send()
        .await?;

    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        return Err(anyhow::anyhow!("failed to delete; received {}, {}", status_code, body).into());
    }
    let response_body = res.json::<serde_json::Value>().await?;
    info!(
        "parameterized replaceable event (kind {}): deleted {} event(s) of for pubkey {}, identifier_tag {}",
        event.kind.as_u32(),
        response_body["deleted"],
        event.pubkey,
        identifier_tag,
    );
    Ok(())
}

pub async fn handle_update(
    es_client: &Elasticsearch,
    index_prefix: &str,
    alias_name: &str,
    event: &Event,
) -> anyhow::Result<()> {
    let index_name = index_name_for_event(index_prefix, event)?;
    info!("{} {}", index_name, event.as_json());

    if is_ephemeral_event(event) {
        return Ok(());
    }

    // TODO parameterize ttl
    let ok = can_exist(&index_name, &Utc::now(), 7, 1).unwrap_or(false);
    if !ok {
        warn!("index {} is out of range; skipping", index_name);
        return Ok(());
    }

    let id = event.id.to_hex();

    let doc = Document {
        event: event.clone(),
        text: extract_text(&event),
        tags: convert_tags(&event.tags),
        identifier_tag: extract_identifier_tag(&event.tags),
    };
    let res = es_client
        .index(IndexParts::IndexId(index_name.as_str(), &id))
        .body(doc)
        .send()
        .await?;
    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        error!("failed to index; received {}, {}", status_code, body);
    }

    if is_replaceable_event(event) {
        delete_replaceable_event(es_client, alias_name, event).await?;
    }
    if is_parameterized_replaceable_event(event) {
        delete_parameterized_replaceable_event(es_client, alias_name, event).await?;
    }
    if let Kind::EventDeletion = event.kind {
        handle_deletion_event(es_client, alias_name, event).await?;
    }

    Ok(())
}

async fn handle_deletion_event(
    es_client: &Elasticsearch,
    index_name: &str,
    event: &Event,
) -> anyhow::Result<()> {
    let deletion_event = event;
    log::info!("deletion event: {}", deletion_event.as_json());
    let ids_to_delete = deletion_event
        .tags
        .iter()
        .filter_map(|tag| match tag {
            Tag::Event(e, _, _) => Some(e.to_hex()),
            _ => None,
        })
        .collect::<Vec<String>>();
    log::info!("ids to delete: {:?}", ids_to_delete);

    let res = es_client
        .delete_by_query(DeleteByQueryParts::Index(&[index_name]))
        .body(json!({
            "query": {
                "bool": {
                    "must": [
                        {
                            "terms": {
                                "_id": ids_to_delete
                            },
                        },
                        {
                            "term": {
                                "event.pubkey": deletion_event.pubkey.to_string()
                            },
                        }
                    ]
                }
            }
        }))
        .send()
        .await?;

    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        error!("failed to delete; received {}, {}", status_code, body);
        return Err(anyhow::anyhow!("failed to delete"));
    }

    let response_body = res.json::<serde_json::Value>().await?;
    info!(
        "delete event: deleted {} event(s) of for pubkey {}",
        response_body["deleted"], event.pubkey,
    );

    Ok(())
}

pub async fn handle_event(
    state: Arc<AppState>,
    addr: SocketAddr,
    msg: &Vec<serde_json::Value>,
) -> anyhow::Result<()> {
    if msg.len() != 2 {
        return Err(anyhow::anyhow!("invalid array length"));
    }

    let event =
        serde_json::from_value::<Event>(msg[1].clone()).context("parsing subscription id")?;
    event.verify().context("failed to verify event")?;

    let index_template_name = "nostr"; // TODO parameterize
    let alias_name = "nostr"; // TODO parameterize

    handle_update(&state.es_client, &alias_name, &index_template_name, &event).await?;
    log::info!("{} EVENT {:?}", addr, event);

    Ok(())
}

#[cfg(test)]
mod tests {
    use nostr_sdk::Tag;

    use crate::indexer::handlers::extract_identifier_tag;

    #[test]
    fn test_identifier_tag() {
        assert_eq!(
            extract_identifier_tag(&vec![Tag::Identifier("hello".to_string())]),
            "hello".to_string()
        );

        assert_eq!(
            extract_identifier_tag(&vec![
                Tag::Identifier("foo".to_string()),
                Tag::Identifier("bar".to_string())
            ]),
            "foo".to_string()
        );

        assert_eq!(extract_identifier_tag(&vec![]), "".to_string());
        assert_eq!(
            extract_identifier_tag(&vec![Tag::Identifier("".to_string())]),
            "".to_string()
        );
        assert_eq!(
            extract_identifier_tag(&vec![Tag::Hashtag("hello".to_string())]),
            "".to_string()
        );
    }
}
