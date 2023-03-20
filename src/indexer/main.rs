use chrono::TimeZone;
use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::IndicesPutIndexTemplateParts,
    DeleteParts, Elasticsearch, IndexParts, SearchParts,
};
use env_logger;
use lingua::LanguageDetector;
use lingua::LanguageDetectorBuilder;
use log::{error, info};
use nostr_sdk::prelude::*;
use serde::Serialize;
use std::env;

async fn create_index_template(
    es_client: &Elasticsearch,
    template_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("putting index template: {}", template_name);
    let res = es_client
        .indices()
        .put_index_template(IndicesPutIndexTemplateParts::Name(template_name))
        .body(json!({
            "index_patterns": ["nostr-*"],
            "template": {
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                        "analysis": {
                        "analyzer": {
                            "ngram_analyzer": {
                            "type": "custom",
                            "tokenizer": "ngram_tokenizer",
                            "filter": ["icu_normalizer", "lowercase"],
                            },
                        },
                        "tokenizer": {
                            "ngram_tokenizer": {
                            "type": "ngram",
                            "min_gram": "1",
                            "max_gram": "2",
                            },
                        },
                        },
                    },
                },
                "mappings": {
                    "dynamic": false,
                    "properties": {
                        "event": {
                            "dynamic": false,
                            "properties": {
                                "content": {
                                    "type": "text",
                                    "index": false
                                },
                                "created_at": {
                                    "type": "date",
                                    "format": "epoch_second"
                                },
                                "kind": {
                                    "type": "integer"
                                },
                                "id": {
                                    "type": "keyword"
                                },
                                "pubkey": {
                                    "type": "keyword"
                                },
                                "sig": {
                                    "type": "keyword",
                                    "index": false
                                },
                                "tags": {
                                    "type": "keyword"
                                },
                            }
                        },
                        "text": {
                            "type": "text",
                            "analyzer": "ngram_analyzer",
                            "index": "true",
                        },
                        "language": {
                            "type": "keyword"
                        }
                    }
                },
                "aliases": {
                    "nostr": {}
                }
            }
        }))
        .send()
        .await?;

    if !res.status_code().is_success() {
        let status = res.status_code();
        let body = res.text().await?;
        return Err(format!("failed to create index: received {}, {}", status, body).into());
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct Document {
    event: Event,
    text: String,
    language: String,
}

fn index_name_for_event(prefix: &str, event: &Event) -> Result<String, Box<dyn std::error::Error>> {
    let dt = chrono::Utc.timestamp_opt(event.created_at.as_i64(), 0);
    if let Some(dt) = dt.single() {
        Ok(format!("{}-{}", prefix, dt.format("%Y.%m.%d").to_string()))
    } else {
        Err(format!("failed to parse date: {}", event.created_at).into())
    }
}

async fn handle_text_note(
    es_client: &Elasticsearch,
    language_detector: &LanguageDetector,
    index_prefix: &str,
    event: &Event,
) -> Result<(), Box<dyn std::error::Error>> {
    let language = language_detector.detect_language_of(&event.content);
    let language = match language {
        Some(l) => l.iso_code_639_1().to_string(),
        None => "unknown".to_string(),
    };

    let index_name = index_name_for_event(index_prefix, event)?;
    println!("{} {} {}", index_name, language, event.as_json());
    let id = event.id.to_hex();
    let doc = Document {
        event: event.clone(),
        text: event.content.clone(),
        language,
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
    Ok(())
}

async fn delete_event(
    es_client: &Elasticsearch,
    alias_name: &str,
    id: &str,
    pubkey: &XOnlyPublicKey,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("try to delete: id={}", id);

    let res = es_client
        .search(SearchParts::Index(&[alias_name]))
        .body(json!({
            "query": {
                "term": {
                    "_id": id
                }
            }
        }))
        .size(1)
        .send()
        .await?;

    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        return Err(format!("failed to fetch; received {}, {}", status_code, body).into());
    }
    let response_body = res.json::<serde_json::Value>().await?;
    let hits_array = match response_body["hits"]["hits"].as_array() {
        Some(hits) => hits,
        None => return Err("Failed to retrieve hits from response".into()),
    };
    let hit = match hits_array.first() {
        Some(hit) => hit,
        None => return Err(format!("Event with ID {} not found in search results", id).into()),
    };

    let event_to_be_deleted = serde_json::from_value::<Event>(hit["_source"]["event"].clone())?;

    let ok_to_delete = *pubkey == event_to_be_deleted.pubkey;
    info!(
        "can event {} be deleted? {}",
        event_to_be_deleted.id, ok_to_delete
    );
    if !ok_to_delete {
        return Err(format!(
            "pubkey mismatch: pub key was {}, but that of the event {} was {}",
            pubkey, id, event_to_be_deleted.pubkey
        )
        .into());
    }

    let index_name = hit["_index"].as_str();
    let index_name = match index_name {
        Some(index_name) => index_name,
        None => return Err("failed to get index name".into()),
    };

    let res = es_client
        .delete(DeleteParts::IndexId(&index_name, &id))
        .send()
        .await?;

    if !res.status_code().is_success() {
        let status_code = res.status_code();
        let body = res.text().await?;
        error!("failed to delete; received {}, {}", status_code, body);
        return Err("failed to delete".into());
    }
    info!("deleted: id={}", id);
    Ok(())
}

async fn handle_deletion_event(
    es_client: &Elasticsearch,
    index_name: &str,
    event: &Event,
) -> Result<(), Box<dyn std::error::Error>> {
    let deletion_event = event;
    println!("deletion event: {}", deletion_event.as_json());
    for tag in &deletion_event.tags {
        if let Tag::Event(e, _, _) = tag {
            let id = e.to_hex();
            let result = delete_event(es_client, index_name, &id, &deletion_event.pubkey).await;
            if result.is_err() {
                error!("failed to delete event; {}", result.err().unwrap());
                continue;
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env::set_var("RUST_LOG", "info");
    env_logger::init();

    // env vars
    let es_url = env::var("ES_URL").expect("ES_URL is not set; set it to the URL of elasticsearch");
    let relays = env::var("NOSTR_RELAYS")
        .expect("NOSTR_RELAYS is not set; set it to the comma-separated URLs of relays");

    // prepare elasticsearch client
    let es_url = Url::parse(&es_url).expect("invalid elasticsearch url");
    let conn_pool = SingleNodeConnectionPool::new(es_url);
    let es_transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let es_client = Elasticsearch::new(es_transport);
    let index_template_name = "nostr";
    create_index_template(&es_client, index_template_name).await?;
    info!("elasticsearch index ready");

    // prepare lingua
    info!("loading language models");
    let language_detector = LanguageDetectorBuilder::from_all_languages()
        .with_preloaded_language_models()
        .build();
    info!("language models loaded");

    // prepare nostr client
    let my_keys: Keys = Keys::generate();
    let nostr_client = Client::new(&my_keys);

    for relay in relays.split(',') {
        info!("adding relay: {}", relay);
        nostr_client.add_relay(relay, None).await?;
    }
    nostr_client.connect().await;
    info!("connected to relays");

    let subscription = Filter::new()
        .limit(0)
        .kinds(vec![Kind::TextNote, Kind::EventDeletion]);
    nostr_client.subscribe(vec![subscription]).await;
    info!("ready to receive messages");

    loop {
        let mut notifications = nostr_client.notifications();
        while let Ok(notification) = notifications.recv().await {
            if let RelayPoolNotification::Event(_url, event) = notification {
                match event.kind {
                    Kind::TextNote => {
                        handle_text_note(
                            &es_client,
                            &language_detector,
                            &index_template_name,
                            &event,
                        )
                        .await?;
                    }
                    Kind::EventDeletion => {
                        handle_deletion_event(&es_client, &index_template_name, &event).await?;
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }
    }
}
