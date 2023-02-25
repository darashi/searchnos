#[macro_use]
extern crate serde_json;

use elasticsearch::{
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::{IndicesCreateParts, IndicesExistsParts},
    DeleteParts, Elasticsearch, GetParts, IndexParts,
};
use env_logger;
use log::{error, info, warn};
use nostr_sdk::prelude::*;
use serde::Serialize;
use std::env;

async fn create_index(
    es_client: &Elasticsearch,
    index_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if es_client
        .indices()
        .exists(IndicesExistsParts::Index(&[index_name]))
        .send()
        .await?
        .status_code()
        .is_success()
    {
        info!("index already exists; use it as-is");
        return Ok(());
    }

    info!("index does not exist; create it");
    let res = es_client
        .indices()
        .create(IndicesCreateParts::Index(&index_name))
        .body(json!({
            "settings": {
                "index": {
                    "number_of_shards": 3,
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
}

async fn handle_text_note(
    es_client: &Elasticsearch,
    index_name: &str,
    event: &Event,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", event.as_json());
    let id = event.id.to_hex();
    let doc = Document {
        event: event.clone(),
        text: event.content.clone(),
    };
    let res = es_client
        .index(IndexParts::IndexId(index_name, &id))
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
            info!("try to delete: id={}", id);
            let res = es_client
                .get(GetParts::IndexId(&index_name, &id))
                .send()
                .await;
            if res.is_err() {
                error!("failed to fetch; {}", res.err().unwrap());
                continue;
            }
            let res = res.unwrap();
            if res.status_code() == 404 {
                warn!("event not found; id={}", id);
                continue;
            } else if !res.status_code().is_success() {
                let status_code = res.status_code();
                let body = res.text().await?;
                error!("failed to fetch; received {}, {}", status_code, body);
                continue;
            }
            let hit = res.json::<serde_json::Value>().await?;
            let event: Event = serde_json::from_value(hit["_source"]["event"].clone())?;

            let pubkey = event.pubkey;
            let ok_to_delete = deletion_event.pubkey == pubkey;
            info!("can event {} be deleted? {}", event.as_json(), ok_to_delete);
            if !ok_to_delete {
                error!("pubkey mismatch: pub key of deletion event {} was {}, but that of the event {} was {}",
                    deletion_event.id, deletion_event.pubkey, id, pubkey);
                continue;
            }
            let res = es_client
                .delete(DeleteParts::IndexId(&index_name, &id))
                .send()
                .await;
            if res.is_err() {
                error!("failed to delete; {}", res.err().unwrap());
                continue;
            }
            let res = res.unwrap();
            if !res.status_code().is_success() {
                let status_code = res.status_code();
                let body = res.text().await?;
                error!("failed to delete; received {}, {}", status_code, body);
                continue;
            }
            info!("deleted: id={}", id);
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
    let index_name = "nostr";
    create_index(&es_client, index_name).await?;
    info!("elasticsearch index ready");

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
                        handle_text_note(&es_client, &index_name, &event).await?;
                    }
                    Kind::EventDeletion => {
                        handle_deletion_event(&es_client, &index_name, &event).await?;
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }
    }
}
