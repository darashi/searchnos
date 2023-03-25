use std::error::Error;

use chrono::{DateTime, Utc};
use elasticsearch::{Elasticsearch, SearchParts};
use nostr_sdk::prelude::Event;
use serde::Deserialize;
use serde_json::Value;

use crate::condition::Condition;

#[derive(Deserialize, Debug)]
struct Document {
    event: Event,
    #[allow(dead_code)]
    text: String,
    #[allow(dead_code)]
    timestamp: DateTime<Utc>,
    #[allow(dead_code)]
    language: String,
}

struct ElasticsearchQuery {
    query: Value,
    size: i64,
    sort: Vec<&'static str>,
}

fn build_query(
    condition: &Condition,
    limit: &Option<usize>,
    cursor: &Option<DateTime<Utc>>,
) -> ElasticsearchQuery {
    let phrase = condition.query();
    let q = json!({
        "simple_query_string": {
            "query": phrase,
            "fields": ["text"],
            "default_operator": "and"
        }
    });

    let query = if let Some(t) = cursor {
        json!({
            "query": {
                "bool": {
                    "must": [
                        q,
                        {
                            "range": {
                                "timestamp": {
                                    "gt": t.to_rfc3339()
                                }
                            }
                        }
                    ]
                }
            }
        })
    } else {
        json!({ "query": q })
    };

    const MAX_LIMIT: usize = 10_000;
    let size = limit
        .and_then(|l| Some(std::cmp::min(l, MAX_LIMIT)))
        .unwrap_or(MAX_LIMIT) as i64;

    // If `cursor` is specified, we search notes in chronological order.
    // Otherwise, we search notes in reverse chronological order.
    let order = if cursor.is_some() {
        "timestamp:asc"
    } else {
        "timestamp:desc"
    };
    let sort = vec![order];

    ElasticsearchQuery { query, size, sort }
}

pub async fn do_search(
    es_client: &Elasticsearch,
    index_name: &String,
    condition: &Condition,
    cursor: &Option<DateTime<Utc>>,
    limit: &Option<usize>,
) -> Result<(Vec<Event>, Option<DateTime<Utc>>), Box<dyn Error + Send + Sync>> {
    let q = build_query(&condition, limit, &cursor);

    let search_response = es_client
        .search(SearchParts::Index(&[index_name.as_str()]))
        .body(q.query)
        .sort(&q.sort)
        .size(q.size)
        .send()
        .await?;

    if search_response.status_code().is_success() == false {
        return Err(format!("unexpected status code: {}", search_response.status_code()).into());
    }

    let response_body = search_response.json::<Value>().await?;

    let mut notes = vec![];
    let mut latest_timestamp: Option<DateTime<Utc>> = cursor.clone();
    for hit in response_body["hits"]["hits"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
    {
        let doc: Document = serde_json::from_value(hit["_source"].clone())?;
        match latest_timestamp {
            Some(t) => {
                if t < doc.timestamp {
                    latest_timestamp = Some(doc.timestamp);
                }
            }
            None => {
                latest_timestamp = Some(doc.timestamp);
            }
        }
        let note: Event = doc.event;
        notes.push(note);
    }
    if cursor.is_none() {
        notes.reverse();
    }

    return Ok((notes, latest_timestamp));
}
