use elasticsearch::Error;
use elasticsearch::{Elasticsearch, SearchParts};
use nostr_sdk::prelude::Event;
use serde::Deserialize;
use serde_json::Value;

use crate::condition::Condition;

#[derive(Deserialize)]
struct Document {
    event: Event,
    #[allow(dead_code)]
    text: String,
}

pub async fn do_search(
    es_client: &Elasticsearch,
    index_name: &String,
    condition: &Condition,
    since: &Option<u64>,
    limit: &Option<usize>,
) -> Result<Vec<Event>, Error> {
    let phrase = condition.query();
    let q = json!({
        "simple_query_string": {
            "query": phrase,
            "fields": ["text"],
            "default_operator": "and"
        }
    });
    let query = if let Some(t) = since {
        json!({
            "query": {
                "bool": {
                    "must": [
                        q,
                        {
                            "range": {
                                "event.created_at": {
                                    "gt": format!("{}", t)
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

    // If `since` is specified, we search notes in chronological order.
    // Otherwise, we search notes in reverse chronological order.
    let order = if since.is_some() {
        "event.created_at:asc"
    } else {
        "event.created_at:desc"
    };
    const MAX_LIMIT: usize = 10_000;
    let size = limit
        .and_then(|l| Some(std::cmp::min(l, MAX_LIMIT)))
        .unwrap_or(MAX_LIMIT) as i64;

    let search_response = es_client
        .search(SearchParts::Index(&[index_name.as_str()]))
        .body(query)
        .sort(&vec![order])
        .size(size)
        .send()
        .await?;
    let response_body = search_response.json::<Value>().await?;
    let mut notes = vec![];
    for note in response_body["hits"]["hits"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
    {
        let doc: Document = serde_json::from_value(note["_source"].clone())?;
        let note: Event = doc.event;
        notes.push(note);
    }
    if since.is_none() {
        notes.reverse();
    }

    return Ok(notes);
}
