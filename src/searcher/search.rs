use elasticsearch::Error;
use elasticsearch::{Elasticsearch, SearchParts};
use nostr_sdk::prelude::Event;
use serde_json::Value;

use crate::condition::Condition;

struct Document {
    event: Event,
    text: String,
}

pub async fn do_search(
    es_client: &Elasticsearch,
    index_name: &String,
    condition: &Condition,
    since: Option<u64>,
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

    let search_response = es_client
        .search(SearchParts::Index(&[index_name.as_str()]))
        .body(query)
        .sort(&vec![order])
        .size(1_000)
        .send()
        .await?;
    let response_body = search_response.json::<Value>().await?;
    let mut notes = vec![];
    for note in response_body["hits"]["hits"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
    {
        let note: Event = serde_json::from_value(note["_source"]["event"].clone())?;
        notes.push(note);
    }
    if since.is_none() {
        notes.reverse();
    }

    return Ok(notes);
}
