use std::{collections::HashSet, fmt};

use chrono::{DateTime, Utc};
use elasticsearch::{Elasticsearch, SearchParts};
use nostr_sdk::prelude::Event;
use serde::Deserialize;
use serde_json::{json, Value};

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

#[derive(Debug, Clone)]
pub struct ElasticsearchQuery {
    query: Value,
    size: i64,
    sort: Vec<&'static str>,
}

fn gen_query(must_conditions: Vec<Option<Value>>) -> Value {
    json!({
        "query": {
            "bool": {
                // exclude None
                "must": must_conditions.into_iter().filter_map(|c| c).collect::<Vec<_>>()
            }
        }
    })
}

fn gen_exact_search_query<T>(field: &str, conds: HashSet<T>) -> Option<Value>
where
    T: fmt::Display,
{
    let ids: Vec<String> = conds
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();

    if ids.is_empty() {
        None
    } else {
        Some(json!({
            "terms": {
                field: ids
            }
        }))
    }
}

fn gen_tag_query<T>(field: &str, conds: &HashSet<T>) -> Option<Value>
where
    T: fmt::Display,
{
    if conds.is_empty() {
        return None;
    }
    let conds: Vec<String> = conds
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    Some(json!({
        "terms": {
            field: conds
        }
    }))
}

impl ElasticsearchQuery {
    pub fn from_filter(filter: nostr_sdk::Filter, cursor: Option<DateTime<Utc>>) -> Self {
        const MAX_LIMIT: usize = 10_000;
        const DEFAULT_LIMIT: usize = 500;

        let tags = &filter.generic_tags;

        let created_at_condition = match (filter.since, filter.until) {
            (Some(since), Some(until)) => Some(json!({
                "range": {
                    "event.created_at": {
                        "gte": since.as_u64()*1000,
                        "lte": until.as_u64()*1000
                    }
                }
            })),
            (Some(since), None) => Some(json!({
                "range": {
                    "event.created_at": {
                        "gte": since.as_u64()*1000
                    }
                }
            })),
            (None, Some(until)) => Some(json!({
                "range": {
                    "event.created_at": {
                        "lte": until.as_u64()*1000,
                    }
                }
            })),
            (None, None) => None,
        };

        let kinds_condition = if filter.kinds.is_empty() {
            None
        } else {
            Some(json!({
                "terms": {
                    "event.kind": filter.kinds
                }
            }))
        };

        let ids_condition = gen_exact_search_query("event.id", filter.ids);
        let authors_condition = gen_exact_search_query("event.pubkey", filter.authors);

        let mut must_conditinos = vec![
            ids_condition,
            authors_condition,
            kinds_condition,
            created_at_condition,
        ];

        if let Some(search) = filter.search {
            let terms = search.trim().split_ascii_whitespace();
            for term in terms {
                must_conditinos.push(Some(json!({
                    "match_phrase": {
                        "text": term,
                    }
                })));
            }
        }

        for (tag_name, values) in tags {
            let tag_condition = gen_tag_query(&format!("tags.{}", tag_name), &values);
            must_conditinos.push(tag_condition);
        }

        match cursor {
            None => {
                // pre-EOSE query
                // treat `limit` as `size` and fetch in reverse chronological order
                let size = filter
                    .limit
                    .and_then(|l| Some(std::cmp::min(l, MAX_LIMIT)))
                    .unwrap_or(DEFAULT_LIMIT) as i64;

                ElasticsearchQuery {
                    query: gen_query(must_conditinos),
                    size,
                    sort: vec!["event.created_at:desc"], // respect created_at for pre-EOSE search
                }
            }
            Some(cursor) => {
                // post-EOSE query
                // ignore `limit` of the filter and fetch in chronological order
                let cursor_condition = Some(json!({
                    "range": {
                        "timestamp": {
                            "gt": cursor.to_rfc3339()
                        }
                    }
                }));
                must_conditinos.push(cursor_condition);

                ElasticsearchQuery {
                    query: gen_query(must_conditinos),
                    size: MAX_LIMIT as i64,
                    sort: vec!["timestamp:asc"], // use timestamp because events with past create_at may arrive
                }
            }
        }
    }

    pub async fn execute(
        &self,
        es_client: &Elasticsearch,
        index_name: &String,
        cursor: Option<DateTime<Utc>>,
    ) -> anyhow::Result<(Vec<Event>, Option<DateTime<Utc>>)> {
        let search_response = es_client
            .search(SearchParts::Index(&[index_name.as_str()]))
            .body(&self.query)
            .sort(&self.sort)
            .size(self.size)
            .send()
            .await;

        let search_response = match search_response {
            Err(err) => {
                log::error!("failed to execute search query {:?}: {}", self, err);
                return Err(anyhow::anyhow!("failed to execute search query"));
            }
            Ok(search_response) => search_response,
        };

        if !search_response.status_code().is_success() {
            return Err(anyhow::anyhow!(
                "unexpected status code: {}",
                search_response.status_code()
            ));
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

        Ok((notes, latest_timestamp))
    }
}
