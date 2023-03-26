use std::fmt;

use chrono::{DateTime, Utc};
use elasticsearch::{Elasticsearch, SearchParts};
use nostr_sdk::prelude::{Event, Filter};
use serde::Deserialize;
use serde_json::Value;

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

fn gen_prefix_search_query<T>(field: &str, conds: Option<Vec<T>>) -> Option<Value>
where
    T: fmt::Display,
{
    conds.and_then(|ids| {
        let ids: Vec<String> = ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>();
        // often requested to be an exact match search with 64 characters,
        // so it is processed separately from the prefix search.
        let (ids, id_prefixes): (Vec<_>, Vec<_>) = ids.into_iter().partition(|id| id.len() == 64);

        let ids_cond = if ids.is_empty() {
            vec![]
        } else {
            vec![json!({
                "terms": {
                    field: ids
                }
            })]
        };

        let id_prefix_conds = id_prefixes
            .into_iter()
            .map(|id| {
                json!({
                    "prefix": {
                        field: id
                    }
                })
            })
            .collect::<Vec<_>>();

        let should_conds = [ids_cond, id_prefix_conds].concat();

        Some(json!({
            "bool": {
                "should": should_conds,
                "minimum_should_match": 1
            }
        }))
    })
}

fn gen_tag_query<T>(field: &str, conds: Option<Vec<T>>) -> Option<Value>
where
    T: fmt::Display,
{
    conds.and_then(|conds| {
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
    })
}

impl ElasticsearchQuery {
    pub fn from_filter(filter: Filter, cursor: Option<DateTime<Utc>>) -> Self {
        const MAX_LIMIT: usize = 10_000;
        const DEFAULT_LIMIT: usize = 500;

        let search_condition = filter.search.and_then(|search| {
            Some(json!({
                "simple_query_string": {
                    "query": search,
                    "fields": ["text"],
                    "default_operator": "and"
                }
            }))
        });

        let created_at_condition = match (filter.since, filter.until) {
            (Some(since), Some(until)) => Some(json!({
                "range": {
                    "event.created_at": {
                        "gt": since.as_u64(),
                        "lt": until.as_u64()
                    }
                }
            })),
            (Some(since), None) => Some(json!({
                "range": {
                    "event.created_at": {
                        "gt": since.as_u64()
                    }
                }
            })),
            (None, Some(until)) => Some(json!({
                "range": {
                    "event.created_at": {
                        "lt": until.as_u64()
                    }
                }
            })),
            (None, None) => None,
        };

        let kinds_condition = filter.kinds.and_then(|kinds| {
            Some(json!({
                "terms": {
                    "event.kind": kinds
                }
            }))
        });

        let ids_condition = gen_prefix_search_query("event.id", filter.ids);
        let authors_condition = gen_prefix_search_query("event.pubkey", filter.authors);
        let e_condition: Option<Value> = gen_tag_query("tags.e", filter.events);
        let p_condition: Option<Value> = gen_tag_query("tags.p", filter.pubkeys);

        match cursor {
            None => {
                // pre-EOSE query
                // treat `limit` as `size` and fetch in reverse chronological order
                let size = filter
                    .limit
                    .and_then(|l| Some(std::cmp::min(l, MAX_LIMIT)))
                    .unwrap_or(DEFAULT_LIMIT) as i64;

                ElasticsearchQuery {
                    query: gen_query(vec![
                        ids_condition,
                        authors_condition,
                        kinds_condition,
                        created_at_condition,
                        e_condition,
                        p_condition,
                        search_condition,
                    ]),
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

                ElasticsearchQuery {
                    query: gen_query(vec![
                        ids_condition,
                        authors_condition,
                        kinds_condition,
                        created_at_condition,
                        e_condition,
                        p_condition,
                        search_condition,
                        cursor_condition,
                    ]),
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
            .await?;

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
