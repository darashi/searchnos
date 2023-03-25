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

impl ElasticsearchQuery {
    pub fn from_filter(filter: Filter, cursor: Option<DateTime<Utc>>) -> Self {
        const MAX_LIMIT: usize = 10_000;

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

        let search_query = filter.search.and_then(|search| {
            Some(json!({
                "simple_query_string": {
                    "query": search,
                    "fields": ["text"],
                    "default_operator": "and"
                }
            }))
        });

        match cursor {
            None => {
                // the first time query
                // treat `limit` as `size` and fetch in reverse chronological order
                let size = filter
                    .limit
                    .and_then(|l| Some(std::cmp::min(l, MAX_LIMIT)))
                    .unwrap_or(MAX_LIMIT) as i64;
                ElasticsearchQuery {
                    query: gen_query(vec![search_query]),
                    size,
                    sort: vec!["timestamp:desc"],
                }
            }
            Some(cursor) => {
                // this is a continuation query (after EOSE)
                // ignore `limit` of the filter and fetch in chronological order
                let time_condition = Some(json!({
                    "range": {
                        "timestamp": {
                            "gt": cursor.to_rfc3339()
                        }
                    }
                }));

                ElasticsearchQuery {
                    query: gen_query(vec![search_query, time_condition]),
                    size: MAX_LIMIT as i64,
                    sort: vec!["timestamp:asc"],
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
