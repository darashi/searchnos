use elasticsearch::{
    indices::IndicesPutIndexTemplateParts, ingest::IngestPutPipelineParts, Elasticsearch,
};
use log::info;
use nostr_sdk::prelude::*;

pub async fn put_pipeline(
    es_client: &Elasticsearch,
    pipeline_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("putting pipeline: {}", pipeline_name);
    let res = es_client
        .ingest()
        .put_pipeline(IngestPutPipelineParts::Id(pipeline_name))
        .body(json!({
            "description": "nostr pipeline",
            "processors": [
                {
                    "inference": {
                        "model_id": "lang_ident_model_1",
                        "inference_config": {
                            "classification": {
                                "num_top_classes": 3
                            }
                        },
                        "field_mappings": {},
                        "target_field": "_ml.lang_ident"
                    }
                },
                {
                    "rename": {
                        "field": "_ml.lang_ident.predicted_value",
                        "target_field": "language"
                    }
                },
                {
                    "remove": {
                        "field": "_ml"
                    }
                },
                {
                    "set": {
                        "field": "timestamp",
                        "value": "{{{_ingest.timestamp}}}"
                    }
                }
            ]
        }))
        .send()
        .await?;

    if !res.status_code().is_success() {
        let status = res.status_code();
        let body = res.text().await?;
        return Err(format!("failed to put pipeline: received {}, {}", status, body).into());
    }
    Ok(())
}

pub async fn create_index_template(
    es_client: &Elasticsearch,
    template_name: &str,
    pipeline_name: &str,
    index_name_prefix: &str,
    index_alias_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("putting index template: {}", template_name);
    let res = es_client
        .indices()
        .put_index_template(IndicesPutIndexTemplateParts::Name(template_name))
        .body(json!({
            "index_patterns": [format!("{}-*", index_name_prefix)],
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
                        "default_pipeline": pipeline_name
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
                                    "type": "text",
                                    "index_prefixes": {
                                        "min_chars": 1,
                                        "max_chars": 19
                                    }
                                },
                                "pubkey": {
                                    "type": "text",
                                    "index_prefixes": {
                                        "min_chars": 1,
                                        "max_chars": 19
                                    }
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
                        },
                        "timestamp": {
                            "type": "date"
                        },
                        "tags": {
                            "dynamic": true,
                            "properties": {
                                "*": {
                                    "type": "keyword"
                                }
                            }
                        },
                        "identifier_tag": {
                            "type": "keyword"
                        }
                    }
                },
                "aliases": {
                    index_alias_name: {}
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
