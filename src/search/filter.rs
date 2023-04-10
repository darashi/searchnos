use std::collections::HashMap;

use nostr_sdk::{Kind, Timestamp};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Filter {
    pub ids: Option<Vec<String>>,
    pub authors: Option<Vec<String>>,
    pub kinds: Option<Vec<Kind>>,
    pub search: Option<String>,
    pub since: Option<Timestamp>,
    pub until: Option<Timestamp>,
    pub limit: Option<usize>,

    #[serde(flatten)]
    pub extra: HashMap<String, Vec<String>>,
}

impl Filter {
    pub fn tags(&self) -> HashMap<String, Vec<String>> {
        self.extra
            .iter()
            .filter(|(k, _)| k.starts_with('#') && k.len() == 2)
            .map(|(k, v)| (k[1..].to_string(), v.clone()))
            .collect::<HashMap<_, _>>()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use crate::search::filter::Filter;

    #[test]
    fn test_search() {
        let src = json!({"search": "hello"});

        assert_eq!(
            serde_json::from_value::<Filter>(src).unwrap(),
            Filter {
                ids: None,
                authors: None,
                kinds: None,
                search: Some("hello".to_string()),
                since: None,
                until: None,
                limit: None,
                extra: HashMap::new(),
            }
        );
    }

    #[test]
    fn test_tags() {
        let src = json!({"#t": ["hello", "world"], "#r": ["http://example.com"]});

        let extra = vec![
            (
                "#t".to_string(),
                vec!["hello".to_string(), "world".to_string()],
            ),
            ("#r".to_string(), vec!["http://example.com".to_string()]),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(
            serde_json::from_value::<Filter>(src).unwrap(),
            Filter {
                ids: None,
                authors: None,
                kinds: None,
                search: None,
                since: None,
                until: None,
                limit: None,
                extra,
            }
        );
    }

    #[test]
    fn test_ignore_unknown_extra() {
        let src = json!({"#t": ["hello", "world"], "foo": ["http://example.com"], "#bar": ["baz"], "#": ["empty"]});

        assert_eq!(
            serde_json::from_value::<Filter>(src).unwrap().tags(),
            vec![(
                "#t".to_string(),
                vec!["hello".to_string(), "world".to_string()]
            )]
            .into_iter()
            .collect::<HashMap<_, _>>()
        );
    }
}
