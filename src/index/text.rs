pub use nostr_sdk::prelude::*;
use nostr_sdk::Event;
use std::collections::HashMap;

pub fn extract_text(event: &Event) -> String {
    match event.kind {
        Kind::Metadata => {
            let content: HashMap<String, String> =
                serde_json::from_str(&event.content).unwrap_or_default();
            let texts: Vec<String> = content.values().map(|s| s.to_string()).collect();
            texts.join(" ")
        }
        Kind::LongFormTextNote => {
            let mut items = vec![event.content.clone()];
            items.extend(event.tags.iter().filter_map(|tag| match tag {
                nostr_sdk::Tag::Title(title) => Some(title.clone()),
                nostr_sdk::Tag::Summary(summary) => Some(summary.clone()),
                _ => None,
            }));
            items.join(" ")
        }

        _ => event.content.clone(),
    }
}

#[cfg(test)]
mod tests {
    use nostr_sdk::{Keys, Kind, Tag};

    use crate::index::text::extract_text;

    #[test]
    fn test_extract_text_note() {
        let keys = Keys::generate();
        let event = nostr_sdk::EventBuilder::new(
            Kind::TextNote,
            "hello world",
            &[
                Tag::Identifier("foo".to_string()),
                Tag::Hashtag("bar".to_string()),
                Tag::Title("title".to_string()),
                Tag::Summary("summary".to_string()),
            ],
        )
        .to_event(&keys)
        .unwrap();

        assert_eq!(extract_text(&event), "hello world".to_string());
    }

    #[test]
    fn test_extract_text_long_form_content() {
        let keys = Keys::generate();
        let event = nostr_sdk::EventBuilder::new(
            Kind::LongFormTextNote,
            "# hello\n\nworld",
            &[
                Tag::Identifier("foo".to_string()),
                Tag::Hashtag("bar".to_string()),
                Tag::Title("title".to_string()),
                Tag::Summary("summary".to_string()),
            ],
        )
        .to_event(&keys)
        .unwrap();

        assert_eq!(
            extract_text(&event),
            "# hello\n\nworld title summary".to_string()
        );
    }
}
