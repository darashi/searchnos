pub use nostr_sdk::prelude::*;
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
            for tag in event.tags.iter() {
                if let ("title" | "summary", Some(value)) = (tag.kind(), tag.content()) {
                    items.push(value.to_owned());
                }
            }
            items.join(" ")
        }

        _ => event.content.clone(),
    }
}

#[cfg(test)]
mod tests {
    use nostr_sdk::prelude::{FinalizeEvent, Keys, Kind, Tag};

    use crate::index::text::extract_text;

    #[test]
    fn test_extract_text_note() {
        let keys = Keys::generate();
        let event = nostr_sdk::prelude::EventBuilder::new(Kind::TextNote, "hello world")
            .tags([
                Tag::identifier("foo"),
                Tag::hashtag("bar"),
                Tag::custom("title", ["title"]),
                Tag::custom("summary", ["summary"]),
            ])
            .finalize(&keys)
            .unwrap();

        assert_eq!(extract_text(&event), "hello world".to_string());
    }

    #[test]
    fn test_extract_text_long_form_content() {
        let keys = Keys::generate();
        let event =
            nostr_sdk::prelude::EventBuilder::new(Kind::LongFormTextNote, "# hello\n\nworld")
                .tags([
                    Tag::identifier("foo"),
                    Tag::hashtag("bar"),
                    Tag::custom("title", ["title"]),
                    Tag::custom("summary", ["summary"]),
                ])
                .finalize(&keys)
                .unwrap();

        assert_eq!(
            extract_text(&event),
            "# hello\n\nworld title summary".to_string()
        );
    }
}
