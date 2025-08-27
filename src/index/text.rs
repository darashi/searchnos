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
                match tag.as_standardized() {
                    Some(TagStandard::Title(title)) => items.push(title.to_owned()),
                    Some(TagStandard::Summary(summary)) => items.push(summary.to_owned()),
                    _ => {}
                };
            }
            items.join(" ")
        }

        _ => event.content.clone(),
    }
}

#[cfg(test)]
mod tests {
    use nostr_sdk::{Keys, Kind, Tag, TagStandard};

    use crate::index::text::extract_text;

    #[test]
    fn test_extract_text_note() {
        let keys = Keys::generate();
        let event = nostr_sdk::EventBuilder::new(Kind::TextNote, "hello world")
            .tags([
                Tag::identifier("foo"),
                Tag::hashtag("bar"),
                Tag::from_standardized(TagStandard::Title("title".to_string())),
                Tag::from_standardized(TagStandard::Summary("summary".to_string())),
            ])
            .sign_with_keys(&keys)
            .unwrap();

        assert_eq!(extract_text(&event), "hello world".to_string());
    }

    #[test]
    fn test_extract_text_long_form_content() {
        let keys = Keys::generate();
        let event = nostr_sdk::EventBuilder::new(Kind::LongFormTextNote, "# hello\n\nworld")
            .tags([
                Tag::identifier("foo"),
                Tag::hashtag("bar"),
                Tag::from_standardized(TagStandard::Title("title".to_string())),
                Tag::from_standardized(TagStandard::Summary("summary".to_string())),
            ])
            .sign_with_keys(&keys)
            .unwrap();

        assert_eq!(
            extract_text(&event),
            "# hello\n\nworld title summary".to_string()
        );
    }
}
