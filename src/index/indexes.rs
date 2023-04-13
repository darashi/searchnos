use chrono::{DateTime, TimeZone, Utc};
use nostr_sdk::Event;

const DATE_FORMAT: &str = "%Y.%m.%d";

pub fn index_name_for_event(prefix: &str, event: &Event) -> anyhow::Result<String> {
    let dt = chrono::Utc.timestamp_opt(event.created_at.as_i64(), 0);
    if let Some(dt) = dt.single() {
        Ok(format!("{}-{}", prefix, dt.format(DATE_FORMAT).to_string()))
    } else {
        Err(anyhow::anyhow!("failed to parse date: {}", event.created_at).into())
    }
}

pub fn can_exist(
    index_name: &str,
    current_time: &DateTime<Utc>,
    ttl_in_days: Option<u64>,
    allow_future_days: u64,
) -> anyhow::Result<bool> {
    let date_str = index_name.split('-').nth(1).unwrap_or("");
    let index_date = chrono::NaiveDate::parse_from_str(date_str, DATE_FORMAT)?;
    let index_time = index_date.and_hms_opt(0, 0, 0);
    let index_time = if let Some(index_time) = index_time {
        index_time
    } else {
        return Ok(false);
    };
    let index_time = Utc.from_utc_datetime(&index_time);
    let diff: chrono::Duration = current_time.signed_duration_since(index_time);

    if let Some(ttl_in_days) = ttl_in_days {
        let ttl_duration: chrono::Duration = chrono::Duration::days(ttl_in_days as i64);
        if diff >= ttl_duration {
            return Ok(false);
        }
    }
    Ok(-chrono::Duration::days(allow_future_days as i64) <= diff)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::index::indexes::can_exist;

    #[test]
    fn test_can_exist() {
        let current_time = chrono::DateTime::from_str("2023-03-20T00:00:00Z").unwrap();
        assert_eq!(
            can_exist("nostr-2023.03.22", &current_time, Some(2), 1).unwrap(),
            false
        );
        assert_eq!(
            can_exist("nostr-2023.03.21", &current_time, Some(2), 1).unwrap(),
            true
        );
        assert_eq!(
            can_exist("nostr-2023.03.20", &current_time, Some(2), 1).unwrap(),
            true
        );
        assert_eq!(
            can_exist("nostr-2023.03.19", &current_time, Some(2), 1).unwrap(),
            true
        );
        assert_eq!(
            can_exist("nostr-2023.03.18", &current_time, Some(2), 1).unwrap(),
            false
        );

        // ttl is not specified
        assert_eq!(
            can_exist("nostr-2023.03.18", &current_time, None, 1).unwrap(),
            true
        );
    }
}
