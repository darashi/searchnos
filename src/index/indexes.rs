use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use nostr_sdk::Event;
use std::collections::HashSet;

const DATE_FORMAT_DAY: &str = "%Y.%m.%d";
const DATE_FORMAT_YEAR: &str = "%Y";

pub fn index_name_for_event(prefix: &str, event: &Event) -> anyhow::Result<String> {
    let dt = chrono::Utc.timestamp_opt(event.created_at.as_u64() as i64, 0);
    if let Some(dt) = dt.single() {
        Ok(format!("{}-{}", prefix, dt.format(DATE_FORMAT_DAY)))
    } else {
        Err(anyhow::anyhow!(
            "failed to parse date: {}",
            event.created_at
        ))
    }
}

pub fn index_name_for_event_with_policy(
    prefix: &str,
    event: &Event,
    yearly_index_kinds: &HashSet<u16>,
) -> anyhow::Result<String> {
    index_name_by_policy(
        prefix,
        event.created_at.as_u64(),
        event.kind.as_u16(),
        yearly_index_kinds,
    )
}

fn index_name_by_policy(
    prefix: &str,
    created_at_epoch: u64,
    kind: u16,
    yearly_index_kinds: &HashSet<u16>,
) -> anyhow::Result<String> {
    let dt = chrono::Utc.timestamp_opt(created_at_epoch as i64, 0);
    if let Some(dt) = dt.single() {
        if yearly_index_kinds.contains(&kind) {
            Ok(format!("{}-{}", prefix, dt.format(DATE_FORMAT_YEAR)))
        } else {
            Ok(format!("{}-{}", prefix, dt.format(DATE_FORMAT_DAY)))
        }
    } else {
        Err(anyhow::anyhow!("failed to parse date: {}", created_at_epoch))
    }
}

pub fn can_exist(
    index_name: &str,
    current_time: &DateTime<Utc>,
    ttl_in_days: Option<u64>,
    allow_future_days: u64,
    yearly_ttl_years: Option<u64>,
) -> anyhow::Result<bool> {
    let date_str = index_name.split('-').nth(1).unwrap_or("");
    // Try day-based format first, then fallback to year-based
    let (index_time, is_yearly) = if let Ok(index_date) = NaiveDate::parse_from_str(date_str, DATE_FORMAT_DAY) {
        (index_date.and_hms_opt(0, 0, 0), false)
    } else if date_str.len() == 4 {
        // Year-only format
        if let Ok(year) = date_str.parse::<i32>() {
            (NaiveDate::from_ymd_opt(year, 1, 1).and_then(|d| d.and_hms_opt(0, 0, 0)), true)
        } else {
            (None, false)
        }
    } else {
        (None, false)
    };
    let Some(index_time) = index_time else { return Ok(false) };
    let index_time = Utc.from_utc_datetime(&index_time);
    let diff: chrono::Duration = current_time.signed_duration_since(index_time);

    if is_yearly {
        if let Some(years) = yearly_ttl_years {
            // Expire at Jan 1 of (index_year + years)
            let index_year = index_time.year();
            if let Some(expire_date) = NaiveDate::from_ymd_opt(index_year + years as i32, 1, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
            {
                let expire_time = Utc.from_utc_datetime(&expire_date);
                if *current_time >= expire_time {
                    return Ok(false);
                }
            }
        }
    } else if let Some(ttl_in_days) = ttl_in_days {
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

    use crate::index::indexes::{can_exist, index_name_by_policy};
    use std::collections::HashSet;

    #[test]
    fn test_can_exist() {
        let current_time = chrono::DateTime::from_str("2023-03-20T00:00:00Z").unwrap();
        assert!(!can_exist("nostr-2023.03.22", &current_time, Some(2), 1, None).unwrap());
        assert!(can_exist("nostr-2023.03.21", &current_time, Some(2), 1, None).unwrap());
        assert!(can_exist("nostr-2023.03.20", &current_time, Some(2), 1, None).unwrap());
        assert!(can_exist("nostr-2023.03.19", &current_time, Some(2), 1, None).unwrap());
        assert!(!can_exist("nostr-2023.03.18", &current_time, Some(2), 1, None).unwrap());

        // ttl is not specified
        assert!(can_exist("nostr-2023.03.18", &current_time, None, 1, None).unwrap());
    }

    #[test]
    fn test_yearly_can_exist() {
        // On 2025-01-01, with TTL 2 years:
        // - nostr-2023 should expire (2023 + 2 => 2025-01-01 cutoff)
        // - nostr-2024 should remain
        let t1 = chrono::DateTime::from_str("2025-01-01T00:00:00Z").unwrap();
        assert!(!can_exist("nostr-2023", &t1, None, 1, Some(2)).unwrap());
        assert!(can_exist("nostr-2024", &t1, None, 1, Some(2)).unwrap());

        // On 2024-12-31, 2023 should still exist
        let t2 = chrono::DateTime::from_str("2024-12-31T23:59:59Z").unwrap();
        assert!(can_exist("nostr-2023", &t2, None, 1, Some(2)).unwrap());
    }

    #[test]
    fn test_index_name_for_event_with_policy() {
        // 2023-03-20 00:00:00Z
        let ts = 1679270400u64;
        let kind_yearly = 0u16;
        let kind_daily = 1u16;

        let mut yearly_kinds: HashSet<u16> = HashSet::new();
        yearly_kinds.insert(kind_yearly);

        let idx1 = index_name_by_policy("nostr", ts, kind_yearly, &yearly_kinds).unwrap();
        assert_eq!(idx1, "nostr-2023");

        let idx2 = index_name_by_policy("nostr", ts, kind_daily, &yearly_kinds).unwrap();
        assert_eq!(idx2, "nostr-2023.03.20");
    }
}
