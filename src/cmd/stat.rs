use std::error::Error;

use chrono::{DateTime, Utc};

use crate::CommonArgs;
use searchnos_db::SearchnosDB;

pub async fn run(common: CommonArgs) -> Result<(), Box<dyn Error>> {
    let db = SearchnosDB::open(&common.db_path)?;
    let db_stats = db.database_stats()?;
    let index_used_bytes = db_stats.iter().map(|stat| stat.total_bytes).sum::<usize>();
    let events_total = db_stats
        .iter()
        .find(|stat| stat.name == "events")
        .map(|stat| stat.count)
        .unwrap_or(0);
    let kind_stats = db.kind_stats()?;

    println!("index_used_bytes: {}", format_number(index_used_bytes));
    println!("kind_stats:");
    if kind_stats.is_empty() {
        println!("  none");
        return Ok(());
    }

    let mut rows = Vec::with_capacity(kind_stats.len());
    let mut kind_width = "kind".len();
    let mut count_width = "count".len();
    let mut oldest_width = "oldest".len();
    let mut newest_width = "newest".len();
    let mut total_oldest: Option<u64> = None;
    let mut total_newest: Option<u64> = None;

    for stat in kind_stats {
        let kind_str = stat.kind.to_string();
        let count_str = format_number(stat.count);
        let oldest_str = format_datetime(stat.oldest_created_at);
        let newest_str = format_datetime(stat.newest_created_at);
        kind_width = kind_width.max(kind_str.len());
        count_width = count_width.max(count_str.len());
        oldest_width = oldest_width.max(oldest_str.len());
        newest_width = newest_width.max(newest_str.len());
        rows.push((kind_str, count_str, oldest_str, newest_str));

        if let Some(oldest) = stat.oldest_created_at {
            total_oldest = Some(match total_oldest {
                Some(current) => current.min(oldest),
                None => oldest,
            });
        }

        if let Some(newest) = stat.newest_created_at {
            total_newest = Some(match total_newest {
                Some(current) => current.max(newest),
                None => newest,
            });
        }
    }

    let total_count_str = format_number(events_total);
    let total_oldest_str = format_datetime(total_oldest);
    let total_newest_str = format_datetime(total_newest);
    kind_width = kind_width.max("TOTAL".len());
    count_width = count_width.max(total_count_str.len());
    oldest_width = oldest_width.max(total_oldest_str.len());
    newest_width = newest_width.max(total_newest_str.len());

    let kind_separator = "-".repeat(kind_width);
    let count_separator = "-".repeat(count_width);
    let oldest_separator = "-".repeat(oldest_width);
    let newest_separator = "-".repeat(newest_width);

    println!(
        "  {kind:<kind_width$} {count:>count_width$} {oldest:<oldest_width$} {newest:<newest_width$}",
        kind = "kind",
        count = "count",
        oldest = "oldest",
        newest = "newest",
        kind_width = kind_width,
        count_width = count_width,
        oldest_width = oldest_width,
        newest_width = newest_width,
    );
    println!(
        "  {kind:<kind_width$} {count:>count_width$} {oldest:<oldest_width$} {newest:<newest_width$}",
        kind = kind_separator,
        count = count_separator,
        oldest = oldest_separator,
        newest = newest_separator,
        kind_width = kind_width,
        count_width = count_width,
        oldest_width = oldest_width,
        newest_width = newest_width,
    );

    for (kind, count, oldest, newest) in rows {
        println!(
            "  {kind:<kind_width$} {count:>count_width$} {oldest:<oldest_width$} {newest:<newest_width$}",
            kind = kind,
            count = count,
            oldest = oldest,
            newest = newest,
            kind_width = kind_width,
            count_width = count_width,
            oldest_width = oldest_width,
            newest_width = newest_width,
        );
    }

    println!(
        "  {kind:<kind_width$} {count:>count_width$} {oldest:<oldest_width$} {newest:<newest_width$}",
        kind = kind_separator,
        count = count_separator,
        oldest = oldest_separator,
        newest = newest_separator,
        kind_width = kind_width,
        count_width = count_width,
        oldest_width = oldest_width,
        newest_width = newest_width,
    );

    println!(
        "  {kind:<kind_width$} {count:>count_width$} {oldest:<oldest_width$} {newest:<newest_width$}",
        kind = "TOTAL",
        count = total_count_str,
        oldest = total_oldest_str,
        newest = total_newest_str,
        kind_width = kind_width,
        count_width = count_width,
        oldest_width = oldest_width,
        newest_width = newest_width,
    );

    Ok(())
}

fn format_number(value: usize) -> String {
    let digits = value.to_string();
    let len = digits.len();
    let mut formatted = String::with_capacity(len + len / 3);
    for (index, ch) in digits.chars().enumerate() {
        if index != 0 && (len - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(ch);
    }
    formatted
}

fn format_datetime(timestamp: Option<u64>) -> String {
    match timestamp
        .and_then(|ts| i64::try_from(ts).ok())
        .and_then(|ts| DateTime::<Utc>::from_timestamp(ts, 0))
    {
        Some(datetime) => datetime.to_rfc3339(),
        None => "-".to_string(),
    }
}
