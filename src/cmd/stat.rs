use crate::CommonArgs;
use searchnos_db::DatabaseStats;

pub async fn run(common: CommonArgs) -> anyhow::Result<()> {
    let db = common.open_db()?;
    let stats = db
        .database_stats()
        .map_err(|err| anyhow::anyhow!("failed to collect database stats: {err}"))?;

    if stats.is_empty() {
        println!("No databases found");
        return Ok(());
    }

    print_stats_table(&stats);

    Ok(())
}

fn print_stats_table(stats: &[DatabaseStats]) {
    let name_width = stats
        .iter()
        .map(|stat| stat.name.len())
        .max()
        .unwrap_or(0)
        .max("name".len())
        .max(16);

    let mut formatted_rows = Vec::with_capacity(stats.len());
    let mut total_count = 0usize;
    let mut total_key_bytes = 0usize;
    let mut total_value_bytes = 0usize;
    let mut total_total_bytes = 0usize;

    let mut count_width = "count".len();
    let mut key_bytes_width = "key_bytes".len();
    let mut value_bytes_width = "value_bytes".len();
    let mut total_bytes_width = "total_bytes".len();

    for stat in stats {
        let count_str = format_number(stat.count);
        let key_bytes_str = format_number(stat.key_bytes);
        let value_bytes_str = format_number(stat.value_bytes);
        let total_bytes_str = format_number(stat.total_bytes);

        count_width = count_width.max(count_str.len());
        key_bytes_width = key_bytes_width.max(key_bytes_str.len());
        value_bytes_width = value_bytes_width.max(value_bytes_str.len());
        total_bytes_width = total_bytes_width.max(total_bytes_str.len());

        formatted_rows.push((
            stat.name.clone(),
            count_str,
            key_bytes_str,
            value_bytes_str,
            total_bytes_str,
        ));

        total_count += stat.count;
        total_key_bytes += stat.key_bytes;
        total_value_bytes += stat.value_bytes;
        total_total_bytes += stat.total_bytes;
    }

    let total_count_str = format_number(total_count);
    let total_key_bytes_str = format_number(total_key_bytes);
    let total_value_bytes_str = format_number(total_value_bytes);
    let total_total_bytes_str = format_number(total_total_bytes);

    count_width = count_width.max(total_count_str.len());
    key_bytes_width = key_bytes_width.max(total_key_bytes_str.len());
    value_bytes_width = value_bytes_width.max(total_value_bytes_str.len());
    total_bytes_width = total_bytes_width.max(total_total_bytes_str.len());

    let separator = "-".repeat(name_width);
    let count_separator = "-".repeat(count_width);
    let key_bytes_separator = "-".repeat(key_bytes_width);
    let value_bytes_separator = "-".repeat(value_bytes_width);
    let total_bytes_separator = "-".repeat(total_bytes_width);

    println!(
        "{:<name_width$} {:>count_width$} {:>key_bytes_width$} {:>value_bytes_width$} {:>total_bytes_width$}",
        "name",
        "count",
        "key_bytes",
        "value_bytes",
        "total_bytes",
        name_width = name_width,
        count_width = count_width,
        key_bytes_width = key_bytes_width,
        value_bytes_width = value_bytes_width,
        total_bytes_width = total_bytes_width
    );
    println!(
        "{:<name_width$} {:>count_width$} {:>key_bytes_width$} {:>value_bytes_width$} {:>total_bytes_width$}",
        separator,
        count_separator,
        key_bytes_separator,
        value_bytes_separator,
        total_bytes_separator,
        name_width = name_width,
        count_width = count_width,
        key_bytes_width = key_bytes_width,
        value_bytes_width = value_bytes_width,
        total_bytes_width = total_bytes_width
    );

    for (name, count, key_bytes, value_bytes, total_bytes) in &formatted_rows {
        println!(
            "{:<name_width$} {:>count_width$} {:>key_bytes_width$} {:>value_bytes_width$} {:>total_bytes_width$}",
            name,
            count,
            key_bytes,
            value_bytes,
            total_bytes,
            name_width = name_width,
            count_width = count_width,
            key_bytes_width = key_bytes_width,
            value_bytes_width = value_bytes_width,
            total_bytes_width = total_bytes_width
        );
    }

    println!(
        "{:<name_width$} {:>count_width$} {:>key_bytes_width$} {:>value_bytes_width$} {:>total_bytes_width$}",
        "-".repeat(name_width),
        count_separator,
        key_bytes_separator,
        value_bytes_separator,
        total_bytes_separator,
        name_width = name_width,
        count_width = count_width,
        key_bytes_width = key_bytes_width,
        value_bytes_width = value_bytes_width,
        total_bytes_width = total_bytes_width
    );

    println!(
        "{:<name_width$} {:>count_width$} {:>key_bytes_width$} {:>value_bytes_width$} {:>total_bytes_width$}",
        "TOTAL",
        total_count_str,
        total_key_bytes_str,
        total_value_bytes_str,
        total_total_bytes_str,
        name_width = name_width,
        count_width = count_width,
        key_bytes_width = key_bytes_width,
        value_bytes_width = value_bytes_width,
        total_bytes_width = total_bytes_width
    );
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
