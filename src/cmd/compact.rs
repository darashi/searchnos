use indicatif::HumanBytes;

use crate::CommonArgs;
use searchnos::db_adapter::open_db;
use searchnos_db::SearchnosDB;

/// Compact the current hot event file into per-day partitions.
pub async fn run(common: CommonArgs) -> anyhow::Result<()> {
    let db = open_db(&common.db_path)?;
    compact_and_print(&db)?;

    Ok(())
}

pub fn compact_and_print(db: &SearchnosDB) -> anyhow::Result<()> {
    let stats = db
        .compact()
        .map_err(|err| anyhow::anyhow!("failed to compact database: {err}"))?;

    println!(
        "Compacted {} events from {} into {} partition files",
        stats.events,
        HumanBytes(stats.bytes),
        stats.files
    );

    Ok(())
}
