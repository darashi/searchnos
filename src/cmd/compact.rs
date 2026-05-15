use indicatif::HumanBytes;

use crate::CommonArgs;
use searchnos::db_adapter::open_db;

/// Compact the current hot event file into per-day partitions.
pub async fn run(common: CommonArgs) -> anyhow::Result<()> {
    let db = open_db(&common.db_path)?;
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
