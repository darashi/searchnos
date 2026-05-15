use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{ensure, Context};
use indicatif::{HumanBytes, ProgressBar};

use crate::{byte_progress_style, cmd::compact::compact_and_print, CommonArgs};
use searchnos::db_adapter::open_db;

/// Load stored ndb notes from length-prefixed binary streams.
pub async fn run(common: CommonArgs, input_paths: Vec<PathBuf>) -> anyhow::Result<()> {
    ensure!(
        !input_paths.is_empty(),
        "at least one input dump file is required"
    );

    let inputs = input_paths
        .into_iter()
        .map(|path| {
            path.metadata()
                .with_context(|| format!("failed to read metadata for {}", path.display()))
                .map(|metadata| metadata.len())
                .map(|bytes| (path, bytes))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let total_bytes = inputs.iter().map(|(_, bytes)| *bytes).sum();

    let db = open_db(&common.db_path)?;

    let pb = ProgressBar::new(total_bytes);
    pb.set_style(byte_progress_style());

    let mut total_count = 0u64;
    let mut bytes_read = 0u64;
    for (input_path, input_bytes) in &inputs {
        let file = File::open(input_path)
            .with_context(|| format!("failed to open {}", input_path.display()))?;
        let mut reader = BufReader::new(file);
        let base_bytes_read = bytes_read;

        let count = db
            .load_events_with_progress(&mut reader, |progress| {
                pb.set_position((base_bytes_read + progress.bytes_read).min(total_bytes));
            })
            .with_context(|| format!("failed to load events from {}", input_path.display()))?;

        bytes_read = base_bytes_read + *input_bytes;
        total_count += count;
        pb.set_position(bytes_read.min(total_bytes));
    }

    if total_count == 0 {
        pb.set_position(total_bytes);
    }
    pb.finish_with_message(format!(
        "Loaded {total_count} events ({}) from {} files",
        HumanBytes(bytes_read),
        inputs.len()
    ));

    compact_and_print(&db)?;

    Ok(())
}
