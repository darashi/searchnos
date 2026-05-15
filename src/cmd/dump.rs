use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use indicatif::{HumanBytes, ProgressBar, ProgressState, ProgressStyle};

use crate::CommonArgs;
use searchnos::db_adapter::open_db;

/// Dump stored ndb notes to a length-prefixed binary stream.
pub async fn run(common: CommonArgs, output_path: PathBuf) -> anyhow::Result<()> {
    let db = open_db(&common.db_path)?;
    let file = File::create(&output_path)?;
    let mut writer = BufWriter::new(file);

    let pb = ProgressBar::new_spinner();
    pb.set_style(streaming_progress_style());

    let mut bytes_written = 0u64;
    let count = db
        .dump_events_with_progress(&mut writer, |progress| {
            bytes_written = progress.bytes_written;
            pb.set_position(progress.events_written);
        })
        .map_err(|err| anyhow::anyhow!("failed to dump events: {err}"))?;
    writer.flush()?;

    pb.finish_with_message(format!(
        "Dumped {count} events ({}) to {}",
        HumanBytes(bytes_written),
        output_path.display()
    ));

    Ok(())
}

fn streaming_progress_style() -> ProgressStyle {
    ProgressStyle::with_template("{spinner} {pos} events [{elapsed_precise}, {per_sec_ev}]")
        .expect("streaming progress template must be valid")
        .with_key(
            "per_sec_ev",
            |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = write!(w, "{:.2} ev/s", state.per_sec());
            },
        )
}
