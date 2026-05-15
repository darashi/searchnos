use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use indicatif::{HumanBytes, ProgressBar};
use searchnos_db::{DumpProgress, SearchnosDB};

use crate::{default_progress_style, CommonArgs};

/// Dump stored ndb notes to a length-prefixed binary stream.
pub async fn run(common: CommonArgs, output_path: PathBuf) -> anyhow::Result<()> {
    let db = SearchnosDB::open(&common.db_path)?;
    let file = File::create(&output_path)?;
    let writer = BufWriter::new(file);

    let pb = ProgressBar::new(0);
    pb.set_style(default_progress_style());

    let mut last_progress = DumpProgress {
        events_written: 0,
        total_events: 0,
        bytes_written: 0,
    };

    let count = db.dump_events_with_progress(writer, |progress| {
        if progress.total_events != last_progress.total_events {
            pb.set_length(progress.total_events);
        }
        pb.set_position(progress.events_written);
        last_progress = progress;
    })?;

    if count == 0 {
        pb.set_length(0);
    }
    pb.finish_with_message(format!(
        "Dumped {count} events ({}) to {}",
        HumanBytes(last_progress.bytes_written),
        output_path.display()
    ));

    Ok(())
}
