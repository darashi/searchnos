use indicatif::ProgressBar;
use searchnos_db::ReindexProgressPhase;

use crate::{default_progress_style, CommonArgs};

/// Rebuild partition search and visibility sidecars.
pub async fn run(common: CommonArgs, force: bool) -> anyhow::Result<()> {
    let db = common.open_db()?;
    let pb = ProgressBar::new(0);
    pb.set_style(default_progress_style());

    let stats = db
        .reindex_with_progress(force, |progress| {
            pb.set_length(progress.file_total);
            pb.set_position(progress.file_index.min(progress.file_total));
            match progress.phase {
                ReindexProgressPhase::Started => {
                    pb.set_message(format!("Reindexing {}", progress.path.display()));
                }
                ReindexProgressPhase::Finished => {
                    pb.set_message(format!(
                        "Reindexed {} events from {}",
                        progress.events,
                        progress.path.display()
                    ));
                }
                ReindexProgressPhase::Skipped => {
                    pb.set_message(format!("Skipped {}", progress.path.display()));
                }
            }
        })
        .map_err(|err| anyhow::anyhow!("failed to reindex database: {err}"))?;

    pb.finish_with_message(format!(
        "Reindexed {} partition files ({} skipped, {} events)",
        stats.files, stats.skipped_files, stats.events
    ));

    Ok(())
}
