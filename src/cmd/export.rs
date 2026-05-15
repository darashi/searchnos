use crate::CommonArgs;
use searchnos::db_adapter::open_db;
use std::io::Write;

pub async fn run(common: CommonArgs) -> anyhow::Result<()> {
    let db = open_db(&common.db_path)?;
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let mut write_error = None;
    db.stream_query("[{}]", |event_json| {
        if let Err(err) = writeln!(stdout, "{event_json}") {
            write_error = Some(err);
            return false;
        }
        true
    })
    .map_err(|err| anyhow::anyhow!("failed to export events: {err}"))?;
    if let Some(err) = write_error {
        return Err(err.into());
    }
    Ok(())
}
