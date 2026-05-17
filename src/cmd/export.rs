use crate::CommonArgs;
use std::io::Write;

pub async fn run(common: CommonArgs) -> anyhow::Result<()> {
    let db = common.open_db()?;
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
