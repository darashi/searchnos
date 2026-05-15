use crate::CommonArgs;
use searchnos_db::SearchnosDB;

pub async fn run(common: CommonArgs) -> anyhow::Result<()> {
    let db = SearchnosDB::open(&common.db_path)?;
    for event in db.query("[]")? {
        println!("{event}");
    }
    Ok(())
}
