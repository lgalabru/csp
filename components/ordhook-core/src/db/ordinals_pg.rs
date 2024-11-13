use chainhook_postgres::{pg_numeric_u64::PgNumericU64, tokio_postgres::{Client, GenericClient}};
use refinery::embed_migrations;

embed_migrations!("../../migrations/ordinals");
pub async fn migrate(pg_client: &mut Client) -> Result<(), String> {
    return match migrations::runner()
        .set_migration_table_name("pgmigrations")
        .run_async(pg_client)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Error running pg migrations: {e}"))
    };
}

pub async fn get_chain_tip_block_height<T: GenericClient>(pg_client: &T) -> Result<Option<u64>, String> {
    let row = pg_client
        .query_opt("SELECT block_height FROM chain_tip", &[])
        .await
        .map_err(|e| format!("error getting chain tip: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let max: Option<PgNumericU64> = row.get("block_height");
    Ok(max.map(|v| v.0))
}

pub async fn get_highest_inscription_number<T: GenericClient>(pg_client: &T) -> Result<Option<u64>, String> {
    let row = pg_client
        .query_opt("SELECT MAX(number) AS max FROM inscriptions", &[])
        .await
        .map_err(|e| format!("error getting highest inscription number: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let max: Option<PgNumericU64> = row.get("max");
    Ok(max.map(|v| v.0))
}
