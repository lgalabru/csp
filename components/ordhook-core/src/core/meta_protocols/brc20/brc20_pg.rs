use chainhook_postgres::tokio_postgres::Client;
use refinery::embed_migrations;

embed_migrations!("../../migrations/ordinals-brc20");
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
