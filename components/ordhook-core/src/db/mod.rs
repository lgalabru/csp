pub mod blocks;
pub mod cursor;
pub mod models;
pub mod ordinals_pg;

use chainhook_postgres::pg_connect_with_retry;

use chainhook_sdk::utils::Context;

use crate::{config::Config, core::meta_protocols::brc20::brc20_pg, try_info};

pub async fn migrate_dbs(config: &Config, ctx: &Context) -> Result<(), String> {
    {
        try_info!(ctx, "Running ordinals DB migrations");
        let mut pg_client = pg_connect_with_retry(&config.ordinals_db.to_conn_config(), ctx).await;
        ordinals_pg::migrate(&mut pg_client).await?;
    }
    if let (Some(brc20_db), true) = (&config.brc20_db, config.meta_protocols.brc20) {
        try_info!(ctx, "Running brc20 DB migrations");
        let mut pg_client = pg_connect_with_retry(&brc20_db.to_conn_config(), ctx).await;
        brc20_pg::migrate(&mut pg_client).await?;
    }
    Ok(())
}

/// Drops DB files in a test environment.
#[cfg(test)]
pub fn drop_all_dbs(config: &Config) {
    let dir_path = &config.expected_cache_path();
    if dir_path.exists() {
        std::fs::remove_dir_all(dir_path).unwrap();
    }
}
