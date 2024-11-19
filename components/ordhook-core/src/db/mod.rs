pub mod blocks;
pub mod cursor;
pub mod models;
pub mod ordinals_pg;

use chainhook_postgres::{pg_connect_with_retry, tokio_postgres::Client};

use chainhook_sdk::utils::Context;

use crate::{
    config::{Config, PostgresConfig},
    core::meta_protocols::brc20::brc20_pg,
    try_info,
};

pub async fn pg_connect(pg_config: &PostgresConfig, ctx: &Context) -> Client {
    pg_connect_with_retry(&pg_config.to_conn_config(), ctx).await
}

pub async fn migrate_dbs(config: &Config, ctx: &Context) -> Result<(), String> {
    {
        try_info!(ctx, "Running ordinals DB migrations");
        let mut pg_client = pg_connect(&config.ordinals_db, ctx).await;
        ordinals_pg::migrate(&mut pg_client).await?;
    }
    if let (Some(brc20_db), true) = (&config.brc20_db, config.meta_protocols.brc20) {
        try_info!(ctx, "Running brc20 DB migrations");
        let mut pg_client = pg_connect(&brc20_db, ctx).await;
        brc20_pg::migrate(&mut pg_client).await?;
    }
    Ok(())
}

// pub struct SqliteDbConnections {
//     pub ordinals: Connection,
//     pub brc20: Option<Connection>,
// }

/// Opens and initializes all SQLite databases required for Ordhook operation, depending if they are requested by the current
/// `Config`. Returns a struct with all the open connections.
// pub fn initialize_sqlite_dbs(config: &Config, ctx: &Context) -> SqliteDbConnections {
//     SqliteDbConnections {
//         ordinals: initialize_ordinals_db(&config.expected_cache_path(), ctx),
//         brc20: match config.meta_protocols.brc20 {
//             true => Some(initialize_brc20_db(
//                 Some(&config.expected_cache_path()),
//                 ctx,
//             )),
//             false => None,
//         },
//     }
// }

/// Opens all DBs required for Ordhook operation (read/write), including blocks DB.
// pub fn open_all_dbs_rw(
//     config: &Config,
//     ctx: &Context,
// ) -> Result<(DB, SqliteDbConnections), String> {
//     let blocks_db = open_blocks_db_with_retry(true, &config, ctx);
//     let inscriptions_db = open_ordinals_db_rw(&config.expected_cache_path(), ctx)?;
//     let brc20_db = brc20_new_rw_db_conn(config, ctx);
//     Ok((
//         blocks_db,
//         SqliteDbConnections {
//             ordinals: inscriptions_db,
//             brc20: brc20_db,
//         },
//     ))
// }

/// Deletes all block data from all databases within the specified block range.
// pub fn drop_block_data_from_all_dbs(
//     start_block: u64,
//     end_block: u64,
//     blocks_db_rw: &DB,
//     sqlite_dbs_rw: &SqliteDbConnections,
//     ctx: &Context,
// ) -> Result<(), String> {
//     try_info!(
//         ctx,
//         "Deleting entries from block #{start_block} to block #{end_block}"
//     );
//     delete_blocks_in_block_range(start_block as u32, end_block as u32, &blocks_db_rw, &ctx);
//     try_info!(
//         ctx,
//         "Deleting inscriptions and locations from block #{start_block} to block #{end_block}"
//     );
//     // FIXME
//     delete_inscriptions_in_block_range(
//         start_block as u32,
//         end_block as u32,
//         &sqlite_dbs_rw.ordinals,
//         &ctx,
//     );
//     if let Some(conn) = &sqlite_dbs_rw.brc20 {
//         delete_activity_in_block_range(start_block as u32, end_block as u32, &conn, &ctx);
//         try_info!(
//             ctx,
//             "Deleting BRC-20 activity from block #{start_block} to block #{end_block}"
//         );
//     }
//     Ok(())
// }

/// Drops DB files in a test environment.
#[cfg(test)]
pub fn drop_all_dbs(config: &Config) {
    let dir_path = &config.expected_cache_path();
    if dir_path.exists() {
        std::fs::remove_dir_all(dir_path).unwrap();
    }
}
