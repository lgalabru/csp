pub mod blocks;
pub mod cursor;
pub mod ordinals;
pub mod ordinals_pg;

use blocks::{delete_blocks_in_block_range, open_blocks_db_with_retry};

use ordinals::{delete_inscriptions_in_block_range, initialize_ordinals_db, open_ordinals_db_rw};
use rocksdb::DB;
use rusqlite::Connection;

use chainhook_sdk::utils::Context;

use crate::{
    config::Config,
    core::meta_protocols::brc20::db::{
        brc20_new_rw_db_conn, delete_activity_in_block_range, initialize_brc20_db,
    },
    try_info,
};

pub struct SqliteDbConnections {
    pub ordinals: Connection,
    pub brc20: Option<Connection>,
}

/// Opens and initializes all SQLite databases required for Ordhook operation, depending if they are requested by the current
/// `Config`. Returns a struct with all the open connections.
pub fn initialize_sqlite_dbs(config: &Config, ctx: &Context) -> SqliteDbConnections {
    SqliteDbConnections {
        ordinals: initialize_ordinals_db(&config.expected_cache_path(), ctx),
        brc20: match config.meta_protocols.brc20 {
            true => Some(initialize_brc20_db(
                Some(&config.expected_cache_path()),
                ctx,
            )),
            false => None,
        },
    }
}

/// Opens all DBs required for Ordhook operation (read/write), including blocks DB.
pub fn open_all_dbs_rw(
    config: &Config,
    ctx: &Context,
) -> Result<(DB, SqliteDbConnections), String> {
    let blocks_db = open_blocks_db_with_retry(true, &config, ctx);
    let inscriptions_db = open_ordinals_db_rw(&config.expected_cache_path(), ctx)?;
    let brc20_db = brc20_new_rw_db_conn(config, ctx);
    Ok((
        blocks_db,
        SqliteDbConnections {
            ordinals: inscriptions_db,
            brc20: brc20_db,
        },
    ))
}

/// Deletes all block data from all databases within the specified block range.
pub fn drop_block_data_from_all_dbs(
    start_block: u64,
    end_block: u64,
    blocks_db_rw: &DB,
    sqlite_dbs_rw: &SqliteDbConnections,
    ctx: &Context,
) -> Result<(), String> {
    try_info!(
        ctx,
        "Deleting entries from block #{start_block} to block #{end_block}"
    );
    delete_blocks_in_block_range(start_block as u32, end_block as u32, &blocks_db_rw, &ctx);
    try_info!(
        ctx,
        "Deleting inscriptions and locations from block #{start_block} to block #{end_block}"
    );
    delete_inscriptions_in_block_range(
        start_block as u32,
        end_block as u32,
        &sqlite_dbs_rw.ordinals,
        &ctx,
    );
    if let Some(conn) = &sqlite_dbs_rw.brc20 {
        delete_activity_in_block_range(start_block as u32, end_block as u32, &conn, &ctx);
        try_info!(
            ctx,
            "Deleting BRC-20 activity from block #{start_block} to block #{end_block}"
        );
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
