pub mod blocks;
pub mod cursor;
pub mod ordinals;

use std::path::PathBuf;

use blocks::{delete_blocks_in_block_range, open_ordhook_db_conn_rocks_db_loop};

use ordinals::{delete_inscriptions_in_block_range, open_readwrite_ordhook_db_conn};
use rocksdb::DB;
use rusqlite::Connection;

use chainhook_sdk::utils::Context;

use crate::{core::meta_protocols::brc20::db::delete_activity_in_block_range, try_info};

pub fn open_readwrite_ordhook_dbs(
    base_dir: &PathBuf,
    ulimit: usize,
    memory_available: usize,
    ctx: &Context,
) -> Result<(DB, Connection), String> {
    let blocks_db =
        open_ordhook_db_conn_rocks_db_loop(true, &base_dir, ulimit, memory_available, &ctx);
    let inscriptions_db = open_readwrite_ordhook_db_conn(&base_dir, &ctx)?;
    Ok((blocks_db, inscriptions_db))
}

pub fn delete_data_in_ordhook_db(
    start_block: u64,
    end_block: u64,
    inscriptions_db_conn_rw: &Connection,
    blocks_db_rw: &DB,
    brc_20_db_conn_rw: &Option<Connection>,
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
        &inscriptions_db_conn_rw,
        &ctx,
    );
    if let Some(conn) = brc_20_db_conn_rw {
        delete_activity_in_block_range(start_block as u32, end_block as u32, &conn, &ctx);
        try_info!(
            ctx,
            "Deleting BRC-20 activity from block #{start_block} to block #{end_block}"
        );
    }
    Ok(())
}
