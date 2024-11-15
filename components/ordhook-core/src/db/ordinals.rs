use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use rusqlite::{Connection, OpenFlags, ToSql, Transaction};

use chainhook_sdk::{
    types::{
        BitcoinBlockData, BlockIdentifier, OrdinalInscriptionNumber, OrdinalInscriptionRevealData,
        TransactionIdentifier,
    },
    utils::Context,
};

use crate::{
    core::protocol::{
        inscription_parsing::{
            get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
        },
        satoshi_numbering::TraversalResult,
    },
    try_error, try_warn,
    utils::{
        format_outpoint_to_watch, parse_inscription_id, parse_outpoint_to_watch,
        parse_satpoint_to_watch,
    },
};

pub fn get_default_ordinals_db_file_path(base_dir: &PathBuf) -> PathBuf {
    let mut destination_path = base_dir.clone();
    destination_path.push("hord.sqlite");
    destination_path
}

pub fn open_ordinals_db(base_dir: &PathBuf, ctx: &Context) -> Result<Connection, String> {
    let path = get_default_ordinals_db_file_path(&base_dir);
    let conn = open_existing_readonly_db(&path, ctx);
    Ok(conn)
}

pub fn open_ordinals_db_rw(base_dir: &PathBuf, ctx: &Context) -> Result<Connection, String> {
    let db_path = get_default_ordinals_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(Some(&db_path), ctx);
    Ok(conn)
}

pub fn initialize_ordinals_db(base_dir: &PathBuf, ctx: &Context) -> Connection {
    let db_path = get_default_ordinals_db_file_path(&base_dir);
    let conn = create_or_open_readwrite_db(Some(&db_path), ctx);
    // TODO: introduce initial output
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS inscriptions (
            inscription_id TEXT NOT NULL PRIMARY KEY,
            input_index INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            ordinal_number INTEGER NOT NULL,
            jubilee_inscription_number INTEGER NOT NULL,
            classic_inscription_number INTEGER NOT NULL,
            CONSTRAINT inscription_id_uniqueness UNIQUE (inscription_id),
            CONSTRAINT jubilee_inscription_number_uniqueness UNIQUE (inscription_id),
            CONSTRAINT classic_inscription_number_uniqueness UNIQUE (inscription_id)
        )",
        [],
    ) {
        try_warn!(
            ctx,
            "Unable to create table inscriptions: {}",
            e.to_string()
        );
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_ordinal_number ON inscriptions(ordinal_number);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_jubilee_inscription_number ON inscriptions(jubilee_inscription_number);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }

        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_classic_inscription_number ON inscriptions(classic_inscription_number);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }

        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS index_inscriptions_on_block_height ON inscriptions(block_height);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }
    }
    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS locations (
            ordinal_number INTEGER NOT NULL,
            block_height INTEGER NOT NULL,
            tx_index INTEGER NOT NULL,
            outpoint_to_watch TEXT NOT NULL,
            offset INTEGER NOT NULL,
            CONSTRAINT ordinal_number_outpoint_to_watch_offset_uniqueness UNIQUE (ordinal_number, outpoint_to_watch)
        )",
        [],
    ) {
        try_warn!(
            ctx,
            "Unable to create table locations: {}",
            e.to_string()
        );
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_indexed_on_block_height ON locations(block_height);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_indexed_on_outpoint_to_watch ON locations(outpoint_to_watch);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS locations_indexed_on_ordinal_number ON locations(ordinal_number);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }
    }

    if let Err(e) = conn.execute(
        "CREATE TABLE IF NOT EXISTS sequence_metadata (
            block_height INTEGER NOT NULL,
            nth_classic_pos_number INTEGER NOT NULL,
            nth_classic_neg_number INTEGER NOT NULL,
            nth_jubilee_number INTEGER NOT NULL
        )",
        [],
    ) {
        try_warn!(
            ctx,
            "Unable to create table sequence_metadata: {}",
            e.to_string()
        );
    } else {
        if let Err(e) = conn.execute(
            "CREATE INDEX IF NOT EXISTS sequence_metadata_indexed_on_block_height ON sequence_metadata(block_height);",
            [],
        ) {
            try_warn!(ctx, "unable to create hord.sqlite: {}", e.to_string());
        }
    }

    conn
}

pub fn create_or_open_readwrite_db(db_path: Option<&PathBuf>, ctx: &Context) -> Connection {
    let open_flags = if let Some(db_path) = db_path {
        match std::fs::metadata(&db_path) {
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    // need to create
                    if let Some(dirp) = PathBuf::from(&db_path).parent() {
                        std::fs::create_dir_all(dirp).unwrap_or_else(|e| {
                            try_error!(ctx, "{}", e.to_string());
                        });
                    }
                    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
                } else {
                    panic!("FATAL: could not stat {}", db_path.display());
                }
            }
            Ok(_md) => {
                // can just open
                OpenFlags::SQLITE_OPEN_READ_WRITE
            }
        }
    } else {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    };

    let path = match db_path {
        Some(path) => path.to_str().unwrap(),
        None => ":memory:",
    };
    let conn = loop {
        match Connection::open_with_flags(&path, open_flags) {
            Ok(conn) => break conn,
            Err(e) => {
                try_error!(ctx, "{}", e.to_string());
            }
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    connection_with_defaults_pragma(conn)
}

pub fn open_existing_readonly_db(db_path: &PathBuf, ctx: &Context) -> Connection {
    let open_flags = match std::fs::metadata(db_path) {
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                panic!("FATAL: could not find {}", db_path.display());
            } else {
                panic!("FATAL: could not stat {}", db_path.display());
            }
        }
        Ok(_md) => {
            // can just open
            OpenFlags::SQLITE_OPEN_READ_ONLY
        }
    };

    let conn = loop {
        match Connection::open_with_flags(db_path, open_flags) {
            Ok(conn) => break conn,
            Err(e) => {
                try_warn!(ctx, "unable to open hord.rocksdb: {}", e.to_string());
            }
        };
        std::thread::sleep(std::time::Duration::from_secs(1));
    };
    connection_with_defaults_pragma(conn)
}

fn connection_with_defaults_pragma(conn: Connection) -> Connection {
    conn.busy_timeout(std::time::Duration::from_secs(300))
        .expect("unable to set db timeout");
    conn.pragma_update(None, "mmap_size", 512 * 1024 * 1024)
        .expect("unable to enable mmap_size");
    conn.pragma_update(None, "cache_size", 512 * 1024 * 1024)
        .expect("unable to enable cache_size");
    conn.pragma_update(None, "journal_mode", &"WAL")
        .expect("unable to enable wal");
    conn
}

pub fn insert_entry_in_inscriptions(
    inscription_data: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO inscriptions (inscription_id, ordinal_number, jubilee_inscription_number, classic_inscription_number, block_height, input_index) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![&inscription_data.inscription_id, &inscription_data.ordinal_number, &inscription_data.inscription_number.jubilee, &inscription_data.inscription_number.classic, &block_identifier.index, &inscription_data.inscription_input_index],
    ) {
        try_warn!(ctx, "unable to insert inscription in hord.sqlite: {} - {:?}", e.to_string(), inscription_data);
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrdinalLocation {
    pub offset: u64,
    pub block_height: u64,
    pub tx_index: usize,
}

pub fn insert_entries_from_block_in_inscriptions(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    for inscription_data in get_inscriptions_revealed_in_block(&block).iter() {
        insert_entry_in_inscriptions(
            inscription_data,
            &block.block_identifier,
            inscriptions_db_conn_rw,
            &ctx,
        );
    }
}

pub fn update_ordinals_db_with_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    let mut locations_to_insert = HashMap::new();

    for inscription_data in get_inscriptions_revealed_in_block(&block).iter() {
        insert_entry_in_inscriptions(
            inscription_data,
            &block.block_identifier,
            inscriptions_db_conn_rw,
            &ctx,
        );
        let (tx, output_index, offset) =
            parse_satpoint_to_watch(&inscription_data.satpoint_post_inscription);
        let outpoint_to_watch = format_outpoint_to_watch(&tx, output_index);
        let _ = locations_to_insert.insert(
            (inscription_data.ordinal_number, outpoint_to_watch),
            OrdinalLocation {
                offset,
                block_height: block.block_identifier.index,
                tx_index: inscription_data.tx_index,
            },
        );
    }

    for transfer_data in get_inscriptions_transferred_in_block(&block).iter() {
        let (tx, output_index, offset) =
            parse_satpoint_to_watch(&transfer_data.satpoint_post_transfer);
        let outpoint_to_watch = format_outpoint_to_watch(&tx, output_index);
        let _ = locations_to_insert.insert(
            (transfer_data.ordinal_number, outpoint_to_watch),
            OrdinalLocation {
                offset,
                block_height: block.block_identifier.index,
                tx_index: transfer_data.tx_index,
            },
        );
    }

    for ((ordinal_number, outpoint_to_watch), location_data) in locations_to_insert {
        insert_ordinal_transfer_in_locations_tx(
            ordinal_number,
            &outpoint_to_watch,
            location_data,
            &inscriptions_db_conn_rw,
            ctx,
        );
    }
}

pub fn update_sequence_metadata_with_block(
    block: &BitcoinBlockData,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    let mut nth_classic_pos_number = find_nth_classic_pos_number_at_block_height(
        &block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    )
    .unwrap_or(0);
    let mut nth_classic_neg_number = find_nth_classic_neg_number_at_block_height(
        &block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    )
    .unwrap_or(0);
    let mut nth_jubilee_number = find_nth_jubilee_number_at_block_height(
        &block.block_identifier.index,
        inscriptions_db_conn_rw,
        ctx,
    )
    .unwrap_or(0);
    for inscription_data in get_inscriptions_revealed_in_block(&block).iter() {
        nth_classic_pos_number =
            nth_classic_pos_number.max(inscription_data.inscription_number.classic);
        nth_classic_neg_number =
            nth_classic_neg_number.min(inscription_data.inscription_number.classic);
        nth_jubilee_number = nth_jubilee_number.max(inscription_data.inscription_number.jubilee);
    }
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO sequence_metadata (block_height, nth_classic_pos_number, nth_classic_neg_number, nth_jubilee_number) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![&block.block_identifier.index, nth_classic_pos_number, nth_classic_neg_number, nth_jubilee_number],
    ) {
        try_warn!(ctx, "unable to update sequence_metadata: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn insert_ordinal_transfer_in_locations_tx(
    ordinal_number: u64,
    outpoint_to_watch: &str,
    data: OrdinalLocation,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    let mut retry = 0;
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "INSERT INTO locations (ordinal_number, outpoint_to_watch, offset, block_height, tx_index) VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![&ordinal_number, &outpoint_to_watch, data.offset, data.block_height, &data.tx_index],
    ) {
        retry += 1;
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
        if retry > 2 {
            try_error!(ctx, "unable to insert inscription in location in hord.sqlite: {}", e.to_string());
            return
        }
    }
}

pub fn perform_query_exists(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
) -> bool {
    let res = perform_query(query, args, db_conn, ctx, |_| true, true);
    !res.is_empty()
}

pub fn perform_query_one<F, T>(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
    mapping_func: F,
) -> Option<T>
where
    F: Fn(&rusqlite::Row<'_>) -> T,
{
    let mut res = perform_query(query, args, db_conn, ctx, mapping_func, true);
    match res.is_empty() {
        true => None,
        false => Some(res.remove(0)),
    }
}

pub fn perform_query_set<F, T>(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
    mapping_func: F,
) -> Vec<T>
where
    F: Fn(&rusqlite::Row<'_>) -> T,
{
    perform_query(query, args, db_conn, ctx, mapping_func, false)
}

fn perform_query<F, T>(
    query: &str,
    args: &[&dyn ToSql],
    db_conn: &Connection,
    ctx: &Context,
    mapping_func: F,
    stop_at_first: bool,
) -> Vec<T>
where
    F: Fn(&rusqlite::Row<'_>) -> T,
{
    let mut results = vec![];
    loop {
        let mut stmt = match db_conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                try_warn!(ctx, "unable to prepare query {query}: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };

        match stmt.query(args) {
            Ok(mut rows) => loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        let r = mapping_func(row);
                        results.push(r);
                        if stop_at_first {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        try_warn!(
                            ctx,
                            "unable to iterate over results from {query}: {}",
                            e.to_string()
                        );
                        std::thread::sleep(std::time::Duration::from_secs(5));
                        continue;
                    }
                }
            },
            Err(e) => {
                try_warn!(ctx, "unable to execute query {query}: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(5));
                continue;
            }
        };
        break;
    }
    results
}

pub fn find_latest_inscription_block_height(
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<u64>, String> {
    let args: &[&dyn ToSql] = &[];
    let query = "SELECT block_height FROM sequence_metadata ORDER BY block_height DESC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let block_height: u64 = row.get(0).unwrap();
        block_height
    });
    Ok(entry)
}

pub fn find_initial_inscription_transfer_data(
    ordinal_number: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<TransferData>, String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "SELECT outpoint_to_watch, offset, tx_index FROM locations WHERE ordinal_number = ? ORDER BY block_height ASC, tx_index ASC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let outpoint_to_watch: String = row.get(0).unwrap();
        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        let tx_index: u64 = row.get(2).unwrap();
        TransferData {
            transaction_identifier_location,
            output_index,
            inscription_offset_intra_output,
            tx_index,
        }
    });
    Ok(entry)
}

pub fn find_latest_inscription_transfer_data(
    ordinal_number: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<TransferData>, String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "SELECT outpoint_to_watch, offset, tx_index FROM locations WHERE ordinal_number = ? ORDER BY block_height DESC, tx_index DESC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let outpoint_to_watch: String = row.get(0).unwrap();
        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let inscription_offset_intra_output: u64 = row.get(1).unwrap();
        let tx_index: u64 = row.get(2).unwrap();
        TransferData {
            transaction_identifier_location,
            output_index,
            inscription_offset_intra_output,
            tx_index,
        }
    });
    Ok(entry)
}

pub fn find_latest_transfers_block_height(db_conn: &Connection, ctx: &Context) -> Option<u64> {
    let args: &[&dyn ToSql] = &[];
    let query = "SELECT block_height FROM locations ORDER BY block_height DESC LIMIT 1";
    let entry = perform_query_one(query, args, db_conn, ctx, |row| {
        let block_height: u64 = row.get(0).unwrap();
        block_height
    });
    entry
}

pub fn get_latest_indexed_inscription_number(db_conn: &Connection, ctx: &Context) -> Option<u64> {
    let args: &[&dyn ToSql] = &[];
    let query = "SELECT COALESCE(MAX(jubilee_inscription_number), 0) FROM inscriptions";
    perform_query_one(query, args, db_conn, ctx, |row| row.get(0).unwrap())
}

#[derive(Debug, Clone)]
pub struct TransferData {
    pub inscription_offset_intra_output: u64,
    pub transaction_identifier_location: TransactionIdentifier,
    pub output_index: usize,
    pub tx_index: u64,
}

pub fn find_all_transfers_in_block(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> BTreeMap<u64, Vec<TransferData>> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];

    let mut stmt = loop {
        match db_conn.prepare("SELECT ordinal_number, offset, outpoint_to_watch, tx_index FROM locations WHERE block_height = ? ORDER BY tx_index ASC")
        {
            Ok(stmt) => break stmt,
            Err(e) => {
                try_warn!(ctx, "unable to prepare query hord.sqlite: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };

    let mut results: BTreeMap<u64, Vec<TransferData>> = BTreeMap::new();
    let mut rows = loop {
        match stmt.query(args) {
            Ok(rows) => break rows,
            Err(e) => {
                try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                let ordinal_number: u64 = row.get(0).unwrap();
                let inscription_offset_intra_output: u64 = row.get(1).unwrap();
                let outpoint_to_watch: String = row.get(2).unwrap();
                let tx_index: u64 = row.get(3).unwrap();
                let (transaction_identifier_location, output_index) =
                    parse_outpoint_to_watch(&outpoint_to_watch);
                let transfer = TransferData {
                    inscription_offset_intra_output,
                    transaction_identifier_location,
                    output_index,
                    tx_index,
                };
                results
                    .entry(ordinal_number)
                    .and_modify(|v| v.push(transfer.clone()))
                    .or_insert(vec![transfer]);
            }
            Ok(None) => break,
            Err(e) => {
                try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
    return results;
}

pub fn find_all_inscription_transfers(
    inscription_id: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Vec<(TransferData, u64)> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let query = "SELECT offset, outpoint_to_watch, tx_index, block_height FROM locations WHERE inscription_id = ? ORDER BY block_height ASC, tx_index ASC";
    perform_query_set(query, args, db_conn, ctx, |row| {
        let inscription_offset_intra_output: u64 = row.get(0).unwrap();
        let outpoint_to_watch: String = row.get(1).unwrap();
        let tx_index: u64 = row.get(2).unwrap();
        let block_height: u64 = row.get(3).unwrap();

        let (transaction_identifier_location, output_index) =
            parse_outpoint_to_watch(&outpoint_to_watch);
        let transfer = TransferData {
            inscription_offset_intra_output,
            transaction_identifier_location,
            output_index,
            tx_index,
        };
        (transfer, block_height)
    })
}

pub fn find_nth_classic_pos_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT nth_classic_pos_number FROM sequence_metadata WHERE block_height < ? ORDER BY block_height DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
    .or_else(|| compute_nth_classic_pos_number_at_block_height(block_height, db_conn, ctx))
}

pub fn find_nth_classic_neg_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT nth_classic_neg_number FROM sequence_metadata WHERE block_height < ? ORDER BY block_height DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
    .or_else(|| compute_nth_classic_neg_number_at_block_height(block_height, db_conn, ctx))
}

pub fn find_nth_jubilee_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT nth_jubilee_number FROM sequence_metadata WHERE block_height < ? ORDER BY block_height DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
    .or_else(|| compute_nth_jubilee_number_at_block_height(block_height, db_conn, ctx))
}

pub fn compute_nth_jubilee_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    try_warn!(
        ctx,
        "Start computing latest_inscription_number at block height: {block_height}"
    );
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT jubilee_inscription_number FROM inscriptions WHERE block_height < ? ORDER BY jubilee_inscription_number DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
}

pub fn compute_nth_classic_pos_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    try_warn!(
        ctx,
        "Start computing latest_inscription_number at block height: {block_height}"
    );
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT classic_inscription_number FROM inscriptions WHERE block_height < ? ORDER BY classic_inscription_number DESC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
}

pub fn compute_nth_classic_neg_number_at_block_height(
    block_height: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<i64> {
    try_warn!(
        ctx,
        "Start computing nth_classic_neg_number at block height: {block_height}"
    );
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];
    let query = "SELECT classic_inscription_number FROM inscriptions WHERE block_height < ? ORDER BY classic_inscription_number ASC LIMIT 1";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_number: i64 = row.get(0).unwrap();
        inscription_number
    })
}

pub fn find_blessed_inscription_with_ordinal_number(
    ordinal_number: &u64,
    db_conn: &Connection,
    ctx: &Context,
) -> Option<String> {
    let args: &[&dyn ToSql] = &[&ordinal_number.to_sql().unwrap()];
    let query = "SELECT inscription_id FROM inscriptions WHERE ordinal_number = ? AND classic_inscription_number >= 0";
    perform_query_one(query, args, db_conn, ctx, |row| {
        let inscription_id: String = row.get(0).unwrap();
        inscription_id
    })
}

pub fn find_inscription_with_id(
    inscription_id: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Result<Option<(TraversalResult, u64)>, String> {
    let args: &[&dyn ToSql] = &[&inscription_id.to_sql().unwrap()];
    let query = "SELECT classic_inscription_number, jubilee_inscription_number, ordinal_number, block_height, input_index FROM inscriptions WHERE inscription_id = ?";
    let entry = perform_query_one(query, args, db_conn, ctx, move |row| {
        let inscription_number = OrdinalInscriptionNumber {
            classic: row.get(0).unwrap(),
            jubilee: row.get(1).unwrap(),
        };
        let ordinal_number: u64 = row.get(2).unwrap();
        let block_height: u64 = row.get(3).unwrap();
        let inscription_input_index: usize = row.get(4).unwrap();
        let (transaction_identifier_inscription, _) = parse_inscription_id(inscription_id);
        (
            inscription_number,
            ordinal_number,
            inscription_input_index,
            transaction_identifier_inscription,
            block_height,
        )
    });

    let Some((
        inscription_number,
        ordinal_number,
        inscription_input_index,
        transaction_identifier_inscription,
        block_height,
    )) = entry
    else {
        return Err(format!(
            "unable to retrieve inscription for {inscription_id}"
        ));
    };

    Ok(Some((
        TraversalResult {
            inscription_number,
            ordinal_number,
            inscription_input_index,
            transaction_identifier_inscription,
            transfers: 0,
        },
        block_height,
    )))
}

pub fn find_all_inscriptions_in_block(
    block_height: &u64,
    inscriptions_db_tx: &Connection,
    ctx: &Context,
) -> BTreeMap<String, TraversalResult> {
    let args: &[&dyn ToSql] = &[&block_height.to_sql().unwrap()];

    let mut stmt = loop {
        match inscriptions_db_tx.prepare("SELECT classic_inscription_number, jubilee_inscription_number, ordinal_number, inscription_id, input_index FROM inscriptions where block_height = ?")
        {
            Ok(stmt) => break stmt,
            Err(e) => {
                try_warn!(ctx, "unable to prepare query hord.sqlite: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };

    let mut rows = loop {
        match stmt.query(args) {
            Ok(rows) => break rows,
            Err(e) => {
                try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    };
    let mut results = BTreeMap::new();
    loop {
        match rows.next() {
            Ok(Some(row)) => {
                let inscription_number = OrdinalInscriptionNumber {
                    classic: row.get(0).unwrap(),
                    jubilee: row.get(1).unwrap(),
                };
                let ordinal_number: u64 = row.get(2).unwrap();
                let inscription_id: String = row.get(3).unwrap();
                let inscription_input_index: usize = row.get(4).unwrap();
                let (transaction_identifier_inscription, _) =
                    { parse_inscription_id(&inscription_id) };
                let traversal = TraversalResult {
                    inscription_number,
                    ordinal_number,
                    inscription_input_index,
                    transfers: 0,
                    transaction_identifier_inscription: transaction_identifier_inscription.clone(),
                };
                results.insert(inscription_id, traversal);
            }
            Ok(None) => break,
            Err(e) => {
                try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
    }
    return results;
}

#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct WatchedSatpoint {
    pub ordinal_number: u64,
    pub offset: u64,
}

pub fn find_inscribed_ordinals_at_wached_outpoint(
    outpoint: &str,
    db_conn: &Connection,
    ctx: &Context,
) -> Vec<WatchedSatpoint> {
    let args: &[&dyn ToSql] = &[&outpoint.to_sql().unwrap()];
    let query = "SELECT ordinal_number, offset FROM locations WHERE outpoint_to_watch = ? ORDER BY offset ASC";
    perform_query_set(query, args, db_conn, ctx, |row| {
        let ordinal_number: u64 = row.get(0).unwrap();
        let offset: u64 = row.get(1).unwrap();
        WatchedSatpoint {
            ordinal_number,
            offset,
        }
    })
}

pub fn delete_inscriptions_in_block_range(
    start_block: u32,
    end_block: u32,
    inscriptions_db_conn_rw: &Connection,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM inscriptions WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM locations WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = inscriptions_db_conn_rw.execute(
        "DELETE FROM sequence_metadata WHERE block_height >= ?1 AND block_height <= ?2",
        rusqlite::params![&start_block, &end_block],
    ) {
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn remove_entry_from_inscriptions(
    inscription_id: &str,
    inscriptions_db_rw_conn: &Connection,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM inscriptions WHERE inscription_id = ?1",
        rusqlite::params![&inscription_id],
    ) {
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM locations WHERE inscription_id = ?1",
        rusqlite::params![&inscription_id],
    ) {
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

pub fn remove_entries_from_locations_at_block_height(
    block_height: &u64,
    inscriptions_db_rw_conn: &Transaction,
    ctx: &Context,
) {
    while let Err(e) = inscriptions_db_rw_conn.execute(
        "DELETE FROM locations WHERE block_height = ?1",
        rusqlite::params![&block_height],
    ) {
        try_warn!(ctx, "unable to query hord.sqlite: {}", e.to_string());
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
