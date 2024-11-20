use std::collections::{BTreeMap, HashMap};

use chainhook_postgres::{
    deadpool_postgres::GenericClient,
    tokio_postgres::{types::ToSql, Client},
    types::{PgBigIntU32, PgNumericU64},
    utils,
};
use chainhook_sdk::types::{
    bitcoin::TxIn, BitcoinBlockData, OrdinalInscriptionNumber, OrdinalOperation,
    TransactionIdentifier,
};
use refinery::embed_migrations;

use crate::{
    core::protocol::{satoshi_numbering::TraversalResult, satoshi_tracking::WatchedSatpoint},
    utils::format_outpoint_to_watch,
};

use super::models::{
    DbCurrentLocation, DbInscription, DbInscriptionRecursion, DbLocation, DbSatoshi,
};

embed_migrations!("../../migrations/ordinals");
pub async fn migrate(client: &mut Client) -> Result<(), String> {
    return match migrations::runner()
        .set_migration_table_name("pgmigrations")
        .run_async(client)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Error running pg migrations: {e}")),
    };
}

pub async fn get_chain_tip_block_height<T: GenericClient>(
    client: &T,
) -> Result<Option<u64>, String> {
    let row = client
        .query_opt("SELECT block_height FROM chain_tip", &[])
        .await
        .map_err(|e| format!("get_chain_tip_block_height: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let max: Option<PgNumericU64> = row.get("block_height");
    Ok(max.map(|v| v.0))
}

pub async fn get_highest_inscription_number<T: GenericClient>(
    client: &T,
) -> Result<Option<i64>, String> {
    let row = client
        .query_opt("SELECT MAX(number) AS max FROM inscriptions", &[])
        .await
        .map_err(|e| format!("get_highest_inscription_number: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let max: Option<i64> = row.get("max");
    Ok(max)
}

pub async fn get_highest_blessed_classic_inscription_number<T: GenericClient>(
    client: &T,
) -> Result<Option<i64>, String> {
    let row = client
        .query_opt(
            "SELECT MAX(classic_number) AS max FROM inscriptions WHERE classic_number >= 0",
            &[],
        )
        .await
        .map_err(|e| format!("get_highest_blessed_classic_inscription_number: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let max: Option<i64> = row.get("max");
    Ok(max)
}

pub async fn get_lowest_cursed_classic_inscription_number<T: GenericClient>(
    client: &T,
) -> Result<Option<i64>, String> {
    let row = client
        .query_opt(
            "SELECT MIN(classic_number) AS min FROM inscriptions WHERE classic_number < 0",
            &[],
        )
        .await
        .map_err(|e| format!("get_lowest_cursed_classic_inscription_number: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let min: Option<i64> = row.get("min");
    Ok(min)
}

pub async fn get_blessed_inscription_id_for_ordinal_number<T: GenericClient>(
    client: &T,
    ordinal_number: u64,
) -> Result<Option<String>, String> {
    let row = client
        .query_opt("SELECT inscription_id FROM inscriptions WHERE ordinal_number = $1 AND classic_number >= 0", &[&PgNumericU64(ordinal_number)])
        .await
        .map_err(|e| format!("get_blessed_inscription_id_for_ordinal_number: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let id: Option<String> = row.get("inscription_id");
    Ok(id)
}

pub async fn has_ordinal_activity_at_block<T: GenericClient>(
    client: &T,
    block_height: u64,
) -> Result<bool, String> {
    let row = client
        .query_opt(
            "SELECT 1 FROM locations WHERE block_height = $1 LIMIT 1",
            &[&PgNumericU64(block_height)],
        )
        .await
        .map_err(|e| format!("has_ordinal_activity_at_block: {e}"))?;
    Ok(row.is_some())
}

pub async fn get_inscriptions_at_block<T: GenericClient>(
    client: &T,
    block_height: u64,
) -> Result<BTreeMap<String, TraversalResult>, String> {
    let rows = client
        .query(
            "SELECT number, classic_number, ordinal_number, inscription_id, input_index, tx_id
            FROM inscriptions
            WHERE block_height = $1",
            &[&PgNumericU64(block_height)],
        )
        .await
        .map_err(|e| format!("get_inscriptions_at_block: {e}"))?;
    let mut results = BTreeMap::new();
    for row in rows.iter() {
        let inscription_number = OrdinalInscriptionNumber {
            classic: row.get("classic_number"),
            jubilee: row.get("number"),
        };
        let ordinal_number: PgNumericU64 = row.get("ordinal_number");
        let inscription_id: String = row.get("inscription_id");
        let inscription_input_index: PgBigIntU32 = row.get("input_index");
        let tx_id: String = row.get("tx_id");
        let traversal = TraversalResult {
            inscription_number,
            ordinal_number: ordinal_number.0,
            inscription_input_index: inscription_input_index.0 as usize,
            transfers: 0,
            transaction_identifier_inscription: TransactionIdentifier { hash: tx_id },
        };
        results.insert(inscription_id, traversal);
    }
    Ok(results)
}

pub async fn get_inscribed_satpoints_at_tx_inputs<T: GenericClient>(
    inputs: &Vec<TxIn>,
    client: &T,
) -> Result<HashMap<usize, Vec<WatchedSatpoint>>, String> {
    let mut results = HashMap::new();
    for chunk in inputs.chunks(500) {
        let outpoints: Vec<(String, String)> = chunk
            .iter()
            .enumerate()
            .map(|(vin, input)| {
                (
                    vin.to_string(),
                    format_outpoint_to_watch(
                        &input.previous_output.txid,
                        input.previous_output.vout as usize,
                    ),
                )
            })
            .collect();
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for (vin, input) in outpoints.iter() {
            params.push(vin);
            params.push(input);
        }
        let rows = client
            .query(
                &format!(
                    "WITH inputs (vin, output) AS (VALUES {})
                    SELECT i.vin, l.ordinal_number, l.\"offset\"
                    FROM current_locations AS l
                    INNER JOIN inputs AS i ON i.output = l.output",
                    utils::multi_row_query_param_str(chunk.len(), 2)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("get_inscriptions_at_tx_inputs: {e}"))?;
        for row in rows.iter() {
            let vin: String = row.get("vin");
            let vin_key = vin.parse::<usize>().unwrap();
            let ordinal_number: PgNumericU64 = row.get("ordinal_number");
            let offset: PgNumericU64 = row.get("offset");
            let entry = results.entry(vin_key).or_insert(vec![]);
            entry.push(WatchedSatpoint {
                ordinal_number: ordinal_number.0,
                offset: offset.0,
            });
        }
    }
    Ok(results)
}

async fn insert_inscriptions<T: GenericClient>(
    inscriptions: &Vec<DbInscription>,
    client: &T,
) -> Result<(), String> {
    if inscriptions.len() == 0 {
        return Ok(());
    }
    for chunk in inscriptions.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.inscription_id);
            params.push(&row.ordinal_number);
            params.push(&row.number);
            params.push(&row.classic_number);
            params.push(&row.block_height);
            params.push(&row.block_hash);
            params.push(&row.tx_index);
            params.push(&row.address);
            params.push(&row.mime_type);
            params.push(&row.content_type);
            params.push(&row.content_length);
            params.push(&row.content);
            params.push(&row.fee);
            params.push(&row.curse_type);
            params.push(&row.recursive);
            params.push(&row.input_index);
            params.push(&row.pointer);
            params.push(&row.metadata);
            params.push(&row.metaprotocol);
            params.push(&row.parent);
            params.push(&row.delegate);
            params.push(&row.timestamp);
        }
        client
            .query(
                &format!("INSERT INTO inscriptions
                    (inscription_id, ordinal_number, number, classic_number, block_height, block_hash, tx_index, address,
                    mime_type, content_type, content_length, content, fee, curse_type, recursive, input_index, pointer, metadata,
                    metaprotocol, parent, delegate, timestamp)
                    VALUES {}
                    ON CONFLICT (number) DO NOTHING", utils::multi_row_query_param_str(chunk.len(), 22)),
                &params,
            )
            .await
            .map_err(|e| format!("insert_inscriptions: {e}"))?;
    }
    Ok(())
}

async fn insert_inscription_recursions<T: GenericClient>(
    inscription_recursions: &Vec<DbInscriptionRecursion>,
    client: &T,
) -> Result<(), String> {
    if inscription_recursions.len() == 0 {
        return Ok(());
    }
    for chunk in inscription_recursions.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.inscription_id);
            params.push(&row.ref_inscription_id);
        }
        client
            .query(
                &format!(
                    "INSERT INTO inscription_recursions
                    (inscription_id, ref_inscription_id)
                    VALUES {}
                    ON CONFLICT (inscription_id, ref_inscription_id) DO NOTHING",
                    utils::multi_row_query_param_str(chunk.len(), 2)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("insert_inscription_recursions: {e}"))?;
    }
    Ok(())
}

async fn insert_locations<T: GenericClient>(
    locations: &Vec<DbLocation>,
    client: &T,
) -> Result<(), String> {
    if locations.len() == 0 {
        return Ok(());
    }
    for chunk in locations.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.ordinal_number);
            params.push(&row.block_height);
            params.push(&row.tx_index);
            params.push(&row.tx_id);
            params.push(&row.block_hash);
            params.push(&row.address);
            params.push(&row.output);
            params.push(&row.offset);
            params.push(&row.prev_output);
            params.push(&row.prev_offset);
            params.push(&row.value);
            params.push(&row.transfer_type);
            params.push(&row.timestamp);
        }
        client
            .query(
                &format!(
                    "WITH location_inserts AS (
                        INSERT INTO locations (ordinal_number, block_height, tx_index, tx_id, block_hash, address, output,
                            \"offset\", prev_output, prev_offset, value, transfer_type, timestamp)
                        VALUES {}
                        ON CONFLICT (ordinal_number, block_height, tx_index) DO NOTHING
                        RETURNING ordinal_number, block_height, block_hash, tx_index
                    ),
                    prev_transfer_index AS (
                        SELECT MAX(block_transfer_index) AS max
                        FROM inscription_transfers
                        WHERE block_height = (SELECT block_height FROM location_inserts LIMIT 1)
                    ),
                    moved_inscriptions AS (
                        SELECT
                        i.inscription_id, i.number, i.ordinal_number, li.block_height, li.block_hash, li.tx_index,
                        (
                            ROW_NUMBER() OVER (ORDER BY li.block_height ASC, li.tx_index ASC) + (SELECT COALESCE(max, -1) FROM prev_transfer_index)
                        ) AS block_transfer_index
                        FROM inscriptions AS i
                        INNER JOIN location_inserts AS li ON li.ordinal_number = i.ordinal_number
                        WHERE i.block_height < li.block_height OR (i.block_height = li.block_height AND i.tx_index < li.tx_index)
                    )
                    INSERT INTO inscription_transfers
                        (inscription_id, number, ordinal_number, block_height, block_hash, tx_index, block_transfer_index)
                        (SELECT * FROM moved_inscriptions)
                        ON CONFLICT (block_height, block_transfer_index) DO NOTHING",
                    utils::multi_row_query_param_str(chunk.len(), 13)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("insert_locations: {e}"))?;
    }
    Ok(())
}

async fn insert_satoshis<T: GenericClient>(
    satoshis: &Vec<DbSatoshi>,
    client: &T,
) -> Result<(), String> {
    if satoshis.len() == 0 {
        return Ok(());
    }
    for chunk in satoshis.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.ordinal_number);
            params.push(&row.rarity);
            params.push(&row.coinbase_height);
        }
        client
            .query(
                &format!(
                    "INSERT INTO satoshis
                    (ordinal_number, rarity, coinbase_height)
                    VALUES {}
                    ON CONFLICT (ordinal_number) DO NOTHING",
                    utils::multi_row_query_param_str(chunk.len(), 3)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("insert_satoshis: {e}"))?;
    }
    Ok(())
}

async fn insert_current_locations<T: GenericClient>(
    current_locations: &HashMap<PgNumericU64, DbCurrentLocation>,
    client: &T,
) -> Result<(), String> {
    let moved_sats: Vec<&PgNumericU64> = current_locations.keys().collect();
    let new_locations: Vec<&DbCurrentLocation> = current_locations.values().collect();
    // Deduct counts from previous owners
    for chunk in moved_sats.chunks(500) {
        let c = chunk.to_vec();
        client
            .query(
                "WITH prev_owners AS (
                    SELECT address, COUNT(*) AS count
                    FROM current_locations
                    WHERE ordinal_number = ANY ($1)
                    GROUP BY address
                )
                UPDATE counts_by_address
                SET count = (
                    SELECT counts_by_address.count - p.count
                    FROM prev_owners AS p
                    WHERE p.address = counts_by_address.address
                )
                WHERE EXISTS (SELECT 1 FROM prev_owners AS p WHERE p.address = counts_by_address.address)",
                &[&c],
            )
            .await
            .map_err(|e| format!("insert_current_locations: {e}"))?;
    }
    // Insert locations
    for chunk in new_locations.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.ordinal_number);
            params.push(&row.block_height);
            params.push(&row.tx_id);
            params.push(&row.tx_index);
            params.push(&row.address);
            params.push(&row.output);
            params.push(&row.offset);
        }
        client
            .query(
                &format!(
                    "INSERT INTO current_locations (ordinal_number, block_height, tx_id, tx_index, address, output, \"offset\")
                    VALUES {}
                    ON CONFLICT (ordinal_number) DO UPDATE SET
                        block_height = EXCLUDED.block_height,
                        tx_index = EXCLUDED.tx_index,
                        address = EXCLUDED.address
                    WHERE
                        EXCLUDED.block_height > current_locations.block_height OR
                        (EXCLUDED.block_height = current_locations.block_height AND
                            EXCLUDED.tx_index > current_locations.tx_index)",
                    utils::multi_row_query_param_str(chunk.len(), 7)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("insert_current_locations: {e}"))?;
    }
    // Update owner counts
    for chunk in moved_sats.chunks(500) {
        let c = chunk.to_vec();
        client
            .query(
                "WITH new_owners AS (
                    SELECT address, COUNT(*) AS count
                    FROM current_locations
                    WHERE ordinal_number = ANY ($1)
                    GROUP BY address
                )
                INSERT INTO counts_by_address (address, count)
                (SELECT address, count FROM new_owners)
                ON CONFLICT (address) DO UPDATE SET count = counts_by_address.count + EXCLUDED.count",
                &[&c],
            )
            .await
            .map_err(|e| format!("insert_current_locations: {e}"))?;
    }
    Ok(())
}

async fn update_mime_type_counts<T: GenericClient>(
    counts: &HashMap<String, i32>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    for (key, value) in counts {
        params.push(key);
        params.push(value);
    }
    client
        .query(
            &format!(
                "INSERT INTO counts_by_mime_type (mime_type, count) VALUES {}
                ON CONFLICT (mime_type) DO UPDATE SET count = counts_by_mime_type.count + EXCLUDED.count",
                utils::multi_row_query_param_str(counts.len(), 2)
            ),
            &params,
        )
        .await
        .map_err(|e| format!("update_mime_type_counts: {e}"))?;
    Ok(())
}

async fn update_sat_rarity_counts<T: GenericClient>(
    counts: &HashMap<String, i32>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    for (key, value) in counts {
        params.push(key);
        params.push(value);
    }
    client
        .query(
            &format!(
                "INSERT INTO counts_by_sat_rarity (rarity, count) VALUES {}
                ON CONFLICT (rarity) DO UPDATE SET count = counts_by_sat_rarity.count + EXCLUDED.count",
                utils::multi_row_query_param_str(counts.len(), 2)
            ),
            &params,
        )
        .await
        .map_err(|e| format!("update_sat_rarity_counts: {e}"))?;
    Ok(())
}

async fn update_inscription_type_counts<T: GenericClient>(
    counts: &HashMap<String, i32>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    for (key, value) in counts {
        params.push(key);
        params.push(value);
    }
    client
        .query(
            &format!(
                "INSERT INTO counts_by_type (type, count) VALUES {}
                ON CONFLICT (type) DO UPDATE SET count = counts_by_type.count + EXCLUDED.count",
                utils::multi_row_query_param_str(counts.len(), 2)
            ),
            &params,
        )
        .await
        .map_err(|e| format!("update_inscription_type_counts: {e}"))?;
    Ok(())
}

async fn update_genesis_address_counts<T: GenericClient>(
    counts: &HashMap<String, i32>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    for (key, value) in counts {
        params.push(key);
        params.push(value);
    }
    client
        .query(
            &format!(
                "INSERT INTO counts_by_genesis_address (address, count) VALUES {}
                ON CONFLICT (address) DO UPDATE SET count = counts_by_genesis_address.count + EXCLUDED.count",
                utils::multi_row_query_param_str(counts.len(), 2)
            ),
            &params,
        )
        .await
        .map_err(|e| format!("update_genesis_address_counts: {e}"))?;
    Ok(())
}

async fn update_recursive_counts<T: GenericClient>(
    counts: &HashMap<bool, i32>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
    for (key, value) in counts {
        params.push(key);
        params.push(value);
    }
    client
        .query(
            &format!(
                "INSERT INTO counts_by_recursive (recursive, count) VALUES {}
                ON CONFLICT (recursive) DO UPDATE SET count = counts_by_recursive.count + EXCLUDED.count",
                utils::multi_row_query_param_str(counts.len(), 2)
            ),
            &params,
        )
        .await
        .map_err(|e| format!("update_recursive_counts: {e}"))?;
    Ok(())
}

async fn update_counts_by_block<T: GenericClient>(
    block_height: u64,
    block_hash: &String,
    inscription_count: usize,
    timestamp: u32,
    client: &T,
) -> Result<(), String> {
    client
        .query(
        "WITH prev_entry AS (
                SELECT inscription_count_accum
                FROM counts_by_block
                WHERE block_height < $1
                ORDER BY block_height DESC
                LIMIT 1
            )
            INSERT INTO counts_by_block (block_height, block_hash, inscription_count, inscription_count_accum, timestamp)
            VALUES ($1, $2, $3, COALESCE((SELECT inscription_count_accum FROM prev_entry), 0) + $3, $4)",
            &[&PgNumericU64(block_height), block_hash, &(inscription_count as i32), &PgBigIntU32(timestamp)],
        )
        .await
        .map_err(|e| format!("update_counts_by_block: {e}"))?;
    Ok(())
}

pub async fn update_chain_tip<T: GenericClient>(
    block_height: u64,
    client: &T,
) -> Result<(), String> {
    client
        .query(
            "UPDATE chain_tip SET block_height = $1",
            &[&PgNumericU64(block_height)],
        )
        .await
        .map_err(|e| format!("update_chain_tip: {e}"))?;
    Ok(())
}

pub async fn insert_block<T: GenericClient>(
    block: &BitcoinBlockData,
    client: &T,
) -> Result<(), String> {
    let mut satoshis = vec![];
    let mut inscriptions = vec![];
    let mut locations = vec![];
    let mut inscription_recursions = vec![];
    let mut current_locations: HashMap<PgNumericU64, DbCurrentLocation> = HashMap::new();
    let mut mime_type_counts = HashMap::new();
    let mut sat_rarity_counts = HashMap::new();
    let mut inscription_type_counts = HashMap::new();
    let mut genesis_address_counts = HashMap::new();
    let mut recursive_counts = HashMap::new();

    let mut update_current_location =
        |ordinal_number: PgNumericU64, new_location: DbCurrentLocation| match current_locations
            .get(&ordinal_number)
        {
            Some(current_location) => {
                if new_location.block_height > current_location.block_height
                    || (new_location.block_height == current_location.block_height
                        && new_location.tx_index > current_location.tx_index)
                {
                    current_locations.insert(ordinal_number, new_location);
                }
            }
            None => {
                current_locations.insert(ordinal_number, new_location);
            }
        };
    for (tx_index, tx) in block.transactions.iter().enumerate() {
        for operation in tx.metadata.ordinal_operations.iter() {
            match operation {
                OrdinalOperation::InscriptionRevealed(reveal) => {
                    let mut inscription = DbInscription::from_reveal(
                        reveal,
                        &block.block_identifier,
                        tx_index,
                        block.timestamp,
                    );
                    let mime_type = inscription.mime_type.clone();
                    let genesis_address = inscription.address.clone();
                    let recursions = DbInscriptionRecursion::from_reveal(reveal);
                    let is_recursive = recursions.len() > 0;
                    if is_recursive {
                        inscription.recursive = true;
                    }
                    inscription_recursions.extend(recursions);
                    inscriptions.push(inscription);
                    locations.push(DbLocation::from_reveal(
                        reveal,
                        &block.block_identifier,
                        &tx.transaction_identifier,
                        tx_index,
                        block.timestamp,
                    ));
                    let satoshi = DbSatoshi::from_reveal(reveal);
                    let rarity = satoshi.rarity.clone();
                    satoshis.push(satoshi);
                    update_current_location(
                        PgNumericU64(reveal.ordinal_number),
                        DbCurrentLocation::from_reveal(
                            reveal,
                            &block.block_identifier,
                            &tx.transaction_identifier,
                            tx_index,
                        ),
                    );
                    let inscription_type = if reveal.inscription_number.classic < 0 {
                        "cursed".to_string()
                    } else {
                        "blessed".to_string()
                    };
                    mime_type_counts
                        .entry(mime_type)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);
                    sat_rarity_counts
                        .entry(rarity)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);
                    inscription_type_counts
                        .entry(inscription_type)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);
                    if let Some(genesis_address) = genesis_address {
                        genesis_address_counts
                            .entry(genesis_address)
                            .and_modify(|c| *c += 1)
                            .or_insert(1);
                    }
                    recursive_counts
                        .entry(is_recursive)
                        .and_modify(|c| *c += 1)
                        .or_insert(1);
                }
                OrdinalOperation::InscriptionTransferred(transfer) => {
                    locations.push(DbLocation::from_transfer(
                        transfer,
                        &block.block_identifier,
                        &tx.transaction_identifier,
                        tx_index,
                        block.timestamp,
                    ));
                    update_current_location(
                        PgNumericU64(transfer.ordinal_number),
                        DbCurrentLocation::from_transfer(
                            transfer,
                            &block.block_identifier,
                            &tx.transaction_identifier,
                            tx_index,
                        ),
                    );
                }
            }
        }
    }

    insert_inscriptions(&inscriptions, client).await?;
    insert_inscription_recursions(&inscription_recursions, client).await?;
    insert_locations(&locations, client).await?;
    insert_satoshis(&satoshis, client).await?;
    insert_current_locations(&current_locations, client).await?;
    update_mime_type_counts(&mime_type_counts, client).await?;
    update_sat_rarity_counts(&sat_rarity_counts, client).await?;
    update_inscription_type_counts(&inscription_type_counts, client).await?;
    update_genesis_address_counts(&genesis_address_counts, client).await?;
    update_recursive_counts(&recursive_counts, client).await?;
    update_counts_by_block(
        block.block_identifier.index,
        &block.block_identifier.hash,
        inscriptions.len(),
        block.timestamp,
        client,
    )
    .await?;
    update_chain_tip(block.block_identifier.index, client).await?;

    Ok(())
}
