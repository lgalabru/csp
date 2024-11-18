use std::collections::{BTreeMap, HashMap};

use chainhook_postgres::{
    tokio_postgres::{types::ToSql, Client, GenericClient},
    types::{PgBigIntU32, PgNumericU64},
    utils,
};
use chainhook_sdk::types::{
    bitcoin::TxIn, BitcoinBlockData, OrdinalInscriptionNumber, OrdinalOperation,
    TransactionIdentifier,
};
use refinery::embed_migrations;

use crate::{core::protocol::satoshi_numbering::TraversalResult, utils::format_outpoint_to_watch};

use super::{
    models::{DbInscription, DbInscriptionRecursion, DbLocation},
    ordinals::WatchedSatpoint,
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

async fn insert_locations<T: GenericClient>(locations: &Vec<DbLocation>, client: &T) -> Result<(), String> {
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
                        INSERT INTO locations (ordinal_number, block_height, tx_index, tx_id, block_hash, address, output, offset
                            prev_output, prev_offset, value, transfer_type, timestamp)
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
                        i.genesis_id, i.number, i.ordinal_number, li.block_height, li.block_hash, li.tx_index,
                        (
                            ROW_NUMBER() OVER (ORDER BY li.block_height ASC, li.tx_index ASC) + (SELECT COALESCE(max, -1) FROM prev_transfer_index)
                        ) AS block_transfer_index
                        FROM inscriptions AS i
                        INNER JOIN location_inserts AS li ON li.ordinal_number = i.ordinal_number
                        WHERE i.block_height < li.block_height OR (i.block_height = li.block_height AND i.tx_index < li.tx_index)
                    )
                    INSERT INTO inscription_transfers
                        (genesis_id, number, ordinal_number, block_height, block_hash, tx_index, block_transfer_index)
                        (SELECT * FROM moved_inscriptions)
                        ON CONFLICT (block_height, block_transfer_index) DO NOTHING",
                    utils::multi_row_query_param_str(chunk.len(), 13)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("insert_inscription_recursions: {e}"))?;
    }
    Ok(())
}

pub async fn insert_block<T: GenericClient>(
    block: &BitcoinBlockData,
    client: &T,
) -> Result<(), String> {
    let mut inscriptions = vec![];
    let mut locations = vec![];
    let mut inscription_recursions = vec![];
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
                    let recursions = DbInscriptionRecursion::from_reveal(reveal);
                    if recursions.len() > 0 {
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
                }
                OrdinalOperation::InscriptionTransferred(transfer) => {
                    locations.push(DbLocation::from_transfer(
                        transfer,
                        &block.block_identifier,
                        &tx.transaction_identifier,
                        tx_index,
                        block.timestamp,
                    ));
                }
            }
        }
    }

    insert_inscriptions(&inscriptions, client).await?;
    insert_inscription_recursions(&inscription_recursions, client).await?;
    insert_locations(&locations, client).await?;
    Ok(())
}
