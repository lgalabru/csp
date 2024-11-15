use std::collections::BTreeMap;

use chainhook_postgres::{
    tokio_postgres::{Client, GenericClient},
    types::{PgBigIntU32, PgNumericU64},
};
use chainhook_sdk::types::{OrdinalInscriptionNumber, TransactionIdentifier};
use refinery::embed_migrations;

use crate::core::protocol::satoshi_numbering::TraversalResult;

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
