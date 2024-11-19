use std::collections::HashMap;

use chainhook_postgres::{
    tokio_postgres::{types::ToSql, Client, GenericClient},
    types::{PgNumericU128, PgNumericU64},
    utils,
};
use chainhook_sdk::types::{
    BitcoinBlockData, Brc20BalanceData, Brc20Operation, Brc20TokenDeployData, Brc20TransferData,
};
use refinery::embed_migrations;

use super::{
    models::{DbOperation, DbToken},
    u128_amount_to_decimals_str,
};

embed_migrations!("../../migrations/ordinals-brc20");
pub async fn migrate(pg_client: &mut Client) -> Result<(), String> {
    return match migrations::runner()
        .set_migration_table_name("pgmigrations")
        .run_async(pg_client)
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Error running pg migrations: {e}")),
    };
}

pub async fn get_token<T: GenericClient>(
    ticker: &String,
    client: &T,
) -> Result<Option<DbToken>, String> {
    let row = client
        .query_opt("SELECT * FROM tokens WHERE ticker = $1", &[&ticker])
        .await
        .map_err(|e| format!("get_token: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(DbToken::from_pg_row(&row)))
}

pub async fn get_token_minted_supply<T: GenericClient>(
    ticker: &String,
    client: &T,
) -> Result<Option<u128>, String> {
    let row = client
        .query_opt(
            "SELECT minted_supply FROM tokens WHERE ticker = $1",
            &[&ticker],
        )
        .await
        .map_err(|e| format!("get_token_minted_supply: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let supply: PgNumericU128 = row.get("minted_supply");
    Ok(Some(supply.0))
}

pub async fn get_token_available_balance_for_address<T: GenericClient>(
    ticker: &String,
    address: &String,
    client: &T,
) -> Result<Option<u128>, String> {
    let row = client
        .query_opt(
            "SELECT avail_balance FROM balances WHERE ticker = $1 AND address = $2",
            &[&ticker, &address],
        )
        .await
        .map_err(|e| format!("get_token_available_balance_for_address: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    let supply: PgNumericU128 = row.get("avail_balance");
    Ok(Some(supply.0))
}

pub async fn get_unsent_token_transfer<T: GenericClient>(
    ordinal_number: u64,
    client: &T,
) -> Result<Option<DbOperation>, String> {
    let row = client
        .query_opt(
            "SELECT * FROM operations
            WHERE ordinal_number = $1 AND operation = 'transfer'
            AND NOT EXISTS (SELECT 1 FROM operations WHERE ordinal_number = $1 AND operation = 'transfer_send')
            LIMIT 1",
            &[&PgNumericU64(ordinal_number)],
        )
        .await
        .map_err(|e| format!("get_unsent_token_transfer: {e}"))?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(DbOperation::from_pg_row(&row)))
}

pub async fn insert_tokens<T: GenericClient>(
    tokens: &Vec<DbToken>,
    client: &T,
) -> Result<(), String> {
    for chunk in tokens.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.ticker);
            params.push(&row.display_ticker);
            params.push(&row.inscription_id);
            params.push(&row.inscription_number);
            params.push(&row.block_height);
            params.push(&row.block_hash);
            params.push(&row.tx_id);
            params.push(&row.tx_index);
            params.push(&row.address);
            params.push(&row.max);
            params.push(&row.limit);
            params.push(&row.decimals);
            params.push(&row.self_mint);
            params.push(&row.minted_supply);
            params.push(&row.burned_supply);
            params.push(&row.tx_count);
            params.push(&row.timestamp);
        }
        client
            .query(
                &format!("INSERT INTO tokens
                    (ticker, display_ticker, inscription_id, inscription_number, block_height, block_hash, tx_id, tx_index,
                    address, max, \"limit\", decimals, self_mint, minted_supply, burned_supply, tx_count, timestamp)
                    VALUES {}
                    ON CONFLICT (ticker) DO NOTHING", utils::multi_row_query_param_str(chunk.len(), 17)),
                &params,
            )
            .await
            .map_err(|e| format!("insert_tokens: {e}"))?;
    }
    Ok(())
}

pub async fn insert_operations<T: GenericClient>(
    operations: &Vec<DbOperation>,
    client: &T,
) -> Result<(), String> {
    for chunk in operations.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.ticker);
            params.push(&row.operation);
            params.push(&row.inscription_id);
            params.push(&row.ordinal_number);
            params.push(&row.block_height);
            params.push(&row.block_hash);
            params.push(&row.tx_id);
            params.push(&row.tx_index);
            params.push(&row.output);
            params.push(&row.offset);
            params.push(&row.timestamp);
            params.push(&row.address);
            params.push(&row.to_address);
            params.push(&row.amount);
        }
        client
            .query(
                &format!("INSERT INTO operations
                    (ticker, operation, inscription_id, ordinal_number, block_height, block_hash, tx_id, tx_index, output,
                    \"offset\", timestamp, address, to_address, avail_balance, trans_balance)
                    VALUES {}
                    ON CONFLICT (inscription_id, operation) DO NOTHING", utils::multi_row_query_param_str(chunk.len(), 14)),
                &params,
            )
            .await
            .map_err(|e| format!("insert_operations: {e}"))?;
    }
    Ok(())
}

async fn get_operations_at_block<T: GenericClient>(
    block_height: u64,
    client: &T,
) -> Result<HashMap<u64, DbOperation>, String> {
    let rows = client
        .query(
            "SELECT * FROM operations WHERE block_height = $1 AND operation <> 'transfer_receive'",
            &[&PgNumericU64(block_height)],
        )
        .await
        .map_err(|e| format!("get_inscriptions_at_block: {e}"))?;
    let mut map = HashMap::new();
    for row in rows.iter() {
        let tx_index: PgNumericU64 = row.get("tx_index");
        map.insert(tx_index.0, DbOperation::from_pg_row(row));
    }
    Ok(map)
}

pub async fn insert_block_operations<T: GenericClient>(
    block: &mut BitcoinBlockData,
    client: &T,
) -> Result<(), String> {
    // FIXME
    Ok(())
}

/// Adds previously-indexed BRC-20 operation metadata to a `BitcoinBlockData` block.
pub async fn augment_block_with_operations<T: GenericClient>(
    block: &mut BitcoinBlockData,
    client: &T,
) -> Result<(), String> {
    let mut token_map = HashMap::new();
    let mut operation_map = get_operations_at_block(block.block_identifier.index, client).await?;
    for tx in block.transactions.iter_mut() {
        let Some(entry) = operation_map.remove(&(tx.metadata.index as u64)) else {
            continue;
        };
        if token_map.get(&entry.ticker).is_none() {
            let Some(row) = get_token(&entry.ticker, client).await? else {
                unreachable!("BRC-20 token not found when processing operation");
            };
            token_map.insert(entry.ticker.clone(), row);
        }
        let token = token_map
            .get(&entry.ticker)
            .expect("Token not present in map");
        let decimals = token.decimals.0;
        match entry.operation.as_str() {
            "deploy" => {
                tx.metadata.brc20_operation = Some(Brc20Operation::Deploy(Brc20TokenDeployData {
                    tick: token.display_ticker.clone(),
                    max: u128_amount_to_decimals_str(token.max.0, decimals),
                    lim: u128_amount_to_decimals_str(token.limit.0, decimals),
                    dec: token.decimals.0.to_string(),
                    address: token.address.clone(),
                    inscription_id: token.inscription_id.clone(),
                    self_mint: token.self_mint,
                }));
            }
            "mint" => {
                tx.metadata.brc20_operation = Some(Brc20Operation::Mint(Brc20BalanceData {
                    tick: token.display_ticker.clone(),
                    amt: u128_amount_to_decimals_str(entry.amount.0, decimals),
                    address: entry.address.clone(),
                    inscription_id: entry.inscription_id.clone(),
                }));
            }
            "transfer" => {
                tx.metadata.brc20_operation = Some(Brc20Operation::Transfer(Brc20BalanceData {
                    tick: token.display_ticker.clone(),
                    amt: u128_amount_to_decimals_str(entry.amount.0, decimals),
                    address: entry.address.clone(),
                    inscription_id: entry.inscription_id.clone(),
                }));
            }
            "transfer_send" => {
                tx.metadata.brc20_operation =
                    Some(Brc20Operation::TransferSend(Brc20TransferData {
                        tick: token.display_ticker.clone(),
                        amt: u128_amount_to_decimals_str(entry.amount.0, decimals),
                        sender_address: entry.address.clone(),
                        receiver_address: entry.to_address.unwrap().clone(),
                        inscription_id: entry.inscription_id,
                    }));
            }
            // `transfer_receive` ops are not reflected in transaction metadata, they are sent as part of `transfer_send`.
            _ => {}
        }
    }
    Ok(())
}
