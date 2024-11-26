use std::collections::HashMap;

use chainhook_postgres::{
    deadpool_postgres::GenericClient,
    tokio_postgres::{types::ToSql, Client},
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
    if tokens.len() == 0 {
        return Ok(());
    }
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
            params.push(&row.tx_count);
            params.push(&row.timestamp);
        }
        client
            .query(
                &format!("INSERT INTO tokens
                    (ticker, display_ticker, inscription_id, inscription_number, block_height, block_hash, tx_id, tx_index,
                    address, max, \"limit\", decimals, self_mint, minted_supply, tx_count, timestamp)
                    VALUES {}
                    ON CONFLICT (ticker) DO NOTHING", utils::multi_row_query_param_str(chunk.len(), 16)),
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
    if operations.len() == 0 {
        return Ok(());
    }
    for chunk in operations.chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for row in chunk.iter() {
            params.push(&row.ticker);
            params.push(&row.operation);
            params.push(&row.inscription_id);
            params.push(&row.inscription_number);
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
                // Insert operations and figure out balance changes directly in postgres so we can do direct arithmetic with
                // NUMERIC values.
                &format!(
                    "WITH inserts AS (
                        INSERT INTO operations
                        (ticker, operation, inscription_id, inscription_number, ordinal_number, block_height, block_hash, tx_id,
                        tx_index, output, \"offset\", timestamp, address, to_address, amount)
                        VALUES {}
                        ON CONFLICT (inscription_id, operation) DO NOTHING
                        RETURNING address, ticker, operation, amount
                    ),
                    balance_changes AS (
                        SELECT ticker, address,
                            CASE
                                WHEN operation = 'mint' OR operation = 'transfer_receive' THEN amount
                                WHEN operation = 'transfer' THEN -1 * amount
                                ELSE 0
                            END AS avail_balance,
                            CASE
                                WHEN operation = 'transfer' THEN amount
                                WHEN operation = 'transfer_send' THEN -1 * amount
                                ELSE 0
                            END AS trans_balance,
                            CASE
                                WHEN operation = 'mint' OR operation = 'transfer_receive' THEN amount
                                WHEN operation = 'transfer_send' THEN -1 * amount
                                ELSE 0
                            END AS total_balance
                        FROM inserts
                    ),
                    grouped_balance_changes AS (
                        SELECT ticker, address, SUM(avail_balance) AS avail_balance, SUM(trans_balance) AS trans_balance,
                            SUM(total_balance) AS total_balance
                        FROM balance_changes
                        GROUP BY ticker, address
                    )
                    INSERT INTO balances (ticker, address, avail_balance, trans_balance, total_balance)
                    (SELECT ticker, address, avail_balance, trans_balance, total_balance FROM grouped_balance_changes)
                    ON CONFLICT (ticker, address) DO UPDATE SET
                        avail_balance = balances.avail_balance + EXCLUDED.avail_balance,
                        trans_balance = balances.trans_balance + EXCLUDED.trans_balance,
                        total_balance = balances.total_balance + EXCLUDED.total_balance
                    ", utils::multi_row_query_param_str(chunk.len(), 15)),
                &params,
            )
            .await
            .map_err(|e| format!("insert_operations: {e}"))?;
    }
    Ok(())
}

pub async fn update_operation_counts<T: GenericClient>(
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
                "INSERT INTO counts_by_operation (operation, count) VALUES {}
                ON CONFLICT (operation) DO UPDATE SET count = counts_by_operation.count + EXCLUDED.count",
                utils::multi_row_query_param_str(counts.len(), 2)
            ),
            &params,
        )
        .await
        .map_err(|e| format!("update_operation_counts: {e}"))?;
    Ok(())
}

pub async fn update_address_operation_counts<T: GenericClient>(
    counts: &HashMap<String, HashMap<String, i32>>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    for chunk in counts.keys().collect::<Vec<&String>>().chunks(500) {
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        let mut insert_rows = 0;
        for address in chunk {
            let map = counts.get(*address).unwrap();
            for (operation, value) in map {
                params.push(*address);
                params.push(operation);
                params.push(value);
                insert_rows += 1;
            }
        }
        client
            .query(
                &format!(
                    "INSERT INTO counts_by_address_operation (address, operation, count) VALUES {}
                    ON CONFLICT (address, operation) DO UPDATE SET count = counts_by_address_operation.count + EXCLUDED.count",
                    utils::multi_row_query_param_str(insert_rows, 3)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("update_address_operation_counts: {e}"))?;
    }
    Ok(())
}

pub async fn update_token_operation_counts<T: GenericClient>(
    counts: &HashMap<String, i32>,
    client: &T,
) -> Result<(), String> {
    if counts.len() == 0 {
        return Ok(());
    }
    for chunk in counts.keys().collect::<Vec<&String>>().chunks(500) {
        let mut converted = HashMap::new();
        for tick in chunk {
            converted.insert(*tick, counts.get(*tick).unwrap().to_string());
        }
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for (tick, value) in converted.iter() {
            params.push(*tick);
            params.push(value);
        }
        client
            .query(
                &format!(
                    "WITH changes (ticker, tx_count) AS (VALUES {})
                    UPDATE tokens SET tx_count = (
                        SELECT tokens.tx_count + c.tx_count::int
                        FROM changes AS c
                        WHERE c.ticker = tokens.ticker
                    )
                    WHERE EXISTS (SELECT 1 FROM changes AS c WHERE c.ticker = tokens.ticker)",
                    utils::multi_row_query_param_str(chunk.len(), 2)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("update_token_operation_counts: {e}"))?;
    }
    Ok(())
}

pub async fn update_token_minted_supplies<T: GenericClient>(
    supplies: &HashMap<String, PgNumericU128>,
    client: &T,
) -> Result<(), String> {
    if supplies.len() == 0 {
        return Ok(());
    }
    for chunk in supplies.keys().collect::<Vec<&String>>().chunks(500) {
        let mut converted = HashMap::new();
        for tick in chunk {
            converted.insert(*tick, supplies.get(*tick).unwrap().0.to_string());
        }
        let mut params: Vec<&(dyn ToSql + Sync)> = vec![];
        for (tick, value) in converted.iter() {
            params.push(*tick);
            params.push(value);
        }
        client
            .query(
                &format!(
                    "WITH changes (ticker, minted_supply) AS (VALUES {})
                    UPDATE tokens SET minted_supply = (
                        SELECT tokens.minted_supply + c.minted_supply::numeric
                        FROM changes AS c
                        WHERE c.ticker = tokens.ticker
                    )
                    WHERE EXISTS (SELECT 1 FROM changes AS c WHERE c.ticker = tokens.ticker)",
                    utils::multi_row_query_param_str(chunk.len(), 2)
                ),
                &params,
            )
            .await
            .map_err(|e| format!("update_token_minted_supplies: {e}"))?;
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

pub async fn rollback_block_operations<T: GenericClient>(
    block_height: u64,
    client: &T,
) -> Result<(), String> {
    client
        .execute(
            "WITH ops AS (SELECT * FROM operations WHERE block_height = $1),
            balance_changes AS (
                SELECT ticker, address,
                    CASE
                        WHEN operation = 'mint' OR operation = 'transfer_receive' THEN amount
                        WHEN operation = 'transfer' THEN -1 * amount
                        ELSE 0
                    END AS avail_balance,
                    CASE
                        WHEN operation = 'transfer' THEN amount
                        WHEN operation = 'transfer_send' THEN -1 * amount
                        ELSE 0
                    END AS trans_balance,
                    CASE
                        WHEN operation = 'mint' OR operation = 'transfer_receive' THEN amount
                        WHEN operation = 'transfer_send' THEN -1 * amount
                        ELSE 0
                    END AS total_balance
                FROM ops
            ),
            grouped_balance_changes AS (
                SELECT ticker, address, SUM(avail_balance) AS avail_balance, SUM(trans_balance) AS trans_balance,
                    SUM(total_balance) AS total_balance
                FROM balance_changes
                GROUP BY ticker, address
            ),
            balance_updates AS (
                UPDATE balances SET avail_balance = (
                    SELECT balances.avail_balance - SUM(grouped_balance_changes.avail_balance)
                    FROM grouped_balance_changes
                    WHERE grouped_balance_changes.address = balances.address AND grouped_balance_changes.ticker = balances.ticker
                ), trans_balance = (
                    SELECT balances.trans_balance - SUM(grouped_balance_changes.trans_balance)
                    FROM grouped_balance_changes
                    WHERE grouped_balance_changes.address = balances.address AND grouped_balance_changes.ticker = balances.ticker
                ), total_balance = (
                    SELECT balances.total_balance - SUM(grouped_balance_changes.total_balance)
                    FROM grouped_balance_changes
                    WHERE grouped_balance_changes.address = balances.address AND grouped_balance_changes.ticker = balances.ticker
                )
                WHERE EXISTS (
                    SELECT 1 FROM grouped_balance_changes
                    WHERE grouped_balance_changes.ticker = balances.ticker AND grouped_balance_changes.address = balances.address
                )
            ),
            token_mint_updates AS (
                UPDATE tokens SET minted_supply = (
                    SELECT tokens.minted_supply - SUM(ops.amount)
                    FROM ops
                    WHERE ops.ticker = tokens.ticker AND ops.operation = 'mint'
                    GROUP BY ops.ticker
                )
                WHERE EXISTS (SELECT 1 FROM ops WHERE ops.ticker = tokens.ticker)
            ),
            token_tx_count_updates AS (
                UPDATE tokens SET tx_count = (
                    SELECT tokens.tx_count - COUNT(*)
                    FROM ops
                    WHERE ops.ticker = tokens.ticker AND ops.operation <> 'transfer_receive'
                    GROUP BY ops.ticker
                )
                WHERE EXISTS (SELECT 1 FROM ops WHERE ops.ticker = tokens.ticker)
            ),
            address_op_count_updates AS (
                UPDATE counts_by_address_operation SET count = (
                    SELECT counts_by_address_operation.count - COUNT(*)
                    FROM ops
                    WHERE ops.address = counts_by_address_operation.address
                        AND ops.operation = counts_by_address_operation.operation
                    GROUP BY ops.address, ops.operation
                )
                WHERE EXISTS (
                    SELECT 1 FROM ops
                    WHERE ops.address = counts_by_address_operation.address
                        AND ops.operation = counts_by_address_operation.operation
                )
            ),
            op_count_updates AS (
                UPDATE counts_by_operation SET count = (
                    SELECT counts_by_operation.count - COUNT(*)
                    FROM ops
                    WHERE ops.operation = counts_by_operation.operation
                    GROUP BY ops.operation
                )
                WHERE EXISTS (
                    SELECT 1 FROM ops
                    WHERE ops.operation = counts_by_operation.operation
                )
            ),
            token_deletes AS (DELETE FROM tokens WHERE block_height = $1)
            DELETE FROM operations WHERE block_height = $1",
            &[&PgNumericU64(block_height)],
        )
        .await
        .map_err(|e| format!("rollback_block_operations: {e}"))?;
    Ok(())
}
