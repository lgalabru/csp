use std::collections::HashMap;

use chainhook_postgres::tokio_postgres::Transaction;
use chainhook_sdk::{
    types::{
        BitcoinBlockData, Brc20BalanceData, Brc20Operation, Brc20TokenDeployData,
        Brc20TransferData, OrdinalOperation,
    },
    utils::Context,
};

use crate::{core::meta_protocols::brc20::u128_amount_to_decimals_str, try_info};

use super::{
    brc20_activation_height,
    cache::Brc20MemoryCache,
    parser::ParsedBrc20Operation,
    verifier::{verify_brc20_operation, verify_brc20_transfer, VerifiedBrc20Operation},
};

/// Indexes BRC-20 operations in a Bitcoin block. Also writes the indexed data to DB.
pub async fn index_block_and_insert_brc20_operations(
    block: &mut BitcoinBlockData,
    brc20_operation_map: &mut HashMap<String, ParsedBrc20Operation>,
    brc20_cache: &mut Brc20MemoryCache,
    brc20_db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<(), String> {
    if block.block_identifier.index < brc20_activation_height(&block.metadata.network) {
        return Ok(());
    }
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        for op in tx.metadata.ordinal_operations.iter() {
            match op {
                OrdinalOperation::InscriptionRevealed(reveal) => {
                    if let Some(parsed_brc20_operation) =
                        brc20_operation_map.get(&reveal.inscription_id)
                    {
                        match verify_brc20_operation(
                            parsed_brc20_operation,
                            reveal,
                            &block.block_identifier,
                            &block.metadata.network,
                            brc20_cache,
                            &brc20_db_tx,
                            &ctx,
                        )
                        .await?
                        {
                            Some(VerifiedBrc20Operation::TokenDeploy(token)) => {
                                tx.metadata.brc20_operation =
                                    Some(Brc20Operation::Deploy(Brc20TokenDeployData {
                                        tick: token.tick.clone(),
                                        max: u128_amount_to_decimals_str(token.max, token.dec),
                                        lim: u128_amount_to_decimals_str(token.lim, token.dec),
                                        dec: token.dec.to_string(),
                                        address: token.address.clone(),
                                        inscription_id: reveal.inscription_id.clone(),
                                        self_mint: token.self_mint,
                                    }));
                                brc20_cache.insert_token_deploy(
                                    &token,
                                    reveal,
                                    &block.block_identifier,
                                    block.timestamp,
                                    &tx.transaction_identifier,
                                    tx_index as u64,
                                )?;
                                try_info!(
                                    ctx,
                                    "BRC-20 deploy {} ({}) at block {}",
                                    token.tick,
                                    token.address,
                                    block.block_identifier.index
                                );
                            }
                            Some(VerifiedBrc20Operation::TokenMint(balance)) => {
                                let Some(token) =
                                    brc20_cache.get_token(&balance.tick, brc20_db_tx).await?
                                else {
                                    unreachable!();
                                };
                                tx.metadata.brc20_operation =
                                    Some(Brc20Operation::Mint(Brc20BalanceData {
                                        tick: balance.tick.clone(),
                                        amt: u128_amount_to_decimals_str(
                                            balance.amt,
                                            token.decimals.0,
                                        ),
                                        address: balance.address.clone(),
                                        inscription_id: reveal.inscription_id.clone(),
                                    }));
                                brc20_cache
                                    .insert_token_mint(
                                        &balance,
                                        reveal,
                                        &block.block_identifier,
                                        block.timestamp,
                                        &tx.transaction_identifier,
                                        tx_index as u64,
                                        brc20_db_tx,
                                    )
                                    .await?;
                                try_info!(
                                    ctx,
                                    "BRC-20 mint {} {} ({}) at block {}",
                                    balance.tick,
                                    balance.amt,
                                    balance.address,
                                    block.block_identifier.index
                                );
                            }
                            Some(VerifiedBrc20Operation::TokenTransfer(balance)) => {
                                let Some(token) =
                                    brc20_cache.get_token(&balance.tick, brc20_db_tx).await?
                                else {
                                    unreachable!();
                                };
                                tx.metadata.brc20_operation =
                                    Some(Brc20Operation::Transfer(Brc20BalanceData {
                                        tick: balance.tick.clone(),
                                        amt: u128_amount_to_decimals_str(
                                            balance.amt,
                                            token.decimals.0,
                                        ),
                                        address: balance.address.clone(),
                                        inscription_id: reveal.inscription_id.clone(),
                                    }));
                                brc20_cache
                                    .insert_token_transfer(
                                        &balance,
                                        reveal,
                                        &block.block_identifier,
                                        block.timestamp,
                                        &tx.transaction_identifier,
                                        tx_index as u64,
                                        brc20_db_tx,
                                    )
                                    .await?;
                                try_info!(
                                    ctx,
                                    "BRC-20 transfer {} {} ({}) at block {}",
                                    balance.tick,
                                    balance.amt,
                                    balance.address,
                                    block.block_identifier.index
                                );
                            }
                            Some(VerifiedBrc20Operation::TokenTransferSend(_)) => {
                                unreachable!("BRC-20 token transfer send should never be generated on reveal")
                            }
                            None => {
                                brc20_cache.ignore_inscription(reveal.ordinal_number);
                            }
                        }
                    } else {
                        brc20_cache.ignore_inscription(reveal.ordinal_number);
                    }
                }
                OrdinalOperation::InscriptionTransferred(transfer) => {
                    match verify_brc20_transfer(transfer, brc20_cache, &brc20_db_tx, &ctx).await? {
                        Some(data) => {
                            let Some(token) =
                                brc20_cache.get_token(&data.tick, brc20_db_tx).await?
                            else {
                                unreachable!();
                            };
                            let Some(unsent_transfer) = brc20_cache
                                .get_unsent_token_transfer(transfer.ordinal_number, brc20_db_tx)
                                .await?
                            else {
                                unreachable!();
                            };
                            tx.metadata.brc20_operation =
                                Some(Brc20Operation::TransferSend(Brc20TransferData {
                                    tick: data.tick.clone(),
                                    amt: u128_amount_to_decimals_str(data.amt, token.decimals.0),
                                    sender_address: data.sender_address.clone(),
                                    receiver_address: data.receiver_address.clone(),
                                    inscription_id: unsent_transfer.inscription_id,
                                }));
                            brc20_cache
                                .insert_token_transfer_send(
                                    &data,
                                    &transfer,
                                    &block.block_identifier,
                                    block.timestamp,
                                    &tx.transaction_identifier,
                                    tx_index as u64,
                                    brc20_db_tx,
                                )
                                .await?;
                            try_info!(
                                ctx,
                                "BRC-20 transfer_send {} {} ({} -> {}) at block {}",
                                data.tick,
                                data.amt,
                                data.sender_address,
                                data.receiver_address,
                                block.block_identifier.index
                            );
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    brc20_cache.db_cache.flush(brc20_db_tx).await?;
    Ok(())
}
