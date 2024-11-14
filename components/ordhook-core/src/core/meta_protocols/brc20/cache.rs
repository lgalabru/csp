use std::num::NonZeroUsize;

use chainhook_postgres::{
    tokio_postgres::Transaction,
    types::{PgBigIntU32, PgNumericU128, PgNumericU64, PgSmallIntU8},
};
use chainhook_sdk::types::{
    BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    TransactionIdentifier,
};
use lru::LruCache;

use crate::{config::Config, core::protocol::satoshi_tracking::get_output_and_offset_from_satpoint};

use super::{
    brc20_pg,
    models::{DbOperation, DbToken},
    verifier::{VerifiedBrc20BalanceData, VerifiedBrc20TokenDeployData, VerifiedBrc20TransferData},
};

/// If the given `config` has BRC-20 enabled, returns a BRC-20 memory cache.
pub fn brc20_new_cache(config: &Config) -> Option<Brc20MemoryCache> {
    if config.meta_protocols.brc20 {
        Some(Brc20MemoryCache::new(config.resources.brc20_lru_cache_size))
    } else {
        None
    }
}

/// Keeps BRC20 DB rows before they're inserted into Postgres. Use `flush` to insert.
pub struct Brc20DbCache {
    operations: Vec<DbOperation>,
    token_rows: Vec<DbToken>,
}

impl Brc20DbCache {
    fn new() -> Self {
        Brc20DbCache {
            operations: Vec::new(),
            token_rows: Vec::new(),
        }
    }

    pub async fn flush(&mut self, db_tx: &Transaction<'_>) -> Result<(), String> {
        if self.token_rows.len() > 0 {
            brc20_pg::insert_tokens(&self.token_rows, db_tx).await?;
            self.token_rows.clear();
        }
        if self.operations.len() > 0 {
            brc20_pg::insert_operations(&self.operations, db_tx).await?;
            self.operations.clear();
        }
        Ok(())
    }
}

/// In-memory cache that keeps verified token data to avoid excessive reads to the database.
pub struct Brc20MemoryCache {
    tokens: LruCache<String, DbToken>,
    token_minted_supplies: LruCache<String, u128>,
    token_addr_avail_balances: LruCache<String, u128>, // key format: "tick:address"
    unsent_transfers: LruCache<u64, DbOperation>,
    ignored_inscriptions: LruCache<u64, bool>,
    pub db_cache: Brc20DbCache,
}

impl Brc20MemoryCache {
    pub fn new(lru_size: usize) -> Self {
        Brc20MemoryCache {
            tokens: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_minted_supplies: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            token_addr_avail_balances: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            unsent_transfers: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            ignored_inscriptions: LruCache::new(NonZeroUsize::new(lru_size).unwrap()),
            db_cache: Brc20DbCache::new(),
        }
    }

    pub async fn get_token(
        &mut self,
        tick: &String,
        db_tx: &Transaction<'_>,
    ) -> Result<Option<DbToken>, String> {
        if let Some(token) = self.tokens.get(tick) {
            return Ok(Some(token.clone()));
        }
        self.handle_cache_miss(db_tx).await?;
        match brc20_pg::get_token(tick, db_tx).await? {
            Some(db_token) => {
                self.tokens.put(tick.clone(), db_token.clone());
                return Ok(Some(db_token));
            }
            None => return Ok(None),
        }
    }

    pub async fn get_token_minted_supply(
        &mut self,
        tick: &String,
        db_tx: &Transaction<'_>,
    ) -> Result<Option<u128>, String> {
        if let Some(minted) = self.token_minted_supplies.get(tick) {
            return Ok(Some(minted.clone()));
        }
        self.handle_cache_miss(db_tx).await?;
        if let Some(minted_supply) = brc20_pg::get_token_minted_supply(tick, db_tx).await? {
            self.token_minted_supplies
                .put(tick.to_string(), minted_supply);
            return Ok(Some(minted_supply));
        }
        return Ok(None);
    }

    pub async fn get_token_address_avail_balance(
        &mut self,
        tick: &String,
        address: &String,
        db_tx: &Transaction<'_>,
    ) -> Result<Option<u128>, String> {
        let key = format!("{}:{}", tick, address);
        if let Some(balance) = self.token_addr_avail_balances.get(&key) {
            return Ok(Some(balance.clone()));
        }
        self.handle_cache_miss(db_tx).await?;
        if let Some(balance) =
            brc20_pg::get_token_available_balance_for_address(tick, address, db_tx).await?
        {
            self.token_addr_avail_balances.put(key, balance);
            return Ok(Some(balance));
        }
        return Ok(None);
    }

    pub async fn get_unsent_token_transfer(
        &mut self,
        ordinal_number: u64,
        db_tx: &Transaction<'_>,
    ) -> Result<Option<DbOperation>, String> {
        // Use `get` instead of `contains` so we promote this value in the LRU.
        if let Some(_) = self.ignored_inscriptions.get(&ordinal_number) {
            return Ok(None);
        }
        if let Some(row) = self.unsent_transfers.get(&ordinal_number) {
            return Ok(Some(row.clone()));
        }
        self.handle_cache_miss(db_tx).await?;
        match brc20_pg::get_unsent_token_transfer(ordinal_number, db_tx).await? {
            Some(row) => {
                self.unsent_transfers.put(ordinal_number, row.clone());
                return Ok(Some(row));
            }
            None => {
                // Inscription is not relevant for BRC20.
                self.ignore_inscription(ordinal_number);
                return Ok(None);
            }
        }
    }

    /// Marks an ordinal number as ignored so we don't bother computing its transfers for BRC20 purposes.
    pub fn ignore_inscription(&mut self, ordinal_number: u64) {
        self.ignored_inscriptions.put(ordinal_number, true);
    }

    pub fn insert_token_deploy(
        &mut self,
        data: &VerifiedBrc20TokenDeployData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
    ) -> Result<(), String> {
        let (output, offset) = get_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription)?;
        let token = DbToken {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            block_height: PgNumericU64(block_identifier.index),
            ticker: data.tick.clone(),
            display_ticker: data.display_tick.clone(),
            max: PgNumericU128(data.max),
            address: data.address.clone(),
            self_mint: data.self_mint,
            block_hash: block_identifier.hash.clone(),
            tx_id: tx_identifier.hash.clone(),
            tx_index: PgNumericU64(tx_index),
            limit: PgNumericU128(data.lim),
            decimals: PgSmallIntU8(data.dec),
            minted_supply: PgNumericU128(0),
            burned_supply: PgNumericU128(0),
            tx_count: PgBigIntU32(0),
            timestamp: PgBigIntU32(timestamp),
        };
        self.tokens.put(token.ticker.clone(), token.clone());
        self.token_minted_supplies.put(token.ticker.clone(), 0);
        self.token_addr_avail_balances
            .put(format!("{}:{}", token.ticker, data.address), 0);
        self.db_cache.token_rows.push(token);
        self.db_cache.operations.push(DbOperation {
            ticker: data.tick.clone(),
            operation: "deploy".to_string(),
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            block_hash: block_identifier.hash.clone(),
            tx_id: tx_identifier.hash.clone(),
            tx_index: PgNumericU64(tx_index),
            output,
            offset: PgNumericU64(offset),
            timestamp: PgBigIntU32(timestamp),
            address: data.address.clone(),
            to_address: None,
            amount: PgNumericU128(0),
        });
        self.ignore_inscription(reveal.ordinal_number);
        Ok(())
    }

    pub async fn insert_token_mint(
        &mut self,
        data: &VerifiedBrc20BalanceData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
        db_tx: &Transaction<'_>,
    ) -> Result<(), String> {
        let Some(minted) = self.get_token_minted_supply(&data.tick, db_tx).await? else {
            unreachable!("BRC-20 deployed token should have a minted supply entry");
        };
        let (output, offset) = get_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription)?;
        self.token_minted_supplies
            .put(data.tick.clone(), minted + data.amt);
        let balance = self
            .get_token_address_avail_balance(&data.tick, &data.address, db_tx)
            .await?
            .unwrap_or(0);
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.address),
            balance + data.amt, // Increase for minter.
        );
        self.db_cache.operations.push(DbOperation {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.address.clone(),
            amount: PgNumericU128(data.amt),
            operation: "mint".to_string(),
            block_hash: block_identifier.hash.clone(),
            tx_id: tx_identifier.hash.clone(),
            output,
            offset: PgNumericU64(offset),
            timestamp: PgBigIntU32(timestamp),
            to_address: None,
        });
        self.ignore_inscription(reveal.ordinal_number);
        Ok(())
    }

    pub async fn insert_token_transfer(
        &mut self,
        data: &VerifiedBrc20BalanceData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
        db_tx: &Transaction<'_>,
    ) -> Result<(), String> {
        let Some(balance) = self
            .get_token_address_avail_balance(&data.tick, &data.address, db_tx)
            .await?
        else {
            unreachable!("BRC-20 transfer insert attempted for an address with no balance");
        };
        let (output, offset) = get_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription)?;
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.address),
            balance - data.amt, // Decrease for sender.
        );
        let ledger_row = DbOperation {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.address.clone(),
            amount: PgNumericU128(data.amt),
            operation: "transfer".to_string(),
            block_hash: block_identifier.hash.clone(),
            tx_id: tx_identifier.hash.clone(),
            output,
            offset: PgNumericU64(offset),
            timestamp: PgBigIntU32(timestamp),
            to_address: None,
        };
        self.unsent_transfers
            .put(reveal.ordinal_number, ledger_row.clone());
        self.db_cache.operations.push(ledger_row);
        self.ignored_inscriptions.pop(&reveal.ordinal_number); // Just in case.
        Ok(())
    }

    pub async fn insert_token_transfer_send(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
        db_tx: &Transaction<'_>,
    ) -> Result<(), String> {
        let (output, offset) = get_output_and_offset_from_satpoint(&transfer.satpoint_post_transfer)?;
        let transfer_row = self
            .get_unsent_transfer_row(transfer.ordinal_number, db_tx)
            .await?;
        self.db_cache.operations.push(DbOperation {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: PgNumericU64(transfer.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.sender_address.clone(),
            amount: PgNumericU128(data.amt),
            operation: "transfer_send".to_string(),
            block_hash: block_identifier.hash.clone(),
            tx_id: tx_identifier.hash.clone(),
            output: output.clone(),
            offset: PgNumericU64(offset),
            timestamp: PgBigIntU32(timestamp),
            to_address: Some(data.receiver_address.clone()),
        });
        self.db_cache.operations.push(DbOperation {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: PgNumericU64(transfer.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.receiver_address.clone(),
            amount: PgNumericU128(data.amt),
            operation: "transfer_receive".to_string(),
            block_hash: block_identifier.hash.clone(),
            tx_id: tx_identifier.hash.clone(),
            output,
            offset: PgNumericU64(offset),
            timestamp: PgBigIntU32(timestamp),
            to_address: None,
        });
        let balance = self
            .get_token_address_avail_balance(&data.tick, &data.receiver_address, db_tx)
            .await?
            .unwrap_or(0);
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.receiver_address),
            balance + data.amt, // Increase for receiver.
        );
        // We're not interested in further transfers.
        self.unsent_transfers.pop(&transfer.ordinal_number);
        self.ignore_inscription(transfer.ordinal_number);
        Ok(())
    }

    //
    //
    //

    async fn get_unsent_transfer_row(
        &mut self,
        ordinal_number: u64,
        db_tx: &Transaction<'_>,
    ) -> Result<DbOperation, String> {
        if let Some(transfer) = self.unsent_transfers.get(&ordinal_number) {
            return Ok(transfer.clone());
        }
        self.handle_cache_miss(db_tx).await?;
        let Some(transfer) = brc20_pg::get_unsent_token_transfer(ordinal_number, db_tx).await?
        else {
            unreachable!("Invalid transfer ordinal number {}", ordinal_number)
        };
        self.unsent_transfers.put(ordinal_number, transfer.clone());
        return Ok(transfer);
    }

    async fn handle_cache_miss(&mut self, db_tx: &Transaction<'_>) -> Result<(), String> {
        // TODO: Measure this event somewhere
        self.db_cache.flush(db_tx).await?;
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use chainhook_sdk::types::{BitcoinNetwork, BlockIdentifier};
//     use test_case::test_case;

//     use crate::core::meta_protocols::brc20::{
//         db::initialize_brc20_db,
//         parser::{ParsedBrc20BalanceData, ParsedBrc20Operation},
//         test_utils::{get_test_ctx, Brc20RevealBuilder},
//         verifier::{
//             verify_brc20_operation, VerifiedBrc20BalanceData, VerifiedBrc20Operation,
//             VerifiedBrc20TokenDeployData,
//         },
//     };

//     use super::Brc20MemoryCache;

//     #[test]
//     fn test_brc20_memory_cache_transfer_miss() {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         // LRU size as 1 so we can test a miss.
//         let mut cache = Brc20MemoryCache::new(1);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new().inscription_number(0).build(),
//             &BlockIdentifier {
//                 index: 800000,
//                 hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
//                     .to_string(),
//             },
//             0,
//             &tx,
//             &ctx,
//         );
//         let block = BlockIdentifier {
//             index: 800002,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let address1 = "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string();
//         let address2 = "bc1pngjqgeamkmmhlr6ft5yllgdmfllvcvnw5s7ew2ler3rl0z47uaesrj6jte".to_string();
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: address1.clone(),
//             },
//             &Brc20RevealBuilder::new().inscription_number(1).build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_transfer(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 100.0,
//                 address: address1.clone(),
//             },
//             &Brc20RevealBuilder::new().inscription_number(2).build(),
//             &block,
//             1,
//             &tx,
//             &ctx,
//         );
//         // These mint+transfer from a 2nd address will delete the first address' entries from cache.
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: address2.clone(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(3)
//                 .inscriber_address(Some(address2.clone()))
//                 .build(),
//             &block,
//             2,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_transfer(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 100.0,
//                 address: address2.clone(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(4)
//                 .inscriber_address(Some(address2.clone()))
//                 .build(),
//             &block,
//             3,
//             &tx,
//             &ctx,
//         );
//         // Validate another transfer from the first address. Should pass because we still have 900 avail balance.
//         let result = verify_brc20_operation(
//             &ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: "100".to_string(),
//             }),
//             &Brc20RevealBuilder::new()
//                 .inscription_number(5)
//                 .inscriber_address(Some(address1.clone()))
//                 .build(),
//             &block,
//             &BitcoinNetwork::Mainnet,
//             &mut cache,
//             &tx,
//             &ctx,
//         );
//         assert!(
//             result
//                 == Ok(VerifiedBrc20Operation::TokenTransfer(
//                     VerifiedBrc20BalanceData {
//                         tick: "pepe".to_string(),
//                         amt: 100.0,
//                         address: address1
//                     }
//                 ))
//         )
//     }

//     #[test_case(500.0 => Ok(Some(500.0)); "with transfer amt")]
//     #[test_case(1000.0 => Ok(Some(0.0)); "with transfer to zero")]
//     fn test_brc20_memory_cache_transfer_avail_balance(amt: f64) -> Result<Option<f64>, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new().inscription_number(0).build(),
//             &BlockIdentifier {
//                 index: 800000,
//                 hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
//                     .to_string(),
//             },
//             0,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new().inscription_number(1).build(),
//             &BlockIdentifier {
//                 index: 800001,
//                 hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
//                     .to_string(),
//             },
//             0,
//             &tx,
//             &ctx,
//         );
//         assert!(
//             cache.get_token_address_avail_balance(
//                 "pepe",
//                 "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp",
//                 &tx,
//                 &ctx,
//             ) == Some(1000.0)
//         );
//         cache.insert_token_transfer(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new().inscription_number(2).build(),
//             &BlockIdentifier {
//                 index: 800002,
//                 hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
//                     .to_string(),
//             },
//             0,
//             &tx,
//             &ctx,
//         );
//         Ok(cache.get_token_address_avail_balance(
//             "pepe",
//             "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp",
//             &tx,
//             &ctx,
//         ))
//     }
// }
