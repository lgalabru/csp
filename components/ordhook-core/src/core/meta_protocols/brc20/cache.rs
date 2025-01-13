use std::{collections::HashMap, num::NonZeroUsize};

use chainhook_postgres::{
    deadpool_postgres::GenericClient,
    types::{PgBigIntU32, PgNumericU128, PgNumericU64, PgSmallIntU8},
};
use chainhook_sdk::types::{
    BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    TransactionIdentifier,
};
use lru::LruCache;
use maplit::hashmap;

use crate::{
    config::Config, core::protocol::satoshi_tracking::parse_output_and_offset_from_satpoint,
};

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
    operation_counts: HashMap<String, i32>,
    address_operation_counts: HashMap<String, HashMap<String, i32>>,
    token_operation_counts: HashMap<String, i32>,
    token_minted_supplies: HashMap<String, PgNumericU128>,
}

impl Brc20DbCache {
    fn new() -> Self {
        Brc20DbCache {
            operations: Vec::new(),
            token_rows: Vec::new(),
            operation_counts: HashMap::new(),
            address_operation_counts: HashMap::new(),
            token_operation_counts: HashMap::new(),
            token_minted_supplies: HashMap::new(),
        }
    }

    pub async fn flush<T: GenericClient>(&mut self, client: &T) -> Result<(), String> {
        brc20_pg::insert_tokens(&self.token_rows, client).await?;
        self.token_rows.clear();
        brc20_pg::insert_operations(&self.operations, client).await?;
        self.operations.clear();
        brc20_pg::update_operation_counts(&self.operation_counts, client).await?;
        self.operation_counts.clear();
        brc20_pg::update_address_operation_counts(&self.address_operation_counts, client).await?;
        self.address_operation_counts.clear();
        brc20_pg::update_token_operation_counts(&self.token_operation_counts, client).await?;
        self.token_operation_counts.clear();
        brc20_pg::update_token_minted_supplies(&self.token_minted_supplies, client).await?;
        self.token_minted_supplies.clear();
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

    pub async fn get_token<T: GenericClient>(
        &mut self,
        tick: &String,
        client: &T,
    ) -> Result<Option<DbToken>, String> {
        if let Some(token) = self.tokens.get(tick) {
            return Ok(Some(token.clone()));
        }
        self.handle_cache_miss(client).await?;
        match brc20_pg::get_token(tick, client).await? {
            Some(db_token) => {
                self.tokens.put(tick.clone(), db_token.clone());
                return Ok(Some(db_token));
            }
            None => return Ok(None),
        }
    }

    pub async fn get_token_minted_supply<T: GenericClient>(
        &mut self,
        tick: &String,
        client: &T,
    ) -> Result<Option<u128>, String> {
        if let Some(minted) = self.token_minted_supplies.get(tick) {
            return Ok(Some(minted.clone()));
        }
        self.handle_cache_miss(client).await?;
        if let Some(minted_supply) = brc20_pg::get_token_minted_supply(tick, client).await? {
            self.token_minted_supplies
                .put(tick.to_string(), minted_supply);
            return Ok(Some(minted_supply));
        }
        return Ok(None);
    }

    pub async fn get_token_address_avail_balance<T: GenericClient>(
        &mut self,
        tick: &String,
        address: &String,
        client: &T,
    ) -> Result<Option<u128>, String> {
        let key = format!("{}:{}", tick, address);
        if let Some(balance) = self.token_addr_avail_balances.get(&key) {
            return Ok(Some(balance.clone()));
        }
        self.handle_cache_miss(client).await?;
        if let Some(balance) =
            brc20_pg::get_token_available_balance_for_address(tick, address, client).await?
        {
            self.token_addr_avail_balances.put(key, balance);
            return Ok(Some(balance));
        }
        return Ok(None);
    }

    pub async fn get_unsent_token_transfer<T: GenericClient>(
        &mut self,
        ordinal_number: u64,
        client: &T,
    ) -> Result<Option<DbOperation>, String> {
        // Use `get` instead of `contains` so we promote this value in the LRU.
        if let Some(_) = self.ignored_inscriptions.get(&ordinal_number) {
            return Ok(None);
        }
        if let Some(row) = self.unsent_transfers.get(&ordinal_number) {
            return Ok(Some(row.clone()));
        }
        self.handle_cache_miss(client).await?;
        match brc20_pg::get_unsent_token_transfer(ordinal_number, client).await? {
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
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription)?;
        let token = DbToken {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            block_height: PgNumericU64(block_identifier.index),
            ticker: data.tick.clone(),
            display_ticker: data.display_tick.clone(),
            max: PgNumericU128(data.max),
            address: data.address.clone(),
            self_mint: data.self_mint,
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
            tx_index: PgNumericU64(tx_index),
            limit: PgNumericU128(data.lim),
            decimals: PgSmallIntU8(data.dec),
            minted_supply: PgNumericU128(0),
            tx_count: 0,
            timestamp: PgBigIntU32(timestamp),
        };
        self.tokens.put(token.ticker.clone(), token.clone());
        self.token_minted_supplies.put(token.ticker.clone(), 0);
        self.token_addr_avail_balances
            .put(format!("{}:{}", token.ticker, data.address), 0);
        self.db_cache.token_rows.push(token);
        let operation = "deploy".to_string();
        self.increase_operation_count(operation.clone(), 1);
        self.increase_address_operation_count(data.address.clone(), operation.clone(), 1);
        self.db_cache.operations.push(DbOperation {
            ticker: data.tick.clone(),
            operation,
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
            tx_index: PgNumericU64(tx_index),
            output,
            offset: PgNumericU64(offset.unwrap()),
            timestamp: PgBigIntU32(timestamp),
            address: data.address.clone(),
            to_address: None,
            amount: PgNumericU128(0),
        });
        self.increase_token_operation_count(data.tick.clone(), 1);
        self.ignore_inscription(reveal.ordinal_number);
        Ok(())
    }

    pub async fn insert_token_mint<T: GenericClient>(
        &mut self,
        data: &VerifiedBrc20BalanceData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
        client: &T,
    ) -> Result<(), String> {
        let Some(minted) = self.get_token_minted_supply(&data.tick, client).await? else {
            unreachable!("BRC-20 deployed token should have a minted supply entry");
        };
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription)?;
        self.token_minted_supplies
            .put(data.tick.clone(), minted + data.amt);
        let balance = self
            .get_token_address_avail_balance(&data.tick, &data.address, client)
            .await?
            .unwrap_or(0);
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.address),
            balance + data.amt, // Increase for minter.
        );
        let operation = "mint".to_string();
        self.increase_operation_count(operation.clone(), 1);
        self.increase_address_operation_count(data.address.clone(), operation.clone(), 1);
        self.db_cache.operations.push(DbOperation {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.address.clone(),
            amount: PgNumericU128(data.amt),
            operation,
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
            output,
            offset: PgNumericU64(offset.unwrap()),
            timestamp: PgBigIntU32(timestamp),
            to_address: None,
        });
        self.increase_token_operation_count(data.tick.clone(), 1);
        self.db_cache
            .token_minted_supplies
            .entry(data.tick.clone())
            .and_modify(|c| *c += data.amt)
            .or_insert(PgNumericU128(data.amt));
        self.ignore_inscription(reveal.ordinal_number);
        Ok(())
    }

    pub async fn insert_token_transfer<T: GenericClient>(
        &mut self,
        data: &VerifiedBrc20BalanceData,
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
        client: &T,
    ) -> Result<(), String> {
        let Some(balance) = self
            .get_token_address_avail_balance(&data.tick, &data.address, client)
            .await?
        else {
            unreachable!("BRC-20 transfer insert attempted for an address with no balance");
        };
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription)?;
        self.token_addr_avail_balances.put(
            format!("{}:{}", data.tick, data.address),
            balance - data.amt, // Decrease for sender.
        );
        let operation = "transfer".to_string();
        self.increase_operation_count(operation.clone(), 1);
        self.increase_address_operation_count(data.address.clone(), operation.clone(), 1);
        let ledger_row = DbOperation {
            inscription_id: reveal.inscription_id.clone(),
            inscription_number: reveal.inscription_number.jubilee,
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.address.clone(),
            amount: PgNumericU128(data.amt),
            operation,
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
            output,
            offset: PgNumericU64(offset.unwrap()),
            timestamp: PgBigIntU32(timestamp),
            to_address: None,
        };
        self.increase_token_operation_count(data.tick.clone(), 1);
        self.unsent_transfers
            .put(reveal.ordinal_number, ledger_row.clone());
        self.db_cache.operations.push(ledger_row);
        self.ignored_inscriptions.pop(&reveal.ordinal_number); // Just in case.
        Ok(())
    }

    pub async fn insert_token_transfer_send<T: GenericClient>(
        &mut self,
        data: &VerifiedBrc20TransferData,
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        timestamp: u32,
        tx_identifier: &TransactionIdentifier,
        tx_index: u64,
        client: &T,
    ) -> Result<(), String> {
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&transfer.satpoint_post_transfer)?;
        let transfer_row = self
            .get_unsent_transfer_row(transfer.ordinal_number, client)
            .await?;
        let operation = "transfer_send".to_string();
        self.increase_operation_count(operation.clone(), 1);
        self.increase_address_operation_count(data.sender_address.clone(), operation.clone(), 1);
        if data.sender_address != data.receiver_address {
            self.increase_address_operation_count(
                data.receiver_address.clone(),
                operation.clone(),
                1,
            );
        }
        self.db_cache.operations.push(DbOperation {
            inscription_id: transfer_row.inscription_id.clone(),
            inscription_number: transfer_row.inscription_number,
            ordinal_number: PgNumericU64(transfer.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgNumericU64(tx_index),
            ticker: data.tick.clone(),
            address: data.sender_address.clone(),
            amount: PgNumericU128(data.amt),
            operation: operation.clone(),
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
            output: output.clone(),
            offset: PgNumericU64(offset.unwrap()),
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
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
            output,
            offset: PgNumericU64(offset.unwrap()),
            timestamp: PgBigIntU32(timestamp),
            to_address: None,
        });
        self.increase_token_operation_count(data.tick.clone(), 1);
        let balance = self
            .get_token_address_avail_balance(&data.tick, &data.receiver_address, client)
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

    fn increase_operation_count(&mut self, operation: String, delta: i32) {
        self.db_cache
            .operation_counts
            .entry(operation)
            .and_modify(|c| *c += delta)
            .or_insert(delta);
    }

    fn increase_address_operation_count(&mut self, address: String, operation: String, delta: i32) {
        self.db_cache
            .address_operation_counts
            .entry(address)
            .and_modify(|c| {
                (*c).entry(operation.clone())
                    .and_modify(|c| *c += delta)
                    .or_insert(delta);
            })
            .or_insert(hashmap! { operation => delta });
    }

    fn increase_token_operation_count(&mut self, tick: String, delta: i32) {
        self.db_cache
            .token_operation_counts
            .entry(tick)
            .and_modify(|c| *c += delta)
            .or_insert(delta);
    }

    async fn get_unsent_transfer_row<T: GenericClient>(
        &mut self,
        ordinal_number: u64,
        client: &T,
    ) -> Result<DbOperation, String> {
        if let Some(transfer) = self.unsent_transfers.get(&ordinal_number) {
            return Ok(transfer.clone());
        }
        self.handle_cache_miss(client).await?;
        let Some(transfer) = brc20_pg::get_unsent_token_transfer(ordinal_number, client).await?
        else {
            unreachable!("Invalid transfer ordinal number {}", ordinal_number)
        };
        self.unsent_transfers.put(ordinal_number, transfer.clone());
        return Ok(transfer);
    }

    async fn handle_cache_miss<T: GenericClient>(&mut self, client: &T) -> Result<(), String> {
        // TODO: Measure this event somewhere
        self.db_cache.flush(client).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use chainhook_postgres::{pg_begin, pg_pool_client};
    use chainhook_sdk::types::{BitcoinNetwork, BlockIdentifier, TransactionIdentifier};
    use test_case::test_case;

    use crate::{
        core::meta_protocols::brc20::{
            brc20_pg,
            parser::{ParsedBrc20BalanceData, ParsedBrc20Operation},
            test_utils::{get_test_ctx, Brc20RevealBuilder},
            verifier::{
                verify_brc20_operation, VerifiedBrc20BalanceData, VerifiedBrc20Operation,
                VerifiedBrc20TokenDeployData,
            },
        },
        db::{pg_reset_db, pg_test_connection, pg_test_connection_pool},
    };

    use super::Brc20MemoryCache;

    #[tokio::test]
    async fn test_brc20_memory_cache_transfer_miss() -> Result<(), String> {
        let ctx = get_test_ctx();
        let mut pg_client = pg_test_connection().await;
        let _ = brc20_pg::migrate(&mut pg_client).await;
        {
            let mut ord_client = pg_pool_client(&pg_test_connection_pool()).await.unwrap();
            let client = pg_begin(&mut ord_client).await.unwrap();

            // LRU size as 1 so we can test a miss.
            let mut cache = Brc20MemoryCache::new(1);
            cache.insert_token_deploy(
                &VerifiedBrc20TokenDeployData {
                    tick: "pepe".to_string(),
                    display_tick: "pepe".to_string(),
                    max: 21000000_000000000000000000,
                    lim: 1000_000000000000000000,
                    dec: 18,
                    address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                    self_mint: false,
                },
                &Brc20RevealBuilder::new().inscription_number(0).build(),
                &BlockIdentifier {
                    index: 800000,
                    hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                        .to_string(),
                },
                0,
                &TransactionIdentifier {
                    hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d391"
                        .to_string(),
                },
                0,
            )?;
            let block = BlockIdentifier {
                index: 800002,
                hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                    .to_string(),
            };
            let address1 = "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string();
            let address2 =
                "bc1pngjqgeamkmmhlr6ft5yllgdmfllvcvnw5s7ew2ler3rl0z47uaesrj6jte".to_string();
            cache
                .insert_token_mint(
                    &VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt: 1000_000000000000000000,
                        address: address1.clone(),
                    },
                    &Brc20RevealBuilder::new().inscription_number(1).build(),
                    &block,
                    0,
                    &TransactionIdentifier {
                        hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d392"
                            .to_string(),
                    },
                    1,
                    &client,
                )
                .await?;
            cache
                .insert_token_transfer(
                    &VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt: 100_000000000000000000,
                        address: address1.clone(),
                    },
                    &Brc20RevealBuilder::new().inscription_number(2).build(),
                    &block,
                    1,
                    &TransactionIdentifier {
                        hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d393"
                            .to_string(),
                    },
                    2,
                    &client,
                )
                .await?;
            // These mint+transfer from a 2nd address will delete the first address' entries from cache.
            cache
                .insert_token_mint(
                    &VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt: 1000_000000000000000000,
                        address: address2.clone(),
                    },
                    &Brc20RevealBuilder::new()
                        .inscription_number(3)
                        .inscriber_address(Some(address2.clone()))
                        .build(),
                    &block,
                    2,
                    &TransactionIdentifier {
                        hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d394"
                            .to_string(),
                    },
                    3,
                    &client,
                )
                .await?;
            cache
                .insert_token_transfer(
                    &VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt: 100_000000000000000000,
                        address: address2.clone(),
                    },
                    &Brc20RevealBuilder::new()
                        .inscription_number(4)
                        .inscriber_address(Some(address2.clone()))
                        .build(),
                    &block,
                    3,
                    &TransactionIdentifier {
                        hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d395"
                            .to_string(),
                    },
                    4,
                    &client,
                )
                .await?;
            // Validate another transfer from the first address. Should pass because we still have 900 avail balance.
            let result = verify_brc20_operation(
                &ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
                    tick: "pepe".to_string(),
                    amt: "100".to_string(),
                }),
                &Brc20RevealBuilder::new()
                    .inscription_number(5)
                    .inscriber_address(Some(address1.clone()))
                    .build(),
                &block,
                &BitcoinNetwork::Mainnet,
                &mut cache,
                &client,
                &ctx,
            )
            .await;
            assert!(
                result
                    == Ok(Some(VerifiedBrc20Operation::TokenTransfer(
                        VerifiedBrc20BalanceData {
                            tick: "pepe".to_string(),
                            amt: 100_000000000000000000,
                            address: address1
                        }
                    )))
            );
        }
        pg_reset_db(&mut pg_client).await;
        Ok(())
    }

    #[test_case(500_000000000000000000 => Ok(Some(500_000000000000000000)); "with transfer amt")]
    #[test_case(1000_000000000000000000 => Ok(Some(0)); "with transfer to zero")]
    #[tokio::test]
    async fn test_brc20_memory_cache_transfer_avail_balance(
        amt: u128,
    ) -> Result<Option<u128>, String> {
        let mut pg_client = pg_test_connection().await;
        brc20_pg::migrate(&mut pg_client).await?;
        let result = {
            let mut ord_client = pg_pool_client(&pg_test_connection_pool()).await?;
            let client = pg_begin(&mut ord_client).await?;

            let mut cache = Brc20MemoryCache::new(10);
            cache.insert_token_deploy(
                &VerifiedBrc20TokenDeployData {
                    tick: "pepe".to_string(),
                    display_tick: "pepe".to_string(),
                    max: 21000000_000000000000000000,
                    lim: 1000_000000000000000000,
                    dec: 18,
                    address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                    self_mint: false,
                },
                &Brc20RevealBuilder::new().inscription_number(0).build(),
                &BlockIdentifier {
                    index: 800000,
                    hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                        .to_string(),
                },
                0,
                &TransactionIdentifier {
                    hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d391"
                        .to_string(),
                },
                0,
            )?;
            cache
                .insert_token_mint(
                    &VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt: 1000_000000000000000000,
                        address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                    },
                    &Brc20RevealBuilder::new().inscription_number(1).build(),
                    &BlockIdentifier {
                        index: 800001,
                        hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                            .to_string(),
                    },
                    0,
                    &TransactionIdentifier {
                        hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d392"
                            .to_string(),
                    },
                    1,
                    &client,
                )
                .await?;
            assert!(
                cache
                    .get_token_address_avail_balance(
                        &"pepe".to_string(),
                        &"324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                        &client
                    )
                    .await?
                    == Some(1000_000000000000000000)
            );
            cache
                .insert_token_transfer(
                    &VerifiedBrc20BalanceData {
                        tick: "pepe".to_string(),
                        amt,
                        address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                    },
                    &Brc20RevealBuilder::new().inscription_number(2).build(),
                    &BlockIdentifier {
                        index: 800002,
                        hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
                            .to_string(),
                    },
                    0,
                    &TransactionIdentifier {
                        hash: "8c8e37ce3ddd869767f8d839d16acc7ea4ec9dd7e3c73afd42a0abb859d7d393"
                            .to_string(),
                    },
                    2,
                    &client,
                )
                .await?;
            Ok(cache
                .get_token_address_avail_balance(
                    &"pepe".to_string(),
                    &"324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
                    &client,
                )
                .await?)
        };
        pg_reset_db(&mut pg_client).await;
        result
    }
}
