use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    hash::BuildHasherDefault,
    sync::Arc,
};

use chainhook_postgres::{tokio_postgres::Transaction, with_pg_transaction};
use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::Network,
    types::{
        BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, BlockIdentifier,
        OrdinalInscriptionCurseType, OrdinalInscriptionNumber,
        OrdinalInscriptionTransferDestination, OrdinalOperation, TransactionIdentifier,
    },
    utils::Context,
};
use crossbeam_channel::unbounded;
use dashmap::DashMap;
use fxhash::FxHasher;

use crate::{
    config::Config,
    core::{meta_protocols::brc20::brc20_pg, resolve_absolute_pointer},
    db::{cursor::TransactionBytesCursor, ordinals_pg},
    ord::height::Height,
    try_error, try_info,
    utils::format_inscription_id,
};

use std::sync::mpsc::channel;

use super::{
    satoshi_numbering::{compute_satoshi_number, TraversalResult},
    satoshi_tracking::{
        augment_transaction_with_ordinal_transfers, compute_satpoint_post_transfer,
    },
};

/// Parallelize the computation of ordinals numbers for inscriptions present in a block.
///
/// This function will:
/// 1) Limit the number of ordinals numbers to compute by filtering out all the ordinals numbers  pre-computed
/// and present in the L1 cache.
/// 2) Create a threadpool, by spawning as many threads as specified by the config to process the batch ordinals to
/// retrieve
/// 3) Consume eventual entries in cache L1
/// 4) Inject the ordinals to compute (random order) in a priority queue
/// via the command line).
/// 5) Keep injecting ordinals from next blocks (if any) as long as the ordinals from the current block are not all
/// computed and augment the cache L1 for future blocks.
///
/// If the block has already been computed in the past (so presence of ordinals number present in the `inscriptions` db)
/// the transaction is removed from the set to compute, and not injected in L1 either.
/// This behaviour should be refined.
///
/// # Panics
/// - unability to spawn threads
///
/// # Todos / Optimizations
/// - Pre-computed entries are being consumed from L1, and then re-injected in L1, which is wasting a bunch of cycles.
///
pub fn parallelize_inscription_data_computations(
    block: &BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    db_tx: &Transaction,
    config: &Config,
    ctx: &Context,
) -> Result<bool, String> {
    let inner_ctx = if config.logs.ordinals_internals {
        ctx.clone()
    } else {
        Context::empty()
    };

    try_info!(
        inner_ctx,
        "Inscriptions data computation for block #{} started",
        block.block_identifier.index
    );

    let (transactions_ids, l1_cache_hits) =
        get_transactions_to_process(block, cache_l1, db_tx, ctx);

    let has_transactions_to_process = !transactions_ids.is_empty() || !l1_cache_hits.is_empty();

    let thread_pool_capacity = config.resources.get_optimal_thread_pool_capacity();

    // Nothing to do? early return
    if !has_transactions_to_process {
        return Ok(false);
    }

    let expected_traversals = transactions_ids.len() + l1_cache_hits.len();
    let (traversal_tx, traversal_rx) = unbounded();

    let mut tx_thread_pool = vec![];
    let mut thread_pool_handles = vec![];

    for thread_index in 0..thread_pool_capacity {
        let (tx, rx) = channel();
        tx_thread_pool.push(tx);

        let moved_traversal_tx = traversal_tx.clone();
        let moved_ctx = inner_ctx.clone();
        let moved_config = config.clone();

        let local_cache = cache_l2.clone();

        let handle = hiro_system_kit::thread_named("Worker")
            .spawn(move || {
                while let Ok(Some((
                    transaction_id,
                    block_identifier,
                    input_index,
                    inscription_pointer,
                    prioritary,
                ))) = rx.recv()
                {
                    let traversal: Result<(TraversalResult, u64, _), String> =
                        compute_satoshi_number(
                            &block_identifier,
                            &transaction_id,
                            input_index,
                            inscription_pointer,
                            &local_cache,
                            &moved_config,
                            &moved_ctx,
                        );
                    let _ = moved_traversal_tx.send((traversal, prioritary, thread_index));
                }
            })
            .expect("unable to spawn thread");
        thread_pool_handles.push(handle);
    }

    // Consume L1 cache: if the traversal was performed in a previous round
    // retrieve it and inject it to the "reduce" worker (by-passing the "map" thread pool)
    let mut round_robin_thread_index = 0;
    for key in l1_cache_hits.iter() {
        if let Some(entry) = cache_l1.get(key) {
            let _ = traversal_tx.send((
                Ok((entry.clone(), key.2, vec![])),
                true,
                round_robin_thread_index,
            ));
            round_robin_thread_index = (round_robin_thread_index + 1) % thread_pool_capacity;
        }
    }

    let next_block_heights = next_blocks
        .iter()
        .map(|b| format!("{}", b.block_identifier.index))
        .collect::<Vec<_>>();

    try_info!(
        inner_ctx,
        "Number of inscriptions in block #{} to process: {} (L1 cache hits: {}, queue: [{}], L1 cache len: {}, L2 cache len: {})",
        block.block_identifier.index,
        transactions_ids.len(),
        l1_cache_hits.len(),
        next_block_heights.join(", "),
        cache_l1.len(),
        cache_l2.len(),
    );

    let mut priority_queue = VecDeque::new();
    let mut warmup_queue = VecDeque::new();

    for (transaction_id, input_index, inscription_pointer) in transactions_ids.into_iter() {
        priority_queue.push_back((
            transaction_id,
            block.block_identifier.clone(),
            input_index,
            inscription_pointer,
            true,
        ));
    }

    // Feed each worker from the thread pool with 2 workitems each
    for thread_index in 0..thread_pool_capacity {
        let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
    }
    for thread_index in 0..thread_pool_capacity {
        let _ = tx_thread_pool[thread_index].send(priority_queue.pop_front());
    }

    let mut next_block_iter = next_blocks.iter();
    let mut traversals_received = 0;
    while let Ok((traversal_result, prioritary, thread_index)) = traversal_rx.recv() {
        if prioritary {
            traversals_received += 1;
        }
        match traversal_result {
            Ok((traversal, inscription_pointer, _)) => {
                try_info!(
                    inner_ctx,
                    "Completed ordinal number retrieval for Satpoint {}:{}:{} (block: #{}:{}, transfers: {}, progress: {traversals_received}/{expected_traversals}, priority queue: {prioritary}, thread: {thread_index})",
                    traversal.transaction_identifier_inscription.hash,
                    traversal.inscription_input_index,
                    inscription_pointer,
                    traversal.get_ordinal_coinbase_height(),
                    traversal.get_ordinal_coinbase_offset(),
                    traversal.transfers
                );
                cache_l1.insert(
                    (
                        traversal.transaction_identifier_inscription.clone(),
                        traversal.inscription_input_index,
                        inscription_pointer,
                    ),
                    traversal,
                );
            }
            Err(e) => {
                try_error!(inner_ctx, "Unable to compute inscription's Satoshi: {e}");
            }
        }

        if traversals_received == expected_traversals {
            break;
        }

        if let Some(w) = priority_queue.pop_front() {
            let _ = tx_thread_pool[thread_index].send(Some(w));
        } else {
            if let Some(w) = warmup_queue.pop_front() {
                let _ = tx_thread_pool[thread_index].send(Some(w));
            } else {
                if let Some(next_block) = next_block_iter.next() {
                    let (transactions_ids, _) =
                        get_transactions_to_process(next_block, cache_l1, db_tx, ctx);

                    try_info!(
                        inner_ctx,
                        "Number of inscriptions in block #{} to pre-process: {}",
                        block.block_identifier.index,
                        transactions_ids.len()
                    );

                    for (transaction_id, input_index, inscription_pointer) in
                        transactions_ids.into_iter()
                    {
                        warmup_queue.push_back((
                            transaction_id,
                            next_block.block_identifier.clone(),
                            input_index,
                            inscription_pointer,
                            false,
                        ));
                    }
                    let _ = tx_thread_pool[thread_index].send(warmup_queue.pop_front());
                }
            }
        }
    }
    try_info!(
        inner_ctx,
        "Inscriptions data computation for block #{} collected",
        block.block_identifier.index
    );

    // Collect eventual results for incoming blocks
    for tx in tx_thread_pool.iter() {
        // Empty the queue
        if let Ok((traversal_result, _prioritary, thread_index)) = traversal_rx.try_recv() {
            if let Ok((traversal, inscription_pointer, _)) = traversal_result {
                try_info!(
                    inner_ctx,
                    "Completed ordinal number retrieval for Satpoint {}:{}:{} (block: #{}:{}, transfers: {}, pre-retrieval, thread: {thread_index})",
                    traversal.transaction_identifier_inscription.hash,
                    traversal.inscription_input_index,
                    inscription_pointer,
                    traversal.get_ordinal_coinbase_height(),
                    traversal.get_ordinal_coinbase_offset(),
                    traversal.transfers
                );
                cache_l1.insert(
                    (
                        traversal.transaction_identifier_inscription.clone(),
                        traversal.inscription_input_index,
                        inscription_pointer,
                    ),
                    traversal,
                );
            }
        }
        let _ = tx.send(None);
    }

    let _ = hiro_system_kit::thread_named("Garbage collection").spawn(move || {
        for handle in thread_pool_handles.into_iter() {
            let _ = handle.join();
        }
    });

    try_info!(
        inner_ctx,
        "Inscriptions data computation for block #{} ended",
        block.block_identifier.index
    );

    Ok(has_transactions_to_process)
}

/// Given a block, a cache L1, and a readonly DB connection, returns a tuple with the transactions that must be included
/// for ordinals computation and the list of transactions where we have a cache hit.
///
/// This function will:
/// 1) Retrieve all the eventual inscriptions previously stored in DB for the block  
/// 2) Traverse the list of transaction present in the block (except coinbase).
/// 3) Check if the transaction is present in the cache L1 and augment the cache hit list accordingly and move on to the
/// next transaction.
/// 4) Check if the transaction was processed in the pastand move on to the next transaction.
/// 5) Augment the list of transaction to process.
///
/// # Todos / Optimizations
/// - DB query (inscriptions + locations) could be expensive.
///
fn get_transactions_to_process(
    block: &BitcoinBlockData,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    db_tx: &Transaction,
    ctx: &Context,
) -> (
    HashSet<(TransactionIdentifier, usize, u64)>,
    Vec<(TransactionIdentifier, usize, u64)>,
) {
    let mut transactions_ids = HashSet::new();
    let mut l1_cache_hits = vec![];

    for tx in block.transactions.iter().skip(1) {
        let inputs = tx
            .metadata
            .inputs
            .iter()
            .map(|i| i.previous_output.value)
            .collect::<Vec<u64>>();

        // Have a new inscription been revealed, if so, are looking at a re-inscription
        for ordinal_event in tx.metadata.ordinal_operations.iter() {
            let inscription_data = match ordinal_event {
                OrdinalOperation::InscriptionRevealed(inscription_data) => inscription_data,
                OrdinalOperation::InscriptionTransferred(_) => {
                    continue;
                }
            };

            let (input_index, relative_offset) = match inscription_data.inscription_pointer {
                Some(pointer) => resolve_absolute_pointer(&inputs, pointer),
                None => (inscription_data.inscription_input_index, 0),
            };

            let key = (
                tx.transaction_identifier.clone(),
                input_index,
                relative_offset,
            );
            if cache_l1.contains_key(&key) {
                l1_cache_hits.push(key);
                continue;
            }

            if transactions_ids.contains(&key) {
                continue;
            }

            // Enqueue for traversals
            transactions_ids.insert(key);
        }
    }
    (transactions_ids, l1_cache_hits)
}

/// Helper caching inscription sequence cursor
///
/// When attributing an inscription number to a new inscription, retrieving the next inscription number to use (both for
/// blessed and cursed sequence) is an expensive operation, challenging to optimize from a SQL point of view.
/// This structure is wrapping the expensive SQL query and helping us keeping track of the next inscription number to
/// use.
///
pub struct SequenceCursor {
    pos_cursor: Option<i64>,
    neg_cursor: Option<i64>,
    jubilee_cursor: Option<i64>,
    current_block_height: u64,
}

impl SequenceCursor {
    pub fn new() -> Self {
        SequenceCursor {
            jubilee_cursor: None,
            pos_cursor: None,
            neg_cursor: None,
            current_block_height: 0,
        }
    }

    pub fn reset(&mut self) {
        self.pos_cursor = None;
        self.neg_cursor = None;
        self.jubilee_cursor = None;
        self.current_block_height = 0;
    }

    pub async fn pick_next(
        &mut self,
        cursed: bool,
        block_height: u64,
        network: &Network,
        db_tx: &Transaction<'_>,
    ) -> Result<OrdinalInscriptionNumber, String> {
        if block_height < self.current_block_height {
            self.reset();
        }
        self.current_block_height = block_height;

        let classic = match cursed {
            true => self.pick_next_neg_classic(db_tx).await?,
            false => self.pick_next_pos_classic(db_tx).await?,
        };

        let jubilee = if block_height >= get_jubilee_block_height(&network) {
            self.pick_next_jubilee_number(db_tx).await?
        } else {
            classic
        };
        Ok(OrdinalInscriptionNumber { classic, jubilee })
    }

    pub async fn increment(&mut self, cursed: bool, db_tx: &Transaction<'_>) -> Result<(), String> {
        self.increment_jubilee_number(db_tx).await?;
        if cursed {
            self.increment_neg_classic(db_tx).await?;
        } else {
            self.increment_pos_classic(db_tx).await?;
        };
        Ok(())
    }

    async fn pick_next_pos_classic(&mut self, db_tx: &Transaction<'_>) -> Result<i64, String> {
        match self.pos_cursor {
            None => {
                match ordinals_pg::get_highest_blessed_classic_inscription_number(db_tx).await? {
                    Some(inscription_number) => {
                        self.pos_cursor = Some(inscription_number);
                        Ok(inscription_number + 1)
                    }
                    _ => Ok(0),
                }
            }
            Some(value) => Ok(value + 1),
        }
    }

    async fn pick_next_jubilee_number(&mut self, db_tx: &Transaction<'_>) -> Result<i64, String> {
        match self.jubilee_cursor {
            None => match ordinals_pg::get_highest_inscription_number(db_tx).await? {
                Some(inscription_number) => {
                    self.jubilee_cursor = Some(inscription_number as i64);
                    Ok(inscription_number as i64 + 1)
                }
                _ => Ok(0),
            },
            Some(value) => Ok(value + 1),
        }
    }

    async fn pick_next_neg_classic(&mut self, db_tx: &Transaction<'_>) -> Result<i64, String> {
        match self.neg_cursor {
            None => match ordinals_pg::get_lowest_cursed_classic_inscription_number(db_tx).await? {
                Some(inscription_number) => {
                    self.neg_cursor = Some(inscription_number);
                    Ok(inscription_number - 1)
                }
                _ => Ok(-1),
            },
            Some(value) => Ok(value - 1),
        }
    }

    async fn increment_neg_classic(&mut self, db_tx: &Transaction<'_>) -> Result<(), String> {
        self.neg_cursor = Some(self.pick_next_neg_classic(db_tx).await?);
        Ok(())
    }

    async fn increment_pos_classic(&mut self, db_tx: &Transaction<'_>) -> Result<(), String> {
        self.pos_cursor = Some(self.pick_next_pos_classic(db_tx).await?);
        Ok(())
    }

    async fn increment_jubilee_number(&mut self, db_tx: &Transaction<'_>) -> Result<(), String> {
        self.jubilee_cursor = Some(self.pick_next_jubilee_number(db_tx).await?);
        Ok(())
    }
}

pub fn get_jubilee_block_height(network: &Network) -> u64 {
    match network {
        Network::Bitcoin => 824544,
        Network::Regtest => 110,
        Network::Signet => 175392,
        Network::Testnet => 2544192,
        _ => unreachable!(),
    }
}

pub fn get_bitcoin_network(network: &BitcoinNetwork) -> Network {
    match network {
        BitcoinNetwork::Mainnet => Network::Bitcoin,
        BitcoinNetwork::Regtest => Network::Regtest,
        BitcoinNetwork::Testnet => Network::Testnet,
        BitcoinNetwork::Signet => Network::Signet,
    }
}

/// Given a `BitcoinBlockData` that have been augmented with the functions `parse_inscriptions_in_raw_tx`,
/// `parse_inscriptions_in_standardized_tx` or `parse_inscriptions_and_standardize_block`, mutate the ordinals drafted
/// informations with actual, consensus data.
pub async fn augment_block_with_inscriptions(
    block: &mut BitcoinBlockData,
    sequence_cursor: &mut SequenceCursor,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<bool, String> {
    // Handle re-inscriptions
    let mut reinscriptions_data = HashMap::new();
    for (_, inscription_data) in inscriptions_data.iter() {
        if inscription_data.ordinal_number != 0 {
            // TODO(rafaelcr): This seems like an expensive call to do all the time. Can we optimize?
            if let Some(inscription_id) =
                ordinals_pg::get_blessed_inscription_id_for_ordinal_number(
                    db_tx,
                    inscription_data.ordinal_number,
                )
                .await?
            {
                reinscriptions_data.insert(inscription_data.ordinal_number, inscription_id);
            }
        }
    }

    // Handle sat oveflows
    let mut sats_overflows = VecDeque::new();
    let mut any_event = false;

    let network = get_bitcoin_network(&block.metadata.network);
    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_tx = &block.transactions[0].clone();
    let mut cumulated_fees = 0u64;

    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        any_event |= augment_transaction_with_ordinals_inscriptions_data(
            tx,
            tx_index,
            &block.block_identifier,
            sequence_cursor,
            &network,
            inscriptions_data,
            coinbase_tx,
            coinbase_subsidy,
            &mut cumulated_fees,
            &mut sats_overflows,
            &mut reinscriptions_data,
            db_tx,
            ctx,
        )
        .await?;
    }

    // Handle sats overflow
    while let Some((tx_index, op_index)) = sats_overflows.pop_front() {
        let OrdinalOperation::InscriptionRevealed(ref mut inscription_data) =
            block.transactions[tx_index].metadata.ordinal_operations[op_index]
        else {
            continue;
        };
        let is_cursed = inscription_data.curse_type.is_some();
        let inscription_number = sequence_cursor
            .pick_next(is_cursed, block.block_identifier.index, &network, db_tx)
            .await?;
        inscription_data.inscription_number = inscription_number;

        sequence_cursor.increment(is_cursed, db_tx).await?;
        try_info!(
            ctx,
            "Unbound inscription {} (#{}) detected on Satoshi {} (block #{}, {} transfers)",
            inscription_data.inscription_id,
            inscription_data.get_inscription_number(),
            inscription_data.ordinal_number,
            block.block_identifier.index,
            inscription_data.transfers_pre_inscription,
        );
    }
    Ok(any_event)
}

/// Given a `BitcoinTransactionData` that have been augmented with the functions `parse_inscriptions_in_raw_tx` or
/// `parse_inscriptions_in_standardized_tx`,  mutate the ordinals drafted informations with actual, consensus data, by
/// using informations from `inscription_data` and `reinscription_data`.
///
/// Transactions are not fully correct from a consensus point of view state transient state after the execution of this
/// function.
async fn augment_transaction_with_ordinals_inscriptions_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    block_identifier: &BlockIdentifier,
    sequence_cursor: &mut SequenceCursor,
    network: &Network,
    inscriptions_data: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    coinbase_tx: &BitcoinTransactionData,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    sats_overflows: &mut VecDeque<(usize, usize)>,
    reinscriptions_data: &mut HashMap<u64, String>,
    db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<bool, String> {
    let inputs = tx
        .metadata
        .inputs
        .iter()
        .map(|i| i.previous_output.value)
        .collect::<Vec<u64>>();

    let any_event = tx.metadata.ordinal_operations.is_empty() == false;
    let mut mutated_operations = vec![];
    mutated_operations.append(&mut tx.metadata.ordinal_operations);
    let mut inscription_subindex = 0;
    for (op_index, op) in mutated_operations.iter_mut().enumerate() {
        let (mut is_cursed, inscription) = match op {
            OrdinalOperation::InscriptionRevealed(inscription) => {
                (inscription.curse_type.as_ref().is_some(), inscription)
            }
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let (input_index, relative_offset) = match inscription.inscription_pointer {
            Some(pointer) => resolve_absolute_pointer(&inputs, pointer),
            None => (inscription.inscription_input_index, 0),
        };

        let transaction_identifier = tx.transaction_identifier.clone();
        let inscription_id = format_inscription_id(&transaction_identifier, inscription_subindex);
        let traversal =
            match inscriptions_data.get(&(transaction_identifier, input_index, relative_offset)) {
                Some(traversal) => traversal,
                None => {
                    let err_msg = format!(
                        "Unable to retrieve backward traversal result for inscription {}",
                        tx.transaction_identifier.hash
                    );
                    try_error!(ctx, "{}", err_msg);
                    std::process::exit(1);
                }
            };

        // Do we need to curse the inscription?
        let mut inscription_number = sequence_cursor
            .pick_next(is_cursed, block_identifier.index, network, db_tx)
            .await?;
        let mut curse_type_override = None;
        if !is_cursed {
            // Is this inscription re-inscribing an existing blessed inscription?
            if let Some(exisiting_inscription_id) =
                reinscriptions_data.get(&traversal.ordinal_number)
            {
                try_info!(
                    ctx,
                    "Satoshi #{} was inscribed with blessed inscription {}, cursing inscription {}",
                    traversal.ordinal_number,
                    exisiting_inscription_id,
                    traversal.get_inscription_id(),
                );

                is_cursed = true;
                inscription_number = sequence_cursor
                    .pick_next(is_cursed, block_identifier.index, network, db_tx)
                    .await?;
                curse_type_override = Some(OrdinalInscriptionCurseType::Reinscription)
            }
        };

        inscription.inscription_id = inscription_id;
        inscription.inscription_number = inscription_number;
        inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
        inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
        inscription.ordinal_number = traversal.ordinal_number;
        inscription.transfers_pre_inscription = traversal.transfers;
        inscription.inscription_fee = tx.metadata.fee;
        inscription.tx_index = tx_index;
        inscription.curse_type = match curse_type_override {
            Some(curse_type) => Some(curse_type),
            None => inscription.curse_type.take(),
        };

        let (destination, satpoint_post_transfer, output_value) = compute_satpoint_post_transfer(
            &&*tx,
            input_index,
            relative_offset,
            network,
            coinbase_tx,
            coinbase_subsidy,
            cumulated_fees,
            ctx,
        );

        // Compute satpoint_post_inscription
        inscription.satpoint_post_inscription = satpoint_post_transfer;
        inscription_subindex += 1;

        match destination {
            OrdinalInscriptionTransferDestination::SpentInFees => {
                // Inscriptions are assigned inscription numbers starting at zero, first by the
                // order reveal transactions appear in blocks, and the order that reveal envelopes
                // appear in those transactions.
                // Due to a historical bug in `ord` which cannot be fixed without changing a great
                // many inscription numbers, inscriptions which are revealed and then immediately
                // spent to fees are numbered as if they appear last in the block in which they
                // are revealed.
                sats_overflows.push_back((tx_index, op_index));
                continue;
            }
            OrdinalInscriptionTransferDestination::Burnt(_) => {}
            OrdinalInscriptionTransferDestination::Transferred(address) => {
                inscription.inscription_output_value = output_value.unwrap_or(0);
                inscription.inscriber_address = Some(address);
            }
        };

        // The reinscriptions_data needs to be augmented as we go, to handle transaction chaining.
        if !is_cursed {
            reinscriptions_data.insert(traversal.ordinal_number, traversal.get_inscription_id());
        }

        try_info!(
            ctx,
            "Inscription {} (#{}) detected on Satoshi {} (block #{}, {} transfers)",
            inscription.inscription_id,
            inscription.get_inscription_number(),
            inscription.ordinal_number,
            block_identifier.index,
            inscription.transfers_pre_inscription,
        );

        sequence_cursor.increment(is_cursed, db_tx).await?;
    }
    tx.metadata
        .ordinal_operations
        .append(&mut mutated_operations);

    Ok(any_event)
}

/// Best effort to re-augment a `BitcoinTransactionData` with data coming from `inscriptions` and `locations` tables.
/// Some informations are being lost (curse_type).
fn consolidate_transaction_with_pre_computed_inscription_data(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    coinbase_tx: &BitcoinTransactionData,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    network: &Network,
    inscriptions_data: &mut BTreeMap<String, TraversalResult>,
    ctx: &Context,
) {
    let mut subindex = 0;
    let mut mutated_operations = vec![];
    mutated_operations.append(&mut tx.metadata.ordinal_operations);

    let inputs = tx
        .metadata
        .inputs
        .iter()
        .map(|i| i.previous_output.value)
        .collect::<Vec<u64>>();

    for operation in mutated_operations.iter_mut() {
        let inscription = match operation {
            OrdinalOperation::InscriptionRevealed(ref mut inscription) => inscription,
            OrdinalOperation::InscriptionTransferred(_) => continue,
        };

        let inscription_id = format_inscription_id(&tx.transaction_identifier, subindex);
        let Some(traversal) = inscriptions_data.get(&inscription_id) else {
            // Should we remove the operation instead
            continue;
        };
        subindex += 1;

        inscription.inscription_id = inscription_id.clone();
        inscription.ordinal_offset = traversal.get_ordinal_coinbase_offset();
        inscription.ordinal_block_height = traversal.get_ordinal_coinbase_height();
        inscription.ordinal_number = traversal.ordinal_number;
        inscription.inscription_number = traversal.inscription_number.clone();
        inscription.transfers_pre_inscription = traversal.transfers;
        inscription.inscription_fee = tx.metadata.fee;
        inscription.tx_index = tx_index;

        let (input_index, relative_offset) = match inscription.inscription_pointer {
            Some(pointer) => resolve_absolute_pointer(&inputs, pointer),
            None => (traversal.inscription_input_index, 0),
        };
        // Compute satpoint_post_inscription
        let (destination, satpoint_post_transfer, output_value) = compute_satpoint_post_transfer(
            tx,
            input_index,
            relative_offset,
            network,
            coinbase_tx,
            coinbase_subsidy,
            cumulated_fees,
            ctx,
        );

        inscription.satpoint_post_inscription = satpoint_post_transfer;

        if inscription.inscription_number.classic < 0 {
            inscription.curse_type = Some(OrdinalInscriptionCurseType::Generic);
        }

        match destination {
            OrdinalInscriptionTransferDestination::SpentInFees => continue,
            OrdinalInscriptionTransferDestination::Burnt(_) => continue,
            OrdinalInscriptionTransferDestination::Transferred(address) => {
                inscription.inscription_output_value = output_value.unwrap_or(0);
                inscription.inscriber_address = Some(address);
            }
        }
    }
    tx.metadata
        .ordinal_operations
        .append(&mut mutated_operations);
}

/// Best effort to re-augment a `BitcoinBlockData` with data coming from `inscriptions` and `locations` tables.
/// Some informations are being lost (curse_type).
pub async fn augment_block_with_pre_computed_ordinals_data(
    block: &mut BitcoinBlockData,
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    with_pg_transaction(
        &config.ordinals_db.to_conn_config(),
        ctx,
        |client| async move {
            let network = get_bitcoin_network(&block.metadata.network);
            let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
            let coinbase_tx = &block.transactions[0].clone();
            let mut cumulated_fees = 0;

            // TODO(rafaelcr): Now that we use postgres we should be able to pull all the previous data instead of `TraversalResult`
            let mut inscriptions_data =
                ordinals_pg::get_inscriptions_at_block(client, block.block_identifier.index)
                    .await?;
            for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
                consolidate_transaction_with_pre_computed_inscription_data(
                    tx,
                    tx_index,
                    &coinbase_tx,
                    coinbase_subsidy,
                    &mut cumulated_fees,
                    &network,
                    &mut inscriptions_data,
                    ctx,
                );
                let _ = augment_transaction_with_ordinal_transfers(
                    tx,
                    tx_index,
                    &network,
                    &coinbase_tx,
                    coinbase_subsidy,
                    &mut cumulated_fees,
                    client,
                    ctx,
                )
                .await?;
            }

            // BRC-20
            if let (true, Some(brc20_db)) = (config.meta_protocols.brc20, &config.brc20_db) {
                with_pg_transaction(&brc20_db.to_conn_config(), ctx, |bclient| async move {
                    Ok(brc20_pg::augment_block_with_operations(block, bclient).await?)
                })
                .await?;
            }

            Ok(())
        },
    )
    .await?;
    Ok(())
}

// #[cfg(test)]
// mod test {
//     use chainhook_sdk::{
//         types::{
//             BlockIdentifier, OrdinalInscriptionNumber, OrdinalInscriptionRevealData,
//             OrdinalOperation,
//         },
//         utils::Context,
//     };

//     use crate::{
//         config::Config,
//         core::test_builders::{
//             TestBlockBuilder, TestTransactionBuilder, TestTxInBuilder, TestTxOutBuilder,
//         },
//         db::{drop_all_dbs, initialize_sqlite_dbs, ordinals::insert_entry_in_inscriptions},
//     };

//     use super::augment_block_with_pre_computed_ordinals_data;

//     #[test]
//     fn consolidates_block_with_pre_computed_data() {
//         let ctx = Context::empty();
//         let config = Config::test_default();
//         drop_all_dbs(&config);
//         let mut sqlite_dbs = initialize_sqlite_dbs(&config, &ctx);

//         // Prepare DB data. Set blank values because we will fill these out after augmenting.
//         let reveal = OrdinalInscriptionRevealData {
//             content_bytes: "0x7b200a20202270223a20226272632d3230222c0a2020226f70223a20226465706c6f79222c0a2020227469636b223a20226f726469222c0a2020226d6178223a20223231303030303030222c0a2020226c696d223a202231303030220a7d".to_string(),
//             content_type: "text/plain;charset=utf-8".to_string(),
//             content_length: 94,
//             inscription_number: OrdinalInscriptionNumber { classic: 0, jubilee: 0 },
//             inscription_fee: 0,
//             inscription_output_value: 9000,
//             inscription_id: "c62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9i0".to_string(),
//             inscription_input_index: 0,
//             inscription_pointer: Some(0),
//             inscriber_address: Some("bc1qte0s6pz7gsdlqq2cf6hv5mxcfksykyyyjkdfd5".to_string()),
//             delegate: None,
//             metaprotocol: None,
//             metadata: None,
//             parent: None,
//             ordinal_number: 1971874687500000,
//             ordinal_block_height: 849999,
//             ordinal_offset: 0,
//             tx_index: 0,
//             transfers_pre_inscription: 0,
//             satpoint_post_inscription: "c62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9:0:0".to_string(),
//             curse_type: None,
//         };
//         insert_entry_in_inscriptions(
//             &reveal,
//             &BlockIdentifier {
//                 index: 850000,
//                 hash: "0x000000000000000000029854dcc8becfd64a352e1d2b1f1d3bb6f101a947af0e"
//                     .to_string(),
//             },
//             &sqlite_dbs.ordinals,
//             &ctx,
//         );

//         let mut block = TestBlockBuilder::new()
//             .height(850000)
//             .hash("0x000000000000000000029854dcc8becfd64a352e1d2b1f1d3bb6f101a947af0e".to_string())
//             .add_transaction(TestTransactionBuilder::new().build())
//             .add_transaction(
//                 TestTransactionBuilder::new()
//                     .hash(
//                         "0xc62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9"
//                             .to_string(),
//                     )
//                     .ordinal_operations(vec![OrdinalOperation::InscriptionRevealed(reveal)])
//                     .add_input(TestTxInBuilder::new().build())
//                     .add_output(TestTxOutBuilder::new().value(9000).build())
//                     .build(),
//             )
//             .build();

//         let inscriptions_db_tx = &sqlite_dbs.ordinals.transaction().unwrap();
//         augment_block_with_pre_computed_ordinals_data(
//             &mut block,
//             &inscriptions_db_tx,
//             true,
//             None,
//             &ctx,
//         );

//         let OrdinalOperation::InscriptionRevealed(reveal) =
//             &block.transactions[1].metadata.ordinal_operations[0]
//         else {
//             unreachable!();
//         };
//         assert_eq!(
//             reveal.inscription_id,
//             "c62d436323e14cdcb91dd21cb7814fd1ac5b9ecb6e3cc6953b54c02a343f7ec9i0"
//         );
//         assert_eq!(reveal.ordinal_offset, 0);
//         assert_eq!(reveal.ordinal_block_height, 849999);
//         assert_eq!(reveal.ordinal_number, 1971874687500000);
//         assert_eq!(reveal.transfers_pre_inscription, 0);
//         assert_eq!(reveal.inscription_fee, 0);
//         assert_eq!(reveal.tx_index, 1);
//     }

//     mod cursor {
//         use chainhook_sdk::{bitcoin::Network, utils::Context};

//         use test_case::test_case;

//         use crate::{
//             config::Config,
//             core::protocol::inscription_sequencing::SequenceCursor,
//             core::test_builders::{TestBlockBuilder, TestTransactionBuilder},
//             db::{
//                 drop_all_dbs, initialize_sqlite_dbs, ordinals::update_sequence_metadata_with_block,
//             },
//         };

//         #[test_case((780000, false) => (2, 2); "with blessed pre jubilee")]
//         #[test_case((780000, true) => (-2, -2); "with cursed pre jubilee")]
//         #[test_case((850000, false) => (2, 2); "with blessed post jubilee")]
//         #[test_case((850000, true) => (-2, 2); "with cursed post jubilee")]
//         fn picks_next((block_height, cursed): (u64, bool)) -> (i64, i64) {
//             let ctx = Context::empty();
//             let config = Config::test_default();
//             drop_all_dbs(&config);
//             let db_conns = initialize_sqlite_dbs(&config, &ctx);
//             let mut block = TestBlockBuilder::new()
//                 .transactions(vec![TestTransactionBuilder::new_with_operation().build()])
//                 .build();
//             block.block_identifier.index = block_height;

//             // Pick next twice so we can test all cases.
//             update_sequence_metadata_with_block(&block, &db_conns.ordinals, &ctx);
//             let mut cursor = SequenceCursor::new(&db_conns.ordinals);
//             let _ = cursor.pick_next(
//                 cursed,
//                 block.block_identifier.index + 1,
//                 &Network::Bitcoin,
//                 &ctx,
//             );
//             cursor.increment(cursed, &ctx);

//             block.block_identifier.index = block.block_identifier.index + 1;
//             update_sequence_metadata_with_block(&block, &db_conns.ordinals, &ctx);
//             let next = cursor.pick_next(
//                 cursed,
//                 block.block_identifier.index + 1,
//                 &Network::Bitcoin,
//                 &ctx,
//             );

//             (next.classic, next.jubilee)
//         }

//         #[test]
//         fn resets_on_previous_block() {
//             let ctx = Context::empty();
//             let config = Config::test_default();
//             drop_all_dbs(&config);
//             let db_conns = initialize_sqlite_dbs(&config, &ctx);
//             let block = TestBlockBuilder::new()
//                 .transactions(vec![TestTransactionBuilder::new_with_operation().build()])
//                 .build();
//             update_sequence_metadata_with_block(&block, &db_conns.ordinals, &ctx);
//             let mut cursor = SequenceCursor::new(&db_conns.ordinals);
//             let _ = cursor.pick_next(
//                 false,
//                 block.block_identifier.index + 1,
//                 &Network::Bitcoin,
//                 &ctx,
//             );
//             cursor.increment(false, &ctx);

//             cursor.reset();
//             let next = cursor.pick_next(
//                 false,
//                 block.block_identifier.index - 10,
//                 &Network::Bitcoin,
//                 &ctx,
//             );
//             assert_eq!(next.classic, 0);
//             assert_eq!(next.jubilee, 0);
//         }
//     }
// }
