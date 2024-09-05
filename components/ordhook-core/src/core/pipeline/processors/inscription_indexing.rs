use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{
    types::{BitcoinBlockData, TransactionIdentifier},
    utils::Context,
};
use crossbeam_channel::{Sender, TryRecvError};
use rusqlite::Transaction;

use dashmap::DashMap;
use fxhash::FxHasher;
use rusqlite::Connection;
use std::hash::BuildHasherDefault;

use crate::{
    core::{
        meta_protocols::brc20::{
            cache::{brc20_new_cache, Brc20MemoryCache},
            db::brc20_new_rw_db_conn,
        },
        pipeline::processors::block_archiving::store_compacted_blocks,
        protocol::{
            inscription_parsing::{
                get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
                parse_inscriptions_in_standardized_block,
            },
            inscription_sequencing::{
                augment_block_with_ordinals_inscriptions_data_and_write_to_db_tx,
                get_bitcoin_network, get_jubilee_block_height,
                parallelize_inscription_data_computations, SequenceCursor,
            },
            satoshi_tracking::augment_block_with_ordinals_transfer_data,
        },
        OrdhookConfig,
    },
    db::{
        get_any_entry_in_ordinal_activities, get_latest_indexed_inscription_number,
        open_ordhook_db_conn_rocks_db_loop, open_readonly_ordhook_db_conn,
    },
    service::write_brc20_block_operations,
    try_error, try_info,
    utils::monitoring::PrometheusMonitoring,
};

use crate::db::{TransactionBytesCursor, TraversalResult};

use crate::{
    config::Config,
    core::{
        new_traversals_lazy_cache,
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    },
    db::open_readwrite_ordhook_db_conn,
};

pub fn start_inscription_indexing_processor(
    config: &Config,
    ctx: &Context,
    post_processor: Option<Sender<BitcoinBlockData>>,
    prometheus: &PrometheusMonitoring,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let prometheus = prometheus.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Inscription indexing runloop")
        .spawn(move || {
            let cache_l2 = Arc::new(new_traversals_lazy_cache(2048));
            let garbage_collect_every_n_blocks = 100;
            let mut garbage_collect_nth_block = 0;

            let mut inscriptions_db_conn_rw =
                open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let ordhook_config = config.get_ordhook_config();
            let mut empty_cycles = 0;

            let inscriptions_db_conn =
                open_readonly_ordhook_db_conn(&config.expected_cache_path(), &ctx).unwrap();
            let mut sequence_cursor = SequenceCursor::new(&inscriptions_db_conn);

            let mut brc20_cache = brc20_new_cache(&config);
            let mut brc20_db_conn_rw = brc20_new_rw_db_conn(&config, &ctx);

            loop {
                let (compacted_blocks, mut blocks) = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(compacted_blocks, blocks)) => {
                        empty_cycles = 0;
                        (compacted_blocks, blocks)
                    }
                    Ok(PostProcessorCommand::Terminate) => {
                        let _ = events_tx.send(PostProcessorEvent::Terminated);
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;
                            if empty_cycles == 180 {
                                try_info!(ctx, "Block processor reached expiration");
                                let _ = events_tx.send(PostProcessorEvent::Expired);
                                break;
                            }
                            sleep(Duration::from_secs(1));
                            continue;
                        }
                        _ => {
                            break;
                        }
                    },
                };

                {
                    let blocks_db_rw = open_ordhook_db_conn_rocks_db_loop(
                        true,
                        &config.expected_cache_path(),
                        config.resources.ulimit,
                        config.resources.memory_available,
                        &ctx,
                    );
                    store_compacted_blocks(
                        compacted_blocks,
                        true,
                        &blocks_db_rw,
                        &Context::empty(),
                    );
                }

                // Early return
                if blocks.is_empty() {
                    continue;
                }

                try_info!(ctx, "Processing {} blocks", blocks.len());
                blocks = process_blocks(
                    &mut blocks,
                    &mut sequence_cursor,
                    &cache_l2,
                    &mut inscriptions_db_conn_rw,
                    &mut brc20_cache,
                    &mut brc20_db_conn_rw,
                    &ordhook_config,
                    &post_processor,
                    &prometheus,
                    &ctx,
                );

                garbage_collect_nth_block += blocks.len();
                if garbage_collect_nth_block > garbage_collect_every_n_blocks {
                    try_info!(ctx, "Performing garbage collecting");

                    // Clear L2 cache on a regular basis
                    try_info!(ctx, "Clearing cache L2 ({} entries)", cache_l2.len());
                    cache_l2.clear();

                    // Recreate sqlite db connection on a regular basis
                    inscriptions_db_conn_rw =
                        open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx)
                            .unwrap();
                    inscriptions_db_conn_rw.flush_prepared_statement_cache();
                    garbage_collect_nth_block = 0;
                }
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}

pub fn process_blocks(
    next_blocks: &mut Vec<BitcoinBlockData>,
    sequence_cursor: &mut SequenceCursor,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_conn_rw: &mut Connection,
    brc20_cache: &mut Option<Brc20MemoryCache>,
    brc20_db_conn_rw: &mut Option<Connection>,
    ordhook_config: &OrdhookConfig,
    post_processor: &Option<Sender<BitcoinBlockData>>,
    prometheus: &PrometheusMonitoring,
    ctx: &Context,
) -> Vec<BitcoinBlockData> {
    let mut cache_l1 = BTreeMap::new();
    let mut updated_blocks = vec![];

    for _cursor in 0..next_blocks.len() {
        let inscriptions_db_tx = inscriptions_db_conn_rw.transaction().unwrap();
        let brc20_db_tx = brc20_db_conn_rw.as_mut().map(|c| c.transaction().unwrap());

        let mut block = next_blocks.remove(0);

        // We check before hand if some data were pre-existing, before processing
        // Always discard if we have some existing content at this block height (inscription or transfers)
        let any_existing_activity = get_any_entry_in_ordinal_activities(
            &block.block_identifier.index,
            &inscriptions_db_tx,
            ctx,
        );

        // Invalidate and recompute cursor when crossing the jubilee height
        let jubilee_height =
            get_jubilee_block_height(&get_bitcoin_network(&block.metadata.network));
        if block.block_identifier.index == jubilee_height {
            sequence_cursor.reset();
        }

        let _ = process_block(
            &mut block,
            &next_blocks,
            sequence_cursor,
            &mut cache_l1,
            cache_l2,
            &inscriptions_db_tx,
            brc20_db_tx.as_ref(),
            brc20_cache.as_mut(),
            prometheus,
            ordhook_config,
            ctx,
        );

        let inscriptions_revealed = get_inscriptions_revealed_in_block(&block)
            .iter()
            .map(|d| d.get_inscription_number().to_string())
            .collect::<Vec<String>>();

        let inscriptions_transferred = get_inscriptions_transferred_in_block(&block).len();

        try_info!(
            ctx,
            "Block #{} processed, revealed {} inscriptions [{}] and {inscriptions_transferred} transfers",
            block.block_identifier.index,
            inscriptions_revealed.len(),
            inscriptions_revealed.join(", ")
        );

        if any_existing_activity {
            try_error!(
                ctx,
                "Dropping updates for block #{}, activities present in database",
                block.block_identifier.index,
            );
            let _ = inscriptions_db_tx.rollback();
            let _ = brc20_db_tx.map(|t| t.rollback());
        } else {
            match inscriptions_db_tx.commit() {
                Ok(_) => {
                    if let Some(brc20_db_tx) = brc20_db_tx {
                        match brc20_db_tx.commit() {
                            Ok(_) => {}
                            Err(_) => {
                                // TODO: Synchronize rollbacks and commits between BRC-20 and inscription DBs.
                                todo!()
                            }
                        }
                    }
                }
                Err(e) => {
                    try_error!(
                        ctx,
                        "Unable to update changes in block #{}: {}",
                        block.block_identifier.index,
                        e.to_string()
                    );
                }
            }
        }

        if let Some(post_processor_tx) = post_processor {
            let _ = post_processor_tx.send(block.clone());
        }
        updated_blocks.push(block);
    }
    updated_blocks
}

pub fn process_block(
    block: &mut BitcoinBlockData,
    next_blocks: &Vec<BitcoinBlockData>,
    sequence_cursor: &mut SequenceCursor,
    cache_l1: &mut BTreeMap<(TransactionIdentifier, usize, u64), TraversalResult>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    inscriptions_db_tx: &Transaction,
    brc20_db_tx: Option<&Transaction>,
    brc20_cache: Option<&mut Brc20MemoryCache>,
    prometheus: &PrometheusMonitoring,
    ordhook_config: &OrdhookConfig,
    ctx: &Context,
) -> Result<(), String> {
    // Parsed BRC20 ops will be deposited here for this block.
    let mut brc20_operation_map = HashMap::new();
    parse_inscriptions_in_standardized_block(block, &mut brc20_operation_map, &ctx);

    let any_processable_transactions = parallelize_inscription_data_computations(
        &block,
        &next_blocks,
        cache_l1,
        cache_l2,
        inscriptions_db_tx,
        &ordhook_config,
        ctx,
    )?;

    let inner_ctx = if ordhook_config.logs.ordinals_internals {
        ctx.clone()
    } else {
        Context::empty()
    };

    // Inscriptions
    if any_processable_transactions {
        let _ = augment_block_with_ordinals_inscriptions_data_and_write_to_db_tx(
            block,
            sequence_cursor,
            cache_l1,
            &inscriptions_db_tx,
            &inner_ctx,
        );
    }
    // Transfers
    let _ = augment_block_with_ordinals_transfer_data(block, inscriptions_db_tx, true, &inner_ctx);
    // BRC-20
    match (brc20_db_tx, brc20_cache) {
        (Some(brc20_db_tx), Some(brc20_cache)) => write_brc20_block_operations(
            block,
            &mut brc20_operation_map,
            brc20_cache,
            brc20_db_tx,
            &ctx,
        ),
        _ => {}
    }

    // Monitoring
    prometheus.metrics_block_indexed(block.block_identifier.index);
    prometheus.metrics_inscription_indexed(
        get_latest_indexed_inscription_number(inscriptions_db_tx, &inner_ctx).unwrap_or(0),
    );

    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chainhook_sdk::{
        types::{
            bitcoin::{OutPoint, TxIn},
            BitcoinBlockData, BitcoinBlockMetadata, BitcoinNetwork, BitcoinTransactionData,
            BitcoinTransactionMetadata, BlockIdentifier, OrdinalInscriptionNumber,
            OrdinalInscriptionRevealData, OrdinalOperation, TransactionIdentifier,
        },
        utils::Context,
    };

    use crate::{
        config::Config,
        core::{new_traversals_lazy_cache, protocol::inscription_sequencing::SequenceCursor},
        db::open_readwrite_ordhook_db_conn,
        initialize_databases,
        utils::monitoring::PrometheusMonitoring,
    };

    use super::process_blocks;

    fn new_test_reveal_tx() -> BitcoinTransactionData {
        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier {
                hash: "0xd0f18a93b9a3056738fda5a746fad2bf34d6af2683d23cd0d149a55839f4a1a6".to_string(),
            },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs: vec![
                    TxIn {
                        previous_output: OutPoint {
                            txid: TransactionIdentifier { hash: "0xc1dd7be2e65d730a4179de6c3e0a405bedd9aa087c67630b7a9ed706c42dcb98".to_string() },
                            vout: 0,
                            value: 20000,
                            block_height: 839007,
                        },
                        script_sig: "".to_string(),
                        sequence: 4294967293,
                        witness: vec![
                            "0x67d6ad98e0fe8ecbc26802b4d8dbc146b82313363aaf3817cce9adc792156f5db91f0e3825c495168c64e8a0200f7d9d6d2be7db1878f5ae1df2ae3d3b0f4165".to_string(),
                            "0x20a4708f4864038fcebb242d49be567064bd5c51da729f93027e663acc1c5f21c9ac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d3800387b2270223a226272632d3230222c226f70223a227472616e73666572222c227469636b223a2250555053222c22616d74223a22353030227d68".to_string(),
                            "0xc1e1574b78ce2c76a1a5e8d705f44dbe4af66360e360852af0ace68bb1ece30899".to_string(),
                        ],
                    },
                ],
                outputs: vec![],
                // outputs: vec![
                //     TxOut { value: 330, script_pubkey: "0x512065d342f532b2c75c57ce9623d471554544b07e890d8e3f5a29e72589748b343c".to_string() },
                //     TxOut { value: 1004, script_pubkey: "0x00141e32a0ba325a0cdc2553840209146d906fa88f5e".to_string() },
                // ],
                ordinal_operations: vec![],
                stacks_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 0,
                index: 0,
            },
        }
    }

    fn new_test_block(transactions: Vec<BitcoinTransactionData>) -> BitcoinBlockData {
        BitcoinBlockData {
            block_identifier: BlockIdentifier {
                index: 838964,
                hash: "0x000000000000000000018ddf8a6484db391fb85c9f9ddc384f03a92729423aaf"
                    .to_string(),
            },
            parent_block_identifier: BlockIdentifier {
                hash: "0x000000000000000000021f8b96d34c0f223281d7d825dd3588c2858c96e689d4"
                    .to_string(),
                index: 838963,
            },
            timestamp: 1712982301,
            transactions,
            metadata: BitcoinBlockMetadata {
                network: BitcoinNetwork::Mainnet,
            },
        }
    }

    #[test]
    fn process() {
        let ctx = Context::empty();
        let mut config = Config::mainnet_default();
        config.storage.working_dir = "tmp".to_string();
        // config.storage.observers_working_dir = "tmp".to_string();
        let db_conns = initialize_databases(&config, &ctx);
        let mut next_blocks = vec![new_test_block(vec![new_test_reveal_tx()])];
        let mut sequence_cursor = SequenceCursor::new(&db_conns.ordhook);
        let cache_l2 = Arc::new(new_traversals_lazy_cache(2048));
        let mut conn =
            open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx).expect("");

        let results = process_blocks(
            &mut next_blocks,
            &mut sequence_cursor,
            &cache_l2,
            &mut conn,
            &mut None,
            &mut None,
            &config.get_ordhook_config(),
            &None,
            &PrometheusMonitoring::new(),
            &ctx,
        );

        assert_eq!(results.len(), 1);
        let transactions = &results[0].transactions;
        assert_eq!(transactions.len(), 1);
        let parsed_tx = &transactions[0];
        assert_eq!(parsed_tx.metadata.ordinal_operations.len(), 1);
        assert_eq!(
            parsed_tx.metadata.ordinal_operations[0],
            OrdinalOperation::InscriptionRevealed(
                OrdinalInscriptionRevealData {
                    content_bytes: "0x7b2270223a226272632d3230222c226f70223a227472616e73666572222c227469636b223a2250555053222c22616d74223a22353030227d".to_string(),
                    content_type: "text/plain;charset=utf-8".to_string(),
                    content_length: 56,
                    inscription_number: OrdinalInscriptionNumber { classic: 0, jubilee: 0 },
                    inscription_fee: 0,
                    inscription_output_value: 0,
                    inscription_id: "d0f18a93b9a3056738fda5a746fad2bf34d6af2683d23cd0d149a55839f4a1a6i0".to_string(),
                    inscription_input_index: 0,
                    inscription_pointer: None,
                    inscriber_address: None,
                    delegate: None,
                    metaprotocol: None,
                    metadata: None,
                    parent: None,
                    ordinal_number: 0,
                    ordinal_block_height: 0,
                    ordinal_offset: 0,
                    tx_index: 0,
                    transfers_pre_inscription: 0,
                    satpoint_post_inscription: "".to_string(),
                    curse_type: None,
                },
            )
        );
    }
}
