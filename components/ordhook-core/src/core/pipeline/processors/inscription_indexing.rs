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
            satoshi_numbering::TraversalResult,
            satoshi_tracking::augment_block_with_ordinals_transfer_data,
        },
    },
    db::{
        blocks::open_blocks_db_with_retry,
        cursor::TransactionBytesCursor,
        ordinals::{
            get_any_entry_in_ordinal_activities, get_latest_indexed_inscription_number,
            open_readonly_ordhook_db_conn, open_readwrite_ordhook_db_conn,
        },
    },
    service::write_brc20_block_operations,
    try_error, try_info,
    utils::monitoring::PrometheusMonitoring,
};

use crate::{
    config::Config,
    core::{
        new_traversals_lazy_cache,
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    },
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
                    let blocks_db_rw = open_blocks_db_with_retry(true, &config, &ctx);
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
                    &post_processor,
                    &prometheus,
                    &config,
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
    post_processor: &Option<Sender<BitcoinBlockData>>,
    prometheus: &PrometheusMonitoring,
    config: &Config,
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
            config,
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
    config: &Config,
    ctx: &Context,
) -> Result<(), String> {
    // Parsed BRC20 ops will be deposited here for this block.
    let mut brc20_operation_map = HashMap::new();
    parse_inscriptions_in_standardized_block(block, &mut brc20_operation_map, config, &ctx);

    let any_processable_transactions = parallelize_inscription_data_computations(
        &block,
        &next_blocks,
        cache_l1,
        cache_l2,
        inscriptions_db_tx,
        config,
        ctx,
    )?;

    let inner_ctx = if config.logs.ordinals_internals {
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

    use chainhook_sdk::{types::OrdinalOperation, utils::Context};

    use crate::{
        config::Config,
        core::{
            meta_protocols::brc20::cache::brc20_new_cache, new_traversals_lazy_cache,
            protocol::inscription_sequencing::SequenceCursor,
        },
        db::{blocks::open_blocks_db_with_retry, ordinals::open_readwrite_ordhook_db_conn},
        drop_databases, initialize_databases,
        utils::{
            monitoring::PrometheusMonitoring,
            test_helpers::{new_test_block, new_test_reveal_tx},
        },
    };

    use super::process_blocks;

    #[test]
    fn process_block_with_inscription() {
        let ctx = Context::empty();
        let config = Config::test_default();

        // Create DBs
        drop_databases(&config);
        let db_conns = initialize_databases(&config, &ctx);
        let _ = open_blocks_db_with_retry(true, &config, &ctx);

        let mut next_blocks = vec![new_test_block(vec![new_test_reveal_tx()])];
        let mut sequence_cursor = SequenceCursor::new(&db_conns.ordhook);
        let cache_l2 = Arc::new(new_traversals_lazy_cache(2048));
        let mut inscriptions_db_conn_rw =
            open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx).expect("");

        let results = process_blocks(
            &mut next_blocks,
            &mut sequence_cursor,
            &cache_l2,
            &mut inscriptions_db_conn_rw,
            &mut None,
            &mut None,
            &None,
            &PrometheusMonitoring::new(),
            &config,
            &ctx,
        );

        assert_eq!(results.len(), 1);
        let transactions = &results[0].transactions;
        assert_eq!(transactions.len(), 1);
        let parsed_tx = &transactions[0];
        assert_eq!(parsed_tx.metadata.ordinal_operations.len(), 1);
        let OrdinalOperation::InscriptionRevealed(reveal) =
            &parsed_tx.metadata.ordinal_operations[0]
        else {
            panic!();
        };
        assert_eq!(
            reveal.inscription_id,
            "b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735i0".to_string()
        );
        assert_eq!(reveal.content_bytes, "0x7b200a20202270223a20226272632d3230222c0a2020226f70223a20226465706c6f79222c0a2020227469636b223a20226f726469222c0a2020226d6178223a20223231303030303030222c0a2020226c696d223a202231303030220a7d".to_string());
        assert_eq!(reveal.content_length, 94);
    }

    #[test]
    fn process_block_with_brc20_inscription() {
        let ctx = Context::empty();
        let mut config = Config::mainnet_default();
        config.storage.working_dir = "tmp".to_string();
        config.meta_protocols.brc20 = true;
        drop_databases(&config);
        let mut db_conns = initialize_databases(&config, &ctx);
        let mut next_blocks = vec![new_test_block(vec![new_test_reveal_tx()])];
        let mut sequence_cursor = SequenceCursor::new(&db_conns.ordhook);
        let cache_l2 = Arc::new(new_traversals_lazy_cache(2048));
        let mut inscriptions_db_conn_rw =
            open_readwrite_ordhook_db_conn(&config.expected_cache_path(), &ctx).expect("");
        let mut brc20_cache = brc20_new_cache(&config);

        let _ = process_blocks(
            &mut next_blocks,
            &mut sequence_cursor,
            &cache_l2,
            &mut inscriptions_db_conn_rw,
            &mut brc20_cache,
            &mut db_conns.brc20,
            &None,
            &PrometheusMonitoring::new(),
            &config,
            &ctx,
        );

        // let op = get_brc20_operations_on_block(838964, &db_conns.brc20.unwrap(), &ctx);
        // assert_eq!(op.len(), 1);
    }
}
