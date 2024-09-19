mod http_api;
pub mod observers;
mod runloops;

use crate::config::{Config, PredicatesApi};
use crate::core::meta_protocols::brc20::brc20_activation_height;
use crate::core::meta_protocols::brc20::cache::{brc20_new_cache, Brc20MemoryCache};
use crate::core::meta_protocols::brc20::db::write_augmented_block_to_brc20_db;
use crate::core::meta_protocols::brc20::parser::ParsedBrc20Operation;
use crate::core::meta_protocols::brc20::verifier::{
    verify_brc20_operation, verify_brc20_transfer, VerifiedBrc20Operation,
};
use crate::core::pipeline::bitcoind_download_blocks;
use crate::core::pipeline::processors::block_archiving::start_block_archiving_processor;
use crate::core::pipeline::processors::inscription_indexing::process_block;
use crate::core::pipeline::processors::start_inscription_indexing_processor;
use crate::core::pipeline::processors::transfers_recomputing::start_transfers_recomputing_processor;
use crate::core::protocol::inscription_parsing::{
    get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
};
use crate::core::protocol::inscription_sequencing::SequenceCursor;
use crate::core::{
    first_inscription_height, new_traversals_lazy_cache, should_sync_ordhook_db,
    should_sync_rocks_db,
};
use crate::db::blocks::{
    find_missing_blocks, insert_entry_in_blocks, open_blocks_db_with_retry, run_compaction,
};
use crate::db::cursor::{BlockBytesCursor, TransactionBytesCursor};
use crate::db::ordinals::{
    find_latest_inscription_block_height, get_latest_indexed_inscription_number, open_ordinals_db,
    update_ordinals_db_with_block, update_sequence_metadata_with_block,
};
use crate::db::{drop_block_data_from_all_dbs, open_all_dbs_rw};
use crate::scan::bitcoin::process_block_with_predicates;
use crate::service::observers::create_and_consolidate_chainhook_config_with_predicates;
use crate::service::runloops::start_bitcoin_scan_runloop;
use crate::utils::monitoring::{start_serving_prometheus_metrics, PrometheusMonitoring};
use crate::{try_debug, try_error, try_info};
use chainhook_sdk::chainhooks::bitcoin::BitcoinChainhookOccurrencePayload;
use chainhook_sdk::chainhooks::types::{
    BitcoinChainhookSpecification, ChainhookConfig, ChainhookFullSpecification,
    ChainhookSpecification,
};
use chainhook_sdk::observer::{
    start_event_observer, BitcoinBlockDataCached, DataHandlerEvent, EventObserverConfig,
    HandleBlock, ObserverCommand, ObserverEvent, ObserverSidecar,
};
use chainhook_sdk::types::{
    BitcoinBlockData, BlockIdentifier, Brc20BalanceData, Brc20Operation, Brc20TokenDeployData,
    Brc20TransferData, OrdinalOperation,
};
use chainhook_sdk::utils::{BlockHeights, Context};
use crossbeam_channel::unbounded;
use crossbeam_channel::{select, Sender};
use dashmap::DashMap;
use fxhash::FxHasher;
use http_api::start_observers_http_server;
use rusqlite::Transaction;

use std::collections::{BTreeMap, HashMap};
use std::hash::BuildHasherDefault;
use std::sync::mpsc::channel;
use std::sync::Arc;

pub struct Service {
    pub prometheus: PrometheusMonitoring,
    pub config: Config,
    pub ctx: Context,
}

impl Service {
    pub fn new(config: Config, ctx: Context) -> Self {
        Self {
            prometheus: PrometheusMonitoring::new(),
            config,
            ctx,
        }
    }

    pub async fn run(
        &mut self,
        observer_specs: Vec<BitcoinChainhookSpecification>,
        predicate_activity_relayer: Option<
            crossbeam_channel::Sender<BitcoinChainhookOccurrencePayload>,
        >,
        check_blocks_integrity: bool,
        stream_indexing_to_observers: bool,
    ) -> Result<(), String> {
        // Start Prometheus monitoring server.
        if let Some(port) = self.config.network.prometheus_monitoring_port {
            let registry_moved = self.prometheus.registry.clone();
            let ctx_cloned = self.ctx.clone();
            let _ = std::thread::spawn(move || {
                let _ = hiro_system_kit::nestable_block_on(start_serving_prometheus_metrics(
                    port,
                    registry_moved,
                    ctx_cloned,
                ));
            });
        }
        let ordhook_db = open_ordinals_db(&self.config.expected_cache_path(), &self.ctx)
            .expect("unable to retrieve ordhook db");
        self.prometheus.initialize(
            0,
            get_latest_indexed_inscription_number(&ordhook_db, &self.ctx).unwrap_or(0),
            find_latest_inscription_block_height(&ordhook_db, &self.ctx)?.unwrap_or(0),
        );

        // Catch-up with chain tip.
        let mut event_observer_config = self.config.get_event_observer_config();
        let block_post_processor = if stream_indexing_to_observers && !observer_specs.is_empty() {
            let mut chainhook_config: ChainhookConfig = ChainhookConfig::new();
            let specs = observer_specs.clone();
            for mut observer_spec in specs.into_iter() {
                observer_spec.enabled = true;
                let spec = ChainhookSpecification::Bitcoin(observer_spec);
                chainhook_config.register_specification(spec)?;
            }
            event_observer_config.chainhook_config = Some(chainhook_config);
            let block_tx = start_observer_forwarding(&event_observer_config, &self.ctx);
            Some(block_tx)
        } else {
            None
        };
        if check_blocks_integrity {
            self.check_blocks_db_integrity().await?;
        }
        self.catch_up_to_bitcoin_chain_tip(block_post_processor)
            .await?;
        try_info!(
            self.ctx,
            "Database up to date, service will start streaming blocks"
        );

        // Sidecar channels setup
        let observer_sidecar = self.set_up_observer_sidecar_runloop()?;

        // Create the chainhook runloop tx/rx comms
        let (observer_command_tx, observer_command_rx) = channel();
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();
        let inner_ctx = if self.config.logs.chainhook_internals {
            self.ctx.clone()
        } else {
            Context::empty()
        };

        // Observers handling
        // 1) update event_observer_config with observers ready to be used
        // 2) catch-up outdated observers by dispatching replays
        let (chainhook_config, outdated_observers) =
            create_and_consolidate_chainhook_config_with_predicates(
                observer_specs,
                find_latest_inscription_block_height(&ordhook_db, &self.ctx)?.unwrap(),
                predicate_activity_relayer.is_some(),
                &self.prometheus,
                &self.config,
                &self.ctx,
            )?;
        // Dispatch required replays
        for outdated_observer_spec in outdated_observers.into_iter() {
            let _ = observer_command_tx.send(ObserverCommand::RegisterPredicate(
                ChainhookFullSpecification::Bitcoin(outdated_observer_spec),
            ));
        }
        event_observer_config.chainhook_config = Some(chainhook_config);

        let _ = start_event_observer(
            event_observer_config,
            observer_command_tx.clone(),
            observer_command_rx,
            Some(observer_event_tx),
            Some(observer_sidecar),
            None,
            inner_ctx,
        );

        // If HTTP Predicates API is on, we start:
        // - Thread pool in charge of performing replays
        // - API server
        self.start_main_runloop_with_dynamic_predicates(
            &observer_command_tx,
            observer_event_rx,
            predicate_activity_relayer,
        )?;
        Ok(())
    }

    // TODO: Deprecated? Only used by ordhook-sdk-js.
    pub async fn start_event_observer(
        &mut self,
        observer_sidecar: ObserverSidecar,
    ) -> Result<
        (
            std::sync::mpsc::Sender<ObserverCommand>,
            crossbeam_channel::Receiver<ObserverEvent>,
        ),
        String,
    > {
        let mut event_observer_config = self.config.get_event_observer_config();
        let (chainhook_config, _) = create_and_consolidate_chainhook_config_with_predicates(
            vec![],
            0,
            true,
            &self.prometheus,
            &self.config,
            &self.ctx,
        )?;

        event_observer_config.chainhook_config = Some(chainhook_config);

        // Create the chainhook runloop tx/rx comms
        let (observer_command_tx, observer_command_rx) = channel();
        let (observer_event_tx, observer_event_rx) = crossbeam_channel::unbounded();

        let inner_ctx = if self.config.logs.chainhook_internals {
            self.ctx.clone()
        } else {
            Context::empty()
        };

        let _ = start_event_observer(
            event_observer_config.clone(),
            observer_command_tx.clone(),
            observer_command_rx,
            Some(observer_event_tx),
            Some(observer_sidecar),
            None,
            inner_ctx,
        );

        Ok((observer_command_tx, observer_event_rx))
    }

    // TODO: Deprecated? Only used by ordhook-sdk-js.
    pub fn start_main_runloop(
        &self,
        _observer_command_tx: &std::sync::mpsc::Sender<ObserverCommand>,
        observer_event_rx: crossbeam_channel::Receiver<ObserverEvent>,
        predicate_activity_relayer: Option<
            crossbeam_channel::Sender<BitcoinChainhookOccurrencePayload>,
        >,
    ) -> Result<(), String> {
        loop {
            let event = match observer_event_rx.recv() {
                Ok(cmd) => cmd,
                Err(e) => {
                    error!(
                        self.ctx.expect_logger(),
                        "Error: broken channel {}",
                        e.to_string()
                    );
                    break;
                }
            };
            match event {
                ObserverEvent::BitcoinPredicateTriggered(data) => {
                    if let Some(ref tx) = predicate_activity_relayer {
                        let _ = tx.send(data);
                    }
                }
                ObserverEvent::Terminate => {
                    info!(self.ctx.expect_logger(), "Terminating runloop");
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Starts the predicates HTTP server and the main Bitcoin processing runloop that will wait for ZMQ messages to arrive in
    /// order to index blocks. This function will block the main thread indefinitely.
    pub fn start_main_runloop_with_dynamic_predicates(
        &self,
        observer_command_tx: &std::sync::mpsc::Sender<ObserverCommand>,
        observer_event_rx: crossbeam_channel::Receiver<ObserverEvent>,
        _predicate_activity_relayer: Option<
            crossbeam_channel::Sender<BitcoinChainhookOccurrencePayload>,
        >,
    ) -> Result<(), String> {
        let (bitcoin_scan_op_tx, bitcoin_scan_op_rx) = crossbeam_channel::unbounded();
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let observer_command_tx_moved = observer_command_tx.clone();
        let _ = hiro_system_kit::thread_named("Bitcoin scan runloop")
            .spawn(move || {
                start_bitcoin_scan_runloop(
                    &config,
                    bitcoin_scan_op_rx,
                    observer_command_tx_moved,
                    &ctx,
                );
            })
            .expect("unable to spawn thread");

        if let PredicatesApi::On(_) = self.config.http_api {
            let moved_config = self.config.clone();
            let moved_ctx = self.ctx.clone();
            let moved_observer_commands_tx = observer_command_tx.clone();
            let moved_observer_event_rx = observer_event_rx.clone();
            let moved_prometheus = self.prometheus.clone();
            let _ = hiro_system_kit::thread_named("HTTP Observers API").spawn(move || {
                let _ = hiro_system_kit::nestable_block_on(start_observers_http_server(
                    &moved_config,
                    &moved_observer_commands_tx,
                    moved_observer_event_rx,
                    bitcoin_scan_op_tx,
                    &moved_prometheus,
                    &moved_ctx,
                ));
            });
        }

        // Block the main thread indefinitely until the chainhook-sdk channel is closed.
        loop {
            let event = match observer_event_rx.recv() {
                Ok(cmd) => cmd,
                Err(e) => {
                    try_error!(self.ctx, "Error: broken channel {}", e.to_string());
                    break;
                }
            };
            match event {
                ObserverEvent::Terminate => {
                    try_info!(&self.ctx, "Terminating runloop");
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    // TODO: Deprecated? Only used by ordhook-sdk-js.
    pub fn set_up_observer_config(
        &self,
        predicates: Vec<BitcoinChainhookSpecification>,
        enable_internal_trigger: bool,
    ) -> Result<
        (
            EventObserverConfig,
            Option<crossbeam_channel::Receiver<DataHandlerEvent>>,
        ),
        String,
    > {
        let mut event_observer_config = self.config.get_event_observer_config();
        let (chainhook_config, _) = create_and_consolidate_chainhook_config_with_predicates(
            predicates,
            0,
            enable_internal_trigger,
            &self.prometheus,
            &self.config,
            &self.ctx,
        )?;
        event_observer_config.chainhook_config = Some(chainhook_config);
        let data_rx = if enable_internal_trigger {
            let (tx, rx) = crossbeam_channel::bounded(256);
            event_observer_config.data_handler_tx = Some(tx);
            Some(rx)
        } else {
            None
        };
        Ok((event_observer_config, data_rx))
    }

    pub fn set_up_observer_sidecar_runloop(&self) -> Result<ObserverSidecar, String> {
        let (block_mutator_in_tx, block_mutator_in_rx) = crossbeam_channel::unbounded();
        let (block_mutator_out_tx, block_mutator_out_rx) = crossbeam_channel::unbounded();
        let (chain_event_notifier_tx, chain_event_notifier_rx) = crossbeam_channel::unbounded();
        let observer_sidecar = ObserverSidecar {
            bitcoin_blocks_mutator: Some((block_mutator_in_tx, block_mutator_out_rx)),
            bitcoin_chain_event_notifier: Some(chain_event_notifier_tx),
        };
        let cache_l2 = Arc::new(new_traversals_lazy_cache(100_000));
        let mut brc20_cache = brc20_new_cache(&self.config);
        let ctx = self.ctx.clone();
        let config = self.config.clone();
        let prometheus = self.prometheus.clone();

        let _ = hiro_system_kit::thread_named("Observer Sidecar Runloop").spawn(move || loop {
            select! {
                recv(block_mutator_in_rx) -> msg => {
                    if let Ok((mut blocks_to_mutate, blocks_ids_to_rollback)) = msg {
                        chainhook_sidecar_mutate_blocks(
                            &mut blocks_to_mutate,
                            &blocks_ids_to_rollback,
                            &cache_l2,
                            &mut brc20_cache,
                            &prometheus,
                            &config,
                            &ctx,
                        );
                        let _ = block_mutator_out_tx.send(blocks_to_mutate);
                    }
                }
                recv(chain_event_notifier_rx) -> msg => {
                    if let Ok(command) = msg {
                        chainhook_sidecar_mutate_ordhook_db(command, &config, &ctx)
                    }
                }
            }
        });

        Ok(observer_sidecar)
    }

    pub async fn check_blocks_db_integrity(&mut self) -> Result<(), String> {
        let (tip, missing_blocks) = {
            let blocks_db = open_blocks_db_with_retry(false, &self.config, &self.ctx);

            let ordhook_db = open_ordinals_db(&self.config.expected_cache_path(), &self.ctx)
                .expect("unable to retrieve ordhook db");
            let tip = find_latest_inscription_block_height(&ordhook_db, &self.ctx)?.unwrap() as u32;
            info!(
                self.ctx.expect_logger(),
                "Checking database integrity up to block #{tip}",
            );
            let missing_blocks = find_missing_blocks(&blocks_db, 0, tip, &self.ctx);
            (tip, missing_blocks)
        };
        if !missing_blocks.is_empty() {
            info!(
                self.ctx.expect_logger(),
                "{} missing blocks detected, will attempt to repair data",
                missing_blocks.len()
            );
            let block_ingestion_processor =
                start_block_archiving_processor(&self.config, &self.ctx, false, None);
            bitcoind_download_blocks(
                &self.config,
                missing_blocks.into_iter().map(|x| x as u64).collect(),
                tip.into(),
                &block_ingestion_processor,
                10_000,
                &self.ctx,
            )
            .await?;
        }
        let blocks_db_rw = open_blocks_db_with_retry(false, &self.config, &self.ctx);
        info!(self.ctx.expect_logger(), "Running database compaction",);
        run_compaction(&blocks_db_rw, tip);
        Ok(())
    }

    /// Synchronizes and indexes all databases until their block height matches bitcoind's block height.
    pub async fn catch_up_to_bitcoin_chain_tip(
        &self,
        block_post_processor: Option<crossbeam_channel::Sender<BitcoinBlockData>>,
    ) -> Result<(), String> {
        // 1: Catch up blocks DB so it is at the same height as the ordinals DB.
        if let Some((start_block, end_block)) = should_sync_rocks_db(&self.config, &self.ctx)? {
            let blocks_post_processor = start_block_archiving_processor(
                &self.config,
                &self.ctx,
                true,
                block_post_processor.clone(),
            );
            try_info!(
                self.ctx,
                "Blocks DB: Compressing blocks from #{start_block} to #{end_block}"
            );
            let blocks = BlockHeights::BlockRange(start_block, end_block)
                .get_sorted_entries()
                .map_err(|_e| format!("Block start / end block spec invalid"))?;
            bitcoind_download_blocks(
                &self.config,
                blocks.into(),
                first_inscription_height(&self.config),
                &blocks_post_processor,
                10_000,
                &self.ctx,
            )
            .await?;
        }

        // 2: Catch up ordinals DB until it reaches bitcoind block height. This will also advance blocks DB.
        let mut last_block_processed = 0;
        while let Some((start_block, end_block, speed)) =
            should_sync_ordhook_db(&self.config, &self.ctx)?
        {
            if last_block_processed == end_block {
                break;
            }
            let blocks_post_processor = start_inscription_indexing_processor(
                &self.config,
                &self.ctx,
                block_post_processor.clone(),
                &self.prometheus,
            );

            try_info!(
                self.ctx,
                "Indexing inscriptions from block #{start_block} to block #{end_block}"
            );

            let blocks = BlockHeights::BlockRange(start_block, end_block)
                .get_sorted_entries()
                .map_err(|_e| format!("Block start / end block spec invalid"))?;
            bitcoind_download_blocks(
                &self.config,
                blocks.into(),
                first_inscription_height(&self.config),
                &blocks_post_processor,
                speed,
                &self.ctx,
            )
            .await?;

            last_block_processed = end_block;
        }

        Ok(())
    }

    pub async fn replay_transfers(
        &self,
        blocks: Vec<u64>,
        block_post_processor: Option<crossbeam_channel::Sender<BitcoinBlockData>>,
    ) -> Result<(), String> {
        // Start predicate processor
        let blocks_post_processor =
            start_transfers_recomputing_processor(&self.config, &self.ctx, block_post_processor);

        bitcoind_download_blocks(
            &self.config,
            blocks,
            first_inscription_height(&self.config),
            &blocks_post_processor,
            100,
            &self.ctx,
        )
        .await?;

        Ok(())
    }
}

fn chainhook_sidecar_mutate_ordhook_db(command: HandleBlock, config: &Config, ctx: &Context) {
    let (blocks_db_rw, sqlite_dbs_rw) = match open_all_dbs_rw(&config, &ctx) {
        Ok(dbs) => dbs,
        Err(e) => {
            try_error!(ctx, "Unable to open readwrite connection: {e}");
            return;
        }
    };

    match command {
        HandleBlock::UndoBlock(block) => {
            try_info!(
                ctx,
                "Re-org handling: reverting changes in block #{}",
                block.block_identifier.index
            );
            let res = drop_block_data_from_all_dbs(
                block.block_identifier.index,
                block.block_identifier.index,
                &blocks_db_rw,
                &sqlite_dbs_rw,
                ctx,
            );
            if let Err(e) = res {
                try_error!(
                    ctx,
                    "Unable to rollback bitcoin block {}: {e}",
                    block.block_identifier
                );
            }
        }
        HandleBlock::ApplyBlock(block) => {
            let block_bytes = match BlockBytesCursor::from_standardized_block(&block) {
                Ok(block_bytes) => block_bytes,
                Err(e) => {
                    try_error!(
                        ctx,
                        "Unable to compress block #{}: #{}",
                        block.block_identifier.index,
                        e.to_string()
                    );
                    return;
                }
            };
            insert_entry_in_blocks(
                block.block_identifier.index as u32,
                &block_bytes,
                true,
                &blocks_db_rw,
                &ctx,
            );
            if let Err(e) = blocks_db_rw.flush() {
                try_error!(ctx, "{}", e.to_string());
            }

            update_ordinals_db_with_block(&block, &sqlite_dbs_rw.ordinals, ctx);
            update_sequence_metadata_with_block(&block, &sqlite_dbs_rw.ordinals, &ctx);

            if let Some(brc20_conn_rw) = &sqlite_dbs_rw.brc20 {
                write_augmented_block_to_brc20_db(&block, brc20_conn_rw, ctx);
            }
        }
    }
}

pub fn start_observer_forwarding(
    event_observer_config: &EventObserverConfig,
    ctx: &Context,
) -> Sender<BitcoinBlockData> {
    let (tx_replayer, rx_replayer) = unbounded();
    let mut moved_event_observer_config = event_observer_config.clone();
    let moved_ctx = ctx.clone();

    let _ = hiro_system_kit::thread_named("Initial predicate processing")
        .spawn(move || {
            if let Some(mut chainhook_config) = moved_event_observer_config.chainhook_config.take()
            {
                let mut bitcoin_predicates_ref: Vec<&BitcoinChainhookSpecification> = vec![];
                for bitcoin_predicate in chainhook_config.bitcoin_chainhooks.iter_mut() {
                    bitcoin_predicates_ref.push(bitcoin_predicate);
                }
                while let Ok(block) = rx_replayer.recv() {
                    let future = process_block_with_predicates(
                        block,
                        &bitcoin_predicates_ref,
                        &moved_event_observer_config,
                        &moved_ctx,
                    );
                    let res = hiro_system_kit::nestable_block_on(future);
                    if let Err(_) = res {
                        error!(moved_ctx.expect_logger(), "Initial ingestion failing");
                    }
                }
            }
        })
        .expect("unable to spawn thread");

    tx_replayer
}

pub fn chainhook_sidecar_mutate_blocks(
    blocks_to_mutate: &mut Vec<BitcoinBlockDataCached>,
    blocks_ids_to_rollback: &Vec<BlockIdentifier>,
    cache_l2: &Arc<DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>>>,
    brc20_cache: &mut Option<Brc20MemoryCache>,
    prometheus: &PrometheusMonitoring,
    config: &Config,
    ctx: &Context,
) {
    let mut updated_blocks_ids = vec![];

    let (blocks_db_rw, mut sqlite_dbs_rw) = match open_all_dbs_rw(&config, &ctx) {
        Ok(dbs) => dbs,
        Err(e) => {
            try_error!(ctx, "Unable to open readwrite connection: {e}");
            return;
        }
    };

    for block_id_to_rollback in blocks_ids_to_rollback.iter() {
        if let Err(e) = drop_block_data_from_all_dbs(
            block_id_to_rollback.index,
            block_id_to_rollback.index,
            &blocks_db_rw,
            &sqlite_dbs_rw,
            &ctx,
        ) {
            try_error!(
                ctx,
                "Unable to rollback bitcoin block {}: {e}",
                block_id_to_rollback.index
            );
        }
    }

    let brc20_db_tx = sqlite_dbs_rw
        .brc20
        .as_mut()
        .map(|c| c.transaction().unwrap());
    let inscriptions_db_tx = sqlite_dbs_rw.ordinals.transaction().unwrap();

    for cache in blocks_to_mutate.iter_mut() {
        let block_bytes = match BlockBytesCursor::from_standardized_block(&cache.block) {
            Ok(block_bytes) => block_bytes,
            Err(e) => {
                try_error!(
                    ctx,
                    "Unable to compress block #{}: #{}",
                    cache.block.block_identifier.index,
                    e.to_string()
                );
                continue;
            }
        };

        insert_entry_in_blocks(
            cache.block.block_identifier.index as u32,
            &block_bytes,
            true,
            &blocks_db_rw,
            &ctx,
        );
        if let Err(e) = blocks_db_rw.flush() {
            try_error!(ctx, "{}", e.to_string());
        }

        if cache.processed_by_sidecar {
            update_ordinals_db_with_block(&cache.block, &inscriptions_db_tx, &ctx);
            update_sequence_metadata_with_block(&cache.block, &inscriptions_db_tx, &ctx);
        } else {
            updated_blocks_ids.push(format!("{}", cache.block.block_identifier.index));

            let mut cache_l1 = BTreeMap::new();
            let mut sequence_cursor = SequenceCursor::new(&inscriptions_db_tx);

            let _ = process_block(
                &mut cache.block,
                &vec![],
                &mut sequence_cursor,
                &mut cache_l1,
                &cache_l2,
                &inscriptions_db_tx,
                brc20_db_tx.as_ref(),
                brc20_cache.as_mut(),
                prometheus,
                &config,
                &ctx,
            );

            let inscription_numbers = get_inscriptions_revealed_in_block(&cache.block)
                .iter()
                .map(|d| d.get_inscription_number().to_string())
                .collect::<Vec<String>>();

            let inscriptions_transferred =
                get_inscriptions_transferred_in_block(&cache.block).len();

            try_info!(
                ctx,
                "Block #{} processed, mutated and revealed {} inscriptions [{}] and {inscriptions_transferred} transfers",
                cache.block.block_identifier.index,
                inscription_numbers.len(),
                inscription_numbers.join(", ")
            );
            cache.processed_by_sidecar = true;
        }
    }
    let _ = inscriptions_db_tx.rollback();

    if let Some(tx) = brc20_db_tx {
        let _ = tx.rollback();
    }
}

/// Writes BRC-20 data already included in the augmented `BitcoinBlockData` onto the BRC-20 database. Only called if BRC-20 is
/// enabled.
pub fn write_brc20_block_operations(
    block: &mut BitcoinBlockData,
    brc20_operation_map: &mut HashMap<String, ParsedBrc20Operation>,
    brc20_cache: &mut Brc20MemoryCache,
    db_tx: &Transaction,
    ctx: &Context,
) {
    if block.block_identifier.index < brc20_activation_height(&block.metadata.network) {
        return;
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
                            &db_tx,
                            &ctx,
                        ) {
                            Ok(op) => match op {
                                VerifiedBrc20Operation::TokenDeploy(token) => {
                                    let dec = token.dec as usize;
                                    tx.metadata.brc20_operation =
                                        Some(Brc20Operation::Deploy(Brc20TokenDeployData {
                                            tick: token.tick.clone(),
                                            max: format!(
                                                "{:.precision$}",
                                                token.max,
                                                precision = dec
                                            ),
                                            lim: format!(
                                                "{:.precision$}",
                                                token.lim,
                                                precision = dec
                                            ),
                                            dec: token.dec.to_string(),
                                            address: token.address.clone(),
                                            inscription_id: reveal.inscription_id.clone(),
                                            self_mint: token.self_mint,
                                        }));
                                    brc20_cache.insert_token_deploy(
                                        &token,
                                        reveal,
                                        &block.block_identifier,
                                        tx_index as u64,
                                        db_tx,
                                        ctx,
                                    );
                                    try_info!(
                                        ctx,
                                        "BRC-20 deploy {} ({}) at block {}",
                                        token.tick,
                                        token.address,
                                        block.block_identifier.index
                                    );
                                }
                                VerifiedBrc20Operation::TokenMint(balance) => {
                                    let Some(token) =
                                        brc20_cache.get_token(&balance.tick, db_tx, ctx)
                                    else {
                                        unreachable!();
                                    };
                                    tx.metadata.brc20_operation =
                                        Some(Brc20Operation::Mint(Brc20BalanceData {
                                            tick: balance.tick.clone(),
                                            amt: format!(
                                                "{:.precision$}",
                                                balance.amt,
                                                precision = token.dec as usize
                                            ),
                                            address: balance.address.clone(),
                                            inscription_id: reveal.inscription_id.clone(),
                                        }));
                                    brc20_cache.insert_token_mint(
                                        &balance,
                                        reveal,
                                        &block.block_identifier,
                                        tx_index as u64,
                                        db_tx,
                                        ctx,
                                    );
                                    try_info!(
                                        ctx,
                                        "BRC-20 mint {} {} ({}) at block {}",
                                        balance.tick,
                                        balance.amt,
                                        balance.address,
                                        block.block_identifier.index
                                    );
                                }
                                VerifiedBrc20Operation::TokenTransfer(balance) => {
                                    let Some(token) =
                                        brc20_cache.get_token(&balance.tick, db_tx, ctx)
                                    else {
                                        unreachable!();
                                    };
                                    tx.metadata.brc20_operation =
                                        Some(Brc20Operation::Transfer(Brc20BalanceData {
                                            tick: balance.tick.clone(),
                                            amt: format!(
                                                "{:.precision$}",
                                                balance.amt,
                                                precision = token.dec as usize
                                            ),
                                            address: balance.address.clone(),
                                            inscription_id: reveal.inscription_id.clone(),
                                        }));
                                    brc20_cache.insert_token_transfer(
                                        &balance,
                                        reveal,
                                        &block.block_identifier,
                                        tx_index as u64,
                                        db_tx,
                                        ctx,
                                    );
                                    try_info!(
                                        ctx,
                                        "BRC-20 transfer {} {} ({}) at block {}",
                                        balance.tick,
                                        balance.amt,
                                        balance.address,
                                        block.block_identifier.index
                                    );
                                }
                                VerifiedBrc20Operation::TokenTransferSend(_) => {
                                    unreachable!("BRC-20 token transfer send should never be generated on reveal")
                                }
                            },
                            Err(e) => {
                                try_debug!(ctx, "Error validating BRC-20 operation {}", e);
                            }
                        }
                    } else {
                        brc20_cache.ignore_inscription(reveal.ordinal_number);
                    }
                }
                OrdinalOperation::InscriptionTransferred(transfer) => {
                    match verify_brc20_transfer(transfer, brc20_cache, &db_tx, &ctx) {
                        Ok(data) => {
                            let Some(token) = brc20_cache.get_token(&data.tick, db_tx, ctx) else {
                                unreachable!();
                            };
                            let Some(unsent_transfer) = brc20_cache.get_unsent_token_transfer(
                                transfer.ordinal_number,
                                db_tx,
                                ctx,
                            ) else {
                                unreachable!();
                            };
                            tx.metadata.brc20_operation =
                                Some(Brc20Operation::TransferSend(Brc20TransferData {
                                    tick: data.tick.clone(),
                                    amt: format!(
                                        "{:.precision$}",
                                        data.amt * -1.0,
                                        precision = token.dec as usize
                                    ),
                                    sender_address: data.sender_address.clone(),
                                    receiver_address: data.receiver_address.clone(),
                                    inscription_id: unsent_transfer.inscription_id,
                                }));
                            brc20_cache.insert_token_transfer_send(
                                &data,
                                &transfer,
                                &block.block_identifier,
                                tx_index as u64,
                                db_tx,
                                ctx,
                            );
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
                        Err(e) => {
                            try_debug!(ctx, "Error validating BRC-20 transfer {}", e);
                        }
                    }
                }
            }
        }
    }
    brc20_cache.db_cache.flush(db_tx, ctx);
}
