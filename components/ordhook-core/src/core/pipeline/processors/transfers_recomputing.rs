use std::{
    thread::{sleep, JoinHandle},
    time::Duration,
};

use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
use crossbeam_channel::{Sender, TryRecvError};

use crate::{
    config::Config,
    core::{
        pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
        protocol::{
            inscription_sequencing::consolidate_block_with_pre_computed_ordinals_data,
            satoshi_tracking::augment_block_with_ordinals_transfer_data,
        },
    },
    db::ordinals::{
        insert_entries_from_block_in_inscriptions, open_ordinals_db_rw,
        remove_entries_from_locations_at_block_height,
    },
    try_info, try_warn,
};

pub fn start_transfers_recomputing_processor(
    config: &Config,
    ctx: &Context,
    post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Inscription indexing runloop")
        .spawn(move || {
            let mut inscriptions_db_conn_rw =
                open_ordinals_db_rw(&config.expected_cache_path(), &ctx).unwrap();
            let mut empty_cycles = 0;

            loop {
                let mut blocks = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(_, blocks)) => {
                        empty_cycles = 0;
                        blocks
                    }
                    Ok(PostProcessorCommand::Terminate) => {
                        let _ = events_tx.send(PostProcessorEvent::Terminated);
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            empty_cycles += 1;
                            if empty_cycles == 10 {
                                try_warn!(ctx, "Block processor reached expiration");
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

                try_info!(ctx, "Processing {} blocks", blocks.len());
                let inscriptions_db_tx = inscriptions_db_conn_rw.transaction().unwrap();

                for block in blocks.iter_mut() {
                    consolidate_block_with_pre_computed_ordinals_data(
                        block,
                        &inscriptions_db_tx,
                        false,
                        None,
                        &ctx,
                    );

                    remove_entries_from_locations_at_block_height(
                        &block.block_identifier.index,
                        &inscriptions_db_tx,
                        &ctx,
                    );

                    insert_entries_from_block_in_inscriptions(block, &inscriptions_db_tx, &ctx);

                    augment_block_with_ordinals_transfer_data(
                        block,
                        &inscriptions_db_tx,
                        true,
                        &ctx,
                    );

                    if let Some(ref post_processor) = post_processor {
                        let _ = post_processor.send(block.clone());
                    }
                }
                let _ = inscriptions_db_tx.commit();
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}

#[cfg(test)]
mod test {
    use std::{thread, time::Duration};

    use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
    use crossbeam_channel::unbounded;

    use crate::{
        config::Config,
        core::{
            pipeline::PostProcessorCommand,
            test_builders::{TestBlockBuilder, TestTransactionBuilder},
        },
        db::{
            blocks::open_blocks_db_with_retry, cursor::BlockBytesCursor, drop_all_dbs,
            initialize_sqlite_dbs,
        },
    };

    use super::start_transfers_recomputing_processor;

    #[test]
    fn transfers_recompute_via_processor() {
        let ctx = Context::empty();
        let config = Config::test_default();
        {
            drop_all_dbs(&config);
            let _ = initialize_sqlite_dbs(&config, &ctx);
            let _ = open_blocks_db_with_retry(true, &config, &ctx);
        }

        let (block_tx, block_rx) = unbounded::<BitcoinBlockData>();
        let controller = start_transfers_recomputing_processor(&config, &ctx, Some(block_tx));

        let c0 = controller.commands_tx.clone();
        thread::spawn(move || {
            let block0 = TestBlockBuilder::new()
                .hash(
                    "0x00000000000000000001b228f9faca9e7d11fcecff9d463bd05546ff0aa4651a"
                        .to_string(),
                )
                .height(849999)
                .add_transaction(
                    TestTransactionBuilder::new()
                        .hash(
                            "0xa321c61c83563a377f82ef59301f2527079f6bda7c2d04f9f5954c873f42e8ac"
                                .to_string(),
                        )
                        .build(),
                )
                .build();
            thread::sleep(Duration::from_millis(50));
            let _ = c0.send(PostProcessorCommand::ProcessBlocks(
                vec![(
                    849999,
                    BlockBytesCursor::from_standardized_block(&block0).unwrap(),
                )],
                vec![block0],
            ));
        });
        let _ = block_rx.recv().unwrap();

        // Close channel.
        let _ = controller.commands_tx.send(PostProcessorCommand::Terminate);
    }
}
