use chainhook_sdk::{types::BitcoinBlockData, utils::Context};
use crossbeam_channel::{Sender, TryRecvError};
use rocksdb::DB;
use std::{
    thread::{sleep, JoinHandle},
    time::Duration,
};

use crate::{
    config::Config,
    core::pipeline::{PostProcessorCommand, PostProcessorController, PostProcessorEvent},
    db::blocks::{insert_entry_in_blocks, open_blocks_db_with_retry},
    try_error, try_info,
};

pub fn start_block_archiving_processor(
    config: &Config,
    ctx: &Context,
    update_tip: bool,
    _post_processor: Option<Sender<BitcoinBlockData>>,
) -> PostProcessorController {
    let (commands_tx, commands_rx) = crossbeam_channel::bounded::<PostProcessorCommand>(2);
    let (events_tx, events_rx) = crossbeam_channel::unbounded::<PostProcessorEvent>();

    let config = config.clone();
    let ctx = ctx.clone();
    let handle: JoinHandle<()> = hiro_system_kit::thread_named("Processor Runloop")
        .spawn(move || {
            let blocks_db_rw = open_blocks_db_with_retry(true, &config, &ctx);
            let mut processed_blocks = 0;

            loop {
                let (compacted_blocks, _) = match commands_rx.try_recv() {
                    Ok(PostProcessorCommand::ProcessBlocks(compacted_blocks, blocks)) => {
                        (compacted_blocks, blocks)
                    }
                    Ok(PostProcessorCommand::Terminate) => {
                        let _ = events_tx.send(PostProcessorEvent::Terminated);
                        break;
                    }
                    Err(e) => match e {
                        TryRecvError::Empty => {
                            sleep(Duration::from_secs(1));
                            continue;
                        }
                        _ => {
                            break;
                        }
                    },
                };
                processed_blocks += compacted_blocks.len();
                store_compacted_blocks(compacted_blocks, update_tip, &blocks_db_rw, &ctx);

                if processed_blocks % 10_000 == 0 {
                    let _ = blocks_db_rw.flush_wal(true);
                }
            }

            if let Err(e) = blocks_db_rw.flush() {
                try_error!(ctx, "{}", e.to_string());
            }
        })
        .expect("unable to spawn thread");

    PostProcessorController {
        commands_tx,
        events_rx,
        thread_handle: handle,
    }
}

pub fn store_compacted_blocks(
    mut compacted_blocks: Vec<(u64, Vec<u8>)>,
    update_tip: bool,
    blocks_db_rw: &DB,
    ctx: &Context,
) {
    compacted_blocks.sort_by(|(a, _), (b, _)| a.cmp(b));

    for (block_height, compacted_block) in compacted_blocks.into_iter() {
        insert_entry_in_blocks(
            block_height as u32,
            &compacted_block,
            update_tip,
            &blocks_db_rw,
            &ctx,
        );
        try_info!(ctx, "Block #{block_height} saved to disk");
    }

    if let Err(e) = blocks_db_rw.flush() {
        try_error!(ctx, "{}", e.to_string());
    }
}

#[cfg(test)]
mod test {
    use std::{thread::sleep, time::Duration};

    use chainhook_sdk::utils::Context;

    use crate::{
        config::Config,
        core::{
            pipeline::PostProcessorCommand,
            test_builders::{TestBlockBuilder, TestTransactionBuilder},
        },
        db::{
            blocks::{find_block_bytes_at_block_height, open_blocks_db_with_retry},
            cursor::BlockBytesCursor,
            drop_all_dbs, initialize_sqlite_dbs,
        },
    };

    use super::start_block_archiving_processor;

    #[test]
    fn archive_blocks_via_processor() {
        let ctx = Context::empty();
        let config = Config::test_default();
        {
            drop_all_dbs(&config);
            let _ = initialize_sqlite_dbs(&config, &ctx);
            let _ = open_blocks_db_with_retry(true, &config, &ctx);
        }
        let controller = start_block_archiving_processor(&config, &ctx, true, None);

        // Store a block and terminate.
        let block0 = TestBlockBuilder::new()
            .hash("0x00000000000000000001b228f9faca9e7d11fcecff9d463bd05546ff0aa4651a".to_string())
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
        let _ = controller
            .commands_tx
            .send(PostProcessorCommand::ProcessBlocks(
                vec![(
                    849999,
                    BlockBytesCursor::from_standardized_block(&block0).unwrap(),
                )],
                vec![],
            ));
        sleep(Duration::from_millis(100));
        let _ = controller.commands_tx.send(PostProcessorCommand::Terminate);

        // Check that blocks exist in rocksdb
        let blocks_db = open_blocks_db_with_retry(false, &config, &ctx);
        let result = find_block_bytes_at_block_height(849999, 3, &blocks_db, &ctx);
        assert!(result.is_some());
    }
}
