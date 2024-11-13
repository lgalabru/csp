pub mod meta_protocols;
pub mod pipeline;
pub mod protocol;
#[cfg(test)]
pub mod test_builders;

use chainhook_postgres::pg_connect;
use dashmap::DashMap;
use fxhash::{FxBuildHasher, FxHasher};
use std::hash::BuildHasherDefault;
use std::ops::Div;

use chainhook_sdk::{types::BitcoinNetwork, utils::Context};

use crate::{
    config::Config,
    db::{
        blocks::{
            find_last_block_inserted, find_pinned_block_bytes_at_block_height,
            open_blocks_db_with_retry,
        },
        cursor::TransactionBytesCursor,
        ordinals::{find_latest_inscription_block_height, open_ordinals_db},
        ordinals_pg::get_chain_tip_block_height,
    },
    utils::bitcoind::bitcoind_get_block_height,
};

pub fn first_inscription_height(config: &Config) -> u64 {
    match config.network.bitcoin_network {
        BitcoinNetwork::Mainnet => 767430,
        BitcoinNetwork::Regtest => 1,
        BitcoinNetwork::Testnet => 2413343,
        BitcoinNetwork::Signet => 112402,
    }
}

pub fn new_traversals_cache(
) -> DashMap<(u32, [u8; 8]), (Vec<([u8; 8], u32, u16, u64)>, Vec<u64>), BuildHasherDefault<FxHasher>>
{
    let hasher = FxBuildHasher::default();
    DashMap::with_hasher(hasher)
}

pub fn new_traversals_lazy_cache(
    cache_size: usize,
) -> DashMap<(u32, [u8; 8]), TransactionBytesCursor, BuildHasherDefault<FxHasher>> {
    let hasher = FxBuildHasher::default();
    DashMap::with_capacity_and_hasher(
        ((cache_size.saturating_sub(500)) * 1000 * 1000)
            .div(TransactionBytesCursor::get_average_bytes_size()),
        hasher,
    )
}

#[derive(PartialEq, Debug)]
pub enum SatPosition {
    Output((usize, u64)),
    Fee(u64),
}

pub fn resolve_absolute_pointer(inputs: &Vec<u64>, absolute_pointer_value: u64) -> (usize, u64) {
    let mut selected_index = 0;
    let mut cumulated_input_value = 0;
    // Check for overflow
    let total: u64 = inputs.iter().sum();
    if absolute_pointer_value > total {
        return (0, 0);
    }
    // Identify the input + satoshi offset being inscribed
    for (index, input_value) in inputs.iter().enumerate() {
        if (cumulated_input_value + input_value) > absolute_pointer_value {
            selected_index = index;
            break;
        }
        cumulated_input_value += input_value;
    }
    let relative_pointer_value = absolute_pointer_value - cumulated_input_value;
    (selected_index, relative_pointer_value)
}

pub fn compute_next_satpoint_data(
    input_index: usize,
    inputs: &Vec<u64>,
    outputs: &Vec<u64>,
    relative_pointer_value: u64,
    _ctx: Option<&Context>,
) -> SatPosition {
    let mut absolute_offset_in_inputs = 0;
    for (index, input_value) in inputs.iter().enumerate() {
        if index == input_index {
            break;
        }
        absolute_offset_in_inputs += input_value;
    }
    absolute_offset_in_inputs += relative_pointer_value;

    let mut absolute_offset_of_first_satoshi_in_selected_output = 0;
    let mut selected_output_index = 0;
    let mut floating_bound = 0;

    for (index, output_value) in outputs.iter().enumerate() {
        floating_bound += output_value;
        selected_output_index = index;
        if floating_bound > absolute_offset_in_inputs {
            break;
        }
        absolute_offset_of_first_satoshi_in_selected_output += output_value;
    }

    if selected_output_index == (outputs.len() - 1) && absolute_offset_in_inputs >= floating_bound {
        // Satoshi spent in fees
        return SatPosition::Fee(absolute_offset_in_inputs - floating_bound);
    }
    let relative_offset_in_selected_output =
        absolute_offset_in_inputs - absolute_offset_of_first_satoshi_in_selected_output;
    SatPosition::Output((selected_output_index, relative_offset_in_selected_output))
}

pub fn should_sync_rocks_db(config: &Config, ctx: &Context) -> Result<Option<(u64, u64)>, String> {
    let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
    let inscriptions_db_conn = open_ordinals_db(&config.expected_cache_path(), &ctx)?;
    let last_compressed_block = find_last_block_inserted(&blocks_db) as u64;
    let last_indexed_block = match find_latest_inscription_block_height(&inscriptions_db_conn, ctx)?
    {
        Some(last_indexed_block) => last_indexed_block,
        None => 0,
    };

    let res = if last_compressed_block < last_indexed_block {
        Some((last_compressed_block, last_indexed_block))
    } else {
        None
    };
    Ok(res)
}

pub async fn should_sync_ordinals_db(
    config: &Config,
    ctx: &Context,
) -> Result<Option<(u64, u64, usize)>, String> {
    let blocks_db = open_blocks_db_with_retry(true, &config, &ctx);
    let mut start_block = find_last_block_inserted(&blocks_db) as u64;

    let pg_client = pg_connect(&config.ordinals_db.to_conn_config(), ctx).await?;
    match get_chain_tip_block_height(&pg_client).await? {
        Some(height) => {
            if find_pinned_block_bytes_at_block_height(height as u32, 3, &blocks_db, &ctx).is_none()
            {
                start_block = start_block.min(height);
            } else {
                start_block = height;
            }
            start_block += 1;
        }
        None => {
            start_block = start_block.min(first_inscription_height(config));
        }
    };

    // TODO: Gracefully handle Regtest, Testnet and Signet
    let end_block = bitcoind_get_block_height(config, ctx);
    let (mut end_block, speed) = if start_block < 200_000 {
        (end_block.min(200_000), 10_000)
    } else if start_block < 550_000 {
        (end_block.min(550_000), 1_000)
    } else {
        (end_block, 100)
    };

    if start_block < 767430 && end_block > 767430 {
        end_block = 767430;
    }

    if start_block <= end_block {
        Ok(Some((start_block, end_block, speed)))
    } else {
        Ok(None)
    }
}

#[test]
fn test_identify_next_output_index_destination() {
    assert_eq!(
        compute_next_satpoint_data(0, &vec![20, 30, 45], &vec![20, 30, 45], 10, None),
        SatPosition::Output((0, 10))
    );
    assert_eq!(
        compute_next_satpoint_data(0, &vec![20, 30, 45], &vec![20, 30, 45], 20, None),
        SatPosition::Output((1, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(1, &vec![20, 30, 45], &vec![20, 30, 45], 25, None),
        SatPosition::Output((1, 25))
    );
    assert_eq!(
        compute_next_satpoint_data(1, &vec![20, 30, 45], &vec![20, 5, 45], 26, None),
        SatPosition::Output((2, 21))
    );
    assert_eq!(
        compute_next_satpoint_data(1, &vec![10, 10, 10], &vec![30], 20, None),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(0, &vec![10, 10, 10], &vec![30], 30, None),
        SatPosition::Fee(0)
    );
    assert_eq!(
        compute_next_satpoint_data(0, &vec![10, 10, 10], &vec![30], 0, None),
        SatPosition::Output((0, 0))
    );
    assert_eq!(
        compute_next_satpoint_data(2, &vec![20, 30, 45], &vec![20, 30, 45], 95, None),
        SatPosition::Fee(50)
    );
    assert_eq!(
        compute_next_satpoint_data(
            2,
            &vec![1000, 600, 546, 63034],
            &vec![1600, 10000, 15000],
            1600,
            None
        ),
        SatPosition::Output((1, 1600))
    );
    assert_eq!(
        compute_next_satpoint_data(
            3,
            &vec![6100, 148660, 103143, 7600],
            &vec![81434, 173995],
            257903,
            None
        ),
        SatPosition::Fee(260377)
    );
}
