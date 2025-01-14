use std::collections::HashSet;

use chainhook_postgres::deadpool_postgres::Transaction;
use chainhook_sdk::{
    bitcoincore_rpc_json::bitcoin::{Address, Network, ScriptBuf},
    types::{
        BitcoinBlockData, BitcoinTransactionData, BlockIdentifier, OrdinalInscriptionTransferData, OrdinalInscriptionTransferDestination, OrdinalOperation
    },
    utils::Context,
};

use crate::{
    core::{compute_next_satpoint_data, SatPosition},
    db::ordinals_pg,
    ord::height::Height,
    try_info,
    utils::format_outpoint_to_watch,
};

use super::inscription_sequencing::get_bitcoin_network;

#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct WatchedSatpoint {
    pub ordinal_number: u64,
    pub offset: u64,
}

pub fn parse_output_and_offset_from_satpoint(
    satpoint: &String,
) -> Result<(String, Option<u64>), String> {
    let parts: Vec<&str> = satpoint.split(':').collect();
    let tx_id = parts
        .get(0)
        .ok_or("get_output_and_offset_from_satpoint: inscription_id not found")?;
    let output = parts
        .get(1)
        .ok_or("get_output_and_offset_from_satpoint: output not found")?;
    let offset: Option<u64> = match parts.get(2) {
        Some(part) => Some(
            part.parse::<u64>()
                .map_err(|e| format!("parse_output_and_offset_from_satpoint: {e}"))?,
        ),
        None => None,
    };
    Ok((format!("{}:{}", tx_id, output), offset))
}

pub async fn augment_block_with_transfers(
    block: &mut BitcoinBlockData,
    db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<(), String> {
    let network = get_bitcoin_network(&block.metadata.network);
    let coinbase_subsidy = Height(block.block_identifier.index).subsidy();
    let coinbase_tx = &block.transactions[0].clone();
    let mut cumulated_fees = 0;
    for (tx_index, tx) in block.transactions.iter_mut().enumerate() {
        let _ = augment_transaction_with_ordinal_transfers(
            tx,
            tx_index,
            &block.block_identifier,
            &network,
            &coinbase_tx,
            coinbase_subsidy,
            &mut cumulated_fees,
            db_tx,
            ctx,
        )
        .await?;
    }
    Ok(())
}

pub fn compute_satpoint_post_transfer(
    tx: &BitcoinTransactionData,
    input_index: usize,
    relative_pointer_value: u64,
    network: &Network,
    coinbase_tx: &BitcoinTransactionData,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    ctx: &Context,
) -> (OrdinalInscriptionTransferDestination, String, Option<u64>) {
    let inputs: Vec<u64> = tx
        .metadata
        .inputs
        .iter()
        .map(|o| o.previous_output.value)
        .collect::<_>();
    let outputs = tx.metadata.outputs.iter().map(|o| o.value).collect::<_>();
    let post_transfer_data = compute_next_satpoint_data(
        input_index,
        &inputs,
        &outputs,
        relative_pointer_value,
        Some(ctx),
    );

    let (outpoint_post_transfer, offset_post_transfer, destination, post_transfer_output_value) =
        match post_transfer_data {
            SatPosition::Output((output_index, offset)) => {
                let outpoint = format_outpoint_to_watch(&tx.transaction_identifier, output_index);
                let script_pub_key_hex = tx.metadata.outputs[output_index].get_script_pubkey_hex();
                let updated_address = match ScriptBuf::from_hex(&script_pub_key_hex) {
                    Ok(script) => match Address::from_script(&script, network.clone()) {
                        Ok(address) => {
                            OrdinalInscriptionTransferDestination::Transferred(address.to_string())
                        }
                        Err(e) => {
                            try_info!(
                                ctx,
                                "unable to retrieve address from {script_pub_key_hex}: {}",
                                e.to_string()
                            );
                            OrdinalInscriptionTransferDestination::Burnt(script.to_string())
                        }
                    },
                    Err(e) => {
                        try_info!(
                            ctx,
                            "unable to retrieve address from {script_pub_key_hex}: {}",
                            e.to_string()
                        );
                        OrdinalInscriptionTransferDestination::Burnt(script_pub_key_hex.to_string())
                    }
                };

                (
                    outpoint,
                    offset,
                    updated_address,
                    Some(tx.metadata.outputs[output_index].value),
                )
            }
            SatPosition::Fee(offset) => {
                // Get Coinbase TX
                let total_offset = coinbase_subsidy + *cumulated_fees + offset;
                let outputs = coinbase_tx
                    .metadata
                    .outputs
                    .iter()
                    .map(|o| o.value)
                    .collect();
                let post_transfer_data = compute_next_satpoint_data(
                    0,
                    &vec![total_offset],
                    &outputs,
                    total_offset,
                    Some(ctx),
                );

                // Identify the correct output
                let (output_index, offset) = match post_transfer_data {
                    SatPosition::Output(pos) => pos,
                    _ => {
                        try_info!(ctx, "unable to locate satoshi in coinbase outputs");
                        (0, total_offset)
                    }
                };

                let outpoint =
                    format_outpoint_to_watch(&coinbase_tx.transaction_identifier, output_index);
                (
                    outpoint,
                    offset,
                    OrdinalInscriptionTransferDestination::SpentInFees,
                    None,
                )
            }
        };
    let satpoint_post_transfer = format!("{}:{}", outpoint_post_transfer, offset_post_transfer);

    (
        destination,
        satpoint_post_transfer,
        post_transfer_output_value,
    )
}

pub async fn augment_transaction_with_ordinal_transfers(
    tx: &mut BitcoinTransactionData,
    tx_index: usize,
    block_identifier: &BlockIdentifier,
    network: &Network,
    coinbase_tx: &BitcoinTransactionData,
    coinbase_subsidy: u64,
    cumulated_fees: &mut u64,
    db_tx: &Transaction<'_>,
    ctx: &Context,
) -> Result<Vec<OrdinalInscriptionTransferData>, String> {
    let mut transfers = vec![];

    // The transfers are inserted in storage after the inscriptions.
    // We have a unicity constraing, and can only have 1 ordinals per satpoint.
    let mut updated_sats = HashSet::new();
    for op in tx.metadata.ordinal_operations.iter() {
        if let OrdinalOperation::InscriptionRevealed(data) = op {
            updated_sats.insert(data.ordinal_number);
        }
    }

    // For each satpoint inscribed retrieved, we need to compute the next outpoint to watch
    let input_entries =
        ordinals_pg::get_inscribed_satpoints_at_tx_inputs(&tx.metadata.inputs, db_tx).await?;
    for (input_index, input) in tx.metadata.inputs.iter().enumerate() {
        let Some(entries) = input_entries.get(&input_index) else {
            continue;
        };
        for watched_satpoint in entries.into_iter() {
            if updated_sats.contains(&watched_satpoint.ordinal_number) {
                continue;
            }
            let satpoint_pre_transfer = format!(
                "{}:{}",
                format_outpoint_to_watch(
                    &input.previous_output.txid,
                    input.previous_output.vout as usize,
                ),
                watched_satpoint.offset
            );

            let (destination, satpoint_post_transfer, post_transfer_output_value) =
                compute_satpoint_post_transfer(
                    &&*tx,
                    input_index,
                    watched_satpoint.offset,
                    network,
                    coinbase_tx,
                    coinbase_subsidy,
                    cumulated_fees,
                    ctx,
                );

            let transfer_data = OrdinalInscriptionTransferData {
                ordinal_number: watched_satpoint.ordinal_number,
                destination,
                tx_index,
                satpoint_pre_transfer: satpoint_pre_transfer.clone(),
                satpoint_post_transfer: satpoint_post_transfer.clone(),
                post_transfer_output_value,
            };

            try_info!(
                ctx,
                "Inscription transfer detected on Satoshi {} ({} -> {}) at block #{}",
                transfer_data.ordinal_number,
                satpoint_pre_transfer,
                satpoint_post_transfer,
                block_identifier.index
            );
            transfers.push(transfer_data.clone());
            tx.metadata
                .ordinal_operations
                .push(OrdinalOperation::InscriptionTransferred(transfer_data));
        }
    }
    *cumulated_fees += tx.metadata.fee;

    Ok(transfers)
}

#[cfg(test)]
mod test {
    use chainhook_sdk::{
        bitcoin::Network, types::OrdinalInscriptionTransferDestination, utils::Context,
    };

    use crate::core::test_builders::{TestTransactionBuilder, TestTxInBuilder, TestTxOutBuilder};

    use super::compute_satpoint_post_transfer;

    #[test]
    fn computes_satpoint_spent_as_fee() {
        let ctx = Context::empty();
        let tx = &TestTransactionBuilder::new()
            .add_input(TestTxInBuilder::new().value(10_000).build())
            .add_output(TestTxOutBuilder::new().value(2_000).build())
            .build();
        let coinbase_tx = &TestTransactionBuilder::new()
            .add_output(TestTxOutBuilder::new().value(312_500_000 + 5_000).build())
            .build();

        let (destination, satpoint, value) = compute_satpoint_post_transfer(
            tx,
            0,
            // This offset will make it go to fees.
            5_000,
            &Network::Bitcoin,
            coinbase_tx,
            312_500_000,
            &mut 0,
            &ctx,
        );

        assert_eq!(
            destination,
            OrdinalInscriptionTransferDestination::SpentInFees
        );
        assert_eq!(
            satpoint,
            "b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735:0:312503000"
                .to_string()
        );
        assert_eq!(value, None);
    }

    #[test]
    fn computes_satpoint_for_op_return() {
        let ctx = Context::empty();
        let tx = &TestTransactionBuilder::new()
            .add_input(TestTxInBuilder::new().value(10_000).build())
            .add_output(
                TestTxOutBuilder::new()
                .value(9_000)
                // OP_RETURN
                .script_pubkey("0x6a24aa21a9edd3ce297baa3ee8fd96ecd7613f2743552e2f91ed4864540cf059835ff5b35cff".to_string())
                .build()
            )
            .build();
        let coinbase_tx = &TestTransactionBuilder::new()
            .add_output(TestTxOutBuilder::new().value(312_500_000).build())
            .build();

        let (destination, satpoint, value) = compute_satpoint_post_transfer(
            tx,
            0,
            5_000,
            &Network::Bitcoin,
            coinbase_tx,
            312_500_000,
            &mut 0,
            &ctx,
        );

        assert_eq!(
            destination,
            OrdinalInscriptionTransferDestination::Burnt("OP_RETURN OP_PUSHBYTES_36 aa21a9edd3ce297baa3ee8fd96ecd7613f2743552e2f91ed4864540cf059835ff5b35cff".to_string())
        );
        assert_eq!(
            satpoint,
            "b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735:0:5000".to_string()
        );
        assert_eq!(value, Some(9000));
    }
}
