use chainhook_sdk::bitcoincore_rpc_json::bitcoin::Txid;
use chainhook_sdk::indexer::bitcoin::BitcoinTransactionFullBreakdown;
use chainhook_sdk::indexer::bitcoin::{standardize_bitcoin_block, BitcoinBlockFullBreakdown};
use chainhook_sdk::types::{
    BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData, BlockIdentifier,
    OrdinalInscriptionCurseType, OrdinalInscriptionNumber, OrdinalInscriptionRevealData,
    OrdinalInscriptionTransferData, OrdinalOperation,
};
use chainhook_sdk::utils::Context;
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use crate::config::Config;
use crate::core::meta_protocols::brc20::brc20_activation_height;
use crate::core::meta_protocols::brc20::parser::{parse_brc20_operation, ParsedBrc20Operation};
use crate::ord::envelope::{Envelope, ParsedEnvelope, RawEnvelope};
use crate::ord::inscription::Inscription;
use crate::ord::inscription_id::InscriptionId;
use crate::try_warn;
use {chainhook_sdk::bitcoincore_rpc::bitcoin::Witness, std::str};

pub fn parse_inscriptions_from_witness(
    input_index: usize,
    witness_bytes: Vec<Vec<u8>>,
    txid: &str,
) -> Option<Vec<(OrdinalInscriptionRevealData, Inscription)>> {
    let witness = Witness::from_slice(&witness_bytes);
    let tapscript = witness.tapscript()?;
    let envelopes: Vec<Envelope<Inscription>> = RawEnvelope::from_tapscript(tapscript, input_index)
        .ok()?
        .into_iter()
        .map(|e| ParsedEnvelope::from(e))
        .collect();
    let mut inscriptions = vec![];
    for envelope in envelopes.into_iter() {
        let curse_type = if envelope.payload.unrecognized_even_field {
            Some(OrdinalInscriptionCurseType::UnrecognizedEvenField)
        } else if envelope.payload.duplicate_field {
            Some(OrdinalInscriptionCurseType::DuplicateField)
        } else if envelope.payload.incomplete_field {
            Some(OrdinalInscriptionCurseType::IncompleteField)
        } else if envelope.input != 0 {
            Some(OrdinalInscriptionCurseType::NotInFirstInput)
        } else if envelope.offset != 0 {
            Some(OrdinalInscriptionCurseType::NotAtOffsetZero)
        } else if envelope.payload.pointer.is_some() {
            Some(OrdinalInscriptionCurseType::Pointer)
        } else if envelope.pushnum {
            Some(OrdinalInscriptionCurseType::Pushnum)
        } else if envelope.stutter {
            Some(OrdinalInscriptionCurseType::Stutter)
        } else {
            None
        };

        let inscription_id = InscriptionId {
            txid: Txid::from_str(txid).unwrap(),
            index: input_index as u32,
        };

        let no_content_bytes = vec![];
        let inscription_content_bytes = envelope.payload.body().take().unwrap_or(&no_content_bytes);
        let mut content_bytes = "0x".to_string();
        content_bytes.push_str(&hex::encode(&inscription_content_bytes));

        let parent = envelope.payload.parent().and_then(|i| Some(i.to_string()));
        let delegate = envelope
            .payload
            .delegate()
            .and_then(|i| Some(i.to_string()));
        let metaprotocol = envelope
            .payload
            .metaprotocol()
            .and_then(|p| Some(p.to_string()));
        let metadata = envelope.payload.metadata().and_then(|m| Some(json!(m)));

        let reveal_data = OrdinalInscriptionRevealData {
            content_type: envelope
                .payload
                .content_type()
                .unwrap_or("unknown")
                .to_string(),
            content_bytes,
            content_length: inscription_content_bytes.len(),
            inscription_id: inscription_id.to_string(),
            inscription_input_index: input_index,
            tx_index: 0,
            inscription_output_value: 0,
            inscription_pointer: envelope.payload.pointer(),
            inscription_fee: 0,
            inscription_number: OrdinalInscriptionNumber::zero(),
            inscriber_address: None,
            parent,
            delegate,
            metaprotocol,
            metadata,
            ordinal_number: 0,
            ordinal_block_height: 0,
            ordinal_offset: 0,
            transfers_pre_inscription: 0,
            satpoint_post_inscription: format!(""),
            curse_type,
        };
        inscriptions.push((reveal_data, envelope.payload));
    }
    Some(inscriptions)
}

pub fn parse_inscriptions_from_standardized_tx(
    tx: &mut BitcoinTransactionData,
    block_identifier: &BlockIdentifier,
    network: &BitcoinNetwork,
    brc20_operation_map: &mut HashMap<String, ParsedBrc20Operation>,
    config: &Config,
    ctx: &Context,
) -> Vec<OrdinalOperation> {
    let mut operations = vec![];
    for (input_index, input) in tx.metadata.inputs.iter().enumerate() {
        let witness_bytes: Vec<Vec<u8>> = input
            .witness
            .iter()
            .map(|w| hex::decode(&w[2..]).unwrap())
            .collect();

        if let Some(inscriptions) = parse_inscriptions_from_witness(
            input_index,
            witness_bytes,
            tx.transaction_identifier.get_hash_bytes_str(),
        ) {
            for (reveal, inscription) in inscriptions.into_iter() {
                if config.meta_protocols.brc20
                    && block_identifier.index >= brc20_activation_height(&network)
                {
                    match parse_brc20_operation(&inscription) {
                        Ok(Some(op)) => {
                            brc20_operation_map.insert(reveal.inscription_id.clone(), op);
                        }
                        Ok(None) => {}
                        Err(e) => {
                            try_warn!(ctx, "Error parsing BRC-20 operation: {}", e);
                        }
                    };
                }
                operations.push(OrdinalOperation::InscriptionRevealed(reveal));
            }
        }
    }
    operations
}

pub fn parse_inscriptions_in_raw_tx(
    tx: &BitcoinTransactionFullBreakdown,
    _ctx: &Context,
) -> Vec<OrdinalOperation> {
    let mut operations = vec![];
    for (input_index, input) in tx.vin.iter().enumerate() {
        if let Some(ref witness_data) = input.txinwitness {
            let witness_bytes: Vec<Vec<u8>> = witness_data
                .iter()
                .map(|w| hex::decode(w).unwrap())
                .collect();

            if let Some(inscriptions) =
                parse_inscriptions_from_witness(input_index, witness_bytes, &tx.txid)
            {
                for (reveal, _inscription) in inscriptions.into_iter() {
                    operations.push(OrdinalOperation::InscriptionRevealed(reveal));
                }
            }
        }
    }
    operations
}

pub fn parse_inscriptions_and_standardize_block(
    raw_block: BitcoinBlockFullBreakdown,
    network: &BitcoinNetwork,
    ctx: &Context,
) -> Result<BitcoinBlockData, (String, bool)> {
    let mut ordinal_operations = BTreeMap::new();

    for tx in raw_block.tx.iter() {
        ordinal_operations.insert(tx.txid.to_string(), parse_inscriptions_in_raw_tx(&tx, ctx));
    }

    let mut block = standardize_bitcoin_block(raw_block, network, ctx)?;

    for tx in block.transactions.iter_mut() {
        if let Some(ordinal_operations) =
            ordinal_operations.remove(tx.transaction_identifier.get_hash_bytes_str())
        {
            tx.metadata.ordinal_operations = ordinal_operations;
        }
    }
    Ok(block)
}

pub fn parse_inscriptions_in_standardized_block(
    block: &mut BitcoinBlockData,
    brc20_operation_map: &mut HashMap<String, ParsedBrc20Operation>,
    config: &Config,
    ctx: &Context,
) {
    for tx in block.transactions.iter_mut() {
        tx.metadata.ordinal_operations = parse_inscriptions_from_standardized_tx(
            tx,
            &block.block_identifier,
            &block.metadata.network,
            brc20_operation_map,
            config,
            ctx,
        );
    }
}

pub fn get_inscriptions_revealed_in_block(
    block: &BitcoinBlockData,
) -> Vec<&OrdinalInscriptionRevealData> {
    let mut ops = vec![];
    for tx in block.transactions.iter() {
        for op in tx.metadata.ordinal_operations.iter() {
            if let OrdinalOperation::InscriptionRevealed(op) = op {
                ops.push(op);
            }
        }
    }
    ops
}

pub fn get_inscriptions_transferred_in_block(
    block: &BitcoinBlockData,
) -> Vec<&OrdinalInscriptionTransferData> {
    let mut ops = vec![];
    for tx in block.transactions.iter() {
        for op in tx.metadata.ordinal_operations.iter() {
            if let OrdinalOperation::InscriptionTransferred(op) = op {
                ops.push(op);
            }
        }
    }
    ops
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use chainhook_sdk::{
        bitcoin::Amount,
        indexer::bitcoin::{
            BitcoinBlockFullBreakdown, BitcoinTransactionFullBreakdown,
            BitcoinTransactionInputFullBreakdown, BitcoinTransactionInputPrevoutFullBreakdown,
            GetRawTransactionResultVinScriptSig,
        },
        types::{
            BitcoinBlockData, BitcoinNetwork, BitcoinTransactionData,
            OrdinalInscriptionTransferData, OrdinalInscriptionTransferDestination,
            OrdinalOperation,
        },
        utils::Context,
    };

    use test_case::test_case;

    use crate::{
        config::Config,
        utils::test_helpers::{TestBlockBuilder, TestTransactionBuilder, TestTxInBuilder},
    };

    use super::{
        get_inscriptions_revealed_in_block, get_inscriptions_transferred_in_block,
        parse_inscriptions_and_standardize_block, parse_inscriptions_in_standardized_block,
    };

    pub fn new_test_transfer_tx_with_operation() -> BitcoinTransactionData {
        TestTransactionBuilder::new()
            .ordinal_operations(vec![OrdinalOperation::InscriptionTransferred(
                OrdinalInscriptionTransferData {
                    ordinal_number: 300144140535834,
                    destination: OrdinalInscriptionTransferDestination::Transferred(
                        "bc1pcwway0ne322s0lrc5e905f3chuclvnyy3z6wn86azkgmgcprf3tqvyy7ws"
                            .to_string(),
                    ),
                    satpoint_pre_transfer:
                        "ab2683db34e335c89a5c1d634e6c5bd8d8bca8ded281be84f71f921c9e8783b2:0:0"
                            .to_string(),
                    satpoint_post_transfer:
                        "42fa098abab8d5cca1c303a97bd0404cf8e9b8faaab6dd228a309e66daff8fae:1:0"
                            .to_string(),
                    post_transfer_output_value: Some(546),
                    tx_index: 54,
                },
            )])
            .build()
    }

    pub fn new_test_raw_block(
        transactions: Vec<BitcoinTransactionFullBreakdown>,
    ) -> BitcoinBlockFullBreakdown {
        BitcoinBlockFullBreakdown {
            hash: "000000000000000000018ddf8a6484db391fb85c9f9ddc384f03a92729423aaf".to_string(),
            height: 838964,
            tx: transactions,
            time: 1712982301,
            nonce: 100,
            previousblockhash: Some(
                "000000000000000000021f8b96d34c0f223281d7d825dd3588c2858c96e689d4".to_string(),
            ),
            confirmations: 200,
        }
    }

    pub fn new_test_reveal_raw_tx() -> BitcoinTransactionFullBreakdown {
        BitcoinTransactionFullBreakdown {
            txid: "b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735".to_string(),
            vin: vec![BitcoinTransactionInputFullBreakdown {
                sequence: 4294967293,
                txid: Some("a321c61c83563a377f82ef59301f2527079f6bda7c2d04f9f5954c873f42e8ac".to_string()),
                vout: Some(0),
                script_sig: Some(GetRawTransactionResultVinScriptSig { hex: "".to_string()}),
                txinwitness: Some(vec![
                    "6c00eb3c4d35fedd257051333b4ca81d1a25a37a9af4891f1fec2869edd56b14180eafbda8851d63138a724c9b15384bc5f0536de658bd294d426a36212e6f08".to_string(),
                    "209e2849b90a2353691fccedd467215c88eec89a5d0dcf468e6cf37abed344d746ac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d38004c5e7b200a20202270223a20226272632d3230222c0a2020226f70223a20226465706c6f79222c0a2020227469636b223a20226f726469222c0a2020226d6178223a20223231303030303030222c0a2020226c696d223a202231303030220a7d68".to_string(),
                    "c19e2849b90a2353691fccedd467215c88eec89a5d0dcf468e6cf37abed344d746".to_string(),
                ]),
                prevout: Some(
                    BitcoinTransactionInputPrevoutFullBreakdown { height: 779878, value: Amount::from_sat(14830) }
                ),
            }],
            vout: vec![],
        }
    }

    #[test_case(&TestBlockBuilder::new().build() => 0; "with empty block")]
    #[test_case(&TestBlockBuilder::new().transactions(vec![TestTransactionBuilder::new_with_operation().build()]).build() => 1; "with reveal transaction")]
    #[test_case(&TestBlockBuilder::new().transactions(vec![new_test_transfer_tx_with_operation()]).build() => 0; "with transfer transaction")]
    fn gets_reveals_in_block(block: &BitcoinBlockData) -> usize {
        get_inscriptions_revealed_in_block(block).len()
    }

    #[test_case(&TestBlockBuilder::new().build() => 0; "with empty block")]
    #[test_case(&TestBlockBuilder::new().transactions(vec![TestTransactionBuilder::new_with_operation().build()]).build() => 0; "with reveal transaction")]
    #[test_case(&TestBlockBuilder::new().transactions(vec![new_test_transfer_tx_with_operation()]).build() => 1; "with transfer transaction")]
    fn gets_transfers_in_block(block: &BitcoinBlockData) -> usize {
        get_inscriptions_transferred_in_block(block).len()
    }

    #[test]
    fn parses_inscriptions_in_block() {
        let ctx = Context::empty();
        let config = Config::test_default();
        let mut block = TestBlockBuilder::new()
            .add_transaction(
                TestTransactionBuilder::new()
                    .add_input(TestTxInBuilder::new().build())
                    .build(),
            )
            .build();
        parse_inscriptions_in_standardized_block(&mut block, &mut HashMap::new(), &config, &ctx);
        let OrdinalOperation::InscriptionRevealed(reveal) =
            &block.transactions[0].metadata.ordinal_operations[0]
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
    fn parses_inscriptions_in_raw_block() {
        let raw_block = new_test_raw_block(vec![new_test_reveal_raw_tx()]);
        let block = parse_inscriptions_and_standardize_block(
            raw_block,
            &BitcoinNetwork::Mainnet,
            &Context::empty(),
        )
        .unwrap();
        let OrdinalOperation::InscriptionRevealed(reveal) =
            &block.transactions[0].metadata.ordinal_operations[0]
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
}
