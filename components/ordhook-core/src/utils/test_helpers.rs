use chainhook_sdk::{
    bitcoin::Amount,
    indexer::bitcoin::{
        BitcoinBlockFullBreakdown, BitcoinTransactionFullBreakdown,
        BitcoinTransactionInputFullBreakdown, BitcoinTransactionInputPrevoutFullBreakdown,
        GetRawTransactionResultVinScriptSig,
    },
    types::{
        bitcoin::{OutPoint, TxIn},
        BitcoinBlockData, BitcoinBlockMetadata, BitcoinNetwork, BitcoinTransactionData,
        BitcoinTransactionMetadata, BlockIdentifier, OrdinalInscriptionNumber,
        OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
        OrdinalInscriptionTransferDestination, OrdinalOperation, TransactionIdentifier,
    },
};

pub fn new_test_block(transactions: Vec<BitcoinTransactionData>) -> BitcoinBlockData {
    // Add a coinbase transaction.
    let mut txs = vec![BitcoinTransactionData {
        transaction_identifier: TransactionIdentifier {
            hash: "0xd92cf8bb6f0767a52f82d72365a1bdfbc2009eb2248e945a4d85ae7da81d11c2".to_string(),
        },
        operations: vec![],
        metadata: BitcoinTransactionMetadata {
            inputs: vec![],
            outputs: vec![],
            ordinal_operations: vec![],
            stacks_operations: vec![],
            brc20_operation: None,
            proof: None,
            fee: 0,
            index: 0,
        },
    }];
    txs.extend(transactions);
    BitcoinBlockData {
        block_identifier: BlockIdentifier {
            index: 838964,
            hash: "0x000000000000000000018ddf8a6484db391fb85c9f9ddc384f03a92729423aaf".to_string(),
        },
        parent_block_identifier: BlockIdentifier {
            hash: "0x000000000000000000021f8b96d34c0f223281d7d825dd3588c2858c96e689d4".to_string(),
            index: 838963,
        },
        timestamp: 1712982301,
        transactions: txs,
        metadata: BitcoinBlockMetadata {
            network: BitcoinNetwork::Mainnet,
        },
    }
}

pub fn new_test_raw_block(
    transactions: Vec<BitcoinTransactionFullBreakdown>,
) -> BitcoinBlockFullBreakdown {
    // Add a coinbase transaction.
    let mut txs = vec![BitcoinTransactionFullBreakdown {
        txid: "d92cf8bb6f0767a52f82d72365a1bdfbc2009eb2248e945a4d85ae7da81d11c2".to_string(),
        vin: vec![],
        vout: vec![],
    }];
    txs.extend(transactions);
    BitcoinBlockFullBreakdown {
        hash: "000000000000000000018ddf8a6484db391fb85c9f9ddc384f03a92729423aaf".to_string(),
        height: 838964,
        tx: txs,
        time: 1712982301,
        nonce: 100,
        previousblockhash: Some(
            "000000000000000000021f8b96d34c0f223281d7d825dd3588c2858c96e689d4".to_string(),
        ),
        confirmations: 200,
    }
}

pub fn new_test_reveal_tx() -> BitcoinTransactionData {
    // Represents the `ordi` BRC-20 token deploy.
    BitcoinTransactionData {
        transaction_identifier: TransactionIdentifier {
            hash: "0xb61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735".to_string(),
        },
        operations: vec![],
        metadata: BitcoinTransactionMetadata {
            inputs: vec![
                TxIn {
                    previous_output: OutPoint {
                        txid: TransactionIdentifier { hash: "0xa321c61c83563a377f82ef59301f2527079f6bda7c2d04f9f5954c873f42e8ac".to_string() },
                        vout: 0,
                        value: 14830,
                        block_height: 779878,
                    },
                    script_sig: "".to_string(),
                    sequence: 4294967293,
                    witness: vec![
                        "0x6c00eb3c4d35fedd257051333b4ca81d1a25a37a9af4891f1fec2869edd56b14180eafbda8851d63138a724c9b15384bc5f0536de658bd294d426a36212e6f08".to_string(),
                        "0x209e2849b90a2353691fccedd467215c88eec89a5d0dcf468e6cf37abed344d746ac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d38004c5e7b200a20202270223a20226272632d3230222c0a2020226f70223a20226465706c6f79222c0a2020227469636b223a20226f726469222c0a2020226d6178223a20223231303030303030222c0a2020226c696d223a202231303030220a7d68".to_string(),
                        "0xc19e2849b90a2353691fccedd467215c88eec89a5d0dcf468e6cf37abed344d746".to_string(),
                    ],
                },
            ],
            outputs: vec![],
            ordinal_operations: vec![],
            stacks_operations: vec![],
            brc20_operation: None,
            proof: None,
            fee: 0,
            index: 0,
        },
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

pub fn new_test_reveal_tx_with_operation() -> BitcoinTransactionData {
    let mut data = new_test_reveal_tx();
    data.metadata.ordinal_operations = vec![
        OrdinalOperation::InscriptionRevealed(
            OrdinalInscriptionRevealData {
                content_bytes: "0x7b200a20202270223a20226272632d3230222c0a2020226f70223a20226465706c6f79222c0a2020227469636b223a20226f726469222c0a2020226d6178223a20223231303030303030222c0a2020226c696d223a202231303030220a7d".to_string(),
                content_type: "text/plain;charset=utf-8".to_string(),
                content_length: 94,
                inscription_number: OrdinalInscriptionNumber { classic: 0, jubilee: 0 },
                inscription_fee: 0,
                inscription_output_value: 0,
                inscription_id: "b61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735i0".to_string(),
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
    ];
    data
}

pub fn new_test_transfer_tx() -> BitcoinTransactionData {
    BitcoinTransactionData {
        transaction_identifier: TransactionIdentifier {
            hash: "0x42fa098abab8d5cca1c303a97bd0404cf8e9b8faaab6dd228a309e66daff8fae".to_string(),
        },
        operations: vec![],
        metadata: BitcoinTransactionMetadata {
            inputs: vec![
                TxIn {
                    previous_output: OutPoint {
                        txid: TransactionIdentifier { hash: "0xab2683db34e335c89a5c1d634e6c5bd8d8bca8ded281be84f71f921c9e8783b2".to_string() },
                        vout: 0,
                        value: 546,
                        block_height: 779878,
                    },
                    script_sig: "".to_string(),
                    sequence: 4294967293,
                    witness: vec![
                        "0x17afbd7f7e4b1acff7c8d01ee74fc644ad4b7244559074c46aa0477f8685267b66716486f282e924433ddcf864fe4538d3514b084f3011edfc38223b8724a122".to_string(),
                    ],
                },
            ],
            outputs: vec![],
            ordinal_operations: vec![],
            stacks_operations: vec![],
            brc20_operation: None,
            proof: None,
            fee: 0,
            index: 0,
        },
    }
}

pub fn new_test_transfer_tx_with_operation() -> BitcoinTransactionData {
    let mut data = new_test_transfer_tx();
    data.metadata.ordinal_operations = vec![OrdinalOperation::InscriptionTransferred(
        OrdinalInscriptionTransferData {
            ordinal_number: 300144140535834,
            destination: OrdinalInscriptionTransferDestination::Transferred(
                "bc1pcwway0ne322s0lrc5e905f3chuclvnyy3z6wn86azkgmgcprf3tqvyy7ws".to_string(),
            ),
            satpoint_pre_transfer:
                "ab2683db34e335c89a5c1d634e6c5bd8d8bca8ded281be84f71f921c9e8783b2:0:0".to_string(),
            satpoint_post_transfer:
                "42fa098abab8d5cca1c303a97bd0404cf8e9b8faaab6dd228a309e66daff8fae:1:0".to_string(),
            post_transfer_output_value: Some(546),
            tx_index: 54,
        },
    )];
    data
}
