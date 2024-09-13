use chainhook_sdk::types::{
    bitcoin::{OutPoint, TxIn, TxOut},
    BitcoinBlockData, BitcoinBlockMetadata, BitcoinNetwork, BitcoinTransactionData,
    BitcoinTransactionMetadata, BlockIdentifier, OrdinalInscriptionNumber,
    OrdinalInscriptionRevealData, OrdinalOperation, TransactionIdentifier,
};

pub struct TestBlockBuilder {
    pub height: u64,
    pub hash: String,
    pub transactions: Vec<BitcoinTransactionData>,
}

impl TestBlockBuilder {
    pub fn new() -> Self {
        TestBlockBuilder {
            height: 838964,
            hash: "0x000000000000000000018ddf8a6484db391fb85c9f9ddc384f03a92729423aaf".to_string(),
            transactions: vec![],
        }
    }

    pub fn height(mut self, height: u64) -> Self {
        self.height = height;
        self
    }

    pub fn hash(mut self, hash: String) -> Self {
        self.hash = hash;
        self
    }

    pub fn transactions(mut self, transactions: Vec<BitcoinTransactionData>) -> Self {
        self.transactions = transactions;
        self
    }

    pub fn add_transaction(mut self, transaction: BitcoinTransactionData) -> Self {
        self.transactions.push(transaction);
        self
    }

    pub fn build(&self) -> BitcoinBlockData {
        BitcoinBlockData {
            block_identifier: BlockIdentifier {
                index: self.height,
                hash: self.hash.clone(),
            },
            parent_block_identifier: BlockIdentifier {
                hash: "0x000000000000000000021f8b96d34c0f223281d7d825dd3588c2858c96e689d4"
                    .to_string(),
                index: self.height - 1,
            },
            timestamp: 1712982301,
            transactions: self.transactions.clone(),
            metadata: BitcoinBlockMetadata {
                network: BitcoinNetwork::Mainnet,
            },
        }
    }
}

pub struct TestTransactionBuilder {
    hash: String,
    inputs: Vec<TxIn>,
    outputs: Vec<TxOut>,
    ordinal_operations: Vec<OrdinalOperation>,
}

impl TestTransactionBuilder {
    pub fn new() -> Self {
        TestTransactionBuilder {
            hash: "0xb61b0172d95e266c18aea0c624db987e971a5d6d4ebc2aaed85da4642d635735".to_string(),
            ordinal_operations: vec![],
            inputs: vec![],
            outputs: vec![],
        }
    }

    pub fn new_with_operation() -> Self {
        let mut tx = Self::new();
        tx.ordinal_operations = vec![OrdinalOperation::InscriptionRevealed(
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
        )];
        tx
    }

    pub fn hash(mut self, hash: String) -> Self {
        self.hash = hash;
        self
    }

    pub fn inputs(mut self, inputs: Vec<TxIn>) -> Self {
        self.inputs = inputs;
        self
    }

    pub fn add_input(mut self, input: TxIn) -> Self {
        self.inputs.push(input);
        self
    }

    pub fn outputs(mut self, outputs: Vec<TxOut>) -> Self {
        self.outputs = outputs;
        self
    }

    pub fn add_output(mut self, output: TxOut) -> Self {
        self.outputs.push(output);
        self
    }

    pub fn ordinal_operations(mut self, ordinal_operations: Vec<OrdinalOperation>) -> Self {
        self.ordinal_operations = ordinal_operations;
        self
    }

    pub fn build(self) -> BitcoinTransactionData {
        BitcoinTransactionData {
            transaction_identifier: TransactionIdentifier { hash: self.hash },
            operations: vec![],
            metadata: BitcoinTransactionMetadata {
                inputs: self.inputs,
                outputs: self.outputs,
                ordinal_operations: self.ordinal_operations,
                stacks_operations: vec![],
                brc20_operation: None,
                proof: None,
                fee: 0,
                index: 0,
            },
        }
    }
}

pub struct TestTxInBuilder {
    prev_out_block_height: u64,
    prev_out_tx_hash: String,
    value: u64,
}

impl TestTxInBuilder {
    pub fn new() -> Self {
        TestTxInBuilder {
            prev_out_block_height: 849999,
            prev_out_tx_hash: "0xa321c61c83563a377f82ef59301f2527079f6bda7c2d04f9f5954c873f42e8ac"
                .to_string(),
            value: 10000,
        }
    }

    pub fn prev_out_block_height(mut self, block_height: u64) -> Self {
        self.prev_out_block_height = block_height;
        self
    }

    pub fn prev_out_tx_hash(mut self, hash: String) -> Self {
        self.prev_out_tx_hash = hash;
        self
    }

    pub fn value(mut self, value: u64) -> Self {
        self.value = value;
        self
    }

    pub fn build(self) -> TxIn {
        TxIn {
            previous_output: OutPoint {
                txid: TransactionIdentifier { hash: self.prev_out_tx_hash },
                vout: 0,
                value: self.value,
                block_height: self.prev_out_block_height,
            },
            script_sig: "".to_string(),
            sequence: 4294967293,
            witness: vec![
                "0x6c00eb3c4d35fedd257051333b4ca81d1a25a37a9af4891f1fec2869edd56b14180eafbda8851d63138a724c9b15384bc5f0536de658bd294d426a36212e6f08".to_string(),
                "0x209e2849b90a2353691fccedd467215c88eec89a5d0dcf468e6cf37abed344d746ac0063036f7264010118746578742f706c61696e3b636861727365743d7574662d38004c5e7b200a20202270223a20226272632d3230222c0a2020226f70223a20226465706c6f79222c0a2020227469636b223a20226f726469222c0a2020226d6178223a20223231303030303030222c0a2020226c696d223a202231303030220a7d68".to_string(),
                "0xc19e2849b90a2353691fccedd467215c88eec89a5d0dcf468e6cf37abed344d746".to_string(),
            ],
        }
    }
}

pub struct TestTxOutBuilder {
    value: u64,
    script_pubkey: String,
}

impl TestTxOutBuilder {
    pub fn new() -> Self {
        TestTxOutBuilder {
            value: 5_000,
            script_pubkey: "0x00146aa45f66b73dab2bfbe02f2062a7249204479f85".to_string(),
        }
    }

    pub fn value(mut self, value: u64) -> Self {
        self.value = value;
        self
    }

    pub fn script_pubkey(mut self, script_pubkey: String) -> Self {
        self.script_pubkey = script_pubkey;
        self
    }

    pub fn build(self) -> TxOut {
        TxOut {
            value: self.value,
            script_pubkey: self.script_pubkey,
        }
    }
}
