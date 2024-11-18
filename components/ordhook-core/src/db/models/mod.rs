use chainhook_postgres::types::{PgBigIntU32, PgNumericU64};
use chainhook_sdk::types::{
    BlockIdentifier, OrdinalInscriptionCurseType, OrdinalInscriptionRevealData,
    OrdinalInscriptionTransferData, OrdinalInscriptionTransferDestination, TransactionIdentifier,
};
use regex::Regex;

use crate::core::protocol::satoshi_tracking::parse_output_and_offset_from_satpoint;

lazy_static! {
    pub static ref RECURSIVE_INSCRIPTION_REGEX: Regex =
        Regex::new(r#"/\/content\/([a-fA-F0-9]{64}i\d+)$"#.into()).unwrap();
}

#[derive(Debug, Clone)]
pub struct DbInscription {
    pub inscription_id: String,
    pub ordinal_number: PgNumericU64,
    pub number: i64,
    pub classic_number: i64,
    pub block_height: PgNumericU64,
    pub block_hash: String,
    pub tx_index: PgBigIntU32,
    pub address: Option<String>,
    pub mime_type: String,
    pub content_type: String,
    pub content_length: PgBigIntU32,
    pub content: Vec<u8>,
    pub fee: PgNumericU64,
    pub curse_type: Option<String>,
    pub recursive: bool,
    pub input_index: PgBigIntU32,
    pub pointer: Option<PgNumericU64>,
    pub metadata: Option<String>,
    pub metaprotocol: Option<String>,
    pub parent: Option<String>,
    pub delegate: Option<String>,
    pub timestamp: PgBigIntU32,
}

impl DbInscription {
    pub fn from_reveal(
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        tx_index: usize,
        timestamp: u32,
    ) -> Self {
        DbInscription {
            inscription_id: reveal.inscription_id.clone(),
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            number: reveal.inscription_number.jubilee,
            classic_number: reveal.inscription_number.classic,
            block_height: PgNumericU64(block_identifier.index),
            block_hash: block_identifier.hash.clone(),
            tx_index: PgBigIntU32(tx_index as u32),
            address: reveal.inscriber_address.clone(),
            mime_type: reveal.content_type.split(';').nth(0).unwrap().to_string(),
            content_type: reveal.content_type.clone(),
            content_length: PgBigIntU32(reveal.content_length as u32),
            content: reveal.content_bytes.as_bytes().to_vec(),
            fee: PgNumericU64(reveal.inscription_fee),
            curse_type: reveal.curse_type.as_ref().map(|c| match c {
                OrdinalInscriptionCurseType::DuplicateField => "duplicate_field".to_string(),
                OrdinalInscriptionCurseType::IncompleteField => "incomplete_field".to_string(),
                OrdinalInscriptionCurseType::NotAtOffsetZero => "not_at_offset_zero".to_string(),
                OrdinalInscriptionCurseType::NotInFirstInput => "not_in_first_input".to_string(),
                OrdinalInscriptionCurseType::Pointer => "pointer".to_string(),
                OrdinalInscriptionCurseType::Pushnum => "pushnum".to_string(),
                OrdinalInscriptionCurseType::Reinscription => "reinscription".to_string(),
                OrdinalInscriptionCurseType::Stutter => "stutter".to_string(),
                OrdinalInscriptionCurseType::UnrecognizedEvenField => {
                    "unrecognized_field".to_string()
                }
                OrdinalInscriptionCurseType::Generic => "generic".to_string(),
            }),
            recursive: false, // This will be determined later
            input_index: PgBigIntU32(reveal.inscription_input_index as u32),
            pointer: reveal.inscription_pointer.map(|p| PgNumericU64(p)),
            metadata: reveal.metadata.as_ref().map(|m| m.to_string()),
            metaprotocol: reveal.metaprotocol.clone(),
            parent: reveal.parent.clone(),
            delegate: reveal.delegate.clone(),
            timestamp: PgBigIntU32(timestamp),
        }
    }
    //     pub fn from_pg_row(row: &Row) -> Self {
    //         DbInscription {
    //             inscription_id: row.get("inscription_id"),
    //             ordinal_number: row.get("ordinal_number"),
    //             number: row.get("number"),
    //             classic_number: row.get("classic_number"),
    //             block_height: row.get("block_height"),
    //             block_hash: row.get("block_hash"),
    //             tx_index: row.get("tx_index"),
    //             address: row.get("address"),
    //             mime_type: row.get("mime_type"),
    //             content_type: row.get("content_type"),
    //             content_length: row.get("content_length"),
    //             content: row.get("content"),
    //             fee: todo!(),
    //             curse_type: todo!(),
    //             recursive: todo!(),
    //             input_index: todo!(),
    //             pointer: todo!(),
    //             metadata: todo!(),
    //             metaprotocol: todo!(),
    //             parent: todo!(),
    //             delegate: todo!(),
    //             timestamp: todo!(),
    //         }
    //     }
}

#[derive(Debug, Clone)]
pub struct DbLocation {
    pub ordinal_number: PgNumericU64,
    pub block_height: PgNumericU64,
    pub tx_index: PgBigIntU32,
    pub tx_id: String,
    pub block_hash: String,
    pub address: Option<String>,
    pub output: String,
    pub offset: Option<PgNumericU64>,
    pub prev_output: Option<String>,
    pub prev_offset: Option<PgNumericU64>,
    pub value: Option<PgNumericU64>,
    pub transfer_type: String,
    pub timestamp: PgBigIntU32,
}

impl DbLocation {
    pub fn from_reveal(
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        tx_identifier: &TransactionIdentifier,
        tx_index: usize,
        timestamp: u32,
    ) -> Self {
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription).unwrap();
        DbLocation {
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgBigIntU32(tx_index as u32),
            tx_id: tx_identifier.hash.clone(),
            block_hash: block_identifier.hash.clone(),
            address: reveal.inscriber_address.clone(),
            output,
            offset: offset.map(|o| PgNumericU64(o)),
            prev_output: None,
            prev_offset: None,
            value: Some(PgNumericU64(reveal.inscription_output_value)),
            transfer_type: match reveal.inscriber_address {
                Some(_) => "transferred".to_string(),
                None => {
                    if reveal.inscription_output_value == 0 {
                        "spent_in_fees".to_string()
                    } else {
                        "burnt".to_string()
                    }
                }
            },
            timestamp: PgBigIntU32(timestamp),
        }
    }

    pub fn from_transfer(
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        tx_identifier: &TransactionIdentifier,
        tx_index: usize,
        timestamp: u32,
    ) -> Self {
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&transfer.satpoint_post_transfer).unwrap();
        let (prev_output, prev_offset) =
            parse_output_and_offset_from_satpoint(&transfer.satpoint_pre_transfer).unwrap();
        DbLocation {
            ordinal_number: PgNumericU64(transfer.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_index: PgBigIntU32(tx_index as u32),
            tx_id: tx_identifier.hash.clone(),
            block_hash: block_identifier.hash.clone(),
            address: match transfer.destination {
                OrdinalInscriptionTransferDestination::Transferred(address) => Some(address),
                OrdinalInscriptionTransferDestination::SpentInFees => None,
                OrdinalInscriptionTransferDestination::Burnt(_) => None,
            },
            output,
            offset: offset.map(|o| PgNumericU64(o)),
            prev_output: Some(prev_output),
            prev_offset: prev_offset.map(|o| PgNumericU64(o)),
            value: transfer.post_transfer_output_value.map(|v| PgNumericU64(v)),
            transfer_type: match transfer.destination {
                OrdinalInscriptionTransferDestination::Transferred(_) => "transferred".to_string(),
                OrdinalInscriptionTransferDestination::SpentInFees => "spent_in_fees".to_string(),
                OrdinalInscriptionTransferDestination::Burnt(_) => "burnt".to_string(),
            },
            timestamp: PgBigIntU32(timestamp),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DbInscriptionRecursion {
    pub inscription_id: String,
    pub ref_inscription_id: String,
}

impl DbInscriptionRecursion {
    pub fn from_reveal(reveal: &OrdinalInscriptionRevealData) -> Vec<Self> {
        let mut results = vec![];
        for capture in RECURSIVE_INSCRIPTION_REGEX.captures_iter(&reveal.content_bytes) {
            results.push(DbInscriptionRecursion {
                inscription_id: reveal.inscription_id.clone(),
                ref_inscription_id: capture.get(1).unwrap().as_str().to_string(),
            });
        }
        results
    }
}

#[derive(Debug, Clone)]
pub struct DbSatoshi {
    //
}
