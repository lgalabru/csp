use chainhook_postgres::{
    tokio_postgres::Row,
    types::{PgBigIntU32, PgNumericU64},
    FromPgRow,
};
use chainhook_sdk::types::{
    BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    OrdinalInscriptionTransferDestination, TransactionIdentifier,
};

use crate::core::protocol::satoshi_tracking::parse_output_and_offset_from_satpoint;

#[derive(Debug, Clone, PartialEq, Eq)]
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
            tx_id: tx_identifier.hash[2..].to_string(),
            block_hash: block_identifier.hash[2..].to_string(),
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
            tx_id: tx_identifier.hash[2..].to_string(),
            block_hash: block_identifier.hash[2..].to_string(),
            address: match &transfer.destination {
                OrdinalInscriptionTransferDestination::Transferred(address) => {
                    Some(address.clone())
                }
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

impl FromPgRow for DbLocation {
    fn from_pg_row(row: &Row) -> Self {
        DbLocation {
            ordinal_number: row.get("ordinal_number"),
            block_height: row.get("block_height"),
            tx_index: row.get("tx_index"),
            tx_id: row.get("tx_id"),
            block_hash: row.get("block_hash"),
            address: row.get("address"),
            output: row.get("output"),
            offset: row.get("offset"),
            prev_output: row.get("prev_output"),
            prev_offset: row.get("prev_offset"),
            value: row.get("value"),
            transfer_type: row.get("transfer_type"),
            timestamp: row.get("timestamp"),
        }
    }
}
