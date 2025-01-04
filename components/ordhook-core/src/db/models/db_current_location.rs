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
pub struct DbCurrentLocation {
    pub ordinal_number: PgNumericU64,
    pub block_height: PgNumericU64,
    pub tx_id: String,
    pub tx_index: PgBigIntU32,
    pub address: Option<String>,
    pub output: String,
    pub offset: Option<PgNumericU64>,
}

impl DbCurrentLocation {
    pub fn from_reveal(
        reveal: &OrdinalInscriptionRevealData,
        block_identifier: &BlockIdentifier,
        tx_identifier: &TransactionIdentifier,
        tx_index: usize,
    ) -> Self {
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&reveal.satpoint_post_inscription).unwrap();
        DbCurrentLocation {
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_id: tx_identifier.hash[2..].to_string(),
            tx_index: PgBigIntU32(tx_index as u32),
            address: reveal.inscriber_address.clone(),
            output,
            offset: offset.map(|o| PgNumericU64(o)),
        }
    }

    pub fn from_transfer(
        transfer: &OrdinalInscriptionTransferData,
        block_identifier: &BlockIdentifier,
        tx_identifier: &TransactionIdentifier,
        tx_index: usize,
    ) -> Self {
        let (output, offset) =
            parse_output_and_offset_from_satpoint(&transfer.satpoint_post_transfer).unwrap();
        DbCurrentLocation {
            ordinal_number: PgNumericU64(transfer.ordinal_number),
            block_height: PgNumericU64(block_identifier.index),
            tx_id: tx_identifier.hash[2..].to_string(),
            tx_index: PgBigIntU32(tx_index as u32),
            address: match &transfer.destination {
                OrdinalInscriptionTransferDestination::Transferred(address) => {
                    Some(address.clone())
                }
                OrdinalInscriptionTransferDestination::SpentInFees => None,
                OrdinalInscriptionTransferDestination::Burnt(_) => None,
            },
            output,
            offset: offset.map(|o| PgNumericU64(o)),
        }
    }
}

impl FromPgRow for DbCurrentLocation {
    fn from_pg_row(row: &Row) -> Self {
        DbCurrentLocation {
            ordinal_number: row.get("ordinal_number"),
            block_height: row.get("block_height"),
            tx_id: row.get("tx_id"),
            tx_index: row.get("tx_index"),
            address: row.get("address"),
            output: row.get("output"),
            offset: row.get("offset"),
        }
    }
}
