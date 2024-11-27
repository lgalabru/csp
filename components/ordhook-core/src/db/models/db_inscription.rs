use chainhook_postgres::{
    tokio_postgres::Row,
    types::{PgBigIntU32, PgNumericU64},
    FromPgRow,
};
use chainhook_sdk::types::{
    BlockIdentifier, OrdinalInscriptionCurseType, OrdinalInscriptionRevealData,
    TransactionIdentifier,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbInscription {
    pub inscription_id: String,
    pub ordinal_number: PgNumericU64,
    pub number: i64,
    pub classic_number: i64,
    pub block_height: PgNumericU64,
    pub block_hash: String,
    pub tx_id: String,
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
        tx_identifier: &TransactionIdentifier,
        tx_index: usize,
        timestamp: u32,
    ) -> Self {
        DbInscription {
            inscription_id: reveal.inscription_id.clone(),
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            number: reveal.inscription_number.jubilee,
            classic_number: reveal.inscription_number.classic,
            block_height: PgNumericU64(block_identifier.index),
            block_hash: block_identifier.hash[2..].to_string(),
            tx_id: tx_identifier.hash[2..].to_string(),
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
}

impl FromPgRow for DbInscription {
    fn from_pg_row(row: &Row) -> Self {
        DbInscription {
            inscription_id: row.get("inscription_id"),
            ordinal_number: row.get("ordinal_number"),
            number: row.get("number"),
            classic_number: row.get("classic_number"),
            block_height: row.get("block_height"),
            block_hash: row.get("block_hash"),
            tx_id: row.get("tx_id"),
            tx_index: row.get("tx_index"),
            address: row.get("address"),
            mime_type: row.get("mime_type"),
            content_type: row.get("content_type"),
            content_length: row.get("content_length"),
            content: row.get("content"),
            fee: row.get("fee"),
            curse_type: row.get("curse_type"),
            recursive: row.get("recursive"),
            input_index: row.get("input_index"),
            pointer: row.get("pointer"),
            metadata: row.get("metadata"),
            metaprotocol: row.get("metaprotocol"),
            parent: row.get("parent"),
            delegate: row.get("delegate"),
            timestamp: row.get("timestamp"),
        }
    }
}
