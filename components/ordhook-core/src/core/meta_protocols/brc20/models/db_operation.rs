use chainhook_postgres::{
    tokio_postgres::Row,
    types::{PgBigIntU32, PgNumericU128, PgNumericU64},
    FromPgRow,
};

#[derive(Debug, Clone)]
pub struct DbOperation {
    pub ticker: String,
    pub operation: String,
    pub inscription_id: String,
    pub inscription_number: i64,
    pub ordinal_number: PgNumericU64,
    pub block_height: PgNumericU64,
    pub block_hash: String,
    pub tx_id: String,
    pub tx_index: PgNumericU64,
    pub output: String,
    pub offset: PgNumericU64,
    pub timestamp: PgBigIntU32,
    pub address: String,
    pub to_address: Option<String>,
    pub amount: PgNumericU128,
}

impl FromPgRow for DbOperation {
    fn from_pg_row(row: &Row) -> Self {
        DbOperation {
            ticker: row.get("ticker"),
            operation: row.get("operation"),
            inscription_id: row.get("inscription_id"),
            inscription_number: row.get("inscription_number"),
            ordinal_number: row.get("ordinal_number"),
            block_height: row.get("block_height"),
            block_hash: row.get("block_hash"),
            tx_id: row.get("tx_id"),
            tx_index: row.get("tx_index"),
            output: row.get("output"),
            offset: row.get("offset"),
            timestamp: row.get("timestamp"),
            address: row.get("address"),
            to_address: row.get("to_address"),
            amount: row.get("amount"),
        }
    }
}
