use chainhook_postgres::{tokio_postgres::Row, types::{PgBigIntU32, PgNumericU128, PgNumericU64, PgSmallIntU8}};

#[derive(Debug, Clone)]
pub struct DbToken {
    pub ticker: String,
    pub display_ticker: String,
    pub inscription_id: String,
    pub inscription_number: i64,
    pub block_height: PgNumericU64,
    pub block_hash: String,
    pub tx_id: String,
    pub tx_index: PgNumericU64,
    pub address: String,
    pub max: PgNumericU128,
    pub limit: PgNumericU128,
    pub decimals: PgSmallIntU8,
    pub self_mint: bool,
    pub minted_supply: PgNumericU128,
    pub tx_count: i32,
    pub timestamp: PgBigIntU32,
}

impl DbToken {
    pub fn from_pg_row(row: &Row) -> Self {
        DbToken {
            ticker: row.get("ticker"),
            display_ticker: row.get("display_ticker"),
            inscription_id: row.get("inscription_id"),
            inscription_number: row.get("inscription_number"),
            block_height: row.get("block_height"),
            block_hash: row.get("block_hash"),
            tx_id: row.get("tx_id"),
            tx_index: row.get("tx_index"),
            address: row.get("address"),
            max: row.get("max"),
            limit: row.get("limit"),
            decimals: row.get("decimals"),
            self_mint: row.get("self_mint"),
            minted_supply: row.get("minted_supply"),
            tx_count: row.get("tx_count"),
            timestamp: row.get("timestamp"),
        }
    }
}

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

impl DbOperation {
    pub fn from_pg_row(row: &Row) -> Self {
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
