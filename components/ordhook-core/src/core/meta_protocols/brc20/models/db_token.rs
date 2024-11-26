use chainhook_postgres::{tokio_postgres::Row, types::{PgBigIntU32, PgNumericU128, PgNumericU64, PgSmallIntU8}};

#[derive(Debug, Clone, PartialEq, Eq)]
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
