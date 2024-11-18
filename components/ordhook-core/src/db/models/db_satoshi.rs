use chainhook_postgres::types::PgNumericU64;
use chainhook_sdk::types::OrdinalInscriptionRevealData;

use crate::ord::{rarity::Rarity, sat::Sat};

#[derive(Debug, Clone)]
pub struct DbSatoshi {
    pub ordinal_number: PgNumericU64,
    pub rarity: String,
    pub coinbase_height: PgNumericU64,
}

impl DbSatoshi {
    pub fn from_reveal(reveal: &OrdinalInscriptionRevealData) -> Self {
        let rarity = Rarity::from(Sat(reveal.ordinal_number));
        DbSatoshi {
            ordinal_number: PgNumericU64(reveal.ordinal_number),
            rarity: rarity.to_string(),
            coinbase_height: PgNumericU64(reveal.ordinal_block_height),
        }
    }
}
