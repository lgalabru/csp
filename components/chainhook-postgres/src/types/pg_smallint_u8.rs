use std::error::Error;

use bytes::{BufMut, BytesMut};
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgSmallIntU8(pub u8);

impl ToSql for PgSmallIntU8 {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        out.put_u16(self.0 as u16);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "int2" || ty.name() == "smallint"
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for PgSmallIntU8 {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<PgSmallIntU8, Box<dyn Error + Sync + Send>> {
        let mut arr = [0u8; 1];
        arr.copy_from_slice(&raw[1..2]);
        Ok(PgSmallIntU8(u8::from_be_bytes(arr)))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "int2" || ty.name() == "smallint"
    }
}

#[cfg(test)]
mod test {
    use test_case::test_case;

    use crate::pg_test_client;

    use super::PgSmallIntU8;

    #[test_case(255; "u8 max")]
    #[test_case(0; "zero")]
    #[tokio::test]
    async fn test_u8_to_postgres(val: u8) {
        let mut client = pg_test_client().await;
        let value = PgSmallIntU8(val);
        let tx = client.transaction().await.unwrap();
        let _ = tx.query("CREATE TABLE test (value SMALLINT)", &[]).await;
        let _ = tx
            .query("INSERT INTO test (value) VALUES ($1)", &[&value])
            .await;
        let row = tx.query_one("SELECT value FROM test", &[]).await.unwrap();
        let res: PgSmallIntU8 = row.get("value");
        let _ = tx.rollback().await;
        assert_eq!(res.0, value.0);
    }
}
