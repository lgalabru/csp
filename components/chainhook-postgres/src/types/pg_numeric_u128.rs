use std::{
    cmp::Ordering,
    error::Error,
    io::{Cursor, Read},
    ops::{AddAssign, SubAssign},
};

use bytes::{BufMut, BytesMut};
use num_traits::{ToPrimitive, Zero};
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

/// Transforms a u128 value into postgres' `numeric` wire format.
pub fn u128_into_pg_numeric_bytes(number: u128, out: &mut BytesMut) {
    let mut num = number.clone();
    let mut digits = vec![];
    while !num.is_zero() {
        let remainder = (num % 10000).to_i16().unwrap();
        num /= 10000;
        digits.push(remainder);
    }
    digits.reverse();

    let num_digits = digits.len();
    let weight = if num_digits.is_zero() {
        0
    } else {
        num_digits - 1
    };
    out.reserve(8 + num_digits * 2);
    out.put_u16(num_digits.try_into().unwrap());
    out.put_i16(weight as i16); // Weight
    out.put_u16(0x0000); // Sign: Always positive
    out.put_u16(0x0000); // No decimals
    for digit in digits[0..num_digits].iter() {
        out.put_i16(*digit);
    }
}

fn read_two_bytes(cursor: &mut Cursor<&[u8]>) -> std::io::Result<[u8; 2]> {
    let mut result = [0; 2];
    cursor.read_exact(&mut result)?;
    Ok(result)
}

/// Reads a u128 value from a postgres `numeric` wire format.
pub fn pg_numeric_bytes_to_u128(raw: &[u8]) -> u128 {
    let mut raw = Cursor::new(raw);
    let num_groups = u16::from_be_bytes(read_two_bytes(&mut raw).unwrap());
    let weight = i16::from_be_bytes(read_two_bytes(&mut raw).unwrap());
    let _sign = u16::from_be_bytes(read_two_bytes(&mut raw).unwrap()); // Unused for uint
    let _scale = u16::from_be_bytes(read_two_bytes(&mut raw).unwrap()); // Unused for uint

    let mut groups = Vec::new();
    for _ in 0..num_groups as usize {
        groups.push(u16::from_be_bytes(read_two_bytes(&mut raw).unwrap()));
    }

    let mut result = 0;
    let zero: u16 = 0;
    for i in 0..(weight + 1) {
        let val = groups.get(i as usize).unwrap_or(&zero);
        let next = (*val as u128) * 10000_u128.pow((weight - i) as u32);
        result += next;
    }
    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PgNumericU128(pub u128);

impl ToSql for PgNumericU128 {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        u128_into_pg_numeric_bytes(self.0, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "numeric"
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for PgNumericU128 {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<PgNumericU128, Box<dyn Error + Sync + Send>> {
        let result = pg_numeric_bytes_to_u128(raw);
        Ok(PgNumericU128(result.to_u128().unwrap()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "numeric"
    }
}

impl AddAssign for PgNumericU128 {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}

impl AddAssign<u128> for PgNumericU128 {
    fn add_assign(&mut self, other: u128) {
        self.0 += other;
    }
}

impl SubAssign for PgNumericU128 {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}

impl SubAssign<u128> for PgNumericU128 {
    fn sub_assign(&mut self, other: u128) {
        self.0 -= other;
    }
}

impl PartialOrd for PgNumericU128 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.0.cmp(&other.0))
    }
}

impl Ord for PgNumericU128 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

#[cfg(test)]
mod test {
    use test_case::test_case;

    use crate::pg_test_client;

    use super::PgNumericU128;

    #[test_case(340282366920938463463374607431768211455; "u128 max")]
    #[test_case(80000000000000000; "with trailing zeros")]
    #[test_case(0; "zero")]
    #[tokio::test]
    async fn test_u128_to_postgres(val: u128) {
        let mut client = pg_test_client().await;
        let value = PgNumericU128(val);
        let tx = client.transaction().await.unwrap();
        let _ = tx.query("CREATE TABLE test (value NUMERIC)", &[]).await;
        let _ = tx
            .query("INSERT INTO test (value) VALUES ($1)", &[&value])
            .await;
        let row = tx.query_one("SELECT value FROM test", &[]).await.unwrap();
        let res: PgNumericU128 = row.get("value");
        let _ = tx.rollback().await;
        assert_eq!(res.0, value.0);
    }
}
