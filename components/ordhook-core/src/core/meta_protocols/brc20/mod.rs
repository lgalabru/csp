use chainhook_sdk::types::BitcoinNetwork;

pub mod brc20_pg;
pub mod cache;
pub mod parser;
pub mod test_utils;
pub mod verifier;
pub mod models;

pub fn brc20_activation_height(network: &BitcoinNetwork) -> u64 {
    match network {
        BitcoinNetwork::Mainnet => 779832,
        BitcoinNetwork::Regtest => 0,
        BitcoinNetwork::Testnet => 0,
        BitcoinNetwork::Signet => 0,
    }
}

pub fn brc20_self_mint_activation_height(network: &BitcoinNetwork) -> u64 {
    match network {
        BitcoinNetwork::Mainnet => 837090,
        BitcoinNetwork::Regtest => 0,
        BitcoinNetwork::Testnet => 0,
        BitcoinNetwork::Signet => 0,
    }
}

/// Transform a BRC-20 amount `String` (that may or may not have decimals) to a `u128` value we can store in Postgres. The amount
/// will be shifted to the left by however many decimals the token uses.
pub fn decimals_str_amount_to_u128(amt: &String, decimals: u8) -> u128 {
    let parts: Vec<&str> = amt.split('.').collect();
    let integer = (*parts.get(0).unwrap()).parse::<u128>().unwrap();

    let decimal_str = *parts.get(1).unwrap();
    let mut padded = String::with_capacity(decimals as usize);
    padded.push_str(decimal_str);
    padded.push_str(&"0".repeat(decimals as usize - decimal_str.len()));
    let fractional = padded.parse::<u128>().unwrap();

    (integer * 10u128.pow(decimals as u32)) + fractional
}

/// Transform a BRC-20 amount which was stored in Postgres as a `u128` back to a `String` with decimals included.
pub fn u128_amount_to_decimals_str(amount: u128, decimals: u8) -> String {
    let num_str = amount.to_string();
    let (integer, fractional) = num_str.split_at(num_str.len() - decimals as usize);
    format!("{}.{}", integer, fractional)
}
