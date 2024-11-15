use chainhook_sdk::types::BitcoinNetwork;

pub mod brc20_pg;
pub mod cache;
pub mod db;
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

pub fn format_amount_with_decimals(amount: u128, decimals: u8) -> String {
    let num_str = amount.to_string();
    let (integer_part, fractional_part) = num_str.split_at(num_str.len() - decimals as usize);
    format!("{}.{}", integer_part, fractional_part)
}

pub fn amount_to_u128_shift(amt: &String, decimals: u8) -> u128 {
    let parts: Vec<&str> = amt.split('.').collect();
    let first = (*parts.get(0).unwrap()).parse::<u128>().unwrap();

    let decimal_str = *parts.get(1).unwrap();
    let mut padded = String::with_capacity(decimals as usize);
    padded.push_str(decimal_str);
    padded.push_str(&"0".repeat(decimals as usize - decimal_str.len()));
    let second = padded.parse::<u128>().unwrap();

    (first * 10u128.pow(decimals as u32)) + second
}
