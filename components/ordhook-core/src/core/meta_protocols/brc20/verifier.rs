use chainhook_postgres::deadpool_postgres::Transaction;
use chainhook_sdk::types::{
    BitcoinNetwork, BlockIdentifier, OrdinalInscriptionRevealData, OrdinalInscriptionTransferData,
    OrdinalInscriptionTransferDestination,
};
use chainhook_sdk::utils::Context;

use super::{decimals_str_amount_to_u128, brc20_self_mint_activation_height};
use super::cache::Brc20MemoryCache;
use super::parser::{amt_has_valid_decimals, ParsedBrc20Operation};

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct VerifiedBrc20TokenDeployData {
    pub tick: String,
    pub display_tick: String,
    pub max: u128,
    pub lim: u128,
    pub dec: u8,
    pub address: String,
    pub self_mint: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct VerifiedBrc20BalanceData {
    pub tick: String,
    pub amt: u128,
    pub address: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct VerifiedBrc20TransferData {
    pub tick: String,
    pub amt: u128,
    pub sender_address: String,
    pub receiver_address: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub enum VerifiedBrc20Operation {
    TokenDeploy(VerifiedBrc20TokenDeployData),
    TokenMint(VerifiedBrc20BalanceData),
    TokenTransfer(VerifiedBrc20BalanceData),
    TokenTransferSend(VerifiedBrc20TransferData),
}

pub async fn verify_brc20_operation(
    operation: &ParsedBrc20Operation,
    reveal: &OrdinalInscriptionRevealData,
    block_identifier: &BlockIdentifier,
    network: &BitcoinNetwork,
    cache: &mut Brc20MemoryCache,
    db_tx: &Transaction<'_>,
    _ctx: &Context,
) -> Result<Option<VerifiedBrc20Operation>, String> {
    let Some(inscriber_address) = &reveal.inscriber_address else {
        // return Err(format!("Invalid inscriber address"));
        return Ok(None);
    };
    if inscriber_address.is_empty() {
        // return Err(format!("Empty inscriber address"));
        return Ok(None);
    }
    if reveal.inscription_number.classic < 0 {
        // return Err(format!("Inscription is cursed"));
        return Ok(None);
    }
    match operation {
        ParsedBrc20Operation::Deploy(data) => {
            if cache.get_token(&data.tick, db_tx).await?.is_some() {
                // return Err(format!("Token {} already exists", &data.tick));
                return Ok(None);
            }
            if data.self_mint && block_identifier.index < brc20_self_mint_activation_height(network)
            {
                // return Err(format!(
                //     "Self-minted token deploy {} prohibited before activation height",
                //     &data.tick
                // ));
                return Ok(None);
            }
            let decimals = data.dec.parse::<u8>().unwrap();
            return Ok(Some(VerifiedBrc20Operation::TokenDeploy(
                VerifiedBrc20TokenDeployData {
                    tick: data.tick.clone(),
                    display_tick: data.display_tick.clone(),
                    max: decimals_str_amount_to_u128(&data.max, decimals),
                    lim: decimals_str_amount_to_u128(&data.lim, decimals),
                    dec: decimals,
                    address: inscriber_address.clone(),
                    self_mint: data.self_mint,
                },
            )));
        }
        ParsedBrc20Operation::Mint(data) => {
            let Some(token) = cache.get_token(&data.tick, db_tx).await? else {
                // return Err(format!(
                //     "Token {} does not exist on mint attempt",
                //     &data.tick
                // ));
                return Ok(None);
            };
            if data.tick.len() == 5 {
                let Some(parent) = &reveal.parent else {
                    // return Err(format!(
                    //     "Attempting to mint self-minted token {} without a parent ref",
                    //     &data.tick
                    // ));
                    return Ok(None);
                };
                if parent != &token.inscription_id {
                    // return Err(format!(
                    //     "Mint attempt for self-minted token {} does not point to deploy as parent",
                    //     &data.tick
                    // ));
                    return Ok(None);
                }
            }
            if !amt_has_valid_decimals(&data.amt, token.decimals.0) {
                // return Err(format!(
                //     "Invalid decimals in amt field for {} mint, attempting to mint {}",
                //     token.ticker, data.amt
                // ));
                return Ok(None);
            }
            let amount = decimals_str_amount_to_u128(&data.amt, token.decimals.0);
            if amount > token.limit.0 {
                // return Err(format!(
                //     "Cannot mint more than {} tokens for {}, attempted to mint {}",
                //     token.limit.0, token.ticker, data.amt
                // ));
                return Ok(None);
            }
            let Some(minted_supply) = cache.get_token_minted_supply(&data.tick, db_tx).await?
            else {
                unreachable!("BRC-20 token exists but does not have entries in the ledger");
            };
            let remaining_supply = token.max.0 - minted_supply;
            if remaining_supply == 0 {
                // return Err(format!(
                //     "No supply available for {} mint, attempted to mint {}, remaining {}",
                //     token.ticker, data.amt, remaining_supply
                // ));
                return Ok(None);
            }
            let real_mint_amt = amount.min(token.limit.0.min(remaining_supply));
            return Ok(Some(VerifiedBrc20Operation::TokenMint(
                VerifiedBrc20BalanceData {
                    tick: token.ticker,
                    amt: real_mint_amt,
                    address: inscriber_address.clone(),
                },
            )));
        }
        ParsedBrc20Operation::Transfer(data) => {
            let Some(token) = cache.get_token(&data.tick, db_tx).await? else {
                // return Err(format!(
                //     "Token {} does not exist on transfer attempt",
                //     &data.tick
                // ));
                return Ok(None);
            };
            if !amt_has_valid_decimals(&data.amt, token.decimals.0) {
                // return Err(format!(
                //     "Invalid decimals in amt field for {} transfer, attempting to transfer {}",
                //     token.ticker, data.amt
                // ));
                return Ok(None);
            }
            let Some(avail_balance) = cache
                .get_token_address_avail_balance(&token.ticker, &inscriber_address, db_tx)
                .await?
            else {
                // return Err(format!(
                //     "Balance does not exist for {} transfer, attempting to transfer {}",
                //     token.ticker, data.amt
                // ));
                return Ok(None);
            };
            let amount = decimals_str_amount_to_u128(&data.amt, token.decimals.0);
            if avail_balance < amount {
                // return Err(format!("Insufficient balance for {} transfer, attempting to transfer {}, only {} available", token.ticker, data.amt, avail_balance));
                return Ok(None);
            }
            return Ok(Some(VerifiedBrc20Operation::TokenTransfer(
                VerifiedBrc20BalanceData {
                    tick: token.ticker,
                    amt: amount,
                    address: inscriber_address.clone(),
                },
            )));
        }
    };
}

pub async fn verify_brc20_transfer(
    transfer: &OrdinalInscriptionTransferData,
    cache: &mut Brc20MemoryCache,
    db_tx: &Transaction<'_>,
    _ctx: &Context
) -> Result<Option<VerifiedBrc20TransferData>, String> {
    let Some(transfer_row) = cache
        .get_unsent_token_transfer(transfer.ordinal_number, db_tx)
        .await?
    else {
        // return Err(format!(
        //     "No BRC-20 transfer in ordinal {} or transfer already sent",
        //     transfer.ordinal_number
        // ));
        return Ok(None);
    };
    match &transfer.destination {
        OrdinalInscriptionTransferDestination::Transferred(receiver_address) => {
            return Ok(Some(VerifiedBrc20TransferData {
                tick: transfer_row.ticker.clone(),
                amt: transfer_row.amount.0,
                sender_address: transfer_row.address.clone(),
                receiver_address: receiver_address.to_string(),
            }));
        }
        OrdinalInscriptionTransferDestination::SpentInFees => {
            return Ok(Some(VerifiedBrc20TransferData {
                tick: transfer_row.ticker.clone(),
                amt: transfer_row.amount.0,
                sender_address: transfer_row.address.clone(),
                receiver_address: transfer_row.address.clone(), // Return to sender
            }));
        }
        OrdinalInscriptionTransferDestination::Burnt(_) => {
            return Ok(Some(VerifiedBrc20TransferData {
                tick: transfer_row.ticker.clone(),
                amt: transfer_row.amount.0,
                sender_address: transfer_row.address.clone(),
                receiver_address: "".to_string(),
            }));
        }
    };
}

// #[cfg(test)]
// mod test {
//     use chainhook_sdk::types::{
//         BitcoinNetwork, BlockIdentifier, OrdinalInscriptionRevealData,
//         OrdinalInscriptionTransferData, OrdinalInscriptionTransferDestination,
//     };
//     use test_case::test_case;

//     use crate::core::meta_protocols::brc20::{
//         cache::Brc20MemoryCache,
//         db::initialize_brc20_db,
//         parser::{ParsedBrc20BalanceData, ParsedBrc20Operation, ParsedBrc20TokenDeployData},
//         test_utils::{get_test_ctx, Brc20RevealBuilder, Brc20TransferBuilder},
//         verifier::{
//             VerifiedBrc20BalanceData, VerifiedBrc20Operation, VerifiedBrc20TokenDeployData,
//         },
//     };

//     use super::{verify_brc20_operation, verify_brc20_transfer, VerifiedBrc20TransferData};

//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "pepe".to_string(),
//             display_tick: "pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: false,
//         }),
//         (Brc20RevealBuilder::new().inscriber_address(None).build(), 830000)
//         => Err("Invalid inscriber address".to_string()); "with invalid address"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "$pepe".to_string(),
//             display_tick: "$pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: true,
//         }),
//         (Brc20RevealBuilder::new().build(), 830000)
//         => Err("Self-minted token deploy $pepe prohibited before activation height".to_string());
//         "with self mint before activation"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "$pepe".to_string(),
//             display_tick: "$pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: true,
//         }),
//         (Brc20RevealBuilder::new().build(), 840000)
//         => Ok(VerifiedBrc20Operation::TokenDeploy(VerifiedBrc20TokenDeployData {
//             tick: "$pepe".to_string(),
//             display_tick: "$pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             self_mint: true,
//         }));
//         "with valid self mint"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "pepe".to_string(),
//             display_tick: "pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: false,
//         }),
//         (Brc20RevealBuilder::new().inscriber_address(Some("".to_string())).build(), 830000)
//         => Err("Empty inscriber address".to_string()); "with empty address"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "pepe".to_string(),
//             display_tick: "pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: false,
//         }),
//         (Brc20RevealBuilder::new().inscription_number(-1).build(), 830000)
//         => Err("Inscription is cursed".to_string()); "with cursed inscription"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "pepe".to_string(),
//             display_tick: "pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: false,
//         }),
//         (Brc20RevealBuilder::new().build(), 830000)
//         => Ok(
//             VerifiedBrc20Operation::TokenDeploy(VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             })
//         ); "with deploy"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         (Brc20RevealBuilder::new().build(), 830000)
//         => Err("Token pepe does not exist on mint attempt".to_string());
//         "with mint non existing token"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         (Brc20RevealBuilder::new().build(), 830000)
//         => Err("Token pepe does not exist on transfer attempt".to_string());
//         "with transfer non existing token"
//     )]
//     fn test_brc20_verify_for_empty_db(
//         op: ParsedBrc20Operation,
//         args: (OrdinalInscriptionRevealData, u64),
//     ) -> Result<VerifiedBrc20Operation, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         verify_brc20_operation(
//             &op,
//             &args.0,
//             &BlockIdentifier {
//                 index: args.1,
//                 hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b"
//                     .to_string(),
//             },
//             &BitcoinNetwork::Mainnet,
//             &mut Brc20MemoryCache::new(50),
//             &tx,
//             &ctx,
//         )
//     }

//     #[test_case(
//         ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
//             tick: "pepe".to_string(),
//             display_tick: "pepe".to_string(),
//             max: 21000000.0,
//             lim: 1000.0,
//             dec: 18,
//             self_mint: false,
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Err("Token pepe already exists".to_string()); "with deploy existing token"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Ok(VerifiedBrc20Operation::TokenMint(VerifiedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: 1000.0,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//         })); "with mint"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "10000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Err("Cannot mint more than 1000 tokens for pepe, attempted to mint 10000.0".to_string());
//         "with mint over lim"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "100.000000000000000000000".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Err("Invalid decimals in amt field for pepe mint, attempting to mint 100.000000000000000000000".to_string());
//         "with mint invalid decimals"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "100.0".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Err("Insufficient balance for pepe transfer, attempting to transfer 100.0, only 0 available".to_string());
//         "with transfer on zero balance"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "100.000000000000000000000".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Err("Invalid decimals in amt field for pepe transfer, attempting to transfer 100.000000000000000000000".to_string());
//         "with transfer invalid decimals"
//     )]
//     fn test_brc20_verify_for_existing_token(
//         op: ParsedBrc20Operation,
//         reveal: OrdinalInscriptionRevealData,
//     ) -> Result<VerifiedBrc20Operation, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 835727,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new().inscription_number(0).build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_operation(
//             &op,
//             &reveal,
//             &block,
//             &BitcoinNetwork::Mainnet,
//             &mut cache,
//             &tx,
//             &ctx,
//         )
//     }

//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "$pepe".to_string(),
//             amt: "100.00".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).build()
//         => Err("Attempting to mint self-minted token $pepe without a parent ref".to_string());
//         "with mint without parent pointer"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "$pepe".to_string(),
//             amt: "100.00".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(1).parent(Some("test".to_string())).build()
//         => Err("Mint attempt for self-minted token $pepe does not point to deploy as parent".to_string());
//         "with mint with wrong parent pointer"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "$pepe".to_string(),
//             amt: "100.00".to_string(),
//         }),
//         Brc20RevealBuilder::new()
//             .inscription_number(1)
//             .parent(Some("9bb2314d666ae0b1db8161cb373fcc1381681f71445c4e0335aa80ea9c37fcddi0".to_string()))
//             .build()
//         => Ok(VerifiedBrc20Operation::TokenMint(VerifiedBrc20BalanceData {
//             tick: "$pepe".to_string(),
//             amt: 100.0,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string()
//         }));
//         "with mint with valid parent"
//     )]
//     fn test_brc20_verify_for_existing_self_mint_token(
//         op: ParsedBrc20Operation,
//         reveal: OrdinalInscriptionRevealData,
//     ) -> Result<VerifiedBrc20Operation, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 840000,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "$pepe".to_string(),
//                 display_tick: "$pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: true,
//             },
//             &Brc20RevealBuilder::new().inscription_number(0).build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_operation(
//             &op,
//             &reveal,
//             &block,
//             &BitcoinNetwork::Mainnet,
//             &mut cache,
//             &tx,
//             &ctx,
//         )
//     }

//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(2).build()
//         => Err("No supply available for pepe mint, attempted to mint 1000.0, remaining 0".to_string());
//         "with mint on no more supply"
//     )]
//     fn test_brc20_verify_for_minted_out_token(
//         op: ParsedBrc20Operation,
//         reveal: OrdinalInscriptionRevealData,
//     ) -> Result<VerifiedBrc20Operation, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 835727,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new().inscription_number(0).build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 21000000.0, // For testing
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new().inscription_number(1).build(),
//             &block,
//             1,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_operation(
//             &op,
//             &reveal,
//             &block,
//             &BitcoinNetwork::Mainnet,
//             &mut cache,
//             &tx,
//             &ctx,
//         )
//     }

//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new().inscription_number(2).build()
//         => Ok(VerifiedBrc20Operation::TokenMint(VerifiedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: 500.0,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//         })); "with mint on low supply"
//     )]
//     fn test_brc20_verify_for_almost_minted_out_token(
//         op: ParsedBrc20Operation,
//         reveal: OrdinalInscriptionRevealData,
//     ) -> Result<VerifiedBrc20Operation, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 835727,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new().inscription_number(0).build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 21000000.0 - 500.0, // For testing
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new().inscription_number(1).build(),
//             &block,
//             1,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_operation(
//             &op,
//             &reveal,
//             &block,
//             &BitcoinNetwork::Mainnet,
//             &mut cache,
//             &tx,
//             &ctx,
//         )
//     }

//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new()
//             .inscription_number(3)
//             .inscription_id("04b29b646f6389154e4fa0f0761472c27b9f13a482c715d9976edc474c258bc7i0")
//             .build()
//         => Ok(VerifiedBrc20Operation::TokenMint(VerifiedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: 1000.0,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//         })); "with mint on existing balance address 1"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new()
//             .inscription_number(3)
//             .inscription_id("04b29b646f6389154e4fa0f0761472c27b9f13a482c715d9976edc474c258bc7i0")
//             .inscriber_address(Some("19aeyQe8hGDoA1MHmmh2oM5Bbgrs9Jx7yZ".to_string()))
//             .build()
//         => Ok(VerifiedBrc20Operation::TokenMint(VerifiedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: 1000.0,
//             address: "19aeyQe8hGDoA1MHmmh2oM5Bbgrs9Jx7yZ".to_string(),
//         })); "with mint on existing balance address 2"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "500.0".to_string(),
//         }),
//         Brc20RevealBuilder::new()
//             .inscription_number(3)
//             .inscription_id("04b29b646f6389154e4fa0f0761472c27b9f13a482c715d9976edc474c258bc7i0")
//             .build()
//         => Ok(VerifiedBrc20Operation::TokenTransfer(VerifiedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: 500.0,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//         })); "with transfer"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "1000".to_string(),
//         }),
//         Brc20RevealBuilder::new()
//             .inscription_number(3)
//             .inscription_id("04b29b646f6389154e4fa0f0761472c27b9f13a482c715d9976edc474c258bc7i0")
//             .build()
//         => Ok(VerifiedBrc20Operation::TokenTransfer(VerifiedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: 1000.0,
//             address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//         })); "with transfer full balance"
//     )]
//     #[test_case(
//         ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
//             tick: "pepe".to_string(),
//             amt: "5000.0".to_string(),
//         }),
//         Brc20RevealBuilder::new()
//             .inscription_number(3)
//             .inscription_id("04b29b646f6389154e4fa0f0761472c27b9f13a482c715d9976edc474c258bc7i0")
//             .build()
//         => Err("Insufficient balance for pepe transfer, attempting to transfer 5000.0, only 1000 available".to_string());
//         "with transfer insufficient balance"
//     )]
//     fn test_brc20_verify_for_token_with_mints(
//         op: ParsedBrc20Operation,
//         reveal: OrdinalInscriptionRevealData,
//     ) -> Result<VerifiedBrc20Operation, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 835727,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(0)
//                 .inscription_id(
//                     "e45957c419f130cd5c88cdac3eb1caf2d118aee20c17b15b74a611be395a065di0",
//                 )
//                 .build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         // Mint from 2 addresses
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(1)
//                 .inscription_id(
//                     "269d46f148733ce86153e3ec0e0a3c78780e9b07e90a07e11753f0e934a60724i0",
//                 )
//                 .build(),
//             &block,
//             1,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: "19aeyQe8hGDoA1MHmmh2oM5Bbgrs9Jx7yZ".to_string(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(2)
//                 .inscription_id(
//                     "704b85a939c34ec9dbbf79c0ffc69ba09566d732dbf1af2c04de65b0697aa1f8i0",
//                 )
//                 .build(),
//             &block,
//             2,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_operation(
//             &op,
//             &reveal,
//             &block,
//             &BitcoinNetwork::Mainnet,
//             &mut cache,
//             &tx,
//             &ctx,
//         )
//     }

//     #[test_case(
//         Brc20TransferBuilder::new().ordinal_number(5000).build()
//         => Ok(VerifiedBrc20TransferData {
//             tick: "pepe".to_string(),
//             amt: 500.0,
//             sender_address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             receiver_address: "bc1pls75sfwullhygkmqap344f5cqf97qz95lvle6fvddm0tpz2l5ffslgq3m0".to_string(),
//         });
//         "with transfer"
//     )]
//     #[test_case(
//         Brc20TransferBuilder::new()
//             .ordinal_number(5000)
//             .destination(OrdinalInscriptionTransferDestination::SpentInFees)
//             .build()
//         => Ok(VerifiedBrc20TransferData {
//             tick: "pepe".to_string(),
//             amt: 500.0,
//             sender_address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             receiver_address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string()
//         });
//         "with transfer spent as fee"
//     )]
//     #[test_case(
//         Brc20TransferBuilder::new()
//             .ordinal_number(5000)
//             .destination(OrdinalInscriptionTransferDestination::Burnt("test".to_string()))
//             .build()
//         => Ok(VerifiedBrc20TransferData {
//             tick: "pepe".to_string(),
//             amt: 500.0,
//             sender_address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             receiver_address: "".to_string()
//         });
//         "with transfer burnt"
//     )]
//     #[test_case(
//         Brc20TransferBuilder::new().ordinal_number(200).build()
//         => Err("No BRC-20 transfer in ordinal 200 or transfer already sent".to_string());
//         "with transfer non existent"
//     )]
//     fn test_brc20_verify_transfer_for_token_with_mint_and_transfer(
//         transfer: OrdinalInscriptionTransferData,
//     ) -> Result<VerifiedBrc20TransferData, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 835727,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(0)
//                 .inscription_id(
//                     "e45957c419f130cd5c88cdac3eb1caf2d118aee20c17b15b74a611be395a065di0",
//                 )
//                 .build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(1)
//                 .inscription_id(
//                     "269d46f148733ce86153e3ec0e0a3c78780e9b07e90a07e11753f0e934a60724i0",
//                 )
//                 .build(),
//             &block,
//             1,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_transfer(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 500.0,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(2)
//                 .ordinal_number(5000)
//                 .inscription_id(
//                     "704b85a939c34ec9dbbf79c0ffc69ba09566d732dbf1af2c04de65b0697aa1f8i0",
//                 )
//                 .build(),
//             &block,
//             2,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_transfer(&transfer, &mut cache, &tx, &ctx)
//     }

//     #[test_case(
//         Brc20TransferBuilder::new().ordinal_number(5000).build()
//         => Err("No BRC-20 transfer in ordinal 5000 or transfer already sent".to_string());
//         "with transfer already sent"
//     )]
//     fn test_brc20_verify_transfer_for_token_with_mint_transfer_and_send(
//         transfer: OrdinalInscriptionTransferData,
//     ) -> Result<VerifiedBrc20TransferData, String> {
//         let ctx = get_test_ctx();
//         let mut conn = initialize_brc20_db(None, &ctx);
//         let tx = conn.transaction().unwrap();
//         let block = BlockIdentifier {
//             index: 835727,
//             hash: "00000000000000000002d8ba402150b259ddb2b30a1d32ab4a881d4653bceb5b".to_string(),
//         };
//         let mut cache = Brc20MemoryCache::new(10);
//         cache.insert_token_deploy(
//             &VerifiedBrc20TokenDeployData {
//                 tick: "pepe".to_string(),
//                 display_tick: "pepe".to_string(),
//                 max: 21000000.0,
//                 lim: 1000.0,
//                 dec: 18,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 self_mint: false,
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(0)
//                 .inscription_id(
//                     "e45957c419f130cd5c88cdac3eb1caf2d118aee20c17b15b74a611be395a065di0",
//                 )
//                 .build(),
//             &block,
//             0,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_mint(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 1000.0,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(1)
//                 .inscription_id(
//                     "269d46f148733ce86153e3ec0e0a3c78780e9b07e90a07e11753f0e934a60724i0",
//                 )
//                 .build(),
//             &block,
//             1,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_transfer(
//             &VerifiedBrc20BalanceData {
//                 tick: "pepe".to_string(),
//                 amt: 500.0,
//                 address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//             },
//             &Brc20RevealBuilder::new()
//                 .inscription_number(2)
//                 .ordinal_number(5000)
//                 .inscription_id(
//                     "704b85a939c34ec9dbbf79c0ffc69ba09566d732dbf1af2c04de65b0697aa1f8i0",
//                 )
//                 .build(),
//             &block,
//             2,
//             &tx,
//             &ctx,
//         );
//         cache.insert_token_transfer_send(
//             &VerifiedBrc20TransferData {
//                 tick: "pepe".to_string(),
//                 amt: 500.0,
//                 sender_address: "324A7GHA2azecbVBAFy4pzEhcPT1GjbUAp".to_string(),
//                 receiver_address: "bc1pls75sfwullhygkmqap344f5cqf97qz95lvle6fvddm0tpz2l5ffslgq3m0"
//                     .to_string(),
//             },
//             &Brc20TransferBuilder::new().ordinal_number(5000).build(),
//             &block,
//             3,
//             &tx,
//             &ctx,
//         );
//         verify_brc20_transfer(&transfer, &mut cache, &tx, &ctx)
//     }
// }
