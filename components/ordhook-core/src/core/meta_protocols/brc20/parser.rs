use crate::ord::inscription::Inscription;
use crate::ord::media::{Language, Media};

#[derive(PartialEq, Debug, Clone)]
pub struct ParsedBrc20TokenDeployData {
    pub tick: String,
    pub display_tick: String,
    pub max: String,
    pub lim: String,
    pub dec: String,
    pub self_mint: bool,
}

#[derive(PartialEq, Debug, Clone)]
pub struct ParsedBrc20BalanceData {
    pub tick: String,
    pub amt: String,
}

impl ParsedBrc20BalanceData {
    pub fn float_amt(&self) -> f64 {
        self.amt.parse::<f64>().unwrap()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum ParsedBrc20Operation {
    Deploy(ParsedBrc20TokenDeployData),
    Mint(ParsedBrc20BalanceData),
    Transfer(ParsedBrc20BalanceData),
}

#[derive(Deserialize)]
struct Brc20DeployJson {
    p: String,
    op: String,
    tick: String,
    max: String,
    lim: Option<String>,
    dec: Option<String>,
    self_mint: Option<String>,
}

#[derive(Deserialize)]
struct Brc20MintOrTransferJson {
    p: String,
    op: String,
    tick: String,
    amt: String,
}

pub fn amt_has_valid_decimals(amt: &str, max_decimals: u8) -> bool {
    if amt.contains('.')
        && amt.split('.').nth(1).map_or(0, |s| s.chars().count()) as u8 > max_decimals
    {
        return false;
    }
    true
}

fn parse_float_numeric_value(n: &str, max_decimals: u8) -> Option<f64> {
    if n.chars().all(|c| c.is_ascii_digit() || c == '.') && !n.starts_with('.') && !n.ends_with('.') {
        if !amt_has_valid_decimals(n, max_decimals) {
            return None;
        }
        match n.parse::<f64>() {
            Ok(parsed) => {
                if parsed > u64::MAX as f64 {
                    return None;
                }
                return Some(parsed);
            }
            _ => return None,
        };
    }
    None
}

fn parse_deploy_decimals(n: &str) -> Option<u8> {
    if n.chars().all(|c| c.is_ascii_digit()) {
        match n.parse::<u8>() {
            Ok(parsed) => return Some(parsed),
            _ => return None,
        };
    }
    None
}

/// Attempts to parse an `Inscription` into a BRC20 operation by following the rules explained in
/// https://layer1.gitbook.io/layer1-foundation/protocols/brc-20/indexing
pub fn parse_brc20_operation(
    inscription: &Inscription,
) -> Result<Option<ParsedBrc20Operation>, String> {
    match inscription.media() {
        Media::Code(Language::Json) | Media::Text => {}
        _ => return Ok(None),
    };
    let Some(inscription_body) = inscription.body() else {
        return Ok(None);
    };
    match serde_json::from_slice::<Brc20DeployJson>(inscription_body) {
        Ok(json) => {
            if json.p != "brc-20" || json.op != "deploy" {
                return Ok(None);
            }
            let mut self_mint = false;
            if json.self_mint == Some("true".to_string()) {
                if json.tick.len() != 5 {
                    return Ok(None);
                }
                self_mint = true;
            } else if json.tick.len() != 4 {
                return Ok(None);
            }
            let mut decimals: u8 = 18;
            if let Some(dec) = json.dec {
                let Some(parsed_dec) = parse_deploy_decimals(&dec) else {
                    return Ok(None);
                };
                if parsed_dec > 18 {
                    return Ok(None);
                }
                decimals = parsed_dec;
            }
            let max: String;
            let Some(parsed_max) = parse_float_numeric_value(&json.max, decimals) else {
                return Ok(None);
            };
            if parsed_max == 0.0 {
                if self_mint {
                    max = u64::MAX.to_string();
                } else {
                    return Ok(None);
                }
            } else {
                max = json.max.clone();
            }
            let limit: String;
            if let Some(lim) = json.lim {
                let Some(parsed_lim) = parse_float_numeric_value(&lim, decimals) else {
                    return Ok(None);
                };
                if parsed_lim == 0.0 {
                    return Ok(None);
                }
                limit = lim;
            } else {
                limit = max.clone();
            }
            return Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
                tick: json.tick.to_lowercase(),
                display_tick: json.tick.clone(),
                max,
                lim: limit,
                dec: decimals.to_string(),
                self_mint,
            })));
        }
        Err(_) => match serde_json::from_slice::<Brc20MintOrTransferJson>(inscription_body) {
            Ok(json) => {
                if json.p != "brc-20" || json.tick.len() < 4 || json.tick.len() > 5 {
                    return Ok(None);
                }
                let op_str = json.op.as_str();
                match op_str {
                    "mint" | "transfer" => {
                        let Some(parsed_amt) = parse_float_numeric_value(&json.amt, 18) else {
                            return Ok(None);
                        };
                        if parsed_amt == 0.0 {
                            return Ok(None);
                        }
                        match op_str {
                            "mint" => {
                                return Ok(Some(ParsedBrc20Operation::Mint(
                                    ParsedBrc20BalanceData {
                                        tick: json.tick.to_lowercase(),
                                        amt: json.amt.clone(),
                                    },
                                )));
                            }
                            "transfer" => {
                                return Ok(Some(ParsedBrc20Operation::Transfer(
                                    ParsedBrc20BalanceData {
                                        tick: json.tick.to_lowercase(),
                                        amt: json.amt.clone(),
                                    },
                                )));
                            }
                            _ => return Ok(None),
                        }
                    }
                    _ => return Ok(None),
                }
            }
            Err(_) => return Ok(None),
        },
    };
}

#[cfg(test)]
mod test {
    use super::{parse_brc20_operation, ParsedBrc20Operation};
    use crate::{
        core::meta_protocols::brc20::parser::{ParsedBrc20BalanceData, ParsedBrc20TokenDeployData},
        ord::inscription::Inscription,
    };
    use test_case::test_case;

    struct InscriptionBuilder {
        body: Option<Vec<u8>>,
        content_encoding: Option<Vec<u8>>,
        content_type: Option<Vec<u8>>,
    }

    impl InscriptionBuilder {
        fn new() -> Self {
            InscriptionBuilder {
                body: Some(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "6"}"#.as_bytes().to_vec()),
                content_encoding: Some("utf-8".as_bytes().to_vec()),
                content_type: Some("text/plain".as_bytes().to_vec()),
            }
        }

        fn body(mut self, val: &str) -> Self {
            self.body = Some(val.as_bytes().to_vec());
            self
        }

        fn content_type(mut self, val: &str) -> Self {
            self.content_type = Some(val.as_bytes().to_vec());
            self
        }

        fn build(self) -> Inscription {
            Inscription {
                body: self.body,
                content_encoding: self.content_encoding,
                content_type: self.content_type,
                duplicate_field: false,
                incomplete_field: false,
                metadata: None,
                metaprotocol: None,
                parent: None,
                pointer: None,
                unrecognized_even_field: false,
                delegate: None,
            }
        }
    }

    #[test_case(
        InscriptionBuilder::new().build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "pepe".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: false,
        }))); "with deploy"
    )]
    #[test_case(
        InscriptionBuilder::new().body(&String::from("{\"p\":\"brc-20\",\"op\":\"deploy\",\"tick\":\"X\0\0Z\",\"max\":\"21000000\",\"lim\":\"1000\",\"dec\":\"6\"}")).build()
        => Ok(None); "with deploy null bytes"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "PEPE", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "PEPE".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: false,
        }))); "with deploy uppercase"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "pepe".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "18".to_string(),
            self_mint: false,
        }))); "with deploy without dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "pepe".to_string(),
            max: "21000000".to_string(),
            lim: "21000000".to_string(),
            dec: "18".to_string(),
            self_mint: false,
        }))); "with deploy without lim or dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "dec": "7"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "pepe".to_string(),
            max: "21000000".to_string(),
            lim: "21000000".to_string(),
            dec: "7".to_string(),
            self_mint: false,
        }))); "with deploy without lim"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "😉", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "😉".to_string(),
            display_tick: "😉".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: false,
        }))); "with deploy 4-byte emoji tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "a  b", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "a  b".to_string(),
            display_tick: "a  b".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: false,
        }))); "with deploy 4-byte space tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "$pepe", "max": "21000000", "lim": "1000", "dec": "6", "self_mint": "true"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "$pepe".to_string(),
            display_tick: "$pepe".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: true,
        }))); "with deploy 5-byte self mint"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "$pepe", "max": "0", "lim": "1000", "dec": "6", "self_mint": "true"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "$pepe".to_string(),
            display_tick: "$pepe".to_string(),
            max: (u64::MAX).to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: true,
        }))); "with deploy self mint max 0"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "$pepe", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy 5-byte no self mint"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "6", "foo": 99}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "pepe".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "6".to_string(),
            self_mint: false,
        }))); "with deploy extra fields"
    )]
    #[test_case(
        InscriptionBuilder::new().content_type("text/html").build()
        => Ok(None); "with invalid content_type"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p": "brc-20", "op": "deploy", "tick": "PEPE", "max": "21000000""#).build()
        => Ok(None); "with invalid JSON"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "6",}"#).build()
        => Ok(None); "with deploy JSON5"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"P":"brc20", "OP": "deploy", "TICK": "pepe", "MAX": "21000000", "LIM": "1000", "DEC": "6"}"#).build()
        => Ok(None); "with deploy uppercase fields"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy incorrect p field"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploi", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy incorrect op field"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pep", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy short tick length"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepepepe", "max": "21000000", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy long tick length"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000.", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy malformatted max"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": " 1000  ", "dec": "6"}"#).build()
        => Ok(None); "with deploy malformatted lim"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000.", "dec": "6.0"}"#).build()
        => Ok(None); "with deploy malformatted dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": 21000000, "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy int max"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": 1000, "dec": "6"}"#).build()
        => Ok(None); "with deploy int lim"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": 6}"#).build()
        => Ok(None); "with deploy int dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy empty max"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "", "dec": "6"}"#).build()
        => Ok(None); "with deploy empty lim"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": ""}"#).build()
        => Ok(None); "with deploy empty dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "99996744073709551615", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy large max"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "99996744073709551615", "dec": "6"}"#).build()
        => Ok(None); "with deploy large lim"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "99996744073709551615"}"#).build()
        => Ok(None); "with deploy large dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "0", "lim": "1000", "dec": "6"}"#).build()
        => Ok(None); "with deploy zero max"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "0", "dec": "6"}"#).build()
        => Ok(None); "with deploy zero lim"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000", "dec": "0"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Deploy(ParsedBrc20TokenDeployData {
            tick: "pepe".to_string(),
            display_tick: "pepe".to_string(),
            max: "21000000".to_string(),
            lim: "1000".to_string(),
            dec: "0".to_string(),
            self_mint: false,
        }))); "with deploy zero dec"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000.000", "lim": "1000", "dec": "0"}"#).build()
        => Ok(None); "with deploy extra max decimals"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "deploy", "tick": "pepe", "max": "21000000", "lim": "1000.000", "dec": "0"}"#).build()
        => Ok(None); "with deploy extra lim decimals"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepe", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000".to_string()
        }))); "with mint"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "PEPE", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000".to_string()
        }))); "with mint uppercase"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "😉", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "😉".to_string(),
            amt: "1000".to_string()
        }))); "with mint 4-byte emoji tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "$pepe", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "$pepe".to_string(),
            amt: "1000".to_string()
        }))); "with mint 5-byte tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "a  b", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "a  b".to_string(),
            amt: "1000".to_string()
        }))); "with mint 4-byte space tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "a  b", "amt": "1000", "bar": "test"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Mint(ParsedBrc20BalanceData {
            tick: "a  b".to_string(),
            amt: "1000".to_string()
        }))); "with mint extra fields"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "a  b", "amt": "1000",}"#).build()
        => Ok(None); "with mint JSON5"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"P":"brc-20", "OP": "mint", "TICK": "a  b", "AMT": "1000"}"#).build()
        => Ok(None); "with mint uppercase fields"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc20", "op": "mint", "tick": "pepe", "amt": "1000"}"#).build()
        => Ok(None); "with mint incorrect p field"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mintt", "tick": "pepe", "amt": "1000"}"#).build()
        => Ok(None); "with mint incorrect op field"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepe"}"#).build()
        => Ok(None); "with mint without amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pep", "amt": "1000"}"#).build()
        => Ok(None); "with mint short tick length"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepepepe", "amt": "1000"}"#).build()
        => Ok(None); "with mint long tick length"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepe", "amt": 1000}"#).build()
        => Ok(None); "with mint int amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepe", "amt": ""}"#).build()
        => Ok(None); "with mint empty amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepe", "amt": "0"}"#).build()
        => Ok(None); "with mint zero amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "mint", "tick": "pepe", "amt": "99996744073709551615"}"#).build()
        => Ok(None); "with mint large amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000".to_string()
        }))); "with transfer"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "PEPE", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "pepe".to_string(),
            amt: "1000".to_string()
        }))); "with transfer uppercase"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "😉", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "😉".to_string(),
            amt: "1000".to_string()
        }))); "with transfer 4-byte emoji tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "$pepe", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "$pepe".to_string(),
            amt: "1000".to_string()
        }))); "with transfer 5-byte tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "a  b", "amt": "1000"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "a  b".to_string(),
            amt: "1000".to_string()
        }))); "with transfer 4-byte space tick"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "a  b", "amt": "1000", "bar": "test"}"#).build()
        => Ok(Some(ParsedBrc20Operation::Transfer(ParsedBrc20BalanceData {
            tick: "a  b".to_string(),
            amt: "1000".to_string()
        }))); "with transfer extra fields"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe", "amt": "1000",}"#).build()
        => Ok(None); "with transfer JSON5"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"P":"brc-20", "OP": "transfer", "TICK": "a  b", "AMT": "1000"}"#).build()
        => Ok(None); "with transfer uppercase fields"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc20", "op": "transfer", "tick": "pepe", "amt": "1000"}"#).build()
        => Ok(None); "with transfer incorrect p field"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transferzz", "tick": "pepe", "amt": "1000"}"#).build()
        => Ok(None); "with transfer incorrect op field"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe"}"#).build()
        => Ok(None); "with transfer without amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pep", "amt": "1000"}"#).build()
        => Ok(None); "with transfer short tick length"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepepepe", "amt": "1000"}"#).build()
        => Ok(None); "with transfer long tick length"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe", "amt": 1000}"#).build()
        => Ok(None); "with transfer int amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe", "amt": ""}"#).build()
        => Ok(None); "with transfer empty amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe", "amt": "0"}"#).build()
        => Ok(None); "with transfer zero amt"
    )]
    #[test_case(
        InscriptionBuilder::new().body(r#"{"p":"brc-20", "op": "transfer", "tick": "pepe", "amt": "99996744073709551615"}"#).build()
        => Ok(None); "with transfer large amt"
    )]
    fn test_brc20_parse(inscription: Inscription) -> Result<Option<ParsedBrc20Operation>, String> {
        parse_brc20_operation(&inscription)
    }
}
