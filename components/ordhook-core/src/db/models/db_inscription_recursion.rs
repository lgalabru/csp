use chainhook_sdk::types::OrdinalInscriptionRevealData;
use regex::Regex;

lazy_static! {
    pub static ref RECURSIVE_INSCRIPTION_REGEX: Regex =
        Regex::new(r#"/\/content\/([a-fA-F0-9]{64}i\d+)$"#.into()).unwrap();
}

#[derive(Debug, Clone)]
pub struct DbInscriptionRecursion {
    pub inscription_id: String,
    pub ref_inscription_id: String,
}

impl DbInscriptionRecursion {
    pub fn from_reveal(reveal: &OrdinalInscriptionRevealData) -> Vec<Self> {
        let mut results = vec![];
        for capture in RECURSIVE_INSCRIPTION_REGEX.captures_iter(&reveal.content_bytes) {
            results.push(DbInscriptionRecursion {
                inscription_id: reveal.inscription_id.clone(),
                ref_inscription_id: capture.get(1).unwrap().as_str().to_string(),
            });
        }
        results
    }
}
