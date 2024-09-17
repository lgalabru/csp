pub mod bitcoind;
pub mod logger;
pub mod monitoring;

use std::{
    fs,
    io::{Read, Write},
    path::PathBuf,
};

use chainhook_sdk::types::TransactionIdentifier;

pub fn read_file_content_at_path(file_path: &PathBuf) -> Result<Vec<u8>, String> {
    use std::fs::File;
    use std::io::BufReader;

    let file = File::open(file_path.clone())
        .map_err(|e| format!("unable to read file {}\n{:?}", file_path.display(), e))?;
    let mut file_reader = BufReader::new(file);
    let mut file_buffer = vec![];
    file_reader
        .read_to_end(&mut file_buffer)
        .map_err(|e| format!("unable to read file {}\n{:?}", file_path.display(), e))?;
    Ok(file_buffer)
}

pub fn write_file_content_at_path(file_path: &PathBuf, content: &[u8]) -> Result<(), String> {
    use std::fs::File;
    let mut parent_directory = file_path.clone();
    parent_directory.pop();
    fs::create_dir_all(&parent_directory).map_err(|e| {
        format!(
            "unable to create parent directory {}\n{}",
            parent_directory.display(),
            e
        )
    })?;
    let mut file = File::create(&file_path)
        .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
    file.write_all(content)
        .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
    Ok(())
}

pub fn format_inscription_id(
    transaction_identifier: &TransactionIdentifier,
    inscription_subindex: usize,
) -> String {
    format!(
        "{}i{}",
        transaction_identifier.get_hash_bytes_str(),
        inscription_subindex,
    )
}

pub fn parse_satpoint_to_watch(outpoint_to_watch: &str) -> (TransactionIdentifier, usize, u64) {
    let comps: Vec<&str> = outpoint_to_watch.split(":").collect();
    let tx = TransactionIdentifier::new(comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().expect(&format!(
        "fatal: unable to extract output_index from outpoint {}",
        outpoint_to_watch
    ));
    let offset = comps[2].to_string().parse::<u64>().expect(&format!(
        "fatal: unable to extract offset from outpoint {}",
        outpoint_to_watch
    ));
    (tx, output_index, offset)
}

pub fn format_outpoint_to_watch(
    transaction_identifier: &TransactionIdentifier,
    output_index: usize,
) -> String {
    format!(
        "{}:{}",
        transaction_identifier.get_hash_bytes_str(),
        output_index
    )
}

pub fn parse_inscription_id(inscription_id: &str) -> (TransactionIdentifier, usize) {
    let comps: Vec<&str> = inscription_id.split("i").collect();
    let tx = TransactionIdentifier::new(&comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().expect(&format!(
        "fatal: unable to extract output_index from inscription_id {}",
        inscription_id
    ));
    (tx, output_index)
}

pub fn parse_outpoint_to_watch(outpoint_to_watch: &str) -> (TransactionIdentifier, usize) {
    let comps: Vec<&str> = outpoint_to_watch.split(":").collect();
    let tx = TransactionIdentifier::new(&comps[0]);
    let output_index = comps[1].to_string().parse::<usize>().expect(&format!(
        "fatal: unable to extract output_index from outpoint {}",
        outpoint_to_watch
    ));
    (tx, output_index)
}
