use crate::config::file::ConfigFile;
use crate::config::generator::generate_config;
use clap::{Parser, Subcommand};
use hiro_system_kit;
use ordhook::chainhook_sdk::chainhooks::types::{
    BitcoinChainhookSpecification, HttpHook, InscriptionFeedData, OrdinalsMetaProtocol,
};
use ordhook::chainhook_sdk::chainhooks::types::{
    BitcoinPredicateType, HookAction, OrdinalOperations,
};
use ordhook::chainhook_sdk::utils::BlockHeights;
use ordhook::chainhook_sdk::utils::Context;
use ordhook::config::Config;
use ordhook::core::first_inscription_height;
use ordhook::core::pipeline::bitcoind_download_blocks;
use ordhook::core::pipeline::processors::block_archiving::start_block_archiving_processor;
use ordhook::db::blocks::{
    find_block_bytes_at_block_height, find_last_block_inserted, find_missing_blocks,
    open_blocks_db_with_retry, open_readonly_blocks_db,
};
use ordhook::db::cursor::BlockBytesCursor;
use ordhook::db::migrate_dbs;
use ordhook::service::Service;
use ordhook::try_info;
use std::collections::HashSet;
use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;
use std::{process, u64};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Opts {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum Command {
    /// Generate a new configuration file
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Stream Bitcoin blocks and index ordinals inscriptions and transfers
    #[clap(subcommand)]
    Service(ServiceCommand),
    /// Perform maintenance operations on local databases
    #[clap(subcommand)]
    Db(OrdhookDbCommand),
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum RepairCommand {
    /// Rewrite blocks data in hord.rocksdb
    #[clap(name = "blocks", bin_name = "blocks")]
    Blocks(RepairStorageCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct RepairStorageCommand {
    /// Interval of blocks (--interval 767430:800000)
    #[clap(long = "interval", conflicts_with = "blocks")]
    pub blocks_interval: Option<String>,
    /// List of blocks (--blocks 767430,767431,767433,800000)
    #[clap(long = "blocks", conflicts_with = "interval")]
    pub blocks: Option<String>,
    /// Network threads
    #[clap(long = "network-threads")]
    pub network_threads: Option<usize>,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
    /// Cascade to observers
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub repair_observers: Option<bool>,
    /// Display debug logs
    #[clap(short, long, action = clap::ArgAction::SetTrue)]
    pub debug: Option<bool>,
}

impl RepairStorageCommand {
    pub fn get_blocks(&self) -> Vec<u64> {
        let blocks = match (&self.blocks_interval, &self.blocks) {
            (Some(interval), None) => {
                let blocks = interval.split(':').collect::<Vec<_>>();
                let start_block: u64 = blocks
                    .first()
                    .expect("unable to get start_block")
                    .parse::<u64>()
                    .expect("unable to parse start_block");
                let end_block: u64 = blocks
                    .get(1)
                    .expect("unable to get end_block")
                    .parse::<u64>()
                    .expect("unable to parse end_block");
                BlockHeights::BlockRange(start_block, end_block).get_sorted_entries()
            }
            (None, Some(blocks)) => {
                let blocks = blocks
                    .split(',')
                    .map(|b| b.parse::<u64>().expect("unable to parse block"))
                    .collect::<Vec<_>>();
                BlockHeights::Blocks(blocks).get_sorted_entries()
            }
            _ => unreachable!(),
        };
        blocks.unwrap().into()
    }
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
#[clap(bin_name = "config", aliases = &["config"])]
enum ConfigCommand {
    /// Generate new config
    #[clap(name = "new", bin_name = "new", aliases = &["generate"])]
    New(NewConfig),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct NewConfig {
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum ServiceCommand {
    /// Start chainhook-cli
    #[clap(name = "start", bin_name = "start")]
    Start(StartCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct StartCommand {
    /// Target Regtest network
    #[clap(
        long = "regtest",
        conflicts_with = "testnet",
        conflicts_with = "mainnet"
    )]
    pub regtest: bool,
    /// Target Testnet network
    #[clap(
        long = "testnet",
        conflicts_with = "regtest",
        conflicts_with = "mainnet"
    )]
    pub testnet: bool,
    /// Target Mainnet network
    #[clap(
        long = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub mainnet: bool,
    /// Load config file path
    #[clap(
        long = "config-path",
        conflicts_with = "mainnet",
        conflicts_with = "testnet",
        conflicts_with = "regtest"
    )]
    pub config_path: Option<String>,
    /// Specify relative path of the chainhooks (yaml format) to evaluate
    #[clap(long = "post-to")]
    pub post_to: Vec<String>,
    /// HTTP Auth token
    #[clap(long = "auth-token")]
    pub auth_token: Option<String>,
    /// Check blocks integrity
    #[clap(long = "check-blocks-integrity")]
    pub block_integrity_check: bool,
    /// Stream indexing to observers
    #[clap(long = "stream-indexing")]
    pub stream_indexing_to_observers: bool,
}

#[derive(Subcommand, PartialEq, Clone, Debug)]
enum OrdhookDbCommand {
    /// Initialize a new ordhook db
    #[clap(name = "new", bin_name = "new")]
    New(SyncOrdhookDbCommand),
    /// Catch-up ordhook db
    #[clap(name = "sync", bin_name = "sync")]
    Sync(SyncOrdhookDbCommand),
    /// Rebuild inscriptions entries for a given block
    #[clap(name = "drop", bin_name = "drop")]
    Drop(DropOrdhookDbCommand),
    /// Check integrity
    #[clap(name = "check", bin_name = "check")]
    Check(CheckDbCommand),
    /// Db maintenance related commands
    #[clap(subcommand)]
    Repair(RepairCommand),
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct UpdateOrdhookDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
    /// Transfers only
    pub transfers_only: Option<bool>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct SyncOrdhookDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct DropOrdhookDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct PatchOrdhookDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct MigrateOrdhookDbCommand {
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

#[derive(Parser, PartialEq, Clone, Debug)]
struct CheckDbCommand {
    /// Starting block
    pub start_block: u64,
    /// Ending block
    pub end_block: u64,
    /// Load config file path
    #[clap(long = "config-path")]
    pub config_path: Option<String>,
}

pub fn main() {
    let logger = hiro_system_kit::log::setup_logger();
    let _guard = hiro_system_kit::log::setup_global_logger(logger.clone());
    let ctx = Context {
        logger: Some(logger),
        tracer: false,
    };

    let opts: Opts = match Opts::try_parse() {
        Ok(opts) => opts,
        Err(e) => {
            println!("{}", e);
            process::exit(1);
        }
    };

    if let Err(e) = hiro_system_kit::nestable_block_on(handle_command(opts, &ctx)) {
        error!(ctx.expect_logger(), "{e}");
        std::thread::sleep(std::time::Duration::from_millis(500));
        process::exit(1);
    }
}

async fn handle_command(opts: Opts, ctx: &Context) -> Result<(), String> {
    match opts.command {
        Command::Service(subcmd) => match subcmd {
            ServiceCommand::Start(cmd) => {
                let maintenance_enabled =
                    std::env::var("ORDHOOK_MAINTENANCE").unwrap_or("0".into());
                if maintenance_enabled.eq("1") {
                    try_info!(ctx, "Entering maintenance mode. Unset ORDHOOK_MAINTENANCE and reboot to resume operations");
                    sleep(Duration::from_secs(u64::MAX))
                }

                let config = ConfigFile::default(
                    cmd.regtest,
                    cmd.testnet,
                    cmd.mainnet,
                    &cmd.config_path,
                    &None,
                )?;

                migrate_dbs(&config, ctx).await?;

                // Figure out index start block height.
                let mut service = Service::new(&config, ctx);
                let start_block = service.get_start_block_height().await?;
                try_info!(
                    ctx,
                    "Inscription ingestion will start at block #{start_block}"
                );

                let mut predicates = vec![];
                for post_to in cmd.post_to.iter() {
                    let predicate = build_predicate_from_cli(
                        &config,
                        post_to,
                        None,
                        Some(start_block),
                        cmd.auth_token.clone(),
                        true,
                    )?;
                    predicates.push(predicate);
                }

                return service
                    .run(
                        predicates,
                        None,
                        cmd.block_integrity_check,
                        cmd.stream_indexing_to_observers,
                    )
                    .await;
            }
        },
        Command::Config(subcmd) => match subcmd {
            ConfigCommand::New(cmd) => {
                use std::fs::File;
                use std::io::Write;
                let config =
                    ConfigFile::default(cmd.regtest, cmd.testnet, cmd.mainnet, &None, &None)?;
                let config_content = generate_config(&config.network.bitcoin_network);
                let mut file_path = PathBuf::new();
                file_path.push("Ordhook.toml");
                let mut file = File::create(&file_path)
                    .map_err(|e| format!("unable to open file {}\n{}", file_path.display(), e))?;
                file.write_all(config_content.as_bytes())
                    .map_err(|e| format!("unable to write file {}\n{}", file_path.display(), e))?;
                println!("Created file Ordhook.toml");
            }
        },
        Command::Db(OrdhookDbCommand::New(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path, &None)?;
            migrate_dbs(&config, ctx).await?;
            open_blocks_db_with_retry(true, &config, ctx);
        }
        Command::Db(OrdhookDbCommand::Sync(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path, &None)?;
            migrate_dbs(&config, ctx).await?;
            let service = Service::new(&config, ctx);
            service.catch_up_to_bitcoin_chain_tip(None).await?;
        }
        Command::Db(OrdhookDbCommand::Repair(subcmd)) => match subcmd {
            RepairCommand::Blocks(cmd) => {
                let mut config = ConfigFile::default(false, false, false, &cmd.config_path, &None)?;
                if let Some(network_threads) = cmd.network_threads {
                    config.resources.bitcoind_rpc_threads = network_threads;
                }
                let blocks = cmd.get_blocks();
                let block_ingestion_processor =
                    start_block_archiving_processor(&config, ctx, false, None);
                bitcoind_download_blocks(
                    &config,
                    blocks,
                    first_inscription_height(&config),
                    &block_ingestion_processor,
                    10_000,
                    ctx,
                )
                .await?;
                if let Some(true) = cmd.debug {
                    let blocks_db = open_blocks_db_with_retry(false, &config, ctx);
                    for i in cmd.get_blocks().into_iter() {
                        let block_bytes =
                            find_block_bytes_at_block_height(i as u32, 10, &blocks_db, ctx)
                                .expect("unable to retrieve block {i}");
                        let block = BlockBytesCursor::new(&block_bytes);
                        info!(ctx.expect_logger(), "--------------------");
                        info!(ctx.expect_logger(), "Block: {i}");
                        for tx in block.iter_tx() {
                            info!(ctx.expect_logger(), "Tx: {}", ordhook::hex::encode(tx.txid));
                        }
                    }
                }
            }
        },
        Command::Db(OrdhookDbCommand::Check(cmd)) => {
            let config = ConfigFile::default(false, false, false, &cmd.config_path, &None)?;
            {
                let blocks_db = open_readonly_blocks_db(&config, ctx)?;
                let tip = find_last_block_inserted(&blocks_db);
                println!("Tip: {}", tip);
                let missing_blocks = find_missing_blocks(&blocks_db, 1, tip, ctx);
                println!("{:?}", missing_blocks);
            }
        }
        Command::Db(OrdhookDbCommand::Drop(_cmd)) => {
            // let config = ConfigFile::default(false, false, false, &cmd.config_path, &None)?;

            // println!(
            //     "{} blocks will be deleted. Confirm? [Y/n]",
            //     cmd.end_block - cmd.start_block + 1
            // );
            // FIXME
            // let mut buffer = String::new();
            // std::io::stdin().read_line(&mut buffer).unwrap();
            // if buffer.starts_with('n') {
            //     return Err("Deletion aborted".to_string());
            // }

            // let (blocks_db_rw, sqlite_dbs_rw) = open_all_dbs_rw(&config, &ctx)?;

            // drop_block_data_from_all_dbs(
            //     cmd.start_block,
            //     cmd.end_block,
            //     &blocks_db_rw,
            //     &sqlite_dbs_rw,
            //     ctx,
            // )?;
            // info!(
            //     ctx.expect_logger(),
            //     "Cleaning ordhook_db: {} blocks dropped",
            //     cmd.end_block - cmd.start_block + 1
            // );
        }
    }
    Ok(())
}

pub fn build_predicate_from_cli(
    config: &Config,
    post_to: &str,
    block_heights: Option<&BlockHeights>,
    start_block: Option<u64>,
    auth_token: Option<String>,
    is_streaming: bool,
) -> Result<BitcoinChainhookSpecification, String> {
    // Retrieve last block height known, and display it
    let (start_block, end_block, blocks) = match (start_block, block_heights) {
        (None, Some(BlockHeights::BlockRange(start, end))) => (Some(*start), Some(*end), None),
        (None, Some(BlockHeights::Blocks(blocks))) => (None, None, Some(blocks.clone())),
        (Some(start), None) => (Some(start), None, None),
        _ => unreachable!(),
    };
    let mut meta_protocols: Option<HashSet<OrdinalsMetaProtocol>> = None;
    if config.meta_protocols.brc20 {
        let mut meta = HashSet::<OrdinalsMetaProtocol>::new();
        meta.insert(OrdinalsMetaProtocol::All);
        meta_protocols = Some(meta.clone());
    }
    let predicate = BitcoinChainhookSpecification {
        network: config.network.bitcoin_network.clone(),
        uuid: post_to.to_string(),
        owner_uuid: None,
        name: post_to.to_string(),
        version: 1,
        start_block,
        end_block,
        blocks,
        expire_after_occurrence: None,
        include_proof: false,
        include_inputs: false,
        include_outputs: false,
        include_witness: false,
        expired_at: None,
        enabled: is_streaming,
        predicate: BitcoinPredicateType::OrdinalsProtocol(OrdinalOperations::InscriptionFeed(
            InscriptionFeedData { meta_protocols },
        )),
        action: HookAction::HttpPost(HttpHook {
            url: post_to.to_string(),
            authorization_header: format!("Bearer {}", auth_token.unwrap_or("".to_string())),
        }),
    };

    Ok(predicate)
}
