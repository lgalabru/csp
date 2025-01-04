use ordhook::chainhook_sdk::observer::DEFAULT_INGESTION_PORT;
use ordhook::chainhook_sdk::types::{
    BitcoinBlockSignaling, BitcoinNetwork, StacksNetwork, StacksNodeConfig,
};
use ordhook::config::{
    Config, IndexerConfig, LogConfig, MetaProtocolsConfig, ResourcesConfig, SnapshotConfig,
    SnapshotConfigDownloadUrls, StorageConfig, DEFAULT_BITCOIND_RPC_THREADS,
    DEFAULT_BITCOIND_RPC_TIMEOUT, DEFAULT_BRC20_LRU_CACHE_SIZE, DEFAULT_MEMORY_AVAILABLE,
    DEFAULT_ULIMIT,
};
use std::fs::File;
use std::io::{BufReader, Read};

#[derive(Deserialize, Debug, Clone)]
pub struct ConfigFile {
    pub storage: StorageConfigFile,
    pub ordinals_db: PostgresConfigFile,
    pub brc20_db: Option<PostgresConfigFile>,
    pub http_api: Option<PredicatesApiConfigFile>,
    pub resources: ResourcesConfigFile,
    pub network: NetworkConfigFile,
    pub logs: Option<LogConfigFile>,
    pub snapshot: Option<SnapshotConfigFile>,
    pub meta_protocols: Option<MetaProtocolsConfigFile>,
}

impl ConfigFile {
    pub fn from_file_path(file_path: &str) -> Result<Config, String> {
        let file = File::open(file_path)
            .map_err(|e| format!("unable to read file {}\n{:?}", file_path, e))?;
        let mut file_reader = BufReader::new(file);
        let mut file_buffer = vec![];
        file_reader
            .read_to_end(&mut file_buffer)
            .map_err(|e| format!("unable to read file {}\n{:?}", file_path, e))?;

        let config_file: ConfigFile = match toml::from_slice(&file_buffer) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!("Config file malformatted {}", e));
            }
        };
        ConfigFile::from_config_file(config_file)
    }

    pub fn from_config_file(config_file: ConfigFile) -> Result<Config, String> {
        let (_, bitcoin_network) = match config_file.network.mode.as_str() {
            "devnet" => (StacksNetwork::Devnet, BitcoinNetwork::Regtest),
            "testnet" => (StacksNetwork::Testnet, BitcoinNetwork::Testnet),
            "mainnet" => (StacksNetwork::Mainnet, BitcoinNetwork::Mainnet),
            "signet" => (StacksNetwork::Testnet, BitcoinNetwork::Signet),
            _ => return Err("network.mode not supported".to_string()),
        };

        let snapshot = match config_file.snapshot {
            Some(bootstrap) => match bootstrap.ordinals_url {
                Some(ref url) => SnapshotConfig::Download(SnapshotConfigDownloadUrls {
                    ordinals: url.to_string(),
                    brc20: bootstrap.brc20_url,
                }),
                None => SnapshotConfig::Build,
            },
            None => SnapshotConfig::Build,
        };

        let config = Config {
            storage: StorageConfig {
                working_dir: config_file.storage.working_dir.unwrap_or("ordhook".into()),
                observers_working_dir: config_file
                    .storage
                    .observers_working_dir
                    .unwrap_or("observers".into()),
            },
            ordinals_db: ordhook::config::PgConnectionConfig {
                dbname: config_file.ordinals_db.database,
                host: config_file.ordinals_db.host,
                port: config_file.ordinals_db.port,
                user: config_file.ordinals_db.username,
                password: config_file.ordinals_db.password,
                search_path: config_file.ordinals_db.search_path,
                pool_max_size: config_file.ordinals_db.pool_max_size,
            },
            brc20_db: match config_file.brc20_db {
                Some(brc20_db) => Some(ordhook::config::PgConnectionConfig {
                    dbname: brc20_db.database,
                    host: brc20_db.host,
                    port: brc20_db.port,
                    user: brc20_db.username,
                    password: brc20_db.password,
                    search_path: brc20_db.search_path,
                    pool_max_size: brc20_db.pool_max_size,
                }),
                None => None,
            },
            snapshot,
            resources: ResourcesConfig {
                ulimit: config_file.resources.ulimit.unwrap_or(DEFAULT_ULIMIT),
                cpu_core_available: config_file
                    .resources
                    .cpu_core_available
                    .unwrap_or(num_cpus::get()),
                memory_available: config_file
                    .resources
                    .memory_available
                    .unwrap_or(DEFAULT_MEMORY_AVAILABLE),
                bitcoind_rpc_threads: config_file
                    .resources
                    .bitcoind_rpc_threads
                    .unwrap_or(DEFAULT_BITCOIND_RPC_THREADS),
                bitcoind_rpc_timeout: config_file
                    .resources
                    .bitcoind_rpc_timeout
                    .unwrap_or(DEFAULT_BITCOIND_RPC_TIMEOUT),
                expected_observers_count: config_file
                    .resources
                    .expected_observers_count
                    .unwrap_or(1),
                brc20_lru_cache_size: config_file
                    .resources
                    .brc20_lru_cache_size
                    .unwrap_or(DEFAULT_BRC20_LRU_CACHE_SIZE),
            },
            network: IndexerConfig {
                bitcoind_rpc_url: config_file.network.bitcoind_rpc_url.to_string(),
                bitcoind_rpc_username: config_file.network.bitcoind_rpc_username.to_string(),
                bitcoind_rpc_password: config_file.network.bitcoind_rpc_password.to_string(),
                bitcoin_block_signaling: match config_file.network.bitcoind_zmq_url {
                    Some(ref zmq_url) => BitcoinBlockSignaling::ZeroMQ(zmq_url.clone()),
                    None => BitcoinBlockSignaling::Stacks(StacksNodeConfig::default_localhost(
                        DEFAULT_INGESTION_PORT,
                    )),
                },
                bitcoin_network,
                prometheus_monitoring_port: config_file.network.prometheus_monitoring_port,
            },
            logs: LogConfig {
                ordinals_internals: config_file
                    .logs
                    .as_ref()
                    .and_then(|l| l.ordinals_internals)
                    .unwrap_or(true),
                chainhook_internals: config_file
                    .logs
                    .as_ref()
                    .and_then(|l| l.chainhook_internals)
                    .unwrap_or(true),
            },
            meta_protocols: MetaProtocolsConfig {
                brc20: config_file
                    .meta_protocols
                    .as_ref()
                    .and_then(|l| l.brc20)
                    .unwrap_or(false),
            },
        };
        Ok(config)
    }

    pub fn default(
        devnet: bool,
        testnet: bool,
        mainnet: bool,
        config_path: &Option<String>,
        meta_protocols: &Option<String>,
    ) -> Result<Config, String> {
        let mut config = match (devnet, testnet, mainnet, config_path) {
            (true, false, false, _) => Config::devnet_default(),
            (false, true, false, _) => Config::testnet_default(),
            (false, false, true, _) => Config::mainnet_default(),
            (false, false, false, Some(config_path)) => ConfigFile::from_file_path(config_path)?,
            _ => Err("Invalid combination of arguments".to_string())?,
        };
        if let Some(meta_protocols) = meta_protocols {
            match meta_protocols.as_str() {
                "brc20" => config.meta_protocols.brc20 = true,
                _ => Err("Invalid meta protocol".to_string())?,
            }
        }
        Ok(config)
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct LogConfigFile {
    pub ordinals_internals: Option<bool>,
    pub chainhook_internals: Option<bool>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PostgresConfigFile {
    pub database: String,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    pub search_path: Option<String>,
    pub pool_max_size: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct StorageConfigFile {
    pub working_dir: Option<String>,
    pub observers_working_dir: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct PredicatesApiConfigFile {
    pub http_port: Option<u16>,
    pub database_uri: Option<String>,
    pub display_logs: Option<bool>,
    pub disabled: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct SnapshotConfigFile {
    pub ordinals_url: Option<String>,
    pub brc20_url: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct MetaProtocolsConfigFile {
    pub brc20: Option<bool>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ResourcesConfigFile {
    pub ulimit: Option<usize>,
    pub cpu_core_available: Option<usize>,
    pub memory_available: Option<usize>,
    pub bitcoind_rpc_threads: Option<usize>,
    pub bitcoind_rpc_timeout: Option<u32>,
    pub expected_observers_count: Option<usize>,
    pub brc20_lru_cache_size: Option<usize>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct NetworkConfigFile {
    pub mode: String,
    pub bitcoind_rpc_url: String,
    pub bitcoind_rpc_username: String,
    pub bitcoind_rpc_password: String,
    pub bitcoind_zmq_url: Option<String>,
    pub prometheus_monitoring_port: Option<u16>,
}
