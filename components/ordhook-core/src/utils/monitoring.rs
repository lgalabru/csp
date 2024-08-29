use prometheus::{core::{AtomicU64, GenericGauge}, Registry};

type UInt64Gauge = GenericGauge<AtomicU64>;

#[derive(Debug, Clone)]
pub struct PrometheusMonitoring {
    pub last_indexed_block_height: UInt64Gauge,
    pub last_indexed_inscription_number: UInt64Gauge,
    pub registered_predicates: UInt64Gauge,
    pub registry: Registry,
}

impl PrometheusMonitoring {
    pub fn new() -> PrometheusMonitoring {
        let registry = Registry::new();
        let last_indexed_block_height = PrometheusMonitoring::create_and_register_uint64_gauge(
            &registry,
            "last_indexed_block_height",
            "The latest Bitcoin block indexed for ordinals.",
        );
        let last_indexed_inscription_number = PrometheusMonitoring::create_and_register_uint64_gauge(
            &registry,
            "last_indexed_inscription_number",
            "The latest indexed inscription number.",
        );
        let registered_predicates = PrometheusMonitoring::create_and_register_uint64_gauge(
            &registry,
            "registered_predicates",
            "The current number of predicates registered to receive ordinal events.",
        );
        PrometheusMonitoring {
            last_indexed_block_height,
            last_indexed_inscription_number,
            registered_predicates,
            registry,
        }
    }
    // setup helpers
    pub fn create_and_register_uint64_gauge(
        registry: &Registry,
        name: &str,
        help: &str,
    ) -> UInt64Gauge {
        let g = UInt64Gauge::new(name, help).unwrap();
        registry.register(Box::new(g.clone())).unwrap();
        g
    }

    pub fn initialize(
        &self,
        stx_predicates: u64,
        btc_predicates: u64,
        initial_stx_block: Option<u64>,
    ) {
        self.stx_metrics_set_registered_predicates(stx_predicates);
        self.btc_metrics_set_registered_predicates(btc_predicates);
        if let Some(initial_stx_block) = initial_stx_block {
            self.stx_metrics_block_received(initial_stx_block);
            self.stx_metrics_block_appeneded(initial_stx_block);
            self.stx_metrics_block_evaluated(initial_stx_block);
        }
    }

    // stx helpers
    pub fn stx_metrics_deregister_predicate(&self) {
        self.stx_registered_predicates.dec();
        self.stx_deregistered_predicates.inc();
    }

    pub fn stx_metrics_register_predicate(&self) {
        self.stx_registered_predicates.inc();
    }
    pub fn stx_metrics_set_registered_predicates(&self, registered_predicates: u64) {
        self.stx_registered_predicates.set(registered_predicates);
    }

    pub fn stx_metrics_set_reorg(
        &self,
        timestamp: i64,
        applied_blocks: u64,
        rolled_back_blocks: u64,
    ) {
        self.stx_last_reorg_timestamp.set(timestamp);
        self.stx_last_reorg_applied_blocks.set(applied_blocks);
        self.stx_last_reorg_rolled_back_blocks
            .set(rolled_back_blocks);
    }

    pub fn stx_metrics_block_appeneded(&self, new_block_height: u64) {
        let highest_appended = self.stx_highest_block_appended.get();
        if new_block_height > highest_appended {
            self.stx_highest_block_appended.set(new_block_height);

            let highest_received = self.stx_highest_block_received.get();
            self.stx_canonical_fork_lag
                .set(highest_received.saturating_sub(new_block_height));

            let highest_evaluated = self.stx_highest_block_evaluated.get();
            self.stx_block_evaluation_lag
                .set(new_block_height.saturating_sub(highest_evaluated));
        }
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Could not get current time in ms")
            .as_secs() as u64;
        self.stx_last_block_ingestion_time.set(time);
    }

    pub fn stx_metrics_block_received(&self, new_block_height: u64) {
        let highest_received = self.stx_highest_block_received.get();
        if new_block_height > highest_received {
            self.stx_highest_block_received.set(new_block_height);

            let highest_appended = self.stx_highest_block_appended.get();
            self.stx_canonical_fork_lag
                .set(new_block_height.saturating_sub(highest_appended));
        }
    }

    pub fn stx_metrics_block_evaluated(&self, new_block_height: u64) {
        let highest_evaluated = self.stx_highest_block_evaluated.get();
        if new_block_height > highest_evaluated {
            self.stx_highest_block_evaluated.set(new_block_height);

            let highest_appended = self.stx_highest_block_appended.get();
            self.stx_block_evaluation_lag
                .set(highest_appended.saturating_sub(new_block_height));
        }
    }

    // btc helpers
    pub fn btc_metrics_deregister_predicate(&self) {
        self.btc_registered_predicates.dec();
        self.btc_deregistered_predicates.inc();
    }

    pub fn btc_metrics_register_predicate(&self) {
        self.btc_registered_predicates.inc();
    }

    pub fn btc_metrics_set_registered_predicates(&self, registered_predicates: u64) {
        self.btc_registered_predicates.set(registered_predicates);
    }

    pub fn btc_metrics_set_reorg(
        &self,
        timestamp: i64,
        applied_blocks: u64,
        rolled_back_blocks: u64,
    ) {
        self.btc_last_reorg_timestamp.set(timestamp);
        self.btc_last_reorg_applied_blocks.set(applied_blocks);
        self.btc_last_reorg_rolled_back_blocks
            .set(rolled_back_blocks);
    }

    pub fn btc_metrics_block_appended(&self, new_block_height: u64) {
        let highest_appended = self.btc_highest_block_appended.get();
        if new_block_height > highest_appended {
            self.btc_highest_block_appended.set(new_block_height);

            let highest_received = self.btc_highest_block_received.get();
            self.btc_canonical_fork_lag
                .set(highest_received.saturating_sub(new_block_height));

            let highest_evaluated = self.btc_highest_block_evaluated.get();
            self.btc_block_evaluation_lag
                .set(new_block_height.saturating_sub(highest_evaluated));
        }
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Could not get current time in ms")
            .as_secs() as u64;
        self.btc_last_block_ingestion_time.set(time);
    }

    pub fn btc_metrics_block_received(&self, new_block_height: u64) {
        let highest_received = self.btc_highest_block_received.get();
        if new_block_height > highest_received {
            self.btc_highest_block_received.set(new_block_height);

            let highest_appended = self.btc_highest_block_appended.get();

            self.btc_canonical_fork_lag
                .set(new_block_height.saturating_sub(highest_appended));
        }
    }

    pub fn btc_metrics_block_evaluated(&self, new_block_height: u64) {
        let highest_evaluated = self.btc_highest_block_evaluated.get();
        if new_block_height > highest_evaluated {
            self.btc_highest_block_evaluated.set(new_block_height);

            let highest_appended = self.btc_highest_block_appended.get();
            self.btc_block_evaluation_lag
                .set(highest_appended.saturating_sub(new_block_height));
        }
    }

    pub fn get_metrics(&self) -> JsonValue {
        json!({
            "bitcoin": {
                "last_received_block_height": self.btc_highest_block_received.get(),
                "last_appended_block_height": self.btc_highest_block_appended.get(),
                "last_evaluated_block_height": self.btc_highest_block_evaluated.get(),
                "canonical_fork_lag": self.btc_canonical_fork_lag.get(),
                "block_evaluation_lag": self.btc_block_evaluation_lag.get(),
                "last_block_ingestion_at": self.btc_last_block_ingestion_time.get(),
                "last_reorg": {
                    "timestamp": self.btc_last_reorg_timestamp.get(),
                    "applied_blocks": self.btc_last_reorg_applied_blocks.get(),
                    "rolled_back_blocks": self.btc_last_reorg_rolled_back_blocks.get(),
                },
                "registered_predicates": self.btc_registered_predicates.get(),
                "deregistered_predicates": self.btc_deregistered_predicates.get(),
            },
            "stacks": {
                "last_received_block_height": self.stx_highest_block_received.get(),
                "last_appended_block_height": self.stx_highest_block_appended.get(),
                "last_evaluated_block_height": self.stx_highest_block_evaluated.get(),
                "canonical_fork_lag": self.stx_canonical_fork_lag.get(),
                "block_evaluation_lag": self.btc_block_evaluation_lag.get(),
                "last_block_ingestion_at": self.stx_last_block_ingestion_time.get(),
                "last_reorg": {
                    "timestamp": self.stx_last_reorg_timestamp.get(),
                    "applied_blocks": self.stx_last_reorg_applied_blocks.get(),
                    "rolled_back_blocks": self.stx_last_reorg_rolled_back_blocks.get(),
                },
                "registered_predicates": self.stx_registered_predicates.get(),
                "deregistered_predicates": self.stx_deregistered_predicates.get(),
            }
        })
    }
}
