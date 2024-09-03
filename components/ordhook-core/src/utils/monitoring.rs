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
        total_predicates: u64,
        max_inscription_number: Option<u64>,
        initial_block: Option<u64>,
    ) {
        self.metrics_set_registered_predicates(total_predicates);
        if let Some(initial_block) = initial_block {
            self.metrics_block_indexed(initial_block);
        }
        if let Some(inscription_number) = max_inscription_number {
            self.metrics_inscription_indexed(inscription_number);
        }
    }

    pub fn metrics_deregister_predicate(&self) {
        self.registered_predicates.dec();
    }

    pub fn metrics_register_predicate(&self) {
        self.registered_predicates.inc();
    }

    pub fn metrics_set_registered_predicates(&self, registered_predicates: u64) {
        self.registered_predicates.set(registered_predicates);
    }

    pub fn metrics_inscription_indexed(&self, inscription_number: u64) {
        let highest_appended = self.last_indexed_inscription_number.get();
        if inscription_number > highest_appended {
            self.last_indexed_inscription_number.set(inscription_number);
        }
    }

    pub fn metrics_block_indexed(&self, block_height: u64) {
        let highest_appended = self.last_indexed_block_height.get();
        if block_height > highest_appended {
            self.last_indexed_block_height.set(block_height);
        }
    }
}
