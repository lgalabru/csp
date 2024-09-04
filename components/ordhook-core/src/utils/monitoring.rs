use chainhook_sdk::utils::Context;
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use prometheus::{
    core::{AtomicU64, GenericGauge},
    Encoder, Registry, TextEncoder,
};

use crate::{try_debug, try_info, try_warn};

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
        let last_indexed_inscription_number =
            PrometheusMonitoring::create_and_register_uint64_gauge(
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
        max_inscription_number: u64,
        block_height: u64,
    ) {
        self.metrics_set_registered_predicates(total_predicates);
        self.metrics_block_indexed(block_height);
        self.metrics_inscription_indexed(max_inscription_number);
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

async fn serve_req(
    req: Request<Body>,
    registry: Registry,
    ctx: Context,
) -> Result<Response<Body>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            try_debug!(ctx, "Prometheus monitoring: responding to metrics request");

            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            let mut buffer = vec![];
            let response = match encoder.encode(&metric_families, &mut buffer) {
                Ok(_) => Response::builder()
                    .status(200)
                    .header(CONTENT_TYPE, encoder.format_type())
                    .body(Body::from(buffer))
                    .unwrap(),
                Err(e) => {
                    try_debug!(
                        ctx,
                        "Prometheus monitoring: failed to encode metrics: {}",
                        e.to_string()
                    );
                    Response::builder().status(500).body(Body::empty()).unwrap()
                }
            };
            Ok(response)
        }
        (_, _) => {
            try_debug!(
                ctx,
                "Prometheus monitoring: received request with invalid method/route: {}/{}",
                req.method(),
                req.uri().path()
            );
            let response = Response::builder().status(404).body(Body::empty()).unwrap();

            Ok(response)
        }
    }
}

pub async fn start_serving_prometheus_metrics(port: u16, registry: Registry, ctx: Context) {
    let addr = ([0, 0, 0, 0], port).into();
    let ctx_clone = ctx.clone();
    let make_svc = make_service_fn(|_| {
        let registry = registry.clone();
        let ctx_clone = ctx_clone.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |r| {
                serve_req(r, registry.clone(), ctx_clone.clone())
            }))
        }
    });
    let serve_future = Server::bind(&addr).serve(make_svc);
    try_info!(ctx, "Prometheus monitoring: listening on port {}", port);
    if let Err(err) = serve_future.await {
        try_warn!(ctx, "Prometheus monitoring: server error: {}", err);
    }
}
