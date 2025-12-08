use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use kameo::Actor;
use kameo_actors::pubsub::PubSub;
use muetl::{
    flow::{Edge, Flow, Node, NodeRef, RawFlow},
    impl_config_template, impl_sink_handler, impl_source_handler, logging,
    messages::event::Event,
    registry::Registry,
    runtime::root::Root,
    system::*,
    task_defs::{sink::Sink, source::Source, *},
};
use tokio::time::sleep;

pub struct LogSink {}

impl LogSink {
    pub fn new(_config: &TaskConfig) -> Result<Box<dyn Sink>, String> {
        Ok(Box::new(LogSink {}))
    }
}

impl SinkInput<u64> for LogSink {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, _ctx: &MuetlSinkContext, input: &u64) {
        tracing::info!(value = %input, "LogSink received");
    }
}

impl TaskDef for LogSink {}

impl ConfigTemplate for LogSink {}

impl_sink_handler!(LogSink, task_id = "log_sink", "input" => u64);

pub struct Ticker {
    t: u64,
    period: Duration,
    iterations: u64,
}
impl Ticker {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        Ok(Box::new(Ticker {
            t: 0,
            period: Duration::from_millis(config.require_u64("period_ms")),
            iterations: config.require_u64("iterations"),
        }))
    }
}
impl TaskDef for Ticker {
    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl Output<u64> for Ticker {
    const conn_name: &'static str = "tick";
}

#[async_trait]
impl Source for Ticker {
    async fn run(&mut self, ctx: &crate::task_defs::MuetlContext) -> () {
        if self.t == self.iterations {
            tracing::debug!(iterations = self.t, "Ticker reached max iterations");
            ctx.status
                .send(crate::messages::Status::Finished)
                .await
                .unwrap();
        } else {
            ctx.results
                .send(Event::new(
                    format!("tick-{}", self.t),
                    "tick".to_string(),
                    HashMap::new(),
                    Arc::new(self.t),
                ))
                .await
                .unwrap();
            self.t += 1;
            sleep(self.period).await;
        }
    }
}

impl_source_handler!(Ticker, task_id = "ticker", "tick" => u64);
impl_config_template!(
    Ticker,
    period_ms: Uint = 1000,
    iterations: Uint = 10,
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the logging system
    logging::init();

    println!("Initializing registry...");
    // Initialize the registry with the task definitions we're using
    let mut registry = Registry::new();
    registry.register::<LogSink>();
    registry.register::<Ticker>();

    println!("Creating raw flow...");
    let mut cfg1 = HashMap::<String, ConfigValue>::new();
    cfg1.insert("period_ms".to_string(), ConfigValue::Uint(500));
    cfg1.insert("iterations".to_string(), ConfigValue::Uint(10));

    let mut cfg2 = HashMap::<String, ConfigValue>::new();
    cfg2.insert("period_ms".to_string(), ConfigValue::Uint(500));
    cfg2.insert("iterations".to_string(), ConfigValue::Uint(10));

    // Create a RawFlow representing the processing graph we want
    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "node1".to_string(),
                task_id: "ticker".to_string(),
                configuration: cfg1,
                info: None,
            },
            Node {
                node_id: "node2".to_string(),
                task_id: "ticker".to_string(),
                configuration: cfg2,
                info: None,
            },
            Node {
                node_id: "node3".to_string(),
                task_id: "log_sink".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("node1".to_string(), "tick".to_string()),
                to: NodeRef::new("node3".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("node2".to_string(), "tick".to_string()),
                to: NodeRef::new("node3".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    // Initialize a Root actor with the flow
    println!("Validating raw flow...");
    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    println!("Starting root...");

    // Create root with file logging enabled - logs will be written to ./logs directory
    let root = Root::new(flow, monitor_chan).with_file_logging("./logs")?;

    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    println!("Flow complete! Check ./logs for per-task log files.");
    Ok(())
}
