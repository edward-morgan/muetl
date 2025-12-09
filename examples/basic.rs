use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use kameo::actor::{ActorRef, Spawn};
use kameo_actors::pubsub::PubSub;
use muetl::{
    flow::{Flow, RawFlow},
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

impl SinkInput<i64> for LogSink {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, _ctx: &MuetlSinkContext, input: &i64) {
        tracing::info!(value = %input, "LogSink received");
    }
}

impl TaskDef for LogSink {}

impl ConfigTemplate for LogSink {}

impl_sink_handler!(LogSink, task_id = "log_sink", "input" => i64);

pub struct Ticker {
    t: i64,
    period: Duration,
    iterations: i64,
}
impl Ticker {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        Ok(Box::new(Ticker {
            t: 0,
            period: Duration::from_millis(config.require_i64("period_ms") as u64),
            iterations: config.require_i64("iterations"),
        }))
    }
}
impl TaskDef for Ticker {
    fn deinit(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl Output<i64> for Ticker {
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

impl_source_handler!(Ticker, task_id = "ticker", "tick" => i64);
impl_config_template!(
    Ticker,
    period_ms: Num = 1000,
    iterations: Num = 10,
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

    println!("Creating raw flow from JSON...");
    // Create a RawFlow representing the processing graph we want
    let flow_json = r#"
    {
        "nodes": [
            {
                "node_id": "node1",
                "task_id": "ticker",
                "configuration": {
                    "period_ms": 500,
                    "iterations": 10
                }
            },
            {
                "node_id": "node2",
                "task_id": "ticker",
                "configuration": {
                    "period_ms": 500,
                    "iterations": 10
                }
            },
            {
                "node_id": "node3",
                "task_id": "log_sink",
                "configuration": {}
            }
        ],
        "edges": [
            {
                "from": {
                    "node_id": "node1",
                    "conn_name": "tick"
                },
                "to": {
                    "node_id": "node3",
                    "conn_name": "input"
                }
            },
            {
                "from": {
                    "node_id": "node2",
                    "conn_name": "tick"
                },
                "to": {
                    "node_id": "node3",
                    "conn_name": "input"
                }
            }
        ]
    }
    "#;

    let raw_flow: RawFlow = serde_json::from_str(flow_json).expect("Failed to parse flow JSON");

    // Initialize a Root actor with the flow
    println!("Validating raw flow...");
    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = Spawn::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    println!("Starting root...");

    // Create root with file logging enabled - logs will be written to ./logs directory
    let root = Root::new(flow, monitor_chan).with_file_logging("./logs")?;

    let root_ref: ActorRef<Root> = Spawn::spawn(root);
    root_ref.wait_for_shutdown().await;

    println!("Flow complete! Check ./logs for per-task log files.");
    Ok(())
}
