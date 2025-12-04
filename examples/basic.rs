use std::{any::TypeId, collections::HashMap, sync::Arc};

use kameo::Actor;
use kameo_actors::pubsub::PubSub;
use muetl::{
    daemons::ticker::Ticker,
    flow::{Edge, Flow, Node, NodeRef, RawFlow},
    registry::{Registry, TaskDefInfo, TaskInfo},
    runtime::root::Root,
    sinks::log_sink::LogSink,
    system::*,
    task_defs::{ConfigField, TaskConfigTpl, TaskConfigValue},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Initializing registry...");
    // Initialize the registry with the task definitions we're using
    let mut registry = Registry::new();
    let mut log_sink_inputs = HashMap::new();
    log_sink_inputs.insert(
        "input".to_string(),
        vec![TypeId::of::<String>(), TypeId::of::<u64>()],
    );
    registry.add_def(TaskInfo {
        task_id: "log_sink".to_string(),
        config_tpl: None,
        info: TaskDefInfo::SinkDef {
            inputs: log_sink_inputs,
            build_sink: LogSink::new,
        },
    });

    let mut ticker_outputs = HashMap::new();
    ticker_outputs.insert("tick".to_string(), vec![TypeId::of::<u64>()]);
    registry.add_def(TaskInfo {
        task_id: "ticker".to_string(),
        config_tpl: Some(TaskConfigTpl {
            fields: vec![
                ConfigField::optional_with_default("period_ms", TaskConfigValue::Uint(1000)),
                ConfigField::optional_with_default("iterations", TaskConfigValue::Uint(10)),
            ],
            disallow_unknown_fields: true,
        }),
        info: TaskDefInfo::DaemonDef {
            outputs: ticker_outputs,
            build_daemon: Ticker::new,
        },
    });

    println!("Creating raw flow...");
    let mut cfg1 = HashMap::<String, TaskConfigValue>::new();
    cfg1.insert("period_ms".to_string(), TaskConfigValue::Uint(500));
    cfg1.insert("iterations".to_string(), TaskConfigValue::Uint(10));

    let mut cfg2 = HashMap::<String, TaskConfigValue>::new();
    cfg2.insert("period_ms".to_string(), TaskConfigValue::Uint(500));
    cfg2.insert("iterations".to_string(), TaskConfigValue::Uint(10));

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
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    Ok(())
}
