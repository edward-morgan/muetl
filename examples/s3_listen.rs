//! Example: S3 bucket listener that logs CSV file notifications.
//!
//! This example demonstrates using S3ListSource to monitor an S3 bucket
//! for new CSV files and log them using LogSink.
//!
//! The S3ListSource passes metadata (bucket, key, size, etag, last_modified)
//! as event headers, which LogSink will display.

use std::{any::TypeId, collections::HashMap, env, sync::Arc};

use kameo::{
    actor::{ActorRef, Spawn},
    Actor,
};
use kameo_actors::pubsub::PubSub;
use muetl::{
    flow::{Edge, Flow, Node, NodeRef, RawFlow},
    logging,
    registry::{Registry, TaskDefInfo, TaskInfo},
    runtime::root::Root,
    task_defs::{ConfigField, ConfigType, ConfigValue, TaskConfigTpl},
};
use muetl_tasks::{operators::Filter, sinks::LogSink, sources::S3ListSource};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the logging system
    logging::init();

    println!("Setting up S3 bucket listener for CSV files...");

    // Initialize the registry with task definitions
    let mut registry = Registry::new();

    // Register S3ListSource
    let mut s3_outputs = HashMap::new();
    s3_outputs.insert("object".to_string(), vec![TypeId::of::<()>()]);
    registry.add_def(TaskInfo {
        task_id: "s3_list_source".to_string(),
        config_tpl: Some(TaskConfigTpl {
            fields: vec![
                ConfigField::required("bucket", ConfigType::Str),
                ConfigField::optional("prefix", ConfigType::Str),
                ConfigField::optional("region", ConfigType::Str),
                ConfigField::optional("extensions", ConfigType::Arr(Box::new(ConfigType::Str))),
                ConfigField::optional("min_size", ConfigType::Int),
                ConfigField::optional("max_size", ConfigType::Int),
                ConfigField::optional("key_pattern", ConfigType::Str),
                ConfigField::optional("endpoint", ConfigType::Str),
                ConfigField::optional("exclude_pattern", ConfigType::Str),
                ConfigField::optional("access_key_id", ConfigType::Str),
                ConfigField::optional("secret_access_key", ConfigType::Str),
            ],
            disallow_unknown_fields: true,
        }),
        info: TaskDefInfo::SourceDef {
            outputs: s3_outputs,
            build_source: S3ListSource::new,
        },
    });

    // Register Filter
    let mut filter_inputs = HashMap::new();
    filter_inputs.insert("input".to_string(), vec![TypeId::of::<()>()]);
    let mut filter_outputs = HashMap::new();
    filter_outputs.insert("output".to_string(), vec![TypeId::of::<()>()]);
    registry.add_def(TaskInfo {
        task_id: "filter".to_string(),
        config_tpl: Some(TaskConfigTpl {
            fields: vec![
                ConfigField::required("header_key", ConfigType::Str),
                ConfigField::required("header_value", ConfigType::Str),
                ConfigField::with_default("op", ConfigValue::Str("eq".to_string())),
                ConfigField::with_default("invert", ConfigValue::Bool(false)),
            ],
            disallow_unknown_fields: true,
        }),
        info: TaskDefInfo::OperatorDef {
            inputs: filter_inputs,
            outputs: filter_outputs,
            build_operator: Filter::new,
        },
    });

    // Register LogSink
    let mut log_sink_inputs = HashMap::new();
    log_sink_inputs.insert("input".to_string(), vec![TypeId::of::<()>()]);
    registry.add_def(TaskInfo {
        task_id: "log_sink".to_string(),
        config_tpl: None,
        info: TaskDefInfo::SinkDef {
            inputs: log_sink_inputs,
            build_sink: LogSink::new,
        },
    });

    // Configure S3ListSource to watch df-bucket for CSV files
    let mut s3_config = HashMap::<String, ConfigValue>::new();
    s3_config.insert(
        "bucket".to_string(),
        ConfigValue::Str("df-bucket".to_string()),
    );
    s3_config.insert(
        "endpoint".to_string(),
        ConfigValue::Str(env::var("S3_ENDPOINT").unwrap()),
    );
    s3_config.insert(
        "access_key_id".to_string(),
        ConfigValue::Str(env::var("S3_ACCESS_KEY").unwrap()),
    );
    s3_config.insert(
        "secret_access_key".to_string(),
        ConfigValue::Str(env::var("S3_SECRET_KEY").unwrap()),
    );
    s3_config.insert(
        "extensions".to_string(),
        ConfigValue::Arr(vec![ConfigValue::Str("csv".to_string())]),
    );

    // Configure Filter to pass only objects with size >= 1000
    let mut filter_config = HashMap::<String, ConfigValue>::new();
    filter_config.insert(
        "header_key".to_string(),
        ConfigValue::Str("s3_size".to_string()),
    );
    filter_config.insert(
        "header_value".to_string(),
        ConfigValue::Str("1000".to_string()),
    );
    filter_config.insert("op".to_string(), ConfigValue::Str("ge".to_string()));

    // Create the flow: S3ListSource -> Filter -> LogSink
    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "s3_source".to_string(),
                task_id: "s3_list_source".to_string(),
                configuration: s3_config,
                info: None,
            },
            Node {
                node_id: "size_filter".to_string(),
                task_id: "filter".to_string(),
                configuration: filter_config,
                info: None,
            },
            Node {
                node_id: "logger".to_string(),
                task_id: "log_sink".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("s3_source".to_string(), "object".to_string()),
                to: NodeRef::new("size_filter".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("size_filter".to_string(), "output".to_string()),
                to: NodeRef::new("logger".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    // Initialize and run the flow
    println!("Validating flow...");
    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).map_err(|errs| errs.join(", "))?;

    let monitor_chan = Spawn::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));

    println!("Starting S3 listener for CSV files on df-bucket...");
    println!("(Press Ctrl+C to stop)");

    let root = Root::new(flow, monitor_chan);
    let root_ref: ActorRef<Root> = Spawn::spawn(root);
    root_ref.wait_for_shutdown().await;

    println!("Done.");
    Ok(())
}
