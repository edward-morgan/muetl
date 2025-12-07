//! Integration tests for Operator arithmetic operations.
//!
//! These tests validate end-to-end flows using:
//! - NumberSource (Source) -> Adder/Multiplier (Operator) -> ResultCollector (Sink)

mod common;

use std::{any::TypeId, collections::HashMap, sync::Arc};

use kameo::Actor;
use kameo_actors::pubsub::PubSub;
use muetl::{
    flow::{Edge, Flow, Node, NodeRef, RawFlow},
    registry::{Registry, TaskDefInfo, TaskInfo},
    runtime::root::Root,
    task_defs::ConfigValue,
};

use common::task_defs::{Adder, Multiplier, NumberSource, ResultCollector};

/// Helper to create a registry with all our test task definitions.
fn create_test_registry() -> Registry {
    let mut registry = Registry::new();

    // NumberSource source
    let mut number_source_outputs = HashMap::new();
    number_source_outputs.insert("output".to_string(), vec![TypeId::of::<i64>()]);
    registry.add_def(TaskInfo {
        task_id: "number_source".to_string(),
        config_tpl: None,
        info: TaskDefInfo::SourceDef {
            outputs: number_source_outputs,
            build_source: NumberSource::new,
        },
    });

    // Adder operator
    let mut adder_inputs = HashMap::new();
    adder_inputs.insert("input".to_string(), vec![TypeId::of::<i64>()]);
    let mut adder_outputs = HashMap::new();
    adder_outputs.insert("output".to_string(), vec![TypeId::of::<i64>()]);
    registry.add_def(TaskInfo {
        task_id: "adder".to_string(),
        config_tpl: None,
        info: TaskDefInfo::OperatorDef {
            inputs: adder_inputs,
            outputs: adder_outputs,
            build_operator: Adder::new,
        },
    });

    // Multiplier operator
    let mut multiplier_inputs = HashMap::new();
    multiplier_inputs.insert("input".to_string(), vec![TypeId::of::<i64>()]);
    let mut multiplier_outputs = HashMap::new();
    multiplier_outputs.insert("output".to_string(), vec![TypeId::of::<i64>()]);
    registry.add_def(TaskInfo {
        task_id: "multiplier".to_string(),
        config_tpl: None,
        info: TaskDefInfo::OperatorDef {
            inputs: multiplier_inputs,
            outputs: multiplier_outputs,
            build_operator: Multiplier::new,
        },
    });

    // ResultCollector sink
    let mut collector_inputs = HashMap::new();
    collector_inputs.insert("input".to_string(), vec![TypeId::of::<i64>()]);
    registry.add_def(TaskInfo {
        task_id: "result_collector".to_string(),
        config_tpl: None,
        info: TaskDefInfo::SinkDef {
            inputs: collector_inputs,
            build_sink: ResultCollector::new,
        },
    });

    registry
}

/// Test 1: Basic Operator passthrough
/// Flow: NumberSource -> Adder(+0) -> ResultCollector
/// Validates that an Operator correctly receives events and forwards them downstream.
#[tokio::test]
async fn test_basic_node_passthrough() {
    ResultCollector::clear_all();

    let registry = create_test_registry();

    // NumberSource config: emit 5 numbers (0..5)
    let mut src_config = HashMap::new();
    src_config.insert("count".to_string(), ConfigValue::Int(5));

    // Adder config: add 0 (passthrough)
    let mut adder_config = HashMap::new();
    adder_config.insert("addend".to_string(), ConfigValue::Int(0));

    // ResultCollector config
    let mut collector_config = HashMap::new();
    collector_config.insert("name".to_string(), ConfigValue::Str("test1".to_string()));

    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "source".to_string(),
                task_id: "number_source".to_string(),
                configuration: src_config,
                info: None,
            },
            Node {
                node_id: "adder".to_string(),
                task_id: "adder".to_string(),
                configuration: adder_config,
                info: None,
            },
            Node {
                node_id: "collector".to_string(),
                task_id: "result_collector".to_string(),
                configuration: collector_config,
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("source".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("collector".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    let results = ResultCollector::get_results("test1");
    assert_eq!(results.len(), 5, "Expected 5 results, got {}", results.len());

    let mut sorted_results = results.clone();
    sorted_results.sort();
    assert_eq!(sorted_results, vec![0, 1, 2, 3, 4], "Expected [0,1,2,3,4], got {:?}", sorted_results);
}

/// Test 2: Single Operator transformation
/// Flow: NumberSource -> Adder(+10) -> ResultCollector
/// Validates that an Operator actually transforms data.
#[tokio::test]
async fn test_single_node_transformation() {
    ResultCollector::clear_all();

    let registry = create_test_registry();

    let mut src_config = HashMap::new();
    src_config.insert("count".to_string(), ConfigValue::Int(5));

    let mut adder_config = HashMap::new();
    adder_config.insert("addend".to_string(), ConfigValue::Int(10));

    let mut collector_config = HashMap::new();
    collector_config.insert("name".to_string(), ConfigValue::Str("test2".to_string()));

    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "source".to_string(),
                task_id: "number_source".to_string(),
                configuration: src_config,
                info: None,
            },
            Node {
                node_id: "adder".to_string(),
                task_id: "adder".to_string(),
                configuration: adder_config,
                info: None,
            },
            Node {
                node_id: "collector".to_string(),
                task_id: "result_collector".to_string(),
                configuration: collector_config,
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("source".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("collector".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    let results = ResultCollector::get_results("test2");
    assert_eq!(results.len(), 5, "Expected 5 results, got {}", results.len());

    let mut sorted_results = results.clone();
    sorted_results.sort();
    assert_eq!(sorted_results, vec![10, 11, 12, 13, 14], "Expected [10,11,12,13,14], got {:?}", sorted_results);
}

/// Test 3: Chained Operators
/// Flow: NumberSource -> Adder(+5) -> Multiplier(*2) -> ResultCollector
/// Validates that Operators can be chained together.
/// NumberSource emits 0,1,2,3,4 → Adder produces 5,6,7,8,9 → Multiplier produces 10,12,14,16,18
#[tokio::test]
async fn test_chained_nodes() {
    ResultCollector::clear_all();

    let registry = create_test_registry();

    let mut src_config = HashMap::new();
    src_config.insert("count".to_string(), ConfigValue::Int(5));

    let mut adder_config = HashMap::new();
    adder_config.insert("addend".to_string(), ConfigValue::Int(5));

    let mut multiplier_config = HashMap::new();
    multiplier_config.insert("factor".to_string(), ConfigValue::Int(2));

    let mut collector_config = HashMap::new();
    collector_config.insert("name".to_string(), ConfigValue::Str("test3".to_string()));

    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "source".to_string(),
                task_id: "number_source".to_string(),
                configuration: src_config,
                info: None,
            },
            Node {
                node_id: "adder".to_string(),
                task_id: "adder".to_string(),
                configuration: adder_config,
                info: None,
            },
            Node {
                node_id: "multiplier".to_string(),
                task_id: "multiplier".to_string(),
                configuration: multiplier_config,
                info: None,
            },
            Node {
                node_id: "collector".to_string(),
                task_id: "result_collector".to_string(),
                configuration: collector_config,
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("source".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("multiplier".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("multiplier".to_string(), "output".to_string()),
                to: NodeRef::new("collector".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    let results = ResultCollector::get_results("test3");
    assert_eq!(results.len(), 5, "Expected 5 results, got {}", results.len());

    let mut sorted_results = results.clone();
    sorted_results.sort();
    assert_eq!(sorted_results, vec![10, 12, 14, 16, 18], "Expected [10,12,14,16,18], got {:?}", sorted_results);
}

/// Test 4: Fan-out from Operator
/// Flow: NumberSource -> Adder(+1) -> ResultCollector("a")
///                               \-> ResultCollector("b")
/// Validates that an Operator's output can be sent to multiple downstream Sinks.
#[tokio::test]
async fn test_fan_out_from_node() {
    ResultCollector::clear_all();

    let registry = create_test_registry();

    let mut src_config = HashMap::new();
    src_config.insert("count".to_string(), ConfigValue::Int(5));

    let mut adder_config = HashMap::new();
    adder_config.insert("addend".to_string(), ConfigValue::Int(1));

    let mut collector_a_config = HashMap::new();
    collector_a_config.insert("name".to_string(), ConfigValue::Str("fan_out_a".to_string()));

    let mut collector_b_config = HashMap::new();
    collector_b_config.insert("name".to_string(), ConfigValue::Str("fan_out_b".to_string()));

    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "source".to_string(),
                task_id: "number_source".to_string(),
                configuration: src_config,
                info: None,
            },
            Node {
                node_id: "adder".to_string(),
                task_id: "adder".to_string(),
                configuration: adder_config,
                info: None,
            },
            Node {
                node_id: "collector_a".to_string(),
                task_id: "result_collector".to_string(),
                configuration: collector_a_config,
                info: None,
            },
            Node {
                node_id: "collector_b".to_string(),
                task_id: "result_collector".to_string(),
                configuration: collector_b_config,
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("source".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("collector_a".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("collector_b".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    let results_a = ResultCollector::get_results("fan_out_a");
    let results_b = ResultCollector::get_results("fan_out_b");

    assert_eq!(results_a.len(), 5, "Expected 5 results in collector_a, got {}", results_a.len());
    assert_eq!(results_b.len(), 5, "Expected 5 results in collector_b, got {}", results_b.len());

    let mut sorted_a = results_a.clone();
    sorted_a.sort();
    let mut sorted_b = results_b.clone();
    sorted_b.sort();

    assert_eq!(sorted_a, vec![1, 2, 3, 4, 5], "Expected [1,2,3,4,5] in collector_a, got {:?}", sorted_a);
    assert_eq!(sorted_b, vec![1, 2, 3, 4, 5], "Expected [1,2,3,4,5] in collector_b, got {:?}", sorted_b);
}

/// Test 5: Fan-in to Operator
/// Flow: NumberSource("src1", count=3) -> Adder(+100) -> ResultCollector
///       NumberSource("src2", count=3) -/
/// Validates that an Operator can receive events from multiple upstream Sources.
#[tokio::test]
async fn test_fan_in_to_node() {
    ResultCollector::clear_all();

    let registry = create_test_registry();

    let mut src1_config = HashMap::new();
    src1_config.insert("count".to_string(), ConfigValue::Int(3));

    let mut src2_config = HashMap::new();
    src2_config.insert("count".to_string(), ConfigValue::Int(3));

    let mut adder_config = HashMap::new();
    adder_config.insert("addend".to_string(), ConfigValue::Int(100));

    let mut collector_config = HashMap::new();
    collector_config.insert("name".to_string(), ConfigValue::Str("fan_in".to_string()));

    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "source1".to_string(),
                task_id: "number_source".to_string(),
                configuration: src1_config,
                info: None,
            },
            Node {
                node_id: "source2".to_string(),
                task_id: "number_source".to_string(),
                configuration: src2_config,
                info: None,
            },
            Node {
                node_id: "adder".to_string(),
                task_id: "adder".to_string(),
                configuration: adder_config,
                info: None,
            },
            Node {
                node_id: "collector".to_string(),
                task_id: "result_collector".to_string(),
                configuration: collector_config,
                info: None,
            },
        ],
        edges: vec![
            Edge {
                from: NodeRef::new("source1".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("source2".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("collector".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    let results = ResultCollector::get_results("fan_in");
    assert_eq!(results.len(), 6, "Expected 6 results, got {}", results.len());

    let mut sorted_results = results.clone();
    sorted_results.sort();
    // Both sources emit 0,1,2 which become 100,101,102 after adding 100
    // So we expect two of each: [100, 100, 101, 101, 102, 102]
    assert_eq!(sorted_results, vec![100, 100, 101, 101, 102, 102], "Expected [100,100,101,101,102,102], got {:?}", sorted_results);
}

/// Test 6: Mixed pipeline
/// Flow: NumberSource -> Adder -> Multiplier -> ResultCollector("transformed")
///                   \-> ResultCollector("raw")
/// Validates a complex topology where the same Source feeds both an Operator chain and a direct Sink.
#[tokio::test]
async fn test_mixed_pipeline() {
    ResultCollector::clear_all();

    let registry = create_test_registry();

    let mut src_config = HashMap::new();
    src_config.insert("count".to_string(), ConfigValue::Int(5));

    let mut adder_config = HashMap::new();
    adder_config.insert("addend".to_string(), ConfigValue::Int(10));

    let mut multiplier_config = HashMap::new();
    multiplier_config.insert("factor".to_string(), ConfigValue::Int(3));

    let mut transformed_config = HashMap::new();
    transformed_config.insert("name".to_string(), ConfigValue::Str("transformed".to_string()));

    let mut raw_config = HashMap::new();
    raw_config.insert("name".to_string(), ConfigValue::Str("raw".to_string()));

    let raw_flow = RawFlow {
        nodes: vec![
            Node {
                node_id: "source".to_string(),
                task_id: "number_source".to_string(),
                configuration: src_config,
                info: None,
            },
            Node {
                node_id: "adder".to_string(),
                task_id: "adder".to_string(),
                configuration: adder_config,
                info: None,
            },
            Node {
                node_id: "multiplier".to_string(),
                task_id: "multiplier".to_string(),
                configuration: multiplier_config,
                info: None,
            },
            Node {
                node_id: "transformed_collector".to_string(),
                task_id: "result_collector".to_string(),
                configuration: transformed_config,
                info: None,
            },
            Node {
                node_id: "raw_collector".to_string(),
                task_id: "result_collector".to_string(),
                configuration: raw_config,
                info: None,
            },
        ],
        edges: vec![
            // Source -> Adder -> Multiplier -> transformed_collector
            Edge {
                from: NodeRef::new("source".to_string(), "output".to_string()),
                to: NodeRef::new("adder".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("adder".to_string(), "output".to_string()),
                to: NodeRef::new("multiplier".to_string(), "input".to_string()),
                edge_type: None,
            },
            Edge {
                from: NodeRef::new("multiplier".to_string(), "output".to_string()),
                to: NodeRef::new("transformed_collector".to_string(), "input".to_string()),
                edge_type: None,
            },
            // Source -> raw_collector (direct)
            Edge {
                from: NodeRef::new("source".to_string(), "output".to_string()),
                to: NodeRef::new("raw_collector".to_string(), "input".to_string()),
                edge_type: None,
            },
        ],
    };

    let flow = Flow::parse_from(raw_flow, Arc::new(registry)).unwrap();
    let monitor_chan = PubSub::spawn(PubSub::new(kameo_actors::DeliveryStrategy::Guaranteed));
    let root = Root::new(flow, monitor_chan);
    let root_ref = Root::spawn(root);
    root_ref.wait_for_shutdown().await;

    let raw_results = ResultCollector::get_results("raw");
    let transformed_results = ResultCollector::get_results("transformed");

    assert_eq!(raw_results.len(), 5, "Expected 5 raw results, got {}", raw_results.len());
    assert_eq!(transformed_results.len(), 5, "Expected 5 transformed results, got {}", transformed_results.len());

    let mut sorted_raw = raw_results.clone();
    sorted_raw.sort();
    let mut sorted_transformed = transformed_results.clone();
    sorted_transformed.sort();

    // Raw: 0,1,2,3,4
    assert_eq!(sorted_raw, vec![0, 1, 2, 3, 4], "Expected raw [0,1,2,3,4], got {:?}", sorted_raw);

    // Transformed: (0+10)*3=30, (1+10)*3=33, (2+10)*3=36, (3+10)*3=39, (4+10)*3=42
    assert_eq!(sorted_transformed, vec![30, 33, 36, 39, 42], "Expected transformed [30,33,36,39,42], got {:?}", sorted_transformed);
}
