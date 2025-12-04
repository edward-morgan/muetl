use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use kameo::prelude::*;

use crate::{
    flow::{Edge, Flow, NodeRef},
    registry::{Registry, TaskDefInfo, TaskInfo},
};

use super::NegotiatedType;

/// The Root is the core runtime that controls everything else inside muetl. It's responsible for:
/// 1. Parsing a Muetl flow definition and returning an error if it is invalid.
/// 2. Negotiating types for each edge in the flow and creating Connections.
/// 3. Spawning Actors in the order they need to be.
/// 4. Monitoring the status registry to determine how execution should proceed.
/// 5. Running the flow to completion and optionally exiting.
///
/// Note that in the event that more than one flow is running in a single muetl runtime, multiple Root actors may be present.
pub struct Root {
    id: u64,
    /// A fully validated Flow that will be managed by this Root.
    flow: Flow,
    /// A link to the current Registry that stores TaskDefs
    registry: Arc<Registry>,
}

// Validation takes three steps:
// 1. Check if a def for every node's task_id can be found; error if any cannot be
//     - Populate the Flow with TaskInfo refs for each Node
// 2. Group edges by their `from` NodeRef
// 3. For each edge group:
//     - Look at the grouped edges' NodeRef and initialize a type set to the output types supported by the referenced TaskInfo
//     - Look at each edge's `to` NodeRef and determine the union of types supported by the given input conn_name
//     - If the union is empty, throw an error
//     - If the set of output types supported by the referenced TaskInfo is disjoint with the union of input types, throw an error
//     - Otherwise, set a NegotiatedType for each edge in the group.
// TODO: move this into the RawFlow -> Flow parsing process
fn validate_flow(flow: &mut Flow, reg: Arc<Registry>) -> ValidationResult {
    let mut validation_errors = vec![];
    for (node_id, node) in flow.nodes.iter_mut() {
        // 1. Ensure each referenced node exists in the registry; return an error immediately if any can't be found-
        // don't wait to aggregate any more errors.
        if let Some(def) = reg.def_for(&node.task_id) {
            node.info = Some(def);
        } else {
            return Err(vec![format!(
                "Failed to find TaskDef with id {}",
                node.task_id
            )]);
        }
    }
    // 2. Group edges by their `from` NodeRef
    let mut outgoing_edges: HashMap<NodeRef, Vec<&mut Edge>> = HashMap::new();
    for edge in &flow.edges {
        outgoing_edges.insert(edge.from.clone(), vec![]);
    }
    for edge in flow.edges.iter_mut() {
        let edges = outgoing_edges.get_mut(&edge.from).unwrap();
        edges.push(edge);
    }

    for (edge_source, mut edges) in outgoing_edges {
        // Find the node this edge is from
        // unwrap() is okay here as this has already been parsed/validated
        let source = flow.nodes.get(&edge_source.node_id).unwrap();
        // unwrap() is okay here since we just set the info for all nodes, or else returned an error
        let task_info = source.info.as_ref().unwrap();
        if let Some(mut negotiated_types) = outputs_of(task_info, &edge_source.conn_name) {
            // For each edge, get the supported types of the input and ensure that at least one is available
            for edge in &edges {
                // unwrap() is okay here as this has already been parsed/validated
                let dest = flow.nodes.get(&edge.to.node_id).unwrap();
                // unwrap() is okay here since we just set the info for all nodes, or else returned an error
                let dest_info = dest.info.as_ref().unwrap();
                if let Some(supported_input_types) = inputs_of(&dest_info, &edge.to.conn_name) {
                    // For each input, constrain the set of negotiated types against anything the input type can support
                    negotiated_types = supported_input_types
                        .iter()
                        .filter_map(|tpe| {
                            if negotiated_types.contains(tpe) {
                                Some(*tpe)
                            } else {
                                None
                            }
                        })
                        .collect();
                }
            }

            if negotiated_types.is_empty() {
                validation_errors.push(format!(
                    "no common types exist for {} outgoing edges of {}",
                    edges.len(),
                    edge_source,
                ))
            } else {
                // If there is a type overlap, then choose the first one. In the future this may become more sophisticated.
                edges.iter_mut().for_each(|edge| {
                    edge.edge_type = Some(NegotiatedType::Singleton(negotiated_types[0]))
                })
            }
        } else {
            validation_errors.push(format!(
                "node definition named {} (node_id {}) was not registered with any outputs",
                source.task_id, edge_source.node_id
            ));
        }
    }
    if !validation_errors.is_empty() {
        Err(validation_errors)
    } else {
        Ok(())
    }
}

/// Retrieves the outputs of a given TaskInfo for a conn_name in it. If the conn_name doesn't exist, or if the
/// TaskInfo doesn't have outputs (for example, it's a Sink), then None is returned. Otherwise, the output type
/// vector is copied and returned.
fn outputs_of(task_info: &TaskInfo, conn_name: &String) -> Option<Vec<TypeId>> {
    match &task_info.info {
        TaskDefInfo::DaemonDef { outputs, .. } => outputs.get(conn_name).map(|o| o.clone()),
        _ => None,
    }
}
/// Retrieves the inputs of a given TaskInfo for a conn_name in it. If the conn_name doesn't exist, or if the
/// TaskInfo doesn't have inputs (for example, it's a Daemon), then None is returned. Otherwise, the input type
/// vector is copied and returned.
fn inputs_of(task_info: &TaskInfo, conn_name: &String) -> Option<Vec<TypeId>> {
    match &task_info.info {
        TaskDefInfo::SinkDef { inputs, .. } => inputs.get(conn_name).map(|o| o.clone()),
        _ => None,
    }
}

pub type ValidationResult = Result<(), Vec<String>>;

impl Actor for Root {
    type Args = Self;
    type Error = ();
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        Ok(args)
    }
    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        flow::{Node, RawFlow},
        task_defs::{daemon::Daemon, sink::Sink, TaskConfig},
    };

    use super::*;

    fn dummy_build_sink(_: &TaskConfig) -> Result<Box<dyn Sink>, String> {
        Err(format!("unimplemented"))
    }
    fn dummy_build_daemon(_: &TaskConfig) -> Result<Box<dyn Daemon>, String> {
        Err(format!("unimplemented"))
    }

    fn build_test_registry() -> Registry {
        let mut registry = Registry::new();

        let mut node1_outputs = HashMap::new();
        node1_outputs.insert("output-1".to_string(), vec![TypeId::of::<String>()]);

        let mut node2_inputs = HashMap::new();
        node2_inputs.insert("input-1".to_string(), vec![TypeId::of::<String>()]);

        let node1_ti = TaskInfo {
            task_id: "one".to_string(),
            config_tpl: None,
            info: TaskDefInfo::DaemonDef {
                outputs: node1_outputs,
                build_daemon: dummy_build_daemon,
            },
        };
        let node2_ti = TaskInfo {
            task_id: "two".to_string(),
            config_tpl: None,
            info: TaskDefInfo::SinkDef {
                inputs: node2_inputs,
                build_sink: dummy_build_sink,
            },
        };
        registry.add_def(node1_ti);
        registry.add_def(node2_ti);
        registry
    }

    #[test]
    fn validate_valid_flow() {
        let registry = build_test_registry();

        let node1_name = "node-1".to_string();
        let node2_name = "node-2".to_string();
        let nodes = vec![
            Node {
                node_id: node1_name.clone(),
                task_id: "one".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
            Node {
                node_id: node2_name.clone(),
                task_id: "two".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
        ];
        let edges = vec![Edge {
            from: NodeRef::new(node1_name.clone(), "output-1".to_string()),
            to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
            edge_type: None,
        }];

        let raw_flow = RawFlow { nodes, edges };
        let f = Flow::try_from(raw_flow);
        assert!(f.is_ok());
        let mut flow = f.unwrap();
        let validation_result = validate_flow(&mut flow, Arc::new(registry));
        assert!(validation_result.is_ok());

        println!("Validation result: {:?}", validation_result);
        println!("Processed Flow: {:?}", flow);
    }

    #[test]
    fn validate_invalid_flow_bad_outputs() {
        let registry = build_test_registry();

        let node1_name = "node-1".to_string();
        let node2_name = "node-2".to_string();
        let nodes = vec![
            Node {
                node_id: node1_name.clone(),
                task_id: "one".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
            Node {
                node_id: node2_name.clone(),
                task_id: "two".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
        ];
        let edges = vec![Edge {
            from: NodeRef::new(node1_name.clone(), "output-1".to_string()),
            to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
            edge_type: None,
        }];

        let raw_flow = RawFlow { nodes, edges };
        let f = Flow::try_from(raw_flow);
        assert!(f.is_ok());
        let mut flow = f.unwrap();
        let validation_result = validate_flow(&mut flow, Arc::new(registry));
        assert!(validation_result.is_ok());

        println!("Validation result: {:?}", validation_result);
        println!("Processed Flow: {:?}", flow);
    }
}
