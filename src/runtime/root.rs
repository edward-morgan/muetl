use std::{any::TypeId, collections::HashMap, rc::Rc, sync::Arc};

use kameo::prelude::*;
use kameo_actors::pubsub::PubSub;

use crate::{
    flow::{Edge, Flow, NodeRef},
    messages::StatusUpdate,
    registry::{Registry, TaskDefInfo, TaskInfo},
    util::new_id,
};

use super::{
    connection::{Connection, IncomingConnections, OutgoingConnections},
    daemon_actor::DaemonActor,
    sink_actor::SinkActor,
    NegotiatedType,
};

/// The Root is the core runtime that controls everything else inside muetl. It's responsible for:
/// 1. Spawning Actors in the order they need to be.
/// 2. Monitoring the status registry to determine how execution should proceed.
/// 3. Running the flow to completion and optionally exiting.
///
/// Note that in the event that more than one flow is running in a single muetl runtime, multiple Root actors may be present.
pub struct Root {
    id: u64,
    /// A fully validated Flow that will be managed by this Root.
    flow: Flow,
    /// A channel that all tasks under this Root will send status updates to
    monitor_chan: ActorRef<PubSub<StatusUpdate>>,
}

impl Root {
    pub fn new(flow: Flow, monitor_chan: ActorRef<PubSub<StatusUpdate>>) -> Self {
        Self {
            id: new_id(),
            flow,
            monitor_chan,
        }
    }
}

impl Actor for Root {
    type Args = Self;
    type Error = String;

    /// On startup, the root should instantiate supervised actors for each of the Nodes in the validated Flow it
    /// receives when constructed.
    async fn on_start(args: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        // Turn every Edge into a Connection
        let connections = EdgeConnections::from(args.flow.edges.iter().collect::<Vec<&Edge>>());

        for (node_id, node) in &args.flow.nodes {
            match &node.info.as_ref().unwrap().info {
                TaskDefInfo::DaemonDef {
                    outputs: _outputs,
                    build_daemon,
                } => match build_daemon(&node.configuration) {
                    Ok(daemon) => {
                        let r = DaemonActor::new(
                            Some(daemon),
                            args.monitor_chan.clone(),
                            connections.outgoing_connections_from(&node_id),
                        );
                        DaemonActor::spawn(r);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                },
                TaskDefInfo::SinkDef { inputs, build_sink } => {
                    match build_sink(&node.configuration) {
                        Ok(sink) => {
                            let r = SinkActor::new(
                                Some(sink),
                                args.monitor_chan.clone(),
                                connections.incoming_connections_to(node_id),
                            );
                            SinkActor::spawn(r);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }
        }

        // For each Node, find its subset of connections and pass them to it upon construction
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

struct EdgeConnections<'a> {
    mapping: Vec<(&'a Edge, Connection)>,
}

impl<'a> From<Vec<&'a Edge>> for EdgeConnections<'a> {
    fn from(edges: Vec<&'a Edge>) -> Self {
        EdgeConnections {
            mapping: edges.iter().map(|&e| (e, e.to_connection())).collect(),
        }
    }
}

impl<'a> EdgeConnections<'a> {
    pub fn outgoing_connections_from(&self, edge_node_id: &String) -> OutgoingConnections {
        let conns = self
            .mapping
            .iter()
            .flat_map(|(e, c)| {
                if e.from.node_id == *edge_node_id {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();
        OutgoingConnections::from(&conns)
    }
    pub fn incoming_connections_to(&self, edge_node_id: &String) -> IncomingConnections {
        let conns = self
            .mapping
            .iter()
            .flat_map(|(e, c)| {
                if e.to.node_id == *edge_node_id {
                    Some(c)
                } else {
                    None
                }
            })
            .collect();
        IncomingConnections::from(&conns)
    }
}

// #[cfg(test)]
// mod tests {

//     use crate::{
//         flow::{Node, RawFlow},
//         task_defs::{daemon::Daemon, sink::Sink, TaskConfig},
//     };

//     use super::*;

//     fn dummy_build_sink(_: &TaskConfig) -> Result<Box<dyn Sink>, String> {
//         Err(format!("unimplemented"))
//     }
//     fn dummy_build_daemon(_: &TaskConfig) -> Result<Box<dyn Daemon>, String> {
//         Err(format!("unimplemented"))
//     }

//     fn build_test_registry() -> Registry {
//         let mut registry = Registry::new();

//         let mut node1_outputs = HashMap::new();
//         node1_outputs.insert("output-1".to_string(), vec![TypeId::of::<String>()]);

//         let mut node2_inputs = HashMap::new();
//         node2_inputs.insert("input-1".to_string(), vec![TypeId::of::<String>()]);

//         let node1_ti = TaskInfo {
//             task_id: "one".to_string(),
//             config_tpl: None,
//             info: TaskDefInfo::DaemonDef {
//                 outputs: node1_outputs,
//                 build_daemon: dummy_build_daemon,
//             },
//         };
//         let node2_ti = TaskInfo {
//             task_id: "two".to_string(),
//             config_tpl: None,
//             info: TaskDefInfo::SinkDef {
//                 inputs: node2_inputs,
//                 build_sink: dummy_build_sink,
//             },
//         };
//         registry.add_def(node1_ti);
//         registry.add_def(node2_ti);
//         registry
//     }

//     #[test]
//     fn validate_valid_flow() {
//         let registry = build_test_registry();

//         let node1_name = "node-1".to_string();
//         let node2_name = "node-2".to_string();
//         let nodes = vec![
//             Node {
//                 node_id: node1_name.clone(),
//                 task_id: "one".to_string(),
//                 configuration: HashMap::new(),
//                 info: None,
//             },
//             Node {
//                 node_id: node2_name.clone(),
//                 task_id: "two".to_string(),
//                 configuration: HashMap::new(),
//                 info: None,
//             },
//         ];
//         let edges = vec![Edge {
//             from: NodeRef::new(node1_name.clone(), "output-1".to_string()),
//             to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
//             edge_type: None,
//         }];

//         let raw_flow = RawFlow { nodes, edges };
//         let f = Flow::try_from(raw_flow);
//         assert!(f.is_ok());
//         let mut flow = f.unwrap();
//         let validation_result = validate_flow(&mut flow, Arc::new(registry));
//         assert!(validation_result.is_ok());

//         println!("Validation result: {:?}", validation_result);
//         println!("Processed Flow: {:?}", flow);
//     }

//     #[test]
//     fn validate_invalid_flow_bad_outputs() {
//         let registry = build_test_registry();

//         let node1_name = "node-1".to_string();
//         let node2_name = "node-2".to_string();
//         let nodes = vec![
//             Node {
//                 node_id: node1_name.clone(),
//                 task_id: "one".to_string(),
//                 configuration: HashMap::new(),
//                 info: None,
//             },
//             Node {
//                 node_id: node2_name.clone(),
//                 task_id: "two".to_string(),
//                 configuration: HashMap::new(),
//                 info: None,
//             },
//         ];
//         let edges = vec![Edge {
//             from: NodeRef::new(node1_name.clone(), "output-1".to_string()),
//             to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
//             edge_type: None,
//         }];

//         let raw_flow = RawFlow { nodes, edges };
//         let f = Flow::try_from(raw_flow);
//         assert!(f.is_ok());
//         let mut flow = f.unwrap();
//         let validation_result = validate_flow(&mut flow, Arc::new(registry));
//         assert!(validation_result.is_ok());

//         println!("Validation result: {:?}", validation_result);
//         println!("Processed Flow: {:?}", flow);
//     }
// }
