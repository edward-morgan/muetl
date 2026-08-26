use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, hash::Hash, sync::Arc};

use crate::{
    registry::{Registry, TaskInfo},
    runtime::{connection::Connection, NegotiatedType},
    task_defs::ConfigValue,
};

type ValidationResult = Result<(), Vec<String>>;

#[derive(Serialize, Deserialize)]
pub struct RawFlow {
    pub id: String,
    pub nodes: Vec<RawNode>,
    pub edges: Vec<RawEdge>,
}

#[derive(Serialize, Deserialize)]
/// The raw, unparsed representation of a node in a flow.
pub struct RawNode {
    /// The internal identifier of this Node as it relates to Edges in the Flow.
    pub node_id: String,
    /// The identifier of the Task that this Node coresponds to.
    pub task_id: String,
    /// The raw runtime configuration being supplied to this Node. Note that this will be
    /// validated at runtime against the template advertised by the Node, and an error
    /// may be returned if the provided configuration does not match what is required.
    pub configuration: HashMap<String, ConfigValue>,
}

#[derive(Clone, Serialize, Deserialize)]
/// The raw, unparsed representation of an edge in a flow.
pub struct RawEdge {
    /// The source (node ID and conn name) of the edge.
    pub from: NodeRef,
    /// The target (node ID and conn name) of the edge.
    pub to: NodeRef,
}

impl Display for RawEdge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[from: {}, to: {}]", self.from, self.to)
    }
}

/// The validated version of a RawFlow, parsed from a RawFlow.
#[derive(Debug, Clone)]
pub struct Flow {
    pub id: String,
    /// node_id -> Node
    pub nodes: HashMap<String, Node>,
    pub edges: Vec<Edge>,
}

impl Flow {
    pub fn get_task_info_for(&self, node_ref: &NodeRef) -> Option<Arc<TaskInfo>> {
        self.nodes
            .get(&node_ref.node_id)
            .map(|n| n.info.clone())
            .flatten()
    }

    /// Sole constructor for a `Flow` that accepts a `RawFlow` and `Registry` as input. This adheres to
    /// the "parse, don't validate" approach for working with potentially untrusted data. Both
    /// syntactical and semantic validation is done, and the resulting `Flow` should be considered
    /// fully initialized with references from the Registry.
    pub fn parse_from(raw_flow: RawFlow, reg: Arc<Registry>) -> Result<Self, Vec<String>> {
        match Flow::parse_structure(raw_flow) {
            Ok(mut parsed) => match parsed.validate_flow(reg) {
                Ok(()) => Ok(parsed),
                Err(e) => Err(e),
            },
            Err(e) => Err(vec![e]),
        }
    }

    /// Parse the structure of a RawFlow, producing a Flow if the graph definition:
    /// 1. Contains well-formed node_ids for each node; i.e there are no duplicates.
    /// 2. Contains well-formed edges; i.e. each edge has a valid `to` and `from` node reference.
    ///
    /// Note that further semantic validation that would require information about the nodes as they exist
    /// on a muetl instance is not performed here; that would require a `Registry` to retrieve `TaskInfo`s.
    fn parse_structure(value: RawFlow) -> Result<Self, String> {
        let mut hm = HashMap::new();
        for raw_node in value.nodes {
            match hm.insert(raw_node.node_id.clone(), Node::from(raw_node)) {
                Some(prev) => {
                    return Err(format!(
                        "invalid flow: node id {} is duplicated",
                        prev.node_id,
                    ));
                }
                None => {}
            }
        }
        // Make sure every edge is pointing to nodes that exist
        for edge in &value.edges {
            if !hm.contains_key(&edge.from.node_id) {
                return Err(format!(
                    "invalid flow: nonexistent source node_id for edge {}",
                    edge
                ));
            } else if !hm.contains_key(&edge.to.node_id) {
                return Err(format!(
                    "invalid flow: nonexistent target node_id for edge {}",
                    edge
                ));
            }
        }
        Ok(Flow {
            id: value.id,
            nodes: hm,
            edges: value.edges.iter().map(|e| Edge::from(e.clone())).collect(),
        })
    }

    /// Validation takes three steps:
    /// 1. Check if a def for every node's task_id can be found; error if any cannot be
    ///     - Populate the Flow with TaskInfo refs for each Node
    /// 2. Group edges by their `from` NodeRef
    /// 3. For each edge group:
    ///     - Look at the grouped edges' NodeRef and initialize a type set to the output types supported by the referenced TaskInfo
    ///     - Look at each edge's `to` NodeRef and determine the union of types supported by the given input conn_name
    ///     - If the union is empty, throw an error
    ///     - If the set of output types supported by the referenced TaskInfo is disjoint with the union of input types, throw an error
    ///     - Otherwise, set a NegotiatedType for each edge in the group.
    fn validate_flow(&mut self, reg: Arc<Registry>) -> ValidationResult {
        let mut validation_errors = vec![];
        for (_node_id, node) in self.nodes.iter_mut() {
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

        // Ensure that each Edge connects to known inputs/outputs. If there are any missing Edges then
        // return an error here.
        if let Err(errs) = self.validate_edges(reg.clone()) {
            return Err(errs);
        }

        // 2. Group edges by their `from` NodeRef
        let mut outgoing_edges: HashMap<NodeRef, Vec<&mut Edge>> = HashMap::new();
        for edge in &self.edges {
            outgoing_edges.insert(edge.from.clone(), vec![]);
        }
        for edge in self.edges.iter_mut() {
            let edges = outgoing_edges.get_mut(&edge.from).unwrap();
            edges.push(edge);
        }

        for (edge_source, mut edges) in outgoing_edges {
            // Find the node this edge is from
            // unwrap() is okay here as this has already been parsed/validated
            let source = self.nodes.get(&edge_source.node_id).unwrap();
            // unwrap() is okay here since we just set the info for all nodes, or else returned an error
            let task_info = source.info.as_ref().unwrap();
            if let Some(mut negotiated_types) =
                task_info.info.get_outputs_for(&edge_source.conn_name)
            {
                // For each edge, get the supported types of the input and ensure that at least one is available
                for edge in &edges {
                    // unwrap() is okay here as this has already been parsed/validated
                    let dest = self.nodes.get(&edge.to.node_id).unwrap();
                    // unwrap() is okay here since we just set the info for all nodes, or else returned an error
                    let dest_info = dest.info.as_ref().unwrap();
                    if let Some(supported_input_types) =
                        dest_info.info.get_inputs_for(&edge.to.conn_name)
                    {
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
            tracing::debug!("Resolved Flow: {:?}", self);
            Ok(())
        }
    }

    /// Validates that each edge in this `Flow` references a connection (input or output)
    /// that exists on the `TaskInfo` that the `Registry` contains. For a given Flow, all
    /// missing connection names are returned in the `ValidationResult` instead of short-
    /// circuiting on the first missing one.
    fn validate_edges(&self, reg: Arc<Registry>) -> ValidationResult {
        let mut validation_errors = vec![];
        for edge in self.edges.iter() {
            if let Err(e) = self.validate_node_ref(reg.clone(), &edge.from, true) {
                validation_errors.push(e);
            }
            if let Err(e) = self.validate_node_ref(reg.clone(), &edge.to, false) {
                validation_errors.push(e);
            }
        }

        if validation_errors.is_empty() {
            Ok(())
        } else {
            Err(validation_errors)
        }
    }

    fn validate_node_ref(
        &self,
        reg: Arc<Registry>,
        nr: &NodeRef,
        is_from: bool,
    ) -> Result<(), String> {
        if let Some(node) = self.nodes.get(&nr.node_id) {
            if let Some(task_def) = reg.def_for(&node.task_id) {
                if is_from {
                    if let Some(_types) = task_def.info.get_outputs_for(&nr.conn_name) {
                        Ok(())
                    } else {
                        Err(format!(
                            "node {} with task_id {} does not have an output named {} (available: {:?})",
                            nr.node_id, node.task_id, nr.conn_name, task_def.info.all_output_names()
                        ))
                    }
                } else {
                    if let Some(_types) = task_def.info.get_inputs_for(&nr.conn_name) {
                        Ok(())
                    } else {
                        Err(format!(
                            "node {} with task_id {} does not have an input named {} (available: {:?})",
                            nr.node_id, node.task_id, nr.conn_name, task_def.info.all_input_names()
                        ))
                    }
                }
            } else {
                Err(format!(
                    "node {} with task_id {} not found in registry",
                    nr.node_id, node.task_id
                ))
            }
        } else {
            Err(format!("node {} not found in flow", nr.node_id))
        }
    }
}

#[derive(Debug, Clone)]
pub struct Node {
    /// The internal identifier of this Node as it relates to Edges in the Flow.
    pub node_id: String,
    /// The identifier of the Task that this Node coresponds to.
    pub task_id: String,
    /// The raw runtime configuration being supplied to this Node. Note that this will be
    /// validated at runtime against the template advertised by the Node, and an error
    /// may be returned if the provided configuration does not match what is required.
    pub configuration: HashMap<String, ConfigValue>,
    /// Not parsed from a RawFlow; created in the process of validation by a Root actor.
    pub info: Option<Arc<TaskInfo>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug, Serialize, Deserialize)]
pub struct NodeRef {
    pub node_id: String,
    pub conn_name: String,
}

impl From<RawNode> for Node {
    fn from(raw_node: RawNode) -> Self {
        Self {
            node_id: raw_node.node_id,
            task_id: raw_node.task_id,
            configuration: raw_node.configuration,
            info: None,
        }
    }
}

impl NodeRef {
    pub fn new(node_id: String, conn_name: String) -> Self {
        Self { node_id, conn_name }
    }
}

impl Display for NodeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[node_id: {}, conn_name: {}]",
            self.node_id, self.conn_name
        )
    }
}

#[derive(Debug, Clone)]
pub struct Edge {
    pub from: NodeRef,
    pub to: NodeRef,
    /// Not parsed from a RawFlow; created in the process of validation by a Root actor.
    pub edge_type: Option<NegotiatedType>,
}

impl Display for Edge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[from: {}, to: {}]", self.from, self.to)
    }
}

impl From<RawEdge> for Edge {
    fn from(raw_edge: RawEdge) -> Self {
        Self {
            from: raw_edge.from,
            to: raw_edge.to,
            edge_type: None,
        }
    }
}

impl Edge {
    /// Convert an Edge into a Connection. Since a Flow is validated upon construction, we can assume that the Edge's
    /// `to` and `from` references are valid, and that TaskInfo records exist in the Registry for each Node.
    ///
    /// This function is provided on the Edge side as opposed to the Connection side to encapsulate logic related to
    /// parsing and working with Flows, rather than implement `from_edge()` on a Connection.
    pub fn to_connection(&self) -> Connection {
        Connection::new(
            self.edge_type.as_ref().unwrap().clone(),
            self.from.conn_name.clone(),
            self.to.conn_name.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::registry::TaskDefInfo;
    use crate::task_defs::TaskConfig;
    use std::any::TypeId;

    use super::*;

    #[test]
    fn test_invalid_raw_flow_bad_edge() {
        let node1_name = "node-1".to_string();
        let node2_name = "node-2".to_string();
        let nodes = vec![
            RawNode {
                node_id: node1_name.clone(),
                task_id: "one".to_string(),
                configuration: HashMap::new(),
            },
            RawNode {
                node_id: node2_name.clone(),
                task_id: "two".to_string(),
                configuration: HashMap::new(),
            },
        ];
        let edges = vec![RawEdge {
            from: NodeRef::new("nonexistent".to_string(), "output-1".to_string()), // BAD
            to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
        }];

        let raw_flow = RawFlow {
            id: "test".to_string(),
            nodes,
            edges,
        };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }

    #[test]
    fn test_invalid_raw_flow_duplicate_node_id() {
        let node1_name = "node-1".to_string();
        let node2_name = "node-2".to_string();
        let nodes = vec![
            RawNode {
                node_id: node1_name.clone(),
                task_id: "one".to_string(),
                configuration: HashMap::new(),
            },
            RawNode {
                node_id: node1_name.clone(), // BAD
                task_id: "two".to_string(),
                configuration: HashMap::new(),
            },
        ];
        let edges = vec![RawEdge {
            from: NodeRef::new(node1_name, "output-1".to_string()),
            to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
        }];

        let raw_flow = RawFlow {
            id: "test".to_string(),
            nodes,
            edges,
        };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }

    #[test]
    fn test_invalid_raw_flow_hanging_edge_target() {
        let node1_name = "node-1".to_string();
        let node2_name = "node-2".to_string();
        let nodes = vec![
            RawNode {
                node_id: node1_name.clone(),
                task_id: "one".to_string(),
                configuration: HashMap::new(),
            },
            RawNode {
                node_id: node2_name.clone(),
                task_id: "two".to_string(),
                configuration: HashMap::new(),
            },
        ];
        let edges = vec![RawEdge {
            from: NodeRef::new(node1_name, "output-1".to_string()),
            to: NodeRef::new("nonexistent".to_string(), "input-1".to_string()), // BAD
        }];

        let raw_flow = RawFlow {
            id: "test".to_string(),
            nodes,
            edges,
        };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }
    #[test]
    fn test_invalid_raw_flow_hanging_edge_source() {
        let node1_name = "node-1".to_string();
        let node2_name = "node-2".to_string();
        let nodes = vec![
            RawNode {
                node_id: node1_name.clone(),
                task_id: "one".to_string(),
                configuration: HashMap::new(),
            },
            RawNode {
                node_id: node2_name.clone(),
                task_id: "two".to_string(),
                configuration: HashMap::new(),
            },
        ];
        let edges = vec![RawEdge {
            from: NodeRef::new("nonexistent".to_string(), "output-1".to_string()),
            to: NodeRef::new(node2_name, "input-1".to_string()), // BAD
        }];

        let raw_flow = RawFlow {
            id: "test".to_string(),
            nodes,
            edges,
        };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }

    #[test]
    fn test_raw_flow_from_json_valid() {
        let json = r#"
        {
            "id": "test",
            "nodes": [
                {
                    "node_id": "source-1",
                    "task_id": "sequence_source",
                    "configuration": {}
                },
                {
                    "node_id": "sink-1",
                    "task_id": "log_sink",
                    "configuration": {}
                }
            ],
            "edges": [
                {
                    "from": {
                        "node_id": "source-1",
                        "conn_name": "output"
                    },
                    "to": {
                        "node_id": "sink-1",
                        "conn_name": "input"
                    }
                }
            ]
        }
        "#;

        let raw_flow: RawFlow = serde_json::from_str(json).expect("Failed to parse JSON");
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow from JSON result: {:?}", f);
        assert!(f.is_ok());

        let flow = f.unwrap();
        assert_eq!(flow.nodes.len(), 2);
        assert_eq!(flow.edges.len(), 1);
        assert!(flow.nodes.contains_key("source-1"));
        assert!(flow.nodes.contains_key("sink-1"));
    }

    #[test]
    fn test_raw_flow_from_json_invalid() {
        let json = r#"
        {
            "id": "test",
            "nodes": [
                {
                    "node_id": "source-1",
                    "task_id": "sequence_source",
                    "configuration": {}
                }
            ],
            "edges": [
                {
                    "from": {
                        "node_id": "source-1",
                        "conn_name": "output"
                    },
                    "to": {
                        "node_id": "nonexistent-node",
                        "conn_name": "input"
                    }
                }
            ]
        }
        "#;

        let raw_flow: RawFlow = serde_json::from_str(json).expect("Failed to parse JSON");
        let f = Flow::parse_structure(raw_flow);
        println!("Invalid RawFlow from JSON result: {:?}", f);
        assert!(f.is_err());
    }

    #[test]
    fn test_flow_validates_misnamed_inputs() {
        let mut reg = Registry::new();
        reg.add_def(TaskInfo {
            task_id: "sender".to_string(),
            config_tpl: None,
            info: TaskDefInfo::SourceDef {
                outputs: HashMap::from_iter(vec![(
                    "output".to_string(),
                    vec![TypeId::of::<String>()],
                )]),
                build_source: |_: TaskConfig| Box::pin(async { Err(format!("unimplemented")) }),
            },
        });
        reg.add_def(TaskInfo {
            task_id: "receiver".to_string(),
            config_tpl: None,
            info: TaskDefInfo::OperatorDef {
                inputs: HashMap::from_iter(vec![(
                    "input".to_string(),
                    vec![TypeId::of::<i8>(), TypeId::of::<String>()],
                )]),
                outputs: HashMap::from_iter(vec![]),
                build_operator: |_: TaskConfig| Box::pin(async { Err(format!("unimplemented")) }),
            },
        });
        let raw_flow = RawFlow {
            id: "test-flow".to_string(),
            nodes: vec![
                RawNode {
                    node_id: "sender".to_string(),
                    task_id: "sender".to_string(),
                    configuration: HashMap::default(),
                },
                RawNode {
                    node_id: "receiver".to_string(),
                    task_id: "receiver".to_string(),
                    configuration: HashMap::default(),
                },
            ],
            edges: vec![RawEdge {
                from: NodeRef {
                    node_id: "sender".to_string(),
                    conn_name: "output".to_string(),
                },
                to: NodeRef {
                    node_id: "receiver".to_string(),
                    conn_name: "misnamed-input".to_string(),
                },
            }],
        };
        let f = Flow::parse_from(raw_flow, Arc::new(reg));
        assert!(
            f.is_err(),
            "expected a misnamed input connection to produce an error but got {:?}",
            f
        )
    }
    #[test]
    fn test_flow_validates_misnamed_outputs() {
        let mut reg = Registry::new();
        reg.add_def(TaskInfo {
            task_id: "sender".to_string(),
            config_tpl: None,
            info: TaskDefInfo::SourceDef {
                outputs: HashMap::from_iter(vec![(
                    "output".to_string(),
                    vec![TypeId::of::<String>()],
                )]),
                build_source: |_: TaskConfig| Box::pin(async { Err(format!("unimplemented")) }),
            },
        });
        reg.add_def(TaskInfo {
            task_id: "receiver".to_string(),
            config_tpl: None,
            info: TaskDefInfo::OperatorDef {
                inputs: HashMap::from_iter(vec![(
                    "input".to_string(),
                    vec![TypeId::of::<i8>(), TypeId::of::<String>()],
                )]),
                outputs: HashMap::from_iter(vec![]),
                build_operator: |_: TaskConfig| Box::pin(async { Err(format!("unimplemented")) }),
            },
        });
        let raw_flow = RawFlow {
            id: "test-flow".to_string(),
            nodes: vec![
                RawNode {
                    node_id: "sender".to_string(),
                    task_id: "sender".to_string(),
                    configuration: HashMap::default(),
                },
                RawNode {
                    node_id: "receiver".to_string(),
                    task_id: "receiver".to_string(),
                    configuration: HashMap::default(),
                },
            ],
            edges: vec![RawEdge {
                from: NodeRef {
                    node_id: "sender".to_string(),
                    conn_name: "misnamed-output".to_string(),
                },
                to: NodeRef {
                    node_id: "receiver".to_string(),
                    conn_name: "input".to_string(),
                },
            }],
        };
        let f = Flow::parse_from(raw_flow, Arc::new(reg));
        assert!(
            f.is_err(),
            "expected a misnamed output connection to produce an error but got {:?}",
            f
        )
    }
}
