use std::{any::TypeId, collections::HashMap, fmt::Display, hash::Hash, sync::Arc};

use crate::{
    registry::{Registry, TaskDefInfo, TaskInfo},
    runtime::{connection::Connection, NegotiatedType},
    task_defs::ConfigValue,
};

type ValidationResult = Result<(), Vec<String>>;

pub struct RawFlow {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

/// The validated version of a RawFlow, parsed from a RawFlow.
#[derive(Debug)]
pub struct Flow {
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
            match hm.insert(raw_node.node_id.clone(), raw_node) {
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
            nodes: hm,
            edges: value.edges,
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
            if let Some(mut negotiated_types) = outputs_of(task_info, &edge_source.conn_name) {
                // For each edge, get the supported types of the input and ensure that at least one is available
                for edge in &edges {
                    // unwrap() is okay here as this has already been parsed/validated
                    let dest = self.nodes.get(&edge.to.node_id).unwrap();
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
            tracing::debug!("Resolved Flow: {:?}", self);
            Ok(())
        }
    }
}

/// Retrieves the outputs of a given TaskInfo for a conn_name in it. If the conn_name doesn't exist, or if the
/// TaskInfo doesn't have outputs (for example, it's a Sink), then None is returned. Otherwise, the output type
/// vector is copied and returned.
fn outputs_of(task_info: &TaskInfo, conn_name: &String) -> Option<Vec<TypeId>> {
    match &task_info.info {
        TaskDefInfo::SourceDef { outputs, .. } => outputs.get(conn_name).cloned(),
        TaskDefInfo::OperatorDef { outputs, .. } => outputs.get(conn_name).cloned(),
        TaskDefInfo::SinkDef { .. } => None,
    }
}
/// Retrieves the inputs of a given TaskInfo for a conn_name in it. If the conn_name doesn't exist, or if the
/// TaskInfo doesn't have inputs (for example, it's a Source), then None is returned. Otherwise, the input type
/// vector is copied and returned.
fn inputs_of(task_info: &TaskInfo, conn_name: &String) -> Option<Vec<TypeId>> {
    match &task_info.info {
        TaskDefInfo::SinkDef { inputs, .. } => inputs.get(conn_name).cloned(),
        TaskDefInfo::OperatorDef { inputs, .. } => inputs.get(conn_name).cloned(),
        TaskDefInfo::SourceDef { .. } => None,
    }
}
#[derive(Debug)]
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct NodeRef {
    pub node_id: String,
    pub conn_name: String,
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
    use super::*;

    #[test]
    fn test_invalid_raw_flow_bad_edge() {
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
            from: NodeRef::new("nonexistent".to_string(), "output-1".to_string()), // BAD
            to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
            edge_type: None,
        }];

        let raw_flow = RawFlow { nodes, edges };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }

    #[test]
    fn test_invalid_raw_flow_duplicate_node_id() {
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
                node_id: node1_name.clone(), // BAD
                task_id: "two".to_string(),
                configuration: HashMap::new(),
                info: None,
            },
        ];
        let edges = vec![Edge {
            from: NodeRef::new(node1_name, "output-1".to_string()),
            to: NodeRef::new(node2_name.clone(), "input-1".to_string()),
            edge_type: None,
        }];

        let raw_flow = RawFlow { nodes, edges };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }

    #[test]
    fn test_invalid_raw_flow_hanging_edge_target() {
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
            from: NodeRef::new(node1_name, "output-1".to_string()),
            to: NodeRef::new("nonexistent".to_string(), "input-1".to_string()), // BAD
            edge_type: None,
        }];

        let raw_flow = RawFlow { nodes, edges };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }
    #[test]
    fn test_invalid_raw_flow_hanging_edge_source() {
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
            from: NodeRef::new("nonexistent".to_string(), "output-1".to_string()),
            to: NodeRef::new(node2_name, "input-1".to_string()), // BAD
            edge_type: None,
        }];

        let raw_flow = RawFlow { nodes, edges };
        let f = Flow::parse_structure(raw_flow);
        println!("RawFlow result: {:?}", f);
        assert!(f.is_err());
    }
}
