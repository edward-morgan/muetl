use std::collections::{HashMap, HashSet};

use crate::{runtime::connection::Connection, task_defs::TaskConfig};

pub struct RawFlow {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
}

/// The validated version of a RawFlow, parsed from a RawFlow.
pub struct Flow {
    pub nodes: HashMap<String, Node>,
    pub edges: Vec<Edge>,
}

impl TryFrom<RawFlow> for Flow {
    type Error = String;
    fn try_from(value: RawFlow) -> Result<Self, Self::Error> {
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
        Ok(Flow {
            nodes: hm,
            edges: value.edges,
        })
    }
}

pub struct Node {
    /// The internal identifier of this Node as it relates to Edges in the Flow.
    pub node_id: String,
    /// The identifier of the Task that this Node coresponds to.
    pub task_id: String,
    /// The runtime configuration being supplied to this Node. Note that this will be
    /// checked at runtime against the template advertised by the Node, and an error
    /// may be returned if the provided configuration does not match what is required.
    pub configuration: TaskConfig,
    /// The inputs that are being supplied to this Node via connections to other Nodes.
    pub inputs: Vec<NodeRef>,
    /// The outputs that are being supplied to this Node via connections to other Nodes.
    pub outputs: Vec<NodeRef>,
}

pub struct NodeRef {
    pub node_id: String,
    pub conn_name: String,
}

pub struct Edge {
    pub from: NodeRef,
    pub to: NodeRef,
    /// Not parsed from a RawFlow; created in the process of validation by a Root actor.
    pub conn: Connection,
}
