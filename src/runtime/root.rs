use std::{any::TypeId, collections::HashMap, ops::ControlFlow, rc::Rc, sync::Arc};

use kameo::prelude::*;
use kameo_actors::pubsub::PubSub;

use crate::{
    flow::{Edge, Flow, Node, NodeRef},
    messages::StatusUpdate,
    registry::{Registry, TaskDefInfo, TaskInfo},
    util::new_id,
};

use super::{
    connection::{Connection, IncomingConnections, OutgoingConnections},
    daemon_actor::DaemonActor,
    node_actor::NodeActor,
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
    /// The parsed set of Connections that are retrieved from the Flow and passed to actors
    connections: EdgeConnections,
    /// As Actors are instantiated, this keeps track of the IDs that Kameo assigns them and maps them to node_ids in the
    /// Flow. This is primarily used when a supervised Actor dies and needs to be restarted.
    /// Note that the node mapping contains only *active* Nodes.
    actor_node_mapping: HashMap<ActorId, String>,
}

impl Root {
    pub fn new(flow: Flow, monitor_chan: ActorRef<PubSub<StatusUpdate>>) -> Self {
        let edges = flow.edges.clone();
        Self {
            id: new_id(),
            flow,
            monitor_chan,
            connections: EdgeConnections::from(edges),
            actor_node_mapping: HashMap::new(),
        }
    }

    /// Partition nodes into layers by their role in the graph.
    /// Returns layers in spawn order: sinks first, then nodes, then daemons.
    /// This ensures consumers are subscribed before producers start.
    fn partition_nodes_by_layer(&self) -> Vec<Vec<String>> {
        let mut sinks = vec![];
        let mut nodes = vec![];
        let mut daemons = vec![];

        for (node_id, node) in &self.flow.nodes {
            match &node.info.as_ref().unwrap().info {
                TaskDefInfo::SinkDef { .. } => sinks.push(node_id.clone()),
                TaskDefInfo::NodeDef { .. } => nodes.push(node_id.clone()),
                TaskDefInfo::DaemonDef { .. } => daemons.push(node_id.clone()),
            }
        }

        vec![sinks, nodes, daemons]
    }

    /// Given a node to spawn, build it, and return the actor's ID if successful.
    async fn spawn_actor_for_node(
        &self,
        actor_ref: &ActorRef<Root>,
        node_id: &String,
        node: &Node,
    ) -> Result<ActorId, String> {
        match &node.info.as_ref().unwrap().info {
            TaskDefInfo::DaemonDef {
                outputs: _outputs,
                build_daemon,
            } => match build_daemon(&node.configuration) {
                Ok(daemon) => {
                    let r = DaemonActor::new(
                        Some(daemon),
                        self.monitor_chan.clone(),
                        self.connections.outgoing_connections_from(&node_id),
                    );
                    let r = DaemonActor::spawn_link(&actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => {
                    return Err(e);
                }
            },
            TaskDefInfo::SinkDef {
                inputs: _inputs,
                build_sink,
            } => match build_sink(&node.configuration) {
                Ok(sink) => {
                    let r = SinkActor::new(
                        Some(sink),
                        self.monitor_chan.clone(),
                        self.connections.incoming_connections_to(node_id),
                    );
                    let r = SinkActor::spawn_link(&actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => {
                    return Err(e);
                }
            },
            TaskDefInfo::NodeDef {
                inputs: _inputs,
                outputs: _outputs,
                build_node,
            } => match build_node(&node.configuration) {
                Ok(node_impl) => {
                    let r = NodeActor::new(
                        Some(node_impl),
                        self.monitor_chan.clone(),
                        self.connections.incoming_connections_to(node_id),
                        self.connections.outgoing_connections_from(node_id),
                    );
                    let r = NodeActor::spawn_link(&actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => {
                    return Err(e);
                }
            },
        }
    }
}

impl Actor for Root {
    type Args = Self;
    type Error = String;

    /// On startup, the root should instantiate supervised actors for each of the Nodes in the validated Flow it
    /// receives when constructed.
    ///
    /// Actors are spawned in topological order: sinks first, then nodes, then daemons.
    /// This ensures that consumers are subscribed to PubSub channels before producers
    /// start emitting events, preventing race conditions where events are lost.
    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        // Collect layers into owned data to avoid borrow conflicts
        let layers = args.partition_nodes_by_layer();

        // Spawn in order: sinks, then nodes, then daemons
        for layer in layers {
            for node_id in layer {
                let node = args.flow.nodes.get(&node_id).unwrap();
                match args.spawn_actor_for_node(&actor_ref, &node_id, node).await {
                    Ok(actor_id) => {
                        // Create a mapping from the actor ID to the node name in the Flow
                        args.actor_node_mapping.insert(actor_id, node_id);
                    }
                    Err(e) => return Err(e),
                }
            }
            // Yield to allow subscriptions to complete before spawning the next layer
            tokio::task::yield_now().await;
        }
        Ok(args)
    }
    async fn on_stop(
        &mut self,
        _actor_ref: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        println!("Flow complete.");
        Ok(())
    }

    async fn on_link_died(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        id: ActorId,
        reason: ActorStopReason,
    ) -> Result<ControlFlow<ActorStopReason>, Self::Error> {
        match reason {
            ActorStopReason::Normal => match self.actor_node_mapping.remove(&id) {
                Some(_) => {
                    if self.actor_node_mapping.is_empty() {
                        println!("No supervised actors are still active. Root will close...");
                        Ok(ControlFlow::Break(ActorStopReason::Normal))
                    } else {
                        Ok(ControlFlow::Continue(()))
                    }
                }
                None => Err(format!(
                    "supervised actor with id {} stopped but is not in root node mapping",
                    id
                )),
            },
            ActorStopReason::Killed => match self.actor_node_mapping.get(&id) {
                Some(node_id) => {
                    // Get the node from the flow
                    if let Some(node) = self.flow.nodes.get(node_id) {
                        println!("Node {} was killed. Restarting...", node.task_id);
                        match self
                            // TODO: Upgrading here - probably need to be safer
                            .spawn_actor_for_node(&actor_ref.upgrade().unwrap(), node_id, node)
                            .await
                        {
                            Ok(actor_id) => {
                                self.actor_node_mapping.insert(actor_id, node_id.clone());
                                Ok(ControlFlow::Continue(()))
                            }
                            Err(e) => Err(e),
                        }
                    } else {
                        Err(format!(
                            "supervised actor with id {} stopped but is not in root node mapping",
                            id
                        ))
                    }
                }
                None => Err(format!(
                    "supervised actor with id {} stopped but is not in root node mapping",
                    id
                )),
            },
            ActorStopReason::LinkDied { id, reason } => {
                println!("WARNING: Link with actor {} died with reason '{}'; actor is no longer supervised", id, reason);
                Ok(ControlFlow::Continue(()))
            }
            // TODO: this is copied code from Killed above. Deduplicate?
            ActorStopReason::Panicked(err) => match self.actor_node_mapping.get(&id) {
                Some(node_id) => {
                    // Get the node from the flow
                    if let Some(node) = self.flow.nodes.get(node_id) {
                        println!(
                            "Node {} panicked with reason '{}'. Restarting...",
                            err, node.task_id
                        );
                        match self
                            .spawn_actor_for_node(&actor_ref.upgrade().unwrap(), node_id, node)
                            .await
                        {
                            Ok(actor_id) => {
                                self.actor_node_mapping.insert(actor_id, node_id.clone());
                                Ok(ControlFlow::Continue(()))
                            }
                            Err(e) => Err(e),
                        }
                    } else {
                        Err(format!(
                            "supervised actor with id {} stopped but is not in root node mapping",
                            id
                        ))
                    }
                }
                None => Err(format!(
                    "supervised actor with id {} stopped but is not in root node mapping",
                    id
                )),
            },
        }
    }
}

struct EdgeConnections {
    mapping: Vec<(Edge, Connection)>,
}

impl From<Vec<Edge>> for EdgeConnections {
    fn from(edges: Vec<Edge>) -> Self {
        use std::collections::HashMap;
        use crate::flow::NodeRef;

        // Group edges by their source (from NodeRef) so that fan-out edges share the same Connection/PubSub
        let mut conn_by_source: HashMap<NodeRef, Connection> = HashMap::new();
        let mut mapping = vec![];

        for edge in edges {
            let conn = conn_by_source
                .entry(edge.from.clone())
                .or_insert_with(|| edge.to_connection())
                .clone();
            mapping.push((edge, conn));
        }
        EdgeConnections { mapping }
    }
}

impl EdgeConnections {
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
