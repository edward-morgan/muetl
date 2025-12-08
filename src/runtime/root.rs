use std::{any::TypeId, collections::HashMap, ops::ControlFlow, rc::Rc, sync::Arc};

use kameo::prelude::*;
use kameo_actors::pubsub::PubSub;

use crate::{
    flow::{Edge, Flow, Node, NodeRef},
    logging::FileLogWriter,
    messages::StatusUpdate,
    registry::{Registry, TaskDefInfo, TaskInfo},
    task_defs::TaskConfig,
    util::new_id,
};

use super::{
    connection::{Connection, IncomingConnections, OutgoingConnections},
    operator_actor::OperatorActor,
    sink_actor::SinkActor,
    source_actor::SourceActor,
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
    /// Optional file log writer for routing task logs to separate files.
    file_log_writer: Option<Arc<FileLogWriter>>,
    /// Mapping from node_id to task_id for file logging subscriptions.
    node_task_mapping: HashMap<String, u64>,
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
            file_log_writer: None,
            node_task_mapping: HashMap::new(),
        }
    }

    /// Enable file-based logging for all tasks in this Root.
    ///
    /// Each task's logs will be written to a separate file in the specified directory.
    /// Files are named `{task_name}_{task_id}.log`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use muetl::runtime::root::Root;
    ///
    /// let root = Root::new(flow, monitor_chan)
    ///     .with_file_logging("./logs")?;
    /// ```
    pub fn with_file_logging<P: AsRef<std::path::Path>>(
        mut self,
        log_dir: P,
    ) -> std::io::Result<Self> {
        use crate::logging::global_registry;
        let writer = FileLogWriter::new(log_dir, global_registry())?;
        self.file_log_writer = Some(Arc::new(writer));
        Ok(self)
    }

    /// Enable file-based logging with a custom FileLogWriter.
    pub fn with_file_log_writer(mut self, writer: Arc<FileLogWriter>) -> Self {
        self.file_log_writer = Some(writer);
        self
    }

    /// Partition nodes into layers by their role in the graph.
    /// Returns layers in spawn order: sinks first, then operators, then sources.
    /// This ensures consumers are subscribed before producers start.
    fn partition_nodes_by_layer(&self) -> Vec<Vec<String>> {
        let mut sinks = vec![];
        let mut operators = vec![];
        let mut sources = vec![];

        for (node_id, node) in &self.flow.nodes {
            match &node.info.as_ref().unwrap().info {
                TaskDefInfo::SinkDef { .. } => sinks.push(node_id.clone()),
                TaskDefInfo::OperatorDef { .. } => operators.push(node_id.clone()),
                TaskDefInfo::SourceDef { .. } => sources.push(node_id.clone()),
            }
        }

        vec![sinks, operators, sources]
    }

    /// Validate and resolve configuration for a node against its template.
    /// If the node has no config template, passes through the raw config values.
    fn resolve_config(&self, node: &Node) -> Result<TaskConfig, String> {
        let task_info = node.info.as_ref().unwrap();
        match &task_info.config_tpl {
            Some(tpl) => tpl
                .validate(node.configuration.clone())
                .map_err(|errors| errors.join("; ")),
            None => Ok(TaskConfig::new(node.configuration.clone())),
        }
    }

    /// Given a node to spawn, build it, and return (actor_id, task_id) if successful.
    async fn spawn_actor_for_node(
        &self,
        actor_ref: &ActorRef<Root>,
        node_id: &String,
        node: &Node,
        task_id: u64,
    ) -> Result<ActorId, String> {
        let config = self.resolve_config(node)?;

        let build_result = match &node.info.as_ref().unwrap().info {
            TaskDefInfo::SourceDef {
                outputs: _outputs,
                build_source,
            } => match build_source(&config) {
                Ok(source) => {
                    let r = SourceActor::with_task_id(
                        task_id,
                        self.id,
                        node_id.clone(),
                        Some(source),
                        self.monitor_chan.clone(),
                        self.connections.outgoing_connections_from(&node_id),
                    );
                    let r = SourceActor::spawn_link(&actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => Err(e),
            },
            TaskDefInfo::SinkDef {
                inputs: _inputs,
                build_sink,
            } => match build_sink(&config) {
                Ok(sink) => {
                    let r = SinkActor::with_task_id(
                        task_id,
                        self.id,
                        node_id.clone(),
                        Some(sink),
                        self.monitor_chan.clone(),
                        self.connections.incoming_connections_to(node_id),
                    );
                    let r = SinkActor::spawn_link(&actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => Err(e),
            },
            TaskDefInfo::OperatorDef {
                inputs: _inputs,
                outputs: _outputs,
                build_operator,
            } => match build_operator(&config) {
                Ok(operator) => {
                    let r = OperatorActor::with_task_id(
                        task_id,
                        self.id,
                        node_id.clone(),
                        Some(operator),
                        self.monitor_chan.clone(),
                        self.connections.incoming_connections_to(node_id),
                        self.connections.outgoing_connections_from(node_id),
                    );
                    let r = OperatorActor::spawn_link(&actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => Err(e),
            },
        };
        if build_result.is_ok() {
            // Subscribe this task to file logging if enabled
            if let Some(ref writer) = self.file_log_writer {
                if let Err(e) = writer.subscribe_task(task_id, node_id) {
                    tracing::warn!(
                        node_id = %node_id,
                        task_id = task_id,
                        error = %e,
                        "Failed to subscribe task to file logging"
                    );
                }
            }
        }
        build_result
    }
}

impl Actor for Root {
    type Args = Self;
    type Error = String;

    /// On startup, the root should instantiate supervised actors for each of the Nodes in the validated Flow it
    /// receives when constructed.
    ///
    /// Actors are spawned in topological order: sinks first, then nodes, then sources.
    /// This ensures that consumers are subscribed to PubSub channels before producers
    /// start emitting events, preventing race conditions where events are lost.
    async fn on_start(
        mut args: Self::Args,
        actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        // Collect layers into owned data to avoid borrow conflicts
        let layers = args.partition_nodes_by_layer();

        // Spawn in order: sinks, then nodes, then sources
        for layer in layers {
            for node_id in layer {
                let node = args.flow.nodes.get(&node_id).unwrap();
                // Generate a task ID for this node
                let task_id = new_id();
                args.node_task_mapping.insert(node_id.clone(), task_id);

                match args
                    .spawn_actor_for_node(&actor_ref, &node_id, node, task_id)
                    .await
                {
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
        tracing::info!(root_id = self.id, "Flow complete");
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
                        tracing::info!(
                            root_id = self.id,
                            "No supervised actors are still active; Root will close"
                        );
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
                        // Get the existing task_id for this node (or generate a new one)
                        let task_id = self
                            .node_task_mapping
                            .get(node_id)
                            .copied()
                            .unwrap_or_else(new_id);
                        tracing::warn!(root_id = self.id, node_id = %node_id, task_id = task_id, "Node was killed; restarting");
                        match self
                            // TODO: Upgrading here - probably need to be safer
                            .spawn_actor_for_node(
                                &actor_ref.upgrade().unwrap(),
                                node_id,
                                node,
                                task_id,
                            )
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
                tracing::warn!(root_id = self.id, actor_id = %id, reason = %reason, "Link with actor died; actor is no longer supervised");
                Ok(ControlFlow::Continue(()))
            }
            // TODO: this is copied code from Killed above. Deduplicate?
            ActorStopReason::Panicked(err) => match self.actor_node_mapping.get(&id) {
                Some(node_id) => {
                    // Get the node from the flow
                    if let Some(node) = self.flow.nodes.get(node_id) {
                        // Get the existing task_id for this node (or generate a new one)
                        let task_id = self
                            .node_task_mapping
                            .get(node_id)
                            .copied()
                            .unwrap_or_else(new_id);
                        tracing::error!(root_id = self.id, node_id = %node_id, task_id = task_id, error = %err, "Node panicked; restarting");
                        match self
                            .spawn_actor_for_node(
                                &actor_ref.upgrade().unwrap(),
                                node_id,
                                node,
                                task_id,
                            )
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
        use crate::flow::NodeRef;
        use std::collections::HashMap;

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
