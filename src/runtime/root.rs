use std::{collections::HashMap, ops::ControlFlow, sync::Arc};

use kameo::prelude::*;

use crate::{
    flow::{Edge, Flow, Node},
    logging::FileLogWriter,
    messages::RegisterRuntimeInfo,
    registry::TaskDefInfo,
    runtime::{error::RuntimeError, monitor_actor::Monitor},
    task_defs::TaskConfig,
    util::new_id,
};

use super::{
    connection::{Connection, IncomingConnections, OutgoingConnections},
    operator_actor::OperatorActor,
    sink_actor::SinkActor,
    source_actor::SourceActor,
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
    /// A ref to the monitor, used to register Tasks with it and for Tasks to communicate status updates to.
    monitor: ActorRef<Monitor>,
}

impl Root {
    pub fn new(flow: Flow, monitor: ActorRef<Monitor>) -> Self {
        let edges = flow.edges.clone();
        Self {
            id: new_id(),
            flow,
            connections: EdgeConnections::from(edges),
            actor_node_mapping: HashMap::new(),
            file_log_writer: None,
            node_task_mapping: HashMap::new(),
            monitor,
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
    fn resolve_config(&self, node: &Node) -> Result<TaskConfig, RuntimeError> {
        let task_info = node.info.as_ref().unwrap();
        match &task_info.config_tpl {
            Some(tpl) => tpl
                .validate(node.configuration.clone())
                .map_err(RuntimeError::ConfigResolutionError),
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
    ) -> Result<ActorId, RuntimeError> {
        let config = self.resolve_config(node)?;

        let build_result = match &node.info.as_ref().unwrap().info {
            TaskDefInfo::SourceDef {
                outputs: _outputs,
                build_source,
            } => match build_source(config).await {
                Ok(source) => {
                    let r = SourceActor::with_task_id(
                        task_id,
                        self.id,
                        node_id.clone(),
                        Some(source),
                        self.monitor.clone(),
                        self.connections.outgoing_connections_from(node_id),
                    );
                    let r = SourceActor::spawn_link(actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => Err(e),
            },
            TaskDefInfo::SinkDef {
                inputs: _inputs,
                build_sink,
            } => match build_sink(config).await {
                Ok(sink) => {
                    let r = SinkActor::with_task_id(
                        task_id,
                        self.id,
                        node_id.clone(),
                        Some(sink),
                        self.monitor.clone(),
                        self.connections.incoming_connections_to(node_id),
                    );
                    let r = SinkActor::spawn_link(actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => Err(e),
            },
            TaskDefInfo::OperatorDef {
                inputs: _inputs,
                outputs: _outputs,
                build_operator,
            } => match build_operator(config).await {
                Ok(operator) => {
                    let r = OperatorActor::with_task_id(
                        task_id,
                        self.id,
                        node_id.clone(),
                        Some(operator),
                        self.monitor.clone(),
                        self.connections.incoming_connections_to(node_id),
                        self.connections.outgoing_connections_from(node_id),
                    );
                    let r = OperatorActor::spawn_link(actor_ref, r).await;
                    Ok(r.id())
                }
                Err(e) => Err(e),
            },
        };

        match build_result {
            Ok(actor_id) => {
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
                Ok(actor_id)
            }
            Err(e) => Err(RuntimeError::FailedToBuildTaskError {
                node_id: node_id.clone(),
                flow_id: self.flow.id.clone(),
                msg: e,
            }),
        }
    }
}

impl Actor for Root {
    type Args = Self;
    type Error = RuntimeError;

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

        tracing::info!(layers = ?layers, "Starting nodes by layer.");
        // Spawn in order: sinks, then nodes, then sources
        for layer in layers {
            for node_id in layer {
                let node = args.flow.nodes.get(&node_id).unwrap();
                // Generate a task ID for this node
                let task_id = new_id();
                args.node_task_mapping.insert(node_id.clone(), task_id);

                // Before starting it, send a message to the Monitor that will register the newly-created Task
                let info = RegisterRuntimeInfo {
                    flow_id: args.flow.id.clone(),
                    task_id,
                    // Note that task_def_id in RegisterRuntimeInfo maps to the task_id in a Flow.
                    task_def_id: node.task_id.clone(),
                    node_id: node.node_id.clone(),
                };
                match args.monitor.tell(info.clone()).await {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(RuntimeError::MonitorRegistrationError(info.clone(), e));
                    }
                }

                tracing::info!(node_id = node_id, task_id = task_id, "Starting node.");

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
        tracing::info!(actor_node_mapping = ?args.actor_node_mapping, "Startup complete.");
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
                    // Wait for the monitor to reflect the finished status
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
                None => Err(RuntimeError::UnknownSupervisedActorStoppedError(id)),
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
                        Err(RuntimeError::UnknownSupervisedActorStoppedError(id))
                    }
                }
                None => Err(RuntimeError::UnknownSupervisedActorStoppedError(id)),
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
                        Err(RuntimeError::UnknownSupervisedActorStoppedError(id))
                    }
                }
                None => Err(RuntimeError::UnknownSupervisedActorStoppedError(id)),
            },
        }
    }
}

/// Tracks the mapping from an `Edge` in a `Flow` to the underlying runtime `Connection` where results will be sent.
///
/// The mapping between an `Edge` and a `Connection` is 1-1, as the `Connection` contains some of the same information
/// as an `Edge` (namely the receiver's conn_name). However, multiple `Connection`s may utilize the same underlying
/// `ChannelImpl`.
struct EdgeConnections {
    mapping: Vec<(Edge, Arc<Connection>)>,
}

impl From<Vec<Edge>> for EdgeConnections {
    fn from(edges: Vec<Edge>) -> Self {
        use crate::flow::NodeRef;
        use std::collections::HashMap;

        let mut mapping = vec![];
        let mut by_node_ref: HashMap<NodeRef, Vec<Arc<Connection>>> = HashMap::new();
        for edge in edges {
            let et = edge
                .edge_type
                .clone()
                .expect("flow was not property initialized - types have not been negotiated!");
            // If there are existing Connections for this edge's outgoing NodeRef, look through them to see if the types match
            // and we can reuse the ChannelImpl.
            if let Some(existing_conns) = by_node_ref.get_mut(&edge.from) {
                let new_conn = match existing_conns
                    .iter()
                    .find(|existing| *existing.chan_type == et)
                {
                    Some(existing) => Arc::new(Connection::with_channel(
                        et.clone(),
                        existing.connection_key,
                        edge.from.conn_name.clone(),
                        edge.to.conn_name.clone(),
                        existing.chan_ref.clone(),
                    )),
                    None => Arc::new(Connection::new(
                        et.clone(),
                        edge.from.conn_name.clone(),
                        edge.to.conn_name.clone(),
                    )),
                };

                existing_conns.push(new_conn.clone());
                mapping.push((edge, new_conn.clone()));
            } else {
                let new_conn = Arc::new(Connection::new(
                    et.clone(),
                    edge.from.conn_name.clone(),
                    edge.to.conn_name.clone(),
                ));
                by_node_ref.insert(edge.from.clone(), vec![new_conn.clone()]);
                mapping.push((edge, new_conn.clone()));
            }
        }
        EdgeConnections { mapping }
    }
}

impl EdgeConnections {
    pub fn outgoing_connections_from(&self, edge_node_id: &String) -> OutgoingConnections {
        // This will return every connection from the given edge_node_id; this includes Connections that
        // have the same ConnectionKey (which will also use the same underlying channel). We rely on
        // OutgoingConnections::from to correctly parse that distinction.
        let conns: Vec<Arc<Connection>> = self
            .mapping
            .iter()
            .filter(|(edge, _conn)| edge.from.node_id == *edge_node_id)
            .map(|(_edge, conn)| conn.clone())
            .collect();
        OutgoingConnections::from(&conns)
    }
    pub fn incoming_connections_to(&self, edge_node_id: &String) -> IncomingConnections {
        let conns = self
            .mapping
            .iter()
            .flat_map(|(e, c)| {
                if e.to.node_id == *edge_node_id {
                    Some(c.clone())
                } else {
                    None
                }
            })
            .collect();
        IncomingConnections::from(&conns)
    }
}
