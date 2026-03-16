use std::{collections::HashMap, sync::Mutex};

use kameo::{
    actor::{self, ActorRef},
    prelude::Message,
    Actor,
};
use kameo_actors::pubsub::{PubSub, Subscribe};
use tokio::task;

use crate::{
    flow,
    messages::{
        GetRuntimeInfo, RegisterRuntimeInfo, RetrieveStatus, RuntimeInfo, Status, StatusUpdate,
    },
};

type TaskUid = String;

#[derive(Actor)]
pub struct Monitor {
    /// The current state of all known Tasks, referenced by their internal IDs.
    records: Mutex<HashMap<u64, MonitorRecord>>,
    /// A mapping of each known Flow's ID to the node ID of each Task in it.
    /// This is used by external-facing components that don't know about the internal ID.
    ///
    /// Since a Node ID is only unique in the context of a single Flow, we have to store
    /// them on a per-Flow basis. If you don't know the internal ID, getting the current
    /// state of a Task requires knowing **both** its parent Flow's `id` and its
    /// `node_id`.
    flows_to_node_ids: Mutex<HashMap<String, Vec<String>>>,
}

impl Monitor {
    pub fn new() -> Self {
        return Self {
            records: Mutex::new(HashMap::new()),
            flows_to_node_ids: Mutex::new(HashMap::new()),
        };
    }

    /// Updates the flows_to_node_ids mapping with `flow_id` and `node_id`, creating a new `flow_id`
    /// mapping if necessary.
    fn insert_flow_node_mapping(&mut self, flow_id: String, node_id: String) {
        let mut flows_to_node_ids = self.flows_to_node_ids.lock().unwrap();
        if let Some(nodes_in_flow) = flows_to_node_ids.get_mut(&flow_id) {
            if !nodes_in_flow.contains(&node_id) {
                nodes_in_flow.push(node_id.clone());
            }
        } else {
            flows_to_node_ids.insert(flow_id.to_string(), vec![node_id.to_string()]);
        }
    }

    /// Attempt to find the `MonitorRecord` for a node with the given `node_id` in a flow
    /// with the given `flow_id`.
    fn record_for_node(&self, flow_id: String, node_id: String) -> Option<MonitorRecord> {
        for record in self.records.lock().unwrap().values() {
            if record.flow_id == flow_id && record.node_id == node_id {
                return Some(record.clone());
            }
        }
        None
    }
}

impl Message<StatusUpdate> for Monitor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: StatusUpdate,
        _: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(record) = self.records.lock().unwrap().get_mut(&msg.id) {
            record.statuses.push(msg.status);
        } else {
            tracing::error!(
                id = msg.id,
                "Monitor does not contain a record for this Task"
            )
        }
    }
}

impl Message<RetrieveStatus> for Monitor {
    type Reply = Option<Status>;
    async fn handle(
        &mut self,
        msg: RetrieveStatus,
        _: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(rec) = self.records.lock().unwrap().get_mut(&msg.id) {
            rec.statuses.last().cloned()
        } else {
            None
        }
    }
}

impl Message<GetRuntimeInfo> for Monitor {
    type Reply = Option<RuntimeInfo>;
    async fn handle(
        &mut self,
        msg: GetRuntimeInfo,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.record_for_node(msg.flow_id, msg.node_id)
            .map(|r| r.into())
    }
}

impl Message<RegisterRuntimeInfo> for Monitor {
    type Reply = ();
    async fn handle(
        &mut self,
        msg: RegisterRuntimeInfo,
        ctx: &mut kameo::prelude::Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let mut rec = self.records.lock().unwrap();
        if rec.contains_key(&msg.task_id) {
            tracing::error!(
                "Monitor already contains a runtime record for node with ID = {}",
                msg.task_id
            )
        } else {
            tracing::info!(info = ?msg, "Monitor registering new runtime task");
            rec.insert(
                msg.task_id,
                MonitorRecord {
                    flow_id: msg.flow_id.clone(),
                    id: msg.task_id,
                    task_def_id: msg.task_def_id.clone(),
                    node_id: msg.node_id.clone(),
                    statuses: vec![],
                },
            );
            drop(rec);
            self.insert_flow_node_mapping(msg.flow_id, msg.node_id);
        }
    }
}

#[derive(Clone)]
struct MonitorRecord {
    /// The ID of the Flow that this Task is a part of.
    pub flow_id: String,
    /// The ID of this Task as it has been instantiated in the system. Multiple instances of the
    /// same TaskDef will have different `id`s.
    pub id: u64,
    /// The ID of this TaskDef as it has been registered with the system. Multiple instances of
    /// the same TaskDef will have the same `task_def_id`.
    pub task_def_id: String,
    /// The ID of the Task as it has been instantiated as part of a Flow. This ID is unique in
    /// the context of a single Flow, but not globally.
    pub node_id: String,
    /// All status records received for this Task, with the earliest being first.
    pub statuses: Vec<Status>,
}

impl Into<RuntimeInfo> for MonitorRecord {
    fn into(self) -> RuntimeInfo {
        RuntimeInfo {
            task_id: self.id,
            task_def_id: self.task_def_id,
            node_id: self.node_id,
            current_status: self.statuses.last().cloned(),
        }
    }
}
