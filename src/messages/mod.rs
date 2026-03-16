pub mod event;

/// Tasks can send Status messages to report their current progress and state.
#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Progress(f32),
    Active,
    Finished,
    Failed(String),
}

/// The message type that is sent to a Monitor instance to register a status update
#[derive(Clone)]
pub struct StatusUpdate {
    pub id: u64,
    pub status: Status,
}

pub struct RetrieveStatus {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub enum Metric {
    Gauge(f64),
    Counter(u32),
}

/// SystemEvents are internal messages sent by the Muetl runtime to inform actors of changes they should make to their state.
#[derive(Debug, Clone)]
pub enum SystemEvent {
    /// Inform the task that it should shut down immediately. This is commonly sent to stop downstream
    /// Tasks for a pipeline that is driven by an upstream Source or Daemon that has stopped.
    Shutdown,
}

/// Request to receive the latest runtime information for the node with the given ID.
pub struct GetRuntimeInfo {
    /// The ID of the Flow that contains the node.
    pub flow_id: String,
    /// The ID of the node as it exists in its parent Flow.
    ///
    /// Note that this is **not** the internal-facing `id` of the Task.
    pub node_id: String,
}

/// Represents the current state of a single Task in the system, as understood by the Monitor.
#[derive(Debug)]
pub struct RuntimeInfo {
    /// The ID of this Task as it has been instantiated in the system. Multiple instances of the
    /// same TaskDef will have different ids.
    pub task_id: u64,
    /// The ID of this TaskDef as it has been registered with the system. Multiple instances of
    /// the same TaskDef will have the same task_id.
    pub task_def_id: String,
    /// The human-readable name of this Task. Note that this may not be unique across instances of
    /// the same TaskDef.
    pub node_id: String,
    /// The most recent status for this Task received by the Monitor.
    pub current_status: Option<Status>,
}

/// Request that the Monitor registers a new RuntimeInfo.
#[derive(Debug, Clone)]
pub struct RegisterRuntimeInfo {
    /// The ID of the Flow that this Task is a part of.
    pub flow_id: String,
    /// The ID of this Task as it has been instantiated in the system.
    pub task_id: u64,
    /// The ID of this Task's referenced TaskDef.
    pub task_def_id: String,
    /// The ID of this Task as it exists as a Node in the Flow referenced by
    /// `flow_id`.
    pub node_id: String,
}
