pub mod event;

/// Tasks can send Status messages to report their current progress and state.
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum SystemEvent {
    /// Inform the task that it should shut down immediately. This is commonly sent to stop downstream
    /// Tasks for a pipeline that is driven by an upstream Source or Daemon that has stopped.
    Shutdown,
}
