//! Log entry structure for per-task logging.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use tracing::Level;

/// A single log entry from a task.
#[derive(Debug, Clone)]
pub struct LogEntry {
    /// When the log was recorded.
    pub timestamp: DateTime<Utc>,
    /// Log level (ERROR, WARN, INFO, DEBUG, TRACE).
    pub level: Level,
    /// The trace ID shared by all tasks in the same Root.
    pub trace_id: u64,
    /// The unique task ID that produced this log.
    pub task_id: u64,
    /// Human-readable task name.
    pub task_name: String,
    /// The log message.
    pub message: String,
    /// Additional structured fields from the log event.
    pub fields: HashMap<String, String>,
}

impl LogEntry {
    pub fn new(
        level: Level,
        trace_id: u64,
        task_id: u64,
        task_name: String,
        message: String,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            level,
            trace_id,
            task_id,
            task_name,
            message,
            fields: HashMap::new(),
        }
    }

    pub fn with_fields(mut self, fields: HashMap<String, String>) -> Self {
        self.fields = fields;
        self
    }
}
