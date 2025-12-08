//! Task log registry - manages per-task broadcast channels.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::sync::broadcast;

use super::entry::LogEntry;

/// Default capacity for per-task broadcast channels.
const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

/// Registry that manages per-task log broadcast channels.
///
/// Each task gets its own broadcast channel that can have multiple subscribers.
/// This enables real-time streaming of logs to WebSocket clients, file writers, etc.
pub struct TaskLogRegistry {
    /// Map of task_id -> broadcast sender.
    channels: RwLock<HashMap<u64, broadcast::Sender<LogEntry>>>,
    /// Capacity for new channels.
    channel_capacity: usize,
}

impl TaskLogRegistry {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            channel_capacity: capacity,
        }
    }

    /// Register a new task, creating its broadcast channel.
    /// Returns the sender for the task's channel.
    pub fn register_task(&self, task_id: u64) -> broadcast::Sender<LogEntry> {
        let mut channels = self.channels.write().unwrap();
        if let Some(sender) = channels.get(&task_id) {
            return sender.clone();
        }

        let (tx, _rx) = broadcast::channel(self.channel_capacity);
        channels.insert(task_id, tx.clone());
        tx
    }

    /// Unregister a task, removing its broadcast channel.
    pub fn unregister_task(&self, task_id: u64) {
        let mut channels = self.channels.write().unwrap();
        channels.remove(&task_id);
    }

    /// Subscribe to a task's log stream.
    /// Returns None if the task is not registered.
    pub fn subscribe(&self, task_id: u64) -> Option<broadcast::Receiver<LogEntry>> {
        let channels = self.channels.read().unwrap();
        channels.get(&task_id).map(|tx| tx.subscribe())
    }

    /// Get the sender for a task's channel.
    /// Returns None if the task is not registered.
    pub fn get_sender(&self, task_id: u64) -> Option<broadcast::Sender<LogEntry>> {
        let channels = self.channels.read().unwrap();
        channels.get(&task_id).cloned()
    }

    /// Publish a log entry to a task's channel.
    /// Returns the number of receivers that received the message.
    /// Returns 0 if the task is not registered or has no subscribers.
    pub fn publish(&self, task_id: u64, entry: LogEntry) -> usize {
        let channels = self.channels.read().unwrap();
        if let Some(tx) = channels.get(&task_id) {
            tx.send(entry).unwrap_or(0)
        } else {
            0
        }
    }

    /// Get a list of all registered task IDs.
    pub fn registered_tasks(&self) -> Vec<u64> {
        let channels = self.channels.read().unwrap();
        channels.keys().cloned().collect()
    }
}

impl Default for TaskLogRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global task log registry instance.
/// This is used by the tracing layer to route log events.
static GLOBAL_REGISTRY: once_cell::sync::Lazy<Arc<TaskLogRegistry>> =
    once_cell::sync::Lazy::new(|| Arc::new(TaskLogRegistry::new()));

/// Get the global task log registry.
pub fn global_registry() -> Arc<TaskLogRegistry> {
    GLOBAL_REGISTRY.clone()
}
