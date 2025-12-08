//! File-based log writer that routes logs to separate files per task.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use tokio::sync::broadcast;

use super::{entry::LogEntry, registry::TaskLogRegistry};

/// Manages file-based logging for tasks, routing each task's logs to a separate file.
///
/// # Example
///
/// ```ignore
/// use muetl::logging::{FileLogWriter, global_registry};
///
/// // Create a file writer that writes to ./logs directory
/// let writer = FileLogWriter::new("./logs", global_registry());
///
/// // Subscribe to a task's logs (after the task is registered)
/// writer.subscribe_task(task_id, "my_task");
///
/// // Or use auto-subscribe mode to automatically subscribe to new tasks
/// writer.enable_auto_subscribe();
/// ```
pub struct FileLogWriter {
    /// Base directory where log files are written.
    base_path: PathBuf,
    /// Reference to the task log registry.
    registry: Arc<TaskLogRegistry>,
    /// Active file handles for each task.
    handles: Arc<RwLock<HashMap<u64, TaskFileHandle>>>,
    /// Task names for filename generation.
    task_names: Arc<RwLock<HashMap<u64, String>>>,
}

struct TaskFileHandle {
    writer: BufWriter<File>,
    _abort_handle: tokio::task::AbortHandle,
}

impl FileLogWriter {
    /// Create a new file log writer that writes to the given base directory.
    ///
    /// The directory will be created if it doesn't exist.
    pub fn new<P: AsRef<Path>>(base_path: P, registry: Arc<TaskLogRegistry>) -> std::io::Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        fs::create_dir_all(&base_path)?;

        Ok(Self {
            base_path,
            registry,
            handles: Arc::new(RwLock::new(HashMap::new())),
            task_names: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Subscribe to a task's logs and write them to a file.
    ///
    /// The file will be named `{task_name}_{task_id}.log` in the base directory.
    /// If a subscription already exists for this task, this is a no-op.
    pub fn subscribe_task(&self, task_id: u64, task_name: &str) -> std::io::Result<()> {
        // Check if already subscribed
        {
            let handles = self.handles.read().unwrap();
            if handles.contains_key(&task_id) {
                return Ok(());
            }
        }

        // Get subscription from registry
        let Some(rx) = self.registry.subscribe(task_id) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Task {} is not registered", task_id),
            ));
        };

        // Store task name
        {
            let mut names = self.task_names.write().unwrap();
            names.insert(task_id, task_name.to_string());
        }

        // Create file for this task
        let filename = Self::sanitize_filename(task_name, task_id);
        let file_path = self.base_path.join(filename);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        let writer = BufWriter::new(file);

        // Spawn task to receive logs and write to file
        let handles_clone = Arc::clone(&self.handles);
        let abort_handle = tokio::spawn(Self::log_writer_task(task_id, rx, file_path, handles_clone))
            .abort_handle();

        // Store handle
        {
            let mut handles = self.handles.write().unwrap();
            handles.insert(task_id, TaskFileHandle {
                writer,
                _abort_handle: abort_handle,
            });
        }

        Ok(())
    }

    /// Unsubscribe from a task's logs and close the file.
    pub fn unsubscribe_task(&self, task_id: u64) {
        let mut handles = self.handles.write().unwrap();
        if let Some(mut handle) = handles.remove(&task_id) {
            // Flush before dropping
            let _ = handle.writer.flush();
        }

        let mut names = self.task_names.write().unwrap();
        names.remove(&task_id);
    }

    /// Get the file path for a task's log file.
    pub fn log_file_path(&self, task_id: u64) -> Option<PathBuf> {
        let names = self.task_names.read().unwrap();
        names.get(&task_id).map(|name| {
            let filename = Self::sanitize_filename(name, task_id);
            self.base_path.join(filename)
        })
    }

    /// Get all currently subscribed task IDs.
    pub fn subscribed_tasks(&self) -> Vec<u64> {
        let handles = self.handles.read().unwrap();
        handles.keys().cloned().collect()
    }

    /// Flush all file handles.
    pub fn flush_all(&self) -> std::io::Result<()> {
        let mut handles = self.handles.write().unwrap();
        for handle in handles.values_mut() {
            handle.writer.flush()?;
        }
        Ok(())
    }

    /// Create a sanitized filename from task name and ID.
    fn sanitize_filename(task_name: &str, task_id: u64) -> String {
        let sanitized: String = task_name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        format!("{}_{}.log", sanitized, task_id)
    }

    /// Background task that receives log entries and writes them to a file.
    async fn log_writer_task(
        task_id: u64,
        mut rx: broadcast::Receiver<LogEntry>,
        file_path: PathBuf,
        handles: Arc<RwLock<HashMap<u64, TaskFileHandle>>>,
    ) {
        loop {
            match rx.recv().await {
                Ok(entry) => {
                    let line = Self::format_log_entry(&entry);

                    // Write to file
                    let mut handles = handles.write().unwrap();
                    if let Some(handle) = handles.get_mut(&task_id) {
                        if let Err(e) = writeln!(handle.writer, "{}", line) {
                            eprintln!("Failed to write log entry to {:?}: {}", file_path, e);
                        }
                        // Flush periodically (could be made configurable)
                        let _ = handle.writer.flush();
                    } else {
                        // Handle was removed, exit loop
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    // Channel closed, task is done
                    break;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("Log writer for task {} lagged by {} messages", task_id, n);
                }
            }
        }
    }

    /// Format a log entry as a string for file output.
    fn format_log_entry(entry: &LogEntry) -> String {
        let fields_str = if entry.fields.is_empty() {
            String::new()
        } else {
            let fields: Vec<String> = entry
                .fields
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            format!(" {{{}}}", fields.join(", "))
        };

        format!(
            "{} {} [{}] {}{}",
            entry.timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ"),
            entry.level,
            entry.task_name,
            entry.message,
            fields_str
        )
    }
}

impl Drop for FileLogWriter {
    fn drop(&mut self) {
        // Flush all files on drop
        let _ = self.flush_all();
    }
}
