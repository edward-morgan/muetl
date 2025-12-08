//! Per-task logging infrastructure.
//!
//! This module provides structured, per-task logging using the `tracing` ecosystem.
//! Each task's logs are routed to separate broadcast channels, enabling:
//!
//! - Real-time log streaming (e.g., via WebSocket)
//! - Per-task log isolation
//! - Trace ID correlation across tasks in the same flow
//! - File-based logging with per-task log files
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      tracing Subscriber                          │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  ┌──────────────┐  ┌──────────────┐                             │
//! │  │ Console Layer│  │TaskLog Layer │                             │
//! │  │ (runtime)    │  │(per-task)    │                             │
//! │  └──────────────┘  └──────────────┘                             │
//! └─────────────────────────────────────────────────────────────────┘
//!                             ▲
//!                             │ spans: trace_id, task_id, task_name
//!                             │
//! ┌─────────────────────────────────────────────────────────────────┐
//! │  Task Code: tracing::info!("message");                          │
//! └─────────────────────────────────────────────────────────────────┘
//!                             │
//!                             ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │  FileLogWriter (optional): ./logs/{task_name}_{task_id}.log     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ## Initialization (in main or runtime setup)
//!
//! ```ignore
//! use muetl::logging;
//!
//! // Initialize the logging system
//! logging::init();
//! ```
//!
//! ## File-based Logging
//!
//! The easiest way to enable file logging is via the Root:
//!
//! ```ignore
//! use muetl::runtime::root::Root;
//!
//! let root = Root::new(flow, monitor_chan)
//!     .with_file_logging("./logs")?;
//! ```
//!
//! This will create a separate log file for each task in the flow, named
//! `{task_name}_{task_id}.log`. Log entries include timestamps, log levels,
//! and structured fields.
//!
//! For more control, you can use FileLogWriter directly:
//!
//! ```ignore
//! use muetl::logging::{FileLogWriter, global_registry};
//!
//! let writer = FileLogWriter::new("./logs", global_registry())?;
//! writer.subscribe_task(task_id, "my_task")?;
//! ```
//!
//! ## In Task Actors
//!
//! ```ignore
//! use tracing::Instrument;
//!
//! // Register the task
//! logging::global_registry().register_task(task_id);
//!
//! // Wrap task execution in a span
//! let span = tracing::info_span!(
//!     "task",
//!     trace_id = root_id,
//!     task_id = task_id,
//!     task_name = "my_task",
//! );
//!
//! async {
//!     task.run(&ctx).await;
//! }
//! .instrument(span)
//! .await;
//! ```
//!
//! ## In Task Code
//!
//! ```ignore
//! // Use tracing macros instead of println!
//! tracing::info!("processing event");
//! tracing::debug!(count = 42, "batch complete");
//! tracing::warn!("buffer nearly full");
//! ```
//!
//! ## Subscribing to Task Logs
//!
//! ```ignore
//! let registry = logging::global_registry();
//! if let Some(mut rx) = registry.subscribe(task_id) {
//!     while let Ok(entry) = rx.recv().await {
//!         println!("[{}] {}: {}", entry.task_name, entry.level, entry.message);
//!     }
//! }
//! ```

pub mod entry;
pub mod file_writer;
pub mod layer;
pub mod registry;

pub use entry::LogEntry;
pub use file_writer::FileLogWriter;
pub use layer::TaskLogLayer;
pub use registry::{global_registry, TaskLogRegistry};

use std::sync::Arc;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize the logging system with default configuration.
///
/// This sets up:
/// - A console layer for runtime logs (filtered by RUST_LOG env var)
/// - The TaskLogLayer for per-task log routing
///
/// Call this once at application startup.
pub fn init() {
    init_with_registry(global_registry());
}

/// Initialize the logging system with a custom registry.
pub fn init_with_registry(registry: Arc<TaskLogRegistry>) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().with_target(true))
        .with(TaskLogLayer::new(registry))
        .init();
}

/// Initialize the logging system without the console layer.
/// Useful for testing or when you want to capture all output programmatically.
pub fn init_task_logging_only() {
    init_task_logging_only_with_registry(global_registry());
}

/// Initialize task logging only with a custom registry.
pub fn init_task_logging_only_with_registry(registry: Arc<TaskLogRegistry>) {
    tracing_subscriber::registry()
        .with(TaskLogLayer::new(registry))
        .init();
}
