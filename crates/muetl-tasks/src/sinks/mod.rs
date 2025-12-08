//! Sink implementations for muetl.
//!
//! Sinks are consumers that receive events without producing output.

pub mod discard_sink;
pub mod file_sink;
pub mod log_sink;

pub use discard_sink::DiscardSink;
pub use file_sink::FileSink;
pub use log_sink::LogSink;
