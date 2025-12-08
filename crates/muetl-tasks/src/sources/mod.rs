//! Daemon implementations for muetl.
//!
//! Daemons are producers that generate events without requiring input.

pub mod repeat_source;
pub mod sequence_source;

pub use repeat_source::RepeatSource;
pub use sequence_source::SequenceSource;
