//! Daemon implementations for muetl.
//!
//! Daemons are producers that generate events without requiring input.

pub mod cron_source;
pub mod repeat_source;
pub mod s3_list_source;
pub mod sequence_source;

pub use cron_source::CronSource;
pub use repeat_source::RepeatSource;
pub use s3_list_source::S3ListSource;
pub use sequence_source::SequenceSource;
