//! DiscardSink - consumes and discards all events.

use std::sync::Arc;

use async_trait::async_trait;
use muetl::{
    messages::event::Event,
    task_defs::{sink::Sink, MuetlSinkContext, TaskConfig, TaskDef},
};

/// DiscardSink consumes all events without doing anything.
///
/// Useful for:
/// - Benchmarking sources/operators without sink overhead
/// - Terminating pipelines where output isn't needed
/// - Testing flow configurations
///
/// No configuration options.
pub struct DiscardSink {
    count: u64,
}

impl DiscardSink {
    pub fn new(_config: &TaskConfig) -> Result<Box<dyn Sink>, String> {
        Ok(Box::new(DiscardSink { count: 0 }))
    }

    /// Returns the number of events discarded.
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl TaskDef for DiscardSink {}

#[async_trait]
impl Sink for DiscardSink {
    async fn handle_event_for_conn(
        &mut self,
        _ctx: &MuetlSinkContext,
        conn_name: &String,
        _ev: Arc<Event>,
    ) {
        if conn_name == "input" {
            self.count += 1;
        }
    }
}
