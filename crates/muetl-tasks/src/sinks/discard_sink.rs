//! DiscardSink - consumes and discards all events.

use std::{any::TypeId, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    messages::event::Event,
    registry::{SelfDescribing, TaskDefInfo, TaskInfo},
    task_defs::{sink::Sink, ConfigTemplate, MuetlSinkContext, TaskConfig, TaskDef},
};

/// DiscardSink consumes all events without doing anything.
///
/// Useful for:
/// - Benchmarking sources/operators without sink overhead
/// - Terminating pipelines where output isn't needed
/// - Testing flow configurations
///
/// No configuration options.
/// Note: This sink is type-agnostic and doesn't use the macro.
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

impl ConfigTemplate for DiscardSink {}

impl SelfDescribing for DiscardSink {
    fn task_info() -> TaskInfo {
        let mut inputs = HashMap::new();
        // DiscardSink accepts any type on "input"
        // We'll use an empty list to indicate it accepts any type
        inputs.insert("input".to_string(), vec![]);

        TaskInfo {
            task_id: "urn:rdp:transformer:muetl:discard_sink".to_string(),
            config_tpl: <Self as ConfigTemplate>::config_template(),
            info: TaskDefInfo::SinkDef {
                inputs,
                build_sink: Self::new,
            },
        }
    }
}

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
