//! RepeatSource - emits a fixed value repeatedly.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    messages::{event::Event, Status},
    task_defs::{
        source::Source, ConfigField, ConfigType, ConfigValue, MuetlContext, TaskConfig,
        TaskConfigTpl, TaskDef,
    },
};

/// RepeatSource emits a fixed string value a specified number of times.
///
/// Configuration:
/// - `value` (required): The string value to emit
/// - `count` (default: 10): Number of times to emit the value
///
/// Outputs String values on the "output" connection.
pub struct RepeatSource {
    value: String,
    remaining: u64,
    emitted: u64,
}

impl RepeatSource {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        Ok(Box::new(RepeatSource {
            value: config.require_str("value").to_string(),
            remaining: config.get_u64("count").unwrap_or(10),
            emitted: 0,
        }))
    }
}

impl TaskDef for RepeatSource {}

#[async_trait]
impl Source for RepeatSource {
    async fn run(&mut self, ctx: &MuetlContext) {
        if self.remaining == 0 {
            ctx.status.send(Status::Finished).await.unwrap();
        } else {
            ctx.results
                .send(Event::new(
                    format!("repeat-{}", self.emitted),
                    "output".to_string(),
                    HashMap::new(),
                    Arc::new(self.value.clone()),
                ))
                .await
                .unwrap();
            self.remaining -= 1;
            self.emitted += 1;
        }
    }
}
