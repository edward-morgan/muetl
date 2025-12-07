//! SequenceSource - generates a sequence of integers.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    messages::{event::Event, Status},
    task_defs::{
        source::Source, ConfigField, ConfigValue, MuetlContext, TaskConfig, TaskConfigTpl,
        TaskDef,
    },
};

/// SequenceSource emits a configurable sequence of integers.
///
/// Configuration:
/// - `start` (default: 0): Starting value (inclusive)
/// - `end` (default: 10): Ending value (exclusive)
/// - `step` (default: 1): Increment between values
///
/// Outputs i64 values on the "output" connection.
pub struct SequenceSource {
    current: i64,
    end: i64,
    step: i64,
}

impl SequenceSource {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        let start = config.get_i64("start").unwrap_or(0);
        let end = config.get_i64("end").unwrap_or(10);
        let step = config.get_i64("step").unwrap_or(1);

        if step == 0 {
            return Err("step cannot be zero".to_string());
        }

        Ok(Box::new(SequenceSource {
            current: start,
            end,
            step,
        }))
    }

    fn is_done(&self) -> bool {
        if self.step > 0 {
            self.current >= self.end
        } else {
            self.current <= self.end
        }
    }
}

impl TaskDef for SequenceSource {
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![
                ConfigField::with_default("start", ConfigValue::Int(0)),
                ConfigField::with_default("end", ConfigValue::Int(10)),
                ConfigField::with_default("step", ConfigValue::Int(1)),
            ],
            disallow_unknown_fields: true,
        })
    }
}

#[async_trait]
impl Source for SequenceSource {
    async fn run(&mut self, ctx: &MuetlContext) {
        if self.is_done() {
            ctx.status.send(Status::Finished).await.unwrap();
        } else {
            ctx.results
                .send(Event::new(
                    format!("seq-{}", self.current),
                    "output".to_string(),
                    HashMap::new(),
                    Arc::new(self.current),
                ))
                .await
                .unwrap();
            self.current += self.step;
        }
    }
}
