//! SequenceSource - generates a sequence of integers.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use muetl::{
    impl_config_template, impl_source_handler,
    messages::{event::Event, Status},
    task_defs::{source::Source, MuetlContext, Output, TaskConfig, TaskDef},
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

impl TaskDef for SequenceSource {}

impl Output<i64> for SequenceSource {
    const conn_name: &'static str = "output";
}

impl Source for SequenceSource {
    fn run<'a>(&'a mut self, ctx: &'a MuetlContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
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
        })
    }
}

impl_source_handler!(SequenceSource, task_id = "urn:muetl:source:sequence_source", "output" => i64);
impl_config_template!(
    SequenceSource,
    start: Num = 0,
    end: Num = 10,
    step: Num = 1,
);
