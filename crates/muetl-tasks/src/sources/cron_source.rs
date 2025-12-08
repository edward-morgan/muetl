//! CronSource - emits trigger events on a cron schedule.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use cron::Schedule;
use muetl::{
    messages::{event::Event, Status},
    task_defs::{
        source::Source, ConfigField, ConfigType, ConfigValue, MuetlContext, TaskConfig,
        TaskConfigTpl, TaskDef,
    },
};
use std::str::FromStr;

/// CronSource emits empty trigger events according to a cron schedule.
///
/// Configuration:
/// - `schedule` (required): Cron expression (e.g., "0 * * * * *" for every minute)
/// - `count` (optional): Maximum number of triggers (default: unlimited/0)
///
/// Outputs unit `()` values on the "output" connection.
pub struct CronSource {
    schedule: Schedule,
    count: u64,
    emitted: u64,
}

impl CronSource {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        let schedule_str = config.require_str("schedule");
        let schedule = Schedule::from_str(schedule_str)
            .map_err(|e| format!("invalid cron expression '{}': {}", schedule_str, e))?;
        let count = config.get_u64("count").unwrap_or(0);

        Ok(Box::new(CronSource {
            schedule,
            count,
            emitted: 0,
        }))
    }
}

impl TaskDef for CronSource {}

#[async_trait]
impl Source for CronSource {
    async fn run(&mut self, ctx: &MuetlContext) {
        // Check if we've hit the count limit (0 means unlimited)
        if self.count > 0 && self.emitted >= self.count {
            ctx.status.send(Status::Finished).await.unwrap();
            return;
        }

        // Find the next scheduled time
        let now = Utc::now();
        let next = match self.schedule.upcoming(Utc).next() {
            Some(next) => next,
            None => {
                ctx.status.send(Status::Finished).await.unwrap();
                return;
            }
        };

        // Sleep until the next scheduled time
        let duration = (next - now).to_std().unwrap_or_default();
        tokio::time::sleep(duration).await;

        // Emit the trigger event
        ctx.results
            .send(Event::new(
                format!("cron-{}", self.emitted),
                "output".to_string(),
                HashMap::new(),
                Arc::new(()),
            ))
            .await
            .unwrap();

        self.emitted += 1;
    }
}
