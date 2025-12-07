//! RateLimit operator - throttles event throughput.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use muetl::{
    messages::event::Event,
    task_defs::{
        operator::Operator, ConfigField, ConfigValue, MuetlContext, TaskConfig, TaskConfigTpl,
        TaskDef,
    },
};
use tokio::time::sleep;

/// RateLimit throttles events to a maximum rate.
///
/// Configuration:
/// - `max_per_second` (default: 10): Maximum events per second to allow through
///
/// Events exceeding the rate are delayed (not dropped) to maintain the rate.
pub struct RateLimit {
    min_interval: Duration,
    last_emit: Option<Instant>,
}

impl RateLimit {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let max_per_second = config.get_u64("max_per_second").unwrap_or(10);
        let min_interval = if max_per_second > 0 {
            Duration::from_secs_f64(1.0 / max_per_second as f64)
        } else {
            Duration::ZERO
        };

        Ok(Box::new(RateLimit {
            min_interval,
            last_emit: None,
        }))
    }
}

impl TaskDef for RateLimit {
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![ConfigField::with_default(
                "max_per_second",
                ConfigValue::Uint(10),
            )],
            disallow_unknown_fields: true,
        })
    }
}

#[async_trait]
impl Operator for RateLimit {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name != "input" {
            return;
        }

        // Calculate required delay
        if let Some(last) = self.last_emit {
            let elapsed = last.elapsed();
            if elapsed < self.min_interval {
                sleep(self.min_interval - elapsed).await;
            }
        }

        self.last_emit = Some(Instant::now());

        ctx.results
            .send(Event::new(
                ev.name.clone(),
                "output".to_string(),
                ev.headers.clone(),
                ev.get_data(),
            ))
            .await
            .unwrap();
    }
}
