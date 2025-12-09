//! RateLimit operator - throttles event throughput.

use std::{
    any::TypeId,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use muetl::{
    impl_config_template,
    messages::event::Event,
    registry::{SelfDescribing, TaskDefInfo, TaskInfo},
    task_defs::{operator::Operator, ConfigTemplate, MuetlContext, TaskConfig, TaskDef},
};
use tokio::time::sleep;

/// RateLimit throttles events to a maximum rate.
///
/// Configuration:
/// - `max_per_second` (default: 10): Maximum events per second to allow through
///
/// Events exceeding the rate are delayed (not dropped) to maintain the rate.
/// Note: This operator is type-agnostic and doesn't use the macro.
pub struct RateLimit {
    min_interval: Duration,
    last_emit: Option<Instant>,
}

impl RateLimit {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let max_per_second = config.get_i64("max_per_second").unwrap_or(10);
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

impl TaskDef for RateLimit {}

impl SelfDescribing for RateLimit {
    fn task_info() -> TaskInfo {
        let mut inputs = HashMap::new();
        inputs.insert("input".to_string(), vec![]);

        let mut outputs = HashMap::new();
        outputs.insert("output".to_string(), vec![]);

        TaskInfo {
            task_id: "urn:rdp:transformer:muetl:rate_limit".to_string(),
            config_tpl: <Self as ConfigTemplate>::config_template(),
            info: TaskDefInfo::OperatorDef {
                inputs,
                outputs,
                build_operator: Self::new,
            },
        }
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

        let headers = ctx.event_headers.clone().unwrap_or_default();
        ctx.results
            .send(Event::new(
                ctx.event_name.clone().unwrap_or_default(),
                "output".to_string(),
                headers,
                ev.get_data(),
            ))
            .await
            .unwrap();
    }
}

impl_config_template!(
    RateLimit,
    max_per_second: Num = 10,
);
