//! Batch operator - collects events into groups before emitting.

use std::{
    any::Any,
    collections::HashMap,
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

/// Batch events into groups by count or time window.
///
/// Configuration:
/// - `max_size` (default: 10): Maximum number of events per batch
/// - `max_wait_ms` (default: 1000): Maximum time to wait before emitting a partial batch
///
/// Emits `Vec<Arc<dyn Any + Send + Sync>>` containing the collected event data.
/// Headers from the first event in the batch are used for the output event.
/// Note: This operator is type-agnostic and doesn't use the macro.
pub struct Batch {
    max_size: usize,
    max_wait: Duration,
    buffer: Vec<Arc<dyn Any + Send + Sync>>,
    first_headers: Option<HashMap<String, String>>,
    batch_start: Option<Instant>,
    batch_count: u64,
}

impl Batch {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        Ok(Box::new(Batch {
            max_size: config.get_u64("max_size").unwrap_or(10) as usize,
            max_wait: Duration::from_millis(config.get_u64("max_wait_ms").unwrap_or(1000)),
            buffer: Vec::new(),
            first_headers: None,
            batch_start: None,
            batch_count: 0,
        }))
    }

    fn should_flush(&self) -> bool {
        if self.buffer.len() >= self.max_size {
            return true;
        }
        if let Some(start) = self.batch_start {
            if start.elapsed() >= self.max_wait {
                return true;
            }
        }
        false
    }

    async fn flush(&mut self, ctx: &MuetlContext) {
        if self.buffer.is_empty() {
            return;
        }

        let batch_data: Vec<Arc<dyn Any + Send + Sync>> = self.buffer.drain(..).collect();
        let headers = self.first_headers.take().unwrap_or_default();

        ctx.results
            .send(Event::new(
                format!("batch-{}", self.batch_count),
                "output".to_string(),
                headers,
                Arc::new(batch_data),
            ))
            .await
            .unwrap();

        self.batch_count += 1;
        self.batch_start = None;
    }
}

impl TaskDef for Batch {
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![
                ConfigField::with_default("max_size", ConfigValue::Uint(10)),
                ConfigField::with_default("max_wait_ms", ConfigValue::Uint(1000)),
            ],
            disallow_unknown_fields: true,
        })
    }
}

#[async_trait]
impl Operator for Batch {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name != "input" {
            return;
        }

        // Start timing if this is the first event in the batch
        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
            self.first_headers = ctx.event_headers.clone();
        }

        self.buffer.push(ev.get_data());

        if self.should_flush() {
            self.flush(ctx).await;
        }
    }
}
