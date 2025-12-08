//! Dedup operator - suppresses consecutive duplicate events.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    messages::event::Event,
    task_defs::{
        operator::Operator, ConfigField, ConfigType, MuetlContext, TaskConfig, TaskConfigTpl,
        TaskDef,
    },
};

/// Dedup suppresses consecutive duplicate events based on a header value.
///
/// Configuration:
/// - `dedup_key` (required): The header key to use for deduplication
///
/// Events are passed through only if their dedup_key header value differs
/// from the previous event's value. Events without the dedup_key header
/// are always passed through.
/// Note: This operator is type-agnostic and doesn't use the macro.
pub struct Dedup {
    dedup_key: String,
    last_value: Option<String>,
}

impl Dedup {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        Ok(Box::new(Dedup {
            dedup_key: config.require_str("dedup_key").to_string(),
            last_value: None,
        }))
    }
}

impl TaskDef for Dedup {
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![ConfigField::required("dedup_key", ConfigType::Str)],
            disallow_unknown_fields: true,
        })
    }
}

#[async_trait]
impl Operator for Dedup {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name != "input" {
            return;
        }

        let headers = ctx.event_headers.as_ref().cloned().unwrap_or_default();
        let current_value = headers.get(&self.dedup_key).cloned();

        let should_emit = match (&self.last_value, &current_value) {
            // No dedup key in event - always emit
            (_, None) => true,
            // First event with this key - emit
            (None, Some(_)) => true,
            // Compare with previous
            (Some(last), Some(current)) => last != current,
        };

        if should_emit {
            self.last_value = current_value;
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
}
