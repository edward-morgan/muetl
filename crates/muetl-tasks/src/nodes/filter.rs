//! Filter operator - passes through events that match header conditions.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    messages::event::Event,
    task_defs::{
        operator::Operator, ConfigField, ConfigType, ConfigValue, MuetlContext, TaskConfig,
        TaskConfigTpl, TaskDef,
    },
};

/// Filter events based on header key/value matching.
///
/// Configuration:
/// - `header_key` (required): The header key to check
/// - `header_value` (required): The value to match against
/// - `invert` (default: false): If true, pass events that do NOT match
///
/// Events that match (or don't match if inverted) are passed through unchanged.
/// Note: This operator is type-agnostic and doesn't use the macro.
pub struct Filter {
    header_key: String,
    header_value: String,
    invert: bool,
}

impl Filter {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        Ok(Box::new(Filter {
            header_key: config.require_str("header_key").to_string(),
            header_value: config.require_str("header_value").to_string(),
            invert: config.get_bool("invert").unwrap_or(false),
        }))
    }

    fn matches(&self, headers: &HashMap<String, String>) -> bool {
        let header_matches = headers
            .get(&self.header_key)
            .map(|v| v == &self.header_value)
            .unwrap_or(false);

        if self.invert {
            !header_matches
        } else {
            header_matches
        }
    }
}

impl TaskDef for Filter {
    fn task_config_tpl(&self) -> Option<TaskConfigTpl> {
        Some(TaskConfigTpl {
            fields: vec![
                ConfigField::required("header_key", ConfigType::Str),
                ConfigField::required("header_value", ConfigType::Str),
                ConfigField::with_default("invert", ConfigValue::Bool(false)),
            ],
            disallow_unknown_fields: true,
        })
    }
}

#[async_trait]
impl Operator for Filter {
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
        if self.matches(&headers) {
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
