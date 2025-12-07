//! Filter operator - passes through events that match header conditions.

use std::sync::Arc;

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

    fn matches(&self, ev: &Event) -> bool {
        let header_matches = ev
            .headers
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
        if conn_name == "input" && self.matches(&ev) {
            // Pass through the event unchanged, just update conn_name to "output"
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
}
