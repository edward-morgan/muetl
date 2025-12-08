//! Filter operator - passes through events that match header conditions.

use std::{any::TypeId, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    impl_config_template,
    messages::event::Event,
    registry::{SelfDescribing, TaskDefInfo, TaskInfo},
    task_defs::{operator::Operator, ConfigTemplate, MuetlContext, TaskConfig, TaskDef},
};

/// Comparison operator for filter conditions.
#[derive(Debug, Clone, Default)]
pub enum CompareOp {
    /// Equal (string or numeric)
    #[default]
    Eq,
    /// Not equal (string or numeric)
    Ne,
    /// Less than (numeric)
    Lt,
    /// Less than or equal (numeric)
    Le,
    /// Greater than (numeric)
    Gt,
    /// Greater than or equal (numeric)
    Ge,
}

impl CompareOp {
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "eq" => Ok(CompareOp::Eq),
            "ne" => Ok(CompareOp::Ne),
            "lt" => Ok(CompareOp::Lt),
            "le" => Ok(CompareOp::Le),
            "gt" => Ok(CompareOp::Gt),
            "ge" => Ok(CompareOp::Ge),
            _ => Err(format!(
                "invalid operator '{}': expected one of eq, ne, lt, le, gt, ge",
                s
            )),
        }
    }
}

/// Filter events based on header key/value matching.
///
/// Configuration:
/// - `header_key` (required): The header key to check
/// - `header_value` (required): The value to match against
/// - `op` (default: "eq"): Comparison operator - one of: eq, ne, lt, le, gt, ge
/// - `invert` (default: false): If true, pass events that do NOT match
///
/// For numeric comparisons (lt, le, gt, ge), both the header value and
/// `header_value` config are parsed as integers. If parsing fails, the
/// condition evaluates to false.
///
/// Events that match (or don't match if inverted) are passed through unchanged.
/// Note: This operator is type-agnostic and doesn't use the macro.
pub struct Filter {
    header_key: String,
    header_value: String,
    op: CompareOp,
    invert: bool,
}

impl Filter {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let op = config
            .get_str("op")
            .map(CompareOp::from_str)
            .transpose()?
            .unwrap_or_default();

        Ok(Box::new(Filter {
            header_key: config.require_str("header_key").to_string(),
            header_value: config.require_str("header_value").to_string(),
            op,
            invert: config.get_bool("invert").unwrap_or(false),
        }))
    }

    fn matches(&self, headers: &HashMap<String, String>) -> bool {
        let header_matches = headers
            .get(&self.header_key)
            .map(|actual| self.compare(actual))
            .unwrap_or(false);

        if self.invert {
            !header_matches
        } else {
            header_matches
        }
    }

    fn compare(&self, actual: &str) -> bool {
        match self.op {
            CompareOp::Eq => actual == self.header_value,
            CompareOp::Ne => actual != self.header_value,
            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge => {
                self.compare_numeric(actual)
            }
        }
    }

    fn compare_numeric(&self, actual: &str) -> bool {
        let Ok(actual_num) = actual.parse::<i64>() else {
            return false;
        };
        let Ok(expected_num) = self.header_value.parse::<i64>() else {
            return false;
        };

        match self.op {
            CompareOp::Lt => actual_num < expected_num,
            CompareOp::Le => actual_num <= expected_num,
            CompareOp::Gt => actual_num > expected_num,
            CompareOp::Ge => actual_num >= expected_num,
            _ => unreachable!(),
        }
    }
}

impl TaskDef for Filter {}

impl SelfDescribing for Filter {
    fn task_info() -> TaskInfo {
        let mut inputs = HashMap::new();
        inputs.insert("input".to_string(), vec![]);

        let mut outputs = HashMap::new();
        outputs.insert("output".to_string(), vec![]);

        TaskInfo {
            task_id: "urn:rdp:transformer:muetl:filter".to_string(),
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

impl_config_template!(
    Filter,
    header_key: Str!,
    header_value: Str!,
    op: Str = "eq",
    invert: Bool = false,
);
