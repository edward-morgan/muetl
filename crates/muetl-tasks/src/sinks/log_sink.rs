//! LogSink - a sink that logs all received events.
//!
//! Accepts any primitive type, Vec, HashMap, or unit `()` on the "input" connection.
//! Event headers are included in the log output.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use muetl::messages::event::Event;
use muetl::task_defs::sink::Sink;
use muetl::task_defs::*;
use serde_json::Value as JsonValue;

pub struct LogSink {}

impl LogSink {
    pub fn new(_config: &TaskConfig) -> Result<Box<dyn Sink>, String> {
        Ok(Box::new(LogSink {}))
    }
}

impl TaskDef for LogSink {}

#[async_trait]
impl Sink for LogSink {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlSinkContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name != "input" {
            return;
        }

        let data = ev.get_data();
        let value_str = format_value(&data);

        // Format headers for logging
        let headers_str = if ctx.event_headers.is_empty() {
            String::from("{}")
        } else {
            format!("{:?}", ctx.event_headers)
        };

        tracing::info!(
            event_name = %ctx.event_name,
            headers = %headers_str,
            value = %value_str,
            "LogSink received"
        );
    }
}

/// Format a value for logging, handling common types.
fn format_value(data: &Arc<dyn std::any::Any + Send + Sync>) -> String {
    // Unit type
    if data.downcast_ref::<()>().is_some() {
        return String::from("()");
    }

    // Primitive types
    if let Some(v) = data.downcast_ref::<String>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<&str>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<i8>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<i16>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<i32>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<i64>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<u8>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<u16>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<u32>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<u64>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<f32>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<f64>() {
        return v.to_string();
    }
    if let Some(v) = data.downcast_ref::<bool>() {
        return v.to_string();
    }

    // JSON value
    if let Some(v) = data.downcast_ref::<JsonValue>() {
        return v.to_string();
    }

    // Vec types
    if let Some(v) = data.downcast_ref::<Vec<String>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<i64>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<u64>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<i32>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<u32>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<f64>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<bool>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<Vec<JsonValue>>() {
        return format!("{:?}", v);
    }

    // HashMap types
    if let Some(v) = data.downcast_ref::<HashMap<String, String>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<HashMap<String, i64>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<HashMap<String, u64>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<HashMap<String, f64>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<HashMap<String, bool>>() {
        return format!("{:?}", v);
    }
    if let Some(v) = data.downcast_ref::<HashMap<String, JsonValue>>() {
        return format!("{:?}", v);
    }

    // Fallback for unknown types
    String::from("<unknown type>")
}
