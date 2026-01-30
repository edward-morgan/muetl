//! Example demonstrating an operator that accepts multiple types for the same input connection.
//!
//! This example shows how to use the updated impl_operator_handler! macro to create
//! an operator that can handle multiple types on the same input connection. The macro
//! will try each type in order until one matches.
//!
//! New bracket syntax:
//! - Single type: "conn" => Type
//! - Multiple types: "conn" => [Type1, Type2, ...]
//! - Can mix both styles in the same macro invocation

use std::{collections::HashMap, sync::Arc};

use muetl::{
    impl_config_template, impl_operator_handler,
    messages::event::Event,
    task_defs::{operator::Operator, Input, MuetlContext, TaskConfig, TaskDef},
};

/// An operator that can accept either serde_json::Value or String on the same input.
/// It will try to downcast to serde_json::Value first, then String.
pub struct FlexibleOperator {
    prefix: String,
}

impl FlexibleOperator {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let prefix = config
            .get_str("prefix")
            .unwrap_or("processed")
            .to_string();
        Ok(Box::new(FlexibleOperator { prefix }))
    }
}

impl TaskDef for FlexibleOperator {}

// Handle serde_json::Value inputs
impl Input<serde_json::Value> for FlexibleOperator {
    const conn_name: &'static str = "data";

    async fn handle(&mut self, ctx: &MuetlContext, value: &serde_json::Value) {
        println!("Handling JSON value: {}", value);

        ctx.results
            .send(Event::new(
                format!("{}-json", self.prefix),
                "output".to_string(),
                HashMap::new(),
                Arc::new(format!("Processed JSON: {}", value)),
            ))
            .await
            .unwrap();
    }
}

// Handle String inputs
impl Input<String> for FlexibleOperator {
    const conn_name: &'static str = "data";

    async fn handle(&mut self, ctx: &MuetlContext, value: &String) {
        println!("Handling string value: {}", value);

        ctx.results
            .send(Event::new(
                format!("{}-string", self.prefix),
                "output".to_string(),
                HashMap::new(),
                Arc::new(format!("Processed String: {}", value)),
            ))
            .await
            .unwrap();
    }
}

// This macro now supports multiple types for the same input connection!
// Use bracket syntax to specify multiple types. It will try each type in order.
impl_operator_handler!(
    FlexibleOperator,
    task_id = "flexible_operator",
    inputs(
        "data" => [serde_json::Value, String],  // Try these types in order
    ),
    outputs("output" => String)
);

impl_config_template!(
    FlexibleOperator,
    prefix: Str = "processed",
);

fn main() {
    println!("This is an example demonstrating multi-type input support.");
    println!("The FlexibleOperator can handle both serde_json::Value and String");
    println!("on the same 'data' input connection.");
    println!();
    println!("You can also mix single and multiple types:");
    println!("  inputs(");
    println!("    \"data\" => [serde_json::Value, String],  // Multiple types");
    println!("    \"config\" => i64,                        // Single type");
    println!("  )");
}
