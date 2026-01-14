//! Test task definitions for integration tests.
//!
//! This module provides simple task definitions useful for testing:
//! - `NumberSource`: A Source that emits a configurable sequence of numbers
//! - `Adder`: An Operator that adds a constant to each input number
//! - `Multiplier`: An Operator that multiplies each input number by a constant
//! - `ResultCollector`: A Sink that collects results for assertion

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use muetl::{
    impl_operator_handler, impl_sink_handler,
    messages::event::Event,
    task_defs::{
        source::Source, Input, MuetlContext, MuetlSinkContext, SinkInput, TaskConfig, TaskDef,
    },
};

// ----------------------------------------------------------------------------
// NumberSource - A Source that emits numbers
// ----------------------------------------------------------------------------

pub struct NumberSource {
    current: i64,
    max: i64,
}

impl NumberSource {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        let max = config
            .get("count")
            .and_then(config_value_to_i64)
            .unwrap_or(5);
        Ok(Box::new(NumberSource { current: 0, max }))
    }
}

impl TaskDef for NumberSource {}

#[async_trait]
impl Source for NumberSource {
    async fn run(&mut self, ctx: &MuetlContext) {
        if self.current >= self.max {
            ctx.status
                .send(muetl::messages::Status::Finished)
                .await
                .unwrap();
        } else {
            ctx.results
                .send(Event::new(
                    format!("number-{}", self.current),
                    "output".to_string(),
                    HashMap::new(),
                    Arc::new(self.current),
                ))
                .await
                .unwrap();
            self.current += 1;
        }
    }
}

// ----------------------------------------------------------------------------
// Adder - An Operator that adds a constant to input numbers
// ----------------------------------------------------------------------------

pub struct Adder {
    addend: i64,
}

impl Adder {
    pub fn new(
        config: &TaskConfig,
    ) -> Result<Box<dyn muetl::task_defs::operator::Operator>, String> {
        let addend = config
            .get("addend")
            .and_then(config_value_to_i64)
            .unwrap_or(1);
        Ok(Box::new(Adder { addend }))
    }
}

impl TaskDef for Adder {}

impl Input<i64> for Adder {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, ctx: &MuetlContext, value: &i64) {
        let result = value + self.addend;
        ctx.results
            .send(Event::new(
                format!("{}-plus-{}", ctx.event_name.as_ref().unwrap(), self.addend),
                "output".to_string(),
                HashMap::new(),
                Arc::new(result),
            ))
            .await
            .unwrap();
    }
}

impl_operator_handler!(Adder, "input" => i64);

// ----------------------------------------------------------------------------
// Multiplier - An Operator that multiplies input numbers by a constant
// ----------------------------------------------------------------------------

pub struct Multiplier {
    factor: i64,
}

impl Multiplier {
    pub fn new(
        config: &TaskConfig,
    ) -> Result<Box<dyn muetl::task_defs::operator::Operator>, String> {
        let factor = config
            .get("factor")
            .and_then(config_value_to_i64)
            .unwrap_or(2);
        Ok(Box::new(Multiplier { factor }))
    }
}

impl TaskDef for Multiplier {}

impl Input<i64> for Multiplier {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, ctx: &MuetlContext, value: &i64) {
        let result = value * self.factor;
        ctx.results
            .send(Event::new(
                format!("{}-times-{}", ctx.event_name.as_ref().unwrap(), self.factor),
                "output".to_string(),
                HashMap::new(),
                Arc::new(result),
            ))
            .await
            .unwrap();
    }
}

impl_operator_handler!(Multiplier, "input" => i64);

// ----------------------------------------------------------------------------
// ResultCollector - A Sink that collects results for assertion
// ----------------------------------------------------------------------------

use once_cell::sync::Lazy;
use std::sync::Mutex;

/// Global storage for collected results, keyed by collector name.
/// This allows tests to retrieve results after the flow completes.
pub static COLLECTED_RESULTS: Lazy<Mutex<HashMap<String, Vec<i64>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub struct ResultCollector {
    name: String,
}

impl ResultCollector {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn muetl::task_defs::sink::Sink>, String> {
        let name = config
            .get("name")
            .and_then(config_value_to_string)
            .unwrap_or_else(|| "default".to_string());

        // Initialize the results vector for this collector
        COLLECTED_RESULTS
            .lock()
            .unwrap()
            .insert(name.clone(), Vec::new());

        Ok(Box::new(ResultCollector { name }))
    }

    /// Retrieve collected results for a given collector name.
    pub fn get_results(name: &str) -> Vec<i64> {
        COLLECTED_RESULTS
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .unwrap_or_default()
    }

    /// Clear all collected results (call between tests).
    pub fn clear_all() {
        COLLECTED_RESULTS.lock().unwrap().clear();
    }
}

impl TaskDef for ResultCollector {}

impl SinkInput<i64> for ResultCollector {
    const conn_name: &'static str = "input";
    async fn handle(&mut self, _ctx: &MuetlSinkContext, value: &i64) {
        COLLECTED_RESULTS
            .lock()
            .unwrap()
            .entry(self.name.clone())
            .or_default()
            .push(*value);
    }
}

impl_sink_handler!(ResultCollector, "input" => i64);

// ----------------------------------------------------------------------------
// Helper for extracting i64 from ConfigValue
// ----------------------------------------------------------------------------

fn config_value_to_i64(value: &muetl::task_defs::ConfigValue) -> Option<i64> {
    match value {
        muetl::task_defs::ConfigValue::Num(i) => Some(*i),
        _ => None,
    }
}

fn config_value_to_string(value: &muetl::task_defs::ConfigValue) -> Option<String> {
    match value {
        muetl::task_defs::ConfigValue::Str(s) => Some(s.clone()),
        _ => None,
    }
}
