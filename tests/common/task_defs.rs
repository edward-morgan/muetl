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
    messages::event::Event,
    task_defs::{
        operator::Operator, sink::Sink, source::Source, MuetlContext, MuetlSinkContext, TaskConfig, TaskDef,
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
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let addend = config
            .get("addend")
            .and_then(config_value_to_i64)
            .unwrap_or(1);
        Ok(Box::new(Adder { addend }))
    }
}

impl TaskDef for Adder {}

#[async_trait]
impl Operator for Adder {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name == "input" {
            if let Some(value) = ev.get_data().downcast_ref::<i64>() {
                let result = value + self.addend;
                ctx.results
                    .send(Event::new(
                        format!("{}-plus-{}", ev.name, self.addend),
                        "output".to_string(),
                        HashMap::new(),
                        Arc::new(result),
                    ))
                    .await
                    .unwrap();
            }
        }
    }
}

// ----------------------------------------------------------------------------
// Multiplier - An Operator that multiplies input numbers by a constant
// ----------------------------------------------------------------------------

pub struct Multiplier {
    factor: i64,
}

impl Multiplier {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Operator>, String> {
        let factor = config
            .get("factor")
            .and_then(config_value_to_i64)
            .unwrap_or(2);
        Ok(Box::new(Multiplier { factor }))
    }
}

impl TaskDef for Multiplier {}

#[async_trait]
impl Operator for Multiplier {
    async fn handle_event_for_conn(
        &mut self,
        ctx: &MuetlContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name == "input" {
            if let Some(value) = ev.get_data().downcast_ref::<i64>() {
                let result = value * self.factor;
                ctx.results
                    .send(Event::new(
                        format!("{}-times-{}", ev.name, self.factor),
                        "output".to_string(),
                        HashMap::new(),
                        Arc::new(result),
                    ))
                    .await
                    .unwrap();
            }
        }
    }
}

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
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Sink>, String> {
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

#[async_trait]
impl Sink for ResultCollector {
    async fn handle_event_for_conn(
        &mut self,
        _ctx: &MuetlSinkContext,
        conn_name: &String,
        ev: Arc<Event>,
    ) {
        if conn_name == "input" {
            if let Some(value) = ev.get_data().downcast_ref::<i64>() {
                COLLECTED_RESULTS
                    .lock()
                    .unwrap()
                    .entry(self.name.clone())
                    .or_default()
                    .push(*value);
            }
        }
    }
}

// ----------------------------------------------------------------------------
// Helper for extracting i64 from TaskConfigValue
// ----------------------------------------------------------------------------

fn config_value_to_i64(value: &muetl::task_defs::TaskConfigValue) -> Option<i64> {
    match value {
        muetl::task_defs::TaskConfigValue::Int(i) => Some(*i),
        muetl::task_defs::TaskConfigValue::Uint(u) => Some(*u as i64),
        _ => None,
    }
}

fn config_value_to_string(value: &muetl::task_defs::TaskConfigValue) -> Option<String> {
    match value {
        muetl::task_defs::TaskConfigValue::Str(s) => Some(s.clone()),
        _ => None,
    }
}
