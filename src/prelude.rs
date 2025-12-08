//! Prelude module for convenient imports when building Sources, Operators, and Sinks.
//!
//! This module re-exports the most commonly needed types and traits for implementing
//! muetl task definitions. Use it with:
//!
//! ```ignore
//! use muetl::prelude::*;
//! ```

// Re-export async_trait for the required attribute macro
pub use async_trait::async_trait;

// Std types commonly needed
pub use std::collections::HashMap;
pub use std::sync::Arc;

// Messages
pub use crate::messages::event::Event;
pub use crate::messages::Status;

// Task definition traits
pub use crate::task_defs::operator::Operator;
pub use crate::task_defs::sink::Sink;
pub use crate::task_defs::source::Source;
pub use crate::task_defs::TaskDef;

// Input/Output traits
pub use crate::task_defs::Input;
pub use crate::task_defs::Output;
pub use crate::task_defs::SinkInput;

// Context types
pub use crate::task_defs::MuetlContext;
pub use crate::task_defs::MuetlSinkContext;

// Configuration types
pub use crate::task_defs::ConfigField;
pub use crate::task_defs::ConfigType;
pub use crate::task_defs::ConfigValue;
pub use crate::task_defs::TaskConfig;
pub use crate::task_defs::TaskConfigTpl;

// Macros
pub use crate::impl_operator_handler;
pub use crate::impl_sink_handler;
