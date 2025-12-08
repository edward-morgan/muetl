//! Common utility tasks for muetl.
//!
//! This crate provides commonly-used daemons, nodes, and sinks that can be
//! registered with a muetl runtime. These are separated from the core muetl
//! crate to keep the core minimal and allow optional inclusion of utilities.

pub mod nodes;
pub mod sinks;
pub mod sources;
