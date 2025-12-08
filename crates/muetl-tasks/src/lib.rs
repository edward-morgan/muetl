//! Common utility tasks for muetl.
//!
//! This crate provides commonly-used daemons, nodes, and sinks that can be
//! registered with a muetl runtime. These are separated from the core muetl
//! crate to keep the core minimal and allow optional inclusion of utilities.

pub mod operators;
pub mod sinks;
pub mod sources;

use muetl::registry::Registry;

/// Register all tasks provided by this crate with the given registry.
///
/// This is a convenience function that registers all sources, sinks, and operators
/// included in muetl-tasks. You can call this to quickly add all standard tasks
/// to your registry, or register individual tasks manually for finer control.
///
/// # Example
///
/// ```ignore
/// use muetl::registry::Registry;
/// use muetl_tasks::register_all;
///
/// let mut registry = Registry::new();
/// register_all(&mut registry);
/// ```
pub fn register_all(registry: &mut Registry) {
    // Register all sources
    registry.register::<sources::SequenceSource>();
    registry.register::<sources::CronSource>();
    registry.register::<sources::RepeatSource>();
    registry.register::<sources::S3ListSource>();

    // Register all sinks
    registry.register::<sinks::LogSink>();
    registry.register::<sinks::DiscardSink>();
    registry.register::<sinks::FileSink>();

    // Register all operators
    registry.register::<operators::Batch>();
    registry.register::<operators::Dedup>();
    registry.register::<operators::Filter>();
    registry.register::<operators::JavaScript>();
    registry.register::<operators::RateLimit>();
}
