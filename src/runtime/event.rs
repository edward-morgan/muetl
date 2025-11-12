use std::sync::Arc;

use crate::messages::event::Event;

/// The internal event type passed around by runtime actors. Contains
/// more information about the runtime environment than users writing
/// functions should need to care about.
pub struct InternalEvent {
    /// The sender's ID as set at runtime when instantiated by the system.
    /// A sender_id is composed of the unique ID of the sending actor + an ID representing the connection it's being sent on.
    pub sender_id: u64,
    pub event: Arc<Event>,
}

// TODOs:
// 1. InternalEvent needs to be published by anything that has outputs, NOT Event. This will make the Sink work as expected.
// 2. Runtime needs to publish sender IDs
// 3. Test with a mock Sink
