use std::sync::Arc;

use crate::messages::event::Event;

/// The internal event type passed around by runtime actors. Contains
/// more information about the runtime environment than users writing
/// functions should need to care about.
pub struct InternalEvent {
    /// The sender's ID as set at runtime when instantiated by the system.
    pub sender_id: u64,
    pub event: Event,
}
