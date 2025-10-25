use std::sync::Arc;

use crate::messages::event::Event;

pub mod daemon;
pub mod monitor;
pub mod node;

/// The Message type that internal actors pass around.
pub type EventMessage = Arc<Event>;
