pub mod connection;
pub mod daemon_actor;
pub mod event;
pub mod monitor_actor;
pub mod node_actor;
pub mod root;
pub mod sink_actor;

use kameo::prelude::*;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    future::Future,
    sync::Arc,
};

use crate::runtime::{connection::OutgoingConnections, event::InternalEvent};
use crate::{messages::event::Event, task_defs::OutputType};

/// The Message type that internal actors pass around.
pub type EventMessage = Arc<InternalEvent>;

#[derive(Debug)]
pub enum NegotiatedType {
    Singleton(TypeId),
    AllOf(Vec<TypeId>),
}

impl NegotiatedType {
    // The set of events passed in is validated against the negotiated type:
    // 1. If a singleton type, all events must be of that type.
    // 2. If an AllOf type, then the list of events must contain exactly one
    // event for each type specified.
    pub fn validate_types(&self, events: Vec<&Event>) -> Result<(), String> {
        match self {
            Self::Singleton(tpe) => {
                let illegal_events: Vec<String> = events
                    .iter()
                    .filter_map(|ev| {
                        // Extract the underlying type, otherwise we're getting the TypeId of the Arc
                        if (&*ev.get_data()).type_id() != *tpe {
                            Some(format!(
                                "[{}: {:?}]",
                                ev.name.clone(),
                                (&*ev.get_data()).type_id()
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                if illegal_events.len() > 0 {
                    Err(format!(
                        "all events must match single type {:?}, found illegal events {:?}",
                        tpe, illegal_events,
                    ))
                } else {
                    Ok(())
                }
            }
            Self::AllOf(types) => {
                if types.len() != events.len() {
                    return Err(format!(
                        "number of events ({}) does not match the list of required types ({})",
                        events.len(),
                        types.len()
                    ));
                }
                let mut present = HashMap::new();
                for ev in events {
                    if present.contains_key(&(&*ev.get_data()).type_id()) {
                        return Err(format!("duplicate event for type {:?}; must have a single event for each of {:?}", (&*ev.get_data()).type_id(), types));
                    } else {
                        present.insert((&*ev.get_data()).type_id(), ());
                    }
                }
                Ok(())
            }
        }
    }
}
